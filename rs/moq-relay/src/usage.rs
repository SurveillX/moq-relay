//! SurveillX measured-viewing usage reporter (additive fork; Phase C).
//!
//! When `MOQ_USAGE_REPORT_URL` is set, each metered subscriber session POSTs its
//! viewer-seconds to the cloud usage API on disconnect. Unset env => total no-op,
//! so stock relay behavior is preserved. Publishers, internal/cluster peers, and
//! tokens without a `usid` claim are never reported.
//!
//! The body matches surveillx-api `ReportRequest` (POST /api/usage/v1/report):
//! Bearer auth carries the reporter identity; datetimes are sent as epoch seconds
//! (Pydantic coerces). `transport` is omitted so the cloud uses the session's.

use std::time::{SystemTime, UNIX_EPOCH};

use std::sync::OnceLock;

static REPORT_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

fn report_client() -> &'static reqwest::Client {
	REPORT_CLIENT.get_or_init(|| {
		reqwest::Client::builder()
			.timeout(std::time::Duration::from_secs(5))
			.build()
			.unwrap_or_else(|_| reqwest::Client::new())
	})
}

fn epoch_secs_f64(t: SystemTime) -> f64 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64()
}

/// RAII guard: created when a metered subscriber is accepted; on Drop (session
/// close / connection teardown) it fires one fire-and-forget viewer-seconds POST.
pub struct ViewGuard {
    // Some(..) only when this session should be reported.
    inner: Option<Inner>,
}

struct Inner {
    usid: String,
    jwt: Option<String>,
    start: SystemTime,
    conn_id: u64,
    bytes: Option<u64>,
}

impl ViewGuard {
    /// `is_subscriber` = the connection is a *pure subscriber* (subscribe-only,
    /// no publish grant) — a downlink viewer. Reports only external pure
    /// subscribers that carry a usid, and only when the report URL env is set.
    pub fn new(usid: Option<String>, jwt: Option<String>, conn_id: u64, internal: bool, is_subscriber: bool) -> Self {
        let enabled = std::env::var("MOQ_USAGE_REPORT_URL")
            .map(|u| !u.is_empty())
            .unwrap_or(false);
        match usid {
            Some(usid) if enabled && !internal && is_subscriber => Self {
                inner: Some(Inner { usid, jwt, start: SystemTime::now(), conn_id, bytes: None }),
            },
            _ => Self { inner: None },
        }
    }

    /// Record per-subscriber egress bytes (read at session close). Reported as
    /// `live_fanout_byte` alongside viewer-seconds on drop.
    pub fn set_bytes(&mut self, bytes: Option<u64>) {
        if let Some(inner) = self.inner.as_mut() {
            inner.bytes = bytes;
        }
    }
}

impl Drop for ViewGuard {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            report_view(inner.usid, inner.jwt, inner.conn_id, inner.bytes, inner.start, SystemTime::now());
        }
    }
}

/// Fire-and-forget POST(s) of one subscriber session's usage: viewer-seconds
/// always, plus fan-out egress bytes when the transport exposed a byte count.
fn report_view(usid: String, jwt: Option<String>, conn_id: u64, bytes: Option<u64>, start: SystemTime, end: SystemTime) {
    let url = match std::env::var("MOQ_USAGE_REPORT_URL") {
        Ok(u) if !u.is_empty() => u,
        _ => return,
    };
    let secret = std::env::var("MOQ_USAGE_REPORT_SECRET").unwrap_or_default();
    let start_ms = start.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis();
    let secs = end.duration_since(start).map(|d| d.as_secs()).unwrap_or(0);

    // Viewer-seconds. Skip sub-second / failed-negotiation sessions.
    if secs > 0 {
        post_meter(
            &url, &secret, &usid, jwt.as_deref(),
            "live_viewer_second", secs.to_string(), "second",
            format!("moq:{usid}:{conn_id}:{start_ms}"),
            start, end,
        );
    }

    // Fan-out egress bytes. moq_tiles: each per-camera connection reports its own
    // byte count; the cloud SUMS them (egress scales with cameras -> no union).
    if let Some(b) = bytes {
        if b > 0 {
            post_meter(
                &url, &secret, &usid, jwt.as_deref(),
                "live_fanout_byte", b.to_string(), "byte",
                format!("moqb:{usid}:{conn_id}:{start_ms}"),
                start, end,
            );
        }
    }
}

/// Build + fire one fire-and-forget metered POST (2-try retry on 5xx/transport).
/// transport intentionally omitted -> cloud uses the anchored session's transport
/// (moq vs moq_tiles), which the relay can't know.
#[allow(clippy::too_many_arguments)]
fn post_meter(
    url: &str,
    secret: &str,
    usid: &str,
    jwt: Option<&str>,
    meter: &str,
    quantity: String,
    unit: &str,
    idem: String,
    start: SystemTime,
    end: SystemTime,
) {
    let body = serde_json::json!({
        "usage_session_id": usid,
        "meter": meter,
        "quantity": quantity,
        "unit": unit,
        "observed_at": epoch_secs_f64(end),
        "window_start": epoch_secs_f64(start),
        "window_end": epoch_secs_f64(end),
        "idempotency_key": idem,
        "lease_token": jwt,
    });
    let payload = match serde_json::to_vec(&body) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(%usid, meter, error = %e, "svx usage report serialize failed");
            return;
        }
    };
    let url = url.to_string();
    let secret = secret.to_string();
    let usid = usid.to_string();
    let meter = meter.to_string();
    tokio::spawn(async move {
        for attempt in 0u8..2 {
            let mut req = report_client()
                .post(&url)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(payload.clone());
            if !secret.is_empty() {
                req = req.bearer_auth(&secret);
            }
            match req.send().await {
                Ok(r) if r.status().is_success() => {
                    tracing::debug!(%usid, meter, "svx usage report ok");
                    return;
                }
                Ok(r) => {
                    let retryable = r.status().is_server_error();
                    tracing::warn!(%usid, meter, status = %r.status(), attempt, "svx usage report rejected");
                    if !retryable {
                        return;
                    }
                }
                Err(e) => tracing::warn!(%usid, meter, error = %e, attempt, "svx usage report failed"),
            }
            if attempt == 0 {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    });
}
