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
                inner: Some(Inner { usid, jwt, start: SystemTime::now(), conn_id }),
            },
            _ => Self { inner: None },
        }
    }
}

impl Drop for ViewGuard {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            report_view(inner.usid, inner.jwt, inner.conn_id, inner.start, SystemTime::now());
        }
    }
}

/// Fire-and-forget POST of one subscriber session's viewer-seconds.
fn report_view(usid: String, jwt: Option<String>, conn_id: u64, start: SystemTime, end: SystemTime) {
    let url = match std::env::var("MOQ_USAGE_REPORT_URL") {
        Ok(u) if !u.is_empty() => u,
        _ => return,
    };
    let secret = std::env::var("MOQ_USAGE_REPORT_SECRET").unwrap_or_default();
    let secs = end.duration_since(start).map(|d| d.as_secs()).unwrap_or(0);
    if secs == 0 {
        return; // sub-second / failed-negotiation sessions: nothing to bill.
    }
    let start_ms = start.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis();
    let idem = format!("moq:{usid}:{conn_id}:{start_ms}");
    // transport intentionally omitted -> cloud uses the anchored session's transport
    // (moq vs moq_tiles), which the relay can't know.
    let body = serde_json::json!({
        "usage_session_id": usid,
        "meter": "live_viewer_second",
        "quantity": secs.to_string(),
        "unit": "second",
        "observed_at": epoch_secs_f64(end),
        "window_start": epoch_secs_f64(start),
        "window_end": epoch_secs_f64(end),
        "idempotency_key": idem,
        "lease_token": jwt,
    });
    let payload = match serde_json::to_vec(&body) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(%usid, error = %e, "svx usage report serialize failed");
            return;
        }
    };
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
					tracing::debug!(%usid, secs, "svx usage report ok");
					return;
				}
				Ok(r) => {
					let retryable = r.status().is_server_error();
					tracing::warn!(%usid, status = %r.status(), attempt, "svx usage report rejected");
					if !retryable {
						return;
					}
				}
				Err(e) => tracing::warn!(%usid, error = %e, attempt, "svx usage report failed"),
			}
			if attempt == 0 {
				tokio::time::sleep(std::time::Duration::from_secs(2)).await;
			}
		}
	});
}
