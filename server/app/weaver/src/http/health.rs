//! Liveness and readiness probes.
//!
//! Both sit on the normal router, under the configured base URL, behind the
//! same Host allowlist as every other route — a probe is not a reason to open a
//! second, unguarded surface.
//!
//! The split is the usual one. `/healthz` answers "is this process up and
//! serving HTTP"; if it can reply at all, the answer is yes. `/readyz` answers
//! "should traffic be sent here", which additionally requires that the
//! scheduler is alive and the database answers. Neither probe sends a command
//! through the scheduler channel: a readiness check that queues work behind the
//! pipeline loop would report "not ready" precisely when the box is busiest,
//! which is the opposite of useful.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::extract::Extension;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use weaver_server_core::{Database, SchedulerHandle};

/// How long the readiness probe waits for the database to answer `SELECT 1`.
const DB_PROBE_TIMEOUT: Duration = Duration::from_secs(1);
/// How long a database probe verdict is re-used before the datastore is asked
/// again. `/readyz` is unauthenticated, and the sqlite runtime is a single
/// serialized worker shared with the pipeline's own writes: without this cache
/// a probe flood would queue `SELECT 1`s in front of segment commits. One
/// probe every couple of seconds is plenty for any orchestrator.
const DB_PROBE_CACHE_TTL: Duration = Duration::from_secs(2);
static DB_PROBE_CACHE: Mutex<Option<(Instant, bool)>> = Mutex::new(None);

/// Liveness. Returns 200 as soon as the HTTP server is serving.
pub(super) async fn healthz_handler() -> Response {
    (StatusCode::OK, "ok").into_response()
}

/// The individual readiness checks, separated from the handler so the
/// pass/fail composition is testable without a live server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) struct ReadinessChecks {
    pub(super) scheduler_alive: bool,
    pub(super) database_responsive: bool,
    /// Informational only: a box with no news servers configured yet is still
    /// ready to accept API traffic, so this never gates the verdict.
    pub(super) nntp_activated: bool,
}

impl ReadinessChecks {
    pub(super) fn is_ready(self) -> bool {
        self.scheduler_alive && self.database_responsive
    }

    /// Reasons the probe failed, in a stable order.
    pub(super) fn failure_reasons(self) -> Vec<&'static str> {
        let mut reasons = Vec::new();
        if !self.scheduler_alive {
            reasons.push("scheduler unavailable");
        }
        if !self.database_responsive {
            reasons.push("database unavailable");
        }
        reasons
    }

    pub(super) fn into_response(self) -> Response {
        if self.is_ready() {
            return (StatusCode::OK, "ready").into_response();
        }
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("not ready: {}", self.failure_reasons().join(", ")),
        )
            .into_response()
    }
}

/// Readiness. 200 once the scheduler is alive and the database answers.
pub(super) async fn readyz_handler(
    Extension(handle): Extension<SchedulerHandle>,
    Extension(db): Extension<Database>,
) -> Response {
    evaluate_readiness(&handle, &db).await.into_response()
}

pub(super) async fn evaluate_readiness(handle: &SchedulerHandle, db: &Database) -> ReadinessChecks {
    ReadinessChecks {
        scheduler_alive: scheduler_is_alive(handle),
        database_responsive: database_is_responsive(db).await,
        nntp_activated: handle.nntp_runtime_activation().is_some(),
    }
}

/// Whether the pipeline task is still around to receive work.
///
/// Reads the command channel's liveness rather than sending anything through
/// it, so a saturated but healthy pipeline still reports ready.
fn scheduler_is_alive(handle: &SchedulerHandle) -> bool {
    !handle.is_closed()
}

/// Whether the database answers a trivial query inside [`DB_PROBE_TIMEOUT`].
///
/// The query runs on a blocking worker: the database facade is synchronous, and
/// blocking an axum worker thread on it would be a readiness probe that can
/// itself take the server down.
async fn database_is_responsive(db: &Database) -> bool {
    let now = Instant::now();
    if let Some((probed_at, verdict)) = *DB_PROBE_CACHE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        && now.duration_since(probed_at) < DB_PROBE_CACHE_TTL
    {
        return verdict;
    }
    let db = db.clone();
    let probe = tokio::task::spawn_blocking(move || db.probe_liveness());
    let verdict = matches!(
        tokio::time::timeout(DB_PROBE_TIMEOUT, probe).await,
        Ok(Ok(Ok(())))
    );
    *DB_PROBE_CACHE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((now, verdict));
    verdict
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_requires_the_scheduler_and_the_database() {
        let ready = ReadinessChecks {
            scheduler_alive: true,
            database_responsive: true,
            nntp_activated: false,
        };
        assert!(
            ready.is_ready(),
            "a box with no NNTP generation yet is still ready to serve"
        );
        assert!(ready.failure_reasons().is_empty());

        let no_scheduler = ReadinessChecks {
            scheduler_alive: false,
            ..ready
        };
        assert!(!no_scheduler.is_ready());
        assert_eq!(
            no_scheduler.failure_reasons(),
            vec!["scheduler unavailable"]
        );

        let no_db = ReadinessChecks {
            database_responsive: false,
            ..ready
        };
        assert!(!no_db.is_ready());
        assert_eq!(no_db.failure_reasons(), vec!["database unavailable"]);
    }

    #[test]
    fn every_failed_check_is_named_in_the_reason_list() {
        let broken = ReadinessChecks::default();
        assert!(!broken.is_ready());
        assert_eq!(
            broken.failure_reasons(),
            vec!["scheduler unavailable", "database unavailable"]
        );
    }

    #[tokio::test]
    async fn ready_and_unready_map_to_200_and_503() {
        let ready = ReadinessChecks {
            scheduler_alive: true,
            database_responsive: true,
            nntp_activated: true,
        };
        assert_eq!(ready.into_response().status(), StatusCode::OK);
        assert_eq!(
            ReadinessChecks::default().into_response().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn healthz_is_always_ok_once_it_can_answer() {
        assert_eq!(healthz_handler().await.status(), StatusCode::OK);
    }
}
