// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Liveness and readiness probes.
//
// Both sit on the normal router, under the configured base URL, behind the
// same Host allowlist as every other route — a probe is not a reason to open a
// second, unguarded surface.
//
// The split is the usual one. `/healthz` answers "is this process up and
// serving HTTP"; if it can reply at all, the answer is yes. `/readyz` answers
// "should traffic be sent here", which additionally requires that the
// scheduler is alive and the database answers. Neither probe sends a command
// through the scheduler channel: a readiness check that queues work behind the
// pipeline loop would report "not ready" precisely when the box is busiest,
// which is the opposite of useful.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::Extension;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use tokio::sync::watch;

use weaver_server_core::{Database, SchedulerHandle};

// How long the readiness probe waits for the database to answer `SELECT 1`.
const DB_PROBE_TIMEOUT: Duration = Duration::from_secs(1);
// How long a database probe verdict is re-used before the datastore is asked
// again. `/readyz` is unauthenticated, and the sqlite runtime is a single
// serialized worker shared with the pipeline's own writes: without this cache
// a probe flood would queue `SELECT 1`s in front of segment commits. One
// probe every couple of seconds is plenty for any orchestrator.
const DB_PROBE_CACHE_TTL: Duration = Duration::from_secs(2);
static DB_PROBE: ProbeCache = ProbeCache::new(DB_PROBE_CACHE_TTL, DB_PROBE_TIMEOUT);

// The last database verdict and the probe running now, if any.
//
// At most one probe runs at a time. A request that finds the verdict stale
// while a probe runs waits on that probe instead of starting another, so a
// slow database sees one `SELECT 1`, not one per request. A verdict is
// stamped when its probe ends.
struct ProbeCache {
    ttl: Duration,
    timeout: Duration,
    state: Mutex<ProbeState>,
}

struct ProbeState {
    verdict: Option<(Instant, bool)>,
    running: Option<Arc<watch::Sender<Option<bool>>>>,
}

impl ProbeCache {
    const fn new(ttl: Duration, timeout: Duration) -> Self {
        Self {
            ttl,
            timeout,
            state: Mutex::new(ProbeState {
                verdict: None,
                running: None,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ProbeState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    // The fresh verdict, or the probe to wait on, starting `probe` on a
    // blocking worker when none runs.
    async fn verdict(&'static self, probe: impl FnOnce() -> bool + Send + 'static) -> bool {
        let mut waiting = {
            let mut state = self.lock();
            if let Some((probed_at, verdict)) = state.verdict
                && probed_at.elapsed() < self.ttl
            {
                return verdict;
            }
            match &state.running {
                Some(running) => running.subscribe(),
                None => {
                    let (sender, receiver) = watch::channel(None);
                    let sender = Arc::new(sender);
                    state.running = Some(Arc::clone(&sender));
                    let finish = ProbeFinish {
                        cache: self,
                        sender,
                        verdict: false,
                    };
                    tokio::task::spawn_blocking(move || {
                        let mut finish = finish;
                        finish.verdict = probe();
                    });
                    receiver
                }
            }
        };
        matches!(
            tokio::time::timeout(self.timeout, waiting.wait_for(Option::is_some)).await,
            Ok(Ok(verdict)) if *verdict == Some(true)
        )
    }
}

// Records the verdict and ends the running probe when the probe returns. A
// probe that panics records a failed verdict, so the next request can probe
// again.
struct ProbeFinish {
    cache: &'static ProbeCache,
    sender: Arc<watch::Sender<Option<bool>>>,
    verdict: bool,
}

impl Drop for ProbeFinish {
    fn drop(&mut self) {
        {
            let mut state = self.cache.lock();
            state.verdict = Some((Instant::now(), self.verdict));
            state.running = None;
        }
        self.sender.send_replace(Some(self.verdict));
    }
}

// Liveness. Returns 200 as soon as the HTTP server is serving.
pub(super) async fn healthz_handler() -> Response {
    (StatusCode::OK, "ok").into_response()
}

// The individual readiness checks, separated from the handler so the
// pass/fail composition is testable without a live server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) struct ReadinessChecks {
    pub(super) scheduler_alive: bool,
    pub(super) database_responsive: bool,
    // Informational only: a box with no news servers configured yet is still
    // ready to accept API traffic, so this never gates the verdict.
    pub(super) nntp_activated: bool,
}

impl ReadinessChecks {
    pub(super) fn is_ready(self) -> bool {
        self.scheduler_alive && self.database_responsive
    }

    // Reasons the probe failed, in a stable order.
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

// Readiness. 200 once the scheduler is alive and the database answers.
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

// Whether the pipeline task is still around to receive work.
//
// Reads the command channel's liveness rather than sending anything through
// it, so a saturated but healthy pipeline still reports ready.
fn scheduler_is_alive(handle: &SchedulerHandle) -> bool {
    !handle.is_closed()
}

// Whether the database answers a trivial query inside [`DB_PROBE_TIMEOUT`].
//
// The query runs on a blocking worker: the database facade is synchronous, and
// blocking an axum worker thread on it would be a readiness probe that can
// itself take the server down.
async fn database_is_responsive(db: &Database) -> bool {
    let db = db.clone();
    DB_PROBE.verdict(move || db.probe_liveness().is_ok()).await
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

    use std::sync::atomic::{AtomicUsize, Ordering};

    // A probe that counts its runs and answers `answer` once its gate opens.
    fn gated_probe(
        runs: &Arc<AtomicUsize>,
        gate: &Arc<(Mutex<bool>, std::sync::Condvar)>,
        answer: bool,
    ) -> impl FnOnce() -> bool + Send + 'static {
        let runs = Arc::clone(runs);
        let gate = Arc::clone(gate);
        move || {
            runs.fetch_add(1, Ordering::SeqCst);
            let (open, opened) = &*gate;
            let mut open = open.lock().unwrap();
            while !*open {
                open = opened.wait(open).unwrap();
            }
            answer
        }
    }

    fn open_gate(gate: &(Mutex<bool>, std::sync::Condvar)) {
        *gate.0.lock().unwrap() = true;
        gate.1.notify_all();
    }

    // How many requests wait on the running probe.
    fn waiters(cache: &ProbeCache) -> usize {
        cache
            .lock()
            .running
            .as_ref()
            .map_or(0, |running| running.receiver_count())
    }

    // A stale verdict and many concurrent requests: one probe, and every
    // request answers with its verdict.
    async fn concurrent_requests_share_one_probe(answer: bool) {
        const REQUESTS: usize = 32;
        // Every verdict is stale at once; the wait bound is never reached.
        let cache: &'static ProbeCache = Box::leak(Box::new(ProbeCache::new(
            Duration::ZERO,
            Duration::from_secs(24 * 60 * 60),
        )));
        cache.lock().verdict = Some((Instant::now(), !answer));
        let runs = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));

        let requests: Vec<_> = (0..REQUESTS)
            .map(|_| tokio::spawn(cache.verdict(gated_probe(&runs, &gate, answer))))
            .collect();
        while waiters(cache) < REQUESTS {
            tokio::task::yield_now().await;
        }
        open_gate(&gate);
        for request in requests {
            assert_eq!(request.await.unwrap(), answer);
        }
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert_eq!(
            cache.lock().verdict.map(|(_, verdict)| verdict),
            Some(answer)
        );
        assert!(cache.lock().running.is_none());

        // The probe has ended, so the next stale request probes again.
        let gate = Arc::new((Mutex::new(true), std::sync::Condvar::new()));
        assert_eq!(
            cache.verdict(gated_probe(&runs, &gate, answer)).await,
            answer
        );
        assert_eq!(runs.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_healthy_database_is_probed_once_for_concurrent_requests() {
        concurrent_requests_share_one_probe(true).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unhealthy_database_is_probed_once_for_concurrent_requests() {
        concurrent_requests_share_one_probe(false).await;
    }

    #[tokio::test]
    async fn a_fresh_verdict_answers_without_a_probe() {
        let cache: &'static ProbeCache = Box::leak(Box::new(ProbeCache::new(
            Duration::from_secs(24 * 60 * 60),
            Duration::from_secs(24 * 60 * 60),
        )));
        let runs = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new((Mutex::new(true), std::sync::Condvar::new()));
        for answer in [true, false] {
            cache.lock().verdict = Some((Instant::now(), answer));
            assert_eq!(
                cache.verdict(gated_probe(&runs, &gate, !answer)).await,
                answer
            );
        }
        assert_eq!(runs.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_hung_probe_answers_unready_without_starting_another() {
        // A zero wait bound gives up on a probe that has not answered yet.
        let cache: &'static ProbeCache =
            Box::leak(Box::new(ProbeCache::new(Duration::ZERO, Duration::ZERO)));
        let runs = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        for _ in 0..8 {
            assert!(!cache.verdict(gated_probe(&runs, &gate, true)).await);
        }
        assert!(cache.lock().running.is_some());
        assert!(runs.load(Ordering::SeqCst) <= 1);
        let mut ended = cache.lock().running.as_ref().unwrap().subscribe();
        open_gate(&gate);
        ended.wait_for(Option::is_some).await.unwrap();
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert!(cache.lock().running.is_none());
        assert_eq!(cache.lock().verdict.map(|(_, verdict)| verdict), Some(true));
    }
}
