// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Semaphore, watch};

use crate::jobs::graph_checkpoint::Delta;
use crate::jobs::ids::JobId;
use crate::{Database, StateError};

use super::sql_runtime::{
    is_postgres_connection_error, is_postgres_rolled_back_transient, is_transient_sqlite_busy,
};

// A checkpoint has one owner across retry attempts. Resume is a revision,
// so a resume received during an attempt cannot be lost before it waits.
#[derive(Clone, Debug)]
pub(crate) struct CheckpointControl(Arc<watch::Sender<(u64, bool)>>);

impl Default for CheckpointControl {
    fn default() -> Self {
        Self(Arc::new(watch::channel((0, false)).0))
    }
}

impl CheckpointControl {
    pub(crate) fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub(crate) fn cancelled(&self) -> bool {
        self.0.borrow().1
    }

    pub(crate) fn revision(&self) -> u64 {
        self.0.borrow().0
    }

    pub(crate) fn cancel(&self) {
        self.0.send_modify(|state| state.1 = true);
    }

    pub(crate) fn resume(&self) {
        self.0.send_modify(|state| {
            state.0 = state.0.wrapping_add(1);
            state.1 = false;
        });
    }
}

type BlockedCheckpoint = dyn Fn(u64, &str) + Send + Sync;

// The writer takes a coalesced delta only when its ordered command starts.
// After that it keeps this immutable payload until commit or cancellation.
// Only save_graph_checkpoint is replayed; arbitrary Write closures are not.
pub(crate) struct CheckpointWrite {
    pub(crate) delta: Option<Arc<Delta>>,
    pub(crate) control: CheckpointControl,
    pub(crate) blocked: Box<BlockedCheckpoint>,
    pub(crate) finished: Box<dyn FnOnce(Result<(), StateError>) + Send>,
}

impl CheckpointWrite {
    pub(crate) async fn run(
        self,
        job_id: JobId,
        db: &Database,
        shutdown: watch::Receiver<bool>,
        permits: &Arc<Semaphore>,
    ) -> Result<bool, String> {
        let delta = self.delta;
        let result = replay(
            &self.control,
            shutdown,
            || {
                let db = db.clone();
                let delta = delta.clone();
                let permits = Arc::clone(permits);
                let control = self.control.clone();
                async move {
                    let Some(delta) = delta else { return Ok(()) };
                    // The ordered lane stays owned across retries, while the
                    // SQL execution slot belongs only to this attempt.
                    let _permit = permits.acquire_owned().await.map_err(|error| {
                        StateError::Database(format!("checkpoint execution stopped: {error}"))
                    })?;
                    if control.cancelled() {
                        return Ok(());
                    }
                    tokio::task::spawn_blocking(move || db.save_graph_checkpoint(job_id, delta))
                        .await
                        .map_err(|error| {
                            StateError::Database(format!("checkpoint writer failed: {error}"))
                        })?
                }
            },
            |revision, error| (self.blocked)(revision, &error.to_string()),
        )
        .await;
        let reported = result.as_ref().copied().map_err(ToString::to_string);
        (self.finished)(match result {
            Ok(true) => Ok(()),
            Ok(false) => Err(StateError::Database(
                "checkpoint cancelled before durability acknowledgment".into(),
            )),
            Err(error) => Err(error),
        });
        reported
    }
}

fn retryable(error: &StateError) -> bool {
    is_transient_sqlite_busy(error)
        || is_postgres_rolled_back_transient(error)
        // Replaying this particular transaction is idempotent even when
        // a connection failed after an unknown commit outcome.
        || is_postgres_connection_error(error)
}

fn next_backoff(delay: Duration) -> Duration {
    delay.saturating_mul(2).min(Duration::from_secs(30))
}

#[cfg(test)]
mod tests;

async fn replay<F, Fut>(
    control: &CheckpointControl,
    mut shutdown: watch::Receiver<bool>,
    mut write: F,
    blocked: impl Fn(u64, &StateError),
) -> Result<bool, StateError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), StateError>>,
{
    let mut requests = control.0.subscribe();
    let mut delay = Duration::from_millis(250);
    loop {
        let (revision, cancelled) = *requests.borrow_and_update();
        if cancelled {
            return Ok(false);
        }
        // A blocking database call retains ownership until it returns.
        let result = write().await;
        if control.cancelled() {
            return result.map(|()| false);
        }
        let error = match result {
            Ok(()) => return Ok(true),
            Err(error) => error,
        };
        if *shutdown.borrow() {
            control.cancel();
            return Err(error);
        }
        let transient = retryable(&error);
        tracing::warn!(%error, retryable = transient, retry_delay_ms = delay.as_millis(), "graph checkpoint commit failed");
        if !transient {
            blocked(revision, &error);
        }
        let retry_at = tokio::time::Instant::now() + delay;
        loop {
            let (resumed, cancelled) = *requests.borrow_and_update();
            if cancelled || *shutdown.borrow() {
                control.cancel();
                return Err(error);
            }
            if resumed != revision {
                delay = Duration::from_millis(250);
                break;
            }
            tokio::select! {
                _ = tokio::time::sleep_until(retry_at), if transient => {
                    delay = next_backoff(delay);
                    break;
                }
                changed = requests.changed() => {
                    if changed.is_err() { return Err(error); }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() {
                        control.cancel();
                        return Err(error);
                    }
                }
            }
        }
    }
}
