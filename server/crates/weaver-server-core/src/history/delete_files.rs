// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::JobId;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;
use tracing::{debug, info, warn};

// One job's working directories a history delete will remove, and the ones
// it will leave on disk because their ownership marker no longer matches.
#[derive(Debug, Default)]
struct JobCleanupDirs {
    owned: BTreeSet<PathBuf>,
    left_in_place: Vec<PathBuf>,
}

async fn retry_script_stop_wait<F, W>(job_id: JobId, mut wait: W) -> Result<(), crate::StateError>
where
    F: std::future::Future<Output = Result<(), crate::StateError>>,
    W: FnMut() -> F,
{
    for attempt in 0..7 {
        match wait().await {
            Ok(()) => return Ok(()),
            Err(error) => {
                if attempt == 6 {
                    return Err(crate::StateError::Conflict(format!(
                        "could not confirm scripts stopped for job {} after seven attempts: {error}",
                        job_id.0
                    )));
                }
                warn!(job_id = job_id.0, %error, "could not confirm scripts stopped; retrying history cleanup barrier");
                tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
            }
        }
    }
    unreachable!("the last attempt returns its error")
}

impl JobCleanupDirs {
    async fn admit(
        &mut self,
        intermediate_dir: &std::path::Path,
        job_id: JobId,
        path: PathBuf,
    ) -> Result<(), crate::SchedulerError> {
        match crate::jobs::working_dir::prepare_history_working_dir(intermediate_dir, &path, job_id)
            .await?
        {
            crate::jobs::working_dir::HistoryWorkingDir::Owned => {
                self.owned.insert(path);
            }
            crate::jobs::working_dir::HistoryWorkingDir::LeftInPlace => {
                if !self.left_in_place.contains(&path) {
                    self.left_in_place.push(path);
                }
            }
        }
        Ok(())
    }

    // Removes every owned directory, and returns every directory left on
    // disk: the ones already set aside plus any whose marker stopped matching
    // between the two looks.
    async fn remove(
        self,
        intermediate_dir: &std::path::Path,
        job_id: JobId,
    ) -> Result<Vec<PathBuf>, crate::SchedulerError> {
        let mut left_in_place = self.left_in_place;
        for dir in self.owned {
            // Failed jobs never pass through the finalize close, so drop any
            // cached write handles before their dirs (and paths) are freed
            // for reuse.
            crate::pipeline::close_cached_write_handles_under(&dir).await;
            let root = intermediate_dir.to_path_buf();
            let target = dir.clone();
            let removal = tokio::task::spawn_blocking(move || {
                crate::jobs::working_dir::remove_job_working_dir(&root, &target, job_id)
            })
            .await
            .map_err(|error| crate::SchedulerError::Io(std::io::Error::other(error)))?;
            match removal {
                Ok(crate::jobs::working_dir::HistoryWorkingDir::Owned) => {
                    info!(dir = %dir.display(), "removed historical intermediate directory");
                }
                Ok(crate::jobs::working_dir::HistoryWorkingDir::LeftInPlace) => {
                    left_in_place.push(dir);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(crate::SchedulerError::Io(std::io::Error::new(
                        error.kind(),
                        format!(
                            "failed to remove historical intermediate directory '{}': {error}",
                            dir.display()
                        ),
                    )));
                }
            }
        }
        Ok(left_in_place)
    }
}

// A directory a history delete may remove: a direct child of the
// intermediate root, never the root itself or anything deeper.
fn cleanupable_history_output_dir(
    intermediate_dir: &std::path::Path,
    output_dir: &std::path::Path,
) -> Option<PathBuf> {
    output_dir
        .strip_prefix(intermediate_dir)
        .ok()
        .filter(|suffix| {
            let mut components = suffix.components();
            matches!(components.next(), Some(std::path::Component::Normal(_)))
                && components.next().is_none()
        })
        .map(|_| output_dir.to_path_buf())
}

async fn cleanup_owned_output_dir(dir: Option<&std::path::Path>) {
    let Some(dir) = dir else { return };
    match tokio::fs::symlink_metadata(dir).await {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            warn!(dir = %dir.display(), error = %error, "could not inspect complete output directory before cleanup");
            return;
        }
        Ok(_) => {}
    }
    let ownership_path = dir.to_path_buf();
    let owned = tokio::task::spawn_blocking(move || {
        crate::jobs::working_dir::is_weaver_owned_output_dir(&ownership_path)
    })
    .await
    .unwrap_or(false);
    if !owned {
        warn!(
            dir = %dir.display(),
            "refusing recursive cleanup of an output directory without a valid Weaver ownership marker"
        );
        return;
    }
    match tokio::fs::remove_dir_all(dir).await {
        Ok(()) => {
            debug!(dir = %dir.display(), "removed complete output directory");
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            warn!(
                dir = %dir.display(),
                error = %error,
                "failed to remove complete output directory"
            );
        }
    }
}

// History deletion owns persistence and files independently of the pipeline.
// In order: one read of the
// batch's history rows, the marker judgement of every directory, one delete
// transaction for every row, then per job the script-stopped barrier and only
// after it the job's directories. Returns the ids whose rows the transaction
// removed, and the batch outcome.
pub(crate) async fn delete_history_rows(
    db: &crate::Database,
    intermediate_dir: &std::path::Path,
    job_ids: BTreeSet<JobId>,
    delete_files: bool,
) -> (
    BTreeSet<JobId>,
    Result<crate::HistoryDeleteBatchOutcome, crate::SchedulerError>,
) {
    for id in &job_ids {
        db.cancel_event_scripts(id.0);
    }
    let mut output_dirs = BTreeMap::new();
    let mut refused = BTreeMap::new();
    let ids = job_ids.iter().copied().collect::<Vec<_>>();
    let lookup_db = db.clone();
    let rows = match tokio::task::spawn_blocking(move || {
        let _probe =
            crate::runtime::perf_probe::scope("db.list_job_history_output_dirs.history_delete");
        lookup_db.list_job_history_output_dirs(&ids)
    })
    .await
    {
        Ok(Ok(rows)) => rows,
        Ok(Err(error)) => return (BTreeSet::new(), Err(crate::SchedulerError::State(error))),
        Err(error) => {
            return (
                BTreeSet::new(),
                Err(crate::SchedulerError::Internal(format!(
                    "failed to join history lookup task: {error}"
                ))),
            );
        }
    };

    let mut cleanup = BTreeMap::new();
    let mut delete_ids = Vec::with_capacity(job_ids.len());
    for job_id in job_ids {
        let recorded = rows.get(&job_id).cloned().flatten().map(PathBuf::from);
        let mut dirs = JobCleanupDirs::default();
        let candidates = recorded
            .as_deref()
            .and_then(|dir| cleanupable_history_output_dir(intermediate_dir, dir));
        let admitted = if let Some(path) = candidates {
            dirs.admit(intermediate_dir, job_id, path).await
        } else {
            Ok(())
        };
        if let Err(error) = admitted {
            refused.insert(job_id, error);
            output_dirs.remove(&job_id);
            continue;
        }
        if delete_files {
            if let Some(dir) = recorded {
                output_dirs.entry(job_id).or_insert(dir);
            }
        } else {
            output_dirs.remove(&job_id);
        }
        cleanup.insert(job_id, dirs);
        delete_ids.push(job_id);
    }

    let delete_db = db.clone();
    let delete_set = delete_ids.clone();
    match tokio::task::spawn_blocking(move || {
        delete_db.delete_job_histories_and_forget_duplicate_identities(&delete_set)
    })
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(crate::StateError::Conflict(message))) => {
            return (
                BTreeSet::new(),
                Err(crate::SchedulerError::Conflict(message)),
            );
        }
        Ok(Err(error)) => {
            return (
                BTreeSet::new(),
                Err(crate::SchedulerError::Internal(format!(
                    "failed to delete history bundles: {error}"
                ))),
            );
        }
        Err(error) => {
            return (
                BTreeSet::new(),
                Err(crate::SchedulerError::Internal(format!(
                    "failed to join history delete task: {error}"
                ))),
            );
        }
    }

    // Every accepted id's record is gone now, including an id that had no
    // row. No pipeline state participates in a history deletion.
    let removed = delete_ids.iter().copied().collect::<BTreeSet<_>>();
    let mut deleted = BTreeMap::new();
    for (job_id, dirs) in cleanup {
        if let Err(error) = retry_script_stop_wait(job_id, || {
            crate::post_processing::events::wait_for_job_events_stopped(db, job_id.0)
        })
        .await
        {
            deleted.insert(job_id, Err(crate::SchedulerError::State(error)));
            continue;
        }
        match dirs.remove(intermediate_dir, job_id).await {
            Ok(left_in_place) => {
                cleanup_owned_output_dir(output_dirs.get(&job_id).map(PathBuf::as_path)).await;
                deleted.insert(job_id, Ok(crate::HistoryDeleteOutcome { left_in_place }));
            }
            Err(error) => {
                deleted.insert(job_id, Err(error));
            }
        }
    }
    (
        removed,
        Ok(crate::HistoryDeleteBatchOutcome { deleted, refused }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    #[tokio::test(start_paused = true)]
    async fn history_cleanup_surfaces_persistent_failure_after_bounded_backoff() {
        let mut attempts = Vec::new();
        let error = retry_script_stop_wait(JobId(42), || {
            attempts.push(tokio::time::Instant::now());
            std::future::ready(Err(crate::StateError::Database(
                "fixture unavailable".into(),
            )))
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("after seven attempts"));
        assert_eq!(attempts.len(), 7);
        let gaps: Vec<_> = attempts.windows(2).map(|pair| pair[1] - pair[0]).collect();
        assert_eq!(gaps, [1, 2, 4, 8, 16, 32].map(Duration::from_secs));
    }

    #[tokio::test(start_paused = true)]
    async fn history_cleanup_retries_uncertain_wait_and_still_waits_for_active_script() {
        let db = crate::Database::open_in_memory().unwrap();
        let job_id = JobId(41);
        let active =
            crate::post_processing::events::hold_test_event_run(&db, job_id.0, "cleanup-retry");
        let (failed, failure_seen) = oneshot::channel();
        let (retried, retry_seen) = oneshot::channel();
        let cleanup = tokio::spawn(async move {
            let mut failed = Some(failed);
            let mut retried = Some(retried);
            retry_script_stop_wait(job_id, || {
                let failed = failed.take();
                let retried = if failed.is_none() {
                    retried.take()
                } else {
                    None
                };
                let db = db.clone();
                async move {
                    if let Some(failed) = failed {
                        let _ = failed.send(());
                        return Err(crate::StateError::Database(
                            "injected transient read failure".into(),
                        ));
                    }
                    let _ = retried.unwrap().send(());
                    crate::post_processing::events::wait_for_job_events_stopped(&db, job_id.0).await
                }
            })
            .await
            .unwrap();
        });
        failure_seen.await.unwrap();
        assert!(!cleanup.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        retry_seen.await.unwrap();
        assert!(!cleanup.is_finished());
        drop(active);
        cleanup.await.unwrap();
    }
}
