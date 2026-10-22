// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_graphql::Result;
use tokio::sync::Notify;

use crate::auth::graphql_error;
use crate::history::types::{
    AcceptHistoryDeleteInput, AcceptHistoryDeleteMode, HistoryDeleteAcceptance,
};
use crate::jobs::replay::QueueEventReplay;
use crate::jobs::types::{PersistedQueueEvent, QueueEventKind};
use weaver_server_core::{
    AsyncOperationTargetState, Database, HISTORY_DELETE_BATCH_SIZE,
    HistoryDeleteOperationInsertError, HistoryDeleteOperationRow, SchedulerError, SchedulerHandle,
};

// How long one batch of targets waits for the pipeline to answer its history
// delete.
//
// The pipeline actor answers deletes between its other work, so a slow
// answer is normal; no answer at all is not. Without a bound, a pipeline
// that never replies leaves the whole operation claimed forever and every
// target in it showing as in progress. Past this the batch is failed as
// unanswered and the operation finalises, so the rows can be retried. The
// pipeline may still carry the delete out after the deadline; the error
// text says so, and a retry settles which way it went.
const HISTORY_DELETE_REPLY_DEADLINE: Duration = Duration::from_secs(300);

const PIPELINE_DID_NOT_ANSWER: &str = "pipeline did not answer the history delete within the deadline; the delete may still finish, retry to confirm";

#[derive(Clone)]
pub(crate) struct HistoryDeleteManager {
    db: Database,
    handle: SchedulerHandle,
    replay: QueueEventReplay,
    wake: Arc<Notify>,
}

impl HistoryDeleteManager {
    pub(crate) fn new(db: Database, handle: SchedulerHandle, replay: QueueEventReplay) -> Self {
        let wake = db.history_delete_notification();
        Self {
            db,
            handle,
            replay,
            wake,
        }
    }

    pub(crate) fn spawn_worker(&self) {
        let this = self.clone();
        tokio::spawn(async move {
            this.run_worker().await;
        });
    }

    pub(crate) async fn accept_history_delete(
        &self,
        input: AcceptHistoryDeleteInput,
        file_delete_authorized: bool,
    ) -> Result<HistoryDeleteAcceptance> {
        let acceptance = match input.mode {
            AcceptHistoryDeleteMode::Ids => {
                let ids = dedupe_ids(&input.ids);
                if ids.is_empty() {
                    return Err(graphql_error(
                        "INVALID_INPUT",
                        "history delete requires at least one id",
                    ));
                }

                let db = self.db.clone();
                let ids_for_insert = ids.clone();
                let operation_id = tokio::task::spawn_blocking(move || {
                    db.accept_history_delete_ids(
                        &ids_for_insert,
                        input.delete_files,
                        file_delete_authorized,
                    )
                })
                .await
                .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
                .map_err(map_insert_error)?;

                HistoryDeleteAcceptance {
                    operation_id,
                    accepted_ids: ids.clone(),
                    total_targets: ids.len() as u32,
                }
            }
            AcceptHistoryDeleteMode::AllHistory => {
                let db = self.db.clone();
                let (operation_id, ids) = tokio::task::spawn_blocking(move || {
                    db.accept_all_history_delete(input.delete_files, file_delete_authorized)
                })
                .await
                .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
                .map_err(map_insert_error)?;

                HistoryDeleteAcceptance {
                    operation_id,
                    accepted_ids: ids.clone(),
                    total_targets: ids.len() as u32,
                }
            }
        };

        self.wake.notify_one();
        Ok(acceptance)
    }

    async fn run_worker(self) {
        let db = self.db.clone();
        if let Err(error) =
            tokio::task::spawn_blocking(move || db.recover_running_history_delete_operations())
                .await
                .map_err(|join_error| graphql_error("INTERNAL", join_error.to_string()))
                .and_then(|result| {
                    result.map_err(|db_error| graphql_error("INTERNAL", db_error.to_string()))
                })
        {
            tracing::error!(error = ?error, "failed to recover background history deletes");
        }

        loop {
            let Some(operation) = self.next_operation().await else {
                self.wake.notified().await;
                continue;
            };

            if let Err(error) = self.process_operation(operation).await {
                tracing::error!(
                    error = ?error,
                    operation_id = operation.id,
                    "history delete worker iteration failed"
                );
                if let Err(requeue_error) = self.requeue_operation(operation.id).await {
                    tracing::error!(
                        error = ?requeue_error,
                        operation_id = operation.id,
                        "failed to requeue history delete operation after worker error"
                    );
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }

    async fn next_operation(&self) -> Option<HistoryDeleteOperationRow> {
        let db = self.db.clone();
        match tokio::task::spawn_blocking(move || db.next_history_delete_operation()).await {
            Ok(Ok(operation)) => operation,
            Ok(Err(error)) => {
                tracing::error!(error = %error, "failed to load next history delete operation");
                None
            }
            Err(error) => {
                tracing::error!(error = %error, "failed to join history delete operation lookup");
                None
            }
        }
    }

    async fn requeue_operation(&self, operation_id: u64) -> Result<()> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.requeue_history_delete_operation(operation_id))
            .await
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))
    }

    async fn process_operation(&self, operation: HistoryDeleteOperationRow) -> Result<()> {
        if operation.delete_files && !operation.file_delete_authorized {
            const RESUBMIT_MESSAGE: &str =
                "file deletion was not authorized; resubmit with integration (control) scope";
            let db = self.db.clone();
            tokio::task::spawn_blocking(move || {
                db.fail_pending_history_delete_operation_targets(operation.id, RESUBMIT_MESSAGE)
            })
            .await
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;

            let db = self.db.clone();
            tokio::task::spawn_blocking(move || db.finalize_history_delete_operation(operation.id))
                .await
                .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
                .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;
            return Ok(());
        }

        let db = self.db.clone();
        let targets = tokio::task::spawn_blocking(move || {
            db.list_history_delete_operation_targets(operation.id, operation.delete_files)
        })
        .await
        .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
        .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;

        let target_ids = targets
            .iter()
            .map(|target| target.target_id)
            .collect::<Vec<_>>();
        self.mark_targets(
            operation.id,
            target_ids.clone(),
            AsyncOperationTargetState::Running,
            None,
        )
        .await?;

        for batch in target_ids.chunks(HISTORY_DELETE_BATCH_SIZE) {
            self.process_batch(operation.id, operation.delete_files, batch)
                .await?;
        }

        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.finalize_history_delete_operation(operation.id))
            .await
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;

        Ok(())
    }

    // One pipeline command for the batch, then one target update per
    // distinct result: the clean deletes together, each failure message
    // together.
    async fn process_batch(
        &self,
        operation_id: u64,
        delete_files: bool,
        batch: &[u64],
    ) -> Result<()> {
        let job_ids = batch
            .iter()
            .copied()
            .map(weaver_server_core::JobId)
            .collect::<BTreeSet<_>>();
        let result = tokio::time::timeout(
            HISTORY_DELETE_REPLY_DEADLINE,
            self.handle.delete_history_batch(job_ids, delete_files),
        )
        .await
        .unwrap_or_else(|_| {
            tracing::warn!(
                operation_id,
                targets = batch.len(),
                deadline_secs = HISTORY_DELETE_REPLY_DEADLINE.as_secs(),
                "pipeline did not answer the history delete in time; failing the batch, the delete may still finish"
            );
            Err(SchedulerError::Internal(PIPELINE_DID_NOT_ANSWER.to_string()))
        });

        // Targets grouped by the state and message they end in, and every id
        // whose record is gone, which is announced as removed whatever state
        // its target ends in.
        let mut settled: BTreeMap<(bool, Option<String>), Vec<u64>> = BTreeMap::new();
        let mut removed = Vec::new();
        match result {
            Ok(outcome) => {
                for (job_id, deleted) in &outcome.deleted {
                    removed.push(job_id.0);
                    // The record is gone either way. A directory whose marker
                    // stopped matching it was left on disk, and the target
                    // says which rather than claiming a clean delete; a
                    // cleanup that failed fails the target with its error.
                    let entry = match deleted {
                        Ok(deleted) => (true, left_in_place_note(deleted)),
                        Err(error) => (
                            false,
                            Some(format!("record deleted; cleanup did not finish: {error}")),
                        ),
                    };
                    settled.entry(entry).or_default().push(job_id.0);
                }
                for (job_id, error) in &outcome.refused {
                    let entry = match error {
                        SchedulerError::JobNotFound(_) => {
                            removed.push(job_id.0);
                            (true, None)
                        }
                        error => (false, Some(error.to_string())),
                    };
                    settled.entry(entry).or_default().push(job_id.0);
                }
            }
            Err(error) => {
                settled
                    .entry((false, Some(error.to_string())))
                    .or_default()
                    .extend_from_slice(batch);
            }
        }

        for ((completed, message), ids) in settled {
            let state = if completed {
                AsyncOperationTargetState::Completed
            } else {
                AsyncOperationTargetState::Failed
            };
            self.mark_targets(operation_id, ids, state, message).await?;
        }
        removed.sort_unstable();
        for id in removed {
            self.replay
                .append(PersistedQueueEvent {
                    occurred_at_ms: chrono::Utc::now().timestamp_millis(),
                    kind: QueueEventKind::ItemRemoved,
                    item_id: Some(id),
                    item: None,
                    state: None,
                    previous_state: None,
                    attention: None,
                    global_state: None,
                })
                .await;
        }
        Ok(())
    }

    async fn mark_targets(
        &self,
        operation_id: u64,
        target_ids: Vec<u64>,
        state: AsyncOperationTargetState,
        message: Option<String>,
    ) -> Result<()> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.mark_history_delete_targets_state(
                operation_id,
                &target_ids,
                state,
                message.as_deref(),
            )
        })
        .await
        .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
        .map_err(|error| graphql_error("INTERNAL", error.to_string()))
    }
}

fn left_in_place_note(outcome: &weaver_server_core::HistoryDeleteOutcome) -> Option<String> {
    if outcome.left_in_place.is_empty() {
        return None;
    }
    let dirs: Vec<String> = outcome
        .left_in_place
        .iter()
        .map(|dir| dir.display().to_string())
        .collect();
    Some(format!(
        "record deleted; working directory left in place: {}",
        dirs.join(", ")
    ))
}

fn dedupe_ids(ids: &[u64]) -> Vec<u64> {
    let mut seen = BTreeSet::new();
    let mut ordered = Vec::with_capacity(ids.len());
    for &id in ids {
        if seen.insert(id) {
            ordered.push(id);
        }
    }
    ordered
}

fn map_insert_error(error: HistoryDeleteOperationInsertError) -> async_graphql::Error {
    match error {
        HistoryDeleteOperationInsertError::EmptyTargets => {
            graphql_error("INVALID_INPUT", error.to_string())
        }
        HistoryDeleteOperationInsertError::MissingRows
        | HistoryDeleteOperationInsertError::LockedTargets
        | HistoryDeleteOperationInsertError::NoHistoryRows => {
            graphql_error("CONFLICT", error.to_string())
        }
        HistoryDeleteOperationInsertError::State(state_error) => {
            graphql_error("INTERNAL", state_error.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::{broadcast, mpsc};
    use weaver_server_core::jobs::handle::SharedPipelineState;
    use weaver_server_core::operations::metrics::PipelineMetrics;
    use weaver_server_core::{HistoryDeleteBatchOutcome, JobHistoryRow, JobId};

    fn history(job_id: u64) -> JobHistoryRow {
        JobHistoryRow {
            job_id,
            job_hash: None,
            name: format!("job-{job_id}"),
            status: "complete".to_string(),
            error_message: None,
            total_bytes: 1,
            downloaded_bytes: 1,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: None,
            output_dir: None,
            nzb_path: None,
            created_at: 1,
            completed_at: 2,
            metadata: None,
            server_attribution: None,
        }
    }

    fn deleted_all(job_ids: &BTreeSet<JobId>) -> HistoryDeleteBatchOutcome {
        HistoryDeleteBatchOutcome {
            deleted: job_ids
                .iter()
                .map(|job_id| (*job_id, Ok(Default::default())))
                .collect(),
            refused: BTreeMap::new(),
        }
    }

    fn manager_with_channel(
        db: &Database,
        capacity: usize,
    ) -> (
        HistoryDeleteManager,
        mpsc::Receiver<weaver_server_core::SchedulerCommand>,
    ) {
        let (command_tx, command_rx) = mpsc::channel(capacity);
        let (event_tx, _) = broadcast::channel(1);
        let handle = SchedulerHandle::new(
            command_tx,
            event_tx,
            SharedPipelineState::new(PipelineMetrics::new(), vec![]),
        );
        (
            HistoryDeleteManager::new(db.clone(), handle, QueueEventReplay::new(1)),
            command_rx,
        )
    }

    #[tokio::test]
    async fn a_refused_id_fails_alone_and_the_rest_of_its_batch_completes() {
        let db = Database::open_in_memory().unwrap();
        for id in [11, 12, 13] {
            db.insert_job_history(&history(id)).unwrap();
        }
        db.insert_history_delete_operation(&[11, 12, 13], false, false)
            .unwrap();
        let operation = db.next_history_delete_operation().unwrap().unwrap();
        let (manager, mut commands) = manager_with_channel(&db, 1);
        let worker_db = db.clone();
        let scheduler = tokio::spawn(async move {
            let weaver_server_core::SchedulerCommand::DeleteHistory { job_ids, reply, .. } =
                commands.recv().await.unwrap()
            else {
                panic!("expected history delete command");
            };
            assert_eq!(
                job_ids,
                [JobId(11), JobId(12), JobId(13)].into_iter().collect()
            );
            // 12 is live: the pipeline refuses it. 13 is already gone, which
            // counts as deleted.
            worker_db.delete_job_history(11).unwrap();
            worker_db.delete_job_history(13).unwrap();
            let mut outcome = HistoryDeleteBatchOutcome::default();
            outcome.deleted.insert(JobId(11), Ok(Default::default()));
            outcome.refused.insert(
                JobId(12),
                SchedulerError::Conflict("job 12 is active".into()),
            );
            outcome
                .refused
                .insert(JobId(13), SchedulerError::JobNotFound(JobId(13)));
            reply.send(Ok(outcome)).unwrap();
            commands
        });

        manager.process_operation(operation).await.unwrap();
        let mut commands = scheduler.await.unwrap();

        assert!(commands.try_recv().is_err(), "one batch, one command");
        // Completed targets drop out of the row states; only the refusal stays.
        let state = db.list_history_delete_row_states(&[11, 12, 13]).unwrap();
        assert!(!state.contains_key(&11));
        assert!(!state.contains_key(&13));
        assert_eq!(state[&12].state, AsyncOperationTargetState::Failed);
        assert!(
            state[&12]
                .error_message
                .as_deref()
                .unwrap()
                .contains("active")
        );
        assert!(db.get_job_history(12).unwrap().is_some());
    }

    #[tokio::test]
    async fn a_record_deleted_before_its_cleanup_failed_is_announced_and_fails_its_target() {
        let db = Database::open_in_memory().unwrap();
        for id in [21, 22] {
            db.insert_job_history(&history(id)).unwrap();
        }
        let operation_id = db
            .insert_history_delete_operation(&[21, 22], false, false)
            .unwrap();
        let operation = db.next_history_delete_operation().unwrap().unwrap();
        let (command_tx, mut commands) = mpsc::channel(1);
        let (event_tx, _) = broadcast::channel(1);
        let handle = SchedulerHandle::new(
            command_tx,
            event_tx,
            SharedPipelineState::new(PipelineMetrics::new(), vec![]),
        );
        let replay = QueueEventReplay::new(8);
        let manager = HistoryDeleteManager::new(db.clone(), handle, replay.clone());
        let worker_db = db.clone();
        let scheduler = tokio::spawn(async move {
            let weaver_server_core::SchedulerCommand::DeleteHistory { job_ids, reply, .. } =
                commands.recv().await.unwrap()
            else {
                panic!("expected history delete command");
            };
            // Both records go in the delete transaction; 21's directory then
            // fails to come off disk.
            let ids: Vec<JobId> = job_ids.iter().copied().collect();
            worker_db
                .delete_job_histories_and_forget_duplicate_identities(&ids)
                .unwrap();
            let mut outcome = HistoryDeleteBatchOutcome::default();
            outcome.deleted.insert(
                JobId(21),
                Err(SchedulerError::Io(std::io::Error::other(
                    "synthetic directory removal failure",
                ))),
            );
            outcome.deleted.insert(JobId(22), Ok(Default::default()));
            reply.send(Ok(outcome)).unwrap();
        });

        manager.process_operation(operation).await.unwrap();
        scheduler.await.unwrap();

        let removed: Vec<u64> = replay
            .replay_after(None)
            .await
            .unwrap()
            .into_iter()
            .filter(|notification| notification.event.kind == QueueEventKind::ItemRemoved)
            .filter_map(|notification| notification.event.item_id)
            .collect();
        assert_eq!(
            removed,
            vec![21, 22],
            "a record that is gone is announced removed, whatever its cleanup did"
        );
        let state = db.list_history_delete_row_states(&[21, 22]).unwrap();
        assert!(!state.contains_key(&22));
        assert_eq!(state[&21].state, AsyncOperationTargetState::Failed);
        let message = state[&21].error_message.as_deref().unwrap();
        assert!(message.contains("record deleted"), "{message}");
        assert!(
            message.contains("synthetic directory removal failure"),
            "{message}"
        );
        let summaries = db.list_history_delete_operations(false).unwrap();
        assert_eq!(summaries[0].id, operation_id);
        assert_eq!(
            summaries[0].state,
            weaver_server_core::AsyncOperationState::CompletedWithErrors
        );
    }

    #[tokio::test]
    async fn a_large_delete_reaches_the_scheduler_as_bounded_batches() {
        let db = Database::open_in_memory().unwrap();
        let ids: Vec<u64> = (1..=(HISTORY_DELETE_BATCH_SIZE as u64 + 1)).collect();
        for id in &ids {
            db.insert_job_history(&history(*id)).unwrap();
        }
        db.insert_history_delete_operation(&ids, false, false)
            .unwrap();
        let operation = db.next_history_delete_operation().unwrap().unwrap();
        let (manager, mut commands) = manager_with_channel(&db, 2);
        let worker_db = db.clone();
        let scheduler = tokio::spawn(async move {
            let mut sizes = Vec::new();
            for _ in 0..2 {
                let weaver_server_core::SchedulerCommand::DeleteHistory { job_ids, reply, .. } =
                    commands.recv().await.unwrap()
                else {
                    panic!("expected history delete command");
                };
                sizes.push(job_ids.len());
                let ids: Vec<JobId> = job_ids.iter().copied().collect();
                worker_db
                    .delete_job_histories_and_forget_duplicate_identities(&ids)
                    .unwrap();
                reply.send(Ok(deleted_all(&job_ids))).unwrap();
            }
            (sizes, commands)
        });

        manager.process_operation(operation).await.unwrap();
        let (sizes, mut commands) = scheduler.await.unwrap();

        assert_eq!(sizes, vec![HISTORY_DELETE_BATCH_SIZE, 1]);
        assert!(commands.try_recv().is_err());
        assert!(db.list_history_delete_row_states(&ids).unwrap().is_empty());
        assert_eq!(
            db.list_history_delete_operations(false).unwrap()[0].state,
            weaver_server_core::AsyncOperationState::Completed
        );
    }

    #[tokio::test]
    async fn unproven_file_delete_never_reaches_the_scheduler() {
        let db = Database::open_in_memory().unwrap();
        let output_root = tempfile::tempdir().unwrap();
        let output_dir = output_root.path().join("legacy-output");
        std::fs::create_dir(&output_dir).unwrap();
        std::fs::write(output_dir.join("payload.bin"), b"retain me").unwrap();
        let mut row = history(7);
        row.output_dir = Some(output_dir.to_string_lossy().to_string());
        db.insert_job_history(&row).unwrap();
        let operation_id = db
            .insert_history_delete_operation(&[7], true, false)
            .unwrap();
        let _claimed_before_restart = db.next_history_delete_operation().unwrap().unwrap();
        db.recover_running_history_delete_operations().unwrap();
        let operation = db.next_history_delete_operation().unwrap().unwrap();

        let (command_tx, mut command_rx) = mpsc::channel(1);
        let (event_tx, _) = broadcast::channel(1);
        let handle = SchedulerHandle::new(
            command_tx,
            event_tx,
            SharedPipelineState::new(PipelineMetrics::new(), vec![]),
        );
        let manager = HistoryDeleteManager::new(db.clone(), handle, QueueEventReplay::new(1));

        manager.process_operation(operation).await.unwrap();

        assert!(matches!(
            command_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(db.get_job_history(7).unwrap().is_some());
        assert!(output_dir.join("payload.bin").is_file());
        let state = db.list_history_delete_row_states(&[7]).unwrap();
        assert_eq!(state[&7].state, AsyncOperationTargetState::Failed);
        assert_eq!(
            state[&7].error_message.as_deref(),
            Some("file deletion was not authorized; resubmit with integration (control) scope")
        );
        let summaries = db.list_history_delete_operations(false).unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].id, operation_id);
        assert_eq!(
            summaries[0].state,
            weaver_server_core::AsyncOperationState::CompletedWithErrors
        );
    }

    #[tokio::test]
    async fn authorized_file_delete_survives_recovery_and_removes_its_output() {
        let db = Database::open_in_memory().unwrap();
        let output_root = tempfile::tempdir().unwrap();
        let output_dir = output_root.path().join("authorized-output");
        std::fs::create_dir(&output_dir).unwrap();
        std::fs::write(output_dir.join("payload.bin"), b"delete me").unwrap();
        let mut row = history(8);
        row.output_dir = Some(output_dir.to_string_lossy().to_string());
        db.insert_job_history(&row).unwrap();
        db.insert_history_delete_operation(&[8], true, true)
            .unwrap();
        let _claimed_before_restart = db.next_history_delete_operation().unwrap().unwrap();
        db.recover_running_history_delete_operations().unwrap();
        let operation = db.next_history_delete_operation().unwrap().unwrap();
        assert!(operation.file_delete_authorized);

        let (command_tx, mut command_rx) = mpsc::channel(1);
        let (event_tx, _) = broadcast::channel(1);
        let handle = SchedulerHandle::new(
            command_tx,
            event_tx,
            SharedPipelineState::new(PipelineMetrics::new(), vec![]),
        );
        let db_for_scheduler = db.clone();
        let scheduler = tokio::spawn(async move {
            let command = command_rx.recv().await.expect("expected history delete");
            let weaver_server_core::SchedulerCommand::DeleteHistory {
                job_ids,
                delete_files,
                reply,
            } = command
            else {
                panic!("expected history delete command");
            };
            assert!(delete_files);
            assert_eq!(job_ids.len(), 1);
            let job_id = *job_ids.first().unwrap();
            let output_dir = db_for_scheduler
                .get_job_history(job_id.0)
                .unwrap()
                .and_then(|row| row.output_dir)
                .expect("recorded output directory");
            std::fs::remove_dir_all(output_dir).unwrap();
            db_for_scheduler.delete_job_history(job_id.0).unwrap();
            reply.send(Ok(deleted_all(&job_ids))).unwrap();
        });
        let manager = HistoryDeleteManager::new(db.clone(), handle, QueueEventReplay::new(1));

        manager.process_operation(operation).await.unwrap();
        scheduler.await.unwrap();

        assert!(db.get_job_history(8).unwrap().is_none());
        assert!(!output_dir.exists());
        assert_eq!(
            db.list_history_delete_operations(false).unwrap()[0].state,
            weaver_server_core::AsyncOperationState::Completed
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_pipeline_that_never_answers_fails_the_target_and_finalises() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&history(9)).unwrap();
        let operation_id = db
            .insert_history_delete_operation(&[9], false, false)
            .unwrap();
        let operation = db.next_history_delete_operation().unwrap().unwrap();

        // The command is buffered and held, never answered: its reply sender
        // stays alive in `command_rx`, so the only way out is the deadline.
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let (event_tx, _) = broadcast::channel(1);
        let handle = SchedulerHandle::new(
            command_tx,
            event_tx,
            SharedPipelineState::new(PipelineMetrics::new(), vec![]),
        );
        let manager = HistoryDeleteManager::new(db.clone(), handle, QueueEventReplay::new(1));

        manager.process_operation(operation).await.unwrap();

        let Ok(weaver_server_core::SchedulerCommand::DeleteHistory { job_ids, .. }) =
            command_rx.try_recv()
        else {
            panic!("the delete must have reached the scheduler");
        };
        assert_eq!(job_ids, [JobId(9)].into_iter().collect());
        // The message is the deadline's own, not "channel closed": the target
        // failed because nothing answered, not because the reply was dropped.
        let state = db.list_history_delete_row_states(&[9]).unwrap();
        assert_eq!(state[&9].state, AsyncOperationTargetState::Failed);
        assert_eq!(
            state[&9].error_message.as_deref(),
            Some(PIPELINE_DID_NOT_ANSWER)
        );
        // The row may yet be deleted by the pipeline, so the message must not
        // read as a final refusal.
        assert!(PIPELINE_DID_NOT_ANSWER.contains("may still finish"));
        assert!(PIPELINE_DID_NOT_ANSWER.contains("retry"));
        let summaries = db.list_history_delete_operations(false).unwrap();
        assert_eq!(summaries[0].id, operation_id);
        assert_eq!(
            summaries[0].state,
            weaver_server_core::AsyncOperationState::CompletedWithErrors,
            "an unanswered target must not leave the operation claimed"
        );
    }
    #[tokio::test]
    async fn scheduled_pruning_uses_manual_worker_with_per_status_file_authority() {
        use weaver_server_core::bandwidth::PruneFiles;
        let db = Database::open_in_memory().unwrap();
        for (id, status) in [
            (1, "complete"),
            (2, "failed"),
            (3, "cancelled"),
            (4, "failed"),
        ] {
            let mut row = history(id);
            row.status = status.into();
            db.insert_job_history(&row).unwrap();
        }
        let notification = db.history_delete_notification();
        db.prune_history(
            Some(PruneFiles { delete_files: true }),
            Some(PruneFiles {
                delete_files: false,
            }),
            None,
        )
        .await
        .unwrap();
        notification.notified().await;
        let (commands, mut received) = mpsc::channel(3);
        let (events, _) = broadcast::channel(1);
        let handle = SchedulerHandle::new(
            commands,
            events,
            SharedPipelineState::new(PipelineMetrics::new(), vec![]),
        );
        let worker_db = db.clone();
        let pipeline = tokio::spawn(async move {
            let mut removed = std::collections::BTreeMap::new();
            // One batch per prune operation: the completed rows and the
            // failed rows carry different file authority.
            for _ in 0..2 {
                let weaver_server_core::SchedulerCommand::DeleteHistory {
                    job_ids,
                    delete_files,
                    reply,
                } = received.recv().await.unwrap()
                else {
                    panic!("expected manual delete path");
                };
                for job_id in &job_ids {
                    removed.insert(job_id.0, delete_files);
                    worker_db.delete_job_history(job_id.0).unwrap();
                }
                reply.send(Ok(deleted_all(&job_ids))).unwrap();
            }
            removed
        });
        let manager = HistoryDeleteManager::new(db.clone(), handle, QueueEventReplay::new(1));
        while let Some(operation) = db.next_history_delete_operation().unwrap() {
            assert_eq!(operation.file_delete_authorized, operation.delete_files);
            manager.process_operation(operation).await.unwrap();
        }
        assert_eq!(
            pipeline.await.unwrap(),
            [(1, false), (2, true), (4, true)].into_iter().collect()
        );
        assert!(db.get_job_history(3).unwrap().is_some());
        assert_eq!(db.list_history_delete_operations(false).unwrap().len(), 2);
    }
}
