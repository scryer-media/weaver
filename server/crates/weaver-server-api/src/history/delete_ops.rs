use std::collections::BTreeSet;
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
    AsyncOperationTargetState, Database, HistoryDeleteOperationInsertError,
    HistoryDeleteOperationRow, SchedulerError, SchedulerHandle,
};

#[derive(Clone)]
pub(crate) struct HistoryDeleteManager {
    db: Database,
    handle: SchedulerHandle,
    replay: QueueEventReplay,
    wake: Arc<Notify>,
}

impl HistoryDeleteManager {
    pub(crate) fn new(db: Database, handle: SchedulerHandle, replay: QueueEventReplay) -> Self {
        Self {
            db,
            handle,
            replay,
            wake: Arc::new(Notify::new()),
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
                    db.insert_history_delete_operation(
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
                    db.insert_all_history_delete_operation(
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
                "file deletion was not authorized; resubmit with an admin-scoped caller";
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

        for target in targets {
            let db = self.db.clone();
            tokio::task::spawn_blocking(move || {
                db.mark_history_delete_target_state(
                    target.operation_id,
                    target.target_id,
                    AsyncOperationTargetState::Running,
                    None,
                )
            })
            .await
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;

            let result = self
                .handle
                .delete_history(
                    weaver_server_core::JobId(target.target_id),
                    target.delete_files,
                )
                .await;

            match result {
                Ok(()) | Err(SchedulerError::JobNotFound(_)) => {
                    let db = self.db.clone();
                    tokio::task::spawn_blocking(move || {
                        db.mark_history_delete_target_state(
                            target.operation_id,
                            target.target_id,
                            AsyncOperationTargetState::Completed,
                            None,
                        )
                    })
                    .await
                    .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
                    .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;

                    self.replay
                        .append(PersistedQueueEvent {
                            occurred_at_ms: chrono::Utc::now().timestamp_millis(),
                            kind: QueueEventKind::ItemRemoved,
                            item_id: Some(target.target_id),
                            item: None,
                            state: None,
                            previous_state: None,
                            attention: None,
                            global_state: None,
                        })
                        .await;
                }
                Err(error) => {
                    let error_message = error.to_string();
                    let db = self.db.clone();
                    tokio::task::spawn_blocking(move || {
                        db.mark_history_delete_target_state(
                            target.operation_id,
                            target.target_id,
                            AsyncOperationTargetState::Failed,
                            Some(&error_message),
                        )
                    })
                    .await
                    .map_err(|join_error| graphql_error("INTERNAL", join_error.to_string()))?
                    .map_err(|db_error| graphql_error("INTERNAL", db_error.to_string()))?;
                }
            }
        }

        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.finalize_history_delete_operation(operation.id))
            .await
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;

        Ok(())
    }
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
    use weaver_server_core::JobHistoryRow;
    use weaver_server_core::jobs::handle::SharedPipelineState;
    use weaver_server_core::operations::metrics::PipelineMetrics;

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
        }
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
            Some("file deletion was not authorized; resubmit with an admin-scoped caller")
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
                job_id,
                delete_files,
                reply,
            } = command
            else {
                panic!("expected history delete command");
            };
            assert!(delete_files);
            let output_dir = db_for_scheduler
                .get_job_history(job_id.0)
                .unwrap()
                .and_then(|row| row.output_dir)
                .expect("recorded output directory");
            std::fs::remove_dir_all(output_dir).unwrap();
            db_for_scheduler.delete_job_history(job_id.0).unwrap();
            reply.send(Ok(())).unwrap();
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
}
