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

    // One history-store operation for the batch, then one target update per
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
        let result = self
            .handle
            .delete_history_batch(job_ids, delete_files)
            .await;

        // Targets grouped by the state and message they end in.
        let mut settled: BTreeMap<(bool, Option<String>), Vec<u64>> = BTreeMap::new();
        match result {
            Ok(outcome) => {
                for (job_id, deleted) in &outcome.deleted {
                    // The record is gone either way; a directory whose marker
                    // stopped matching it was left on disk, and the target
                    // says which rather than claiming a clean delete.
                    settled
                        .entry((true, left_in_place_note(deleted)))
                        .or_default()
                        .push(job_id.0);
                }
                for (job_id, error) in &outcome.refused {
                    let entry = match error {
                        SchedulerError::JobNotFound(_) => (true, None),
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

        let mut removed = Vec::new();
        for ((completed, message), ids) in settled {
            let state = if completed {
                removed.extend_from_slice(&ids);
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
    use weaver_server_core::{JobHistoryRow, JobId};

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

    async fn manager_with_idle_pipeline(
        db: &Database,
    ) -> (
        HistoryDeleteManager,
        weaver_server_core::Pipeline,
        tempfile::TempDir,
    ) {
        let root = tempfile::tempdir().unwrap();
        let mut settings = db.load_config().unwrap();
        settings.data_dir = root.path().display().to_string();
        settings.intermediate_dir = Some(root.path().join("intermediate").display().to_string());
        settings.complete_dir = Some(root.path().join("complete").display().to_string());
        let config = Arc::new(tokio::sync::RwLock::new(settings));
        let (pipeline, handle, _) =
            weaver_server_core::pipeline::test_support::idle_pipeline(db.clone(), config).await;
        (
            HistoryDeleteManager::new(db.clone(), handle, QueueEventReplay::new(1)),
            pipeline,
            root,
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
        let (manager, _pipeline, _root) = manager_with_idle_pipeline(&db).await;
        manager
            .handle
            .add_job(
                JobId(12),
                weaver_server_core::JobSpec {
                    name: "Active fixture".into(),
                    password: None,
                    category: None,
                    metadata: vec![],
                    files: vec![],
                    total_bytes: 0,
                },
                std::path::PathBuf::from("fixture.nzb"),
                vec![],
            )
            .await
            .unwrap();
        db.delete_job_history(13).unwrap();
        manager.process_operation(operation).await.unwrap();

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
    async fn a_large_delete_finishes_all_targets_without_a_pipeline_turn() {
        let db = Database::open_in_memory().unwrap();
        let ids: Vec<u64> = (1..=(HISTORY_DELETE_BATCH_SIZE as u64 + 1)).collect();
        for id in &ids {
            db.insert_job_history(&history(*id)).unwrap();
        }
        db.insert_history_delete_operation(&ids, false, false)
            .unwrap();
        let operation = db.next_history_delete_operation().unwrap().unwrap();
        let (manager, _pipeline, _root) = manager_with_idle_pipeline(&db).await;
        manager.process_operation(operation).await.unwrap();

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

        let (manager, _pipeline, _root) = manager_with_idle_pipeline(&db).await;
        let revision = manager.handle.job_revision();
        manager.process_operation(operation).await.unwrap();
        assert_eq!(manager.handle.job_revision(), revision);
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

        weaver_server_core::jobs::working_dir::mark_weaver_owned_output_dir(&output_dir).unwrap();
        let (manager, _pipeline, _root) = manager_with_idle_pipeline(&db).await;
        manager.process_operation(operation).await.unwrap();

        assert!(db.get_job_history(8).unwrap().is_none());
        assert!(!output_dir.exists());
        assert_eq!(
            db.list_history_delete_operations(false).unwrap()[0].state,
            weaver_server_core::AsyncOperationState::Completed
        );
    }

    #[tokio::test]
    async fn deletion_finishes_when_the_pipeline_has_stopped() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&history(9)).unwrap();
        db.insert_history_delete_operation(&[9], false, false)
            .unwrap();
        let operation = db.next_history_delete_operation().unwrap().unwrap();
        let (manager, pipeline, _root) = manager_with_idle_pipeline(&db).await;
        drop(pipeline);
        assert!(manager.handle.is_closed());
        manager.process_operation(operation).await.unwrap();
        assert!(db.get_job_history(9).unwrap().is_none());
        assert_eq!(
            db.list_history_delete_operations(false).unwrap()[0].state,
            weaver_server_core::AsyncOperationState::Completed
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
        let (manager, _pipeline, _root) = manager_with_idle_pipeline(&db).await;
        let mut removed = BTreeMap::new();
        while let Some(operation) = db.next_history_delete_operation().unwrap() {
            assert_eq!(operation.file_delete_authorized, operation.delete_files);
            for target in db
                .list_history_delete_operation_targets(operation.id, operation.delete_files)
                .unwrap()
            {
                removed.insert(target.target_id, operation.delete_files);
            }
            manager.process_operation(operation).await.unwrap();
        }
        assert_eq!(
            removed,
            [(1, false), (2, true), (4, true)].into_iter().collect()
        );
        assert!(db.get_job_history(3).unwrap().is_some());
        assert_eq!(db.list_history_delete_operations(false).unwrap().len(), 2);
    }
}
