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
                    db.insert_history_delete_operation(&ids_for_insert, input.delete_files)
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
                    db.insert_all_history_delete_operation(input.delete_files)
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
