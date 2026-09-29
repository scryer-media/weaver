use crate::bandwidth::PruneFiles;
use crate::{Database, HistoryDeleteOperationInsertError, HistoryFilter};

impl Database {
    /// Manual and scheduled deletion use the same durable worker queue.
    pub fn history_delete_notification(&self) -> std::sync::Arc<tokio::sync::Notify> {
        self.history_delete_wake.clone()
    }

    pub fn accept_history_delete_ids(
        &self,
        ids: &[u64],
        delete_files: bool,
        file_delete_authorized: bool,
    ) -> Result<u64, HistoryDeleteOperationInsertError> {
        let id = self.insert_history_delete_operation(ids, delete_files, file_delete_authorized)?;
        self.history_delete_wake.notify_one();
        Ok(id)
    }

    pub fn accept_all_history_delete(
        &self,
        delete_files: bool,
        file_delete_authorized: bool,
    ) -> Result<(u64, Vec<u64>), HistoryDeleteOperationInsertError> {
        let acceptance =
            self.insert_all_history_delete_operation(delete_files, file_delete_authorized)?;
        self.history_delete_wake.notify_one();
        Ok(acceptance)
    }

    pub async fn prune_history(
        &self,
        failed: Option<PruneFiles>,
        completed: Option<PruneFiles>,
        cancelled: Option<PruneFiles>,
    ) -> Result<(), String> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut errors = Vec::new();
            for delete_files in [false, true] {
                let statuses: Vec<_> = [
                    ("failed", failed),
                    ("complete", completed),
                    ("cancelled", cancelled),
                ]
                .into_iter()
                .filter_map(|(status, policy)| {
                    policy
                        .filter(|policy| policy.delete_files == delete_files)
                        .map(|_| status.to_owned())
                })
                .collect();
                if statuses.is_empty() {
                    continue;
                }
                let rows = match db.list_job_history(&HistoryFilter {
                    statuses: Some(statuses),
                    ..Default::default()
                }) {
                    Ok(rows) => rows,
                    Err(error) => {
                        errors.push(error.to_string());
                        continue;
                    }
                };
                let ids: Vec<_> = rows.into_iter().map(|row| row.job_id).collect();
                if !ids.is_empty() {
                    match db.accept_history_delete_ids(&ids, delete_files, delete_files) {
                        Ok(_) => {}
                        Err(
                            HistoryDeleteOperationInsertError::MissingRows
                            | HistoryDeleteOperationInsertError::LockedTargets,
                        ) => {
                            // A concurrent manual delete must not exclude unrelated rows.
                            // Each acceptance rechecks existence and locks transactionally.
                            for id in ids {
                                match db.accept_history_delete_ids(
                                    &[id],
                                    delete_files,
                                    delete_files,
                                ) {
                                    Ok(_)
                                    | Err(
                                        HistoryDeleteOperationInsertError::MissingRows
                                        | HistoryDeleteOperationInsertError::LockedTargets,
                                    ) => {}
                                    Err(error) => errors.push(error.to_string()),
                                }
                            }
                        }
                        Err(error) => errors.push(error.to_string()),
                    }
                }
            }
            if errors.is_empty() {
                Ok(())
            } else {
                Err(errors.join("; "))
            }
        })
        .await
        .map_err(|error| error.to_string())?
    }
}
