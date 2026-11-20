// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::bandwidth::PruneFiles;
use crate::history::queries::HistoryInternalQueries;
use crate::operations::async_ops::OperationsAsyncOpsInternalQueries;
use crate::operations::async_ops::OperationsAsyncOpsQueries;
use crate::{Database, HistoryDeleteOperationInsertError};

pub trait HistoryDeleteQueueQueries {
    fn history_delete_notification(&self) -> std::sync::Arc<tokio::sync::Notify>;
    fn accept_history_delete_ids(
        &self,
        ids: &[u64],
        delete_files: bool,
        file_delete_authorized: bool,
    ) -> Result<u64, HistoryDeleteOperationInsertError>;
    fn accept_all_history_delete(
        &self,
        delete_files: bool,
        file_delete_authorized: bool,
    ) -> Result<(u64, Vec<u64>), HistoryDeleteOperationInsertError>;
    fn prune_history(
        &self,
        failed: Option<PruneFiles>,
        completed: Option<PruneFiles>,
        cancelled: Option<PruneFiles>,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;
    fn prune_history_by_age(&self, days: u32, now: i64) -> Result<usize, crate::StateError>;
}

impl HistoryDeleteQueueQueries for Database {
    // Manual and scheduled deletion use the same durable worker queue.
    fn history_delete_notification(&self) -> std::sync::Arc<tokio::sync::Notify> {
        self.history_delete_wake.clone()
    }

    fn accept_history_delete_ids(
        &self,
        ids: &[u64],
        delete_files: bool,
        file_delete_authorized: bool,
    ) -> Result<u64, HistoryDeleteOperationInsertError> {
        let id = self.insert_history_delete_operation(ids, delete_files, file_delete_authorized)?;
        self.history_delete_wake.notify_one();
        Ok(id)
    }

    fn accept_all_history_delete(
        &self,
        delete_files: bool,
        file_delete_authorized: bool,
    ) -> Result<(u64, Vec<u64>), HistoryDeleteOperationInsertError> {
        let acceptance =
            self.insert_all_history_delete_operation(delete_files, file_delete_authorized)?;
        self.history_delete_wake.notify_one();
        Ok(acceptance)
    }

    async fn prune_history(
        &self,
        failed: Option<PruneFiles>,
        completed: Option<PruneFiles>,
        cancelled: Option<PruneFiles>,
    ) -> Result<(), String> {
        let db = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut errors = Vec::new();
            for delete_files in [false, true] {
                let statuses: Vec<&str> = [
                    ("failed", failed),
                    // A partial delivery is pruned as the failure clients see.
                    ("partial", failed),
                    ("complete", completed),
                    ("cancelled", cancelled),
                ]
                .into_iter()
                .filter_map(|(status, policy)| {
                    policy
                        .filter(|policy| policy.delete_files == delete_files)
                        .map(|_| status)
                })
                .collect();
                if statuses.is_empty() {
                    continue;
                }
                if let Err(error) =
                    db.prune_history_statuses_paged(&statuses, delete_files, HISTORY_RETENTION_PAGE)
                {
                    errors.push(error.to_string());
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

    // Age retention: every history entry, of any status and hidden ones too,
    // whose job finished `days` or more days before `now` (epoch seconds) is
    // handed to the durable delete queue without its files. Delivered output
    // stays on disk. `days` 0 keeps history forever and reads nothing.
    //
    // The entries are read as ids only, a page at a time, oldest first. The
    // cursor always moves past a page, so an entry another operation holds
    // is passed over this pass and not read again. An entry an earlier
    // delete refused is retried on every pass by queueing that delete
    // again, never by adding an operation per pass. Returns how many pages
    // queued or retried anything; nothing due, or nothing but held entries,
    // writes nothing and wakes nothing.
    fn prune_history_by_age(&self, days: u32, now: i64) -> Result<usize, crate::StateError> {
        self.prune_history_by_age_paged(days, now, HISTORY_RETENTION_PAGE)
    }
}

pub(crate) trait HistoryDeleteQueueInternalQueries {
    fn prune_history_statuses_paged(
        &self,
        statuses: &[&str],
        delete_files: bool,
        page: usize,
    ) -> Result<usize, crate::StateError>;
    fn prune_history_by_age_paged(
        &self,
        days: u32,
        now: i64,
        page: usize,
    ) -> Result<usize, crate::StateError>;
}

impl HistoryDeleteQueueInternalQueries for Database {
    // Status retention: every history entry, hidden ones too, whose status
    // is one of `statuses`, is handed to the durable delete queue with or
    // without its files, read as ids a page at a time, newest first, one
    // status after another. Each page is queued as it is read, as age
    // retention queues its pages. Returns how many pages queued or retried
    // anything.
    fn prune_history_statuses_paged(
        &self,
        statuses: &[&str],
        delete_files: bool,
        page: usize,
    ) -> Result<usize, crate::StateError> {
        let mut queued = 0;
        for status in statuses {
            let mut after = None;
            loop {
                let rows = self.job_history_ids_by_status_page(status, after, page)?;
                let Some(&last) = rows.last() else {
                    break;
                };
                after = Some(last);
                let ids = rows.iter().map(|(_, job_id)| *job_id).collect::<Vec<_>>();
                if self.queue_retention_history_deletes(&ids, delete_files)? {
                    queued += 1;
                    self.history_delete_wake.notify_one();
                }
                if rows.len() < page {
                    break;
                }
            }
        }
        Ok(queued)
    }

    fn prune_history_by_age_paged(
        &self,
        days: u32,
        now: i64,
        page: usize,
    ) -> Result<usize, crate::StateError> {
        if days == 0 {
            return Ok(0);
        }
        let cutoff = now.saturating_sub(i64::from(days) * 86_400);
        let mut after = None;
        let mut queued = 0;
        loop {
            let rows = self.list_history_completed_by(cutoff, after, page)?;
            let Some(&last) = rows.last() else {
                return Ok(queued);
            };
            after = Some(last);
            let ids = rows.iter().map(|(_, job_id)| *job_id).collect::<Vec<_>>();
            if self.queue_retention_history_deletes(&ids, false)? {
                queued += 1;
                self.history_delete_wake.notify_one();
            }
            if rows.len() < page {
                return Ok(queued);
            }
        }
    }
}

// How many history ids one retention page reads and queues as one delete
// operation.
const HISTORY_RETENTION_PAGE: usize = 1_000;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::nzbget_state::HistoryNzbgetStateQueries;
    use crate::history::persistence::HistoryPersistenceInsertJobHistoryQueries;
    use crate::history::queries::HistoryUnpagedQueries;
    use crate::operations::async_ops::OperationsAsyncOpsInternalQueries;
    use crate::operations::async_ops::OperationsAsyncOpsQueries;

    const DAY: i64 = 86_400;
    const NOW: i64 = 1_800_000_000;

    fn row(job_id: u64, status: &str, completed_at: i64) -> crate::JobHistoryRow {
        crate::JobHistoryRow {
            job_id,
            job_hash: None,
            name: format!("retention-{job_id}"),
            status: status.to_string(),
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
            created_at: completed_at - 10,
            completed_at,
            metadata: None,
            server_attribution: None,
            archived_at: None,
            protocol: crate::jobs::Protocol::Nzb,
        }
    }

    // Every id queued for deletion, with whether its files go, across every
    // operation.
    fn queued_targets(db: &Database) -> Vec<(u64, bool)> {
        let mut targets = Vec::new();
        for operation in db.list_history_delete_operations(true).unwrap() {
            for target in db
                .list_history_delete_operation_targets(operation.id, operation.delete_files)
                .unwrap()
            {
                targets.push((target.target_id, operation.delete_files));
            }
        }
        targets.sort();
        targets
    }

    // Whether the delete worker's wake was rung and not yet taken.
    async fn woken(db: &Database) -> bool {
        let wake = db.history_delete_notification();
        tokio::select! {
            biased;
            () = wake.notified() => true,
            () = std::future::ready(()) => false,
        }
    }

    // Retention off reads nothing: it answers even with the database closed,
    // where any read fails.
    #[tokio::test]
    async fn zero_days_reads_and_writes_nothing() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&row(1, "complete", 0)).unwrap();
        let closed = db.clone();
        closed.close().unwrap();

        assert_eq!(db.prune_history_by_age(0, NOW).unwrap(), 0);
        assert!(db.prune_history_by_age(1, NOW).is_err());
    }

    #[tokio::test]
    async fn nothing_due_queues_nothing_and_rings_nothing() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&row(1, "complete", NOW - DAY + 1))
            .unwrap();
        db.insert_job_history(&row(2, "failed", NOW)).unwrap();

        assert_eq!(db.prune_history_by_age(1, NOW).unwrap(), 0);

        assert!(db.list_history_delete_operations(false).unwrap().is_empty());
        assert!(!woken(&db).await);
    }

    // Rows due across several pages are each queued once, without their
    // files, whatever their status and hidden ones too; newer rows stay.
    #[tokio::test]
    async fn due_rows_across_pages_are_queued_once_and_newer_rows_stay() {
        let db = Database::open_in_memory().unwrap();
        let cutoff = NOW - 30 * DAY;
        // Two rows share the cutoff second, so the cursor's tie on job id is
        // crossed at a page boundary.
        for (job_id, status, completed_at) in [
            (1, "complete", cutoff - 5 * DAY),
            (2, "failed", cutoff - DAY),
            (3, "cancelled", cutoff),
            (4, "partial", cutoff),
            (5, "complete", cutoff - 2 * DAY),
            (6, "complete", cutoff + 1),
            (7, "failed", NOW),
        ] {
            db.insert_job_history(&row(job_id, status, completed_at))
                .unwrap();
        }
        db.set_nzbget_history_hidden(2, true).unwrap();

        let queued = db.prune_history_by_age_paged(30, NOW, 2).unwrap();

        assert_eq!(queued, 3);
        assert_eq!(
            queued_targets(&db),
            vec![(1, false), (2, false), (3, false), (4, false), (5, false)]
        );
        assert!(woken(&db).await);

        // A second pass finds every due row already queued and adds nothing.
        assert_eq!(db.prune_history_by_age_paged(30, NOW, 2).unwrap(), 0);
        assert_eq!(queued_targets(&db).len(), 5);
    }

    // A row another operation already holds is passed over, and the pass
    // still reaches the rows after it.
    #[tokio::test]
    async fn a_held_row_does_not_stall_the_pass() {
        let db = Database::open_in_memory().unwrap();
        for job_id in 1..=3 {
            db.insert_job_history(&row(job_id, "complete", job_id as i64))
                .unwrap();
        }
        db.insert_available_history_delete_operation(&[1], true)
            .unwrap()
            .unwrap();

        let queued = db.prune_history_by_age_paged(1, NOW, 1).unwrap();

        assert_eq!(queued, 2);
        assert_eq!(queued_targets(&db), vec![(1, true), (2, false), (3, false)]);
    }

    // An entry the delete worker refused is retried by the next pass, which
    // queues the refused delete again rather than adding an operation; a
    // pass while that retry waits writes and rings nothing; other due
    // entries still join a new operation.
    #[tokio::test]
    async fn a_refused_row_is_retried_without_a_new_operation() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&row(1, "complete", 1)).unwrap();
        let first = db.prune_history_by_age_paged(1, NOW, 10).unwrap();
        assert_eq!(first, 1);
        let operation = db.list_history_delete_operations(true).unwrap()[0].id;
        db.mark_history_delete_target_state(
            operation,
            1,
            crate::AsyncOperationTargetState::Failed,
            Some("refused"),
        )
        .unwrap();
        db.finalize_history_delete_operation(operation).unwrap();
        assert!(woken(&db).await);

        // The retry: the same operation and target, queued again, clean.
        assert_eq!(db.prune_history_by_age_paged(1, NOW, 10).unwrap(), 1);
        assert!(woken(&db).await);
        let operations = db.list_history_delete_operations(false).unwrap();
        assert_eq!(operations.len(), 1);
        assert_eq!(operations[0].id, operation);
        assert_eq!(operations[0].state, crate::AsyncOperationState::Queued);
        assert_eq!(queued_targets(&db), vec![(1, false)]);
        let state = db.list_history_delete_row_states(&[1]).unwrap()[&1].clone();
        assert_eq!(state.state, crate::AsyncOperationTargetState::Queued);
        assert_eq!(state.error_message, None);

        // While it waits, a pass writes and rings nothing.
        assert_eq!(db.prune_history_by_age_paged(1, NOW, 10).unwrap(), 0);
        assert!(!woken(&db).await);
        assert_eq!(db.list_history_delete_operations(false).unwrap().len(), 1);

        db.insert_job_history(&row(2, "complete", 2)).unwrap();
        assert_eq!(db.prune_history_by_age_paged(1, NOW, 10).unwrap(), 1);
        assert_eq!(queued_targets(&db), vec![(1, false), (2, false)]);
        assert_eq!(db.list_history_delete_operations(false).unwrap().len(), 2);
    }

    // Status retention read a page at a time queues the rows the unpaged
    // read queued, hidden ones too, with and without their files, across
    // pages whose cursor crosses rows that share a second; rows of other
    // statuses stay.
    #[tokio::test]
    async fn status_pages_queue_what_the_unpaged_read_queued() {
        let seed = || {
            let db = Database::open_in_memory().unwrap();
            for (job_id, status, completed_at) in [
                (1, "complete", 50),
                (2, "complete", 50),
                (3, "complete", 50),
                (4, "complete", 90),
                (5, "complete", 10),
                (6, "failed", 70),
                (7, "failed", 70),
                (8, "failed", 5),
                (9, "partial", 30),
                (10, "cancelled", 40),
                (11, "cancelled", 40),
            ] {
                db.insert_job_history(&row(job_id, status, completed_at))
                    .unwrap();
            }
            db.set_nzbget_history_hidden(2, true).unwrap();
            db
        };
        let groups: [(&[&str], bool); 2] = [
            (&["failed", "partial", "complete"], false),
            (&["cancelled"], true),
        ];
        let unpaged = seed();
        for (statuses, delete_files) in groups {
            let ids = unpaged.list_job_history_ids_by_status(statuses).unwrap();
            assert!(
                unpaged
                    .queue_retention_history_deletes(&ids, delete_files)
                    .unwrap()
            );
        }
        let paged = seed();
        let mut pages = 0;
        for (statuses, delete_files) in groups {
            pages += paged
                .prune_history_statuses_paged(statuses, delete_files, 2)
                .unwrap();
        }
        // Five complete rows take three pages, three failed two, partial and
        // cancelled one each.
        assert_eq!(pages, 7);
        assert!(woken(&paged).await);
        assert_eq!(queued_targets(&paged), queued_targets(&unpaged));
        assert_eq!(queued_targets(&paged).len(), 11);
        // Nothing new is due: a second pass writes and rings nothing.
        for (statuses, delete_files) in groups {
            assert_eq!(
                paged
                    .prune_history_statuses_paged(statuses, delete_files, 2)
                    .unwrap(),
                0
            );
        }
        assert!(!woken(&paged).await);
    }
}
