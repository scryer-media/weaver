// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::history::delete_queue::HistoryDeleteQueueQueries;
use crate::history::persistence::HistoryPersistenceInsertJobHistoryQueries;
use crate::history::queries::HistoryQueries;
use crate::jobs::persistence::JobsPersistenceQueries;
use crate::{Database, JobHistoryRow};

fn history(job_id: u64, completed_at: i64) -> JobHistoryRow {
    JobHistoryRow {
        protocol: crate::jobs::Protocol::Nzb,
        job_id,
        job_hash: None,
        name: format!("job-{job_id}"),
        status: "complete".into(),
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
    }
}

#[test]
fn legacy_history_delete_payload_defaults_file_delete_authorization_to_false() {
    let payload: HistoryDeleteOperationPayload =
        serde_json::from_str(r#"{"delete_files":true}"#).unwrap();

    assert!(payload.delete_files);
    assert!(!payload.file_delete_authorized);
}

#[test]
fn unproven_file_delete_fails_remaining_targets_without_removing_history() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&history(10, 100)).unwrap();
    db.insert_job_history(&history(11, 101)).unwrap();
    let operation_id = db
        .insert_history_delete_operation(&[10, 11], true, false)
        .unwrap();

    let claimed = db.next_history_delete_operation().unwrap().unwrap();
    assert!(claimed.delete_files);
    assert!(!claimed.file_delete_authorized);
    db.mark_history_delete_target_state(
        operation_id,
        10,
        AsyncOperationTargetState::Completed,
        None,
    )
    .unwrap();
    db.fail_pending_history_delete_operation_targets(
        operation_id,
        "file deletion was not authorized; resubmit with an admin-scoped caller",
    )
    .unwrap();

    assert_eq!(
        db.finalize_history_delete_operation(operation_id).unwrap(),
        AsyncOperationState::CompletedWithErrors
    );
    let states = db.list_history_delete_row_states(&[10, 11]).unwrap();
    assert!(!states.contains_key(&10));
    assert_eq!(states[&11].state, AsyncOperationTargetState::Failed);
    assert_eq!(
        states[&11].error_message.as_deref(),
        Some("file deletion was not authorized; resubmit with an admin-scoped caller")
    );
    assert!(db.get_job_history(10).unwrap().is_some());
    assert!(db.get_job_history(11).unwrap().is_some());
}

#[test]
fn authorized_file_delete_marker_survives_restart_recovery() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&history(10, 100)).unwrap();
    db.insert_history_delete_operation(&[10], true, true)
        .unwrap();

    let claimed = db.next_history_delete_operation().unwrap().unwrap();
    assert!(claimed.file_delete_authorized);
    db.recover_running_history_delete_operations().unwrap();

    let recovered = db.next_history_delete_operation().unwrap().unwrap();
    assert!(recovered.delete_files);
    assert!(recovered.file_delete_authorized);
}

#[tokio::test]
async fn scheduled_prune_skips_locked_rows_and_accepts_both_file_policies() {
    let db = Database::open_in_memory().unwrap();
    for (id, status) in [
        (10, "complete"),
        (11, "complete"),
        (12, "failed"),
        (13, "cancelled"),
    ] {
        let mut row = history(id, 100);
        row.status = status.into();
        db.insert_job_history(&row).unwrap();
    }
    let existing = db
        .insert_history_delete_operation(&[10], false, false)
        .unwrap();
    db.prune_history(
        Some(crate::bandwidth::PruneFiles { delete_files: true }),
        Some(crate::bandwidth::PruneFiles {
            delete_files: false,
        }),
        None,
    )
    .await
    .unwrap();
    let states = db
        .list_history_delete_row_states(&[10, 11, 12, 13])
        .unwrap();
    assert_eq!(states[&10].operation_id, existing);
    assert!(!states[&11].delete_files);
    assert!(states[&12].delete_files);
    assert!(!states.contains_key(&13));
}

#[test]
fn scheduled_prune_batches_available_rows_and_skips_missing_targets() {
    let db = Database::open_in_memory().unwrap();
    for id in 10..110 {
        db.insert_job_history(&history(id, 100)).unwrap();
    }
    let locked = db
        .insert_history_delete_operation(&[10], false, false)
        .unwrap();
    let ids: Vec<_> = (9..111).collect();
    let operation = db
        .insert_available_history_delete_operation(&ids, true)
        .unwrap()
        .unwrap();
    let states = db.list_history_delete_row_states(&ids).unwrap();
    assert_eq!(states.len(), 100);
    assert_eq!(states[&10].operation_id, locked);
    for id in 11..110 {
        assert_eq!(states[&id].operation_id, operation);
        assert!(states[&id].delete_files);
    }
    assert_eq!(
        db.insert_available_history_delete_operation(&ids, true)
            .unwrap(),
        None
    );
}

#[test]
fn insert_history_delete_operation_creates_targets_and_locked_states() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&history(10, 100)).unwrap();
    db.insert_job_history(&history(11, 101)).unwrap();

    let operation_id = db
        .insert_history_delete_operation(&[10, 11], true, true)
        .unwrap();
    let states = db.list_history_delete_row_states(&[10, 11]).unwrap();
    assert_eq!(states.len(), 2);
    assert_eq!(states[&10].operation_id, operation_id);
    assert!(states[&10].locked);
    assert!(states[&11].delete_files);

    let summaries = db.list_history_delete_operations(true).unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].total_targets, 2);
    assert_eq!(summaries[0].queued_targets, 2);
}

#[test]
fn finalize_history_delete_operation_keeps_failed_rows_visible() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&history(10, 100)).unwrap();
    db.insert_job_history(&history(11, 101)).unwrap();
    let operation_id = db
        .insert_history_delete_operation(&[10, 11], false, false)
        .unwrap();

    db.mark_history_delete_target_state(
        operation_id,
        10,
        AsyncOperationTargetState::Completed,
        None,
    )
    .unwrap();
    db.mark_history_delete_target_state(
        operation_id,
        11,
        AsyncOperationTargetState::Failed,
        Some("disk busy"),
    )
    .unwrap();

    let final_state = db.finalize_history_delete_operation(operation_id).unwrap();
    assert_eq!(final_state, AsyncOperationState::CompletedWithErrors);

    let states = db.list_history_delete_row_states(&[10, 11]).unwrap();
    assert!(!states.contains_key(&10));
    assert_eq!(states[&11].state, AsyncOperationTargetState::Failed);
    assert!(!states[&11].locked);
    assert_eq!(states[&11].error_message.as_deref(), Some("disk busy"));
}

#[test]
fn locked_target_lookup_ignores_failed_operations() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&history(10, 100)).unwrap();
    let operation_id = db
        .insert_history_delete_operation(&[10], false, false)
        .unwrap();
    db.mark_history_delete_target_state(
        operation_id,
        10,
        AsyncOperationTargetState::Failed,
        Some("oops"),
    )
    .unwrap();
    db.finalize_history_delete_operation(operation_id).unwrap();

    let locked = db.list_history_delete_locked_target_ids(&[10]).unwrap();
    assert!(locked.is_empty());
}

#[test]
fn insert_history_delete_operation_rejects_locked_targets() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&history(10, 100)).unwrap();
    db.insert_history_delete_operation(&[10], false, false)
        .unwrap();

    let error = db
        .insert_history_delete_operation(&[10], true, true)
        .unwrap_err();
    assert!(matches!(
        error,
        HistoryDeleteOperationInsertError::LockedTargets
    ));
}

#[test]
fn next_history_delete_operation_claims_only_once_until_recovered() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&history(10, 100)).unwrap();
    let operation_id = db
        .insert_history_delete_operation(&[10], false, false)
        .unwrap();

    let claimed = db.next_history_delete_operation().unwrap().unwrap();
    assert_eq!(claimed.id, operation_id);
    assert_eq!(claimed.state, AsyncOperationState::Running);
    assert!(db.next_history_delete_operation().unwrap().is_none());

    db.mark_history_delete_target_state(operation_id, 10, AsyncOperationTargetState::Running, None)
        .unwrap();
    db.recover_running_history_delete_operations().unwrap();

    let recovered = db.next_history_delete_operation().unwrap().unwrap();
    assert_eq!(recovered.id, operation_id);
    let states = db.list_history_delete_row_states(&[10]).unwrap();
    assert_eq!(states[&10].state, AsyncOperationTargetState::Queued);
}

#[test]
fn requeue_history_delete_operation_retries_only_running_targets() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&history(10, 100)).unwrap();
    db.insert_job_history(&history(11, 100)).unwrap();
    let operation_id = db
        .insert_history_delete_operation(&[10, 11], false, false)
        .unwrap();

    let claimed = db.next_history_delete_operation().unwrap().unwrap();
    assert_eq!(claimed.id, operation_id);
    db.mark_history_delete_target_state(
        operation_id,
        10,
        AsyncOperationTargetState::Completed,
        None,
    )
    .unwrap();
    db.mark_history_delete_target_state(operation_id, 11, AsyncOperationTargetState::Running, None)
        .unwrap();

    db.requeue_history_delete_operation(operation_id).unwrap();

    let reclaimed = db.next_history_delete_operation().unwrap().unwrap();
    assert_eq!(reclaimed.id, operation_id);
    let targets = db
        .list_history_delete_operation_targets(operation_id, false)
        .unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].target_id, 11);
    assert_eq!(targets[0].state, AsyncOperationTargetState::Queued);
}

// Exceeds the sqlite bind budget (900) so the `IN (…)` builders and the
// target bulk-insert are forced across multiple chunks.
const CHUNKED_ID_COUNT: u64 = 2_500;

#[test]
fn insert_all_history_delete_operation_chunks_large_target_set() {
    let db = Database::open_in_memory().unwrap();
    for job_id in 1..=CHUNKED_ID_COUNT {
        db.insert_job_history(&history(job_id, 1_000 + job_id as i64))
            .unwrap();
    }

    let (operation_id, ids) = db.insert_all_history_delete_operation(true, true).unwrap();
    assert_eq!(ids.len() as u64, CHUNKED_ID_COUNT);

    // Every id became exactly one queued, locked target across all chunks.
    let summaries = db.list_history_delete_operations(true).unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].id, operation_id);
    assert_eq!(u64::from(summaries[0].total_targets), CHUNKED_ID_COUNT);
    assert_eq!(u64::from(summaries[0].queued_targets), CHUNKED_ID_COUNT);

    // sort_order must be dense and unique 0..N across chunk boundaries so the
    // claim order matches the (completed_at DESC, job_id DESC) input order.
    let targets = db
        .list_history_delete_operation_targets(operation_id, true)
        .unwrap();
    assert_eq!(targets.len() as u64, CHUNKED_ID_COUNT);
    // `list_all_history_job_ids_tx` orders newest-first, so target index 0 is
    // the highest job_id.
    for (index, target) in targets.iter().enumerate() {
        let expected = CHUNKED_ID_COUNT - index as u64;
        assert_eq!(target.target_id, expected, "unexpected order at {index}");
    }
}

#[test]
fn list_history_job_ids_chunks_large_input() {
    let db = Database::open_in_memory().unwrap();
    for job_id in 1..=CHUNKED_ID_COUNT {
        db.insert_job_history(&history(job_id, 1_000 + job_id as i64))
            .unwrap();
    }

    // Query for every present id plus a large block of absent ids; both the
    // present and absent ids together exceed a single sqlite bind chunk.
    let mut query_ids: Vec<u64> = (1..=CHUNKED_ID_COUNT).collect();
    query_ids.extend(1_000_000..1_000_000 + CHUNKED_ID_COUNT);
    let present = db.list_history_job_ids(&query_ids).unwrap();
    assert_eq!(present.len() as u64, CHUNKED_ID_COUNT);
    assert!(present.contains(&1));
    assert!(present.contains(&CHUNKED_ID_COUNT));
    assert!(!present.contains(&1_000_000));
}

#[test]
fn locked_target_lookup_chunks_large_input() {
    let db = Database::open_in_memory().unwrap();
    for job_id in 1..=CHUNKED_ID_COUNT {
        db.insert_job_history(&history(job_id, 1_000 + job_id as i64))
            .unwrap();
    }
    db.insert_all_history_delete_operation(false, false)
        .unwrap();

    let query_ids: Vec<u64> = (1..=CHUNKED_ID_COUNT).collect();
    let locked = db
        .list_history_delete_locked_target_ids(&query_ids)
        .unwrap();
    assert_eq!(locked.len() as u64, CHUNKED_ID_COUNT);
    assert!(locked.contains(&1));
    assert!(locked.contains(&CHUNKED_ID_COUNT));

    // A fresh insert over the same (now locked) rows must be rejected, which
    // exercises the chunked count_existing/count_locked paths in the tx.
    let error = db
        .insert_history_delete_operation(&query_ids, true, true)
        .unwrap_err();
    assert!(matches!(
        error,
        HistoryDeleteOperationInsertError::LockedTargets
    ));
}

fn active(job_id: u64) -> crate::ActiveJob {
    crate::ActiveJob {
        job_id: crate::JobId(job_id),
        nzb_hash: [0xBC; 32],
        nzb_path: std::path::PathBuf::from(format!("/invented/job-{job_id}.nzb")),
        nzb_zstd: Vec::new(),
        output_dir: std::path::PathBuf::from(format!("/invented/job-{job_id}")),
        created_at: 1_700_000_000,
        category: None,
        metadata: Vec::new(),
        status: "queued",
        download_state: "queued",
        post_state: "idle",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
        password_override: None,
        supplied_password_candidates: Vec::new(),
    }
}

fn settle(
    db: &Database,
    operation_id: u64,
    target_id: u64,
    outcome: crate::ArchiveHistoryOutcome,
    message: Option<&str>,
) {
    db.settle_archive_history_targets(
        operation_id,
        &[ArchiveHistorySettlement {
            target_id,
            state: if outcome.settles() {
                AsyncOperationTargetState::Completed
            } else {
                AsyncOperationTargetState::Failed
            },
            outcome,
            message: message.map(str::to_string),
        }],
    )
    .unwrap();
}

// Each distinct id gets its answer in the order given, and only the
// accepted ones are queued, as one archive operation.
#[test]
fn an_archive_accept_answers_each_id_and_queues_the_accepted() {
    use ArchiveHistoryAcceptOutcome as Accept;
    let db = Database::open_in_memory().unwrap();
    for id in [10, 11, 12, 14] {
        db.insert_job_history(&history(id, 100 + id as i64))
            .unwrap();
    }
    crate::pipeline::test_support::mark_entry_archived(
        &db,
        crate::JobId(11),
        Default::default(),
        1_700_000_000,
    );
    db.insert_history_delete_operation(&[12], false, false)
        .unwrap();
    db.create_active_job(&active(13)).unwrap();
    db.create_active_job(&active(14)).unwrap();
    let delete = db.next_history_delete_operation().unwrap().unwrap();
    db.recover_running_history_delete_operations().unwrap();

    let acceptance = db
        .accept_archive_history(&[13, 10, 99, 11, 10, 12, 14])
        .unwrap();

    assert_eq!(
        acceptance.results,
        vec![
            (13, Accept::NotFinished),
            (10, Accept::Accepted),
            (99, Accept::NotFound),
            (11, Accept::AlreadyArchived),
            (12, Accept::Busy),
            (14, Accept::Busy),
        ]
    );
    let operation_id = acceptance.operation_id.unwrap();
    assert_ne!(operation_id, delete.id);
    let targets = db
        .list_history_delete_operation_targets(operation_id, true)
        .unwrap();
    assert_eq!(
        targets.iter().map(|t| t.target_id).collect::<Vec<_>>(),
        [10]
    );
    let state = db.list_history_delete_row_states(&[10]).unwrap()[&10].clone();
    assert_eq!(state.action, HistoryOperationAction::Archive);
    assert_eq!(state.state, AsyncOperationTargetState::Queued);
    assert!(state.locked);
    assert_eq!(state.archive_outcome, None);

    // A request with nothing to accept writes nothing.
    let refused = db.accept_archive_history(&[10, 99]).unwrap();
    assert_eq!(refused.operation_id, None);
    assert_eq!(
        refused.results,
        vec![(10, Accept::Busy), (99, Accept::NotFound)]
    );
}

// A removal the worker could not finish shows on its entry with its
// outcome and message, and holds nothing; a later removal that succeeds
// clears it.
#[test]
fn a_failed_archive_shows_until_a_later_one_succeeds() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&history(10, 100)).unwrap();
    let first = db.accept_archive_history(&[10]).unwrap();
    let first = first.operation_id.unwrap();
    let claimed = db.next_history_delete_operation().unwrap().unwrap();
    assert_eq!(
        (claimed.id, claimed.action),
        (first, HistoryOperationAction::Archive)
    );
    settle(
        &db,
        first,
        10,
        crate::ArchiveHistoryOutcome::NotManaged,
        Some("left /x: not managed by Weaver"),
    );
    assert_eq!(
        db.finalize_history_delete_operation(first).unwrap(),
        AsyncOperationState::CompletedWithErrors
    );

    let state = db.list_history_delete_row_states(&[10]).unwrap()[&10].clone();
    assert_eq!(state.operation_id, first);
    assert_eq!(state.state, AsyncOperationTargetState::Failed);
    assert!(!state.locked);
    assert_eq!(state.action, HistoryOperationAction::Archive);
    assert_eq!(
        state.archive_outcome,
        Some(crate::ArchiveHistoryOutcome::NotManaged)
    );
    assert_eq!(
        state.error_message.as_deref(),
        Some("left /x: not managed by Weaver")
    );

    let second = db.accept_archive_history(&[10]).unwrap();
    let second = second.operation_id.unwrap();
    db.next_history_delete_operation().unwrap().unwrap();
    settle(
        &db,
        second,
        10,
        crate::ArchiveHistoryOutcome::Archived,
        None,
    );
    db.finalize_history_delete_operation(second).unwrap();
    assert!(db.list_history_delete_row_states(&[10]).unwrap().is_empty());
}

// The delete operation list shows deletes only: a waiting and a failed
// archive are seen on their rows, not in the list.
#[test]
fn the_delete_operation_list_leaves_out_archives() {
    let db = Database::open_in_memory().unwrap();
    for id in [10, 11, 12] {
        db.insert_job_history(&history(id, 100 + id as i64))
            .unwrap();
    }
    let failed = db
        .accept_archive_history(&[10])
        .unwrap()
        .operation_id
        .unwrap();
    db.next_history_delete_operation().unwrap().unwrap();
    settle(
        &db,
        failed,
        10,
        crate::ArchiveHistoryOutcome::Failed,
        Some("disk unreachable"),
    );
    db.finalize_history_delete_operation(failed).unwrap();
    let pending = db
        .accept_archive_history(&[11])
        .unwrap()
        .operation_id
        .unwrap();
    let delete = db
        .insert_history_delete_operation(&[12], false, false)
        .unwrap();

    for active_only in [true, false] {
        let listed = db
            .list_history_delete_operations(active_only)
            .unwrap()
            .into_iter()
            .map(|summary| summary.id)
            .collect::<Vec<_>>();
        assert_eq!(listed, [delete], "active_only {active_only}");
    }
    let states = db.list_history_delete_row_states(&[10, 11, 12]).unwrap();
    assert_eq!(states[&10].operation_id, failed);
    assert_eq!(states[&10].state, AsyncOperationTargetState::Failed);
    assert_eq!(states[&11].operation_id, pending);
    assert_eq!(states[&11].action, HistoryOperationAction::Archive);
    assert_eq!(states[&12].action, HistoryOperationAction::Delete);
}

// A delete for an entry whose archive is running is accepted. The
// removal is not superseded: it finishes, then the delete runs.
#[test]
fn a_delete_is_accepted_while_a_archive_runs() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&history(10, 100)).unwrap();
    let removal = db
        .accept_archive_history(&[10])
        .unwrap()
        .operation_id
        .unwrap();
    assert_eq!(
        db.next_history_delete_operation().unwrap().unwrap().id,
        removal
    );

    let delete = db
        .insert_history_delete_operation(&[10], false, false)
        .unwrap();

    let removal_targets = db
        .list_history_delete_operation_targets(removal, true)
        .unwrap();
    assert_eq!(removal_targets.len(), 1);
    assert_eq!(
        db.list_history_delete_row_states(&[10]).unwrap()[&10].operation_id,
        delete
    );
    settle(
        &db,
        removal,
        10,
        crate::ArchiveHistoryOutcome::Archived,
        None,
    );
    db.finalize_history_delete_operation(removal).unwrap();
    assert_eq!(
        db.next_history_delete_operation().unwrap().unwrap().id,
        delete
    );
}

// An archive the worker had claimed when the process stopped is
// queued again at start-up, with its targets, as a delete is.
#[test]
fn a_claimed_archive_is_queued_again_after_a_restart() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&history(10, 100)).unwrap();
    let operation_id = db
        .accept_archive_history(&[10])
        .unwrap()
        .operation_id
        .unwrap();
    db.next_history_delete_operation().unwrap().unwrap();
    db.mark_history_delete_targets_state(
        operation_id,
        &[10],
        AsyncOperationTargetState::Running,
        None,
    )
    .unwrap();
    assert!(db.next_history_delete_operation().unwrap().is_none());

    db.recover_running_history_delete_operations().unwrap();

    let claimed = db.next_history_delete_operation().unwrap().unwrap();
    assert_eq!(claimed.id, operation_id);
    assert_eq!(claimed.action, HistoryOperationAction::Archive);
    let targets = db
        .list_history_delete_operation_targets(operation_id, true)
        .unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].state, AsyncOperationTargetState::Queued);
}

// A delete accepted while an archive of the same entry still waits
// wins: the removal's target ends superseded and only the delete holds
// the entry; the removal's other entries still wait for it.
#[test]
fn a_delete_accepted_after_a_waiting_archive_supersedes_it() {
    let db = Database::open_in_memory().unwrap();
    for id in [10, 11, 12] {
        db.insert_job_history(&history(id, 100 + id as i64))
            .unwrap();
    }
    let removal = db
        .accept_archive_history(&[10, 11])
        .unwrap()
        .operation_id
        .unwrap();

    let delete = db
        .insert_history_delete_operation(&[10], true, true)
        .unwrap();

    let state = db.list_history_delete_row_states(&[10, 11]).unwrap();
    assert_eq!(state[&10].operation_id, delete);
    assert_eq!(state[&10].action, HistoryOperationAction::Delete);
    assert_eq!(state[&11].operation_id, removal);
    let waiting = db
        .list_history_delete_operation_targets(removal, true)
        .unwrap();
    assert_eq!(
        waiting.iter().map(|t| t.target_id).collect::<Vec<_>>(),
        [11]
    );

    // A delete of all history supersedes every waiting removal.
    db.insert_job_history(&history(13, 113)).unwrap();
    let later = db
        .accept_archive_history(&[13])
        .unwrap()
        .operation_id
        .unwrap();
    // The earlier delete still holds 10, so a delete of all is refused
    // until it finishes; it then supersedes the rest.
    assert!(matches!(
        db.insert_all_history_delete_operation(false, false),
        Err(HistoryDeleteOperationInsertError::LockedTargets)
    ));
    db.mark_history_delete_target_state(delete, 10, AsyncOperationTargetState::Completed, None)
        .unwrap();
    db.finalize_history_delete_operation(delete).unwrap();
    db.insert_all_history_delete_operation(false, false)
        .unwrap();
    for operation in [removal, later] {
        assert!(
            db.list_history_delete_operation_targets(operation, true)
                .unwrap()
                .is_empty()
        );
    }
}

// Retention deletes an entry whose archive failed, in a new
// operation, as the removal holds nothing once it has ended.
#[test]
fn retention_deletes_an_entry_whose_archive_failed() {
    let db = Database::open_in_memory().unwrap();
    db.insert_job_history(&history(10, 100)).unwrap();
    let removal = db
        .accept_archive_history(&[10])
        .unwrap()
        .operation_id
        .unwrap();
    // Held while the removal waits.
    assert!(!db.queue_retention_history_deletes(&[10], false).unwrap());
    db.next_history_delete_operation().unwrap().unwrap();
    settle(
        &db,
        removal,
        10,
        crate::ArchiveHistoryOutcome::Failed,
        Some("disk error"),
    );
    db.finalize_history_delete_operation(removal).unwrap();

    assert!(db.queue_retention_history_deletes(&[10], false).unwrap());
    let state = db.list_history_delete_row_states(&[10]).unwrap()[&10].clone();
    assert_eq!(state.action, HistoryOperationAction::Delete);
    assert_eq!(state.state, AsyncOperationTargetState::Queued);
}
