use crate::jobs::ids::{JobId, MessageId, NzbFileId, SegmentId};

use super::*;

fn make_work(job_id: u64, file_index: u32, seg: u32, priority: u32) -> DownloadWork {
    DownloadWork {
        segment_id: SegmentId {
            file_id: NzbFileId {
                job_id: JobId(job_id),
                file_index,
            },
            segment_number: seg,
        },
        message_id: MessageId::new(&format!("msg-{job_id}-{file_index}-{seg}@example.com")),
        groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
        priority,
        byte_estimate: 768_000,
        retry_count: 0,
        is_recovery: false,
        completion_critical: false,
        exclude_servers: vec![],
        avoid_server: None,
    }
}

#[test]
fn empty_queue() {
    let mut q = DownloadQueue::new();
    assert!(q.is_empty());
    assert_eq!(q.len(), 0);
    assert!(q.pop().is_none());
}

#[test]
fn excluded_work_count_tracks_push_pop_and_bulk_removal() {
    let mut q = DownloadQueue::new();
    let mut excluded = make_work(1, 0, 0, 10);
    excluded.exclude_servers = vec![0];
    q.push(excluded);
    q.push(make_work(1, 0, 1, 20));
    let mut other_job = make_work(2, 0, 0, 30);
    other_job.exclude_servers = vec![1];
    q.push(other_job);
    assert_eq!(q.excluded_work_count(), 2);

    let first = q.pop().unwrap();
    assert_eq!(first.exclude_servers, vec![0]);
    assert_eq!(q.excluded_work_count(), 1);

    let removed = q.extract_matching(|work| work.segment_id.file_id.job_id == JobId(2));
    assert_eq!(removed.len(), 1);
    assert_eq!(q.excluded_work_count(), 0);
    assert_eq!(q.len(), 1);

    let mut again = make_work(3, 0, 0, 5);
    again.exclude_servers = vec![2];
    q.push(again);
    assert_eq!(
        q.extract_matching(|work| work.segment_id.file_id.job_id == JobId(3))
            .len(),
        1
    );
    assert_eq!(q.excluded_work_count(), 0);

    let mut last = make_work(4, 0, 0, 5);
    last.exclude_servers = vec![3];
    q.push(last);
    q.drain_all();
    assert_eq!(q.excluded_work_count(), 0);
}

#[test]
fn clear_exclude_servers_drops_stale_indices_and_counter() {
    let mut q = DownloadQueue::new();
    let mut excluded = make_work(1, 0, 0, 10);
    excluded.exclude_servers = vec![4, 7];
    q.push(excluded);
    q.push(make_work(1, 0, 1, 20));
    assert_eq!(q.excluded_work_count(), 1);

    q.clear_exclude_servers();
    assert_eq!(q.excluded_work_count(), 0);
    assert_eq!(q.len(), 2);
    while let Some(work) = q.pop() {
        assert!(work.exclude_servers.is_empty());
    }
}

#[test]
fn priority_ordering() {
    let mut q = DownloadQueue::new();
    q.push(make_work(1, 0, 0, 100));
    q.push(make_work(1, 0, 1, 10));
    q.push(make_work(1, 0, 2, 50));

    let first = q.pop().unwrap();
    assert_eq!(first.priority, 10);

    let second = q.pop().unwrap();
    assert_eq!(second.priority, 50);

    let third = q.pop().unwrap();
    assert_eq!(third.priority, 100);

    assert!(q.pop().is_none());
}

#[test]
fn completion_critical_work_precedes_ordinary_priority() {
    let mut q = DownloadQueue::new();
    q.push(make_work(1, 0, 0, 0));
    let mut critical = make_work(1, 1, 0, 1000);
    critical.is_recovery = true;
    critical.completion_critical = true;
    q.push(critical);

    let first = q.pop().unwrap();
    assert!(first.completion_critical);
    assert_eq!(first.priority, 1000);
    assert!(!q.pop().unwrap().completion_critical);
}

#[test]
fn compatibility_does_not_mix_critical_and_optional_recovery() {
    let mut q = DownloadQueue::new();
    let mut critical = make_work(1, 1, 0, 1000);
    critical.is_recovery = true;
    critical.completion_critical = true;
    let mut optional = make_work(1, 1, 1, 1000);
    optional.is_recovery = true;
    q.push(optional);
    q.push(critical);

    let first = q.pop().unwrap();
    assert!(first.completion_critical);
    assert!(
        q.pop_next_matching_in_class(first.completion_critical, |work| {
            work.completion_critical == first.completion_critical
        })
        .is_none()
    );
    assert_eq!(q.len(), 1);
}

#[test]
fn class_constrained_pop_preserves_constant_time_queue_class_counts() {
    let mut q = DownloadQueue::new();
    let ordinary = make_work(1, 0, 0, 0);
    let mut critical = make_work(1, 1, 0, 1000);
    critical.is_recovery = true;
    critical.completion_critical = true;
    q.push(ordinary);
    q.push(critical);

    assert!(q.has_completion_critical_work());
    assert!(q.has_noncritical_work());
    let selected = q
        .pop_first_matching(|work| !work.completion_critical)
        .expect("ordinary work must be selectable behind the critical heap head");
    assert!(!selected.completion_critical);
    assert!(q.has_completion_critical_work());
    assert!(!q.has_noncritical_work());

    q.push(make_work(1, 2, 0, 0));
    let extracted = q.extract_matching(|work| work.completion_critical);
    assert_eq!(extracted.len(), 1);
    assert!(!q.has_completion_critical_work());
    assert!(q.has_noncritical_work());

    q.drain_all();
    assert!(!q.has_completion_critical_work());
    assert!(!q.has_noncritical_work());
}

#[test]
fn class_constrained_head_pop_does_not_scan_past_incompatible_work() {
    let mut q = DownloadQueue::new();
    q.push(make_work(1, 0, 0, 10));
    q.push(make_work(1, 0, 1, 20));

    assert!(
        q.pop_next_matching_in_class(false, |work| work.segment_id.segment_number == 1)
            .is_none(),
        "ordinary dispatch must remain a heap-head operation"
    );
    assert_eq!(q.pop().unwrap().segment_id.segment_number, 0);
    assert_eq!(q.pop().unwrap().segment_id.segment_number, 1);
}

#[test]
fn recovery_presence_tracks_pop_extract_and_drain() {
    let mut q = DownloadQueue::new();
    let primary = make_work(1, 0, 0, 10);
    let mut recovery = make_work(1, 1, 0, 20);
    recovery.is_recovery = true;
    let mut other_recovery = make_work(2, 0, 0, 30);
    other_recovery.is_recovery = true;

    q.push(primary);
    q.push(recovery);
    q.push(other_recovery);
    assert!(q.has_recovery_work());

    let extracted = q.extract_matching(|work| work.segment_id.file_id.job_id == JobId(2));
    assert_eq!(extracted.len(), 1);
    assert!(q.has_recovery_work());

    let removed = q.extract_matching(|work| work.segment_id.file_id.job_id == JobId(1));
    assert_eq!(removed.len(), 2);
    assert!(!q.has_recovery_work());

    let mut recovery = make_work(3, 0, 0, 10);
    recovery.is_recovery = true;
    q.push(recovery);
    assert!(q.has_recovery_work());
    assert!(q.pop().unwrap().is_recovery);
    assert!(!q.has_recovery_work());

    let mut last_recovery = make_work(4, 0, 0, 10);
    last_recovery.is_recovery = true;
    q.push(last_recovery);
    assert!(q.has_recovery_work());
    q.drain_all();
    assert!(!q.has_recovery_work());
}

#[test]
fn rar_unlock_reprioritize_matching_updates_selected_work_only() {
    let mut q = DownloadQueue::new();
    q.push(make_work(1, 1, 0, 11));
    q.push(make_work(1, 2, 0, 12));
    q.push(make_work(1, 3, 0, 13));

    let changed =
        q.reprioritize_matching(|work| (work.segment_id.file_id.file_index == 3).then_some(3));

    assert_eq!(changed, 1);
    let first = q.pop().unwrap();
    assert_eq!(first.segment_id.file_id.file_index, 3);
    assert_eq!(first.priority, 3);
    assert_eq!(q.pop().unwrap().segment_id.file_id.file_index, 1);
    assert_eq!(q.pop().unwrap().segment_id.file_id.file_index, 2);
}

#[test]
fn rar_unlock_reprioritize_matching_preserves_equal_priority_sequence_order() {
    let mut q = DownloadQueue::new();
    q.push(make_work(1, 1, 0, 11));
    q.push(make_work(1, 2, 0, 12));
    q.push(make_work(1, 3, 0, 13));

    q.reprioritize_matching(|work| {
        matches!(work.segment_id.file_id.file_index, 2 | 3).then_some(3)
    });

    assert_eq!(q.pop().unwrap().segment_id.file_id.file_index, 2);
    assert_eq!(q.pop().unwrap().segment_id.file_id.file_index, 3);
    assert_eq!(q.pop().unwrap().segment_id.file_id.file_index, 1);
}

#[test]
fn rar_unlock_reprioritize_matching_restores_stale_boosts() {
    let mut q = DownloadQueue::new();
    q.push(make_work(1, 2, 0, 3));
    q.push(make_work(1, 3, 0, 13));

    let changed =
        q.reprioritize_matching(|work| (work.segment_id.file_id.file_index == 2).then_some(12));

    assert_eq!(changed, 1);
    let first = q.pop().unwrap();
    assert_eq!(first.segment_id.file_id.file_index, 2);
    assert_eq!(first.priority, 12);
    let second = q.pop().unwrap();
    assert_eq!(second.segment_id.file_id.file_index, 3);
    assert_eq!(second.priority, 13);
}

#[test]
fn rar_unlock_reprioritize_matching_keeps_promoted_recovery_ahead() {
    let mut q = DownloadQueue::new();
    q.push(make_work(1, 10, 0, 2));
    q.push(make_work(1, 3, 0, 13));

    q.reprioritize_matching(|work| (work.segment_id.file_id.file_index == 3).then_some(3));

    let first = q.pop().unwrap();
    assert_eq!(first.segment_id.file_id.file_index, 10);
    assert_eq!(first.priority, 2);
    let second = q.pop().unwrap();
    assert_eq!(second.segment_id.file_id.file_index, 3);
    assert_eq!(second.priority, 3);
}

#[test]
fn rar_unlock_reprioritize_matching_uses_rank_inside_priority_band() {
    let mut q = DownloadQueue::new();
    q.push(make_work(1, 4, 0, 14));
    q.push(make_work(1, 2, 0, 12));
    q.push(make_work(1, 99, 0, 3));
    q.push(make_work(1, 3, 0, 13));

    q.reprioritize_matching_with_rank(|work| match work.segment_id.file_id.file_index {
        2 => Some((3, Some(0))),
        3 => Some((3, Some(1))),
        4 => Some((3, Some(2))),
        _ => None,
    });

    let popped = [
        q.pop().unwrap().segment_id.file_id.file_index,
        q.pop().unwrap().segment_id.file_id.file_index,
        q.pop().unwrap().segment_id.file_id.file_index,
        q.pop().unwrap().segment_id.file_id.file_index,
    ];
    assert_eq!(popped, [2, 3, 4, 99]);
}

#[test]
fn promoted_identity_probe_leads_other_completion_critical_work() {
    let mut q = DownloadQueue::new();
    q.push(make_work(1, 0, 0, 0));
    q.push(make_work(1, 2, 0, 100));
    q.push(make_work(1, 2, 1, 101));
    let mut repair = make_work(1, 99, 0, 1000);
    repair.is_recovery = true;
    repair.completion_critical = true;
    q.push(repair);

    let promoted = q.promote_matching_to_completion_critical_with_rank(|work| {
        if work.priority <= 1 {
            Some((work.priority, None))
        } else {
            (work.segment_id.file_id.file_index == 2 && work.segment_id.segment_number == 0)
                .then_some((2, Some(2)))
        }
    });

    assert_eq!(promoted, 2);
    let index = q.pop().unwrap();
    assert_eq!(index.priority, 0);
    assert!(index.completion_critical);
    let probe = q.pop().unwrap();
    assert_eq!(probe.segment_id.file_id.file_index, 2);
    assert_eq!(probe.segment_id.segment_number, 0);
    assert_eq!(probe.priority, 2);
    assert!(probe.completion_critical);
    assert!(q.pop().unwrap().completion_critical);
    assert!(!q.pop().unwrap().completion_critical);
}

#[test]
fn mixed_priorities() {
    let mut q = DownloadQueue::new();

    // PAR2 index file: priority 0.
    q.push(make_work(1, 0, 0, 0));
    // First RAR volume: priority 1.
    q.push(make_work(1, 1, 0, 1));
    // Second RAR volume: priority 11.
    q.push(make_work(1, 2, 0, 11));
    // PAR2 recovery: priority 1000.
    q.push(make_work(1, 3, 0, 1000));

    let items: Vec<_> = std::iter::from_fn(|| q.pop()).collect();
    assert_eq!(items.len(), 4);
    assert_eq!(items[0].priority, 0); // PAR2 index
    assert_eq!(items[1].priority, 1); // First RAR
    assert_eq!(items[2].priority, 11); // Second RAR
    assert_eq!(items[3].priority, 1000); // PAR2 recovery
}

fn plan(entries: &[(u32, u32, Option<u32>)]) -> HashMap<NzbFileId, (u32, Option<u32>)> {
    entries
        .iter()
        .map(|(file_index, priority, rank)| {
            (
                NzbFileId {
                    job_id: JobId(1),
                    file_index: *file_index,
                },
                (*priority, *rank),
            )
        })
        .collect()
}

fn pop_files(q: &mut DownloadQueue) -> Vec<(u32, u32)> {
    let mut out = Vec::new();
    while let Some(work) = q.pop() {
        out.push((work.segment_id.file_id.file_index, work.priority));
    }
    out
}

#[test]
fn file_priority_plan_applies_to_queued_work_and_to_later_pushes() {
    let mut q = DownloadQueue::new();
    q.push(make_work(1, 0, 0, 10));
    q.push(make_work(1, 1, 0, 10));
    q.push(make_work(1, 2, 0, 1)); // at or below the protected band: untouched

    let changed = q.install_file_priority_plan(plan(&[(1, 3, Some(0)), (2, 3, Some(0))]), 2);
    assert_eq!(changed, 1, "only the unprotected planned file changes key");

    // A push after the plan lands at the planned key without a rebuild.
    q.push(make_work(1, 1, 1, 10));
    assert_eq!(
        pop_files(&mut q),
        vec![(2, 1), (1, 3), (1, 3), (0, 10)],
        "protected item first, then both planned items ahead of the unplanned one"
    );
}

#[test]
fn reinstalling_an_identical_plan_is_a_no_op_until_keys_are_rewritten_elsewhere() {
    let mut q = DownloadQueue::new();
    q.push(make_work(1, 0, 0, 10));
    q.push(make_work(1, 1, 0, 10));
    assert_eq!(q.install_file_priority_plan(plan(&[(1, 3, Some(0))]), 2), 1);
    assert_eq!(q.install_file_priority_plan(plan(&[(1, 3, Some(0))]), 2), 0);

    // Direct-store style rewrite of the same file's key: the next install of
    // the unchanged plan re-asserts it, exactly as a rebuild used to.
    let rewritten =
        q.reprioritize_matching(|work| (work.segment_id.file_id.file_index == 1).then_some(11));
    assert_eq!(rewritten, 1);
    assert_eq!(q.install_file_priority_plan(plan(&[(1, 3, Some(0))]), 2), 1);
    assert_eq!(pop_files(&mut q), vec![(1, 3), (0, 10)]);
}

#[test]
fn a_requeued_retry_keeps_its_planned_rank_without_a_rebuild() {
    let mut q = DownloadQueue::new();
    q.push(make_work(1, 0, 0, 10));
    q.push(make_work(1, 1, 0, 10));
    q.install_file_priority_plan(plan(&[(0, 3, Some(1)), (1, 3, Some(0))]), 2);
    let planned_order = pop_files(&mut q);
    assert_eq!(planned_order.len(), 2);

    // Requeue in the opposite order: the plan, not push order, decides.
    q.push(make_work(1, 0, 0, 10));
    q.push(make_work(1, 1, 0, 10));
    assert_eq!(pop_files(&mut q), planned_order);
}

#[test]
fn queued_count_per_file_tracks_push_pop_removal_and_drain() {
    let mut q = DownloadQueue::new();
    let file0 = NzbFileId {
        job_id: JobId(1),
        file_index: 0,
    };
    let file1 = NzbFileId {
        job_id: JobId(1),
        file_index: 1,
    };
    q.push(make_work(1, 0, 0, 10));
    q.push(make_work(1, 0, 1, 10));
    q.push(make_work(1, 1, 0, 10));
    assert_eq!(q.queued_count_for_file(file0), 2);
    assert_eq!(q.queued_count_for_file(file1), 1);

    q.pop();
    assert_eq!(
        q.queued_count_for_file(file0) + q.queued_count_for_file(file1),
        2
    );

    let drained = q.drain_all();
    assert_eq!(drained.len(), 2);
    assert_eq!(q.queued_count_for_file(file0), 0);
    assert_eq!(q.queued_count_for_file(file1), 0);
}
