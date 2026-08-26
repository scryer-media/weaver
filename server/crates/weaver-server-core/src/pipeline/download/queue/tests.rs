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
        groups: vec!["alt.binaries.test".into()],
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

    q.remove_job(JobId(2));
    assert_eq!(q.excluded_work_count(), 0);
    assert_eq!(q.len(), 1);

    let mut again = make_work(3, 0, 0, 5);
    again.exclude_servers = vec![2];
    q.push(again);
    assert_eq!(q.drain_job(JobId(3)).len(), 1);
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
    assert!(q.pop_next_pipelining_compatible_with(&first).is_none());
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
fn reprioritize_job() {
    let mut q = DownloadQueue::new();
    // Job 1 segments at priority 1000 (PAR2 recovery, normally low priority).
    q.push(make_work(1, 0, 0, 1000));
    q.push(make_work(1, 0, 1, 1000));
    // Job 2 segment at priority 10 (RAR volume).
    q.push(make_work(2, 0, 0, 10));

    // Boost job 1 to priority 1 (damage detected, need recovery blocks).
    q.reprioritize_job(JobId(1), 1);

    // Job 1 segments should now come out first.
    let first = q.pop().unwrap();
    assert_eq!(first.segment_id.file_id.job_id, JobId(1));
    assert_eq!(first.priority, 1);

    let second = q.pop().unwrap();
    assert_eq!(second.segment_id.file_id.job_id, JobId(1));
    assert_eq!(second.priority, 1);

    // Job 2 last.
    let third = q.pop().unwrap();
    assert_eq!(third.segment_id.file_id.job_id, JobId(2));
    assert_eq!(third.priority, 10);
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

#[test]
fn remove_job() {
    let mut q = DownloadQueue::new();
    q.push(make_work(1, 0, 0, 10));
    q.push(make_work(1, 0, 1, 10));
    q.push(make_work(2, 0, 0, 10));
    q.push(make_work(1, 1, 0, 10));
    assert_eq!(q.len(), 4);

    q.remove_job(JobId(1));
    assert_eq!(q.len(), 1);

    let remaining = q.pop().unwrap();
    assert_eq!(remaining.segment_id.file_id.job_id, JobId(2));
    assert!(q.is_empty());
}
