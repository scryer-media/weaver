use crate::jobs::ids::{JobId, MessageId, NzbFileId, SegmentId};

use super::*;

fn make_work(seg: u32) -> DownloadWork {
    DownloadWork {
        segment_id: SegmentId {
            file_id: NzbFileId {
                job_id: JobId(1),
                file_index: 0,
            },
            segment_number: seg,
        },
        message_id: MessageId::new(&format!("msg-{seg}@test.com")),
        groups: vec!["alt.binaries.test".into()],
        priority: 10,
        byte_estimate: 768_000,
        retry_count: 0,
        is_recovery: false,
        exclude_servers: vec![],
    }
}

#[test]
fn schedule_and_drain() {
    let config = RetryConfig {
        max_retries: 3,
        base_delay: Duration::from_millis(1),
        multiplier: 1.0,
    };
    let mut q = RetryQueue::new(config);

    assert!(q.schedule(make_work(0), 0));
    assert_eq!(q.len(), 1);

    // Wait for the 1ms delay to expire.
    std::thread::sleep(Duration::from_millis(5));

    let ready = q.drain_ready();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].retry_count, 1);
    assert_eq!(ready[0].work.retry_count, 1);
    assert!(q.is_empty());
}

#[test]
fn max_retries_exceeded() {
    let config = RetryConfig {
        max_retries: 2,
        base_delay: Duration::from_millis(1),
        multiplier: 1.0,
    };
    let mut q = RetryQueue::new(config);

    assert!(q.schedule(make_work(0), 0));
    assert!(q.schedule(make_work(1), 1));
    assert!(!q.schedule(make_work(2), 2)); // exceeded
    assert_eq!(q.len(), 2);
}

#[test]
fn drain_respects_ready_at() {
    let config = RetryConfig {
        max_retries: 3,
        base_delay: Duration::from_mins(1),
        multiplier: 1.0,
    };
    let mut q = RetryQueue::new(config);

    assert!(q.schedule(make_work(0), 0));

    // Not ready yet — delay is 60s.
    let ready = q.drain_ready();
    assert!(ready.is_empty());
    assert_eq!(q.len(), 1);
}

#[test]
fn next_ready_at_empty() {
    let q = RetryQueue::new(RetryConfig::default());
    assert!(q.next_ready_at().is_none());
}

#[test]
fn next_ready_at_populated() {
    let config = RetryConfig {
        max_retries: 3,
        base_delay: Duration::from_secs(10),
        multiplier: 1.0,
    };
    let mut q = RetryQueue::new(config);
    q.schedule(make_work(0), 0);

    let next = q.next_ready_at().unwrap();
    assert!(next > Instant::now());
}

#[test]
fn ordering_earliest_first() {
    let config = RetryConfig {
        max_retries: 5,
        base_delay: Duration::from_millis(1),
        multiplier: 10.0, // 1ms, 10ms, 100ms, 1s, 10s
    };
    let mut q = RetryQueue::new(config);

    // Schedule with increasing retry counts → increasing delays.
    q.schedule(make_work(0), 2); // 100ms
    q.schedule(make_work(1), 0); // 1ms
    q.schedule(make_work(2), 1); // 10ms

    // Wait long enough for all to be ready.
    std::thread::sleep(Duration::from_millis(150));

    let ready = q.drain_ready();
    assert_eq!(ready.len(), 3);
    // Should come out in order: segment 1 (1ms), segment 2 (10ms), segment 0 (100ms).
    assert_eq!(ready[0].work.segment_id.segment_number, 1);
    assert_eq!(ready[1].work.segment_id.segment_number, 2);
    assert_eq!(ready[2].work.segment_id.segment_number, 0);
}

#[test]
fn delay_calculation() {
    let config = RetryConfig {
        max_retries: 5,
        base_delay: Duration::from_secs(1),
        multiplier: 5.0,
    };
    assert_eq!(config.delay_for_attempt(0), Duration::from_secs(1));
    assert_eq!(config.delay_for_attempt(1), Duration::from_secs(5));
    assert_eq!(config.delay_for_attempt(2), Duration::from_secs(25));
    // Capped at 30s.
    assert_eq!(config.delay_for_attempt(3), Duration::from_secs(30));
}
