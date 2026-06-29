use std::sync::atomic::Ordering;

use super::*;

#[test]
fn metrics_snapshot() {
    let m = PipelineMetrics::new();
    m.bytes_downloaded.store(1024, Ordering::Relaxed);
    m.segments_downloaded.store(5, Ordering::Relaxed);
    m.note_decode_work_queued(1024);
    m.note_decode_work_queued(512);
    m.note_decode_task_started(512);
    m.write_buffered_bytes.store(2048, Ordering::Relaxed);
    m.write_buffered_segments.store(2, Ordering::Relaxed);
    m.download_failures_capacity_unavailable
        .store(3, Ordering::Relaxed);
    m.download_failures_transient.store(4, Ordering::Relaxed);

    let snap = m.snapshot();
    assert_eq!(snap.bytes_downloaded, 1024);
    assert_eq!(snap.segments_downloaded, 5);
    assert_eq!(snap.decode_pending, 1);
    assert_eq!(snap.decode_pending_bytes, 1024);
    assert_eq!(snap.decode_active_bytes, 512);
    assert_eq!(snap.write_buffered_bytes, 2048);
    assert_eq!(snap.write_buffered_segments, 2);
    assert_eq!(snap.download_failures_capacity_unavailable, 3);
    assert_eq!(snap.download_failures_transient, 4);
    assert_eq!(snap.bytes_decoded, 0);
}

#[test]
fn decode_byte_accounting_releases_saturating() {
    let m = PipelineMetrics::new();
    m.note_decode_work_queued(100);
    let queued = m.raw_snapshot();
    assert_eq!(queued.decode_pending, 1);
    assert_eq!(queued.decode_pending_bytes, 100);
    assert_eq!(queued.decode_active_bytes, 0);

    m.note_decode_task_started(100);
    let active = m.raw_snapshot();
    assert_eq!(active.decode_pending, 0);
    assert_eq!(active.decode_pending_bytes, 0);
    assert_eq!(active.decode_active_bytes, 100);

    m.note_decode_task_finished(150);

    let snap = m.raw_snapshot();
    assert_eq!(snap.decode_pending, 0);
    assert_eq!(snap.decode_pending_bytes, 0);
    assert_eq!(snap.decode_active_bytes, 0);
}

#[tokio::test]
async fn concurrent_metrics() {
    let m = PipelineMetrics::new();

    let mut handles = Vec::new();
    for _ in 0..10 {
        let m = Arc::clone(&m);
        handles.push(tokio::spawn(async move {
            for _ in 0..1000 {
                m.bytes_downloaded.fetch_add(100, Ordering::Relaxed);
                m.segments_downloaded.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let snap = m.snapshot();
    assert_eq!(snap.bytes_downloaded, 10 * 1000 * 100);
    assert_eq!(snap.segments_downloaded, 10 * 1000);
}
