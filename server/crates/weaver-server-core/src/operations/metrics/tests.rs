use std::sync::atomic::Ordering;

use super::*;

#[test]
fn metrics_snapshot() {
    let m = PipelineMetrics::new();
    m.bytes_downloaded.store(1024, Ordering::Relaxed);
    m.segments_downloaded.store(5, Ordering::Relaxed);
    m.decode_pending.store(3, Ordering::Relaxed);
    m.write_buffered_bytes.store(2048, Ordering::Relaxed);
    m.write_buffered_segments.store(2, Ordering::Relaxed);

    let snap = m.snapshot();
    assert_eq!(snap.bytes_downloaded, 1024);
    assert_eq!(snap.segments_downloaded, 5);
    assert_eq!(snap.decode_pending, 3);
    assert_eq!(snap.write_buffered_bytes, 2048);
    assert_eq!(snap.write_buffered_segments, 2);
    assert_eq!(snap.bytes_decoded, 0);
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
