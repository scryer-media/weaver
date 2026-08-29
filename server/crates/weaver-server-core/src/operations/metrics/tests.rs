use std::sync::atomic::Ordering;

use super::*;

#[test]
fn spillover_decision_codes_round_trip() {
    for decision in SpilloverDecision::ALL {
        assert_eq!(SpilloverDecision::from_code(decision.as_code()), decision);
    }
}

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
    m.hot_dispatch_job_id.store(42, Ordering::Relaxed);
    m.hot_dispatch_mode
        .store(DispatchShareMode::Shared.as_code(), Ordering::Relaxed);
    m.hot_dispatch_underfill_ms.store(2500, Ordering::Relaxed);
    m.hot_dispatch_lent_connections.store(2, Ordering::Relaxed);
    m.hot_dispatch_last_spillover_decision.store(
        SpilloverDecision::AllowedUnderfill.as_code(),
        Ordering::Relaxed,
    );
    m.hot_dispatch_spillover_allowed_underfill_total
        .store(7, Ordering::Relaxed);

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
    assert_eq!(snap.hot_dispatch_job_id, 42);
    assert_eq!(snap.hot_dispatch_mode, DispatchShareMode::Shared);
    assert_eq!(snap.hot_dispatch_underfill_ms, 2500);
    assert_eq!(snap.hot_dispatch_lent_connections, 2);
    assert_eq!(
        snap.hot_dispatch_last_spillover_decision,
        SpilloverDecision::AllowedUnderfill
    );
    assert_eq!(snap.hot_dispatch_spillover_allowed_underfill_total, 7);
}

#[test]
fn extraction_rejections_are_counted_by_stable_reason() {
    let metrics = PipelineMetrics::new();
    metrics.note_extraction_rejection("unsafe_path");
    metrics.note_extraction_rejection("unsafe_path");
    metrics.note_extraction_rejection("disk_reserve");

    assert_eq!(metrics.extraction_rejections(), [2, 0, 0, 0, 0, 0, 0, 0, 1]);
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

/// The three published rate gauges were once lifetime averages since process
/// start, so a box that downloaded hard for an hour and then stalled kept
/// reporting a healthy rate forever. They are short-window rates now; these
/// tests pin that they follow the window rather than the process lifetime.
#[test]
fn rate_gauges_reflect_the_recent_window_not_the_process_lifetime() {
    let m = PipelineMetrics::new();

    // Prime the window with a first tick at zero.
    let _ = m.snapshot();
    std::thread::sleep(std::time::Duration::from_millis(120));

    m.segments_downloaded.store(600, Ordering::Relaxed);
    m.bytes_decoded.store(6 * 1024 * 1024, Ordering::Relaxed);
    m.bytes_downloaded.store(6 * 1024 * 1024, Ordering::Relaxed);
    let busy = m.snapshot();
    assert!(
        busy.articles_per_sec > 0.0,
        "articles_per_sec was {}",
        busy.articles_per_sec
    );
    assert!(
        busy.decode_rate_mbps > 0.0,
        "decode_rate_mbps was {}",
        busy.decode_rate_mbps
    );

    // Now go idle: the cumulative counters stop moving. A lifetime average
    // would barely budge; the windowed rate must fall.
    for _ in 0..6 {
        std::thread::sleep(std::time::Duration::from_millis(120));
        let _ = m.snapshot();
    }
    let idle = m.snapshot();
    assert!(
        idle.articles_per_sec < busy.articles_per_sec,
        "idle {} should be below busy {}",
        idle.articles_per_sec,
        busy.articles_per_sec
    );
    assert!(
        idle.decode_rate_mbps < busy.decode_rate_mbps,
        "idle {} should be below busy {}",
        idle.decode_rate_mbps,
        busy.decode_rate_mbps
    );
}

#[test]
fn decode_rate_is_reported_in_mib_per_second() {
    let m = PipelineMetrics::new();
    let _ = m.snapshot();
    std::thread::sleep(std::time::Duration::from_millis(200));
    // 4 MiB decoded across roughly 0.2 s is on the order of 20 MiB/s. The exact
    // value depends on the EMA, so assert the unit's order of magnitude rather
    // than a brittle constant: in MB/s this would read ~21, in B/s ~4.2e6.
    m.bytes_decoded.store(4 * 1024 * 1024, Ordering::Relaxed);
    let snap = m.snapshot();
    assert!(
        snap.decode_rate_mbps > 1.0 && snap.decode_rate_mbps < 1000.0,
        "decode_rate_mbps out of plausible MiB/s range: {}",
        snap.decode_rate_mbps
    );
}

#[test]
fn raw_snapshot_carries_the_last_computed_rates_without_advancing_the_window() {
    let m = PipelineMetrics::new();
    let _ = m.snapshot();
    std::thread::sleep(std::time::Duration::from_millis(120));
    m.segments_downloaded.store(300, Ordering::Relaxed);
    m.bytes_downloaded.store(3 * 1024 * 1024, Ordering::Relaxed);
    let ticked = m.snapshot();
    assert!(ticked.articles_per_sec > 0.0);

    // Repeated raw reads must neither advance nor cool the shared tracker.
    for _ in 0..5 {
        std::thread::sleep(std::time::Duration::from_millis(30));
        let raw = m.raw_snapshot();
        assert_eq!(raw.articles_per_sec, ticked.articles_per_sec);
        assert_eq!(raw.decode_rate_mbps, ticked.decode_rate_mbps);
        assert_eq!(raw.current_download_speed, ticked.current_download_speed);
    }
}

#[test]
fn instrumentation_registries_are_reachable_from_the_shared_metrics_handle() {
    let m = PipelineMetrics::new();
    // The exporter reaches all three of these without going through the 100ms
    // snapshot, so MetricsSnapshot stays a fixed-size struct copy.
    assert!(m.server_metrics.snapshot().is_empty());
    assert!(m.job_lifecycle.snapshot().submitted.is_empty());
    assert_eq!(
        m.pipeline_histograms.snapshot().disk_write_duration.count,
        0
    );

    m.pipeline_histograms
        .observe_disk_write(std::time::Duration::from_millis(3));
    assert_eq!(
        m.pipeline_histograms.snapshot().disk_write_duration.count,
        1
    );
}

/// `ALL` is what the Prometheus exporter iterates to build its label sets, so
/// a variant missing from `ALL` silently drops a series. Pin the lists against
/// the dense code space every variant already round-trips through.
#[test]
fn variant_lists_cover_every_code() {
    for decision in SpilloverDecision::ALL {
        assert_eq!(SpilloverDecision::from_code(decision.as_code()), decision);
        assert!(!decision.as_str().is_empty());
    }
    let mut codes: Vec<usize> = SpilloverDecision::ALL
        .iter()
        .map(|decision| decision.as_code())
        .collect();
    codes.sort_unstable();
    assert_eq!(codes, (0..SpilloverDecision::ALL.len()).collect::<Vec<_>>());

    for state in DownloadPressureState::ALL {
        assert_eq!(DownloadPressureState::from_code(state.as_code()), state);
    }
    for reason in DownloadPressureReason::ALL {
        assert_eq!(DownloadPressureReason::from_code(reason.as_code()), reason);
    }
    for mode in DispatchShareMode::ALL {
        assert_eq!(DispatchShareMode::from_code(mode.as_code()), mode);
    }
}
