use super::*;
use crate::pipeline::direct_unpack::wiring::{AbortLatch, DemotionReason};

#[tokio::test]
async fn pausing_a_chase_waiting_for_memory_does_not_wait_for_another_download() {
    let temp = TempDir::new().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    pipeline.direct_unpack_process_memory = Arc::new(
        crate::pipeline::extraction::ProcessMemoryBudget::new(1024 * 1024),
    );
    let (name, bytes, members) = fixture(SimpleArchiveKind::Gz);
    let first = JobId(43125);
    let second = JobId(43126);
    for job in [first, second] {
        insert_stream(&mut pipeline, job, &[(name.clone(), bytes.clone())]).await;
        let id = NzbFileId {
            job_id: job,
            file_index: 0,
        };
        for number in 0..bytes.len().div_ceil(ARTICLE) / 2 {
            land(&mut pipeline, id, &name, &bytes, number).await;
        }
        if job == first {
            assert!(
                wait_for_output(
                    &pipeline
                        .direct_unpack_staging_dir(job, &name)
                        .join(&members[0].0),
                    8192,
                )
                .await
            );
        }
    }
    let second_staging = pipeline.direct_unpack_staging_dir(second, &name);
    pipeline.direct_unpack_abort_job(
        second,
        "paused",
        AbortLatch::Retryable,
        DemotionReason::DownloadEnded,
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while second_staging.exists() && tokio::time::Instant::now() < deadline {
        pipeline.reap_direct_unpack().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let cancelled_without_first = !second_staging.exists();
    assert!(pipeline.direct_unpack.is_armed(first, &name));
    pipeline
        .direct_unpack_shutdown("memory cancellation test cleanup")
        .await;
    assert!(
        cancelled_without_first,
        "a cancelled memory waiter must not depend on another download finishing"
    );
}

#[tokio::test]
async fn truncated_compression_streams_fail_without_repair_data() {
    for kind in [
        SimpleArchiveKind::Deflate,
        SimpleArchiveKind::Gz,
        SimpleArchiveKind::Bzip2,
        SimpleArchiveKind::Xz,
        SimpleArchiveKind::Brotli,
        SimpleArchiveKind::Zstd,
        SimpleArchiveKind::TarGz,
        SimpleArchiveKind::TarBz2,
        SimpleArchiveKind::TarXz,
    ] {
        let temp = TempDir::new().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
        let job = JobId(43123);
        let (name, mut bytes, _) = fixture(kind);
        bytes.truncate(bytes.len() - 8);
        insert_stream(&mut pipeline, job, &[(name.clone(), bytes.clone())]).await;
        let id = NzbFileId {
            job_id: job,
            file_index: 0,
        };
        for number in 0..bytes.len().div_ceil(ARTICLE) {
            land(&mut pipeline, id, &name, &bytes, number).await;
        }
        finish(&mut pipeline, job, &name).await;
        let done = tokio::time::timeout(Duration::from_secs(20), pipeline.extract_done_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let ExtractionDone::FullSet { result, .. } = done else {
            panic!("expected full set")
        };
        assert!(result.is_err(), "{kind:?} accepted a truncated stream");
    }
}

#[tokio::test]
async fn bad_compression_trailers_fail_without_repair_data() {
    for kind in [
        SimpleArchiveKind::Gz,
        SimpleArchiveKind::Bzip2,
        SimpleArchiveKind::Xz,
        SimpleArchiveKind::TarGz,
        SimpleArchiveKind::TarBz2,
        SimpleArchiveKind::TarXz,
    ] {
        let temp = TempDir::new().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
        let job = JobId(43121);
        let (name, mut bytes, _) = fixture(kind);
        let damage = bytes.len() - 5;
        bytes[damage] ^= 0x7f;
        insert_stream(&mut pipeline, job, &[(name.clone(), bytes.clone())]).await;
        let id = NzbFileId {
            job_id: job,
            file_index: 0,
        };
        for number in 0..bytes.len().div_ceil(ARTICLE) {
            land(&mut pipeline, id, &name, &bytes, number).await;
        }
        finish(&mut pipeline, job, &name).await;
        let done = tokio::time::timeout(Duration::from_secs(20), pipeline.extract_done_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let ExtractionDone::FullSet { result, .. } = done else {
            panic!("expected full set")
        };
        assert!(result.is_err(), "{kind:?} accepted a corrupt trailer");
    }
}

#[tokio::test]
async fn concatenated_compression_frames_preserve_every_byte() {
    for kind in [
        SimpleArchiveKind::Gz,
        SimpleArchiveKind::Bzip2,
        SimpleArchiveKind::Xz,
        SimpleArchiveKind::Zstd,
    ] {
        let temp = TempDir::new().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
        let job = JobId(43122);
        let (name, mut bytes, mut members) = fixture(kind);
        bytes.extend_from_within(..);
        members[0].1.extend_from_within(..);
        insert_stream(&mut pipeline, job, &[(name.clone(), bytes.clone())]).await;
        let id = NzbFileId {
            job_id: job,
            file_index: 0,
        };
        for number in 0..bytes.len().div_ceil(ARTICLE) {
            land(&mut pipeline, id, &name, &bytes, number).await;
        }
        finish(&mut pipeline, job, &name).await;
        extracted(&mut pipeline, job, &members).await;
        assert_eq!(pipeline.direct_unpack.counters().consumed, 1);
    }
}

pub(super) async fn lifecycle(kind: SimpleArchiveKind) {
    let temp = TempDir::new().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job = JobId(43120);
    let (name, bytes, members) = fixture(kind);
    insert_stream(&mut pipeline, job, &[(name.clone(), bytes.clone())]).await;
    let id = NzbFileId {
        job_id: job,
        file_index: 0,
    };
    let half = bytes.len().div_ceil(ARTICLE) / 2;
    for number in 0..half {
        land(&mut pipeline, id, &name, &bytes, number).await;
    }
    let staging = pipeline.direct_unpack_staging_dir(job, &name);
    let early = wait_for_output(&staging.join(&members[0].0), 8192).await;
    pipeline.direct_unpack_abort_job(
        job,
        "paused",
        AbortLatch::Retryable,
        DemotionReason::DownloadEnded,
    );
    pipeline.direct_unpack_shutdown("join paused worker").await;
    let removed = !staging.exists();
    land(&mut pipeline, id, &name, &bytes, half).await;
    let rearmed = pipeline.direct_unpack.is_armed(job, &name);
    land(&mut pipeline, id, &name, &bytes, 0).await;
    let discarded = !pipeline.direct_unpack.is_armed(job, &name);
    pipeline
        .direct_unpack_shutdown("join invalidated worker")
        .await;
    assert!(early);
    assert!(removed);
    assert!(rearmed);
    assert!(discarded);
    assert!(!staging.exists());
    assert_eq!(pipeline.direct_unpack.counters().armed, 2);
}
