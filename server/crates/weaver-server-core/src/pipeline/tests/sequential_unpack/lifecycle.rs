use super::*;
use crate::pipeline::direct_unpack::wiring::{AbortLatch, DemotionReason};

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
