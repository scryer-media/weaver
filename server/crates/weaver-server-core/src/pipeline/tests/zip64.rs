use super::*;
use crate::pipeline::completion::finalize::SimpleArchiveKind;

// Independent, forced ZIP64 fixture: all size/offset sentinels, ZIP64 EOCD and
// locator, and (optionally) signed or unsigned 64-bit data descriptors.
fn zip64_fixture(descriptor: Option<bool>) -> (Vec<u8>, Vec<(&'static str, Vec<u8>)>) {
    zip64_fixture_with_compression(descriptor, false)
}

fn zip64_fixture_with_compression(
    descriptor: Option<bool>,
    deflated: bool,
) -> (Vec<u8>, Vec<(&'static str, Vec<u8>)>) {
    use std::io::Write;
    let mut random = 0x12345678u32;
    let mut members = vec![
        ("first.bin", vec![0x31; 8192]),
        (
            "second.bin",
            (0..1024 * 1024)
                .map(|_| {
                    random ^= random << 13;
                    random ^= random >> 17;
                    random ^= random << 5;
                    random as u8
                })
                .collect(),
        ),
    ];
    // Its local header is in the withheld middle, not the directory tail.
    // Listing metadata must not wait for it before extracting first.bin.
    members.push(("third.bin", members[1].1.clone()));
    let mut archive = Vec::new();
    let mut directory = Vec::new();
    let flags: u16 = if descriptor.is_some() { 8 } else { 0 };
    let method = if deflated { 8u16 } else { 0 };
    for (name, bytes) in &members {
        let packed = if deflated {
            let mut encoder =
                flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::fast());
            encoder.write_all(bytes).unwrap();
            encoder.finish().unwrap()
        } else {
            bytes.clone()
        };
        let packed_len = packed.len() as u64;
        let offset = archive.len() as u64;
        let len = bytes.len() as u64;
        let crc = crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, bytes) as u32;
        archive.extend(0x04034b50u32.to_le_bytes());
        for v in [45u16, flags, method, 0, 0] {
            archive.extend(v.to_le_bytes());
        }
        for v in [
            if descriptor.is_some() { 0 } else { crc },
            u32::MAX,
            u32::MAX,
        ] {
            archive.extend(v.to_le_bytes());
        }
        archive.extend((name.len() as u16).to_le_bytes());
        archive.extend(20u16.to_le_bytes());
        archive.extend(name.as_bytes());
        archive.extend(1u16.to_le_bytes());
        archive.extend(16u16.to_le_bytes());
        for size in [len, packed_len] {
            archive.extend(if descriptor.is_some() { 0u64 } else { size }.to_le_bytes());
        }
        archive.extend(packed);
        if let Some(signed) = descriptor {
            if signed {
                archive.extend(0x08074b50u32.to_le_bytes());
            }
            archive.extend(crc.to_le_bytes());
            archive.extend(packed_len.to_le_bytes());
            archive.extend(len.to_le_bytes());
        }
        directory.extend(0x02014b50u32.to_le_bytes());
        for v in [45u16, 45, flags, method, 0, 0] {
            directory.extend(v.to_le_bytes());
        }
        for v in [crc, u32::MAX, u32::MAX] {
            directory.extend(v.to_le_bytes());
        }
        for v in [name.len() as u16, 28, 0, 0, 0] {
            directory.extend(v.to_le_bytes());
        }
        directory.extend(0u32.to_le_bytes());
        directory.extend(u32::MAX.to_le_bytes());
        directory.extend(name.as_bytes());
        directory.extend(1u16.to_le_bytes());
        directory.extend(24u16.to_le_bytes());
        for v in [len, packed_len, offset] {
            directory.extend(v.to_le_bytes());
        }
    }
    let directory_offset = archive.len() as u64;
    let directory_len = directory.len() as u64;
    archive.extend(directory);
    let end_offset = archive.len() as u64;
    archive.extend(0x06064b50u32.to_le_bytes());
    archive.extend(44u64.to_le_bytes());
    archive.extend(45u16.to_le_bytes());
    archive.extend(45u16.to_le_bytes());
    archive.extend([0u8; 8]);
    for v in [
        members.len() as u64,
        members.len() as u64,
        directory_len,
        directory_offset,
    ] {
        archive.extend(v.to_le_bytes());
    }
    archive.extend(0x07064b50u32.to_le_bytes());
    archive.extend(0u32.to_le_bytes());
    archive.extend(end_offset.to_le_bytes());
    archive.extend(1u32.to_le_bytes());
    archive.extend(0x06054b50u32.to_le_bytes());
    archive.extend([0u8; 4]);
    archive.extend([0xffu8; 12]);
    archive.extend(0u16.to_le_bytes());
    (archive, members)
}

const ARTICLE_BYTES: usize = 64 * 1024;
const ARCHIVE_NAME: &str = "payload.zip";

async fn zip_job(pipeline: &mut Pipeline, job_id: JobId, bytes: &[u8]) -> NzbFileId {
    let mut spec = rar_job_spec("ZIP64", &[(ARCHIVE_NAME.to_string(), bytes.to_vec())]);
    spec.files[0].segments = bytes
        .chunks(ARTICLE_BYTES)
        .enumerate()
        .map(|(i, bytes)| {
            segment_spec! {
                number: i as u32, bytes: bytes.len() as u32, message_id: format!("zip64-{i}@test"),
            }
        })
        .collect();
    insert_active_job(pipeline, job_id, spec).await;
    NzbFileId {
        job_id,
        file_index: 0,
    }
}

async fn land_article(pipeline: &mut Pipeline, id: NzbFileId, bytes: &[u8], number: usize) {
    let start = number * ARTICLE_BYTES;
    let end = (start + ARTICLE_BYTES).min(bytes.len());
    submit_decoded_segment(
        pipeline,
        id,
        number as u32,
        start as u64,
        &bytes[start..end],
        ARCHIVE_NAME,
        None,
    )
    .await;
}

async fn reap_zip(pipeline: &mut Pipeline, job_id: JobId) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while pipeline.direct_unpack.is_armed(job_id, ARCHIVE_NAME)
        && tokio::time::Instant::now() < deadline
    {
        pipeline.reap_direct_unpack().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    pipeline.direct_unpack_shutdown("ZIP64 test teardown").await;
}

#[tokio::test]
async fn zip64_direct_unpack_finishes_a_member_before_middle_articles_arrive() {
    for (descriptor, deflated) in [None, Some(false), Some(true)]
        .into_iter()
        .flat_map(|descriptor| [false, true].map(|deflated| (descriptor, deflated)))
    {
        let temp = TempDir::new().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
        let job_id = JobId(43000);
        let (bytes, members) = zip64_fixture_with_compression(descriptor, deflated);
        let id = zip_job(&mut pipeline, job_id, &bytes).await;
        let count = bytes.len().div_ceil(ARTICLE_BYTES);
        // Tail first exercises arming without the first article or a topology.
        land_article(&mut pipeline, id, &bytes, count - 1).await;
        land_article(&mut pipeline, id, &bytes, count - 2).await;
        land_article(&mut pipeline, id, &bytes, 0).await;
        let staging = pipeline.direct_unpack_staging_dir(job_id, ARCHIVE_NAME);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let early = loop {
            if std::fs::read(staging.join(members[0].0)).ok().as_ref() == Some(&members[0].1) {
                break true;
            }
            if tokio::time::Instant::now() >= deadline {
                break false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let incomplete = !pipeline.jobs[&job_id]
            .assembly
            .file(id)
            .unwrap()
            .is_complete();
        let buffered = pipeline.write_buffered_bytes;
        for number in 1..count - 2 {
            land_article(&mut pipeline, id, &bytes, number).await;
        }
        reap_zip(&mut pipeline, job_id).await;
        assert!(
            early,
            "ZIP64 member must finish before the middle arrives: {descriptor:?}"
        );
        assert!(incomplete);
        assert_eq!(
            buffered, 0,
            "tail must be persisted, not parked in write backlog"
        );
        assert_eq!(pipeline.direct_unpack.counters().consumed, 1);
        let done = tokio::time::timeout(Duration::from_secs(30), pipeline.extract_done_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let ExtractionDone::FullSet { result, .. } = done else {
            panic!("full ZIP set expected")
        };
        assert!(result.is_ok());
        for (name, expected) in members {
            assert_eq!(
                std::fs::read(pipeline.extraction_staging_dir(job_id).join(name)).unwrap(),
                expected
            );
        }
        assert_eq!(pipeline.write_buffered_bytes, 0);
    }
}

#[tokio::test]
async fn zip64_par2_repairs_payload_directory_and_missing_articles() {
    for damage in 0..4 {
        let temp = TempDir::new().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
        let job_id = JobId(43001);
        let (original, members) = zip64_fixture(Some(true));
        let id = zip_job(&mut pipeline, job_id, &original).await;
        let par2 = build_repairable_par2_set_for_files(&[(ARCHIVE_NAME, &original)], 64 * 1024, 2);
        install_test_par2_runtime(&mut pipeline, job_id, par2.clone(), &[]);
        let mut damaged = original.clone();
        let count = damaged.len().div_ceil(ARTICLE_BYTES);
        let missing = match damage {
            0 => {
                damaged[4096] ^= 0x7f;
                None
            }
            1 => {
                let offset = damaged.len() - 55;
                damaged[offset] ^= 0x7f;
                None
            }
            2 => Some(4),
            _ => Some(count - 1),
        };
        if missing != Some(count - 1) {
            land_article(&mut pipeline, id, &damaged, count - 1).await;
        }
        for number in 0..count - 1 {
            if missing != Some(number) {
                land_article(&mut pipeline, id, &damaged, number).await;
            }
        }
        if missing.is_none() {
            reap_zip(&mut pipeline, job_id).await;
        }

        let working = pipeline.jobs[&job_id].working_dir.clone();
        let mut options = par2_rs::Par2RepairerOptions::new(working.clone(), Vec::new());
        options.file_set = Some(par2);
        options.repair = false;
        let analysis = par2_rs::Par2Repairer::new(options.clone())
            .verify_or_repair()
            .unwrap();
        pipeline.decide_direct_unpack_before_repair(job_id, Some(&analysis.verification));
        options.repair = true;
        let repair = par2_rs::Par2Repairer::new(options)
            .verify_or_repair()
            .map_err(|e| e.to_string());
        pipeline.settle_direct_unpack_after_repair(job_id, true, &repair);
        pipeline
            .direct_unpack_shutdown("ZIP64 repair test teardown")
            .await;
        assert!(repair.is_ok(), "{:?}", repair.as_ref().err());
        assert!(matches!(
            repair.unwrap().status,
            par2_rs::Par2RepairStatus::Repaired
        ));
        assert_eq!(std::fs::read(working.join(ARCHIVE_NAME)).unwrap(), original);

        // Repair can supply the first complete image of a file whose articles
        // never all arrived. Discover its topology from that repaired image.
        pipeline.try_update_7z_topology(job_id, id);

        pipeline
            .extract_simple_archive(job_id, ARCHIVE_NAME, SimpleArchiveKind::Zip)
            .await
            .unwrap();
        let done = tokio::time::timeout(Duration::from_secs(30), pipeline.extract_done_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let ExtractionDone::FullSet { result, .. } = done else {
            panic!("full ZIP set expected")
        };
        assert!(result.is_ok(), "{:?}", result.as_ref().err());
        for (name, expected) in members {
            assert_eq!(
                blake3::hash(
                    &std::fs::read(pipeline.extraction_staging_dir(job_id).join(name)).unwrap()
                ),
                blake3::hash(&expected)
            );
        }
    }
}

#[tokio::test]
async fn zip64_chase_rearms_after_pause_and_discards_duplicate_rewrites() {
    use crate::pipeline::direct_unpack::wiring::{AbortLatch, DemotionReason};
    let temp = TempDir::new().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job_id = JobId(43002);
    let (bytes, _) = zip64_fixture(Some(true));
    let id = zip_job(&mut pipeline, job_id, &bytes).await;
    let count = bytes.len().div_ceil(ARTICLE_BYTES);
    land_article(&mut pipeline, id, &bytes, count - 1).await;
    land_article(&mut pipeline, id, &bytes, 0).await;
    pipeline.direct_unpack_abort_job(
        job_id,
        "paused",
        AbortLatch::Retryable,
        DemotionReason::DownloadEnded,
    );
    // Reap the old worker before reusing its staging directory.
    pipeline.direct_unpack_shutdown("join paused worker").await;
    land_article(&mut pipeline, id, &bytes, 1).await;
    let rearmed = pipeline.direct_unpack.is_armed(job_id, ARCHIVE_NAME);
    let ranges = pipeline
        .direct_unpack
        .armed_coverage(job_id, ARCHIVE_NAME)
        .map(|c| c.readable_at(0, ((count - 1) * ARTICLE_BYTES) as u64));
    // A duplicate may rewrite bytes already read. Its chase must be discarded
    // even if the part CRC validates the new article.
    land_article(&mut pipeline, id, &bytes, 0).await;
    let discarded = !pipeline.direct_unpack.is_armed(job_id, ARCHIVE_NAME);
    pipeline
        .direct_unpack_shutdown("ZIP64 lifecycle test teardown")
        .await;
    assert!(rearmed);
    assert!(
        ranges.unwrap().unwrap() > 0,
        "rearming must restore committed tail ranges"
    );
    assert!(discarded);
    assert_eq!(pipeline.direct_unpack.counters().armed, 2);
}
