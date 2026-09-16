use super::*;
use crate::pipeline::completion::finalize::SimpleArchiveKind;
use std::io::Write;

const ARTICLE: usize = 64 * 1024;

mod lifecycle;
mod split;
use lifecycle::lifecycle;

type MemberFiles = Vec<(String, Vec<u8>)>;

fn fixture(kind: SimpleArchiveKind) -> (String, Vec<u8>, MemberFiles) {
    let mut random = 0x9812abcd_u32;
    let payload: Vec<u8> = (0..3 * 1024 * 1024)
        .map(|_| {
            random ^= random << 13;
            random ^= random >> 17;
            random ^= random << 5;
            random as u8
        })
        .collect();
    let tar = matches!(
        kind,
        SimpleArchiveKind::Tar
            | SimpleArchiveKind::TarGz
            | SimpleArchiveKind::TarBz2
            | SimpleArchiveKind::TarXz
    );
    let members = if tar {
        vec![
            ("first.bin".to_string(), vec![31; 8192]),
            ("payload.bin".to_string(), payload),
        ]
    } else {
        vec![("payload.bin".to_string(), payload)]
    };
    let input = if tar {
        let mut builder = tar::Builder::new(Vec::new());
        for (name, bytes) in &members {
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, name, bytes.as_slice())
                .unwrap();
        }
        builder.into_inner().unwrap()
    } else {
        members[0].1.clone()
    };
    let (suffix, bytes) = match kind {
        SimpleArchiveKind::Tar => ("tar", input),
        SimpleArchiveKind::TarGz | SimpleArchiveKind::Gz => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            encoder.write_all(&input).unwrap();
            (
                if tar { "tar.gz" } else { "bin.gz" },
                encoder.finish().unwrap(),
            )
        }
        SimpleArchiveKind::TarBz2 | SimpleArchiveKind::Bzip2 => {
            let mut encoder = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::fast());
            encoder.write_all(&input).unwrap();
            (
                if tar { "tar.bz2" } else { "bin.bz2" },
                encoder.finish().unwrap(),
            )
        }
        SimpleArchiveKind::TarXz | SimpleArchiveKind::Xz => {
            let mut encoder = liblzma::write::XzEncoder::new(Vec::new(), 0);
            encoder.write_all(&input).unwrap();
            (
                if tar { "tar.xz" } else { "bin.xz" },
                encoder.finish().unwrap(),
            )
        }
        SimpleArchiveKind::Deflate => {
            let mut encoder =
                flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::fast());
            encoder.write_all(&input).unwrap();
            ("bin.deflate", encoder.finish().unwrap())
        }
        SimpleArchiveKind::Brotli => {
            let mut output = Vec::new();
            {
                let mut encoder = brotli::CompressorWriter::new(&mut output, 4096, 1, 18);
                encoder.write_all(&input).unwrap();
            }
            ("bin.br", output)
        }
        SimpleArchiveKind::Zstd => (
            "bin.zst",
            zstd::stream::encode_all(input.as_slice(), 1).unwrap(),
        ),
        _ => panic!("not a sequential single-file fixture"),
    };
    (format!("payload.{suffix}"), bytes, members)
}

async fn insert_stream(pipeline: &mut Pipeline, job: JobId, files: &[(String, Vec<u8>)]) {
    let mut spec = rar_job_spec("Sequential archives", files);
    for (index, (_, bytes)) in files.iter().enumerate() {
        spec.files[index].segments = bytes
            .chunks(ARTICLE)
            .enumerate()
            .map(|(i, part)| {
                segment_spec! { number: i as u32, bytes: part.len() as u32,
                message_id: format!("stream-{index}-{i}@test"), }
            })
            .collect();
    }
    insert_active_job(pipeline, job, spec).await;
}

async fn land(pipeline: &mut Pipeline, id: NzbFileId, name: &str, bytes: &[u8], number: usize) {
    let start = number * ARTICLE;
    submit_decoded_segment(
        pipeline,
        id,
        number as u32,
        start as u64,
        &bytes[start..(start + ARTICLE).min(bytes.len())],
        name,
        None,
    )
    .await;
}

async fn wait_for_output(path: &std::path::Path, minimum: u64) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if std::fs::metadata(path).is_ok_and(|m| m.len() >= minimum) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn finish(pipeline: &mut Pipeline, job: JobId, set: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while pipeline.direct_unpack.is_armed(job, set) && tokio::time::Instant::now() < deadline {
        pipeline.reap_direct_unpack().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    pipeline
        .direct_unpack_shutdown("sequential test cleanup")
        .await;
}

async fn extracted(pipeline: &mut Pipeline, job: JobId, expected: &[(String, Vec<u8>)]) {
    let done = tokio::time::timeout(Duration::from_secs(20), pipeline.extract_done_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let ExtractionDone::FullSet { result, .. } = done else {
        panic!("expected full set")
    };
    assert!(result.is_ok(), "{:?}", result.as_ref().err());
    for (name, bytes) in expected {
        let actual = std::fs::read(pipeline.extraction_staging_dir(job).join(name)).unwrap();
        assert_eq!(actual.len(), bytes.len());
        assert_eq!(blake3::hash(&actual), blake3::hash(bytes));
    }
}

async fn overlaps_download(kind: SimpleArchiveKind) {
    let temp = TempDir::new().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job = JobId(43100);
    let (name, bytes, members) = fixture(kind);
    insert_stream(&mut pipeline, job, &[(name.clone(), bytes.clone())]).await;
    let id = NzbFileId {
        job_id: job,
        file_index: 0,
    };
    let count = bytes.len().div_ceil(ARTICLE);
    // No tail or archive-size hint is available when output first appears.
    for number in 0..count / 2 {
        land(&mut pipeline, id, &name, &bytes, number).await;
    }
    let early = wait_for_output(
        &pipeline
            .direct_unpack_staging_dir(job, &name)
            .join(&members[0].0),
        8192,
    )
    .await;
    let incomplete = !pipeline.jobs[&job].assembly.file(id).unwrap().is_complete();
    // Reversed remainder tests sparse coverage without leaking uncommitted holes.
    for number in (count / 2..count).rev() {
        land(&mut pipeline, id, &name, &bytes, number).await;
    }
    finish(&mut pipeline, job, &name).await;
    assert!(early, "{kind:?} must write output before the tail arrives");
    assert!(incomplete);
    assert_eq!(pipeline.direct_unpack.counters().consumed, 1, "{kind:?}");
    extracted(&mut pipeline, job, &members).await;
    assert_eq!(pipeline.write_buffered_bytes, 0);
}

async fn repairs_damage_and_missing_articles(kind: SimpleArchiveKind) {
    let (name, original, members) = fixture(kind);
    for damage in 0..5 {
        let temp = TempDir::new().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
        let job = JobId(43101);
        insert_stream(&mut pipeline, job, &[(name.clone(), original.clone())]).await;
        let id = NzbFileId {
            job_id: job,
            file_index: 0,
        };
        let par2 = build_repairable_par2_set_for_files(&[(&name, &original)], ARTICLE as u64, 2);
        install_test_par2_runtime(&mut pipeline, job, par2.clone(), &[]);
        let mut bytes = original.clone();
        let count = bytes.len().div_ceil(ARTICLE);
        let missing = match damage {
            0 => {
                bytes[0] ^= 0x7f;
                None
            }
            1 => {
                bytes[ARTICLE + 100] ^= 0x7f;
                None
            }
            2 => {
                let last = bytes.len() - 1;
                bytes[last] ^= 0x7f;
                None
            }
            3 => Some(count / 2),
            _ => Some(count - 1),
        };
        for number in 0..count {
            if Some(number) != missing {
                land(&mut pipeline, id, &name, &bytes, number).await;
            }
        }
        let working = pipeline.jobs[&job].working_dir.clone();
        let mut options = par2_rs::Par2RepairerOptions::new(working.clone(), Vec::new());
        options.file_set = Some(par2);
        options.repair = false;
        let analysis = par2_rs::Par2Repairer::new(options.clone())
            .verify_or_repair()
            .unwrap();
        pipeline.decide_direct_unpack_before_repair(job, Some(&analysis.verification));
        options.repair = true;
        let repair = par2_rs::Par2Repairer::new(options)
            .verify_or_repair()
            .map_err(|e| e.to_string());
        pipeline.settle_direct_unpack_after_repair(job, true, &repair);
        finish(&mut pipeline, job, &name).await;
        assert!(
            repair.is_ok(),
            "{kind:?}/{damage}: {:?}",
            repair.as_ref().err()
        );
        assert!(matches!(
            repair.unwrap().status,
            par2_rs::Par2RepairStatus::Repaired
        ));
        assert_eq!(std::fs::read(working.join(&name)).unwrap(), original);
        pipeline.try_update_7z_topology(job, id);
        pipeline
            .extract_simple_archive(job, &name, kind)
            .await
            .unwrap();
        extracted(&mut pipeline, job, &members).await;
        assert_eq!(pipeline.write_buffered_bytes, 0);
    }
}

macro_rules! format_tests {
    ($module:ident, $kind:ident) => {
        mod $module {
            use super::*;
            #[tokio::test]
            async fn unpacks_during_download() {
                overlaps_download(SimpleArchiveKind::$kind).await;
            }
            #[tokio::test]
            async fn par2_repairs_headers_payload_trailers_and_missing_articles() {
                repairs_damage_and_missing_articles(SimpleArchiveKind::$kind).await;
            }
            #[tokio::test]
            async fn pause_rearms_and_duplicate_rewrite_discards_output() {
                lifecycle(SimpleArchiveKind::$kind).await;
            }
        }
    };
}
format_tests!(plain_tar, Tar);
format_tests!(tar_gz, TarGz);
format_tests!(tar_bz2, TarBz2);
format_tests!(tar_xz, TarXz);
format_tests!(gzip, Gz);
format_tests!(bzip2_stream, Bzip2);
format_tests!(xz, Xz);
format_tests!(deflate, Deflate);
format_tests!(brotli_stream, Brotli);
format_tests!(zstd_stream, Zstd);
