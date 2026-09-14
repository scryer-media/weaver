use super::*;

#[test]
fn started_member_names_excludes_orphan_split_continuations() {
    let payload = b"movie-payload-spanning-three-volumes";
    let crc = checksum::crc32(payload);
    let first = &payload[..12];
    let last = &payload[24..];

    let mut vol0 = Vec::new();
    vol0.extend_from_slice(&TEST_RAR5_SIG);
    vol0.extend_from_slice(&build_test_rar_main_header(0x0001, None));
    vol0.extend_from_slice(&build_test_rar_file_header(
        "MOVIE.m2ts",
        0x0010,
        first.len() as u64,
        payload.len() as u64,
        None,
    ));
    vol0.extend_from_slice(first);
    vol0.extend_from_slice(&build_test_rar_end_header(true));

    let mut vol2 = Vec::new();
    vol2.extend_from_slice(&TEST_RAR5_SIG);
    vol2.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(2)));
    vol2.extend_from_slice(&build_test_rar_file_header(
        "MOVIE.m2ts",
        0x0008,
        last.len() as u64,
        payload.len() as u64,
        Some(crc),
    ));
    vol2.extend_from_slice(last);
    vol2.extend_from_slice(&build_test_rar_end_header(false));

    // Volume 1 is missing: the member's continuation in volume 2 cannot be
    // linked to its base entry, so it surfaces as a second entry with the
    // same path. Path-collision checks must not treat that as a collision.
    let mut archive = unrar_rs::RarArchive::open(std::io::Cursor::new(vol0)).unwrap();
    archive
        .add_volume(2, Box::new(std::io::Cursor::new(vol2)))
        .unwrap();

    let names = archive.member_names();
    assert_eq!(
        names.iter().filter(|name| **name == "MOVIE.m2ts").count(),
        2,
        "gapped volume set should surface the base entry and the orphan continuation"
    );
    assert_eq!(
        archive.started_member_names(),
        vec!["MOVIE.m2ts"],
        "collision checks must see only the started entry"
    );
}

#[tokio::test]
async fn nested_scan_detects_obfuscated_split_7z_archives_from_staging() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(100_712);
    let staging_dir = temp_dir.path().join("nested-obfuscated-7z");
    tokio::fs::create_dir_all(&staging_dir).await.unwrap();

    let fixture_files = sevenz_fixture_bytes("generated_split_store_plain.7z");
    let obfuscated_files: Vec<(String, Vec<u8>)> = fixture_files
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("51273aad56a8b904e96928935278a627.1{}", index),
                bytes.clone(),
            )
        })
        .collect();
    for (filename, bytes) in &obfuscated_files {
        tokio::fs::write(staging_dir.join(filename), bytes)
            .await
            .unwrap();
    }

    let mut state = minimal_job_state(job_id, "Nested Obfuscated 7z", temp_dir.path().join("wd"));
    state.staging_dir = Some(staging_dir);
    pipeline.jobs.insert(job_id, state);
    pipeline.job_order.push(job_id);

    assert!(matches!(
        pipeline
            .maybe_start_nested_extraction(job_id)
            .await
            .unwrap(),
        crate::pipeline::completion::NestedExtractionDecision::Started
    ));

    let state = pipeline.jobs.get(&job_id).unwrap();
    let topology = state
        .assembly
        .archive_topology_for("51273aad56a8b904e96928935278a627")
        .unwrap();
    assert_eq!(
        topology.archive_type,
        crate::jobs::assembly::ArchiveType::SevenZip
    );
    assert_eq!(topology.complete_volumes.len(), obfuscated_files.len());
    for file_index in 0..obfuscated_files.len() {
        let file = state
            .assembly
            .file(NzbFileId {
                job_id,
                file_index: file_index as u32,
            })
            .unwrap();
        assert!(matches!(
            file.role(),
            weaver_model::files::FileRole::Unknown
        ));
        assert!(matches!(
            pipeline.classified_role_for_file(job_id, file),
            weaver_model::files::FileRole::SevenZipSplit { .. }
        ));
        assert_eq!(
            pipeline
                .detected_archive_identity(job_id, file.file_id())
                .map(|detected| detected.set_name.as_str()),
            Some("51273aad56a8b904e96928935278a627")
        );
    }
}

#[tokio::test]
async fn split_files_register_topology_when_completed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30077);
    let files = [
        ("archive.001".to_string(), b"split-a".to_vec()),
        ("archive.002".to_string(), b"split-b".to_vec()),
    ];
    let spec = JobSpec {
        name: "Split Topology Registration".to_string(),
        password: None,
        total_bytes: files.iter().map(|(_, bytes)| bytes.len() as u64).sum(),
        category: None,
        metadata: vec![],
        files: files
            .iter()
            .enumerate()
            .map(|(index, (filename, bytes))| FileSpec {
                filename: filename.clone(),
                role: FileRole::from_filename(filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: bytes.len() as u32,
                    message_id: format!("split-register-{index}@example.com"),
                }],
            })
            .collect(),
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    let state = pipeline.jobs.get(&job_id).unwrap();
    let topology = state
        .assembly
        .archive_topology_for("archive")
        .expect("split topology should be registered");
    assert_eq!(
        topology.archive_type,
        crate::jobs::assembly::ArchiveType::Split
    );
    assert_eq!(topology.expected_volume_count, Some(files.len() as u32));
    assert_eq!(topology.complete_volumes.len(), files.len());
    assert!(topology.volume_map.contains_key("archive.001"));
    assert!(topology.volume_map.contains_key("archive.002"));
    assert!(matches!(
        state.assembly.set_extraction_readiness("archive"),
        crate::jobs::assembly::ExtractionReadiness::Ready
    ));
}

/// A split suffix is a declaration the post makes, and the topology keeps it
/// as declared: a hole the recovery data later fills must restore the true
/// count, so the count is never clamped to the parts in hand. What the
/// declaration must never buy is work proportional to itself. Every consumer
/// of `expected_volume_count` and of a member's volume range walks it only
/// until the first missing volume, and nothing materializes the range, so a
/// suffix in the hundreds of millions costs the same as one in the tens.
#[tokio::test]
async fn sevenz_split_suffix_far_past_the_named_parts_is_cheap_to_hold() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30078);
    // The second suffix declares a billion volumes; the post names two.
    let files = [
        ("payload.7z.001".to_string(), b"split-a".to_vec()),
        ("payload.7z.999999999".to_string(), b"split-b".to_vec()),
    ];
    let spec = JobSpec {
        name: "Inflated Split Suffix".to_string(),
        password: None,
        total_bytes: files.iter().map(|(_, bytes)| bytes.len() as u64).sum(),
        category: None,
        metadata: vec![],
        files: files
            .iter()
            .enumerate()
            .map(|(index, (filename, bytes))| FileSpec {
                filename: filename.clone(),
                role: FileRole::from_filename(filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: bytes.len() as u32,
                    message_id: format!("split-inflated-{index}@example.com"),
                }],
            })
            .collect(),
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    let started = std::time::Instant::now();
    let state = pipeline.jobs.get(&job_id).unwrap();
    let topology = state
        .assembly
        .archive_topology_for("payload.7z")
        .expect("7z split topology should be registered");
    assert_eq!(
        topology.archive_type,
        crate::jobs::assembly::ArchiveType::SevenZip
    );
    assert_eq!(topology.expected_volume_count, Some(999_999_999));
    assert_eq!(topology.volume_map.len(), 2);
    assert!(
        topology
            .members
            .iter()
            .all(|member| member.last_volume == 999_999_998),
        "members: {:?}",
        topology.members
    );
    // Volume 1 is the first hole, so the set is not extractable, and the
    // completion check sees the same hole. Both answers come from the first
    // missing volume, not from a walk to the declared count.
    assert!(!matches!(
        state.assembly.set_extraction_readiness("payload.7z"),
        crate::jobs::assembly::ExtractionReadiness::Ready
    ));
    assert!(pipeline.job_has_sevenz_set_waiting_for_absent_volumes(job_id));
    assert!(
        started.elapsed() < std::time::Duration::from_secs(1),
        "holding a declared count must not cost work proportional to it"
    );
}
