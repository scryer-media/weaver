//! `rar_extraction` tests, part of a mechanical split of the original file.

use super::*;

#[tokio::test]
async fn extraction_reserves_member_totals_at_open() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(101);
    let counters = pipeline.phase_begin(job_id, JobPhase::Extracting, None);

    let files = build_multifile_multivolume_rar_set();
    let volume_paths = files
        .iter()
        .enumerate()
        .map(|(volume, (filename, bytes))| {
            let path = temp_dir.path().join(filename);
            std::fs::write(&path, bytes).unwrap();
            (volume as u32, path)
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut archive = unrar_rs::RarArchive::open(std::io::Cursor::new(files[0].1.clone())).unwrap();
    for (volume, (_, bytes)) in files.iter().enumerate().skip(1) {
        archive
            .add_volume(volume, Box::new(std::io::Cursor::new(bytes.clone())))
            .unwrap();
    }
    let options = unrar_rs::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };
    let output_dir = temp_dir.path().join("out");
    std::fs::create_dir_all(&output_dir).unwrap();

    // Nothing is known at phase begin — topology may not exist yet. Each
    // member's size lands when its extraction opens.
    assert_eq!(counters.total_bytes.load(Ordering::Relaxed), 0);

    let idx = archive.find_member_sanitized("E01.mkv").unwrap();
    let attempt = std::sync::Arc::new(crate::jobs::PhaseAttemptCounters::new(
        std::sync::Arc::clone(&counters),
    ));
    let (_, written_a, unpacked_a) = Pipeline::extract_rar_member_to_output(
        &mut archive,
        crate::pipeline::extraction::RarExtractionContext::new(
            &volume_paths,
            &pipeline.event_tx,
            job_id,
            "show",
            &output_dir,
            &options,
        )
        .with_phase_attempt(Some(attempt)),
        idx,
    )
    .unwrap();
    assert!(unpacked_a > 0);
    assert_eq!(written_a, unpacked_a);
    assert_eq!(counters.total_bytes.load(Ordering::Relaxed), unpacked_a);
    assert_eq!(counters.completed_bytes.load(Ordering::Relaxed), written_a);

    // A second member grows the same live total — the bar hones in, never
    // restarts.
    let idx = archive.find_member_sanitized("E02.mkv").unwrap();
    let attempt = std::sync::Arc::new(crate::jobs::PhaseAttemptCounters::new(
        std::sync::Arc::clone(&counters),
    ));
    let (_, written_b, unpacked_b) = Pipeline::extract_rar_member_to_output(
        &mut archive,
        crate::pipeline::extraction::RarExtractionContext::new(
            &volume_paths,
            &pipeline.event_tx,
            job_id,
            "show",
            &output_dir,
            &options,
        )
        .with_phase_attempt(Some(attempt)),
        idx,
    )
    .unwrap();
    assert_eq!(
        counters.total_bytes.load(Ordering::Relaxed),
        unpacked_a + unpacked_b
    );
    assert_eq!(
        counters.completed_bytes.load(Ordering::Relaxed),
        written_a + written_b
    );
}

/// `weaver_pipeline_extract_member_duration_seconds` stays absent until a
/// member has actually been extracted, then reports one observation per
/// member. Extraction is rare next to an article, so the wall clock this needs
/// is nowhere near a per-segment path.
#[tokio::test]
async fn extracting_members_records_one_wall_duration_each() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20021);

    let files = build_multifile_multivolume_rar_set();
    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec("Silver Horizon Archive", &files),
    )
    .await;

    let volume_paths = files
        .iter()
        .enumerate()
        .map(|(volume, (filename, bytes))| {
            let path = temp_dir.path().join(filename);
            std::fs::write(&path, bytes).unwrap();
            (volume as u32, path)
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut archive = unrar_rs::RarArchive::open(std::io::Cursor::new(files[0].1.clone())).unwrap();
    for (volume, (_, bytes)) in files.iter().enumerate().skip(1) {
        archive
            .add_volume(volume, Box::new(std::io::Cursor::new(bytes.clone())))
            .unwrap();
    }
    let options = unrar_rs::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };
    let output_dir = temp_dir.path().join("out");
    std::fs::create_dir_all(&output_dir).unwrap();

    // The budget is what carries the job's metrics handle into the extractor,
    // so build it exactly as the extraction task does.
    let budget = pipeline.extraction_budget(job_id, &output_dir).unwrap();
    let root = std::sync::Arc::new(
        crate::pipeline::extraction::ExtractionRoot::open(&output_dir).unwrap(),
    );

    assert!(
        pipeline
            .metrics
            .pipeline_histograms
            .snapshot()
            .extract_member_duration
            .is_none(),
        "the histogram must be absent, not an all-zero series, before the first member"
    );

    for (member, expected_count) in [("E01.mkv", 1u64), ("E02.mkv", 2u64)] {
        let idx = archive.find_member_sanitized(member).unwrap();
        Pipeline::extract_rar_member_to_output(
            &mut archive,
            crate::pipeline::extraction::RarExtractionContext::new(
                &volume_paths,
                &pipeline.event_tx,
                job_id,
                "show",
                &output_dir,
                &options,
            )
            .with_security(std::sync::Arc::clone(&root), std::sync::Arc::clone(&budget)),
            idx,
        )
        .unwrap();

        let histogram = pipeline
            .metrics
            .pipeline_histograms
            .snapshot()
            .extract_member_duration
            .expect("an extracted member turns the histogram on");
        assert_eq!(histogram.count, expected_count);
        assert_eq!(
            histogram.cumulative_counts().last().copied(),
            Some(expected_count),
            "every observation must land in a bucket"
        );
    }
}

#[tokio::test]
async fn extraction_refreshes_stale_cached_headers_for_touched_volumes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let files = build_multifile_multivolume_rar_set();

    let mut good_archive =
        unrar_rs::RarArchive::open(std::io::Cursor::new(files[0].1.clone())).unwrap();
    good_archive
        .add_volume(1, Box::new(std::io::Cursor::new(files[1].1.clone())))
        .unwrap();
    good_archive
        .add_volume(2, Box::new(std::io::Cursor::new(files[2].1.clone())))
        .unwrap();
    good_archive
        .add_volume(3, Box::new(std::io::Cursor::new(files[3].1.clone())))
        .unwrap();

    let mut cached = serde_json::to_value(good_archive.export_headers()).unwrap();
    let members = cached["members"].as_array_mut().unwrap();
    let e02 = members
        .iter_mut()
        .find(|member| member["name"] == "E02.mkv")
        .expect("cached snapshot should contain E02");
    for segment in e02["segments"].as_array_mut().unwrap() {
        segment["volume_index"] = serde_json::json!(0);
    }
    let stale_headers = rmp_serde::to_vec(
        &serde_json::from_value::<unrar_rs::CachedArchiveHeaders>(cached).unwrap(),
    )
    .unwrap();

    let volume_paths = files
        .iter()
        .enumerate()
        .map(|(volume, (filename, bytes))| {
            let path = temp_dir.path().join(filename);
            std::fs::write(&path, bytes).unwrap();
            (volume as u32, path)
        })
        .collect::<std::collections::BTreeMap<_, _>>();

    let options = unrar_rs::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };

    let output_dir_without_refresh = temp_dir.path().join("without-refresh");
    std::fs::create_dir_all(&output_dir_without_refresh).unwrap();
    let mut archive_without_refresh = Pipeline::open_rar_archive_from_snapshot_or_disk(
        crate::pipeline::extraction::RarArchiveSnapshotOpenRequest {
            set_name: "show",
            volume_paths: volume_paths.clone(),
            password_candidates: Vec::new(),
            cached_headers: Some(stale_headers.clone()),
            shared_kdf_cache: std::sync::Arc::new(unrar_rs::crypto::KdfCache::new()),
            open_mode: crate::pipeline::extraction::RarArchiveOpenMode::AttachOnly,
            requested_members: None,
            already_extracted: None,
            budget: None,
        },
    )
    .unwrap()
    .value;
    let idx_without_refresh = archive_without_refresh
        .find_member_sanitized("E02.mkv")
        .unwrap();
    let error = Pipeline::extract_rar_member_to_output(
        &mut archive_without_refresh,
        crate::pipeline::extraction::RarExtractionContext::new(
            &volume_paths,
            &pipeline.event_tx,
            JobId(40100),
            "show",
            &output_dir_without_refresh,
            &options,
        ),
        idx_without_refresh,
    )
    .unwrap_err();
    assert!(
        error.contains("CRC mismatch")
            || error.to_ascii_lowercase().contains("checksum")
            || error.contains("not registered")
            || error.contains("unavailable"),
        "unexpected stale-header extraction error: {error}"
    );

    let output_dir_with_refresh = temp_dir.path().join("with-refresh");
    std::fs::create_dir_all(&output_dir_with_refresh).unwrap();
    let mut archive_with_refresh = Pipeline::open_rar_archive_from_snapshot_or_disk(
        crate::pipeline::extraction::RarArchiveSnapshotOpenRequest {
            set_name: "show",
            volume_paths: volume_paths.clone(),
            password_candidates: Vec::new(),
            cached_headers: Some(stale_headers),
            shared_kdf_cache: std::sync::Arc::new(unrar_rs::crypto::KdfCache::new()),
            open_mode: crate::pipeline::extraction::RarArchiveOpenMode::RefreshProvidedVolumes,
            requested_members: None,
            already_extracted: None,
            budget: None,
        },
    )
    .unwrap()
    .value;
    let idx_with_refresh = archive_with_refresh
        .find_member_sanitized("E02.mkv")
        .unwrap();
    let (member_name, _, _) = Pipeline::extract_rar_member_to_output(
        &mut archive_with_refresh,
        crate::pipeline::extraction::RarExtractionContext::new(
            &volume_paths,
            &pipeline.event_tx,
            JobId(40101),
            "show",
            &output_dir_with_refresh,
            &options,
        ),
        idx_with_refresh,
    )
    .unwrap();

    assert_eq!(member_name, "E02.mkv");
    assert_eq!(
        std::fs::read(output_dir_with_refresh.join("E02.mkv")).unwrap(),
        b"episode-b-payload"
    );
}

#[tokio::test]
async fn recompute_rar_set_state_refreshes_cached_span_when_facts_contradict_snapshot() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40102);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Facts Heal Stale Cached Span", &files);

    insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let mut good_archive =
        unrar_rs::RarArchive::open(std::io::Cursor::new(files[0].1.clone())).unwrap();
    good_archive
        .add_volume(1, Box::new(std::io::Cursor::new(files[1].1.clone())))
        .unwrap();
    good_archive
        .add_volume(2, Box::new(std::io::Cursor::new(files[2].1.clone())))
        .unwrap();
    good_archive
        .add_volume(3, Box::new(std::io::Cursor::new(files[3].1.clone())))
        .unwrap();

    let mut cached = serde_json::to_value(good_archive.export_headers()).unwrap();
    let members = cached["members"].as_array_mut().unwrap();
    let e01 = members
        .iter_mut()
        .find(|member| member["name"] == "E01.mkv")
        .expect("cached snapshot should contain E01");
    let first_segment = e01["segments"]
        .as_array()
        .and_then(|segments| segments.first())
        .cloned()
        .expect("cached snapshot should include the first E01 segment");
    e01["segments"] = serde_json::json!([first_segment]);
    e01["split_after"] = serde_json::json!(false);

    let stale_headers = rmp_serde::to_vec(
        &serde_json::from_value::<unrar_rs::CachedArchiveHeaders>(cached).unwrap(),
    )
    .unwrap();

    pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .expect("RAR set state should exist after all volumes complete")
        .cached_headers = Some(stale_headers.clone());
    pipeline
        .db
        .save_archive_headers(job_id, "show", &stale_headers)
        .unwrap();

    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    let topology = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for("show"))
        .cloned()
        .expect("RAR topology should exist after recompute");
    let e01 = topology
        .members
        .iter()
        .find(|member| member.name == "E01.mkv")
        .expect("recomputed topology should contain E01");
    assert_eq!(e01.first_volume, 0);
    assert_eq!(e01.last_volume, 1);

    let healed_headers = pipeline
        .load_rar_snapshot(job_id, "show")
        .expect("recompute should persist a healed snapshot");
    let healed_archive =
        unrar_rs::RarArchive::deserialize_headers_with_password(&healed_headers, None::<String>)
            .unwrap();
    let healed_e01 = healed_archive
        .metadata()
        .members
        .into_iter()
        .find(|member| member.name == "E01.mkv")
        .expect("healed snapshot should contain E01");
    assert_eq!(healed_e01.volumes.first_volume, 0);
    assert_eq!(healed_e01.volumes.last_volume, 1);
}

#[test]
fn cached_rar_snapshot_alignment_treats_incomplete_tail_growth_as_staleness() {
    let files = build_multifile_multivolume_rar_set();

    let mut cached_archive =
        unrar_rs::RarArchive::open(std::io::Cursor::new(files[0].1.clone())).unwrap();
    cached_archive
        .add_volume(1, Box::new(std::io::Cursor::new(files[1].1.clone())))
        .unwrap();
    cached_archive
        .add_volume(2, Box::new(std::io::Cursor::new(files[2].1.clone())))
        .unwrap();

    let mut facts = BTreeMap::new();
    for (volume, (_, bytes)) in files.iter().enumerate() {
        let mut parsed =
            unrar_rs::RarArchive::parse_volume_facts(std::io::Cursor::new(bytes.clone()), None)
                .unwrap();
        if volume == 3 {
            parsed.more_volumes = true;
            let member = parsed
                .members
                .iter_mut()
                .find(|member| member.name == "E02.mkv")
                .expect("volume 3 should contain E02");
            member.split_after = true;
            member.data_crc32 = None;
        }
        facts.insert(volume as u32, parsed);
    }

    assert_eq!(
        crate::pipeline::archive::topology::cached_rar_snapshot_alignment_with_volume_facts(
            &facts,
            &cached_archive,
        ),
        crate::pipeline::archive::topology::CachedRarSnapshotAlignment::StaleGrowth
    );
}

#[tokio::test]
async fn recompute_rar_set_state_rebuilds_from_live_volumes_when_cached_headers_are_incoherent() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40104);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Facts Heal Incoherent Cached Headers", &files);

    insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let mut good_archive =
        unrar_rs::RarArchive::open(std::io::Cursor::new(files[0].1.clone())).unwrap();
    good_archive
        .add_volume(1, Box::new(std::io::Cursor::new(files[1].1.clone())))
        .unwrap();
    good_archive
        .add_volume(2, Box::new(std::io::Cursor::new(files[2].1.clone())))
        .unwrap();
    good_archive
        .add_volume(3, Box::new(std::io::Cursor::new(files[3].1.clone())))
        .unwrap();

    let mut cached = serde_json::to_value(good_archive.export_headers()).unwrap();
    cached["members"] = serde_json::json!([]);
    let stale_headers = rmp_serde::to_vec(
        &serde_json::from_value::<unrar_rs::CachedArchiveHeaders>(cached).unwrap(),
    )
    .unwrap();

    pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .expect("RAR set state should exist after all volumes complete")
        .cached_headers = Some(stale_headers.clone());
    pipeline
        .db
        .save_archive_headers(job_id, "show", &stale_headers)
        .unwrap();

    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("RAR plan should exist after recompute");
    assert!(
        !matches!(
            plan.phase,
            crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes
        ),
        "incoherent cached headers should be healed from live volumes",
    );
    assert!(
        plan.ready_members
            .iter()
            .any(|member| member.name == "E01.mkv"),
        "healed plan should restore live member readiness",
    );

    let healed_headers = pipeline
        .load_rar_snapshot(job_id, "show")
        .expect("recompute should persist healed headers");
    let healed_archive =
        unrar_rs::RarArchive::deserialize_headers_with_password(&healed_headers, None::<String>)
            .unwrap();
    assert!(
        !healed_archive.metadata().members.is_empty(),
        "healed headers should no longer carry the incoherent empty-member snapshot",
    );
}

#[tokio::test]
async fn rar_password_fallback_from_nzb_meta_is_validated_before_remembering_cached_headers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40104);
    let set_name = "video";
    let files = vec![(
        "video.rar".to_string(),
        rar5_fixture_bytes("rar5_hp_store.rar"),
    )];
    let mut spec = rar_job_spec("RAR Password Fallback From NZB Meta", &files);
    spec.password = Some("wrong-password".to_string());
    insert_active_job_with_persisted_nzb(
        &mut pipeline,
        job_id,
        spec,
        sample_nzb_zstd_with_password("secretpass"),
    )
    .await;

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;

    let key = (job_id, set_name.to_string());
    assert!(
        !pipeline.archive_password_winners.contains_key(&key),
        "topology/header open alone must not remember a password winner"
    );

    let cached_headers = pipeline
        .load_rar_snapshot(job_id, set_name)
        .expect("encrypted volume-zero topology should persist cached headers");
    let volume_paths = pipeline.volume_paths_for_rar_set(job_id, set_name);
    let restart_candidates = pipeline.archive_password_candidates_for_set(job_id, set_name);
    assert_eq!(
        restart_candidates
            .first()
            .map(|candidate| candidate.source().as_str()),
        Some("explicit")
    );

    let selection = Pipeline::open_rar_archive_for_extraction_with_password_candidates(
        crate::pipeline::extraction::RarExtractionOpenRequest {
            set_name,
            volume_paths,
            password_candidates: restart_candidates.clone(),
            cached_headers: Some(cached_headers),
            shared_kdf_cache: std::sync::Arc::new(unrar_rs::crypto::KdfCache::new()),
            open_mode: crate::pipeline::extraction::RarArchiveOpenMode::AttachOnly,
            requested_members: &[],
            already_extracted: None,
            budget: None,
        },
    )
    .expect("cached headers should validate extraction with the fallback winner");

    assert!(selection.validated_password.is_some());
    assert!(
        !selection.archive.metadata().members.is_empty(),
        "cached-header reopen should deserialize member metadata"
    );
    pipeline.remember_archive_password_winner(
        job_id,
        set_name,
        selection.validated_password.as_deref(),
        &restart_candidates,
    );
    let winner = pipeline
        .archive_password_winners
        .get(&key)
        .expect("validated extraction should remember the fallback winner");
    assert_eq!(winner.source().as_str(), "nzb_meta");
}

#[tokio::test]
async fn rar_password_member_encrypted_fallback_from_nzb_meta_is_validated_by_probe() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40105);
    let set_name = "video";
    let files = vec![(
        "video.rar".to_string(),
        rar5_fixture_bytes("rar5_enc_store.rar"),
    )];
    let mut spec = rar_job_spec("RAR Member Password Fallback From NZB Meta", &files);
    spec.password = Some("wrong-password".to_string());
    insert_active_job_with_persisted_nzb(
        &mut pipeline,
        job_id,
        spec,
        sample_nzb_zstd_with_password("testpass123"),
    )
    .await;

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;

    let key = (job_id, set_name.to_string());
    assert!(
        !pipeline.archive_password_winners.contains_key(&key),
        "member-encrypted topology/header open must not remember a password winner"
    );

    let volume_paths = pipeline.volume_paths_for_rar_set(job_id, set_name);
    let candidates = pipeline.archive_password_candidates_for_set(job_id, set_name);
    assert_eq!(
        candidates
            .first()
            .map(|candidate| candidate.source().as_str()),
        Some("explicit")
    );

    let requested_members = vec!["small.txt".to_string()];
    let mut selection = Pipeline::open_rar_archive_for_extraction_with_password_candidates(
        crate::pipeline::extraction::RarExtractionOpenRequest {
            set_name,
            volume_paths: volume_paths.clone(),
            password_candidates: candidates.clone(),
            cached_headers: None,
            shared_kdf_cache: std::sync::Arc::new(unrar_rs::crypto::KdfCache::new()),
            open_mode: crate::pipeline::extraction::RarArchiveOpenMode::AttachOnly,
            requested_members: &requested_members,
            already_extracted: None,
            budget: None,
        },
    )
    .expect("member extraction probe should fall back to the NZB meta password");

    assert!(selection.validated_password.is_some());
    pipeline.remember_archive_password_winner(
        job_id,
        set_name,
        selection.validated_password.as_deref(),
        &candidates,
    );
    let winner = pipeline
        .archive_password_winners
        .get(&key)
        .expect("validated member extraction should remember the fallback winner");
    assert_eq!(winner.source().as_str(), "nzb_meta");

    let output_dir = temp_dir.path().join("member-password-fallback");
    std::fs::create_dir_all(&output_dir).unwrap();
    let options = unrar_rs::ExtractOptions {
        verify: true,
        password: selection.password.clone(),
        restore_owners: false,
    };
    let idx = selection
        .archive
        .find_member_sanitized("small.txt")
        .expect("fixture member should be present");
    let (member_name, _, _) = Pipeline::extract_rar_member_to_output(
        &mut selection.archive,
        crate::pipeline::extraction::RarExtractionContext::new(
            &volume_paths,
            &pipeline.event_tx,
            job_id,
            set_name,
            &output_dir,
            &options,
        ),
        idx,
    )
    .expect("extraction should use the validated fallback password");

    assert_eq!(member_name, "small.txt");
    assert_eq!(
        std::fs::read(output_dir.join("small.txt")).unwrap(),
        rar_original_fixture_bytes("small.txt")
    );
}

#[tokio::test]
async fn rar_password_member_encrypted_cached_headers_probe_after_restart() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40106);
    let set_name = "video";
    let files = vec![(
        "video.rar".to_string(),
        rar5_fixture_bytes("rar5_enc_store.rar"),
    )];
    let mut spec = rar_job_spec("RAR Member Cached Password Fallback From NZB Meta", &files);
    spec.password = Some("wrong-password".to_string());
    insert_active_job_with_persisted_nzb(
        &mut pipeline,
        job_id,
        spec,
        sample_nzb_zstd_with_password("testpass123"),
    )
    .await;

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;

    let key = (job_id, set_name.to_string());
    assert!(
        !pipeline.archive_password_winners.contains_key(&key),
        "member-encrypted topology/header open must not remember a password winner"
    );
    let cached_headers = pipeline
        .load_rar_snapshot(job_id, set_name)
        .expect("member-encrypted topology should persist cached headers");
    let volume_paths = pipeline.volume_paths_for_rar_set(job_id, set_name);
    let candidates = pipeline.archive_password_candidates_for_set(job_id, set_name);
    assert_eq!(
        candidates
            .first()
            .map(|candidate| candidate.source().as_str()),
        Some("explicit")
    );

    let requested_members = vec!["small.txt".to_string()];
    let selection = Pipeline::open_rar_archive_for_extraction_with_password_candidates(
        crate::pipeline::extraction::RarExtractionOpenRequest {
            set_name,
            volume_paths,
            password_candidates: candidates.clone(),
            cached_headers: Some(cached_headers),
            shared_kdf_cache: std::sync::Arc::new(unrar_rs::crypto::KdfCache::new()),
            open_mode: crate::pipeline::extraction::RarArchiveOpenMode::AttachOnly,
            requested_members: &requested_members,
            already_extracted: None,
            budget: None,
        },
    )
    .expect("cached headers should still probe extraction before selecting a member password");

    assert!(selection.validated_password.is_some());
    pipeline.remember_archive_password_winner(
        job_id,
        set_name,
        selection.validated_password.as_deref(),
        &candidates,
    );
    let winner = pipeline
        .archive_password_winners
        .get(&key)
        .expect("validated cached-header probe should remember the fallback winner");
    assert_eq!(winner.source().as_str(), "nzb_meta");
}

#[test]
fn rar_password_candidate_helper_stops_on_non_password_error() {
    let candidates = vec![
        crate::jobs::ArchivePasswordCandidate::new(
            crate::jobs::ArchivePasswordSource::Explicit,
            "first-secret".to_string(),
        ),
        crate::jobs::ArchivePasswordCandidate::new(
            crate::jobs::ArchivePasswordSource::NzbMeta,
            "second-secret".to_string(),
        ),
    ];
    let mut attempts = 0;

    let error = match Pipeline::try_rar_password_candidates::<(), _>(
        "candidate helper",
        &candidates,
        |_password| {
            attempts += 1;
            Err(RarPasswordAttemptError::Fatal(
                "cache decode failed".to_string(),
            ))
        },
    ) {
        Ok(_) => panic!("non-password error should fail immediately"),
        Err(error) => error,
    };

    assert_eq!(attempts, 1);
    assert_eq!(error, "candidate helper: cache decode failed");
}

#[test]
fn rar_password_candidate_helper_redacts_exhausted_candidates() {
    let candidates = vec![
        crate::jobs::ArchivePasswordCandidate::new(
            crate::jobs::ArchivePasswordSource::Explicit,
            "first-secret".to_string(),
        ),
        crate::jobs::ArchivePasswordCandidate::new(
            crate::jobs::ArchivePasswordSource::NzbMeta,
            "second-secret".to_string(),
        ),
    ];
    let mut attempts = 0;

    let error = match Pipeline::try_rar_password_candidates::<(), _>(
        "candidate helper",
        &candidates,
        |_password| {
            attempts += 1;
            Err(RarPasswordAttemptError::Rar(
                unrar_rs::RarError::InvalidPassword,
            ))
        },
    ) {
        Ok(_) => panic!("exhausted password candidates should fail"),
        Err(error) => error,
    };

    assert_eq!(attempts, 2);
    assert!(error.contains("after 2 candidate(s) from explicit,nzb_meta"));
    assert!(!error.contains("first-secret"));
    assert!(!error.contains("second-secret"));
}

#[tokio::test]
async fn completed_rar_volume_queues_refresh_without_inline_recompute() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40118);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Queued Refresh", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let (filename, bytes) = &files[0];
    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
    tokio::fs::write(working_dir.join(filename), bytes)
        .await
        .unwrap();
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .file_mut(file_id)
        .unwrap()
        .commit_segment(0, bytes.len() as u32)
        .unwrap();

    pipeline
        .refresh_archive_state_for_completed_file(job_id, file_id, true)
        .await;

    let key = (job_id, "show".to_string());
    let refresh = pipeline
        .rar_refresh_state
        .get(&key)
        .expect("RAR refresh should be queued");
    assert_eq!(
        refresh.in_flight.map(|request| request.reason),
        Some(RefreshReason::CoverageExpansion)
    );
    assert!(
        pipeline
            .rar_sets
            .get(&key)
            .and_then(|state| state.plan.as_ref())
            .is_none(),
        "file-complete RAR refresh should not rebuild the plan inline"
    );

    drain_rar_refreshes(&mut pipeline).await;
    assert!(
        pipeline
            .rar_sets
            .get(&key)
            .and_then(|state| state.plan.as_ref())
            .is_some(),
        "queued RAR refresh should apply through the orchestrator"
    );
}

#[tokio::test]
async fn rar_refresh_follow_up_covers_holey_inflight_snapshot() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40124);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Holey Refresh Follow-up", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let key = (job_id, "show".to_string());
    write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    assert_eq!(
        pipeline
            .rar_refresh_state
            .get(&key)
            .map(|state| state.refreshed_volumes.clone()),
        Some(BTreeSet::from([0]))
    );

    write_and_complete_rar_volume_without_drain(&mut pipeline, job_id, 2, &files[2].0, &files[2].1)
        .await;
    let stale_done =
        tokio::time::timeout(Duration::from_secs(5), pipeline.rar_refresh_done_rx.recv())
            .await
            .expect("holey RAR refresh result should arrive")
            .expect("RAR refresh channel should stay open");

    write_and_complete_rar_volume_without_drain(&mut pipeline, job_id, 1, &files[1].0, &files[1].1)
        .await;
    assert!(
        pipeline
            .rar_refresh_state
            .get(&key)
            .and_then(|state| state.queued)
            .is_some(),
        "volume 1 completion should queue a refresh behind the stale in-flight result"
    );

    pipeline.handle_rar_refresh_done(stale_done).await;
    {
        let refresh = pipeline
            .rar_refresh_state
            .get(&key)
            .expect("RAR refresh state should remain present");
        assert_eq!(refresh.refreshed_volumes, BTreeSet::from([0, 2]));
        assert!(
            refresh.in_flight.is_some(),
            "stale holey coverage should schedule a follow-up refresh"
        );
    }

    drain_rar_refreshes(&mut pipeline).await;
    let refresh = pipeline
        .rar_refresh_state
        .get(&key)
        .expect("RAR refresh state should remain present");
    assert_eq!(refresh.refreshed_volumes, BTreeSet::from([0, 1, 2]));

    let plan = pipeline
        .rar_sets
        .get(&key)
        .and_then(|state| state.plan.as_ref())
        .expect("RAR plan should be rebuilt after follow-up");
    assert!(
        !plan.waiting_on_volumes.contains(&1),
        "volume 1 is present and must not remain in the waiting set"
    );
    assert!(
        !plan.waiting_on_volumes.contains(&2),
        "volume 2 is present and must not remain in the waiting set"
    );
}

/// A coverage gap the plan CANNOT close must park, not respawn.
///
/// The follow-up machinery exists for absorbable facts: a holey snapshot's
/// next rebuild attaches the present volumes and the gap closes. But a
/// rebuild can also come back with a plan that has NOT absorbed every
/// registered fact — a chain the opened headers cannot extend, a volume
/// binding pointing at bytes no rebuild can attach — and an ungated
/// follow-up then respawns an identical refresh from every completion, at
/// actor speed, forever: identical inputs, identical plan, same gap. The
/// completion fingerprint parks the second identical completion; a real
/// fact change moves the fingerprint and re-arms the gap.
#[tokio::test]
async fn a_refresh_gap_the_plan_cannot_close_parks_instead_of_respawning() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40133);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Unclosable Gap Parks", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }
    let key = (job_id, "show".to_string());
    let extraction_generation = pipeline
        .rar_sets
        .get(&key)
        .expect("RAR set state should exist")
        .extraction_generation;

    // A computed result whose plan absorbed only volume 0, while the set's
    // facts hold every volume: the unclosable-gap shape, reduced to its
    // essence. Rebuilding it twice models a refresh loop whose inputs never
    // change between rounds.
    let unclosable_computed = |pipeline: &Pipeline| {
        let mut topology = pipeline
            .jobs
            .get(&job_id)
            .and_then(|state| state.assembly.archive_topology_for("show"))
            .cloned()
            .expect("RAR topology should exist after all volumes complete");
        topology.complete_volumes.retain(|volume| *volume == 0);
        ComputedRarSetState {
            plan: crate::pipeline::archive::rar_state::RarDerivedPlan {
                phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: Vec::new(),
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::from([1]),
                deletion_eligible: HashSet::new(),
                delete_decisions: BTreeMap::new(),
                topology,
                fallback_reason: None,
            },
            headers: Vec::new(),
            rebuild_source:
                crate::pipeline::archive::topology::RarTopologyRebuildSource::VolumeZero,
            // The refresh saw only volume 0 — the same starved view the plan
            // absorbed, so the facts stay unabsorbed round after round.
            integrated_volumes: BTreeSet::from([0]),
        }
    };
    let request = RarRefreshRequest {
        target_completed_volume: 2,
        reason: RefreshReason::CoverageExpansion,
    };
    let refresh_done = |pipeline: &Pipeline| RarRefreshDone {
        job_id,
        set_name: "show".to_string(),
        request,
        extraction_generation,
        result: Ok(unclosable_computed(pipeline)),
    };

    pipeline.rar_refresh_state.insert(
        key.clone(),
        RarRefreshState {
            in_flight: Some(request),
            queued: None,
            latest_completed_volume: 2,
            refreshed_volumes: BTreeSet::from([0, 1, 2]),
            structure_dirty: false,
            last_error: None,
            last_completion_fingerprint: None,
        },
    );

    // Round one: the gap is new, so a follow-up is the right call — the facts
    // grew past what this plan absorbed and a rebuild might absorb them.
    let done = refresh_done(&pipeline);
    pipeline.handle_rar_refresh_done(done).await;
    assert!(
        pipeline
            .rar_refresh_state
            .get(&key)
            .is_some_and(|state| state.in_flight.is_some()),
        "the first unabsorbed-facts completion must spawn a follow-up refresh"
    );

    // Round two lands on identical inputs and an identical plan: without the
    // park this respawns forever, at actor speed.
    let done = refresh_done(&pipeline);
    pipeline.handle_rar_refresh_done(done).await;
    let parked_fingerprint = {
        let state = pipeline
            .rar_refresh_state
            .get(&key)
            .expect("RAR refresh state should remain present");
        assert!(
            state.in_flight.is_none() && state.queued.is_none(),
            "an identical completion must park the gap instead of respawning"
        );
        state
            .last_completion_fingerprint
            .expect("a successful completion must record its fingerprint")
    };

    // A real fact change re-arms the parked gap: the fingerprint moves, so
    // the next completion follows up again instead of staying parked.
    pipeline
        .rar_sets
        .get_mut(&key)
        .expect("RAR set state should exist")
        .facts_generation += 1;
    pipeline
        .rar_refresh_state
        .get_mut(&key)
        .expect("RAR refresh state should remain present")
        .in_flight = Some(request);
    let done = refresh_done(&pipeline);
    pipeline.handle_rar_refresh_done(done).await;
    let state = pipeline
        .rar_refresh_state
        .get(&key)
        .expect("RAR refresh state should remain present");
    assert!(
        state.in_flight.is_some(),
        "a changed fact must re-arm the parked coverage gap"
    );
    assert_ne!(
        state.last_completion_fingerprint,
        Some(parked_fingerprint),
        "the re-armed completion must move the fingerprint"
    );
}

#[tokio::test]
async fn rar_refresh_follow_up_does_not_starve_covered_ready_members() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40129);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Refresh Follow-up Extraction", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 1, &files[1].0, &files[1].1).await;

    let key = (job_id, "show".to_string());
    let computed = ComputedRarSetState {
        plan: pipeline
            .rar_sets
            .get(&key)
            .and_then(|state| state.plan.as_ref())
            .cloned()
            .expect("RAR plan should exist after volumes 0-1"),
        headers: pipeline
            .load_rar_snapshot(job_id, "show")
            .expect("RAR snapshot should exist after volumes 0-1"),
        rebuild_source: crate::pipeline::archive::topology::RarTopologyRebuildSource::CachedHeaders,
        integrated_volumes: BTreeSet::from([0, 1]),
    };
    let request = RarRefreshRequest {
        target_completed_volume: 1,
        reason: RefreshReason::CoverageExpansion,
    };
    pipeline.rar_refresh_state.insert(
        key.clone(),
        RarRefreshState {
            in_flight: Some(request),
            queued: None,
            latest_completed_volume: 1,
            refreshed_volumes: BTreeSet::from([0, 1]),
            structure_dirty: false,
            last_error: None,
            last_completion_fingerprint: None,
        },
    );

    write_and_complete_rar_volume_without_drain(&mut pipeline, job_id, 2, &files[2].0, &files[2].1)
        .await;
    assert!(
        pipeline
            .rar_refresh_state
            .get(&key)
            .and_then(|state| state.queued)
            .is_some(),
        "newer completed volume should queue follow-up coverage refresh"
    );

    resume_job_downloading_for_test(&mut pipeline, job_id);
    pipeline
        .handle_rar_refresh_done(RarRefreshDone {
            job_id,
            set_name: "show".to_string(),
            request,
            extraction_generation: 0,
            result: Ok(computed),
        })
        .await;

    let set_state = pipeline.rar_sets.get(&key).expect("RAR set should remain");
    assert!(
        set_state.in_flight_members.contains("E01.mkv"),
        "member covered by refreshed volumes 0-1 should start while volume 2 refresh follows up"
    );
    assert!(
        set_state.active_workers > 0,
        "covered ready member should consume an extraction worker"
    );
    let refresh = pipeline
        .rar_refresh_state
        .get(&key)
        .expect("RAR refresh state should remain");
    assert_eq!(refresh.refreshed_volumes, BTreeSet::from([0, 1]));
    assert!(
        refresh.in_flight.is_some(),
        "follow-up coverage refresh should still be launched"
    );

    let done = tokio::time::timeout(Duration::from_secs(5), pipeline.extract_done_rx.recv())
        .await
        .expect("covered member extraction should complete")
        .expect("extraction channel should stay open");
    pipeline.handle_extraction_done(done).await;
}

#[tokio::test]
async fn rar_refresh_parks_until_volume_zero_available() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40141);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Refresh Park", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    // A mid-set volume completes first: no cached headers and no volume 0 on
    // disk, so a refresh compute is guaranteed to fail. The spawn must park
    // quietly instead of failing once per completed volume.
    write_and_complete_rar_volume_without_drain(&mut pipeline, job_id, 2, &files[2].0, &files[2].1)
        .await;

    let key = (job_id, "show".to_string());
    let refresh = pipeline
        .rar_refresh_state
        .get(&key)
        .expect("refresh state should exist after volume completion");
    assert!(
        refresh.in_flight.is_none(),
        "refresh without volume 0 or cached headers must park"
    );
    assert!(
        refresh.last_error.is_none(),
        "parked refresh is an expected state, not an error"
    );
    assert!(
        pipeline.rar_refresh_done_rx.try_recv().is_err(),
        "no refresh compute should have been spawned"
    );
    let set_state = pipeline
        .rar_sets
        .get(&key)
        .expect("RAR set state should exist");
    assert!(
        set_state.plan.is_none(),
        "no plan can exist before volume 0"
    );
    assert!(set_state.facts.contains_key(&2));

    // Volume 0 arriving re-enqueues a refresh through facts registration and
    // the first plan comes up through the normal drain path.
    write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    let set_state = pipeline
        .rar_sets
        .get(&key)
        .expect("RAR set state should exist");
    assert!(
        set_state.plan.is_some(),
        "volume 0 arrival should unlock the first plan"
    );
}

#[tokio::test]
async fn rar_refresh_done_uses_actual_refreshed_frontier() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40119);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Refresh Frontier", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 1, &files[1].0, &files[1].1).await;

    let key = (job_id, "show".to_string());
    let request = RarRefreshRequest {
        target_completed_volume: 3,
        reason: RefreshReason::CoverageExpansion,
    };
    pipeline.rar_refresh_state.insert(
        key.clone(),
        RarRefreshState {
            in_flight: Some(request),
            queued: None,
            latest_completed_volume: 3,
            refreshed_volumes: BTreeSet::from([0, 1]),
            structure_dirty: false,
            last_error: None,
            last_completion_fingerprint: None,
        },
    );

    let computed = ComputedRarSetState {
        plan: pipeline
            .rar_sets
            .get(&key)
            .and_then(|state| state.plan.as_ref())
            .cloned()
            .expect("RAR plan should exist"),
        headers: pipeline
            .load_rar_snapshot(job_id, "show")
            .expect("RAR snapshot should exist"),
        rebuild_source: crate::pipeline::archive::topology::RarTopologyRebuildSource::CachedHeaders,
        integrated_volumes: BTreeSet::from([0, 1]),
    };

    pipeline
        .handle_rar_refresh_done(RarRefreshDone {
            job_id,
            set_name: "show".to_string(),
            request,
            extraction_generation: 0,
            result: Ok(computed),
        })
        .await;

    assert_eq!(
        pipeline
            .rar_refresh_state
            .get(&key)
            .map(|state| state.refreshed_volumes.clone()),
        Some(BTreeSet::from([0, 1])),
        "refresh completion should keep the actual rebuilt frontier"
    );
}

#[tokio::test]
async fn stale_post_extraction_refresh_does_not_replace_live_rar_plan() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40126);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Stale Post Refresh", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let key = (job_id, "show".to_string());
    let stale_plan = pipeline
        .rar_sets
        .get(&key)
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("RAR plan should exist before stale refresh");
    let stale_headers = pipeline
        .load_rar_snapshot(job_id, "show")
        .expect("RAR snapshot should exist before stale refresh");
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&key)
            .expect("RAR set should exist");
        set_state.extraction_generation = 1;
        set_state
            .plan
            .as_mut()
            .expect("RAR plan should be live")
            .ready_members
            .clear();
    }
    pipeline
        .extracted_members
        .entry(job_id)
        .or_default()
        .insert("E01.mkv".to_string());

    let request = RarRefreshRequest {
        target_completed_volume: 3,
        reason: RefreshReason::PostExtraction,
    };
    pipeline.rar_refresh_state.insert(
        key.clone(),
        RarRefreshState {
            in_flight: Some(request),
            queued: None,
            latest_completed_volume: 3,
            refreshed_volumes: BTreeSet::from([0, 1, 2, 3]),
            structure_dirty: false,
            last_error: None,
            last_completion_fingerprint: None,
        },
    );

    pipeline
        .handle_rar_refresh_done(RarRefreshDone {
            job_id,
            set_name: "show".to_string(),
            request,
            extraction_generation: 0,
            result: Ok(ComputedRarSetState {
                plan: stale_plan,
                headers: stale_headers,
                rebuild_source:
                    crate::pipeline::archive::topology::RarTopologyRebuildSource::CachedHeaders,
                integrated_volumes: BTreeSet::new(),
            }),
        })
        .await;

    let set_state = pipeline.rar_sets.get(&key).expect("RAR set should remain");
    assert_eq!(set_state.extraction_generation, 1);
    assert!(
        set_state
            .plan
            .as_ref()
            .expect("live RAR plan should remain")
            .ready_members
            .is_empty(),
        "stale refresh must not replace the live plan"
    );
    assert!(
        !pipeline.rar_member_can_start_extraction(job_id, "show", "E01.mkv"),
        "already extracted member must not become ready again"
    );
    let refresh = pipeline
        .rar_refresh_state
        .get(&key)
        .expect("refresh state should remain");
    assert!(refresh.in_flight.is_none());
    assert!(refresh.queued.is_none());
}

#[tokio::test]
async fn identity_rebind_rejects_an_older_rar_refresh_snapshot() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40138);
    let files = build_multifile_multivolume_rar_set();
    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec("RAR Identity Rebind Refresh Race", &files),
    )
    .await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let key = (job_id, "show".to_string());
    let stale_plan = pipeline
        .rar_sets
        .get(&key)
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("RAR plan should exist before identity rebinding");
    let stale_headers = pipeline
        .load_rar_snapshot(job_id, "show")
        .expect("RAR snapshot should exist before identity rebinding");
    let stale_generation = pipeline
        .rar_sets
        .get(&key)
        .expect("RAR set should exist")
        .extraction_generation;
    let canonical_facts = pipeline.rar_sets.get(&key).unwrap().facts.clone();
    let request = RarRefreshRequest {
        target_completed_volume: 3,
        reason: RefreshReason::PostExtraction,
    };
    let identity_rebuild = RarRefreshRequest {
        target_completed_volume: 3,
        reason: RefreshReason::IdentityRebind,
    };
    pipeline.rar_refresh_state.insert(
        key.clone(),
        RarRefreshState {
            in_flight: Some(request),
            queued: Some(identity_rebuild),
            latest_completed_volume: 3,
            refreshed_volumes: BTreeSet::from([0, 1, 2, 3]),
            structure_dirty: true,
            last_error: None,
            last_completion_fingerprint: None,
        },
    );

    let touched_filenames = files.iter().map(|(filename, _)| filename.clone()).collect();
    pipeline.invalidate_archive_set_for_identity_rebind(job_id, "show", &touched_filenames);
    pipeline.purge_empty_rar_set_if_idle(job_id, "show");
    for (volume, (filename, _)) in files.iter().enumerate() {
        pipeline
            .persist_rar_volume_facts(
                job_id,
                "show",
                filename,
                Some(volume as u32),
                canonical_facts[&(volume as u32)].clone(),
            )
            .unwrap();
    }
    assert_eq!(
        pipeline
            .rar_sets
            .get(&key)
            .expect("RAR set should survive identity rebinding")
            .extraction_generation,
        stale_generation.saturating_add(1)
    );
    assert_eq!(
        pipeline.load_rar_snapshot(job_id, "show").as_deref(),
        Some(stale_headers.as_slice()),
        "identity rebinding retains the snapshot only as a base for rebuilding sets whose \
         volume zero may already be gone"
    );

    pipeline
        .handle_rar_refresh_done(RarRefreshDone {
            job_id,
            set_name: "show".to_string(),
            request,
            extraction_generation: stale_generation,
            result: Ok(ComputedRarSetState {
                plan: stale_plan,
                headers: stale_headers.clone(),
                rebuild_source:
                    crate::pipeline::archive::topology::RarTopologyRebuildSource::CachedHeaders,
                integrated_volumes: BTreeSet::new(),
            }),
        })
        .await;

    let set_state = pipeline.rar_sets.get(&key).expect("RAR set should remain");
    assert_eq!(
        set_state.extraction_generation,
        stale_generation.saturating_add(1)
    );
    assert!(
        set_state.plan.is_none(),
        "the stale plan must stay discarded"
    );
    assert_eq!(
        pipeline.load_rar_snapshot(job_id, "show").as_deref(),
        Some(stale_headers.as_slice()),
        "the older refresh must not alter the retained rebuild base"
    );

    drain_rar_refreshes(&mut pipeline).await;
    let rebuilt_headers = pipeline
        .load_rar_snapshot(job_id, "show")
        .expect("the queued identity rebuild should install fresh headers");
    let rebuilt_archive =
        unrar_rs::RarArchive::deserialize_headers_with_password(&rebuilt_headers, None::<String>)
            .unwrap();
    let e01 = rebuilt_archive
        .metadata()
        .members
        .into_iter()
        .find(|member| member.name == "E01.mkv")
        .unwrap();
    assert_eq!(e01.volumes.first_volume, 0);
    assert_eq!(e01.volumes.last_volume, 1);
}

#[tokio::test]
async fn source_retry_discards_an_in_flight_refresh_without_resurrecting_its_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40139);
    let files = build_multifile_multivolume_rar_set();
    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec("RAR Source Retry Refresh Retirement", &files),
    )
    .await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);
    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let key = (job_id, "show".to_string());
    let stale_plan = pipeline
        .rar_sets
        .get(&key)
        .and_then(|state| state.plan.clone())
        .unwrap();
    let stale_headers = pipeline.load_rar_snapshot(job_id, "show").unwrap();
    let stale_generation = pipeline.rar_sets[&key].extraction_generation;
    let request = RarRefreshRequest {
        target_completed_volume: 3,
        reason: RefreshReason::PostExtraction,
    };
    pipeline.rar_refresh_state.insert(
        key.clone(),
        RarRefreshState {
            in_flight: Some(request),
            ..Default::default()
        },
    );

    pipeline.clear_archive_set_for_source_retry(job_id, "show");
    let tombstone = pipeline
        .rar_sets
        .get(&key)
        .expect("an in-flight refresh needs a generation tombstone");
    assert_eq!(
        tombstone.extraction_generation,
        stale_generation.saturating_add(1)
    );
    assert!(tombstone.facts.is_empty());
    assert!(tombstone.volume_files.is_empty());
    assert!(tombstone.plan.is_none());
    assert!(pipeline.load_rar_snapshot(job_id, "show").is_none());

    pipeline
        .handle_rar_refresh_done(RarRefreshDone {
            job_id,
            set_name: "show".to_string(),
            request,
            extraction_generation: stale_generation,
            result: Ok(ComputedRarSetState {
                plan: stale_plan,
                headers: stale_headers,
                rebuild_source:
                    crate::pipeline::archive::topology::RarTopologyRebuildSource::CachedHeaders,
                integrated_volumes: BTreeSet::new(),
            }),
        })
        .await;

    assert!(!pipeline.rar_sets.contains_key(&key));
    assert!(!pipeline.rar_refresh_state.contains_key(&key));
    assert!(pipeline.load_rar_snapshot(job_id, "show").is_none());
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .archive_topology_for("show")
            .is_none()
    );
}

#[tokio::test]
async fn active_sibling_extraction_does_not_launch_post_refresh_or_reselect_finished_member() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40127);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Active Sibling Tail Race", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }
    resume_job_downloading_for_test(&mut pipeline, job_id);

    let key = (job_id, "show".to_string());
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&key)
            .expect("RAR set should exist");
        set_state.active_workers = 2;
        set_state.in_flight_members = ["E01.mkv".to_string(), "E02.mkv".to_string()]
            .into_iter()
            .collect();
        set_state.extraction_generation = 7;
        set_state.phase = crate::pipeline::archive::rar_state::RarSetPhase::Extracting;
        let plan = set_state.plan.as_mut().expect("RAR plan should exist");
        plan.phase = crate::pipeline::archive::rar_state::RarSetPhase::Extracting;
        plan.ready_members = ["E01.mkv", "E02.mkv", "E03.mkv"]
            .into_iter()
            .map(|name| crate::pipeline::archive::rar_state::RarReadyMember {
                name: name.to_string(),
            })
            .collect();
        plan.member_names.push("E03.mkv".to_string());
        plan.topology
            .members
            .push(crate::jobs::assembly::ArchiveMember {
                name: "E03.mkv".to_string(),
                first_volume: 0,
                last_volume: 0,
                unpacked_size: 0,
            });
    }
    pipeline.rar_refresh_state.insert(
        key.clone(),
        RarRefreshState {
            in_flight: None,
            queued: None,
            latest_completed_volume: 3,
            refreshed_volumes: BTreeSet::from([0, 1, 2, 3]),
            structure_dirty: false,
            last_error: None,
            last_completion_fingerprint: None,
        },
    );

    pipeline
        .handle_extraction_done(ExtractionDone::Batch {
            job_id,
            set_name: "show".to_string(),
            attempted: vec!["E01.mkv".to_string()],
            result: Ok(BatchExtractionOutcome {
                extracted: vec!["E01.mkv".to_string()],
                failed: Vec::new(),
                selected_password: None,
                phase_completed_bytes: 0,
            }),
        })
        .await;

    let set_state = pipeline.rar_sets.get(&key).expect("RAR set should remain");
    assert_eq!(set_state.extraction_generation, 8);
    assert_eq!(set_state.active_workers, 2);
    assert!(set_state.in_flight_members.contains("E02.mkv"));
    assert!(set_state.in_flight_members.contains("E03.mkv"));
    assert!(!set_state.in_flight_members.contains("E01.mkv"));
    assert!(
        !pipeline.rar_member_can_start_extraction(job_id, "show", "E01.mkv"),
        "finished member must be skipped even if a stale plan lists it as ready"
    );
    let refresh = pipeline
        .rar_refresh_state
        .get(&key)
        .expect("refresh state should remain");
    assert!(!matches!(
        refresh.in_flight,
        Some(RarRefreshRequest {
            reason: RefreshReason::PostExtraction,
            ..
        })
    ));
    assert!(!matches!(
        refresh.queued,
        Some(RarRefreshRequest {
            reason: RefreshReason::PostExtraction,
            ..
        })
    ));
}

#[tokio::test]
async fn stale_rar_batch_failure_for_extracted_member_is_ignored() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40135);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Stale Batch Failure", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let key = (job_id, "show".to_string());
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&key)
            .expect("RAR set should exist");
        set_state.active_workers = 1;
        set_state.in_flight_members = ["E01.mkv".to_string()].into_iter().collect();
        set_state.extraction_generation = 11;
        set_state.phase = crate::pipeline::archive::rar_state::RarSetPhase::Extracting;
        let plan = set_state.plan.as_mut().expect("RAR plan should exist");
        plan.phase = crate::pipeline::archive::rar_state::RarSetPhase::Extracting;
    }
    pipeline
        .extracted_members
        .entry(job_id)
        .or_default()
        .insert("E01.mkv".to_string());

    pipeline
        .handle_extraction_done(ExtractionDone::Batch {
            job_id,
            set_name: "show".to_string(),
            attempted: vec!["E01.mkv".to_string()],
            result: Err(
                "failed to finalize output work/sample.mkv.partial: No such file or directory (os error 2)"
                    .to_string(),
            ),
        })
        .await;

    let set_state = pipeline.rar_sets.get(&key).expect("RAR set should remain");
    assert_eq!(set_state.active_workers, 0);
    assert_eq!(set_state.extraction_generation, 12);
    assert!(!set_state.in_flight_members.contains("E01.mkv"));
    assert!(
        !pipeline
            .failed_extractions
            .get(&job_id)
            .is_some_and(|members| members.contains("E01.mkv")),
        "stale duplicate failure must not promote an already extracted member to repair"
    );
    assert!(
        !pipeline.rar_member_can_start_extraction(job_id, "show", "E01.mkv"),
        "already extracted member must stay non-startable after a stale duplicate failure"
    );
    assert!(!matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
}

#[tokio::test]
async fn stale_rar_batch_success_for_non_current_member_is_ignored_before_normalize() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40136);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Stale Batch Success", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let key = (job_id, "show".to_string());
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&key)
            .expect("RAR set should exist");
        set_state.active_workers = 0;
        set_state.in_flight_members.clear();
        set_state.extraction_generation = 11;
        set_state.phase = crate::pipeline::archive::rar_state::RarSetPhase::Complete;
        let plan = set_state.plan.as_mut().expect("RAR plan should exist");
        plan.phase = crate::pipeline::archive::rar_state::RarSetPhase::Complete;
    }

    let stale_staging = pipeline.deterministic_extraction_staging_dir(job_id);
    let _ = tokio::fs::remove_dir_all(&stale_staging).await;
    pipeline.pending_completion_checks.clear();
    let phase_counters = pipeline.phase_begin(job_id, JobPhase::Extracting, None);
    phase_counters.completed_bytes.store(400, Ordering::Relaxed);

    pipeline
        .handle_extraction_done(ExtractionDone::Batch {
            job_id,
            set_name: "show".to_string(),
            attempted: vec!["E01.mkv".to_string()],
            result: Ok(BatchExtractionOutcome {
                extracted: vec!["E01.mkv".to_string()],
                failed: Vec::new(),
                selected_password: None,
                phase_completed_bytes: 123,
            }),
        })
        .await;

    let set_state = pipeline.rar_sets.get(&key).expect("RAR set should remain");
    assert_eq!(set_state.active_workers, 0);
    assert_eq!(set_state.extraction_generation, 12);
    assert!(set_state.in_flight_members.is_empty());
    assert!(
        !pipeline
            .extracted_members
            .get(&job_id)
            .is_some_and(|members| members.contains("E01.mkv")),
        "stale success must not mark extracted members"
    );
    assert_eq!(phase_counters.completed_bytes.load(Ordering::Relaxed), 277);
    assert!(pipeline.pending_completion_checks.is_empty());
    assert!(!matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
}

#[tokio::test]
async fn idle_rar_worker_drain_recomputes_and_clears_obsolete_post_refresh() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40128);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Idle Drain Post Refresh", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }
    resume_job_downloading_for_test(&mut pipeline, job_id);

    let key = (job_id, "show".to_string());
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&key)
            .expect("RAR set should exist");
        set_state.active_workers = 1;
        set_state.in_flight_members = ["E01.mkv".to_string()].into_iter().collect();
        set_state.extraction_generation = 3;
        set_state.phase = crate::pipeline::archive::rar_state::RarSetPhase::Extracting;
    }
    let queued = RarRefreshRequest {
        target_completed_volume: 3,
        reason: RefreshReason::PostExtraction,
    };
    pipeline.rar_refresh_state.insert(
        key.clone(),
        RarRefreshState {
            in_flight: None,
            queued: Some(queued),
            latest_completed_volume: 3,
            refreshed_volumes: BTreeSet::from([0, 1, 2, 3]),
            structure_dirty: false,
            last_error: None,
            last_completion_fingerprint: None,
        },
    );

    pipeline
        .handle_extraction_done(ExtractionDone::Batch {
            job_id,
            set_name: "show".to_string(),
            attempted: vec!["E01.mkv".to_string()],
            result: Ok(BatchExtractionOutcome {
                extracted: vec!["E01.mkv".to_string()],
                failed: Vec::new(),
                selected_password: None,
                phase_completed_bytes: 0,
            }),
        })
        .await;

    let set_state = pipeline.rar_sets.get(&key).expect("RAR set should remain");
    assert_eq!(set_state.extraction_generation, 4);
    assert!(
        !pipeline.rar_member_can_start_extraction(job_id, "show", "E01.mkv"),
        "idle recompute must use the current extracted-member set"
    );
    let refresh = pipeline
        .rar_refresh_state
        .get(&key)
        .expect("refresh state should remain");
    assert!(refresh.in_flight.is_none());
    assert!(
        !matches!(
            refresh.queued,
            Some(RarRefreshRequest {
                reason: RefreshReason::PostExtraction,
                ..
            })
        ),
        "obsolete post-extraction refresh should be cleared after idle recompute"
    );
}

#[tokio::test]
async fn idle_rar_worker_drain_keeps_extracting_when_coverage_refresh_launches() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40130);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Idle Drain Coverage Refresh", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }
    resume_job_downloading_for_test(&mut pipeline, job_id);

    let key = (job_id, "show".to_string());
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&key)
            .expect("RAR set should exist");
        set_state.active_workers = 1;
        set_state.in_flight_members = ["E01.mkv".to_string()].into_iter().collect();
        set_state.extraction_generation = 11;
        set_state.phase = crate::pipeline::archive::rar_state::RarSetPhase::Extracting;
    }
    let queued = RarRefreshRequest {
        target_completed_volume: 3,
        reason: RefreshReason::CoverageExpansion,
    };
    pipeline.rar_refresh_state.insert(
        key.clone(),
        RarRefreshState {
            in_flight: None,
            queued: Some(queued),
            latest_completed_volume: 3,
            refreshed_volumes: BTreeSet::from([0, 1, 2, 3]),
            structure_dirty: false,
            last_error: None,
            last_completion_fingerprint: None,
        },
    );

    pipeline
        .handle_extraction_done(ExtractionDone::Batch {
            job_id,
            set_name: "show".to_string(),
            attempted: vec!["E01.mkv".to_string()],
            result: Ok(BatchExtractionOutcome {
                extracted: vec!["E01.mkv".to_string()],
                failed: Vec::new(),
                selected_password: None,
                phase_completed_bytes: 0,
            }),
        })
        .await;

    let set_state = pipeline.rar_sets.get(&key).expect("RAR set should remain");
    assert_eq!(set_state.extraction_generation, 12);
    assert_eq!(set_state.active_workers, 1);
    assert!(set_state.in_flight_members.contains("E02.mkv"));
    assert!(!set_state.in_flight_members.contains("E01.mkv"));
    let refresh = pipeline
        .rar_refresh_state
        .get(&key)
        .expect("refresh state should remain");
    assert!(matches!(
        refresh.in_flight,
        Some(RarRefreshRequest {
            reason: RefreshReason::CoverageExpansion,
            ..
        })
    ));
    assert!(refresh.queued.is_none());
}

#[tokio::test]
async fn rar_member_refresh_request_retries_after_refresh_error() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40120);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Refresh Retry", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().take(2).enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.rar_refresh_state.insert(
        (job_id, "show".to_string()),
        RarRefreshState {
            in_flight: None,
            queued: None,
            latest_completed_volume: 1,
            refreshed_volumes: BTreeSet::from([0, 1]),
            structure_dirty: false,
            last_error: Some(RarRefreshError::Other("refresh failed".to_string())),
            last_completion_fingerprint: None,
        },
    );

    let request = pipeline
        .rar_member_refresh_request(job_id, "show", "E01.mkv")
        .expect("refresh failure should keep the member blocked");
    assert_eq!(request.reason, RefreshReason::ValidationFailure);
    assert_eq!(request.target_completed_volume, 1);
}

#[tokio::test]
async fn rar_refresh_capacity_pressure_requeues_without_validation_escalation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40125);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Refresh Capacity Retry", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().take(2).enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let key = (job_id, "show".to_string());
    let request = RarRefreshRequest {
        target_completed_volume: 1,
        reason: RefreshReason::CoverageExpansion,
    };
    pipeline.rar_refresh_state.insert(
        key.clone(),
        RarRefreshState {
            in_flight: Some(request),
            queued: None,
            latest_completed_volume: 1,
            refreshed_volumes: BTreeSet::from([0]),
            structure_dirty: false,
            last_error: None,
            last_completion_fingerprint: None,
        },
    );

    let capacity_error = RarRefreshError::CapacityPressure(format!(
        "{}: synthetic EMFILE",
        crate::pipeline::capacity::FD_CAPACITY_ERROR_MARKER
    ));
    pipeline
        .handle_rar_refresh_done(RarRefreshDone {
            job_id,
            set_name: "show".to_string(),
            request,
            extraction_generation: 0,
            result: Err(capacity_error),
        })
        .await;

    let refresh = pipeline
        .rar_refresh_state
        .get(&key)
        .expect("refresh state should remain queued after capacity pressure");
    assert!(
        refresh
            .last_error
            .as_ref()
            .is_some_and(RarRefreshError::is_capacity_pressure)
    );
    assert!(refresh.in_flight.is_none());
    assert_eq!(
        refresh.queued.as_ref().map(|request| request.reason),
        Some(RefreshReason::CoverageExpansion)
    );
    let pending_key = (job_id, "show".to_string(), RarCapacityRetryKind::Refresh);
    assert!(pipeline.pending_rar_capacity_retries.contains(&pending_key));
    assert_eq!(
        pipeline
            .pending_rar_capacity_retries
            .iter()
            .filter(|key| **key == pending_key)
            .count(),
        1
    );

    let member_request = pipeline
        .rar_member_refresh_request(job_id, "show", "E01.mkv")
        .expect("capacity pressure should keep member waiting on refresh");
    assert_eq!(member_request.reason, RefreshReason::CoverageExpansion);

    pipeline.try_rar_extraction(job_id).await;
    let refresh = pipeline
        .rar_refresh_state
        .get(&key)
        .expect("pending capacity retry should keep refresh queued");
    assert!(refresh.in_flight.is_none());
    assert!(refresh.queued.is_some());

    pipeline.schedule_rar_capacity_retry(job_id, "show", RarCapacityRetryKind::Refresh);
    assert_eq!(
        pipeline
            .pending_rar_capacity_retries
            .iter()
            .filter(|key| **key == pending_key)
            .count(),
        1
    );

    pipeline
        .rar_sets
        .get_mut(&key)
        .expect("RAR set should remain available for retry")
        .plan = None;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);
    pipeline
        .handle_rar_capacity_retry(RarCapacityRetry {
            job_id,
            set_name: "show".to_string(),
            kind: RarCapacityRetryKind::Refresh,
        })
        .await;

    assert!(!pipeline.pending_rar_capacity_retries.contains(&pending_key));
    let refresh = pipeline
        .rar_refresh_state
        .get(&key)
        .expect("refresh state should launch queued retry");
    assert_eq!(
        refresh.in_flight.map(|request| request.reason),
        Some(RefreshReason::CoverageExpansion)
    );
    assert!(refresh.queued.is_none());
    assert!(refresh.last_error.is_none());

    drain_rar_refreshes(&mut pipeline).await;
}

#[tokio::test]
async fn rar_member_refresh_request_ignores_verified_suspects_when_covered() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40122);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Covered Refresh Gate", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().take(2).enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let key = (job_id, "show".to_string());
    pipeline.rar_refresh_state.insert(
        key.clone(),
        RarRefreshState {
            in_flight: None,
            queued: None,
            latest_completed_volume: 1,
            refreshed_volumes: BTreeSet::from([0, 1]),
            structure_dirty: false,
            last_error: None,
            last_completion_fingerprint: None,
        },
    );
    pipeline
        .rar_sets
        .get_mut(&key)
        .expect("RAR set should exist")
        .verified_suspect_volumes = HashSet::from([0u32, 1u32]);

    assert!(
        pipeline
            .rar_member_refresh_request(job_id, "show", "E01.mkv")
            .is_none(),
        "covered members should trust refreshed topology even if suspect-volume bookkeeping remains"
    );
}

#[tokio::test]
async fn nested_rar_two_deep_extracts_final_media() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10069);
    let fixture_name = "rar5_nested_2deep.rar";
    let fixture_bytes = rar5_fixture_bytes(fixture_name);
    let spec = rar_job_spec(
        "Nested RAR Two Deep",
        &[(fixture_name.to_string(), fixture_bytes.clone())],
    );
    let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 0, fixture_name, &fixture_bytes).await;

    pipeline.check_job_completion(job_id).await;
    settle_inflight_moves(&mut pipeline).await;
    settle_inflight_moves(&mut pipeline).await;
    settle_inflight_moves(&mut pipeline).await;
    settle_inflight_moves(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 4).await;

    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Nested RAR Two Deep",
    ));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
    assert!(dest.join("sample.mkv").exists());
    assert!(!dest.join("inner.rar").exists());
}

/// The delivery-naming pass, through the real completion path rather than the
/// move function alone: the pipeline resolves the plan from config and the
/// member reaches the complete directory already renamed.
///
/// `sample.mkv` is exactly the shape the pass exists for — a single lowercase
/// word carrying none of the signals a human-written release name carries, on
/// the one file large enough to be the payload.
#[tokio::test]
async fn an_obfuscated_extracted_member_reaches_the_complete_dir_under_the_job_name() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.config.write().await.delivery_naming =
        Some(crate::settings::DeliveryNamingOverrides {
            deobfuscate_delivered_members: Some(true),
            enable_srrdb_lookup: None,
        });
    let job_id = JobId(10070);
    let fixture_name = "rar5_nested_2deep.rar";
    let fixture_bytes = rar5_fixture_bytes(fixture_name);
    let spec = rar_job_spec(
        "Silver Horizon S01E07",
        &[(fixture_name.to_string(), fixture_bytes.clone())],
    );
    let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 0, fixture_name, &fixture_bytes).await;

    pipeline.check_job_completion(job_id).await;
    for _ in 0..4 {
        settle_inflight_moves(&mut pipeline).await;
    }
    drive_extractions_to_terminal(&mut pipeline, job_id, 4).await;

    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Silver Horizon S01E07",
    ));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
    assert!(
        dest.join("Silver Horizon S01E07.mkv").exists(),
        "dest entries: {:?}",
        std::fs::read_dir(&dest)
            .map(|entries| entries
                .flatten()
                .map(|entry| entry.file_name())
                .collect::<Vec<_>>())
            .unwrap_or_default()
    );
    assert!(!dest.join("sample.mkv").exists());
}

#[tokio::test]
async fn nested_rar_three_deep_extracts_through_inner_7z() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10070);
    let fixture_name = "rar5_nested_3deep.rar";
    let fixture_bytes = rar5_fixture_bytes(fixture_name);
    let spec = rar_job_spec(
        "Nested RAR Three Deep",
        &[(fixture_name.to_string(), fixture_bytes.clone())],
    );
    let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 0, fixture_name, &fixture_bytes).await;

    pipeline.check_job_completion(job_id).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 6).await;

    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Nested RAR Three Deep",
    ));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
    assert!(dest.join("sample.mkv").exists());
    assert!(!dest.join("middle.rar").exists());
    assert!(!dest.join("inner.7z").exists());
}

#[tokio::test]
async fn nested_rar_five_deep_stops_at_depth_limit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10071);
    let fixture_name = "rar5_nested_5deep.rar";
    let fixture_bytes = rar5_fixture_bytes(fixture_name);
    let spec = rar_job_spec(
        "Nested RAR Five Deep",
        &[(fixture_name.to_string(), fixture_bytes.clone())],
    );
    let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 0, fixture_name, &fixture_bytes).await;

    pipeline.check_job_completion(job_id).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 8).await;

    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Nested RAR Five Deep",
    ));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
    assert!(dest.join("level2.rar").exists());
    assert!(!dest.join("sample.mkv").exists());
}

#[tokio::test]
async fn nested_single_stream_preserves_non_archive_sibling() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(100_710);
    let staging_dir = temp_dir.path().join("nested-xz-with-sibling");
    tokio::fs::create_dir_all(&staging_dir).await.unwrap();

    let media = b"ordinary outer archive member";
    let notes = b"nested xz sidecar";
    tokio::fs::write(staging_dir.join("sample.mkv"), media)
        .await
        .unwrap();
    let mut xz =
        lzma_rust2::XzWriter::new(Vec::new(), lzma_rust2::XzOptions::with_preset(0)).unwrap();
    std::io::Write::write_all(&mut xz, notes).unwrap();
    tokio::fs::write(staging_dir.join("release.nfo.xz"), xz.finish().unwrap())
        .await
        .unwrap();

    let mut state = minimal_job_state(job_id, "Nested XZ With Sibling", temp_dir.path().join("wd"));
    state.staging_dir = Some(staging_dir.clone());
    pipeline.jobs.insert(job_id, state);
    pipeline.job_order.push(job_id);

    assert!(matches!(
        pipeline
            .maybe_start_nested_extraction(job_id)
            .await
            .unwrap(),
        crate::pipeline::completion::NestedExtractionDecision::Started
    ));
    assert_eq!(
        tokio::fs::read(staging_dir.join("sample.mkv"))
            .await
            .unwrap(),
        media,
        "starting a selective nested pass must preserve ordinary siblings"
    );

    drive_extractions_to_terminal(&mut pipeline, job_id, 2).await;

    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Nested XZ With Sibling",
    ));
    assert_eq!(
        tokio::fs::read(dest.join("sample.mkv")).await.unwrap(),
        media
    );
    assert_eq!(
        tokio::fs::read(dest.join("release.nfo")).await.unwrap(),
        notes
    );
    assert!(!dest.join("release.nfo.xz").exists());
}

#[tokio::test]
async fn nested_scan_detects_obfuscated_rar_archives_from_staging() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(100_711);
    let staging_dir = temp_dir.path().join("nested-obfuscated-rar");
    tokio::fs::create_dir_all(&staging_dir).await.unwrap();

    let files = build_multifile_multivolume_rar_set();
    let obfuscated_files: Vec<(String, Vec<u8>)> = files
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

    let mut state = minimal_job_state(job_id, "Nested Obfuscated RAR", temp_dir.path().join("wd"));
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
    assert!(
        pipeline
            .inflight_extractions
            .get(&job_id)
            .is_none_or(|sets| !sets.contains("51273aad56a8b904e96928935278a627")),
        "nested RAR should use incremental RAR scheduling, not generic full-set extraction"
    );

    let state = pipeline.jobs.get(&job_id).unwrap();
    let topology = state
        .assembly
        .archive_topology_for("51273aad56a8b904e96928935278a627")
        .unwrap();
    assert_eq!(
        topology.archive_type,
        crate::jobs::assembly::ArchiveType::Rar
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
            weaver_model::files::FileRole::RarVolume { .. }
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
async fn upstream_probe_registers_obfuscated_unknown_rar_volumes_before_completion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10072);
    let files = build_multifile_multivolume_rar_set();
    let obfuscated_files: Vec<(String, Vec<u8>)> = files
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("51273aad56a8b904e96928935278a627.1{}", index),
                bytes.clone(),
            )
        })
        .collect();
    let spec = rar_job_spec("Obfuscated Unknown RAR Recovery", &obfuscated_files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in obfuscated_files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
        drain_rar_refreshes(&mut pipeline).await;
        pipeline.retry_par2_authoritative_identity(job_id).await;
        pipeline.try_rar_extraction(job_id).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
    }

    let mut rar_topologies: Vec<String> = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .assembly
        .archive_topologies()
        .iter()
        .filter_map(|(set_name, topology)| {
            (topology.archive_type == crate::jobs::assembly::ArchiveType::Rar)
                .then_some(set_name.clone())
        })
        .collect();
    rar_topologies.sort();
    assert_eq!(rar_topologies, vec!["51273aad56a8b904e96928935278a627"]);
    let detected_archives = pipeline
        .db
        .load_detected_archive_identities(job_id)
        .unwrap();
    assert_eq!(detected_archives.len(), obfuscated_files.len());
    for file_index in 0..obfuscated_files.len() {
        let file = pipeline
            .jobs
            .get(&job_id)
            .unwrap()
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
            weaver_model::files::FileRole::RarVolume { .. }
        ));
        assert_eq!(
            pipeline
                .detected_archive_identity(job_id, file.file_id())
                .map(|detected| detected.set_name.as_str()),
            Some("51273aad56a8b904e96928935278a627")
        );
        assert!(matches!(
            detected_archives
                .get(&(file_index as u32))
                .map(|detected| &detected.kind),
            Some(crate::jobs::assembly::DetectedArchiveKind::Rar)
        ));
    }

    resume_job_downloading_for_test(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 4).await;

    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Obfuscated Unknown RAR Recovery",
    ));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
    assert!(dest.join("E01.mkv").exists());
    assert!(dest.join("E02.mkv").exists());
}

#[tokio::test]
async fn upstream_probe_registers_obfuscated_split_topology_rar_volumes_before_completion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10073);
    let files = build_multifile_multivolume_rar_set();
    let obfuscated_files: Vec<(String, Vec<u8>)> = files
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("51273aad56a8b904e96928935278a627.10{}", index),
                bytes.clone(),
            )
        })
        .collect();
    let spec = rar_job_spec("Obfuscated Split RAR Recovery", &obfuscated_files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in obfuscated_files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
        drain_rar_refreshes(&mut pipeline).await;
        pipeline.retry_par2_authoritative_identity(job_id).await;
        pipeline.try_rar_extraction(job_id).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
    }

    let mut rar_topologies: Vec<String> = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .assembly
        .archive_topologies()
        .iter()
        .filter_map(|(set_name, topology)| {
            (topology.archive_type == crate::jobs::assembly::ArchiveType::Rar)
                .then_some(set_name.clone())
        })
        .collect();
    rar_topologies.sort();
    assert_eq!(rar_topologies, vec!["51273aad56a8b904e96928935278a627"]);
    let detected_archives = pipeline
        .db
        .load_detected_archive_identities(job_id)
        .unwrap();
    assert_eq!(detected_archives.len(), obfuscated_files.len());

    resume_job_downloading_for_test(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 4).await;

    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Obfuscated Split RAR Recovery",
    ));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
    assert!(dest.join("E01.mkv").exists());
    assert!(dest.join("E02.mkv").exists());
}

#[tokio::test]
async fn upstream_probe_falls_back_from_rar_to_7z_for_obfuscated_split_files() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10074);
    let fixture_files = sevenz_fixture_bytes("generated_split_store_plain.7z");
    let obfuscated_files: Vec<(String, Vec<u8>)> = fixture_files
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("51273aad56a8b904e96928935278a627.{}", index + 10),
                bytes.clone(),
            )
        })
        .collect();
    let spec = rar_job_spec("Obfuscated Split 7z Detection", &obfuscated_files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let set_name = "51273aad56a8b904e96928935278a627";
    for (file_index, (filename, bytes)) in obfuscated_files.iter().enumerate() {
        write_and_complete_file_like_decode_worker(
            &mut pipeline,
            job_id,
            file_index as u32,
            filename,
            bytes,
        )
        .await;

        let topology = pipeline
            .jobs
            .get(&job_id)
            .and_then(|state| state.assembly.archive_topology_for(set_name))
            .cloned()
            .expect("detected 7z topology should exist");
        assert_eq!(
            topology.archive_type,
            crate::jobs::assembly::ArchiveType::SevenZip
        );
        assert_eq!(
            topology.expected_volume_count,
            Some(obfuscated_files.len() as u32)
        );

        let expected_complete_volumes: std::collections::HashSet<u32> =
            (0..=file_index as u32).collect();
        assert_eq!(topology.complete_volumes, expected_complete_volumes);
    }

    pipeline.check_job_completion(job_id).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 2).await;

    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "Obfuscated Split 7z Detection",
    ));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
    assert!(dest.join("generated_split_clip.mkv").exists());
}

#[tokio::test]
async fn restore_job_rehydrates_detected_obfuscated_rar_identity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(10076);
    let files = build_multifile_multivolume_rar_set();
    let obfuscated_files: Vec<(String, Vec<u8>)> = files
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("51273aad56a8b904e96928935278a627.1{}", index),
                bytes.clone(),
            )
        })
        .collect();
    let spec = rar_job_spec("Restore Obfuscated RAR", &obfuscated_files);
    insert_active_job(&mut pipeline, job_id, spec.clone()).await;

    for (file_index, (filename, bytes)) in obfuscated_files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
        persist_completed_file_hash(&pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    let recovered = pipeline
        .db
        .load_active_jobs()
        .unwrap()
        .remove(&job_id)
        .unwrap();
    drop(pipeline);
    let (mut restored, _intermediate_dir, complete_dir_restored) =
        new_direct_pipeline(&temp_dir).await;
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            file_progress: recovered.file_progress,
            complete_files: recovered.complete_files,
            detected_archives: recovered.detected_archives,
            file_identities: recovered.file_identities,
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir: recovered.output_dir,
        })
        .await
        .unwrap();

    let restored_file = restored
        .jobs
        .get(&job_id)
        .unwrap()
        .assembly
        .file(NzbFileId {
            job_id,
            file_index: 0,
        })
        .unwrap();
    assert!(matches!(
        restored_file.role(),
        weaver_model::files::FileRole::Unknown
    ));
    assert!(matches!(
        restored.classified_role_for_file(job_id, restored_file),
        weaver_model::files::FileRole::RarVolume { .. }
    ));
    assert_eq!(
        restored
            .detected_archive_identity(job_id, restored_file.file_id())
            .map(|detected| detected.set_name.as_str()),
        Some("51273aad56a8b904e96928935278a627")
    );

    let set_name = "51273aad56a8b904e96928935278a627";
    let topology = restored
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for(set_name))
        .cloned()
        .expect("restored RAR topology should exist");
    assert_eq!(
        topology.archive_type,
        crate::jobs::assembly::ArchiveType::Rar
    );
    assert_eq!(topology.complete_volumes.len(), obfuscated_files.len());
    assert!(!topology.members.is_empty());
    let _ = complete_dir;
    let _ = complete_dir_restored;
}

#[tokio::test]
async fn list_jobs_keeps_legacy_idle_post_state_for_waiting_rar_phase() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20009);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("RAR Waiting", &[("show.part01.rar".to_string(), 512)]),
    )
    .await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Downloading;
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        crate::pipeline::archive::rar_state::RarSetState {
            phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
            ..Default::default()
        },
    );

    let info = pipeline
        .list_jobs()
        .into_iter()
        .find(|info| info.job_id == job_id)
        .expect("job should be listed");

    assert_eq!(info.status, JobStatus::Downloading);
    assert_eq!(
        info.download_state,
        crate::jobs::model::DownloadState::Queued
    );
    assert_eq!(info.post_state, crate::jobs::model::PostState::Idle);
}

/// Regression for the removed PAR2 expected-hash substitution: a completed
/// RAR volume whose yEnc aggregate CRC matches must NOT have the recovery
/// set's EXPECTED MD5 persisted as though it had been calculated. The yEnc
/// header is the poster's own declaration; a same-length different byte
/// sequence with internally consistent yEnc CRCs used to sail through here
/// and later self-certify quick verification against the very hash it was
/// copied from, overriding a `Damaged` IFSC verdict.
#[tokio::test]
async fn completed_rar_with_par2_metadata_never_persists_the_expected_hash() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20020);
    let filename = "show.part01.rar";
    let payload = b"verified rar volume";
    let expected_hash = [0xAB; 16];
    assert_ne!(expected_hash, par2_rs::checksum::md5(payload));
    let spec = rar_job_spec(
        "RAR Expected Hash Fast Path",
        &[(filename.to_string(), payload.to_vec())],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    let par2_file_id = par2_rs::FileId::from_bytes([0x42; 16]);
    let mut par2_set = minimal_par2_file_set();
    par2_set.recovery_file_ids.push(par2_file_id);
    par2_set.files.insert(
        par2_file_id,
        par2_rs::FileDescription {
            file_id: par2_file_id,
            hash_full: expected_hash,
            hash_16k: [0xCD; 16],
            length: payload.len() as u64,
            par2_name: filename.to_string(),
            filename: filename.to_string(),
        },
    );
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_decoded_segment(
        &mut pipeline,
        file_id,
        0,
        0,
        payload,
        filename,
        Some(par2_rs::checksum::crc32(payload)),
    )
    .await;

    // Neither loader may hold the PAR2 expectation. A digest, when one is
    // persisted at all, must be the one calculated from the downloaded
    // bytes — never the recovery set's declared value.
    let actual_hash = par2_rs::checksum::md5(payload);
    let trusted = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    assert_ne!(trusted.get(&0).copied(), Some(expected_hash));
    if let Some(stored) = trusted.get(&0) {
        assert_eq!(*stored, actual_hash);
    }
    let any = pipeline.db.load_complete_file_hashes_any(job_id).unwrap();
    assert_ne!(any.get(&0).copied(), Some(expected_hash));
}

/// Twin of the test above for the no-whole-file-CRC arm the substitution also
/// served: with only part CRCs verified, completion must still not mint the
/// PAR2 expectation as a persisted digest.
#[tokio::test]
async fn completed_rar_without_whole_file_crc_never_persists_the_expected_hash() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20021);
    let filename = "show.part02.rar";
    let payload = b"verified rar volume without whole crc";
    let expected_hash = [0xBC; 16];
    assert_ne!(expected_hash, par2_rs::checksum::md5(payload));
    let spec = rar_job_spec(
        "RAR Expected Hash Missing CRC Fast Path",
        &[(filename.to_string(), payload.to_vec())],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    let par2_file_id = par2_rs::FileId::from_bytes([0x43; 16]);
    let mut par2_set = minimal_par2_file_set();
    par2_set.recovery_file_ids.push(par2_file_id);
    par2_set.files.insert(
        par2_file_id,
        par2_rs::FileDescription {
            file_id: par2_file_id,
            hash_full: expected_hash,
            hash_16k: [0xDE; 16],
            length: payload.len() as u64,
            par2_name: filename.to_string(),
            filename: filename.to_string(),
        },
    );
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_decoded_segment(&mut pipeline, file_id, 0, 0, payload, filename, None).await;

    let actual_hash = par2_rs::checksum::md5(payload);
    let trusted = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    assert_ne!(trusted.get(&0).copied(), Some(expected_hash));
    if let Some(stored) = trusted.get(&0) {
        assert_eq!(*stored, actual_hash);
    }
    let any = pipeline.db.load_complete_file_hashes_any(job_id).unwrap();
    assert_ne!(any.get(&0).copied(), Some(expected_hash));
}

#[tokio::test]
async fn completed_rar_without_verified_part_crc_falls_back_to_actual_hash() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20022);
    let filename = "show.part03.rar";
    let payload = b"rar volume without verified part crc";
    let expected_hash = [0xBD; 16];
    let actual_hash = par2_rs::checksum::md5(payload);
    assert_ne!(expected_hash, actual_hash);
    let spec = rar_job_spec(
        "RAR Expected Hash Missing Part CRC Fallback",
        &[(filename.to_string(), payload.to_vec())],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    let par2_file_id = par2_rs::FileId::from_bytes([0x44; 16]);
    let mut par2_set = minimal_par2_file_set();
    par2_set.recovery_file_ids.push(par2_file_id);
    par2_set.files.insert(
        par2_file_id,
        par2_rs::FileDescription {
            file_id: par2_file_id,
            hash_full: expected_hash,
            hash_16k: [0xEF; 16],
            length: payload.len() as u64,
            par2_name: filename.to_string(),
            filename: filename.to_string(),
        },
    );
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    submit_decoded_segment_with_part_crc_verified(
        &mut pipeline,
        file_id,
        0,
        0,
        payload,
        filename,
        None,
        false,
    )
    .await;

    let hashes = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    assert_eq!(hashes.get(&0).copied(), Some(actual_hash));
}

#[tokio::test]
async fn out_of_order_rar_completion_keeps_pending_continuation_until_start_arrives() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30001);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Snapshot Topology", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 1, &files[1].0, &files[1].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 3, &files[3].0, &files[3].1).await;

    assert_eq!(member_span(&pipeline, job_id, "show", "E02.mkv"), None);
    assert_eq!(
        unresolved_spans(&pipeline, job_id, "show"),
        vec![(1, 1), (3, 3)]
    );
    let fact_volumes: Vec<u32> = pipeline
        .db
        .load_all_rar_volume_facts(job_id)
        .unwrap()
        .get("show")
        .unwrap()
        .iter()
        .map(|(volume, _)| *volume)
        .collect();
    assert_eq!(fact_volumes, vec![0, 1, 3]);

    write_and_complete_rar_volume(&mut pipeline, job_id, 2, &files[2].0, &files[2].1).await;

    assert_eq!(
        member_span(&pipeline, job_id, "show", "E01.mkv"),
        Some((0, 1))
    );
    assert_eq!(
        member_span(&pipeline, job_id, "show", "E02.mkv"),
        Some((2, 3))
    );
    assert!(unresolved_spans(&pipeline, job_id, "show").is_empty());
    let fact_volumes: Vec<u32> = pipeline
        .db
        .load_all_rar_volume_facts(job_id)
        .unwrap()
        .get("show")
        .unwrap()
        .iter()
        .map(|(volume, _)| *volume)
        .collect();
    assert_eq!(fact_volumes, vec![0, 1, 2, 3]);
}

#[tokio::test]
async fn eager_delete_preserves_later_member_volumes_after_out_of_order_completion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30002);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Eager Delete", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in [
        (0usize, &files[0]),
        (1, &files[1]),
        (3, &files[3]),
        (2, &files[2]),
    ] {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline
        .extracted_members
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();
    if let Some(set_state) = pipeline.rar_sets.get_mut(&(job_id, "show".to_string())) {
        set_state.active_workers = 0;
        set_state.in_flight_members.clear();
    }

    pipeline.try_delete_volumes(job_id, "show");

    let deletion_eligible = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .map(|plan| plan.deletion_eligible.clone())
        .unwrap();
    assert!(deletion_eligible.contains(&0));
    assert!(deletion_eligible.contains(&1));
    assert!(!deletion_eligible.contains(&2));
    assert!(!deletion_eligible.contains(&3));
    assert!(!working_dir.join("show.part01.rar").exists());
    assert!(!working_dir.join("show.part02.rar").exists());
    assert!(working_dir.join("show.part03.rar").exists());
    assert!(working_dir.join("show.part04.rar").exists());
    let deleted_rows = pipeline.db.load_deleted_volume_statuses(job_id).unwrap();
    assert!(deleted_rows.is_empty());
}

#[tokio::test]
async fn eager_delete_waits_for_par2_verification_before_removing_rar_sources() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30020);
    let files = build_multifile_multivolume_rar_set();
    let mut spec = rar_job_spec("RAR Eager Delete Waits For PAR2", &files);
    let par2_bytes = [0x50, 0x41, 0x52, 0x32];
    spec.total_bytes = spec.total_bytes.saturating_add(par2_bytes.len() as u64);
    spec.files.push(FileSpec {
        filename: "repair.par2".to_string(),
        role: FileRole::Par2 {
            is_index: true,
            recovery_block_count: 0,
        },
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: par2_bytes.len() as u32,
            message_id: "rar-par2-index@example.com".to_string(),
        }],
    });
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline
        .extracted_members
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();
    if let Some(set_state) = pipeline.rar_sets.get_mut(&(job_id, "show".to_string())) {
        set_state.active_workers = 0;
        set_state.in_flight_members.clear();
    }

    pipeline.try_delete_volumes(job_id, "show");

    assert!(working_dir.join("show.part01.rar").exists());
    assert!(working_dir.join("show.part02.rar").exists());
    assert!(
        pipeline
            .eagerly_deleted
            .get(&job_id)
            .is_none_or(HashSet::is_empty)
    );

    let par2_set = placement_par2_file_set(&files);
    let access = par2_rs::DiskFileAccess::new(working_dir.clone(), &par2_set);
    let verification = par2_rs::verify_all(&par2_set, &access);
    pipeline.recompute_volume_safety_from_verification(job_id, &verification);
    pipeline.par2_verified.insert(job_id);
    pipeline.try_delete_volumes(job_id, "show");

    assert!(!working_dir.join("show.part01.rar").exists());
    assert!(!working_dir.join("show.part02.rar").exists());
}

#[tokio::test]
async fn restore_job_reuses_persisted_rar_volume_facts_after_restart() {
    let temp_dir = tempfile::tempdir().unwrap();
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Snapshot Restore", &files);
    let job_id = JobId(30003);
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;

        for (file_index, (filename, bytes)) in [
            (0usize, &files[0]),
            (1, &files[1]),
            (3, &files[3]),
            (2, &files[2]),
        ] {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
            persist_completed_file_hash(&pipeline, job_id, file_index as u32, filename, bytes)
                .await;
        }

        pipeline
            .extracted_members
            .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
        std::fs::write(working_dir.join("E01.mkv"), b"episode-one").unwrap();
        pipeline
            .db
            .add_extracted_member(job_id, "E01.mkv", &working_dir.join("E01.mkv"))
            .unwrap();
        pipeline.try_delete_volumes(job_id, "show");
        working_dir
    };

    let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
    restored
        .direct_store
        .set_gate(crate::pipeline::direct_store::DirectStoreGate::Disabled);
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            file_progress: HashMap::new(),
            complete_files: files
                .iter()
                .enumerate()
                .map(|(file_index, _)| NzbFileId {
                    job_id,
                    file_index: file_index as u32,
                })
                .collect(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: ["E01.mkv".to_string()].into_iter().collect(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir: working_dir.clone(),
        })
        .await
        .unwrap();

    assert!(restored.try_update_archive_topology_calls >= files.len());

    assert_eq!(
        member_span(&restored, job_id, "show", "E01.mkv"),
        Some((0, 1))
    );
    assert_eq!(
        member_span(&restored, job_id, "show", "E02.mkv"),
        Some((2, 3))
    );
    assert_eq!(
        restored
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .get("show")
            .map(|facts| facts.len()),
        Some(4)
    );
    assert!(!working_dir.join("show.part01.rar").exists());
    assert!(!working_dir.join("show.part02.rar").exists());
    assert!(working_dir.join("show.part03.rar").exists());
    assert!(working_dir.join("show.part04.rar").exists());
}

#[tokio::test]
async fn swapped_rar_volume_arrival_uses_parsed_volume_identity_for_claims() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30032);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Swapped Live Mapping", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 1, &files[1].0, &files[1].1).await;

    // `show.part04.rar` arrives first but actually contains logical volume 2.
    write_and_complete_rar_volume(&mut pipeline, job_id, 3, &files[3].0, &files[2].1).await;

    let key = (job_id, "show".to_string());
    let plan = pipeline
        .rar_sets
        .get(&key)
        .and_then(|state| state.plan.as_ref())
        .expect("RAR plan should exist after swapped volume arrival");
    assert!(
        plan.delete_decisions
            .values()
            .all(|decision| !decision.owners.is_empty())
    );
    assert!(plan.waiting_on_volumes.contains(&3));
    assert_eq!(
        Pipeline::rar_volume_filename(&plan.topology.volume_map, 2),
        Some("show.part04.rar")
    );

    // The counterpart arrives under the opposite filename and should complete the mapping.
    write_and_complete_rar_volume(&mut pipeline, job_id, 2, &files[2].0, &files[3].1).await;

    let plan = pipeline
        .rar_sets
        .get(&key)
        .and_then(|state| state.plan.as_ref())
        .expect("RAR plan should exist after swapped pair arrival");
    assert!(
        plan.delete_decisions
            .values()
            .all(|decision| !decision.owners.is_empty())
    );
    assert_eq!(
        Pipeline::rar_volume_filename(&plan.topology.volume_map, 2),
        Some("show.part04.rar")
    );
    assert_eq!(
        Pipeline::rar_volume_filename(&plan.topology.volume_map, 3),
        Some("show.part03.rar")
    );
    assert_eq!(
        member_span(&pipeline, job_id, "show", "E02.mkv"),
        Some((2, 3))
    );
}
