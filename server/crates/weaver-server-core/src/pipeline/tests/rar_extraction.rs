use super::*;

#[tokio::test]
async fn extraction_member_total_reservation_is_idempotent_across_retries() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(101);
    let counters = pipeline.phase_begin(job_id, JobPhase::Extracting, None);

    pipeline.phase_reserve_extraction_member_total(
        job_id,
        "movie.part01.rar",
        "movie.mkv",
        1_000,
        &counters,
    );
    pipeline.phase_reserve_extraction_member_total(
        job_id,
        "movie.part01.rar",
        "movie.mkv",
        1_000,
        &counters,
    );
    pipeline.phase_reserve_extraction_member_total(
        job_id,
        "movie.part01.rar",
        "sample.srt",
        25,
        &counters,
    );

    assert_eq!(counters.total_bytes.load(Ordering::Relaxed), 1_025);
}

#[tokio::test]
async fn extraction_refreshes_stale_cached_headers_for_touched_volumes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let files = build_multifile_multivolume_rar_set();

    let mut good_archive =
        weaver_unrar::RarArchive::open(std::io::Cursor::new(files[0].1.clone())).unwrap();
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
        &serde_json::from_value::<weaver_unrar::CachedArchiveHeaders>(cached).unwrap(),
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

    let options = weaver_unrar::ExtractOptions {
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
            shared_kdf_cache: std::sync::Arc::new(weaver_unrar::crypto::KdfCache::new()),
            open_mode: crate::pipeline::extraction::RarArchiveOpenMode::AttachOnly,
            requested_members: None,
            already_extracted: None,
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
            shared_kdf_cache: std::sync::Arc::new(weaver_unrar::crypto::KdfCache::new()),
            open_mode: crate::pipeline::extraction::RarArchiveOpenMode::RefreshProvidedVolumes,
            requested_members: None,
            already_extracted: None,
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
        weaver_unrar::RarArchive::open(std::io::Cursor::new(files[0].1.clone())).unwrap();
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
        &serde_json::from_value::<weaver_unrar::CachedArchiveHeaders>(cached).unwrap(),
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
    let healed_archive = weaver_unrar::RarArchive::deserialize_headers_with_password(
        &healed_headers,
        None::<String>,
    )
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
        weaver_unrar::RarArchive::open(std::io::Cursor::new(files[0].1.clone())).unwrap();
    cached_archive
        .add_volume(1, Box::new(std::io::Cursor::new(files[1].1.clone())))
        .unwrap();
    cached_archive
        .add_volume(2, Box::new(std::io::Cursor::new(files[2].1.clone())))
        .unwrap();

    let mut facts = BTreeMap::new();
    for (volume, (_, bytes)) in files.iter().enumerate() {
        let mut parsed =
            weaver_unrar::RarArchive::parse_volume_facts(std::io::Cursor::new(bytes.clone()), None)
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
        weaver_unrar::RarArchive::open(std::io::Cursor::new(files[0].1.clone())).unwrap();
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
        &serde_json::from_value::<weaver_unrar::CachedArchiveHeaders>(cached).unwrap(),
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
    let healed_archive = weaver_unrar::RarArchive::deserialize_headers_with_password(
        &healed_headers,
        None::<String>,
    )
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
            shared_kdf_cache: std::sync::Arc::new(weaver_unrar::crypto::KdfCache::new()),
            open_mode: crate::pipeline::extraction::RarArchiveOpenMode::AttachOnly,
            requested_members: &[],
            already_extracted: None,
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
            shared_kdf_cache: std::sync::Arc::new(weaver_unrar::crypto::KdfCache::new()),
            open_mode: crate::pipeline::extraction::RarArchiveOpenMode::AttachOnly,
            requested_members: &requested_members,
            already_extracted: None,
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
    let options = weaver_unrar::ExtractOptions {
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
            shared_kdf_cache: std::sync::Arc::new(weaver_unrar::crypto::KdfCache::new()),
            open_mode: crate::pipeline::extraction::RarArchiveOpenMode::AttachOnly,
            requested_members: &requested_members,
            already_extracted: None,
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
                weaver_unrar::RarError::InvalidPassword,
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
        crate::jobs::model::DownloadState::Downloading
    );
    assert_eq!(info.post_state, crate::jobs::model::PostState::Idle);
}

#[tokio::test]
async fn completed_rar_with_par2_metadata_and_verified_yenc_crc_uses_expected_hash() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20020);
    let filename = "show.part01.rar";
    let payload = b"verified rar volume";
    let expected_hash = [0xAB; 16];
    assert_ne!(expected_hash, weaver_par2::checksum::md5(payload));
    let spec = rar_job_spec(
        "RAR Expected Hash Fast Path",
        &[(filename.to_string(), payload.to_vec())],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    let par2_file_id = weaver_par2::FileId::from_bytes([0x42; 16]);
    let mut par2_set = minimal_par2_file_set();
    par2_set.recovery_file_ids.push(par2_file_id);
    par2_set.files.insert(
        par2_file_id,
        weaver_par2::FileDescription {
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
        Some(weaver_par2::checksum::crc32(payload)),
    )
    .await;

    let hashes = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    assert_eq!(hashes.get(&0).copied(), Some(expected_hash));
}

#[tokio::test]
async fn completed_rar_without_whole_file_crc_uses_expected_hash_when_parts_verified() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20021);
    let filename = "show.part02.rar";
    let payload = b"verified rar volume without whole crc";
    let expected_hash = [0xBC; 16];
    assert_ne!(expected_hash, weaver_par2::checksum::md5(payload));
    let spec = rar_job_spec(
        "RAR Expected Hash Missing CRC Fast Path",
        &[(filename.to_string(), payload.to_vec())],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    let par2_file_id = weaver_par2::FileId::from_bytes([0x43; 16]);
    let mut par2_set = minimal_par2_file_set();
    par2_set.recovery_file_ids.push(par2_file_id);
    par2_set.files.insert(
        par2_file_id,
        weaver_par2::FileDescription {
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

    let hashes = pipeline.db.load_complete_file_hashes(job_id).unwrap();
    assert_eq!(hashes.get(&0).copied(), Some(expected_hash));
}

#[tokio::test]
async fn completed_rar_without_verified_part_crc_falls_back_to_actual_hash() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20022);
    let filename = "show.part03.rar";
    let payload = b"rar volume without verified part crc";
    let expected_hash = [0xBD; 16];
    let actual_hash = weaver_par2::checksum::md5(payload);
    assert_ne!(expected_hash, actual_hash);
    let spec = rar_job_spec(
        "RAR Expected Hash Missing Part CRC Fallback",
        &[(filename.to_string(), payload.to_vec())],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    let par2_file_id = weaver_par2::FileId::from_bytes([0x44; 16]);
    let mut par2_set = minimal_par2_file_set();
    par2_set.recovery_file_ids.push(par2_file_id);
    par2_set.files.insert(
        par2_file_id,
        weaver_par2::FileDescription {
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
    let access = weaver_par2::DiskFileAccess::new(working_dir.clone(), &par2_set);
    let verification = weaver_par2::verify_all(&par2_set, &access);
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

#[tokio::test]
async fn par2_metadata_immediately_rebinds_obfuscated_rar_file_identity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30111);
    let canonical_filename = "show.part001.rar";
    let obfuscated_filename = "51273aad56a8b904e96928935278a627.101";
    let rar_bytes = build_multifile_multivolume_rar_set()[0].1.clone();
    let par2_filename = "repair.par2";
    let par2_bytes = build_test_par2_index(canonical_filename, &rar_bytes, 8);
    let spec = JobSpec {
        name: "PAR2 Canonical Rebind".to_string(),
        password: None,
        total_bytes: (rar_bytes.len() + par2_bytes.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: obfuscated_filename.to_string(),
                role: FileRole::from_filename(obfuscated_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: rar_bytes.len() as u32,
                    message_id: "rar-obfuscated@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: par2_filename.to_string(),
                role: FileRole::from_filename(par2_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "repair-index@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_file(&mut pipeline, job_id, 0, obfuscated_filename, &rar_bytes).await;
    write_and_complete_file(&mut pipeline, job_id, 1, par2_filename, &par2_bytes).await;
    pipeline
        .try_load_par2_metadata(
            job_id,
            NzbFileId {
                job_id,
                file_index: 1,
            },
        )
        .await;
    drain_rar_refreshes(&mut pipeline).await;

    let identity = pipeline
        .file_identity(
            job_id,
            NzbFileId {
                job_id,
                file_index: 0,
            },
        )
        .cloned()
        .expect("data file identity should exist");
    assert_eq!(identity.current_filename, canonical_filename);
    assert_eq!(
        identity.canonical_filename.as_deref(),
        Some(canonical_filename)
    );
    assert_eq!(identity.classification_source, FileIdentitySource::Par2);
    assert!(matches!(
        identity
            .classification
            .as_ref()
            .map(|classification| &classification.kind),
        Some(crate::jobs::assembly::DetectedArchiveKind::Rar)
    ));
    assert!(!working_dir.join(obfuscated_filename).exists());
    assert!(working_dir.join(canonical_filename).exists());

    let topology = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for("show"))
        .cloned()
        .expect("PAR2 rebinding should rebuild RAR topology");
    assert!(topology.volume_map.contains_key(canonical_filename));
    assert!(!topology.volume_map.contains_key(obfuscated_filename));
    assert!(
        !pipeline
            .rar_sets
            .contains_key(&(job_id, "51273aad56a8b904e96928935278a627".to_string())),
        "old obfuscated RAR set should not survive canonical PAR2 rebinding"
    );
}

#[tokio::test]
async fn authoritative_par2_identity_clears_preexisting_stale_rar_set_state() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30131);
    let old_set_name = "51273aad56a8b904e96928935278a627";
    let old_filename = "51273aad56a8b904e96928935278a627.101";
    let canonical_filename = "show.part01.rar";
    let rar_bytes = build_multifile_multivolume_rar_set()[0].1.clone();

    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec(
            "Authoritative PAR2 Identity Stale Set",
            &[(old_filename.to_string(), rar_bytes.clone())],
        ),
    )
    .await;

    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: old_filename.to_string(),
                current_filename: canonical_filename.to_string(),
                canonical_filename: Some(canonical_filename.to_string()),
                classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name: old_set_name.to_string(),
                    volume_index: Some(0),
                }),
                classification_source: crate::jobs::record::FileIdentitySource::Par2,
            },
        )
        .unwrap();

    pipeline.rar_sets.insert(
        (job_id, old_set_name.to_string()),
        crate::pipeline::archive::rar_state::RarSetState {
            plan: Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
                phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["E01.mkv".to_string()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::from([1u32]),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology: crate::jobs::assembly::ArchiveTopology {
                    archive_type: crate::jobs::assembly::ArchiveType::Rar,
                    volume_map: HashMap::from([(old_filename.to_string(), 0)]),
                    complete_volumes: [0u32].into_iter().collect(),
                    expected_volume_count: Some(2),
                    members: vec![crate::jobs::assembly::ArchiveMember {
                        name: "E01.mkv".to_string(),
                        first_volume: 0,
                        last_volume: 1,
                        unpacked_size: 0,
                    }],
                    unresolved_spans: vec![crate::jobs::assembly::ArchivePendingSpan {
                        first_volume: 1,
                        last_volume: 1,
                    }],
                },
                fallback_reason: None,
            }),
            ..Default::default()
        },
    );

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(canonical_filename.to_string(), rar_bytes)]),
        &[],
    );

    pipeline.retry_par2_authoritative_identity(job_id).await;

    let identity = pipeline
        .file_identity(
            job_id,
            NzbFileId {
                job_id,
                file_index: 0,
            },
        )
        .cloned()
        .expect("file identity should exist");
    assert_eq!(identity.current_filename, canonical_filename);
    assert_eq!(
        identity
            .classification
            .as_ref()
            .map(|classification| classification.set_name.as_str()),
        Some("show")
    );
    assert!(!pipeline.job_has_live_rar_waiting_for_missing_volumes(job_id));
    assert!(
        !pipeline
            .rar_sets
            .contains_key(&(job_id, old_set_name.to_string()))
    );
    assert!(pipeline.invalid_rar_retry_frontier_reason(job_id).is_none());
}

#[tokio::test]
async fn restore_job_scrubs_stale_par2_rar_set_state_before_rar_runtime_rebuild() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30132);
    let old_set_name = "51273aad56a8b904e96928935278a627";
    let old_filename = "51273aad56a8b904e96928935278a627.101";
    let canonical_filename = "show.part01.rar";
    let rar_bytes = build_multifile_multivolume_rar_set()[0].1.clone();
    let spec = rar_job_spec(
        "Restore Stale PAR2 RAR Set",
        &[(old_filename.to_string(), rar_bytes.clone())],
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;

    write_and_complete_file(&mut pipeline, job_id, 0, old_filename, &rar_bytes).await;
    persist_completed_file_hash(&pipeline, job_id, 0, old_filename, &rar_bytes).await;
    std::fs::rename(
        working_dir.join(old_filename),
        working_dir.join(canonical_filename),
    )
    .unwrap();

    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: old_filename.to_string(),
                current_filename: canonical_filename.to_string(),
                canonical_filename: Some(canonical_filename.to_string()),
                classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name: old_set_name.to_string(),
                    volume_index: Some(0),
                }),
                classification_source: crate::jobs::record::FileIdentitySource::Par2,
            },
        )
        .unwrap();

    let encoded_facts = rmp_serde::to_vec_named(&dummy_rar_volume_facts(0)).unwrap();
    pipeline
        .db
        .save_rar_volume_facts(job_id, old_set_name, 0, &encoded_facts)
        .unwrap();

    let recovered = pipeline
        .db
        .load_active_jobs()
        .unwrap()
        .remove(&job_id)
        .unwrap();
    drop(pipeline);

    let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec: spec.clone(),
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

    let identity = restored
        .file_identity(
            job_id,
            NzbFileId {
                job_id,
                file_index: 0,
            },
        )
        .cloned()
        .expect("file identity should exist after restore");
    assert_eq!(identity.current_filename, canonical_filename);
    assert_eq!(
        identity
            .classification
            .as_ref()
            .map(|classification| classification.set_name.as_str()),
        Some("show")
    );
    assert!(
        !restored
            .rar_sets
            .contains_key(&(job_id, old_set_name.to_string()))
    );
    assert!(
        restored
            .rar_sets
            .contains_key(&(job_id, "show".to_string()))
    );
    assert!(
        !restored
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .contains_key(old_set_name)
    );
}

#[tokio::test]
async fn par2_metadata_rebinds_obfuscated_rar_after_late_content_probe() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30112);
    let canonical_files = build_multifile_multivolume_rar_set();
    let obfuscated_files: Vec<(String, Vec<u8>)> = canonical_files
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("51273aad56a8b904e96928935278a627.{}", index + 101),
                bytes.clone(),
            )
        })
        .collect();
    let spec = rar_job_spec("PAR2 Late Canonical Rebind", &obfuscated_files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&canonical_files),
        &[],
    );

    for (index, ((_, _), (obfuscated_filename, bytes))) in canonical_files
        .iter()
        .zip(obfuscated_files.iter())
        .enumerate()
    {
        write_and_complete_file(
            &mut pipeline,
            job_id,
            index as u32,
            obfuscated_filename,
            bytes,
        )
        .await;
        pipeline.retry_par2_authoritative_identity(job_id).await;
    }
    drain_rar_refreshes(&mut pipeline).await;

    for (index, ((canonical_filename, _), (obfuscated_filename, _))) in canonical_files
        .iter()
        .zip(obfuscated_files.iter())
        .enumerate()
    {
        let identity = pipeline
            .file_identity(
                job_id,
                NzbFileId {
                    job_id,
                    file_index: index as u32,
                },
            )
            .cloned()
            .expect("data file identity should exist");
        assert_eq!(identity.current_filename, *canonical_filename);
        assert_eq!(
            identity.canonical_filename.as_deref(),
            Some(canonical_filename.as_str())
        );
        assert_eq!(identity.classification_source, FileIdentitySource::Par2);
        assert!(!working_dir.join(obfuscated_filename).exists());
        assert!(working_dir.join(canonical_filename).exists());
    }

    let topology = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for("show"))
        .cloned()
        .expect("late PAR2 rebinding should rebuild RAR topology");
    for (canonical_filename, _) in &canonical_files {
        assert!(topology.volume_map.contains_key(canonical_filename));
    }
    for (obfuscated_filename, _) in &obfuscated_files {
        assert!(!topology.volume_map.contains_key(obfuscated_filename));
    }
    assert!(
        !pipeline
            .rar_sets
            .contains_key(&(job_id, "51273aad56a8b904e96928935278a627".to_string())),
        "old obfuscated RAR set should not survive late canonical PAR2 rebinding"
    );
}

#[tokio::test]
async fn waiting_for_missing_volumes_ignores_stale_noncurrent_rar_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30129);
    let canonical_filename = "show.part01.rar";

    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec(
            "Ignore Stale RAR Waiting",
            &[(canonical_filename.to_string(), vec![0xAB; 64])],
        ),
    )
    .await;

    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: "51273aad56a8b904e96928935278a627.101".to_string(),
                current_filename: canonical_filename.to_string(),
                canonical_filename: Some(canonical_filename.to_string()),
                classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name: "show".to_string(),
                    volume_index: Some(0),
                }),
                classification_source: crate::jobs::record::FileIdentitySource::Par2,
            },
        )
        .unwrap();

    pipeline.rar_sets.insert(
        (job_id, "51273aad56a8b904e96928935278a627".to_string()),
        crate::pipeline::archive::rar_state::RarSetState {
            plan: Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
                phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: Vec::new(),
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::from([1u32]),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology: crate::jobs::assembly::ArchiveTopology {
                    archive_type: crate::jobs::assembly::ArchiveType::Rar,
                    volume_map: HashMap::from([(
                        "51273aad56a8b904e96928935278a627.101".to_string(),
                        0,
                    )]),
                    complete_volumes: [0u32].into_iter().collect(),
                    expected_volume_count: Some(2),
                    members: Vec::new(),
                    unresolved_spans: vec![crate::jobs::assembly::ArchivePendingSpan {
                        first_volume: 1,
                        last_volume: 1,
                    }],
                },
                fallback_reason: None,
            }),
            ..Default::default()
        },
    );

    assert!(!pipeline.job_has_live_rar_waiting_for_missing_volumes(job_id));
}

#[tokio::test]
async fn waiting_for_missing_volumes_still_tracks_current_rar_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30130);
    let canonical_filename = "show.part01.rar";

    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec(
            "Track Current RAR Waiting",
            &[(canonical_filename.to_string(), vec![0xCD; 64])],
        ),
    )
    .await;

    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: canonical_filename.to_string(),
                current_filename: canonical_filename.to_string(),
                canonical_filename: Some(canonical_filename.to_string()),
                classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name: "show".to_string(),
                    volume_index: Some(0),
                }),
                classification_source: crate::jobs::record::FileIdentitySource::Par2,
            },
        )
        .unwrap();

    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        crate::pipeline::archive::rar_state::RarSetState {
            plan: Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
                phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: Vec::new(),
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::from([1u32]),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology: crate::jobs::assembly::ArchiveTopology {
                    archive_type: crate::jobs::assembly::ArchiveType::Rar,
                    volume_map: HashMap::from([(canonical_filename.to_string(), 0)]),
                    complete_volumes: [0u32].into_iter().collect(),
                    expected_volume_count: Some(2),
                    members: Vec::new(),
                    unresolved_spans: vec![crate::jobs::assembly::ArchivePendingSpan {
                        first_volume: 1,
                        last_volume: 1,
                    }],
                },
                fallback_reason: None,
            }),
            ..Default::default()
        },
    );

    assert!(pipeline.job_has_live_rar_waiting_for_missing_volumes(job_id));
}

#[tokio::test]
async fn clean_par2_verification_exits_verifying_for_rar_extraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30113);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("Clean PAR2 RAR Verify Starts Extraction", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
        persist_completed_file_hash(&pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert!(pipeline.par2_verified.contains(&job_id));
    assert_ne!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Verifying)
    );

    let done = next_extraction_done(&mut pipeline).await;
    match done {
        ExtractionDone::Batch {
            job_id: done_job_id,
            attempted,
            result,
            ..
        } => {
            assert_eq!(done_job_id, job_id);
            assert!(!attempted.is_empty());
            assert!(result.is_ok());
        }
        _ => panic!("expected RAR extraction batch"),
    }
}

#[tokio::test]
async fn clean_par2_verification_exits_verifying_for_sevenz_extraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30116);
    let files = sevenz_fixture_bytes("generated_split_store_plain.7z");
    let spec = rar_job_spec("Clean PAR2 7z Verify Starts Extraction", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, file_index as u32, filename, bytes).await;
    }

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert_ne!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Verifying)
    );

    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::FullSet {
            job_id: done_job_id,
            result,
            ..
        } => {
            assert_eq!(*done_job_id, job_id);
            if let Err(error) = result {
                panic!("7z extraction failed: {error}");
            }
        }
        _ => panic!("expected 7z extraction result"),
    }
    pipeline.handle_extraction_done(done).await;
    assert!(pipeline.par2_verified.contains(&job_id));
}

#[tokio::test]
async fn clean_par2_verification_exits_verifying_for_single_sevenz_extraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30117);
    let archive_filename = "archive.7z";
    let seven_zip_bytes = vec![0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C, 0x00, 0x04];
    let spec = standalone_job_spec(
        "Clean PAR2 Single 7z Verify Starts Extraction",
        &[(archive_filename.to_string(), seven_zip_bytes.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(archive_filename.to_string(), seven_zip_bytes.clone())]),
        &[],
    );
    write_and_complete_file(&mut pipeline, job_id, 0, archive_filename, &seven_zip_bytes).await;
    persist_completed_file_hash(&pipeline, job_id, 0, archive_filename, &seven_zip_bytes).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.assembly.set_archive_topology(
            archive_filename.to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::SevenZip,
                volume_map: HashMap::from([(archive_filename.to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "sample.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: 0,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    }

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert_ne!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Verifying)
    );

    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::FullSet {
            job_id: done_job_id,
            set_name,
            result,
        } => {
            assert_eq!(*done_job_id, job_id);
            assert_eq!(set_name, archive_filename);
            assert!(result.is_err());
        }
        _ => panic!("expected single 7z extraction result"),
    }
    pipeline.handle_extraction_done(done).await;
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
}

#[tokio::test]
async fn clean_par2_verification_exits_verifying_for_gzip_extraction() {
    use std::io::Write;

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30118);
    let archive_filename = "payload.gz";
    let payload = b"gzip finalize payload";
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(payload).unwrap();
    let gzip_bytes = encoder.finish().unwrap();
    let spec = standalone_job_spec(
        "Clean PAR2 Gzip Verify Starts Extraction",
        &[(archive_filename.to_string(), gzip_bytes.len() as u32)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;

    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        placement_par2_file_set(&[(archive_filename.to_string(), gzip_bytes.clone())]),
        &[],
    );
    write_and_complete_file(&mut pipeline, job_id, 0, archive_filename, &gzip_bytes).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.assembly.set_archive_topology(
            archive_filename.to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Gz,
                volume_map: HashMap::from([(archive_filename.to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "payload".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: payload.len() as u64,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    }

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert_ne!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Verifying)
    );

    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::FullSet {
            job_id: done_job_id,
            set_name,
            result,
        } => {
            assert_eq!(*done_job_id, job_id);
            assert_eq!(set_name, archive_filename);
            assert!(result.is_ok());
        }
        _ => panic!("expected gzip extraction result"),
    }
    pipeline.handle_extraction_done(done).await;
    assert!(pipeline.par2_verified.contains(&job_id));
}

#[tokio::test]
async fn restore_job_does_not_rehydrate_lossy_extraction_attempt_state() {
    let temp_dir = tempfile::tempdir().unwrap();
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Restart Runtime Restore", &files);
    let job_id = JobId(30034);
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;

        for (file_index, (filename, bytes)) in files.iter().enumerate() {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
        }

        pipeline
            .db
            .add_failed_extraction(job_id, "E10.mkv")
            .unwrap();
        pipeline
            .db
            .add_failed_extraction(job_id, "E15.mkv")
            .unwrap();
        pipeline
            .db
            .set_active_job_normalization_retried(job_id, true)
            .unwrap();
        pipeline
            .db
            .replace_verified_suspect_volumes(
                job_id,
                "show",
                &std::collections::HashSet::from([1u32, 2u32]),
            )
            .unwrap();
        working_dir
    };

    let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
    restored
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            file_progress: HashMap::new(),
            complete_files: HashSet::from([
                NzbFileId {
                    job_id,
                    file_index: 0,
                },
                NzbFileId {
                    job_id,
                    file_index: 1,
                },
            ]),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
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
            working_dir,
        })
        .await
        .unwrap();

    assert!(!restored.failed_extractions.contains_key(&job_id));
    assert!(restored.normalization_retried.contains(&job_id));
    assert!(
        restored
            .rar_sets
            .get(&(job_id, "show".to_string()))
            .is_none_or(|state| state.verified_suspect_volumes.is_empty())
    );
}

#[tokio::test]
async fn normalization_refresh_rebuilds_rar_snapshot_from_disk() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30004);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Normalize Refresh", &files);
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

    tokio::fs::write(working_dir.join("show.part03.rar"), &files[2].1)
        .await
        .unwrap();
    pipeline
        .refresh_rar_topology_after_normalization(
            job_id,
            &["show.part03.rar".to_string()].into_iter().collect(),
        )
        .await
        .unwrap();

    assert_eq!(
        member_span(&pipeline, job_id, "show", "E02.mkv"),
        Some((2, 3))
    );
    assert!(
        pipeline
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .contains_key("show")
    );
}

#[tokio::test]
async fn live_rebuild_failure_retains_previous_rar_volume_facts_and_topology() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30005);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Rebuild Failure", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let original_facts = pipeline
        .db
        .load_all_rar_volume_facts(job_id)
        .unwrap()
        .get("show")
        .cloned()
        .expect("good facts should be persisted");

    pipeline
        .extracted_members
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline.try_delete_volumes(job_id, "show");

    assert!(working_dir.join("show.part01.rar").exists());
    assert!(working_dir.join("show.part02.rar").exists());

    let corrupt_part04 = vec![0u8; files[3].1.len()];
    tokio::fs::write(working_dir.join(&files[3].0), &corrupt_part04)
        .await
        .unwrap();

    pipeline
        .try_update_archive_topology(
            job_id,
            NzbFileId {
                job_id,
                file_index: 3,
            },
        )
        .await;

    assert_eq!(
        pipeline
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .get("show")
            .cloned(),
        Some(original_facts)
    );
    assert_eq!(
        member_span(&pipeline, job_id, "show", "E01.mkv"),
        Some((0, 1))
    );
    assert_eq!(
        member_span(&pipeline, job_id, "show", "E02.mkv"),
        Some((2, 3))
    );
}

#[tokio::test]
async fn incremental_rar_batches_survive_eager_delete_of_earlier_volumes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30006);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Incremental Batches", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().take(2).enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.try_rar_extraction(job_id).await;
    let first_done = next_extraction_done(&mut pipeline).await;
    match &first_done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E01.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected batch extraction completion"),
    }
    pipeline.handle_extraction_done(first_done).await;

    assert!(!working_dir.join("show.part01.rar").exists());
    assert!(!working_dir.join("show.part02.rar").exists());

    write_and_complete_rar_volume(&mut pipeline, job_id, 2, &files[2].0, &files[2].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 3, &files[3].0, &files[3].1).await;
    pipeline.try_rar_extraction(job_id).await;

    let second_done = next_extraction_done(&mut pipeline).await;
    match &second_done {
        ExtractionDone::Batch {
            job_id: done_job_id,
            attempted,
            result,
            ..
        } => {
            assert_eq!(*done_job_id, job_id);
            assert_eq!(attempted, &vec!["E02.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected batch extraction completion"),
    }
    pipeline.handle_extraction_done(second_done).await;
    settle_inflight_moves(&mut pipeline).await;

    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
}

#[tokio::test]
async fn non_solid_incremental_rar_batches_cleanup_chunks_after_finalize() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30007);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Incremental Chunks", &files);
    let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    let staging_dir = pipeline.extraction_staging_dir(job_id);

    for (file_index, (filename, bytes)) in files.iter().take(2).enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.try_rar_extraction(job_id).await;
    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E01.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected batch extraction completion"),
    }
    pipeline.handle_extraction_done(done).await;

    let chunks = pipeline.db.get_extraction_chunks(job_id, "show").unwrap();
    assert!(chunks.iter().all(|chunk| chunk.member_name != "E01.mkv"));
    assert!(staging_dir.join("E01.mkv").exists());
    assert!(
        !staging_dir
            .join(".weaver-chunks")
            .join("show")
            .join("E01.mkv")
            .exists()
    );
}

#[tokio::test]
async fn non_solid_rar_set_dispatches_two_members_concurrently() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner = RuntimeTuner::with_connection_limit(
        crate::runtime::system_profile::SystemProfile {
            cpu: crate::runtime::system_profile::CpuProfile {
                physical_cores: 8,
                logical_cores: 8,
                simd: crate::runtime::system_profile::SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: crate::runtime::system_profile::MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: crate::runtime::system_profile::DiskProfile {
                storage_class: crate::runtime::system_profile::StorageClass::Ssd,
                filesystem: crate::runtime::system_profile::FilesystemType::Apfs,
                sequential_write_mbps: 2000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        },
        4,
    );

    let job_id = JobId(30008);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Concurrent Members", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.try_rar_extraction(job_id).await;

    let set_state = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .expect("set state should exist");
    assert_eq!(set_state.active_workers, 2);
    assert_eq!(
        set_state.in_flight_members,
        ["E01.mkv".to_string(), "E02.mkv".to_string()]
            .into_iter()
            .collect()
    );

    let first_done = next_extraction_done(&mut pipeline).await;
    let second_done = next_extraction_done(&mut pipeline).await;
    let mut attempted_members = Vec::new();
    for done in [&first_done, &second_done] {
        match done {
            ExtractionDone::Batch {
                attempted, result, ..
            } => {
                assert_eq!(attempted.len(), 1);
                assert!(
                    result
                        .as_ref()
                        .is_ok_and(|outcome| outcome.failed.is_empty())
                );
                attempted_members.push(attempted[0].clone());
            }
            _ => panic!("expected batch extraction completion"),
        }
    }
    attempted_members.sort();
    assert_eq!(
        attempted_members,
        vec!["E01.mkv".to_string(), "E02.mkv".to_string()]
    );

    pipeline.handle_extraction_done(first_done).await;
    pipeline.handle_extraction_done(second_done).await;
}

#[tokio::test]
async fn rar_eager_delete_waits_for_all_active_workers_in_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner = RuntimeTuner::with_connection_limit(
        crate::runtime::system_profile::SystemProfile {
            cpu: crate::runtime::system_profile::CpuProfile {
                physical_cores: 8,
                logical_cores: 8,
                simd: crate::runtime::system_profile::SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: crate::runtime::system_profile::MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: crate::runtime::system_profile::DiskProfile {
                storage_class: crate::runtime::system_profile::StorageClass::Ssd,
                filesystem: crate::runtime::system_profile::FilesystemType::Apfs,
                sequential_write_mbps: 2000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        },
        4,
    );

    let job_id = JobId(30009);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Delete Waits For Workers", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.try_rar_extraction(job_id).await;
    assert_eq!(
        pipeline
            .rar_sets
            .get(&(job_id, "show".to_string()))
            .map(|state| state.active_workers),
        Some(2)
    );

    let first_done = next_extraction_done(&mut pipeline).await;
    let second_done = next_extraction_done(&mut pipeline).await;

    pipeline.handle_extraction_done(first_done).await;
    assert_eq!(
        pipeline
            .rar_sets
            .get(&(job_id, "show".to_string()))
            .map(|state| state.active_workers),
        Some(1)
    );
    for (filename, _) in &files {
        assert!(
            working_dir.join(filename).exists(),
            "{filename} should not be eagerly deleted while another set worker is active"
        );
    }

    pipeline.handle_extraction_done(second_done).await;
    for (filename, _) in &files {
        assert!(
            !working_dir.join(filename).exists(),
            "{filename} should be eagerly deleted after all set workers finish"
        );
    }
}

#[tokio::test]
async fn non_solid_rar_scheduler_skips_duplicate_ready_members() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner = RuntimeTuner::with_connection_limit(
        crate::runtime::system_profile::SystemProfile {
            cpu: crate::runtime::system_profile::CpuProfile {
                physical_cores: 8,
                logical_cores: 8,
                simd: crate::runtime::system_profile::SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: crate::runtime::system_profile::MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: crate::runtime::system_profile::DiskProfile {
                storage_class: crate::runtime::system_profile::StorageClass::Ssd,
                filesystem: crate::runtime::system_profile::FilesystemType::Apfs,
                sequential_write_mbps: 2000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        },
        4,
    );

    let job_id = JobId(30010);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Duplicate Ready Members", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let set_state = pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .expect("set state should exist");
    let plan = set_state
        .plan
        .as_mut()
        .expect("ready plan should exist after all volumes complete");
    let first = plan.ready_members[0].name.clone();
    let second = plan.ready_members[1].name.clone();
    plan.ready_members = vec![
        crate::pipeline::rar_state::RarReadyMember {
            name: first.clone(),
        },
        crate::pipeline::rar_state::RarReadyMember {
            name: first.clone(),
        },
        crate::pipeline::rar_state::RarReadyMember {
            name: second.clone(),
        },
    ];

    pipeline.try_rar_extraction(job_id).await;

    let set_state = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .expect("set state should exist");
    assert_eq!(set_state.active_workers, 2);
    assert_eq!(
        set_state.in_flight_members,
        [first.clone(), second.clone()].into_iter().collect()
    );

    let first_done = next_extraction_done(&mut pipeline).await;
    let second_done = next_extraction_done(&mut pipeline).await;
    let mut attempted_members = Vec::new();
    for done in [&first_done, &second_done] {
        match done {
            ExtractionDone::Batch {
                attempted, result, ..
            } => {
                assert_eq!(attempted.len(), 1);
                assert!(
                    result
                        .as_ref()
                        .is_ok_and(|outcome| outcome.failed.is_empty())
                );
                attempted_members.push(attempted[0].clone());
            }
            _ => panic!("expected batch extraction completion"),
        }
    }
    attempted_members.sort();
    assert_eq!(attempted_members, vec![first, second]);

    pipeline.handle_extraction_done(first_done).await;
    pipeline.handle_extraction_done(second_done).await;
}

#[tokio::test]
async fn non_solid_rar_scheduler_waits_for_link_dependency_source() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner = RuntimeTuner::with_connection_limit(
        crate::runtime::system_profile::SystemProfile {
            cpu: crate::runtime::system_profile::CpuProfile {
                physical_cores: 8,
                logical_cores: 8,
                simd: crate::runtime::system_profile::SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: crate::runtime::system_profile::MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: crate::runtime::system_profile::DiskProfile {
                storage_class: crate::runtime::system_profile::StorageClass::Ssd,
                filesystem: crate::runtime::system_profile::FilesystemType::Apfs,
                sequential_write_mbps: 2000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        },
        4,
    );

    let job_id = JobId(30011);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Link Dependency Source", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let set_key = (job_id, "show".to_string());
    let (source_member, dependent_member, source_first_volume, source_last_volume) = {
        let set_state = pipeline
            .rar_sets
            .get_mut(&set_key)
            .expect("set state should exist");
        let plan = set_state
            .plan
            .as_mut()
            .expect("ready plan should exist after all volumes complete");
        let source_member = plan.ready_members[0].name.clone();
        let dependent_member = plan.ready_members[1].name.clone();
        let source = plan
            .topology
            .members
            .iter()
            .find(|member| member.name == source_member)
            .expect("source member should be present in topology");
        let source_first_volume = source.first_volume;
        let source_last_volume = source.last_volume;
        plan.member_dependencies.insert(
            dependent_member.clone(),
            crate::pipeline::rar_state::RarMemberDependency {
                source_member: source_member.clone(),
                source_first_volume,
                source_last_volume,
            },
        );
        plan.ready_members = vec![
            crate::pipeline::rar_state::RarReadyMember {
                name: source_member.clone(),
            },
            crate::pipeline::rar_state::RarReadyMember {
                name: dependent_member.clone(),
            },
        ];
        (
            source_member,
            dependent_member,
            source_first_volume,
            source_last_volume,
        )
    };
    pipeline.extracted_members.insert(job_id, HashSet::new());
    resume_job_downloading_for_test(&mut pipeline, job_id);
    assert_eq!(
        pipeline
            .rar_sets
            .get(&set_key)
            .and_then(|set| set.plan.as_ref())
            .and_then(|plan| plan.member_dependencies.get(&dependent_member))
            .map(|dependency| {
                (
                    dependency.source_first_volume,
                    dependency.source_last_volume,
                )
            }),
        Some((source_first_volume, source_last_volume))
    );

    pipeline.try_rar_extraction(job_id).await;

    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("set state should exist");
    assert_eq!(set_state.active_workers, 1);
    assert_eq!(
        set_state.in_flight_members,
        [source_member.clone()].into_iter().collect()
    );

    let source_done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(source_done).await;
    pipeline.try_rar_extraction(job_id).await;

    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("set state should still exist");
    assert_eq!(set_state.active_workers, 1);
    assert_eq!(
        set_state.in_flight_members,
        [dependent_member].into_iter().collect()
    );
}

#[tokio::test]
async fn rar_identity_rebind_preserves_in_flight_workers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner = RuntimeTuner::with_connection_limit(
        crate::runtime::system_profile::SystemProfile {
            cpu: crate::runtime::system_profile::CpuProfile {
                physical_cores: 8,
                logical_cores: 8,
                simd: crate::runtime::system_profile::SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: crate::runtime::system_profile::MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: crate::runtime::system_profile::DiskProfile {
                storage_class: crate::runtime::system_profile::StorageClass::Ssd,
                filesystem: crate::runtime::system_profile::FilesystemType::Apfs,
                sequential_write_mbps: 2000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        },
        4,
    );

    let job_id = JobId(30012);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Rebind Preserves Workers", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.try_rar_extraction(job_id).await;

    let expected_in_flight: std::collections::HashSet<String> =
        ["E01.mkv".to_string(), "E02.mkv".to_string()]
            .into_iter()
            .collect();
    let set_key = (job_id, "show".to_string());
    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("set state should exist");
    assert_eq!(set_state.active_workers, 2);
    assert_eq!(set_state.in_flight_members, expected_in_flight);

    let touched_filenames = [files[0].0.clone()].into_iter().collect();
    pipeline.invalidate_archive_set_for_identity_rebind(job_id, "show", &touched_filenames);

    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("active set should survive identity rebind");
    assert_eq!(set_state.active_workers, 2);
    assert_eq!(set_state.in_flight_members, expected_in_flight);
    assert!(set_state.plan.is_none());

    pipeline.try_rar_extraction(job_id).await;
    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("set state should still exist");
    assert_eq!(set_state.active_workers, 2);
    assert_eq!(set_state.in_flight_members, expected_in_flight);

    let first_done = next_extraction_done(&mut pipeline).await;
    let second_done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(first_done).await;
    pipeline.handle_extraction_done(second_done).await;
    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("set state should still exist after completions");
    assert_eq!(set_state.active_workers, 0);
    assert!(set_state.in_flight_members.is_empty());
}

#[tokio::test]
async fn rar_identity_rebind_removes_empty_set_after_in_flight_workers_finish() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner = RuntimeTuner::with_connection_limit(
        crate::runtime::system_profile::SystemProfile {
            cpu: crate::runtime::system_profile::CpuProfile {
                physical_cores: 8,
                logical_cores: 8,
                simd: crate::runtime::system_profile::SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: crate::runtime::system_profile::MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: crate::runtime::system_profile::DiskProfile {
                storage_class: crate::runtime::system_profile::StorageClass::Ssd,
                filesystem: crate::runtime::system_profile::FilesystemType::Apfs,
                sequential_write_mbps: 2000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        },
        4,
    );

    let job_id = JobId(30014);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Rebind Removes Empty Set", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.try_rar_extraction(job_id).await;

    let set_key = (job_id, "show".to_string());
    let touched_filenames = files
        .iter()
        .map(|(filename, _)| filename.clone())
        .collect::<HashSet<_>>();
    pipeline.invalidate_archive_set_for_identity_rebind(job_id, "show", &touched_filenames);

    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("active set should survive rebind while extraction workers are still running");
    assert!(set_state.volume_files.is_empty());
    assert!(set_state.active_workers > 0);

    let first_done = next_extraction_done(&mut pipeline).await;
    let second_done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(first_done).await;
    pipeline.handle_extraction_done(second_done).await;

    assert!(
        !pipeline.rar_sets.contains_key(&set_key),
        "fully invalidated RAR set should be purged once in-flight workers finish"
    );
}

#[tokio::test]
async fn non_solid_rar_incremental_requires_member_chain_not_download_activity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30011);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Incremental Readiness", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    pipeline.active_downloads = 1;
    pipeline.active_download_passes.insert(job_id);
    pipeline.active_downloads_by_job.insert(job_id, 1);

    assert!(!pipeline.rar_member_can_start_extraction(job_id, "show", "E01.mkv"));
    pipeline.try_rar_extraction(job_id).await;
    let set_state = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .expect("set state should exist after first RAR volume");
    assert_eq!(set_state.active_workers, 0);
    assert!(set_state.in_flight_members.is_empty());

    write_and_complete_rar_volume(&mut pipeline, job_id, 1, &files[1].0, &files[1].1).await;

    assert!(pipeline.rar_member_can_start_extraction(job_id, "show", "E01.mkv"));
    pipeline.try_rar_extraction(job_id).await;
    let set_state = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .expect("set state should exist after second RAR volume");
    assert_eq!(set_state.active_workers, 1);
    assert_eq!(
        set_state.in_flight_members,
        ["E01.mkv".to_string()].into_iter().collect()
    );

    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E01.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected incremental batch extraction"),
    }
    pipeline.handle_extraction_done(done).await;
}

#[tokio::test]
async fn rar_extraction_capacity_pressure_keeps_member_waiting_without_repair_promotion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30014);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Extraction Capacity Retry", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 1, &files[1].0, &files[1].1).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    let key = (job_id, "show".to_string());
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&key)
            .expect("set state should exist after ready RAR member");
        set_state.active_workers = 1;
        set_state.in_flight_members.insert("E01.mkv".to_string());
    }

    let capacity_error = format!(
        "{}: synthetic EMFILE",
        crate::pipeline::capacity::FD_CAPACITY_ERROR_MARKER
    );
    pipeline
        .handle_extraction_done(ExtractionDone::Batch {
            job_id,
            set_name: "show".to_string(),
            attempted: vec!["E01.mkv".to_string()],
            result: Err(capacity_error.clone()),
        })
        .await;

    let set_state = pipeline
        .rar_sets
        .get(&key)
        .expect("set state should remain after capacity pressure");
    assert_eq!(set_state.active_workers, 0);
    assert!(!set_state.in_flight_members.contains("E01.mkv"));
    assert!(
        !pipeline
            .failed_extractions
            .get(&job_id)
            .is_some_and(|members| members.contains("E01.mkv")),
        "capacity pressure must not mark the member failed"
    );
    let pending_key = (job_id, "show".to_string(), RarCapacityRetryKind::Extraction);
    assert!(pipeline.pending_rar_capacity_retries.contains(&pending_key));

    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&key)
            .expect("set state should exist for duplicate capacity retry");
        set_state.active_workers = 1;
        set_state.in_flight_members.insert("E01.mkv".to_string());
    }
    pipeline
        .handle_extraction_done(ExtractionDone::Batch {
            job_id,
            set_name: "show".to_string(),
            attempted: vec!["E01.mkv".to_string()],
            result: Err(capacity_error),
        })
        .await;
    assert_eq!(
        pipeline
            .pending_rar_capacity_retries
            .iter()
            .filter(|key| **key == pending_key)
            .count(),
        1
    );

    resume_job_downloading_for_test(&mut pipeline, job_id);
    pipeline.try_rar_extraction(job_id).await;
    let set_state = pipeline
        .rar_sets
        .get(&key)
        .expect("set state should remain blocked by pending capacity retry");
    assert_eq!(set_state.active_workers, 0);
    assert!(!set_state.in_flight_members.contains("E01.mkv"));

    pipeline
        .handle_rar_capacity_retry(RarCapacityRetry {
            job_id,
            set_name: "show".to_string(),
            kind: RarCapacityRetryKind::Extraction,
        })
        .await;
    assert!(!pipeline.pending_rar_capacity_retries.contains(&pending_key));
    let set_state = pipeline
        .rar_sets
        .get(&key)
        .expect("set state should remain after extraction retry wakeup");
    assert_eq!(set_state.active_workers, 1);
    assert!(set_state.in_flight_members.contains("E01.mkv"));

    let done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(done).await;
}

#[tokio::test]
async fn rar_full_set_capacity_pressure_retries_without_failing_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30025);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Full Set Capacity Retry", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let set_name = "show".to_string();
    let key = (job_id, set_name.clone());
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&key)
            .expect("set state should exist after RAR volumes complete");
        set_state.active_workers = 1;
        set_state.in_flight_members.clear();
        set_state.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
        let plan = set_state
            .plan
            .as_mut()
            .expect("RAR plan should exist after volume facts are built");
        plan.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
    }
    pipeline
        .inflight_extractions
        .entry(job_id)
        .or_default()
        .insert(set_name.clone());

    let capacity_error = format!(
        "{}: synthetic EMFILE",
        crate::pipeline::capacity::FD_CAPACITY_ERROR_MARKER
    );
    pipeline
        .handle_extraction_done(ExtractionDone::FullSet {
            job_id,
            set_name: set_name.clone(),
            result: Err(capacity_error),
        })
        .await;

    assert!(!matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    assert!(!pipeline.failed_extractions.contains_key(&job_id));
    assert!(
        pipeline
            .inflight_extractions
            .get(&job_id)
            .is_none_or(|sets| !sets.contains(&set_name))
    );
    let set_state = pipeline
        .rar_sets
        .get(&key)
        .expect("RAR set should remain after full-set capacity pressure");
    assert_eq!(set_state.active_workers, 0);
    assert!(matches!(
        set_state.phase,
        crate::pipeline::rar_state::RarSetPhase::FallbackFullSet
    ));
    assert!(matches!(
        set_state.plan.as_ref().map(|plan| plan.phase),
        Some(crate::pipeline::rar_state::RarSetPhase::FallbackFullSet)
    ));
    let pending_key = (
        job_id,
        set_name.clone(),
        RarCapacityRetryKind::FullSetExtraction,
    );
    assert!(pipeline.pending_rar_capacity_retries.contains(&pending_key));

    resume_job_downloading_for_test(&mut pipeline, job_id);
    pipeline
        .handle_rar_capacity_retry(RarCapacityRetry {
            job_id,
            set_name: set_name.clone(),
            kind: RarCapacityRetryKind::FullSetExtraction,
        })
        .await;
    assert!(!pipeline.pending_rar_capacity_retries.contains(&pending_key));
    let set_state = pipeline
        .rar_sets
        .get(&key)
        .expect("RAR set should be extracting after full-set retry wakeup");
    assert_eq!(set_state.active_workers, 1);
    assert!(matches!(
        set_state.phase,
        crate::pipeline::rar_state::RarSetPhase::Extracting
    ));

    let done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(done).await;
}

#[tokio::test]
async fn rar_full_set_member_capacity_pressure_retries_without_repair_promotion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40127);
    let set_name =
        setup_extracting_rar_full_set(&mut pipeline, job_id, "RAR Full Set Member Capacity Retry")
            .await;
    let capacity_error = format!(
        "{}: synthetic EMFILE during read",
        crate::pipeline::capacity::FD_CAPACITY_ERROR_MARKER
    );

    pipeline
        .handle_extraction_done(ExtractionDone::FullSet {
            job_id,
            set_name: set_name.clone(),
            result: Ok(FullSetExtractionOutcome {
                extracted: vec!["E01.mkv".to_string()],
                failed: vec![("E02.mkv".to_string(), capacity_error)],
                selected_password: None,
            }),
        })
        .await;

    assert!(!matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    assert!(
        !pipeline
            .failed_extractions
            .get(&job_id)
            .is_some_and(|members| members.contains("E02.mkv")),
        "capacity pressure must not mark the failed member for repair"
    );
    let pending_key = (
        job_id,
        set_name.clone(),
        RarCapacityRetryKind::FullSetExtraction,
    );
    assert!(pipeline.pending_rar_capacity_retries.contains(&pending_key));
    let set_state = pipeline
        .rar_sets
        .get(&(job_id, set_name))
        .expect("RAR set should remain after member capacity pressure");
    assert_eq!(set_state.active_workers, 0);
    assert!(matches!(
        set_state.phase,
        crate::pipeline::rar_state::RarSetPhase::FallbackFullSet
    ));
}

#[tokio::test]
async fn rar_full_set_mixed_failures_defer_repair_when_capacity_pressure_is_present() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40128);
    let set_name =
        setup_extracting_rar_full_set(&mut pipeline, job_id, "RAR Full Set Mixed Capacity Retry")
            .await;
    let capacity_error = format!(
        "{}: synthetic EMFILE during read",
        crate::pipeline::capacity::FD_CAPACITY_ERROR_MARKER
    );

    pipeline
        .handle_extraction_done(ExtractionDone::FullSet {
            job_id,
            set_name: set_name.clone(),
            result: Ok(FullSetExtractionOutcome {
                extracted: Vec::new(),
                failed: vec![
                    ("E01.mkv".to_string(), capacity_error),
                    ("E02.mkv".to_string(), "Invalid checksum".to_string()),
                ],
                selected_password: None,
            }),
        })
        .await;

    assert!(
        !pipeline.failed_extractions.contains_key(&job_id),
        "mixed capacity passes must not promote any member until a capacity-free retry confirms it"
    );
    let pending_key = (job_id, set_name, RarCapacityRetryKind::FullSetExtraction);
    assert!(pipeline.pending_rar_capacity_retries.contains(&pending_key));
}

#[tokio::test]
async fn non_solid_rar_incremental_uses_ready_plan_even_if_complete_volumes_lag() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30013);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Incremental Ready Plan Wins", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let set_key = (job_id, "show".to_string());
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&set_key)
            .expect("set state should exist after all RAR volumes complete");
        let plan = set_state
            .plan
            .as_mut()
            .expect("ready plan should exist after all RAR volumes complete");
        plan.ready_members = vec![crate::pipeline::rar_state::RarReadyMember {
            name: "E02.mkv".to_string(),
        }];
        plan.topology.complete_volumes = [2u32].into_iter().collect();
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let mut topology = state
            .assembly
            .archive_topology_for("show")
            .cloned()
            .expect("assembly topology should exist for completed RAR set");
        topology.complete_volumes = [2u32].into_iter().collect();
        state
            .assembly
            .set_archive_topology("show".to_string(), topology);
    }

    assert!(pipeline.rar_member_can_start_extraction(job_id, "show", "E02.mkv"));

    resume_job_downloading_for_test(&mut pipeline, job_id);
    pipeline.try_rar_extraction(job_id).await;

    let set_state = pipeline
        .rar_sets
        .get(&set_key)
        .expect("set state should still exist while retry is in flight");
    assert_eq!(set_state.active_workers, 1);
    assert_eq!(
        set_state.in_flight_members,
        ["E02.mkv".to_string()].into_iter().collect()
    );

    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E02.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected incremental batch extraction"),
    }
    pipeline.handle_extraction_done(done).await;
}

#[tokio::test]
async fn solid_rar_keeps_later_members_ready_after_earlier_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30009);
    let fixture_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/rar5/rar5_solid.rar");
    let fixture_bytes = tokio::fs::read(&fixture_path).await.unwrap();
    let archive =
        weaver_unrar::RarArchive::open(std::fs::File::open(&fixture_path).unwrap()).unwrap();
    let member_names = archive.member_names();
    assert!(member_names.len() >= 3);

    let spec = rar_job_spec(
        "Solid Failure Continuation",
        &[("solid.rar".to_string(), fixture_bytes.clone())],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);
    write_and_complete_rar_volume(&mut pipeline, job_id, 0, "solid.rar", &fixture_bytes).await;

    // Rebuild with extracted + failed state that mirrors a solid archive
    // where earlier members were attempted before later members.
    pipeline
        .extracted_members
        .insert(job_id, [member_names[0].to_string()].into_iter().collect());
    pipeline
        .failed_extractions
        .insert(job_id, [member_names[1].to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "solid")
        .await
        .unwrap();

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "solid".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("solid set plan should exist");

    assert!(plan.is_solid);
    assert_eq!(plan.phase, crate::pipeline::rar_state::RarSetPhase::Ready);
    let ready_members: Vec<String> = plan
        .ready_members
        .into_iter()
        .map(|member| member.name)
        .collect();
    assert_eq!(
        ready_members,
        member_names[2..]
            .iter()
            .map(|member| member.to_string())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn solid_rar4_pipeline_extracts_large_fixture_end_to_end() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30010);
    let fixture_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/rar4/rar4_solid.rar");
    let fixture_bytes = tokio::fs::read(&fixture_path).await.unwrap();

    let spec = rar_job_spec(
        "RAR4 Solid End-to-End",
        &[("solid.rar".to_string(), fixture_bytes.clone())],
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 0, "solid.rar", &fixture_bytes).await;

    pipeline.check_job_completion(job_id).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 4).await;

    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(
        "RAR4 Solid End-to-End",
    ));
    let dest_entries = std::fs::read_dir(&dest)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let working_entries = std::fs::read_dir(&working_dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(
        matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete)
        ),
        "job status: {:?}, dest entries: {:?}, working entries: {:?}",
        job_status_for_assert(&pipeline, job_id),
        dest_entries,
        working_entries
    );
    assert!(
        dest.join("sample.mkv").exists(),
        "dest entries: {:?}, working entries: {:?}",
        dest_entries,
        working_entries
    );
    assert!(
        dest.join("file1.txt").exists(),
        "dest entries: {:?}",
        dest_entries
    );
    assert!(
        dest.join("file2.txt").exists(),
        "dest entries: {:?}",
        dest_entries
    );
    assert!(!working_dir.join("solid.rar").exists());
}

#[tokio::test]
async fn check_job_completion_retains_par2_until_rar_extraction_finishes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30011);
    let mut files = build_multifile_multivolume_rar_set();
    files.push((
        "repair.vol00+01.par2".to_string(),
        b"retained-par2-placeholder".to_vec(),
    ));
    let spec = rar_job_spec("RAR Keeps PAR2 During Extraction", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().take(4).enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let par2_filename = &files[4].0;
    tokio::fs::write(working_dir.join(par2_filename), &files[4].1)
        .await
        .unwrap();
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        minimal_par2_file_set(),
        &[(4, par2_filename, 1, true)],
    );

    let set_state = pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .expect("RAR set state should exist after volume facts are built");
    set_state.active_workers = 1;
    set_state.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
    if let Some(plan) = set_state.plan.as_mut() {
        plan.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
    }

    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Extracting)
    );
    assert!(working_dir.join(par2_filename).exists());
    assert!(pipeline.par2_set(job_id).is_some());
    assert_eq!(
        pipeline
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&4))
            .map(|file| file.recovery_blocks),
        Some(1)
    );
    assert_eq!(
        pipeline
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&4))
            .map(|file| file.promoted),
        Some(true)
    );
}

#[tokio::test]
async fn check_job_completion_defers_verify_while_rar_workers_are_active() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30015);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Verify Barrier", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);
    pipeline
        .failed_extractions
        .insert(job_id, ["E02.mkv".to_string()].into_iter().collect());

    let set_state = pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .expect("RAR set state should exist");
    set_state.active_workers = 1;
    set_state.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
    if let Some(plan) = set_state.plan.as_mut() {
        plan.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
    }

    // Set status to Extracting to match what the real pipeline would have
    // done when extraction workers were spawned.  The bounded workload
    // queue may gate through QueuedExtract first, but by the time workers
    // are active the status is always Extracting.
    pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::Extracting;
    pipeline.pending_completion_checks.clear();

    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Extracting)
    );
    assert!(pipeline.pending_completion_checks.is_empty());
    assert_eq!(
        pipeline
            .metrics
            .verify_active
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert!(!pipeline.normalization_retried.contains(&job_id));

    let set_state = pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .expect("RAR set state should exist");
    set_state.active_workers = 0;
    set_state.in_flight_members.insert("E02.mkv".to_string());
    pipeline.pending_completion_checks.clear();

    pipeline.check_job_completion(job_id).await;

    assert!(pipeline.pending_completion_checks.is_empty());
    assert_eq!(
        pipeline
            .metrics
            .verify_active
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn partial_rar_extraction_does_not_bypass_par2_early() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30012);
    let mut files = build_multifile_multivolume_rar_set();
    files.push((
        "repair.vol00+01.par2".to_string(),
        b"retained-par2-placeholder".to_vec(),
    ));
    let spec = rar_job_spec("RAR Partial Keeps PAR2", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().take(2).enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let par2_filename = &files[4].0;
    tokio::fs::write(working_dir.join(par2_filename), &files[4].1)
        .await
        .unwrap();
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        minimal_par2_file_set(),
        &[(4, par2_filename, 1, true)],
    );

    pipeline.try_rar_extraction(job_id).await;
    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E01.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected batch extraction completion"),
    }
    pipeline.handle_extraction_done(done).await;

    assert!(!pipeline.par2_bypassed.contains(&job_id));
    assert!(working_dir.join(par2_filename).exists());
    assert!(pipeline.par2_set(job_id).is_some());
    assert_eq!(
        pipeline
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.files.get(&4))
            .map(|file| file.promoted),
        Some(true)
    );
}

#[tokio::test]
async fn rar_completion_prefers_incremental_batches_over_full_set_after_eager_delete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30013);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Completion Uses Batch", &files);
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
    pipeline.try_delete_volumes(job_id, "show");

    assert!(!working_dir.join("show.part01.rar").exists());
    assert!(!working_dir.join("show.part02.rar").exists());

    resume_job_downloading_for_test(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;

    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E02.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        ExtractionDone::FullSet { .. } => {
            panic!("RAR completion should not fall back to full-set extraction here")
        }
    }
    pipeline.handle_extraction_done(done).await;
}

#[tokio::test]
async fn eager_delete_exclusions_do_not_hide_suspect_deleted_rar_damage() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30014);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Verify Keeps Suspect Deleted Volumes", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline.eagerly_deleted.insert(
        job_id,
        ["show.part02.rar".to_string(), "show.part04.rar".to_string()]
            .into_iter()
            .collect(),
    );
    pipeline
        .extracted_members
        .insert(job_id, ["E02.mkv".to_string()].into_iter().collect());
    pipeline
        .failed_extractions
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    let mut verification = weaver_par2::VerificationResult {
        files: vec![
            weaver_par2::verify::FileVerification {
                file_id: weaver_par2::FileId::from_bytes([1; 16]),
                filename: "show.part02.rar".to_string(),
                status: weaver_par2::verify::FileStatus::Missing,
                valid_slices: vec![false; 3],
                missing_slice_count: 3,
            },
            weaver_par2::verify::FileVerification {
                file_id: weaver_par2::FileId::from_bytes([2; 16]),
                filename: "show.part04.rar".to_string(),
                status: weaver_par2::verify::FileStatus::Missing,
                valid_slices: vec![false; 2],
                missing_slice_count: 2,
            },
        ],
        recovery_blocks_available: 3,
        total_missing_blocks: 5,
        repairable: weaver_par2::verify::Repairability::Insufficient {
            blocks_needed: 5,
            blocks_available: 3,
            deficit: 2,
        },
    };

    let (skipped_blocks, retained_suspect_blocks) =
        pipeline.apply_eager_delete_exclusions(job_id, &mut verification);

    assert_eq!(skipped_blocks, 2);
    assert_eq!(retained_suspect_blocks, 3);
    assert_eq!(verification.total_missing_blocks, 3);
    assert!(matches!(
        verification.files[0].status,
        weaver_par2::verify::FileStatus::Missing
    ));
    assert_eq!(verification.files[0].missing_slice_count, 3);
    assert!(matches!(
        verification.files[1].status,
        weaver_par2::verify::FileStatus::Complete
    ));
    assert_eq!(verification.files[1].missing_slice_count, 0);
    assert!(matches!(
        verification.repairable,
        weaver_par2::verify::Repairability::Repairable {
            blocks_needed: 3,
            blocks_available: 3
        }
    ));

    pipeline.recompute_volume_safety_from_verification(job_id, &verification);

    let verified_suspect = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .map(|state| state.verified_suspect_volumes.clone())
        .unwrap_or_default();
    assert!(verified_suspect.contains(&1));
    assert!(!verified_suspect.contains(&3));
}

#[tokio::test]
async fn recoverable_full_set_extraction_error_defers_to_repair_flow() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30022);
    let spec = standalone_job_spec(
        "Recoverable Full Set Extraction Error",
        &[("sample.bin".to_string(), 100)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline
        .inflight_extractions
        .entry(job_id)
        .or_default()
        .insert("archive.zip".to_string());

    pipeline
        .handle_extraction_done(ExtractionDone::FullSet {
            job_id,
            set_name: "archive.zip".to_string(),
            result: Err("failed to extract sample.mkv: Invalid checksum".to_string()),
        })
        .await;

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );
    assert_eq!(
        pipeline.failed_extractions.get(&job_id),
        Some(&HashSet::from(["archive.zip".to_string()]))
    );
    assert!(
        pipeline
            .inflight_extractions
            .get(&job_id)
            .is_none_or(HashSet::is_empty)
    );
}

#[tokio::test]
async fn nonrecoverable_full_set_extraction_error_fails_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30023);
    let spec = standalone_job_spec(
        "Nonrecoverable Full Set Extraction Error",
        &[("sample.bin".to_string(), 100)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline
        .inflight_extractions
        .entry(job_id)
        .or_default()
        .insert("archive.zip".to_string());

    pipeline
        .handle_extraction_done(ExtractionDone::FullSet {
            job_id,
            set_name: "archive.zip".to_string(),
            result: Err("failed to parse zip central directory".to_string()),
        })
        .await;

    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
}

#[tokio::test]
async fn non_rar_full_set_capacity_pressure_still_fails_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30026);
    let spec = standalone_job_spec(
        "Non-RAR Capacity Pressure Still Fails",
        &[("sample.bin".to_string(), 100)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline
        .inflight_extractions
        .entry(job_id)
        .or_default()
        .insert("archive.zip".to_string());

    pipeline
        .handle_extraction_done(ExtractionDone::FullSet {
            job_id,
            set_name: "archive.zip".to_string(),
            result: Err(format!(
                "{}: synthetic EMFILE",
                crate::pipeline::capacity::FD_CAPACITY_ERROR_MARKER
            )),
        })
        .await;

    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    assert!(pipeline.pending_rar_capacity_retries.is_empty());
}

#[tokio::test]
async fn clean_verify_retries_non_rar_full_set_extraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30024);
    let spec = standalone_job_spec(
        "Non-RAR Extraction Retry After Verify",
        &[("archive.zip".to_string(), 16)],
    );
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join("archive.zip"), b"not-a-real-zip")
        .await
        .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 14)
            .unwrap();
        state.assembly.set_archive_topology(
            "archive.zip".to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Zip,
                volume_map: HashMap::from([("archive.zip".to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "sample.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: 0,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    }

    install_test_par2_runtime(&mut pipeline, job_id, minimal_par2_file_set(), &[]);
    pipeline
        .failed_extractions
        .insert(job_id, ["archive.zip".to_string()].into_iter().collect());

    pipeline.check_job_completion(job_id).await;

    assert!(!pipeline.failed_extractions.contains_key(&job_id));
    let done = next_extraction_done(&mut pipeline).await;
    match done {
        ExtractionDone::FullSet { set_name, .. } => {
            assert_eq!(set_name, "archive.zip");
        }
        _ => panic!("expected full-set extraction retry"),
    }
}

#[tokio::test]
async fn rar_state_recompute_supplements_stale_volume_registry_from_assembly() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30025);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Registry Merge", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .unwrap()
        .volume_files = std::collections::BTreeMap::from([(0u32, "show.part01.rar".to_string())]);

    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    let volume_paths = pipeline.volume_paths_for_rar_set(job_id, "show");
    assert_eq!(volume_paths.len(), 4);
    assert!(volume_paths.contains_key(&0));
    assert!(volume_paths.contains_key(&1));
    assert!(volume_paths.contains_key(&2));
    assert!(volume_paths.contains_key(&3));

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("RAR plan should exist");
    assert_eq!(plan.topology.complete_volumes.len(), 4);
    assert!(plan.topology.volume_map.values().any(|volume| *volume == 1));
    assert!(plan.topology.volume_map.values().any(|volume| *volume == 2));
    assert!(plan.topology.volume_map.values().any(|volume| *volume == 3));
}

#[tokio::test]
async fn incremental_rar_member_extraction_uses_member_span_volume_window() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30090);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Member Span Window", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let all_paths = pipeline.volume_paths_for_rar_set(job_id, "show");
    assert_eq!(all_paths.len(), 4);

    let member_paths = pipeline.volume_paths_for_rar_members(
        job_id,
        "show",
        &["E02.mkv".to_string()],
        &all_paths,
        true,
        false,
    );
    assert_eq!(member_paths.keys().copied().collect::<Vec<_>>(), vec![2, 3]);

    {
        let plan = pipeline
            .rar_sets
            .get_mut(&(job_id, "show".to_string()))
            .and_then(|state| state.plan.as_mut())
            .expect("RAR plan should exist");
        plan.member_dependencies.insert(
            "E02.mkv".to_string(),
            crate::pipeline::rar_state::RarMemberDependency {
                source_member: "E01.mkv".to_string(),
                source_first_volume: 0,
                source_last_volume: 1,
            },
        );
    }
    let filecopy_paths = pipeline.volume_paths_for_rar_members(
        job_id,
        "show",
        &["E02.mkv".to_string()],
        &all_paths,
        true,
        false,
    );
    assert_eq!(
        filecopy_paths.keys().copied().collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );

    let without_cached_headers = pipeline.volume_paths_for_rar_members(
        job_id,
        "show",
        &["E02.mkv".to_string()],
        &all_paths,
        false,
        false,
    );
    assert_eq!(without_cached_headers.len(), all_paths.len());

    let solid_paths = pipeline.volume_paths_for_rar_members(
        job_id,
        "show",
        &["E02.mkv".to_string()],
        &all_paths,
        true,
        true,
    );
    assert_eq!(solid_paths.len(), all_paths.len());
}

#[tokio::test]
async fn generic_par2_repair_requeues_extraction_for_7z_and_gzip_payloads() {
    for (job_id, payload_filename, original_payload) in [
        (
            JobId(30082),
            "archive.7z",
            vec![0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C, 0x00, 0x04],
        ),
        (
            JobId(30083),
            "payload.gz",
            vec![0x1F, 0x8B, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03],
        ),
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let index_filename = "repair.par2";
        let recovery_filename = "repair.vol00+01.par2";
        let damaged_payload = vec![0u8; original_payload.len()];
        let par2_bytes = build_test_par2_index(
            payload_filename,
            &original_payload,
            original_payload.len() as u64,
        );
        let recovery_bytes = vec![0xCC; original_payload.len()];
        let spec = JobSpec {
            name: format!("Archive Repair {}", job_id.0),
            password: None,
            total_bytes: (original_payload.len() + par2_bytes.len() + recovery_bytes.len()) as u64,
            category: None,
            metadata: vec![],
            files: vec![
                FileSpec {
                    filename: payload_filename.to_string(),
                    role: FileRole::from_filename(payload_filename),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![segment_spec! {
                        number: 0,
                        bytes: original_payload.len() as u32,
                        message_id: format!("archive-{}-0@example.com", job_id.0),
                    }],
                },
                FileSpec {
                    filename: index_filename.to_string(),
                    role: FileRole::from_filename(index_filename),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![segment_spec! {
                        number: 0,
                        bytes: par2_bytes.len() as u32,
                        message_id: format!("archive-{}-index@example.com", job_id.0),
                    }],
                },
                FileSpec {
                    filename: recovery_filename.to_string(),
                    role: FileRole::from_filename(recovery_filename),
                    groups: vec!["alt.binaries.test".to_string()],
                    posted_at_epoch: None,
                    segments: vec![segment_spec! {
                        number: 0,
                        bytes: recovery_bytes.len() as u32,
                        message_id: format!("archive-{}-recovery@example.com", job_id.0),
                    }],
                },
            ],
        };
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

        tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
            .await
            .unwrap();
        {
            let state = pipeline.jobs.get_mut(&job_id).unwrap();
            state.download_queue = DownloadQueue::new();
            state.recovery_queue = DownloadQueue::new();
        }
        write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
        write_and_complete_file(&mut pipeline, job_id, 2, recovery_filename, &recovery_bytes).await;
        install_test_par2_runtime(
            &mut pipeline,
            job_id,
            build_repairable_par2_set(
                payload_filename,
                &original_payload,
                original_payload.len() as u64,
                1,
            ),
            &[
                (1, index_filename, 0, false),
                (2, recovery_filename, 1, true),
            ],
        );

        pipeline.check_job_completion(job_id).await;

        let queued_job = pipeline
            .pending_completion_checks
            .pop_front()
            .expect("post-repair completion should be scheduled");
        assert_eq!(queued_job, job_id);
        pipeline.check_job_completion(queued_job).await;

        assert!(matches!(
            pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
            Some(JobStatus::Extracting | JobStatus::QueuedExtract)
        ));
        let done = next_extraction_done(&mut pipeline).await;
        match done {
            ExtractionDone::FullSet { set_name, .. } => {
                assert_eq!(set_name, payload_filename);
            }
            _ => panic!("expected full-set extraction after generic PAR2 repair"),
        }
    }
}

#[tokio::test]
async fn no_par2_retry_reclassifies_obfuscated_rar_redownload_as_7z() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30079);
    let filename = "51273aad56a8b904e96928935278a627";
    let rar_bytes = rar5_fixture_bytes("rar5_store.rar");
    let seven_zip_bytes = vec![0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C, 0x00, 0x04];
    let spec = rar_job_spec(
        "Obfuscated RAR Retry Reclassifies As 7z",
        &[(filename.to_string(), rar_bytes.clone())],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);
    pipeline.jobs.get_mut(&job_id).unwrap().download_queue = DownloadQueue::new();

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, filename, &rar_bytes).await;
    pipeline
        .failed_extractions
        .insert(job_id, HashSet::from([filename.to_string()]));

    resume_job_downloading_for_test(&mut pipeline, job_id);
    pipeline.check_job_completion(job_id).await;

    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        let file = state
            .assembly
            .file(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap();
        assert!(matches!(
            pipeline.classified_role_for_file(job_id, file),
            FileRole::Unknown
        ));
        assert!(state.assembly.archive_topology_for(filename).is_none());
    }
    assert!(pipeline.rar_sets.is_empty());
    assert!(
        pipeline
            .db
            .load_all_archive_headers(job_id)
            .unwrap()
            .is_empty()
    );
    assert!(
        pipeline
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .is_empty()
    );

    write_and_complete_file(&mut pipeline, job_id, 0, filename, &seven_zip_bytes).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    let file = state
        .assembly
        .file(NzbFileId {
            job_id,
            file_index: 0,
        })
        .unwrap();
    assert!(matches!(
        pipeline.classified_role_for_file(job_id, file),
        weaver_model::files::FileRole::SevenZipArchive
    ));
    let topology = state
        .assembly
        .archive_topology_for(filename)
        .expect("replacement payload should create a fresh 7z topology");
    assert_eq!(
        topology.archive_type,
        crate::jobs::assembly::ArchiveType::SevenZip
    );
    assert!(topology.complete_volumes.contains(&0));
    assert!(matches!(
        state.assembly.set_extraction_readiness(filename),
        crate::jobs::assembly::ExtractionReadiness::Ready
    ));
}

#[tokio::test]
async fn re_registering_identical_rar_facts_still_recomputes_readiness() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30030);
    let filename = "archive.rar";
    let fixture_bytes = rar5_fixture_bytes("rar5_store.rar");
    let spec = rar_job_spec(
        "RAR Recompute On Same Facts",
        &[(filename.to_string(), fixture_bytes.clone())],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, filename, &fixture_bytes).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let topo = state.assembly.archive_topology_for_mut("archive").unwrap();
        topo.complete_volumes.clear();
    }

    pipeline
        .refresh_archive_state_for_completed_file(
            job_id,
            NzbFileId {
                job_id,
                file_index: 0,
            },
            true,
        )
        .await;
    drain_rar_refreshes(&mut pipeline).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    let topo = state.assembly.archive_topology_for("archive").unwrap();
    assert!(topo.complete_volumes.contains(&0));
    assert!(matches!(
        state.assembly.set_extraction_readiness("archive"),
        crate::jobs::assembly::ExtractionReadiness::Ready
    ));
}

#[tokio::test]
async fn no_par2_rar_failure_requeues_member_owner_volumes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30027);
    let files = vec![("archive.rar".to_string(), vec![1u8; 64])];
    let spec = rar_job_spec("No PAR2 RAR Retry", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
        state.assembly.set_archive_topology(
            "archive".to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Rar,
                volume_map: HashMap::from([("archive.rar".to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "work/sample.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: 0,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    }
    pipeline
        .failed_extractions
        .insert(job_id, HashSet::from(["work/sample.mkv".to_string()]));

    pipeline.check_job_completion(job_id).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(state.status, JobStatus::Downloading));
    assert_eq!(state.download_queue.len(), 1);
    assert_eq!(state.assembly.complete_data_file_count(), 0);
    assert!(!pipeline.failed_extractions.contains_key(&job_id));
    assert!(pipeline.normalization_retried.contains(&job_id));
}

#[tokio::test]
async fn no_par2_runtime_rar_failure_requeues_owner_volumes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30091);
    let filename = "51273aad56a8b904e96928935278a627";
    let set_name = filename.to_string();
    let member_name = "sample.mkv".to_string();
    let files = vec![(filename.to_string(), vec![1u8; 64])];
    let spec = rar_job_spec("No PAR2 Runtime RAR Retry", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
    }

    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::from([(filename.to_string(), 0)]),
        complete_volumes: [0u32].into_iter().collect(),
        expected_volume_count: Some(1),
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: member_name.clone(),
            first_volume: 0,
            last_volume: 0,
            unpacked_size: 0,
        }],
        unresolved_spans: Vec::new(),
    };
    pipeline.rar_sets.insert(
        (job_id, set_name.clone()),
        crate::pipeline::archive::rar_state::RarSetState {
            volume_files: std::collections::BTreeMap::from([(0, filename.to_string())]),
            phase: crate::pipeline::archive::rar_state::RarSetPhase::AwaitingRepair,
            plan: Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
                phase: crate::pipeline::archive::rar_state::RarSetPhase::AwaitingRepair,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec![member_name.clone()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::new(),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology,
                fallback_reason: None,
            }),
            ..Default::default()
        },
    );
    pipeline
        .failed_extractions
        .insert(job_id, HashSet::from([member_name]));

    pipeline.check_job_completion(job_id).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(state.status, JobStatus::Downloading));
    assert_eq!(state.download_queue.len(), 1);
    assert_eq!(state.assembly.complete_data_file_count(), 0);
    assert!(!pipeline.rar_sets.contains_key(&(job_id, set_name)));
    assert!(!pipeline.failed_extractions.contains_key(&job_id));
    assert!(pipeline.normalization_retried.contains(&job_id));
}

#[tokio::test]
async fn exhausted_rar_failed_member_skips_lower_bound_recovery_preflight() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30191);
    let filename = "archive.rar";
    let files = vec![(filename.to_string(), vec![1u8; 64])];
    let spec = rar_job_spec("Exhausted RAR Lower Bound Skip", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.assembly.set_archive_topology(
            "archive".to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Rar,
                volume_map: HashMap::from([(filename.to_string(), 0)]),
                complete_volumes: [0u32].into_iter().collect(),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "sample.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: 0,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    }
    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);

    pipeline
        .test_promote_recovery_for_failed_member(job_id, "archive", "sample.mkv")
        .await;

    assert_eq!(pipeline.par2_lower_bound_preflight_calls, 0);
    assert_eq!(pipeline.jobs.get(&job_id).unwrap().download_queue.len(), 0);
}

#[tokio::test]
async fn rar_waiting_for_missing_volumes_without_par2_fails_after_download_completion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30077);
    let working_dir = intermediate_dir.join("rar-missing-tail");
    let mut state = minimal_job_state(job_id, "RAR Missing Tail", working_dir);
    state.download_queue = DownloadQueue::new();
    state.recovery_queue = DownloadQueue::new();
    state.status = JobStatus::Downloading;
    state.refresh_runtime_lanes_from_status();
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::from([
            ("show.part01.rar".to_string(), 0),
            ("show.part02.rar".to_string(), 1),
        ]),
        complete_volumes: [0u32, 1u32].into_iter().collect(),
        expected_volume_count: Some(4),
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: "work/sample.mkv".to_string(),
            first_volume: 0,
            last_volume: 3,
            unpacked_size: 0,
        }],
        unresolved_spans: vec![crate::jobs::assembly::ArchivePendingSpan {
            first_volume: 2,
            last_volume: 3,
        }],
    };
    state
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());
    pipeline.jobs.insert(job_id, state);
    pipeline.job_order.push(job_id);
    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([
                (0u32, dummy_rar_volume_facts(0)),
                (1u32, dummy_rar_volume_facts(1)),
            ]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            shared_kdf_cache: std::sync::Arc::new(weaver_unrar::crypto::KdfCache::new()),
            verified_suspect_volumes: HashSet::new(),
            active_workers: 0,
            in_flight_members: HashSet::new(),
            extraction_generation: 0,
            phase: rar_state::RarSetPhase::WaitingForVolumes,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["work/sample.mkv".to_string()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::from([2u32, 3u32]),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology,
                fallback_reason: None,
            }),
        },
    );

    pipeline.check_job_completion(job_id).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!("job should have failed");
    };
    assert!(error.contains("no PAR2 metadata is available for repair"));
}

#[tokio::test]
async fn legacy_reconcile_schedules_waiting_rar_completion_check() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30078);
    let working_dir = intermediate_dir.join("rar-waiting-no-spin");
    let mut state = minimal_job_state(job_id, "RAR Waiting No Spin", working_dir);
    state.download_queue = DownloadQueue::new();
    state.recovery_queue = DownloadQueue::new();
    state.status = JobStatus::Downloading;
    state.refresh_runtime_lanes_from_status();
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::from([
            ("show.part01.rar".to_string(), 0),
            ("show.part02.rar".to_string(), 1),
        ]),
        complete_volumes: [0u32, 1u32].into_iter().collect(),
        expected_volume_count: Some(4),
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: "work/sample.mkv".to_string(),
            first_volume: 0,
            last_volume: 3,
            unpacked_size: 0,
        }],
        unresolved_spans: vec![crate::jobs::assembly::ArchivePendingSpan {
            first_volume: 2,
            last_volume: 3,
        }],
    };
    state
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());
    pipeline.jobs.insert(job_id, state);
    pipeline.job_order.push(job_id);
    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([
                (0u32, dummy_rar_volume_facts(0)),
                (1u32, dummy_rar_volume_facts(1)),
            ]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            shared_kdf_cache: std::sync::Arc::new(weaver_unrar::crypto::KdfCache::new()),
            verified_suspect_volumes: HashSet::new(),
            active_workers: 0,
            in_flight_members: HashSet::new(),
            extraction_generation: 0,
            phase: rar_state::RarSetPhase::WaitingForVolumes,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["work/sample.mkv".to_string()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::from([2u32, 3u32]),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology,
                fallback_reason: None,
            }),
        },
    );

    pipeline.pending_completion_checks.clear();

    pipeline.reconcile_job_progress(job_id).await;

    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id]
    );
    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(state.status, JobStatus::Downloading);
}

#[tokio::test]
async fn rar_completion_waiting_for_volumes_does_not_requeue_itself() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30079);
    let working_dir = intermediate_dir.join("rar-waiting-check-no-spin");
    let mut state = minimal_job_state(job_id, "RAR Waiting Check No Spin", working_dir);
    state.status = JobStatus::Downloading;
    state.refresh_runtime_lanes_from_status();
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::from([
            ("show.part01.rar".to_string(), 0),
            ("show.part02.rar".to_string(), 1),
        ]),
        complete_volumes: [0u32, 1u32].into_iter().collect(),
        expected_volume_count: Some(4),
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: "work/sample.mkv".to_string(),
            first_volume: 0,
            last_volume: 3,
            unpacked_size: 0,
        }],
        unresolved_spans: vec![crate::jobs::assembly::ArchivePendingSpan {
            first_volume: 2,
            last_volume: 3,
        }],
    };
    state
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());
    pipeline.jobs.insert(job_id, state);
    pipeline.job_order.push(job_id);
    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([
                (0u32, dummy_rar_volume_facts(0)),
                (1u32, dummy_rar_volume_facts(1)),
            ]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            shared_kdf_cache: std::sync::Arc::new(weaver_unrar::crypto::KdfCache::new()),
            verified_suspect_volumes: HashSet::new(),
            active_workers: 0,
            in_flight_members: HashSet::new(),
            extraction_generation: 0,
            phase: rar_state::RarSetPhase::Ready,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::Ready,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["work/sample.mkv".to_string()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::from([2u32, 3u32]),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology,
                fallback_reason: None,
            }),
        },
    );

    pipeline.pending_completion_checks.clear();

    pipeline.check_job_completion(job_id).await;

    assert!(pipeline.pending_completion_checks.is_empty());
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Downloading)
    );
}

#[tokio::test]
async fn clean_member_keeps_failed_neighbor_boundary_volume_suspect() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30015);
    let files = vec![
        ("show.part01.rar".to_string(), vec![1u8]),
        ("show.part02.rar".to_string(), vec![2u8]),
        ("show.part03.rar".to_string(), vec![3u8]),
    ];
    let spec = rar_job_spec("RAR Boundary Suspect Claims", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: std::collections::HashMap::from([
            ("show.part01.rar".to_string(), 0),
            ("show.part02.rar".to_string(), 1),
            ("show.part03.rar".to_string(), 2),
        ]),
        complete_volumes: [0u32, 1u32, 2u32].into_iter().collect(),
        expected_volume_count: Some(3),
        members: vec![
            crate::jobs::assembly::ArchiveMember {
                name: "E10.mkv".to_string(),
                first_volume: 0,
                last_volume: 1,
                unpacked_size: 0,
            },
            crate::jobs::assembly::ArchiveMember {
                name: "E11.mkv".to_string(),
                first_volume: 1,
                last_volume: 2,
                unpacked_size: 0,
            },
        ],
        unresolved_spans: Vec::new(),
    };
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());

    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([
                (0u32, dummy_rar_volume_facts(0)),
                (1u32, dummy_rar_volume_facts(1)),
                (2u32, dummy_rar_volume_facts(2)),
            ]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            shared_kdf_cache: std::sync::Arc::new(weaver_unrar::crypto::KdfCache::new()),
            verified_suspect_volumes: std::collections::HashSet::from([1u32]),
            active_workers: 0,
            in_flight_members: std::collections::HashSet::new(),
            extraction_generation: 0,
            phase: rar_state::RarSetPhase::Ready,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::Ready,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["E10.mkv".to_string(), "E11.mkv".to_string()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: std::collections::HashSet::new(),
                deletion_eligible: std::collections::HashSet::new(),
                delete_decisions: std::collections::BTreeMap::from([(
                    1u32,
                    rar_state::RarVolumeDeleteDecision {
                        owners: vec!["E10.mkv".to_string(), "E11.mkv".to_string()],
                        clean_owners: vec!["E11.mkv".to_string()],
                        failed_owners: vec!["E10.mkv".to_string()],
                        pending_owners: Vec::new(),
                        unresolved_boundary: false,
                        ownership_eligible: false,
                    },
                )]),
                topology,
                fallback_reason: None,
            }),
        },
    );

    let suspect = pipeline.suspect_rar_volumes_for_job(job_id);
    let decision = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .and_then(|plan| plan.delete_decisions.get(&1))
        .unwrap();

    assert!(suspect.contains(&1));
    assert!(!Pipeline::claim_clean_rar_volume(decision));
}

#[tokio::test]
async fn normalization_refresh_preserves_deleted_untouched_rar_facts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30016);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Normalization Keeps Facts", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    tokio::fs::remove_file(working_dir.join("show.part01.rar"))
        .await
        .unwrap();
    tokio::fs::remove_file(working_dir.join("show.part02.rar"))
        .await
        .unwrap();

    pipeline
        .refresh_rar_topology_after_normalization(
            job_id,
            &["show.part03.rar".to_string(), "show.part04.rar".to_string()]
                .into_iter()
                .collect(),
        )
        .await
        .unwrap();

    let facts: Vec<u32> = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .expect("RAR set should exist")
        .facts
        .keys()
        .copied()
        .collect();
    assert_eq!(facts, vec![0, 1, 2, 3]);
    assert_eq!(
        pipeline
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .get("show")
            .map(|rows| rows.len()),
        Some(4)
    );
}

#[tokio::test]
async fn clean_verify_after_file_swap_refreshes_stale_rar_snapshot_without_volume_zero() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30021);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Swap Refreshes Snapshot", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 1, &files[1].0, &files[1].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 2, &files[2].0, &files[3].1).await;
    write_and_complete_rar_volume(&mut pipeline, job_id, 3, &files[3].0, &files[2].1).await;

    for (file_index, (filename, _)) in files.iter().enumerate() {
        let current_bytes = tokio::fs::read(working_dir.join(filename)).await.unwrap();
        persist_completed_file_hash(
            &pipeline,
            job_id,
            file_index as u32,
            filename,
            &current_bytes,
        )
        .await;
    }

    tokio::fs::remove_file(working_dir.join(&files[0].0))
        .await
        .unwrap();
    tokio::fs::remove_file(working_dir.join(&files[1].0))
        .await
        .unwrap();
    pipeline.eagerly_deleted.insert(
        job_id,
        [files[0].0.clone(), files[1].0.clone()]
            .into_iter()
            .collect(),
    );

    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);
    pipeline
        .extracted_members
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline
        .failed_extractions
        .insert(job_id, ["E02.mkv".to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    resume_job_downloading_for_test(&mut pipeline, job_id);

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("RAR plan should exist after placement correction");
    let suspect_volumes = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .map(|state| state.verified_suspect_volumes.clone())
        .expect("RAR state should exist after placement correction");
    let part03_identity = pipeline
        .file_identity(
            job_id,
            NzbFileId {
                job_id,
                file_index: 2,
            },
        )
        .cloned()
        .expect("swapped volume identity should persist after placement correction");
    let part04_identity = pipeline
        .file_identity(
            job_id,
            NzbFileId {
                job_id,
                file_index: 3,
            },
        )
        .cloned()
        .expect("counterpart swapped volume identity should persist after placement correction");
    assert!(pipeline.invalid_rar_retry_frontier_reason(job_id).is_none());
    assert_eq!(suspect_volumes, [2, 3].into_iter().collect());
    assert_eq!(part03_identity.current_filename, "show.part04.rar");
    assert_eq!(
        part03_identity.canonical_filename.as_deref(),
        Some("show.part04.rar")
    );
    assert_eq!(
        part03_identity.classification_source,
        FileIdentitySource::Par2
    );
    assert_eq!(part04_identity.current_filename, "show.part03.rar");
    assert_eq!(
        part04_identity.canonical_filename.as_deref(),
        Some("show.part03.rar")
    );
    assert_eq!(
        part04_identity.classification_source,
        FileIdentitySource::Par2
    );
    assert_eq!(
        Pipeline::rar_volume_filename(&plan.topology.volume_map, 2),
        Some("show.part03.rar")
    );
    assert_eq!(
        Pipeline::rar_volume_filename(&plan.topology.volume_map, 3),
        Some("show.part04.rar")
    );
    assert!(
        plan.ready_members
            .iter()
            .any(|member| member.name == "E02.mkv"),
        "stale cached headers must be refreshed so the swapped member can retry: {:?}",
        plan.waiting_on_volumes
    );

    let done = next_extraction_done(&mut pipeline).await;
    match &done {
        ExtractionDone::Batch {
            attempted, result, ..
        } => {
            assert_eq!(attempted, &vec!["E02.mkv".to_string()]);
            assert!(
                result
                    .as_ref()
                    .is_ok_and(|outcome| outcome.failed.is_empty())
            );
        }
        _ => panic!("expected incremental retry batch"),
    }
}

#[tokio::test]
async fn ownerless_live_rar_plan_error_requires_named_member_facts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30083);
    let working_dir = temp_dir.path().join("ownerless-live-guard");
    let mut state = minimal_job_state(job_id, "Ownerless Live Guard", working_dir);
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::from([("show.part01.rar".to_string(), 0)]),
        complete_volumes: [0u32].into_iter().collect(),
        expected_volume_count: Some(1),
        members: Vec::new(),
        unresolved_spans: Vec::new(),
    };
    state
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());
    pipeline.jobs.insert(job_id, state);
    let mut stale_named_facts = dummy_named_rar_volume_facts(0, "E01.mkv");
    stale_named_facts.members[0].is_directory = true;

    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: BTreeMap::from([(0u32, stale_named_facts)]),
            volume_files: BTreeMap::from([(0u32, "show.part01.rar".to_string())]),
            cached_headers: None,
            shared_kdf_cache: std::sync::Arc::new(weaver_unrar::crypto::KdfCache::new()),
            verified_suspect_volumes: HashSet::new(),
            active_workers: 0,
            in_flight_members: HashSet::new(),
            extraction_generation: 0,
            phase: rar_state::RarSetPhase::WaitingForVolumes,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: Vec::new(),
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::new(),
                deletion_eligible: HashSet::new(),
                delete_decisions: BTreeMap::from([(
                    0u32,
                    rar_state::RarVolumeDeleteDecision {
                        owners: Vec::new(),
                        clean_owners: Vec::new(),
                        failed_owners: Vec::new(),
                        pending_owners: Vec::new(),
                        unresolved_boundary: false,
                        ownership_eligible: false,
                    },
                )]),
                topology,
                fallback_reason: None,
            }),
        },
    );

    let error = pipeline
        .ownerless_live_rar_plan_error_for_job(job_id)
        .expect("named member facts should make ownerless live plans invalid");
    assert!(error.contains("ownerless present RAR volumes"));

    pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .unwrap()
        .facts
        .insert(0, dummy_rar_volume_facts(0));
    assert!(
        pipeline
            .ownerless_live_rar_plan_error_for_job(job_id)
            .is_none(),
        "empty malformed facts stay non-fatal for restore-time eager-delete auditing"
    );
}

#[tokio::test]
async fn rar_completion_waits_for_pending_refresh_before_terminal_decision() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30084);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Pending Refresh Barrier", &files);

    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let topology = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for("show"))
        .cloned()
        .expect("RAR topology should exist after all volumes complete");
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&(job_id, "show".to_string()))
            .expect("RAR set state should exist");
        set_state.phase = crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes;
        set_state.plan = Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
            phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
            is_solid: false,
            ready_members: Vec::new(),
            member_names: Vec::new(),
            member_dependencies: HashMap::new(),
            waiting_on_volumes: HashSet::new(),
            deletion_eligible: HashSet::new(),
            delete_decisions: BTreeMap::new(),
            topology,
            fallback_reason: None,
        });
    }

    pipeline.rar_refresh_state.insert(
        (job_id, "show".to_string()),
        RarRefreshState {
            in_flight: Some(RarRefreshRequest {
                target_completed_volume: 3,
                reason: RefreshReason::CoverageExpansion,
            }),
            queued: None,
            latest_completed_volume: 3,
            refreshed_volumes: BTreeSet::from([0, 1, 2]),
            structure_dirty: false,
            last_error: None,
        },
    );
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    resume_job_downloading_for_test(&mut pipeline, job_id);

    pipeline.check_job_completion(job_id).await;

    assert!(pipeline.job_has_pending_rar_refresh_for_current_sets(job_id));
    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert!(
        !matches!(
            pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
            Some(JobStatus::Failed { .. })
        ),
        "completion should not fail or repair-evaluate while a current RAR refresh is pending"
    );
    assert!(
        pipeline.job_has_incoherent_rar_waiting_state(job_id),
        "pending-refresh barrier should not force an eager topology rebuild"
    );
}

#[tokio::test]
async fn incoherent_rar_waiting_state_heals_before_reverification() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30080);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Incoherent Waiting Heals Before Verify", &files);

    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);
    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let mut good_archive =
        weaver_unrar::RarArchive::open(std::io::Cursor::new(files[0].1.clone())).unwrap();
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
        &serde_json::from_value::<weaver_unrar::CachedArchiveHeaders>(cached).unwrap(),
    )
    .unwrap();
    let topology = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for("show"))
        .cloned()
        .expect("RAR topology should exist after all volumes complete");

    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&(job_id, "show".to_string()))
            .expect("RAR set state should exist");
        set_state.cached_headers = Some(stale_headers.clone());
        set_state.phase = crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes;
        set_state.plan = Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
            phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
            is_solid: false,
            ready_members: Vec::new(),
            member_names: Vec::new(),
            member_dependencies: HashMap::new(),
            waiting_on_volumes: HashSet::new(),
            deletion_eligible: HashSet::new(),
            delete_decisions: BTreeMap::new(),
            topology,
            fallback_reason: None,
        });
    }
    pipeline
        .db
        .save_archive_headers(job_id, "show", &stale_headers)
        .unwrap();

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    resume_job_downloading_for_test(&mut pipeline, job_id);

    pipeline.check_job_completion(job_id).await;

    assert_eq!(drain_job_verification_started(&mut events, job_id), 0);
    assert!(!pipeline.job_has_incoherent_rar_waiting_state(job_id));

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("RAR plan should exist after healing");
    assert!(
        !matches!(
            plan.phase,
            crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes
        ),
        "completion healing should not leave the set in an impossible waiting state",
    );
    assert!(
        plan.ready_members
            .iter()
            .any(|member| member.name == "E01.mkv"),
        "healed plan should restore ready members without another PAR2 verification",
    );
}

#[tokio::test]
async fn retry_archive_extraction_after_verify_or_repair_heals_incoherent_rar_state_for_mixed_archive_jobs()
 {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30081);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Mixed Archive Heal", &files);

    insert_active_job(&mut pipeline, job_id, spec).await;
    install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .set_archive_topology(
            "bonus".to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Split,
                volume_map: std::collections::HashMap::from([("bonus.001".to_string(), 0u32)]),
                complete_volumes: std::collections::HashSet::from([0u32]),
                expected_volume_count: Some(1),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "bonus.bin".to_string(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: 10,
                }],
                unresolved_spans: Vec::new(),
            },
        );

    let mut good_archive =
        weaver_unrar::RarArchive::open(std::io::Cursor::new(files[0].1.clone())).unwrap();
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
        &serde_json::from_value::<weaver_unrar::CachedArchiveHeaders>(cached).unwrap(),
    )
    .unwrap();
    let topology = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.archive_topology_for("show"))
        .cloned()
        .expect("RAR topology should exist after all volumes complete");

    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&(job_id, "show".to_string()))
            .expect("RAR set state should exist");
        set_state.cached_headers = Some(stale_headers.clone());
        set_state.phase = crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes;
        set_state.plan = Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
            phase: crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes,
            is_solid: false,
            ready_members: Vec::new(),
            member_names: Vec::new(),
            member_dependencies: HashMap::new(),
            waiting_on_volumes: HashSet::new(),
            deletion_eligible: HashSet::new(),
            delete_decisions: BTreeMap::new(),
            topology,
            fallback_reason: None,
        });
    }
    pipeline
        .db
        .save_archive_headers(job_id, "show", &stale_headers)
        .unwrap();
    pipeline.inflight_extractions.insert(
        job_id,
        ["show".to_string(), "bonus".to_string()]
            .into_iter()
            .collect(),
    );

    pipeline
        .retry_archive_extraction_after_verify_or_repair(job_id)
        .await;

    assert!(!pipeline.job_has_incoherent_rar_waiting_state(job_id));
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Downloading)
    );

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("RAR plan should exist after mixed-archive healing");
    assert!(
        !matches!(
            plan.phase,
            crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes
        ),
        "mixed-archive retry should heal the impossible waiting state",
    );
    assert!(
        plan.ready_members
            .iter()
            .any(|member| member.name == "E01.mkv"),
        "mixed-archive retry should restore live member readiness",
    );
}

#[tokio::test]
async fn retry_archive_extraction_after_verify_or_repair_preserves_recoverable_rar_fallback_plan() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30082);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Recoverable Fallback", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    tokio::fs::remove_file(working_dir.join("show.part01.rar"))
        .await
        .unwrap();

    let corrupt_headers = vec![0xFF, 0x00, 0x13];
    pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .expect("RAR set state should exist after all volumes complete")
        .cached_headers = Some(corrupt_headers.clone());
    pipeline
        .db
        .save_archive_headers(job_id, "show", &corrupt_headers)
        .unwrap();

    pipeline
        .retry_archive_extraction_after_verify_or_repair(job_id)
        .await;

    assert!(!matches!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Failed { .. })
    ));

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("fallback plan should be retained after recompute failure");
    assert!(matches!(
        plan.phase,
        crate::pipeline::archive::rar_state::RarSetPhase::FallbackFullSet
    ));
    assert!(plan.fallback_reason.as_deref().is_some_and(|reason| {
        reason.contains("cannot be rebuilt without cached headers or volume 0")
    }));
}

#[tokio::test]
async fn rar_retry_frontier_rejects_waiting_on_deleted_volume() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30018);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("RAR Retry Frontier Rejects Deleted Waiting Volume", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    tokio::fs::remove_file(working_dir.join("show.part04.rar"))
        .await
        .unwrap();
    pipeline.eagerly_deleted.insert(
        job_id,
        ["show.part04.rar".to_string()].into_iter().collect(),
    );
    pipeline
        .extracted_members
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline
        .recompute_rar_set_state(job_id, "show")
        .await
        .unwrap();

    let plan = pipeline
        .rar_sets
        .get(&(job_id, "show".to_string()))
        .and_then(|state| state.plan.as_ref())
        .cloned()
        .expect("RAR plan should exist");
    assert!(plan.waiting_on_volumes.contains(&3));
    assert!(!plan.deletion_eligible.contains(&3));
    assert_eq!(
        pipeline.invalid_rar_retry_frontier_reason(job_id),
        Some("set 'show' waiting volumes already deleted: [3]".to_string())
    );
    assert!(pipeline.invalid_rar_retry_frontier_reason(job_id).is_some());
}

#[tokio::test]
async fn eager_delete_retains_volume_with_failed_member_claim() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30019);
    let files = vec![
        ("show.part01.rar".to_string(), vec![1u8]),
        ("show.part02.rar".to_string(), vec![2u8]),
        ("show.part03.rar".to_string(), vec![3u8]),
    ];
    let spec = rar_job_spec("RAR Failed Claim Delete Guard", &files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (filename, bytes) in &files {
        tokio::fs::write(working_dir.join(filename), bytes)
            .await
            .unwrap();
    }

    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: std::collections::HashMap::from([
            ("show.part01.rar".to_string(), 0),
            ("show.part02.rar".to_string(), 1),
            ("show.part03.rar".to_string(), 2),
        ]),
        complete_volumes: [0u32, 1u32, 2u32].into_iter().collect(),
        expected_volume_count: Some(3),
        members: vec![
            crate::jobs::assembly::ArchiveMember {
                name: "E10.mkv".to_string(),
                first_volume: 0,
                last_volume: 1,
                unpacked_size: 0,
            },
            crate::jobs::assembly::ArchiveMember {
                name: "E11.mkv".to_string(),
                first_volume: 1,
                last_volume: 2,
                unpacked_size: 0,
            },
        ],
        unresolved_spans: Vec::new(),
    };
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());
    pipeline
        .failed_extractions
        .insert(job_id, ["E10.mkv".to_string()].into_iter().collect());
    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([(1u32, dummy_rar_volume_facts(1))]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            shared_kdf_cache: std::sync::Arc::new(weaver_unrar::crypto::KdfCache::new()),
            verified_suspect_volumes: std::collections::HashSet::new(),
            active_workers: 0,
            in_flight_members: std::collections::HashSet::new(),
            extraction_generation: 0,
            phase: rar_state::RarSetPhase::Ready,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::Ready,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["E10.mkv".to_string(), "E11.mkv".to_string()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: std::collections::HashSet::new(),
                deletion_eligible: [1u32].into_iter().collect(),
                delete_decisions: std::collections::BTreeMap::from([(
                    1u32,
                    rar_state::RarVolumeDeleteDecision {
                        owners: vec!["E10.mkv".to_string(), "E11.mkv".to_string()],
                        clean_owners: vec!["E11.mkv".to_string()],
                        failed_owners: vec!["E10.mkv".to_string()],
                        pending_owners: Vec::new(),
                        unresolved_boundary: false,
                        ownership_eligible: false,
                    },
                )]),
                topology,
                fallback_reason: None,
            }),
        },
    );

    pipeline.try_delete_volumes(job_id, "show");

    assert!(working_dir.join("show.part02.rar").exists());
    assert!(
        !pipeline
            .eagerly_deleted
            .get(&job_id)
            .is_some_and(|deleted| deleted.contains("show.part02.rar"))
    );
}

#[tokio::test]
async fn paused_queued_extraction_is_not_promoted_when_capacity_frees() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30089);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Paused Queued Extract", &[("archive.7z".to_string(), 100)]),
    )
    .await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Paused;
        state.paused_resume_status = Some(JobStatus::Downloading);
        state.queued_extract_at_epoch_ms = Some(crate::jobs::model::epoch_ms_now());
        state.refresh_runtime_lanes_from_status();
    }

    pipeline.promote_queued_extractions();

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(state.status, JobStatus::Paused));
    assert!(matches!(
        state.paused_resume_status,
        Some(JobStatus::Downloading)
    ));
}

#[tokio::test]
async fn queued_repair_blocks_extraction_promotion() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30090);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Awaiting Repair Extract Guard",
            &[("archive.rar".to_string(), 100)],
        ),
    )
    .await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::QueuedRepair;
        state.refresh_runtime_lanes_from_status();
    }

    assert!(!pipeline.maybe_start_extraction(job_id).await);

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(matches!(state.status, JobStatus::QueuedRepair));
}

#[tokio::test]
async fn exhausted_incomplete_rar_member_is_not_scheduled_for_extraction() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30092);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Incomplete RAR Extract Guard",
            &[("archive.rar".to_string(), 100)],
        ),
    )
    .await;

    let set_name = "archive".to_string();
    let member_name = "work/sample.mkv".to_string();
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::from([("archive.rar".to_string(), 0)]),
        complete_volumes: HashSet::new(),
        expected_volume_count: Some(1),
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: member_name.clone(),
            first_volume: 0,
            last_volume: 0,
            unpacked_size: 100,
        }],
        unresolved_spans: Vec::new(),
    };

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state.status = JobStatus::Downloading;
        state
            .assembly
            .set_archive_topology(set_name.clone(), topology.clone());
        state.refresh_runtime_lanes_from_status();
    }
    pipeline.rar_sets.insert(
        (job_id, set_name.clone()),
        crate::pipeline::archive::rar_state::RarSetState {
            phase: crate::pipeline::archive::rar_state::RarSetPhase::Ready,
            plan: Some(crate::pipeline::archive::rar_state::RarDerivedPlan {
                phase: crate::pipeline::archive::rar_state::RarSetPhase::Ready,
                is_solid: false,
                ready_members: vec![crate::pipeline::archive::rar_state::RarReadyMember {
                    name: member_name,
                }],
                member_names: vec!["work/sample.mkv".to_string()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::new(),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology,
                fallback_reason: None,
            }),
            ..Default::default()
        },
    );

    pipeline.try_rar_extraction(job_id).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(!matches!(state.status, JobStatus::Extracting));
    assert_eq!(
        pipeline
            .rar_sets
            .get(&(job_id, set_name))
            .map(|set| set.active_workers),
        Some(0)
    );
}

#[tokio::test]
async fn incomplete_download_with_active_extraction_defers_instead_of_failing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30091);
    let payload_filename = "payload.mkv";
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let spec = JobSpec {
        name: "Incomplete Download Active Extraction".to_string(),
        password: None,
        total_bytes: 320,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 128,
                        message_id: "active-extract-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 128,
                        message_id: "active-extract-payload-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 32,
                    message_id: "active-extract-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 32,
                    message_id: "active-extract-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.download_queue.push(DownloadWork {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: 1,
            },
            message_id: MessageId::new("active-extract-payload-1@example.com"),
            groups: vec!["alt.binaries.test".to_string()],
            priority: 0,
            byte_estimate: 128,
            retry_count: 0,
            is_recovery: false,
            exclude_servers: Vec::new(),
        });
        state.recovery_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 128)
            .unwrap();
        state.status = JobStatus::Extracting;
        state.refresh_runtime_lanes_from_status();
    }
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        minimal_par2_file_set(),
        &[(1, index_filename, 0, false)],
    );
    pipeline
        .inflight_extractions
        .entry(job_id)
        .or_default()
        .insert("payload".to_string());

    pipeline.check_job_completion(job_id).await;

    let status = job_status_for_assert(&pipeline, job_id);
    assert!(!matches!(status, Some(JobStatus::Failed { .. })));
    assert!(pipeline.jobs.contains_key(&job_id));
}

#[tokio::test]
async fn rar_unlock_non_solid_boosts_next_member_volume_then_recomputes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40100);
    let files = (0..5)
        .map(|volume| {
            (
                format!("show.part{:02}.rar", volume + 1),
                vec![volume as u8; 8],
            )
        })
        .collect::<Vec<_>>();
    let spec = rar_job_spec("RAR Unlock Non Solid", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    reset_rar_unlock_queue(&mut pipeline, job_id, &[2, 3, 4]);
    install_rar_unlock_plan(
        &mut pipeline,
        job_id,
        "show",
        false,
        5,
        HashSet::from([0, 1]),
        vec![
            crate::jobs::assembly::ArchiveMember {
                name: "done.mkv".to_string(),
                first_volume: 0,
                last_volume: 1,
                unpacked_size: 10,
            },
            crate::jobs::assembly::ArchiveMember {
                name: "next-small.mkv".to_string(),
                first_volume: 1,
                last_volume: 2,
                unpacked_size: 20,
            },
            crate::jobs::assembly::ArchiveMember {
                name: "later-wide.mkv".to_string(),
                first_volume: 3,
                last_volume: 4,
                unpacked_size: 30,
            },
        ],
    );
    pipeline
        .extracted_members
        .insert(job_id, HashSet::from(["done.mkv".to_string()]));

    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);
    let first = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    assert_eq!(first.segment_id.file_id.file_index, 2);
    assert_eq!(first.priority, 3);

    pipeline
        .extracted_members
        .entry(job_id)
        .or_default()
        .insert("next-small.mkv".to_string());
    reset_rar_unlock_queue(&mut pipeline, job_id, &[3, 4]);
    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let next = state.download_queue.pop().unwrap();
    let after_next = state.download_queue.pop().unwrap();
    assert_eq!(next.segment_id.file_id.file_index, 3);
    assert_eq!(next.priority, 3);
    assert_eq!(after_next.segment_id.file_id.file_index, 4);
    assert_eq!(after_next.priority, 3);
}

#[tokio::test]
async fn rar_unlock_solid_boosts_only_earliest_sequential_missing_volumes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40101);
    let files = (0..7)
        .map(|volume| {
            (
                format!("show.part{:02}.rar", volume + 1),
                vec![volume as u8; 8],
            )
        })
        .collect::<Vec<_>>();
    let spec = rar_job_spec("RAR Unlock Solid", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    reset_rar_unlock_queue(&mut pipeline, job_id, &[1, 2, 3, 4, 5, 6]);
    install_rar_unlock_plan(
        &mut pipeline,
        job_id,
        "show",
        true,
        7,
        HashSet::from([0]),
        vec![
            crate::jobs::assembly::ArchiveMember {
                name: "solid-first.mkv".to_string(),
                first_volume: 0,
                last_volume: 5,
                unpacked_size: 100,
            },
            crate::jobs::assembly::ArchiveMember {
                name: "solid-later.mkv".to_string(),
                first_volume: 6,
                last_volume: 6,
                unpacked_size: 1,
            },
        ],
    );

    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let boosted = (0..4)
        .map(|_| state.download_queue.pop().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        boosted
            .iter()
            .map(|work| work.segment_id.file_id.file_index)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert!(boosted.iter().all(|work| work.priority == 3));
    let unboosted = state.download_queue.pop().unwrap();
    assert_eq!(unboosted.segment_id.file_id.file_index, 5);
    assert_eq!(unboosted.priority, 15);
}

#[tokio::test]
async fn rar_unlock_uses_par2_authoritative_identity_for_obfuscated_volume() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40102);
    let spec = JobSpec {
        name: "RAR Unlock PAR2 Identity".to_string(),
        password: None,
        total_bytes: 8,
        category: None,
        metadata: Vec::new(),
        files: vec![FileSpec {
            filename: "obfuscated.bin".to_string(),
            role: FileRole::Unknown,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: 8,
                message_id: "rar-unlock-obfuscated@example.com".to_string(),
            }],
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.download_queue.push(rar_unlock_work(
            job_id,
            0,
            FileRole::Unknown.download_priority(),
        ));
    }
    install_rar_unlock_plan(
        &mut pipeline,
        job_id,
        "show",
        false,
        3,
        HashSet::from([0, 1]),
        vec![crate::jobs::assembly::ArchiveMember {
            name: "next.mkv".to_string(),
            first_volume: 0,
            last_volume: 2,
            unpacked_size: 100,
        }],
    );
    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: "obfuscated.bin".to_string(),
                current_filename: "show.part03.rar".to_string(),
                canonical_filename: Some("show.part03.rar".to_string()),
                classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name: "show".to_string(),
                    volume_index: Some(2),
                }),
                classification_source: FileIdentitySource::Par2,
            },
        )
        .unwrap();

    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);
    let work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    assert_eq!(work.segment_id.file_id.file_index, 0);
    assert_eq!(work.priority, 3);
}

#[tokio::test]
async fn rar_unlock_bootstraps_volume_prefix_before_plan_exists() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40140);
    let spec = JobSpec {
        name: "RAR Unlock Bootstrap".to_string(),
        password: None,
        total_bytes: 32,
        category: None,
        metadata: Vec::new(),
        files: (0..4)
            .map(|index| FileSpec {
                filename: format!("obfuscated-{index}.bin"),
                role: FileRole::Unknown,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 8,
                    message_id: format!("rar-unlock-bootstrap-{index}@example.com"),
                }],
            })
            .collect(),
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        for index in 0..4 {
            state.download_queue.push(rar_unlock_work(
                job_id,
                index,
                FileRole::Unknown.download_priority(),
            ));
        }
    }
    // PAR2 metadata classified every file, but no volume has completed yet:
    // no rar_sets plan exists, so only the bootstrap prefix selection can
    // pull volume 0 forward.
    for index in 0..4u32 {
        pipeline
            .set_file_identity(
                job_id,
                crate::jobs::record::ActiveFileIdentity {
                    file_index: index,
                    source_filename: format!("obfuscated-{index}.bin"),
                    current_filename: format!("vol.part{:02}.rar", index + 1),
                    canonical_filename: Some(format!("vol.part{:02}.rar", index + 1)),
                    classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                        kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                        set_name: "vol".to_string(),
                        volume_index: Some(index),
                    }),
                    classification_source: FileIdentitySource::Par2,
                },
            )
            .unwrap();
    }

    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);

    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let order = (0..4)
        .map(|_| state.download_queue.pop().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        order
            .iter()
            .map(|work| work.segment_id.file_id.file_index)
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3],
        "volume 0 must lead the bootstrap prefix"
    );
    assert_eq!(
        order[0].priority, 1,
        "volume 0 queue entries are corrected to the protected base priority"
    );
    assert!(
        order[1..].iter().all(|work| work.priority == 3),
        "later prefix volumes ride the unlock tier"
    );
}

#[tokio::test]
async fn rar_unlock_does_not_boost_untrusted_topology_disagreement() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40103);
    let spec = JobSpec {
        name: "RAR Unlock Untrusted Identity".to_string(),
        password: None,
        total_bytes: 8,
        category: None,
        metadata: Vec::new(),
        files: vec![FileSpec {
            filename: "not-the-topology-name.rar".to_string(),
            role: FileRole::Unknown,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: 8,
                message_id: "rar-unlock-untrusted@example.com".to_string(),
            }],
        }],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.download_queue.push(rar_unlock_work(
            job_id,
            0,
            FileRole::Unknown.download_priority(),
        ));
    }
    install_rar_unlock_plan(
        &mut pipeline,
        job_id,
        "show",
        false,
        3,
        HashSet::from([0, 1]),
        vec![crate::jobs::assembly::ArchiveMember {
            name: "next.mkv".to_string(),
            first_volume: 0,
            last_volume: 2,
            unpacked_size: 100,
        }],
    );
    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: "not-the-topology-name.rar".to_string(),
                current_filename: "not-the-topology-name.rar".to_string(),
                canonical_filename: None,
                classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                    kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                    set_name: "show".to_string(),
                    volume_index: Some(2),
                }),
                classification_source: FileIdentitySource::Probe,
            },
        )
        .unwrap();

    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);
    let work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    assert_eq!(work.segment_id.file_id.file_index, 0);
    assert_eq!(work.priority, FileRole::Unknown.download_priority());
}

#[tokio::test]
async fn rar_unlock_no_plan_or_topology_leaves_queue_unchanged() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40104);
    let files = (0..3)
        .map(|volume| {
            (
                format!("show.part{:02}.rar", volume + 1),
                vec![volume as u8; 8],
            )
        })
        .collect::<Vec<_>>();
    let spec = rar_job_spec("RAR Unlock No Topology", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    reset_rar_unlock_queue(&mut pipeline, job_id, &[2]);

    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);

    let work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    assert_eq!(work.segment_id.file_id.file_index, 2);
    assert_eq!(work.priority, 12);
}

#[tokio::test]
async fn rar_unlock_missing_required_volume_without_queued_work_does_not_boost() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40105);
    let files = (0..4)
        .map(|volume| {
            (
                format!("show.part{:02}.rar", volume + 1),
                vec![volume as u8; 8],
            )
        })
        .collect::<Vec<_>>();
    let spec = rar_job_spec("RAR Unlock Missing Queued Volume", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    reset_rar_unlock_queue(&mut pipeline, job_id, &[3]);
    install_rar_unlock_plan(
        &mut pipeline,
        job_id,
        "show",
        false,
        4,
        HashSet::from([0, 1]),
        vec![crate::jobs::assembly::ArchiveMember {
            name: "needs-missing-volume-two.mkv".to_string(),
            first_volume: 1,
            last_volume: 2,
            unpacked_size: 100,
        }],
    );

    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);

    let work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    assert_eq!(work.segment_id.file_id.file_index, 3);
    assert_eq!(work.priority, 13);
}

#[tokio::test]
async fn rar_unlock_counts_active_volume_as_near_unlock_prerequisite() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40106);
    let files = (0..5)
        .map(|volume| {
            (
                format!("show.part{:02}.rar", volume + 1),
                vec![volume as u8; 8],
            )
        })
        .collect::<Vec<_>>();
    let spec = rar_job_spec("RAR Unlock Active Prerequisite", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    reset_rar_unlock_queue(&mut pipeline, job_id, &[3]);
    pipeline.active_downloads_by_file.insert(
        NzbFileId {
            job_id,
            file_index: 2,
        },
        1,
    );
    install_rar_unlock_plan(
        &mut pipeline,
        job_id,
        "show",
        false,
        5,
        HashSet::from([0, 1]),
        vec![crate::jobs::assembly::ArchiveMember {
            name: "needs-active-and-queued.mkv".to_string(),
            first_volume: 1,
            last_volume: 3,
            unpacked_size: 100,
        }],
    );

    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);

    let work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    assert_eq!(work.segment_id.file_id.file_index, 3);
    assert_eq!(work.priority, 3);
}

#[tokio::test]
async fn rar_unlock_ranked_window_uses_logical_volume_order() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40107);
    let files = (0..5)
        .map(|volume| {
            (
                format!("show.part{:02}.rar", volume + 1),
                vec![volume as u8; 8],
            )
        })
        .collect::<Vec<_>>();
    let spec = rar_job_spec("RAR Unlock Ranked Window", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    reset_rar_unlock_queue(&mut pipeline, job_id, &[4, 2, 3]);
    install_rar_unlock_plan(
        &mut pipeline,
        job_id,
        "show",
        false,
        5,
        HashSet::from([0, 1]),
        vec![crate::jobs::assembly::ArchiveMember {
            name: "wide-next.mkv".to_string(),
            first_volume: 1,
            last_volume: 4,
            unpacked_size: 100,
        }],
    );

    pipeline.mark_rar_unlock_priorities_dirty(job_id);
    pipeline.apply_rar_unlock_priorities_if_dirty(job_id);

    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let popped = [
        state.download_queue.pop().unwrap(),
        state.download_queue.pop().unwrap(),
        state.download_queue.pop().unwrap(),
    ];
    assert_eq!(
        popped
            .iter()
            .map(|work| work.segment_id.file_id.file_index)
            .collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
    assert!(popped.iter().all(|work| work.priority == 3));
}

#[tokio::test]
async fn rar_unlock_dirty_priorities_apply_before_lane_refill() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(40108);
    let files = (0..5)
        .map(|volume| {
            (
                format!("show.part{:02}.rar", volume + 1),
                vec![volume as u8; 8],
            )
        })
        .collect::<Vec<_>>();
    let spec = rar_job_spec("RAR Unlock Refill Dirty", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    reset_rar_unlock_queue(&mut pipeline, job_id, &[4, 2, 3]);
    install_rar_unlock_plan(
        &mut pipeline,
        job_id,
        "show",
        false,
        5,
        HashSet::from([0, 1]),
        vec![crate::jobs::assembly::ArchiveMember {
            name: "wide-next.mkv".to_string(),
            first_volume: 1,
            last_volume: 4,
            unpacked_size: 100,
        }],
    );
    pipeline.mark_rar_unlock_priorities_dirty(job_id);

    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
        job_id,
        server_idx: 0,
        remote_ip: "127.0.0.1".parse().unwrap(),
        supports_pipelining: false,
        current_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        compatibility: DownloadBatchCompatibility {
            priority: 3,
            is_recovery: false,
            groups: vec!["alt.binaries.test".to_string()],
            exclude_servers: Vec::new(),
        },
        response_tx,
    });

    let response = response_rx.await.unwrap();
    let lease = response
        .lease
        .expect("priority-3 RAR unlock lane should refill after reprioritization");
    assert_eq!(lease.job_id, job_id);
    assert_eq!(lease.works[0].segment_id.file_id.file_index, 2);
    assert_eq!(lease.works[0].priority, 3);
    assert!(!pipeline.rar_unlock_priority_dirty_jobs.contains(&job_id));
}

#[tokio::test]
async fn rar_unlock_retry_requeue_marks_rar_volume_dirty_only() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let rar_job_id = JobId(40109);
    let rar_files = (0..3)
        .map(|volume| {
            (
                format!("show.part{:02}.rar", volume + 1),
                vec![volume as u8; 8],
            )
        })
        .collect::<Vec<_>>();
    let rar_spec = rar_job_spec("RAR Unlock Retry Dirty", &rar_files);
    insert_active_job(&mut pipeline, rar_job_id, rar_spec).await;
    pipeline.rar_unlock_priority_dirty_jobs.clear();

    pipeline.requeue_retry_work(rar_unlock_work(rar_job_id, 2, 12));
    assert!(
        pipeline
            .rar_unlock_priority_dirty_jobs
            .contains(&rar_job_id)
    );

    let standalone_job_id = JobId(40110);
    let standalone_spec = standalone_job_spec(
        "RAR Unlock Retry Non RAR",
        &[("standalone.bin".to_string(), 8)],
    );
    insert_active_job(&mut pipeline, standalone_job_id, standalone_spec).await;
    pipeline.rar_unlock_priority_dirty_jobs.clear();

    pipeline.requeue_retry_work(DownloadWork {
        segment_id: SegmentId {
            file_id: NzbFileId {
                job_id: standalone_job_id,
                file_index: 0,
            },
            segment_number: 0,
        },
        message_id: MessageId::new("rar-unlock-non-rar-retry@example.com"),
        groups: vec!["alt.binaries.test".to_string()],
        priority: FileRole::Standalone.download_priority(),
        byte_estimate: 1024,
        retry_count: 1,
        is_recovery: false,
        exclude_servers: Vec::new(),
    });
    assert!(
        !pipeline
            .rar_unlock_priority_dirty_jobs
            .contains(&standalone_job_id)
    );
}

#[tokio::test]
async fn impossible_rar_state_fails_loudly_after_forced_recompute() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30076);
    let working_dir = intermediate_dir.join("impossible-rar-state");
    let mut state = minimal_job_state(job_id, "Impossible RAR State", working_dir);
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::new(),
        complete_volumes: HashSet::new(),
        expected_volume_count: None,
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: "E10.mkv".to_string(),
            first_volume: 0,
            last_volume: 1,
            unpacked_size: 0,
        }],
        unresolved_spans: Vec::new(),
    };
    state
        .assembly
        .set_archive_topology("show".to_string(), topology.clone());
    pipeline.jobs.insert(job_id, state);
    pipeline.job_order.push(job_id);
    pipeline.rar_sets.insert(
        (job_id, "show".to_string()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([(0u32, dummy_rar_volume_facts(0))]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            shared_kdf_cache: std::sync::Arc::new(weaver_unrar::crypto::KdfCache::new()),
            verified_suspect_volumes: HashSet::new(),
            active_workers: 0,
            in_flight_members: HashSet::new(),
            extraction_generation: 0,
            phase: rar_state::RarSetPhase::Ready,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::Ready,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["E10.mkv".to_string()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: HashSet::new(),
                deletion_eligible: HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology,
                fallback_reason: None,
            }),
        },
    );

    pipeline.check_job_completion(job_id).await;

    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!("job should have failed");
    };
    assert!(error.contains("invalid RAR state after recompute"));
}

#[tokio::test]
async fn extraction_queue_limits_to_tuner_capacity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let extraction_limit = pipeline.tuner.max_concurrent_extractions();
    let jobs: Vec<JobId> = (0..=extraction_limit)
        .map(|idx| JobId(31101 + idx as u64))
        .collect();
    let queued_job = jobs[extraction_limit];

    for (idx, job_id) in jobs.iter().enumerate() {
        pipeline.jobs.insert(
            *job_id,
            minimal_job_state(
                *job_id,
                &format!("extract-{idx}"),
                temp_dir.path().join(format!("extract-{idx}")),
            ),
        );
    }

    for job_id in jobs.iter().take(extraction_limit) {
        assert!(pipeline.maybe_start_extraction(*job_id).await);
        assert_eq!(
            pipeline.jobs.get(job_id).map(|state| state.status.clone()),
            Some(JobStatus::Extracting)
        );
    }
    assert_eq!(
        pipeline.metrics.extract_active.load(Ordering::Relaxed),
        extraction_limit
    );

    assert!(!pipeline.maybe_start_extraction(queued_job).await);
    assert_eq!(
        pipeline
            .jobs
            .get(&queued_job)
            .map(|state| state.status.clone()),
        Some(JobStatus::QueuedExtract)
    );

    pipeline.transition_postprocessing_status(jobs[0], JobStatus::Downloading, Some("downloading"));

    assert_eq!(
        pipeline.metrics.extract_active.load(Ordering::Relaxed),
        extraction_limit
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&queued_job)
            .map(|state| state.status.clone()),
        Some(JobStatus::Extracting)
    );
    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![queued_job]
    );
}

#[tokio::test]
async fn queued_rar_extract_restart_relaunches_idle_ready_work() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31202);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("Restore queued RAR extract", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let key = (job_id, "show".to_string());
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&key)
            .expect("RAR set state should exist after all volumes complete");
        assert!(
            set_state
                .plan
                .as_ref()
                .is_some_and(|plan| !plan.ready_members.is_empty())
        );
        set_state.active_workers = 0;
        set_state.in_flight_members.clear();
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::QueuedExtract;
        state.refresh_runtime_lanes_from_status();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline.pending_completion_checks.clear();
    pipeline.check_job_completion(job_id).await;

    let active_workers = pipeline
        .rar_sets
        .get(&key)
        .map(|state| state.active_workers)
        .unwrap_or_default();
    assert!(
        active_workers > 0,
        "idle restored QueuedExtract job should relaunch ready RAR work"
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Extracting)
    );

    for _ in 0..active_workers {
        let done = next_extraction_done(&mut pipeline).await;
        pipeline.handle_extraction_done(done).await;
    }
}

#[tokio::test]
async fn extracting_rar_restart_with_failed_member_enters_repair_or_relaunches() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31203);
    let files = build_multifile_multivolume_rar_set();
    let index_filename = "repair.par2";
    let recovery_filename = "repair.vol00+01.par2";
    let par2_bytes = build_test_par2_index(&files[0].0, &files[0].1, files[0].1.len() as u64);
    let recovery_bytes = vec![0xAA; files[0].1.len()];
    let mut all_files = files.clone();
    all_files.push((index_filename.to_string(), par2_bytes.clone()));
    all_files.push((recovery_filename.to_string(), recovery_bytes.clone()));
    let spec = rar_job_spec("Restore failed RAR repair", &all_files);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }
    write_and_complete_file(
        &mut pipeline,
        job_id,
        files.len() as u32,
        index_filename,
        &par2_bytes,
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        files.len() as u32 + 1,
        recovery_filename,
        &recovery_bytes,
    )
    .await;

    let mut damaged_first_volume = files[0].1.clone();
    damaged_first_volume[0] ^= 0xFF;
    tokio::fs::write(working_dir.join(&files[0].0), &damaged_first_volume)
        .await
        .unwrap();
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(&files[0].0, &files[0].1, files[0].1.len() as u64, 1),
        &[
            (files.len() as u32, index_filename, 0, false),
            (files.len() as u32 + 1, recovery_filename, 1, true),
        ],
    );

    pipeline
        .failed_extractions
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    let key = (job_id, "show".to_string());
    {
        let set_state = pipeline
            .rar_sets
            .get_mut(&key)
            .expect("RAR set state should exist after all volumes complete");
        set_state.active_workers = 0;
        set_state.in_flight_members.clear();
        set_state.phase = crate::pipeline::rar_state::RarSetPhase::AwaitingRepair;
        if let Some(plan) = set_state.plan.as_mut() {
            plan.phase = crate::pipeline::rar_state::RarSetPhase::AwaitingRepair;
        }
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Extracting;
        state.refresh_runtime_lanes_from_status();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline.pending_completion_checks.clear();
    pipeline.check_job_completion(job_id).await;

    let active_workers = pipeline
        .rar_sets
        .get(&key)
        .map(|state| state.active_workers)
        .unwrap_or_default();
    let status = pipeline.jobs.get(&job_id).map(|state| state.status.clone());
    let completion_check_pending = pipeline.pending_completion_checks.contains(&job_id);

    assert!(
        !matches!(status, Some(JobStatus::Failed { .. })),
        "restart repair liveness must not terminally fail; status={status:?}"
    );

    assert!(
        active_workers > 0
            || matches!(
                status,
                Some(JobStatus::Repairing | JobStatus::QueuedRepair | JobStatus::Complete)
            )
            || completion_check_pending,
        "stale idle failed RAR extraction must enter repair or relaunch extraction; status={status:?}, active_workers={active_workers}"
    );

    for _ in 0..active_workers {
        let done = next_extraction_done(&mut pipeline).await;
        pipeline.handle_extraction_done(done).await;
    }
}

#[tokio::test]
async fn direct_full_set_rar_extraction_registers_and_blocks_incremental_batches() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31204);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("Direct full-set RAR ownership", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let key = (job_id, "show".to_string());
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Extracting;
        state.refresh_runtime_lanes_from_status();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline
        .extract_rar_set(job_id, "show")
        .await
        .expect("direct full-set extraction should launch");
    assert!(
        pipeline
            .inflight_extractions
            .get(&job_id)
            .is_some_and(|sets| sets.contains("show")),
        "direct full-set extraction must register set ownership"
    );

    pipeline.try_rar_extraction(job_id).await;

    let set_state = pipeline
        .rar_sets
        .get(&key)
        .expect("RAR set should remain while full-set extraction is active");
    assert_eq!(set_state.active_workers, 1);
    assert!(
        set_state.in_flight_members.is_empty(),
        "incremental batches must not start for a set owned by direct full-set extraction"
    );

    let done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(done).await;
}

#[tokio::test]
async fn active_full_set_rar_extraction_blocks_restart_repair_and_relaunch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31208);
    let files = build_multifile_multivolume_rar_set();
    let spec = rar_job_spec("Active full-set RAR restart guard", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, file_index as u32, filename, bytes)
            .await;
    }

    let key = (job_id, "show".to_string());
    pipeline
        .failed_extractions
        .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
    pipeline
        .extract_rar_set(job_id, "show")
        .await
        .expect("direct full-set extraction should launch");
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.status = JobStatus::Extracting;
        state.refresh_runtime_lanes_from_status();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }

    pipeline.pending_completion_checks.clear();
    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
        Some(JobStatus::Extracting)
    );
    assert_eq!(pipeline.metrics.repair_active.load(Ordering::Relaxed), 0);
    assert!(pipeline.pending_completion_checks.is_empty());
    assert!(
        pipeline
            .failed_extractions
            .get(&job_id)
            .is_some_and(|members| members.contains("E01.mkv"))
    );
    let set_state = pipeline
        .rar_sets
        .get(&key)
        .expect("RAR set should remain while full-set extraction is active");
    assert_eq!(set_state.active_workers, 1);
    assert!(set_state.in_flight_members.is_empty());
    assert!(
        pipeline
            .inflight_extractions
            .get(&job_id)
            .is_some_and(|sets| sets.contains("show"))
    );

    let done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(done).await;
}

#[tokio::test]
async fn reconcile_job_progress_marks_waiting_for_rar_volumes_without_clobbering_download_lane() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31233);
    let set_name = "show".to_string();

    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, "waiting-rar", temp_dir.path().join("waiting-rar")),
    );
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .set_archive_topology(
            set_name.clone(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Rar,
                volume_map: std::collections::HashMap::from([
                    ("show.part01.rar".to_string(), 0u32),
                    ("show.part02.rar".to_string(), 1u32),
                    ("show.part03.rar".to_string(), 2u32),
                ]),
                complete_volumes: std::collections::HashSet::from([0u32, 1u32]),
                expected_volume_count: Some(3),
                members: vec![crate::jobs::assembly::ArchiveMember {
                    name: "E01.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 2,
                    unpacked_size: 100,
                }],
                unresolved_spans: Vec::new(),
            },
        );
    pipeline.rar_sets.insert(
        (job_id, set_name.clone()),
        rar_state::RarSetState {
            facts: std::collections::BTreeMap::from([
                (0u32, dummy_rar_volume_facts(0)),
                (1u32, dummy_rar_volume_facts(1)),
            ]),
            volume_files: std::collections::BTreeMap::new(),
            cached_headers: None,
            shared_kdf_cache: std::sync::Arc::new(weaver_unrar::crypto::KdfCache::new()),
            verified_suspect_volumes: std::collections::HashSet::new(),
            active_workers: 0,
            in_flight_members: std::collections::HashSet::new(),
            extraction_generation: 0,
            phase: rar_state::RarSetPhase::WaitingForVolumes,
            plan: Some(rar_state::RarDerivedPlan {
                phase: rar_state::RarSetPhase::WaitingForVolumes,
                is_solid: false,
                ready_members: Vec::new(),
                member_names: vec!["E01.mkv".to_string()],
                member_dependencies: HashMap::new(),
                waiting_on_volumes: std::collections::HashSet::from([2u32]),
                deletion_eligible: std::collections::HashSet::new(),
                delete_decisions: std::collections::BTreeMap::new(),
                topology: pipeline
                    .jobs
                    .get(&job_id)
                    .unwrap()
                    .assembly
                    .archive_topology_for(&set_name)
                    .unwrap()
                    .clone(),
                fallback_reason: None,
            }),
        },
    );

    pipeline.reconcile_job_progress(job_id).await;

    let state = pipeline.jobs.get(&job_id).unwrap();
    assert_eq!(
        state.download_state,
        crate::jobs::model::DownloadState::Downloading
    );
    assert_eq!(state.post_state, crate::jobs::model::PostState::Idle);
    assert_eq!(state.status, JobStatus::Downloading);
}

#[tokio::test]
async fn purge_idle_rar_set_drops_shared_kdf_cache() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31244);
    let set_name = "show".to_string();

    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, "purge-job", temp_dir.path().join("purge-job")),
    );

    let shared_kdf_cache = std::sync::Arc::new(weaver_unrar::crypto::KdfCache::new());
    let weak_cache = std::sync::Arc::downgrade(&shared_kdf_cache);

    pipeline.rar_sets.insert(
        (job_id, set_name.clone()),
        rar_state::RarSetState {
            shared_kdf_cache,
            ..Default::default()
        },
    );

    pipeline.purge_empty_rar_set_if_idle(job_id, &set_name);

    assert!(!pipeline.rar_sets.contains_key(&(job_id, set_name.clone())));
    assert!(weak_cache.upgrade().is_none());
}
