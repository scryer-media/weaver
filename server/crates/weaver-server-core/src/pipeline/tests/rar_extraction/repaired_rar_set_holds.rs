//! `rar_extraction` tests, part of a mechanical split of the original file.

use super::*;

#[tokio::test]
async fn repaired_rar_set_holds_extraction_until_its_plan_is_rebuilt() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30242);
    let files = build_multifile_multivolume_rar_set();
    complete_rar_set_with_plan(
        &mut pipeline,
        job_id,
        &files,
        "Repaired Set Extraction Hold",
    )
    .await;

    let key = (job_id, "show".to_string());
    let verification = all_complete_verification(&files);
    let rewritten: HashSet<par2_rs::FileId> = [verification.files[1].file_id].into_iter().collect();

    pipeline
        .refresh_verified_complete_archive_topologies(job_id, &verification, &rewritten)
        .await;

    assert!(
        pipeline.job_has_pending_rar_refresh_for_current_sets(job_id),
        "the repaired set must be pending in the gate the completion path already defers on"
    );
    assert!(
        pipeline
            .rar_refresh_state
            .get(&key)
            .is_some_and(|state| state.structure_dirty),
        "a repaired set's plan must be marked stale so a member cannot start on it"
    );
}

#[tokio::test]
async fn a_clean_verdict_leaves_a_complete_rar_set_plan_alone() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30243);
    let files = build_multifile_multivolume_rar_set();
    complete_rar_set_with_plan(&mut pipeline, job_id, &files, "Clean Verdict Plan Kept").await;

    let key = (job_id, "show".to_string());
    let verification = all_complete_verification(&files);

    // Nothing was repaired, so nothing is stale: the additive inclusion must not
    // turn every clean verdict into a header-level rebuild.
    pipeline
        .refresh_verified_complete_archive_topologies(job_id, &verification, &HashSet::new())
        .await;

    assert!(
        !pipeline
            .rar_refresh_state
            .get(&key)
            .is_some_and(|state| state.structure_dirty),
        "a clean verdict must not mark an intact set's plan stale"
    );
}

#[tokio::test]
async fn renamed_verified_files_still_refresh_without_a_repair_write_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30244);
    let files = build_multifile_multivolume_rar_set();
    complete_rar_set_with_plan(&mut pipeline, job_id, &files, "Renamed Arm Unchanged").await;

    let mut verification = all_complete_verification(&files);
    verification.files[2].status =
        par2_rs::verify::FileStatus::Renamed(std::path::PathBuf::from(&files[2].0));

    // The pre-existing arm, unchanged: a rename refreshes on its own, with no
    // repair write set involved.
    assert_eq!(
        pipeline.verified_complete_archive_file_ids_needing_refresh(
            job_id,
            &verification,
            &HashSet::new(),
        ),
        vec![NzbFileId {
            job_id,
            file_index: 2
        }],
        "the renamed arm must keep refreshing exactly as before"
    );
}

#[tokio::test]
async fn a_stale_alias_set_with_no_live_claimants_retires_instead_of_failing() {
    // The job-10162 shape: an obfuscated post's PAR2 index rebinds every file
    // to canonical names early, the canonical set finalizes direct (nothing is
    // ever written under the alias names), but a topology entry keyed by the
    // posted alias survives. Extraction of that alias set finds no volumes on
    // disk and no live identity claims its names — it must retire, not fail a
    // clean job.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(90211);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Silver Horizon Stale Alias",
            &[("Silver.Horizon.S01E01.mkv".to_string(), 64)],
        ),
    )
    .await;

    let alias = "bWq99zz0testAliasSetName";
    let mut stale = crate::pipeline::archive::rar_state::RarSetState::default();
    stale.volume_files.insert(0, format!("{alias}.part001.rar"));
    stale.volume_files.insert(1, format!("{alias}.part002.rar"));
    pipeline.rar_sets.insert((job_id, alias.to_string()), stale);

    let extracted = pipeline.extract_rar_set(job_id, alias).await;
    assert_eq!(extracted, Ok(0), "retirement is not a failure");
    assert!(
        !pipeline.rar_sets.contains_key(&(job_id, alias.to_string())),
        "the alias set no live file claims is retired"
    );

    // The negative control: a set live files DO classify into keeps its
    // topology even when its volumes are missing on disk — that shape is
    // genuinely missing volumes, not a stale alias.
    let claimed_job = JobId(90212);
    let claimed_spec = JobSpec {
        name: "Silver Horizon Claimed Set".to_string(),
        password: None,
        total_bytes: 64,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: "claimed.part001.rar".to_string(),
            role: FileRole::from_filename("claimed.part001.rar"),
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![segment_spec! {
                number: 0,
                bytes: 64,
                message_id: "claimed-set-volume@example.com".to_string(),
            }],
        }],
    };
    insert_active_job(&mut pipeline, claimed_job, claimed_spec).await;
    let mut claimed = crate::pipeline::archive::rar_state::RarSetState::default();
    claimed
        .volume_files
        .insert(0, "claimed.part001.rar".to_string());
    pipeline
        .rar_sets
        .insert((claimed_job, "claimed".to_string()), claimed);

    assert_eq!(
        pipeline.clear_archive_set_if_unreferenced_and_idle(claimed_job, "claimed"),
        ArchiveSetRetirement::StillReferenced,
        "the helper must say why it did nothing rather than leave the caller to guess"
    );
    let _ = pipeline.extract_rar_set(claimed_job, "claimed").await;
    assert!(
        pipeline
            .rar_sets
            .contains_key(&(claimed_job, "claimed".to_string())),
        "a set with live claimants is never retired by the missing-volume path"
    );
}

#[tokio::test]
async fn a_claimed_alias_set_the_job_already_extracted_is_absorbed_not_rescheduled() {
    // One step past the job-10162 shape: the alias name is not merely a
    // surviving topology entry, it is what every live identity says it is — so
    // retirement is refused — and no runtime set was ever registered under it,
    // so the map key the guard used to read as proof of retirement was never
    // there to begin with. Announcing a retirement that did not happen and
    // re-arming the completion check hands the same name straight back: the
    // check draws its candidates from the live classifications, which nothing
    // touched.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(90213);
    let alias = "kP42x7aliasCrystalMeridian";
    let canonical = "crystal.meridian";
    let volumes = [
        "crystal.meridian.part001.rar".to_string(),
        "crystal.meridian.part002.rar".to_string(),
    ];
    claimed_alias_set_job(
        &mut pipeline,
        job_id,
        "Crystal Meridian Claimed Alias",
        alias,
        &volumes,
    )
    .await;

    // The canonical set holds the same files and has already delivered them.
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .set_archive_topology(
            canonical.to_string(),
            crate::jobs::assembly::ArchiveTopology {
                archive_type: crate::jobs::assembly::ArchiveType::Rar,
                volume_map: HashMap::from([(volumes[0].clone(), 0), (volumes[1].clone(), 1)]),
                complete_volumes: [0u32, 1].into_iter().collect(),
                expected_volume_count: Some(2),
                members: Vec::new(),
                unresolved_spans: Vec::new(),
            },
        );
    pipeline
        .extracted_archives
        .entry(job_id)
        .or_default()
        .insert(canonical.to_string());

    assert!(
        pipeline.volume_paths_for_rar_set(job_id, alias).is_empty()
            && !pipeline.rar_sets.contains_key(&(job_id, alias.to_string())),
        "the fixture must reproduce the shape: nothing on disk and no runtime set"
    );
    assert_eq!(
        pipeline.clear_archive_set_if_unreferenced_and_idle(job_id, alias),
        ArchiveSetRetirement::StillReferenced,
        "the live classifications keep claiming the alias, so retirement cannot run — and \
         those same classifications keep offering it to the completion check"
    );

    assert_eq!(
        pipeline.extract_rar_set(job_id, alias).await,
        Ok(0),
        "a job whose content is already delivered is not failed over a second name for it"
    );
    assert!(
        pipeline
            .extracted_archives
            .get(&job_id)
            .is_some_and(|sets| sets.contains(alias)),
        "the alias must be answered rather than retired: retirement never ran, so only an \
         absorption can stop the check re-offering the name"
    );

    let checks = drain_completion_checks(&mut pipeline, 16).await;
    assert!(
        checks <= 4,
        "the completion check must reach a fixed point, not re-arm itself: {checks} checks ran"
    );
    assert!(
        pipeline.pending_completion_checks.is_empty(),
        "no completion check may be left armed with nothing to do"
    );
    assert!(
        pipeline
            .inflight_extractions
            .get(&job_id)
            .is_none_or(|sets| sets.is_empty()),
        "the absorbed name must never be dispatched to an extraction with nothing to open"
    );
    assert!(
        matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Moving)
        ),
        "the job finalizes on the content it already holds: {:?}",
        job_status_for_assert(&pipeline, job_id)
    );
}

#[tokio::test]
async fn a_claimed_alias_set_this_job_never_materialized_is_absorbed() {
    // The same shape with no surviving name link: an identity rebind can
    // rewrite either side of it, so the alias's own materialization record is
    // what settles the question. Volume facts are persisted the moment a
    // volume's headers parse off real bytes, and this job has none under the
    // alias — it was named, never written.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(90214);
    let alias = "zT08m3aliasSilverHorizon";
    let volumes = [
        "silver.horizon.part001.rar".to_string(),
        "silver.horizon.part002.rar".to_string(),
    ];
    claimed_alias_set_job(
        &mut pipeline,
        job_id,
        "Silver Horizon Unmaterialized Alias",
        alias,
        &volumes,
    )
    .await;
    pipeline
        .extracted_archives
        .entry(job_id)
        .or_default()
        .insert("silver.horizon".to_string());

    assert_eq!(
        pipeline.classify_unmaterialized_archive_set(job_id, alias),
        UnmaterializedArchiveSet::NeverMaterialized,
    );
    assert_eq!(pipeline.extract_rar_set(job_id, alias).await, Ok(0));
    assert!(
        pipeline
            .extracted_archives
            .get(&job_id)
            .is_some_and(|sets| sets.contains(alias))
    );

    drain_completion_checks(&mut pipeline, 16).await;
    assert!(pipeline.pending_completion_checks.is_empty());
}

#[tokio::test]
async fn a_claimed_alias_set_is_absorbed_on_persisted_facts_after_a_restart() {
    // The restart cut of the same shape: `extracted_archives` is runtime
    // memory and comes back empty, so the in-memory record of where the bytes
    // went is gone. The persisted volume facts survive, and facts parsed under
    // another set name are the durable half of the same story — this job read
    // real archive bytes under that name, and never parsed any under the
    // alias.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(90216);
    let alias = "qV31k9aliasAmberRelay";
    let volumes = [
        "amber.relay.part001.rar".to_string(),
        "amber.relay.part002.rar".to_string(),
    ];
    claimed_alias_set_job(
        &mut pipeline,
        job_id,
        "Amber Relay Restarted Alias",
        alias,
        &volumes,
    )
    .await;

    pipeline
        .db
        .save_rar_volume_facts(job_id, "amber.relay", 0, b"facts")
        .unwrap();

    assert!(
        !pipeline.extracted_archives.contains_key(&job_id),
        "the fixture must reproduce the post-restart shape: no in-memory extracted record"
    );
    assert_eq!(
        pipeline.classify_unmaterialized_archive_set(job_id, alias),
        UnmaterializedArchiveSet::NeverMaterialized,
        "facts persisted under another set name must carry the absorption across a restart"
    );
    assert_eq!(pipeline.extract_rar_set(job_id, alias).await, Ok(0));
    assert!(
        pipeline
            .extracted_archives
            .get(&job_id)
            .is_some_and(|sets| sets.contains(alias))
    );

    drain_completion_checks(&mut pipeline, 16).await;
    assert!(pipeline.pending_completion_checks.is_empty());
}

#[tokio::test]
async fn a_refresh_that_integrated_a_members_span_satisfies_the_coverage_it_owed() {
    // The job-11857 shape: a restored set holds every volume on disk while the
    // facts ledger recorded only a prefix. Coverage measured by fact-backed
    // `complete_volumes` makes `rar_member_refresh_request` demand a refresh
    // that registers no fact and so can never answer the demand — a full-speed
    // livelock. Coverage is what the refresh integrated into the header view;
    // a rebuild that saw the member's whole span settles the demand it
    // triggered.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(90217);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Silver Horizon Prefix Facts",
            &[("silver.horizon.mkv".to_string(), 64)],
        ),
    )
    .await;

    let set_name = "silver.horizon";
    let member = "silver.horizon.mkv";
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: HashMap::from([
            ("silver.horizon.rar".to_string(), 0),
            ("silver.horizon.r00".to_string(), 1),
            ("silver.horizon.r01".to_string(), 2),
        ]),
        // The ledger's view: only the prefix ever registered facts.
        complete_volumes: [0u32, 1].into_iter().collect(),
        expected_volume_count: Some(3),
        members: vec![crate::jobs::assembly::ArchiveMember {
            name: member.to_string(),
            first_volume: 0,
            last_volume: 2,
            unpacked_size: 64,
        }],
        unresolved_spans: Vec::new(),
    };
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .set_archive_topology(set_name.to_string(), topology.clone());

    let key = (job_id, set_name.to_string());
    let mut set_state = crate::pipeline::archive::rar_state::RarSetState::default();
    for (filename, volume) in &topology.volume_map {
        set_state.volume_files.insert(*volume, filename.clone());
    }
    pipeline.rar_sets.insert(key.clone(), set_state);

    let request = RarRefreshRequest {
        target_completed_volume: 2,
        reason: RefreshReason::CoverageExpansion,
    };
    pipeline.rar_refresh_state.insert(
        key.clone(),
        RarRefreshState {
            in_flight: Some(request),
            queued: None,
            latest_completed_volume: 2,
            // The stale baseline a fact-derived measure produces.
            refreshed_volumes: BTreeSet::from([0, 1]),
            structure_dirty: false,
            last_error: None,
            last_completion_fingerprint: None,
        },
    );

    assert!(
        pipeline
            .rar_member_refresh_request(job_id, set_name, member)
            .is_some(),
        "before the refresh lands, the member's span exceeds the recorded coverage"
    );

    pipeline
        .handle_rar_refresh_done(RarRefreshDone {
            job_id,
            set_name: set_name.to_string(),
            request,
            extraction_generation: 0,
            result: Ok(ComputedRarSetState {
                plan: crate::pipeline::archive::rar_state::RarDerivedPlan {
                    phase: crate::pipeline::archive::rar_state::RarSetPhase::Ready,
                    is_solid: false,
                    ready_members: vec![crate::pipeline::archive::rar_state::RarReadyMember {
                        name: member.to_string(),
                    }],
                    member_names: vec![member.to_string()],
                    member_dependencies: HashMap::new(),
                    waiting_on_volumes: HashSet::new(),
                    deletion_eligible: HashSet::new(),
                    delete_decisions: BTreeMap::new(),
                    topology: topology.clone(),
                    fallback_reason: None,
                },
                headers: Vec::new(),
                rebuild_source:
                    crate::pipeline::archive::topology::RarTopologyRebuildSource::VolumeZero,
                // What the rebuild actually opened: the whole set, facts or no
                // facts.
                integrated_volumes: BTreeSet::from([0, 1, 2]),
            }),
        })
        .await;

    let refresh = pipeline
        .rar_refresh_state
        .get(&key)
        .expect("refresh state should remain");
    assert_eq!(
        refresh.refreshed_volumes,
        BTreeSet::from([0, 1, 2]),
        "coverage records what the refresh integrated, not what the facts ledger holds"
    );
    assert!(
        refresh.in_flight.is_none() && refresh.queued.is_none(),
        "a refresh that integrated the span leaves nothing owed — no respawn"
    );
    assert_eq!(
        pipeline.rar_member_refresh_request(job_id, set_name, member),
        None,
        "the demand this refresh was spawned for is satisfied by its own completion"
    );
}

#[tokio::test]
async fn a_claimed_set_with_genuinely_missing_volumes_fails_once_instead_of_looping() {
    // Nothing was ever extracted for this job and nothing is on disk: the
    // volumes are simply absent. That is a real failure and it must be reported
    // once — never as a retirement, and never as a reschedule that comes back.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(90215);
    let alias = "hR55w2aliasEmberCascade";
    let volumes = [
        "ember.cascade.part001.rar".to_string(),
        "ember.cascade.part002.rar".to_string(),
    ];
    claimed_alias_set_job(
        &mut pipeline,
        job_id,
        "Ember Cascade Missing Volumes",
        alias,
        &volumes,
    )
    .await;

    assert_eq!(
        pipeline.classify_unmaterialized_archive_set(job_id, alias),
        UnmaterializedArchiveSet::MissingVolumes,
        "with nothing extracted anywhere, the bytes went nowhere: this is a real absence"
    );

    // The first visit dispatches the extraction whose failure fails the job.
    assert_eq!(pipeline.extract_rar_set(job_id, alias).await, Ok(0));
    assert!(
        !pipeline
            .extracted_archives
            .get(&job_id)
            .is_some_and(|sets| sets.contains(alias)),
        "a genuinely missing set is never absorbed"
    );
    // A check that re-runs before that failure lands says so synchronously
    // rather than queueing a second doomed extraction behind the first.
    assert!(
        pipeline.extract_rar_set(job_id, alias).await.is_err(),
        "the second visit must report the absence, not spawn again"
    );

    let done = tokio::time::timeout(Duration::from_secs(5), pipeline.extract_done_rx.recv())
        .await
        .expect("the dispatched extraction should report back")
        .expect("the extraction channel should stay open");
    pipeline.handle_extraction_done(done).await;

    assert!(
        matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Failed { .. })
        ),
        "a set with no volumes and nowhere for its bytes to have gone fails the job: {:?}",
        job_status_for_assert(&pipeline, job_id)
    );
    drain_completion_checks(&mut pipeline, 16).await;
    assert!(
        pipeline.pending_completion_checks.is_empty(),
        "a failed job holds no armed completion check"
    );
}

#[tokio::test]
async fn an_old_numbering_rar4_set_keys_facts_by_the_layout_not_the_parsed_number() {
    // Production job 11857: an old-numbering RAR4 set's headers state no
    // volume number, so every volume parses as volume 0. Keying the facts
    // ledger by that parse folded the whole set onto one entry, the ledger
    // never grew past a prefix, and the member gate demanded coverage no
    // refresh could record. The layout names the volume; the header only
    // validates it.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(90218);
    let payload = vec![0x5Au8; 96];
    let files =
        single_member_rar4_old_numbering_set("silver.horizon", "silver.horizon.mkv", &payload, 3);
    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec("Silver Horizon Old Numbering", &files),
    )
    .await;

    for (index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, index as u32, filename, bytes).await;
    }

    let key = (job_id, "silver.horizon".to_string());
    let state = pipeline.rar_sets.get(&key).expect("RAR set state");
    assert_eq!(
        state.facts.keys().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([0, 1, 2]),
        "facts key by the layout-derived volume even when every header parses as volume 0"
    );
    assert_eq!(
        state.volume_files.get(&0).map(String::as_str),
        Some("silver.horizon.rar")
    );
    assert_eq!(
        state.volume_files.get(&1).map(String::as_str),
        Some("silver.horizon.r00")
    );
    assert_eq!(
        state.volume_files.get(&2).map(String::as_str),
        Some("silver.horizon.r01")
    );
    let plan = state.plan.as_ref().expect("completed set derives a plan");
    assert_eq!(
        plan.topology.complete_volumes,
        HashSet::from([0, 1, 2]),
        "the ledger's completeness view covers the whole set, not a collapsed prefix"
    );
    assert_eq!(
        pipeline.rar_member_refresh_request(job_id, "silver.horizon", "silver.horizon.mkv"),
        None,
        "a fully-present set owes no coverage refresh"
    );
}

#[tokio::test]
async fn restore_rebuilds_layout_keys_for_an_old_numbering_rar4_set() {
    // The restart half of job 11857: the legacy ledger holds one collapsed row
    // (every header parses as volume 0) while the whole set sits on disk.
    // Restore must trust the row's layout key, rebuild the live map by the
    // layout, and let fresh parses win — not fold the set back onto volume 0
    // and then discard everything else as stale.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(90219);
    let payload = vec![0xC3u8; 96];
    let files =
        single_member_rar4_old_numbering_set("silver.horizon", "silver.horizon.mkv", &payload, 3);
    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec("Silver Horizon Restored Old Numbering", &files),
    )
    .await;

    // All three volumes are on disk and complete in the assembly, but nothing
    // ran through live registration: this is a job coming back from a restart.
    let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
    for (index, (filename, bytes)) in files.iter().enumerate() {
        tokio::fs::write(working_dir.join(filename), bytes)
            .await
            .unwrap();
        pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: index as u32,
            })
            .unwrap()
            .commit_segment(0, bytes.len() as u32)
            .unwrap();
    }

    // The legacy DB shape: one row, keyed by the collapsed parse.
    let facts =
        unrar_rs::RarArchive::parse_volume_facts(std::io::Cursor::new(files[0].1.clone()), None)
            .unwrap();
    assert_eq!(
        facts.volume_number, None,
        "old numbering states no volume number"
    );
    let blob = rmp_serde::to_vec_named(&facts).unwrap();
    pipeline
        .db
        .save_rar_volume_facts(job_id, "silver.horizon", 0, &blob)
        .unwrap();

    pipeline.restore_rar_state_for_job(job_id).await;

    let key = (job_id, "silver.horizon".to_string());
    let state = pipeline
        .rar_sets
        .get(&key)
        .expect("RAR set state survives restore");
    assert_eq!(
        state.facts.keys().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([0, 1, 2]),
        "restore rebuilds the ledger by layout keys instead of collapsing onto the parsed 0"
    );
    assert_eq!(
        state.volume_files.get(&1).map(String::as_str),
        Some("silver.horizon.r00"),
        "the live volume-file map is layout-keyed, one entry per file"
    );
    let plan = state
        .plan
        .as_ref()
        .expect("restore recomputes the plan for a set with facts");
    assert_eq!(plan.topology.complete_volumes, HashSet::from([0, 1, 2]));
    assert_eq!(
        pipeline.rar_member_refresh_request(job_id, "silver.horizon", "silver.horizon.mkv"),
        None,
        "a restored, fully-present set owes no coverage refresh"
    );
}

#[tokio::test]
async fn invalidating_a_rar_snapshot_forgets_the_persisted_copy_too() {
    // `load_rar_snapshot` falls back to the database when the in-memory copy
    // is gone. Clearing only `cached_headers` therefore invalidated nothing:
    // the next recompute quietly rebuilt from the persisted snapshot.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(90221);
    let files = build_multifile_multivolume_rar_set();
    insert_active_job(
        &mut pipeline,
        job_id,
        rar_job_spec("Snapshot Invalidation", &files),
    )
    .await;
    for (index, (filename, bytes)) in files.iter().enumerate() {
        write_and_complete_rar_volume(&mut pipeline, job_id, index as u32, filename, bytes).await;
    }
    assert!(pipeline.load_rar_snapshot(job_id, "show").is_some());

    pipeline
        .rar_sets
        .get_mut(&(job_id, "show".to_string()))
        .unwrap()
        .cached_headers = None;
    assert!(
        pipeline.load_rar_snapshot(job_id, "show").is_some(),
        "dropping the in-memory copy alone still serves the persisted snapshot"
    );

    pipeline.invalidate_rar_snapshot(job_id, "show");
    assert!(pipeline.load_rar_snapshot(job_id, "show").is_none());
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .archive_topology_for("show")
            .is_some(),
        "invalidation keeps the topology; only the header view is forgotten"
    );
}

#[tokio::test]
async fn a_recovery_volume_restore_rebuilds_the_set_from_disk_and_extracts_the_member() {
    // Bench run 2026-09-04, job 10000: the restore wrote volume 2, but the
    // recompute after it rebuilt from the header snapshot persisted while the
    // set still had the hole. The plan it produced put the member in volume 0
    // alone, extraction was handed that one volume, and the decode ran out of
    // bits at its end. The rebuild after a restore has to read the disk.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(90222);
    let job_name = "Silver Horizon Recovery Volume";
    let payload =
        stage_old_numbering_rar4_set_with_a_recovery_volume(&mut pipeline, job_id, job_name).await;
    let key = (job_id, "silver.horizon".to_string());
    let stale_snapshot = pipeline
        .load_rar_snapshot(job_id, "silver.horizon")
        .expect("the incomplete set persisted a header snapshot");

    // The completion checkpoint is what reaches for the recovery volume once
    // downloads are exhausted and the set is still waiting on a hole. The
    // fixture committed its segments directly, so empty the queue the way a
    // finished download pass would have.
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .drain_all();
    pipeline.check_job_completion(job_id).await;

    let state = pipeline.rar_sets.get(&key).expect("set state");
    assert_eq!(
        state.volume_files.get(&2).map(String::as_str),
        Some("silver.horizon.r01"),
        "the restore registers the rebuilt volume under its layout number: {}",
        debug_job_state(&pipeline, job_id)
    );
    assert!(state.facts.contains_key(&2));
    let fresh_snapshot = pipeline
        .load_rar_snapshot(job_id, "silver.horizon")
        .expect("the rebuild after the restore persists a fresh snapshot");
    assert_ne!(fresh_snapshot, stale_snapshot);
    let plan = state.plan.as_ref().expect("plan");
    assert!(
        plan.waiting_on_volumes.is_empty(),
        "nothing is missing once the hole is rebuilt: {plan:?}"
    );
    let member = plan
        .topology
        .members
        .iter()
        .find(|member| member.name == "silver.horizon.mkv")
        .expect("member");
    assert_eq!(
        (member.first_volume, member.last_volume),
        (0, 3),
        "the rebuilt topology spans every volume, the restored one included"
    );

    drive_extractions_to_terminal(&mut pipeline, job_id, 8).await;
    assert!(
        matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete)
        ),
        "{}",
        debug_job_state(&pipeline, job_id)
    );
    let dest = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(job_name));
    assert_eq!(
        std::fs::read(dest.join("silver.horizon.mkv")).unwrap(),
        payload,
        "the member extracted through the restored volume is byte-exact"
    );
}

#[tokio::test]
async fn a_source_retry_keeps_a_restored_recovery_volume_registered() {
    // The second half of the same bench failure: after the botched extraction
    // the source retry cleared the set, and the rebuild that followed only
    // saw the assembly's files. The restored volume was still on disk, but
    // nothing registered it any more — "volume 2 unavailable: not registered"
    // — and the set went back to waiting on a hole it had already filled.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(90223);
    stage_old_numbering_rar4_set_with_a_recovery_volume(
        &mut pipeline,
        job_id,
        "Silver Horizon Recovery Volume Retry",
    )
    .await;
    pause_job_for_rar_fixture_setup(&mut pipeline, job_id);
    assert!(
        pipeline
            .try_restore_rar_recovery_volumes(job_id)
            .await
            .expect("restore runs"),
        "the recovery volume rebuilds the hole"
    );
    let key = (job_id, "silver.horizon".to_string());
    assert_eq!(
        pipeline.rar_sets[&key]
            .volume_files
            .get(&2)
            .map(String::as_str),
        Some("silver.horizon.r01")
    );

    pipeline.clear_archive_set_for_source_retry(job_id, "silver.horizon");

    let state = pipeline
        .rar_sets
        .get(&key)
        .expect("the restored volume keeps the set registered across the retry");
    assert_eq!(
        state.volume_files.get(&2).map(String::as_str),
        Some("silver.horizon.r01")
    );
    assert!(state.facts.contains_key(&2));
    assert!(
        !state.volume_files.contains_key(&0) && !state.volume_files.contains_key(&3),
        "downloaded volumes are the retry's to re-register: {:?}",
        state.volume_files
    );
    assert!(state.plan.is_none());
    assert!(
        pipeline
            .load_rar_snapshot(job_id, "silver.horizon")
            .is_none()
    );
    assert!(
        pipeline
            .volume_paths_for_rar_set(job_id, "silver.horizon")
            .contains_key(&2),
        "the rebuild's volume view still reaches the restored file"
    );
    let persisted = pipeline.db.load_all_rar_volume_facts(job_id).unwrap();
    assert_eq!(
        persisted
            .get("silver.horizon")
            .map(|rows| rows.iter().map(|(volume, _)| *volume).collect::<Vec<_>>()),
        Some(vec![2]),
        "the restored volume's facts survive the retry's ledger reset"
    );

    // The retry's re-downloads re-register the NZB volumes one by one; with the
    // restored volume still known, the set comes back whole rather than waiting
    // on a hole it already filled.
    resume_job_downloading_for_test(&mut pipeline, job_id);
    for file_index in 0..3u32 {
        pipeline
            .refresh_archive_state_for_completed_file(
                job_id,
                NzbFileId { job_id, file_index },
                true,
            )
            .await;
        drain_rar_refreshes(&mut pipeline).await;
    }
    let plan = pipeline.rar_sets[&key]
        .plan
        .as_ref()
        .expect("re-registration rebuilds the plan");
    assert!(
        plan.waiting_on_volumes.is_empty(),
        "the set is whole again: {plan:?}"
    );
    assert_eq!(
        plan.topology.complete_volumes,
        HashSet::from([0, 1, 2, 3]),
        "the rebuilt topology counts the restored volume"
    );
}

#[tokio::test]
async fn a_par2_repaired_volume_rebuilds_the_set_from_disk_not_the_stale_snapshot() {
    // A volume whose articles were never posted leaves the set with a hole,
    // and the header snapshot persisted while that hole was open describes a
    // set that ends before it. PAR2 recovery recreates the volume, but the
    // plan rebuild the repair triggers read that snapshot back — so the
    // member's span stopped at the last volume the snapshot knew, extraction
    // was handed a truncated volume list, and the decode failed. Only the
    // re-analysis afterwards saw the whole set.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(90231);
    let job_name = "Silver Horizon Repaired Volume";
    let payload: Vec<u8> = (0..5120u32)
        .map(|index| (index.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    let volumes: Vec<(String, Vec<u8>)> =
        single_member_rar4_old_numbering_set("silver.horizon", "silver.horizon.mkv", &payload, 5)
            .into_iter()
            .map(|(filename, bytes)| (filename, with_rar4_header_crcs(bytes)))
            .collect();
    assert_eq!(volumes.len(), 5);

    // The NZB carries all five volumes; volume 2's articles answer 430, so
    // four of them land and the set registers with a hole.
    insert_active_job(&mut pipeline, job_id, rar_job_spec(job_name, &volumes)).await;
    for (index, (filename, bytes)) in volumes.iter().enumerate() {
        if index == 2 {
            continue;
        }
        write_and_complete_rar_volume(&mut pipeline, job_id, index as u32, filename, bytes).await;
    }
    let key = (job_id, "silver.horizon".to_string());
    let stale_snapshot = pipeline
        .load_rar_snapshot(job_id, "silver.horizon")
        .expect("the set with the hole persisted a header snapshot");

    // PAR2 repair writes the missing volume and the post-repair refresh
    // registers it, exactly as `refresh_verified_complete_archive_topologies`
    // does for each rewritten output.
    write_and_complete_rar_volume(&mut pipeline, job_id, 2, &volumes[2].0, &volumes[2].1).await;
    pipeline.invalidate_rar_plans_for_repaired_sets(
        job_id,
        std::iter::once("silver.horizon".to_string()).collect(),
    );
    assert!(
        pipeline
            .load_rar_snapshot(job_id, "silver.horizon")
            .is_none(),
        "the pre-repair snapshot must not survive to answer the rebuild"
    );
    drain_rar_refreshes(&mut pipeline).await;

    let fresh_snapshot = pipeline
        .load_rar_snapshot(job_id, "silver.horizon")
        .expect("the rebuild persists a snapshot of the repaired set");
    assert_ne!(fresh_snapshot, stale_snapshot);

    let state = pipeline.rar_sets.get(&key).expect("set state");
    let plan = state.plan.as_ref().expect("plan");
    assert!(
        plan.waiting_on_volumes.is_empty(),
        "nothing is missing once the repair fills the hole: {plan:?}"
    );
    let member = plan
        .topology
        .members
        .iter()
        .find(|member| member.name == "silver.horizon.mkv")
        .expect("member");
    assert_eq!(
        (member.first_volume, member.last_volume),
        (0, 4),
        "the first dispatch after the repair must see every volume: {}",
        debug_job_state(&pipeline, job_id)
    );

    let volume_paths_map = pipeline.volume_paths_for_rar_set(job_id, "silver.horizon");
    let dispatched = pipeline.volume_paths_for_rar_members(
        job_id,
        "silver.horizon",
        &["silver.horizon.mkv".to_string()],
        &volume_paths_map,
        pipeline
            .load_rar_snapshot(job_id, "silver.horizon")
            .is_some(),
        plan.is_solid,
    );
    assert_eq!(
        dispatched.keys().copied().collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4],
        "extraction is dispatched with the whole repaired set"
    );
}

#[tokio::test]
async fn damage_on_record_holds_extraction_until_the_recovery_set_rules() {
    // The set's bytes were already found wrong — the direct router's part
    // CRC32 did not match what it routed — and the recovery set has not been
    // asked yet. Extraction ran first anyway, read the whole set to reach a
    // failure that was already known, and only then was PAR2 allowed to
    // repair, after which the set was read a second time.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(90232);
    let (set_name, volumes) =
        rar_set_with_recovery_awaiting_volumes(&mut pipeline, job_id, "Silver Horizon Damaged Set")
            .await;

    // The fact, recorded the way a damage demotion records it, and at the same
    // point: before the demoted volumes are replayed into the conventional
    // completion seam. Keyed on the damage rather than on any one reason, so a
    // seam that repairs a mismatched part in place instead of demoting still
    // reaches this.
    pipeline.note_known_archive_set_damage(job_id, &set_name);
    // The same predicate the completion checkpoint reads to refuse the stored
    // set's "a clean decode would prove integrity" shortcut, so this asserts
    // the input to both halves of the gate.
    assert!(pipeline.archive_extraction_held_for_known_damage(job_id));

    // Every volume lands, and each completion walks the archive-probe door
    // that dispatches extraction.
    complete_rar_volumes(&mut pipeline, job_id, &volumes).await;
    pipeline.try_rar_extraction(job_id).await;
    assert!(
        !extraction_dispatched(&pipeline, job_id, &set_name),
        "extraction must not open a set already known damaged: {}",
        debug_job_state(&pipeline, job_id)
    );

    // Asked and answered releases the hold too, even when the answer is one
    // the pass could not act on: an unrepairable set must not hold its own
    // undamaged members waiting for a second verdict that is never coming.
    let set_id = pipeline
        .par2_served_set_id(job_id)
        .expect("the fixture serves one recovery set");
    pipeline
        .ensure_par2_runtime(job_id)
        .ensure_set_runtime(set_id)
        .settled = true;
    assert!(!pipeline.archive_extraction_held_for_known_damage(job_id));
    pipeline
        .ensure_par2_runtime(job_id)
        .ensure_set_runtime(set_id)
        .settled = false;
    assert!(pipeline.archive_extraction_held_for_known_damage(job_id));

    // The verdict lands — a clean pass and the tail of a repair both mark the
    // job verified — and the hold lifts on the same tick.
    pipeline.par2_verified.insert(job_id);
    assert!(!pipeline.archive_extraction_held_for_known_damage(job_id));
    pipeline.try_rar_extraction(job_id).await;
    assert!(
        extraction_dispatched(&pipeline, job_id, &set_name),
        "with the verdict in hand the same set extracts: {}",
        debug_job_state(&pipeline, job_id)
    );
}

#[tokio::test]
async fn a_clean_job_extracts_without_waiting_for_a_recovery_verdict() {
    // The other half of the requirement: nothing on record means nothing
    // changes. A clean job with recovery data still extracts on the volume
    // completions themselves, with no verdict and no hash pass in front of it.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _intermediate_dir, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(90233);
    let (set_name, volumes) =
        rar_set_with_recovery_awaiting_volumes(&mut pipeline, job_id, "Silver Horizon Clean Set")
            .await;

    assert!(!pipeline.archive_extraction_held_for_known_damage(job_id));
    complete_rar_volumes(&mut pipeline, job_id, &volumes).await;
    assert!(!pipeline.par2_verified.contains(&job_id));
    assert!(
        extraction_dispatched(&pipeline, job_id, &set_name),
        "a clean job keeps the extract-first path: {}",
        debug_job_state(&pipeline, job_id)
    );
}
