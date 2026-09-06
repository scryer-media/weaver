//! Restart
//! Repair while still direct

use super::*;

// ---------------------------------------------------------------------------
// Restart
// ---------------------------------------------------------------------------

#[tokio::test]
async fn restart_after_refetch_demotion_restores_incomplete_source_ownership() {
    let member_name = "Silver.Horizon.S01E25.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41057);

    let (pipeline, working_dir, _) =
        demote_mid_download(&temp_dir, job_id, &volumes, |_, working_dir| {
            // Reconstruction refuses per volume, so a *whole*-set refetch — the
            // state this restart differential is about — needs every covered
            // volume to lose its envelope, not just the first one. Volume 2 was
            // never covered and has nothing to rebuild either way.
            for volume_index in 0..2 {
                std::fs::remove_file(
                    working_dir.join(format!("silver.horizon.f0.vol{volume_index:05}.envelope")),
                )
                .unwrap();
            }
        })
        .await;
    assert!(
        pipeline.db.load_direct_coverage(job_id).unwrap().is_empty(),
        "the direct checkpoint must be retired before the simulated crash"
    );
    let (persisted_progress, persisted_complete) =
        pipeline.db.load_active_file_runtime(job_id).unwrap();
    assert!(persisted_progress.is_empty() && persisted_complete.is_empty());
    let file_progress = persisted_progress;
    let complete_files = persisted_complete
        .into_iter()
        .map(|file_index| NzbFileId { job_id, file_index })
        .collect();
    assert_eq!(
        pipeline.direct_store.pending_materialization_files(job_id),
        volumes.len(),
        "the live process still owns the demotion gate before the crash"
    );
    drop(pipeline);

    let (mut restarted, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    restarted.direct_store.set_gate(DirectStoreGate::Enabled);
    restarted
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec: direct_store_job_spec("Silver Horizon", &volumes),
            complete_files,
            file_progress,
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
            working_dir: working_dir.clone(),
        })
        .await
        .unwrap();

    assert_eq!(
        restarted.direct_store.pending_materialization_files(job_id),
        0,
        "the in-memory gate is intentionally not persisted"
    );
    let expected: Vec<(u32, u32)> = (0..volumes.len() as u32)
        .flat_map(|file_index| [(file_index, 0), (file_index, 1)])
        .collect();
    assert_eq!(peek_queued_segments(&mut restarted, job_id), expected);
    assert!(
        restarted.job_has_pending_download_pipeline_work(job_id),
        "ordinary restart assembly must hold PAR2 behind the queued source articles"
    );

    for (file_index, segment_number) in expected {
        dispatch_and_submit(
            &mut restarted,
            job_id,
            &volumes,
            file_index,
            segment_number,
            2,
        )
        .await;
    }
    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "a checkpoint-free restart may re-admit the incomplete set, but it must \
         retain the no-source-volume direct path"
    );
    assert!(
        format!("{:?}", restarted.direct_store.sets_for(job_id)).contains("Finalized"),
        "the restarted set must not settle until all source articles have been rerouted"
    );

    restarted.schedule_job_completion_check(job_id);
    drain_rar_refreshes(&mut restarted).await;
    drive_extractions_to_terminal(&mut restarted, job_id, 64).await;
    let (member, _) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(member.as_deref(), Some(payload.as_slice()));
    assert!(matches!(
        job_status_for_assert(&restarted, job_id),
        Some(JobStatus::Complete)
    ));
}

/// The headline restart differential.
///
/// Volume 0 arrives whole, volume 1 arrives half. After the restart the
/// checkpoint's floors must keep every article below them off the download
/// queue, everything above them must come back, and the finished member must be
/// byte-identical to a run that was never interrupted — and to the conventional
/// extractor.
#[tokio::test]
async fn a_mid_download_restart_honours_its_floors_and_completes_byte_identically() {
    const ARTICLES: usize = 4;
    let member_name = "Silver.Horizon.S01E30.mkv";
    let payload: Vec<u8> = (0..8000u32).map(|index| (index % 251) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41060);
    // Volume 0 complete, volume 1 half — so the restore has one file it can skip
    // entirely and one it must skip only part of.
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1), (0, 2), (0, 3), (1, 0), (1, 1)];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    let mut pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;

    // The set came back from its checkpoint rather than from zero.
    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the restored job must carry its direct set");
    assert!(
        set.has_restart_seeded_coverage(),
        "coverage restored from a checkpoint is seeded and unverified until it is re-read"
    );
    // Suppression re-armed: a restored set's source volumes are still direct,
    // so no legacy floor, completed-file row or archive re-probe may be written
    // for them. The restore itself relies on the same thing in the other
    // direction — no completed-file row exists for a direct volume by
    // construction, so the legacy skip plan contributes nothing and the
    // coverage row is the only reason anything is skipped at all.
    assert!(
        (0..volumes.len() as u32)
            .all(|file_index| pipeline.is_direct_source_file(NzbFileId { job_id, file_index })),
        "every restored source volume must still be suppressed as a direct volume"
    );
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .is_some_and(|state| state.assembly.archive_topologies().is_empty()),
        "a restored direct set must not enter the archive topology"
    );

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        !queued.iter().any(|(file_index, _)| *file_index == 0),
        "volume 0 was complete at the barrier; none of its articles may be refetched, got {queued:?}"
    );
    assert!(
        !queued.contains(&(1, 0)),
        "volume 1's checkpointed articles must not be refetched, got {queued:?}"
    );
    // Volume 1's *second* article is checkpointed too and still comes back. The
    // floor counts **decoded** source bytes while the spec's `<segment bytes>`
    // is the yEnc-encoded size (~3% larger), so walking the spec against a
    // decoded floor always stops one article short. That is safe — it refetches
    // — and it is bounded to one article per *partially covered* volume, which
    // is one article per volume in flight. A volume the checkpoint calls
    // complete does not pay it at all, which is what the `complete` bit is for
    // and what volume 0 above demonstrates.
    assert!(
        queued.contains(&(1, 1)) && queued.contains(&(1, 2)) && queued.contains(&(1, 3)),
        "volume 1's uncheckpointed articles must come back, got {queued:?}"
    );
    assert!(
        (0..ARTICLES as u32).all(|segment| queued.contains(&(2, segment))),
        "volume 2 never arrived at all and must be refetched whole, got {queued:?}"
    );

    // Non-vacuity: the restart really did save work.
    assert!(
        queued.len() < volumes.len() * ARTICLES,
        "a restart that refetches everything is not honouring any floor"
    );

    // Feed exactly what the restore asked for, and nothing else.
    for (file_index, segment_number) in queued.clone() {
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            ARTICLES,
        )
        .await;
    }
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_none_or(|set| !set.has_restart_seeded_coverage()),
        "the member gate must have re-read the pre-restart ranges before finalizing"
    );
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let complete_dir = temp_dir.path().join("complete");
    let (restarted_member, restarted_location) =
        member_after_gate(&complete_dir, &working_dir, member_name);
    let restarted_status = job_status_for_assert(&pipeline, job_id);
    assert!(
        !volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists()),
        "a restarted direct set must still never materialize a source volume"
    );
    drop(pipeline);

    let uninterrupted = run_direct_store_gate(
        DirectStoreGate::Enabled,
        JobId(41061),
        member_name,
        &volumes,
        &in_order_arrivals(volumes.len()),
    )
    .await;
    let conventional = run_direct_store_gate(
        DirectStoreGate::Disabled,
        JobId(41062),
        member_name,
        &volumes,
        &in_order_arrivals(volumes.len()),
    )
    .await;

    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional gate must produce the member"
    );
    assert_eq!(
        (
            restarted_member.as_deref(),
            restarted_location,
            &restarted_status
        ),
        (
            uninterrupted.member.as_deref(),
            uninterrupted.member_location,
            &uninterrupted.status
        ),
        "a restarted direct job must finish exactly as an uninterrupted one"
    );
    assert_eq!(
        (restarted_member.as_deref(), restarted_location),
        (conventional.member.as_deref(), conventional.member_location),
        "a restarted direct job must finish exactly as the conventional extractor"
    );
}

/// A byte corrupted on disk while the process was down is caught by the
/// re-read, not committed.
#[tokio::test]
async fn a_byte_corrupted_while_the_process_was_down_fails_the_member_gate() {
    const ARTICLES: usize = 2;
    let member_name = "Silver.Horizon.S01E31.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 241) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41063);
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1)];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    // Flip a byte the checkpoint claims, while "the process is down".
    let partial = direct_partial(&temp_dir, JobId(41063), member_name);
    let mut bytes = std::fs::read(&partial).expect("volume 0 routed into the member partial");
    assert!(!bytes.is_empty(), "the partial must hold routed bytes");
    bytes[10] ^= 0xff;
    std::fs::write(&partial, &bytes).unwrap();

    let mut pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;
    let queued = peek_queued_segments(&mut pipeline, job_id);
    for (file_index, segment_number) in queued {
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            ARTICLES,
        )
        .await;
    }

    // The **reason**, not merely "something demoted". A bare `Demoted` passes for
    // a set that never got as far as the re-read — a refused row, a rebuild
    // failure, a missing destination — and this test's whole subject is the
    // re-read catching a byte that changed on disk.
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(MemberChecksumMismatch)")
            || shape.contains("Demoted(PartChecksumMismatch)"),
        "corruption introduced while the process was down must fail the re-read gate on a \
         checksum, not on some earlier refusal, got {shape}"
    );
    // The demotion deletes the set's partial outputs rather than committing
    // them, and requeues the volumes for the conventional path — which in this
    // harness has no server behind it, so the job stops here rather than
    // finishing. What matters is that the corrupted bytes were never promoted to
    // a destination.
    let complete_dir = temp_dir.path().join("complete");
    let (member, _) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        member, None,
        "corrupt coverage must not be committed to the member's destination"
    );
    assert!(
        !partial.exists(),
        "a demoted set deletes its partials rather than leaving corrupt bytes behind"
    );
}

/// The payload lands on the **complete** volume and the scratch does not.
///
/// The two are observed on a live job rather than derived from a plan, because
/// the derivation is only half the claim: what matters operationally is which
/// filesystem the bytes are actually written to as the articles arrive, and that
/// they are still there — under the staging root — when finalization renames
/// them into place.
#[tokio::test]
async fn a_direct_set_writes_payload_to_the_staging_root_and_scratch_to_the_working_dir() {
    let member_name = "Silver.Horizon.S01E44.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 149) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    // Small enough that the first held payload pages out, so the scratch file
    // this test is half about actually exists.
    pipeline.direct_store.set_holds_budget(64);
    let job_id = JobId(41120);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    let staging = payload_root(&temp_dir, job_id);
    assert!(
        !staging.starts_with(&working_dir),
        "non-vacuity: the harness must give the job two genuinely separate roots"
    );

    // Payload before the header, so the router has to hold it.
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;
    let scratch = working_dir.join(".weaver-holds.silver.horizon.f0");
    assert!(
        scratch.exists(),
        "the paged holds must reach a scratch file in the intermediate directory"
    );
    assert!(
        !staging.join(".weaver-holds.silver.horizon.f0").exists(),
        "the holds scratch is working data and must never be written to the complete volume"
    );

    // Mid-flight: the derivation the live set is actually routing through.
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    {
        let set = pipeline
            .direct_store
            .set(job_id, 0)
            .expect("the set was admitted");
        let members = set.router.member_partials();
        assert!(!members.is_empty(), "non-vacuity: nothing routed");
        for (_, name, relative) in members {
            let partial = set.plan().destination_path(relative);
            let destination = set
                .plan()
                .member_output_path(name)
                .expect("the member resolves");
            assert!(
                partial.starts_with(&staging) && destination.starts_with(&staging),
                "{name}: both sides of the commit rename must be under the staging root"
            );
            assert!(
                !working_dir.join(relative).exists(),
                "{name}: no payload byte may be written into the intermediate directory"
            );
        }
        assert!(
            set.plan().holds_scratch_path().starts_with(&working_dir)
                && set.plan().envelope_path(0).starts_with(&working_dir),
            "the set's working files stay in the intermediate directory"
        );
    }
    assert!(
        direct_envelopes_left(&working_dir) > 0,
        "the envelopes are working data and belong beside the scratch"
    );
    assert_eq!(
        direct_envelopes_left(&staging),
        0,
        "and none of them may be written to the complete volume"
    );

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if (file_index, segment_number) == (0, 0) || (file_index, segment_number) == (0, 1) {
            continue;
        }
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Finalized"),
        "the set must finalize, got {shape}"
    );

    assert_eq!(
        std::fs::read(staging.join(member_name)).ok().as_deref(),
        Some(payload.as_slice()),
        "the committed member is in the staging root, ready to be published by rename"
    );
    assert!(
        !working_dir.join(member_name).exists(),
        "and nowhere in the intermediate directory"
    );
    assert!(
        !any_direct_partial(&working_dir),
        "no `.direct.partial` may ever have been created in the intermediate directory"
    );
}

/// A restart re-derives the same destinations, in the same staging root.
///
/// The staging root is deterministic per job id, so the "after" pipeline builds
/// it from the job id alone — before the job state exists — and has to arrive at
/// the byte-identical path the "before" pipeline wrote into. If it did not, every
/// destination claim in the checkpoint would fail its probe, the row would be
/// deleted and the set would redownload from zero: safe, silent, and a complete
/// loss of the resume.
#[tokio::test]
async fn a_restart_re_derives_its_destinations_in_the_same_staging_root() {
    const ARTICLES: usize = 2;
    let member_name = "Silver.Horizon.S01E45.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 239) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41121);
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1)];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    let partial = direct_partial(&temp_dir, job_id, member_name);
    let before = std::fs::read(&partial).expect("volume 0 routed into the member partial");
    assert!(!before.is_empty(), "non-vacuity: nothing was routed");

    let mut pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;

    // The row survived, which is only possible if the probe found the partial —
    // and the probe joins the claim onto the root this pipeline re-derived.
    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the restored job must carry its direct set");
    assert!(
        set.has_restart_seeded_coverage(),
        "the checkpoint must have been accepted, not refused on a missing destination"
    );
    let staging = payload_root(&temp_dir, job_id);
    for (_, name, relative) in set.router.member_partials() {
        assert_eq!(
            set.plan().destination_path(relative),
            partial,
            "{name}: the resumed run must re-derive the very path the previous one wrote"
        );
        assert!(
            set.plan()
                .member_output_path(name)
                .expect("the member resolves")
                .starts_with(&staging)
        );
    }
    assert_eq!(
        std::fs::read(&partial).ok(),
        Some(before),
        "and the bytes under it are the ones the checkpoint claims"
    );

    // Non-vacuity for the *skip*: a restore that re-derived a different root
    // would refuse the row and requeue every segment of the set.
    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        queued.len() < volumes.len() * ARTICLES,
        "the accepted checkpoint must let the resumed job skip what it already has, got {queued:?}"
    );

    // And it finishes: the resumed run's own writes land beside the restored
    // ones, in the same root, and the commit rename still never crosses.
    for (file_index, segment_number) in queued {
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            ARTICLES,
        )
        .await;
    }
    drain_rar_refreshes(&mut pipeline).await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Finalized"),
        "a resumed set must finish from its checkpoint, got {shape}"
    );
    assert_eq!(
        std::fs::read(staging.join(member_name)).ok().as_deref(),
        Some(payload.as_slice()),
        "and commit the member in the staging root, byte for byte"
    );
    assert!(
        !any_direct_partial(&working_dir) && !working_dir.join(member_name).exists(),
        "a resumed run leaves no payload in the intermediate directory either"
    );
}

/// With the gate off the rows are ignored, the job redownloads conventionally,
/// and the direct-store files nothing claims are swept out of the way.
#[tokio::test]
async fn a_restart_with_the_gate_off_redownloads_and_sweeps_the_orphans() {
    const ARTICLES: usize = 2;
    let member_name = "Silver.Horizon.S01E32.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 233) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41064);
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1), (1, 0)];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    let partial = direct_partial(&temp_dir, JobId(41064), member_name);
    let envelope = working_dir.join("silver.horizon.f0.vol00000.envelope");
    assert!(partial.exists() && envelope.exists(), "non-vacuity");

    let mut pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Disabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert_eq!(
        queued.len(),
        volumes.len() * ARTICLES,
        "a disabled gate must redownload the whole job, got {queued:?}"
    );
    assert!(!partial.exists(), "the orphaned partial must be swept");
    assert!(!envelope.exists(), "the orphaned envelope must be swept");
    // Rows are **ignored**, not deleted: a re-enabled binary can still judge
    // them, and it refuses them on the destination probe.
    let rows = pipeline.db.load_direct_coverage(job_id).unwrap();
    assert!(
        !rows.is_empty(),
        "a disabled gate must not destroy coverage a re-enabled one could judge"
    );
}

/// A row written against a different layout plan is refused, its files are
/// swept, and the row is deleted.
#[tokio::test]
async fn a_digest_mismatch_sweeps_the_sets_files_and_deletes_the_row() {
    const ARTICLES: usize = 2;
    let member_name = "Silver.Horizon.S01E33.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 229) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41065);
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1), (1, 0)];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    // Corrupt the row's plan digest by rewriting the blob's tail. Any decode or
    // digest failure lands on the same refusal path.
    {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
        let rows = pipeline.db.load_direct_coverage(job_id).unwrap();
        let (set_name, blob) = rows.into_iter().next().expect("a committed coverage row");
        let mut corrupted = blob;
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0xff;
        pipeline
            .db
            .save_direct_coverage(job_id, &set_name, &corrupted)
            .unwrap();
    }

    let partial = direct_partial(&temp_dir, JobId(41065), member_name);
    assert!(partial.exists(), "non-vacuity");

    let mut pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert_eq!(
        queued.len(),
        volumes.len() * ARTICLES,
        "a refused row must redownload the whole set, got {queued:?}"
    );
    assert!(
        !partial.exists(),
        "a refused set's partial must be swept before it redownloads"
    );
    assert!(
        pipeline.db.load_direct_coverage(job_id).unwrap().is_empty(),
        "a refused row is deleted"
    );
}

/// A member first seen in a **later volume** must not silently invalidate the
/// checkpoint the earlier volumes wrote.
///
/// The plan digest binds the member names and their unpacked sizes, so it
/// changes the moment a set adopts a member it had not seen — which is the
/// ordinary shape of a multi-member set, whose members are discovered in
/// whatever order their volumes arrive. A digest stamped once, at the first
/// member, describes a plan the restart no longer computes: every row written
/// afterwards is refused for a set nothing is wrong with, and the whole thing
/// redownloads.
#[tokio::test]
async fn a_member_first_seen_in_a_later_volume_still_restarts_from_its_checkpoint() {
    const ARTICLES: usize = 2;
    let episode = "Silver.Horizon.S01E40.mkv";
    let notes = "Silver.Horizon.S01E40.nfo";
    let members = vec![
        (
            episode,
            (0..2400u32).map(|index| (index % 251) as u8).collect(),
        ),
        // Sized so the split lands on a member boundary: the episode occupies
        // volumes 0 and 1 whole, and volume 2 holds nothing but the notes. The
        // notes' header therefore does not exist anywhere the barrier can see
        // until volume 2's first article arrives, long after the barrier was
        // built for the episode.
        (
            notes,
            (0..1200u32).map(|index| (index % 241) as u8).collect(),
        ),
    ];
    let volumes = multi_member_store_set(&members, 3);
    let names: Vec<&str> = members.iter().map(|(name, _)| *name).collect();

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41067);
    // Volume 0 whole, volume 1 half, and volume 2's header — the article that
    // introduces the second member.
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1), (1, 0), (2, 0)];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    // Non-vacuity: the run really did discover the second member after the first
    // barrier existed, and really did route bytes for both.
    let episode_partial = direct_partial(&temp_dir, JobId(41067), episode);
    let notes_partial = direct_partial(&temp_dir, JobId(41067), notes);
    assert!(
        episode_partial.exists() && notes_partial.exists(),
        "both members must have routed before the restart"
    );

    // The committed blob, captured before the restore can delete it, so the
    // refusal path this test exists for can be named rather than inferred from a
    // redownload.
    let committed = {
        let (pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let rows = pipeline.db.load_direct_coverage(job_id).unwrap();
        rows.into_iter()
            .next()
            .expect("the shutdown barrier must have committed a row")
            .1
    };

    let mut pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;

    // The invariant itself, stated where it can be read: the digest the row
    // carries is the digest this run computes for the same set.
    let rows = pipeline.db.load_direct_coverage(job_id).unwrap();
    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the restored job must carry its direct set");
    let judgement = crate::pipeline::direct_store::restart::restore_set(
        &crate::pipeline::direct_store::restart::DestinationRoots::for_plan(set.plan()),
        &committed,
        &set.expected_set(),
    )
    .await;
    assert!(
        judgement.is_ok(),
        "the committed row describes this very set and must be accepted, got {:?}",
        judgement.err()
    );
    let blob = rows
        .get(set.set_name())
        .expect("an accepted row is kept until the next barrier replaces it");
    let snapshot = crate::pipeline::direct_store::snapshot::decode(blob)
        .expect("the committed row must decode");
    assert_eq!(
        snapshot.plan_digest,
        set.expected_set().plan_digest,
        "a checkpoint must be stamped with the digest of the plan it was written under, \
         including members the set adopted after the barrier was built"
    );
    assert!(
        set.has_restart_seeded_coverage(),
        "the row was accepted, so its coverage came back seeded and unverified"
    );
    assert!(
        episode_partial.exists() && notes_partial.exists(),
        "an accepted row keeps its destinations; only a refused set's files are swept"
    );

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        !queued.iter().any(|(file_index, _)| *file_index == 0),
        "volume 0 was complete at the barrier; none of its articles may be refetched, got {queued:?}"
    );
    assert!(
        queued.len() < volumes.len() * ARTICLES,
        "a restart that refetches everything is not honouring any floor"
    );

    for (file_index, segment_number) in queued {
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            ARTICLES,
        )
        .await;
    }
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let complete_dir = temp_dir.path().join("complete");
    let restarted: Vec<GateMember> = names
        .iter()
        .map(|name| {
            let (bytes, location) = member_after_gate(&complete_dir, &working_dir, name);
            (name.to_string(), bytes, location)
        })
        .collect();
    let restarted_status = job_status_for_assert(&pipeline, job_id);
    assert!(
        !volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists()),
        "a restarted direct set must still never materialize a source volume"
    );
    drop(pipeline);

    let conventional = run_multi_member_gate(
        DirectStoreGate::Disabled,
        JobId(41068),
        &names,
        &volumes,
        &in_order_arrivals(volumes.len()),
    )
    .await;
    for (name, bytes, _) in &conventional.members {
        let expected = members
            .iter()
            .find(|(member, _)| member == name)
            .map(|(_, payload)| payload.as_slice());
        assert_eq!(
            bytes.as_deref(),
            expected,
            "the conventional extractor should reproduce {name}"
        );
    }
    assert_eq!(
        (restarted, restarted_status),
        (conventional.members, conventional.status),
        "a restarted multi-member direct job must finish exactly as the conventional extractor"
    );
}

/// A restart in the window after a member migration keeps its checkpoint
/// (`task_9ee23560`).
///
/// The migration moves a tolerated split BLAKE2sp-only member's bytes into the
/// envelope and **unlinks its partial**. Both halves of this pass are needed for
/// the row to survive that: the barrier has to stop claiming the file that is
/// gone, and it has to re-stamp the plan digest the member's departure changed.
/// Either one missing is a refused row and a whole-set redownload.
#[tokio::test]
async fn a_restart_after_a_member_migration_keeps_its_checkpoint() {
    const ARTICLES: usize = 2;
    let store_name = "Silver.Horizon.S01E54.mkv";
    let extra_name = "Silver.Horizon.S01E54.nfo";
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 97) as u8).collect();
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        4,
        ToleranceExtra::Blake2OnlySplit,
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41069);
    // Every article but volume 1's second. The extra's chain closes on the last
    // volume, so the migration has run; the hole in volume 1 is what keeps the
    // set mid-download, which is the whole window this test is about.
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1), (1, 0), (2, 0), (2, 1), (3, 0), (3, 1)];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    // Non-vacuity: the migration really ran, and it really did leave the routed
    // member's own destination alone.
    let extra_partial = direct_partial(&temp_dir, JobId(41069), extra_name);
    let store_partial = direct_partial(&temp_dir, JobId(41069), store_name);
    assert!(
        !extra_partial.exists(),
        "the migration must have deleted the migrated member's partial"
    );
    assert!(
        store_partial.exists(),
        "the routed member's partial must be untouched by the migration"
    );

    let committed = {
        let (pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let rows = pipeline.db.load_direct_coverage(job_id).unwrap();
        rows.into_iter()
            .next()
            .expect("the shutdown barrier must have committed a row")
            .1
    };
    let snapshot = crate::pipeline::direct_store::snapshot::decode(&committed)
        .expect("the committed row must decode");
    let claimed: Vec<&str> = snapshot
        .destinations
        .iter()
        .map(|claim| claim.relative_path.as_str())
        .collect();
    assert!(
        !claimed.contains(&format!("{extra_name}.f0.direct.partial").as_str()),
        "the checkpoint may not claim a destination the migration deleted, got {claimed:?}"
    );
    assert!(
        claimed.contains(&format!("{store_name}.f0.direct.partial").as_str()),
        "non-vacuity: the surviving member is still claimed, got {claimed:?}"
    );

    let mut pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;

    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the restored job must carry its direct set");
    let judgement = crate::pipeline::direct_store::restart::restore_set(
        &crate::pipeline::direct_store::restart::DestinationRoots::for_plan(set.plan()),
        &committed,
        &set.expected_set(),
    )
    .await;
    assert!(
        judgement.is_ok(),
        "a checkpoint written after a migration describes the set that comes back and must \
         be accepted, got {:?}",
        judgement.err()
    );
    assert!(
        store_partial.exists(),
        "an accepted row keeps its destinations; only a refused set's files are swept"
    );

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        queued.iter().all(|(file_index, _)| *file_index == 1),
        "every volume the checkpoint calls complete must stay off the queue, got {queued:?}"
    );
    // Volume 1 is the partially covered one, and it pays the documented
    // one-article rounding: the floor counts decoded bytes while the spec
    // declares yEnc-encoded ones, so the last checkpointed article of a
    // part-covered volume comes back with the one that never arrived.
    assert!(
        queued.contains(&(1, 1)),
        "the article that never arrived must be refetched, got {queued:?}"
    );

    for (file_index, segment_number) in queued {
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            ARTICLES,
        )
        .await;
    }
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let complete_dir = temp_dir.path().join("complete");
    let restarted: Vec<GateMember> = [store_name, extra_name]
        .iter()
        .map(|name| {
            let (bytes, location) = member_after_gate(&complete_dir, &working_dir, name);
            (name.to_string(), bytes, location)
        })
        .collect();
    let restarted_status = job_status_for_assert(&pipeline, job_id);
    assert!(
        !volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists()),
        "a restarted direct set must still never materialize a source volume"
    );
    drop(pipeline);

    let conventional = run_multi_member_gate(
        DirectStoreGate::Disabled,
        JobId(41070),
        &[store_name, extra_name],
        &volumes,
        &in_order_arrivals(volumes.len()),
    )
    .await;
    assert_eq!(
        conventional.members[1].1.as_deref(),
        Some(extra_payload.as_slice()),
        "the conventional extractor should reproduce the migrated member"
    );
    assert_eq!(
        (restarted, restarted_status),
        (conventional.members, conventional.status),
        "a set restarted after a migration must extract both members byte-identically to \
         the conventional extractor"
    );
}

/// Holds scratch from a killed run is swept at restore: it is append-only and
/// meaningless without the in-memory index that named its regions.
#[tokio::test]
async fn restart_sweeps_stale_holds_scratch() {
    const ARTICLES: usize = 2;
    let member_name = "Silver.Horizon.S01E34.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 227) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41066);
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1)];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    // A killed run's scratch for a set this spec no longer produces: no plan
    // names it, so the prefix rule is the only thing that can find it.
    let stale_scratch = working_dir.join(".weaver-holds.a-set-that-is-gone");
    std::fs::write(&stale_scratch, vec![0u8; 4096]).unwrap();
    let unrelated = working_dir.join("keep-me.txt");
    std::fs::write(&unrelated, b"not direct-store's").unwrap();

    // Collateral the sweep used to take with it. `.envelope` is an extension a
    // user's archive can carry, and the walk descends eight levels into a tree
    // whose shape the archive controls — so a member extracted under that name is
    // a file the sweep must leave completely alone.
    let extracted_member = working_dir.join("chapter.envelope");
    std::fs::write(&extracted_member, b"an extracted member, not an envelope").unwrap();
    let nested = working_dir.join("Season 01");
    std::fs::create_dir_all(&nested).unwrap();
    let nested_member = nested.join("notes.envelope");
    std::fs::write(&nested_member, b"also a member").unwrap();
    // …and a holds-scratch *name* below the top level is not holds scratch
    // either: the real one lives at the working-directory root by construction.
    let nested_lookalike = nested.join(".weaver-holds.not-really");
    std::fs::write(&nested_lookalike, b"still not direct-store's").unwrap();

    let _pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;

    assert!(
        !stale_scratch.exists(),
        "scratch from a killed run has no index and must be swept"
    );
    assert!(
        unrelated.exists(),
        "the sweep must only touch direct-store's own files"
    );
    for kept in [&extracted_member, &nested_member, &nested_lookalike] {
        assert!(
            kept.exists(),
            "{} is a file the archive named, not one direct-store owns; the sweep must \
             not delete it",
            kept.display()
        );
    }
}

/// A restart inside the PAR2 finalization wait — the common case now, because a
/// par2-bearing set stays byte-complete-but-uncommitted for the whole PAR2
/// download and verify.
///
/// Nothing of the set may be refetched: only the PAR2 index is still owed.
#[tokio::test]
async fn a_restart_during_the_par2_wait_refetches_nothing_of_the_set() {
    let member_name = "Silver.Horizon.S01E35.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 199) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41070);
    let (spec, index_file_index) = par2_bearing_job_spec("Silver Horizon", &volumes, &par2_bytes);

    // Phase one: every volume arrives, the PAR2 index does not. The set is
    // routed, gated and byte-complete, and finalization is waiting.
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;
        for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
            submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number)
                .await;
        }
        let set = pipeline
            .direct_store
            .set(job_id, 0)
            .expect("the set must still exist: finalization is waiting for PAR2");
        assert!(
            set.all_volumes_complete() && !set.is_finalized() && !set.is_demoted(),
            "the set must be byte-complete and unfinalized, which is the window this test is about"
        );
        // The last volume's completion already demanded a `PhaseChange` barrier,
        // so the coverage is durable without a shutdown; demanding shutdown here
        // only proves the row is there to read.
        pipeline
            .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
            .await;
        working_dir
    };

    let (mut pipeline, _, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            complete_files: HashSet::new(),
            file_progress: HashMap::new(),
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
            working_dir: working_dir.clone(),
        })
        .await
        .unwrap();

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        queued
            .iter()
            .all(|(file_index, _)| *file_index == index_file_index),
        "a byte-complete set must refetch nothing; only the PAR2 index is still owed, got {queued:?}"
    );
    assert!(
        !queued.is_empty(),
        "non-vacuity: the PAR2 index really was still outstanding"
    );

    take_queued_segment(
        &mut pipeline,
        job_id,
        SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: index_file_index,
            },
            segment_number: 0,
        },
    );
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        &par2_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;

    // The set is byte-perfect — its virtual volumes read back exactly the
    // volumes the conventional gate would have written — and nothing of it was
    // refetched. It must now also *stay* direct.
    //
    // The gate that used to break this is upstream of direct-store:
    // `clean_par2_integrity_gate` was computed from the job's **archive
    // topology**, which a direct set never enters by construction. While the
    // job is live that never showed, because live PAR2 verifies from the decode
    // buffer and short-circuits the authoritative pass — rev 9's "live PAR2 is
    // load-bearing". After a restart there is no decode buffer: no article of
    // the set arrives, live PAR2 has nothing to hash, the gate read `None`, and
    // the completion gate took its repair branch — which materializes every
    // still-routing set and redownloads a set that was already perfect. Direct
    // RAR sets now contribute `StrongDecode` themselves.
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "a byte-complete direct set must survive a par2-bearing restart rather than \
         being materialized and redownloaded, got {shape}"
    );

    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let complete_dir = temp_dir.path().join("complete");
    let (member, location) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        (member.as_deref(), location),
        (Some(payload.as_slice()), Some("complete")),
        "the restarted par2-bearing job must finish where the conventional gate does"
    );
    assert!(
        !volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists()),
        "and must still never materialize a source volume"
    );
}

/// The predicate that keeps the `StrongDecode` contribution honest: the fold is
/// job-wide and the strongest contribution wins, so a restored direct RAR set in
/// a job that also carries a conventional split archive must contribute
/// **nothing** — otherwise the split archive's authoritative PAR2 pass is skipped
/// on the strength of member CRCs that say nothing about it.
///
/// The set is restored rather than live so the *other* two predicates are
/// satisfied and `only_rar_archives` is the one thing deciding the outcome.
#[tokio::test]
async fn a_restored_direct_set_beside_a_split_archive_still_runs_the_authoritative_pass() {
    let member_name = "Silver.Horizon.S01E42.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 197) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let split_bytes: Vec<u8> = (0..1500u32).map(|index| (index % 211) as u8).collect();

    // One PAR2 index describing the RAR volumes *and* the split archive's parts,
    // so a single par2 set covers both populations of the job.
    let split_parts: Vec<(String, Vec<u8>)> = (0..2usize)
        .map(|part| {
            let chunk = split_bytes.len().div_ceil(2);
            let start = part * chunk;
            let end = ((part + 1) * chunk).min(split_bytes.len());
            (
                format!("silver.horizon.iso.{:03}", part + 1),
                split_bytes[start..end].to_vec(),
            )
        })
        .collect();
    let mut all_files = volumes.clone();
    all_files.extend(split_parts.iter().cloned());
    let described: Vec<(&str, &[u8])> = all_files
        .iter()
        .map(|(filename, bytes)| (filename.as_str(), bytes.as_slice()))
        .collect();
    let par2_bytes = build_test_par2_index_for_files(&described, PAR2_SLICE_BYTES);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41082);
    let (spec, index_file_index) = par2_bearing_job_spec("Silver Horizon", &all_files, &par2_bytes);

    // Phase one: everything but the PAR2 index arrives, then the process dies
    // inside the finalization wait — the restart the gate change is about.
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;
        for (file_index, segment_number) in in_order_arrivals(all_files.len()) {
            submit_volume_article(
                &mut pipeline,
                job_id,
                &all_files,
                file_index,
                segment_number,
            )
            .await;
        }
        pipeline
            .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
            .await;
        working_dir
    };

    let (mut pipeline, _, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            complete_files: HashSet::new(),
            file_progress: HashMap::new(),
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
            working_dir: working_dir.clone(),
        })
        .await
        .unwrap();

    // The split archive is not direct-store's, so restart owes its articles;
    // feeding them back is what puts it into the archive topology.
    let queued = peek_queued_segments(&mut pipeline, job_id);
    for (file_index, segment_number) in queued {
        if file_index == index_file_index {
            continue;
        }
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            &all_files,
            file_index,
            segment_number,
            2,
        )
        .await;
    }

    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .any(|set| set.was_restored() && !set.is_demoted()),
        "non-vacuity: the RAR set really did come back from its checkpoint, which is \
         what satisfies the gate's other two predicates"
    );
    assert!(
        pipeline.jobs.get(&job_id).is_some_and(|state| state
            .assembly
            .archive_topologies()
            .values()
            .any(|topology| topology.archive_type != crate::jobs::assembly::ArchiveType::Rar)),
        "non-vacuity: the split archive really is in the topology as a non-RAR archive, \
         so `only_rar_archives` is false and is the predicate under test"
    );

    assert!(
        !pipeline.direct_rar_contributes_strong_decode(job_id),
        "a restored direct RAR set must contribute nothing to a job that also carries a \
         conventional split archive: the fold is job-wide and the strongest contribution \
         wins, so contributing here skips the authoritative pass for the split archive — \
         whose integrity the RAR members' CRC32s say nothing about"
    );

    // The other side of the same predicate, through the same harness: the RAR
    // volumes alone — the identical set, restored the identical way — do earn the
    // contribution. Without this the assertion above passes for any reason at
    // all, including the contribution being dead.
    let rar_only_job = JobId(41083);
    let (rar_spec, _) = par2_bearing_job_spec("Silver Horizon", &volumes, &par2_bytes);
    let rar_working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
        let working_dir = insert_active_job(&mut pipeline, rar_only_job, rar_spec.clone()).await;
        for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
            submit_volume_article(
                &mut pipeline,
                rar_only_job,
                &volumes,
                file_index,
                segment_number,
            )
            .await;
        }
        pipeline
            .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
            .await;
        working_dir
    };
    let (mut rar_pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    rar_pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    rar_pipeline
        .restore_job(RestoreJobRequest {
            job_id: rar_only_job,
            job_hash: [0; 32],
            spec: rar_spec,
            complete_files: HashSet::new(),
            file_progress: HashMap::new(),
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
            working_dir: rar_working_dir,
        })
        .await
        .unwrap();
    // The contribution is only earned once the gate re-arm has re-read the
    // restored bytes — that is the third predicate — and the re-arm runs at the
    // download/verify boundary, which for a byte-complete set is here.
    assert!(
        rar_pipeline
            .direct_store
            .set(rar_only_job, 0)
            .is_some_and(|set| set.was_restored() && set.has_restart_seeded_coverage()),
        "non-vacuity: the set came back seeded and unverified, which is the state the \
         third predicate refuses"
    );
    assert!(
        !rar_pipeline.direct_rar_contributes_strong_decode(rar_only_job),
        "a set still carrying unverified restart-seeded bytes has decoded nothing this \
         run and must not claim decode strength"
    );
    rar_pipeline.finalize_ready_direct_sets(rar_only_job).await;
    assert!(
        rar_pipeline.direct_rar_contributes_strong_decode(rar_only_job),
        "non-vacuity: the same restored set in an all-RAR job does earn the contribution \
         once its gates are re-armed, so the refusal above is the split archive's doing \
         and not a dead predicate"
    );
}

/// The last holds failure mode: the scratch file cannot be opened at all.
#[tokio::test]
async fn a_scratch_io_failure_demotes_the_set() {
    let member_name = "Silver.Horizon.S01E36.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 149) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(64);
    let job_id = JobId(41072);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // A directory where the scratch file belongs: every open of it fails, on
    // every platform, without needing permission games.
    std::fs::create_dir(working_dir.join(".weaver-holds.silver.horizon.f0")).unwrap();

    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(HoldsScratchFailed)"),
        "a scratch that cannot be written must demote with its own reason, got {shape}"
    );
}

/// The pause demand, driven through the command seam it is wired at.
#[tokio::test]
async fn pausing_a_job_with_dirty_direct_coverage_demands_a_barrier() {
    let member_name = "Silver.Horizon.S01E37.mkv";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 191) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(41073);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in [(0u32, 0u32), (0, 1)] {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }

    let generation = |pipeline: &Pipeline| -> u64 {
        pipeline
            .db
            .load_direct_coverage(job_id)
            .unwrap()
            .values()
            .next()
            .map(|blob| {
                crate::pipeline::direct_store::snapshot::decode(blob)
                    .unwrap()
                    .generation
            })
            .unwrap_or(0)
    };
    // Neither the byte threshold nor the 5 s timer has fired: the coverage is
    // dirty and uncheckpointed, which is exactly the state a pause must not
    // leave behind.
    let before = generation(&pipeline);

    let (reply, _rx) = tokio::sync::oneshot::channel();
    let command = SchedulerCommand::PauseJob { job_id, reply };
    let scope = Pipeline::pause_barrier_scope(&command)
        .expect("a pause command must be classified as a barrier demand");
    pipeline.demand_direct_store_barriers_for_pause(scope).await;

    let after = generation(&pipeline);
    assert!(
        after > before,
        "pausing a job with dirty direct coverage must advance the row's generation \
         ({before} -> {after})"
    );

    // And a command that is not a pause raises no demand at all.
    let (reply, _rx) = tokio::sync::oneshot::channel();
    assert!(
        Pipeline::pause_barrier_scope(&SchedulerCommand::ResumeJob { job_id, reply }).is_none(),
        "only pause commands demand a barrier"
    );
}

/// The checkpoint's per-volume `complete` bit means *all bytes durable*, and
/// restart skips every segment of the file on the strength of it.
///
/// A volume can finish downloading while every one of its bytes is still held —
/// here a middle volume arrives whole before the volume whose chain would let
/// the layout place it — and a bit latched at the article-complete seam
/// checkpoints `{floor: header prefix only, complete: true}`. Restart then skips
/// every segment of a volume whose payload does not exist, and the set can
/// neither finalize (its member gate has nothing to compose) nor demote (its
/// reconstruction has nothing to read): a permanent zombie.
#[tokio::test]
async fn a_volume_completing_into_held_bytes_is_not_checkpointed_complete() {
    const ARTICLES: usize = 2;
    const HELD: u32 = 1;
    let member_name = "Silver.Horizon.S01E40.mkv";
    let payload: Vec<u8> = (0..6000u32).map(|index| (index % 239) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41080);
    // The middle volume arrives whole while the volume that starts the member's
    // chain is entirely absent, so the layout cannot place a byte of it.
    let arrivals: Vec<(u32, u32)> = vec![(HELD, 0), (HELD, 1)];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    // What the row actually says about that volume, read straight out of the
    // database the restart is about to consult.
    {
        let (probe, _, _) = new_direct_pipeline(&temp_dir).await;
        let snapshot = coverage_snapshot_of(&probe, job_id);
        let entry = snapshot
            .floors
            .iter()
            .find(|entry| entry.file_index == HELD)
            .expect("the completed volume must appear in the checkpoint");
        // Only the volume's own header prefix could be classified — its payload
        // belongs to a member chain the layout cannot place without the volume
        // before it — so the floor stops far short of the volume.
        assert!(
            entry.floor > 0 && entry.floor < volumes[HELD as usize].1.len() as u64,
            "non-vacuity: this volume's payload really is all held, so its floor covers \
             only the header prefix (floor {} of {} bytes)",
            entry.floor,
            volumes[HELD as usize].1.len()
        );
        assert!(
            !entry.complete,
            "a volume whose floor covers none of its payload must not be checkpointed \
             complete: restart would skip every segment of a volume whose bytes do not \
             exist ({entry:?})"
        );
    }

    let mut pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        (0..ARTICLES as u32).all(|segment| queued.contains(&(HELD, segment))),
        "every segment of a volume whose bytes were held must come back, got {queued:?}"
    );
    assert_eq!(
        queued.len(),
        volumes.len() * ARTICLES,
        "only a header prefix was durable, so the whole job is owed, got {queued:?}"
    );

    for (file_index, segment_number) in queued {
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            ARTICLES,
        )
        .await;
    }
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let complete_dir = temp_dir.path().join("complete");
    let (member, _) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        member.as_deref(),
        Some(payload.as_slice()),
        "the job must finish rather than wedge on a volume it was told it already had"
    );
}

/// A restart mid-download of a set's **last** volume.
///
/// The one shape the structural proof cannot reach: the cached facts stopped
/// short of the end-of-archive record, and the volume *closes* the member
/// chain, so `split_after` says nothing about whether a second member's header
/// sits past the first's data area. An earlier shape demoted here by design.
///
/// The expensive arm does the only thing that actually answers the question: it
/// rebuilds the walk's reader out of the volume's **envelope**, which holds every
/// non-member byte at its true physical offset and is therefore exactly the
/// header region, and re-parses. The set finishes one-pass and byte-identically
/// instead of paying a materialization.
#[tokio::test]
async fn a_restored_last_volume_reconfirms_from_its_envelope_and_finalizes() {
    const ARTICLES: usize = 4;
    let member_name = "Silver.Horizon.S01E41.mkv";
    let payload: Vec<u8> = (0..8000u32).map(|index| (index % 229) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41081);
    // Restart lands mid-download of the **last** volume: its end-of-archive
    // record has not arrived, so the cached facts stop short of it.
    let arrivals: Vec<(u32, u32)> = vec![
        (0, 0),
        (0, 1),
        (0, 2),
        (0, 3),
        (1, 0),
        (1, 1),
        (1, 2),
        (1, 3),
        (2, 0),
        (2, 1),
    ];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    let mut pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_some_and(|set| !set.is_demoted()),
        "non-vacuity: the set came back from its checkpoint still routing"
    );

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        queued.iter().any(|(file_index, _)| *file_index == 2),
        "non-vacuity: the last volume really was still owed articles, got {queued:?}"
    );
    for (file_index, segment_number) in queued {
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            ARTICLES,
        )
        .await;
    }
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "the re-parse must confirm the restored last volume rather than demote it, got {shape}"
    );
    // One pass: a demotion is what would have written these.
    for (filename, _) in &volumes {
        assert!(
            !working_dir.join(filename).exists(),
            "{filename} must never reach disk for a set that stayed direct"
        );
    }
    let complete_dir = temp_dir.path().join("complete");
    let (member, _) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        member.as_deref(),
        Some(payload.as_slice()),
        "the reconfirmed set must commit its member byte for byte"
    );
}

/// The other half of the same seam: the re-parse is a *proof*, not a permission.
///
/// With the restored volume's envelope gone, the walk's reader has a hole where
/// the headers were — nothing can prove the tail holds no undiscovered member —
/// and the volume must demote under its own name exactly as it always has. What
/// must not happen is confirming on the strength of "every article arrived",
/// which would file an unproven region into an envelope that finalization
/// deletes.
#[tokio::test]
async fn a_restored_last_volume_whose_envelope_is_gone_still_demotes_by_name() {
    const ARTICLES: usize = 4;
    let member_name = "Silver.Horizon.S01E42.mkv";
    let payload: Vec<u8> = (0..8000u32).map(|index| (index % 229) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41082);
    let arrivals: Vec<(u32, u32)> = vec![
        (0, 0),
        (0, 1),
        (0, 2),
        (0, 3),
        (1, 0),
        (1, 1),
        (1, 2),
        (1, 3),
        (2, 0),
        (2, 1),
    ];
    let working_dir =
        direct_store_before_restart(&temp_dir, job_id, &volumes, &arrivals, ARTICLES).await;

    let mut pipeline = direct_store_after_restart(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
    )
    .await;
    // The last volume's envelope, gone from under an already-validated
    // checkpoint. Deleted *after* the restore so the row itself is untouched and
    // this isolates the re-parse: the remaining articles recreate the file, but
    // only with the tail bytes they carry, so the header region the walk has to
    // read is a hole.
    let envelope = working_dir.join("silver.horizon.f0.vol00002.envelope");
    assert!(
        envelope.exists(),
        "non-vacuity: the volume must have had an envelope to lose"
    );
    crate::pipeline::release_cached_write_handle(&envelope);
    std::fs::remove_file(&envelope).unwrap();

    for (file_index, segment_number) in peek_queued_segments(&mut pipeline, job_id) {
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            ARTICLES,
        )
        .await;
    }

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(UnconfirmedRestoredVolume)"),
        "a restored volume the re-parse cannot run over must demote under its own \
         reason rather than hold its tail, got {shape}"
    );
    // Never wedged. The demotion is a *transition*, not a dead end: the set's
    // routed output is gone and the conventional path owns the volumes now,
    // either as materialized files or as work back on the queue. What must not
    // happen — and what holding the tail produced — is a set that stays direct,
    // finalizes nothing and refetches nothing.
    let materialized = volumes
        .iter()
        .filter(|(filename, _)| working_dir.join(filename).exists())
        .count();
    let requeued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        materialized > 0 || !requeued.is_empty(),
        "a demoted set must hand its volumes to the conventional path, either \
         materialized or refetched (materialized {materialized}, requeued {requeued:?})"
    );
    assert!(
        requeued.contains(&(2, 0))
            && requeued.contains(&(2, 3))
            && !requeued.contains(&(2, 1))
            && !requeued.contains(&(2, 2)),
        "only the restored volume's unmaterialized edge articles should return to conventional ownership, got {requeued:?}"
    );
    for (file_index, (filename, bytes)) in volumes.iter().enumerate() {
        let Some(on_disk) = std::fs::read(working_dir.join(filename)).ok() else {
            continue;
        };
        assert!(
            on_disk.len() <= bytes.len(),
            "{filename} must not grow past its canonical geometry"
        );
        for segment_number in 0..ARTICLES as u32 {
            if requeued.contains(&(file_index as u32, segment_number)) {
                continue;
            }
            let (start, end) = article_extent(bytes.len(), segment_number, ARTICLES);
            assert!(
                end <= on_disk.len(),
                "{filename} segment {segment_number} stayed materialized, so its full extent must exist"
            );
            assert_eq!(
                &on_disk[start..end],
                &bytes[start..end],
                "{filename} segment {segment_number} stayed materialized, so its bytes must be exact"
            );
        }
    }
    let complete_dir = temp_dir.path().join("complete");
    let (member, _) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        member, None,
        "an unconfirmed volume's set must not commit a member it could not prove"
    );
}

// ---------------------------------------------------------------------------
// Repair while still direct
// ---------------------------------------------------------------------------

#[tokio::test]
async fn par2_damage_in_the_envelope_repairs_while_the_set_stays_direct() {
    // The first transition, end to end. The damaged byte is in a recovery
    // record's data area: outside every member's packed range, so neither the
    // per-part packed CRC32 nor the whole-member CRC32 covers it, and inside a
    // service block's data rather than a header, so the walk still parses and
    // the volume still confirms. PAR2 is the only layer that can see it — which
    // is exactly the set-up an earlier shape could only demote on.
    let member_name = "Silver.Horizon.S01E21.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 211) as u8).collect();
    let rr_bytes = 512;
    let clean = recovery_record_store_set(member_name, &payload, 3, rr_bytes);
    // The PAR2 set describes the *clean* volumes and carries enough recovery to
    // rebuild the damaged slice; the job downloads a damaged volume.
    let par2_bytes = repairable_par2_index(&clean, 4);
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, rr_bytes);

    let conventional = run_repairable_par2_gate(
        DirectStoreGate::Disabled,
        JobId(41061),
        member_name,
        &volumes,
        &par2_bytes,
    )
    .await;
    let direct = run_repairable_par2_gate(
        DirectStoreGate::Enabled,
        JobId(41062),
        member_name,
        &volumes,
        &par2_bytes,
    )
    .await;

    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the gate-off reference must repair the volume and extract the member; \
         status={:?}",
        conventional.status
    );
    assert_eq!(
        direct.member.as_deref(),
        conventional.member.as_deref(),
        "a direct set must repair in place and produce the same bytes the \
         gate-off gate produces after its repair; sets = {}",
        direct.sets
    );
    assert!(
        !direct.sets.contains("Demoted"),
        "the set must stay direct through the repair — that is the whole \
         transition, and demoting instead costs a materialization of every \
         volume; got {}",
        direct.sets
    );
    assert_eq!(
        direct.finalized, 1,
        "and it must finalize once the re-verify clears it — committing its own \
         partials, which is the only way a member reaches its destination \
         without a volume file ever existing; sets = {}",
        direct.sets
    );
    assert!(
        !direct.volume_file_seen,
        "no volume may materialize under its own name: that path belongs to \
         demotion, and a half-repaired file sitting there would be read as a \
         downloaded volume by every conventional path"
    );
    assert_eq!(
        direct.materialized, 1,
        "exactly the one damaged volume materializes. The other two are read as \
         repair *sources* through the hybrid provider, which is what makes the \
         expansion set empty by construction rather than merely small"
    );
    assert_eq!(
        direct.repair_scratch_left, 0,
        "the repair scratch is deleted once its spans are routed, so the set \
         returns to fully virtual"
    );
    assert!(
        matches!(direct.status, Some(JobStatus::Complete)),
        "the job must complete, got {:?} with sets {}",
        direct.status,
        direct.sets
    );
}

#[tokio::test]
async fn a_wholly_absent_volume_is_repaired_in_place_and_the_set_stays_direct() {
    // The bench shape: every article of one middle volume answered 430, so the
    // set holds no byte of it — no envelope content, no covered run, nothing to
    // materialize from. The repair target therefore has to be *created* at the
    // length PAR2 describes and written slice by slice, and the volume that
    // comes back has never been parsed before, so the re-route has to stage it,
    // parse it and confirm it from its own repaired image.
    //
    // Before the fix the sweep left no file at all and par2's first write
    // failed with `No such file or directory` at offset 0, which demoted the
    // whole set and reconstructed four healthy volumes to repair the fifth
    // conventionally.
    let member_name = "Silver.Horizon.S01E30.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 211) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 5);
    // Enough recovery to rebuild an entire volume from nothing.
    let par2_bytes = repairable_par2_index(&volumes, 12);

    let direct = run_repairable_par2_gate_inner(
        DirectStoreGate::Enabled,
        JobId(41220),
        member_name,
        &volumes,
        &par2_bytes,
        IndexPosition::Last,
        None,
        2,
        Some(2),
    )
    .await;

    assert!(
        !direct.sets.contains("Demoted"),
        "a volume nobody posted is repairable in place; the set must not demote, \
         got {}",
        direct.sets
    );
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "the member must verify against its own whole-member CRC32 over bytes \
         the repair supplied; sets = {}",
        direct.sets
    );
    assert_eq!(
        direct.finalized, 1,
        "and the set must commit its own partials rather than hand the archive \
         to the conventional path; sets = {}",
        direct.sets
    );
    assert_eq!(
        direct.materialized, 1,
        "exactly the absent volume becomes a repair target; the four that \
         arrived are read virtually as repair sources"
    );
    assert!(
        !direct.volume_file_seen,
        "no source volume may appear under its own name: the repair target is \
         scratch, and a file at the volume's name is what demotion produces"
    );
    assert_eq!(
        direct.repair_scratch_left, 0,
        "the scratch target is deleted once its spans are routed"
    );
    assert!(
        matches!(direct.status, Some(JobStatus::Complete)),
        "the job must complete, got {:?} with sets {}",
        direct.status,
        direct.sets
    );
}

#[tokio::test]
async fn an_absent_closing_volume_is_confirmed_from_its_own_repaired_image() {
    // The absent volume's harder half. A middle volume is confirmed by the
    // *next* volume's header facts saying more volumes follow; the closing one
    // has nothing after it, so its classification frontier can only be closed
    // by the volume itself — and the set holds no byte of it.
    //
    // What licenses closing it here is that the repair rewrote the volume end
    // to end: the staged image is the whole volume, so the parse walking it is
    // authoritative. A partial rewrite is not licensed and does not get the
    // shortcut.
    let member_name = "Silver.Horizon.S01E31.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 211) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 5);
    let par2_bytes = repairable_par2_index(&volumes, 12);

    let direct = run_repairable_par2_gate_inner(
        DirectStoreGate::Enabled,
        JobId(41221),
        member_name,
        &volumes,
        &par2_bytes,
        IndexPosition::Last,
        None,
        2,
        Some(volumes.len() as u32 - 1),
    )
    .await;

    assert!(
        !direct.sets.contains("Demoted"),
        "the closing volume's repaired image must confirm the set rather than \
         leave its spans held, got {}",
        direct.sets
    );
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "the member's tail comes from the repaired closing volume; sets = {}",
        direct.sets
    );
    assert_eq!(
        direct.finalized, 1,
        "and the set must finalize on it; sets = {}",
        direct.sets
    );
    assert!(
        !direct.volume_file_seen,
        "no source volume may appear under its own name"
    );
}

#[tokio::test]
async fn a_part_checksum_mismatch_waits_for_par2_instead_of_demoting() {
    // 128 bytes of a volume's member payload flipped under a yEnc CRC that
    // agrees. RAR's own packed CRC32 for that part catches it the moment the
    // part completes — seconds into the download — and the set used to demote
    // there and then, reconstructing every other volume so a conventional
    // repairer could work over real files.
    //
    // The mismatch is a *fact* now, not a verdict: the set stays direct, the
    // volume is read authoritatively by the PAR2 pass rather than stood in for
    // on the wire evidence that just lied, and the repair puts it right in
    // place.
    let member_name = "Silver.Horizon.S01E32.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 211) as u8).collect();
    let clean = single_member_store_set(member_name, &payload, 5);
    // The PAR2 describes the clean volumes; the job downloads a corrupted one.
    let par2_bytes = repairable_par2_index(&clean, 12);
    let mut volumes = clean.clone();
    damage_member_payload(&mut volumes, 2, 128);

    let direct = run_repairable_par2_gate(
        DirectStoreGate::Enabled,
        JobId(41230),
        member_name,
        &volumes,
        &par2_bytes,
    )
    .await;

    assert!(
        !direct.sets.contains("Demoted"),
        "a part-checksum mismatch with recovery behind it must not demote, got {}",
        direct.sets
    );
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "the member must verify against its whole-member CRC32 after the repair; \
         sets = {}",
        direct.sets
    );
    assert_eq!(
        direct.finalized, 1,
        "and the set must commit its own partials; sets = {}",
        direct.sets
    );
    assert_eq!(
        direct.materialized, 1,
        "only the suspect volume becomes a repair target"
    );
    assert!(
        !direct.volume_file_seen,
        "no source volume may appear under its own name"
    );
    // The authoritative read, stated as a count: the pre-repair pass stands in
    // for no file at all, because the only evidence that could have claimed one
    // is the wire evidence the mismatch contradicted.
    let first_pass = direct
        .verify_read_splits
        .first()
        .copied()
        .expect("the pre-repair pass ran");
    assert_eq!(
        first_pass.0, 0,
        "no volume may be stood in for while a part-checksum mismatch is on \
         record; splits = {:?}",
        direct.verify_read_splits
    );
    assert!(
        matches!(direct.status, Some(JobStatus::Complete)),
        "the job must complete, got {:?} with sets {}",
        direct.status,
        direct.sets
    );
}

#[tokio::test]
async fn a_part_checksum_mismatch_with_no_recovery_demotes_as_it_always_did() {
    // The other side of the same fact. Nothing in this job's NZB carries a
    // recovery block, so there is no answer coming and waiting for one would
    // park the set forever. The demotion is immediate and under its own reason,
    // which is what the `part_checksum_mismatch` metric has always counted.
    let member_name = "Silver.Horizon.S01E33.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 211) as u8).collect();
    let mut volumes = single_member_store_set(member_name, &payload, 5);
    damage_member_payload(&mut volumes, 2, 128);

    let shape = tolerance_shape(JobId(41231), &volumes).await;
    assert!(
        shape.contains("Demoted(PartChecksumMismatch)"),
        "with no recovery to answer it, a part-checksum mismatch demotes exactly \
         as before, got {shape}"
    );
}

#[tokio::test]
async fn a_repair_reads_its_sources_by_file_index_when_the_par2_index_leads_the_nzb() {
    // The index-space regression, and the reason every other repair fixture is
    // blind to it. A repair materializes its damaged volumes through the hybrid
    // provider, and that provider is keyed by **job file index** so one
    // instance can answer for every set of a job. The reconstruction plan was
    // built with the set's own **volume index**. The two are the same number
    // exactly when a set's volumes are NZB files `0..n-1` — true whenever the
    // PAR2 is appended last, which is every fixture and most real NZBs, and
    // false the moment a `.par2` or `.nfo` leads or a job carries two sets.
    //
    // When they differ the sweep reads a *different volume's* bytes: volume 1
    // asks for file index 1 and is handed volume 0, fails its composed CRC32,
    // and the whole set demotes with `materialization_failed` as the only signal
    // that anything went wrong. Volume 0 asks for file index 0 — the PAR2 index
    // — and finds nothing at all.
    //
    // Same fixture and same damage as
    // `par2_damage_in_the_envelope_repairs_while_the_set_stays_direct`, with the
    // index moved to NZB position 0 and nothing else changed.
    let member_name = "Silver.Horizon.S01E26.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 211) as u8).collect();
    let rr_bytes = 512;
    let clean = recovery_record_store_set(member_name, &payload, 3, rr_bytes);
    let par2_bytes = repairable_par2_index(&clean, 4);
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, rr_bytes);

    let direct = run_repairable_par2_gate_at(
        DirectStoreGate::Enabled,
        JobId(41065),
        member_name,
        &volumes,
        &par2_bytes,
        IndexPosition::First,
        None,
    )
    .await;

    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "the repair must produce the member byte for byte, whatever number the \
         NZB happens to give the set's volumes; sets = {}",
        direct.sets
    );
    assert!(
        !direct.sets.contains("Demoted"),
        "and the set must stay direct: reading the wrong volume's bytes fails a \
         composed CRC32 and demotes the whole set, which is the failure this \
         asserts against; got {}",
        direct.sets
    );
    assert_eq!(
        direct.materialized, 1,
        "still exactly the one damaged volume; sets = {}",
        direct.sets
    );
    assert!(
        !direct.volume_file_seen,
        "no volume may materialize under its own name"
    );
    assert_eq!(direct.repair_scratch_left, 0);
    assert!(
        matches!(direct.status, Some(JobStatus::Complete)),
        "the job must complete, got {:?} with sets {}",
        direct.status,
        direct.sets
    );
}

#[tokio::test]
async fn an_unrepairable_direct_set_still_demotes_whole() {
    // The fallback, unchanged: with no recovery blocks the damage cannot be
    // repaired, so repair refuses before it materializes anything and the
    // whole-set demotion answers instead. Same fixture, same damage, one
    // difference — the recovery stream.
    let member_name = "Silver.Horizon.S01E22.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 197) as u8).collect();
    let rr_bytes = 512;
    let clean = recovery_record_store_set(member_name, &payload, 3, rr_bytes);
    let par2_bytes = repairable_par2_index(&clean, 0);
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, rr_bytes);

    let direct = run_repairable_par2_gate(
        DirectStoreGate::Enabled,
        JobId(41063),
        member_name,
        &volumes,
        &par2_bytes,
    )
    .await;

    assert!(
        direct.sets.contains("Demoted(Par2Damaged)"),
        "an unrepairable direct set must demote whole, exactly as it did before \
         phase 6, got {}",
        direct.sets
    );
    assert!(
        direct.volume_file_seen,
        "and the demotion must materialize its volumes for the conventional path"
    );
    assert_eq!(
        direct.repair_scratch_left, 0,
        "a refused repair leaves no scratch behind"
    );
    // The damage sits entirely in volume 1's RAR recovery record; the member's
    // own data slices are untouched. Once the demotion hands its materialized
    // volumes back to the conventional completion seam, extraction reads the
    // intact data and delivers the member byte for byte — the damaged
    // recovery record is archive residue the job never needed. This test once
    // asserted the job must NOT complete, but that pinned an accident: the
    // demoted volumes never entered the archive topology at all, so
    // extraction never had the chance to prove the data was fine. Delivering
    // provably intact content is the outcome the whole pipeline exists for.
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "the member's data slices are undamaged, so the conventional path must \
         deliver it byte for byte; sets = {}",
        direct.sets
    );
    assert!(
        matches!(direct.status, Some(JobStatus::Complete)),
        "with the member delivered intact, the job completes; the unrepairable \
         damage lived only in a recovery record nothing needed, got {:?}",
        direct.status
    );
}

#[tokio::test]
async fn a_rewrite_over_the_holds_budget_demotes_before_it_materializes_anything() {
    // Every repaired byte re-enters the router as a hold, so a rewrite is
    // charged against the same RAM ceiling ordinary staging is — and the first
    // shape never checked: it read every rewrite span of every damaged volume
    // whole, then let the router copy them into staging, so a missing-volume
    // repair of a large set peaked at about twice the repaired bytes with
    // nothing bounding either term.
    //
    // The A/B is the budget and nothing else. A later pass revisits the bound
    // itself — routing a repaired volume in budget-sized instalments lifts it —
    // but until then an over-budget rewrite demotes, and it must do so before
    // the checkpoint delete, so finding out costs the set nothing.
    let member_name = "Silver.Horizon.S01E28.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 211) as u8).collect();
    let (volumes, par2_bytes) = repairable_envelope_damage(member_name, &payload);

    let inside_dir = tempfile::tempdir().unwrap();
    let (inside, _) =
        live_damaged_direct_job(&inside_dir, JobId(41101), &volumes, &par2_bytes, None).await;
    let inside_sets = format!("{:?}", inside.direct_store.sets_for(JobId(41101)));
    assert_eq!(
        inside.direct_store.repair_materialized_volumes, 1,
        "non-vacuity: inside the default budget this fixture repairs while \
         direct, so the only thing the run below changes is the ceiling; \
         sets = {inside_sets}"
    );
    assert!(
        !inside_sets.contains("Demoted"),
        "and it stays direct doing it; got {inside_sets}"
    );

    let over_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41102);
    // One byte: under every rewrite, and small enough to be unambiguous. Routing
    // itself survives it — holds page out to scratch rather than demoting.
    let (over, working_dir) =
        live_damaged_direct_job(&over_dir, job_id, &volumes, &par2_bytes, Some(1)).await;

    let sets = format!("{:?}", over.direct_store.sets_for(job_id));
    assert_eq!(
        over.direct_store.repair_materialized_volumes, 0,
        "an over-budget repair must refuse before the materialization, not after \
         reading the bytes it exists to avoid reading; sets = {sets}"
    );
    assert_eq!(
        over.direct_store.repair_attempts, 0,
        "and before the checkpoint delete, so it does not spend the set's one \
         repair attempt on a refusal that cost it nothing; sets = {sets}"
    );
    assert!(
        sets.contains("Demoted"),
        "the set then takes the whole-set demotion, which is always correct; \
         got {sets}"
    );
    assert_eq!(
        direct_scratch_left(&working_dir),
        0,
        "no repair scratch may exist: none was ever opened"
    );
}

#[tokio::test]
async fn a_second_damage_verdict_after_a_repair_demotes_instead_of_repairing_again() {
    // Nothing else terminates the loop. A set that is damaged again after a
    // completed repair produces the same verdict on every completion check, and
    // without a bound each one materializes, repairs, re-routes and re-verifies
    // — forever. One attempt, then the whole-set demotion.
    let member_name = "Silver.Horizon.S01E29.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 191) as u8).collect();
    let (volumes, par2_bytes) = repairable_envelope_damage(member_name, &payload);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41103);
    let (mut pipeline, working_dir) =
        live_damaged_direct_job(&temp_dir, job_id, &volumes, &par2_bytes, None).await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert_eq!(
        (
            pipeline.direct_store.repair_attempts,
            pipeline.direct_store.repair_materialized_volumes
        ),
        (1, 1),
        "non-vacuity: the set must have had its one real repair before the \
         second verdict can be a repeat; sets = {sets}"
    );
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .any(|set| !set.is_demoted() && !set.is_finalized() && set.repair_attempted()),
        "and it must still be live with its latch burned; got {sets}"
    );

    // Fresh damage under the repaired set, in the member partial the virtual
    // volume reads its member bytes back out of. This is the shape the bound
    // exists for: a repair that did not leave the set verifiable, however it got
    // there.
    //
    // Placed inside volume 1's byte range (`chunk` is 800 bytes here, so
    // [800, 1600) is volume 1 — the one `repairable_envelope_damage` already
    // damaged and repaired), not at a fixed low offset. The post-repair pass
    // is selective now: it reads back only the volumes the repair rewrote and
    // carries every other volume's pre-repair verdict forward unread — the
    // same trust class the conventional selective pass already accepts for
    // bytes that move outside its own write set. Damage anywhere else in the
    // set would not be caught by this pass and would prove nothing about the
    // bound this test exists to pin.
    let partial = std::fs::read_dir(payload_root(&temp_dir, JobId(41103)))
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().ends_with(".direct.partial"))
        })
        .expect("the live set still holds its member partial");
    let mut bytes = std::fs::read(&partial).unwrap();
    bytes[810] ^= 0xFF;
    std::fs::write(&partial, &bytes).unwrap();

    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");
    let mut first_pending_work = None;
    let resolution = loop {
        let resolution = pipeline
            .resolve_direct_sets_before_par2_repairer(
                job_id,
                Arc::clone(&par2_set),
                working_dir.clone(),
            )
            .await;
        if !matches!(
            resolution,
            crate::pipeline::direct_store::wiring::DirectPar2Resolution::Pending
        ) {
            break resolution;
        }
        if first_pending_work.is_none() {
            let pending = pipeline
                .direct_post_repair_in_flight
                .get(&job_id)
                .expect("the first check must leave one post-repair ticket in flight");
            first_pending_work = Some((pending.work_id, pending.recovery_set_id));

            let duplicate = pipeline
                .resolve_direct_sets_before_par2_repairer(
                    job_id,
                    Arc::clone(&par2_set),
                    working_dir.clone(),
                )
                .await;
            assert!(
                matches!(
                    duplicate,
                    crate::pipeline::direct_store::wiring::DirectPar2Resolution::Pending
                ),
                "a repeated completion check must reuse the in-flight ticket; got {duplicate:?}"
            );
            assert_eq!(
                pipeline
                    .direct_post_repair_in_flight
                    .get(&job_id)
                    .map(|work| (work.work_id, work.recovery_set_id)),
                first_pending_work,
                "the repeated check must not replace or duplicate the ticket"
            );
            pipeline.remove_pending_completion_check(job_id);
            pipeline.schedule_job_completion_check_if_download_pipeline_drained(
                job_id,
                "post-repair ticket test",
            );
            assert!(
                !pipeline.pending_completion_checks.contains(&job_id),
                "normal drain ticks must not poll an in-flight post-repair ticket"
            );

            let (work_id, recovery_set_id) = first_pending_work.unwrap();
            pipeline.handle_direct_post_repair_done(crate::pipeline::DirectPostRepairWorkDone {
                job_id,
                work_id: work_id.wrapping_add(1),
                recovery_set_id,
                result: Err("stale verdict".to_string()),
            });
            assert!(
                !pipeline.direct_post_repair_results.contains_key(&job_id),
                "a stale ticket must not publish a verdict"
            );
        }
        let done = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            pipeline.direct_post_repair_done_rx.recv(),
        )
        .await
        .expect("the post-repair read-back should finish")
        .expect("the post-repair completion channel stays open");
        pipeline.handle_direct_post_repair_done(done);
    };

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        matches!(
            resolution,
            crate::pipeline::direct_store::wiring::DirectPar2Resolution::Unresolved
        ),
        "a set that has had its attempt must fall through to the demotion rather \
         than report a repair; got {resolution:?}; sets = {sets}"
    );
    assert_eq!(
        pipeline.direct_store.repair_attempts, 1,
        "and no second attempt may be made at all — that is the loop, one lap of \
         it. Counted rather than inferred: an attempt that refuses downstream \
         leaves the same traces as one that was never made; sets = {sets}"
    );
    assert_eq!(
        direct_scratch_left(&working_dir),
        0,
        "no scratch, for the same reason"
    );
}
