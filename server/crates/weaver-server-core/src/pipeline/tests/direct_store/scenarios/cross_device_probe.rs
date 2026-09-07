//! Cross-device probe

use super::*;

// ---------------------------------------------------------------------------
// Cross-device probe
//
// Every other test in this file puts the intermediate and complete directories
// under one `TempDir`, so they share a filesystem and `rename(2)` between them
// always succeeds. That hides the exact failure this subsystem cares about: on
// the ordinary deployment — intermediate on local disk, complete on a NAS — a
// publish rename across the two returns `EXDEV` and completion falls back to
// copying every byte a second time.
//
// These probes run only when `WEAVER_XDEV_INTERMEDIATE` and
// `WEAVER_XDEV_COMPLETE` name directories on two different filesystems, which is
// what the container harness provides (two `--tmpfs` mounts). They print a
// machine-readable evidence block — device and inode numbers at each stage — and
// assert the verdict named by `WEAVER_XDEV_EXPECT` (`rename` or `copy`), so the
// same source can be run against a tree that writes payload to the intermediate
// filesystem and one that writes it to the complete filesystem, and each is held
// to what it actually guarantees.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_uuencoded_article_demotes_the_direct_set_that_claims_its_volume() {
    // Sets are admitted from the NZB's filenames, before a single article has
    // been decoded, so an archive posted in uuencode is admitted exactly like a
    // yEnc one — and can never be routed, because a uuencode article declares
    // no offset to route on. Excluding those articles quietly is not enough: a
    // starved set neither finalizes nor demotes, its volumes keep answering
    // `is_direct_source_file`, and that suppresses the archive probe which
    // dispatches extraction. The job would complete with its archive sitting
    // unextracted on disk.
    let member_name = "Silver.Horizon.S01E17.mkv";
    let payload: Vec<u8> = (0..1600u32).map(|index| (index % 251) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(41099);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    insert_active_job(&mut pipeline, job_id, spec).await;

    // Admission is lazy, so the very first article of the job is uuencode and
    // the set has not been built yet — which is exactly the shape that must
    // still end with a demoted set rather than one admitted moments later.
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    super::decode_and_files::submit_uu_segment_named(
        &mut pipeline,
        file_id,
        0,
        &volumes[0].1[..64],
        false,
        false,
        member_name,
    )
    .await;

    assert!(
        !pipeline.direct_store.sets_for(job_id).is_empty(),
        "the set was admitted"
    );
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .all(|set| set.is_demoted()),
        "a uuencode article must take the set off the direct path"
    );
    assert!(
        !pipeline.is_direct_source_file(file_id),
        "the volume is back on the conventional path, so extraction can be dispatched for it"
    );
}

#[tokio::test]
async fn the_post_direct_repair_pass_reads_only_what_the_repair_rewrote() {
    // Before a repair, the quiet direct pass stands in for every volume the
    // dual-CRC grid adjudicated and reads only the rest — that is the whole
    // point of the grid and it is measured by the sibling test.
    //
    // After a repair, it stands in for nothing FROM THE WIRE — the grid and
    // the session are both skipped, unconditionally, for the reasons this
    // whole seam exists (see `verify_direct_sets_quietly`'s docs). But it is
    // no longer required to read every volume either: the repair carries its
    // own pre-repair verdict forward, and every volume that verdict already
    // called `Complete` was proven by a DISK read minutes ago in this same
    // flow, not by wire evidence — so only the volume(s) the repair actually
    // rewrote need reading again. This is the direct-store mirror of what
    // `verify_repaired_par2_files_with_placement` already does for a
    // conventional set.
    const RR_BYTES: usize = 512;
    let member_name = "Silver.Horizon.S03E09.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 211) as u8).collect();
    let clean = recovery_record_store_set(member_name, &payload, 3, RR_BYTES);
    let par2_bytes = repairable_par2_index(&clean, 4);
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, RR_BYTES);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41410);
    let (mut pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed::default(),
    )
    .await;

    assert_eq!(
        pipeline.direct_verify_read_splits.first().copied(),
        Some((2, 1)),
        "non-vacuity: the PRE-repair pass must have claimed the two clean \
         volumes and read only the one damaged one, or there is nothing for \
         the repair's write set to be narrower than; splits = {:?}",
        pipeline.direct_verify_read_splits
    );
    assert!(
        pipeline.direct_post_repair_read_splits.is_empty(),
        "and nothing may be recorded as post-repair before a repair has run"
    );

    drive_grid_fed_job_to_terminal(&mut pipeline, job_id).await;

    assert!(
        !pipeline.direct_post_repair_read_splits.is_empty(),
        "a repair ran, so at least one pass must have been a read-back; all \
         splits = {:?}",
        pipeline.direct_verify_read_splits
    );
    for (claimed, read) in &pipeline.direct_post_repair_read_splits {
        assert_eq!(
            *claimed, 0,
            "a post-repair pass may stand in for nothing FROM THE WIRE; \
             splits = {:?}",
            pipeline.direct_post_repair_read_splits
        );
        assert_eq!(
            *read, 1,
            "and with a live carry it must read only the one volume the \
             repair actually rewrote, not the two the pre-repair pass had \
             already proven from disk; splits = {:?}",
            pipeline.direct_post_repair_read_splits
        );
    }
    assert_eq!(
        pipeline.par2_authoritative_verify_calls, 0,
        "the point of carrying the pre-repair verdict forward is that the \
         completion gate settles it directly instead of asking \
         `verify_par2_with_placement` to read this virtual set again to reach \
         the same answer — a whole-set pass here would be exactly the second \
         read this design exists to remove"
    );
}

#[tokio::test]
async fn a_post_repair_pass_without_a_surviving_carry_still_reads_every_volume() {
    // The fallback this whole design depends on staying reachable: a carry
    // can go missing — the job restarted, the pipeline evicted it, the
    // recovery set got rebound — and when it does, the post-repair pass has
    // nothing narrower to trust than the full, unconditional read it has
    // always taken. Proven here by tearing the carry out from under a repair
    // that just ran, before calling the gate again to force the read-back
    // that would otherwise have consulted it.
    //
    // Driven through direct, single-step calls to
    // `resolve_direct_sets_before_par2_repairer` — the same shape the ticket
    // liveness tests already use — rather than the full completion-check
    // drive loop, because that loop's own queue pumping can settle the ticket
    // before a test gets a chance to evict anything between the repair and
    // the read-back it leaves behind.
    let member_name = "Silver.Horizon.S01E30.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 191) as u8).collect();
    let (volumes, par2_bytes) = repairable_envelope_damage(member_name, &payload);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41432);
    let (mut pipeline, working_dir) =
        live_damaged_direct_job(&temp_dir, job_id, &volumes, &par2_bytes, None).await;

    assert_eq!(
        pipeline.direct_store.repair_attempts, 1,
        "non-vacuity: the set must have had its one real repair, or there is \
         no carry for this test to evict"
    );
    assert!(
        !pipeline.direct_post_repair_carry.is_empty(),
        "non-vacuity: the repair must have left a carry behind for this test \
         to evict"
    );
    pipeline.direct_post_repair_carry.clear();

    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");
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
        let done = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            pipeline.direct_post_repair_done_rx.recv(),
        )
        .await
        .expect("the post-repair read-back should finish")
        .expect("the post-repair completion channel stays open");
        pipeline.handle_direct_post_repair_done(done);
    };

    assert!(
        matches!(
            resolution,
            crate::pipeline::direct_store::wiring::DirectPar2Resolution::Clean(_)
        ),
        "with no fresh damage the read-back must settle the set clean; got \
         {resolution:?}"
    );
    assert!(
        !pipeline.direct_post_repair_read_splits.is_empty(),
        "a repair ran, so at least one pass must have been a read-back; all \
         splits = {:?}",
        pipeline.direct_verify_read_splits
    );
    for (claimed, read) in &pipeline.direct_post_repair_read_splits {
        assert_eq!(
            *claimed, 0,
            "a post-repair pass may stand in for nothing FROM THE WIRE; \
             splits = {:?}",
            pipeline.direct_post_repair_read_splits
        );
        assert_eq!(
            *read, 3,
            "and without a surviving carry it must fall back to reading every \
             described volume, exactly as it always did; splits = {:?}",
            pipeline.direct_post_repair_read_splits
        );
    }
}

#[tokio::test]
async fn a_disk_fault_in_the_repaired_volume_is_caught_after_a_repair() {
    // The half of the safety property a selective post-repair read-back keeps:
    // a fault under the volume the repair itself just rewrote is still caught,
    // because that volume is exactly what the write set names — carried or
    // not, it is never one of the files a post-repair pass stands in for.
    //
    // Introduced *between* the repair landing and the read-back that follows
    // it, so this is not the repair's own accounting catching its own
    // mistake: it is a second, independent fault — a bad sector, a stray
    // write from something else entirely — landing on bytes the repair had
    // already got right.
    const RR_BYTES: usize = 512;
    let member_name = "Silver.Horizon.S03E10.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 211) as u8).collect();
    let clean = recovery_record_store_set(member_name, &payload, 3, RR_BYTES);
    let par2_bytes = repairable_par2_index(&clean, 4);
    // Volume 1's recovery record is damaged on the WIRE, which is what gives
    // the job a repair to run — and makes volume 1 the write set.
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, RR_BYTES);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41411);
    let (mut pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed::default(),
    )
    .await;

    let pre_repair_splits = pipeline.direct_verify_read_splits.clone();
    assert_eq!(
        pre_repair_splits.first().copied(),
        Some((2, 1)),
        "non-vacuity: volume 1 has to have been the one READ (and found \
         damaged) pre-repair, or it is not the write set the fault below is \
         supposed to land inside; splits = {pre_repair_splits:?}"
    );

    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    let mut faulted = false;
    for _ in 0..48 {
        if matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        drain_rar_refreshes(&mut pipeline).await;
        pipeline.check_job_completion(job_id).await;
        // The instant the repair has left its carry behind — after it ran,
        // before the read-back that consults it — land the fault on the
        // volume the carry's write set names, and never again.
        if !faulted
            && pipeline
                .direct_post_repair_carry
                .get(&job_id)
                .is_some_and(|carry| !carry.write_set.is_empty())
        {
            let envelope = envelope_path_for_volume(&pipeline, job_id, 1);
            let mut bytes = std::fs::read(&envelope).expect("volume 1's envelope exists on disk");
            assert!(
                bytes.len() > 64,
                "non-vacuity: the envelope must actually hold bytes to corrupt; len = {}",
                bytes.len()
            );
            for byte in bytes.iter_mut().take(64) {
                *byte ^= 0xFF;
            }
            std::fs::write(&envelope, &bytes).expect("the fault lands on disk");
            faulted = true;
        }
        pump_pipeline_runtime_queues(&mut pipeline).await;
        settle_inflight_moves(&mut pipeline).await;
        if let Ok(Some(done)) = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            pipeline.extract_done_rx.recv(),
        )
        .await
        {
            pipeline.handle_extraction_done(done).await;
            pump_pipeline_runtime_queues(&mut pipeline).await;
            settle_inflight_moves(&mut pipeline).await;
        }
    }

    assert!(
        faulted,
        "non-vacuity: the repair must actually have left a carry to land the \
         fault beside, or this test proves nothing"
    );

    let post_repair = pipeline.direct_post_repair_read_splits.clone();
    assert!(
        !post_repair.is_empty(),
        "a repair ran, so a read-back pass must have followed it; splits = {:?}",
        pipeline.direct_verify_read_splits
    );
    assert!(
        post_repair
            .iter()
            .all(|(claimed, read)| *claimed == 0 && *read >= 1),
        "and every read-back lap must at least have read volume 1 back; \
         splits = {post_repair:?}"
    );

    // The verdict the last quiet pass reached. Volume 1 is the file the fault
    // landed on, and it must not come back Complete.
    let verdict = pipeline
        .last_direct_verdict
        .clone()
        .expect("a quiet pass recorded its verdict");
    let volume_one = &clean[1].0;
    let entry = verdict
        .files
        .iter()
        .find(|file| file.filename == *volume_one)
        .unwrap_or_else(|| panic!("volume 1 is described; verdict = {verdict:?}"));
    assert!(
        !matches!(entry.status, par2_rs::verify::FileStatus::Complete),
        "the post-repair pass reads back every volume in the write set, and \
         volume 1 is in it whether the carry survives or not — a `Complete` \
         verdict here is a corrupt member shipping in a finished job. \
         status = {:?}",
        entry.status
    );
}

#[tokio::test]
async fn a_disk_fault_outside_the_write_set_is_the_accepted_residual_after_a_repair() {
    // The trade-off this whole redesign makes, pinned rather than left
    // implicit: a selective post-repair pass reads back the write set and
    // nothing else, so a fault landing on a volume the repair did NOT rewrite
    // — this one was grid-claimed pre-repair and never touched a disk read at
    // all — ships unnoticed. This is not a bug this pass forgot to close; it
    // is the same trust class `verify_repaired_par2_files_with_placement`
    // already accepts for a conventional set's untouched files, extended here
    // to the direct-store path for the same reason: re-reading bytes the
    // repair never wrote answers a question a very recent pass already
    // answered, and a knob to force the wider read would only buy back the
    // cost this redesign exists to remove.
    //
    // If this test ever starts failing because the fault gets caught, that is
    // a sign the selective read-back widened again — worth knowing, and worth
    // deciding on purpose rather than by accident.
    const RR_BYTES: usize = 512;
    let member_name = "Silver.Horizon.S03E13.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 211) as u8).collect();
    let clean = recovery_record_store_set(member_name, &payload, 3, RR_BYTES);
    let par2_bytes = repairable_par2_index(&clean, 4);
    // Volume 1's recovery record is damaged on the WIRE, which is what gives
    // the job a repair to run at all. Volume 0 is perfect on the wire and is
    // never in the write set.
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, RR_BYTES);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41431);
    let (mut pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed::default(),
    )
    .await;

    let pre_repair_splits = pipeline.direct_verify_read_splits.clone();
    assert_eq!(
        pre_repair_splits.first().copied(),
        Some((2, 1)),
        "non-vacuity: volume 0 has to have been CLAIMED by the grid rather \
         than read, or it was never a candidate for the write set to leave \
         behind; splits = {pre_repair_splits:?}"
    );

    // The fault. Volume 0's envelope is rewritten on disk with bytes the wire
    // never carried — a bad sector, a short write, a neighbour scribbling. The
    // grid's claim for volume 0 still stands: it was made from article CRCs
    // recorded when those bytes were durable, and nothing has re-read them
    // since — and after the repair runs, nothing will, because volume 0 is
    // never in its write set.
    let envelope = envelope_path_for_volume(&pipeline, job_id, 0);
    let mut faulted = std::fs::read(&envelope).expect("volume 0's envelope exists on disk");
    assert!(
        faulted.len() > 64,
        "non-vacuity: the envelope must actually hold bytes to corrupt; len = {}",
        faulted.len()
    );
    for byte in faulted.iter_mut().take(64) {
        *byte ^= 0xFF;
    }
    std::fs::write(&envelope, &faulted).expect("the fault lands on disk");

    drive_grid_fed_job_to_terminal(&mut pipeline, job_id).await;

    let post_repair = pipeline.direct_post_repair_read_splits.clone();
    assert!(
        !post_repair.is_empty(),
        "a repair ran, so a read-back pass must have followed it; splits = {:?}",
        pipeline.direct_verify_read_splits
    );
    for (claimed, read) in &post_repair {
        assert_eq!(*claimed, 0, "splits = {post_repair:?}");
        assert_eq!(
            *read, 1,
            "a live carry narrows the read-back to the one volume the repair \
             actually rewrote — volume 0 is not it; splits = {post_repair:?}"
        );
    }

    // The verdict the last quiet pass reached. Volume 0 was never read back,
    // so its carried pre-repair verdict — `Complete`, from the grid — stands.
    let verdict = pipeline
        .last_direct_verdict
        .clone()
        .expect("a quiet pass recorded its verdict");
    let volume_zero = &clean[0].0;
    let entry = verdict
        .files
        .iter()
        .find(|file| file.filename == *volume_zero)
        .unwrap_or_else(|| panic!("volume 0 is described; verdict = {verdict:?}"));
    assert!(
        matches!(entry.status, par2_rs::verify::FileStatus::Complete),
        "this is the accepted residual, not a surprise: a fault outside the \
         write set does not get caught, because nothing reads that volume \
         back post-repair when a carry survives. status = {:?}",
        entry.status
    );
}

#[tokio::test]
async fn a_clean_direct_verdict_settles_without_a_second_whole_set_pass() {
    // The other half of the redesign: a direct set that was never damaged
    // reaches `DirectPar2Resolution::Clean` from the completion gate's own
    // direct-aware seam, and that verdict now settles the job directly
    // instead of being thrown away and re-derived by asking
    // `verify_par2_with_placement` to read the same virtual volumes again.
    // Proven two ways: the whole-set authoritative counter never moves, and
    // the job's verification-complete event fires exactly once — not zero
    // (the gate must still announce a verdict) and not twice (which is what
    // the discarded-verdict bug produced: one from the direct gate's own
    // settle, a second from the redundant whole-set pass it used to fall
    // through to).
    let member_name = "Silver.Horizon.S03E14.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41440);
    // The last article is withheld so the set is still live — and has not
    // yet reached a verdict — when the feed returns, which is what lets this
    // test subscribe before the one verification event it means to count.
    // `GridFeed::default()` reaches the verdict *inside* the feed itself, too
    // early for a subscription taken after it to see anything.
    let (mut pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed {
            withhold_last_article: true,
            ..GridFeed::default()
        },
    )
    .await;

    let mut events = pipeline.event_tx.subscribe();

    // The withheld article: the last volume's last segment, delivered now
    // that the subscription is in place.
    let last_ordinal = volumes.len() as u32 - 1;
    let last_segment = GRID_ARTICLES as u32 - 1;
    let (start, end) = grid_article_extent(&volumes, last_ordinal, last_segment);
    let (filename, bytes) = &volumes[last_ordinal as usize];
    submit_grid_cut_article(
        &mut pipeline,
        job_id,
        IndexPosition::First.volume_file_index(last_ordinal),
        last_segment,
        start as u64,
        &bytes[start..end],
        filename,
    )
    .await;

    drive_grid_fed_job_to_terminal(&mut pipeline, job_id).await;

    assert!(
        matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete)
        ),
        "non-vacuity: the job must actually finish, or the assertions below \
         prove nothing; status = {:?}",
        job_status_for_assert(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_authoritative_verify_calls, 0,
        "an undamaged direct set's verdict must be settled from the direct \
         gate's own read, never from a whole-set `verify_par2_with_placement` \
         pass over the same virtual volumes"
    );

    let announced = drain_job_events(&mut events, job_id);
    let verification_complete: Vec<_> = announced
        .iter()
        .filter(|event| matches!(event, PipelineEvent::JobVerificationComplete { .. }))
        .collect();
    assert_eq!(
        verification_complete.len(),
        1,
        "exactly one verdict must be announced — the discarded-verdict bug \
         this design fixes produced a second one from the redundant whole-set \
         pass; got {announced:?}"
    );
    assert!(
        matches!(
            verification_complete[0],
            PipelineEvent::JobVerificationComplete { passed: true, .. }
        ),
        "and it must report the clean verdict the set actually had; got \
         {:?}",
        verification_complete[0]
    );
}

#[tokio::test]
async fn demoting_a_carrying_set_clears_the_carry_and_the_ticket_slots() {
    // A demoted set's volumes become real files and hand off to the
    // conventional repairer, which brings its own post-repair pass — so any
    // post-repair bookkeeping this job was carrying for the direct gate must
    // not survive to describe bytes a different repair path now owns. Left
    // behind, a stale carry would (at best) be silently ignored by the
    // recovery-set-id check and (at worst, if the demoted set later comes
    // back on the same recovery set) stand in for volumes a conventional
    // repair just rewrote.
    let member_name = "Silver.Horizon.S01E31.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 191) as u8).collect();
    let (volumes, par2_bytes) = repairable_envelope_damage(member_name, &payload);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41433);
    let (mut pipeline, _working_dir) =
        live_damaged_direct_job(&temp_dir, job_id, &volumes, &par2_bytes, None).await;

    assert_eq!(
        pipeline.direct_store.repair_attempts, 1,
        "non-vacuity: the set must have had its one real repair, or there is \
         no carry for the demotion below to clear"
    );
    assert!(
        !pipeline.direct_post_repair_carry.is_empty(),
        "non-vacuity: the repair must have left a carry behind"
    );

    // A fabricated in-flight ticket and a parked result, standing in for
    // whatever bookkeeping a real post-repair read-back would have left mid
    // flight. The demotion must clear these too, not only the carry — a
    // lingering in-flight entry is what used to suppress the drain-tick
    // re-arm in `schedule_job_completion_check_if_download_pipeline_drained`.
    let recovery_set_id = pipeline
        .par2_set(job_id)
        .map(|set| set.recovery_set_id)
        .expect("the index parsed");
    pipeline.direct_post_repair_in_flight.insert(
        job_id,
        crate::pipeline::DirectPostRepairWork {
            work_id: 1,
            recovery_set_id,
            submitted_at: std::time::Instant::now(),
        },
    );
    pipeline.direct_post_repair_results.insert(
        job_id,
        (
            recovery_set_id,
            Ok(par2_rs::VerificationResult {
                files: Vec::new(),
                recovery_blocks_available: 0,
                total_missing_blocks: 0,
                repairable: par2_rs::verify::Repairability::NotNeeded,
            }),
        ),
    );

    pipeline
        .demote_direct_set(job_id, 0, DemotionReason::UnparsableVolume)
        .await;
    settle_direct_post_repair_work(&mut pipeline).await;

    assert!(
        pipeline.direct_post_repair_carry.is_empty(),
        "the demotion must clear this job's post-repair carry"
    );
    assert!(
        pipeline.direct_post_repair_in_flight.is_empty(),
        "and its in-flight ticket bookkeeping"
    );
    assert!(
        pipeline.direct_post_repair_results.is_empty(),
        "and any result parked waiting for a gate lap to consume it"
    );
}

#[tokio::test]
async fn a_ticket_parked_against_a_stale_recovery_set_is_dropped_for_a_fresh_one() {
    // The permanent-park bug this fix closes: before it, an in-flight ticket
    // whose `recovery_set_id` disagreed with the set currently being resolved
    // returned `None` forever — no new ticket ever started, so no result ever
    // arrived, so nothing ever re-armed the job. `recovery_set_id` can
    // legitimately drift out from under an in-flight ticket (a later PAR2
    // index rebinds the served set), so the ticket has to be dropped and
    // replaced instead of parking behind it.
    let member_name = "Silver.Horizon.S01E32.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 191) as u8).collect();
    let (volumes, par2_bytes) = repairable_envelope_damage(member_name, &payload);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41434);
    let (mut pipeline, working_dir) =
        live_damaged_direct_job(&temp_dir, job_id, &volumes, &par2_bytes, None).await;

    assert_eq!(
        pipeline.direct_store.repair_attempts, 1,
        "non-vacuity: the set must have had its one real repair, or the gate \
         below never reaches the ticket seam at all"
    );

    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");
    let stale_recovery_set_id = par2_rs::RecoverySetId::from_bytes([0xAA; 16]);
    assert_ne!(
        stale_recovery_set_id, par2_set.recovery_set_id,
        "non-vacuity: the fabricated id must actually disagree with the set \
         this job serves"
    );
    // A ticket parked against a recovery set this job no longer serves — the
    // shape a rebind leaves behind, fabricated directly rather than driving a
    // second PAR2 index through the harness to produce it.
    pipeline.direct_post_repair_in_flight.insert(
        job_id,
        crate::pipeline::DirectPostRepairWork {
            work_id: 999,
            recovery_set_id: stale_recovery_set_id,
            submitted_at: std::time::Instant::now(),
        },
    );

    let resolution = pipeline
        .resolve_direct_sets_before_par2_repairer(
            job_id,
            Arc::clone(&par2_set),
            working_dir.clone(),
        )
        .await;

    assert!(
        matches!(
            resolution,
            crate::pipeline::direct_store::wiring::DirectPar2Resolution::Pending
        ),
        "a fresh ticket must have started for the set this job actually \
         serves; got {resolution:?}"
    );
    let in_flight = pipeline
        .direct_post_repair_in_flight
        .get(&job_id)
        .expect("a fresh ticket must be in flight");
    assert_eq!(
        in_flight.recovery_set_id, par2_set.recovery_set_id,
        "the stale entry must have been replaced, not left in place — a \
         ticket still parked against the fabricated id would mean the park \
         was never broken"
    );
    assert_ne!(
        in_flight.work_id, 999,
        "the fresh ticket must carry a work id of its own, fencing the stale \
         one's done message if it ever lands"
    );

    // And the job actually finishes — the permanent-park bug's whole
    // signature was that nothing downstream of the stale entry ever ran
    // again.
    let done = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        pipeline.direct_post_repair_done_rx.recv(),
    )
    .await
    .expect("the fresh ticket's read-back should finish")
    .expect("the post-repair completion channel stays open");
    assert_eq!(
        done.recovery_set_id, par2_set.recovery_set_id,
        "the result that lands must be the fresh ticket's, not a stale one"
    );
    pipeline.handle_direct_post_repair_done(done);
    let resolution = pipeline
        .resolve_direct_sets_before_par2_repairer(job_id, par2_set, working_dir)
        .await;
    assert!(
        !matches!(
            resolution,
            crate::pipeline::direct_store::wiring::DirectPar2Resolution::Pending
        ),
        "with the fresh ticket's result in hand the gate must reach a verdict \
         rather than parking again; got {resolution:?}"
    );
}

#[tokio::test]
async fn the_post_repair_discriminator_follows_the_repair_latch() {
    // The read-back rule is only as good as the question "has a repair run?",
    // and the two tests above cannot pin that question on their own: the grid is
    // independently empty after a repair, so they pass either way (verified by
    // re-running them with the guard removed). This pins the discriminator.
    const RR_BYTES: usize = 512;
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 211) as u8).collect();

    // A set nothing damaged. It verifies clean and never repairs, so every pass
    // it sees may stand in for what the grid adjudicated.
    let temp_dir = tempfile::tempdir().unwrap();
    let clean_job = JobId(41412);
    let undamaged = single_member_store_set("Silver.Horizon.S03E11.mkv", &payload, 3);
    let clean_par2 = par2_index_over_volumes(&undamaged);
    let (clean_pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        clean_job,
        &undamaged,
        &clean_par2,
        GridFeed::default(),
    )
    .await;
    assert!(
        !clean_pipeline.direct_sets_repaired_in_place(clean_job),
        "a set that was never repaired must not be read back as if it had been;          sets = {:?}",
        clean_pipeline.direct_store.sets_for(clean_job)
    );

    // And one whose recovery record arrived damaged, which repairs in place.
    let damaged_temp = tempfile::tempdir().unwrap();
    let damaged_job = JobId(41413);
    let clean_set = recovery_record_store_set("Amber.Trail.S03E11.mkv", &payload, 3, RR_BYTES);
    let damaged_par2 = repairable_par2_index(&clean_set, 4);
    let mut damaged = clean_set.clone();
    damage_recovery_record(&mut damaged, 1, RR_BYTES);
    let (mut pipeline, _, _) = grid_fed_direct_job(
        &damaged_temp,
        damaged_job,
        &damaged,
        &damaged_par2,
        GridFeed::default(),
    )
    .await;
    assert!(
        pipeline.direct_store.repair_attempts > 0,
        "non-vacuity: the fixture must actually repair in place, or the latch \
         never burns and this asserts nothing"
    );
    assert!(
        pipeline.direct_sets_repaired_in_place(damaged_job),
        "the latch burns at a repair's first irreversible step, so from that \
         moment on every quiet pass is a read-back"
    );

    // A demoted set is not this pass's business: its volumes became real files
    // and the conventional repairer brings its own post-repair verification.
    for index in 0..pipeline.direct_store.sets_for(damaged_job).len() {
        if let Some(set) = pipeline.direct_store.set_mut(damaged_job, index) {
            set.demote(crate::pipeline::direct_store::router::DemotionReason::UnparsableVolume);
        }
    }
    assert!(
        !pipeline.direct_sets_repaired_in_place(damaged_job),
        "a demoted set hands its volumes to the conventional path"
    );
}

#[tokio::test]
async fn an_obfuscated_direct_set_still_reaches_zero_io_grid_adjudication() {
    // An obfuscated post names its volumes after a hash while the recovery set
    // describes their real names. A volume that binds to no description
    // forfeits the dual-CRC grid entirely — nothing to measure a verdict
    // against — so every volume would be read back at completion.
    //
    // MEASURED, and it is not what the content binder was written for: this
    // shape already binds by NAME. PAR2-driven identity classification assigns
    // each volume a `canonical_filename` of "silver.horizon.partNN.rar" with
    // `classification_source: Par2`, and `resolve_par2_file_binding` searches
    // the canonical name along with the posted one. Verified by running this
    // test with the content fallback removed: it still passes.
    //
    // It stays as the regression pin for that path — the claim "an obfuscated
    // direct set gets zero-I/O adjudication" is worth holding however it is
    // achieved, and if the classifier ever stops inferring the name, the
    // content binder is what keeps this green.
    let member_name = "Silver.Horizon.S04E01.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let described = single_member_store_set(member_name, &payload, 3);
    // The recovery set describes the volumes under their real names...
    let par2_bytes = par2_index_over_volumes(&described);
    // ...while the post carries them under an obfuscated stem.
    let posted: Vec<(String, Vec<u8>)> = described
        .iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            (
                format!("a7f3e91c8b2d4f60.part{:02}.rar", index + 1),
                bytes.clone(),
            )
        })
        .collect();
    assert!(
        posted
            .iter()
            .zip(described.iter())
            .all(|((posted, _), (real, _))| posted != real),
        "non-vacuity: the posted names must differ from the described ones"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41420);
    let (pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &posted,
        &par2_bytes,
        GridFeed {
            retained_session: true,
            ..GridFeed::default()
        },
    )
    .await;

    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");
    for ordinal in 0..posted.len() as u32 {
        let file_id = NzbFileId {
            job_id,
            file_index: IndexPosition::First.volume_file_index(ordinal),
        };
        let bound = pipeline
            .resolve_par2_file_binding(file_id)
            .unwrap_or_else(|| panic!("volume {ordinal} must bind to a description"));
        let described_name = &par2_set
            .file_description(&bound.par2_file_id)
            .expect("the bound description")
            .filename;
        assert_eq!(
            described_name, &described[ordinal as usize].0,
            "volume {ordinal} must bind to the description holding its own bytes, \
             not merely to some description"
        );
    }

    // And the binding reached the grid while it mattered. The observables have
    // to be records of what the pass DID rather than state inspected
    // afterwards: finalization retires a finalized set's grid entries on
    // purpose, so a post-hoc look at the collector finds nothing whether the
    // adjudication happened or not.
    assert!(
        pipeline.direct_session_pass_calls > 0,
        "the zero-I/O session arm answered, which it can only do when every \
         described volume is adjudicated in stream"
    );
    assert!(
        pipeline.direct_verify_read_splits.is_empty(),
        "so no volume was read back at all; splits = {:?}",
        pipeline.direct_verify_read_splits
    );
    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        sets.contains("Finalized"),
        "and the verdict has to settle the aggregate gate and clear the set, or the \
         zero-I/O pass concluded something the job could not act on; got {sets}"
    );
}

#[tokio::test]
async fn a_damage_demotion_puts_the_set_on_record_and_an_ordinary_one_does_not() {
    // What the completion gate keys on is the *fact* that bytes were found
    // wrong, not the label the demotion carried, so the two populations have
    // to stay separable here: "direct routing could not be used" says nothing
    // about the archive, and must not cost a clean job its extract-first path.
    let member_name = "Silver.Horizon.S01E11.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let capacity_dir = tempfile::tempdir().unwrap();
    let (mut capacity_pipeline, _, _) = new_direct_pipeline(&capacity_dir).await;
    capacity_pipeline
        .direct_store
        .set_gate(DirectStoreGate::Enabled);
    let capacity_job = JobId(41120);
    insert_active_job(
        &mut capacity_pipeline,
        capacity_job,
        direct_store_job_spec("Silver Horizon Capacity", &volumes),
    )
    .await;
    admit_first_volume_article(&mut capacity_pipeline, capacity_job, &volumes).await;
    capacity_pipeline
        .demote_direct_set(capacity_job, 0, DemotionReason::HoldsBudgetExceeded)
        .await;
    settle_direct_post_repair_work(&mut capacity_pipeline).await;
    assert!(
        !capacity_pipeline.archive_extraction_held_for_known_damage(capacity_job),
        "a RAM-budget demotion is not evidence about the bytes"
    );
    assert!(
        !capacity_pipeline
            .known_damaged_archive_sets
            .contains_key(&capacity_job)
    );

    let damaged_dir = tempfile::tempdir().unwrap();
    let (mut damaged_pipeline, _, _) = new_direct_pipeline(&damaged_dir).await;
    damaged_pipeline
        .direct_store
        .set_gate(DirectStoreGate::Enabled);
    let damaged_job = JobId(41121);
    insert_active_job(
        &mut damaged_pipeline,
        damaged_job,
        direct_store_job_spec("Silver Horizon Damaged", &volumes),
    )
    .await;
    admit_first_volume_article(&mut damaged_pipeline, damaged_job, &volumes).await;
    damaged_pipeline
        .demote_direct_set(damaged_job, 0, DemotionReason::PartChecksumMismatch)
        .await;
    settle_direct_post_repair_work(&mut damaged_pipeline).await;
    assert_eq!(
        damaged_pipeline
            .known_damaged_archive_sets
            .get(&damaged_job)
            .map(|sets| sets.iter().cloned().collect::<Vec<_>>()),
        Some(vec!["silver.horizon".to_string()]),
        "the set the mismatch was found in goes on record by name"
    );
}

/// The direct-store twin of `the_last_decode_to_settle_retires_the_probe`.
///
/// A routed source article leaves `handle_decode_success` at its early return,
/// ahead of the reorder buffer and the conventional file-complete seam, so
/// nothing further along that path can notice the job has drained. The last
/// article of a fully routed set is exactly the article an in-flight probe
/// ends up waiting behind: the download result that carried it ran the drain
/// sequence while it was still decoding, and the route itself schedules
/// nothing. Left there, the job holds its completion checkpoint — and with it
/// PAR2 recovery promotion — until the probe's own soft timeout expires.
#[tokio::test]
async fn the_last_routed_article_retires_the_probe() {
    let member_name = "Silver.Horizon.S01E09.mkv";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 167) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);
    let articles = 2usize;
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(410099);

    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = direct_store_job_spec_with_articles("Silver Horizon", &volumes, articles);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    let last_file_index = volumes.len() as u32 - 1;
    let last_segment_number = articles as u32 - 1;
    let last_segment = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: last_file_index,
        },
        segment_number: last_segment_number,
    };

    // Every article but the set's last one routes as it is dispatched.
    for file_index in 0..volumes.len() as u32 {
        for segment_number in 0..articles as u32 {
            let segment_id = SegmentId {
                file_id: NzbFileId { job_id, file_index },
                segment_number,
            };
            take_queued_segment(&mut pipeline, job_id, segment_id);
            if segment_id == last_segment {
                continue;
            }
            submit_volume_article_of(
                &mut pipeline,
                job_id,
                &volumes,
                file_index,
                segment_number,
                articles,
            )
            .await;
        }
    }

    pipeline.activate_health_probes(job_id);
    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        assert!(state.health_probing);
        // A probe rides alongside the download instead of standing in front of
        // it, so it never moves the job out of Downloading.
        assert!(matches!(state.status, JobStatus::Downloading));
    }

    let (filename, bytes) = &volumes[last_file_index as usize];
    let (start, end) = article_extent(bytes.len(), last_segment_number, articles);
    park_job_on_its_final_decode(&mut pipeline, last_segment, (end - start) as u64);

    pipeline.maybe_finish_download_pass(job_id);
    assert!(
        pipeline.jobs.get(&job_id).unwrap().health_probing,
        "an unsettled decode is pending download work: {}",
        debug_job_state(&pipeline, job_id)
    );

    settle_queued_decode(
        &mut pipeline,
        last_segment.file_id,
        last_segment_number,
        start as u64,
        &bytes[start..end],
        filename,
    )
    .await;

    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "the set must still be routing, not writing volumes — otherwise this \
         test is not covering the routed early return"
    );
    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(
        !state.health_probing,
        "the routed article that settled last must retire the probe: {}",
        debug_job_state(&pipeline, job_id)
    );
    assert!(
        !matches!(state.status, JobStatus::Checking),
        "a retired probe must not leave the job parked in Checking: {}",
        debug_job_state(&pipeline, job_id)
    );
    assert!(
        pipeline.pending_completion_checks.contains(&job_id),
        "retiring the probe must hand the job straight to the completion \
         checkpoint"
    );
}
