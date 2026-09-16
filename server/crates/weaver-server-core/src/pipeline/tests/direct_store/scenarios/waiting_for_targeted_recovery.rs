//! Waiting for targeted recovery instead of demoting
//! The config surface, and sparse marking
//! Encrypted direct-store
//! The machinery encryption changes nothing about, asserted rather than

use super::*;

// ---------------------------------------------------------------------------
// Waiting for targeted recovery instead of demoting
// ---------------------------------------------------------------------------

#[tokio::test]
async fn damage_needing_undownloaded_recovery_waits_instead_of_demoting() {
    // The bug this whole path was written around, in one assertion. Recovery is
    // fetched only once damage is known, so the first damage verdict of a job's
    // life always reads zero merged recovery blocks — and any damage at all
    // exceeds zero. The set therefore declined, demoted, materialized every
    // volume, and the recovery it was about to receive repaired physical files
    // instead. No fixture could reach the in-place repair, because the input it
    // needs is guaranteed absent at the only moment it was consulted.
    //
    // So the verdict is answered by *waiting*: ask for the recovery the damage
    // needs and stay direct until it lands.
    let member_name = "Silver.Horizon.S02E01.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let (volumes, index_bytes, recovery_bytes) =
        recovery_in_a_separate_volume(member_name, &payload, 4, &[1]);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41111);
    let (mut pipeline, working_dir, index_file_index, recovery_file_index) =
        direct_job_with_undownloaded_recovery(
            &temp_dir,
            job_id,
            &volumes,
            &index_bytes,
            RECOVERY_VOLUME_NAME,
            &recovery_bytes,
        )
        .await;

    // Demanded rather than waited for: the checkpoint row is written on a timer,
    // and the claim below — that waiting costs the set nothing durable — is
    // vacuous if there was no row to keep.
    pipeline
        .demand_direct_store_barriers(job_id, BarrierDemand::PhaseChange)
        .await;
    let coverage_before = pipeline.db.load_direct_coverage(job_id).unwrap();
    assert!(
        !coverage_before.is_empty(),
        "non-vacuity: the live set must hold a checkpoint row before the verdict, \
         or 'the row survives' proves nothing"
    );

    deliver_par2_index(&mut pipeline, job_id, index_file_index, &index_bytes).await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert_eq!(
        insufficient_verdict(&pipeline),
        Some((1, 0)),
        "non-vacuity: the pass must reach the verdict this exists for — damage \
         that needs blocks, with none merged yet; sets = {sets}"
    );
    assert_eq!(
        pipeline.direct_store.repair_defers, 1,
        "the set must wait for the recovery rather than answer with what it has; \
         sets = {sets}"
    );
    assert!(
        !sets.contains("Demoted"),
        "and it must still be direct while it waits — a demotion here throws \
         away the outputs the wait exists to keep; got {sets}"
    );
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .all(|set| !set.repair_attempted()),
        "the once-latch must be intact: the deferred pass has to be the set's \
         *first* real attempt, or the retry refuses with AlreadyRepaired and \
         demotes for the verdict the recovery was about to answer; got {sets}"
    );
    assert_eq!(
        pipeline.direct_store.repair_attempts, 0,
        "nothing irreversible may have run — no checkpoint delete, no \
         materialization; sets = {sets}"
    );
    assert_eq!(
        pipeline.db.load_direct_coverage(job_id).unwrap(),
        coverage_before,
        "the checkpoint row is deleted by an attempt, and no attempt was made"
    );
    assert_eq!(
        direct_scratch_left(&working_dir),
        0,
        "no scratch: nothing was materialized"
    );
    assert!(
        pipeline.is_promoted_recovery_file(job_id, recovery_file_index),
        "and the wait must have asked for something — the recovery volume that \
         covers the damage is now promoted"
    );
    assert!(pipeline.jobs.get(&job_id).is_some_and(|state| {
        state.download_queue.count_matching(|work| {
            work.segment_id.file_id.file_index == recovery_file_index && work.completion_critical
        }) > 0
    }));
    assert!(
        pipeline
            .list_jobs()
            .into_iter()
            .find(|job| job.job_id == job_id)
            .is_some_and(|job| job.fetching_repair_data),
        "the public job snapshot must expose the completion-critical repair fetch"
    );
    assert!(
        pipeline.job_has_promoted_recovery_pipeline_work(job_id, "test"),
        "with its work on the wire, which is what makes the wait bounded"
    );

    // Every later tick of the gate while that work is on the wire has to answer
    // without verifying anything. The pass is a full PAR2 scan, the gate ticks
    // on every completing article, and until the wave merges the scan can only
    // reach the verdict that started the wait.
    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");
    let before = pipeline.direct_session_pass_calls;
    let resolution = pipeline
        .resolve_direct_sets_before_par2_repairer(job_id, par2_set, working_dir.clone())
        .await;
    assert!(
        matches!(
            resolution,
            crate::pipeline::direct_store::wiring::DirectPar2Resolution::Deferred
        ),
        "a job already waiting stays waiting; got {resolution:?}"
    );
    assert_eq!(
        pipeline.direct_session_pass_calls, before,
        "and does not re-verify to find that out"
    );
}

#[tokio::test]
async fn a_deferred_direct_set_repairs_in_place_once_its_recovery_lands() {
    // The other half, and the only one that proves the feature reachable: the
    // wait has to end in an in-place repair. A completing PAR2 file already
    // merges its slices and re-checks the job, so nothing new re-arms the gate —
    // the set simply has to still be direct when that happens.
    let member_name = "Silver.Horizon.S02E02.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 179) as u8).collect();
    let (volumes, index_bytes, recovery_bytes) =
        recovery_in_a_separate_volume(member_name, &payload, 4, &[1]);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41112);
    let (mut pipeline, working_dir, index_file_index, recovery_file_index) =
        direct_job_with_undownloaded_recovery(
            &temp_dir,
            job_id,
            &volumes,
            &index_bytes,
            RECOVERY_VOLUME_NAME,
            &recovery_bytes,
        )
        .await;

    deliver_par2_index(&mut pipeline, job_id, index_file_index, &index_bytes).await;
    assert_eq!(
        pipeline.direct_store.repair_defers, 1,
        "non-vacuity: act one has to be the wait, or act two proves nothing"
    );
    assert_eq!(
        pipeline.direct_store.repair_attempts, 0,
        "and the set must arrive at act two with its attempt unspent"
    );

    // The promoted work leaves the queue as it is picked up, which the harness
    // does not model — it delivers articles without ever dequeuing them. Left
    // in, the wave reads as permanently in flight and the gate rightly refuses
    // to re-verify while it is.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
    }

    // Act two: the promoted recovery volume arrives. This is the production
    // re-arm and nothing else — the decode seam merges the slices and runs the
    // completion gate, which is where the repair now finds non-zero recovery.
    let mut repair_events = pipeline.event_tx.subscribe();
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: recovery_file_index,
        },
        0,
        0,
        &recovery_bytes,
        RECOVERY_VOLUME_NAME,
        None,
    )
    .await;

    // The repair never enters `JobStatus::Repairing`, so the event pair is the
    // only public record it ran — a consumer reading this job's history must
    // see a repair, not a job that was never damaged.
    let announced = drain_job_events(&mut repair_events, job_id);
    assert_eq!(
        announced
            .iter()
            .filter(|event| matches!(event, PipelineEvent::RepairStarted { .. }))
            .count(),
        1,
        "the in-place repair must announce itself; got {announced:?}"
    );
    assert_eq!(
        announced
            .iter()
            .filter(|event| matches!(
                event,
                PipelineEvent::RepairComplete {
                    slices_repaired: 1,
                    ..
                }
            ))
            .count(),
        1,
        "and must report completion with the one slice it rebuilt; got {announced:?}"
    );

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert_eq!(
        (
            pipeline.direct_store.repair_attempts,
            pipeline.direct_store.repair_materialized_volumes
        ),
        (1, 1),
        "the set must repair in place once the blocks are merged, materializing \
         only the damaged volume; sets = {sets}"
    );
    assert_eq!(
        pipeline
            .metrics
            .direct_sets_repaired_while_direct
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "and the lifetime counter — which had never read non-zero — must count \
         it; sets = {sets}"
    );
    assert!(
        !sets.contains("Demoted"),
        "the set stays direct throughout; got {sets}"
    );
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .any(|set| !set.is_demoted() && set.repair_attempted()),
        "with its one attempt now spent, which is what bounds a set that comes \
         back damaged; got {sets}"
    );
    assert!(
        !volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists()),
        "and no source volume may exist under its own name — the repair reads \
         every clean volume virtually"
    );
    assert_eq!(
        direct_scratch_left(&working_dir),
        0,
        "the scratch is deleted once its spans are routed"
    );
}

#[tokio::test]
async fn a_direct_set_demotes_when_the_recovery_it_needs_cannot_arrive() {
    // The livelock guard, and the reason the wait is decided rather than
    // assumed. Waiting is only correct while recovery is actually coming: with
    // the recovery articles gone there is nothing to promote and nothing on the
    // wire, so the answer has to be the immediate demotion it always was. This
    // branch has produced two livelocks already; an unbounded wait would be the
    // third.
    let member_name = "Silver.Horizon.S02E03.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 181) as u8).collect();
    let (volumes, index_bytes, recovery_bytes) =
        recovery_in_a_separate_volume(member_name, &payload, 4, &[1]);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41113);
    let (mut pipeline, _working_dir, index_file_index, recovery_file_index) =
        direct_job_with_undownloaded_recovery(
            &temp_dir,
            job_id,
            &volumes,
            &index_bytes,
            RECOVERY_VOLUME_NAME,
            &recovery_bytes,
        )
        .await;
    // The one difference from the deferring run: the recovery work is gone, as
    // it is for a job whose recovery articles came back unavailable. The blocks
    // are still advertised in the NZB, so capacity still covers the damage — the
    // only thing missing is any way to fetch them.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.recovery_queue = crate::DownloadQueue::new();
    }

    deliver_par2_index(&mut pipeline, job_id, index_file_index, &index_bytes).await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        pipeline
            .total_recovery_block_capacity(job_id, pipeline.par2_served_set_id(job_id).unwrap())
            >= 1,
        "non-vacuity: the NZB still advertises enough recovery, so the demotion \
         below is about the recovery being unreachable and nothing else"
    );
    assert_eq!(
        pipeline.direct_store.repair_defers, 0,
        "the damage must be answered by demoting rather than waiting; sets = {sets}"
    );
    assert_eq!(
        pipeline.direct_store.repair_attempts, 0,
        "and decided before any attempt: an insufficient verdict is one the \
         planner refuses outright, so an attempt could only burn the latch and \
         the checkpoint row on its way to the same refusal; sets = {sets}"
    );
    assert!(
        !pipeline.is_promoted_recovery_file(job_id, recovery_file_index),
        "there was nothing left to promote, which is exactly why it demotes"
    );
    assert!(
        sets.contains("Demoted(Par2Damaged)"),
        "the set materializes for the conventional path, which reaches the same \
         dead end with better diagnostics; got {sets}"
    );
}

#[tokio::test]
async fn the_defer_budget_bounds_a_job_that_keeps_coming_back_short() {
    // The arithmetic bound behind the structural one. The first wave asks for
    // every block the verdict needs, so a second only happens if the first
    // arrived and still fell short — but "already promoted" is derived state,
    // and a derivation that goes wrong here waits forever. Budget spent, the
    // next short verdict demotes even with recovery still parked and ready to
    // promote.
    let member_name = "Silver.Horizon.S02E04.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 193) as u8).collect();
    let (volumes, index_bytes, recovery_bytes) =
        recovery_in_a_separate_volume(member_name, &payload, 4, &[1]);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41114);
    let (mut pipeline, _working_dir, index_file_index, recovery_file_index) =
        direct_job_with_undownloaded_recovery(
            &temp_dir,
            job_id,
            &volumes,
            &index_bytes,
            RECOVERY_VOLUME_NAME,
            &recovery_bytes,
        )
        .await;
    // Three waves already spent. Everything else is exactly the run that waits.
    pipeline
        .direct_store
        .set_repair_defer_waves(job_id, MAX_DIRECT_REPAIR_DEFER_WAVES);

    deliver_par2_index(&mut pipeline, job_id, index_file_index, &index_bytes).await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert_eq!(
        pipeline.direct_store.repair_defers, 0,
        "a job past its budget must not start another wait; sets = {sets}"
    );
    assert_eq!(
        pipeline.direct_store.repair_attempts, 0,
        "and the demotion is decided without spending an attempt on a verdict \
         the planner would refuse; sets = {sets}"
    );
    assert!(
        !pipeline.is_promoted_recovery_file(job_id, recovery_file_index),
        "and must not promote another wave of recovery to wait for"
    );
    assert!(
        sets.contains("Demoted(Par2Damaged)"),
        "it demotes instead; got {sets}"
    );
}

#[tokio::test]
async fn damage_beyond_every_advertised_recovery_block_never_waits() {
    // Waiting can only ever help damage the recovery *set* could cover.
    // `blocks_available` is what has merged; the NZB's advertised total is the
    // ceiling, and past it no amount of downloading changes the answer. Delaying
    // the conventional path there helps nobody: it reaches the same dead end,
    // with the diagnostics that name it.
    //
    // Driven at the decision itself rather than through a fixture, because the
    // damage a small fixture can produce cannot outrun the advertised capacity
    // the same fixture implies — and this contract is about one comparison, not
    // about how damage is reached. The call is the production one; only the
    // block count is chosen.
    let member_name = "Silver.Horizon.S02E05.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 197) as u8).collect();
    let (volumes, index_bytes, recovery_bytes) =
        recovery_in_a_separate_volume(member_name, &payload, 4, &[1]);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41115);
    let (mut pipeline, _working_dir, _index_file_index, recovery_file_index) =
        direct_job_with_undownloaded_recovery(
            &temp_dir,
            job_id,
            &volumes,
            &index_bytes,
            RECOVERY_VOLUME_NAME,
            &recovery_bytes,
        )
        .await;

    let par2_set =
        par2_rs::Par2FileSet::from_files(&[&index_bytes]).expect("fixture index must parse");
    let recovery_set_id = par2_set.recovery_set_id;
    install_test_par2_runtime(&mut pipeline, job_id, par2_set, &[]);
    let capacity = pipeline.total_recovery_block_capacity(job_id, recovery_set_id);
    assert!(
        capacity > 0,
        "non-vacuity: the job must advertise recovery, so the refusal below is \
         about the comparison and not about there being nothing to ask for"
    );

    assert!(
        !pipeline.defer_direct_repair_for_recovery(job_id, capacity + 1, 0),
        "one block past everything the NZB advertises, and no download can close \
         the gap: the answer has to be the immediate demotion"
    );
    assert_eq!(pipeline.direct_store.repair_defers, 0, "nothing waited");
    assert!(
        !pipeline.is_promoted_recovery_file(job_id, recovery_file_index),
        "and nothing was fetched for a wait that could never end — a refusal here \
         must cost no bandwidth at all"
    );

    // The A/B, on the same job: one block fewer is inside the advertised
    // capacity, and that alone flips the answer.
    assert!(
        pipeline.defer_direct_repair_for_recovery(job_id, capacity, 0),
        "non-vacuity: at the ceiling the same call waits, so the ceiling is the \
         only thing under test"
    );
    assert!(
        pipeline.is_promoted_recovery_file(job_id, recovery_file_index),
        "and asks for the recovery it means to wait for"
    );
}

#[tokio::test]
async fn a_direct_set_still_receiving_articles_neither_repairs_nor_waits_nor_demotes() {
    // The settle guard, unchanged and now load-bearing twice over. A set with
    // articles outstanding has holes where its missing ranges will go, and PAR2
    // cannot tell a hole from corruption. Repairing there spends recovery blocks
    // rebuilding bytes already on their way — and *waiting* there is worse,
    // because it fetches those blocks first. The verdict is not yet evidence of
    // anything, so nothing acts on it.
    let member_name = "Silver.Horizon.S02E06.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 199) as u8).collect();
    let (volumes, index_bytes, recovery_bytes) =
        recovery_in_a_separate_volume(member_name, &payload, 4, &[1]);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41116);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);

    let (mut spec, index_file_index) =
        par2_bearing_job_spec("Silver Horizon", &volumes, &index_bytes);
    let recovery_file_index =
        append_par2_recovery_volume(&mut spec, RECOVERY_VOLUME_NAME, &recovery_bytes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    // Every article but the last volume's tail. The set is live, damaged where
    // the fixture damaged it, and genuinely incomplete everywhere else — and the
    // ordinary download queue is left alone, so the job reads as still
    // downloading, which is the truth.
    let mut arrivals = in_order_arrivals(volumes.len());
    arrivals.pop();
    for (file_index, segment_number) in arrivals {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        &index_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;

    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");
    let verification = pipeline
        .verify_direct_sets_quietly(job_id, par2_set, working_dir.clone())
        .await
        .expect("the quiet pass reached a verdict");
    assert!(
        verification.needs_repair(),
        "non-vacuity: the pass must see damage, or every guard below is trivially \
         satisfied"
    );
    let resolution = pipeline
        .resolve_direct_sets_with_par2_damage(job_id, &verification)
        .await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert_eq!(
        resolution,
        crate::pipeline::direct_store::wiring::DirectDamageResolution::Unresolved,
        "an unsettled set answers nothing; sets = {sets}"
    );
    assert_eq!(
        (
            pipeline.direct_store.repair_attempts,
            pipeline.direct_store.repair_defers
        ),
        (0, 0),
        "it neither repairs nor waits; sets = {sets}"
    );
    assert!(
        !sets.contains("Demoted"),
        "nor demotes — the bytes it is missing are on their way; got {sets}"
    );
    assert!(
        !pipeline.is_promoted_recovery_file(job_id, recovery_file_index),
        "and no recovery is promoted to rebuild bytes that are already coming, \
         which is the bandwidth the deferred recovery fetch exists to save"
    );
}

#[tokio::test]
async fn two_sets_sharing_a_clamped_partial_keep_their_bytes_apart() {
    // Internal direct-store paths were derived from names alone, and
    // `path_component_with_suffix` clamps a long
    // name's stem to fit `DOWNLOAD_FILENAME_MAX_BYTES` — so two members whose
    // names differ only past the clamp point reached the *same*
    // `.direct.partial` while their final destinations stayed distinct. Both
    // routers then wrote one file, each passed its integrity gates over its own
    // in-memory buffers, and one member's bytes were silently wrong on disk.
    //
    // The shape is deliberate in two ways. The names share their first 226
    // bytes and differ only in the tail, because the collision under test is
    // the clamp, not equality: members with *equal* names also collide on the
    // final destination, where last-writer-wins is the same semantic two
    // conventionally extracted archives already have — bytes can never witness
    // that. Distinct finals are what make each set's output independently
    // assertable. And the arrival order interleaves the two sets (the spec
    // builder already orders the files that way), because sequential sets
    // merely time-slice a shared path — each finalize renames it away before
    // the neighbour writes — and the corruption needs both sets holding it at
    // once.
    let stem: String = format!("Silver.Horizon.{}", "x".repeat(211));
    let member_a = format!("{stem}-alpha.mkv");
    let member_b = format!("{stem}-omega.mkv");
    let payload_a: Vec<u8> = (0..3000u32).map(|index| (index % 251) as u8).collect();
    let payload_b: Vec<u8> = (0..2500u32).map(|index| (97 + index % 101) as u8).collect();
    let set_a = single_member_store_set(&member_a, &payload_a, 2);
    let set_b = renamed_set(
        "amber.trail",
        single_member_store_set(&member_b, &payload_b, 2),
    );
    let volumes: Vec<(String, Vec<u8>)> = set_a.iter().chain(set_b.iter()).cloned().collect();
    let job_id = JobId(41120);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    let mut volume_file_seen = false;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
        for (filename, _) in &volumes {
            if working_dir.join(filename).exists() {
                volume_file_seen = true;
            }
        }
    }
    assert_eq!(
        pipeline.direct_store.sets_for(job_id).len(),
        2,
        "the fixture must admit two direct sets or it is testing nothing"
    );
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    assert_eq!(
        pipeline.direct_store.finalized_sets, 2,
        "both sets must finalize direct; under a shared partial one set's \
         commit finds its bytes gone or mixed and demotes"
    );
    assert!(!volume_file_seen, "no source volume may ever materialize");
    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    assert_eq!(
        std::fs::read(output_root.join(&member_a)).ok().as_deref(),
        Some(payload_a.as_slice()),
        "set A's member must arrive whole under its own name"
    );
    assert_eq!(
        std::fs::read(output_root.join(&member_b)).ok().as_deref(),
        Some(payload_b.as_slice()),
        "set B's member must arrive whole under its own name"
    );
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "a job whose sets collide only inside the clamp still completes"
    );
}

#[tokio::test]
async fn a_finalized_set_does_not_stop_its_live_neighbour_repairing_while_direct() {
    // Second round: the capability itself, where the first round could only
    // assert the absence of the bug it fixed.
    //
    // Round one fixed the **attribution**. The quiet pass repair runs in front
    // of the repairer was a bare `verify_all`, without the two damage
    // adjustments the authoritative pass applies to its own verdict, so a
    // finalized set's absent volumes all read `Missing`, `damaged_files_by_set`
    // found no live owner for them and refused the whole attempt with
    // `DamageOutsideDirectSets` — and the set that *was* live and *was*
    // repairable demoted instead, for damage belonging to files the job
    // legitimately finished without. After it, the pass reached the right set.
    //
    // It still could not repair it, and that is what this test is now about.
    // PAR2 repair is Reed–Solomon over the whole recovery set: rebuilding one
    // slice reads the surviving slice of *every other file* at the same index,
    // the finalized set's volumes included — and those had no image left, their
    // partials renamed to their destinations and their envelopes deleted, so
    // `execute_repair` failed on the first one it could not open. The two halves
    // were mutually exclusive: stage the finalized set's volumes and the bug
    // round one fixed does not trigger; leave them absent and the neighbour
    // cannot repair. The old assertion was the honest end state of that
    // stalemate — an attempt was made, and it failed.
    //
    // Retention closes it. A set finalizing beside a live neighbour keeps its
    // envelopes and re-points its member extents at the committed destinations
    // — the same bytes, because a commit is a rename — so it stays readable for
    // exactly as long as something in this job can ask.
    let finalized_member = "Silver.Horizon.S01E27.mkv";
    let live_member = "Amber.Trail.S01E01.mkv";
    let finalized_payload: Vec<u8> = (0..2400u32).map(|index| (index % 199) as u8).collect();
    let live_payload: Vec<u8> = (0..3000u32).map(|index| (index % 227) as u8).collect();
    let TwoSetPar2Fixture {
        volumes,
        live_set,
        par2_bytes,
        lost,
    } = two_set_par2_fixture(
        finalized_member,
        live_member,
        &finalized_payload,
        &live_payload,
    );

    // The reference: the same job, the same lost article, the conventional
    // repairer over real volume files. What the direct run has to match.
    let conventional = run_lost_article_gate(
        DirectStoreGate::Disabled,
        JobId(41090),
        live_member,
        &volumes,
        &par2_bytes,
        lost,
        None,
    )
    .await;
    assert_eq!(
        conventional.member.as_deref(),
        Some(live_payload.as_slice()),
        "non-vacuity: the gate-off reference must repair the lost article and \
         extract the damaged set's member, or there is nothing to be identical \
         to; status={:?}",
        conventional.status
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41091);
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    let working_dir =
        direct_job_with_one_finalized_neighbour(&mut pipeline, job_id, &volumes, &par2_bytes, lost)
            .await;
    assert!(
        direct_envelopes_left(&working_dir) > 0,
        "non-vacuity for the retention itself: the finalized set must still own \
         its envelopes here, or the repair below is reading nothing it would not \
         have had anyway"
    );
    assert_eq!(
        std::fs::read(payload_root(&temp_dir, job_id).join(finalized_member)).ok(),
        Some(finalized_payload.clone()),
        "and its member must be committed at its destination — the staging root, \
         not the working directory — because that is what the retained image \
         reads the member extents back out of"
    );

    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");
    let resolution = pipeline
        .resolve_direct_sets_before_par2_repairer(job_id, par2_set, working_dir.clone())
        .await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        matches!(
            resolution,
            crate::pipeline::direct_store::wiring::DirectPar2Resolution::Repaired
        ),
        "the live set's damage is its own and its inputs are all readable — the \
         finalized neighbour's through its retained envelopes and committed \
         members — so the repair must succeed in place. `Unresolved` is the old \
         stalemate: attributed correctly, then `execute_repair` failing on a \
         source volume nothing could open. got {resolution:?}; sets = {sets}"
    );
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .all(|set| !set.is_finalized() || !set.repair_attempted()),
        "and the finalized set is never the one repaired: it is a repair \
         *source*, never a target — nothing may write into a set whose members \
         are already committed. sets = {sets}"
    );
    assert!(
        live_set
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "nothing may materialize under a live volume's own name: that path \
         belongs to demotion"
    );

    // From here it is an ordinary finish: the repaired set re-verifies, its
    // member gate re-arms, it finalizes, and the job completes.
    let mut envelopes_when_all_terminal = None;
    for _ in 0..48 {
        if matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        drain_rar_refreshes(&mut pipeline).await;
        pipeline.check_job_completion(job_id).await;
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
        // Sampled at the first moment both sets are committed, which is where
        // the job's PAR2 story concludes and the retention window shuts. Read
        // here rather than after the loop because a completed job's working
        // directory is cleaned up, and an assertion over a directory that is not
        // there passes for the wrong reason.
        if envelopes_when_all_terminal.is_none() && pipeline.direct_store.finalized_sets >= 2 {
            envelopes_when_all_terminal = Some(direct_envelopes_left(&working_dir));
        }
    }

    let status = job_status_for_assert(&pipeline, job_id);
    let (member, from) = member_after_gate(&complete_dir, &working_dir, live_member);
    assert_eq!(
        member.as_deref(),
        conventional.member.as_deref(),
        "the repaired member must be byte-identical to the gate-off run over the \
         same damage; status={status:?} found_in={from:?}"
    );
    assert_eq!(
        member_after_gate(&complete_dir, &working_dir, finalized_member).0,
        Some(finalized_payload),
        "and the finalized neighbour's own output must be untouched by having \
         been read as a repair source"
    );
    assert_eq!(
        envelopes_when_all_terminal,
        Some(0),
        "once both sets are committed nothing in this job can ask for a virtual \
         volume again, so every retained envelope must be gone — promptly, and \
         without waiting for the working directory to be cleaned up"
    );
    assert_eq!(
        direct_scratch_left(&working_dir),
        0,
        "and the repair scratch dies with the repair, as it always has"
    );
}

#[tokio::test]
async fn a_job_that_dies_inside_the_retention_window_sweeps_its_envelopes_on_restart() {
    // Retention is in-memory bookkeeping over files on disk, and nothing about
    // it is persisted — deliberately, because there is nothing a restart could
    // do with it. A finalized set retires its checkpoint row, so restore
    // rebuilds it fresh, claims none of its envelopes, and the orphan sweep
    // takes every one of them. The point of the test is that deferring the
    // delete did not quietly move an envelope out of the sweep's reach.
    let finalized_member = "Silver.Horizon.S01E28.mkv";
    let live_member = "Amber.Trail.S01E02.mkv";
    let finalized_payload: Vec<u8> = (0..2400u32).map(|index| (index % 193) as u8).collect();
    let live_payload: Vec<u8> = (0..3000u32).map(|index| (index % 229) as u8).collect();
    let TwoSetPar2Fixture {
        volumes,
        par2_bytes,
        lost,
        ..
    } = two_set_par2_fixture(
        finalized_member,
        live_member,
        &finalized_payload,
        &live_payload,
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41092);
    let working_dir = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let working_dir = direct_job_with_one_finalized_neighbour(
            &mut pipeline,
            job_id,
            &volumes,
            &par2_bytes,
            lost,
        )
        .await;
        assert!(
            direct_envelopes_left(&working_dir) > 0,
            "non-vacuity: the job must die holding retained envelopes"
        );
        working_dir
    };

    // The restart. Same working directory, same spec, fresh pipeline: exactly
    // what a killed process leaves behind.
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let (spec, _) = par2_bearing_job_spec("Silver Horizon", &volumes, &par2_bytes);
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

    assert_eq!(
        direct_envelopes_left(&working_dir),
        0,
        "a retained envelope is claimed by nothing after a restart — its set's \
         checkpoint row was retired at finalization — so the orphan sweep must \
         delete it rather than leave it for the job's whole second life"
    );
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .all(|set| set.retained_volumes().is_none()),
        "and no restored set may believe it is holding an image: retention is \
         never restored, because there is nothing on disk left to serve"
    );
}

#[tokio::test]
async fn an_encrypted_finalized_set_keeps_a_retained_image_that_reads_back_as_posted() {
    // The retention path, over cipher. Encrypted sets were once refused
    // a retained image outright: its destinations hold **plaintext** while PAR2
    // describes the posted **cipher**, so an image assembled from the committed
    // members answered every read that crossed a member with the wrong bytes.
    //
    // The re-encrypting overlay is what makes that image honest, and a commit is
    // a rename, so the committed member is byte-for-byte the partial the live
    // image read. The assertion is therefore not "it retained something" but
    // that the retained image reads back **as the source volumes were posted** —
    // which is the only property the neighbour's repair depends on.
    let encrypted_member = "Silver.Horizon.S02E18.mkv";
    let live_member = "Amber.Trail.S01E03.mkv";
    let encrypted_payload: Vec<u8> = (0..2400u32).map(|index| (index % 191) as u8).collect();
    let live_payload: Vec<u8> = (0..3000u32).map(|index| (index % 233) as u8).collect();
    let encrypted_set = encrypted_store_set(
        encrypted_member,
        &encrypted_payload,
        2,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );
    let live_set = renamed_set(
        "amber.trail",
        recovery_record_store_set(live_member, &live_payload, 3, 256),
    );
    let volumes: Vec<(String, Vec<u8>)> = encrypted_set
        .iter()
        .chain(live_set.iter())
        .cloned()
        .collect();
    let par2_bytes = repairable_par2_index(&volumes, 16);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41093);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let (mut spec, _index_file_index) =
        par2_bearing_job_spec("Silver Horizon", &volumes, &par2_bytes);
    spec.password = Some("moonlit-harbour".to_string());
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // The encrypted set arrives whole, so it is the one that finalizes. Its
    // neighbour gets each volume's first article only: enough to open an
    // envelope of its own — which the last assertion reads as a control — and
    // far from enough to finalize alongside.
    for (file_index, segment_number) in in_order_arrivals(encrypted_set.len()) {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    for file_index in encrypted_set.len() as u32..volumes.len() as u32 {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, 0).await;
    }
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .any(|set| !set.is_demoted() && set.router.routes_encrypted()),
        "non-vacuity: the set under test must really be decrypting at write time"
    );

    pipeline.par2_verified.insert(job_id);
    pipeline.finalize_ready_direct_sets(job_id).await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    let encrypted_index = pipeline
        .direct_store
        .sets_for(job_id)
        .iter()
        .position(|set| set.router.routes_encrypted())
        .unwrap_or_else(|| panic!("the encrypted set must still be there; got {sets}"));
    assert!(
        pipeline
            .direct_store
            .set(job_id, encrypted_index)
            .is_some_and(|set| set.is_finalized()),
        "non-vacuity: the encrypted set must have finalized beside a live \
         neighbour, which is the only state the refusal has anything to do in; \
         got {sets}"
    );
    assert_eq!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .filter(|set| !set.is_finalized() && !set.is_demoted())
            .count(),
        1,
        "and its neighbour must still be live, or the refusal is indistinguishable \
         from the ordinary no-neighbour delete; got {sets}"
    );

    let retained = pipeline
        .direct_store
        .set(job_id, encrypted_index)
        .and_then(|set| set.retained_volumes().map(<[_]>::to_vec))
        .unwrap_or_else(|| {
            panic!("an encrypted set the overlay can serve must keep its image; got {sets}")
        });
    let envelopes = pipeline
        .direct_store
        .set(job_id, encrypted_index)
        .expect("the encrypted set is there")
        .plan()
        .envelope_paths();
    assert!(
        envelopes.iter().any(|envelope| envelope.exists()),
        "a retained image reads through the envelopes, so they must outlive \
         finalization with it"
    );
    assert!(
        direct_envelopes_left(&working_dir) > 0,
        "non-vacuity for that check"
    );

    // The property the whole thing exists for: the image answers in **posted**
    // space. Reading it back must reproduce the encrypted source volumes byte
    // for byte, even though the bytes it is assembled from are the decrypted
    // member at its committed destination plus the envelopes.
    let provider = crate::pipeline::direct_store::provider::HybridVolumeProvider::new(retained);
    for (ordinal, (_, posted)) in encrypted_set.iter().enumerate() {
        let mut reader = provider
            .open(ordinal as u32)
            .expect("every retained volume is registered");
        let mut read_back = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut read_back).unwrap();
        assert_eq!(
            &read_back, posted,
            "retained encrypted volume {ordinal} must read back as it was posted"
        );
    }
    assert!(
        provider.cipher_counters().reencrypted_bytes() > 0,
        "non-vacuity: the read above must really have gone through the overlay \
         rather than past an empty cipher map"
    );
}

#[tokio::test]
async fn a_lost_article_inside_a_member_repairs_and_reconfirms_the_volume() {
    // Two things at once, because one article carries both.
    //
    // The lost article is the **second half** of the middle volume: member
    // payload, the recovery record, and the end-of-archive header. So the repair
    // has to route bytes back into a `.direct.partial` at a range the member's
    // coverage map never held — re-arming a whole-member gate that could not
    // previously fire — *and* into the envelope, where the restored end record
    // is what lets the header walk finish and confirm a volume that had no proof
    // no further header could appear.
    let member_name = "Silver.Horizon.S01E23.mkv";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 223) as u8).collect();
    let rr_bytes = 256;
    let volumes = recovery_record_store_set(member_name, &payload, 3, rr_bytes);
    let par2_bytes = repairable_par2_index(&volumes, 12);

    let conventional = run_lost_article_gate(
        DirectStoreGate::Disabled,
        JobId(41071),
        member_name,
        &volumes,
        &par2_bytes,
        (1, 1),
        None,
    )
    .await;
    let direct = run_lost_article_gate(
        DirectStoreGate::Enabled,
        JobId(41072),
        member_name,
        &volumes,
        &par2_bytes,
        (1, 1),
        None,
    )
    .await;

    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the gate-off reference must repair the lost article and extract the \
         member; status={:?}",
        conventional.status
    );
    assert_eq!(
        direct.member.as_deref(),
        conventional.member.as_deref(),
        "a direct set must repair a hole in a member's packed range in place and \
         produce the gate-off bytes; sets = {}",
        direct.sets
    );
    assert!(
        !direct.sets.contains("Demoted"),
        "and it must not demote to do it, got {}",
        direct.sets
    );
    assert_eq!(
        direct.finalized, 1,
        "a volume whose end record arrived only in the repaired bytes must still \
         confirm, or the set could never finalize; sets = {}",
        direct.sets
    );
    assert!(
        !direct.volume_file_seen,
        "no volume file, repaired or otherwise"
    );
    assert_eq!(
        direct.materialized, 1,
        "only the volume that lost an article materializes"
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
async fn a_lost_article_inside_an_encrypted_member_repairs_and_reconfirms_the_volume() {
    // The encrypted twin of `a_lost_article_inside_a_member_repairs_and_
    // reconfirms_the_volume`, with the same assertions. It used to pin a known
    // limit instead: the cipher block straddling the hole's edge is held —
    // its other half is in the article that never came — so the volume's
    // placed run stopped just short of an article boundary, the sweep had no
    // composed reference for it, materialization was refused, and the set
    // demoted for a hole PAR2 could have filled.
    //
    // Two things closed that. The virtual volume now serves holds straight
    // from staging as the posted bytes they are, so the edge blocks on both
    // sides of the hole read back and the composition reaches the article
    // boundary. And a decrypted run files its own CBC predecessor as a
    // checkpoint, so the run that begins right after the hole re-encrypts
    // from its own start instead of chaining through plaintext the hole never
    // delivered.
    let member_name = "Silver.Horizon.S01E25.mkv";
    let password = "moonlit-harbour";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 223) as u8).collect();
    let volumes = encrypted_store_set_with_recovery(
        member_name,
        &payload,
        3,
        password,
        Some(password),
        true,
        256,
    );
    let par2_bytes = repairable_par2_index(&volumes, 12);

    let conventional = run_lost_article_gate(
        DirectStoreGate::Disabled,
        JobId(41081),
        member_name,
        &volumes,
        &par2_bytes,
        (1, 1),
        Some(password),
    )
    .await;
    let direct = run_lost_article_gate(
        DirectStoreGate::Enabled,
        JobId(41082),
        member_name,
        &volumes,
        &par2_bytes,
        (1, 1),
        Some(password),
    )
    .await;

    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the gate-off reference must repair the lost article and extract the \
         member; status={:?}",
        conventional.status
    );
    assert_eq!(
        direct.member.as_deref(),
        conventional.member.as_deref(),
        "an encrypted direct set must repair a hole in a member's packed range in \
         place and produce the gate-off bytes; sets = {}",
        direct.sets
    );
    assert!(
        !direct.sets.contains("Demoted"),
        "and it must not demote to do it, got {}",
        direct.sets
    );
    assert_eq!(
        direct.finalized, 1,
        "a volume whose end record arrived only in the repaired bytes must still \
         confirm, or the set could never finalize; sets = {}",
        direct.sets
    );
    assert!(
        !direct.volume_file_seen,
        "no volume file, repaired or otherwise"
    );
    assert_eq!(
        direct.materialized, 1,
        "only the volume that lost an article materializes"
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
async fn an_interior_hole_sizes_the_repair_by_the_slices_it_actually_touches() {
    // The wave-2 review note, priced. The middle article of the middle volume
    // is lost, so the volume has an interior hole with healthy bytes on both
    // sides. Before repair landed the verifier's sequential sweep stopped at
    // that hole and called every slice after it damaged; a repair sized from
    // that count spends a recovery block per slice, and the ones past the hole
    // are blocks spent rebuilding bytes that were never broken. Enough of them
    // and a repairable set reads as unrepairable.
    const ARTICLES: usize = 3;
    let member_name = "Silver.Horizon.S01E24.mkv";
    let payload: Vec<u8> = (0..6000u32).map(|index| (index % 229) as u8).collect();
    let volumes = recovery_record_store_set(member_name, &payload, 3, 256);
    let par2_bytes = repairable_par2_index(&volumes, 64);

    let volume_len = volumes[1].1.len();
    let (hole_start, hole_end) = article_extent(volume_len, 1, ARTICLES);
    // Slices the hole overlaps, and slices from the hole to the volume's end —
    // the honest count and the inflated one. The fixture is only interesting
    // while the two differ.
    let slice = PAR2_SLICE_BYTES as usize;
    let touched = hole_end.div_ceil(slice) - hole_start / slice;
    let to_the_end = volume_len.div_ceil(slice) - hole_start / slice;
    assert!(
        touched < to_the_end,
        "the fixture must have healthy slices after the hole, or the accounting \
         fix is untestable: touched={touched} to_the_end={to_the_end}"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(41081);
    let (spec, index_file_index) =
        par2_bearing_job_spec_with_articles("Silver Horizon", &volumes, &par2_bytes, ARTICLES);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for file_index in 0..volumes.len() as u32 {
        for segment_number in 0..ARTICLES as u32 {
            if (file_index, segment_number) == (1, 1) {
                continue;
            }
            submit_volume_article_of(
                &mut pipeline,
                job_id,
                &volumes,
                file_index,
                segment_number,
                ARTICLES,
            )
            .await;
        }
    }
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
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    let mut sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    for _ in 0..48 {
        if matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        drain_rar_refreshes(&mut pipeline).await;
        pipeline.check_job_completion(job_id).await;
        sample_direct_sets(&pipeline, job_id, &mut sets);
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
        sample_direct_sets(&pipeline, job_id, &mut sets);
    }

    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    assert_eq!(
        std::fs::read(output_root.join(member_name)).ok().as_deref(),
        Some(payload.as_slice()),
        "the interior hole must repair and the member must come out whole; \
         sets = {sets}"
    );
    assert_eq!(
        pipeline.direct_store.repair_recovery_blocks_used, touched,
        "the repair must spend one recovery block per slice the hole actually \
         touches ({touched}), not one per slice from the hole to the end of the \
         volume ({to_the_end}); sets = {sets}"
    );
    assert_eq!(pipeline.direct_store.repair_materialized_volumes, 1);
    assert_eq!(direct_scratch_left(&working_dir), 0);
    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "and no volume file may exist at the end of it"
    );
}

// ---------------------------------------------------------------------------
// The config surface, and sparse marking
// ---------------------------------------------------------------------------

/// Every other test here reaches for `set_gate`. This one comes through
/// configuration, which is what the operator surface exposes: the
/// `[direct_store]` table turns routing on, and turning it **off** at startup
/// makes a restart ignore and sweep the mid-flight direct state and redownload
/// the job conventionally.
#[tokio::test]
async fn the_config_gate_routes_and_a_config_off_restart_sweeps_and_redownloads() {
    use crate::settings::DirectStoreOverrides;

    // `DirectStoreSettings::resolve` reads the real environment, and the env
    // override deliberately beats config. A developer with the variable
    // exported would be testing the override, not the config.
    if crate::pipeline::direct_store::env_override().is_some() {
        return;
    }

    const ARTICLES: usize = 2;
    let member_name = "Silver.Horizon.S01E52.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 239) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41090);
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1), (1, 0)];

    // First run: config on. The job routes, so partials, envelopes and a
    // coverage row exist — the non-vacuity the sweep assertions below depend
    // on.
    let working_dir = {
        let (mut pipeline, _, _) = new_config_gated_direct_pipeline(
            &temp_dir,
            DirectStoreOverrides {
                enabled: Some(true),
                ..Default::default()
            },
        )
        .await;
        let spec = direct_store_job_spec_with_articles("Silver Horizon", &volumes, ARTICLES);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
        for (file_index, segment_number) in &arrivals {
            submit_volume_article_of(
                &mut pipeline,
                job_id,
                &volumes,
                *file_index,
                *segment_number,
                ARTICLES,
            )
            .await;
        }
        pipeline
            .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
            .await;
        working_dir
    };

    let partial = direct_partial(&temp_dir, JobId(41090), member_name);
    let envelope = working_dir.join("silver.horizon.f0.vol00000.envelope");
    assert!(
        partial.exists() && envelope.exists(),
        "the config table must be able to turn routing on at all"
    );
    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "and a config-gated direct job writes no source volume either"
    );

    // Second run: config off. Same working directory, same spec, a fresh
    // pipeline.
    let (mut pipeline, _, _) = new_config_gated_direct_pipeline(
        &temp_dir,
        DirectStoreOverrides {
            enabled: Some(false),
            ..Default::default()
        },
    )
    .await;
    let rows_before = pipeline.db.load_direct_coverage(job_id).unwrap();
    assert!(!rows_before.is_empty(), "phase 1 must have checkpointed");
    let spec = direct_store_job_spec_with_articles("Silver Horizon", &volumes, ARTICLES);
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
    assert_eq!(
        queued.len(),
        volumes.len() * ARTICLES,
        "a config-disabled gate must redownload the whole job conventionally, got {queued:?}"
    );
    assert!(
        !partial.exists(),
        "mid-flight direct partials must be swept when config turns the gate off"
    );
    assert!(!envelope.exists(), "and so must their envelopes");
    assert!(
        pipeline.direct_store.sets_for(job_id).is_empty(),
        "and no direct set may survive the restore"
    );
    // Ignored, not deleted: a re-enabled binary can still judge the rows, and it
    // refuses them on the destination probe.
    assert!(
        !pipeline.db.load_direct_coverage(job_id).unwrap().is_empty(),
        "a disabled gate must not destroy coverage a re-enabled one could judge"
    );
}

/// A destination that cannot be marked sparse demotes the set, and the refusal
/// happens before the file holds a hole — so nothing it created is left on
/// disk.
#[tokio::test]
async fn a_destination_that_cannot_be_marked_sparse_demotes_before_it_holds_a_hole() {
    use crate::pipeline::direct_store::sparse::SparseMarking;

    let member_name = "Silver.Horizon.S01E53.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 151) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline
        .direct_store
        .set_sparse_marking(SparseMarking::AlwaysFail);
    let job_id = JobId(41091);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(SparseMarkFailed)"),
        "a destination that cannot be marked sparse must demote with its own reason, got {shape}"
    );
    assert!(
        !direct_partial(&temp_dir, JobId(41091), member_name).exists(),
        "the refused destination must not be left behind"
    );
    assert!(
        !working_dir
            .join("silver.horizon.f0.vol00000.envelope")
            .exists(),
        "nor the envelope the same batch would have created"
    );
}

#[tokio::test]
async fn direct_repair_placement_preserves_verified_output_on_sparse_failure() {
    failed_direct_repair_placement(true).await;
}

#[tokio::test]
async fn direct_repair_placement_preserves_verified_output_on_write_failure() {
    failed_direct_repair_placement(false).await;
}

async fn failed_direct_repair_placement(sparse_failure: bool) {
    use crate::pipeline::direct_store::sparse::SparseMarking;
    use crate::pipeline::direct_store::wiring::DirectPlacementError;

    let member_name = "Silver.Horizon.S01E53.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 151) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(41103);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;

    // The native repair has installed only this volume. A failed reroute must
    // return control without starting a reconstruction over its verified image.
    let installed = working_dir.join(&volumes[1].0);
    std::fs::write(&installed, &volumes[1].1).unwrap();
    let set = pipeline.direct_store.set_mut(job_id, 0).unwrap();
    let before = set.volume_coverage(1);
    let clean_coverage = set.volume_coverage(0);
    let envelope = working_dir.join(set.plan().envelope_relative_path(1));
    let spans = set.router.route(1, 0, &volumes[1].1).unwrap();
    assert!(!spans.is_empty());
    if sparse_failure {
        pipeline
            .direct_store
            .set_sparse_marking(SparseMarking::AlwaysFail);
    } else {
        std::fs::create_dir(&envelope).unwrap();
    }
    let failure = pipeline
        .try_place_direct_spans(job_id, 0, &spans)
        .await
        .unwrap_err();
    match failure {
        DirectPlacementError::Sparse { path, error } => {
            assert!(sparse_failure);
            assert_eq!(path, envelope);
            assert!(error.to_string().contains("test injection"));
        }
        DirectPlacementError::Write(error) => {
            assert!(!sparse_failure);
            assert!(
                error.raw_os_error().is_some(),
                "the original OS error must survive"
            );
        }
    }
    let set = pipeline.direct_store.set(job_id, 0).unwrap();
    assert!(!set.is_demoted());
    assert_eq!(set.volume_coverage(1), before);
    assert_eq!(set.volume_coverage(0), clean_coverage);
    assert!(!pipeline.direct_demotion_in_flight.contains_key(&job_id));
    assert_eq!(std::fs::read(installed).unwrap(), volumes[1].1);
    assert!(!working_dir.join(&volumes[0].0).exists());
}

/// The same rule for the holds scratch, which the router creates itself rather
/// than through the destination-preparation seam.
#[tokio::test]
async fn a_holds_scratch_that_cannot_be_marked_sparse_demotes_the_set() {
    use crate::pipeline::direct_store::sparse::SparseMarking;

    let member_name = "Silver.Horizon.S01E54.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 149) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(64);
    pipeline
        .direct_store
        .set_sparse_marking(SparseMarking::AlwaysFail);
    let job_id = JobId(41092);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // Article 1 of volume 0 lands before its header, so it has to be held; the
    // 64-byte budget forces the very first hold to page.
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(HoldsScratchFailed)"),
        "an unmarkable scratch must demote with the scratch's own reason, got {shape}"
    );
    let scratch_files: Vec<String> = std::fs::read_dir(&working_dir)
        .unwrap()
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .filter(|name| name.starts_with(".weaver-holds."))
        .collect();
    assert!(
        scratch_files.is_empty(),
        "the scratch it could not mark must not survive, got {scratch_files:?}"
    );
}

// ---------------------------------------------------------------------------
// Encrypted direct-store
//
// The spine is the same differential established earlier, with one input added:
// the identical job is run with routing on and off *with the same password*,
// and the outputs must be byte-identical. With routing on, no source volume may
// ever appear on disk — which for an encrypted set also means the ciphertext
// never does.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_encrypted_store_set_routes_plaintext_and_matches_the_conventional_extractor() {
    let member_name = "Silver.Horizon.S02E01.mkv";
    // 3000 is not a multiple of 16, so `cipher_size` is 3008 and the member
    // carries 8 bytes of tail padding: the final block decrypts to plaintext
    // that runs past the member's declared end, and none of it may reach the
    // destination.
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 251) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        4,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );
    let arrivals = in_order_arrivals(volumes.len());

    let conventional = run_gate_with_password(
        DirectStoreGate::Disabled,
        None,
        None,
        JobId(43001),
        member_name,
        &volumes,
        &arrivals,
        Some("moonlit-harbour"),
    )
    .await;
    let direct = run_gate_with_password(
        DirectStoreGate::Enabled,
        None,
        None,
        JobId(43002),
        member_name,
        &volumes,
        &arrivals,
        Some("moonlit-harbour"),
    )
    .await;

    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written the encrypted source volumes"
    );
    assert!(
        !direct.volume_file_seen,
        "direct routing must never create a source volume file, encrypted or not"
    );
    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional extractor should decrypt the member with the job's password"
    );
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "the routed member must be plaintext at its final offsets, with no tail padding"
    );
    assert_eq!(
        (direct.member, direct.member_location, direct.status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "an encrypted set routed plaintext-once must be byte-identical to the conventional \
         extractor with the same password"
    );
}

#[tokio::test]
async fn a_wrong_password_with_a_check_present_never_routes_a_byte() {
    let member_name = "Silver.Horizon.S02E02.mkv";
    let payload: Vec<u8> = (0..2048u32).map(|index| (index % 199) as u8).collect();
    // The header states the check for the *right* password; the job holds the
    // wrong one, so admission refutes it before a single byte is decrypted.
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );
    let arrivals = in_order_arrivals(volumes.len());

    let refused =
        encrypted_routing_outcome(JobId(43011), &volumes, &arrivals, Some("wrong-key")).await;
    assert!(
        refused
            .shape
            .contains("Demoted(EncryptedMemberRefused(WrongPassword))"),
        "a refuted password must be refused at admission, before any byte routes, got {}",
        refused.shape
    );
    assert!(
        !refused.partial_seen,
        "nothing may be written on the strength of a refuted password"
    );
    assert!(
        refused.volume_file_seen,
        "the demotion must hand the volumes to the conventional path, not drop them — which is \
         the parity a wrong password gets: it fails there too, for the same reason"
    );

    // Non-vacuity: the identical set with the right password routes, so the
    // refusal above is a decision about the password and not about the fixture.
    let admitted =
        encrypted_routing_outcome(JobId(43012), &volumes, &arrivals, Some("moonlit-harbour")).await;
    assert!(
        !admitted.shape.contains("Demoted"),
        "the same set with the right password must route, got {}",
        admitted.shape
    );
    assert!(
        !admitted.volume_file_seen,
        "the admitted set must never materialize a source volume"
    );
}

#[tokio::test]
async fn a_wrong_password_with_no_check_routes_until_the_keyed_member_gate_catches_it() {
    let member_name = "Silver.Horizon.S02E03.mkv";
    let payload: Vec<u8> = (0..2100u32).map(|index| (index % 211) as u8).collect();
    // No password-check value in the header at all, so admission can conclude
    // nothing and routes provisionally. The whole-member checksum is keyed,
    // which makes it the only thing that can catch the wrong password: layer 1's
    // packed hashes cover cipher bytes and pass whatever key was used, so they
    // are wire integrity and not a password test.
    let volumes = encrypted_store_set(member_name, &payload, 3, "moonlit-harbour", None, true);
    let arrivals = in_order_arrivals(volumes.len());

    let caught =
        encrypted_routing_outcome(JobId(43021), &volumes, &arrivals, Some("wrong-key")).await;
    assert!(
        caught.shape.contains("Demoted(MemberChecksumMismatch)"),
        "a wrong password the header could not refute must be caught by the keyed member gate, \
         got {}",
        caught.shape
    );
    // Deliberately no volume-file assertion here, unlike the check-present
    // case: this gate fires on the *last* article, when the member's plaintext
    // first composes, so the handover has no further articles to ride and the
    // materialization is the demotion machinery's own — already pinned by
    // `a_demoted_set_materializes_its_covered_volumes_instead_of_refetching_them`.

    // Non-vacuity, and the keyed-fold claim itself: the identical fixture with
    // the right password verifies through the same keyed fold and completes.
    let clean = run_gate_with_password(
        DirectStoreGate::Enabled,
        None,
        None,
        JobId(43022),
        member_name,
        &volumes,
        &arrivals,
        Some("moonlit-harbour"),
    )
    .await;
    assert_eq!(
        clean.member.as_deref(),
        Some(payload.as_slice()),
        "a check-less encrypted member with the right password must still verify and complete"
    );
}

#[tokio::test]
async fn an_encrypted_set_with_no_password_demotes_instead_of_routing_ciphertext() {
    let member_name = "Silver.Horizon.S02E04.mkv";
    let payload: Vec<u8> = (0..1500u32).map(|index| (index % 181) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );
    let arrivals = in_order_arrivals(volumes.len());

    // Checklist site 2, as a test: `EncryptedStore` is not `Ineligible`, so the
    // predicate this replaced counted every encrypted member routable and would
    // have sailed past the `routable == 0` demotion with nothing to route.
    let outcome = encrypted_routing_outcome(JobId(43031), &volumes, &arrivals, None).await;
    assert!(
        outcome
            .shape
            .contains("Demoted(EncryptedMemberRefused(NoPassword))"),
        "an encrypted set with no password must demote, by name, got {}",
        outcome.shape
    );
    assert!(
        !outcome.partial_seen,
        "a set with no key must not create a destination for ciphertext"
    );
}

#[tokio::test]
async fn an_encrypted_span_that_arrives_before_its_predecessor_block_is_held_then_drained() {
    let member_name = "Silver.Horizon.S02E05.mkv";
    let payload: Vec<u8> = (0..2600u32).map(|index| (index % 173) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );

    // Every volume's header article first, ascending, then the payload halves in
    // reverse. The headers alone complete the chain, so the drain runs for every
    // volume while each one's *predecessor* bytes are still outstanding: volume
    // n's part starts at a non-block-aligned cipher offset, so its first block's
    // 16 preceding bytes live in the tail of volume n-1 — which has not arrived.
    // Every part boundary is exercised in both directions, and the reverse
    // payload order means the holds are released from the far end back.
    let mut arrivals: Vec<(u32, u32)> = (0..volumes.len() as u32).map(|index| (index, 0)).collect();
    arrivals.extend((0..volumes.len() as u32).rev().map(|index| (index, 1)));

    let conventional = run_gate_with_password(
        DirectStoreGate::Disabled,
        None,
        None,
        JobId(43041),
        member_name,
        &volumes,
        &arrivals,
        Some("moonlit-harbour"),
    )
    .await;

    // The direct side is driven inline rather than through the gate helper, for
    // one assertion the helper cannot make: byte-identical output proves the
    // drain produced the right bytes, but it cannot tell a set that *held* a
    // straddling block from one whose arrival order never made it hold. The
    // counter is that difference, and without it this test would pass just as
    // happily against a build that had deleted the edge-hold path.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(43042);
    let mut spec = direct_store_job_spec("Silver Horizon", &volumes);
    spec.password = Some("moonlit-harbour".to_string());
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in &arrivals {
        submit_volume_article(
            &mut pipeline,
            job_id,
            &volumes,
            *file_index,
            *segment_number,
        )
        .await;
    }

    let blocks_held = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the encrypted set must still be routing")
        .router
        .blocks_held();
    assert!(
        blocks_held > 0,
        "this arrival order must have made the drain hold a cipher block for a \
         predecessor it did not have yet; it held {blocks_held}"
    );

    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
    let (member, member_location) = member_after_gate(&complete_dir, &working_dir, member_name);
    let status = job_status_for_assert(&pipeline, job_id);

    assert_eq!(
        member.as_deref(),
        Some(payload.as_slice()),
        "a held straddling block must drain to the same plaintext once its other half lands"
    );
    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "holding a cipher block must not fall back to writing the volume"
    );
    assert_eq!(
        (member, member_location, status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "out-of-order encrypted arrival must be byte-identical to the conventional extractor"
    );
}

#[tokio::test]
async fn a_forged_password_check_admits_and_the_keyed_member_gate_catches_it_anyway() {
    // The forgeable-check risk, as a test. The RAR5 password check is 8
    // unauthenticated bytes a writer chooses, so a hostile archive can carry the
    // check for a password that does **not** decrypt its data and have admission
    // report `Verified`. That is why the check is an admission *test* and never a
    // reason to skip the keyed member gate: the gate is the authority.
    let member_name = "Silver.Horizon.S02E14.mkv";
    let payload: Vec<u8> = (0..2100u32).map(|index| (index % 211) as u8).collect();
    // Data encrypted with one password, the header's check forged for another —
    // and the job holds the forged one.
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("wrong-key"),
        true,
    );
    let arrivals = in_order_arrivals(volumes.len());

    let caught =
        encrypted_routing_outcome(JobId(43131), &volumes, &arrivals, Some("wrong-key")).await;
    assert!(
        !caught.shape.contains("EncryptedMemberRefused"),
        "the forged check must have *passed* admission — otherwise this test proves \
         nothing about the gate behind it, got {}",
        caught.shape
    );
    assert!(
        caught.shape.contains("Demoted(MemberChecksumMismatch)"),
        "the keyed whole-member fold is the authority a forged check cannot reach, got {}",
        caught.shape
    );
    // Deliberately no volume-file assertion, for the same reason
    // `a_wrong_password_with_no_check_routes_until_the_keyed_member_gate_catches_it`
    // has none: this gate fires on the *last* article, when the member's
    // plaintext first composes, so the handover has no further articles to ride
    // and the materialization is the demotion machinery's own.
    assert!(
        !caught.partial_seen,
        "the refused member's destination must not be left on disk for the \
         conventional extractor to find"
    );
}

#[tokio::test]
async fn a_duplicate_encrypted_article_advances_nothing_twice() {
    let member_name = "Silver.Horizon.S02E06.mkv";
    let payload: Vec<u8> = (0..1900u32).map(|index| (index % 167) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );

    // Every article twice. A duplicate that re-entered the cipher composition
    // would fold the same run into layer 1 twice and fail the part gate; one
    // that re-entered layer 2 would do the same to the keyed member fold.
    let mut arrivals = Vec::new();
    for file_index in 0..volumes.len() as u32 {
        for segment_number in 0..2u32 {
            arrivals.push((file_index, segment_number));
            arrivals.push((file_index, segment_number));
        }
    }

    let direct = run_gate_with_password(
        DirectStoreGate::Enabled,
        None,
        None,
        JobId(43051),
        member_name,
        &volumes,
        &arrivals,
        Some("moonlit-harbour"),
    )
    .await;
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "duplicate encrypted articles must leave the member exactly once-written"
    );
    assert!(!direct.volume_file_seen);
}

#[tokio::test]
async fn an_encrypted_member_whose_size_is_block_aligned_carries_no_padding() {
    // The other side of the tail-padding case: `unpacked_size % 16 == 0`, so
    // `cipher_size == unpacked_size`, no padding exists and no envelope span is
    // emitted for one. Byte-identical output either way is the assertion —
    // padding arithmetic that was off by a block would show up here as a short
    // member or an over-long one.
    let member_name = "Silver.Horizon.S02E07.mkv";
    let payload: Vec<u8> = (0..2048u32).map(|index| (index % 149) as u8).collect();
    assert_eq!(payload.len() % 16, 0);
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );
    let arrivals = in_order_arrivals(volumes.len());

    let direct = run_gate_with_password(
        DirectStoreGate::Enabled,
        None,
        None,
        JobId(43061),
        member_name,
        &volumes,
        &arrivals,
        Some("moonlit-harbour"),
    )
    .await;
    assert_eq!(direct.member.as_deref(), Some(payload.as_slice()));
    assert!(!direct.volume_file_seen);
}

#[tokio::test]
async fn a_password_that_arrives_before_the_first_article_still_admits_the_set() {
    // Named for what it actually proves. weaver *does* support setting a
    // password after add (`setJobPassword`, and the NZBGet facade's
    // `*Unpack:Password`), both of which mutate the live job spec, and the
    // direct sets are built once per job and memoized, so the seam re-reads the
    // spec while any set is still willing to take one.
    //
    // The window is **pre-first-article**, not "any time during the download":
    // admission runs from the first successful header parse, and its
    // `NoPassword` refusal is a demotion. A password arriving after that finds
    // the set already on the conventional path, which asks the job's whole
    // candidate list anyway. Waiting for one instead would mean holding every
    // arriving byte for a set that will most likely never get a password, and
    // then throwing all of it away on a scratch-ceiling breach.
    let member_name = "Silver.Horizon.S02E08.mkv";
    let payload: Vec<u8> = (0..1700u32).map(|index| (index % 157) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(43071);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // The set exists and has taken no password yet — nothing has parsed, so no
    // encrypted member has been classified and nothing has demoted. Reached
    // through the routing seam, which is what admits the sets lazily.
    assert!(
        pipeline
            .direct_route_target(crate::jobs::ids::NzbFileId {
                job_id,
                file_index: 0,
            })
            .is_some(),
        "the set should be admitted and still routing before any password exists"
    );
    assert!(
        pipeline
            .direct_store
            .sets_for(job_id)
            .iter()
            .all(|set| !set.is_demoted()),
        "a set with no password must not demote before it has classified anything"
    );

    // The post-add write both API surfaces perform.
    pipeline
        .jobs
        .get_mut(&job_id)
        .expect("the job is live")
        .spec
        .password = Some("moonlit-harbour".to_string());

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    assert_eq!(
        std::fs::read(output_root.join(member_name)).ok().as_deref(),
        Some(payload.as_slice()),
        "a password set after add must admit the set and produce the member"
    );
    assert!(
        !working_dir.join(&volumes[0].0).exists(),
        "the set admitted on a post-add password must still route, not materialize"
    );
}

#[tokio::test]
async fn a_password_corrected_before_the_first_article_admits_with_the_correction() {
    // The dead branch, end to end. `KeyRing::set_password` has always handled a
    // password *changing* while nothing is admitted — but the seam that calls
    // it stopped asking the moment any password was held, so a job added with
    // the wrong one and corrected before its first header parsed derived keys
    // from the stale one, admitted on the header's check, and failed the keyed
    // member gate a whole download later.
    let member_name = "Silver.Horizon.S02E16.mkv";
    let payload: Vec<u8> = (0..1700u32).map(|index| (index % 157) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(43141);
    let mut spec = direct_store_job_spec("Silver Horizon", &volumes);
    // Added with a typo.
    spec.password = Some("moonlit-harbor".to_string());
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // The sets are built here, holding the wrong password — which is exactly the
    // state the old window treated as settled.
    assert!(
        pipeline
            .direct_route_target(crate::jobs::ids::NzbFileId {
                job_id,
                file_index: 0,
            })
            .is_some(),
        "the set should be admitted and still routing before anything has parsed"
    );

    // The correction, through the same live-spec write both API surfaces do.
    pipeline
        .jobs
        .get_mut(&job_id)
        .expect("the job is live")
        .spec
        .password = Some("moonlit-harbour".to_string());

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "the correction must be the password admission derives from, got {shape}"
    );

    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
    let (member, location) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        member.as_deref(),
        Some(payload.as_slice()),
        "a password corrected before the first article must produce the member"
    );
    assert_eq!(location, Some("complete"));
    assert!(
        !working_dir.join(&volumes[0].0).exists(),
        "the corrected set must still route, not materialize"
    );
}

#[tokio::test]
async fn an_encrypted_set_restarted_mid_download_honours_its_floors_and_completes_byte_identically()
{
    const ARTICLES: usize = 4;
    let member_name = "Silver.Horizon.S02E09.mkv";
    let payload: Vec<u8> = (0..8000u32).map(|index| (index % 251) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(43081);
    // Volume 0 complete, volume 1 half: one file the restore can skip whole and
    // one it must skip only part of — and the part boundary between them is not
    // 16-aligned, so the resumed run has to decrypt at a coverage frontier whose
    // predecessor cipher block is gone with the process that wrote it.
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1), (0, 2), (0, 3), (1, 0), (1, 1)];
    let working_dir = direct_store_before_restart_with_password(
        &temp_dir,
        job_id,
        &volumes,
        &arrivals,
        ARTICLES,
        Some("moonlit-harbour"),
    )
    .await;

    let mut pipeline = direct_store_after_restart_with_password(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
        Some("moonlit-harbour"),
    )
    .await;

    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the restored encrypted job must carry its direct set");
    assert!(
        !set.is_demoted(),
        "a restart that still has the password must re-admit the set, not demote it"
    );
    assert!(
        set.has_restart_seeded_coverage(),
        "restored coverage is seeded and unverified until it is re-read as plaintext"
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
    assert!(
        queued.len() < volumes.len() * ARTICLES,
        "a restart that refetches everything is not honouring any floor"
    );

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
        "the keyed member gate must have re-read the pre-restart plaintext before finalizing"
    );
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let complete_dir = temp_dir.path().join("complete");
    let (restarted, location) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        restarted.as_deref(),
        Some(payload.as_slice()),
        "a restarted encrypted set must finish byte-identical to an uninterrupted one"
    );
    assert_eq!(location, Some("complete"));
    assert!(
        !working_dir.join(&volumes[0].0).exists(),
        "a restarted encrypted set must still never materialize a source volume"
    );
}

#[tokio::test]
async fn an_encrypted_set_restarted_without_its_password_demotes_by_name() {
    const ARTICLES: usize = 2;
    let member_name = "Silver.Horizon.S02E10.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 239) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(43091);
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1)];
    let working_dir = direct_store_before_restart_with_password(
        &temp_dir,
        job_id,
        &volumes,
        &arrivals,
        ARTICLES,
        Some("moonlit-harbour"),
    )
    .await;

    // The password is never persisted, by design. A restore that cannot supply
    // one has to demote — by name, and while the set's routed bytes can still
    // materialize its volumes — rather than sit unable to decrypt and unable to
    // give up.
    let mut pipeline = direct_store_after_restart_with_password(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
        None,
    )
    .await;

    // The layout rebuild runs the same admission the live parse does, so the
    // checkpoint is refused before it can seed anything. That is the "never
    // wedges" half: no seeded coverage means no member left permanently
    // unverifiable, and every article comes back.
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_none_or(|set| !set.has_restart_seeded_coverage()),
        "a set that cannot decrypt must not be left holding seeded coverage it can never re-arm"
    );
    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert_eq!(
        queued.len(),
        volumes.len() * ARTICLES,
        "a refused checkpoint must skip nothing, got {queued:?}"
    );

    // And the "by name" half: the first article to arrive reaches the same
    // admission decision and demotes under its own reason, rather than routing
    // ciphertext or holding forever.
    submit_volume_article_of(&mut pipeline, job_id, &volumes, 0, 0, ARTICLES).await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(EncryptedMemberRefused(NoPassword))"),
        "a restored encrypted set with no password must demote by name, got {shape}"
    );
}

// ---------------------------------------------------------------------------
// The machinery encryption changes nothing about, asserted rather than
// assumed. Each of these is the encrypted twin of a plaintext guarantee.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn encrypted_routing_leaves_the_plan_135_guarantees_untouched() {
    let member_name = "Silver.Horizon.S02E11.mkv";
    let payload: Vec<u8> = (0..5000u32).map(|index| (index % 227) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(43101);
    let mut spec = direct_store_job_spec("Silver Horizon", &volumes);
    spec.password = Some("moonlit-harbour".to_string());
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // Payload before headers, so the holds machinery carries cipher bytes the
    // layout cannot place yet — the same hold a plaintext set uses, over cipher.
    let mut arrivals: Vec<(u32, u32)> = (0..volumes.len() as u32).map(|index| (index, 1)).collect();
    arrivals.extend((0..volumes.len() as u32).map(|index| (index, 0)));
    for (file_index, segment_number) in &arrivals {
        submit_volume_article(
            &mut pipeline,
            job_id,
            &volumes,
            *file_index,
            *segment_number,
        )
        .await;
    }

    // Suppression: an encrypted set's source volumes are direct volumes, so no
    // legacy floor, completed-file row or archive re-probe may be written.
    assert!(
        (0..volumes.len() as u32)
            .all(|file_index| pipeline.is_direct_source_file(NzbFileId { job_id, file_index })),
        "every encrypted source volume must be suppressed as a direct volume"
    );
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .is_some_and(|state| state.assembly.archive_topologies().is_empty()),
        "an encrypted direct set must not enter the archive topology"
    );

    // Coverage floors and barriers: the barrier runs and publishes floors over
    // the *source* volumes, which are cipher space — untouched by the write
    // transform, because the transform happens on the destination side.
    pipeline
        .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
        .await;
    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the encrypted set must still be routing");
    assert!(!set.is_demoted(), "no gate here may demote a clean set");
    // Non-vacuity for everything below: this set really is decrypting at write
    // time, so these are the encrypted twins of the plaintext guarantees rather
    // than the plaintext originals under a new name.
    assert!(
        set.router.routes_encrypted(),
        "the fixture must have admitted an encrypted member"
    );
    assert!(
        (0..volumes.len() as u32).all(|volume_index| set.volume_coverage(volume_index).is_empty()
            || set.volume_coverage(volume_index).contiguous_from_zero() > 0),
        "every touched volume must have a contiguous floor in source (cipher) space"
    );

    // The sweep and finalization wait: the job still finishes, in one place,
    // byte-identical.
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
    let (member, location) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(member.as_deref(), Some(payload.as_slice()));
    assert_eq!(location, Some("complete"));
    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "the sweep must not have left a source volume behind"
    );
}

#[tokio::test]
async fn a_par2_bearing_encrypted_job_routes_direct_and_completes_byte_identically() {
    // The headline differential, and the one the overlay exists for. An earlier
    // shape refused this job at admission
    // (`EncryptedMemberRefused(Par2Declared)`) because an encrypted set's
    // destinations hold plaintext while PAR2 describes the posted cipher — and
    // nearly every encrypted release carries PAR2, so that refusal reached
    // almost every set the feature was built for.
    //
    // The re-encrypting overlay retires it: the pass reads the set's source
    // volumes virtually, the overlay turns the member ranges back into what was
    // posted, and the verdict is the one a physical volume would have produced.
    // Held to exactly the standard the plaintext par2-bearing differential is:
    // same bytes, same place, same status as the gate-off run with the same
    // password, and not one source volume file at any point.
    let member_name = "Silver.Horizon.S02E12.mkv";
    // 2400 is a multiple of 16, so the *other* differential below carries the
    // tail padding; this one pins the block-aligned shape.
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 193) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );

    let conventional = run_par2_direct_gate_with_password(
        DirectStoreGate::Disabled,
        JobId(43110),
        member_name,
        &volumes,
        Some("moonlit-harbour"),
    )
    .await;
    let direct = run_par2_direct_gate_with_password(
        DirectStoreGate::Enabled,
        JobId(43111),
        member_name,
        &volumes,
        Some("moonlit-harbour"),
    )
    .await;

    assert!(
        direct.admitted,
        "a par2-bearing job must admit its encrypted sets now that the verifier \
         can read them as they were posted"
    );
    assert!(
        !direct.demotions.contains("Demoted"),
        "nothing here may demote the set, got {}",
        direct.demotions
    );
    assert!(
        conventional.volume_file_seen,
        "the gate-off reference must have written the encrypted source volumes"
    );
    assert!(
        !direct.volume_file_seen,
        "a par2-bearing encrypted direct job must never create a source volume \
         file, got {}",
        direct.demotions
    );
    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the gate-off reference must decrypt the member with the job's password"
    );
    assert_eq!(
        (
            direct.member.as_deref(),
            direct.member_location,
            &direct.status
        ),
        (
            conventional.member.as_deref(),
            conventional.member_location,
            &conventional.status
        ),
        "an encrypted par2-bearing job routed plaintext-once must be byte-identical \
         to the gate-off run with the same password; sets = {}",
        direct.demotions
    );
    assert!(
        direct.demotions.contains("Finalized"),
        "the set should have finalized once verification cleared it, got {}",
        direct.demotions
    );
    // The point, unchanged by encryption: verification finishes with the
    // download, and it does so entirely against virtual volumes — the pass
    // reads the set through the overlay, which answers in posted space.
    assert!(
        direct.verdict_reached,
        "the clean encrypted set must have reached a PAR2 verdict; \
         authoritative={}",
        direct.authoritative_verify_calls
    );
}

#[tokio::test]
async fn a_recovery_set_the_spec_never_declared_no_longer_demotes_an_encrypted_set() {
    // The belt, and what the overlay leaves of it. A PAR2 file the job's spec
    // did **not** classify as one — a deobfuscated or renamed file — can make a
    // recovery set real mid-job, long after an encrypted set started routing.
    // An earlier shape had to demote such a set before the authoritative pass,
    // because the pass would otherwise have read plaintext where cipher
    // belongs; the whole set's volumes then came back off the wire.
    //
    // With the overlay the pass reads posted bytes, so the belt no longer fires
    // on "encrypted" at all. It fires only on
    // `posted_bytes_unavailable`, and this set can serve every byte it routed —
    // so the assertion is that the set is **still direct** and that its virtual
    // volumes really do answer as posted.
    //
    // Modelled by stripping the PAR2 role from the *spec* after the job is
    // inserted: the assembly keeps its own copy, so the index still parses
    // through the production path, while `job_spec_has_par2_file` answers no.
    // The last article is held back so the set is still live when the recovery
    // set lands, which is the only state the guard has anything to do in.
    let member_name = "Silver.Horizon.S02E15.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 193) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(43112);
    let (mut spec, index_file_index) =
        par2_bearing_job_spec("Silver Horizon", &volumes, &par2_bytes);
    spec.password = Some("moonlit-harbour".to_string());
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline
        .jobs
        .get_mut(&job_id)
        .expect("the job is live")
        .spec
        .files[index_file_index as usize]
        .role = weaver_model::files::FileRole::Unknown;

    let held_back = (volumes.len() as u32 - 1, 1u32);
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if (file_index, segment_number) == held_back {
            continue;
        }
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    // Non-vacuity: it really did route the encrypted set before the recovery set
    // turned up, so what the guard decides below is about a live encrypted set.
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_some_and(|set| !set.is_demoted() && set.router.routes_encrypted()),
        "the encrypted set must have been routing before the recovery set arrived"
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
    assert!(
        pipeline.par2_set(job_id).is_some(),
        "the index must have parsed through the production path, or the guard has no \
         recovery set to react to and the test proves nothing"
    );

    assert!(
        !pipeline.demote_unbindable_direct_sets(job_id).await,
        "a late recovery set must no longer take an encrypted set out of direct mode"
    );
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "the set must still be routing after the guard has run, got {shape}"
    );
    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "and nothing may have been materialized: the set never left direct mode"
    );

    // The reason it may stay: every volume it has bytes for answers as posted.
    // The last one is deliberately short an article, so this also pins that a
    // half-arrived volume reads its covered prefix rather than refusing whole.
    for (file_index, (_, posted)) in volumes.iter().enumerate().take(volumes.len() - 1) {
        let file_index = file_index as u32;
        let (volume_index, _, provider) = pipeline
            .direct_virtual_volume(NzbFileId { job_id, file_index })
            .unwrap_or_else(|| panic!("volume {file_index} must answer through the overlay"));
        let mut reader = provider.open(volume_index).expect("registered");
        let mut read_back = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut read_back).unwrap();
        assert_eq!(
            &read_back, posted,
            "encrypted direct volume {file_index} must read back as it was posted"
        );
    }
    let _ = &complete_dir;
}

#[tokio::test]
async fn par2_damage_in_an_encrypted_sets_envelope_repairs_while_the_set_stays_direct() {
    // The first transition, over cipher. The damaged byte is in a recovery
    // record's data area — outside the member's packed range, so neither the
    // per-part packed CRC32 over cipher nor the keyed whole-member fold over
    // plaintext covers it, and inside a service block's data rather than a
    // header, so the walk still parses and the volume still confirms. PAR2 is
    // the only layer that can see it.
    //
    // Everything the repair then does goes through the overlay: the damaged
    // volume is **materialized** out of the set's own bytes, which for an
    // encrypted set means re-encrypting every member range it covers, and the
    // repaired spans re-enter the router with `replace` set, which is the path
    // The cache-invalidation guards. Held to the same standard as the plaintext
    // version: the gate-off run with the same password.
    let member_name = "Silver.Horizon.S02E21.mkv";
    // Not a multiple of 16, so the member carries tail padding and the
    // materialization has to re-encrypt a final block out of it.
    let payload: Vec<u8> = (0..2405u32).map(|index| (index % 211) as u8).collect();
    let rr_bytes = 512;
    let clean = encrypted_store_set_with_recovery(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
        rr_bytes,
    );
    // The PAR2 set describes the *clean* volumes and carries enough recovery to
    // rebuild the damaged slice; the job downloads a damaged volume.
    let par2_bytes = repairable_par2_index(&clean, 4);
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, rr_bytes);

    let conventional = run_repairable_par2_gate_at(
        DirectStoreGate::Disabled,
        JobId(43121),
        member_name,
        &volumes,
        &par2_bytes,
        IndexPosition::Last,
        Some("moonlit-harbour"),
    )
    .await;
    let direct = run_repairable_par2_gate_at(
        DirectStoreGate::Enabled,
        JobId(43122),
        member_name,
        &volumes,
        &par2_bytes,
        IndexPosition::Last,
        Some("moonlit-harbour"),
    )
    .await;

    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the gate-off reference must repair the volume and decrypt the member; \
         status={:?}",
        conventional.status
    );
    assert_eq!(
        (direct.member.as_deref(), &direct.status),
        (conventional.member.as_deref(), &conventional.status),
        "a repaired encrypted direct set must produce the gate-off output, with \
         the same status; sets = {}",
        direct.sets
    );
    assert!(
        !direct.volume_file_seen,
        "repair-while-direct materializes only under a scratch name, never the \
         volume's own; sets = {}",
        direct.sets
    );
    assert_eq!(
        direct.repair_scratch_left, 0,
        "the materialized copies must be deleted whether the repair succeeded or fell back"
    );
    assert_eq!(
        direct.materialized, 1,
        "only the damaged volume may be materialized, and it must have been; \
         sets = {}",
        direct.sets
    );
    assert!(
        direct.finalized > 0,
        "the set must have stayed direct and committed its members from its own \
         partials; sets = {}",
        direct.sets
    );
    assert!(
        !direct.sets.contains("Demoted"),
        "nothing here may demote the set, got {}",
        direct.sets
    );
}

#[tokio::test]
async fn par2_damage_in_a_split_encrypted_members_first_article_repairs_while_the_set_stays_direct()
{
    // The drain-order finding, and the coverage gap that hid it: every other
    // repair test in this file damages a *recovery record* in a volume's
    // **last** article, so the rewrite it provokes never reaches a member
    // extent's first cipher block. Here the damaged volume is posted as a
    // single article, so the rewrite — widened to whole articles — starts at
    // physical zero and the whole of volume 1's member extent is re-routed,
    // first block included.
    //
    // That block straddles the volume boundary: part of it was posted in volume
    // 0, and its CBC predecessor lies wholly in volume 0. Both were routed and
    // dropped from staging long before the repair, and no checkpoint survives at
    // the extent's low edge once the two volumes' decrypted runs coalesce — so
    // `cipher_edge_reads` is the only thing that can put them back. It used to
    // ask for the straddling bytes alone, which left the block undecryptable,
    // the span held, and the whole set demoted under `RepairRerouteFailed` into
    // a full refetch of every article.
    //
    // Held to the same standard as the other repair differentials: the gate-off
    // run with the same password, byte for byte.
    let member_name = "Silver.Horizon.S02E24.mkv";
    // Not a multiple of 16, so the member also carries tail padding.
    let payload: Vec<u8> = (0..2405u32).map(|index| (index % 223) as u8).collect();
    let rr_bytes = 512;
    let clean = encrypted_store_set_with_recovery(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
        rr_bytes,
    );
    // The RR keeps PAR2 the only layer that can see the damage: it belongs to no
    // member, so no packed or whole-member checksum covers it. What makes this
    // test different is not where the damage is but how wide the *rewrite* is.
    let par2_bytes = repairable_par2_index(&clean, 4);
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, rr_bytes);

    let conventional = run_repairable_par2_gate_with_articles(
        DirectStoreGate::Disabled,
        JobId(43151),
        member_name,
        &volumes,
        &par2_bytes,
        IndexPosition::Last,
        Some("moonlit-harbour"),
        1,
    )
    .await;
    let direct = run_repairable_par2_gate_with_articles(
        DirectStoreGate::Enabled,
        JobId(43152),
        member_name,
        &volumes,
        &par2_bytes,
        IndexPosition::Last,
        Some("moonlit-harbour"),
        1,
    )
    .await;

    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the gate-off reference must repair the volume and decrypt the member; \
         status={:?}",
        conventional.status
    );
    assert_eq!(
        (direct.member.as_deref(), &direct.status),
        (conventional.member.as_deref(), &conventional.status),
        "a repair reaching a split encrypted member's first block must produce \
         the gate-off output, with the same status; sets = {}",
        direct.sets
    );
    assert!(
        !direct.sets.contains("Demoted"),
        "and it must not demote — a set that reroutes its own repair never \
         refetches an article; got {}",
        direct.sets
    );
    assert!(
        !direct.volume_file_seen,
        "repair-while-direct materializes only under a scratch name, never the \
         volume's own; sets = {}",
        direct.sets
    );
    assert_eq!(
        direct.materialized, 1,
        "only the damaged volume may be materialized, and it must have been; \
         sets = {}",
        direct.sets
    );
    assert_eq!(direct.repair_scratch_left, 0);
    assert!(
        direct.finalized > 0,
        "the set must have stayed direct and committed its members from its own \
         partials; sets = {}",
        direct.sets
    );
}

#[tokio::test]
async fn an_encrypted_set_that_demotes_rebuilds_byte_exact_posted_volumes() {
    // The *other* transition, over cipher. An earlier shape refused
    // reconstruction for an encrypted set outright
    // (`ReconstructionFailure::EncryptedPostedBytes`) and refetched every
    // article instead, because the member partials hold plaintext where the
    // volume holds cipher.
    //
    // The overlay makes the sweep read posted bytes, so a demoting encrypted set
    // materializes its volumes out of its own routed bytes like any other — and
    // the standard is byte-exactness against what was posted, not merely "a file
    // appeared", because reconstruction publishes a floor over what it writes
    // and nothing downstream ever re-checks those bytes.
    //
    // The demotion is forced by a reason that has nothing to do with encryption
    // — a scratch failure — so what is under test is the sweep and only the
    // sweep. What keeps the set *live* while every article is in is the job's
    // PAR2 file: finalization waits for a verdict, and the index is never
    // posted, so the set sits in the wait a demotion can still reach. (A
    // finalized set can never be demoted, and one that has committed its members
    // has nothing left to reconstruct from.)
    //
    // Every article, deliberately. An encrypted member's last cipher block in
    // volume *n* straddles into volume *n+1*, so a set missing any article holds
    // the ≤15 bytes below that boundary — and the covered run then stops
    // mid-article with nothing in the yEnc composition to vouch for it, which is
    // `UnverifiableRun` and a refusal to reconstruct at all. That is correct,
    // fail-closed behaviour; it is simply not what this test is about.
    let member_name = "Silver.Horizon.S02E22.mkv";
    let payload: Vec<u8> = (0..2405u32).map(|index| (index % 197) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(43131);
    let (mut spec, _index_file_index) =
        par2_bearing_job_spec("Silver Horizon", &volumes, &par2_bytes);
    spec.password = Some("moonlit-harbour".to_string());
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // Dequeued as they are delivered, so the queue at the end says what the
    // demotion put *back* rather than what the harness never took out.
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        take_queued_segment(
            &mut pipeline,
            job_id,
            SegmentId {
                file_id: NzbFileId { job_id, file_index },
                segment_number,
            },
        );
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_some_and(|set| !set.is_demoted() && set.router.routes_encrypted()),
        "non-vacuity: the set must have routed encrypted before it is demoted"
    );

    pipeline
        .demote_direct_set(job_id, 0, DemotionReason::HoldsScratchFailed)
        .await;
    settle_direct_post_repair_work(&mut pipeline).await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted"),
        "the set must have demoted, got {shape}"
    );

    // The whole point: real files, byte-exact against what was *posted* rather
    // than against what the destinations hold.
    for (filename, posted) in &volumes {
        let rebuilt = std::fs::read(working_dir.join(filename))
            .unwrap_or_else(|error| panic!("{filename} must have been materialized: {error}"));
        assert_eq!(
            &rebuilt, posted,
            "{filename} must be rebuilt byte-exactly as it was posted, not as it \
             was decrypted"
        );
    }
    // And not one volume article came back off the wire — That refusal
    // refetched every one of them, which is exactly what the sweep exists to
    // avoid. The PAR2 index is still outstanding because it was never posted.
    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        queued
            .iter()
            .all(|(file_index, _)| *file_index as usize >= volumes.len()),
        "an encrypted set that reconstructs must not also refetch its volumes, \
         got {queued:?}"
    );
}

#[tokio::test]
async fn a_par2_bearing_encrypted_set_restarted_mid_download_verifies_and_completes_byte_identically()
 {
    // The restart differential, and the shape the write side could not reach: a
    // par2-bearing job was refused encrypted admission outright, so nothing
    // encrypted ever survived a restart *and* faced a verifier.
    //
    // Three things have to hold together here. The floors have to be honoured
    // (nothing below them comes back off the wire); the keyed member gate has to
    // re-arm by re-reading the pre-restart **plaintext** from disk; and the
    // overlay has to serve **posted** bytes over coverage this process never
    // wrote — which means the crypt facts, the cipher checkpoints and the
    // retained tail padding all came back through snapshot schema v4.
    const ARTICLES: usize = 4;
    let member_name = "Silver.Horizon.S02E23.mkv";
    // Not a multiple of 16, so the retained tail padding is part of what the
    // snapshot has to carry across the restart.
    let payload: Vec<u8> = (0..8005u32).map(|index| (index % 251) as u8).collect();
    let volumes = encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some("moonlit-harbour"),
        true,
    );
    let par2_bytes = par2_index_over_volumes(&volumes);
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(43141);

    // Volume 0 whole, volume 1 half: one file the restore skips entirely and one
    // it skips only part of, with a part boundary that is not 16-aligned — so
    // the resumed run decrypts at a coverage frontier whose predecessor cipher
    // block died with the process that wrote it.
    let (working_dir, index_file_index) = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
        let mut spec = direct_store_job_spec_with_articles("Silver Horizon", &volumes, ARTICLES);
        let index_file_index = append_par2_index(&mut spec, &par2_bytes);
        spec.password = Some("moonlit-harbour".to_string());
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
        for (file_index, segment_number) in [(0u32, 0u32), (0, 1), (0, 2), (0, 3), (1, 0), (1, 1)] {
            submit_volume_article_of(
                &mut pipeline,
                job_id,
                &volumes,
                file_index,
                segment_number,
                ARTICLES,
            )
            .await;
        }
        pipeline
            .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
            .await;
        (working_dir, index_file_index)
    };

    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let mut spec = direct_store_job_spec_with_articles("Silver Horizon", &volumes, ARTICLES);
    append_par2_index(&mut spec, &par2_bytes);
    spec.password = Some("moonlit-harbour".to_string());
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

    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the restored par2-bearing encrypted job must carry its direct set");
    assert!(
        !set.is_demoted(),
        "a par2-bearing encrypted set must survive a restart now that the pass \
         can read it as posted"
    );
    assert!(
        set.has_restart_seeded_coverage(),
        "restored coverage is seeded and unverified until it is re-read as plaintext"
    );

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        !queued.iter().any(|(file_index, _)| *file_index == 0),
        "volume 0 was complete at the barrier; none of its articles may be \
         refetched, got {queued:?}"
    );
    assert!(
        !queued.contains(&(1, 0)),
        "volume 1's checkpointed articles must not be refetched, got {queued:?}"
    );

    // The restored set answers in posted space before a single new article
    // arrives — which is only possible if the snapshot carried the crypt facts,
    // the cipher checkpoints and the tail padding across the restart.
    let (volume_index, _, provider) = pipeline
        .direct_virtual_volume(NzbFileId {
            job_id,
            file_index: 0,
        })
        .expect("the restored volume must answer through the overlay");
    let mut reader = provider.open(volume_index).expect("registered");
    let mut read_back = Vec::new();
    std::io::Read::read_to_end(&mut reader, &mut read_back).unwrap();
    assert_eq!(
        read_back, volumes[0].1,
        "a restored encrypted volume must read back exactly as it was posted"
    );

    for (file_index, segment_number) in queued.clone() {
        if file_index == index_file_index {
            continue;
        }
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
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_none_or(|set| !set.has_restart_seeded_coverage()),
        "the keyed member gate must have re-read the pre-restart plaintext before \
         finalizing"
    );

    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
    let (member, location) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        member.as_deref(),
        Some(payload.as_slice()),
        "a restarted par2-bearing encrypted set must still produce the member"
    );
    assert_eq!(location, Some("complete"));
    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "and it must never have materialized a source volume"
    );
}
