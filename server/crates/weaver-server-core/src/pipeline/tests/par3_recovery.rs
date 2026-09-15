//! PAR3 recovery: deterministic verdicts, per-cohort acquisition, telemetry.

use super::*;
use crate::operations::metrics::Par3OutcomeClass;
use crate::operations::metrics::Par3Phase;
use crate::pipeline::tests::direct_store::{
    demotion_fixture_volumes, direct_store_job_spec, submit_volume_article, take_queued_segment,
};

const INDEX: &[u8] = include_bytes!("../repair/backend/fixtures/set.par3");
const VOLUME: &[u8] = include_bytes!("../repair/backend/fixtures/set.vol0+1.par3");

/// The protected payload the fixture carriers were built over, with `damage`
/// applied so more blocks are lost than the single recovery block can cover.
fn damaged_payload(damage: &[usize]) -> Vec<(&'static str, Vec<u8>)> {
    let mut a: Vec<u8> = (0..5000u32).map(|i| (i * 7 + 3) as u8).collect();
    for &offset in damage {
        a[offset] ^= 0xff;
    }
    vec![
        ("a.bin", a),
        ("b.txt", b"qrstuvwxyz".to_vec()),
        (
            "sub/c.bin",
            (0..4000u32).map(|i| (i * 13 + 1) as u8).collect(),
        ),
    ]
}

/// Publish a job whose carriers arrive in `carrier_order`, and drive it to
/// whatever verdict PAR3 reaches. The payload files always arrive first; only
/// the carriers — and so the packet bytes the engine ingests — are reordered.
/// What a run reached: the job's own failure text, which is the verdict's
/// `Display`, and the counter of verdict classes it passed through.
#[derive(Debug)]
struct Verdict {
    failure: Option<String>,
    classes: [u64; crate::operations::metrics::Par3OutcomeClass::COUNT],
    /// What the carriers' own scans reported, captured as soon as every
    /// carrier had been scanned and before completion could retire the job.
    damage: Vec<String>,
    /// Where the job's outputs would have been written.
    working: PathBuf,
    /// Whether the completion check stopped claiming another step on its own.
    terminated: bool,
}

async fn verdict_for_carrier_order(
    root: &TempDir,
    job_id: JobId,
    carrier_order: &[(&str, &[u8])],
    damage: &[usize],
) -> Verdict {
    let (mut pipeline, _, _) = new_direct_pipeline(root).await;
    let payload: Vec<(String, Vec<u8>)> = damaged_payload(damage)
        .into_iter()
        .map(|(name, bytes)| (name.to_string(), bytes))
        .collect();
    let carriers: Vec<(String, Vec<u8>)> = carrier_order
        .iter()
        .map(|(name, bytes)| ((*name).to_string(), bytes.to_vec()))
        .collect();
    run_par3_job(&mut pipeline, job_id, &payload, &carriers).await
}

/// Publish a job whose protected files are `payload` and whose PAR3 carriers
/// are `carriers`, and drive it to whatever verdict PAR3 reaches.
async fn run_par3_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    payload: &[(String, Vec<u8>)],
    carriers: &[(String, Vec<u8>)],
) -> Verdict {
    let mut files: Vec<(String, Vec<u8>)> = payload.to_vec();
    let first_carrier = files.len() as u32;
    files.extend(carriers.iter().cloned());
    let mut spec = standalone_job_spec(
        "deterministic verdict",
        &files
            .iter()
            .map(|(name, bytes)| (name.clone(), bytes.len() as u32))
            .collect::<Vec<_>>(),
    );
    for file in &mut spec.files {
        file.role = FileRole::from_filename(&file.filename);
    }
    let working = insert_active_job(pipeline, job_id, spec).await;
    for (name, _) in &files {
        if let Some(parent) = std::path::Path::new(name).parent()
            && parent != std::path::Path::new("")
        {
            tokio::fs::create_dir_all(working.join(parent))
                .await
                .unwrap();
        }
    }
    for (index, (name, bytes)) in files.iter().enumerate() {
        write_and_complete_file(pipeline, job_id, index as u32, name, bytes).await;
    }
    for offset in 0..carriers.len() as u32 {
        pipeline
            .try_load_par3_metadata(
                job_id,
                NzbFileId {
                    job_id,
                    file_index: first_carrier + offset,
                },
            )
            .await;
        settle_par3_recovery(pipeline, job_id).await;
    }
    let damage = pipeline
        .par3_runtime
        .as_ref()
        .map(|runtime| {
            runtime
                .carrier_damage(job_id)
                .iter()
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default();
    // Drive completion until PAR3 stops claiming the next step, settling every
    // blocking work unit it asks for along the way.
    // Every announced file has already been written, so the segments still
    // sitting in the queue are spent: drain them rather than letting them
    // stand in for download work that will never arrive.
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .extract_matching(|_| true);
    // A completion check that keeps claiming another step forever is itself a
    // defect, so the bound is generous and whether it was reached is reported.
    let mut terminated = false;
    for _ in 0..64 {
        if !pipeline.check_par3_completion(job_id).await {
            terminated = true;
            break;
        }
        settle_par3_recovery(pipeline, job_id).await;
    }
    Verdict {
        terminated,
        failure: match job_status_for_assert(pipeline, job_id) {
            Some(JobStatus::Failed { error }) => Some(error),
            _ => None,
        },
        classes: pipeline.metrics.par3.outcomes(),
        damage,
        working,
    }
}

async fn settle_par3_recovery(pipeline: &mut Pipeline, job_id: JobId) {
    while pipeline
        .par3_runtime
        .as_ref()
        .is_some_and(|runtime| runtime.has_work(job_id))
    {
        let done =
            tokio::time::timeout(Duration::from_secs(10), pipeline.repair_work_done_rx.recv())
                .await
                .unwrap()
                .unwrap();
        pipeline.handle_repair_work_done(done).await;
    }
}

/// Deliverable: the same carrier bytes presented in two orders must reach the
/// same outcome, with an identical cohort deficit list — not merely the same
/// class, and not a list that happens to be sorted differently.
#[tokio::test]
async fn the_same_carrier_bytes_in_two_orders_reach_the_same_verdict() {
    // Two damaged blocks against one recovery block: the deficit is real and
    // the verdict is stable whichever carrier the engine ingests first.
    let damage = [300usize, 2300, 4300];
    let index_first = TempDir::new().unwrap();
    let forward = verdict_for_carrier_order(
        &index_first,
        JobId(3401),
        &[("set.par3", INDEX), ("set.vol0+1.par3", VOLUME)],
        &damage,
    )
    .await;
    let volume_first = TempDir::new().unwrap();
    let reversed = verdict_for_carrier_order(
        &volume_first,
        JobId(3402),
        &[("set.vol0+1.par3", VOLUME), ("set.par3", INDEX)],
        &damage,
    )
    .await;

    // The class counters say which verdicts each run passed through at all.
    assert_eq!(
        forward.classes[Par3OutcomeClass::Unrecoverable.index()],
        1,
        "the forward order must reach an unrecoverable verdict"
    );
    assert_eq!(
        forward.classes, reversed.classes,
        "carrier order changed which verdicts the job reached"
    );
    // The job's failure text is the verdict's own `Display`, so comparing it
    // compares the rendered cohort deficit list, not merely the class.
    let failure = forward
        .failure
        .as_deref()
        .expect("an unrecoverable job must carry its verdict as its failure");
    assert!(
        failure.contains("cohort 0/1") && failure.contains("short 1"),
        "the verdict must name the cohort and what it is short by: {failure}"
    );
    assert_eq!(
        forward.failure, reversed.failure,
        "carrier order changed the reported cohort deficits"
    );
}

/// Deliverable: withholding the recovery articles leaves the job in a wait,
/// publishes what it is short by, and lets the slot's stall grow and clear.
#[tokio::test]
async fn withheld_recovery_articles_show_as_a_wait_a_deficit_and_a_stall() {
    let root = TempDir::new().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    let job_id = JobId(3403);
    let payload = damaged_payload(&[300, 2300]);
    let mut files: Vec<(String, Vec<u8>)> = payload
        .into_iter()
        .map(|(name, bytes)| (name.to_string(), bytes))
        .collect();
    files.push(("set.par3".into(), INDEX.to_vec()));
    // The recovery volume is announced but never delivered: its segments stay
    // queued, which is exactly the withheld-articles case.
    files.push(("set.vol0+1.par3".into(), Vec::new()));
    let mut spec = standalone_job_spec(
        "withheld recovery",
        &files
            .iter()
            .map(|(name, bytes)| (name.clone(), (bytes.len() as u32).max(600_000)))
            .collect::<Vec<_>>(),
    );
    for file in &mut spec.files {
        file.role = FileRole::from_filename(&file.filename);
    }
    spec.files[4].segments = (0..4)
        .map(|number| {
            segment_spec! {
                number: number,
                bytes: 600_000,
                message_id: format!("withheld-{number}@invented.test"),
            }
        })
        .collect();
    let working = insert_active_job(&mut pipeline, job_id, spec).await;
    tokio::fs::create_dir(working.join("sub")).await.unwrap();
    for (index, (name, bytes)) in files[..4].iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, index as u32, name, bytes).await;
    }
    pipeline
        .try_load_par3_metadata(
            job_id,
            NzbFileId {
                job_id,
                file_index: 3,
            },
        )
        .await;
    settle_par3_recovery(&mut pipeline, job_id).await;

    let plan = pipeline.par3_cohort_plan(job_id);
    assert!(
        !plan.is_empty(),
        "a damaged payload must leave at least one cohort short"
    );
    assert!(plan.needed_bytes > 0);
    let metrics = Arc::clone(&pipeline.metrics);
    assert_eq!(
        metrics
            .par3
            .recovery_needed_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
        plan.needed_bytes,
        "the deficit gauge must carry what the plan asked for"
    );
    assert_eq!(
        metrics
            .par3
            .cohorts_with_deficit
            .load(std::sync::atomic::Ordering::Relaxed),
        plan.windows.len()
    );

    // The payload and index files are complete; their own segments must not
    // be left in the queue to compete with the recovery window for its budget.
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .extract_matching(|work| work.segment_id.file_id.file_index < 4);
    assert!(pipeline.promote_par3_recovery_window(job_id, false));
    let promoted = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .extract_matching(|work| work.segment_id.file_id.file_index == 4);
    assert!(!promoted.is_empty(), "the deficient cohort must be fetched");
    assert_eq!(
        metrics
            .par3
            .recovery_articles_requested_total
            .load(std::sync::atomic::Ordering::Relaxed),
        promoted.len() as u64
    );
    assert_eq!(
        metrics.par3.outcomes()
            [crate::operations::metrics::Par3OutcomeClass::NeedsRecovery.index()],
        1,
        "an admitted window is a recorded NeedsRecovery verdict, not a failure"
    );

    // The slot reports the wait, and its stall grows and then clears. The
    // model is driven at chosen instants rather than by sleeping for the
    // thirty-second threshold.
    const THRESHOLD: u64 = crate::operations::metrics::PAR3_STALL_THRESHOLD_MS;
    metrics
        .par3
        .store_phase(job_id.0, Par3Phase::AwaitingRecoveryArticles, 0);
    let fresh = metrics.par3.observe_for_test(1_000);
    let slot = fresh
        .iter()
        .find(|slot| slot.job_id == job_id.0)
        .expect("the job must own a slot while it waits");
    assert_eq!(slot.phase, Par3Phase::AwaitingRecoveryArticles);
    assert_eq!(slot.current_stall_ms, 0);
    assert_eq!(
        metrics
            .par3
            .stalls_total
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );

    // Past the threshold the wait is a stall, and it keeps growing.
    let stalled = metrics.par3.observe_for_test(THRESHOLD + 5_000);
    assert!(
        stalled
            .iter()
            .any(|slot| slot.job_id == job_id.0 && slot.current_stall_ms == 5_000),
        "a withheld window must show as a growing stall"
    );
    let grown = metrics.par3.observe_for_test(THRESHOLD + 9_000);
    assert!(
        grown
            .iter()
            .any(|slot| slot.job_id == job_id.0 && slot.current_stall_ms == 9_000)
    );
    assert_eq!(
        metrics
            .par3
            .stalls_total
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "an ongoing stall is one stall"
    );

    // Progress clears it and credits the whole duration exactly once.
    metrics
        .par3
        .set_last_progress_for_test(job_id.0, THRESHOLD + 9_000);
    let cleared = metrics.par3.observe_for_test(THRESHOLD + 9_100);
    assert!(cleared.iter().all(|slot| slot.current_stall_ms == 0));
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.par3.stall_duration_ms, 9_100);
    assert_eq!(snapshot.par3.stalls_total, 1, "the stall is not recounted");
}

/// Deliverable: an admission refused over budget records the refusing budget
/// and separates a collision with a peer from a set that can never fit.
#[test]
fn an_over_budget_admission_names_its_budget_and_its_class() {
    use crate::operations::metrics::{Par3AdmissionReason, Par3OutcomeClass};
    use crate::pipeline::repair::par3::outcome::{admission_reason, classify_memory_refusal};
    use par3_rs::runtime::EngineError;

    // A refusal while a peer holds a work unit, for a set that would fit
    // alone, is a wait: the peer's handback resolves it.
    let waiting = classify_memory_refusal(4 << 20, 16 << 20, 1 << 20, true);
    assert_eq!(waiting.class(), Par3OutcomeClass::WaitingForMemory);
    assert!(!waiting.is_terminal(), "a peer collision is not a failure");

    // The same need with no peer in flight, or a need past the ceiling, can
    // never be satisfied by waiting.
    for outcome in [
        classify_memory_refusal(4 << 20, 16 << 20, 1 << 20, false),
        classify_memory_refusal(64 << 20, 16 << 20, 16 << 20, true),
    ] {
        assert_eq!(outcome.class(), Par3OutcomeClass::DoesNotFit);
        assert!(outcome.is_terminal());
    }

    // Each refusal reaches the slot its own budget names.
    for (label, expected) in [
        ("memory budget", Par3AdmissionReason::RetainedState),
        (
            "PAR3 retained payload",
            Par3AdmissionReason::ResolvedMetadata,
        ),
        ("PAR3 host state", Par3AdmissionReason::AssessmentView),
        (
            "PAR3 disk fallback space",
            Par3AdmissionReason::DiskFallbackSpace,
        ),
        ("job carrier count", Par3AdmissionReason::CarrierCount),
        ("job PAR3 set count", Par3AdmissionReason::SetCount),
        ("something the engine invented", Par3AdmissionReason::Other),
    ] {
        let metrics = crate::operations::metrics::Par3Metrics::default();
        let reason = admission_reason(&EngineError::ResourceLimit(label));
        assert_eq!(reason, expected, "{label}");
        metrics.note_admission_refused(reason);
        let refused = metrics.admission_refused();
        assert_eq!(refused[expected.index()], 1, "{label}");
        assert_eq!(refused.iter().sum::<u64>(), 1, "{label} leaked into a peer");
    }
}

/// A refused spill with a peer holding the budget parks instead of failing —
/// and the peer's handback is what wakes it. The park is bounded by that peer:
/// the second attempt runs with the slot free, so it reaches a terminal
/// verdict rather than parking again.
#[tokio::test]
async fn a_peer_handback_wakes_every_job_parked_on_par3_memory() {
    use crate::pipeline::direct_store::wiring::DirectStoreRuntime;
    use crate::pipeline::direct_store::{DirectStoreGate, DirectStoreSettings};
    use crate::pipeline::repair::par3::work::Coordinator;
    use par3_rs::source::SourceId;
    use std::sync::atomic::Ordering::Relaxed;

    let root = TempDir::new().unwrap();
    let peer = JobId(52101);
    let parked = JobId(52102);
    let volumes = demotion_fixture_volumes("parked.mkv");
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    // No disk may be reserved, so the fallback is refused and the only thing
    // that could still admit this image is the native budget a peer holds.
    pipeline.direct_store = DirectStoreRuntime::with_settings(DirectStoreSettings {
        gate: DirectStoreGate::Enabled,
        holds_disk_reserve_bytes: u64::MAX,
        ..Default::default()
    });
    let spec = direct_store_job_spec("PAR3 parked spill", &volumes);
    insert_active_job(&mut pipeline, parked, spec).await;
    let segment = SegmentId {
        file_id: NzbFileId {
            job_id: parked,
            file_index: 0,
        },
        segment_number: 1,
    };
    take_queued_segment(&mut pipeline, parked, segment);
    submit_volume_article(&mut pipeline, parked, &volumes, 0, 1).await;

    // The peer takes a real work unit: it occupies a coordinator slot until it
    // hands back, which is exactly the collision the parked job waits out.
    let carrier = root.path().join("peer.par3");
    std::fs::write(&carrier, INDEX).unwrap();
    let mut coordinator = Coordinator::new(
        pipeline.repair_work_done_tx.clone(),
        std::sync::Arc::clone(&pipeline.metrics),
    );
    coordinator
        .force_dispatch(peer, SourceId(0), carrier)
        .unwrap();
    coordinator.force_spill(parked, SourceId(0));
    pipeline.par3_runtime = Some(Box::new(coordinator));

    assert!(pipeline.spill_par3_source(parked).await);
    let metrics = std::sync::Arc::clone(&pipeline.metrics);
    assert_eq!(
        metrics.par3.outcomes()[Par3OutcomeClass::WaitingForMemory.index()],
        1,
        "a refusal with a peer in flight is a wait, not a verdict"
    );
    assert_eq!(metrics.par3.waiting_for_memory_active.load(Relaxed), 1);
    assert!(
        matches!(job_status_for_assert(&pipeline, parked), Some(status) if !matches!(status, JobStatus::Failed { .. })),
        "a parked job has not failed"
    );
    let phase = |job_id: JobId| {
        metrics
            .par3
            .observe_for_test(metrics.now_ms())
            .into_iter()
            .find(|slot| slot.job_id == job_id.0)
            .map(|slot| slot.phase)
    };
    assert_eq!(phase(parked), Some(Par3Phase::AwaitingMemory));

    // Nothing on the park path reports progress, so the wait ages like any
    // other phase and a long one is a stall rather than a silent gap.
    let parked_slot = metrics
        .par3
        .observe_for_test(metrics.now_ms())
        .into_iter()
        .find(|slot| slot.job_id == parked.0)
        .expect("the parked job holds a slot");
    let crossed =
        parked_slot.last_progress_ms + crate::operations::metrics::PAR3_STALL_THRESHOLD_MS + 1;
    let crossed_view = metrics.par3.observe_for_test(crossed);
    let aged = crossed_view
        .iter()
        .find(|slot| slot.job_id == parked.0)
        .expect("the parked job still holds its slot");
    assert_eq!(aged.phase, Par3Phase::AwaitingMemory);
    assert_eq!(aged.current_stall_ms, 1, "the park itself is the stall");
    assert_eq!(
        crossed_view
            .iter()
            .filter(|slot| slot.current_stall_ms != 0)
            .count() as u64,
        metrics.par3.stalls_total.load(Relaxed),
        "every slot past the threshold, the park included, counts exactly once"
    );

    // The wait is wall time like any other: let it register before it ends.
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;

    let done = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        pipeline.repair_work_done_rx.recv(),
    )
    .await
    .expect("the peer work unit finishes")
    .expect("the coordinator holds the sender");
    pipeline.handle_repair_work_done(done).await;
    assert!(
        pipeline.pending_completion_checks.contains(&parked),
        "the handback owes every parked job a completion check"
    );
    while let Some(job_id) = pipeline.pending_completion_checks.pop_front() {
        pipeline.check_job_completion(job_id).await;
    }

    // With the slot free the second attempt has nothing left to wait for, so
    // the refusal is spoken as a terminal verdict against the budget that
    // actually refused it.
    assert_eq!(metrics.par3.waiting_for_memory_active.load(Relaxed), 0);
    assert!(
        metrics.par3.waiting_for_memory_ms_total.load(Relaxed) > 0,
        "the wait is credited when it ends"
    );
    assert_eq!(
        metrics.par3.outcomes()[Par3OutcomeClass::NotExecutable.index()],
        1,
        "a park is bounded by the peer's lifetime"
    );
    assert!(matches!(
        job_status_for_assert(&pipeline, parked),
        Some(JobStatus::Failed { error }) if error.contains("disk fallback")
    ));
    for slot in metrics.par3.observe_for_test(metrics.now_ms()) {
        assert_eq!(
            slot.phase,
            Par3Phase::Idle,
            "job {} still occupies a slot after both jobs settled",
            slot.job_id
        );
    }
}

/// The fixture index's Root packet, the one vital packet a test damages.
const ROOT_PACKET: std::ops::Range<usize> = 672..781;

/// Damage a packet in place: past its 48-byte header, so the scanner still
/// finds the packet and still reads its declared length, and only the hash it
/// carries stops describing the body behind it.
fn with_damaged_packet(carrier: &[u8], packet: std::ops::Range<usize>) -> Vec<u8> {
    let mut bytes = carrier.to_vec();
    for byte in &mut bytes[packet.start + 48..packet.end] {
        *byte ^= 0xff;
    }
    bytes
}

/// Deliverable: every vital packet can be taken from the volume carriers, so a
/// set whose index file never arrives still repairs.
#[tokio::test]
async fn a_set_whose_index_never_arrives_still_repairs_from_its_volumes() {
    let root = TempDir::new().unwrap();
    let job_id = JobId(52201);
    // One damaged block against the one recovery block the volume ships.
    let verdict =
        verdict_for_carrier_order(&root, job_id, &[("set.vol0+1.par3", VOLUME)], &[300]).await;
    assert_eq!(
        verdict.failure, None,
        "the volumes carry every vital packet: {verdict:?}",
    );
    assert_eq!(
        verdict.classes[Par3OutcomeClass::MetadataIncomplete.index()],
        0,
        "a missing index file is not incomplete metadata"
    );
    let repaired = tokio::fs::read(verdict.working.join("a.bin"))
        .await
        .unwrap();
    assert_eq!(
        repaired,
        damaged_payload(&[]).into_iter().next().unwrap().1,
        "the protected file was reconstructed"
    );
}

/// Deliverable: a damaged copy of a vital packet in one carrier is summarised
/// with its offset, and the surviving copy in another carrier still repairs.
#[tokio::test]
async fn a_damaged_root_copy_is_summarised_and_the_surviving_copy_repairs() {
    let root = TempDir::new().unwrap();
    let job_id = JobId(52202);
    let damaged_index = with_damaged_packet(INDEX, ROOT_PACKET);
    let verdict = verdict_for_carrier_order(
        &root,
        job_id,
        &[("set.par3", &damaged_index), ("set.vol0+1.par3", VOLUME)],
        &[300],
    )
    .await;
    assert_eq!(verdict.failure, None, "{verdict:?}");
    assert_eq!(
        verdict.damage.len(),
        1,
        "exactly one carrier was damaged: {verdict:?}"
    );
    assert!(
        verdict.damage[0].contains(&format!("from offset {}", ROOT_PACKET.start)),
        "the summary names the damaged range's own offset: {verdict:?}"
    );
    assert!(
        verdict.damage[0].contains(&format!("damaged {} byte", ROOT_PACKET.len())),
        "the summary names the damaged range's length: {verdict:?}"
    );
    assert_ne!(
        verdict.classes[Par3OutcomeClass::CarrierDamage.index()],
        0,
        "the damage was counted as its own class"
    );
    let repaired = tokio::fs::read(verdict.working.join("a.bin"))
        .await
        .unwrap();
    assert_eq!(
        repaired,
        damaged_payload(&[]).into_iter().next().unwrap().1,
        "the surviving Root copy planned the repair"
    );
}

/// Deliverable: with no authenticated Root copy in any carrier the job fails as
/// incomplete metadata, and says which packet family it never saw.
#[tokio::test]
async fn no_authenticated_root_anywhere_names_the_root_packet() {
    let root = TempDir::new().unwrap();
    let job_id = JobId(52203);
    let damaged_index = with_damaged_packet(INDEX, ROOT_PACKET);
    let damaged_volume = with_damaged_packet(VOLUME, ROOT_PACKET);
    let verdict = verdict_for_carrier_order(
        &root,
        job_id,
        &[
            ("set.par3", &damaged_index),
            ("set.vol0+1.par3", &damaged_volume),
        ],
        &[300],
    )
    .await;
    let failure = verdict
        .failure
        .clone()
        .unwrap_or_else(|| panic!("a set with no Root must fail: {verdict:#?}"));
    assert!(
        failure.contains("authenticated metadata remains incomplete"),
        "{failure}"
    );
    assert!(
        failure.contains("no authenticated root packet"),
        "the verdict names the family it never saw: {failure}"
    );
    assert_ne!(
        verdict.classes[Par3OutcomeClass::MetadataIncomplete.index()],
        0
    );
    assert!(
        verdict.terminated,
        "the completion check reached its verdict and stopped"
    );
}

/// Deliverable: a carrier whose own file is already whole never takes a slot
/// or a byte of a recovery window's budget. Production leaves such a carrier's
/// spent queue entries behind, and before this they were promoted again on
/// every pass — which both wasted the window and kept it permanently active.
#[tokio::test]
async fn a_complete_carrier_never_spends_a_recovery_window_budget() {
    use std::sync::atomic::Ordering::Relaxed;

    let root = TempDir::new().unwrap();
    let job_id = JobId(52204);
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    let payload = damaged_payload(&[300, 2300, 4300]);
    let mut files: Vec<(String, Vec<u8>)> = payload
        .iter()
        .map(|(name, bytes)| ((*name).to_string(), bytes.clone()))
        .collect();
    let complete_carrier = files.len() as u32;
    files.push(("set.par3".into(), INDEX.to_vec()));
    let incomplete_carrier = files.len() as u32;
    files.push(("set.vol0+1.par3".into(), VOLUME.to_vec()));
    let mut spec = standalone_job_spec(
        "complete carrier budget",
        &files
            .iter()
            .map(|(name, bytes)| (name.clone(), bytes.len() as u32))
            .collect::<Vec<_>>(),
    );
    for file in &mut spec.files {
        file.role = FileRole::from_filename(&file.filename);
    }
    let working = insert_active_job(&mut pipeline, job_id, spec).await;
    tokio::fs::create_dir_all(working.join("sub"))
        .await
        .unwrap();
    // Everything but the volume carrier lands. The volume's article is still
    // owed, so a window has real work to do; the index carrier's article is
    // spent, and only its queue entry survives.
    for (index, (name, bytes)) in files.iter().enumerate() {
        if index as u32 == incomplete_carrier {
            continue;
        }
        write_and_complete_file(&mut pipeline, job_id, index as u32, name, bytes).await;
    }
    pipeline
        .try_load_par3_metadata(
            job_id,
            NzbFileId {
                job_id,
                file_index: complete_carrier,
            },
        )
        .await;
    settle_par3_recovery(&mut pipeline, job_id).await;
    let queued = |pipeline: &Pipeline, file_index: u32| {
        pipeline.jobs[&job_id]
            .download_queue
            .count_matching(|work| work.segment_id.file_id.file_index == file_index)
    };
    assert_ne!(
        queued(&pipeline, complete_carrier),
        0,
        "production does leave a complete carrier's segment queued"
    );
    assert!(
        pipeline.jobs[&job_id]
            .assembly
            .file(NzbFileId {
                job_id,
                file_index: complete_carrier
            })
            .is_some_and(|file| file.is_complete()),
        "that carrier's own file is whole"
    );

    assert!(
        pipeline.promote_par3_recovery_window(job_id, false),
        "a window is admitted for the carrier that is still owed"
    );
    assert_eq!(
        pipeline
            .metrics
            .par3
            .recovery_articles_requested_total
            .load(Relaxed),
        1,
        "only the owed carrier's article was requested"
    );
    let promoted = pipeline.jobs[&job_id]
        .download_queue
        .count_matching(|work| {
            work.priority == crate::pipeline::repair::PROMOTED_RECOVERY_PRIORITY
                && work.segment_id.file_id.file_index == complete_carrier
        });
    assert_eq!(
        promoted, 0,
        "the complete carrier's spent segment took no part of the window"
    );
    let owed = pipeline.jobs[&job_id]
        .download_queue
        .count_matching(|work| {
            work.priority == crate::pipeline::repair::PROMOTED_RECOVERY_PRIORITY
                && work.segment_id.file_id.file_index == incomplete_carrier
        });
    assert_eq!(
        owed, 1,
        "the window went to the carrier that still owes bytes"
    );
}

/// The fixture payload with only its smallest protected file damaged, so a
/// repair rebuilds ten bytes while the five-thousand-byte file beside it is
/// verified and never touched.
fn payload_with_only_the_small_file_damaged() -> Vec<(String, Vec<u8>)> {
    let mut payload: Vec<(String, Vec<u8>)> = damaged_payload(&[])
        .into_iter()
        .map(|(name, bytes)| (name.to_string(), bytes))
        .collect();
    payload[1].1[0] ^= 0xff;
    payload
}

/// What the fixture set's carriers are, in the order a job announces them.
fn fixture_carriers() -> Vec<(String, Vec<u8>)> {
    vec![
        ("set.par3".to_string(), INDEX.to_vec()),
        ("set.vol0+1.par3".to_string(), VOLUME.to_vec()),
    ]
}

/// Deliverable: output planning refuses before the first output byte when the
/// working directory cannot hold what the repair will write, and the verdict
/// names the shortfall in bytes.
///
/// Only the ten-byte file is damaged, so the shortfall proves what was
/// counted: twenty bytes, the one rebuilt output and its staging copy, with
/// the nine thousand intact bytes beside it excluded.
#[tokio::test]
async fn output_planning_names_its_byte_shortfall_before_any_output_byte() {
    use crate::pipeline::direct_store::wiring::DirectStoreRuntime;
    use crate::pipeline::direct_store::{DirectStoreGate, DirectStoreSettings};

    let root = TempDir::new().unwrap();
    let job_id = JobId(52205);
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    // Every byte on the volume is reserved, so the probe reports room for
    // nothing at all and planning has to say so.
    pipeline.direct_store = DirectStoreRuntime::with_settings(DirectStoreSettings {
        gate: DirectStoreGate::Enabled,
        holds_disk_reserve_bytes: u64::MAX,
        ..Default::default()
    });
    let payload = payload_with_only_the_small_file_damaged();
    let damaged = payload[1].1.clone();
    let verdict = run_par3_job(&mut pipeline, job_id, &payload, &fixture_carriers()).await;
    let failure = verdict
        .failure
        .clone()
        .unwrap_or_else(|| panic!("planning must refuse: {verdict:#?}"));
    assert!(
        failure.contains("PAR3 output planning is 20 bytes short"),
        "only the rebuilt output and its staging copy are counted: {failure}"
    );
    assert!(
        failure.contains("needs 20 bytes"),
        "the verdict names what installing this set needs: {failure}"
    );
    assert_ne!(
        verdict.classes[Par3OutcomeClass::NoOutputSpace.index()],
        0,
        "the refusal was counted under its own class"
    );
    assert_eq!(
        tokio::fs::read(verdict.working.join("b.txt"))
            .await
            .unwrap(),
        damaged,
        "no output byte was written"
    );
}

/// Deliverable: a file the repair will not rewrite never counts against the
/// free space. An intact file larger than everything the disk will grant must
/// not turn a ten-byte repair into a refusal.
///
/// The intact file is deliberately tens of megabytes: the window between "the
/// bytes that will be written" and "every byte the set protects" has to be
/// wide enough that the free space other work on the same volume consumes
/// while this test runs cannot close it.
#[tokio::test]
async fn an_intact_file_larger_than_the_free_space_does_not_refuse_the_plan() {
    use crate::pipeline::direct_store::wiring::DirectStoreRuntime;
    use crate::pipeline::direct_store::{DirectStoreGate, DirectStoreSettings};

    const INTACT_BYTES: usize = 32 << 20;
    /// Comfortably under the intact file, comfortably over the ten-byte
    /// repair, and far wider than any plausible disk movement beside it.
    const GRANTED: u64 = 20 << 20;

    let root = TempDir::new().unwrap();
    let job_id = JobId(52210);
    let source_dir = root.path().join("protected");
    std::fs::create_dir_all(&source_dir).unwrap();
    let intact: Vec<u8> = (0..INTACT_BYTES)
        .map(|i| (i as u32 * 7 + 3) as u8)
        .collect();
    let small = b"kestrel".to_vec();
    std::fs::write(source_dir.join("alpha.bin"), &intact).unwrap();
    std::fs::write(source_dir.join("small.bin"), &small).unwrap();
    let report = par3_rs::create(
        &par3_rs::InputSpec::new(
            &source_dir,
            &[
                std::path::PathBuf::from("alpha.bin"),
                std::path::PathBuf::from("small.bin"),
            ],
        ),
        &root.path().join("kestrel"),
        &par3_rs::CreateOptions::default()
            .with_block_size(1 << 20)
            .with_recovery(par3_rs::RecoveryAmount::Blocks(2)),
    )
    .expect("a set over one large file and one small one");
    let carriers: Vec<(String, Vec<u8>)> = report
        .files_written
        .iter()
        .map(|path| {
            (
                path.file_name().unwrap().to_string_lossy().into_owned(),
                std::fs::read(path).unwrap(),
            )
        })
        .collect();
    let mut damaged = small.clone();
    damaged[0] ^= 0xff;
    let payload = vec![
        ("alpha.bin".to_string(), intact.clone()),
        ("small.bin".to_string(), damaged),
    ];

    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    // Set the job up and scan its carriers first: the database, the payload
    // and the carriers are all on disk by then, so the reserve below is taken
    // against the free space planning will actually see.
    scan_carriers_only(&mut pipeline, job_id, &payload, &carriers).await;
    let working = pipeline.jobs[&job_id].working_dir.clone();
    let available = crate::operations::disk::probe_disk_space(&working)
        .expect("a probe of the job's own directory")
        .available_bytes;
    pipeline.direct_store = DirectStoreRuntime::with_settings(DirectStoreSettings {
        gate: DirectStoreGate::Enabled,
        holds_disk_reserve_bytes: available.saturating_sub(GRANTED),
        ..Default::default()
    });

    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .extract_matching(|_| true);
    for _ in 0..64 {
        if !pipeline.check_par3_completion(job_id).await {
            break;
        }
        settle_par3_recovery(&mut pipeline, job_id).await;
    }
    assert_eq!(
        pipeline.metrics.par3.outcomes()[Par3OutcomeClass::NoOutputSpace.index()],
        0,
        "an intact file is not a claim on the disk"
    );
    assert!(
        !matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Failed { .. })
        ),
        "the plan was not refused: {:?}",
        job_status_for_assert(&pipeline, job_id)
    );
    assert_eq!(
        std::fs::read(working.join("small.bin")).unwrap(),
        small,
        "the damaged file was reconstructed"
    );
}

/// Deliverable: a set that names a file weaver will not create fails the job
/// before any output byte, and says which rule refused the name.
///
/// Unix only: the refused name is a Windows reserved device name, and the
/// protected file has to exist on disk for a genuine set to be built over it.
#[cfg(unix)]
#[tokio::test]
async fn a_set_naming_a_reserved_device_fails_before_any_output_byte() {
    let root = TempDir::new().unwrap();
    let job_id = JobId(52206);
    let source_dir = root.path().join("protected");
    std::fs::create_dir_all(&source_dir).unwrap();
    let alpha: Vec<u8> = (0..5000u32).map(|i| (i * 11 + 5) as u8).collect();
    let device = b"kestrel".to_vec();
    std::fs::write(source_dir.join("alpha.bin"), &alpha).unwrap();
    std::fs::write(source_dir.join("CON"), &device).unwrap();
    // The engine's own creation API builds the set. It stores the name as
    // given, so this is a genuine PAR3 set over a genuinely named file — not a
    // hand-edited packet.
    let report = par3_rs::create(
        &par3_rs::InputSpec::new(
            &source_dir,
            &[
                std::path::PathBuf::from("alpha.bin"),
                std::path::PathBuf::from("CON"),
            ],
        ),
        &root.path().join("kestrel"),
        &par3_rs::CreateOptions::default()
            .with_block_size(2000)
            .with_recovery(par3_rs::RecoveryAmount::Blocks(4)),
    )
    .expect("the engine builds a set over the reserved name");
    let carriers: Vec<(String, Vec<u8>)> = report
        .files_written
        .iter()
        .map(|path| {
            (
                path.file_name().unwrap().to_string_lossy().into_owned(),
                std::fs::read(path).unwrap(),
            )
        })
        .collect();
    let mut damaged = alpha.clone();
    damaged[300] ^= 0xff;
    let payload = vec![
        ("alpha.bin".to_string(), damaged.clone()),
        ("CON".to_string(), device),
    ];
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    let verdict = run_par3_job(&mut pipeline, job_id, &payload, &carriers).await;
    let failure = verdict
        .failure
        .clone()
        .unwrap_or_else(|| panic!("the set names a file weaver will not create: {verdict:#?}"));
    assert!(
        failure.contains("weaver will not create"),
        "the verdict refuses the name: {failure}"
    );
    assert!(
        failure.contains("reserved device name"),
        "the verdict names the rule that refused it: {failure}"
    );
    assert!(
        failure.contains("CON"),
        "the verdict names the path: {failure}"
    );
    assert_ne!(
        verdict.classes[Par3OutcomeClass::UnsafePath.index()],
        0,
        "the refusal was counted under its own class"
    );
    assert_eq!(
        std::fs::read(verdict.working.join("alpha.bin")).unwrap(),
        damaged,
        "no output byte was written"
    );
}

/// Append a packet of the test's choosing to a carrier, built and hashed by the
/// engine's own builder so it authenticates like every other packet in the set.
fn carrier_with(carrier: &[u8], body: par3_rs::packet::PacketBody) -> Vec<u8> {
    let set_id = par3_rs::InputSetId(carrier[32..40].try_into().expect("packet header"));
    let mut bytes = carrier.to_vec();
    bytes.extend_from_slice(&par3_rs::packet::Packet::new(set_id, body).to_bytes());
    bytes
}

/// Drive a job to the point where every carrier has been scanned, without
/// running the completion check that would consume the option-packet report.
async fn scan_carriers_only(
    pipeline: &mut Pipeline,
    job_id: JobId,
    payload: &[(String, Vec<u8>)],
    carriers: &[(String, Vec<u8>)],
) {
    let mut files: Vec<(String, Vec<u8>)> = payload.to_vec();
    let first_carrier = files.len() as u32;
    files.extend(carriers.iter().cloned());
    let mut spec = standalone_job_spec(
        "par3 carriers",
        &files
            .iter()
            .map(|(name, bytes)| (name.clone(), bytes.len() as u32))
            .collect::<Vec<_>>(),
    );
    for file in &mut spec.files {
        file.role = FileRole::from_filename(&file.filename);
    }
    let working = insert_active_job(pipeline, job_id, spec).await;
    tokio::fs::create_dir_all(working.join("sub"))
        .await
        .unwrap();
    for (index, (name, bytes)) in files.iter().enumerate() {
        write_and_complete_file(pipeline, job_id, index as u32, name, bytes).await;
    }
    for offset in 0..carriers.len() as u32 {
        pipeline
            .try_load_par3_metadata(
                job_id,
                NzbFileId {
                    job_id,
                    file_index: first_carrier + offset,
                },
            )
            .await;
        settle_par3_recovery(pipeline, job_id).await;
    }
}

/// Deliverable: a permission option packet is reported exactly once per job and
/// never applied, and the set it rides along with still repairs.
#[tokio::test]
async fn an_option_packet_is_reported_once_and_never_installed() {
    let root = TempDir::new().unwrap();
    let job_id = JobId(52207);
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    let damage = [300usize];
    let payload: Vec<(String, Vec<u8>)> = damaged_payload(&damage)
        .into_iter()
        .map(|(name, bytes)| (name.to_string(), bytes))
        .collect();
    let carriers = vec![(
        "set.vol0+1.par3".to_string(),
        carrier_with(
            VOLUME,
            par3_rs::packet::PacketBody::Opaque {
                packet_type: par3_rs::packet::PacketType::UnixPermissions,
                body: vec![0xa4, 0x01, 0, 0],
            },
        ),
    )];
    scan_carriers_only(&mut pipeline, job_id, &payload, &carriers).await;
    let runtime = pipeline.par3_runtime.as_mut().expect("admitted job");
    let report = runtime
        .take_option_packet_report(job_id)
        .expect("the option packet is worth one line");
    assert_eq!(report.present, 1, "{report}");
    assert!(
        runtime.take_option_packet_report(job_id).is_none(),
        "the line is owed once per job, not once per completion check"
    );

    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .extract_matching(|_| true);
    for _ in 0..64 {
        if !pipeline.check_par3_completion(job_id).await {
            break;
        }
        settle_par3_recovery(&mut pipeline, job_id).await;
    }
    assert!(
        !matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Failed { .. })
        ),
        "an option packet never fails a job"
    );
    let working = pipeline.jobs[&job_id].working_dir.clone();
    assert_eq!(
        tokio::fs::read(working.join("a.bin")).await.unwrap(),
        damaged_payload(&[]).into_iter().next().unwrap().1,
        "the protected bytes were still reconstructed"
    );
}

/// Deliverable: an option packet a File packet points at but that nothing
/// authenticated is counted as unresolved and does not block recovery.
#[tokio::test]
async fn an_unresolved_option_reference_does_not_block_recovery() {
    let root = TempDir::new().unwrap();
    let job_id = JobId(52208);
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    let damage = [300usize];
    let payload: Vec<(String, Vec<u8>)> = damaged_payload(&damage)
        .into_iter()
        .map(|(name, bytes)| (name.to_string(), bytes))
        .collect();
    let carriers = vec![(
        "set.vol0+1.par3".to_string(),
        carrier_with(
            VOLUME,
            par3_rs::packet::PacketBody::File(par3_rs::packet::file::FilePacket {
                name: "kestrel.bin".into(),
                quick_rolling_hash: 0,
                fingerprint: [0; 16],
                option_hashes: vec![[0x5a; 16]],
                chunks: Vec::new(),
            }),
        ),
    )];
    scan_carriers_only(&mut pipeline, job_id, &payload, &carriers).await;
    let report = pipeline
        .par3_runtime
        .as_mut()
        .expect("admitted job")
        .take_option_packet_report(job_id)
        .expect("a dangling option reference is worth one line");
    assert_eq!(report.referenced, 1, "{report}");
    assert_eq!(report.unresolved, 1, "{report}");

    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .extract_matching(|_| true);
    for _ in 0..64 {
        if !pipeline.check_par3_completion(job_id).await {
            break;
        }
        settle_par3_recovery(&mut pipeline, job_id).await;
    }
    assert!(
        !matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Failed { .. })
        ),
        "an unresolved option reference never fails a job"
    );
    let working = pipeline.jobs[&job_id].working_dir.clone();
    assert_eq!(
        tokio::fs::read(working.join("a.bin")).await.unwrap(),
        damaged_payload(&[]).into_iter().next().unwrap().1,
        "recovery ran to completion"
    );
}

/// Deliverable: metadata built to exhaust the host stops at a named engine
/// ceiling instead, the verdict names that ceiling, and the completion check
/// still reaches an answer and stops.
#[tokio::test]
async fn hostile_metadata_fails_with_the_ceiling_it_reached() {
    let root = TempDir::new().unwrap();
    let job_id = JobId(52209);
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    // One authenticated Creator packet per fabricated input set, each built by
    // the engine's own builder. Nothing is malformed: the hostility is that
    // the carrier claims more input sets than one job will ever hold open.
    let mut hostile = Vec::new();
    for index in 0..400u64 {
        let mut id = [0u8; 8];
        id.copy_from_slice(&index.to_le_bytes());
        hostile.extend_from_slice(
            &par3_rs::packet::Packet::new(
                par3_rs::InputSetId(id),
                par3_rs::packet::PacketBody::Creator(par3_rs::packet::creator::CreatorPacket::new(
                    "kestrel",
                )),
            )
            .to_bytes(),
        );
    }
    let payload: Vec<(String, Vec<u8>)> = damaged_payload(&[300])
        .into_iter()
        .map(|(name, bytes)| (name.to_string(), bytes))
        .collect();
    let carriers = vec![("set.par3".to_string(), hostile)];
    let verdict = run_par3_job(&mut pipeline, job_id, &payload, &carriers).await;
    let failure = verdict
        .failure
        .clone()
        .unwrap_or_else(|| panic!("a set past an engine ceiling fails: {verdict:#?}"));
    assert!(
        failure.contains("exceeds an engine execution limit"),
        "{failure}"
    );
    assert!(
        failure.contains("job PAR3 set count"),
        "the verdict names the ceiling it reached: {failure}"
    );
    assert_ne!(
        verdict.classes[Par3OutcomeClass::NotExecutable.index()],
        0,
        "the refusal was counted under its own class"
    );
    assert!(
        verdict.terminated,
        "the completion check reached its verdict and stopped"
    );
}
