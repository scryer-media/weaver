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
struct Verdict {
    failure: Option<String>,
    classes: [u64; crate::operations::metrics::Par3OutcomeClass::COUNT],
}

async fn verdict_for_carrier_order(
    root: &TempDir,
    job_id: JobId,
    carrier_order: &[(&str, &[u8])],
    damage: &[usize],
) -> Verdict {
    let (mut pipeline, _, _) = new_direct_pipeline(root).await;
    let mut files: Vec<(String, Vec<u8>)> = damaged_payload(damage)
        .into_iter()
        .map(|(name, bytes)| (name.to_string(), bytes))
        .collect();
    let first_carrier = files.len() as u32;
    for (name, bytes) in carrier_order {
        files.push(((*name).to_string(), bytes.to_vec()));
    }
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
    let working = insert_active_job(&mut pipeline, job_id, spec).await;
    tokio::fs::create_dir(working.join("sub")).await.unwrap();
    for (index, (name, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, index as u32, name, bytes).await;
    }
    for offset in 0..carrier_order.len() as u32 {
        pipeline
            .try_load_par3_metadata(
                job_id,
                NzbFileId {
                    job_id,
                    file_index: first_carrier + offset,
                },
            )
            .await;
        settle_par3_recovery(&mut pipeline, job_id).await;
    }
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
    for _ in 0..16 {
        if !pipeline.check_par3_completion(job_id).await {
            break;
        }
        settle_par3_recovery(&mut pipeline, job_id).await;
    }
    Verdict {
        failure: match job_status_for_assert(&pipeline, job_id) {
            Some(JobStatus::Failed { error }) => Some(error),
            _ => None,
        },
        classes: pipeline.metrics.par3.outcomes(),
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
