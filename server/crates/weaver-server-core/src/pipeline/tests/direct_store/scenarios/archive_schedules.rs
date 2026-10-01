//! Every arrival permutation of two volumes with two articles each, with a
//! duplicate, restart, or demotion at every nonterminal position.
use super::*;

fn enable_schedule_trace() {
    if std::env::var_os("WEAVER_ARCHIVE_SCHEDULE_TRACE").is_none() {
        return;
    }
    struct Trace(std::sync::atomic::AtomicU64);
    impl tracing::Subscriber for Trace {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(
                self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1,
            )
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Fields(String);
            impl tracing::field::Visit for Fields {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    use std::fmt::Write;
                    let _ = write!(self.0, " {field}={value:?}");
                }
            }
            let mut fields = Fields(String::new());
            event.record(&mut fields);
            eprintln!("{}{}", event.metadata().target(), fields.0);
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }
    let _ = tracing::subscriber::set_global_default(Trace(std::sync::atomic::AtomicU64::new(0)));
}

pub(super) fn arrival_orders() -> Vec<Vec<(u32, u32)>> {
    fn permute(at: usize, items: &mut [(u32, u32)], output: &mut Vec<Vec<(u32, u32)>>) {
        if at == items.len() {
            output.push(items.to_vec());
            return;
        }
        for i in at..items.len() {
            items.swap(at, i);
            permute(at + 1, items, output);
            items.swap(at, i);
        }
    }
    let mut orders = Vec::new();
    permute(0, &mut [(0, 0), (0, 1), (1, 0), (1, 1)], &mut orders);
    assert_eq!(orders.len(), 24);
    orders
}

pub(super) fn duplicate_orders() -> Vec<Vec<(u32, u32)>> {
    let mut result = vec![];
    for order in arrival_orders() {
        result.push(order.clone());
        for at in 0..3 {
            let mut duplicate = order.clone();
            duplicate.insert(at, order[at]);
            result.push(duplicate);
        }
    }
    result
}

pub(super) struct Outcome {
    pub status: Option<JobStatus>,
    pub files: BTreeMap<String, Option<Vec<u8>>>,
    pub trace: Vec<String>,
    pub finalized: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum BoundaryAction {
    None,
    Restart,
    Demote,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Interruption {
    None,
    Restart(usize),
    Demote(usize),
    Loss {
        mask: u8,
        index_first: bool,
    },
    Combined {
        mask: u8,
        index_first: bool,
        action: BoundaryAction,
        at: usize,
    },
}

impl Interruption {
    fn loss(self) -> Option<(u8, bool)> {
        match self {
            Self::Loss { mask, index_first }
            | Self::Combined {
                mask, index_first, ..
            } => Some((mask, index_first)),
            _ => None,
        }
    }

    fn action_at(self, boundary: usize) -> BoundaryAction {
        match self {
            Self::Restart(at) if at == boundary => BoundaryAction::Restart,
            Self::Demote(at) if at == boundary => BoundaryAction::Demote,
            Self::Combined { action, at, .. } if at == boundary => action,
            _ => BoundaryAction::None,
        }
    }
}

type Schedule = (Vec<(u32, u32)>, Interruption);

pub(super) fn combined_schedules(shard: usize) -> Vec<(usize, Schedule)> {
    let mut orders = std::collections::BTreeSet::new();
    for order in arrival_orders() {
        orders.insert(order.clone());
        // Repeat any earlier article at every nonterminal insertion point.
        for at in 1..order.len() {
            for &article in &order[..at] {
                let mut duplicate = order.clone();
                duplicate.insert(at, article);
                orders.insert(duplicate);
            }
        }
    }
    assert_eq!(orders.len(), 168);
    let mut cases = std::collections::BTreeSet::new();
    for mask in 1u8..16 {
        for order in &orders {
            let available =
                |&&(file, article): &&(u32, u32)| mask & (1 << (file * 2 + article)) == 0;
            let received = order.iter().filter(available).copied().collect::<Vec<_>>();
            for index_first in [false, true] {
                cases.insert((
                    received.clone(),
                    Interruption::Combined {
                        mask,
                        index_first,
                        action: BoundaryAction::None,
                        at: 0,
                    },
                ));
                for boundary in 0..=order.len() {
                    let at = order[..boundary].iter().filter(available).count();
                    for action in [BoundaryAction::Restart, BoundaryAction::Demote] {
                        cases.insert((
                            received.clone(),
                            Interruption::Combined {
                                mask,
                                index_first,
                                action,
                                at,
                            },
                        ));
                    }
                }
            }
        }
    }
    // Lost arrivals have no publication effect. Identical received-event and
    // interruption sequences are one case, regardless of which lost item was
    // attempted first. Keep all distinct boundaries, including empty prefixes.
    assert_eq!(cases.len(), 4518);
    let mut cases: Vec<_> = cases.into_iter().collect();
    // Also cross delayed duplicates with interruption when nothing was lost.
    // A complete clean arrival sequence may already finalize the job, so its
    // interruption boundaries stop before the last distinct article arrives.
    // Append these cases to keep the repair cases' replay indices stable.
    for order in orders {
        cases.push((order.clone(), Interruption::None));
        for at in 0..order.len() {
            cases.push((order.clone(), Interruption::Restart(at)));
            cases.push((order.clone(), Interruption::Demote(at)));
        }
    }
    assert_eq!(cases.len(), 6318);
    assert!(shard < 32);
    let selected = std::env::var("WEAVER_ARCHIVE_COMBINED_CASE")
        .ok()
        .map(|selection| {
            if let Some((start, end)) = selection.split_once("..") {
                start.parse::<usize>().expect("decimal range start")
                    ..end.parse::<usize>().expect("decimal range end")
            } else {
                let case = selection.parse::<usize>().expect("decimal combined case");
                case..case.checked_add(1).expect("combined case in range")
            }
        });
    assert!(
        selected
            .as_ref()
            .is_none_or(|range| range.start < range.end && range.end <= cases.len())
    );
    cases
        .into_iter()
        .enumerate()
        .filter(|(case, _)| {
            case % 32 == shard && selected.as_ref().is_none_or(|range| range.contains(case))
        })
        .collect()
}

pub(super) fn schedules() -> Vec<(Vec<(u32, u32)>, Interruption)> {
    let mut result: Vec<_> = duplicate_orders()
        .into_iter()
        .map(|order| (order, Interruption::None))
        .collect();
    for order in arrival_orders() {
        for at in 1..4 {
            result.push((order.clone(), Interruption::Restart(at)));
            result.push((order.clone(), Interruption::Demote(at)));
        }
    }
    for mask in 1..16 {
        for index_first in [false, true] {
            result.push((
                in_order_arrivals(2),
                Interruption::Loss { mask, index_first },
            ));
        }
    }
    if let Ok(case) = std::env::var("WEAVER_ARCHIVE_SCHEDULE_CASE") {
        let case = case
            .parse::<usize>()
            .expect("decimal archive schedule index");
        vec![
            result
                .get(case)
                .expect("archive schedule index in range")
                .clone(),
        ]
    } else {
        result
    }
}

pub(super) async fn run_archive(
    gate: DirectStoreGate,
    spec: JobSpec,
    volumes: &[(String, Vec<u8>)],
    order: &[(u32, u32)],
    wanted: &[&str],
) -> Outcome {
    run_schedule(gate, spec, volumes, order, wanted, Interruption::None).await
}

pub(super) async fn run_schedule(
    gate: DirectStoreGate,
    mut spec: JobSpec,
    volumes: &[(String, Vec<u8>)],
    order: &[(u32, u32)],
    wanted: &[&str],
    interruption: Interruption,
) -> Outcome {
    enable_schedule_trace();
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete) = new_direct_pipeline(&root).await;
    pipeline.direct_store.set_gate(gate);
    let job = JobId(42200);
    let output = complete.join(crate::jobs::working_dir::sanitize_dirname(&spec.name));
    let loss = interruption.loss();
    let recovery = if loss.is_some() {
        let blocks = volumes
            .iter()
            .map(|(_, bytes)| bytes.len().div_ceil(PAR2_SLICE_BYTES as usize))
            .sum();
        let bytes = repairable_par2_index(volumes, blocks);
        let index = append_par2_index(&mut spec, &bytes);
        Some((index, bytes))
    } else {
        None
    };
    let working_dir = insert_active_job(&mut pipeline, job, spec.clone()).await;
    let mut trace = vec![];
    if loss.is_some_and(|(_, index_first)| index_first) {
        let (index, bytes) = recovery.as_ref().unwrap();
        submit_decoded_segment(
            &mut pipeline,
            NzbFileId {
                job_id: job,
                file_index: *index,
            },
            0,
            0,
            bytes,
            "silver.horizon.par2",
            None,
        )
        .await;
    }
    for step in 0..=order.len() {
        match interruption.action_at(step) {
            BoundaryAction::Demote => {
                pipeline
                    .demote_direct_set(job, 0, DemotionReason::HoldsBudgetExceeded)
                    .await;
                settle_direct_post_repair_work(&mut pipeline).await;
                trace.push(format!("demote at {step}"));
            }
            BoundaryAction::Restart => {
                pipeline
                    .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
                    .await;
                drop(pipeline);
                (pipeline, _, _) = new_direct_pipeline(&root).await;
                pipeline.direct_store.set_gate(gate);
                pipeline
                    .restore_job(RestoreJobRequest {
                        job_id: job,
                        job_hash: [0; 32],
                        spec: spec.clone(),
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
                trace.push(format!("restart at {step}"));
            }
            BoundaryAction::None => {}
        }
        let Some(&(file, article)) = order.get(step) else {
            break;
        };
        if loss.is_some_and(|(mask, _)| mask & (1 << (file * 2 + article)) != 0) {
            trace.push(format!("lost {file}:{article}"));
            continue;
        }
        submit_volume_article_of(&mut pipeline, job, volumes, file, article, 2).await;
        trace.push(format!(
            "arrive {file}:{article}: {:?}",
            pipeline.direct_store.sets_for(job)
        ));
    }
    if loss.is_some_and(|(_, index_first)| !index_first) {
        let (index, bytes) = recovery.as_ref().unwrap();
        submit_decoded_segment(
            &mut pipeline,
            NzbFileId {
                job_id: job,
                file_index: *index,
            },
            0,
            0,
            bytes,
            "silver.horizon.par2",
            None,
        )
        .await;
    }
    // Every wait below is for a registered operation. A demotion can request
    // more articles; service those before waiting for an extraction result.
    for _ in 0..128 {
        if let Some((mask, _)) = loss {
            let state = pipeline.jobs.get_mut(&job).unwrap();
            let mut queued = state.download_queue.drain_all();
            queued.extend(state.recovery_queue.drain_all());
            for work in queued {
                let file = work.segment_id.file_id.file_index;
                let article = work.segment_id.segment_number;
                if file < volumes.len() as u32 && mask & (1 << (file * 2 + article)) == 0 {
                    submit_volume_article_of(&mut pipeline, job, volumes, file, article, 2).await;
                } else if let Some((index, bytes)) = &recovery
                    && file == *index
                {
                    submit_decoded_segment(
                        &mut pipeline,
                        NzbFileId {
                            job_id: job,
                            file_index: *index,
                        },
                        0,
                        0,
                        bytes,
                        "silver.horizon.par2",
                        None,
                    )
                    .await;
                }
            }
        }
        drain_rar_refreshes(&mut pipeline).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
        if matches!(
            job_status_for_assert(&pipeline, job),
            Some(JobStatus::Complete | JobStatus::Failed { .. })
        ) {
            break;
        }
        let mut queued = peek_queued_segments(&mut pipeline, job);
        queued.dedup();
        if !queued.is_empty() {
            if let Some((mask, _)) = loss {
                // A container probe can re-request its missing first article
                // while the queues settle. Answer that request as unavailable
                // before completion checks exhaustion, just like a server does.
                let state = pipeline.jobs.get_mut(&job).unwrap();
                state.download_queue = crate::DownloadQueue::new();
                state.recovery_queue = crate::DownloadQueue::new();
                for (file, article) in queued {
                    if file < volumes.len() as u32 && mask & (1 << (file * 2 + article)) == 0 {
                        submit_volume_article_of(&mut pipeline, job, volumes, file, article, 2)
                            .await;
                    }
                }
            } else {
                trace.push(format!("refetch {queued:?}"));
                for (file, article) in queued {
                    if matches!(
                        job_status_for_assert(&pipeline, job),
                        Some(JobStatus::Complete | JobStatus::Failed { .. })
                    ) {
                        break;
                    }
                    dispatch_and_submit(&mut pipeline, job, volumes, file, article, 2).await;
                }
                continue;
            }
        }
        // Drive the actor's quiescent-write action explicitly. A demoted
        // partial volume can leave a short write behind after the queue drains.
        pipeline.flush_quiescent_write_backlog().await;
        pipeline.check_job_completion(job).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
        if matches!(
            job_status_for_assert(&pipeline, job),
            Some(JobStatus::Complete | JobStatus::Failed { .. })
        ) {
            break;
        }
        if !peek_queued_segments(&mut pipeline, job).is_empty() {
            continue;
        }
        if pipeline
            .inflight_extractions
            .get(&job)
            .is_some_and(|sets| !sets.is_empty())
            || pipeline.has_active_rar_workers(job)
        {
            let done = pipeline
                .extract_done_rx
                .recv()
                .await
                .expect("registered extraction receipt");
            pipeline.handle_extraction_done(done).await;
        } else if recovery.is_none() {
            panic!(
                "archive stalled without an outstanding operation: {} trace={trace:?}",
                debug_job_state(&pipeline, job)
            );
        }
    }
    let status = job_status_for_assert(&pipeline, job);
    let queued = if pipeline.jobs.contains_key(&job) {
        peek_queued_segments(&mut pipeline, job)
    } else {
        Vec::new()
    };
    trace.push(format!(
        "terminal: {}; analyses={} repairs={}; par2={:?}; queued={queued:?}; sets={:?}",
        debug_job_state(&pipeline, job),
        pipeline.par2_repairer_analyze_calls,
        pipeline.par2_repairer_execute_calls,
        pipeline
            .par2_set(job)
            .map(|set| (set.files.len(), set.recovery_slices.len())),
        pipeline.direct_store.sets_for(job)
    ));
    let files = wanted
        .iter()
        .map(|name| ((*name).to_string(), std::fs::read(output.join(name)).ok()))
        .collect();
    settle_direct_output_removals(root.path()).await;
    Outcome {
        status,
        files,
        trace,
        finalized: pipeline.direct_store.finalized_sets,
    }
}

#[tokio::test]
async fn conventional_par2_scan_cannot_borrow_direct_scratch_awaiting_removal() {
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job = JobId(42201);
    let volumes = quick_open_store_set("feature.mkv", &[42; 193], None);
    let index = repairable_par2_index(&volumes, 0);
    let mut spec = direct_store_job_spec("Scratch scan lifetime", &volumes);
    let index_file = append_par2_index(&mut spec, &index);
    let working = insert_active_job(&mut pipeline, job, spec).await;
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id: job,
            file_index: index_file,
        },
        0,
        0,
        &index,
        "silver.horizon.par2",
        None,
    )
    .await;
    let set = pipeline.par2_set(job).unwrap().clone();
    let plan = pipeline.direct_store.set(job, 0).unwrap().plan().clone();
    let scratch_paths = [
        plan.envelope_path(0),
        plan.repair_path(0),
        plan.holds_scratch_path(),
    ];
    for scratch in scratch_paths {
        // Deterministically hold the pre-unlink state. These bytes would
        // satisfy a source scan, but their owner is free to delete them before
        // execution: conventional repair must never retain them as evidence.
        std::fs::write(&scratch, &volumes[0].1).unwrap();
        let mut options = par2_rs::Par2RepairSessionOptions::new(working.clone(), vec![]);
        options.file_set = Some((*set).clone());
        options.exclude_paths = pipeline.par2_extra_scan_exclusions(job, set.recovery_set_id);
        let mut session = par2_rs::Par2RepairSession::open(options).unwrap();
        let outcome = session.analyze().unwrap();
        assert_eq!(outcome.available_blocks, 0, "scratch={scratch:?}");
        assert_eq!(outcome.status, par2_rs::Par2RepairStatus::Insufficient);
        std::fs::remove_file(&scratch).unwrap();
    }
}

#[tokio::test]
async fn encrypted_restart_repair_preserves_direct_finalization() {
    let name = "nested/feature.mkv";
    let password = "moonlit-harbour";
    let payload: Vec<u8> = (0..6001).map(|n| ((n * 7 + n / 251) % 253) as u8).collect();
    for salt in [Some(TEST_RAR4_SALT), None] {
        let volumes = encrypted_rar4_store_set(name, &payload, 2, password, salt);
        let mut spec = direct_store_job_spec("Encrypted restart repair", &volumes);
        spec.password = Some(password.to_string());
        let outcome = run_schedule(
            DirectStoreGate::Enabled,
            spec,
            &volumes,
            &[(0, 0), (0, 0), (1, 0), (1, 1)],
            &[name],
            Interruption::Combined {
                mask: 2,
                index_first: false,
                action: BoundaryAction::Restart,
                at: 1,
            },
        )
        .await;
        assert_eq!(
            outcome.status,
            Some(JobStatus::Complete),
            "{:?}",
            outcome.trace
        );
        assert_eq!(outcome.finalized, 1, "{:?}", outcome.trace);
        assert_eq!(outcome.files[name].as_deref(), Some(payload.as_slice()));
    }
}

#[derive(Clone, Copy, Debug)]
enum Format {
    Rar4,
    Rar5,
    Rar4Encrypted,
    Rar4Unsalted,
    Rar5Encrypted,
    Rar5KeyedChecksum,
    Rar5EncryptedHeaders,
    Rar5UncheckedHeaders,
    QuickOpen,
    Blake2,
}

async fn campaign(format: Format, shard: Option<usize>) {
    let name = "nested/feature.mkv";
    let password = "moonlit-harbour";
    let length = if matches!(format, Format::QuickOpen) {
        193
    } else {
        6001
    };
    let payload: Vec<u8> = (0..length)
        .map(|n| ((n * 7 + n / 251) % 253) as u8)
        .collect();
    let volumes = match format {
        Format::Rar4 => single_member_rar4_store_set(name, &payload, 2),
        Format::Rar5 => single_member_store_set(name, &payload, 2),
        Format::Rar4Encrypted => {
            encrypted_rar4_store_set(name, &payload, 2, password, Some(TEST_RAR4_SALT))
        }
        Format::Rar4Unsalted => encrypted_rar4_store_set(name, &payload, 2, password, None),
        Format::Rar5Encrypted => {
            encrypted_store_set(name, &payload, 2, password, Some(password), false)
        }
        Format::Rar5KeyedChecksum => {
            encrypted_store_set(name, &payload, 2, password, Some(password), true)
        }
        Format::Rar5EncryptedHeaders => {
            header_encrypted_store_set(name, &payload, 2, password, HeaderCheck::For(password))
        }
        Format::Rar5UncheckedHeaders => {
            header_encrypted_store_set(name, &payload, 2, password, HeaderCheck::Absent)
        }
        Format::QuickOpen => quick_open_store_set(name, &payload, None),
        Format::Blake2 => blake2_store_set(
            name,
            &payload,
            2,
            unrar_rs::crypto::blake2sp_hash(&payload),
            true,
        ),
    };
    if matches!(format, Format::Blake2) {
        let mut archive = unrar_rs::RarArchive::open_volumes(
            volumes
                .iter()
                .map(|(_, bytes)| {
                    Box::new(std::io::Cursor::new(bytes.clone())) as Box<dyn unrar_rs::ReadSeek>
                })
                .collect(),
        )
        .unwrap();
        let mut extracted = Vec::new();
        archive
            .by_index(0)
            .unwrap()
            .copy_to(&mut extracted)
            .unwrap();
        assert_eq!(extracted, payload);
    }
    let encrypted = matches!(
        format,
        Format::Rar4Encrypted
            | Format::Rar4Unsalted
            | Format::Rar5Encrypted
            | Format::Rar5KeyedChecksum
            | Format::Rar5EncryptedHeaders
            | Format::Rar5UncheckedHeaders
    );
    let mut spec = direct_store_job_spec("Archive schedules", &volumes);
    spec.password = encrypted.then(|| password.to_string());
    let baseline = run_archive(
        DirectStoreGate::Disabled,
        spec.clone(),
        &volumes,
        &in_order_arrivals(2),
        &[name],
    )
    .await;
    assert_eq!(baseline.status, Some(JobStatus::Complete));
    assert!(
        baseline.files[name].as_deref() == Some(payload.as_slice()),
        "conventional oracle {format:?}"
    );
    if encrypted && shard.is_none() {
        for order in arrival_orders() {
            let mut wrong = spec.clone();
            wrong.password = Some("incorrect-key".to_string());
            let rejected =
                run_archive(DirectStoreGate::Enabled, wrong, &volumes, &order, &[name]).await;
            assert!(
                matches!(rejected.status, Some(JobStatus::Failed { .. })),
                "wrong password {format:?} {order:?}: {:?}",
                rejected.status
            );
            assert_eq!(rejected.finalized, 0);
            assert!(
                rejected.files[name].is_none(),
                "wrong password published output: {format:?} {order:?}"
            );
        }
    }
    let cases = shard.map_or_else(
        || schedules().into_iter().enumerate().collect(),
        combined_schedules,
    );
    for (case, (order, interruption)) in cases {
        eprintln!(
            "{format:?} shard={shard:?} case={case} order={order:?} interruption={interruption:?}"
        );
        let actual = run_schedule(
            DirectStoreGate::Enabled,
            spec.clone(),
            &volumes,
            &order,
            &[name],
            interruption,
        )
        .await;
        assert_eq!(
            actual.status,
            Some(JobStatus::Complete),
            "{format:?} case={case} order={order:?} interruption={interruption:?} trace={:?}",
            actual.trace
        );
        if matches!(interruption, Interruption::None) {
            let expected = usize::from(!matches!(
                format,
                Format::Rar5UncheckedHeaders | Format::Blake2
            ));
            assert_eq!(
                actual.finalized, expected,
                "{format:?} case={case} order={order:?}: {:?}",
                actual.trace
            );
        }
        assert_eq!(
            actual.files[name].as_deref(),
            Some(payload.as_slice()),
            "{format:?} case={case} order={order:?}"
        );
    }
}

#[tokio::test]
async fn rar4_arrival_schedules() {
    campaign(Format::Rar4, None).await;
}
#[tokio::test]
async fn rar5_arrival_schedules() {
    campaign(Format::Rar5, None).await;
}
#[tokio::test]
async fn rar4_encrypted_arrival_schedules() {
    campaign(Format::Rar4Encrypted, None).await;
}
#[tokio::test]
async fn rar4_unsalted_arrival_schedules() {
    campaign(Format::Rar4Unsalted, None).await;
}
#[tokio::test]
async fn rar5_encrypted_arrival_schedules() {
    campaign(Format::Rar5Encrypted, None).await;
}
#[tokio::test]
async fn rar5_keyed_checksum_arrival_schedules() {
    campaign(Format::Rar5KeyedChecksum, None).await;
}
#[tokio::test]
async fn rar5_header_encrypted_arrival_schedules() {
    campaign(Format::Rar5EncryptedHeaders, None).await;
}
#[tokio::test]
async fn rar5_unchecked_header_arrival_schedules() {
    campaign(Format::Rar5UncheckedHeaders, None).await;
}
#[tokio::test]
async fn quick_open_arrival_schedules() {
    campaign(Format::QuickOpen, None).await;
}
#[tokio::test]
async fn blake2_arrival_schedules() {
    campaign(Format::Blake2, None).await;
}

// These are test names, not runner jobs. Nextest partitions the named shards
// across the bounded CI runner matrix. Every shard is independently replayable.
macro_rules! combined_campaign {
    ($module:ident, $variant:expr, $run:ident) => {
        mod $module {
            use super::*;
            #[tokio::test]
            async fn shard_00() {
                $run($variant, Some(0)).await;
            }
            #[tokio::test]
            async fn shard_01() {
                $run($variant, Some(1)).await;
            }
            #[tokio::test]
            async fn shard_02() {
                $run($variant, Some(2)).await;
            }
            #[tokio::test]
            async fn shard_03() {
                $run($variant, Some(3)).await;
            }
            #[tokio::test]
            async fn shard_04() {
                $run($variant, Some(4)).await;
            }
            #[tokio::test]
            async fn shard_05() {
                $run($variant, Some(5)).await;
            }
            #[tokio::test]
            async fn shard_06() {
                $run($variant, Some(6)).await;
            }
            #[tokio::test]
            async fn shard_07() {
                $run($variant, Some(7)).await;
            }
            #[tokio::test]
            async fn shard_08() {
                $run($variant, Some(8)).await;
            }
            #[tokio::test]
            async fn shard_09() {
                $run($variant, Some(9)).await;
            }
            #[tokio::test]
            async fn shard_10() {
                $run($variant, Some(10)).await;
            }
            #[tokio::test]
            async fn shard_11() {
                $run($variant, Some(11)).await;
            }
            #[tokio::test]
            async fn shard_12() {
                $run($variant, Some(12)).await;
            }
            #[tokio::test]
            async fn shard_13() {
                $run($variant, Some(13)).await;
            }
            #[tokio::test]
            async fn shard_14() {
                $run($variant, Some(14)).await;
            }
            #[tokio::test]
            async fn shard_15() {
                $run($variant, Some(15)).await;
            }
            #[tokio::test]
            async fn shard_16() {
                $run($variant, Some(16)).await;
            }
            #[tokio::test]
            async fn shard_17() {
                $run($variant, Some(17)).await;
            }
            #[tokio::test]
            async fn shard_18() {
                $run($variant, Some(18)).await;
            }
            #[tokio::test]
            async fn shard_19() {
                $run($variant, Some(19)).await;
            }
            #[tokio::test]
            async fn shard_20() {
                $run($variant, Some(20)).await;
            }
            #[tokio::test]
            async fn shard_21() {
                $run($variant, Some(21)).await;
            }
            #[tokio::test]
            async fn shard_22() {
                $run($variant, Some(22)).await;
            }
            #[tokio::test]
            async fn shard_23() {
                $run($variant, Some(23)).await;
            }
            #[tokio::test]
            async fn shard_24() {
                $run($variant, Some(24)).await;
            }
            #[tokio::test]
            async fn shard_25() {
                $run($variant, Some(25)).await;
            }
            #[tokio::test]
            async fn shard_26() {
                $run($variant, Some(26)).await;
            }
            #[tokio::test]
            async fn shard_27() {
                $run($variant, Some(27)).await;
            }
            #[tokio::test]
            async fn shard_28() {
                $run($variant, Some(28)).await;
            }
            #[tokio::test]
            async fn shard_29() {
                $run($variant, Some(29)).await;
            }
            #[tokio::test]
            async fn shard_30() {
                $run($variant, Some(30)).await;
            }
            #[tokio::test]
            async fn shard_31() {
                $run($variant, Some(31)).await;
            }
        }
    };
}
pub(super) use combined_campaign;
combined_campaign!(combined_rar4, Format::Rar4, campaign);
combined_campaign!(combined_rar5, Format::Rar5, campaign);
combined_campaign!(combined_rar4_encrypted, Format::Rar4Encrypted, campaign);
combined_campaign!(combined_rar4_unsalted, Format::Rar4Unsalted, campaign);
combined_campaign!(combined_rar5_encrypted, Format::Rar5Encrypted, campaign);
combined_campaign!(
    combined_rar5_keyed_checksum,
    Format::Rar5KeyedChecksum,
    campaign
);
combined_campaign!(
    combined_rar5_header_encrypted,
    Format::Rar5EncryptedHeaders,
    campaign
);
combined_campaign!(
    combined_rar5_unchecked_header,
    Format::Rar5UncheckedHeaders,
    campaign
);
combined_campaign!(combined_quick_open, Format::QuickOpen, campaign);
combined_campaign!(combined_blake2, Format::Blake2, campaign);
