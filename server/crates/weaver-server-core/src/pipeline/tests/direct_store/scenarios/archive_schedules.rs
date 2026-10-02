//! Bounded exhaustive schedules over four articles: one or two volumes,
//! every arrival permutation, loss subset and single duplicate/interruption.
//! This enumerates delivery boundaries, not background worker or filesystem
//! interleavings; those require separate tests that force the competing events.
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
    pub chase_armed: u64,
    pub chase_consumed: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ExtractionProfile {
    DirectStore,
    Chase,
    Conventional,
}

impl ExtractionProfile {
    fn configure(self, pipeline: &mut Pipeline) {
        use crate::pipeline::direct_unpack::settings::{DirectUnpackGate, DirectUnpackSettings};
        use crate::pipeline::direct_unpack::wiring::DirectUnpackRuntime;
        pipeline
            .direct_store
            .set_gate(if self == Self::DirectStore {
                DirectStoreGate::Enabled
            } else {
                DirectStoreGate::Disabled
            });
        pipeline.direct_unpack = DirectUnpackRuntime::with_settings(DirectUnpackSettings {
            gate: if self == Self::Conventional {
                DirectUnpackGate::Disabled
            } else {
                DirectUnpackGate::Enabled
            },
        });
    }

    pub(super) fn includes(self, interruption: Interruption) -> bool {
        // Conventional extraction has no speculative output to demote. Keep
        // its arrival, loss and restart cases without counting no-op demotions.
        self != Self::Conventional
            || !matches!(
                interruption,
                Interruption::Demote(_)
                    | Interruption::Combined {
                        action: BoundaryAction::Demote,
                        ..
                    }
            )
    }

    pub(super) fn assert_route(self, outcome: &Outcome) {
        if self != Self::DirectStore {
            assert_eq!(outcome.finalized, 0, "{:?}", outcome.trace);
        }
        if self == Self::Conventional {
            assert_eq!(outcome.chase_armed, 0, "{:?}", outcome.trace);
            assert_eq!(outcome.chase_consumed, 0, "{:?}", outcome.trace);
        }
    }
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

pub(super) async fn run_schedule(
    gate: DirectStoreGate,
    spec: JobSpec,
    volumes: &[(String, Vec<u8>)],
    order: &[(u32, u32)],
    wanted: &[&str],
    interruption: Interruption,
) -> Outcome {
    let profile = match gate {
        DirectStoreGate::Enabled => ExtractionProfile::DirectStore,
        DirectStoreGate::Disabled => ExtractionProfile::Chase,
    };
    run_profile_schedule(profile, spec, volumes, order, wanted, interruption).await
}

async fn deliver_schedule_article(
    pipeline: &mut Pipeline,
    job: JobId,
    volumes: &[(String, Vec<u8>)],
    file: u32,
    article: u32,
    articles: usize,
) {
    let id = SegmentId {
        file_id: NzbFileId {
            job_id: job,
            file_index: file,
        },
        segment_number: article,
    };
    let state = pipeline.jobs.get_mut(&job).unwrap();
    // The schedule owns delivery, including deliberate duplicates. Retire a
    // queued copy when present so a later drain cannot fabricate a rewrite.
    for queue in [&mut state.download_queue, &mut state.recovery_queue] {
        let queued = queue.drain_all();
        for work in queued {
            if work.segment_id != id {
                queue.push(work);
            }
        }
    }
    submit_volume_article_of(pipeline, job, volumes, file, article, articles).await;
}

pub(super) async fn run_profile_schedule(
    profile: ExtractionProfile,
    mut spec: JobSpec,
    volumes: &[(String, Vec<u8>)],
    order: &[(u32, u32)],
    wanted: &[&str],
    interruption: Interruption,
) -> Outcome {
    enable_schedule_trace();
    let articles = spec.files[0].segments.len();
    assert_eq!(volumes.len() * articles, 4, "four scheduled article slots");
    assert!(
        spec.files[..volumes.len()]
            .iter()
            .all(|file| file.segments.len() == articles)
    );
    let order: Vec<_> = order
        .iter()
        .map(|&(file, article)| {
            let slot = file * 2 + article;
            (slot / articles as u32, slot % articles as u32)
        })
        .collect();
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete) = new_direct_pipeline(&root).await;
    profile.configure(&mut pipeline);
    let mut chase_armed = 0;
    let mut chase_consumed = 0;
    let job = JobId(42200);
    let output = complete.join(crate::jobs::working_dir::sanitize_dirname(&spec.name));
    let loss = interruption.loss();
    let recovery = if loss.is_some() {
        // Keep several repair blocks per article even for larger compressed
        // fixtures, without turning extraction scheduling into a codec benchmark.
        let slice = volumes
            .iter()
            .map(|(_, bytes)| bytes.len())
            .max()
            .unwrap()
            .div_ceil(32)
            .div_ceil(4)
            * 4;
        let slice = slice.max(PAR2_SLICE_BYTES as usize);
        let blocks = volumes
            .iter()
            .map(|(_, bytes)| bytes.len().div_ceil(slice))
            .sum();
        let described: Vec<_> = volumes
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        let bytes = build_test_par2_with_recovery(&described, slice as u64, blocks);
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
                if profile == ExtractionProfile::DirectStore {
                    pipeline
                        .demote_direct_set(job, 0, DemotionReason::HoldsBudgetExceeded)
                        .await;
                }
                // An incompatible set may already have left direct store and
                // started chase. Withdraw that owner at the same boundary too.
                use crate::pipeline::direct_unpack::wiring::{
                    AbortLatch, DemotionReason as ChaseDemotion,
                };
                pipeline.direct_unpack_abort_job(
                    job,
                    "schedule withdraws speculative extraction",
                    AbortLatch::Permanent,
                    ChaseDemotion::MemoryYielded,
                );
                settle_direct_post_repair_work(&mut pipeline).await;
                trace.push(format!("demote at {step}"));
            }
            BoundaryAction::Restart => {
                pipeline
                    .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
                    .await;
                let counters = pipeline.direct_unpack.counters();
                chase_armed += counters.armed;
                chase_consumed += counters.consumed;
                pipeline.direct_unpack_shutdown("schedule restart").await;
                drop(pipeline);
                (pipeline, _, _) = new_direct_pipeline(&root).await;
                profile.configure(&mut pipeline);
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
        if loss.is_some_and(|(mask, _)| mask & (1 << (file * articles as u32 + article)) != 0) {
            trace.push(format!("lost {file}:{article}"));
            continue;
        }
        deliver_schedule_article(&mut pipeline, job, volumes, file, article, articles).await;
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
                if file < volumes.len() as u32
                    && mask & (1 << (file * articles as u32 + article)) == 0
                {
                    submit_volume_article_of(&mut pipeline, job, volumes, file, article, articles)
                        .await;
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
                    if file < volumes.len() as u32
                        && mask & (1 << (file * articles as u32 + article)) == 0
                    {
                        submit_volume_article_of(
                            &mut pipeline,
                            job,
                            volumes,
                            file,
                            article,
                            articles,
                        )
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
                    dispatch_and_submit(&mut pipeline, job, volumes, file, article, articles).await;
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
    let counters = pipeline.direct_unpack.counters();
    chase_armed += counters.armed;
    chase_consumed += counters.consumed;
    trace.push(format!(
        "profile={profile:?}; chase_armed={chase_armed}; chase_consumed={chase_consumed}; current={counters:?}"
    ));
    Outcome {
        status,
        files,
        trace,
        finalized: pipeline.direct_store.finalized_sets,
        chase_armed,
        chase_consumed,
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
    profile_campaign(format, shard, ExtractionProfile::DirectStore).await;
}

async fn chase_campaign(format: Format, shard: Option<usize>) {
    profile_campaign(format, shard, ExtractionProfile::Chase).await;
}

async fn conventional_campaign(format: Format, shard: Option<usize>) {
    profile_campaign(format, shard, ExtractionProfile::Conventional).await;
}

async fn profile_campaign(format: Format, shard: Option<usize>, profile: ExtractionProfile) {
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
    let baseline = run_profile_schedule(
        ExtractionProfile::Conventional,
        spec.clone(),
        &volumes,
        &in_order_arrivals(2),
        &[name],
        Interruption::None,
    )
    .await;
    assert_eq!(baseline.status, Some(JobStatus::Complete));
    ExtractionProfile::Conventional.assert_route(&baseline);
    assert!(
        baseline.files[name].as_deref() == Some(payload.as_slice()),
        "conventional oracle {format:?}"
    );
    if encrypted && (shard.is_none() || shard == Some(0)) {
        for order in arrival_orders() {
            let mut wrong = spec.clone();
            wrong.password = Some("incorrect-key".to_string());
            let rejected = run_profile_schedule(
                profile,
                wrong,
                &volumes,
                &order,
                &[name],
                Interruption::None,
            )
            .await;
            profile.assert_route(&rejected);
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
        if !profile.includes(interruption) {
            continue;
        }
        eprintln!(
            "{format:?} profile={profile:?} shard={shard:?} case={case} order={order:?} interruption={interruption:?}"
        );
        let actual = run_profile_schedule(
            profile,
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
        profile.assert_route(&actual);
        // A duplicate can invalidate an already-running RAR chase. Clean
        // unique arrivals must consume chase; duplicate schedules still must
        // admit it and produce the same verified output through safe fallback.
        let unique_arrivals = order.len() == 4;
        if profile == ExtractionProfile::Chase && matches!(interruption, Interruption::None) {
            assert!(actual.chase_armed > 0, "{format:?}: {:?}", actual.trace);
            if unique_arrivals {
                assert_eq!(actual.chase_consumed, 1, "{format:?}: {:?}", actual.trace);
            }
        }
        if profile == ExtractionProfile::DirectStore && matches!(interruption, Interruption::None) {
            let expected = usize::from(!matches!(
                format,
                Format::Rar5UncheckedHeaders | Format::Blake2
            ));
            assert_eq!(
                actual.finalized, expected,
                "{format:?} case={case} order={order:?}: {:?}",
                actual.trace
            );
            if expected == 0 {
                assert!(actual.chase_armed > 0, "{format:?}: {:?}", actual.trace);
                if unique_arrivals {
                    assert_eq!(actual.chase_consumed, 1, "{format:?}: {:?}", actual.trace);
                }
            }
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

#[derive(Clone, Copy, Debug)]
enum CompressedFormat {
    Rar4Mixed,
    Rar4Lz,
    Rar4Solid,
    Rar4SolidEncrypted,
    Rar4SolidHeaders,
    Rar4Ppmd,
    Rar4PpmdEncrypted,
    Rar4PpmdHeaders,
    Rar4Encrypted,
    Rar4Headers,
    Rar5Mixed,
    Rar5Lz,
    Rar5Encrypted,
    Rar5Headers,
    Rar5Solid,
    Rar5SolidEncrypted,
    Rar5SolidHeaders,
}

impl CompressedFormat {
    fn fixture(self) -> (&'static str, &'static [u8], Option<&'static str>) {
        // Real RAR encoders produced these archives. Embed them so the compiled
        // nextest archive remains self-contained on a matrix runner.
        macro_rules! fixture {
            ($name:literal, $password:expr $(,)?) => {
                (
                    $name,
                    include_bytes!(concat!(
                        env!("CARGO_MANIFEST_DIR"),
                        "/tests/fixtures/extraction_profiles/",
                        $name
                    ))
                    .as_slice(),
                    $password,
                )
            };
        }
        match self {
            Self::Rar4Mixed => fixture!("rar4_multifile_lz.rar", None),
            Self::Rar4Lz => fixture!("rar4_lz.rar", None),
            Self::Rar4Solid => fixture!("rar4_lz_solid_mv.rar", None),
            Self::Rar4SolidEncrypted => {
                fixture!("rar4_solid_lz_encrypted.rar", Some("moonlit-harbour"))
            }
            Self::Rar4SolidHeaders => {
                fixture!("rar4_solid_lz_headers.rar", Some("moonlit-harbour"))
            }
            Self::Rar4Ppmd => fixture!("rar4_solid_ppmd_plain.rar", None),
            Self::Rar4PpmdEncrypted => {
                fixture!("rar4_solid_ppmd_encrypted.rar", Some("moonlit-harbour"))
            }
            Self::Rar4PpmdHeaders => {
                fixture!("rar4_solid_ppmd_headers.rar", Some("moonlit-harbour"))
            }
            Self::Rar4Encrypted => fixture!("rar4_enc_lz.rar", Some("testpass123")),
            Self::Rar4Headers => fixture!("rar4_hp_lz.rar", Some("secretpass")),
            Self::Rar5Mixed => fixture!("rar5_multifile_lz.rar", None),
            Self::Rar5Lz => fixture!("rar5_lz.rar", None),
            Self::Rar5Encrypted => fixture!("rar5_enc_lz.rar", Some("testpass123")),
            Self::Rar5Headers => fixture!("rar5_hp_lz.rar", Some("secretpass")),
            Self::Rar5Solid => fixture!("rar5_solid_small.rar", None),
            Self::Rar5SolidEncrypted => {
                fixture!("rar5_solid_encrypted_small.rar", Some("moonlit-harbour"),)
            }
            Self::Rar5SolidHeaders => {
                fixture!("rar5_solid_headers_small.rar", Some("moonlit-harbour"),)
            }
        }
    }
}

async fn compressed_direct_campaign(format: CompressedFormat, shard: Option<usize>) {
    compressed_campaign(format, shard, ExtractionProfile::DirectStore).await;
}

async fn compressed_chase_campaign(format: CompressedFormat, shard: Option<usize>) {
    compressed_campaign(format, shard, ExtractionProfile::Chase).await;
}

async fn compressed_conventional_campaign(format: CompressedFormat, shard: Option<usize>) {
    compressed_campaign(format, shard, ExtractionProfile::Conventional).await;
}

async fn compressed_campaign(
    format: CompressedFormat,
    shard: Option<usize>,
    profile: ExtractionProfile,
) {
    let (fixture_name, bytes, password) = format.fixture();
    let mut archive = match password {
        Some(password) => {
            unrar_rs::RarArchive::open_with_password(std::io::Cursor::new(bytes), password)
        }
        None => unrar_rs::RarArchive::open(std::io::Cursor::new(bytes)),
    }
    .unwrap();
    let mut expected = BTreeMap::new();
    for index in 0..archive.len() {
        let info = archive.member_info(index).unwrap();
        if info.is_directory {
            continue;
        }
        let mut output = Vec::new();
        archive
            .by_index(index)
            .unwrap()
            .copy_to(&mut output)
            .unwrap();
        expected.insert(info.name, output);
    }
    assert!(!expected.is_empty());
    // Pin the oracle to the official reader, independently of the Rust reader
    // used both here and in the pipeline. A shared decoder error cannot bless
    // the bytes subsequently compared by every schedule.
    let oracle: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/extraction_profiles/expected.json"
    )))
    .unwrap();
    let oracle = oracle["archives"][fixture_name].as_object().unwrap();
    assert_eq!(expected.len(), oracle.len());
    for (name, bytes) in &expected {
        use sha2::Digest;
        assert_eq!(bytes.len() as u64, oracle[name]["size"].as_u64().unwrap());
        assert_eq!(
            hex::encode(sha2::Sha256::digest(bytes)),
            oracle[name]["sha256"].as_str().unwrap()
        );
    }
    let wanted = expected.keys().map(String::as_str).collect::<Vec<_>>();
    let volumes = vec![("compressed.rar".to_owned(), bytes.to_vec())];
    let mut spec = direct_store_job_spec_with_articles("Compressed archive schedules", &volumes, 4);
    spec.password = password.map(str::to_owned);
    if password.is_some() && shard == Some(0) {
        for order in arrival_orders() {
            let mut wrong = spec.clone();
            wrong.password = Some("incorrect-key".to_owned());
            let outcome = run_profile_schedule(
                profile,
                wrong,
                &volumes,
                &order,
                &wanted,
                Interruption::None,
            )
            .await;
            assert!(
                matches!(outcome.status, Some(JobStatus::Failed { .. })),
                "{format:?}: {:?}",
                outcome.trace
            );
            assert_eq!(outcome.finalized, 0, "{:?}", outcome.trace);
            assert!(
                outcome.files.values().all(Option::is_none),
                "wrong password published output: {format:?}"
            );
            profile.assert_route(&outcome);
        }
    }
    for (case, (order, interruption)) in combined_schedules(shard.unwrap()) {
        if !profile.includes(interruption) {
            continue;
        }
        eprintln!(
            "{format:?} profile={profile:?} shard={shard:?} case={case} order={order:?} interruption={interruption:?}"
        );
        let outcome = run_profile_schedule(
            profile,
            spec.clone(),
            &volumes,
            &order,
            &wanted,
            interruption,
        )
        .await;
        assert_eq!(
            outcome.status,
            Some(JobStatus::Complete),
            "{format:?}: {:?}",
            outcome.trace
        );
        profile.assert_route(&outcome);
        if profile != ExtractionProfile::Conventional && interruption == Interruption::None {
            assert!(outcome.chase_armed > 0, "{format:?}: {:?}", outcome.trace);
            if order.len() == 4 {
                assert_eq!(outcome.chase_consumed, 1, "{format:?}: {:?}", outcome.trace);
                if profile == ExtractionProfile::DirectStore {
                    assert_eq!(
                        outcome.finalized,
                        usize::from(matches!(
                            format,
                            CompressedFormat::Rar4Mixed | CompressedFormat::Rar5Mixed
                        )),
                        "{format:?}: {:?}",
                        outcome.trace
                    );
                }
            }
        }
        for (name, bytes) in &expected {
            assert_eq!(
                outcome.files[name].as_deref(),
                Some(bytes.as_slice()),
                "{format:?} member={name} case={case}: {:?}",
                outcome.trace
            );
        }
    }
}

// These are test names, not runner jobs. Nextest partitions the named shards
// across the bounded CI runner matrix. Every shard is independently replayable.
macro_rules! combined_campaign {
    ($module:ident, $variant:expr, $run:ident) => {
        mod $module {
            use super::*;
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_00() {
                $run($variant, Some(0)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_01() {
                $run($variant, Some(1)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_02() {
                $run($variant, Some(2)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_03() {
                $run($variant, Some(3)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_04() {
                $run($variant, Some(4)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_05() {
                $run($variant, Some(5)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_06() {
                $run($variant, Some(6)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_07() {
                $run($variant, Some(7)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_08() {
                $run($variant, Some(8)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_09() {
                $run($variant, Some(9)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_10() {
                $run($variant, Some(10)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_11() {
                $run($variant, Some(11)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_12() {
                $run($variant, Some(12)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_13() {
                $run($variant, Some(13)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_14() {
                $run($variant, Some(14)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_15() {
                $run($variant, Some(15)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_16() {
                $run($variant, Some(16)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_17() {
                $run($variant, Some(17)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_18() {
                $run($variant, Some(18)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_19() {
                $run($variant, Some(19)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_20() {
                $run($variant, Some(20)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_21() {
                $run($variant, Some(21)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_22() {
                $run($variant, Some(22)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_23() {
                $run($variant, Some(23)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_24() {
                $run($variant, Some(24)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_25() {
                $run($variant, Some(25)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_26() {
                $run($variant, Some(26)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_27() {
                $run($variant, Some(27)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_28() {
                $run($variant, Some(28)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_29() {
                $run($variant, Some(29)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn shard_30() {
                $run($variant, Some(30)).await;
            }
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
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

combined_campaign!(combined_chase_rar4, Format::Rar4, chase_campaign);
combined_campaign!(combined_chase_rar5, Format::Rar5, chase_campaign);
combined_campaign!(
    combined_chase_rar4_encrypted,
    Format::Rar4Encrypted,
    chase_campaign
);
combined_campaign!(
    combined_chase_rar4_unsalted,
    Format::Rar4Unsalted,
    chase_campaign
);
combined_campaign!(
    combined_chase_rar5_encrypted,
    Format::Rar5Encrypted,
    chase_campaign
);
combined_campaign!(
    combined_chase_rar5_keyed_checksum,
    Format::Rar5KeyedChecksum,
    chase_campaign
);
combined_campaign!(
    combined_chase_rar5_header_encrypted,
    Format::Rar5EncryptedHeaders,
    chase_campaign
);
combined_campaign!(
    combined_chase_rar5_unchecked_header,
    Format::Rar5UncheckedHeaders,
    chase_campaign
);
combined_campaign!(combined_chase_quick_open, Format::QuickOpen, chase_campaign);
combined_campaign!(combined_chase_blake2, Format::Blake2, chase_campaign);

combined_campaign!(
    combined_conventional_rar4,
    Format::Rar4,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_rar5,
    Format::Rar5,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_rar4_encrypted,
    Format::Rar4Encrypted,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_rar4_unsalted,
    Format::Rar4Unsalted,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_rar5_encrypted,
    Format::Rar5Encrypted,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_rar5_keyed_checksum,
    Format::Rar5KeyedChecksum,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_rar5_header_encrypted,
    Format::Rar5EncryptedHeaders,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_rar5_unchecked_header,
    Format::Rar5UncheckedHeaders,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_quick_open,
    Format::QuickOpen,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_blake2,
    Format::Blake2,
    conventional_campaign
);

combined_campaign!(
    combined_compressed_direct_rar4_mixed,
    CompressedFormat::Rar4Mixed,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_lz,
    CompressedFormat::Rar4Lz,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_encrypted,
    CompressedFormat::Rar4Encrypted,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_headers,
    CompressedFormat::Rar4Headers,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_direct_rar5_mixed,
    CompressedFormat::Rar5Mixed,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_direct_rar5_lz,
    CompressedFormat::Rar5Lz,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_direct_rar5_encrypted,
    CompressedFormat::Rar5Encrypted,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_direct_rar5_headers,
    CompressedFormat::Rar5Headers,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_mixed,
    CompressedFormat::Rar4Mixed,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_lz,
    CompressedFormat::Rar4Lz,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_encrypted,
    CompressedFormat::Rar4Encrypted,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_headers,
    CompressedFormat::Rar4Headers,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_chase_rar5_mixed,
    CompressedFormat::Rar5Mixed,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_chase_rar5_lz,
    CompressedFormat::Rar5Lz,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_chase_rar5_encrypted,
    CompressedFormat::Rar5Encrypted,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_chase_rar5_headers,
    CompressedFormat::Rar5Headers,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_mixed,
    CompressedFormat::Rar4Mixed,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_lz,
    CompressedFormat::Rar4Lz,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_encrypted,
    CompressedFormat::Rar4Encrypted,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_headers,
    CompressedFormat::Rar4Headers,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar5_mixed,
    CompressedFormat::Rar5Mixed,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar5_lz,
    CompressedFormat::Rar5Lz,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar5_encrypted,
    CompressedFormat::Rar5Encrypted,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar5_headers,
    CompressedFormat::Rar5Headers,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_direct_rar5_solid,
    CompressedFormat::Rar5Solid,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar5_solid,
    CompressedFormat::Rar5Solid,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar5_solid,
    CompressedFormat::Rar5Solid,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_direct_rar5_solid_encrypted,
    CompressedFormat::Rar5SolidEncrypted,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar5_solid_encrypted,
    CompressedFormat::Rar5SolidEncrypted,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar5_solid_encrypted,
    CompressedFormat::Rar5SolidEncrypted,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_direct_rar5_solid_headers,
    CompressedFormat::Rar5SolidHeaders,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar5_solid_headers,
    CompressedFormat::Rar5SolidHeaders,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar5_solid_headers,
    CompressedFormat::Rar5SolidHeaders,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_solid,
    CompressedFormat::Rar4Solid,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_solid,
    CompressedFormat::Rar4Solid,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_solid,
    CompressedFormat::Rar4Solid,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_solid_encrypted,
    CompressedFormat::Rar4SolidEncrypted,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_solid_encrypted,
    CompressedFormat::Rar4SolidEncrypted,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_solid_encrypted,
    CompressedFormat::Rar4SolidEncrypted,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_solid_headers,
    CompressedFormat::Rar4SolidHeaders,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_solid_headers,
    CompressedFormat::Rar4SolidHeaders,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_solid_headers,
    CompressedFormat::Rar4SolidHeaders,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_ppmd,
    CompressedFormat::Rar4Ppmd,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_ppmd,
    CompressedFormat::Rar4Ppmd,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_ppmd,
    CompressedFormat::Rar4Ppmd,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_ppmd_encrypted,
    CompressedFormat::Rar4PpmdEncrypted,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_ppmd_encrypted,
    CompressedFormat::Rar4PpmdEncrypted,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_ppmd_encrypted,
    CompressedFormat::Rar4PpmdEncrypted,
    compressed_conventional_campaign
);
combined_campaign!(
    combined_compressed_direct_rar4_ppmd_headers,
    CompressedFormat::Rar4PpmdHeaders,
    compressed_direct_campaign
);
combined_campaign!(
    combined_compressed_chase_rar4_ppmd_headers,
    CompressedFormat::Rar4PpmdHeaders,
    compressed_chase_campaign
);
combined_campaign!(
    combined_compressed_conventional_rar4_ppmd_headers,
    CompressedFormat::Rar4PpmdHeaders,
    compressed_conventional_campaign
);
