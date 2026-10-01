//! Every arrival permutation of two volumes with two articles each, with a
//! duplicate, restart, or demotion at every nonterminal position.
use super::*;

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

#[derive(Clone, Copy, Debug)]
pub(super) enum Interruption {
    None,
    Restart(usize),
    Demote(usize),
    Loss { mask: u8, index_first: bool },
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
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete) = new_direct_pipeline(&root).await;
    pipeline.direct_store.set_gate(gate);
    let job = JobId(42200);
    let output = complete.join(crate::jobs::working_dir::sanitize_dirname(&spec.name));
    let recovery = if let Interruption::Loss { .. } = interruption {
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
    if matches!(
        interruption,
        Interruption::Loss {
            index_first: true,
            ..
        }
    ) {
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
    for (step, &(file, article)) in order.iter().enumerate() {
        if matches!(interruption, Interruption::Loss { mask, .. } if mask & (1 << (file * 2 + article)) != 0)
        {
            trace.push(format!("lost {file}:{article}"));
            continue;
        }
        submit_volume_article_of(&mut pipeline, job, volumes, file, article, 2).await;
        trace.push(format!(
            "arrive {file}:{article}: {:?}",
            pipeline.direct_store.sets_for(job)
        ));
        match interruption {
            Interruption::Demote(at) if step + 1 == at => {
                pipeline
                    .demote_direct_set(job, 0, DemotionReason::HoldsBudgetExceeded)
                    .await;
                settle_direct_post_repair_work(&mut pipeline).await;
                trace.push(format!("demote at {at}"));
            }
            Interruption::Restart(at) if step + 1 == at => {
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
                trace.push(format!("restart at {at}"));
            }
            _ => {}
        }
    }
    if matches!(
        interruption,
        Interruption::Loss {
            index_first: false,
            ..
        }
    ) {
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
        if let Interruption::Loss { mask, .. } = interruption {
            let state = pipeline.jobs.get_mut(&job).unwrap();
            state.recovery_queue = crate::DownloadQueue::new();
            let queued = state.download_queue.drain_all();
            for work in queued {
                let file = work.segment_id.file_id.file_index;
                let article = work.segment_id.segment_number;
                if file < volumes.len() as u32 && mask & (1 << (file * 2 + article)) == 0 {
                    submit_volume_article_of(&mut pipeline, job, volumes, file, article, 2).await;
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
            if let Interruption::Loss { mask, .. } = interruption {
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

async fn campaign(format: Format) {
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
    if encrypted {
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
    for (case, (order, interruption)) in schedules().into_iter().enumerate() {
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
    campaign(Format::Rar4).await;
}
#[tokio::test]
async fn rar5_arrival_schedules() {
    campaign(Format::Rar5).await;
}
#[tokio::test]
async fn rar4_encrypted_arrival_schedules() {
    campaign(Format::Rar4Encrypted).await;
}
#[tokio::test]
async fn rar4_unsalted_arrival_schedules() {
    campaign(Format::Rar4Unsalted).await;
}
#[tokio::test]
async fn rar5_encrypted_arrival_schedules() {
    campaign(Format::Rar5Encrypted).await;
}
#[tokio::test]
async fn rar5_keyed_checksum_arrival_schedules() {
    campaign(Format::Rar5KeyedChecksum).await;
}
#[tokio::test]
async fn rar5_header_encrypted_arrival_schedules() {
    campaign(Format::Rar5EncryptedHeaders).await;
}
#[tokio::test]
async fn rar5_unchecked_header_arrival_schedules() {
    campaign(Format::Rar5UncheckedHeaders).await;
}
#[tokio::test]
async fn quick_open_arrival_schedules() {
    campaign(Format::QuickOpen).await;
}
#[tokio::test]
async fn blake2_arrival_schedules() {
    campaign(Format::Blake2).await;
}
