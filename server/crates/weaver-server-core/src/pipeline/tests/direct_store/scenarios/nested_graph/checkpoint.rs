use super::*;
use crate::pipeline::direct_store::manifest::graph::JobGraph;
use crate::pipeline::direct_store::restart::ArchiveRestoreRows;
use crate::pipeline::direct_store::set::DirectSet;

#[tokio::test]
async fn rejected_tolerated_ticket_joins_exact_scope_and_native_producer_before_cleanup() {
    use crate::pipeline::extraction::runtime::ArchiveRuntime;
    use crate::pipeline::extraction::session::InputVersion;
    let directory = tempfile::tempdir().unwrap();
    let staging = directory.path().join("retired-staging");
    std::fs::create_dir(&staging).unwrap();
    let job = JobId(41984);
    let (mut pipeline, _, _) = new_direct_pipeline(&directory).await;
    let mut runtime = ArchiveRuntime::new();
    let lease = runtime.admit(None, InputVersion(0)).unwrap();
    let owner = lease.owner;
    let (joined, joined_rx) = tokio::sync::oneshot::channel();
    let observed = staging.clone();
    runtime.track_worker(
        owner,
        tokio::spawn(async move {
            lease.cancelled().await;
            assert!(observed.exists());
            joined.send(()).unwrap();
        }),
    );
    pipeline.archive_runtime.insert(job, runtime);
    let observed = staging.clone();
    let native = tokio::spawn(async move {
        joined_rx.await.unwrap();
        assert!(observed.exists());
        Ok(FullSetExtractionOutcome {
            extracted: Vec::new(),
            failed: Vec::new(),
            selected_password: None,
        })
    });
    pipeline
        .handle_direct_tolerated_done(DirectToleratedWorkDone {
            set_name: "retired".into(),
            job_id: job,
            work_id: 1,
            set_index: 0,
            used_chase: false,
            chase_owner: Some(owner),
            retry_after_chase: true,
            result: Err("discarded ticket".into()),
            retirement: Some(
                crate::pipeline::completion::finalize::extract::ChaseRetirement::for_test(
                    staging.clone(),
                    native,
                ),
            ),
        })
        .await;
    assert!(!staging.exists());
    assert!(!pipeline.archive_runtime[&job].sessions.accepts(owner));
}

fn record_write(
    root: &DirectSetRouter,
    graph: &mut NestedGraph,
    write: &NodeSpan,
    placements: &mut Vec<(
        crate::pipeline::direct_store::router::PlacementReceipt,
        PathBuf,
    )>,
    covered: &mut ByteRanges,
) {
    let router = graph.router(root, write.owner);
    let path = match write.span.destination {
        DirectDestination::Envelope { volume_index } => router.plan().envelope_path(volume_index),
        DirectDestination::Member { member_id } => {
            let partial = router
                .member_partials()
                .into_iter()
                .find(|(id, _, _)| *id == member_id)
                .unwrap()
                .2;
            router.plan().destination_path(partial)
        }
    };
    for source in place(root, graph, write) {
        covered.insert(source.offset, source.len);
    }
    placements.push((write.placement(), path));
}

#[tokio::test]
async fn job_graph_cas_replays_legacy_prefix_and_restart_rederives_missing_delivery() {
    let directory = tempfile::tempdir().unwrap();
    let job = JobId(41983);
    let inner = stored(SetFormat::Rar, "leaf.bin", &vec![43; 32_769]);
    let volumes = single_member_store_set("inner.rar", &inner, 1);
    let outer = &volumes[0].1;
    let spec = direct_store_job_spec("prefix replay", &volumes);
    let (mut pipeline, _, _) = new_direct_pipeline(&directory).await;
    insert_active_job(&mut pipeline, job, spec.clone()).await;
    let working = pipeline.jobs[&job].working_dir.clone();
    let destination = pipeline.deterministic_extraction_staging_dir(job);
    pipeline.jobs.get_mut(&job).unwrap().staging_dir = Some(destination.clone());
    let plan = DirectSetPlan::discover(&spec, &working, &destination)
        .0
        .remove(0);
    let mut root = DirectSetRouter::new(plan.clone());
    root.note_declared_volume_size(0, outer.len() as u64)
        .unwrap();
    let mut empty = NestedGraph::default();
    let mut placements = Vec::new();
    let mut covered = ByteRanges::new();
    for span in root
        .route(
            0,
            0,
            &[bytes::Bytes::copy_from_slice(&outer[..outer.len() / 2])],
        )
        .unwrap()
    {
        let write = NodeSpan {
            owner: ROOT_NODE,
            generation: 0,
            raw_child: None,
            span,
        };
        empty.forward(&root, ROOT_NODE, Vec::new()).unwrap();
        record_write(&root, &mut empty, &write, &mut placements, &mut covered);
    }
    let mut set = DirectSet::new(job, plan);
    set.router = root;
    for (receipt, path) in placements {
        set.seed_qualified_placement(
            receipt.owner,
            receipt.destination,
            path,
            vec![(
                receipt.destination_offset,
                receipt.destination_offset + receipt.len,
            )],
        );
    }
    set.seed_root_source_coverage(0, covered.ranges().to_vec());
    assert!(!set.nested_replay_members().is_empty());
    pipeline.direct_store.install_restored(job, vec![set]);
    // This is the global writer path, without the promotion caller's replay.
    pipeline.commit_archive_graph(job).await.unwrap();
    let set = pipeline.direct_store.set(job, 0).unwrap();
    assert!(set.has_nested_checkpoint_authority());
    assert_eq!(set.nested_graph().unwrap().nodes.len(), 1);
    assert!(set.nested_replay_members().is_empty());
    let selected = pipeline.db.load_archive_checkpoint(job).unwrap().unwrap();
    let bytes = crate::pipeline::direct_store::manifest::ManifestStore::for_job(&working, job)
        .read(&selected.reference)
        .unwrap();
    let selected = JobGraph::decode(&bytes).unwrap();
    // The selected first CAS precedes in-memory replay. A crash here has no
    // child binding; its authoritative parent partial must seed delivery again.
    assert!(selected.roots[0].nodes.is_empty());
    let restored = pipeline
        .restore_direct_store_coverage(
            job,
            &spec,
            &working,
            &HashMap::new(),
            ArchiveRestoreRows::Graph(Box::new(selected)),
        )
        .await
        .unwrap();
    assert!(!restored.sets[0].nested_replay_members().is_empty());
    pipeline.direct_store.install_restored(job, restored.sets);
    pipeline.replay_direct_archive_prefixes(job).await.unwrap();
    pipeline.replay_direct_archive_prefixes(job).await.unwrap();
    let set = pipeline.direct_store.set(job, 0).unwrap();
    assert_eq!(set.nested_graph().unwrap().nodes.len(), 1);
    assert!(set.nested_replay_members().is_empty());
}

#[tokio::test]
async fn graph_checkpoint_restores_holes_raw_overlays_and_rekeys_parent_members() {
    let directory = tempfile::tempdir().unwrap();
    let job_id = JobId(41982);
    let payload: Vec<u8> = (0..16_385).map(|index| (index % 251) as u8).collect();
    let inner = stored(SetFormat::Rar, "leaf.bin", &payload);
    let volumes = single_member_store_set("inner.rar", &inner, 1);
    let outer = &volumes[0].1;
    let spec = direct_store_job_spec("graph checkpoint", &volumes);
    let (mut pipeline, _, _) = new_direct_pipeline(&directory).await;
    let working = directory.path().join("working");
    let destination = pipeline.deterministic_extraction_staging_dir(job_id);
    let plan = DirectSetPlan::discover(&spec, &working, &destination)
        .0
        .remove(0);
    let mut root = DirectSetRouter::new(plan.clone());
    root.note_declared_volume_size(0, outer.len() as u64)
        .unwrap();
    let mut graph = NestedGraph::default();
    let mut placements = Vec::new();
    let mut covered = ByteRanges::new();
    let middle = outer.len() / 2;
    let writes = root
        .route(0, 0, &[bytes::Bytes::copy_from_slice(&outer[..middle])])
        .unwrap();
    let writes = graph.forward(&root, ROOT_NODE, writes).unwrap();
    let mut withheld = false;
    for write in writes {
        if !withheld
            && write.owner == 1
            && matches!(write.span.destination, DirectDestination::Member { .. })
        {
            withheld = true;
            continue;
        }
        record_write(&root, &mut graph, &write, &mut placements, &mut covered);
    }
    assert!(withheld);
    graph.node_mut(1).mode = NodeMode::MaterializationPending(DemotionReason::HoldsScratchCeiling);
    let writes = root
        .route(
            0,
            middle as u64,
            &[bytes::Bytes::copy_from_slice(&outer[middle..])],
        )
        .unwrap();
    for write in graph.forward(&root, ROOT_NODE, writes).unwrap() {
        record_write(&root, &mut graph, &write, &mut placements, &mut covered);
    }
    let tail = root.note_volume_complete(0, outer.len() as u64).unwrap();
    for write in graph.forward(&root, ROOT_NODE, tail).unwrap() {
        record_write(&root, &mut graph, &write, &mut placements, &mut covered);
    }
    assert!(!graph.node(1).inputs[&0].raw_placed.is_empty());
    graph.node_mut(1).inputs.get_mut(&0).unwrap().length = None;
    let mut set = DirectSet::new(job_id, plan);
    set.router = root;
    set.install_restored_nested(graph);
    for (receipt, path) in placements {
        set.seed_qualified_placement(
            receipt.owner,
            receipt.destination,
            path,
            vec![(
                receipt.destination_offset,
                receipt.destination_offset + receipt.len,
            )],
        );
    }
    set.seed_root_source_coverage(0, covered.ranges().to_vec());
    let mut saved = JobGraph::empty(1);
    crate::pipeline::direct_store::graph_checkpoint::capture_direct_root(&mut saved, &set).unwrap();
    // Simulate a historical member number that differs from the canonical
    // facts rebuild. The durable parent path remains the identity.
    let mut historical_plan = set.nested_graph().unwrap().node(1).router.plan().clone();
    historical_plan.volumes.insert(0, 99);
    historical_plan.files = HashMap::from([(99, 0)]);
    saved.roots[0].nodes[0].inputs[0].parent_member = 99;
    let mut metadata =
        crate::pipeline::direct_store::snapshot::decode(&saved.roots[0].nodes[0].snapshot).unwrap();
    metadata.plan_digest = historical_plan.digest(
        &set.nested_graph()
            .unwrap()
            .node(1)
            .router
            .member_digest_entries(),
    );
    saved.roots[0].nodes[0].snapshot =
        crate::pipeline::direct_store::snapshot::encode(&metadata).unwrap();
    let saved = JobGraph::decode(&saved.encode().unwrap()).unwrap();
    let mut restored = pipeline
        .restore_direct_store_coverage(
            job_id,
            &spec,
            &working,
            &HashMap::new(),
            ArchiveRestoreRows::Graph(Box::new(saved)),
        )
        .await
        .unwrap();
    let set = restored.sets.remove(0);
    let graph = set.nested_graph().unwrap();
    assert_ne!(graph.node(1).inputs[&0].parent_member, 99);
    assert_eq!(graph.node(1).inputs[&0].length, None);
    assert!(!graph.node(1).inputs[&0].raw_placed.is_empty());
    let image = graph.input_views(&set.router, 1).remove(&0).unwrap();
    let ranges = image.readable_ranges();
    assert!(!image.covered.missing(0, inner.len() as u64).is_empty());
    let mut reader = VirtualVolumeReader::<true>::new(image, Arc::default());
    for (start, end) in ranges {
        reader.seek(SeekFrom::Start(start)).unwrap();
        let mut bytes = vec![0; (end - start) as usize];
        reader.read_exact(&mut bytes).unwrap();
        assert_eq!(bytes, inner[start as usize..end as usize]);
    }
}
