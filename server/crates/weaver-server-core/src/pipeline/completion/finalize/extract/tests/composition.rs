use super::*;
use std::collections::BTreeMap;
use std::io::{Seek, SeekFrom};

use crate::pipeline::direct_store::plan::{DirectSetPlan, SetFormat};
use crate::pipeline::direct_store::router::{DirectDestination, DirectSetRouter, RoutedSpan};
use crate::pipeline::extraction::output::Integrity;
use crate::pipeline::extraction::output::channel::{ChannelOutput, Event, StreamOwner};
use crate::pipeline::extraction::session::{ArchiveSessions, InputVersion};
use crate::pipeline::extraction::storage::{Destination, DestinationRegistry, WriteTicket};

fn destination_path(router: &DirectSetRouter, destination: DirectDestination) -> PathBuf {
    match destination {
        DirectDestination::Envelope { volume_index } => router.plan().envelope_path(volume_index),
        DirectDestination::Member { member_id } => {
            let (_, _, partial) = router
                .member_partials()
                .into_iter()
                .find(|(id, _, _)| *id == member_id)
                .unwrap();
            router.plan().destination_path(partial)
        }
    }
}

fn ticket(
    registry: &mut DestinationRegistry,
    sessions: &ArchiveSessions,
    namespace: StreamOwner,
    base: &Path,
    path: &Path,
    receipt: crate::pipeline::direct_store::router::PlacementReceipt,
) -> WriteTicket {
    let destination = Destination {
        namespace,
        owner: receipt.owner,
        destination: receipt.destination,
    };
    registry
        .register(
            sessions,
            destination,
            path.strip_prefix(base).unwrap(),
            None,
        )
        .unwrap();
    registry
        .begin_write(
            sessions,
            destination,
            receipt.destination_offset,
            receipt.len,
        )
        .unwrap()
}

fn place(router: &DirectSetRouter, spans: Vec<RoutedSpan>) {
    for span in spans {
        let path = destination_path(router, span.destination);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .unwrap();
        file.seek(SeekFrom::Start(span.destination_offset)).unwrap();
        for bytes in span.bytes {
            file.write_all(&bytes).unwrap();
        }
    }
}

#[tokio::test]
async fn adaptive_decode_feeds_store_with_end_header_beyond_bridge_capacity() {
    decoded_parent_store(false).await;
}

#[tokio::test]
async fn decoded_parent_child_demotion_materializes_without_compressed_coverage() {
    decoded_parent_store(true).await;
}

async fn decoded_parent_store(force_demotion: bool) {
    let temp = TempDir::new().unwrap();
    let out_dir = temp.path().join("output");
    let working_dir = temp.path().join("working");
    fs::create_dir_all(&out_dir).unwrap();
    fs::create_dir_all(&working_dir).unwrap();
    let payload = incompressible(128 * 1024, 17);
    let inner = sevenz_archive_with_method(
        sevenz_turbo::EncoderConfiguration::new(sevenz_turbo::EncoderMethod::COPY),
        &[("leaf.bin", &payload)],
    );
    let ordinary_leaf = incompressible(256 * 1024, 41);
    let outer = sevenz_archive_with_dictionary(
        1024 * 1024,
        &[("ordinary.bin", &ordinary_leaf), ("inner.7z", &inner)],
    );
    let (root, budget) = test_extraction_security_with_memory(&out_dir, 256 * 1024 * 1024);
    let lifecycle_budget = Arc::clone(&budget);
    let mut sessions = ArchiveSessions::new(1, 0);
    let outer_node = sessions.register(None, InputVersion(1)).unwrap();
    let parent_session = sessions.start(outer_node).unwrap();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(2);
    let (output, generation) = ChannelOutput::new(
        parent_session.owner,
        budget.try_reserve_memory(8192).unwrap(),
        sender,
        tokio::runtime::Handle::current(),
        Arc::clone(&budget),
    )
    .unwrap();
    sessions.attach_channel(generation.clone()).unwrap();
    let mut catalog = crate::pipeline::extraction::catalog::decoded::DecodedCatalog::new(
        generation.clone(),
        budget.try_reserve_memory(4096).unwrap(),
    );
    let worker = tokio::task::spawn_blocking(move || {
        let (event_tx, _events) = tokio::sync::broadcast::channel(32);
        let memory = SevenZipDecodeMemory::ReservedPerPass {
            end_header_bytes: sevenz_end_header_bytes(&outer),
            widening: ChaseWidening::default(),
        };
        output.run(|output| {
            let selected = crate::pipeline::extraction::output::select::SelectOutput {
                first: output,
                second: crate::pipeline::extraction::output::channel::TrackedFiles {
                    output,
                    root: &root,
                },
                select_first: |_: usize, path: &Path| path == Path::new("inner.7z"),
            };
            extract_7z_to(
                SevenZipStreamContext {
                    job_id: JobId(41817),
                    set_name: "outer.7z",
                    budget: &budget,
                    password: &sevenz_turbo::Password::empty(),
                    event_tx: &event_tx,
                    phase_counters: &Arc::new(PhaseCounters::default()),
                    decode_memory: &memory,
                    decode_threads: 4,
                },
                &selected,
                || Ok(Cursor::new(outer.clone())),
            )
        })
    });
    use crate::pipeline::direct_store::nested::catalog::DecodedNamespace;
    use crate::pipeline::direct_store::nested::{NestedGraph, ROOT_NODE};
    let plan = DirectSetPlan {
        set_name: "outer".into(),
        format: SetFormat::SevenZip,
        volumes: BTreeMap::from([(0, 0)]),
        files: HashMap::from([(0, 0)]),
        identity: None,
        unposted: Default::default(),
        working_dir,
        destination_dir: out_dir,
    };
    let resources = crate::pipeline::direct_store::router::RouterResources {
        accountant: Arc::default(),
        holds_budget: 4096,
        scratch_ceiling: 1024 * 1024,
        sparse: Default::default(),
        migration_ceiling_bytes: 1024 * 1024,
    };
    let mut graph = NestedGraph::default();
    let mut registry =
        DestinationRegistry::new(lifecycle_budget.try_reserve_memory(64 * 1024).unwrap()).unwrap();
    let mut destinations = BTreeMap::new();
    let mut child_session = None;
    let mut demoted = false;
    let mut retained_beyond_bridge = false;
    let mut data_complete = false;
    let mut integrity = false;
    let mut physical = HashSet::new();
    let mut physical_complete = false;
    let mut physical_verified = false;
    let mut physical_tickets = HashMap::new();
    while let Some(frame) = receiver.recv().await {
        assert!(!frame.lease.is_revoked());
        let member_id = catalog.accept(&frame).unwrap();
        match &frame.event {
            Event::PhysicalFile { path, .. } => {
                destinations.insert(member_id, plan.destination_dir.join(path));
            }
            Event::File { .. } => {
                let node = sessions
                    .register(Some(parent_session.owner), InputVersion(2))
                    .unwrap();
                child_session = Some(sessions.start(node).unwrap());
                destinations.insert(
                    member_id,
                    plan.destination_dir
                        .join(format!("member-{member_id}.partial")),
                );
            }
            _ => {}
        }
        let namespace = DecodedNamespace {
            catalog: &catalog,
            plan: &plan,
            resources: &resources,
            destinations: &destinations,
        };
        if let Event::PhysicalFile { path, length } = &frame.event {
            assert_eq!(path, Path::new("ordinary.bin"));
            assert_eq!(*length, Some(ordinary_leaf.len() as u64));
            assert!(physical.insert(frame.member));
            let destination = Destination {
                namespace: parent_session.owner,
                owner: crate::pipeline::direct_store::router::SpanOwner {
                    generation: parent_session.owner.generation,
                    ..Default::default()
                },
                destination: DirectDestination::Member { member_id },
            };
            registry
                .register(
                    &sessions,
                    destination,
                    destinations[&member_id].strip_prefix(temp.path()).unwrap(),
                    *length,
                )
                .unwrap();
            physical_tickets.insert(
                member_id,
                (
                    destination,
                    registry
                        .begin_write(&sessions, destination, 0, length.unwrap())
                        .unwrap(),
                ),
            );
        }
        if physical.contains(&frame.member) {
            match frame.event {
                Event::PhysicalFile { .. } => {}
                Event::DataComplete { length } => {
                    assert_eq!(length, ordinary_leaf.len() as u64);
                    let (destination, pending) = physical_tickets.remove(&member_id).unwrap();
                    registry.placed(&sessions, pending).unwrap();
                    registry.seal(&sessions, destination, length).unwrap();
                    physical_complete = true;
                }
                Event::Integrity(Integrity::Verified) => physical_verified = true,
                _ => panic!("physical leaf payload must not cross the stream bridge"),
            }
            continue;
        }
        let spans = match frame.event {
            Event::File { path, length } => {
                assert_eq!(path, Path::new("inner.7z"));
                assert_eq!(length, Some(inner.len() as u64));
                Vec::new()
            }
            Event::Bytes { offset, bytes } => {
                vec![RoutedSpan {
                    owner: Default::default(),
                    destination: DirectDestination::Member { member_id },
                    destination_offset: offset,
                    volume_index: 0,
                    source_offset: offset,
                    bytes: vec![bytes],
                }]
            }
            Event::DataComplete { length } => {
                data_complete = true;
                assert_eq!(length, inner.len() as u64);
                Vec::new()
            }
            Event::Integrity(Integrity::Verified) => {
                integrity = true;
                Vec::new()
            }
            Event::Metadata(_) => Vec::new(),
            _ => panic!("unexpected decoder output"),
        };
        // Router retention is already charged to its bounded holds/scratch;
        // emitted writes retain the frame admission until their placement.
        for write in graph.forward(&namespace, ROOT_NODE, spans).unwrap() {
            if write.owner == ROOT_NODE {
                assert!(demoted);
                let receipt = write.placement();
                let path = &destinations[&member_id];
                let pending = ticket(
                    &mut registry,
                    &sessions,
                    parent_session.owner,
                    temp.path(),
                    path,
                    receipt,
                );
                let mut file = fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .open(path)
                    .unwrap();
                file.seek(SeekFrom::Start(write.span.destination_offset))
                    .unwrap();
                for bytes in &write.span.bytes {
                    file.write_all(bytes).unwrap();
                }
                registry.placed(&sessions, pending).unwrap();
                // Decoded member writes carry only member coordinates.
                let receipt = crate::pipeline::direct_store::nested::placement::MemberPlacement {
                    owner: receipt.owner,
                    member: member_id,
                    offset: receipt.destination_offset,
                    len: receipt.len,
                };
                assert!(
                    graph
                        .record_member_placement(&namespace, receipt)
                        .unwrap()
                        .is_empty()
                );
            } else {
                let receipt = write.placement();
                let path = destination_path(&graph.node(write.owner).router, receipt.destination);
                let pending = ticket(
                    &mut registry,
                    &sessions,
                    parent_session.owner,
                    temp.path(),
                    &path,
                    receipt,
                );
                place(&graph.node(write.owner).router, vec![write.span]);
                registry.placed(&sessions, pending).unwrap();
                assert!(
                    graph
                        .record_placement(&namespace, receipt)
                        .unwrap()
                        .is_empty(),
                    "decoded output must never advance compressed source coverage"
                );
            }
        }
        retained_beyond_bridge |= graph.node(1).router.staged_bytes() > 8192;
        if force_demotion
            && !demoted
            && graph.node(1).inputs[&0]
                .delivered
                .ranges()
                .last()
                .is_some_and(|&(_, end)| end >= inner.len() as u64 / 2)
        {
            graph.node_mut(1).mode =
                crate::pipeline::direct_store::nested::NodeMode::MaterializationPending(
                    crate::pipeline::direct_store::router::DemotionReason::HoldsScratchCeiling,
                );
            let owner = child_session.as_ref().unwrap().owner;
            assert_eq!(sessions.revoke(owner).unwrap(), [owner]);
            assert!(sessions.accepts(parent_session.owner));
            demoted = true;
        }
        drop(frame.lease);
    }
    assert_eq!(
        worker.await.unwrap().unwrap().extracted,
        ["ordinary.bin", "inner.7z"]
    );
    catalog.close().unwrap();
    sessions.close(parent_session.owner).unwrap();
    let namespace = DecodedNamespace {
        catalog: &catalog,
        plan: &plan,
        resources: &resources,
        destinations: &destinations,
    };
    for write in graph.close_parent_roster(&namespace, ROOT_NODE).unwrap() {
        let receipt = write.placement();
        let path = destination_path(&graph.node(write.owner).router, receipt.destination);
        let pending = ticket(
            &mut registry,
            &sessions,
            parent_session.owner,
            temp.path(),
            &path,
            receipt,
        );
        place(&graph.node(write.owner).router, vec![write.span]);
        registry.placed(&sessions, pending).unwrap();
        assert!(
            graph
                .record_placement(&namespace, receipt)
                .unwrap()
                .is_empty()
        );
    }
    let child = &graph.node(1).router;
    assert_eq!(
        fs::read(plan.destination_dir.join("ordinary.bin")).unwrap(),
        ordinary_leaf
    );
    assert!(!generation.is_revoked());
    assert!(retained_beyond_bridge);
    assert!(data_complete && integrity);
    assert!(physical_complete && physical_verified);
    if force_demotion {
        assert!(demoted);
        let work = graph.prepare_materialization(&namespace, 1).unwrap();
        let pending: Vec<_> = work
            .writes()
            .map(|(path, receipt)| {
                let destination = Destination {
                    namespace: parent_session.owner,
                    owner: receipt.owner,
                    destination: DirectDestination::Member {
                        member_id: receipt.member,
                    },
                };
                registry
                    .register(
                        &sessions,
                        destination,
                        path.strip_prefix(temp.path()).unwrap(),
                        None,
                    )
                    .unwrap();
                registry
                    .begin_write(&sessions, destination, receipt.offset, receipt.len)
                    .unwrap()
            })
            .collect();
        let copied = crate::pipeline::direct_store::nested::materialize::copy_inputs(
            work,
            Default::default(),
        )
        .unwrap();
        let receipts = graph.apply_materialization(&namespace, copied).unwrap();
        assert_eq!(receipts.len(), pending.len());
        for write in pending {
            registry.placed(&sessions, write).unwrap();
        }
        assert!(!receipts.is_empty());
        assert!(receipts.iter().all(|(_, source)| source.is_empty()));
        assert_eq!(fs::read(&destinations[&1]).unwrap(), inner);
        let claims = registry.checkpoint_claims().unwrap();
        let (_, _, _, physical_input) = claims
            .iter()
            .find(|(_, relative, _, _)| {
                relative == destinations[&1].strip_prefix(temp.path()).unwrap()
            })
            .unwrap();
        assert_eq!(physical_input, &[(0, inner.len() as u64)]);
        let (physical, nested) = graph.member_backings(&namespace, ROOT_NODE);
        assert_eq!(physical.len(), 2);
        assert!(nested.is_empty());
        let old_session = child_session.as_ref().unwrap();
        sessions.joined(old_session.owner).unwrap();
        let replacement = sessions
            .prepare_replacement(old_session.owner.node, InputVersion(2))
            .unwrap();
        // This harness supplies the checkpoint acknowledgement; durability is
        // exercised independently by the job checkpoint repository tests.
        sessions.committed_replacement(replacement, 1).unwrap();
        let next = sessions.start(old_session.owner.node).unwrap();
        assert_eq!(next.owner.generation, old_session.owner.generation + 1);
        assert_eq!(sessions.original(next.owner.node), Some(InputVersion(2)));
        let fallback_dir = temp.path().join("fallback");
        fs::create_dir(&fallback_dir).unwrap();
        let (root, budget) = test_extraction_security_with_memory(&fallback_dir, 256 * 1024 * 1024);
        let context = conventional_7z_context(&fallback_dir, root, budget, &inner, 4);
        let result =
            extract_7z_stream(&context, || Ok(fs::File::open(&destinations[&1]).unwrap())).unwrap();
        assert_eq!(result.extracted, ["leaf.bin"]);
        assert_eq!(fs::read(fallback_dir.join("leaf.bin")).unwrap(), payload);
        sessions.close(next.owner).unwrap();
        assert!(sessions.ancestry_verified(next.owner));
        assert_eq!(
            sessions.revoke(parent_session.owner).unwrap(),
            [parent_session.owner, next.owner]
        );
        assert!(!sessions.ancestry_verified(next.owner));
    } else {
        assert!(child.all_members_verified());
        let (_, _, partial) = child.member_partials()[0];
        assert_eq!(
            fs::read(child.plan().destination_path(partial)).unwrap(),
            payload
        );
        sessions
            .close(child_session.as_ref().unwrap().owner)
            .unwrap();
        assert!(sessions.ancestry_verified(child_session.as_ref().unwrap().owner));
    }
    for owner in [
        StreamOwner {
            root: 2,
            node: 0,
            generation: 1,
        },
        StreamOwner {
            root: 1,
            node: 1,
            generation: 1,
        },
        StreamOwner {
            root: 1,
            node: 0,
            generation: 2,
        },
    ] {
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let (_output, replacement) = ChannelOutput::new(
            owner,
            lifecycle_budget.try_reserve_memory(8192).unwrap(),
            sender,
            tokio::runtime::Handle::current(),
            Arc::clone(&lifecycle_budget),
        )
        .unwrap();
        let replacement_catalog =
            crate::pipeline::extraction::catalog::decoded::DecodedCatalog::new(
                replacement,
                lifecycle_budget.try_reserve_memory(4096).unwrap(),
            );
        let replacement_namespace = DecodedNamespace {
            catalog: &replacement_catalog,
            plan: &plan,
            resources: &resources,
            destinations: &destinations,
        };
        assert!(
            graph
                .forward(&replacement_namespace, ROOT_NODE, Vec::new())
                .is_err()
        );
        assert!(
            graph
                .close_parent_roster(&replacement_namespace, ROOT_NODE)
                .is_err()
        );
    }
    generation.revoke();
    assert!(graph.forward(&namespace, ROOT_NODE, Vec::new()).is_err());
    assert!(graph.close_parent_roster(&namespace, ROOT_NODE).is_err());
}

#[test]
fn stored_parent_source_feeds_adaptive_decoder_after_child_direct_refusal() {
    use crate::pipeline::direct_store::nested::{NestedGraph, NodeMode, ROOT_NODE};
    use crate::pipeline::direct_store::provider::VirtualVolumeReader;

    let temp = TempDir::new().unwrap();
    let working = temp.path().join("working");
    let staging = temp.path().join("staging");
    let decoded = temp.path().join("decoded");
    for path in [&working, &staging, &decoded] {
        fs::create_dir_all(path).unwrap();
    }
    let payload = incompressible(128 * 1024, 31);
    let inner = sevenz_archive_with_dictionary(1024 * 1024, &[("leaf.bin", &payload)]);
    let outer = sevenz_archive_with_method(
        sevenz_turbo::EncoderConfiguration::new(sevenz_turbo::EncoderMethod::COPY),
        &[("inner.7z", &inner)],
    );
    let mut parent = DirectSetRouter::new(DirectSetPlan {
        set_name: "outer".into(),
        format: SetFormat::SevenZip,
        volumes: BTreeMap::from([(0, 0)]),
        files: HashMap::from([(0, 0)]),
        identity: None,
        unposted: Default::default(),
        working_dir: working,
        destination_dir: staging,
    });
    parent
        .note_declared_volume_size(0, outer.len() as u64)
        .unwrap();
    let mut graph = NestedGraph::default();
    for (index, chunk) in outer.chunks(769).enumerate() {
        let spans = parent
            .route(
                0,
                (index * 769) as u64,
                &[bytes::Bytes::copy_from_slice(chunk)],
            )
            .unwrap();
        for write in graph.forward(&parent, ROOT_NODE, spans).unwrap() {
            let receipt = write.placement();
            place(graph.router(&parent, write.owner), vec![write.span]);
            graph.record_placement(&parent, receipt).unwrap();
        }
    }
    let tail = parent.note_volume_complete(0, outer.len() as u64).unwrap();
    let mut writes = graph.forward(&parent, ROOT_NODE, tail).unwrap();
    writes.extend(graph.close_parent_roster(&parent, ROOT_NODE).unwrap());
    for write in writes {
        let receipt = write.placement();
        place(graph.router(&parent, write.owner), vec![write.span]);
        graph.record_placement(&parent, receipt).unwrap();
    }
    assert_eq!(graph.nodes.len(), 1);
    assert_eq!(graph.node(1).mode, NodeMode::Engine);
    let image = graph.input_views(&parent, 1).remove(&0).unwrap();
    let mut original = VirtualVolumeReader::<true>::new(image.clone(), Arc::default());
    let mut observed = Vec::new();
    original.read_to_end(&mut observed).unwrap();
    assert_eq!(
        observed, inner,
        "fallback consumes the original compressed input"
    );

    let (root, budget) = test_extraction_security_with_memory(&decoded, 256 * 1024 * 1024);
    let mut context = conventional_7z_context(&decoded, root, budget, &inner, 4);
    context.decode_memory = SevenZipDecodeMemory::ReservedPerPass {
        end_header_bytes: sevenz_end_header_bytes(&inner),
        widening: ChaseWidening::default(),
    };
    let result = extract_7z_stream(&context, || {
        Ok(VirtualVolumeReader::<true>::new(
            image.clone(),
            Arc::default(),
        ))
    })
    .unwrap();
    assert_eq!(result.extracted, ["leaf.bin"]);
    assert_eq!(fs::read(decoded.join("leaf.bin")).unwrap(), payload);
}
