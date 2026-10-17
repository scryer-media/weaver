use super::*;
mod checkpoint;
mod seeks;
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use crate::pipeline::direct_store::ByteRanges;
use crate::pipeline::direct_store::nested::{
    NestedGraph, NodeMode, NodeSpan, ROOT_NODE, SourceSpan,
};
use crate::pipeline::direct_store::plan::{DirectSetPlan, SetFormat};
use crate::pipeline::direct_store::provider::{VirtualVolume, VirtualVolumeReader};
use crate::pipeline::direct_store::router::{DirectDestination, DirectSetRouter};
use crate::pipeline::extraction::runtime::limits::MAX_DEPTH;

fn stored(format: SetFormat, member: &'static str, bytes: &[u8]) -> Vec<u8> {
    match format {
        SetFormat::Rar => single_member_store_set(member, bytes, 1).remove(0).1,
        SetFormat::SevenZip => super::sevenz_store::build_7z_shaped(
            &[super::sevenz_store::Entry::file(member, bytes.to_vec())],
            sevenz_turbo::EncoderMethod::COPY,
            None,
            false,
        ),
        SetFormat::Stream(_) => unreachable!("the store layers are archives"),
    }
}

#[test]
fn decoded_namespace_discovery_uses_remaining_archive_depth() {
    // Three archives nest below the namespace: each level of depth the
    // namespace already sits at takes one child away.
    for (namespace_depth, children) in [(MAX_DEPTH - 2, 2), (MAX_DEPTH - 1, 1), (MAX_DEPTH, 0)] {
        let dir = tempfile::tempdir().unwrap();
        let inner = stored(SetFormat::SevenZip, "leaf.bin", &[37; 1024]);
        let middle = stored(SetFormat::SevenZip, "inner.7z", &inner);
        let outer = stored(SetFormat::SevenZip, "middle.7z", &middle);
        let mut root = DirectSetRouter::new(DirectSetPlan {
            set_name: "outer".into(),
            format: SetFormat::SevenZip,
            volumes: BTreeMap::from([(0, 0)]),
            files: HashMap::from([(0, 0)]),
            identity: None,
            unposted: Default::default(),
            working_dir: dir.path().join("working"),
            destination_dir: dir.path().join("staging"),
        });
        root.note_declared_volume_size(0, outer.len() as u64)
            .unwrap();
        let mut graph = NestedGraph::default();
        graph.set_namespace_depth(namespace_depth).unwrap();
        let spans = root
            .route(0, 0, &[bytes::Bytes::from(outer.clone())])
            .unwrap();
        graph.forward(&root, ROOT_NODE, spans).unwrap();
        let tail = root.note_volume_complete(0, outer.len() as u64).unwrap();
        graph.forward(&root, ROOT_NODE, tail).unwrap();
        graph.close_parent_roster(&root, ROOT_NODE).unwrap();
        assert_eq!(graph.nodes.len(), children);
        if children != 0 {
            assert!(
                graph.set_namespace_depth(MAX_DEPTH).is_err(),
                "restored over-depth topology must reject"
            );
        }
    }
}

#[test]
fn containing_roster_closure_reaches_all_three_store_layers() {
    let formats = [SetFormat::Rar, SetFormat::SevenZip];
    let archive_name = |format| match format {
        SetFormat::Rar => "inner.rar",
        SetFormat::SevenZip => "inner.7z",
        SetFormat::Stream(_) => unreachable!("the store layers are archives"),
    };
    for outer_format in formats {
        for middle_format in formats {
            for inner_format in formats {
                let dir = tempfile::tempdir().unwrap();
                let payload = vec![37; 32_769];
                let inner = stored(inner_format, "leaf.bin", &payload);
                let middle = stored(middle_format, archive_name(inner_format), &inner);
                let outer = stored(outer_format, archive_name(middle_format), &middle);
                let mut root = DirectSetRouter::new(DirectSetPlan {
                    set_name: "outer".into(),
                    format: outer_format,
                    volumes: BTreeMap::from([(0, 0)]),
                    files: HashMap::from([(0, 0)]),
                    identity: None,
                    unposted: Default::default(),
                    working_dir: dir.path().join("working"),
                    destination_dir: dir.path().join("staging"),
                });
                root.note_declared_volume_size(0, outer.len() as u64)
                    .unwrap();
                let mut graph = NestedGraph::default();
                let mut pending = Vec::new();
                for (index, chunk) in outer.chunks(769).enumerate() {
                    let spans = root
                        .route(
                            0,
                            (index * 769) as u64,
                            &[bytes::Bytes::copy_from_slice(chunk)],
                        )
                        .unwrap();
                    pending.extend(graph.forward(&root, ROOT_NODE, spans).unwrap());
                }
                let tail = root.note_volume_complete(0, outer.len() as u64).unwrap();
                pending.extend(graph.forward(&root, ROOT_NODE, tail).unwrap());
                pending.extend(graph.close_parent_roster(&root, ROOT_NODE).unwrap());
                assert_eq!(graph.nodes.len(), 2);
                for node in &graph.nodes {
                    assert_eq!(node.mode, NodeMode::Direct);
                    assert!(node.roster_closed);
                    assert!(node.router.all_members_verified());
                }
                for write in pending {
                    place(&root, &mut graph, &write);
                }
                let leaf = &graph.node(2).router;
                let (_, _, partial) = leaf.member_partials()[0];
                assert_eq!(
                    std::fs::read(leaf.plan().destination_path(partial)).unwrap(),
                    payload
                );
                // An empty copy result still belongs to its captured lineage.
                // All bytes are already raw, so no per-range receipt can supply
                // the ancestor fence on behalf of the completion itself.
                let member = graph.node(2).inputs[&0].parent_member;
                let parent = &graph.node(1).router;
                let (_, _, partial) = parent
                    .member_partials()
                    .into_iter()
                    .find(|(id, _, _)| *id == member)
                    .unwrap();
                let raw_path = parent.plan().destination_path(partial);
                std::fs::create_dir_all(raw_path.parent().unwrap()).unwrap();
                std::fs::write(raw_path, &inner).unwrap();
                graph.node_mut(2).mode =
                    NodeMode::MaterializationPending(DemotionReason::HoldsScratchCeiling);
                graph
                    .record_member_placement(
                        &root,
                        crate::pipeline::direct_store::nested::placement::MemberPlacement {
                            owner: crate::pipeline::direct_store::router::SpanOwner {
                                node: 1,
                                generation: 1,
                                raw_child: Some(crate::pipeline::direct_store::router::RawChild {
                                    node: 2,
                                    volume: 0,
                                    generation: 1,
                                }),
                            },
                            member,
                            offset: 0,
                            len: inner.len() as u64,
                        },
                    )
                    .unwrap();
                let work = graph.prepare_materialization(&root, 2).unwrap();
                let copied = crate::pipeline::direct_store::nested::materialize::copy_inputs(
                    work,
                    Default::default(),
                )
                .unwrap();
                graph.node_mut(1).mode = NodeMode::Materialized;
                assert!(graph.apply_materialization(&root, copied).is_err());
                assert_eq!(graph.node(2).generation, 1);
                assert!(matches!(
                    graph.node(2).mode,
                    NodeMode::MaterializationPending(_)
                ));
            }
        }
    }
}

fn place(root: &DirectSetRouter, graph: &mut NestedGraph, write: &NodeSpan) -> Vec<SourceSpan> {
    let router = graph.router(root, write.owner);
    let path = match write.span.destination {
        DirectDestination::Envelope { volume_index } => router.plan().envelope_path(volume_index),
        DirectDestination::Member { member_id } => {
            let (_, _, partial) = router
                .member_partials()
                .into_iter()
                .find(|(member, _, _)| *member == member_id)
                .unwrap();
            router.plan().destination_path(partial)
        }
    };
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(write.span.destination_offset))
        .unwrap();
    for piece in &write.span.bytes {
        file.write_all(piece).unwrap();
    }
    graph.record_placement(root, write.placement()).unwrap()
}

#[test]
fn nested_graph_routes_all_four_store_pairs_and_reconstructs_originals() {
    for outer_format in [SetFormat::Rar, SetFormat::SevenZip] {
        for inner_format in [SetFormat::Rar, SetFormat::SevenZip] {
            let dir = tempfile::tempdir().unwrap();
            let payload: Vec<u8> = (0..16_385).map(|i| (i % 251) as u8).collect();
            let inner = stored(inner_format, "leaf.bin", &payload);
            let inner_name = match inner_format {
                SetFormat::Rar => "inner.rar",
                SetFormat::SevenZip => "inner.7z",
                SetFormat::Stream(_) => unreachable!("the store layers are archives"),
            };
            let outer = stored(outer_format, inner_name, &inner);
            let plan = DirectSetPlan {
                set_name: "outer".into(),
                format: outer_format,
                volumes: BTreeMap::from([(0, 0)]),
                files: HashMap::from([(0, 0)]),
                identity: None,
                unposted: Default::default(),
                working_dir: dir.path().join("working"),
                destination_dir: dir.path().join("staging"),
            };
            std::fs::create_dir_all(&plan.working_dir).unwrap();
            std::fs::create_dir_all(&plan.destination_dir).unwrap();
            let mut root = DirectSetRouter::new(plan);
            root.note_declared_volume_size(0, outer.len() as u64)
                .unwrap();
            let mut graph = NestedGraph::default();
            let mut pending = Vec::new();
            // Routing and confirming are distinct. Both can emit writes.
            for (index, chunk) in outer.chunks(769).enumerate() {
                let spans = root
                    .route(
                        0,
                        (index * 769) as u64,
                        &[bytes::Bytes::copy_from_slice(chunk)],
                    )
                    .unwrap();
                pending.extend(graph.forward(&root, ROOT_NODE, spans).unwrap());
            }
            let tail = root.note_volume_complete(0, outer.len() as u64).unwrap();
            pending.extend(graph.forward(&root, ROOT_NODE, tail).unwrap());
            pending.extend(graph.close_parent_roster(&root, ROOT_NODE).unwrap());
            assert_eq!(graph.nodes.len(), 1, "{outer_format:?}/{inner_format:?}");
            assert_eq!(graph.node(1).mode, NodeMode::Direct);
            assert!(graph.node(1).roster_closed);
            assert!(graph.node(1).router.all_members_verified());
            assert!(
                graph.node(1).inputs[&0].placed.ranges().is_empty(),
                "forwarding is not placement"
            );
            let mut covered = ByteRanges::new();
            let mut envelope_covered = ByteRanges::new();
            for write in pending {
                for source in place(&root, &mut graph, &write) {
                    covered.insert(source.offset, source.len);
                }
                if write.owner == ROOT_NODE
                    && matches!(write.span.destination, DirectDestination::Envelope { .. })
                {
                    envelope_covered.insert(write.span.destination_offset, write.span.len());
                }
            }
            let (partials, nested) = graph.member_backings(&root, ROOT_NODE);
            assert!(
                partials.is_empty(),
                "the inner archive has no physical partial"
            );
            for (_, _, partial) in root.member_partials() {
                assert!(!root.plan().destination_path(partial).exists());
            }
            let image = VirtualVolume {
                backing_leases: Default::default(),
                volume_index: 0,
                envelope: root.plan().envelope_path(0),
                extents: root.volume_member_extents(0),
                partials,
                nested,
                covered,
                envelope_covered,
                held: Arc::new(root.held_runs(0)),
                len: outer.len() as u64,
                ciphers: root.member_ciphers(),
            };
            let mut reader = VirtualVolumeReader::<true>::new(image, Arc::default());
            let mut reconstructed = Vec::new();
            reader.read_to_end(&mut reconstructed).unwrap();
            assert_eq!(reconstructed, outer);
            let (_, _, leaf) = graph.node(1).router.member_partials()[0];
            assert_eq!(
                std::fs::read(graph.node(1).router.plan().destination_path(leaf)).unwrap(),
                payload
            );
        }
    }
}

#[test]
fn halfway_child_demotion_materializes_old_outputs_and_new_raw_input() {
    use crate::pipeline::direct_store::nested::materialize::copy_inputs;
    let dir = tempfile::tempdir().unwrap();
    let payload: Vec<u8> = (0..16_385).map(|i| (i % 251) as u8).collect();
    let inner = stored(SetFormat::Rar, "leaf.bin", &payload);
    let outer = stored(SetFormat::Rar, "inner.rar", &inner);
    let mut root = DirectSetRouter::new(DirectSetPlan {
        set_name: "outer".into(),
        format: SetFormat::Rar,
        volumes: BTreeMap::from([(0, 0)]),
        files: HashMap::from([(0, 0)]),
        identity: None,
        unposted: Default::default(),
        working_dir: dir.path().join("working"),
        destination_dir: dir.path().join("staging"),
    });
    let mut graph = NestedGraph::default();
    let midpoint = outer.len() / 2;
    let first = root
        .route(0, 0, &[bytes::Bytes::copy_from_slice(&outer[..midpoint])])
        .unwrap();
    let writes = graph.forward(&root, ROOT_NODE, first).unwrap();
    assert_eq!(graph.nodes.len(), 1);
    let stale_receipt = writes
        .iter()
        .find(|write| write.owner == 1)
        .unwrap()
        .placement();
    for write in writes {
        place(&root, &mut graph, &write);
    }
    assert!(!graph.node(1).inputs[&0].placed.is_empty());
    graph.node_mut(1).mode = NodeMode::MaterializationPending(DemotionReason::HoldsScratchCeiling);
    let last = root
        .route(
            0,
            midpoint as u64,
            &[bytes::Bytes::copy_from_slice(&outer[midpoint..])],
        )
        .unwrap();
    let writes = graph.forward(&root, ROOT_NODE, last).unwrap();
    assert!(writes.iter().any(|write| {
        write
            .raw_child
            .is_some_and(|raw| raw.node == 1 && raw.volume == 0)
    }));
    let raw_receipt = writes
        .iter()
        .find(|write| write.raw_child.is_some())
        .unwrap()
        .placement();
    let mut stale_raw = raw_receipt;
    stale_raw.owner.raw_child.as_mut().unwrap().generation += 1;
    assert!(graph.record_placement(&root, stale_raw).is_err());
    let mut wrong_sink = raw_receipt;
    wrong_sink.destination = DirectDestination::Envelope { volume_index: 0 };
    assert!(graph.record_placement(&root, wrong_sink).is_err());
    let mut out_of_bounds = raw_receipt;
    out_of_bounds.destination_offset = inner.len() as u64;
    assert!(graph.record_placement(&root, out_of_bounds).is_err());
    for invalid_node in [0, 2, u32::MAX] {
        let mut invalid = raw_receipt;
        invalid.owner.raw_child.as_mut().unwrap().node = invalid_node;
        assert!(graph.record_placement(&root, invalid).is_err());
    }
    for invalid_node in [2, u32::MAX] {
        let mut invalid = stale_receipt;
        invalid.owner.node = invalid_node;
        assert!(graph.record_placement(&root, invalid).is_err());
    }
    let mut invalid_generation = raw_receipt;
    invalid_generation.owner.generation = 1;
    assert!(graph.record_placement(&root, invalid_generation).is_err());
    let mut unknown_source = raw_receipt;
    unknown_source.volume_index = u32::MAX;
    assert!(graph.record_placement(&root, unknown_source).is_err());
    let mut overflowing_source = raw_receipt;
    overflowing_source.source_offset = u64::MAX;
    assert!(graph.record_placement(&root, overflowing_source).is_err());
    let replacement = DirectSetRouter::new(root.plan().clone());
    assert!(graph.record_placement(&replacement, raw_receipt).is_err());
    assert!(graph.forward(&replacement, ROOT_NODE, Vec::new()).is_err());
    assert!(graph.close_parent_roster(&replacement, ROOT_NODE).is_err());
    let mut mismatched_envelope = stale_receipt;
    mismatched_envelope.destination = DirectDestination::Envelope {
        volume_index: u32::MAX,
    };
    assert!(graph.record_placement(&root, mismatched_envelope).is_err());
    let mut overflowing_destination = stale_receipt;
    overflowing_destination.destination_offset = u64::MAX;
    assert!(
        graph
            .record_placement(&root, overflowing_destination)
            .is_err()
    );
    assert!(graph.node(1).inputs[&0].raw_placed.is_empty());
    for write in writes {
        place(&root, &mut graph, &write);
    }
    let tail = root.note_volume_complete(0, outer.len() as u64).unwrap();
    for write in graph.forward(&root, ROOT_NODE, tail).unwrap() {
        place(&root, &mut graph, &write);
    }

    // A refusal may precede the header that would resolve the final length.
    // Raw placement still extends the readable input beyond forwarded bytes.
    graph.node_mut(1).inputs.get_mut(&0).unwrap().length = None;
    let before = graph.input_views(&root, 1).remove(&0).unwrap();
    assert_eq!(before.len, inner.len() as u64);
    let mut read = VirtualVolumeReader::<true>::new(before.clone(), Arc::default());
    let mut bytes = Vec::new();
    read.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, inner, "hybrid view must remain exact before copying");
    drop(read);
    let old_paths = before.backing_paths();
    let work = graph.prepare_materialization(&root, 1).unwrap();
    let result = copy_inputs(work, Default::default()).unwrap();
    let receipts = graph.apply_materialization(&root, result).unwrap();
    assert!(!receipts.is_empty());
    assert_eq!(graph.node(1).mode, NodeMode::Materialized);
    assert!(
        !graph.backing_readers_drained(1),
        "a lazy frozen view still owns the old files"
    );
    assert!(graph.record_placement(&root, stale_receipt).is_err());
    assert!(graph.record_placement(&root, raw_receipt).is_err());
    let (physical, nested) = graph.member_backings(&root, ROOT_NODE);
    assert!(nested.is_empty());
    assert_eq!(physical.len(), 1);
    assert_eq!(
        std::fs::read(physical.values().next().unwrap()).unwrap(),
        inner
    );
    assert!(
        old_paths.iter().all(|path| path.exists()),
        "old checkpoint backings cannot be removed by materialization"
    );
    let mut read = VirtualVolumeReader::<true>::new(before, Arc::default());
    let mut bytes = Vec::new();
    read.read_to_end(&mut bytes).unwrap();
    assert_eq!(
        bytes, inner,
        "a pinned old reader remains valid until retirement"
    );
    drop(read);
    assert!(graph.backing_readers_drained(1));
}

#[test]
fn metadata_only_updates_settle_delivered_inputs_after_roster_closure() {
    for repeat_roster in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let inner = stored(SetFormat::Rar, "leaf.bin", &[17; 4097]);
        let outer = stored(SetFormat::Rar, "inner.rar", &inner);
        let mut root = DirectSetRouter::new(DirectSetPlan {
            set_name: "outer".into(),
            format: SetFormat::Rar,
            volumes: BTreeMap::from([(0, 0)]),
            files: HashMap::from([(0, 0)]),
            identity: None,
            unposted: Default::default(),
            working_dir: dir.path().join("working"),
            destination_dir: dir.path().join("staging"),
        });
        root.route(0, 0, &[bytes::Bytes::from(outer)]).unwrap();
        let mut graph = NestedGraph::default();
        // Publish the catalog first; payload and final size observations can
        // reach the child independently of the containing roster's closure.
        assert!(
            graph
                .close_parent_roster(&root, ROOT_NODE)
                .unwrap()
                .is_empty()
        );
        let node = graph.node_mut(1);
        assert!(node.roster_closed);
        node.router
            .route(0, 0, &[bytes::Bytes::copy_from_slice(&inner)])
            .unwrap();
        let input = node.inputs.get_mut(&0).unwrap();
        input.length = None;
        input.delivered.insert(0, inner.len() as u64);
        assert!(!input.confirmed);

        // Neither event contains payload. Both must reconcile the newly
        // resolved length rather than waiting for an arrival that cannot come.
        if repeat_roster {
            graph.close_parent_roster(&root, ROOT_NODE).unwrap();
        } else {
            graph.forward(&root, ROOT_NODE, Vec::new()).unwrap();
        }
        assert_eq!(graph.node(1).inputs[&0].length, Some(inner.len() as u64));
        assert!(graph.node(1).inputs[&0].confirmed);
        assert_eq!(graph.node(1).mode, NodeMode::Direct);
        assert!(graph.node(1).router.all_members_verified());
    }
}
