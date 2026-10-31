// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_graph::{Emit, Kind, NodeId, Posted, Provenance, Tree, VolumeIndex, Why};

use super::super::direct_store::scenarios::{
    HeaderCheck, encrypted_store_set, header_encrypted_store_set,
};

const PASSWORD: &str = "a-test-passphrase";

fn bound_second(headers: bool, old_number: u32) -> (Tree, NodeId, Vec<weaver_graph::MemberId>) {
    let body = super::harness::payload(1031, 71);
    let volumes = if headers {
        header_encrypted_store_set(
            "payload.bin",
            &body,
            2,
            PASSWORD,
            HeaderCheck::For(PASSWORD),
        )
    } else {
        encrypted_store_set("payload.bin", &body, 2, PASSWORD, Some(PASSWORD), false)
    };
    let mut tree = Tree::new();
    let mut builder = weaver_objects::Objects::default();
    let mut effects = Vec::new();
    let mut members = Vec::new();
    for (index, (_, bytes)) in volumes.into_iter().enumerate() {
        let number = if index == 0 { 1 } else { old_number };
        let name = format!("set.part{number:02}.rar");
        let member = tree.announce_root(&name, Some(bytes.len() as u64), &mut effects);
        tree.deliver(
            NodeId::ROOT,
            member,
            Posted {
                offset: 0,
                bytes: bytes.into(),
                provenance: Provenance::Fresh,
            },
            &mut builder,
            &mut effects,
        );
        members.push(member);
    }
    let (store, _) = tree.child_of(NodeId::ROOT, members[0]).unwrap();
    assert_eq!(tree.kind(store), Some(Kind::RarStore));
    // Exercise the existing Store→Engine transition with the live binding.
    tree.emitted(
        store,
        vec![Emit::NeedsEngine(Why::EncryptedHeader)],
        &mut builder,
        &mut effects,
    );
    let (engine, _) = tree.child_of(NodeId::ROOT, members[0]).unwrap();
    assert_eq!(tree.kind(engine), Some(Kind::RarEngine));
    // The writer buries the retired store once its retained bytes have
    // been handed to the engine. Complete that ordinary transition here.
    for member in &members {
        tree.bury(NodeId::ROOT, *member, &mut effects);
    }
    let expected_before = if headers { old_number - 1 } else { 1 };
    assert_eq!(
        tree.child_of(NodeId::ROOT, members[1]),
        Some((engine, VolumeIndex(expected_before)))
    );
    (tree, engine, members)
}

fn renamed_second(headers: bool, old_number: u32, new_number: u32) -> (Tree, NodeId) {
    let (mut tree, engine, members) = bound_second(headers, old_number);
    tree.rename(
        NodeId::ROOT,
        members[1],
        &format!("set.part{new_number:02}.rar"),
        &mut weaver_objects::Objects::default(),
        &mut Vec::new(),
    );
    let expected_after = if headers { new_number - 1 } else { 1 };
    assert_eq!(
        tree.child_of(NodeId::ROOT, members[1]),
        Some((engine, VolumeIndex(expected_after)))
    );
    (tree, engine)
}

#[test]
fn corrected_name_reorders_opaque_rar_engine_volume_through_tree() {
    let (tree, engine) = renamed_second(true, 3, 2);
    let restored = Tree::rebuild(&tree.records(), &weaver_objects::Objects::default()).unwrap();
    assert_eq!(restored.volumes(engine), tree.volumes(engine));
    assert_eq!(
        tree.volumes(engine)
            .unwrap()
            .1
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        [VolumeIndex(0), VolumeIndex(1)]
    );
}

#[test]
fn unchanged_opaque_rar_engine_number_stays_bound() {
    renamed_second(true, 2, 2);
}

#[test]
fn header_proven_rar_engine_position_outranks_corrected_label() {
    renamed_second(false, 2, 9);
}

#[test]
fn corrected_basename_moves_opaque_rar_volume_to_its_named_engine() {
    let (mut tree, old, members) = bound_second(true, 2);
    let bytes = tree
        .namespace(NodeId::ROOT)
        .unwrap()
        .first_bytes(members[0])
        .to_vec();
    let mut builder = weaver_objects::Objects::default();
    let mut effects = Vec::new();
    let first = tree.announce_root("other.part01.rar", Some(bytes.len() as u64), &mut effects);
    tree.deliver(
        NodeId::ROOT,
        first,
        Posted {
            offset: 0,
            bytes: bytes.into(),
            provenance: Provenance::Fresh,
        },
        &mut builder,
        &mut effects,
    );
    let store = tree.child_of(NodeId::ROOT, first).unwrap().0;
    assert_ne!(store, old);
    tree.emitted(
        store,
        vec![Emit::NeedsEngine(Why::EncryptedHeader)],
        &mut builder,
        &mut effects,
    );
    let engine = tree.child_of(NodeId::ROOT, first).unwrap().0;
    tree.bury(NodeId::ROOT, first, &mut effects);
    tree.rename(
        NodeId::ROOT,
        members[1],
        "other.part02.rar",
        &mut builder,
        &mut effects,
    );
    assert_eq!(
        tree.child_of(NodeId::ROOT, members[1]),
        Some((engine, VolumeIndex(1)))
    );
}

#[test]
fn unparseable_opaque_label_and_proven_header_links_keep_engine_membership() {
    for (headers, name) in [(true, "opaque"), (false, "other.part09.rar")] {
        let (mut tree, engine, members) = bound_second(headers, 2);
        tree.rename(
            NodeId::ROOT,
            members[1],
            name,
            &mut weaver_objects::Objects::default(),
            &mut Vec::new(),
        );
        assert_eq!(
            tree.child_of(NodeId::ROOT, members[1]),
            Some((engine, VolumeIndex(1)))
        );
    }
}

#[test]
fn corrected_opaque_basename_can_found_its_own_streaming_set() {
    let (mut tree, old, members) = bound_second(true, 2);
    tree.rename(
        NodeId::ROOT,
        members[1],
        "other.part02.rar",
        &mut weaver_objects::Objects::default(),
        &mut Vec::new(),
    );
    let (new, at) = tree
        .child_of(NodeId::ROOT, members[1])
        .expect("the returned source must remain adoptable");
    assert_ne!(new, old);
    assert_eq!(tree.kind(new), Some(Kind::RarStore));
    assert_eq!(at, VolumeIndex(1));
}
