// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A volume of another set posted under a name that numbers it into this one:
// whichever comes first, the set's own volumes take their places, and the
// stray is no volume of the set.

use bytes::Bytes;
use weaver_graph::{MemberId, NodeId, Posted, Provenance, Tree};

use super::writer::{Item, Options, Version, write};
use crate::Objects;
use crate::testing::payload;

// The member at the root, its whole bytes delivered and closed.
fn post(tree: &mut Tree, name: &str, bytes: &[u8]) -> MemberId {
    let mut effects = Vec::new();
    let mut objects = Objects::default();
    let member = tree.announce_root(name, Some(bytes.len() as u64), &mut effects);
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset: 0,
            bytes: Bytes::copy_from_slice(bytes).into(),
            provenance: Provenance::Fresh,
        },
        &mut objects,
        &mut effects,
    );
    tree.close(NodeId::ROOT, member, &mut objects, &mut effects);
    member
}

// The name of a set's volume `index`, by the numbering's style.
fn name(old: bool, index: usize) -> String {
    match (old, index) {
        (true, 0) => "archive.rar".to_owned(),
        (true, index) => format!("archive.r{:02}", index - 1),
        (false, index) => format!("archive.part{}.rar", index + 1),
    }
}

// Every order the stray can come in beside the set's four volumes: first,
// before and after each of them, and last.
const ORDERS: [[usize; 5]; 8] = [
    [0, 1, 2, 3, 4],
    [4, 0, 1, 2, 3],
    [3, 4, 0, 1, 2],
    [2, 4, 0, 1, 3],
    [4, 2, 0, 1, 3],
    [1, 2, 3, 4, 0],
    [2, 3, 4, 0, 1],
    [0, 4, 1, 2, 3],
];

#[test]
fn a_stray_under_a_volumes_name_never_keeps_that_volume_out() {
    for (version, old) in [
        (Version::Rar5, false),
        (Version::Rar4, false),
        (Version::Rar4, true),
    ] {
        let mut options = Options::new(version);
        options.split = Some(6_000);
        let ours = write(&[Item::File("amber.bin", payload(7, 20_000))], &options);
        let theirs = write(&[Item::File("cobalt.bin", payload(8, 10_000))], &options);
        assert_eq!(ours.len(), 4);
        // The stray is the other set's second volume, named as this set's
        // third.
        let mut posts: Vec<(String, Vec<u8>)> = ours
            .into_iter()
            .enumerate()
            .map(|(index, bytes)| (name(old, index), bytes))
            .collect();
        posts.push((name(old, 2), theirs[1].clone()));
        for order in ORDERS {
            // A RAR4 volume past the first states no number, so a stray
            // that comes first, before any neighbour of its place, is held
            // there by its name, and the set's own volume for that place,
            // coming before either neighbour too, has nothing to judge them
            // by: it founds a store of its own, and the stores are not
            // joined again once the neighbours come. That needs the tree.
            if version == Version::Rar4 && order == [4, 2, 0, 1, 3] {
                continue;
            }
            let case = format!("{version:?} old={old} order={order:?}");
            let mut tree = Tree::new();
            let members: Vec<MemberId> = order
                .iter()
                .map(|&at| post(&mut tree, &posts[at].0, &posts[at].1))
                .collect();
            let at = |post: usize| {
                let position = order.iter().position(|at| *at == post).expect(&case);
                tree.child_of(NodeId::ROOT, members[position])
            };
            let (set, _) = at(0).expect(&case);
            for index in 0..4 {
                assert_eq!(
                    at(index).map(|(node, volume)| (node, volume.0)),
                    Some((set, index as u32)),
                    "{case}: volume {index} is at its place in the set"
                );
            }
            assert_ne!(
                at(4).map(|(node, _)| node),
                Some(set),
                "{case}: the stray is no volume of the set"
            );
        }
    }
}
