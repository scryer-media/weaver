// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A volume of another set posted under a name that numbers it into this one:
// whichever comes first, the set's own volumes take their places, and the
// stray is no volume of the set.

use bytes::Bytes;
use weaver_graph::{Effect, MemberId, NodeId, Posted, Provenance, Tree};

use super::writer::{Item, Options, Version, write};
use crate::Objects;
use crate::testing::payload;

// The member at the root, its whole bytes delivered and closed. The owner
// hands each member bound meanwhile over to its object, as the server's
// namespace does on every `Bound`, so the object hears each close.
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
    hand_over(tree, &mut effects, 0);
    let done = effects.len();
    tree.close(NodeId::ROOT, member, &mut objects, &mut effects);
    hand_over(tree, &mut effects, done);
    member
}

// Every member bound at the root since `done` handed over, in turn, until
// no new one comes. A placement that undid itself would bind without end.
fn hand_over(tree: &mut Tree, effects: &mut Vec<Effect>, mut done: usize) {
    for _ in 0..64 {
        let Some((at, member)) =
            effects[done..]
                .iter()
                .enumerate()
                .find_map(|(at, effect)| match effect {
                    Effect::Bound {
                        node: NodeId::ROOT,
                        member,
                        ..
                    } => Some((at, *member)),
                    _ => None,
                })
        else {
            return;
        };
        done += at + 1;
        tree.handed_over(NodeId::ROOT, member, &mut Objects::default(), effects);
    }
    panic!("a placement that never settles");
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
        for (order, restarted) in ORDERS
            .into_iter()
            .flat_map(|order| [(order, false), (order, true)])
        {
            let case = format!("{version:?} old={old} order={order:?} restarted={restarted}");
            let mut tree = Tree::new();
            let mut members = Vec::new();
            for &at in &order {
                members.push(post(&mut tree, &posts[at].0, &posts[at].1));
                // Restarted once the stray has taken its place, before
                // anything shows it is no volume of the set.
                if restarted && at == 4 {
                    tree = Tree::rebuild(&tree.records(), &Objects::default()).expect(&case);
                }
            }
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

// A schedule step: bytes of a post, the rest of it and its close, or a restart.
#[derive(Clone, Copy)]
enum Step {
    // The post's chunk, `CHUNK` bytes at a time.
    Chunk(usize, usize),
    // Every chunk of the post from the one given on, then its close.
    Rest(usize, usize),
    Restart,
}

const CHUNK: usize = 2_000;

// A set of four volumes, its last a few bytes, beside a three volume set's
// second posted as `archive.part03.rar` and announced first, as an NZB that
// lists it first does: the stray's store is the first one asked for every
// volume after it. Whichever of the two stores a volume is first bound to,
// the store whose held neighbour runs into it ends holding it.
#[test]
fn a_volume_bound_beside_a_stray_goes_to_the_store_whose_volume_runs_into_it() {
    let mut options = Options::new(Version::Rar5);
    options.split = Some(6_000);
    let ours = write(&[Item::File("amber.bin", payload(7, 18_100))], &options);
    let theirs = write(&[Item::File("cobalt.bin", payload(8, 15_000))], &options);
    assert_eq!(ours.len(), 4);
    assert!(theirs.len() >= 3);
    assert!(ours[3].len() < CHUNK, "the last volume is one chunk");
    let mut posts: Vec<(String, Vec<u8>)> = ours
        .into_iter()
        .enumerate()
        .map(|(index, bytes)| (name(false, index), bytes))
        .collect();
    posts.push(("archive.part03.rar".to_owned(), theirs[1].clone()));
    use Step::{Chunk, Rest, Restart};
    // The last volume bound while the third is still short of its end, so
    // nothing yet runs into it; and bound once the third ran into it.
    let early = vec![
        Chunk(4, 0),
        Chunk(2, 0),
        Chunk(0, 0),
        Chunk(1, 0),
        Rest(3, 0),
        Restart,
        Rest(4, 1),
        Rest(0, 1),
        Rest(1, 1),
        Rest(2, 1),
    ];
    let late = vec![
        Chunk(4, 0),
        Rest(2, 0),
        Chunk(0, 0),
        Chunk(1, 0),
        Rest(3, 0),
        Restart,
        Rest(4, 1),
        Rest(0, 1),
        Rest(1, 1),
    ];
    for (label, schedule) in [("early", early), ("late", late)] {
        for restarted in [false, true] {
            let case = format!("{label} restarted={restarted}");
            let mut tree = Tree::new();
            let mut effects = Vec::new();
            let mut members = vec![MemberId(0); posts.len()];
            for at in [4usize, 0, 1, 3, 2] {
                members[at] =
                    tree.announce_root(&posts[at].0, Some(posts[at].1.len() as u64), &mut effects);
            }
            let deliver = |tree: &mut Tree, at: usize, chunk: usize| {
                let bytes = &posts[at].1;
                let from = (chunk * CHUNK).min(bytes.len());
                let to = ((chunk + 1) * CHUNK).min(bytes.len());
                let mut effects = Vec::new();
                tree.deliver(
                    NodeId::ROOT,
                    members[at],
                    Posted {
                        offset: from as u64,
                        bytes: Bytes::copy_from_slice(&bytes[from..to]).into(),
                        provenance: Provenance::Fresh,
                    },
                    &mut Objects::default(),
                    &mut effects,
                );
                hand_over(tree, &mut effects, 0);
            };
            let close = |tree: &mut Tree, at: usize| {
                let mut effects = Vec::new();
                tree.close(
                    NodeId::ROOT,
                    members[at],
                    &mut Objects::default(),
                    &mut effects,
                );
                hand_over(tree, &mut effects, 0);
            };
            for step in &schedule {
                match *step {
                    Chunk(at, chunk) => deliver(&mut tree, at, chunk),
                    Rest(at, chunk) => {
                        for chunk in chunk..posts[at].1.len().div_ceil(CHUNK) {
                            deliver(&mut tree, at, chunk);
                        }
                        close(&mut tree, at);
                    }
                    Restart if restarted => {
                        tree = Tree::rebuild(&tree.records(), &Objects::default()).expect(&case);
                    }
                    Restart => {}
                }
            }
            let at = |post: usize| tree.child_of(NodeId::ROOT, members[post]);
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
