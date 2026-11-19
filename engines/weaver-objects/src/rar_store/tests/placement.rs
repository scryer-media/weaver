// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Where a store places a volume and where it holds one misplaced are judged
// on the same facts, so a volume it gives back is never claimed again where
// it would be given back once more. Each post is run in a tree whose owner
// hands every bound volume over as the server does, and the hand-overs are
// counted: a placement that undid itself would hand the same volume over
// without end.

use bytes::Bytes;
use weaver_graph::{Effect, Kind, MemberId, NodeId, Posted, Provenance, Tree, VolumeIndex, Why};

use super::super::{claim, counts};
use super::writer::{Item, Options, Version, write};
use super::{find, items, split};
use crate::Objects;
use crate::grid::crc32;
use crate::testing::payload;

// More hand-overs than any of these posts makes: a run that reaches it is
// one that never settles.
const ENOUGH: usize = 64;

struct Run {
    tree: Tree,
    effects: Vec<Effect>,
    // Where the owner has handed over to: effects before it are done.
    done: usize,
    handed: usize,
    // How many hand-overs the run may make before it counts as unsettled.
    enough: usize,
}

impl Run {
    fn new() -> Self {
        Self {
            tree: Tree::new(),
            effects: Vec::new(),
            done: 0,
            handed: 0,
            enough: ENOUGH,
        }
    }

    fn announce(&mut self, name: &str, length: usize) -> MemberId {
        self.tree
            .announce_root(name, Some(length as u64), &mut self.effects)
    }

    fn deliver(&mut self, member: MemberId, offset: usize, bytes: &[u8]) {
        self.tree.deliver(
            NodeId::ROOT,
            member,
            Posted {
                offset: offset as u64,
                bytes: Bytes::copy_from_slice(bytes).into(),
                provenance: Provenance::Fresh,
            },
            &mut Objects::default(),
            &mut self.effects,
        );
        self.hand_over();
    }

    fn close(&mut self, member: MemberId) {
        self.tree.close(
            NodeId::ROOT,
            member,
            &mut Objects::default(),
            &mut self.effects,
        );
        self.hand_over();
    }

    // The owner hands each member bound since over to its object, as the
    // server's namespace does on every `Bound`, until no new one comes.
    fn hand_over(&mut self) {
        while self.handed < self.enough {
            let Some(member) = self.effects[self.done..]
                .iter()
                .find_map(|effect| match effect {
                    Effect::Bound {
                        node: NodeId::ROOT,
                        member,
                        ..
                    } => Some(*member),
                    _ => None,
                })
            else {
                self.done = self.effects.len();
                return;
            };
            let at = self.effects[self.done..]
                .iter()
                .position(
                    |effect| matches!(effect, Effect::Bound { member: m, .. } if *m == member),
                )
                .expect("the bound effect");
            self.done += at + 1;
            self.handed += 1;
            self.tree.handed_over(
                NodeId::ROOT,
                member,
                &mut Objects::default(),
                &mut self.effects,
            );
        }
    }

    // The owner, once it has read each member an engine took over back
    // through the store that gave it up, buries what that store retired,
    // as the server's namespace does on `NeedsEngine`.
    fn bury(&mut self, members: &[MemberId]) {
        for member in members {
            self.tree.bury(NodeId::ROOT, *member, &mut self.effects);
        }
        self.hand_over();
    }

    // The job goes quiet over the store holding `member`, once: a store
    // holding volumes the wrong way round lays them in their headers' order
    // then. One with nothing to order refuses its set instead, here, since
    // no writer placed its bytes: whether it did.
    fn quiet(&mut self, member: MemberId) -> bool {
        let (node, _) = self.tree.child_of(NodeId::ROOT, member).expect("held");
        let before = self.effects.len();
        self.tree
            .quiet(node, &mut Objects::default(), &mut self.effects);
        self.hand_over();
        self.effects[before..]
            .iter()
            .any(|effect| matches!(effect, Effect::Refused { .. }))
    }

    // Where each member is held.
    fn places(&self, members: &[MemberId]) -> Vec<Option<(NodeId, VolumeIndex)>> {
        members
            .iter()
            .map(|member| self.tree.child_of(NodeId::ROOT, *member))
            .collect()
    }

    fn given_back(&self, member: MemberId) -> usize {
        self.effects
            .iter()
            .filter(
                |effect| matches!(effect, Effect::GivenBack { member: at, .. } if *at == member),
            )
            .count()
    }

    fn at(&self, member: MemberId) -> Option<VolumeIndex> {
        self.tree
            .child_of(NodeId::ROOT, member)
            .map(|(_, volume)| volume)
    }
}

fn front(volume: &[u8]) -> &[u8] {
    &volume[..4096.min(volume.len())]
}

// A set's first volume posted under a later volume's name and left open,
// then its second posted under the first volume's name, read to its close.
// The first volume's headers place it first; the second's link it after.
#[test]
fn a_continuation_named_first_beside_a_first_named_third_settles() {
    let set = write(&items(), &split(Version::Rar4));
    assert_eq!(set.len(), 3);
    let mut run = Run::new();
    let first = run.announce("set.part3.rar", set[0].len());
    run.deliver(first, 0, front(&set[0]));
    let second = run.announce("set.part1.rar", set[1].len());
    run.deliver(second, 0, &set[1]);
    run.close(second);
    assert_eq!(run.handed, 2);
    assert_eq!(run.at(first), Some(VolumeIndex(0)));
    assert_eq!(run.at(second), Some(VolumeIndex(1)));
    assert_eq!(run.given_back(second), 0);
}

// Two sets of one kind. The first volume of one is posted first and left
// open; the second volume of the other is posted under the name of the first
// set's third, read to its close. Its header numbers it second, and the
// volume before it does not carry on into it: no place in the first set is
// its own.
#[test]
fn a_volume_whose_header_and_name_disagree_with_its_neighbour_settles() {
    let one = write(&items(), &split(Version::Rar5));
    let other = write(
        &[
            Item::File("delta.bin", payload(7, 10_000)),
            Item::File("epsilon.bin", payload(8, 5_003)),
        ],
        &split(Version::Rar5),
    );
    assert!(one.len() >= 3 && other.len() >= 2);
    let mut run = Run::new();
    let first = run.announce("set.part01.rar", one[0].len());
    run.deliver(first, 0, front(&one[0]));
    let held = run.tree.child_of(NodeId::ROOT, first).expect("a store");
    let stray = run.announce("set.part03.rar", other[1].len());
    run.deliver(stray, 0, &other[1]);
    run.close(stray);
    assert_eq!(run.handed, 2);
    assert_eq!(run.tree.child_of(NodeId::ROOT, first), Some(held));
    // Never the first set's: wherever it went, it is not held beside it.
    assert_ne!(
        run.tree.child_of(NodeId::ROOT, stray).map(|(node, _)| node),
        Some(held.0)
    );
    assert!(run.given_back(stray) <= 1);
}

// An archive that is no volume, posted under the name of a set's second
// volume, whose second file header never arrives: its walk stops short of
// its end, but past its main header and first file header, so its headers
// have said it is no volume. Its name places it nowhere: it is a set of its
// own from those first bytes, held at the first place of a store apart from
// the set's, read to its close, and never handed back and forth.
#[test]
fn an_archive_short_of_its_end_under_a_volume_name_is_handed_over_once() {
    let set = write(&items(), &split(Version::Rar4));
    let lone = write(&items(), &Options::new(Version::Rar4));
    assert_eq!(lone.len(), 1);
    let lone = &lone[0];
    let name = find(lone, b"beta.bin");
    // The second file header, name and all.
    let hole = (name - 32, name + 8);
    let mut run = Run::new();
    let first = run.announce("set.part1.rar", set[0].len());
    run.deliver(first, 0, &set[0]);
    run.close(first);
    let second = run.announce("set.part2.rar", lone.len());
    run.deliver(second, 0, &lone[..hole.0]);
    run.deliver(second, hole.1, &lone[hole.1..]);
    run.close(second);
    assert_eq!(run.handed, 2);
    assert_eq!(run.given_back(second), 0);
    assert_eq!(run.at(second), Some(VolumeIndex(0)));
    assert_ne!(
        run.tree
            .child_of(NodeId::ROOT, second)
            .map(|(node, _)| node),
        run.tree.child_of(NodeId::ROOT, first).map(|(node, _)| node)
    );
}

// A RAR5 volume's main header with its number set to `number`.
fn renumbered(volume: &[u8], number: u32) -> Vec<u8> {
    const SIGNATURE: usize = 8;
    let size = usize::from(volume[SIGNATURE + 4]);
    assert!(size < 0x80, "a one-byte header size");
    let inner = &volume[SIGNATURE + 5..SIGNATURE + 5 + size];
    // Kind, header flags, archive flags, volume number: one byte each.
    assert_eq!(inner, &[1, 0, inner[2], inner[3]], "a numbered main header");
    assert_eq!(inner[2] & 0x3, 0x3, "a volume with its number");
    let mut vint = Vec::new();
    let mut rest = number;
    loop {
        let low = (rest & 0x7f) as u8;
        rest >>= 7;
        if rest == 0 {
            vint.push(low);
            break;
        }
        vint.push(low | 0x80);
    }
    let mut sized = vec![(3 + vint.len()) as u8, 1, 0, inner[2]];
    sized.extend(vint);
    let header = crc32(&sized);
    let mut out = volume[..SIGNATURE].to_vec();
    out.extend(header.to_le_bytes());
    out.append(&mut sized);
    out.extend_from_slice(&volume[SIGNATURE + 5 + size..]);
    out
}

// The gap two far-numbered volumes leave before them, said as one run,
// marks what one loss told for each of its volumes marks: nothing, since
// the store holds none of them.
#[test]
fn a_gap_marks_what_a_loss_for_each_of_its_volumes_marks() {
    const AT: u32 = 2_000;
    let set = write(&items(), &split(Version::Rar5));
    let length = set[1].len() as u64;
    let posted = [renumbered(&set[1], AT), renumbered(&set[2], AT + 1)];
    let mut run = Run::new();
    let mut members = Vec::new();
    for (name, bytes) in ["c0ffee", "d00d"].into_iter().zip(&posted) {
        let member = run.announce(name, bytes.len());
        run.deliver(member, 0, bytes);
        members.push(member);
    }
    let before = run.effects.len();
    for member in &members {
        run.close(*member);
    }
    let marked = |effects: &[Effect]| {
        effects
            .iter()
            .filter(|effect| matches!(effect, Effect::Damaged { .. } | Effect::Lost { .. }))
            .count()
    };
    assert_eq!(marked(&run.effects[before..]), 0);
    let (node, _) = run.tree.child_of(NodeId::ROOT, members[0]).expect("bound");
    let mut each = Vec::new();
    for volume in 0..AT {
        run.tree.damage(
            node,
            VolumeIndex(volume),
            weaver_graph::ByteRange::new(0, length),
            &mut each,
        );
    }
    assert!(each.is_empty(), "{each:?}");
}

// Two volumes of a set numbered far past any first, the second its set's
// last, posted under names that number nothing. The work their close does
// for the volumes before them is a few effects, not one for every number.
#[test]
fn two_volumes_numbered_far_out_say_no_gap_for_every_number_before_them() {
    const FAR: u32 = 400_000_000;
    let set = write(&items(), &split(Version::Rar5));
    assert_eq!(set.len(), 3);
    let posted = [renumbered(&set[1], FAR), renumbered(&set[2], FAR + 1)];
    let mut run = Run::new();
    let mut members = Vec::new();
    for (name, bytes) in ["c0ffee", "d00d"].into_iter().zip(&posted) {
        let member = run.announce(name, bytes.len());
        run.deliver(member, 0, bytes);
        members.push(member);
    }
    assert_eq!(run.at(members[0]), Some(VolumeIndex(FAR)));
    assert_eq!(run.at(members[1]), Some(VolumeIndex(FAR + 1)));
    let before = run.effects.len();
    for &member in &members {
        run.close(member);
    }
    let damaged = run.effects[before..]
        .iter()
        .filter(|effect| matches!(effect, Effect::Damaged { .. }))
        .count();
    assert_eq!(damaged, 0);
    assert_eq!(run.handed, 2);
    assert!(
        run.effects.len() - before < 32,
        "{}",
        run.effects.len() - before
    );
    // The gap before them is said as one run, which marks nothing: no
    // volume of it is held. The set still never opens, and the quiet job
    // refuses it for the first volume it never had.
    assert!(
        !run.effects[before..]
            .iter()
            .any(|effect| matches!(effect, Effect::Refused { .. })),
    );
    let (node, _) = run.tree.child_of(NodeId::ROOT, members[0]).expect("bound");
    let before = run.effects.len();
    run.tree
        .quiet(node, &mut Objects::default(), &mut run.effects);
    assert!(run.effects[before..].iter().any(|effect| matches!(
        effect,
        Effect::Refused {
            why: Why::FirstBytesLost,
            ..
        }
    )));
}

// A six-volume set posted under a release's `part01` to `part06`, its true
// last volume as `part05` and its true fifth as `part06`, each renamed by
// a recovery set to its true one-digit `<stem>.partN.rar` as soon as its
// front is bound, as the bench post's PAR2 volumes renamed them. Fronts
// come in posted order, the rest of each volume after. Every volume is one
// store's, at its true place.
#[test]
fn a_set_renamed_from_swapped_posted_numbers_is_one_store() {
    for (version, whole) in [
        (Version::Rar4, false),
        (Version::Rar5, false),
        (Version::Rar4, true),
        (Version::Rar5, true),
    ] {
        let mut options = super::split(version);
        options.split = Some(10_000);
        let set = write(&[Item::File("track.flac", payload(9, 57_000))], &options);
        assert_eq!(set.len(), 6, "{version:?}");
        let mut run = Run::new();
        // Posted number, true volume.
        let posted = [(1, 0), (2, 1), (3, 2), (4, 3), (5, 5), (6, 4)];
        // The job lists every file before any byte arrives.
        let mut members = [MemberId(0); 6];
        for (number, volume) in posted {
            members[volume] =
                run.announce(&format!("Release.part{number:02}.rar"), set[volume].len());
        }
        let rest = |run: &mut Run, volume: usize| {
            let at = front(&set[volume]).len();
            run.deliver(members[volume], at, &set[volume][at..]);
            run.close(members[volume]);
        };
        for (_, volume) in posted {
            let member = members[volume];
            run.deliver(member, 0, front(&set[volume]));
            run.tree.rename(
                NodeId::ROOT,
                member,
                &format!("5Kw2CyRs.part{}.rar", volume + 1),
                &mut Objects::default(),
                &mut run.effects,
            );
            run.hand_over();
            if whole {
                rest(&mut run, volume);
            }
        }
        if !whole {
            for (_, volume) in posted {
                rest(&mut run, volume);
            }
        }
        let places: Vec<_> = members
            .iter()
            .map(|member| run.tree.child_of(NodeId::ROOT, *member))
            .collect();
        let store = places[0].map(|(node, _)| node);
        for (volume, place) in places.iter().enumerate() {
            assert_eq!(
                *place,
                store.map(|node| (node, VolumeIndex(volume as u32))),
                "{version:?} whole {whole} volume {volume}: {places:?}"
            );
        }
    }
}

// The same set compressed, so only the engine reads it, in RAR4, whose end
// records here state no number: names alone order it. A recovery set
// renames its volumes one at a time to a stem no posted name carries, and
// each rename weighs against the recovery's own names, not the posted names
// of volumes it has not renamed yet. A name that moves its volume onto a
// place a posted name holds takes the place, and the holder is placed
// again. Posted backwards, one volume founds a store of its own before the
// engine holds the rest; the recovery's names bring it into the engine. In
// whatever order the renames come, every volume is one engine's, at its
// true place.
#[test]
fn a_compressed_set_renamed_from_swapped_posted_numbers_is_one_engine() {
    // Posted number, true volume: the last two swapped, the first two, or
    // every one backwards.
    let last = [(1, 0), (2, 1), (3, 2), (4, 3), (5, 5), (6, 4)];
    let first = [(1, 1), (2, 0), (3, 2), (4, 3), (5, 4), (6, 5)];
    let backwards = [(1, 5), (2, 4), (3, 3), (4, 2), (5, 1), (6, 0)];
    let renames = [
        [0, 1, 2, 3, 5, 4],
        [4, 5, 3, 2, 1, 0],
        [1, 2, 3, 4, 5, 0],
        [5, 4, 3, 2, 1, 0],
        [2, 0, 1, 3, 4, 5],
    ];
    for (posted, order) in [last, first, backwards]
        .into_iter()
        .flat_map(|posted| renames.map(|order| (posted, order)))
    {
        let mut options = super::split(Version::Rar4);
        options.split = Some(10_000);
        options.compressed = Some(0);
        let set = write(&[Item::File("track.flac", payload(9, 57_000))], &options);
        assert_eq!(set.len(), 6);
        let mut run = Run::new();
        let mut members = [MemberId(0); 6];
        for (number, volume) in posted {
            members[volume] =
                run.announce(&format!("Release.part{number:02}.rar"), set[volume].len());
        }
        for (_, volume) in posted {
            run.deliver(members[volume], 0, &set[volume]);
            run.close(members[volume]);
        }
        run.bury(&members);
        let engine = run
            .tree
            .child_of(NodeId::ROOT, members[0])
            .map(|(node, _)| node);
        assert_eq!(
            engine.and_then(|node| run.tree.kind(node)),
            Some(Kind::RarEngine)
        );
        for volume in order {
            run.tree.rename(
                NodeId::ROOT,
                members[volume],
                &format!("5Kw2CyRs.part{}.rar", volume + 1),
                &mut Objects::default(),
                &mut run.effects,
            );
            run.hand_over();
            run.bury(&members);
        }
        let places: Vec<_> = members
            .iter()
            .map(|member| run.tree.child_of(NodeId::ROOT, *member))
            .collect();
        let engine = places[0].map(|(node, _)| node);
        assert_eq!(
            engine.and_then(|node| run.tree.kind(node)),
            Some(Kind::RarEngine)
        );
        for (volume, place) in places.iter().enumerate() {
            assert_eq!(
                *place,
                engine.map(|node| (node, VolumeIndex(volume as u32))),
                "posted {posted:?}, renamed in {order:?}, volume {volume}: {places:?}"
            );
        }
    }
}

// Two compressed RAR4 sets posted under one stem, their end records stating
// no number: set A's four volumes, its last two under each other's numbers,
// and set B's second volume under the number after A's last, so the engine
// holds it by its posted name alone. B's recovery set renames that volume
// first. No member runs across from A's last volume into it, so B's name
// does not start the engine's recovery numbering: the volume leaves the
// engine. A's own recovery names then bring A's volumes to their true
// places, in one engine.
#[test]
fn another_sets_volume_held_by_its_posted_name_leaves_when_renamed_first() {
    let set = |name: &'static str, seed: u64| {
        let mut options = super::split(Version::Rar4);
        options.split = Some(10_000);
        options.compressed = Some(0);
        write(&[Item::File(name, payload(seed, 37_000))], &options)
    };
    let a = set("track.flac", 9);
    let b = set("other.flac", 11);
    assert_eq!(a.len(), 4);
    assert!(b.len() > 2);
    let mut run = Run::new();
    // Posted number, volume of A.
    let posted = [(1, 0), (2, 1), (3, 3), (4, 2)];
    let mut members = [MemberId(0); 4];
    for (number, volume) in posted {
        members[volume] = run.announce(&format!("Release.part{number:02}.rar"), a[volume].len());
    }
    let stray = run.announce("Release.part05.rar", b[1].len());
    for (_, volume) in posted {
        run.deliver(members[volume], 0, &a[volume]);
        run.close(members[volume]);
    }
    run.deliver(stray, 0, &b[1]);
    run.close(stray);
    let mut all = members.to_vec();
    all.push(stray);
    run.bury(&all);
    let engine = run
        .tree
        .child_of(NodeId::ROOT, members[0])
        .map(|(node, _)| node);
    assert_eq!(
        engine.and_then(|node| run.tree.kind(node)),
        Some(Kind::RarEngine)
    );
    assert_eq!(
        run.tree.child_of(NodeId::ROOT, stray),
        engine.map(|node| (node, VolumeIndex(4))),
        "the engine holds B's volume by its posted name"
    );
    run.tree.rename(
        NodeId::ROOT,
        stray,
        "Other.part2.rar",
        &mut Objects::default(),
        &mut run.effects,
    );
    run.hand_over();
    run.bury(&all);
    let left = run.tree.child_of(NodeId::ROOT, stray).map(|(node, _)| node);
    assert_ne!(left, engine, "B's volume is still the engine's");
    for (volume, member) in members.iter().enumerate() {
        run.tree.rename(
            NodeId::ROOT,
            *member,
            &format!("5Kw2CyRs.part{}.rar", volume + 1),
            &mut Objects::default(),
            &mut run.effects,
        );
        run.hand_over();
        run.bury(&all);
    }
    let places: Vec<_> = members
        .iter()
        .map(|member| run.tree.child_of(NodeId::ROOT, *member))
        .collect();
    let engine = places[0].map(|(node, _)| node);
    assert_eq!(
        engine.and_then(|node| run.tree.kind(node)),
        Some(Kind::RarEngine)
    );
    for (volume, place) in places.iter().enumerate() {
        assert_eq!(
            *place,
            engine.map(|node| (node, VolumeIndex(volume as u32))),
            "volume {volume}: {places:?}"
        );
    }
    let left = run.tree.child_of(NodeId::ROOT, stray).map(|(node, _)| node);
    assert_ne!(left, engine, "B's volume came back to A's engine");
}

// A compressed RAR4 set, its end records stating no number, some of its
// volumes held and the rest not yet come. The recovery set renames one
// volume first while neither volume beside it is held: nothing ties it to
// the set yet, so it is given back. Its neighbours come after, and the
// recovery's names for the rest bring it back: the set ends one engine,
// every volume at its true place, posted in order or with its last two
// numbers swapped.
#[test]
fn a_volume_renamed_before_its_neighbours_come_rejoins_its_engine() {
    // Volumes in the set, volumes held before the first rename, the volume
    // renamed first, and the order the rest arrive in.
    let cases: [(usize, &[usize], usize, &[usize]); 4] = [
        (4, &[0, 3], 3, &[1, 2]),
        (4, &[0, 3], 3, &[2, 1]),
        (6, &[0, 3, 5], 3, &[2, 4, 1]),
        (6, &[0, 1, 5], 1, &[4, 2, 3]),
    ];
    for (count, early, first, rest) in cases {
        for swapped in [false, true] {
            let mut options = super::split(Version::Rar4);
            options.split = Some(10_000);
            options.compressed = Some(0);
            let set = write(
                &[Item::File("track.flac", payload(9, count * 10_000 - 3_000))],
                &options,
            );
            assert_eq!(set.len(), count);
            let posted = |volume: usize| match volume {
                _ if swapped && volume == count - 2 => count,
                _ if swapped && volume == count - 1 => count - 1,
                _ => volume + 1,
            };
            let mut run = Run::new();
            let members: Vec<MemberId> = (0..count)
                .map(|volume| {
                    run.announce(
                        &format!("Release.part{:02}.rar", posted(volume)),
                        set[volume].len(),
                    )
                })
                .collect();
            let arrive = |run: &mut Run, volume: usize| {
                run.deliver(members[volume], 0, &set[volume]);
                run.close(members[volume]);
                run.bury(&members);
            };
            let rename = |run: &mut Run, volume: usize| {
                run.tree.rename(
                    NodeId::ROOT,
                    members[volume],
                    &format!("5Kw2CyRs.part{}.rar", volume + 1),
                    &mut Objects::default(),
                    &mut run.effects,
                );
                run.hand_over();
                run.bury(&members);
            };
            for volume in early {
                arrive(&mut run, *volume);
            }
            rename(&mut run, first);
            for volume in rest {
                arrive(&mut run, *volume);
            }
            for volume in (0..count).filter(|volume| *volume != first) {
                rename(&mut run, volume);
            }
            let places: Vec<_> = members
                .iter()
                .map(|member| run.tree.child_of(NodeId::ROOT, *member))
                .collect();
            let engine = places[0].map(|(node, _)| node);
            assert_eq!(
                engine.and_then(|node| run.tree.kind(node)),
                Some(Kind::RarEngine),
                "{count} volumes, swapped {swapped}: {places:?}"
            );
            for (volume, place) in places.iter().enumerate() {
                assert_eq!(
                    *place,
                    engine.map(|node| (node, VolumeIndex(volume as u32))),
                    "{count} volumes, swapped {swapped}, early {early:?}, \
                     first {first}, volume {volume}: {places:?}"
                );
            }
        }
    }
}

// A long compressed RAR4 set, its end records stating no number, its last
// two volumes posted under each other's numbers, renamed one volume at a
// time by a recovery set to a stem no posted name carries. Placing each
// volume again reads the volumes bound by index: a rename looks up a few
// of them, however long the set, and only a volume placed by its links
// alone walks the set. Every volume is one engine's, at its true place.
#[test]
fn a_long_compressed_set_renamed_one_at_a_time_looks_volumes_up() {
    const VOLUMES: usize = 300;
    let mut options = super::split(Version::Rar4);
    options.split = Some(5_000);
    options.compressed = Some(0);
    let set = write(
        &[Item::File("track.flac", payload(9, VOLUMES * 5_000 - 700))],
        &options,
    );
    assert_eq!(set.len(), VOLUMES);
    let mut run = Run::new();
    run.enough = 4 * VOLUMES;
    let posted: Vec<usize> = (0..VOLUMES)
        .map(|volume| match volume {
            _ if volume == VOLUMES - 2 => VOLUMES - 1,
            _ if volume == VOLUMES - 1 => VOLUMES - 2,
            _ => volume,
        })
        .collect();
    let mut members = vec![MemberId(0); VOLUMES];
    for (number, volume) in posted.iter().enumerate() {
        members[*volume] = run.announce(
            &format!("Release.part{:03}.rar", number + 1),
            set[*volume].len(),
        );
    }
    for volume in &posted {
        run.deliver(members[*volume], 0, &set[*volume]);
        run.close(members[*volume]);
    }
    crate::engine::counts::take();
    // The most volumes a rename looked up, and the renames that walked the
    // set, with how many volumes each walked.
    let mut most = 0;
    let mut walks = Vec::new();
    for (volume, member) in members.iter().enumerate() {
        run.tree.rename(
            NodeId::ROOT,
            *member,
            &format!("5Kw2CyRs.part{:03}.rar", volume + 1),
            &mut Objects::default(),
            &mut run.effects,
        );
        run.hand_over();
        let counted = crate::engine::counts::take();
        match counted.walked {
            0 => most = most.max(counted.lookups),
            walked => walks.push((volume, walked)),
        }
    }
    assert!(most <= 4, "a rename looked up {most} volumes");
    // The one rename that moves its volume onto a place a posted name held:
    // the holder, given back, is placed by its links, once.
    assert_eq!(walks.len(), 1, "{walks:?}");
    assert!(walks[0].1 <= VOLUMES, "{walks:?}");
    let places: Vec<_> = members
        .iter()
        .map(|member| run.tree.child_of(NodeId::ROOT, *member))
        .collect();
    let engine = places[0].map(|(node, _)| node);
    assert_eq!(
        engine.and_then(|node| run.tree.kind(node)),
        Some(Kind::RarEngine)
    );
    for (volume, place) in places.iter().enumerate() {
        assert_eq!(
            *place,
            engine.map(|node| (node, VolumeIndex(volume as u32))),
            "volume {volume}"
        );
    }
}

// A long RAR4 set whose end records state no number, its last two volumes
// posted under each other's numbers, renamed to their true names before the
// job is quiet: the names place the two, and the set is never ordered by
// its headers. Its rest comes in small pieces with no new header among
// them, as articles do.
#[test]
fn a_long_set_renamed_before_quiet_is_placed_by_its_names() {
    const VOLUMES: usize = 300;
    let set = long_set(VOLUMES);
    let mut run = Run::new();
    run.enough = 4 * VOLUMES;
    let posted: Vec<usize> = (0..VOLUMES)
        .map(|volume| match volume {
            _ if volume == VOLUMES - 2 => VOLUMES - 1,
            _ if volume == VOLUMES - 1 => VOLUMES - 2,
            _ => volume,
        })
        .collect();
    let mut members = vec![MemberId(0); VOLUMES];
    for (number, volume) in posted.iter().enumerate() {
        members[*volume] = run.announce(
            &format!("Release.part{:03}.rar", number + 1),
            set[*volume].len(),
        );
    }
    counts::take();
    for volume in &posted {
        run.deliver(members[*volume], 0, front(&set[*volume]));
    }
    for volume in &posted {
        rest_in_pieces(&mut run, members[*volume], &set[*volume]);
    }
    for volume in [VOLUMES - 2, VOLUMES - 1] {
        run.tree.rename(
            NodeId::ROOT,
            members[volume],
            &format!("5Kw2CyRs.part{}.rar", volume + 1),
            &mut Objects::default(),
            &mut run.effects,
        );
        run.hand_over();
    }
    let held = run.places(&members);
    assert!(run.quiet(members[0]), "nothing to order");
    let counted = counts::take();
    assert_eq!(counted.orders, 0);
    // The first rename gives the true second-to-last its place while the
    // true last, held there on trust by its posted name, still sits in it:
    // the layout refutes that place once and is laid again short of it.
    assert_eq!(counted.cut, 1, "{counted:?}");
    assert!(
        counted.asked.len() <= 3 * VOLUMES,
        "{}",
        counted.asked.len()
    );
    at_true_places(&held, "renamed");
    // Only the two renamed move: the first renamed waits for the second to
    // leave its place, so it is handed on twice.
    let given: Vec<usize> = members
        .iter()
        .map(|member| run.given_back(*member))
        .collect();
    assert!(given[..VOLUMES - 2].iter().all(|given| *given == 0));
    assert_eq!(given[VOLUMES - 2..], [2, 1]);
}

// One track split over `volumes` RAR4 volumes whose end records state no
// number.
fn long_set(volumes: usize) -> Vec<Vec<u8>> {
    let mut options = super::split(Version::Rar4);
    options.split = Some(5_000);
    let set = write(
        &[Item::File("track.flac", payload(9, volumes * 5_000 - 700))],
        &options,
    );
    assert_eq!(set.len(), volumes);
    set
}

// A volume's bytes past its front, in small pieces with no new header
// among them, as articles bring them, then its close.
fn rest_in_pieces(run: &mut Run, member: MemberId, bytes: &[u8]) {
    let mut at = front(bytes).len();
    while at < bytes.len() {
        let to = (at + 256).min(bytes.len());
        run.deliver(member, at, &bytes[at..to]);
        at = to;
    }
    run.close(member);
}

// Where each volume of `set` ends up when posted under `numbers`, the
// posted number of each true volume, with no name ever corrected: fronts in
// posted order, each volume's rest after its front when `whole`, else every
// rest after every front, then the job quiet. Each volume's place, how
// often it was given back, and what the store counted.
type Places = Vec<Option<(NodeId, VolumeIndex)>>;

struct Laid {
    places: Places,
    given: Vec<usize>,
    counted: counts::Counts,
}

fn posted_unrenamed(set: &[Vec<u8>], numbers: &[usize], whole: bool) -> Laid {
    let mut run = Run::new();
    run.enough = 4 * set.len();
    let mut members = vec![MemberId(0); set.len()];
    let mut posted: Vec<(usize, usize)> = numbers
        .iter()
        .enumerate()
        .map(|(volume, number)| (*number, volume))
        .collect();
    posted.sort_unstable();
    for (number, volume) in &posted {
        members[*volume] =
            run.announce(&format!("Release.part{number:03}.rar"), set[*volume].len());
    }
    counts::take();
    for (_, volume) in &posted {
        run.deliver(members[*volume], 0, front(&set[*volume]));
        if whole {
            rest_in_pieces(&mut run, members[*volume], &set[*volume]);
        }
    }
    if !whole {
        for (_, volume) in &posted {
            rest_in_pieces(&mut run, members[*volume], &set[*volume]);
        }
    }
    let held = run.places(&members);
    let refused = run.quiet(members[0]);
    let counted = counts::take();
    Laid {
        // A set with nothing to order is refused here at quiet, for want of
        // a writer: it stood where it was held.
        places: match refused {
            true => held,
            false => run.places(&members),
        },
        given: members
            .iter()
            .map(|member| run.given_back(*member))
            .collect(),
        counted,
    }
}

// Every volume in one store, at its true place.
fn at_true_places(places: &[Option<(NodeId, VolumeIndex)>], case: &str) {
    let store = places[0].map(|(node, _)| node);
    assert!(store.is_some(), "{case}");
    for (volume, place) in places.iter().enumerate() {
        assert_eq!(
            *place,
            store.map(|node| (node, VolumeIndex(volume as u32))),
            "{case} volume {volume}: {places:?}"
        );
    }
}

// A RAR4 set with no end records, so no header states a number past the
// first, posted as a film's was: its first volume under the release's
// `part01.rar`, the rest under the poster's `<stem>.rNN`. Every volume's
// front holds its whole header, so each walk reads to its end at once.
// The middle volumes each only carry the one member on, which links every
// free place to all of them alike; only their names order them. Fronts come
// in posted order, with a middle volume before the one before it, and last
// volume first: each volume is one store's, at its true place.
#[test]
fn a_middle_volume_before_the_one_before_it_takes_its_named_place() {
    let mut options = super::split(Version::Rar4);
    options.split = Some(5_000);
    let set: Vec<Vec<u8>> = write(
        &[Item::File("track.flac", payload(9, 6 * 5_000 - 700))],
        &options,
    )
    .into_iter()
    .map(|mut volume| {
        // The end record the writer closes each volume with: seven bytes.
        assert_eq!(volume[volume.len() - 5], 0x7b);
        volume.truncate(volume.len() - 7);
        volume
    })
    .collect();
    assert_eq!(set.len(), 6);
    for order in [
        [0, 1, 2, 3, 4, 5],
        [0, 2, 1, 3, 4, 5],
        [0, 3, 2, 1, 4, 5],
        [5, 4, 3, 2, 1, 0],
    ] {
        let mut run = Run::new();
        let members: Vec<MemberId> = (0..set.len())
            .map(|volume| {
                let name = match volume {
                    0 => "Release.part01.rar".to_owned(),
                    _ => format!("65606-1.r{:02}", volume - 1),
                };
                run.announce(&name, set[volume].len())
            })
            .collect();
        for volume in order {
            run.deliver(members[volume], 0, front(&set[volume]));
        }
        for volume in order {
            let at = front(&set[volume]).len();
            run.deliver(members[volume], at, &set[volume][at..]);
            run.close(members[volume]);
        }
        let store = run
            .tree
            .child_of(NodeId::ROOT, members[0])
            .map(|(node, _)| node);
        assert!(store.is_some(), "{order:?}");
        for (volume, member) in members.iter().enumerate() {
            assert_eq!(
                run.tree.child_of(NodeId::ROOT, *member),
                store.map(|node| (node, VolumeIndex(volume as u32))),
                "{order:?} volume {volume}"
            );
        }
    }
}

// A pack of sets and whole archives posted under one shared `partNNN`
// stem, in the order its posts came in, each post delivered in frames so
// that every volume is held short of its end while the next ones arrive.
// The posted numbers run on across sets; only the volumes' own headers set
// them apart. A whole archive states at its main header that it is no
// volume and founds a set of its own from its first bytes; a set's volumes
// state their numbers (RAR5 in the main header, RAR4 in the end record, as
// rar writes them) and sit at those places in their own set's store.
#[test]
fn a_pack_under_one_stem_is_placed_by_its_volume_headers() {
    const FRAMED: usize = 32 * 1024;
    let sets: &[(usize, usize)] = &[
        (1, 1),
        (2, 8),
        (10, 2),
        (12, 1),
        (13, 2),
        (15, 2),
        (17, 1),
        (18, 1),
        (19, 1),
        (20, 1),
        (21, 12),
        (33, 2),
        (35, 1),
        (36, 1),
        (37, 1),
        (38, 1),
        (39, 1),
        (40, 1),
        (41, 1),
        (42, 1),
        (43, 1),
        (44, 1),
        (45, 1),
        (46, 1),
        (47, 1),
        (48, 1),
    ];
    let arrival = [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 41, 21, 42, 43, 44,
        45, 22, 46, 47, 48, 23, 24, 31, 25, 32, 26, 33, 27, 34, 28, 35, 29, 30, 36, 37, 38, 39, 40,
    ];
    for version in [Version::Rar5, Version::Rar4] {
        let mut by_number = std::collections::BTreeMap::new();
        let mut truth = Vec::new();
        for (index, (first, volumes)) in sets.iter().enumerate() {
            let name: &'static str = Box::leak(format!("clip{index:02}.mp4").into_boxed_str());
            let mut options = Options::new(version);
            options.numbered = true;
            if *volumes > 1 {
                options.split = Some(98 * 1024);
            }
            let size = match *volumes {
                1 => 3 * 32 * 1024,
                n => n * 98 * 1024 - 4096,
            };
            let set = write(
                &[Item::File(name, payload(index as u64 + 3, size))],
                &options,
            );
            assert_eq!(set.len(), *volumes, "{version:?} set {index}");
            for (at, bytes) in set.into_iter().enumerate() {
                by_number.insert(first + at, bytes);
                truth.push((first + at, index, at));
            }
        }
        let posted = |number: usize| match number < 10 {
            true => format!("Karla.Pack.XXX-U4A.part{number:02}.rar"),
            false => format!("Karla.Pack.XXX-U4A.part{number:03}.rar"),
        };
        let mut run = Run::new();
        let mut members = std::collections::BTreeMap::new();
        for number in arrival {
            let bytes = &by_number[&number];
            let member = run.announce(&posted(number), bytes.len());
            for at in (0..bytes.len()).step_by(FRAMED) {
                run.deliver(member, at, &bytes[at..(at + FRAMED).min(bytes.len())]);
            }
            run.close(member);
            members.insert(number, member);
        }
        let mut stores = std::collections::BTreeMap::new();
        for (number, set, at) in &truth {
            let held = run.tree.child_of(NodeId::ROOT, members[number]);
            let Some((node, index)) = held else {
                panic!("{version:?} part{number:03} of set {set} is held by no store");
            };
            assert_eq!(
                index,
                VolumeIndex(*at as u32),
                "{version:?} part{number:03} is volume {at} of set {set}"
            );
            assert_eq!(
                *stores.entry(*set).or_insert(node),
                node,
                "{version:?} part{number:03}: set {set} is held by one store"
            );
        }
        let nodes: std::collections::BTreeSet<_> = stores.values().collect();
        assert_eq!(nodes.len(), sets.len(), "{version:?}: one store per set");
    }
}

// A six-volume RAR4 set whose end records state no number, posted under a
// release's `part001` to `part006` with its true last volume as `part005`
// and its true fifth as `part006`, and no name ever corrected. The last
// volume's header closes the track the fifth carries on, so the fifth comes
// first: at quiet the two trade places once each, and the set is one store.
#[test]
fn a_set_posted_under_swapped_numbers_is_ordered_by_its_headers() {
    let mut options = super::split(Version::Rar4);
    options.split = Some(10_000);
    let set = write(&[Item::File("track.flac", payload(9, 57_000))], &options);
    assert_eq!(set.len(), 6);
    for whole in [false, true] {
        let posted = posted_unrenamed(&set, &[1, 2, 3, 4, 6, 5], whole);
        at_true_places(&posted.places, &format!("whole {whole}"));
        assert_eq!(posted.given, [0, 0, 0, 0, 1, 1], "whole {whole}");
        assert_eq!(posted.counted.orders, 1, "whole {whole}");
    }
}

// The same set with two middle volumes posted under each other's numbers:
// each only carries the track on, so their headers are alike and say
// nothing of their order. The names stand, and neither moves.
#[test]
fn two_middle_volumes_alike_keep_their_posted_places() {
    let mut options = super::split(Version::Rar4);
    options.split = Some(10_000);
    let set = write(&[Item::File("track.flac", payload(9, 57_000))], &options);
    for whole in [false, true] {
        let posted = posted_unrenamed(&set, &[1, 3, 2, 4, 5, 6], whole);
        let store = posted.places[0].map(|(node, _)| node);
        let at =
            |volume: usize| posted.places[volume].map(|(node, at)| (Some(node) == store, at.0));
        assert_eq!(at(1), Some((true, 2)), "whole {whole}");
        assert_eq!(at(2), Some((true, 1)), "whole {whole}");
        assert_eq!(posted.given, [0; 6], "whole {whole}");
        assert_eq!(posted.counted.orders, 0, "whole {whole}");
    }
}

// A long set posted in its true order under its true numbers is mapped as
// it arrives and never ordered: each volume is laid on what its front says
// and again on what its end says, however its bytes come in.
#[test]
fn a_long_set_in_order_is_never_ordered() {
    const VOLUMES: usize = 300;
    let set = long_set(VOLUMES);
    let numbers: Vec<usize> = (1..=VOLUMES).collect();
    for whole in [false, true] {
        let posted = posted_unrenamed(&set, &numbers, whole);
        at_true_places(&posted.places, &format!("whole {whole}"));
        assert_eq!(posted.given, vec![0; VOLUMES], "whole {whole}");
        let counted = &posted.counted;
        assert_eq!((counted.orders, counted.cut), (0, 0), "whole {whole}");
        // The last volume's front holds its end record: it is laid once.
        assert_eq!(counted.asked.len(), 2 * VOLUMES - 1, "whole {whole}");
    }
}

// A long set held the wrong way round is ordered once, at quiet: each
// volume moves once at most, and is laid a few times at most, however
// many volumes the set holds and however its bytes come in.
fn ordered_once(volumes: usize, numbers: &[usize], moving: &[usize]) {
    // Delivered whole, the misplaced volume is laid at its posted place
    // before the set is seen out of order: the layout refutes that place
    // once. In pieces every front is in first, and nothing is refuted.
    let cut = |whole| usize::from(whole);
    ordered_once_as(&long_set(volumes), numbers, moving, "", &[false, true], cut);
}

// [`ordered_once`] over `set` as given, `shape` naming it, its bytes coming
// in each of the ways `wholes` names, the layout refuting a place as often
// as `cut` says for each.
fn ordered_once_as(
    set: &[Vec<u8>],
    numbers: &[usize],
    moving: &[usize],
    shape: &str,
    wholes: &[bool],
    cut: impl Fn(bool) -> usize,
) {
    let volumes = set.len();
    for &whole in wholes {
        let case = format!("{shape} {volumes} whole {whole}");
        let posted = posted_unrenamed(set, numbers, whole);
        at_true_places(&posted.places, &case);
        for (volume, given) in posted.given.iter().enumerate() {
            let moves = usize::from(moving.contains(&volume));
            assert_eq!(*given, moves, "{case} volume {volume}");
        }
        let counted = &posted.counted;
        assert_eq!(counted.cut, cut(whole), "{case}");
        assert_eq!(counted.orders, 1, "{case}");
        assert!(
            counted.asked.len() <= 3 * volumes,
            "{case}: {}",
            counted.asked.len()
        );
    }
}

// The last two volumes posted under each other's numbers: the two move.
#[test]
fn a_long_set_with_its_last_two_swapped_is_ordered_once() {
    for volumes in [40, 300] {
        let numbers: Vec<usize> = (0..volumes)
            .map(|volume| match volume {
                _ if volume == volumes - 2 => volumes,
                _ if volume == volumes - 1 => volumes - 1,
                _ => volume + 1,
            })
            .collect();
        ordered_once(volumes, &numbers, &[volumes - 2, volumes - 1]);
    }
}

// The true last volume posted under the second number, every volume after
// it one number later: every volume but the first moves, once.
#[test]
fn a_last_volume_posted_second_is_ordered_once() {
    for volumes in [40, 300] {
        let numbers: Vec<usize> = (0..volumes)
            .map(|volume| match volume {
                0 => 1,
                _ if volume == volumes - 1 => 2,
                _ => volume + 2,
            })
            .collect();
        let moving: Vec<usize> = (1..volumes).collect();
        ordered_once(volumes, &numbers, &moving);
    }
}

// The one file header of a RAR4 volume [`long_set`] writes, rewritten by
// `change` over its body (packed size, unpacked size, host, CRC, time,
// version, method, name length, attributes, name), its header CRC sealed
// again. The header's size stays what it was.
fn rewrite_rar4_part(volume: &mut [u8], change: impl FnOnce(&mut [u8])) {
    // The signature, then the main header.
    const AT: usize = 7 + 13;
    assert_eq!(volume[AT + 2], 0x74, "a file header");
    let size = u16::from_le_bytes([volume[AT + 5], volume[AT + 6]]) as usize;
    change(&mut volume[AT + 7..AT + size]);
    let crc = (crc32(&volume[AT + 2..AT + size]) as u16).to_le_bytes();
    volume[AT..AT + 2].copy_from_slice(&crc);
}

// The 548b shape, as its post was: no volume has an end record, and every
// part between the first and the last states a larger size for the member
// than the two ends do.
fn overstating(volumes: usize) -> Vec<Vec<u8>> {
    let mut set = long_set(volumes);
    for volume in &mut set {
        // The end record the writer closes each volume with: seven bytes.
        assert_eq!(volume[volume.len() - 5], 0x7b);
        volume.truncate(volume.len() - 7);
    }
    for volume in &mut set[1..volumes - 1] {
        rewrite_rar4_part(volume, |body| {
            let size = u32::from_le_bytes(body[4..8].try_into().unwrap());
            body[4..8].copy_from_slice(&(size + 62_609_861).to_le_bytes());
        });
    }
    set
}

// The 7e3f shape: every part past the first names the member otherwise, as
// the packer wrote it.
fn renamed_on(volumes: usize) -> Vec<Vec<u8>> {
    let mut set = long_set(volumes);
    for volume in &mut set[1..] {
        rewrite_rar4_part(volume, |body| {
            let at = body.len() - "track.flac".len();
            assert_eq!(&body[at..], b"track.flac");
            body[at..].copy_from_slice(b"TRACK.flac");
        });
    }
    set
}

// Either shape with its last two volumes posted under each other's numbers
// and nothing to rename them: the headers order the two once, at quiet,
// with the same bounds a set naming its member alike keeps. The order
// links parts by the name alone, as the claim does, so the 548b set is
// ordered even where the run follows the first volume, whose part states
// the true size and the next one a larger. The 7e3f set is ordered where
// its fronts come before its rest and the run follows a volume past the
// first: the claim holds a part named otherwise than the first part apart
// from it (`claim::apart`), so a volume read whole beside the first one,
// or a set of three, is not this order's to lay.
#[test]
fn a_set_misstating_its_member_with_its_last_two_swapped_is_ordered_once() {
    for volumes in [3, 6, 40] {
        let numbers: Vec<usize> = (0..volumes)
            .map(|volume| match volume {
                _ if volume == volumes - 2 => volumes,
                _ if volume == volumes - 1 => volumes - 1,
                _ => volume + 1,
            })
            .collect();
        let moving = [volumes - 2, volumes - 1];
        // With no end record, each front holds its volume's whole header,
        // so the misplaced volume is laid at its posted place before the set
        // is seen out of order, however its bytes come in: the layout
        // refutes that place once.
        let set = overstating(volumes);
        ordered_once_as(&set, &numbers, &moving, "548b", &[false, true], |_| 1);
        if volumes > 3 {
            let set = renamed_on(volumes);
            ordered_once_as(&set, &numbers, &moving, "7e3f", &[false], |_| 0);
        }
    }
}

// A RAR4 set with no end records, so its volumes state no number past the
// first and each front holds its whole header: an end read that states no
// number settles a volume only where its own name does not number it
// elsewhere (`claim::settled`). Posted under its true numbers, every volume
// is at its true place. Posted with every volume past the first one number
// on, the names put each one place past its true one and leave the second
// place empty; nothing in the headers states a number, so each is held
// where its name puts it, none is given back, and the set waits on the
// second place, as it did before `settled` read the name.
#[test]
fn a_set_named_one_place_on_is_held_where_its_names_put_it() {
    for volumes in [3, 6] {
        let set: Vec<Vec<u8>> = long_set(volumes)
            .into_iter()
            .map(|mut volume| {
                // The end record the writer closes each volume with.
                assert_eq!(volume[volume.len() - 5], 0x7b);
                volume.truncate(volume.len() - 7);
                volume
            })
            .collect();
        for whole in [false, true] {
            let case = format!("{volumes} whole {whole}");
            let numbers: Vec<usize> = (1..=volumes).collect();
            let posted = posted_unrenamed(&set, &numbers, whole);
            at_true_places(&posted.places, &case);
            assert_eq!(posted.given, vec![0; volumes], "{case}");

            let numbers: Vec<usize> = (0..volumes)
                .map(|volume| if volume == 0 { 1 } else { volume + 2 })
                .collect();
            let posted = posted_unrenamed(&set, &numbers, whole);
            let store = posted.places[0].map(|(node, _)| node);
            for (volume, place) in posted.places.iter().enumerate() {
                let named = if volume == 0 { 0 } else { volume + 1 };
                assert_eq!(
                    *place,
                    store.map(|node| (node, VolumeIndex(named as u32))),
                    "{case} one on, volume {volume}"
                );
            }
            assert_eq!(posted.given, vec![0; volumes], "{case} one on");
        }
    }
}

// A long compressed RAR4 set, its end records stating no number, posted in
// order under names that number nothing. The engine places each volume by
// its links alone, and a placement reads the volumes bound beside it by
// index: the set is walked a bounded number of times, however long it is,
// not once per volume placed.
//
// Measured on 2026-11-18 before the engine filed the places beside its
// volumes: 4,950 volumes walked and 14,949 looked up for 100 volumes, 44,850
// and 134,849 for 300, which is N(N-1)/2 walked, nine times the work for
// three times the set.
#[test]
fn a_long_nameless_compressed_set_walks_its_volumes_a_bounded_number_of_times() {
    for volumes in [100, 300] {
        let mut options = super::split(Version::Rar4);
        options.split = Some(5_000);
        options.compressed = Some(0);
        let set = write(
            &[Item::File("track.flac", payload(9, volumes * 5_000 - 700))],
            &options,
        );
        assert_eq!(set.len(), volumes);
        let mut run = Run::new();
        run.enough = 4 * volumes;
        let members: Vec<MemberId> = set
            .iter()
            .enumerate()
            .map(|(volume, bytes)| {
                run.announce(
                    &format!("{:032x}", 0x5eed_0000 + volume as u64),
                    bytes.len(),
                )
            })
            .collect();
        crate::engine::counts::take();
        for (member, bytes) in members.iter().zip(&set) {
            run.deliver(*member, 0, bytes);
            run.close(*member);
        }
        let counted = crate::engine::counts::take();
        let places = run.places(&members);
        let engine = places[0].map(|(node, _)| node);
        assert_eq!(
            engine.and_then(|node| run.tree.kind(node)),
            Some(Kind::RarEngine)
        );
        for (volume, place) in places.iter().enumerate() {
            assert_eq!(
                *place,
                engine.map(|node| (node, VolumeIndex(volume as u32))),
                "volume {volume}"
            );
        }
        assert!(
            counted.walked <= 4 * volumes,
            "{volumes} volumes: {counted:?}"
        );
    }
}

// A long stored RAR4 set, its end records stating no number, posted in
// order under names that number nothing. The store places each volume by
// its headers' links alone (`RarStore::claim_by_headers` asks
// `claim::place` once per claim): the places it reads of those it filed
// beside the volumes held stay bounded, however long the set.
//
// Measured on 2026-11-18 before the store filed them: 4,950 held indices
// walked for 100 volumes, 44,850 for 300, once over every held volume per
// volume placed.
#[test]
fn a_long_nameless_stored_set_placed_by_headers_walks_its_volumes_a_bounded_number_of_times() {
    for volumes in [100, 300] {
        let set = long_set(volumes);
        let mut run = Run::new();
        run.enough = 4 * volumes;
        let members: Vec<MemberId> = set
            .iter()
            .enumerate()
            .map(|(volume, bytes)| {
                run.announce(
                    &format!("{:032x}", 0x5707_0000 + volume as u64),
                    bytes.len(),
                )
            })
            .collect();
        counts::take();
        for (member, bytes) in members.iter().zip(&set) {
            run.deliver(*member, 0, bytes);
            run.close(*member);
        }
        let counted = counts::take();
        let places = run.places(&members);
        let store = places[0].map(|(node, _)| node);
        assert_eq!(
            store.and_then(|node| run.tree.kind(node)),
            Some(Kind::RarStore)
        );
        for (volume, place) in places.iter().enumerate() {
            assert_eq!(
                *place,
                store.map(|node| (node, VolumeIndex(volume as u32))),
                "volume {volume}"
            );
        }
        assert!(
            counted.beside <= 4 * volumes,
            "{volumes} volumes: {} places read",
            counted.beside
        );
    }
}

// The held volumes with the places filed beside them, as an owner keeps
// them, counting each place a placement reads.
struct Filed<'a> {
    held: std::collections::BTreeMap<u32, claim::Held<'a>>,
    beside: crate::beside::Beside,
    read: std::cell::Cell<usize>,
}

impl<'a> Filed<'a> {
    fn new() -> Self {
        Self {
            held: std::collections::BTreeMap::new(),
            beside: crate::beside::Beside::default(),
            read: std::cell::Cell::new(0),
        }
    }

    fn hold(&mut self, at: u32, facts: &'a unrar_rs::RarVolumeFacts) {
        let held = claim::Held {
            facts: Some(facts),
            successor: None,
        };
        self.held.insert(at, held);
        self.beside.file(at, true);
    }

    fn release(&mut self, at: u32) {
        self.held.remove(&at);
        self.beside.file(at, false);
    }
}

impl claim::Holds for Filed<'_> {
    fn at(&self, index: u32) -> Option<claim::Held<'_>> {
        self.held.get(&index).copied()
    }
}

impl claim::Places for Filed<'_> {
    fn beside(&self) -> impl Iterator<Item = u32> + '_ {
        self.beside
            .free()
            .inspect(|_| self.read.set(self.read.get() + 1))
    }

    fn held(&self) -> impl Iterator<Item = u32> + '_ {
        self.beside
            .held()
            .inspect(|_| self.read.set(self.read.get() + 1))
    }
}

// What a volume's front states, as the store reads it.
fn facts(volume: &[u8]) -> unrar_rs::RarVolumeFacts {
    claim::front_facts(
        front(volume),
        None,
        std::sync::Arc::new(unrar_rs::KdfCache::new()),
    )
    .expect("a volume's front")
}

// Each volume of `fronts` arrives in `order` and is placed both by the
// places filed beside the held ones and by a walk over every held volume, as
// `place` read them before; held where the two place it, the first at the
// first place, as a store founded on it holds it. Then each volume held in
// `released` goes and comes back. The two agree on every placement: where
// each one ends.
fn placed_both_ways(
    fronts: &[unrar_rs::RarVolumeFacts],
    order: &[usize],
    released: &[usize],
) -> Vec<Option<u32>> {
    let mut walked = std::collections::BTreeMap::new();
    let mut filed = Filed::new();
    let mut at = vec![None; fronts.len()];
    for &volume in order {
        at[volume] = place_both_ways(&fronts[volume], &mut walked, &mut filed);
    }
    for &volume in released {
        let Some(held) = at[volume] else {
            continue;
        };
        walked.remove(&held);
        filed.release(held);
        at[volume] = place_both_ways(&fronts[volume], &mut walked, &mut filed);
    }
    at
}

// One volume placed both ways, held where they place it.
fn place_both_ways<'a>(
    facts: &'a unrar_rs::RarVolumeFacts,
    walked: &mut std::collections::BTreeMap<u32, claim::Held<'a>>,
    filed: &mut Filed<'a>,
) -> Option<u32> {
    let old = claim::place(facts, &*walked);
    let new = claim::place(facts, &*filed);
    assert_eq!(new, old, "held {:?}", walked.keys().collect::<Vec<_>>());
    let placed = match walked.is_empty() {
        true => Some(VolumeIndex(0)),
        false => new,
    };
    let placed = placed.filter(|placed| !walked.contains_key(&placed.0))?;
    walked.insert(
        placed.0,
        claim::Held {
            facts: Some(facts),
            successor: None,
        },
    );
    filed.hold(placed.0, facts);
    Some(placed.0)
}

// The places filed beside the held volumes place every volume where the
// walk over all of them placed it: a set in order, reversed, shuffled, two
// sets under one stem interleaved, and a set with volumes missing, each
// with volumes given back and claimed again.
#[test]
fn placing_by_the_places_filed_beside_held_volumes_matches_the_walk_over_them() {
    let one: Vec<_> = long_set(12).iter().map(|volume| facts(volume)).collect();
    let mut options = super::split(Version::Rar4);
    options.split = Some(5_000);
    let other: Vec<_> = write(
        &[Item::File("track.flac", payload(4, 8 * 5_000 - 1_100))],
        &options,
    )
    .iter()
    .map(|volume| facts(volume))
    .collect();
    assert_eq!(other.len(), 8);
    let both: Vec<_> = one.iter().chain(&other).cloned().collect();
    let in_order: Vec<usize> = (0..12).collect();
    let reversed: Vec<usize> = (0..12).rev().collect();
    let shuffled = vec![5, 0, 11, 3, 1, 8, 2, 10, 4, 9, 6, 7];
    let interleaved: Vec<usize> = (0..12)
        .flat_map(|volume| [Some(volume), (volume < 8).then_some(12 + volume)])
        .flatten()
        .collect();
    let gaps: Vec<usize> = (0..12)
        .filter(|volume| ![3, 7, 8].contains(volume))
        .collect();
    let cases: [(&[_], &[usize], &[usize]); 6] = [
        (&one, &in_order, &[11, 5, 0]),
        (&one, &reversed, &[11, 10]),
        (&one, &shuffled, &[2, 3, 11]),
        (&both, &interleaved, &[13, 4, 19]),
        (&both, &(12..20).chain(0..12).collect::<Vec<_>>(), &[0, 12]),
        (&one, &gaps, &[4, 9, 0]),
    ];
    for (fronts, order, released) in cases {
        let at = placed_both_ways(fronts, order, released);
        if std::ptr::eq(order, in_order.as_slice()) {
            assert_eq!(at, (0..12).map(Some).collect::<Vec<_>>());
        }
    }
}

// Every order of `0..n`, each once.
fn orders(n: usize) -> Vec<Vec<usize>> {
    if n == 0 {
        return vec![Vec::new()];
    }
    let mut out = Vec::new();
    for order in orders(n - 1) {
        for at in 0..=order.len() {
            let mut next = order.clone();
            next.insert(at, n - 1);
            out.push(next);
        }
    }
    out
}

// A stored four-volume RAR4 set whose end records state no number, posted
// under names that number nothing, each volume whole as it comes, in every
// one of the 24 orders: the links place each volume, a set's last volume
// that came before the ones it follows makes way for each of them, and
// every order ends with each volume of one store at its true place. A
// volume moves a bounded number of times: the run hands over no more than
// four times per volume. Two members, so each volume's headers link it to
// its neighbours alone: one ends in the second volume, the other starts
// there and ends in the last.
#[test]
fn a_nameless_set_offered_in_every_order_ends_at_its_true_places() {
    let mut options = super::split(Version::Rar4);
    options.split = Some(5_000);
    let set = write(
        &[
            Item::File("lantern.bin", payload(3, 7_000)),
            Item::File("beacon.bin", payload(5, 11_500)),
        ],
        &options,
    );
    assert_eq!(set.len(), 4);
    let orders = orders(set.len());
    assert_eq!(orders.len(), 24);
    for order in orders {
        let mut run = Run::new();
        run.enough = 4 * set.len();
        let members: Vec<MemberId> = (0..set.len())
            .map(|volume| {
                run.announce(
                    &format!("{:032x}", 0x0b5c_0000 + volume as u64),
                    set[volume].len(),
                )
            })
            .collect();
        for volume in &order {
            run.deliver(members[*volume], 0, &set[*volume]);
            run.close(members[*volume]);
        }
        let case = format!("order {order:?}");
        assert!(run.handed < run.enough, "{case}: never settled");
        at_true_places(&run.places(&members), &case);
    }
}
