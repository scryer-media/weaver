// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Where a store places a volume and where it holds one misplaced are judged
// on the same facts, so a volume it gives back is never claimed again where
// it would be given back once more. Each post is run in a tree whose owner
// hands every bound volume over as the server does, and the hand-overs are
// counted: a placement that undid itself would hand the same volume over
// without end.

use bytes::Bytes;
use weaver_graph::{Effect, MemberId, NodeId, Posted, Provenance, Tree, VolumeIndex, Why};

use super::super::counts;
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

// A long RAR4 set whose end records state no number, its last two volumes
// posted under each other's numbers: the layout refutes the one held where
// it is not once, and lays the set again once, however many volumes it
// holds and however its bytes come in after. Its rest comes in small pieces
// with no new header among them, as articles do. Renamed to their true
// names, the two take their places, in one store.
#[test]
fn a_long_set_refuting_a_volume_lays_it_again_once() {
    const VOLUMES: usize = 300;
    let mut options = super::split(Version::Rar4);
    options.split = Some(5_000);
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
    counts::take();
    for volume in &posted {
        run.deliver(members[*volume], 0, front(&set[*volume]));
    }
    for volume in &posted {
        let bytes = &set[*volume];
        let mut at = front(bytes).len();
        while at < bytes.len() {
            let to = (at + 256).min(bytes.len());
            run.deliver(members[*volume], at, &bytes[at..to]);
            at = to;
        }
        run.close(members[*volume]);
    }
    let counted = counts::take();
    // Each volume is laid on what its front says, again on what its end
    // says, and once more in the one layout laid again short of the refuted
    // volume: three times, not once for every piece that came in after.
    assert_eq!(counted.cut, 1, "{counted:?}");
    assert!(
        counted.asked.len() <= 3 * VOLUMES,
        "{}",
        counted.asked.len()
    );
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
    let store = run
        .tree
        .child_of(NodeId::ROOT, members[0])
        .map(|(node, _)| node);
    assert!(store.is_some());
    for (volume, member) in members.iter().enumerate() {
        assert_eq!(
            run.tree.child_of(NodeId::ROOT, *member),
            store.map(|node| (node, VolumeIndex(volume as u32))),
            "volume {volume}"
        );
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
