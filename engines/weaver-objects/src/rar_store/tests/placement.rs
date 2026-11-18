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
// its end, so its headers have not yet said whether it is one of a set.
// Its name places it second, and nothing it has stated says otherwise: it is
// held there, read to its close, and never handed back and forth.
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
    assert_eq!(run.at(second), Some(VolumeIndex(1)));
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
    let set = long_set(volumes);
    for whole in [false, true] {
        let case = format!("{volumes} whole {whole}");
        let posted = posted_unrenamed(&set, numbers, whole);
        at_true_places(&posted.places, &case);
        for (volume, given) in posted.given.iter().enumerate() {
            let moves = usize::from(moving.contains(&volume));
            assert_eq!(*given, moves, "{case} volume {volume}");
        }
        let counted = &posted.counted;
        // Delivered whole, the misplaced volume is laid at its posted place
        // before the set is seen out of order: the layout refutes that place
        // once. In pieces every front is in first, and nothing is refuted.
        assert_eq!(counted.cut, usize::from(whole), "{case}");
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
