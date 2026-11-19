// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A nameless RAR4 set handed to the engine with fronts that stop short of
// its volumes' end records: the engine bids for the tails that state where
// its links alone hold a volume, as far ahead as the store bids, and moves a
// volume its tail says is elsewhere.

use std::collections::BTreeMap;

use weaver_graph::{Delivery, Evidenced, Provenance, Pull, Seek};

use super::*;
use crate::rar_store::WINDOW;
use crate::rar_store::tests::writer::{Item, Options, Version, write};

// What a store hands on of a volume's front: never past its first 4 KiB.
const FRONT: usize = 4096;

// One file stored across `volumes` volumes of 5,000 bytes of data each,
// the last a little short, but still longer than a front,
// every end record stating its number where `numbered`.
fn set(version: Version, volumes: usize, numbered: bool) -> Vec<Vec<u8>> {
    let mut options = Options::new(version);
    options.split = Some(5_000);
    options.numbered = numbered;
    let length = 5_000 * volumes - 200;
    let set = write(
        &[Item::File("long.bin", crate::testing::payload(9, length))],
        &options,
    );
    assert_eq!(set.len(), volumes);
    set
}

// A name that numbers nothing.
fn hashed(volume: usize) -> String {
    format!("{:032x}", 0x0b5c_0000 + volume)
}

// The engine with `order[place]` bound at each place, as a store hands
// on volumes its links alone placed, each with its front and its length.
fn handed(set: &[Vec<u8>], order: &[usize]) -> RarEngine {
    let mut engine = RarEngine::new();
    for (place, &volume) in order.iter().enumerate() {
        let bytes = &set[volume];
        engine.bound(
            VolumeIndex(place as u32),
            &hashed(volume),
            &bytes[..bytes.len().min(FRONT)],
            Some(bytes.len() as u64),
        );
    }
    engine
}

fn wanted(engine: &RarEngine) -> Vec<Seek> {
    let mut seeks = Vec::new();
    engine.wants(&mut seeks);
    seeks
}

// The bytes of `range` of the volume at `place`, delivered clean, with the
// bytes already kept read back from `kept`.
fn deliver(
    engine: &mut RarEngine,
    place: u32,
    bytes: &[u8],
    range: ByteRange,
    provenance: Provenance,
    kept: &BTreeMap<u32, Vec<u8>>,
) -> Vec<Emit> {
    let mut out = Vec::new();
    let mut read = |placement: &Placement, into: &mut Vec<u8>| match placement {
        Placement::Envelope { envelope, range } => kept.get(envelope).is_some_and(|kept| {
            let (start, end) = (range.start as usize, range.end as usize);
            end <= kept.len() && {
                into.extend_from_slice(&kept[start..end]);
                true
            }
        }),
        _ => false,
    };
    engine.deliver(
        Delivery {
            volume: VolumeIndex(place),
            offset: range.start,
            bytes: &Evidenced::from(bytes[range.start as usize..range.end as usize].to_vec()),
            provenance,
        },
        &mut read,
        &mut out,
    );
    out
}

fn last(bytes: &[u8], len: u64) -> ByteRange {
    let length = bytes.len() as u64;
    ByteRange::new(length - len, length)
}

// A 100-volume set: the first volume's front states its place, so only the
// other 99 wait for their tails, and the engine bids for the first eight of
// those at a time, each over its last bytes only, never the whole set.
// Each tail read lets the bid move on: 99 tail seeks in all, at most eight
// at once.
#[test]
fn a_nameless_rar4_set_bids_for_the_tails_ahead_only() {
    let set = set(Version::Rar4, 100, true);
    let order: Vec<usize> = (0..set.len()).collect();
    let mut engine = handed(&set, &order);
    let kept = BTreeMap::new();
    let mut seen = std::collections::BTreeSet::new();
    let mut most = 0;
    loop {
        let seeks = wanted(&engine);
        if seeks.is_empty() {
            break;
        }
        most = most.max(seeks.len());
        for seek in &seeks {
            let bytes = &set[seek.volume.0 as usize];
            assert_eq!(seek.range.end, bytes.len() as u64, "{seek:?}");
            assert!(seek.range.len() <= 20, "{seek:?}");
            assert!(seek.range.start as usize >= FRONT, "{seek:?}");
            seen.insert(seek.volume.0);
        }
        let first = seeks[0].volume.0;
        let bytes = &set[first as usize];
        let out = deliver(
            &mut engine,
            first,
            bytes,
            last(bytes, 64),
            Provenance::Fresh,
            &kept,
        );
        assert!(out.is_empty(), "volume {first} is where its tail says");
    }
    assert_eq!(most, WINDOW as usize);
    assert_eq!(seen, (1..100).collect());
}

// A RAR5 volume states its number in its main header, and a volume whose
// front reached its end record states its place there: neither is bid for.
// Nor is a volume its posted name numbers.
#[test]
fn no_tail_is_bid_for_where_the_front_or_name_places_the_volume() {
    let rar5 = set(Version::Rar5, 4, false);
    assert!(wanted(&handed(&rar5, &[0, 1, 2, 3])).is_empty(), "RAR5");

    let rar4 = set(Version::Rar4, 4, true);
    let mut whole = RarEngine::new();
    for (place, bytes) in rar4.iter().enumerate() {
        whole.bound(
            VolumeIndex(place as u32),
            &hashed(place),
            bytes,
            Some(bytes.len() as u64),
        );
    }
    assert!(
        wanted(&whole).is_empty(),
        "fronts that reach the end records"
    );

    let mut named = RarEngine::new();
    for (place, bytes) in rar4.iter().enumerate() {
        named.bound(
            VolumeIndex(place as u32),
            &format!("long.part{}.rar", place + 1),
            &bytes[..FRONT],
            Some(bytes.len() as u64),
        );
    }
    assert!(wanted(&named).is_empty(), "names that number the volumes");

    let mut lengthless = RarEngine::new();
    for (place, bytes) in rar4.iter().enumerate() {
        lengthless.bound(
            VolumeIndex(place as u32),
            &hashed(place),
            &bytes[..FRONT],
            None,
        );
    }
    assert!(
        wanted(&lengthless).is_empty(),
        "no length says where the tail is"
    );
}

// Middle volumes their links placed in the order they came: the tail of the
// one at place 1 states 2, and it goes back to be claimed there; the one at
// place 2 is then claimed at 1 by its tail, stated in its front.
#[test]
fn a_volume_its_tail_says_is_elsewhere_goes_back() {
    let set = set(Version::Rar4, 4, true);
    let mut engine = handed(&set, &[0, 2, 1, 3]);
    let kept = BTreeMap::new();
    let out = deliver(
        &mut engine,
        1,
        &set[2],
        last(&set[2], 64),
        Provenance::Fresh,
        &kept,
    );
    assert_eq!(out, vec![Emit::GiveBack(VolumeIndex(1))]);
    let carried = engine.give_back(VolumeIndex(1)).unwrap();
    assert_eq!(
        engine.reclaim(
            &hashed(2),
            &set[2][..FRONT],
            Some(set[2].len() as u64),
            &*carried
        ),
        vec![VolumeIndex(2)],
        "its tail's number is in what it carries"
    );
    assert!(
        engine.provisional(VolumeIndex(2), &hashed(2), &set[2][..FRONT], Some(&carried)),
        "the volume its links hold there makes way"
    );
    let out = deliver(
        &mut engine,
        3,
        &set[3],
        last(&set[3], 64),
        Provenance::Fresh,
        &kept,
    );
    assert!(out.is_empty(), "the last volume is where its tail says");
}

// A tail split across two deliveries is read when the second comes, the
// first read back from where it went; damaged bytes are not read at all.
#[test]
fn a_tail_is_read_whole_across_deliveries_and_never_from_damage() {
    let set = set(Version::Rar4, 4, true);
    let mut engine = handed(&set, &[0, 2, 1, 3]);
    let tail = wanted(&engine)[0].range;
    let split = tail.start + 5;
    let mut kept = BTreeMap::new();
    let out = deliver(
        &mut engine,
        1,
        &set[2],
        ByteRange::new(tail.start, set[2].len() as u64),
        Provenance::Damaged,
        &kept,
    );
    assert!(out.is_empty(), "damaged bytes state nothing");
    let out = deliver(
        &mut engine,
        1,
        &set[2],
        ByteRange::new(split, tail.end),
        Provenance::Fresh,
        &kept,
    );
    assert!(out.is_empty(), "the front of the tail is not in yet");
    kept.insert(1, set[2].clone());
    let out = deliver(
        &mut engine,
        1,
        &set[2],
        ByteRange::new(split, tail.end),
        Provenance::Fresh,
        &kept,
    );
    assert_eq!(out, vec![Emit::GiveBack(VolumeIndex(1))]);
}

// Fallbacks. A tail that states no number, and one nothing will bring or
// that comes only in the normal order, leave the volume where its links
// hold it, and nothing more is bid for it.
#[test]
fn a_tail_that_states_nothing_leaves_the_volume_on_its_links() {
    let unnumbered = set(Version::Rar4, 4, false);
    let mut engine = handed(&unnumbered, &[0, 2, 1, 3]);
    let kept = BTreeMap::new();
    for (place, volume) in [(1, 2), (2, 1), (3, 3)] {
        let bytes = &unnumbered[volume];
        let out = deliver(
            &mut engine,
            place,
            bytes,
            last(bytes, 64),
            Provenance::Fresh,
            &kept,
        );
        assert!(out.is_empty(), "place {place}");
    }
    assert!(wanted(&engine).is_empty());

    let set = set(Version::Rar4, 4, true);
    for pull in [Pull::Lost, Pull::Unseekable] {
        let mut engine = handed(&set, &[0, 2, 1, 3]);
        for seek in wanted(&engine) {
            engine.pulled(seek, pull, &mut Vec::new());
        }
        assert!(wanted(&engine).is_empty(), "{pull:?}");
    }
    let mut engine = handed(&set, &[0, 2, 1, 3]);
    for seek in wanted(&engine) {
        engine.pulled(seek, Pull::Moved(1), &mut Vec::new());
    }
    assert_eq!(
        wanted(&engine).len(),
        3,
        "a tail brought forward is still awaited"
    );

    // A volume closed with its tail unread, as a restore closes one again,
    // has had every byte it will.
    for place in 1..4 {
        engine.volume_closed(VolumeIndex(place), &mut Vec::new());
    }
    assert!(wanted(&engine).is_empty(), "closed");
}
