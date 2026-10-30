// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_graph::{
    Budget, ByteRange, Delivery, Emit, Extent, Kind, Located, MemberId, Object, ObjectId,
    Placement, Provenance, Seek, VolumeIndex,
};

const MEMBER: MemberId = MemberId(0);

// The object a split set's piece founds, as the graph builds it, with the
// pieces bound by name in the order given.
fn set(names: &[&str]) -> Box<dyn Object> {
    let mut object = crate::build(Kind::Split, ObjectId(0), Budget::default()).unwrap();
    for name in names {
        let places = object.claim(name, &[], None);
        let [volume] = places[..] else {
            panic!("{name} claimed at {places:?}");
        };
        object.bound(volume, name, &[], None);
    }
    object
}

fn deliver(object: &mut dyn Object, volume: u32, offset: u64, bytes: &[u8]) -> Vec<Emit> {
    let mut out = Vec::new();
    object.deliver(
        Delivery {
            volume: VolumeIndex(volume),
            offset,
            bytes: &weaver_graph::Evidenced::from(bytes.to_vec()),
            provenance: Provenance::Fresh,
        },
        &mut |_, _| false,
        &mut out,
    );
    out
}

fn sized(object: &mut dyn Object, volume: u32, length: u64) -> Vec<Emit> {
    let mut out = Vec::new();
    object.sized(VolumeIndex(volume), length, &mut out);
    out
}

fn closed(object: &mut dyn Object, volume: u32, length: u64) -> Vec<Emit> {
    let mut out = sized(object, volume, length);
    object.volume_closed(VolumeIndex(volume), &mut out);
    out
}

fn locate(object: &dyn Object, volume: u32, range: ByteRange) -> Vec<Placement> {
    let mut out = Vec::new();
    object.locate(VolumeIndex(volume), range, &mut out);
    out
}

fn placed(emits: &[Emit]) -> Vec<(u64, usize)> {
    emits
        .iter()
        .filter_map(|emit| match emit {
            Emit::Bytes { offset, bytes, .. } => Some((*offset, bytes.len())),
            _ => None,
        })
        .collect()
}

fn lengths(emits: &[Emit]) -> Vec<Option<u64>> {
    emits
        .iter()
        .filter_map(|emit| match emit {
            Emit::Member { length, .. } => Some(*length),
            _ => None,
        })
        .collect()
}

// A child that wants the tail of the joined file cannot be told where it
// lies while a piece before it has not declared its length: the store says
// so, and wants one article of every piece that has not, which declares it.
// Once every piece has, the tail lies in the last piece.
#[test]
fn a_tail_seek_through_an_undeclared_split_wants_one_article_per_piece() {
    let mut object = set(&["vessel.bin.003", "vessel.bin.001", "vessel.bin.002"]);
    let mut wants = Vec::new();
    object.wants(&mut wants);
    let mut asked: Vec<(u32, ByteRange)> = wants
        .iter()
        .map(|seek: &Seek| (seek.volume.0, seek.range))
        .collect();
    asked.sort_by_key(|(volume, _)| *volume);
    assert_eq!(
        asked,
        vec![
            (0, ByteRange::at(0, 1)),
            (1, ByteRange::at(0, 1)),
            (2, ByteRange::at(0, 1)),
        ]
    );
    let tail = ByteRange::new(2_400, 2_500);
    let mut extents = Vec::new();
    assert_eq!(object.extent(MEMBER, tail, &mut extents), Located::NotYet);
    assert!(extents.is_empty());
    for volume in 0..3 {
        sized(&mut *object, volume, 1_000);
    }
    let mut wants = Vec::new();
    object.wants(&mut wants);
    assert!(wants.is_empty(), "{wants:?}");
    assert_eq!(object.extent(MEMBER, tail, &mut extents), Located::Whole);
    assert_eq!(
        extents,
        vec![Extent {
            volume: VolumeIndex(2),
            range: ByteRange::new(400, 500),
        }]
    );
}

// A later piece's bytes wait behind an earlier piece that has not declared
// its length, and go to their place once it has.
#[test]
fn a_later_piece_is_held_until_every_piece_before_it_declared() {
    let mut object = set(&["vessel.bin.001", "vessel.bin.002", "vessel.bin.003"]);
    sized(&mut *object, 2, 50);
    let out = deliver(&mut *object, 2, 0, &[7; 40]);
    assert!(placed(&out).is_empty());
    assert_eq!(
        locate(&*object, 2, ByteRange::at(0, 40)),
        vec![Placement::Held {
            range: ByteRange::at(0, 40)
        }]
    );
    sized(&mut *object, 0, 100);
    sized(&mut *object, 1, 100);
    assert_eq!(
        locate(&*object, 2, ByteRange::at(0, 40)),
        vec![Placement::Member {
            member: MEMBER,
            range: ByteRange::at(200, 40),
        }]
    );
    let out = deliver(&mut *object, 2, 0, &[7; 40]);
    assert_eq!(placed(&out), vec![(200, 40)]);
}

// A piece that reaches past the length it declared holds what lies past
// it; at its close every later piece placed so far is asked for again,
// highest first, read back where it was placed, and placed after the
// longer piece; the member is said at its new length, and whole only once
// every moved piece came again.
#[test]
fn a_piece_that_closes_longer_than_it_declared_moves_every_later_piece() {
    let mut object = set(&["vessel.bin.001", "vessel.bin.002", "vessel.bin.003"]);
    sized(&mut *object, 0, 100);
    sized(&mut *object, 1, 100);
    sized(&mut *object, 2, 100);
    assert_eq!(
        placed(&deliver(&mut *object, 0, 0, &[1; 100])),
        vec![(0, 100)]
    );
    assert_eq!(
        placed(&deliver(&mut *object, 2, 0, &[3; 100])),
        vec![(200, 100)]
    );
    assert_eq!(placed(&closed(&mut *object, 2, 100)), vec![]);
    assert_eq!(placed(&closed(&mut *object, 0, 100)), vec![]);
    assert_eq!(
        placed(&deliver(&mut *object, 1, 0, &[2; 100])),
        vec![(100, 100)]
    );
    assert!(placed(&deliver(&mut *object, 1, 100, &[2; 20])).is_empty());
    assert_eq!(
        locate(&*object, 1, ByteRange::at(90, 30)),
        vec![
            Placement::Member {
                member: MEMBER,
                range: ByteRange::at(190, 10),
            },
            Placement::Held {
                range: ByteRange::at(100, 20)
            },
        ]
    );
    let out = closed(&mut *object, 1, 100);
    assert_eq!(
        out.iter()
            .filter(|emit| matches!(emit, Emit::Again { .. }))
            .collect::<Vec<_>>(),
        vec![&Emit::Again {
            volume: VolumeIndex(2),
            range: ByteRange::new(0, 100),
        }]
    );
    assert_eq!(lengths(&out), vec![Some(320)]);
    assert!(!out.contains(&Emit::MemberClosed(MEMBER)));
    // Read back where it was placed.
    assert_eq!(
        locate(&*object, 2, ByteRange::at(0, 100)),
        vec![Placement::Member {
            member: MEMBER,
            range: ByteRange::at(200, 100),
        }]
    );
    // The held tail of the longer piece is placed now.
    assert_eq!(
        locate(&*object, 1, ByteRange::at(100, 20)),
        vec![Placement::Member {
            member: MEMBER,
            range: ByteRange::at(200, 20),
        }]
    );
    let out = deliver(&mut *object, 2, 0, &[3; 100]);
    assert_eq!(placed(&out), vec![(220, 100)]);
    assert!(!out.contains(&Emit::MemberClosed(MEMBER)));
    let out = deliver(&mut *object, 1, 100, &[2; 20]);
    assert_eq!(placed(&out), vec![(200, 20)]);
    assert!(out.contains(&Emit::MemberClosed(MEMBER)));
}

// A restart between the close that moved a later piece and the last of its
// bytes coming again: the restored set still reads them back where they
// sat, places them where they go now, and closes the member only once every
// moved byte came.
#[test]
fn a_set_restored_mid_move_reads_back_where_it_was_and_closes_once_whole() {
    let mut object = set(&["vessel.bin.001", "vessel.bin.002", "vessel.bin.003"]);
    for volume in 0..3 {
        sized(&mut *object, volume, 100);
    }
    deliver(&mut *object, 0, 0, &[1; 100]);
    deliver(&mut *object, 2, 0, &[3; 100]);
    closed(&mut *object, 2, 100);
    closed(&mut *object, 0, 100);
    deliver(&mut *object, 1, 0, &[2; 100]);
    deliver(&mut *object, 1, 100, &[2; 20]);
    let out = closed(&mut *object, 1, 100);
    assert!(out.iter().any(|emit| matches!(emit, Emit::Again { .. })));
    let mut object = crate::restore(&object.facts(), Budget::default()).unwrap();
    assert_eq!(
        locate(&*object, 2, ByteRange::at(0, 100)),
        vec![Placement::Member {
            member: MEMBER,
            range: ByteRange::at(200, 100),
        }]
    );
    let out = deliver(&mut *object, 2, 0, &[3; 100]);
    assert_eq!(placed(&out), vec![(220, 100)]);
    assert!(!out.contains(&Emit::MemberClosed(MEMBER)));
    let out = deliver(&mut *object, 1, 100, &[2; 20]);
    assert_eq!(placed(&out), vec![(200, 20)]);
    assert!(out.contains(&Emit::MemberClosed(MEMBER)));
}

// A set is counted from its first piece, and a piece of another set is not
// claimed.
#[test]
fn a_piece_is_placed_by_its_number_in_its_own_set() {
    let mut object = set(&["vessel.bin.002"]);
    assert_eq!(
        object.claim("VESSEL.BIN.005", &[], None),
        vec![VolumeIndex(4)]
    );
    assert_eq!(
        object.claim("vessel.bin.001", &[], None),
        vec![VolumeIndex(0)]
    );
    assert!(object.claim("other.bin.001", &[], None).is_empty());
    assert!(object.claim("vessel.bin", &[], None).is_empty());
}

// A set missing a number is no file: what was placed of it is forgotten,
// and its pieces go back.
#[test]
fn a_set_missing_a_number_goes_back_at_quiet() {
    struct Stood;
    impl weaver_graph::Standing for Stood {
        fn damaged(&self) -> bool {
            false
        }
        fn unplaced(&self) -> bool {
            true
        }
        fn lost(&self) -> bool {
            false
        }
        fn asking(&self) -> bool {
            false
        }
        fn beside(&self) -> Vec<weaver_graph::Beside<'_>> {
            Vec::new()
        }
    }
    let mut object = set(&["vessel.bin.001", "vessel.bin.002", "vessel.bin.004"]);
    let mut out = Vec::new();
    object.quiet(&Stood, &mut out);
    assert_eq!(
        out,
        vec![
            Emit::Retract(MEMBER),
            Emit::Refuse(weaver_graph::Why::Unrepairable)
        ]
    );
}

#[test]
fn its_facts_restore_the_set_as_it_stood() {
    let mut object = set(&["vessel.bin.001", "vessel.bin.002"]);
    sized(&mut *object, 0, 100);
    deliver(&mut *object, 0, 0, &[1; 60]);
    let restored = crate::restore(&object.facts(), Budget::default()).unwrap();
    assert_eq!(
        locate(&*restored, 1, ByteRange::at(0, 10)),
        vec![Placement::Member {
            member: MEMBER,
            range: ByteRange::at(100, 10),
        }]
    );
}
