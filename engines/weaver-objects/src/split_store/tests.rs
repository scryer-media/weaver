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
    deliver_as(object, volume, offset, bytes, Provenance::Fresh)
}

fn deliver_as(
    object: &mut dyn Object,
    volume: u32,
    offset: u64,
    bytes: &[u8],
    provenance: Provenance,
) -> Vec<Emit> {
    let mut out = Vec::new();
    object.deliver(
        Delivery {
            volume: VolumeIndex(volume),
            offset,
            bytes: &weaver_graph::Evidenced::from(bytes.to_vec()),
            provenance,
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
    // The held tail of the longer piece waits: its place still holds the
    // moved piece's bytes, which read back from there.
    assert_eq!(
        locate(&*object, 1, ByteRange::at(100, 20)),
        vec![Placement::Held {
            range: ByteRange::at(100, 20)
        }]
    );
    let out = deliver(&mut *object, 2, 0, &[3; 100]);
    assert_eq!(placed(&out), vec![(220, 100)]);
    assert!(!out.contains(&Emit::MemberClosed(MEMBER)));
    // Once they came again, the tail is placed.
    assert_eq!(
        locate(&*object, 1, ByteRange::at(100, 20)),
        vec![Placement::Member {
            member: MEMBER,
            range: ByteRange::at(200, 20),
        }]
    );
    let out = deliver(&mut *object, 1, 100, &[2; 20]);
    assert_eq!(placed(&out), vec![(200, 20)]);
    assert!(out.contains(&Emit::MemberClosed(MEMBER)));
}

// A piece posted short closed at what it reached, and the member closed
// with it; a recovery set then proves the piece longer and rebuilds its
// tail. The proven length moves every later piece, closed or not, and the
// rebuilt tail waits for the place the next piece still sits in.
#[test]
fn a_length_proven_after_a_piece_closed_moves_every_later_piece() {
    let mut object = set(&["vessel.bin.001", "vessel.bin.002"]);
    sized(&mut *object, 0, 100);
    sized(&mut *object, 1, 100);
    deliver(&mut *object, 0, 0, &[1; 100]);
    deliver(&mut *object, 1, 0, &[2; 100]);
    closed(&mut *object, 0, 100);
    let out = closed(&mut *object, 1, 100);
    assert!(out.contains(&Emit::MemberClosed(MEMBER)));
    let out = sized(&mut *object, 0, 120);
    assert_eq!(
        out.iter()
            .filter(|emit| matches!(emit, Emit::Again { .. }))
            .collect::<Vec<_>>(),
        vec![&Emit::Again {
            volume: VolumeIndex(1),
            range: ByteRange::new(0, 100),
        }]
    );
    assert_eq!(lengths(&out), vec![Some(220)]);
    let out = deliver(&mut *object, 0, 100, &[1; 20]);
    assert!(placed(&out).is_empty());
    assert_eq!(
        locate(&*object, 0, ByteRange::at(100, 20)),
        vec![Placement::Held {
            range: ByteRange::at(100, 20)
        }]
    );
    let out = deliver(&mut *object, 1, 0, &[2; 100]);
    assert_eq!(placed(&out), vec![(120, 100)]);
    assert!(!out.contains(&Emit::MemberClosed(MEMBER)));
    let out = deliver(&mut *object, 0, 100, &[1; 20]);
    assert_eq!(placed(&out), vec![(100, 20)]);
    assert!(out.contains(&Emit::MemberClosed(MEMBER)));
}

// A piece that closes short moves the next one back, onto bytes the piece
// after it goes to: the old place of an earlier piece, starting before the
// later one's. The later piece's bytes wait until the moved bytes came
// again, then go where they go.
#[test]
fn a_piece_waits_for_an_earlier_piece_still_sitting_where_it_goes() {
    let mut object = set(&["vessel.bin.001", "vessel.bin.002", "vessel.bin.003"]);
    for volume in 0..3 {
        sized(&mut *object, volume, 100);
    }
    deliver(&mut *object, 1, 0, &[2; 100]);
    deliver(&mut *object, 0, 0, &[1; 80]);
    let out = closed(&mut *object, 0, 100);
    assert!(out.contains(&Emit::Again {
        volume: VolumeIndex(1),
        range: ByteRange::new(0, 100),
    }));
    let out = deliver(&mut *object, 2, 0, &[3; 50]);
    assert!(placed(&out).is_empty(), "{out:?}");
    assert_eq!(
        locate(&*object, 1, ByteRange::at(0, 100)),
        vec![Placement::Member {
            member: MEMBER,
            range: ByteRange::at(100, 100),
        }]
    );
    let out = deliver(&mut *object, 1, 0, &[2; 100]);
    assert_eq!(placed(&out), vec![(80, 100)]);
    assert_eq!(
        locate(&*object, 2, ByteRange::at(0, 50)),
        vec![Placement::Member {
            member: MEMBER,
            range: ByteRange::at(180, 50),
        }]
    );
    let out = deliver(&mut *object, 2, 0, &[3; 50]);
    assert_eq!(placed(&out), vec![(180, 50)]);
}

// The same move, and new bytes of the moved piece itself go onto its own
// old place: they wait, and its placed bytes still read back from there,
// until they came again.
#[test]
fn a_moved_piece_waits_for_its_own_bytes_still_sitting_where_it_goes() {
    let mut object = set(&["vessel.bin.001", "vessel.bin.002", "vessel.bin.003"]);
    for volume in 0..3 {
        sized(&mut *object, volume, 100);
    }
    deliver(&mut *object, 1, 0, &[2; 60]);
    deliver(&mut *object, 0, 0, &[1; 80]);
    let out = closed(&mut *object, 0, 100);
    assert!(out.contains(&Emit::Again {
        volume: VolumeIndex(1),
        range: ByteRange::new(0, 60),
    }));
    let out = deliver(&mut *object, 1, 60, &[2; 40]);
    assert!(placed(&out).is_empty(), "{out:?}");
    assert_eq!(
        locate(&*object, 1, ByteRange::at(0, 60)),
        vec![Placement::Member {
            member: MEMBER,
            range: ByteRange::at(100, 60),
        }]
    );
    let out = deliver(&mut *object, 1, 0, &[2; 60]);
    assert_eq!(placed(&out), vec![(80, 60)]);
    assert_eq!(
        locate(&*object, 1, ByteRange::at(60, 40)),
        vec![Placement::Member {
            member: MEMBER,
            range: ByteRange::at(140, 40),
        }]
    );
}

// Bytes handed again to a piece already closed and placed whole change
// nothing the member's close waits on: no close is said again.
#[test]
fn bytes_again_to_a_whole_piece_say_no_close() {
    let mut object = set(&["vessel.bin.001", "vessel.bin.002"]);
    sized(&mut *object, 0, 100);
    sized(&mut *object, 1, 100);
    deliver(&mut *object, 0, 0, &[1; 100]);
    deliver(&mut *object, 1, 0, &[2; 100]);
    closed(&mut *object, 0, 100);
    let out = closed(&mut *object, 1, 100);
    assert!(out.contains(&Emit::MemberClosed(MEMBER)));
    let out = deliver(&mut *object, 0, 0, &[1; 100]);
    assert_eq!(placed(&out), vec![(0, 100)]);
    assert!(!out.contains(&Emit::MemberClosed(MEMBER)), "{out:?}");
    assert!(lengths(&out).is_empty(), "{out:?}");
}

// The last piece closes longer than it declared while the bytes past its
// declared length are still held; a recovery set then proves the length it
// declared. Every byte it holds within that length is placed, so the
// member is whole then and says so: no later event is needed to close it.
#[test]
fn a_length_proven_down_to_what_a_closed_piece_placed_closes_the_member() {
    let mut object = set(&["vessel.bin.001", "vessel.bin.002"]);
    sized(&mut *object, 0, 100);
    sized(&mut *object, 1, 100);
    deliver(&mut *object, 0, 0, &[1; 100]);
    closed(&mut *object, 0, 100);
    let out = deliver(&mut *object, 1, 0, &[2; 120]);
    assert_eq!(placed(&out), vec![(100, 100)]);
    let out = closed(&mut *object, 1, 100);
    assert!(!out.contains(&Emit::MemberClosed(MEMBER)), "{out:?}");
    let out = sized(&mut *object, 1, 100);
    assert_eq!(lengths(&out), vec![Some(200)]);
    assert!(out.contains(&Emit::MemberClosed(MEMBER)), "{out:?}");
}

// A piece whose tail was held behind a later piece's old place, one
// article and the next, both handed again once that place came back: the
// later article is handed first. The member is not whole while the
// earlier article is still held, and closes once it is placed.
#[test]
fn a_piece_placed_past_an_article_still_held_is_not_whole() {
    let mut object = set(&[
        "vessel.bin.001",
        "vessel.bin.002",
        "vessel.bin.003",
        "vessel.bin.004",
    ]);
    for volume in 0..4 {
        sized(&mut *object, volume, 100);
    }
    deliver(&mut *object, 3, 0, &[4; 100]);
    closed(&mut *object, 3, 100);
    let out = deliver(&mut *object, 0, 0, &[1; 120]);
    assert_eq!(placed(&out), vec![(0, 100)]);
    let out = closed(&mut *object, 0, 100);
    assert!(out.contains(&Emit::Again {
        volume: VolumeIndex(3),
        range: ByteRange::new(0, 100),
    }));
    assert_eq!(
        placed(&deliver(&mut *object, 0, 100, &[1; 20])),
        vec![(100, 20)]
    );
    assert_eq!(
        placed(&deliver(&mut *object, 1, 0, &[2; 100])),
        vec![(120, 100)]
    );
    closed(&mut *object, 1, 100);
    assert_eq!(
        placed(&deliver(&mut *object, 2, 0, &[3; 80])),
        vec![(220, 80)]
    );
    assert!(placed(&deliver(&mut *object, 2, 80, &[3; 10])).is_empty());
    assert!(placed(&deliver(&mut *object, 2, 90, &[3; 10])).is_empty());
    closed(&mut *object, 2, 100);
    let out = deliver(&mut *object, 3, 0, &[4; 100]);
    assert_eq!(placed(&out), vec![(320, 100)]);
    assert!(!out.contains(&Emit::MemberClosed(MEMBER)), "{out:?}");
    let out = deliver(&mut *object, 2, 90, &[3; 10]);
    assert_eq!(placed(&out), vec![(310, 10)]);
    assert!(!out.contains(&Emit::MemberClosed(MEMBER)), "{out:?}");
    let out = deliver(&mut *object, 2, 80, &[3; 10]);
    assert_eq!(placed(&out), vec![(300, 10)]);
    assert!(out.contains(&Emit::MemberClosed(MEMBER)), "{out:?}");
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

const RAR5: &[u8] = b"Rar!\x1a\x07\x01\x00";

// A piece's front: `head`, then plain bytes to `len`.
fn front(head: &[u8], len: usize) -> Vec<u8> {
    let mut bytes = head.to_vec();
    bytes.resize(len, 0x5a);
    bytes
}

// Ruling "Signature refutes name" (2026-10-30): RAR volumes posted under
// numbered names each open with a head, so a piece other than the first
// that signs refutes the set, whichever came first. Every piece is placed
// where the map says, so each goes back as it was posted, and nothing
// failed. The same set, either order.
#[test]
fn a_later_piece_that_signs_refutes_the_set_in_either_order() {
    let first = front(RAR5, 100);
    let second = front(RAR5, 100);

    let mut object = set(&["4f9c2e.001", "4f9c2e.002"]);
    sized(&mut *object, 0, 100);
    sized(&mut *object, 1, 100);
    let out = deliver(&mut *object, 0, 0, &first);
    assert_eq!(placed(&out), vec![(0, 100)]);
    assert!(!out.contains(&Emit::Refuted));
    let out = deliver(&mut *object, 1, 0, &second);
    assert_eq!(placed(&out), vec![(100, 100)]);
    assert!(out.contains(&Emit::Refuted));

    let mut object = set(&["4f9c2e.002", "4f9c2e.001"]);
    sized(&mut *object, 1, 100);
    let out = deliver(&mut *object, 1, 0, &second);
    assert!(out.contains(&Emit::Refuted));
    assert_eq!(
        locate(&*object, 1, ByteRange::at(0, 100)),
        vec![Placement::Held {
            range: ByteRange::at(0, 100)
        }],
        "its place waits on the piece before it, and it is refuted all the same"
    );
}

// A byte-split RAR: only the first piece opens with a head, so nothing
// refutes, and the pieces join into the one file they were cut from.
#[test]
fn a_byte_split_rar_whose_later_piece_is_headless_still_joins() {
    let mut object = set(&["vessel.rar.001", "vessel.rar.002"]);
    sized(&mut *object, 0, 100);
    sized(&mut *object, 1, 100);
    let out = deliver(&mut *object, 1, 0, &front(&[], 100));
    assert_eq!(placed(&out), vec![(100, 100)]);
    let out = deliver(&mut *object, 0, 0, &front(RAR5, 100));
    assert_eq!(placed(&out), vec![(0, 100)]);
    let mut out = closed(&mut *object, 1, 100);
    out.extend(closed(&mut *object, 0, 100));
    assert!(!out.contains(&Emit::Refuted));
    assert!(out.contains(&Emit::MemberClosed(MEMBER)));
}

// A two-piece set whose second piece opens on `head`, the second delivered
// first: whether the set was refuted, and whether its member closed whole.
fn second_piece_opening_on(head: &[u8], len: usize) -> (bool, bool) {
    let mut object = set(&["vessel.bin.001", "vessel.bin.002"]);
    sized(&mut *object, 0, len as u64);
    sized(&mut *object, 1, len as u64);
    let mut out = deliver(&mut *object, 1, 0, &front(head, len));
    out.extend(deliver(&mut *object, 0, 0, &front(&[], len)));
    out.extend(closed(&mut *object, 0, len as u64));
    out.extend(closed(&mut *object, 1, len as u64));
    (
        out.contains(&Emit::Refuted),
        out.contains(&Emit::MemberClosed(MEMBER)),
    )
}

// A tar member header at its own 512-byte boundary.
fn ustar_header() -> Vec<u8> {
    let mut header = vec![0_u8; 512];
    header[..10].copy_from_slice(b"quill.bin\0");
    header[148..156].copy_from_slice(b"0000000\0");
    header[257..263].copy_from_slice(b"ustar\0");
    header
}

// A PAR2 packet header stating a 64-byte packet.
fn par2_packet_header() -> Vec<u8> {
    let mut header = b"PAR2\0PKT".to_vec();
    header.extend_from_slice(&64u64.to_le_bytes());
    header.resize(64, 0);
    header
}

// A 7z signature header whose start header matches its CRC.
fn sevenz_signature_header() -> Vec<u8> {
    let mut header = vec![0x37, 0x7a, 0xbc, 0xaf, 0x27, 0x1c, 0, 4];
    header.resize(32, 0xaa);
    let crc = crate::grid::crc32(&header[12..]);
    header[8..12].copy_from_slice(&crc.to_le_bytes());
    header
}

// Ruling 2026-10-30 5:47 PM MDT: a kind whose signature may stand inside a
// file never refutes, since a later piece of a split file can open on it by
// right: a tar member header, a zip entry, a PAR2 or PAR3 packet, a zstd
// frame, a further gzip, bzip2 or xz stream. Each joins.
#[test]
fn a_later_piece_opening_on_a_signature_that_recurs_inside_a_file_joins() {
    let gzip = [0x1f, 0x8b, 0x08, 0x00, 0, 0, 0, 0, 0x00, 0x03];
    let xz = [0xfd, b'7', b'z', b'X', b'Z', 0x00, 0x00, 0x04];
    let cases: [(&str, Vec<u8>); 10] = [
        ("tar member header", ustar_header()),
        ("zip local file header", b"PK\x03\x04".to_vec()),
        ("zip data descriptor", b"PK\x07\x08".to_vec()),
        ("zip end record", b"PK\x05\x06".to_vec()),
        ("PAR2 packet header", par2_packet_header()),
        ("PAR3 packet header", b"PAR3\0PKT".to_vec()),
        ("zstd frame", vec![0x28, 0xb5, 0x2f, 0xfd]),
        ("gzip member", gzip.to_vec()),
        ("bzip2 stream", b"BZh9".to_vec()),
        ("xz stream", xz.to_vec()),
    ];
    for (what, head) in cases {
        assert_eq!(
            second_piece_opening_on(&head, 1024),
            (false, true),
            "{what}"
        );
    }
}

// A kind whose signature stands only at a file's head refutes: a RAR
// marker of either version, a 7z signature header whose start header
// holds, a RAR5 recovery volume's header.
#[test]
fn a_later_piece_opening_on_a_head_only_signature_refutes() {
    let cases: [(&str, Vec<u8>); 4] = [
        ("RAR5 marker", RAR5.to_vec()),
        ("RAR4 marker", b"Rar!\x1a\x07\x00".to_vec()),
        ("7z signature header", sevenz_signature_header()),
        ("RAR5 recovery header", b"Rar!\x1aRev".to_vec()),
    ];
    for (what, head) in cases {
        assert!(second_piece_opening_on(&head, 1024).0, "{what}");
    }
    // A start header that fails its CRC only opens like a 7z: it joins.
    let mut damaged = sevenz_signature_header();
    damaged[20] ^= 1;
    assert_eq!(second_piece_opening_on(&damaged, 1024), (false, true));
}

// The front is the piece's, not one article's: a later piece whose first
// article is four bytes, then the rest, refutes once the bytes a head
// needs have come, exactly as the same piece delivered whole does.
#[test]
fn a_later_piece_whose_front_comes_in_short_articles_is_judged_as_one_front() {
    let piece = front(&sevenz_signature_header(), 100);

    let mut whole = set(&["vessel.bin.001", "vessel.bin.002"]);
    sized(&mut *whole, 1, 100);
    let whole = deliver(&mut *whole, 1, 0, &piece);
    assert!(whole.contains(&Emit::Refuted));

    let mut cut = set(&["vessel.bin.001", "vessel.bin.002"]);
    sized(&mut *cut, 1, 100);
    let first = deliver(&mut *cut, 1, 0, &piece[..4]);
    assert!(!first.contains(&Emit::Refuted), "four bytes decide nothing");
    let rest = deliver(&mut *cut, 1, 4, &piece[4..]);
    assert_eq!(
        rest.iter().filter(|emit| **emit == Emit::Refuted).count(),
        1,
        "the rest of the front refutes, once"
    );

    // A restart between the two keeps the four bytes.
    let mut cut = set(&["vessel.bin.001", "vessel.bin.002"]);
    sized(&mut *cut, 1, 100);
    deliver(&mut *cut, 1, 0, &piece[..4]);
    let mut cut = crate::restore(&cut.facts(), Budget::default()).unwrap();
    assert!(deliver(&mut *cut, 1, 4, &piece[4..]).contains(&Emit::Refuted));
}

// A piece is judged once: a front that joined is not judged again when its
// bytes come again, and one that refuted refutes no second time.
#[test]
fn a_piece_is_judged_once() {
    let mut object = set(&["vessel.bin.001", "vessel.bin.002"]);
    sized(&mut *object, 1, 100);
    assert!(!deliver(&mut *object, 1, 0, &front(&[], 100)).contains(&Emit::Refuted));
    assert!(!deliver(&mut *object, 1, 0, &front(RAR5, 100)).contains(&Emit::Refuted));

    let mut object = set(&["vessel.bin.001", "vessel.bin.002"]);
    sized(&mut *object, 1, 100);
    assert!(deliver(&mut *object, 1, 0, &front(RAR5, 100)).contains(&Emit::Refuted));
    assert!(!deliver(&mut *object, 1, 0, &front(RAR5, 100)).contains(&Emit::Refuted));
}

// Damaged bytes judge no front: a later piece whose first article was lost
// arrives with nothing where its head stood, and the repair that brings
// the head back refutes the set, whether one slice or several bring it.
#[test]
fn a_front_mended_by_a_repair_is_judged_on_the_mended_bytes() {
    let piece = front(RAR5, 100);
    let lost = front(&[], 100);

    let mut object = set(&["vessel.bin.001", "vessel.bin.002"]);
    sized(&mut *object, 1, 100);
    let out = deliver_as(&mut *object, 1, 0, &lost, Provenance::Damaged);
    assert!(!out.contains(&Emit::Refuted));
    let out = deliver_as(&mut *object, 1, 0, &piece, Provenance::Repaired);
    assert!(out.contains(&Emit::Refuted));

    let mut object = set(&["vessel.bin.001", "vessel.bin.002"]);
    sized(&mut *object, 1, 100);
    deliver_as(&mut *object, 1, 0, &lost, Provenance::Refuted);
    let mut out = deliver_as(&mut *object, 1, 0, &piece[..4], Provenance::Repaired);
    out.extend(deliver_as(
        &mut *object,
        1,
        4,
        &piece[4..],
        Provenance::Repaired,
    ));
    assert_eq!(out.iter().filter(|emit| **emit == Emit::Refuted).count(), 1);
}

// The first piece opens the joined file, so its front never refutes,
// whatever it signs and however its articles cut it.
#[test]
fn the_first_piece_never_refutes() {
    for head in [
        RAR5.to_vec(),
        sevenz_signature_header(),
        b"Rar!\x1aRev".to_vec(),
        ustar_header(),
    ] {
        let mut object = set(&["vessel.bin.001", "vessel.bin.002"]);
        sized(&mut *object, 0, 1024);
        let piece = front(&head, 1024);
        let mut out = deliver(&mut *object, 0, 0, &piece[..4]);
        out.extend(deliver(&mut *object, 0, 4, &piece[4..]));
        out.extend(deliver(&mut *object, 0, 0, &piece));
        assert!(!out.contains(&Emit::Refuted), "{:x?}", &head[..8]);
    }
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
