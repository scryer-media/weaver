// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::Arc;

use bytes::Bytes;
use weaver_graph::holds::{HoldBudgets, HoldsAccountant};
use weaver_graph::{ByteRange, MemberId, NodeId, Pull, Purpose, Seek, VolumeIndex, Weight};

use super::layout::Span;
use super::{Class, Move, PieceFile, PieceLayout, PieceRoot, Unit, Verified};
use crate::operations::disk::CapacityReader;
use crate::pipeline::graph::checkpoint;
use crate::pipeline::graph::engine::{RunMail, Runs};
use crate::pipeline::graph::job::{GraphJob, Storage};
use crate::pipeline::graph::limits::Caps;
use crate::pipeline::graph::namespace::NamespaceWriter;
use crate::pipeline::graph::root::facts::RootFactsError;
use crate::pipeline::graph::root::{Ask, Root};
use crate::pipeline::graph::status::RepairSlots;

const PIECE: u64 = 1024;

fn file(name: &str, length: u64) -> PieceFile {
    PieceFile {
        name: name.to_owned(),
        length,
        padding: false,
    }
}

// Three files whose pieces straddle them: a ends inside piece 0, b runs
// from there into piece 3, c from there to the end, inside piece 4.
//
//   piece   0         1         2         3         4
//   bytes   0..1024   ..2048    ..3072    ..4096    ..4200
//   a       0..1000
//   b         1000 ........................ 3500
//   c                                         3500 ... 4200
fn layout() -> Arc<PieceLayout> {
    Arc::new(
        PieceLayout::new(
            PIECE,
            vec![file("a.bin", 1000), file("b.bin", 2500), file("c.bin", 700)],
        )
        .unwrap(),
    )
}

fn total(layout: &PieceLayout) -> u64 {
    layout.files().map(|file| file.length).sum()
}

fn torrent_bytes(layout: &PieceLayout) -> Vec<u8> {
    (0..total(layout)).map(|at| (at % 251) as u8).collect()
}

// Each volume's share of the piece, as the storage hands it over.
fn shares(layout: &PieceLayout, piece: u32) -> Vec<Verified> {
    shares_of(layout, &torrent_bytes(layout), piece)
}

// Each volume's share of the piece of `bytes`, the torrent's whole content.
fn shares_of(layout: &PieceLayout, bytes: &[u8], piece: u32) -> Vec<Verified> {
    let range = layout.piece_range(piece);
    layout
        .spans(piece)
        .into_iter()
        .filter_map(|span| {
            let at = (range.start + span.at) as usize;
            Some(Verified {
                volume: span.volume?,
                piece,
                offset: span.range.start,
                bytes: Bytes::copy_from_slice(&bytes[at..at + span.range.len() as usize]),
            })
        })
        .collect()
}

fn critical(volume: u32, range: ByteRange) -> Ask {
    Ask {
        seek: Seek {
            volume: VolumeIndex(volume),
            range,
            purpose: Purpose::Data,
            weight: Weight::FIRST,
        },
        bidder: NodeId(1),
        in_order: false,
    }
}

fn recovery(volume: u32, range: ByteRange) -> Ask {
    Ask {
        seek: Seek {
            volume: VolumeIndex(volume),
            range,
            purpose: Purpose::Recovery,
            weight: Weight::FIRST,
        },
        bidder: NodeId(1),
        in_order: false,
    }
}

#[test]
fn a_volume_range_maps_onto_the_pieces_across_file_boundaries() {
    let layout = layout();
    assert_eq!(layout.piece_count(), 5);
    assert_eq!(layout.volume_pieces(VolumeIndex(0)), 0..1);
    assert_eq!(layout.volume_pieces(VolumeIndex(1)), 0..4);
    assert_eq!(layout.volume_pieces(VolumeIndex(2)), 3..5);
    // b's byte 24 is the torrent's byte 1024, the first of piece 1.
    assert_eq!(layout.pieces(VolumeIndex(1), ByteRange::new(23, 25)), 0..2);
    assert_eq!(layout.pieces(VolumeIndex(1), ByteRange::new(24, 25)), 1..2);
    // Cut at the volume's end, and nothing past it.
    assert_eq!(
        layout.pieces(VolumeIndex(0), ByteRange::new(900, u64::MAX)),
        0..1
    );
    assert!(
        layout
            .pieces(VolumeIndex(0), ByteRange::new(1000, 2000))
            .is_empty()
    );
    assert!(
        layout
            .pieces(VolumeIndex(3), ByteRange::new(0, 1))
            .is_empty()
    );

    assert_eq!(
        layout.spans(0),
        vec![
            Span {
                file: 0,
                volume: Some(VolumeIndex(0)),
                at: 0,
                range: ByteRange::new(0, 1000),
            },
            Span {
                file: 1,
                volume: Some(VolumeIndex(1)),
                at: 1000,
                range: ByteRange::new(0, 24),
            },
        ]
    );
    assert_eq!(
        layout.spans(3),
        vec![
            Span {
                file: 1,
                volume: Some(VolumeIndex(1)),
                at: 0,
                range: ByteRange::new(2072, 2500),
            },
            Span {
                file: 2,
                volume: Some(VolumeIndex(2)),
                at: 428,
                range: ByteRange::new(0, 596),
            },
        ]
    );
    assert_eq!(
        layout.span(VolumeIndex(2), 4),
        Some(ByteRange::new(596, 700))
    );
    assert_eq!(layout.span(VolumeIndex(0), 1), None);
    assert_eq!(layout.piece_range(4), ByteRange::new(4096, 4200));
}

// A padding file fills a piece out: it is in the layout, never a volume,
// and an empty file is a volume with no piece.
#[test]
fn padding_is_laid_out_but_never_a_volume() {
    let layout = PieceLayout::new(
        PIECE,
        vec![
            file("x.bin", 1000),
            PieceFile {
                name: ".pad/24".to_owned(),
                length: 24,
                padding: true,
            },
            file("empty.bin", 0),
            file("y.bin", 100),
        ],
    )
    .unwrap();
    assert_eq!(layout.volume_count(), 3);
    assert_eq!(layout.volume_file(VolumeIndex(2)), 3);
    assert_eq!(layout.file_volume(1), None);
    assert!(layout.volume_pieces(VolumeIndex(1)).is_empty());
    assert_eq!(layout.volume_pieces(VolumeIndex(2)), 1..2);
    let spans = layout.spans(0);
    assert_eq!(spans.len(), 2);
    assert_eq!(spans[1].volume, None);
    assert_eq!(spans[1].range, ByteRange::new(0, 24));
    assert_eq!(
        PieceLayout::new(0, Vec::new()),
        Err(super::LayoutError::NoPieceLength)
    );
}

#[test]
fn receive_delivers_each_file_share_in_its_own_coordinates() {
    let layout = layout();
    let mut root = PieceRoot::new(Arc::clone(&layout));
    let mut have = root.have();
    assert!(have.not_any());
    let mut piece = shares(&layout, 0).into_iter();
    let arrival = root.receive(piece.next().unwrap());
    assert_eq!(
        (
            arrival.node,
            arrival.member,
            arrival.offset,
            arrival.bytes.len()
        ),
        (NodeId::ROOT, MemberId(0), 0, 1000)
    );
    // Half of piece 0 in hand is not the piece.
    assert!(!root.have()[0]);
    let arrival = root.receive(piece.next().unwrap());
    assert_eq!(
        (arrival.member, arrival.offset, arrival.bytes.len()),
        (MemberId(1), 0, 24)
    );
    assert_eq!(&arrival.bytes[..], &torrent_bytes(&layout)[1000..1024]);
    have.set(0, true);
    assert_eq!(root.have(), have);
    assert_eq!(
        root.arrived_ranges(VolumeIndex(1)),
        vec![(0, ByteRange::new(0, 24))]
    );
    assert!(!root.volume_waiting(VolumeIndex(0)));
    assert!(root.volume_waiting(VolumeIndex(1)));
    assert!(!root.waiting(VolumeIndex(1), 0));
    assert!(root.waiting(VolumeIndex(1), 1));
    // Bytes that read back as they were handed over match; others do not.
    let bytes = torrent_bytes(&layout);
    assert!(root.matches(VolumeIndex(1), ByteRange::new(0, 24), &bytes[1000..1024]));
    assert!(!root.matches(VolumeIndex(1), ByteRange::new(0, 24), &[0; 24]));
    // A range holding only part of a share is taken as read.
    assert!(root.matches(VolumeIndex(1), ByteRange::new(0, 10), &[0; 10]));
    assert!(root.unseen().is_empty());
}

#[test]
fn a_class_raised_by_a_seek_becomes_the_selection() {
    let layout = layout();
    let mut root = PieceRoot::new(Arc::clone(&layout));
    // Nothing raised: no file is named.
    assert!(root.selection().is_empty());

    // c's first bytes lie in piece 3, which also carries b's last.
    let answers = root.answer(&[critical(2, ByteRange::new(0, 10))], |_| true);
    assert_eq!(answers, vec![Pull::Moved(1)]);
    assert_eq!(
        root.take_moves(),
        vec![Move {
            piece: 3,
            class: Class::Critical,
        }]
    );
    assert_eq!(root.class(VolumeIndex(1), 3), Some(Class::Critical));
    assert_eq!(root.class(VolumeIndex(0), 3), None);
    assert_eq!(root.selection(), vec![1, 2]);

    // A recovery seek does not lower a critical piece; it raises the rest.
    root.answer(&[recovery(1, ByteRange::new(0, u64::MAX))], |_| true);
    assert_eq!(
        root.take_moves(),
        vec![
            Move {
                piece: 0,
                class: Class::Recovery,
            },
            Move {
                piece: 1,
                class: Class::Recovery,
            },
            Move {
                piece: 2,
                class: Class::Recovery,
            },
        ]
    );
    assert_eq!(root.class(VolumeIndex(1), 3), Some(Class::Critical));
    assert_eq!(root.selection(), vec![0, 1, 2]);

    // Once the raised pieces are in, nothing is raised, and no file is
    // named, though c still has a piece to come.
    for piece in 0..4 {
        for share in shares(&layout, piece) {
            root.receive(share);
        }
    }
    assert!(root.selection().is_empty());

    // The first piece of every file still waiting heads the fetch.
    root.sample();
    assert_eq!(
        root.take_moves(),
        Vec::<Move>::new(),
        "c's first piece, 3, is in"
    );
}

#[test]
fn a_waiting_piece_is_moved_never_lost() {
    let layout = layout();
    let mut root = PieceRoot::new(Arc::clone(&layout));
    for share in shares(&layout, 1) {
        root.receive(share);
    }
    // No peer has a piece yet: it waits, and a seek of it moves it.
    assert_eq!(
        root.answer(&[critical(1, ByteRange::new(0, 100))], |_| true),
        vec![Pull::Moved(1)]
    );
    assert!(root.damage().is_empty());
    // Past the end is never unseekable: there is just nothing to move.
    assert_eq!(
        root.answer(&[critical(0, ByteRange::new(5000, 6000))], |_| true),
        vec![Pull::Moved(0)]
    );
    assert!(root.damage().is_empty());
}

#[test]
fn the_have_bitfield_comes_back_from_the_facts() {
    let layout = layout();
    let mut root = PieceRoot::new(Arc::clone(&layout));
    for piece in [0, 3] {
        for share in shares(&layout, piece) {
            root.receive(share);
        }
    }
    root.answer(&[recovery(2, ByteRange::new(600, 700))], |_| true);
    root.take_moves();
    let facts = root.facts();
    let mut restored = PieceRoot::restore(Arc::clone(&layout), &facts).unwrap();
    assert_eq!(restored.have(), root.have());
    assert_eq!(restored.have().iter_ones().collect::<Vec<_>>(), vec![0, 3]);
    assert_eq!(
        restored.take_moves(),
        vec![Move {
            piece: 4,
            class: Class::Recovery,
        }]
    );
    // A share that does not read back takes its piece out of hand.
    restored.unarrive(VolumeIndex(2), 3);
    assert_eq!(restored.have().iter_ones().collect::<Vec<_>>(), vec![0]);

    let other = Arc::new(PieceLayout::new(PIECE * 2, vec![file("a.bin", 4200)]).unwrap());
    assert_eq!(
        PieceRoot::restore(other, &facts).err(),
        Some(RootFactsError::Mismatch("another piece length"))
    );
}

fn accountant() -> Arc<HoldsAccountant> {
    Arc::new(HoldsAccountant::new(HoldBudgets {
        resident_limit: u64::MAX,
        scratch_total: u64::MAX,
        scratch_ceiling: u64::MAX,
        disk_reserve: 0,
    }))
}

pub(crate) fn storage(dir: &std::path::Path) -> Storage {
    Storage {
        working_dir: dir.to_path_buf(),
        accountant: accountant(),
        free_disk: CapacityReader::unknown(),
        slots: 1,
        mail: RunMail::default().sender(),
        passwords: Vec::new(),
        caps: Caps::unbounded(),
        nesting: u32::MAX,
        unpack: true,
    }
}

fn torrent_job(layout: &Arc<PieceLayout>, dir: &std::path::Path) -> GraphJob<super::PieceRoot> {
    let accountant = accountant();
    let writer = NamespaceWriter::new(
        1,
        dir.to_path_buf(),
        Arc::clone(&accountant),
        CapacityReader::unknown(),
    );
    let runs = Runs::new(1, 1, RunMail::default().sender(), accountant);
    GraphJob::torrent(
        Arc::clone(layout),
        Box::new(weaver_objects::Objects),
        writer,
        runs,
        u32::MAX,
    )
}

// Plain files in pieces: each file is written whole, a piece fetched again
// delivers only the share still to come, and the job restores from its
// checkpoint with the same pieces in hand and nothing to fetch again.
#[test]
fn a_torrent_job_writes_its_files_and_restores_its_pieces() {
    let layout = layout();
    let dir = tempfile::tempdir().unwrap();
    let mut job = torrent_job(&layout, dir.path());
    for piece in 0..4 {
        job.verified(shares(&layout, piece), &mut RepairSlots::default(), 1)
            .unwrap();
    }
    // Piece 3 again: b's share and c's are both in hand already.
    job.verified(shares(&layout, 3), &mut RepairSlots::default(), 1)
        .unwrap();
    let bytes = torrent_bytes(&layout);
    assert_eq!(
        std::fs::read(dir.path().join("a.bin")).unwrap(),
        bytes[..1000]
    );
    assert_eq!(
        std::fs::read(dir.path().join("b.bin")).unwrap(),
        bytes[1000..3500]
    );
    assert_eq!(job.have().iter_ones().collect::<Vec<_>>(), vec![0, 1, 2, 3]);
    // c still has a piece to come, but nothing raised it.
    assert!(job.selection().is_empty());
    // c's share of piece 3 is in hand; its share of piece 4 is not.
    assert_eq!(job.file_have(), vec![1000, 2500, 4096 - 3500]);

    let rows = checkpoint::rows(&job.snapshot());
    let snapshot = checkpoint::snapshot(&rows).unwrap();
    let restored =
        GraphJob::restore_torrent(&snapshot, Arc::clone(&layout), 1, storage(dir.path()), 1)
            .unwrap();
    assert!(restored.refetched.is_empty());
    assert_eq!(restored.job.have(), job.have());
    assert_eq!(restored.job.file_have(), job.file_have());
    assert_eq!(restored.settled.len(), 1 + 4 + 1);

    // The last piece completes c.
    let mut job = restored.job;
    job.verified(shares(&layout, 4), &mut RepairSlots::default(), 1)
        .unwrap();
    assert!(job.have().all());
    assert_eq!(
        std::fs::read(dir.path().join("c.bin")).unwrap(),
        bytes[3500..]
    );
    assert!(job.namespace().members().all(|member| member.closed));
}

// A piece that fails its hash after it verified is in hand no longer:
// each file's share of it waits again, its bytes are no longer covered and
// its files are open, so the share fetched again is delivered and rewrites
// what was damaged. A checkpoint taken meanwhile says the same, and a
// piece no share of which is in hand is left as it stands.
#[test]
fn a_piece_that_fails_its_hash_waits_to_be_fetched_again() {
    let layout = layout();
    let dir = tempfile::tempdir().unwrap();
    let mut job = torrent_job(&layout, dir.path());
    for piece in 0..5 {
        job.verified(shares(&layout, piece), &mut RepairSlots::default(), 1)
            .unwrap();
    }
    assert!(job.have().all());
    let bytes = torrent_bytes(&layout);
    // Piece 3 carries b's 3072..3500 and c's 0..596.
    let damaged = vec![0x5a; 4096 - 3072];
    let mut on_disk = bytes.clone();
    on_disk[3072..4096].copy_from_slice(&damaged);
    std::fs::write(dir.path().join("b.bin"), &on_disk[1000..3500]).unwrap();
    std::fs::write(dir.path().join("c.bin"), &on_disk[3500..]).unwrap();

    assert!(job.failed(3, &mut RepairSlots::default(), 1).unwrap());
    assert_eq!(job.have().iter_ones().collect::<Vec<_>>(), vec![0, 1, 2, 4]);
    // The piece is fetched again at its files' own level: nothing is raised.
    assert!(job.selection().is_empty());
    assert_eq!(job.file_have(), vec![1000, 2500 - 428, 700 - 596]);
    for (member, hole) in [(1, ByteRange::new(2072, 2500)), (2, ByteRange::new(0, 596))] {
        let record = job.namespace().get(MemberId(member)).unwrap();
        assert!(!record.closed, "{member}");
        assert!(!record.coverage.covered(hole), "{member}");
    }
    assert!(job.namespace().get(MemberId(0)).unwrap().closed);
    // Nothing of it is in hand: failing again changes nothing.
    assert!(!job.failed(3, &mut RepairSlots::default(), 1).unwrap());
    assert_eq!(job.have().iter_ones().collect::<Vec<_>>(), vec![0, 1, 2, 4]);

    let snapshot = checkpoint::snapshot(&checkpoint::rows(&job.snapshot())).unwrap();
    let restored =
        GraphJob::restore_torrent(&snapshot, Arc::clone(&layout), 1, storage(dir.path()), 1)
            .unwrap();
    assert!(restored.refetched.is_empty());
    assert_eq!(restored.job.have(), job.have());

    // Fetched again, the piece rewrites both files and closes them again.
    job.verified(shares(&layout, 3), &mut RepairSlots::default(), 1)
        .unwrap();
    assert!(job.have().all());
    assert_eq!(
        std::fs::read(dir.path().join("b.bin")).unwrap(),
        bytes[1000..3500]
    );
    assert_eq!(
        std::fs::read(dir.path().join("c.bin")).unwrap(),
        bytes[3500..]
    );
    assert!(job.namespace().members().all(|member| member.closed));
}

// libtorrent `torrent::force_recheck` forgets every piece before checking,
// and `torrent::write_resume_data` saves none the check has not found: once
// a check of every piece begins, no piece is had, though every share in hand
// still reads, and a checkpoint taken then restores a job that still waits
// for the check. Each piece the check finds is had again, and one it does
// not is given back.
#[test]
fn a_check_of_every_piece_has_none_until_it_finds_them() {
    let layout = layout();
    let dir = tempfile::tempdir().unwrap();
    let mut job = torrent_job(&layout, dir.path());
    for piece in 0..5 {
        job.verified(shares(&layout, piece), &mut RepairSlots::default(), 1)
            .unwrap();
    }
    assert!(!job.unchecked());
    let bytes = torrent_bytes(&layout);

    assert!(job.checking(&mut RepairSlots::default(), 1).unwrap());
    assert!(job.unchecked());
    assert_eq!(job.have().count_ones(), 0);
    let mut read = Vec::new();
    assert!(job.posted(VolumeIndex(1), ByteRange::new(0, 2500), &mut read));
    assert_eq!(read, bytes[1000..3500]);
    // Nothing is fetched while the check reads.
    assert!(job.selection().is_empty());

    let snapshot = checkpoint::snapshot(&checkpoint::rows(&job.snapshot())).unwrap();
    let restored =
        GraphJob::restore_torrent(&snapshot, Arc::clone(&layout), 1, storage(dir.path()), 1)
            .unwrap();
    assert!(restored.refetched.is_empty());
    assert!(restored.job.unchecked());
    assert_eq!(restored.job.have().count_ones(), 0);

    for piece in [0, 1, 2, 4] {
        assert!(job.found(piece, &mut RepairSlots::default(), 1).unwrap());
    }
    assert!(!job.found(0, &mut RepairSlots::default(), 1).unwrap());
    assert!(job.failed(3, &mut RepairSlots::default(), 1).unwrap());
    assert!(!job.unchecked());
    assert_eq!(job.have().iter_ones().collect::<Vec<_>>(), vec![0, 1, 2, 4]);
    assert!(job.selection().is_empty());
}

// A share that never arrived, or was lost, is not in hand: only an arrived
// one is given back.
#[test]
fn only_a_share_in_hand_is_given_back() {
    let layout = layout();
    let mut root = PieceRoot::new(Arc::clone(&layout));
    for share in shares(&layout, 0) {
        root.receive(share);
    }
    root.lost(VolumeIndex(2), 4);
    assert_eq!(
        root.in_hand(0),
        vec![
            (VolumeIndex(0), ByteRange::new(0, 1000)),
            (VolumeIndex(1), ByteRange::new(0, 24))
        ]
    );
    assert!(root.in_hand(1).is_empty());
    assert!(root.in_hand(4).is_empty());
}

// A torrent's checkpoint never restores into an article job, nor the
// other way about.
#[test]
fn a_torrent_checkpoint_is_read_only_by_a_torrent_job() {
    let layout = layout();
    let dir = tempfile::tempdir().unwrap();
    let job = torrent_job(&layout, dir.path());
    let snapshot = checkpoint::snapshot(&checkpoint::rows(&job.snapshot())).unwrap();
    assert!(matches!(
        GraphJob::restore(&snapshot, &[], 1, storage(dir.path()), 1),
        Err(crate::pipeline::graph::job::restore::RestoreFailure::Root(
            RootFactsError::Mismatch("another kind of root")
        ))
    ));
    let _ = Unit::Waiting;
}

// A job that does not unpack opens no member: an archive's volume stays a
// leaf and lands as it is, where the real objects would open it.
#[test]
fn a_job_that_does_not_unpack_leaves_an_archive_a_leaf() {
    let layout = Arc::new(PieceLayout::new(PIECE, vec![file("set.rar", 2000)]).unwrap());
    let mut bytes = b"Rar!\x1a\x07\x01\x00".to_vec();
    bytes.extend((bytes.len()..2000).map(|at| (at % 251) as u8));
    let job = |unpack: bool, dir: &std::path::Path| {
        let accountant = accountant();
        let writer = NamespaceWriter::new(
            1,
            dir.to_path_buf(),
            Arc::clone(&accountant),
            CapacityReader::unknown(),
        );
        let runs = Runs::new(1, 1, RunMail::default().sender(), accountant);
        let mut job = GraphJob::torrent(
            Arc::clone(&layout),
            crate::pipeline::graph::job::builder(unpack),
            writer,
            runs,
            u32::MAX,
        );
        // No run slot: an object is built, but no engine starts.
        for piece in 0..2 {
            job.verified(
                shares_of(&layout, &bytes, piece),
                &mut RepairSlots::default(),
                0,
            )
            .unwrap();
        }
        job
    };

    let dir = tempfile::tempdir().unwrap();
    let leaves = job(false, dir.path());
    assert!(leaves.tree().child_of(NodeId::ROOT, MemberId(0)).is_none());
    assert_eq!(std::fs::read(dir.path().join("set.rar")).unwrap(), bytes);

    let dir = tempfile::tempdir().unwrap();
    let opened = job(true, dir.path());
    assert!(opened.tree().child_of(NodeId::ROOT, MemberId(0)).is_some());
}

// libtorrent `torrent::move_storage` and `storage_utils::move_storage`: a
// torrent moves whole while it is live. The pieces in hand are read from
// the new directory, the next piece lands there, nothing is left under the
// old one, and a restore over the new directory finds every piece.
#[test]
fn a_torrent_job_moves_its_directory_and_goes_on_from_there() {
    let layout = layout();
    let root = tempfile::tempdir().unwrap();
    let (from, to) = (root.path().join("from"), root.path().join("to"));
    std::fs::create_dir_all(&from).unwrap();
    let mut job = torrent_job(&layout, &from);
    for piece in 0..4 {
        job.verified(shares(&layout, piece), &mut RepairSlots::default(), 1)
            .unwrap();
    }
    let bytes = torrent_bytes(&layout);

    // The claimed destination, as the service makes it: an empty directory.
    std::fs::create_dir(&to).unwrap();
    assert!(!job.relocating());
    job.relocate(to.clone());
    assert!(!job.relocating());
    assert_eq!(job.relocated().unwrap().unwrap(), to);
    assert!(job.relocated().is_none());
    assert!(!from.exists());
    assert_eq!(std::fs::read(to.join("a.bin")).unwrap(), bytes[..1000]);
    assert!(
        job.member_files()
            .iter()
            .all(|(_, path)| path.starts_with(&to)),
        "{:?}",
        job.member_files()
    );

    // A peer's read comes from the new directory.
    let mut read = Vec::new();
    assert!(job.posted(VolumeIndex(1), ByteRange::new(0, 2500), &mut read));
    assert_eq!(read, bytes[1000..3500]);

    // The last piece completes c, under the new directory.
    job.verified(shares(&layout, 4), &mut RepairSlots::default(), 1)
        .unwrap();
    assert!(job.have().all());
    assert_eq!(std::fs::read(to.join("c.bin")).unwrap(), bytes[3500..]);
    assert!(!from.exists());

    let snapshot = checkpoint::snapshot(&checkpoint::rows(&job.snapshot())).unwrap();
    let restored =
        GraphJob::restore_torrent(&snapshot, Arc::clone(&layout), 1, storage(&to), 1).unwrap();
    assert!(restored.refetched.is_empty());
    assert!(restored.job.have().all());
}

// libtorrent `torrent::move_storage` is a fence job: it runs once every disk
// job before it is done. Here a move asked for while an engine's run is
// reading waits, and happens at the first step that finds nothing out.
#[tokio::test]
async fn a_move_waits_while_a_run_is_out() {
    let layout = Arc::new(PieceLayout::new(PIECE, vec![file("set.rar", 2000)]).unwrap());
    let mut bytes = b"Rar!\x1a\x07\x01\x00".to_vec();
    bytes.extend((bytes.len()..2000).map(|at| (at % 251) as u8));
    let root = tempfile::tempdir().unwrap();
    let (from, to) = (root.path().join("from"), root.path().join("to"));
    std::fs::create_dir_all(&from).unwrap();
    std::fs::create_dir(&to).unwrap();
    let accountant = accountant();
    let writer = NamespaceWriter::new(
        1,
        from.clone(),
        Arc::clone(&accountant),
        CapacityReader::unknown(),
    );
    let mut mail = RunMail::default();
    let runs = Runs::new(1, 1, mail.sender(), accountant);
    let mut job = GraphJob::torrent(
        Arc::clone(&layout),
        Box::new(weaver_objects::Objects),
        writer,
        runs,
        u32::MAX,
    );
    let mut slots = RepairSlots::default();
    // The archive's header starts its run, which reads on as bytes come.
    job.verified(shares_of(&layout, &bytes, 0), &mut slots, 1)
        .unwrap();
    assert!(job.awaits_mail());

    job.relocate(to.clone());
    assert!(job.relocating());
    assert!(job.relocated().is_none());
    // The volume's bytes are kept under the old directory still, and none
    // has reached the new one.
    assert!(std::fs::read_dir(&from).unwrap().next().is_some());
    assert!(std::fs::read_dir(&to).unwrap().next().is_none());

    // The rest of the volume is not an archive: the run ends on it, and
    // the job hears so. The move happens at the step that finds the job
    // quiet.
    job.verified(shares_of(&layout, &bytes, 1), &mut slots, 1)
        .unwrap();
    while job.awaits_mail() {
        let said = mail.recv().await.expect("the run speaks before it ends");
        job.run_said(said, &mut slots, 1).unwrap();
        job.try_relocate();
    }
    assert!(!job.relocating());
    assert_eq!(job.relocated().unwrap().unwrap(), to);
    assert_eq!(std::fs::read(to.join("set.rar")).unwrap(), bytes);
    assert!(!from.exists());
}

// A move that fails leaves the job writing where it was, and says why once.
#[test]
fn a_failed_move_leaves_the_job_where_it_was() {
    let layout = layout();
    let root = tempfile::tempdir().unwrap();
    let (from, to) = (root.path().join("from"), root.path().join("to"));
    std::fs::create_dir_all(&from).unwrap();
    let mut job = torrent_job(&layout, &from);
    job.verified(shares(&layout, 0), &mut RepairSlots::default(), 1)
        .unwrap();
    // No claim was made: nothing is at `to`, so the move has nothing to
    // make way for and fails there.
    job.relocate(to.clone());
    assert!(!job.relocating());
    assert!(job.relocated().unwrap().is_err());
    assert!(job.relocated().is_none());
    let bytes = torrent_bytes(&layout);
    job.verified(shares(&layout, 1), &mut RepairSlots::default(), 1)
        .unwrap();
    assert_eq!(
        std::fs::read(from.join("b.bin")).unwrap()[..1048],
        bytes[1000..2048]
    );
    assert!(!to.exists());
}

// A torrent's pieces come in any order: those that arrive before the piece
// carrying a file's front, which says what the file is, are on disk in the
// file all the same. The checkpoint restores every one of them, nothing is
// fetched again, and the pieces still to come complete the file.
#[test]
fn pieces_before_a_files_front_restore_from_its_file() {
    let layout = Arc::new(PieceLayout::new(PIECE, vec![file("a.bin", 5000)]).unwrap());
    let bytes: Vec<u8> = (0..5000u32).map(|at| (at % 251) as u8).collect();
    let dir = tempfile::tempdir().unwrap();
    let mut job = torrent_job(&layout, dir.path());
    for piece in [1, 3] {
        job.verified(
            shares_of(&layout, &bytes, piece),
            &mut RepairSlots::default(),
            1,
        )
        .unwrap();
    }
    assert_eq!(job.have().iter_ones().collect::<Vec<_>>(), vec![1, 3]);
    assert!(job.in_memory().is_empty());

    let snapshot = checkpoint::snapshot(&checkpoint::rows(&job.snapshot())).unwrap();
    drop(job);
    let restored =
        GraphJob::restore_torrent(&snapshot, Arc::clone(&layout), 1, storage(dir.path()), 1)
            .unwrap();
    assert!(restored.refetched.is_empty());
    assert_eq!(
        restored.job.have().iter_ones().collect::<Vec<_>>(),
        vec![1, 3]
    );

    let mut job = restored.job;
    for piece in [0, 2, 4] {
        job.verified(
            shares_of(&layout, &bytes, piece),
            &mut RepairSlots::default(),
            1,
        )
        .unwrap();
    }
    assert!(job.have().all());
    assert_eq!(std::fs::read(dir.path().join("a.bin")).unwrap(), bytes);
}
