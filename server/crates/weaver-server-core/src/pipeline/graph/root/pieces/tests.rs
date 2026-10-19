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
    let bytes = torrent_bytes(layout);
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
    // Nothing raised: every file still waiting is fetched.
    assert_eq!(root.selection(), vec![0, 1, 2]);

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

    // Once the raised pieces are in, nothing is raised, and every file
    // with a piece to come is fetched again.
    for piece in 0..4 {
        for share in shares(&layout, piece) {
            root.receive(share);
        }
    }
    assert_eq!(root.selection(), vec![2]);

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
        job.verified(shares(&layout, piece), 1).unwrap();
    }
    // Piece 3 again: b's share and c's are both in hand already.
    job.verified(shares(&layout, 3), 1).unwrap();
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
    assert_eq!(job.selection(), vec![2]);
    // c's share of piece 3 is in hand; its share of piece 4 is not.
    assert_eq!(job.file_have(), vec![1000, 2500, 4096 - 3500]);

    let rows = checkpoint::rows(&job.snapshot().unwrap());
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
    job.verified(shares(&layout, 4), 1).unwrap();
    assert!(job.have().all());
    assert_eq!(
        std::fs::read(dir.path().join("c.bin")).unwrap(),
        bytes[3500..]
    );
    assert!(job.namespace().members().all(|member| member.closed));
}

// A torrent's checkpoint never restores into an article job, nor the
// other way about.
#[test]
fn a_torrent_checkpoint_is_read_only_by_a_torrent_job() {
    let layout = layout();
    let dir = tempfile::tempdir().unwrap();
    let mut job = torrent_job(&layout, dir.path());
    let snapshot = checkpoint::snapshot(&checkpoint::rows(&job.snapshot().unwrap())).unwrap();
    assert!(matches!(
        GraphJob::restore(&snapshot, &[], 1, storage(dir.path()), 1),
        Err(crate::pipeline::graph::job::restore::RestoreFailure::Root(
            RootFactsError::Mismatch("another kind of root")
        ))
    ));
    let _ = Unit::Waiting;
}
