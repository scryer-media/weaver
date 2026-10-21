// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeMap;
use std::sync::Arc;

use weaver_graph::{
    ByteRange, CoverageRecord, CryptRecord, Facts, Kind, MemberId, MemberRecord, NodeId,
    NodeRecord, NodeState, ObjectId, Part, Provenance, Run, TreeRecord, VolumeIndex, Why,
};

use super::{CheckpointError, FileState, Snapshot, rows, rows_with, snapshot};
use crate::jobs::graph_checkpoint::{Delta, GraphCheckpoint};
use crate::pipeline::graph::envelope::durable::KeptRecord;
use crate::pipeline::graph::root::facts::{
    ArticleFacts, ClassFacts, Encoded, RootFacts, RootFactsError, VolumeFacts, decode, encode,
};
use crate::pipeline::graph::root::phase::RootPhase;

fn member(id: u32, name: &str) -> MemberRecord {
    MemberRecord {
        member: MemberId(id),
        name: name.to_owned(),
        output_name: name.to_owned(),
        length: Some(1_000),
        closed: false,
        bound_to: None,
        owner: None,
        given_up: None,
        buried: false,
        undecided: false,
        copy_of: None,
    }
}

fn node(id: u32, parent: Option<u32>, kind: Option<Kind>) -> NodeRecord {
    NodeRecord {
        node: NodeId(id),
        parent: parent.map(NodeId),
        volumes: Vec::new(),
        window: None,
        kind,
        state: NodeState::Live,
        reason: None,
        facts: kind.map(|kind| Facts {
            kind,
            version: 3,
            bytes: vec![1, 2, 3],
            parts: vec![Part {
                key: 1 << 32 | 2,
                bytes: Arc::from([4, 5].as_slice()),
            }],
        }),
        members: Vec::new(),
        coverage: Vec::new(),
        envelopes: Vec::new(),
        crypt: Vec::new(),
    }
}

// Every field a checkpoint carries, in every shape it takes.
fn full() -> Snapshot {
    let mut root = node(0, None, None);
    root.members = vec![
        member(0, "set.part1.rar"),
        member(1, "set.par2"),
        member(2, "set.r00"),
        member(3, "set.part01.rar"),
    ];
    root.members[2].buried = true;
    root.members[3].copy_of = Some(MemberId(0));
    root.members[0].bound_to = Some(ObjectId(1));
    root.members[0].owner = Some((NodeId(1), VolumeIndex(0)));
    root.members[0].closed = true;
    root.members[1].undecided = true;
    root.members[1].length = None;
    root.coverage = vec![
        CoverageRecord {
            member: MemberId(0),
            start: 0,
            end: 400,
            provenance: Provenance::Fresh,
        },
        CoverageRecord {
            member: MemberId(0),
            start: 400,
            end: 500,
            provenance: Provenance::Damaged,
        },
    ];
    let mut store = node(1, Some(0), Some(Kind::RarStore));
    store.volumes = vec![(VolumeIndex(0), MemberId(0))];
    store.members = vec![member(0, "inner.bin")];
    store.members[0].given_up = Some((NodeId(2), VolumeIndex(0)));
    store.coverage = vec![CoverageRecord {
        member: MemberId(0),
        start: 0,
        end: 300,
        provenance: Provenance::Repaired,
    }];
    store.crypt = vec![CryptRecord {
        member: MemberId(0),
        run_start: -1,
        seed: [9; 16],
    }];
    let mut retired = node(2, Some(1), Some(Kind::Xz));
    retired.volumes = vec![(VolumeIndex(0), MemberId(0))];
    retired.window = Some(ByteRange::new(10, 90));
    retired.state = NodeState::Retired;
    retired.reason = Some(Why::HoldsBreached);
    Snapshot {
        tree: TreeRecord {
            nodes: vec![root, store, retired],
            next_node: 3,
        },
        root: RootFacts {
            next_node: 3,
            posted: vec!["alpha.part1.rar".to_owned(), "alpha.par2".to_owned()],
            volumes: vec![
                VolumeFacts {
                    articles: vec![
                        (0, ArticleFacts::Arrived(Some((0, 400)))),
                        (1, ArticleFacts::Lost { damaged: true }),
                    ],
                    known: vec![(0, 0), (1, 400)],
                    ordered_only: false,
                    segments: vec![(0, 400, 0xdead_beef, true)],
                    raised: Vec::new(),
                },
                VolumeFacts {
                    articles: vec![(0, ArticleFacts::Waiting)],
                    known: vec![(0, 0)],
                    ordered_only: true,
                    segments: Vec::new(),
                    raised: vec![(0, ClassFacts::Recovery)],
                },
            ],
            read_backs: 7,
            reported: Some(RootPhase::Done),
        },
        files: BTreeMap::from([
            (
                (NodeId(1), MemberId(0)),
                FileState {
                    has_file: true,
                    refused: Some(Why::Unrepairable),
                    unusable_at: None,
                },
            ),
            (
                (NodeId(0), MemberId(1)),
                FileState {
                    has_file: true,
                    refused: None,
                    unusable_at: Some(1),
                },
            ),
            (
                (NodeId(0), MemberId(0)),
                FileState {
                    has_file: false,
                    refused: None,
                    unusable_at: Some(0),
                },
            ),
        ]),
        kept: vec![
            KeptRecord {
                node: NodeId(1),
                envelope: 0,
                runs: vec![
                    Run {
                        range: ByteRange::new(0, 20),
                        provenance: Provenance::Fresh,
                    },
                    Run {
                        range: ByteRange::new(300, 400),
                        provenance: Provenance::Damaged,
                    },
                ],
                closed: Some(500),
                settled: true,
            },
            KeptRecord {
                node: NodeId(1),
                envelope: 1,
                runs: Vec::new(),
                closed: None,
                settled: false,
            },
        ],
        passwords: BTreeMap::from([(NodeId(1), 2)]),
    }
}

#[test]
fn a_snapshot_comes_back_from_its_rows() {
    let taken = full();
    assert_eq!(snapshot(&rows(&taken)), Ok(taken));
}

#[test]
fn the_phase_last_reported_comes_back_with_the_root() {
    let taken = full();
    let (version, bytes, parts) = encode(&mut Encoded::default(), &taken.root);
    let facts = decode(version, &bytes, &parts).unwrap();
    assert_eq!(facts.reported, Some(RootPhase::Done));
}

// Root facts written whole, before they were kept as parts, are refused by
// their version, not misread.
#[test]
fn root_facts_of_the_whole_blob_layout_are_refused() {
    let taken = full().root;
    let before = rmp_serde::to_vec(&taken).unwrap();
    assert_eq!(decode(2, &before, &[]), Err(RootFactsError::Version(2)));
}

// Many articles, of which the step changes one.
fn long_post(arrived: u32) -> Snapshot {
    let mut taken = full();
    let volume = &mut taken.root.volumes[0];
    volume.articles = (0..100)
        .map(|ordinal| {
            let start = u64::from(ordinal) * 64;
            let article = if ordinal < arrived {
                ArticleFacts::Arrived(Some((start, start + 64)))
            } else {
                ArticleFacts::Waiting
            };
            (ordinal, article)
        })
        .collect();
    volume.segments = (0..arrived)
        .map(|ordinal| {
            let start = u64::from(ordinal) * 64;
            (start, start + 64, ordinal, false)
        })
        .collect();
    taken
}

#[test]
fn a_step_writes_only_the_rows_and_parts_it_changed() {
    let mut root = Encoded::default();
    let before = rows_with(&mut root, &long_post(40));
    let after = rows_with(&mut root, &long_post(41));
    let delta = Delta::between(Some(&before), &after);
    // The article's chunk and the segment's chunk; the root row only names
    // its parts, so it stays as it was.
    assert!(delta.nodes.is_empty(), "{delta:?}");
    assert_eq!(delta.parts.len(), 2, "{delta:?}");
    assert_eq!(delta.rows(), 2);
    assert!(Delta::between(Some(&after), &after).is_empty());
    // A fresh encoding of the same snapshot is the same rows.
    assert_eq!(rows(&long_post(41)), after);
    assert_eq!(snapshot(&after), Ok(long_post(41)));
}

#[test]
fn a_change_queued_behind_another_writes_both_as_one() {
    let first = rows(&full());
    let mut second = first.clone();
    second.members.pop();
    second.coverage[0].verified = true;
    let mut third = second.clone();
    third.coverage[0].verified = false;
    let mut queued = Delta::between(Some(&first), &second);
    queued.absorb(Delta::between(Some(&second), &third));
    assert_eq!(
        queued.members.values().filter(|row| row.is_none()).count(),
        1
    );
    assert_eq!(queued.coverage.len(), 1);
    assert_eq!(
        queued.coverage.values().next(),
        Some(&Some(first.coverage[0].clone()))
    );
    // Nothing known of the database clears the job whatever follows.
    let mut whole = Delta::between(None, &first);
    whole.absorb(Delta::between(Some(&first), &second));
    assert!(whole.whole);
    queued.absorb(Delta::between(None, &third));
    assert_eq!(queued, Delta::between(None, &third));
    assert!(!Delta::between(Some(&GraphCheckpoint::default()), &first).is_empty());
}

#[test]
fn parts_of_a_node_not_in_the_checkpoint_are_refused() {
    let mut checkpoint = rows(&full());
    let mut stray = checkpoint.parts[0].clone();
    stray.node = 9;
    checkpoint.parts.push(stray);
    assert!(matches!(
        snapshot(&checkpoint),
        Err(CheckpointError::Row { node: 9, .. })
    ));
}

#[test]
fn rows_without_a_root_are_refused() {
    let mut checkpoint = rows(&full());
    checkpoint.nodes.retain(|row| row.node != 0);
    assert_eq!(snapshot(&checkpoint), Err(CheckpointError::NoRoot));
}

#[test]
fn a_row_no_build_could_have_written_is_refused() {
    let mut checkpoint = rows(&full());
    checkpoint.nodes[1].kind = "stream_lzip".to_owned();
    assert!(matches!(
        snapshot(&checkpoint),
        Err(CheckpointError::Row { node: 1, .. })
    ));
    let mut checkpoint = rows(&full());
    checkpoint.coverage[0].end = -4;
    assert!(snapshot(&checkpoint).is_err());
}
