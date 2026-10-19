// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeMap;

use weaver_graph::{
    ByteRange, CoverageRecord, CryptRecord, Facts, Kind, MemberId, MemberRecord, NodeId,
    NodeRecord, NodeState, ObjectId, Provenance, Run, TreeRecord, VolumeIndex, Why,
};

use super::{CheckpointError, FileState, Snapshot, rows, snapshot};
use crate::pipeline::graph::envelope::durable::KeptRecord;
use crate::pipeline::graph::root::facts::{
    ArticleFacts, ClassFacts, RootFacts, VolumeFacts, decode, encode,
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
    ];
    root.members[2].buried = true;
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
    let facts = decode(RootFacts::VERSION, &encode(&taken.root)).unwrap();
    assert_eq!(facts.reported, Some(RootPhase::Done));
}

// Root facts written before the reported phase was kept: the rest reads as
// it was written, and no phase was reported.
#[test]
fn root_facts_without_a_reported_phase_restore_none() {
    #[derive(serde::Serialize)]
    struct Before<'a> {
        next_node: u32,
        posted: &'a [String],
        volumes: &'a [VolumeFacts],
        read_backs: u64,
    }
    let taken = full().root;
    let before = rmp_serde::to_vec(&Before {
        next_node: taken.next_node,
        posted: &taken.posted,
        volumes: &taken.volumes,
        read_backs: taken.read_backs,
    })
    .unwrap();
    let facts = decode(RootFacts::VERSION, &before).unwrap();
    assert_eq!(
        facts,
        RootFacts {
            reported: None,
            ..taken
        }
    );
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
