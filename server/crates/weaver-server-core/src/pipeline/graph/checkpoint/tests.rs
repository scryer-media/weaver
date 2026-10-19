// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeMap;
use std::sync::Arc;

use weaver_graph::{
    ByteRange, CoverageRecord, CryptRecord, Facts, Kind, MemberId, MemberRecord, NodeId,
    NodeRecord, NodeState, ObjectId, Part, Provenance, Run, TreeRecord, VolumeIndex, Why,
};

use super::{CheckpointError, FileState, Snapshot, rows, snapshot};
use crate::jobs::graph_checkpoint::{Delta, GraphCheckpoint};
use crate::pipeline::graph::envelope::durable::KeptRecord;
use crate::pipeline::graph::root::ArticleRoot;
use crate::pipeline::graph::root::facts::{
    ArticleFacts, ClassFacts, Encoded, JobFacts, PostFacts, RootBlob, RootFactsError, RootRow,
    VolumeFacts, decode, encode, encode_post,
};
use crate::pipeline::graph::root::phase::RootPhase;
use crate::pipeline::graph::root::pieces::PieceRoot;

// The root row of version 2, as builds before roots had kinds wrote it:
// the article root's facts with the job's beside them.
#[derive(serde::Serialize)]
struct Version2<'a> {
    next_node: u32,
    posted: &'a [String],
    volumes: &'a [VolumeFacts],
    read_backs: u64,
    reported: Option<RootPhase>,
}

// Rewrite an article job's root row as a build before roots had kinds
// wrote it.
pub(crate) fn as_version_2(checkpoint: &mut GraphCheckpoint) {
    let row = checkpoint
        .nodes
        .iter_mut()
        .find(|row| row.node == 0)
        .expect("a root row");
    let parts: Vec<Part> = checkpoint
        .parts
        .iter()
        .filter(|part| part.node == 0)
        .map(|part| Part {
            key: part.part as u64,
            bytes: Arc::clone(&part.bytes),
        })
        .collect();
    let now = decode(RootRow::VERSION, &row.facts, &parts).unwrap();
    let post = now.root.read::<ArticleRoot>().unwrap();
    row.facts_version = 2;
    row.facts = rmp_serde::to_vec(&Version2 {
        next_node: now.next_node,
        posted: &post.posted,
        volumes: &post.volumes,
        read_backs: now.job.read_backs,
        reported: now.job.reported,
    })
    .unwrap();
    checkpoint.parts.retain(|part| part.node != 0);
}

fn article_blob(facts: PostFacts) -> RootBlob {
    article_blob_kept(&mut Encoded::default(), facts)
}

// The post encoded by `kept`, so parts unchanged since it last encoded
// keep their bytes.
fn article_blob_kept(kept: &mut Encoded, facts: PostFacts) -> RootBlob {
    let (bytes, parts) = encode_post(kept, &facts);
    RootBlob {
        root: "articles".to_owned(),
        version: PostFacts::VERSION,
        bytes,
        parts,
    }
}

fn post(blob: &RootBlob) -> PostFacts {
    blob.read::<ArticleRoot>().unwrap()
}

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
        job: JobFacts {
            read_backs: 7,
            reported: Some(RootPhase::Done),
        },
        root: article_blob(PostFacts {
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
        }),
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
fn the_phase_last_reported_comes_back_with_the_job() {
    let taken = full();
    let row = decode(
        RootRow::VERSION,
        &encode(&RootRow {
            next_node: taken.tree.next_node,
            job: taken.job,
            root: taken.root.clone(),
        }),
        &taken.root.parts,
    )
    .unwrap();
    assert_eq!(row.job.reported, Some(RootPhase::Done));
    assert_eq!(row.root, taken.root);
}

// An article checkpoint a build before roots had kinds wrote comes back as
// the same snapshot: the article root's facts, and the job's.
#[test]
fn an_article_checkpoint_from_before_roots_had_kinds_comes_back() {
    let taken = full();
    let mut checkpoint = rows(&taken);
    as_version_2(&mut checkpoint);
    assert_eq!(checkpoint.nodes[0].facts_version, 2);
    assert!(checkpoint.parts.iter().all(|part| part.node != 0));
    assert_eq!(snapshot(&checkpoint), Ok(taken));
}

// Root facts written whole, before they were kept as parts, are refused by
// their version, not misread.
#[test]
fn root_facts_without_a_reported_phase_restore_none() {
    #[derive(serde::Serialize)]
    struct Before<'a> {
        next_node: u32,
        posted: &'a [String],
        volumes: &'a [VolumeFacts],
        read_backs: u64,
    }
    let taken = full();
    let facts = post(&taken.root);
    let before = rmp_serde::to_vec(&Before {
        next_node: taken.tree.next_node,
        posted: &facts.posted,
        volumes: &facts.volumes,
        read_backs: taken.job.read_backs,
    })
    .unwrap();
    let row = decode(2, &before, &[]).unwrap();
    assert_eq!(
        row.job,
        JobFacts {
            read_backs: 7,
            reported: None,
        }
    );
    assert_eq!(post(&row.root), facts);
}

#[test]
fn facts_are_read_only_into_the_root_that_wrote_them() {
    let taken = full();
    assert_eq!(
        taken.root.read::<PieceRoot>(),
        Err(RootFactsError::Mismatch("another kind of root"))
    );
    let mut newer = taken.root.clone();
    newer.version += 1;
    assert_eq!(
        newer.read::<ArticleRoot>(),
        Err(RootFactsError::Version(newer.version))
    );
    assert_eq!(decode(1, &[], &[]), Err(RootFactsError::Version(1)));
}

// A post written whole, before its facts were kept as parts, is refused by
// its version, not misread.
#[test]
fn root_facts_of_the_whole_blob_layout_are_refused() {
    let taken = full();
    let whole = RootBlob {
        root: "articles".to_owned(),
        version: 1,
        bytes: rmp_serde::to_vec(&post(&taken.root)).unwrap(),
        parts: Vec::new(),
    };
    assert_eq!(whole.read::<ArticleRoot>(), Err(RootFactsError::Version(1)));
}

// Many articles, of which the step changes one, encoded by `kept`.
fn long_post(arrived: u32, kept: &mut Encoded) -> Snapshot {
    let mut taken = full();
    let mut facts = post(&taken.root);
    let volume = &mut facts.volumes[0];
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
    taken.root = article_blob_kept(kept, facts);
    taken
}

#[test]
fn a_step_writes_only_the_rows_and_parts_it_changed() {
    let mut kept = Encoded::default();
    let before = rows(&long_post(40, &mut kept));
    let after = rows(&long_post(41, &mut kept));
    let delta = Delta::between(Some(&before), &after);
    // The article's chunk and the segment's chunk; the root row only names
    // its parts, so it stays as it was.
    assert!(delta.nodes.is_empty(), "{delta:?}");
    assert_eq!(delta.parts.len(), 2, "{delta:?}");
    assert_eq!(delta.rows(), 2);
    assert!(Delta::between(Some(&after), &after).is_empty());
    // A fresh encoding of the same snapshot is the same rows.
    let fresh = long_post(41, &mut Encoded::default());
    assert_eq!(rows(&fresh), after);
    assert_eq!(snapshot(&after), Ok(fresh));
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
