// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeMap;
use std::sync::Arc;

use weaver_graph::{
    ByteRange, CoverageRecord, CryptRecord, Facts, Kind, MemberId, MemberRecord, NodeId,
    NodeRecord, NodeState, ObjectId, Part, Provenance, Run, TreeRecord, VolumeIndex, Why,
};

use weaver_graph::holds::ScratchHold;

use super::{
    CheckpointError, FileState, HoldRecord, Pending, Snapshot, envelope_rows, hold_rows, node_rows,
    part_rows, root_row, snapshot,
};
use crate::jobs::graph_checkpoint::{Delta, GraphCheckpoint, PartRow, Row};
use crate::pipeline::graph::envelope::durable::KeptRecord;
use crate::pipeline::graph::job::{GraphJob, RootFile};
use crate::pipeline::graph::root::ArticleRoot;
use crate::pipeline::graph::root::facts::{
    ArticleFacts, ClassFacts, Encoded, Encoding, Parts, RootFacts, RootFactsError, VolumeFacts,
    decode, of_ordinal,
};
use crate::pipeline::graph::root::phase::RootPhase;
use crate::pipeline::graph::root::tests::{decoded, file, job_with};
use crate::pipeline::graph::status::RepairSlots;

fn member(id: u32, name: &str) -> MemberRecord {
    MemberRecord {
        member: MemberId(id),
        name: name.to_owned(),
        output_name: name.to_owned(),
        length: Some(1_000),
        closed: false,
        crc32: None,
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
    root.members[0].crc32 = Some(0xDEAD_BEEF);
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
                    on_disk: None,
                    refused: Some(Why::Unrepairable),
                },
            ),
            (
                (NodeId(0), MemberId(1)),
                FileState {
                    has_file: true,
                    on_disk: Some(".weaver-chunks/graph-waiting-0-1".to_owned()),
                    refused: None,
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
                file: None,
            },
            KeptRecord {
                node: NodeId(1),
                envelope: 1,
                runs: Vec::new(),
                closed: None,
                settled: false,
                file: Some(".weaver-chunks/graph-envelope-1-1-1".to_owned()),
            },
        ],
        holds: vec![
            HoldRecord {
                node: NodeId(0),
                held: ScratchHold {
                    volume: VolumeIndex(1),
                    range: ByteRange::new(0, 64),
                    scratch_offset: 128,
                },
            },
            HoldRecord {
                node: NodeId(1),
                held: ScratchHold {
                    volume: VolumeIndex(0),
                    range: ByteRange::new(20, 300),
                    scratch_offset: 0,
                },
            },
            HoldRecord {
                node: NodeId(1),
                held: ScratchHold {
                    volume: VolumeIndex(0),
                    range: ByteRange::new(400, 410),
                    scratch_offset: 280,
                },
            },
        ],
        passwords: BTreeMap::from([(NodeId(1), 2)]),
    }
}

// The root's facts as a checkpoint encodes them: the root restored from
// them, encoded whole.
fn encoding(facts: &RootFacts) -> Encoding {
    let files: Vec<RootFile> = facts
        .posted
        .iter()
        .zip(&facts.volumes)
        .map(|(name, volume)| RootFile {
            name: name.clone(),
            articles: volume
                .articles
                .iter()
                .map(|(ordinal, _)| (*ordinal, 64))
                .collect(),
        })
        .collect();
    ArticleRoot::restore(&files, facts).unwrap().encode(
        &mut Encoded::default(),
        facts.next_node,
        facts.read_backs,
        facts.reported,
    )
}

// Every row of the snapshot, each table in key order.
fn rows(taken: &Snapshot) -> GraphCheckpoint {
    let encoding = encoding(&taken.root);
    let Parts::All(parts) = encoding.parts else {
        panic!("a fresh encoding is whole");
    };
    let mut out = GraphCheckpoint {
        nodes: vec![root_row(encoding.version, encoding.bytes)],
        envelopes: envelope_rows(&taken.kept),
        ..GraphCheckpoint::default()
    };
    for node in &taken.tree.nodes {
        let rows = node_rows(
            node,
            |member| {
                taken
                    .files
                    .get(&(node.node, member))
                    .cloned()
                    .unwrap_or_default()
            },
            taken.passwords.get(&node.node).map(|index| *index as i64),
        );
        out.nodes.extend(rows.nodes);
        out.members.extend(rows.members);
        out.coverage.extend(rows.coverage);
        out.crypt.extend(rows.crypt);
        out.parts.extend(rows.parts);
        let holds: Vec<ScratchHold> = taken
            .holds
            .iter()
            .filter(|record| record.node == node.node)
            .map(|record| record.held)
            .collect();
        out.holds.extend(hold_rows(node.node, &holds));
    }
    out.parts
        .extend(part_rows(i64::from(NodeId::ROOT.0), &parts));
    out.nodes.sort_by_key(Row::key);
    out.members.sort_by_key(Row::key);
    out.coverage.sort_by_key(Row::key);
    out.crypt.sort_by_key(Row::key);
    out.parts.sort_by_key(Row::key);
    out
}

#[test]
fn a_snapshot_comes_back_from_its_rows() {
    let taken = full();
    assert_eq!(snapshot(&rows(&taken)), Ok(taken));
}

#[test]
fn hold_rows_come_back_as_the_holds_a_restore_adopts() {
    let taken = full();
    let rows = rows(&taken);
    assert_eq!(rows.holds.len(), 3);
    assert_eq!(rows.holds[2].scratch_offset, 280);
    assert_eq!(snapshot(&rows).map(|back| back.holds), Ok(taken.holds));
    // A change to one hold rewrites that row alone.
    let mut moved = rows.clone();
    moved.holds[1].scratch_offset = 64;
    let delta = between(Some(&rows), &moved);
    assert_eq!(delta.rows(), 1, "{delta:?}");
    assert_eq!(delta.holds.len(), 1);
    // A negative offset is no row a build could have written.
    let mut bad = rows;
    bad.holds[0].scratch_offset = -1;
    assert!(matches!(
        snapshot(&bad),
        Err(CheckpointError::Row { node: 0, .. })
    ));
}

#[test]
fn the_phase_last_reported_comes_back_with_the_root() {
    let taken = full();
    let encoding = encoding(&taken.root);
    let Parts::All(parts) = encoding.parts else {
        panic!("a fresh encoding is whole");
    };
    let facts = decode(encoding.version, &encoding.bytes, &parts).unwrap();
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

fn intake(job: &mut GraphJob, ordinal: u32) {
    let offset = u64::from(ordinal) * 64;
    job.intake(
        decoded(0, ordinal, offset, &[7; 64], Some(6_400)),
        &mut RepairSlots::default(),
        1,
    )
    .unwrap();
}

// A job of many articles, the first `arrived` of them in.
fn long_post(arrived: u32) -> (GraphJob, tempfile::TempDir) {
    let (mut job, dir, _) = job_with(&[file("long.bin", &[64; 100])], Vec::new());
    for ordinal in 0..arrived {
        intake(&mut job, ordinal);
    }
    (job, dir)
}

// The root's facts parts a change writes, by part: `None` for one it
// deletes.
fn root_parts(delta: &Delta) -> BTreeMap<i64, Option<Arc<[u8]>>> {
    let root = i64::from(NodeId::ROOT.0);
    delta
        .parts
        .iter()
        .filter(|((node, _), _)| *node == root)
        .map(|((_, part), row)| (*part, row.as_ref().map(|row| Arc::clone(&row.bytes))))
        .collect()
}

// What the database holds once a whole change is written.
fn held(delta: &Delta) -> GraphCheckpoint {
    fn of<R: Clone, K>(changes: &BTreeMap<K, Option<R>>) -> Vec<R> {
        changes.values().flatten().cloned().collect()
    }
    assert!(delta.whole);
    GraphCheckpoint {
        feed_verdict: delta.feed_verdict.clone(),
        nodes: of(&delta.nodes),
        members: of(&delta.members),
        coverage: of(&delta.coverage),
        envelopes: of(&delta.envelopes),
        crypt: of(&delta.crypt),
        parts: of(&delta.parts),
        holds: of(&delta.holds),
        held_parts: of(&delta.held_parts),
    }
}

#[test]
fn a_step_writes_only_the_rows_and_parts_it_changed() {
    let (mut job, _dir) = long_post(40);
    let pending = Pending::default();
    assert!(pending.put(&job, Vec::new()));
    let first = pending.take().0.unwrap();
    assert!(first.whole);
    intake(&mut job, 40);
    assert!(pending.put(&job, Vec::new()));
    let step = pending.take().0.unwrap();
    assert!(!step.whole);
    // The arrival's chunk alone of the root's parts; the root row only names
    // its parts, so it stays as it was.
    let changed = root_parts(&step);
    assert!(!changed.is_empty(), "{step:?}");
    assert!(
        changed
            .keys()
            .all(|part| *part as u32 == of_ordinal(40) && changed[part].is_some()),
        "{changed:?}"
    );
    assert!(
        !step.nodes.contains_key(&i64::from(NodeId::ROOT.0)),
        "{step:?}"
    );
    assert!(
        !pending.put(&job, Vec::new()),
        "an unchanged job writes nothing"
    );
    // The parts it wrote over the ones before are a fresh checkpoint's.
    let mut written: BTreeMap<i64, Option<Arc<[u8]>>> = root_parts(&first);
    written.extend(changed);
    let fresh = Pending::default();
    assert!(fresh.put(&job, Vec::new()));
    assert_eq!(root_parts(&fresh.take().0.unwrap()), written);
}

#[test]
fn a_pending_writes_what_the_database_does_not_hold() {
    let (job, _dir) = long_post(20);
    let pending = Pending::default();
    assert!(pending.put(&job, Vec::new()));
    let first = pending.take().0.unwrap();
    // A write that failed: the next checkpoint is the whole job again, every
    // root part with it.
    pending.forget();
    assert!(pending.put(&job, Vec::new()));
    let again = pending.take().0.unwrap();
    assert!(again.whole);
    assert_eq!(root_parts(&again), root_parts(&first));
    assert!(!root_parts(&again).is_empty());
    // What the database holds, as a restore read it: the job unchanged
    // writes nothing.
    let restored = Pending::default();
    restored.written(held(&first));
    assert!(!restored.put(&job, Vec::new()));
    assert!(restored.take().0.is_none());
    // A root part the database holds and the root no longer has is deleted,
    // and only it.
    let stale = PartRow {
        node: i64::from(NodeId::ROOT.0),
        part: (1 << 32) | 99,
        bytes: Arc::from([1, 2].as_slice()),
    };
    let mut rows = held(&first);
    rows.parts.push(stale.clone());
    let restored = Pending::default();
    restored.written(rows);
    assert!(restored.put(&job, Vec::new()));
    let delta = restored.take().0.unwrap();
    assert_eq!(delta.rows(), 1, "{delta:?}");
    assert_eq!(root_parts(&delta), BTreeMap::from([(stale.part, None)]));
}

#[test]
fn checkpoint_acknowledgments_belong_to_the_owner_and_latest_awaited_stamp() {
    use crate::pipeline::graph::engine::{Heard, Said};
    let (mut job, _dir) = long_post(20);
    let pending = job.pending().clone();
    pending.put(&job, Vec::new());
    let (_, first) = pending.take();
    job.owe_mail(first);
    intake(&mut job, 20);
    pending.put(&job, Vec::new());
    let (_, latest) = pending.take();
    assert!(latest > first);
    job.owe_mail(latest);
    let mut slots = RepairSlots::default();
    let message = |stamp, control| Said {
        scope: 0,
        node: NodeId::ROOT,
        generation: stamp,
        heard: Heard::Written {
            stamp: Some(stamp),
            control,
        },
    };
    job.run_said(message(latest, Pending::default().control()), &mut slots, 0)
        .unwrap();
    assert!(
        job.awaits_mail(),
        "a former owner's completion cannot release this graph"
    );
    job.run_said(message(first, pending.control()), &mut slots, 0)
        .unwrap();
    assert!(
        job.awaits_mail(),
        "an older checkpoint cannot release the current wait"
    );
    job.run_said(message(latest, pending.control()), &mut slots, 0)
        .unwrap();
    assert!(!job.awaits_mail());
}

// What `after` changes of `before`, which is the checkpoint the database
// holds or will once what is queued is written; `None` when nothing says
// what it holds. Both are in key order.
fn between(before: Option<&GraphCheckpoint>, after: &GraphCheckpoint) -> Delta {
    let mut out = Delta {
        whole: before.is_none(),
        ..Delta::default()
    };
    out.compare(before.unwrap_or(&GraphCheckpoint::default()), after);
    out
}

#[test]
fn a_change_queued_behind_another_writes_both_as_one() {
    let first = rows(&full());
    let mut second = first.clone();
    second.members.pop();
    second.coverage[0].verified = true;
    let mut third = second.clone();
    third.coverage[0].verified = false;
    let mut queued = between(Some(&first), &second);
    queued.absorb(between(Some(&second), &third));
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
    let mut whole = between(None, &first);
    whole.absorb(between(Some(&first), &second));
    assert!(whole.whole);
    queued.absorb(between(None, &third));
    assert_eq!(queued, between(None, &third));
    assert!(!between(Some(&GraphCheckpoint::default()), &first).is_empty());
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
