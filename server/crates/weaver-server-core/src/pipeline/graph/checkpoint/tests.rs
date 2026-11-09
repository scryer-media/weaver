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
    CheckpointError, FileState, HoldRecord, Pending, Snapshot, envelope_rows, hold_rows,
    member_rows_built, node_rows, part_rows, root_row, snapshot,
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
        fronted: false,
        refuted: None,
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
    root.members[2].refuted = Some(Kind::Split);
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
            pending_verifications: Vec::new(),
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
        facts.pending_verifications.clone(),
    )
}

// The borrowed full-snapshot encoder, independent of incremental owned conversions.
pub(super) fn borrowed_node_rows(
    node: &NodeRecord,
    files: impl Fn(MemberId) -> FileState,
    password: Option<i64>,
) -> GraphCheckpoint {
    let id = i64::from(node.node.0);
    let mut out = GraphCheckpoint::default();
    if let (Some(kind), Some(facts)) = (node.kind, &node.facts) {
        let volumes: Vec<(u32, u32)> = node
            .volumes
            .iter()
            .map(|(volume, member)| (volume.0, member.0))
            .collect();
        out.nodes.push(super::NodeRow {
            node: id,
            parent: node.parent.map(|parent| i64::from(parent.0)),
            parent_member: node.volumes.first().map(|(_, member)| i64::from(member.0)),
            volumes: rmp_serde::to_vec(&volumes).ok(),
            window: node
                .window
                .map(|window| (window.start as i64, window.end as i64)),
            kind: kind.to_string(),
            state: super::state_name(node.state).to_owned(),
            reason: node.reason.map(super::why_text),
            facts_version: i64::from(facts.version),
            facts: facts.bytes.clone(),
            winning_password: password,
        });
        out.parts.extend(facts.parts.iter().map(|part| PartRow {
            node: id,
            part: part.key as i64,
            bytes: Arc::clone(&part.bytes),
        }));
    }
    out.crypt
        .extend(node.crypt.iter().map(|crypt| super::CryptRow {
            node: id,
            member: i64::from(crypt.member.0),
            run_start: crypt.run_start,
            seed: crypt.seed.to_vec(),
        }));
    out.members.extend(node.members.iter().map(|member| {
        let state = files(member.member);
        super::MemberRow {
            node: id,
            member: i64::from(member.member.0),
            name: member.name.clone(),
            output_name: member.output_name.clone(),
            crc32: member.crc32.map(i64::from),
            length: member.length.map(|length| length as i64),
            closed: member.closed,
            bound: member.bound_to.map(|object| i64::from(object.0)),
            owner: member.owner.map(super::pair),
            given_up: member.given_up.map(super::pair),
            buried: member.buried,
            undecided: member.undecided,
            has_file: state.has_file,
            on_disk: state.on_disk,
            refused: state.refused.map(super::why_text),
            copy_of: member.copy_of.map(|into| i64::from(into.0)),
            refuted_kind: member.refuted.map(|kind| kind.to_string()),
        }
    }));
    out.coverage
        .extend(node.coverage.iter().map(|run| super::CoverageRow {
            node: id,
            member: i64::from(run.member.0),
            start: run.start as i64,
            end: run.end as i64,
            provenance: super::provenance_name(run.provenance).to_owned(),
            verified: false,
        }));
    out.members.sort_by_key(Row::key);
    out.coverage.sort_by_key(Row::key);
    out.crypt.sort_by_key(Row::key);
    out.parts.sort_by_key(Row::key);
    out
}

#[test]
fn owned_record_conversions_match_the_borrowed_full_snapshot_encoder() {
    let mut taken = full();
    taken.tree.nodes[0].members[0].output_name = "renamed.bin".to_string();
    taken.tree.nodes[0].members[1].output_name.clear();
    taken.files.insert(
        (NodeId::ROOT, MemberId(0)),
        FileState {
            has_file: true,
            on_disk: Some("staged.bin".to_string()),
            refused: None,
        },
    );
    taken
        .files
        .insert((NodeId::ROOT, MemberId(1)), FileState::default());
    for node in &mut taken.tree.nodes {
        if let Some(facts) = &mut node.facts {
            facts.parts.push(Part {
                key: u64::MAX,
                bytes: Arc::from([8, 9].as_slice()),
            });
        }
    }
    for node in &taken.tree.nodes {
        let password = taken.passwords.get(&node.node).map(|index| *index as i64);
        let state = |member| {
            taken
                .files
                .get(&(node.node, member))
                .cloned()
                .unwrap_or_default()
        };
        let expected = borrowed_node_rows(node, state, password);
        let mut actual = super::own_rows(node.clone(), password);
        for member in node.members.clone() {
            let coverage = node
                .coverage
                .iter()
                .filter(|run| run.member == member.member)
                .cloned()
                .collect();
            let files = state(member.member);
            let rows = super::member_rows(i64::from(node.node.0), member, coverage, files);
            actual.members.extend(rows.members);
            actual.coverage.extend(rows.coverage);
        }
        actual.members.sort_by_key(Row::key);
        actual.coverage.sort_by_key(Row::key);
        assert_eq!(actual, expected, "node {:?}", node.node);
    }
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
        .extend(part_rows(i64::from(NodeId::ROOT.0), parts));
    out.nodes.sort_by_key(Row::key);
    out.members.sort_by_key(Row::key);
    out.coverage.sort_by_key(Row::key);
    out.crypt.sort_by_key(Row::key);
    out.parts.sort_by_key(Row::key);
    out
}

// A restore tells every loss again, so no loss comes back as one already
// told.
#[test]
fn a_snapshot_comes_back_from_its_rows() {
    let taken = full();
    let mut back = taken.clone();
    back.root.volumes[0].articles[1] = (1, ArticleFacts::Lost { damaged: false });
    assert_eq!(snapshot(&rows(&taken)), Ok(back));
}

// The same rows written to the database and read back: the kind a
// member's reading was refuted as comes back with it, so a restart after
// a refutation founds nothing of that kind from the member's name.
#[test]
fn a_snapshot_comes_back_from_the_database() {
    let db = crate::persistence::Database::open_in_memory().unwrap();
    let job = crate::jobs::ids::JobId(4281);
    db.create_active_job(&crate::ActiveJob {
        job_id: job,
        nzb_hash: [0xA5; 32],
        nzb_path: std::path::PathBuf::from("/tmp/refuted.nzb"),
        nzb_zstd: crate::ingest::compress_nzb_bytes(
            br#"<?xml version="1.0"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"/>"#,
        )
        .unwrap(),
        output_dir: std::path::PathBuf::from("/tmp/refuted"),
        created_at: 1_700_000_000,
        category: None,
        metadata: Vec::new(),
        status: "queued",
        download_state: "queued",
        post_state: "idle",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
        password_override: None,
        supplied_password_candidates: Vec::new(),
    })
    .unwrap();
    let written = rows(&full());
    db.save_graph_checkpoint(job, Arc::new(between(None, &written)))
        .unwrap();
    let read = db
        .load_graph_checkpoint(job)
        .unwrap()
        .expect("a checkpoint was written");
    let back = snapshot(&read).unwrap();
    assert_eq!(back.tree.nodes[0].members[2].refuted, Some(Kind::Split));
    assert_eq!(Ok(back), snapshot(&written));
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

// A restore keeps in the working directory what its checkpoint names, and
// nothing else: a step's file whose checkpoint never committed, a member's
// file a committed checkpoint stopped naming, a recovery's scratch and its
// repaired file go, and so does a file at the name a member waits for while
// the member's own file still sits where it sat; every member's file, every
// envelope, holds scratch and spooled part the rows name stays, and so do
// the directory's marker and the run's cache.
#[test]
fn a_restore_removes_what_its_checkpoint_does_not_name() {
    use weaver_graph::names::{CACHE_DIR, INTERNAL_DIR};
    let dir = tempfile::tempdir().unwrap();
    let mut taken = full();
    taken.tree.nodes[1].members[0].output_name = "inner/inner.bin".to_owned();
    let named = [
        ".weaver-cache/article-cache-1/body".to_owned(),
        format!("{INTERNAL_DIR}/graph-envelope-1-0"),
        format!("{INTERNAL_DIR}/graph-envelope-1-1-1"),
        format!("{INTERNAL_DIR}/graph-holds-0.scratch"),
        format!("{INTERNAL_DIR}/graph-holds-1.scratch"),
        format!("{INTERNAL_DIR}/graph-waiting-0-1"),
        format!("{INTERNAL_DIR}/held-0-1.uu"),
        crate::jobs::working_dir::WORKING_DIR_MARKER.to_owned(),
        "inner/inner.bin".to_owned(),
    ];
    let unnamed = [
        format!("{INTERNAL_DIR}/graph-envelope-1-2"),
        format!("{INTERNAL_DIR}/graph-holds-2.scratch"),
        format!("{INTERNAL_DIR}/graph-recovery-0-3.repaired"),
        format!("{INTERNAL_DIR}/graph-recovery-0-3/set.par2"),
        format!("{INTERNAL_DIR}/graph-waiting-0-2"),
        "inner/stray.bin".to_owned(),
        "set.par2".to_owned(),
        "set.part1.rar".to_owned(),
        "stray/deeper/file.bin".to_owned(),
    ];
    for name in named.iter().chain(&unnamed) {
        let path = dir.path().join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"bytes").unwrap();
    }
    assert!(named[0].starts_with(CACHE_DIR));
    let mut rows = rows(&taken);
    rows.held_parts
        .push(crate::jobs::graph_checkpoint::HeldPartRow {
            file: 0,
            ordinal: 1,
            decoded_len: 5,
            spool_path: format!("{INTERNAL_DIR}/held-0-1.uu"),
            article: None,
        });
    let restored = snapshot(&rows).unwrap();
    crate::pipeline::graph::restore::sweep_unnamed(dir.path(), &restored, &rows.held_parts)
        .unwrap();
    fn files(root: &std::path::Path, at: &std::path::Path, into: &mut Vec<String>) {
        for entry in std::fs::read_dir(root.join(at)).unwrap() {
            let entry = entry.unwrap();
            let name = at.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                files(root, &name, into);
            } else {
                into.push(name.to_str().unwrap().replace('\\', "/"));
            }
        }
    }
    let mut left = Vec::new();
    files(dir.path(), std::path::Path::new(""), &mut left);
    left.sort();
    assert_eq!(left, named);
    assert!(!dir.path().join("stray").exists());
}

// A directory member the checkpoint names stays, but what is in it is the
// job's only as far as the checkpoint names it: a file a committed
// checkpoint took back from it, or one written after the last that
// committed, goes like any other.
#[test]
fn a_restore_sweeps_inside_a_directory_its_checkpoint_names() {
    let dir = tempfile::tempdir().unwrap();
    let mut taken = full();
    taken.tree.nodes[1].members[0].output_name = "folder/".to_owned();
    std::fs::create_dir_all(dir.path().join("folder/deeper")).unwrap();
    std::fs::write(dir.path().join("folder/obsolete.txt"), b"bytes").unwrap();
    std::fs::write(dir.path().join("folder/deeper/late.bin"), b"bytes").unwrap();
    let rows = rows(&taken);
    let restored = snapshot(&rows).unwrap();
    crate::pipeline::graph::restore::sweep_unnamed(dir.path(), &restored, &rows.held_parts)
        .unwrap();
    assert!(dir.path().join("folder").is_dir());
    assert!(!dir.path().join("folder/obsolete.txt").exists());
    assert!(!dir.path().join("folder/deeper").exists());
}

// One archive names the directory "Folder/" and the file "folder/b.bin"
// in it (a Windows-made archive whose entries disagree in case). On a
// case-insensitive disk the file is written into the one directory, which
// sits on disk as "Folder". The checkpoint names both; a restore must keep
// the file. Only meaningful where the disk folds case.
#[test]
fn a_restore_keeps_a_named_file_whose_directory_differs_in_case() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("Folder")).unwrap();
    std::fs::write(dir.path().join("Folder/b.bin"), b"bytes").unwrap();
    if !dir.path().join("folder/b.bin").exists() {
        // A case-sensitive disk keeps two paths; nothing to test.
        return;
    }
    let mut taken = full();
    taken.tree.nodes[1].members[0].output_name = "Folder/".to_owned();
    taken.tree.nodes[1].members.push(member(1, "folder/b.bin"));
    taken.files.insert(
        (NodeId(1), MemberId(1)),
        FileState {
            has_file: true,
            on_disk: None,
            refused: None,
        },
    );
    let rows = rows(&taken);
    let restored = snapshot(&rows).unwrap();
    crate::pipeline::graph::restore::sweep_unnamed(dir.path(), &restored, &rows.held_parts)
        .unwrap();
    assert!(dir.path().join("Folder").is_dir());
    assert!(
        dir.path().join("Folder/b.bin").exists(),
        "the restore removed a file its checkpoint names"
    );
}

// A member's file the checkpoint says waits to move, gone from where it sat
// and found at the name it wants, moved before the checkpoint that would
// have said so: the restore keeps it there, opens it there, and the name it
// sat at stays empty.
#[test]
fn a_restore_keeps_a_file_that_moved_before_its_checkpoint_said_so() {
    use weaver_graph::names::INTERNAL_DIR;
    let dir = tempfile::tempdir().unwrap();
    let taken = full();
    let waiting = format!("{INTERNAL_DIR}/graph-waiting-0-1");
    std::fs::write(dir.path().join("set.par2"), b"bytes").unwrap();
    let rows = rows(&taken);
    let restored = snapshot(&rows).unwrap();
    assert_eq!(
        crate::pipeline::graph::namespace::restored_at(
            dir.path(),
            "set.par2",
            Some(waiting.as_str())
        )
        .unwrap(),
        "set.par2"
    );
    crate::pipeline::graph::restore::sweep_unnamed(dir.path(), &restored, &rows.held_parts)
        .unwrap();
    assert!(dir.path().join("set.par2").exists());
    assert!(!dir.path().join(&waiting).exists());

    std::fs::create_dir_all(dir.path().join(INTERNAL_DIR)).unwrap();
    std::fs::write(dir.path().join(&waiting), b"bytes").unwrap();
    assert_eq!(
        crate::pipeline::graph::namespace::restored_at(
            dir.path(),
            "set.par2",
            Some(waiting.as_str())
        )
        .unwrap(),
        waiting
    );
}

#[cfg(unix)]
#[test]
fn a_restore_inspection_error_stops_cleanup_before_any_file_is_removed() {
    use weaver_graph::names::INTERNAL_DIR;
    let dir = tempfile::tempdir().unwrap();
    let taken = full();
    std::os::unix::fs::symlink(INTERNAL_DIR, dir.path().join(INTERNAL_DIR)).unwrap();
    std::fs::write(dir.path().join("set.par2"), b"destination").unwrap();
    std::fs::write(
        dir.path().join("unclaimed.bin"),
        b"preserve until inspected",
    )
    .unwrap();

    assert!(crate::pipeline::graph::restore::sweep_unnamed(dir.path(), &taken, &[]).is_err());
    assert_eq!(
        std::fs::read(dir.path().join("set.par2")).unwrap(),
        b"destination"
    );
    assert_eq!(
        std::fs::read(dir.path().join("unclaimed.bin")).unwrap(),
        b"preserve until inspected"
    );
    assert!(
        std::fs::symlink_metadata(dir.path().join(INTERNAL_DIR))
            .unwrap()
            .file_type()
            .is_symlink()
    );
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
fn touched_flags_merge_every_combination_without_losing_other_sources() {
    let flags = |bits| super::Touched {
        own: bits & 1 != 0,
        members: bits & 2 != 0,
        files: bits & 4 != 0,
        kept: bits & 8 != 0,
    };
    for first in 0..16 {
        for second in 0..16 {
            let mut merged = flags(first);
            merged.merge(flags(second));
            assert_eq!(merged, flags(first | second));
        }
    }
}

#[test]
fn the_touched_visitor_keeps_sources_separate_until_the_caller_merges_them() {
    let (job, _dir, _) = fifty_members();
    let now = job.generations();
    let collect = |at| {
        let mut all: BTreeMap<NodeId, super::Touched> = BTreeMap::new();
        job.visit_touched_since(at, |node, touched| {
            all.entry(node).or_default().merge(touched);
        });
        all
    };
    assert!(collect(now).is_empty());
    let tree = collect(super::Generations { tree: 0, ..now });
    assert!(!tree.is_empty());
    assert!(tree.values().all(|flags| !flags.files && !flags.kept));
    let files = collect(super::Generations { files: 0, ..now });
    assert!(!files.is_empty());
    assert!(files.values().all(|flags| *flags
        == super::Touched {
            files: true,
            ..super::Touched::default()
        }));
    let both = collect(super::Generations {
        tree: 0,
        files: 0,
        ..now
    });
    assert!(both[&NodeId::ROOT].members && both[&NodeId::ROOT].files);
}

#[test]
fn the_first_capture_deletes_restored_node_and_member_ids_no_tree_can_have() {
    let (job, _dir) = long_post(20);
    let fresh = Pending::default();
    assert!(fresh.put(&job, Vec::new()));
    let mut rows = held(&fresh.take().0.unwrap());
    let root = i64::from(NodeId::ROOT.0);
    let member = rows.members[0].clone();
    let coverage = rows.coverage[0].clone();
    let node = rows.nodes[0].clone();
    let invalid = [-1, i64::from(u32::MAX) + 1];
    for id in invalid {
        rows.nodes.push(super::NodeRow {
            node: id,
            ..node.clone()
        });
        rows.members.push(super::MemberRow {
            node: id,
            ..member.clone()
        });
        rows.coverage.push(super::CoverageRow {
            node: id,
            ..coverage.clone()
        });
        rows.members.push(super::MemberRow {
            member: id,
            ..member.clone()
        });
        rows.coverage.push(super::CoverageRow {
            member: id,
            ..coverage.clone()
        });
    }
    let pending = Pending::default();
    pending.written(rows);
    assert!(pending.put(&job, Vec::new()));
    let delta = pending.take().0.unwrap();
    assert_eq!(delta.rows(), 10, "{delta:?}");
    for id in invalid {
        assert_eq!(delta.nodes.get(&id), Some(&None));
        assert_eq!(delta.members.get(&(id, member.member)), Some(&None));
        assert_eq!(
            delta.coverage.get(&(id, coverage.member, coverage.start)),
            Some(&None)
        );
        assert_eq!(delta.members.get(&(root, id)), Some(&None));
        assert_eq!(delta.coverage.get(&(root, id, coverage.start)), Some(&None));
    }
    assert!(!pending.put(&job, Vec::new()));
    assert!(pending.take().0.is_none());
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

// A post of fifty files of two 64-byte articles each, every first article
// in.
fn fifty_members() -> (GraphJob, tempfile::TempDir, Vec<RootFile>) {
    let files: Vec<RootFile> = (0..50)
        .map(|index| file(&format!("member{index:02}.bin"), &[64, 64]))
        .collect();
    let (mut job, dir, _) = job_with(&files, Vec::new());
    for volume in 0..50 {
        arrive(&mut job, volume, 0);
    }
    (job, dir, files)
}

// The article `ordinal` of file `volume` arrives.
fn arrive(job: &mut GraphJob, volume: u32, ordinal: u32) {
    let offset = u64::from(ordinal) * 64;
    job.intake(
        decoded(volume, ordinal, offset, &[volume as u8; 64], Some(128)),
        &mut RepairSlots::default(),
        1,
    )
    .unwrap();
}

// The root member the file `volume` was posted as.
fn root_member(job: &GraphJob, volume: u32) -> i64 {
    let name = format!("member{volume:02}.bin");
    let namespace = job.tree().namespace(NodeId::ROOT).unwrap();
    let member = namespace
        .records()
        .find(|record| record.name == name)
        .unwrap();
    i64::from(member.id.0)
}

// One article for one member of fifty: the checkpoint builds that member's
// rows and no other's, and writes only its rows beside the root's facts.
#[test]
fn a_step_builds_and_writes_only_the_member_it_changed() {
    let (mut job, _dir, _) = fifty_members();
    let pending = Pending::default();
    assert!(pending.put(&job, Vec::new()));
    assert!(pending.take().0.unwrap().whole);

    arrive(&mut job, 49, 1);
    let built = member_rows_built();
    assert!(pending.put(&job, Vec::new()));
    assert_eq!(
        member_rows_built() - built,
        1,
        "one member's rows are built"
    );

    let step = pending.take().0.unwrap();
    let member = root_member(&job, 49);
    let root = i64::from(NodeId::ROOT.0);
    assert!(
        !step.members.is_empty() && !step.coverage.is_empty(),
        "{step:?}"
    );
    assert!(
        step.members.keys().all(|key| *key == (root, member)),
        "{:?}",
        step.members.keys()
    );
    assert!(
        step.coverage
            .keys()
            .all(|(node, of, _)| (*node, *of) == (root, member)),
        "{:?}",
        step.coverage.keys()
    );
    assert!(step.nodes.is_empty(), "{:?}", step.nodes.keys());
    assert!(step.envelopes.is_empty() && step.crypt.is_empty() && step.holds.is_empty());
    assert!(
        step.parts.keys().all(|(node, _)| *node == root),
        "only the root's facts parts: {:?}",
        step.parts.keys()
    );
}

// Three steps, each one member's, written over the whole job: the rows read
// back are a fresh checkpoint's, and the job restored from them has every
// one of the fifty members as the job does.
#[test]
fn a_restore_after_single_member_steps_has_every_member() {
    let (mut job, dir, files) = fifty_members();
    let pending = Pending::default();
    assert!(pending.put(&job, Vec::new()));
    let mut database = pending.take().0.unwrap();
    for volume in [49, 7, 23] {
        arrive(&mut job, volume, 1);
        assert!(pending.put(&job, Vec::new()));
        database.absorb(pending.take().0.unwrap());
    }
    let read = snapshot(&held(&database)).unwrap();
    let fresh = Pending::default();
    assert!(fresh.put(&job, Vec::new()));
    assert_eq!(read, snapshot(&held(&fresh.take().0.unwrap())).unwrap());

    let (restored, _mail) = restored(&read, &files, dir.path());
    let members = |job: &GraphJob| {
        let record = job.tree().record(NodeId::ROOT).unwrap();
        (record.members, record.coverage)
    };
    let (now, coverage) = members(&job);
    assert_eq!(now.len(), 50);
    assert_eq!(members(&restored.job), (now, coverage));
    for volume in 0..50 {
        let member = MemberId(root_member(&job, volume) as u32);
        assert_eq!(
            restored.job.writer().file_state(NodeId::ROOT, member),
            job.writer().file_state(NodeId::ROOT, member),
            "member {volume}"
        );
    }
}

// The job a restore makes from `read`, in `dir`, with the mail its runs
// write to, which must outlive it.
fn restored(
    read: &Snapshot,
    files: &[RootFile],
    dir: &std::path::Path,
) -> (
    crate::pipeline::graph::job::restore::Restored,
    crate::pipeline::graph::engine::RunMail,
) {
    use weaver_graph::holds::{HoldBudgets, HoldsAccountant};

    use crate::operations::disk::CapacityReader;
    use crate::pipeline::graph::engine::RunMail;
    use crate::pipeline::graph::job::Storage;
    use crate::pipeline::graph::limits::Caps;

    let mail = RunMail::default();
    let restored = GraphJob::restore(
        read,
        files,
        2,
        Storage {
            working_dir: dir.to_owned(),
            output: None,
            accountant: Arc::new(HoldsAccountant::new(HoldBudgets {
                resident_limit: u64::MAX,
                scratch_total: u64::MAX,
                scratch_ceiling: u64::MAX,
                disk_reserve: 0,
            })),
            free_disk: CapacityReader::unknown(),
            slots: 1,
            mail: mail.sender(),
            passwords: Vec::new(),
            caps: Caps::unbounded(),
            nesting: u32::MAX,
            hardware: Default::default(),
            metrics: None,
        },
        0,
    )
    .unwrap();
    (restored, mail)
}

// A member whose first article came damaged closes before its front says
// what it is, and its close decides it a leaf. A checkpoint can still name
// it undecided, as it names a member an object gave back, whose parse a
// restore does not keep; the damaged front reads back as damage it holds,
// and no lost article is told again. The close the restore tells again
// decides it as the first close did, or every run waiting on what the
// namespace still decides waits for good.
#[test]
fn a_restored_member_that_closed_before_its_front_said_is_decided() {
    let files = vec![file("lonely.bin", &[64, 64])];
    let (mut job, dir, _) = job_with(&files, Vec::new());
    let mut damaged = decoded(0, 0, 0, &[3; 64], Some(128));
    damaged.intact = false;
    job.intake(damaged, &mut RepairSlots::default(), 1).unwrap();
    arrive(&mut job, 0, 1);
    let member = job
        .tree()
        .namespace(NodeId::ROOT)
        .unwrap()
        .records()
        .find(|record| record.name == "lonely.bin")
        .unwrap()
        .id;
    assert!(!job.tree().is_undecided(NodeId::ROOT, member));

    let pending = Pending::default();
    assert!(pending.put(&job, Vec::new()));
    let mut read = snapshot(&held(&pending.take().0.unwrap())).unwrap();
    let record = read
        .tree
        .nodes
        .iter_mut()
        .find(|node| node.node == NodeId::ROOT)
        .unwrap()
        .members
        .iter_mut()
        .find(|record| record.member == member)
        .unwrap();
    assert!(record.closed);
    record.undecided = true;

    let (restored, _mail) = restored(&read, &files, dir.path());
    assert!(!restored.job.tree().is_undecided(NodeId::ROOT, member));
    assert!(!restored.job.tree().deciding(NodeId::ROOT));
}

// Keep the paused clock under the test's control while SQLite works on its
// own thread. An idle runtime would otherwise advance to the next timer.
struct ClockHeld(tokio::task::JoinHandle<()>);

impl ClockHeld {
    fn new() -> Self {
        Self(tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        }))
    }
}

impl Drop for ClockHeld {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn checkpoint_database() -> (crate::Database, crate::jobs::ids::JobId) {
    let db = crate::Database::open_in_memory().unwrap();
    let job = crate::jobs::ids::JobId(4282);
    db.create_active_job(&crate::ActiveJob {
        job_id: job,
        nzb_hash: [0xA6; 32],
        nzb_path: "checkpoint.nzb".into(),
        nzb_zstd: crate::ingest::compress_nzb_bytes(
            br#"<?xml version="1.0"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"/>"#,
        )
        .unwrap(),
        output_dir: "checkpoint".into(),
        created_at: 1_700_000_000,
        category: None,
        metadata: Vec::new(),
        status: "queued",
        download_state: "queued",
        post_state: "idle",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
        password_override: None,
        supplied_password_candidates: Vec::new(),
    })
    .unwrap();
    (db, job)
}

fn writes(db: &crate::Database) -> usize {
    db.graph_checkpoint_writes
        .load(std::sync::atomic::Ordering::Relaxed)
}

#[tokio::test(start_paused = true)]
async fn small_steps_share_one_transaction_and_restore_every_members_bytes() {
    let _clock = ClockHeld::new();
    let (db, id) = checkpoint_database();
    let (mut job, dir, files) = fifty_members();
    let pending = Pending::default();
    let (mail, _heard) = tokio::sync::mpsc::unbounded_channel();
    let now = tokio::time::Instant::now();
    for volume in 0..50 {
        arrive(&mut job, volume, 1);
        pending.put(&job, Vec::new());
        pending.queue(&db, id, &mail, false);
        db.flush_write_queue().await.unwrap();
    }
    assert_eq!(
        writes(&db),
        0,
        "an idle database does not flush small steps"
    );
    assert_eq!(tokio::time::Instant::now(), now);
    pending.queue(&db, id, &mail, true);
    db.flush_write_queue().await.unwrap();
    assert_eq!(writes(&db), 1);
    let read = snapshot(&db.load_graph_checkpoint(id).unwrap().unwrap()).unwrap();
    let (restored, _mail) = restored(&read, &files, dir.path());
    let fresh = Pending::default();
    fresh.put(&job, Vec::new());
    assert_eq!(read, snapshot(&held(&fresh.take().0.unwrap())).unwrap());
    for volume in 0..50 {
        let member = MemberId(root_member(&job, volume) as u32);
        assert_eq!(
            restored.job.writer().file_state(NodeId::ROOT, member),
            job.writer().file_state(NodeId::ROOT, member)
        );
        assert_eq!(
            std::fs::read(dir.path().join(format!("member{volume:02}.bin"))).unwrap(),
            vec![volume as u8; 128]
        );
    }
}

#[tokio::test(start_paused = true)]
async fn the_oldest_small_step_writes_once_when_its_age_is_due() {
    let _clock = ClockHeld::new();
    let (db, id) = checkpoint_database();
    let (mut job, _dir) = long_post(1);
    let pending = Pending::default();
    let (mail, _heard) = tokio::sync::mpsc::unbounded_channel();
    pending.put(&job, Vec::new());
    pending.queue(&db, id, &mail, false);
    db.flush_write_queue().await.unwrap();
    assert_eq!(writes(&db), 0);
    tokio::time::advance(std::time::Duration::from_secs(4)).await;
    intake(&mut job, 1);
    pending.put(&job, Vec::new());
    pending.queue(&db, id, &mail, false);
    let timer = pending.slot().timer.take().unwrap();
    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    timer.await.unwrap();
    db.flush_write_queue().await.unwrap();
    assert_eq!(writes(&db), 1);
    let committed = pending.slot().committed;
    assert_eq!(committed, pending.stamp());
    pending.queue(&db, id, &mail, false);
    db.flush_write_queue().await.unwrap();
    assert_eq!(writes(&db), 1);
}

#[tokio::test(start_paused = true)]
async fn age_and_force_share_one_write_behind_an_in_flight_checkpoint() {
    let _clock = ClockHeld::new();
    for age_first in [false, true] {
        let (db, id) = checkpoint_database();
        let (mut job, _dir) = long_post(1);
        let pending = Pending::default();
        let (mail, mut heard) = tokio::sync::mpsc::unbounded_channel();
        pending.put(&job, Vec::new());
        // Hold the first immutable payload after take, before its SQL and
        // acknowledgement, without blocking the runtime that drives the timer.
        let (first, first_stamp) = pending.take();
        let first = first.unwrap();
        let writer = pending.clone();
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        db.try_queue_write("held_graph_checkpoint", move |db| {
            entered.send(()).unwrap();
            released.recv().unwrap();
            let whole = first.whole;
            db.save_graph_checkpoint(id, Arc::new(first))?;
            writer.committed(first_stamp, whole);
            Ok(())
        })
        .unwrap();
        waiting.await.unwrap();

        for ordinal in 1..4 {
            intake(&mut job, ordinal);
            pending.put(&job, Vec::new());
            pending.queue(&db, id, &mail, false);
        }
        let stamp = pending.stamp();
        // Keep the timer handle so taking the queued delta cannot cancel
        // the age callback whose ordering this test controls.
        let timer = pending.slot().timer.take().unwrap();
        if age_first {
            tokio::time::advance(super::CHECKPOINT_AGE).await;
            timer.await.unwrap();
            assert_eq!(pending.await_commit(stamp, id.0, &mail, &db), None);
        } else {
            assert_eq!(pending.await_commit(stamp, id.0, &mail, &db), None);
            tokio::time::advance(super::CHECKPOINT_AGE).await;
            timer.await.unwrap();
        }
        assert_eq!(writes(&db), 0);
        assert!(heard.try_recv().is_err());
        release.send(()).unwrap();
        let said = heard.recv().await.unwrap();
        assert_eq!(said.generation, stamp);
        assert!(
            matches!(said.heard, super::Heard::Written { stamp: Some(written), .. } if written == stamp)
        );
        db.flush_write_queue().await.unwrap();
        assert_eq!(writes(&db), 2, "age_first={age_first}");
        assert!(heard.try_recv().is_err());
        let fresh = Pending::default();
        fresh.put(&job, Vec::new());
        assert_eq!(
            snapshot(&db.load_graph_checkpoint(id).unwrap().unwrap()).unwrap(),
            snapshot(&held(&fresh.take().0.unwrap())).unwrap()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn awaiting_a_stamp_forces_its_write_without_advancing_time() {
    let _clock = ClockHeld::new();
    let (db, id) = checkpoint_database();
    let (job, _dir) = long_post(1);
    let pending = Pending::default();
    let (mail, mut heard) = tokio::sync::mpsc::unbounded_channel();
    pending.put(&job, Vec::new());
    pending.queue(&db, id, &mail, false);
    db.flush_write_queue().await.unwrap();
    assert_eq!(writes(&db), 0);
    let now = tokio::time::Instant::now();
    let stamp = pending.stamp();
    assert_eq!(pending.await_commit(stamp, id.0, &mail, &db), None);
    let said = heard.recv().await.unwrap();
    assert!(
        matches!(said.heard, super::Heard::Written { stamp: Some(written), .. } if written == stamp)
    );
    assert_eq!(writes(&db), 1);
    assert_eq!(tokio::time::Instant::now(), now);
}

#[tokio::test(start_paused = true)]
async fn dirty_bytes_at_the_barrier_force_a_write() {
    let _clock = ClockHeld::new();
    let (db, id) = checkpoint_database();
    // The post declares one byte per article; each actually covers 64.
    let (mut job, _dir, _) = job_with(&[file("member00.bin", &[1, 1])], Vec::new());
    arrive(&mut job, 0, 0);
    let pending = Pending::default();
    let (mail, _heard) = tokio::sync::mpsc::unbounded_channel();
    pending.put(&job, Vec::new());
    pending.slot().dirty_bytes = super::CHECKPOINT_BYTES - 64;
    pending.queue(&db, id, &mail, false);
    db.flush_write_queue().await.unwrap();
    assert_eq!(writes(&db), 0);
    arrive(&mut job, 0, 1);
    pending.put(&job, Vec::new());
    assert_eq!(pending.slot().dirty_bytes, super::CHECKPOINT_BYTES);
    pending.queue(&db, id, &mail, false);
    db.flush_write_queue().await.unwrap();
    assert_eq!(writes(&db), 1);
    assert_eq!(pending.slot().dirty_bytes, 0);
}

#[test]
fn dirty_coverage_counts_new_ranges_without_recounting_splits_or_provenance() {
    let rows = |ranges: &[(i64, i64)]| -> Vec<super::CoverageRow> {
        ranges
            .iter()
            .map(|&(start, end)| super::CoverageRow {
                node: 7,
                member: 3,
                start,
                end,
                provenance: "fresh".into(),
                verified: false,
            })
            .collect()
    };
    let before = rows(&[(10, 20), (30, 40)]);
    for (ranges, expected) in [
        (vec![(0, 50)], 30),
        (vec![(10, 15), (15, 20), (30, 40)], 0),
        (vec![(0, 5), (12, 18), (35, 45), (50, 60)], 20),
        (vec![(0, 10), (20, 30), (40, 50)], 30),
        (vec![], 0),
    ] {
        let mut after = rows(&ranges);
        for row in &mut after {
            row.provenance = "repaired".into();
            row.verified = true;
        }
        assert_eq!(
            super::added_coverage(&before, &after),
            expected,
            "{ranges:?}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_dropped_writer_leaves_only_committed_coverage_to_restore() {
    let _clock = ClockHeld::new();
    let (db, id) = checkpoint_database();
    let (mut job, dir, files) = fifty_members();
    let pending = Pending::default();
    let (mail, _heard) = tokio::sync::mpsc::unbounded_channel();
    pending.put(&job, Vec::new());
    pending.queue(&db, id, &mail, true);
    db.flush_write_queue().await.unwrap();
    let committed = db.load_graph_checkpoint(id).unwrap().unwrap();
    arrive(&mut job, 7, 1);
    pending.put(&job, Vec::new());
    pending.queue(&db, id, &mail, false);
    let timer = pending.slot().timer.as_ref().unwrap().abort_handle();
    drop(pending);
    tokio::time::advance(super::CHECKPOINT_AGE).await;
    while !timer.is_finished() {
        tokio::task::yield_now().await;
    }
    db.flush_write_queue().await.unwrap();
    assert_eq!(writes(&db), 1);
    let read = db.load_graph_checkpoint(id).unwrap().unwrap();
    assert_eq!(read, committed);
    let (restored, _mail) = restored(&snapshot(&read).unwrap(), &files, dir.path());
    assert!(
        restored
            .job
            .tree()
            .record(NodeId::ROOT)
            .unwrap()
            .coverage
            .iter()
            .all(|coverage| coverage.end == 64)
    );
}

// A job's steps between checkpoints take none: the write they join waits
// on the first of them, and the run mail hears once it is due, not before.
#[tokio::test(start_paused = true)]
async fn steps_take_no_checkpoint_until_their_write_is_due() {
    use crate::pipeline::graph::engine::Heard;
    let _clock = ClockHeld::new();
    let (db, id) = checkpoint_database();
    let (mut job, _dir) = long_post(1);
    let pending = Pending::default();
    let (mail, mut heard) = tokio::sync::mpsc::unbounded_channel();
    assert!(
        pending.stepped(id, &mail),
        "the first checkpoint is due at once"
    );
    pending.put(&job, Vec::new());
    let first = tokio::time::Instant::now();
    for ordinal in 1..20 {
        intake(&mut job, ordinal);
        pending.delivered(1_000);
        assert!(!pending.stepped(id, &mail));
    }
    assert_eq!(pending.tally().checkpoints, 1);
    assert!(heard.try_recv().is_err());
    tokio::time::advance(super::CHECKPOINT_AGE).await;
    let said = heard.recv().await.unwrap();
    assert!(
        matches!(&said.heard, Heard::CheckpointDue { control } if control.same(&pending.control()))
    );
    assert_eq!(tokio::time::Instant::now(), first + super::CHECKPOINT_AGE);
    assert!(pending.stepped(id, &mail));
    pending.put(&job, Vec::new());
    assert!(!pending.unread_steps());
    pending.queue(&db, id, &mail, false);
    db.flush_write_queue().await.unwrap();
    assert_eq!(writes(&db), 1, "one write takes every step");
    assert_eq!(pending.tally().checkpoints, 2);
    let read = snapshot(&db.load_graph_checkpoint(id).unwrap().unwrap()).unwrap();
    let fresh = Pending::default();
    fresh.put(&job, Vec::new());
    assert_eq!(read, snapshot(&held(&fresh.take().0.unwrap())).unwrap());
    assert!(heard.try_recv().is_err());
}

// A step whose bytes take the job's dirty bytes to the threshold is due at
// once, with no wake.
#[tokio::test(start_paused = true)]
async fn a_step_that_reaches_the_byte_threshold_is_due_at_once() {
    let _clock = ClockHeld::new();
    let (mut job, _dir) = long_post(1);
    let pending = Pending::default();
    let (mail, _heard) = tokio::sync::mpsc::unbounded_channel();
    let id = crate::jobs::ids::JobId(0);
    pending.put(&job, Vec::new());
    pending.take();
    intake(&mut job, 1);
    pending.delivered(super::CHECKPOINT_BYTES - 1);
    assert!(!pending.stepped(id, &mail));
    intake(&mut job, 2);
    pending.delivered(1);
    assert!(pending.stepped(id, &mail));
}
// A post of four files of two 64-byte articles each, nothing in yet.
fn four_members() -> (GraphJob, tempfile::TempDir, Vec<RootFile>) {
    let files: Vec<RootFile> = (0..4)
        .map(|index| file(&format!("member{index:02}.bin"), &[64, 64]))
        .collect();
    let (job, dir, _) = job_with(&files, Vec::new());
    (job, dir, files)
}

// What a crash costs a job: the articles on disk that no write queued for
// the database names. Two are in when a checkpoint commits and two more
// arrive whose change waits for its barrier; the job a restore makes from
// the database has the first two only, and the job says the other two are
// what a crash now would fetch again.
#[tokio::test(start_paused = true)]
async fn a_crash_costs_only_the_articles_since_the_committed_checkpoint() {
    let _clock = ClockHeld::new();
    let (db, id) = checkpoint_database();
    let (mut job, dir, files) = four_members();
    arrive(&mut job, 0, 0);
    arrive(&mut job, 1, 0);
    let pending = job.pending().clone();
    let (mail, _heard) = tokio::sync::mpsc::unbounded_channel();
    pending.put(&job, Vec::new());
    pending.queue(&db, id, &mail, true);
    db.flush_write_queue().await.unwrap();
    assert_eq!(writes(&db), 1);
    assert_eq!(job.on_disk(), [(0, 0), (1, 0)].into_iter().collect());
    assert_eq!(job.in_memory(), Vec::<(u32, u32)>::new());
    arrive(&mut job, 2, 0);
    arrive(&mut job, 3, 0);
    pending.put(&job, Vec::new());
    pending.queue(&db, id, &mail, false);
    db.flush_write_queue().await.unwrap();
    assert_eq!(writes(&db), 1, "the change waits for its barrier");
    assert_eq!(job.in_memory(), vec![(2, 0), (3, 0)]);
    let read = snapshot(&db.load_graph_checkpoint(id).unwrap().unwrap()).unwrap();
    let (restored, _mail) = restored(&read, &files, dir.path());
    assert_eq!(
        restored.job.on_disk(),
        [(0, 0), (1, 0)].into_iter().collect()
    );
    assert_eq!(restored.job.in_memory(), Vec::<(u32, u32)>::new());
}

// A forced write takes every change: once it is queued a crash costs
// nothing the job has on disk, before the write has run and after.
#[tokio::test(start_paused = true)]
async fn a_forced_write_leaves_a_crash_nothing_to_fetch_again() {
    let _clock = ClockHeld::new();
    let (db, id) = checkpoint_database();
    let (mut job, dir, files) = four_members();
    arrive(&mut job, 0, 0);
    let pending = job.pending().clone();
    let (mail, _heard) = tokio::sync::mpsc::unbounded_channel();
    pending.put(&job, Vec::new());
    pending.queue(&db, id, &mail, true);
    db.flush_write_queue().await.unwrap();
    arrive(&mut job, 1, 0);
    arrive(&mut job, 2, 0);
    pending.put(&job, Vec::new());
    pending.queue(&db, id, &mail, false);
    assert_eq!(job.in_memory(), vec![(1, 0), (2, 0)]);
    pending.queue(&db, id, &mail, true);
    assert_eq!(job.in_memory(), Vec::<(u32, u32)>::new());
    db.flush_write_queue().await.unwrap();
    assert_eq!(writes(&db), 2);
    assert_eq!(job.in_memory(), Vec::<(u32, u32)>::new());
    let read = snapshot(&db.load_graph_checkpoint(id).unwrap().unwrap()).unwrap();
    let (restored, _mail) = restored(&read, &files, dir.path());
    assert_eq!(restored.job.on_disk(), job.on_disk());
}

// A plain file's article, 8 KiB of bytes its file and ordinal say.
const PIECE: u32 = 8 * 1024;
// The front a namespace keeps of each member, which no record keeps.
const FRONT: u64 = 16 * 1024;

fn piece(volume: u32, ordinal: u32) -> Vec<u8> {
    (0..PIECE)
        .map(|at| (at as u8).wrapping_mul(31) ^ (volume as u8 * 16 + ordinal as u8))
        .collect()
}

// Three plain files of six articles, the first four of each in: each is
// on disk well past its front.
fn plain_files() -> (GraphJob, tempfile::TempDir, Vec<RootFile>) {
    let files: Vec<RootFile> = (0..3)
        .map(|index| file(&format!("part{index}.mkv"), &[PIECE; 6]))
        .collect();
    let (mut job, dir, _) = job_with(&files, Vec::new());
    for volume in 0..3 {
        for ordinal in 0..4 {
            plain(&mut job, volume, ordinal);
        }
    }
    (job, dir, files)
}

fn plain(job: &mut GraphJob, volume: u32, ordinal: u32) {
    job.intake(
        decoded(
            volume,
            ordinal,
            u64::from(ordinal * PIECE),
            &piece(volume, ordinal),
            Some(u64::from(6 * PIECE)),
        ),
        &mut RepairSlots::default(),
        1,
    )
    .unwrap();
}

// The job's checkpoint as the database would hold it now.
fn checkpointed(job: &GraphJob) -> Snapshot {
    let pending = Pending::default();
    assert!(pending.put(job, Vec::new()));
    snapshot(&held(&pending.take().0.unwrap())).unwrap()
}

// What `step` read from and wrote to the job's files, and hashed.
fn touched<T>(step: impl FnOnce() -> T) -> (T, u64, u64, u64) {
    use crate::pipeline::graph::leaf::counting;
    let (read, written) = counting::so_far();
    let hashed = weaver_graph::evidence::counting::hashed();
    let out = step();
    let (read_after, written_after) = counting::so_far();
    (
        out,
        read_after - read,
        written_after - written,
        weaver_graph::evidence::counting::hashed() - hashed,
    )
}

// A restart trusts its checkpoint: a restore of plain files reads none of
// their bytes to check them, hashes none and writes none. The one thing it
// reads is each member's front, which no record keeps, as far as it came;
// nothing is fetched again, and the job holds what it held.
#[test]
fn a_restore_of_plain_files_reads_only_their_fronts_and_writes_nothing() {
    let (job, dir, files) = plain_files();
    let read = checkpointed(&job);
    let ((restored, _mail), bytes_read, written, hashed) =
        touched(|| restored(&read, &files, dir.path()));
    assert_eq!(written, 0, "a restore writes nothing");
    assert_eq!(hashed, 0, "a restore hashes nothing");
    // A test build's restore ends by asking its crash oracle what is on
    // disk, which reads every arrived article back: that read is the
    // test's, not the restore's.
    let (_, oracle, _, _) = touched(|| restored.job.on_disk());
    assert_eq!(oracle, 3 * 4 * u64::from(PIECE));
    assert_eq!(
        bytes_read - oracle,
        3 * FRONT,
        "a restore reads the fronts alone"
    );
    assert!(restored.refetched.is_empty());
    assert_eq!(restored.job.on_disk(), job.on_disk());
    let coverage = |job: &GraphJob| job.tree().record(NodeId::ROOT).unwrap().coverage;
    assert_eq!(coverage(&restored.job), coverage(&job));
}

// A file cut shorter than its record says, as a disk that lost its tail
// leaves it: the restore reads no byte of it to find that, only its length,
// and the articles past the cut are fetched again. The articles that come
// again land, and the file holds every byte as it came.
#[test]
fn a_file_cut_short_before_a_restore_fetches_again_what_it_lost() {
    let (job, dir, files) = plain_files();
    let read = checkpointed(&job);
    let path = dir.path().join("part1.mkv");
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(u64::from(2 * PIECE) + 100)
        .unwrap();
    let ((restored, _mail), _, written, hashed) = touched(|| restored(&read, &files, dir.path()));
    assert_eq!(written, 0, "a restore writes nothing");
    assert_eq!(hashed, 0, "a restore hashes nothing");
    assert_eq!(
        restored.refetched,
        vec![(VolumeIndex(1), 2), (VolumeIndex(1), 3)],
        "the articles past the cut, and no other, are fetched again"
    );
    let mut job = restored.job;
    for ordinal in 2..6 {
        plain(&mut job, 1, ordinal);
    }
    let whole: Vec<u8> = (0..6).flat_map(|ordinal| piece(1, ordinal)).collect();
    assert!(
        std::fs::read(&path).unwrap() == whole,
        "the file holds every byte"
    );
}

// Every reason a checkpoint row keeps reads back as itself, and a decode
// too large keeps the sizes it carried, each one it did not say read back
// unsaid. A name it never wrote is no reason.
#[test]
fn every_reason_a_row_keeps_reads_back_with_its_sizes() {
    let too_large = |needed, allowed| Why::TooLarge { needed, allowed };
    let reasons = super::REASONS.into_iter().chain([
        too_large(Some(2 << 30), Some(1 << 30)),
        too_large(Some(2 << 30), None),
        too_large(None, Some(1 << 30)),
    ]);
    for why in reasons {
        assert_eq!(
            super::why_named(&super::why_text(why)),
            Some(why),
            "{why:?}"
        );
    }
    assert_eq!(
        super::why_text(too_large(Some(2 << 30), None)),
        "too_large:2147483648:-"
    );
    for text in [
        "too_large",
        "too_large:1",
        "unsupported:1:2",
        "too_large:x:-",
        "nothing",
    ] {
        assert_eq!(super::why_named(text), None, "{text}");
    }
}
