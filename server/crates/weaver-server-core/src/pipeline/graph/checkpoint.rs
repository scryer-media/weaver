// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A graph job's checkpoint: the tree's records, the root's facts, which
// members have files, the kept envelopes and which candidate each node was
// offered last, as the rows the database keeps. It is taken after every
// step that reached the tree, when the step's bytes are written, so it only
// ever describes bytes on disk; holds in memory are left out and fetched
// again.
//
// One writer per job: a checkpoint waiting for the database is replaced by
// the next one rather than queued behind it, so the database only ever
// writes the newest, and a slow database costs writes, never order.
//
// Nothing secret is in any of it. The tree records carry no password and no
// key, the objects' facts carry key tuples and salts but never a key, and a
// node's password is the index of its candidate, never the candidate.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use tracing::warn;
use weaver_graph::{
    ByteRange, CoverageRecord, CryptRecord, Facts, Kind, MemberId, MemberRecord, NodeId,
    NodeRecord, NodeState, ObjectId, Provenance, Run, TreeRecord, VolumeIndex, Why,
};

use super::envelope::durable::KeptRecord;
use super::root::facts::{self, RootFacts, RootFactsError};
use crate::jobs::graph_checkpoint::{
    CoverageRow, CryptRow, EnvelopeRow, GraphCheckpoint, MemberRow, NodeRow,
};
use crate::jobs::ids::JobId;
use crate::pipeline::Pipeline;

// The root row's kind: the root is the article object, not one the builder
// makes.
const ROOT_KIND: &str = "root";

// A job's checkpoint, as the graph layer reads and writes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Snapshot {
    pub(crate) tree: TreeRecord,
    pub(crate) root: RootFacts,
    // What the writer holds of each member it holds anything of.
    pub(crate) files: BTreeMap<(NodeId, MemberId), FileState>,
    pub(crate) kept: Vec<KeptRecord>,
    // The candidate each node was offered last, by index.
    pub(crate) passwords: BTreeMap<NodeId, usize>,
}

// What the writer holds of one member.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FileState {
    pub(crate) has_file: bool,
    // Why it is a leaf, when something refused to own it.
    pub(crate) refused: Option<Why>,
    // Its place among the members carrying recovery data no object here
    // reads, which is the order the job found them in.
    pub(crate) unusable_at: Option<usize>,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum CheckpointError {
    #[error("the checkpoint has no root row")]
    NoRoot,
    #[error("node {node}: {reason}")]
    Row { node: i64, reason: &'static str },
    #[error(transparent)]
    Root(#[from] RootFactsError),
}

// A checkpoint queued for the database and not written yet. The write takes
// whatever is here when it runs.
#[derive(Clone, Default)]
pub(crate) struct Pending(Arc<Mutex<Option<Arc<GraphCheckpoint>>>>);

impl Pending {
    // Leave the newest checkpoint; true when no write is queued to take it.
    fn put(&self, checkpoint: GraphCheckpoint) -> bool {
        let mut slot = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let idle = slot.is_none();
        *slot = Some(Arc::new(checkpoint));
        idle
    }

    fn take(&self) -> Option<Arc<GraphCheckpoint>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).take()
    }
}

impl Pipeline {
    // Checkpoint the job's tree as it stands now.
    pub(crate) fn checkpoint_graph_job(&mut self, job_id: JobId) {
        let Some(graph) = self.graph_jobs.get(&job_id) else {
            return;
        };
        let pending = graph.pending().clone();
        if !pending.put(rows(&graph.snapshot())) {
            return;
        }
        let writer = pending.clone();
        let queued = self
            .db
            .try_queue_job_write(job_id, "save_graph_checkpoint", move |db| {
                match writer.take() {
                    Some(checkpoint) => db.save_graph_checkpoint(job_id, checkpoint),
                    None => Ok(()),
                }
            });
        if let Err(error) = queued {
            // The next step queues the newest again.
            pending.take();
            warn!(job_id = job_id.0, error = %error, "could not queue the graph checkpoint");
        }
    }
}

pub(crate) fn rows(snapshot: &Snapshot) -> GraphCheckpoint {
    let mut out = GraphCheckpoint::default();
    out.nodes.push(NodeRow {
        node: i64::from(NodeId::ROOT.0),
        parent: None,
        parent_member: None,
        volumes: None,
        window: None,
        kind: ROOT_KIND.to_owned(),
        state: state_name(NodeState::Live).to_owned(),
        reason: None,
        facts_version: i64::from(RootFacts::VERSION),
        facts: facts::encode(&snapshot.root),
        winning_password: None,
    });
    for node in &snapshot.tree.nodes {
        let id = i64::from(node.node.0);
        if let (Some(kind), Some(facts)) = (node.kind, &node.facts) {
            let volumes: Vec<(u32, u32)> = node
                .volumes
                .iter()
                .map(|(volume, member)| (volume.0, member.0))
                .collect();
            out.nodes.push(NodeRow {
                node: id,
                parent: node.parent.map(|parent| i64::from(parent.0)),
                parent_member: node.volumes.first().map(|(_, member)| i64::from(member.0)),
                volumes: rmp_serde::to_vec(&volumes).ok(),
                window: node
                    .window
                    .map(|window| (window.start as i64, window.end as i64)),
                kind: kind.to_string(),
                state: state_name(node.state).to_owned(),
                reason: node.reason.map(|why| why_name(why).to_owned()),
                facts_version: i64::from(facts.version),
                facts: facts.bytes.clone(),
                winning_password: snapshot
                    .passwords
                    .get(&node.node)
                    .map(|index| *index as i64),
            });
        }
        for member in &node.members {
            let state = snapshot
                .files
                .get(&(node.node, member.member))
                .copied()
                .unwrap_or_default();
            out.members.push(MemberRow {
                node: id,
                member: i64::from(member.member.0),
                name: member.name.clone(),
                output_name: member.output_name.clone(),
                length: member.length.map(|length| length as i64),
                closed: member.closed,
                bound: member.bound_to.map(|object| i64::from(object.0)),
                owner: member.owner.map(pair),
                given_up: member.given_up.map(pair),
                buried: member.buried,
                undecided: member.undecided,
                has_file: state.has_file,
                refused: state.refused.map(|why| why_name(why).to_owned()),
                unusable_at: state.unusable_at.map(|at| at as i64),
            });
        }
        out.coverage
            .extend(node.coverage.iter().map(|run| CoverageRow {
                node: id,
                member: i64::from(run.member.0),
                start: run.start as i64,
                end: run.end as i64,
                provenance: provenance_name(run.provenance).to_owned(),
                verified: false,
            }));
        out.crypt.extend(node.crypt.iter().map(|crypt| CryptRow {
            node: id,
            member: i64::from(crypt.member.0),
            run_start: crypt.run_start,
            seed: crypt.seed.to_vec(),
        }));
    }
    for kept in &snapshot.kept {
        let row = |range: ByteRange, provenance| EnvelopeRow {
            node: i64::from(kept.node.0),
            volume: i64::from(kept.envelope),
            start: range.start as i64,
            end: range.end as i64,
            provenance: provenance_name(provenance).to_owned(),
            closed: kept.closed.map(|length| length as i64),
            settled: kept.settled,
        };
        // An envelope with nothing kept yet still says whether it closed.
        if kept.runs.is_empty() {
            out.envelopes
                .push(row(ByteRange::new(0, 0), Provenance::Fresh));
        }
        out.envelopes
            .extend(kept.runs.iter().map(|run| row(run.range, run.provenance)));
    }
    out
}

pub(crate) fn snapshot(rows: &GraphCheckpoint) -> Result<Snapshot, CheckpointError> {
    let root = rows
        .nodes
        .iter()
        .find(|row| row.node == i64::from(NodeId::ROOT.0))
        .filter(|row| row.kind == ROOT_KIND)
        .ok_or(CheckpointError::NoRoot)?;
    let root = facts::decode(version(root)?, &root.facts)?;
    let mut nodes: BTreeMap<NodeId, NodeRecord> = BTreeMap::new();
    let mut passwords = BTreeMap::new();
    for row in &rows.nodes {
        let node = node_id(row.node, row.node)?;
        let refuse = |reason| CheckpointError::Row {
            node: row.node,
            reason,
        };
        let mut record = NodeRecord {
            node,
            parent: None,
            volumes: Vec::new(),
            window: None,
            kind: None,
            state: NodeState::Live,
            reason: None,
            facts: None,
            members: Vec::new(),
            coverage: Vec::new(),
            envelopes: Vec::new(),
            crypt: Vec::new(),
        };
        if row.kind != ROOT_KIND {
            let kind = kind_named(&row.kind).ok_or(refuse("an unknown kind"))?;
            let volumes: Vec<(u32, u32)> = row
                .volumes
                .as_deref()
                .and_then(|bytes| rmp_serde::from_slice(bytes).ok())
                .ok_or(refuse("its volumes do not decode"))?;
            record.parent = row
                .parent
                .map(|parent| node_id(row.node, parent))
                .transpose()?;
            record.volumes = volumes
                .into_iter()
                .map(|(volume, member)| (VolumeIndex(volume), MemberId(member)))
                .collect();
            record.window = row
                .window
                .map(|(start, end)| range(row.node, start, end))
                .transpose()?;
            record.kind = Some(kind);
            record.state = state_named(&row.state).ok_or(refuse("an unknown state"))?;
            record.reason = row
                .reason
                .as_deref()
                .map(|why| why_named(why).ok_or(refuse("an unknown reason")))
                .transpose()?;
            record.facts = Some(Facts {
                kind,
                version: version(row)?,
                bytes: row.facts.clone(),
            });
        }
        if let Some(index) = row.winning_password {
            passwords.insert(
                node,
                usize::try_from(index).map_err(|_| refuse("a password index"))?,
            );
        }
        nodes.insert(node, record);
    }
    let mut files = BTreeMap::new();
    for row in &rows.members {
        let node = node_id(row.node, row.node)?;
        let refuse = |reason| CheckpointError::Row {
            node: row.node,
            reason,
        };
        let member = MemberId(u32::try_from(row.member).map_err(|_| refuse("a member id"))?);
        let id_pair = |pair: (i64, i64)| -> Result<(NodeId, VolumeIndex), CheckpointError> {
            Ok((
                node_id(row.node, pair.0)?,
                VolumeIndex(u32::try_from(pair.1).map_err(|_| refuse("a volume"))?),
            ))
        };
        let refused = row
            .refused
            .as_deref()
            .map(|why| why_named(why).ok_or(refuse("an unknown refusal")))
            .transpose()?;
        let record = MemberRecord {
            member,
            name: row.name.clone(),
            output_name: row.output_name.clone(),
            length: row
                .length
                .map(|length| u64::try_from(length).map_err(|_| refuse("a length")))
                .transpose()?,
            closed: row.closed,
            bound_to: row
                .bound
                .map(|object| {
                    u32::try_from(object)
                        .map(ObjectId)
                        .map_err(|_| refuse("an object id"))
                })
                .transpose()?,
            owner: row.owner.map(id_pair).transpose()?,
            given_up: row.given_up.map(id_pair).transpose()?,
            buried: row.buried,
            undecided: row.undecided,
        };
        nodes
            .get_mut(&node)
            .ok_or(refuse("members of a node not in the checkpoint"))?
            .members
            .push(record);
        let state = FileState {
            has_file: row.has_file,
            refused,
            unusable_at: row
                .unusable_at
                .map(|at| usize::try_from(at).map_err(|_| refuse("a recovery position")))
                .transpose()?,
        };
        if state != FileState::default() {
            files.insert((node, member), state);
        }
    }
    for row in &rows.coverage {
        let node = node_id(row.node, row.node)?;
        let refuse = |reason| CheckpointError::Row {
            node: row.node,
            reason,
        };
        let run = range(row.node, row.start, row.end)?;
        let record = CoverageRecord {
            member: MemberId(u32::try_from(row.member).map_err(|_| refuse("a member id"))?),
            start: run.start,
            end: run.end,
            provenance: provenance_named(&row.provenance).ok_or(refuse("a provenance"))?,
        };
        nodes
            .get_mut(&node)
            .ok_or(refuse("coverage of a node not in the checkpoint"))?
            .coverage
            .push(record);
    }
    for row in &rows.crypt {
        let node = node_id(row.node, row.node)?;
        let refuse = |reason| CheckpointError::Row {
            node: row.node,
            reason,
        };
        let record = CryptRecord {
            member: MemberId(u32::try_from(row.member).map_err(|_| refuse("a member id"))?),
            run_start: row.run_start,
            seed: row
                .seed
                .as_slice()
                .try_into()
                .map_err(|_| refuse("a seed that is not 16 bytes"))?,
        };
        nodes
            .get_mut(&node)
            .ok_or(refuse("a seed of a node not in the checkpoint"))?
            .crypt
            .push(record);
    }
    let mut kept: BTreeMap<(NodeId, u32), KeptRecord> = BTreeMap::new();
    for row in &rows.envelopes {
        let node = node_id(row.node, row.node)?;
        let refuse = |reason| CheckpointError::Row {
            node: row.node,
            reason,
        };
        let envelope = u32::try_from(row.volume).map_err(|_| refuse("an envelope"))?;
        let entry = kept.entry((node, envelope)).or_insert_with(|| KeptRecord {
            node,
            envelope,
            runs: Vec::new(),
            closed: None,
            settled: false,
        });
        entry.closed = row
            .closed
            .map(|length| u64::try_from(length).map_err(|_| refuse("a length")))
            .transpose()?;
        entry.settled = row.settled;
        let run = range(row.node, row.start, row.end)?;
        if !run.is_empty() {
            entry.runs.push(Run {
                range: run,
                provenance: provenance_named(&row.provenance).ok_or(refuse("a provenance"))?,
            });
        }
    }
    Ok(Snapshot {
        tree: TreeRecord {
            nodes: nodes.into_values().collect(),
            next_node: root.next_node,
        },
        root,
        files,
        kept: kept.into_values().collect(),
        passwords,
    })
}

fn pair((node, volume): (NodeId, VolumeIndex)) -> (i64, i64) {
    (i64::from(node.0), i64::from(volume.0))
}

fn node_id(row: i64, id: i64) -> Result<NodeId, CheckpointError> {
    u32::try_from(id)
        .map(NodeId)
        .map_err(|_| CheckpointError::Row {
            node: row,
            reason: "a node id",
        })
}

fn version(row: &NodeRow) -> Result<u16, CheckpointError> {
    u16::try_from(row.facts_version).map_err(|_| CheckpointError::Row {
        node: row.node,
        reason: "a facts version",
    })
}

fn range(row: i64, start: i64, end: i64) -> Result<ByteRange, CheckpointError> {
    match (u64::try_from(start), u64::try_from(end)) {
        (Ok(start), Ok(end)) if start <= end => Ok(ByteRange::new(start, end)),
        _ => Err(CheckpointError::Row {
            node: row,
            reason: "a range that is inverted or negative",
        }),
    }
}

const KINDS: [Kind; 17] = [
    Kind::RarStore,
    Kind::SevenZipStore,
    Kind::Zip,
    Kind::Tar,
    Kind::Xz,
    Kind::Gzip,
    Kind::Bzip2,
    Kind::Zstd,
    Kind::Brotli,
    Kind::Deflate,
    Kind::Split,
    Kind::RarEngine,
    Kind::SevenZipEngine,
    Kind::ZipEngine,
    Kind::Par2,
    Kind::Par3,
    Kind::Sfv,
];

// The kind a row names, by the name the kind displays as.
fn kind_named(name: &str) -> Option<Kind> {
    KINDS.into_iter().find(|kind| kind.to_string() == name)
}

// The plan's node states: a live node is open, a retired one refused.
fn state_name(state: NodeState) -> &'static str {
    match state {
        NodeState::Live => "open",
        NodeState::Retired => "refused",
    }
}

fn state_named(name: &str) -> Option<NodeState> {
    [NodeState::Live, NodeState::Retired]
        .into_iter()
        .find(|state| state_name(*state) == name)
}

fn provenance_name(provenance: Provenance) -> &'static str {
    match provenance {
        Provenance::Fresh => "fresh",
        Provenance::Damaged => "damaged",
        Provenance::Repaired => "repaired",
    }
}

fn provenance_named(name: &str) -> Option<Provenance> {
    [Provenance::Fresh, Provenance::Damaged, Provenance::Repaired]
        .into_iter()
        .find(|provenance| provenance_name(*provenance) == name)
}

const REASONS: [Why; 12] = [
    Why::Compressed,
    Why::TailUnreachable,
    Why::EncryptedHeader,
    Why::Encrypted,
    Why::UnboundedEntry,
    Why::NotAFile,
    Why::Unrepairable,
    Why::FirstBytesLost,
    Why::HoldsBreached,
    Why::Unsupported,
    Why::Unmapped,
    Why::TooDeep,
];

pub(crate) fn why_name(why: Why) -> &'static str {
    match why {
        Why::Compressed => "compressed",
        Why::TailUnreachable => "tail_unreachable",
        Why::EncryptedHeader => "encrypted_header",
        Why::Encrypted => "encrypted",
        Why::UnboundedEntry => "unbounded_entry",
        Why::NotAFile => "not_a_file",
        Why::Unrepairable => "unrepairable",
        Why::FirstBytesLost => "first_bytes_lost",
        Why::HoldsBreached => "holds_breached",
        Why::Unsupported => "unsupported",
        Why::Unmapped => "unmapped",
        Why::TooDeep => "too_deep",
    }
}

fn why_named(name: &str) -> Option<Why> {
    REASONS.into_iter().find(|why| why_name(*why) == name)
}

#[cfg(test)]
mod tests;
