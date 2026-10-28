// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A graph job's checkpoint: the tree's records, the root's facts, which
// members have files, the kept envelopes, the holds paged to scratch and
// which candidate each node was offered last, as the rows the database
// keeps. It is taken after every step that reached the tree, when the step's
// bytes are written, so it only ever describes bytes on disk; holds in
// memory are left out and fetched again. A graceful stop pages every hold it
// can to scratch first, so its last checkpoint leaves out only what scratch
// could not take.
//
// Each checkpoint is written as what it changed of the one before: a step
// costs the database the rows it changed, not the job's. One writer per job:
// a change waiting for the database takes in the next one rather than
// queueing behind it, so a slow database costs writes, never order.
//
// Nothing secret is in any of it. The tree records carry no password and no
// key, the objects' facts carry key tuples and salts but never a key, and a
// node's password is the index of its candidate, never the candidate.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use tracing::{debug, warn};
use weaver_graph::holds::ScratchHold;
use weaver_graph::{
    ByteRange, CoverageRecord, CryptRecord, Facts, Kind, MemberId, MemberRecord, NodeId,
    NodeRecord, NodeState, ObjectId, Part, Provenance, Run, TreeRecord, VolumeIndex, Why,
};

use tokio::sync::mpsc::UnboundedSender;

use super::engine::{Heard, Said};
use super::envelope::durable::KeptRecord;
use super::job::GraphJob;
use super::root::facts::{self, Parts, RootFacts, RootFactsError};
use crate::jobs::graph_checkpoint::{
    Changes, CoverageRow, CryptRow, Delta, EnvelopeRow, GraphCheckpoint, HoldRow, MemberRow,
    NodeRow, PartRow, Row,
};
use crate::jobs::ids::JobId;
use crate::pipeline::Pipeline;

// The root row's kind: the root is the article object, not one the builder
// makes.
const ROOT_KIND: &str = "root";

// A job's checkpoint, as a restore reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Snapshot {
    pub(crate) tree: TreeRecord,
    pub(crate) root: RootFacts,
    // What the writer holds of each member it holds anything of.
    pub(crate) files: BTreeMap<(NodeId, MemberId), FileState>,
    pub(crate) kept: Vec<KeptRecord>,
    // The holds each namespace had paged to scratch.
    pub(crate) holds: Vec<HoldRecord>,
    // The candidate each node was offered last, by index.
    pub(crate) passwords: BTreeMap<NodeId, usize>,
}

// A hold of the namespace at `node` paged to its scratch file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct HoldRecord {
    pub(crate) node: NodeId,
    pub(crate) held: ScratchHold,
}

// The generations a checkpoint read the job at: of the tree, of the
// writer's files, of its kept envelopes, and of the offers of candidates.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Generations {
    pub(crate) tree: u64,
    pub(crate) files: u64,
    pub(crate) kept: u64,
    pub(crate) offered: u64,
}

// What the writer holds of one member.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FileState {
    pub(crate) has_file: bool,
    // The name the file sits at while it waits to move to the member's
    // output name.
    pub(crate) on_disk: Option<String>,
    // Why it is a leaf, when something refused to own it.
    pub(crate) refused: Option<Why>,
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

// A job's checkpoints on their way to the database: the rows the database
// holds once what is queued is written, and the change queued for it.
#[derive(Clone, Default)]
pub(crate) struct Pending(Arc<Mutex<Slot>>);

#[derive(Default)]
struct Slot {
    root: facts::Encoded,
    // The root's facts parts as `root` last encoded them, by part: what the
    // database holds of them once what is queued is written, when it holds
    // the job at all. They are kept apart from `last`, so a step compares
    // only the parts it changed.
    root_parts: BTreeMap<i64, Arc<[u8]>>,
    // `None` until a checkpoint is written or read: what the database holds
    // of the job is not known, so the next change is the whole job.
    held: Option<Held>,
    // The change a queued write takes: `Some` exactly while one is queued.
    queued: Option<Delta>,
    tally: Tally,
    // The stamp of the last checkpoint that changed anything. A write
    // takes the stamp with the change, and once it has committed every
    // checkpoint through that stamp is in the database.
    asked: u64,
    // The last stamp a write committed.
    committed: u64,
    // A write failed, so the database holds less than the stamps say until
    // a write of the whole job commits.
    lost: bool,
    // Who to tell once a stamp has committed, or a write has failed, while
    // a member's file waits on it to move.
    owed: Option<Owed>,
}

// The word a move waits on, and where to send it.
struct Owed {
    stamp: u64,
    scope: u64,
    mail: UnboundedSender<Said>,
}

impl Owed {
    fn send(self, stamp: Option<u64>) {
        // A pipeline that has gone hears nothing, and needs to.
        let _ = self.mail.send(Said {
            scope: self.scope,
            node: NodeId::ROOT,
            generation: 0,
            heard: Heard::Written(stamp),
        });
    }
}

// Every row the database holds of the job but the root's facts parts, the
// rows of each node apart, its kept envelopes with them, by the node's id as
// the rows have it, and the generations of the job they were read at: a
// step reads and compares again only the nodes marked since.
#[derive(Default)]
struct Held {
    root: Option<NodeRow>,
    nodes: BTreeMap<i64, GraphCheckpoint>,
    // `None` for rows a restore read, not ones taken from the job: the next
    // step reads every node.
    read: Option<Generations>,
    // The post's encoded bytes of the articles the rows' root facts settle,
    // as the last checkpoint taken from the job encoded them: the job's
    // durable download floor. 0 for rows a restore read; the restore's own
    // floor stands for them until the job's first checkpoint.
    settled_bytes: u64,
}

// What a job's checkpoints asked the database to write, counted as each is
// taken: how many were taken, how many rows they changed, and how many bytes
// of facts they wrote.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Tally {
    pub(crate) checkpoints: usize,
    pub(crate) rows: usize,
    pub(crate) facts: usize,
}

impl Pending {
    fn slot(&self) -> std::sync::MutexGuard<'_, Slot> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // Queue what this checkpoint changes; true when a write must be queued
    // to take it.
    pub(super) fn put(&self, job: &GraphJob) -> bool {
        let mut slot = self.slot();
        let slot = &mut *slot;
        let mut delta = Delta {
            whole: slot.held.is_none(),
            ..Delta::default()
        };
        let held = slot.held.get_or_insert_with(Held::default);
        let encoding = job.encode_root(&mut slot.root, job.next_node());
        held.settled_bytes = job.settled_bytes();
        let root = root_row(encoding.version, encoding.bytes);
        if held.root.as_ref() != Some(&root) {
            delta.nodes.insert(root.key(), Some(root.clone()));
            held.root = Some(root);
        }
        let mut nodes: BTreeSet<i64> = job
            .changed_since(held.read.unwrap_or_default())
            .into_iter()
            .map(|node| i64::from(node.0))
            .collect();
        if held.read.is_none() {
            nodes.extend(held.nodes.keys());
        }
        held.read = Some(job.generations());
        for node in nodes {
            // An id no tree could have names a node the job does not have.
            let now = u32::try_from(node)
                .map(NodeId)
                .map_or_else(|_| GraphCheckpoint::default(), |node| job_rows(job, node));
            let was = held.nodes.remove(&node).unwrap_or_default();
            delta.compare(&was, &now);
            if now != GraphCheckpoint::default() {
                held.nodes.insert(node, now);
            }
        }
        let changed = root_parts(&mut slot.root_parts, encoding.parts);
        if delta.whole {
            let root = i64::from(NodeId::ROOT.0);
            delta
                .parts
                .extend(slot.root_parts.iter().map(|(part, bytes)| {
                    let row = PartRow {
                        node: root,
                        part: *part,
                        bytes: Arc::clone(bytes),
                    };
                    (row.key(), Some(row))
                }));
        } else {
            delta.parts.extend(changed);
        }
        slot.tally.checkpoints += 1;
        slot.tally.rows += delta.rows();
        slot.tally.facts += delta.facts();
        if delta.is_empty() {
            return false;
        }
        slot.asked += 1;
        match &mut slot.queued {
            Some(queued) => {
                queued.absorb(delta);
                false
            }
            None => {
                slot.queued = Some(delta);
                true
            }
        }
    }

    // The change a write takes, and the stamp of the last checkpoint it
    // carries: every checkpoint through it is in the database once the
    // write commits.
    pub(super) fn take(&self) -> (Option<Delta>, u64) {
        let mut slot = self.slot();
        (slot.queued.take(), slot.asked)
    }

    // The stamp of the last checkpoint that changed anything.
    pub(crate) fn stamp(&self) -> u64 {
        self.slot().asked
    }

    // A move waits on the checkpoint `stamp`: the last stamp committed when
    // it already has, and otherwise the mail says so once it does, or once
    // a write fails.
    pub(super) fn await_commit(
        &self,
        stamp: u64,
        scope: u64,
        mail: &UnboundedSender<Said>,
    ) -> Option<u64> {
        let mut slot = self.slot();
        if !slot.lost && slot.committed >= stamp {
            return Some(slot.committed);
        }
        slot.owed = Some(Owed {
            stamp,
            scope,
            mail: mail.clone(),
        });
        None
    }

    // A write committed every checkpoint through `stamp`; `whole` when it
    // wrote the whole job.
    fn committed(&self, stamp: u64, whole: bool) {
        let mut slot = self.slot();
        if whole {
            slot.lost = false;
        }
        if slot.lost {
            return;
        }
        slot.committed = slot.committed.max(stamp);
        let committed = slot.committed;
        if slot
            .owed
            .as_ref()
            .is_some_and(|owed| owed.stamp <= committed)
            && let Some(owed) = slot.owed.take()
        {
            owed.send(Some(committed));
        }
    }

    // A write failed or was never queued, so what the database holds is not
    // known: the next checkpoint writes the whole job, and queues it. A move
    // waiting on a commit hears so, and waits on the next one.
    fn forget(&self) {
        let mut slot = self.slot();
        slot.held = None;
        slot.queued = None;
        slot.lost = true;
        if let Some(owed) = slot.owed.take() {
            owed.send(None);
        }
    }

    // The database holds these rows, as a restore read them. A job's
    // checkpoints are one `Pending`, told this before it takes any, so its
    // encoder has read nothing and the next encoding reads the root whole
    // and is compared with them.
    pub(crate) fn written(&self, rows: GraphCheckpoint) {
        let mut slot = self.slot();
        let root = i64::from(NodeId::ROOT.0);
        let mut held = Held::default();
        for row in rows.nodes {
            if row.node == root {
                held.root = Some(row);
                continue;
            }
            held.nodes.entry(row.node).or_default().nodes.push(row);
        }
        for row in rows.members {
            held.nodes.entry(row.node).or_default().members.push(row);
        }
        for row in rows.coverage {
            held.nodes.entry(row.node).or_default().coverage.push(row);
        }
        for row in rows.crypt {
            held.nodes.entry(row.node).or_default().crypt.push(row);
        }
        slot.root_parts.clear();
        for row in rows.parts {
            if row.node == root {
                slot.root_parts.insert(row.part, row.bytes);
            } else {
                held.nodes.entry(row.node).or_default().parts.push(row);
            }
        }
        for row in rows.envelopes {
            held.nodes.entry(row.node).or_default().envelopes.push(row);
        }
        for row in rows.holds {
            held.nodes.entry(row.node).or_default().holds.push(row);
        }
        slot.held = Some(held);
    }

    // The bytes the rows the database holds, once what is queued is
    // written, spare a restore fetching again; 0 while what it holds is not
    // known.
    pub(crate) fn settled_bytes(&self) -> u64 {
        self.slot()
            .held
            .as_ref()
            .map_or(0, |held| held.settled_bytes)
    }

    #[cfg(test)]
    pub(crate) fn tally(&self) -> Tally {
        self.slot().tally
    }
}

impl Pipeline {
    // A graph job's durable download floor: the bytes its last checkpoint
    // spares a restore. `None` once the job has gone.
    pub(in crate::pipeline) fn graph_checkpointed_bytes(&self, job_id: JobId) -> Option<u64> {
        self.graph_jobs
            .get(&job_id)
            .map(|graph| graph.pending().settled_bytes())
    }

    // Checkpoint the job's tree as it stands now. A member's file that
    // waits to move moves once the checkpoint that records the name it
    // wants has committed: when it already has, the move runs here and the
    // checkpoint is taken again to record it; otherwise the database's
    // word that it has comes through the run mail, and a write is queued
    // to carry it if none would.
    pub(crate) fn checkpoint_graph_job(&mut self, job_id: JobId) {
        let mail = self.graph_runs.written();
        loop {
            let Some(graph) = self.graph_jobs.get_mut(&job_id) else {
                return;
            };
            let pending = graph.pending().clone();
            let mut queue = pending.put(graph);
            graph.recorded(pending.stamp());
            let mut moved = false;
            if graph.moves_waiting() {
                match pending.await_commit(graph.awaited(), job_id.0, &mail) {
                    Some(committed) => match graph.committed(committed) {
                        Ok(left) => moved = left,
                        // A file that could not move fails the job, as it
                        // does when the run mail says the commit.
                        Err(error) => return self.after_graph_result(job_id, Err(error.into())),
                    },
                    None => queue |= graph.owe_mail(),
                }
            }
            if queue {
                self.queue_graph_checkpoint(job_id, pending);
            }
            if !moved {
                return;
            }
        }
    }

    // Queue the write that takes the job's waiting change, if any, and says
    // what it committed. A write dropped before it runs has failed.
    fn queue_graph_checkpoint(&self, job_id: JobId, pending: Pending) {
        let unwritten = Unwritten(Some(pending.clone()));
        let queued = self
            .db
            .try_queue_job_write(job_id, "save_graph_checkpoint", move |db| {
                let mut unwritten = unwritten;
                let Some(writer) = unwritten.0.take() else {
                    return Ok(());
                };
                let (delta, stamp) = writer.take();
                // Nothing queued: a write queued earlier took the change
                // through `stamp`. The database runs a job's writes one at
                // a time in the order they were queued, on SQLite's single
                // writer and on the job's own Postgres lane alike, so that
                // write has already committed it, or failed and said so,
                // and this one only passes its word on.
                let Some(delta) = delta else {
                    writer.committed(stamp, false);
                    return Ok(());
                };
                let (rows, facts, whole) = (delta.rows(), delta.facts(), delta.whole);
                let started = Instant::now();
                let written = db.save_graph_checkpoint(job_id, Arc::new(delta));
                debug!(
                    job_id = job_id.0,
                    rows,
                    facts,
                    elapsed_us = started.elapsed().as_micros() as u64,
                    "graph checkpoint written"
                );
                match &written {
                    Ok(()) => writer.committed(stamp, whole),
                    Err(_) => writer.forget(),
                }
                written
            });
        if let Err(error) = queued {
            pending.forget();
            warn!(job_id = job_id.0, error = %error, "could not queue the graph checkpoint");
        }
    }
}

// A queued checkpoint write that has not run. Dropped unrun, as when the
// database's writer goes down first, it has failed.
struct Unwritten(Option<Pending>);

impl Drop for Unwritten {
    fn drop(&mut self) {
        if let Some(pending) = self.0.take() {
            pending.forget();
        }
    }
}

// What an encoding of the root changed of the root's facts parts `held`,
// which it brings up to date: a part counts as changed when its bytes did.
fn root_parts(held: &mut BTreeMap<i64, Arc<[u8]>>, parts: Parts) -> Changes<PartRow> {
    let node = i64::from(NodeId::ROOT.0);
    let mut out = Changes::<PartRow>::new();
    let mut put = |held: &mut BTreeMap<i64, Arc<[u8]>>, part: i64, bytes: Option<Arc<[u8]>>| {
        let row = match bytes {
            Some(bytes) => {
                let same = held
                    .get(&part)
                    .is_some_and(|was| Arc::ptr_eq(was, &bytes) || *was == bytes);
                if same {
                    return;
                }
                held.insert(part, Arc::clone(&bytes));
                Some(PartRow { node, part, bytes })
            }
            None => {
                if held.remove(&part).is_none() {
                    return;
                }
                None
            }
        };
        out.insert((node, part), row);
    };
    match parts {
        Parts::All(parts) => {
            let now: BTreeMap<i64, Arc<[u8]>> = parts
                .into_iter()
                .map(|part| (part.key as i64, part.bytes))
                .collect();
            let gone: Vec<i64> = held
                .keys()
                .filter(|part| !now.contains_key(part))
                .copied()
                .collect();
            for part in gone {
                put(held, part, None);
            }
            for (part, bytes) in now {
                put(held, part, Some(bytes));
            }
        }
        Parts::Changed(parts) => {
            for part in parts {
                put(held, part.key as i64, part.bytes);
            }
        }
    }
    out
}

fn part_rows(node: i64, parts: &[Part]) -> impl Iterator<Item = PartRow> + '_ {
    parts.iter().map(move |part| PartRow {
        node,
        // The key's bits, as the column's integer.
        part: part.key as i64,
        bytes: Arc::clone(&part.bytes),
    })
}

fn root_row(version: u16, facts: Vec<u8>) -> NodeRow {
    NodeRow {
        node: i64::from(NodeId::ROOT.0),
        parent: None,
        parent_member: None,
        volumes: None,
        window: None,
        kind: ROOT_KIND.to_owned(),
        state: state_name(NodeState::Live).to_owned(),
        reason: None,
        facts_version: i64::from(version),
        facts,
        winning_password: None,
    }
}

// The rows the job has now of one node: its own, as `node_rows` takes
// them, its kept envelopes' and its holds on scratch.
fn job_rows(job: &GraphJob, node: NodeId) -> GraphCheckpoint {
    let mut out = job
        .node(node)
        .map_or_else(GraphCheckpoint::default, |(record, files)| {
            node_rows(
                &record,
                |member| files.get(&member).cloned().unwrap_or_default(),
                job.last_offered(node).map(|index| index as i64),
            )
        });
    out.envelopes = envelope_rows(&job.kept_records(node));
    out.holds = hold_rows(node, &job.hold_records(node));
    out
}

// A namespace's holds on scratch, in key order, as the ledger lists them.
fn hold_rows(node: NodeId, holds: &[ScratchHold]) -> Vec<HoldRow> {
    let node = i64::from(node.0);
    let mut out: Vec<HoldRow> = holds
        .iter()
        .map(|held| HoldRow {
            node,
            volume: i64::from(held.volume.0),
            start: held.range.start as i64,
            end: held.range.end as i64,
            scratch_offset: held.scratch_offset as i64,
        })
        .collect();
    out.sort_by_key(Row::key);
    out
}

// One node's rows, each table in key order: its own row and facts parts
// when it holds an object, and its members, coverage and seeds. `files`
// says what the writer holds of each member, `password` the candidate the
// node was offered last.
fn node_rows(
    node: &NodeRecord,
    files: impl Fn(MemberId) -> FileState,
    password: Option<i64>,
) -> GraphCheckpoint {
    let mut out = GraphCheckpoint::default();
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
            winning_password: password,
        });
        out.parts.extend(part_rows(id, &facts.parts));
    }
    for member in &node.members {
        let state = files(member.member);
        out.members.push(MemberRow {
            node: id,
            member: i64::from(member.member.0),
            name: member.name.clone(),
            output_name: member.output_name.clone(),
            crc32: member.crc32.map(i64::from),
            length: member.length.map(|length| length as i64),
            closed: member.closed,
            bound: member.bound_to.map(|object| i64::from(object.0)),
            owner: member.owner.map(pair),
            given_up: member.given_up.map(pair),
            buried: member.buried,
            undecided: member.undecided,
            has_file: state.has_file,
            on_disk: state.on_disk,
            refused: state.refused.map(|why| why_name(why).to_owned()),
            copy_of: member.copy_of.map(|into| i64::from(into.0)),
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
    out.nodes.sort_by_key(Row::key);
    out.members.sort_by_key(Row::key);
    out.coverage.sort_by_key(Row::key);
    out.crypt.sort_by_key(Row::key);
    out.parts.sort_by_key(Row::key);
    out
}

// The kept envelopes' rows, in key order.
fn envelope_rows(kept: &[KeptRecord]) -> Vec<EnvelopeRow> {
    let mut out = Vec::new();
    for kept in kept {
        let row = |range: ByteRange, provenance| EnvelopeRow {
            node: i64::from(kept.node.0),
            volume: i64::from(kept.envelope),
            start: range.start as i64,
            end: range.end as i64,
            provenance: provenance_name(provenance).to_owned(),
            closed: kept.closed.map(|length| length as i64),
            settled: kept.settled,
            file: kept.file.clone(),
        };
        // An envelope with nothing kept yet still says whether it closed.
        if kept.runs.is_empty() {
            out.push(row(ByteRange::new(0, 0), Provenance::Fresh));
        }
        out.extend(kept.runs.iter().map(|run| row(run.range, run.provenance)));
    }
    out.sort_by_key(Row::key);
    out
}

pub(crate) fn snapshot(rows: &GraphCheckpoint) -> Result<Snapshot, CheckpointError> {
    let root = rows
        .nodes
        .iter()
        .find(|row| row.node == i64::from(NodeId::ROOT.0))
        .filter(|row| row.kind == ROOT_KIND)
        .ok_or(CheckpointError::NoRoot)?;
    let mut parts: BTreeMap<i64, Vec<Part>> = BTreeMap::new();
    for row in &rows.parts {
        parts.entry(row.node).or_default().push(Part {
            key: row.part as u64,
            bytes: Arc::clone(&row.bytes),
        });
    }
    if let Some(node) = parts
        .keys()
        .find(|node| !rows.nodes.iter().any(|row| row.node == **node))
    {
        return Err(CheckpointError::Row {
            node: *node,
            reason: "facts parts of a node not in the checkpoint",
        });
    }
    let root = facts::decode(
        version(root)?,
        &root.facts,
        parts.get(&root.node).map_or(&[], Vec::as_slice),
    )?;
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
                parts: parts.remove(&row.node).unwrap_or_default(),
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
            crc32: row
                .crc32
                .map(|crc32| u32::try_from(crc32).map_err(|_| refuse("a checksum")))
                .transpose()?,
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
            copy_of: row
                .copy_of
                .map(|into| {
                    u32::try_from(into)
                        .map(MemberId)
                        .map_err(|_| refuse("a member id"))
                })
                .transpose()?,
        };
        nodes
            .get_mut(&node)
            .ok_or(refuse("members of a node not in the checkpoint"))?
            .members
            .push(record);
        let state = FileState {
            has_file: row.has_file,
            on_disk: row.on_disk.clone(),
            refused,
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
            file: None,
        });
        entry.file.clone_from(&row.file);
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
    let mut holds = Vec::with_capacity(rows.holds.len());
    for row in &rows.holds {
        let node = node_id(row.node, row.node)?;
        let refuse = |reason| CheckpointError::Row {
            node: row.node,
            reason,
        };
        let scratch_offset =
            u64::try_from(row.scratch_offset).map_err(|_| refuse("a scratch offset"))?;
        holds.push(HoldRecord {
            node,
            held: ScratchHold {
                volume: VolumeIndex(u32::try_from(row.volume).map_err(|_| refuse("a volume"))?),
                range: range(row.node, row.start, row.end)?,
                scratch_offset,
            },
        });
    }
    Ok(Snapshot {
        tree: TreeRecord {
            nodes: nodes.into_values().collect(),
            next_node: root.next_node,
        },
        root,
        files,
        kept: kept.into_values().collect(),
        holds,
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

const KINDS: [Kind; 19] = [
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
    Kind::RarRecovery,
    Kind::RarRecord,
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

const REASONS: [Why; 13] = [
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
    Why::OrderUnknown,
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
        Why::OrderUnknown => "order_unknown",
    }
}

fn why_named(name: &str) -> Option<Why> {
    REASONS.into_iter().find(|why| why_name(*why) == name)
}

#[cfg(test)]
mod tests;
