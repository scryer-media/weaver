// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A graph job's checkpoint as the database holds it: plain rows, keyed by
// job, which the graph layer turns to and from its tree. A checkpoint is
// written as what changed since the one before it: the rows that are new or
// different, and the keys of the rows that went away, in one transaction,
// so a restore reads one whole checkpoint or none, and a step costs the
// database what the step changed.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::ids::JobId;
use super::persistence::lock_active_job_for_write_tx;
use crate::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime, SqlTx};
use crate::{Database, StateError};

// Every table a checkpoint writes. A job's rows go with the job.
pub(crate) const TABLES: [&str; 9] = [
    "graph_nodes",
    "graph_members",
    "graph_coverage",
    "graph_envelopes",
    "graph_crypt",
    "graph_recovery",
    "graph_holds",
    "graph_facts_parts",
    "graph_held_parts",
];

// Rows per statement: enough to keep a large change to a few statements,
// few enough to stay under every engine's bind limit.
const BATCH: usize = 64;

// Every row, each table in key order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct GraphCheckpoint {
    pub(crate) nodes: Vec<NodeRow>,
    pub(crate) members: Vec<MemberRow>,
    pub(crate) coverage: Vec<CoverageRow>,
    pub(crate) envelopes: Vec<EnvelopeRow>,
    pub(crate) crypt: Vec<CryptRow>,
    pub(crate) parts: Vec<PartRow>,
    pub(crate) holds: Vec<HoldRow>,
    pub(crate) held_parts: Vec<HeldPartRow>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NodeRow {
    pub(crate) node: i64,
    pub(crate) parent: Option<i64>,
    pub(crate) parent_member: Option<i64>,
    pub(crate) volumes: Option<Vec<u8>>,
    pub(crate) window: Option<(i64, i64)>,
    pub(crate) kind: String,
    pub(crate) state: String,
    pub(crate) reason: Option<String>,
    pub(crate) facts_version: i64,
    pub(crate) facts: Vec<u8>,
    pub(crate) winning_password: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MemberRow {
    pub(crate) node: i64,
    pub(crate) member: i64,
    pub(crate) name: String,
    pub(crate) output_name: String,
    pub(crate) crc32: Option<i64>,
    pub(crate) length: Option<i64>,
    pub(crate) closed: bool,
    pub(crate) bound: Option<i64>,
    pub(crate) owner: Option<(i64, i64)>,
    pub(crate) given_up: Option<(i64, i64)>,
    pub(crate) buried: bool,
    pub(crate) undecided: bool,
    pub(crate) has_file: bool,
    pub(crate) refused: Option<String>,
    pub(crate) copy_of: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CoverageRow {
    pub(crate) node: i64,
    pub(crate) member: i64,
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) provenance: String,
    pub(crate) verified: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EnvelopeRow {
    pub(crate) node: i64,
    pub(crate) volume: i64,
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) provenance: String,
    pub(crate) closed: Option<i64>,
    pub(crate) settled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CryptRow {
    pub(crate) node: i64,
    pub(crate) member: i64,
    pub(crate) run_start: i64,
    pub(crate) seed: Vec<u8>,
}

// A hold of a namespace paged to scratch: a range of one of its members,
// named as the ledger's volume, and where its bytes start in the node's
// scratch file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HoldRow {
    pub(crate) node: i64,
    pub(crate) volume: i64,
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) scratch_offset: i64,
}

// A part that arrived and waits to be placed, spooled to a file under the
// job's working directory: a uuencode part ahead of its file's cursor, or an
// article that declared no usable start ahead of the ordinal it follows.
// `spool_path` is relative to the working directory. A held article keeps
// the facts its decode found; a uuencode part keeps none.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HeldPartRow {
    pub(crate) file: i64,
    pub(crate) ordinal: i64,
    pub(crate) decoded_len: i64,
    pub(crate) spool_path: String,
    pub(crate) article: Option<HeldArticleRow>,
}

// What a held article's decode found, but its bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HeldArticleRow {
    pub(crate) raw_size: i64,
    pub(crate) crc_valid: bool,
    pub(crate) truncation_suspected: bool,
    pub(crate) part_crc_verified: bool,
    pub(crate) part_crc: i64,
    pub(crate) expected_file_crc: Option<i64>,
    pub(crate) file_size: i64,
    pub(crate) part: Option<i64>,
    pub(crate) total: Option<i64>,
    pub(crate) begin: Option<i64>,
    pub(crate) end: Option<i64>,
    pub(crate) name: String,
    // The decode pass's CRC32 segments, each as its offset, length and
    // checksum, little-endian.
    pub(crate) crc_segments: Vec<u8>,
}

// One sealed part of a node's facts. Its bytes are shared with the facts
// they were taken from, so an unchanged part is known unchanged without
// reading it.
#[derive(Clone, Debug, Eq)]
pub(crate) struct PartRow {
    pub(crate) node: i64,
    pub(crate) part: i64,
    pub(crate) bytes: Arc<[u8]>,
}

impl PartialEq for PartRow {
    fn eq(&self, other: &Self) -> bool {
        self.node == other.node
            && self.part == other.part
            && (Arc::ptr_eq(&self.bytes, &other.bytes) || self.bytes == other.bytes)
    }
}

// A row of one table: its key, its columns, and its values.
pub(crate) trait Row: Clone + PartialEq {
    type Key: Ord + Clone + std::fmt::Debug;
    const TABLE: &'static str;
    // The key's columns, after the job's.
    const KEY: &'static [&'static str];
    // Every column, the job's first.
    const COLUMNS: &'static str;

    fn key(&self) -> Self::Key;
    fn key_args(key: &Self::Key) -> Vec<SqlArg>;
    fn args(&self, job: i64) -> Vec<SqlArg>;

    // The bytes of facts the row carries.
    fn facts(&self) -> usize {
        0
    }
}

fn opt_pair(pair: Option<(i64, i64)>) -> [SqlArg; 2] {
    [
        SqlArg::OptI64(pair.map(|pair| pair.0)),
        SqlArg::OptI64(pair.map(|pair| pair.1)),
    ]
}

impl Row for NodeRow {
    type Key = i64;
    const TABLE: &'static str = "graph_nodes";
    const KEY: &'static [&'static str] = &["node_id"];
    const COLUMNS: &'static str = "job_id, node_id, parent_node_id, parent_member_id, volumes, \
         window_start, window_end, kind, state, reason, facts_version, facts, winning_password";

    fn key(&self) -> i64 {
        self.node
    }

    fn key_args(key: &i64) -> Vec<SqlArg> {
        vec![SqlArg::I64(*key)]
    }

    fn args(&self, job: i64) -> Vec<SqlArg> {
        let [window_start, window_end] = opt_pair(self.window);
        vec![
            SqlArg::I64(job),
            SqlArg::I64(self.node),
            SqlArg::OptI64(self.parent),
            SqlArg::OptI64(self.parent_member),
            SqlArg::OptBytes(self.volumes.clone()),
            window_start,
            window_end,
            SqlArg::Text(self.kind.clone()),
            SqlArg::Text(self.state.clone()),
            SqlArg::OptText(self.reason.clone()),
            SqlArg::I64(self.facts_version),
            SqlArg::Bytes(self.facts.clone()),
            SqlArg::OptI64(self.winning_password),
        ]
    }

    fn facts(&self) -> usize {
        self.facts.len()
    }
}

impl Row for MemberRow {
    type Key = (i64, i64);
    const TABLE: &'static str = "graph_members";
    const KEY: &'static [&'static str] = &["node_id", "member_id"];
    const COLUMNS: &'static str = "job_id, node_id, member_id, name, output_name, crc32, \
         length, closed, bound_node_id, owner_node_id, owner_volume, given_up_node_id, \
         given_up_volume, buried, undecided, has_file, refused, copy_of";

    fn key(&self) -> (i64, i64) {
        (self.node, self.member)
    }

    fn key_args(key: &(i64, i64)) -> Vec<SqlArg> {
        vec![SqlArg::I64(key.0), SqlArg::I64(key.1)]
    }

    fn args(&self, job: i64) -> Vec<SqlArg> {
        let [owner_node, owner_volume] = opt_pair(self.owner);
        let [given_up_node, given_up_volume] = opt_pair(self.given_up);
        vec![
            SqlArg::I64(job),
            SqlArg::I64(self.node),
            SqlArg::I64(self.member),
            SqlArg::Text(self.name.clone()),
            SqlArg::Text(self.output_name.clone()),
            SqlArg::OptI64(self.crc32),
            SqlArg::OptI64(self.length),
            SqlArg::Bool(self.closed),
            SqlArg::OptI64(self.bound),
            owner_node,
            owner_volume,
            given_up_node,
            given_up_volume,
            SqlArg::Bool(self.buried),
            SqlArg::Bool(self.undecided),
            SqlArg::Bool(self.has_file),
            SqlArg::OptText(self.refused.clone()),
            SqlArg::OptI64(self.copy_of),
        ]
    }
}

impl Row for CoverageRow {
    type Key = (i64, i64, i64);
    const TABLE: &'static str = "graph_coverage";
    const KEY: &'static [&'static str] = &["node_id", "member_id", "range_start"];
    const COLUMNS: &'static str =
        "job_id, node_id, member_id, range_start, range_end, provenance, verified";

    fn key(&self) -> (i64, i64, i64) {
        (self.node, self.member, self.start)
    }

    fn key_args(key: &(i64, i64, i64)) -> Vec<SqlArg> {
        vec![SqlArg::I64(key.0), SqlArg::I64(key.1), SqlArg::I64(key.2)]
    }

    fn args(&self, job: i64) -> Vec<SqlArg> {
        vec![
            SqlArg::I64(job),
            SqlArg::I64(self.node),
            SqlArg::I64(self.member),
            SqlArg::I64(self.start),
            SqlArg::I64(self.end),
            SqlArg::Text(self.provenance.clone()),
            SqlArg::Bool(self.verified),
        ]
    }
}

impl Row for EnvelopeRow {
    type Key = (i64, i64, i64);
    const TABLE: &'static str = "graph_envelopes";
    const KEY: &'static [&'static str] = &["node_id", "volume", "range_start"];
    const COLUMNS: &'static str =
        "job_id, node_id, volume, range_start, range_end, provenance, closed, settled";

    fn key(&self) -> (i64, i64, i64) {
        (self.node, self.volume, self.start)
    }

    fn key_args(key: &(i64, i64, i64)) -> Vec<SqlArg> {
        vec![SqlArg::I64(key.0), SqlArg::I64(key.1), SqlArg::I64(key.2)]
    }

    fn args(&self, job: i64) -> Vec<SqlArg> {
        vec![
            SqlArg::I64(job),
            SqlArg::I64(self.node),
            SqlArg::I64(self.volume),
            SqlArg::I64(self.start),
            SqlArg::I64(self.end),
            SqlArg::Text(self.provenance.clone()),
            SqlArg::OptI64(self.closed),
            SqlArg::Bool(self.settled),
        ]
    }
}

impl Row for CryptRow {
    type Key = (i64, i64, i64);
    const TABLE: &'static str = "graph_crypt";
    const KEY: &'static [&'static str] = &["node_id", "member_id", "run_start"];
    const COLUMNS: &'static str = "job_id, node_id, member_id, run_start, seed";

    fn key(&self) -> (i64, i64, i64) {
        (self.node, self.member, self.run_start)
    }

    fn key_args(key: &(i64, i64, i64)) -> Vec<SqlArg> {
        vec![SqlArg::I64(key.0), SqlArg::I64(key.1), SqlArg::I64(key.2)]
    }

    fn args(&self, job: i64) -> Vec<SqlArg> {
        vec![
            SqlArg::I64(job),
            SqlArg::I64(self.node),
            SqlArg::I64(self.member),
            SqlArg::I64(self.run_start),
            SqlArg::Bytes(self.seed.clone()),
        ]
    }
}

impl Row for HoldRow {
    type Key = (i64, i64, i64);
    const TABLE: &'static str = "graph_holds";
    const KEY: &'static [&'static str] = &["node_id", "volume", "range_start"];
    const COLUMNS: &'static str = "job_id, node_id, volume, range_start, range_end, scratch_offset";

    fn key(&self) -> (i64, i64, i64) {
        (self.node, self.volume, self.start)
    }

    fn key_args(key: &(i64, i64, i64)) -> Vec<SqlArg> {
        vec![SqlArg::I64(key.0), SqlArg::I64(key.1), SqlArg::I64(key.2)]
    }

    fn args(&self, job: i64) -> Vec<SqlArg> {
        vec![
            SqlArg::I64(job),
            SqlArg::I64(self.node),
            SqlArg::I64(self.volume),
            SqlArg::I64(self.start),
            SqlArg::I64(self.end),
            SqlArg::I64(self.scratch_offset),
        ]
    }
}

impl Row for PartRow {
    type Key = (i64, i64);
    const TABLE: &'static str = "graph_facts_parts";
    const KEY: &'static [&'static str] = &["node_id", "part"];
    const COLUMNS: &'static str = "job_id, node_id, part, bytes";

    fn key(&self) -> (i64, i64) {
        (self.node, self.part)
    }

    fn key_args(key: &(i64, i64)) -> Vec<SqlArg> {
        vec![SqlArg::I64(key.0), SqlArg::I64(key.1)]
    }

    fn args(&self, job: i64) -> Vec<SqlArg> {
        vec![
            SqlArg::I64(job),
            SqlArg::I64(self.node),
            SqlArg::I64(self.part),
            SqlArg::Bytes(self.bytes.to_vec()),
        ]
    }

    fn facts(&self) -> usize {
        self.bytes.len()
    }
}

impl Row for HeldPartRow {
    type Key = (i64, i64);
    const TABLE: &'static str = "graph_held_parts";
    const KEY: &'static [&'static str] = &["file_index", "ordinal"];
    const COLUMNS: &'static str = "job_id, file_index, ordinal, decoded_len, spool_path, \
        raw_size, crc_valid, truncation_suspected, part_crc_verified, part_crc, \
        expected_file_crc, file_size, yenc_part, yenc_total, yenc_begin, yenc_end, yenc_name, \
        crc_segments";

    fn key(&self) -> (i64, i64) {
        (self.file, self.ordinal)
    }

    fn key_args(key: &(i64, i64)) -> Vec<SqlArg> {
        vec![SqlArg::I64(key.0), SqlArg::I64(key.1)]
    }

    fn args(&self, job: i64) -> Vec<SqlArg> {
        let article = self.article.as_ref();
        vec![
            SqlArg::I64(job),
            SqlArg::I64(self.file),
            SqlArg::I64(self.ordinal),
            SqlArg::I64(self.decoded_len),
            SqlArg::Text(self.spool_path.clone()),
            SqlArg::OptI64(article.map(|article| article.raw_size)),
            SqlArg::OptBool(article.map(|article| article.crc_valid)),
            SqlArg::OptBool(article.map(|article| article.truncation_suspected)),
            SqlArg::OptBool(article.map(|article| article.part_crc_verified)),
            SqlArg::OptI64(article.map(|article| article.part_crc)),
            SqlArg::OptI64(article.and_then(|article| article.expected_file_crc)),
            SqlArg::OptI64(article.map(|article| article.file_size)),
            SqlArg::OptI64(article.and_then(|article| article.part)),
            SqlArg::OptI64(article.and_then(|article| article.total)),
            SqlArg::OptI64(article.and_then(|article| article.begin)),
            SqlArg::OptI64(article.and_then(|article| article.end)),
            SqlArg::OptText(article.map(|article| article.name.clone())),
            SqlArg::OptBytes(article.map(|article| article.crc_segments.clone())),
        ]
    }
}

// The rows of one table a checkpoint changes, by key: the row as it is now,
// or `None` for one that went away.
pub(crate) type Changes<R> = BTreeMap<<R as Row>::Key, Option<R>>;

// What one checkpoint changes of the one before it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Delta {
    // Nothing is known of what the database holds of the job, so every row
    // of it goes before these are written.
    pub(crate) whole: bool,
    pub(crate) nodes: Changes<NodeRow>,
    pub(crate) members: Changes<MemberRow>,
    pub(crate) coverage: Changes<CoverageRow>,
    pub(crate) envelopes: Changes<EnvelopeRow>,
    pub(crate) crypt: Changes<CryptRow>,
    pub(crate) parts: Changes<PartRow>,
    pub(crate) holds: Changes<HoldRow>,
    pub(crate) held_parts: Changes<HeldPartRow>,
}

impl Delta {
    // What `after` changes of `before`, taken into this change: rows of one
    // part of the job, whose other rows are compared apart. Both are in key
    // order.
    pub(crate) fn compare(&mut self, before: &GraphCheckpoint, after: &GraphCheckpoint) {
        changes(&mut self.nodes, &before.nodes, &after.nodes);
        changes(&mut self.members, &before.members, &after.members);
        changes(&mut self.coverage, &before.coverage, &after.coverage);
        changes(&mut self.envelopes, &before.envelopes, &after.envelopes);
        changes(&mut self.crypt, &before.crypt, &after.crypt);
        changes(&mut self.parts, &before.parts, &after.parts);
        changes(&mut self.holds, &before.holds, &after.holds);
        changes(&mut self.held_parts, &before.held_parts, &after.held_parts);
    }

    // This change followed by `later`, as one.
    pub(crate) fn absorb(&mut self, later: Delta) {
        if later.whole {
            *self = later;
            return;
        }
        self.nodes.extend(later.nodes);
        self.members.extend(later.members);
        self.coverage.extend(later.coverage);
        self.envelopes.extend(later.envelopes);
        self.crypt.extend(later.crypt);
        self.parts.extend(later.parts);
        self.holds.extend(later.holds);
        self.held_parts.extend(later.held_parts);
    }

    pub(crate) fn is_empty(&self) -> bool {
        !self.whole && self.rows() == 0
    }

    // How many rows it writes or deletes.
    pub(crate) fn rows(&self) -> usize {
        self.nodes.len()
            + self.members.len()
            + self.coverage.len()
            + self.envelopes.len()
            + self.crypt.len()
            + self.parts.len()
            + self.holds.len()
            + self.held_parts.len()
    }

    // How many bytes of facts it writes.
    pub(crate) fn facts(&self) -> usize {
        fn of<R: Row>(changes: &Changes<R>) -> usize {
            changes.values().flatten().map(Row::facts).sum()
        }
        of(&self.nodes) + of(&self.parts)
    }
}

// The rows of `after` not in `before` as they are, and the keys of
// `before` not in `after`, into `out`. Both are in key order, so one pass
// over each finds them.
fn changes<R: Row>(out: &mut Changes<R>, before: &[R], after: &[R]) {
    let mut was = before.iter().peekable();
    for row in after {
        let key = row.key();
        while let Some(gone) = was.next_if(|old| old.key() < key) {
            out.insert(gone.key(), None);
        }
        match was.next_if(|old| old.key() == key) {
            Some(old) if old == row => {}
            _ => {
                out.insert(key, Some(row.clone()));
            }
        }
    }
    out.extend(was.map(|gone| (gone.key(), None)));
}

impl Database {
    // Write what the checkpoint changed. A job whose active row is gone
    // keeps none: its rows are deleted and nothing is written.
    pub(crate) fn save_graph_checkpoint(
        &self,
        job_id: JobId,
        delta: Arc<Delta>,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "save_graph_checkpoint", |tx| {
                let delta = Arc::clone(&delta);
                Box::pin(async move { write_delta(tx, job_id, &delta).await })
            })
            .await
        })
    }

    // The job's checkpoint, or `None` when it has no rows. Read in one
    // transaction, so it is one checkpoint even beside a writer.
    pub(crate) fn load_graph_checkpoint(
        &self,
        job_id: JobId,
    ) -> Result<Option<GraphCheckpoint>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "load_graph_checkpoint", |tx| {
                Box::pin(async move { read_checkpoint(tx, job_id).await })
            })
            .await
        })
    }
}

async fn write_delta(tx: &mut SqlTx<'_>, job_id: JobId, delta: &Delta) -> Result<(), StateError> {
    lock_active_job_for_write_tx(tx, job_id).await?;
    let id = job_id.0 as i64;
    let live = tx
        .fetch_optional(
            "SELECT 1 AS live FROM active_jobs WHERE job_id = {}",
            &[SqlArg::I64(id)],
        )
        .await?
        .is_some();
    if !live || delta.whole {
        for table in TABLES {
            tx.execute(
                &format!("DELETE FROM {table} WHERE job_id = {{}}"),
                &[SqlArg::I64(id)],
            )
            .await?;
        }
    }
    if !live {
        return Ok(());
    }
    write(tx, id, &delta.nodes).await?;
    write(tx, id, &delta.members).await?;
    write(tx, id, &delta.coverage).await?;
    write(tx, id, &delta.envelopes).await?;
    write(tx, id, &delta.crypt).await?;
    write(tx, id, &delta.parts).await?;
    write(tx, id, &delta.holds).await?;
    write(tx, id, &delta.held_parts).await?;
    Ok(())
}

// One table's changes: every changed key's row is deleted, and the rows as
// they are now inserted, a batch per statement.
async fn write<R: Row>(
    tx: &mut SqlTx<'_>,
    job: i64,
    changes: &Changes<R>,
) -> Result<(), StateError> {
    let keys: Vec<&R::Key> = changes.keys().collect();
    let matches = format!(
        "({})",
        R::KEY
            .iter()
            .map(|column| format!("{column} = {{}}"))
            .collect::<Vec<_>>()
            .join(" AND ")
    );
    for batch in keys.chunks(BATCH) {
        let sql = format!(
            "DELETE FROM {} WHERE job_id = {{}} AND ({})",
            R::TABLE,
            vec![matches.as_str(); batch.len()].join(" OR ")
        );
        let mut args = vec![SqlArg::I64(job)];
        args.extend(batch.iter().flat_map(|key| R::key_args(key)));
        tx.execute(&sql, &args).await?;
    }
    let rows: Vec<&R> = changes.values().flatten().collect();
    for batch in rows.chunks(BATCH) {
        let Some(width) = batch.first().map(|row| row.args(job).len()) else {
            continue;
        };
        let tuple = format!("({})", vec!["{}"; width].join(", "));
        let sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            R::TABLE,
            R::COLUMNS,
            vec![tuple; batch.len()].join(", ")
        );
        let args: Vec<SqlArg> = batch.iter().flat_map(|row| row.args(job)).collect();
        tx.execute(&sql, &args).await?;
    }
    Ok(())
}

async fn read_checkpoint(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
) -> Result<Option<GraphCheckpoint>, StateError> {
    let args = [SqlArg::I64(job_id.0 as i64)];
    let nodes = tx
        .fetch_all(
            "SELECT node_id, parent_node_id, parent_member_id, volumes, window_start, window_end, \
             kind, state, reason, facts_version, facts, winning_password \
             FROM graph_nodes WHERE job_id = {} ORDER BY node_id",
            &args,
        )
        .await?;
    if nodes.is_empty() {
        return Ok(None);
    }
    let pair = |row: &SqlRow, first: &str, second: &str| -> Result<_, StateError> {
        Ok(row.opt_i64(first)?.zip(row.opt_i64(second)?))
    };
    let nodes = nodes
        .iter()
        .map(|row| {
            Ok(NodeRow {
                node: row.i64("node_id")?,
                parent: row.opt_i64("parent_node_id")?,
                parent_member: row.opt_i64("parent_member_id")?,
                volumes: row.opt_bytes("volumes")?,
                window: pair(row, "window_start", "window_end")?,
                kind: row.text("kind")?,
                state: row.text("state")?,
                reason: row.opt_text("reason")?,
                facts_version: row.i64("facts_version")?,
                facts: row.bytes("facts")?,
                winning_password: row.opt_i64("winning_password")?,
            })
        })
        .collect::<Result<_, StateError>>()?;
    let members = tx
        .fetch_all(
            "SELECT node_id, member_id, name, output_name, crc32, length, closed, bound_node_id, \
             owner_node_id, owner_volume, given_up_node_id, given_up_volume, buried, \
             undecided, has_file, refused, copy_of \
             FROM graph_members WHERE job_id = {} ORDER BY node_id, member_id",
            &args,
        )
        .await?
        .iter()
        .map(|row| {
            Ok(MemberRow {
                node: row.i64("node_id")?,
                member: row.i64("member_id")?,
                name: row.text("name")?,
                output_name: row.text("output_name")?,
                crc32: row.opt_i64("crc32")?,
                length: row.opt_i64("length")?,
                closed: row.bool("closed")?,
                bound: row.opt_i64("bound_node_id")?,
                owner: pair(row, "owner_node_id", "owner_volume")?,
                given_up: pair(row, "given_up_node_id", "given_up_volume")?,
                buried: row.bool("buried")?,
                undecided: row.bool("undecided")?,
                has_file: row.bool("has_file")?,
                refused: row.opt_text("refused")?,
                copy_of: row.opt_i64("copy_of")?,
            })
        })
        .collect::<Result<_, StateError>>()?;
    let coverage = tx
        .fetch_all(
            "SELECT node_id, member_id, range_start, range_end, provenance, verified \
             FROM graph_coverage WHERE job_id = {} ORDER BY node_id, member_id, range_start",
            &args,
        )
        .await?
        .iter()
        .map(|row| {
            Ok(CoverageRow {
                node: row.i64("node_id")?,
                member: row.i64("member_id")?,
                start: row.i64("range_start")?,
                end: row.i64("range_end")?,
                provenance: row.text("provenance")?,
                verified: row.bool("verified")?,
            })
        })
        .collect::<Result<_, StateError>>()?;
    let envelopes = tx
        .fetch_all(
            "SELECT node_id, volume, range_start, range_end, provenance, closed, settled \
             FROM graph_envelopes WHERE job_id = {} ORDER BY node_id, volume, range_start",
            &args,
        )
        .await?
        .iter()
        .map(|row| {
            Ok(EnvelopeRow {
                node: row.i64("node_id")?,
                volume: row.i64("volume")?,
                start: row.i64("range_start")?,
                end: row.i64("range_end")?,
                provenance: row.text("provenance")?,
                closed: row.opt_i64("closed")?,
                settled: row.bool("settled")?,
            })
        })
        .collect::<Result<_, StateError>>()?;
    let crypt = tx
        .fetch_all(
            "SELECT node_id, member_id, run_start, seed \
             FROM graph_crypt WHERE job_id = {} ORDER BY node_id, member_id, run_start",
            &args,
        )
        .await?
        .iter()
        .map(|row| {
            Ok(CryptRow {
                node: row.i64("node_id")?,
                member: row.i64("member_id")?,
                run_start: row.i64("run_start")?,
                seed: row.bytes("seed")?,
            })
        })
        .collect::<Result<_, StateError>>()?;
    let parts = tx
        .fetch_all(
            "SELECT node_id, part, bytes \
             FROM graph_facts_parts WHERE job_id = {} ORDER BY node_id, part",
            &args,
        )
        .await?
        .iter()
        .map(|row| {
            Ok(PartRow {
                node: row.i64("node_id")?,
                part: row.i64("part")?,
                bytes: Arc::from(row.bytes("bytes")?),
            })
        })
        .collect::<Result<_, StateError>>()?;
    let holds = tx
        .fetch_all(
            "SELECT node_id, volume, range_start, range_end, scratch_offset \
             FROM graph_holds WHERE job_id = {} ORDER BY node_id, volume, range_start",
            &args,
        )
        .await?
        .iter()
        .map(|row| {
            Ok(HoldRow {
                node: row.i64("node_id")?,
                volume: row.i64("volume")?,
                start: row.i64("range_start")?,
                end: row.i64("range_end")?,
                scratch_offset: row.i64("scratch_offset")?,
            })
        })
        .collect::<Result<_, StateError>>()?;
    let held_parts = tx
        .fetch_all(
            "SELECT file_index, ordinal, decoded_len, spool_path, raw_size, crc_valid, \
             truncation_suspected, part_crc_verified, part_crc, expected_file_crc, file_size, \
             yenc_part, yenc_total, yenc_begin, yenc_end, yenc_name, crc_segments \
             FROM graph_held_parts WHERE job_id = {} ORDER BY file_index, ordinal",
            &args,
        )
        .await?
        .iter()
        .map(|row| {
            let article = match row.opt_i64("raw_size")? {
                None => None,
                Some(raw_size) => Some(HeldArticleRow {
                    raw_size,
                    crc_valid: row.bool("crc_valid")?,
                    truncation_suspected: row.bool("truncation_suspected")?,
                    part_crc_verified: row.bool("part_crc_verified")?,
                    part_crc: row.i64("part_crc")?,
                    expected_file_crc: row.opt_i64("expected_file_crc")?,
                    file_size: row.i64("file_size")?,
                    part: row.opt_i64("yenc_part")?,
                    total: row.opt_i64("yenc_total")?,
                    begin: row.opt_i64("yenc_begin")?,
                    end: row.opt_i64("yenc_end")?,
                    name: row.text("yenc_name")?,
                    crc_segments: row.bytes("crc_segments")?,
                }),
            };
            Ok(HeldPartRow {
                file: row.i64("file_index")?,
                ordinal: row.i64("ordinal")?,
                decoded_len: row.i64("decoded_len")?,
                spool_path: row.text("spool_path")?,
                article,
            })
        })
        .collect::<Result<_, StateError>>()?;
    Ok(Some(GraphCheckpoint {
        nodes,
        members,
        coverage,
        envelopes,
        crypt,
        parts,
        holds,
        held_parts,
    }))
}
