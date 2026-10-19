// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A graph job's checkpoint as the database holds it: plain rows, keyed by
// job, which the graph layer turns to and from its tree. A checkpoint
// replaces every row of the job at once, in one transaction, so a restore
// reads one whole checkpoint or none.

use std::sync::Arc;

use super::ids::JobId;
use super::persistence::lock_active_job_for_write_tx;
use crate::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime, SqlTx};
use crate::{Database, StateError};

// Every table a checkpoint writes. A job's rows go with the job.
pub(crate) const TABLES: [&str; 7] = [
    "graph_nodes",
    "graph_members",
    "graph_coverage",
    "graph_envelopes",
    "graph_crypt",
    "graph_recovery",
    "graph_holds",
];

// Rows per INSERT: enough to keep a large tree to a few statements, few
// enough to stay under every engine's bind limit.
const BATCH: usize = 64;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct GraphCheckpoint {
    pub(crate) nodes: Vec<NodeRow>,
    pub(crate) members: Vec<MemberRow>,
    pub(crate) coverage: Vec<CoverageRow>,
    pub(crate) envelopes: Vec<EnvelopeRow>,
    pub(crate) crypt: Vec<CryptRow>,
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
    pub(crate) length: Option<i64>,
    pub(crate) closed: bool,
    pub(crate) bound: Option<i64>,
    pub(crate) owner: Option<(i64, i64)>,
    pub(crate) given_up: Option<(i64, i64)>,
    pub(crate) buried: bool,
    pub(crate) undecided: bool,
    pub(crate) has_file: bool,
    pub(crate) refused: Option<String>,
    pub(crate) unusable_at: Option<i64>,
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

impl Database {
    // Replace the job's checkpoint with this one. A job whose active row is
    // gone keeps none: its rows are deleted and nothing is written.
    pub(crate) fn save_graph_checkpoint(
        &self,
        job_id: JobId,
        checkpoint: Arc<GraphCheckpoint>,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "save_graph_checkpoint", |tx| {
                let checkpoint = Arc::clone(&checkpoint);
                Box::pin(async move { write_checkpoint(tx, job_id, &checkpoint).await })
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

async fn write_checkpoint(
    tx: &mut SqlTx<'_>,
    job_id: JobId,
    checkpoint: &GraphCheckpoint,
) -> Result<(), StateError> {
    lock_active_job_for_write_tx(tx, job_id).await?;
    let id = job_id.0 as i64;
    for table in TABLES {
        tx.execute(
            &format!("DELETE FROM {table} WHERE job_id = {{}}"),
            &[SqlArg::I64(id)],
        )
        .await?;
    }
    // A job is live while its queue row or its torrent row is.
    let live = tx
        .fetch_optional(
            "SELECT 1 AS live FROM active_jobs WHERE job_id = {}
             UNION ALL SELECT 1 AS live FROM torrents WHERE job_id = {}",
            &[SqlArg::I64(id), SqlArg::I64(id)],
        )
        .await?
        .is_some();
    if !live {
        return Ok(());
    }
    let job = || SqlArg::I64(id);
    let opt_pair = |pair: Option<(i64, i64)>| {
        (
            SqlArg::OptI64(pair.map(|pair| pair.0)),
            SqlArg::OptI64(pair.map(|pair| pair.1)),
        )
    };
    let nodes = checkpoint.nodes.iter().map(|row| {
        let (window_start, window_end) = opt_pair(row.window);
        vec![
            job(),
            SqlArg::I64(row.node),
            SqlArg::OptI64(row.parent),
            SqlArg::OptI64(row.parent_member),
            SqlArg::OptBytes(row.volumes.clone()),
            window_start,
            window_end,
            SqlArg::Text(row.kind.clone()),
            SqlArg::Text(row.state.clone()),
            SqlArg::OptText(row.reason.clone()),
            SqlArg::I64(row.facts_version),
            SqlArg::Bytes(row.facts.clone()),
            SqlArg::OptI64(row.winning_password),
        ]
    });
    insert(
        tx,
        "graph_nodes (job_id, node_id, parent_node_id, parent_member_id, volumes, \
         window_start, window_end, kind, state, reason, facts_version, facts, winning_password)",
        nodes,
    )
    .await?;
    let members = checkpoint.members.iter().map(|row| {
        let (owner_node, owner_volume) = opt_pair(row.owner);
        let (given_up_node, given_up_volume) = opt_pair(row.given_up);
        vec![
            job(),
            SqlArg::I64(row.node),
            SqlArg::I64(row.member),
            SqlArg::Text(row.name.clone()),
            SqlArg::Text(row.output_name.clone()),
            SqlArg::OptI64(row.length),
            SqlArg::Bool(row.closed),
            SqlArg::OptI64(row.bound),
            owner_node,
            owner_volume,
            given_up_node,
            given_up_volume,
            SqlArg::Bool(row.buried),
            SqlArg::Bool(row.undecided),
            SqlArg::Bool(row.has_file),
            SqlArg::OptText(row.refused.clone()),
            SqlArg::OptI64(row.unusable_at),
        ]
    });
    insert(
        tx,
        "graph_members (job_id, node_id, member_id, name, output_name, length, closed, \
         bound_node_id, owner_node_id, owner_volume, given_up_node_id, given_up_volume, \
         buried, undecided, has_file, refused, unusable_at)",
        members,
    )
    .await?;
    let coverage = checkpoint.coverage.iter().map(|row| {
        vec![
            job(),
            SqlArg::I64(row.node),
            SqlArg::I64(row.member),
            SqlArg::I64(row.start),
            SqlArg::I64(row.end),
            SqlArg::Text(row.provenance.clone()),
            SqlArg::Bool(row.verified),
        ]
    });
    insert(
        tx,
        "graph_coverage (job_id, node_id, member_id, range_start, range_end, provenance, verified)",
        coverage,
    )
    .await?;
    let envelopes = checkpoint.envelopes.iter().map(|row| {
        vec![
            job(),
            SqlArg::I64(row.node),
            SqlArg::I64(row.volume),
            SqlArg::I64(row.start),
            SqlArg::I64(row.end),
            SqlArg::Text(row.provenance.clone()),
            SqlArg::OptI64(row.closed),
            SqlArg::Bool(row.settled),
        ]
    });
    insert(
        tx,
        "graph_envelopes (job_id, node_id, volume, range_start, range_end, provenance, closed, settled)",
        envelopes,
    )
    .await?;
    let crypt = checkpoint.crypt.iter().map(|row| {
        vec![
            job(),
            SqlArg::I64(row.node),
            SqlArg::I64(row.member),
            SqlArg::I64(row.run_start),
            SqlArg::Bytes(row.seed.clone()),
        ]
    });
    insert(
        tx,
        "graph_crypt (job_id, node_id, member_id, run_start, seed)",
        crypt,
    )
    .await?;
    Ok(())
}

// Insert rows of one shape, a batch per statement.
async fn insert(
    tx: &mut SqlTx<'_>,
    into: &str,
    rows: impl Iterator<Item = Vec<SqlArg>>,
) -> Result<(), StateError> {
    let rows: Vec<Vec<SqlArg>> = rows.collect();
    for batch in rows.chunks(BATCH) {
        let Some(width) = batch.first().map(Vec::len) else {
            continue;
        };
        let tuple = format!("({})", vec!["{}"; width].join(", "));
        let sql = format!(
            "INSERT INTO {into} VALUES {}",
            vec![tuple; batch.len()].join(", ")
        );
        let args: Vec<SqlArg> = batch.iter().flatten().cloned().collect();
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
            "SELECT node_id, member_id, name, output_name, length, closed, bound_node_id, \
             owner_node_id, owner_volume, given_up_node_id, given_up_volume, buried, \
             undecided, has_file, refused, unusable_at \
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
                length: row.opt_i64("length")?,
                closed: row.bool("closed")?,
                bound: row.opt_i64("bound_node_id")?,
                owner: pair(row, "owner_node_id", "owner_volume")?,
                given_up: pair(row, "given_up_node_id", "given_up_volume")?,
                buried: row.bool("buried")?,
                undecided: row.bool("undecided")?,
                has_file: row.bool("has_file")?,
                refused: row.opt_text("refused")?,
                unusable_at: row.opt_i64("unusable_at")?,
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
    Ok(Some(GraphCheckpoint {
        nodes,
        members,
        coverage,
        envelopes,
        crypt,
    }))
}
