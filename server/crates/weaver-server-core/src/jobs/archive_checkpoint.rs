use crate::StateError;
use crate::jobs::ids::JobId;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime, SqlTx};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ArchiveCheckpointRef {
    pub(crate) generation: u64,
    pub(crate) reference: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CheckpointCommit {
    Committed,
    Conflict,
    JobGone,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ArchiveAuthority {
    JobGone,
    Job(ArchiveCheckpointRef),
    Legacy(BTreeMap<String, Vec<u8>>),
}

fn decode_reference(row: SqlRow) -> Result<ArchiveCheckpointRef, StateError> {
    let generation = u64::try_from(row.i64("generation")?)
        .ok()
        .filter(|generation| *generation != 0)
        .ok_or_else(|| StateError::Database("invalid archive checkpoint generation".into()))?;
    let reference = row.bytes("reference")?;
    if reference.is_empty() || reference.len() > 256 {
        return Err(StateError::Database(
            "invalid archive checkpoint reference length".into(),
        ));
    }
    Ok(ArchiveCheckpointRef {
        generation,
        reference,
    })
}

pub(super) async fn lock_parent(tx: &mut SqlTx<'_>, job_id: JobId) -> Result<bool, StateError> {
    let sql = match tx {
        SqlTx::Postgres(_) => "SELECT 1 FROM active_jobs WHERE job_id = {} FOR UPDATE",
        SqlTx::Sqlite(_) => "SELECT 1 FROM active_jobs WHERE job_id = {}",
    };
    Ok(tx
        .fetch_optional(sql, &[SqlArg::I64(job_id.0 as i64)])
        .await?
        .is_some())
}

impl Database {
    // Choose authority and read its complete inventory under the same parent
    // lock. Filesystem validation happens afterwards under exclusive job
    // ownership; this transaction does not grant a long-lived backing lease.
    pub(crate) fn load_archive_authority(
        &self,
        job_id: JobId,
    ) -> Result<ArchiveAuthority, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "load_archive_authority", |tx| {
                Box::pin(async move {
                    if !lock_parent(tx, job_id).await? { return Ok(ArchiveAuthority::JobGone); }
                    let args = [SqlArg::I64(job_id.0 as i64)];
                    if let Some(row) = tx.fetch_optional(
                        "SELECT generation, reference FROM active_archive_checkpoint WHERE job_id = {}", &args,
                    ).await? {
                        return decode_reference(row).map(ArchiveAuthority::Job);
                    }
                    // Bound the inventory before fetching any legacy blobs.
                    let sizes = tx.fetch_all(
                        "SELECT length(set_name) AS name_bytes, length(snapshot) AS snapshot_bytes FROM active_direct_coverage WHERE job_id = {} LIMIT 4097", &args,
                    ).await?;
                    let mut bytes = 0u64;
                    for row in &sizes {
                        let name = u64::try_from(row.i64("name_bytes")?).unwrap_or(u64::MAX);
                        let snapshot = u64::try_from(row.i64("snapshot_bytes")?).unwrap_or(u64::MAX);
                        bytes = bytes.saturating_add(name.saturating_mul(4)).saturating_add(snapshot).saturating_add(512);
                    }
                    if sizes.len() > 4096 || bytes > 16 * 1024 * 1024 {
                        return Err(StateError::Database("legacy archive checkpoint inventory exceeds its limit".into()));
                    }
                    let rows = tx.fetch_all(
                        "SELECT set_name, snapshot FROM active_direct_coverage WHERE job_id = {} ORDER BY set_name", &args,
                    ).await?;
                    let mut legacy = BTreeMap::new();
                    for row in rows { legacy.insert(row.text("set_name")?, row.bytes("snapshot")?); }
                    Ok(ArchiveAuthority::Legacy(legacy))
                })
            }).await
        })
    }

    pub(crate) fn load_archive_checkpoint(
        &self,
        job_id: JobId,
    ) -> Result<Option<ArchiveCheckpointRef>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT generation, reference FROM active_archive_checkpoint WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            row.map(decode_reference).transpose()
        })
    }

    // One job owner advances the complete graph. CAS also fences delayed
    // callbacks, and the legacy handover is part of the same transaction.
    pub(crate) fn commit_archive_checkpoint(
        &self,
        job_id: JobId,
        expected_generation: Option<u64>,
        next: ArchiveCheckpointRef,
    ) -> Result<CheckpointCommit, StateError> {
        let expected = expected_generation.unwrap_or(0);
        if expected_generation == Some(0)
            || expected.checked_add(1) != Some(next.generation)
            || next.generation > i64::MAX as u64
            || next.reference.is_empty()
            || next.reference.len() > 256
        {
            return Err(StateError::Database(
                "invalid archive checkpoint transition".into(),
            ));
        }
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "commit_archive_checkpoint", |tx| {
                let next = next.clone();
                Box::pin(async move {
                    if !lock_parent(tx, job_id).await? {
                        return Ok(CheckpointCommit::JobGone);
                    }
                    let affected = if let Some(expected) = expected_generation {
                        tx.execute(
                            "UPDATE active_archive_checkpoint SET generation = {}, reference = {} WHERE job_id = {} AND generation = {}",
                            &[SqlArg::I64(next.generation as i64), SqlArg::Bytes(next.reference), SqlArg::I64(job_id.0 as i64), SqlArg::I64(expected as i64)],
                        ).await?
                    } else {
                        tx.execute(
                            "INSERT INTO active_archive_checkpoint (job_id, generation, reference) VALUES ({}, {}, {}) ON CONFLICT(job_id) DO NOTHING",
                            &[SqlArg::I64(job_id.0 as i64), SqlArg::I64(next.generation as i64), SqlArg::Bytes(next.reference)],
                        ).await?
                    };
                    if affected != 1 { return Ok(CheckpointCommit::Conflict); }
                    if expected_generation.is_none() {
                        tx.execute("DELETE FROM active_direct_coverage WHERE job_id = {}", &[SqlArg::I64(job_id.0 as i64)]).await?;
                        tx.execute("DELETE FROM active_rar_volume_facts WHERE job_id = {}", &[SqlArg::I64(job_id.0 as i64)]).await?;
                    }
                    Ok(CheckpointCommit::Committed)
                })
            }).await
        })
    }
}
