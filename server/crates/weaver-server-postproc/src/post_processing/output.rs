// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::post_processing::database::PostprocDatabase;
use crate::post_processing::settings::PostProcessingSettingsQueries;
use std::collections::BTreeSet;
use std::io::Read;

use super::model::{EventScriptSettings, ScriptEventLabel, ScriptKind, ScriptResult, ScriptStatus};
use super::run_metrics::RetentionAction;
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime, SqlTx};

const MAX_DECODE_BYTES: usize = 8 * 1024 * 1024;
const EXCERPT_BYTES: usize = 4096;

fn error(error: impl std::fmt::Display) -> StateError {
    StateError::Database(error.to_string())
}

// Which recorded runs to list. An unset field matches every run.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct ScriptRunFilter {
    pub job_id: Option<u64>,
    pub script: Option<String>,
    pub kind: Option<ScriptKind>,
    pub status: Option<ScriptStatus>,
}

// One recorded run of a script, whatever started it.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScriptRun {
    // Names the run's kept output, when `output_retained`.
    pub id: String,
    // Position in the order runs were recorded; later runs are higher.
    pub seq: i64,
    pub job_id: Option<u64>,
    // Known once the job has reached history.
    pub job_name: Option<String>,
    pub output_retained: bool,
    pub result: ScriptResult,
}

pub fn excerpt(output: &[u8]) -> String {
    let start = output.len().saturating_sub(EXCERPT_BYTES);
    let start = if start > 0 {
        output[start..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(start, |offset| start + offset + 1)
    } else {
        0
    };
    String::from_utf8_lossy(&output[start..]).into_owned()
}

pub fn decode_output(bytes: &[u8], ceiling: usize) -> Result<String, StateError> {
    let ceiling = ceiling.min(MAX_DECODE_BYTES);
    let mut decoded = Vec::new();
    zstd::stream::read::Decoder::new(bytes)
        .map_err(error)?
        .take(ceiling as u64 + 1)
        .read_to_end(&mut decoded)
        .map_err(error)?;
    if decoded.len() > ceiling {
        return Err(error("script output exceeds its decode ceiling"));
    }
    Ok(String::from_utf8_lossy(&decoded).into_owned())
}

// Kept output is small and written once, so it is compressed hard.
pub const COMPRESSION_LEVEL: i32 = 19;

// Record a run and its kept output. `raw_bytes` is everything the script
// wrote, of which `output` is the newest part.
pub async fn retain_output<D: PostprocDatabase>(
    db: D,
    job_id: Option<u64>,
    mut result: ScriptResult,
    output: Vec<u8>,
    raw_bytes: u64,
    limits: EventScriptSettings,
) -> Result<ScriptResult, StateError> {
    result.output_tail = excerpt(&output);
    tokio::task::spawn_blocking(move || {
        let compressed = if output.is_empty() {
            Vec::new()
        } else {
            zstd::stream::encode_all(output.as_slice(), COMPRESSION_LEVEL).map_err(error)?
        };
        super::run_metrics::record_output(result.label(), raw_bytes, compressed.len() as u64);
        db.insert_script_output(job_id, result, raw_bytes, compressed, limits)
    })
    .await
    .map_err(error)?
}

// A run counts against the failed-run allowance when it did not finish its
// work: it failed, ran out of time, or was cancelled.
fn retained_as_failure(status: ScriptStatus) -> bool {
    matches!(
        status,
        ScriptStatus::Failed
            | ScriptStatus::TimedOut
            | ScriptStatus::Cancelled
            | ScriptStatus::Interrupted
    )
}

// Of one job's runs (or one event kind's jobless runs), newest first, the
// ids the limits no longer keep: past the newest `runs_per_job`, only the
// newest `failed_runs_per_job` failed runs stay.
fn expired_runs(runs: &[(String, bool)], limits: &EventScriptSettings) -> Vec<String> {
    let mut failed_kept = 0;
    runs.iter()
        .skip(limits.script_output_runs_per_job as usize)
        .filter(|(_, failed)| {
            let keep = *failed && failed_kept < limits.script_output_failed_runs_per_job;
            failed_kept += u32::from(keep);
            !keep
        })
        .map(|(id, _)| id.clone())
        .collect()
}

// A run's id and whether its stored `status` column counts it as failed.
fn run_and_failure(row: &SqlRow) -> Result<(String, bool), StateError> {
    let failed =
        ScriptStatus::from_persisted(&row.text("status")?).is_some_and(retained_as_failure);
    Ok((row.text("id")?, failed))
}

async fn delete_runs(tx: &mut SqlTx<'_>, ids: Vec<String>) -> Result<(), StateError> {
    for chunk in ids.chunks(256) {
        let sql = format!(
            "DELETE FROM script_outputs WHERE id IN ({})",
            vec!["{}"; chunk.len()].join(", ")
        );
        let args = chunk.iter().cloned().map(SqlArg::Text).collect::<Vec<_>>();
        tx.execute(&sql, &args).await?;
    }
    Ok(())
}

// Apply the retention limits to every job and event kind at once.
async fn trim_all_tx(tx: &mut SqlTx<'_>, limits: &EventScriptSettings) -> Result<u64, StateError> {
    tx.execute(
        "UPDATE script_output_state SET next_seq = next_seq WHERE singleton = 1",
        &[],
    )
    .await?;
    let orphans = tx.execute(
        "DELETE FROM script_outputs WHERE
            (job_id IS NOT NULL
             AND NOT EXISTS (SELECT 1 FROM active_jobs WHERE active_jobs.job_id = script_outputs.job_id)
             AND NOT EXISTS (SELECT 1 FROM job_history WHERE job_history.job_id = script_outputs.job_id))
            OR (job_id IS NULL AND event LIKE 'feed:%'
                AND NOT EXISTS (SELECT 1 FROM rss_feeds WHERE script_outputs.event = 'feed:' || CAST(rss_feeds.id AS TEXT)))",
        &[],
    ).await?;
    let trimmed = tx
        .execute(
            "WITH ranked AS (
            SELECT id, job_id, CASE WHEN job_id IS NULL THEN event ELSE NULL END AS event_group,
                seq, status,
                ROW_NUMBER() OVER (
                    PARTITION BY job_id, CASE WHEN job_id IS NULL THEN event ELSE NULL END
                    ORDER BY seq DESC
                ) AS position
            FROM script_outputs
        ), overflow AS (
            SELECT id, status,
                SUM(CASE WHEN status IN ('failed', 'timed_out', 'cancelled', 'interrupted') THEN 1 ELSE 0 END)
                    OVER (PARTITION BY job_id, event_group ORDER BY seq DESC) AS failed_position
            FROM ranked WHERE position > {}
        )
        DELETE FROM script_outputs WHERE id IN (
            SELECT id FROM overflow
            WHERE status NOT IN ('failed', 'timed_out', 'cancelled', 'interrupted') OR failed_position > {}
        )",
            &[
                SqlArg::I64(i64::from(limits.script_output_runs_per_job)),
                SqlArg::I64(i64::from(limits.script_output_failed_runs_per_job)),
            ],
        )
        .await?;
    Ok(orphans + trimmed)
}

// Background trims after a retention limit was lowered: one at a time, and a
// request made while one runs folds into a single further pass.
#[derive(Default)]
pub struct RetentionTrim {
    state: std::sync::Mutex<TrimState>,
    settled: tokio::sync::Notify,
}

#[derive(Default)]
pub struct TrimState {
    pub requested: u64,
    pub completed: u64,
    pub running: bool,
}

impl RetentionTrim {
    pub fn state(&self) -> std::sync::MutexGuard<'_, TrimState> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
}

// Record a result that has no output to keep, such as a script that could
// not be started.
pub async fn retain_result<D: PostprocDatabase>(
    db: D,
    job_id: Option<u64>,
    result: ScriptResult,
    limits: EventScriptSettings,
) -> Result<ScriptResult, StateError> {
    tokio::task::spawn_blocking(move || {
        db.insert_script_output(job_id, result, 0, Vec::new(), limits)
    })
    .await
    .map_err(error)?
}

trait PostProcessingOutputLocalQueries {
    fn insert_script_output(
        &self,
        job_id: Option<u64>,
        result: ScriptResult,
        raw_bytes: u64,
        output: Vec<u8>,
        limits: EventScriptSettings,
    ) -> Result<ScriptResult, StateError>;
}

impl<D: PostprocDatabase> PostProcessingOutputLocalQueries for D {
    fn insert_script_output(
        &self,
        job_id: Option<u64>,
        result: ScriptResult,
        raw_bytes: u64,
        output: Vec<u8>,
        limits: EventScriptSettings,
    ) -> Result<ScriptResult, StateError> {
        let datastore = self.datastore();
        let job_id = job_id.map(i64::try_from).transpose().map_err(error)?;
        let raw_bytes = i64::try_from(raw_bytes).unwrap_or(i64::MAX);
        let retention = super::run_metrics::RetentionTally::default();
        let committed = retention.clone();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "retain_script_output", |tx| {
                let mut result = result.clone();
                let output = output.clone();
                let limits = limits.clone();
                let retention = retention.clone();
                Box::pin(async move {
                    retention.reset();
                    retention.set(RetentionAction::Discarded, 1);
                    // This row serializes insertion and trimming on both SQL backends.
                    let state = tx.fetch_optional("UPDATE script_output_state SET next_seq = next_seq + 1 WHERE singleton = 1 RETURNING next_seq", &[]).await?.ok_or_else(|| error("script output state is missing"))?;
                    let seq = state.i64("next_seq")?;
                    if let Some(instance_id) = &result.instance_id
                        && tx.fetch_optional("SELECT id FROM script_instances WHERE id = {}", &[SqlArg::Text(instance_id.clone())]).await?.is_none() {
                        return Ok(result);
                    }
                    if let super::model::ScriptEventLabel::Feed(id) = result.event
                        && tx.fetch_optional("SELECT id FROM rss_feeds WHERE id = {}", &[SqlArg::I64(id as i64)]).await?.is_none() {
                        return Ok(result);
                    }
                    if let Some(job_id) = job_id
                        && tx.fetch_optional("SELECT job_id FROM active_jobs WHERE job_id = {} UNION ALL SELECT job_id FROM job_history WHERE job_id = {} LIMIT 1", &[SqlArg::I64(job_id), SqlArg::I64(job_id)]).await?.is_none() {
                        return Ok(result);
                    }
                    let stored_bytes = output.len() as i64;
                    let mut entropy = [0_u8; 16];
                    getrandom::fill(&mut entropy).map_err(error)?;
                    let id = format!("script-output-{}", hex::encode(entropy));
                    result.output_id = (stored_bytes > 0).then(|| id.clone());
                    tx.execute("INSERT INTO script_outputs (id, job_id, event, script, status, seq, raw_bytes, truncated, output, stored_bytes, result_json, created_at, instance_id) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})", &[
                        SqlArg::Text(id), SqlArg::OptI64(job_id), SqlArg::Text(result.event.to_string()), SqlArg::Text(result.script.to_string()), SqlArg::Text(result.status.as_str().to_string()), SqlArg::I64(seq), SqlArg::I64(raw_bytes), SqlArg::Bool(result.output_truncated), SqlArg::Bytes(output), SqlArg::I64(stored_bytes), SqlArg::Text(serde_json::to_string(&result).map_err(error)?), SqlArg::I64(result.finished_at_epoch_ms), SqlArg::OptText(result.instance_id.clone()),
                    ]).await?;
                    // A download's runs are kept together; runs that belong to
                    // no download are kept per kind of event.
                    let rows = match job_id {
                        Some(job_id) => tx.fetch_all("SELECT id, status FROM script_outputs WHERE job_id = {} ORDER BY seq DESC", &[SqlArg::I64(job_id)]).await?,
                        None => tx.fetch_all("SELECT id, status FROM script_outputs WHERE job_id IS NULL AND event = {} ORDER BY seq DESC", &[SqlArg::Text(result.event.to_string())]).await?,
                    };
                    let runs = rows
                        .iter()
                        .map(run_and_failure)
                        .collect::<Result<Vec<_>, _>>()?;
                    let expired = expired_runs(&runs, &limits);
                    retention.set(RetentionAction::Discarded, 0);
                    retention.set(RetentionAction::Kept, 1);
                    retention.pruned(super::run_metrics::pruned_scripts(tx, &expired).await?);
                    delete_runs(tx, expired).await?;
                    Ok(result)
                })
            }).await
        })
        .inspect(|_| committed.commit())
    }
}

pub trait PostProcessingOutputQueries {
    fn request_script_output_trim(&self);
    fn script_output_trims_settled(&self) -> impl std::future::Future<Output = ()> + Send;
    fn trim_script_outputs(&self) -> Result<u64, StateError>;
    fn script_output(&self, id: &str) -> Result<Option<String>, StateError>;
    fn script_output_retained(&self, id: &str) -> Result<bool, StateError>;
    fn retained_script_output_ids(&self, job_id: u64) -> Result<BTreeSet<String>, StateError>;
    fn event_script_results(&self, job_id: u64) -> Result<Vec<ScriptResult>, StateError>;
    fn script_runs(
        &self,
        filter: ScriptRunFilter,
        before: Option<i64>,
        limit: u32,
    ) -> Result<Vec<ScriptRun>, StateError>;
    fn script_run_count(&self, filter: &ScriptRunFilter) -> Result<u64, StateError>;
    fn script_run_status_counts(
        &self,
        filter: &ScriptRunFilter,
    ) -> Result<Vec<(ScriptStatus, u64)>, StateError>;
}

impl<D: PostprocDatabase> PostProcessingOutputQueries for D {
    // Trim every job and event kind to the stored retention limits, in the
    // background, after a limit was lowered. Returns at once; see
    // [`Database::script_output_trims_settled`].
    fn request_script_output_trim(&self) {
        let trim = &self.script_runtime().trim;
        {
            let mut state = trim.state();
            state.requested += 1;
            if state.running {
                // The pass under way, or the one after it, sees this request.
                return;
            }
            state.running = true;
        }
        let db = self.clone();
        let pass = async move {
            loop {
                let target = db.script_runtime().trim.state().requested;
                let worker = db.clone();
                let outcome = tokio::task::spawn_blocking(move || worker.trim_script_outputs())
                    .await
                    .map_err(error)
                    .and_then(|outcome| outcome);
                if let Err(error) = outcome {
                    tracing::warn!(%error, "could not trim retained script runs");
                }
                let trim = &db.script_runtime().trim;
                let again = {
                    let mut state = trim.state();
                    state.completed = target;
                    state.running = state.requested > target;
                    state.running
                };
                trim.settled.notify_waiters();
                if !again {
                    break;
                }
            }
        };
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(pass);
            }
            // Outside a runtime nothing is waiting on a request path: trim
            // here.
            Err(_) => {
                drop(pass);
                if let Err(error) = self.trim_script_outputs() {
                    tracing::warn!(%error, "could not trim retained script runs");
                }
                let mut state = self.script_runtime().trim.state();
                state.completed = state.requested;
                state.running = false;
            }
        }
    }

    // Wait until every trim requested before this call has finished.
    async fn script_output_trims_settled(&self) {
        let trim = &self.script_runtime().trim;
        let target = trim.state().requested;
        loop {
            let settled = trim.settled.notified();
            tokio::pin!(settled);
            settled.as_mut().enable();
            if trim.state().completed >= target {
                return;
            }
            settled.await;
        }
    }

    // Apply the stored retention limits to every recorded run now.
    fn trim_script_outputs(&self) -> Result<u64, StateError> {
        let limits = self.post_processing_settings()?.event_scripts;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "trim_script_outputs", |tx| {
                let limits = limits.clone();
                Box::pin(async move { trim_all_tx(tx, &limits).await })
            })
            .await
        })
        .inspect(|pruned| super::run_metrics::record_retention(RetentionAction::Pruned, *pruned))
    }

    fn script_output(&self, id: &str) -> Result<Option<String>, StateError> {
        let datastore = self.datastore();
        let id = id.to_string();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT output FROM script_outputs WHERE id = {} AND stored_bytes > 0",
                &[SqlArg::Text(id)],
            )
            .await?;
            row.map(|row| decode_output(&row.bytes("output")?, MAX_DECODE_BYTES))
                .transpose()
        })
    }

    fn script_output_retained(&self, id: &str) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let id = id.to_string();
        self.run_sql_blocking_read(async move {
            Ok(SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT id FROM script_outputs WHERE id = {} AND stored_bytes > 0",
                &[SqlArg::Text(id)],
            )
            .await?
            .is_some())
        })
    }

    fn retained_script_output_ids(&self, job_id: u64) -> Result<BTreeSet<String>, StateError> {
        let datastore = self.datastore();
        let job_id = i64::try_from(job_id).map_err(error)?;
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT id FROM script_outputs WHERE job_id = {} AND stored_bytes > 0",
                &[SqlArg::I64(job_id)],
            )
            .await?
            .into_iter()
            .map(|row| row.text("id"))
            .collect()
        })
    }

    // The job's runs that are not part of a post-processing pass: its event
    // scripts, and the post-processing scripts nothing waited for.
    fn event_script_results(&self, job_id: u64) -> Result<Vec<ScriptResult>, StateError> {
        let datastore = self.datastore();
        let job_id = i64::try_from(job_id).map_err(error)?;
        let results: Vec<ScriptResult> = self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(datastore.read_exec(), "SELECT result_json FROM script_outputs WHERE job_id = {} ORDER BY seq DESC LIMIT 128", &[SqlArg::I64(job_id)]).await?.into_iter().map(|row| serde_json::from_str(&row.text("result_json")?).map_err(error)).collect()
        })?;
        Ok(results
            .into_iter()
            .filter(|result| result.event != ScriptEventLabel::PostProcessing || result.background)
            .collect())
    }

    // Recorded runs, latest first. `before` continues a listing below the
    // `seq` of the last run already returned.
    fn script_runs(
        &self,
        filter: ScriptRunFilter,
        before: Option<i64>,
        limit: u32,
    ) -> Result<Vec<ScriptRun>, StateError> {
        let datastore = self.datastore();
        let (mut conditions, mut args) = script_run_conditions(&filter)?;
        if let Some(before) = before {
            conditions.push("o.seq < {}");
            args.push(SqlArg::I64(before));
        }
        let mut sql = String::from(
            "SELECT o.id, o.seq, o.job_id, o.stored_bytes, o.result_json, h.name AS job_name FROM script_outputs o LEFT JOIN job_history h ON h.job_id = o.job_id",
        );
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }
        sql.push_str(" ORDER BY o.seq DESC LIMIT {}");
        args.push(SqlArg::I64(i64::from(limit)));
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args)
                .await?
                .into_iter()
                .map(|row| {
                    Ok(ScriptRun {
                        id: row.text("id")?,
                        seq: row.i64("seq")?,
                        job_id: row
                            .opt_i64("job_id")?
                            .map(u64::try_from)
                            .transpose()
                            .map_err(error)?,
                        job_name: row.opt_text("job_name")?,
                        output_retained: row.i64("stored_bytes")? > 0,
                        result: serde_json::from_str(&row.text("result_json")?).map_err(error)?,
                    })
                })
                .collect()
        })
    }

    // How many recorded runs the filter matches, across every page.
    fn script_run_count(&self, filter: &ScriptRunFilter) -> Result<u64, StateError> {
        let datastore = self.datastore();
        let (conditions, args) = script_run_conditions(filter)?;
        let mut sql = String::from("SELECT COUNT(*) AS total FROM script_outputs o");
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }
        let total = self.run_sql_blocking_read(async move {
            match SqlRuntime::fetch_optional(datastore.read_exec(), &sql, &args).await? {
                Some(row) => row.i64("total"),
                None => Ok(0),
            }
        })?;
        u64::try_from(total).map_err(error)
    }

    // How many of the runs the filter matches ended each way. The filter's
    // own status is left out, so the answer covers every status at once.
    fn script_run_status_counts(
        &self,
        filter: &ScriptRunFilter,
    ) -> Result<Vec<(ScriptStatus, u64)>, StateError> {
        let datastore = self.datastore();
        let (conditions, args) = script_run_conditions(&ScriptRunFilter {
            status: None,
            ..filter.clone()
        })?;
        let mut sql = String::from("SELECT o.status, COUNT(*) AS total FROM script_outputs o");
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }
        sql.push_str(" GROUP BY o.status");
        self.run_sql_blocking_read(async move {
            let mut counts = Vec::new();
            for row in SqlRuntime::fetch_all(datastore.read_exec(), &sql, &args).await? {
                // A status this build does not know is counted under no tab.
                if let Some(status) = ScriptStatus::from_persisted(&row.text("status")?) {
                    counts.push((status, u64::try_from(row.i64("total")?).map_err(error)?));
                }
            }
            Ok(counts)
        })
    }
}

// The SQL conditions, over `script_outputs o`, that select the runs a filter matches.
fn script_run_conditions(
    filter: &ScriptRunFilter,
) -> Result<(Vec<&'static str>, Vec<SqlArg>), StateError> {
    let mut conditions = Vec::new();
    let mut args = Vec::new();
    if let Some(job_id) = filter.job_id {
        conditions.push("o.job_id = {}");
        args.push(SqlArg::I64(i64::try_from(job_id).map_err(error)?));
    }
    if let Some(script) = &filter.script {
        conditions.push("o.script = {}");
        args.push(SqlArg::Text(script.clone()));
    }
    if let Some(kind) = filter.kind {
        conditions.push(match kind {
            ScriptKind::PostProcessing => "o.event = 'post_processing'",
            ScriptKind::Queue => "o.event LIKE 'queue:%'",
            ScriptKind::Scan => "o.event = 'scan'",
            ScriptKind::Scheduler => "o.event LIKE 'scheduler:%'",
            ScriptKind::Feed => "o.event LIKE 'feed:%'",
        });
    }
    if let Some(status) = filter.status {
        conditions.push("o.status = {}");
        args.push(SqlArg::Text(status.as_str().to_string()));
    }
    Ok((conditions, args))
}

// The script rows of a job are deleted with its other rows, in weaver-server-jobs.
pub use weaver_server_jobs::jobs::child_rows::{
    delete_script_state_rows_tx, delete_script_state_tx,
};
