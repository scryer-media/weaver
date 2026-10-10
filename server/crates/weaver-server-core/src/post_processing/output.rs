use std::collections::BTreeSet;
use std::io::Read;

use super::model::{EventScriptSettings, ScriptEventLabel, ScriptKind, ScriptResult, ScriptStatus};
use crate::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime, SqlTx};
use crate::persistence::{Database, StateError};

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
const COMPRESSION_LEVEL: i32 = 19;

// Record a run and its kept output. `raw_bytes` is everything the script
// wrote, of which `output` is the newest part.
pub async fn retain_output(
    db: Database,
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
        ScriptStatus::Failed | ScriptStatus::TimedOut | ScriptStatus::Cancelled
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
                SUM(CASE WHEN status IN ('failed', 'timed_out', 'cancelled') THEN 1 ELSE 0 END)
                    OVER (PARTITION BY job_id, event_group ORDER BY seq DESC) AS failed_position
            FROM ranked WHERE position > {}
        )
        DELETE FROM script_outputs WHERE id IN (
            SELECT id FROM overflow
            WHERE status NOT IN ('failed', 'timed_out', 'cancelled') OR failed_position > {}
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
pub(crate) struct RetentionTrim {
    state: std::sync::Mutex<TrimState>,
    settled: tokio::sync::Notify,
}

#[derive(Default)]
struct TrimState {
    requested: u64,
    completed: u64,
    running: bool,
}

impl RetentionTrim {
    fn state(&self) -> std::sync::MutexGuard<'_, TrimState> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
}

// Record a result that has no output to keep, such as a script that could
// not be started.
pub(crate) async fn retain_result(
    db: Database,
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

impl Database {
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
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "retain_script_output", |tx| {
                let mut result = result.clone();
                let output = output.clone();
                let limits = limits.clone();
                Box::pin(async move {
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
                    delete_runs(tx, expired_runs(&runs, &limits)).await?;
                    Ok(result)
                })
            }).await
        })
    }

    // Trim every job and event kind to the stored retention limits, in the
    // background, after a limit was lowered. Returns at once; see
    // [`Database::script_output_trims_settled`].
    pub fn request_script_output_trim(&self) {
        let trim = &self.script_runtime.trim;
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
                let target = db.script_runtime.trim.state().requested;
                let worker = db.clone();
                let outcome = tokio::task::spawn_blocking(move || worker.trim_script_outputs())
                    .await
                    .map_err(error)
                    .and_then(|outcome| outcome);
                if let Err(error) = outcome {
                    tracing::warn!(%error, "could not trim retained script runs");
                }
                let trim = &db.script_runtime.trim;
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
                let mut state = self.script_runtime.trim.state();
                state.completed = state.requested;
                state.running = false;
            }
        }
    }

    // Wait until every trim requested before this call has finished.
    pub async fn script_output_trims_settled(&self) {
        let trim = &self.script_runtime.trim;
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
    pub fn trim_script_outputs(&self) -> Result<u64, StateError> {
        let limits = self.post_processing_settings()?.event_scripts;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "trim_script_outputs", |tx| {
                let limits = limits.clone();
                Box::pin(async move { trim_all_tx(tx, &limits).await })
            })
            .await
        })
    }

    pub fn script_output(&self, id: &str) -> Result<Option<String>, StateError> {
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

    pub fn script_output_retained(&self, id: &str) -> Result<bool, StateError> {
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

    pub fn retained_script_output_ids(&self, job_id: u64) -> Result<BTreeSet<String>, StateError> {
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
    pub fn event_script_results(&self, job_id: u64) -> Result<Vec<ScriptResult>, StateError> {
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
    pub fn script_runs(
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
    pub fn script_run_count(&self, filter: &ScriptRunFilter) -> Result<u64, StateError> {
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
    pub fn script_run_status_counts(
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

// Archive moves active rows to history, so deletion is explicit rather than an FK cascade.
pub(crate) async fn delete_script_state_tx(
    tx: &mut SqlTx<'_>,
    job_id: i64,
) -> Result<(), StateError> {
    // Taken in the same order as insertion takes it.
    tx.execute(
        "UPDATE script_output_state SET next_seq = next_seq WHERE singleton = 1",
        &[],
    )
    .await?;
    tx.execute(
        "DELETE FROM script_outputs WHERE job_id = {}",
        &[SqlArg::I64(job_id)],
    )
    .await?;
    tx.execute(
        "DELETE FROM script_event_queue WHERE job_id = {}",
        &[SqlArg::I64(job_id)],
    )
    .await?;
    tx.execute(
        "DELETE FROM script_job_state WHERE job_id = {}",
        &[SqlArg::I64(job_id)],
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::model::{ScriptAdapter, ScriptName};
    use super::*;

    fn feed(db: &Database, id: u32) {
        db.insert_rss_feed(&crate::RssFeedRow {
            id,
            name: format!("feed {id}"),
            url: "https://example.test/feed".into(),
            enabled: true,
            poll_interval_secs: 60,
            username: None,
            password: None,
            default_category: None,
            default_metadata: Vec::new(),
            scripts: Vec::new(),
            etag: None,
            last_modified: None,
            last_polled_at: None,
            last_success_at: None,
            last_error: None,
            consecutive_failures: 0,
        })
        .unwrap();
    }

    fn active(db: &Database, id: u64) {
        db.create_active_job(&crate::ActiveJob {
            job_id: crate::JobId(id),
            nzb_hash: [id as u8; 32],
            nzb_path: "fixture.nzb".into(),
            nzb_zstd: Vec::new(),
            output_dir: "fixture".into(),
            created_at: 1,
            category: None,
            metadata: Vec::new(),
            status: "downloading",
            download_state: "downloading",
            post_state: "idle",
            run_state: "active",
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            password_override: None,
        })
        .unwrap();
    }

    // Record a run whose kept output is everything it wrote.
    async fn retain_output(
        db: Database,
        job_id: Option<u64>,
        result: ScriptResult,
        output: Vec<u8>,
        limits: EventScriptSettings,
    ) -> Result<ScriptResult, StateError> {
        let written = output.len() as u64;
        super::retain_output(db, job_id, result, output, written, limits).await
    }

    fn result(event: ScriptEventLabel) -> ScriptResult {
        ScriptResult {
            script: ScriptName::new("test.sh").unwrap(),
            instance_id: None,
            instance_name: None,
            event,
            output_id: None,
            background: false,
            adapter: ScriptAdapter::Nzbget,
            status: ScriptStatus::Succeeded,
            exit_code: Some(93),
            duration_ms: 1,
            output_tail: String::new(),
            output_truncated: false,
            error_message: None,
            finished_at_epoch_ms: 1,
        }
    }

    #[test]
    fn decode_is_bounded_and_rejects_uncompressed_data() {
        assert!(decode_output(b"invalid frame", 64).is_err());
        let bytes = zstd::stream::encode_all(&b"1234567"[..], COMPRESSION_LEVEL).unwrap();
        assert!(decode_output(&bytes, 6).is_err());
        assert_eq!(decode_output(&bytes, 7).unwrap(), "1234567");
    }

    #[tokio::test]
    async fn effect_cache_tracks_writes_deletion_and_reopening() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("effects.db");
        let db = Database::open(&path).unwrap();
        active(&db, 1);
        assert!(!db.job_script_effects(1).unwrap().marked_bad);
        let effects = super::super::effects::JobScriptEffects {
            marked_bad: true,
            parameters: [("fixture".into(), "saved".into())].into(),
            ..Default::default()
        };
        db.save_job_script_effects(1, &effects).unwrap();
        assert!(db.clone().job_script_effects(1).unwrap().marked_bad);
        db.close().unwrap();
        let reopened = Database::open(&path).unwrap();
        let restored = reopened.job_script_effects(1).unwrap();
        assert!(restored.marked_bad);
        assert_eq!(restored.parameters.get("fixture").unwrap(), "saved");
        reopened.delete_active_job(crate::JobId(1)).unwrap();
        let removed = reopened.job_script_effects(1).unwrap();
        assert!(!removed.marked_bad);
        assert!(removed.parameters.is_empty());
    }

    #[tokio::test]
    async fn deleting_a_feed_removes_its_runs_and_refuses_late_results() {
        let db = Database::open_in_memory().unwrap();
        feed(&db, 1);
        let saved = retain_output(
            db.clone(),
            None,
            result(ScriptEventLabel::Feed(1)),
            b"before deletion".to_vec(),
            EventScriptSettings::default(),
        )
        .await
        .unwrap();
        assert!(db.delete_rss_feed(1).unwrap());
        assert!(
            !db.script_output_retained(saved.output_id.as_deref().unwrap())
                .unwrap()
        );
        let late = retain_output(
            db.clone(),
            None,
            result(ScriptEventLabel::Feed(1)),
            b"after deletion".to_vec(),
            EventScriptSettings::default(),
        )
        .await
        .unwrap();
        assert!(late.output_id.is_none());
        assert!(runs_of(&db, None).is_empty());
    }

    #[tokio::test]
    async fn periodic_retention_removes_preexisting_orphan_groups() {
        let db = Database::open_in_memory().unwrap();
        feed(&db, 1);
        retain_output(
            db.clone(),
            None,
            result(ScriptEventLabel::Feed(1)),
            b"orphan".to_vec(),
            EventScriptSettings::default(),
        )
        .await
        .unwrap();
        let datastore = db.datastore();
        db.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "remove_feed_fixture", |tx| {
                Box::pin(async move { tx.execute("DELETE FROM rss_feeds WHERE id = 1", &[]).await })
            })
            .await
        })
        .unwrap();
        assert_eq!(db.trim_script_outputs().unwrap(), 1);
        assert!(runs_of(&db, None).is_empty());
    }

    #[tokio::test]
    async fn jobless_output_run_cap_is_scoped_to_event_label() {
        let db = Database::open_in_memory().unwrap();
        feed(&db, 1);
        feed(&db, 2);
        let limits = EventScriptSettings {
            script_output_runs_per_job: 1,
            ..Default::default()
        };
        let first = retain_output(
            db.clone(),
            None,
            result(ScriptEventLabel::Feed(1)),
            b"first".to_vec(),
            limits.clone(),
        )
        .await
        .unwrap();
        let other_feed = retain_output(
            db.clone(),
            None,
            result(ScriptEventLabel::Feed(2)),
            b"other".to_vec(),
            limits.clone(),
        )
        .await
        .unwrap();
        let second = retain_output(
            db.clone(),
            None,
            result(ScriptEventLabel::Feed(1)),
            b"second".to_vec(),
            limits,
        )
        .await
        .unwrap();
        assert!(
            !db.script_output_retained(first.output_id.as_deref().unwrap())
                .unwrap()
        );
        assert!(
            db.script_output_retained(other_feed.output_id.as_deref().unwrap())
                .unwrap()
        );
        assert!(
            db.script_output_retained(second.output_id.as_deref().unwrap())
                .unwrap()
        );
    }

    #[tokio::test]
    async fn per_job_ring_evicts_oldest_and_retains_excerpts() {
        let db = Database::open_in_memory().unwrap();
        active(&db, 1);
        let limits = EventScriptSettings {
            script_output_runs_per_job: 1,
            ..Default::default()
        };
        let first = retain_output(
            db.clone(),
            Some(1),
            result(ScriptEventLabel::PostProcessing),
            b"first".to_vec(),
            limits.clone(),
        )
        .await
        .unwrap();
        let second = retain_output(
            db.clone(),
            Some(1),
            result(ScriptEventLabel::PostProcessing),
            b"second".to_vec(),
            limits,
        )
        .await
        .unwrap();
        assert_eq!(first.output_tail, "first");
        assert_eq!(
            db.script_output(first.output_id.as_deref().unwrap())
                .unwrap(),
            None
        );
        assert_eq!(
            db.script_output(second.output_id.as_deref().unwrap())
                .unwrap()
                .as_deref(),
            Some("second")
        );
        let retained = db.retained_script_output_ids(1).unwrap();
        assert!(!retained.contains(first.output_id.as_deref().unwrap()));
        assert!(retained.contains(second.output_id.as_deref().unwrap()));
    }

    fn with_status(event: ScriptEventLabel, status: ScriptStatus) -> ScriptResult {
        ScriptResult {
            status,
            ..result(event)
        }
    }

    fn runs_of(db: &Database, job_id: Option<u64>) -> Vec<ScriptRun> {
        db.script_runs(Default::default(), None, 500)
            .unwrap()
            .into_iter()
            .filter(|run| run.job_id == job_id)
            .collect()
    }

    fn tails(runs: &[ScriptRun]) -> Vec<String> {
        runs.iter()
            .map(|run| run.result.output_tail.clone())
            .collect()
    }

    #[tokio::test]
    async fn a_job_keeps_only_its_newest_runs_rows_and_output_together() {
        let db = Database::open_in_memory().unwrap();
        active(&db, 1);
        active(&db, 2);
        let limits = EventScriptSettings {
            script_output_runs_per_job: 3,
            ..Default::default()
        };
        let mut kept = Vec::new();
        for n in 0..6 {
            kept.push(
                retain_output(
                    db.clone(),
                    Some(1),
                    result(ScriptEventLabel::PostProcessing),
                    format!("run {n}").into_bytes(),
                    limits.clone(),
                )
                .await
                .unwrap(),
            );
        }
        retain_output(
            db.clone(),
            Some(2),
            result(ScriptEventLabel::PostProcessing),
            b"other job".to_vec(),
            limits,
        )
        .await
        .unwrap();
        assert_eq!(tails(&runs_of(&db, Some(1))), ["run 5", "run 4", "run 3"]);
        assert_eq!(tails(&runs_of(&db, Some(2))), ["other job"]);
        // A run that is no longer kept leaves no output behind either.
        assert_eq!(
            db.script_output(kept[0].output_id.as_deref().unwrap())
                .unwrap(),
            None
        );
        assert_eq!(
            db.script_output(kept[5].output_id.as_deref().unwrap())
                .unwrap()
                .as_deref(),
            Some("run 5")
        );
    }

    #[tokio::test]
    async fn failed_runs_outlast_the_newest_runs_up_to_their_own_limit() {
        let db = Database::open_in_memory().unwrap();
        active(&db, 1);
        let limits = EventScriptSettings {
            script_output_runs_per_job: 2,
            script_output_failed_runs_per_job: 2,
            ..Default::default()
        };
        let recorded = [
            ("failed 0", ScriptStatus::Failed),
            ("ok 1", ScriptStatus::Succeeded),
            ("timed out 2", ScriptStatus::TimedOut),
            ("warning 3", ScriptStatus::Warning),
            ("cancelled 4", ScriptStatus::Cancelled),
            ("skipped 5", ScriptStatus::Skipped),
            ("ok 6", ScriptStatus::Succeeded),
            ("ok 7", ScriptStatus::Succeeded),
        ];
        for (output, status) in recorded {
            retain_output(
                db.clone(),
                Some(1),
                with_status(ScriptEventLabel::PostProcessing, status),
                output.as_bytes().to_vec(),
                limits.clone(),
            )
            .await
            .unwrap();
        }
        // The newest two, then the newest two failures; the oldest failure,
        // and every older run that did not fail, are gone.
        assert_eq!(
            tails(&runs_of(&db, Some(1))),
            ["ok 7", "ok 6", "cancelled 4", "timed out 2"]
        );
    }

    #[tokio::test]
    async fn runs_without_a_job_are_kept_per_event() {
        let db = Database::open_in_memory().unwrap();
        feed(&db, 1);
        feed(&db, 2);
        let limits = EventScriptSettings {
            script_output_runs_per_job: 1,
            script_output_failed_runs_per_job: 1,
            ..Default::default()
        };
        for (event, output, status) in [
            (ScriptEventLabel::Scan, "scan failed", ScriptStatus::Failed),
            (ScriptEventLabel::Scan, "scan old", ScriptStatus::Succeeded),
            // Each feed keeps runs of its own.
            (ScriptEventLabel::Feed(1), "feed 1", ScriptStatus::Succeeded),
            (ScriptEventLabel::Feed(2), "feed 2", ScriptStatus::Succeeded),
            (ScriptEventLabel::Scan, "scan new", ScriptStatus::Succeeded),
        ] {
            retain_output(
                db.clone(),
                None,
                with_status(event, status),
                output.as_bytes().to_vec(),
                limits.clone(),
            )
            .await
            .unwrap();
        }
        assert_eq!(
            tails(&runs_of(&db, None)),
            ["scan new", "feed 2", "feed 1", "scan failed"]
        );
    }

    #[tokio::test]
    async fn lowering_a_limit_trims_every_job_and_event_kind_soon_after_the_save() {
        let db = Database::open_in_memory().unwrap();
        active(&db, 1);
        active(&db, 2);
        let generous = EventScriptSettings::default();
        for job_id in [Some(1), Some(2), None] {
            for (n, status) in [
                ScriptStatus::Failed,
                ScriptStatus::Succeeded,
                ScriptStatus::Failed,
                ScriptStatus::Succeeded,
                ScriptStatus::Succeeded,
            ]
            .into_iter()
            .enumerate()
            {
                retain_output(
                    db.clone(),
                    job_id,
                    with_status(ScriptEventLabel::Scan, status),
                    format!("{n}").into_bytes(),
                    generous.clone(),
                )
                .await
                .unwrap();
            }
        }
        let mut settings = db.post_processing_settings().unwrap();
        // Raising a limit deletes nothing.
        settings.event_scripts.script_output_runs_per_job = 64;
        db.save_post_processing_settings_preserving_extensions(settings.clone(), true)
            .unwrap();
        db.script_output_trims_settled().await;
        assert_eq!(db.script_run_count(&Default::default()).unwrap(), 15);

        settings.event_scripts.script_output_runs_per_job = 2;
        settings.event_scripts.script_output_failed_runs_per_job = 1;
        db.save_post_processing_settings_preserving_extensions(settings, true)
            .unwrap();
        db.script_output_trims_settled().await;
        for job_id in [Some(1), Some(2), None] {
            assert_eq!(tails(&runs_of(&db, job_id)), ["4", "3", "2"]);
        }
    }

    #[tokio::test]
    async fn a_trim_requested_while_one_runs_is_folded_into_one_more_pass() {
        let db = Database::open_in_memory().unwrap();
        active(&db, 1);
        for n in 0..4 {
            retain_output(
                db.clone(),
                Some(1),
                result(ScriptEventLabel::PostProcessing),
                format!("{n}").into_bytes(),
                EventScriptSettings::default(),
            )
            .await
            .unwrap();
        }
        let mut settings = db.post_processing_settings().unwrap();
        settings.event_scripts.script_output_runs_per_job = 3;
        db.save_post_processing_settings(&settings).unwrap();
        settings.event_scripts.script_output_runs_per_job = 1;
        db.save_post_processing_settings(&settings).unwrap();
        db.script_output_trims_settled().await;
        // The later, lower limit is the one that holds.
        assert_eq!(tails(&runs_of(&db, Some(1))), ["3"]);
        let state = db.script_runtime.trim.state();
        assert!(!state.running);
        assert_eq!(state.completed, state.requested);
    }

    #[tokio::test]
    async fn the_stored_run_counts_every_byte_written_and_is_compressed_hard() {
        let db = Database::open_in_memory().unwrap();
        active(&db, 1);
        let output = b"kept tail\n".repeat(100);
        let kept = super::retain_output(
            db.clone(),
            Some(1),
            ScriptResult {
                output_truncated: true,
                ..result(ScriptEventLabel::PostProcessing)
            },
            output.clone(),
            1_000_000,
            EventScriptSettings::default(),
        )
        .await
        .unwrap();
        let id = kept.output_id.clone().unwrap();
        let datastore = db.datastore();
        let row = db
            .run_sql_blocking_read(async move {
                SqlRuntime::fetch_optional(
                    datastore.read_exec(),
                    "SELECT raw_bytes, truncated, output FROM script_outputs WHERE id = {}",
                    &[SqlArg::Text(id)],
                )
                .await
            })
            .unwrap()
            .unwrap();
        assert_eq!(row.i64("raw_bytes").unwrap(), 1_000_000);
        assert!(row.bool("truncated").unwrap());
        assert_eq!(
            row.bytes("output").unwrap(),
            zstd::stream::encode_all(output.as_slice(), 19).unwrap()
        );
        let runs = runs_of(&db, Some(1));
        assert!(runs[0].result.output_truncated);
    }

    #[tokio::test]
    async fn deleted_jobs_cannot_recreate_output() {
        let db = Database::open_in_memory().unwrap();
        active(&db, 1);
        let event = ScriptEventLabel::Queue(super::super::model::QueueEvent::NzbAdded);
        retain_output(
            db.clone(),
            Some(1),
            result(event.clone()),
            b"excerpt".to_vec(),
            Default::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            db.event_script_results(1).unwrap()[0].output_tail,
            "excerpt"
        );
        db.delete_active_job(crate::JobId(1)).unwrap();
        let late = retain_output(
            db.clone(),
            Some(1),
            result(event),
            b"late".to_vec(),
            Default::default(),
        )
        .await
        .unwrap();
        assert!(late.output_id.is_none());
        assert!(db.event_script_results(1).unwrap().is_empty());
    }

    fn named(event: ScriptEventLabel, script: &str) -> ScriptResult {
        ScriptResult {
            script: ScriptName::new(script).unwrap(),
            ..result(event)
        }
    }

    fn history(db: &Database, job_id: u64, name: &str) {
        db.insert_job_history(&crate::JobHistoryRow {
            job_id,
            job_hash: None,
            name: name.into(),
            status: "complete".into(),
            error_message: None,
            total_bytes: 0,
            downloaded_bytes: 0,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1_000,
            category: None,
            output_dir: None,
            nzb_path: None,
            created_at: 1,
            completed_at: 2,
            metadata: None,
            server_attribution: None,
        })
        .unwrap();
    }

    #[tokio::test]
    async fn a_jobs_runs_outside_its_pass_include_the_scripts_nothing_waited_for() {
        let db = Database::open_in_memory().unwrap();
        active(&db, 1);
        let limits = EventScriptSettings::default();
        for (script, background) in [("pass.sh", false), ("detached.sh", true)] {
            let result = ScriptResult {
                background,
                ..named(ScriptEventLabel::PostProcessing, script)
            };
            retain_output(db.clone(), Some(1), result, b"out".to_vec(), limits.clone())
                .await
                .unwrap();
        }
        let results = db.event_script_results(1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].script.as_str(), "detached.sh");
        assert!(results[0].background);
    }

    #[tokio::test]
    async fn an_empty_output_is_recorded_without_a_retained_frame() {
        let db = Database::open_in_memory().unwrap();
        let kept = retain_output(
            db.clone(),
            None,
            result(ScriptEventLabel::Scan),
            Vec::new(),
            EventScriptSettings::default(),
        )
        .await
        .unwrap();
        assert!(kept.output_id.is_none());
        let runs = db.script_runs(Default::default(), None, 10).unwrap();
        assert_eq!(runs.len(), 1);
        assert!(!runs[0].output_retained);
        assert!(db.script_output(&runs[0].id).unwrap().is_none());
    }

    #[tokio::test]
    async fn a_result_with_nothing_to_show_is_kept_without_output() {
        let db = Database::open_in_memory().unwrap();
        active(&db, 1);
        let kept = retain_result(
            db.clone(),
            Some(1),
            named(ScriptEventLabel::PostProcessing, "missing.sh"),
            EventScriptSettings::default(),
        )
        .await
        .unwrap();
        assert!(kept.output_id.is_none());
        let runs = db.script_runs(Default::default(), None, 10).unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].result.script.as_str(), "missing.sh");
        assert!(!runs[0].output_retained);
    }

    #[tokio::test]
    async fn runs_are_listed_latest_first_whatever_started_them() {
        let db = Database::open_in_memory().unwrap();
        feed(&db, 9);
        active(&db, 1);
        history(&db, 2, "finished job");
        let limits = EventScriptSettings::default();
        let added = ScriptEventLabel::Queue(super::super::model::QueueEvent::NzbAdded);
        let recorded = [
            (Some(1), ScriptEventLabel::PostProcessing, "pass.sh"),
            (Some(2), added, "queue.sh"),
            (None, ScriptEventLabel::Scan, "scan.sh"),
            (None, ScriptEventLabel::Scheduler(4), "nightly.sh"),
            (None, ScriptEventLabel::Feed(9), "feed.sh"),
            (Some(2), ScriptEventLabel::PostProcessing, "pass.sh"),
        ];
        for (job_id, event, script) in recorded.clone() {
            retain_output(
                db.clone(),
                job_id,
                named(event, script),
                script.as_bytes().to_vec(),
                limits.clone(),
            )
            .await
            .unwrap();
        }
        let scripts = |runs: &[ScriptRun]| {
            runs.iter()
                .map(|run| (run.job_id, run.result.script.to_string()))
                .collect::<Vec<_>>()
        };

        let all = db.script_runs(Default::default(), None, 10).unwrap();
        assert_eq!(
            scripts(&all),
            recorded
                .iter()
                .rev()
                .map(|(job_id, _, script)| (*job_id, script.to_string()))
                .collect::<Vec<_>>()
        );
        assert!(all.iter().all(|run| run.output_retained));
        assert!(all.windows(2).all(|pair| pair[0].seq > pair[1].seq));
        // A job still in the queue has no name to show; one in history does.
        assert_eq!(all[0].job_name.as_deref(), Some("finished job"));
        assert_eq!(all[5].job_name, None);
        assert_eq!(
            db.script_output(&all[2].id).unwrap().as_deref(),
            Some("nightly.sh"),
            "a run that belongs to no job still has its output"
        );

        // Each page continues below the last run of the one before it.
        let first = db.script_runs(Default::default(), None, 4).unwrap();
        assert_eq!(scripts(&first), scripts(&all[..4]));
        let rest = db
            .script_runs(Default::default(), Some(first[3].seq), 4)
            .unwrap();
        assert_eq!(scripts(&rest), scripts(&all[4..]));

        let by = |filter: ScriptRunFilter| scripts(&db.script_runs(filter, None, 10).unwrap());
        let kind = |kind| ScriptRunFilter {
            kind: Some(kind),
            ..Default::default()
        };
        assert_eq!(
            by(kind(ScriptKind::PostProcessing)),
            [(Some(2), "pass.sh".into()), (Some(1), "pass.sh".into())]
        );
        assert_eq!(by(kind(ScriptKind::Queue)), [(Some(2), "queue.sh".into())]);
        assert_eq!(by(kind(ScriptKind::Scan)), [(None, "scan.sh".into())]);
        assert_eq!(
            by(kind(ScriptKind::Scheduler)),
            [(None, "nightly.sh".into())]
        );
        assert_eq!(by(kind(ScriptKind::Feed)), [(None, "feed.sh".into())]);
        assert_eq!(
            by(ScriptRunFilter {
                job_id: Some(2),
                ..Default::default()
            }),
            [(Some(2), "pass.sh".into()), (Some(2), "queue.sh".into())]
        );
        assert_eq!(
            by(ScriptRunFilter {
                script: Some("pass.sh".into()),
                job_id: Some(1),
                ..Default::default()
            }),
            [(Some(1), "pass.sh".into())]
        );
    }

    #[tokio::test]
    async fn the_run_count_is_the_filtered_set_across_every_page() {
        let db = Database::open_in_memory().unwrap();
        feed(&db, 9);
        active(&db, 1);
        let limits = EventScriptSettings::default();
        let recorded = [
            (Some(1), ScriptEventLabel::PostProcessing, "pass.sh"),
            (None, ScriptEventLabel::Scan, "scan.sh"),
            (Some(1), ScriptEventLabel::PostProcessing, "tidy.sh"),
            (None, ScriptEventLabel::Feed(9), "feed.sh"),
            (None, ScriptEventLabel::Scan, "scan.sh"),
        ];
        for (job_id, event, script) in recorded {
            retain_output(
                db.clone(),
                job_id,
                named(event, script),
                script.as_bytes().to_vec(),
                limits.clone(),
            )
            .await
            .unwrap();
        }
        let kind = |kind| ScriptRunFilter {
            kind: Some(kind),
            ..Default::default()
        };

        assert_eq!(db.script_run_count(&Default::default()).unwrap(), 5);
        assert_eq!(db.script_run_count(&kind(ScriptKind::Scan)).unwrap(), 2);
        assert_eq!(
            db.script_run_count(&kind(ScriptKind::PostProcessing))
                .unwrap(),
            2
        );
        assert_eq!(db.script_run_count(&kind(ScriptKind::Queue)).unwrap(), 0);
        assert_eq!(
            db.script_run_count(&ScriptRunFilter {
                job_id: Some(1),
                script: Some("tidy.sh".into()),
                ..Default::default()
            })
            .unwrap(),
            1
        );
        // A page is smaller than the set it is drawn from; the count is not.
        assert_eq!(
            db.script_runs(kind(ScriptKind::Scan), None, 1)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(db.script_run_count(&kind(ScriptKind::Scan)).unwrap(), 2);
    }

    #[tokio::test]
    async fn runs_are_listed_and_counted_by_how_they_ended() {
        let db = Database::open_in_memory().unwrap();
        feed(&db, 9);
        let limits = EventScriptSettings::default();
        let recorded = [
            (ScriptEventLabel::Scan, "a.sh", ScriptStatus::Succeeded),
            (ScriptEventLabel::Scan, "b.sh", ScriptStatus::Failed),
            (ScriptEventLabel::Feed(9), "c.sh", ScriptStatus::Failed),
            (ScriptEventLabel::Scan, "d.sh", ScriptStatus::TimedOut),
            (ScriptEventLabel::Feed(9), "e.sh", ScriptStatus::Succeeded),
        ];
        for (event, script, status) in recorded {
            retain_output(
                db.clone(),
                None,
                ScriptResult {
                    status,
                    ..named(event, script)
                },
                script.as_bytes().to_vec(),
                limits.clone(),
            )
            .await
            .unwrap();
        }
        let ended = |status| ScriptRunFilter {
            status: Some(status),
            ..Default::default()
        };
        let scripts = |filter: ScriptRunFilter| {
            db.script_runs(filter, None, 10)
                .unwrap()
                .into_iter()
                .map(|run| run.result.script.to_string())
                .collect::<Vec<_>>()
        };
        let counts = |filter: &ScriptRunFilter| {
            let mut counts = db.script_run_status_counts(filter).unwrap();
            counts.sort_by_key(|(status, _)| status.as_str());
            counts
        };

        assert_eq!(scripts(ended(ScriptStatus::Failed)), ["c.sh", "b.sh"]);
        assert_eq!(scripts(ended(ScriptStatus::TimedOut)), ["d.sh"]);
        assert_eq!(scripts(ended(ScriptStatus::Warning)), [] as [&str; 0]);
        assert_eq!(
            db.script_run_count(&ended(ScriptStatus::Failed)).unwrap(),
            2
        );
        // A status narrows within the trigger, not instead of it.
        let scan_failures = ScriptRunFilter {
            kind: Some(ScriptKind::Scan),
            ..ended(ScriptStatus::Failed)
        };
        assert_eq!(scripts(scan_failures.clone()), ["b.sh"]);

        assert_eq!(
            counts(&Default::default()),
            [
                (ScriptStatus::Failed, 2),
                (ScriptStatus::Succeeded, 2),
                (ScriptStatus::TimedOut, 1),
            ]
        );
        // The counts answer for every status, whichever one is being shown.
        assert_eq!(
            counts(&scan_failures),
            [
                (ScriptStatus::Failed, 1),
                (ScriptStatus::Succeeded, 1),
                (ScriptStatus::TimedOut, 1),
            ]
        );
    }
}
