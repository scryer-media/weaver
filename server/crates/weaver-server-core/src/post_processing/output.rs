use std::collections::BTreeSet;
use std::io::Read;

use super::model::{EventScriptSettings, ScriptEventLabel, ScriptKind, ScriptResult};
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx};
use crate::persistence::{Database, StateError};

const MAX_DECODE_BYTES: usize = 8 * 1024 * 1024;
const EXCERPT_BYTES: usize = 4096;

fn error(error: impl std::fmt::Display) -> StateError {
    StateError::Database(error.to_string())
}

/// Which recorded runs to list. An unset field matches every run.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct ScriptRunFilter {
    pub job_id: Option<u64>,
    pub script: Option<String>,
    pub kind: Option<ScriptKind>,
}

/// One recorded run of a script, whatever started it.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScriptRun {
    /// Names the run's kept output, when `output_retained`.
    pub id: String,
    /// Position in the order runs were recorded; later runs are higher.
    pub seq: i64,
    pub job_id: Option<u64>,
    /// Known once the job has reached history.
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
    if bytes.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        zstd::stream::read::Decoder::new(bytes)
            .map_err(error)?
            .take(ceiling as u64 + 1)
            .read_to_end(&mut decoded)
            .map_err(error)?;
    } else {
        decoded.extend_from_slice(&bytes[..bytes.len().min(ceiling + 1)]);
    }
    if decoded.len() > ceiling {
        return Err(error("script output exceeds its decode ceiling"));
    }
    Ok(String::from_utf8_lossy(&decoded).into_owned())
}

pub async fn retain_output(
    db: Database,
    job_id: Option<u64>,
    mut result: ScriptResult,
    output: Vec<u8>,
    limits: EventScriptSettings,
) -> Result<ScriptResult, StateError> {
    result.output_tail = excerpt(&output);
    tokio::task::spawn_blocking(move || {
        let mut compressed = zstd::stream::encode_all(output.as_slice(), 3).map_err(error)?;
        if compressed.len() as u64 > limits.script_output_run_cap_bytes {
            compressed.clear();
        }
        db.insert_script_output(job_id, result, output.len(), compressed, limits)
    })
    .await
    .map_err(error)?
}

/// Record a result that has no output to keep, such as a script that could
/// not be started.
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
        raw_bytes: usize,
        output: Vec<u8>,
        limits: EventScriptSettings,
    ) -> Result<ScriptResult, StateError> {
        let datastore = self.datastore();
        let job_id = job_id.map(i64::try_from).transpose().map_err(error)?;
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "retain_script_output", |tx| {
                let mut result = result.clone();
                let mut output = output.clone();
                let limits = limits.clone();
                Box::pin(async move {
                    // This row serializes insertion/eviction on both SQL backends.
                    let state = tx.fetch_optional("UPDATE script_output_state SET next_seq = next_seq + 1 WHERE singleton = 1 RETURNING next_seq, used_bytes", &[]).await?.ok_or_else(|| error("script output state is missing"))?;
                    let seq = state.i64("next_seq")?;
                    if let Some(job_id) = job_id
                        && tx.fetch_optional("SELECT job_id FROM active_jobs WHERE job_id = {} UNION ALL SELECT job_id FROM job_history WHERE job_id = {} LIMIT 1", &[SqlArg::I64(job_id), SqlArg::I64(job_id)]).await?.is_none() {
                        return Ok(result);
                    }
                    let mut used = state.i64("used_bytes")?;
                    let removed = tx.fetch_all("DELETE FROM script_outputs WHERE id IN (SELECT id FROM script_outputs WHERE job_id IS NOT DISTINCT FROM {} AND (job_id IS NOT NULL OR event = {}) ORDER BY seq DESC LIMIT 128 OFFSET {}) RETURNING stored_bytes", &[SqlArg::OptI64(job_id), SqlArg::Text(result.event.to_string()), SqlArg::I64(i64::from(limits.script_output_runs_per_job.saturating_sub(1)))]).await?;
                    for row in removed { used -= row.i64("stored_bytes")?; }
                    let stored_bytes = output.len() as i64;
                    let ring_bytes = i64::try_from(limits.script_output_ring_bytes).unwrap_or(i64::MAX);
                    // Every kept payload counts against the one budget. Event runs
                    // give way first; post-processing output is evicted, oldest first,
                    // only to make room for newer post-processing output. An evicted
                    // run keeps its result and excerpt.
                    let evictable = if result.event == ScriptEventLabel::PostProcessing {
                        "stored_bytes > 0 ORDER BY CASE WHEN event = 'post_processing' THEN 1 ELSE 0 END, seq"
                    } else {
                        "event <> 'post_processing' AND stored_bytes > 0 ORDER BY seq"
                    };
                    let evict = format!("SELECT id, stored_bytes FROM script_outputs WHERE {evictable} LIMIT 128");
                    'evict: while stored_bytes <= ring_bytes && used + stored_bytes > ring_bytes {
                        let rows = tx.fetch_all(&evict, &[]).await?;
                        if rows.is_empty() { break; }
                        for row in rows {
                            tx.execute("UPDATE script_outputs SET output = {}, stored_bytes = 0 WHERE id = {}", &[SqlArg::Bytes(Vec::new()), SqlArg::Text(row.text("id")?)]).await?;
                            used -= row.i64("stored_bytes")?;
                            if used + stored_bytes <= ring_bytes { break 'evict; }
                        }
                    }
                    // When what may not be evicted fills the budget, the run keeps
                    // only its excerpt.
                    let retain = used + stored_bytes <= ring_bytes;
                    if !retain { output.clear(); }
                    let stored_bytes = output.len() as i64;
                    let mut entropy = [0_u8; 16];
                    getrandom::fill(&mut entropy).map_err(error)?;
                    let id = format!("script-output-{}", hex::encode(entropy));
                    result.output_id = (stored_bytes > 0).then(|| id.clone());
                        tx.execute("INSERT INTO script_outputs (id, job_id, event, script, seq, raw_bytes, truncated, output, stored_bytes, result_json, created_at) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})", &[
                            SqlArg::Text(id), SqlArg::OptI64(job_id), SqlArg::Text(result.event.to_string()), SqlArg::Text(result.script.to_string()), SqlArg::I64(seq), SqlArg::I64(raw_bytes as i64), SqlArg::Bool(result.output_truncated), SqlArg::Bytes(output), SqlArg::I64(stored_bytes), SqlArg::Text(serde_json::to_string(&result).map_err(error)?), SqlArg::I64(result.finished_at_epoch_ms),
                        ]).await?;
                        used += stored_bytes;
                    tx.execute("UPDATE script_output_state SET used_bytes = {} WHERE singleton = 1", &[SqlArg::I64(used.max(0))]).await?;
                    Ok(result)
                })
            }).await
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

    /// The job's runs that are not part of a post-processing pass: its event
    /// scripts, and the post-processing scripts nothing waited for.
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

    /// Recorded runs, latest first. `before` continues a listing below the
    /// `seq` of the last run already returned.
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

    /// How many recorded runs the filter matches, across every page.
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
}

/// The SQL conditions, over `script_outputs o`, that select the runs a filter matches.
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
    Ok((conditions, args))
}

/// Archive moves active rows to history, so deletion is explicit rather than an FK cascade.
pub(crate) async fn delete_script_state_tx(
    tx: &mut SqlTx<'_>,
    job_id: i64,
) -> Result<(), StateError> {
    tx.execute(
        "UPDATE script_output_state SET used_bytes = used_bytes WHERE singleton = 1",
        &[],
    )
    .await?;
    let rows = tx
        .fetch_all(
            "DELETE FROM script_outputs WHERE job_id = {} RETURNING stored_bytes",
            &[SqlArg::I64(job_id)],
        )
        .await?;
    let bytes = rows
        .into_iter()
        .map(|row| row.i64("stored_bytes"))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .sum::<i64>();
    tx.execute(
        "UPDATE script_output_state SET used_bytes = used_bytes - {} WHERE singleton = 1",
        &[SqlArg::I64(bytes)],
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
    use super::super::model::{ScriptAdapter, ScriptName, ScriptStatus};
    use super::*;

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
    fn decode_is_bounded_and_accepts_legacy_plain_text() {
        assert_eq!(decode_output(b"legacy", 6).unwrap(), "legacy");
        let bytes = zstd::stream::encode_all(&b"1234567"[..], 3).unwrap();
        assert!(decode_output(&bytes, 6).is_err());
        assert_eq!(decode_output(&bytes, 7).unwrap(), "1234567");
    }

    #[tokio::test]
    async fn jobless_output_run_cap_is_scoped_to_event_label() {
        let db = Database::open_in_memory().unwrap();
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

    #[tokio::test]
    async fn global_eviction_preserves_terminal_output_and_event_excerpts() {
        let db = Database::open_in_memory().unwrap();
        for id in 1..=3 {
            active(&db, id);
        }
        let body: Vec<u8> = (0..=255).collect();
        let compressed = zstd::stream::encode_all(body.as_slice(), 3).unwrap().len() as u64;
        let limits = EventScriptSettings {
            script_output_ring_bytes: compressed + 16,
            ..Default::default()
        };
        let terminal = retain_output(
            db.clone(),
            Some(1),
            result(ScriptEventLabel::PostProcessing),
            b"PP".to_vec(),
            limits.clone(),
        )
        .await
        .unwrap();
        let event = ScriptEventLabel::Queue(super::super::model::QueueEvent::NzbAdded);
        let old = retain_output(
            db.clone(),
            Some(2),
            result(event.clone()),
            body.clone(),
            limits.clone(),
        )
        .await
        .unwrap();
        let new = retain_output(db.clone(), Some(3), result(event), body, limits)
            .await
            .unwrap();
        assert!(
            db.script_output_retained(terminal.output_id.as_deref().unwrap())
                .unwrap()
        );
        assert!(
            !db.script_output_retained(old.output_id.as_deref().unwrap())
                .unwrap()
        );
        assert!(
            db.script_output_retained(new.output_id.as_deref().unwrap())
                .unwrap()
        );
        assert_eq!(
            db.event_script_results(2).unwrap()[0].output_tail,
            old.output_tail
        );
        db.delete_active_job(crate::JobId(3)).unwrap();
        assert!(
            !db.script_output_retained(new.output_id.as_deref().unwrap())
                .unwrap()
        );
        assert!(db.event_script_results(3).unwrap().is_empty());
    }

    #[tokio::test]
    async fn global_budget_bounds_post_processing_output() {
        let db = Database::open_in_memory().unwrap();
        for id in 1..=4 {
            active(&db, id);
        }
        let body: Vec<u8> = (0..=255).collect();
        let compressed = zstd::stream::encode_all(body.as_slice(), 3).unwrap().len() as u64;
        let limits = EventScriptSettings {
            script_output_ring_bytes: compressed + compressed / 2,
            ..Default::default()
        };
        let pass = |job_id: u64| {
            retain_output(
                db.clone(),
                Some(job_id),
                result(ScriptEventLabel::PostProcessing),
                body.clone(),
                limits.clone(),
            )
        };
        let first = pass(1).await.unwrap();
        let event = ScriptEventLabel::Queue(super::super::model::QueueEvent::NzbAdded);
        // An event run does not displace post-processing output; it keeps only
        // its excerpt.
        let queued = retain_output(
            db.clone(),
            Some(2),
            result(event),
            body.clone(),
            limits.clone(),
        )
        .await
        .unwrap();
        assert!(queued.output_id.is_none());
        assert!(
            db.script_output_retained(first.output_id.as_deref().unwrap())
                .unwrap()
        );

        // A newer pass evicts the oldest pass's output, not its result.
        let second = pass(3).await.unwrap();
        assert!(
            !db.script_output_retained(first.output_id.as_deref().unwrap())
                .unwrap()
        );
        assert!(
            db.script_output_retained(second.output_id.as_deref().unwrap())
                .unwrap()
        );
        let runs = db
            .script_runs(
                ScriptRunFilter {
                    job_id: Some(1),
                    ..Default::default()
                },
                None,
                10,
            )
            .unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].result.output_tail, first.output_tail);
        assert!(!runs[0].output_retained);

        // Output larger than the whole budget is not kept, and evicts nothing.
        let tight = EventScriptSettings {
            script_output_ring_bytes: compressed - 1,
            ..limits
        };
        let oversized = retain_output(
            db.clone(),
            Some(4),
            result(ScriptEventLabel::PostProcessing),
            body,
            tight,
        )
        .await
        .unwrap();
        assert!(oversized.output_id.is_none());
        assert!(!oversized.output_tail.is_empty());
        assert!(
            db.script_output_retained(second.output_id.as_deref().unwrap())
                .unwrap()
        );
    }

    #[tokio::test]
    async fn cap_keeps_result_and_deleted_jobs_cannot_recreate_output() {
        let db = Database::open_in_memory().unwrap();
        active(&db, 1);
        let limits = EventScriptSettings {
            script_output_run_cap_bytes: 1,
            ..Default::default()
        };
        let event = ScriptEventLabel::Queue(super::super::model::QueueEvent::NzbAdded);
        let stored = retain_output(
            db.clone(),
            Some(1),
            result(event.clone()),
            b"excerpt".to_vec(),
            limits,
        )
        .await
        .unwrap();
        assert!(stored.output_id.is_none());
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
}
