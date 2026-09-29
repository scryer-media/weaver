use std::collections::BTreeSet;
use std::io::Read;

use super::model::{EventScriptSettings, ScriptEventLabel, ScriptResult};
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx};
use crate::persistence::{Database, StateError};

const MAX_DECODE_BYTES: usize = 8 * 1024 * 1024;
const EXCERPT_BYTES: usize = 4096;

fn error(error: impl std::fmt::Display) -> StateError {
    StateError::Database(error.to_string())
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
                    if used + stored_bytes > limits.script_output_ring_bytes as i64 {
                        for row in tx.fetch_all("SELECT id, stored_bytes FROM script_outputs WHERE event <> 'post_processing' AND stored_bytes > 0 ORDER BY seq LIMIT 128", &[]).await? {
                            tx.execute("UPDATE script_outputs SET output = {}, stored_bytes = 0 WHERE id = {}", &[SqlArg::Bytes(Vec::new()), SqlArg::Text(row.text("id")?)]).await?;
                            used -= row.i64("stored_bytes")?;
                            if used + stored_bytes <= limits.script_output_ring_bytes as i64 { break; }
                        }
                    }
                    // Terminal results are protected by the per-job ring. When those
                    // alone fill the budget, new event runs retain only their excerpt.
                    let retain = result.event == ScriptEventLabel::PostProcessing || used + stored_bytes <= limits.script_output_ring_bytes as i64;
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

    pub fn event_script_results(&self, job_id: u64) -> Result<Vec<ScriptResult>, StateError> {
        let datastore = self.datastore();
        let job_id = i64::try_from(job_id).map_err(error)?;
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(datastore.read_exec(), "SELECT result_json FROM script_outputs WHERE job_id = {} AND event <> 'post_processing' ORDER BY seq DESC LIMIT 128", &[SqlArg::I64(job_id)]).await?.into_iter().map(|row| serde_json::from_str(&row.text("result_json")?).map_err(error)).collect()
        })
    }
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
            event,
            output_id: None,
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
}
