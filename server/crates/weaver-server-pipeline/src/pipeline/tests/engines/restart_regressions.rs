// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::harness::{JOB, Scenario};
use super::restart_fixtures;
use crate::jobs::graph_checkpoint::{GraphCheckpoint, JobsGraphCheckpointInternalQueries};
use crate::jobs::model::SchedulingMemoryEstimate;
use crate::jobs::queries::JobsQueries;
use crate::pipeline::graph::engine::Heard;
use crate::{DownloadWork, JobId, JobSpec, JobState, Pipeline, SchedulerError};

pub fn installed_job(pipeline: &Pipeline, id: JobId) -> &JobState {
    &pipeline.jobs[&id]
}

pub fn has_graph(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.graph_jobs.contains_key(&id)
}

pub fn awaits_graph_mail(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline
        .graph_jobs
        .get(&id)
        .is_some_and(|job| job.awaits_mail())
}

pub fn graph_awaits_mail(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.graph_jobs[&id].awaits_mail()
}

pub fn graph_retiring(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.retiring_graph_jobs.contains_key(&id)
}

pub async fn hear_run(pipeline: &mut Pipeline) {
    let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
    pipeline.graph_run_said(said);
}

pub fn hear_pending_runs(pipeline: &mut Pipeline) {
    pipeline.hear_graph_runs();
}

pub enum RunOutput {
    Emit,
    End,
    Other,
}

pub struct RunReply(crate::pipeline::graph::engine::Said);

impl RunReply {
    pub async fn receive(pipeline: &mut Pipeline) -> Self {
        Self(pipeline.graph_runs.recv().await.expect("a live run speaks"))
    }

    pub fn output(&self, pipeline: &Pipeline, id: JobId) -> RunOutput {
        let said = &self.0;
        if !pipeline.graph_jobs[&id]
            .runs()
            .current(said.node, said.generation)
        {
            return RunOutput::Other;
        }
        match said.heard {
            Heard::Emit(_) => RunOutput::Emit,
            Heard::Ended => RunOutput::End,
            _ => RunOutput::Other,
        }
    }

    pub fn deliver(self, pipeline: &mut Pipeline) {
        pipeline.graph_run_said(self.0);
    }
}

pub async fn restart(
    temp_dir: &tempfile::TempDir,
    pipeline: Pipeline,
    scenario: &Scenario,
    in_memory: &mut BTreeSet<(u32, u32)>,
) -> Pipeline {
    restart_fixtures::restart(temp_dir, pipeline, scenario, in_memory, &mut |_| {}).await
}

pub async fn restart_as_it_stands(
    temp_dir: &tempfile::TempDir,
    pipeline: Pipeline,
    scenario: &Scenario,
) -> (
    Pipeline,
    tokio::sync::broadcast::Receiver<crate::events::model::PipelineEvent>,
) {
    restart_fixtures::restart_as_it_stands(temp_dir, pipeline, scenario, &mut |_| {}).await
}

pub fn effective_downloaded(state: &JobState) -> u64 {
    Pipeline::effective_downloaded_bytes(state)
}

pub fn durable_floor(pipeline: &Pipeline, id: JobId) -> u64 {
    pipeline.durable_download_floor_bytes_for_job(id)
}

pub fn advance_floor(pipeline: &mut Pipeline, id: JobId) {
    pipeline.advance_durable_download_floor(id);
}

pub fn undurable_bytes(pipeline: &Pipeline, id: JobId) -> u64 {
    pipeline.estimated_undurable_download_bytes_for_job(id)
}

pub fn admission_decision(
    pipeline: &Pipeline,
    id: JobId,
    work: &DownloadWork,
    open: &[DownloadWork],
) -> &'static str {
    pipeline
        .checkpoint_admission(id)
        .decision(&work.view(), open)
        .as_str()
}

pub fn holds_every_payload(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline
        .checkpoint_admission(id)
        .holds_every_payload_article()
}

pub fn failed_bytes(pipeline: &Pipeline, id: JobId) -> u64 {
    pipeline.derived_failed_bytes(id)
}

#[derive(Debug, PartialEq, Eq)]
pub struct StoredCheckpoint(GraphCheckpoint);

impl StoredCheckpoint {
    pub fn read(pipeline: &Pipeline, id: JobId) -> Option<Self> {
        pipeline.db.load_graph_checkpoint(id).unwrap().map(Self)
    }

    pub fn covered(&self) -> BTreeMap<i64, i64> {
        let mut out = BTreeMap::new();
        for row in self.0.coverage.iter().filter(|row| row.node == 0) {
            *out.entry(row.member).or_default() += row.end - row.start;
        }
        out
    }

    pub fn root_version(&self) -> i64 {
        self.0
            .nodes
            .iter()
            .find(|row| row.node == 0)
            .unwrap()
            .facts_version
    }

    pub fn held_parts_count(&self) -> usize {
        self.0.held_parts.len()
    }

    pub fn first_held_part(&self) -> (i64, i64, &Path) {
        let part = &self.0.held_parts[0];
        (part.file, part.ordinal, Path::new(&part.spool_path))
    }
}

pub const ROOT_FACTS_VERSION: i64 = crate::pipeline::graph::root::facts::RootFacts::VERSION as i64;

pub fn restores() -> usize {
    crate::pipeline::graph::restore::RESTORES.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn memory_cost(spec: &JobSpec) -> u64 {
    spec.scheduling_memory_estimate()
}

pub fn retained_reservation_refused(pipeline: &Pipeline) -> bool {
    pipeline
        .process_memory_budget
        .try_reserve_retained(1)
        .is_err()
}

pub fn has_article_memory(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.job_article_memory.contains_key(&id)
}

pub fn resume_runtime(pipeline: &mut Pipeline, id: JobId) -> Result<(), SchedulerError> {
    pipeline.resume_job_runtime(id)
}

pub async fn spill_parts(pipeline: &mut Pipeline) {
    pipeline.spill_parked_parts().await;
}

pub fn parking(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.parking_jobs.contains(&id)
}

pub fn spilling(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.spilling_jobs.contains(&id)
}

pub fn in_flight(pipeline: &Pipeline) -> usize {
    pipeline.articles_in_flight
}

pub fn supplied_candidates_empty(pipeline: &Pipeline) -> bool {
    let (_, supplied) = pipeline
        .db
        .load_active_job_password_sources(JOB)
        .unwrap()
        .expect("the job has its row");
    supplied.is_empty()
}

pub async fn change_checkpoint(pipeline: &Pipeline, sql: &'static str) {
    let datastore = pipeline.db.datastore();
    crate::persistence::sql_runtime::SqlRuntime::run_in_transaction(
        &datastore,
        "test_change_checkpoint",
        |tx| {
            Box::pin(async move {
                let args: Vec<_> = sql
                    .matches("{}")
                    .map(|_| crate::persistence::sql_runtime::SqlArg::I64(JOB.0 as i64))
                    .collect();
                // SQLite checks a RENAME COLUMN against the schema this
                // connection last read, and another connection of the pool
                // may have renamed it since. Reading the schema first brings
                // the connection up to date.
                tx.execute("SELECT count(*) FROM sqlite_schema", &[])
                    .await?;
                tx.execute(sql, &args).await?;
                Ok(())
            })
        },
    )
    .await
    .unwrap();
}

pub async fn store_candidates(pipeline: &Pipeline, plain: &str) {
    use crate::persistence::sql_runtime::SqlArg;
    let key = pipeline.db.encryption_key().unwrap();
    let sealed = crate::persistence::encryption::encrypt_value(key, plain).unwrap();
    let datastore = pipeline.db.datastore();
    crate::persistence::sql_runtime::SqlRuntime::run_in_transaction(
        &datastore,
        "test_store_candidates",
        |tx| {
            let sealed = sealed.clone();
            Box::pin(async move {
                tx.execute(
                    "UPDATE active_jobs SET password_candidates = {} WHERE job_id = {}",
                    &[SqlArg::Text(sealed), SqlArg::I64(JOB.0 as i64)],
                )
                .await?;
                Ok(())
            })
        },
    )
    .await
    .unwrap();
}

pub async fn remove_checkpoint(pipeline: &Pipeline) {
    let datastore = pipeline.db.datastore();
    crate::persistence::sql_runtime::SqlRuntime::run_in_transaction(
        &datastore,
        "test_drop_checkpoint",
        |tx| {
            Box::pin(async move {
                for table in crate::jobs::graph_checkpoint::TABLES {
                    tx.execute(
                        &format!("DELETE FROM {table} WHERE job_id = {{}}"),
                        &[crate::persistence::sql_runtime::SqlArg::I64(JOB.0 as i64)],
                    )
                    .await?;
                }
                Ok(())
            })
        },
    )
    .await
    .unwrap();
}
