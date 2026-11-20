// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::Path;

use super::{TerminalPostProcessingEvent, settle_script_admissions};
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::post_processing::events::{
    EventContext, PostProcessingEventsInvalidateQueueScriptAdmissionInternalQueries,
};
use crate::post_processing::instances::{
    InstanceTrigger, PostProcessingInstancesQueries, ScriptInstanceDraft,
};
use crate::post_processing::model::{PostProcessingSettings, QueueEvent, ScriptName};
use crate::post_processing::settings::PostProcessingSettingsQueries;
use crate::{Database, JobId, Pipeline, StateError};

pub fn queue_instance(event: QueueEvent) -> ScriptInstanceDraft {
    ScriptInstanceDraft::new(
        ScriptName::new("queue.sh").unwrap(),
        InstanceTrigger::Queue(event),
    )
}

pub fn enable_queue_script(db: &Database, root: &Path) {
    let scripts = db
        .initialize_post_processing_script_directory(root, None)
        .unwrap();
    std::fs::write(
        scripts.join("queue.sh"),
        "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\nexit 0\n",
    )
    .unwrap();
    db.save_post_processing_settings(&PostProcessingSettings {
        execution_enabled: true,
        ..Default::default()
    })
    .unwrap();
    // One instance for each thing that can happen to a download.
    for event in QueueEvent::ALL {
        db.create_script_instance(queue_instance(event)).unwrap();
    }
}

pub async fn receive_queue_done(
    pipeline: &mut Pipeline,
) -> Option<(JobId, Result<(), StateError>)> {
    match pipeline
        .terminal_post_processing_done_rx
        .recv()
        .await
        .unwrap()
    {
        TerminalPostProcessingEvent::QueueDone(id, result) => Some((id, result)),
        _ => None,
    }
}

pub async fn receive_arrival_done(
    pipeline: &mut Pipeline,
) -> Option<(JobId, Result<(), StateError>)> {
    match pipeline
        .terminal_post_processing_done_rx
        .recv()
        .await
        .unwrap()
    {
        TerminalPostProcessingEvent::AddedScriptsDone(id, result) => Some((id, result)),
        _ => None,
    }
}

pub fn raise_queue_event(pipeline: &Pipeline, id: JobId, event: QueueEvent, reason: Option<&str>) {
    pipeline.raise_queue_script_event(id, event, reason);
}

pub fn queue_context(pipeline: &Pipeline, id: JobId, event: QueueEvent) -> Option<EventContext> {
    pipeline.queue_script_context(id, event)
}

pub fn claim_started_run(db: &Database, run_id: &str, tag: &'static str) {
    let datastore = db.datastore();
    let claimed_run = run_id.to_owned();
    db.run_sql_blocking(async move {
        SqlRuntime::run_in_transaction(&datastore, tag, |tx| {
            let claimed_run = claimed_run.clone();
            Box::pin(async move {
                tx.execute(
                    "UPDATE script_event_queue SET state = 'started' WHERE run_id = {}",
                    &[SqlArg::Text(claimed_run)],
                )
                .await?;
                Ok(())
            })
        })
        .await
    })
    .unwrap();
}

pub async fn cleanup_cancelled_dirs(
    db: &Database,
    id: JobId,
    dir: &Path,
    staging: Option<&Path>,
) -> Result<bool, StateError> {
    Pipeline::cleanup_cancelled_job_directories(db, id, dir, staging).await
}

pub fn finish_event(db: &Database, run_id: &str) {
    db.finish_script_event_for_test(run_id).unwrap();
}

pub fn queue_script_waiting(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.queue_script_waiters.contains(&id)
}

pub fn queue_script_completed(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.queue_scripts_completed.contains(&id)
}

pub async fn queue_scripts_done(
    pipeline: &mut Pipeline,
    id: JobId,
    result: Result<(), StateError>,
) {
    pipeline.handle_queue_scripts_done(id, result).await;
}

pub async fn settle_terminal_scripts(pipeline: &mut Pipeline) {
    settle_script_admissions(pipeline).await;
}

pub fn mark_queue_script_waiting(pipeline: &mut Pipeline, id: JobId) {
    pipeline.queue_script_waiters.insert(id);
}

pub fn queue_scripts_admitted(pipeline: &mut Pipeline, id: JobId) {
    pipeline.handle_queue_scripts_admitted(id);
}

pub fn raise_arrival(pipeline: &mut Pipeline, id: JobId) {
    pipeline.raise_added_script_event(id);
}

pub fn arrival_done(pipeline: &mut Pipeline, id: JobId, result: Result<(), StateError>) {
    pipeline.handle_added_scripts_done(id, result);
}

pub fn mark_arrival_held(pipeline: &mut Pipeline, id: JobId) {
    pipeline.added_script_holds.insert(id);
}

pub async fn admit_queue_event(
    db: &Database,
    event: EventContext,
    barrier: bool,
) -> Option<String> {
    db.admit_queue_script_event(event, barrier)
        .await
        .unwrap()
        .unwrap()
}

pub async fn wait_for_download_scripts(
    db: &Database,
    id: JobId,
    downloaded_run: Option<&str>,
) -> Result<(), StateError> {
    Pipeline::wait_for_download_queue_scripts(db, id, downloaded_run).await
}

pub fn completion_gate(pipeline: &mut Pipeline, id: JobId, files_complete: bool) -> bool {
    pipeline.queue_script_completion_gate(id, files_complete)
}

pub async fn admitted_so_far(db: &Database) {
    db.admitted_so_far().await;
}

pub fn queued_script_events(db: &Database, event: QueueEvent) -> Vec<EventContext> {
    db.queued_script_events(event)
}

// The NZBNA_DELETESTATUS of every NZB_DELETED event queued for the job, in
// the order queued. Admissions are taken in the order raised, so once one
// raised after them has been admitted, every earlier one has been.
pub async fn deletions_queued(db: &Database, job_id: JobId, after: EventContext) -> Vec<String> {
    let _ = db.admit_queue_script_event(after, false).await;
    let datastore = db.datastore();
    let payloads = db.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT payload FROM script_event_queue WHERE job_id = {} AND event = 'NZB_DELETED' ORDER BY seq",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?
            .iter()
            .map(|row| row.text("payload"))
            .collect::<Result<Vec<_>, _>>()
        })
        .unwrap();
    payloads
        .iter()
        .map(|payload| {
            let context: EventContext = serde_json::from_str(payload).unwrap();
            context.env["NZBNA_DELETESTATUS"].clone()
        })
        .collect()
}

// The events queued for `job_id`, by name, in the order queued.
pub fn job_events(db: &Database, job_id: JobId) -> Vec<String> {
    let datastore = db.datastore();
    db.run_sql_blocking_read(async move {
        SqlRuntime::fetch_all(
            datastore.read_exec(),
            "SELECT event FROM script_event_queue WHERE job_id = {} ORDER BY seq",
            &[SqlArg::I64(job_id.0 as i64)],
        )
        .await?
        .iter()
        .map(|row| row.text("event"))
        .collect::<Result<Vec<_>, _>>()
    })
    .unwrap()
}
