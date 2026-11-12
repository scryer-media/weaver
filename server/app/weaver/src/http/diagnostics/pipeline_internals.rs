// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// `pipeline-internals.json`: the read-only pipeline snapshot, which exists
// nowhere else.
//
// The pipeline builds the snapshot at the top of a turn, so a pipeline stuck
// in a long turn cannot answer, and that is exactly when the package is
// asked for. The request does not inherit the wedge: past a short wait the
// file is written from what can be read without the pipeline answering, the
// queue's shared rows, the published counters and the pipeline's own stamp
// of the turn it is in, and says `answered: false`. The manifest still names
// the full snapshot as missing, so a reader sees both.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde::Serialize;

use weaver_server_core::SchedulerHandle;
use weaver_server_core::operations::instrumentation::PipelineTurnReading;

use super::archive::Component;
use super::bounded::REASON_TIMED_OUT;

pub(super) const FILE: &str = "pipeline-internals.json";

// How long the package waits for the pipeline's own snapshot. A healthy
// pipeline answers within one turn, which is milliseconds; one that has not
// answered in this long is in a turn the fallback describes.
pub(super) const ANSWER_WAIT: Duration = Duration::from_secs(3);

// Why the full snapshot is missing.
const REASON_PIPELINE_STOPPED: &str = "pipeline_stopped";

// The file written in place of the snapshot when the pipeline does not
// answer.
#[derive(Debug, Serialize)]
struct UnansweredInternals {
    // Always false: this is not the pipeline's snapshot.
    answered: bool,
    reason: &'static str,
    timeout_ms: u64,
    captured_at_epoch_ms: f64,
    turn: TurnFacts,
    queue: QueueFacts,
    work: WorkFacts,
}

// The pipeline's stamp of the turn it is in.
#[derive(Debug, Serialize)]
struct TurnFacts {
    // What the turn is doing: `waiting_for_work` and `between_turns` mean no
    // work is in progress.
    phase: &'static str,
    in_progress: bool,
    // How long the pipeline has been busy without waiting, while it is.
    busy_ms: Option<u64>,
    turns_completed: u64,
    since_last_turn_ended_ms: Option<u64>,
    // Jobs installed in the pipeline when the last turn closed.
    installed_jobs: Option<u64>,
}

// The queue's shared rows.
#[derive(Debug, Serialize)]
struct QueueFacts {
    jobs: usize,
    jobs_by_status: BTreeMap<&'static str, u64>,
    max_concurrent_nzbs: usize,
    queue_window: usize,
    global_paused: bool,
    scheduled_pause: bool,
}

// The counters the pipeline and its workers publish as they go.
#[derive(Debug, Serialize)]
struct WorkFacts {
    active_downloads: usize,
    download_queue_depth: usize,
    recovery_queue_depth: usize,
    download_lanes_active: usize,
    download_jobs_eligible: usize,
    download_jobs_hot: usize,
    active_decodes: usize,
    decode_pending: usize,
    commit_pending: usize,
    jobs_verifying: usize,
    jobs_repairing: usize,
    jobs_extracting: usize,
    parked_infrastructure_work: usize,
    download_pressure_state: &'static str,
    download_pressure_reason: &'static str,
}

pub(super) async fn collect_pipeline_internals(handle: &SchedulerHandle) -> Vec<Component> {
    collect_pipeline_internals_within(handle, ANSWER_WAIT, Instant::now).await
}

// The snapshot, or past `wait` the fallback, with turn durations measured
// against `now`.
pub(super) async fn collect_pipeline_internals_within(
    handle: &SchedulerHandle,
    wait: Duration,
    now: impl FnOnce() -> Instant,
) -> Vec<Component> {
    let (reason, error) = match tokio::time::timeout(wait, handle.pipeline_diagnostics()).await {
        Ok(Ok(diagnostics)) => return vec![Component::json(FILE, &diagnostics)],
        Ok(Err(error)) => (REASON_PIPELINE_STOPPED, error.to_string()),
        Err(_) => (
            REASON_TIMED_OUT,
            format!(
                "the pipeline did not answer the diagnostics request within {}; {FILE} holds \
                 what could be read without it",
                wait_label(wait)
            ),
        ),
    };
    let fallback = unanswered(handle, reason, wait, now());
    vec![
        Component::failed_because(FILE, reason, error),
        Component::json(FILE, &fallback),
    ]
}

fn wait_label(wait: Duration) -> String {
    if wait.subsec_millis() == 0 {
        format!("{}s", wait.as_secs())
    } else {
        format!("{}ms", wait.as_millis())
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn unanswered(
    handle: &SchedulerHandle,
    reason: &'static str,
    wait: Duration,
    now: Instant,
) -> UnansweredInternals {
    let (queue, turn) = handle.pipeline_unanswered_state(now);
    let metrics = handle.get_live_metrics();
    let PipelineTurnReading {
        phase,
        busy_for,
        turns_completed,
        since_last_turn_ended,
        installed_jobs,
    } = turn;
    UnansweredInternals {
        answered: false,
        reason,
        timeout_ms: millis(wait),
        captured_at_epoch_ms: weaver_server_core::jobs::epoch_ms_now(),
        turn: TurnFacts {
            phase: phase.as_str(),
            in_progress: phase.is_busy(),
            busy_ms: busy_for.map(millis),
            turns_completed,
            since_last_turn_ended_ms: since_last_turn_ended.map(millis),
            installed_jobs,
        },
        queue: QueueFacts {
            jobs: queue.jobs,
            jobs_by_status: weaver_server_core::JOB_STATUS_KEYS
                .iter()
                .map(|key| (*key, queue.jobs_by_status.count(key) as u64))
                .collect(),
            max_concurrent_nzbs: queue.max_concurrent_nzbs,
            queue_window: queue.queue_window,
            global_paused: queue.global_paused,
            scheduled_pause: queue.scheduled_pause,
        },
        work: WorkFacts {
            active_downloads: metrics.active_downloads,
            download_queue_depth: metrics.download_queue_depth,
            recovery_queue_depth: metrics.recovery_queue_depth,
            download_lanes_active: metrics.download_lanes_active,
            download_jobs_eligible: metrics.download_jobs_eligible,
            download_jobs_hot: metrics.download_jobs_hot,
            active_decodes: metrics.active_decodes,
            decode_pending: metrics.decode_pending,
            commit_pending: metrics.commit_pending,
            jobs_verifying: metrics.verify_active,
            jobs_repairing: metrics.repair_active,
            jobs_extracting: metrics.extract_active,
            parked_infrastructure_work: metrics.parked_infrastructure_work,
            download_pressure_state: metrics.download_pressure_state.as_str(),
            download_pressure_reason: metrics.download_pressure_reason.as_str(),
        },
    }
}
