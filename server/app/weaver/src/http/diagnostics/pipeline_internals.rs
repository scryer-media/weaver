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
// answer. Every field is a number, a boolean or a fixed code, never text read
// from a job or an error: it ships without the stand-in table, so keep it
// that way.
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
    articles_in_flight: usize,
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
    // The fallback holds only numbers, booleans and fixed codes, so it ships
    // even when the stand-in table cannot be built; the pipeline's error may
    // carry a name, so its entry then says only what the code says. The
    // answered snapshot names jobs and always waits for the table.
    vec![
        Component::failed_because(FILE, reason, error).names_nothing_as(Err(format!(
            "the pipeline did not answer the diagnostics request; {FILE} holds what \
                 could be read without it"
        ))),
        Component::json(FILE, &fallback).names_nothing(),
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
            articles_in_flight: metrics.articles_in_flight,
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

// `pipeline-turn-phases.json`: what the pipeline's turns spent their time on
// while the package was collected, sampled from its turn stamp. The stamp is
// a few atomics the pipeline writes anyway, so the pipeline does no work for
// this: the sampler reads the current phase every `sample_every_ms` and counts
// it. A phase's share of `samples` is its share of the window, to within one
// sample interval per phase change. Numbers and fixed codes only.
pub(super) const PHASES_FILE: &str = "pipeline-turn-phases.json";

// How often the sampler reads the stamp. The timer may fire less often than
// asked on some systems; `samples` says how many readings were taken.
pub(super) const PHASE_SAMPLE_EVERY: Duration = Duration::from_millis(1);

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct TurnPhaseSamples {
    sample_every_ms: u64,
    samples: u64,
    window_ms: u64,
    turns_completed_in_window: u64,
    // Readings per phase, in the phase codes `pipeline-internals.json` uses.
    samples_by_phase: BTreeMap<&'static str, u64>,
}

impl TurnPhaseSamples {
    pub(super) fn component(&self) -> Component {
        Component::json(PHASES_FILE, self).names_nothing()
    }
}

// Runs `work` to completion while sampling the turn stamp through `read`
// every `every`, and returns both. The first reading is taken at once.
pub(super) async fn sample_turn_phases_while<T>(
    mut read: impl FnMut(Instant) -> PipelineTurnReading,
    every: Duration,
    work: impl std::future::Future<Output = T>,
) -> (T, TurnPhaseSamples) {
    let started = tokio::time::Instant::now();
    let first = read(Instant::now());
    let mut last_turns = first.turns_completed;
    let mut samples = TurnPhaseSamples {
        sample_every_ms: millis(every),
        samples: 1,
        window_ms: 0,
        turns_completed_in_window: 0,
        samples_by_phase: BTreeMap::from([(first.phase.as_str(), 1)]),
    };
    let mut ticks = tokio::time::interval_at(started + every, every);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(work);
    let output = loop {
        tokio::select! {
            biased;
            output = &mut work => break output,
            _ = ticks.tick() => {
                let reading = read(Instant::now());
                samples.samples += 1;
                *samples.samples_by_phase.entry(reading.phase.as_str()).or_default() += 1;
                last_turns = reading.turns_completed;
            }
        }
    };
    samples.window_ms = millis(started.elapsed());
    samples.turns_completed_in_window = last_turns.saturating_sub(first.turns_completed);
    (output, samples)
}

#[cfg(test)]
mod phase_sampler_tests {
    use super::*;
    use weaver_server_core::operations::instrumentation::PipelinePhase;

    fn reading(phase: PipelinePhase, turns_completed: u64) -> PipelineTurnReading {
        PipelineTurnReading {
            phase,
            busy_for: None,
            turns_completed,
            since_last_turn_ended: None,
            installed_jobs: None,
        }
    }

    // Every reading is counted under its phase, and the work's end, not a
    // clock, ends the sampling: the script finishes the work on its last
    // reading.
    #[tokio::test(start_paused = true)]
    async fn each_reading_counts_under_its_phase_until_the_work_ends() {
        let script = [
            reading(PipelinePhase::WaitingForWork, 10),
            reading(PipelinePhase::Dispatch, 10),
            reading(PipelinePhase::Dispatch, 11),
            reading(PipelinePhase::DbReply, 12),
            reading(PipelinePhase::WaitingForWork, 14),
        ];
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<&'static str>();
        let mut done_tx = Some(done_tx);
        let mut next = 0;
        let read = |_now: Instant| {
            let current = script[next];
            next += 1;
            if next == script.len() {
                done_tx
                    .take()
                    .expect("one finish")
                    .send("collected")
                    .unwrap();
            }
            current
        };

        let (output, samples) =
            sample_turn_phases_while(
                read,
                PHASE_SAMPLE_EVERY,
                async move { done_rx.await.unwrap() },
            )
            .await;

        assert_eq!(output, "collected");
        assert_eq!(samples.samples, 5);
        assert_eq!(samples.turns_completed_in_window, 4);
        assert_eq!(
            samples.samples_by_phase,
            BTreeMap::from([("db_reply", 1), ("dispatch", 2), ("waiting_for_work", 2)])
        );
        assert_eq!(samples.sample_every_ms, 1);
    }
}
