// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// A name no other test records under, so the shared registry cannot
// leak another test's runs into these counts.
fn result(name: &str, status: &str, exit: i32) -> ScriptResult {
    let mut result: ScriptResult = serde_json::from_str(&format!(
        r#"{{"script":"notify.sh","adapter":"nzbget","status":"{status}","exitCode":{exit},"durationMs":1500,"finishedAtEpochMs":1700000000000}}"#
    ))
    .unwrap();
    result.instance_name = Some(name.into());
    result
}

fn script(name: &str) -> ScriptMetrics {
    counters_snapshot()
        .scripts
        .into_iter()
        .find(|s| &*s.script == name)
        .expect("the script was recorded")
}

#[test]
fn a_finished_run_fills_every_status_and_the_last_run_gauges() {
    let name = "run-metrics-finished";
    record_finished(&result(name, "failed", 3), true);
    let metrics = script(name);

    let statuses: Vec<_> = metrics.runs.iter().map(|r| (r.status, r.runs)).collect();
    assert_eq!(statuses.len(), RunStatus::ALL.len());
    for (status, runs) in statuses {
        assert_eq!(runs, u64::from(status == RunStatus::Failed), "{status:?}");
    }
    assert!(
        metrics
            .runs
            .iter()
            .all(|r| r.kind == RunKind::PostProcessing
                && r.adapter == ScriptAdapter::Nzbget
                && r.waited)
    );
    assert_eq!(metrics.durations.len(), 1);
    assert_eq!(metrics.durations[0].status, RunStatus::Failed);
    assert_eq!(metrics.durations[0].duration.count, 1);
    assert_eq!(metrics.nonzero_exits, 1);
    assert_eq!(metrics.last_exit_code, Some(3));
    assert_eq!(metrics.last_duration_ms, Some(1500));
    assert_eq!(metrics.last_finished_epoch_ms, Some(1_700_000_000_000));
    assert_eq!(metrics.last_status, Some(RunStatus::Failed));
}

#[test]
fn a_run_that_never_launched_and_an_interrupted_run_have_their_own_statuses() {
    let name = "run-metrics-not-started";
    record_finished(&result(name, "failed", 0), false);
    assert_eq!(script(name).last_status, Some(RunStatus::NotStarted));
    record_interrupted(&result(name, "failed", 0));
    let metrics = script(name);
    assert_eq!(metrics.last_status, Some(RunStatus::Interrupted));
    let count = |status| {
        metrics
            .runs
            .iter()
            .find(|r| r.status == status)
            .map_or(0, |r| r.runs)
    };
    assert_eq!(count(RunStatus::NotStarted), 1);
    assert_eq!(count(RunStatus::Interrupted), 1);
    assert_eq!(count(RunStatus::Failed), 0);
    assert_eq!(metrics.nonzero_exits, 0);
}

#[test]
fn an_interrupted_script_counts_as_interrupted() {
    let name = "run-metrics-cut-off";
    let mut cut_off = result(name, "interrupted", 0);
    cut_off.exit_code = None;
    record_finished(&cut_off, true);
    assert_eq!(script(name).last_status, Some(RunStatus::Interrupted));

    // A script that failed on its own is a failure.
    record_finished(&result(name, "failed", 1), true);
    assert_eq!(script(name).last_status, Some(RunStatus::Failed));
}
