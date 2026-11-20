// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::super::instances::{InstanceSchedule, ScriptInstanceDraft};
use super::super::model::ScriptName;
use super::*;
use crate::post_processing::instances::PostProcessingInstancesQueries;

fn at(time: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(time, "%Y-%m-%d %H:%M").unwrap()
}

fn job(times: &[&str]) -> ScriptInstance {
    ScriptInstance {
        id: "test".into(),
        name: "test".into(),
        script: ScriptName::new("tidy.sh").unwrap(),
        trigger: InstanceTrigger::Schedule,
        inputs: Vec::new(),
        categories: Vec::new(),
        enabled: true,
        blocking: true,
        timeout_seconds: None,
        run_order: 0,
        schedule: InstanceSchedule {
            days: Vec::new(),
            times: times.iter().map(|time| time.to_string()).collect(),
            run_at_startup: false,
        },
    }
}

#[test]
fn startup_skips_old_runs_and_forward_jumps_catch_up_once() {
    let mut tracker = Occurrences::default();
    let jobs = [job(&["01:30"])];
    assert!(tracker.advance(&jobs, at("2026-10-25 01:31")).is_empty());
    assert_eq!(tracker.advance(&jobs, at("2026-10-26 01:31")), ["test"]);
    assert!(tracker.advance(&jobs, at("2026-10-26 01:29")).is_empty());
    assert!(tracker.advance(&jobs, at("2026-10-26 01:30")).is_empty());
    assert!(tracker.advance(&jobs, at("2026-10-26 01:00")).is_empty());
    assert!(tracker.advance(&jobs, at("2026-10-26 01:31")).is_empty());
}

#[test]
fn a_long_sleep_catches_up_the_latest_weekly_occurrence_once() {
    let mut tracker = Occurrences::default();
    let mut weekly = job(&["01:30", "02:30"]);
    weekly.schedule.days = vec![Weekday::Mon];
    let jobs = [weekly];
    tracker.advance(&jobs, at("2026-09-01 00:00"));
    assert_eq!(tracker.advance(&jobs, at("2026-10-09 12:00")), ["test"]);
    assert!(tracker.fired.contains(&(
        "test".into(),
        at("2026-10-05 02:30").date(),
        at("2026-10-05 02:30").time()
    )));
    assert!(tracker.advance(&jobs, at("2026-10-09 12:01")).is_empty());
}

#[test]
fn spring_gap_and_hourly_times_fire_once_per_occurrence() {
    let mut tracker = Occurrences::default();
    let jobs = [job(&["02:30"])];
    tracker.advance(&jobs, at("2026-03-08 01:59"));
    assert_eq!(tracker.advance(&jobs, at("2026-03-08 03:00")).len(), 1);
    assert!(tracker.advance(&jobs, at("2026-03-08 03:01")).is_empty());
    let jobs = [job(&["*:15"])];
    assert_eq!(tracker.advance(&jobs, at("2026-03-08 03:15")).len(), 1);
    assert_eq!(tracker.advance(&jobs, at("2026-03-08 04:15")).len(), 1);
}

#[test]
fn startup_requires_an_explicit_opt_in_and_fires_only_once() {
    let mut startup = job(&[]);
    let mut tracker = Occurrences::default();
    assert!(
        tracker
            .advance(&[startup.clone()], at("2026-03-08 01:00"))
            .is_empty()
    );
    startup.schedule.run_at_startup = true;
    let mut tracker = Occurrences::default();
    assert_eq!(
        tracker.advance(&[startup.clone()], at("2026-03-08 01:00")),
        ["test"]
    );
    assert!(
        tracker
            .advance(&[startup], at("2026-03-08 01:01"))
            .is_empty()
    );
}

#[test]
fn a_time_named_twice_runs_the_job_once() {
    let mut tracker = Occurrences::default();
    let jobs = [job(&["*:15", "03:15", "04:30"])];
    tracker.advance(&jobs, at("2026-03-08 03:14"));
    assert_eq!(tracker.advance(&jobs, at("2026-03-08 03:15")), ["test"]);
    assert_eq!(tracker.advance(&jobs, at("2026-03-08 04:15")), ["test"]);
    assert_eq!(tracker.advance(&jobs, at("2026-03-08 04:30")), ["test"]);
}

#[test]
fn a_job_runs_only_on_its_days_and_only_while_turned_on() {
    let mut friday = job(&["07:00"]);
    friday.schedule.days = vec![Weekday::Fri];
    let mut off = job(&["07:00"]);
    off.id = "off".into();
    off.enabled = false;
    let jobs = [friday, off];
    let mut tracker = Occurrences::default();
    // 2026-10-08 is a Thursday.
    tracker.advance(&jobs, at("2026-10-08 06:59"));
    assert!(tracker.advance(&jobs, at("2026-10-08 07:00")).is_empty());
    tracker.advance(&jobs, at("2026-10-09 06:59"));
    assert_eq!(tracker.advance(&jobs, at("2026-10-09 07:00")), ["test"]);
}

#[test]
fn the_jobs_are_read_again_only_after_one_changed() {
    let db = Database::open_in_memory().unwrap();
    let script = ScriptName::new("tidy.sh").unwrap();
    assert!(db.schedule_jobs().unwrap().is_empty());
    let first = db
        .create_script_instance(
            ScriptInstanceDraft::new(script.clone(), InstanceTrigger::Schedule)
                .runs_at(&[], &["07:00"]),
        )
        .unwrap();
    let read = db.schedule_jobs().unwrap();
    assert_eq!(read.len(), 1);
    // Served from memory until something changes.
    assert!(Arc::ptr_eq(&read, &db.schedule_jobs().unwrap()));

    let mut moved = ScriptInstanceDraft::from_instance(&first);
    moved.trigger = InstanceTrigger::Scan;
    db.update_script_instance(&first.id, moved).unwrap();
    assert!(db.schedule_jobs().unwrap().is_empty());
}
