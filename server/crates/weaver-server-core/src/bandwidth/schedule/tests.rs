use super::*;

#[tokio::test]
async fn daylight_saving_fallback_does_not_reassert_or_rewind_hold_tracks() {
    let entries = vec![
        entry("pause", "00:30", vec![], ScheduleAction::Pause),
        entry("resume", "01:30", vec![], ScheduleAction::Resume),
    ];
    let mut evaluator = HoldEvaluator::default();
    let mut actions = Vec::new();
    for (local, utc) in [
        (local_time(28, 1, 59), local_time(28, 7, 59)),
        (local_time(28, 1, 0), local_time(28, 8, 0)),
        (local_time(28, 1, 30), local_time(28, 8, 30)),
        (local_time(28, 2, 0), local_time(28, 9, 0)),
    ] {
        assert!(
            evaluator
                .apply_effects_at(&entries, local, utc, |action, _| {
                    actions.push(action);
                    std::future::ready(Ok(()))
                })
                .await
                .is_empty()
        );
    }
    assert_eq!(actions, [ScheduleAction::Resume]);
    evaluator
        .apply_effects_at(
            &entries,
            local_time(28, 1, 29),
            local_time(28, 8, 54),
            |action, _| {
                actions.push(action);
                std::future::ready(Ok(()))
            },
        )
        .await;
    assert_eq!(
        actions.last(),
        Some(&ScheduleAction::Pause),
        "a backward UTC jump exceeding the correction tolerance still replays holds"
    );
}

#[tokio::test]
async fn startup_readiness_reports_server_and_intake_failures() {
    for (action, succeeds) in [
        (
            ScheduleAction::SetServerActive {
                server_id: 999,
                active: false,
            },
            false,
        ),
        (ScheduleAction::PauseWatchFolderScanning, false),
    ] {
        let (commands, _received) = tokio::sync::mpsc::channel(1);
        let (events, _) = tokio::sync::broadcast::channel(1);
        let handle = SchedulerHandle::new(
            commands,
            events,
            crate::SharedPipelineState::new(crate::PipelineMetrics::new(), vec![]),
        );
        let schedules = Arc::new(RwLock::new(vec![entry("initial", "00:00", vec![], action)]));
        let (task, ready) =
            spawn_evaluator_with_services(handle, schedules, ScheduleServices::default());
        assert_eq!(ready.await.unwrap().is_ok(), succeeds);
        task.shutdown().await;
    }
}
use crate::runtime::HardwareProfile;

fn find_active_on_track(
    entries: &[ScheduleEntry],
    day: Weekday,
    time: NaiveTime,
    track: ScheduleTrack,
) -> Option<&ScheduleEntry> {
    most_recently_fired(entries, day, time, |entry| {
        entry.action.track() == Some(track)
    })
    .map(|(entry, _, _)| entry)
}

fn find_active_entry(
    entries: &[ScheduleEntry],
    day: Weekday,
    time: NaiveTime,
) -> Option<&ScheduleEntry> {
    find_active_on_track(entries, day, time, ScheduleTrack::Downloads)
}

fn find_active_profile(
    entries: &[ScheduleEntry],
    day: Weekday,
    time: NaiveTime,
) -> Option<HardwareProfile> {
    find_active_on_track(entries, day, time, ScheduleTrack::Profile).and_then(|entry| {
        match entry.action {
            ScheduleAction::HardwareProfile { profile } => Some(profile),
            _ => None,
        }
    })
}

fn entry(id: &str, time: &str, days: Vec<Weekday>, action: ScheduleAction) -> ScheduleEntry {
    ScheduleEntry {
        id: id.into(),
        enabled: true,
        label: String::new(),
        days,
        time: time.into(),
        times: Vec::new(),
        every_hour_at_minute: None,
        action,
    }
}

#[test]
fn picks_most_recent_entry() {
    let entries = vec![
        entry("1", "08:00", vec![], ScheduleAction::Pause),
        entry("2", "18:00", vec![], ScheduleAction::Resume),
    ];
    let now = NaiveTime::from_hms_opt(20, 0, 0).unwrap();
    let active = find_active_entry(&entries, Weekday::Mon, now).unwrap();
    assert_eq!(active.id, "2");
}

#[test]
fn picks_pause_before_resume_fires() {
    let entries = vec![
        entry("1", "08:00", vec![], ScheduleAction::Pause),
        entry("2", "18:00", vec![], ScheduleAction::Resume),
    ];
    let now = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
    let active = find_active_entry(&entries, Weekday::Mon, now).unwrap();
    assert_eq!(active.id, "1");
}

#[test]
fn respects_day_filter() {
    let entries = vec![
        entry("mon", "08:00", vec![Weekday::Mon], ScheduleAction::Pause),
        entry("wed", "08:00", vec![Weekday::Wed], ScheduleAction::Resume),
    ];
    let now = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
    assert_eq!(
        find_active_entry(&entries, Weekday::Mon, now).unwrap().id,
        "mon"
    );
    assert_eq!(
        find_active_entry(&entries, Weekday::Wed, now).unwrap().id,
        "wed"
    );
}

#[test]
fn a_rule_holds_across_the_days_it_skips() {
    let entries = vec![
        entry("mon", "08:00", vec![Weekday::Mon], ScheduleAction::Pause),
        entry("fri", "08:00", vec![Weekday::Fri], ScheduleAction::Resume),
    ];
    let now = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
    assert_eq!(
        find_active_entry(&entries, Weekday::Wed, now).unwrap().id,
        "mon"
    );
    assert_eq!(
        find_active_entry(&entries, Weekday::Sun, now).unwrap().id,
        "fri"
    );
}

#[test]
fn a_pause_set_in_the_evening_holds_past_midnight() {
    let entries = vec![
        entry("resume", "06:00", vec![], ScheduleAction::Resume),
        entry("pause", "23:00", vec![], ScheduleAction::Pause),
    ];
    for (hour, minute, expected) in [
        (23, 30, "pause"),
        (0, 0, "pause"),
        (5, 59, "pause"),
        (6, 0, "resume"),
        (22, 59, "resume"),
    ] {
        let now = NaiveTime::from_hms_opt(hour, minute, 0).unwrap();
        assert_eq!(
            find_active_entry(&entries, Weekday::Tue, now).unwrap().id,
            expected,
            "at {hour:02}:{minute:02}"
        );
    }
}

#[test]
fn a_single_rule_is_in_force_before_its_time_of_day() {
    let entries = vec![entry("1", "18:00", vec![], ScheduleAction::Pause)];
    let now = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
    assert_eq!(
        find_active_entry(&entries, Weekday::Mon, now).unwrap().id,
        "1"
    );
}

#[test]
fn empty_days_means_every_day() {
    let entries = vec![entry("1", "08:00", vec![], ScheduleAction::Pause)];
    let now = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
    assert!(find_active_entry(&entries, Weekday::Sat, now).is_some());
}

#[test]
fn disabled_entry_skipped() {
    let mut e = entry("1", "08:00", vec![], ScheduleAction::Pause);
    e.enabled = false;
    let now = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
    assert!(find_active_entry(&[e], Weekday::Mon, now).is_none());
}

#[test]
fn speed_limit_entry() {
    let entries = vec![
        entry(
            "1",
            "09:00",
            vec![],
            ScheduleAction::SpeedLimit {
                bytes_per_sec: 1_000_000,
            },
        ),
        entry("2", "17:00", vec![], ScheduleAction::Resume),
    ];
    let now = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
    let active = find_active_on_track(&entries, Weekday::Mon, now, ScheduleTrack::Speed).unwrap();
    assert_eq!(
        active.action,
        ScheduleAction::SpeedLimit {
            bytes_per_sec: 1_000_000
        }
    );
}

fn profile_rule(
    id: &str,
    time: &str,
    days: Vec<Weekday>,
    profile: HardwareProfile,
) -> ScheduleEntry {
    entry(id, time, days, ScheduleAction::HardwareProfile { profile })
}

fn at(hour: u32, minute: u32) -> NaiveTime {
    NaiveTime::from_hms_opt(hour, minute, 0).unwrap()
}

#[test]
fn profile_rule_holds_across_midnight() {
    let entries = vec![
        profile_rule("day", "17:00", vec![], HardwareProfile::Efficient),
        profile_rule("night", "23:00", vec![], HardwareProfile::Performance),
    ];
    assert_eq!(
        find_active_profile(&entries, Weekday::Tue, at(16, 59)),
        Some(HardwareProfile::Performance)
    );
    assert_eq!(
        find_active_profile(&entries, Weekday::Tue, at(17, 0)),
        Some(HardwareProfile::Efficient)
    );
    assert_eq!(
        find_active_profile(&entries, Weekday::Tue, at(23, 30)),
        Some(HardwareProfile::Performance)
    );
    assert_eq!(
        find_active_profile(&entries, Weekday::Wed, at(0, 5)),
        Some(HardwareProfile::Performance)
    );
}

#[test]
fn profile_rule_holds_across_days_it_skips() {
    let entries = vec![
        profile_rule(
            "fri",
            "18:00",
            vec![Weekday::Fri],
            HardwareProfile::Efficient,
        ),
        profile_rule(
            "mon",
            "08:00",
            vec![Weekday::Mon],
            HardwareProfile::Balanced,
        ),
    ];
    assert_eq!(
        find_active_profile(&entries, Weekday::Sun, at(12, 0)),
        Some(HardwareProfile::Efficient)
    );
    assert_eq!(
        find_active_profile(&entries, Weekday::Thu, at(12, 0)),
        Some(HardwareProfile::Balanced)
    );
}

#[test]
fn single_weekly_profile_rule_is_in_force_all_week() {
    let entries = vec![profile_rule(
        "only",
        "18:00",
        vec![Weekday::Fri],
        HardwareProfile::Efficient,
    )];
    assert_eq!(
        find_active_profile(&entries, Weekday::Fri, at(17, 0)),
        Some(HardwareProfile::Efficient)
    );
}

#[test]
fn no_profile_rule_means_no_scheduled_profile() {
    let mut disabled = profile_rule("off", "08:00", vec![], HardwareProfile::Efficient);
    disabled.enabled = false;
    let entries = vec![
        disabled,
        entry("pause", "08:00", vec![], ScheduleAction::Pause),
    ];
    assert_eq!(find_active_profile(&entries, Weekday::Mon, at(9, 0)), None);
}

#[test]
fn profile_rules_and_other_actions_do_not_displace_each_other() {
    let entries = vec![
        entry(
            "limit",
            "08:00",
            vec![],
            ScheduleAction::SpeedLimit {
                bytes_per_sec: 1_000_000,
            },
        ),
        profile_rule("profile", "17:00", vec![], HardwareProfile::Efficient),
    ];
    let active =
        find_active_on_track(&entries, Weekday::Mon, at(18, 0), ScheduleTrack::Speed).unwrap();
    assert_eq!(active.id, "limit");
    assert_eq!(
        find_active_profile(&entries, Weekday::Mon, at(18, 0)),
        Some(HardwareProfile::Efficient)
    );
}

#[test]
fn later_profile_rule_wins_at_the_same_minute() {
    let entries = vec![
        profile_rule("a", "17:00", vec![], HardwareProfile::Efficient),
        profile_rule("b", "17:00", vec![], HardwareProfile::Balanced),
    ];
    assert_eq!(
        find_active_profile(&entries, Weekday::Mon, at(17, 0)),
        Some(HardwareProfile::Balanced)
    );
}

fn local_time(day: u32, hour: u32, minute: u32) -> NaiveDateTime {
    chrono::NaiveDate::from_ymd_opt(2026, 9, day)
        .unwrap()
        .and_hms_opt(hour, minute, 0)
        .unwrap()
}

async fn tick(
    evaluator: &mut HoldEvaluator,
    entries: &[ScheduleEntry],
    now: NaiveDateTime,
) -> Vec<ScheduleAction> {
    let mut actions = Vec::new();
    evaluator
        .apply(entries, now, |action| {
            actions.push(action);
            std::future::ready(Ok(()))
        })
        .await;
    actions
}

#[tokio::test]
async fn startup_replays_every_track_and_watch_rules_do_not_hide_download_state() {
    let entries = vec![
        entry(
            "limit",
            "22:00",
            vec![],
            ScheduleAction::SpeedLimit {
                bytes_per_sec: 1024,
            },
        ),
        entry("pause", "23:00", vec![], ScheduleAction::Pause),
        entry(
            "watch",
            "23:30",
            vec![],
            ScheduleAction::PauseWatchFolderScanning,
        ),
        profile_rule("profile", "23:45", vec![], HardwareProfile::Efficient),
    ];
    let mut evaluator = HoldEvaluator::default();
    assert_eq!(
        tick(&mut evaluator, &entries, local_time(29, 1, 0)).await,
        vec![
            ScheduleAction::Pause,
            ScheduleAction::PauseWatchFolderScanning,
            ScheduleAction::SpeedLimit {
                bytes_per_sec: 1024
            },
            ScheduleAction::HardwareProfile {
                profile: HardwareProfile::Efficient
            },
        ]
    );
    assert!(
        tick(&mut evaluator, &entries, local_time(29, 1, 1))
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn equal_actions_from_different_rules_and_new_daily_occurrences_reapply() {
    let entries = vec![
        entry("first", "08:00", vec![], ScheduleAction::Pause),
        entry("second", "09:00", vec![], ScheduleAction::Pause),
    ];
    let mut evaluator = HoldEvaluator::default();
    assert_eq!(
        tick(&mut evaluator, &entries, local_time(28, 8, 0)).await,
        vec![ScheduleAction::Pause]
    );
    assert_eq!(
        tick(&mut evaluator, &entries, local_time(28, 9, 0)).await,
        vec![ScheduleAction::Pause]
    );
    // Advance in regular ticks so this exercises occurrence identity, not jump replay.
    let start = local_time(28, 9, 0);
    for minutes in 1..23 * 60 {
        assert!(
            tick(&mut evaluator, &entries, start + Duration::minutes(minutes))
                .await
                .is_empty()
        );
    }
    assert_eq!(
        tick(&mut evaluator, &entries, local_time(29, 8, 0)).await,
        vec![ScheduleAction::Pause]
    );
}

#[tokio::test]
async fn a_failed_apply_retries_without_repeating_successful_tracks() {
    let entries = vec![
        entry("pause", "08:00", vec![], ScheduleAction::Pause),
        entry(
            "watch",
            "08:00",
            vec![],
            ScheduleAction::PauseWatchFolderScanning,
        ),
    ];
    let mut evaluator = HoldEvaluator::default();
    evaluator
        .apply(&entries, local_time(28, 8, 0), |action| {
            std::future::ready(if action == ScheduleAction::Pause {
                Err("unavailable".into())
            } else {
                Ok(())
            })
        })
        .await;
    assert_eq!(
        tick(&mut evaluator, &entries, local_time(28, 8, 1)).await,
        vec![ScheduleAction::Pause]
    );
    assert!(
        tick(&mut evaluator, &entries, local_time(28, 8, 2))
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn deleting_the_last_rule_does_not_revert_and_reenabling_reapplies() {
    let entries = vec![entry("pause", "08:00", vec![], ScheduleAction::Pause)];
    let mut evaluator = HoldEvaluator::default();
    tick(&mut evaluator, &entries, local_time(28, 8, 0)).await;
    assert!(
        tick(&mut evaluator, &[], local_time(28, 8, 1))
            .await
            .is_empty()
    );
    assert_eq!(
        tick(&mut evaluator, &entries, local_time(28, 8, 2)).await,
        vec![ScheduleAction::Pause]
    );
    let mut edited = entries;
    edited[0].action = ScheduleAction::Resume;
    assert_eq!(
        tick(&mut evaluator, &edited, local_time(28, 8, 3)).await,
        vec![ScheduleAction::Resume]
    );
}

#[tokio::test]
async fn clock_jumps_replay_holds() {
    let entries = vec![entry("pause", "08:00", vec![], ScheduleAction::Pause)];
    let mut evaluator = HoldEvaluator::default();
    tick(&mut evaluator, &entries, local_time(28, 8, 0)).await;
    assert!(
        tick(&mut evaluator, &entries, local_time(28, 9, 30))
            .await
            .is_empty()
    );
    assert_eq!(
        tick(&mut evaluator, &entries, local_time(28, 11, 1)).await,
        vec![ScheduleAction::Pause]
    );
    assert_eq!(
        tick(&mut evaluator, &entries, local_time(28, 10, 55)).await,
        vec![ScheduleAction::Pause]
    );
}

#[tokio::test]
async fn small_backward_clock_steps_preserve_applied_holds() {
    let entries = vec![
        entry("resume", "08:00", vec![], ScheduleAction::Resume),
        entry(
            "speed",
            "08:00",
            vec![],
            ScheduleAction::SpeedLimit {
                bytes_per_sec: 1024,
            },
        ),
    ];
    let mut evaluator = HoldEvaluator::default();
    let now = local_time(28, 8, 0);
    assert_eq!(tick(&mut evaluator, &entries, now).await.len(), 2);
    // Crossing the occurrence boundary backwards must not replace an operator's
    // manual pause or speed setting, nor replay when the clock catches up.
    assert!(
        tick(&mut evaluator, &entries, now - Duration::seconds(1))
            .await
            .is_empty()
    );
    assert!(tick(&mut evaluator, &entries, now).await.is_empty());
}

#[test]
fn one_shots_skip_startup_and_fire_crossed_minutes_once() {
    let entries = vec![entry(
        "scan",
        "08:00",
        vec![],
        ScheduleAction::ScanWatchFolder,
    )];
    let mut evaluator = OneShotEvaluator::default();
    assert!(evaluator.due(&entries, local_time(28, 7, 59)).is_empty());
    assert_eq!(evaluator.due(&entries, local_time(28, 8, 1)), entries);
    assert!(evaluator.due(&entries, local_time(28, 8, 1)).is_empty());
    let mut restarted = OneShotEvaluator::default();
    assert!(restarted.due(&entries, local_time(28, 8, 1)).is_empty());
}

#[test]
fn one_shots_deduplicate_fall_back_and_cross_spring_gap() {
    let entries = vec![entry(
        "scan",
        "01:30",
        vec![],
        ScheduleAction::ScanWatchFolder,
    )];
    let mut evaluator = OneShotEvaluator::default();
    evaluator.due(&entries, local_time(28, 1, 29));
    assert_eq!(evaluator.due(&entries, local_time(28, 1, 31)).len(), 1);
    assert!(evaluator.due(&entries, local_time(28, 1, 0)).is_empty());
    assert!(evaluator.due(&entries, local_time(28, 1, 31)).is_empty());
    let entries = vec![entry(
        "gap",
        "02:30",
        vec![],
        ScheduleAction::ScanWatchFolder,
    )];
    evaluator.due(&entries, local_time(28, 1, 59));
    assert_eq!(evaluator.due(&entries, local_time(28, 3, 0)).len(), 1);
}

#[test]
fn one_shots_suppress_large_forward_and_backward_jumps() {
    let entries = vec![entry(
        "scan",
        "08:00",
        vec![],
        ScheduleAction::ScanWatchFolder,
    )];
    let mut evaluator = OneShotEvaluator::default();
    evaluator.due(&entries, local_time(28, 7, 0));
    assert!(evaluator.due(&entries, local_time(28, 9, 0)).is_empty());
    assert!(evaluator.due(&entries, local_time(28, 7, 0)).is_empty());
    assert_eq!(evaluator.due(&entries, local_time(28, 8, 0)).len(), 1);
}

#[test]
fn one_shots_support_distinct_rules_multiple_times_hourly_and_weekdays() {
    let mut first = entry(
        "first",
        "08:00",
        vec![Weekday::Mon],
        ScheduleAction::ScanWatchFolder,
    );
    first.times = vec!["08:00".into(), "08:15".into(), "08:15".into()];
    let mut hourly = entry("hourly", "00:00", vec![], ScheduleAction::ScanWatchFolder);
    hourly.every_hour_at_minute = Some(15);
    let entries = vec![first, hourly];
    let mut evaluator = OneShotEvaluator::default();
    evaluator.due(&entries, local_time(28, 7, 59));
    assert_eq!(evaluator.due(&entries, local_time(28, 8, 16)).len(), 3);
    assert_eq!(evaluator.due(&entries, local_time(28, 9, 16)).len(), 1);
    evaluator.due(&entries, local_time(29, 7, 59));
    assert_eq!(evaluator.due(&entries, local_time(29, 8, 16)).len(), 1);
}

#[tokio::test]
async fn pause_all_replays_independent_components_and_resume_ends_them() {
    let entries = vec![
        entry("all", "22:00", vec![], ScheduleAction::PauseAll),
        entry(
            "watch",
            "23:00",
            vec![],
            ScheduleAction::ResumeWatchFolderScanning,
        ),
        entry("resume", "06:00", vec![], ScheduleAction::Resume),
    ];
    let mut evaluator = HoldEvaluator::default();
    let mut effects = Vec::new();
    evaluator
        .apply_effects(&entries, local_time(28, 23, 30), |action, track| {
            effects.push((track, action));
            std::future::ready(Ok(()))
        })
        .await;
    assert_eq!(
        effects,
        vec![
            (ScheduleTrack::Downloads, ScheduleAction::PauseAll),
            (
                ScheduleTrack::WatchFolder,
                ScheduleAction::ResumeWatchFolderScanning
            ),
            (ScheduleTrack::Rss, ScheduleAction::PauseAll),
        ]
    );
    effects.clear();
    evaluator
        .apply_effects(&entries, local_time(29, 6, 0), |action, track| {
            effects.push((track, action));
            std::future::ready(Ok(()))
        })
        .await;
    assert_eq!(effects.len(), 3);
    assert!(
        effects
            .iter()
            .all(|(_, action)| *action == ScheduleAction::Resume)
    );
}

#[tokio::test]
async fn timed_resume_defers_scheduled_resume_without_suppressing_pause_or_retry() {
    let db = crate::Database::open_in_memory().unwrap();
    db.set_setting("nzbget.scheduled_resume_at", "12345")
        .unwrap();
    let (commands, mut received) = tokio::sync::mpsc::channel(4);
    let (events, _) = tokio::sync::broadcast::channel(1);
    let handle = SchedulerHandle::new(
        commands,
        events,
        crate::SharedPipelineState::new(crate::PipelineMetrics::new(), vec![]),
    );
    let services = ScheduleServices {
        db: Some(db.clone()),
        ..Default::default()
    };
    let entries = vec![entry("resume", "08:00", vec![], ScheduleAction::Resume)];
    let mut evaluator = HoldEvaluator::default();
    evaluator
        .apply_effects(&entries, local_time(28, 8, 0), |action, track| {
            apply_schedule_action(handle.clone(), services.clone(), action, Some(track))
        })
        .await;
    assert!(evaluator.applied.is_empty());
    assert!(received.try_recv().is_err());

    let pipeline = tokio::spawn(async move {
        let mut applied = Vec::new();
        while let Some(command) = received.recv().await {
            if let crate::SchedulerCommand::ApplyScheduleAction { action, reply } = command {
                applied.push(action);
                reply.send(()).unwrap();
            } else {
                panic!("unexpected schedule command");
            }
        }
        applied
    });
    apply_schedule_action(
        handle.clone(),
        services.clone(),
        ScheduleAction::Pause,
        Some(ScheduleTrack::Downloads),
    )
    .await
    .unwrap();
    assert_eq!(
        db.get_setting("nzbget.scheduled_resume_at")
            .unwrap()
            .as_deref(),
        Some("12345")
    );
    db.set_setting("nzbget.scheduled_resume_at", "0").unwrap();
    evaluator
        .apply_effects(&entries, local_time(28, 8, 1), |action, track| {
            apply_schedule_action(handle.clone(), services.clone(), action, Some(track))
        })
        .await;
    assert_eq!(evaluator.applied.len(), 1);
    drop(handle);
    assert_eq!(
        pipeline.await.unwrap(),
        [ScheduleAction::Pause, ScheduleAction::Resume]
    );
}

#[tokio::test]
async fn resume_clears_pause_all_components_after_the_pause_rule_is_removed() {
    let mut evaluator = HoldEvaluator::default();
    tick(
        &mut evaluator,
        &[entry("all", "22:00", vec![], ScheduleAction::PauseAll)],
        local_time(28, 22, 0),
    )
    .await;
    assert!(
        tick(&mut evaluator, &[], local_time(28, 22, 30))
            .await
            .is_empty()
    );
    let entries = vec![entry("resume", "23:00", vec![], ScheduleAction::Resume)];
    let mut effects = Vec::new();
    evaluator
        .apply_effects(&entries, local_time(28, 23, 0), |action, track| {
            effects.push((track, action));
            std::future::ready(Ok(()))
        })
        .await;
    assert_eq!(
        effects,
        [
            (ScheduleTrack::Downloads, ScheduleAction::Resume),
            (ScheduleTrack::WatchFolder, ScheduleAction::Resume),
            (ScheduleTrack::Rss, ScheduleAction::Resume),
        ]
    );
}

#[tokio::test]
async fn one_shot_dispatch_queues_every_occurrence_without_blocking_hold_changes() {
    let entries: Vec<_> = (0..40)
        .map(|index| {
            entry(
                &format!("scan-{index}"),
                "08:00",
                vec![],
                ScheduleAction::ScanWatchFolder,
            )
        })
        .collect();
    let mut evaluator = OneShotEvaluator::default();
    evaluator.due(&entries, local_time(28, 7, 59));
    let mut dispatcher = OneShotDispatcher::default();
    dispatcher
        .pending
        .extend(evaluator.due(&entries, local_time(28, 8, 0)));
    let permits = Arc::new(tokio::sync::Semaphore::new(0));
    let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
    let (finished, mut finishes) = tokio::sync::mpsc::unbounded_channel();
    let apply = |entry: ScheduleEntry| {
        let permits = permits.clone();
        let started = started.clone();
        let finished = finished.clone();
        async move {
            started.send(entry.id.clone()).unwrap();
            permits.acquire().await.unwrap().forget();
            finished.send(entry.id).unwrap();
        }
    };
    dispatcher.dispatch(apply);
    let mut ids = BTreeSet::new();
    for _ in 0..MAX_RUNNING_ONE_SHOTS {
        assert!(ids.insert(starts.recv().await.unwrap()));
    }
    assert_eq!(dispatcher.pending.len(), 8);
    assert_eq!(dispatcher.running.len(), MAX_RUNNING_ONE_SHOTS);
    assert!(starts.try_recv().is_err());
    assert!(finishes.try_recv().is_err());
    let mut holds = HoldEvaluator::default();
    assert_eq!(
        tick(
            &mut holds,
            &[entry("pause", "08:00", vec![], ScheduleAction::Pause)],
            local_time(28, 8, 0)
        )
        .await,
        [ScheduleAction::Pause]
    );
    assert!(evaluator.due(&entries, local_time(28, 8, 1)).is_empty());
    permits.add_permits(entries.len());
    while let Some(result) = dispatcher.running.join_next().await {
        result.unwrap();
        dispatcher.dispatch(apply);
    }
    let mut completed = BTreeSet::new();
    for _ in &entries {
        assert!(completed.insert(finishes.recv().await.unwrap()));
    }
    assert_eq!(
        completed,
        entries.iter().map(|entry| entry.id.clone()).collect()
    );
    assert!(finishes.try_recv().is_err());
    assert!(dispatcher.pending.is_empty());
}

#[tokio::test]
async fn initial_replay_and_shutdown_wait_for_runtime_acknowledgement() {
    let (commands, mut received) = tokio::sync::mpsc::channel(1);
    let (events, _) = tokio::sync::broadcast::channel(1);
    let handle = SchedulerHandle::new(
        commands,
        events,
        crate::SharedPipelineState::new(crate::PipelineMetrics::new(), vec![]),
    );
    let schedules = Arc::new(RwLock::new(vec![entry(
        "pause",
        "08:00",
        vec![],
        ScheduleAction::Pause,
    )]));
    let (task, mut replayed) =
        spawn_evaluator_with_services(handle, schedules, ScheduleServices::default());
    let Some(crate::SchedulerCommand::ApplyScheduleAction { action, reply }) =
        received.recv().await
    else {
        panic!("initial hold was not sent to the runtime");
    };
    assert_eq!(action, ScheduleAction::Pause);
    assert_eq!(
        replayed.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    );
    let mut shutdown = Box::pin(task.shutdown());
    tokio::select! {
        biased;
        () = &mut shutdown => panic!("shutdown must wait for the running hold"),
        () = std::future::ready(()) => {},
    }
    reply.send(()).unwrap();
    replayed.await.unwrap().unwrap();
    shutdown.await;
    assert!(received.recv().await.is_none());
}

#[tokio::test]
async fn timed_download_resume_does_not_defer_intake_resume() {
    let db = crate::Database::open_in_memory().unwrap();
    db.set_setting("nzbget.scheduled_resume_at", "12345")
        .unwrap();
    let config = Arc::new(RwLock::new(db.load_config().unwrap()));
    let (commands, mut received) = tokio::sync::mpsc::channel(1);
    let (events, _) = tokio::sync::broadcast::channel(1);
    let handle = SchedulerHandle::new(
        commands,
        events,
        crate::SharedPipelineState::new(crate::PipelineMetrics::new(), vec![]),
    );
    let rss = crate::rss::RssService::new(handle.clone(), config.clone(), db.clone());
    let watch_folder = WatchFolderService::new(db.clone(), handle.clone(), config.clone());
    rss.set_scheduled_paused(true);
    watch_folder.set_scanning_paused(true).await.unwrap();
    let services = ScheduleServices {
        db: Some(db),
        rss: Some(rss.clone()),
        watch_folder: Some(watch_folder),
        ..Default::default()
    };
    for track in [ScheduleTrack::Rss, ScheduleTrack::WatchFolder] {
        apply_schedule_action(
            handle.clone(),
            services.clone(),
            ScheduleAction::Resume,
            Some(track),
        )
        .await
        .unwrap();
    }
    assert!(!rss.is_scheduled_paused());
    assert!(!config.read().await.watch_folder.scanning_paused);
    assert!(matches!(
        apply_schedule_action(
            handle,
            services,
            ScheduleAction::Resume,
            Some(ScheduleTrack::Downloads)
        )
        .await,
        Err(ScheduleApplyError::Pending)
    ));
    assert!(received.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn failed_initial_hold_reports_readiness_and_retries_after_repair() {
    let db = crate::Database::open_in_memory().unwrap();
    let config = Arc::new(RwLock::new(db.load_config().unwrap()));
    let (commands, _) = tokio::sync::mpsc::channel(1);
    let (events, _) = tokio::sync::broadcast::channel(1);
    let handle = SchedulerHandle::new(
        commands,
        events,
        crate::SharedPipelineState::new(crate::PipelineMetrics::new(), vec![]),
    );
    let watch_folder = WatchFolderService::new(db.clone(), handle.clone(), config.clone());
    let schedules = Arc::new(RwLock::new(vec![entry(
        "watch",
        "00:00",
        vec![],
        ScheduleAction::PauseWatchFolderScanning,
    )]));
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        crate::persistence::sql_runtime::SqlRuntime::execute(datastore.read_exec(),
            "CREATE TRIGGER reject_watch_pause BEFORE INSERT ON settings WHEN NEW.key = 'watch_folder.scanning_paused' BEGIN SELECT RAISE(FAIL, 'injected write failure'); END", &[]).await?;
        Ok(())
    }).unwrap();
    let (task, replayed) = spawn_evaluator_with_services(
        handle,
        schedules,
        ScheduleServices {
            watch_folder: Some(watch_folder.clone()),
            db: Some(db.clone()),
            ..Default::default()
        },
    );
    assert!(replayed.await.unwrap().is_err());
    assert!(config.read().await.watch_folder.scanning_paused);
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        crate::persistence::sql_runtime::SqlRuntime::execute(
            datastore.read_exec(),
            "DROP TRIGGER reject_watch_pause",
            &[],
        )
        .await?;
        Ok(())
    })
    .unwrap();
    tokio::time::advance(crate::e2e_clock::schedule_poll_interval()).await;
    while db
        .get_setting("watch_folder.scanning_paused")
        .unwrap()
        .as_deref()
        != Some("true")
    {
        tokio::task::yield_now().await;
    }
    task.shutdown().await;
}

#[tokio::test]
async fn failed_watch_folder_resume_does_not_pause_scanning() {
    let db = crate::Database::open_in_memory().unwrap();
    let config = Arc::new(RwLock::new(db.load_config().unwrap()));
    let (commands, _) = tokio::sync::mpsc::channel(1);
    let (events, _) = tokio::sync::broadcast::channel(1);
    let handle = SchedulerHandle::new(
        commands,
        events,
        crate::SharedPipelineState::new(crate::PipelineMetrics::new(), vec![]),
    );
    let watch_folder = WatchFolderService::new(db.clone(), handle.clone(), config.clone());
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        crate::persistence::sql_runtime::SqlRuntime::execute(datastore.read_exec(),
            "CREATE TRIGGER reject_watch_pause BEFORE INSERT ON settings WHEN NEW.key = 'watch_folder.scanning_paused' BEGIN SELECT RAISE(FAIL, 'injected write failure'); END", &[]).await?;
        Ok(())
    }).unwrap();
    let services = ScheduleServices {
        watch_folder: Some(watch_folder),
        db: Some(db.clone()),
        ..Default::default()
    };
    assert!(
        apply_schedule_action(
            handle.clone(),
            services.clone(),
            ScheduleAction::Resume,
            Some(ScheduleTrack::WatchFolder),
        )
        .await
        .is_err()
    );
    assert!(!config.read().await.watch_folder.scanning_paused);
    assert!(
        apply_schedule_action(
            handle,
            services,
            ScheduleAction::PauseWatchFolderScanning,
            Some(ScheduleTrack::WatchFolder),
        )
        .await
        .is_err()
    );
    assert!(config.read().await.watch_folder.scanning_paused);
}

#[tokio::test]
async fn failed_scheduled_server_activation_restores_persisted_state() {
    let db = crate::Database::open_in_memory().unwrap();
    let server: crate::servers::ServerConfig = serde_json::from_value(serde_json::json!({
        "id": 42, "host": "news.example.test", "port": 119, "tls": false,
        "connections": 1, "active": false
    }))
    .unwrap();
    db.insert_server(&server).unwrap();
    let config = Arc::new(RwLock::new(db.load_config().unwrap()));
    let (commands, _) = tokio::sync::mpsc::channel(1);
    let (events, _) = tokio::sync::broadcast::channel(1);
    let handle = SchedulerHandle::new(
        commands,
        events,
        crate::SharedPipelineState::new(crate::PipelineMetrics::new(), vec![]),
    );
    let service = crate::servers::service::ServersService::new(db.clone(), config.clone(), handle);
    for _ in 0..2 {
        let error = service.set_active(42, true).await.unwrap_err();
        assert!(error.contains("server transfer policy registry unavailable"));
        assert!(!config.read().await.servers[0].active);
        assert!(!db.load_config().unwrap().servers[0].active);
    }
}

#[tokio::test(start_paused = true)]
async fn initial_metadata_failure_pauses_intake_until_recovery() {
    let db = crate::Database::open_in_memory().unwrap();
    let config = Arc::new(RwLock::new(db.load_config().unwrap()));
    let (commands, _) = tokio::sync::mpsc::channel(1);
    let (events, _) = tokio::sync::broadcast::channel(1);
    let handle = SchedulerHandle::new(
        commands,
        events,
        crate::SharedPipelineState::new(crate::PipelineMetrics::new(), vec![]),
    );
    let watch_folder = WatchFolderService::new(db.clone(), handle.clone(), config.clone());
    let rss = crate::rss::RssService::new(handle.clone(), config.clone(), db.clone());
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        crate::persistence::sql_runtime::SqlRuntime::execute(
            datastore.read_exec(),
            "ALTER TABLE settings RENAME TO unavailable_settings",
            &[],
        )
        .await?;
        Ok(())
    })
    .unwrap();
    let (task, replayed) = spawn_evaluator_with_services(
        handle,
        Arc::new(RwLock::new(vec![])),
        ScheduleServices {
            watch_folder: Some(watch_folder),
            rss: Some(rss.clone()),
            db: Some(db.clone()),
            ..Default::default()
        },
    );
    assert!(
        replayed
            .await
            .unwrap()
            .unwrap_err()
            .contains("cannot load schedule intake state")
    );
    assert!(config.read().await.watch_folder.scanning_paused);
    assert!(rss.is_scheduled_paused());
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        crate::persistence::sql_runtime::SqlRuntime::execute(
            datastore.read_exec(),
            "ALTER TABLE unavailable_settings RENAME TO settings",
            &[],
        )
        .await?;
        Ok(())
    })
    .unwrap();
    tokio::time::advance(crate::e2e_clock::schedule_poll_interval()).await;
    while rss.is_scheduled_paused() {
        tokio::task::yield_now().await;
    }
    assert!(!config.read().await.watch_folder.scanning_paused);
    task.shutdown().await;
}
