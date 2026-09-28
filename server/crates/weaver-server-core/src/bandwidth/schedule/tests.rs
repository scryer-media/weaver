use super::*;

fn entry(id: &str, time: &str, days: Vec<Weekday>, action: ScheduleAction) -> ScheduleEntry {
    ScheduleEntry {
        id: id.into(),
        enabled: true,
        label: String::new(),
        days,
        time: time.into(),
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
    let active = find_active_entry(&entries, Weekday::Mon, now).unwrap();
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
    let active = find_active_entry(&entries, Weekday::Mon, at(18, 0)).unwrap();
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
