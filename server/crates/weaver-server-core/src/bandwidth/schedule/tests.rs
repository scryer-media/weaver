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
    let entries = vec![entry(
        "1",
        "08:00",
        vec![Weekday::Mon, Weekday::Fri],
        ScheduleAction::Pause,
    )];
    let now = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
    assert!(find_active_entry(&entries, Weekday::Mon, now).is_some());
    assert!(find_active_entry(&entries, Weekday::Wed, now).is_none());
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
fn no_entries_before_current_time() {
    let entries = vec![entry("1", "18:00", vec![], ScheduleAction::Pause)];
    let now = NaiveTime::from_hms_opt(8, 0, 0).unwrap();
    assert!(find_active_entry(&entries, Weekday::Mon, now).is_none());
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
