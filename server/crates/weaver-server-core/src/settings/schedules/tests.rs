use super::*;

fn rule(id: &str, time: &str, days: Vec<Weekday>, action: ScheduleAction) -> ScheduleEntry {
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

fn limit(id: &str, time: &str, days: Vec<Weekday>) -> ScheduleEntry {
    rule(
        id,
        time,
        days,
        ScheduleAction::SpeedLimit {
            bytes_per_sec: 1024,
        },
    )
}

#[test]
fn migration_preserves_resets_across_watch_rules_and_week_boundaries() {
    let entries = vec![
        limit("limit", "23:00", vec![Weekday::Sun]),
        rule(
            "watch",
            "23:30",
            vec![Weekday::Sun],
            ScheduleAction::PauseWatchFolderScanning,
        ),
        rule("resume", "07:00", vec![], ScheduleAction::Resume),
        rule("pause", "22:00", vec![], ScheduleAction::Pause),
    ];
    let migrated = migrate_legacy_tracks(entries.clone());
    assert_eq!(migrated.len(), entries.len() + 1);
    let reset = &migrated[3];
    assert_eq!(reset.time, "07:00");
    assert_eq!(reset.days, vec![Weekday::Mon]);
    assert_eq!(reset.action, ScheduleAction::ConfiguredSpeedLimit);
    assert_eq!(migrate_legacy_tracks(migrated.clone()), migrated);
}

#[test]
fn migration_keeps_same_minute_order_and_avoids_id_collisions() {
    let migrated = migrate_legacy_tracks(vec![
        limit("limit", "07:00", vec![]),
        rule("pause", "23:00", vec![], ScheduleAction::Pause),
        limit("pause-speed-reset", "23:00", vec![]),
        rule("resume", "23:00", vec![], ScheduleAction::Resume),
    ]);
    assert_eq!(migrated.len(), 6);
    assert_eq!(migrated[2].id, "pause-speed-reset-reset");
    assert_eq!(migrated[3].id, "pause-speed-reset");
    assert_eq!(migrated[5].action, ScheduleAction::ConfiguredSpeedLimit);
    assert!(migrated[2].days.is_empty());
}

#[test]
fn migration_preserves_reset_when_a_legacy_disabled_limit_is_enabled_later() {
    let mut disabled = limit("disabled", "07:00", vec![]);
    disabled.enabled = false;
    let entries = vec![
        disabled,
        limit("invalid", "25:00", vec![]),
        rule("pause", "08:00", vec![], ScheduleAction::Pause),
        rule("resume", "09:00", vec![], ScheduleAction::Resume),
    ];
    let migrated = migrate_legacy_tracks(entries);
    assert_eq!(migrated.len(), 5);
    assert_eq!(migrated[3].action, ScheduleAction::ConfiguredSpeedLimit);
    assert_eq!(migrated[3].time, "08:00");
    assert!(!migrated[0].enabled);
    assert!(migrate_legacy_tracks(vec![]).is_empty());
}

#[test]
fn a_same_minute_watch_rule_shadowing_pause_does_not_add_a_speed_reset() {
    let entries = vec![
        limit("limit", "07:00", vec![]),
        rule("pause", "23:00", vec![], ScheduleAction::Pause),
        rule(
            "watch",
            "23:00",
            vec![],
            ScheduleAction::PauseWatchFolderScanning,
        ),
    ];
    assert_eq!(migrate_legacy_tracks(entries.clone()), entries);
}

#[test]
fn pause_all_history_survives_rule_removal() {
    let db = Database::open_in_memory().unwrap();
    db.save_schedules(&[rule("all", "22:00", vec![], ScheduleAction::PauseAll)])
        .unwrap();
    db.save_schedules(&[]).unwrap();
    assert_eq!(
        db.get_setting("schedule_pause_all_used")
            .unwrap()
            .as_deref(),
        Some("true")
    );
}

#[test]
fn migration_restores_configured_limit_after_an_unlimited_override_too() {
    let entries = vec![
        rule(
            "unlimited",
            "06:00",
            vec![],
            ScheduleAction::SpeedLimit { bytes_per_sec: 0 },
        ),
        rule("resume", "08:00", vec![], ScheduleAction::Resume),
    ];
    let migrated = migrate_legacy_tracks(entries);
    assert_eq!(migrated.len(), 3);
    assert_eq!(
        migrated[0].action,
        ScheduleAction::SpeedLimit { bytes_per_sec: 0 }
    );
    assert_eq!(migrated[2].action, ScheduleAction::ConfiguredSpeedLimit);
    assert_eq!(migrate_legacy_tracks(migrated.clone()), migrated);
}

#[test]
fn legacy_settings_are_migrated_once_and_persisted_atomically() {
    let db = Database::open_in_memory().unwrap();
    let entries = vec![
        limit("limit", "07:00", vec![]),
        rule("resume", "23:00", vec![], ScheduleAction::Resume),
    ];
    db.set_setting("schedules", &serde_json::to_string(&entries).unwrap())
        .unwrap();
    let migrated = db.list_schedules().unwrap();
    assert_eq!(migrated.len(), 3);
    assert_eq!(
        db.get_setting(TRACKS_VERSION_KEY).unwrap().as_deref(),
        Some(TRACKS_VERSION)
    );
    assert_eq!(
        decode(&db.get_setting("schedules").unwrap().unwrap()).unwrap(),
        migrated
    );
    assert_eq!(db.list_schedules().unwrap(), migrated);
    // An operator can remove the compatibility rule without it coming back.
    db.save_schedules(&entries).unwrap();
    assert_eq!(db.list_schedules().unwrap(), entries);
}

#[test]
fn new_schedule_sets_are_never_migrated() {
    let db = Database::open_in_memory().unwrap();
    let entries = vec![
        limit("limit", "07:00", vec![]),
        rule("resume", "23:00", vec![], ScheduleAction::Resume),
    ];
    db.save_schedules(&entries).unwrap();
    assert_eq!(db.list_schedules().unwrap(), entries);
}

#[test]
fn invalid_legacy_json_leaves_settings_and_version_untouched() {
    let db = Database::open_in_memory().unwrap();
    db.set_setting("schedules", "invalid").unwrap();
    assert!(db.list_schedules().is_err());
    assert_eq!(
        db.get_setting("schedules").unwrap().as_deref(),
        Some("invalid")
    );
    assert_eq!(db.get_setting(TRACKS_VERSION_KEY).unwrap(), None);
}

#[test]
fn disabled_legacy_download_rules_enable_their_reset_companions() {
    for action in [ScheduleAction::Pause, ScheduleAction::Resume] {
        let db = Database::open_in_memory().unwrap();
        let mut disabled = rule("download", "08:00", vec![], action);
        disabled.enabled = false;
        let entries = vec![
            limit("limit", "07:00", vec![]),
            disabled,
            rule("later", "09:00", vec![], ScheduleAction::Resume),
        ];
        db.set_setting("schedules", &serde_json::to_string(&entries).unwrap())
            .unwrap();
        let mut migrated = db.list_schedules().unwrap();
        assert_eq!(migrated.len(), 5);
        assert!(!migrated[2].enabled);
        assert!(migrated[4].enabled);
        assert_eq!(migrate_legacy_tracks(migrated.clone()), migrated);
        migrated[1].enabled = true;
        db.save_schedules(&migrated).unwrap();
        let mut saved = db.list_schedules().unwrap();
        assert!(saved[2].enabled);
        saved[2].enabled = false;
        db.save_schedules(&saved).unwrap();
        saved[1].enabled = false;
        db.save_schedules(&saved).unwrap();
        saved[1].enabled = true;
        db.save_schedules(&saved).unwrap();
        assert!(!db.list_schedules().unwrap()[2].enabled);
        saved.remove(2);
        saved[1].enabled = false;
        db.save_schedules(&saved).unwrap();
        saved[1].enabled = true;
        db.save_schedules(&saved).unwrap();
        assert_eq!(db.list_schedules().unwrap(), saved);
    }
}

#[test]
fn unreadable_reset_links_do_not_block_schedule_saves() {
    let db = Database::open_in_memory().unwrap();
    let entries = vec![limit("limit", "07:00", vec![])];
    db.save_schedules(&entries).unwrap();
    db.set_setting("schedule_legacy_speed_reset_links", "invalid")
        .unwrap();
    db.save_schedules(&entries).unwrap();
    assert_eq!(db.list_schedules().unwrap(), entries);
    assert_eq!(
        db.get_setting("schedule_legacy_speed_reset_links")
            .unwrap()
            .as_deref(),
        Some("{}")
    );
}
