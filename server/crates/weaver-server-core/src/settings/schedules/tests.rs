use super::*;
use crate::bandwidth::{SpeedLimitChange, Weekday};
use crate::proxies::{EgressBinding, EgressInterface};

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

fn speeds(limits: &[(SpeedTarget, u64)]) -> ScheduleAction {
    ScheduleAction::SpeedLimit {
        limits: limits
            .iter()
            .map(|&(target, bytes_per_sec)| SpeedLimitChange {
                target,
                bytes_per_sec,
            })
            .collect(),
    }
}

fn second_egress(db: &Database) -> u32 {
    db.create_egress_interface(&EgressInterface {
        id: 0,
        name: "second line".into(),
        binding: EgressBinding::SourceAddress {
            address: "192.0.2.1".parse().unwrap(),
        },
        enabled: true,
        max_download_speed: 0,
        download_quota: Default::default(),
    })
    .unwrap()
    .id
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
fn speed_and_quota_rules_round_trip_with_their_targets() {
    let db = Database::open_in_memory().unwrap();
    let egress = second_egress(&db);
    let entries = vec![
        rule(
            "speeds",
            "07:00",
            vec![Weekday::Mon],
            speeds(&[
                (SpeedTarget::Global, 5_000_000),
                (SpeedTarget::Egress(egress), 0),
            ]),
        ),
        rule(
            "count",
            "08:00",
            vec![],
            ScheduleAction::SetQuotaMetering {
                enabled: false,
                target: QuotaTarget::Egress(egress),
            },
        ),
        rule("rss", "09:00", vec![], ScheduleAction::PauseRss),
    ];
    db.save_schedules(&entries).unwrap();
    assert_eq!(db.list_schedules().unwrap(), entries);
}

#[test]
fn a_quota_rule_saved_without_a_target_counts_for_every_egress() {
    let db = Database::open_in_memory().unwrap();
    db.set_setting(
        "schedules",
        r#"[{"id":"q","time":"08:00","action":{"type":"set_quota_metering","enabled":false}}]"#,
    )
    .unwrap();
    assert_eq!(
        db.list_schedules().unwrap()[0].action,
        ScheduleAction::SetQuotaMetering {
            enabled: false,
            target: QuotaTarget::AllEgresses,
        }
    );
}

#[test]
fn a_rule_naming_a_missing_egress_is_refused() {
    let db = Database::open_in_memory().unwrap();
    for action in [
        speeds(&[(SpeedTarget::Egress(99), 1)]),
        ScheduleAction::SetQuotaMetering {
            enabled: true,
            target: QuotaTarget::Egress(99),
        },
        speeds(&[(SpeedTarget::Server(99), 1)]),
    ] {
        assert!(
            db.save_schedules(&[rule("r", "07:00", vec![], action)])
                .is_err()
        );
    }
    assert!(db.list_schedules().unwrap().is_empty());
}

#[test]
fn a_deleted_egress_drops_out_of_speed_and_quota_rules_on_load() {
    let db = Database::open_in_memory().unwrap();
    let egress = second_egress(&db);
    let entries = vec![
        rule(
            "speeds",
            "07:00",
            vec![],
            speeds(&[
                (SpeedTarget::Global, 1_000),
                (SpeedTarget::Egress(egress), 2_000),
                (SpeedTarget::Egress(0), 3_000),
            ]),
        ),
        rule(
            "only",
            "08:00",
            vec![],
            speeds(&[(SpeedTarget::Egress(egress), 2_000)]),
        ),
        rule(
            "count",
            "09:00",
            vec![],
            ScheduleAction::SetQuotaMetering {
                enabled: false,
                target: QuotaTarget::Egress(egress),
            },
        ),
    ];
    db.save_schedules(&entries).unwrap();
    db.delete_egress_interface(egress).unwrap();

    let loaded = db.list_schedules().unwrap();
    assert_eq!(
        loaded,
        vec![
            rule(
                "speeds",
                "07:00",
                vec![],
                speeds(&[
                    (SpeedTarget::Global, 1_000),
                    (SpeedTarget::Egress(0), 3_000)
                ]),
            ),
            // Kept, doing nothing, so the operator sees what became of it.
            rule("only", "08:00", vec![], speeds(&[])),
        ]
    );
    // What was taken out is saved, so it is reported once.
    assert_eq!(
        decode(&db.get_setting("schedules").unwrap().unwrap()).unwrap(),
        loaded
    );
}

#[test]
fn an_egress_given_a_deleted_egress_id_inherits_none_of_its_rules() {
    let db = Database::open_in_memory().unwrap();
    let egress = second_egress(&db);
    db.save_schedules(&[
        rule(
            "speeds",
            "07:00",
            vec![],
            speeds(&[
                (SpeedTarget::Global, 1_000),
                (SpeedTarget::Egress(egress), 2_000),
            ]),
        ),
        rule(
            "count",
            "08:00",
            vec![],
            ScheduleAction::SetQuotaMetering {
                enabled: false,
                target: QuotaTarget::Egress(egress),
            },
        ),
    ])
    .unwrap();
    db.delete_egress_interface(egress).unwrap();

    // The delete itself took the rules out, before any load tidied them.
    let stripped = vec![rule(
        "speeds",
        "07:00",
        vec![],
        speeds(&[(SpeedTarget::Global, 1_000)]),
    )];
    assert_eq!(
        decode(&db.get_setting("schedules").unwrap().unwrap()).unwrap(),
        stripped
    );

    let reused = second_egress(&db);
    assert_eq!(reused, egress);
    assert_eq!(db.list_schedules().unwrap(), stripped);
}

#[test]
fn an_unreadable_rule_is_left_out_and_the_rest_load() {
    let db = Database::open_in_memory().unwrap();
    db.set_setting(
        "schedules",
        r#"[{"id":"gone","label":"Fetch feeds","time":"08:00","action":{"type":"fetch_rss","feed_id":null}},
            {"id":"kept","time":"09:00","action":{"type":"pause"}}]"#,
    )
    .unwrap();
    assert_eq!(
        db.list_schedules().unwrap(),
        vec![rule("kept", "09:00", vec![], ScheduleAction::Pause)]
    );
}

#[test]
fn unreadable_schedules_are_an_error_and_left_as_they_are() {
    let db = Database::open_in_memory().unwrap();
    db.set_setting("schedules", "invalid").unwrap();
    assert!(db.list_schedules().is_err());
    assert_eq!(
        db.get_setting("schedules").unwrap().as_deref(),
        Some("invalid")
    );
}
