// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

fn instance(id: &str, trigger: InstanceTrigger, categories: &[&str], order: i64) -> ScriptInstance {
    ScriptInstance {
        id: id.into(),
        name: id.into(),
        script: ScriptName::new("notify.sh").unwrap(),
        trigger,
        inputs: Vec::new(),
        categories: categories.iter().map(|name| name.to_string()).collect(),
        enabled: true,
        blocking: true,
        timeout_seconds: None,
        run_order: order,
        schedule: InstanceSchedule::default(),
    }
}

fn ids(instances: &[ScriptInstance]) -> Vec<&str> {
    instances
        .iter()
        .map(|instance| instance.id.as_str())
        .collect()
}

#[test]
fn a_category_adds_its_own_instances_after_the_ones_for_every_category() {
    let pp = InstanceTrigger::PostProcessing;
    let all = [
        instance("tv-late", pp, &["TV"], 0),
        instance("everything", pp, &[], 1),
        instance("movies", pp, &["movies"], 2),
        instance("tv-early", pp, &["tv", "anime"], 3),
        instance("scan", InstanceTrigger::Scan, &[], 4),
    ];
    let event = ScriptEventLabel::PostProcessing;
    let resolve = |category, mode| resolve_instances(&all, &event, category, mode);

    assert_eq!(
        ids(&resolve(Some(" tv "), GlobalScriptsRun::Always)),
        ["everything", "tv-late", "tv-early"]
    );
    assert_eq!(
        ids(&resolve(None, GlobalScriptsRun::Always)),
        ["everything"]
    );
    assert_eq!(
        ids(&resolve(
            Some("tv"),
            GlobalScriptsRun::OnlyWithoutCategoryScripts
        )),
        ["tv-late", "tv-early"]
    );
    assert_eq!(
        ids(&resolve(
            Some("books"),
            GlobalScriptsRun::OnlyWithoutCategoryScripts
        )),
        ["everything"]
    );
}

#[test]
fn an_instance_runs_only_on_its_own_trigger_and_only_while_enabled() {
    let added = InstanceTrigger::Queue(QueueEvent::NzbAdded);
    let mut off = instance("off", added, &[], 0);
    off.enabled = false;
    let all = [
        off,
        instance("added", added, &[], 1),
        instance(
            "deleted",
            InstanceTrigger::Queue(QueueEvent::NzbDeleted),
            &[],
            2,
        ),
        instance("feed", InstanceTrigger::Feed, &[], 3),
    ];
    let resolved = resolve_instances(
        &all,
        &ScriptEventLabel::Queue(QueueEvent::NzbAdded),
        Some("tv"),
        GlobalScriptsRun::Always,
    );
    assert_eq!(ids(&resolved), ["added"]);
    assert_eq!(
        ids(&resolve_instances(
            &all,
            &ScriptEventLabel::Feed(7),
            None,
            GlobalScriptsRun::OnlyWithoutCategoryScripts,
        )),
        ["feed"]
    );
}

#[test]
fn a_trigger_reads_back_as_it_was_written() {
    for trigger in [
        InstanceTrigger::PostProcessing,
        InstanceTrigger::Queue(QueueEvent::FileDownloaded),
        InstanceTrigger::Scan,
        InstanceTrigger::Schedule,
        InstanceTrigger::Feed,
    ] {
        assert_eq!(
            trigger.to_string().parse::<InstanceTrigger>().unwrap(),
            trigger
        );
    }
    assert!("queue:NOPE".parse::<InstanceTrigger>().is_err());
    assert!("cron".parse::<InstanceTrigger>().is_err());
}

#[test]
fn a_schedule_instance_keeps_its_run_times_and_another_trigger_drops_them() {
    let db = Database::open_in_memory().unwrap();
    let script = ScriptName::new("tidy.sh").unwrap();
    let saved = db
        .create_script_instance(
            ScriptInstanceDraft::new(script.clone(), InstanceTrigger::Schedule)
                .runs_at(
                    &[Weekday::Fri, Weekday::Mon, Weekday::Fri],
                    &["19:30", "*:05", "7:00"],
                )
                .at_startup(),
        )
        .unwrap();
    assert_eq!(
        saved.schedule,
        InstanceSchedule {
            days: vec![Weekday::Mon, Weekday::Fri],
            times: vec!["*:05".into(), "07:00".into(), "19:30".into()],
            run_at_startup: true,
        }
    );
    assert_eq!(db.script_instance(&saved.id).unwrap().unwrap(), saved);

    let every_day = db
        .update_script_instance(
            &saved.id,
            ScriptInstanceDraft::from_instance(&saved).runs_at(&Weekday::ALL, &["07:00"]),
        )
        .unwrap();
    assert!(every_day.schedule.days.is_empty());

    for wrong in ["*", "25:00", "noon"] {
        assert!(
            db.create_script_instance(
                ScriptInstanceDraft::new(script.clone(), InstanceTrigger::Schedule)
                    .runs_at(&[], &[wrong])
            )
            .is_err(),
            "accepted {wrong}"
        );
    }

    let mut moved = ScriptInstanceDraft::from_instance(&saved);
    moved.trigger = InstanceTrigger::Scan;
    let moved = db.update_script_instance(&saved.id, moved).unwrap();
    assert!(moved.schedule.is_empty());
}
