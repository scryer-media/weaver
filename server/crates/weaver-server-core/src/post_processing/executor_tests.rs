use super::executor::{PostProcessingExecutor, execution_refusal};
use super::instances::{
    InstanceInputDraft, InstanceTrigger, ScriptInstance, ScriptInstanceDraft, ScriptInstanceError,
};
use super::model::{
    GlobalScriptsRun, PostProcessingSettings, PostProcessingSummary, QueueEvent, ScriptAdapter,
    ScriptEventLabel, ScriptName, ScriptResult, ScriptStatus,
};
use super::settings::normalize_script_directory;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

fn script(name: &str) -> ScriptName {
    ScriptName::new(name).unwrap()
}

fn draft(name: &str, trigger: InstanceTrigger) -> ScriptInstanceDraft {
    ScriptInstanceDraft::new(script(name), trigger)
}

fn names(instances: &[ScriptInstance]) -> Vec<&str> {
    instances
        .iter()
        .map(|instance| instance.name.as_str())
        .collect()
}

/// The stored value of one input, exactly as the row holds it.
fn stored_input(db: &Database, instance: &str, name: &str) -> Option<String> {
    let datastore = db.datastore();
    let (instance, name) = (instance.to_string(), name.to_string());
    db.run_sql_blocking_read(async move {
        SqlRuntime::fetch_optional(
            datastore.read_exec(),
            "SELECT value FROM script_instance_inputs WHERE instance_id = {} AND name = {}",
            &[SqlArg::Text(instance), SqlArg::Text(name)],
        )
        .await?
        .map(|row| row.text("value"))
        .transpose()
    })
    .unwrap()
}

fn refused(db: &Database, draft: ScriptInstanceDraft) -> &'static str {
    match db.create_script_instance(draft) {
        Err(ScriptInstanceError::Invalid(reason)) => reason,
        other => panic!("expected the instance to be refused, got {other:?}"),
    }
}

#[test]
fn execution_is_refused_while_disabled_and_under_strict_security() {
    let mut settings = PostProcessingSettings::default();
    assert_eq!(
        execution_refusal(&settings, false),
        Some("post-processing script execution is disabled")
    );
    settings.execution_enabled = true;
    assert_eq!(execution_refusal(&settings, false), None);
    // Strict security refuses even when the operator turned execution on, and
    // does so at run time rather than as a startup time bomb.
    assert_eq!(
        execution_refusal(&settings, true),
        Some("WEAVER_STRICT_SECURITY=1 refuses post-processing script execution")
    );
    settings.execution_enabled = false;
    assert_eq!(
        execution_refusal(&settings, true),
        Some("WEAVER_STRICT_SECURITY=1 refuses post-processing script execution")
    );
}

#[test]
fn settings_round_trip_through_the_settings_kv() {
    let db = Database::open_in_memory().unwrap();

    assert_eq!(
        db.post_processing_settings().unwrap(),
        PostProcessingSettings::default()
    );
    assert_eq!(
        PostProcessingSettings::default().global_scripts_run,
        GlobalScriptsRun::Always
    );
    let settings = PostProcessingSettings {
        execution_enabled: true,
        concurrency: 4,
        termination_grace_seconds: 15,
        python_interpreter: Some("/usr/bin/python3".into()),
        global_scripts_run: GlobalScriptsRun::OnlyWithoutCategoryScripts,
        ..PostProcessingSettings::default()
    };
    db.save_post_processing_settings(&settings).unwrap();
    assert_eq!(db.post_processing_settings().unwrap(), settings);
}

#[test]
fn an_instance_reads_back_as_it_was_saved() {
    let db = Database::open_in_memory().unwrap();
    let saved = db
        .create_script_instance(
            draft("notify.sh", InstanceTrigger::Queue(QueueEvent::NzbAdded))
                .named("  Tell the family  ")
                .input("Host", "mail.example.invalid")
                .secret_input("Token", "hunter2")
                .input("Empty", "")
                .category(" Movies ")
                .category("movies")
                .category("TV")
                .fire_and_forget()
                .timeout(90),
        )
        .unwrap();

    assert_eq!(saved.name, "Tell the family");
    assert_eq!(saved.script, script("notify.sh"));
    assert_eq!(saved.trigger, InstanceTrigger::Queue(QueueEvent::NzbAdded));
    // The same category under another casing is one category.
    assert_eq!(saved.categories, ["Movies", "TV"]);
    assert!(saved.enabled);
    assert!(!saved.blocking);
    assert_eq!(saved.timeout_seconds, Some(90));
    assert_eq!(
        saved
            .inputs
            .iter()
            .map(|input| (input.name.as_str(), input.value.as_str(), input.secret))
            .collect::<Vec<_>>(),
        // A secret's value is never handed back.
        [
            ("Host", "mail.example.invalid", false),
            ("Token", "", true),
            ("Empty", "", false),
        ]
    );
    assert_eq!(db.script_instances().unwrap(), std::slice::from_ref(&saved));
    assert_eq!(db.script_instance(&saved.id).unwrap(), Some(saved.clone()));
    assert_eq!(db.script_instance("missing").unwrap(), None);

    let sealed = stored_input(&db, &saved.id, "Token").unwrap();
    assert!(
        !sealed.contains("hunter2"),
        "a secret input must not be stored in clear"
    );
    let inputs = db.script_instance_run_inputs(&saved.id).unwrap().unwrap();
    assert_eq!(
        inputs
            .iter()
            .map(|input| (input.name().as_str(), input.value().is_secret()))
            .collect::<Vec<_>>(),
        [("Host", false), ("Token", true), ("Empty", false)]
    );
    assert!(db.script_instance_run_inputs("missing").unwrap().is_none());

    // An instance with no name of its own takes its script's.
    let unnamed = db
        .create_script_instance(draft("cleanup.py", InstanceTrigger::PostProcessing))
        .unwrap();
    assert_eq!(unnamed.name, "cleanup.py");
    assert!(unnamed.blocking);
    assert_eq!(unnamed.timeout_seconds, None);
    assert!(unnamed.categories.is_empty());
}

#[test]
fn an_update_keeps_a_secret_that_was_not_sent_again() {
    let db = Database::open_in_memory().unwrap();
    let saved = db
        .create_script_instance(
            draft("notify.sh", InstanceTrigger::PostProcessing)
                .input("Host", "old.example.invalid")
                .secret_input("Token", "hunter2")
                .secret_input("Other", "second"),
        )
        .unwrap();
    let sealed = stored_input(&db, &saved.id, "Token").unwrap();

    // What the editor sends back: plain values as shown, secrets left alone.
    let mut edit = ScriptInstanceDraft::from_instance(&saved).named("Renamed");
    edit.inputs[0].value = Some("new.example.invalid".into());
    edit.inputs.retain(|input| input.name != "Other");
    edit.inputs.push(InstanceInputDraft {
        name: "NeverSet".into(),
        value: None,
        secret: true,
    });
    let updated = db.update_script_instance(&saved.id, edit).unwrap();

    assert_eq!(updated.id, saved.id);
    assert_eq!(updated.name, "Renamed");
    assert_eq!(updated.run_order, saved.run_order);
    assert_eq!(
        updated
            .inputs
            .iter()
            .map(|input| (input.name.as_str(), input.value.as_str(), input.secret))
            .collect::<Vec<_>>(),
        // A secret that was never given a value has nothing to keep.
        [("Host", "new.example.invalid", false), ("Token", "", true)]
    );
    assert_eq!(stored_input(&db, &saved.id, "Token").unwrap(), sealed);
    assert_eq!(stored_input(&db, &saved.id, "Other"), None);

    // Sending a value replaces what was stored.
    let mut edit = ScriptInstanceDraft::from_instance(&updated);
    edit.inputs[1].value = Some("rotated".into());
    db.update_script_instance(&saved.id, edit).unwrap();
    let rotated = stored_input(&db, &saved.id, "Token").unwrap();
    assert_ne!(rotated, sealed);
    assert!(!rotated.contains("rotated"));

    assert!(matches!(
        db.update_script_instance("missing", draft("notify.sh", InstanceTrigger::Scan)),
        Err(ScriptInstanceError::NotFound)
    ));
}

#[test]
fn an_instance_that_could_not_run_as_written_is_refused() {
    let db = Database::open_in_memory().unwrap();
    let pp = InstanceTrigger::PostProcessing;

    assert_eq!(
        refused(&db, draft("a.sh", pp).named("x".repeat(129))),
        "instance name is too long or holds a control character"
    );
    assert_eq!(
        refused(&db, draft("a.sh", pp).named("two\nlines")),
        "instance name is too long or holds a control character"
    );
    for seconds in [0, 7 * 24 * 60 * 60 + 1] {
        assert_eq!(
            refused(&db, draft("a.sh", pp).timeout(seconds)),
            "timeout must be between one second and seven days"
        );
    }
    assert_eq!(
        refused(&db, draft("a.sh", pp).category("  ")),
        "category name is invalid"
    );
    // A category only means something where there is a download to have one.
    for trigger in [
        InstanceTrigger::Scan,
        InstanceTrigger::Schedule,
        InstanceTrigger::Feed,
    ] {
        assert_eq!(
            refused(&db, draft("a.sh", trigger).category("movies")),
            "only post-processing and queue instances can be narrowed to categories"
        );
    }
    assert_eq!(
        refused(&db, draft("a.sh", pp).input("Host", "a").input("HOST", "b")),
        "input names must be unique"
    );
    assert_eq!(
        refused(&db, draft("a.sh", pp).input("", "a")),
        "input name is invalid"
    );
    assert_eq!(
        refused(&db, draft("a.sh", pp).input("Host", "nul\0byte")),
        "input value is invalid"
    );
    assert_eq!(
        refused(
            &db,
            draft("a.sh", pp).input("Host", "x".repeat(64 * 1024 + 1))
        ),
        "input value is invalid"
    );
    assert!(db.script_instances().unwrap().is_empty());

    // The longest name and timeout that are allowed.
    db.create_script_instance(
        draft("a.sh", pp)
            .named("x".repeat(128))
            .timeout(7 * 24 * 60 * 60),
    )
    .unwrap();
}

#[test]
fn a_new_instance_goes_last_and_a_reorder_moves_only_what_it_names() {
    let db = Database::open_in_memory().unwrap();
    let pp = InstanceTrigger::PostProcessing;
    let ids = ["first", "second", "third", "fourth"]
        .map(|name| {
            db.create_script_instance(draft("a.sh", pp).named(name))
                .unwrap()
                .id
        })
        .to_vec();
    let saved = db.script_instances().unwrap();
    assert_eq!(names(&saved), ["first", "second", "third", "fourth"]);
    assert_eq!(
        saved
            .iter()
            .map(|instance| instance.run_order)
            .collect::<Vec<_>>(),
        [0, 1, 2, 3]
    );

    // The ones named come first in the order given; the rest keep theirs. An
    // id that no longer exists is passed over.
    db.reorder_script_instances(&[ids[2].clone(), "gone".into(), ids[0].clone()])
        .unwrap();
    let saved = db.script_instances().unwrap();
    assert_eq!(names(&saved), ["third", "first", "second", "fourth"]);
    assert_eq!(
        saved
            .iter()
            .map(|instance| instance.run_order)
            .collect::<Vec<_>>(),
        [0, 1, 2, 3]
    );

    assert!(db.delete_script_instance(&ids[0]).unwrap());
    assert!(!db.delete_script_instance(&ids[0]).unwrap());
    let last = db
        .create_script_instance(draft("a.sh", pp).named("fifth"))
        .unwrap();
    assert_eq!(
        names(&db.script_instances().unwrap()),
        ["third", "second", "fourth", "fifth"]
    );
    assert_eq!(last.run_order, 4);
}

#[test]
fn the_global_scripts_setting_decides_whether_a_category_adds_or_replaces() {
    let db = Database::open_in_memory().unwrap();
    let pp = InstanceTrigger::PostProcessing;
    for instance in [
        draft("a.sh", pp).named("movies").category("Movies"),
        draft("a.sh", pp).named("everything"),
        draft("a.sh", pp).named("off").disabled(),
        draft("a.sh", InstanceTrigger::Scan).named("scan"),
    ] {
        db.create_script_instance(instance).unwrap();
    }
    let event = ScriptEventLabel::PostProcessing;
    let resolved = |category| names_owned(&db.script_instances_for(&event, category).unwrap());

    assert_eq!(resolved(Some("movies")), ["everything", "movies"]);
    assert_eq!(resolved(Some("tv")), ["everything"]);
    assert_eq!(resolved(None), ["everything"]);

    db.save_post_processing_settings(&PostProcessingSettings {
        global_scripts_run: GlobalScriptsRun::OnlyWithoutCategoryScripts,
        ..PostProcessingSettings::default()
    })
    .unwrap();
    assert_eq!(resolved(Some("MOVIES")), ["movies"]);
    assert_eq!(resolved(Some("tv")), ["everything"]);
    assert_eq!(resolved(None), ["everything"]);
}

fn names_owned(instances: &[ScriptInstance]) -> Vec<String> {
    instances
        .iter()
        .map(|instance| instance.name.clone())
        .collect()
}

#[test]
fn a_feed_runs_only_feed_instances_and_loses_one_that_stops_being_one() {
    let db = Database::open_in_memory().unwrap();
    let first = db
        .create_script_instance(draft("feed.sh", InstanceTrigger::Feed).named("first"))
        .unwrap();
    let second = db
        .create_script_instance(draft("feed.sh", InstanceTrigger::Feed).named("second"))
        .unwrap();
    let scan = db
        .create_script_instance(draft("scan.sh", InstanceTrigger::Scan))
        .unwrap();

    assert!(matches!(
        db.set_feed_script_instances(7, std::slice::from_ref(&scan.id)),
        Err(ScriptInstanceError::Invalid(
            "only a feed instance can be attached to a feed"
        ))
    ));
    assert!(matches!(
        db.set_feed_script_instances(7, &[first.id.clone(), first.id.clone()]),
        Err(ScriptInstanceError::Invalid(
            "an instance can be attached to a feed once"
        ))
    ));
    assert!(matches!(
        db.set_feed_script_instances(7, &["missing".into()]),
        Err(ScriptInstanceError::NotFound)
    ));
    assert!(db.feed_script_instance_ids(7).unwrap().is_empty());

    // A feed runs its instances in the order it was given them.
    db.set_feed_script_instances(7, &[second.id.clone(), first.id.clone()])
        .unwrap();
    db.set_feed_script_instances(9, std::slice::from_ref(&first.id))
        .unwrap();
    assert_eq!(
        db.feed_script_instance_ids(7).unwrap(),
        [second.id.clone(), first.id.clone()]
    );
    let by_feed = db.feed_script_instance_ids_by_feed().unwrap();
    assert_eq!(by_feed.len(), 2);
    assert_eq!(by_feed[&9], std::slice::from_ref(&first.id));

    // Editing an instance leaves it attached while it is still a feed one.
    db.update_script_instance(
        &first.id,
        ScriptInstanceDraft::from_instance(&first).named("renamed"),
    )
    .unwrap();
    assert_eq!(
        db.feed_script_instance_ids(9).unwrap(),
        std::slice::from_ref(&first.id)
    );

    let mut moved = ScriptInstanceDraft::from_instance(&first);
    moved.trigger = InstanceTrigger::Scan;
    db.update_script_instance(&first.id, moved).unwrap();
    assert_eq!(
        db.feed_script_instance_ids(7).unwrap(),
        std::slice::from_ref(&second.id)
    );
    assert!(db.feed_script_instance_ids(9).unwrap().is_empty());

    assert!(db.delete_script_instance(&second.id).unwrap());
    assert!(db.feed_script_instance_ids_by_feed().unwrap().is_empty());

    db.set_feed_script_instances(7, &[]).unwrap();
    db.delete_feed_script_instances(7).unwrap();
}

#[test]
fn a_job_keeps_the_instances_it_was_admitted_with_through_a_directory_change() {
    let db = Database::open_in_memory().unwrap();
    let root = tempfile::tempdir().unwrap();
    let old_root = normalize_script_directory(&root.path().join("old")).unwrap();
    let new_root = normalize_script_directory(&root.path().join("new")).unwrap();
    db.replace_post_processing_script_directory(&old_root)
        .unwrap();
    let executor = PostProcessingExecutor::new(db.clone(), old_root.clone(), 1);

    // Nothing is captured while scripts are turned off.
    db.create_script_instance(draft("notify.sh", InstanceTrigger::PostProcessing))
        .unwrap();
    assert!(executor.admit_job_scripts(None).unwrap().is_none());

    db.save_post_processing_settings(&PostProcessingSettings {
        execution_enabled: true,
        ..PostProcessingSettings::default()
    })
    .unwrap();
    let (_, admitted_root) = db.post_processing_script_admission().unwrap();
    let admission = executor.admit_job_scripts(None).unwrap().unwrap();
    assert!(admission.has_enabled_entries());

    db.replace_post_processing_script_directory(&new_root)
        .unwrap();

    assert_eq!(admitted_root, old_root);
    assert!(admission.has_enabled_entries());
    // A job admitted now would run nothing: the new directory has not been
    // looked over by the operator yet.
    assert!(
        db.script_instances()
            .unwrap()
            .iter()
            .all(|instance| !instance.enabled)
    );
    assert!(
        !executor
            .admit_job_scripts(None)
            .unwrap()
            .unwrap()
            .has_enabled_entries()
    );
}

#[test]
fn job_results_and_summary_are_stored_on_the_job_and_read_back() {
    let db = Database::open_in_memory().unwrap();
    let results = vec![ScriptResult {
        script: script("notify.sh"),
        instance_id: Some("one".into()),
        instance_name: Some("Tell the family".into()),
        event: Default::default(),
        adapter: ScriptAdapter::Sabnzbd,
        status: ScriptStatus::Warning,
        exit_code: Some(3),
        duration_ms: 12,
        output_tail: "tail".into(),
        output_id: None,
        background: false,
        output_truncated: false,
        error_message: Some("exited 3".into()),
        finished_at_epoch_ms: 1_000,
    }];
    // No row for the job yet, so the write is a no-op rather than an error.
    db.save_job_post_processing_results(7, PostProcessingSummary::Warning, &results)
        .unwrap();
    assert!(db.job_post_processing_results(7).unwrap().is_empty());
}
