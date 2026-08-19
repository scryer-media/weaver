use super::executor::{execution_refusal, resolve_script_list};
use super::model::{
    OptionName, OptionValue, PostProcessingSettings, PostProcessingSummary, ResolvedOption,
    ScriptAdapter, ScriptList, ScriptListEntry, ScriptLists, ScriptName, ScriptResult,
    ScriptStatus, SecretOptionValue,
};
use super::settings::{encode_job_script_override, job_script_override};
use crate::persistence::Database;

fn script(name: &str) -> ScriptName {
    ScriptName::new(name).unwrap()
}

fn list(names: &[&str]) -> ScriptList {
    ScriptList::new(
        names
            .iter()
            .map(|name| ScriptListEntry::new(script(name)))
            .collect(),
    )
    .unwrap()
}

fn names(list: &ScriptList) -> Vec<String> {
    list.entries()
        .iter()
        .map(|entry| entry.script.as_str().to_string())
        .collect()
}

#[test]
fn resolution_prefers_a_job_override_then_the_category_then_the_global_default() {
    let mut lists = ScriptLists {
        global: list(&["global.sh"]),
        ..ScriptLists::default()
    };
    lists
        .categories
        .insert("movies".into(), list(&["movies.sh"]));

    assert_eq!(
        names(&resolve_script_list(&lists, None, None)),
        ["global.sh"]
    );
    assert_eq!(
        names(&resolve_script_list(&lists, Some("movies"), None)),
        ["movies.sh"]
    );
    assert_eq!(
        names(&resolve_script_list(&lists, Some("tv"), None)),
        ["global.sh"]
    );

    let override_entries = vec![ScriptListEntry::new(script("job.sh"))];
    assert_eq!(
        names(&resolve_script_list(
            &lists,
            Some("movies"),
            Some(override_entries)
        )),
        ["job.sh"]
    );
    // An explicit empty override means "run nothing", not "fall back".
    assert!(resolve_script_list(&lists, Some("movies"), Some(vec![])).is_empty());
}

#[test]
fn a_facade_override_round_trips_through_job_metadata() {
    let encoded = encode_job_script_override(&[script("first.sh"), script("second.sh")]);
    let metadata = vec![(
        super::settings::JOB_SCRIPT_OVERRIDE_METADATA_KEY.to_string(),
        encoded,
    )];
    let decoded = job_script_override(&metadata).unwrap();
    assert_eq!(
        decoded
            .iter()
            .map(|entry| entry.script.as_str())
            .collect::<Vec<_>>(),
        ["first.sh", "second.sh"]
    );

    // No key at all inherits the configured lists.
    assert!(job_script_override(&[]).is_none());
    // An empty value is an explicit "no scripts", which must not inherit.
    let disabled = vec![(
        super::settings::JOB_SCRIPT_OVERRIDE_METADATA_KEY.to_string(),
        String::new(),
    )];
    assert_eq!(job_script_override(&disabled).unwrap().len(), 0);
    // Names that could escape the scripts directory are dropped, not fatal.
    let hostile = vec![(
        super::settings::JOB_SCRIPT_OVERRIDE_METADATA_KEY.to_string(),
        "../escape,ok.sh".to_string(),
    )];
    assert_eq!(
        job_script_override(&hostile)
            .unwrap()
            .iter()
            .map(|entry| entry.script.as_str())
            .collect::<Vec<_>>(),
        ["ok.sh"]
    );
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
fn settings_lists_and_options_round_trip_through_the_settings_kv() {
    let db = Database::open_in_memory().unwrap();

    assert_eq!(
        db.post_processing_settings().unwrap(),
        PostProcessingSettings::default()
    );
    let settings = PostProcessingSettings {
        execution_enabled: true,
        concurrency: 4,
        termination_grace_seconds: 15,
        python_interpreter: Some("/usr/bin/python3".into()),
        ..PostProcessingSettings::default()
    };
    db.save_post_processing_settings(&settings).unwrap();
    assert_eq!(db.post_processing_settings().unwrap(), settings);

    let mut lists = ScriptLists {
        global: list(&["global.sh"]),
        ..ScriptLists::default()
    };
    lists
        .categories
        .insert("movies".into(), list(&["movies.sh"]));
    db.save_post_processing_script_lists(&lists).unwrap();
    assert_eq!(db.post_processing_script_lists().unwrap(), lists);
}

#[test]
fn secret_options_are_stored_encrypted_and_returned_for_execution() {
    let db = Database::open_in_memory().unwrap();
    let name = script("notify.sh");
    let options = vec![
        ResolvedOption::new(
            OptionName::new("Host").unwrap(),
            OptionValue::String("mail.example.invalid".into()),
        ),
        ResolvedOption::new(
            OptionName::new("Token").unwrap(),
            OptionValue::Secret(SecretOptionValue::from_admin_input("hunter2")),
        ),
    ];
    db.save_post_processing_script_options(&name, &options)
        .unwrap();

    let raw = db
        .get_setting("post_processing.script_options.v1")
        .unwrap()
        .unwrap();
    assert!(raw.contains("mail.example.invalid"));
    assert!(
        !raw.contains("hunter2"),
        "a secret option must not sit in the settings KV in cleartext"
    );

    let loaded = db.post_processing_script_options(&name).unwrap();
    assert_eq!(loaded.len(), 2);
    let secret = loaded
        .iter()
        .find(|option| option.name().as_str() == "Token")
        .unwrap();
    assert!(secret.value().is_secret());
    assert_eq!(
        db.post_processing_scripts_with_options().unwrap(),
        vec!["notify.sh".to_string()]
    );

    // Clearing the options removes the entry rather than leaving an empty shell.
    db.save_post_processing_script_options(&name, &[]).unwrap();
    assert!(db.post_processing_script_options(&name).unwrap().is_empty());
    assert!(
        db.post_processing_scripts_with_options()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn job_results_and_summary_are_stored_on_the_job_and_read_back() {
    let db = Database::open_in_memory().unwrap();
    let results = vec![ScriptResult {
        script: script("notify.sh"),
        adapter: ScriptAdapter::Sabnzbd,
        status: ScriptStatus::Warning,
        exit_code: Some(3),
        duration_ms: 12,
        output_tail: "tail".into(),
        output_truncated: false,
        error_message: Some("exited 3".into()),
        finished_at_epoch_ms: 1_000,
    }];
    // No row for the job yet, so the write is a no-op rather than an error.
    db.save_job_post_processing_results(7, PostProcessingSummary::Warning, &results)
        .unwrap();
    assert!(db.job_post_processing_results(7).unwrap().is_empty());
}
