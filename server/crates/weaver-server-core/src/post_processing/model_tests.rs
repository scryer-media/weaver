use super::model::{
    OptionName, OptionValue, PostProcessingSettings, PostProcessingSummary, ResolvedOption,
    ScriptAdapter, ScriptList, ScriptListEntry, ScriptLists, ScriptManifest, ScriptName,
    ScriptOption, ScriptOptionType, ScriptStatus, SecretOptionValue, merge_post_processing_summary,
};

fn script(name: &str) -> ScriptName {
    ScriptName::new(name).unwrap()
}

fn manifest(options: Vec<ScriptOption>) -> ScriptManifest {
    ScriptManifest::new(
        ScriptAdapter::Sabnzbd,
        None,
        "Example".into(),
        None,
        "run.sh".into(),
        vec![],
        options,
    )
    .unwrap()
}

fn option(name: &str, option_type: ScriptOptionType, default: Option<OptionValue>) -> ScriptOption {
    ScriptOption::new(
        None,
        OptionName::new(name).unwrap(),
        option_type,
        default,
        None,
        vec![],
        vec![],
        false,
    )
    .unwrap()
}

#[test]
fn script_names_stay_inside_the_scripts_directory() {
    assert!(ScriptName::new("cleanup.sh").is_ok());
    assert!(ScriptName::new("Video Sort").is_ok());
    for rejected in [
        "",
        " leading",
        "trailing ",
        "../escape",
        "nested/name",
        r"nested\name",
        "stream:name",
        ".hidden",
        "trailing.",
        "CON",
        "com1.txt",
        "null\0byte",
    ] {
        assert!(
            ScriptName::new(rejected).is_err(),
            "accepted {rejected:?} as a script name"
        );
    }
}

#[test]
fn script_lists_reject_duplicates_and_zero_timeouts() {
    assert!(ScriptList::new(vec![ScriptListEntry::new(script("a.sh"))]).is_ok());
    assert!(
        ScriptList::new(vec![
            ScriptListEntry::new(script("a.sh")),
            ScriptListEntry::new(script("a.sh")),
        ])
        .is_err()
    );
    let zero = ScriptListEntry {
        script: script("a.sh"),
        enabled: true,
        timeout_seconds: Some(0),
    };
    assert!(ScriptList::new(vec![zero]).is_err());
}

#[test]
fn category_overrides_beat_the_global_default_case_insensitively() {
    let mut lists = ScriptLists {
        global: ScriptList::new(vec![ScriptListEntry::new(script("global.sh"))]).unwrap(),
        ..ScriptLists::default()
    };
    lists.categories.insert(
        "Movies".into(),
        ScriptList::new(vec![ScriptListEntry::new(script("movies.sh"))]).unwrap(),
    );

    assert_eq!(
        lists.resolve(None).entries()[0].script.as_str(),
        "global.sh"
    );
    assert_eq!(
        lists.resolve(Some("tv")).entries()[0].script.as_str(),
        "global.sh"
    );
    // Download clients echo their own casing back, so the lookup cannot be exact.
    for category in ["Movies", "movies", " MOVIES "] {
        assert_eq!(
            lists.resolve(Some(category)).entries()[0].script.as_str(),
            "movies.sh",
            "category {category:?} did not resolve its override"
        );
    }
}

#[test]
fn disabled_entries_are_kept_in_order_but_never_run() {
    let list = ScriptList::new(vec![
        ScriptListEntry::new(script("first.sh")),
        ScriptListEntry {
            script: script("second.sh"),
            enabled: false,
            timeout_seconds: None,
        },
        ScriptListEntry::new(script("third.sh")),
    ])
    .unwrap();
    assert_eq!(list.entries().len(), 3);
    let enabled = list
        .enabled_entries()
        .map(|entry| entry.script.as_str().to_string())
        .collect::<Vec<_>>();
    assert_eq!(enabled, ["first.sh", "third.sh"]);
}

#[test]
fn the_job_rollup_reports_the_worst_script_outcome() {
    use PostProcessingSummary::{Cancelled, Failed, Interrupted, NotRun, Succeeded, Warning};
    assert_eq!(merge_post_processing_summary(NotRun, NotRun), NotRun);
    assert_eq!(merge_post_processing_summary(Succeeded, NotRun), Succeeded);
    assert_eq!(merge_post_processing_summary(Succeeded, Warning), Warning);
    assert_eq!(merge_post_processing_summary(Warning, Failed), Failed);
    assert_eq!(
        merge_post_processing_summary(Failed, Interrupted),
        Interrupted
    );
    assert_eq!(
        merge_post_processing_summary(Interrupted, Cancelled),
        Cancelled
    );
    assert_eq!(
        merge_post_processing_summary(Cancelled, Succeeded),
        Cancelled
    );
}

#[test]
fn script_status_maps_onto_the_job_summary() {
    assert_eq!(
        ScriptStatus::Succeeded.summary(),
        PostProcessingSummary::Succeeded
    );
    // NZBGet's "NONE" is a decision, not a problem.
    assert_eq!(
        ScriptStatus::Skipped.summary(),
        PostProcessingSummary::Succeeded
    );
    assert_eq!(
        ScriptStatus::Warning.summary(),
        PostProcessingSummary::Warning
    );
    assert_eq!(
        ScriptStatus::Failed.summary(),
        PostProcessingSummary::Failed
    );
    assert_eq!(
        ScriptStatus::TimedOut.summary(),
        PostProcessingSummary::Failed
    );
    assert_eq!(
        ScriptStatus::Cancelled.summary(),
        PostProcessingSummary::Cancelled
    );
}

#[test]
fn options_merge_over_manifest_defaults_and_reject_undeclared_or_mistyped_keys() {
    let manifest = manifest(vec![
        option(
            "mode",
            ScriptOptionType::String,
            Some(OptionValue::String("safe".into())),
        ),
        option("token", ScriptOptionType::Secret, None),
    ]);

    let resolved = manifest.resolve_options(&[]).unwrap();
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].name().as_str(), "mode");

    let supplied = vec![
        ResolvedOption::new(
            OptionName::new("mode").unwrap(),
            OptionValue::String("fast".into()),
        ),
        ResolvedOption::new(
            OptionName::new("token").unwrap(),
            OptionValue::Secret(SecretOptionValue::from_admin_input("hunter2")),
        ),
    ];
    let resolved = manifest.resolve_options(&supplied).unwrap();
    assert_eq!(resolved.len(), 2);
    assert!(resolved[1].value().is_secret());

    let undeclared = vec![ResolvedOption::new(
        OptionName::new("nope").unwrap(),
        OptionValue::String("x".into()),
    )];
    assert!(manifest.resolve_options(&undeclared).is_err());

    let mistyped = vec![ResolvedOption::new(
        OptionName::new("mode").unwrap(),
        OptionValue::Integer(1),
    )];
    assert!(manifest.resolve_options(&mistyped).is_err());
}

#[test]
fn a_required_option_without_a_value_is_refused() {
    let required = ScriptOption::new(
        None,
        OptionName::new("token").unwrap(),
        ScriptOptionType::String,
        None,
        None,
        vec![],
        vec![],
        true,
    )
    .unwrap();
    assert!(manifest(vec![required]).resolve_options(&[]).is_err());
}

#[test]
fn secret_options_never_carry_a_manifest_default_and_never_serialize() {
    assert!(
        ScriptOption::new(
            None,
            OptionName::new("token").unwrap(),
            ScriptOptionType::Secret,
            Some(OptionValue::String("plaintext".into())),
            None,
            vec![],
            vec![],
            false,
        )
        .is_err()
    );
    let secret = OptionValue::Secret(SecretOptionValue::from_admin_input("hunter2"));
    let json = serde_json::to_string(&secret).unwrap();
    assert!(json.contains("[REDACTED]"));
    assert!(!json.contains("hunter2"));
    assert!(serde_json::from_str::<OptionValue>(&json).is_err());
}

#[test]
fn settings_bound_concurrency_and_require_a_grace_period() {
    let mut settings = PostProcessingSettings::default();
    assert!(
        !settings.execution_enabled,
        "execution stays off by default"
    );
    assert!(settings.validate().is_ok());
    settings.concurrency = 0;
    assert!(settings.validate().is_err());
    settings.concurrency = 9;
    assert!(settings.validate().is_err());
    settings.concurrency = 8;
    settings.termination_grace_seconds = 0;
    assert!(settings.validate().is_err());
}
