use super::model::{
    DEFAULT_UNACCEPTABLE_EXTENSIONS, OptionName, OptionValue, PostProcessingSettings,
    PostProcessingSummary, ResolvedOption, ScriptAdapter, ScriptManifest, ScriptName, ScriptOption,
    ScriptOptionType, ScriptStatus, SecretOptionValue, merge_post_processing_summary,
};

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

#[test]
fn unacceptable_extension_patterns_are_normalized_and_match_final_extensions() {
    let settings = PostProcessingSettings {
        unacceptable_extensions: vec![
            " R?? ".into(),
            "ZIP*".into(),
            "EXE".into(),
            "exe".into(),
            "?x".into(),
        ],
        ..PostProcessingSettings::default()
    }
    .normalized()
    .unwrap();

    assert_eq!(
        settings.unacceptable_extensions,
        ["?x", "exe", "r??", "zip*"]
    );
    assert_eq!(
        settings.unacceptable_extension_match("payload.ExE"),
        Some("exe")
    );
    assert_eq!(
        settings.unacceptable_extension_match("archive.R42"),
        Some("r??")
    );
    assert_eq!(
        settings.unacceptable_extension_match("archive.Zip64"),
        Some("zip*")
    );
    assert_eq!(settings.unacceptable_extension_match("archive.r007"), None);
    assert_eq!(settings.unacceptable_extension_match("no_extension"), None);
    assert_eq!(settings.unacceptable_extension_match(".hidden"), None);
    assert_eq!(
        settings.unacceptable_extension_match("folder/.hidden"),
        None
    );
    assert_eq!(
        settings.unacceptable_extension_match("payload.éx"),
        Some("?x")
    );
}

#[test]
fn unacceptable_extension_patterns_reject_paths_dots_and_regex_syntax() {
    for pattern in [".exe", "*.exe", "re:exe", "folder/exe", "exe\\payload", ""] {
        let settings = PostProcessingSettings {
            unacceptable_extensions: vec![pattern.into()],
            ..PostProcessingSettings::default()
        };
        assert!(settings.normalized().is_err(), "accepted {pattern:?}");
    }
}

#[test]
fn the_rename_executable_floor_matches_the_default_unacceptable_extensions() {
    let mut floor: Vec<&str> = weaver_nzb::delivery_rename::EXECUTABLE_EXTENSIONS.to_vec();
    floor.sort_unstable();
    let mut defaults: Vec<&str> = DEFAULT_UNACCEPTABLE_EXTENSIONS.to_vec();
    defaults.sort_unstable();
    assert_eq!(floor, defaults);
}

#[test]
fn a_result_says_it_was_not_waited_for_only_when_that_is_so() {
    let stored = r#"{"script":"notify.sh","adapter":"sabnzbd","status":"succeeded","exitCode":0,"durationMs":1,"finishedAtEpochMs":1}"#;
    let result: super::model::ScriptResult = serde_json::from_str(stored).unwrap();
    assert!(!result.background);
    assert!(
        !serde_json::to_string(&result)
            .unwrap()
            .contains("background")
    );
    let detached = super::model::ScriptResult {
        background: true,
        ..result
    };
    let stored = serde_json::to_string(&detached).unwrap();
    assert!(
        serde_json::from_str::<super::model::ScriptResult>(&stored)
            .unwrap()
            .background
    );
}

#[test]
fn a_result_names_its_instance_only_when_it_ran_as_one() {
    let stored = r#"{"script":"notify.sh","adapter":"sabnzbd","status":"succeeded","exitCode":0,"durationMs":1,"finishedAtEpochMs":1}"#;
    let result: super::model::ScriptResult = serde_json::from_str(stored).unwrap();
    assert_eq!(result.instance_id, None);
    assert_eq!(result.instance_name, None);
    assert!(!serde_json::to_string(&result).unwrap().contains("instance"));
    let named = super::model::ScriptResult {
        instance_id: Some("one".into()),
        instance_name: Some("Notify the family".into()),
        ..result
    };
    let read_back: super::model::ScriptResult =
        serde_json::from_str(&serde_json::to_string(&named).unwrap()).unwrap();
    assert_eq!(read_back, named);
}
