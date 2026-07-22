use std::collections::HashSet;

use super::model::*;

fn verified_digest(seed: char) -> VerifiedExtensionDigest {
    VerifiedExtensionDigest::from_verified_package_digest(
        ExtensionDigest::new(format!("sha256:{}", seed.to_string().repeat(64))).unwrap(),
    )
}

fn revision(seed: char) -> ExtensionRevision {
    ExtensionRevision::from_verified(
        ExtensionId::new("example.extension").unwrap(),
        DeclaredExtensionVersion::new("1.0.0").unwrap(),
        verified_digest(seed),
    )
}

fn roots() -> ApprovedFilesystemRoots {
    ApprovedFilesystemRoots::new(vec![
        ApprovedFilesystemRoot::new("/srv/weaver/output").unwrap(),
    ])
}

fn selection(seed: char) -> ExtensionSelection {
    ExtensionSelection::pinned(
        ExtensionId::new("example.extension").unwrap(),
        revision(seed).revision_id().clone(),
    )
}

fn ordered_step(
    index: u32,
    timeout_policy: TimeoutPolicy,
    options: Vec<ResolvedOption>,
) -> OrderedStep {
    OrderedStep::new(
        index,
        selection('a'),
        RunWhen::Always,
        OnFailure::Continue,
        OutcomeImpact::Warning,
        timeout_policy,
        roots(),
        options,
    )
    .unwrap()
}

#[test]
fn nzbget_identity_preserves_compatibility_name_without_collisions() {
    let names = [
        "My Script",
        "My-Script",
        "My_Script",
        "my script",
        "My.Script!",
        "Mý Script",
    ];
    let ids = names
        .into_iter()
        .map(|name| NzbgetCompatibilityName::new(name).unwrap())
        .map(|name| {
            assert!(!name.as_str().is_empty());
            name.weaver_extension_id().unwrap()
        })
        .collect::<HashSet<_>>();
    assert_eq!(ids.len(), names.len());
}

#[test]
fn revisions_are_digest_bound_even_when_manifest_versions_match() {
    let first = revision('a');
    let second = revision('b');
    assert_eq!(first.declared_version().as_str(), "1.0.0");
    assert_eq!(second.declared_version().as_str(), "1.0.0");
    assert_ne!(first.revision_id(), second.revision_id());
    assert_ne!(
        ExtensionSelection::pinned(first.extension_id().clone(), first.revision_id().clone())
            .revision_id(),
        ExtensionSelection::pinned(second.extension_id().clone(), second.revision_id().clone())
            .revision_id()
    );

    let mut wire = serde_json::to_value(&first).unwrap();
    wire["revision_id"] = serde_json::Value::String("sha256-deadbeef".to_string());
    assert!(serde_json::from_value::<ExtensionRevision>(wire).is_err());
}

#[test]
fn generic_secret_serde_fails_closed_for_resolved_options_profiles_and_plans() {
    let secret = ResolvedOption::new(
        OptionName::new("api_key").unwrap(),
        ResolvedOptionValue::Secret(SecretOptionValue::for_execution("do-not-log")),
    );
    let profile = Profile::new(
        ProfileId::new("default-profile").unwrap(),
        "Default".to_string(),
        vec![ordered_step(
            0,
            TimeoutPolicy::Default24Hours,
            vec![secret.clone()],
        )],
    )
    .unwrap();
    let profile_json = serde_json::to_string(&profile).unwrap();
    assert!(!format!("{profile:?}").contains("do-not-log"));
    assert!(!profile_json.contains("do-not-log"));
    assert!(serde_json::from_str::<Profile>(&profile_json).is_err());

    let frozen = FrozenPlan::new(
        FrozenPlanProvenance::Explicit,
        vec![
            FrozenPlanStep::new(
                0,
                revision('a'),
                RunWhen::Always,
                OnFailure::Continue,
                OutcomeImpact::Warning,
                TimeoutPolicy::Default24Hours,
                roots(),
                vec![secret],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let frozen_json = serde_json::to_string(&frozen).unwrap();
    assert!(!frozen_json.contains("do-not-log"));
    assert!(serde_json::from_str::<FrozenPlan>(&frozen_json).is_err());
    assert!(
        serde_json::from_str::<ResolvedOptionValue>(r#"{"type":"secret","value":"[REDACTED]"}"#)
            .is_err()
    );
}

#[test]
fn submission_and_revision_selection_stay_separate_and_validated() {
    let selected = selection('a');
    let submission = SubmissionPlanSelection::extensions(vec![selected.clone()]).unwrap();
    assert_eq!(submission.mode(), SubmissionPlanSelectionMode::Extensions);
    assert_eq!(submission.selected_extensions().unwrap(), [selected]);
    assert_eq!(
        SubmissionPlanSelection::inherit().mode(),
        SubmissionPlanSelectionMode::Inherit
    );
    assert_eq!(
        SubmissionPlanSelection::disabled().mode(),
        SubmissionPlanSelectionMode::Disabled
    );
    assert_eq!(
        SubmissionPlanSelection::profile(ProfileId::new("default-profile").unwrap()).mode(),
        SubmissionPlanSelectionMode::Profile
    );
    assert!(SubmissionPlanSelection::extensions(vec![]).is_err());
    assert!(serde_json::from_str::<ExtensionSelection>(
        r#"{"extension_id":"example.extension","mode":"latest_approved","revision_id":"sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#
    )
    .is_err());
}

#[test]
fn profiles_freeze_roots_timeouts_and_canonical_ordering() {
    let finite = TimeoutPolicy::Finite(NonZeroTimeoutSeconds::new(90).unwrap());
    let profile = Profile::new(
        ProfileId::new("default-profile").unwrap(),
        "Default".to_string(),
        vec![
            ordered_step(1, finite, vec![]),
            ordered_step(0, finite, vec![]),
        ],
    )
    .unwrap();
    assert_eq!(profile.steps()[0].index(), 0);
    assert_eq!(profile.steps()[0].approved_roots().as_slice().len(), 1);
    assert_eq!(profile.steps()[0].timeout_policy(), finite);
    assert!(NonZeroTimeoutSeconds::new(0).is_err());
    assert!(serde_json::from_str::<NonZeroTimeoutSeconds>("0").is_err());
    assert!(
        Profile::new(
            ProfileId::new("default-profile").unwrap(),
            "Default".to_string(),
            vec![ordered_step(2, TimeoutPolicy::Unlimited, vec![])]
        )
        .is_err()
    );
    assert!(
        OrderedStep::new(
            0,
            selection('a'),
            RunWhen::Always,
            OnFailure::Continue,
            OutcomeImpact::Warning,
            TimeoutPolicy::Default24Hours,
            roots(),
            vec![
                ResolvedOption::new(
                    OptionName::new("Api.Key").unwrap(),
                    ResolvedOptionValue::String("a".into())
                ),
                ResolvedOption::new(
                    OptionName::new("api.key").unwrap(),
                    ResolvedOptionValue::String("b".into())
                ),
            ],
        )
        .is_err()
    );
}

#[test]
fn approved_roots_reject_portable_unsafe_components() {
    for root in [
        r"C:\CON",
        r"C:\CON .txt",
        r"C:\safe\trailing. ",
        r"C:\safe\file.txt:alternate-stream",
        r"\\?\C:\safe",
        r"\\?\UNC\server\share",
        r"\\.\PhysicalDrive0",
        r"\??\C:\safe",
        "/srv/../weaver",
        "/srv/control\ncharacter",
    ] {
        assert!(ApprovedFilesystemRoot::new(root).is_err());
    }
    assert!(ApprovedFilesystemRoot::new("/").is_ok());
    assert!(ApprovedFilesystemRoot::new(r"C:\").is_ok());
    assert!(ApprovedFilesystemRoot::new(r"\\server\share").is_ok());
}

#[test]
fn settings_validate_allowed_roots_and_duplicates() {
    let mut settings = PostProcessingSettings {
        allowed_roots: vec!["/srv/weaver".into()],
        ..PostProcessingSettings::default()
    };
    assert!(settings.validate().is_ok());

    settings.allowed_roots.push("/srv/weaver".into());
    assert!(settings.validate().is_err());
    settings.allowed_roots = vec![r"\\?\C:\unsafe".into()];
    assert!(settings.validate().is_err());
}

#[test]
fn frozen_plans_preserve_provenance_and_policy() {
    let frozen_step = FrozenPlanStep::new(
        0,
        revision('a'),
        RunWhen::PipelineSuccess,
        OnFailure::Stop,
        OutcomeImpact::FailJob,
        TimeoutPolicy::Unlimited,
        roots(),
        vec![],
    )
    .unwrap();
    let plan = FrozenPlan::new(
        FrozenPlanProvenance::CategoryProfile {
            profile_id: ProfileId::new("default-profile").unwrap(),
        },
        vec![frozen_step],
    )
    .unwrap();
    assert_eq!(plan.steps()[0].approved_roots().as_slice().len(), 1);
    assert_eq!(plan.steps()[0].timeout_policy(), TimeoutPolicy::Unlimited);
    assert!(FrozenPlan::new(FrozenPlanProvenance::Disabled, vec![]).is_ok());
    assert!(FrozenPlan::new(FrozenPlanProvenance::Empty, vec![]).is_ok());
    assert!(FrozenPlan::new(FrozenPlanProvenance::Disabled, plan.steps().to_vec()).is_err());
}

#[test]
fn pipeline_and_post_processing_outcomes_remain_separate() {
    let pipeline = PipelineOutcome::Failed {
        stage: PipelineFailureStage::Repair,
        code: "par2_unrecoverable".to_string(),
        message: "repair data is insufficient".to_string(),
    };
    assert!(matches!(
        pipeline,
        PipelineOutcome::Failed {
            stage: PipelineFailureStage::Repair,
            ..
        }
    ));
    assert_eq!(PostProcessingSummary::NotRun, PostProcessingSummary::NotRun);
    assert_eq!(
        PostProcessingSummary::Interrupted,
        PostProcessingSummary::Interrupted
    );
}

#[test]
fn artifact_conditions_are_safe_content_prerequisites_and_remain_backward_compatible() {
    let condition = ArtifactCondition::new(vec![".mkv".into(), ".srt".into()], 2).unwrap();
    let matching = [
        std::path::Path::new("/output/movie.mkv"),
        std::path::Path::new("/output/movie.srt"),
    ];
    assert!(condition.matches(matching));
    assert!(!condition.matches([std::path::Path::new("/output/movie.mkv")]));
    assert!(ArtifactCondition::new(vec!["../movie.mkv".into()], 0).is_err());

    let step = ordered_step(0, TimeoutPolicy::Default24Hours, vec![])
        .with_artifact_condition(condition.clone());
    let round_trip: OrderedStep =
        serde_json::from_value(serde_json::to_value(&step).unwrap()).unwrap();
    assert_eq!(round_trip.artifact_condition(), &condition);

    let mut legacy = serde_json::to_value(&step).unwrap();
    legacy.as_object_mut().unwrap().remove("artifact_condition");
    let legacy: OrderedStep = serde_json::from_value(legacy).unwrap();
    assert_eq!(legacy.artifact_condition(), &ArtifactCondition::default());
}
