use super::manifest::parse_native_manifest;
use super::model::{
    ApprovedFilesystemRoot, ApprovedFilesystemRoots, ArtifactCondition, AttemptStatus,
    ExtensionDigest, ExtensionSelection, FrozenPlan, FrozenPlanProvenance, OnFailure, OptionName,
    OrderedStep, OutcomeImpact, PipelineOutcome, PostProcessingSettings, PostProcessingSummary,
    Profile, ProfileId, ResolvedOption, ResolvedOptionValue, RunStatus, RunWhen, SecretOptionValue,
    SubmissionPlanSelection, TimeoutPolicy, TrustState, VerifiedExtensionDigest,
};
use super::persistence::{LogStream, TerminalIntent, encode_control_effects};
use super::runner::ControlEffects;
use crate::history::JobHistoryRow;
use crate::jobs::{ActiveJob, JobId};
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlRuntime, StoreDatastore};

fn history_row(job_id: u64) -> JobHistoryRow {
    JobHistoryRow {
        job_id,
        job_hash: None,
        name: format!("history-{job_id}"),
        status: "complete".into(),
        error_message: None,
        total_bytes: 1,
        downloaded_bytes: 1,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at: 1,
        completed_at: 2,
        metadata: None,
    }
}

fn active_job(job_id: u64) -> ActiveJob {
    ActiveJob {
        job_id: JobId(job_id),
        nzb_hash: [0; 32],
        nzb_path: format!("/tmp/{job_id}.nzb").into(),
        nzb_zstd: vec![],
        output_dir: format!("/tmp/{job_id}").into(),
        created_at: 1,
        category: None,
        metadata: vec![],
        status: "queued",
        download_state: "queued",
        post_state: "idle",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
    }
}

fn manifest() -> super::model::ExtensionManifest {
    let digest = ExtensionDigest::new(format!("blake3:{}", "a".repeat(64))).unwrap();
    parse_native_manifest(
        r#"{
            "schema_version": 1,
            "kind": "native",
            "id": "example.processor",
            "name": "Example Processor",
            "version": "1.0.0",
            "entrypoint": "process.sh",
            "commands": [],
            "options": [{"name": "ApiKey", "type": "secret"}]
        }"#,
        VerifiedExtensionDigest::from_verified_package_digest(digest),
    )
    .unwrap()
}

fn option_manifest(seed: char, version: &str) -> super::model::ExtensionManifest {
    let digest = ExtensionDigest::new(format!("blake3:{}", seed.to_string().repeat(64))).unwrap();
    let json = serde_json::json!({
        "schema_version": 1,
        "kind": "native",
        "id": "example.options",
        "name": "Option Processor",
        "version": version,
        "entrypoint": "process.sh",
        "commands": [],
        "options": [
            {"name": "mode", "type": "string", "default": "safe", "select": ["safe", "fast"]},
            {"name": "token", "type": "string", "required": true}
        ]
    })
    .to_string();
    parse_native_manifest(
        &json,
        VerifiedExtensionDigest::from_verified_package_digest(digest),
    )
    .unwrap()
}

#[test]
fn revisions_profiles_and_frozen_plans_are_durable_and_secret_safe() {
    let db = Database::open_in_memory().unwrap();
    let manifest = manifest();
    db.upsert_discovered_extension(&manifest, Some("/data/scripts/example"), 10)
        .unwrap();

    let revision = manifest.revision();
    assert!(
        db.approve_extension_revision(
            revision.extension_id(),
            revision.revision_id(),
            "/data/managed/blake3/aaaa",
            20,
        )
        .unwrap()
    );
    let stored = db
        .extension_revision(revision.extension_id(), revision.revision_id())
        .unwrap()
        .unwrap();
    assert_eq!(stored.trust_state, TrustState::Approved);

    let profile_id = ProfileId::new("movies").unwrap();
    let secret_name = OptionName::new("ApiKey").unwrap();
    let profile = Profile::new(
        profile_id.clone(),
        "Movies".into(),
        vec![
            OrderedStep::new(
                0,
                ExtensionSelection::pinned(
                    revision.extension_id().clone(),
                    revision.revision_id().clone(),
                ),
                RunWhen::Always,
                OnFailure::Continue,
                OutcomeImpact::Warning,
                TimeoutPolicy::Default24Hours,
                ApprovedFilesystemRoots::new(vec![]),
                vec![ResolvedOption::new(
                    secret_name.clone(),
                    ResolvedOptionValue::Secret(SecretOptionValue::for_execution("top-secret")),
                )],
            )
            .unwrap()
            .with_artifact_condition(
                ArtifactCondition::new(vec![".mkv".into(), ".srt".into()], 2).unwrap(),
            ),
        ],
    )
    .unwrap();
    db.save_post_processing_profile(&profile, true, 30).unwrap();
    db.assign_category_post_processing_profile("movies", Some(&profile_id))
        .unwrap();

    let frozen = db
        .resolve_post_processing_plan(None, Some("movies"))
        .unwrap();
    assert!(matches!(
        frozen.provenance(),
        FrozenPlanProvenance::CategoryProfile { profile_id: id } if id == &profile_id
    ));
    assert_eq!(
        frozen.steps()[0].options()[0].value().clone(),
        ResolvedOptionValue::Secret(SecretOptionValue::for_execution("top-secret"))
    );
    assert_eq!(
        frozen.steps()[0].artifact_condition(),
        &ArtifactCondition::new(vec![".mkv".into(), ".srt".into()], 2).unwrap()
    );

    db.save_frozen_post_processing_plan(42, &frozen, 40)
        .unwrap();
    db.assign_category_post_processing_profile("movies", None)
        .unwrap();
    let reloaded = db.frozen_post_processing_plan(42).unwrap().unwrap();
    assert_eq!(reloaded, frozen);
    let persisted_key = db.encryption_key().unwrap().clone();
    assert!(db.has_encrypted_credentials().unwrap());
    db.validate_encrypted_credentials(&persisted_key).unwrap();
    assert!(
        db.validate_encrypted_credentials(
            &crate::persistence::encryption::EncryptionKey::generate()
        )
        .is_err()
    );

    let datastore = db.datastore();
    let secret_json = db
        .run_sql_blocking(async move { stored_secret_json(datastore).await })
        .unwrap();
    assert!(!secret_json.contains("top-secret"));
    assert!(secret_json.contains("enc:v1:"));
    assert!(!format!("{reloaded:?}").contains("top-secret"));
}

#[test]
fn option_resolution_rebinding_and_root_revocation_preserve_frozen_plan_contracts() {
    let db = Database::open_in_memory().unwrap();
    let original = option_manifest('c', "1.0.0");
    db.upsert_discovered_extension(&original, Some("/scripts/options-v1"), 10)
        .unwrap();
    db.approve_extension_revision(
        original.revision().extension_id(),
        original.revision().revision_id(),
        "/managed/options-v1",
        20,
    )
    .unwrap();
    db.save_post_processing_settings(&PostProcessingSettings {
        allowed_roots: vec!["/srv/approved".into()],
        ..PostProcessingSettings::default()
    })
    .unwrap();

    let selection = ExtensionSelection::pinned(
        original.revision().extension_id().clone(),
        original.revision().revision_id().clone(),
    );
    let approved_roots =
        ApprovedFilesystemRoots::new(vec![ApprovedFilesystemRoot::new("/srv/approved").unwrap()]);
    let profile_id = ProfileId::new("option-profile").unwrap();
    let profile = Profile::new(
        profile_id.clone(),
        "Options".into(),
        vec![
            OrderedStep::new(
                0,
                selection.clone(),
                RunWhen::PipelineSuccess,
                OnFailure::Stop,
                OutcomeImpact::FailJob,
                TimeoutPolicy::Unlimited,
                approved_roots.clone(),
                vec![
                    ResolvedOption::new(
                        OptionName::new("token").unwrap(),
                        ResolvedOptionValue::String("first".into()),
                    ),
                    ResolvedOption::new(
                        OptionName::new("mode").unwrap(),
                        ResolvedOptionValue::String("fast".into()),
                    ),
                ],
            )
            .unwrap()
            .with_artifact_condition(ArtifactCondition::new(vec![".mkv".into()], 1).unwrap()),
            OrderedStep::new(
                1,
                selection.clone(),
                RunWhen::PipelineFailure,
                OnFailure::Continue,
                OutcomeImpact::Warning,
                TimeoutPolicy::Default24Hours,
                approved_roots,
                vec![ResolvedOption::new(
                    OptionName::new("token").unwrap(),
                    ResolvedOptionValue::String("second".into()),
                )],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    db.save_post_processing_profile(&profile, true, 30).unwrap();
    db.assign_category_post_processing_profile("options", Some(&profile_id))
        .unwrap();
    let historical = db
        .resolve_post_processing_plan(None, Some("options"))
        .unwrap();
    assert_eq!(historical.steps().len(), 2);
    assert_eq!(historical.steps()[1].options()[0].name().as_str(), "mode");
    assert_eq!(
        historical.steps()[1].options()[0].value(),
        &ResolvedOptionValue::String("safe".into())
    );

    let latest = option_manifest('d', "2.0.0");
    db.upsert_discovered_extension(&latest, Some("/scripts/options-v2"), 35)
        .unwrap();
    db.approve_extension_revision(
        latest.revision().extension_id(),
        latest.revision().revision_id(),
        "/managed/options-v2",
        40,
    )
    .unwrap();
    let rebound = db.rebind_frozen_post_processing_plan(&historical).unwrap();
    assert_eq!(rebound.steps().len(), 2);
    assert!(
        rebound
            .steps()
            .iter()
            .all(|step| { step.revision().revision_id() == latest.revision().revision_id() })
    );
    assert_eq!(rebound.steps()[0].run_when(), RunWhen::PipelineSuccess);
    assert_eq!(rebound.steps()[0].on_failure(), OnFailure::Stop);
    assert_eq!(rebound.steps()[0].outcome_impact(), OutcomeImpact::FailJob);
    assert_eq!(
        rebound.steps()[0].timeout_policy(),
        TimeoutPolicy::Unlimited
    );
    assert_eq!(
        rebound.steps()[0].artifact_condition(),
        &ArtifactCondition::new(vec![".mkv".into()], 1).unwrap()
    );

    let explicit = SubmissionPlanSelection::extensions(vec![ExtensionSelection::latest_approved(
        latest.revision().extension_id().clone(),
    )])
    .unwrap();
    assert!(
        db.resolve_post_processing_plan(Some(&explicit), None)
            .is_err()
    );

    let invalid_profile_id = ProfileId::new("invalid-options").unwrap();
    let invalid_profile = Profile::new(
        invalid_profile_id.clone(),
        "Invalid options".into(),
        vec![
            OrderedStep::new(
                0,
                selection,
                RunWhen::Always,
                OnFailure::Continue,
                OutcomeImpact::Warning,
                TimeoutPolicy::Default24Hours,
                ApprovedFilesystemRoots::new(vec![]),
                vec![ResolvedOption::new(
                    OptionName::new("undeclared").unwrap(),
                    ResolvedOptionValue::String("value".into()),
                )],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    db.save_post_processing_profile(&invalid_profile, true, 50)
        .unwrap();
    db.assign_category_post_processing_profile("invalid", Some(&invalid_profile_id))
        .unwrap();
    assert!(
        db.resolve_post_processing_plan(None, Some("invalid"))
            .is_err()
    );

    let wrong_type_id = ProfileId::new("wrong-option-type").unwrap();
    let wrong_type = Profile::new(
        wrong_type_id.clone(),
        "Wrong option type".into(),
        vec![
            OrderedStep::new(
                0,
                ExtensionSelection::pinned(
                    original.revision().extension_id().clone(),
                    original.revision().revision_id().clone(),
                ),
                RunWhen::Always,
                OnFailure::Continue,
                OutcomeImpact::Warning,
                TimeoutPolicy::Default24Hours,
                ApprovedFilesystemRoots::new(vec![]),
                vec![
                    ResolvedOption::new(
                        OptionName::new("mode").unwrap(),
                        ResolvedOptionValue::Boolean(true),
                    ),
                    ResolvedOption::new(
                        OptionName::new("token").unwrap(),
                        ResolvedOptionValue::String("present".into()),
                    ),
                ],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    db.save_post_processing_profile(&wrong_type, true, 55)
        .unwrap();
    db.assign_category_post_processing_profile("wrong-type", Some(&wrong_type_id))
        .unwrap();
    assert!(
        db.resolve_post_processing_plan(None, Some("wrong-type"))
            .is_err()
    );

    db.save_post_processing_settings(&PostProcessingSettings::default())
        .unwrap();
    assert!(
        db.resolve_post_processing_plan(None, Some("options"))
            .is_err()
    );
}

async fn stored_secret_json(datastore: StoreDatastore) -> Result<String, crate::StateError> {
    let row = SqlRuntime::fetch_optional(
        datastore.read_exec(),
        "SELECT secret_options_json FROM post_processing_job_plans WHERE job_id = {}",
        &[crate::persistence::sql_runtime::SqlArg::I64(42)],
    )
    .await?
    .expect("stored plan");
    row.text("secret_options_json")
}

#[test]
fn omitted_selection_keeps_the_empty_plan_compatibility_contract() {
    let db = Database::open_in_memory().unwrap();
    assert_eq!(
        db.freeze_submission_post_processing_plan(None).unwrap(),
        None
    );
    assert_eq!(
        db.freeze_submission_post_processing_plan(Some(&SubmissionPlanSelection::inherit()))
            .unwrap(),
        None
    );
    let disabled = db
        .freeze_submission_post_processing_plan(Some(&SubmissionPlanSelection::disabled()))
        .unwrap()
        .unwrap();
    assert!(matches!(
        disabled.provenance(),
        FrozenPlanProvenance::Disabled
    ));
    let plan = db.resolve_post_processing_plan(None, None).unwrap();
    assert!(matches!(plan.provenance(), FrozenPlanProvenance::Empty));
    assert!(plan.steps().is_empty());
}

#[test]
fn finishing_attempt_persists_and_preserves_output_truncation() {
    let db = Database::open_in_memory().unwrap();
    let manifest = manifest();
    db.upsert_discovered_extension(&manifest, Some("/data/scripts/example"), 10)
        .unwrap();
    let revision = manifest.revision();
    db.approve_extension_revision(
        revision.extension_id(),
        revision.revision_id(),
        "/data/managed/blake3/aaaa",
        20,
    )
    .unwrap();
    let selection = SubmissionPlanSelection::extensions(vec![ExtensionSelection::pinned(
        revision.extension_id().clone(),
        revision.revision_id().clone(),
    )])
    .unwrap();
    let plan = db
        .resolve_post_processing_plan(Some(&selection), None)
        .unwrap();
    let create_running_attempt = |job_id, timestamp| {
        let run_id = db
            .create_post_processing_run(
                job_id,
                &plan,
                &PipelineOutcome::Succeeded,
                TerminalIntent::Complete,
                None,
                timestamp,
            )
            .unwrap();
        let attempt_id = db
            .enqueue_post_processing_attempt(
                &run_id,
                &plan.steps()[0],
                manifest.adapter(),
                None,
                timestamp + 1,
            )
            .unwrap();
        assert!(
            db.mark_post_processing_attempt_starting(
                &attempt_id,
                &serde_json::json!({"program": "process.sh"}),
                "/work/job",
                timestamp + 2,
            )
            .unwrap()
        );
        assert!(
            db.mark_post_processing_attempt_running(&attempt_id)
                .unwrap()
        );
        (run_id, attempt_id)
    };

    let (runner_run_id, runner_attempt_id) = create_running_attempt(101, 30);
    assert!(
        db.finish_post_processing_attempt(
            &runner_attempt_id,
            AttemptStatus::Succeeded,
            Some(0),
            None,
            None,
            true,
            40,
        )
        .unwrap()
    );
    assert!(db.post_processing_attempts(&runner_run_id).unwrap()[0].output_truncated);

    let (streamed_run_id, streamed_attempt_id) = create_running_attempt(102, 50);
    let line = vec![b'x'; super::persistence::MAX_LOGICAL_LINE_BYTES];
    for offset in 0..66 {
        db.append_post_processing_log(&streamed_attempt_id, LogStream::Stdout, &line, 60 + offset)
            .unwrap();
    }
    assert!(
        db.post_processing_logs(&streamed_attempt_id, None, 500)
            .unwrap()
            .truncated
    );
    assert!(
        db.finish_post_processing_attempt(
            &streamed_attempt_id,
            AttemptStatus::Succeeded,
            Some(0),
            None,
            None,
            false,
            130,
        )
        .unwrap()
    );
    assert!(db.post_processing_attempts(&streamed_run_id).unwrap()[0].output_truncated);
}

#[test]
fn attempts_are_durable_bounded_and_interrupted_without_implicit_rerun() {
    let db = Database::open_in_memory().unwrap();
    let manifest = manifest();
    db.upsert_discovered_extension(&manifest, Some("/data/scripts/example"), 10)
        .unwrap();
    let revision = manifest.revision();
    db.approve_extension_revision(
        revision.extension_id(),
        revision.revision_id(),
        "/data/managed/blake3/aaaa",
        20,
    )
    .unwrap();
    let selection = SubmissionPlanSelection::extensions(vec![ExtensionSelection::pinned(
        revision.extension_id().clone(),
        revision.revision_id().clone(),
    )])
    .unwrap();
    let plan = db
        .resolve_post_processing_plan(Some(&selection), None)
        .unwrap();
    let run_id = db
        .create_post_processing_run(
            7,
            &plan,
            &PipelineOutcome::Succeeded,
            TerminalIntent::Complete,
            None,
            30,
        )
        .unwrap();
    let attempt_id = db
        .enqueue_post_processing_attempt(
            &run_id,
            &plan.steps()[0],
            manifest.adapter(),
            Some(blake3::hash(b"token-one").as_bytes().to_vec()),
            31,
        )
        .unwrap();
    assert!(
        db.mark_post_processing_attempt_starting(
            &attempt_id,
            &serde_json::json!({"program": "process.sh"}),
            "/work/job",
            32,
        )
        .unwrap()
    );
    assert!(
        db.mark_post_processing_attempt_running(&attempt_id)
            .unwrap()
    );
    assert!(db.mark_post_processing_run_running(&run_id, 33).unwrap());
    assert_eq!(
        db.validate_post_processing_control_token(&attempt_id, "token-one")
            .unwrap(),
        Some(7)
    );
    assert_eq!(
        db.validate_post_processing_control_token(&attempt_id, "wrong-token")
            .unwrap(),
        None
    );

    let other_run_id = db
        .create_post_processing_run(
            8,
            &plan,
            &PipelineOutcome::Succeeded,
            TerminalIntent::Complete,
            None,
            34,
        )
        .unwrap();
    let other_attempt_id = db
        .enqueue_post_processing_attempt(
            &other_run_id,
            &plan.steps()[0],
            manifest.adapter(),
            Some(blake3::hash(b"token-two").as_bytes().to_vec()),
            35,
        )
        .unwrap();
    assert!(
        db.mark_post_processing_attempt_starting(
            &other_attempt_id,
            &serde_json::json!({"program": "process.sh"}),
            "/work/other-job",
            36,
        )
        .unwrap()
    );
    assert!(
        db.mark_post_processing_attempt_running(&other_attempt_id)
            .unwrap()
    );
    assert!(
        db.mark_post_processing_run_running(&other_run_id, 37)
            .unwrap()
    );
    assert_eq!(
        db.validate_post_processing_control_token(&other_attempt_id, "token-one")
            .unwrap(),
        None
    );
    assert_eq!(
        db.validate_post_processing_control_token(&other_attempt_id, "token-two")
            .unwrap(),
        Some(8)
    );
    let artifact_dir = tempfile::tempdir().unwrap();
    let artifact_path = artifact_dir.path().join("movie.mkv");
    std::fs::write(&artifact_path, b"artifact").unwrap();
    let expected_effects = ControlEffects {
        directory: Some(std::path::PathBuf::from("/work/renamed")),
        repair_requested: true,
        progress: Some(serde_json::json!({"percent": 75})),
        artifacts: vec![artifact_path.clone()],
        ..ControlEffects::default()
    };
    assert!(
        db.finish_post_processing_attempt(
            &other_attempt_id,
            AttemptStatus::Succeeded,
            Some(0),
            None,
            Some(encode_control_effects(&expected_effects).unwrap()),
            false,
            38,
        )
        .unwrap()
    );
    assert!(
        db.finish_post_processing_run(
            &other_run_id,
            RunStatus::Succeeded,
            PostProcessingSummary::Succeeded,
            39,
        )
        .unwrap()
    );
    assert_eq!(
        db.validate_post_processing_control_token(&other_attempt_id, "token-two")
            .unwrap(),
        None
    );
    let stored_effects = db.post_processing_attempts(&other_run_id).unwrap();
    assert_eq!(stored_effects[0].control_effects(), expected_effects);
    assert_eq!(
        stored_effects[0].reported_progress(),
        Some(serde_json::json!({"percent": 75}))
    );
    let artifacts = db.post_processing_artifacts(&other_run_id).unwrap();
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].path, artifact_path);
    assert!(artifacts[0].exists);
    assert!(artifacts[0].is_file);
    assert!(!artifacts[0].is_symlink);
    assert_eq!(artifacts[0].size_bytes, Some(8));

    let line = vec![b'x'; super::persistence::MAX_LOGICAL_LINE_BYTES];
    for offset in 0..66 {
        db.append_post_processing_log(&attempt_id, LogStream::Stdout, &line, 40 + offset)
            .unwrap();
    }
    let logs = db.post_processing_logs(&attempt_id, None, 500).unwrap();
    assert!(logs.truncated);
    assert_eq!(logs.chunks.first().unwrap().sequence, 0);
    assert!(logs.chunks.len() < 66);

    assert_eq!(db.recover_interrupted_post_processing(200).unwrap(), 1);
    assert_eq!(
        db.validate_post_processing_control_token(&attempt_id, "token-one")
            .unwrap(),
        None
    );
    let run = db.post_processing_run(&run_id).unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Interrupted);
    assert_eq!(run.summary, PostProcessingSummary::Interrupted);
    let attempts = db.post_processing_attempts(&run_id).unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].status, AttemptStatus::Interrupted);
    let metrics = db.post_processing_metrics_snapshot().unwrap();
    assert_eq!(metrics.queue_depth, 0);
    assert_eq!(metrics.active_attempts, 0);
    assert_eq!(metrics.duration_count, 2);
    assert_eq!(metrics.duration_sum_millis, 170);
    assert_eq!(metrics.interrupted, 1);
    assert_eq!(metrics.truncated, 1);
    assert!(
        !db.mark_post_processing_attempt_running(&attempt_id)
            .unwrap()
    );
}

#[test]
fn startup_interrupts_only_orphaned_queued_post_processing_runs() {
    let db = Database::open_in_memory().unwrap();
    let plan = FrozenPlan::new(FrozenPlanProvenance::Disabled, vec![]).unwrap();
    db.create_active_job(&active_job(50)).unwrap();
    let active_run = db
        .create_post_processing_run(
            50,
            &plan,
            &PipelineOutcome::Succeeded,
            TerminalIntent::Complete,
            None,
            10,
        )
        .unwrap();
    db.insert_job_history(&history_row(51)).unwrap();
    let orphaned_run = db
        .create_post_processing_run(
            51,
            &plan,
            &PipelineOutcome::Succeeded,
            TerminalIntent::Complete,
            None,
            11,
        )
        .unwrap();

    assert_eq!(db.recover_interrupted_post_processing(20).unwrap(), 0);
    assert_eq!(
        db.post_processing_run(&active_run).unwrap().unwrap().status,
        RunStatus::Queued
    );
    assert_eq!(
        db.post_processing_run(&orphaned_run)
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Interrupted
    );
}

#[test]
fn history_bundle_deletion_conflicts_with_queued_reruns_then_cleans_dependents() {
    let db = Database::open_in_memory().unwrap();
    let plan = FrozenPlan::new(FrozenPlanProvenance::Disabled, vec![]).unwrap();
    db.insert_job_history(&history_row(61)).unwrap();
    let source_run = db
        .create_post_processing_run(
            61,
            &plan,
            &PipelineOutcome::Succeeded,
            TerminalIntent::Complete,
            None,
            10,
        )
        .unwrap();
    db.finish_post_processing_run(
        &source_run,
        RunStatus::Succeeded,
        PostProcessingSummary::Succeeded,
        11,
    )
    .unwrap();
    db.save_frozen_post_processing_plan(61, &plan, 12).unwrap();
    let rerun = db
        .create_history_post_processing_rerun(
            61,
            &plan,
            &PipelineOutcome::Succeeded,
            TerminalIntent::Complete,
            &source_run,
            13,
        )
        .unwrap();
    let external_rerun = db
        .create_post_processing_run(
            62,
            &plan,
            &PipelineOutcome::Succeeded,
            TerminalIntent::Complete,
            Some(&source_run),
            14,
        )
        .unwrap();

    assert!(matches!(
        db.delete_job_history(61),
        Err(crate::StateError::Conflict(_))
    ));
    db.finish_post_processing_run(
        &rerun,
        RunStatus::Cancelled,
        PostProcessingSummary::Cancelled,
        15,
    )
    .unwrap();

    assert!(db.delete_job_history(61).unwrap());
    assert!(db.get_job_history(61).unwrap().is_none());
    assert!(db.frozen_post_processing_plan(61).unwrap().is_none());
    assert!(db.post_processing_run(&source_run).unwrap().is_none());
    assert!(db.post_processing_run(&rerun).unwrap().is_none());
    assert!(
        db.post_processing_run(&external_rerun)
            .unwrap()
            .unwrap()
            .rerun_of_run_id
            .is_none()
    );
}
