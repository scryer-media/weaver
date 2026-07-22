use std::path::PathBuf;
use std::time::Duration;

use super::manifest::parse_native_manifest;
use super::model::{
    ApprovedFilesystemRoots, ArtifactCondition, ExtensionDigest, ExtensionSelection, FrozenPlan,
    FrozenPlanProvenance, FrozenPlanStep, OnFailure, OutcomeImpact, PipelineOutcome,
    PostProcessingSummary, RunStatus, RunWhen, SubmissionPlanSelection, TimeoutPolicy,
    VerifiedExtensionDigest,
};
use super::persistence::TerminalIntent;
use super::runner::{InterpreterConfig, JobExecutionContext};
use super::service::{PostProcessingService, merge_post_processing_summary};
use crate::persistence::Database;

fn manifest() -> super::model::ExtensionManifest {
    let digest = ExtensionDigest::new(format!("blake3:{}", "b".repeat(64))).unwrap();
    parse_native_manifest(
        r#"{
            "schema_version": 1,
            "kind": "native",
            "id": "example.cancel",
            "name": "Cancellation Example",
            "version": "1.0.0",
            "entrypoint": "process.sh",
            "commands": [],
            "options": []
        }"#,
        VerifiedExtensionDigest::from_verified_package_digest(digest),
    )
    .unwrap()
}

#[test]
fn post_processing_summary_never_downgrades_a_failure() {
    assert_eq!(
        merge_post_processing_summary(
            PostProcessingSummary::Failed,
            PostProcessingSummary::Warning,
        ),
        PostProcessingSummary::Failed
    );
    assert_eq!(
        merge_post_processing_summary(
            PostProcessingSummary::Warning,
            PostProcessingSummary::Failed,
        ),
        PostProcessingSummary::Failed
    );
}

#[tokio::test]
async fn paused_queued_run_can_be_cancelled_before_spawn() {
    let db = Database::open_in_memory().unwrap();
    let manifest = manifest();
    db.upsert_discovered_extension(&manifest, Some("/scripts/example"), 10)
        .unwrap();
    let revision = manifest.revision();
    db.approve_extension_revision(
        revision.extension_id(),
        revision.revision_id(),
        "/managed/example",
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
            91,
            &plan,
            &PipelineOutcome::Succeeded,
            TerminalIntent::Complete,
            None,
            30,
        )
        .unwrap();
    let service = PostProcessingService::new(db.clone(), 1);
    service.pause();
    let task_service = service.clone();
    let task_run_id = run_id.clone();
    let task = tokio::spawn(async move {
        task_service
            .execute_existing(
                &task_run_id,
                JobExecutionContext {
                    job_id: 91,
                    name: "cancel-me".into(),
                    nzb_filename: "cancel-me.nzb".into(),
                    category: None,
                    group: None,
                    source_url: None,
                    working_directory: PathBuf::from("/work/cancel-me"),
                    final_directory: PathBuf::from("/complete/cancel-me"),
                    pipeline_outcome: PipelineOutcome::Succeeded,
                    par_status: 2,
                    unpack_status: 2,
                },
                InterpreterConfig::default(),
                None,
            )
            .await
    });

    for _ in 0..50 {
        if service.cancel_job(91) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(service.cancel_job(91));
    let report = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("queued cancellation should not wait for resume")
        .unwrap()
        .unwrap();
    assert_eq!(report.summary, PostProcessingSummary::Cancelled);
    let run = db.post_processing_run(&run_id).unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Cancelled);
    assert_eq!(run.summary, PostProcessingSummary::Cancelled);
    assert!(db.post_processing_attempts(&run_id).unwrap().is_empty());
    assert!(!service.cancel_job(91));
}

#[tokio::test]
async fn unmet_artifact_condition_skips_without_spawning_an_attempt() {
    let db = Database::open_in_memory().unwrap();
    let plan = FrozenPlan::new(
        FrozenPlanProvenance::Explicit,
        vec![
            FrozenPlanStep::new(
                0,
                manifest().revision().clone(),
                RunWhen::Always,
                OnFailure::Continue,
                OutcomeImpact::Warning,
                TimeoutPolicy::Default24Hours,
                ApprovedFilesystemRoots::new(vec![]),
                vec![],
            )
            .unwrap()
            .with_artifact_condition(ArtifactCondition::new(vec![".mkv".into()], 1).unwrap()),
        ],
    )
    .unwrap();
    let run_id = db
        .create_post_processing_run(
            92,
            &plan,
            &PipelineOutcome::Succeeded,
            TerminalIntent::Complete,
            None,
            30,
        )
        .unwrap();
    let working_directory = tempfile::tempdir().unwrap();
    let service = PostProcessingService::new(db.clone(), 1);
    let report = service
        .execute_existing(
            &run_id,
            JobExecutionContext {
                job_id: 92,
                name: "condition-skip".into(),
                nzb_filename: "condition-skip.nzb".into(),
                category: None,
                group: None,
                source_url: None,
                working_directory: working_directory.path().to_path_buf(),
                final_directory: working_directory.path().join("complete"),
                pipeline_outcome: PipelineOutcome::Succeeded,
                par_status: 2,
                unpack_status: 2,
            },
            InterpreterConfig::default(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(report.summary, PostProcessingSummary::NotRun);
    assert!(db.post_processing_attempts(&run_id).unwrap().is_empty());
    let run = db.post_processing_run(&run_id).unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Skipped);
}

#[tokio::test]
async fn paused_queue_reordering_controls_the_pending_run_order() {
    let db = Database::open_in_memory().unwrap();
    let manifest = manifest();
    db.upsert_discovered_extension(&manifest, Some("/scripts/example"), 10)
        .unwrap();
    let revision = manifest.revision();
    db.approve_extension_revision(
        revision.extension_id(),
        revision.revision_id(),
        "/managed/example",
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
    let first_run = db
        .create_post_processing_run(
            101,
            &plan,
            &PipelineOutcome::Succeeded,
            TerminalIntent::Complete,
            None,
            30,
        )
        .unwrap();
    let second_run = db
        .create_post_processing_run(
            102,
            &plan,
            &PipelineOutcome::Succeeded,
            TerminalIntent::Complete,
            None,
            31,
        )
        .unwrap();
    let db_for_assert = db.clone();
    let service = PostProcessingService::new(db, 1);
    service.pause();

    let first_service = service.clone();
    let first_task_run = first_run.clone();
    let first_task = tokio::spawn(async move {
        first_service
            .execute_existing(
                &first_task_run,
                JobExecutionContext {
                    job_id: 101,
                    name: "first".into(),
                    nzb_filename: "first.nzb".into(),
                    category: None,
                    group: None,
                    source_url: None,
                    working_directory: PathBuf::from("/work/first"),
                    final_directory: PathBuf::from("/complete/first"),
                    pipeline_outcome: PipelineOutcome::Succeeded,
                    par_status: 2,
                    unpack_status: 2,
                },
                InterpreterConfig::default(),
                None,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while service.queued_run_ids().len() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let second_service = service.clone();
    let second_task_run = second_run.clone();
    let second_task = tokio::spawn(async move {
        second_service
            .execute_existing(
                &second_task_run,
                JobExecutionContext {
                    job_id: 102,
                    name: "second".into(),
                    nzb_filename: "second.nzb".into(),
                    category: None,
                    group: None,
                    source_url: None,
                    working_directory: PathBuf::from("/work/second"),
                    final_directory: PathBuf::from("/complete/second"),
                    pipeline_outcome: PipelineOutcome::Succeeded,
                    par_status: 2,
                    unpack_status: 2,
                },
                InterpreterConfig::default(),
                None,
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while service.queued_run_ids().len() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let reordered = vec![
        second_run.as_str().to_string(),
        first_run.as_str().to_string(),
    ];
    service.reorder_queued_runs(&reordered).unwrap();
    assert_eq!(service.queued_run_ids(), reordered);
    assert_eq!(
        db_for_assert
            .post_processing_run(&second_run)
            .unwrap()
            .unwrap()
            .queue_position,
        0
    );
    assert_eq!(
        db_for_assert
            .post_processing_run(&first_run)
            .unwrap()
            .unwrap()
            .queue_position,
        1
    );
    assert!(
        service
            .reorder_queued_runs(&[first_run.as_str().to_string()])
            .is_err()
    );

    assert!(service.cancel_job(101));
    assert!(service.cancel_job(102));
    first_task.await.unwrap().unwrap();
    second_task.await.unwrap().unwrap();
    assert!(service.queued_run_ids().is_empty());
}
