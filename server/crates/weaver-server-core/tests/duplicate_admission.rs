use std::path::PathBuf;
use std::sync::{Arc, Barrier};

use weaver_model::files::FileRole;
use weaver_server_core::ingest::{
    compress_nzb_bytes, reconcile_duplicate_fingerprint_backfill,
    run_duplicate_fingerprint_backfill_batch,
};
use weaver_server_core::{
    ActiveJob, CallerScopedIdempotency, Database, DuplicateAction, DuplicateAdmission,
    DuplicateAdmissionRequest, DuplicateBackfillState, DuplicateMode, DuplicatePolicy, FileSpec,
    FingerprintEvidence, FingerprintKind, JobHistoryRow, JobId, JobSpec, SegmentSpec,
    SemanticCandidateSource, SemanticCandidateState, SemanticDuplicate,
    SemanticDuplicateLifecycleEvent, SemanticPromotionState, SemanticTerminalCause,
    SubmissionOrigin, classify_semantic_terminal_cause, normalize_semantic_duplicate_key,
    record_semantic_duplicate_lifecycle_metric, semantic_duplicate_lifecycle_metrics_snapshot,
};

fn spec(files: Vec<Vec<(&str, u32)>>) -> JobSpec {
    JobSpec {
        name: "The.Release-Name".to_string(),
        password: None,
        files: files
            .into_iter()
            .enumerate()
            .map(|(file_index, segments)| FileSpec {
                filename: format!("payload-{file_index}.rar"),
                role: FileRole::Unknown,
                groups: vec![],
                posted_at_epoch: None,
                segments: segments
                    .into_iter()
                    .enumerate()
                    .map(|(ordinal, (message_id, bytes))| SegmentSpec {
                        ordinal: ordinal as u32,
                        article_number: ordinal as u32 + 1,
                        bytes,
                        message_id: message_id.to_string(),
                    })
                    .collect(),
            })
            .collect(),
        total_bytes: 0,
        category: None,
        metadata: vec![],
    }
}

fn request(spec: &JobSpec, raw_hash_byte: u8) -> DuplicateAdmissionRequest {
    DuplicateAdmissionRequest {
        evidence: FingerprintEvidence::from_validated_spec(spec, [raw_hash_byte; 32]),
        mode: DuplicateMode::Enforce,
        semantic: None,
        semantic_source: None,
        origin: SubmissionOrigin::Api,
        idempotency: None,
        policy: DuplicatePolicy::default(),
    }
}

fn score_request(
    spec: &JobSpec,
    raw_hash_byte: u8,
    key: &str,
    score: i64,
) -> DuplicateAdmissionRequest {
    let mut request = request(spec, raw_hash_byte);
    request.mode = DuplicateMode::Score;
    request.semantic = SemanticDuplicate::from_source(key, score);
    request.semantic_source = Some(SemanticCandidateSource {
        nzb_zstd: vec![raw_hash_byte],
        filename: Some(format!("candidate-{raw_hash_byte}.nzb")),
        password: None,
        category: None,
        metadata: vec![("origin".to_string(), "test".to_string())],
    });
    request
}

fn accepted(admission: DuplicateAdmission) -> (weaver_server_core::JobId, DuplicateAction) {
    match admission {
        DuplicateAdmission::Accepted {
            job_id, decision, ..
        } => (job_id, decision.action),
        other => panic!("expected accepted admission, got {other:?}"),
    }
}

fn history(job_id: u64, status: &str) -> JobHistoryRow {
    JobHistoryRow {
        job_id,
        job_hash: None,
        name: "The.Release-Name".to_string(),
        status: status.to_string(),
        error_message: None,
        total_bytes: 0,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1_000,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at: 1,
        completed_at: 2,
        metadata: None,
    }
}

fn active_backfill_job(job_id: u64, nzb_zstd: Vec<u8>) -> ActiveJob {
    ActiveJob {
        job_id: JobId(job_id),
        nzb_hash: [job_id as u8; 32],
        nzb_path: PathBuf::from(format!("job-{job_id}.nzb")),
        nzb_zstd,
        output_dir: PathBuf::from(format!("output-{job_id}")),
        created_at: job_id,
        category: None,
        metadata: Vec::new(),
        status: "queued",
        download_state: "queued",
        post_state: "idle",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
    }
}

#[test]
fn sqlite_admission_enforces_default_status_matrix() {
    let db = Database::open_in_memory().unwrap();
    let original = spec(vec![vec![("one@example", 10), ("two@example", 20)]]);
    let (first, action) = accepted(
        db.admit_duplicate_submission(&request(&original, 1))
            .unwrap(),
    );
    assert_eq!(action, DuplicateAction::Accept);
    db.activate_duplicate_admission(first).unwrap();

    let blocked = db
        .admit_duplicate_submission(&request(&original, 2))
        .unwrap();
    assert!(matches!(blocked, DuplicateAdmission::Blocked { .. }));

    let bytes_changed = spec(vec![vec![("one@example", 11), ("two@example", 20)]]);
    let (bytes_changed_job, action) = accepted(
        db.admit_duplicate_submission(&request(&bytes_changed, 3))
            .unwrap(),
    );
    assert_eq!(action, DuplicateAction::Pause);
    db.insert_job_history(&history(bytes_changed_job.0, "failed"))
        .unwrap();

    let articles_reordered = spec(vec![vec![("two@example", 20), ("one@example", 10)]]);
    let (_, action) = accepted(
        db.admit_duplicate_submission(&request(&articles_reordered, 4))
            .unwrap(),
    );
    assert_eq!(action, DuplicateAction::Warn);

    db.insert_job_history(&history(first.0, "failed")).unwrap();
    let (_, action) = accepted(
        db.admit_duplicate_submission(&request(&original, 5))
            .unwrap(),
    );
    assert_eq!(action, DuplicateAction::Warn);
}

#[test]
fn idempotency_conflicts_even_when_force_bypasses_duplicates() {
    let db = Database::open_in_memory().unwrap();
    let original = spec(vec![vec![("one@example", 10)]]);
    let key = CallerScopedIdempotency::new("graphql:user-1", "request-1").unwrap();
    let mut first_request = request(&original, 7);
    first_request.idempotency = Some(key.clone());
    let (first, _) = accepted(db.admit_duplicate_submission(&first_request).unwrap());

    assert!(matches!(
        db.admit_duplicate_submission(&first_request).unwrap(),
        DuplicateAdmission::Idempotent { job_id, .. } if job_id == first
    ));

    let mut conflicting = request(&spec(vec![vec![("other@example", 10)]]), 8);
    conflicting.mode = DuplicateMode::Force;
    conflicting.idempotency = Some(key);
    assert!(matches!(
        db.admit_duplicate_submission(&conflicting).unwrap(),
        DuplicateAdmission::IdempotencyConflict { job_id } if job_id == first
    ));
}

#[test]
fn visible_history_deletion_does_not_forget_identity() {
    let db = Database::open_in_memory().unwrap();
    let original = spec(vec![vec![("one@example", 10)]]);
    let (job_id, _) = accepted(
        db.admit_duplicate_submission(&request(&original, 9))
            .unwrap(),
    );
    db.activate_duplicate_admission(job_id).unwrap();
    db.insert_job_history(&history(job_id.0, "complete"))
        .unwrap();
    assert!(db.delete_job_history(job_id.0).unwrap());
    assert!(db.duplicate_snapshot(job_id).unwrap().is_some());
    assert!(db.forget_duplicate_identity(job_id).unwrap());
    assert!(db.duplicate_snapshot(job_id).unwrap().is_none());
}

#[test]
fn backfill_checkpoint_is_resumable() {
    let db = Database::open_in_memory().unwrap();
    assert!(db.duplicate_backfill_state().unwrap().is_none());
    db.save_duplicate_backfill_state(DuplicateBackfillState {
        cursor_job_id: Some(weaver_server_core::JobId(42)),
        completed_at: None,
    })
    .unwrap();
    assert_eq!(
        db.duplicate_backfill_state()
            .unwrap()
            .unwrap()
            .cursor_job_id,
        Some(weaver_server_core::JobId(42))
    );
}

#[tokio::test]
async fn fingerprint_backfill_resumes_and_is_idempotent() {
    let db = Database::open_in_memory().unwrap();
    let nzb_zstd = compress_nzb_bytes(
        br#"<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
              <file poster="test@test.com" date="1234567890" subject="fixture - &quot;file.rar&quot; yEnc (1/1)">
                <groups><group>alt.binaries.test</group></groups>
                <segments><segment bytes="500000" number="1">fixture-seg@test.com</segment></segments>
              </file>
            </nzb>"#,
    )
    .unwrap();
    for job_id in 1..=65 {
        let payload = if job_id == 1 {
            vec![0]
        } else {
            nzb_zstd.clone()
        };
        db.create_active_job(&active_backfill_job(job_id, payload))
            .unwrap();
    }
    // History can legitimately outlive its persisted NZB; it must not stall
    // the cursor after an otherwise valid resume page.
    db.insert_job_history(&history(66, "complete")).unwrap();

    let first = run_duplicate_fingerprint_backfill_batch(&db).await.unwrap();
    assert_eq!(first.scanned, 64);
    assert_eq!(first.backfilled, 63);
    assert_eq!(first.skipped, 1);
    assert!(!first.completed);
    assert!(db.duplicate_snapshot(JobId(64)).unwrap().is_some());
    assert!(db.duplicate_snapshot(JobId(65)).unwrap().is_none());

    let second = reconcile_duplicate_fingerprint_backfill(&db, 2)
        .await
        .unwrap();
    assert_eq!(second.scanned, 2);
    assert_eq!(second.backfilled, 1);
    assert_eq!(second.skipped, 1);
    assert!(second.completed);
    assert!(db.duplicate_snapshot(JobId(65)).unwrap().is_some());

    let replay = run_duplicate_fingerprint_backfill_batch(&db).await.unwrap();
    assert_eq!(replay.scanned, 0);
    assert!(replay.completed);
    assert_eq!(
        db.duplicate_backfill_state()
            .unwrap()
            .unwrap()
            .cursor_job_id,
        Some(JobId(66))
    );
}

#[test]
fn score_admission_keeps_ties_parked_and_promotes_higher_scores() {
    let db = Database::open_in_memory().unwrap();
    let incumbent = spec(vec![vec![("incumbent@example", 10)]]);
    let tie = spec(vec![vec![("tie@example", 10)]]);
    let upgrade = spec(vec![vec![("upgrade@example", 10)]]);

    let first = match db
        .admit_duplicate_submission(&score_request(&incumbent, 21, "  Group－A  ", 10))
        .unwrap()
    {
        DuplicateAdmission::Accepted {
            job_id,
            semantic: Some(semantic),
            ..
        } => {
            assert_eq!(semantic.state, SemanticCandidateState::Active);
            assert_eq!(semantic.normalized_key, "group-a");
            job_id
        }
        outcome => panic!("expected active score candidate, got {outcome:?}"),
    };

    let parked = match db
        .admit_duplicate_submission(&score_request(&tie, 22, "group-a", 10))
        .unwrap()
    {
        DuplicateAdmission::Parked {
            job_id, semantic, ..
        } => {
            assert_eq!(semantic.state, SemanticCandidateState::Parked);
            job_id
        }
        outcome => panic!("expected tied candidate to park, got {outcome:?}"),
    };
    assert_eq!(
        db.duplicate_snapshot(parked).unwrap().unwrap().lifecycle,
        weaver_server_core::DuplicateJobLifecycle::Parked
    );

    let upgraded = match db
        .admit_duplicate_submission(&score_request(&upgrade, 23, "GROUP-A", 11))
        .unwrap()
    {
        DuplicateAdmission::Accepted {
            job_id,
            semantic: Some(semantic),
            ..
        } if semantic.state == SemanticCandidateState::Active
            && semantic.superseded_job_id == Some(first) =>
        {
            job_id
        }
        outcome => panic!("expected higher score candidate to supersede, got {outcome:?}"),
    };
    assert_eq!(
        db.semantic_candidate_snapshot(first)
            .unwrap()
            .unwrap()
            .state,
        SemanticCandidateState::Parked
    );
    assert!(
        db.semantic_candidate_snapshot(first)
            .unwrap()
            .unwrap()
            .source_stored
    );

    db.insert_job_history(&history(first.0, "cancelled"))
        .unwrap();
    let superseded = db.semantic_candidate_snapshot(first).unwrap().unwrap();
    assert_eq!(superseded.state, SemanticCandidateState::Parked);
    assert!(superseded.source_stored);
    assert!(db.semantic_candidate_snapshot(upgraded).unwrap().is_some());
}

#[test]
fn all_disables_semantic_grouping_but_not_article_fingerprint_policy() {
    let db = Database::open_in_memory().unwrap();
    let original = spec(vec![vec![("same@example", 10)]]);
    let mut first = score_request(&original, 31, "group-a", 100);
    first.mode = DuplicateMode::All;
    first.semantic = None;
    first.semantic_source = None;
    let (job_id, _) = accepted(db.admit_duplicate_submission(&first).unwrap());
    db.activate_duplicate_admission(job_id).unwrap();

    let mut duplicate = score_request(&original, 32, "group-a", 1);
    duplicate.mode = DuplicateMode::All;
    assert!(matches!(
        db.admit_duplicate_submission(&duplicate).unwrap(),
        DuplicateAdmission::Blocked { .. }
    ));
}

#[test]
fn semantic_keys_use_nfkc_and_full_case_folding() {
    assert_eq!(
        normalize_semantic_duplicate_key("  ＳＴＲＡＳＳＥ  "),
        normalize_semantic_duplicate_key("Straße")
    );
    assert_eq!(
        normalize_semantic_duplicate_key("ΟΣ"),
        normalize_semantic_duplicate_key("ος")
    );
    assert_eq!(normalize_semantic_duplicate_key(" \u{3000} "), None);
}

#[test]
fn semantic_terminal_causes_are_typed_and_deterministic() {
    assert_eq!(
        classify_semantic_terminal_cause("failed", Some("PAR2 repair failed"), 1_000, 0),
        SemanticTerminalCause::RepairFailure
    );
    assert_eq!(
        classify_semantic_terminal_cause("failed", Some("disk full while moving"), 1_000, 0),
        SemanticTerminalCause::DiskOrPermissionFailure
    );
    assert_eq!(
        classify_semantic_terminal_cause("cancelled", Some("missing article"), 0, 100),
        SemanticTerminalCause::UserCancelled
    );
    assert_eq!(
        classify_semantic_terminal_cause("failed", Some("NNTP connection timed out"), 0, 100),
        SemanticTerminalCause::ServerAuthQuotaOrOutage
    );
    assert_eq!(
        classify_semantic_terminal_cause("failed", Some("NNTP access denied"), 0, 100),
        SemanticTerminalCause::ServerAuthQuotaOrOutage
    );
    assert!(SemanticTerminalCause::PasswordFailure.is_promotable());
    assert!(!SemanticTerminalCause::Shutdown.is_promotable());
}

#[test]
fn promotable_failure_claims_and_releases_best_parked_candidate() {
    let db = Database::open_in_memory().unwrap();
    let failed = spec(vec![vec![("failed@example", 10)]]);
    let fallback = spec(vec![vec![("fallback@example", 10)]]);
    let failed_job = accepted(
        db.admit_duplicate_submission(&score_request(&failed, 41, "group-promote", 100))
            .unwrap(),
    )
    .0;
    let fallback_job = match db
        .admit_duplicate_submission(&score_request(&fallback, 42, "group-promote", 90))
        .unwrap()
    {
        DuplicateAdmission::Parked { job_id, .. } => job_id,
        outcome => panic!("expected parked fallback, got {outcome:?}"),
    };

    let mut failed_history = history(failed_job.0, "failed");
    failed_history.error_message = Some("PAR2 repair failed".to_string());
    db.insert_job_history(&failed_history).unwrap();
    let failed_snapshot = db.semantic_candidate_snapshot(failed_job).unwrap().unwrap();
    assert_eq!(failed_snapshot.state, SemanticCandidateState::Nonblocking);
    assert_eq!(
        failed_snapshot.terminal_cause,
        Some(SemanticTerminalCause::RepairFailure)
    );
    assert_eq!(
        failed_snapshot.promotion_state,
        SemanticPromotionState::Pending
    );

    let claim = db
        .claim_semantic_promotion(failed_job)
        .unwrap()
        .expect("promotable failure should claim fallback");
    assert_eq!(claim.job_id, fallback_job);
    assert_eq!(claim.source.nzb_zstd, vec![42]);
    let claimed = db
        .semantic_candidate_snapshot(fallback_job)
        .unwrap()
        .unwrap();
    assert_eq!(claimed.state, SemanticCandidateState::Active);
    assert_eq!(claimed.promotion_state, SemanticPromotionState::Claimed);

    db.release_semantic_promotion_claim(fallback_job, claim.generation)
        .unwrap();
    let released = db
        .semantic_candidate_snapshot(fallback_job)
        .unwrap()
        .unwrap();
    assert_eq!(released.state, SemanticCandidateState::Parked);
    assert_eq!(released.promotion_state, SemanticPromotionState::None);
}

#[test]
fn typed_terminal_cause_overrides_legacy_error_in_durable_promotion_policy() {
    let db = Database::open_in_memory().unwrap();
    let winner = spec(vec![vec![("typed-winner@example", 10)]]);
    let fallback = spec(vec![vec![("typed-fallback@example", 10)]]);
    let winner_job = accepted(
        db.admit_duplicate_submission(&score_request(&winner, 91, "typed-terminal", 100))
            .unwrap(),
    )
    .0;
    let _fallback_job = db
        .admit_duplicate_submission(&score_request(&fallback, 92, "typed-terminal", 90))
        .unwrap();
    let mut history = history(winner_job.0, "failed");
    history.error_message = Some("PAR2 repair failed".to_string());
    db.insert_job_history(&history).unwrap();

    // Pipeline provenance takes precedence over the legacy raw-error fallback.
    db.apply_semantic_terminal_cause(winner_job, SemanticTerminalCause::ServerAuthQuotaOrOutage)
        .unwrap();
    let snapshot = db.semantic_candidate_snapshot(winner_job).unwrap().unwrap();
    assert_eq!(
        snapshot.terminal_cause,
        Some(SemanticTerminalCause::ServerAuthQuotaOrOutage)
    );
    assert_eq!(snapshot.promotion_state, SemanticPromotionState::None);
    assert!(db.claim_semantic_promotion(winner_job).unwrap().is_none());
}

#[test]
fn successful_lower_score_is_suppressed_not_redownloaded_after_upgrade_failure() {
    let db = Database::open_in_memory().unwrap();
    let lower = spec(vec![vec![("success@example", 10)]]);
    let upgrade = spec(vec![vec![("upgrade@example", 10)]]);
    let parked = spec(vec![vec![("parked@example", 10)]]);
    let lower_job = accepted(
        db.admit_duplicate_submission(&score_request(&lower, 51, "successful-winner", 10))
            .unwrap(),
    )
    .0;
    db.insert_job_history(&history(lower_job.0, "complete"))
        .unwrap();
    assert_eq!(
        db.semantic_candidate_snapshot(lower_job)
            .unwrap()
            .unwrap()
            .terminal_cause,
        Some(SemanticTerminalCause::Success)
    );

    let upgrade_job = accepted(
        db.admit_duplicate_submission(&score_request(&upgrade, 52, "successful-winner", 20))
            .unwrap(),
    )
    .0;
    assert_eq!(
        db.semantic_candidate_snapshot(lower_job)
            .unwrap()
            .unwrap()
            .state,
        SemanticCandidateState::Suppressed
    );
    let parked_job = match db
        .admit_duplicate_submission(&score_request(&parked, 53, "successful-winner", 5))
        .unwrap()
    {
        DuplicateAdmission::Parked { job_id, .. } => job_id,
        outcome => panic!("expected parked lower candidate, got {outcome:?}"),
    };

    let mut failed_upgrade = history(upgrade_job.0, "failed");
    failed_upgrade.error_message = Some("archive unpack failed".to_string());
    db.insert_job_history(&failed_upgrade).unwrap();
    // Defense in depth: the parked score-5 source must not resurrect after
    // the score-20 upgrade fails because score-10 already succeeded.
    assert!(db.claim_semantic_promotion(upgrade_job).unwrap().is_none());
    assert_eq!(
        db.semantic_candidate_snapshot(parked_job)
            .unwrap()
            .unwrap()
            .state,
        SemanticCandidateState::Parked
    );
    assert_eq!(
        db.semantic_candidate_snapshot(lower_job)
            .unwrap()
            .unwrap()
            .state,
        SemanticCandidateState::Suppressed
    );
}

#[tokio::test]
async fn explicit_forget_tombstone_prevents_backfill_resurrection() {
    let db = Database::open_in_memory().unwrap();
    let nzb_zstd = compress_nzb_bytes(
        br#"<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
              <file poster="test@test.com" date="1234567890" subject="fixture - &quot;file.rar&quot; yEnc (1/1)">
                <groups><group>alt.binaries.test</group></groups>
                <segments><segment bytes="500000" number="1">fixture-seg@test.com</segment></segments>
              </file>
            </nzb>"#,
    )
    .unwrap();
    db.create_active_job(&active_backfill_job(71, nzb_zstd.clone()))
        .unwrap();
    assert_eq!(
        run_duplicate_fingerprint_backfill_batch(&db)
            .await
            .unwrap()
            .backfilled,
        1
    );
    assert!(db.forget_duplicate_identity(JobId(71)).unwrap());
    assert!(db.duplicate_snapshot(JobId(71)).unwrap().is_none());

    // Replaying from before the forgotten job cannot restore its snapshot even
    // though its persisted active NZB remains available for normal recovery.
    db.save_duplicate_backfill_state(DuplicateBackfillState {
        cursor_job_id: None,
        completed_at: None,
    })
    .unwrap();
    let replay = run_duplicate_fingerprint_backfill_batch(&db).await.unwrap();
    assert_eq!(replay.scanned, 0);
    assert!(db.duplicate_snapshot(JobId(71)).unwrap().is_none());

    // Forget is also authoritative during the first backfill window, before a
    // snapshot exists at all. Only a truly unknown ID returns false.
    db.create_active_job(&active_backfill_job(72, nzb_zstd))
        .unwrap();
    assert!(db.forget_duplicate_identity(JobId(72)).unwrap());
    assert!(!db.forget_duplicate_identity(JobId(73)).unwrap());
    db.save_duplicate_backfill_state(DuplicateBackfillState {
        cursor_job_id: None,
        completed_at: None,
    })
    .unwrap();
    assert_eq!(
        run_duplicate_fingerprint_backfill_batch(&db)
            .await
            .unwrap()
            .scanned,
        0
    );
    assert!(db.duplicate_snapshot(JobId(72)).unwrap().is_none());
}

#[test]
fn higher_score_supersession_preserves_reserved_lower_candidate_for_promotion() {
    let db = Database::open_in_memory().unwrap();
    let lower = spec(vec![vec![("reserved-lower@example", 10)]]);
    let higher = spec(vec![vec![("reserved-higher@example", 10)]]);
    let lower_job = accepted(
        db.admit_duplicate_submission(&score_request(&lower, 81, "pre-enqueue-race", 10))
            .unwrap(),
    )
    .0;
    let higher_job = accepted(
        db.admit_duplicate_submission(&score_request(&higher, 82, "pre-enqueue-race", 20))
            .unwrap(),
    )
    .0;

    // This is the deterministic database half of the pre-enqueue race: the
    // incumbent is no longer authoritative, but its stored NZB remains a real
    // parked candidate rather than being released/deleted by the stale submit.
    assert_eq!(
        db.semantic_candidate_snapshot(lower_job)
            .unwrap()
            .unwrap()
            .state,
        SemanticCandidateState::Parked
    );
    let mut failed_higher = history(higher_job.0, "failed");
    failed_higher.error_message = Some("decode corrupt data".to_string());
    db.insert_job_history(&failed_higher).unwrap();
    let claim = db
        .claim_semantic_promotion(higher_job)
        .unwrap()
        .expect("the retained lower candidate must remain promotable");
    assert_eq!(claim.job_id, lower_job);
    assert_eq!(claim.source.nzb_zstd, vec![81]);
}

#[test]
fn bulk_duplicate_summaries_return_persisted_action_and_evidence_in_one_contract() {
    let db = Database::open_in_memory().unwrap();
    let first = spec(vec![vec![("summary-one@example", 10)]]);
    let second = spec(vec![vec![("summary-two@example", 20)]]);
    let first_job = accepted(
        db.admit_duplicate_submission(&request(&first, 101))
            .unwrap(),
    )
    .0;
    let second_job = accepted(
        db.admit_duplicate_submission(&request(&second, 102))
            .unwrap(),
    )
    .0;

    let summaries = db
        .duplicate_summaries(&[second_job, first_job, first_job])
        .unwrap();
    assert_eq!(summaries.len(), 2);
    let first_summary = summaries.get(&first_job).unwrap();
    assert_eq!(first_summary.admission_action, DuplicateAction::Accept);
    assert_eq!(first_summary.admission_reason, None);
    assert_eq!(first_summary.snapshot.job_id, first_job);
    assert!(!first_summary.fingerprints.is_empty());
    assert!(first_summary.semantic.is_none());
    let second_summary = summaries.get(&second_job).unwrap();
    assert_eq!(second_summary.snapshot.job_id, second_job);
    assert_eq!(second_summary.admission_action, DuplicateAction::Warn);
    assert_eq!(
        second_summary.admission_reason,
        Some(FingerprintKind::NormalizedName)
    );
}

#[test]
fn semantic_lifecycle_metrics_are_fixed_cardinality_events() {
    let before = semantic_duplicate_lifecycle_metrics_snapshot()
        .into_iter()
        .find(|metric| metric.event == "promotion_failure")
        .map(|metric| metric.count)
        .unwrap_or_default();
    record_semantic_duplicate_lifecycle_metric(SemanticDuplicateLifecycleEvent::PromotionFailure);
    let metrics = semantic_duplicate_lifecycle_metrics_snapshot();
    assert_eq!(
        metrics
            .iter()
            .find(|metric| metric.event == "promotion_failure")
            .map(|metric| metric.count),
        Some(before + 1)
    );
    assert!(metrics.iter().all(|metric| {
        matches!(
            metric.event,
            "park"
                | "supersede"
                | "promote"
                | "promotion_recovery"
                | "promotion_retry"
                | "promotion_failure"
                | "promotion_no_fallback"
        )
    }));
}

#[test]
fn sqlite_concurrent_initial_score_claim_has_one_lease_owner() {
    let db = Database::open_in_memory().unwrap();
    let incumbent = spec(vec![vec![("claim-incumbent@example", 10)]]);
    let fallback = spec(vec![vec![("claim-fallback@example", 10)]]);
    let incumbent_job = accepted(
        db.admit_duplicate_submission(&score_request(&incumbent, 111, "sqlite-lease", 100))
            .unwrap(),
    )
    .0;
    let fallback_job = match db
        .admit_duplicate_submission(&score_request(&fallback, 112, "sqlite-lease", 90))
        .unwrap()
    {
        DuplicateAdmission::Parked { job_id, .. } => job_id,
        outcome => panic!("expected parked fallback, got {outcome:?}"),
    };
    assert!(db.mark_semantic_candidate_bad(incumbent_job).unwrap());

    let barrier = Arc::new(Barrier::new(3));
    let owners = (0..2)
        .map(|_| {
            let db = db.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                db.claim_semantic_promotion(incumbent_job).unwrap()
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let claims = owners
        .into_iter()
        .filter_map(|owner| owner.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].job_id, fallback_job);
    let snapshot = db
        .semantic_candidate_snapshot(fallback_job)
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.state, SemanticCandidateState::Active);
    assert_eq!(snapshot.promotion_state, SemanticPromotionState::Claimed);
}

#[test]
fn semantic_lifecycle_metrics_record_durable_transitions_without_identity_labels() {
    let metric_count = |event| {
        semantic_duplicate_lifecycle_metrics_snapshot()
            .into_iter()
            .find(|metric| metric.event == event)
            .map(|metric| metric.count)
            .unwrap_or_default()
    };
    let before_park = metric_count("park");
    let before_supersede = metric_count("supersede");
    let before_retry = metric_count("promotion_retry");
    let before_no_fallback = metric_count("promotion_no_fallback");

    let db = Database::open_in_memory().unwrap();
    let first = spec(vec![vec![("metric-first@example", 10)]]);
    let parked = spec(vec![vec![("metric-parked@example", 10)]]);
    let upgrade = spec(vec![vec![("metric-upgrade@example", 10)]]);
    let first_job = accepted(
        db.admit_duplicate_submission(&score_request(&first, 121, "metric-group", 100))
            .unwrap(),
    )
    .0;
    let _parked_job = match db
        .admit_duplicate_submission(&score_request(&parked, 122, "metric-group", 90))
        .unwrap()
    {
        DuplicateAdmission::Parked { job_id, .. } => job_id,
        outcome => panic!("expected parked candidate, got {outcome:?}"),
    };
    let upgrade_job = accepted(
        db.admit_duplicate_submission(&score_request(&upgrade, 123, "metric-group", 110))
            .unwrap(),
    )
    .0;
    assert!(db.mark_semantic_candidate_bad(upgrade_job).unwrap());
    let retry = db
        .claim_semantic_promotion(upgrade_job)
        .unwrap()
        .expect("superseded candidate should be claimable");
    assert_eq!(retry.job_id, first_job);
    assert!(
        db.release_semantic_promotion_claim(retry.job_id, retry.generation)
            .unwrap()
    );

    let no_fallback = spec(vec![vec![("metric-none@example", 10)]]);
    let no_fallback_job = accepted(
        db.admit_duplicate_submission(&score_request(&no_fallback, 124, "metric-none", 100))
            .unwrap(),
    )
    .0;
    assert!(db.mark_semantic_candidate_bad(no_fallback_job).unwrap());
    assert!(
        db.claim_semantic_promotion(no_fallback_job)
            .unwrap()
            .is_none()
    );

    assert!(metric_count("park") > before_park);
    assert!(metric_count("supersede") > before_supersede);
    assert!(metric_count("promotion_retry") > before_retry);
    assert!(metric_count("promotion_no_fallback") > before_no_fallback);
}
