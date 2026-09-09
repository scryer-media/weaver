use super::*;

const INDEX: &[u8] = include_bytes!("../repair/backend/fixtures/set.par3");

#[tokio::test]
async fn embedded_discovery_cache_is_withdrawn_before_admission_on_writes_and_rebinding() {
    let root = TempDir::new().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    let first = NzbFileId {
        job_id: JobId(3114),
        file_index: 0,
    };
    let sibling = NzbFileId {
        file_index: 1,
        ..first
    };
    let other = NzbFileId {
        job_id: JobId(3115),
        ..first
    };
    for file in [first, sibling, other] {
        pipeline.par3_inside_probes.insert(file).unwrap();
    }
    assert!(pipeline.par3_runtime.is_none());
    pipeline.invalidate_par3_source_write(first);
    assert!(!pipeline.par3_inside_probes.contains(first));
    assert!(pipeline.par3_inside_probes.contains(sibling));
    assert!(pipeline.par3_inside_probes.contains(other));
    pipeline.par3_inside_probes.insert(first).unwrap();
    pipeline.invalidate_par3_bindings(first.job_id);
    assert!(!pipeline.par3_inside_probes.contains(first));
    assert!(!pipeline.par3_inside_probes.contains(sibling));
    assert!(pipeline.par3_inside_probes.contains(other));
    assert!(
        pipeline.par3_runtime.is_none(),
        "ordinary source changes must not allocate an engine"
    );
}

#[tokio::test]
async fn missing_par2_metadata_requires_current_par3_evidence_for_every_payload() {
    for unprotected in [false, true] {
        let root = TempDir::new().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
        let job_id = JobId(3113);
        let mut files = vec![
            ("a.bin", (0..5000u32).map(|i| (i * 7 + 3) as u8).collect()),
            ("b.txt", b"qrstuvwxyz".to_vec()),
            (
                "sub/c.bin",
                (0..4000u32).map(|i| (i * 13 + 1) as u8).collect(),
            ),
            ("set.par3", INDEX.to_vec()),
        ];
        if unprotected {
            files.push(("unprotected.bin", b"not covered by the PAR3 set".to_vec()));
        }
        let mut descriptions: Vec<_> = files
            .iter()
            .map(|(name, bytes)| ((*name).into(), bytes.len() as u32))
            .collect();
        descriptions.push(("missing.par2".into(), 100));
        let mut spec = standalone_job_spec("Missing PAR2 metadata", &descriptions);
        for file in &mut spec.files {
            file.role = FileRole::from_filename(&file.filename);
        }
        let working = insert_active_job(&mut pipeline, job_id, spec).await;
        tokio::fs::create_dir(working.join("sub")).await.unwrap();
        for (index, (name, bytes)) in files.iter().enumerate() {
            write_and_complete_file(&mut pipeline, job_id, index as u32, name, bytes).await;
        }
        assert!(!pipeline.par3_verifies_all_payloads(job_id));
        pipeline
            .try_load_par3_metadata(
                job_id,
                NzbFileId {
                    job_id,
                    file_index: 3,
                },
            )
            .await;
        settle_par3(&mut pipeline, job_id).await;
        let runtime = pipeline.par3_runtime.as_ref().unwrap();
        assert!(runtime.verified(job_id));
        let reads = runtime.source_verifications(job_id);
        for _ in 0..3 {
            assert_eq!(pipeline.par3_verifies_all_payloads(job_id), !unprotected);
        }
        assert_eq!(
            pipeline
                .par3_runtime
                .as_ref()
                .unwrap()
                .source_verifications(job_id),
            reads,
            "completion policy must consume current evidence without reading sources"
        );
        assert!(!pipeline.par2_verified.contains(&job_id));
        pipeline.invalidate_par3_source_write(NzbFileId {
            job_id,
            file_index: 0,
        });
        assert!(
            !pipeline.par3_verifies_all_payloads(job_id),
            "a source write must withdraw the alternative completion verdict"
        );
    }
}

#[tokio::test]
async fn alternate_repair_requires_a_typed_native_reason_for_every_failed_set() {
    use crate::pipeline::repair::backend::AlternateRepairReason;
    let root = TempDir::new().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    let job_id = JobId(3112);
    let mut spec = standalone_job_spec(
        "Typed alternate repair",
        &[("set.par3".into(), INDEX.len() as u32)],
    );
    spec.files[0].role = FileRole::from_filename("set.par3");
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, "set.par3", INDEX).await;
    pipeline
        .try_load_par3_metadata(
            job_id,
            NzbFileId {
                job_id,
                file_index: 0,
            },
        )
        .await;
    settle_par3(&mut pipeline, job_id).await;
    let set_id = par2_rs::RecoverySetId::from_bytes([7; 16]);
    let set = pipeline
        .ensure_par2_runtime(job_id)
        .sets
        .entry(set_id)
        .or_default();
    set.settled = true;
    // The message deliberately includes recovery language. Eligibility must
    // come from the native verdict kind, never a substring in an error body.
    set.failure = Some("I/O failure while reading insufficient recovery".into());
    assert!(!pipeline.par3_has_work_after_par2_failure(job_id));
    for reason in [
        AlternateRepairReason::InsufficientRecovery,
        AlternateRepairReason::ResourceLimited,
    ] {
        pipeline
            .ensure_par2_runtime(job_id)
            .sets
            .get_mut(&set_id)
            .unwrap()
            .alternate_repair = Some(reason);
        assert!(pipeline.par3_has_work_after_par2_failure(job_id));
    }
    let sibling = par2_rs::RecoverySetId::from_bytes([8; 16]);
    let set = pipeline
        .ensure_par2_runtime(job_id)
        .sets
        .entry(sibling)
        .or_default();
    set.settled = true;
    set.failure = Some("cancelled".into());
    assert!(
        !pipeline.par3_has_work_after_par2_failure(job_id),
        "an eligible sibling cannot turn a terminal failure into fallback work"
    );
}

#[tokio::test]
async fn par2_capacity_and_promotion_exclude_par3_recovery_carriers() {
    let root = TempDir::new().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    let job_id = JobId(3110);
    let payload = vec![3u8; 10000];
    let index = build_test_par2_index_for_files(&[("payload.bin", &payload)], 2048);
    let mut spec = standalone_job_spec(
        "Mixed recovery ownership",
        &[
            ("payload.bin".into(), payload.len() as u32),
            ("repair.par2".into(), index.len() as u32),
            ("repair.vol0+16.par3".into(), 1 << 20),
        ],
    );
    for file in &mut spec.files {
        file.role = FileRole::from_filename(&file.filename);
    }
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 1, "repair.par2", &index).await;
    pipeline
        .try_load_par2_metadata(
            job_id,
            NzbFileId {
                job_id,
                file_index: 1,
            },
        )
        .await;
    let set_id = pipeline.par2_served_set_id(job_id).unwrap();
    assert_eq!(pipeline.total_recovery_block_capacity(job_id, set_id), 0);
    assert_eq!(pipeline.promote_recovery_targeted(job_id, set_id, 1), 0);
    assert_eq!(
        pipeline.jobs[&job_id].recovery_queue.len(),
        1,
        "the PAR3 volume must remain available to its own engine"
    );
}

#[tokio::test]
async fn par2_installation_fences_par3_evidence_and_reverifies_only_the_rewritten_source() {
    let root = TempDir::new().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    let job_id = JobId(3111);
    let original: Vec<u8> = (0..5000u32).map(|i| (i * 7 + 3) as u8).collect();
    let mut files = vec![
        ("a.bin", original.clone()),
        ("b.txt", b"qrstuvwxyz".to_vec()),
        (
            "sub/c.bin",
            (0..4000u32).map(|i| (i * 13 + 1) as u8).collect(),
        ),
        ("set.par3", INDEX.to_vec()),
    ];
    let par2 = build_test_par2_index_for_files(
        &[
            ("a.bin", &files[0].1),
            ("b.txt", &files[1].1),
            ("sub/c.bin", &files[2].1),
        ],
        2048,
    );
    files.push(("set.par2", par2));
    files.push((
        "sibling.par2",
        build_test_par2_index_for_files(&[("a.bin", &original)], 2048),
    ));
    files[0].1[2300] ^= 1;
    let mut spec = standalone_job_spec(
        "Cross-format source fence",
        &files
            .iter()
            .map(|(name, bytes)| ((*name).into(), bytes.len() as u32))
            .collect::<Vec<_>>(),
    );
    for file in &mut spec.files {
        file.role = FileRole::from_filename(&file.filename);
    }
    let working = insert_active_job(&mut pipeline, job_id, spec).await;
    tokio::fs::create_dir(working.join("sub")).await.unwrap();
    for (index, (name, bytes)) in files.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, index as u32, name, bytes).await;
    }
    for file_index in [4, 5] {
        pipeline
            .try_load_par2_metadata(job_id, NzbFileId { job_id, file_index })
            .await;
    }
    assert_eq!(pipeline.par2_servable_set_ids(job_id).len(), 2);
    assert!(
        pipeline
            .resolve_par2_file_binding(NzbFileId {
                job_id,
                file_index: 0,
            })
            .is_none(),
        "handoff must resolve the selected set when the global binding is ambiguous"
    );
    pipeline
        .try_load_par3_metadata(
            job_id,
            NzbFileId {
                job_id,
                file_index: 3,
            },
        )
        .await;
    settle_par3(&mut pipeline, job_id).await;
    let source = par3_rs::source::SourceId(0);
    let clean = par3_rs::source::SourceId(1);
    let runtime = pipeline.par3_runtime.as_ref().unwrap();
    assert!(!runtime.verified_file(job_id, source));
    assert!(runtime.source_verified(job_id, clean));
    let reads = runtime.source_verifications(job_id);
    let set_id = pipeline.par2_served_set_id(job_id).unwrap();
    // Archive-type shortcuts can settle before an extraction failure arrives.
    // Native PAR3 damage must reopen both overlapping claims without reading
    // sources, then require PAR2's own authoritative pass.
    for id in pipeline.par2_servable_set_ids(job_id) {
        pipeline.settle_par2_set(job_id, id,
            crate::pipeline::completion::finalize::check::Par2SetSettlementReason::Clean {
                slice_size: 2048,
                verification_mode: crate::pipeline::completion::finalize::check::CleanPar2VerificationMode::StrongDecode,
            }).await;
    }
    assert!(pipeline.par2_verified.contains(&job_id));
    assert!(pipeline.reopen_par2_strong_decode_claims_on_par3_damage(job_id));
    assert!(!pipeline.par2_verified.contains(&job_id));
    assert!(
        pipeline
            .par2_runtime(job_id)
            .unwrap()
            .sets
            .values()
            .all(|set| !set.settled)
    );
    assert!(pipeline.par3_requires_authoritative_par2(job_id));
    assert!(!pipeline.reopen_par2_strong_decode_claims_on_par3_damage(job_id));
    assert_eq!(
        pipeline
            .par3_runtime
            .as_ref()
            .unwrap()
            .source_verifications(job_id),
        reads
    );
    let native = pipeline
        .par2_set_for(job_id, set_id)
        .unwrap()
        .as_ref()
        .clone();
    let mut options = par2_rs::Par2RepairSessionOptions::new(working.clone(), Vec::new());
    options.file_set = Some(native);
    options.memory_limit = Some(8 << 20);
    let outcome = tokio::task::spawn_blocking(move || {
        par2_rs::Par2RepairSession::open(options)
            .unwrap()
            .analyze()
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(
        outcome
            .verification
            .files
            .iter()
            .filter(|file| !matches!(file.status, par2_rs::verify::FileStatus::Complete))
            .count(),
        1
    );
    pipeline
        .fence_par3_before_par2_repair(job_id, set_id, Some(&outcome.verification))
        .unwrap();
    assert!(pipeline.par3_runtime.as_ref().unwrap().has_work(job_id));
    assert!(
        !pipeline
            .par3_runtime
            .as_ref()
            .unwrap()
            .verified_file(job_id, source)
    );
    // Model the verified installation after the native PAR2 operation. The
    // strong PAR3 proof must come from its fresh source read, not this write.
    tokio::fs::write(working.join("a.bin"), &original)
        .await
        .unwrap();
    let rewritten = outcome
        .verification
        .files
        .iter()
        .filter(|file| !matches!(file.status, par2_rs::verify::FileStatus::Complete))
        .map(|file| file.file_id)
        .collect();
    pipeline
        .refresh_par3_after_par2_repair(job_id, set_id, &rewritten)
        .unwrap();
    settle_par3(&mut pipeline, job_id).await;
    let runtime = pipeline.par3_runtime.as_ref().unwrap();
    assert!(runtime.verified(job_id));
    assert_eq!(
        runtime.source_verifications(job_id),
        reads + 1,
        "the repaired source owes native verification; clean siblings owe none"
    );
}

#[tokio::test]
async fn par3_index_avoids_speculative_recovery_and_defers_early_health_abort() {
    let root = TempDir::new().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    let job_id = JobId(3104);
    let mut spec = standalone_job_spec(
        "PAR3 health",
        &[
            ("payload.bin".into(), 10000),
            ("set.par3".into(), 100),
            ("set.vol0+1.par3".into(), 3000),
        ],
    );
    for file in &mut spec.files[1..] {
        file.role = FileRole::from_filename(&file.filename);
    }
    insert_active_job(&mut pipeline, job_id, spec).await;
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    assert_eq!(state.recovery_queue.len(), 1);
    assert_eq!(state.download_queue.len(), 2);
    state.failed_bytes = 3000;
    pipeline.check_health(job_id);
    assert!(!matches!(
        pipeline.jobs[&job_id].status,
        JobStatus::Failed { .. }
    ));
    // An unparsed hint earns time to discover metadata, not fabricated blocks
    // or speculative verification state.
    assert!(pipeline.par3_runtime.is_none());
}

async fn settle_par3(pipeline: &mut Pipeline, job_id: JobId) {
    while pipeline.par3_runtime.as_ref().unwrap().has_work(job_id) {
        let done =
            tokio::time::timeout(Duration::from_secs(10), pipeline.repair_work_done_rx.recv())
                .await
                .unwrap()
                .unwrap();
        pipeline.handle_repair_work_done(done).await;
    }
}

#[tokio::test]
async fn par3_repairs_an_interior_article_hole_and_reconciles_completion() {
    assert_par3_repairs_from_status(JobStatus::Downloading).await;
}

#[tokio::test]
async fn par3_repair_can_follow_retired_extraction_work() {
    for status in [JobStatus::Extracting, JobStatus::QueuedExtract] {
        assert_par3_repairs_from_status(status).await;
    }
}

async fn assert_par3_repairs_from_status(status: JobStatus) {
    let root = TempDir::new().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    let job_id = JobId(3103);
    let expected: Vec<u8> = (0..5000u32).map(|i| (i * 7 + 3) as u8).collect();
    let recovery = include_bytes!("../repair/backend/fixtures/set.vol0+1.par3");
    let files = [
        ("a.bin", expected.clone()),
        ("b.txt", b"qrstuvwxyz".to_vec()),
        (
            "sub/c.bin",
            (0..4000u32).map(|i| (i * 13 + 1) as u8).collect(),
        ),
        ("set.par3", INDEX.to_vec()),
        ("set.vol0+1.par3", recovery.to_vec()),
    ];
    let mut spec = standalone_job_spec(
        "PAR3 article hole",
        &files
            .iter()
            .map(|(name, bytes)| ((*name).into(), bytes.len() as u32))
            .collect::<Vec<_>>(),
    );
    spec.files[0].segments = [2000, 2000, 1000]
        .into_iter()
        .enumerate()
        .map(|(number, bytes)| {
            segment_spec! {
                number: number as u32, bytes: bytes, message_id: format!("hole-{number}@example.com"),
            }
        })
        .collect();
    for file in &mut spec.files[3..] {
        file.role = FileRole::from_filename(&file.filename);
    }
    let working = insert_active_job(&mut pipeline, job_id, spec).await;
    tokio::fs::create_dir(working.join("sub")).await.unwrap();
    let mut damaged = expected.clone();
    damaged[2000..4000].fill(0);
    tokio::fs::write(working.join("a.bin"), damaged)
        .await
        .unwrap();
    {
        let file = pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap();
        file.record_placement(0, 0, 2000);
        file.commit_segment(0, 2000).unwrap();
        file.record_placement(2, 4000, 1000);
        file.commit_segment(2, 1000).unwrap();
    }
    for (index, (name, bytes)) in files.iter().enumerate().skip(1) {
        write_and_complete_file(&mut pipeline, job_id, index as u32, name, bytes).await;
        pipeline
            .try_load_par3_metadata(
                job_id,
                NzbFileId {
                    job_id,
                    file_index: index as u32,
                },
            )
            .await;
    }
    let clean_stamp = tokio::fs::metadata(working.join("sub/c.bin"))
        .await
        .unwrap()
        .modified()
        .unwrap();
    settle_par3(&mut pipeline, job_id).await;
    assert_eq!(
        pipeline
            .par3_runtime
            .as_ref()
            .unwrap()
            .assessments(job_id)
            .next()
            .unwrap()
            .1
            .status,
        par3_rs::session::RepairStatus::Ready
    );
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.status = status;
        state.refresh_runtime_lanes_from_status();
    }
    assert!(pipeline.check_par3_completion(job_id).await);
    assert!(matches!(
        pipeline.jobs[&job_id].status,
        JobStatus::Repairing
    ));
    settle_par3(&mut pipeline, job_id).await;
    assert_eq!(
        tokio::fs::read(working.join("a.bin")).await.unwrap(),
        expected
    );
    assert!(
        pipeline.jobs[&job_id]
            .assembly
            .file(NzbFileId {
                job_id,
                file_index: 0
            })
            .unwrap()
            .is_complete()
    );
    assert_eq!(
        pipeline
            .par3_runtime
            .as_ref()
            .unwrap()
            .assessments(job_id)
            .next()
            .unwrap()
            .1
            .status,
        par3_rs::session::RepairStatus::Complete
    );
    assert_eq!(
        tokio::fs::metadata(working.join("sub/c.bin"))
            .await
            .unwrap()
            .modified()
            .unwrap(),
        clean_stamp
    );
    let repaired = NzbFileId {
        job_id,
        file_index: 0,
    };
    let verifications = pipeline
        .par3_runtime
        .as_ref()
        .unwrap()
        .source_verifications(job_id);
    // Discovery can run again after repair invalidates an earlier negative
    // probe. It must not republish the old article hole over the installed file.
    for _ in 0..3 {
        pipeline.try_load_par3_metadata(job_id, repaired).await;
        settle_par3(&mut pipeline, job_id).await;
        assert!(pipeline.par3_runtime.as_ref().unwrap().verified(job_id));
        assert_eq!(
            pipeline
                .par3_runtime
                .as_ref()
                .unwrap()
                .source_verifications(job_id),
            verifications
        );
    }
    assert!(!pipeline.check_par3_completion(job_id).await);
    assert!(!pipeline.par2_verified.contains(&job_id));
}

#[tokio::test]
async fn late_metadata_assesses_committed_files_and_exposes_native_damage() {
    for damaged in [false, true] {
        let root = TempDir::new().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
        let job_id = JobId(3102);
        let mut payload: Vec<u8> = (0..5000u32).map(|i| (i * 7 + 3) as u8).collect();
        if damaged {
            payload[2300] ^= 1;
        }
        let files = [
            ("a.bin", payload),
            ("b.txt", b"qrstuvwxyz".to_vec()),
            (
                "sub/c.bin",
                (0..4000u32).map(|i| (i * 13 + 1) as u8).collect(),
            ),
            ("set.par3", INDEX.to_vec()),
        ];
        let mut spec = standalone_job_spec(
            "PAR3 assessment",
            &files
                .iter()
                .map(|(name, bytes)| ((*name).to_owned(), bytes.len() as u32))
                .collect::<Vec<_>>(),
        );
        spec.files[3].role = FileRole::from_filename("set.par3");
        let working = insert_active_job(&mut pipeline, job_id, spec).await;
        tokio::fs::create_dir(working.join("sub")).await.unwrap();
        for (index, (name, bytes)) in files.iter().enumerate() {
            write_and_complete_file(&mut pipeline, job_id, index as u32, name, bytes).await;
            pipeline
                .try_load_par3_metadata(
                    job_id,
                    NzbFileId {
                        job_id,
                        file_index: index as u32,
                    },
                )
                .await;
        }
        while pipeline.par3_runtime.as_ref().unwrap().has_work(job_id) {
            let done =
                tokio::time::timeout(Duration::from_secs(10), pipeline.repair_work_done_rx.recv())
                    .await
                    .unwrap()
                    .unwrap();
            pipeline.handle_repair_work_done(done).await;
        }
        let views: Vec<_> = pipeline
            .par3_runtime
            .as_ref()
            .unwrap()
            .assessments(job_id)
            .collect();
        assert_eq!(views.len(), 1);
        let view = views[0].1;
        assert_eq!(
            view.status,
            if damaged {
                par3_rs::session::RepairStatus::NeedRecovery
            } else {
                par3_rs::session::RepairStatus::Complete
            }
        );
        assert_eq!(
            view.files.iter().filter(|file| file.complete).count(),
            if damaged { 2 } else { 3 }
        );
        if damaged {
            assert_eq!(view.files[0].unresolved.len(), 1);
            assert_eq!(view.files[0].unresolved[0], 2000..4000);
            assert_eq!(view.requirements[0].additional, 1);
        }
        let expected_counts = if damaged { [0, 1, 0, 0] } else { [1, 0, 0, 0] };
        let reads = pipeline
            .par3_runtime
            .as_ref()
            .unwrap()
            .source_verifications(job_id);
        for _ in 0..3 {
            pipeline.note_par3_verification(job_id);
            pipeline.note_job_unverifiable_if_no_par2_set(job_id);
            assert_eq!(par3_verification_counts(&pipeline), expected_counts);
            assert_eq!(
                pipeline
                    .par3_runtime
                    .as_ref()
                    .unwrap()
                    .source_verifications(job_id),
                reads
            );
        }
        let source = NzbFileId {
            job_id,
            file_index: 0,
        };
        pipeline.invalidate_par2_session_for_file_write(source);
        pipeline.note_par3_verification(job_id);
        assert_eq!(par3_verification_counts(&pipeline), expected_counts);
        assert_eq!(
            pipeline
                .par3_runtime
                .as_ref()
                .unwrap()
                .assessments(job_id)
                .count(),
            0
        );
        let mut replacement = files[0].1.clone();
        replacement[2300] ^= 1;
        tokio::fs::write(working.join("a.bin"), replacement)
            .await
            .unwrap();
        pipeline.try_load_par3_metadata(job_id, source).await;
        while pipeline.par3_runtime.as_ref().unwrap().has_work(job_id) {
            let done =
                tokio::time::timeout(Duration::from_secs(10), pipeline.repair_work_done_rx.recv())
                    .await
                    .unwrap()
                    .unwrap();
            pipeline.handle_repair_work_done(done).await;
        }
        let (_, view) = pipeline
            .par3_runtime
            .as_ref()
            .unwrap()
            .assessments(job_id)
            .next()
            .unwrap();
        assert_eq!(
            view.status,
            if damaged {
                par3_rs::session::RepairStatus::Complete
            } else {
                par3_rs::session::RepairStatus::NeedRecovery
            }
        );
        pipeline.note_job_unverifiable_if_no_par2_set(job_id);
        assert_eq!(par3_verification_counts(&pipeline), [1, 1, 0, 0]);
    }
}

fn par3_verification_counts(pipeline: &Pipeline) -> [u64; 4] {
    let snapshot = pipeline.metrics.job_lifecycle.snapshot();
    ["intact", "damaged", "missing", "unverifiable"].map(|outcome| {
        snapshot
            .verifications
            .iter()
            .find(|(name, _)| *name == outcome)
            .unwrap()
            .1
    })
}

#[test]
fn repair_completion_envelope_preserves_par2_queue_payload_size() {
    assert_eq!(
        std::mem::size_of::<RepairWorkDone>(),
        std::mem::size_of::<Par2AnalysisWorkDone>()
    );
}

#[tokio::test]
async fn par2_only_publications_create_no_par3_runtime_or_worker_queue() {
    let root = TempDir::new().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    let job_id = JobId(3100);
    let spec = standalone_job_spec(
        "PAR2 isolation",
        &[("payload.bin".into(), 4), ("set.par2".into(), 8)],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, name, bytes) in [
        (0, "payload.bin", &b"data"[..]),
        (1, "set.par2", &b"PAR2\0PKT"[..]),
    ] {
        write_and_complete_file(&mut pipeline, job_id, file_index, name, bytes).await;
        let file_id = NzbFileId { job_id, file_index };
        pipeline.file_prefix_16k.insert(file_id, bytes.to_vec());
        pipeline.try_load_par3_metadata(job_id, file_id).await;
        assert!(pipeline.par3_runtime.is_none());
    }
}

#[tokio::test]
async fn completion_waits_for_authenticated_carrier_worker_including_renamed_input() {
    for filename in ["set.par3", "renamed.bin"] {
        let root = TempDir::new().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
        let job_id = JobId(3101);
        let mut spec =
            standalone_job_spec("PAR3 discovery", &[(filename.into(), INDEX.len() as u32)]);
        spec.files[0].role = FileRole::from_filename(filename);
        let working = insert_active_job(&mut pipeline, job_id, spec).await;
        write_and_complete_file(&mut pipeline, job_id, 0, filename, INDEX).await;
        let file_id = NzbFileId {
            job_id,
            file_index: 0,
        };
        pipeline
            .file_prefix_16k
            .insert(file_id, INDEX[..64].to_vec());
        {
            let state = pipeline.jobs.get_mut(&job_id).unwrap();
            state.download_queue = DownloadQueue::new();
            state.status = JobStatus::Downloading;
            state.refresh_runtime_lanes_from_status();
        }
        pipeline.try_load_par3_metadata(job_id, file_id).await;
        assert!(pipeline.par3_runtime.as_ref().unwrap().has_work(job_id));
        pipeline.check_job_completion(job_id).await;
        assert!(matches!(
            pipeline.jobs[&job_id].status,
            JobStatus::Downloading
        ));
        assert!(working.join(filename).exists());
        let done =
            tokio::time::timeout(Duration::from_secs(10), pipeline.repair_work_done_rx.recv())
                .await
                .unwrap()
                .unwrap();
        pipeline.handle_repair_work_done(done).await;
        let coordinator = pipeline.par3_runtime.as_ref().unwrap();
        assert!(!coordinator.has_work(job_id));
        assert_eq!(coordinator.authenticated_set_count(job_id), 1);
        pipeline.clear_par2_runtime_state(job_id);
        assert_eq!(
            pipeline
                .par3_runtime
                .as_ref()
                .unwrap()
                .authenticated_set_count(job_id),
            0
        );
    }
}

#[tokio::test]
async fn restored_completed_images_are_reverified_without_article_placements() {
    for changed in [false, true] {
        let root = TempDir::new().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
        let job_id = JobId(3111);
        let mut files = [
            (
                "a.bin",
                (0..5000u32).map(|i| (i * 7 + 3) as u8).collect::<Vec<_>>(),
            ),
            ("b.txt", b"qrstuvwxyz".to_vec()),
            (
                "sub/c.bin",
                (0..4000u32).map(|i| (i * 13 + 1) as u8).collect(),
            ),
            ("set.par3", INDEX.to_vec()),
        ];
        if changed {
            files[0].1[123] ^= 1;
        }
        let mut spec = standalone_job_spec(
            "PAR3 restored images",
            &files
                .iter()
                .map(|(name, bytes)| ((*name).into(), bytes.len() as u32 + 333))
                .collect::<Vec<_>>(),
        );
        spec.files[3].role = FileRole::from_filename("set.par3");
        let working = insert_active_job(&mut pipeline, job_id, spec).await;
        tokio::fs::create_dir(working.join("sub")).await.unwrap();
        for (index, (name, bytes)) in files.iter().enumerate() {
            tokio::fs::write(working.join(name), bytes).await.unwrap();
            let file = pipeline
                .jobs
                .get_mut(&job_id)
                .unwrap()
                .assembly
                .file_mut(NzbFileId {
                    job_id,
                    file_index: index as u32,
                })
                .unwrap();
            file.mark_complete();
            assert!(file.placement_of(0).is_none());
        }
        pipeline
            .try_load_par3_metadata(
                job_id,
                NzbFileId {
                    job_id,
                    file_index: 3,
                },
            )
            .await;
        settle_par3(&mut pipeline, job_id).await;
        let runtime = pipeline.par3_runtime.as_ref().unwrap();
        assert_eq!(runtime.verified(job_id), !changed);
        let (_, view) = runtime.assessments(job_id).next().unwrap();
        assert_eq!(
            view.files.iter().filter(|file| file.complete).count(),
            if changed { 2 } else { 3 }
        );
        if changed {
            assert_eq!(view.status, par3_rs::session::RepairStatus::NeedRecovery);
        }
    }
}
