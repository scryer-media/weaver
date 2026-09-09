use super::*;

const INDEX: &[u8] = include_bytes!("../repair/backend/fixtures/set.par3");

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
        state.status = JobStatus::Downloading;
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
        let source = NzbFileId {
            job_id,
            file_index: 0,
        };
        pipeline.invalidate_par2_session_for_file_write(source);
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
    }
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
