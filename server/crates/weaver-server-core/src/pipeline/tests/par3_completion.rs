use super::*;

const INDEX: &[u8] = include_bytes!("../repair/backend/fixtures/set.par3");

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
