// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

use crate::ingest::{FullFileName, full_file_names};

#[tokio::test]
async fn a_par2_rename_past_the_name_limit_records_the_described_name_in_full() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31101);
    let posted_name = "c9d4e7a1.bin";
    let described_name = format!("{} - 07.mkv", vec!["Harbor"; 50].join(" "));
    let payload: Vec<u8> = (0..20_480)
        .map(|index| (index as u8).wrapping_mul(37).wrapping_add(5))
        .collect();
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Harbor Rename",
            &[(posted_name.to_string(), payload.len() as u32)],
        ),
    )
    .await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set_for_files(
            &[(described_name.as_str(), payload.as_slice())],
            1024,
            1,
        ),
        &[],
    );
    write_and_complete_file(&mut pipeline, job_id, 0, posted_name, &payload).await;
    pipeline.try_deobfuscate_files_with_par2(job_id).await;

    let disk_name = pipeline
        .effective_file_identity(
            job_id,
            NzbFileId {
                job_id,
                file_index: 0,
            },
        )
        .unwrap()
        .current_filename;
    assert_ne!(disk_name, posted_name);
    assert!(disk_name.len() < described_name.len());
    assert_eq!(
        std::fs::read(working_dir.join(&disk_name)).unwrap(),
        payload
    );
    assert_eq!(
        full_file_names(&pipeline.jobs[&job_id].spec.metadata),
        vec![FullFileName {
            name: disk_name,
            full_name: described_name,
        }]
    );
}

#[tokio::test]
async fn a_delivery_rename_reaches_the_finished_job_with_its_full_name() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.config.write().await.delivery_naming =
        Some(crate::settings::DeliveryNamingOverrides {
            deobfuscate_delivered_members: Some(true),
            enable_srrdb_lookup: None,
        });
    let job_id = JobId(31102);
    let job_name = "Silver Horizon: Part 2";
    let working_dir = intermediate_dir.join(format!(
        "{}.#{}",
        crate::jobs::working_dir::sanitize_dirname(job_name),
        job_id.0
    ));
    tokio::fs::create_dir_all(&working_dir).await.unwrap();
    std::fs::File::create(working_dir.join("Yb5drZSkNi20UCMkb.mkv"))
        .unwrap()
        .set_len(64 * 1024 * 1024)
        .unwrap();
    pipeline.jobs.insert(
        job_id,
        minimal_job_state(job_id, job_name, working_dir.clone()),
    );

    pipeline.start_move_to_complete(job_id).await.unwrap();
    let done = pipeline.move_done_rx.recv().await.unwrap();
    let dest = done.dest.clone();
    pipeline.handle_move_to_complete_done(done).await;

    assert!(dest.join("Silver Horizon_ Part 2.mkv").is_file());
    let finished = pipeline
        .finished_jobs
        .iter()
        .find(|job| job.job_id == job_id)
        .unwrap();
    assert_eq!(
        full_file_names(&finished.metadata),
        vec![FullFileName {
            name: "Silver Horizon_ Part 2.mkv".to_string(),
            full_name: "Silver Horizon: Part 2.mkv".to_string(),
        }]
    );
}
