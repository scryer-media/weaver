// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

use crate::ingest::{FullFileName, full_file_names};

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
    pipeline.job_order.push(job_id);
    pipeline.report_all_for_test();

    pipeline.start_move_to_complete(job_id).await.unwrap();
    let done = pipeline.move_done_rx.recv().await.unwrap();
    let dest = done.dest.clone();
    pipeline.handle_move_to_complete_done(done).await;

    assert!(dest.join("Silver Horizon_ Part 2.mkv").is_file());
    let finished = pipeline
        .finished_jobs()
        .await
        .into_iter()
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
