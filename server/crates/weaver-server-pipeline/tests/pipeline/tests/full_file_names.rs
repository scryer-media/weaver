// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_composition::JobId;
use weaver_server_pipeline::pipeline::tests::{
    delivery_fixture::*, history_fixture::finished_jobs, new_direct_pipeline,
};

use weaver_server_composition::ingest::{FullFileName, full_file_names};

#[tokio::test]
async fn a_delivery_rename_reaches_the_finished_job_with_its_full_name() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    enable_delivery_renaming(&mut pipeline).await;
    let job_id = JobId(31102);
    let job_name = "Silver Horizon: Part 2";
    let working_dir = intermediate_dir.join(format!(
        "{}.#{}",
        weaver_server_pipeline::jobs::working_dir::sanitize_dirname(job_name),
        job_id.0
    ));
    tokio::fs::create_dir_all(&working_dir).await.unwrap();
    std::fs::File::create(working_dir.join("Yb5drZSkNi20UCMkb.mkv"))
        .unwrap()
        .set_len(64 * 1024 * 1024)
        .unwrap();
    install_delivery_job(&mut pipeline, job_id, job_name, working_dir);
    let (dest, _) = finish_delivery(&mut pipeline, job_id).await;

    assert!(dest.join("Silver Horizon_ Part 2.mkv").is_file());
    let finished = finished_jobs(&mut pipeline)
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
