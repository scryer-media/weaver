// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

pub async fn enable_delivery_renaming(pipeline: &mut Pipeline) {
    pipeline.config.write().await.delivery_naming =
        Some(crate::settings::DeliveryNamingOverrides {
            deobfuscate_delivered_members: Some(true),
            enable_srrdb_lookup: None,
        });
}

pub fn install_delivery_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job_name: &str,
    working_dir: PathBuf,
) {
    pipeline
        .jobs
        .insert(job_id, minimal_job_state(job_id, job_name, working_dir));
    pipeline.job_order.push(job_id);
    pipeline.report_all_for_test();
}

pub async fn finish_delivery(pipeline: &mut Pipeline, job_id: JobId) -> (PathBuf, bool) {
    pipeline.start_move_to_complete(job_id).await.unwrap();
    let done = pipeline.move_done_rx.recv().await.unwrap();
    let dest = done.dest.clone();
    let succeeded = done.result.is_ok();
    pipeline.handle_move_to_complete_done(done).await;
    settle_script_admissions(pipeline).await;

    (dest, succeeded)
}
