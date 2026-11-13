// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

/// An isolated pipeline with no network providers and a fixed machine profile.
/// The caller owns its task and temporary directories.
pub async fn idle_pipeline(
    db: crate::Database,
    config: crate::settings::SharedConfig,
) -> (Pipeline, crate::SchedulerHandle, crate::SharedPipelineState) {
    use crate::runtime::system_profile::{
        CpuProfile, DiskProfile, FilesystemType, MemoryProfile, SimdSupport, StorageClass,
    };
    let settings = config.read().await.clone();
    let root = PathBuf::from(&settings.data_dir);
    let intermediate = settings
        .intermediate_dir
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("intermediate"));
    let complete = settings
        .complete_dir
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("complete"));
    let (events, _) = broadcast::channel(1024);
    let state = crate::SharedPipelineState::new(PipelineMetrics::new(), vec![]);
    let handle = crate::SchedulerHandle::new(events.clone(), state.clone());
    let pipeline = Pipeline::new(
        events,
        NntpClient::new(weaver_nntp::client::NntpClientConfig {
            servers: vec![],
            max_idle_age: Duration::from_secs(1),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(15),
        }),
        16 * 1024 * 1024,
        SystemProfile {
            cpu: CpuProfile {
                physical_cores: 4,
                logical_cores: 4,
                simd: SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
                guessed: false,
            },
            disk: DiskProfile {
                storage_class: StorageClass::Ssd,
                filesystem: FilesystemType::Apfs,
                sequential_write_mbps: 1000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        },
        root,
        intermediate,
        complete,
        0,
        4,
        vec![],
        false,
        state.clone(),
        db,
        config,
        crate::runtime::StartupDiskGate::opened(),
    )
    .await
    .expect("isolated pipeline initialization");
    (pipeline, handle, state)
}

/// Install a facade fixture through the real admission and runtime path.
pub async fn seed_job(pipeline: &mut Pipeline, job: crate::JobInfo) {
    let bytes = br#"<?xml version="1.0"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb"><file poster="fixture" date="1" subject="&quot;payload.bin&quot; yEnc (1/1)"><groups><group>alt.binaries.test</group></groups><segments><segment bytes="1024" number="1">fixture@example.invalid</segment></segments></file></nzb>"#.to_vec();
    let path = pipeline.nzb_dir.join(format!("{}.nzb", job.job_id.0));
    let nzb = crate::ingest::parse_persisted_nzb_bytes(&bytes).unwrap();
    let mut spec =
        crate::ingest::nzb_to_spec(&nzb, &path, job.category.clone(), job.metadata.clone());
    spec.name = job.name.clone();
    spec.password = job.password.clone();
    spec.total_bytes = job.total_bytes;
    pipeline
        .add_job(
            job.job_id,
            spec,
            path,
            bytes,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
    // A fixture past the queue window is its row alone.
    if let Some(state) = pipeline.jobs.get_mut(&job.job_id) {
        state.status = job.status.clone();
        state.download_state = job.download_state;
        state.post_state = job.post_state;
        state.run_state = job.run_state;
        state.downloaded_bytes = job.downloaded_bytes;
        state.failed_bytes = job.failed_bytes;
        if state.status == JobStatus::Paused {
            state.paused_resume_status = Some(JobStatus::Queued);
            state.paused_resume_download_state = Some(crate::DownloadState::Queued);
            state.paused_resume_post_state = Some(crate::PostState::Idle);
        }
    }
    // The row carries what the dispatcher would report for it, so a dispatch
    // pass finds nothing stale and never replaces the seeded job with the
    // pipeline's own report: no lane holds it, and its queued fixture segment
    // waits on whatever the dispatcher says.
    let id = job.job_id;
    let mut job = job;
    if job.download_wait_reason.is_none() {
        job.download_wait_reason = pipeline.job_dispatch_hold_reason(id).map(str::to_owned);
    }
    pipeline.shared_state.queue.write(|inner| {
        let row = inner
            .jobs
            .iter_mut()
            .find(|row| row.id == id)
            .expect("admitted fixture");
        row.report.job = job;
        row.report.download_active = false;
        queue::Change::changed(())
    });
}

/// Say a history entry's files were removed at `removed_at`, with `listing`
/// as the record of them, as a files removal writes it.
pub fn stamp_files_removed(
    db: &crate::Database,
    job_id: crate::JobId,
    listing: crate::history::output_files::OutputFileListing,
    removed_at: i64,
) {
    db.record_history_removed_files(&[(job_id, listing)])
        .unwrap();
    db.stamp_history_files_removed(&[job_id], removed_at)
        .unwrap();
}

/// Put `summary` on a finished job's history row, as the archive copies it
/// from the active row.
pub fn put_history_download_summary(
    db: &crate::Database,
    job_id: crate::JobId,
    summary: &crate::jobs::download_summary::DownloadSummary,
) {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
    let json = summary.to_storage_json().unwrap();
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::run_in_transaction(&datastore, "test_history_download_summary", |tx| {
            let json = json.clone();
            Box::pin(async move {
                tx.execute(
                    "UPDATE job_history SET download_summary = {} WHERE job_id = {}",
                    &[SqlArg::Text(json), SqlArg::I64(job_id.0 as i64)],
                )
                .await?;
                Ok(())
            })
        })
        .await
    })
    .unwrap();
}

/// Initialize real admission resources without running the data plane.
#[cfg(test)]
pub(crate) fn admission_resources(
    db: crate::Database,
    root: &std::path::Path,
) -> queue::AdmissionResources {
    queue::AdmissionResources {
        db,
        intermediate_dir: root.join("intermediate"),
        complete_dir: root.join("complete"),
        config: Arc::new(tokio::sync::RwLock::new(crate::settings::Config {
            data_dir: root.display().to_string(),
            hardware_profile: None,
            intermediate_dir: None,
            complete_dir: None,
            servers: vec![],
            categories: vec![],
            retry: None,
            max_download_speed: None,
            propagation_delay_secs: None,
            max_concurrent_nzbs: None,
            cleanup_after_extract: None,
            delete_failed_job_files: None,
            history_retention_days: None,
            watch_folder: Default::default(),
            duplicate_policy: Default::default(),
            direct_store: None,
            delivery_naming: None,
            metrics: Default::default(),
            config_path: None,
        })),
    }
}
