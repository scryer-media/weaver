// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use bytes::Bytes;
use std::sync::atomic::Ordering;
use weaver_server_base::events::model::PipelineEvent;
use weaver_server_base::runtime::system_profile::{
    CpuProfile, DiskProfile, FilesystemType, MemoryProfile, SimdSupport, StorageClass,
    SystemProfile,
};
use weaver_server_pipeline::pipeline::tests::{
    decode_fixture::*, new_direct_pipeline_with_backlog, uu_fixture::set_write_budget,
};
use weaver_server_pipeline::pipeline::tests::{
    drain_job_events,
    history_fixture::database,
    queue_fixture::{apply_intents, fail_job, handle, report_all, report_job},
    settle_failed_releases,
    uu_fixture::{backlog, has_job, job_status},
    yenc_fixture::held_parts,
};
use weaver_server_pipeline::pipeline::tests::{
    history_fixture::set_installed_status,
    insert_active_job, new_direct_pipeline,
    queue_fixture::{installed_status, metrics},
    segmented_job_spec, standalone_job_spec, standalone_with_par2_job_spec,
    uu_fixture::refresh_download_pressure,
    wait_until,
    yenc_fixture::{next_retry, set_retention},
};
use weaver_server_pipeline::{
    DownloadPressureReason, DownloadPressureState, JobId, JobStatus, NzbFileId, SegmentId,
};
use weaver_server_pipeline::{Pipeline, settings::queries::SettingsQueries};

#[test]
fn hdd_profile_allocates_more_write_backlog_than_ssd() {
    let profile = SystemProfile {
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
    };
    let mut hdd_profile = profile.clone();
    hdd_profile.disk.storage_class = StorageClass::Hdd;
    let backlog_base_bytes = 48 * 1024 * 1024;

    let ssd_budget = compute_write_backlog_budget_bytes(&profile, backlog_base_bytes);
    let hdd_budget = compute_write_backlog_budget_bytes(&hdd_profile, backlog_base_bytes);

    assert!(hdd_budget > ssd_budget);
}

// A storage class learned after start-up reaches the tuner's machine and
// sizes the backlog limits again, on the pipeline's own turn, once.
#[tokio::test]
async fn a_storage_class_learned_after_start_up_sizes_the_backlog_again() {
    let temp_dir = tempfile::tempdir().unwrap();
    let backlog_base_bytes = 48 * 1024 * 1024;
    let (mut pipeline, _, _) =
        new_direct_pipeline_with_backlog(&temp_dir, backlog_base_bytes, 4).await;
    let ssd_write = write_limit(&pipeline);
    let ssd_spool = spool_limit(&pipeline);
    let mut hdd = system_profile(&pipeline).clone();
    hdd.disk.storage_class = StorageClass::Hdd;
    hdd.disk.filesystem = FilesystemType::Ext4;
    let hdd_write = compute_write_backlog_budget_bytes(&hdd, backlog_base_bytes);
    assert!(hdd_write > ssd_write);

    set_storage_classification(&pipeline, StorageClass::Hdd, FilesystemType::Ext4);
    run_queue_intents(&mut pipeline).await;

    let disk = &system_profile(&pipeline).disk;
    assert_eq!(disk.storage_class, StorageClass::Hdd);
    assert_eq!(disk.filesystem, FilesystemType::Ext4);
    assert_eq!(write_limit(&pipeline), hdd_write);
    assert_eq!(
        decode_limit(&pipeline),
        compute_decode_backlog_budget_bytes(&hdd, backlog_base_bytes, hdd_write)
    );
    assert!(
        spool_limit(&pipeline) > ssd_spool,
        "the spool follows the write limit"
    );

    // Applied once: a later turn leaves limits set since alone.
    set_write_budget(&mut pipeline, 1);
    run_queue_intents(&mut pipeline).await;
    assert_eq!(write_limit(&pipeline), 1);
}

#[test]
fn decode_backlog_budget_is_separate_bounded_raw_article_cache() {
    let profile = SystemProfile {
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
    };
    let backlog_base_bytes = 144 * 1024 * 1024;

    let write_budget = compute_write_backlog_budget_bytes(&profile, backlog_base_bytes);
    let decode_budget =
        compute_decode_backlog_budget_bytes(&profile, backlog_base_bytes, write_budget);

    assert!(decode_budget > write_budget);
    // Memory-scaled: available/12 dominates the tier's base term on an 8 GiB
    // machine, bounded by available/8.
    assert_eq!(decode_budget, 8 * 1024 * 1024 * 1024 / 12);
}

#[test]
fn decode_backlog_budget_respects_effective_memory_cap() {
    let profile = SystemProfile {
        cpu: CpuProfile {
            physical_cores: 2,
            logical_cores: 2,
            simd: SimdSupport::default(),
            cgroup_limit: None,
        },
        memory: MemoryProfile {
            total_bytes: 512 * 1024 * 1024,
            available_bytes: 512 * 1024 * 1024,
            cgroup_limit: Some(512 * 1024 * 1024),
            guessed: false,
        },
        disk: DiskProfile {
            storage_class: StorageClass::Ssd,
            filesystem: FilesystemType::Ext4,
            sequential_write_mbps: 250.0,
            random_read_iops: 5_000.0,
            same_filesystem: true,
        },
    };
    let backlog_base_bytes = 24 * 1024 * 1024;

    let write_budget = compute_write_backlog_budget_bytes(&profile, backlog_base_bytes);
    let decode_budget =
        compute_decode_backlog_budget_bytes(&profile, backlog_base_bytes, write_budget);

    assert_eq!(decode_budget, 64 * 1024 * 1024);
}

#[test]
fn backlog_budgets_share_one_memory_ceiling() {
    const GIB: u64 = 1024 * 1024 * 1024;
    let host = SystemProfile {
        cpu: CpuProfile {
            physical_cores: 8,
            logical_cores: 8,
            simd: SimdSupport::default(),
            cgroup_limit: None,
        },
        memory: MemoryProfile {
            total_bytes: 16 * GIB,
            available_bytes: 16 * GIB,
            cgroup_limit: None,
            guessed: false,
        },
        disk: DiskProfile {
            storage_class: StorageClass::Ssd,
            filesystem: FilesystemType::Ext4,
            sequential_write_mbps: 1000.0,
            random_read_iops: 50_000.0,
            same_filesystem: true,
        },
    };
    let mut boxed = host.clone();
    boxed.memory.cgroup_limit = Some(GIB);
    let mut small_host = host.clone();
    small_host.memory.available_bytes = GIB;
    // A 384 MiB base: above an eighth of the container's limit, well
    // below an eighth of the host's memory.
    let backlog_base_bytes = 384 * 1024 * 1024;
    let budgets = |profile: &SystemProfile| {
        let write = compute_write_backlog_budget_bytes(profile, backlog_base_bytes);
        let decode = compute_decode_backlog_budget_bytes(profile, backlog_base_bytes, write);
        (write, decode)
    };

    // Without a container limit the host figure decides, as before.
    assert_eq!(
        budgets(&host),
        (384 * 1024 * 1024, (16 * GIB / 12) as usize)
    );
    // A container limit scales both budgets down from the same ceiling the
    // host would have with that much memory.
    assert_eq!(budgets(&boxed), ((GIB / 8) as usize, (GIB / 8) as usize));
    assert_eq!(budgets(&boxed), budgets(&small_host));
}

// The backlog limits and the pending writes per file come out of the memory
// tier exactly as they did when the tier sized an article buffer pool and the
// limits were read off that pool's size. The old arithmetic is copied here as
// the reference, so any drift in the tier table or the limit formulas fails.
#[test]
fn backlog_sizing_matches_the_former_pool_sized_limits_on_every_host() {
    use weaver_server_base::runtime::backlog_sizing::BacklogSizing;

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;

    // The former pool: buffer counts per tier by memory.
    fn old_counts(available_bytes: u64, cgroup_limit: Option<u64>) -> (usize, usize, usize) {
        const UNCAPPED_HOST_MEMORY_CAP_BYTES: u64 = (8 * 1024 * 1024 * 1024) - 1;
        let sizing =
            cgroup_limit.unwrap_or_else(|| available_bytes.min(UNCAPPED_HOST_MEMORY_CAP_BYTES));
        let available_mb = (sizing / (1024 * 1024)) as usize;
        if available_mb < 1024 {
            (32, 4, 1)
        } else if available_mb < 2048 {
            (64, 8, 2)
        } else if available_mb < 4096 {
            (128, 16, 4)
        } else if available_mb < 8192 {
            (192, 24, 6)
        } else if available_mb < 16384 {
            (256, 32, 8)
        } else if available_mb < 32768 {
            (384, 48, 12)
        } else {
            (512, 64, 16)
        }
    }
    fn old_pool_total_bytes((small, medium, large): (usize, usize, usize)) -> usize {
        small * 512 * 1024 + medium * 1024 * 1024 + large * 4 * 1024 * 1024
    }
    fn old_write_max_pending((small, _, _): (usize, usize, usize)) -> usize {
        if small <= 64 {
            4
        } else if small <= 128 {
            8
        } else {
            16
        }
    }
    fn old_ceiling(profile: &SystemProfile) -> usize {
        let available_bytes = profile.memory.available_bytes.max(256 * 1024 * 1024);
        match profile.memory.cgroup_limit {
            Some(cgroup_limit) => available_bytes.min(cgroup_limit) as usize,
            None => available_bytes as usize,
        }
    }
    fn old_write_budget(profile: &SystemProfile, scratch_bytes: usize) -> usize {
        let available_bytes = old_ceiling(profile);
        let base = scratch_bytes
            .max(64 * 1024 * 1024)
            .min((available_bytes / 8).max(64 * 1024 * 1024));
        match profile.disk.storage_class {
            StorageClass::Ssd => base,
            StorageClass::Hdd => (base.saturating_mul(2)).min((available_bytes / 4).max(base)),
            StorageClass::Network | StorageClass::Unknown => {
                (base.saturating_mul(3) / 2).min((available_bytes / 5).max(base))
            }
        }
    }
    fn old_decode_budget(profile: &SystemProfile, scratch_bytes: usize, write: usize) -> usize {
        let available_bytes = old_ceiling(profile);
        let scaled = available_bytes / 12;
        let memory_cap = (available_bytes / 8).max(write).min(4 * 1024 * 1024 * 1024);
        let target = scratch_bytes.saturating_mul(3).max(scaled).max(write);
        target.min(memory_cap).max(1)
    }

    let mut checked = 0;
    for available in [
        512 * MIB,
        GIB,
        2 * GIB,
        4 * GIB,
        8 * GIB,
        16 * GIB,
        32 * GIB,
        64 * GIB,
    ] {
        for cgroup_limit in [
            None,
            Some(GIB),
            Some(4 * GIB),
            Some(16 * GIB),
            Some(64 * GIB),
        ] {
            for storage_class in [StorageClass::Ssd, StorageClass::Hdd, StorageClass::Network] {
                let profile = SystemProfile {
                    cpu: CpuProfile {
                        physical_cores: 4,
                        logical_cores: 4,
                        simd: SimdSupport::default(),
                        cgroup_limit: None,
                    },
                    memory: MemoryProfile {
                        total_bytes: available,
                        available_bytes: available,
                        cgroup_limit,
                        guessed: false,
                    },
                    disk: DiskProfile {
                        storage_class: storage_class.clone(),
                        filesystem: FilesystemType::Ext4,
                        sequential_write_mbps: 500.0,
                        random_read_iops: 10_000.0,
                        same_filesystem: true,
                    },
                };
                let case = format!("{available} bytes, limit {cgroup_limit:?}, {storage_class:?}");

                let counts = old_counts(available, cgroup_limit);
                let old_scratch = old_pool_total_bytes(counts);
                let old_write = old_write_budget(&profile, old_scratch);
                let old_decode = old_decode_budget(&profile, old_scratch, old_write);

                let sizing = BacklogSizing::for_runtime_memory(available, cgroup_limit);
                let write = compute_write_backlog_budget_bytes(&profile, sizing.base_bytes);
                let decode =
                    compute_decode_backlog_budget_bytes(&profile, sizing.base_bytes, write);

                assert_eq!(sizing.base_bytes, old_scratch, "{case}");
                assert_eq!(write, old_write, "write backlog limit, {case}");
                assert_eq!(decode, old_decode, "decode backlog limit, {case}");
                assert_eq!(
                    sizing.pending_writes_per_file,
                    old_write_max_pending(counts),
                    "pending writes per file, {case}"
                );
                checked += 1;
            }
        }
    }
    assert_eq!(checked, 8 * 5 * 3);
}

#[tokio::test]
async fn pump_decode_queue_releases_bytes_for_inactive_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20027);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Inactive Decode Discard",
            &[("discard.bin".to_string(), 512u32)],
        ),
    )
    .await;

    let raw = Bytes::from_static(b"queued raw article");
    let raw_size = raw.len() as u64;
    metrics(&pipeline).note_decode_work_queued(raw_size);
    queue_decode(
        &mut pipeline,
        SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number: 0,
        },
        raw,
    );
    set_installed_status(
        &mut pipeline,
        job_id,
        JobStatus::Failed {
            error: "inactive".to_string(),
        },
    );

    pump(&mut pipeline);

    assert!(pending_count(&pipeline) == 0);
    assert_eq!(metrics(&pipeline).decode_pending.load(Ordering::Relaxed), 0);
    assert_eq!(
        metrics(&pipeline)
            .decode_pending_bytes
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        metrics(&pipeline)
            .decode_active_bytes
            .load(Ordering::Relaxed),
        0
    );
}

// `weaver_pipeline_decode_task_duration_seconds` is absent until the decode
// path has timed something, then reports exactly one observation per decode
// task — the single clock read the task is allowed, taken once at its end
// whichever way the task exits.
#[tokio::test]
async fn decode_tasks_record_one_wall_duration_each() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20033);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Silver Horizon Decode Timing",
            &[("silver-horizon.bin".to_string(), 512u32)],
        ),
    )
    .await;

    assert!(
        metrics(&pipeline)
            .pipeline_histograms
            .snapshot()
            .decode_task_duration
            .is_none(),
        "the histogram must be absent, not an all-zero series, before the first task"
    );

    // The payload is not valid yEnc, so both tasks take the decode-failure
    // exit. That is deliberate: the measurement has to close on every path out
    // of the task, not only the happy one.
    for segment_number in 0..2u32 {
        let raw = Bytes::from_static(b"not a yenc article");
        metrics(&pipeline).note_decode_work_queued(raw.len() as u64);
        queue_decode(
            &mut pipeline,
            SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number,
            },
            raw,
        );
    }

    pump(&mut pipeline);
    assert!(pending_count(&pipeline) == 0);

    let metrics = metrics(&pipeline);
    wait_until(|| {
        metrics
            .pipeline_histograms
            .snapshot()
            .decode_task_duration
            .is_some_and(|histogram| histogram.count == 2)
    })
    .await;
}

#[tokio::test]
async fn pump_decode_queue_respects_decode_thread_limit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20031);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Decode Thread Limit",
            &[(("limited.bin").to_string(), 512u32)],
        ),
    )
    .await;

    let decode_limit = decode_threads(&pipeline);
    let raw_size = 32usize;
    for segment_number in 0..(decode_limit + 3) {
        let raw = Bytes::from(vec![segment_number as u8; raw_size]);
        metrics(&pipeline).note_decode_work_queued(raw.len() as u64);
        queue_decode(
            &mut pipeline,
            SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number: segment_number as u32,
            },
            raw,
        );
    }

    pump(&mut pipeline);

    assert_eq!(sum_active(&pipeline), decode_limit);
    assert_eq!(pending_count(&pipeline), 3);
    assert_eq!(metrics(&pipeline).decode_pending.load(Ordering::Relaxed), 3);
    assert_eq!(
        metrics(&pipeline)
            .decode_pending_bytes
            .load(Ordering::Relaxed),
        (3 * raw_size) as u64
    );
    assert_eq!(
        metrics(&pipeline)
            .decode_active_bytes
            .load(Ordering::Relaxed),
        (decode_limit * raw_size) as u64
    );
}

// The active-decode gauge is published on every decode start and finish, so
// it reads the kept sum instead of walking every job's count each time.
#[tokio::test]
async fn the_active_decode_gauge_reads_the_kept_sum_without_walking_jobs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let segment = |job: u64, segment_number: u32| SegmentId {
        file_id: NzbFileId {
            job_id: JobId(job),
            file_index: 0,
        },
        segment_number,
    };

    let walks = walk_count(&pipeline);
    for (job, segment_number) in [(20041, 0), (20041, 1), (20042, 0)] {
        note_started(&mut pipeline, segment(job, segment_number), 64);
    }
    assert_eq!(metrics(&pipeline).active_decodes.load(Ordering::Relaxed), 3);
    remove_active(&mut pipeline, JobId(20041));
    publish_active(&pipeline);
    assert_eq!(metrics(&pipeline).active_decodes.load(Ordering::Relaxed), 1);
    assert_eq!(
        walk_count(&pipeline),
        walks,
        "publishing the gauge must not walk the per-job counts"
    );
    assert_eq!(sum_active(&pipeline), 1);
}

#[tokio::test]
async fn hard_decode_pressure_latches_until_soft_watermark() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    set_decode_limit(&mut pipeline, 1000);

    metrics(&pipeline)
        .decode_pending_bytes
        .store(1000, Ordering::Relaxed);
    refresh_download_pressure(&mut pipeline);
    assert_eq!(
        metrics(&pipeline)
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Hard.as_code()
    );
    assert_eq!(
        metrics(&pipeline)
            .download_pressure_reason
            .load(Ordering::Relaxed),
        DownloadPressureReason::Decode.as_code()
    );

    metrics(&pipeline)
        .decode_pending_bytes
        .store(701, Ordering::Relaxed);
    refresh_download_pressure(&mut pipeline);
    assert_eq!(
        metrics(&pipeline)
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Hard.as_code()
    );
    assert_eq!(
        metrics(&pipeline)
            .download_pressure_stalls_total
            .load(Ordering::Relaxed),
        1
    );

    metrics(&pipeline)
        .decode_pending_bytes
        .store(699, Ordering::Relaxed);
    refresh_download_pressure(&mut pipeline);
    assert_eq!(
        metrics(&pipeline)
            .download_pressure_state
            .load(Ordering::Relaxed),
        DownloadPressureState::Clear.as_code()
    );
    assert_eq!(
        metrics(&pipeline)
            .download_pressure_reason
            .load(Ordering::Relaxed),
        DownloadPressureReason::None.as_code()
    );
}

#[test]
fn decode_retry_exclude_servers_appends_actual_source_server_once() {
    assert_eq!(
        retry_exclusions(weaver_nntp::ServerSet::EMPTY, Some(1)),
        vec![1]
    );
    assert_eq!(
        retry_exclusions(weaver_nntp::ServerSet::from([1]), Some(1)),
        vec![1]
    );
    assert_eq!(
        retry_exclusions(weaver_nntp::ServerSet::from([1]), Some(0)),
        vec![1, 0]
    );
    assert_eq!(
        retry_exclusions(weaver_nntp::ServerSet::from([1, 0]), None),
        vec![1, 0]
    );
}

#[tokio::test]
async fn repeated_data_decode_failures_mark_failed_bytes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20016);
    let spec = segmented_job_spec("Repeated Decode Failure", "broken.bin", &[64, 4096]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };

    for _ in 0..=MAX_RETRIES {
        decode_failure(
            &mut pipeline,
            segment_id,
            "crc mismatch",
            Some(0),
            weaver_nntp::ServerSet::EMPTY,
        );
    }

    assert_eq!(installed_failed_bytes(&pipeline, job_id), Some(64));
}

#[tokio::test]
async fn single_server_decode_failure_gives_the_article_up_for_repair() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    set_retention(&mut pipeline, &[0]);
    let job_id = JobId(20017);
    let spec = segmented_job_spec("Single Server Decode Failure", "broken.bin", &[64, 4096]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };

    // The only server produced a bad body: there is nowhere left to retry,
    // so the article fails now instead of queueing work no lane may serve.
    decode_failure(
        &mut pipeline,
        segment_id,
        "crc mismatch",
        Some(0),
        weaver_nntp::ServerSet::EMPTY,
    );

    assert_eq!(installed_failed_bytes(&pipeline, job_id), Some(64));
    assert_eq!(
        metrics(&pipeline).segments_retried.load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        metrics(&pipeline)
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        1
    );
    assert!(!pending_retry(&pipeline, segment_id));
    assert_eq!(
        installed_status(&pipeline, job_id).cloned(),
        Some(JobStatus::Downloading)
    );
}

#[tokio::test]
async fn decode_failure_moves_to_the_remaining_server_then_gives_up() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    set_retention(&mut pipeline, &[0, 0]);
    let job_id = JobId(20018);
    let spec = segmented_job_spec("Two Server Decode Failure", "broken.bin", &[64, 4096]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };

    // Server 0 produced a bad body; server 1 is still untried, so the
    // article moves there.
    decode_failure(
        &mut pipeline,
        segment_id,
        "crc mismatch",
        Some(0),
        weaver_nntp::ServerSet::EMPTY,
    );
    let work = next_retry(&mut pipeline).await;
    assert_eq!(work.exclude_servers, vec![0]);
    assert_eq!(installed_failed_bytes(&pipeline, job_id), Some(0));

    // Server 1 produced a bad body too: every server has been tried, so the
    // article fails well inside the retry budget.
    decode_failure(
        &mut pipeline,
        segment_id,
        "crc mismatch",
        Some(1),
        work.exclude_servers,
    );
    assert_eq!(installed_failed_bytes(&pipeline, job_id), Some(64));
    assert_eq!(
        metrics(&pipeline).segments_retried.load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        metrics(&pipeline)
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn recovery_decode_failures_do_not_mark_health_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20013);
    let spec = standalone_with_par2_job_spec("Recovery Decode Failure", 128, 64);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 2,
        },
        segment_number: 0,
    };

    for _ in 0..=MAX_RETRIES {
        decode_failure(
            &mut pipeline,
            segment_id,
            "crc mismatch",
            None,
            weaver_nntp::ServerSet::EMPTY,
        );
    }

    assert_eq!(installed_failed_bytes(&pipeline, job_id), Some(0));
    assert_eq!(
        installed_status(&pipeline, job_id).cloned(),
        Some(JobStatus::Downloading)
    );
}

#[tokio::test]
async fn decode_failure_drains_backlog_and_keeps_commands_responsive() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_backlog(&temp_dir, 5632 * 1024, 4).await;
    let job_id = JobId(20003);
    let files = vec![("broken.bin".to_string(), 64u32)];
    let spec = standalone_job_spec("Decode Failure", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let raw = Bytes::from_static(b"not a yenc article");
    let raw_size = raw.len() as u64;
    raw_download(&mut pipeline, segment_id, raw, None).await;

    assert_eq!(metrics(&pipeline).decode_pending.load(Ordering::Relaxed), 0);
    assert_eq!(
        metrics(&pipeline)
            .decode_pending_bytes
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        metrics(&pipeline)
            .decode_active_bytes
            .load(Ordering::Relaxed),
        raw_size
    );

    let done = next_completion(&mut pipeline).await;
    let (failed_segment, failed_raw_size, _) = done.failure();
    assert_eq!(failed_segment, segment_id);
    assert_eq!(failed_raw_size, raw_size);

    done.apply(&mut pipeline).await;

    assert_eq!(metrics(&pipeline).decode_pending.load(Ordering::Relaxed), 0);
    assert_eq!(
        metrics(&pipeline)
            .decode_pending_bytes
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        metrics(&pipeline)
            .decode_active_bytes
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(metrics(&pipeline).decode_errors.load(Ordering::Relaxed), 1);

    report_all(&pipeline);
    handle(&pipeline).pause_all().await.unwrap();
    apply_intents(&mut pipeline).await;
    assert!(is_paused(&pipeline));
    database(&pipeline).flush_write_queue().await.unwrap();
    assert_eq!(
        database(&pipeline)
            .get_setting("global_paused")
            .unwrap()
            .as_deref(),
        Some("true")
    );

    report_all(&pipeline);
    handle(&pipeline).resume_all().await.unwrap();
    apply_intents(&mut pipeline).await;
    assert!(!is_paused(&pipeline));
    database(&pipeline).flush_write_queue().await.unwrap();
    assert_eq!(
        database(&pipeline)
            .get_setting("global_paused")
            .unwrap()
            .as_deref(),
        Some("false")
    );
}

#[tokio::test]
async fn decode_failure_retries_excluding_actual_source_server() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20014);
    let files = vec![("broken.bin".to_string(), 64u32)];
    let spec = standalone_job_spec("Decode Failure Retry", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    raw_download(
        &mut pipeline,
        segment_id,
        Bytes::from_static(b"not a yenc article"),
        Some(0),
    )
    .await;

    let done = next_completion(&mut pipeline).await;
    let (failed_segment, _, servers) = done.failure();
    assert_eq!(failed_segment, segment_id);
    assert_eq!(
        servers,
        source(&pipeline, Some(0), weaver_nntp::ServerSet::EMPTY)
    );

    done.apply(&mut pipeline).await;

    assert_eq!(installed_failed_bytes(&pipeline, job_id), Some(0));

    let work = next_retry(&mut pipeline).await;
    assert_eq!(work.exclude_servers, vec![0]);
}

// A decoded article does not build its job's report: the job is marked, and
// the snapshot tick publishes the report once. The job's last decode
// publishes at once.
#[tokio::test]
async fn a_decoded_article_leaves_its_jobs_report_to_the_tick_until_the_last() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20031);
    let files = vec![
        ("one.bin".to_string(), 64u32),
        ("two.bin".to_string(), 64u32),
    ];
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Report Per Tick", &files),
    )
    .await;

    for file_index in 0..2 {
        let segment_id = SegmentId {
            file_id: NzbFileId { job_id, file_index },
            segment_number: 0,
        };
        raw_download(
            &mut pipeline,
            segment_id,
            Bytes::from_static(b"not a yenc article"),
            Some(0),
        )
        .await;
    }
    let first = next_completion(&mut pipeline).await;
    let last = next_completion(&mut pipeline).await;
    let published = |p: &Pipeline| reported_progress(p, job_id).0;

    first.apply(&mut pipeline).await;
    assert!(reports_due(&pipeline).contains(&job_id));
    // The figure moves; the row says what it said until the tick.
    let before = published(&pipeline);
    set_failed_bytes(&mut pipeline, job_id, before + 7);
    assert_eq!(published(&pipeline), before);
    refresh_snapshot(&mut pipeline);
    assert!(reports_due(&pipeline).is_empty());
    assert_eq!(published(&pipeline), before + 7);

    set_failed_bytes(&mut pipeline, job_id, before + 9);
    last.apply(&mut pipeline).await;
    assert!(reports_due(&pipeline).is_empty());
    assert_eq!(published(&pipeline), before + 9);
}

// A result or retry that only moves a job's figures leaves its report to the
// tick; one that moves its stage publishes at once. A job with no report
// writes nothing.
#[tokio::test]
async fn a_report_soon_defers_figures_and_publishes_a_stage_at_once() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20032);
    let files = vec![("one.bin".to_string(), 64u32)];
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Report Soon", &files),
    )
    .await;
    report_job(&mut pipeline, job_id);
    let published = |p: &Pipeline| reported_progress(p, job_id);
    let (before, _) = published(&pipeline);

    set_failed_bytes(&mut pipeline, job_id, before + 5);
    report_soon(&mut pipeline, job_id);
    assert!(reports_due(&pipeline).contains(&job_id));
    assert_eq!(published(&pipeline), (before, false));

    set_finalizing(&mut pipeline, job_id);
    report_soon(&mut pipeline, job_id);
    assert!(reports_due(&pipeline).is_empty());
    assert_eq!(published(&pipeline), (before + 5, true));

    let revision = queue_revision(&pipeline);
    report_job(&mut pipeline, JobId(20033));
    assert_eq!(queue_revision(&pipeline), revision);
}

#[tokio::test]
async fn streamed_decode_failure_retries_excluding_actual_source_server() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20018);
    let files = vec![("broken-streamed.bin".to_string(), 64u32)];
    let spec = standalone_job_spec("Streamed Decode Failure Retry", &files);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    submit_streamed_failure(&mut pipeline, segment_id).await;

    assert_eq!(metrics(&pipeline).decode_errors.load(Ordering::Relaxed), 1);
    assert!(no_completion_ready(&mut pipeline));

    let work = next_retry(&mut pipeline).await;
    assert_eq!(work.exclude_servers, vec![0]);
}

#[tokio::test]
async fn queued_yenc_layout_mismatch_retries_before_decode_acceptance() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20915);
    let filename = "queued-layout.bin";
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Queued Layout Mismatch", &[(filename.to_string(), 4)]),
    )
    .await;
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let mut events = handle(&pipeline).subscribe_events();
    submit_queued_layout_mismatch(&mut pipeline, segment_id, filename).await;

    assert_eq!(metrics(&pipeline).bytes_decoded.load(Ordering::Relaxed), 0);
    assert_eq!(
        metrics(&pipeline).segments_decoded.load(Ordering::Relaxed),
        0
    );
    assert_eq!(metrics(&pipeline).decode_errors.load(Ordering::Relaxed), 1);
    assert_eq!(downloaded_bytes(&pipeline, job_id), Some(0));
    assert!(!held_file(&pipeline, segment_id.file_id));
    assert!(!working_dir.join(filename).exists());
    assert!(
        !drain_job_events(&mut events, job_id)
            .iter()
            .any(|event| matches!(event, PipelineEvent::SegmentDecoded { .. }))
    );

    let work = next_retry(&mut pipeline).await;
    assert_eq!(work.exclude_servers, vec![2, 1]);
}

#[tokio::test]
async fn fused_yenc_layout_mismatch_retries_before_decode_acceptance() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20916);
    let filename = "fused-layout.bin";
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Fused Layout Mismatch", &[(filename.to_string(), 4)]),
    )
    .await;
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };
    let mut events = handle(&pipeline).subscribe_events();
    submit_fused_layout_mismatch(&mut pipeline, segment_id, filename).await;

    assert_eq!(metrics(&pipeline).bytes_decoded.load(Ordering::Relaxed), 0);
    assert_eq!(
        metrics(&pipeline).segments_decoded.load(Ordering::Relaxed),
        0
    );
    assert_eq!(metrics(&pipeline).decode_errors.load(Ordering::Relaxed), 1);
    assert_eq!(downloaded_bytes(&pipeline, job_id), Some(0));
    assert!(!held_file(&pipeline, segment_id.file_id));
    assert!(!working_dir.join(filename).exists());
    assert!(
        !drain_job_events(&mut events, job_id)
            .iter()
            .any(|event| matches!(event, PipelineEvent::SegmentDecoded { .. }))
    );

    let work = next_retry(&mut pipeline).await;
    assert_eq!(work.exclude_servers, vec![3, 2]);
}

// A body fetched under one pool and decoded after a rebuild that reordered
// the servers and added one: the indices it carries name the old pool's
// servers and are carried to the new pool by server, so its decode failure
// is retried on the one server that has not had it, never judged against
// the old indices. Read as current, "server 1 failed, server 0 excluded"
// would name the wrong servers; dropped, it would ask both again.
#[tokio::test]
async fn a_decode_failure_after_a_rebuild_is_retried_on_the_new_pool() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    set_retention(&mut pipeline, &[0, 0]);
    let job_id = JobId(20019);
    let spec = segmented_job_spec("Rebuilt Pool Decode Failure", "broken.bin", &[64, 4096]);
    insert_active_job(&mut pipeline, job_id, spec).await;
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    };

    // Fetched from server 1 with server 0 already excluded.
    let fetched = source(&pipeline, Some(1), weaver_nntp::ServerSet::from([0]));
    // The pool is rebuilt as old 1, old 0, and a new server.
    let rebuilt_generation = generation(&pipeline) + 1;
    rebuild(&mut pipeline, &[1, 0, 2], 4, rebuilt_generation);
    assert_eq!(
        current_source(&pipeline, fetched),
        (Some(0), weaver_nntp::ServerSet::from([1]))
    );

    fail_from(&mut pipeline, segment_id, "crc mismatch", fetched);

    let (retry_generation, work) = next_generation_retry(&mut pipeline).await;
    assert_eq!(retry_generation, rebuilt_generation);
    assert_eq!(work.exclude_servers, [0, 1]);
    assert_eq!(installed_failed_bytes(&pipeline, job_id), Some(0));
    assert_eq!(
        metrics(&pipeline)
            .segments_failed_permanent
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn fail_job_clears_write_backlog_accounting() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20005);
    let spec = standalone_job_spec("Fail Clears Backlog", &[("stalled.bin".to_string(), 64u32)]);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    // A decoded part held for the part before it, the write backlog's charge
    // for it booked as the park books it.
    park_for_backlog(&mut pipeline, file_id);
    assert_eq!(held_parts(&pipeline), 1);

    fail_job(&mut pipeline, job_id, "forced failure".to_string());

    settle_failed_releases(&mut pipeline).await;

    assert!(!held_file(&pipeline, file_id));
    assert_eq!(held_parts(&pipeline), 0);
    assert_eq!(backlog(&pipeline).write_buffered_bytes, 0);
    assert_eq!(backlog(&pipeline).write_buffered_segments, 0);
    assert_eq!(
        metrics(&pipeline)
            .write_buffered_bytes
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        metrics(&pipeline)
            .write_buffered_segments
            .load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn disk_write_failure_fails_job_before_commit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(20016);
    let spec = standalone_job_spec("Disk Write Failure", &[("blocked.bin".to_string(), 4u32)]);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    tokio::fs::create_dir(working_dir.join("blocked.bin"))
        .await
        .unwrap();

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    write_blocked_file(&mut pipeline, file_id).await;

    let status = job_status(&pipeline, job_id).unwrap();
    assert!(
        matches!(
            &status,
            JobStatus::Failed { error }
                if error.contains("write path failed") && error.contains("blocked.bin")
        ),
        "{status:?}"
    );
    settle_failed_releases(&mut pipeline).await;
    assert!(!has_job(&pipeline, job_id));
    assert!(working_dir.join("blocked.bin").is_dir());
}
