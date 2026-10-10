use super::*;

use crate::HardwareProfileInForce;
use crate::runtime::HardwareProfile;

/// Twelve cores and 8 GiB. Balanced is recommended; efficient differs from it
/// in every limit, pool size included; performance is out of reach for want
/// of memory.
fn twelve_core_machine() -> SystemProfile {
    SystemProfile {
        cpu: CpuProfile {
            physical_cores: 12,
            logical_cores: 24,
            simd: SimdSupport::default(),
            cgroup_limit: None,
        },
        memory: MemoryProfile {
            total_bytes: 8 * 1024 * 1024 * 1024,
            available_bytes: 8 * 1024 * 1024 * 1024,
            cgroup_limit: None,
        },
        disk: DiskProfile {
            storage_class: StorageClass::Ssd,
            filesystem: FilesystemType::Apfs,
            sequential_write_mbps: 1000.0,
            random_read_iops: 50_000.0,
            same_filesystem: true,
        },
    }
}

async fn pipeline_on_twelve_cores(temp_dir: &TempDir) -> Pipeline {
    let (pipeline, _, _) = new_direct_pipeline_on_machine(
        temp_dir.path().join("data"),
        temp_dir.path().join("intermediate"),
        temp_dir.path().join("complete"),
        temp_dir.path().join("weaver.db"),
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        20,
        None,
        twelve_core_machine(),
    )
    .await;
    pipeline
}

async fn choose(pipeline: &mut Pipeline, profile: HardwareProfile) {
    let (reply, received) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::SetHardwareProfile { profile, reply })
        .await;
    received.await.unwrap();
}

async fn schedule(pipeline: &mut Pipeline, profile: Option<HardwareProfile>) {
    let (reply, received) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::SetScheduledHardwareProfile { profile, reply })
        .await;
    received.await.unwrap();
}

/// Everything a profile decides, as the next activity would find it.
fn assert_limits_of(pipeline: &Pipeline, profile: HardwareProfile) {
    let tuning = profile.tuning(pipeline.tuner.system_profile());
    assert_eq!(pipeline.tuner.profile_tuning(), tuning);
    assert_eq!(
        pipeline.tuner.params().decode_thread_count,
        tuning.decode_threads
    );
    assert_eq!(
        pipeline.tuner.params().extract_thread_count,
        tuning.extract_threads
    );
    assert_eq!(
        pipeline.pp_pool.current_num_threads(),
        tuning.extract_threads
    );
    assert_eq!(
        pipeline.chase_pool.current_num_threads(),
        tuning.extract_threads
    );
    assert_eq!(
        pipeline.shared_state.sevenz_decode_memory_bytes(),
        tuning.sevenz_decode_memory_bytes
    );
    assert_eq!(
        pipeline.process_memory_budget.limit(),
        pipeline.extraction_limits.max_memory_bytes
    );
    // An operator's environment pin outranks every profile.
    if std::env::var_os("WEAVER_EXTRACTION_MAX_MEMORY_BYTES").is_none() {
        assert_eq!(
            pipeline.extraction_limits.max_memory_bytes,
            tuning.extraction_memory_bytes
        );
    }
}

#[tokio::test]
async fn a_chosen_profile_is_in_force_for_the_next_activity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = pipeline_on_twelve_cores(&temp_dir).await;
    assert_limits_of(&pipeline, HardwareProfile::Balanced);
    assert_eq!(pipeline.tuner.params().max_concurrent_downloads, 20);
    assert_eq!(pipeline.tuner.params().decode_thread_count, 4);
    assert_eq!(pipeline.pp_pool.current_num_threads(), 6);
    assert_eq!(
        pipeline.shared_state.hardware_profile_in_force(),
        Some(HardwareProfileInForce {
            active: HardwareProfile::Balanced,
            scheduled: None,
        })
    );
    // What an extraction that started before the change holds.
    let running_pool = Arc::clone(&pipeline.pp_pool);
    let running_budget = pipeline.process_memory_budget.for_job(1);

    choose(&mut pipeline, HardwareProfile::Efficient).await;

    assert_limits_of(&pipeline, HardwareProfile::Efficient);
    assert_eq!(pipeline.tuner.params().max_concurrent_downloads, 10);
    assert_eq!(pipeline.tuner.params().decode_thread_count, 2);
    assert_eq!(pipeline.pp_pool.current_num_threads(), 4);
    assert_eq!(
        pipeline.configured_hardware_profile,
        HardwareProfile::Efficient
    );
    assert_eq!(
        pipeline.shared_state.hardware_profile_in_force(),
        Some(HardwareProfileInForce {
            active: HardwareProfile::Efficient,
            scheduled: None,
        })
    );
    // Running work finishes on what it started with.
    assert!(!Arc::ptr_eq(&running_pool, &pipeline.pp_pool));
    assert_eq!(running_pool.current_num_threads(), 6);
    assert_ne!(
        running_budget.limit(),
        pipeline.process_memory_budget.limit()
    );
}

#[tokio::test]
async fn a_scheduled_profile_overrides_the_choice_until_the_schedule_lets_go() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = pipeline_on_twelve_cores(&temp_dir).await;

    // A scheduled speed limit is a separate track the profile rule must not
    // end.
    let (reply, received) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::ApplyScheduleAction {
            action: crate::bandwidth::ScheduleAction::SpeedLimit {
                limits: vec![crate::bandwidth::SpeedLimitChange {
                    target: crate::bandwidth::SpeedTarget::Global,
                    bytes_per_sec: 128 * 1024,
                }],
            },
            reply,
        })
        .await;
    received.await.unwrap();

    let (reply, received) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::ApplyScheduleAction {
            action: crate::bandwidth::ScheduleAction::HardwareProfile {
                profile: HardwareProfile::Efficient,
            },
            reply,
        })
        .await;
    received.await.unwrap();

    assert_limits_of(&pipeline, HardwareProfile::Efficient);
    assert_eq!(
        pipeline.shared_state.hardware_profile_in_force(),
        Some(HardwareProfileInForce {
            active: HardwareProfile::Efficient,
            scheduled: Some(HardwareProfile::Efficient),
        })
    );
    assert_eq!(pipeline.scheduled_rate_limit, Some(128 * 1024));
    assert_eq!(pipeline.rate_limiter.rate(), 128 * 1024);
    assert!(!pipeline.global_paused);

    // The operator's choice is recorded but waits for the schedule.
    choose(&mut pipeline, HardwareProfile::Balanced).await;
    assert_limits_of(&pipeline, HardwareProfile::Efficient);

    schedule(&mut pipeline, None).await;
    assert_limits_of(&pipeline, HardwareProfile::Balanced);
    assert_eq!(
        pipeline.shared_state.hardware_profile_in_force(),
        Some(HardwareProfileInForce {
            active: HardwareProfile::Balanced,
            scheduled: None,
        })
    );
}

#[tokio::test]
async fn a_scheduled_profile_the_machine_cannot_honour_is_skipped() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = pipeline_on_twelve_cores(&temp_dir).await;
    let pool = Arc::clone(&pipeline.pp_pool);

    schedule(&mut pipeline, Some(HardwareProfile::Performance)).await;

    assert_limits_of(&pipeline, HardwareProfile::Balanced);
    assert!(Arc::ptr_eq(&pool, &pipeline.pp_pool), "nothing was rebuilt");
    assert_eq!(
        pipeline.shared_state.hardware_profile_in_force(),
        Some(HardwareProfileInForce {
            active: HardwareProfile::Balanced,
            scheduled: None,
        })
    );

    // A profile it can honour still applies after the skip.
    schedule(&mut pipeline, Some(HardwareProfile::Efficient)).await;
    assert_limits_of(&pipeline, HardwareProfile::Efficient);
}
