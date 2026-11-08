// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

use crate::HardwareProfileInForce;
use crate::runtime::HardwareProfile;
use weaver_graph::Budget;

use crate::pipeline::graph::hardware::Grant;

// Twelve cores and 8 GiB. Balanced is recommended; efficient differs from it
// in every limit, pool size included; performance is out of reach for want
// of memory.
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
    pipeline.report_all_for_test();
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
        .set_hardware_profile(profile)
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
}

async fn schedule(pipeline: &mut Pipeline, profile: Option<HardwareProfile>) {
    pipeline.report_all_for_test();
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
        .set_scheduled_hardware_profile(profile)
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
}

// Everything a profile decides, as the next activity would find it.
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
    let grant = pipeline.graph_hardware.grant();
    assert_eq!(pool_size(&grant), tuning.extract_threads);
    assert_eq!(
        lent(&grant.run),
        (
            Some(tuning.sevenz_decode_memory_bytes),
            tuning.extract_threads,
            true
        )
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
    assert_eq!(pool_size(&pipeline.graph_hardware.grant()), 6);
    assert_eq!(
        pipeline.shared_state.hardware_profile_in_force(),
        Some(HardwareProfileInForce {
            active: HardwareProfile::Balanced,
            scheduled: None,
        })
    );
    // What an extraction that started before the change holds.
    let running = pipeline.graph_hardware.grant();

    choose(&mut pipeline, HardwareProfile::Efficient).await;

    assert_limits_of(&pipeline, HardwareProfile::Efficient);
    assert_eq!(pipeline.tuner.params().max_concurrent_downloads, 10);
    assert_eq!(pipeline.tuner.params().decode_thread_count, 2);
    assert_eq!(pool_size(&pipeline.graph_hardware.grant()), 4);
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
    assert!(!Arc::ptr_eq(
        running.pool.as_ref().unwrap(),
        pipeline.graph_hardware.grant().pool.as_ref().unwrap()
    ));
    assert_eq!(pool_size(&running), 6);
}

#[tokio::test]
async fn a_scheduled_profile_overrides_the_choice_until_the_schedule_lets_go() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = pipeline_on_twelve_cores(&temp_dir).await;

    // A scheduled speed limit is a separate track the profile rule must not
    // end.
    pipeline.report_all_for_test();
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
        .apply_schedule_action(crate::bandwidth::ScheduleAction::SpeedLimit {
            limits: vec![crate::bandwidth::SpeedLimitChange {
                target: crate::bandwidth::SpeedTarget::Global,
                bytes_per_sec: 128 * 1024,
            }],
        })
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;

    pipeline.report_all_for_test();
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
        .apply_schedule_action(crate::bandwidth::ScheduleAction::HardwareProfile {
            profile: HardwareProfile::Efficient,
        })
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;

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
    let pool = pipeline.graph_hardware.grant().pool.unwrap();

    schedule(&mut pipeline, Some(HardwareProfile::Performance)).await;

    assert_limits_of(&pipeline, HardwareProfile::Balanced);
    assert!(
        Arc::ptr_eq(
            &pool,
            pipeline.graph_hardware.grant().pool.as_ref().unwrap()
        ),
        "nothing was rebuilt"
    );
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

// A run's grant as its memory, its threads, and whether the pool is lent.
fn lent(budget: &Budget) -> (Option<u64>, usize, bool) {
    (budget.memory_bytes, budget.threads, budget.pool.is_some())
}

fn pool_size(grant: &Grant) -> usize {
    grant
        .pool
        .as_ref()
        .expect("a pipeline's grant has a pool")
        .current_num_threads()
}

// A run that waits for its gate to open, then says what it was granted.
struct Told {
    gate: std::sync::mpsc::Receiver<()>,
    told: std::sync::mpsc::Sender<Budget>,
}

impl weaver_graph::Extraction for Told {
    fn run(
        self: Box<Self>,
        _: Arc<dyn weaver_graph::Volumes>,
        _: &mut weaver_graph::Sink<'_>,
        budget: Budget,
    ) {
        let _ = self.gate.recv();
        let _ = self.told.send(budget);
    }
}

// A run going when the profile changes keeps the budget it started on; the
// run after the change starts on the new profile's.
#[tokio::test]
async fn a_profile_change_reaches_the_next_run_and_not_the_running_one() {
    use crate::pipeline::graph::engine::{RunMail, Runs};
    use crate::pipeline::graph::envelope::Envelopes;
    use weaver_graph::NodeId;
    use weaver_graph::holds::{HoldBudgets, HoldsAccountant};

    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = pipeline_on_twelve_cores(&temp_dir).await;
    let mut envelopes = Envelopes::new(temp_dir.path().join("envelopes"));
    let mut mail = RunMail::default();
    let accountant = Arc::new(HoldsAccountant::new(HoldBudgets {
        resident_limit: u64::MAX,
        scratch_total: u64::MAX,
        scratch_ceiling: u64::MAX,
        disk_reserve: 0,
    }));
    let mut runs = Runs::new(1, 2, mail.sender(), accountant).on(pipeline.graph_hardware.clone());
    let (first, second) = (NodeId(1), NodeId(2));
    let (told, heard) = std::sync::mpsc::channel();

    let (open_first, first_gate) = std::sync::mpsc::channel();
    assert_eq!(runs.ask(first, 2), vec![first]);
    runs.start(
        first,
        Box::new(Told {
            gate: first_gate,
            told: told.clone(),
        }),
        envelopes.shelf(first),
    );

    choose(&mut pipeline, HardwareProfile::Efficient).await;

    let (open_second, second_gate) = std::sync::mpsc::channel();
    assert_eq!(runs.ask(second, 2), vec![second]);
    runs.start(
        second,
        Box::new(Told {
            gate: second_gate,
            told,
        }),
        envelopes.shelf(second),
    );
    open_second.send(()).unwrap();
    let second_budget = heard.recv().unwrap();
    open_first.send(()).unwrap();
    let first_budget = heard.recv().unwrap();

    let balanced = HardwareProfile::Balanced.tuning(pipeline.tuner.system_profile());
    let efficient = HardwareProfile::Efficient.tuning(pipeline.tuner.system_profile());
    assert_eq!(
        lent(&first_budget),
        (
            Some(balanced.sevenz_decode_memory_bytes),
            balanced.extract_threads,
            true
        )
    );
    assert_eq!(
        lent(&second_budget),
        (
            Some(efficient.sevenz_decode_memory_bytes),
            efficient.extract_threads,
            true
        )
    );
    assert_ne!(first_budget, second_budget);
    for _ in 0..2 {
        mail.recv().await.unwrap();
    }
}

// The efficient profile holds a repair to its share of the machine's memory
// and to its repair cap, and the pool the repair runs inside is no wider:
// PAR2 spreads across the pool, PAR3 across the budget's workers.
#[tokio::test]
async fn the_efficient_profile_holds_a_repair_to_its_cap() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = pipeline_on_twelve_cores(&temp_dir).await;
    let balanced = pipeline.graph_hardware.grant();
    // Twelve cores, every one but one for repair, held to the balanced
    // profile's six extract threads; an eighth of 8 GiB, up to 1 GiB.
    assert_eq!(
        balanced.repair,
        Budget {
            memory_bytes: Some(1024 * 1024 * 1024),
            threads: 6,
            pool: None,
        }
    );

    choose(&mut pipeline, HardwareProfile::Efficient).await;

    let grant = pipeline.graph_hardware.grant();
    let tuning = pipeline.tuner.profile_tuning();
    assert_eq!(tuning.par3_cpu_cap, Some(4));
    // A sixteenth of 8 GiB is 512 MiB, held to the 256 MiB ceiling.
    assert_eq!(
        grant.repair,
        Budget {
            memory_bytes: Some(256 * 1024 * 1024),
            threads: 4,
            pool: None,
        }
    );
    assert_eq!(grant.install(rayon::current_num_threads), 4);
    assert_eq!(balanced.install(rayon::current_num_threads), 6);
}
