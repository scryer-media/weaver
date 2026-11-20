// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::Arc;
use weaver_graph::Budget;
use weaver_server_pipeline::pipeline::tests::{
    hardware_fixture::*,
    new_direct_pipeline_on_machine,
    queue_fixture::{apply_intents, handle, report_all},
};
use weaver_server_pipeline::{HardwareProfile, HardwareProfileInForce};

#[tokio::test]
async fn a_chosen_profile_is_in_force_for_the_next_activity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = pipeline_on_twelve_cores(&temp_dir).await;
    assert_limits_of(&pipeline, HardwareProfile::Balanced);
    assert_eq!(tuner(&pipeline).params().max_concurrent_downloads, 20);
    assert_eq!(tuner(&pipeline).params().decode_thread_count, 4);
    assert_eq!(profile_grant(&pipeline).size(), 6);
    assert_eq!(
        shared(&pipeline).hardware_profile_in_force(),
        Some(HardwareProfileInForce {
            active: HardwareProfile::Balanced,
            scheduled: None,
        })
    );
    // What an extraction that started before the change holds.
    let running = profile_grant(&pipeline);

    choose(&mut pipeline, HardwareProfile::Efficient).await;

    assert_limits_of(&pipeline, HardwareProfile::Efficient);
    assert_eq!(tuner(&pipeline).params().max_concurrent_downloads, 10);
    assert_eq!(tuner(&pipeline).params().decode_thread_count, 2);
    assert_eq!(profile_grant(&pipeline).size(), 4);
    assert_eq!(configured(&pipeline), HardwareProfile::Efficient);
    assert_eq!(
        shared(&pipeline).hardware_profile_in_force(),
        Some(HardwareProfileInForce {
            active: HardwareProfile::Efficient,
            scheduled: None,
        })
    );
    // Running work finishes on what it started with.
    assert!(!Arc::ptr_eq(
        &running.pool(),
        &profile_grant(&pipeline).pool()
    ));
    assert_eq!(running.size(), 6);
}

// The holds keep exactly what their settings resolve to under every profile:
// the decode pool is apart from them and never raises their limit.
#[tokio::test]
async fn the_holds_keep_their_settings_on_every_profile() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = pipeline_on_twelve_cores(&temp_dir).await;
    let settings = resolved_holds(&pipeline, &temp_dir).await;
    assert_eq!(holds(&pipeline), settings);
    for profile in [HardwareProfile::Efficient, HardwareProfile::Balanced] {
        choose(&mut pipeline, profile).await;
        assert_limits_of(&pipeline, profile);
        assert_eq!(holds(&pipeline), settings);
    }
}

#[tokio::test]
async fn a_scheduled_profile_overrides_the_choice_until_the_schedule_lets_go() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = pipeline_on_twelve_cores(&temp_dir).await;

    // A scheduled speed limit is a separate track the profile rule must not
    // end.
    report_all(&pipeline);
    handle(&pipeline)
        .apply_schedule_action(
            weaver_server_pipeline::bandwidth::ScheduleAction::SpeedLimit {
                limits: vec![weaver_server_pipeline::bandwidth::SpeedLimitChange {
                    target: weaver_server_pipeline::bandwidth::SpeedTarget::Global,
                    bytes_per_sec: Some(128 * 1024),
                    upload_bytes_per_sec: None,
                }],
            },
        )
        .await
        .unwrap();
    apply_intents(&mut pipeline).await;

    report_all(&pipeline);
    handle(&pipeline)
        .apply_schedule_action(
            weaver_server_pipeline::bandwidth::ScheduleAction::HardwareProfile {
                profile: HardwareProfile::Efficient,
            },
        )
        .await
        .unwrap();
    apply_intents(&mut pipeline).await;

    assert_limits_of(&pipeline, HardwareProfile::Efficient);
    assert_eq!(
        shared(&pipeline).hardware_profile_in_force(),
        Some(HardwareProfileInForce {
            active: HardwareProfile::Efficient,
            scheduled: Some(HardwareProfile::Efficient),
        })
    );
    assert_eq!(scheduled_limit(&pipeline), Some(128 * 1024));
    assert_eq!(rate(&pipeline), Some(128 * 1024));
    assert!(!shared(&pipeline).is_paused());

    // The operator's choice is recorded but waits for the schedule.
    choose(&mut pipeline, HardwareProfile::Balanced).await;
    assert_limits_of(&pipeline, HardwareProfile::Efficient);

    schedule(&mut pipeline, None).await;
    assert_limits_of(&pipeline, HardwareProfile::Balanced);
    assert_eq!(
        shared(&pipeline).hardware_profile_in_force(),
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
    let pool = profile_grant(&pipeline).pool();

    schedule(&mut pipeline, Some(HardwareProfile::Performance)).await;

    assert_limits_of(&pipeline, HardwareProfile::Balanced);
    assert!(
        Arc::ptr_eq(&pool, &profile_grant(&pipeline).pool()),
        "nothing was rebuilt"
    );
    assert_eq!(
        shared(&pipeline).hardware_profile_in_force(),
        Some(HardwareProfileInForce {
            active: HardwareProfile::Balanced,
            scheduled: None,
        })
    );

    // A profile it can honour still applies after the skip.
    schedule(&mut pipeline, Some(HardwareProfile::Efficient)).await;
    assert_limits_of(&pipeline, HardwareProfile::Efficient);
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
    use weaver_graph::NodeId;

    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = pipeline_on_twelve_cores(&temp_dir).await;
    let mut runs = ProfileRuns::new(&pipeline, &temp_dir);
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
    );
    open_second.send(()).unwrap();
    let second_budget = heard.recv().unwrap();
    open_first.send(()).unwrap();
    let first_budget = heard.recv().unwrap();

    let balanced = HardwareProfile::Balanced.tuning(tuner(&pipeline).system_profile());
    let efficient = HardwareProfile::Efficient.tuning(tuner(&pipeline).system_profile());
    assert_eq!(
        lent(&first_budget),
        (
            Some(balanced.sevenz_decode_memory_bytes),
            Some(balanced.decode_window_bytes),
            balanced.extract_threads,
            true
        )
    );
    assert_eq!(
        lent(&second_budget),
        (
            Some(efficient.sevenz_decode_memory_bytes),
            Some(efficient.decode_window_bytes),
            efficient.extract_threads,
            true
        )
    );
    assert_ne!(first_budget, second_budget);
    for _ in 0..2 {
        runs.receive().await;
    }
}

// The efficient profile holds a repair to its share of the machine's memory
// and to its repair cap, and the pool the repair runs inside is no wider:
// PAR2 spreads across the pool, PAR3 across the budget's workers.
#[tokio::test]
async fn the_efficient_profile_holds_a_repair_to_its_cap() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = pipeline_on_twelve_cores(&temp_dir).await;
    let balanced = profile_grant(&pipeline);
    // Twelve cores, every one but one for repair, held to the balanced
    // profile's six extract threads; an eighth of 8 GiB is 1 GiB, held to
    // the 512 MiB ceiling.
    assert_eq!(
        *balanced.repair(),
        Budget {
            memory_bytes: Some(512 * 1024 * 1024),
            window_bytes: None,
            window_reserved: false,
            threads: 6,
            pool: None,
            lender: None,
        }
    );

    choose(&mut pipeline, HardwareProfile::Efficient).await;

    let grant = profile_grant(&pipeline);
    let tuning = tuner(&pipeline).profile_tuning();
    assert_eq!(tuning.par3_cpu_cap, Some(4));
    // A sixteenth of 8 GiB is 512 MiB, held to the 128 MiB ceiling.
    assert_eq!(
        *grant.repair(),
        Budget {
            memory_bytes: Some(128 * 1024 * 1024),
            window_bytes: None,
            window_reserved: false,
            threads: 4,
            pool: None,
            lender: None,
        }
    );
    assert_eq!(grant.threads(), 4);
    assert_eq!(balanced.threads(), 6);
    assert_eq!(grant.repair_threads(), 4);
    assert_eq!(balanced.repair_threads(), 6);
}

// A repair runs inside a pool as wide as the threads its grant gives it,
// on every profile: PAR2 spreads across the pool it runs in. The
// performance profile grants a repair every core but one, wider than its
// extract threads, and a profile back at the extract width repairs inside
// the post-processing pool again. Twelve cores with 64 GiB, so every
// profile is in reach.
#[tokio::test]
async fn a_repair_runs_inside_a_pool_as_wide_as_its_grant() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut machine = twelve_core_machine();
    machine.memory.total_bytes = 64 * 1024 * 1024 * 1024;
    machine.memory.available_bytes = 64 * 1024 * 1024 * 1024;
    let (mut pipeline, _, _) = new_direct_pipeline_on_machine(
        temp_dir.path().join("data"),
        temp_dir.path().join("intermediate"),
        temp_dir.path().join("complete"),
        temp_dir.path().join("weaver.db"),
        16 * 1024 * 1024,
        20,
        None,
        machine,
    )
    .await;
    for profile in [
        HardwareProfile::Performance,
        HardwareProfile::Efficient,
        HardwareProfile::Balanced,
        HardwareProfile::Performance,
    ] {
        choose(&mut pipeline, profile).await;
        let grant = profile_grant(&pipeline);
        let expected = match profile {
            HardwareProfile::Performance => 11,
            HardwareProfile::Balanced => 6,
            HardwareProfile::Efficient => 4,
        };
        assert_eq!(grant.repair().threads, expected, "{profile:?}");
        assert_eq!(grant.repair_threads(), expected, "{profile:?}");
        assert_eq!(
            grant.threads(),
            tuner(&pipeline).params().extract_thread_count,
            "{profile:?}"
        );
        let shared = Arc::ptr_eq(&grant.repair_pool(), &grant.pool());
        assert_eq!(shared, expected == grant.threads());
    }
}

// Only memory read from the machine is a hard stop for a run above the
// ceiling. Where it could not be read, the sizes the profile works from are
// a default, and nothing bounds the run: no hard stop, and no "this machine
// has" to say.
#[tokio::test]
async fn a_hard_stop_holds_only_to_memory_read_from_the_machine() {
    let temp_dir = tempfile::tempdir().unwrap();
    let pipeline = pipeline_on_twelve_cores(&temp_dir).await;
    let grant = profile_grant(&pipeline);
    assert_eq!(grant.machine_bytes(), Some(8 * 1024 * 1024 * 1024));
    assert_eq!(grant.above_ceiling(), 6 * 1024 * 1024 * 1024);

    let mut guessed = twelve_core_machine();
    guessed.memory.guessed = true;
    let other = tempfile::tempdir().unwrap();
    let (pipeline, _, _) = new_direct_pipeline_on_machine(
        other.path().join("data"),
        other.path().join("intermediate"),
        other.path().join("complete"),
        other.path().join("weaver.db"),
        16 * 1024 * 1024,
        20,
        None,
        guessed,
    )
    .await;
    let grant = profile_grant(&pipeline);
    assert_eq!(grant.machine_bytes(), None);
    assert_eq!(grant.above_ceiling(), u64::MAX);
    // What the guessed size feeds elsewhere is as before.
    assert_eq!(
        tuner(&pipeline).profile_tuning(),
        HardwareProfile::Balanced.tuning(&twelve_core_machine())
    );
}
