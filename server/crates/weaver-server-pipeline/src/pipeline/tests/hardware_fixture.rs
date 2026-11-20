// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

use crate::runtime::HardwareProfile;
use weaver_graph::Budget;

use crate::pipeline::graph::hardware::Grant;

// Twelve cores and 8 GiB. Balanced is recommended; efficient differs from it
// in every limit, pool size included; performance is out of reach for want
// of memory.
pub fn twelve_core_machine() -> SystemProfile {
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
            guessed: false,
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

pub async fn pipeline_on_twelve_cores(temp_dir: &TempDir) -> Pipeline {
    let (pipeline, _, _) = new_direct_pipeline_on_machine(
        temp_dir.path().join("data"),
        temp_dir.path().join("intermediate"),
        temp_dir.path().join("complete"),
        temp_dir.path().join("weaver.db"),
        16 * 1024 * 1024,
        20,
        None,
        twelve_core_machine(),
    )
    .await;
    pipeline
}

pub async fn choose(pipeline: &mut Pipeline, profile: HardwareProfile) {
    pipeline.report_all_for_test();
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
        .set_hardware_profile(profile)
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
}

pub async fn schedule(pipeline: &mut Pipeline, profile: Option<HardwareProfile>) {
    pipeline.report_all_for_test();
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
        .set_scheduled_hardware_profile(profile)
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
}

// Everything a profile decides, as the next activity would find it.
pub fn assert_limits_of(pipeline: &Pipeline, profile: HardwareProfile) {
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
    // The next usenet socket dialled through a WireGuard tunnel.
    assert_eq!(
        weaver_tunnel::pipe::socket_buffers(&weaver_tunnel::pipe::Purpose::Nntp {
            server: 1,
            leg: 0,
            probing: false,
        }),
        Some(weaver_tunnel::wireguard::TcpBuffers::new(
            tuning.wireguard_usenet_receive_bytes,
            weaver_tunnel::pipe::USENET_SEND_BYTES,
        ))
    );
    // The decode pool fits one run at the profile's ceiling, and its
    // overhead.
    assert_eq!(
        pipeline.graph_decode.room().limit,
        tuning
            .sevenz_decode_memory_bytes
            .max(tuning.decode_window_bytes)
            + crate::pipeline::graph::engine::RUN_OVERHEAD
    );
    assert_eq!(
        lent(&grant.run),
        (
            Some(tuning.sevenz_decode_memory_bytes),
            Some(tuning.decode_window_bytes),
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

// A run's grant as its memory, its window, its threads, and whether the
// pool is lent.
pub fn lent(budget: &Budget) -> (Option<u64>, Option<u64>, usize, bool) {
    (
        budget.memory_bytes,
        budget.window_bytes,
        budget.threads,
        budget.pool.is_some(),
    )
}

fn pool_size(grant: &Grant) -> usize {
    grant
        .pool
        .as_ref()
        .expect("a pipeline's grant has a pool")
        .current_num_threads()
}

pub async fn resolved_holds(
    pipeline: &Pipeline,
    temp_dir: &TempDir,
) -> weaver_graph::holds::HoldBudgets {
    use crate::pipeline::graph::limits::{HoldSettings, HostFacts};
    HoldSettings::resolve(
        &*pipeline.config.read().await,
        HostFacts::probe(
            &temp_dir.path().join("intermediate"),
            HardwareProfile::Balanced
                .tuning(pipeline.tuner.system_profile())
                .direct_store_resident_default_cap_bytes,
        ),
    )
    .budgets()
}

use crate::pipeline::graph::{
    engine::{DecodePool, RunMail, Runs},
    envelope::Envelopes,
};
pub struct ProfileRuns {
    runs: Runs,
    mail: RunMail,
    envelopes: Envelopes,
}
impl ProfileRuns {
    pub fn new(pipeline: &Pipeline, dir: &TempDir) -> Self {
        let mail = RunMail::default();
        let pool = Arc::new(DecodePool::new(u64::MAX));
        Self {
            runs: Runs::new(1, 2, mail.sender(), pool).on(pipeline.graph_hardware.clone()),
            mail,
            envelopes: Envelopes::new(dir.path().join("envelopes")),
        }
    }
    pub fn ask(&mut self, node: weaver_graph::NodeId, limit: usize) -> Vec<weaver_graph::NodeId> {
        self.runs.ask(node, limit)
    }
    pub fn start(
        &mut self,
        node: weaver_graph::NodeId,
        extraction: Box<dyn weaver_graph::Extraction>,
    ) {
        self.runs
            .start(node, extraction, self.envelopes.shelf(node));
    }
    pub async fn receive(&mut self) {
        self.mail.recv().await.unwrap();
    }
}
pub struct ProfileGrant(Grant);
pub fn profile_grant(pipeline: &Pipeline) -> ProfileGrant {
    ProfileGrant(pipeline.graph_hardware.grant())
}
impl ProfileGrant {
    pub fn size(&self) -> usize {
        pool_size(&self.0)
    }
    pub fn pool(&self) -> Arc<rayon::ThreadPool> {
        Arc::clone(self.0.pool.as_ref().unwrap())
    }
    pub fn repair_pool(&self) -> Arc<rayon::ThreadPool> {
        Arc::clone(self.0.repair_pool.as_ref().unwrap())
    }
    pub fn repair(&self) -> &Budget {
        &self.0.repair
    }
    pub fn machine_bytes(&self) -> Option<u64> {
        self.0.machine_bytes
    }
    pub fn above_ceiling(&self) -> u64 {
        self.0.above_ceiling()
    }
    pub fn threads(&self) -> usize {
        self.0.install(rayon::current_num_threads)
    }
    pub fn repair_threads(&self) -> usize {
        self.0.install_repair(rayon::current_num_threads)
    }
}
pub fn tuner(pipeline: &Pipeline) -> &RuntimeTuner {
    &pipeline.tuner
}
pub fn holds(pipeline: &Pipeline) -> weaver_graph::holds::HoldBudgets {
    pipeline.graph_holds.budgets()
}
pub fn shared(pipeline: &Pipeline) -> &SharedPipelineState {
    &pipeline.shared_state
}
pub fn configured(pipeline: &Pipeline) -> HardwareProfile {
    pipeline.configured_hardware_profile
}
pub fn scheduled_limit(pipeline: &Pipeline) -> Option<u64> {
    pipeline.scheduled_bandwidth.and_then(|b| b.download_bps)
}
pub fn rate(pipeline: &Pipeline) -> Option<u64> {
    pipeline.rate_limiter.rate()
}
