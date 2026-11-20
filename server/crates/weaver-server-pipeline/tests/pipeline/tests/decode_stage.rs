// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_base::runtime::system_profile::{
    CpuProfile, DiskProfile, FilesystemType, MemoryProfile, SimdSupport, StorageClass,
    SystemProfile,
};
use weaver_server_pipeline::pipeline::tests::{
    decode_fixture::*, new_direct_pipeline_with_backlog, uu_fixture::set_write_budget,
};

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
