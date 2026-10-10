use crate::runtime::system_profile::*;

use super::*;

// A machine with `cores` physical cores and `memory_gib` of RAM. Disk plays no
// part in the profile decision, so it stays fixed.
fn machine(cores: usize, memory_gib: u64) -> SystemProfile {
    SystemProfile {
        cpu: CpuProfile {
            physical_cores: cores,
            logical_cores: cores * 2,
            simd: SimdSupport::default(),
            cgroup_limit: None,
        },
        memory: MemoryProfile {
            total_bytes: memory_gib * GIB,
            available_bytes: memory_gib * GIB / 2,
            cgroup_limit: None,
        },
        disk: DiskProfile {
            storage_class: StorageClass::Ssd,
            filesystem: FilesystemType::Ext4,
            sequential_write_mbps: 2000.0,
            random_read_iops: 50000.0,
            same_filesystem: true,
        },
    }
}

#[test]
fn a_four_core_eight_gibibyte_board_stops_at_balanced() {
    let pi = machine(4, 8);
    assert_eq!(
        HardwareProfile::available(&pi),
        vec![HardwareProfile::Efficient, HardwareProfile::Balanced]
    );
    assert_eq!(HardwareProfile::recommended(&pi), HardwareProfile::Balanced);
    let unmet = HardwareProfile::Performance
        .unmet_requirement(&pi)
        .expect("performance is out of reach here");
    assert!(unmet.contains("16 GiB"), "{unmet}");
    assert!(unmet.contains("8 GiB"), "{unmet}");
}

#[test]
fn a_small_machine_is_offered_nothing_to_choose() {
    let small = machine(2, 2);
    assert_eq!(
        HardwareProfile::available(&small),
        vec![HardwareProfile::Efficient]
    );
    assert_eq!(
        HardwareProfile::recommended(&small),
        HardwareProfile::Efficient
    );
    assert!(
        HardwareProfile::Balanced
            .unmet_requirement(&small)
            .is_some()
    );
}

#[test]
fn a_large_machine_is_recommended_performance() {
    let large = machine(16, 32);
    assert_eq!(HardwareProfile::available(&large), HardwareProfile::ALL);
    assert_eq!(
        HardwareProfile::recommended(&large),
        HardwareProfile::Performance
    );
    for profile in HardwareProfile::ALL {
        assert_eq!(profile.unmet_requirement(&large), None);
    }
}

#[test]
fn a_container_is_held_to_its_quota_not_the_host() {
    let mut boxed = machine(16, 32);
    boxed.cpu.cgroup_limit = Some(2.5);
    boxed.memory.cgroup_limit = Some(2 * GIB);
    assert_eq!(HardwareProfile::effective_cores(&boxed), 2);
    assert_eq!(HardwareProfile::effective_memory_bytes(&boxed), 2 * GIB);
    assert_eq!(
        HardwareProfile::available(&boxed),
        vec![HardwareProfile::Efficient]
    );

    // Cores alone are enough to hold a machine back, even with memory to spare.
    let mut cpu_bound = machine(16, 32);
    cpu_bound.cpu.cgroup_limit = Some(4.0);
    assert_eq!(
        HardwareProfile::available(&cpu_bound),
        vec![HardwareProfile::Efficient, HardwareProfile::Balanced]
    );
}

// A cpuset or affinity mask narrows the CPUs a process may use without any
// quota, while the physical count is still read host-wide.
#[test]
fn a_process_pinned_to_two_cpus_is_held_to_them_without_a_quota() {
    let mut pinned = machine(16, 32);
    pinned.cpu.logical_cores = 2;
    assert_eq!(HardwareProfile::effective_cores(&pinned), 2);
    assert_eq!(
        HardwareProfile::available(&pinned),
        vec![HardwareProfile::Efficient]
    );
    let tuning = HardwareProfile::Efficient.tuning(&pinned);
    assert_eq!(tuning.decode_threads, 2);
    assert_eq!(tuning.extract_threads, 1);
}

#[test]
fn a_fractional_quota_below_one_core_still_leaves_a_thread() {
    let mut sliver = machine(8, 8);
    sliver.cpu.cgroup_limit = Some(0.5);
    assert_eq!(HardwareProfile::effective_cores(&sliver), 1);
    let tuning = HardwareProfile::Efficient.tuning(&sliver);
    assert_eq!(tuning.extract_threads, 1);
}

#[test]
fn each_profile_decides_its_own_limits() {
    let large = machine(16, 32);

    let efficient = HardwareProfile::Efficient.tuning(&large);
    assert_eq!(efficient.sevenz_decode_memory_bytes, 512 * MIB);
    assert_eq!(efficient.extraction_memory_bytes, 8 * GIB);
    assert_eq!(efficient.decode_threads, 2);
    assert_eq!(efficient.extract_threads, 4);
    assert_eq!(efficient.max_concurrent_downloads_cap, Some(10));

    let balanced = HardwareProfile::Balanced.tuning(&large);
    assert_eq!(balanced.sevenz_decode_memory_bytes, GIB);
    assert_eq!(balanced.extraction_memory_bytes, 16 * GIB);
    assert_eq!(balanced.decode_threads, 4);
    assert_eq!(balanced.extract_threads, 8);
    assert_eq!(balanced.max_concurrent_downloads_cap, None);

    let performance = HardwareProfile::Performance.tuning(&large);
    assert_eq!(performance.sevenz_decode_memory_bytes, 4 * GIB);
    assert_eq!(performance.extraction_memory_bytes, 16 * GIB);
    assert_eq!(performance.decode_threads, 16);
    assert_eq!(performance.extract_threads, 8);
    assert_eq!(performance.max_concurrent_downloads_cap, None);
}

// The extraction, PAR3 and holds limits shrink with the profile, and the
// widest profile leaves each exactly where the machine alone put it.
#[test]
fn each_profile_sizes_extraction_par3_and_holds_limits() {
    for large in [machine(4, 8), machine(16, 64)] {
        let efficient = HardwareProfile::Efficient.tuning(&large);
        assert_eq!(efficient.max_concurrent_extractions, 2);
        assert_eq!(
            efficient.par3_memory,
            MemoryShare {
                divisor: 16,
                cap_bytes: 256 * MIB
            }
        );
        assert_eq!(efficient.par3_cpu_cap, Some(efficient.extract_threads));
        assert_eq!(
            efficient.direct_store_resident_default_cap_bytes,
            Some(256 * MIB)
        );

        let balanced = HardwareProfile::Balanced.tuning(&large);
        assert_eq!(balanced.max_concurrent_extractions, 4);
        assert_eq!(
            balanced.par3_memory,
            MemoryShare {
                divisor: 8,
                cap_bytes: GIB
            }
        );
        assert_eq!(balanced.par3_cpu_cap, Some(balanced.extract_threads));
        assert_eq!(balanced.direct_store_resident_default_cap_bytes, None);

        let performance = HardwareProfile::Performance.tuning(&large);
        assert_eq!(performance.max_concurrent_extractions, 6);
        assert_eq!(
            performance.par3_memory,
            MemoryShare {
                divisor: 8,
                cap_bytes: 2 * GIB
            }
        );
        assert_eq!(performance.par3_cpu_cap, None);
        assert_eq!(performance.direct_store_resident_default_cap_bytes, None);
    }

    let share = HardwareProfile::Balanced.tuning(&machine(4, 8)).par3_memory;
    assert_eq!(share.of(8 * GIB), GIB);
    assert_eq!(share.of(64 * GIB), GIB);
    assert_eq!(share.of(GIB), 128 * MIB);
}

#[test]
fn the_smallest_profile_still_extracts_with_every_core_it_can_spare() {
    // The allowance is what holds memory down, so the smallest profile buys
    // back the wall time threads are free to give it.
    let board = machine(4, 8);
    assert_eq!(HardwareProfile::Efficient.tuning(&board).extract_threads, 2);

    let cramped = machine(8, 2);
    assert_eq!(
        HardwareProfile::Efficient.tuning(&cramped).extract_threads,
        4
    );
    assert_eq!(
        HardwareProfile::Efficient
            .tuning(&cramped)
            .sevenz_decode_memory_bytes,
        512 * MIB,
        "more threads never widen the allowance that bounds them"
    );
}

#[test]
fn thread_counts_follow_the_cores_a_machine_actually_has() {
    let modest = machine(4, 8);
    let balanced = HardwareProfile::Balanced.tuning(&modest);
    assert_eq!(balanced.decode_threads, 4);
    assert_eq!(balanced.extract_threads, 2);

    let wide = machine(64, 256);
    let performance = HardwareProfile::Performance.tuning(&wide);
    assert_eq!(
        performance.decode_threads, 16,
        "decode threads stay bounded"
    );
    assert_eq!(
        performance.extract_threads, 8,
        "post-processing threads stay bounded"
    );
}

#[test]
fn persisted_values_round_trip_and_unknown_text_reads_as_unchosen() {
    for profile in HardwareProfile::ALL {
        assert_eq!(HardwareProfile::parse(profile.as_str()), Some(profile));
    }
    assert_eq!(
        HardwareProfile::parse(" PERFORMANCE "),
        Some(HardwareProfile::Performance)
    );
    assert_eq!(HardwareProfile::parse("turbo"), None);
    assert_eq!(HardwareProfile::parse(""), None);
}

#[test]
fn a_requirement_reads_in_whole_gibibytes_and_a_machine_in_tenths() {
    assert_eq!(format_gibibytes(16 * GIB), "16 GiB");
    assert_eq!(format_gibibytes(GIB / 2), "0.5 GiB");
    assert_eq!(format_gibibytes(7 * GIB + 800 * MIB), "7.8 GiB");
}
