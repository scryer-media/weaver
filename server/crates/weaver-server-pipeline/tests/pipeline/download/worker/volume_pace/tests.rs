// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use weaver_server_disk::operations::disk::volume_buffers::volume_limits;

const GIB: u64 = 1024 * MIB;
// The reference load: 70 connections of 800,000 B articles.
const REFERENCE_CONNECTIONS: usize = 70;
const REFERENCE_ARTICLE_BYTES: u64 = 800_000;

// The Performance figure, and the write rates of a hard disk, a SATA
// SSD and an NVMe SSD.
const FIGURE: u64 = 256 * MIB;
const MB: u64 = 1_000_000;
const HDD: u64 = 50 * MB;
const SATA: u64 = 200 * MB;
const NVME: u64 = 2_000 * MB;

// Shares are rounded down, and the weights are floating point.
const ROUNDING: u64 = 3;

fn hard_bytes(volumes: &[VolumeLimits]) -> Vec<u64> {
    volumes.iter().map(|volume| volume.hard_bytes).collect()
}

fn hard_sum(volumes: &[VolumeLimits]) -> u128 {
    volumes.iter().map(|v| u128::from(v.hard_bytes)).sum()
}

fn assert_near(volumes: &[VolumeLimits], expected: &[u64]) {
    assert_eq!(volumes.len(), expected.len(), "{volumes:?}");
    for (volume, &want) in volumes.iter().zip(expected) {
        assert!(
            volume.hard_bytes.abs_diff(want) <= ROUNDING,
            "{volumes:?} against {expected:?}"
        );
    }
}

#[test]
fn volume_pace_reference_machine_gets_the_performance_figure() {
    // A 16 GB machine: no load floor or memory ceiling is in the way.
    let budget = total_buffer_budget_bytes(
        HardwareProfile::Performance,
        16 * GIB,
        REFERENCE_CONNECTIONS,
        REFERENCE_ARTICLE_BYTES,
    );
    assert_eq!(budget, 256 * MIB);
    // And the figure holds four times what is in flight.
    assert!(budget >= 4 * 70 * 800_000);
}

#[test]
fn volume_pace_profiles_step_down_from_performance() {
    let budget = |profile| total_buffer_budget_bytes(profile, 16 * GIB, 10, 800_000);
    assert_eq!(budget(HardwareProfile::Performance), 256 * MIB);
    assert_eq!(budget(HardwareProfile::Balanced), 128 * MIB);
    assert_eq!(budget(HardwareProfile::Efficient), 64 * MIB);
}

#[test]
fn volume_pace_floor_binds_when_the_load_outgrows_the_profile() {
    // Balanced at the reference load: 4 x 70 x 0.8 MB is above 128 MiB.
    let budget = total_buffer_budget_bytes(
        HardwareProfile::Balanced,
        16 * GIB,
        REFERENCE_CONNECTIONS,
        REFERENCE_ARTICLE_BYTES,
    );
    assert_eq!(budget, 224_000_000);
    assert!(budget > 128 * MIB);

    // A lighter load leaves the profile's own figure in place.
    let light = total_buffer_budget_bytes(HardwareProfile::Balanced, 16 * GIB, 20, 800_000);
    assert_eq!(light, 128 * MIB);
}

#[test]
fn volume_pace_ceiling_binds_over_the_profile_and_the_floor() {
    // An eighth of 1 GiB is below the Performance figure and the floor.
    let budget = total_buffer_budget_bytes(
        HardwareProfile::Performance,
        GIB,
        REFERENCE_CONNECTIONS,
        REFERENCE_ARTICLE_BYTES,
    );
    assert_eq!(budget, 128 * MIB);

    // Just enough memory for the figure: the ceiling no longer binds.
    let enough = total_buffer_budget_bytes(
        HardwareProfile::Performance,
        2 * GIB,
        REFERENCE_CONNECTIONS,
        REFERENCE_ARTICLE_BYTES,
    );
    assert_eq!(enough, 256 * MIB);
}

#[test]
fn volume_pace_tiny_container_is_respected() {
    // A container's limit is the memory ceiling however small it is: an
    // eighth of 256 MiB, and of 128 MiB.
    for profile in HardwareProfile::ALL {
        for (ceiling, figure) in [(256 * MIB, 32 * MIB), (128 * MIB, 16 * MIB)] {
            let budget = total_buffer_budget_bytes(
                profile,
                ceiling,
                REFERENCE_CONNECTIONS,
                REFERENCE_ARTICLE_BYTES,
            );
            assert_eq!(budget, figure, "{profile:?}");
        }
    }
}

#[test]
fn volume_pace_budget_is_never_zero_and_never_overflows() {
    assert_eq!(
        total_buffer_budget_bytes(HardwareProfile::Efficient, 0, 0, 0),
        1
    );
    assert_eq!(
        total_buffer_budget_bytes(HardwareProfile::Performance, u64::MAX, usize::MAX, u64::MAX),
        u64::MAX / 8
    );
}

#[test]
fn volume_pace_memory_ceiling_caps_the_sum() {
    // An eighth of 3 GiB is 384 MiB: one volume has the figure, two
    // cannot both have it and share the 384 MiB by weight.
    let memory = 3 * GIB;
    assert_eq!(hard_bytes(&volume_limits(FIGURE, memory, &[HDD])), [FIGURE]);
    let two = volume_limits(FIGURE, memory, &[SATA, SATA]);
    assert_eq!(hard_bytes(&two), [192 * MIB; 2]);
    let two = volume_limits(FIGURE, memory, &[HDD, NVME]);
    assert_near(&two, &[FIGURE, 128 * MIB]);
    let three = volume_limits(FIGURE, memory, &[NVME, SATA, HDD]);
    assert_near(&three, &[16 * MIB, 112 * MIB, FIGURE]);

    // A 1 GiB container: the figure is the whole eighth, and two
    // volumes halve it.
    let figure = total_buffer_budget_bytes(HardwareProfile::Performance, GIB, 70, 800_000);
    assert_eq!(figure, 128 * MIB);
    let two = volume_limits(figure, GIB, &[SATA, SATA]);
    assert_eq!(hard_bytes(&two), [64 * MIB; 2]);

    let rates = [HDD, SATA, NVME, 0, NVME, SATA];
    for profile in HardwareProfile::ALL {
        for ceiling in [128 * MIB, 256 * MIB, GIB, 3 * GIB, 16 * GIB, 64 * GIB] {
            for connections in [1_usize, 10, 70, 400] {
                let figure = total_buffer_budget_bytes(profile, ceiling, connections, 800_000);
                for count in 1..=rates.len() {
                    let shared = volume_limits(figure, ceiling, &rates[..count]);
                    assert!(
                        hard_sum(&shared) <= u128::from(ceiling / 8),
                        "{profile:?}, ceiling {ceiling}, {connections} connections, {count} volumes"
                    );
                }
            }
        }
    }
}

#[test]
fn volume_pace_intake_is_open_below_the_full_line_and_stopped_at_it() {
    let limits = VolumeLimits::with_hard(1000);
    assert_eq!(limits.soft_bytes, 700);
    assert_eq!(volume_intake(0, limits), VolumeIntake::Open);
    assert_eq!(volume_intake(700, limits), VolumeIntake::Open);
    assert_eq!(volume_intake(999, limits), VolumeIntake::Open);
    assert_eq!(volume_intake(1000, limits), VolumeIntake::Stopped);
    assert_eq!(volume_intake(u64::MAX, limits), VolumeIntake::Stopped);

    // The full Performance figure: 179.2 MiB and 256 MiB.
    let limits = VolumeLimits::with_hard(FIGURE);
    let soft = FIGURE * 70 / 100;
    assert_eq!(volume_intake(soft, limits), VolumeIntake::Open);
    assert_eq!(volume_intake(FIGURE - 1, limits), VolumeIntake::Open);
    assert_eq!(volume_intake(FIGURE, limits), VolumeIntake::Stopped);

    // A one-byte limit is open when empty and stopped otherwise.
    let limits = VolumeLimits::with_hard(1);
    assert_eq!(volume_intake(0, limits), VolumeIntake::Open);
    assert_eq!(volume_intake(1, limits), VolumeIntake::Stopped);
}
