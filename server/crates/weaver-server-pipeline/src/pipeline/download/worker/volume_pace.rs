// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The download side of the per-volume write buffer. Each volume has one
// writer and one buffer limit, shared by every stream writing to that
// volume. This module says how large a full share is for the download load
// (the figure), and what a volume asks of its lanes given how full it is.
// How the volumes share the room, and the keeper that puts the limits on
// them, is `operations::disk::volume_buffers`.
//
// What a limit counts (`Volume::unwritten_bytes`): the articles leased to
// lanes and not arrived, at their size in the post, what arrived and is
// being decoded, and what waits for the writer. Nothing is counted past the
// hard limit, but the one article a volume that counts nothing grants.
//
// The sizing functions here are pure: numbers in, numbers out, no clock and
// no pipeline state. Each limit is a cap on what may wait, not memory taken
// up front.

use crate::pipeline::graph::disk::VolumeLeases;
use crate::runtime::HardwareProfile;
use weaver_server_disk::operations::disk::volume::Volume;
use weaver_server_disk::operations::disk::volume_buffers::{
    VOLUME_BUFFER_MEMORY_DIVISOR, VolumeLimits,
};

pub const MIB: u64 = 1024 * 1024;

// The Performance figure. The reference load, 70 connections of 800,000 B
// articles, has 56,000,000 B in flight; this holds that with room to put it
// in order.
const VOLUME_BUFFER_PERFORMANCE_BYTES: u64 = 256 * MIB;

// The Balanced figure: half of Performance.
const VOLUME_BUFFER_BALANCED_BYTES: u64 = 128 * MIB;

// The Efficient figure: half of Balanced.
const VOLUME_BUFFER_EFFICIENT_BYTES: u64 = 64 * MIB;

// The figure's floor, in multiples of what is in flight (connections x
// article size): a buffer smaller than what is in flight cannot put
// anything in order.
const VOLUME_BUFFER_IN_FLIGHT_MULTIPLE: u64 = 4;

// The size of a typical article, for the figure's floor: that of the
// reference load.
pub(in crate::pipeline) const TYPICAL_ARTICLE_BYTES: u64 = 800_000;

// The figure: the bytes that may wait for one volume's writer when the
// volume has a full share (see `volume_limits`).
//
// The profile gives the target, the load gives a floor and memory gives a
// ceiling. `memory_ceiling_bytes` is the memory the backlogs are carved
// from, already capped by a container's limit. The ceiling wins over the
// floor, so a small container is respected whatever the load. Never zero.
pub fn total_buffer_budget_bytes(
    profile: HardwareProfile,
    memory_ceiling_bytes: u64,
    connections: usize,
    typical_article_bytes: u64,
) -> u64 {
    let target = match profile {
        HardwareProfile::Efficient => VOLUME_BUFFER_EFFICIENT_BYTES,
        HardwareProfile::Balanced => VOLUME_BUFFER_BALANCED_BYTES,
        HardwareProfile::Performance => VOLUME_BUFFER_PERFORMANCE_BYTES,
    };
    let floor = in_flight_bytes(connections, typical_article_bytes)
        .saturating_mul(VOLUME_BUFFER_IN_FLIGHT_MULTIPLE);
    let ceiling = memory_ceiling_bytes / VOLUME_BUFFER_MEMORY_DIVISOR;
    target.max(floor).min(ceiling).max(1)
}

// What every connection holds when each carries one typical article.
fn in_flight_bytes(connections: usize, typical_article_bytes: u64) -> u64 {
    (connections as u64).saturating_mul(typical_article_bytes)
}

// What a volume asks of the lanes sending towards it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeIntake {
    // The volume does not hold its lanes back.
    Open,
    // Its lanes stop sending.
    Stopped,
}

// The state of a volume counting `counted_bytes` against a hard
// limit of `limits.hard_bytes`. Below it the volume is open, from it
// stopped. How much a lane is handed below it is the room the volume has
// (`volume_share_of_lane`); the soft limit is a reported figure, not a line
// for leases.
pub fn volume_intake(counted_bytes: u64, limits: VolumeLimits) -> VolumeIntake {
    if counted_bytes >= limits.hard_bytes {
        VolumeIntake::Stopped
    } else {
        VolumeIntake::Open
    }
}

// The state of a volume as it stands: what it counts, against the limit
// leases stop at (`VolumeLeases::lease_limit`). A volume that counts nothing is
// open whatever its limit, for the one article it grants then.
pub(in crate::pipeline) fn intake_of(volume: &Volume) -> VolumeIntake {
    let (soft_bytes, _) = volume.buffer_limits();
    let counted = volume.unwritten_bytes();
    if counted == 0 {
        return VolumeIntake::Open;
    }
    volume_intake(
        counted,
        VolumeLimits {
            soft_bytes,
            hard_bytes: volume.lease_limit(),
        },
    )
}
