//! Three named profiles that decide every hardware-derived limit at once.
//!
//! An operator picks a name, not a set of knobs: the profile answers how much
//! memory a 7z decoder may hold, how much of the machine's RAM extraction may
//! reserve, and how many decode and post-processing threads run. A profile that
//! the machine cannot honour is never offered, so the pick is always one the
//! hardware can keep.

use serde::{Deserialize, Serialize};

use crate::runtime::system_profile::SystemProfile;

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

/// Everything a profile decides, resolved against the machine it runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProfileTuning {
    /// Memory a conventional 7z extraction may hold while decoding. The
    /// extraction ceiling still bounds it; this only stops one archive from
    /// taking the whole allowance.
    pub sevenz_decode_memory_bytes: u64,
    /// The extraction memory ceiling, before the `[64 MiB, 64 GiB]` clamp and
    /// before an explicit environment override replaces it.
    pub extraction_memory_bytes: u64,
    /// Articles decoded at once.
    pub decode_threads: usize,
    /// Threads in the post-processing and chase pools: 7z and xz decode
    /// threads, PAR2 verify and repair, and concurrent chases all come from
    /// these. Fixed when the pools are built, so a profile change reaches them
    /// at the next start.
    pub extract_threads: usize,
    /// A startup cap on concurrent downloads, chosen by the profile rather
    /// than derived from pressure. Per-job live memory scales with the number
    /// of downloads in flight, which is the whole point of the efficient
    /// profile; `None` leaves the configured connection count alone.
    pub max_concurrent_downloads_cap: Option<usize>,
}

/// One profile's whole definition: what it needs, and what it decides.
///
/// Every number the profiles differ by lives in [`TABLE`] below, so retuning a
/// profile after a benchmark sweep is a one-line change here and nowhere else.
struct ProfileRow {
    profile: HardwareProfile,
    /// Memory the machine must have for this profile to be offered.
    min_memory_bytes: u64,
    /// Physical cores, after any cgroup limit, the machine must have.
    min_cores: usize,
    sevenz_decode_memory_bytes: u64,
    /// The share of the machine's memory extraction may reserve.
    extraction_memory_divisor: u64,
    max_concurrent_downloads_cap: Option<usize>,
}

/// Ordered from the least demanding to the most: `available` preserves this
/// order, and `recommended` takes the last entry it yields.
const TABLE: [ProfileRow; 3] = [
    ProfileRow {
        profile: HardwareProfile::Efficient,
        min_memory_bytes: 0,
        min_cores: 0,
        sevenz_decode_memory_bytes: 512 * MIB,
        extraction_memory_divisor: 4,
        max_concurrent_downloads_cap: Some(10),
    },
    ProfileRow {
        profile: HardwareProfile::Balanced,
        min_memory_bytes: 4 * GIB,
        min_cores: 4,
        sevenz_decode_memory_bytes: GIB,
        extraction_memory_divisor: 2,
        max_concurrent_downloads_cap: None,
    },
    ProfileRow {
        profile: HardwareProfile::Performance,
        min_memory_bytes: 16 * GIB,
        min_cores: 8,
        sevenz_decode_memory_bytes: 4 * GIB,
        extraction_memory_divisor: 2,
        max_concurrent_downloads_cap: None,
    },
];

/// How hard Weaver leans on the machine it runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HardwareProfile {
    /// Small machines and shared hosts: the smallest limits that still work.
    Efficient,
    /// The default for an ordinary desktop or server.
    Balanced,
    /// Large machines: the widest limits Weaver offers.
    Performance,
}

impl HardwareProfile {
    pub const ALL: [Self; 3] = [Self::Efficient, Self::Balanced, Self::Performance];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Efficient => "efficient",
            Self::Balanced => "balanced",
            Self::Performance => "performance",
        }
    }

    /// Parse a persisted value. Unknown text reads as "never chosen", so a
    /// hand-edited setting degrades to the recommendation instead of failing
    /// startup or silently picking the widest limits.
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "efficient" => Some(Self::Efficient),
            "balanced" => Some(Self::Balanced),
            "performance" => Some(Self::Performance),
            _ => None,
        }
    }

    /// Memory this machine can actually use: a container's limit where there is
    /// one, the installed RAM otherwise.
    pub fn effective_memory_bytes(probe: &SystemProfile) -> u64 {
        match probe.memory.cgroup_limit {
            Some(limit) => limit.min(probe.memory.total_bytes.max(1)),
            None => probe.memory.total_bytes,
        }
    }

    /// Physical cores this machine can actually use. The physical count is
    /// read host-wide, so it is capped by the CPUs this process may run on,
    /// which an affinity mask or cpuset narrows without any quota. A
    /// fractional cgroup quota rounds down but never to zero: half a core is
    /// still one thread's worth of work, and a zero here would divide by
    /// nothing downstream.
    pub fn effective_cores(probe: &SystemProfile) -> usize {
        let cores = probe
            .cpu
            .physical_cores
            .max(1)
            .min(probe.cpu.logical_cores.max(1));
        match probe.cpu.cgroup_limit {
            Some(limit) if limit > 0.0 => cores.min((limit as usize).max(1)),
            _ => cores,
        }
    }

    /// The profiles this machine can honour, least demanding first. Efficient
    /// has no requirements, so this is never empty.
    pub fn available(probe: &SystemProfile) -> Vec<Self> {
        let memory = Self::effective_memory_bytes(probe);
        let cores = Self::effective_cores(probe);
        TABLE
            .iter()
            .filter(|row| memory >= row.min_memory_bytes && cores >= row.min_cores)
            .map(|row| row.profile)
            .collect()
    }

    /// The most capable profile this machine can honour.
    pub fn recommended(probe: &SystemProfile) -> Self {
        Self::available(probe)
            .last()
            .copied()
            .unwrap_or(Self::Efficient)
    }

    /// Why this machine cannot offer a profile, phrased for an operator who
    /// asked for it anyway. `None` when the profile is available.
    pub fn unmet_requirement(self, probe: &SystemProfile) -> Option<String> {
        let row = self.row();
        let memory = Self::effective_memory_bytes(probe);
        let cores = Self::effective_cores(probe);
        if memory >= row.min_memory_bytes && cores >= row.min_cores {
            return None;
        }
        Some(format!(
            "the {} profile needs {} of memory and {} cores; this machine has {} and {}",
            self.as_str(),
            format_gibibytes(row.min_memory_bytes),
            row.min_cores,
            format_gibibytes(memory),
            cores,
        ))
    }

    /// Every derived limit, resolved against this machine.
    ///
    /// The two extraction knobs are independent, which is why the smallest
    /// profile is not the single-threaded one. Memory is bounded by
    /// `sevenz_decode_memory_bytes` alone: the decoder sizes its window from
    /// that allowance and then runs as many workers as it is given inside it,
    /// so at the efficient profile's allowance extra threads cost nothing in
    /// memory and buy most of the wall time back — decoding a large archive
    /// with one thread takes roughly twice as long as with two, and a single
    /// thread is the slowest arrangement at every allowance. Threads are
    /// therefore scaled with the machine's cores at every profile, and only
    /// the allowance separates them.
    ///
    /// An incompressible stream — a video payload, the common case — needs no
    /// help here: the decoder narrows itself to a couple of workers on that
    /// shape whatever it is offered, so the wider counts below are spent only
    /// on the archives that can use them.
    pub fn tuning(self, probe: &SystemProfile) -> ProfileTuning {
        let row = self.row();
        let memory = Self::effective_memory_bytes(probe);
        let cores = Self::effective_cores(probe);
        ProfileTuning {
            sevenz_decode_memory_bytes: row.sevenz_decode_memory_bytes,
            extraction_memory_bytes: memory / row.extraction_memory_divisor,
            decode_threads: match self {
                Self::Efficient => 2,
                Self::Balanced => cores.min(4),
                Self::Performance => cores.min(16),
            },
            extract_threads: match self {
                Self::Efficient => (cores / 2).clamp(1, 4),
                Self::Balanced | Self::Performance => (cores / 2).clamp(1, 8),
            },
            max_concurrent_downloads_cap: row.max_concurrent_downloads_cap,
        }
    }

    fn row(self) -> &'static ProfileRow {
        TABLE
            .iter()
            .find(|row| row.profile == self)
            .expect("every profile has a table row")
    }
}

/// Whole gibibytes where the value divides evenly, one decimal otherwise, so a
/// requirement reads as "16 GiB" and a machine as "7.8 GiB".
fn format_gibibytes(bytes: u64) -> String {
    if bytes.is_multiple_of(GIB) {
        format!("{} GiB", bytes / GIB)
    } else {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    }
}

#[cfg(test)]
mod tests;
