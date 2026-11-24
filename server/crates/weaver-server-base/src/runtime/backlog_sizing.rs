// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How large the pipeline's backlogs may grow, sized from memory.
//
// Two limits and one count follow from the memory the service may use: the
// decoded bytes that may wait for the writer, the bytes that may wait to be
// decoded, and the segments each open file may hold before it is written.
// The memory is placed in a tier, and the tier gives a base size and that
// count. The pipeline sizes both byte limits from the base size and the
// machine's memory (`compute_write_backlog_budget_bytes` and
// `compute_decode_backlog_budget_bytes`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BacklogSizing {
    // The tier's base size, in bytes: the least the write backlog limit
    // grows to before memory caps it, and a third of the least the decode
    // backlog limit grows to.
    pub base_bytes: usize,
    // How many decoded segments each open file may hold before it is
    // written. Fewer on small machines.
    pub pending_writes_per_file: usize,
}

impl BacklogSizing {
    // On hosts with no memory limit, size as the 4–8 GiB tier however much
    // memory is free, so a large machine does not grow its backlogs with it.
    const UNCAPPED_HOST_MEMORY_CAP_BYTES: u64 = (8 * 1024 * 1024 * 1024) - 1;

    // The sizing for this machine: a container's memory limit when there is
    // one, otherwise the available memory capped to the 4–8 GiB tier.
    pub fn for_runtime_memory(available_bytes: u64, cgroup_limit: Option<u64>) -> Self {
        Self::for_memory(Self::sizing_memory_bytes(available_bytes, cgroup_limit))
    }

    // The memory the tier is chosen from.
    pub fn sizing_memory_bytes(available_bytes: u64, cgroup_limit: Option<u64>) -> u64 {
        cgroup_limit.unwrap_or_else(|| available_bytes.min(Self::UNCAPPED_HOST_MEMORY_CAP_BYTES))
    }

    // The tier for `memory_bytes`.
    fn for_memory(memory_bytes: u64) -> Self {
        const MIB: usize = 1024 * 1024;
        let memory_mib = memory_bytes / (1024 * 1024);
        let (base_mib, pending_writes_per_file) = if memory_mib < 1024 {
            (24, 4)
        } else if memory_mib < 2048 {
            (48, 4)
        } else if memory_mib < 4096 {
            (96, 8)
        } else if memory_mib < 8192 {
            (144, 16)
        } else if memory_mib < 16384 {
            (192, 16)
        } else if memory_mib < 32768 {
            (288, 16)
        } else {
            (384, 16)
        };
        Self {
            base_bytes: base_mib * MIB,
            pending_writes_per_file,
        }
    }
}

#[cfg(test)]
mod tests;
