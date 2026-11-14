// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

const MIB: usize = 1024 * 1024;
const GIB: u64 = 1024 * 1024 * 1024;

#[test]
fn an_uncapped_host_sizes_as_the_four_to_eight_gib_tier() {
    let uncapped = BacklogSizing::for_runtime_memory(32 * GIB, None);
    assert_eq!(uncapped.base_bytes, 144 * MIB);
    assert_eq!(uncapped.pending_writes_per_file, 16);
}

#[test]
fn a_container_sizes_from_its_memory_limit() {
    let capped = BacklogSizing::for_runtime_memory(32 * GIB, Some(1536 * 1024 * 1024));
    assert_eq!(capped.base_bytes, 48 * MIB);
    assert_eq!(capped.pending_writes_per_file, 4);

    // A limit above the free memory still names the tier.
    let large = BacklogSizing::for_runtime_memory(GIB, Some(64 * GIB));
    assert_eq!(large.base_bytes, 384 * MIB);
    assert_eq!(large.pending_writes_per_file, 16);
}
