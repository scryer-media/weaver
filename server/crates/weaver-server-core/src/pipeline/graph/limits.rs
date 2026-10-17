// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The graph engine's limits, from the settings that already exist. The
// engine adds no settings of its own: an operator who tuned the direct
// store's holds or the extraction concurrency gets the same limits here.

use weaver_graph::holds::HoldBudgets;

use crate::RuntimeTuner;
use crate::pipeline::direct_store::DirectStoreSettings;

// The holds budgets and the number of slots.
//
//   resident_limit   holds_resident_limit_bytes, process-wide RAM, as before
//   scratch_total    holds_scratch_total_bytes, process-wide scratch, as before
//   scratch_ceiling  holds_scratch_ceiling_bytes, which bounded one set's
//                    scratch and now bounds one namespace's
//   disk_reserve     holds_disk_reserve_bytes, zero still disables it
//   slots            the extraction concurrency the tuner allows, which
//                    already bounds streaming member extractions
pub(crate) fn budgets(holds: &DirectStoreSettings, tuner: &RuntimeTuner) -> (HoldBudgets, usize) {
    let budgets = HoldBudgets {
        resident_limit: holds.holds_resident_limit_bytes,
        scratch_total: holds.holds_scratch_total_bytes,
        scratch_ceiling: holds.holds_scratch_ceiling_bytes,
        disk_reserve: holds.holds_disk_reserve_bytes,
    };
    (budgets, tuner.max_concurrent_extractions())
}
