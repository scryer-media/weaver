// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A RAR4 set posted under a hex stem with its middle part never posted,
// under the graph engine. Each row replays one case of the archive matrix's
// grouping cross, built by its generator and judged by its rule set.

use super::super::direct_store::scenarios::archive_schedules::RecoveryFormat;
use super::super::direct_store::scenarios::archive_schedules::graph::grouping::cell_case;
use super::super::direct_store::scenarios::archive_schedules::grouping::{
    Binding, Cell, Container, Naming,
};

// Replay index 293: every article in order, the process to die the first
// time a member's file waits to move, before the checkpoint naming where it
// goes commits.
const MOVING_UNCOMMITTED: usize = 293;

// The posted names are the names the parts keep, so no file waits to move
// and the crash never comes. The PAR2 index describes only the posted
// parts, so nothing rebuilds the missing one and the job fails while the
// schedule still keeps the database's word back for that crash. The failed
// job waits for that word before it goes, and hears it.
#[tokio::test]
async fn a_hex_gap_rar4_set_failing_before_any_file_waits_to_move_goes() {
    cell_case(
        Cell {
            container: Container::Rar4,
            naming: Naming::HexNumberedGap,
            binding: Binding::Par2Posted,
        },
        MOVING_UNCOMMITTED,
        RecoveryFormat::Par2,
    )
    .await;
}
