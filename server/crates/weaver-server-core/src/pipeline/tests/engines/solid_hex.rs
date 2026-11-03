// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A solid RAR set posted under hex names, volumes lost and recreated by
// PAR2 from recovery volumes that arrive one at a time, under the graph
// engine. Each row replays one case of the archive
// matrix's grouping cross, built by its generator and judged by its rule
// set.

use super::super::direct_store::scenarios::archive_schedules::RecoveryFormat;
use super::super::direct_store::scenarios::archive_schedules::graph::grouping::cell_case;
use super::super::direct_store::scenarios::archive_schedules::grouping::{
    Binding, Cell, Container, Naming,
};

// Replay index 168: every article in order, the first volume lost whole.
const FIRST_LOST: usize = 168;
// Replay index 192: every article in order, volumes 0, 2 and 3 lost whole.
const ALL_BUT_SECOND_LOST: usize = 192;

// The first volume, recreated from the set, replays to the engine that
// owns the solid set; the replay ends once the member is handed on, so it
// never writes a member its namespace has already let go. The lost range
// it replays is damage, zero filled, and says nothing of the volume: its
// front is what was written, so the store reads the volume's own headers.
#[tokio::test]
async fn a_hex_named_solid_set_whose_first_volume_par2_recreates_delivers() {
    cell_case(
        Cell {
            container: Container::Rar5Solid,
            naming: Naming::HexScattered,
            binding: Binding::Par2Posted,
        },
        FIRST_LOST,
        RecoveryFormat::Par2Ladder,
    )
    .await;
}

// Only the second volume of a RAR4 solid set arrives. The recreated ones
// state no number of their own, and the one that opens on a continued
// member is never placed first: no store founded on the second volume
// takes it there, finds it misplaced and gives the set back to be founded
// again.
#[tokio::test]
async fn a_hex_named_rar4_solid_set_par2_recreates_around_its_second_volume_delivers() {
    cell_case(
        Cell {
            container: Container::Rar4Solid,
            naming: Naming::HexScattered,
            binding: Binding::Par2Posted,
        },
        ALL_BUT_SECOND_LOST,
        RecoveryFormat::Par2Ladder,
    )
    .await;
}
