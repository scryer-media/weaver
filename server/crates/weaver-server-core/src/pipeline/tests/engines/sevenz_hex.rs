// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A 7z set split into pieces posted under hex names, under the graph
// engine. Each row replays one case of the archive matrix's grouping
// cross, built by its generator and judged by its rule set.

use super::super::direct_store::scenarios::archive_schedules::RecoveryFormat;
use super::super::direct_store::scenarios::archive_schedules::graph::grouping::cell_case;
use super::super::direct_store::scenarios::archive_schedules::grouping::{
    Binding, Cell, Container, Naming,
};

// Replay index 96: every article in order, the pipeline restarted after
// the first piece arrives.
const RESTART_AFTER_FIRST: usize = 96;

// The first piece's bytes were held in memory when the pipeline went down,
// so the restored job fetches it again after the others. The solid set
// goes to its engine before that piece's front comes back; the engine
// reads the container's length from it when it does, so the two middle
// pieces, which only a name could order, go where the refused set went
// and the job fails with nothing delivered.
#[tokio::test]
async fn a_hex_named_solid_7z_set_restarted_after_its_first_piece_fails_under_graph() {
    cell_case(
        Cell {
            container: Container::SevenZipSolid,
            naming: Naming::HexScattered,
            binding: Binding::Nothing,
        },
        RESTART_AFTER_FIRST,
        RecoveryFormat::Absent,
    )
    .await;
}
