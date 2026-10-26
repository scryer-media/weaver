// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A RAR5 set with enciphered headers posted under hex names, volumes
// recreated by PAR2, under the graph engine. Nothing of a volume reads
// without the password, so each one arriving before any store holds it
// founds a store of its own, and the volumes come together only as each
// store's walk finds its volume misplaced and gives it back. Each row
// replays one case of the archive matrix's grouping cross, built by its
// generator and judged by its rule set.

use super::super::direct_store::scenarios::archive_schedules::RecoveryFormat;
use super::super::direct_store::scenarios::archive_schedules::graph::grouping::cell_case;
use super::super::direct_store::scenarios::archive_schedules::grouping::{
    Binding, Cell, Container, Naming,
};

const CELL: Cell = Cell {
    container: Container::Rar5EncryptedHeaders,
    naming: Naming::HexScattered,
    binding: Binding::Par2Posted,
};

// Replay index 196: every volume lost whole, the PAR2 set in last.
const ALL_LOST: usize = 196;
// Replay index 285: every article in order, the first volume withheld until
// late, after the recovery volumes.
const FIRST_WITHHELD: usize = 285;

// All four volumes come back from one repair together, each founding a
// store. A volume given back carries its walk only as far as its main
// header, since the rest of its headers are held below until its next
// owner wants them: the store that holds the first volume takes it on the
// number that main header states, and the set comes together in one store.
#[tokio::test]
async fn an_encrypted_header_hex_set_par2_recreates_whole_delivers() {
    cell_case(CELL, ALL_LOST, RecoveryFormat::Par2).await;
}

// The set is refused at quiet for its missing first volume, which the
// ladder's repair then recreates, and every volume founds a store again.
// The first volume's store has walked no further than its encryption
// header when the second volume is offered to it: that says nothing
// against the second volume's stated number, so the store takes it.
#[tokio::test]
async fn an_encrypted_header_hex_set_whose_withheld_first_volume_par2_recreates_delivers() {
    cell_case(CELL, FIRST_WITHHELD, RecoveryFormat::Par2Ladder).await;
}
