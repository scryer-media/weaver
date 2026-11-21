// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::{HexTwoSets, Selection};
use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::graph::two_sets::{TwoSets, two_set_campaign, two_set_order};

#[tokio::test]
async fn two_set_arrival_schedules() {
    two_set_campaign(TwoSets::Stored, Selection::Smoke).await;
}

#[tokio::test]
async fn rar5_hex_two_set_arrival_schedules() {
    two_set_campaign(TwoSets::Hex(HexTwoSets::Rar5), Selection::Smoke).await;
}

#[tokio::test]
async fn rar4_hex_two_set_arrival_schedules() {
    two_set_campaign(TwoSets::Hex(HexTwoSets::Rar4), Selection::Smoke).await;
}

// The first archive's second volume, then the second archive's first, then
// the rest.
#[tokio::test]
async fn rar5_hex_two_sets_interleaved_across_positions_never_merge() {
    // Slots 2, 1, 0, 3: A1, B0, A0, B1.
    two_set_order(
        TwoSets::Hex(HexTwoSets::Rar5),
        &[(1, 0), (0, 1), (0, 0), (1, 1)],
    )
    .await;
}

// Each archive's later volume ahead of its first, as a `.r00` posted before
// its `.rar` arrives: the first archive is whole before the second appears.
#[tokio::test]
async fn rar5_hex_two_sets_with_each_later_volume_first_never_merge() {
    // Slots 2, 0, 3, 1: A1, A0, B1, B0.
    two_set_order(
        TwoSets::Hex(HexTwoSets::Rar5),
        &[(1, 0), (0, 0), (1, 1), (0, 1)],
    )
    .await;
}
