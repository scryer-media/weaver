// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The extended campaigns: each schedule of a campaign's pool admitted as a
// job and judged by the archive matrix's one rule set.
use super::*;
#[cfg(test)]
use crate::pipeline::tests::direct_store::scenarios::archive_schedules::graph::{
    GraphFixture, graph_campaign, graph_compressed_campaign, rar_post,
};
use crate::pipeline::tests::direct_store::scenarios::archive_schedules::graph::{
    assert_cells, graph_fixture, graph_fixture_cases,
};

#[cfg(test)]
mod formats;
#[cfg(test)]
mod repair;

use super::depth::Family;

pub async fn graph_family_campaign(family: Family, format: Format, selection: Selection) {
    eprintln!("graph {family:?} {format:?} selection={selection:?}");
    let cases = family.selected(format, selection);
    assert_cells(&graph_fixture_cases(&graph_fixture(format, family.slots()), cases).await);
}
