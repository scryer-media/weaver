// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The extended campaigns: each schedule of a campaign's pool admitted as a
// job and judged by the archive matrix's one rule set.
use super::*;
use crate::pipeline::tests::direct_store::scenarios::archive_schedules::graph::{
    GraphFixture, assert_cells, graph_campaign, graph_compressed_campaign, graph_fixture,
    graph_fixture_cases, rar_post,
};

mod depth;
mod formats;
mod repair;
