// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The extended campaigns under the graph engine: the classic campaigns'
// fixtures and schedule pools, each schedule admitted as a graph job and
// judged by the archive matrix's one graph rule set. The graph has no
// operator demotion, so a pool keeps every case but the demoting ones, each
// under its classic replay index.
use super::*;
use crate::pipeline::tests::direct_store::scenarios::archive_schedules::graph::{
    GraphFixture, assert_cells, graph_campaign, graph_compressed_campaign, graph_fixture,
    graph_fixture_cases, rar_post, runs_under_graph,
};

mod depth;
mod formats;
mod repair;
