// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Every table a checkpoint writes. A job's rows go with the job.
pub const TABLES: [&str; 9] = [
    "graph_nodes",
    "graph_members",
    "graph_coverage",
    "graph_envelopes",
    "graph_crypt",
    "graph_recovery",
    "graph_holds",
    "graph_facts_parts",
    "graph_held_parts",
];
