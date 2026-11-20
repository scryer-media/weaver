// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_server_pipeline::{history, jobs};

mod pipeline {
    use weaver_server_pipeline::pipeline::graph;
    mod tests {
        use weaver_server_pipeline::pipeline::tests::{
            build_test_rar_end_header, build_test_rar_file_header, build_test_rar4_file_header,
            direct_store, rar4_misstated_set, rar5_misstated_set,
        };
        mod engines {
            use weaver_server_pipeline::pipeline::tests::engines::{
                depth as deep, harness, rar_fixtures as rar, rar_fixtures as rar_rev,
                restart_fixtures as restart, sevenz_fixtures as sevenz, stream_fixtures as streams,
                zip,
            };
            mod binding_order;
            mod misstated_sizes;
            mod nested_rar4;
            mod recovery_contracts;
            mod recovery_geometry;
            mod release_layers;
            mod size_bounds;
            mod split_sets;
            mod stage_events;
            mod waiting_sets;
        }
    }
}
