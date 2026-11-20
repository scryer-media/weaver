// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_server_pipeline::{history, jobs};

mod pipeline {
    use weaver_server_pipeline::pipeline::graph;
    mod tests {
        use weaver_server_pipeline::pipeline::tests::{
            TEST_RAR5_SIG, build_test_par2_index_for_files, build_test_rar_compressed_file_header,
            build_test_rar_directory_header, build_test_rar_end_header, build_test_rar_file_header,
            build_test_rar_file_header_with_extra, build_test_rar_main_header, direct_store,
            encode_test_rar_vint, single_member_rar4_old_numbering_set, test_rar_compression_info,
        };
        mod engines {
            use weaver_server_pipeline::pipeline::tests::engines::{
                harness, rar_fixtures as rar, sfv, stream_fixtures as streams, zip,
            };
            mod classic_rar_crypt;
            mod furniture;
            mod header_names;
            mod nested_sfv;
            mod partial;
            mod restart;
            mod scene_zips;
            mod short_nested_sets;
            mod stall_rule;
            mod unlisted_par2;
        }
    }
}
