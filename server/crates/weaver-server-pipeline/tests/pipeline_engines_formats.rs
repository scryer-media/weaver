// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_server_pipeline::{history, jobs};

mod pipeline {
    mod tests {
        use weaver_server_pipeline::pipeline::tests::{
            TEST_RAR4_SIG, TEST_RAR5_SIG, build_test_rar_directory_header,
            build_test_rar_end_header, build_test_rar_file_header, build_test_rar_main_header,
            build_test_rar4_block, build_test_rar4_end_header, build_test_rar4_file_header,
            direct_store,
        };
        mod engines {
            use weaver_server_pipeline::pipeline::tests::engines::{
                harness, rar_fixtures, sevenz_fixtures,
            };
            mod rar;
            mod rar_mixed;
            mod sevenz;
            mod sevenz_mixed;
        }
    }
}
