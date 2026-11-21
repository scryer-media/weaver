// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_server_composition::JobHistoryRow;
pub use weaver_server_pipeline::{history, jobs};

mod pipeline {
    use weaver_server_pipeline::pipeline::tests::engines::harness::health_milli;
    mod tests {
        use weaver_server_pipeline::pipeline::tests::{
            direct_store, single_member_rar4_old_numbering_set,
            single_recovery_volume_for_rar4_set, with_rar4_header_crcs,
        };
        mod engines {
            use weaver_server_pipeline::pipeline::tests::engines::{
                archive_scenario_fixtures, harness, rar_fixtures as rar, rar_fixtures,
                restart_fixtures as restart, stream_fixtures as streams,
            };
            mod sevenz {
                pub(super) use weaver_server_pipeline::pipeline::tests::engines::sevenz_fixtures::{container, store::SECRET};
            }
            mod accounting;
            mod download_summary;
            mod embedded;
            mod encrypted_hex;
            mod pairings;
            mod password;
            mod rar_rev;
            mod recovery_audit;
            mod sets;
            mod sevenz_hex;
            mod solid_hex;
        }
    }
}
