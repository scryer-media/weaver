// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_server_pipeline::{history, jobs};

mod pipeline {
    mod tests {
        use weaver_server_pipeline::pipeline::tests::direct_store;
        use weaver_server_pipeline::pipeline::tests::single_member_rar4_old_numbering_set_ending;
        mod engines {
            use weaver_server_pipeline::pipeline::tests::engines::harness;
            use weaver_server_pipeline::pipeline::tests::engines::rar_fixtures as rar;
            mod classic_par2_index;
            mod classic_uu;
            mod classic_zip64;
            mod dashed_volumes;
            mod depth;
            mod frontless_rar;
            mod frontless_records;
            mod frontless_sets;
            mod lost_index;
            mod nameless_recovery;
            mod prefix_sets;
            mod rar4_end_records;
            mod renamed_held_volumes;
            mod sealed;
            mod sfv;
            mod status_rulings;
            mod unlisted_data;
            mod unnamed_index;
            mod unused_sets;
            mod volume_order;
            mod withheld_par2;
            mod zero_lost;
            mod zip;
            mod zip_par3;
        }
    }
}
