// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_server_pipeline::{history, jobs};

mod pipeline {
    mod tests {
        mod par2_completion;
        mod engines {
            pub(super) use weaver_server_pipeline::pipeline::tests::engines::{
                harness, restart_fixtures as restart,
            };
        }
        mod direct_store {
            pub(super) use weaver_server_pipeline::pipeline::tests::direct_store::single_member_store_set;
            pub(super) mod scenarios {
                pub(in crate::pipeline::tests) mod sevenz_store {
                    pub(in crate::pipeline::tests) use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::sevenz_store::{Entry, build_7z_shaped};
                }
            }
        }
    }
}
