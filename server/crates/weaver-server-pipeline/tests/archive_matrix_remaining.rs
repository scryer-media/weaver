// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_server_pipeline::{history, jobs};

macro_rules! cases {
    ($($name:ident),+ $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                fixture::$name().await;
            }
        )+
    };
}

macro_rules! sync_cases {
    ($($name:ident),+ $(,)?) => {
        $(
            #[test]
            fn $name() {
                fixture::$name();
            }
        )+
    };
}

mod pipeline {
    mod tests {
        mod par2;
        mod par2_completion;
        mod engines {
            pub(super) use weaver_server_pipeline::pipeline::tests::engines::{
                harness, restart_fixtures as restart,
            };
        }
        mod direct_store {
            pub(super) use weaver_server_pipeline::pipeline::tests::direct_store::single_member_store_set;
            mod tests {
                mod par2_schedules;
            }
            pub(super) mod scenarios {
                pub(super) use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::encrypted_store_set;
                mod archive_schedules {
                    mod graph;
                    mod extended {
                        mod depth;
                        mod graph {
                            mod formats;
                            mod repair;
                        }
                        mod repair;
                        mod tier2;
                    }
                }
                pub(in crate::pipeline::tests) mod sevenz_store {
                    pub(in crate::pipeline::tests) use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::sevenz_store::{Entry, build_7z_shaped};
                    mod schedules {
                        mod graph;
                    }
                }
            }
        }
    }
}
