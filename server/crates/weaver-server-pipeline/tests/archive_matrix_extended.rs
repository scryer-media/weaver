// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

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
        mod direct_store {
            mod scenarios {
                mod archive_schedules {
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
            }
        }
    }
}
