// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// These are test names, not runner jobs. Nextest partitions the named shards
// across the bounded CI runner matrix. Every shard is independently replayable.
macro_rules! combined_campaign {
    ($module:ident, $variant:expr, $run:ident) => {
        mod $module {
            use super::*;
            combined_campaign!(@shards $variant, $run);
            // A test of its own, so no shard carries the password schedules.
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn wrong_password() {
                $run($variant, Selection::WrongPassword).await;
            }
        }
    };
    (@shards $variant:expr, $run:ident) => {
        combined_campaign!(
            @each $variant, $run;
                shard_00 0,
                shard_01 1,
                shard_02 2,
                shard_03 3,
                shard_04 4,
                shard_05 5,
                shard_06 6,
                shard_07 7,
                shard_08 8,
                shard_09 9,
                shard_10 10,
                shard_11 11,
                shard_12 12,
                shard_13 13,
                shard_14 14,
                shard_15 15,
                shard_16 16,
                shard_17 17,
                shard_18 18,
                shard_19 19,
                shard_20 20,
                shard_21 21,
                shard_22 22,
                shard_23 23,
                shard_24 24,
                shard_25 25,
                shard_26 26,
                shard_27 27,
                shard_28 28,
                shard_29 29,
                shard_30 30,
                shard_31 31,
                shard_32 32,
                shard_33 33,
                shard_34 34,
                shard_35 35,
                shard_36 36,
                shard_37 37,
                shard_38 38,
                shard_39 39,
                shard_40 40,
                shard_41 41,
                shard_42 42,
                shard_43 43,
                shard_44 44,
                shard_45 45,
                shard_46 46,
                shard_47 47,
                shard_48 48,
                shard_49 49,
                shard_50 50,
                shard_51 51,
                shard_52 52,
                shard_53 53,
                shard_54 54,
                shard_55 55,
                shard_56 56,
                shard_57 57,
                shard_58 58,
                shard_59 59,
                shard_60 60,
                shard_61 61,
                shard_62 62,
                shard_63 63
        );
    };
    (@each $variant:expr, $run:ident; $($name:ident $shard:literal),+) => {
        $(
            #[tokio::test]
            #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
            async fn $name() {
                $run($variant, Selection::Shard($shard)).await;
            }
        )+
    };
}
