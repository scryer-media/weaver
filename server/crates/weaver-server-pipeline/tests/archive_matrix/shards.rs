// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Emits `shards` ignored campaign tests for a family, as nested modules of
// hundreds, tens and units, so shard `h d s` runs shard `100h + 10d + s`.
macro_rules! tier2_shards {
    ($family:path, $shards:expr; $($h:ident $hv:literal)+) => {
        const _: () = assert!(100 * [$($hv),+].len() == $shards);
        $(mod $h {
            use super::*;
            tier2_shards!(@tens $family, $shards, $hv);
        })+
    };
    (@tens $family:path, $shards:expr, $hv:literal) => {
        tier2_shards!(@ten $family, $shards, $hv; d0 0 d1 1 d2 2 d3 3 d4 4 d5 5 d6 6 d7 7 d8 8 d9 9);
    };
    (@ten $family:path, $shards:expr, $hv:literal; $($d:ident $dv:literal)+) => {
        $(mod $d {
            use super::*;
            tier2_shards!(@units $family, $shards, $hv, $dv; s0 0 s1 1 s2 2 s3 3 s4 4 s5 5 s6 6 s7 7 s8 8 s9 9);
        })+
    };
    (@units $family:path, $shards:expr, $hv:literal, $dv:literal; $($s:ident $sv:literal)+) => {
        $(
            #[tokio::test]
            #[ignore = "opt-in extended archive matrix; run with the archive-matrix-extended Nextest profile and --run-ignored all"]
            async fn $s() {
                $family($hv * 100 + $dv * 10 + $sv, $shards).await;
            }
        )+
    };
}
