// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::{
    Format,
    extended::{graph::repair::graph_par3_campaign, repair::Slice},
};

// Shards cut each campaign to well under a thousand cases, so no test
// approaches the runner's per-test limit. Each shard calls `$run` with the
// bracketed arguments and its slice.
macro_rules! par3_shards {
    ($module:ident, $run:path, $args:tt) => {
        mod $module {
            use super::*;
            par3_shards!(@each $run, $args, 8;
                shard_0 0, shard_1 1, shard_2 2, shard_3 3,
                shard_4 4, shard_5 5, shard_6 6, shard_7 7);
        }
    };
    (@each $run:path, $args:tt, $of:literal; $($name:ident $shard:literal),+) => {
        $(
            #[tokio::test]
            #[ignore = "opt-in extended archive matrix; run with the archive-matrix-extended Nextest profile and --run-ignored all"]
            async fn $name() {
                par3_shards!(@call $run, $args, Slice::Shard { shard: $shard, of: $of });
            }
        )+
    };
    (@call $run:path, [$($arg:expr),*], $selection:expr) => {
        $run($($arg,)* $selection).await
    };
}

macro_rules! graph_par3_smoke {
    ($($name:ident $format:ident),+ $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                graph_par3_campaign(Format::$format, Slice::Smoke).await;
            }
        )+
    };
}

graph_par3_smoke!(
    par3_rar4_loss_smoke Rar4,
    par3_rar5_loss_smoke Rar5,
    par3_rar4_encrypted_loss_smoke Rar4Encrypted,
    par3_rar5_encrypted_loss_smoke Rar5Encrypted,
    par3_rar4_four_volume_loss_smoke Rar4FourVolumes,
    par3_rar5_four_volume_loss_smoke Rar5FourVolumes,
    par3_rar5_obfuscated_loss_smoke Rar5Obfuscated,
);

par3_shards!(combined_par3_rar4, graph_par3_campaign, [Format::Rar4]);
par3_shards!(combined_par3_rar5, graph_par3_campaign, [Format::Rar5]);
par3_shards!(
    combined_par3_rar4_encrypted,
    graph_par3_campaign,
    [Format::Rar4Encrypted]
);
par3_shards!(
    combined_par3_rar5_encrypted,
    graph_par3_campaign,
    [Format::Rar5Encrypted]
);
par3_shards!(
    combined_par3_rar4_four_volume,
    graph_par3_campaign,
    [Format::Rar4FourVolumes]
);
par3_shards!(
    combined_par3_rar5_four_volume,
    graph_par3_campaign,
    [Format::Rar5FourVolumes]
);
par3_shards!(
    combined_par3_rar5_obfuscated,
    graph_par3_campaign,
    [Format::Rar5Obfuscated]
);
