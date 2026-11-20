// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#[macro_use]
#[path = "archive_matrix/shards.rs"]
mod shards;
mod pipeline {
    mod tests {
        mod direct_store {
            mod scenarios {
                mod archive_schedules {
                    mod extended {
                        mod tier2 {
                            mod damage {
                                mod combined_damage {
                                    use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::extended::tier2::damage::combined_damage::run_shard;
                                    tier2_shards!(run_shard, 4500;
        h00 0 h01 1 h02 2 h03 3 h04 4 h05 5 h06 6 h07 7 h08 8 h09 9 h10 10 h11 11 h12 12 h13 13 h14 14
        h15 15 h16 16 h17 17 h18 18 h19 19 h20 20 h21 21 h22 22 h23 23 h24 24 h25 25 h26 26 h27 27 h28 28
        h29 29 h30 30 h31 31 h32 32 h33 33 h34 34 h35 35 h36 36 h37 37 h38 38 h39 39 h40 40 h41 41 h42 42
        h43 43 h44 44);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
