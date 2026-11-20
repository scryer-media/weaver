// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::{
    graph::grouping::{cell_campaign, cell_smoke},
    grouping::{Binding, Cell, Container, Naming, assert_generated},
};

// Emits one smoke under `graph::grouping::grouping_smoke` and one ignored
// campaign under `graph::grouping::combined_grouping` per listed cell, and
// the list itself as `GENERATED_CELLS`.
macro_rules! graph_grouping_cells {
    ($($tag:ident $c:ident $C:ident {
        $($n:ident $N:ident [$($b:ident $B:ident)*])*
    })*) => {
        const GENERATED_CELLS: &[Cell] = &[$($($(Cell {
            container: Container::$C,
            naming: Naming::$N,
            binding: Binding::$B,
        },)*)*)*];
        mod grouping_smoke {
            $(mod $c {
                $(mod $n {
                    use super::super::super::*;
                    $(
                        #[tokio::test]
                        async fn $b() {
                            cell_smoke(Cell {
                                container: Container::$C,
                                naming: Naming::$N,
                                binding: Binding::$B,
                            })
                            .await;
                        }
                    )*
                })*
            })*
        }
        mod combined_grouping {
            $(mod $c {
                $(mod $n {
                    use super::super::super::*;
                    $(
                        #[tokio::test]
                        #[ignore = "opt-in archive matrix; run with the archive-matrix Nextest profile and --run-ignored all"]
                        async fn $b() {
                            cell_campaign(Cell {
                                container: Container::$C,
                                naming: Naming::$N,
                                binding: Binding::$B,
                            })
                            .await;
                        }
                    )*
                })*
            })*
        }
    };
}

grouping_listing!(graph_grouping_cells);

// The generator emits a smoke and a campaign for exactly the possible cells.
#[test]
fn grouping_generator_covers_every_possible_cell() {
    assert_generated(GENERATED_CELLS);
}
