// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#[macro_use]
#[path = "archive_matrix/depth_shards.rs"]
mod depth_shards;

#[macro_use]
#[path = "archive_matrix/grouping_listing.rs"]
mod grouping_listing;

mod pipeline {
    mod tests {
        mod direct_store {
            mod scenarios {
                mod archive_schedules {
                    mod extended {
                        mod graph {
                            mod depth;
                        }
                    }
                    mod graph {
                        mod grouping;
                    }
                }
            }
        }
    }
}
