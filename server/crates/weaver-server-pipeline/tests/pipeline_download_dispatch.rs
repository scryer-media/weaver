// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod pipeline {
    mod tests {
        mod download_dispatch {
            mod alternate_message_ids;
            mod article_scheduler;
            mod article_slots;
            mod blocking_lane_guards;
            mod busy_turn_completions;
            mod checkpoint_progress;
            mod credentials_refusal;
            mod dispatch_ineligible_log;
            mod dispatch_server_order;
            mod first_article_gate;
            mod lane_depth_seeding;
            mod lane_failure_visibility;
            mod lane_holdings;
            mod owned_download_lane_pool;
            mod propagation_delay;
            mod server_ladder;
            mod stall_retry;
        }
    }
}
