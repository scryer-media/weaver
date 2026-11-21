// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod pipeline {
    mod tests {
        mod archive_output_contract;
        mod archive_passwords;
        mod archive_topology;
        mod bandwidth_split;
        mod core;
        mod full_file_names;
        mod hardware_profile;
        mod idle;
        mod memory_release;
        mod post_processing_pause;
        mod provider_scheduling;
        mod server_attribution;
        mod terminal_settlement;
        mod direct_store {
            mod scenarios {
                mod sevenz_store;
            }
            mod tests {
                mod lent_store_loss;
            }
        }
    }
}
