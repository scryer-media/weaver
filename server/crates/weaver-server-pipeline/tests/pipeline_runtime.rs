// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod pipeline {
    mod tests {
        mod archive_passwords;
        mod archive_topology;
        mod bandwidth_split;
        mod core;
        mod full_file_names;
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
