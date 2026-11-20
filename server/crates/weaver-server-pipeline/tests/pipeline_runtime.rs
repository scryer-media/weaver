// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod pipeline {
    mod tests {
        mod archive_topology;
        mod core;
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
