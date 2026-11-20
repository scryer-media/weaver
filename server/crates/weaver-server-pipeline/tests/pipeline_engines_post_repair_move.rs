// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod pipeline {
    mod tests {
        mod engines {
            #[path = "rar_post_repair_move.rs"]
            mod rar;
        }
    }
}
