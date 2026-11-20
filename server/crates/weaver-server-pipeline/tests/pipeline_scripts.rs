// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod pipeline {
    mod tests {
        mod delivered_restore;
        #[cfg(unix)]
        mod post_processing_completion;
        mod script_events;
    }
}
