// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_server_settings::tags::*;

impl TagStore for crate::Database {
    fn tag_cache(&self) -> &TagCache {
        &self.tags
    }
}

#[cfg(test)]
pub(crate) mod tests;
