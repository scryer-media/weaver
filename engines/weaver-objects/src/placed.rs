// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A store's placement number: one that moves whenever anything its
// `locate` reads may change, so its owner asks again only then. Every
// store's numbers come from one counter, so no two stores, a store restored
// in place of another included, ever share one.

use std::sync::atomic::{AtomicU64, Ordering};

static PLACED: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Placed(u64);

impl Default for Placed {
    fn default() -> Self {
        Self(PLACED.fetch_add(1, Ordering::Relaxed) + 1)
    }
}

impl Placed {
    // Something `locate` reads may have changed.
    pub(crate) fn moved(&mut self) {
        *self = Self::default();
    }

    pub(crate) fn number(&self) -> u64 {
        self.0
    }
}
