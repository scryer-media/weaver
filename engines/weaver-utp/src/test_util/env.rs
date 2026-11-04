// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

use std::{sync::Arc, time::Duration};

use parking_lot::Mutex;
use tokio::time::Instant;

use crate::traits::UtpEnvironment;

pub struct MockRandom {
    pub current: u16,
}

impl Default for MockRandom {
    fn default() -> Self {
        Self { current: 1 }
    }
}

impl MockRandom {
    fn next(&mut self) -> u16 {
        let current = self.current;
        self.current = self.current.wrapping_add(100);
        current
    }
}

struct MockUtpEnvironmentInner {
    now: Instant,
    random: MockRandom,
}

#[derive(Clone)]
pub struct MockUtpEnvironment {
    inner: Arc<Mutex<MockUtpEnvironmentInner>>,
}

impl Default for MockUtpEnvironment {
    fn default() -> Self {
        Self::new()
    }
}

impl MockUtpEnvironment {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MockUtpEnvironmentInner {
                now: Instant::now(),
                random: Default::default(),
            })),
        }
    }

    pub fn increment_now(&self, dur: Duration) {
        self.inner.lock().now += dur;
    }
}

impl UtpEnvironment for MockUtpEnvironment {
    fn now(&self) -> Instant {
        self.inner.lock().now
    }

    fn copy(&self) -> Self {
        self.clone()
    }

    fn random_u16(&self) -> u16 {
        self.inner.lock().random.next()
    }
}
