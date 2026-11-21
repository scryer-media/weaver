// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::{DispatchFixture, Duration, Instant, JobId};

impl DispatchFixture {
    pub const LOG_INTERVAL: Duration = crate::pipeline::download::DISPATCH_INELIGIBLE_LOG_INTERVAL;

    pub fn last_ineligible_log(&self, id: JobId) -> Option<Instant> {
        self.0.dispatch_ineligible_log_throttle.last_emitted_at(id)
    }

    pub fn last_slot_wait_log(&self) -> Option<Instant> {
        self.0.dispatch_slot_wait_log_throttle.last_emitted_at(())
    }

    pub fn last_stall_log(&self) -> Option<Instant> {
        self.0.last_download_dispatch_stall_log_at
    }
}
