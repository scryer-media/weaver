// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::num::NonZeroUsize;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

/// Pending connects on a socket with no finite connections limit.
pub(super) const MAX_PENDING_CONNECTS: usize = 32;

/// How many connects may be pending at once: as many as the socket admits
/// streams, so a caller bounded by that limit never waits here.
pub(super) fn pending_connects(connections_limit: Option<NonZeroUsize>) -> usize {
    connections_limit.map_or(MAX_PENDING_CONNECTS, |limit| {
        limit.get().min(Semaphore::MAX_PERMITS)
    })
}

/// One permit follows a connect from its request queue through its handshake.
/// Established streams do not retain a permit.
pub(super) struct ConnectAdmission {
    permits: Arc<Semaphore>,
}

impl Default for ConnectAdmission {
    fn default() -> Self {
        Self::new(MAX_PENDING_CONNECTS)
    }
}

impl ConnectAdmission {
    pub(super) fn new(permits: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(permits)),
        }
    }

    #[cfg(test)]
    pub(super) fn available_permits(&self) -> usize {
        self.permits.available_permits()
    }

    pub(super) async fn acquire(
        &self,
        cancellation: &CancellationToken,
    ) -> crate::Result<OwnedSemaphorePermit> {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(crate::Error::DispatcherDead),
            permit = Arc::clone(&self.permits).acquire_owned() => permit.map_err(|_| crate::Error::DispatcherDead),
        }
    }
}

/// Notifications coalesce; the flags on current records are authoritative.
#[derive(Default)]
pub(crate) struct RetirementSignals {
    dirty: AtomicBool,
    notified: Notify,
}

impl RetirementSignals {
    pub(crate) fn registration(self: &Arc<Self>) -> (Arc<RetirementFlag>, RetirementGuard) {
        let flag = Arc::new(RetirementFlag {
            retired: AtomicBool::new(false),
            signals: Arc::clone(self),
        });
        (Arc::clone(&flag), RetirementGuard { flag: Some(flag) })
    }

    /// Arm `notifications().notified()` before taking dirty and scanning flags.
    /// A retirement racing that scan leaves another dirty notification.
    pub(crate) fn take_dirty(&self) -> bool {
        self.dirty.swap(false, Ordering::AcqRel)
    }

    pub(crate) fn notifications(&self) -> &Notify {
        &self.notified
    }
}

pub(crate) struct RetirementFlag {
    retired: AtomicBool,
    signals: Arc<RetirementSignals>,
}

impl RetirementFlag {
    pub(crate) fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }

    fn retire(&self) {
        if !self.retired.swap(true, Ordering::AcqRel) {
            self.signals.dirty.store(true, Ordering::Release);
            self.signals.notified.notify_one();
        }
    }
}

/// Owned by the attempt future or stream starter, separately from its map entry.
/// Retiring an old owner cannot address or remove a replacement map entry.
pub(crate) struct RetirementGuard {
    flag: Option<Arc<RetirementFlag>>,
}

impl RetirementGuard {
    pub(crate) fn disarm(&mut self) {
        self.flag.take();
    }
}

impl Drop for RetirementGuard {
    fn drop(&mut self) {
        if let Some(flag) = self.flag.take() {
            flag.retire();
        }
    }
}

#[cfg(test)]
mod tests;
