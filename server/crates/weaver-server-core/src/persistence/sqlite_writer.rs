// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::Arc;

use tokio::sync::{Mutex, MutexGuard};

use crate::persistence::wal_checkpoint::WalSignal;

// One per SQLite database: every write takes it, so writes wait their turn
// here instead of in SQLite's busy handler.
pub(crate) type SqliteWriterGate = Arc<WriterGate>;

pub(crate) struct WriterGate {
    lock: Mutex<()>,
    // A file database's WAL: a write that takes the gate waits for the
    // checkpoint a commit past the threshold asked for.
    wal: Option<&'static WalSignal>,
    // How many callers wait for the gate, so a test can wait until a write
    // has parked here.
    #[cfg(test)]
    waiting: tokio::sync::watch::Sender<usize>,
}

impl WriterGate {
    pub(crate) async fn lock(&self) -> MutexGuard<'_, ()> {
        #[cfg(test)]
        let _waiting = Waiting::new(&self.waiting);
        let guard = self.lock.lock().await;
        // Checked once the gate is ours, so a write already queued on it
        // cannot slip in after the commit that crossed the threshold. The
        // checkpoint task never takes the gate, so this wait always ends.
        if let Some(wal) = self.wal {
            wal.caught_up().await;
        }
        guard
    }

    #[cfg(test)]
    pub(crate) fn wal(&self) -> Option<&'static WalSignal> {
        self.wal
    }

    #[cfg(test)]
    pub(crate) async fn until_waiting(&self, count: usize) {
        let mut waiting = self.waiting.subscribe();
        let _ = waiting.wait_for(|waiting| *waiting >= count).await;
    }
}

#[cfg(test)]
struct Waiting<'a>(&'a tokio::sync::watch::Sender<usize>);

#[cfg(test)]
impl<'a> Waiting<'a> {
    fn new(waiting: &'a tokio::sync::watch::Sender<usize>) -> Self {
        waiting.send_modify(|waiting| *waiting += 1);
        Self(waiting)
    }
}

#[cfg(test)]
impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.send_modify(|waiting| *waiting -= 1);
    }
}

pub(crate) fn new_writer_gate(wal: Option<&'static WalSignal>) -> SqliteWriterGate {
    Arc::new(WriterGate {
        lock: Mutex::new(()),
        wal,
        #[cfg(test)]
        waiting: tokio::sync::watch::channel(0).0,
    })
}

#[cfg(test)]
mod tests;
