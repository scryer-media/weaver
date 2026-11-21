// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! A magnet's tracker task, from its metadata fetch to the run that follows.
//!
//! Fetching a torrent's metadata and then downloading it is one torrent to
//! its trackers: `started` once, as the fetch begins, and `stopped` once,
//! when the torrent stops. The fetch's task is held here while the metadata
//! is fetched and after, until the run that follows takes it over or the
//! holder lets go of it, which sends `stopped`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::task::Poll;

use futures::StreamExt;
use parking_lot::Mutex;

use crate::tracker::{TrackerStatus, TrackerTask};

struct Held {
    task: Mutex<Option<TrackerTask>>,
    // The peers the trackers gave the fetch, in the order given.
    found: Mutex<Vec<SocketAddr>>,
}

/// A magnet's tracker task, held from its metadata fetch for the run that
/// follows: pass it to that run's add in
/// [`AddTorrentOptions::tracker_handover`](crate::AddTorrentOptions::tracker_handover),
/// with the identity the fetch announced under. Dropping it before a run
/// takes it over ends the task and sends `stopped`.
pub struct TrackerHandover {
    held: Arc<Held>,
    status: TrackerStatus,
}

impl TrackerHandover {
    pub(crate) fn new(task: TrackerTask) -> Self {
        Self {
            status: task.status.clone(),
            held: Arc::new(Held {
                task: Mutex::new(Some(task)),
                found: Mutex::default(),
            }),
        }
    }

    /// The held task's announce table.
    pub(crate) fn status(&self) -> &TrackerStatus {
        &self.status
    }

    /// The held task as the fetch reads it: its announce table, and the
    /// peers it finds until a run takes it over. Dropping this sends
    /// nothing; the holder decides.
    pub(crate) fn task(&self) -> TrackerTask {
        let held = Arc::downgrade(&self.held);
        let peers = futures::stream::poll_fn(move |cx| {
            let Some(held) = held.upgrade() else {
                return Poll::Ready(None);
            };
            let mut task = held.task.lock();
            let Some(task) = task.as_mut() else {
                return Poll::Ready(None);
            };
            let next = task.peers.poll_next_unpin(cx);
            if let Poll::Ready(Some(addr)) = next {
                let mut found = held.found.lock();
                if !found.contains(&addr) {
                    found.push(addr);
                }
            }
            next
        });
        TrackerTask {
            peers: peers.boxed(),
            status: self.status.clone(),
        }
    }

    /// The task, for the run that starts now: the peers it gave the fetch
    /// come first, then those it finds from here on.
    pub(crate) fn take(self) -> Option<TrackerTask> {
        let task = self.held.task.lock().take()?;
        let found = std::mem::take(&mut *self.held.found.lock());
        Some(TrackerTask {
            peers: futures::stream::iter(found).chain(task.peers).boxed(),
            status: task.status,
        })
    }
}
