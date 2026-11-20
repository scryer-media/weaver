// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Decode windows on scratch memory. A member whose decoder window (a RAR,
// LZMA, LZMA2 or xz dictionary, a 7z coder's) is larger than the profile
// lets a decoder hold in RAM is lent a lease of the job's scratch
// (`weaver_server_disk::scratch`), so it decodes instead of being refused. No window over
// the scratch's ceiling is lent. A window no folder has room for is refused,
// and the profile's policy decides what follows: scratch-then-fail fails the
// job with the sizes, scratch-then-RAM starts the run again in RAM.
//
// What is the graph's own here: each node's run borrows through a lender
// handed for one start of it, and what that run asked is kept under that
// start, so the owner reads a refusal for want of room when the decoder's
// refusal reaches it.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use weaver_graph::{NodeId, WindowLender, WindowMemory};

use crate::operations::CapacityReader;
use weaver_server_disk::scratch::{Scratch, ScratchLedger};

impl crate::pipeline::Pipeline {
    // Weaver's data folder, the one holding its database, and the free
    // space of its disk: the second place a decode window may go.
    pub(crate) fn window_data_dir(&self) -> (PathBuf, CapacityReader, ScratchLedger) {
        (
            self.script_data_dir.clone(),
            self.storage_capacity
                .reader(crate::operations::StorageRoot::Data),
            self.window_ledgers.data.clone(),
        )
    }
}

// One job's decode windows: its scratch, and what each node's run asked of
// it under the start it was last handed a lender for.
pub(crate) struct Windows {
    scratch: Arc<Scratch>,
    // The starts its lenders were handed for, counted.
    starts: AtomicU64,
    asked: Mutex<HashMap<NodeId, Asked>>,
}

// One start of a run that was handed a lender: the lender knows it by this,
// so a run stopped before it, still asking, is not heard as the new one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Start(u64);

// What a run asked of its lender under one start.
struct Asked {
    start: Start,
    // The widest window it asked for, lent or refused.
    widest: u64,
    // Whether a window was refused because no folder had room for it.
    refused: bool,
}

impl std::fmt::Debug for Windows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&*self.scratch, f)
    }
}

impl Windows {
    // The windows of the job whose scratch is `scratch`.
    pub(crate) fn new(scratch: Arc<Scratch>) -> Self {
        Self {
            scratch,
            starts: AtomicU64::new(0),
            asked: Mutex::new(HashMap::new()),
        }
    }

    // The job's scratch, which its windows are lent from.
    #[cfg(test)]
    pub(crate) fn scratch(&self) -> &Arc<Scratch> {
        &self.scratch
    }

    // The windows lent now, and their bytes.
    #[cfg(test)]
    pub(crate) fn live(&self) -> (u64, u64) {
        self.scratch.live()
    }

    // Whether a quota refused a window's file since this was last asked.
    pub(crate) fn take_quota(&self) -> bool {
        self.scratch.take_quota()
    }

    // The free space every folder's disk keeps past any window.
    pub(crate) fn kept_free(&self) -> u64 {
        self.scratch.kept_free()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<NodeId, Asked>> {
        self.asked.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // What the node's run borrows its windows through, as its decoder asks
    // for them, and the start it is handed it for. A run handed it starts
    // with nothing asked on record; what a run handed one before asks from
    // here is not recorded.
    pub(crate) fn for_node(self: &Arc<Self>, node: NodeId) -> (Arc<dyn WindowLender>, Start) {
        let start = Start(self.starts.fetch_add(1, Ordering::Relaxed));
        self.lock().insert(
            node,
            Asked {
                start,
                widest: 0,
                refused: false,
            },
        );
        let lender = Arc::new(NodeWindows {
            windows: Arc::clone(self),
            node,
            start,
        });
        (lender, start)
    }

    // The widest window the node's run asked for under `start`, lent or
    // refused, once one was refused because no folder had room for it
    // (`io::ErrorKind::StorageFull`): `None` while none was.
    pub(crate) fn refused(&self, node: NodeId, start: Start) -> Option<u64> {
        self.lock()
            .get(&node)
            .filter(|asked| asked.start == start && asked.refused)
            .map(|asked| asked.widest)
    }
}

// A run's view of its job's windows, naming their files for its node.
struct NodeWindows {
    windows: Arc<Windows>,
    node: NodeId,
    start: Start,
}

impl NodeWindows {
    // What the run asked, kept under its start alone: a run started again
    // since was handed a lender of its own.
    fn record(&self, len: usize, refused: bool) {
        let mut asked = self.windows.lock();
        if let Some(asked) = asked
            .get_mut(&self.node)
            .filter(|asked| asked.start == self.start)
        {
            asked.widest = asked.widest.max(len as u64);
            asked.refused |= refused;
        }
    }
}

impl WindowLender for NodeWindows {
    // Every window lent is kept, and every one refused for want of room,
    // before the decoder hears it, so the owner reads them when the
    // decoder's refusal reaches it. A window that may not be lent at all
    // (empty, or over the ceiling) is the decoder's own refusal.
    fn lend(&self, len: usize) -> io::Result<Box<dyn WindowMemory>> {
        let lent = self.windows.scratch.allocate(self.node.0, len);
        match &lent {
            Ok(_) => self.record(len, false),
            Err(error) if error.kind() == io::ErrorKind::StorageFull => self.record(len, true),
            Err(_) => {}
        }
        Ok(Box::new(lent?))
    }

    fn ceiling(&self) -> u64 {
        self.windows.scratch.ceiling()
    }

    fn room(&self) -> Option<u64> {
        self.windows.scratch.room()
    }
}

// The lenders a run is handed, where windows can be mapped.
#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests;
