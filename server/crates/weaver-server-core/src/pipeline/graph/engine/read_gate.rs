// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::{Arc, mpsc};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use weaver_graph::{ByteRange, NodeId, VolumeIndex};

use crate::pipeline::graph::envelope::Shelf;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReadKey {
    pub(crate) scope: u64,
    pub(crate) node: NodeId,
    pub(crate) generation: u64,
    pub(crate) volume: VolumeIndex,
    pub(crate) range: ByteRange,
}

#[derive(Debug)]
pub(crate) enum ReadStep {
    Ready(ReadKey),
    Completed(ReadKey, Result<Vec<u8>, String>),
}

// One exact read of one running instance. Neither pause holds a shelf or
// file lock. Dropping the actor's gate disconnects both pauses.
pub(super) struct ReadPause {
    key: ReadKey,
    events: UnboundedSender<ReadStep>,
    proceed: mpsc::Receiver<()>,
}

pub(crate) struct ReadGate {
    pub(crate) key: ReadKey,
    events: UnboundedReceiver<ReadStep>,
    proceed: mpsc::Sender<()>,
    shelf: Arc<Shelf>,
}

impl ReadGate {
    pub(crate) async fn next(&mut self) -> ReadStep {
        self.events
            .recv()
            .await
            .expect("the scoped reader reports its step")
    }

    pub(crate) fn release(&self) {
        self.proceed.send(()).expect("the scoped reader is paused");
    }

    pub(crate) fn generation_is_current(&self) -> bool {
        self.shelf.generation() == self.key.generation
    }
}

impl ReadPause {
    pub(super) fn new(key: ReadKey, shelf: Arc<Shelf>) -> (Self, ReadGate) {
        let (send, events) = unbounded_channel();
        let (proceed, receive) = mpsc::channel();
        (
            Self {
                key,
                events: send,
                proceed: receive,
            },
            ReadGate {
                key,
                events,
                proceed,
                shelf,
            },
        )
    }

    pub(super) fn matches(&self, generation: u64, volume: VolumeIndex, range: ByteRange) -> bool {
        self.key.generation == generation && self.key.volume == volume && self.key.range == range
    }

    pub(super) fn ready(&self) {
        if self.events.send(ReadStep::Ready(self.key)).is_ok() {
            let _ = self.proceed.recv();
        }
    }

    pub(super) fn completed(self, bytes: Result<Vec<u8>, String>) {
        if self
            .events
            .send(ReadStep::Completed(self.key, bytes))
            .is_ok()
        {
            let _ = self.proceed.recv();
        }
    }
}
