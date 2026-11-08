// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job as the lender of the work its objects hold that is too slow for
// the loop: a keystream run over held ciphertext, a key derived from a
// password. The object is lent out of its node, does its work on the
// post-processing pool, and comes back through the run mail with what it
// said, numbered by the loan; the tree applies that, then hears what waited
// for it. Each object is out at most once, and no slot is taken: the work
// is bounded by what the object held as it left.

use std::collections::BTreeMap;
use std::fmt;

use tokio::sync::mpsc;
use weaver_graph::{Emit, NodeId, Object};

use super::super::engine::{Heard, Said};
use super::super::hardware::Hardware;

pub(crate) struct Works {
    scope: u64,
    mail: mpsc::Sender<Said>,
    // The number the last loan went out under.
    serial: u64,
    // The loans out, by node, under their numbers.
    out: BTreeMap<NodeId, u64>,
    // A checkpoint found a loan out and took nothing: until one is
    // recorded, the work is done on the loop, so the next step takes it.
    owed: bool,
    hardware: Hardware,
}

// An object back from its work, with what it said.
pub(crate) struct Worked {
    pub(crate) object: Box<dyn Object>,
    pub(crate) said: Vec<Emit>,
}

impl fmt::Debug for Worked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Worked")
            .field("kind", &self.object.kind())
            .field("said", &self.said.len())
            .finish()
    }
}

impl Works {
    pub(crate) fn new(scope: u64, mail: mpsc::Sender<Said>, hardware: Hardware) -> Self {
        Self {
            scope,
            mail,
            serial: 0,
            out: BTreeMap::new(),
            owed: false,
            hardware,
        }
    }

    pub(crate) fn out(&self) -> bool {
        !self.out.is_empty()
    }

    // No work goes out once the pipeline is shutting down, so the last
    // checkpoint finds none of it out, nor while a checkpoint is owed.
    pub(crate) fn admit(&self) -> bool {
        !self.owed && !self.hardware.shutdown().started()
    }

    pub(crate) fn owe_checkpoint(&mut self) {
        self.owed = true;
    }

    pub(crate) fn checkpointed(&mut self) {
        self.owed = false;
    }

    // The loan numbered `serial` came back to `node`: it is no longer out.
    // `false` for anything else, which is not heard.
    pub(crate) fn returned(&mut self, node: NodeId, serial: u64) -> bool {
        if self.out.get(&node) == Some(&serial) {
            self.out.remove(&node);
            return true;
        }
        false
    }

    pub(crate) fn lend(&mut self, node: NodeId, mut object: Box<dyn Object>) {
        self.serial += 1;
        let (scope, serial) = (self.scope, self.serial);
        self.out.insert(node, serial);
        let mail = self.mail.clone();
        let grant = self.hardware.grant();
        tokio::task::spawn_blocking(move || {
            let _job = crate::runtime::panic_trap::JobScope::enter(crate::JobId(scope));
            let mut said = Vec::new();
            grant.install(|| object.work(&mut said));
            // A closed channel means the pipeline is gone, and no one is
            // left to tell.
            let _ = mail.blocking_send(Said {
                scope,
                node,
                generation: serial,
                heard: Heard::Worked(Box::new(Worked { object, said })),
            });
        });
    }
}
