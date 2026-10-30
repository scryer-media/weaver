// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Only a missing fold needs a worker. The sparse obligation stays with the
// namespace until an answer for the same backing and bytes is accepted.

use std::fs::File;
use std::sync::atomic::{AtomicBool, Ordering};

use super::*;
use crate::pipeline::graph::hardware::Shutdown;
use crate::pipeline::graph::leaf::read_exact_at;
use weaver_graph::Digest;

#[derive(Debug)]
pub(crate) struct Verification {
    pub(crate) node: NodeId,
    member: MemberId,
    path: PathBuf,
    revision: (u64, u64),
    length: u64,
    stated: u32,
    coverage: Coverage,
    folded: Option<u32>,
    pub(crate) stop: Arc<AtomicBool>,
    #[cfg(test)]
    barrier: Option<ReadBarrier>,
}

#[derive(Debug)]
pub(crate) struct Verified {
    check: Verification,
    actual: Result<Option<u32>, LeafError>,
}

#[cfg(test)]
#[derive(Debug)]
struct ReadBarrier {
    started: tokio::sync::oneshot::Sender<()>,
    resume: std::sync::mpsc::Receiver<()>,
}

impl Verification {
    #[cfg(test)]
    pub(crate) fn pause_after_read(
        &mut self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (started, ready) = tokio::sync::oneshot::channel();
        let (resume, wait) = std::sync::mpsc::channel();
        self.barrier = Some(ReadBarrier {
            started,
            resume: wait,
        });
        (ready, resume)
    }

    pub(crate) fn run(self, shutdown: &Shutdown) -> Verified {
        #[cfg(test)]
        let (mut barrier, this) = {
            let mut this = self;
            (this.barrier.take(), this)
        };
        #[cfg(not(test))]
        let this = self;
        this.run_with(shutdown, |_| {
            #[cfg(test)]
            if let Some(barrier) = barrier.take() {
                let _ = barrier.started.send(());
                let _ = barrier.resume.recv();
            }
        })
    }

    pub(super) fn run_with(self, shutdown: &Shutdown, mut read: impl FnMut(usize)) -> Verified {
        let stopped = || self.stop.load(Ordering::Relaxed) || shutdown.started();
        let io = |source| LeafError::Io {
            path: self.path.clone(),
            source,
        };
        let actual = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if stopped() {
                return Ok(None);
            }
            if let Some(folded) = self.folded {
                return Ok(Some(folded));
            }
            let file = File::open(&self.path).map_err(io)?;
            let mut bytes = vec![0; READ_BACK as usize];
            let mut at = 0;
            let mut crc = 0;
            while at < self.length {
                if stopped() {
                    return Ok(None);
                }
                let len = (self.length - at).min(READ_BACK) as usize;
                read_exact_at(&file, at, &mut bytes[..len]).map_err(io)?;
                read(len);
                if stopped() {
                    return Ok(None);
                }
                let part =
                    crc_turbo::checksum(crc_turbo::CrcAlgorithm::Crc32IsoHdlc, &bytes[..len]);
                crc = crc_turbo::checksum_combine(
                    crc_turbo::CrcAlgorithm::Crc32IsoHdlc,
                    crc,
                    part,
                    len as u64,
                );
                at += len as u64;
            }
            Ok(Some(crc as u32))
        }))
        .unwrap_or_else(|_| {
            Err(io(std::io::Error::other(
                "checksum verification worker panicked",
            )))
        });
        Verified {
            check: self,
            actual,
        }
    }
}

impl NamespaceWriter {
    pub(crate) fn verifications_waiting(&self) -> bool {
        !self.verifications.is_empty()
    }

    pub(crate) fn pending_verifications(&self) -> Vec<(u32, u32)> {
        self.verifications
            .iter()
            .map(|(node, member)| (node.0, member.0))
            .collect()
    }

    pub(crate) fn restore_verifications(&mut self, pending: &[(u32, u32)]) {
        self.verifications.extend(
            pending
                .iter()
                .map(|&(node, member)| (NodeId(node), MemberId(member))),
        );
    }

    // Whether an obligation can still be met or still waits to be: a check
    // of it can be lent now, or its namespace has not ended, so what keeps
    // it from a check (still open, short, or damaged) may yet clear. One of
    // an ended namespace no check can be lent for is met by nothing: the
    // census reads its member damaged or short, and a member whose file an
    // object took over is that object's to read.
    pub(crate) fn verifications_due(&self, tree: &Tree, payload_waiting: bool) -> bool {
        self.verifications
            .iter()
            .any(|&(node, member)| self.owed(tree, payload_waiting, node, member))
    }

    // The namespace ended: its obligations nothing can meet are dropped.
    pub(crate) fn drop_unmet_verifications(&mut self, tree: &Tree, payload_waiting: bool) {
        let unmet: Vec<(NodeId, MemberId)> = self
            .verifications
            .iter()
            .copied()
            .filter(|&(node, member)| !self.owed(tree, payload_waiting, node, member))
            .collect();
        for key in unmet {
            self.verifications.remove(&key);
        }
    }

    fn owed(&self, tree: &Tree, payload_waiting: bool, node: NodeId, member: MemberId) -> bool {
        !self.ended(tree, node, payload_waiting) || self.verification(tree, node, member).is_some()
    }

    // The member's stated CRC and length, when its bytes can be checked:
    // closed, every byte in, none damaged.
    fn checkable(&self, tree: &Tree, node: NodeId, member: MemberId) -> Option<(u32, u64)> {
        let record = tree.namespace(node)?.get(member)?;
        let (stated, length) = (record.crc32?, record.length?);
        let whole = ByteRange::new(0, length);
        let checkable =
            record.closed && record.coverage.covered(whole) && !record.coverage.damaged(whole);
        checkable.then_some((stated, length))
    }

    // No filesystem call: the worker opens the recorded path. A closed
    // member's bytes may still be rewritten; its return validates both the
    // file revision and the graph's verdict before using even an IO error.
    fn verification(&self, tree: &Tree, node: NodeId, member: MemberId) -> Option<Verification> {
        let (stated, length) = self.checkable(tree, node, member)?;
        let record = tree.namespace(node)?.get(member)?;
        let leaf = self.files.get(&node)?.leaves.get(&member)?;
        Some(Verification {
            node,
            member,
            path: leaf.path().to_owned(),
            revision: leaf.revision(),
            length,
            stated,
            coverage: record.coverage.clone(),
            folded: leaf
                .folded(length, Algorithm::Crc32)
                .map(|folded| folded as u32),
            stop: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            barrier: None,
        })
    }

    // Admission permits one outstanding loan per job. Do not remove its
    // obligation here: a checkpoint taken during the read must retain it.
    // One that cannot be checked yet stays too: a later arrival may clean
    // its member, and only a verdict or the namespace's end settles it.
    pub(crate) fn take_verification(&self, tree: &Tree) -> Option<Verification> {
        self.verifications
            .iter()
            .find_map(|&(node, member)| self.verification(tree, node, member))
    }

    pub(crate) fn verified(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        verified: Verified,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let Verified { check, actual } = verified;
        let key = (check.node, check.member);
        if !self.verifications.contains(&key) || check.stop.load(Ordering::Relaxed) {
            return Ok(());
        }
        // A member that can no longer be checked keeps its obligation for
        // when it can, as a stale read does for its next check.
        let Some(current) = self.verification(tree, check.node, check.member) else {
            return Ok(());
        };
        if current.revision != check.revision
            || current.path != check.path
            || current.length != check.length
            || current.stated != check.stated
            || current.coverage != check.coverage
        {
            return Ok(());
        }
        let Some(actual) = actual? else {
            return Ok(());
        };
        self.files_mut(check.node)
            .leaves
            .get_mut(&check.member)
            .expect("the current backing was checked above")
            .fold_passed(check.length, Digest::crc32(actual));
        self.verifications.remove(&key);
        if actual != check.stated {
            let mut effects = Vec::new();
            tree.checksum_failed(check.node, check.member, &mut effects);
            self.apply_each(tree, builder, segments, effects, Write::Again, out)?;
        }
        Ok(())
    }
}
