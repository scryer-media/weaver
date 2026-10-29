// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Only a missing fold needs a worker. The sparse obligation stays with the
// namespace until an answer for the same backing and bytes is accepted.

use std::fs::File;
use std::sync::atomic::{AtomicBool, Ordering};

use super::*;
use crate::pipeline::graph::hardware::Shutdown;
use crate::pipeline::graph::leaf::read_exact_at;

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

    // No filesystem call: the worker opens the recorded path. A closed
    // member's bytes may still be rewritten; its return validates both the
    // file revision and the graph's verdict before using even an IO error.
    fn verification(&self, tree: &Tree, node: NodeId, member: MemberId) -> Option<Verification> {
        let record = tree.namespace(node)?.get(member)?;
        let (stated, length) = (record.crc32?, record.length?);
        let whole = ByteRange::new(0, length);
        if !record.closed || !record.coverage.covered(whole) || record.coverage.damaged(whole) {
            return None;
        }
        let leaf = self.files.get(&node)?.leaves.get(&member)?;
        Some(Verification {
            node,
            member,
            path: leaf.path().to_owned(),
            revision: leaf.revision(),
            length,
            stated,
            coverage: record.coverage.clone(),
            folded: leaf.folded(length),
            stop: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            barrier: None,
        })
    }

    // Admission permits one outstanding loan per job. Do not remove its
    // obligation here: a checkpoint taken during the read must retain it.
    pub(crate) fn take_verification(&mut self, tree: &Tree) -> Option<Verification> {
        while let Some(&(node, member)) = self.verifications.first() {
            if let Some(check) = self.verification(tree, node, member) {
                return Some(check);
            }
            self.verifications.remove(&(node, member));
        }
        None
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
        let Some(current) = self.verification(tree, check.node, check.member) else {
            self.verifications.remove(&key);
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
            .fold_passed(check.length, actual);
        self.verifications.remove(&key);
        if actual != check.stated {
            let mut effects = Vec::new();
            tree.checksum_failed(check.node, check.member, &mut effects);
            self.apply_each(tree, builder, segments, effects, out)?;
        }
        Ok(())
    }
}
