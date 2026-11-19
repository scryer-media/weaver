// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Only a missing fold needs a worker. The sparse obligation stays with the
// namespace until an answer for the same backing and bytes is accepted.

use std::fs::File;

use super::*;
use crate::operations::disk::reader::{Reading, TurnStop};
use crate::operations::disk::volume::Volume;
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
    // Everything submitted for the file when the check was lent: the
    // worker reads once it has returned.
    settled: Option<crate::operations::disk::writer::Barrier>,
    // The volume the file lies on: the check reads in its turns.
    volume: Option<Arc<Volume>>,
    pub(crate) stop: Arc<TurnStop>,
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
        this.run_with(shutdown, |_len| {
            #[cfg(test)]
            checked::read(_len);
            #[cfg(test)]
            if let Some(barrier) = barrier.take() {
                let _ = barrier.started.send(());
                let _ = barrier.resume.recv();
            }
        })
    }

    pub(super) fn run_with(self, shutdown: &Shutdown, mut read: impl FnMut(usize)) -> Verified {
        let stopped = || self.stop.raised() || shutdown.started();
        let io = |source| LeafError::Io {
            device: crate::pipeline::graph::limits::Device::Working,
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
            // The check runs on a thread of its own, which the loop never
            // waits for.
            if let Some(settled) = &self.settled {
                settled.wait_aside();
                // A write of the file failed: there is nothing to check,
                // and the failure fails the job.
                if settled.unwritten() {
                    return Ok(None);
                }
            }
            let file = File::open(&self.path).map_err(io)?;
            let mut bytes = vec![0; READ_BACK as usize];
            let mut at = 0;
            let mut crc = 0;
            // Each read in a turn of the file's volume: on one with more
            // than one turn, the one the check took first and keeps until
            // it returns.
            let reads = self.volume.as_deref().map(Volume::read_turns);
            let mut reading = Reading::default();
            while at < self.length {
                if stopped() {
                    return Ok(None);
                }
                let len = (self.length - at).min(READ_BACK) as usize;
                let into = &mut bytes[..len];
                // A wait for a turn ends when the job stops the check.
                match reads {
                    Some(reads) => {
                        match reading
                            .read_unless(reads, &self.stop, || read_exact_at(&file, at, into))
                        {
                            Some(read) => read,
                            None => return Ok(None),
                        }
                    }
                    None => read_exact_at(&file, at, into),
                }
                .map_err(io)?;
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
    pub(crate) fn verifications_due(&self, tree: &Tree, coming: &Coming) -> bool {
        self.verifications
            .iter()
            .any(|&(node, member)| self.owed(tree, coming, node, member))
    }

    // The namespace ended: its obligations nothing can meet are dropped.
    pub(crate) fn drop_unmet_verifications(&mut self, tree: &Tree, coming: &Coming) {
        let unmet: Vec<(NodeId, MemberId)> = self
            .verifications
            .iter()
            .copied()
            .filter(|&(node, member)| !self.owed(tree, coming, node, member))
            .collect();
        for key in unmet {
            self.verifications.remove(&key);
        }
    }

    fn owed(&self, tree: &Tree, coming: &Coming, node: NodeId, member: MemberId) -> bool {
        !self.ended(tree, node, coming) || self.verification(tree, node, member).is_some()
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
            settled: leaf.settled(),
            volume: leaf.volume().cloned(),
            stop: Arc::default(),
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
        if !self.verifications.contains(&key) || check.stop.raised() {
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
            .fold_read(check.length, Digest::crc32(actual));
        self.verifications.remove(&key);
        if actual != check.stated {
            let mut effects = Vec::new();
            tree.checksum_failed(check.node, check.member, &mut effects);
            self.apply_each(tree, builder, segments, effects, Write::Again, out)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod turns {
    use std::sync::Arc;

    use weaver_graph::{Coverage, MemberId, NodeId};

    use super::{READ_BACK, Verification};
    use crate::operations::disk::StorageKind;
    use crate::operations::disk::reader::{SOLID_READERS, SPINNING_READERS};
    use crate::operations::disk::volume::Volume;
    use crate::pipeline::graph::hardware::Shutdown;

    fn check(path: std::path::PathBuf, length: u64, volume: Option<Arc<Volume>>) -> Verification {
        Verification {
            node: NodeId(1),
            member: MemberId(0),
            path,
            revision: (0, 0),
            length,
            stated: 0,
            coverage: Coverage::default(),
            folded: None,
            settled: None,
            volume,
            stop: Arc::default(),
            barrier: None,
        }
    }

    // A member's check made in turns of its file's volume reads what the
    // check made without them reads, read for read, and comes to the same
    // CRC, or the same error. On solid state it holds one turn across its
    // reads; on a spinning volume each read takes the one turn and gives it
    // back. Every turn is back once it returns.
    #[test]
    fn a_check_in_its_volumes_turns_reads_what_a_check_without_them_reads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("member.bin");
        let length = 2 * READ_BACK + READ_BACK / 2 + 7;
        let body: Vec<u8> = (0..length).map(|at| (at * 31 % 251) as u8).collect();
        std::fs::write(&path, &body).unwrap();
        let shutdown = Shutdown::default();
        let run = |volume: Option<Arc<Volume>>, path: std::path::PathBuf| {
            let mut reads = Vec::new();
            let mut free = Vec::new();
            let watched = volume.clone();
            let verified = check(path, length, volume).run_with(&shutdown, |len| {
                reads.push(len);
                if let Some(volume) = &watched {
                    free.push(volume.read_turns().turns().available_permits());
                }
            });
            let actual = verified.actual.map_err(|error| {
                matches!(
                    error,
                    crate::pipeline::graph::leaf::LeafError::Io { ref source, .. }
                        if source.kind() == std::io::ErrorKind::NotFound
                )
            });
            (actual, reads, free)
        };
        let (old, old_reads, _) = run(None, path.clone());
        let crc = crc_turbo::checksum(crc_turbo::CrcAlgorithm::Crc32IsoHdlc, &body) as u32;
        assert_eq!(old, Ok(Some(crc)));
        assert_eq!(old_reads.len(), 3);
        let (old_missing, _, _) = run(None, dir.path().join("gone.bin"));
        assert_eq!(old_missing, Err(true));
        for (kind, during, all) in [
            (StorageKind::Solid, SOLID_READERS - 1, SOLID_READERS),
            (StorageKind::Unknown, SOLID_READERS - 1, SOLID_READERS),
            (StorageKind::Rotational, SPINNING_READERS, SPINNING_READERS),
        ] {
            let volume = Arc::new(Volume::new(None, "test".to_owned()));
            volume.reads(|| kind);
            let (new, new_reads, free) = run(Some(Arc::clone(&volume)), path.clone());
            assert_eq!(new, old, "{kind:?}");
            assert_eq!(new_reads, old_reads, "{kind:?}");
            assert_eq!(free, vec![during; old_reads.len()], "{kind:?}");
            assert_eq!(volume.read_turns().turns().available_permits(), all);
            let (missing, _, _) = run(Some(Arc::clone(&volume)), dir.path().join("gone.bin"));
            assert_eq!(missing, old_missing, "{kind:?}");
            assert_eq!(volume.read_turns().turns().available_permits(), all);
        }
    }

    // A check waiting for a turn that the volume's other readers hold ends
    // when its job stops it, having read nothing, on solid state and on a
    // spinning volume alike.
    #[test]
    fn a_check_waiting_for_a_turn_ends_when_it_is_stopped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("member.bin");
        std::fs::write(&path, vec![7; 64]).unwrap();
        for kind in [StorageKind::Solid, StorageKind::Rotational] {
            let volume = Arc::new(Volume::new(None, "test".to_owned()));
            let reads = volume.reads(|| kind);
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            let every = runtime
                .block_on(Arc::clone(reads.turns()).acquire_many_owned(reads.readers() as u32))
                .unwrap();
            let check = check(path.clone(), 64, Some(Arc::clone(&volume)));
            let stop = Arc::clone(&check.stop);
            let waiting = std::thread::spawn(move || {
                let mut read = 0;
                let verified = check.run_with(&Shutdown::default(), |len| read += len);
                (verified.actual.ok(), read)
            });
            stop.await_waiters(1);
            stop.raise();
            assert_eq!(waiting.join().unwrap(), (Some(None), 0), "{kind:?}");
            drop(every);
            assert_eq!(
                reads.turns().available_permits(),
                reads.readers(),
                "{kind:?}"
            );
        }
    }
}

// The bytes every member check in this process read from its file, so a
// test can hold a check to what it reads.
#[cfg(test)]
pub(crate) mod checked {
    use std::sync::atomic::{AtomicU64, Ordering};

    static READ: AtomicU64 = AtomicU64::new(0);

    pub(super) fn read(len: usize) {
        READ.fetch_add(len as u64, Ordering::Relaxed);
    }

    pub(crate) fn so_far() -> u64 {
        READ.load(Ordering::Relaxed)
    }
}
