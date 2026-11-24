// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use super::blocks::hash_landed_piece;
use super::blocks::land_blocks;
use super::blocks::{piece_passing, piece_stored, settle_landed_piece};
use std::sync::Arc;

use crate::core::lengths::ChunkInfo;
use tracing::{debug, error};

use crate::{
    disk::DiskJob, file_ops::Verdict, peer_connection::WriterRequest, session_stats::disk::DiskOp,
    type_aliases::PeerHandle,
};

use super::peer::PeerTx;

use super::{HashJob, Landed, PieceCheck, Stored, StoredPiece, WriteJob, Written};

impl Drop for Written {
    fn drop(&mut self) {
        if let Some(disk) = self.0.take() {
            disk.written();
        }
    }
}

impl DiskJob for WriteJob {
    // One torrent's writes run on one thread at a time, in the order
    // queued: writes racing on the same files cost each other more than
    // they gain.
    fn key(&self) -> usize {
        Arc::as_ptr(&self.state) as usize
    }

    fn run(self) {
        let mut one = std::collections::VecDeque::from([self]);
        Self::run_batch(&mut one);
    }

    // Each run of blocks is landed as one batch.
    fn run_batch(batch: &mut std::collections::VecDeque<Self>) {
        WriteJob::land_batch(batch);
    }
}

impl WriteJob {
    // The batch is landed a torrent at a time: each run of jobs of one
    // torrent takes the torrent's lock twice, not twice per block.
    fn land_batch(batch: &mut std::collections::VecDeque<Self>) {
        let mut landed = Vec::new();
        while let Some(head) = batch.front() {
            let state = head.state.clone();
            let run = batch
                .iter()
                .take_while(|job| Arc::ptr_eq(&job.state, &state))
                .count();
            let jobs: Vec<WriteJob> = batch.drain(..run).collect();
            // A torrent paused or removed writes nothing more: the blocks
            // are fetched again in a later run (libtorrent aborts the jobs
            // of a storage it closes, `peer_connection::on_disk_write_complete`
            // marks the block cancelled). `land_blocks` refuses them under
            // the run's fence; here they are only spared its count.
            if state.cancellation_token.is_cancelled() {
                drop(jobs);
                continue;
            }
            landed.clear();
            {
                let call = state.shared.spawner.stats().call(DiskOp::BlockWrite);
                call.bytes(jobs.iter().map(|job| u64::from(job.chunk.size)).sum());
                let pieces: Vec<_> = jobs.iter().map(|job| job.block.piece()).collect();
                let blocks: Vec<_> = jobs
                    .iter()
                    .zip(&pieces)
                    .map(|(job, piece)| (job.addr, piece, job.chunk))
                    .collect();
                land_blocks(&state, &blocks, &mut landed);
            }
            for (job, landed) in jobs.into_iter().zip(landed.drain(..)) {
                job.landed(landed);
            }
        }
    }
}

impl WriteJob {
    // The block is written or dropped: its buffer goes back, and when it was
    // its piece's last, the piece is checked on a hashing thread, so the
    // writes queued behind it do not wait for the check (libtorrent
    // `torrent::verify_piece` posts `async_hash`).
    fn landed(self, landed: Landed) {
        let WriteJob {
            state,
            tx,
            addr,
            counters,
            chunk,
            block,
            room,
            written,
            ended,
            _run,
        } = self;
        drop(block);
        drop(room);
        drop(written);
        match landed {
            Ok(None) => {}
            Ok(Some((time, senders, check))) => {
                let spawner = state.shared.spawner.clone();
                spawner.hash().push(HashJob {
                    check,
                    state,
                    tx,
                    addr,
                    counters,
                    chunk,
                    time,
                    senders,
                    waiting: None,
                    written: Written(None),
                    queued: Some(spawner.check_queued()),
                    ended,
                    stored: None,
                    _run,
                });
            }
            Err(e) => chunk_failed(&tx, addr, &chunk, e),
        }
    }
}

// The connection that sent the block ends for its error.
fn chunk_failed(tx: &PeerTx, addr: PeerHandle, chunk: &ChunkInfo, e: anyhow::Error) {
    debug!(?addr, "error processing received chunk {chunk:?}: {e:#}");
    let _ = tx.send(WriterRequest::Disconnect(Err(
        e.context(format!("error processing received chunk {chunk:?}"))
    )));
}

impl PieceCheck {
    fn done(mut self) {
        self.state = None;
    }
}

impl Drop for PieceCheck {
    fn drop(&mut self) {
        // A run that has ended has queued its pieces again itself, and its
        // storage is no longer this check's to tell.
        if let Some(state) = self.state.take()
            && let Some(_turn) = state.storage_turn()
            && let Err(e) = state.requeue_piece(self.piece)
        {
            debug!(piece = ?self.piece, "a dropped check: {e:#}");
        }
    }
}

impl DiskJob for HashJob {
    // The checks of one torrent share nothing but its lock, taken for the
    // verdict. The torrent is the key, so the session's torrents are served
    // in turn: the hashing thread takes one check of a torrent, and
    // a torrent with more waiting goes behind the others that wait, so none
    // holds the thread from another however many pieces it has queued.
    const SIDE_BY_SIDE: bool = true;

    fn key(&self) -> usize {
        Arc::as_ptr(&self.state) as usize
    }

    fn run(mut self) {
        let since = std::time::Instant::now();
        self.queued = None;
        let state = self.state.clone();
        // A check whose run has ended is dropped unrun.
        let Some(_turn) = state.storage_turn() else {
            return;
        };
        let stats = state.shared.spawner.stats().clone();
        // Back from its storage: the piece is had, or not, here.
        if let Some(stored) = self.stored.take() {
            match stored {
                Stored::Written(written, epoch) => self.stored(written, epoch),
                // Dropped unwritten: fetched again as the job is dropped
                // (`PieceCheck`).
                Stored::Dropped => drop(self),
            }
            stats.hash_thread_worked(since);
            return;
        }
        // A block of the piece its storage took and then failed to write,
        // as when a piece given back went to its files, is a disk's error,
        // not a sender's: the piece is not checked, and goes the way of one
        // the storage refused, its senders neither credited nor blamed. Those
        // writes were made when the piece was given back, so the refusal is
        // read against the upload mode count of then: one made before upload
        // mode was last left enters nothing, as any refused write is read.
        let given_back = state.take_given_back(self.chunk.piece_index.get());
        if let Err(e) = state.files.piece_written(self.chunk.piece_index) {
            self.stored(Err(e), given_back);
            stats.hash_thread_worked(since);
            return;
        }
        match hash_landed_piece(&self.state, self.chunk.piece_index) {
            // A piece its storage holds in memory is handed to the storage
            // whole now, which writes it on a thread of its own, in the
            // order the checks ended, with the hashing thread on to the
            // next check (libtorrent hashes on `hashing_threads` and writes
            // on `aio_threads`, mmap_disk_io.cpp:369-374). It is had once
            // the storage says it wrote it.
            // A run told to end meanwhile hands its storage nothing more:
            // the piece is fetched again in a later run, its room kept.
            Ok(Verdict::Passed)
                if self.state.files.writes_to_memory()
                    && state.cancellation_token.is_cancelled() => {}
            Ok(Verdict::Passed) if self.state.files.writes_to_memory() => {
                let piece = self.chunk.piece_index;
                let epoch = piece_passing(&state, piece);
                let call = stats.call(DiskOp::PieceWrite);
                call.bytes(u64::from(state.lengths.piece_length(piece)));
                state.files.store_verified_piece(
                    piece,
                    StoredPiece {
                        job: Some(self),
                        epoch,
                    },
                );
                drop(call);
            }
            Ok(verdict) => self.settle(verdict),
            // A check that gave no verdict, its piece unread: the torrent
            // ends in error, as a write's error does (libtorrent
            // `torrent::on_piece_verified` hands a hash job's error to
            // `handle_disk_error`, which sets the error and pauses). Its
            // connection ends for the error, and the piece goes back to the
            // queue as the job is dropped (`PieceCheck`).
            Err(e) => {
                error!(
                    id = state.shared.id,
                    info_hash = ?state.shared.info_hash,
                    "FATAL: error checking a piece written: {e:#}"
                );
                chunk_failed(&self.tx, self.addr, &self.chunk, anyhow::anyhow!("{e:#}"));
                let _ = state.on_fatal_error(e);
            }
        }
        stats.hash_thread_worked(since);
    }
}

impl HashJob {
    // The piece's verdict is acted on, and its check is over.
    fn settle(self, verdict: Verdict) {
        self.end(|state, addr, counters, chunk, time, senders| {
            settle_landed_piece(state, addr, counters, chunk, time, senders, verdict)
        });
    }

    // The piece passed, and its storage wrote it or refused it: it is had,
    // or fetched again, and its check is over.
    fn stored(self, written: anyhow::Result<()>, epoch: u64) {
        self.end(|state, _, counters, chunk, time, senders| {
            piece_stored(state, counters, chunk, time, senders, epoch, written)
        });
    }

    fn end(
        self,
        act: impl FnOnce(
            &super::TorrentStateLive,
            PeerHandle,
            &super::AtomicPeerCounters,
            &ChunkInfo,
            Option<std::time::Duration>,
            crate::piece_tracker::Senders,
        ) -> anyhow::Result<()>,
    ) {
        let HashJob {
            state,
            tx,
            addr,
            counters,
            chunk,
            time,
            senders,
            check,
            waiting,
            written,
            queued: _,
            ended,
            stored: _,
            _run,
        } = self;
        let settled = act(&state, addr, &counters, &chunk, time, senders);
        check.done();
        // The piece waits no more before its connection is told so.
        drop(waiting);
        drop(written);
        if let Err(e) = settled {
            chunk_failed(&tx, addr, &chunk, e);
        }
        drop(ended);
    }
}

impl StoredPiece {
    /// The storage's write of the piece ended: `written` is its result.
    /// The piece goes back to its torrent's hashing thread, which has it,
    /// or fetches it again on an error, as a write's error is handled.
    /// Called from any thread; it waits on nothing.
    pub fn stored(mut self, written: anyhow::Result<()>) {
        let epoch = self.epoch;
        self.back(Stored::Written(written, epoch));
    }

    fn back(&mut self, stored: Stored) {
        if let Some(mut job) = self.job.take() {
            job.stored = Some(stored);
            let hash = job.state.shared.spawner.hash().clone();
            hash.push(job);
        }
    }
}

impl Drop for StoredPiece {
    // Dropped unsaid: the piece was not written.
    fn drop(&mut self) {
        self.back(Stored::Dropped);
    }
}
