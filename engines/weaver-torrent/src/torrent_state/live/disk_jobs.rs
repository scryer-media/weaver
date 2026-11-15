// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use super::blocks::hash_landed_piece;
use super::blocks::land_blocks;
use super::blocks::settle_landed_piece;
use std::sync::Arc;

use crate::core::lengths::ChunkInfo;
use tracing::{debug, error};

use crate::{
    disk::DiskJob, file_ops::Verdict, peer_connection::WriterRequest, session_stats::disk::DiskOp,
    type_aliases::PeerHandle,
};

use super::peer::PeerTx;

use super::{DiskWrite, HashJob, Landed, PieceCheck, PieceWrite, WriteJob, Written};

impl Drop for Written {
    fn drop(&mut self) {
        if let Some(disk) = self.0.take() {
            disk.written();
        }
    }
}

impl DiskJob for DiskWrite {
    // One torrent's writes run on one thread at a time, in the order
    // queued: writes racing on the same files cost each other more than
    // they gain, and whole pieces reach the files in the order their
    // checks ended.
    fn key(&self) -> usize {
        let state = match self {
            DiskWrite::Block(job) => &job.state,
            DiskWrite::Piece(PieceWrite(job)) => &job.state,
        };
        Arc::as_ptr(state) as usize
    }

    fn run(self) {
        let mut one = std::collections::VecDeque::from([self]);
        Self::run_batch(&mut one);
    }

    // Each run of blocks is landed as one batch; a piece is written on its
    // turn among them.
    fn run_batch(batch: &mut std::collections::VecDeque<Self>) {
        let mut blocks = std::collections::VecDeque::new();
        while let Some(job) = batch.pop_front() {
            match job {
                DiskWrite::Block(job) => {
                    blocks.push_back(job);
                    while let Some(DiskWrite::Block(_)) = batch.front() {
                        if let Some(DiskWrite::Block(job)) = batch.pop_front() {
                            blocks.push_back(job);
                        }
                    }
                    WriteJob::run_batch(&mut blocks);
                }
                DiskWrite::Piece(PieceWrite(job)) => {
                    let state = job.state.clone();
                    // Dropped unwritten once its run has ended: the piece is
                    // fetched again in a later run.
                    if let Some(_turn) = state.storage_turn() {
                        job.settle(Verdict::Passed);
                    }
                }
            }
        }
    }
}

impl WriteJob {
    // The batch is landed a torrent at a time: each run of jobs of one
    // torrent takes the torrent's lock twice, not twice per block.
    fn run_batch(batch: &mut std::collections::VecDeque<Self>) {
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
            written,
            _run,
        } = self;
        drop(block);
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
                    ended: None,
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
        match hash_landed_piece(&self.state, self.chunk.piece_index) {
            // A piece its storage holds in memory is written whole now: on
            // the torrent's turn of the write threads, in the order the
            // checks ended, with the hashing thread on to the next check
            // (libtorrent hashes on `hashing_threads` and writes on
            // `aio_threads`, mmap_disk_io.cpp:369-374).
            Ok(Verdict::Passed) if self.state.files.writes_to_memory() => {
                let disk = self.state.shared.spawner.disk().clone();
                disk.push(DiskWrite::Piece(PieceWrite(self)));
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
            _run,
        } = self;
        let settled = settle_landed_piece(&state, addr, &counters, &chunk, time, senders, verdict);
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
