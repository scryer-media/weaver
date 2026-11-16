// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use super::{hash_fetch, smart_ban};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use crate::buffers::ByteBuf;
use crate::core::lengths::{ChunkInfo, ValidPieceIndex};
use crate::peer_protocol::Piece;
use anyhow::Context;
use parking_lot::RwLockReadGuard;
use tracing::{debug, error, trace, warn};

use crate::{
    chunk_tracker::ChunkMarkingResult,
    disk::Block,
    file_ops::{Verdict, inconsistent_hashes},
    piece_tracker::Arrival,
    session_stats::{
        disk::DiskOp,
        swarm::{self, Direction},
    },
    type_aliases::PeerHandle,
};

use super::peer::{
    RemoveInflightRequestResult, stats::atomic::PeerCountersAtomic as AtomicPeerCounters,
};

use super::{
    Admitted, HashJob, Landed, PeerHandler, PieceCheck, TorrentStateLive, WriteJob, Written,
};

impl PeerHandler {
    /// Checks the block the peer sent names a chunk of the torrent, counts
    /// it, and takes it off the peer's requests: the block's chunk.
    pub(super) fn received_block(
        &self,
        index: u32,
        begin: u32,
        len: usize,
    ) -> anyhow::Result<ChunkInfo> {
        let piece_index = self
            .state
            .lengths
            .validate_piece_index(index)
            .with_context(|| format!("peer sent an invalid piece {index}"))?;
        let chunk_info = match self.state.lengths.chunk_info_from_received_data(
            piece_index,
            begin,
            len.try_into().context("bug")?,
        ) {
            Some(i) => i,
            None => {
                anyhow::bail!(
                    "peer sent us an invalid piece: index={index} begin={begin} len={len}"
                );
            }
        };

        // Peer chunk/byte counters.
        self.counters
            .fetched_bytes
            .fetch_add(len as u64, Ordering::Relaxed);
        self.counters.fetched_chunks.fetch_add(1, Ordering::Relaxed);

        // A block we had stopped waiting for, or never asked for, is data
        // all the same: whether it is written is the piece tracker's call,
        // which writes a block the piece lacks and counts one it holds as
        // redundant, and the peer is kept either way (libtorrent
        // `peer_connection::start_receive_piece` queues an unrequested
        // block as not wanted, and `incoming_piece` writes it unless the
        // picker holds it, then counting it redundant).
        self.state
            .peers
            .with_live_mut(self.addr, "inflight_requests.remove", |h| {
                match h.remove_inflight_request(&chunk_info) {
                    RemoveInflightRequestResult::Expected => {
                        self.swarm().block(swarm::BlockArrival::Requested);
                    }
                    RemoveInflightRequestResult::LateCanceled => {
                        self.swarm().block(swarm::BlockArrival::Cancelled);
                        trace!(?chunk_info, "peer sent a block after we cancelled it");
                    }
                    RemoveInflightRequestResult::Unexpected => {
                        self.swarm().block(swarm::BlockArrival::Unrequested);
                        debug!(
                            ?chunk_info,
                            inflight = ?h.inflight_requests_debug(),
                            "peer sent a block we did not ask for"
                        );
                    }
                }
            })
            .context("peer not found")?;

        // This one is used to calculate download speed.
        self.state
            .stats
            .fetched_bytes
            .fetch_add(len as u64, Ordering::Relaxed);
        self.state
            .session_stats
            .counters
            .fetched_bytes
            .fetch_add(len as u64, Ordering::Relaxed);
        self.count_bytes(Direction::In, swarm::ByteKind::Payload, len as u64);

        Ok(chunk_info)
    }

    pub(super) async fn on_received_piece(&self, piece: Piece<ByteBuf<'_>>) -> anyhow::Result<()> {
        let chunk_info = self.received_block(piece.index, piece.begin, piece.len())?;
        self.land_here(&piece, chunk_info).await
    }

    /// A block received into its own buffer moves to the disk threads, which
    /// write it, check its piece when it was the last, and hand the buffer
    /// back (libtorrent `peer_connection::incoming_piece` queues
    /// `disk_interface::async_write`, and the torrent takes the completion,
    /// `peer_connection::on_disk_write_complete`). A block the storage keeps
    /// in memory is copied there here.
    pub(super) async fn queue_block(&self, mut block: Block) -> anyhow::Result<()> {
        let chunk_info = self.received_block(block.index, block.begin, block.len())?;
        if self.state.files.writes_to_memory() {
            return self.land_here(&block.piece(), chunk_info).await;
        }
        let pool = block.buf_mut().pool().clone();
        if self.disk.queued(pool.charge(block.buf_mut())) {
            self.state.shared.spawner.stats().peer_stopped();
        }
        self.state.shared.spawner.disk().push(WriteJob {
            state: self.state.clone(),
            tx: self.tx.clone(),
            addr: self.addr,
            counters: self.counters.clone(),
            chunk: chunk_info,
            block,
            written: Written(Some(self.disk.clone())),
            _run: self.state.tasks.token(),
        });
        Ok(())
    }

    /// The pool that counts what this connection has waiting for the disk:
    /// whole pieces where the storage holds them in memory, blocks where it
    /// writes them as they come.
    pub(super) fn waits_pool(&self) -> &crate::disk::BlockPool {
        let spawner = &self.state.shared.spawner;
        if self.state.files.writes_to_memory() {
            spawner.checks()
        } else {
            spawner.pool()
        }
    }

    /// Writes the block on this connection's turn. When it was its piece's
    /// last, the piece is checked on the hashing thread, never here: with
    /// this connection back at its socket where the storage holds the piece
    /// in memory, and with it waiting for the check otherwise.
    async fn land_here(
        &self,
        piece: &Piece<ByteBuf<'_>>,
        chunk_info: ChunkInfo,
    ) -> anyhow::Result<()> {
        let state = &self.state;
        let spawner = &state.shared.spawner;
        let land = || land_block(state, self.addr, piece, &chunk_info);
        // A block the storage copies into memory is copied here, on this
        // thread: only a write that reaches a file, and the check of a
        // piece, take a blocking thread.
        let from_memory = state.files.writes_to_memory();
        let landed = if from_memory {
            land()
        } else {
            spawner.block_in_place_with_semaphore(land).await
        }
        .with_context(|| format!("error processing received chunk {chunk_info:?}"))?;
        let Some((time, senders, piece_check)) = landed else {
            return Ok(());
        };
        // The piece is out of flight and out of the queue from here until its
        // verdict, so its check belongs to the torrent, not to this
        // connection, and goes on when the connection ends (libtorrent
        // `torrent::verify_piece` posts the hash job bound to the torrent,
        // `shared_from_this()`, and `torrent::on_piece_verified` takes its
        // verdict whatever became of the sender).
        if from_memory {
            // The whole piece waits in the storage's memory for the hashing
            // thread and then for its write: it is counted until both are
            // done, and the connection stops reading with the piece that
            // leaves the count past its high mark (libtorrent
            // `peer_connection::incoming_piece`, `m_outstanding_writing_bytes`
            // against `max_queued_disk_bytes`, allows each connection one
            // block more; a whole piece more is not allowed here).
            let piece_len = u64::from(state.lengths.piece_length(chunk_info.piece_index));
            let (charge, waiting) = spawner.checks().charge_bytes(piece_len);
            if self.disk.queued_piece(charge) {
                spawner.stats().peer_stopped();
            }
            spawner.hash().push(HashJob {
                check: piece_check,
                state: self.state.clone(),
                tx: self.tx.clone(),
                addr: self.addr,
                counters: self.counters.clone(),
                chunk: chunk_info,
                time,
                senders,
                waiting: Some(waiting),
                written: Written(Some(self.disk.clone())),
                queued: Some(spawner.check_queued()),
                ended: None,
                stored: None,
                _run: self.state.tasks.token(),
            });
            return Ok(());
        }
        // The piece is in its files: it is read back and checked on the
        // hashing thread like any other, and this connection waits for the
        // check's end, which a check that fails tells it of as an error of
        // its own.
        let (ended, end) = tokio::sync::oneshot::channel();
        let since = std::time::Instant::now();
        spawner.hash().push(HashJob {
            check: piece_check,
            state: self.state.clone(),
            tx: self.tx.clone(),
            addr: self.addr,
            counters: self.counters.clone(),
            chunk: chunk_info,
            time,
            senders,
            waiting: None,
            written: Written(None),
            queued: Some(spawner.check_queued()),
            ended: Some(ended),
            stored: None,
            _run: self.state.tasks.token(),
        });
        // Dropped unsent when the check is over, however it ended.
        let _ = end.await;
        spawner.stats().check_waited(since);
        Ok(())
    }
}

// Writes the block, and when it was the piece's last, hands back the
// piece's download time and senders for its check.
pub(super) fn land_block(
    state: &Arc<TorrentStateLive>,
    addr: PeerHandle,
    piece: &Piece<ByteBuf<'_>>,
    chunk_info: &ChunkInfo,
) -> Landed {
    let mut landed = Vec::with_capacity(1);
    land_blocks(state, &[(addr, piece, *chunk_info)], &mut landed);
    landed.pop().unwrap_or(Ok(None))
}

// Writes a batch of blocks of one torrent, taking the torrent's lock once
// to admit them all and once to mark them all, rather than twice per block
// (libtorrent runs the completions of a disk thread's batch together on the
// network thread, `disk_io_thread::call_job_handlers`). `landed` gets one
// entry per block, in order.
pub(super) fn land_blocks(
    state: &Arc<TorrentStateLive>,
    blocks: &[(PeerHandle, &Piece<ByteBuf<'_>>, ChunkInfo)],
    landed: &mut Vec<Landed>,
) {
    let first = landed.len();
    landed.extend(blocks.iter().map(|_| Ok(None)));
    let landed = &mut landed[first..];
    // A run that has ended writes nothing more: its blocks are fetched again
    // in a later run.
    let Some(_turn) = state.storage_turn() else {
        return;
    };
    // Dropped after every lock below, so a panic that unwinds through them
    // finds them free.
    let mut admitted = Admitted {
        state,
        chunks: Vec::new(),
    };
    // A block we lack is written, whoever owns its piece now. While it is,
    // no one steals the piece: its lock is held, read, until the block is
    // marked, so that when it was the last block its piece is ours to
    // check. Each piece's lock is taken once, however many of its blocks
    // the batch holds.
    let mut held: Vec<(u32, RwLockReadGuard<'_, ()>)> = Vec::new();
    let mut cancels: Vec<(usize, Vec<PeerHandle>)> = Vec::new();
    {
        let mut g = state.lock_write("check_steal");
        let pieces = match g.get_pieces_mut() {
            Ok(pieces) => pieces,
            Err(e) => {
                let e = anyhow::Error::from(e);
                for slot in landed.iter_mut() {
                    *slot = Err(anyhow::anyhow!("{e:#}"));
                }
                return;
            }
        };
        for (i, (addr, piece, chunk_info)) in blocks.iter().enumerate() {
            if !held.iter().any(|(index, _)| *index == piece.index)
                && let Some(lock) = state.per_piece_locks.get(piece.index as usize)
            {
                held.push((piece.index, lock.read()));
            }
            let admit = |p| state.files.reserve_piece(p, &state.room_notify);
            match pieces.admitted_block_arrived(chunk_info, *addr, admit) {
                Arrival::Write { cancel } => {
                    admitted.chunks.push(*chunk_info);
                    cancels.push((i, cancel));
                }
                Arrival::Discard => {
                    state.add_redundant_bytes(piece.len() as u64);
                    state
                        .session_stats
                        .swarm
                        .wasted(swarm::Waste::Redundant, piece.len() as u64);
                    debug!(
                        "{chunk_info:?} is had, arrived from another first, or has no room, ignoring"
                    );
                }
            }
        }
    }
    // The block was asked of other peers too: they need not send it
    // (libtorrent `torrent::cancel_block`).
    let mut written = Vec::with_capacity(cancels.len());
    for (i, cancel) in cancels {
        let chunk_info = &blocks[i].2;
        for other in cancel {
            state
                .peers
                .with_live_mut(other, "cancel_duplicate", |live| {
                    live.cancel_inflight_request(chunk_info)
                });
        }
        let (addr, piece, chunk_info) = &blocks[i];
        // A block whose write failed is no longer being written, and is
        // fetched again (libtorrent `peer_connection::on_disk_write_complete`
        // calls `piece_picker::write_failed`, then
        // `torrent::handle_disk_error`).
        let epoch = state.upload_epoch();
        match state.file_ops().write_chunk(*addr, piece, chunk_info) {
            Ok(()) => written.push(i),
            Err(e) => {
                admitted
                    .chunks
                    .retain(|c| c.absolute_index != chunk_info.absolute_index);
                landed[i] = write_failed(state, chunk_info, e, epoch);
            }
        }
    }
    if written.is_empty() {
        return;
    }

    let mut g = state.lock_write("mark_chunk_downloaded");
    let pieces = match g.get_pieces_mut() {
        Ok(pieces) => pieces,
        Err(e) => {
            let e = anyhow::Error::from(e);
            for i in written {
                landed[i] = Err(anyhow::anyhow!("{e:#}"));
            }
            return;
        }
    };
    for i in written {
        let (addr, piece, chunk_info) = &blocks[i];
        let chunk_marking_result = pieces.mark_chunk_downloaded(piece, *addr);
        trace!(?piece, chunk_marking_result=?chunk_marking_result);
        landed[i] = match chunk_marking_result {
            Some(ChunkMarkingResult::Completed) => {
                trace!("piece={} done, will write and checksum", piece.index);
                // Out of flight and the queue, so no one takes it during
                // its hash check. From here its check owns it: dropped
                // before a verdict, as by a panic in this batch, it puts
                // the piece back. Never dropped while `g` is held: the
                // slot it sits in outlives this function's locks.
                let (time, senders) = pieces.take_inflight(chunk_info.piece_index);
                let check = PieceCheck {
                    piece: chunk_info.piece_index,
                    state: Some(state.clone()),
                };
                Ok(Some((time, senders, check)))
            }
            Some(ChunkMarkingResult::PreviouslyCompleted) => {
                // TODO: we might need to send cancellations here.
                debug!("piece={} was done by someone else, ignoring", piece.index);
                Ok(None)
            }
            Some(ChunkMarkingResult::NotCompleted) => Ok(None),
            None => Err(anyhow::anyhow!(
                "bogus data received: {:?}, cannot map this to a chunk, dropping peer",
                piece
            )),
        };
    }
    admitted.chunks.clear();
    // The pieces' locks go once their blocks are marked: a completed one is
    // out of flight now.
    drop(g);
    drop(held);
}

impl Drop for Admitted<'_> {
    fn drop(&mut self) {
        if self.chunks.is_empty() {
            return;
        }
        if let Ok(pieces) = self.state.lock_write("write_failed").get_pieces_mut() {
            for chunk in &self.chunks {
                pieces.write_failed(chunk);
            }
        }
    }
}

// A block whose write, made at `epoch`, failed: fetched again, and the
// disk error handled.
fn write_failed(
    state: &TorrentStateLive,
    chunk_info: &ChunkInfo,
    e: anyhow::Error,
    epoch: u64,
) -> Landed {
    state
        .lock_write("write_failed")
        .get_pieces_mut()?
        .write_failed(chunk_info);
    if state.handle_disk_error(&e, epoch) {
        return Ok(None);
    }
    error!(
        id = state.shared.id,
        info_hash = ?state.shared.info_hash,
        "FATAL: error writing chunk to disk: {e:#}"
    );
    state.on_fatal_error(e).map(|()| None)
}

// Hashes the piece whose last block was written: where the storage holds it
// in memory, or read back from its files.
pub(super) fn hash_landed_piece(
    state: &TorrentStateLive,
    piece_index: ValidPieceIndex,
) -> anyhow::Result<Verdict> {
    let stats = state.shared.spawner.stats();
    let call = stats.call(if state.files.writes_to_memory() {
        DiskOp::HashMemory
    } else {
        DiskOp::HashFiles
    });
    call.bytes(u64::from(state.lengths.piece_length(piece_index)));
    state
        .file_ops()
        .check_piece(piece_index)
        .with_context(|| format!("error checking piece={}", piece_index.get()))
}

// What follows a piece's verdict: one that passed is had once the storage
// took it; one that failed is fetched again. A passed piece its storage
// holds in memory does not come here: it is handed over to be written
// (`StoredPiece`), and comes back to `piece_stored`.
pub(super) fn settle_landed_piece(
    state: &TorrentStateLive,
    addr: PeerHandle,
    counters: &AtomicPeerCounters,
    chunk_info: &ChunkInfo,
    full_piece_download_time: Option<Duration>,
    senders: crate::piece_tracker::Senders,
    verdict: Verdict,
) -> anyhow::Result<()> {
    let index = chunk_info.piece_index.get();
    let piece_index = chunk_info.piece_index;
    let read_block = |who: PeerHandle, chunk: &ChunkInfo, buf: &mut [u8]| {
        state.file_ops().read_chunk(who, chunk, buf)
    };

    match verdict {
        Verdict::Inconsistent => {
            state
                .session_stats
                .swarm
                .piece_check(swarm::PieceCheck::Inconsistent);
            return state.on_fatal_error(inconsistent_hashes(chunk_info.piece_index));
        }
        Verdict::Passed => {
            let epoch = piece_passing(state, piece_index);
            // A storage that writes blocks as they come has nothing left
            // to write: it is told here, on this thread. One that holds the
            // piece in memory is handed it to write instead
            // (`StoredPiece`), and the piece comes back to
            // `piece_stored` once it did.
            let written = state.files.on_piece_completed(piece_index);
            return piece_stored(
                state,
                counters,
                chunk_info,
                full_piece_download_time,
                senders,
                epoch,
                written,
            );
        }
        Verdict::Failed => {
            warn!(
                id = state.shared.id,
                info_hash = ?state.shared.info_hash,
                ?addr,
                "checksum for piece={} did not validate", index
            );
            // The whole piece counts as corrupt (libtorrent
            // `torrent::piece_failed` calls `add_failed_bytes` with
            // the piece size).
            state.stats.failed_bytes.fetch_add(
                u64::from(state.lengths.piece_length(piece_index)),
                Ordering::Relaxed,
            );
            let checks = &state.session_stats.swarm;
            checks.piece_check(swarm::PieceCheck::Failed);
            checks.wasted(
                swarm::Waste::HashFailed,
                u64::from(state.lengths.piece_length(piece_index)),
            );
            // Before the storage lets go of the failed bytes.
            let blocks = smart_ban::read_back(&state.lengths, piece_index, &senders, read_block);
            for liar in state.peers.piece_failed(piece_index, blocks) {
                state.ban_for_bad_data(liar);
            }
            // Checked by v2, a piece of more than one sender has its
            // block hashes fetched, which name the senders of its bad
            // blocks; until then no one is blamed (libtorrent
            // `torrent::piece_failed` calls `verify_block_hashes`).
            // Otherwise, or when one peer sent it all, every sender
            // is blamed, and the sole sender known to be bad
            // (libtorrent `torrent::piece_failed`,
            // `torrent::penalize_peers`).
            let v2_request = state.metadata.v2.as_ref().and_then(|v2| {
                let request = hash_fetch::block_request(
                    v2,
                    state.lengths.default_piece_length().into(),
                    piece_index.get(),
                )?;
                Some((request, v2.share(piece_index.get())?))
            });
            match v2_request {
                Some((request, share)) if !senders.sole_sender() => {
                    let failed = hash_fetch::failed_blocks(
                        &state.lengths,
                        piece_index,
                        share,
                        &senders,
                        read_block,
                    );
                    state
                        .layer_fetch
                        .verify_blocks(request, piece_index, failed);
                }
                _ => {
                    state.penalize_peers(senders.participants(), piece_index, senders.sole_sender())
                }
            }
            state.requeue_piece(piece_index)?;
        }
        // No hash to check the piece against: nobody is to blame
        // (libtorrent `torrent::on_piece_verified` neither passes nor
        // fails it).
        Verdict::Unknown => {
            state
                .session_stats
                .swarm
                .piece_check(swarm::PieceCheck::Unknown);
            state.requeue_piece(piece_index)?
        }
    };
    Ok(())
}

// A piece passed its check, before its storage takes it: the fetch of its
// layer and its peers hear it, and the senders of the bad blocks it
// replaced are banned while the good bytes are still there to read back.
// The upload epoch, taken before the storage is asked, which may refuse it.
pub(super) fn piece_passing(state: &TorrentStateLive, piece_index: ValidPieceIndex) -> u64 {
    let read_block = |who: PeerHandle, chunk: &ChunkInfo, buf: &mut [u8]| {
        state.file_ops().read_chunk(who, chunk, buf)
    };
    state.layer_fetch.piece_passed(piece_index);
    let failed = state.peers.piece_passed(piece_index);
    for sender in smart_ban::senders_of_bad_blocks(&failed, read_block) {
        state.ban_for_bad_data(sender);
    }
    state.upload_epoch()
}

// A passed piece its storage took, or refused with `written`'s error.
//
// The piece is ours only once the storage has taken it (libtorrent: the
// write completes before the hash job, and only then
// `torrent::piece_passed`). One it refuses is downloaded again, and its
// senders are neither credited nor blamed: on a disk error libtorrent
// `torrent::on_piece_verified` calls neither `piece_passed` nor
// `piece_failed`. A refusal for want of room puts the torrent in upload
// mode (`torrent::handle_disk_error`), judged at `epoch`.
pub(super) fn piece_stored(
    state: &TorrentStateLive,
    counters: &AtomicPeerCounters,
    chunk_info: &ChunkInfo,
    full_piece_download_time: Option<Duration>,
    senders: crate::piece_tracker::Senders,
    epoch: u64,
    written: anyhow::Result<()>,
) -> anyhow::Result<()> {
    let index = chunk_info.piece_index.get();
    let piece_index = chunk_info.piece_index;
    if let Err(e) = written {
        warn!(
            id = state.shared.id,
            info_hash = ?state.shared.info_hash,
            "the storage refused piece={index}: {e:#}"
        );
        state.handle_disk_error(&e, epoch);
        state
            .session_stats
            .swarm
            .piece_check(swarm::PieceCheck::StorageRefused);
        return state.requeue_piece(piece_index);
    }
    state
        .session_stats
        .swarm
        .piece_check(swarm::PieceCheck::Passed);
    state.on_piece_completed(piece_index)?;
    // Every peer that sent a block of it earns a point and leaves parole
    // (libtorrent `torrent::piece_passed`).
    for peer in senders.participants() {
        state
            .peers
            .with_peer_mut(peer, "piece_passed", |p| p.piece_passed(&state.peers));
    }

    let piece_len = state.lengths.piece_length(chunk_info.piece_index) as u64;
    if let Some(time) = full_piece_download_time {
        #[allow(clippy::cast_possible_truncation)]
        state
            .stats
            .total_piece_download_ms
            .fetch_add(time.as_millis() as u64, Ordering::Relaxed);

        // Per-peer piece counters.
        counters.on_piece_completed(piece_len, time);
    }

    trace!(piece = index, "successfully downloaded and verified");

    state.transmit_haves(chunk_info.piece_index);
    state.update_interest_of_holders(chunk_info.piece_index);
    Ok(())
}
