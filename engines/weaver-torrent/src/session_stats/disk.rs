// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! What a session's disk work and its peers' sockets did, as counters a
//! scrape reads without a lock.
//!
//! Every count is one relaxed add where the event happens. Time is read
//! only around a disk call, a wait for a blocking permit and a wait for a
//! piece check: never per block copied in memory, never per message.

use std::{
    sync::atomic::{AtomicU64, Ordering::Relaxed},
    time::Instant,
};

use serde::Serialize;

/// A kind of disk call, counted apart from the others.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskOp {
    /// A checked piece handed whole to the storage.
    PieceWrite,
    /// A batch of received blocks written to their files.
    BlockWrite,
    /// The blocks of one upload write read for a peer on TCP.
    UploadReadTcp,
    /// The same for a peer on uTP.
    UploadReadUtp,
    /// A downloaded piece hashed where the storage holds it in memory.
    HashMemory,
    /// A downloaded piece read back from its files and hashed.
    HashFiles,
    /// A piece checked before its first upload in seed mode.
    SeedCheck,
    /// A torrent's check of the files it starts with.
    InitialCheck,
}

impl DiskOp {
    pub const ALL: [DiskOp; 8] = [
        DiskOp::PieceWrite,
        DiskOp::BlockWrite,
        DiskOp::UploadReadTcp,
        DiskOp::UploadReadUtp,
        DiskOp::HashMemory,
        DiskOp::HashFiles,
        DiskOp::SeedCheck,
        DiskOp::InitialCheck,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            DiskOp::PieceWrite => "piece_write",
            DiskOp::BlockWrite => "block_write",
            DiskOp::UploadReadTcp => "upload_read_tcp",
            DiskOp::UploadReadUtp => "upload_read_utp",
            DiskOp::HashMemory => "hash_memory",
            DiskOp::HashFiles => "hash_files",
            DiskOp::SeedCheck => "seed_check",
            DiskOp::InitialCheck => "initial_check",
        }
    }

    pub fn index(self) -> usize {
        self as usize
    }
}

/// The transport of the peer a socket or an upload read serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Tcp,
    Utp,
}

impl Transport {
    pub const ALL: [Transport; 2] = [Transport::Tcp, Transport::Utp];

    pub fn as_str(self) -> &'static str {
        match self {
            Transport::Tcp => "tcp",
            Transport::Utp => "utp",
        }
    }

    pub fn index(self) -> usize {
        self as usize
    }
}

#[derive(Debug, Default)]
struct OpCounters {
    calls: AtomicU64,
    bytes: AtomicU64,
    nanos: AtomicU64,
    in_flight: AtomicU64,
    most_in_flight: AtomicU64,
}

/// One kind of disk call since the session opened.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct DiskOpSnapshot {
    pub calls: u64,
    pub bytes: u64,
    /// Time spent inside the calls, summed.
    pub nanos: u64,
    /// Calls running now.
    pub in_flight: u64,
    /// The most that ran at once.
    pub most_in_flight: u64,
}

#[derive(Debug, Default)]
struct SocketCounters {
    receive_calls: AtomicU64,
    send_calls: AtomicU64,
}

/// The calls the peers' sockets of one transport took.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SocketSnapshot {
    /// Reads that returned bytes, after the handshakes that open a
    /// connection.
    pub receive_calls: u64,
    /// Buffers handed to the socket to be written whole, after the
    /// handshakes that open a connection.
    pub send_calls: u64,
}

/// A session's disk and socket counters.
#[derive(Debug, Default)]
pub struct DiskStats {
    ops: [OpCounters; DiskOp::ALL.len()],
    sockets: [SocketCounters; Transport::ALL.len()],
    upload_read_blocks: [AtomicU64; Transport::ALL.len()],
    permit_acquisitions: AtomicU64,
    permit_waits: AtomicU64,
    permit_wait_nanos: AtomicU64,
    peers_stopped: AtomicU64,
    peers_resumed: AtomicU64,
    check_waits: AtomicU64,
    check_wait_nanos: AtomicU64,
    read_buffer_growths: AtomicU64,
    // Writers holding a batch buffer.
    batch_buffers: AtomicU64,
    read_seeks: AtomicU64,
    write_seeks: AtomicU64,
}

/// A session's disk and socket counters, read at one moment.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize)]
pub struct DiskStatsSnapshot {
    /// By [`DiskOp::index`].
    pub ops: [DiskOpSnapshot; DiskOp::ALL.len()],
    /// By [`Transport::index`].
    pub sockets: [SocketSnapshot; Transport::ALL.len()],
    /// Blocks the upload reads read, by [`Transport::index`]; the reads are
    /// the calls of the upload read operations.
    pub upload_read_blocks: [u64; Transport::ALL.len()],
    /// Blocking permits taken, how many of them had to wait, and the time
    /// those waited.
    pub permit_acquisitions: u64,
    pub permit_waits: u64,
    pub permit_wait_nanos: u64,
    /// Blocking permits held now, and how many there are.
    pub permits_in_use: u64,
    pub permits: u64,
    /// Jobs waiting for a thread of each disk queue now.
    pub write_jobs_queued: u64,
    pub hash_jobs_queued: u64,
    pub read_jobs_queued: u64,
    /// Bytes of received blocks queued for the disk now.
    pub queued_write_bytes: u64,
    /// Times a connection stopped reading for the disk, and read again.
    pub peers_stopped: u64,
    pub peers_resumed: u64,
    /// Times a connection waited for the check of a piece it completed,
    /// and the time waited.
    pub check_waits: u64,
    pub check_wait_nanos: u64,
    /// Times a connection's read buffer was made larger.
    pub read_buffer_growths: u64,
    /// Peer writers holding a batch buffer for runs of blocks.
    pub batch_buffers: u64,
    /// File reads and writes that did not start where the last one of the
    /// same file ended.
    pub read_seeks: u64,
    pub write_seeks: u64,
}

/// Where the last read and the last write of one file ended.
#[derive(Debug, Default)]
pub struct FileEnds {
    pub read: AtomicU64,
    pub write: AtomicU64,
}

/// A disk call being counted: its time and its place among the calls in
/// flight end when it is dropped.
pub struct DiskCall<'a> {
    counters: &'a OpCounters,
    started: Instant,
}

impl DiskCall<'_> {
    /// The call moved `bytes`.
    pub fn bytes(&self, bytes: u64) {
        self.counters.bytes.fetch_add(bytes, Relaxed);
    }
}

impl Drop for DiskCall<'_> {
    fn drop(&mut self) {
        let nanos = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.counters.nanos.fetch_add(nanos, Relaxed);
        self.counters.in_flight.fetch_sub(1, Relaxed);
    }
}

impl DiskStats {
    /// A disk call of kind `op` starts here and ends when the value is
    /// dropped.
    pub fn call(&self, op: DiskOp) -> DiskCall<'_> {
        let counters = &self.ops[op.index()];
        counters.calls.fetch_add(1, Relaxed);
        let now = counters.in_flight.fetch_add(1, Relaxed) + 1;
        counters.most_in_flight.fetch_max(now, Relaxed);
        DiskCall {
            counters,
            started: Instant::now(),
        }
    }

    /// An upload read read `blocks` blocks.
    pub fn upload_read(&self, transport: Transport, blocks: u64) {
        self.upload_read_blocks[transport.index()].fetch_add(blocks, Relaxed);
    }

    /// `calls` reads of a peer's socket returned bytes.
    pub fn received(&self, transport: Transport, calls: u64) {
        self.sockets[transport.index()]
            .receive_calls
            .fetch_add(calls, Relaxed);
    }

    /// One buffer was written to a peer's socket.
    pub fn sent(&self, transport: Transport) {
        self.sockets[transport.index()]
            .send_calls
            .fetch_add(1, Relaxed);
    }

    /// A blocking permit was taken at once.
    pub fn permit_taken(&self) {
        self.permit_acquisitions.fetch_add(1, Relaxed);
    }

    /// A blocking permit was taken after a wait that began at `since`.
    pub fn permit_waited(&self, since: Instant) {
        self.permit_acquisitions.fetch_add(1, Relaxed);
        self.permit_waits.fetch_add(1, Relaxed);
        self.permit_wait_nanos
            .fetch_add(nanos_since(since), Relaxed);
    }

    pub fn peer_stopped(&self) {
        self.peers_stopped.fetch_add(1, Relaxed);
    }

    pub fn peer_resumed(&self) {
        self.peers_resumed.fetch_add(1, Relaxed);
    }

    /// A connection waited since `since` for the check of a piece.
    pub fn check_waited(&self, since: Instant) {
        self.check_waits.fetch_add(1, Relaxed);
        self.check_wait_nanos.fetch_add(nanos_since(since), Relaxed);
    }

    pub fn read_buffers_grew(&self, times: u64) {
        self.read_buffer_growths.fetch_add(times, Relaxed);
    }

    pub fn batch_buffer_made(&self) {
        self.batch_buffers.fetch_add(1, Relaxed);
    }

    pub fn batch_buffer_let_go(&self) {
        self.batch_buffers.fetch_sub(1, Relaxed);
    }

    /// A read of `len` bytes at `offset` of a file whose last read ended at
    /// `last_end`: counted when it does not start there.
    pub fn file_read(&self, last_end: &AtomicU64, offset: u64, len: u64) {
        if last_end.swap(offset + len, Relaxed) != offset {
            self.read_seeks.fetch_add(1, Relaxed);
        }
    }

    /// The same for a write.
    pub fn file_written(&self, last_end: &AtomicU64, offset: u64, len: u64) {
        if last_end.swap(offset + len, Relaxed) != offset {
            self.write_seeks.fetch_add(1, Relaxed);
        }
    }

    /// The counters now. The gauges a queue or a semaphore keeps are the
    /// caller's to fill.
    pub fn snapshot(&self) -> DiskStatsSnapshot {
        DiskStatsSnapshot {
            ops: std::array::from_fn(|i| {
                let op = &self.ops[i];
                DiskOpSnapshot {
                    calls: op.calls.load(Relaxed),
                    bytes: op.bytes.load(Relaxed),
                    nanos: op.nanos.load(Relaxed),
                    in_flight: op.in_flight.load(Relaxed),
                    most_in_flight: op.most_in_flight.load(Relaxed),
                }
            }),
            sockets: std::array::from_fn(|i| {
                let socket = &self.sockets[i];
                SocketSnapshot {
                    receive_calls: socket.receive_calls.load(Relaxed),
                    send_calls: socket.send_calls.load(Relaxed),
                }
            }),
            upload_read_blocks: std::array::from_fn(|i| self.upload_read_blocks[i].load(Relaxed)),
            permit_acquisitions: self.permit_acquisitions.load(Relaxed),
            permit_waits: self.permit_waits.load(Relaxed),
            permit_wait_nanos: self.permit_wait_nanos.load(Relaxed),
            peers_stopped: self.peers_stopped.load(Relaxed),
            peers_resumed: self.peers_resumed.load(Relaxed),
            check_waits: self.check_waits.load(Relaxed),
            check_wait_nanos: self.check_wait_nanos.load(Relaxed),
            read_buffer_growths: self.read_buffer_growths.load(Relaxed),
            batch_buffers: self.batch_buffers.load(Relaxed),
            read_seeks: self.read_seeks.load(Relaxed),
            write_seeks: self.write_seeks.load(Relaxed),
            ..Default::default()
        }
    }
}

fn nanos_since(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use super::{DiskOp, DiskStats, Transport};

    // Calls in flight are counted while they run and not after: three at
    // once leave a high-water mark of three and a gauge of zero.
    #[test]
    fn calls_in_flight_return_to_zero_and_leave_their_high_water_mark() {
        let stats = DiskStats::default();
        let read = |stats: &DiskStats| stats.snapshot().ops[DiskOp::UploadReadTcp.index()];
        {
            let first = stats.call(DiskOp::UploadReadTcp);
            first.bytes(100);
            let second = stats.call(DiskOp::UploadReadTcp);
            let third = stats.call(DiskOp::UploadReadTcp);
            third.bytes(20);
            assert_eq!(read(&stats).in_flight, 3);
            drop(second);
            assert_eq!(read(&stats).in_flight, 2);
            drop((first, third));
        }
        let fourth = stats.call(DiskOp::UploadReadTcp);
        drop(fourth);
        let now = read(&stats);
        assert_eq!(
            (now.calls, now.bytes, now.in_flight, now.most_in_flight),
            (4, 120, 0, 3)
        );
        // Another kind of call is counted apart.
        assert_eq!(stats.snapshot().ops[DiskOp::PieceWrite.index()].calls, 0);
        stats.upload_read(Transport::Tcp, 32);
        assert_eq!(stats.snapshot().upload_read_blocks, [32, 0]);
    }

    // Calls of one file that follow one another are not counted; one that
    // starts anywhere else is.
    #[test]
    fn a_call_that_does_not_start_where_the_last_ended_is_counted() {
        let stats = DiskStats::default();
        let (reads, writes) = (AtomicU64::new(0), AtomicU64::new(0));
        // 0..10, 10..30, 30..40 follow one another: none counted.
        for (offset, len) in [(0, 10), (10, 20), (30, 10)] {
            stats.file_read(&reads, offset, len);
        }
        assert_eq!(stats.snapshot().read_seeks, 0);
        // Back to 0, then on from 5, then a jump to 100, then on from it.
        for (offset, len) in [(0, 5), (5, 5), (100, 10), (110, 1)] {
            stats.file_read(&reads, offset, len);
        }
        assert_eq!(stats.snapshot().read_seeks, 2);
        // Writes are counted apart, and a first write past the start counts.
        for (offset, len) in [(16384, 16384), (32768, 16384), (0, 16384)] {
            stats.file_written(&writes, offset, len);
        }
        let now = stats.snapshot();
        assert_eq!((now.read_seeks, now.write_seeks), (2, 2));
    }
}
