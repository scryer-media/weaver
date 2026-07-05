//! Off-thread hashing for high-throughput extraction paths.
//!
//! Store-mode extraction and the solid decode apply phase are wall-clock bound
//! by hashing performed inline on the hot thread. This module moves checksum
//! work onto dedicated worker threads:
//!
//! - CRC32 runs on one worker, fed whole chunks in stream order (an mpsc
//!   channel preserves FIFO order, so a single sequential hasher suffices).
//! - BLAKE2sp is parallelized by construction: the format defines 8
//!   independent BLAKE2s leaf streams, interleaved in 64-byte blocks. A
//!   splitter thread deinterleaves incoming chunks and 8 lane workers advance
//!   their leaf states; the root node combines the leaf digests at finalize.
//!   The output is bit-identical to a serial BLAKE2sp (verified against
//!   `blake2s_simd::blake2sp` in tests).
//!
//! Pipelines spawn threads per instance, so callers should only use them for
//! members large enough to amortize spawn cost (see [`PIPELINE_MIN_BYTES`]).

use std::io;
use std::sync::mpsc;
use std::thread::JoinHandle;

use blake2s_simd::Params as Blake2sParams;

/// Chunk buffers cycled between the submitter and the hash workers.
const CHUNK_CAPACITY: usize = 4 * 1024 * 1024;
/// Bounded in-flight chunks: caps memory and applies backpressure when the
/// hash side falls behind the reader.
const MAX_IN_FLIGHT: usize = 4;
/// BLAKE2sp interleaves its leaves in 64-byte blocks.
const BLAKE_BLOCK: usize = 64;
/// Number of BLAKE2sp leaf lanes, fixed by the format.
const LANES: usize = 8;
/// Below this size the thread spawn + channel overhead outweighs the win;
/// callers should stay on the inline hashing path.
pub(crate) const PIPELINE_MIN_BYTES: u64 = 8 * 1024 * 1024;

pub(crate) struct HashPipelineOutputs {
    pub crc32: Option<u32>,
    pub blake2sp: Option<[u8; 32]>,
}

enum ChunkMsg {
    Data(Vec<u8>),
}

enum LaneMsg {
    Data(Vec<u8>),
}

/// Off-thread hasher accepting in-order stream chunks.
pub(crate) struct HashPipeline {
    chunk_tx: Option<mpsc::SyncSender<ChunkMsg>>,
    free_rx: mpsc::Receiver<Vec<u8>>,
    coordinator: Option<JoinHandle<CoordinatorResult>>,
    compute_crc: bool,
    compute_blake: bool,
}

struct CoordinatorResult {
    crc32: Option<u32>,
    blake2sp: Option<[u8; 32]>,
}

impl HashPipeline {
    pub(crate) fn new(compute_crc: bool, compute_blake: bool) -> Self {
        assert!(
            compute_crc || compute_blake,
            "hash pipeline needs at least one hash kind"
        );
        let (chunk_tx, chunk_rx) = mpsc::sync_channel::<ChunkMsg>(MAX_IN_FLIGHT);
        let (free_tx, free_rx) = mpsc::channel::<Vec<u8>>();

        let coordinator = std::thread::Builder::new()
            .name("weaver-rar-hash".into())
            .spawn(move || coordinator_loop(chunk_rx, free_tx, compute_crc, compute_blake))
            .expect("spawn hash pipeline coordinator");

        Self {
            chunk_tx: Some(chunk_tx),
            free_rx,
            coordinator: Some(coordinator),
            compute_crc,
            compute_blake,
        }
    }

    /// Fetch a recycled chunk buffer (or allocate one). The returned buffer is
    /// empty with at least [`CHUNK_CAPACITY`] capacity.
    #[cfg(test)]
    pub(crate) fn take_buffer(&self) -> Vec<u8> {
        let mut buf = self.take_buffer_raw();
        buf.clear();
        buf
    }

    /// Fetch a recycled chunk buffer without clearing it (recycled contents
    /// are stale chunk data, useful when the caller overwrites in place).
    fn take_buffer_raw(&self) -> Vec<u8> {
        match self.free_rx.try_recv() {
            Ok(buf) => buf,
            Err(_) => Vec::with_capacity(CHUNK_CAPACITY),
        }
    }

    /// Submit the next chunk of the stream, in order.
    pub(crate) fn submit(&self, chunk: Vec<u8>) -> io::Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        self.chunk_tx
            .as_ref()
            .expect("hash pipeline already finalized")
            .send(ChunkMsg::Data(chunk))
            .map_err(|_| io::Error::other("hash pipeline worker terminated early"))
    }

    /// Copy `data` into a pooled buffer and submit it. Convenience for
    /// writer-wrapper call sites that only have a borrowed slice.
    #[cfg(test)]
    pub(crate) fn update(&self, data: &[u8]) -> io::Result<()> {
        let mut offset = 0;
        while offset < data.len() {
            let take = (data.len() - offset).min(CHUNK_CAPACITY);
            let mut buf = self.take_buffer();
            buf.extend_from_slice(&data[offset..offset + take]);
            self.submit(buf)?;
            offset += take;
        }
        Ok(())
    }

    /// Signal end of stream and collect the digests.
    pub(crate) fn finalize(mut self) -> io::Result<HashPipelineOutputs> {
        drop(self.chunk_tx.take());
        let result = self
            .coordinator
            .take()
            .expect("hash pipeline already finalized")
            .join()
            .map_err(|_| io::Error::other("hash pipeline coordinator panicked"))?;
        debug_assert_eq!(result.crc32.is_some(), self.compute_crc);
        debug_assert_eq!(result.blake2sp.is_some(), self.compute_blake);
        Ok(HashPipelineOutputs {
            crc32: result.crc32,
            blake2sp: result.blake2sp,
        })
    }
}

impl Drop for HashPipeline {
    fn drop(&mut self) {
        drop(self.chunk_tx.take());
        if let Some(handle) = self.coordinator.take() {
            let _ = handle.join();
        }
    }
}

fn coordinator_loop(
    chunk_rx: mpsc::Receiver<ChunkMsg>,
    free_tx: mpsc::Sender<Vec<u8>>,
    compute_crc: bool,
    compute_blake: bool,
) -> CoordinatorResult {
    let mut crc_lanes = compute_crc.then(CrcLanes::spawn);
    let mut blake = compute_blake.then(BlakeLanes::spawn);

    while let Ok(ChunkMsg::Data(chunk)) = chunk_rx.recv() {
        if let Some(ref mut lanes) = blake {
            lanes.split_chunk(&chunk);
        }
        match crc_lanes {
            // The CRC lanes own the buffer and recycle it when done.
            Some(ref mut lanes) => lanes.submit(chunk),
            None => {
                let _ = free_tx.send(chunk);
            }
        }
    }

    let crc32 = crc_lanes.take().map(|lanes| lanes.finalize(&free_tx));
    let blake2sp = blake.take().map(BlakeLanes::finalize);
    CoordinatorResult { crc32, blake2sp }
}

/// CRC32 of a whole stream, computed as independent per-chunk CRCs on two
/// worker threads and folded back together in chunk order. Folding uses the
/// standard GF(2) length-shift operator: crc(A ++ B) =
/// shift(crc(A), len(B)) ^ crc(B). Two lanes keep CRC off the read loop's
/// critical path even when a single lane cannot match the read rate.
struct CrcLanes {
    txs: Vec<mpsc::SyncSender<(u64, Vec<u8>)>>,
    handles: Vec<JoinHandle<(Vec<ChunkCrc>, Vec<Vec<u8>>)>>,
    next_seq: u64,
}

struct ChunkCrc {
    seq: u64,
    crc: u32,
    len: u64,
}

const CRC_LANE_COUNT: usize = 2;

impl CrcLanes {
    fn spawn() -> Self {
        let mut txs = Vec::with_capacity(CRC_LANE_COUNT);
        let mut handles = Vec::with_capacity(CRC_LANE_COUNT);
        for lane in 0..CRC_LANE_COUNT {
            let (tx, rx) = mpsc::sync_channel::<(u64, Vec<u8>)>(MAX_IN_FLIGHT);
            let handle = std::thread::Builder::new()
                .name(format!("weaver-rar-crc-{lane}"))
                .spawn(move || {
                    let mut results: Vec<ChunkCrc> = Vec::new();
                    let mut spare: Vec<Vec<u8>> = Vec::new();
                    while let Ok((seq, chunk)) = rx.recv() {
                        results.push(ChunkCrc {
                            seq,
                            crc: crc32fast::hash(&chunk),
                            len: chunk.len() as u64,
                        });
                        if spare.len() < MAX_IN_FLIGHT {
                            spare.push(chunk);
                        }
                    }
                    (results, spare)
                })
                .expect("spawn CRC lane");
            txs.push(tx);
            handles.push(handle);
        }
        Self {
            txs,
            handles,
            next_seq: 0,
        }
    }

    fn submit(&mut self, chunk: Vec<u8>) {
        let seq = self.next_seq;
        self.next_seq += 1;
        let lane = (seq as usize) % self.txs.len();
        let _ = self.txs[lane].send((seq, chunk));
    }

    fn finalize(mut self, free_tx: &mpsc::Sender<Vec<u8>>) -> u32 {
        self.txs.clear();
        let mut results: Vec<ChunkCrc> = Vec::new();
        for handle in self.handles.drain(..) {
            let (lane_results, spare) = handle.join().unwrap_or((Vec::new(), Vec::new()));
            results.extend(lane_results);
            for buf in spare {
                let _ = free_tx.send(buf);
            }
        }
        results.sort_unstable_by_key(|entry| entry.seq);

        let mut ops: Vec<(u64, CrcShiftOp)> = Vec::new();
        let mut crc = 0u32;
        for entry in results {
            let op = match ops.iter().find(|(len, _)| *len == entry.len) {
                Some((_, op)) => op,
                None => {
                    ops.push((entry.len, CrcShiftOp::new(entry.len)));
                    &ops.last().expect("just pushed").1
                }
            };
            crc = op.shift(crc) ^ entry.crc;
        }
        crc
    }
}

/// Operator that advances a finalized CRC32 past `len` zero bytes, so
/// per-chunk CRCs can be folded into the CRC of the concatenated stream.
struct CrcShiftOp {
    op: Option<[u32; 32]>,
}

impl CrcShiftOp {
    fn new(len: u64) -> Self {
        if len == 0 {
            return Self { op: None };
        }

        fn mat_vec(mat: &[u32; 32], mut vec: u32) -> u32 {
            let mut sum = 0u32;
            let mut idx = 0usize;
            while vec != 0 {
                if vec & 1 != 0 {
                    sum ^= mat[idx];
                }
                vec >>= 1;
                idx += 1;
            }
            sum
        }
        fn mat_square(dst: &mut [u32; 32], src: &[u32; 32]) {
            for n in 0..32 {
                dst[n] = mat_vec(src, src[n]);
            }
        }

        // One-bit shift operator over the reflected CRC32 polynomial; square
        // it up to the byte level, then square-and-multiply over `len`.
        let mut odd = [0u32; 32];
        odd[0] = 0xEDB8_8320;
        for (n, item) in odd.iter_mut().enumerate().skip(1) {
            *item = 1 << (n - 1);
        }
        let mut even = [0u32; 32];
        mat_square(&mut even, &odd); // 2 bits
        mat_square(&mut odd, &even); // 4 bits
        mat_square(&mut even, &odd); // 8 bits = 1 byte

        let mut combined = [0u32; 32];
        for (n, item) in combined.iter_mut().enumerate() {
            *item = 1 << n;
        }
        let mut per_step = even;
        let mut scratch = [0u32; 32];
        let mut remaining = len;
        loop {
            if remaining & 1 != 0 {
                let previous = combined;
                for n in 0..32 {
                    combined[n] = mat_vec(&per_step, previous[n]);
                }
            }
            remaining >>= 1;
            if remaining == 0 {
                break;
            }
            mat_square(&mut scratch, &per_step);
            per_step = scratch;
        }

        Self { op: Some(combined) }
    }

    fn shift(&self, crc: u32) -> u32 {
        let Some(ref op) = self.op else {
            return crc;
        };
        let mut sum = 0u32;
        let mut vec = crc;
        let mut idx = 0usize;
        while vec != 0 {
            if vec & 1 != 0 {
                sum ^= op[idx];
            }
            vec >>= 1;
            idx += 1;
        }
        sum
    }
}

struct BlakeLanes {
    lane_tx: Vec<mpsc::SyncSender<LaneMsg>>,
    lane_handles: Vec<JoinHandle<[u8; 32]>>,
    lane_free_rx: mpsc::Receiver<Vec<u8>>,
    lane_free_tx: mpsc::Sender<Vec<u8>>,
    /// Absolute stream offset of the next incoming byte.
    stream_offset: u64,
    /// Per-lane pending output buffers being filled for the current batch.
    pending: Vec<Vec<u8>>,
}

impl BlakeLanes {
    fn spawn() -> Self {
        let (lane_free_tx, lane_free_rx) = mpsc::channel::<Vec<u8>>();
        let mut lane_tx = Vec::with_capacity(LANES);
        let mut lane_handles = Vec::with_capacity(LANES);
        for lane in 0..LANES {
            let (tx, rx) = mpsc::sync_channel::<LaneMsg>(MAX_IN_FLIGHT);
            let free_tx = lane_free_tx.clone();
            let handle = std::thread::Builder::new()
                .name(format!("weaver-rar-b2-{lane}"))
                .spawn(move || {
                    let mut state = blake2sp_leaf_params(lane).to_state();
                    while let Ok(LaneMsg::Data(segment)) = rx.recv() {
                        state.update(&segment);
                        let _ = free_tx.send(segment);
                    }
                    let mut digest = [0u8; 32];
                    digest.copy_from_slice(state.finalize().as_bytes());
                    digest
                })
                .expect("spawn BLAKE2sp lane worker");
            lane_tx.push(tx);
            lane_handles.push(handle);
        }
        Self {
            lane_tx,
            lane_handles,
            lane_free_rx,
            lane_free_tx,
            stream_offset: 0,
            pending: (0..LANES).map(|_| Vec::new()).collect(),
        }
    }

    fn lane_buffer(&mut self) -> Vec<u8> {
        match self.lane_free_rx.try_recv() {
            Ok(mut buf) => {
                buf.clear();
                buf
            }
            Err(_) => Vec::with_capacity(CHUNK_CAPACITY / LANES + BLAKE_BLOCK),
        }
    }

    /// Deinterleave a stream chunk into the 8 lane substreams and dispatch
    /// full batches. BLAKE2sp assigns 64-byte block `b` to lane `b % 8`.
    fn split_chunk(&mut self, mut data: &[u8]) {
        while !data.is_empty() {
            let block_index = self.stream_offset / BLAKE_BLOCK as u64;
            let lane = (block_index % LANES as u64) as usize;
            let block_used = (self.stream_offset % BLAKE_BLOCK as u64) as usize;
            let take = (BLAKE_BLOCK - block_used).min(data.len());

            if self.pending[lane].is_empty() && self.pending[lane].capacity() == 0 {
                self.pending[lane] = self.lane_buffer();
            }
            self.pending[lane].extend_from_slice(&data[..take]);
            self.stream_offset += take as u64;
            data = &data[take..];

            if self.pending[lane].len() >= CHUNK_CAPACITY / LANES {
                let segment = std::mem::take(&mut self.pending[lane]);
                let _ = self.lane_tx[lane].send(LaneMsg::Data(segment));
            }
        }
    }

    fn finalize(mut self) -> [u8; 32] {
        for lane in 0..LANES {
            let segment = std::mem::take(&mut self.pending[lane]);
            if !segment.is_empty() {
                let _ = self.lane_tx[lane].send(LaneMsg::Data(segment));
            }
        }
        drop(self.lane_tx);
        drop(self.lane_free_tx);

        let mut root = blake2sp_root_params().to_state();
        for handle in self.lane_handles {
            let digest = handle.join().unwrap_or([0u8; 32]);
            root.update(&digest);
        }
        let mut out = [0u8; 32];
        out.copy_from_slice(root.finalize().as_bytes());
        out
    }
}

/// BLAKE2sp leaf parameters per the BLAKE2 tree spec (fanout 8, depth 2).
fn blake2sp_leaf_params(lane: usize) -> Blake2sParams {
    let mut params = Blake2sParams::new();
    params
        .hash_length(32)
        .fanout(8)
        .max_depth(2)
        .max_leaf_length(0)
        .node_offset(lane as u64)
        .node_depth(0)
        .inner_hash_length(32);
    if lane == LANES - 1 {
        params.last_node(true);
    }
    params
}

/// BLAKE2sp root-node parameters.
fn blake2sp_root_params() -> Blake2sParams {
    let mut params = Blake2sParams::new();
    params
        .hash_length(32)
        .fanout(8)
        .max_depth(2)
        .max_leaf_length(0)
        .node_offset(0)
        .node_depth(1)
        .inner_hash_length(32)
        .last_node(true);
    params
}

/// Threshold at which accumulated writer-path bytes are flushed to the
/// pipeline (keeps channel traffic coarse when the producer writes small
/// spans, as the solid apply phase does).
const PENDING_FLUSH_BYTES: usize = 1024 * 1024;

/// Final digests from a [`SharedHashStream`]. Fields are `Some` iff the
/// corresponding hash was requested at construction.
pub(crate) struct StreamHashOutputs {
    pub crc32: Option<u32>,
    pub rar14: Option<u16>,
    pub blake2sp: Option<[u8; 32]>,
}

enum StreamState {
    /// Hash inline on the submitting thread (small members, or RAR 1.4
    /// checksums which are not worth threading).
    Inline {
        crc: Option<crc32fast::Hasher>,
        rar14: Option<u16>,
        blake: Option<Box<crate::crypto::Blake2spHasher>>,
        pool: Vec<Vec<u8>>,
    },
    /// Hash on worker threads; `pending` coalesces small writer-path updates.
    Pipelined {
        pipeline: HashPipeline,
        pending: Vec<u8>,
    },
    Finished,
}

/// Shared, thread-safe hashing front-end for extraction output streams.
///
/// One instance tracks the checksums of a single member's output stream and
/// is safe to share across successive per-volume writers (`Arc`). Internally
/// it hashes inline for small members and spawns the off-thread
/// [`HashPipeline`] for large ones; `Nop` behavior (no hashes requested)
/// still serves recycled buffers so call sites need no branching.
pub(crate) struct SharedHashStream {
    state: std::sync::Mutex<StreamState>,
}

impl SharedHashStream {
    pub(crate) fn new(
        compute_crc: bool,
        compute_rar14: bool,
        compute_blake: bool,
        expected_len: u64,
    ) -> std::sync::Arc<Self> {
        let pipelined =
            !compute_rar14 && (compute_crc || compute_blake) && expected_len >= PIPELINE_MIN_BYTES;
        let state = if pipelined {
            StreamState::Pipelined {
                pipeline: HashPipeline::new(compute_crc, compute_blake),
                pending: Vec::new(),
            }
        } else {
            StreamState::Inline {
                crc: compute_crc.then(crc32fast::Hasher::new),
                rar14: compute_rar14.then_some(0u16),
                blake: compute_blake.then(|| Box::new(crate::crypto::Blake2spHasher::new())),
                pool: Vec::new(),
            }
        };
        std::sync::Arc::new(Self {
            state: std::sync::Mutex::new(state),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, StreamState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Borrow-slice update (writer wrappers). May copy into an internal
    /// accumulation buffer in pipelined mode. Do not mix with
    /// [`Self::submit`] on the same stream.
    pub(crate) fn update(&self, data: &[u8]) -> io::Result<()> {
        let mut state = self.lock();
        match &mut *state {
            StreamState::Inline {
                crc, rar14, blake, ..
            } => {
                inline_update(crc, rar14, blake, data);
                Ok(())
            }
            StreamState::Pipelined { pipeline, pending } => {
                pending.extend_from_slice(data);
                if pending.len() >= PENDING_FLUSH_BYTES {
                    let chunk = std::mem::take(pending);
                    pipeline.submit(chunk)?;
                }
                Ok(())
            }
            StreamState::Finished => Err(io::Error::other("hash stream already finalized")),
        }
    }

    /// Fetch a recycled, cleared buffer for the zero-extra-copy submit path.
    #[cfg(test)]
    pub(crate) fn take_buffer(&self) -> Vec<u8> {
        let mut buf = self.take_buffer_len(0);
        buf.clear();
        buf
    }

    /// Fetch a recycled buffer resized to exactly `len` bytes. Only bytes
    /// beyond previously written data are zeroed: recycled bytes may hold
    /// stale chunk data, which callers overwrite via `Read` before use.
    pub(crate) fn take_buffer_len(&self, len: usize) -> Vec<u8> {
        let mut buf = {
            let mut state = self.lock();
            match &mut *state {
                StreamState::Inline { pool, .. } => pool.pop().unwrap_or_default(),
                StreamState::Pipelined { pipeline, .. } => pipeline.take_buffer_raw(),
                StreamState::Finished => Vec::new(),
            }
        };
        if buf.len() < len {
            buf.resize(len, 0);
        } else {
            buf.truncate(len);
        }
        buf
    }

    /// Submit an owned in-order chunk previously obtained from
    /// [`Self::take_buffer`]. Do not mix with [`Self::update`].
    pub(crate) fn submit(&self, chunk: Vec<u8>) -> io::Result<()> {
        let mut state = self.lock();
        match &mut *state {
            StreamState::Inline {
                crc,
                rar14,
                blake,
                pool,
            } => {
                inline_update(crc, rar14, blake, &chunk);
                if pool.len() < MAX_IN_FLIGHT {
                    pool.push(chunk);
                }
                Ok(())
            }
            StreamState::Pipelined { pipeline, .. } => pipeline.submit(chunk),
            StreamState::Finished => Err(io::Error::other("hash stream already finalized")),
        }
    }

    /// Finish the stream and return the digests.
    pub(crate) fn finalize(&self) -> io::Result<StreamHashOutputs> {
        let mut state = self.lock();
        match std::mem::replace(&mut *state, StreamState::Finished) {
            StreamState::Inline {
                crc, rar14, blake, ..
            } => Ok(StreamHashOutputs {
                crc32: crc.map(|hasher| hasher.finalize()),
                rar14,
                blake2sp: blake.map(|hasher| hasher.finalize()),
            }),
            StreamState::Pipelined { pipeline, pending } => {
                if !pending.is_empty() {
                    pipeline.submit(pending)?;
                }
                let outputs = pipeline.finalize()?;
                Ok(StreamHashOutputs {
                    crc32: outputs.crc32,
                    rar14: None,
                    blake2sp: outputs.blake2sp,
                })
            }
            StreamState::Finished => Err(io::Error::other("hash stream already finalized")),
        }
    }
}

fn inline_update(
    crc: &mut Option<crc32fast::Hasher>,
    rar14: &mut Option<u16>,
    blake: &mut Option<Box<crate::crypto::Blake2spHasher>>,
    data: &[u8],
) {
    if let Some(hasher) = crc {
        hasher.update(data);
    }
    if let Some(checksum) = rar14 {
        *checksum = crate::rar4::header::checksum14_update(*checksum, data);
    }
    if let Some(hasher) = blake {
        hasher.update(data);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference_blake2sp(data: &[u8]) -> [u8; 32] {
        let mut out = [0u8; 32];
        out.copy_from_slice(
            blake2s_simd::blake2sp::Params::new()
                .hash_length(32)
                .hash(data)
                .as_bytes(),
        );
        out
    }

    fn deterministic_bytes(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect()
    }

    fn run_pipeline(data: &[u8], chunk_sizes: &[usize]) -> HashPipelineOutputs {
        let pipeline = HashPipeline::new(true, true);
        let mut offset = 0;
        let mut size_index = 0;
        while offset < data.len() {
            let take = chunk_sizes[size_index % chunk_sizes.len()]
                .max(1)
                .min(data.len() - offset);
            pipeline.update(&data[offset..offset + take]).unwrap();
            offset += take;
            size_index += 1;
        }
        pipeline.finalize().unwrap()
    }

    #[test]
    fn pipeline_matches_reference_hashes_across_sizes() {
        for &len in &[
            0usize, 1, 63, 64, 65, 127, 128, 511, 512, 513, 1024, 4095, 4096, 4097, 65_535, 65_536,
            65_537, 1_000_000,
        ] {
            let data = deterministic_bytes(len, len as u64);
            let outputs = run_pipeline(&data, &[97, 64, 1, 511, 4096, 1 << 20]);
            assert_eq!(
                outputs.crc32.unwrap(),
                crc32fast::hash(&data),
                "crc mismatch at len {len}"
            );
            assert_eq!(
                outputs.blake2sp.unwrap(),
                reference_blake2sp(&data),
                "blake2sp mismatch at len {len}"
            );
        }
    }

    #[test]
    fn pipeline_matches_reference_on_large_streams() {
        let data = deterministic_bytes(23_456_789, 42);
        let outputs = run_pipeline(&data, &[CHUNK_CAPACITY]);
        assert_eq!(outputs.crc32.unwrap(), crc32fast::hash(&data));
        assert_eq!(outputs.blake2sp.unwrap(), reference_blake2sp(&data));
    }

    #[test]
    fn manual_lane_construction_matches_blake2sp_reference() {
        // Direct check of the tree parameters, independent of threading.
        for &len in &[0usize, 1, 64, 512, 5_000, 100_000] {
            let data = deterministic_bytes(len, 7 + len as u64);
            let mut leaves: Vec<_> = (0..LANES)
                .map(|lane| blake2sp_leaf_params(lane).to_state())
                .collect();
            for (index, block) in data.chunks(BLAKE_BLOCK).enumerate() {
                leaves[index % LANES].update(block);
            }
            let mut root = blake2sp_root_params().to_state();
            for leaf in &mut leaves {
                root.update(leaf.finalize().as_bytes());
            }
            let mut out = [0u8; 32];
            out.copy_from_slice(root.finalize().as_bytes());
            assert_eq!(out, reference_blake2sp(&data), "len {len}");
        }
    }

    #[test]
    fn shared_stream_matches_reference_in_both_modes() {
        // Below the pipeline threshold (inline) and above it (pipelined),
        // via both the update() and take_buffer()/submit() interfaces.
        for &len in &[100_000usize, (PIPELINE_MIN_BYTES as usize) + 12_345] {
            let data = deterministic_bytes(len, len as u64);

            let via_update = SharedHashStream::new(true, false, true, len as u64);
            for chunk in data.chunks(70_001) {
                via_update.update(chunk).unwrap();
            }
            let outputs = via_update.finalize().unwrap();
            assert_eq!(outputs.crc32.unwrap(), crc32fast::hash(&data));
            assert_eq!(outputs.blake2sp.unwrap(), reference_blake2sp(&data));

            let via_submit = SharedHashStream::new(true, false, true, len as u64);
            for chunk in data.chunks(CHUNK_CAPACITY) {
                let mut buf = via_submit.take_buffer();
                buf.extend_from_slice(chunk);
                via_submit.submit(buf).unwrap();
            }
            let outputs = via_submit.finalize().unwrap();
            assert_eq!(outputs.crc32.unwrap(), crc32fast::hash(&data));
            assert_eq!(outputs.blake2sp.unwrap(), reference_blake2sp(&data));
        }
    }

    #[test]
    fn shared_stream_rar14_stays_inline_and_matches() {
        let data = deterministic_bytes(50_000_000, 3);
        let stream = SharedHashStream::new(false, true, false, data.len() as u64);
        stream.update(&data).unwrap();
        let outputs = stream.finalize().unwrap();
        let mut expected = 0u16;
        expected = crate::rar4::header::checksum14_update(expected, &data);
        assert_eq!(outputs.rar14.unwrap(), expected);
        assert!(outputs.crc32.is_none());
    }

    #[test]
    fn crc_only_pipeline_recycles_buffers() {
        let pipeline = HashPipeline::new(true, false);
        let data = deterministic_bytes(3 * CHUNK_CAPACITY + 17, 9);
        for chunk in data.chunks(CHUNK_CAPACITY) {
            let mut buf = pipeline.take_buffer();
            buf.extend_from_slice(chunk);
            pipeline.submit(buf).unwrap();
        }
        let outputs = pipeline.finalize().unwrap();
        assert_eq!(outputs.crc32.unwrap(), crc32fast::hash(&data));
        assert!(outputs.blake2sp.is_none());
    }

    #[test]
    fn crc_shift_op_folds_chunk_crcs() {
        let data: Vec<u8> = (0..1_000_003).map(|i| (i * 131 % 256) as u8).collect();
        let whole = crc32fast::hash(&data);
        for split in [0usize, 1, 63, 4096, 65_536, 999_999, 1_000_003] {
            let (a, b) = data.split_at(split);
            let folded =
                CrcShiftOp::new(b.len() as u64).shift(crc32fast::hash(a)) ^ crc32fast::hash(b);
            assert_eq!(folded, whole, "split at {split}");
        }
    }
}
