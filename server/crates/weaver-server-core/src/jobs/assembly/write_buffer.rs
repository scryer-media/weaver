use std::collections::BTreeMap;

use crate::runtime::buffers::BufferHandle;

pub trait BufferedChunk {
    fn len_bytes(&self) -> usize;
}

impl BufferedChunk for Vec<u8> {
    fn len_bytes(&self) -> usize {
        self.len()
    }
}

impl BufferedChunk for BufferHandle {
    fn len_bytes(&self) -> usize {
        self.len()
    }
}

/// Reorder buffer that collects out-of-order decoded segments and releases
/// them in sequential file-offset order, enabling sequential disk writes
/// even when 50+ connections produce segments in arbitrary order.
pub struct WriteReorderBuffer<T> {
    /// Segments waiting to be written, keyed by their file offset.
    ///
    /// Every key is at or above [`write_cursor`](Self::write_cursor). A key
    /// below it could never match the cursor again — the cursor only moves
    /// forward — so it would stall every later contiguous drain.
    pending: BTreeMap<u64, PendingChunk<T>>,
    /// Duplicate arrivals covering a range the buffer has already released for
    /// writing: the same article decoded twice.
    ///
    /// They carry no ordering information, so they never enter `pending`. They
    /// are handed back on the next drain rather than dropped, because the
    /// caller charges its write backlog for every insert and only gets those
    /// bytes back when the buffer returns the chunk (or still reports it in
    /// `buffered_bytes`/`buffered_len` at teardown).
    redundant: Vec<(u64, T)>,
    /// The next expected sequential write offset.
    write_cursor: u64,
    /// Maximum number of buffered segments before forcing eviction.
    max_pending: usize,
    /// Total bytes currently retained in memory.
    buffered_bytes: usize,
    /// Number of buffered entries currently retained in memory.
    buffered_segments: usize,
}

enum PendingChunk<T> {
    Buffered(T),
    Persisted { len: usize },
}

impl<T: BufferedChunk> WriteReorderBuffer<T> {
    /// Create a new reorder buffer.
    ///
    /// `max_pending` controls how many segments can be buffered before the
    /// oldest entry is forcibly evicted to guarantee forward progress.
    pub fn new(max_pending: usize) -> Self {
        Self {
            pending: BTreeMap::new(),
            redundant: Vec::new(),
            write_cursor: 0,
            max_pending,
            buffered_bytes: 0,
            buffered_segments: 0,
        }
    }

    /// Insert a decoded segment into the buffer.
    ///
    /// A segment whose range the buffer already released for writing — it sits
    /// behind the write cursor, or an arrival for the same offset is already
    /// queued — is a duplicate of an article that was decoded twice. Its bytes
    /// are already on disk or already sequenced, so it is queued for immediate
    /// hand-back instead of taking a place in the ordered map.
    pub fn insert(&mut self, offset: u64, data: T) {
        self.buffered_bytes += data.len_bytes();
        self.buffered_segments += 1;

        if offset < self.write_cursor || self.pending.contains_key(&offset) {
            self.redundant.push((offset, data));
            return;
        }
        self.pending.insert(offset, PendingChunk::Buffered(data));
    }

    /// Drain any contiguous segments that are now ready for sequential writing.
    pub fn drain_ready(&mut self) -> Vec<(u64, T)> {
        self.drain_ready_with_contiguous_end().0
    }

    /// Drain ready segments and return the contiguous end represented by the
    /// drain, including already-persisted gaps that were bridged.
    pub fn drain_ready_with_contiguous_end(&mut self) -> (Vec<(u64, T)>, u64) {
        // Duplicate arrivals are writable immediately: everything they cover is
        // already sequenced, so they never wait on the cursor and never move it.
        // They lead the batch so that the copy holding the offset is the one
        // written last, leaving the cursor describing what is really on disk.
        let mut ready = self.take_redundant();

        // Drain contiguous segments starting from write_cursor.
        while let Some((&offset, _)) = self.pending.first_key_value() {
            if offset != self.write_cursor {
                break;
            }

            let (off, entry) = self.pending.pop_first().unwrap();
            match entry {
                PendingChunk::Buffered(buf) => {
                    let len = buf.len_bytes();
                    self.forget_buffered(len);
                    self.write_cursor += len as u64;
                    ready.push((off, buf));
                }
                PendingChunk::Persisted { len } => {
                    self.write_cursor += len as u64;
                }
            }
        }

        (ready, self.write_cursor)
    }

    /// Whether the buffer exceeds its per-file in-memory segment limit.
    pub fn exceeds_max_pending(&self) -> bool {
        self.buffered_segments > self.max_pending
    }

    /// Remove the lowest-offset buffered segment without advancing the cursor.
    ///
    /// The caller is expected to persist the returned segment directly and then
    /// reinsert a `Persisted` marker with [`mark_persisted`](Self::mark_persisted)
    /// so future sequential drains can skip over the already-written range.
    ///
    /// Duplicate arrivals come out first: they are already sequenced, so the
    /// caller can persist them without waiting on any gap. Every segment
    /// counted by [`buffered_len`](Self::buffered_len) is reachable this way,
    /// which is what keeps the callers' backlog-relief loops making progress.
    pub fn take_oldest_buffered(&mut self) -> Option<(u64, T)> {
        if !self.redundant.is_empty() {
            let (offset, buf) = self.redundant.remove(0);
            self.forget_buffered(buf.len_bytes());
            return Some((offset, buf));
        }

        let offset = self
            .pending
            .iter()
            .find_map(|(offset, entry)| match entry {
                PendingChunk::Buffered(_) => Some(*offset),
                PendingChunk::Persisted { .. } => None,
            })?;

        let entry = self.pending.remove(&offset)?;
        match entry {
            PendingChunk::Buffered(buf) => {
                self.forget_buffered(buf.len_bytes());
                Some((offset, buf))
            }
            PendingChunk::Persisted { .. } => unreachable!("selected buffered entry"),
        }
    }

    pub fn take_oldest_buffered_batch(&mut self, max_segments: usize) -> Vec<(u64, T)> {
        let mut drained = Vec::new();
        if max_segments == 0 {
            return drained;
        }

        while drained.len() < max_segments {
            let Some((offset, chunk)) = self.take_oldest_buffered() else {
                break;
            };
            drained.push((offset, chunk));
        }
        drained
    }

    /// Record that an out-of-order range has already been persisted directly.
    pub fn mark_persisted(&mut self, offset: u64, len: usize) {
        if offset < self.write_cursor {
            // The cursor already spans this range, so there is no gap for a
            // bridge marker to fill — and a key behind the cursor would stall
            // every later contiguous drain. This is a duplicate arrival that
            // backlog relief wrote out before the next drain reclaimed it.
            return;
        }

        match self.pending.insert(offset, PendingChunk::Persisted { len }) {
            None | Some(PendingChunk::Persisted { .. }) => {}
            Some(PendingChunk::Buffered(buf)) => {
                // This should not happen in normal operation. The bytes are on
                // disk now, but the caller charged its write backlog for that
                // chunk, so hand it back instead of dropping it.
                self.redundant.push((offset, buf));
            }
        }
    }

    pub fn buffered_len(&self) -> usize {
        self.buffered_segments
    }

    pub fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.redundant.is_empty()
    }

    /// Flush all remaining buffered segments, sorted by offset.
    ///
    /// Call this when a file is complete to drain any stragglers that never
    /// formed a contiguous run with the write cursor.
    pub fn flush_all(&mut self) -> Vec<(u64, T)> {
        let mut out = Vec::with_capacity(self.buffered_segments);
        out.append(&mut self.redundant);
        while let Some((off, entry)) = self.pending.pop_first() {
            if let PendingChunk::Buffered(buf) = entry {
                out.push((off, buf));
            }
        }
        out.sort_by_key(|(offset, _)| *offset);
        self.write_cursor = 0;
        self.buffered_bytes = 0;
        self.buffered_segments = 0;
        out
    }

    /// Take every queued duplicate arrival, releasing its in-memory accounting.
    /// Take every buffered segment out of the reorder stage at once,
    /// duplicates included, leaving the cursor where it stands.
    ///
    /// For the identity seam's reclaim: a file whose offset-zero article just
    /// bound to a direct set may already have later articles parked here, and
    /// every one of them belongs to the routed volume rather than to a
    /// sequential file write. Only meaningful while nothing has been
    /// persisted — the caller refuses to bind a file with flushed bytes — so
    /// `Persisted` markers are not expected and are left in place if a caller
    /// ever violates that.
    pub fn take_all_buffered(&mut self) -> Vec<(u64, T)> {
        let mut taken = self.take_redundant();
        let offsets: Vec<u64> = self
            .pending
            .iter()
            .filter_map(|(offset, entry)| {
                matches!(entry, PendingChunk::Buffered(_)).then_some(*offset)
            })
            .collect();
        for offset in offsets {
            if let Some(PendingChunk::Buffered(buf)) = self.pending.remove(&offset) {
                self.forget_buffered(buf.len_bytes());
                taken.push((offset, buf));
            }
        }
        taken
    }

    fn take_redundant(&mut self) -> Vec<(u64, T)> {
        let taken = std::mem::take(&mut self.redundant);
        for (_, chunk) in &taken {
            self.forget_buffered(chunk.len_bytes());
        }
        taken
    }

    /// Drop one retained chunk from the in-memory accounting.
    fn forget_buffered(&mut self, len: usize) {
        self.buffered_bytes = self.buffered_bytes.saturating_sub(len);
        self.buffered_segments = self.buffered_segments.saturating_sub(1);
    }
}

#[cfg(test)]
mod tests;
