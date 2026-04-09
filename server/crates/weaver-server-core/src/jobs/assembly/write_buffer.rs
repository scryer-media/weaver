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
    pending: BTreeMap<u64, PendingChunk<T>>,
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
            write_cursor: 0,
            max_pending,
            buffered_bytes: 0,
            buffered_segments: 0,
        }
    }

    /// Insert a decoded segment into the buffer.
    pub fn insert(&mut self, offset: u64, data: T) {
        let len = data.len_bytes();
        if let Some(replaced) = self.pending.insert(offset, PendingChunk::Buffered(data)) {
            self.remove_accounting(&replaced);
        }
        self.buffered_bytes += len;
        self.buffered_segments += 1;
    }

    /// Drain any contiguous segments that are now ready for sequential writing.
    pub fn drain_ready(&mut self) -> Vec<(u64, T)> {
        self.drain_ready_with_contiguous_end().0
    }

    /// Drain ready segments and return the contiguous end represented by the
    /// drain, including already-persisted gaps that were bridged.
    pub fn drain_ready_with_contiguous_end(&mut self) -> (Vec<(u64, T)>, u64) {
        // Drain contiguous segments starting from write_cursor.
        let mut ready = Vec::new();
        while let Some((&offset, _)) = self.pending.first_key_value() {
            if offset != self.write_cursor {
                break;
            }

            let (off, entry) = self.pending.pop_first().unwrap();
            match entry {
                PendingChunk::Buffered(buf) => {
                    let len = buf.len_bytes();
                    self.buffered_bytes = self.buffered_bytes.saturating_sub(len);
                    self.buffered_segments = self.buffered_segments.saturating_sub(1);
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
    pub fn take_oldest_buffered(&mut self) -> Option<(u64, T)> {
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
                let len = buf.len_bytes();
                self.buffered_bytes = self.buffered_bytes.saturating_sub(len);
                self.buffered_segments = self.buffered_segments.saturating_sub(1);
                Some((offset, buf))
            }
            PendingChunk::Persisted { .. } => unreachable!("selected buffered entry"),
        }
    }

    /// Record that an out-of-order range has already been persisted directly.
    pub fn mark_persisted(&mut self, offset: u64, len: usize) {
        match self.pending.insert(offset, PendingChunk::Persisted { len }) {
            None | Some(PendingChunk::Persisted { .. }) => {}
            Some(PendingChunk::Buffered(buf)) => {
                // This should not happen in normal operation, but keep
                // accounting consistent if a caller overwrote a buffered entry.
                let bytes = buf.len_bytes();
                self.buffered_bytes = self.buffered_bytes.saturating_sub(bytes);
                self.buffered_segments = self.buffered_segments.saturating_sub(1);
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
        self.pending.is_empty()
    }

    /// Flush all remaining buffered segments, sorted by offset.
    ///
    /// Call this when a file is complete to drain any stragglers that never
    /// formed a contiguous run with the write cursor.
    pub fn flush_all(&mut self) -> Vec<(u64, T)> {
        let mut out = Vec::with_capacity(self.buffered_segments);
        while let Some((off, entry)) = self.pending.pop_first() {
            if let PendingChunk::Buffered(buf) = entry {
                out.push((off, buf));
            }
        }
        self.write_cursor = 0;
        self.buffered_bytes = 0;
        self.buffered_segments = 0;
        out
    }

    fn remove_accounting(&mut self, entry: &PendingChunk<T>) {
        if let PendingChunk::Buffered(buf) = entry {
            self.buffered_bytes = self.buffered_bytes.saturating_sub(buf.len_bytes());
            self.buffered_segments = self.buffered_segments.saturating_sub(1);
        }
    }
}

#[cfg(test)]
mod tests;
