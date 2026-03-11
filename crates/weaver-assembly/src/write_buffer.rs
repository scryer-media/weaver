use std::collections::BTreeMap;

use weaver_core::buffer::BufferHandle;

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
    pending: BTreeMap<u64, T>,
    /// The next expected sequential write offset.
    write_cursor: u64,
    /// Maximum number of buffered segments before forcing eviction.
    max_pending: usize,
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
        }
    }

    /// Insert a decoded segment and return any segments that are now ready
    /// for sequential writing.
    ///
    /// Returns a vec of `(offset, data)` pairs in write order. The caller
    /// should write them to disk at the given offsets (which will be
    /// sequential whenever possible).
    pub fn insert(&mut self, offset: u64, data: T) -> Vec<(u64, T)> {
        self.pending.insert(offset, data);

        // Drain contiguous segments starting from write_cursor.
        let mut ready = Vec::new();
        while let Some(entry) = self.pending.first_key_value() {
            if *entry.0 != self.write_cursor {
                break;
            }
            // Safe: we just confirmed the key exists.
            let (off, buf) = self.pending.pop_first().unwrap();
            self.write_cursor += buf.len_bytes() as u64;
            ready.push((off, buf));
        }

        // If nothing was drained and we exceed capacity, force-evict the
        // oldest (lowest offset) entry to guarantee forward progress.
        if ready.is_empty() && self.pending.len() > self.max_pending {
            let (off, buf) = self.pending.pop_first().unwrap();
            ready.push((off, buf));
        }

        ready
    }

    /// Flush all remaining buffered segments, sorted by offset.
    ///
    /// Call this when a file is complete to drain any stragglers that never
    /// formed a contiguous run with the write cursor.
    pub fn flush_all(&mut self) -> Vec<(u64, T)> {
        let mut out = Vec::with_capacity(self.pending.len());
        while let Some((off, buf)) = self.pending.pop_first() {
            out.push((off, buf));
        }
        self.write_cursor = 0;
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequential_inserts_return_immediately() {
        let mut buf = WriteReorderBuffer::<Vec<u8>>::new(16);

        // Insert segments in perfect order.
        let ready = buf.insert(0, vec![0u8; 1000]);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, 0);
        assert_eq!(ready[0].1.len(), 1000);

        let ready = buf.insert(1000, vec![1u8; 500]);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, 1000);

        let ready = buf.insert(1500, vec![2u8; 200]);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, 1500);

        // Nothing should remain buffered.
        let flushed = buf.flush_all();
        assert!(flushed.is_empty());
    }

    #[test]
    fn out_of_order_buffers_until_gap_filled() {
        let mut buf = WriteReorderBuffer::<Vec<u8>>::new(16);

        // Insert segment 2 first (offset 2000), then segment 1 (offset 1000).
        // Neither can be written yet because segment 0 (offset 0) is missing.
        let ready = buf.insert(2000, vec![0u8; 1000]);
        assert!(ready.is_empty());

        let ready = buf.insert(1000, vec![0u8; 1000]);
        assert!(ready.is_empty());

        // Now insert segment 0 — all three should drain in order.
        let ready = buf.insert(0, vec![0u8; 1000]);
        assert_eq!(ready.len(), 3);
        assert_eq!(ready[0].0, 0);
        assert_eq!(ready[1].0, 1000);
        assert_eq!(ready[2].0, 2000);
    }

    #[test]
    fn overflow_forces_oldest_eviction() {
        let mut buf = WriteReorderBuffer::<Vec<u8>>::new(2);

        // Insert 2 segments that don't start at write_cursor (0).
        let ready = buf.insert(1000, vec![0u8; 500]);
        assert!(ready.is_empty());

        let ready = buf.insert(2000, vec![0u8; 500]);
        assert!(ready.is_empty());

        // Third insert exceeds max_pending (2), should force-evict oldest.
        let ready = buf.insert(3000, vec![0u8; 500]);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].0, 1000); // Lowest offset evicted.
    }

    #[test]
    fn flush_all_drains_everything() {
        let mut buf = WriteReorderBuffer::<Vec<u8>>::new(16);

        buf.insert(5000, vec![0u8; 100]);
        buf.insert(3000, vec![0u8; 200]);
        buf.insert(1000, vec![0u8; 300]);

        let flushed = buf.flush_all();
        assert_eq!(flushed.len(), 3);
        // Should be sorted by offset.
        assert_eq!(flushed[0].0, 1000);
        assert_eq!(flushed[1].0, 3000);
        assert_eq!(flushed[2].0, 5000);
    }

    #[test]
    fn partial_contiguous_run() {
        let mut buf = WriteReorderBuffer::<Vec<u8>>::new(16);

        // Insert offset 0 and 1000, but gap at 2000.
        let ready = buf.insert(0, vec![0u8; 1000]);
        assert_eq!(ready.len(), 1);

        // Insert offset 2000 (gap at 1000).
        let ready = buf.insert(2000, vec![0u8; 500]);
        assert!(ready.is_empty());

        // Fill the gap — should release 1000 and 2000.
        let ready = buf.insert(1000, vec![0u8; 1000]);
        assert_eq!(ready.len(), 2);
        assert_eq!(ready[0].0, 1000);
        assert_eq!(ready[1].0, 2000);
    }
}
