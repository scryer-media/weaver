//! Sliding window / ring buffer for LZ decompression.
//!
//! The window holds previously decompressed output. Length-distance pairs
//! reference bytes already written to the window. The window wraps around
//! when it reaches the dictionary size.

use std::io::Write;
use std::ptr;

use crate::error::{RarError, RarResult};

/// Maximum incremental match length used by RAR LZ decoding.
const MAX_INC_LZ_MATCH: usize = 0x1004;
/// Fragmented dictionary cutoff for 32-bit fallback allocations.
#[cfg_attr(not(target_pointer_width = "32"), allow(dead_code))]
const FRAGMENTED_MIN_WINDOW: usize = 0x1000000;
/// Minimum fragmented dictionary allocation attempt.
#[cfg_attr(not(any(test, target_pointer_width = "32")), allow(dead_code))]
const FRAGMENTED_BLOCK_SIZE: usize = 0x400000;
/// Maximum fragmented dictionary block count.
#[cfg_attr(not(any(test, target_pointer_width = "32")), allow(dead_code))]
const FRAGMENTED_MAX_BLOCKS: usize = 32;

enum WindowStorage {
    Contiguous(Vec<u8>),
    // On 64-bit release builds the fragmented fallback is unreachable, so the
    // variant is compiled out entirely: every storage access then reduces to
    // a direct Vec access with no discriminant dispatch on the hot path.
    #[cfg(any(test, target_pointer_width = "32"))]
    Fragmented(FragmentedWindow),
}

impl WindowStorage {
    fn try_contiguous(len: usize) -> Result<Self, String> {
        debug_assert!(len > 0);
        let layout = std::alloc::Layout::array::<u8>(len).map_err(|err| err.to_string())?;
        // alloc_zeroed reaches calloc/mmap for large sizes, so the OS hands
        // back lazy zero pages: a RAR7-scale dictionary (>4 GiB) reserves
        // address space up front but commits physical pages only as the
        // window fills. Vec::resize(len, 0) would memset-commit the whole
        // declared size before the first decoded byte. Window reads of
        // never-written areas must still observe zeros.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(format!("allocation of {len} bytes failed"));
        }
        // SAFETY: ptr came from the global allocator with the exact layout
        // Vec<u8> uses to deallocate len == capacity bytes, and every byte is
        // initialized (zeroed).
        let buf = unsafe { Vec::from_raw_parts(ptr, len, len) };
        Ok(Self::Contiguous(buf))
    }

    #[cfg(target_pointer_width = "32")]
    fn try_fragmented(len: usize) -> RarResult<Self> {
        Ok(Self::Fragmented(FragmentedWindow::try_new(
            len,
            FRAGMENTED_BLOCK_SIZE,
        )?))
    }

    #[cfg(test)]
    fn fragmented_for_test(len: usize, block_size: usize) -> RarResult<Self> {
        Ok(Self::Fragmented(FragmentedWindow::try_new_fixed_blocks(
            len, block_size,
        )?))
    }

    #[inline(always)]
    fn len(&self) -> usize {
        match self {
            Self::Contiguous(buf) => buf.len(),
            #[cfg(any(test, target_pointer_width = "32"))]
            Self::Fragmented(buf) => buf.len(),
        }
    }

    fn is_fragmented(&self) -> bool {
        match self {
            Self::Contiguous(_) => false,
            #[cfg(any(test, target_pointer_width = "32"))]
            Self::Fragmented(_) => true,
        }
    }

    fn as_mut_slice(&mut self) -> Option<&mut [u8]> {
        match self {
            Self::Contiguous(buf) => Some(buf.as_mut_slice()),
            #[cfg(any(test, target_pointer_width = "32"))]
            Self::Fragmented(_) => None,
        }
    }

    #[inline(always)]
    fn get(&self, idx: usize) -> u8 {
        match self {
            Self::Contiguous(buf) => buf[idx],
            #[cfg(any(test, target_pointer_width = "32"))]
            Self::Fragmented(buf) => buf.get(idx),
        }
    }

    #[inline(always)]
    fn set(&mut self, idx: usize, value: u8) {
        match self {
            Self::Contiguous(buf) => buf[idx] = value,
            #[cfg(any(test, target_pointer_width = "32"))]
            Self::Fragmented(buf) => buf.set(idx, value),
        }
    }

    fn copy_from_slice(&mut self, start: usize, bytes: &[u8]) {
        match self {
            Self::Contiguous(buf) => buf[start..start + bytes.len()].copy_from_slice(bytes),
            #[cfg(any(test, target_pointer_width = "32"))]
            Self::Fragmented(buf) => buf.copy_from_slice(start, bytes),
        }
    }

    fn fill(&mut self, start: usize, len: usize, value: u8) {
        match self {
            Self::Contiguous(buf) => buf[start..start + len].fill(value),
            #[cfg(any(test, target_pointer_width = "32"))]
            Self::Fragmented(buf) => buf.fill(start, len, value),
        }
    }

    fn fill_all(&mut self, value: u8) {
        match self {
            Self::Contiguous(buf) => buf.fill(value),
            #[cfg(any(test, target_pointer_width = "32"))]
            Self::Fragmented(buf) => buf.fill_all(value),
        }
    }

    fn extend_from_range(&self, start: usize, len: usize, out: &mut Vec<u8>) {
        match self {
            Self::Contiguous(buf) => out.extend_from_slice(&buf[start..start + len]),
            #[cfg(any(test, target_pointer_width = "32"))]
            Self::Fragmented(buf) => buf.extend_from_range(start, len, out),
        }
    }

    fn write_range_to_writer<W: Write + ?Sized>(
        &self,
        start: usize,
        len: usize,
        writer: &mut W,
    ) -> std::io::Result<()> {
        match self {
            Self::Contiguous(buf) => writer.write_all(&buf[start..start + len]),
            #[cfg(any(test, target_pointer_width = "32"))]
            Self::Fragmented(buf) => buf.write_range_to_writer(start, len, writer),
        }
    }
}

#[cfg_attr(not(any(test, target_pointer_width = "32")), allow(dead_code))]
struct FragmentedWindow {
    blocks: Vec<Box<[u8]>>,
    cumulative_ends: Vec<usize>,
    len: usize,
}

#[cfg_attr(not(target_pointer_width = "32"), allow(dead_code))]
impl FragmentedWindow {
    fn try_new(len: usize, min_block_size: usize) -> RarResult<Self> {
        if len == 0 || min_block_size == 0 {
            return Err(RarError::DictionaryTooLarge {
                size: len as u64,
                max: 0,
            });
        }

        let mut blocks = Vec::new();
        blocks
            .try_reserve_exact(FRAGMENTED_MAX_BLOCKS)
            .map_err(|err| RarError::ResourceLimit {
                detail: format!(
                    "failed to reserve fragmented RAR dictionary block table for {len} bytes: {err}"
                ),
            })?;
        let mut cumulative_ends = Vec::new();
        cumulative_ends
            .try_reserve_exact(FRAGMENTED_MAX_BLOCKS)
            .map_err(|err| RarError::ResourceLimit {
                detail: format!(
                    "failed to reserve fragmented RAR dictionary bounds for {len} bytes: {err}"
                ),
            })?;

        let mut allocated = 0usize;
        while allocated < len && blocks.len() < FRAGMENTED_MAX_BLOCKS {
            let remaining = len - allocated;
            let min_chunk =
                Self::fragmented_min_block_size(remaining, blocks.len(), min_block_size)?;
            let mut chunk = remaining;
            let block = loop {
                match Self::try_zeroed_block(chunk) {
                    Ok(block) => break block,
                    Err(err) if chunk > min_chunk => {
                        let reduced = chunk.saturating_sub((chunk / 32).max(1));
                        chunk = reduced.max(min_chunk);
                        if chunk == remaining {
                            return Err(RarError::ResourceLimit {
                                detail: format!(
                                    "failed to allocate {chunk} byte RAR fragmented dictionary block: {err}"
                                ),
                            });
                        }
                    }
                    Err(err) => {
                        return Err(RarError::ResourceLimit {
                            detail: format!(
                                "failed to allocate at least {min_chunk} bytes for RAR fragmented dictionary block: {err}"
                            ),
                        });
                    }
                }
            };
            allocated += block.len();
            blocks.push(block);
            cumulative_ends.push(allocated);
        }

        if allocated < len {
            return Err(RarError::ResourceLimit {
                detail: format!(
                    "failed to allocate {len} byte fragmented RAR dictionary within {FRAGMENTED_MAX_BLOCKS} blocks"
                ),
            });
        }

        Ok(Self {
            blocks,
            cumulative_ends,
            len,
        })
    }

    #[cfg(test)]
    fn try_new_fixed_blocks(len: usize, block_size: usize) -> RarResult<Self> {
        if len == 0 || block_size == 0 {
            return Err(RarError::DictionaryTooLarge {
                size: len as u64,
                max: 0,
            });
        }

        let block_count = len.div_ceil(block_size);
        if block_count > FRAGMENTED_MAX_BLOCKS {
            return Err(RarError::ResourceLimit {
                detail: format!(
                    "failed to allocate {len} byte fragmented RAR dictionary: requires {block_count} blocks, maximum {FRAGMENTED_MAX_BLOCKS}"
                ),
            });
        }

        let mut blocks = Vec::new();
        blocks
            .try_reserve_exact(block_count)
            .map_err(|err| RarError::ResourceLimit {
                detail: format!(
                    "failed to reserve fragmented RAR dictionary block table for {len} bytes: {err}"
                ),
            })?;
        let mut cumulative_ends = Vec::new();
        cumulative_ends
            .try_reserve_exact(block_count)
            .map_err(|err| RarError::ResourceLimit {
                detail: format!(
                    "failed to reserve fragmented RAR dictionary bounds for {len} bytes: {err}"
                ),
            })?;

        let mut allocated = 0usize;
        while allocated < len {
            let chunk = block_size.min(len - allocated);
            let mut block = Vec::new();
            block
                .try_reserve_exact(chunk)
                .map_err(|err| RarError::ResourceLimit {
                    detail: format!(
                        "failed to allocate {chunk} byte RAR fragmented dictionary block: {err}"
                    ),
                })?;
            block.resize(chunk, 0);
            allocated += chunk;
            blocks.push(block.into_boxed_slice());
            cumulative_ends.push(allocated);
        }

        Ok(Self {
            blocks,
            cumulative_ends,
            len,
        })
    }

    fn fragmented_min_block_size(
        remaining: usize,
        block_num: usize,
        min_block_size: usize,
    ) -> RarResult<usize> {
        let attempts_left = FRAGMENTED_MAX_BLOCKS
            .checked_sub(block_num)
            .ok_or_else(|| RarError::ResourceLimit {
                detail: format!(
                    "RAR fragmented dictionary exceeded {FRAGMENTED_MAX_BLOCKS} blocks"
                ),
            })?;
        if attempts_left == 0 {
            return Err(RarError::ResourceLimit {
                detail: "RAR fragmented dictionary has no remaining block slots".to_string(),
            });
        }

        Ok((remaining / attempts_left).max(min_block_size))
    }

    fn try_zeroed_block(len: usize) -> Result<Box<[u8]>, std::collections::TryReserveError> {
        let mut block = Vec::new();
        block.try_reserve_exact(len)?;
        block.resize(len, 0);
        Ok(block.into_boxed_slice())
    }

    fn len(&self) -> usize {
        self.len
    }

    fn locate(&self, idx: usize) -> (usize, usize) {
        debug_assert!(idx < self.len);
        let block = self
            .cumulative_ends
            .partition_point(|&block_end| block_end <= idx);
        let block_start = if block == 0 {
            0
        } else {
            self.cumulative_ends[block - 1]
        };
        (block, idx - block_start)
    }

    fn contiguous_len(&self, idx: usize, len: usize) -> usize {
        let (block, offset) = self.locate(idx);
        let block_len = self.blocks[block].len();
        (block_len - offset).min(len)
    }

    fn get(&self, idx: usize) -> u8 {
        let (block, offset) = self.locate(idx);
        self.blocks[block][offset]
    }

    fn set(&mut self, idx: usize, value: u8) {
        let (block, offset) = self.locate(idx);
        self.blocks[block][offset] = value;
    }

    fn copy_from_slice(&mut self, mut start: usize, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            let chunk = self.contiguous_len(start, bytes.len());
            let (block, offset) = self.locate(start);
            self.blocks[block][offset..offset + chunk].copy_from_slice(&bytes[..chunk]);
            start += chunk;
            bytes = &bytes[chunk..];
        }
    }

    fn fill(&mut self, mut start: usize, mut len: usize, value: u8) {
        while len > 0 {
            let chunk = self.contiguous_len(start, len);
            let (block, offset) = self.locate(start);
            self.blocks[block][offset..offset + chunk].fill(value);
            start += chunk;
            len -= chunk;
        }
    }

    fn fill_all(&mut self, value: u8) {
        for block in &mut self.blocks {
            block.fill(value);
        }
    }

    fn extend_from_range(&self, mut start: usize, mut len: usize, out: &mut Vec<u8>) {
        while len > 0 {
            let chunk = self.contiguous_len(start, len);
            let (block, offset) = self.locate(start);
            out.extend_from_slice(&self.blocks[block][offset..offset + chunk]);
            start += chunk;
            len -= chunk;
        }
    }

    fn write_range_to_writer<W: Write + ?Sized>(
        &self,
        mut start: usize,
        mut len: usize,
        writer: &mut W,
    ) -> std::io::Result<()> {
        while len > 0 {
            let chunk = self.contiguous_len(start, len);
            let (block, offset) = self.locate(start);
            writer.write_all(&self.blocks[block][offset..offset + chunk])?;
            start += chunk;
            len -= chunk;
        }
        Ok(())
    }
}

/// Sliding window ring buffer used during LZ decompression.
pub struct Window {
    /// The ring buffer.
    buf: WindowStorage,
    /// Current write position (wraps at buf.len()).
    pos: usize,
    /// Total number of bytes ever written (monotonically increasing).
    total_written: u64,
    /// Absolute dictionary position consumed by the external writer.
    total_flushed: u64,
    /// Absolute output ranges that advanced the dictionary but must not be
    /// emitted to callers (truncated final matches). Almost always empty, so
    /// the visible output is derived as the complement — this keeps the
    /// per-literal hot path free of range bookkeeping.
    invisible_ranges: Vec<(u64, u64)>,
}

impl Window {
    /// Create a new window with the given dictionary size.
    ///
    /// The dictionary size determines the maximum lookback distance for
    /// length-distance copies.
    pub fn new(dict_size: usize) -> Self {
        Self::try_new(dict_size).expect("window allocation failed")
    }

    /// Fallibly create a new window with the given dictionary size.
    ///
    /// Use fallible allocation on production paths so a crafted large-window
    /// archive cannot abort the daemon through Rust's infallible Vec allocation.
    pub fn try_new(dict_size: usize) -> RarResult<Self> {
        if dict_size == 0 {
            return Err(RarError::DictionaryTooLarge { size: 0, max: 0 });
        }

        let buf = match WindowStorage::try_contiguous(dict_size) {
            Ok(buf) => buf,
            Err(err) => {
                #[cfg(target_pointer_width = "32")]
                if dict_size >= FRAGMENTED_MIN_WINDOW {
                    return Ok(Self {
                        buf: WindowStorage::try_fragmented(dict_size)?,
                        pos: 0,
                        total_written: 0,
                        total_flushed: 0,
                        invisible_ranges: Vec::new(),
                    });
                }

                return Err(RarError::ResourceLimit {
                    detail: format!("failed to allocate {dict_size} byte RAR dictionary: {err}"),
                });
            }
        };

        Ok(Self {
            buf,
            pos: 0,
            total_written: 0,
            total_flushed: 0,
            invisible_ranges: Vec::new(),
        })
    }

    #[cfg(test)]
    fn new_fragmented_for_test(dict_size: usize, block_size: usize) -> Self {
        Self {
            buf: WindowStorage::fragmented_for_test(dict_size, block_size)
                .expect("fragmented window allocation failed"),
            pos: 0,
            total_written: 0,
            total_flushed: 0,
            invisible_ranges: Vec::new(),
        }
    }

    /// Record a dictionary-only advance: bytes in `[start_total,
    /// start_total + len)` advanced the window but must not reach callers.
    fn mark_invisible(&mut self, start_total: u64, len: u64) {
        if len == 0 {
            return;
        }
        if let Some((last_start, last_len)) = self.invisible_ranges.last_mut()
            && last_start.saturating_add(*last_len) == start_total
        {
            *last_len += len;
            return;
        }
        self.invisible_ranges.push((start_total, len));
    }

    /// Sum of invisible bytes intersecting `[start, end)`.
    fn invisible_overlap(&self, start: u64, end: u64) -> u64 {
        if self.invisible_ranges.is_empty() {
            return 0;
        }
        self.invisible_ranges
            .iter()
            .map(|&(inv_start, inv_len)| {
                let inv_end = inv_start.saturating_add(inv_len);
                let overlap_start = inv_start.max(start);
                let overlap_end = inv_end.min(end);
                overlap_end.saturating_sub(overlap_start)
            })
            .sum()
    }

    /// Drop invisible ranges that ended at or before the flushed border.
    fn gc_invisible(&mut self) {
        if self.invisible_ranges.is_empty() {
            return;
        }
        let border = self.total_flushed;
        self.invisible_ranges
            .retain(|&(start, len)| start.saturating_add(len) > border);
    }

    /// Write a single literal byte to the window.
    #[inline(always)]
    pub fn put_byte(&mut self, b: u8) {
        self.buf.set(self.pos, b);
        self.pos += 1;
        if self.pos >= self.buf.len() {
            self.pos = 0;
        }
        self.total_written += 1;
    }

    /// Write an up-to-8-byte literal batch from the parallel apply loop.
    ///
    /// A full batch away from the window edge compiles to one fixed 8-byte
    /// store; the variable-length `put_bytes` path costs a memmove call per
    /// batch, which dominates the apply phase on literal-heavy streams.
    #[inline(always)]
    pub fn put_literal_batch(&mut self, bytes: &[u8; 8], n: usize) {
        debug_assert!((1..=8).contains(&n));
        if n == 8
            && let WindowStorage::Contiguous(buf) = &mut self.buf
            && self.pos + 8 <= buf.len()
        {
            buf[self.pos..self.pos + 8].copy_from_slice(bytes);
            self.pos += 8;
            if self.pos == buf.len() {
                self.pos = 0;
            }
            self.total_written += 8;
            return;
        }
        self.put_bytes(&bytes[..n]);
    }

    /// Write a contiguous slice of literal bytes to the window.
    #[inline]
    pub fn put_bytes(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }

        let dict_size = self.buf.len();
        let dst = self.pos;
        let length = bytes.len();

        if dst + length <= dict_size {
            self.buf.copy_from_slice(dst, bytes);
            self.pos = dst + length;
            if self.pos == dict_size {
                self.pos = 0;
            }
        } else {
            let first = dict_size - dst;
            self.buf.copy_from_slice(dst, &bytes[..first]);
            let second = length - first;
            self.buf.copy_from_slice(0, &bytes[first..]);
            self.pos = second;
        }

        self.total_written += length as u64;
    }

    #[inline]
    fn copy_no_wrap_fast(&mut self, src: usize, distance: usize, length: usize) {
        let dst = self.pos;
        let buf = self
            .buf
            .as_mut_slice()
            .expect("no-wrap fast copy requires contiguous window storage");

        unsafe {
            let buf_ptr = buf.as_mut_ptr();
            let src_ptr = buf_ptr.add(src);
            let dst_ptr = buf_ptr.add(dst);

            if distance >= length {
                ptr::copy_nonoverlapping(src_ptr, dst_ptr, length);
            } else {
                // Seed from the look-back source once, then expand from the
                // just-written destination prefix.
                let mut copied = distance;
                ptr::copy_nonoverlapping(src_ptr, dst_ptr, copied);

                while copied < length {
                    let chunk = copied.min(length - copied);
                    ptr::copy_nonoverlapping(dst_ptr, dst_ptr.add(copied), chunk);
                    copied += chunk;
                }
            }
        }

        self.pos = dst + length;
        self.total_written += length as u64;
    }

    /// Copy `length` bytes from `distance` bytes back in the output.
    ///
    /// Handles overlapping copies correctly (e.g., distance=1, length=100
    /// repeats the last byte 100 times).
    #[inline]
    fn copy_advance(&mut self, distance: usize, length: usize) -> RarResult<()> {
        let dict_size = self.buf.len();
        if length == 0 {
            return Ok(());
        }

        if distance == 0 {
            self.advance_preserving_window(length);
            return Ok(());
        }

        let first_win_done = self.total_written >= dict_size as u64;
        if distance > dict_size || (distance > self.pos && !first_win_done) {
            self.fill_zeroes_advance(length);
            return Ok(());
        }

        let src = if distance <= self.pos {
            self.pos - distance
        } else {
            dict_size - (distance - self.pos)
        };

        // Fast path: distance=1 is byte-fill (very common RLE pattern).
        if distance == 1 {
            let byte = self.buf.get(src);
            let dst = self.pos;
            if dst + length <= dict_size {
                self.buf.fill(dst, length, byte);
                self.pos = dst + length;
                if self.pos >= dict_size {
                    self.pos = 0;
                }
            } else {
                let mut remaining = length;
                while remaining > 0 {
                    let chunk = remaining.min(dict_size - self.pos);
                    self.buf.fill(self.pos, chunk, byte);
                    self.pos += chunk;
                    if self.pos == dict_size {
                        self.pos = 0;
                    }
                    remaining -= chunk;
                }
            }
            self.total_written += length as u64;
            return Ok(());
        }

        // If both pointers are sufficiently far
        // from the end of the window, CopyString can avoid wrap handling for
        // the maximum legal match length, not just for this specific length.
        let fast_limit = dict_size.saturating_sub(MAX_INC_LZ_MATCH);
        if !self.buf.is_fragmented()
            && length <= MAX_INC_LZ_MATCH
            && src < fast_limit
            && self.pos < fast_limit
        {
            self.copy_no_wrap_fast(src, distance, length);
            return Ok(());
        }

        // General path with wrap handling and overlap support.
        // Keep it branch-light and forward-copying.
        let mut src = src;
        let mut dst = self.pos;
        let mut remaining = length;

        while remaining > 0 {
            let byte = self.buf.get(src);
            self.buf.set(dst, byte);
            src += 1;
            if src == dict_size {
                src = 0;
            }
            dst += 1;
            if dst == dict_size {
                dst = 0;
            }
            remaining -= 1;
        }

        self.pos = dst;
        self.total_written += length as u64;
        Ok(())
    }

    /// Copy `length` bytes from `distance` bytes back and expose all copied bytes.
    ///
    /// Handles overlapping copies correctly (e.g., distance=1, length=100
    /// repeats the last byte 100 times).
    #[inline]
    pub fn copy(&mut self, distance: usize, length: usize) -> RarResult<()> {
        self.copy_with_visible_len(distance, length, length)
    }

    /// Copy a full match into the dictionary while exposing only `visible_len`.
    ///
    /// Advance the sliding dictionary by the complete match even when a
    /// malformed final match crosses the declared unpacked size. Only the bytes
    /// within the declared member size are emitted to callers.
    #[inline]
    pub(crate) fn copy_with_visible_len(
        &mut self,
        distance: usize,
        length: usize,
        visible_len: usize,
    ) -> RarResult<()> {
        let start_total = self.total_written;
        self.copy_advance(distance, length)?;
        let visible = visible_len.min(length);
        if visible < length {
            self.mark_invisible(start_total + visible as u64, (length - visible) as u64);
        }
        Ok(())
    }

    /// Copy for RAR 1.5 damaged-stream compatibility.
    ///
    /// RAR 1.5 zero-fills invalid pre-window, oversize, and
    /// zero-distance matches instead of hard-failing. Valid streams never use
    /// distance zero, but this keeps damaged old-RAR recovery aligned.
    pub(crate) fn copy_rar15_with_visible_len(
        &mut self,
        distance: usize,
        length: usize,
        visible_len: usize,
    ) -> RarResult<()> {
        let start_total = self.total_written;
        if distance == 0 {
            self.fill_zeroes_advance(length);
        } else {
            self.copy_advance(distance, length)?;
        }
        let visible = visible_len.min(length);
        if visible < length {
            self.mark_invisible(start_total + visible as u64, (length - visible) as u64);
        }
        Ok(())
    }

    fn fill_zeroes_advance(&mut self, length: usize) {
        let dict_size = self.buf.len();
        let mut remaining = length;
        while remaining > 0 {
            let chunk = remaining.min(dict_size - self.pos);
            self.buf.fill(self.pos, chunk, 0);
            self.pos += chunk;
            if self.pos == dict_size {
                self.pos = 0;
            }
            remaining -= chunk;
        }
        self.total_written += length as u64;
    }

    fn advance_preserving_window(&mut self, length: usize) {
        let dict_size = self.buf.len();
        self.pos = (self.pos + length) % dict_size;
        self.total_written += length as u64;
    }

    /// Get a byte at a specific distance back from the current position.
    ///
    /// `distance` is 1-based: distance=1 returns the last byte written.
    #[inline]
    pub fn get_byte(&self, distance: usize) -> u8 {
        let dict_size = self.buf.len();
        let idx = if distance <= self.pos {
            self.pos - distance
        } else {
            dict_size - (distance - self.pos)
        };
        self.buf.get(idx)
    }

    /// Total number of bytes ever written to the window.
    pub fn total_written(&self) -> u64 {
        self.total_written
    }

    /// Current write position in the ring buffer.
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Get the dictionary size (window capacity).
    pub fn dict_size(&self) -> usize {
        self.buf.len()
    }

    /// Copy output bytes from the window into the destination buffer.
    ///
    /// `start_total` is the absolute position (based on total_written) to start copying.
    /// `len` is the number of bytes to copy.
    /// Returns the bytes copied.
    pub fn try_copy_output(&self, start_total: u64, len: usize) -> RarResult<Vec<u8>> {
        let end_total =
            start_total
                .checked_add(len as u64)
                .ok_or_else(|| RarError::CorruptArchive {
                    detail: "window output range overflows u64".to_string(),
                })?;
        if start_total > self.total_written || end_total > self.total_written {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "window output range [{start_total}, {end_total}) exceeds written output {}",
                    self.total_written
                ),
            });
        }

        let dict_size = self.buf.len();
        let distance = (self.total_written - start_total) as usize;
        if distance > dict_size {
            return Err(RarError::CorruptArchive {
                detail: format!(
                    "window output start {start_total} is outside the {} byte dictionary history",
                    dict_size
                ),
            });
        }

        let mut result = Vec::with_capacity(len);

        let mut idx = if distance <= self.pos {
            self.pos - distance
        } else {
            dict_size - (distance - self.pos)
        };

        let mut remaining = len;
        while remaining > 0 {
            let contig = (dict_size - idx).min(remaining);
            self.buf.extend_from_range(idx, contig, &mut result);
            idx = (idx + contig) % dict_size;
            remaining -= contig;
        }

        Ok(result)
    }

    #[cfg(test)]
    pub fn copy_output(&self, start_total: u64, len: usize) -> Vec<u8> {
        self.try_copy_output(start_total, len)
            .expect("valid window output range")
    }

    /// Flush unflushed bytes from the window to a writer.
    ///
    /// Writes all bytes between `total_flushed` and `total_written` to the
    /// provided writer. Handles ring buffer wrap-around correctly.
    /// Returns the number of bytes written.
    pub fn flush_to_writer<W: Write + ?Sized>(&mut self, writer: &mut W) -> std::io::Result<u64> {
        let unflushed = self.unflushed_bytes();
        if unflushed == 0 {
            self.total_flushed = self.total_written;
            self.gc_invisible();
            return Ok(0);
        }

        let dict_size = self.buf.len();
        if unflushed > dict_size as u64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "window overrun: {unflushed} unflushed bytes exceeds dictionary size {dict_size}"
                ),
            ));
        }
        self.write_visible_span(self.total_flushed, self.total_written, writer)?;
        self.total_flushed = self.total_written;
        self.gc_invisible();
        Ok(unflushed)
    }

    /// Write the visible bytes of `[start, end)` to `writer`, skipping any
    /// invisible ranges. Ranges are stored in increasing start order.
    fn write_visible_span<W: Write + ?Sized>(
        &self,
        start: u64,
        end: u64,
        writer: &mut W,
    ) -> std::io::Result<u64> {
        let mut written = 0u64;
        let mut cursor = start;
        if !self.invisible_ranges.is_empty() {
            for &(inv_start, inv_len) in &self.invisible_ranges {
                let inv_end = inv_start.saturating_add(inv_len);
                if inv_end <= cursor {
                    continue;
                }
                if inv_start >= end {
                    break;
                }
                if inv_start > cursor {
                    let gap_end = inv_start.min(end);
                    self.write_range_to_writer(cursor, (gap_end - cursor) as usize, writer)?;
                    written += gap_end - cursor;
                }
                cursor = cursor.max(inv_end.min(end));
            }
        }
        if cursor < end {
            self.write_range_to_writer(cursor, (end - cursor) as usize, writer)?;
            written += end - cursor;
        }
        Ok(written)
    }

    /// Flush visible output up to `up_to`, advancing across hidden dictionary
    /// bytes without emitting them.
    pub(crate) fn flush_visible_until<W: Write + ?Sized>(
        &mut self,
        up_to: u64,
        writer: &mut W,
    ) -> std::io::Result<u64> {
        let target = up_to.min(self.total_written);
        if target <= self.total_flushed {
            return Ok(0);
        }

        let written = self.write_visible_span(self.total_flushed, target, writer)?;
        self.total_flushed = target;
        self.gc_invisible();
        Ok(written)
    }

    /// Return visible subranges within `[start_total, start_total + len)`.
    ///
    /// Each tuple is `(offset_inside_range, visible_len)`. Hidden bytes are
    /// dictionary-only advancement and must not be emitted to callers.
    pub(crate) fn visible_subranges(&self, start_total: u64, len: usize) -> Vec<(usize, usize)> {
        let end_total = start_total
            .saturating_add(len as u64)
            .min(self.total_written);
        let mut ranges = Vec::new();
        let mut cursor = start_total;

        for &(inv_start, inv_len) in &self.invisible_ranges {
            let inv_end = inv_start.saturating_add(inv_len);
            if inv_end <= cursor {
                continue;
            }
            if inv_start >= end_total {
                break;
            }
            if inv_start > cursor {
                let gap_end = inv_start.min(end_total);
                ranges.push(((cursor - start_total) as usize, (gap_end - cursor) as usize));
            }
            cursor = cursor.max(inv_end.min(end_total));
        }
        if cursor < end_total {
            ranges.push((
                (cursor - start_total) as usize,
                (end_total - cursor) as usize,
            ));
        }

        ranges
    }

    /// Number of unflushed bytes currently in the window.
    pub fn unflushed_bytes(&self) -> u64 {
        let span = self.total_written.saturating_sub(self.total_flushed);
        span - self.invisible_overlap(self.total_flushed, self.total_written)
    }

    /// Absolute position up to which data has been flushed.
    pub fn total_flushed(&self) -> u64 {
        self.total_flushed
    }

    /// Write a specific absolute output range to a writer without advancing
    /// the flushed marker.
    pub fn write_range_to_writer<W: Write + ?Sized>(
        &self,
        start_total: u64,
        len: usize,
        writer: &mut W,
    ) -> std::io::Result<()> {
        if len == 0 {
            return Ok(());
        }

        let dict_size = self.buf.len();
        let end_total = start_total.checked_add(len as u64).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "range overflow")
        })?;
        if end_total > self.total_written {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "requested range [{start_total}, {end_total}) exceeds total written {}",
                    self.total_written
                ),
            ));
        }
        let distance_from_end = self.total_written.checked_sub(start_total).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "requested range start {start_total} exceeds total written {}",
                    self.total_written
                ),
            )
        })?;
        if distance_from_end > dict_size as u64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "requested range start {start_total} is older than window capacity {dict_size}"
                ),
            ));
        }

        let distance = distance_from_end as usize;
        let mut idx = if distance <= self.pos {
            self.pos - distance
        } else {
            dict_size - (distance - self.pos)
        };

        let mut remaining = len;
        while remaining > 0 {
            let contig = (dict_size - idx).min(remaining);
            self.buf.write_range_to_writer(idx, contig, writer)?;
            idx = (idx + contig) % dict_size;
            remaining -= contig;
        }

        Ok(())
    }

    /// Manually mark data as flushed up to a given total position.
    /// Used when data is extracted via `copy_output` and written externally.
    pub fn mark_flushed(&mut self, up_to: u64) {
        self.total_flushed = up_to.min(self.total_written);
        self.gc_invisible();
    }

    /// Reset the window for a new file (non-solid mode).
    pub fn reset(&mut self) {
        self.buf.fill_all(0);
        self.pos = 0;
        self.total_written = 0;
        self.total_flushed = 0;
        self.invisible_ranges.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_new_rejects_zero_dictionary() {
        let Err(err) = Window::try_new(0) else {
            panic!("zero dictionary unexpectedly succeeded");
        };
        assert!(matches!(
            err,
            RarError::DictionaryTooLarge { size: 0, max: 0 }
        ));
    }

    #[test]
    fn try_new_reports_capacity_overflow_as_resource_limit() {
        let Err(err) = Window::try_new(usize::MAX) else {
            panic!("usize::MAX dictionary unexpectedly succeeded");
        };
        assert!(matches!(err, RarError::ResourceLimit { .. }));
    }

    #[test]
    fn fragmented_window_matches_contiguous_copy_and_flush() {
        let mut contiguous = Window::new(16);
        let mut fragmented = Window::new_fragmented_for_test(16, 5);

        for window in [&mut contiguous, &mut fragmented] {
            window.put_bytes(b"ABCDEF");
            window.copy(4, 5).unwrap();
            window.copy_with_visible_len(3, 4, 2).unwrap();
            window.put_bytes(b"Z");
        }

        assert_eq!(contiguous.total_written(), fragmented.total_written());
        assert_eq!(contiguous.position(), fragmented.position());
        for distance in 1..=16 {
            assert_eq!(
                contiguous.get_byte(distance),
                fragmented.get_byte(distance),
                "distance {distance}"
            );
        }

        let mut contiguous_out = Vec::new();
        let mut fragmented_out = Vec::new();
        assert_eq!(
            contiguous.flush_to_writer(&mut contiguous_out).unwrap(),
            fragmented.flush_to_writer(&mut fragmented_out).unwrap()
        );
        assert_eq!(contiguous_out, fragmented_out);
    }

    #[test]
    fn fragmented_window_writes_ranges_across_storage_blocks() {
        let mut window = Window::new_fragmented_for_test(12, 5);
        window.put_bytes(b"ABCDEFGHIJKL");

        let mut out = Vec::new();
        window.write_range_to_writer(3, 7, &mut out).unwrap();

        assert_eq!(out, b"DEFGHIJ");
        assert_eq!(window.try_copy_output(2, 8).unwrap(), b"CDEFGHIJ");
    }

    #[test]
    fn fragmented_window_enforces_block_count_limit() {
        let Err(err) = WindowStorage::fragmented_for_test(FRAGMENTED_MAX_BLOCKS + 1, 1) else {
            panic!("fragmented window unexpectedly exceeded block count limit");
        };

        assert!(matches!(err, RarError::ResourceLimit { .. }));
    }

    #[test]
    fn fragmented_window_min_block_size_is_not_a_128mib_cap() {
        let len = FRAGMENTED_BLOCK_SIZE * FRAGMENTED_MAX_BLOCKS + FRAGMENTED_MAX_BLOCKS;

        let min_first =
            FragmentedWindow::fragmented_min_block_size(len, 0, FRAGMENTED_BLOCK_SIZE).unwrap();

        assert_eq!(min_first, len / FRAGMENTED_MAX_BLOCKS);
        assert!(min_first > FRAGMENTED_BLOCK_SIZE);
    }

    #[test]
    fn fragmented_window_reset_zeroes_all_storage_blocks() {
        let mut window = Window::new_fragmented_for_test(10, 3);
        window.put_bytes(b"ABCDEFGHIJ");
        window.reset();

        window.copy(5, 5).unwrap();

        assert_eq!(window.copy_output(0, 5), b"\0\0\0\0\0");
    }

    #[test]
    fn test_put_byte() {
        let mut w = Window::new(16);
        w.put_byte(0xAA);
        w.put_byte(0xBB);
        assert_eq!(w.total_written(), 2);
        assert_eq!(w.position(), 2);
    }

    #[test]
    fn test_get_byte() {
        let mut w = Window::new(16);
        w.put_byte(0x01);
        w.put_byte(0x02);
        w.put_byte(0x03);
        // distance=1 -> last byte = 0x03
        assert_eq!(w.get_byte(1), 0x03);
        // distance=2 -> 0x02
        assert_eq!(w.get_byte(2), 0x02);
        // distance=3 -> 0x01
        assert_eq!(w.get_byte(3), 0x01);
    }

    #[test]
    fn test_copy_non_overlapping() {
        let mut w = Window::new(256);
        // Write "ABCD"
        w.put_byte(b'A');
        w.put_byte(b'B');
        w.put_byte(b'C');
        w.put_byte(b'D');
        // Copy from distance=4 (start of "ABCD"), length=4
        w.copy(4, 4).unwrap();
        assert_eq!(w.total_written(), 8);
        // Should have written "ABCDABCD"
        let output = w.copy_output(0, 8);
        assert_eq!(&output, b"ABCDABCD");
    }

    #[test]
    fn test_copy_overlapping() {
        let mut w = Window::new(256);
        // Write single byte
        w.put_byte(b'X');
        // Copy from distance=1, length=5 => repeat 'X' 5 times
        w.copy(1, 5).unwrap();
        assert_eq!(w.total_written(), 6);
        let output = w.copy_output(0, 6);
        assert_eq!(&output, b"XXXXXX");
    }

    #[test]
    fn test_copy_pattern_repeat() {
        let mut w = Window::new(256);
        // Write "AB"
        w.put_byte(b'A');
        w.put_byte(b'B');
        // Copy distance=2, length=6 => "ABABAB"
        w.copy(2, 6).unwrap();
        let output = w.copy_output(0, 8);
        assert_eq!(&output, b"ABABABAB");
    }

    #[test]
    fn test_wrap_around() {
        let mut w = Window::new(4);
        // Write 5 bytes, should wrap around
        w.put_byte(b'A');
        w.put_byte(b'B');
        w.put_byte(b'C');
        w.put_byte(b'D');
        w.put_byte(b'E'); // wraps, overwrites 'A'
        assert_eq!(w.position(), 1);
        assert_eq!(w.total_written(), 5);
        // Last byte (distance=1) should be 'E'
        assert_eq!(w.get_byte(1), b'E');
        // 4 back from current should be 'B' (D was at pos 3, C at 2, B at 1)
        assert_eq!(w.get_byte(4), b'B');
    }

    #[test]
    fn test_copy_across_wrap() {
        let mut w = Window::new(8);
        // Fill to near wrap point
        for b in b"ABCDEF" {
            w.put_byte(*b);
        }
        // pos is now 6. Copy distance=4, length=4 => copies "CDEF"
        // This will wrap around the buffer
        w.copy(4, 4).unwrap();
        assert_eq!(w.total_written(), 10);
        let output = w.copy_output(6, 4);
        assert_eq!(&output, b"CDEF");
    }

    #[test]
    fn test_copy_output() {
        let mut w = Window::new(256);
        for b in b"Hello, world!" {
            w.put_byte(*b);
        }
        let output = w.copy_output(0, 13);
        assert_eq!(&output, b"Hello, world!");
        let partial = w.copy_output(7, 6);
        assert_eq!(&partial, b"world!");
    }

    #[test]
    fn test_try_copy_output_rejects_future_start() {
        let mut w = Window::new(16);
        for b in b"abc" {
            w.put_byte(*b);
        }

        assert!(matches!(
            w.try_copy_output(4, 1),
            Err(RarError::CorruptArchive { .. })
        ));
    }

    #[test]
    fn test_try_copy_output_rejects_evicted_history() {
        let mut w = Window::new(4);
        for b in b"abcdef" {
            w.put_byte(*b);
        }

        assert!(matches!(
            w.try_copy_output(0, 1),
            Err(RarError::CorruptArchive { .. })
        ));
    }

    #[test]
    fn test_reset() {
        let mut w = Window::new(16);
        w.put_byte(0xFF);
        w.put_byte(0xAA);
        w.reset();
        assert_eq!(w.total_written(), 0);
        assert_eq!(w.position(), 0);
    }

    #[test]
    fn test_large_copy() {
        let mut w = Window::new(1024);
        // Write a pattern and copy it many times
        for i in 0..10u8 {
            w.put_byte(i);
        }
        w.copy(10, 100).unwrap(); // repeat 10-byte pattern 10 times
        assert_eq!(w.total_written(), 110);
        let output = w.copy_output(0, 110);
        for (i, &b) in output.iter().enumerate() {
            assert_eq!(b, (i % 10) as u8, "mismatch at position {i}");
        }
    }

    #[test]
    fn test_copy_single_byte_repeat() {
        // distance=1 with large length: classic RLE pattern
        let mut w = Window::new(256);
        w.put_byte(b'Z');
        w.copy(1, 255).unwrap();
        assert_eq!(w.total_written(), 256);
        let output = w.copy_output(0, 256);
        assert!(output.iter().all(|&b| b == b'Z'));
    }

    #[test]
    fn test_distance_one_fill_handles_multiple_wraps() {
        let mut w = Window::new(4);
        w.put_byte(b'Z');
        w.copy(1, 10).unwrap();

        assert_eq!(w.total_written(), 11);
        assert_eq!(w.position(), 3);
        for distance in 1..=4 {
            assert_eq!(w.get_byte(distance), b'Z');
        }
    }

    #[test]
    fn test_copy_output_partial() {
        let mut w = Window::new(256);
        for b in b"0123456789" {
            w.put_byte(*b);
        }
        // Read just the middle portion
        let output = w.copy_output(3, 4);
        assert_eq!(&output, b"3456");
    }

    #[test]
    fn test_multiple_wraps() {
        let mut w = Window::new(4);
        // Write 12 bytes through a 4-byte window (3 full wraps)
        for i in 0..12u8 {
            w.put_byte(i);
        }
        assert_eq!(w.total_written(), 12);
        assert_eq!(w.position(), 0); // 12 % 4 = 0
        // Last 4 bytes should be 8, 9, 10, 11
        assert_eq!(w.get_byte(1), 11);
        assert_eq!(w.get_byte(2), 10);
        assert_eq!(w.get_byte(3), 9);
        assert_eq!(w.get_byte(4), 8);
    }

    #[test]
    fn test_generic_zero_distance_preserves_zeroed_first_window_like_rar_behavior() {
        let mut w = Window::new(8);
        w.put_bytes(b"ABCD");

        w.copy(0, 2).unwrap();

        assert_eq!(w.total_written(), 6);
        assert_eq!(w.copy_output(4, 2), b"\0\0");
        assert_eq!(w.get_byte(1), 0);
        assert_eq!(w.get_byte(2), 0);
    }

    #[test]
    fn test_generic_zero_distance_preserves_wrapped_window_bytes_like_rar_behavior() {
        let mut w = Window::new(4);
        w.put_bytes(b"ABCD");
        w.put_byte(b'E');

        w.copy(0, 2).unwrap();

        assert_eq!(w.total_written(), 7);
        assert_eq!(w.copy_output(5, 2), b"BC");
        assert_eq!(w.get_byte(1), b'C');
        assert_eq!(w.get_byte(2), b'B');
    }

    #[test]
    fn test_rar15_zero_distance_still_zero_fills() {
        let mut w = Window::new(4);
        w.put_bytes(b"ABCD");
        w.put_byte(b'E');

        w.copy_rar15_with_visible_len(0, 2, 2).unwrap();

        assert_eq!(w.total_written(), 7);
        assert_eq!(w.copy_output(5, 2), b"\0\0");
        assert_eq!(w.get_byte(1), 0);
        assert_eq!(w.get_byte(2), 0);
    }

    #[test]
    fn test_window_valid_distance() {
        let mut w = Window::new(256);
        w.put_byte(b'A');

        assert!(w.copy(1, 1).is_ok());
    }

    #[test]
    fn test_pre_window_distance_zero_fills_like_rar_behavior() {
        let mut w = Window::new(8);
        w.put_byte(b'A');

        w.copy(4, 3).unwrap();

        assert_eq!(w.total_written(), 4);
        assert_eq!(w.get_byte(1), 0);
        assert_eq!(w.get_byte(2), 0);
        assert_eq!(w.get_byte(3), 0);
        assert_eq!(w.get_byte(4), b'A');
    }

    #[test]
    fn test_oversize_distance_zero_fills_like_rar_behavior() {
        let mut w = Window::new(8);
        w.put_bytes(b"ABCD");

        w.copy(9, 2).unwrap();

        assert_eq!(w.total_written(), 6);
        assert_eq!(w.get_byte(1), 0);
        assert_eq!(w.get_byte(2), 0);
    }

    #[test]
    fn full_match_can_advance_dictionary_past_visible_output() {
        let mut w = Window::new(16);
        w.put_bytes(b"ABCD");
        w.mark_flushed(4);

        w.copy_with_visible_len(4, 4, 2).unwrap();

        assert_eq!(w.total_written(), 8);
        assert_eq!(w.copy_output(4, 2), b"AB");
        assert_eq!(w.copy_output(6, 2), b"CD");

        let mut emitted = Vec::new();
        assert_eq!(w.flush_to_writer(&mut emitted).unwrap(), 2);
        assert_eq!(emitted, b"AB");
        assert_eq!(w.total_flushed(), w.total_written());
    }

    #[test]
    fn visible_ranges_skip_hidden_gap_before_later_output() {
        let mut w = Window::new(16);
        w.put_bytes(b"ABCD");
        w.mark_flushed(4);
        w.copy_with_visible_len(4, 4, 2).unwrap();
        w.put_bytes(b"Z");

        let mut emitted = Vec::new();
        assert_eq!(w.flush_to_writer(&mut emitted).unwrap(), 3);
        assert_eq!(emitted, b"ABZ");
        assert_eq!(w.total_flushed(), w.total_written());
    }

    #[test]
    fn visible_subranges_reports_only_visible_parts_inside_range() {
        let mut w = Window::new(16);
        w.put_bytes(b"ABCD");
        w.mark_flushed(4);
        w.copy_with_visible_len(4, 4, 2).unwrap();
        w.put_bytes(b"Z");

        assert_eq!(w.visible_subranges(4, 5), vec![(0, 2), (4, 1)]);
        assert_eq!(w.visible_subranges(6, 3), vec![(2, 1)]);
        assert!(w.visible_subranges(6, 2).is_empty());
    }

    #[test]
    fn flush_visible_until_stops_before_hidden_gap_boundary() {
        let mut w = Window::new(16);
        w.put_bytes(b"ABCD");
        w.mark_flushed(4);
        w.copy_with_visible_len(4, 4, 2).unwrap();
        w.put_bytes(b"Z");

        let mut emitted = Vec::new();
        assert_eq!(w.flush_visible_until(6, &mut emitted).unwrap(), 2);
        assert_eq!(emitted, b"AB");
        assert_eq!(w.total_flushed(), 6);

        let mut tail = Vec::new();
        assert_eq!(w.flush_to_writer(&mut tail).unwrap(), 1);
        assert_eq!(tail, b"Z");
        assert_eq!(w.total_flushed(), w.total_written());
    }

    #[test]
    fn test_distance_wraps_after_first_window_done() {
        let mut w = Window::new(4);
        w.put_bytes(b"ABCD");
        w.put_byte(b'E');

        w.copy(3, 2).unwrap();

        assert_eq!(w.get_byte(1), b'D');
        assert_eq!(w.get_byte(2), b'C');
    }

    #[test]
    fn overlong_match_candidate_uses_wrap_safe_copy_path() {
        let mut w = Window::new(8192);
        w.put_bytes(b"AB");

        w.copy(2, MAX_INC_LZ_MATCH + 2048).unwrap();

        assert_eq!(w.total_written(), (MAX_INC_LZ_MATCH + 2050) as u64);
        assert_eq!(w.position(), (MAX_INC_LZ_MATCH + 2050) % 8192);
    }

    #[test]
    fn test_copy_wraps_source_and_dest() {
        // Window of 8, write 6 bytes, then copy distance=6, length=8
        // This wraps both source and destination
        let mut w = Window::new(8);
        for b in b"ABCDEF" {
            w.put_byte(*b);
        }
        // pos = 6, copy from pos 0 (dist=6), length=8
        // Should produce: ABCDEF ABCDEFAB (wrapping)
        w.copy(6, 8).unwrap();
        assert_eq!(w.total_written(), 14);
        // Last 8 bytes written
        let output = w.copy_output(6, 8);
        assert_eq!(&output, b"ABCDEFAB");
    }

    #[test]
    fn test_flush_to_writer_rejects_overfull_unflushed_region() {
        let mut w = Window::new(8);
        for i in 0..16u8 {
            w.put_byte(i);
        }

        let mut out = Vec::new();
        let err = w.flush_to_writer(&mut out).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }
}
