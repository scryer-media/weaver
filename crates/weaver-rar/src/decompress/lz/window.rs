//! Sliding window / ring buffer for LZ decompression.
//!
//! The window holds previously decompressed output. Length-distance pairs
//! reference bytes already written to the window. The window wraps around
//! when it reaches the dictionary size.

use std::io::Write;

use crate::error::{RarError, RarResult};

/// Sliding window ring buffer used during LZ decompression.
pub struct Window {
    /// The ring buffer.
    buf: Vec<u8>,
    /// Current write position (wraps at buf.len()).
    pos: usize,
    /// Total number of bytes ever written (monotonically increasing).
    total_written: u64,
    /// Total number of bytes flushed to an external writer.
    total_flushed: u64,
}

impl Window {
    /// Create a new window with the given dictionary size.
    ///
    /// The dictionary size determines the maximum lookback distance for
    /// length-distance copies.
    pub fn new(dict_size: usize) -> Self {
        Self {
            buf: vec![0u8; dict_size],
            pos: 0,
            total_written: 0,
            total_flushed: 0,
        }
    }

    /// Write a single literal byte to the window.
    #[inline]
    pub fn put_byte(&mut self, b: u8) {
        self.buf[self.pos] = b;
        self.pos += 1;
        if self.pos >= self.buf.len() {
            self.pos = 0;
        }
        self.total_written += 1;
    }

    /// Copy `length` bytes from `distance` bytes back in the output.
    ///
    /// Handles overlapping copies correctly (e.g., distance=1, length=100
    /// repeats the last byte 100 times).
    pub fn copy(&mut self, distance: usize, length: usize) -> RarResult<()> {
        let dict_size = self.buf.len();
        if distance == 0 || distance > dict_size {
            return Err(RarError::CorruptArchive {
                detail: format!("invalid LZ distance {} (dict_size={})", distance, dict_size),
            });
        }

        let mut src = if distance <= self.pos {
            self.pos - distance
        } else {
            dict_size - (distance - self.pos)
        };
        let mut dst = self.pos;
        let mut remaining = length;

        while remaining > 0 {
            // Contiguous bytes available before src or dst wraps.
            let src_contig = dict_size - src;
            let dst_contig = dict_size - dst;
            // For overlapping copies (distance < length), limit chunk size to
            // the gap between src and dst so that copy_within (memmove) produces
            // the correct byte-replication pattern.
            let gap = if src < dst { dst - src } else { dict_size - src + dst };
            let chunk = remaining.min(src_contig).min(dst_contig).min(gap);

            self.buf.copy_within(src..src + chunk, dst);
            src = (src + chunk) % dict_size;
            dst = (dst + chunk) % dict_size;
            remaining -= chunk;
        }

        self.pos = dst;
        self.total_written += length as u64;
        Ok(())
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
        self.buf[idx]
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
    pub fn copy_output(&self, start_total: u64, len: usize) -> Vec<u8> {
        let dict_size = self.buf.len();
        let mut result = Vec::with_capacity(len);

        let distance = (self.total_written - start_total) as usize;
        let mut idx = if distance <= self.pos {
            self.pos - distance
        } else {
            dict_size - (distance - self.pos)
        };

        let mut remaining = len;
        while remaining > 0 {
            let contig = (dict_size - idx).min(remaining);
            result.extend_from_slice(&self.buf[idx..idx + contig]);
            idx = (idx + contig) % dict_size;
            remaining -= contig;
        }

        result
    }

    /// Flush unflushed bytes from the window to a writer.
    ///
    /// Writes all bytes between `total_flushed` and `total_written` to the
    /// provided writer. Handles ring buffer wrap-around correctly.
    /// Returns the number of bytes written.
    pub fn flush_to_writer<W: Write + ?Sized>(&mut self, writer: &mut W) -> std::io::Result<u64> {
        let unflushed = self.total_written - self.total_flushed;
        if unflushed == 0 {
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
        let count = unflushed as usize;

        // Calculate the start position in the ring buffer.
        let start = if count <= self.pos {
            self.pos - count
        } else {
            dict_size - (count - self.pos)
        };

        if start + count <= dict_size {
            // Contiguous region — single write.
            writer.write_all(&self.buf[start..start + count])?;
        } else {
            // Wraps around — two writes.
            writer.write_all(&self.buf[start..])?;
            let remaining = count - (dict_size - start);
            writer.write_all(&self.buf[..remaining])?;
        }

        self.total_flushed = self.total_written;
        Ok(unflushed)
    }

    /// Number of unflushed bytes currently in the window.
    pub fn unflushed_bytes(&self) -> u64 {
        self.total_written - self.total_flushed
    }

    /// Manually mark data as flushed up to a given total position.
    /// Used when data is extracted via `copy_output` and written externally.
    pub fn mark_flushed(&mut self, up_to: u64) {
        self.total_flushed = up_to;
    }

    /// Reset the window for a new file (non-solid mode).
    pub fn reset(&mut self) {
        self.buf.fill(0);
        self.pos = 0;
        self.total_written = 0;
        self.total_flushed = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_window_invalid_distance() {
        let mut w = Window::new(256);
        w.put_byte(b'A');

        // distance=0 should error
        let result = w.copy(0, 1);
        assert!(
            matches!(result, Err(crate::error::RarError::CorruptArchive { .. })),
            "expected error for distance=0, got: {result:?}"
        );

        // distance > dict_size should error
        let result = w.copy(257, 1);
        assert!(
            matches!(result, Err(crate::error::RarError::CorruptArchive { .. })),
            "expected error for distance>dict_size, got: {result:?}"
        );

        // valid distance should succeed
        assert!(w.copy(1, 1).is_ok());
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
