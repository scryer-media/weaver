//! PPMd variant H decompression for RAR5.
//!
//! RAR5 uses PPMd (Prediction by Partial Matching, variant H by Dmitry Shkarin)
//! as an alternative to LZ compression. Within a RAR5 compressed stream, blocks
//! can alternate between LZ and PPMd encoding, signaled by the first bit of
//! each block (0 = LZ, 1 = PPMd).
//!
//! PPMd block header (read from the bitstream after the block type bit):
//! - `reset_flag` (1 bit): if 1, reset the PPMd model
//! - If reset_flag == 1:
//!   - `max_order` (7 bits): model order
//!   - `alloc_size_mb` (8 bits): sub-allocator size in megabytes
//!
//! Reference: 7-zip Ppmd7.c (public domain), Shkarin's original PPMd (public domain).

pub mod alloc;
pub mod model;
pub mod range;
pub mod see;

use crate::error::{RarError, RarResult};
use model::Model;
use range::RangeDecoder;

/// PPMd decoder state, persisted across blocks within a file.
pub struct PpmdDecoder {
    /// The PPMd model, or None if not yet initialized.
    model: Option<Model>,
}

impl PpmdDecoder {
    /// Create a new PPMd decoder (uninitialized — will init on first block).
    pub fn new() -> Self {
        Self { model: None }
    }

    /// Decode a PPMd block from the compressed stream.
    ///
    /// `data` is the remaining compressed data starting at the PPMd block header.
    /// `unpacked_remaining` is how many decompressed bytes are still needed.
    /// `output` receives decompressed bytes.
    ///
    /// Returns the number of compressed bytes consumed.
    pub fn decode_block(
        &mut self,
        data: &[u8],
        unpacked_remaining: u64,
        output: &mut Vec<u8>,
    ) -> RarResult<usize> {
        if data.is_empty() {
            return Ok(0);
        }

        // First byte contains flags.
        let flags = data[0];
        let reset = (flags & 0x80) != 0;
        let mut pos = 1;

        if reset {
            // Read model parameters.
            let max_order = (flags & 0x7F) as usize;
            if pos >= data.len() {
                return Err(RarError::CorruptArchive {
                    detail: "PPMd block truncated after flags".into(),
                });
            }
            let alloc_mb = data[pos] as usize;
            pos += 1;

            let alloc_size = if alloc_mb == 0 { 1 } else { alloc_mb } * 1024 * 1024;
            let order = if max_order == 0 { 2 } else { max_order.max(2) };

            self.model = Some(Model::new(order, alloc_size));
        }

        let model = self.model.as_mut().ok_or_else(|| RarError::CorruptArchive {
            detail: "PPMd block without model initialization".into(),
        })?;

        // Initialize range decoder from remaining data.
        let rc_data = &data[pos..];
        let mut rc = RangeDecoder::new(rc_data)?;

        // Decode symbols until we reach unpacked_remaining or end of data.
        let target = unpacked_remaining as usize;
        let start_len = output.len();

        while output.len() - start_len < target {
            match model.decode_symbol(&mut rc)? {
                Some(sym) => output.push(sym),
                None => break, // end of block
            }
        }

        Ok(pos + rc.position())
    }

    /// Reset the decoder for a new file (non-solid mode).
    pub fn reset(&mut self) {
        self.model = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ppmd_decoder_creation() {
        let decoder = PpmdDecoder::new();
        assert!(decoder.model.is_none());
    }

    #[test]
    fn test_ppmd_block_without_init() {
        let mut decoder = PpmdDecoder::new();
        // No reset flag, no model — should error.
        let data = [0x00, 0x00, 0x00, 0x00, 0x00];
        let mut output = Vec::new();
        let result = decoder.decode_block(&data, 10, &mut output);
        assert!(result.is_err());
    }

    #[test]
    fn test_ppmd_block_with_init() {
        let mut decoder = PpmdDecoder::new();
        // Reset flag set, order=6, alloc=1MB.
        // flags = 0x80 | 6 = 0x86
        // Then alloc_mb = 1
        // Then 4 bytes for range decoder init.
        let mut data = vec![0x86, 0x01];
        // Range decoder needs at least 4 bytes of code.
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
        // More data for the decoder to consume.
        data.extend_from_slice(&[0x00; 100]);

        let mut output = Vec::new();
        // Decode up to 5 bytes — this exercises the model init + decode path.
        let result = decoder.decode_block(&data, 5, &mut output);
        assert!(result.is_ok());
        assert_eq!(output.len(), 5);
    }
}
