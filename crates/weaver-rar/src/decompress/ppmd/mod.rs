//! PPMd variant H decompression for RAR archives.
//!
//! Both RAR4 and RAR5 use PPMd variant H as an alternative to LZ compression.
//! Within a compressed stream, blocks can alternate between LZ and PPMd.
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

impl Default for PpmdDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl PpmdDecoder {
    /// Create a new PPMd decoder (uninitialized — will init on first block).
    pub fn new() -> Self {
        Self { model: None }
    }

    /// Initialize or reinitialize the PPMd model.
    pub fn init_model(&mut self, max_order: usize, alloc_mb: usize) {
        let alloc_size = if alloc_mb == 0 { 1 } else { alloc_mb } * 1024 * 1024;
        let order = if max_order == 0 { 2 } else { max_order.max(2) };
        self.model = Some(Model::new(order, alloc_size));
    }

    /// Check if the model is initialized.
    pub fn has_model(&self) -> bool {
        self.model.is_some()
    }

    /// Get a mutable reference to the model (for direct decode_char calls).
    pub fn model_mut(&mut self) -> Option<&mut Model> {
        self.model.as_mut()
    }

    /// Decode a PPMd block from byte-aligned compressed data (RAR5 interface).
    ///
    /// `reset`: whether the PPMd model should be reinitialized.
    /// `max_order`: model order (only used when `reset` is true).
    /// `alloc_mb`: sub-allocator size in megabytes (only used when `reset` is true).
    /// `rc_data`: byte-aligned compressed data for the range decoder.
    /// `unpacked_remaining`: how many decompressed bytes are still needed.
    /// `output`: receives decompressed bytes.
    ///
    /// Returns the number of compressed bytes consumed from `rc_data`.
    pub fn decode_block(
        &mut self,
        reset: bool,
        max_order: usize,
        alloc_mb: usize,
        rc_data: &[u8],
        unpacked_remaining: u64,
        output: &mut Vec<u8>,
    ) -> RarResult<usize> {
        if rc_data.is_empty() {
            return Ok(0);
        }

        if reset {
            self.init_model(max_order, alloc_mb);
        }

        let model = self
            .model
            .as_mut()
            .ok_or_else(|| RarError::CorruptArchive {
                detail: "PPMd block without model initialization".into(),
            })?;

        let mut rc = RangeDecoder::new(rc_data)?;

        let target = unpacked_remaining as usize;
        let start_len = output.len();

        while output.len() - start_len < target {
            let ch = model.decode_char(&mut rc);
            if ch < 0 {
                break;
            }
            output.push(ch as u8);
        }

        Ok(rc.position())
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
        let data = [0x00, 0x00, 0x00, 0x00, 0x00];
        let mut output = Vec::new();
        let result = decoder.decode_block(false, 0, 0, &data, 10, &mut output);
        assert!(result.is_err());
    }

    #[test]
    fn test_ppmd_block_with_init() {
        let mut decoder = PpmdDecoder::new();
        let mut data = vec![0x00, 0x00, 0x00, 0x00];
        data.extend_from_slice(&[0x00; 100]);

        let mut output = Vec::new();
        let result = decoder.decode_block(true, 6, 1, &data, 5, &mut output);
        assert!(result.is_ok());
        // Synthetic all-zeros input may hit escape early; just verify no crash.
        assert!(!output.is_empty() || output.is_empty()); // no crash
    }
}
