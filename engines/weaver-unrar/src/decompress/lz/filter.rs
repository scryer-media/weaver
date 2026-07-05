//! RAR5 post-decompression filters.
//!
//! RAR5 uses fixed filter types (no VM) applied to specific byte ranges
//! of the decompressed output. Filters reverse transformations that were applied
//! during compression to improve redundancy for specific data patterns.

use memchr::{memchr, memchr2};

/// RAR5 filter types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterType {
    Delta,
    E8,
    E8E9,
    Arm,
    /// Unknown filter types consume the covered block without writing
    /// transformed bytes and let the final CRC report corruption.
    Unsupported(u8),
}

impl FilterType {
    /// Parse a filter type from its 3-bit code.
    pub fn from_code(code: u8) -> Self {
        match code {
            0 => FilterType::Delta,
            1 => FilterType::E8,
            2 => FilterType::E8E9,
            3 => FilterType::Arm,
            _ => FilterType::Unsupported(code),
        }
    }

    pub fn emits_output(self) -> bool {
        !matches!(self, FilterType::Unsupported(_))
    }
}

/// A filter that is pending application to a range of output bytes.
#[derive(Debug, Clone)]
pub struct PendingFilter {
    /// The type of filter to apply.
    pub filter_type: FilterType,
    /// Absolute byte offset in the output where this filter's block starts.
    pub block_start: u64,
    /// Length of the filter block in bytes.
    pub block_length: usize,
    /// Number of channels for DELTA filter (1-32).
    pub channels: u8,
}

/// Add a pending RAR5 filter with defensive queue behavior.
///
/// Weaver cannot always flush at descriptor-read time, so this helper keeps the
/// queue bounded by resetting it before accepting the next malformed-stream
/// filter.
pub(crate) fn push_pending_filter(
    filters: &mut Vec<PendingFilter>,
    filter: PendingFilter,
    max_filters: usize,
) {
    if filters.len() >= max_filters {
        filters.clear();
    }
    filters.push(filter);
}

/// Apply the DELTA filter in-place.
///
/// RAR5 stores bytes from the same channel in contiguous groups, so decoding
/// must both reverse the subtractive delta and de-interleave the channels.
pub fn apply_delta(data: &mut [u8], channels: u8) {
    let ch = channels as usize;
    if ch == 0 {
        return;
    }

    let src = data.to_vec();
    let mut src_pos = 0usize;
    for channel in 0..ch {
        let mut prev = 0u8;
        let mut dest = channel;
        while dest < data.len() && src_pos < src.len() {
            prev = prev.wrapping_sub(src[src_pos]);
            data[dest] = prev;
            src_pos += 1;
            dest += ch;
        }
    }
}

/// Apply the E8 filter in-place (x86 CALL instruction fixup).
///
/// Scans for 0xE8 bytes and converts absolute addresses back to relative.
/// `file_offset` is the absolute position in the output stream where this
/// block starts.
pub fn apply_e8(data: &mut [u8], file_offset: u64) {
    apply_e8e9_inner(data, X86FilterOffsetMode::Rar5 { file_offset }, false);
}

/// Apply the E8E9 filter in-place (x86 CALL + JMP instruction fixup).
///
/// Same as E8 but also processes 0xE9 (JMP) instructions.
pub fn apply_e8e9(data: &mut [u8], file_offset: u64) {
    apply_e8e9_inner(data, X86FilterOffsetMode::Rar5 { file_offset }, true);
}

pub(crate) fn apply_rar4_e8(data: &mut [u8], file_offset: u32) {
    apply_e8e9_inner(data, X86FilterOffsetMode::Rar4 { file_offset }, false);
}

pub(crate) fn apply_rar4_e8e9(data: &mut [u8], file_offset: u32) {
    apply_e8e9_inner(data, X86FilterOffsetMode::Rar4 { file_offset }, true);
}

#[derive(Clone, Copy)]
enum X86FilterOffsetMode {
    Rar5 { file_offset: u64 },
    Rar4 { file_offset: u32 },
}

impl X86FilterOffsetMode {
    #[inline]
    fn address_offset(self, address_start: usize) -> u32 {
        const FILE_SIZE: u32 = 0x01_00_00_00;

        match self {
            Self::Rar5 { file_offset } => (address_start as u64 + file_offset) as u32 % FILE_SIZE,
            Self::Rar4 { file_offset } => file_offset.wrapping_add(address_start as u32),
        }
    }
}

/// Shared implementation for E8 and E8E9 filters.
///
/// Uses `memchr`'s portable safe API for candidate search. Any SIMD dispatch
/// happens inside `memchr` behind its runtime CPU feature checks and fallback.
fn apply_e8e9_inner(data: &mut [u8], offset_mode: X86FilterOffsetMode, include_e9: bool) {
    const FILE_SIZE: u32 = 0x01_00_00_00;
    if data.len() < 5 {
        return;
    }

    let last_start = data.len() - 4;
    let mut i = 0usize;
    while i < last_start {
        let rel = if include_e9 {
            memchr2(0xE8, 0xE9, &data[i..last_start])
        } else {
            memchr(0xE8, &data[i..last_start])
        };
        let Some(found) = rel else {
            break;
        };

        i += found;

        let addr = u32::from_le_bytes([data[i + 1], data[i + 2], data[i + 3], data[i + 4]]);
        let offset = offset_mode.address_offset(i + 1);

        if addr & 0x8000_0000 != 0 {
            if addr.wrapping_add(offset) & 0x8000_0000 == 0 {
                let bytes = addr.wrapping_add(FILE_SIZE).to_le_bytes();
                data[i + 1..i + 5].copy_from_slice(&bytes);
            }
        } else if addr.wrapping_sub(FILE_SIZE) & 0x8000_0000 != 0 {
            let bytes = addr.wrapping_sub(offset).to_le_bytes();
            data[i + 1..i + 5].copy_from_slice(&bytes);
        }

        i += 5;
    }
}

/// Apply the ARM filter in-place (ARM BL instruction fixup).
///
/// Scans for ARM BL instructions at 4-byte aligned positions and converts
/// absolute branch targets back to relative offsets.
pub fn apply_arm(data: &mut [u8], file_offset: u64) {
    if data.len() < 4 {
        return;
    }

    let mut i = 0usize;
    while i + 3 < data.len() {
        if data[i + 3] == 0xEB {
            // Extract 24-bit signed offset from bytes [i..i+3].
            let b0 = data[i] as u32;
            let b1 = data[i + 1] as u32;
            let b2 = data[i + 2] as u32;
            let offset = b0 | (b1 << 8) | (b2 << 16);

            let cur_pos = ((file_offset as u32) + (i as u32)) / 4;
            let relative = offset.wrapping_sub(cur_pos) & 0x00FF_FFFF;

            data[i] = relative as u8;
            data[i + 1] = (relative >> 8) as u8;
            data[i + 2] = (relative >> 16) as u8;
        }
        i += 4;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_delta_single_channel() {
        let mut data = [255u8, 254, 253, 252];
        apply_delta(&mut data, 1);
        // RAR delta uses subtractive accumulation.
        // Encoded bytes [255, 254, 253, 252] decode to [1, 3, 6, 10].
        assert_eq!(data, [1, 3, 6, 10]);
    }

    #[test]
    fn test_apply_delta_multi_channel() {
        // RAR groups each channel contiguously in the encoded stream.
        // Channel 0 encoded deltas [246, 251, 249] -> [10, 15, 22]
        // Channel 1 encoded deltas [236, 253, 255] -> [20, 23, 24]
        let mut data = [246u8, 251, 249, 236, 253, 255];
        apply_delta(&mut data, 2);
        // Interleaved: [10, 20, 15, 23, 22, 24]
        assert_eq!(data, [10, 20, 15, 23, 22, 24]);
    }

    #[test]
    fn test_apply_e8_basic() {
        // Place a 0xE8 at position 0 with an absolute address that should
        // be converted back to relative.
        // RAR uses the current written offset of the 4-byte address field,
        // so file_offset = 0 and position = 0 yields an address offset of 1.
        // Stored absolute addr = 100 (which is >= 0 and < file_size)
        // Expected relative = 100 - 1 = 99
        let file_size = 256; // block_length
        let mut data = vec![0u8; file_size];
        data[0] = 0xE8;
        let addr: i32 = 100;
        let bytes = addr.to_le_bytes();
        data[1] = bytes[0];
        data[2] = bytes[1];
        data[3] = bytes[2];
        data[4] = bytes[3];

        apply_e8(&mut data, 0);

        let result = i32::from_le_bytes([data[1], data[2], data[3], data[4]]);
        assert_eq!(result, 99); // 100 - 1
    }

    #[test]
    fn test_apply_e8e9_processes_both() {
        let file_size = 256;
        let mut data = vec![0u8; file_size];

        // E8 at position 0: absolute addr = 50, address offset = 1, relative = 49
        data[0] = 0xE8;
        let addr1: i32 = 50;
        let b1 = addr1.to_le_bytes();
        data[1] = b1[0];
        data[2] = b1[1];
        data[3] = b1[2];
        data[4] = b1[3];

        // E9 at position 10: absolute addr = 80, address offset = 11, relative = 69
        data[10] = 0xE9;
        let addr2: i32 = 80;
        let b2 = addr2.to_le_bytes();
        data[11] = b2[0];
        data[12] = b2[1];
        data[13] = b2[2];
        data[14] = b2[3];

        apply_e8e9(&mut data, 0);

        let r1 = i32::from_le_bytes([data[1], data[2], data[3], data[4]]);
        assert_eq!(r1, 49); // 50 - 1

        let r2 = i32::from_le_bytes([data[11], data[12], data[13], data[14]]);
        assert_eq!(r2, 69); // 80 - 11
    }

    fn apply_rar4_e8e9_scalar(data: &mut [u8], file_offset: u32, include_e9: bool) {
        const FILE_SIZE: u32 = 0x01_00_00_00;

        let cmp_byte2 = if include_e9 { 0xE9 } else { 0xE8 };
        let data_size = data.len();
        let mut cur_pos = 0usize;
        while cur_pos + 4 < data_size {
            let cur_byte = data[cur_pos];
            cur_pos += 1;
            if cur_byte == 0xE8 || cur_byte == cmp_byte2 {
                let offset = file_offset.wrapping_add(cur_pos as u32);
                let addr = u32::from_le_bytes([
                    data[cur_pos],
                    data[cur_pos + 1],
                    data[cur_pos + 2],
                    data[cur_pos + 3],
                ]);
                if (addr & 0x8000_0000) != 0 {
                    if ((addr.wrapping_add(offset)) & 0x8000_0000) == 0 {
                        let bytes = addr.wrapping_add(FILE_SIZE).to_le_bytes();
                        data[cur_pos..cur_pos + 4].copy_from_slice(&bytes);
                    }
                } else if ((addr.wrapping_sub(FILE_SIZE)) & 0x8000_0000) != 0 {
                    let bytes = addr.wrapping_sub(offset).to_le_bytes();
                    data[cur_pos..cur_pos + 4].copy_from_slice(&bytes);
                }
                cur_pos += 4;
            }
        }
    }

    fn rar4_dense_candidate_data() -> Vec<u8> {
        let mut data = vec![0x11; 96];
        for (index, pos) in (0..80).step_by(5).enumerate() {
            data[pos] = if index % 2 == 0 { 0xE8 } else { 0xE9 };
            let addr = match index % 4 {
                0 => 0x0000_1234u32,
                1 => 0x00FF_FF00u32,
                2 => 0x8000_0010u32,
                _ => 0xFFFF_FF00u32,
            };
            data[pos + 1..pos + 5].copy_from_slice(&addr.to_le_bytes());
        }
        data
    }

    fn rar4_sparse_candidate_data() -> Vec<u8> {
        let mut data = vec![0x31; 257];
        for (pos, opcode, addr) in [
            (3usize, 0xE8u8, 0x0000_2000u32),
            (64, 0xE9, 0x00FF_FFF0),
            (129, 0xE8, 0xFFFF_FFF0),
            (240, 0xE9, 0x0200_0000),
        ] {
            data[pos] = opcode;
            data[pos + 1..pos + 5].copy_from_slice(&addr.to_le_bytes());
        }
        data
    }

    fn assert_rar4_filter_matches_scalar(mut data: Vec<u8>, file_offset: u32, include_e9: bool) {
        let mut expected = data.clone();
        apply_rar4_e8e9_scalar(&mut expected, file_offset, include_e9);

        if include_e9 {
            apply_rar4_e8e9(&mut data, file_offset);
        } else {
            apply_rar4_e8(&mut data, file_offset);
        }

        assert_eq!(
            data, expected,
            "include_e9={include_e9} file_offset={file_offset:#010x}"
        );
    }

    #[test]
    fn test_rar4_e8_filter_matches_scalar_reference() {
        for file_offset in [0, 0x00FF_FFFC, 0x0100_0000, 0xFFFF_FFF0] {
            assert_rar4_filter_matches_scalar(rar4_dense_candidate_data(), file_offset, false);
            assert_rar4_filter_matches_scalar(rar4_sparse_candidate_data(), file_offset, false);
        }
    }

    #[test]
    fn test_rar4_e8e9_filter_matches_scalar_reference() {
        for file_offset in [0, 0x00FF_FFFC, 0x0100_0000, 0xFFFF_FFF0] {
            assert_rar4_filter_matches_scalar(rar4_dense_candidate_data(), file_offset, true);
            assert_rar4_filter_matches_scalar(rar4_sparse_candidate_data(), file_offset, true);
        }
    }

    #[test]
    fn test_rar4_short_and_no_candidate_filters_match_scalar_reference() {
        for data in [
            Vec::new(),
            vec![0xE8],
            vec![0xE8, 0x01, 0x00],
            vec![0xE9, 0x01, 0x00, 0x00],
            vec![0x22; 64],
        ] {
            for file_offset in [0, 0x0100_0000, 0xFFFF_FFF0] {
                assert_rar4_filter_matches_scalar(data.clone(), file_offset, false);
                assert_rar4_filter_matches_scalar(data.clone(), file_offset, true);
            }
        }
    }

    #[test]
    fn test_apply_arm_basic() {
        // ARM BL at position 0, file_offset = 0
        // cur_pos = (0 + 0) / 4 = 0
        // Stored absolute offset = 0x00_00_10 (16 in instruction units)
        // Relative = 16 - 0 = 16
        let mut data = [0x10u8, 0x00, 0x00, 0xEB];
        apply_arm(&mut data, 0);
        // relative = 0x10 - 0 = 0x10
        assert_eq!(data, [0x10, 0x00, 0x00, 0xEB]);

        // Now with file_offset = 16 (4 instructions in)
        // cur_pos = 16 / 4 = 4
        // relative = 0x10 - 4 = 0x0C
        let mut data2 = [0x10u8, 0x00, 0x00, 0xEB];
        apply_arm(&mut data2, 16);
        assert_eq!(data2, [0x0C, 0x00, 0x00, 0xEB]);
    }

    #[test]
    fn test_filter_no_match() {
        // Buffer with no E8/E9 bytes should pass through unchanged.
        let original = [0x01u8, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let mut data = original;
        apply_e8(&mut data, 0);
        assert_eq!(data, original);

        let mut data2 = original;
        apply_e8e9(&mut data2, 0);
        assert_eq!(data2, original);
    }

    #[test]
    fn test_filter_type_from_code() {
        assert_eq!(FilterType::from_code(0), FilterType::Delta);
        assert_eq!(FilterType::from_code(1), FilterType::E8);
        assert_eq!(FilterType::from_code(2), FilterType::E8E9);
        assert_eq!(FilterType::from_code(3), FilterType::Arm);
        assert_eq!(FilterType::from_code(4), FilterType::Unsupported(4));
        assert_eq!(FilterType::from_code(7), FilterType::Unsupported(7));
        assert!(!FilterType::Unsupported(7).emits_output());
    }

    #[test]
    fn push_pending_filter_resets_full_queue_like_rar_behavior() {
        let mut filters = vec![
            PendingFilter {
                filter_type: FilterType::E8,
                block_start: 0,
                block_length: 5,
                channels: 0,
            },
            PendingFilter {
                filter_type: FilterType::Arm,
                block_start: 8,
                block_length: 4,
                channels: 0,
            },
        ];

        push_pending_filter(
            &mut filters,
            PendingFilter {
                filter_type: FilterType::Delta,
                block_start: 12,
                block_length: 3,
                channels: 1,
            },
            2,
        );

        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].filter_type, FilterType::Delta);
        assert_eq!(filters[0].block_start, 12);
    }

    #[test]
    fn test_delta_zero_channels_noop() {
        let mut data = [1u8, 2, 3];
        apply_delta(&mut data, 0);
        assert_eq!(data, [1, 2, 3]);
    }

    #[test]
    fn test_e8_short_buffer() {
        // Buffer too short for any E8 conversion
        let mut data = [0xE8u8, 0x01, 0x00];
        let original = data;
        apply_e8(&mut data, 0);
        assert_eq!(data, original);
    }

    #[test]
    fn test_arm_short_buffer() {
        let mut data = [0x10u8, 0x00, 0xEB];
        let original = data;
        apply_arm(&mut data, 0);
        assert_eq!(data, original);
    }
}
