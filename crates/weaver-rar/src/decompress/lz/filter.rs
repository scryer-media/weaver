//! RAR5 post-decompression filters.
//!
//! RAR5 uses four fixed filter types (no VM) applied to specific byte ranges
//! of the decompressed output. Filters reverse transformations that were applied
//! during compression to improve redundancy for specific data patterns.

/// The four RAR5 filter types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterType {
    Delta = 0,
    E8 = 1,
    E8E9 = 2,
    Arm = 3,
}

impl FilterType {
    /// Parse a filter type from its 3-bit code. Returns `None` for unknown types.
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(FilterType::Delta),
            1 => Some(FilterType::E8),
            2 => Some(FilterType::E8E9),
            3 => Some(FilterType::Arm),
            _ => None,
        }
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

/// Apply the DELTA filter in-place.
///
/// Reverses delta encoding: for each channel, accumulates a running sum.
/// Input is interleaved channel data where byte `i` belongs to channel
/// `i % channels`.
pub fn apply_delta(data: &mut [u8], channels: u8) {
    let ch = channels as usize;
    if ch == 0 {
        return;
    }
    for i in ch..data.len() {
        data[i] = data[i].wrapping_add(data[i - ch]);
    }
}

/// Apply the E8 filter in-place (x86 CALL instruction fixup).
///
/// Scans for 0xE8 bytes and converts absolute addresses back to relative.
/// `file_offset` is the absolute position in the output stream where this
/// block starts.
pub fn apply_e8(data: &mut [u8], file_offset: u64) {
    apply_e8e9_inner(data, file_offset, false);
}

/// Apply the E8E9 filter in-place (x86 CALL + JMP instruction fixup).
///
/// Same as E8 but also processes 0xE9 (JMP) instructions.
pub fn apply_e8e9(data: &mut [u8], file_offset: u64) {
    apply_e8e9_inner(data, file_offset, true);
}

/// Shared implementation for E8 and E8E9 filters.
///
/// The file_size for range checking is the block_length (data.len()).
fn apply_e8e9_inner(data: &mut [u8], file_offset: u64, include_e9: bool) {
    let file_size = data.len() as i64;
    if data.len() < 5 {
        return;
    }

    let mut i = 0usize;
    while i < data.len() - 4 {
        let b = data[i];
        if b == 0xE8 || (include_e9 && b == 0xE9) {
            let addr = i32::from_le_bytes([data[i + 1], data[i + 2], data[i + 3], data[i + 4]]);
            let cur_pos = (file_offset as i64) + (i as i64) + 5;

            // RAR5 range check: after subtracting current position, the result
            // must be >= 0 and < file_size to be considered a valid conversion.
            if addr >= 0 && i64::from(addr) < file_size {
                // Stored as absolute, convert back to relative.
                let relative = addr - cur_pos as i32;
                let bytes = relative.to_le_bytes();
                data[i + 1] = bytes[0];
                data[i + 2] = bytes[1];
                data[i + 3] = bytes[2];
                data[i + 4] = bytes[3];
            } else if addr < 0 && addr.wrapping_add(cur_pos as i32) >= 0 {
                // Negative address that wraps into valid range.
                let relative = addr.wrapping_add(cur_pos as i32);
                // Convert: this was stored as `addr = relative + cur_pos`, so
                // the original relative value is actually just `relative` here
                // which equals `addr + cur_pos`. We need to store it as-is since
                // it represents the absolute form.
                let bytes = relative.to_le_bytes();
                data[i + 1] = bytes[0];
                data[i + 2] = bytes[1];
                data[i + 3] = bytes[2];
                data[i + 4] = bytes[3];
            }

            i += 5;
        } else {
            i += 1;
        }
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

            // Convert from absolute back to relative by subtracting
            // the current instruction's position (in units of 4 bytes).
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
        let mut data = [1u8, 2, 3, 4];
        apply_delta(&mut data, 1);
        // Running sum: 1, 1+2=3, 3+3=6, 6+4=10
        assert_eq!(data, [1, 3, 6, 10]);
    }

    #[test]
    fn test_apply_delta_multi_channel() {
        // 2 channels: channel 0 = positions 0,2,4; channel 1 = positions 1,3,5
        let mut data = [10u8, 20, 5, 3, 7, 1];
        apply_delta(&mut data, 2);
        // Channel 0: 10, 10+5=15, 15+7=22
        // Channel 1: 20, 20+3=23, 23+1=24
        // Interleaved: [10, 20, 15, 23, 22, 24]
        assert_eq!(data, [10, 20, 15, 23, 22, 24]);
    }

    #[test]
    fn test_apply_e8_basic() {
        // Place a 0xE8 at position 0 with an absolute address that should
        // be converted back to relative.
        // file_offset = 0, position = 0, so cur_pos = 0 + 0 + 5 = 5
        // Stored absolute addr = 100 (which is >= 0 and < file_size)
        // Expected relative = 100 - 5 = 95
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
        assert_eq!(result, 95); // 100 - 5
    }

    #[test]
    fn test_apply_e8e9_processes_both() {
        let file_size = 256;
        let mut data = vec![0u8; file_size];

        // E8 at position 0: absolute addr = 50, cur_pos = 5, relative = 45
        data[0] = 0xE8;
        let addr1: i32 = 50;
        let b1 = addr1.to_le_bytes();
        data[1] = b1[0];
        data[2] = b1[1];
        data[3] = b1[2];
        data[4] = b1[3];

        // E9 at position 10: absolute addr = 80, cur_pos = 15, relative = 65
        data[10] = 0xE9;
        let addr2: i32 = 80;
        let b2 = addr2.to_le_bytes();
        data[11] = b2[0];
        data[12] = b2[1];
        data[13] = b2[2];
        data[14] = b2[3];

        apply_e8e9(&mut data, 0);

        let r1 = i32::from_le_bytes([data[1], data[2], data[3], data[4]]);
        assert_eq!(r1, 45); // 50 - 5

        let r2 = i32::from_le_bytes([data[11], data[12], data[13], data[14]]);
        assert_eq!(r2, 65); // 80 - 15
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
        assert_eq!(FilterType::from_code(0), Some(FilterType::Delta));
        assert_eq!(FilterType::from_code(1), Some(FilterType::E8));
        assert_eq!(FilterType::from_code(2), Some(FilterType::E8E9));
        assert_eq!(FilterType::from_code(3), Some(FilterType::Arm));
        assert_eq!(FilterType::from_code(4), None);
        assert_eq!(FilterType::from_code(7), None);
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
