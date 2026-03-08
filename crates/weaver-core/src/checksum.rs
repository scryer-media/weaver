use crc32fast::Hasher as Crc32Hasher;
use md5::{Digest, Md5};

/// Streaming CRC32 + MD5 checksum state for a single file slice.
///
/// Feeds data incrementally and produces the final (CRC32, MD5) pair that can
/// be compared against PAR2 IFSC checksum entries.
///
/// For the last slice of a file (which may be shorter than slice_size), the
/// PAR2 spec requires the remaining bytes to be zero-padded for checksum
/// computation. Call [`finalize`](SliceChecksumState::finalize) with the
/// `pad_to` parameter to handle this.
#[derive(Clone)]
pub struct SliceChecksumState {
    crc32: Crc32Hasher,
    md5: Md5,
    bytes_fed: u64,
}

impl SliceChecksumState {
    /// Create a new checksum state.
    pub fn new() -> Self {
        Self {
            crc32: Crc32Hasher::new(),
            md5: Md5::new(),
            bytes_fed: 0,
        }
    }

    /// Feed data into the checksum accumulators.
    pub fn update(&mut self, data: &[u8]) {
        self.crc32.update(data);
        self.md5.update(data);
        self.bytes_fed += data.len() as u64;
    }

    /// How many bytes have been fed so far.
    pub fn bytes_fed(&self) -> u64 {
        self.bytes_fed
    }

    /// Finalize and return (CRC32, MD5).
    ///
    /// If `pad_to` is specified and greater than `bytes_fed`, zero bytes are
    /// fed to reach that length (for the last slice of a file).
    pub fn finalize(mut self, pad_to: Option<u64>) -> (u32, [u8; 16]) {
        if let Some(target) = pad_to {
            if target > self.bytes_fed {
                let padding = vec![0u8; (target - self.bytes_fed) as usize];
                self.crc32.update(&padding);
                self.md5.update(&padding);
            }
        }
        let crc = self.crc32.finalize();
        let md5: [u8; 16] = self.md5.finalize().into();
        (crc, md5)
    }
}

impl Default for SliceChecksumState {
    fn default() -> Self {
        Self::new()
    }
}

/// Streaming full-file MD5 hash state.
#[derive(Clone)]
pub struct FileHashState {
    md5: Md5,
    bytes_fed: u64,
}

impl FileHashState {
    pub fn new() -> Self {
        Self {
            md5: Md5::new(),
            bytes_fed: 0,
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        self.md5.update(data);
        self.bytes_fed += data.len() as u64;
    }

    pub fn bytes_fed(&self) -> u64 {
        self.bytes_fed
    }

    pub fn finalize(self) -> [u8; 16] {
        self.md5.finalize().into()
    }
}

impl Default for FileHashState {
    fn default() -> Self {
        Self::new()
    }
}

/// Combine two CRC32 values as if the underlying data were concatenated.
///
/// Given `crc1 = CRC32(A)` and `crc2 = CRC32(B)`, returns `CRC32(A || B)`.
/// `len2` is the byte length of B.
///
/// Uses the GF(2) matrix-exponentiation algorithm (same approach as zlib's
/// `crc32_combine`).
pub fn crc32_combine(crc1: u32, crc2: u32, len2: u64) -> u32 {
    if len2 == 0 {
        return crc1;
    }

    // CRC32 polynomial (reflected): x^32 + x^26 + x^23 + x^22 + x^16 +
    // x^12 + x^11 + x^10 + x^8 + x^7 + x^5 + x^4 + x^2 + x + 1
    const POLY: u32 = 0xEDB8_8320;

    // We need to compute crc1 * x^(8*len2) mod P in GF(2), then XOR with crc2.
    // Use square-and-multiply on a 32x32 bit matrix over GF(2).

    // `mat_square_vec` applies a GF(2) matrix (represented as 32 u32 rows)
    // to a 32-bit vector.
    #[inline]
    fn mat_vec(mat: &[u32; 32], vec: u32) -> u32 {
        let mut result = 0u32;
        let mut v = vec;
        let mut i = 0;
        while v != 0 {
            if v & 1 != 0 {
                result ^= mat[i];
            }
            v >>= 1;
            i += 1;
        }
        result
    }

    // Square a GF(2) matrix in place.
    fn mat_square(square: &mut [u32; 32], mat: &[u32; 32]) {
        for n in 0..32 {
            square[n] = mat_vec(mat, mat[n]);
        }
    }

    // Build the "multiply by x" operator matrix (one zero-byte step).
    // For CRC32 (reflected), shifting by one bit:
    //   If bit 0 is set: shift right 1, XOR poly
    //   Else: shift right 1
    // But we want 8 bits at a time. Start with the single-bit operator,
    // then square it 3 times to get the 8-bit operator.
    let mut op = [0u32; 32];
    // Single-bit shift operator
    op[0] = POLY;
    for n in 1..32 {
        op[n] = 1 << (n - 1);
    }

    // Square 3 times: 1-bit -> 2-bit -> 4-bit -> 8-bit operator
    let mut tmp = [0u32; 32];
    for _ in 0..3 {
        mat_square(&mut tmp, &op);
        op = tmp;
    }

    // Now apply the 8-bit operator `len2` times using square-and-multiply
    // on the exponent.
    let mut result = crc1;
    let mut remaining = len2;

    // `op` is the current power of the operator
    while remaining > 0 {
        if remaining & 1 != 0 {
            result = mat_vec(&op, result);
        }
        remaining >>= 1;
        if remaining > 0 {
            mat_square(&mut tmp, &op);
            op = tmp;
        }
    }

    result ^ crc2
}

/// Compute CRC32 of a byte slice.
pub fn crc32(data: &[u8]) -> u32 {
    let mut hasher = Crc32Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Compute MD5 of a byte slice.
pub fn md5(data: &[u8]) -> [u8; 16] {
    Md5::digest(data).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_known_value() {
        assert_eq!(crc32(b""), 0);
        assert_eq!(crc32(b"123456789"), 0xCBF43926);
    }

    #[test]
    fn md5_known_value() {
        let empty_md5 = md5(b"");
        assert_eq!(
            empty_md5,
            [
                0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04, 0xe9, 0x80, 0x09, 0x98, 0xec,
                0xf8, 0x42, 0x7e
            ]
        );
    }

    #[test]
    fn streaming_matches_oneshot() {
        let data = b"Hello, World! This is test data for streaming checksum.";

        let expected_crc = crc32(data);
        let expected_md5 = md5(data);

        for chunk_size in [1, 3, 7, 13, data.len()] {
            let mut state = SliceChecksumState::new();
            for chunk in data.chunks(chunk_size) {
                state.update(chunk);
            }
            let (crc, md5_hash) = state.finalize(None);
            assert_eq!(crc, expected_crc, "CRC32 mismatch for chunk_size={chunk_size}");
            assert_eq!(
                md5_hash, expected_md5,
                "MD5 mismatch for chunk_size={chunk_size}"
            );
        }
    }

    #[test]
    fn slice_checksum_with_padding() {
        let data = b"short";
        let pad_to = 16u64;

        let mut padded = data.to_vec();
        padded.resize(pad_to as usize, 0);
        let expected_crc = crc32(&padded);
        let expected_md5 = md5(&padded);

        let mut state = SliceChecksumState::new();
        state.update(data);
        let (crc, md5_hash) = state.finalize(Some(pad_to));
        assert_eq!(crc, expected_crc);
        assert_eq!(md5_hash, expected_md5);
    }

    #[test]
    fn file_hash_state_works() {
        let data = b"test file content";
        let expected = md5(data);

        let mut state = FileHashState::new();
        state.update(&data[..5]);
        state.update(&data[5..]);
        assert_eq!(state.bytes_fed(), data.len() as u64);
        assert_eq!(state.finalize(), expected);
    }

    #[test]
    fn crc32_combine_basic() {
        let a = b"Hello, ";
        let b = b"World!";
        let combined = crc32(&[a.as_slice(), b.as_slice()].concat());
        let result = crc32_combine(crc32(a), crc32(b), b.len() as u64);
        assert_eq!(result, combined);
    }

    #[test]
    fn crc32_combine_empty_second() {
        let a = b"data";
        assert_eq!(crc32_combine(crc32(a), crc32(b""), 0), crc32(a));
    }

    #[test]
    fn crc32_combine_zero_padding() {
        // Simulate PAR2 last-slice zero-padding
        let data = b"short";
        let padded_len = 64u64;
        let padding = vec![0u8; (padded_len - data.len() as u64) as usize];
        let expected = crc32(&[data.as_slice(), &padding].concat());
        let result = crc32_combine(crc32(data), crc32(&padding), padding.len() as u64);
        assert_eq!(result, expected);
    }

    #[test]
    fn crc32_combine_large_length() {
        // Combine many small pieces
        let parts: Vec<Vec<u8>> = (0..100u8).map(|i| vec![i; 100]).collect();
        let full: Vec<u8> = parts.iter().flat_map(|p| p.iter().copied()).collect();
        let expected = crc32(&full);

        let mut combined = crc32(&parts[0]);
        for part in &parts[1..] {
            combined = crc32_combine(combined, crc32(part), part.len() as u64);
        }
        assert_eq!(combined, expected);
    }

    #[test]
    fn bytes_fed_tracking() {
        let mut state = SliceChecksumState::new();
        assert_eq!(state.bytes_fed(), 0);
        state.update(b"hello");
        assert_eq!(state.bytes_fed(), 5);
        state.update(b"world");
        assert_eq!(state.bytes_fed(), 10);
    }
}
