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
        if let Some(target) = pad_to
            && target > self.bytes_fed
        {
            let padding = vec![0u8; (target - self.bytes_fed) as usize];
            self.crc32.update(&padding);
            self.md5.update(&padding);
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
pub fn crc32_combine(crc1: u32, crc2: u32, len2: u64) -> u32 {
    if len2 == 0 {
        return crc1;
    }

    const POLY: u32 = 0xEDB8_8320;

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

    fn mat_square(square: &mut [u32; 32], mat: &[u32; 32]) {
        for n in 0..32 {
            square[n] = mat_vec(mat, mat[n]);
        }
    }

    let mut op = [0u32; 32];
    op[0] = POLY;
    for (n, item) in op.iter_mut().enumerate().skip(1) {
        *item = 1 << (n - 1);
    }

    let mut tmp = [0u32; 32];
    for _ in 0..3 {
        mat_square(&mut tmp, &op);
        op = tmp;
    }

    let mut result = crc1;
    let mut remaining = len2;

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
