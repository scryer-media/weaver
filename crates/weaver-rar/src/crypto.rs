//! AES key derivation and decryption for RAR archives.
//!
//! - RAR5: AES-256-CBC with PBKDF2-HMAC-SHA256 key derivation
//! - RAR4: AES-128-CBC with custom iterative SHA-1 key derivation
//!
//! Provides both batch decryption (`decrypt_data`) and streaming decryption
//! via [`DecryptingReader`], which wraps any `Read` source and decrypts
//! AES-CBC on-the-fly using manual block-level operations.
//!
//! Includes a [`KdfCache`] that avoids re-deriving keys when the same
//! password+salt combination is used across multiple members (which is
//! the common case). Matches unrar's `KDF3Cache`/`KDF5Cache` approach.

use std::io::Read;
use std::sync::Mutex;

use aes::cipher::{BlockDecrypt, KeyInit};
use aes::{Aes128, Aes256};
use cbc::cipher::{BlockDecryptMut, KeyIvInit};
use hmac::Hmac;
use sha1::Sha1;
use sha2::Sha256;

use crate::error::{RarError, RarResult};

type Aes256CbcDec = cbc::Decryptor<Aes256>;
type Aes128CbcDec = cbc::Decryptor<Aes128>;
type HmacSha256 = Hmac<Sha256>;

/// Derive AES-256 key from password and salt using PBKDF2-HMAC-SHA256.
///
/// RAR5 KDF: iterations = 1 << kdf_count (confirmed against unrar source).
/// Returns only the 32-byte key. IVs in RAR5 are read from the stream
/// (each encrypted block is preceded by a 16-byte IV), not derived.
pub fn derive_key(password: &str, salt: &[u8; 16], kdf_count: u8) -> ([u8; 32], [u8; 16]) {
    let iterations = 1u32 << (kdf_count as u32);

    let mut key = [0u8; 32];
    pbkdf2::pbkdf2::<HmacSha256>(password.as_bytes(), salt, iterations, &mut key)
        .expect("HMAC can be initialized with any key length");

    // IV is not derived — return zeros. Callers that need an IV read it
    // from the stream (header encryption) or from the file header (file
    // data encryption).
    (key, [0u8; 16])
}

/// Decrypt data using AES-256-CBC.
///
/// The input must be a multiple of 16 bytes (AES block size).
/// Returns the decrypted data (no padding removal — RAR5 tracks exact sizes separately).
pub fn decrypt_data(key: &[u8; 32], iv: &[u8; 16], data: &[u8]) -> RarResult<Vec<u8>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    if !data.len().is_multiple_of(16) {
        return Err(RarError::CorruptArchive {
            detail: format!(
                "encrypted data length {} is not a multiple of AES block size (16)",
                data.len()
            ),
        });
    }

    let mut buf = data.to_vec();

    let decryptor = Aes256CbcDec::new(key.into(), iv.into());
    decryptor
        .decrypt_padded_mut::<cbc::cipher::block_padding::NoPadding>(&mut buf)
        .map_err(|_| RarError::CorruptArchive {
            detail: "AES-256-CBC decryption failed".into(),
        })?;

    Ok(buf)
}

/// Verify a password using the optional check value from the encryption header.
///
/// RAR5 uses a continuous PBKDF2 chain (reference: unrar crypt5.cpp):
///   Key       = PBKDF2(password, salt, Count)       — AES-256 key
///   V1        = PBKDF2(password, salt, Count + 16)   — HashKey (for HMAC CRC)
///   V2        = PBKDF2(password, salt, Count + 32)   — PswCheckValue (for password check)
///   PswCheck  = XOR-fold V2 from 32 bytes into 8 bytes
///
/// The check_data field is 12 bytes: first 8 = PswCheck, last 4 = SHA256 checksum.
pub fn verify_password_check(
    password: &str,
    salt: &[u8; 16],
    kdf_count: u8,
    check_data: &[u8; 12],
) -> bool {
    let iterations = (1u32 << (kdf_count as u32)) + 32;

    // Derive V2 (PswCheckValue) using standard PBKDF2 with Count+32 iterations.
    // This is equivalent to unrar's continuous chain because the XOR accumulation
    // in PBKDF2 (Fn = U1 ^ U2 ^ ... ^ U_n) is identical regardless of whether
    // you do it in one call or three sequential segments.
    let mut v2 = [0u8; 32];
    pbkdf2::pbkdf2::<HmacSha256>(password.as_bytes(), salt, iterations, &mut v2)
        .expect("HMAC can be initialized with any key length");

    // XOR-fold V2 from 32 bytes to 8 bytes (unrar: PswCheck[I%8] ^= V2[I]).
    let mut psw_check = [0u8; 8];
    for (i, &byte) in v2.iter().enumerate() {
        psw_check[i % 8] ^= byte;
    }

    psw_check == check_data[..8]
}

// =============================================================================
// KDF cache — avoids re-deriving keys for repeated password+salt combinations
// =============================================================================

const KDF_CACHE_SLOTS: usize = 4;

/// Cached RAR5 key derivation result.
struct Kdf5Entry {
    password: String,
    salt: [u8; 16],
    kdf_count: u8,
    key: [u8; 32],
    psw_check: [u8; 8],
}

/// Cached RAR4 key derivation result.
struct Kdf3Entry {
    password: String,
    salt: [u8; 8],
    key: [u8; 16],
    iv: [u8; 16],
}

/// Thread-safe KDF cache matching unrar's `KDF3Cache[4]`/`KDF5Cache[4]`.
///
/// Stores the most recent key derivation results and returns cached values
/// when the same password+salt combination is requested again. This avoids
/// re-running expensive KDF iterations (262k SHA-1 for RAR4, up to 2^24
/// PBKDF2 rounds for RAR5) on every member in an encrypted archive.
pub struct KdfCache {
    rar5: Mutex<(Vec<Kdf5Entry>, usize)>,
    rar4: Mutex<(Vec<Kdf3Entry>, usize)>,
}

impl KdfCache {
    pub fn new() -> Self {
        Self {
            rar5: Mutex::new((Vec::with_capacity(KDF_CACHE_SLOTS), 0)),
            rar4: Mutex::new((Vec::with_capacity(KDF_CACHE_SLOTS), 0)),
        }
    }

    /// Derive (or return cached) RAR5 AES-256 key.
    pub fn derive_key_rar5(&self, password: &str, salt: &[u8; 16], kdf_count: u8) -> [u8; 32] {
        let mut guard = self.rar5.lock().unwrap();
        let (entries, pos) = &mut *guard;

        // Check cache.
        for entry in entries.iter() {
            if entry.password == password && entry.salt == *salt && entry.kdf_count == kdf_count {
                return entry.key;
            }
        }

        // Cache miss — derive key.
        let (key, _) = derive_key(password, salt, kdf_count);

        // Compute psw_check too (count+32 iterations) so verify is free.
        let iterations_check = (1u32 << (kdf_count as u32)) + 32;
        let mut v2 = [0u8; 32];
        pbkdf2::pbkdf2::<HmacSha256>(password.as_bytes(), salt, iterations_check, &mut v2)
            .expect("HMAC can be initialized with any key length");
        let mut psw_check = [0u8; 8];
        for (i, &byte) in v2.iter().enumerate() {
            psw_check[i % 8] ^= byte;
        }

        let entry = Kdf5Entry {
            password: password.to_string(),
            salt: *salt,
            kdf_count,
            key,
            psw_check,
        };

        // Rotating insertion into fixed-size cache.
        if entries.len() < KDF_CACHE_SLOTS {
            entries.push(entry);
        } else {
            entries[*pos] = entry;
        }
        *pos = (*pos + 1) % KDF_CACHE_SLOTS;

        key
    }

    /// Verify password check value using cached data (avoids separate PBKDF2).
    pub fn verify_password_rar5(
        &self,
        password: &str,
        salt: &[u8; 16],
        kdf_count: u8,
        check_data: &[u8; 12],
    ) -> bool {
        // This populates the cache (including psw_check).
        let _ = self.derive_key_rar5(password, salt, kdf_count);

        let guard = self.rar5.lock().unwrap();
        let (entries, _) = &*guard;
        for entry in entries.iter() {
            if entry.password == password && entry.salt == *salt && entry.kdf_count == kdf_count {
                return entry.psw_check == check_data[..8];
            }
        }
        false
    }

    /// Derive (or return cached) RAR4 AES-128 key and IV.
    pub fn derive_key_rar4(&self, password: &str, salt: &[u8; 8]) -> ([u8; 16], [u8; 16]) {
        let mut guard = self.rar4.lock().unwrap();
        let (entries, pos) = &mut *guard;

        // Check cache.
        for entry in entries.iter() {
            if entry.password == password && entry.salt == *salt {
                return (entry.key, entry.iv);
            }
        }

        // Cache miss — derive key.
        let (key, iv) = rar4_derive_key(password, salt);

        let entry = Kdf3Entry {
            password: password.to_string(),
            salt: *salt,
            key,
            iv,
        };

        if entries.len() < KDF_CACHE_SLOTS {
            entries.push(entry);
        } else {
            entries[*pos] = entry;
        }
        *pos = (*pos + 1) % KDF_CACHE_SLOTS;

        (key, iv)
    }
}

impl Default for KdfCache {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// RAR4 encryption: AES-128-CBC with custom SHA-1 key derivation
// =============================================================================

/// RAR4 key derivation iteration count.
const RAR4_KDF_ITERATIONS: u32 = 0x40000; // 262144

/// Derive AES-128 key and IV from password and salt using RAR4's custom KDF.
///
/// RAR4 KDF algorithm (reference: unrar crypt3.cpp SetKey30):
/// - Encodes password as UTF-16LE
/// - Concatenates password_utf16le + salt into a single buffer
/// - Iterates 262144 times, each time hashing: buffer + 3-byte iteration counter
/// - At iterations 0, 16384, 32768, ... (i.e. `i % (262144/16) == 0`), the current
///   SHA-1 intermediate digest word H4's low byte is extracted as an IV byte
/// - After all iterations, the final SHA-1 digest words H0-H3 are extracted as the
///   AES-128 key in little-endian byte order per word
pub fn rar4_derive_key(password: &str, salt: &[u8; 8]) -> ([u8; 16], [u8; 16]) {
    use sha1::Digest;

    // Encode password as UTF-16LE, then append salt — matching unrar's RawPsw buffer.
    let mut raw_psw: Vec<u8> = password
        .encode_utf16()
        .flat_map(|c| c.to_le_bytes())
        .collect();
    raw_psw.extend_from_slice(salt);

    let iv_interval = RAR4_KDF_ITERATIONS / 16;
    let mut iv = [0u8; 16];
    let mut sha = Sha1::new();

    for i in 0..RAR4_KDF_ITERATIONS {
        sha.update(&raw_psw);

        // Append iteration counter as 3 bytes LE.
        let i_bytes = [i as u8, (i >> 8) as u8, (i >> 16) as u8];
        sha.update(i_bytes);

        // Extract IV byte at each interval boundary (unrar: `I%(HashRounds/16)==0`).
        if i % iv_interval == 0 {
            let intermediate = sha.clone().finalize();
            let iv_index = (i / iv_interval) as usize;
            // (byte)digest[4] in unrar = low byte of H4 = last byte of big-endian output.
            iv[iv_index] = intermediate[19];
        }
    }

    let digest = sha.finalize();

    // unrar extracts key bytes in LE order per 32-bit digest word:
    //   AESKey[I*4+J] = (byte)(digest[I] >> (J*8))
    // The Rust sha1 crate outputs bytes in big-endian, so we reverse within each
    // 4-byte group to match unrar's little-endian extraction.
    let mut key = [0u8; 16];
    for word in 0..4 {
        key[word * 4] = digest[word * 4 + 3]; // LSB
        key[word * 4 + 1] = digest[word * 4 + 2];
        key[word * 4 + 2] = digest[word * 4 + 1];
        key[word * 4 + 3] = digest[word * 4]; // MSB
    }

    (key, iv)
}

/// Decrypt data using AES-128-CBC (RAR4).
///
/// The input must be a multiple of 16 bytes (AES block size).
/// Returns the decrypted data (no padding removal — RAR4 tracks exact sizes via unpacked_size).
pub fn rar4_decrypt_data(key: &[u8; 16], iv: &[u8; 16], data: &[u8]) -> RarResult<Vec<u8>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    if !data.len().is_multiple_of(16) {
        return Err(RarError::CorruptArchive {
            detail: format!(
                "RAR4 encrypted data length {} is not a multiple of AES block size (16)",
                data.len()
            ),
        });
    }

    let mut buf = data.to_vec();
    let decryptor = Aes128CbcDec::new(key.into(), iv.into());
    decryptor
        .decrypt_padded_mut::<cbc::cipher::block_padding::NoPadding>(&mut buf)
        .map_err(|_| RarError::CorruptArchive {
            detail: "AES-128-CBC decryption failed".into(),
        })?;

    Ok(buf)
}

// =============================================================================
// Streaming AES-CBC decryption
// =============================================================================

const AES_BLOCK: usize = 16;

/// Stateful AES-256-CBC decryptor for incremental (streaming) decryption.
///
/// Unlike `decrypt_data` which requires all data at once, this carries the
/// CBC IV state across calls to `decrypt_blocks`.
pub struct CbcDecryptor {
    cipher: Aes256,
    iv: [u8; AES_BLOCK],
}

impl CbcDecryptor {
    pub fn new(key: &[u8; 32], iv: &[u8; AES_BLOCK]) -> Self {
        Self {
            cipher: Aes256::new(key.into()),
            iv: *iv,
        }
    }

    /// Decrypt `data` in-place. `data.len()` MUST be a multiple of 16.
    /// Updates internal IV state for subsequent calls.
    pub fn decrypt_blocks(&mut self, data: &mut [u8]) {
        debug_assert!(data.len().is_multiple_of(AES_BLOCK));
        for block in data.chunks_exact_mut(AES_BLOCK) {
            // Save ciphertext — it becomes the IV for the next block.
            let mut ct = [0u8; AES_BLOCK];
            ct.copy_from_slice(block);

            // Decrypt in-place.
            let gen_block = aes::cipher::generic_array::GenericArray::from_mut_slice(block);
            self.cipher.decrypt_block(gen_block);

            // XOR with IV to complete CBC.
            for (b, iv_byte) in block.iter_mut().zip(self.iv.iter()) {
                *b ^= iv_byte;
            }

            self.iv = ct;
        }
    }
}

/// Stateful AES-128-CBC decryptor for RAR4 archives.
pub struct Rar4CbcDecryptor {
    cipher: Aes128,
    iv: [u8; AES_BLOCK],
}

impl Rar4CbcDecryptor {
    pub fn new(key: &[u8; 16], iv: &[u8; AES_BLOCK]) -> Self {
        Self {
            cipher: Aes128::new(key.into()),
            iv: *iv,
        }
    }

    /// Decrypt `data` in-place. `data.len()` MUST be a multiple of 16.
    pub fn decrypt_blocks(&mut self, data: &mut [u8]) {
        debug_assert!(data.len().is_multiple_of(AES_BLOCK));
        for block in data.chunks_exact_mut(AES_BLOCK) {
            let mut ct = [0u8; AES_BLOCK];
            ct.copy_from_slice(block);
            let gen_block = aes::cipher::generic_array::GenericArray::from_mut_slice(block);
            self.cipher.decrypt_block(gen_block);
            for (b, iv_byte) in block.iter_mut().zip(self.iv.iter()) {
                *b ^= iv_byte;
            }
            self.iv = ct;
        }
    }
}

/// Decryptor enum that handles both RAR5 (AES-256) and RAR4 (AES-128).
pub enum CbcDecryptorAny {
    Rar5(Box<CbcDecryptor>),
    Rar4(Box<Rar4CbcDecryptor>),
}

impl CbcDecryptorAny {
    pub fn decrypt_blocks(&mut self, data: &mut [u8]) {
        match self {
            Self::Rar5(d) => d.decrypt_blocks(data),
            Self::Rar4(d) => d.decrypt_blocks(data),
        }
    }
}

/// Decryption buffer size: 256KB = 16384 AES blocks.
/// Larger buffers let the `cbc` crate pipeline AES-NI operations.
/// unrar uses a 32KB I/O buffer but decrypts in bulk after read;
/// we match that throughput with a single larger decrypt buffer.
const DECRYPT_BUF_SIZE: usize = 256 * 1024;

/// A `Read` adapter that decrypts AES-CBC on-the-fly.
///
/// Wraps an inner `Read` source (e.g. `ChainedSegmentReader`) and decrypts
/// data as it flows through. Handles partial AES blocks at read boundaries
/// by buffering internally.
///
/// The total data from the inner reader MUST be a multiple of 16 bytes
/// (guaranteed by RAR's archive format for encrypted members).
pub struct DecryptingReader<R: Read> {
    inner: R,
    decryptor: CbcDecryptorAny,
    /// Bytes read from inner but not yet forming a complete AES block.
    pending: [u8; AES_BLOCK],
    pending_len: usize,
    /// Heap-allocated decryption buffer (too large for stack).
    out_buf: Box<[u8]>,
    out_pos: usize,
    out_len: usize,
    /// Inner reader hit EOF.
    inner_eof: bool,
}

impl<R: Read> DecryptingReader<R> {
    /// Create a new decrypting reader for RAR5 (AES-256-CBC).
    pub fn new_rar5(inner: R, key: &[u8; 32], iv: &[u8; 16]) -> Self {
        Self {
            inner,
            decryptor: CbcDecryptorAny::Rar5(Box::new(CbcDecryptor::new(key, iv))),
            pending: [0u8; AES_BLOCK],
            pending_len: 0,
            out_buf: vec![0u8; DECRYPT_BUF_SIZE].into_boxed_slice(),
            out_pos: 0,
            out_len: 0,
            inner_eof: false,
        }
    }

    /// Create a new decrypting reader for RAR4 (AES-128-CBC).
    pub fn new_rar4(inner: R, key: &[u8; 16], iv: &[u8; 16]) -> Self {
        Self {
            inner,
            decryptor: CbcDecryptorAny::Rar4(Box::new(Rar4CbcDecryptor::new(key, iv))),
            pending: [0u8; AES_BLOCK],
            pending_len: 0,
            out_buf: vec![0u8; DECRYPT_BUF_SIZE].into_boxed_slice(),
            out_pos: 0,
            out_len: 0,
            inner_eof: false,
        }
    }
}

impl<R: Read> Read for DecryptingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Return any buffered decrypted data first.
        if self.out_pos < self.out_len {
            let n = (self.out_len - self.out_pos).min(buf.len());
            buf[..n].copy_from_slice(&self.out_buf[self.out_pos..self.out_pos + n]);
            self.out_pos += n;
            return Ok(n);
        }

        if self.inner_eof && self.pending_len == 0 {
            return Ok(0);
        }

        // Read directly into out_buf, prepending any pending partial block.
        let raw_start;
        if self.pending_len > 0 {
            self.out_buf[..self.pending_len].copy_from_slice(&self.pending[..self.pending_len]);
            raw_start = self.pending_len;
            self.pending_len = 0;
        } else {
            raw_start = 0;
        }

        // Fill out_buf as much as possible from inner.
        // Loop to fill the buffer since a single read() may return short.
        let mut total = raw_start;
        if !self.inner_eof {
            // Read at least enough to make progress, ideally fill the buffer.
            while total < DECRYPT_BUF_SIZE {
                let n = self
                    .inner
                    .read(&mut self.out_buf[total..DECRYPT_BUF_SIZE])?;
                if n == 0 {
                    self.inner_eof = true;
                    break;
                }
                total += n;
                // Don't loop forever on small reads — one good read is enough
                // to make progress. But try to fill the buffer for pipeline efficiency.
                if total >= DECRYPT_BUF_SIZE / 2 {
                    break;
                }
            }
        }

        if total == 0 {
            return Ok(0);
        }

        // How many complete AES blocks do we have?
        let complete = (total / AES_BLOCK) * AES_BLOCK;
        let leftover = total - complete;

        // Save leftover for next call.
        if leftover > 0 {
            self.pending[..leftover].copy_from_slice(&self.out_buf[complete..total]);
            self.pending_len = leftover;
        }

        if complete == 0 {
            if self.inner_eof {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "encrypted data not aligned to AES block size",
                ));
            }
            return self.read(buf);
        }

        // Decrypt complete blocks in-place (no extra copy).
        self.decryptor.decrypt_blocks(&mut self.out_buf[..complete]);
        self.out_pos = 0;
        self.out_len = complete;

        // Copy to caller.
        let n = complete.min(buf.len());
        buf[..n].copy_from_slice(&self.out_buf[..n]);
        self.out_pos = n;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_key_deterministic() {
        let salt = [0xAA; 16];
        let (key1, iv1) = derive_key("password", &salt, 0);
        let (key2, iv2) = derive_key("password", &salt, 0);
        assert_eq!(key1, key2);
        assert_eq!(iv1, iv2);

        // Different password produces different key
        let (key3, _) = derive_key("other", &salt, 0);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_decrypt_round_trip() {
        use aes::Aes256;
        use cbc::cipher::{BlockEncryptMut, KeyIvInit};

        type Aes256CbcEnc = cbc::Encryptor<Aes256>;

        let key = [0x42u8; 32];
        let iv = [0x13u8; 16];

        // Plaintext must be a multiple of 16 bytes
        let plaintext = b"Hello RAR world!"; // exactly 16 bytes
        assert_eq!(plaintext.len(), 16);

        // Encrypt
        let mut ciphertext = plaintext.to_vec();
        let encryptor = Aes256CbcEnc::new((&key).into(), (&iv).into());
        encryptor
            .encrypt_padded_mut::<cbc::cipher::block_padding::NoPadding>(&mut ciphertext, 16)
            .unwrap();

        // Decrypt
        let decrypted = decrypt_data(&key, &iv, &ciphertext).unwrap();
        assert_eq!(&decrypted, plaintext);
    }

    #[test]
    fn test_decrypt_empty() {
        let key = [0u8; 32];
        let iv = [0u8; 16];
        let result = decrypt_data(&key, &iv, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_decrypt_bad_length() {
        let key = [0u8; 32];
        let iv = [0u8; 16];
        let result = decrypt_data(&key, &iv, &[0u8; 15]);
        assert!(matches!(result, Err(RarError::CorruptArchive { .. })));
    }

    #[test]
    fn test_verify_password_check_consistent() {
        let salt = [0xBB; 16];
        let kdf_count = 0u8;

        // Derive V2 (PswCheckValue) using PBKDF2 with Count+32 iterations,
        // matching the continuous chain in unrar's crypt5.cpp.
        let iterations = (1u32 << (kdf_count as u32)) + 32;
        let mut v2 = [0u8; 32];
        pbkdf2::pbkdf2::<hmac::Hmac<sha2::Sha256>>(b"testpass", &salt, iterations, &mut v2)
            .unwrap();

        // XOR-fold V2 from 32 bytes to 8 bytes.
        let mut psw_check = [0u8; 8];
        for (i, &byte) in v2.iter().enumerate() {
            psw_check[i % 8] ^= byte;
        }

        let mut check_data = [0u8; 12];
        check_data[..8].copy_from_slice(&psw_check);

        assert!(verify_password_check(
            "testpass",
            &salt,
            kdf_count,
            &check_data
        ));
        assert!(!verify_password_check(
            "wrongpass",
            &salt,
            kdf_count,
            &check_data
        ));
    }

    // RAR4 crypto tests

    #[test]
    fn test_rar4_derive_key_deterministic() {
        let salt = [0xCC; 8];
        let (key1, iv1) = rar4_derive_key("password", &salt);
        let (key2, iv2) = rar4_derive_key("password", &salt);
        assert_eq!(key1, key2);
        assert_eq!(iv1, iv2);

        // Different password produces different key.
        let (key3, _) = rar4_derive_key("other", &salt);
        assert_ne!(key1, key3);

        // Different salt produces different key.
        let (key4, _) = rar4_derive_key("password", &[0xDD; 8]);
        assert_ne!(key1, key4);
    }

    #[test]
    fn test_rar4_decrypt_round_trip() {
        use cbc::cipher::{BlockEncryptMut, KeyIvInit};

        type Aes128CbcEnc = cbc::Encryptor<Aes128>;

        let key = [0x42u8; 16];
        let iv = [0x13u8; 16];

        let plaintext = b"RAR4 encrypted!!"; // 16 bytes
        assert_eq!(plaintext.len(), 16);

        // Encrypt
        let mut ciphertext = plaintext.to_vec();
        let encryptor = Aes128CbcEnc::new((&key).into(), (&iv).into());
        encryptor
            .encrypt_padded_mut::<cbc::cipher::block_padding::NoPadding>(&mut ciphertext, 16)
            .unwrap();

        // Decrypt
        let decrypted = rar4_decrypt_data(&key, &iv, &ciphertext).unwrap();
        assert_eq!(&decrypted, plaintext);
    }

    #[test]
    fn test_rar4_decrypt_empty() {
        let key = [0u8; 16];
        let iv = [0u8; 16];
        let result = rar4_decrypt_data(&key, &iv, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_rar4_decrypt_bad_length() {
        let key = [0u8; 16];
        let iv = [0u8; 16];
        let result = rar4_decrypt_data(&key, &iv, &[0u8; 15]);
        assert!(matches!(result, Err(RarError::CorruptArchive { .. })));
    }

    #[test]
    fn test_rar4_kdf_with_derived_key_round_trip() {
        // Derive key, encrypt, decrypt, verify round-trip.
        use cbc::cipher::{BlockEncryptMut, KeyIvInit};

        type Aes128CbcEnc = cbc::Encryptor<Aes128>;

        let salt = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let (key, iv) = rar4_derive_key("testpassword", &salt);

        // Encrypt 32 bytes of plaintext.
        let plaintext = b"Hello from RAR4 encryption test!"; // 32 bytes
        assert_eq!(plaintext.len(), 32);

        let mut ciphertext = plaintext.to_vec();
        let encryptor = Aes128CbcEnc::new((&key).into(), (&iv).into());
        encryptor
            .encrypt_padded_mut::<cbc::cipher::block_padding::NoPadding>(&mut ciphertext, 32)
            .unwrap();

        assert_ne!(&ciphertext, plaintext);

        let decrypted = rar4_decrypt_data(&key, &iv, &ciphertext).unwrap();
        assert_eq!(&decrypted, plaintext);
    }
}
