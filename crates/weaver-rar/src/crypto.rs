//! AES key derivation and decryption for RAR archives.
//!
//! - RAR5: AES-256-CBC with PBKDF2-HMAC-SHA256 key derivation
//! - RAR4: AES-128-CBC with custom iterative SHA-1 key derivation
//!
//! Provides both batch decryption (`decrypt_data`) and streaming decryption
//! via [`DecryptingReader`], which wraps any `Read` source and decrypts
//! AES-CBC on-the-fly using manual block-level operations.

use std::io::Read;

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

/// Derive AES-256 key and IV from password and salt using PBKDF2-HMAC-SHA256.
///
/// RAR5 KDF: iterations = 1 << (kdf_count + 15)
/// Derives 32 bytes for the key, then uses a second PBKDF2 call with a
/// modified salt to derive the 16-byte IV.
pub fn derive_key(password: &str, salt: &[u8; 16], kdf_count: u8) -> ([u8; 32], [u8; 16]) {
    let iterations = 1u32 << (kdf_count as u32 + 15);

    // RAR5 derives key material by running PBKDF2 on password + salt.
    // The key is 32 bytes, derived with the base salt.
    let mut key = [0u8; 32];
    pbkdf2::pbkdf2::<HmacSha256>(password.as_bytes(), salt, iterations, &mut key)
        .expect("HMAC can be initialized with any key length");

    // The IV is derived by running PBKDF2 again with salt + 1-byte suffix.
    let mut iv_salt = Vec::with_capacity(salt.len() + 1);
    iv_salt.extend_from_slice(salt);
    iv_salt.push(1); // IV derivation marker

    let mut iv = [0u8; 16];
    pbkdf2::pbkdf2::<HmacSha256>(password.as_bytes(), &iv_salt, iterations, &mut iv)
        .expect("HMAC can be initialized with any key length");

    (key, iv)
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
/// RAR5 password check: derive a check value from the password and compare
/// against the stored 8-byte value (first 8 of the 12-byte check data).
pub fn verify_password_check(
    password: &str,
    salt: &[u8; 16],
    kdf_count: u8,
    check_data: &[u8; 12],
) -> bool {
    let mut check_salt = Vec::with_capacity(salt.len() + 1);
    check_salt.extend_from_slice(salt);
    check_salt.push(2); // Password check derivation marker

    let iterations = 1u32 << (kdf_count as u32 + 15);
    let mut derived_check = [0u8; 8];
    pbkdf2::pbkdf2::<HmacSha256>(
        password.as_bytes(),
        &check_salt,
        iterations,
        &mut derived_check,
    )
    .expect("HMAC can be initialized with any key length");

    derived_check == check_data[..8]
}

// =============================================================================
// RAR4 encryption: AES-128-CBC with custom SHA-1 key derivation
// =============================================================================

/// RAR4 key derivation iteration count.
const RAR4_KDF_ITERATIONS: u32 = 0x40000; // 262144

/// Derive AES-128 key and IV from password and salt using RAR4's custom KDF.
///
/// RAR4 KDF algorithm (reference: libarchive, BSD licensed):
/// - Encodes password as UTF-16LE
/// - Iterates 262144 times, each time hashing: `password_utf16le + salt + iteration_le_bytes`
/// - Every 16384th iteration (i.e. when `(i+1) % (RAR4_KDF_ITERATIONS/16) == 0`),
///   the current SHA-1 digest's first byte is extracted as an IV byte
/// - After all iterations, the final SHA-1 digest's first 16 bytes become the AES-128 key
pub fn rar4_derive_key(password: &str, salt: &[u8; 8]) -> ([u8; 16], [u8; 16]) {
    use sha1::Digest;

    // Encode password as UTF-16LE.
    let password_utf16: Vec<u8> = password
        .encode_utf16()
        .flat_map(|c| c.to_le_bytes())
        .collect();

    let iv_interval = RAR4_KDF_ITERATIONS / 16;
    let mut iv = [0u8; 16];
    let mut sha = Sha1::new();

    for i in 0..RAR4_KDF_ITERATIONS {
        sha.update(&password_utf16);
        sha.update(salt);

        // Append iteration counter as 3 bytes LE.
        let i_bytes = [i as u8, (i >> 8) as u8, (i >> 16) as u8];
        sha.update(i_bytes);

        // Extract IV byte at each interval boundary.
        if (i + 1) % iv_interval == 0 {
            // Clone the hasher to get intermediate digest without consuming it.
            let intermediate = sha.clone().finalize();
            let iv_index = ((i + 1) / iv_interval - 1) as usize;
            iv[iv_index] = intermediate[19]; // last byte of SHA-1 digest
        }
    }

    let digest = sha.finalize();
    let mut key = [0u8; 16];
    key.copy_from_slice(&digest[..16]);

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

/// Decryption buffer size: 4096 bytes = 256 AES blocks.
const DECRYPT_BUF_SIZE: usize = 4096;

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
    /// Decrypted data ready to be consumed by the caller.
    out_buf: [u8; DECRYPT_BUF_SIZE],
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
            out_buf: [0u8; DECRYPT_BUF_SIZE],
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
            out_buf: [0u8; DECRYPT_BUF_SIZE],
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

        // Read from inner into a temp buffer, prepending any pending bytes.
        // We want to fill out_buf with complete AES blocks.
        let mut raw = [0u8; DECRYPT_BUF_SIZE + AES_BLOCK];
        let raw_start;
        if self.pending_len > 0 {
            raw[..self.pending_len].copy_from_slice(&self.pending[..self.pending_len]);
            raw_start = self.pending_len;
            self.pending_len = 0;
        } else {
            raw_start = 0;
        }

        // Read more from inner.
        let bytes_read = if !self.inner_eof {
            let n = self.inner.read(&mut raw[raw_start..raw_start + DECRYPT_BUF_SIZE])?;
            if n == 0 {
                self.inner_eof = true;
            }
            n
        } else {
            0
        };

        let total = raw_start + bytes_read;
        if total == 0 {
            return Ok(0);
        }

        // How many complete AES blocks do we have?
        let complete = (total / AES_BLOCK) * AES_BLOCK;
        let leftover = total - complete;

        // Save leftover for next call.
        if leftover > 0 {
            self.pending[..leftover].copy_from_slice(&raw[complete..total]);
            self.pending_len = leftover;
        }

        if complete == 0 {
            // Not enough data for a full block yet. If inner is EOF, this
            // means the data wasn't block-aligned — shouldn't happen.
            if self.inner_eof {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "encrypted data not aligned to AES block size",
                ));
            }
            // Try reading more.
            return self.read(buf);
        }

        // Decrypt complete blocks in-place.
        self.out_buf[..complete].copy_from_slice(&raw[..complete]);
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

        // Derive a check value
        let iterations = 1u32 << (kdf_count as u32 + 15);
        let mut check_salt = Vec::with_capacity(17);
        check_salt.extend_from_slice(&salt);
        check_salt.push(2);

        let mut derived = [0u8; 8];
        pbkdf2::pbkdf2::<hmac::Hmac<sha2::Sha256>>(
            b"testpass",
            &check_salt,
            iterations,
            &mut derived,
        )
        .unwrap();

        let mut check_data = [0u8; 12];
        check_data[..8].copy_from_slice(&derived);

        assert!(verify_password_check("testpass", &salt, kdf_count, &check_data));
        assert!(!verify_password_check("wrongpass", &salt, kdf_count, &check_data));
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
