//! AES key derivation and decryption for RAR archives.
//!
//! - RAR5: AES-256-CBC with PBKDF2-HMAC-SHA256 key derivation
//! - RAR4: AES-128-CBC with custom iterative SHA-1 key derivation
//!
//! Provides both batch decryption (`decrypt_data`) and streaming decryption
//! via [`DecryptingReader`], which wraps any `Read` source and decrypts
//! AES-CBC on-the-fly using the cipher backend's block-mode implementation.
//!
//! Includes a [`KdfCache`] that avoids re-deriving keys when the same
//! password+salt combination is used across multiple members (which is
//! the common case).

use std::borrow::Cow;
use std::io::Read;
use std::ptr::null_mut;
use std::sync::Mutex;

use aws_lc_rs::{digest as aws_digest, hmac as aws_hmac};
use aws_lc_sys::{
    EVP_CIPHER, EVP_CIPHER_CTX, EVP_CIPHER_CTX_free, EVP_CIPHER_CTX_new,
    EVP_CIPHER_CTX_set_padding, EVP_DecryptInit_ex, EVP_DecryptUpdate, EVP_aes_128_cbc,
    EVP_aes_256_cbc,
};
#[cfg(test)]
use aws_lc_sys::{EVP_EncryptInit_ex, EVP_EncryptUpdate};
use blake2s_simd::blake2sp;
use subtle::ConstantTimeEq;
use zeroize::Zeroize;

use crate::error::{RarError, RarResult};
use crate::rar4::types::Rar4EncryptionMethod;

pub const CRYPT5_KDF_LG2_COUNT_MAX: u8 = 24;

/// RAR standard crypto uses AWS-LC on Scryer's supported targets. RAR4's
/// custom RAR29 SHA-1 KDF and legacy RAR 1.5/2.0 ciphers are RAR-specific
/// legacy algorithms, so they stay as local implementations.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rar5KeyMaterial {
    pub key: [u8; 32],
    pub hash_key: [u8; 32],
    pub psw_check: [u8; 8],
}

impl Drop for Rar5KeyMaterial {
    fn drop(&mut self) {
        self.key.zeroize();
        self.hash_key.zeroize();
        self.psw_check.zeroize();
    }
}

fn hmac_sha256_aws_lc(password: &aws_hmac::Key, data: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(aws_hmac::sign(password, data).as_ref());
    out
}

pub(crate) fn sha256_digest(data: &[u8]) -> [u8; 32] {
    let digest = aws_digest::digest(&aws_digest::SHA256, data);
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_ref());
    out
}

fn fold_password_check(value: &[u8; 32]) -> [u8; 8] {
    let mut psw_check = [0u8; 8];
    for (index, byte) in value.iter().copied().enumerate() {
        psw_check[index % psw_check.len()] ^= byte;
    }
    psw_check
}

const MAXPASSWORD_RAR: usize = 128;
const RAR_PASSWORD_MAX_UNITS: usize = MAXPASSWORD_RAR - 1;

fn rar_password_compat(password: &str) -> Cow<'_, str> {
    let mut units = 0usize;
    for (index, ch) in password.char_indices() {
        let next = units + ch.len_utf16();
        if next > RAR_PASSWORD_MAX_UNITS {
            return Cow::Owned(password[..index].to_string());
        }
        units = next;
    }
    Cow::Borrowed(password)
}

const CP437_HIGH_CODEPOINTS: [u16; 128] = [
    0x00C7, 0x00FC, 0x00E9, 0x00E2, 0x00E4, 0x00E0, 0x00E5, 0x00E7, 0x00EA, 0x00EB, 0x00E8, 0x00EF,
    0x00EE, 0x00EC, 0x00C4, 0x00C5, 0x00C9, 0x00E6, 0x00C6, 0x00F4, 0x00F6, 0x00F2, 0x00FB, 0x00F9,
    0x00FF, 0x00D6, 0x00DC, 0x00A2, 0x00A3, 0x00A5, 0x20A7, 0x0192, 0x00E1, 0x00ED, 0x00F3, 0x00FA,
    0x00F1, 0x00D1, 0x00AA, 0x00BA, 0x00BF, 0x2310, 0x00AC, 0x00BD, 0x00BC, 0x00A1, 0x00AB, 0x00BB,
    0x2591, 0x2592, 0x2593, 0x2502, 0x2524, 0x2561, 0x2562, 0x2556, 0x2555, 0x2563, 0x2551, 0x2557,
    0x255D, 0x255C, 0x255B, 0x2510, 0x2514, 0x2534, 0x252C, 0x251C, 0x2500, 0x253C, 0x255E, 0x255F,
    0x255A, 0x2554, 0x2569, 0x2566, 0x2560, 0x2550, 0x256C, 0x2567, 0x2568, 0x2564, 0x2565, 0x2559,
    0x2558, 0x2552, 0x2553, 0x256B, 0x256A, 0x2518, 0x250C, 0x2588, 0x2584, 0x258C, 0x2590, 0x2580,
    0x03B1, 0x00DF, 0x0393, 0x03C0, 0x03A3, 0x03C3, 0x00B5, 0x03C4, 0x03A6, 0x0398, 0x03A9, 0x03B4,
    0x221E, 0x03C6, 0x03B5, 0x2229, 0x2261, 0x00B1, 0x2265, 0x2264, 0x2320, 0x2321, 0x00F7, 0x2248,
    0x00B0, 0x2219, 0x00B7, 0x221A, 0x207F, 0x00B2, 0x25A0, 0x00A0,
];

fn encode_cp437_char(ch: char) -> u8 {
    let codepoint = ch as u32;
    if codepoint <= 0x7f {
        return codepoint as u8;
    }

    CP437_HIGH_CODEPOINTS
        .iter()
        .position(|&mapped| u32::from(mapped) == codepoint)
        .map(|index| 0x80u8 + index as u8)
        .unwrap_or(b'?')
}

fn rar_password_oem_bytes_compat(password: &str) -> Vec<u8> {
    rar_password_compat(password)
        .chars()
        .map(encode_cp437_char)
        .collect()
}

pub fn derive_rar5_material(
    password: &str,
    salt: &[u8; 16],
    kdf_count: u8,
) -> RarResult<Rar5KeyMaterial> {
    if kdf_count > CRYPT5_KDF_LG2_COUNT_MAX {
        return Err(RarError::UnsupportedEncryptionKdf {
            count: kdf_count,
            max: CRYPT5_KDF_LG2_COUNT_MAX,
        });
    }

    let count = 1u32 << kdf_count;
    let password = rar_password_compat(password);
    let password_mac = aws_hmac::Key::new(aws_hmac::HMAC_SHA256, password.as_bytes());

    let mut salt_block = [0u8; 20];
    salt_block[..salt.len()].copy_from_slice(salt);
    salt_block[19] = 1;

    let mut u = hmac_sha256_aws_lc(&password_mac, &salt_block);
    let mut fn_value = u;

    let mut key = [0u8; 32];
    let mut hash_key = [0u8; 32];
    let mut psw_check_value = [0u8; 32];

    for (rounds, output) in [
        (count.saturating_sub(1), &mut key),
        (16, &mut hash_key),
        (16, &mut psw_check_value),
    ] {
        for _ in 0..rounds {
            u = hmac_sha256_aws_lc(&password_mac, &u);
            for (acc, next) in fn_value.iter_mut().zip(u.iter()) {
                *acc ^= *next;
            }
        }
        *output = fn_value;
    }

    let mut psw_check = fold_password_check(&psw_check_value);
    let material = Rar5KeyMaterial {
        key,
        hash_key,
        psw_check,
    };

    salt_block.zeroize();
    u.zeroize();
    fn_value.zeroize();
    key.zeroize();
    hash_key.zeroize();
    psw_check_value.zeroize();
    psw_check.zeroize();

    Ok(material)
}

/// Derive AES-256 key from password and salt using PBKDF2-HMAC-SHA256.
///
/// RAR5 KDF: iterations = 1 << kdf_count.
/// Returns only the 32-byte key. IVs in RAR5 are read from the stream
/// (each encrypted block is preceded by a 16-byte IV), not derived.
pub fn derive_key(
    password: &str,
    salt: &[u8; 16],
    kdf_count: u8,
) -> RarResult<([u8; 32], [u8; 16])> {
    // IV is not derived — return zeros. Callers that need an IV read it
    // from the stream (header encryption) or from the file header (file
    // data encryption).
    let mut material = derive_rar5_material(password, salt, kdf_count)?;
    let key = material.key;
    material.key.zeroize();
    material.hash_key.zeroize();
    material.psw_check.zeroize();

    Ok((key, [0u8; 16]))
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
    let mut decryptor = CbcDecryptor::new(key, iv);
    decryptor.decrypt_blocks(&mut buf);

    Ok(buf)
}

/// Verify a password using the optional check value from the encryption header.
///
/// RAR5 uses a continuous PBKDF2 chain:
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
    derive_rar5_material(password, salt, kdf_count)
        .map(|mut material| {
            let matches = password_check_matches(&material.psw_check, check_data);
            material.key.zeroize();
            material.hash_key.zeroize();
            material.psw_check.zeroize();
            matches
        })
        .unwrap_or(false)
}

fn password_check_matches(psw_check: &[u8; 8], check_data: &[u8; 12]) -> bool {
    psw_check.as_slice().ct_eq(&check_data[..8]).into()
}

pub fn convert_crc32_to_mac(value: u32, key: &[u8; 32]) -> u32 {
    let digest = hmac_sha256_aws_lc(
        &aws_hmac::Key::new(aws_hmac::HMAC_SHA256, key),
        &value.to_le_bytes(),
    );
    let mut mac = 0u32;
    for (index, byte) in digest.iter().copied().enumerate() {
        mac ^= (byte as u32) << ((index & 3) * 8);
    }
    mac
}

pub fn convert_blake2_to_mac(value: [u8; 32], key: &[u8; 32]) -> [u8; 32] {
    hmac_sha256_aws_lc(&aws_hmac::Key::new(aws_hmac::HMAC_SHA256, key), &value)
}

#[derive(Clone, Debug)]
pub struct Blake2spHasher {
    inner: blake2sp::State,
}

impl Default for Blake2spHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Blake2spHasher {
    pub fn new() -> Self {
        Self {
            inner: blake2sp::State::new(),
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        self.inner.update(data);
    }

    pub fn finalize(&self) -> [u8; 32] {
        *self.inner.clone().finalize().as_array()
    }
}

pub fn blake2sp_hash(data: &[u8]) -> [u8; 32] {
    *blake2sp::blake2sp(data).as_array()
}

// =============================================================================
// KDF cache — avoids re-deriving keys for repeated password+salt combinations
// =============================================================================

const KDF_CACHE_SLOTS: usize = 4;

/// Cached RAR5 key derivation result.
#[derive(Debug)]
struct Kdf5Entry {
    password: String,
    salt: [u8; 16],
    kdf_count: u8,
    key: [u8; 32],
    hash_key: [u8; 32],
    psw_check: [u8; 8],
}

impl Drop for Kdf5Entry {
    fn drop(&mut self) {
        self.password.zeroize();
        self.salt.zeroize();
        self.kdf_count.zeroize();
        self.key.zeroize();
        self.hash_key.zeroize();
        self.psw_check.zeroize();
    }
}

/// Cached RAR4 key derivation result.
#[derive(Debug)]
struct Kdf3Entry {
    password: String,
    salt: Option<[u8; 8]>,
    key: [u8; 16],
    iv: [u8; 16],
}

impl Drop for Kdf3Entry {
    fn drop(&mut self) {
        self.password.zeroize();
        if let Some(salt) = self.salt.as_mut() {
            salt.zeroize();
        }
        self.salt = None;
        self.key.zeroize();
        self.iv.zeroize();
    }
}

/// Thread-safe KDF cache for repeated RAR3/RAR5 key derivations.
///
/// Stores the most recent key derivation results and returns cached values
/// when the same password+salt combination is requested again. This avoids
/// re-running expensive KDF iterations (262k SHA-1 for RAR4, up to 2^24
/// PBKDF2 rounds for RAR5) on every member in an encrypted archive.
#[derive(Debug)]
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

    pub fn derive_material_rar5(
        &self,
        password: &str,
        salt: &[u8; 16],
        kdf_count: u8,
    ) -> RarResult<Rar5KeyMaterial> {
        let mut guard = self.rar5.lock().unwrap();
        let (entries, pos) = &mut *guard;

        for entry in entries.iter() {
            if entry.password == password && entry.salt == *salt && entry.kdf_count == kdf_count {
                return Ok(Rar5KeyMaterial {
                    key: entry.key,
                    hash_key: entry.hash_key,
                    psw_check: entry.psw_check,
                });
            }
        }

        let material = derive_rar5_material(password, salt, kdf_count)?;

        let entry = Kdf5Entry {
            password: password.to_string(),
            salt: *salt,
            kdf_count,
            key: material.key,
            hash_key: material.hash_key,
            psw_check: material.psw_check,
        };

        if entries.len() < KDF_CACHE_SLOTS {
            entries.push(entry);
        } else {
            entries[*pos] = entry;
        }
        *pos = (*pos + 1) % KDF_CACHE_SLOTS;

        Ok(material)
    }

    /// Derive (or return cached) RAR5 AES-256 key.
    pub fn derive_key_rar5(
        &self,
        password: &str,
        salt: &[u8; 16],
        kdf_count: u8,
    ) -> RarResult<[u8; 32]> {
        Ok(self.derive_material_rar5(password, salt, kdf_count)?.key)
    }

    pub fn derive_hash_key_rar5(
        &self,
        password: &str,
        salt: &[u8; 16],
        kdf_count: u8,
    ) -> RarResult<[u8; 32]> {
        Ok(self
            .derive_material_rar5(password, salt, kdf_count)?
            .hash_key)
    }

    /// Verify password check value using cached data (avoids separate PBKDF2).
    pub fn verify_password_rar5(
        &self,
        password: &str,
        salt: &[u8; 16],
        kdf_count: u8,
        check_data: &[u8; 12],
    ) -> bool {
        self.derive_material_rar5(password, salt, kdf_count)
            .map(|material| password_check_matches(&material.psw_check, check_data))
            .unwrap_or(false)
    }

    /// Derive (or return cached) RAR4 AES-128 key and IV.
    pub fn derive_key_rar4(&self, password: &str, salt: Option<&[u8; 8]>) -> ([u8; 16], [u8; 16]) {
        let mut guard = self.rar4.lock().unwrap();
        let (entries, pos) = &mut *guard;

        // Check cache.
        for entry in entries.iter() {
            if entry.password == password && entry.salt.as_ref() == salt {
                return (entry.key, entry.iv);
            }
        }

        // Cache miss — derive key.
        let (key, iv) = rar4_derive_key(password, salt);

        let entry = Kdf3Entry {
            password: password.to_string(),
            salt: salt.copied(),
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

    #[cfg(test)]
    pub(crate) fn rar4_cached_entry_count(&self) -> usize {
        self.rar4.lock().unwrap().0.len()
    }

    #[cfg(test)]
    pub(crate) fn rar5_cached_entry_count(&self) -> usize {
        self.rar5.lock().unwrap().0.len()
    }
}

impl Default for KdfCache {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Legacy RAR4 file encryption and RAR30 AES key derivation
// =============================================================================

fn crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    for (index, entry) in table.iter_mut().enumerate() {
        let mut crc = index as u32;
        for _ in 0..8 {
            crc = if (crc & 1) != 0 {
                0xEDB8_8320 ^ (crc >> 1)
            } else {
                crc >> 1
            };
        }
        *entry = crc;
    }
    table
}

fn raw_get_u32(data: &[u8]) -> u32 {
    u32::from_le_bytes([data[0], data[1], data[2], data[3]])
}

fn raw_put_u32(value: u32, data: &mut [u8]) {
    data[..4].copy_from_slice(&value.to_le_bytes());
}

#[derive(Clone)]
struct Rar13Decryptor {
    key: [u8; 3],
}

impl Rar13Decryptor {
    fn new(password: &str) -> Self {
        let password = rar_password_compat(password);
        Self::new_from_bytes(password.as_bytes())
    }

    fn new_dos(password: &str) -> Self {
        let mut password = rar_password_oem_bytes_compat(password);
        let decryptor = Self::new_from_bytes(&password);
        password.zeroize();
        decryptor
    }

    fn new_from_bytes(password: &[u8]) -> Self {
        let mut key = [0u8; 3];
        for &byte in password {
            key[0] = key[0].wrapping_add(byte);
            key[1] ^= byte;
            key[2] = key[2].wrapping_add(byte).rotate_left(1);
        }
        Self { key }
    }

    fn new_comment() -> Self {
        Self { key: [0, 7, 77] }
    }

    fn decrypt(&mut self, data: &mut [u8]) {
        for byte in data {
            self.key[1] = self.key[1].wrapping_add(self.key[2]);
            self.key[0] = self.key[0].wrapping_add(self.key[1]);
            *byte = byte.wrapping_sub(self.key[0]);
        }
    }
}

pub(crate) fn decrypt_rar14_packed_comment(data: &mut [u8]) {
    Rar13Decryptor::new_comment().decrypt(data);
}

impl Drop for Rar13Decryptor {
    fn drop(&mut self) {
        self.key.zeroize();
    }
}

#[derive(Clone)]
struct Rar15Decryptor {
    key: [u16; 4],
    crc_tab: [u32; 256],
}

impl Rar15Decryptor {
    fn new(password: &str) -> Self {
        let password = rar_password_compat(password);
        Self::new_from_bytes(password.as_bytes())
    }

    fn new_dos(password: &str) -> Self {
        let mut password = rar_password_oem_bytes_compat(password);
        let decryptor = Self::new_from_bytes(&password);
        password.zeroize();
        decryptor
    }

    fn new_from_bytes(password: &[u8]) -> Self {
        let crc_tab = crc32_table();
        let psw_crc = !crc32fast::hash(password);
        let mut key = [(psw_crc & 0xFFFF) as u16, (psw_crc >> 16) as u16, 0, 0];

        for &byte in password {
            key[2] ^= (byte as u16) ^ (crc_tab[byte as usize] as u16);
            key[3] = key[3].wrapping_add(byte as u16 + ((crc_tab[byte as usize] >> 16) as u16));
        }

        Self { key, crc_tab }
    }

    fn decrypt(&mut self, data: &mut [u8]) {
        for byte in data {
            self.key[0] = self.key[0].wrapping_add(0x1234);
            let crc = self.crc_tab[((self.key[0] & 0x01FE) >> 1) as usize];
            self.key[1] ^= crc as u16;
            self.key[2] = self.key[2].wrapping_sub((crc >> 16) as u16);
            self.key[0] ^= self.key[2];
            self.key[3] = self.key[3].rotate_right(1) ^ self.key[1];
            self.key[3] = self.key[3].rotate_right(1);
            self.key[0] ^= self.key[3];
            *byte ^= (self.key[0] >> 8) as u8;
        }
    }
}

impl Drop for Rar15Decryptor {
    fn drop(&mut self) {
        self.key.zeroize();
    }
}

#[derive(Clone)]
struct Rar20Decryptor {
    key: [u32; 4],
    subst: [u8; 256],
    crc_tab: [u32; 256],
}

impl Rar20Decryptor {
    fn new(password: &str) -> Self {
        let password = rar_password_compat(password);
        Self::new_from_bytes(password.as_bytes())
    }

    fn new_dos(password: &str) -> Self {
        let mut password = rar_password_oem_bytes_compat(password);
        let decryptor = Self::new_from_bytes(&password);
        password.zeroize();
        decryptor
    }

    fn new_from_bytes(pwd_bytes: &[u8]) -> Self {
        const INIT_SUBST_TABLE20: [u8; 256] = [
            215, 19, 149, 35, 73, 197, 192, 205, 249, 28, 16, 119, 48, 221, 2, 42, 232, 1, 177,
            233, 14, 88, 219, 25, 223, 195, 244, 90, 87, 239, 153, 137, 255, 199, 147, 70, 92, 66,
            246, 13, 216, 40, 62, 29, 217, 230, 86, 6, 71, 24, 171, 196, 101, 113, 218, 123, 93,
            91, 163, 178, 202, 67, 44, 235, 107, 250, 75, 234, 49, 167, 125, 211, 83, 114, 157,
            144, 32, 193, 143, 36, 158, 124, 247, 187, 89, 214, 141, 47, 121, 228, 61, 130, 213,
            194, 174, 251, 97, 110, 54, 229, 115, 57, 152, 94, 105, 243, 212, 55, 209, 245, 63, 11,
            164, 200, 31, 156, 81, 176, 227, 21, 76, 99, 139, 188, 127, 17, 248, 51, 207, 120, 189,
            210, 8, 226, 41, 72, 183, 203, 135, 165, 166, 60, 98, 7, 122, 38, 155, 170, 69, 172,
            252, 238, 39, 134, 59, 128, 236, 27, 240, 80, 131, 3, 85, 206, 145, 79, 154, 142, 159,
            220, 201, 133, 74, 64, 20, 129, 224, 185, 138, 103, 173, 182, 43, 34, 254, 82, 198,
            151, 231, 180, 58, 10, 118, 26, 102, 12, 50, 132, 22, 191, 136, 111, 162, 179, 45, 4,
            148, 108, 161, 56, 78, 126, 242, 222, 15, 175, 146, 23, 33, 241, 181, 190, 77, 225, 0,
            46, 169, 186, 68, 95, 237, 65, 53, 208, 253, 168, 9, 18, 100, 52, 116, 184, 160, 96,
            109, 37, 30, 106, 140, 104, 150, 5, 204, 117, 112, 84,
        ];

        let crc_tab = crc32_table();
        let key = [0xD3A3_B879, 0x3F6D_12F7, 0x7515_A235, 0xA4E7_F123];
        let mut subst = INIT_SUBST_TABLE20;

        for j in 0..256u32 {
            let mut i = 0usize;
            while i < pwd_bytes.len() {
                let left = pwd_bytes[i];
                let right = pwd_bytes.get(i + 1).copied().unwrap_or(0);
                let mut n1 = (crc_tab[left.wrapping_sub(j as u8) as usize] & 0xFF) as u8;
                let n2 = (crc_tab[right.wrapping_add(j as u8) as usize] & 0xFF) as u8;
                let mut k = 1usize;
                while n1 != n2 {
                    let swap_index = (n1 as usize + i + k) & 0xFF;
                    subst.swap(n1 as usize, swap_index);
                    n1 = n1.wrapping_add(1);
                    k += 1;
                }
                i += 2;
            }
        }

        let mut padded = pwd_bytes.to_vec();
        let remainder = padded.len() & (AES_BLOCK - 1);
        if remainder != 0 {
            padded.resize((padded.len() + AES_BLOCK - 1) & !(AES_BLOCK - 1), 0);
        }

        let mut decryptor = Self {
            key,
            subst,
            crc_tab,
        };
        for chunk in padded.chunks_exact_mut(AES_BLOCK) {
            decryptor.encrypt_block(chunk);
        }
        padded.zeroize();
        decryptor
    }

    fn subst_long(&self, value: u32) -> u32 {
        self.subst[(value & 0xFF) as usize] as u32
            | ((self.subst[((value >> 8) & 0xFF) as usize] as u32) << 8)
            | ((self.subst[((value >> 16) & 0xFF) as usize] as u32) << 16)
            | ((self.subst[((value >> 24) & 0xFF) as usize] as u32) << 24)
    }

    fn update_keys(&mut self, data: &[u8; AES_BLOCK]) {
        for chunk in data.chunks_exact(4) {
            self.key[0] ^= self.crc_tab[chunk[0] as usize];
            self.key[1] ^= self.crc_tab[chunk[1] as usize];
            self.key[2] ^= self.crc_tab[chunk[2] as usize];
            self.key[3] ^= self.crc_tab[chunk[3] as usize];
        }
    }

    fn encrypt_block(&mut self, block: &mut [u8]) {
        const NROUNDS: usize = 32;

        let mut a = raw_get_u32(&block[0..4]) ^ self.key[0];
        let mut b = raw_get_u32(&block[4..8]) ^ self.key[1];
        let mut c = raw_get_u32(&block[8..12]) ^ self.key[2];
        let mut d = raw_get_u32(&block[12..16]) ^ self.key[3];

        for round in 0..NROUNDS {
            let t = (c.wrapping_add(d.rotate_left(11))) ^ self.key[round & 3];
            let ta = a ^ self.subst_long(t);
            let t = (d ^ c.rotate_left(17)).wrapping_add(self.key[round & 3]);
            let tb = b ^ self.subst_long(t);
            a = c;
            b = d;
            c = ta;
            d = tb;
        }

        raw_put_u32(c ^ self.key[0], &mut block[0..4]);
        raw_put_u32(d ^ self.key[1], &mut block[4..8]);
        raw_put_u32(a ^ self.key[2], &mut block[8..12]);
        raw_put_u32(b ^ self.key[3], &mut block[12..16]);

        let mut ciphertext = [0u8; AES_BLOCK];
        ciphertext.copy_from_slice(block);
        self.update_keys(&ciphertext);
    }

    fn decrypt_block(&mut self, block: &mut [u8]) {
        const NROUNDS: i32 = 32;

        let mut ciphertext = [0u8; AES_BLOCK];
        ciphertext.copy_from_slice(block);

        let mut a = raw_get_u32(&block[0..4]) ^ self.key[0];
        let mut b = raw_get_u32(&block[4..8]) ^ self.key[1];
        let mut c = raw_get_u32(&block[8..12]) ^ self.key[2];
        let mut d = raw_get_u32(&block[12..16]) ^ self.key[3];

        for round in (0..NROUNDS).rev() {
            let t = (c.wrapping_add(d.rotate_left(11))) ^ self.key[(round as usize) & 3];
            let ta = a ^ self.subst_long(t);
            let t = (d ^ c.rotate_left(17)).wrapping_add(self.key[(round as usize) & 3]);
            let tb = b ^ self.subst_long(t);
            a = c;
            b = d;
            c = ta;
            d = tb;
        }

        raw_put_u32(c ^ self.key[0], &mut block[0..4]);
        raw_put_u32(d ^ self.key[1], &mut block[4..8]);
        raw_put_u32(a ^ self.key[2], &mut block[8..12]);
        raw_put_u32(b ^ self.key[3], &mut block[12..16]);
        self.update_keys(&ciphertext);
    }
}

impl Drop for Rar20Decryptor {
    fn drop(&mut self) {
        self.key.zeroize();
        self.subst.zeroize();
    }
}

/// RAR4 key derivation iteration count.
const RAR4_KDF_ITERATIONS: u32 = 0x40000; // 262144

#[derive(Clone)]
struct Rar29Sha1 {
    state: [u32; 5],
    buffer: [u8; 64],
    count: u64,
}

impl Drop for Rar29Sha1 {
    fn drop(&mut self) {
        self.state.zeroize();
        self.buffer.zeroize();
        self.count.zeroize();
    }
}

impl Rar29Sha1 {
    fn new() -> Self {
        Self {
            state: [
                0x6745_2301,
                0xefcd_ab89,
                0x98ba_dcfe,
                0x1032_5476,
                0xc3d2_e1f0,
            ],
            buffer: [0; 64],
            count: 0,
        }
    }

    fn process(&mut self, data: &[u8]) {
        let mut i = 0usize;
        let mut j = (self.count & 63) as usize;
        self.count += data.len() as u64;

        if j + data.len() > 63 {
            i = 64 - j;
            self.buffer[j..64].copy_from_slice(&data[..i]);
            self.transform_buffer();

            while i + 63 < data.len() {
                self.transform_block((&data[i..i + 64]).try_into().unwrap());
                i += 64;
            }
            j = 0;
        }

        if data.len() > i {
            let len = data.len() - i;
            self.buffer[j..j + len].copy_from_slice(&data[i..]);
        }
    }

    fn process_rar29(&mut self, data: &mut [u8]) {
        let mut i = 0usize;
        let mut j = (self.count & 63) as usize;
        self.count += data.len() as u64;

        if j + data.len() > 63 {
            i = 64 - j;
            self.buffer[j..64].copy_from_slice(&data[..i]);
            self.transform_buffer();

            while i + 63 < data.len() {
                let workspace = self.transform_block((&data[i..i + 64]).try_into().unwrap());
                for (chunk, word) in data[i..i + 64].chunks_exact_mut(4).zip(workspace) {
                    chunk.copy_from_slice(&word.to_le_bytes());
                }
                i += 64;
            }
            j = 0;
        }

        if data.len() > i {
            let len = data.len() - i;
            self.buffer[j..j + len].copy_from_slice(&data[i..]);
        }
    }

    fn finish_words(mut self) -> [u32; 5] {
        let bit_length = self.count * 8;
        let mut buf_pos = (self.count & 63) as usize;
        self.buffer[buf_pos] = 0x80;
        buf_pos += 1;

        if buf_pos != 56 {
            if buf_pos > 56 {
                self.buffer[buf_pos..64].fill(0);
                self.transform_buffer();
                buf_pos = 0;
            }
            self.buffer[buf_pos..56].fill(0);
        }

        self.buffer[56..60].copy_from_slice(&((bit_length >> 32) as u32).to_be_bytes());
        self.buffer[60..64].copy_from_slice(&(bit_length as u32).to_be_bytes());
        self.transform_buffer();
        self.state
    }

    fn transform_buffer(&mut self) -> [u32; 16] {
        let block = self.buffer;
        self.transform_block(&block)
    }

    /// One SHA-1 block. Returns the final 16 message-schedule words
    /// W[64..80) — `process_rar29` writes them back over the input to
    /// replicate WinRAR's buggy in-place RAR29 SHA-1.
    fn transform_block(&mut self, block: &[u8; 64]) -> [u32; 16] {
        #[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
        if sha1_hw_enabled() {
            // SAFETY: the required target features were verified at runtime.
            return unsafe { sha1_hw::transform_block(&mut self.state, block) };
        }
        self.transform_block_scalar(block)
    }

    fn transform_block_scalar(&mut self, block: &[u8; 64]) -> [u32; 16] {
        let mut workspace = [0u32; 16];
        for (word, chunk) in workspace.iter_mut().zip(block.chunks_exact(4)) {
            *word = u32::from_be_bytes(chunk.try_into().unwrap());
        }

        let [mut a, mut b, mut c, mut d, mut e] = self.state;
        for i in 0..80 {
            let w = if i < 16 {
                workspace[i]
            } else {
                let value = (workspace[(i + 13) & 15]
                    ^ workspace[(i + 8) & 15]
                    ^ workspace[(i + 2) & 15]
                    ^ workspace[i & 15])
                    .rotate_left(1);
                workspace[i & 15] = value;
                value
            };
            let (f, k) = match i {
                0..=19 => (((b & (c ^ d)) ^ d), 0x5a82_7999),
                20..=39 => (b ^ c ^ d, 0x6ed9_eba1),
                40..=59 => ((((b | c) & d) | (b & c)), 0x8f1b_bcdc),
                _ => (b ^ c ^ d, 0xca62_c1d6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(w);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }

        self.state[0] = self.state[0].wrapping_add(a);
        self.state[1] = self.state[1].wrapping_add(b);
        self.state[2] = self.state[2].wrapping_add(c);
        self.state[3] = self.state[3].wrapping_add(d);
        self.state[4] = self.state[4].wrapping_add(e);

        workspace
    }
}

#[cfg(target_arch = "aarch64")]
fn sha1_hw_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| std::arch::is_aarch64_feature_detected!("sha2"))
}

#[cfg(target_arch = "x86_64")]
fn sha1_hw_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::arch::is_x86_feature_detected!("sha")
            && std::arch::is_x86_feature_detected!("sse4.1")
            && std::arch::is_x86_feature_detected!("ssse3")
    })
}

/// x86 SHA-extension (SHA-NI) SHA-1 block transform — the x86 twin of the
/// aarch64 module below; see its docs for the schedule-writeback contract.
#[cfg(target_arch = "x86_64")]
mod sha1_hw {
    use std::arch::x86_64::*;

    /// Process one 64-byte block, updating `state` and returning the final
    /// 16 message-schedule words W[64..80).
    #[target_feature(enable = "sha,sse4.1,ssse3")]
    pub fn transform_block(state: &mut [u32; 5], block: &[u8; 64]) -> [u32; 16] {
        unsafe {
            // Byte shuffle producing big-endian dwords with W0 in the high
            // lane: each message register holds [W3, W2, W1, W0].
            let mask = _mm_set_epi64x(0x0001_0203_0405_0607, 0x0809_0a0b_0c0d_0e0f);

            let mut abcd = _mm_loadu_si128(state.as_ptr() as *const __m128i);
            abcd = _mm_shuffle_epi32::<0x1B>(abcd);
            let mut e0 = _mm_set_epi32(state[4] as i32, 0, 0, 0);
            let abcd_save = abcd;
            let e_save = e0;
            let mut e1;

            let ptr = block.as_ptr() as *const __m128i;
            let mut m0 = _mm_shuffle_epi8(_mm_loadu_si128(ptr), mask);
            let mut m1 = _mm_shuffle_epi8(_mm_loadu_si128(ptr.add(1)), mask);
            let mut m2 = _mm_shuffle_epi8(_mm_loadu_si128(ptr.add(2)), mask);
            let mut m3 = _mm_shuffle_epi8(_mm_loadu_si128(ptr.add(3)), mask);

            // Rounds 0-3
            e0 = _mm_add_epi32(e0, m0);
            e1 = abcd;
            abcd = _mm_sha1rnds4_epu32::<0>(abcd, e0);
            // Rounds 4-7
            e1 = _mm_sha1nexte_epu32(e1, m1);
            e0 = abcd;
            abcd = _mm_sha1rnds4_epu32::<0>(abcd, e1);
            m0 = _mm_sha1msg1_epu32(m0, m1);
            // Rounds 8-11
            e0 = _mm_sha1nexte_epu32(e0, m2);
            e1 = abcd;
            abcd = _mm_sha1rnds4_epu32::<0>(abcd, e0);
            m1 = _mm_sha1msg1_epu32(m1, m2);
            m0 = _mm_xor_si128(m0, m2);
            // Rounds 12-15
            e1 = _mm_sha1nexte_epu32(e1, m3);
            e0 = abcd;
            m0 = _mm_sha1msg2_epu32(m0, m3);
            abcd = _mm_sha1rnds4_epu32::<0>(abcd, e1);
            m2 = _mm_sha1msg1_epu32(m2, m3);
            m1 = _mm_xor_si128(m1, m3);
            // Rounds 16-19
            e0 = _mm_sha1nexte_epu32(e0, m0);
            e1 = abcd;
            m1 = _mm_sha1msg2_epu32(m1, m0);
            abcd = _mm_sha1rnds4_epu32::<0>(abcd, e0);
            m3 = _mm_sha1msg1_epu32(m3, m0);
            m2 = _mm_xor_si128(m2, m0);
            // Rounds 20-23
            e1 = _mm_sha1nexte_epu32(e1, m1);
            e0 = abcd;
            m2 = _mm_sha1msg2_epu32(m2, m1);
            abcd = _mm_sha1rnds4_epu32::<1>(abcd, e1);
            m0 = _mm_sha1msg1_epu32(m0, m1);
            m3 = _mm_xor_si128(m3, m1);
            // Rounds 24-27
            e0 = _mm_sha1nexte_epu32(e0, m2);
            e1 = abcd;
            m3 = _mm_sha1msg2_epu32(m3, m2);
            abcd = _mm_sha1rnds4_epu32::<1>(abcd, e0);
            m1 = _mm_sha1msg1_epu32(m1, m2);
            m0 = _mm_xor_si128(m0, m2);
            // Rounds 28-31
            e1 = _mm_sha1nexte_epu32(e1, m3);
            e0 = abcd;
            m0 = _mm_sha1msg2_epu32(m0, m3);
            abcd = _mm_sha1rnds4_epu32::<1>(abcd, e1);
            m2 = _mm_sha1msg1_epu32(m2, m3);
            m1 = _mm_xor_si128(m1, m3);
            // Rounds 32-35
            e0 = _mm_sha1nexte_epu32(e0, m0);
            e1 = abcd;
            m1 = _mm_sha1msg2_epu32(m1, m0);
            abcd = _mm_sha1rnds4_epu32::<1>(abcd, e0);
            m3 = _mm_sha1msg1_epu32(m3, m0);
            m2 = _mm_xor_si128(m2, m0);
            // Rounds 36-39
            e1 = _mm_sha1nexte_epu32(e1, m1);
            e0 = abcd;
            m2 = _mm_sha1msg2_epu32(m2, m1);
            abcd = _mm_sha1rnds4_epu32::<1>(abcd, e1);
            m0 = _mm_sha1msg1_epu32(m0, m1);
            m3 = _mm_xor_si128(m3, m1);
            // Rounds 40-43
            e0 = _mm_sha1nexte_epu32(e0, m2);
            e1 = abcd;
            m3 = _mm_sha1msg2_epu32(m3, m2);
            abcd = _mm_sha1rnds4_epu32::<2>(abcd, e0);
            m1 = _mm_sha1msg1_epu32(m1, m2);
            m0 = _mm_xor_si128(m0, m2);
            // Rounds 44-47
            e1 = _mm_sha1nexte_epu32(e1, m3);
            e0 = abcd;
            m0 = _mm_sha1msg2_epu32(m0, m3);
            abcd = _mm_sha1rnds4_epu32::<2>(abcd, e1);
            m2 = _mm_sha1msg1_epu32(m2, m3);
            m1 = _mm_xor_si128(m1, m3);
            // Rounds 48-51
            e0 = _mm_sha1nexte_epu32(e0, m0);
            e1 = abcd;
            m1 = _mm_sha1msg2_epu32(m1, m0);
            abcd = _mm_sha1rnds4_epu32::<2>(abcd, e0);
            m3 = _mm_sha1msg1_epu32(m3, m0);
            m2 = _mm_xor_si128(m2, m0);
            // Rounds 52-55
            e1 = _mm_sha1nexte_epu32(e1, m1);
            e0 = abcd;
            m2 = _mm_sha1msg2_epu32(m2, m1);
            abcd = _mm_sha1rnds4_epu32::<2>(abcd, e1);
            m0 = _mm_sha1msg1_epu32(m0, m1);
            m3 = _mm_xor_si128(m3, m1);
            // Rounds 56-59
            e0 = _mm_sha1nexte_epu32(e0, m2);
            e1 = abcd;
            m3 = _mm_sha1msg2_epu32(m3, m2);
            abcd = _mm_sha1rnds4_epu32::<2>(abcd, e0);
            m1 = _mm_sha1msg1_epu32(m1, m2);
            m0 = _mm_xor_si128(m0, m2);
            // Rounds 60-63
            e1 = _mm_sha1nexte_epu32(e1, m3);
            e0 = abcd;
            m0 = _mm_sha1msg2_epu32(m0, m3);
            abcd = _mm_sha1rnds4_epu32::<3>(abcd, e1);
            m2 = _mm_sha1msg1_epu32(m2, m3);
            m1 = _mm_xor_si128(m1, m3);
            // Rounds 64-67
            e0 = _mm_sha1nexte_epu32(e0, m0);
            e1 = abcd;
            m1 = _mm_sha1msg2_epu32(m1, m0);
            abcd = _mm_sha1rnds4_epu32::<3>(abcd, e0);
            m3 = _mm_sha1msg1_epu32(m3, m0);
            m2 = _mm_xor_si128(m2, m0);
            // Rounds 68-71
            e1 = _mm_sha1nexte_epu32(e1, m1);
            e0 = abcd;
            m2 = _mm_sha1msg2_epu32(m2, m1);
            abcd = _mm_sha1rnds4_epu32::<3>(abcd, e1);
            m3 = _mm_xor_si128(m3, m1);
            // Rounds 72-75
            e0 = _mm_sha1nexte_epu32(e0, m2);
            e1 = abcd;
            m3 = _mm_sha1msg2_epu32(m3, m2);
            abcd = _mm_sha1rnds4_epu32::<3>(abcd, e0);
            // Rounds 76-79
            e1 = _mm_sha1nexte_epu32(e1, m3);
            e0 = abcd;
            abcd = _mm_sha1rnds4_epu32::<3>(abcd, e1);

            // Combine with saved state.
            e0 = _mm_sha1nexte_epu32(e0, e_save);
            abcd = _mm_add_epi32(abcd, abcd_save);
            abcd = _mm_shuffle_epi32::<0x1B>(abcd);
            _mm_storeu_si128(state.as_mut_ptr() as *mut __m128i, abcd);
            state[4] = _mm_extract_epi32::<3>(e0) as u32;

            // Final schedule words: [W3,W2,W1,W0] lane order per register,
            // so reverse each before storing.
            let mut workspace = [0u32; 16];
            let out = workspace.as_mut_ptr() as *mut __m128i;
            _mm_storeu_si128(out, _mm_shuffle_epi32::<0x1B>(m0));
            _mm_storeu_si128(out.add(1), _mm_shuffle_epi32::<0x1B>(m1));
            _mm_storeu_si128(out.add(2), _mm_shuffle_epi32::<0x1B>(m2));
            _mm_storeu_si128(out.add(3), _mm_shuffle_epi32::<0x1B>(m3));
            workspace
        }
    }
}

/// ARMv8 crypto-extension SHA-1 block transform.
///
/// The RAR29 KDF runs 262,144 iterations, so the block transform dominates
/// key derivation. The schedule the hardware path computes is the standard
/// SHA-1 schedule; WinRAR's bug is only WHERE those words end up (written
/// back over the input), so returning W[64..80) keeps bug-for-bug parity
/// with the scalar path.
#[cfg(target_arch = "aarch64")]
mod sha1_hw {
    use std::arch::aarch64::*;

    /// Process one 64-byte block, updating `state` and returning the final
    /// 16 message-schedule words W[64..80).
    #[target_feature(enable = "sha2")]
    pub fn transform_block(state: &mut [u32; 5], block: &[u8; 64]) -> [u32; 16] {
        unsafe {
            let k0 = vdupq_n_u32(0x5a82_7999);
            let k1 = vdupq_n_u32(0x6ed9_eba1);
            let k2 = vdupq_n_u32(0x8f1b_bcdc);
            let k3 = vdupq_n_u32(0xca62_c1d6);

            let mut m0 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(block.as_ptr())));
            let mut m1 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(block.as_ptr().add(16))));
            let mut m2 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(block.as_ptr().add(32))));
            let mut m3 = vreinterpretq_u32_u8(vrev32q_u8(vld1q_u8(block.as_ptr().add(48))));

            let abcd_saved = vld1q_u32(state.as_ptr());
            let e_saved = state[4];
            let mut abcd = abcd_saved;
            let mut e0 = e_saved;
            let mut e1;

            let mut t0 = vaddq_u32(m0, k0);
            let mut t1 = vaddq_u32(m1, k0);

            // Rounds 0-3
            e1 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1cq_u32(abcd, e0, t0);
            t0 = vaddq_u32(m2, k0);
            m0 = vsha1su0q_u32(m0, m1, m2);
            // Rounds 4-7
            e0 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1cq_u32(abcd, e1, t1);
            t1 = vaddq_u32(m3, k0);
            m0 = vsha1su1q_u32(m0, m3);
            m1 = vsha1su0q_u32(m1, m2, m3);
            // Rounds 8-11
            e1 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1cq_u32(abcd, e0, t0);
            t0 = vaddq_u32(m0, k0);
            m1 = vsha1su1q_u32(m1, m0);
            m2 = vsha1su0q_u32(m2, m3, m0);
            // Rounds 12-15
            e0 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1cq_u32(abcd, e1, t1);
            t1 = vaddq_u32(m1, k1);
            m2 = vsha1su1q_u32(m2, m1);
            m3 = vsha1su0q_u32(m3, m0, m1);
            // Rounds 16-19
            e1 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1cq_u32(abcd, e0, t0);
            t0 = vaddq_u32(m2, k1);
            m3 = vsha1su1q_u32(m3, m2);
            m0 = vsha1su0q_u32(m0, m1, m2);
            // Rounds 20-23
            e0 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1pq_u32(abcd, e1, t1);
            t1 = vaddq_u32(m3, k1);
            m0 = vsha1su1q_u32(m0, m3);
            m1 = vsha1su0q_u32(m1, m2, m3);
            // Rounds 24-27
            e1 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1pq_u32(abcd, e0, t0);
            t0 = vaddq_u32(m0, k1);
            m1 = vsha1su1q_u32(m1, m0);
            m2 = vsha1su0q_u32(m2, m3, m0);
            // Rounds 28-31
            e0 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1pq_u32(abcd, e1, t1);
            t1 = vaddq_u32(m1, k1);
            m2 = vsha1su1q_u32(m2, m1);
            m3 = vsha1su0q_u32(m3, m0, m1);
            // Rounds 32-35
            e1 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1pq_u32(abcd, e0, t0);
            t0 = vaddq_u32(m2, k2);
            m3 = vsha1su1q_u32(m3, m2);
            m0 = vsha1su0q_u32(m0, m1, m2);
            // Rounds 36-39
            e0 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1pq_u32(abcd, e1, t1);
            t1 = vaddq_u32(m3, k2);
            m0 = vsha1su1q_u32(m0, m3);
            m1 = vsha1su0q_u32(m1, m2, m3);
            // Rounds 40-43
            e1 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1mq_u32(abcd, e0, t0);
            t0 = vaddq_u32(m0, k2);
            m1 = vsha1su1q_u32(m1, m0);
            m2 = vsha1su0q_u32(m2, m3, m0);
            // Rounds 44-47
            e0 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1mq_u32(abcd, e1, t1);
            t1 = vaddq_u32(m1, k2);
            m2 = vsha1su1q_u32(m2, m1);
            m3 = vsha1su0q_u32(m3, m0, m1);
            // Rounds 48-51
            e1 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1mq_u32(abcd, e0, t0);
            t0 = vaddq_u32(m2, k2);
            m3 = vsha1su1q_u32(m3, m2);
            m0 = vsha1su0q_u32(m0, m1, m2);
            // Rounds 52-55
            e0 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1mq_u32(abcd, e1, t1);
            t1 = vaddq_u32(m3, k3);
            m0 = vsha1su1q_u32(m0, m3);
            m1 = vsha1su0q_u32(m1, m2, m3);
            // Rounds 56-59
            e1 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1mq_u32(abcd, e0, t0);
            t0 = vaddq_u32(m0, k3);
            m1 = vsha1su1q_u32(m1, m0);
            m2 = vsha1su0q_u32(m2, m3, m0);
            // Rounds 60-63
            e0 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1pq_u32(abcd, e1, t1);
            t1 = vaddq_u32(m1, k3);
            m2 = vsha1su1q_u32(m2, m1);
            m3 = vsha1su0q_u32(m3, m0, m1);
            // Rounds 64-67
            e1 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1pq_u32(abcd, e0, t0);
            t0 = vaddq_u32(m2, k3);
            m3 = vsha1su1q_u32(m3, m2);
            // Rounds 68-71
            e0 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1pq_u32(abcd, e1, t1);
            t1 = vaddq_u32(m3, k3);
            // Rounds 72-75
            e1 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1pq_u32(abcd, e0, t0);
            // Rounds 76-79
            e0 = vsha1h_u32(vgetq_lane_u32::<0>(abcd));
            abcd = vsha1pq_u32(abcd, e1, t1);

            let mut new_state = [0u32; 5];
            vst1q_u32(new_state.as_mut_ptr(), vaddq_u32(abcd_saved, abcd));
            new_state[4] = e_saved.wrapping_add(e0);
            *state = new_state;

            // Final schedule words: m0..m3 hold W[64..80) after their last
            // su0/su1 updates.
            let mut workspace = [0u32; 16];
            vst1q_u32(workspace.as_mut_ptr(), m0);
            vst1q_u32(workspace.as_mut_ptr().add(4), m1);
            vst1q_u32(workspace.as_mut_ptr().add(8), m2);
            vst1q_u32(workspace.as_mut_ptr().add(12), m3);
            workspace
        }
    }
}

/// Derive AES-128 key and IV from password and salt using RAR4's custom KDF.
///
/// RAR4 KDF algorithm:
/// - Encodes password as UTF-16LE
/// - Concatenates password_utf16le + salt into a single buffer
/// - Iterates 262144 times, each time hashing: buffer + 3-byte iteration counter
/// - At iterations 0, 16384, 32768, ... (i.e. `i % (262144/16) == 0`), the current
///   SHA-1 intermediate digest word H4's low byte is extracted as an IV byte
/// - After all iterations, the final SHA-1 digest words H0-H3 are extracted as the
///   AES-128 key in little-endian byte order per word
fn rar4_derive_key_material(password: &str, salt: Option<&[u8; 8]>) -> ([u8; 16], [u8; 16]) {
    let password = rar_password_compat(password);
    // Encode password as UTF-16LE, then append salt if present for both salted
    // and saltless RAR30 members.
    let mut raw_psw: Vec<u8> = password
        .encode_utf16()
        .flat_map(|c| c.to_le_bytes())
        .collect();
    if let Some(salt) = salt {
        raw_psw.extend_from_slice(salt);
    }

    let iv_interval = RAR4_KDF_ITERATIONS / 16;
    let mut iv = [0u8; 16];
    let mut sha = Rar29Sha1::new();

    for i in 0..RAR4_KDF_ITERATIONS {
        sha.process_rar29(&mut raw_psw);

        // Append iteration counter as 3 bytes LE.
        let i_bytes = [i as u8, (i >> 8) as u8, (i >> 16) as u8];
        sha.process(&i_bytes);

        // Extract one IV byte at each interval boundary.
        if i % iv_interval == 0 {
            let intermediate = sha.clone().finish_words();
            let iv_index = (i / iv_interval) as usize;
            iv[iv_index] = intermediate[4] as u8;
        }
    }

    let mut digest = sha.finish_words();

    // RAR4 stores key bytes in little-endian order per 32-bit digest word.
    let mut key = [0u8; 16];
    for word in 0..4 {
        key[word * 4..word * 4 + 4].copy_from_slice(&digest[word].to_le_bytes());
    }

    let result = (key, iv);
    raw_psw.zeroize();
    digest.zeroize();
    key.zeroize();
    iv.zeroize();

    result
}

pub fn rar4_derive_key(password: &str, salt: Option<&[u8; 8]>) -> ([u8; 16], [u8; 16]) {
    rar4_derive_key_material(password, salt)
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
    let mut decryptor = Rar4CbcDecryptor::new(key, iv);
    decryptor.decrypt_blocks(&mut buf);

    Ok(buf)
}

// =============================================================================
// Streaming AES-CBC decryption
// =============================================================================

const AES_BLOCK: usize = 16;

#[cfg(test)]
fn encrypt_cbc_for_test(
    cipher: *const EVP_CIPHER,
    key: &[u8],
    iv: &[u8; AES_BLOCK],
    plaintext: &[u8],
) -> Vec<u8> {
    assert!(plaintext.len().is_multiple_of(AES_BLOCK));
    assert!(plaintext.len() <= i32::MAX as usize);

    let ctx = unsafe { EVP_CIPHER_CTX_new() };
    assert!(!ctx.is_null(), "aws-lc EVP_CIPHER_CTX_new must succeed");

    let init = unsafe { EVP_EncryptInit_ex(ctx, cipher, null_mut(), key.as_ptr(), iv.as_ptr()) };
    assert_eq!(init, 1, "aws-lc EVP_EncryptInit_ex must succeed");

    let no_padding = unsafe { EVP_CIPHER_CTX_set_padding(ctx, 0) };
    assert_eq!(
        no_padding, 1,
        "aws-lc EVP_CIPHER_CTX_set_padding(0) must succeed"
    );

    let mut ciphertext = vec![0u8; plaintext.len()];
    let mut out_len = 0_i32;
    let result = unsafe {
        EVP_EncryptUpdate(
            ctx,
            ciphertext.as_mut_ptr(),
            &mut out_len,
            plaintext.as_ptr(),
            plaintext.len() as i32,
        )
    };
    unsafe { EVP_CIPHER_CTX_free(ctx) };

    assert_eq!(result, 1, "aws-lc EVP_EncryptUpdate must succeed");
    assert_eq!(
        out_len as usize,
        plaintext.len(),
        "aws-lc CBC encrypt must write the full block-aligned input"
    );
    ciphertext
}

#[cfg(test)]
pub(crate) fn encrypt_aes128_cbc_for_test(
    key: &[u8; 16],
    iv: &[u8; AES_BLOCK],
    plaintext: &[u8],
) -> Vec<u8> {
    encrypt_cbc_for_test(unsafe { EVP_aes_128_cbc() }, key, iv, plaintext)
}

#[cfg(test)]
pub(crate) fn encrypt_aes256_cbc_for_test(
    key: &[u8; 32],
    iv: &[u8; AES_BLOCK],
    plaintext: &[u8],
) -> Vec<u8> {
    encrypt_cbc_for_test(unsafe { EVP_aes_256_cbc() }, key, iv, plaintext)
}

struct AwsLcCbcDecryptor {
    ctx: *mut EVP_CIPHER_CTX,
}

const AWS_LC_MAX_DECRYPT_UPDATE_LEN: usize = (i32::MAX as usize / AES_BLOCK) * AES_BLOCK;

unsafe impl Send for AwsLcCbcDecryptor {}

impl AwsLcCbcDecryptor {
    fn new_aes256(key: &[u8; 32], iv: &[u8; AES_BLOCK]) -> Self {
        Self::new(unsafe { EVP_aes_256_cbc() }, key, iv)
    }

    fn new_aes128(key: &[u8; 16], iv: &[u8; AES_BLOCK]) -> Self {
        Self::new(unsafe { EVP_aes_128_cbc() }, key, iv)
    }

    fn new(cipher: *const EVP_CIPHER, key: &[u8], iv: &[u8; AES_BLOCK]) -> Self {
        let ctx = unsafe { EVP_CIPHER_CTX_new() };
        assert!(!ctx.is_null(), "aws-lc EVP_CIPHER_CTX_new must succeed");

        let init =
            unsafe { EVP_DecryptInit_ex(ctx, cipher, null_mut(), key.as_ptr(), iv.as_ptr()) };
        assert_eq!(init, 1, "aws-lc EVP_DecryptInit_ex must succeed");

        let no_padding = unsafe { EVP_CIPHER_CTX_set_padding(ctx, 0) };
        assert_eq!(
            no_padding, 1,
            "aws-lc EVP_CIPHER_CTX_set_padding(0) must succeed"
        );

        Self { ctx }
    }

    fn decrypt_blocks(&mut self, data: &mut [u8]) {
        debug_assert!(data.len().is_multiple_of(AES_BLOCK));
        for chunk in data.chunks_mut(AWS_LC_MAX_DECRYPT_UPDATE_LEN) {
            let mut out_len = 0_i32;
            let input_len = chunk.len() as i32;
            let result = unsafe {
                EVP_DecryptUpdate(
                    self.ctx,
                    chunk.as_mut_ptr(),
                    &mut out_len,
                    chunk.as_ptr(),
                    input_len,
                )
            };
            assert_eq!(result, 1, "aws-lc EVP_DecryptUpdate must succeed");
            assert!(out_len >= 0, "aws-lc output length must be non-negative");
            assert_eq!(
                out_len as usize,
                chunk.len(),
                "aws-lc CBC decrypt must write the full block-aligned input"
            );
        }
    }
}

impl Drop for AwsLcCbcDecryptor {
    fn drop(&mut self) {
        unsafe { EVP_CIPHER_CTX_free(self.ctx) };
    }
}

/// Stateful AES-256-CBC decryptor for incremental (streaming) decryption.
///
/// Unlike `decrypt_data` which requires all data at once, this carries the
/// CBC IV state across calls to `decrypt_blocks`.
pub struct CbcDecryptor {
    decryptor: AwsLcCbcDecryptor,
}

impl CbcDecryptor {
    pub fn new(key: &[u8; 32], iv: &[u8; AES_BLOCK]) -> Self {
        Self {
            decryptor: AwsLcCbcDecryptor::new_aes256(key, iv),
        }
    }

    /// Decrypt `data` in-place. `data.len()` MUST be a multiple of 16.
    /// Updates internal IV state for subsequent calls.
    pub fn decrypt_blocks(&mut self, data: &mut [u8]) {
        debug_assert!(data.len().is_multiple_of(AES_BLOCK));
        self.decryptor.decrypt_blocks(data);
    }
}

/// Stateful AES-128-CBC decryptor for RAR4 archives.
pub struct Rar4CbcDecryptor {
    decryptor: AwsLcCbcDecryptor,
}

impl Rar4CbcDecryptor {
    pub fn new(key: &[u8; 16], iv: &[u8; AES_BLOCK]) -> Self {
        Self {
            decryptor: AwsLcCbcDecryptor::new_aes128(key, iv),
        }
    }

    /// Decrypt `data` in-place. `data.len()` MUST be a multiple of 16.
    pub fn decrypt_blocks(&mut self, data: &mut [u8]) {
        debug_assert!(data.len().is_multiple_of(AES_BLOCK));
        self.decryptor.decrypt_blocks(data);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DecryptorMode {
    Streaming,
    BlockAligned,
}

/// Decryptor enum that handles RAR5 and all RAR4 file-encryption variants.
enum CbcDecryptorAny {
    Rar5(Box<CbcDecryptor>),
    Rar4(Box<Rar4CbcDecryptor>),
    Rar13(Rar13Decryptor),
    Rar15(Box<Rar15Decryptor>),
    Rar20(Box<Rar20Decryptor>),
}

impl CbcDecryptorAny {
    fn mode(&self) -> DecryptorMode {
        match self {
            Self::Rar13(_) | Self::Rar15(_) => DecryptorMode::Streaming,
            Self::Rar5(_) | Self::Rar4(_) | Self::Rar20(_) => DecryptorMode::BlockAligned,
        }
    }

    pub fn decrypt(&mut self, data: &mut [u8]) {
        match self {
            Self::Rar5(d) => d.decrypt_blocks(data),
            Self::Rar4(d) => d.decrypt_blocks(data),
            Self::Rar13(d) => d.decrypt(data),
            Self::Rar15(d) => d.decrypt(data),
            Self::Rar20(d) => {
                debug_assert!(data.len().is_multiple_of(AES_BLOCK));
                for block in data.chunks_exact_mut(AES_BLOCK) {
                    d.decrypt_block(block);
                }
            }
        }
    }
}

/// Decryption buffer size.
/// Keep this modest because encrypted extraction already pays for the dictionary
/// window and output sink; a very large decrypt staging buffer does not buy much
/// once compressed input is streamed.
const DECRYPT_BUF_SIZE: usize = 64 * 1024;

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

impl<R: Read> Drop for DecryptingReader<R> {
    fn drop(&mut self) {
        self.pending.zeroize();
        self.out_buf.zeroize();
    }
}

impl<R: Read> DecryptingReader<R> {
    fn new_with_decryptor(inner: R, decryptor: CbcDecryptorAny) -> Self {
        Self {
            inner,
            decryptor,
            pending: [0u8; AES_BLOCK],
            pending_len: 0,
            out_buf: vec![0u8; DECRYPT_BUF_SIZE].into_boxed_slice(),
            out_pos: 0,
            out_len: 0,
            inner_eof: false,
        }
    }

    /// Create a new decrypting reader for RAR5 (AES-256-CBC).
    pub fn new_rar5(inner: R, key: &[u8; 32], iv: &[u8; 16]) -> Self {
        Self::new_with_decryptor(
            inner,
            CbcDecryptorAny::Rar5(Box::new(CbcDecryptor::new(key, iv))),
        )
    }

    /// Create a new decrypting reader for RAR4 (AES-128-CBC).
    pub fn new_rar4(inner: R, key: &[u8; 16], iv: &[u8; 16]) -> Self {
        Self::new_with_decryptor(
            inner,
            CbcDecryptorAny::Rar4(Box::new(Rar4CbcDecryptor::new(key, iv))),
        )
    }

    pub fn new_rar4_legacy(inner: R, method: Rar4EncryptionMethod, password: &str) -> Self {
        let decryptor = match method {
            Rar4EncryptionMethod::Rar13 => CbcDecryptorAny::Rar13(Rar13Decryptor::new(password)),
            Rar4EncryptionMethod::Rar15 => {
                CbcDecryptorAny::Rar15(Box::new(Rar15Decryptor::new(password)))
            }
            Rar4EncryptionMethod::Rar20 => {
                CbcDecryptorAny::Rar20(Box::new(Rar20Decryptor::new(password)))
            }
            Rar4EncryptionMethod::Rar30 => unreachable!("RAR30 must use AES constructor"),
        };
        Self::new_with_decryptor(inner, decryptor)
    }

    pub(crate) fn new_rar4_legacy_dos(
        inner: R,
        method: Rar4EncryptionMethod,
        password: &str,
    ) -> Self {
        let decryptor = match method {
            Rar4EncryptionMethod::Rar13 => {
                CbcDecryptorAny::Rar13(Rar13Decryptor::new_dos(password))
            }
            Rar4EncryptionMethod::Rar15 => {
                CbcDecryptorAny::Rar15(Box::new(Rar15Decryptor::new_dos(password)))
            }
            Rar4EncryptionMethod::Rar20 => {
                CbcDecryptorAny::Rar20(Box::new(Rar20Decryptor::new_dos(password)))
            }
            Rar4EncryptionMethod::Rar30 => unreachable!("RAR30 must use AES constructor"),
        };
        Self::new_with_decryptor(inner, decryptor)
    }
}

impl<R: Read> Read for DecryptingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.decryptor.mode() == DecryptorMode::Streaming {
            let read = self.inner.read(buf)?;
            if read == 0 {
                return Ok(0);
            }
            self.decryptor.decrypt(&mut buf[..read]);
            return Ok(read);
        }

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
        self.decryptor.decrypt(&mut self.out_buf[..complete]);
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

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    /// The hardware SHA-1 path must match the scalar path exactly — both the
    /// digest state and the returned final schedule words that feed the
    /// RAR29 in-place corruption.
    #[cfg(any(target_arch = "aarch64", target_arch = "x86_64"))]
    #[test]
    fn rar29_sha1_hw_transform_matches_scalar() {
        if !sha1_hw_enabled() {
            eprintln!("skipping: SHA extensions not available");
            return;
        }

        let mut seed = 0x1234_5678_9abc_def0u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };

        for round in 0..4096 {
            let mut block = [0u8; 64];
            for chunk in block.chunks_exact_mut(8) {
                chunk.copy_from_slice(&next().to_le_bytes());
            }
            let mut state = [0u32; 5];
            for word in &mut state {
                *word = next() as u32;
            }

            let mut scalar = Rar29Sha1::new();
            scalar.state = state;
            let mut hw = Rar29Sha1::new();
            hw.state = state;

            let ws_scalar = scalar.transform_block_scalar(&block);
            // SAFETY: sha2 availability checked above.
            let ws_hw = unsafe { sha1_hw::transform_block(&mut hw.state, &block) };

            assert_eq!(scalar.state, hw.state, "state diverged at round {round}");
            assert_eq!(ws_scalar, ws_hw, "schedule diverged at round {round}");
        }
    }

    #[test]
    fn test_rar_password_compat_matches_rar_behavior_limit() {
        let exact = "a".repeat(RAR_PASSWORD_MAX_UNITS);
        assert!(matches!(rar_password_compat(&exact), Cow::Borrowed(_)));

        let too_long = format!("{exact}tail");
        assert_eq!(rar_password_compat(&too_long).as_ref(), exact);

        let with_two_unit_scalar = format!("{}😀x", "a".repeat(RAR_PASSWORD_MAX_UNITS - 2));
        let expected = format!("{}😀", "a".repeat(RAR_PASSWORD_MAX_UNITS - 2));
        assert_eq!(
            rar_password_compat(&with_two_unit_scalar).as_ref(),
            expected
        );
    }

    #[test]
    fn test_rar_dos_password_compat_uses_cp437_oem_bytes() {
        assert_eq!(rar_password_oem_bytes_compat("Grüße"), b"Gr\x81\xe1e");
        assert_eq!(rar_password_oem_bytes_compat("€"), b"?");
    }

    #[test]
    fn test_rar_dos_password_compat_truncates_before_oem_encoding() {
        let prefix = "a".repeat(RAR_PASSWORD_MAX_UNITS);
        let long = format!("{prefix}ü");

        assert_eq!(rar_password_oem_bytes_compat(&long), prefix.as_bytes());
    }

    #[test]
    fn test_rar5_kdf_ignores_password_tail_after_rar_behavior_limit() {
        let salt = [0x5Au8; 16];
        let prefix = "p".repeat(RAR_PASSWORD_MAX_UNITS);
        let material = derive_rar5_material(&prefix, &salt, 0).unwrap();

        assert_eq!(
            derive_rar5_material(&format!("{prefix}a"), &salt, 0).unwrap(),
            material
        );
        assert_eq!(
            derive_rar5_material(&format!("{prefix}b"), &salt, 0).unwrap(),
            material
        );
    }

    #[test]
    fn test_rar4_kdf_ignores_password_tail_after_rar_behavior_limit() {
        let salt = [0xA5u8; 8];
        let prefix = "p".repeat(RAR_PASSWORD_MAX_UNITS);
        let expected = rar4_derive_key(&prefix, Some(&salt));

        assert_eq!(
            rar4_derive_key(&format!("{prefix}a"), Some(&salt)),
            expected
        );
    }

    #[test]
    fn test_legacy_rar_decryptors_ignore_password_tail_after_rar_behavior_limit() {
        let prefix = "p".repeat(RAR_PASSWORD_MAX_UNITS);
        let long = format!("{prefix}tail");

        assert_eq!(
            Rar13Decryptor::new(&long).key,
            Rar13Decryptor::new(&prefix).key
        );
        assert_eq!(
            Rar15Decryptor::new(&long).key,
            Rar15Decryptor::new(&prefix).key
        );

        let rar20_long = Rar20Decryptor::new(&long);
        let rar20_prefix = Rar20Decryptor::new(&prefix);
        assert_eq!(rar20_long.key, rar20_prefix.key);
        assert_eq!(rar20_long.subst, rar20_prefix.subst);
    }

    #[test]
    fn test_legacy_rar_dos_password_uses_oem_bytes_for_non_ascii() {
        assert_ne!(
            Rar13Decryptor::new("ü").key,
            Rar13Decryptor::new_dos("ü").key
        );
        assert_eq!(
            Rar13Decryptor::new_dos("ü").key,
            Rar13Decryptor::new_from_bytes(&[0x81]).key
        );

        assert_ne!(
            Rar15Decryptor::new("ü").key,
            Rar15Decryptor::new_dos("ü").key
        );
        assert_eq!(
            Rar15Decryptor::new_dos("ü").key,
            Rar15Decryptor::new_from_bytes(&[0x81]).key
        );

        let rar20_dos = Rar20Decryptor::new_dos("ü");
        let rar20_bytes = Rar20Decryptor::new_from_bytes(&[0x81]);
        assert_ne!(Rar20Decryptor::new("ü").key, rar20_dos.key);
        assert_eq!(rar20_dos.key, rar20_bytes.key);
        assert_eq!(rar20_dos.subst, rar20_bytes.subst);
    }

    #[test]
    fn test_rar14_packed_comment_decrypt_uses_rar_behavior_fixed_key() {
        let mut data = [0x54, 0x55, 0x56];

        decrypt_rar14_packed_comment(&mut data);

        assert_eq!(data, [0x00, 0x60, 0x73]);
    }

    #[test]
    fn test_derive_key_deterministic() {
        let salt = [0xAA; 16];
        let (key1, iv1) = derive_key("password", &salt, 0).unwrap();
        let (key2, iv2) = derive_key("password", &salt, 0).unwrap();
        assert_eq!(key1, key2);
        assert_eq!(iv1, iv2);

        // Different password produces different key
        let (key3, _) = derive_key("other", &salt, 0).unwrap();
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_decrypt_round_trip() {
        let key = [0x42u8; 32];
        let iv = [0x13u8; 16];

        // Plaintext must be a multiple of 16 bytes
        let plaintext = b"Hello RAR world!"; // exactly 16 bytes
        assert_eq!(plaintext.len(), 16);

        let ciphertext = encrypt_aes256_cbc_for_test(&key, &iv, plaintext);

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

        // Derive the password check value using the production RAR5 material
        // builder, which internally follows the Count+32 PBKDF2 chain.
        let psw_check = derive_rar5_material("testpass", &salt, kdf_count)
            .unwrap()
            .psw_check;

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

    #[test]
    fn test_password_check_helper_matches_only_expected_prefix() {
        let psw_check = [1, 2, 3, 4, 5, 6, 7, 8];
        let mut check_data = [0u8; 12];
        check_data[..8].copy_from_slice(&psw_check);

        assert!(password_check_matches(&psw_check, &check_data));

        check_data[7] ^= 0xFF;
        assert!(!password_check_matches(&psw_check, &check_data));
    }

    #[test]
    fn test_rar5_aws_lc_material_matches_reference_vector() {
        let salt = [
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D,
            0x1E, 0x1F,
        ];
        let native = derive_rar5_material("e2e-test-password", &salt, 6).unwrap();

        assert_eq!(
            hex(&native.key),
            "4e0cc9bdeddb830e9f03f0720ac32be4c8572ed5d250ae815dff1bf85e2af67e"
        );
        assert_eq!(
            hex(&native.hash_key),
            "7de3c9354ee545c2c1b3e4f0a05ebe177465de87c1d134e8914ace0d7ad73a68"
        );
        assert_eq!(hex(&native.psw_check), "c2599769ca19cc07");
    }

    #[test]
    fn test_rar5_kdf_count_above_rar_behavior_limit_is_unsupported() {
        let salt = [0xAB; 16];
        let result = derive_rar5_material(
            "cache-pass",
            &salt,
            CRYPT5_KDF_LG2_COUNT_MAX.saturating_add(1),
        );

        assert!(matches!(
            result,
            Err(RarError::UnsupportedEncryptionKdf { count, max })
                if count == CRYPT5_KDF_LG2_COUNT_MAX + 1
                    && max == CRYPT5_KDF_LG2_COUNT_MAX
        ));
    }

    #[test]
    fn test_rar5_kdf_cache_reuses_cached_material() {
        let cache = KdfCache::new();
        let salt = [0xAB; 16];

        cache.derive_material_rar5("cache-pass", &salt, 4).unwrap();
        assert_eq!(cache.rar5.lock().unwrap().0.len(), 1);

        cache.derive_material_rar5("cache-pass", &salt, 4).unwrap();
        assert_eq!(cache.rar5.lock().unwrap().0.len(), 1);
    }

    // RAR4 crypto tests

    #[test]
    fn test_rar4_derive_key_deterministic() {
        let salt = [0xCC; 8];
        let (key1, iv1) = rar4_derive_key("password", Some(&salt));
        let (key2, iv2) = rar4_derive_key("password", Some(&salt));
        assert_eq!(key1, key2);
        assert_eq!(iv1, iv2);

        // Different password produces different key.
        let (key3, _) = rar4_derive_key("other", Some(&salt));
        assert_ne!(key1, key3);

        // Different salt produces different key.
        let (key4, _) = rar4_derive_key("password", Some(&[0xDD; 8]));
        assert_ne!(key1, key4);
    }

    #[test]
    fn test_rar4_derive_key_matches_rar_behavior_rar29_sha1() {
        let salt = [0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88];
        let (short_key, short_iv) = rar4_derive_key("password", Some(&salt));
        assert_eq!(hex(&short_key), "6dc5de01e3b2dbe3be10be0a04a61451");
        assert_eq!(hex(&short_iv), "28578a432b367b73dccfd439911f9584");

        let long_password = "abcdefghijklmnopqrstuvwxyzabcdef";
        let (long_key, long_iv) = rar4_derive_key(long_password, Some(&salt));
        assert_eq!(hex(&long_key), "d74f5e96dd94aa870efe4fdcd3d3e155");
        assert_eq!(hex(&long_iv), "4b12f8f5e926761d3ab3a3c98cc00d48");

        let (saltless_key, saltless_iv) = rar4_derive_key(long_password, None);
        assert_eq!(hex(&saltless_key), "a067cc19f522570c5440adfabc8ae733");
        assert_eq!(hex(&saltless_iv), "1a1eb51d88c1905a6c09328074c39f42");
    }

    #[test]
    fn test_rar4_long_password_kdf_matches_reference_vector() {
        // Generated from the RAR4 KDF and RAR29 SHA-1 reference algorithm.
        let salt = [0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77];
        let password = "abcdefghijklmnopqrstuvwxyzabcdef";

        let (key, iv) = rar4_derive_key(password, Some(&salt));

        assert_eq!(hex(&key), "6409b206ed974788e3d4819e4edba9b1");
        assert_eq!(hex(&iv), "87bbc0bf98daa1aa13e010cf14ced6ce");
    }

    #[test]
    fn test_rar4_derive_key_saltless_differs_from_salted() {
        let salt = [0xCC; 8];
        let (saltless_key, saltless_iv) = rar4_derive_key("password", None);
        let (salted_key, salted_iv) = rar4_derive_key("password", Some(&salt));
        assert_ne!(saltless_key, salted_key);
        assert_ne!(saltless_iv, salted_iv);
    }

    #[test]
    fn test_rar4_decrypt_round_trip() {
        let key = [0x42u8; 16];
        let iv = [0x13u8; 16];

        let plaintext = b"RAR4 encrypted!!"; // 16 bytes
        assert_eq!(plaintext.len(), 16);

        let ciphertext = encrypt_aes128_cbc_for_test(&key, &iv, plaintext);

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
        let salt = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let (key, iv) = rar4_derive_key("testpassword", Some(&salt));

        // Encrypt 32 bytes of plaintext.
        let plaintext = b"Hello from RAR4 encryption test!"; // 32 bytes
        assert_eq!(plaintext.len(), 32);

        let ciphertext = encrypt_aes128_cbc_for_test(&key, &iv, plaintext);

        assert_ne!(&ciphertext, plaintext);

        let decrypted = rar4_decrypt_data(&key, &iv, &ciphertext).unwrap();
        assert_eq!(&decrypted, plaintext);
    }

    #[test]
    fn test_rar4_custom_kdf_matches_reference_vector() {
        let salt = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let (key, iv) = rar4_derive_key("e2e-test-password", Some(&salt));

        assert_eq!(hex(&key), "36b07b37fb4e20e63b54fd54aa00ede9");
        assert_eq!(hex(&iv), "57ea4f82b145f2aa06f7c23f546d9561");
    }

    #[test]
    fn test_aws_lc_decrypt_update_chunk_limit_is_i32_aligned() {
        assert_eq!(AWS_LC_MAX_DECRYPT_UPDATE_LEN % AES_BLOCK, 0);
        assert!(AWS_LC_MAX_DECRYPT_UPDATE_LEN <= i32::MAX as usize);
        assert!(AWS_LC_MAX_DECRYPT_UPDATE_LEN + AES_BLOCK > i32::MAX as usize);
    }

    struct ChunkedCursor {
        cursor: std::io::Cursor<Vec<u8>>,
        max_chunk: usize,
    }

    impl ChunkedCursor {
        fn new(bytes: Vec<u8>, max_chunk: usize) -> Self {
            Self {
                cursor: std::io::Cursor::new(bytes),
                max_chunk,
            }
        }
    }

    impl Read for ChunkedCursor {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let limit = buf.len().min(self.max_chunk);
            self.cursor.read(&mut buf[..limit])
        }
    }

    #[test]
    fn test_rar4_streaming_reader_multi_block_round_trip() {
        let key = [0x21u8; 16];
        let iv = [0x43u8; 16];
        let plaintext = [0x52u8; AES_BLOCK * 4];
        let ciphertext = encrypt_aes128_cbc_for_test(&key, &iv, &plaintext);

        let inner = ChunkedCursor::new(ciphertext, 23);
        let mut reader = DecryptingReader::new_rar4(inner, &key, &iv);
        let mut actual = Vec::new();
        let mut chunk = [0u8; 19];
        loop {
            let read = reader.read(&mut chunk).unwrap();
            if read == 0 {
                break;
            }
            actual.extend_from_slice(&chunk[..read]);
        }

        assert_eq!(actual, plaintext);
    }

    #[test]
    fn test_rar5_streaming_reader_multi_block_round_trip() {
        let key = [0x34u8; 32];
        let iv = [0x56u8; 16];
        let plaintext = [0x35u8; AES_BLOCK * 4];
        let ciphertext = encrypt_aes256_cbc_for_test(&key, &iv, &plaintext);

        let inner = ChunkedCursor::new(ciphertext, 29);
        let mut reader = DecryptingReader::new_rar5(inner, &key, &iv);
        let mut actual = Vec::new();
        let mut chunk = [0u8; 17];
        loop {
            let read = reader.read(&mut chunk).unwrap();
            if read == 0 {
                break;
            }
            actual.extend_from_slice(&chunk[..read]);
        }

        assert_eq!(actual, plaintext);
    }
}
