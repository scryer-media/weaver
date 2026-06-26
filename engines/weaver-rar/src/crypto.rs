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
//! the common case). Matches unrar's `KDF3Cache`/`KDF5Cache` approach.

use std::io::Read;
use std::sync::Mutex;

#[cfg(not(feature = "native-crypto"))]
use aes::cipher::{Block, BlockModeDecrypt, BlockSizeUser};
#[cfg(not(feature = "native-crypto"))]
use aes::{Aes128, Aes256};
#[cfg(feature = "native-crypto")]
use aws_lc_rs::hmac as aws_hmac;
#[cfg(feature = "native-crypto")]
use aws_lc_sys::{
    EVP_CIPHER, EVP_CIPHER_CTX, EVP_CIPHER_CTX_free, EVP_CIPHER_CTX_new,
    EVP_CIPHER_CTX_set_padding, EVP_DecryptInit_ex, EVP_DecryptUpdate, EVP_aes_128_cbc,
    EVP_aes_256_cbc,
};
use blake2s_simd::blake2sp;
#[cfg(not(feature = "native-crypto"))]
use cbc::cipher::KeyIvInit;
use hmac::digest::KeyInit;
use hmac::{Hmac, Mac};
use sha2::Sha256;
#[cfg(feature = "native-crypto")]
use std::ptr::null_mut;

use crate::error::{RarError, RarResult};
use crate::rar4::types::Rar4EncryptionMethod;

#[cfg(not(feature = "native-crypto"))]
type Aes256CbcDec = cbc::Decryptor<Aes256>;
#[cfg(not(feature = "native-crypto"))]
type Aes128CbcDec = cbc::Decryptor<Aes128>;
type HmacSha256 = Hmac<Sha256>;

pub const CRYPT5_KDF_LG2_COUNT_MAX: u8 = 24;

#[cfg_attr(feature = "native-crypto", allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RarCryptoBackend {
    RustCrypto,
    #[cfg(feature = "native-crypto")]
    NativeAwsLc,
}

const fn default_rar_crypto_backend() -> RarCryptoBackend {
    #[cfg(feature = "native-crypto")]
    {
        RarCryptoBackend::NativeAwsLc
    }
    #[cfg(not(feature = "native-crypto"))]
    {
        RarCryptoBackend::RustCrypto
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rar5KeyMaterial {
    pub key: [u8; 32],
    pub hash_key: [u8; 32],
    pub psw_check: [u8; 8],
}

fn hmac_sha256_rustcrypto(password: &HmacSha256, data: &[u8]) -> [u8; 32] {
    let mut mac = password.clone();
    mac.update(data);
    mac.finalize().into_bytes().into()
}

#[cfg(feature = "native-crypto")]
fn hmac_sha256_native(password: &aws_hmac::Key, data: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(aws_hmac::sign(password, data).as_ref());
    out
}

fn fold_password_check(value: &[u8; 32]) -> [u8; 8] {
    let mut psw_check = [0u8; 8];
    for (index, byte) in value.iter().copied().enumerate() {
        psw_check[index % psw_check.len()] ^= byte;
    }
    psw_check
}

fn derive_rar5_material_rustcrypto(
    password: &str,
    salt: &[u8; 16],
    kdf_count: u8,
) -> RarResult<Rar5KeyMaterial> {
    if kdf_count > CRYPT5_KDF_LG2_COUNT_MAX {
        return Err(RarError::CorruptArchive {
            detail: format!(
                "RAR5 KDF log2 count {} exceeds maximum {}",
                kdf_count, CRYPT5_KDF_LG2_COUNT_MAX
            ),
        });
    }

    let count = 1u32 << kdf_count;
    let password_mac = <HmacSha256 as KeyInit>::new_from_slice(password.as_bytes())
        .expect("HMAC accepts arbitrary keys");

    let mut salt_block = [0u8; 20];
    salt_block[..salt.len()].copy_from_slice(salt);
    salt_block[19] = 1;

    let mut u = hmac_sha256_rustcrypto(&password_mac, &salt_block);
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
            u = hmac_sha256_rustcrypto(&password_mac, &u);
            for (acc, next) in fn_value.iter_mut().zip(u.iter()) {
                *acc ^= *next;
            }
        }
        *output = fn_value;
    }

    Ok(Rar5KeyMaterial {
        key,
        hash_key,
        psw_check: fold_password_check(&psw_check_value),
    })
}

#[cfg(feature = "native-crypto")]
fn derive_rar5_material_native(
    password: &str,
    salt: &[u8; 16],
    kdf_count: u8,
) -> RarResult<Rar5KeyMaterial> {
    if kdf_count > CRYPT5_KDF_LG2_COUNT_MAX {
        return Err(RarError::CorruptArchive {
            detail: format!(
                "RAR5 KDF log2 count {} exceeds maximum {}",
                kdf_count, CRYPT5_KDF_LG2_COUNT_MAX
            ),
        });
    }

    let count = 1u32 << kdf_count;
    let password_mac = aws_hmac::Key::new(aws_hmac::HMAC_SHA256, password.as_bytes());

    let mut salt_block = [0u8; 20];
    salt_block[..salt.len()].copy_from_slice(salt);
    salt_block[19] = 1;

    let mut u = hmac_sha256_native(&password_mac, &salt_block);
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
            u = hmac_sha256_native(&password_mac, &u);
            for (acc, next) in fn_value.iter_mut().zip(u.iter()) {
                *acc ^= *next;
            }
        }
        *output = fn_value;
    }

    Ok(Rar5KeyMaterial {
        key,
        hash_key,
        psw_check: fold_password_check(&psw_check_value),
    })
}

fn derive_rar5_material_with_backend(
    backend: RarCryptoBackend,
    password: &str,
    salt: &[u8; 16],
    kdf_count: u8,
) -> RarResult<Rar5KeyMaterial> {
    match backend {
        RarCryptoBackend::RustCrypto => derive_rar5_material_rustcrypto(password, salt, kdf_count),
        #[cfg(feature = "native-crypto")]
        RarCryptoBackend::NativeAwsLc => derive_rar5_material_native(password, salt, kdf_count),
    }
}

pub fn derive_rar5_material(
    password: &str,
    salt: &[u8; 16],
    kdf_count: u8,
) -> RarResult<Rar5KeyMaterial> {
    derive_rar5_material_with_backend(default_rar_crypto_backend(), password, salt, kdf_count)
}

/// Derive AES-256 key from password and salt using PBKDF2-HMAC-SHA256.
///
/// RAR5 KDF: iterations = 1 << kdf_count (confirmed against unrar source).
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
    Ok((
        derive_rar5_material(password, salt, kdf_count)?.key,
        [0u8; 16],
    ))
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
    derive_rar5_material(password, salt, kdf_count)
        .map(|material| material.psw_check == check_data[..8])
        .unwrap_or(false)
}

pub fn convert_crc32_to_mac(value: u32, key: &[u8; 32]) -> u32 {
    let digest = match default_rar_crypto_backend() {
        RarCryptoBackend::RustCrypto => hmac_sha256_rustcrypto(
            &<HmacSha256 as KeyInit>::new_from_slice(key).expect("HMAC accepts arbitrary keys"),
            &value.to_le_bytes(),
        ),
        #[cfg(feature = "native-crypto")]
        RarCryptoBackend::NativeAwsLc => hmac_sha256_native(
            &aws_hmac::Key::new(aws_hmac::HMAC_SHA256, key),
            &value.to_le_bytes(),
        ),
    };
    let mut mac = 0u32;
    for (index, byte) in digest.iter().copied().enumerate() {
        mac ^= (byte as u32) << ((index & 3) * 8);
    }
    mac
}

pub fn convert_blake2_to_mac(value: [u8; 32], key: &[u8; 32]) -> [u8; 32] {
    match default_rar_crypto_backend() {
        RarCryptoBackend::RustCrypto => hmac_sha256_rustcrypto(
            &<HmacSha256 as KeyInit>::new_from_slice(key).expect("HMAC accepts arbitrary keys"),
            &value,
        ),
        #[cfg(feature = "native-crypto")]
        RarCryptoBackend::NativeAwsLc => {
            hmac_sha256_native(&aws_hmac::Key::new(aws_hmac::HMAC_SHA256, key), &value)
        }
    }
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

/// Cached RAR4 key derivation result.
#[derive(Debug)]
struct Kdf3Entry {
    password: String,
    salt: Option<[u8; 8]>,
    key: [u8; 16],
    iv: [u8; 16],
}

/// Thread-safe KDF cache matching unrar's `KDF3Cache[4]`/`KDF5Cache[4]`.
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
            .map(|material| material.psw_check == check_data[..8])
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
        let mut key = [0u8; 3];
        for &byte in password.as_bytes() {
            key[0] = key[0].wrapping_add(byte);
            key[1] ^= byte;
            key[2] = key[2].wrapping_add(byte).rotate_left(1);
        }
        Self { key }
    }

    fn decrypt(&mut self, data: &mut [u8]) {
        for byte in data {
            self.key[1] = self.key[1].wrapping_add(self.key[2]);
            self.key[0] = self.key[0].wrapping_add(self.key[1]);
            *byte = byte.wrapping_sub(self.key[0]);
        }
    }
}

#[derive(Clone)]
struct Rar15Decryptor {
    key: [u16; 4],
    crc_tab: [u32; 256],
}

impl Rar15Decryptor {
    fn new(password: &str) -> Self {
        let crc_tab = crc32_table();
        let psw_crc = !crc32fast::hash(password.as_bytes());
        let mut key = [(psw_crc & 0xFFFF) as u16, (psw_crc >> 16) as u16, 0, 0];

        for &byte in password.as_bytes() {
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

#[derive(Clone)]
struct Rar20Decryptor {
    key: [u32; 4],
    subst: [u8; 256],
    crc_tab: [u32; 256],
}

impl Rar20Decryptor {
    fn new(password: &str) -> Self {
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
        let pwd_bytes = password.as_bytes();
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

/// RAR4 key derivation iteration count.
const RAR4_KDF_ITERATIONS: u32 = 0x40000; // 262144

#[derive(Clone)]
struct Rar29Sha1 {
    state: [u32; 5],
    buffer: [u8; 64],
    count: u64,
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

    fn transform_block(&mut self, block: &[u8; 64]) -> [u32; 16] {
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
fn rar4_derive_key_unrar(password: &str, salt: Option<&[u8; 8]>) -> ([u8; 16], [u8; 16]) {
    // Encode password as UTF-16LE, then append salt if present — matching
    // unrar's RawPsw buffer for both salted and saltless RAR30 members.
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

        // Extract IV byte at each interval boundary (unrar: `I%(HashRounds/16)==0`).
        if i % iv_interval == 0 {
            let intermediate = sha.clone().finish_words();
            let iv_index = (i / iv_interval) as usize;
            iv[iv_index] = intermediate[4] as u8;
        }
    }

    let digest = sha.finish_words();

    // unrar extracts key bytes in LE order per 32-bit digest word:
    //   AESKey[I*4+J] = (byte)(digest[I] >> (J*8))
    let mut key = [0u8; 16];
    for word in 0..4 {
        key[word * 4..word * 4 + 4].copy_from_slice(&digest[word].to_le_bytes());
    }

    (key, iv)
}

fn rar4_derive_key_with_backend(
    _backend: RarCryptoBackend,
    password: &str,
    salt: Option<&[u8; 8]>,
) -> ([u8; 16], [u8; 16]) {
    rar4_derive_key_unrar(password, salt)
}

pub fn rar4_derive_key(password: &str, salt: Option<&[u8; 8]>) -> ([u8; 16], [u8; 16]) {
    rar4_derive_key_with_backend(default_rar_crypto_backend(), password, salt)
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

#[cfg(not(feature = "native-crypto"))]
fn cipher_blocks_mut<C>(data: &mut [u8]) -> &mut [Block<C>]
where
    C: BlockSizeUser,
{
    debug_assert_eq!(std::mem::size_of::<Block<C>>(), AES_BLOCK);
    debug_assert!(data.len().is_multiple_of(AES_BLOCK));

    // SAFETY: `Block<C>` is a byte-backed fixed-size buffer. We only reinterpret
    // a multiple-of-block-size `u8` slice, then immediately hand it to the
    // cipher backend without changing its lifetime or aliasing guarantees.
    let (prefix, blocks, suffix) = unsafe { data.align_to_mut::<Block<C>>() };
    debug_assert!(prefix.is_empty());
    debug_assert!(suffix.is_empty());
    blocks
}

#[cfg(feature = "native-crypto")]
struct AwsLcCbcDecryptor {
    ctx: *mut EVP_CIPHER_CTX,
}

#[cfg(feature = "native-crypto")]
unsafe impl Send for AwsLcCbcDecryptor {}

#[cfg(feature = "native-crypto")]
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
        if data.is_empty() {
            return;
        }

        let mut out_len = 0_i32;
        let input_len = i32::try_from(data.len()).expect("block-aligned CBC input fits in i32");
        let result = unsafe {
            EVP_DecryptUpdate(
                self.ctx,
                data.as_mut_ptr(),
                &mut out_len,
                data.as_ptr(),
                input_len,
            )
        };
        assert_eq!(result, 1, "aws-lc EVP_DecryptUpdate must succeed");
        assert_eq!(
            usize::try_from(out_len).expect("aws-lc output length must be non-negative"),
            data.len(),
            "aws-lc CBC decrypt must write the full block-aligned input"
        );
    }
}

#[cfg(feature = "native-crypto")]
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
    #[cfg(feature = "native-crypto")]
    decryptor: AwsLcCbcDecryptor,
    #[cfg(not(feature = "native-crypto"))]
    decryptor: Aes256CbcDec,
}

impl CbcDecryptor {
    pub fn new(key: &[u8; 32], iv: &[u8; AES_BLOCK]) -> Self {
        Self {
            #[cfg(feature = "native-crypto")]
            decryptor: AwsLcCbcDecryptor::new_aes256(key, iv),
            #[cfg(not(feature = "native-crypto"))]
            decryptor: Aes256CbcDec::new(key.into(), iv.into()),
        }
    }

    /// Decrypt `data` in-place. `data.len()` MUST be a multiple of 16.
    /// Updates internal IV state for subsequent calls.
    pub fn decrypt_blocks(&mut self, data: &mut [u8]) {
        debug_assert!(data.len().is_multiple_of(AES_BLOCK));
        #[cfg(feature = "native-crypto")]
        self.decryptor.decrypt_blocks(data);
        #[cfg(not(feature = "native-crypto"))]
        self.decryptor
            .decrypt_blocks(cipher_blocks_mut::<Aes256CbcDec>(data));
    }
}

/// Stateful AES-128-CBC decryptor for RAR4 archives.
pub struct Rar4CbcDecryptor {
    #[cfg(feature = "native-crypto")]
    decryptor: AwsLcCbcDecryptor,
    #[cfg(not(feature = "native-crypto"))]
    decryptor: Aes128CbcDec,
}

impl Rar4CbcDecryptor {
    pub fn new(key: &[u8; 16], iv: &[u8; AES_BLOCK]) -> Self {
        Self {
            #[cfg(feature = "native-crypto")]
            decryptor: AwsLcCbcDecryptor::new_aes128(key, iv),
            #[cfg(not(feature = "native-crypto"))]
            decryptor: Aes128CbcDec::new(key.into(), iv.into()),
        }
    }

    /// Decrypt `data` in-place. `data.len()` MUST be a multiple of 16.
    pub fn decrypt_blocks(&mut self, data: &mut [u8]) {
        debug_assert!(data.len().is_multiple_of(AES_BLOCK));
        #[cfg(feature = "native-crypto")]
        self.decryptor.decrypt_blocks(data);
        #[cfg(not(feature = "native-crypto"))]
        self.decryptor
            .decrypt_blocks(cipher_blocks_mut::<Aes128CbcDec>(data));
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
        use aes::Aes256;
        use cbc::cipher::{BlockModeEncrypt, KeyIvInit};

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
            .encrypt_padded::<cbc::cipher::block_padding::NoPadding>(&mut ciphertext, 16)
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

        // Derive the password check value using the production RAR5 material
        // builder, which internally follows unrar's Count+32 PBKDF2 chain.
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

    #[cfg(feature = "native-crypto")]
    #[test]
    fn test_rar5_native_backend_matches_reference_vector() {
        let salt = [
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D,
            0x1E, 0x1F,
        ];
        let rustcrypto = derive_rar5_material_with_backend(
            RarCryptoBackend::RustCrypto,
            "e2e-test-password",
            &salt,
            6,
        )
        .unwrap();
        let native = derive_rar5_material_with_backend(
            RarCryptoBackend::NativeAwsLc,
            "e2e-test-password",
            &salt,
            6,
        )
        .unwrap();

        assert_eq!(native, rustcrypto);
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
    fn test_rar4_derive_key_matches_unrar_rar29_sha1() {
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
    fn test_rar4_long_password_kdf_matches_local_unrar_vector() {
        // Generated with the local unrar checkout's crypt3.cpp SetKey30 loop
        // and sha1.cpp sha1_process_rar29 implementation.
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
        use aes::Aes128;
        use cbc::cipher::{BlockModeEncrypt, KeyIvInit};

        type Aes128CbcEnc = cbc::Encryptor<Aes128>;

        let key = [0x42u8; 16];
        let iv = [0x13u8; 16];

        let plaintext = b"RAR4 encrypted!!"; // 16 bytes
        assert_eq!(plaintext.len(), 16);

        // Encrypt
        let mut ciphertext = plaintext.to_vec();
        let encryptor = Aes128CbcEnc::new((&key).into(), (&iv).into());
        encryptor
            .encrypt_padded::<cbc::cipher::block_padding::NoPadding>(&mut ciphertext, 16)
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
        use aes::Aes128;
        use cbc::cipher::{BlockModeEncrypt, KeyIvInit};

        type Aes128CbcEnc = cbc::Encryptor<Aes128>;

        let salt = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let (key, iv) = rar4_derive_key("testpassword", Some(&salt));

        // Encrypt 32 bytes of plaintext.
        let plaintext = b"Hello from RAR4 encryption test!"; // 32 bytes
        assert_eq!(plaintext.len(), 32);

        let mut ciphertext = plaintext.to_vec();
        let encryptor = Aes128CbcEnc::new((&key).into(), (&iv).into());
        encryptor
            .encrypt_padded::<cbc::cipher::block_padding::NoPadding>(&mut ciphertext, 32)
            .unwrap();

        assert_ne!(&ciphertext, plaintext);

        let decrypted = rar4_decrypt_data(&key, &iv, &ciphertext).unwrap();
        assert_eq!(&decrypted, plaintext);
    }

    #[cfg(feature = "native-crypto")]
    #[test]
    fn test_rar4_native_backend_matches_reference_vector() {
        let salt = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let rustcrypto = rar4_derive_key_with_backend(
            RarCryptoBackend::RustCrypto,
            "e2e-test-password",
            Some(&salt),
        );
        let native = rar4_derive_key_with_backend(
            RarCryptoBackend::NativeAwsLc,
            "e2e-test-password",
            Some(&salt),
        );

        assert_eq!(native, rustcrypto);
        assert_eq!(hex(&native.0), "36b07b37fb4e20e63b54fd54aa00ede9");
        assert_eq!(hex(&native.1), "57ea4f82b145f2aa06f7c23f546d9561");
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
        use aes::Aes128;
        use cbc::cipher::{BlockModeEncrypt, KeyIvInit};

        type Aes128CbcEnc = cbc::Encryptor<Aes128>;

        let key = [0x21u8; 16];
        let iv = [0x43u8; 16];
        let plaintext = [0x52u8; AES_BLOCK * 4];
        let mut ciphertext = plaintext.to_vec();
        let encryptor = Aes128CbcEnc::new((&key).into(), (&iv).into());
        encryptor
            .encrypt_padded::<cbc::cipher::block_padding::NoPadding>(
                &mut ciphertext,
                plaintext.len(),
            )
            .unwrap();

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
        use aes::Aes256;
        use cbc::cipher::{BlockModeEncrypt, KeyIvInit};

        type Aes256CbcEnc = cbc::Encryptor<Aes256>;

        let key = [0x34u8; 32];
        let iv = [0x56u8; 16];
        let plaintext = [0x35u8; AES_BLOCK * 4];
        let mut ciphertext = plaintext.to_vec();
        let encryptor = Aes256CbcEnc::new((&key).into(), (&iv).into());
        encryptor
            .encrypt_padded::<cbc::cipher::block_padding::NoPadding>(
                &mut ciphertext,
                plaintext.len(),
            )
            .unwrap();

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
