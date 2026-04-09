use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use super::manifest::{BackupManifest, BackupServiceError, io_err};

pub(crate) fn write_plain_archive(
    dest: &Path,
    manifest: &BackupManifest,
    backup_db_path: &Path,
) -> Result<(), std::io::Error> {
    let file = File::create(dest)?;
    let encoder = zstd::stream::write::Encoder::new(file, 19)?;
    let mut tar = tar::Builder::new(encoder);

    let manifest_bytes = serde_json::to_vec_pretty(manifest)?;
    let mut header = tar::Header::new_gnu();
    header.set_size(manifest_bytes.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(&mut header, "manifest.json", manifest_bytes.as_slice())?;
    tar.append_path_with_name(backup_db_path, "backup.db")?;

    let encoder = tar.into_inner()?;
    encoder.finish()?;
    Ok(())
}

/// File format: `WEAVER_ENC\0` (10 bytes) + salt (32 bytes) + nonce (12 bytes) + ciphertext+tag
const ENCRYPT_MAGIC: &[u8; 10] = b"WEAVER_ENC";
const SALT_LEN: usize = 32;
const PBKDF2_ROUNDS: u32 = 600_000;

fn derive_key(password: &str, salt: &[u8]) -> [u8; 32] {
    let mut key = [0u8; 32];
    pbkdf2::pbkdf2_hmac::<sha2::Sha256>(password.as_bytes(), salt, PBKDF2_ROUNDS, &mut key);
    key
}

pub(crate) fn encrypt_archive(
    input: &Path,
    output: &Path,
    password: &str,
) -> Result<(), std::io::Error> {
    use aes_gcm::aead::generic_array::GenericArray;
    use aes_gcm::{Aes256Gcm, KeyInit, aead::Aead};

    let mut plaintext = Vec::new();
    File::open(input)?.read_to_end(&mut plaintext)?;

    let mut salt = [0u8; SALT_LEN];
    getrandom::fill(&mut salt).map_err(|e| std::io::Error::other(e.to_string()))?;

    let key = derive_key(password, &salt);
    let cipher = Aes256Gcm::new(GenericArray::from_slice(&key));

    let mut nonce_bytes = [0u8; 12];
    getrandom::fill(&mut nonce_bytes).map_err(|e| std::io::Error::other(e.to_string()))?;
    let nonce = GenericArray::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, plaintext.as_ref())
        .map_err(|e| std::io::Error::other(format!("encryption failed: {e}")))?;

    let mut out = File::create(output)?;
    use std::io::Write;
    out.write_all(ENCRYPT_MAGIC)?;
    out.write_all(&salt)?;
    out.write_all(&nonce_bytes)?;
    out.write_all(&ciphertext)?;
    Ok(())
}

pub(crate) fn maybe_decrypt_archive(
    input: &Path,
    password: Option<String>,
    work_dir: &Path,
) -> Result<PathBuf, BackupServiceError> {
    if !is_encrypted(input)? {
        return Ok(input.to_path_buf());
    }

    let password = password
        .filter(|value| !value.is_empty())
        .ok_or(BackupServiceError::PasswordRequired)?;
    let output = work_dir.join("backup.tar.zst");
    decrypt_archive(input, &output, &password)?;
    Ok(output)
}

fn decrypt_archive(input: &Path, output: &Path, password: &str) -> Result<(), BackupServiceError> {
    use aes_gcm::aead::generic_array::GenericArray;
    use aes_gcm::{Aes256Gcm, KeyInit, aead::Aead};

    let mut data = Vec::new();
    File::open(input)
        .map_err(io_err)?
        .read_to_end(&mut data)
        .map_err(io_err)?;

    let header_len = ENCRYPT_MAGIC.len() + SALT_LEN + 12;
    if data.len() < header_len {
        return Err(BackupServiceError::InvalidPassword);
    }

    let salt = &data[ENCRYPT_MAGIC.len()..ENCRYPT_MAGIC.len() + SALT_LEN];
    let nonce_bytes = &data[ENCRYPT_MAGIC.len() + SALT_LEN..header_len];
    let ciphertext = &data[header_len..];

    let key = derive_key(password, salt);
    let cipher = Aes256Gcm::new(GenericArray::from_slice(&key));
    let nonce = GenericArray::from_slice(nonce_bytes);

    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|_| BackupServiceError::InvalidPassword)?;

    std::fs::write(output, plaintext).map_err(io_err)?;
    Ok(())
}

pub(crate) fn unpack_plain_archive(
    input: &Path,
    output_dir: &Path,
) -> Result<(), BackupServiceError> {
    let file = File::open(input).map_err(io_err)?;
    let decoder = zstd::stream::read::Decoder::new(file).map_err(io_err)?;
    let mut archive = tar::Archive::new(decoder);
    archive.unpack(output_dir).map_err(io_err)?;
    Ok(())
}

fn is_encrypted(path: &Path) -> Result<bool, BackupServiceError> {
    let mut header = [0u8; 10];
    let mut file = File::open(path).map_err(io_err)?;
    let read = file.read(&mut header).map_err(io_err)?;
    Ok(read >= ENCRYPT_MAGIC.len() && header[..ENCRYPT_MAGIC.len()] == *ENCRYPT_MAGIC)
}
