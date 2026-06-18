use std::io::{self, Cursor, Read, Write};
use std::path::Path;

use weaver_nzb::{Nzb, NzbError};

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

#[derive(Debug)]
pub enum PersistedNzbError {
    Io(io::Error),
    Parse(NzbError),
}

impl std::fmt::Display for PersistedNzbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "{error}"),
            Self::Parse(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for PersistedNzbError {}

pub fn decode_persisted_nzb_bytes(bytes: &[u8]) -> io::Result<Vec<u8>> {
    if bytes.starts_with(&ZSTD_MAGIC) {
        zstd::stream::decode_all(Cursor::new(bytes))
    } else {
        Ok(bytes.to_vec())
    }
}

pub fn parse_persisted_nzb_bytes(bytes: &[u8]) -> Result<Nzb, PersistedNzbError> {
    let decoded = decode_persisted_nzb_bytes(bytes).map_err(PersistedNzbError::Io)?;
    weaver_nzb::parse_nzb(&decoded).map_err(PersistedNzbError::Parse)
}

pub fn compress_nzb_bytes(nzb_bytes: &[u8]) -> io::Result<Vec<u8>> {
    if nzb_bytes.starts_with(&ZSTD_MAGIC) {
        return Ok(nzb_bytes.to_vec());
    }

    let mut encoder = zstd::stream::Encoder::new(Vec::new(), 3)?;
    encoder.write_all(nzb_bytes)?;
    encoder.finish()
}

/// Migration-only helper for absorbing legacy file-backed NZBs into DB blobs.
pub fn load_persisted_nzb_storage_bytes(path: &Path) -> io::Result<Vec<u8>> {
    let bytes = std::fs::read(path)?;
    compress_nzb_bytes(&bytes)
}

pub fn persist_decoded_nzb_reader_to_zstd<R: Read>(
    source: &mut R,
) -> Result<(Vec<u8>, Nzb), PersistedNzbError> {
    let mut encoder = zstd::stream::Encoder::new(Vec::new(), 3).map_err(PersistedNzbError::Io)?;
    io::copy(source, &mut encoder).map_err(PersistedNzbError::Io)?;
    let bytes = encoder.finish().map_err(PersistedNzbError::Io)?;
    let nzb = parse_persisted_nzb_bytes(&bytes)?;
    Ok((bytes, nzb))
}

pub fn hash_persisted_nzb_bytes(bytes: &[u8]) -> [u8; 32] {
    let decoded = decode_persisted_nzb_bytes(bytes).unwrap_or_else(|_| bytes.to_vec());
    let mut hasher = blake3::Hasher::new();
    hasher.update(&decoded);
    finalize_blake3(hasher)
}

fn finalize_blake3(hasher: blake3::Hasher) -> [u8; 32] {
    let digest = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_bytes());
    out
}
