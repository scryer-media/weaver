use std::io::{self, BufReader, Cursor, Read, Write};
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

pub struct PreparedPersistedNzb {
    pub nzb_zstd: Vec<u8>,
    pub nzb: Nzb,
    pub raw_job_hash: [u8; 32],
}

struct ObservedReader<R, W> {
    source: R,
    copy: W,
    hasher: blake3::Hasher,
    error: Option<io::Error>,
}

impl<R: Read, W: Write> Read for ObservedReader<R, W> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let read = match self.source.read(buffer) {
            Ok(read) => read,
            Err(error) => {
                self.error = Some(io::Error::new(error.kind(), error.to_string()));
                return Err(error);
            }
        };
        if read != 0 {
            if let Err(error) = self.copy.write_all(&buffer[..read]) {
                self.error = Some(io::Error::new(error.kind(), error.to_string()));
                return Err(error);
            }
            self.hasher.update(&buffer[..read]);
        }
        Ok(read)
    }
}

fn parse_decoded_nzb_reader<R: Read, W: Write>(
    source: R,
    copy: W,
) -> Result<(Nzb, W, [u8; 32]), PersistedNzbError> {
    let observed = ObservedReader {
        source,
        copy,
        hasher: blake3::Hasher::new(),
        error: None,
    };
    let mut reader = BufReader::new(observed);
    let parsed = weaver_nzb::parse_nzb_reader(&mut reader);
    let observed = reader.into_inner();
    if let Some(error) = observed.error {
        return Err(PersistedNzbError::Io(error));
    }
    let nzb = parsed.map_err(PersistedNzbError::Parse)?;
    Ok((nzb, observed.copy, finalize_blake3(observed.hasher)))
}

pub fn decode_persisted_nzb_bytes(bytes: &[u8]) -> io::Result<Vec<u8>> {
    if bytes.starts_with(&ZSTD_MAGIC) {
        zstd::stream::decode_all(Cursor::new(bytes))
    } else {
        Ok(bytes.to_vec())
    }
}

pub fn parse_persisted_nzb_bytes(bytes: &[u8]) -> Result<Nzb, PersistedNzbError> {
    parse_and_hash_persisted_nzb_bytes(bytes).map(|(nzb, _)| nzb)
}

pub fn parse_and_hash_persisted_nzb_bytes(
    bytes: &[u8],
) -> Result<(Nzb, [u8; 32]), PersistedNzbError> {
    let (nzb, _, raw_hash) = if bytes.starts_with(&ZSTD_MAGIC) {
        let decoder =
            zstd::stream::read::Decoder::new(Cursor::new(bytes)).map_err(PersistedNzbError::Io)?;
        parse_decoded_nzb_reader(decoder, io::sink())?
    } else {
        parse_decoded_nzb_reader(Cursor::new(bytes), io::sink())?
    };
    Ok((nzb, raw_hash))
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
) -> Result<PreparedPersistedNzb, PersistedNzbError> {
    let encoder = zstd::stream::Encoder::new(Vec::new(), 3).map_err(PersistedNzbError::Io)?;
    let (nzb, encoder, raw_hash) = parse_decoded_nzb_reader(source, encoder)?;
    let bytes = encoder.finish().map_err(PersistedNzbError::Io)?;
    Ok(PreparedPersistedNzb {
        nzb_zstd: bytes,
        nzb,
        raw_job_hash: raw_hash,
    })
}

pub fn hash_persisted_nzb_bytes(bytes: &[u8]) -> [u8; 32] {
    let result = if bytes.starts_with(&ZSTD_MAGIC) {
        zstd::stream::read::Decoder::new(Cursor::new(bytes))
            .and_then(|mut decoder| hash_reader(&mut decoder))
    } else {
        hash_reader(&mut Cursor::new(bytes))
    };
    result.unwrap_or_else(|_| hash_bytes(bytes))
}

fn hash_reader(reader: &mut impl Read) -> io::Result<[u8; 32]> {
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(finalize_blake3(hasher));
        }
        hasher.update(&buffer[..read]);
    }
}

fn hash_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(bytes);
    finalize_blake3(hasher)
}

fn finalize_blake3(hasher: blake3::Hasher) -> [u8; 32] {
    let digest = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_bytes());
    out
}
