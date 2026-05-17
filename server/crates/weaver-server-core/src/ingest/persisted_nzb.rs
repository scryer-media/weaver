use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Cursor, Read, Write};
use std::path::{Path, PathBuf};

use weaver_nzb::{Nzb, NzbError, parse_nzb_reader};

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
const HASH_BUFFER_SIZE: usize = 64 * 1024;

pub enum PersistedNzbReader {
    Plain(BufReader<File>),
    Zstd(BufReader<zstd::stream::read::Decoder<'static, BufReader<File>>>),
}

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

impl Read for PersistedNzbReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(reader) => reader.read(buf),
            Self::Zstd(reader) => reader.read(buf),
        }
    }
}

impl BufRead for PersistedNzbReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        match self {
            Self::Plain(reader) => reader.fill_buf(),
            Self::Zstd(reader) => reader.fill_buf(),
        }
    }

    fn consume(&mut self, amt: usize) {
        match self {
            Self::Plain(reader) => reader.consume(amt),
            Self::Zstd(reader) => reader.consume(amt),
        }
    }
}

pub fn open_persisted_nzb_reader(path: &Path) -> io::Result<PersistedNzbReader> {
    let reader = BufReader::new(File::open(path)?);
    open_persisted_nzb_reader_from_buffer(reader)
}

pub fn parse_persisted_nzb(path: &Path) -> Result<Nzb, PersistedNzbError> {
    let reader = open_persisted_nzb_reader(path).map_err(PersistedNzbError::Io)?;
    parse_nzb_reader(reader).map_err(PersistedNzbError::Parse)
}

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

pub async fn remove_persisted_nzb_if_exists(path: &Path) {
    if let Err(error) = tokio::fs::remove_file(path).await
        && error.kind() != io::ErrorKind::NotFound
    {
        tracing::warn!(
            path = %path.display(),
            error = %error,
            "failed to remove orphaned persisted nzb"
        );
    }
}

pub fn write_compressed_nzb(path: &Path, nzb_bytes: &[u8]) -> io::Result<()> {
    let file = File::create(path)?;
    let writer = io::BufWriter::new(file);
    let mut encoder = zstd::stream::Encoder::new(writer, 3)?;
    encoder.write_all(nzb_bytes)?;
    let mut writer = encoder.finish()?;
    writer.flush()?;
    Ok(())
}

pub fn compress_nzb_bytes(nzb_bytes: &[u8]) -> io::Result<Vec<u8>> {
    if nzb_bytes.starts_with(&ZSTD_MAGIC) {
        return Ok(nzb_bytes.to_vec());
    }

    let mut encoder = zstd::stream::Encoder::new(Vec::new(), 3)?;
    encoder.write_all(nzb_bytes)?;
    encoder.finish()
}

pub fn load_persisted_nzb_storage_bytes(path: &Path) -> io::Result<Vec<u8>> {
    let bytes = std::fs::read(path)?;
    compress_nzb_bytes(&bytes)
}

pub fn persist_decoded_nzb_reader<R: Read>(
    path: &Path,
    source: &mut R,
) -> Result<Nzb, PersistedNzbError> {
    let partial_path = path.with_extension("nzb.part");
    let persist_result = (|| {
        let file = File::create(&partial_path).map_err(PersistedNzbError::Io)?;
        let writer = io::BufWriter::new(file);
        let mut encoder = zstd::stream::Encoder::new(writer, 3).map_err(PersistedNzbError::Io)?;
        io::copy(source, &mut encoder).map_err(PersistedNzbError::Io)?;
        let mut writer = encoder.finish().map_err(PersistedNzbError::Io)?;
        writer.flush().map_err(PersistedNzbError::Io)?;
        std::fs::rename(&partial_path, path).map_err(PersistedNzbError::Io)?;
        parse_persisted_nzb(path)
    })();

    if persist_result.is_err() {
        let _ = std::fs::remove_file(&partial_path);
    }

    persist_result
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

pub fn hash_persisted_nzb(path: &Path) -> io::Result<[u8; 32]> {
    let mut reader = open_persisted_nzb_reader(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0u8; HASH_BUFFER_SIZE];

    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(finalize_blake3(hasher))
}

pub fn hash_persisted_nzb_bytes(bytes: &[u8]) -> [u8; 32] {
    let decoded = decode_persisted_nzb_bytes(bytes).unwrap_or_else(|_| bytes.to_vec());
    let mut hasher = blake3::Hasher::new();
    hasher.update(&decoded);
    finalize_blake3(hasher)
}

pub fn hash_persisted_nzb_or_empty(path: &Path) -> [u8; 32] {
    hash_persisted_nzb(path).unwrap_or_else(|_| finalize_blake3(blake3::Hasher::new()))
}

pub fn cleanup_orphaned_persisted_nzbs(
    nzb_dir: &Path,
    referenced_paths: &HashSet<PathBuf>,
) -> Result<usize, io::Error> {
    if !nzb_dir.exists() {
        return Ok(0);
    }

    let mut removed = 0usize;
    for entry in std::fs::read_dir(nzb_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let path = entry.path();
        if referenced_paths.contains(&path) {
            continue;
        }
        std::fs::remove_file(&path)?;
        removed += 1;
    }

    Ok(removed)
}

fn open_persisted_nzb_reader_from_buffer(
    mut reader: BufReader<File>,
) -> io::Result<PersistedNzbReader> {
    let is_zstd = {
        let header = reader.fill_buf()?;
        header.starts_with(&ZSTD_MAGIC)
    };

    if is_zstd {
        let decoder = zstd::stream::read::Decoder::with_buffer(reader)?;
        Ok(PersistedNzbReader::Zstd(BufReader::new(decoder)))
    } else {
        Ok(PersistedNzbReader::Plain(reader))
    }
}

fn finalize_blake3(hasher: blake3::Hasher) -> [u8; 32] {
    let digest = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_bytes());
    out
}
