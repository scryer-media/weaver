use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::path::Path;

use sha2::{Digest, Sha256};
use weaver_nzb::{Nzb, NzbError, parse_nzb_reader};

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
const HASH_BUFFER_SIZE: usize = 64 * 1024;

pub(crate) enum PersistedNzbReader {
    Plain(BufReader<File>),
    Zstd(BufReader<zstd::stream::read::Decoder<'static, BufReader<File>>>),
}

#[derive(Debug)]
pub(crate) enum PersistedNzbError {
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

pub(crate) fn open_persisted_nzb_reader(path: &Path) -> io::Result<PersistedNzbReader> {
    let reader = BufReader::new(File::open(path)?);
    open_persisted_nzb_reader_from_buffer(reader)
}

pub(crate) fn parse_persisted_nzb(path: &Path) -> Result<Nzb, PersistedNzbError> {
    let reader = open_persisted_nzb_reader(path).map_err(PersistedNzbError::Io)?;
    parse_nzb_reader(reader).map_err(PersistedNzbError::Parse)
}

pub(crate) fn hash_persisted_nzb(path: &Path) -> io::Result<[u8; 32]> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; HASH_BUFFER_SIZE];

    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(finalize_sha256(hasher))
}

pub(crate) fn hash_persisted_nzb_or_empty(path: &Path) -> [u8; 32] {
    hash_persisted_nzb(path).unwrap_or_else(|_| finalize_sha256(Sha256::new()))
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

fn finalize_sha256(hasher: Sha256) -> [u8; 32] {
    let digest = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Write;

    fn minimal_nzb(name: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="test@test.com" date="1234567890" subject="{name} - &quot;file.rar&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="500000" number="1">{name}-seg1@test.com</segment></segments>
  </file>
</nzb>"#
        )
    }

    #[test]
    fn parse_persisted_zstd_nzb() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("job.nzb");
        let xml = minimal_nzb("persisted-zstd");
        let compressed = zstd::bulk::compress(xml.as_bytes(), 3).unwrap();
        std::fs::write(&path, compressed).unwrap();

        let nzb = parse_persisted_nzb(&path).unwrap();

        assert_eq!(nzb.files.len(), 1);
        assert_eq!(
            nzb.files[0].subject,
            "persisted-zstd - \"file.rar\" yEnc (1/1)"
        );
    }

    #[test]
    fn parse_persisted_plain_nzb() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("job.nzb");
        std::fs::write(&path, minimal_nzb("persisted-plain")).unwrap();

        let nzb = parse_persisted_nzb(&path).unwrap();

        assert_eq!(nzb.files.len(), 1);
        assert_eq!(
            nzb.files[0].subject,
            "persisted-plain - \"file.rar\" yEnc (1/1)"
        );
    }

    #[test]
    fn parse_persisted_corrupt_zstd_fails() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("job.nzb");
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(&ZSTD_MAGIC).unwrap();
        file.write_all(b"not-a-valid-frame").unwrap();
        file.flush().unwrap();

        assert!(parse_persisted_nzb(&path).is_err());
    }

    #[test]
    fn hash_persisted_nzb_matches_stored_bytes_hash() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("job.nzb");
        let compressed = zstd::bulk::compress(minimal_nzb("persisted-hash").as_bytes(), 3).unwrap();
        std::fs::write(&path, &compressed).unwrap();

        let expected = {
            let mut hasher = Sha256::new();
            hasher.update(&compressed);
            finalize_sha256(hasher)
        };

        assert_eq!(hash_persisted_nzb(&path).unwrap(), expected);
    }
}
