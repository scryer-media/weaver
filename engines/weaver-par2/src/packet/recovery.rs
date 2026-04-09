use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use bytes::Bytes;

use crate::error::{Par2Error, Result};
use crate::types::RecoveryExponent;

#[derive(Debug, Clone)]
pub enum RecoverySliceData {
    InMemory(Bytes),
    FileBacked {
        path: PathBuf,
        offset: u64,
        len: usize,
    },
}

impl RecoverySliceData {
    pub fn in_memory(data: Bytes) -> Self {
        Self::InMemory(data)
    }

    pub fn file_backed(path: PathBuf, offset: u64, len: usize) -> Self {
        Self::FileBacked { path, offset, len }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::InMemory(data) => data.len(),
            Self::FileBacked { len, .. } => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::InMemory(data) => Some(data.as_ref()),
            Self::FileBacked { .. } => None,
        }
    }

    pub fn file_span(&self) -> Option<(&Path, u64, usize)> {
        match self {
            Self::InMemory(_) => None,
            Self::FileBacked { path, offset, len } => Some((path.as_path(), *offset, *len)),
        }
    }

    pub fn to_vec(&self) -> io::Result<Vec<u8>> {
        let mut out = vec![0u8; self.len()];
        self.read_range_padded(0, &mut out)?;
        Ok(out)
    }

    pub fn read_range_padded(&self, start: usize, dst: &mut [u8]) -> io::Result<()> {
        dst.fill(0);

        match self {
            Self::InMemory(data) => {
                if start >= data.len() {
                    return Ok(());
                }
                let end = (start + dst.len()).min(data.len());
                let copy_len = end - start;
                dst[..copy_len].copy_from_slice(&data[start..end]);
                Ok(())
            }
            Self::FileBacked { path, offset, len } => {
                if start >= *len {
                    return Ok(());
                }

                let read_len = dst.len().min(*len - start);
                let mut file = File::open(path)?;
                read_exact_at_fallback(&mut file, offset + start as u64, &mut dst[..read_len])
            }
        }
    }
}

impl From<Bytes> for RecoverySliceData {
    fn from(value: Bytes) -> Self {
        Self::InMemory(value)
    }
}

impl From<Vec<u8>> for RecoverySliceData {
    fn from(value: Vec<u8>) -> Self {
        Self::InMemory(Bytes::from(value))
    }
}

fn read_exact_at_fallback(file: &mut File, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(buf)
}

/// Parsed Recovery Slice packet.
///
/// Contains one recovery block identified by its exponent.
/// The actual Reed-Solomon math is not performed here; we just store the data.
#[derive(Debug, Clone)]
pub struct RecoverySlicePacket {
    /// The exponent identifying this recovery block.
    pub exponent: RecoveryExponent,
    /// The recovery data (length should equal slice_size from the Main packet).
    pub data: RecoverySliceData,
}

impl RecoverySlicePacket {
    /// Parse a Recovery Slice packet from its body (after the 64-byte header).
    pub fn parse(body: &[u8]) -> Result<Self> {
        if body.len() < 4 {
            return Err(Par2Error::InvalidRecoveryPacket {
                reason: format!("body too short: {} bytes, need at least 4", body.len()),
            });
        }

        let exponent = u32::from_le_bytes(body[0..4].try_into().unwrap());
        let data = RecoverySliceData::in_memory(Bytes::copy_from_slice(&body[4..]));

        Ok(RecoverySlicePacket { exponent, data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_recovery() {
        let mut body = Vec::new();
        body.extend_from_slice(&42u32.to_le_bytes());
        body.extend_from_slice(&[0xAB; 128]);

        let pkt = RecoverySlicePacket::parse(&body).unwrap();
        assert_eq!(pkt.exponent, 42);
        assert_eq!(pkt.data.len(), 128);
        assert!(pkt.data.as_bytes().unwrap().iter().all(|&b| b == 0xAB));
    }

    #[test]
    fn parse_recovery_empty_data() {
        let body = 0u32.to_le_bytes();
        let pkt = RecoverySlicePacket::parse(&body).unwrap();
        assert_eq!(pkt.exponent, 0);
        assert_eq!(pkt.data.len(), 0);
    }

    #[test]
    fn reject_too_short() {
        let body = [0u8; 2];
        let err = RecoverySlicePacket::parse(&body).unwrap_err();
        assert!(matches!(err, Par2Error::InvalidRecoveryPacket { .. }));
    }
}
