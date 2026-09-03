//! The 32-byte 7z signature header, parsed on its own.
//!
//! Direct unpack needs the archive's exact total length before a single packed
//! byte has landed, because the gated reader answers `SeekFrom::End` from it and
//! the admission check needs to know how much is coming. The signature header
//! carries that: it is the first 32 bytes of the set's first part, and it
//! declares where the end header sits, which is also where the archive stops.
//!
//! Layout (all integers little-endian):
//!
//! | Range   | Field                                            |
//! |---------|--------------------------------------------------|
//! | `0..6`  | magic `37 7A BC AF 27 1C`                        |
//! | `6..8`  | format version (major, minor)                    |
//! | `8..12` | CRC-32 of bytes `12..32`                         |
//! | `12..20`| next header offset, relative to byte 32          |
//! | `20..28`| next header size                                 |
//! | `28..32`| CRC-32 of the end header                         |
//!
//! Weaver parses this itself rather than reaching into the 7z decoder: the
//! decoder wants a reader positioned over a complete archive, and at admission
//! time there is no complete archive — only these 32 bytes.

use std::fmt;
use std::ops::Range;

/// Byte length of the signature header.
pub const SIGNATURE_HEADER_LEN: u64 = 32;

/// The 7z magic bytes that open every archive.
pub const MAGIC: [u8; 6] = [0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C];

/// Why a byte slice is not a usable 7z signature header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartHeaderError {
    /// Fewer than [`SIGNATURE_HEADER_LEN`] bytes were supplied.
    TooShort {
        /// How many bytes the caller actually had.
        len: usize,
    },
    /// The leading six bytes are not [`MAGIC`].
    BadMagic,
    /// The stored CRC-32 does not match the bytes it covers.
    CrcMismatch {
        /// CRC recorded in bytes `8..12`.
        expected: u32,
        /// CRC computed over bytes `12..32`.
        actual: u32,
    },
    /// `32 + next_header_offset + next_header_size` does not fit in a `u64`.
    LengthOverflow {
        /// The declared end-header offset.
        next_header_offset: u64,
        /// The declared end-header size.
        next_header_size: u64,
    },
}

impl fmt::Display for StartHeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooShort { len } => {
                write!(f, "7z signature header needs 32 bytes, got {len}")
            }
            Self::BadMagic => f.write_str("not a 7z archive: signature magic mismatch"),
            Self::CrcMismatch { expected, actual } => write!(
                f,
                "7z signature header CRC mismatch: declared {expected:#010x}, computed {actual:#010x}"
            ),
            Self::LengthOverflow {
                next_header_offset,
                next_header_size,
            } => write!(
                f,
                "7z signature header declares an impossible archive length: 32 + {next_header_offset} + {next_header_size} overflows"
            ),
        }
    }
}

impl std::error::Error for StartHeaderError {}

/// The two lengths the signature header exists to carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartHeader {
    /// Offset of the end header, relative to byte 32 of the archive.
    pub next_header_offset: u64,
    /// Byte length of the end header.
    pub next_header_size: u64,
    /// CRC-32 the end header is expected to hash to.
    pub next_header_crc: u32,
}

impl StartHeader {
    /// Parse and validate the signature header.
    ///
    /// Only the first [`SIGNATURE_HEADER_LEN`] bytes are read; a longer slice
    /// (the head of a part file, say) is accepted as-is.
    pub fn parse(bytes: &[u8]) -> Result<Self, StartHeaderError> {
        let header: &[u8; 32] = bytes
            .get(..32)
            .and_then(|slice| slice.try_into().ok())
            .ok_or(StartHeaderError::TooShort { len: bytes.len() })?;

        if header[..6] != MAGIC {
            return Err(StartHeaderError::BadMagic);
        }

        // Bytes 6..8 are the format version. Deliberately unvalidated: the
        // decoder is what decides whether it can read a given version, and
        // rejecting here would refuse archives it would have handled.

        let expected = u32::from_le_bytes(header[8..12].try_into().expect("4-byte slice"));
        let actual =
            crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, &header[12..32]) as u32;
        if expected != actual {
            return Err(StartHeaderError::CrcMismatch { expected, actual });
        }

        Ok(Self {
            next_header_offset: u64::from_le_bytes(
                header[12..20].try_into().expect("8-byte slice"),
            ),
            next_header_size: u64::from_le_bytes(header[20..28].try_into().expect("8-byte slice")),
            next_header_crc: u32::from_le_bytes(header[28..32].try_into().expect("4-byte slice")),
        })
    }

    /// Exact total byte length of the archive the header opens.
    ///
    /// The end header is the last structure in the file, so the archive ends
    /// where it ends.
    pub fn total_len(&self) -> Result<u64, StartHeaderError> {
        SIGNATURE_HEADER_LEN
            .checked_add(self.next_header_offset)
            .and_then(|offset| offset.checked_add(self.next_header_size))
            .ok_or(StartHeaderError::LengthOverflow {
                next_header_offset: self.next_header_offset,
                next_header_size: self.next_header_size,
            })
    }

    /// Absolute byte range occupied by the end header.
    ///
    /// The range a direct-unpack worker has to have on disk before the decoder
    /// can list the archive at all, which is why it is worth prefetching ahead
    /// of the packed streams that precede it.
    pub fn end_header_range(&self) -> Result<Range<u64>, StartHeaderError> {
        let start = SIGNATURE_HEADER_LEN
            .checked_add(self.next_header_offset)
            .ok_or(StartHeaderError::LengthOverflow {
                next_header_offset: self.next_header_offset,
                next_header_size: self.next_header_size,
            })?;
        Ok(start..self.total_len()?)
    }

    /// Absolute byte range holding the packed streams, between the signature
    /// header and the end header.
    pub fn packed_range(&self) -> Result<Range<u64>, StartHeaderError> {
        Ok(SIGNATURE_HEADER_LEN..self.end_header_range()?.start)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a well-formed signature header for the declared lengths.
    fn signature_header(next_header_offset: u64, next_header_size: u64) -> [u8; 32] {
        let mut header = [0u8; 32];
        header[..6].copy_from_slice(&MAGIC);
        header[6] = 0;
        header[7] = 4;
        header[12..20].copy_from_slice(&next_header_offset.to_le_bytes());
        header[20..28].copy_from_slice(&next_header_size.to_le_bytes());
        header[28..32].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
        let crc = crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, &header[12..32]) as u32;
        header[8..12].copy_from_slice(&crc.to_le_bytes());
        header
    }

    #[test]
    fn parses_declared_lengths_and_derives_total() {
        let header = signature_header(1_000, 200);
        let parsed = StartHeader::parse(&header).expect("valid header");

        assert_eq!(parsed.next_header_offset, 1_000);
        assert_eq!(parsed.next_header_size, 200);
        assert_eq!(parsed.next_header_crc, 0xDEAD_BEEF);
        assert_eq!(parsed.total_len().expect("no overflow"), 32 + 1_000 + 200);
        assert_eq!(
            parsed.end_header_range().expect("no overflow"),
            1_032..1_232
        );
        assert_eq!(parsed.packed_range().expect("no overflow"), 32..1_032);
    }

    #[test]
    fn trailing_bytes_beyond_the_header_are_ignored() {
        let mut bytes = signature_header(64, 16).to_vec();
        bytes.extend_from_slice(&[0xAB; 512]);

        let parsed = StartHeader::parse(&bytes).expect("valid header");
        assert_eq!(parsed.total_len().expect("no overflow"), 112);
    }

    #[test]
    fn rejects_a_short_slice() {
        let header = signature_header(10, 10);
        assert_eq!(
            StartHeader::parse(&header[..31]),
            Err(StartHeaderError::TooShort { len: 31 })
        );
    }

    #[test]
    fn rejects_foreign_magic() {
        let mut header = signature_header(10, 10);
        header[0] = 0x52;
        assert_eq!(StartHeader::parse(&header), Err(StartHeaderError::BadMagic));
    }

    #[test]
    fn rejects_a_corrupted_length_field() {
        let mut header = signature_header(10, 10);
        // Flip a byte the CRC covers without repairing the CRC.
        header[12] ^= 0xFF;

        assert!(matches!(
            StartHeader::parse(&header),
            Err(StartHeaderError::CrcMismatch { .. })
        ));
    }

    #[test]
    fn rejects_lengths_that_overflow_the_total() {
        let header = signature_header(u64::MAX, 64);
        let parsed = StartHeader::parse(&header).expect("CRC is still valid");

        assert!(matches!(
            parsed.total_len(),
            Err(StartHeaderError::LengthOverflow { .. })
        ));
        assert!(matches!(
            parsed.end_header_range(),
            Err(StartHeaderError::LengthOverflow { .. })
        ));
    }
}
