//! End of archive header (type 5) parsing.
//!
//! Flags:
//! - 0x0001: More volumes follow

use crate::error::RarResult;
use crate::header::common::{self, RawHeader};
use crate::vint;

/// End-of-archive flags.
pub mod flags {
    /// More volumes follow in the set.
    pub const MORE_VOLUMES: u64 = 0x0001;
}

/// Parsed end of archive header.
#[derive(Debug, Clone)]
pub struct EndArchiveHeader {
    /// Whether more volumes follow.
    pub more_volumes: bool,
    /// End-of-archive specific flags (raw).
    pub end_flags: u64,
}

/// Parse an end-of-archive header from a raw header.
pub fn parse(raw: &RawHeader) -> RarResult<EndArchiveHeader> {
    let offset = common::type_specific_offset(raw)?;
    let body = &raw.body[offset..];

    let end_flags = if !body.is_empty() {
        let (f, _) = vint::read_vint(body)?;
        f
    } else {
        0
    };

    let more_volumes = end_flags & flags::MORE_VOLUMES != 0;

    Ok(EndArchiveHeader {
        more_volumes,
        end_flags,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::common::read_raw_header;
    use crate::vint::encode_vint;

    fn build_end_header(end_flags: u64) -> Vec<u8> {
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_vint(end_flags));

        let header_type = 5u64;
        let header_flags = 0u64;

        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(header_type));
        body.extend_from_slice(&encode_vint(header_flags));
        body.extend_from_slice(&type_body);

        let header_size = body.len() as u64;
        let header_size_bytes = encode_vint(header_size);

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header_size_bytes);
        hasher.update(&body);
        let crc = hasher.finalize();

        let mut data = Vec::new();
        data.extend_from_slice(&crc.to_le_bytes());
        data.extend_from_slice(&header_size_bytes);
        data.extend_from_slice(&body);
        data
    }

    #[test]
    fn test_end_archive_no_more() {
        let data = build_end_header(0);
        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let end = parse(&raw).unwrap();
        assert!(!end.more_volumes);
    }

    #[test]
    fn test_end_archive_more_volumes() {
        let data = build_end_header(flags::MORE_VOLUMES);
        let mut cursor = std::io::Cursor::new(data);
        let raw = read_raw_header(&mut cursor).unwrap().unwrap();
        let end = parse(&raw).unwrap();
        assert!(end.more_volumes);
    }
}
