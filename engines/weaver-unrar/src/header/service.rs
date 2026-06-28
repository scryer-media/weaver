//! Service header (type 3) parsing.
//!
//! Service headers have the same structure as file headers.
//! They are used for metadata like NTFS streams, recovery records, etc.
//! Common service header names: "CMT" (comment), "QO" (quick open),
//! "ACL" (NTFS ACL), "STM" (NTFS stream), "RR" (recovery record).

use crate::error::RarResult;
use crate::header::common::RawHeader;
use crate::header::file::{self, FileHeader};

/// Parsed service header. Structurally identical to a file header.
#[derive(Debug, Clone)]
pub struct ServiceHeader {
    /// The underlying file header fields.
    pub inner: FileHeader,
    /// Offset where the service header begins in the archive stream.
    pub header_offset: u64,
    /// Whether this service block depends on the preceding file block.
    pub is_child: bool,
    /// Whether UnRAR would preserve this child block when updating its host.
    pub is_inherited: bool,
}

impl ServiceHeader {
    /// Service record name (e.g. "CMT", "RR", "QO").
    pub fn service_name(&self) -> &str {
        &self.inner.name
    }
}

/// Parse a service header from a raw header.
pub fn parse(raw: &RawHeader, data_offset: u64) -> RarResult<ServiceHeader> {
    let inner = file::parse(raw, data_offset)?;
    Ok(ServiceHeader {
        inner,
        header_offset: raw.offset,
        is_child: raw.flags & crate::header::common::flags::CHILD != 0,
        is_inherited: raw.flags & crate::header::common::flags::INHERITED != 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::header::common::{self, HeaderType};
    use crate::vint::encode_vint;

    fn minimal_service_raw(flags: u64) -> RawHeader {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_vint(3)); // HEAD_SERVICE.
        body.extend_from_slice(&encode_vint(flags));
        body.extend_from_slice(&encode_vint(0)); // File flags.
        body.extend_from_slice(&encode_vint(0)); // Unpacked size.
        body.extend_from_slice(&encode_vint(0)); // Attributes.
        body.extend_from_slice(&encode_vint(0)); // Store compression.
        body.extend_from_slice(&encode_vint(3)); // Unix host OS.
        body.extend_from_slice(&encode_vint(3)); // Name length.
        body.extend_from_slice(b"CMT");

        RawHeader {
            offset: 42,
            crc32: 0,
            header_size: body.len() as u64,
            header_size_vint_len: 1,
            header_type: HeaderType::Service,
            flags,
            extra_area_size: 0,
            data_area_size: 0,
            body,
        }
    }

    #[test]
    fn service_preserves_unrar_child_and_inherited_header_flags() {
        let raw = minimal_service_raw(common::flags::CHILD | common::flags::INHERITED);
        let service = parse(&raw, 128).unwrap();

        assert_eq!(service.header_offset, 42);
        assert_eq!(service.service_name(), "CMT");
        assert!(service.is_child);
        assert!(service.is_inherited);
    }
}
