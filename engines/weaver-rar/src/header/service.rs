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
    Ok(ServiceHeader { inner })
}
