use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// Monotonically increasing job identifier. One NZB submission = one job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct JobId(pub u64);

/// Identifies a file within an NZB (index into the NZB's `<file>` list).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NzbFileId {
    pub job_id: JobId,
    pub file_index: u32,
}

/// Identifies a segment (article) within an NZB file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SegmentId {
    pub file_id: NzbFileId,
    pub segment_number: u32,
}

/// NNTP article message-id (shared, immutable).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(pub Arc<str>);

/// Server identifier (index into the server config list).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServerId(pub u16);

/// Connection identifier within a server pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId {
    pub server: ServerId,
    pub index: u16,
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "job-{}", self.0)
    }
}

impl fmt::Display for NzbFileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/file-{}", self.job_id, self.file_index)
    }
}

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/seg-{}", self.file_id, self.segment_number)
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{}>", self.0)
    }
}

impl fmt::Display for ServerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "server-{}", self.0)
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/conn-{}", self.server, self.index)
    }
}

impl MessageId {
    pub fn new(id: &str) -> Self {
        Self(Arc::from(id))
    }
}

#[cfg(test)]
mod tests;
