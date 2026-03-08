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
mod tests {
    use super::*;

    #[test]
    fn job_id_display() {
        assert_eq!(JobId(42).to_string(), "job-42");
    }

    #[test]
    fn segment_id_equality_and_hash() {
        use std::collections::HashSet;

        let a = SegmentId {
            file_id: NzbFileId {
                job_id: JobId(1),
                file_index: 0,
            },
            segment_number: 5,
        };
        let b = SegmentId {
            file_id: NzbFileId {
                job_id: JobId(1),
                file_index: 0,
            },
            segment_number: 5,
        };
        let c = SegmentId {
            file_id: NzbFileId {
                job_id: JobId(1),
                file_index: 0,
            },
            segment_number: 6,
        };

        assert_eq!(a, b);
        assert_ne!(a, c);

        let mut set = HashSet::new();
        set.insert(a);
        assert!(set.contains(&b));
        assert!(!set.contains(&c));
    }

    #[test]
    fn message_id_display() {
        let mid = MessageId::new("abc123@example.com");
        assert_eq!(mid.to_string(), "<abc123@example.com>");
    }

    #[test]
    fn message_id_clone_is_cheap() {
        let mid = MessageId::new("test@example.com");
        let cloned = mid.clone();
        // Arc: both point to same allocation
        assert!(Arc::ptr_eq(&mid.0, &cloned.0));
    }

    #[test]
    fn nzb_file_id_display() {
        let fid = NzbFileId {
            job_id: JobId(3),
            file_index: 7,
        };
        assert_eq!(fid.to_string(), "job-3/file-7");
    }

    #[test]
    fn segment_id_display() {
        let sid = SegmentId {
            file_id: NzbFileId {
                job_id: JobId(1),
                file_index: 2,
            },
            segment_number: 10,
        };
        assert_eq!(sid.to_string(), "job-1/file-2/seg-10");
    }

    #[test]
    fn serde_roundtrip() {
        let job = JobId(99);
        let json = serde_json::to_string(&job).unwrap();
        let back: JobId = serde_json::from_str(&json).unwrap();
        assert_eq!(job, back);

        let seg = SegmentId {
            file_id: NzbFileId {
                job_id: JobId(1),
                file_index: 2,
            },
            segment_number: 3,
        };
        let json = serde_json::to_string(&seg).unwrap();
        let back: SegmentId = serde_json::from_str(&json).unwrap();
        assert_eq!(seg, back);
    }
}
