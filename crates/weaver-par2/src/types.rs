use std::fmt;

/// 16-byte MD5-based file identifier.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileId(pub(crate) [u8; 16]);

impl FileId {
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FileId(")?;
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        write!(f, ")")
    }
}

impl fmt::Display for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

/// 16-byte recovery set identifier (MD5 of main packet body).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecoverySetId(pub(crate) [u8; 16]);

impl RecoverySetId {
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for RecoverySetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecoverySetId(")?;
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        write!(f, ")")
    }
}

impl fmt::Display for RecoverySetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

/// Index of a slice within a file.
pub type SliceIndex = u32;

/// Exponent of a recovery block.
pub type RecoveryExponent = u32;

/// CRC32 + MD5 checksum pair for a single file slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SliceChecksum {
    pub crc32: u32,
    pub md5: [u8; 16],
}

/// Lightweight cooperative cancellation token.
///
/// Clone and share across threads. Once cancelled, all holders observe it.
#[derive(Clone)]
pub struct CancellationToken(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl CancellationToken {
    pub fn new() -> Self {
        Self(std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)))
    }

    pub fn cancel(&self) {
        self.0.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Progress update from a long-running PAR2 operation.
#[derive(Debug, Clone)]
pub struct ProgressUpdate {
    /// What stage the operation is in.
    pub stage: ProgressStage,
    /// Current item index (0-based).
    pub current: u32,
    /// Total number of items.
    pub total: u32,
    /// Cumulative bytes processed so far.
    pub bytes_processed: u64,
}

/// Stage of a PAR2 operation for progress reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressStage {
    /// Verifying file integrity.
    Verifying,
    /// Reading recovery slice data.
    ReadingRecovery,
    /// Reconstructing missing slices via Reed-Solomon.
    Repairing,
    /// Writing repaired data back to files.
    WritingRepaired,
}

/// Callback type for progress reporting.
pub type ProgressCallback = Box<dyn Fn(ProgressUpdate) + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_id_display() {
        let id = FileId([
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54,
            0x32, 0x10,
        ]);
        assert_eq!(format!("{id}"), "0123456789abcdeffedcba9876543210");
    }

    #[test]
    fn file_id_debug() {
        let id = FileId([0; 16]);
        let dbg = format!("{id:?}");
        assert!(dbg.starts_with("FileId("));
        assert!(dbg.ends_with(')'));
    }

    #[test]
    fn recovery_set_id_roundtrip() {
        let bytes = [1u8; 16];
        let id = RecoverySetId::from_bytes(bytes);
        assert_eq!(*id.as_bytes(), bytes);
    }

    #[test]
    fn file_id_equality() {
        let a = FileId([0xAA; 16]);
        let b = FileId([0xAA; 16]);
        let c = FileId([0xBB; 16]);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
