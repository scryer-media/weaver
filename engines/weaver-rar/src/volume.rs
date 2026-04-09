//! Multi-volume topology tracking and on-disk volume providers.
//!
//! Tracks which volumes are present, which members span which volumes,
//! and provides queries for extraction readiness.

use crate::archive::ReadSeek;
use crate::types::VolumeSpan;
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

/// State of a volume in the set.
#[derive(Debug)]
pub enum VolumeState {
    /// Volume is available for reading.
    Present,
    /// Volume is known to exist but not yet available.
    Missing,
}

/// Tracks the topology of a multi-volume archive set.
#[derive(Debug)]
pub struct VolumeSet {
    /// Ordered list of volume states (index = volume number).
    volumes: Vec<VolumeState>,
    /// Whether the last volume has been seen (end of archive without more_volumes flag).
    last_volume_seen: bool,
}

impl VolumeSet {
    /// Create a new volume set with a single volume (the initial archive).
    pub fn single() -> Self {
        Self {
            volumes: vec![VolumeState::Present],
            last_volume_seen: false,
        }
    }

    /// Create a new volume set for a multi-volume archive.
    pub fn new() -> Self {
        Self {
            volumes: Vec::new(),
            last_volume_seen: false,
        }
    }

    /// Register a volume as present.
    pub fn add_volume(&mut self, index: usize) {
        while self.volumes.len() <= index {
            self.volumes.push(VolumeState::Missing);
        }
        self.volumes[index] = VolumeState::Present;
    }

    /// Mark a volume as missing (known to exist but not available).
    pub fn mark_missing(&mut self, index: usize) {
        while self.volumes.len() <= index {
            self.volumes.push(VolumeState::Missing);
        }
        self.volumes[index] = VolumeState::Missing;
    }

    /// Mark that the last volume in the set has been seen.
    pub fn set_last_volume_seen(&mut self) {
        self.last_volume_seen = true;
    }

    /// Returns true if we know this is the complete set (last volume seen).
    pub fn is_complete(&self) -> bool {
        self.last_volume_seen
    }

    /// Returns the number of known volumes.
    pub fn volume_count(&self) -> usize {
        self.volumes.len()
    }

    /// Check if a volume is present.
    pub fn is_present(&self, index: usize) -> bool {
        self.volumes
            .get(index)
            .is_some_and(|s| matches!(s, VolumeState::Present))
    }

    /// Check if all volumes needed for a member span are present.
    pub fn is_extractable(&self, span: &VolumeSpan) -> bool {
        (span.first_volume..=span.last_volume).all(|i| self.is_present(i))
    }

    /// List missing volumes needed for a member span.
    pub fn missing_volumes(&self, span: &VolumeSpan) -> Vec<usize> {
        (span.first_volume..=span.last_volume)
            .filter(|&i| !self.is_present(i))
            .collect()
    }

    /// Get the total expected volume count (if known).
    pub fn expected_count(&self) -> Option<usize> {
        if self.last_volume_seen {
            Some(self.volumes.len())
        } else {
            None
        }
    }

    /// Return a compact bitmap of known volume presence.
    pub fn presence(&self) -> Vec<bool> {
        self.volumes
            .iter()
            .map(|state| matches!(state, VolumeState::Present))
            .collect()
    }

    /// Return whether the terminating volume has been observed.
    pub fn last_volume_seen(&self) -> bool {
        self.last_volume_seen
    }

    /// Restore a volume set from serialized presence state.
    pub fn from_presence(presence: Vec<bool>, last_volume_seen: bool) -> Self {
        let volumes = presence
            .into_iter()
            .map(|present| {
                if present {
                    VolumeState::Present
                } else {
                    VolumeState::Missing
                }
            })
            .collect();
        Self {
            volumes,
            last_volume_seen,
        }
    }
}

impl Default for VolumeSet {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// VolumeProvider: on-demand volume access for streaming extraction
// ---------------------------------------------------------------------------

/// Error returned by a [`VolumeProvider`].
#[derive(Debug)]
pub enum VolumeProviderError {
    /// Volume will never become available.
    Unavailable { volume: usize, reason: String },
    /// I/O error opening the volume file.
    Io(std::io::Error),
}

impl fmt::Display for VolumeProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable { volume, reason } => {
                write!(f, "volume {volume} unavailable: {reason}")
            }
            Self::Io(e) => write!(f, "volume I/O error: {e}"),
        }
    }
}

impl std::error::Error for VolumeProviderError {}

/// Provides volume readers on demand for streaming extraction.
///
/// Implementations return readers for volumes already on disk.
pub trait VolumeProvider: Send + Sync {
    /// Get a reader for the given volume index.
    fn get_volume(&self, index: usize) -> Result<Box<dyn ReadSeek>, VolumeProviderError>;
}

/// Volume provider where all volumes are already on disk.
pub struct StaticVolumeProvider {
    paths: HashMap<usize, PathBuf>,
}

impl StaticVolumeProvider {
    pub fn new(paths: HashMap<usize, PathBuf>) -> Self {
        Self { paths }
    }

    pub fn from_ordered(paths: Vec<PathBuf>) -> Self {
        Self {
            paths: paths.into_iter().enumerate().collect(),
        }
    }
}

impl VolumeProvider for StaticVolumeProvider {
    fn get_volume(&self, index: usize) -> Result<Box<dyn ReadSeek>, VolumeProviderError> {
        let path = self
            .paths
            .get(&index)
            .ok_or_else(|| VolumeProviderError::Unavailable {
                volume: index,
                reason: "not registered".into(),
            })?;
        let file = std::fs::File::open(path).map_err(VolumeProviderError::Io)?;
        Ok(Box::new(file))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_volume() {
        let vs = VolumeSet::single();
        assert!(vs.is_present(0));
        assert!(!vs.is_present(1));
        assert_eq!(vs.volume_count(), 1);
    }

    #[test]
    fn test_multi_volume() {
        let mut vs = VolumeSet::new();
        vs.add_volume(0);
        vs.add_volume(1);
        vs.add_volume(2);
        assert!(vs.is_present(0));
        assert!(vs.is_present(1));
        assert!(vs.is_present(2));
        assert_eq!(vs.volume_count(), 3);
    }

    #[test]
    fn test_missing_volumes() {
        let mut vs = VolumeSet::new();
        vs.add_volume(0);
        vs.mark_missing(1);
        vs.add_volume(2);

        let span = VolumeSpan {
            first_volume: 0,
            last_volume: 2,
        };
        assert!(!vs.is_extractable(&span));
        assert_eq!(vs.missing_volumes(&span), vec![1]);
    }

    #[test]
    fn test_extractable_single() {
        let vs = VolumeSet::single();
        let span = VolumeSpan::single(0);
        assert!(vs.is_extractable(&span));
    }

    #[test]
    fn test_expected_count() {
        let mut vs = VolumeSet::new();
        vs.add_volume(0);
        vs.add_volume(1);
        assert!(vs.expected_count().is_none());

        vs.set_last_volume_seen();
        assert_eq!(vs.expected_count(), Some(2));
    }

    #[test]
    fn test_sparse_volume_registration() {
        let mut vs = VolumeSet::new();
        vs.add_volume(5); // Registers 0..=5, only 5 is Present
        assert_eq!(vs.volume_count(), 6);
        assert!(!vs.is_present(0));
        assert!(!vs.is_present(4));
        assert!(vs.is_present(5));
    }

    #[test]
    fn test_static_volume_provider() {
        let dir = std::env::temp_dir().join("weaver_test_static_vp");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("vol0.rar");
        std::fs::write(&path, b"test data").unwrap();

        let provider = StaticVolumeProvider::from_ordered(vec![path]);
        let mut reader = provider.get_volume(0).unwrap();
        let mut buf = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut buf).unwrap();
        assert_eq!(buf, b"test data");

        assert!(provider.get_volume(1).is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
