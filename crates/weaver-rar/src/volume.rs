//! Multi-volume topology tracking and on-demand volume providers.
//!
//! Tracks which volumes are present, which members span which volumes,
//! and provides queries for extraction readiness.
//!
//! Also defines [`VolumeProvider`] for streaming extraction: volumes can
//! be requested on demand, with [`WaitingVolumeProvider`] blocking until
//! a volume finishes downloading.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Condvar, Mutex};

use crate::archive::ReadSeek;
use crate::types::VolumeSpan;

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
    /// Extraction was cancelled (job removed, app shutting down).
    Cancelled,
    /// I/O error opening the volume file.
    Io(std::io::Error),
}

impl fmt::Display for VolumeProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable { volume, reason } => {
                write!(f, "volume {volume} unavailable: {reason}")
            }
            Self::Cancelled => write!(f, "extraction cancelled"),
            Self::Io(e) => write!(f, "volume I/O error: {e}"),
        }
    }
}

impl std::error::Error for VolumeProviderError {}

/// Provides volume readers on demand for streaming extraction.
///
/// Implementations may block (e.g. waiting for a download to finish) or
/// return immediately if all volumes are already on disk.
pub trait VolumeProvider: Send + Sync {
    /// Get a reader for the given volume index.
    ///
    /// May block until the volume becomes available. Returns an error if
    /// the volume will never be available or if extraction was cancelled.
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
        let path = self.paths.get(&index).ok_or_else(|| {
            VolumeProviderError::Unavailable {
                volume: index,
                reason: "not registered".into(),
            }
        })?;
        let file = std::fs::File::open(path).map_err(VolumeProviderError::Io)?;
        Ok(Box::new(file))
    }
}

/// Internal state for [`WaitingVolumeProvider`].
struct WaitingState {
    /// Volumes that have completed downloading.
    available: HashMap<usize, PathBuf>,
    /// Set when the download is done (all volumes available) or cancelled.
    finished: bool,
    /// Error message if cancelled/failed.
    error: Option<String>,
}

/// Volume provider that blocks until volumes finish downloading.
///
/// The async pipeline calls [`volume_ready`] as each volume completes.
/// The extraction thread (running in `spawn_blocking`) calls [`get_volume`],
/// which blocks on a condvar until the requested volume is available.
pub struct WaitingVolumeProvider {
    state: Mutex<WaitingState>,
    notify: Condvar,
}

impl WaitingVolumeProvider {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(WaitingState {
                available: HashMap::new(),
                finished: false,
                error: None,
            }),
            notify: Condvar::new(),
        }
    }

    /// Signal that a volume has finished downloading and is on disk.
    pub fn volume_ready(&self, index: usize, path: PathBuf) {
        let mut state = self.state.lock().unwrap();
        state.available.insert(index, path);
        self.notify.notify_all();
    }

    /// Signal that all volumes have been provided.
    pub fn mark_finished(&self) {
        let mut state = self.state.lock().unwrap();
        state.finished = true;
        self.notify.notify_all();
    }

    /// Signal that the download was cancelled or failed.
    pub fn mark_cancelled(&self, reason: String) {
        let mut state = self.state.lock().unwrap();
        state.finished = true;
        state.error = Some(reason);
        self.notify.notify_all();
    }
}

impl VolumeProvider for WaitingVolumeProvider {
    fn get_volume(&self, index: usize) -> Result<Box<dyn ReadSeek>, VolumeProviderError> {
        let state = self.state.lock().unwrap();
        let state = self
            .notify
            .wait_while(state, |s| {
                !s.available.contains_key(&index) && !s.finished && s.error.is_none()
            })
            .unwrap();

        // Check for cancellation first.
        if let Some(ref err) = state.error {
            if !state.available.contains_key(&index) {
                return Err(VolumeProviderError::Unavailable {
                    volume: index,
                    reason: err.clone(),
                });
            }
        }

        let path = state.available.get(&index).ok_or_else(|| {
            if state.finished {
                VolumeProviderError::Unavailable {
                    volume: index,
                    reason: "download finished without this volume".into(),
                }
            } else {
                VolumeProviderError::Cancelled
            }
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

    #[test]
    fn test_waiting_volume_provider() {
        use std::sync::Arc;
        use std::thread;

        let dir = std::env::temp_dir().join("weaver_test_waiting_vp");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("vol0.rar");
        std::fs::write(&path, b"volume zero").unwrap();

        let provider = Arc::new(WaitingVolumeProvider::new());
        let p2 = Arc::clone(&provider);

        // Spawn a thread that requests volume 0 (will block briefly).
        let handle = thread::spawn(move || {
            let mut reader = p2.get_volume(0).unwrap();
            let mut buf = Vec::new();
            std::io::Read::read_to_end(&mut reader, &mut buf).unwrap();
            buf
        });

        // Signal that volume 0 is ready.
        provider.volume_ready(0, path);

        let result = handle.join().unwrap();
        assert_eq!(result, b"volume zero");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_waiting_volume_provider_cancelled() {
        use std::sync::Arc;
        use std::thread;

        let provider = Arc::new(WaitingVolumeProvider::new());
        let p2 = Arc::clone(&provider);

        let handle = thread::spawn(move || p2.get_volume(0));

        // Cancel while the thread is waiting.
        provider.mark_cancelled("job removed".into());

        let result = handle.join().unwrap();
        assert!(result.is_err());
    }
}
