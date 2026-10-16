use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use super::{ExtractionRoot, NamespaceError};

// The coordinator's scratch admission covers one full donor inventory in
// addition to native repair outputs. Only one inventory exists at a time.
pub(super) struct Copies {
    root: PathBuf,
    pub(super) paths: Vec<PathBuf>,
}

impl Copies {
    pub(super) fn create(
        source_root: &Path,
        root: &Path,
        candidates: &BTreeSet<PathBuf>,
        limit: u64,
        cancellation: &par2_rs::CancellationToken,
    ) -> Result<Self, NamespaceError> {
        if cancellation.is_cancelled() {
            return Err(NamespaceError::Cancelled);
        }
        let source = std::fs::canonicalize(source_root)?;
        let parent = std::fs::canonicalize(root.parent().ok_or("donor root has no parent")?)?;
        let root = parent.join(root.file_name().ok_or("donor root has no name")?);
        if root.starts_with(&source) || source.starts_with(&root) {
            return Err("donor scratch must be separate from writable recovery outputs".into());
        }
        // Existing paths are never adopted or removed. Drop owns only the
        // directory successfully created by this operation.
        std::fs::create_dir(&root)?;
        let mut copies = Self {
            root,
            paths: Vec::new(),
        };
        let output =
            cap_std::fs::Dir::open_ambient_dir(&copies.root, cap_std::ambient_authority())?;
        let mut total = 0u64;
        let mut buffer = [0u8; 64 * 1024];
        for (index, candidate) in candidates.iter().enumerate() {
            if cancellation.is_cancelled() {
                return Err(NamespaceError::Cancelled);
            }
            // The explicit immutable inventory can belong to the preceding
            // generation. Open only this named regular file through its
            // parent capability; the engine sees solely the private copy.
            let parent = candidate.parent().ok_or("donor has no parent")?;
            let input_root = ExtractionRoot::open(parent)?;
            let name = candidate.file_name().ok_or("donor has no name")?;
            let mut input = input_root.open_input(Path::new(name))?;
            let length = input.metadata()?.len();
            total = total
                .checked_add(length)
                .filter(|bytes| *bytes <= limit)
                .ok_or(NamespaceError::ResourceLimited)?;
            let name = index.to_string();
            let mut options = cap_std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            let mut file = output.open_with(&name, &options)?;
            let mut remaining = length;
            while remaining != 0 {
                if cancellation.is_cancelled() {
                    return Err(NamespaceError::Cancelled);
                }
                let count = remaining.min(buffer.len() as u64) as usize;
                input.read_exact(&mut buffer[..count])?;
                file.write_all(&buffer[..count])?;
                remaining -= count as u64;
            }
            if input.read(&mut buffer[..1])? != 0 {
                return Err("donor length changed during private copy".into());
            }
            file.flush()?;
            copies.paths.push(copies.root.join(name));
        }
        Ok(copies)
    }
}

impl Drop for Copies {
    fn drop(&mut self) {
        // Session handles are dropped before this guard. Failed cleanup leaves
        // the owned scratch for terminal cleanup; the next create fails closed.
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
