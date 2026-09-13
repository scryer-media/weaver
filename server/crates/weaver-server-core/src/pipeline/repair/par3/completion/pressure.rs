//! Cleanup of native-owned staging outputs before a bounded disk retry.

use std::path::{Path, PathBuf};

pub(super) fn clear_temporaries(root: &Path, paths: &[PathBuf]) -> std::io::Result<()> {
    let root = root.canonicalize()?;
    for path in paths {
        let parent = path
            .parent()
            .ok_or_else(|| std::io::Error::other("PAR3 temporary has no parent"))?
            .canonicalize()?;
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        if !parent.starts_with(&root)
            || !name.starts_with(".par3-repair-")
            || !name.ends_with(".tmp")
        {
            return Err(std::io::Error::other(
                "PAR3 temporary is outside the job staging area",
            ));
        }
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
