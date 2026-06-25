use std::path::PathBuf;

use super::keystore::KeyStore;

/// File-based key store. Stores the encryption master key as a plain text file
/// with restrictive permissions (0600 on Unix). Available on all platforms.
pub struct KeyFile {
    path: PathBuf,
}

impl KeyFile {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            path: data_dir.join("encryption.key"),
        }
    }
}

impl KeyStore for KeyFile {
    fn get_key(&self) -> Result<Option<String>, String> {
        match std::fs::read_to_string(&self.path) {
            Ok(contents) => {
                let trimmed = contents.trim().to_string();
                if trimmed.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(trimmed))
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(format!(
                "failed to read key file at {}: {e}",
                self.path.display()
            )),
        }
    }

    fn delete_key(&self) -> Result<(), String> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(format!(
                "failed to delete key file at {}: {e}",
                self.path.display()
            )),
        }
    }

    fn name(&self) -> &'static str {
        "key file"
    }
}
