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

    fn set_key(&self, key_base64: &str) -> Result<(), String> {
        use std::io::Write;

        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create directory {}: {e}", parent.display()))?;
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&self.path)
                .map_err(|e| {
                    format!("failed to create key file at {}: {e}", self.path.display())
                })?;
            file.write_all(key_base64.as_bytes())
                .map_err(|e| format!("failed to write key file at {}: {e}", self.path.display()))?;
        }

        #[cfg(not(unix))]
        {
            std::fs::write(&self.path, key_base64)
                .map_err(|e| format!("failed to write key file at {}: {e}", self.path.display()))?;
        }

        Ok(())
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
