use std::io::Write;
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

    fn create_key_if_absent(&self, key: &str) -> Result<Option<String>, String> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                format!(
                    "failed to create key directory at {}: {e}",
                    parent.display()
                )
            })?;
        }

        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }

        let mut file = match options.open(&self.path) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                return self
                    .get_key()?
                    .map(Some)
                    .ok_or_else(|| format!("key file at {} is empty", self.path.display()));
            }
            Err(e) => {
                return Err(format!(
                    "failed to create key file at {}: {e}",
                    self.path.display()
                ));
            }
        };

        let write_result = file
            .write_all(key.as_bytes())
            .and_then(|()| file.sync_all());
        if let Err(e) = write_result {
            let _ = std::fs::remove_file(&self.path);
            return Err(format!(
                "failed to persist key file at {}: {e}",
                self.path.display()
            ));
        }

        Ok(Some(key.to_string()))
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
