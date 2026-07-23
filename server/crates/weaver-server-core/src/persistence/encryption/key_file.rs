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

    fn replace_key(&self, key: &str) -> Result<bool, String> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "failed to create key directory at {}: {error}",
                    parent.display()
                )
            })?;
        }
        let parent = self
            .path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let mut staged = tempfile::Builder::new()
            .prefix(".encryption-key-")
            .tempfile_in(parent)
            .map_err(|error| format!("failed to stage encryption key: {error}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            staged
                .as_file()
                .set_permissions(std::fs::Permissions::from_mode(0o600))
                .map_err(|error| format!("failed to protect staged encryption key: {error}"))?;
        }
        staged
            .write_all(key.as_bytes())
            .and_then(|()| staged.as_file().sync_all())
            .map_err(|error| format!("failed to write staged encryption key: {error}"))?;
        staged
            .persist(&self.path)
            .map_err(|error| format!("failed to replace encryption key: {}", error.error))?;
        #[cfg(unix)]
        std::fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| format!("failed to sync encryption key directory: {error}"))?;
        Ok(true)
    }

    fn can_replace(&self) -> bool {
        true
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replacement_is_atomic_and_owner_only() {
        let directory = tempfile::tempdir().unwrap();
        let store = KeyFile::new(directory.path().to_path_buf());
        store.create_key_if_absent("old-key").unwrap();
        assert!(store.replace_key("new-key").unwrap());
        assert_eq!(store.get_key().unwrap().as_deref(), Some("new-key"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(directory.path().join("encryption.key"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }
    }
}
