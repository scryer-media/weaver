use std::path::PathBuf;

pub trait KeyStore: Send + Sync {
    fn get_key(&self) -> Result<Option<String>, String>;
    /// Persist a newly generated key without replacing an existing key.
    ///
    /// Stores that are externally managed or read-only return `Ok(None)`.
    fn create_key_if_absent(&self, _key: &str) -> Result<Option<String>, String> {
        Ok(None)
    }
    /// Atomically replace a Weaver-managed key. Externally managed stores
    /// return `Ok(false)` and must be validated instead of overwritten.
    fn replace_key(&self, _key: &str) -> Result<bool, String> {
        Ok(false)
    }
    fn can_replace(&self) -> bool {
        false
    }
    #[allow(dead_code)]
    fn delete_key(&self) -> Result<(), String>;
    fn name(&self) -> &'static str;
}

#[cfg(target_os = "macos")]
fn force_key_file() -> bool {
    std::env::var("WEAVER_FORCE_KEY_FILE")
        .map(|value| {
            let value = value.trim().to_ascii_lowercase();
            !value.is_empty() && !matches!(value.as_str(), "0" | "false" | "no" | "off")
        })
        .unwrap_or(false)
}

#[allow(clippy::vec_init_then_push)]
pub fn platform_keystores(_data_dir: Option<PathBuf>) -> Vec<Box<dyn KeyStore>> {
    let mut stores: Vec<Box<dyn KeyStore>> = Vec::new();

    // Skip macOS Keychain in debug builds — each recompile changes the binary
    // hash, causing repeated password prompts. Use file-based key instead.
    #[cfg(target_os = "macos")]
    if force_key_file() || cfg!(debug_assertions) {
        if let Some(dir) = _data_dir.clone() {
            stores.push(Box::new(super::key_file::KeyFile::new(dir)));
        }
    } else {
        stores.push(Box::new(super::macos::MacOSKeychain::new()));
    }

    #[cfg(target_os = "windows")]
    stores.push(Box::new(super::windows::WindowsCredentialManager::new()));

    #[cfg(target_os = "linux")]
    {
        stores.push(Box::new(super::linux::DockerSecret));
        if let Some(dir) = _data_dir {
            stores.push(Box::new(super::key_file::KeyFile::new(dir)));
        }
    }

    stores
}
