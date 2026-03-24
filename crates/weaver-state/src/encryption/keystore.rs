use std::path::PathBuf;

pub trait KeyStore: Send + Sync {
    fn get_key(&self) -> Result<Option<String>, String>;
    fn set_key(&self, key_base64: &str) -> Result<(), String>;
    #[allow(dead_code)]
    fn delete_key(&self) -> Result<(), String>;
    fn name(&self) -> &'static str;
}

#[allow(clippy::vec_init_then_push)]
pub fn platform_keystores(_data_dir: Option<PathBuf>) -> Vec<Box<dyn KeyStore>> {
    let mut stores: Vec<Box<dyn KeyStore>> = Vec::new();

    // Skip macOS Keychain in debug builds — each recompile changes the binary
    // hash, causing repeated password prompts. Use file-based key instead.
    #[cfg(target_os = "macos")]
    if cfg!(debug_assertions) {
        if let Some(dir) = _data_dir.clone() {
            stores.push(Box::new(super::key_file::KeyFile::new(dir)));
        }
    } else {
        stores.push(Box::new(super::macos::MacOSKeychain));
    }

    #[cfg(target_os = "windows")]
    stores.push(Box::new(super::windows::WindowsCredentialManager));

    #[cfg(target_os = "linux")]
    {
        stores.push(Box::new(super::linux::DockerSecret));
        if let Some(dir) = _data_dir {
            stores.push(Box::new(super::key_file::KeyFile::new(dir)));
        }
    }

    stores
}
