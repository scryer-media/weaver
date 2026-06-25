use super::keystore::KeyStore;
use keyring_core::{Entry, Error};

const SERVICE: &str = "weaver";
const ACCOUNT: &str = "encryption-master-key";

pub struct WindowsCredentialManager;

impl WindowsCredentialManager {
    fn entry() -> Result<Entry, String> {
        Entry::new(SERVICE, ACCOUNT).map_err(|e| format!("failed to create credential entry: {e}"))
    }
}

impl KeyStore for WindowsCredentialManager {
    fn get_key(&self) -> Result<Option<String>, String> {
        let entry = Self::entry()?;
        match entry.get_password() {
            Ok(password) => {
                let trimmed = password.trim().to_string();
                if trimmed.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(trimmed))
                }
            }
            Err(Error::NoEntry) => Ok(None),
            Err(e) => Err(format!("Windows Credential Manager error: {e}")),
        }
    }

    fn delete_key(&self) -> Result<(), String> {
        let entry = Self::entry()?;
        match entry.delete_credential() {
            Ok(()) => Ok(()),
            Err(Error::NoEntry) => Ok(()),
            Err(e) => Err(format!(
                "failed to delete key from Windows Credential Manager: {e}"
            )),
        }
    }

    fn name(&self) -> &'static str {
        "Windows Credential Manager"
    }
}
