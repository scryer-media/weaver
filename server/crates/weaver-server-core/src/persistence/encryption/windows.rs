use super::keystore::KeyStore;
use keyring::{Entry, Error};
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, WAIT_ABANDONED_0, WAIT_OBJECT_0};
use windows_sys::Win32::System::Threading::{
    CreateMutexW, INFINITE, ReleaseMutex, WaitForSingleObject,
};

const SERVICE: &str = "weaver";
const ACCOUNT: &str = "encryption-master-key";
const CREATE_MUTEX_NAME: &str = "Local\\ScryerMedia.Weaver.EncryptionKey";

struct CredentialCreateGuard(HANDLE);

impl CredentialCreateGuard {
    fn acquire() -> Result<Self, String> {
        let name: Vec<u16> = CREATE_MUTEX_NAME.encode_utf16().chain(Some(0)).collect();
        // SAFETY: The security attributes pointer is null, and `name` is a live,
        // nul-terminated UTF-16 buffer for the duration of the call.
        let handle = unsafe { CreateMutexW(std::ptr::null(), 0, name.as_ptr()) };
        if handle.is_null() {
            return Err(format!(
                "failed to create Windows encryption-key mutex: {}",
                std::io::Error::last_os_error()
            ));
        }

        // SAFETY: `handle` was returned by CreateMutexW and remains owned here.
        let wait_result = unsafe { WaitForSingleObject(handle, INFINITE) };
        if wait_result != WAIT_OBJECT_0 && wait_result != WAIT_ABANDONED_0 {
            // SAFETY: The handle is valid and has not been closed yet.
            unsafe { CloseHandle(handle) };
            return Err(format!(
                "failed to acquire Windows encryption-key mutex: wait result {wait_result}"
            ));
        }

        Ok(Self(handle))
    }
}

impl Drop for CredentialCreateGuard {
    fn drop(&mut self) {
        // SAFETY: This guard owns the acquired mutex handle until this drop.
        unsafe {
            ReleaseMutex(self.0);
            CloseHandle(self.0);
        }
    }
}

pub struct WindowsCredentialManager {
    service: String,
    account: String,
}

impl WindowsCredentialManager {
    pub fn new() -> Self {
        Self {
            service: SERVICE.to_string(),
            account: ACCOUNT.to_string(),
        }
    }

    #[cfg(test)]
    fn with_identifiers(service: String, account: String) -> Self {
        Self { service, account }
    }

    fn entry(&self) -> Result<Entry, String> {
        Entry::new(&self.service, &self.account)
            .map_err(|e| format!("failed to create credential entry: {e}"))
    }
}

impl KeyStore for WindowsCredentialManager {
    fn get_key(&self) -> Result<Option<String>, String> {
        let entry = self.entry()?;
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

    fn create_key_if_absent(&self, key: &str) -> Result<Option<String>, String> {
        let _guard = CredentialCreateGuard::acquire()?;
        let entry = self.entry()?;
        match entry.get_password() {
            Ok(existing) => {
                let existing = existing.trim().to_string();
                if existing.is_empty() {
                    Err("Windows Credential Manager encryption-key entry is empty".into())
                } else {
                    Ok(Some(existing))
                }
            }
            Err(Error::NoEntry) => {
                entry.set_password(key).map_err(|e| {
                    format!("failed to store key in Windows Credential Manager: {e}")
                })?;
                self.get_key()?.map(Some).ok_or_else(|| {
                    "Windows Credential Manager did not retain the encryption key".to_string()
                })
            }
            Err(e) => Err(format!("Windows Credential Manager error: {e}")),
        }
    }

    fn replace_key(&self, key: &str) -> Result<bool, String> {
        let _guard = CredentialCreateGuard::acquire()?;
        self.entry()?
            .set_password(key)
            .map(|()| true)
            .map_err(|error| {
                format!("failed to replace key in Windows Credential Manager: {error}")
            })
    }

    fn can_replace(&self) -> bool {
        true
    }

    fn delete_key(&self) -> Result<(), String> {
        let entry = self.entry()?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::encryption::EncryptionKey;

    struct CredentialCleanup<'a>(&'a WindowsCredentialManager);

    impl Drop for CredentialCleanup<'_> {
        fn drop(&mut self) {
            let _ = self.0.delete_key();
        }
    }

    #[test]
    fn generated_key_is_created_once_and_reused() {
        let account = format!(
            "encryption-master-key-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let store = WindowsCredentialManager::with_identifiers("weaver-test".into(), account);
        let _cleanup = CredentialCleanup(&store);
        let first = EncryptionKey::generate().to_base64();
        let second = EncryptionKey::generate().to_base64();
        let restored = EncryptionKey::generate().to_base64();

        assert_eq!(
            store.create_key_if_absent(&first).unwrap().as_deref(),
            Some(first.as_str())
        );
        assert_eq!(
            store.create_key_if_absent(&second).unwrap().as_deref(),
            Some(first.as_str())
        );
        assert_eq!(store.get_key().unwrap().as_deref(), Some(first.as_str()));
        assert!(store.replace_key(&restored).unwrap());
        assert_eq!(store.get_key().unwrap().as_deref(), Some(restored.as_str()));
    }

    #[test]
    fn empty_existing_credential_is_not_overwritten() {
        let account = format!(
            "encryption-master-key-empty-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let store = WindowsCredentialManager::with_identifiers("weaver-test".into(), account);
        let _cleanup = CredentialCleanup(&store);
        store.entry().unwrap().set_password("   ").unwrap();

        let replacement = EncryptionKey::generate().to_base64();
        let error = store.create_key_if_absent(&replacement).unwrap_err();

        assert!(error.contains("is empty"));
        assert_eq!(store.entry().unwrap().get_password().unwrap(), "   ");
    }
}
