use super::keystore::KeyStore;

const SERVICE: &str = if cfg!(debug_assertions) {
    "media.weaver.app.dev"
} else {
    "media.weaver.app"
};
const ACCOUNT: &str = "encryption-master-key";

pub struct MacOSKeychain {
    service: String,
    account: String,
}

impl MacOSKeychain {
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

    fn keychain() -> Result<security_framework::os::macos::keychain::SecKeychain, String> {
        security_framework::os::macos::keychain::SecKeychain::default()
            .map_err(|e| format!("failed to open the default macOS Keychain: {e}"))
    }
}

impl KeyStore for MacOSKeychain {
    fn get_key(&self) -> Result<Option<String>, String> {
        match Self::keychain()?.find_generic_password(&self.service, &self.account) {
            Ok((password, _item)) => {
                let key = String::from_utf8(password.as_ref().to_vec())
                    .map_err(|e| format!("keychain entry is not valid UTF-8: {e}"))?;
                let trimmed = key.trim().to_string();
                if trimmed.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(trimmed))
                }
            }
            Err(e) if e.code() == security_framework_sys::base::errSecItemNotFound => Ok(None),
            Err(e) => Err(format!("macOS Keychain error: {e}")),
        }
    }

    fn create_key_if_absent(&self, key: &str) -> Result<Option<String>, String> {
        if let Some(existing) = self.get_key()? {
            return Ok(Some(existing));
        }

        let keychain = Self::keychain()?;
        match keychain.add_generic_password(&self.service, &self.account, key.as_bytes()) {
            Ok(()) => Ok(Some(key.to_string())),
            Err(e) if e.code() == security_framework_sys::base::errSecDuplicateItem => self
                .get_key()?
                .map(Some)
                .ok_or_else(|| "macOS Keychain entry appeared concurrently but is empty".into()),
            Err(e) => Err(format!("failed to store key in the macOS Keychain: {e}")),
        }
    }

    fn delete_key(&self) -> Result<(), String> {
        match Self::keychain()?.find_generic_password(&self.service, &self.account) {
            Ok((_password, item)) => {
                item.delete();
                Ok(())
            }
            Err(e) if e.code() == security_framework_sys::base::errSecItemNotFound => Ok(()),
            Err(e) => Err(format!("failed to delete key from macOS Keychain: {e}")),
        }
    }

    fn name(&self) -> &'static str {
        "macOS Keychain"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::encryption::EncryptionKey;

    struct KeychainCleanup<'a>(&'a MacOSKeychain);

    impl Drop for KeychainCleanup<'_> {
        fn drop(&mut self) {
            let _ = self.0.delete_key();
        }
    }

    #[test]
    fn generated_key_is_created_once_and_reused() {
        let account = format!("key-test-{}", std::process::id());
        let store = MacOSKeychain::with_identifiers("weaver-test".into(), account);
        let _cleanup = KeychainCleanup(&store);
        let first = EncryptionKey::generate().to_base64();
        let second = EncryptionKey::generate().to_base64();

        assert_eq!(
            store.create_key_if_absent(&first).unwrap().as_deref(),
            Some(first.as_str())
        );
        assert_eq!(
            store.create_key_if_absent(&second).unwrap().as_deref(),
            Some(first.as_str())
        );
        assert_eq!(store.get_key().unwrap().as_deref(), Some(first.as_str()));
    }
}
