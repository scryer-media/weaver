use super::keystore::KeyStore;

const SERVICE: &str = "media.weaver.app";
const ACCOUNT: &str = "encryption-master-key";

pub struct MacOSKeychain;

impl KeyStore for MacOSKeychain {
    fn get_key(&self) -> Result<Option<String>, String> {
        match security_framework::passwords::get_generic_password(SERVICE, ACCOUNT) {
            Ok(bytes) => {
                let key = String::from_utf8(bytes)
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

    fn set_key(&self, key_base64: &str) -> Result<(), String> {
        security_framework::passwords::set_generic_password(SERVICE, ACCOUNT, key_base64.as_bytes())
            .map_err(|e| format!("failed to store key in macOS Keychain: {e}"))
    }

    fn delete_key(&self) -> Result<(), String> {
        match security_framework::passwords::delete_generic_password(SERVICE, ACCOUNT) {
            Ok(()) => Ok(()),
            Err(e) if e.code() == security_framework_sys::base::errSecItemNotFound => Ok(()),
            Err(e) => Err(format!("failed to delete key from macOS Keychain: {e}")),
        }
    }

    fn name(&self) -> &'static str {
        "macOS Keychain"
    }
}
