use std::path::PathBuf;
use std::sync::{
    Once,
    atomic::{AtomicBool, Ordering},
};

const DISABLE_PLATFORM_KEYSTORE_ENV: &str = "WEAVER_DISABLE_PLATFORM_KEYSTORE";
static DISABLE_PLATFORM_KEYSTORE_FOR_PROCESS: AtomicBool = AtomicBool::new(false);

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

/// Make platform keystores unreachable for the rest of this process and any
/// children it spawns. Test helpers may call this before constructing
/// services; the automatic harness detection in [`platform_keystore_disabled`]
/// covers the tests that don't.
#[doc(hidden)]
#[allow(dead_code)] // for test helpers that spawn children; harness auto-detection covers today's tests
pub fn disable_platform_keystore_for_tests() {
    DISABLE_PLATFORM_KEYSTORE_FOR_PROCESS.store(true, Ordering::SeqCst);

    static SET_DISABLE_ENV: Once = Once::new();
    SET_DISABLE_ENV.call_once(|| {
        // Setting the env flag makes the platform-keystore block inherited by
        // any child process spawned from the test.
        unsafe { std::env::set_var(DISABLE_PLATFORM_KEYSTORE_ENV, "1") };
    });
}

fn platform_keystore_disabled() -> bool {
    if DISABLE_PLATFORM_KEYSTORE_FOR_PROCESS.load(Ordering::SeqCst) {
        return true;
    }

    if cfg!(test) {
        return true;
    }

    if running_under_rust_test_harness() {
        return true;
    }

    platform_keystore_disabled_by_env()
}

fn platform_keystore_disabled_by_env() -> bool {
    std::env::var(DISABLE_PLATFORM_KEYSTORE_ENV)
        .ok()
        .is_some_and(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}

/// Detect the cargo test harness from inside a non-`cfg(test)` build.
///
/// Integration tests compile this crate as a dependency, so `cfg!(test)` is
/// false there — and those are exactly the binaries that used to reach the
/// real macOS Keychain and pop access prompts. Cargo exposes
/// `CARGO_TARGET_TMPDIR` to integration tests and benches (inherited by child
/// processes), and test binaries live in `deps/` with a trailing metadata
/// hash. Treat that process tree as non-interactive for keystore use.
fn running_under_rust_test_harness() -> bool {
    if std::env::var_os("CARGO_TARGET_TMPDIR").is_some() {
        return true;
    }

    let Ok(exe) = std::env::current_exe() else {
        return false;
    };
    let Some(parent) = exe.parent() else {
        return false;
    };
    if parent.file_name().and_then(|name| name.to_str()) != Some("deps") {
        return false;
    }

    exe.file_stem()
        .and_then(|stem| stem.to_str())
        .is_some_and(has_rust_test_binary_hash_suffix)
}

fn has_rust_test_binary_hash_suffix(stem: &str) -> bool {
    stem.rsplit_once('-').is_some_and(|(_, suffix)| {
        suffix.len() >= 8 && suffix.chars().all(|ch| ch.is_ascii_hexdigit())
    })
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
    // Only the INTERACTIVE stores are unreachable from test processes: the
    // macOS Keychain (whose access dialogs are what this gate exists for) and
    // the Windows Credential Manager (which would pollute a real store). The
    // file-backed and Docker-secret backends stay available — tests exercise
    // their semantics against temp data dirs and they can never prompt. The
    // manual keychain tests construct `MacOSKeychain` directly and are gated
    // by `WEAVER_MANUAL_KEYCHAIN_TEST`, so they are unaffected by this.
    #[cfg(any(target_os = "macos", target_os = "windows"))]
    let interactive_disabled = platform_keystore_disabled();

    let mut stores: Vec<Box<dyn KeyStore>> = Vec::new();

    // Skip macOS Keychain in debug builds — each recompile changes the binary
    // hash, causing repeated password prompts. Use file-based key instead.
    #[cfg(target_os = "macos")]
    if interactive_disabled || force_key_file() || cfg!(debug_assertions) {
        if let Some(dir) = _data_dir.clone() {
            stores.push(Box::new(super::key_file::KeyFile::new(dir)));
        }
    } else {
        stores.push(Box::new(super::macos::MacOSKeychain::new()));
    }

    #[cfg(target_os = "windows")]
    if !interactive_disabled {
        stores.push(Box::new(
            super::windows::WindowsCredentialManager::for_data_dir(_data_dir.as_deref()),
        ));
    }

    #[cfg(target_os = "linux")]
    {
        stores.push(Box::new(super::linux::DockerSecret));
        if let Some(dir) = _data_dir {
            stores.push(Box::new(super::key_file::KeyFile::new(dir)));
        }
    }

    stores
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn platform_keystore_flag_defaults_to_enabled_when_env_is_absent() {
        let _guard = env_lock().lock().expect("lock env guard");
        let original = std::env::var(DISABLE_PLATFORM_KEYSTORE_ENV).ok();
        unsafe { std::env::remove_var(DISABLE_PLATFORM_KEYSTORE_ENV) };

        assert!(!platform_keystore_disabled_by_env());

        match original {
            Some(value) => unsafe { std::env::set_var(DISABLE_PLATFORM_KEYSTORE_ENV, value) },
            None => unsafe { std::env::remove_var(DISABLE_PLATFORM_KEYSTORE_ENV) },
        }
    }

    #[test]
    fn platform_keystore_flag_disables_interactive_stores() {
        let _guard = env_lock().lock().expect("lock env guard");
        let original = std::env::var(DISABLE_PLATFORM_KEYSTORE_ENV).ok();
        unsafe { std::env::set_var(DISABLE_PLATFORM_KEYSTORE_ENV, "1") };

        assert!(platform_keystore_disabled_by_env());
        assert!(
            platform_keystores(None)
                .iter()
                .all(|store| store.name() != "macOS Keychain")
        );

        match original {
            Some(value) => unsafe { std::env::set_var(DISABLE_PLATFORM_KEYSTORE_ENV, value) },
            None => unsafe { std::env::remove_var(DISABLE_PLATFORM_KEYSTORE_ENV) },
        }
    }

    #[test]
    fn test_helper_sets_inheritable_disable_flag() {
        let _guard = env_lock().lock().expect("lock env guard");
        let original = std::env::var(DISABLE_PLATFORM_KEYSTORE_ENV).ok();
        unsafe { std::env::remove_var(DISABLE_PLATFORM_KEYSTORE_ENV) };

        disable_platform_keystore_for_tests();

        assert_eq!(
            std::env::var(DISABLE_PLATFORM_KEYSTORE_ENV).as_deref(),
            Ok("1")
        );

        match original {
            Some(value) => unsafe { std::env::set_var(DISABLE_PLATFORM_KEYSTORE_ENV, value) },
            None => unsafe { std::env::remove_var(DISABLE_PLATFORM_KEYSTORE_ENV) },
        }
    }

    #[test]
    fn platform_keystore_is_disabled_in_test_binaries() {
        assert!(platform_keystore_disabled());
        // The interactive stores must be unreachable from any test process;
        // the file-backed stores stay (tests exercise them via temp dirs).
        assert!(
            platform_keystores(None)
                .iter()
                .all(|store| store.name() != "macOS Keychain")
        );
    }

    #[test]
    fn detects_rust_test_binary_hash_suffix() {
        assert!(has_rust_test_binary_hash_suffix(
            "integration_pipeline-a1b2c3d4e5f6"
        ));
        assert!(!has_rust_test_binary_hash_suffix("weaver-server"));
        assert!(!has_rust_test_binary_hash_suffix("not-a-test"));
    }
}
