//! The temporary Windows upgrade helper.
//!
//! The helper logic lives in the shared `application-updater` crate; this
//! binding supplies Weaver's product identity and its per-user startup
//! registration, which an MSI major upgrade can drop and the helper restores.

use std::path::Path;

use application_updater::helper::TrayStartup;
use weaver_server_core::application_upgrade::WEAVER_PRODUCT;

/// Weaver's per-user "start at login" registration.
struct WeaverTrayStartup;

impl TrayStartup for WeaverTrayStartup {
    fn startup_enabled(&self) -> Result<bool, String> {
        #[cfg(windows)]
        {
            crate::windows_startup::startup_enabled()
        }
        #[cfg(not(windows))]
        {
            Ok(false)
        }
    }

    fn register_startup(&self, tray_path: &Path) -> Result<(), String> {
        #[cfg(windows)]
        {
            crate::windows_startup::register_startup(tray_path)
        }
        #[cfg(not(windows))]
        {
            let _ = tray_path;
            Ok(())
        }
    }
}

/// Runs the helper and reports whether this process was one. Called before
/// anything else in `main`: a helper process must never start a server.
pub fn maybe_run_upgrade_helper() -> Result<bool, String> {
    application_updater::helper::maybe_run_upgrade_helper(&WEAVER_PRODUCT, &WeaverTrayStartup)
}
