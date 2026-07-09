mod intake;
mod scanner;
mod service;

use serde::{Deserialize, Serialize};

pub use scanner::{
    MarkerRename, ScanIssue, WatchFolderScanReport, WatchFolderScanner, WatchFolderScannerError,
};
pub use service::{WatchFolderService, WatchFolderServiceError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WatchFolderMode {
    #[default]
    Off,
    Polling,
    Realtime,
}

impl WatchFolderMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Polling => "polling",
            Self::Realtime => "realtime",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "off" => Some(Self::Off),
            "polling" => Some(Self::Polling),
            "realtime" => Some(Self::Realtime),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchFolderConfig {
    #[serde(default)]
    pub mode: WatchFolderMode,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,
    #[serde(default = "default_stability_secs")]
    pub stability_secs: u64,
    #[serde(default = "default_category_from_subfolders")]
    pub category_from_subfolders: bool,
    #[serde(default)]
    pub scanning_paused: bool,
}

impl Default for WatchFolderConfig {
    fn default() -> Self {
        Self {
            mode: WatchFolderMode::Off,
            path: None,
            poll_interval_secs: default_poll_interval_secs(),
            stability_secs: default_stability_secs(),
            category_from_subfolders: default_category_from_subfolders(),
            scanning_paused: false,
        }
    }
}

impl WatchFolderConfig {
    pub fn normalized_path(&self) -> Option<String> {
        self.path
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.poll_interval_secs == 0 {
            return Err("watch_folder.poll_interval_secs must be greater than 0".to_string());
        }
        if !matches!(self.mode, WatchFolderMode::Off) && self.normalized_path().is_none() {
            return Err(
                "watch_folder.path must be set when watch folder mode is not off".to_string(),
            );
        }
        Ok(())
    }

    pub fn automatic_scanning_enabled(&self) -> bool {
        !matches!(self.mode, WatchFolderMode::Off)
            && !self.scanning_paused
            && self.normalized_path().is_some()
    }
}

fn default_poll_interval_secs() -> u64 {
    60
}

fn default_stability_secs() -> u64 {
    3
}

fn default_category_from_subfolders() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_off_with_safe_values() {
        let cfg = WatchFolderConfig::default();

        assert_eq!(cfg.mode, WatchFolderMode::Off);
        assert_eq!(cfg.path, None);
        assert_eq!(cfg.poll_interval_secs, 60);
        assert_eq!(cfg.stability_secs, 3);
        assert!(cfg.category_from_subfolders);
        assert!(!cfg.scanning_paused);
        assert!(!cfg.automatic_scanning_enabled());
    }

    #[test]
    fn validates_path_for_active_modes() {
        let mut cfg = WatchFolderConfig {
            mode: WatchFolderMode::Polling,
            ..WatchFolderConfig::default()
        };
        assert!(cfg.validate().is_err());

        cfg.path = Some("/incoming".to_string());
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn parses_mode_strings() {
        assert_eq!(WatchFolderMode::parse("off"), Some(WatchFolderMode::Off));
        assert_eq!(
            WatchFolderMode::parse("polling"),
            Some(WatchFolderMode::Polling)
        );
        assert_eq!(
            WatchFolderMode::parse("realtime"),
            Some(WatchFolderMode::Realtime)
        );
        assert_eq!(WatchFolderMode::parse("manual"), None);
    }
}
