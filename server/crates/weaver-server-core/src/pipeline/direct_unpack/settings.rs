//! Whether direct unpack is switched on, resolved once.
//!
//! Precedence is **environment, then config, then default** — the same rule and
//! the same vocabulary as direct-store, so an operator who has learned one knob
//! has learned both. The default is **off**: the feature stays dark until a
//! release decision turns it on, not a config-default change.

use std::sync::OnceLock;

use super::super::direct_store::parse_enabled;

/// Env override for direct unpack.
///
/// Config is the durable operator surface; this exists for incident response,
/// when turning the feature off has to be possible without editing config and
/// waiting for a reload.
pub const DIRECT_UNPACK_ENV: &str = "WEAVER_DIRECT_UNPACK";

/// Whether the env override forces direct unpack on or off, if it says anything
/// at all. Read once, like the direct-store gate.
pub fn env_override() -> Option<bool> {
    static OVERRIDE: OnceLock<Option<bool>> = OnceLock::new();
    *OVERRIDE.get_or_init(|| parse_enabled(std::env::var(DIRECT_UNPACK_ENV).ok().as_deref()))
}

/// Resolved gate value, passed explicitly so callers and tests do not race the
/// process-wide `OnceLock`.
///
/// **Defaults off.**
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DirectUnpackGate {
    Enabled,
    #[default]
    Disabled,
}

impl DirectUnpackGate {
    /// Whether a set may be admitted to the chase.
    pub fn is_enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

/// Everything direct unpack reads out of configuration, resolved once at
/// pipeline construction.
///
/// Resolved up front rather than at each read point for the same reason
/// direct-store does it: a set admitted under an enabled gate must not find the
/// gate disabled partway through, with a decoder already chasing its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DirectUnpackSettings {
    pub gate: DirectUnpackGate,
}

impl DirectUnpackSettings {
    /// Resolves against a loaded config, with the environment winning.
    pub fn resolve(config: &crate::settings::Config) -> Self {
        Self::resolve_parts(
            config.direct_unpack.as_ref().and_then(|cfg| cfg.enabled),
            env_override(),
        )
    }

    /// The precedence rule itself, with the environment passed in so it is
    /// testable without mutating process state.
    pub fn resolve_parts(config_enabled: Option<bool>, env_enabled: Option<bool>) -> Self {
        let enabled = env_enabled.or(config_enabled).unwrap_or(false);
        Self {
            gate: if enabled {
                DirectUnpackGate::Enabled
            } else {
                DirectUnpackGate::Disabled
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::{Config, DirectUnpackOverrides};

    fn gate(config_enabled: Option<bool>, env_enabled: Option<bool>) -> DirectUnpackGate {
        DirectUnpackSettings::resolve_parts(config_enabled, env_enabled).gate
    }

    #[test]
    fn the_default_is_off() {
        assert_eq!(gate(None, None), DirectUnpackGate::Disabled);
        assert!(!DirectUnpackSettings::default().gate.is_enabled());
    }

    #[test]
    fn config_decides_when_the_environment_is_silent() {
        assert_eq!(gate(Some(true), None), DirectUnpackGate::Enabled);
        assert_eq!(gate(Some(false), None), DirectUnpackGate::Disabled);
    }

    #[test]
    fn the_environment_overrides_config_in_both_directions() {
        assert_eq!(gate(Some(false), Some(true)), DirectUnpackGate::Enabled);
        assert_eq!(gate(Some(true), Some(false)), DirectUnpackGate::Disabled);
    }

    #[test]
    fn the_environment_decides_when_config_is_absent() {
        assert_eq!(gate(None, Some(true)), DirectUnpackGate::Enabled);
        assert_eq!(gate(None, Some(false)), DirectUnpackGate::Disabled);
    }

    #[test]
    fn an_unrecognised_override_defers_to_config() {
        // `parse_enabled` yields `None` for a typo, which must not silently
        // disable a feature the operator turned on in config.
        assert_eq!(parse_enabled(Some("mabye")), None);
        assert_eq!(
            gate(Some(true), parse_enabled(Some("mabye"))),
            DirectUnpackGate::Enabled
        );
    }

    #[test]
    fn resolve_reads_the_config_table() {
        let mut config = Config {
            data_dir: "/tmp/weaver-direct-unpack".to_string(),
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            tuner: None,
            servers: Vec::new(),
            categories: Vec::new(),
            retry: None,
            max_download_speed: None,
            cleanup_after_extract: None,
            isp_bandwidth_cap: None,
            ip_replacement_trial_extra_connections: None,
            watch_folder: crate::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: crate::jobs::DuplicatePolicy::default(),
            direct_store: None,
            direct_unpack: None,
            delivery_naming: None,
            metrics: Default::default(),
            config_path: None,
        };
        // Skipped rather than asserted when the developer running the suite has
        // an override exported: `resolve` reads the real process environment,
        // and the precedence rule itself is covered above with the environment
        // injected.
        if env_override().is_some() {
            return;
        }

        // An absent table is "every default", and the default is OFF: the
        // feature ships dark until a release decision says otherwise.
        assert!(!DirectUnpackSettings::resolve(&config).gate.is_enabled());

        config.direct_unpack = Some(DirectUnpackOverrides {
            enabled: Some(true),
        });
        assert!(DirectUnpackSettings::resolve(&config).gate.is_enabled());

        config.direct_unpack = Some(DirectUnpackOverrides {
            enabled: Some(false),
        });
        assert!(!DirectUnpackSettings::resolve(&config).gate.is_enabled());
    }
}
