//! Explicit, file-backed clock override for deterministic end-to-end tests.
//!
//! Production always uses the system clock. The override is consulted only
//! when `WEAVER_E2E_MODE=1` (or `true`) and `WEAVER_E2E_CLOCK_FILE` points to
//! an RFC 3339 timestamp. The process-start configuration is cached, while the
//! file itself is read on every call so an isolated e2e flow can advance reset
//! boundaries without waiting for wall-clock time. The timestamp is an instant
//! projected into the process timezone; the release harness pins `TZ=UTC`.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use chrono::{DateTime, Local, Utc};

const E2E_MODE_ENV: &str = "WEAVER_E2E_MODE";
const E2E_CLOCK_FILE_ENV: &str = "WEAVER_E2E_CLOCK_FILE";
const E2E_SCHEDULE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const PRODUCTION_SCHEDULE_POLL_INTERVAL: Duration = Duration::from_secs(60);

enum ClockConfig {
    System,
    File(PathBuf),
}

static CLOCK_CONFIG: OnceLock<ClockConfig> = OnceLock::new();

pub fn local_now() -> DateTime<Local> {
    match clock_config() {
        ClockConfig::System => Local::now(),
        ClockConfig::File(path) => read_clock(path)
            .unwrap_or_else(|error| panic!("{error}"))
            .with_timezone(&Local),
    }
}

pub fn utc_now() -> DateTime<Utc> {
    local_now().with_timezone(&Utc)
}

pub fn validate() -> Result<(), String> {
    match clock_config() {
        ClockConfig::System => Ok(()),
        ClockConfig::File(path) => read_clock(path).map(|_| ()),
    }
}

pub fn schedule_poll_interval() -> Duration {
    match clock_config() {
        ClockConfig::System => PRODUCTION_SCHEDULE_POLL_INTERVAL,
        ClockConfig::File(_) => E2E_SCHEDULE_POLL_INTERVAL,
    }
}

fn clock_config() -> &'static ClockConfig {
    CLOCK_CONFIG.get_or_init(|| {
        configured_clock_path()
            .map(ClockConfig::File)
            .unwrap_or(ClockConfig::System)
    })
}

fn configured_clock_path() -> Option<PathBuf> {
    let enabled = std::env::var(E2E_MODE_ENV)
        .ok()
        .is_some_and(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true"));
    if !enabled {
        return None;
    }
    std::env::var_os(E2E_CLOCK_FILE_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn read_clock(path: &Path) -> Result<DateTime<chrono::FixedOffset>, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|error| format!("read Weaver e2e clock {}: {error}", path.display()))?;
    parse_rfc3339(raw.trim())
        .map_err(|error| format!("parse Weaver e2e clock {}: {error}", path.display()))
}

fn parse_rfc3339(value: &str) -> Result<DateTime<chrono::FixedOffset>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value)
}

#[cfg(test)]
mod tests {
    use super::parse_rfc3339;

    #[test]
    fn parses_clock_contract_as_rfc3339() {
        let parsed = parse_rfc3339("2032-03-14T01:59:59-07:00").expect("valid clock");
        assert_eq!(parsed.timestamp(), 1_962_867_599);
    }

    #[test]
    fn rejects_ambiguous_clock_values() {
        assert!(parse_rfc3339("2032-03-14 01:59:59").is_err());
    }
}
