// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Explicit, file-backed clock override for deterministic end-to-end tests.
//
// Production always uses the system clock. The override is consulted only
// when `WEAVER_E2E_MODE=1` (or `true`) and `WEAVER_E2E_CLOCK_FILE` points to
// an RFC 3339 timestamp. The process-start configuration is cached, while the
// file itself is read on every call so an isolated e2e flow can advance reset
// boundaries without waiting for wall-clock time. The timestamp is an instant
// projected into the process timezone; the release harness pins `TZ=UTC`.

//
// The torrent service takes a second, separate override: with
// `WEAVER_E2E_MODE` on, `WEAVER_E2E_TORRENT_CLOCK_OFFSET_FILE` names a file
// holding whole seconds added to its wall clock ([`TorrentOffsetClock`]).
// Its clock keeps running, so rates and seeding time accrue as they do in
// production, and a flow fast-forwards seeding time by raising the offset.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use chrono::{DateTime, Local, Utc};

use crate::clock::Clock;

const E2E_MODE_ENV: &str = "WEAVER_E2E_MODE";
const E2E_CLOCK_FILE_ENV: &str = "WEAVER_E2E_CLOCK_FILE";
const E2E_TORRENT_CLOCK_OFFSET_FILE_ENV: &str = "WEAVER_E2E_TORRENT_CLOCK_OFFSET_FILE";
const E2E_SCHEDULE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const PRODUCTION_SCHEDULE_POLL_INTERVAL: Duration = Duration::from_secs(60);

enum ClockConfig {
    System,
    File(PathBuf),
}

static CLOCK_CONFIG: OnceLock<ClockConfig> = OnceLock::new();
static TORRENT_CLOCK_OFFSET_PATH: OnceLock<Option<PathBuf>> = OnceLock::new();

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

// Seconds since the Unix epoch on the same clock as [`local_now`], without
// resolving the local timezone. Per-article accounting only buckets and
// compares instants; it never needs a calendar.
pub fn unix_seconds() -> i64 {
    match clock_config() {
        ClockConfig::System => Utc::now().timestamp(),
        ClockConfig::File(path) => read_clock(path)
            .unwrap_or_else(|error| panic!("{error}"))
            .timestamp(),
    }
}

pub fn validate() -> Result<(), String> {
    match clock_config() {
        ClockConfig::System => Ok(()),
        ClockConfig::File(path) => read_clock(path).map(|_| ()),
    }?;
    match torrent_clock_offset_path() {
        None => Ok(()),
        Some(path) => read_offset_seconds(path).map(|_| ()),
    }
}

/// The torrent service's clock under an e2e offset, or `None` when the
/// override is off and the service keeps the system clock.
pub fn torrent_clock() -> Option<TorrentOffsetClock> {
    torrent_clock_offset_path().map(|path| TorrentOffsetClock::new(path.to_path_buf()))
}

/// The system clock with the whole seconds held in a file added to its
/// wall-clock reading. The file is read at every reading, so a flow moves
/// the clock on by rewriting it. A missing or empty file adds nothing. The
/// monotonic reading is the system's own, so it never steps back.
pub struct TorrentOffsetClock {
    path: PathBuf,
}

impl TorrentOffsetClock {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Clock for TorrentOffsetClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn unix_ms(&self) -> i64 {
        let seconds = read_offset_seconds(&self.path).unwrap_or_else(|error| panic!("{error}"));
        // `read_offset_seconds` bounds the offset to fit in milliseconds.
        let offset_ms = i64::try_from(seconds)
            .unwrap_or(i64::MAX)
            .saturating_mul(1000);
        Utc::now().timestamp_millis().saturating_add(offset_ms)
    }
}

fn torrent_clock_offset_path() -> Option<&'static Path> {
    TORRENT_CLOCK_OFFSET_PATH
        .get_or_init(|| {
            torrent_offset_path_for(
                e2e_mode_enabled(),
                std::env::var_os(E2E_TORRENT_CLOCK_OFFSET_FILE_ENV),
            )
        })
        .as_deref()
}

// The offset file the torrent clock reads: the configured path, honoured
// only in e2e mode.
fn torrent_offset_path_for(e2e_mode: bool, configured: Option<OsString>) -> Option<PathBuf> {
    if !e2e_mode {
        return None;
    }
    configured
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn read_offset_seconds(path: &Path) -> Result<u64, String> {
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => {
            return Err(format!(
                "read Weaver e2e torrent clock offset {}: {error}",
                path.display()
            ));
        }
    };
    parse_offset_seconds(&raw).map_err(|error| {
        format!(
            "parse Weaver e2e torrent clock offset {}: {error}",
            path.display()
        )
    })
}

// Whole seconds as ASCII digits, optionally followed by whitespace; nothing
// at all is zero.
fn parse_offset_seconds(raw: &str) -> Result<u64, String> {
    let digits = raw.trim_end();
    if digits.is_empty() {
        return Ok(0);
    }
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("expected whole seconds, found {digits:?}"));
    }
    digits
        .parse::<u64>()
        .ok()
        .filter(|seconds| i64::try_from(*seconds).is_ok_and(|s| s.checked_mul(1000).is_some()))
        .ok_or_else(|| format!("offset {digits} is out of range"))
}

pub fn override_enabled() -> bool {
    matches!(clock_config(), ClockConfig::File(_))
}

// An explicit E2E control holds between articles, where no socket read is
// outstanding. This lets the driver advance the deadline without racing an
// unrelated network timeout. Production never reads the control file.
pub fn hold_speed_test_after_article() -> bool {
    match clock_config() {
        ClockConfig::System => false,
        ClockConfig::File(path) => path.with_extension("speed-test-hold").exists(),
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

// Whether `WEAVER_E2E_MODE` is `1` or `true`.
pub fn e2e_mode_enabled() -> bool {
    std::env::var(E2E_MODE_ENV)
        .ok()
        .is_some_and(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true"))
}

fn configured_clock_path() -> Option<PathBuf> {
    if !e2e_mode_enabled() {
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

    // The torrent clock's offset file is honoured only in e2e mode.
    #[test]
    fn the_torrent_offset_file_needs_e2e_mode() {
        use super::torrent_offset_path_for;
        use std::path::PathBuf;

        let path = || Some("/offset".into());
        assert_eq!(torrent_offset_path_for(false, path()), None);
        assert_eq!(torrent_offset_path_for(false, None), None);
        assert_eq!(torrent_offset_path_for(true, None), None);
        assert_eq!(torrent_offset_path_for(true, Some("".into())), None);
        assert_eq!(
            torrent_offset_path_for(true, path()),
            Some(PathBuf::from("/offset"))
        );
    }

    #[test]
    fn the_torrent_offset_is_whole_seconds() {
        use super::parse_offset_seconds;

        assert_eq!(parse_offset_seconds(""), Ok(0));
        assert_eq!(parse_offset_seconds("0"), Ok(0));
        assert_eq!(parse_offset_seconds("3600"), Ok(3600));
        assert_eq!(parse_offset_seconds("3600\n"), Ok(3600));
        assert_eq!(parse_offset_seconds("3600 \r\n"), Ok(3600));
        for bad in [
            " 1",
            "-1",
            "+1",
            "1.5",
            "1e3",
            "abc",
            "1 2",
            "99999999999999999999",
        ] {
            assert!(parse_offset_seconds(bad).is_err(), "{bad:?}");
        }
    }

    // The clock adds the file's seconds to the system's wall clock, reading
    // the file again at every reading, and adds nothing while it is missing
    // or empty.
    #[test]
    fn the_torrent_clock_adds_the_files_offset_at_each_reading() {
        use super::TorrentOffsetClock;
        use crate::clock::Clock;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("offset");
        let clock = TorrentOffsetClock::new(path.clone());
        let within = |offset_s: i64| {
            let before = chrono::Utc::now().timestamp_millis();
            let read = clock.unix_ms();
            let after = chrono::Utc::now().timestamp_millis();
            let offset_ms = offset_s * 1000;
            assert!(
                before + offset_ms <= read && read <= after + offset_ms,
                "{before} <= {read} - {offset_ms} <= {after}"
            );
        };

        within(0);
        std::fs::write(&path, "").unwrap();
        within(0);
        std::fs::write(&path, "86400\n").unwrap();
        within(86_400);
        std::fs::write(&path, "90000").unwrap();
        within(90_000);
        std::fs::remove_file(&path).unwrap();
        within(0);
    }

    #[test]
    #[should_panic(expected = "parse Weaver e2e torrent clock offset")]
    fn a_malformed_torrent_offset_fails_the_reading() {
        use super::TorrentOffsetClock;
        use crate::clock::Clock;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("offset");
        std::fs::write(&path, "soon").unwrap();
        TorrentOffsetClock::new(path).unix_ms();
    }
}
