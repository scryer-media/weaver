//! Log format, colour, timestamp and default-level resolution for the service
//! subscriber.
//!
//! The resolution rules live here, apart from subscriber construction, so they
//! can be unit-tested without installing a global subscriber.

use std::ffi::OsString;
use std::fmt;

use chrono::{DateTime, Local, SecondsFormat, TimeZone};
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;

/// Renders a log timestamp as RFC 3339 in the zone of `now`.
///
/// Weaver emits log timestamps in the host's local zone so that operators who
/// set `TZ` (for example through Docker) see wall-clock times that match their
/// other services. The offset is always written explicitly (`Z` for UTC,
/// otherwise `+HH:MM`/`-HH:MM`) so the string stays machine-parseable.
fn render_timestamp<Tz>(now: DateTime<Tz>, precision: SecondsFormat) -> String
where
    Tz: TimeZone,
    Tz::Offset: fmt::Display,
{
    now.to_rfc3339_opts(precision, true)
}

/// Timer for `tracing_subscriber` layers that prints the host's local
/// wall-clock time (honouring `TZ`) instead of the UTC default.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct LocalTimer;

impl FormatTime for LocalTimer {
    fn format_time(&self, w: &mut Writer<'_>) -> fmt::Result {
        w.write_str(&render_timestamp(Local::now(), SecondsFormat::Micros))
    }
}

/// Environment variable selecting the stdout/log-file record format.
pub(crate) const LOG_FORMAT_ENV: &str = "WEAVER_LOG_FORMAT";
/// Environment variable selecting stdout colouring.
pub(crate) const LOG_COLOR_ENV: &str = "WEAVER_LOG_COLOR";

/// Record format for the stdout and log-file layers.
///
/// The in-memory ring buffer that backs the web log viewer is deliberately not
/// covered: the viewer parses the human-readable `tracing` line format, so that
/// layer keeps its format regardless of what stdout is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum LogFormat {
    #[default]
    Text,
    Json,
}

impl LogFormat {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "text" | "plain" | "pretty" => Some(Self::Text),
            "json" => Some(Self::Json),
            _ => None,
        }
    }

    /// Resolve the format from the CLI flag, then the environment, then the
    /// default. An unrecognised value falls back to text rather than aborting
    /// startup: a mistyped log format must never keep the service down.
    pub(crate) fn resolve(cli: Option<&str>, env: Option<&OsString>) -> Self {
        if let Some(value) = cli
            && let Some(format) = Self::parse(value)
        {
            return format;
        }
        env.and_then(|value| value.to_str())
            .and_then(Self::parse)
            .unwrap_or_default()
    }
}

/// Colour policy for the stdout layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum LogColor {
    #[default]
    Auto,
    Always,
    Never,
}

impl LogColor {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "always" | "yes" | "true" | "1" => Some(Self::Always),
            "never" | "no" | "false" | "0" => Some(Self::Never),
            _ => None,
        }
    }

    pub(crate) fn resolve(env: Option<&OsString>) -> Self {
        env.and_then(|value| value.to_str())
            .and_then(Self::parse)
            .unwrap_or_default()
    }

    /// Whether stdout should be coloured.
    ///
    /// `NO_COLOR` (any value, per the informal convention) forces colour off
    /// unless the operator asked for `always` explicitly. `auto` colours only
    /// when stdout is a terminal, so piping to a file or a log collector no
    /// longer embeds escape sequences — which is what it did unconditionally
    /// before.
    pub(crate) fn should_colour(self, stdout_is_terminal: bool, no_color_set: bool) -> bool {
        match self {
            Self::Always => true,
            Self::Never => false,
            Self::Auto => stdout_is_terminal && !no_color_set,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn os(value: &str) -> OsString {
        OsString::from(value)
    }

    #[test]
    fn log_format_defaults_to_text_and_accepts_json() {
        assert_eq!(LogFormat::resolve(None, None), LogFormat::Text);
        assert_eq!(LogFormat::resolve(None, Some(&os("json"))), LogFormat::Json);
        assert_eq!(
            LogFormat::resolve(None, Some(&os("  JSON  "))),
            LogFormat::Json
        );
        assert_eq!(LogFormat::resolve(None, Some(&os("text"))), LogFormat::Text);
    }

    #[test]
    fn log_format_cli_flag_wins_over_the_environment() {
        assert_eq!(
            LogFormat::resolve(Some("json"), Some(&os("text"))),
            LogFormat::Json
        );
        assert_eq!(
            LogFormat::resolve(Some("text"), Some(&os("json"))),
            LogFormat::Text
        );
    }

    #[test]
    fn unrecognised_log_format_falls_back_instead_of_failing() {
        assert_eq!(
            LogFormat::resolve(Some("yaml"), Some(&os("json"))),
            LogFormat::Json,
            "a bad CLI value defers to the environment"
        );
        assert_eq!(
            LogFormat::resolve(Some("yaml"), Some(&os("xml"))),
            LogFormat::Text
        );
        assert_eq!(LogFormat::parse("yaml"), None);
    }

    #[test]
    fn log_color_defaults_to_auto() {
        assert_eq!(LogColor::resolve(None), LogColor::Auto);
        assert_eq!(LogColor::resolve(Some(&os("nonsense"))), LogColor::Auto);
        assert_eq!(LogColor::resolve(Some(&os("NEVER"))), LogColor::Never);
        assert_eq!(LogColor::resolve(Some(&os("always"))), LogColor::Always);
    }

    #[test]
    fn auto_colour_follows_the_terminal_and_honours_no_color() {
        assert!(LogColor::Auto.should_colour(true, false));
        assert!(!LogColor::Auto.should_colour(false, false));
        assert!(
            !LogColor::Auto.should_colour(true, true),
            "NO_COLOR wins over an interactive terminal"
        );
    }

    #[test]
    fn explicit_colour_choices_override_detection() {
        assert!(LogColor::Always.should_colour(false, true));
        assert!(!LogColor::Never.should_colour(true, false));
    }

    #[test]
    fn render_timestamp_writes_explicit_offset_for_non_utc_zones() {
        let zone = chrono::FixedOffset::east_opt(2 * 3600).expect("offset");
        let now = zone
            .with_ymd_and_hms(2026, 9, 6, 15, 30, 11)
            .single()
            .expect("timestamp");

        assert_eq!(
            render_timestamp(now, SecondsFormat::Micros),
            "2026-09-06T15:30:11.000000+02:00"
        );
    }

    #[test]
    fn render_timestamp_keeps_z_suffix_for_utc() {
        let now = chrono::Utc
            .with_ymd_and_hms(2026, 9, 6, 13, 30, 11)
            .single()
            .expect("timestamp");

        assert_eq!(
            render_timestamp(now, SecondsFormat::Micros),
            "2026-09-06T13:30:11.000000Z"
        );
    }

    /// Locks the contract between the ring-buffer line format and the web log
    /// viewer's parser: the viewer splits a leading RFC 3339 timestamp off each
    /// line, so an emitted line must still start with one once the timer moved
    /// off UTC.
    #[test]
    fn emitted_lines_start_with_a_parseable_timestamp_then_the_level() {
        use std::io::Write;
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::fmt::MakeWriter;
        use tracing_subscriber::layer::SubscriberExt;

        #[derive(Clone, Default)]
        struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

        impl Write for SharedBuffer {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().expect("buffer lock").extend_from_slice(buf);
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        impl<'writer> MakeWriter<'writer> for SharedBuffer {
            type Writer = Self;

            fn make_writer(&'writer self) -> Self::Writer {
                self.clone()
            }
        }

        let buffer = SharedBuffer::default();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_timer(LocalTimer)
                .with_writer(buffer.clone())
                .with_ansi(false),
        );

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("weaver local timestamp probe");
        });

        let raw = String::from_utf8(buffer.0.lock().expect("buffer lock").clone()).expect("utf-8");
        let line = raw.lines().next().expect("one emitted line");
        let (timestamp, rest) = line.split_once(' ').expect("timestamp then level");

        DateTime::parse_from_rfc3339(timestamp)
            .unwrap_or_else(|error| panic!("line {line:?} lacks an rfc3339 timestamp: {error}"));
        assert!(
            rest.trim_start().starts_with("INFO"),
            "level should follow the timestamp, line was {line:?}"
        );
    }

    #[test]
    fn local_timer_writes_rfc3339_with_the_hosts_offset() {
        let mut buffer = String::new();
        let mut writer = Writer::new(&mut buffer);
        LocalTimer.format_time(&mut writer).expect("format time");

        let parsed = DateTime::parse_from_rfc3339(&buffer).expect("rfc3339 timestamp");
        let now = chrono::Utc::now();
        assert!(
            (now - parsed.with_timezone(&chrono::Utc))
                .num_seconds()
                .abs()
                < 60
        );
        assert_eq!(
            parsed.offset().local_minus_utc(),
            Local::now().offset().local_minus_utc()
        );
    }
}
