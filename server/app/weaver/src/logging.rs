//! Log format, colour and default-level resolution for the service subscriber.
//!
//! The resolution rules live here, apart from subscriber construction, so they
//! can be unit-tested without installing a global subscriber.

use std::ffi::OsString;

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
}
