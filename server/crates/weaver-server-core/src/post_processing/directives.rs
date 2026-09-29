use super::model::{QueueEvent, ScriptEventLabel};

pub const MAX_PARAMETER_NAME_BYTES: usize = 256;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Directive {
    Parameter { name: String, value: String },
    Directory(String),
    FinalDirectory(String),
    MarkBad,
    Name(String),
    Category(String),
    Priority(i32),
    Top(bool),
    Paused(bool),
    DupeKey(String),
    DupeScore(i32),
    DupeMode(DupeMode),
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DupeMode {
    Score,
    All,
    Force,
}

impl DupeMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Score => "SCORE",
            Self::All => "ALL",
            Self::Force => "FORCE",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub enum ScriptLogLevel {
    Debug,
    Detail,
    Info,
    Warning,
    Error,
}

impl ScriptLogLevel {
    pub fn event_kind(self) -> &'static str {
        match self {
            Self::Error => "ScriptError",
            Self::Warning => "ScriptWarning",
            Self::Info => "ScriptOutput",
            Self::Detail => "ScriptDetail",
            Self::Debug => "ScriptDebug",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ScriptOutputEvent {
    Directive(Directive),
    Log { level: ScriptLogLevel, text: String },
}

pub fn valid_parameter_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_PARAMETER_NAME_BYTES
        && !name.chars().any(|ch| ch == '=' || ch.is_control())
        && crate::is_public_history_attribute_key(name)
}

pub(crate) fn validate_parameter_size<'a>(
    parameters: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<(), String> {
    let mut bytes = 0usize;
    for (count, (name, value)) in parameters.into_iter().enumerate() {
        bytes = bytes.saturating_add(name.len()).saturating_add(value.len());
        if count >= 4096 || bytes > 1024 * 1024 {
            return Err("script parameters exceed the 4096-entry or 1 MiB limit".into());
        }
    }
    Ok(())
}

/// Parse one complete, already-redacted line. Fragmented lines never enter here.
pub fn parse_line(event: &ScriptEventLabel, line: &str) -> (String, Option<ScriptOutputEvent>) {
    let line = line.trim_end();
    let (level, text) = [
        ("[INFO] ", ScriptLogLevel::Info),
        ("[WARNING] ", ScriptLogLevel::Warning),
        ("[ERROR] ", ScriptLogLevel::Error),
        ("[DETAIL] ", ScriptLogLevel::Detail),
        ("[DEBUG] ", ScriptLogLevel::Debug),
    ]
    .into_iter()
    .find_map(|(prefix, level)| line.strip_prefix(prefix).map(|text| (Some(level), text)))
    .unwrap_or((None, line));
    if let Some(command) = text.strip_prefix("[NZB] ") {
        let invalid = || {
            (
                "Invalid command".to_string(),
                Some(ScriptOutputEvent::Log {
                    level: ScriptLogLevel::Error,
                    text: "Invalid command".to_string(),
                }),
            )
        };
        let Some((key, value)) = command.split_once('=') else {
            return invalid();
        };
        let boolean = || match value {
            "0" => Some(false),
            "1" => Some(true),
            _ => None,
        };
        let directive = if let Some(name) = key.strip_prefix("NZBPR_") {
            if !valid_parameter_name(name) || value.contains('\0') {
                return invalid();
            }
            Directive::Parameter {
                name: name.to_string(),
                value: value.to_string(),
            }
        } else {
            match key {
                "DIRECTORY" => Directive::Directory(value.to_string()),
                "FINALDIR" => Directive::FinalDirectory(value.to_string()),
                "MARK" if value == "BAD" => Directive::MarkBad,
                "NZBNAME" => Directive::Name(value.to_string()),
                "CATEGORY" => Directive::Category(value.to_string()),
                "PRIORITY" => {
                    let Ok(value) = value.parse() else {
                        return invalid();
                    };
                    Directive::Priority(value)
                }
                "TOP" => {
                    let Some(value) = boolean() else {
                        return invalid();
                    };
                    Directive::Top(value)
                }
                "PAUSED" => {
                    let Some(value) = boolean() else {
                        return invalid();
                    };
                    Directive::Paused(value)
                }
                "DUPEKEY" => Directive::DupeKey(value.to_string()),
                "DUPESCORE" => {
                    let Ok(value) = value.parse() else {
                        return invalid();
                    };
                    Directive::DupeScore(value)
                }
                "DUPEMODE" => Directive::DupeMode(match value.to_ascii_lowercase().as_str() {
                    "score" => DupeMode::Score,
                    "all" => DupeMode::All,
                    "force" => DupeMode::Force,
                    _ => return invalid(),
                }),
                _ => return invalid(),
            }
        };
        let allowed = match event {
            ScriptEventLabel::PostProcessing => matches!(
                directive,
                Directive::Parameter { .. }
                    | Directive::Directory(_)
                    | Directive::FinalDirectory(_)
                    | Directive::MarkBad
            ),
            ScriptEventLabel::Queue(queue) => {
                matches!(directive, Directive::Parameter { .. } | Directive::MarkBad)
                    || (*queue == QueueEvent::NzbDownloaded
                        && matches!(directive, Directive::Directory(_)))
            }
            ScriptEventLabel::Scan => !matches!(
                directive,
                Directive::Directory(_) | Directive::FinalDirectory(_) | Directive::MarkBad
            ),
            ScriptEventLabel::Scheduler(_) | ScriptEventLabel::Feed(_) => false,
        };
        return if allowed {
            (String::new(), Some(ScriptOutputEvent::Directive(directive)))
        } else {
            let text = format!("Command {key} is not allowed for {event}");
            (
                text.clone(),
                Some(ScriptOutputEvent::Log {
                    level: ScriptLogLevel::Warning,
                    text,
                }),
            )
        };
    }
    (
        text.to_string(),
        level.map(|level| ScriptOutputEvent::Log {
            level,
            text: text.to_string(),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directives_follow_each_kinds_contract() {
        let cases = [
            ("NZBPR_token=value", [true, true, true, true, false, false]),
            (
                "DIRECTORY=/complete",
                [true, false, true, false, false, false],
            ),
            (
                "FINALDIR=/complete",
                [true, false, false, false, false, false],
            ),
            ("MARK=BAD", [true, true, true, false, false, false]),
            ("NZBNAME=name", [false, false, false, true, false, false]),
            ("CATEGORY=tv", [false, false, false, true, false, false]),
            ("PRIORITY=50", [false, false, false, true, false, false]),
            ("TOP=1", [false, false, false, true, false, false]),
            ("PAUSED=0", [false, false, false, true, false, false]),
            ("DUPEKEY=key", [false, false, false, true, false, false]),
            ("DUPESCORE=-1", [false, false, false, true, false, false]),
            ("DUPEMODE=force", [false, false, false, true, false, false]),
        ];
        let events = [
            ScriptEventLabel::PostProcessing,
            ScriptEventLabel::Queue(QueueEvent::NzbAdded),
            ScriptEventLabel::Queue(QueueEvent::NzbDownloaded),
            ScriptEventLabel::Scan,
            ScriptEventLabel::Scheduler(0),
            ScriptEventLabel::Feed(1),
        ];
        for (command, allowed) in cases {
            for (event, expected) in events.iter().zip(allowed) {
                let (_, result) = parse_line(event, &format!("[NZB] {command}\r\n"));
                assert_eq!(
                    matches!(result, Some(ScriptOutputEvent::Directive(_))),
                    expected,
                    "{event}: {command}"
                );
            }
        }
    }

    #[test]
    fn invalid_commands_and_names_are_individual_errors() {
        for command in [
            "unknown=x",
            "MARK=GOOD",
            "TOP=yes",
            "PAUSED=2",
            "DUPEMODE=other",
            "NZBPR_=x",
            "NZBPR_weaver.internal=x",
            "NZBPR_WeAvEr.internal=x",
            "NZBPR___weaver_script_selection=x",
            "NZBPR___WeAvEr_script_selection=x",
            "NZBPR_bad\0=x",
            "PRIORITY=big",
            "no_equals",
        ] {
            assert!(matches!(
                parse_line(&ScriptEventLabel::Scan, &format!("[NZB] {command}")).1,
                Some(ScriptOutputEvent::Log {
                    level: ScriptLogLevel::Error,
                    ..
                })
            ));
        }
        assert!(!valid_parameter_name(
            &"x".repeat(MAX_PARAMETER_NAME_BYTES + 1)
        ));
    }

    #[test]
    fn parameter_budget_bounds_entries_and_combined_bytes() {
        assert!(validate_parameter_size(std::iter::repeat_n(("key", "value"), 4096)).is_ok());
        assert!(validate_parameter_size(std::iter::repeat_n(("key", "value"), 4097)).is_err());
        let value = "x".repeat(1024 * 1024 - 3);
        assert!(validate_parameter_size([("key", value.as_str())]).is_ok());
        assert!(validate_parameter_size([("key", value.as_str()), ("extra", "")]).is_err());
    }

    #[test]
    fn prefixes_are_stripped_and_trailing_cr_is_trimmed() {
        for prefix in ["INFO", "WARNING", "ERROR", "DETAIL", "DEBUG"] {
            let (tail, event) = parse_line(
                &ScriptEventLabel::PostProcessing,
                &format!("[{prefix}] hello  \r\n"),
            );
            assert_eq!(tail, "hello");
            assert!(matches!(event, Some(ScriptOutputEvent::Log { text, .. }) if text == "hello"));
        }
        assert_eq!(
            parse_line(&ScriptEventLabel::PostProcessing, "[nzb] MARK=BAD").1,
            None
        );
    }
}
