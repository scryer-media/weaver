// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Log format, colour, timestamp and default-level resolution for the service
// subscriber.
//
// The resolution rules live here, apart from subscriber construction, so they
// can be unit-tested without installing a global subscriber.

use std::ffi::OsString;
use std::fmt;
use std::io::Write;
use std::sync::OnceLock;

use chrono::{DateTime, Local, SecondsFormat, TimeZone};
use serde_json::Value;
use tracing::field::{Field, Visit};
use tracing::span::Record;
use tracing::{Event, Subscriber};
use tracing_subscriber::field::RecordFields;
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::fmt::{FmtContext, FormattedFields};
use tracing_subscriber::registry::LookupSpan;
use weaver_server_core::runtime::log_delivery::{DeliveryGuard, LogDelivery};

// Renders a log timestamp as RFC 3339 in the zone of `now`.
//
// Weaver emits log timestamps in the host's local zone so that operators who
// set `TZ` (for example through Docker) see wall-clock times that match their
// other services. The offset is always written explicitly (`Z` for UTC,
// otherwise `+HH:MM`/`-HH:MM`) so the string stays machine-parseable.
fn render_timestamp<Tz>(now: DateTime<Tz>, precision: SecondsFormat) -> String
where
    Tz: TimeZone,
    Tz::Offset: fmt::Display,
{
    now.to_rfc3339_opts(precision, true)
}

// Timer for `tracing_subscriber` layers that prints the host's local
// wall-clock time (honouring `TZ`) instead of the UTC default.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct LocalTimer;

impl FormatTime for LocalTimer {
    fn format_time(&self, w: &mut Writer<'_>) -> fmt::Result {
        w.write_str(&render_timestamp(Local::now(), SecondsFormat::Micros))
    }
}

// Environment variable selecting the stdout/log-file record format.
pub(crate) const LOG_FORMAT_ENV: &str = "WEAVER_LOG_FORMAT";
// Environment variable selecting stdout colouring.
pub(crate) const LOG_COLOR_ENV: &str = "WEAVER_LOG_COLOR";

// Record format for the stdout and log-file layers. JSON unless the
// operator asks for text.
//
// The in-memory ring buffer that backs the web log viewer is not covered: it
// always holds JSON records, which the viewer draws as lines itself, whatever
// stdout is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum LogFormat {
    Text,
    #[default]
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

    // Resolve the format from the CLI flag, then the environment, then the
    // default. An unrecognised value falls back to the default rather than
    // aborting startup: a mistyped log format must never keep the service
    // down.
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

// One JSON object per event, on one line:
//
// `{"timestamp":"2026-11-03T08:10:00.123456-07:00","level":"INFO","target":"weaver::jobs","fields":{"message":"job added","job_id":7},"spans":[{"name":"request","fields":{"id":3}}]}`
//
// `timestamp` is local time with its offset, as `LocalTimer` writes it. The
// event's fields, its message first, sit in a `fields` object in the order the
// event names them. `spans` lists the spans the event happened in, outermost
// first, each with its `name` and, when it has any, its own `fields` object in
// the order the span names them; it is left out when there are none.
//
// A value is written as JSON where JSON can hold it: integers whole, `u64` and
// `i64` included, and wider integers as strings. A float that is not a number
// or is infinite, which JSON cannot hold, is written as the string the text
// format shows for it: "NaN", "inf" or "-inf". A `?` field holds its Debug
// text and a `%` field its Display text, both as strings.
//
// Span fields are kept by `JsonSpanFields`, which the layer must use with this
// format.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct JsonRecord;

impl<S, N> FormatEvent<S, N> for JsonRecord
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let metadata = event.metadata();
        let mut fields = OrderedFields::default();
        event.record(&mut fields);

        let mut record = String::with_capacity(256);
        record.push_str("{\"timestamp\":");
        push_json_string(
            &mut record,
            &render_timestamp(Local::now(), SecondsFormat::Micros),
        );
        record.push_str(",\"level\":");
        push_json_string(&mut record, metadata.level().as_str());
        record.push_str(",\"target\":");
        push_json_string(&mut record, metadata.target());
        record.push_str(",\"fields\":{");
        for (index, (name, value)) in fields.message_first().enumerate() {
            if index > 0 {
                record.push(',');
            }
            push_json_string(&mut record, name);
            record.push(':');
            record.push_str(&value.to_string());
        }
        record.push('}');

        if let Some(scope) = ctx.event_scope() {
            let mut first = true;
            for span in scope.from_root() {
                record.push_str(if first { ",\"spans\":[" } else { "," });
                first = false;
                record.push_str("{\"name\":");
                push_json_string(&mut record, span.name());
                let extensions = span.extensions();
                // The span's fields as `JsonSpanFields` kept them: one JSON
                // object. Any other shape is left out rather than mangled.
                if let Some(formatted) = extensions.get::<FormattedFields<N>>() {
                    let body = formatted.fields.trim();
                    if body.len() > 2 && body.starts_with('{') && body.ends_with('}') {
                        record.push_str(",\"fields\":");
                        record.push_str(body);
                    }
                }
                record.push('}');
            }
            if !first {
                record.push(']');
            }
        }
        record.push('}');
        writer.write_str(&record)?;
        writer.write_char('\n')
    }
}

// A span's fields as one JSON object, in the order the span names them, for
// `JsonRecord` to carry. Fields recorded later are added at the end.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct JsonSpanFields;

impl<'writer> FormatFields<'writer> for JsonSpanFields {
    fn format_fields<R: RecordFields>(
        &self,
        mut writer: Writer<'writer>,
        fields: R,
    ) -> fmt::Result {
        let mut ordered = OrderedFields::default();
        fields.record(&mut ordered);
        writer.write_str(&ordered.object())
    }

    fn add_fields(&self, current: &mut FormattedFields<Self>, fields: &Record<'_>) -> fmt::Result {
        let mut ordered = OrderedFields::default();
        fields.record(&mut ordered);
        if ordered.0.is_empty() {
            return Ok(());
        }
        let more = ordered.object();
        if current.fields.len() <= 2 {
            current.fields = more;
        } else {
            current.fields.pop();
            current.fields.push(',');
            current.fields.push_str(&more[1..]);
        }
        Ok(())
    }
}

fn push_json_string(out: &mut String, value: &str) {
    // Serializing a string cannot fail.
    out.push_str(&serde_json::to_string(value).unwrap_or_default());
}

// An event's fields in the order the event names them.
#[derive(Default)]
struct OrderedFields(Vec<(&'static str, Value)>);

impl OrderedFields {
    // The fields as one JSON object, in order.
    fn object(&self) -> String {
        let mut object = String::from("{");
        for (index, (name, value)) in self.0.iter().enumerate() {
            if index > 0 {
                object.push(',');
            }
            push_json_string(&mut object, name);
            object.push(':');
            object.push_str(&value.to_string());
        }
        object.push('}');
        object
    }

    fn message_first(&self) -> impl Iterator<Item = &(&'static str, Value)> {
        let message = self.0.iter().filter(|(name, _)| *name == "message");
        message.chain(self.0.iter().filter(|(name, _)| *name != "message"))
    }
}

impl Visit for OrderedFields {
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.push((field.name(), Value::from(value)));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.push((field.name(), Value::from(value)));
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        self.0
            .push((field.name(), Value::String(value.to_string())));
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        self.0
            .push((field.name(), Value::String(value.to_string())));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.push((field.name(), Value::from(value)));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        // JSON has no NaN or infinity; the text format's spelling survives.
        let value = if value.is_finite() {
            Value::from(value)
        } else {
            Value::String(value.to_string())
        };
        self.0.push((field.name(), value));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.push((field.name(), Value::String(value.to_owned())));
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.0
            .push((field.name(), Value::String(value.to_string())));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0
            .push((field.name(), Value::String(format!("{value:?}"))));
    }
}

// Hands each formatted event to a delivery, whose own thread writes it.
pub(crate) struct DeliveredWriter(pub(crate) LogDelivery);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for DeliveredWriter {
    type Writer = weaver_server_core::runtime::log_delivery::EventWriter<'a>;
    fn make_writer(&'a self) -> Self::Writer {
        self.0.make_writer()
    }
}

// A console or file layer: the event is formatted on the thread that logged
// it and written by the delivery's thread. A sink that stops taking writes,
// such as a stdout pipe nobody reads, costs counted dropped events, never a
// thread that logs.
pub(crate) fn delivered_layer<S>(
    format: LogFormat,
    ansi: bool,
    delivery: LogDelivery,
) -> Box<dyn tracing_subscriber::Layer<S> + Send + Sync>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let writer = DeliveredWriter(delivery);
    match format {
        LogFormat::Json => Box::new(
            tracing_subscriber::fmt::layer()
                .fmt_fields(JsonSpanFields)
                .event_format(JsonRecord)
                .with_writer(writer)
                .with_ansi(false),
        ),
        LogFormat::Text => Box::new(
            tracing_subscriber::fmt::layer()
                .with_timer(LocalTimer)
                .with_writer(writer)
                .with_ansi(ansi),
        ),
    }
}

// Standard output as a delivered sink: the process-wide stdout lock is taken
// by the delivery's thread alone.
pub(crate) fn open_stdout_delivery() -> std::io::Result<(LogDelivery, DeliveryGuard)> {
    LogDelivery::new("stdout", |bytes| {
        let mut stdout = std::io::stdout().lock();
        stdout.write_all(bytes)?;
        stdout.flush()
    })
}

static CONSOLE_FORMAT: OnceLock<LogFormat> = OnceLock::new();

// Records the format the console layers were built with, so output written
// outside tracing can match it.
pub(crate) fn set_console_format(format: LogFormat) {
    let _ = CONSOLE_FORMAT.set(format);
}

// What to tell the operator next to a setup code.
const SETUP_CODE_INSTRUCTIONS: &str = "Open Weaver in your browser and enter this code to create the administrator account. It works until setup finishes or Weaver restarts.";

// Prints the first-run setup code to stderr, in the console's log format.
//
// The code goes around tracing on purpose: tracing also feeds the in-app log
// viewer and the log file, and neither should hold a live credential.
pub(crate) fn announce_setup_code(code: &str) {
    let format = CONSOLE_FORMAT.get().copied().unwrap_or_default();
    let timestamp = render_timestamp(Local::now(), SecondsFormat::Micros);
    let announcement = setup_code_announcement(code, format, &timestamp);
    let mut stderr = std::io::stderr().lock();
    let _ = stderr.write_all(announcement.as_bytes());
    let _ = stderr.flush();
}

// The setup code announcement: a banner a person can't scroll past in text
// mode, one record shaped like the rest of the log in JSON mode. Both carry
// `SETUP_CODE_MARKER` directly before the code, which launchers look for.
fn setup_code_announcement(code: &str, format: LogFormat, timestamp: &str) -> String {
    use weaver_server_core::auth::SETUP_CODE_MARKER;

    match format {
        LogFormat::Json => {
            let record = serde_json::json!({
                "timestamp": timestamp,
                "level": "WARN",
                "fields": {
                    "message": format!(
                        "ACTION REQUIRED: {SETUP_CODE_MARKER}{code}. {SETUP_CODE_INSTRUCTIONS}"
                    ),
                    "setup_code": code,
                },
                "target": "weaver::setup",
            });
            format!("{record}\n")
        }
        LogFormat::Text => {
            let mut rows = vec![
                String::new(),
                "FIRST-TIME SETUP: ACTION REQUIRED".to_string(),
                String::new(),
                format!("{SETUP_CODE_MARKER}{code}"),
                String::new(),
            ];
            rows.extend(wrap_words(SETUP_CODE_INSTRUCTIONS, 56));
            rows.push(String::new());
            let width = rows.iter().map(String::len).max().unwrap_or(0) + 6;
            let rule = "#".repeat(width + 2);
            let mut banner = format!("\n{rule}\n");
            for row in rows {
                banner.push_str(&format!("#   {row:<inner$}#\n", inner = width - 3));
            }
            banner.push_str(&format!("{rule}\n\n"));
            banner
        }
    }
}

fn wrap_words(text: &str, width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut line = String::new();
    for word in text.split_whitespace() {
        if !line.is_empty() && line.len() + 1 + word.len() > width {
            lines.push(std::mem::take(&mut line));
        }
        if !line.is_empty() {
            line.push(' ');
        }
        line.push_str(word);
    }
    if !line.is_empty() {
        lines.push(line);
    }
    lines
}

// Colour policy for the stdout layer.
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

    // Whether stdout should be coloured.
    //
    // `NO_COLOR` (any value, per the informal convention) forces colour off
    // unless the operator asked for `always` explicitly. `auto` colours only
    // when stdout is a terminal, so piping to a file or a log collector no
    // longer embeds escape sequences — which is what it did unconditionally
    // before.
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
    fn log_format_defaults_to_json_and_accepts_text() {
        assert_eq!(LogFormat::resolve(None, None), LogFormat::Json);
        assert_eq!(LogFormat::resolve(Some("text"), None), LogFormat::Text);
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
            LogFormat::Json
        );
        assert_eq!(
            LogFormat::resolve(Some("yaml"), Some(&os("text"))),
            LogFormat::Text
        );
        assert_eq!(LogFormat::parse("yaml"), None);
    }

    #[test]
    fn setup_code_banner_is_boxed_and_launchers_can_find_the_code() {
        use weaver_server_core::auth::find_setup_code;

        let banner = setup_code_announcement("K7P-M2X", LogFormat::Text, "unused");
        let rows: Vec<&str> = banner.lines().filter(|row| !row.is_empty()).collect();
        let width = rows[0].len();
        assert!(rows.iter().all(|row| row.len() == width), "{banner}");
        assert!(rows[0].chars().all(|c| c == '#'));
        assert!(banner.contains("FIRST-TIME SETUP: ACTION REQUIRED"));
        assert_eq!(
            banner
                .lines()
                .filter_map(find_setup_code)
                .collect::<Vec<_>>(),
            ["K7P-M2X"]
        );
    }

    #[test]
    fn setup_code_json_record_is_one_parseable_line() {
        use weaver_server_core::auth::find_setup_code;

        let record = setup_code_announcement("K7P-M2X", LogFormat::Json, "2026-09-14T12:00:00Z");
        assert_eq!(record.lines().count(), 1);
        let value: serde_json::Value = serde_json::from_str(record.trim_end()).expect("json");
        assert_eq!(value["timestamp"], "2026-09-14T12:00:00Z");
        assert_eq!(value["level"], "WARN");
        assert_eq!(value["fields"]["setup_code"], "K7P-M2X");
        assert!(
            value["fields"]["message"]
                .as_str()
                .unwrap()
                .starts_with("ACTION REQUIRED: Weaver one-time setup code: K7P-M2X. ")
        );
        assert_eq!(find_setup_code(&record), Some("K7P-M2X"));
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

    // The text format, which an operator can still ask for, starts each line
    // with an RFC 3339 timestamp in local time and then the level.
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

    // Everything one JSON-record layer wrote while `emit` ran.
    fn json_records(emit: impl FnOnce()) -> String {
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
                .fmt_fields(JsonSpanFields)
                .event_format(JsonRecord)
                .with_writer(buffer.clone())
                .with_ansi(false),
        );
        tracing::subscriber::with_default(subscriber, emit);
        let raw = buffer.0.lock().expect("buffer lock").clone();
        String::from_utf8(raw).expect("utf-8")
    }

    #[test]
    fn a_json_record_is_one_object_per_event_with_its_fields_in_order() {
        let raw = json_records(|| {
            tracing::info!(job_id = 7_u64, name = "My Show", ok = true, "job added");
            tracing::warn!(error = %"disk full\nretrying", "write failed");
        });
        let lines: Vec<&str> = raw.lines().collect();
        assert_eq!(lines.len(), 2, "{raw}");

        let record: Value = serde_json::from_str(lines[0]).expect("json");
        assert_eq!(record["level"], "INFO");
        assert_eq!(record["target"], module_path!());
        assert_eq!(record["fields"]["message"], "job added");
        assert_eq!(record["fields"]["job_id"], 7);
        assert_eq!(record["fields"]["name"], "My Show");
        assert_eq!(record["fields"]["ok"], true);
        assert!(record.get("spans").is_none(), "{raw}");
        // The fields keep the event's order, message first, so a reader
        // matching `message job_id=` still finds them together.
        assert!(
            lines[0].contains(
                r#""fields":{"message":"job added","job_id":7,"name":"My Show","ok":true}"#
            ),
            "{raw}"
        );
        // Keys read top-down the way a person scans a record.
        assert!(lines[0].starts_with(r#"{"timestamp":""#), "{raw}");

        // A newline inside a value stays inside the one record.
        let record: Value = serde_json::from_str(lines[1]).expect("json");
        assert_eq!(record["level"], "WARN");
        assert_eq!(record["fields"]["error"], "disk full\nretrying");
    }

    #[test]
    fn a_json_record_carries_the_spans_it_happened_in() {
        let raw = json_records(|| {
            let outer = tracing::info_span!("request", id = 3_u64);
            let _outer = outer.enter();
            let inner = tracing::info_span!("job", job_id = 9_u64);
            let _inner = inner.enter();
            tracing::info!("inside");
        });
        let record: Value = serde_json::from_str(raw.trim_end()).expect("json");
        assert_eq!(
            record["spans"],
            serde_json::json!([
                { "name": "request", "fields": { "id": 3 } },
                { "name": "job", "fields": { "job_id": 9 } },
            ]),
            "{raw}"
        );
    }

    #[test]
    fn a_json_record_is_stamped_in_local_time() {
        let raw = json_records(|| tracing::info!("stamp"));
        let record: Value = serde_json::from_str(raw.trim_end()).expect("json");
        let stamp = record["timestamp"].as_str().expect("timestamp");
        let parsed = DateTime::parse_from_rfc3339(stamp).expect("rfc3339");
        assert_eq!(
            parsed.offset().local_minus_utc(),
            Local::now().offset().local_minus_utc()
        );
        // Microseconds, as the text format writes them.
        let fraction = stamp.split('.').nth(1).expect("fraction");
        assert_eq!(fraction.chars().take_while(char::is_ascii_digit).count(), 6);
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

    // A console sink that stops taking writes, as a stdout pipe whose reader
    // stalled does, holds no thread that logs: a task on the runtime keeps
    // logging and finishes while the sink holds its first event, and each
    // event the full queue cannot take is counted as dropped.
    #[test]
    fn a_stalled_console_sink_never_holds_a_thread_that_logs() {
        use std::sync::mpsc;
        use tracing_subscriber::layer::SubscriberExt;
        use weaver_server_core::runtime::log_delivery::counter_snapshot;

        const SINK: &str = "stdout-stall-fixture";
        const EVENTS: u64 = 1_000;
        let (entered, entering) = mpsc::channel();
        let (release, released) = mpsc::channel::<()>();
        let (written, writes) = mpsc::channel();
        let mut first = true;
        let (delivery, guard) = LogDelivery::new(SINK, move |bytes| {
            if std::mem::take(&mut first) {
                entered.send(()).unwrap();
                released.recv().unwrap();
            }
            written.send(bytes.to_vec()).unwrap();
            Ok(())
        })
        .unwrap();
        let subscriber =
            tracing_subscriber::registry().with(delivered_layer(LogFormat::Json, false, delivery));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();

        let logged = tracing::subscriber::with_default(subscriber, || {
            tracing::info!("held by the sink");
            entering.recv().unwrap();
            // The runtime has this one thread, so a write that waited on the
            // held sink would stop the task, and the test, here.
            runtime.block_on(async {
                tokio::spawn(async {
                    for beat in 0..EVENTS {
                        tracing::info!(beat, "weaver heartbeat");
                    }
                    EVENTS
                })
                .await
                .unwrap()
            })
        });

        assert_eq!(logged, EVENTS);
        let dropped = counter_snapshot()[SINK].0;
        assert!(dropped > 0, "a full queue drops events and counts them");
        release.send(()).unwrap();
        // Every event is written or counted dropped, never both.
        for _ in 0..1 + EVENTS - dropped {
            writes.recv().unwrap();
        }
        drop(guard);
        assert!(writes.recv().is_err(), "nothing past the count is written");
    }
}

// Real tracing events through JsonRecord, each line read back by serde_json
// and compared with what the text layer wrote for the same event.
#[cfg(test)]
mod json_record_against_text {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::fmt::MakeWriter;
    use tracing_subscriber::layer::SubscriberExt;

    #[derive(Clone, Default)]
    struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for SharedBuffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'w> MakeWriter<'w> for SharedBuffer {
        type Writer = Self;
        fn make_writer(&'w self) -> Self::Writer {
            self.clone()
        }
    }

    // (json lines, old text lines) for the same events.
    fn both(emit: impl Fn()) -> (Vec<String>, Vec<String>) {
        let json = SharedBuffer::default();
        let text = SharedBuffer::default();
        let subscriber = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .fmt_fields(JsonSpanFields)
                    .event_format(JsonRecord)
                    .with_writer(json.clone())
                    .with_ansi(false),
            )
            .with(
                tracing_subscriber::fmt::layer()
                    .with_timer(LocalTimer)
                    .with_writer(text.clone())
                    .with_ansi(false),
            );
        tracing::subscriber::with_default(subscriber, emit);
        let read = |b: &SharedBuffer| {
            String::from_utf8(b.0.lock().unwrap().clone())
                .unwrap()
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        };
        let _ = std::io::stdout().flush();
        (read(&json), read(&text))
    }

    #[test]
    fn every_escape_path_is_valid_json() {
        let (json, _) = both(|| {
            tracing::info!(
                quote = "a\"b",
                backslash = r"C:\x\y",
                control = "nul\0esc\u{1b}del\u{7f}ls\u{2028}",
                debug_bytes = ?b"\xff\xfe",
                debug_str = ?"q\"",
                big = u64::MAX,
                neg = i64::MIN,
                wide = u128::MAX,
                "msg with \"quotes\" and \\ and \t tab"
            );
            tracing::info!(
                nan = f64::NAN,
                inf = f64::INFINITY,
                ninf = f64::NEG_INFINITY,
                "floats"
            );
            tracing::info!(only = 1_u64);
            let span = tracing::info_span!("outer", job_id = 1_u64, name = "field-called-name");
            let _g = span.enter();
            tracing::info!(job_id = 2_u64, "inner");
        });
        assert_eq!(json.len(), 4, "{json:#?}");
        for line in &json {
            serde_json::from_str::<Value>(line).unwrap_or_else(|e| panic!("{e}: {line}"));
        }
        let first: Value = serde_json::from_str(&json[0]).unwrap();
        assert_eq!(first["fields"]["big"], u64::MAX);
        assert_eq!(
            first["fields"]["control"],
            "nul\0esc\u{1b}del\u{7f}ls\u{2028}"
        );
    }

    // A float that is not a number or is infinite reads in the record as the
    // text the text line writes, `NaN` or `inf`, never as null.
    #[test]
    fn non_finite_floats_read_as_the_text_line_showed_them() {
        let (json, text) = both(|| {
            tracing::info!(nan = f64::NAN, inf = f64::INFINITY, "floats");
        });
        let record: Value = serde_json::from_str(&json[0]).unwrap();
        assert!(text[0].contains("nan=NaN inf=inf"), "{}", text[0]);
        assert_ne!(record["fields"]["nan"], Value::Null, "{}", json[0]);
        assert_ne!(record["fields"]["inf"], Value::Null, "{}", json[0]);
    }

    // A span with a field called `name` keeps that field under the span's
    // fields; the span's own name stays the span's name.
    #[test]
    fn a_span_field_called_name_keeps_the_span_name() {
        let (json, _) = both(|| {
            let span = tracing::info_span!("outer", name = "field-called-name");
            let _g = span.enter();
            tracing::info!("inner");
        });
        let record: Value = serde_json::from_str(&json[0]).unwrap();
        assert_eq!(record["spans"][0]["name"], "outer", "{}", json[0]);
    }

    // The span's fields as the text layer orders them: declaration order.
    #[test]
    fn span_fields_keep_their_declared_order() {
        let (json, text) = both(|| {
            let span = tracing::info_span!("req", zeta = 1_u64, alpha = 2_u64);
            let _g = span.enter();
            tracing::info!("inner");
        });
        assert!(text[0].contains("req{zeta=1 alpha=2}"), "{}", text[0]);
        let line = weaver_server_core::runtime::log_buffer::text_line(&json[0]);
        assert!(line.contains("req{zeta=1 alpha=2}"), "{line}");
    }

    // text_line against the old text layer for ordinary events, timestamp cut.
    // One difference is by design: a JSON record cannot tell a `%` value from
    // a string field, so a value holding a space is quoted, where the old
    // layer wrote a `%` value bare.
    #[test]
    fn text_line_matches_the_old_text_layer() {
        let (json, text) = both(|| {
            tracing::info!(job_id = 7_u64, name = %"My Show", path = %"/data/a", "job added");
            tracing::warn!(error = %"disk full", "write failed");
            tracing::info!(err = ?std::io::Error::from(std::io::ErrorKind::NotFound), "dbg");
            tracing::info!(name = ?String::from("plain"), "debug string");
        });
        let cut = |s: &str| s.split_once(' ').unwrap().1.to_owned();
        let mut diffs = Vec::new();
        for (j, t) in json.iter().zip(&text) {
            let line = weaver_server_core::runtime::log_buffer::text_line(j);
            let old = cut(t)
                .replace("name=My Show", "name=\"My Show\"")
                .replace("error=disk full", "error=\"disk full\"");
            if cut(&line) != old {
                diffs.push(format!("old: {}\nnew: {}", cut(t), cut(&line)));
            }
        }
        assert!(diffs.is_empty(), "\n{}", diffs.join("\n"));
    }
}
