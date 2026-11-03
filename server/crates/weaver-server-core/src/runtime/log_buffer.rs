// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::VecDeque;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::broadcast;

const DEFAULT_CAPACITY: usize = 1000;
const BROADCAST_CAPACITY: usize = 256;

// The log file path the process actually resolved at startup.
//
// The resolution rules (CLI flag, environment, platform default) live in the
// binary, so anything that needs the *real* path — the diagnostics package, in
// particular — would otherwise have to guess at them a second time and get it
// wrong. Recorded once, read-only afterwards.
static RESOLVED_LOG_FILE_PATH: OnceLock<PathBuf> = OnceLock::new();

// Records the resolved log file path. The first call wins; later calls are
// ignored so a test or a second subscriber cannot move the recorded path.
pub fn set_log_file_path(path: PathBuf) {
    let _ = RESOLVED_LOG_FILE_PATH.set(path);
}

// The resolved log file path, or `None` when this process writes no log file.
pub fn log_file_path() -> Option<&'static Path> {
    RESOLVED_LOG_FILE_PATH.get().map(PathBuf::as_path)
}

// The active log file followed by every rotated generation that exists on
// disk, in newest-first order, then the stderr capture a supervisor keeps
// beside the log. Entries that do not exist are skipped, so the result is
// exactly the set of files a collector can read.
pub fn existing_log_files() -> Vec<PathBuf> {
    let Some(path) = log_file_path() else {
        return Vec::new();
    };
    let mut files = super::log_file::existing_files(path);
    files.extend(super::log_file::existing_files(&stderr_capture_path(path)));
    files
}

// Where a process that supervises this one keeps its captured stderr: the
// runtime's last words on a crash go there, not through the logger, and a
// collector wants them with the log. `weaver.log` pairs with
// `weaver.stderr.log`; the rotation engine also discovers its legacy `.1`.
pub fn stderr_capture_path(log_file: &Path) -> PathBuf {
    log_file.with_extension("stderr.log")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stderr_capture_sits_beside_the_log_it_belongs_to() {
        let capture = stderr_capture_path(Path::new("/var/weaver/logs/weaver.log"));
        assert_eq!(capture, Path::new("/var/weaver/logs/weaver.stderr.log"));
        assert_eq!(
            capture.with_extension("log.1"),
            Path::new("/var/weaver/logs/weaver.stderr.log.1")
        );
    }
}

// Thread-safe ring buffer that captures log records for live viewing.
//
// Implements `io::Write` so it can be used as a tracing subscriber layer writer.
// Each complete line (terminated by `\n`) is stored in the ring buffer and
// broadcast to any active subscribers. The service writes one JSON record
// per line; `text_line` turns one back into the familiar text line for a
// reader that wants text, so the buffer keeps only the one copy.
#[derive(Clone)]
pub struct LogRingBuffer {
    inner: Arc<Mutex<RingBufferInner>>,
    tx: broadcast::Sender<String>,
}

struct RingBufferInner {
    lines: VecDeque<String>,
    capacity: usize,
    // Accumulates partial writes (no trailing newline yet).
    partial: String,
}

impl LogRingBuffer {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            inner: Arc::new(Mutex::new(RingBufferInner {
                lines: VecDeque::with_capacity(capacity),
                capacity,
                partial: String::new(),
            })),
            tx,
        }
    }

    pub fn with_default_capacity() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }

    // Returns the last `limit` lines from the buffer.
    pub fn snapshot(&self, limit: usize) -> Vec<String> {
        let inner = self.inner.lock().unwrap();
        let safe_limit = limit.min(inner.lines.len());
        inner
            .lines
            .iter()
            .skip(inner.lines.len().saturating_sub(safe_limit))
            .cloned()
            .collect()
    }

    // Subscribe to live log line broadcasts.
    pub fn subscribe(&self) -> broadcast::Receiver<String> {
        self.tx.subscribe()
    }
}

impl Write for LogRingBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let text = String::from_utf8_lossy(buf);
        let mut inner = self.inner.lock().unwrap();

        let mut new_lines = Vec::new();
        for ch in text.chars() {
            if ch == '\n' {
                if !inner.partial.is_empty() {
                    let line = std::mem::take(&mut inner.partial);
                    if inner.lines.len() >= inner.capacity {
                        inner.lines.pop_front();
                    }
                    inner.lines.push_back(line.clone());
                    new_lines.push(line);
                }
            } else {
                inner.partial.push(ch);
            }
        }
        drop(inner);
        for line in new_lines {
            let _ = self.tx.send(line);
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// A JSON log record as the familiar text line:
// `{timestamp} {LEVEL} {spans}{target}: {message} {key=value ...}`.
//
// A string value is written bare unless it is empty or holds a space, a
// quote, an `=` or a control character, in which case it is quoted. One that
// is already a quoted string, as a `?` field's Debug text of a string is, is
// written as it stands. A line that is not a record is returned as it is.
pub fn text_line(record: &str) -> String {
    let Ok(record) = serde_json::from_str::<TextRecord>(record) else {
        return record.to_owned();
    };
    let mut line = format!("{} {:>5} ", record.timestamp, record.level);
    for span in &record.spans {
        line.push_str(&span.name);
        let fields: Vec<String> = span
            .fields
            .0
            .iter()
            .map(|(key, value)| format!("{key}={}", text_value(value)))
            .collect();
        if !fields.is_empty() {
            line.push('{');
            line.push_str(&fields.join(" "));
            line.push('}');
        }
        line.push(':');
    }
    if !record.spans.is_empty() {
        line.push(' ');
    }
    line.push_str(&record.target);
    line.push(':');
    for (key, value) in &record.fields.0 {
        line.push(' ');
        if key == "message" {
            line.push_str(value.as_str().unwrap_or(&value.to_string()));
        } else {
            line.push_str(key);
            line.push('=');
            line.push_str(&text_value(value));
        }
    }
    line
}

fn text_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) if is_quoted(text) => text.clone(),
        serde_json::Value::String(text) => {
            let quote = text.is_empty()
                || text
                    .chars()
                    .any(|c| c.is_whitespace() || c.is_control() || c == '"' || c == '=');
            if quote {
                format!("{text:?}")
            } else {
                text.clone()
            }
        }
        other => other.to_string(),
    }
}

// Whether the text is a string already quoted: a `"` at each end.
fn is_quoted(text: &str) -> bool {
    text.len() >= 2 && text.starts_with('"') && text.ends_with('"')
}

#[derive(serde::Deserialize)]
struct TextSpan {
    #[serde(default)]
    name: String,
    #[serde(default)]
    fields: InOrder,
}

#[derive(serde::Deserialize)]
struct TextRecord {
    timestamp: String,
    level: String,
    target: String,
    #[serde(default)]
    fields: InOrder,
    #[serde(default)]
    spans: Vec<TextSpan>,
}

// A JSON object's entries in the order they were written.
#[derive(Default)]
struct InOrder(Vec<(String, serde_json::Value)>);

impl<'de> serde::Deserialize<'de> for InOrder {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Entries;

        impl<'de> serde::de::Visitor<'de> for Entries {
            type Value = InOrder;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an object")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<InOrder, A::Error> {
                let mut entries = Vec::new();
                while let Some(entry) = map.next_entry()? {
                    entries.push(entry);
                }
                Ok(InOrder(entries))
            }
        }

        deserializer.deserialize_map(Entries)
    }
}

#[cfg(test)]
mod text_line_tests {
    use super::*;

    #[test]
    fn a_record_reads_as_the_text_line_it_stands_for() {
        let record = r#"{"timestamp":"2026-11-03T08:10:00.123456-07:00","level":"INFO","target":"weaver::jobs","fields":{"message":"job added","job_id":7,"name":"My Show","path":"/data/a","ok":true}}"#;
        assert_eq!(
            text_line(record),
            r#"2026-11-03T08:10:00.123456-07:00  INFO weaver::jobs: job added job_id=7 name="My Show" path=/data/a ok=true"#
        );
    }

    #[test]
    fn spans_come_before_the_target_as_text_writes_them() {
        let record = r#"{"timestamp":"2026-11-03T08:10:00.123456Z","level":"ERROR","target":"weaver::http","fields":{"message":"failed"},"spans":[{"name":"request","fields":{"id":3}},{"name":"bare"}]}"#;
        assert_eq!(
            text_line(record),
            "2026-11-03T08:10:00.123456Z ERROR request{id=3}:bare: weaver::http: failed"
        );
    }

    #[test]
    fn a_debug_string_is_quoted_once() {
        let record = r#"{"timestamp":"2026-11-03T08:10:00.123456-07:00","level":"INFO","target":"weaver::jobs","fields":{"message":"dbg","name":"\"plain\""}}"#;
        assert_eq!(
            text_line(record),
            r#"2026-11-03T08:10:00.123456-07:00  INFO weaver::jobs: dbg name="plain""#
        );
    }

    #[test]
    fn a_line_that_is_not_a_record_is_left_alone() {
        assert_eq!(text_line("plain words"), "plain words");
        assert_eq!(text_line("{\"half\":"), "{\"half\":");
    }

    #[test]
    fn the_buffer_keeps_one_record_per_line() {
        let mut buffer = LogRingBuffer::new(2);
        buffer.write_all(b"{\"a\":1}\n{\"b\":").unwrap();
        buffer.write_all(b"2}\n{\"c\":3}\n").unwrap();
        assert_eq!(buffer.snapshot(10), ["{\"b\":2}", "{\"c\":3}"]);
    }
}
