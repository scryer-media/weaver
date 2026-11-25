// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// `search_service_logs`: the in-memory service log, newest first, by
// regular expression and level, each line written as `service-log.txt`
// writes it and sanitized by the package's pass before it is matched, so a
// pattern finds only what the package would show. ADMIN only.
//
// Lines are numbered as the buffer holds them, so a `before` cursor still
// points at the same line after newer ones arrive; a line that rolled off
// the buffer is gone.

use std::sync::atomic::{AtomicBool, Ordering};

use serde::Deserialize;
use serde_json::{Map, Value, json};
use weaver_server_core::runtime::log_buffer::text_line;

use super::diagnostics_section::cut_chars;
use super::{
    Arguments, ToolContext, ToolEntry, ToolFuture, admit_diagnostics, capped_result, coded_error,
    invalid_argument, local_read, tool,
};
use crate::http::graphql::refusal;

// The longest pattern accepted, in characters.
const PATTERN_MAX_CHARS: usize = 256;
// Compiled program and lazy DFA size limits, so a pattern cannot claim
// unbounded memory.
const PATTERN_SIZE_LIMIT: usize = 1 << 20;
const PATTERN_DFA_SIZE_LIMIT: usize = 1 << 20;
// Characters of one line kept in a result.
const LINE_MAX_CHARS: usize = 500;
// Characters of matching lines one result holds; with the line cap this
// keeps a result near 5,000 tokens and under the 32 KiB cap.
const LINES_BUDGET_CHARS: usize = 16_000;
const DEFAULT_MAX_LINES: i64 = 40;
const MAX_LINES: i64 = 100;
const CURSOR_PREFIX: &str = "buffer:";

// A log level, lowest first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    const ALL: [Self; 5] = [
        Self::Trace,
        Self::Debug,
        Self::Info,
        Self::Warn,
        Self::Error,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::Trace => "TRACE",
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|level| level.as_str().eq_ignore_ascii_case(value))
    }
}

// A case-insensitive pattern, bounded in length and compiled size.
#[derive(Debug)]
struct LogPattern(regex::Regex);

impl LogPattern {
    fn new(pattern: &str) -> Option<Self> {
        if pattern.chars().count() > PATTERN_MAX_CHARS {
            return None;
        }
        regex::RegexBuilder::new(pattern)
            .case_insensitive(true)
            .size_limit(PATTERN_SIZE_LIMIT)
            .dfa_size_limit(PATTERN_DFA_SIZE_LIMIT)
            .build()
            .ok()
            .map(Self)
    }
}

fn parse_cursor(value: &str) -> Option<u64> {
    value.strip_prefix(CURSOR_PREFIX)?.parse().ok()
}

fn render_cursor(number: u64) -> String {
    format!("{CURSOR_PREFIX}{number}")
}

pub(super) fn entry() -> ToolEntry {
    let levels = LogLevel::ALL
        .iter()
        .map(|level| level.as_str())
        .collect::<Vec<_>>();
    ToolEntry {
        tool: tool(
            "search_service_logs",
            "Searches the sanitized in-memory service log newest first by regex and level; page with before: next_cursor. Needs an ADMIN key.",
            json!({
                "type": "object",
                "properties": {
                    "pattern": {"type": "string", "maxLength": PATTERN_MAX_CHARS},
                    "level": {"enum": levels},
                    "max_lines": {"type": "integer", "minimum": 1, "maximum": MAX_LINES},
                    "before": {"type": "string"},
                },
            }),
            local_read(),
        ),
        accepts: &["pattern", "level", "max_lines", "before"],
        run,
    }
}

fn run(context: ToolContext, arguments: Arguments) -> ToolFuture {
    Box::pin(async move {
        let min_level = match arguments.str("level", 20)? {
            None => None,
            Some(level) => Some(
                LogLevel::parse(level)
                    .ok_or_else(|| invalid_argument("level", "must be one of the listed levels"))?,
            ),
        };
        let max_lines = arguments
            .int("max_lines", 1, MAX_LINES)?
            .unwrap_or(DEFAULT_MAX_LINES) as usize;
        let before = match arguments.str("before", 40)? {
            None => None,
            Some(cursor) => Some(
                parse_cursor(cursor)
                    .ok_or_else(|| invalid_argument("before", "must be a next_cursor value"))?,
            ),
        };
        let pattern = arguments
            .str("pattern", PATTERN_MAX_CHARS)?
            .map(str::to_string);
        if let Some(refused) = admit_diagnostics(&context) {
            return Ok(refused);
        }
        let pattern = match pattern.as_deref().map(LogPattern::new) {
            None => None,
            Some(Some(pattern)) => Some(pattern),
            Some(None) => {
                return Ok(coded_error(
                    "BAD_USER_INPUT",
                    refusal::MCP_LOG_PATTERN_INVALID,
                    "The pattern is not a valid regular expression, or it is too large once \
                     compiled.",
                ));
            }
        };
        let search = LogSearch {
            pattern,
            min_level,
            max_lines,
            max_chars: LINES_BUDGET_CHARS,
            before,
        };

        let log = context.diagnostics.service_log().await;
        let table_complete = log.table_complete();
        let cancelled = context.cancelled.clone();
        let searched = tokio::task::spawn_blocking(move || {
            let sanitizer = log.sanitizer().ok()?;
            Some(search_log(
                &log.records,
                log.first,
                |line| sanitizer.line(line),
                &search,
                &cancelled,
            ))
        })
        .await;
        let found = match searched {
            Ok(Some(found)) => found,
            // A line the table could not replace would go out as it is.
            Ok(None) => {
                return Ok(capped_result(
                    &json!({"logs_unavailable": "the stand-in table could not be built", "returned": 0}),
                    context.result_max_bytes,
                ));
            }
            Err(_) => {
                return Ok(capped_result(
                    &json!({"logs_unavailable": "the search did not finish", "returned": 0}),
                    context.result_max_bytes,
                ));
            }
        };
        let mut result = Map::new();
        result.insert("scanned".into(), json!(found.scanned));
        result.insert("returned".into(), json!(found.lines.len()));
        result.insert(
            "lines".into(),
            Value::Array(
                found
                    .lines
                    .into_iter()
                    .map(|line| json!({"source": "buffer", "line": line}))
                    .collect(),
            ),
        );
        result.insert(
            "next_cursor".into(),
            json!(found.next_cursor.map(render_cursor)),
        );
        result.insert("stand_in_table_complete".into(), json!(table_complete));
        Ok(capped_result(
            &Value::Object(result),
            context.result_max_bytes,
        ))
    })
}

#[derive(Debug)]
struct LogSearch {
    pattern: Option<LogPattern>,
    // Lines at this level or above.
    min_level: Option<LogLevel>,
    max_lines: usize,
    // Characters of matching lines a result holds at most. The line that
    // would pass it starts the next page; the first line always fits.
    max_chars: usize,
    // Only lines numbered below this one.
    before: Option<u64>,
}

#[derive(Debug, Default)]
struct LogSearchResult {
    // Newest first, sanitized and cut.
    lines: Vec<String>,
    // Lines read to produce the result.
    scanned: usize,
    // Where the next older page starts; `None` when nothing older is held.
    next_cursor: Option<u64>,
    // Whether the search stopped because the client went away.
    cancelled: bool,
}

#[derive(Deserialize)]
struct LevelOnly<'a> {
    #[serde(borrow)]
    level: std::borrow::Cow<'a, str>,
}

fn record_level(record: &str) -> Option<LogLevel> {
    serde_json::from_str::<LevelOnly<'_>>(record)
        .ok()
        .and_then(|record| LogLevel::parse(&record.level))
}

// Searches `records`, numbered from `first`, newest first. A line is
// written as text and sanitized before the pattern sees it. Checks
// `cancelled` before each line.
fn search_log(
    records: &[String],
    first: u64,
    sanitize: impl Fn(&str) -> String,
    search: &LogSearch,
    cancelled: &AtomicBool,
) -> LogSearchResult {
    let mut result = LogSearchResult::default();
    let max_lines = search.max_lines.max(1);
    let end = first + records.len() as u64;
    // The number of the line after the next one to read.
    let mut next = search.before.map_or(end, |before| before.min(end));
    let mut chars = 0;
    while next > first {
        if cancelled.load(Ordering::Acquire) {
            result.cancelled = true;
            return result;
        }
        if result.lines.len() >= max_lines {
            result.next_cursor = Some(next);
            return result;
        }
        let number = next - 1;
        let record = &records[(number - first) as usize];
        if let Some(minimum) = search.min_level
            && record_level(record).is_none_or(|level| level < minimum)
        {
            result.scanned += 1;
            next = number;
            continue;
        }
        let line = sanitize(&text_line(record));
        if let Some(pattern) = &search.pattern
            && !pattern.0.is_match(&line)
        {
            result.scanned += 1;
            next = number;
            continue;
        }
        let projected = cut_chars(&line, LINE_MAX_CHARS);
        let size = projected.chars().count();
        if !result.lines.is_empty() && chars + size > search.max_chars {
            // This line starts the next page.
            result.next_cursor = Some(next);
            return result;
        }
        chars += size;
        result.scanned += 1;
        result.lines.push(projected.to_string());
        next = number;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(level: &str, message: &str) -> String {
        format!(
            "{{\"timestamp\":\"2026-11-24T10:00:00Z\",\"level\":\"{level}\",\"target\":\"weaver\",\"fields\":{{\"message\":\"{message}\"}}}}"
        )
    }

    fn search(max_lines: usize, before: Option<u64>) -> LogSearch {
        LogSearch {
            pattern: None,
            min_level: None,
            max_lines,
            max_chars: LINES_BUDGET_CHARS,
            before,
        }
    }

    fn run(records: &[String], first: u64, search: &LogSearch) -> LogSearchResult {
        search_log(
            records,
            first,
            str::to_string,
            search,
            &AtomicBool::new(false),
        )
    }

    fn records(count: usize) -> Vec<String> {
        (0..count)
            .map(|index| record("INFO", &format!("line {index}")))
            .collect()
    }

    #[test]
    fn pages_run_newest_first_and_hold_their_place_as_lines_arrive() {
        // Lines 10..15 are held; the oldest five rolled off.
        let held = records(15)[10..].to_vec();
        let first = run(&held, 10, &search(2, None));
        assert!(first.lines[0].ends_with("line 14"), "{:?}", first.lines);
        assert!(first.lines[1].ends_with("line 13"));
        assert_eq!(first.next_cursor, Some(13));

        // Two newer lines arrive and two roll off: the cursor still means
        // "older than line 13".
        let moved = records(17)[12..].to_vec();
        let second = run(&moved, 12, &search(2, first.next_cursor));
        assert_eq!(second.lines.len(), 1);
        assert!(second.lines[0].ends_with("line 12"));
        assert_eq!(second.next_cursor, None);

        // A cursor older than every held line finds nothing.
        let gone = run(&moved, 12, &search(2, Some(5)));
        assert!(gone.lines.is_empty());
        assert_eq!(gone.next_cursor, None);
    }

    #[test]
    fn a_page_exactly_full_at_the_oldest_line_has_no_cursor() {
        let held = records(2);
        let page = run(&held, 0, &search(2, None));
        assert_eq!(page.lines.len(), 2);
        assert_eq!(page.next_cursor, None);
    }

    #[test]
    fn the_level_filter_keeps_that_level_and_above() {
        let held = vec![
            record("ERROR", "broke"),
            record("DEBUG", "noise"),
            record("WARN", "careful"),
            "not a record".to_string(),
            record("INFO", "fine"),
        ];
        let mut warn = search(10, None);
        warn.min_level = Some(LogLevel::Warn);
        let found = run(&held, 0, &warn);
        assert_eq!(found.lines.len(), 2, "{:?}", found.lines);
        assert!(found.lines[0].contains("WARN") && found.lines[0].ends_with("careful"));
        assert!(found.lines[1].contains("ERROR") && found.lines[1].ends_with("broke"));
        assert_eq!(found.scanned, 5);
    }

    #[test]
    fn the_pattern_matches_the_sanitized_line_case_insensitively() {
        let held = vec![record("INFO", "secret-name done"), record("INFO", "other")];
        let mut by_pattern = search(10, None);
        by_pattern.pattern = LogPattern::new("SECRET|stand-in");
        let found = search_log(
            &held,
            0,
            |line| line.replace("secret-name", "stand-in"),
            &by_pattern,
            &AtomicBool::new(false),
        );
        assert_eq!(found.lines.len(), 1);
        assert!(found.lines[0].ends_with("stand-in done"));
    }

    #[test]
    fn a_bad_or_oversized_pattern_is_refused() {
        assert!(LogPattern::new("(unclosed").is_none());
        assert!(LogPattern::new(&"a".repeat(PATTERN_MAX_CHARS + 1)).is_none());
        assert!(LogPattern::new(&"a".repeat(PATTERN_MAX_CHARS)).is_some());
        // Small to write, too large once compiled.
        assert!(LogPattern::new(r"\w{1000}\w{1000}\w{1000}").is_none());
    }

    #[test]
    fn the_character_budget_ends_a_page_and_the_next_page_resumes_there() {
        let long = "x".repeat(LINE_MAX_CHARS + 50);
        let held = vec![record("INFO", &long); 3];
        let mut small = search(10, None);
        small.max_chars = LINE_MAX_CHARS + 10;
        let first = run(&held, 0, &small);
        assert_eq!(first.lines.len(), 1);
        assert_eq!(first.lines[0].chars().count(), LINE_MAX_CHARS);
        assert_eq!(first.next_cursor, Some(2));
        small.before = first.next_cursor;
        let second = run(&held, 0, &small);
        assert_eq!(second.lines.len(), 1);
        assert_eq!(second.next_cursor, Some(1));
    }

    #[test]
    fn a_raised_cancellation_flag_stops_the_search() {
        let held = records(5);
        let found = search_log(
            &held,
            0,
            |_| unreachable!("no line is read once cancelled"),
            &search(10, None),
            &AtomicBool::new(true),
        );
        assert!(found.cancelled);
        assert!(found.lines.is_empty());
        assert_eq!(found.scanned, 0);
    }

    #[test]
    fn cursors_round_trip_and_refuse_other_text() {
        assert_eq!(parse_cursor(&render_cursor(42)), Some(42));
        assert_eq!(parse_cursor("file:3"), None);
        assert_eq!(parse_cursor("buffer:x"), None);
    }
}
