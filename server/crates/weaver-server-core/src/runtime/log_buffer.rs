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

// Thread-safe ring buffer that captures log lines for live viewing.
//
// Implements `io::Write` so it can be used as a tracing subscriber layer writer.
// Each complete line (terminated by `\n`) is stored in the ring buffer and
// broadcast to any active subscribers.
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
