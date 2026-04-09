use std::collections::VecDeque;
use std::io::Write;
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;

const DEFAULT_CAPACITY: usize = 1000;
const BROADCAST_CAPACITY: usize = 256;

/// Thread-safe ring buffer that captures log lines for live viewing.
///
/// Implements `io::Write` so it can be used as a tracing subscriber layer writer.
/// Each complete line (terminated by `\n`) is stored in the ring buffer and
/// broadcast to any active subscribers.
#[derive(Clone)]
pub struct LogRingBuffer {
    inner: Arc<Mutex<RingBufferInner>>,
    tx: broadcast::Sender<String>,
}

struct RingBufferInner {
    lines: VecDeque<String>,
    capacity: usize,
    /// Accumulates partial writes (no trailing newline yet).
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

    /// Returns the last `limit` lines from the buffer.
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

    /// Subscribe to live log line broadcasts.
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
