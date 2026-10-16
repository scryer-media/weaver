use std::io::{self, Write};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc,
};
use std::time::{Duration, Instant};

const QUEUED_EVENTS: usize = 128;
const MAX_EVENT_BYTES: usize = 16 * 1024;
const SHUTDOWN_BOUND: Duration = Duration::from_secs(2);

#[derive(Default)]
struct LogCounters {
    dropped: AtomicU64,
    errors: AtomicU64,
}
type Registry = std::sync::Mutex<Vec<(&'static str, Arc<LogCounters>)>>;
fn registry() -> &'static Registry {
    static REGISTRY: std::sync::OnceLock<Registry> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(Registry::default)
}

/// Counters retain completed sinks so restarts cannot decrease process totals.
pub fn counter_snapshot() -> std::collections::BTreeMap<&'static str, (u64, u64)> {
    let mut totals = std::collections::BTreeMap::from([("file", (0, 0)), ("stderr", (0, 0))]);
    for (sink, counters) in registry().lock().unwrap().iter() {
        let total = totals.entry(sink).or_default();
        total.0 += counters.dropped.load(Ordering::Relaxed);
        total.1 += counters.errors.load(Ordering::Relaxed);
    }
    totals
}

#[derive(Clone)]
pub struct LogDelivery {
    sender: mpsc::SyncSender<Vec<u8>>,
    counters: Arc<LogCounters>,
    stopping: Arc<AtomicBool>,
}

pub struct DeliveryGuard {
    delivery: LogDelivery,
    stopping: Arc<AtomicBool>,
    finished: mpsc::Receiver<()>,
    shutdown_started: bool,
}

impl LogDelivery {
    pub fn new(
        sink: &'static str,
        mut write: impl FnMut(&[u8]) -> io::Result<()> + Send + 'static,
    ) -> io::Result<(Self, DeliveryGuard)> {
        let (sender, receiver) = mpsc::sync_channel::<Vec<u8>>(QUEUED_EVENTS);
        let stopping = Arc::new(AtomicBool::new(false));
        let stop = stopping.clone();
        let (finished_tx, finished) = mpsc::channel();
        let counters = Arc::new(LogCounters::default());
        registry().lock().unwrap().push((sink, counters.clone()));
        let worker_counters = counters.clone();
        std::thread::Builder::new()
            .name(format!("log-{sink}"))
            .spawn(move || {
                loop {
                    let event = if stop.load(Ordering::Acquire) {
                        receiver.try_recv().ok()
                    } else {
                        receiver.recv().ok()
                    };
                    let Some(event) = event else { break };
                    if !event.is_empty() && write(&event).is_err() {
                        worker_counters.errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                // Release sink resources on this thread, including file-log shutdown.
                drop(write);
                let _ = finished_tx.send(());
            })?;
        let delivery = Self {
            sender,
            counters,
            stopping: stopping.clone(),
        };
        let guard = DeliveryGuard {
            delivery: delivery.clone(),
            stopping,
            finished,
            shutdown_started: false,
        };
        Ok((delivery, guard))
    }
}

impl DeliveryGuard {
    /// Readers may still hold pipe contents after their child has exited.
    /// Their drain and the delivery drain share one shutdown budget.
    pub fn shutdown_after(mut self, producers: Vec<mpsc::Receiver<()>>) {
        let deadline = Instant::now() + SHUTDOWN_BOUND;
        self.drain_with(producers, |completion| {
            let _ = completion.recv_timeout(deadline.saturating_duration_since(Instant::now()));
        });
    }

    fn drain_with(
        &mut self,
        producers: Vec<mpsc::Receiver<()>>,
        mut wait: impl FnMut(&mpsc::Receiver<()>),
    ) {
        if self.shutdown_started {
            return;
        }
        for producer in producers {
            wait(&producer);
        }
        self.shutdown_started = true;
        self.stopping.store(true, Ordering::Release);
        // Wake an idle worker. A full queue already supplies its wake-up.
        let _ = self.delivery.sender.try_send(Vec::new());
        // A failed disk or blocked stdout must not prevent process shutdown.
        wait(&self.finished);
    }
}

impl Drop for DeliveryGuard {
    fn drop(&mut self) {
        let deadline = Instant::now() + SHUTDOWN_BOUND;
        self.drain_with(Vec::new(), |completion| {
            let _ = completion.recv_timeout(deadline.saturating_duration_since(Instant::now()));
        });
    }
}

pub struct EventWriter<'a> {
    delivery: &'a LogDelivery,
    bytes: Vec<u8>,
    oversized: bool,
}

impl LogDelivery {
    pub fn make_writer(&self) -> EventWriter<'_> {
        EventWriter {
            delivery: self,
            bytes: Vec::new(),
            oversized: false,
        }
    }
}

impl Write for EventWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.bytes.len().saturating_add(bytes.len()) > MAX_EVENT_BYTES {
            self.oversized = true;
            self.bytes.clear();
        } else if !self.oversized {
            self.bytes.extend_from_slice(bytes);
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for EventWriter<'_> {
    fn drop(&mut self) {
        // Enqueue complete events: never interleave fragments or emit truncated JSON.
        if self.oversized
            || self.delivery.stopping.load(Ordering::Acquire)
            || self
                .delivery
                .sender
                .try_send(std::mem::take(&mut self.bytes))
                .is_err()
        {
            self.delivery
                .counters
                .dropped
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown_waits_for_pipe_producers_before_stopping_delivery() {
        let (sent, received) = mpsc::channel();
        let (writer, mut guard) = LogDelivery::new("shutdown-fixture", move |bytes| {
            sent.send(bytes.to_vec()).unwrap();
            Ok(())
        })
        .unwrap();
        let (producer_finished, producer) = mpsc::channel();
        let (waiting, ready) = mpsc::channel();
        let shutdown = std::thread::spawn(move || {
            let mut first = true;
            guard.drain_with(vec![producer], |completion| {
                if first {
                    first = false;
                    waiting.send(()).unwrap();
                }
                completion.recv().unwrap();
            });
        });
        ready.recv().unwrap();
        // The child has exited; its reader is only now forwarding the pipe tail.
        assert!(!writer.stopping.load(Ordering::Acquire));
        writer.make_writer().write_all(b"final stderr\n").unwrap();
        producer_finished.send(()).unwrap();
        shutdown.join().unwrap();
        assert_eq!(received.recv().unwrap(), b"final stderr\n");
        assert!(received.recv().is_err());
        assert_eq!(writer.counters.dropped.load(Ordering::Relaxed), 0);
    }

    fn drain(guard: DeliveryGuard) {
        guard.stopping.store(true, Ordering::Release);
        let _ = guard.delivery.sender.try_send(Vec::new());
        guard.finished.recv().unwrap();
    }

    #[test]
    fn full_queue_drops_whole_events_without_blocking_runtime_tasks() {
        {
            let (entered, entering) = mpsc::channel();
            let (release, released) = mpsc::channel();
            let (written, writes) = mpsc::channel();
            let mut first = true;
            let (writer, guard) = LogDelivery::new("fixture", move |bytes| {
                if first {
                    first = false;
                    entered.send(()).unwrap();
                    released.recv().unwrap();
                }
                written.send(bytes.to_vec()).unwrap();
                Ok(())
            })
            .unwrap();
            writer.make_writer().write_all(b"first").unwrap();
            entering.recv().unwrap();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            runtime.block_on(async {
                for _ in 0..QUEUED_EVENTS {
                    writer.make_writer().write_all(b"queued").unwrap();
                }
                writer.make_writer().write_all(b"dropped").unwrap();
            });
            assert_eq!(writer.counters.dropped.load(Ordering::SeqCst), 1);
            release.send(()).unwrap();
            drain(guard);
            assert_eq!(writes.recv().unwrap(), b"first");
            assert_eq!(writes.into_iter().count(), QUEUED_EVENTS);
        }
    }

    #[test]
    fn oversized_events_are_omitted_and_sink_errors_do_not_stop_delivery() {
        {
            let (written, writes) = mpsc::channel();
            let (writer, guard) = LogDelivery::new("fixture", move |bytes| {
                if bytes == b"fail" {
                    return Err(io::Error::other("fixture failure"));
                }
                written.send(bytes.to_vec()).unwrap();
                Ok(())
            })
            .unwrap();
            writer
                .make_writer()
                .write_all(&vec![b'x'; MAX_EVENT_BYTES + 1])
                .unwrap();
            writer.make_writer().write_all(b"fail").unwrap();
            {
                let mut event = writer.make_writer();
                event.write_all(b"one").unwrap();
                event.write_all(b"\ntwo\n").unwrap();
            }
            drain(guard);
            assert_eq!(writes.recv().unwrap(), b"one\ntwo\n");
            assert!(writes.recv().is_err());
            assert_eq!(writer.counters.dropped.load(Ordering::SeqCst), 1);
            assert_eq!(writer.counters.errors.load(Ordering::SeqCst), 1);
            writer.make_writer().write_all(b"after shutdown").unwrap();
            assert_eq!(writer.counters.dropped.load(Ordering::SeqCst), 2);
        }
    }

    #[test]
    fn shutdown_does_not_join_a_stalled_sink() {
        let (entered, entering) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let (finished, finish) = mpsc::channel();
        let (writer, guard) = LogDelivery::new("fixture", move |_| {
            entered.send(()).unwrap();
            released.recv().unwrap();
            finished.send(()).unwrap();
            Ok(())
        })
        .unwrap();
        writer.make_writer().write_all(b"held").unwrap();
        entering.recv().unwrap();
        drop(guard); // The production shutdown bound must return while the sink is held.
        release.send(()).unwrap();
        finish.recv().unwrap();
    }
}
