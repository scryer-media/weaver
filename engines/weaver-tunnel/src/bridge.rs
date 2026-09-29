//! An explicitly owned, revocable loopback bridge for socket-based consumers.
use crate::{NoopTunnelObserver, TunnelError, TunnelProvider, socks5::Socks5Front};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

type ReadCallback = Arc<dyn Fn(usize) + Send + Sync>;

/// Protocol-aware feedback for one connection, independent of other streams.
#[derive(Default)]
pub struct ConnectionOutcome {
    callbacks: Mutex<OutcomeCallbacks>,
    reads: OnceLock<Arc<[ReadCallback]>>,
}
#[derive(Default)]
struct OutcomeCallbacks {
    failed: bool,
    closed: bool,
    retiring: bool,
    failure: Vec<Box<dyn Fn() + Send + Sync>>,
    close: Vec<Box<dyn Fn() + Send + Sync>>,
    retire: Vec<Box<dyn Fn() + Send + Sync>>,
    reads: Vec<ReadCallback>,
    deliveries: Vec<Arc<dyn Fn(u64, Duration) + Send + Sync>>,
    latencies: Vec<Arc<dyn Fn(Duration) + Send + Sync>>,
}
impl std::fmt::Debug for ConnectionOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionOutcome").finish_non_exhaustive()
    }
}
impl ConnectionOutcome {
    /// Recall an idle socket now, or a busy socket at its next article boundary.
    pub fn on_retire(&self, callback: impl Fn() + Send + Sync + 'static) {
        let mut callbacks = self.callbacks.lock().expect("connection outcome");
        if callbacks.retiring {
            drop(callbacks);
            callback();
        } else if !callbacks.closed {
            callbacks.retire.push(Box::new(callback));
        }
    }
    pub fn retire(&self) {
        let callbacks = {
            let mut callbacks = self.callbacks.lock().expect("connection outcome");
            if callbacks.retiring || callbacks.closed {
                return;
            }
            callbacks.retiring = true;
            std::mem::take(&mut callbacks.retire)
        };
        for callback in callbacks {
            callback();
        }
    }
    pub fn on_body_latency(&self, callback: impl Fn(Duration) + Send + Sync + 'static) {
        self.callbacks
            .lock()
            .expect("connection outcome")
            .latencies
            .push(Arc::new(callback));
    }
    pub fn body_latency(&self, elapsed: Duration) {
        let callbacks = self
            .callbacks
            .lock()
            .expect("connection outcome")
            .latencies
            .clone();
        for callback in callbacks {
            callback(elapsed);
        }
    }

    /// Register during connection setup, before the first read seals the callbacks.
    pub fn on_read(&self, callback: impl Fn(usize) + Send + Sync + 'static) {
        debug_assert!(
            self.reads.get().is_none(),
            "read callbacks register before I/O"
        );
        self.callbacks
            .lock()
            .expect("connection outcome")
            .reads
            .push(Arc::new(callback));
    }
    pub fn read(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let callbacks = self.reads.get_or_init(|| {
            std::mem::take(&mut self.callbacks.lock().expect("connection outcome").reads).into()
        });
        for callback in callbacks.iter() {
            callback(bytes);
        }
    }
    pub fn on_delivery(&self, callback: impl Fn(u64, Duration) + Send + Sync + 'static) {
        self.callbacks
            .lock()
            .expect("connection outcome")
            .deliveries
            .push(Arc::new(callback));
    }
    pub fn delivered(&self, bytes: u64, wire: Duration) {
        let callbacks = self
            .callbacks
            .lock()
            .expect("connection outcome")
            .deliveries
            .clone();
        for callback in callbacks {
            callback(bytes, wire);
        }
    }
    pub fn on_failure(&self, callback: impl Fn() + Send + Sync + 'static) {
        let mut callbacks = self.callbacks.lock().expect("connection outcome");
        if callbacks.failed {
            drop(callbacks);
            callback();
        } else if !callbacks.closed {
            callbacks.failure.push(Box::new(callback));
        }
    }
    pub fn on_close(&self, callback: impl Fn() + Send + Sync + 'static) {
        let mut callbacks = self.callbacks.lock().expect("connection outcome");
        if callbacks.closed {
            drop(callbacks);
            callback();
        } else {
            callbacks.close.push(Box::new(callback));
        }
    }
    pub fn failed(&self) {
        let callbacks = {
            let mut callbacks = self.callbacks.lock().expect("connection outcome");
            if callbacks.failed || callbacks.closed {
                return;
            }
            callbacks.failed = true;
            std::mem::take(&mut callbacks.failure)
        };
        for callback in callbacks {
            callback();
        }
    }
    /// The socket owner reports closure independently of failure evidence.
    pub fn closed(&self) {
        let callbacks = {
            let mut callbacks = self.callbacks.lock().expect("connection outcome");
            if callbacks.closed {
                return;
            }
            callbacks.closed = true;
            callbacks.failure.clear();
            std::mem::take(&mut callbacks.close)
        };
        for callback in callbacks {
            callback();
        }
    }
}
impl Drop for ConnectionOutcome {
    fn drop(&mut self) {
        self.closed();
    }
}
pub(crate) type Outcomes = Arc<Mutex<HashMap<SocketAddr, Weak<ConnectionOutcome>>>>;

pub struct Bridge {
    pub connect_timeout: Duration,
    addr: SocketAddr,
    credentials: (String, String),
    outcomes: Outcomes,
    revoked: AtomicBool,
    shutdown: Arc<tokio::sync::Notify>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    provider: Arc<dyn TunnelProvider>,
    handle: tokio::runtime::Handle,
    streams: Arc<crate::direct::Streams>,
    direct_slots: Arc<tokio::sync::Semaphore>,
}

impl std::fmt::Debug for Bridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bridge")
            .field("revoked", &self.revoked.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl Bridge {
    pub fn start(
        handle: &tokio::runtime::Handle,
        provider: Arc<dyn TunnelProvider>,
        timeout: Duration,
        credentials: (String, String),
    ) -> Result<Arc<Self>, TunnelError> {
        if credentials.0.is_empty()
            || credentials.1.is_empty()
            || credentials.0.len() > 255
            || credentials.1.len() > 255
        {
            return Err(TunnelError::Engine("invalid bridge credentials".into()));
        }
        let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .map_err(|_| TunnelError::Engine("could not bind proxy bridge".into()))?;
        listener
            .set_nonblocking(true)
            .map_err(|_| TunnelError::Engine("could not configure proxy bridge".into()))?;
        let addr = listener
            .local_addr()
            .map_err(|_| TunnelError::Engine("could not read proxy bridge address".into()))?;
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let stopped = shutdown.clone();
        let outcomes = Outcomes::default();
        let mut front = Socks5Front::authenticated(
            provider.clone(),
            Arc::new(NoopTunnelObserver),
            "route".into(),
            timeout,
            credentials.clone(),
        );
        front.outcomes = outcomes.clone();
        let direct_slots = front.slots.clone();
        let front = Arc::new(front);
        let task = handle.spawn(async move {
            if let Ok(listener) = tokio::net::TcpListener::from_std(listener) {
                front.serve(listener, stopped).await;
            }
        });
        Ok(Arc::new(Self {
            connect_timeout: timeout,
            addr,
            credentials,
            outcomes,
            revoked: AtomicBool::new(false),
            shutdown,
            task: Mutex::new(Some(task)),
            provider,
            handle: handle.clone(),
            streams: Arc::default(),
            direct_slots,
        }))
    }

    /// Dial on the owning runtime and hand the stream directly to an in-process
    /// consumer. Dropping this future cancels establishment; no relay is used.
    pub async fn dial(
        &self,
        host: &str,
        port: u16,
    ) -> std::io::Result<(crate::direct::DirectStream, Arc<ConnectionOutcome>)> {
        self.addr()?;
        let permit = self
            .direct_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| std::io::Error::other("too many direct tunnel connections"))?;
        let provider = self.provider.clone();
        let streams = self.streams.clone();
        let host = host.to_owned();
        let timeout = self.connect_timeout;
        let pending = streams.begin_dial()?;
        let mut task = self.handle.spawn(async move {
            let _pending = pending;
            let slot = permit;
            let outcome = Arc::new(ConnectionOutcome::default());
            let stream = tokio::select! {
                biased;
                () = streams.cancelled() => return Err(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "proxy route was revoked")),
                result = tokio::time::timeout(timeout, provider.dial_observed(&host, port, outcome.clone())) => {
                    result.map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "proxy connection timed out"))?
                        .map_err(std::io::Error::other)?
                }
            };
            Ok((streams.track(stream, slot)?, outcome))
        });
        struct Abort(tokio::task::AbortHandle);
        impl Drop for Abort {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _abort = Abort(task.abort_handle());
        (&mut task).await.map_err(std::io::Error::other)?
    }

    pub fn runtime(&self) -> &tokio::runtime::Handle {
        &self.handle
    }
    pub fn addr(&self) -> Result<SocketAddr, std::io::Error> {
        if self.revoked.load(Ordering::Acquire) {
            return Err(std::io::Error::other("proxy route was revoked"));
        }
        Ok(self.addr)
    }
    pub fn credentials(&self) -> (&str, &str) {
        (&self.credentials.0, &self.credentials.1)
    }
    pub fn register_connection(
        &self,
        local_addr: SocketAddr,
    ) -> Result<Arc<ConnectionOutcome>, std::io::Error> {
        self.addr()?;
        let outcome = Arc::new(ConnectionOutcome::default());
        let mut outcomes = self.outcomes.lock().expect("bridge outcomes");
        outcomes.retain(|_, value| value.strong_count() > 0);
        if outcomes.len() >= 1024 {
            return Err(std::io::Error::other("too many bridge connections"));
        }
        outcomes.insert(local_addr, Arc::downgrade(&outcome));
        Ok(outcome)
    }
    pub async fn revoke(&self) {
        self.revoked.store(true, Ordering::Release);
        self.streams.revoke();
        self.direct_slots.close();
        self.shutdown.notify_one();
        let task = self.task.lock().expect("bridge task").take();
        if let Some(task) = task {
            let _ = task.await;
        }
        self.streams.wait_drained().await;
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        self.streams.revoke();
        self.shutdown.notify_one();
        if let Some(task) = self.task.get_mut().expect("bridge task").take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests;
