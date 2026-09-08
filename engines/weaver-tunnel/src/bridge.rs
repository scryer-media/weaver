//! An explicitly owned, revocable loopback bridge for socket-based consumers.
use crate::{NoopTunnelObserver, TunnelError, TunnelProvider, socks5::Socks5Front};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

/// Protocol-aware feedback for one connection, independent of other streams.
#[derive(Default)]
pub struct ConnectionOutcome {
    failure: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}
impl ConnectionOutcome {
    pub fn on_failure(&self, callback: impl Fn() + Send + Sync + 'static) {
        *self.failure.lock().expect("connection outcome") = Some(Box::new(callback));
    }
    pub fn failed(&self) {
        if let Some(callback) = self.failure.lock().expect("connection outcome").take() {
            callback();
        }
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
            provider,
            Arc::new(NoopTunnelObserver),
            "route".into(),
            timeout,
            credentials.clone(),
        );
        front.outcomes = outcomes.clone();
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
        }))
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
        self.shutdown.notify_one();
        let task = self.task.lock().expect("bridge task").take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        self.shutdown.notify_one();
        if let Some(task) = self.task.get_mut().expect("bridge task").take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests;
