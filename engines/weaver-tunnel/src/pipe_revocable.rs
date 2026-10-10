use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use super::{ConnectionOutcome, DialError, Dialed, DialedStream, Dialer, Resolution, Target};

/// One leg's sockets and streams. Revocation also cancels in-flight opens.
pub struct Revocable {
    inner: Arc<dyn Dialer>,
    sockets: crate::revocation::SocketRegistry,
    streams: Arc<crate::direct::Streams>,
    slots: Arc<tokio::sync::Semaphore>,
    outcomes: Mutex<Vec<Weak<ConnectionOutcome>>>,
}

impl Revocable {
    pub fn new(inner: Arc<dyn Dialer>) -> Self {
        Self {
            inner,
            sockets: Default::default(),
            streams: Default::default(),
            slots: Arc::new(tokio::sync::Semaphore::new(16384)),
            outcomes: Mutex::new(Vec::new()),
        }
    }
    pub async fn cancelled(&self) {
        self.streams.cancelled().await;
    }
    pub fn is_revoked(&self) -> bool {
        self.sockets.check().is_err()
    }
    pub async fn dial_using(
        &self,
        inner: &dyn Dialer,
        target: &Target,
    ) -> Result<Dialed, DialError> {
        let _pending = self
            .streams
            .begin_dial()
            .map_err(|e| DialError::Skipped(e.to_string()))?;
        let mut dialed = tokio::select! {
            biased;
            _ = self.streams.cancelled() => return Err(DialError::Skipped("leg revoked".into())),
            result = inner.dial(target) => result?,
        };
        dialed.stream = match dialed.stream {
            DialedStream::Socket(socket) => {
                let owned = self
                    .sockets
                    .track(socket2::SockRef::from(&socket))
                    .map_err(|e| DialError::Skipped(e.to_string()))?;
                dialed.outcome.on_close(move || {
                    let _ = &owned;
                });
                DialedStream::Socket(socket)
            }
            DialedStream::Tunnel(stream) => {
                let permit = self
                    .slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| DialError::Skipped("leg stream limit reached".into()))?;
                let stream = self
                    .streams
                    .track(stream, permit)
                    .map_err(|e| DialError::Skipped(e.to_string()))?;
                DialedStream::Tunnel(Box::new(stream))
            }
        };
        // Serialize insertion with revoke's drain, and check the permanent gate again.
        let mut outcomes = self.outcomes.lock().expect("leg outcomes");
        self.sockets
            .check()
            .map_err(|e| DialError::Skipped(e.to_string()))?;
        outcomes.retain(|o| o.strong_count() > 0);
        outcomes.push(Arc::downgrade(&dialed.outcome));
        Ok(dialed)
    }
    pub fn revoke(&self) {
        if !self.is_revoked() {
            crate::metrics::record_revocation();
        }
        self.sockets.revoke();
        self.streams.revoke();
        self.slots.close();
        let outcomes = std::mem::take(&mut *self.outcomes.lock().expect("leg outcomes"));
        for outcome in outcomes.into_iter().filter_map(|o| o.upgrade()) {
            outcome.closed();
        }
    }
}
impl Drop for Revocable {
    fn drop(&mut self) {
        self.revoke();
    }
}

#[async_trait::async_trait]
impl Dialer for Revocable {
    fn over_limit_cleared(&self) {
        self.inner.over_limit_cleared();
    }
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        self.dial_using(self.inner.as_ref(), target).await
    }
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        self.sockets
            .check()
            .map_err(|e| DialError::Skipped(e.to_string()))?;
        self.inner.resolve(host).await
    }
    fn budget(&self) -> Duration {
        self.inner.budget()
    }
    async fn shutdown(&self) {
        self.revoke();
        self.streams.wait_drained().await;
        self.inner.shutdown().await;
    }
    fn describe(&self) -> String {
        self.inner.describe()
    }
}
