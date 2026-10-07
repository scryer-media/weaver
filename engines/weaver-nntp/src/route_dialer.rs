use std::sync::Arc;
use weaver_tunnel::pipe::{Dialed, DialedStream, Dialer, Purpose, Target};

use crate::{NntpError, ServerConfig, route_stream::BlockingSocket};

/// One address race per direct leg. The blocking driver owns every race attempt.
pub struct AddressPlanned {
    pub plan: Arc<crate::address_plan::AddressPlan>,
    pub egress: Arc<weaver_tunnel::pipe::Egress>,
}
impl AddressPlanned {
    pub fn new(label: String, egress: Arc<weaver_tunnel::pipe::Egress>) -> Self {
        Self {
            plan: Arc::new(crate::address_plan::AddressPlan::new(label)),
            egress,
        }
    }
    pub fn snapshot(&self) -> crate::AddressPlanSnapshot {
        self.plan.snapshot()
    }
}

#[async_trait::async_trait]
impl Dialer for AddressPlanned {
    fn over_limit_cleared(&self) {
        self.plan.note_over_limit_cleared();
    }
    async fn dial(&self, target: &Target) -> Result<Dialed, weaver_tunnel::pipe::DialError> {
        use weaver_tunnel::{
            bridge::ConnectionOutcome,
            pipe::{DialError, DialPath, SetupHandle},
        };
        if !matches!(
            target.purpose,
            Purpose::Nntp { .. } | Purpose::NntpProbe { .. }
        ) {
            return self.egress.dial(target).await;
        }
        let dialer = Arc::new(
            crate::address_plan::TcpDialer::new(&target.host, target.port, self.egress.timeout)
                .with_egress(self.egress.binding.clone()),
        );
        let plan = self.plan.clone();
        let (socket, peer) = tokio::task::spawn_blocking(move || plan.connect(&dialer))
            .await
            .map_err(|e| DialError::Egress(std::io::Error::other(e)))?
            .map_err(|error| self.egress.connect_error(error))?;
        socket.set_nonblocking(true).map_err(DialError::Egress)?;
        let source = socket.local_addr().ok();
        let stream = tokio::net::TcpStream::from_std(socket).map_err(DialError::Egress)?;
        let plan = self.plan.clone();
        let delivery_plan = plan.clone();
        let outcome = Arc::new(ConnectionOutcome::default());
        let latency_plan = plan.clone();
        outcome
            .on_body_latency(move |elapsed| latency_plan.record_body_latency(peer.ip(), elapsed));
        outcome
            .on_delivery(move |bytes, wire| delivery_plan.record_delivery(peer.ip(), bytes, wire));
        Ok(Dialed {
            stream: DialedStream::Socket(stream),
            outcome,
            path: DialPath {
                egress: self.egress.id,
                ..Default::default()
            },
            peer: Some(peer),
            source,
            setup: Some(SetupHandle::new(move |reached| {
                plan.record_setup(peer, reached)
            })),
        })
    }
    async fn resolve(
        &self,
        host: &str,
    ) -> Result<weaver_tunnel::pipe::Resolution, weaver_tunnel::pipe::DialError> {
        self.egress.resolve(host).await
    }
    fn budget(&self) -> std::time::Duration {
        self.egress.budget()
    }
    fn describe(&self) -> String {
        format!("planned {}", self.egress.describe())
    }
}

/// Slack past the route budget before the outer deadline abandons a dial.
const ROUTE_BUDGET_BACKSTOP: std::time::Duration = std::time::Duration::from_secs(1);

pub struct RouteDialer {
    pub inner: Arc<dyn Dialer>,
    pub egress_controls: Arc<crate::transfer::ServerTransferRegistry>,
    pub runtime: tokio::runtime::Handle,
    pub server: u32,
}
impl std::fmt::Debug for RouteDialer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouteDialer")
            .field("server", &self.server)
            .field("path", &self.inner.describe())
            .finish()
    }
}
impl RouteDialer {
    pub async fn dial(&self, config: &ServerConfig) -> Result<Dialed, NntpError> {
        let target = Target {
            host: config.host.clone(),
            port: config.port,
            addresses: Vec::new(),
            purpose: Purpose::Nntp {
                server: self.server,
                leg: 0,
            },
        };
        // A dialer that enforces its own budget (a weighted route books a
        // timed-out leg) must see its deadline first; this one is a backstop.
        tokio::time::timeout(
            self.inner.budget() + ROUTE_BUDGET_BACKSTOP,
            self.inner.dial(&target),
        )
        .await
        .unwrap_or_else(|_| {
            Err(weaver_tunnel::pipe::DialError::Timeout {
                stage: "network route".into(),
            })
        })
        .map_err(|error| NntpError::Route(Arc::new(error)))
    }
    pub(crate) fn blocking_stream(
        &self,
        stream: DialedStream,
        config: &ServerConfig,
    ) -> Result<BlockingSocket, NntpError> {
        match stream {
            DialedStream::Socket(socket) => {
                let socket = socket.into_std()?;
                socket.set_nonblocking(false)?;
                socket.set_read_timeout(Some(config.command_timeout))?;
                socket.set_write_timeout(Some(config.command_timeout))?;
                Ok(BlockingSocket::Tcp(socket))
            }
            DialedStream::Tunnel(stream) => Ok(BlockingSocket::tunnel_boxed(
                stream,
                self.runtime.clone(),
                config.command_timeout,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use weaver_tunnel::pipe::DialError;

    #[tokio::test(start_paused = true)]
    async fn routed_acquisition_obeys_the_composed_budget() {
        struct Hung(tokio::sync::Notify);
        #[async_trait::async_trait]
        impl Dialer for Hung {
            async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
                self.0.notify_one();
                std::future::pending().await
            }
            fn budget(&self) -> Duration {
                Duration::from_secs(1)
            }
            fn describe(&self) -> String {
                "hung fixture".into()
            }
        }
        let inner = Arc::new(Hung(tokio::sync::Notify::new()));
        let route = RouteDialer {
            inner: inner.clone(),
            egress_controls: Arc::new(crate::transfer::ServerTransferRegistry::new()),
            runtime: tokio::runtime::Handle::current(),
            server: 1,
        };
        let started = inner.0.notified();
        let dial = tokio::spawn(async move { route.dial(&ServerConfig::default()).await });
        started.await;
        tokio::time::advance(Duration::from_secs(1) + ROUTE_BUDGET_BACKSTOP).await;
        assert!(
            matches!(dial.await.unwrap(), Err(NntpError::Route(error)) if matches!(error.as_ref(), DialError::Timeout { .. }))
        );
    }
}
