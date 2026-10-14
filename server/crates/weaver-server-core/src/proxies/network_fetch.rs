use super::*;
use std::net::IpAddr;
use weaver_tunnel::{
    TunnelError, TunnelStream,
    bridge::ConnectionOutcome,
    pipe::{Purpose, Resolution},
};

// A fetch attempt owns one concrete path for both DNS validation and HTTP.
pub struct FetchAttempt {
    route: Arc<LiveNetworkRoute>,
    guard: Arc<Revocable>,
    stage: Arc<dyn Dialer>,
    position: usize,
    purpose: Purpose,
    pacing: Option<Arc<weaver_nntp::transfer::ServerTransferControl>>,
    legacy_status: Arc<ConsumerRoute>,
    selected_proxy: Option<u32>,
    ladder: Option<Arc<Fallback>>,
    rung: usize,
    transport_error: Mutex<Option<Arc<DialError>>>,
    setups: Mutex<Vec<weaver_tunnel::pipe::SetupHandle>>,
    last_member: bool,
    // The leg's final attempt this sync: no later rung, and no direct
    // fallback, remains after it.
    last_attempt: bool,
    // The addresses the fetch checked for each host, so a dial uses exactly
    // those instead of resolving again.
    addresses: Mutex<HashMap<String, Vec<IpAddr>>>,
}
impl FetchAttempt {
    pub async fn cancelled(&self) {
        self.guard.cancelled().await;
    }
    pub fn take_transport_error(&self) -> Option<Arc<DialError>> {
        self.transport_error
            .lock()
            .expect("fetch transport error")
            .take()
    }
    pub fn report(&self, error: Option<&DialError>) {
        if self.guard.is_revoked() {
            return;
        }
        for setup in std::mem::take(&mut *self.setups.lock().expect("fetch setup")) {
            setup.complete(error.is_none());
        }
        self.stage.report_request(error);
        // One failed member is not a failed pool rung while another can be tried.
        if error.is_some() && !self.last_member {
            return;
        }
        if let Some(ladder) = &self.ladder {
            ladder.report_rung(self.rung, error);
        }
        match (error, self.selected_proxy) {
            (Some(error), Some(id)) if error.is_path_evidence() => {
                self.legacy_status.fail_dial(id, error)
            }
            (None, id) => self.legacy_status.success(id),
            _ => {}
        }
        // A failed rung is not a failed leg while a later rung, or the direct
        // fallback, is still to be tried this sync. A connection's dialer
        // reports one outcome to its leg after every rung has had its turn;
        // a fetch's attempts report the same way, or two cooling rungs would
        // take the leg down before the fallback behind them ever ran.
        if error.is_some() && !self.last_attempt {
            return;
        }
        self.route.weighted.report_external(
            self.position,
            &(self.guard.clone() as Arc<dyn Dialer>),
            error,
        );
    }
}
#[async_trait::async_trait]
impl FetchPath for FetchAttempt {
    // The addresses `host` is reached at, as the attempt's exit resolves
    // it, or `Unresolved` when the exit proxy is handed the name.
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        if self.guard.is_revoked() {
            return Err(DialError::Skipped("leg revoked".into()));
        }
        self.stage.resolve(host).await
    }
    // Pins the checked addresses a direct dial of `host` uses.
    fn pin(&self, host: &str, addresses: Vec<IpAddr>) {
        self.addresses
            .lock()
            .expect("fetch addresses")
            .insert(host.to_owned(), addresses);
    }
    fn budget(&self) -> Duration {
        self.stage.budget()
    }
    fn body_failed(&self, error: &DialError) {
        self.report(Some(error));
    }
}
#[async_trait::async_trait]
impl TunnelProvider for FetchAttempt {
    async fn dial(&self, host: &str, port: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
        self.dial_observed(host, port, Arc::default()).await
    }
    async fn dial_observed(
        &self,
        host: &str,
        port: u16,
        observed: Arc<ConnectionOutcome>,
    ) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let host = host.trim_matches(['[', ']']);
        let addresses = self
            .addresses
            .lock()
            .expect("fetch addresses")
            .get(host)
            .cloned()
            .unwrap_or_default();
        let target = Target {
            host: host.into(),
            port,
            purpose: self.purpose.clone(),
            addresses,
        };
        let mut dialed = self
            .guard
            .dial_using(self.stage.as_ref(), &target)
            .await
            .map_err(|error| {
                let message = error.to_string();
                *self.transport_error.lock().expect("fetch transport error") =
                    Some(Arc::new(error));
                TunnelError::Engine(message)
            })?;
        dialed.path.rung = self.selected_proxy.map(|_| self.rung);
        self.route
            .weighted
            .track_external(
                self.position,
                &(self.guard.clone() as Arc<dyn Dialer>),
                &mut dialed,
            )
            .map_err(|error| TunnelError::Engine(error.to_string()))?;
        let outcome = dialed.outcome.clone();
        observed.on_failure(move || outcome.failed());
        let outcome = dialed.outcome.clone();
        observed.on_close(move || outcome.closed());
        if let Some(setup) = dialed.setup.take() {
            self.setups.lock().expect("fetch setup").push(setup);
        }
        // Keep connection accounting alive until the bridge releases its stream.
        Ok(Box::new(FetchStream {
            inner: weaver_tunnel::direct::DirectStream::from_stream(dialed.stream.into_tunnel()),
            outcome: dialed.outcome,
            pacing: self.pacing.clone(),
            waiting: None,
            pending: Vec::new(),
            offset: 0,
        }))
    }
    fn describe(&self) -> String {
        self.stage.describe()
    }
}
struct FetchStream {
    inner: weaver_tunnel::direct::DirectStream,
    outcome: Arc<ConnectionOutcome>,
    pacing: Option<Arc<weaver_nntp::transfer::ServerTransferControl>>,
    waiting: Option<std::pin::Pin<Box<dyn std::future::Future<Output = Duration> + Send>>>,
    pending: Vec<u8>,
    offset: usize,
}
impl Drop for FetchStream {
    fn drop(&mut self) {
        self.outcome.closed();
    }
}
impl tokio::io::AsyncRead for FetchStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::task::Poll;
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.pacing.is_none() {
            let before = buf.filled().len();
            let result = std::pin::Pin::new(&mut self.inner).poll_read(cx, buf);
            self.outcome.read(buf.filled().len() - before);
            return result;
        }
        loop {
            if let Some(waiting) = &mut self.waiting {
                if waiting.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                self.waiting = None;
            }
            if self.offset < self.pending.len() {
                let count = buf.remaining().min(self.pending.len() - self.offset);
                buf.put_slice(&self.pending[self.offset..self.offset + count]);
                self.offset += count;
                if self.offset == self.pending.len() {
                    self.pending.clear();
                    self.offset = 0;
                }
                return Poll::Ready(Ok(()));
            }
            let this = self.as_mut().get_mut();
            this.pending.resize(8192, 0);
            let mut read = tokio::io::ReadBuf::new(&mut this.pending);
            let result = std::pin::Pin::new(&mut this.inner).poll_read(cx, &mut read);
            let count = read.filled().len();
            this.pending.truncate(count);
            match result {
                Poll::Ready(Ok(())) if count > 0 => {
                    self.outcome.read(count);
                    if let Some(pacing) = self.pacing.clone() {
                        self.waiting =
                            Some(Box::pin(async move { pacing.pace_read_async(count).await }));
                    }
                }
                result => return result,
            }
        }
    }
}
impl tokio::io::AsyncWrite for FetchStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
impl NetworkRuntime {
    // The paths one HTTP fetch for `consumer` may take, in the order it
    // tries them.
    pub fn fetch_attempts(
        &self,
        consumer: Consumer,
        legacy_status: Arc<ConsumerRoute>,
    ) -> Result<Vec<Arc<FetchAttempt>>, String> {
        let purpose = match consumer {
            Consumer::Rss(feed) => Purpose::Feed { feed },
            Consumer::Outbound => Purpose::Outbound,
            Consumer::Server(_) => return Err("a server's route does not carry fetches".into()),
        };
        let route = self.route(consumer, 1, Duration::from_secs(30))?;
        let config = self.configuration.read().expect("network configuration");
        let legs = route.legs.read().expect("route legs");
        let health = route.weighted.allocations();
        let mut attempts = Vec::new();
        for (position, leg) in legs.iter().enumerate() {
            if !matches!(
                health[position].health,
                LegHealthState::Up | LegHealthState::Probing
            ) {
                continue;
            }
            let bottom = Self::bottom(&config, leg.definition.egress_id, Duration::from_secs(30))?;
            let (rungs, direct) = match &leg.definition.path {
                LegPath::Direct => (Vec::new(), true),
                LegPath::Ladder {
                    rungs,
                    direct_fallback,
                } => (rungs.clone(), *direct_fallback),
            };
            let direct_rung = rungs.len();
            let probing = matches!(health[position].health, LegHealthState::Probing);
            let mut leg_attempts = Vec::new();
            for (rung_index, rung) in rungs.into_iter().enumerate() {
                if leg
                    .ladder
                    .as_ref()
                    .is_some_and(|ladder| !ladder.allows_rung(rung_index, probing))
                {
                    continue;
                }
                let stages = match rung {
                    Rung::Proxy { id } => vec![(
                        self.hop(&config, id, bottom.clone(), bottom.clone(), &[])?,
                        id,
                    )],
                    Rung::Chain { ids } => {
                        let mut stage: Arc<dyn Dialer> = bottom.clone();
                        for (i, id) in ids.iter().enumerate() {
                            stage = self.hop(&config, *id, stage, bottom.clone(), &ids[..i])?;
                        }
                        vec![(stage, *ids.last().ok_or("empty chain")?)]
                    }
                    Rung::Pool { id } => {
                        if config.pools.get(&id).is_none_or(|pool| !pool.enabled) {
                            continue;
                        }
                        self.pool(&config, id, bottom.clone())?;
                        let pools = self.pools.lock().expect("network pools");
                        let Some(pool) = pools.get(&(id, bottom.id())) else {
                            continue;
                        };
                        pool.lease_members()
                            .into_iter()
                            .map(|(member, stage)| (stage, member))
                            .collect()
                    }
                };
                let count = stages.len();
                for (member_index, (stage, last)) in stages.into_iter().enumerate() {
                    leg_attempts.push(FetchAttempt {
                        route: route.clone(),
                        guard: leg.stage.clone(),
                        stage,
                        position,
                        purpose: purpose.clone(),
                        pacing: self
                            .egress_controls
                            .get(weaver_nntp::transfer::StableServerId(
                                leg.definition.egress_id,
                            )),
                        legacy_status: legacy_status.clone(),
                        selected_proxy: Some(last),
                        ladder: leg.ladder.clone(),
                        rung: rung_index,
                        transport_error: Mutex::new(None),
                        setups: Mutex::new(Vec::new()),
                        last_member: member_index + 1 == count,
                        last_attempt: false,
                        addresses: Mutex::default(),
                    });
                }
            }
            if direct
                && leg
                    .ladder
                    .as_ref()
                    .is_none_or(|ladder| ladder.allows_rung(direct_rung, probing))
            {
                leg_attempts.push(FetchAttempt {
                    route: route.clone(),
                    guard: leg.stage.clone(),
                    stage: bottom,
                    position,
                    purpose: purpose.clone(),
                    pacing: self
                        .egress_controls
                        .get(weaver_nntp::transfer::StableServerId(
                            leg.definition.egress_id,
                        )),
                    legacy_status: legacy_status.clone(),
                    selected_proxy: None,
                    ladder: leg.ladder.clone(),
                    rung: direct_rung,
                    transport_error: Mutex::new(None),
                    setups: Mutex::new(Vec::new()),
                    last_member: true,
                    last_attempt: false,
                    addresses: Mutex::default(),
                });
            }
            if let Some(last) = leg_attempts.last_mut() {
                last.last_attempt = true;
            }
            attempts.extend(leg_attempts.into_iter().map(Arc::new));
        }
        Ok(attempts)
    }
}
