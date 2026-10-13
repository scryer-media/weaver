use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use weaver_tunnel::bridge::ConnectionOutcome;
use weaver_tunnel::pipe::{
    DialError, DialPath, Dialed, Dialer, Purpose, Resolution, Target, Transports,
};
use weaver_tunnel::{TunnelError, TunnelProvider, TunnelStream};

use super::model::{
    ConnectionResult, LegResult, Sample, SpeedTestDialPath, SpeedTestResult, UsenetResult,
};

// One connection a run opened, or failed to open.
pub(crate) struct ConnectionRecord {
    leg: usize,
    connect_ms: u64,
    path: SpeedTestDialPath,
    source: Option<SocketAddr>,
    bytes: AtomicU64,
    error: Mutex<Option<String>>,
}

impl ConnectionRecord {
    pub(crate) fn add_bytes(&self, bytes: u64) {
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    // The first error ends the connection; later ones are its consequence.
    pub(crate) fn fail(&self, error: impl Into<String>) {
        let mut slot = self.error.lock().expect("connection error");
        if slot.is_none() {
            *slot = Some(error.into());
        }
    }
    fn has_error(&self) -> bool {
        self.error.lock().expect("connection error").is_some()
    }
    fn result(&self) -> ConnectionResult {
        ConnectionResult {
            leg: self.leg,
            connect_ms: self.connect_ms,
            path: self.path.clone(),
            source_address: self.source.map(|source| source.to_string()),
            bytes: self.bytes.load(Ordering::Relaxed),
            error: self.error.lock().expect("connection error").clone(),
        }
    }
}

#[derive(Default)]
struct LegCounters {
    bytes: AtomicU64,
    sampled: AtomicU64,
}

#[derive(Default)]
pub(crate) struct UsenetCounters {
    pub requested: AtomicU64,
    pub delivered: AtomicU64,
    pub missing: AtomicU64,
    pub failed: AtomicU64,
    pub declared: AtomicU64,
    pub decoded: AtomicU64,
}

// Facts about the Usenet connection that do not change during a run.
#[derive(Debug, Clone, Default)]
pub(crate) struct UsenetFacts {
    pub pipelining: bool,
    pub tls: bool,
    pub tls_cipher_suite: Option<String>,
}

// The live counters of one run. The resolvers read a partial result off
// them while the run measures.
pub(crate) struct Progress {
    started: OnceLock<Instant>,
    finished: OnceLock<Instant>,
    legs: Vec<LegCounters>,
    connections: Mutex<Vec<Arc<ConnectionRecord>>>,
    samples: Mutex<(Duration, Vec<Sample>)>,
    gauge_peak: AtomicU64,
    pub usenet: UsenetCounters,
    usenet_facts: OnceLock<UsenetFacts>,
    // Told when an article or a stream is counted, so a reader can wait on
    // the counts instead of on time.
    notify: OnceLock<tokio::sync::watch::Sender<u64>>,
}

impl Progress {
    pub(crate) fn new(legs: usize) -> Arc<Self> {
        Arc::new(Self {
            started: OnceLock::new(),
            finished: OnceLock::new(),
            legs: (0..legs).map(|_| LegCounters::default()).collect(),
            connections: Mutex::new(Vec::new()),
            samples: Mutex::new((Duration::ZERO, Vec::new())),
            gauge_peak: AtomicU64::new(0),
            usenet: UsenetCounters::default(),
            usenet_facts: OnceLock::new(),
            notify: OnceLock::new(),
        })
    }

    pub(crate) fn notify_with(&self, notify: tokio::sync::watch::Sender<u64>) {
        let _ = self.notify.set(notify);
    }

    // An article or a stream was counted.
    pub(crate) fn counted(&self) {
        if let Some(notify) = self.notify.get() {
            notify.send_modify(|revision| *revision = revision.wrapping_add(1));
        }
    }

    // The clock starts when the run passes the gate.
    pub(crate) fn start(&self) {
        let _ = self.started.set(Instant::now());
    }

    pub(crate) fn finish(&self) {
        if self.started.get().is_some() {
            let _ = self.finished.set(Instant::now());
        }
    }

    pub(crate) fn measuring(&self) -> bool {
        self.started.get().is_some()
    }

    pub(crate) fn set_usenet_facts(&self, facts: UsenetFacts) {
        let _ = self.usenet_facts.set(facts);
    }

    pub(crate) fn add_leg_bytes(&self, leg: usize, bytes: u64) {
        if let Some(counters) = self.legs.get(leg) {
            counters.bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    pub(crate) fn note_gauge(&self, bytes_per_second: u64) {
        self.gauge_peak
            .fetch_max(bytes_per_second, Ordering::Relaxed);
    }

    fn elapsed(&self) -> Duration {
        match (self.started.get(), self.finished.get()) {
            (Some(started), Some(finished)) => finished.saturating_duration_since(*started),
            (Some(started), None) => started.elapsed(),
            _ => Duration::ZERO,
        }
    }

    // Close one sample bin per leg: the bytes each leg moved since the last.
    pub(crate) fn sample(&self) {
        if !self.measuring() {
            return;
        }
        let at = self.elapsed();
        let mut samples = self.samples.lock().expect("speed test samples");
        if at <= samples.0 && !samples.1.is_empty() {
            return;
        }
        samples.0 = at;
        for (leg, counters) in self.legs.iter().enumerate() {
            let total = counters.bytes.load(Ordering::Relaxed);
            let previous = counters.sampled.swap(total, Ordering::Relaxed);
            samples.1.push(Sample {
                at_ms: at.as_millis() as u64,
                leg,
                bytes: total.saturating_sub(previous),
            });
        }
    }

    pub(crate) fn record(
        &self,
        leg: usize,
        connect: Duration,
        path: SpeedTestDialPath,
        source: Option<SocketAddr>,
        error: Option<String>,
    ) -> Arc<ConnectionRecord> {
        let record = Arc::new(ConnectionRecord {
            leg,
            connect_ms: connect.as_millis() as u64,
            path,
            source,
            bytes: AtomicU64::new(0),
            error: Mutex::new(error),
        });
        self.connections
            .lock()
            .expect("speed test connections")
            .push(record.clone());
        record
    }

    // Attach the error that ended a leg's work to the leg's newest
    // connection still without one, or record it on its own.
    pub(crate) fn end_leg_connection(&self, leg: usize, egress: u32, error: String) {
        let connections = self.connections.lock().expect("speed test connections");
        if let Some(record) = connections
            .iter()
            .rev()
            .find(|record| record.leg == leg && !record.has_error())
        {
            record.fail(error);
            return;
        }
        drop(connections);
        self.record(
            leg,
            Duration::ZERO,
            SpeedTestDialPath {
                egress_id: egress,
                ..Default::default()
            },
            None,
            Some(error),
        );
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.legs
            .iter()
            .map(|counters| counters.bytes.load(Ordering::Relaxed))
            .sum()
    }

    // The result so far, or the final one once the run has finished.
    pub(crate) fn result(
        &self,
        usenet: bool,
        bandwidth: Option<super::model::BandwidthResult>,
    ) -> Option<SpeedTestResult> {
        if !self.measuring() {
            return None;
        }
        let elapsed = self.elapsed();
        let seconds = elapsed.as_secs_f64();
        let rate = |bytes: u64| {
            if seconds > 0.0 {
                bytes as f64 / seconds
            } else {
                0.0
            }
        };
        let samples = self.samples.lock().expect("speed test samples").1.clone();
        let connections: Vec<ConnectionResult> = self
            .connections
            .lock()
            .expect("speed test connections")
            .iter()
            .map(|record| record.result())
            .collect();
        // Bin rates, using each bin's own width.
        let mut bin_rates: Vec<(u64, usize, f64)> = Vec::new();
        let mut previous_at = vec![0u64; self.legs.len()];
        for sample in &samples {
            let width = sample.at_ms.saturating_sub(previous_at[sample.leg]).max(1);
            previous_at[sample.leg] = sample.at_ms;
            bin_rates.push((
                sample.at_ms,
                sample.leg,
                sample.bytes as f64 * 1000.0 / width as f64,
            ));
        }
        let legs: Vec<LegResult> = self
            .legs
            .iter()
            .enumerate()
            .map(|(position, counters)| {
                let bytes = counters.bytes.load(Ordering::Relaxed);
                let mine: Vec<&ConnectionResult> = connections
                    .iter()
                    .filter(|connection| connection.leg == position)
                    .collect();
                let mut sources: Vec<String> = mine
                    .iter()
                    .filter_map(|connection| connection.source_address.clone())
                    .map(|source| {
                        source
                            .parse::<SocketAddr>()
                            .map(|addr| addr.ip().to_string())
                            .unwrap_or(source)
                    })
                    .collect();
                sources.sort();
                sources.dedup();
                LegResult {
                    position,
                    bytes,
                    average_bps: rate(bytes),
                    peak_bps: bin_rates
                        .iter()
                        .filter(|(_, leg, _)| *leg == position)
                        .map(|(_, _, rate)| *rate)
                        .fold(0.0, f64::max),
                    connections: mine
                        .iter()
                        .filter(|c| c.error.is_none() || c.bytes > 0)
                        .count() as u32,
                    source_addresses: sources,
                }
            })
            .collect();
        let bytes = self.bytes();
        // The run's peak: for Usenet the highest reading of the header gauge,
        // which this run's bytes drive. Bins stand in when the gauge never
        // ticked during the run.
        let mut bin_totals: Vec<(u64, f64)> = Vec::new();
        for (at, _, rate) in &bin_rates {
            match bin_totals.iter_mut().find(|(t, _)| t == at) {
                Some((_, total)) => *total += rate,
                None => bin_totals.push((*at, *rate)),
            }
        }
        let bins_peak = bin_totals.iter().map(|(_, rate)| *rate).fold(0.0, f64::max);
        let gauge_peak = self.gauge_peak.load(Ordering::Relaxed) as f64;
        let peak = if usenet && gauge_peak > 0.0 {
            gauge_peak
        } else {
            bins_peak
        };
        let usenet = usenet.then(|| {
            let facts = self.usenet_facts.get().cloned().unwrap_or_default();
            UsenetResult {
                articles_requested: self.usenet.requested.load(Ordering::Relaxed),
                delivered: self.usenet.delivered.load(Ordering::Relaxed),
                missing: self.usenet.missing.load(Ordering::Relaxed),
                failed: self.usenet.failed.load(Ordering::Relaxed),
                declared_bytes: self.usenet.declared.load(Ordering::Relaxed),
                decoded_bytes: self.usenet.decoded.load(Ordering::Relaxed),
                pipelining: facts.pipelining,
                tls: facts.tls,
                tls_cipher_suite: facts.tls_cipher_suite,
            }
        });
        Some(SpeedTestResult {
            bytes,
            elapsed_ms: elapsed.as_millis() as u64,
            average_bps: rate(bytes),
            peak_bps: peak,
            legs,
            connections,
            usenet,
            bandwidth,
            samples,
        })
    }
}

fn dial_path(path: &DialPath, rung_map: &[usize]) -> SpeedTestDialPath {
    SpeedTestDialPath {
        egress_id: path.egress,
        rung: path.rung.and_then(|rung| rung_map.get(rung).copied()),
        pool_id: path.pool,
        member_id: path.member,
        proxy_ids: path.proxies.clone(),
    }
}

// Where a run's dials go and how they are recorded.
#[derive(Clone)]
pub(crate) struct LegRecorder {
    pub progress: Arc<Progress>,
    pub leg: usize,
    pub egress: u32,
    pub rung_map: Arc<[usize]>,
}

impl LegRecorder {
    fn dialed(&self, connect: Duration, dialed: &mut Dialed) -> Arc<ConnectionRecord> {
        dialed.path.leg = Some(self.leg);
        let record = self.progress.record(
            self.leg,
            connect,
            dial_path(&dialed.path, &self.rung_map),
            dialed.source,
            None,
        );
        let failed = record.clone();
        dialed
            .outcome
            .on_failure(move || failed.fail("connection failed"));
        record
    }

    fn failed(&self, connect: Duration, error: &DialError) {
        self.progress.record(
            self.leg,
            connect,
            SpeedTestDialPath {
                egress_id: self.egress,
                ..Default::default()
            },
            None,
            Some(error.to_string()),
        );
    }
}

// A leg's dialing stage for NNTP. Every dial is a probe of this leg, so the
// draft's health inputs never reach the live route's evidence, and every
// connection is recorded with the path it took.
pub(crate) struct RecordingDialer {
    pub inner: Arc<dyn Dialer>,
    pub recorder: LegRecorder,
}

#[async_trait::async_trait]
impl Dialer for RecordingDialer {
    async fn prepare(&self) -> Result<(), DialError> {
        self.inner.prepare().await
    }
    fn has_capacity(&self) -> bool {
        self.inner.has_capacity()
    }
    fn report_request(&self, error: Option<&DialError>) {
        self.inner.report_request(error);
    }
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        let mut target = target.clone();
        if let Purpose::Nntp { server, .. } | Purpose::NntpProbe { server, .. } = target.purpose {
            target.purpose = Purpose::NntpProbe {
                server,
                leg: self.recorder.leg,
            };
        }
        let started = Instant::now();
        match self.inner.dial(&target).await {
            Ok(mut dialed) => {
                let record = self.recorder.dialed(started.elapsed(), &mut dialed);
                dialed
                    .outcome
                    .on_read(move |bytes| record.add_bytes(bytes as u64));
                Ok(dialed)
            }
            Err(error) => {
                self.recorder.failed(started.elapsed(), &error);
                Err(error)
            }
        }
    }
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        self.inner.resolve(host).await
    }
    fn offers(&self) -> Transports {
        self.inner.offers()
    }
    fn budget(&self) -> Duration {
        self.inner.budget()
    }
    async fn shutdown(&self) {}
    fn describe(&self) -> String {
        format!(
            "speed test leg {}: {}",
            self.recorder.leg,
            self.inner.describe()
        )
    }
}

// A leg's dialing stage for HTTP, fronted by a bridge for reqwest. It dials
// only the addresses the run checked for the host.
pub(crate) struct StreamProvider {
    stage: Arc<dyn Dialer>,
    pins: Mutex<Option<(String, Vec<IpAddr>)>>,
    recorder: Option<LegRecorder>,
    current: Mutex<Option<Arc<ConnectionRecord>>>,
}

impl StreamProvider {
    pub(crate) fn new(stage: Arc<dyn Dialer>, recorder: Option<LegRecorder>) -> Arc<Self> {
        Arc::new(Self {
            stage,
            pins: Mutex::new(None),
            recorder,
            current: Mutex::new(None),
        })
    }
    pub(crate) fn pin(&self, host: &str, addresses: Vec<IpAddr>) {
        *self.pins.lock().expect("stream pins") = Some((host.to_owned(), addresses));
    }
    pub(crate) fn add_bytes(&self, bytes: u64) {
        if let Some(record) = &*self.current.lock().expect("stream connection") {
            record.add_bytes(bytes);
        }
    }
    // Record the error that ended the stream on its connection, or on its
    // own when no connection was opened.
    pub(crate) fn fail(&self, error: String) {
        let current = self.current.lock().expect("stream connection").clone();
        match (current, &self.recorder) {
            (Some(record), _) => record.fail(error),
            (None, Some(recorder)) => {
                recorder
                    .progress
                    .end_leg_connection(recorder.leg, recorder.egress, error);
            }
            (None, None) => {}
        }
    }
    pub(crate) fn dialed_any(&self) -> bool {
        self.current.lock().expect("stream connection").is_some()
    }
}

#[async_trait::async_trait]
impl TunnelProvider for StreamProvider {
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
            .pins
            .lock()
            .expect("stream pins")
            .as_ref()
            .filter(|(pinned, _)| pinned == host)
            .map(|(_, addresses)| addresses.clone())
            .unwrap_or_default();
        let target = Target {
            host: host.into(),
            port,
            purpose: Purpose::Probe,
            addresses,
        };
        let started = Instant::now();
        let mut dialed = match self.stage.dial(&target).await {
            Ok(dialed) => dialed,
            Err(error) => {
                if let Some(recorder) = &self.recorder {
                    recorder.failed(started.elapsed(), &error);
                }
                return Err(match error {
                    DialError::Fatal(error) => error,
                    error => TunnelError::Engine(error.to_string()),
                });
            }
        };
        if let Some(recorder) = &self.recorder {
            let record = recorder.dialed(started.elapsed(), &mut dialed);
            *self.current.lock().expect("stream connection") = Some(record);
        }
        let upstream = dialed.outcome.clone();
        observed.on_failure(move || upstream.failed());
        let upstream = dialed.outcome.clone();
        observed.on_close(move || upstream.closed());
        Ok(dialed.into_observed_stream())
    }
    fn describe(&self) -> String {
        format!("speed test stream: {}", self.stage.describe())
    }
}
