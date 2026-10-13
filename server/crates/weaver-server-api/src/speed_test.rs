use std::time::Duration;

use async_graphql::{Context, Enum, InputObject, Object, Result, SimpleObject};
use weaver_server_core::proxies as core_proxies;
use weaver_server_core::speed_test as core;

use crate::auth::AdminGuard;
use crate::networking::{LegPathGql, NetworkConsumerKind, RouteFailover, RouteInput, RouteLegGql};
use crate::observability::refusal_error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum SpeedTestKind {
    Bandwidth,
    Usenet,
}

impl From<core::SpeedTestKindLabel> for SpeedTestKind {
    fn from(value: core::SpeedTestKindLabel) -> Self {
        match value {
            core::SpeedTestKindLabel::Bandwidth => Self::Bandwidth,
            core::SpeedTestKindLabel::Usenet => Self::Usenet,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum SpeedTestStatus {
    Running,
    Completed,
    Cancelled,
    Failed,
}

impl From<core::SpeedTestStatus> for SpeedTestStatus {
    fn from(value: core::SpeedTestStatus) -> Self {
        match value {
            core::SpeedTestStatus::Running => Self::Running,
            core::SpeedTestStatus::Completed => Self::Completed,
            core::SpeedTestStatus::Cancelled => Self::Cancelled,
            core::SpeedTestStatus::Failed => Self::Failed,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum SabTestSize {
    #[graphql(name = "MB_100")]
    Mb100,
    #[graphql(name = "GB_1")]
    Gb1,
    #[graphql(name = "GB_10")]
    Gb10,
}

impl From<core::SabTestSize> for SabTestSize {
    fn from(value: core::SabTestSize) -> Self {
        match value {
            core::SabTestSize::Mb100 => Self::Mb100,
            core::SabTestSize::Gb1 => Self::Gb1,
            core::SabTestSize::Gb10 => Self::Gb10,
        }
    }
}

impl From<SabTestSize> for core::SabTestSize {
    fn from(value: SabTestSize) -> Self {
        match value {
            SabTestSize::Mb100 => Self::Mb100,
            SabTestSize::Gb1 => Self::Gb1,
            SabTestSize::Gb10 => Self::Gb10,
        }
    }
}

#[derive(Clone, Debug, SimpleObject)]
pub struct BandwidthProvider {
    pub id: String,
    pub name: String,
    pub fixed_sizes: Vec<f64>,
    pub any_size: bool,
    // The largest per-stream size an any-size provider serves.
    pub max_bytes: Option<f64>,
    pub needs_server: bool,
}

#[derive(Clone, Debug, SimpleObject)]
pub struct BandwidthProviderServer {
    pub id: String,
    pub name: String,
    pub location: Option<String>,
    pub host: String,
}

#[derive(Clone, Debug, InputObject)]
pub struct LegRungInput {
    pub leg: u32,
    pub rung: u32,
}

#[derive(Clone, Debug, Default, InputObject)]
pub struct PathOverrideInput {
    #[graphql(default)]
    pub down_legs: Vec<u32>,
    #[graphql(default)]
    pub down_rungs: Vec<LegRungInput>,
    #[graphql(default)]
    pub down_proxies: Vec<u32>,
    #[graphql(default)]
    pub down_egresses: Vec<u32>,
}

impl From<PathOverrideInput> for core::PathOverride {
    fn from(value: PathOverrideInput) -> Self {
        Self {
            down_legs: value
                .down_legs
                .into_iter()
                .map(|leg| leg as usize)
                .collect(),
            down_rungs: value
                .down_rungs
                .into_iter()
                .map(|r| (r.leg as usize, r.rung as usize))
                .collect(),
            down_proxies: value.down_proxies,
            down_egresses: value.down_egresses,
        }
    }
}

#[derive(Clone, Debug, InputObject)]
pub struct BandwidthTestInput {
    pub provider_id: Option<String>,
    pub server: Option<String>,
    pub custom_url: Option<String>,
    // Bytes each stream asks for.
    pub bytes: f64,
    #[graphql(default = 4)]
    pub streams: u32,
}

#[derive(Clone, Debug, InputObject)]
pub struct UsenetTestInput {
    pub sab_size: Option<SabTestSize>,
    pub nzb_url: Option<String>,
    pub connections: Option<u32>,
}

#[derive(Clone, Debug, InputObject)]
pub struct SpeedTestInput {
    pub kind: NetworkConsumerKind,
    pub id: u32,
    pub route: Option<RouteInput>,
    pub overrides: Option<PathOverrideInput>,
    pub bandwidth: Option<BandwidthTestInput>,
    pub usenet: Option<UsenetTestInput>,
    #[graphql(default = 120)]
    pub max_duration_seconds: u32,
}

impl SpeedTestInput {
    fn request(self) -> Result<core::SpeedTestRequest> {
        let consumer = self.kind.consumer(self.id);
        let kind = match (self.bandwidth, self.usenet) {
            (Some(bandwidth), None) => {
                let provider = match (bandwidth.provider_id, bandwidth.custom_url) {
                    (Some(id), None) => core::BandwidthProvider::Builtin {
                        id,
                        server: bandwidth.server,
                    },
                    (None, Some(url)) => core::BandwidthProvider::Custom {
                        url: reqwest::Url::parse(&url)
                            .map_err(|e| async_graphql::Error::new(format!("customUrl: {e}")))?,
                    },
                    _ => {
                        return Err(async_graphql::Error::new(
                            "a bandwidth test takes exactly one of providerId and customUrl",
                        ));
                    }
                };
                if !bandwidth.bytes.is_finite() || bandwidth.bytes < 1.0 {
                    return Err(async_graphql::Error::new(
                        "a bandwidth run must ask for at least one byte",
                    ));
                }
                core::SpeedTestKind::Bandwidth {
                    provider,
                    bytes: bandwidth.bytes as u64,
                    streams: u8::try_from(bandwidth.streams).unwrap_or(u8::MAX),
                }
            }
            (None, Some(usenet)) => {
                let nzb = match (usenet.sab_size, usenet.nzb_url) {
                    (Some(size), None) => core::TestNzbSource::SabnzbdTest { size: size.into() },
                    (None, Some(url)) => core::TestNzbSource::Url(
                        reqwest::Url::parse(&url)
                            .map_err(|e| async_graphql::Error::new(format!("nzbUrl: {e}")))?,
                    ),
                    _ => {
                        return Err(async_graphql::Error::new(
                            "a Usenet test takes exactly one of sabSize and nzbUrl",
                        ));
                    }
                };
                core::SpeedTestKind::Usenet {
                    nzb,
                    connections: usenet
                        .connections
                        .map(|c| u16::try_from(c).unwrap_or(u16::MAX)),
                }
            }
            _ => {
                return Err(async_graphql::Error::new(
                    "a speed test takes exactly one of bandwidth and usenet",
                ));
            }
        };
        Ok(core::SpeedTestRequest {
            consumer,
            route: self.route.map(RouteInput::route).transpose()?,
            overrides: self.overrides.map(Into::into).unwrap_or_default(),
            kind,
            max_duration: Duration::from_secs(u64::from(self.max_duration_seconds)),
        })
    }
}

#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "PathOverride")]
pub struct PathOverrideGql {
    pub down_legs: Vec<u32>,
    pub down_rungs: Vec<LegRung>,
    pub down_proxies: Vec<u32>,
    pub down_egresses: Vec<u32>,
}

#[derive(Clone, Debug, SimpleObject)]
pub struct LegRung {
    pub leg: u32,
    pub rung: u32,
}

#[derive(Clone, Debug, SimpleObject)]
pub struct NamedId {
    pub kind: String,
    pub id: u32,
    pub name: String,
}

#[derive(Clone, Debug, SimpleObject)]
pub struct TestedLeg {
    pub position: u32,
    pub egress_id: u32,
    pub weight: u32,
    pub down: bool,
    pub down_reason: Option<String>,
    pub path: LegPathGql,
    pub target: u32,
}

#[derive(Clone, Debug, SimpleObject)]
pub struct TestedRoute {
    pub legs: Vec<TestedLeg>,
    pub failover: RouteFailover,
    pub overrides: PathOverrideGql,
    pub names: Vec<NamedId>,
}

#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "DialPath")]
pub struct DialPathGql {
    pub egress_id: u32,
    pub rung: Option<u32>,
    pub pool_id: Option<u32>,
    pub member_id: Option<u32>,
    pub proxy_ids: Vec<u32>,
}

#[derive(Clone, Debug, SimpleObject)]
pub struct ConnectionResult {
    pub leg: u32,
    pub connect_ms: u32,
    pub path: DialPathGql,
    pub source_address: Option<String>,
    pub bytes: f64,
    pub error: Option<String>,
}

#[derive(Clone, Debug, SimpleObject)]
pub struct LegResult {
    pub position: u32,
    pub bytes: f64,
    pub average_bps: f64,
    pub peak_bps: f64,
    pub connections: u32,
    pub source_addresses: Vec<String>,
}

#[derive(Clone, Debug, SimpleObject)]
pub struct UsenetResult {
    pub articles_requested: u32,
    pub delivered: u32,
    pub missing: u32,
    pub failed: u32,
    pub declared_bytes: f64,
    pub decoded_bytes: f64,
    pub pipelining: bool,
    pub tls: bool,
    pub tls_cipher_suite: Option<String>,
}

#[derive(Clone, Debug, SimpleObject)]
pub struct BandwidthResult {
    pub provider_id: Option<String>,
    pub server: Option<String>,
    pub url: String,
    pub streams: u32,
}

#[derive(Clone, Debug, SimpleObject)]
pub struct Sample {
    pub at_ms: u32,
    pub leg: u32,
    pub bytes: f64,
}

#[derive(Clone, Debug, SimpleObject)]
pub struct SpeedTestResult {
    pub bytes: f64,
    pub elapsed_ms: u32,
    pub average_bps: f64,
    pub peak_bps: f64,
    pub legs: Vec<LegResult>,
    pub connections: Vec<ConnectionResult>,
    pub usenet: Option<UsenetResult>,
    pub bandwidth: Option<BandwidthResult>,
    pub samples: Vec<Sample>,
}

// The request a run was started with, as accepted.
#[derive(Clone, Debug, SimpleObject)]
pub struct BandwidthTestRequest {
    pub provider_id: Option<String>,
    pub server: Option<String>,
    pub custom_url: Option<String>,
    // Bytes each stream asks for.
    pub bytes: f64,
    pub streams: u32,
}

#[derive(Clone, Debug, SimpleObject)]
pub struct UsenetTestRequest {
    pub sab_size: Option<SabTestSize>,
    pub nzb_url: Option<String>,
    // Absent takes the server's whole connection allowance.
    pub connections: Option<u32>,
}

// Exactly one of `bandwidth` and `usenet` is set.
#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "SpeedTestRequest")]
pub struct SpeedTestRequestGql {
    pub bandwidth: Option<BandwidthTestRequest>,
    pub usenet: Option<UsenetTestRequest>,
    pub max_duration_seconds: u32,
}

impl From<&core::SpeedTestRequest> for SpeedTestRequestGql {
    fn from(request: &core::SpeedTestRequest) -> Self {
        let (bandwidth, usenet) = match &request.kind {
            core::SpeedTestKind::Bandwidth {
                provider,
                bytes,
                streams,
            } => {
                let (provider_id, server, custom_url) = match provider {
                    core::BandwidthProvider::Builtin { id, server } => {
                        (Some(id.clone()), server.clone(), None)
                    }
                    core::BandwidthProvider::Custom { url } => (None, None, Some(url.to_string())),
                };
                (
                    Some(BandwidthTestRequest {
                        provider_id,
                        server,
                        custom_url,
                        bytes: *bytes as f64,
                        streams: u32::from(*streams),
                    }),
                    None,
                )
            }
            core::SpeedTestKind::Usenet { nzb, connections } => {
                let (sab_size, nzb_url) = match nzb {
                    core::TestNzbSource::SabnzbdTest { size } => (Some((*size).into()), None),
                    core::TestNzbSource::Url(url) => (None, Some(url.to_string())),
                };
                (
                    None,
                    Some(UsenetTestRequest {
                        sab_size,
                        nzb_url,
                        connections: connections.map(u32::from),
                    }),
                )
            }
        };
        Self {
            bandwidth,
            usenet,
            max_duration_seconds: small(request.max_duration.as_secs()),
        }
    }
}

#[derive(Clone, Debug, SimpleObject)]
pub struct SpeedTestRun {
    pub id: u32,
    // "server:N" or "rss:N".
    pub consumer: String,
    pub consumer_name: String,
    pub kind: SpeedTestKind,
    pub request: SpeedTestRequestGql,
    pub status: SpeedTestStatus,
    // While running: "preparing", "pausing", "draining N", "leasing",
    // "measuring" or "releasing".
    pub phase: Option<String>,
    // Epoch milliseconds.
    pub started_at: f64,
    pub finished_at: Option<f64>,
    pub error: Option<String>,
    pub tested: TestedRoute,
    pub result: Option<SpeedTestResult>,
}

fn small(value: u64) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

fn index(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

impl From<core::SpeedTestResult> for SpeedTestResult {
    fn from(r: core::SpeedTestResult) -> Self {
        Self {
            bytes: r.bytes as f64,
            elapsed_ms: small(r.elapsed_ms),
            average_bps: r.average_bps,
            peak_bps: r.peak_bps,
            legs: r
                .legs
                .into_iter()
                .map(|leg| LegResult {
                    position: index(leg.position),
                    bytes: leg.bytes as f64,
                    average_bps: leg.average_bps,
                    peak_bps: leg.peak_bps,
                    connections: leg.connections,
                    source_addresses: leg.source_addresses,
                })
                .collect(),
            connections: r
                .connections
                .into_iter()
                .map(|c| ConnectionResult {
                    leg: index(c.leg),
                    connect_ms: small(c.connect_ms),
                    path: DialPathGql {
                        egress_id: c.path.egress_id,
                        rung: c.path.rung.map(index),
                        pool_id: c.path.pool_id,
                        member_id: c.path.member_id,
                        proxy_ids: c.path.proxy_ids,
                    },
                    source_address: c.source_address,
                    bytes: c.bytes as f64,
                    error: c.error,
                })
                .collect(),
            usenet: r.usenet.map(|u| UsenetResult {
                articles_requested: small(u.articles_requested),
                delivered: small(u.delivered),
                missing: small(u.missing),
                failed: small(u.failed),
                declared_bytes: u.declared_bytes as f64,
                decoded_bytes: u.decoded_bytes as f64,
                pipelining: u.pipelining,
                tls: u.tls,
                tls_cipher_suite: u.tls_cipher_suite,
            }),
            bandwidth: r.bandwidth.map(|b| BandwidthResult {
                provider_id: b.provider_id,
                server: b.server,
                url: b.url,
                streams: b.streams,
            }),
            samples: r
                .samples
                .into_iter()
                .map(|s| Sample {
                    at_ms: small(s.at_ms),
                    leg: index(s.leg),
                    bytes: s.bytes as f64,
                })
                .collect(),
        }
    }
}

impl From<core::SpeedTestRun> for SpeedTestRun {
    fn from(run: core::SpeedTestRun) -> Self {
        let phase = run.phase_label();
        let tested = run.tested;
        Self {
            id: run.id,
            consumer: run.consumer.key(),
            consumer_name: run.consumer_name,
            kind: run.kind.into(),
            request: (&run.request).into(),
            status: run.status.into(),
            phase,
            started_at: run.started_at_ms as f64,
            finished_at: run.finished_at_ms.map(|at| at as f64),
            error: run.error,
            tested: TestedRoute {
                legs: tested
                    .legs
                    .into_iter()
                    .map(|leg| TestedLeg {
                        position: index(leg.position),
                        egress_id: leg.egress_id,
                        weight: u32::from(leg.weight),
                        down: leg.down,
                        down_reason: leg.down_reason,
                        path: RouteLegGql::from(core_proxies::RouteLeg {
                            egress_id: leg.egress_id,
                            weight: leg.weight,
                            path: leg.path,
                        })
                        .path,
                        target: u32::from(leg.target),
                    })
                    .collect(),
                failover: tested.failover.into(),
                overrides: PathOverrideGql {
                    down_legs: tested.overrides.down_legs.into_iter().map(index).collect(),
                    down_rungs: tested
                        .overrides
                        .down_rungs
                        .into_iter()
                        .map(|(leg, rung)| LegRung {
                            leg: index(leg),
                            rung: index(rung),
                        })
                        .collect(),
                    down_proxies: tested.overrides.down_proxies,
                    down_egresses: tested.overrides.down_egresses,
                },
                names: tested
                    .names
                    .into_iter()
                    .map(|n| NamedId {
                        kind: n.kind.into(),
                        id: n.id,
                        name: n.name,
                    })
                    .collect(),
            },
            result: run.result.map(Into::into),
        }
    }
}

fn service(ctx: &Context<'_>) -> Result<core::SpeedTestService> {
    ctx.data::<core::SpeedTestService>().cloned()
}

fn speed_test_error(error: core::SpeedTestError) -> async_graphql::Error {
    match error {
        core::SpeedTestError::Refused(refusal) => refusal_error(refusal),
        core::SpeedTestError::Invalid(message) => async_graphql::Error::new(message),
    }
}

#[derive(Default)]
pub struct SpeedTestQuery;

#[Object]
impl SpeedTestQuery {
    #[graphql(guard = "AdminGuard")]
    async fn bandwidth_providers(&self) -> Vec<BandwidthProvider> {
        core::PROVIDERS
            .iter()
            .map(|p| BandwidthProvider {
                id: p.id.into(),
                name: p.name.into(),
                fixed_sizes: p.fixed_sizes.iter().map(|size| *size as f64).collect(),
                any_size: p.any_size,
                max_bytes: p.max_size.map(|size| size as f64),
                needs_server: p.needs_server,
            })
            .collect()
    }

    #[graphql(guard = "AdminGuard")]
    async fn bandwidth_provider_servers(
        &self,
        ctx: &Context<'_>,
        provider_id: String,
        kind: NetworkConsumerKind,
        id: u32,
    ) -> Result<Vec<BandwidthProviderServer>> {
        let servers = service(ctx)?
            .provider_servers(&provider_id, kind.consumer(id))
            .await
            .map_err(speed_test_error)?;
        Ok(servers
            .into_iter()
            .map(|s| BandwidthProviderServer {
                id: s.id,
                name: s.name,
                location: s.location,
                host: s.host,
            })
            .collect())
    }

    // The running run and the last ten finished, newest first.
    #[graphql(guard = "AdminGuard")]
    async fn speed_test_runs(&self, ctx: &Context<'_>) -> Result<Vec<SpeedTestRun>> {
        Ok(service(ctx)?.runs().into_iter().map(Into::into).collect())
    }

    #[graphql(guard = "AdminGuard")]
    async fn speed_test_run(&self, ctx: &Context<'_>, id: u32) -> Result<Option<SpeedTestRun>> {
        Ok(service(ctx)?.run(id).map(Into::into))
    }
}

#[derive(Default)]
pub struct SpeedTestMutation;

#[Object]
impl SpeedTestMutation {
    #[graphql(guard = "AdminGuard")]
    async fn start_speed_test(
        &self,
        ctx: &Context<'_>,
        input: SpeedTestInput,
    ) -> Result<SpeedTestRun> {
        let request = input.request()?;
        let run = service(ctx)?
            .start(request)
            .await
            .map_err(speed_test_error)?;
        Ok(run.into())
    }

    // False when no run with this id is running.
    #[graphql(guard = "AdminGuard")]
    async fn cancel_speed_test(&self, ctx: &Context<'_>, id: u32) -> Result<bool> {
        Ok(service(ctx)?.cancel(id))
    }
}
