// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::time::Duration;

use async_graphql::{Context, Enum, InputObject, Object, Result, SimpleObject, Upload};
use weaver_server_core::proxies as core_proxies;
use weaver_server_core::speed_test as core;

use crate::auth::{AdminGuard, graphql_error};
use crate::networking::{LegPathGql, NetworkConsumerKind, RouteFailover, RouteInput, RouteLegGql};
use crate::observability::refusal_error;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum SpeedTestKind {
    Bandwidth,
    Usenet,
    Torrent,
}

impl From<core::SpeedTestKindLabel> for SpeedTestKind {
    fn from(value: core::SpeedTestKindLabel) -> Self {
        match value {
            core::SpeedTestKindLabel::Bandwidth => Self::Bandwidth,
            core::SpeedTestKindLabel::Usenet => Self::Usenet,
            core::SpeedTestKindLabel::Torrent => Self::Torrent,
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
    // The host a run downloads from; absent for a list provider, whose
    // servers each carry their own.
    pub host: Option<String>,
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
    // How much is posted to the provider after the run pulls, in MiB; 32
    // when not given, at most 128.
    pub upload_mib: Option<u32>,
}

// Exactly one of `sab_size`, `nzb_url`, `nzb_upload` and `nzb_run_id`.
#[derive(Clone, Debug, InputObject)]
pub struct UsenetTestInput {
    pub sab_size: Option<SabTestSize>,
    pub nzb_url: Option<String>,
    // One NZB file, read the way a fetched test NZB is.
    pub nzb_upload: Option<Upload>,
    // A held run whose uploaded NZB this run reuses.
    pub nzb_run_id: Option<u32>,
    pub connections: Option<u32>,
    #[graphql(default = 120)]
    pub max_duration_seconds: u32,
}

#[derive(Clone, Debug, InputObject)]
pub struct TorrentTestInput {
    // How long the run pulls from the swarm; 20 when absent.
    pub max_duration_seconds: Option<u32>,
}

#[derive(Clone, Debug, InputObject)]
pub struct SpeedTestInput {
    pub kind: NetworkConsumerKind,
    pub id: u32,
    pub route: Option<RouteInput>,
    pub overrides: Option<PathOverrideInput>,
    pub bandwidth: Option<BandwidthTestInput>,
    pub usenet: Option<UsenetTestInput>,
    pub torrent: Option<TorrentTestInput>,
}

const MIB: u64 = 1024 * 1024;

fn invalid(message: impl Into<String>) -> async_graphql::Error {
    graphql_error("INVALID_INPUT", message)
}

impl SpeedTestInput {
    fn request(
        self,
        ctx: &Context<'_>,
        service: &core::SpeedTestService,
    ) -> Result<core::SpeedTestRequest> {
        let consumer = self.kind.consumer(self.id);
        let kind = match (self.bandwidth, self.usenet, self.torrent) {
            (Some(bandwidth), None, None) => {
                let provider = match (bandwidth.provider_id, bandwidth.custom_url) {
                    (Some(id), None) => core::BandwidthProvider::Builtin {
                        id,
                        server: bandwidth.server,
                    },
                    (None, Some(url)) => core::BandwidthProvider::Custom {
                        url: reqwest::Url::parse(&url)
                            .map_err(|e| invalid(format!("customUrl: {e}")))?,
                    },
                    _ => {
                        return Err(invalid(
                            "a bandwidth test takes exactly one of providerId and customUrl",
                        ));
                    }
                };
                let upload = core::UploadRequest {
                    bytes: bandwidth.upload_mib.map(|mib| u64::from(mib) * MIB),
                };
                core::SpeedTestKind::Bandwidth { provider, upload }
            }
            (None, Some(usenet), None) => {
                let nzb = match (
                    usenet.sab_size,
                    usenet.nzb_url,
                    usenet.nzb_upload,
                    usenet.nzb_run_id,
                ) {
                    (Some(size), None, None, None) => {
                        core::TestNzbSource::SabnzbdTest { size: size.into() }
                    }
                    (None, Some(url), None, None) => core::TestNzbSource::Url(
                        reqwest::Url::parse(&url).map_err(|e| invalid(format!("nzbUrl: {e}")))?,
                    ),
                    (None, None, Some(upload), None) => {
                        let upload = upload
                            .value(ctx)
                            .map_err(|e| invalid(format!("invalid upload: {e}")))?;
                        // The upload arrives in memory and stays there; it is
                        // never written anywhere.
                        let nzb = core::UploadedNzb::read(upload.filename, &upload.content[..])
                            .map_err(invalid)?;
                        core::TestNzbSource::Upload { nzb, run: None }
                    }
                    (None, None, None, Some(run)) => core::TestNzbSource::Upload {
                        nzb: service.held_nzb(run).map_err(speed_test_error)?,
                        run: Some(run),
                    },
                    _ => {
                        return Err(invalid(
                            "a Usenet test takes exactly one of sabSize, nzbUrl, nzbUpload and nzbRunId",
                        ));
                    }
                };
                core::SpeedTestKind::Usenet {
                    nzb,
                    connections: usenet
                        .connections
                        .map(|c| u16::try_from(c).unwrap_or(u16::MAX)),
                    max_duration: Duration::from_secs(u64::from(usenet.max_duration_seconds)),
                }
            }
            (None, None, Some(torrent)) => core::SpeedTestKind::Torrent {
                source: core::TestTorrentSource::debian(),
                max_duration: torrent
                    .max_duration_seconds
                    .map_or(core::DEFAULT_TORRENT_DURATION, |seconds| {
                        Duration::from_secs(u64::from(seconds))
                    }),
            },
            _ => {
                return Err(invalid(
                    "a speed test takes exactly one of bandwidth, usenet and torrent",
                ));
            }
        };
        Ok(core::SpeedTestRequest {
            consumer,
            route: self.route.map(RouteInput::route).transpose()?,
            overrides: self.overrides.map(Into::into).unwrap_or_default(),
            kind,
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
    // The connection's rate over the newest sample bin.
    pub bytes_per_second: f64,
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
    // Parallel connections the run ended with.
    pub connections: u32,
}

#[derive(Clone, Debug, SimpleObject)]
pub struct Sample {
    pub at_ms: u32,
    pub leg: u32,
    pub bytes: f64,
    // Usenet only; 0 for a bandwidth run.
    pub decoded_bytes: f64,
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
    // A bandwidth run's upload, once it has posted.
    pub upload: Option<UploadResult>,
    pub samples: Vec<Sample>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum UploadOutcome {
    // The provider takes no upload.
    Unavailable,
    Measured,
    Failed,
    // No peer asked for anything.
    NoDemand,
}

// A post through the route: the route's upload capacity.
#[derive(Clone, Debug, SimpleObject)]
pub struct HttpUploadResult {
    pub outcome: UploadOutcome,
    // The provider that takes no upload, for UNAVAILABLE.
    pub provider: Option<String>,
    pub url: Option<String>,
    pub bytes: f64,
    pub elapsed_ms: u32,
    pub bps: f64,
    pub failure: Option<SpeedTestFailureGql>,
}

// The test torrent's piece served to the peers its trackers sent. The rate
// is a floor: it is only what those peers asked for.
#[derive(Clone, Debug, SimpleObject)]
pub struct PeerUploadResult {
    pub outcome: UploadOutcome,
    pub bytes: f64,
    pub elapsed_ms: u32,
    pub bps: f64,
    pub peers: u32,
    pub error: Option<String>,
}

#[derive(Clone, Debug, SimpleObject)]
pub struct UploadResult {
    // Absent for a torrent run, which posts nothing.
    pub http: Option<HttpUploadResult>,
    // Absent while torrents are off.
    pub peers: Option<PeerUploadResult>,
}

impl From<core::UploadResult> for UploadResult {
    fn from(upload: core::UploadResult) -> Self {
        let http = upload.http.map(|http| match http {
            core::HttpUpload::Unavailable { provider } => HttpUploadResult {
                outcome: UploadOutcome::Unavailable,
                provider: Some(provider),
                url: None,
                bytes: 0.0,
                elapsed_ms: 0,
                bps: 0.0,
                failure: None,
            },
            core::HttpUpload::Measured {
                url,
                bytes,
                elapsed_ms,
                bps,
            } => HttpUploadResult {
                outcome: UploadOutcome::Measured,
                provider: None,
                url: Some(url),
                bytes: bytes as f64,
                elapsed_ms: small(elapsed_ms),
                bps,
                failure: None,
            },
            core::HttpUpload::Failed { url, error } => HttpUploadResult {
                outcome: UploadOutcome::Failed,
                provider: None,
                url: Some(url),
                bytes: 0.0,
                elapsed_ms: 0,
                bps: 0.0,
                failure: Some(error.into()),
            },
        });
        let peers = upload.peers.map(|peers| match peers {
            core::PeerUpload::NoDemand => PeerUploadResult {
                outcome: UploadOutcome::NoDemand,
                bytes: 0.0,
                elapsed_ms: 0,
                bps: 0.0,
                peers: 0,
                error: None,
            },
            core::PeerUpload::Measured {
                bytes,
                elapsed,
                peers,
            } => PeerUploadResult {
                outcome: UploadOutcome::Measured,
                bytes: bytes as f64,
                elapsed_ms: small(elapsed.as_millis() as u64),
                bps: bytes as f64 / elapsed.as_secs_f64().max(0.001),
                peers,
                error: None,
            },
            core::PeerUpload::Failed(error) => PeerUploadResult {
                outcome: UploadOutcome::Failed,
                bytes: 0.0,
                elapsed_ms: 0,
                bps: 0.0,
                peers: 0,
                error: Some(error),
            },
        });
        Self { http, peers }
    }
}

// The request a run was started with, as accepted.
#[derive(Clone, Debug, SimpleObject)]
pub struct BandwidthTestRequest {
    pub provider_id: Option<String>,
    pub server: Option<String>,
    pub custom_url: Option<String>,
    pub upload_mib: Option<u32>,
}

#[derive(Clone, Debug, SimpleObject)]
pub struct UsenetTestRequest {
    pub sab_size: Option<SabTestSize>,
    pub nzb_url: Option<String>,
    // The uploaded NZB's filename; absent for a SAB size or a URL.
    pub nzb_filename: Option<String>,
    // The held run whose uploaded NZB this run reused.
    pub nzb_run_id: Option<u32>,
    // Absent takes the server's whole connection allowance.
    pub connections: Option<u32>,
    pub max_duration_seconds: u32,
}

#[derive(Clone, Debug, SimpleObject)]
pub struct TorrentTestRequest {
    pub max_duration_seconds: u32,
}

// Exactly one of `bandwidth`, `usenet` and `torrent` is set.
#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "SpeedTestRequest")]
pub struct SpeedTestRequestGql {
    pub bandwidth: Option<BandwidthTestRequest>,
    pub usenet: Option<UsenetTestRequest>,
    pub torrent: Option<TorrentTestRequest>,
}

impl From<&core::SpeedTestRequest> for SpeedTestRequestGql {
    fn from(request: &core::SpeedTestRequest) -> Self {
        let (bandwidth, usenet, torrent) = match &request.kind {
            core::SpeedTestKind::Bandwidth { provider, upload } => {
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
                        upload_mib: upload.bytes.map(|bytes| small(bytes.div_ceil(MIB))),
                    }),
                    None,
                    None,
                )
            }
            core::SpeedTestKind::Usenet {
                nzb,
                connections,
                max_duration,
            } => {
                let (sab_size, nzb_url, nzb_filename, nzb_run_id) = match nzb {
                    core::TestNzbSource::SabnzbdTest { size } => {
                        (Some((*size).into()), None, None, None)
                    }
                    core::TestNzbSource::Url(url) => (None, Some(url.to_string()), None, None),
                    core::TestNzbSource::Upload { nzb, run } => {
                        (None, None, Some(nzb.filename().to_owned()), *run)
                    }
                };
                (
                    None,
                    Some(UsenetTestRequest {
                        sab_size,
                        nzb_url,
                        nzb_filename,
                        nzb_run_id,
                        connections: connections.map(u32::from),
                        max_duration_seconds: small(max_duration.as_secs()),
                    }),
                    None,
                )
            }
            core::SpeedTestKind::Torrent { max_duration, .. } => (
                None,
                None,
                Some(TorrentTestRequest {
                    max_duration_seconds: small(max_duration.as_secs()),
                }),
            ),
        };
        Self {
            bandwidth,
            usenet,
            torrent,
        }
    }
}

// Why a run failed. A client words each kind itself and falls back to the
// failure's message for OTHER, or for a kind it does not know.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum SpeedTestFailureCode {
    // The provider answered HTTP 429.
    RateLimited,
    // The provider answered another status that is not a success.
    HttpStatus,
    TlsVerification,
    Unresolved,
    AddressNotAllowed,
    EgressQuotaUsed,
    NoLegUp,
    LeaseRefused,
    ServerLeftPool,
    Stopped,
    StoppedUnexpectedly,
    NoBytes,
    Other,
}

#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "SpeedTestFailure")]
pub struct SpeedTestFailureGql {
    pub code: SpeedTestFailureCode,
    // The daemon's own account, naming the URL, host or status.
    pub message: String,
    // The values the client's sentence for the code may name.
    pub params: async_graphql::Json<serde_json::Map<String, serde_json::Value>>,
}

impl From<core::SpeedTestFailure> for SpeedTestFailureGql {
    fn from(failure: core::SpeedTestFailure) -> Self {
        use core::SpeedTestFailure as F;
        let code = match &failure {
            F::RateLimited { .. } => SpeedTestFailureCode::RateLimited,
            F::HttpStatus { .. } => SpeedTestFailureCode::HttpStatus,
            F::TlsVerification { .. } => SpeedTestFailureCode::TlsVerification,
            F::Unresolved { .. } => SpeedTestFailureCode::Unresolved,
            F::AddressNotAllowed { .. } => SpeedTestFailureCode::AddressNotAllowed,
            F::EgressQuotaUsed { .. } => SpeedTestFailureCode::EgressQuotaUsed,
            F::NoLegUp => SpeedTestFailureCode::NoLegUp,
            F::LeaseRefused { .. } => SpeedTestFailureCode::LeaseRefused,
            F::ServerLeftPool => SpeedTestFailureCode::ServerLeftPool,
            F::Stopped => SpeedTestFailureCode::Stopped,
            F::StoppedUnexpectedly => SpeedTestFailureCode::StoppedUnexpectedly,
            F::NoBytes => SpeedTestFailureCode::NoBytes,
            F::Other(_) => SpeedTestFailureCode::Other,
        };
        Self {
            code,
            message: failure.to_string(),
            params: async_graphql::Json(failure.params()),
        }
    }
}

#[derive(Clone, Debug, SimpleObject)]
pub struct SpeedTestRun {
    pub id: u32,
    // "server:N", "rss:N" or "outbound".
    pub consumer: String,
    pub consumer_name: String,
    pub kind: SpeedTestKind,
    pub request: SpeedTestRequestGql,
    pub status: SpeedTestStatus,
    // While running: "preparing", "pausing", "draining N", "leasing",
    // "measuring", "uploading" or "releasing".
    pub phase: Option<String>,
    // Epoch milliseconds.
    pub started_at: f64,
    pub finished_at: Option<f64>,
    // Why a failed run failed.
    pub failure: Option<SpeedTestFailureGql>,
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
                    bytes_per_second: c.bytes_per_second as f64,
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
                connections: b.connections,
            }),
            upload: r.upload.map(Into::into),
            samples: r
                .samples
                .into_iter()
                .map(|s| Sample {
                    at_ms: small(s.at_ms),
                    leg: index(s.leg),
                    bytes: s.bytes as f64,
                    decoded_bytes: s.decoded_bytes as f64,
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
            failure: run.error.map(Into::into),
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
        core::SpeedTestError::Invalid(message) => invalid(message),
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
                host: p.host.map(Into::into),
                needs_server: p.needs_server(),
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
        let service = service(ctx)?;
        let request = input.request(ctx, &service)?;
        let run = service.start(request).await.map_err(speed_test_error)?;
        Ok(run.into())
    }

    // False when no run with this id is running.
    #[graphql(guard = "AdminGuard")]
    async fn cancel_speed_test(&self, ctx: &Context<'_>, id: u32) -> Result<bool> {
        Ok(service(ctx)?.cancel(id))
    }
}
