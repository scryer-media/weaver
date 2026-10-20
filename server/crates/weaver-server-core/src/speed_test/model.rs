// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::Arc;
use std::time::Duration;

use super::failure::SpeedTestFailure;
use super::http::{NZB_LIMIT, TestArticle, parse_test_nzb};
use crate::proxies::{Consumer, Failover, LegPath, Route};

// What a run measures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpeedTestKind {
    // Repeated HTTPS GETs of the provider's largest download through the
    // route, over as many connections as the line takes.
    Bandwidth {
        provider: BandwidthProvider,
        upload: UploadRequest,
    },
    // The live fused article fetch of every article in a test NZB from the
    // server. `None` takes the server's whole connection allowance.
    Usenet {
        nzb: TestNzbSource,
        connections: Option<u16>,
        max_duration: Duration,
    },
}

impl SpeedTestKind {
    pub fn label(&self) -> SpeedTestKindLabel {
        match self {
            Self::Bandwidth { .. } => SpeedTestKindLabel::Bandwidth,
            Self::Usenet { .. } => SpeedTestKindLabel::Usenet,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SpeedTestKindLabel {
    Bandwidth,
    Usenet,
}

impl SpeedTestKindLabel {
    pub const ALL: [Self; 2] = [Self::Bandwidth, Self::Usenet];

    pub fn index(self) -> usize {
        match self {
            Self::Bandwidth => 0,
            Self::Usenet => 1,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bandwidth => "bandwidth",
            Self::Usenet => "usenet",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BandwidthProvider {
    // A provider from the built-in table; `server` picks one entry of a list
    // provider's server list. Without one, a list provider run takes the
    // nearest: the first listed server to answer a ping through the route.
    Builtin { id: String, server: Option<String> },
    // Any HTTPS URL. The egress block rules apply to it.
    Custom { url: reqwest::Url },
}

// What a bandwidth run sends after it has pulled.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UploadRequest {
    // An HTTPS endpoint that takes a POST. The egress block rules apply to
    // it. Without one, nothing is posted.
    pub url: Option<reqwest::Url>,
    // How much is posted; `DEFAULT_UPLOAD_BYTES` when not given.
    pub bytes: Option<u64>,
}

// What a post sends when the request names no size.
pub const DEFAULT_UPLOAD_BYTES: u64 = 32 * 1024 * 1024;
// The most a post may send.
pub const MAX_UPLOAD_BYTES: u64 = 128 * 1024 * 1024;
// The most the test torrent serves to peers.
pub const PEER_UPLOAD_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SabTestSize {
    Mb100,
    Gb1,
    Gb10,
}

impl SabTestSize {
    pub fn url(self) -> &'static str {
        match self {
            Self::Mb100 => "https://sabnzbd.org/tests/test_download_100MB.nzb",
            Self::Gb1 => "https://sabnzbd.org/tests/test_download_1000MB.nzb",
            Self::Gb10 => "https://sabnzbd.org/tests/test_download_10GB.nzb",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestNzbSource {
    SabnzbdTest { size: SabTestSize },
    Url(reqwest::Url),
    // An uploaded NZB; `run` is the held run it was taken from, when it was.
    Upload { nzb: UploadedNzb, run: Option<u32> },
}

impl TestNzbSource {
    // Where the NZB is fetched from; `None` for an upload.
    pub fn url(&self) -> Result<Option<reqwest::Url>, String> {
        match self {
            Self::SabnzbdTest { size } => reqwest::Url::parse(size.url())
                .map(Some)
                .map_err(|e| e.to_string()),
            Self::Url(url) => Ok(Some(url.clone())),
            Self::Upload { .. } => Ok(None),
        }
    }
}

// An NZB uploaded with a request. It is held in memory only, for as long as
// the run that holds it is.
#[derive(Clone, PartialEq, Eq)]
pub struct UploadedNzb {
    filename: String,
    bytes: Arc<[u8]>,
}

impl UploadedNzb {
    // Refused past the size a fetched test NZB may have, and when it does not
    // read as a fetched one would.
    pub fn read(filename: String, bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() > NZB_LIMIT {
            return Err(format!("the test NZB is larger than {NZB_LIMIT} bytes"));
        }
        parse_test_nzb(bytes)?;
        Ok(Self {
            filename,
            bytes: bytes.into(),
        })
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub(crate) fn articles(&self) -> Result<Vec<TestArticle>, String> {
        parse_test_nzb(&self.bytes)
    }
}

impl std::fmt::Debug for UploadedNzb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadedNzb")
            .field("filename", &self.filename)
            .field("bytes", &self.bytes.len())
            .finish()
    }
}

// What the operator took down for one run. Leg positions index the route as
// it is tested (saved or draft); rung positions index inside their leg.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PathOverride {
    pub down_legs: Vec<usize>,
    pub down_rungs: Vec<(usize, usize)>,
    // A downed proxy also leaves every pool it is a member of, and takes down
    // every chain it is a hop of.
    pub down_proxies: Vec<u32>,
    pub down_egresses: Vec<u32>,
}

impl PathOverride {
    pub fn is_empty(&self) -> bool {
        self.down_legs.is_empty()
            && self.down_rungs.is_empty()
            && self.down_proxies.is_empty()
            && self.down_egresses.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpeedTestRequest {
    // Usenet runs need a server.
    pub consumer: Consumer,
    // `None` tests the saved route; `Some` an unsaved draft.
    pub route: Option<Route>,
    pub overrides: PathOverride,
    pub kind: SpeedTestKind,
}

// The longest a Usenet run may measure for.
pub const MAX_DURATION: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SpeedTestStatus {
    Running,
    Completed,
    Cancelled,
    Failed,
}

impl SpeedTestStatus {
    // The statuses a run can finish with.
    pub const FINISHED: [Self; 3] = [Self::Completed, Self::Cancelled, Self::Failed];

    // Position in `FINISHED`; `None` while running.
    pub fn finished_index(self) -> Option<usize> {
        match self {
            Self::Running => None,
            Self::Completed => Some(0),
            Self::Cancelled => Some(1),
            Self::Failed => Some(2),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }
}

// Where a running run is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpeedTestPhase {
    // Drafting the route and, for Usenet, fetching the test NZB.
    Preparing,
    Pausing,
    // Waiting for in-flight articles to finish.
    Draining,
    // Taking the server's connections from the live pool.
    Leasing,
    Measuring,
    // Posting to the upload endpoint, then serving the test torrent.
    Uploading,
    // Giving the connections, the queue and the draft back.
    Releasing,
}

// A name an id had when the run was tested.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedId {
    // "egress", "proxy" or "pool".
    pub kind: &'static str,
    pub id: u32,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestedLegSnapshot {
    pub position: usize,
    pub egress_id: u32,
    pub weight: u8,
    pub down: bool,
    pub down_reason: Option<String>,
    // The leg's path as the route has it; the run's overrides say which of
    // its rungs, members and hops were down.
    pub path: LegPath,
    // Connections the leg was given; a bandwidth run's grow as it ramps.
    pub target: u16,
}

// The route exactly as it was tested, with the names it had then, so a run
// still reads correctly after the route, a proxy or an egress is edited.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestedRouteSnapshot {
    pub legs: Vec<TestedLegSnapshot>,
    pub failover: Failover,
    pub overrides: PathOverride,
    pub names: Vec<NamedId>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SpeedTestDialPath {
    pub egress_id: u32,
    // Rung position in the leg as the route has it, not as it was trimmed.
    pub rung: Option<usize>,
    pub pool_id: Option<u32>,
    pub member_id: Option<u32>,
    pub proxy_ids: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionResult {
    pub leg: usize,
    pub connect_ms: u64,
    pub path: SpeedTestDialPath,
    pub source_address: Option<String>,
    pub bytes: u64,
    // The connection's rate over the newest sample bin.
    pub bytes_per_second: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LegResult {
    pub position: usize,
    pub bytes: u64,
    pub average_bps: f64,
    pub peak_bps: f64,
    pub connections: u32,
    pub source_addresses: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UsenetResult {
    pub articles_requested: u64,
    pub delivered: u64,
    pub missing: u64,
    pub failed: u64,
    // Payload bytes as the NZB declared them, for the articles requested.
    pub declared_bytes: u64,
    pub decoded_bytes: u64,
    pub pipelining: bool,
    pub tls: bool,
    pub tls_cipher_suite: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BandwidthResult {
    pub provider_id: Option<String>,
    // The list server the run pulled from, asked for or picked as nearest.
    pub server: Option<String>,
    pub url: String,
    // Parallel connections the run ended with.
    pub connections: u32,
}

// A post to the upload endpoint, through the route.
#[derive(Debug, Clone, PartialEq)]
pub enum HttpUpload {
    // The request named no endpoint.
    NotConfigured,
    Measured {
        url: String,
        bytes: u64,
        elapsed_ms: u64,
        bps: f64,
    },
    Failed {
        url: String,
        error: SpeedTestFailure,
    },
}

// What a bandwidth run sent. The post measures the route's upload; the
// peers' figure is at least what the route can send, since it is only what
// the peers asked for.
#[derive(Debug, Clone, PartialEq)]
pub struct UploadResult {
    pub http: HttpUpload,
    // `None` while torrents are off.
    pub peers: Option<super::PeerUpload>,
}

// Bytes one leg moved in one second of the measurement.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample {
    pub at_ms: u64,
    pub leg: usize,
    pub bytes: u64,
    // Decoded bytes the leg produced in that second; Usenet only.
    pub decoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpeedTestResult {
    pub bytes: u64,
    pub elapsed_ms: u64,
    pub average_bps: f64,
    pub peak_bps: f64,
    pub legs: Vec<LegResult>,
    pub connections: Vec<ConnectionResult>,
    pub usenet: Option<UsenetResult>,
    pub bandwidth: Option<BandwidthResult>,
    pub upload: Option<UploadResult>,
    pub samples: Vec<Sample>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpeedTestRun {
    pub id: u32,
    pub consumer: Consumer,
    pub consumer_name: String,
    pub kind: SpeedTestKindLabel,
    pub request: SpeedTestRequest,
    pub status: SpeedTestStatus,
    pub phase: Option<SpeedTestPhase>,
    // Articles still in flight while the run drains.
    pub draining: Option<usize>,
    // Epoch milliseconds.
    pub started_at_ms: i64,
    pub finished_at_ms: Option<i64>,
    // Why a failed run failed.
    pub error: Option<SpeedTestFailure>,
    pub tested: TestedRouteSnapshot,
    // Partial while the run measures; absent when it never reached measuring.
    pub result: Option<SpeedTestResult>,
}

impl SpeedTestRun {
    // "preparing", "pausing", "draining N", "leasing", "measuring" or
    // "releasing" while running; `None` once finished.
    pub fn phase_label(&self) -> Option<String> {
        if self.status != SpeedTestStatus::Running {
            return None;
        }
        Some(match self.phase? {
            SpeedTestPhase::Preparing => "preparing".into(),
            SpeedTestPhase::Pausing => "pausing".into(),
            SpeedTestPhase::Draining => format!("draining {}", self.draining.unwrap_or(0)),
            SpeedTestPhase::Leasing => "leasing".into(),
            SpeedTestPhase::Measuring => "measuring".into(),
            SpeedTestPhase::Uploading => "uploading".into(),
            SpeedTestPhase::Releasing => "releasing".into(),
        })
    }
}
