//! HTTP request counters and latency, keyed by **route template**.
//!
//! Raw request paths are never used as a label: `/api/jobs/{job_id}/nzb` would
//! otherwise mint one time series per job and blow the exporter's cardinality
//! open. Every request is classified into a small closed set of templates, and
//! anything unrecognised lands in `other`.
//!
//! Storage is a fixed array of atomics indexed by that route enum, so the
//! middleware never allocates, never locks and never hashes.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;

use weaver_server_core::operations::instrumentation::{
    AtomicHistogram, HTTP_REQUEST_DURATION_BOUNDS, HttpMetricsSnapshot, HttpRequestCount,
};

/// Route templates the exporter reports. Closed set by design.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RouteLabel {
    Graphql,
    GraphqlWs,
    JsonRpc,
    XmlRpc,
    Metrics,
    JobNzb,
    JobOutputFile,
    Backup,
    Login,
    Logout,
    AuthStatus,
    Static,
    Other,
}

impl RouteLabel {
    pub(crate) const ALL: [Self; 13] = [
        Self::Graphql,
        Self::GraphqlWs,
        Self::JsonRpc,
        Self::XmlRpc,
        Self::Metrics,
        Self::JobNzb,
        Self::JobOutputFile,
        Self::Backup,
        Self::Login,
        Self::Logout,
        Self::AuthStatus,
        Self::Static,
        Self::Other,
    ];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Graphql => "/graphql",
            Self::GraphqlWs => "/graphql/ws",
            Self::JsonRpc => "/jsonrpc",
            Self::XmlRpc => "/xmlrpc",
            Self::Metrics => "/metrics",
            Self::JobNzb => "/api/jobs/{job_id}/nzb",
            Self::JobOutputFile => "/api/jobs/{job_id}/output-file",
            Self::Backup => "/api/backup/*",
            Self::Login => "/api/login",
            Self::Logout => "/api/logout",
            Self::AuthStatus => "/api/auth/status",
            Self::Static => "static",
            Self::Other => "other",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::Graphql => 0,
            Self::GraphqlWs => 1,
            Self::JsonRpc => 2,
            Self::XmlRpc => 3,
            Self::Metrics => 4,
            Self::JobNzb => 5,
            Self::JobOutputFile => 6,
            Self::Backup => 7,
            Self::Login => 8,
            Self::Logout => 9,
            Self::AuthStatus => 10,
            Self::Static => 11,
            Self::Other => 12,
        }
    }

    /// Classify a request path into a template.
    ///
    /// `base_url` is stripped first so a deployment served under a prefix
    /// produces the same labels as one served at the root. Job-scoped paths are
    /// matched structurally so the job id never reaches a label.
    pub(crate) fn classify(path: &str, base_url: &str) -> Self {
        let path = if !base_url.is_empty() {
            path.strip_prefix(base_url).unwrap_or(path)
        } else {
            path
        };
        let path = if path.is_empty() { "/" } else { path };

        match path {
            "/graphql" => return Self::Graphql,
            "/graphql/ws" => return Self::GraphqlWs,
            "/jsonrpc" => return Self::JsonRpc,
            "/xmlrpc" => return Self::XmlRpc,
            "/metrics" => return Self::Metrics,
            "/api/login" => return Self::Login,
            "/api/logout" => return Self::Logout,
            "/api/auth/status" => return Self::AuthStatus,
            "/healthz" | "/readyz" => return Self::Other,
            _ => {}
        }

        if path.starts_with("/api/backup") {
            return Self::Backup;
        }
        if let Some(rest) = path.strip_prefix("/api/jobs/") {
            // ".../{job_id}/<leaf>" — the id segment is discarded, never labelled.
            if let Some((_id, leaf)) = rest.split_once('/') {
                return match leaf {
                    "nzb" => Self::JobNzb,
                    "output-file" => Self::JobOutputFile,
                    _ => Self::Other,
                };
            }
            return Self::Other;
        }
        if path.starts_with("/api/") {
            return Self::Other;
        }
        // Everything else is served by the SPA asset fallback.
        Self::Static
    }
}

/// HTTP methods that get their own label. Anything else is folded into `other`
/// so a scanner probing exotic verbs cannot mint series.
const METHODS: [&str; 6] = ["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"];
/// Status codes reported exactly. This is the closed set weaver's own handlers
/// and middleware produce (plus the class boundaries); anything outside it
/// collapses onto its class boundary (`200`, `300`, `400`, `500`) so a scanner
/// cannot mint series, while the codes an operator actually alerts on — 401/403
/// auth, 404, 413 body limit, 421 Host allowlist, 429, 503 RPC gate — stay
/// distinguishable.
const STATUS_CODES: [u16; 23] = [
    101, 200, 201, 204, 206, 300, 301, 302, 304, 400, 401, 403, 404, 405, 408, 413, 415, 421, 429,
    500, 501, 502, 503,
];
const STATUS_SLOTS: usize = STATUS_CODES.len();

fn method_index(method: &str) -> usize {
    METHODS
        .iter()
        .position(|candidate| *candidate == method)
        .unwrap_or(METHODS.len())
}

fn method_label(index: usize) -> &'static str {
    METHODS.get(index).copied().unwrap_or("other")
}

/// Map a status code onto its slot: exact when it is in [`STATUS_CODES`],
/// otherwise the class boundary (`101` stands in for every 1xx, since the
/// GraphQL websocket upgrade is the only informational response weaver sends).
fn status_slot(status: u16) -> usize {
    if let Some(slot) = STATUS_CODES.iter().position(|code| *code == status) {
        return slot;
    }
    let boundary = match status {
        100..=199 => 101,
        200..=299 => 200,
        300..=399 => 300,
        400..=499 => 400,
        _ => 500,
    };
    STATUS_CODES
        .iter()
        .position(|code| *code == boundary)
        .expect("class boundaries are members of STATUS_CODES")
}

fn status_label(slot: usize) -> u16 {
    STATUS_CODES.get(slot).copied().unwrap_or(500)
}

/// Fixed-size HTTP counters. One instance lives for the process.
///
/// Not a per-segment path, but written on every request, so it is built to the
/// same standard: array indexing plus `Relaxed` `fetch_add`s, no allocation and
/// no lock on the request path.
#[derive(Debug)]
pub(crate) struct HttpMetrics {
    /// `[route][method][status_slot]`.
    requests: Vec<AtomicU64>,
    duration: Vec<AtomicHistogram>,
}

impl Default for HttpMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpMetrics {
    const METHOD_SLOTS: usize = METHODS.len() + 1;

    pub(crate) fn new() -> Self {
        let cells = RouteLabel::ALL.len() * Self::METHOD_SLOTS * STATUS_SLOTS;
        Self {
            requests: (0..cells).map(|_| AtomicU64::new(0)).collect(),
            duration: RouteLabel::ALL
                .iter()
                .map(|_| AtomicHistogram::new(HTTP_REQUEST_DURATION_BOUNDS))
                .collect(),
        }
    }

    const fn cell(route: usize, method: usize, status: usize) -> usize {
        (route * Self::METHOD_SLOTS + method) * STATUS_SLOTS + status
    }

    /// Record one completed request. Array indexing plus `Relaxed` adds.
    pub(crate) fn record(
        &self,
        route: RouteLabel,
        method: &str,
        status: u16,
        elapsed: std::time::Duration,
    ) {
        let cell = Self::cell(route.index(), method_index(method), status_slot(status));
        self.requests[cell].fetch_add(1, Ordering::Relaxed);
        self.duration[route.index()].observe(elapsed);
    }

    /// Copy the counters out. Scrape-time only; allocates. Cells that have
    /// never been hit are omitted, but every route keeps a duration series so
    /// the histogram families pre-exist.
    pub(crate) fn snapshot(&self) -> HttpMetricsSnapshot {
        let mut requests = Vec::new();
        for route in RouteLabel::ALL {
            for method in 0..Self::METHOD_SLOTS {
                for status in 0..STATUS_SLOTS {
                    let count = self.requests[Self::cell(route.index(), method, status)]
                        .load(Ordering::Relaxed);
                    if count == 0 {
                        continue;
                    }
                    requests.push(HttpRequestCount {
                        route: route.as_str(),
                        method: method_label(method),
                        status: status_label(status),
                        count,
                    });
                }
            }
        }
        HttpMetricsSnapshot {
            requests,
            duration: RouteLabel::ALL
                .iter()
                .map(|route| (route.as_str(), self.duration[route.index()].snapshot()))
                .collect(),
        }
    }
}

/// Shared handle installed as an axum extension and read by the exporter.
#[derive(Clone, Debug)]
pub(crate) struct HttpMetricsHandle {
    metrics: Arc<HttpMetrics>,
    base_url: Arc<String>,
}

impl HttpMetricsHandle {
    pub(crate) fn new(base_url: String) -> Self {
        Self {
            metrics: Arc::new(HttpMetrics::new()),
            base_url: Arc::new(base_url),
        }
    }

    pub(crate) fn snapshot(&self) -> HttpMetricsSnapshot {
        self.metrics.snapshot()
    }
}

/// Axum middleware recording one observation per request.
///
/// `/metrics` is skipped: counting the scrape in the numbers the scrape returns
/// makes every rate self-referential and tells an operator nothing.
pub(crate) async fn track_requests(
    handle: HttpMetricsHandle,
    request: Request,
    next: Next,
) -> Response {
    let route = RouteLabel::classify(request.uri().path(), &handle.base_url);
    if route == RouteLabel::Metrics {
        return next.run(request).await;
    }
    let method = request.method().clone();
    let started = Instant::now();
    let response = next.run(request).await;
    handle.metrics.record(
        route,
        method.as_str(),
        response.status().as_u16(),
        started.elapsed(),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_routes_classify_to_their_template() {
        assert_eq!(RouteLabel::classify("/graphql", ""), RouteLabel::Graphql);
        assert_eq!(
            RouteLabel::classify("/graphql/ws", ""),
            RouteLabel::GraphqlWs
        );
        assert_eq!(RouteLabel::classify("/jsonrpc", ""), RouteLabel::JsonRpc);
        assert_eq!(RouteLabel::classify("/xmlrpc", ""), RouteLabel::XmlRpc);
        assert_eq!(RouteLabel::classify("/metrics", ""), RouteLabel::Metrics);
        assert_eq!(RouteLabel::classify("/api/login", ""), RouteLabel::Login);
        assert_eq!(
            RouteLabel::classify("/api/backup/export", ""),
            RouteLabel::Backup
        );
    }

    #[test]
    fn job_ids_never_reach_a_label() {
        assert_eq!(
            RouteLabel::classify("/api/jobs/10184/nzb", ""),
            RouteLabel::JobNzb
        );
        assert_eq!(
            RouteLabel::classify("/api/jobs/99999999/output-file", ""),
            RouteLabel::JobOutputFile
        );
        // Two different ids must not produce two labels.
        assert_eq!(
            RouteLabel::classify("/api/jobs/1/nzb", "").as_str(),
            RouteLabel::classify("/api/jobs/2/nzb", "").as_str()
        );
        assert!(!RouteLabel::JobNzb.as_str().contains("10184"));
    }

    #[test]
    fn unknown_paths_fall_back_without_minting_series() {
        assert_eq!(RouteLabel::classify("/api/unknown", ""), RouteLabel::Other);
        assert_eq!(RouteLabel::classify("/api/jobs/5", ""), RouteLabel::Other);
        assert_eq!(RouteLabel::classify("/healthz", ""), RouteLabel::Other);
        assert_eq!(
            RouteLabel::classify("/assets/index-abc123.js", ""),
            RouteLabel::Static
        );
        assert_eq!(RouteLabel::classify("/", ""), RouteLabel::Static);
    }

    #[test]
    fn a_base_url_prefix_is_stripped_before_classification() {
        assert_eq!(
            RouteLabel::classify("/weaver/graphql", "/weaver"),
            RouteLabel::Graphql
        );
        assert_eq!(
            RouteLabel::classify("/weaver/api/jobs/7/nzb", "/weaver"),
            RouteLabel::JobNzb
        );
        assert_eq!(
            RouteLabel::classify("/weaver", "/weaver"),
            RouteLabel::Static
        );
    }

    #[test]
    fn recorded_requests_land_in_their_own_cell() {
        let metrics = HttpMetrics::new();
        metrics.record(
            RouteLabel::Graphql,
            "POST",
            200,
            std::time::Duration::from_millis(12),
        );
        metrics.record(
            RouteLabel::Graphql,
            "POST",
            200,
            std::time::Duration::from_millis(8),
        );
        metrics.record(
            RouteLabel::Login,
            "POST",
            401,
            std::time::Duration::from_millis(3),
        );

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.requests.len(), 2, "only touched cells are emitted");
        let graphql = snapshot
            .requests
            .iter()
            .find(|row| row.route == "/graphql")
            .expect("graphql row");
        assert_eq!(graphql.method, "POST");
        assert_eq!(graphql.status, 200);
        assert_eq!(graphql.count, 2);

        let login = snapshot
            .requests
            .iter()
            .find(|row| row.route == "/api/login")
            .expect("login row");
        assert_eq!(login.status, 401);
        assert_eq!(login.count, 1);

        assert_eq!(snapshot.duration.len(), RouteLabel::ALL.len());
        let graphql_duration = snapshot
            .duration
            .iter()
            .find(|(route, _)| *route == "/graphql")
            .expect("graphql duration");
        assert_eq!(graphql_duration.1.count, 2);
    }

    #[test]
    fn exotic_methods_and_statuses_collapse_instead_of_expanding() {
        let metrics = HttpMetrics::new();
        metrics.record(
            RouteLabel::Other,
            "PROPFIND",
            418,
            std::time::Duration::from_millis(1),
        );
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.requests.len(), 1);
        assert_eq!(snapshot.requests[0].method, "other");
        assert_eq!(snapshot.requests[0].status, 400, "418 collapses onto 4xx");
    }

    #[test]
    fn operationally_relevant_statuses_keep_their_exact_code() {
        let metrics = HttpMetrics::new();
        for status in [421u16, 413, 503, 401, 404, 304] {
            metrics.record(
                RouteLabel::JsonRpc,
                "POST",
                status,
                std::time::Duration::from_millis(1),
            );
        }
        let snapshot = metrics.snapshot();
        let mut seen: Vec<u16> = snapshot.requests.iter().map(|row| row.status).collect();
        seen.sort_unstable();
        assert_eq!(seen, vec![304, 401, 404, 413, 421, 503]);
        // Unknown codes land on their class boundary, never on a neighbour.
        assert_eq!(status_label(status_slot(599)), 500);
        assert_eq!(status_label(status_slot(299)), 200);
        assert_eq!(status_label(status_slot(101)), 101, "websocket upgrade");
        assert_eq!(status_label(status_slot(103)), 101);
    }
}
