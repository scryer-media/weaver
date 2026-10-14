// Why a run that started failed, or why one of its connections ended. A
// cause of its own is a variant a client words itself; `Display` is the
// daemon's own account, naming the URL, host or status, for logs and for a
// cause a client has no words for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpeedTestFailure {
    // The provider answered HTTP 429: it is rate limiting this address.
    RateLimited {
        url: String,
    },
    // The provider answered another status that is not a success.
    HttpStatus {
        url: String,
        status: u16,
    },
    TlsVerification {
        host: String,
    },
    // The host did not resolve through the leg; `reason` is the resolver's,
    // absent when it answered with no address.
    Unresolved {
        host: String,
        reason: Option<String>,
    },
    // The leg's proxies have no DNS servers to resolve the host through.
    NoDnsServers {
        host: String,
    },
    // The host resolves to an address the egress block rules refuse.
    AddressNotAllowed {
        host: String,
    },
    EgressQuotaUsed {
        egress: u32,
    },
    NoLegUp,
    // The live pool would not lend the server's connections.
    LeaseRefused {
        server: u32,
        name: String,
        reason: String,
    },
    ServerLeftPool,
    // Stopped from outside the run, as when Weaver shuts down.
    Stopped,
    // Stopped on a fault of its own.
    StoppedUnexpectedly,
    // Nothing was received, and no connection said why.
    NoBytes,
    // Any other cause, in the daemon's words.
    Other(String),
}

impl SpeedTestFailure {
    // A GET of `url` answered with `status`, which is not a success.
    pub(crate) fn answered(url: &reqwest::Url, status: reqwest::StatusCode) -> Self {
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            Self::RateLimited {
                url: url.to_string(),
            }
        } else {
            Self::HttpStatus {
                url: url.to_string(),
                status: status.as_u16(),
            }
        }
    }

    // The values a client's sentence for the failure may name.
    pub fn params(&self) -> serde_json::Map<String, serde_json::Value> {
        let value = match self {
            Self::RateLimited { url } => serde_json::json!({ "url": url }),
            Self::HttpStatus { url, status } => {
                serde_json::json!({ "url": url, "status": status })
            }
            Self::TlsVerification { host }
            | Self::Unresolved { host, .. }
            | Self::NoDnsServers { host }
            | Self::AddressNotAllowed { host } => serde_json::json!({ "host": host }),
            Self::EgressQuotaUsed { egress } => serde_json::json!({ "egressId": egress }),
            Self::LeaseRefused {
                server,
                name,
                reason,
            } => serde_json::json!({ "serverId": server, "name": name, "reason": reason }),
            _ => return serde_json::Map::new(),
        };
        match value {
            serde_json::Value::Object(params) => params,
            _ => serde_json::Map::new(),
        }
    }
}

// A status as an HTTP status line words it: "429 Too Many Requests".
fn status_line(status: u16) -> String {
    reqwest::StatusCode::from_u16(status)
        .map(|status| status.to_string())
        .unwrap_or_else(|_| status.to_string())
}

impl std::fmt::Display for SpeedTestFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RateLimited { url } => write!(
                f,
                "GET {url} answered HTTP {}",
                reqwest::StatusCode::TOO_MANY_REQUESTS
            ),
            Self::HttpStatus { url, status } => {
                write!(f, "GET {url} answered HTTP {}", status_line(*status))
            }
            Self::TlsVerification { host } => write!(f, "TLS verification failed for {host}"),
            Self::Unresolved {
                host,
                reason: Some(reason),
            } => write!(f, "could not resolve {host}: {reason}"),
            Self::Unresolved { host, reason: None } => write!(f, "no address resolved for {host}"),
            Self::NoDnsServers { host } => write!(
                f,
                "the leg's proxies have no DNS servers, so {host} cannot be resolved through the route"
            ),
            Self::AddressNotAllowed { host } => {
                write!(f, "{host} resolves to an address that is not allowed")
            }
            Self::EgressQuotaUsed { egress } => {
                write!(f, "egress {egress} has used its download quota")
            }
            Self::NoLegUp => f.write_str("no leg is up"),
            Self::LeaseRefused { name, reason, .. } => {
                write!(f, "server {name} cannot lend its connections: {reason}")
            }
            Self::ServerLeftPool => f.write_str("the server left the live pool"),
            Self::Stopped => f.write_str("the speed test was stopped"),
            Self::StoppedUnexpectedly => f.write_str("the speed test stopped unexpectedly"),
            Self::NoBytes => f.write_str("no bytes were received"),
            Self::Other(detail) => f.write_str(detail),
        }
    }
}

impl std::error::Error for SpeedTestFailure {}

// A cause with no variant of its own is kept in the daemon's words.
impl From<String> for SpeedTestFailure {
    fn from(detail: String) -> Self {
        Self::Other(detail)
    }
}

impl From<&str> for SpeedTestFailure {
    fn from(detail: &str) -> Self {
        Self::Other(detail.to_owned())
    }
}
