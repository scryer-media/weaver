use std::collections::HashSet;
use std::net::SocketAddr;

use async_graphql::{Enum, Error, ErrorExtensions, InputObject, SimpleObject};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum ApiKeyScope {
    Read,
    Control,
    Admin,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct ApiKey {
    pub id: i64,
    pub name: String,
    pub scope: ApiKeyScope,
    pub created_at: f64,
    pub last_used_at: Option<f64>,
}

#[derive(Debug, Clone, SimpleObject)]
pub struct CreateApiKeyResult {
    pub key: ApiKey,
    pub raw_key: String,
}

#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct LoginStatusResult {
    pub enabled: bool,
    pub username: Option<String>,
}

/// Which layer decided the address Weaver is listening on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, async_graphql::Enum)]
pub enum HttpBindAddressSource {
    /// `WEAVER_HTTP_BIND_ADDRESS` is set and wins; the field is read-only.
    Environment,
    /// Chosen in this UI and stored in the settings table.
    Setting,
    /// Nothing configured; loopback.
    Default,
}

impl From<weaver_server_core::security::BindAddressSource> for HttpBindAddressSource {
    fn from(source: weaver_server_core::security::BindAddressSource) -> Self {
        use weaver_server_core::security::BindAddressSource as Core;
        match source {
            Core::Environment => Self::Environment,
            Core::Setting => Self::Setting,
            Core::Default => Self::Default,
        }
    }
}

/// The listening address, where it came from, and whether that combination
/// leaves the admin interface reachable without a login.
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct HttpBindAddressStatus {
    /// The address in force for the running process.
    pub address: String,
    /// The stored setting, which differs from `address` only when the
    /// environment is overriding it or a change is awaiting a restart.
    pub stored_address: Option<String>,
    pub source: HttpBindAddressSource,
    /// False when the environment pins the address, in which case editing it
    /// here would be ignored.
    pub editable: bool,
    /// True when the address the NEXT restart will use is reachable beyond
    /// this machine while no login is configured — evaluated against the
    /// pending choice, so the warning fires when the decision is made rather
    /// than after the restart that already exposed the instance.
    pub exposed_without_login: bool,
    /// True when the address the next restart will use differs from the
    /// running one — including a cleared setting falling back to loopback.
    pub restart_required: bool,
    /// Set when the process could not bind its configured address and is
    /// serving on loopback instead; the reason, verbatim, for a banner.
    pub bind_fallback: Option<String>,
}

/// Compute what the next restart binds and whether that differs from now.
///
/// Parsed comparison, never strings: `0:0:0:0:0:0:0:1` and `::1` are the same
/// address, and a stored spelling must not read as an eternal pending restart.
/// A cleared setting (stored `None`) pends a return to the default, which is a
/// restart-worthy difference when the process is currently bound wider.
pub(crate) fn pending_bind_state(
    running: std::net::IpAddr,
    stored: Option<&str>,
    editable: bool,
) -> (std::net::IpAddr, bool) {
    if !editable {
        // The environment re-answers identically on every start; a stored
        // value is inert rather than pending.
        return (running, false);
    }
    let pending = stored
        .and_then(|value| value.trim().parse::<std::net::IpAddr>().ok())
        .unwrap_or(weaver_server_core::security::DEFAULT_HTTP_BIND_ADDRESS);
    (pending, pending != running)
}

/// Whether the address the next restart binds leaves the admin interface
/// reachable beyond this machine with no login configured.
///
/// Loopback is judged canonically, so an IPv4-mapped `::ffff:127.0.0.1` reads
/// as this machine rather than as an exposed binding.
pub(crate) fn exposed_without_login(login_enabled: bool, pending: std::net::IpAddr) -> bool {
    !login_enabled && !weaver_server_core::security::ip_is_loopback(pending)
}

/// The browser-admission policy: mode, networks, and whether the UI may edit
/// it (`WEAVER_TRUSTED_CIDRS` pins it read-only, like the bind variable).
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct AccessPolicyStatus {
    /// `login_required`, `login_except_local`, or `no_login`.
    pub mode: String,
    /// The trusted networks in force right now (live — policy edits apply
    /// immediately, unlike the bind address).
    pub trusted_networks: Vec<String>,
    pub editable: bool,
    /// True when the environment supplied the list.
    pub env_pinned: bool,
    /// True once an access mode has actually been stored and understood.
    ///
    /// Distinguishes "the operator chose login-required" from "nothing is
    /// stored and the query defaulted to login-required", which is what an
    /// install upgraded from a version without these settings looks like. The
    /// UI uses it to offer the security wizard exactly once.
    pub configured: bool,
    /// `WEAVER_STRICT_SECURITY` is on, so every trusting mode will be refused.
    /// Exposed so the UI can disable those choices with the real reason rather
    /// than letting the operator pick one and fail on submit.
    pub strict_security: bool,
}

/// Request evidence made available to the network-access resolver by the HTTP
/// transport. It is deliberately optional: schema-only callers cannot invent
/// a peer or forwarding chain, and the resolver reports that absence instead
/// of presenting a guess as a network diagnostic.
#[derive(Clone)]
pub struct NetworkRequestSecurityContext {
    pub peer: Option<SocketAddr>,
    pub headers: axum::http::HeaderMap,
}

/// The source controlling the trusted-network list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum NetworkAccessSource {
    Environment,
    Stored,
}

/// The observed request identity used for remembered-browser admission.
#[derive(Debug, Clone, SimpleObject)]
pub struct NetworkClientDiagnostics {
    /// False when the transport did not attach request evidence to GraphQL.
    pub available: bool,
    pub peer: Option<String>,
    pub resolved_client: Option<String>,
    pub forwarding_headers_ignored: bool,
    /// `None` when no request evidence was available.
    pub remembered_client_allowed: Option<bool>,
}

/// Complete, effective browser network policy with the running and stored
/// listener state nested in one admin-only query.
#[derive(Debug, Clone, SimpleObject)]
pub struct NetworkAccessStatus {
    pub proxies_editable: bool,
    pub proxies_env_pinned: bool,
    pub proxies_source: NetworkAccessSource,
    pub authenticated_access: bool,
    pub legacy_compatibility: bool,
    pub trusted_networks: Vec<String>,
    /// Explicit `WEAVER_TRUSTED_PROXIES` entries only; the service never
    /// discovers or infers proxies from a private network.
    pub trusted_proxies: Vec<String>,
    pub trusted_networks_source: NetworkAccessSource,
    pub editable: bool,
    pub env_pinned: bool,
    pub remembered_policy_valid: bool,
    pub current_client: NetworkClientDiagnostics,
    pub bind_address: HttpBindAddressStatus,
}

/// A draft for validating the two network settings before a later mutation
/// persists either one. An empty trusted-network list permits remembered
/// sessions from any resolvable client in authenticated access mode.
#[derive(Debug, Clone, InputObject)]
pub struct NetworkAccessInput {
    /// Omitted preserves the current proxy list; an empty list trusts no proxy.
    pub trusted_proxies: Option<Vec<String>>,
    pub trusted_networks: Vec<String>,
    pub bind_address: Option<String>,
}

/// Normalized, non-persistent result of [`NetworkAccessInput`] validation.
#[derive(Debug, Clone, SimpleObject)]
pub struct NetworkAccessPreview {
    pub trusted_proxies: Vec<String>,
    pub trusted_networks: Vec<String>,
    /// `None` means the stored bind setting would be cleared to its loopback
    /// default on restart.
    pub bind_address: Option<String>,
    /// Whether the submitted bind setting differs from the running listener.
    pub restart_required: bool,
    /// How the current observed client would fare under the submitted list.
    /// `None` means this GraphQL request had no transport evidence attached.
    pub current_client_allowed: Option<bool>,
}

pub(crate) fn network_client_diagnostics(
    security: &weaver_server_core::security::RuntimeSecurityConfig,
    request: Option<&NetworkRequestSecurityContext>,
) -> NetworkClientDiagnostics {
    let Some(request) = request else {
        return NetworkClientDiagnostics {
            available: false,
            peer: None,
            resolved_client: None,
            forwarding_headers_ignored: false,
            remembered_client_allowed: None,
        };
    };
    let resolved_client = security.resolve_client_ip(request.peer, &request.headers);
    NetworkClientDiagnostics {
        available: true,
        peer: request.peer.map(|peer| peer.ip().to_string()),
        resolved_client: resolved_client.map(|ip| ip.to_string()),
        forwarding_headers_ignored: security
            .forwarding_headers_ignored(request.peer, &request.headers),
        remembered_client_allowed: Some(
            security.remembered_client_allowed(request.peer, &request.headers),
        ),
    }
}

pub(crate) fn preview_network_access(
    input: &NetworkAccessInput,
    security: &weaver_server_core::security::RuntimeSecurityConfig,
    bind_status: &HttpBindAddressStatus,
    request: Option<&NetworkRequestSecurityContext>,
) -> Result<NetworkAccessPreview, Error> {
    let normalized_networks = normalize_trusted_networks(&input.trusted_networks)?;
    let normalized_proxies = input
        .trusted_proxies
        .as_ref()
        .map(|entries| {
            entries
                .iter()
                .map(|entry| {
                    weaver_server_core::security::parse_ip_or_cidr(entry)
                        .map(|network| network.trunc().to_string())
                        .ok_or_else(|| {
                            network_access_error(
                                "INVALID_TRUSTED_PROXIES",
                                format!("Invalid proxy address or CIDR: {entry}"),
                            )
                        })
                })
                .collect::<Result<Vec<_>, Error>>()
        })
        .transpose()?;
    let trusted_networks = if security.trust_env_pinned {
        security
            .trusted_cidrs()
            .iter()
            .map(ToString::to_string)
            .collect()
    } else {
        normalized_networks
    };
    let trusted_proxies: Vec<String> = if security.proxies_env_pinned() {
        security
            .trusted_proxies()
            .iter()
            .map(ToString::to_string)
            .collect()
    } else {
        normalized_proxies.unwrap_or_else(|| {
            security
                .trusted_proxies()
                .iter()
                .map(ToString::to_string)
                .collect()
        })
    };
    let mut seen_proxies = HashSet::new();
    if trusted_proxies
        .iter()
        .any(|proxy| !seen_proxies.insert(proxy))
    {
        return Err(network_access_error(
            "DUPLICATE_TRUSTED_PROXY",
            "Trusted proxies contains duplicate addresses",
        ));
    }
    let bind_address = normalize_bind_address(input.bind_address.as_deref())?;
    let (_, restart_required) = pending_bind_state(
        security.http_bind_address,
        if input.bind_address.is_some() {
            bind_address.as_deref()
        } else {
            bind_status.stored_address.as_deref()
        },
        bind_status.editable,
    );

    let current_client_allowed = request.map(|request| {
        let draft_networks = trusted_networks
            .iter()
            .filter_map(|network| weaver_server_core::security::parse_ip_or_cidr(network))
            .collect::<Vec<_>>();
        let draft_proxies = trusted_proxies
            .iter()
            .filter_map(|proxy| weaver_server_core::security::parse_ip_or_cidr(proxy))
            .collect::<Vec<_>>();
        security
            .resolve_client_ip_with_proxies(request.peer, &request.headers, &draft_proxies)
            .is_some_and(|client| {
                draft_networks.is_empty()
                    || draft_networks
                        .iter()
                        .any(|network| network.contains(&client))
            })
    });

    Ok(NetworkAccessPreview {
        trusted_proxies,
        trusted_networks,
        bind_address,
        restart_required,
        current_client_allowed,
    })
}

fn normalize_trusted_networks(values: &[String]) -> Result<Vec<String>, Error> {
    let json = serde_json::to_string(values)
        .map_err(|error| network_access_error("INVALID_TRUSTED_NETWORKS", error.to_string()))?;
    let parsed = weaver_server_core::security::parse_trusted_networks_json(&json)
        .map_err(|error| network_access_error("INVALID_TRUSTED_NETWORKS", error.to_string()))?;
    let mut seen = HashSet::new();
    let mut normalized = Vec::with_capacity(parsed.len());
    for network in parsed {
        let network = network.trunc().to_string();
        if !seen.insert(network.clone()) {
            return Err(network_access_error(
                "DUPLICATE_TRUSTED_NETWORK",
                format!("trusted networks contains duplicate CIDR {network:?}"),
            ));
        }
        normalized.push(network);
    }
    Ok(normalized)
}

fn normalize_bind_address(value: Option<&str>) -> Result<Option<String>, Error> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    let (address, _) = weaver_server_core::security::resolve_bind_address(None, Some(value))
        .map_err(|error| network_access_error("INVALID_BIND_ADDRESS", error.to_string()))?;
    Ok(Some(address.to_string()))
}

fn network_access_error(code: &'static str, message: impl Into<String>) -> Error {
    Error::new(message.into()).extend_with(|_, extensions| {
        extensions.set("code", code);
    })
}

#[cfg(test)]
mod pending_bind_tests {
    use super::{exposed_without_login, pending_bind_state};
    use std::net::IpAddr;

    fn ip(value: &str) -> IpAddr {
        value.parse().unwrap()
    }

    #[test]
    fn equivalent_spellings_are_not_a_pending_restart() {
        // The S6 shape: a non-canonical spelling of the running address.
        let (pending, restart) = pending_bind_state(ip("::1"), Some("0:0:0:0:0:0:0:1"), true);
        assert_eq!(pending, ip("::1"));
        assert!(!restart, "same address in another spelling is not pending");
    }

    #[test]
    fn a_cleared_setting_pends_the_default() {
        // The S5 shape: clearing while running wide must report a restart.
        let (pending, restart) = pending_bind_state(ip("0.0.0.0"), None, true);
        assert_eq!(pending, ip("127.0.0.1"));
        assert!(restart, "clearing while bound wide pends a change");

        // And clearing while already on the default pends nothing.
        let (_, restart) = pending_bind_state(ip("127.0.0.1"), None, true);
        assert!(!restart);
    }

    #[test]
    fn exposure_judges_loopback_canonically() {
        // The S7 shape: the mapped spelling of loopback is still this machine.
        assert!(!exposed_without_login(false, ip("::ffff:127.0.0.1")));
        assert!(!exposed_without_login(false, ip("127.0.0.1")));
        assert!(!exposed_without_login(false, ip("::1")));

        // A mapped routable address is exposed, and a login clears it either way.
        assert!(exposed_without_login(false, ip("::ffff:192.168.1.5")));
        assert!(exposed_without_login(false, ip("0.0.0.0")));
        assert!(!exposed_without_login(true, ip("0.0.0.0")));
    }

    #[test]
    fn an_environment_pin_never_pends() {
        let (pending, restart) = pending_bind_state(ip("0.0.0.0"), Some("127.0.0.1"), false);
        assert_eq!(pending, ip("0.0.0.0"));
        assert!(!restart, "a stored value is inert under an environment pin");
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CallerIdentity {
    Local([u8; 32]),
    Jwt([u8; 32]),
    ApiKey([u8; 32]),
}
