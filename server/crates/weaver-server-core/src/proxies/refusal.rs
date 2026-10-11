use crate::persistence::Refusal;
use crate::servers::DownloadQuotaFault;

// Why a networking edit was refused. Each variant is one sentence; `code`
// names the condition and is part of the API, so a client words the refusal
// itself by matching the code. Two variants share a code where the same
// condition is found at two places that word it differently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkRefusal {
    // Egress interfaces.
    EgressNameLength,
    EgressSystemImmutable,
    EgressSystemBindingReserved,
    EgressInterfaceNameInvalid,
    EgressInterfaceNameRequired,
    EgressInterfaceBindingUnsupported,
    EgressSourceAddressRequired,
    EgressSourceAddressInvalid,
    EgressSourceAddressNotUnicast,
    EgressSpeedLimitTooLarge,
    EgressQuota(DownloadQuotaFault),
    EgressGone,
    EgressNotFound,
    EgressSystemUndeletable,
    EgressInUse,

    // Proxy pools.
    PoolNameLength,
    PoolMemberCount,
    PoolProxyRepeated,
    PoolProxyMissing,
    PoolMemberKindMismatch,
    PoolGone,
    PoolInUse,
    PoolEditRepeatsProxy,

    // Routes.
    RouteLegCount,
    RouteLegWeightRange,
    RouteLegWeightSum,
    RouteLadderRungCount,
    RouteChainLength,
    RouteDirectPathFalse,
    RouteAndRoutingBoth,
    RouteEgressMissing,
    RouteEgressDisabled,
    RoutePoolMissing,
    RouteProxyMissing,
    RoutingProxyMissing,
    RouteProxyRepeated,
    RoutingProxyRepeated,
    RoutingProxyCount,
    RouteChainTransport,
    RouteRssProxyNeedsDns,
    RssRouteProxyNeedsDns,
    RouteHasAdvancedLegs,
    RouteConsumerGone,

    // WireGuard sessions across every route.
    WireGuardPathConflict {
        proxy: u32,
        egress: u32,
        paths: Vec<Vec<u32>>,
    },
    WireGuardInstanceLimit {
        required: usize,
        limit: usize,
    },

    // Proxy profiles.
    ProxyNameLength,
    ProxyEndpointInvalid,
    ProxyTimeoutRange,
    ProxyAddressCount,
    ProxyDnsServerInvalid,
    ProxyCredentialTooLarge,
    ProxySshUsernameRequired,
    ProxySshKeyRequired,
    ProxySshPasswordUnsupported,
    ProxySocks5PasswordNeedsUsername,
    ProxySocks5CredentialLength,
    ProxyHttpUsernameColon,
    ProxyHttp3PasswordNeedsUsername,
    // What the tunnel engine found wrong with a profile, in its own words:
    // an unusable key, tunnel address, or HTTP/3 authority.
    ProxyTunnel(String),
    ProxyKindImmutable,
    ProxyKindLockedByPool,
    ProxyLimitReached,
    ProxyRevisionChanged,
    ProxyGone,
    ProxyNotFound,
    ProxyInPool,
    ProxyInUse,
    ProxyHostKeyNotSsh,
}

impl NetworkRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::EgressNameLength => "EGRESS_NAME_LENGTH",
            Self::EgressSystemImmutable => "EGRESS_SYSTEM_IMMUTABLE",
            Self::EgressSystemBindingReserved => "EGRESS_SYSTEM_BINDING_RESERVED",
            Self::EgressInterfaceNameInvalid => "EGRESS_INTERFACE_NAME_INVALID",
            Self::EgressInterfaceNameRequired => "EGRESS_INTERFACE_NAME_REQUIRED",
            Self::EgressInterfaceBindingUnsupported => "EGRESS_INTERFACE_BINDING_UNSUPPORTED",
            Self::EgressSourceAddressRequired => "EGRESS_SOURCE_ADDRESS_REQUIRED",
            Self::EgressSourceAddressInvalid => "EGRESS_SOURCE_ADDRESS_INVALID",
            Self::EgressSourceAddressNotUnicast => "EGRESS_SOURCE_ADDRESS_NOT_UNICAST",
            Self::EgressSpeedLimitTooLarge => "EGRESS_SPEED_LIMIT_TOO_LARGE",
            Self::EgressQuota(DownloadQuotaFault::LimitTooLarge) => "EGRESS_QUOTA_LIMIT_TOO_LARGE",
            Self::EgressQuota(DownloadQuotaFault::LimitRequired) => "EGRESS_QUOTA_LIMIT_REQUIRED",
            Self::EgressQuota(DownloadQuotaFault::ResetTimeRange) => {
                "EGRESS_QUOTA_RESET_TIME_RANGE"
            }
            Self::EgressQuota(DownloadQuotaFault::MonthlyResetDayRange) => {
                "EGRESS_QUOTA_RESET_DAY_RANGE"
            }
            Self::EgressGone | Self::EgressNotFound => "EGRESS_NOT_FOUND",
            Self::EgressSystemUndeletable => "EGRESS_SYSTEM_UNDELETABLE",
            Self::EgressInUse => "EGRESS_IN_USE",

            Self::PoolNameLength => "POOL_NAME_LENGTH",
            Self::PoolMemberCount => "POOL_MEMBER_COUNT",
            Self::PoolProxyRepeated => "POOL_PROXY_REPEATED",
            Self::PoolProxyMissing => "POOL_PROXY_MISSING",
            Self::PoolMemberKindMismatch => "POOL_MEMBER_KIND_MISMATCH",
            Self::PoolGone => "POOL_NOT_FOUND",
            Self::PoolInUse => "POOL_IN_USE",
            Self::PoolEditRepeatsProxy => "POOL_EDIT_REPEATS_PROXY",

            Self::RouteLegCount => "ROUTE_LEG_COUNT",
            Self::RouteLegWeightRange => "ROUTE_LEG_WEIGHT_RANGE",
            Self::RouteLegWeightSum => "ROUTE_LEG_WEIGHT_SUM",
            Self::RouteLadderRungCount => "ROUTE_LADDER_RUNG_COUNT",
            Self::RouteChainLength => "ROUTE_CHAIN_LENGTH",
            Self::RouteDirectPathFalse => "ROUTE_DIRECT_PATH_FALSE",
            Self::RouteAndRoutingBoth => "ROUTE_AND_ROUTING_BOTH",
            Self::RouteEgressMissing => "ROUTE_EGRESS_MISSING",
            Self::RouteEgressDisabled => "ROUTE_EGRESS_DISABLED",
            Self::RoutePoolMissing => "ROUTE_POOL_MISSING",
            Self::RouteProxyMissing | Self::RoutingProxyMissing => "ROUTE_PROXY_MISSING",
            Self::RouteProxyRepeated | Self::RoutingProxyRepeated => "ROUTE_PROXY_REPEATED",
            Self::RoutingProxyCount => "ROUTE_PROXY_COUNT",
            Self::RouteChainTransport => "ROUTE_CHAIN_TRANSPORT",
            Self::RouteRssProxyNeedsDns | Self::RssRouteProxyNeedsDns => {
                "ROUTE_RSS_PROXY_NEEDS_DNS"
            }
            Self::RouteHasAdvancedLegs => "ROUTE_HAS_ADVANCED_LEGS",
            Self::RouteConsumerGone => "ROUTE_CONSUMER_NOT_FOUND",

            Self::WireGuardPathConflict { .. } => "WIREGUARD_PROXY_PATH_CONFLICT",
            Self::WireGuardInstanceLimit { .. } => "WIREGUARD_INSTANCE_LIMIT",

            Self::ProxyNameLength => "PROXY_NAME_LENGTH",
            Self::ProxyEndpointInvalid => "PROXY_ENDPOINT_INVALID",
            Self::ProxyTimeoutRange => "PROXY_TIMEOUT_RANGE",
            Self::ProxyAddressCount => "PROXY_ADDRESS_COUNT",
            Self::ProxyDnsServerInvalid => "PROXY_DNS_SERVER_INVALID",
            Self::ProxyCredentialTooLarge => "PROXY_CREDENTIAL_TOO_LARGE",
            Self::ProxySshUsernameRequired => "PROXY_SSH_USERNAME_REQUIRED",
            Self::ProxySshKeyRequired => "PROXY_SSH_KEY_REQUIRED",
            Self::ProxySshPasswordUnsupported => "PROXY_SSH_PASSWORD_UNSUPPORTED",
            Self::ProxySocks5PasswordNeedsUsername => "PROXY_SOCKS5_PASSWORD_NEEDS_USERNAME",
            Self::ProxySocks5CredentialLength => "PROXY_SOCKS5_CREDENTIAL_LENGTH",
            Self::ProxyHttpUsernameColon => "PROXY_HTTP_USERNAME_COLON",
            Self::ProxyHttp3PasswordNeedsUsername => "PROXY_HTTP3_PASSWORD_NEEDS_USERNAME",
            Self::ProxyTunnel(_) => "PROXY_TUNNEL_CONFIGURATION",
            Self::ProxyKindImmutable => "PROXY_KIND_IMMUTABLE",
            Self::ProxyKindLockedByPool => "PROXY_KIND_LOCKED_BY_POOL",
            Self::ProxyLimitReached => "PROXY_LIMIT_REACHED",
            Self::ProxyRevisionChanged => "PROXY_REVISION_CHANGED",
            Self::ProxyGone | Self::ProxyNotFound => "PROXY_NOT_FOUND",
            Self::ProxyInPool => "PROXY_IN_POOL",
            Self::ProxyInUse => "PROXY_IN_USE",
            Self::ProxyHostKeyNotSsh => "PROXY_HOST_KEY_NOT_SSH",
        }
    }

    // The values a sentence names, for a client that words the refusal itself.
    pub fn params(&self) -> serde_json::Map<String, serde_json::Value> {
        let value = match self {
            Self::WireGuardPathConflict {
                proxy,
                egress,
                paths,
            } => serde_json::json!({ "proxyId": proxy, "egressId": egress, "paths": paths }),
            Self::WireGuardInstanceLimit { required, limit } => serde_json::json!({
                "required": required,
                "limit": limit,
                "instanceMib": super::WIREGUARD_INSTANCE_BYTES / (1024 * 1024),
            }),
            _ => return serde_json::Map::new(),
        };
        match value {
            serde_json::Value::Object(params) => params,
            _ => serde_json::Map::new(),
        }
    }
}

impl std::fmt::Display for NetworkRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sentence = match self {
            Self::EgressNameLength => "egress name must contain 1–128 characters",
            Self::EgressSystemImmutable => {
                "the System egress cannot be renamed, rebound, or disabled"
            }
            Self::EgressSystemBindingReserved => "only egress 0 may use the System binding",
            Self::EgressInterfaceNameInvalid => {
                "interface name must be nonempty and contain no NUL"
            }
            Self::EgressInterfaceNameRequired => "interface name is required",
            Self::EgressInterfaceBindingUnsupported => {
                "Interface binding is not supported on Windows; use a source address"
            }
            Self::EgressSourceAddressRequired => "source address is required",
            Self::EgressSourceAddressInvalid => "invalid source address",
            Self::EgressSourceAddressNotUnicast => "source address must be a unicast host address",
            Self::EgressSpeedLimitTooLarge => "egress speed limit is too large",
            Self::EgressQuota(fault) => return f.write_str(&fault.message("egress")),
            Self::EgressGone => "egress no longer exists",
            Self::EgressNotFound => "egress not found",
            Self::EgressSystemUndeletable => "the System egress cannot be deleted",
            Self::EgressInUse => "egress is referenced by a server or RSS feed",

            Self::PoolNameLength => "pool name must contain 1–128 characters",
            Self::PoolMemberCount => "a pool requires between two and sixteen members",
            Self::PoolProxyRepeated => "a proxy cannot appear twice in a pool",
            Self::PoolProxyMissing => "pool references a missing proxy",
            Self::PoolMemberKindMismatch => "all pool members must have the pool's proxy kind",
            Self::PoolGone => "proxy pool no longer exists",
            Self::PoolInUse => "pool is referenced by a server or RSS feed",
            Self::PoolEditRepeatsProxy => "pool edit would repeat a proxy in an existing ladder",

            Self::RouteLegCount => "a route requires between one and eight legs",
            Self::RouteLegWeightRange => "leg weights must be between 1 and 100",
            Self::RouteLegWeightSum => "leg weights must sum to exactly 100",
            Self::RouteLadderRungCount => "a ladder requires between one and eight rungs",
            Self::RouteChainLength => "a chain requires two or three proxies",
            Self::RouteDirectPathFalse => "a direct path must be true",
            Self::RouteAndRoutingBoth => "specify route or routing, not both",
            Self::RouteEgressMissing => "route references a missing egress",
            Self::RouteEgressDisabled => "route references a disabled egress",
            Self::RoutePoolMissing => "route references a missing pool",
            Self::RouteProxyMissing => "route references a missing proxy",
            Self::RoutingProxyMissing => "routing policy references a missing proxy",
            Self::RouteProxyRepeated => {
                "a proxy cannot appear twice in a ladder, including pools and chains"
            }
            Self::RoutingProxyRepeated => "a proxy cannot appear twice in a ladder",
            Self::RoutingProxyCount => "a route supports at most eight proxies",
            Self::RouteChainTransport => {
                "WireGuard and HTTP/3 must be the first proxy in a chain, directly above the egress; WireGuard may also sit directly on another WireGuard proxy"
            }
            Self::RouteRssProxyNeedsDns => "every RSS proxy and pool member requires DNS servers",
            Self::RssRouteProxyNeedsDns => "RSS proxy routes require configured DNS servers",
            Self::RouteHasAdvancedLegs => {
                "this route has advanced legs; edit it in Networking instead of replacing its legacy projection"
            }
            Self::RouteConsumerGone => "routing consumer no longer exists",

            Self::WireGuardPathConflict {
                proxy,
                egress,
                paths,
            } => {
                let paths = paths
                    .iter()
                    .map(|path| {
                        path.iter()
                            .map(|id| format!("proxy {id}"))
                            .collect::<Vec<_>>()
                            .join(" → ")
                    })
                    .collect::<Vec<_>>()
                    .join(" and ");
                return write!(
                    f,
                    "WireGuard proxy {proxy} is used on egress {egress} by two different paths ({paths}); a WireGuard server accepts one connection per key, so a WireGuard proxy may reach the egress by only one path"
                );
            }
            Self::WireGuardInstanceLimit { required, limit } => {
                return write!(
                    f,
                    "routes require {required} WireGuard instances × 516 MiB = {} MiB; this host permits {limit} instances (available memory / 4 / 516 MiB, clamped to 1–8). Pool members count individually.",
                    required * 516
                );
            }

            Self::ProxyNameLength => "proxy name must contain 1–128 characters",
            Self::ProxyEndpointInvalid => "proxy endpoint must have a valid host and port",
            Self::ProxyTimeoutRange => "proxy timeout must be between 1 and 300 seconds",
            Self::ProxyAddressCount => "too many DNS servers or tunnel addresses",
            Self::ProxyDnsServerInvalid => "DNS servers must be IP addresses",
            Self::ProxyCredentialTooLarge => "proxy credential is too large",
            Self::ProxySshUsernameRequired => "SSH requires a username",
            Self::ProxySshKeyRequired => "SSH requires an Ed25519 private key",
            Self::ProxySshPasswordUnsupported => {
                "SSH requires an Ed25519 private key and takes no password"
            }
            Self::ProxySocks5PasswordNeedsUsername => "SOCKS5 proxy password requires a username",
            Self::ProxySocks5CredentialLength => "SOCKS5 credentials must fit in 255 bytes",
            Self::ProxyHttpUsernameColon => "HTTP proxy username cannot contain a colon",
            Self::ProxyHttp3PasswordNeedsUsername => "HTTP/3 proxy password requires a username",
            Self::ProxyTunnel(detail) => detail,
            Self::ProxyKindImmutable => "create a new profile to change proxy type",
            Self::ProxyKindLockedByPool => {
                "proxy kind cannot change while the proxy belongs to a pool"
            }
            Self::ProxyLimitReached => "at most 128 proxy profiles are supported",
            Self::ProxyRevisionChanged => "proxy revision changed",
            Self::ProxyGone => "proxy no longer exists",
            Self::ProxyNotFound => "proxy not found",
            Self::ProxyInPool => "proxy belongs to a pool; remove it from the pool first",
            Self::ProxyInUse => "proxy is referenced by a server or RSS feed",
            Self::ProxyHostKeyNotSsh => "only SSH profiles have host-key trust",
        };
        f.write_str(sentence)
    }
}

impl std::error::Error for NetworkRefusal {}

// Callers that report a plain string keep `?` working.
impl From<NetworkRefusal> for String {
    fn from(refusal: NetworkRefusal) -> Self {
        refusal.to_string()
    }
}

impl From<&NetworkRefusal> for Refusal {
    fn from(refusal: &NetworkRefusal) -> Self {
        Self {
            code: refusal.code(),
            message: refusal.to_string(),
            params: refusal.params(),
        }
    }
}

impl From<NetworkRefusal> for Refusal {
    fn from(refusal: NetworkRefusal) -> Self {
        Self::from(&refusal)
    }
}

impl From<NetworkRefusal> for crate::StateError {
    fn from(refusal: NetworkRefusal) -> Self {
        Self::Refused(refusal.into())
    }
}

// A profile the tunnel engine cannot use is refused in the engine's words.
impl From<weaver_tunnel::TunnelError> for NetworkRefusal {
    fn from(error: weaver_tunnel::TunnelError) -> Self {
        Self::ProxyTunnel(error.to_string())
    }
}

#[cfg(test)]
#[path = "refusal_tests.rs"]
mod tests;
