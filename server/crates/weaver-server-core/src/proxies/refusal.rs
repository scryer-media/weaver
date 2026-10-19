// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::persistence::Refusal;
use super::EgressQuotaFault;

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
    EgressQuota(EgressQuotaFault),
    BandwidthLimitTooLarge,
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
    PoolInUseEmpty,

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
    RoutePoolEmpty,
    RouteProxyMissing,
    RoutingProxyMissing,
    RouteProxyRepeated,
    RoutingProxyRepeated,
    RoutingProxyCount,
    RouteChainTransport,
    RouteHasAdvancedLegs,
    RouteConsumerGone,
    TorrentRouteLegs,
    TorrentRoutePool,

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
    ProxyWireGuardNeedsDnsServers,
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

    // Speed tests.
    SpeedTestAlreadyRunning {
        run: u32,
    },
    SpeedTestQueueBusy,
    SpeedTestUsenetNeedsServer,
    SpeedTestLegDownAll,
    SpeedTestQuotaBlocked {
        holder: SpeedTestQuotaHolder,
        id: u32,
        name: String,
    },
    SpeedTestLeaseRefused {
        server: u32,
        name: String,
        reason: String,
    },

    // Tests through a drafted route.
    TestNeedsRoute,
    TestTargetRequired,
    TestSubjectInRoute,
    TestPathCount {
        limit: usize,
    },
}

// Whose download quota keeps a speed test from starting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpeedTestQuotaHolder {
    Server,
    Egress,
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
            Self::EgressQuota(EgressQuotaFault::LimitTooLarge) => "EGRESS_QUOTA_LIMIT_TOO_LARGE",
            Self::EgressQuota(EgressQuotaFault::ResetTimeRange) => "EGRESS_QUOTA_RESET_TIME_RANGE",
            Self::EgressQuota(EgressQuotaFault::MonthlyResetDayRange) => {
                "EGRESS_QUOTA_RESET_DAY_RANGE"
            }
            Self::EgressQuota(EgressQuotaFault::SoftAboveHard) => "EGRESS_QUOTA_SOFT_ABOVE_HARD",
            Self::BandwidthLimitTooLarge => "BANDWIDTH_LIMIT_TOO_LARGE",
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
            Self::PoolInUseEmpty => "POOL_IN_USE_EMPTY",

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
            Self::RoutePoolEmpty => "ROUTE_POOL_EMPTY",
            Self::RouteProxyMissing | Self::RoutingProxyMissing => "ROUTE_PROXY_MISSING",
            Self::RouteProxyRepeated | Self::RoutingProxyRepeated => "ROUTE_PROXY_REPEATED",
            Self::RoutingProxyCount => "ROUTE_PROXY_COUNT",
            Self::RouteChainTransport => "ROUTE_CHAIN_TRANSPORT",
            Self::RouteHasAdvancedLegs => "ROUTE_HAS_ADVANCED_LEGS",
            Self::RouteConsumerGone => "ROUTE_CONSUMER_NOT_FOUND",
            Self::TorrentRouteLegs => "TORRENT_ROUTE_LEGS",
            Self::TorrentRoutePool => "TORRENT_ROUTE_POOL",

            Self::WireGuardPathConflict { .. } => "WIREGUARD_PROXY_PATH_CONFLICT",
            Self::WireGuardInstanceLimit { .. } => "WIREGUARD_INSTANCE_LIMIT",

            Self::ProxyNameLength => "PROXY_NAME_LENGTH",
            Self::ProxyEndpointInvalid => "PROXY_ENDPOINT_INVALID",
            Self::ProxyTimeoutRange => "PROXY_TIMEOUT_RANGE",
            Self::ProxyAddressCount => "PROXY_ADDRESS_COUNT",
            Self::ProxyDnsServerInvalid => "PROXY_DNS_SERVER_INVALID",
            Self::ProxyWireGuardNeedsDnsServers => "PROXY_WIREGUARD_NEEDS_DNS_SERVERS",
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

            Self::SpeedTestAlreadyRunning { .. } => "SPEED_TEST_ALREADY_RUNNING",
            Self::SpeedTestQueueBusy => "SPEED_TEST_QUEUE_BUSY",
            Self::SpeedTestUsenetNeedsServer => "SPEED_TEST_USENET_NEEDS_SERVER",
            Self::SpeedTestLegDownAll => "SPEED_TEST_LEG_DOWN_ALL",
            Self::SpeedTestQuotaBlocked { .. } => "SPEED_TEST_QUOTA_BLOCKED",
            Self::SpeedTestLeaseRefused { .. } => "SPEED_TEST_LEASE_REFUSED",
            Self::TestNeedsRoute => "TEST_NEEDS_ROUTE",
            Self::TestTargetRequired => "TEST_TARGET_REQUIRED",
            Self::TestSubjectInRoute => "TEST_SUBJECT_IN_ROUTE",
            Self::TestPathCount { .. } => "TEST_PATH_COUNT",
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
            Self::SpeedTestAlreadyRunning { run } => serde_json::json!({ "runId": run }),
            Self::SpeedTestQuotaBlocked { holder, id, name } => serde_json::json!({
                "scope": match holder {
                    SpeedTestQuotaHolder::Server => "server",
                    SpeedTestQuotaHolder::Egress => "egress",
                },
                "id": id,
                "name": name,
            }),
            Self::SpeedTestLeaseRefused {
                server,
                name,
                reason,
            } => serde_json::json!({ "serverId": server, "name": name, "reason": reason }),
            Self::TestPathCount { limit } => serde_json::json!({ "limit": limit }),
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
            Self::EgressQuota(fault) => fault.message(),
            Self::BandwidthLimitTooLarge => "bandwidth limit is too large",
            Self::EgressGone => "egress no longer exists",
            Self::EgressNotFound => "egress not found",
            Self::EgressSystemUndeletable => "the System egress cannot be deleted",
            Self::EgressInUse => "egress is referenced by a server or RSS feed",

            Self::PoolNameLength => "pool name must contain 1–128 characters",
            Self::PoolMemberCount => "a pool takes at most sixteen members",
            Self::PoolProxyRepeated => "a proxy cannot appear twice in a pool",
            Self::PoolProxyMissing => "pool references a missing proxy",
            Self::PoolMemberKindMismatch => "all pool members must have the pool's proxy kind",
            Self::PoolGone => "proxy pool no longer exists",
            Self::PoolInUse => "pool is referenced by a server or RSS feed",
            Self::PoolEditRepeatsProxy => "pool edit would repeat a proxy in an existing ladder",
            Self::PoolInUseEmpty => {
                "a pool referenced by a server or RSS feed needs at least one member"
            }

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
            Self::RoutePoolEmpty => "route references a pool with no members",
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
            Self::RouteHasAdvancedLegs => {
                "this route has advanced legs; edit it in Networking instead of replacing its legacy projection"
            }
            Self::RouteConsumerGone => "routing consumer no longer exists",
            Self::TorrentRouteLegs => "a torrent route has exactly one leg",
            Self::TorrentRoutePool => "a torrent route cannot use a pool",

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
            Self::ProxyWireGuardNeedsDnsServers => {
                "a WireGuard proxy resolves names only through DNS servers"
            }
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

            Self::SpeedTestAlreadyRunning { run } => {
                return write!(
                    f,
                    "speed test {run} is still running; cancel it or wait for it to finish"
                );
            }
            Self::SpeedTestQueueBusy => {
                "the previous speed test is still giving the queue back; start again once it has"
            }
            Self::SpeedTestUsenetNeedsServer => {
                "a Usenet speed test runs on a server; a feed can run a bandwidth test only"
            }
            Self::SpeedTestLegDownAll => {
                "every leg of the route would be down; leave at least one leg up"
            }
            Self::SpeedTestQuotaBlocked { holder, name, .. } => {
                let holder = match holder {
                    SpeedTestQuotaHolder::Server => "server",
                    SpeedTestQuotaHolder::Egress => "egress",
                };
                return write!(
                    f,
                    "{holder} {name} has used its download quota, and a speed test books its bytes against it"
                );
            }
            Self::SpeedTestLeaseRefused { name, reason, .. } => {
                return write!(f, "server {name} cannot lend its connections: {reason}");
            }

            Self::TestNeedsRoute => "a test goes through a route drafted for it",
            Self::TestTargetRequired => "a test of the route alone needs a host and port to reach",
            Self::TestSubjectInRoute => {
                "the route a proxy is tested through cannot carry that proxy"
            }
            Self::TestPathCount { limit } => {
                return write!(
                    f,
                    "a test tries every path through the route, and at most {limit}; draft a smaller route"
                );
            }
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
