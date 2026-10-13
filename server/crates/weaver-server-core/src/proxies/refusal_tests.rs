use std::collections::{BTreeMap, BTreeSet};

use super::NetworkRefusal as R;
use crate::servers::DownloadQuotaFault as Quota;
use crate::{Refusal, StateError};

// Every refusal with the code and the sentence a client is promised. A code
// is part of the API: renaming one, or rewording a sentence, fails here.
fn table() -> Vec<(R, &'static str, &'static str)> {
    vec![
        (
            R::EgressNameLength,
            "EGRESS_NAME_LENGTH",
            "egress name must contain 1–128 characters",
        ),
        (
            R::EgressSystemImmutable,
            "EGRESS_SYSTEM_IMMUTABLE",
            "the System egress cannot be renamed, rebound, or disabled",
        ),
        (
            R::EgressSystemBindingReserved,
            "EGRESS_SYSTEM_BINDING_RESERVED",
            "only egress 0 may use the System binding",
        ),
        (
            R::EgressInterfaceNameInvalid,
            "EGRESS_INTERFACE_NAME_INVALID",
            "interface name must be nonempty and contain no NUL",
        ),
        (
            R::EgressInterfaceNameRequired,
            "EGRESS_INTERFACE_NAME_REQUIRED",
            "interface name is required",
        ),
        (
            R::EgressInterfaceBindingUnsupported,
            "EGRESS_INTERFACE_BINDING_UNSUPPORTED",
            "Interface binding is not supported on Windows; use a source address",
        ),
        (
            R::EgressSourceAddressRequired,
            "EGRESS_SOURCE_ADDRESS_REQUIRED",
            "source address is required",
        ),
        (
            R::EgressSourceAddressInvalid,
            "EGRESS_SOURCE_ADDRESS_INVALID",
            "invalid source address",
        ),
        (
            R::EgressSourceAddressNotUnicast,
            "EGRESS_SOURCE_ADDRESS_NOT_UNICAST",
            "source address must be a unicast host address",
        ),
        (
            R::EgressSpeedLimitTooLarge,
            "EGRESS_SPEED_LIMIT_TOO_LARGE",
            "egress speed limit is too large",
        ),
        (
            R::EgressQuota(Quota::LimitTooLarge),
            "EGRESS_QUOTA_LIMIT_TOO_LARGE",
            "egress download quota limit exceeds database range",
        ),
        (
            R::EgressQuota(Quota::LimitRequired),
            "EGRESS_QUOTA_LIMIT_REQUIRED",
            "enabled egress download quota limit must be greater than zero",
        ),
        (
            R::EgressQuota(Quota::ResetTimeRange),
            "EGRESS_QUOTA_RESET_TIME_RANGE",
            "egress download quota reset time must be between 0 and 1439",
        ),
        (
            R::EgressQuota(Quota::MonthlyResetDayRange),
            "EGRESS_QUOTA_RESET_DAY_RANGE",
            "egress download quota monthly reset day must be between 1 and 31",
        ),
        (R::EgressGone, "EGRESS_NOT_FOUND", "egress no longer exists"),
        (R::EgressNotFound, "EGRESS_NOT_FOUND", "egress not found"),
        (
            R::EgressSystemUndeletable,
            "EGRESS_SYSTEM_UNDELETABLE",
            "the System egress cannot be deleted",
        ),
        (
            R::EgressInUse,
            "EGRESS_IN_USE",
            "egress is referenced by a server or RSS feed",
        ),
        (
            R::PoolNameLength,
            "POOL_NAME_LENGTH",
            "pool name must contain 1–128 characters",
        ),
        (
            R::PoolMemberCount,
            "POOL_MEMBER_COUNT",
            "a pool requires between two and sixteen members",
        ),
        (
            R::PoolProxyRepeated,
            "POOL_PROXY_REPEATED",
            "a proxy cannot appear twice in a pool",
        ),
        (
            R::PoolProxyMissing,
            "POOL_PROXY_MISSING",
            "pool references a missing proxy",
        ),
        (
            R::PoolMemberKindMismatch,
            "POOL_MEMBER_KIND_MISMATCH",
            "all pool members must have the pool's proxy kind",
        ),
        (R::PoolGone, "POOL_NOT_FOUND", "proxy pool no longer exists"),
        (
            R::PoolInUse,
            "POOL_IN_USE",
            "pool is referenced by a server or RSS feed",
        ),
        (
            R::PoolEditRepeatsProxy,
            "POOL_EDIT_REPEATS_PROXY",
            "pool edit would repeat a proxy in an existing ladder",
        ),
        (
            R::RouteLegCount,
            "ROUTE_LEG_COUNT",
            "a route requires between one and eight legs",
        ),
        (
            R::RouteLegWeightRange,
            "ROUTE_LEG_WEIGHT_RANGE",
            "leg weights must be between 1 and 100",
        ),
        (
            R::RouteLegWeightSum,
            "ROUTE_LEG_WEIGHT_SUM",
            "leg weights must sum to exactly 100",
        ),
        (
            R::RouteLadderRungCount,
            "ROUTE_LADDER_RUNG_COUNT",
            "a ladder requires between one and eight rungs",
        ),
        (
            R::RouteChainLength,
            "ROUTE_CHAIN_LENGTH",
            "a chain requires two or three proxies",
        ),
        (
            R::RouteDirectPathFalse,
            "ROUTE_DIRECT_PATH_FALSE",
            "a direct path must be true",
        ),
        (
            R::RouteAndRoutingBoth,
            "ROUTE_AND_ROUTING_BOTH",
            "specify route or routing, not both",
        ),
        (
            R::RouteEgressMissing,
            "ROUTE_EGRESS_MISSING",
            "route references a missing egress",
        ),
        (
            R::RouteEgressDisabled,
            "ROUTE_EGRESS_DISABLED",
            "route references a disabled egress",
        ),
        (
            R::RoutePoolMissing,
            "ROUTE_POOL_MISSING",
            "route references a missing pool",
        ),
        (
            R::RouteProxyMissing,
            "ROUTE_PROXY_MISSING",
            "route references a missing proxy",
        ),
        (
            R::RoutingProxyMissing,
            "ROUTE_PROXY_MISSING",
            "routing policy references a missing proxy",
        ),
        (
            R::RouteProxyRepeated,
            "ROUTE_PROXY_REPEATED",
            "a proxy cannot appear twice in a ladder, including pools and chains",
        ),
        (
            R::RoutingProxyRepeated,
            "ROUTE_PROXY_REPEATED",
            "a proxy cannot appear twice in a ladder",
        ),
        (
            R::RoutingProxyCount,
            "ROUTE_PROXY_COUNT",
            "a route supports at most eight proxies",
        ),
        (
            R::RouteChainTransport,
            "ROUTE_CHAIN_TRANSPORT",
            "WireGuard and HTTP/3 must be the first proxy in a chain, directly above the egress; WireGuard may also sit directly on another WireGuard proxy",
        ),
        (
            R::RouteRssProxyNeedsDns,
            "ROUTE_RSS_PROXY_NEEDS_DNS",
            "every RSS proxy and pool member requires DNS servers",
        ),
        (
            R::RssRouteProxyNeedsDns,
            "ROUTE_RSS_PROXY_NEEDS_DNS",
            "RSS proxy routes require configured DNS servers",
        ),
        (
            R::RouteHasAdvancedLegs,
            "ROUTE_HAS_ADVANCED_LEGS",
            "this route has advanced legs; edit it in Networking instead of replacing its legacy projection",
        ),
        (
            R::RouteConsumerGone,
            "ROUTE_CONSUMER_NOT_FOUND",
            "routing consumer no longer exists",
        ),
        (
            R::WireGuardPathConflict {
                proxy: 2,
                egress: 0,
                paths: vec![vec![1, 2], vec![2]],
            },
            "WIREGUARD_PROXY_PATH_CONFLICT",
            "WireGuard proxy 2 is used on egress 0 by two different paths (proxy 1 → proxy 2 and proxy 2); a WireGuard server accepts one connection per key, so a WireGuard proxy may reach the egress by only one path",
        ),
        (
            R::WireGuardInstanceLimit {
                required: 3,
                limit: 2,
            },
            "WIREGUARD_INSTANCE_LIMIT",
            "routes require 3 WireGuard instances × 516 MiB = 1548 MiB; this host permits 2 instances (available memory / 4 / 516 MiB, clamped to 1–8). Pool members count individually.",
        ),
        (
            R::ProxyNameLength,
            "PROXY_NAME_LENGTH",
            "proxy name must contain 1–128 characters",
        ),
        (
            R::ProxyEndpointInvalid,
            "PROXY_ENDPOINT_INVALID",
            "proxy endpoint must have a valid host and port",
        ),
        (
            R::ProxyTimeoutRange,
            "PROXY_TIMEOUT_RANGE",
            "proxy timeout must be between 1 and 300 seconds",
        ),
        (
            R::ProxyAddressCount,
            "PROXY_ADDRESS_COUNT",
            "too many DNS servers or tunnel addresses",
        ),
        (
            R::ProxyDnsServerInvalid,
            "PROXY_DNS_SERVER_INVALID",
            "DNS servers must be IP addresses",
        ),
        (
            R::ProxyCredentialTooLarge,
            "PROXY_CREDENTIAL_TOO_LARGE",
            "proxy credential is too large",
        ),
        (
            R::ProxySshUsernameRequired,
            "PROXY_SSH_USERNAME_REQUIRED",
            "SSH requires a username",
        ),
        (
            R::ProxySshKeyRequired,
            "PROXY_SSH_KEY_REQUIRED",
            "SSH requires an Ed25519 private key",
        ),
        (
            R::ProxySshPasswordUnsupported,
            "PROXY_SSH_PASSWORD_UNSUPPORTED",
            "SSH requires an Ed25519 private key and takes no password",
        ),
        (
            R::ProxySocks5PasswordNeedsUsername,
            "PROXY_SOCKS5_PASSWORD_NEEDS_USERNAME",
            "SOCKS5 proxy password requires a username",
        ),
        (
            R::ProxySocks5CredentialLength,
            "PROXY_SOCKS5_CREDENTIAL_LENGTH",
            "SOCKS5 credentials must fit in 255 bytes",
        ),
        (
            R::ProxyHttpUsernameColon,
            "PROXY_HTTP_USERNAME_COLON",
            "HTTP proxy username cannot contain a colon",
        ),
        (
            R::ProxyHttp3PasswordNeedsUsername,
            "PROXY_HTTP3_PASSWORD_NEEDS_USERNAME",
            "HTTP/3 proxy password requires a username",
        ),
        (
            R::ProxyTunnel("the engine's own sentence".into()),
            "PROXY_TUNNEL_CONFIGURATION",
            "the engine's own sentence",
        ),
        (
            R::ProxyKindImmutable,
            "PROXY_KIND_IMMUTABLE",
            "create a new profile to change proxy type",
        ),
        (
            R::ProxyKindLockedByPool,
            "PROXY_KIND_LOCKED_BY_POOL",
            "proxy kind cannot change while the proxy belongs to a pool",
        ),
        (
            R::ProxyLimitReached,
            "PROXY_LIMIT_REACHED",
            "at most 128 proxy profiles are supported",
        ),
        (
            R::ProxyRevisionChanged,
            "PROXY_REVISION_CHANGED",
            "proxy revision changed",
        ),
        (R::ProxyGone, "PROXY_NOT_FOUND", "proxy no longer exists"),
        (R::ProxyNotFound, "PROXY_NOT_FOUND", "proxy not found"),
        (
            R::ProxyInPool,
            "PROXY_IN_POOL",
            "proxy belongs to a pool; remove it from the pool first",
        ),
        (
            R::ProxyInUse,
            "PROXY_IN_USE",
            "proxy is referenced by a server or RSS feed",
        ),
        (
            R::ProxyHostKeyNotSsh,
            "PROXY_HOST_KEY_NOT_SSH",
            "only SSH profiles have host-key trust",
        ),
        (
            R::SpeedTestAlreadyRunning { run: 4 },
            "SPEED_TEST_ALREADY_RUNNING",
            "speed test 4 is still running; cancel it or wait for it to finish",
        ),
        (
            R::SpeedTestQueueBusy,
            "SPEED_TEST_QUEUE_BUSY",
            "the previous speed test is still giving the queue back; start again once it has",
        ),
        (
            R::SpeedTestUsenetNeedsServer,
            "SPEED_TEST_USENET_NEEDS_SERVER",
            "a Usenet speed test runs on a server; a feed can run a bandwidth test only",
        ),
        (
            R::SpeedTestLegDownAll,
            "SPEED_TEST_LEG_DOWN_ALL",
            "every leg of the route would be down; leave at least one leg up",
        ),
        (
            R::SpeedTestQuotaBlocked {
                holder: super::SpeedTestQuotaHolder::Egress,
                id: 3,
                name: "Uplink".into(),
            },
            "SPEED_TEST_QUOTA_BLOCKED",
            "egress Uplink has used its download quota, and a speed test books its bytes against it",
        ),
        (
            R::SpeedTestLeaseRefused {
                server: 2,
                name: "Primary".into(),
                reason: "its health tracker has it out of service".into(),
            },
            "SPEED_TEST_LEASE_REFUSED",
            "server Primary cannot lend its connections: its health tracker has it out of service",
        ),
    ]
}

// Names every variant, so a new one does not compile until it is listed here
// and, by the count below, has its row in the table.
fn slot(refusal: &R) -> usize {
    match refusal {
        R::EgressNameLength => 0,
        R::EgressSystemImmutable => 1,
        R::EgressSystemBindingReserved => 2,
        R::EgressInterfaceNameInvalid => 3,
        R::EgressInterfaceNameRequired => 4,
        R::EgressInterfaceBindingUnsupported => 5,
        R::EgressSourceAddressRequired => 6,
        R::EgressSourceAddressInvalid => 7,
        R::EgressSourceAddressNotUnicast => 8,
        R::EgressSpeedLimitTooLarge => 9,
        R::EgressQuota(Quota::LimitTooLarge) => 10,
        R::EgressQuota(Quota::LimitRequired) => 11,
        R::EgressQuota(Quota::ResetTimeRange) => 12,
        R::EgressQuota(Quota::MonthlyResetDayRange) => 13,
        R::EgressGone => 14,
        R::EgressNotFound => 15,
        R::EgressSystemUndeletable => 16,
        R::EgressInUse => 17,
        R::PoolNameLength => 18,
        R::PoolMemberCount => 19,
        R::PoolProxyRepeated => 20,
        R::PoolProxyMissing => 21,
        R::PoolMemberKindMismatch => 22,
        R::PoolGone => 23,
        R::PoolInUse => 24,
        R::PoolEditRepeatsProxy => 25,
        R::RouteLegCount => 26,
        R::RouteLegWeightRange => 27,
        R::RouteLegWeightSum => 28,
        R::RouteLadderRungCount => 29,
        R::RouteChainLength => 30,
        R::RouteDirectPathFalse => 31,
        R::RouteAndRoutingBoth => 32,
        R::RouteEgressMissing => 33,
        R::RouteEgressDisabled => 34,
        R::RoutePoolMissing => 35,
        R::RouteProxyMissing => 36,
        R::RoutingProxyMissing => 37,
        R::RouteProxyRepeated => 38,
        R::RoutingProxyRepeated => 39,
        R::RoutingProxyCount => 40,
        R::RouteChainTransport => 41,
        R::RouteRssProxyNeedsDns => 42,
        R::RssRouteProxyNeedsDns => 43,
        R::RouteHasAdvancedLegs => 44,
        R::RouteConsumerGone => 45,
        R::WireGuardPathConflict { .. } => 46,
        R::WireGuardInstanceLimit { .. } => 47,
        R::ProxyNameLength => 48,
        R::ProxyEndpointInvalid => 49,
        R::ProxyTimeoutRange => 50,
        R::ProxyAddressCount => 51,
        R::ProxyDnsServerInvalid => 52,
        R::ProxyCredentialTooLarge => 53,
        R::ProxySshUsernameRequired => 54,
        R::ProxySshKeyRequired => 55,
        R::ProxySshPasswordUnsupported => 56,
        R::ProxySocks5PasswordNeedsUsername => 57,
        R::ProxySocks5CredentialLength => 58,
        R::ProxyHttpUsernameColon => 59,
        R::ProxyHttp3PasswordNeedsUsername => 60,
        R::ProxyTunnel(_) => 61,
        R::ProxyKindImmutable => 62,
        R::ProxyKindLockedByPool => 63,
        R::ProxyLimitReached => 64,
        R::ProxyRevisionChanged => 65,
        R::ProxyGone => 66,
        R::ProxyNotFound => 67,
        R::ProxyInPool => 68,
        R::ProxyInUse => 69,
        R::ProxyHostKeyNotSsh => 70,
        R::SpeedTestAlreadyRunning { .. } => 71,
        R::SpeedTestQueueBusy => 72,
        R::SpeedTestUsenetNeedsServer => 73,
        R::SpeedTestLegDownAll => 74,
        R::SpeedTestQuotaBlocked { .. } => 75,
        R::SpeedTestLeaseRefused { .. } => 76,
    }
}
const SLOTS: usize = 77;

#[test]
fn every_refusal_keeps_its_code_and_its_sentence() {
    let table = table();
    let slots: BTreeSet<usize> = table.iter().map(|(refusal, _, _)| slot(refusal)).collect();
    assert_eq!(slots.len(), table.len(), "a refusal has two rows");
    assert_eq!(
        slots,
        (0..SLOTS).collect(),
        "a refusal has no row in the table"
    );
    for (refusal, code, sentence) in table {
        assert_eq!(refusal.code(), code, "{refusal:?}");
        assert_eq!(refusal.to_string(), sentence, "{refusal:?}");
    }
}

#[test]
fn codes_are_screaming_snake_case_and_shared_only_by_the_same_condition() {
    let mut sentences: BTreeMap<&str, usize> = BTreeMap::new();
    for (_, code, _) in table() {
        assert!(
            !code.is_empty()
                && code.split('_').all(|word| !word.is_empty()
                    && word
                        .bytes()
                        .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())),
            "{code}"
        );
        *sentences.entry(code).or_default() += 1;
    }
    let shared: Vec<&str> = sentences
        .into_iter()
        .filter(|&(_, count)| count > 1)
        .map(|(code, _)| code)
        .collect();
    assert_eq!(
        shared,
        [
            "EGRESS_NOT_FOUND",
            "PROXY_NOT_FOUND",
            "ROUTE_PROXY_MISSING",
            "ROUTE_PROXY_REPEATED",
            "ROUTE_RSS_PROXY_NEEDS_DNS",
        ]
    );
}

#[test]
fn a_refusal_reaches_a_state_error_as_its_plain_sentence_and_code() {
    for (refusal, code, sentence) in table() {
        let error = StateError::from(refusal.clone());
        // Nothing in the sentence reads as a storage fault.
        assert_eq!(error.to_string(), sentence);
        assert!(!sentence.contains("database error") && !sentence.contains("db error"));
        let carried = error.refusal().expect("a refusal");
        assert_eq!(carried.code, code);
        assert_eq!(carried.message, sentence);
        assert_eq!(carried, &Refusal::from(refusal));
    }
    // A storage fault is not a refusal and keeps its own wording.
    let fault = StateError::Database("unknown stored egress binding".into());
    assert!(fault.refusal().is_none());
    assert_eq!(
        fault.to_string(),
        "database error: unknown stored egress binding"
    );
}

#[test]
fn only_the_sentences_that_name_values_carry_them() {
    for (refusal, code, _) in table() {
        let params = serde_json::Value::Object(refusal.params());
        match code {
            "WIREGUARD_PROXY_PATH_CONFLICT" => assert_eq!(
                params,
                serde_json::json!({ "proxyId": 2, "egressId": 0, "paths": [[1, 2], [2]] })
            ),
            "WIREGUARD_INSTANCE_LIMIT" => assert_eq!(
                params,
                serde_json::json!({ "required": 3, "limit": 2, "instanceMib": 516 })
            ),
            "SPEED_TEST_ALREADY_RUNNING" => {
                assert_eq!(params, serde_json::json!({ "runId": 4 }))
            }
            "SPEED_TEST_QUOTA_BLOCKED" => assert_eq!(
                params,
                serde_json::json!({ "scope": "egress", "id": 3, "name": "Uplink" })
            ),
            "SPEED_TEST_LEASE_REFUSED" => assert_eq!(
                params,
                serde_json::json!({
                    "serverId": 2,
                    "name": "Primary",
                    "reason": "its health tracker has it out of service",
                })
            ),
            _ => assert_eq!(params, serde_json::json!({}), "{code}"),
        }
    }
}

#[test]
fn validators_refuse_with_the_variant_for_what_is_wrong() {
    use crate::proxies::{EgressBinding, EgressInterface, ProxyPool, RoutingPolicy};
    use std::collections::HashMap;

    let mut egress = EgressInterface::system();
    egress.name = "renamed".into();
    assert_eq!(egress.validate(), Err(R::EgressSystemImmutable));
    let mut egress = EgressInterface {
        id: 4,
        name: "Second".into(),
        binding: EgressBinding::System,
        ..EgressInterface::system()
    };
    assert_eq!(egress.validate(), Err(R::EgressSystemBindingReserved));
    egress.binding = EgressBinding::Interface {
        name: String::new(),
    };
    assert_eq!(egress.validate(), Err(R::EgressInterfaceNameInvalid));
    egress.binding = EgressBinding::SourceAddress {
        address: "0.0.0.0".parse().unwrap(),
    };
    assert_eq!(egress.validate(), Err(R::EgressSourceAddressNotUnicast));
    egress.binding = EgressBinding::SourceAddress {
        address: "192.0.2.1".parse().unwrap(),
    };
    egress.download_quota.enabled = true;
    assert_eq!(egress.validate(), Err(R::EgressQuota(Quota::LimitRequired)));
    egress.name = " ".into();
    assert_eq!(egress.validate(), Err(R::EgressNameLength));

    let pool = |member_ids: Vec<u32>| ProxyPool {
        id: 1,
        name: "Region".into(),
        kind: crate::proxies::ProxyKind::Socks5,
        member_ids,
        enabled: true,
    };
    let profiles = HashMap::new();
    assert_eq!(pool(vec![1]).validate(&profiles), Err(R::PoolMemberCount));
    assert_eq!(
        pool(vec![1, 2]).validate(&profiles),
        Err(R::PoolProxyMissing)
    );

    let legacy = |proxy_ids: Vec<u32>| RoutingPolicy {
        proxy_ids,
        ..Default::default()
    };
    assert_eq!(legacy(vec![1, 1]).validate(), Err(R::RoutingProxyRepeated));
    assert_eq!(
        legacy((1..=9).collect()).validate(),
        Err(R::RoutingProxyCount)
    );
}
