use std::net::{IpAddr, SocketAddr};

use http::{HeaderMap, HeaderValue};
use weaver_server_core::security::{HttpAuthority, RuntimeSecurityConfig};

fn authenticated() -> RuntimeSecurityConfig {
    let config = RuntimeSecurityConfig::default();
    config.apply_stored_access_policy_revision(None, None, false);
    assert!(config.authenticated_access_mode());
    config
}

fn proxied() -> RuntimeSecurityConfig {
    let mut config = authenticated();
    config.trusted_proxies = vec!["10.0.0.0/24".parse().unwrap()];
    config.set_trusted_cidrs(vec!["192.168.1.0/24".parse().unwrap()]);
    config
}

fn peer(value: &str) -> Option<SocketAddr> {
    Some(value.parse().unwrap())
}

fn forwarded(value: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-for", HeaderValue::from_str(value).unwrap());
    headers
}

#[test]
fn untrusted_peer_cannot_choose_client_identity() {
    let config = proxied();
    let headers = forwarded("192.168.1.30");
    assert_eq!(
        config.resolve_client_ip(peer("203.0.113.7:54321"), &headers),
        Some("203.0.113.7".parse::<IpAddr>().unwrap())
    );
    assert!(!config.is_trusted_client(peer("203.0.113.7:54321"), &headers));
}

#[test]
fn nearest_untrusted_hop_wins_over_spoofed_prefix() {
    let config = proxied();
    let headers = forwarded("192.168.1.30, 203.0.113.7, 10.0.0.2");
    assert_eq!(
        config.resolve_client_ip(peer("10.0.0.3:54321"), &headers),
        Some("203.0.113.7".parse::<IpAddr>().unwrap())
    );
    assert!(!config.is_trusted_client(peer("10.0.0.3:54321"), &headers));
}

#[test]
fn trusted_proxy_cannot_become_the_client_when_forwarding_is_unresolved() {
    let config = proxied();
    config.set_trusted_cidrs(vec!["10.0.0.0/24".parse().unwrap()]);
    for headers in [
        HeaderMap::new(),
        forwarded(""),
        forwarded("192.168.1.30, invalid"),
        forwarded(",192.168.1.30"),
        forwarded("192.168.1.30,"),
        forwarded("192.168.1.30,,10.0.0.2"),
        forwarded("10.0.0.2, 10.0.0.3"),
        forwarded(&vec!["192.168.1.30"; 33].join(", ")),
    ] {
        assert_eq!(
            config.resolve_client_ip(peer("10.0.0.3:54321"), &headers),
            None,
            "missing, malformed, oversized, and all-proxy chains must be unresolved"
        );
        assert!(!config.is_trusted_client(peer("10.0.0.3:54321"), &headers));
    }
}

#[test]
fn alternate_forwarding_headers_cannot_grant_client_trust() {
    let config = proxied();
    let mut headers = HeaderMap::new();
    headers.insert("x-real-ip", HeaderValue::from_static("192.168.1.30"));
    headers.insert("forwarded", HeaderValue::from_static("for=192.168.1.30"));
    assert_eq!(
        config.resolve_client_ip(peer("10.0.0.3:54321"), &headers),
        None
    );
    assert!(!config.is_trusted_client(peer("10.0.0.3:54321"), &headers));
}

#[test]
fn mapped_ipv4_addresses_obey_the_same_proxy_and_client_policy() {
    let config = proxied();
    let headers = forwarded("::ffff:192.168.1.30, ::ffff:10.0.0.2");
    assert_eq!(
        config.resolve_client_ip(peer("[::ffff:10.0.0.3]:54321"), &headers),
        Some("192.168.1.30".parse::<IpAddr>().unwrap())
    );
    assert!(config.is_trusted_client(peer("[::ffff:10.0.0.3]:54321"), &headers));
}

#[test]
fn authenticated_hosts_are_ergonomic_but_explicit_restrictions_survive() {
    let mut config = authenticated();
    let external = HttpAuthority::parse("media.example.test:9090").unwrap();
    let docker = HttpAuthority::parse("weaver:9090").unwrap();
    assert!(config.is_http_authority_allowed(&external));
    assert!(config.is_http_authority_allowed(&docker));
    config.http_allowed_hosts = vec![external.clone()];
    assert!(config.is_http_authority_allowed(&external));
    assert!(!config.is_http_authority_allowed(&docker));
}

#[test]
fn legacy_cidr_mode_keeps_its_hostname_guard() {
    let config = RuntimeSecurityConfig::default();
    config.apply_stored_access_policy_revision(Some("login_except_local"), None, true);
    assert!(!config.authenticated_access_mode());
    assert!(
        !config.is_http_authority_allowed(&HttpAuthority::parse("attacker.example.test").unwrap())
    );
}

#[test]
fn damaged_established_install_is_never_inferred_to_be_fresh() {
    let config = RuntimeSecurityConfig::default();
    config.apply_stored_access_policy_revision(None, None, true);
    assert!(!config.authenticated_access_mode());
    config.apply_stored_access_policy_revision(None, Some("authenticated-v1"), true);
    assert!(config.authenticated_access_mode());
}

#[test]
fn stored_authenticated_restrictions_survive_policy_resolution() {
    let config = RuntimeSecurityConfig::default();
    config.apply_stored_access_policy_revision(
        Some("login_required"),
        Some("authenticated-v1"),
        true,
    );
    config.apply_stored_trust(Some("login_required"), Some(r#"["192.168.1.0/24"]"#));
    assert!(config.remembered_client_allowed(peer("192.168.1.30:1234"), &HeaderMap::new()));
    assert!(!config.remembered_client_allowed(peer("203.0.113.7:1234"), &HeaderMap::new()));
}

#[test]
fn malformed_stored_restrictions_never_become_unrestricted_sessions() {
    let config = authenticated();
    config.apply_stored_trust(Some("login_required"), Some("damaged JSON"));
    assert!(!config.remembered_policy_valid());
    assert!(!config.remembered_client_allowed(peer("192.168.1.30:1234"), &HeaderMap::new()));
    config.apply_stored_trust(Some("login_required"), Some("[]"));
    assert!(config.remembered_policy_valid());
    assert!(config.remembered_client_allowed(peer("192.168.1.30:1234"), &HeaderMap::new()));
}

#[test]
fn stored_proxies_and_networks_resolve_as_one_live_policy() {
    let config = authenticated();
    config.apply_stored_trust(Some("login_required"), Some(r#"["192.168.1.0/24"]"#));
    config.apply_stored_proxies(Some(r#"["10.0.0.0/24"]"#));
    let router_clone = config.clone();
    let headers = forwarded("192.168.1.30");
    assert!(router_clone.remembered_client_allowed(peer("10.0.0.3:1234"), &headers));
    config.set_authenticated_network_policy(
        vec!["203.0.113.0/24".parse().unwrap()],
        vec!["10.0.0.0/24".parse().unwrap()],
    );
    assert!(!router_clone.remembered_client_allowed(peer("10.0.0.3:1234"), &headers));
    config.apply_stored_proxies(Some("damaged"));
    assert!(!config.remembered_policy_valid());
    assert!(!router_clone.remembered_client_allowed(peer("203.0.113.7:1234"), &headers));
}

#[test]
fn repeated_forwarded_fields_form_one_bounded_chain() {
    let config = proxied();
    let mut headers = forwarded("192.168.1.30");
    headers.append(
        "x-forwarded-for",
        HeaderValue::from_static("203.0.113.7,10.0.0.2"),
    );
    assert_eq!(
        config.resolve_client_ip(peer("10.0.0.3:1234"), &headers),
        Some("203.0.113.7".parse().unwrap())
    );
    headers.append("x-forwarded-for", HeaderValue::from_static(""));
    assert_eq!(
        config.resolve_client_ip(peer("10.0.0.3:1234"), &headers),
        None
    );
}
