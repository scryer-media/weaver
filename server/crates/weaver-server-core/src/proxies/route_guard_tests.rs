// The guard that keeps every outbound connection on its route. clippy.toml
// bans dialing, resolving and building HTTP clients outside the routing
// layer. These tests pin the ban, every place it is lifted, and the crates
// that may carry a network client of their own, so widening any of them is a
// change reviewed here rather than an accident.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

// Built here so this file never counts as an exception itself.
const LIFT: &str = concat!("clippy::", "disallowed", "_methods");

// The calls clippy refuses outside the routing layer.
const BANNED: &[&str] = &[
    "reqwest::Client::new",
    "reqwest::ClientBuilder::build",
    "reqwest::get",
    "application_updater::pipeline::application_upgrade_http_client",
    "artifact_trust::prime_sigstore_trust_roots",
    "artifact_trust::refresh_sigstore_trusted_root",
    "tokio_tungstenite::connect_async",
    "tokio::net::TcpStream::connect",
    "tokio::net::TcpSocket::connect",
    "std::net::TcpStream::connect",
    "std::net::TcpStream::connect_timeout",
    "socket2::Socket::connect",
    "socket2::Socket::connect_timeout",
    "tokio::net::UdpSocket::bind",
    "std::net::UdpSocket::bind",
    "quinn::Endpoint::client",
    "tokio::net::lookup_host",
    "std::net::ToSocketAddrs::to_socket_addrs",
    "weaver_tunnel::resolution::connect_host",
    "weaver_tunnel::resolution::host_lookup",
    "weaver_tunnel::resolution::host_lookup_blocking",
    "weaver_tunnel::egress::SocketEgress::connect",
    "weaver_tunnel::egress::SocketEgress::connect_blocking",
    "weaver_tunnel::egress::SocketEgress::bind_udp",
    "weaver_nntp::route_dialer::AddressPlanned::new",
    "weaver_tunnel::pipe::Egress::new",
];

// Every file that lifts the ban, and how many times. Each lift names why the
// call there is the route itself, or reaches only this machine; crate roots
// lift it for their own tests' loopback fixtures.
const LIFTED: &[(&str, usize)] = &[
    ("engines/weaver-nntp/benches/body_server_availability.rs", 1),
    ("engines/weaver-nntp/src/address_plan.rs", 2),
    ("engines/weaver-nntp/src/lib.rs", 1),
    ("engines/weaver-nntp/src/route_dialer.rs", 1),
    ("engines/weaver-nntp/tests/proxy_routing.rs", 1),
    ("engines/weaver-nntp/tests/socket_budget.rs", 1),
    ("engines/weaver-nntp/tests/wireguard_throughput.rs", 1),
    ("engines/weaver-tunnel/src/egress.rs", 2),
    ("engines/weaver-tunnel/src/lib.rs", 2),
    ("engines/weaver-tunnel/src/pipe.rs", 5),
    ("engines/weaver-tunnel/src/resolution.rs", 3),
    ("engines/weaver-tunnel/src/transport.rs", 1),
    ("server/app/weaver/src/commands/nzb.rs", 1),
    ("server/app/weaver/src/main.rs", 1),
    ("server/app/weaver/src/tray/shared.rs", 1),
    ("server/crates/weaver-server-core/src/lib.rs", 1),
    (
        "server/crates/weaver-server-core/src/proxies/network_runtime.rs",
        3,
    ),
    (
        "server/crates/weaver-server-core/src/proxies/routed_http.rs",
        2,
    ),
    ("server/crates/weaver-server-core/src/proxies/runtime.rs", 3),
    ("server/crates/weaver-server-core/src/speed_test/http.rs", 1),
    ("xtask/src/main.rs", 1),
];

// The crates that open connections of their own, and every crate allowed to
// depend on one. A new dependent can reach the network without a route.
const CLIENT_CRATES: &[(&str, &[&str])] = &[
    (
        "reqwest",
        &[
            "application-updater",
            "artifact-trust",
            "weaver",
            "weaver-server-api",
            "weaver-server-core",
        ],
    ),
    ("hyper", &["axum", "hyper-rustls", "hyper-util", "reqwest"]),
    ("hyper-util", &["axum", "hyper-rustls", "reqwest"]),
    (
        "quinn",
        &["h3-quinn", "proxy-tunnels", "reqwest", "weaver-tunnel"],
    ),
    ("h3", &["h3-quinn", "proxy-tunnels", "weaver-tunnel"]),
    ("tokio-tungstenite", &["axum", "weaver"]),
];

// Crates that connect or resolve names their own way, none of which may enter
// the build.
const FORBIDDEN_CRATES: &[&str] = &[
    "async-tungstenite",
    "attohttpc",
    "awc",
    "curl",
    "hickory-resolver",
    "isahc",
    "minreq",
    "surf",
    "tokio-socks",
    "trust-dns-resolver",
    "ureq",
];

fn workspace() -> PathBuf {
    std::fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..")).unwrap()
}

#[test]
fn clippy_bans_every_unrouted_call() {
    let config = std::fs::read_to_string(workspace().join("clippy.toml")).unwrap();
    let missing: Vec<_> = BANNED
        .iter()
        .filter(|path| !config.contains(&format!("path = \"{path}\"")))
        .collect();
    assert!(missing.is_empty(), "clippy.toml no longer bans {missing:?}");
}

#[test]
fn every_lift_of_the_route_ban_is_pinned() {
    let root = workspace();
    let mut found = BTreeMap::new();
    for dir in ["engines", "server", "shared", "xtask"] {
        count_lifts(&root, &root.join(dir), &mut found);
    }
    let pinned: BTreeMap<String, usize> = LIFTED
        .iter()
        .map(|(path, count)| ((*path).to_owned(), *count))
        .collect();
    assert_eq!(
        found, pinned,
        "the route ban is lifted somewhere new: fetch with proxies::routed_fetch or \
         proxies::with_routed_client instead, or pin the lift here once its reason is reviewed"
    );
}

fn count_lifts(root: &Path, dir: &Path, found: &mut BTreeMap<String, usize>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            if !path
                .file_name()
                .is_some_and(|name| name == "target" || name == "node_modules")
            {
                count_lifts(root, &path, found);
            }
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            let count = std::fs::read_to_string(&path)
                .unwrap()
                .matches(LIFT)
                .count();
            if count > 0 {
                let relative = path.strip_prefix(root).unwrap();
                found.insert(relative.to_string_lossy().replace('\\', "/"), count);
            }
        }
    }
}

#[test]
fn only_pinned_crates_carry_a_network_client() {
    let lock = std::fs::read_to_string(workspace().join("Cargo.lock")).unwrap();
    let packages = lock_packages(&lock);
    let present: Vec<_> = FORBIDDEN_CRATES
        .iter()
        .filter(|forbidden| packages.iter().any(|(name, _)| name == *forbidden))
        .collect();
    assert!(
        present.is_empty(),
        "{present:?} connect or resolve without a route"
    );
    for (client, allowed) in CLIENT_CRATES {
        let dependents: BTreeSet<&str> = packages
            .iter()
            .filter(|(_, dependencies)| dependencies.iter().any(|name| name == client))
            .map(|(name, _)| name.as_str())
            .collect();
        let allowed: BTreeSet<&str> = allowed.iter().copied().collect();
        assert_eq!(
            dependents, allowed,
            "the crates that depend on {client} changed: a new one can reach the network \
             without a route, so review it before pinning it here"
        );
    }
}

// Each package in a lockfile, with the names of the packages it depends on.
fn lock_packages(lock: &str) -> Vec<(String, Vec<String>)> {
    lock.split("[[package]]")
        .skip(1)
        .map(|block| {
            let name = block
                .lines()
                .find_map(|line| line.trim().strip_prefix("name = "))
                .unwrap()
                .trim_matches('"')
                .to_owned();
            let dependencies = block
                .split_once("dependencies = [")
                .and_then(|(_, rest)| rest.split_once(']'))
                .map(|(list, _)| list)
                .unwrap_or_default()
                .split(',')
                .filter_map(|entry| entry.trim().trim_matches('"').split(' ').next())
                .filter(|name| !name.is_empty())
                .map(str::to_owned)
                .collect();
            (name, dependencies)
        })
        .collect()
}
