// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The guard that keeps every outbound connection on its route. clippy.toml
// bans dialing, resolving and building HTTP clients outside the routing
// layer. These tests pin the ban, every place it is lifted, and the crates
// that may carry a network client of their own, so widening any of them is a
// change reviewed here rather than an accident.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::persistence::write_guard_tests::writer_gate_lift;

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
    "weaver_tunnel::egress::SocketEgress::bind_udp_reusing",
    "weaver_tunnel::egress::SocketEgress::listen_tcp",
    "weaver_tunnel::egress::SocketEgress::join_multicast",
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
    ("engines/weaver-nntp/tests/demand_throughput.rs", 1),
    ("engines/weaver-nntp/tests/proxy_routing.rs", 1),
    ("engines/weaver-nntp/tests/socket_budget.rs", 1),
    ("engines/weaver-nntp/tests/wireguard_throughput.rs", 1),
    // The LSD's groups, on the local network alone.
    ("engines/weaver-torrent/src/lsd.rs", 1),
    // The kernel route's own dial, datagram bind and listener, from its
    // device or the host's.
    ("engines/weaver-torrent/src/network.rs", 3),
    ("engines/weaver-torrent/src/tests/e2e_session_stop.rs", 1),
    ("engines/weaver-tunnel/src/egress.rs", 5),
    ("engines/weaver-tunnel/src/egress/multicast.rs", 1),
    ("engines/weaver-tunnel/src/lib.rs", 2),
    ("engines/weaver-tunnel/src/pipe.rs", 5),
    ("engines/weaver-tunnel/src/resolution.rs", 3),
    ("engines/weaver-tunnel/src/test_support/http_client.rs", 2),
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
    ("server/crates/weaver-server-core/src/speed_test/http.rs", 1),
    // A leg's clients, one built for each target and shape and kept for
    // the leg.
    ("server/crates/weaver-server-core/src/torrents/net.rs", 1),
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
            // The torrent session builds no client: it takes every one,
            // the port mappers' too, from `torrents::net`, built on the route.
            "weaver-torrent",
            // Only under `test-support`: a finished client on the host's
            // egress for consumers' tests, never in a product build.
            "weaver-tunnel",
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
            // A lift of the write ban is pinned by the write guard instead.
            let text = std::fs::read_to_string(&path).unwrap();
            let count = text
                .match_indices(LIFT)
                .filter(|(at, _)| !writer_gate_lift(&text[at + LIFT.len()..]))
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

// The lockfile cannot tell a product edge from a test one, so weaver-tunnel's
// pin above holds only while its manifest keeps reqwest optional, turned on
// by its test-support feature alone.
#[test]
fn the_tunnels_client_is_for_tests_alone() {
    let path = workspace().join("engines/weaver-tunnel/Cargo.toml");
    let manifest: toml::Table = std::fs::read_to_string(path).unwrap().parse().unwrap();
    let reqwest = manifest
        .get("dependencies")
        .and_then(|dependencies| dependencies.get("reqwest"));
    assert_eq!(
        reqwest
            .and_then(|reqwest| reqwest.get("optional"))
            .and_then(toml::Value::as_bool),
        Some(true),
        "weaver-tunnel's reqwest must stay an optional dependency"
    );
    let elsewhere: Vec<String> = ["build-dependencies", "target"]
        .into_iter()
        .filter(|table| {
            manifest
                .get(*table)
                .is_some_and(|value| value.to_string().contains("reqwest"))
        })
        .map(str::to_owned)
        .collect();
    let turned_on_by: Vec<&str> = manifest["features"]
        .as_table()
        .unwrap()
        .iter()
        .filter(|(_, enables)| {
            enables.as_array().unwrap().iter().any(|feature| {
                let feature = feature.as_str().unwrap();
                feature.trim_start_matches("dep:").split(['/', '?']).next() == Some("reqwest")
            })
        })
        .map(|(name, _)| name.as_str())
        .collect();
    assert_eq!(turned_on_by, ["test-support"]);
    assert!(
        elsewhere.is_empty(),
        "weaver-tunnel names reqwest in {elsewhere:?}"
    );
}

// weaver-tunnel's test-support feature, which carries its client, is on
// only where a crate's tests turn it on: never by default, and never through
// a dependency a product build takes.
#[test]
fn the_tunnels_test_support_is_never_built_into_a_product() {
    let root = workspace();
    let workspace_manifest = manifest_at(&root.join("Cargo.toml"));
    let members: Vec<String> = workspace_manifest["workspace"]["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|member| member.as_str().unwrap().to_owned())
        .collect();
    let manifests: Vec<(String, toml::Table)> = members
        .iter()
        .map(|member| {
            let manifest = manifest_at(&root.join(member).join("Cargo.toml"));
            let name = manifest["package"]["name"].as_str().unwrap().to_owned();
            (name, manifest)
        })
        .collect();

    // Every feature of every member that turns the tunnel's test support on.
    let mut turning_on = BTreeSet::from([("weaver-tunnel".to_owned(), "test-support".to_owned())]);
    loop {
        let before = turning_on.len();
        for (name, manifest) in &manifests {
            let Some(features) = manifest.get("features").and_then(toml::Value::as_table) else {
                continue;
            };
            for (feature, enables) in features {
                let turns_on = enables.as_array().unwrap().iter().any(|enabled| {
                    let enabled = enabled.as_str().unwrap();
                    let (crate_name, enabled) = match enabled.split_once('/') {
                        Some((dependency, enabled)) => (
                            package_of(manifest, dependency.trim_end_matches('?')),
                            enabled,
                        ),
                        None => (name.clone(), enabled),
                    };
                    turning_on.contains(&(crate_name, enabled.to_owned()))
                });
                if turns_on {
                    turning_on.insert((name.clone(), feature.clone()));
                }
            }
        }
        if turning_on.len() == before {
            break;
        }
    }
    // A member built by default, a product binary among them, never builds
    // it.
    let by_default: Vec<&String> = turning_on
        .iter()
        .filter(|(_, feature)| feature == "default")
        .map(|(name, _)| name)
        .collect();
    assert!(
        by_default.is_empty(),
        "default features turn on the tunnel's test support: {by_default:?}"
    );

    let mut product_edges = Vec::new();
    let workspace_dependencies = workspace_manifest["workspace"]
        .get("dependencies")
        .and_then(toml::Value::as_table);
    if let Some(dependencies) = workspace_dependencies {
        product_edges.extend(turning_on_through(
            "the workspace",
            dependencies,
            &turning_on,
        ));
    }
    for (name, manifest) in &manifests {
        let mut tables = Vec::new();
        for kind in ["dependencies", "build-dependencies"] {
            tables.extend(manifest.get(kind).and_then(toml::Value::as_table));
            for target in manifest
                .get("target")
                .and_then(toml::Value::as_table)
                .into_iter()
                .flat_map(|targets| targets.values())
            {
                tables.extend(target.get(kind).and_then(toml::Value::as_table));
            }
        }
        for dependencies in tables {
            product_edges.extend(turning_on_through(name, dependencies, &turning_on));
        }
    }
    assert!(
        product_edges.is_empty(),
        "a product dependency turns weaver-tunnel's test support on: {product_edges:?}"
    );
}

fn manifest_at(path: &Path) -> toml::Table {
    std::fs::read_to_string(path).unwrap().parse().unwrap()
}

// The package a manifest's dependency `key` names.
fn package_of(manifest: &toml::Table, key: &str) -> String {
    ["dependencies", "dev-dependencies", "build-dependencies"]
        .into_iter()
        .filter_map(|kind| manifest.get(kind)?.get(key)?.get("package")?.as_str())
        .next()
        .unwrap_or(key)
        .to_owned()
}

// Each dependency in `dependencies` whose features, its default ones among
// them, include one of `turning_on`.
fn turning_on_through(
    owner: &str,
    dependencies: &toml::Table,
    turning_on: &BTreeSet<(String, String)>,
) -> Vec<String> {
    dependencies
        .iter()
        .filter(|(key, dependency)| {
            let package = dependency
                .get("package")
                .and_then(toml::Value::as_str)
                .unwrap_or(key);
            let defaults = dependency
                .get("default-features")
                .and_then(toml::Value::as_bool)
                .unwrap_or(true);
            let features = dependency
                .get("features")
                .and_then(toml::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(toml::Value::as_str);
            defaults
                .then_some("default")
                .into_iter()
                .chain(features)
                .any(|feature| turning_on.contains(&(package.to_owned(), feature.to_owned())))
        })
        .map(|(key, _)| format!("{owner} -> {key}"))
        .collect()
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

// The torrent engine looks no name up by itself: every name a session asks
// for goes through the resolver its embedder gives it, which for Weaver is
// the leg's, on the resolution record.
#[test]
fn the_torrent_engine_looks_no_name_up_itself() {
    let mut found = Vec::new();
    lookups_in(&workspace().join("engines/weaver-torrent/src"), &mut found);
    assert!(
        found.is_empty(),
        "the torrent engine resolves on its own in {found:?}"
    );
}

fn lookups_in(dir: &Path, found: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            lookups_in(&path, found);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            let source = std::fs::read_to_string(&path).unwrap();
            if ["lookup_host", "ToSocketAddrs", ".to_socket_addrs("]
                .iter()
                .any(|call| source.contains(call))
            {
                found.push(path);
            }
        }
    }
}
