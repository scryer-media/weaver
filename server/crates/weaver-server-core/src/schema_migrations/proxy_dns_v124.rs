// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Migration 124, upgrade step: the DNS a saved proxy named becomes a saved
// DNS profile, and each route takes the profile of the proxy on its first
// leg.
//
// Before 124 a proxy named its resolver, Weaver or the proxy, and a list of
// server addresses that resolver asked, plain on port 53, taking answers as
// sent. Weaver with no servers is the host answering: the built-in System
// profile, and nothing is written. Weaver with servers is those servers
// asked from the egress. The proxy with no servers is the proxy handed the
// name. The proxy with servers is, for WireGuard, the tunnel's own servers,
// which stay on the proxy as tunnel configuration; for any other kind, those
// servers asked through the proxy.
//
// Each distinct setting becomes one profile, and one server list when it
// lists servers, named after the first proxy that had it. A profile where
// the proxy answers lets the host answer a stage with no proxy, as the host
// did before. A route takes the profile of the proxy the first rung of its
// first leg through a proxy ends at, or of the first proxy a legacy route
// names; a route with no proxy keeps System. A route any of whose proxies
// the proxy resolved for takes the profile where the proxy answers, so no
// name that proxy was handed is now looked up elsewhere; else one any of
// whose proxies listed servers takes those servers, so no name they
// answered is now asked of the host. A proxy whose DNS cannot be read
// counts as one the proxy resolved for. A route whose proxies had different
// settings is logged.
//
// A proxy's own address, when it is a name, was found by the host. A proxy
// whose setting listed servers asked from the egress now finds it by those
// servers instead: one proxy name profile on the same list for each distinct
// setting, shared by the proxies that had it. Every other proxy keeps the
// built-in System profile, which finds a first hop's address by the host and
// an inner hop's through the hop that carries it, as before.
//
// The resolver key is removed from every proxy, and the server list from
// every proxy but a WireGuard one whose tunnel asks it. The fields and
// values are spelled out here as they stood at schema 124, so later changes
// to the types do not change what this step does.

use std::collections::HashMap;

use serde_json::{Map, Value, json};

use crate::StateError;
use crate::persistence::sql_runtime::{SqlArg, SqlConn, SqlEngine, max_rows_for_engine};

pub(crate) const HOOK_ID: &str = "dns_profiles_from_proxies_v124";
// The schema that first keeps DNS profiles. A backup taken below it carries
// proxies that name a resolver instead.
pub(crate) const SCHEMA_VERSION: i64 = 124;

const RESOLVER: &str = "resolver";
const DNS_SERVERS: &str = "dns_servers";
// A key an unreleased build wrote; it is dropped like the resolver.
const DNS: &str = "dns";

// The DNS one legacy proxy stood for.
#[derive(Clone, PartialEq, Eq, Hash)]
enum Setting {
    System,
    Proxy,
    // Servers asked from the egress, or through the proxy.
    Servers {
        through_proxy: bool,
        servers: Vec<String>,
    },
}

// What the step makes of one proxy: its DNS, and its config rewritten.
fn upgraded(raw: &str) -> Option<(Setting, String)> {
    let mut document: Value = serde_json::from_str(raw).ok()?;
    let object = document.as_object_mut()?;
    let setting = setting_of(object)?;
    let wireguard = object.get("kind").and_then(Value::as_str) == Some("WireGuard");
    object.remove(RESOLVER);
    object.remove(DNS);
    let keeps_servers = wireguard && setting == Setting::Proxy;
    if !keeps_servers {
        object.remove(DNS_SERVERS);
    }
    Some((setting, serde_json::to_string(&document).ok()?))
}

fn setting_of(object: &Map<String, Value>) -> Option<Setting> {
    let proxy = match object.get(RESOLVER).and_then(Value::as_str) {
        None | Some("Weaver") => false,
        Some("Proxy") => true,
        Some(_) => return None,
    };
    let mut servers = match object.get(DNS_SERVERS) {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(servers)) => servers
            .iter()
            .map(|server| {
                let ip: std::net::IpAddr = server.as_str()?.parse().ok()?;
                Some(std::net::SocketAddr::new(ip, 53).to_string())
            })
            .collect::<Option<Vec<_>>>()?,
        Some(_) => return None,
    };
    // The old list took the same address twice; a list holds a server once,
    // in its first place, and at most ten.
    let mut seen = std::collections::HashSet::new();
    servers.retain(|server| seen.insert(server.clone()));
    servers.truncate(10);
    let wireguard = object.get("kind").and_then(Value::as_str) == Some("WireGuard");
    Some(match (proxy, servers.is_empty()) {
        (false, true) => Setting::System,
        (true, true) => Setting::Proxy,
        (true, false) if wireguard => Setting::Proxy,
        (through_proxy, false) => Setting::Servers {
            through_proxy,
            servers,
        },
    })
}

// The proxy, or pool, the first rung of a route's first leg through a proxy
// names, as the route was stored.
fn first_proxy(policy: &Value) -> Option<(Option<u32>, Option<u32>)> {
    let id = |value: &Value| value.as_u64().and_then(|id| u32::try_from(id).ok());
    if let Some(Value::Array(legs)) = policy.get("legs") {
        let Some(path) = legs
            .iter()
            .filter_map(|leg| leg.get("path"))
            .find(|path| path.get("kind").and_then(Value::as_str) == Some("ladder"))
        else {
            return Some((None, None));
        };
        let rung = path.get("rungs")?.as_array()?.first()?;
        return Some(match rung.get("kind").and_then(Value::as_str)? {
            "proxy" => (rung.get("id").and_then(id), None),
            "chain" => (rung.get("ids")?.as_array()?.last().and_then(id), None),
            "pool" => (None, rung.get("id").and_then(id)),
            _ => return None,
        });
    }
    let first = policy.get("proxyIds")?.as_array()?.first().and_then(id);
    Some((first, None))
}

// Every proxy a stored route's rungs end at, pool members included, in the
// order the route names them.
fn every_proxy(policy: &Value, pools: &HashMap<u32, Vec<u32>>) -> Vec<u32> {
    let id = |value: &Value| value.as_u64().and_then(|id| u32::try_from(id).ok());
    let Some(Value::Array(legs)) = policy.get("legs") else {
        return policy
            .get("proxyIds")
            .and_then(Value::as_array)
            .map(|ids| ids.iter().filter_map(id).collect())
            .unwrap_or_default();
    };
    let mut proxies = Vec::new();
    for leg in legs {
        let Some(Value::Array(rungs)) = leg.get("path").and_then(|path| path.get("rungs")) else {
            continue;
        };
        for rung in rungs {
            match rung.get("kind").and_then(Value::as_str) {
                Some("proxy") => proxies.extend(rung.get("id").and_then(id)),
                Some("chain") => proxies.extend(
                    rung.get("ids")
                        .and_then(Value::as_array)
                        .and_then(|ids| ids.last())
                        .and_then(id),
                ),
                Some("pool") => proxies.extend(
                    rung.get("id")
                        .and_then(id)
                        .and_then(|pool| pools.get(&pool))
                        .into_iter()
                        .flatten()
                        .copied(),
                ),
                _ => {}
            }
        }
    }
    proxies
}

// The setting a route takes from its proxies' settings, `first` the first
// rung's and `all` every rung's in route order: the proxy answering when any
// proxy answered, so no name a proxy was handed is now looked up elsewhere;
// else listed servers, the first rung's when it lists them, so no name a
// server answered is now asked of the host; else the first rung's.
fn chosen<'a>(first: Option<&'a Setting>, all: &[&'a Setting]) -> Option<&'a Setting> {
    let listed = |setting: &&Setting| matches!(setting, Setting::Servers { .. });
    if first == Some(&Setting::Proxy) || all.contains(&&Setting::Proxy) {
        Some(&Setting::Proxy)
    } else if let Some(servers) = first
        .filter(listed)
        .or_else(|| all.iter().copied().find(listed))
    {
        Some(servers)
    } else {
        first
    }
}

// Run the step on a connection that is already inside the transaction it
// belongs to.
pub(crate) async fn dns_profiles_from_proxies(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    let mut settings: HashMap<u32, Setting> = HashMap::new();
    let mut profiles: HashMap<Setting, u32> = HashMap::new();
    // The proxy name profile of each setting that lists servers asked from
    // the egress, and the proxies given each.
    let mut name_profiles: HashMap<Setting, u32> = HashMap::new();
    let mut named: HashMap<u32, Vec<i64>> = HashMap::new();
    let mut next_list = 1_i64;
    let mut next_profile = 1_u32;
    for row in conn
        .fetch_all("SELECT id, config FROM proxy_profiles ORDER BY id", &[])
        .await?
    {
        let id = row.i64("id")?;
        let raw = row.text("config")?;
        // A proxy whose DNS cannot be read may have been handed names, so
        // its routes keep handing them to the proxy.
        let (setting, config) = match upgraded(&raw) {
            Some((setting, config)) => {
                if config != raw {
                    conn.execute(
                        "UPDATE proxy_profiles SET config = {} WHERE id = {}",
                        &[SqlArg::Text(config.clone()), SqlArg::I64(id)],
                    )
                    .await?;
                }
                (setting, Some(config))
            }
            None => {
                tracing::warn!(
                    proxy = id,
                    "a saved proxy could not be read; its routes are given the DNS where the proxy answers"
                );
                (Setting::Proxy, None)
            }
        };
        let Ok(proxy) = u32::try_from(id) else {
            continue;
        };
        settings.insert(proxy, setting.clone());
        if let Some(profile) = name_profiles.get(&setting) {
            named.entry(*profile).or_default().push(id);
        }
        if setting == Setting::System || profiles.contains_key(&setting) {
            continue;
        }
        let name: String = config
            .as_deref()
            .and_then(|config| serde_json::from_str::<Value>(config).ok())
            .and_then(|document| document.get("name")?.as_str().map(str::to_owned))
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| format!("Proxy {proxy}"))
            .chars()
            .take(128)
            .collect();
        let (who, transit, list) = match &setting {
            Setting::System | Setting::Proxy => ("proxy", None, None),
            Setting::Servers {
                through_proxy,
                servers,
            } => {
                let stored = json!({
                    "v": 1,
                    "servers": servers
                        .iter()
                        .map(|address| json!({ "address": address, "wire": "Plain" }))
                        .collect::<Vec<_>>(),
                });
                conn.execute(
                    "INSERT INTO dns_server_lists (id, name, servers) VALUES ({}, {}, {})",
                    &[
                        SqlArg::I64(next_list),
                        SqlArg::Text(name.clone()),
                        SqlArg::Text(stored.to_string()),
                    ],
                )
                .await?;
                next_list += 1;
                (
                    "weaver",
                    Some(if *through_proxy { "proxy" } else { "egress" }),
                    Some(next_list - 1),
                )
            }
        };
        let profile = |id: u32, name: &str, kind: &str, transit: Option<&str>| {
            vec![
                SqlArg::I64(i64::from(id)),
                SqlArg::Text(name.to_owned()),
                SqlArg::Text(kind.into()),
                SqlArg::Text(who.into()),
                SqlArg::OptText(transit.map(str::to_owned)),
                SqlArg::Text("as_sent".into()),
                SqlArg::OptI64(list),
                SqlArg::Bool(setting == Setting::Proxy),
            ]
        };
        const INSERT: &str = "INSERT INTO dns_profiles (id, name, kind, who, transit, trust, server_list_id, system_fallback) VALUES ({}, {}, {}, {}, {}, {}, {}, {})";
        conn.execute(INSERT, &profile(next_profile, &name, "route", transit))
            .await?;
        profiles.insert(setting.clone(), next_profile);
        next_profile += 1;
        if matches!(
            setting,
            Setting::Servers {
                through_proxy: false,
                ..
            }
        ) {
            let name: String = format!("{name} (proxy address)")
                .chars()
                .take(128)
                .collect();
            conn.execute(
                INSERT,
                &profile(next_profile, &name, "proxy_name", Some("egress")),
            )
            .await?;
            name_profiles.insert(setting, next_profile);
            named.entry(next_profile).or_default().push(id);
            next_profile += 1;
        }
    }
    let engine = match conn {
        SqlConn::Sqlite(_) => SqlEngine::Sqlite,
        SqlConn::Postgres(_) => SqlEngine::Postgres,
    };
    for (profile, proxies) in named {
        // One bind is the profile's id.
        for chunk in proxies.chunks(max_rows_for_engine(engine, 1).saturating_sub(1).max(1)) {
            let placeholders = vec!["{}"; chunk.len()].join(", ");
            let mut args = vec![SqlArg::I64(i64::from(profile))];
            args.extend(chunk.iter().copied().map(SqlArg::I64));
            conn.execute(
                &format!(
                    "UPDATE proxy_profiles SET dns_profile_id = {{}} WHERE id IN ({placeholders})"
                ),
                &args,
            )
            .await?;
        }
    }
    if profiles.is_empty() {
        return Ok(());
    }
    let pools: HashMap<u32, Vec<u32>> = conn
        .fetch_all("SELECT id, member_ids FROM proxy_pools", &[])
        .await?
        .into_iter()
        .filter_map(|row| {
            let id = u32::try_from(row.i64("id").ok()?).ok()?;
            let members: Vec<u32> = serde_json::from_str(&row.text("member_ids").ok()?).ok()?;
            Some((id, members))
        })
        .collect();
    // The consumers given each profile, written one statement per profile.
    let mut given: HashMap<u32, Vec<String>> = HashMap::new();
    for row in conn
        .fetch_all("SELECT consumer, policy FROM proxy_routes", &[])
        .await?
    {
        let consumer = row.text("consumer")?;
        let Ok(policy) = serde_json::from_str::<Value>(&row.text("policy")?) else {
            continue;
        };
        let proxy = match first_proxy(&policy) {
            Some((Some(proxy), _)) => Some(proxy),
            Some((None, Some(pool))) => pools
                .get(&pool)
                .and_then(|members| members.first())
                .copied(),
            _ => None,
        };
        let first = proxy.and_then(|proxy| settings.get(&proxy));
        let all: Vec<&Setting> = every_proxy(&policy, &pools)
            .iter()
            .filter_map(|proxy| settings.get(proxy))
            .collect();
        if let Some(first) = first
            && all.iter().any(|setting| *setting != first)
        {
            tracing::warn!(
                route = %consumer,
                "the proxies of this route had differing DNS settings; it takes the proxy answering if any proxy did, else listed servers, else the host"
            );
        }
        let setting = chosen(first, &all);
        let Some(profile) = setting.and_then(|setting| profiles.get(setting)) else {
            continue;
        };
        given.entry(*profile).or_default().push(consumer);
    }
    for (profile, consumers) in given {
        // One bind is the profile's id.
        for chunk in consumers.chunks(max_rows_for_engine(engine, 1).saturating_sub(1).max(1)) {
            let placeholders = vec!["{}"; chunk.len()].join(", ");
            let mut args = vec![SqlArg::I64(i64::from(profile))];
            args.extend(chunk.iter().cloned().map(SqlArg::Text));
            conn.execute(
                &format!(
                    "UPDATE proxy_routes SET dns_profile_id = {{}} WHERE consumer IN ({placeholders})"
                ),
                &args,
            )
            .await?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "proxy_dns_v124_tests.rs"]
mod tests;
