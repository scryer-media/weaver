// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Migration 121, upgrade step: every saved proxy spells out its DNS as who
// answers, the path to its servers, the servers with the wire each is asked
// over, and how their answers are judged, keeping how it resolves names
// today.
//
// Before 121 a proxy named its resolver, Weaver or the proxy, and a list of
// server addresses that resolver asked, plain on port 53, taking answers as
// sent. Weaver with no servers is the host answering, and with servers those
// servers asked from the egress; the proxy with no servers is the proxy
// handed the name, and with servers those servers asked through it. The two
// old keys are removed. A profile that already spells out its DNS is left
// alone.
//
// The fields and values are spelled out here as they stood at schema 121,
// so later changes to the profile type do not change what this step does.

use serde_json::{Map, Value, json};

use crate::StateError;
use crate::persistence::sql_runtime::{SqlArg, SqlConn};

pub(crate) const HOOK_ID: &str = "spell_out_proxy_dns_v121";
// The schema that first spells out a proxy's DNS. A backup taken below it
// carries profiles that name a resolver instead.
pub(crate) const SCHEMA_VERSION: i64 = 121;

const DNS: &str = "dns";
const RESOLVER: &str = "resolver";
const DNS_SERVERS: &str = "dns_servers";

// Run the step on a connection that is already inside the transaction it
// belongs to.
pub(crate) async fn spell_out_proxy_dns(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    let rows = conn
        .fetch_all("SELECT id, config FROM proxy_profiles ORDER BY id", &[])
        .await?;
    for row in rows {
        let id = row.i64("id")?;
        let Some(spelled) = spelled_out(&row.text("config")?) else {
            continue;
        };
        conn.execute(
            "UPDATE proxy_profiles SET config = {} WHERE id = {}",
            &[SqlArg::Text(spelled), SqlArg::I64(id)],
        )
        .await?;
    }
    Ok(())
}

// The profile with its DNS spelled out, or `None` when it already spells it
// out or cannot be read.
fn spelled_out(raw: &str) -> Option<String> {
    let mut document: Value = match serde_json::from_str(raw) {
        Ok(document) => document,
        Err(_) => {
            tracing::warn!("a saved proxy could not be read and its DNS was not spelled out");
            return None;
        }
    };
    let object = document.as_object_mut()?;
    if object.contains_key(DNS) {
        return None;
    }
    let Some(dns) = dns_of(object) else {
        tracing::warn!("a saved proxy could not be read and its DNS was not spelled out");
        return None;
    };
    object.remove(RESOLVER);
    object.remove(DNS_SERVERS);
    object.insert(DNS.into(), dns);
    serde_json::to_string(&document).ok()
}

// The DNS a profile's resolver and server list stand for.
fn dns_of(object: &Map<String, Value>) -> Option<Value> {
    let proxy = match object.get(RESOLVER).and_then(Value::as_str)? {
        "Weaver" => false,
        "Proxy" => true,
        _ => return None,
    };
    let servers = match object.get(DNS_SERVERS) {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(servers)) => servers
            .iter()
            .map(|server| {
                let ip: std::net::IpAddr = server.as_str()?.parse().ok()?;
                Some(json!({
                    "address": std::net::SocketAddr::new(ip, 53).to_string(),
                    "wire": "Plain",
                }))
            })
            .collect::<Option<Vec<_>>>()?,
        Some(_) => return None,
    };
    // The old list took the same address twice; a server is now listed once,
    // so a repeat keeps only its first place.
    let mut servers = servers;
    let mut seen = std::collections::HashSet::new();
    servers.retain(|server| seen.insert(server["address"].to_string()));
    let who = match (proxy, servers.is_empty()) {
        (false, true) => "Host",
        (true, true) => "Proxy",
        (_, false) => "Servers",
    };
    let path = if servers.is_empty() || proxy {
        "ThroughProxy"
    } else {
        "Direct"
    };
    Some(json!({
        "who": who,
        "path": path,
        "servers": servers,
        "trust": "AsSent",
    }))
}

#[cfg(test)]
mod tests {
    use sqlx::SqlitePool;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::super::{apply_version_range, embedded_catalog, embedded_payload_bytes};
    use super::*;
    use crate::migration_assets::MigrationInstallKind;
    use crate::proxies::{
        DnsPath, DnsTrust, DnsWho, ProxyDns, ProxyDnsServer, ProxyDnsWire, ProxyKind, ProxyProfile,
        ResolvedBy,
    };

    async fn below_schema_121() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        super::super::replay_catalog_into_fresh_db(
            &pool,
            &embedded_catalog().unwrap(),
            &embedded_payload_bytes().unwrap(),
            Some(SCHEMA_VERSION - 1),
            true,
        )
        .await
        .unwrap();
        pool
    }

    async fn upgrade_to_121(pool: &SqlitePool) {
        apply_version_range(
            pool,
            &embedded_catalog().unwrap(),
            &embedded_payload_bytes().unwrap(),
            MigrationInstallKind::Upgrade,
            SCHEMA_VERSION,
            SCHEMA_VERSION,
        )
        .await
        .unwrap();
    }

    async fn save(pool: &SqlitePool, id: i64, config: &str) {
        sqlx::query("INSERT INTO proxy_profiles (id, config, password) VALUES (?1, ?2, NULL)")
            .bind(id)
            .bind(config)
            .execute(pool)
            .await
            .unwrap();
    }

    async fn saved(pool: &SqlitePool, id: i64) -> String {
        sqlx::query_scalar("SELECT config FROM proxy_profiles WHERE id = ?1")
            .bind(id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn profile(pool: &SqlitePool, id: i64) -> ProxyProfile {
        serde_json::from_str(&saved(pool, id).await).unwrap()
    }

    // A profile as a build below 121 saved it.
    fn before_121(kind: &str, resolver: &str, dns_servers: &str) -> String {
        format!(
            r#"{{"id":0,"name":"proxy","kind":"{kind}","enabled":true,"host":"proxy.invalid","port":1080,"resolver":"{resolver}","dns_servers":{dns_servers},"tunnel_addresses":[],"peer_public_key":null,"mtu":1420,"keepalive_seconds":null,"timeout_seconds":30,"host_key_fingerprint":null,"revision":3,"natpmp":false,"forwarded_port":null}}"#
        )
    }

    fn plain(address: &str) -> ProxyDnsServer {
        ProxyDnsServer {
            address: address.parse().unwrap(),
            wire: ProxyDnsWire::Plain,
        }
    }

    fn servers(path: DnsPath, servers: Vec<ProxyDnsServer>) -> ProxyDns {
        ProxyDns {
            who: DnsWho::Servers,
            path,
            servers,
            trust: DnsTrust::AsSent,
        }
    }

    #[tokio::test]
    async fn an_upgrade_keeps_how_every_proxy_resolves() {
        let pool = below_schema_121().await;
        save(&pool, 1, &before_121("Socks5", "Weaver", "[]")).await;
        save(
            &pool,
            2,
            &before_121("Socks5", "Weaver", r#"["192.0.2.53","2001:db8::53"]"#),
        )
        .await;
        save(&pool, 3, &before_121("Ssh", "Proxy", "[]")).await;
        save(
            &pool,
            4,
            &before_121("Socks5", "Proxy", r#"["192.0.2.53"]"#),
        )
        .await;

        upgrade_to_121(&pool).await;

        let host = profile(&pool, 1).await;
        assert_eq!(host.dns, ProxyDns::host());
        assert_eq!(host.resolved_by(), ResolvedBy::Host);
        let direct = profile(&pool, 2).await;
        assert_eq!(
            direct.dns,
            servers(
                DnsPath::Direct,
                vec![plain("192.0.2.53:53"), plain("[2001:db8::53]:53")]
            )
        );
        assert_eq!(direct.resolved_by(), ResolvedBy::Servers);
        assert_eq!(direct.revision, 3);
        let proxy = profile(&pool, 3).await;
        assert_eq!(
            (proxy.kind, proxy.dns.clone()),
            (ProxyKind::Ssh, ProxyDns::proxy())
        );
        assert_eq!(proxy.resolved_by(), ResolvedBy::Proxy);
        let through = profile(&pool, 4).await;
        assert_eq!(
            through.dns,
            servers(DnsPath::ThroughProxy, vec![plain("192.0.2.53:53")])
        );
        assert_eq!(through.resolved_by(), ResolvedBy::ProxyServers);
        for id in 1..=4 {
            let raw: Value = serde_json::from_str(&saved(&pool, id).await).unwrap();
            assert!(raw.get(RESOLVER).is_none() && raw.get(DNS_SERVERS).is_none());
        }
        // A save takes each mapped DNS. The SSH proxy is left out: its keys
        // are stored apart and are not under test.
        for id in [1, 2, 4] {
            profile(&pool, id).await.validate().unwrap();
        }
    }

    // A WireGuard proxy left to Weaver with no servers is resolved by the
    // host, and one with servers asks them through the tunnel, as before.
    #[tokio::test]
    async fn an_upgrade_keeps_how_a_wireguard_proxy_resolves() {
        let pool = below_schema_121().await;
        save(&pool, 1, &before_121("WireGuard", "Weaver", "[]")).await;
        save(
            &pool,
            2,
            &before_121("WireGuard", "Proxy", r#"["10.0.0.2"]"#),
        )
        .await;
        save(
            &pool,
            3,
            &before_121("WireGuard", "Weaver", r#"["10.0.0.2"]"#),
        )
        .await;

        upgrade_to_121(&pool).await;

        assert_eq!(profile(&pool, 1).await.dns, ProxyDns::host());
        let through = profile(&pool, 2).await;
        assert_eq!(
            through.dns,
            servers(DnsPath::ThroughProxy, vec![plain("10.0.0.2:53")])
        );
        assert_eq!(
            through
                .proxy_dns_servers()
                .iter()
                .map(|server| server.address.ip())
                .collect::<Vec<_>>(),
            vec!["10.0.0.2".parse::<std::net::IpAddr>().unwrap()]
        );
        // Asked from the egress, outside the tunnel, as it was saved.
        let direct = profile(&pool, 3).await;
        assert_eq!(
            direct.dns,
            servers(DnsPath::Direct, vec![plain("10.0.0.2:53")])
        );
        assert!(direct.proxy_dns_servers().is_empty());
    }

    // The old list could name one address twice; the upgrade keeps it once,
    // in its first place, so the proxy still saves.
    #[tokio::test]
    async fn an_upgrade_lists_a_repeated_server_once() {
        let pool = below_schema_121().await;
        save(
            &pool,
            1,
            &before_121(
                "Socks5",
                "Weaver",
                r#"["192.0.2.53","198.51.100.53","192.0.2.53"]"#,
            ),
        )
        .await;

        upgrade_to_121(&pool).await;

        let upgraded = profile(&pool, 1).await;
        assert_eq!(
            upgraded.dns,
            servers(
                DnsPath::Direct,
                vec![plain("192.0.2.53:53"), plain("198.51.100.53:53")]
            )
        );
        upgraded.validate().unwrap();
    }

    #[tokio::test]
    async fn an_upgrade_is_idempotent_and_keeps_a_spelled_out_dns() {
        let pool = below_schema_121().await;
        save(
            &pool,
            1,
            &before_121("Socks5", "Proxy", r#"["192.0.2.53"]"#),
        )
        .await;
        upgrade_to_121(&pool).await;
        let once = saved(&pool, 1).await;
        let mut conn = pool.acquire().await.unwrap();
        spell_out_proxy_dns(&mut SqlConn::Sqlite(&mut conn))
            .await
            .unwrap();
        drop(conn);
        assert_eq!(saved(&pool, 1).await, once);
        assert_eq!(spelled_out(&once), None);
    }

    #[tokio::test]
    async fn an_upgrade_leaves_an_unreadable_proxy_alone() {
        let pool = below_schema_121().await;
        save(&pool, 1, "{not json").await;
        let unknown = before_121("Socks5", "Elsewhere", "[]");
        save(&pool, 2, &unknown).await;
        let bad_server = before_121("Socks5", "Proxy", r#"["not-an-ip"]"#);
        save(&pool, 3, &bad_server).await;

        upgrade_to_121(&pool).await;

        assert_eq!(saved(&pool, 1).await, "{not json");
        assert_eq!(saved(&pool, 2).await, unknown);
        assert_eq!(saved(&pool, 3).await, bad_server);
    }

    #[tokio::test]
    async fn an_upgrade_without_proxies_writes_nothing() {
        let pool = below_schema_121().await;
        upgrade_to_121(&pool).await;
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM proxy_profiles")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
}
