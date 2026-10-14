// Migration 64, upgrade step: every saved proxy is given the resolver that
// keeps how it resolves names today.
//
// A proxy now says who resolves a name dialled through it, Weaver or the
// proxy, and its DNS server list belongs to that resolver. Before 64 a
// proxy's DNS servers were always asked through the proxy, and a proxy with
// none handed the name to the proxy, except WireGuard, which has no way to
// take a name and was resolved by Weaver's own DNS. So a WireGuard proxy
// with no DNS servers is resolved by Weaver and every other proxy by the
// proxy. A profile that already names its resolver is left alone.
//
// The fields and values are spelled out here as they stood at schema 64, so
// later changes to the profile type do not change what this step does.

use serde_json::Value;

use crate::StateError;
use crate::persistence::sql_runtime::{SqlArg, SqlConn};

pub(crate) const HOOK_ID: &str = "name_proxy_resolvers_v64";
// The schema that first names a proxy's resolver. A backup taken below it
// carries profiles without one.
pub(crate) const SCHEMA_VERSION: i64 = 64;

const RESOLVER: &str = "resolver";

// Run the step on a connection that is already inside the transaction it
// belongs to.
pub(crate) async fn name_proxy_resolvers(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    let rows = conn
        .fetch_all("SELECT id, config FROM proxy_profiles ORDER BY id", &[])
        .await?;
    for row in rows {
        let id = row.i64("id")?;
        let Some(named) = named(&row.text("config")?) else {
            continue;
        };
        conn.execute(
            "UPDATE proxy_profiles SET config = {} WHERE id = {}",
            &[SqlArg::Text(named), SqlArg::I64(id)],
        )
        .await?;
    }
    Ok(())
}

// The profile with its resolver named, or `None` when it already names one
// or cannot be read.
fn named(raw: &str) -> Option<String> {
    let mut document: Value = match serde_json::from_str(raw) {
        Ok(document) => document,
        Err(_) => {
            tracing::warn!("a saved proxy could not be read and was not given a resolver");
            return None;
        }
    };
    let object = document.as_object_mut()?;
    if object.contains_key(RESOLVER) {
        return None;
    }
    let wireguard = object.get("kind").and_then(Value::as_str) == Some("WireGuard");
    let no_servers = object
        .get("dns_servers")
        .and_then(Value::as_array)
        .is_none_or(Vec::is_empty);
    let resolver = if wireguard && no_servers {
        "Weaver"
    } else {
        "Proxy"
    };
    object.insert(RESOLVER.into(), Value::from(resolver));
    serde_json::to_string(&document).ok()
}

#[cfg(test)]
mod tests {
    use sqlx::SqlitePool;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::super::{apply_version_range, embedded_catalog, embedded_payload_bytes};
    use super::*;
    use crate::migration_assets::MigrationInstallKind;
    use crate::proxies::{ProxyKind, ProxyProfile, Resolver};

    async fn below_schema_64() -> SqlitePool {
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

    async fn upgrade_to_64(pool: &SqlitePool) {
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

    // A profile as a build below 64 saved it.
    fn before_64(kind: &str, dns_servers: &str) -> String {
        format!(
            r#"{{"id":0,"name":"proxy","kind":"{kind}","enabled":true,"host":"proxy.invalid","port":1080,"dns_servers":{dns_servers},"tunnel_addresses":[],"peer_public_key":null,"mtu":1420,"keepalive_seconds":null,"timeout_seconds":30,"host_key_fingerprint":null,"revision":3}}"#
        )
    }

    #[tokio::test]
    async fn an_upgrade_keeps_how_every_proxy_resolves() {
        let pool = below_schema_64().await;
        save(&pool, 1, &before_64("Ssh", "[]")).await;
        save(&pool, 2, &before_64("Socks5", r#"["192.0.2.53"]"#)).await;
        save(&pool, 3, &before_64("WireGuard", "[]")).await;
        save(&pool, 4, &before_64("WireGuard", r#"["10.0.0.2"]"#)).await;

        upgrade_to_64(&pool).await;

        let ssh = profile(&pool, 1).await;
        assert_eq!((ssh.kind, ssh.resolver), (ProxyKind::Ssh, Resolver::Proxy));
        assert!(ssh.dns_servers.is_empty());
        let socks = profile(&pool, 2).await;
        assert_eq!(socks.resolver, Resolver::Proxy);
        assert_eq!(
            socks.dns_servers,
            vec!["192.0.2.53".parse::<std::net::IpAddr>().unwrap()]
        );
        assert_eq!(socks.revision, 3);
        assert_eq!(profile(&pool, 3).await.resolver, Resolver::Weaver);
        assert_eq!(profile(&pool, 4).await.resolver, Resolver::Proxy);
    }

    #[tokio::test]
    async fn an_upgrade_keeps_a_named_resolver() {
        let pool = below_schema_64().await;
        let original =
            before_64("Ssh", "[]").replace(r#""enabled""#, r#""resolver":"Weaver","enabled""#);
        save(&pool, 1, &original).await;

        upgrade_to_64(&pool).await;

        assert_eq!(saved(&pool, 1).await, original);
    }

    #[tokio::test]
    async fn an_upgrade_without_proxies_writes_nothing() {
        let pool = below_schema_64().await;
        upgrade_to_64(&pool).await;
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM proxy_profiles")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
}
