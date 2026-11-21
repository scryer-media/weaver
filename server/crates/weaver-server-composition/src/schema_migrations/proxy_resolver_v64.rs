// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub(crate) use weaver_server_migrations::schema_migrations::proxy_resolver_v64::*;

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use sqlx::SqlitePool;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::super::{apply_version_range, embedded_catalog, embedded_payload_bytes};
    use super::*;
    use crate::migration_assets::MigrationInstallKind;

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

    // The profile as schema 64 stored it. Later steps change its shape, so
    // it is read as the JSON this step wrote.
    async fn profile(pool: &SqlitePool, id: i64) -> Value {
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
        assert_eq!(
            (&ssh["kind"], &ssh[RESOLVER]),
            (&"Ssh".into(), &"Proxy".into())
        );
        assert_eq!(ssh["dns_servers"], serde_json::json!([]));
        let socks = profile(&pool, 2).await;
        assert_eq!(socks[RESOLVER], "Proxy");
        assert_eq!(socks["dns_servers"], serde_json::json!(["192.0.2.53"]));
        assert_eq!(socks["revision"], 3);
        assert_eq!(profile(&pool, 3).await[RESOLVER], "Weaver");
        assert_eq!(profile(&pool, 4).await[RESOLVER], "Proxy");

        // What this step names, the later shape reads once the upgrade to
        // DNS profiles has run over it.
        apply_version_range(
            &pool,
            &embedded_catalog().unwrap(),
            &embedded_payload_bytes().unwrap(),
            MigrationInstallKind::Upgrade,
            SCHEMA_VERSION + 1,
            super::super::proxy_dns_v125::SCHEMA_VERSION,
        )
        .await
        .unwrap();
        for id in 1..=4 {
            let read: crate::proxies::ProxyProfile =
                serde_json::from_str(&saved(&pool, id).await).unwrap();
            assert_eq!(read.revision, 3);
        }
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
