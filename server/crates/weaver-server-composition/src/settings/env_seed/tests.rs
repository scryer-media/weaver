// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::proxies::ProxiesNetworkPersistenceQueries;
use crate::servers::queries::ServersQueries;
use crate::settings::persistence::SettingsPersistenceQueries;
use crate::settings::queries::SettingsQueries;
use crate::settings::service::SettingsServiceQueries;
use weaver_server_settings::categories::CategoryConfig;

fn parse(vars: &[(&str, &str)]) -> Result<EnvSeedConfig, EnvSeedError> {
    parse_env_seed(vars.iter().copied())
}

#[test]
fn parses_single_server_defaults() {
    let seed = parse(&[("WEAVER_SERVER_1_HOSTNAME", "news.example.com")]).unwrap();

    assert_eq!(seed.servers.len(), 1);
    let server = &seed.servers[0].config;
    assert_eq!(server.id, 1);
    assert_eq!(server.host, "news.example.com");
    assert_eq!(server.port, 563);
    assert!(server.tls);
    assert_eq!(server.connections, 10);
    assert!(server.active);
    assert_eq!(server.priority, 0);
    assert!(!server.supports_pipelining);
    assert!(!seed.servers[0].pipelining_pinned);
    assert_eq!(server.max_download_speed, 0);
    assert_eq!(server.download_quota, ServerDownloadQuotaConfig::default());
}

#[test]
fn parses_server_download_limits() {
    let seed = parse(&[
        ("WEAVER_SERVER_1_HOSTNAME", "news.example.com"),
        ("WEAVER_SERVER_1_MAX_DOWNLOAD_SPEED", "2500000"),
        ("WEAVER_SERVER_1_DOWNLOAD_QUOTA_ENABLED", "true"),
        ("WEAVER_SERVER_1_DOWNLOAD_QUOTA_LIMIT_BYTES", "9000000"),
        ("WEAVER_SERVER_1_DOWNLOAD_QUOTA_PERIOD", "weekly"),
        (
            "WEAVER_SERVER_1_DOWNLOAD_QUOTA_RESET_TIME_MINUTES_LOCAL",
            "375",
        ),
        ("WEAVER_SERVER_1_DOWNLOAD_QUOTA_WEEKLY_RESET_WEEKDAY", "thu"),
        ("WEAVER_SERVER_1_DOWNLOAD_QUOTA_MONTHLY_RESET_DAY", "31"),
    ])
    .unwrap();

    let server = &seed.servers[0].config;
    assert_eq!(server.max_download_speed, 2_500_000);
    assert!(server.download_quota.enabled);
    assert_eq!(server.download_quota.limit_bytes, 9_000_000);
    assert_eq!(
        server.download_quota.period,
        ServerDownloadQuotaPeriod::Weekly
    );
    assert_eq!(server.download_quota.reset_time_minutes_local, 375);
    assert_eq!(
        server.download_quota.weekly_reset_weekday,
        QuotaWeekday::Thu
    );
    assert_eq!(server.download_quota.monthly_reset_day, 31);
}

#[test]
fn parses_multiple_servers_and_plain_port_default() {
    let seed = parse(&[
        ("WEAVER_SERVER_1_HOSTNAME", "plain.example.com"),
        ("WEAVER_SERVER_1_TLS", "false"),
        ("WEAVER_SERVER_1_CONNECTIONS", "4"),
        ("WEAVER_SERVER_2_HOSTNAME", "tls.example.com"),
        ("WEAVER_SERVER_2_PORT", "443"),
        ("WEAVER_SERVER_2_USERNAME", " user "),
        ("WEAVER_SERVER_2_PASSWORD", " pass "),
        ("WEAVER_SERVER_2_PRIORITY", "1"),
        ("WEAVER_SERVER_2_TLS_CA_CERT", "/etc/ssl/custom.pem"),
        ("WEAVER_SERVER_2_PIPELINING", "true"),
    ])
    .unwrap();

    assert_eq!(seed.servers.len(), 2);
    // Unstated is not the same answer as stated false: only the second
    // server carries a pin the probe must leave alone.
    assert!(!seed.servers[0].config.supports_pipelining);
    assert!(!seed.servers[0].pipelining_pinned);
    assert!(seed.servers[1].config.supports_pipelining);
    assert!(seed.servers[1].pipelining_pinned);
    assert_eq!(seed.servers[1].config.pipelining_depth, None);
    assert_eq!(seed.servers[0].config.port, 119);
    assert!(!seed.servers[0].config.tls);
    assert_eq!(seed.servers[0].config.connections, 4);
    assert_eq!(seed.servers[1].config.port, 443);
    assert_eq!(seed.servers[1].config.username.as_deref(), Some("user"));
    assert_eq!(seed.servers[1].config.password.as_deref(), Some("pass"));
    assert_eq!(seed.servers[1].config.priority, 1);
    assert_eq!(
        seed.servers[1].config.tls_ca_cert.as_deref(),
        Some(std::path::Path::new("/etc/ssl/custom.pem"))
    );
}

#[test]
fn records_an_explicitly_disabled_pipelining_flag_as_a_pin() {
    let seed = parse(&[
        ("WEAVER_SERVER_1_HOSTNAME", "news.example.com"),
        ("WEAVER_SERVER_1_PIPELINING", "false"),
    ])
    .unwrap();

    assert!(!seed.servers[0].config.supports_pipelining);
    assert!(seed.servers[0].pipelining_pinned);
}

#[test]
fn rejects_non_contiguous_server_indexes() {
    let error = parse(&[("WEAVER_SERVER_2_HOSTNAME", "news.example.com")]).unwrap_err();

    assert!(error.to_string().contains("contiguous from 1"));
}

#[test]
fn rejects_server_without_hostname() {
    let error = parse(&[("WEAVER_SERVER_1_PORT", "563")]).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("WEAVER_SERVER_1_HOSTNAME is required")
    );
}

#[test]
fn rejects_invalid_values() {
    assert!(
        parse(&[("WEAVER_CLEANUP_AFTER_EXTRACT", "maybe")])
            .unwrap_err()
            .to_string()
            .contains("boolean")
    );
    assert!(
        parse(&[
            ("WEAVER_SERVER_1_HOSTNAME", "news"),
            ("WEAVER_SERVER_1_PORT", "0")
        ])
        .unwrap_err()
        .to_string()
        .contains("1 to 65535")
    );
    assert!(
        parse(&[("WEAVER_MAX_DOWNLOAD_SPEED", "fast")])
            .unwrap_err()
            .to_string()
            .contains("unsigned integer")
    );
    assert!(
        parse(&[
            ("WEAVER_SERVER_1_HOSTNAME", "news"),
            ("WEAVER_SERVER_1_DOWNLOAD_QUOTA_ENABLED", "true")
        ])
        .unwrap_err()
        .to_string()
        .contains("greater than zero")
    );
    assert!(
        parse(&[
            ("WEAVER_SERVER_1_HOSTNAME", "news"),
            (
                "WEAVER_SERVER_1_DOWNLOAD_QUOTA_RESET_TIME_MINUTES_LOCAL",
                "1440"
            )
        ])
        .unwrap_err()
        .to_string()
        .contains("0 to 1439")
    );

    let outside_database_range = ((i64::MAX as u64) + 1).to_string();
    assert!(
        parse(&[
            ("WEAVER_SERVER_1_HOSTNAME", "news"),
            (
                "WEAVER_SERVER_1_MAX_DOWNLOAD_SPEED",
                outside_database_range.as_str()
            )
        ])
        .unwrap_err()
        .to_string()
        .contains("max download speed exceeds database range")
    );
    assert!(
        parse(&[
            ("WEAVER_SERVER_1_HOSTNAME", "news"),
            (
                "WEAVER_SERVER_1_DOWNLOAD_QUOTA_LIMIT_BYTES",
                outside_database_range.as_str()
            )
        ])
        .unwrap_err()
        .to_string()
        .contains("download quota limit exceeds database range")
    );
}

#[test]
fn seeds_missing_core_settings() {
    let db = Database::open_in_memory().unwrap();
    let mut config = db.load_config().unwrap();
    let seed = parse(&[
        ("WEAVER_DATA_DIR", "/config"),
        ("WEAVER_INTERMEDIATE_DIR", "/downloads/incomplete"),
        ("WEAVER_COMPLETE_DIR", "/downloads/complete"),
        ("WEAVER_MAX_DOWNLOAD_SPEED", "1000"),
        ("WEAVER_CLEANUP_AFTER_EXTRACT", "false"),
    ])
    .unwrap();

    let seeded = apply_core_seed(&db, &mut config, &seed).unwrap();

    assert_eq!(seeded, 5);
    assert_eq!(config.data_dir, "/config");
    assert_eq!(
        config.intermediate_dir.as_deref(),
        Some("/downloads/incomplete")
    );
    assert_eq!(config.complete_dir.as_deref(), Some("/downloads/complete"));
    assert_eq!(config.bandwidth.download_bps, Some(1000));
    assert_eq!(config.cleanup_after_extract, Some(false));
    assert_eq!(
        db.get_setting("data_dir").unwrap().as_deref(),
        Some("/config")
    );
    assert_eq!(db.network_bandwidth().unwrap().download_bps, Some(1000));
}

#[test]
fn an_unstored_data_dir_default_yields_to_the_environment() {
    let db = Database::open_in_memory().unwrap();
    let mut config = db.load_config().unwrap();
    // What startup leaves behind when nothing is stored: a folder derived
    // from the config path, held in memory only.
    config.data_dir = "/config".to_string();
    let seed = parse(&[("WEAVER_DATA_DIR", "/env")]).unwrap();

    let seeded = apply_core_seed(&db, &mut config, &seed).unwrap();

    assert_eq!(seeded, 1);
    assert_eq!(config.data_dir, "/env");
    assert_eq!(db.get_setting("data_dir").unwrap().as_deref(), Some("/env"));
}

#[test]
fn an_unstored_data_dir_default_is_stored_when_the_environment_names_none() {
    let db = Database::open_in_memory().unwrap();
    let mut config = db.load_config().unwrap();
    config.data_dir = "/config".to_string();
    let seed = parse(&[("WEAVER_COMPLETE_DIR", "/downloads/complete")]).unwrap();

    let seeded = apply_core_seed(&db, &mut config, &seed).unwrap();

    assert_eq!(seeded, 1);
    assert_eq!(config.data_dir, "/config");
    assert_eq!(
        db.get_setting("data_dir").unwrap().as_deref(),
        Some("/config")
    );
    assert_eq!(db.load_config().unwrap().data_dir, "/config");
}

#[test]
fn parses_scripts_directory_seed_without_applying_it_as_a_general_setting() {
    let seed = parse(&[("WEAVER_SCRIPTS_DIR", "/scripts")]).unwrap();
    assert_eq!(seed.core.scripts_dir.as_deref(), Some("/scripts"));
}

#[test]
fn preserves_existing_core_settings() {
    let db = Database::open_in_memory().unwrap();
    db.set_setting("data_dir", "/db").unwrap();
    db.set_network_bandwidth(&crate::proxies::Bandwidth {
        download_bps: Some(2000),
        upload_bps: None,
        split: None,
    })
    .unwrap();
    let mut config = db.load_config().unwrap();
    let seed = parse(&[
        ("WEAVER_DATA_DIR", "/env"),
        ("WEAVER_MAX_DOWNLOAD_SPEED", "1000"),
    ])
    .unwrap();

    let seeded = apply_core_seed(&db, &mut config, &seed).unwrap();

    assert_eq!(seeded, 0);
    assert_eq!(config.data_dir, "/db");
    assert_eq!(config.bandwidth.download_bps, Some(2000));
    assert_eq!(db.get_setting("data_dir").unwrap().as_deref(), Some("/db"));
}

#[test]
fn seeds_servers_only_when_database_has_no_servers() {
    let db = Database::open_in_memory().unwrap();
    let mut config = db.load_config().unwrap();
    let seed = parse(&[
        ("WEAVER_SERVER_1_HOSTNAME", "news.example.com"),
        ("WEAVER_SERVER_1_USERNAME", "user"),
    ])
    .unwrap();

    let seeded = apply_server_seed(&db, &mut config, &seed).unwrap();

    assert_eq!(seeded, 1);
    assert_eq!(config.servers.len(), 1);
    let persisted = db.list_servers().unwrap();
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].host, "news.example.com");
    assert_eq!(persisted[0].username.as_deref(), Some("user"));
}

fn probe_answer(success: bool, supports_pipelining: bool) -> ServerConnectivityResult {
    ServerConnectivityResult {
        success,
        message: if success {
            "Connected successfully".to_string()
        } else {
            "connection refused".to_string()
        },
        latency_ms: success.then_some(12),
        first_byte_latency_ms: success.then_some(34),
        first_byte_latency_band: None,
        supports_pipelining: success && supports_pipelining,
        adoptable_tls_name_mismatch_certificate_der: None,
        tls_cipher_suite: None,
        tls_honors_client_cipher_order: None,
    }
}

#[tokio::test]
async fn stores_what_the_probe_found_for_a_server_left_unstated() {
    let db = Database::open_in_memory().unwrap();
    let mut config = db.load_config().unwrap();
    let mut seed = parse(&[("WEAVER_SERVER_1_HOSTNAME", "news.example.com")]).unwrap();

    let outcomes =
        probe_seeded_server_pipelining(&mut seed, |_| async { probe_answer(true, true) }).await;
    apply_server_seed(&db, &mut config, &seed).unwrap();

    assert_eq!(
        outcomes,
        vec![SeededServerProbeOutcome {
            server_id: 1,
            supports_pipelining: true,
            first_byte_latency_ms: Some(34),
            failure: None,
        }]
    );
    assert!(config.servers[0].supports_pipelining);
    assert!(db.list_servers().unwrap()[0].supports_pipelining);
}

#[tokio::test]
async fn keeps_the_sequential_default_when_a_seeded_server_cannot_be_reached() {
    let db = Database::open_in_memory().unwrap();
    let mut config = db.load_config().unwrap();
    let mut seed = parse(&[("WEAVER_SERVER_1_HOSTNAME", "news.example.com")]).unwrap();

    let outcomes =
        probe_seeded_server_pipelining(&mut seed, |_| async { probe_answer(false, true) }).await;
    let seeded = apply_server_seed(&db, &mut config, &seed).unwrap();

    assert_eq!(seeded, 1);
    assert_eq!(outcomes[0].failure.as_deref(), Some("connection refused"));
    assert!(!outcomes[0].supports_pipelining);
    assert!(!config.servers[0].supports_pipelining);
    assert!(!db.list_servers().unwrap()[0].supports_pipelining);
}

#[tokio::test]
async fn leaves_a_stated_pipelining_flag_alone() {
    let db = Database::open_in_memory().unwrap();
    let mut config = db.load_config().unwrap();
    let mut seed = parse(&[
        ("WEAVER_SERVER_1_HOSTNAME", "news.example.com"),
        ("WEAVER_SERVER_1_PIPELINING", "false"),
    ])
    .unwrap();

    let outcomes =
        probe_seeded_server_pipelining(&mut seed, |_| async { probe_answer(true, true) }).await;
    apply_server_seed(&db, &mut config, &seed).unwrap();

    assert!(outcomes.is_empty());
    assert!(!config.servers[0].supports_pipelining);
    assert!(!db.list_servers().unwrap()[0].supports_pipelining);
}

#[tokio::test(start_paused = true)]
async fn asks_every_seeded_server_at_once_under_one_deadline() {
    let mut seed = parse(&[
        ("WEAVER_SERVER_1_HOSTNAME", "slow.example.com"),
        ("WEAVER_SERVER_2_HOSTNAME", "slower.example.com"),
        ("WEAVER_SERVER_3_HOSTNAME", "silent.example.com"),
    ])
    .unwrap();
    let started = tokio::time::Instant::now();

    // Asked one after another, the first two would not both fit inside the
    // deadline, and the silent one would hold boot forever.
    let outcomes = probe_seeded_server_pipelining(&mut seed, |server| async move {
        match server.id {
            1 | 2 => {
                tokio::time::sleep(SEED_PROBE_DEADLINE * 3 / 5).await;
                probe_answer(true, true)
            }
            _ => std::future::pending().await,
        }
    })
    .await;

    assert_eq!(started.elapsed(), SEED_PROBE_DEADLINE);
    assert_eq!(
        outcomes
            .iter()
            .map(|outcome| (outcome.server_id, outcome.failure.is_none()))
            .collect::<Vec<_>>(),
        vec![(1, true), (2, true), (3, false)]
    );
    assert!(seed.servers[0].config.supports_pipelining);
    assert!(seed.servers[1].config.supports_pipelining);
    assert!(!seed.servers[2].config.supports_pipelining);
    assert!(
        outcomes[2]
            .failure
            .as_deref()
            .is_some_and(|reason| reason.contains("no answer"))
    );
}

#[tokio::test]
async fn does_not_ask_an_inactive_seeded_server() {
    let mut seed = parse(&[
        ("WEAVER_SERVER_1_HOSTNAME", "news.example.com"),
        ("WEAVER_SERVER_1_ACTIVE", "false"),
    ])
    .unwrap();

    let outcomes =
        probe_seeded_server_pipelining(&mut seed, |_| async { probe_answer(true, true) }).await;

    assert!(outcomes.is_empty());
    assert!(!seed.servers[0].config.supports_pipelining);
}

#[test]
fn asks_nothing_when_the_database_already_has_servers() {
    let db = Database::open_in_memory().unwrap();
    let mut config = db.load_config().unwrap();
    config.servers = db.list_servers().unwrap();
    let seed = parse(&[("WEAVER_SERVER_1_HOSTNAME", "news.example.com")]).unwrap();
    assert!(server_seed_applies(&config, &seed));

    config.servers = vec![existing_server()];

    assert!(!server_seed_applies(&config, &seed));
}

fn existing_server() -> ServerConfig {
    ServerConfig {
        id: 1,
        host: "ui.example.com".to_string(),
        port: 563,
        tls: true,
        username: None,
        password: None,
        connections: 5,
        active: true,
        supports_pipelining: false,
        pipelining_depth: None,
        tls_name_mismatch_certificate_der: None,
        priority: 0,
        backfill: false,
        retention_days: 0,
        max_download_speed: 0,
        download_quota: ServerDownloadQuotaConfig::default(),
        tls_ca_cert: None,
    }
}

#[test]
fn preserves_existing_servers() {
    let db = Database::open_in_memory().unwrap();
    let existing = Config {
        upload_cap: None,
        seeding: None,
        torrent: Default::default(),
        data_dir: "/db".to_string(),
        hardware_profile: None,
        intermediate_dir: None,
        complete_dir: None,
        servers: vec![existing_server()],
        categories: vec![CategoryConfig {
            upload_cap: Default::default(),
            seeding: None,
            torrent_route: false,
            unpack: None,
            id: 1,
            name: "Movies".to_string(),
            dest_dir: None,
            aliases: String::new(),
        }],
        retry: None,
        bandwidth: Default::default(),
        cleanup_after_extract: None,
        delete_failed_job_files: None,
        history_retention_days: None,
        propagation_delay_secs: None,
        max_concurrent_nzbs: None,
        watch_folder: weaver_server_ingest::watch_folder::WatchFolderConfig::default(),
        duplicate_policy: Default::default(),
        direct_store: None,
        delivery_naming: None,
        metrics: Default::default(),
        config_path: None,
    };
    db.save_config(&existing).unwrap();
    let mut config = db.load_config().unwrap();
    let seed = parse(&[("WEAVER_SERVER_1_HOSTNAME", "env.example.com")]).unwrap();

    let seeded = apply_server_seed(&db, &mut config, &seed).unwrap();

    assert_eq!(seeded, 0);
    assert_eq!(config.servers[0].host, "ui.example.com");
    assert_eq!(db.list_servers().unwrap()[0].host, "ui.example.com");
}
