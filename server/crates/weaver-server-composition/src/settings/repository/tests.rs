// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::SettingsRepositoryQueries;
use crate::Database;
use crate::persistence::encryption::is_encrypted;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::proxies::ProxiesPersistenceQueries;
use crate::servers::ServerConfig;
use crate::servers::persistence::ServersPersistenceQueries;
use crate::servers::queries::ServersQueries;
use crate::settings::Config;
use crate::settings::SettingsArchivePasswordsQueries;
use crate::settings::archive_passwords::SettingsArchivePasswordsInternalQueries;
use crate::settings::model::{Client, Leaf, TrackerRule};
use crate::settings::persistence::SettingsPersistenceQueries;
use crate::settings::queries::SettingsQueries;
use crate::settings::service::SettingsServiceQueries;
use weaver_server_settings::categories::CategoryConfig;
use weaver_server_settings::categories::persistence::CategoriesPersistenceQueries;
use weaver_server_settings::categories::queries::CategoriesQueries;
use weaver_server_settings::categories::service::CategoriesServiceInternalQueries;

#[test]
fn settings_roundtrip() {
    let db = Database::open_in_memory().unwrap();
    db.set_setting("foo", "bar").unwrap();
    assert_eq!(db.get_setting("foo").unwrap(), Some("bar".to_string()));
    assert_eq!(db.get_setting("missing").unwrap(), None);
}

// A database that names no torrent listen port draws one on its first
// load, from the listen ports, and every later load keeps it.
#[test]
fn a_database_naming_no_listen_port_draws_one_and_keeps_it() {
    let db = Database::open_in_memory().unwrap();
    let drawn = db.load_config().unwrap().torrent.base_port;
    assert!(crate::torrents::sessions::LISTEN_PORTS.contains(&drawn));
    assert_eq!(
        db.get_setting("torrent.base_port").unwrap(),
        Some(drawn.to_string())
    );
    assert_eq!(db.load_config().unwrap().torrent.base_port, drawn);
    db.set_setting("torrent.base_port", "6881").unwrap();
    assert_eq!(db.load_config().unwrap().torrent.base_port, 6881);
}

// While the environment pins the listen port, a database naming none
// reports the pin, draws no port and writes no row for it.
#[test]
fn a_pinned_listen_port_with_none_saved_is_the_pin_and_writes_no_row() {
    let mut db = Database::open_in_memory().unwrap();
    db.set_torrent_port_pin(Some(51_413));
    let torrent = db.load_config().unwrap().torrent;
    assert_eq!(torrent.base_port, 51_413);
    assert!(torrent.base_port_pinned);
    assert_eq!(db.get_setting("torrent.base_port").unwrap(), None);
    db.load_config().unwrap();
    assert_eq!(db.get_setting("torrent.base_port").unwrap(), None);
}

// A saved port is left as it is while the pin is the one reported, and
// comes back once the pin is gone.
#[test]
fn a_pinned_listen_port_over_a_saved_one_leaves_the_saved_row() {
    let mut db = Database::open_in_memory().unwrap();
    db.set_setting("torrent.base_port", "6881").unwrap();
    db.set_torrent_port_pin(Some(51_413));
    let torrent = db.load_config().unwrap().torrent;
    assert_eq!(torrent.base_port, 51_413);
    assert!(torrent.base_port_pinned);
    assert_eq!(
        db.get_setting("torrent.base_port").unwrap().as_deref(),
        Some("6881")
    );
    db.set_torrent_port_pin(None);
    let torrent = db.load_config().unwrap().torrent;
    assert_eq!(torrent.base_port, 6881);
    assert!(!torrent.base_port_pinned);
}

// Removing the pin from a database that never saved a port draws one, as
// a database without a pin always does.
#[test]
fn an_unpinned_load_after_a_pinned_one_draws_as_today() {
    let mut db = Database::open_in_memory().unwrap();
    db.set_torrent_port_pin(Some(51_413));
    db.load_config().unwrap();
    db.set_torrent_port_pin(None);
    let torrent = db.load_config().unwrap().torrent;
    assert!(!torrent.base_port_pinned);
    assert!(crate::torrents::sessions::LISTEN_PORTS.contains(&torrent.base_port));
    assert_eq!(
        db.get_setting("torrent.base_port").unwrap(),
        Some(torrent.base_port.to_string())
    );
}

// Saving pinned settings writes every other torrent key and keeps the
// port row as it was: unchanged when saved, absent when not.
#[test]
fn saving_pinned_torrent_settings_leaves_the_port_row_as_it_was() {
    let mut db = Database::open_in_memory().unwrap();
    db.set_torrent_port_pin(Some(51_413));
    let mut torrent = db.load_config().unwrap().torrent;
    torrent.max_active_downloads = 7;
    db.save_torrent_config(&torrent).unwrap();
    assert_eq!(db.get_setting("torrent.base_port").unwrap(), None);
    assert_eq!(
        db.get_setting("torrent.max_active_downloads")
            .unwrap()
            .as_deref(),
        Some("7")
    );

    db.set_setting("torrent.base_port", "6881").unwrap();
    db.save_torrent_config(&torrent).unwrap();
    assert_eq!(
        db.get_setting("torrent.base_port").unwrap().as_deref(),
        Some("6881")
    );

    // Unpinned, the save writes the port as it always has.
    torrent.base_port_pinned = false;
    db.save_torrent_config(&torrent).unwrap();
    assert_eq!(
        db.get_setting("torrent.base_port").unwrap().as_deref(),
        Some("51413")
    );
}

#[test]
fn settings_upsert() {
    let db = Database::open_in_memory().unwrap();
    db.set_setting("key", "v1").unwrap();
    db.set_setting("key", "v2").unwrap();
    assert_eq!(db.get_setting("key").unwrap(), Some("v2".to_string()));
}

#[test]
fn server_crud() {
    let db = Database::open_in_memory().unwrap();

    let server = ServerConfig {
        id: 1,
        host: "news.example.com".to_string(),
        port: 443,
        tls: true,
        username: Some("user".to_string()),
        password: Some("pass".to_string()),
        connections: 10,
        active: true,
        supports_pipelining: false,
        pipelining_depth: None,
        tls_name_mismatch_certificate_der: Some(vec![0x30, 0x82, 0x01, 0x0a]),
        priority: 0,
        backfill: false,
        retention_days: 0,
        max_download_speed: 2_500_000,
        download_quota: crate::servers::ServerDownloadQuotaConfig {
            enabled: true,
            limit_bytes: 50_000_000,
            period: crate::servers::ServerDownloadQuotaPeriod::Weekly,
            reset_time_minutes_local: 7 * 60,
            weekly_reset_weekday: crate::bandwidth::QuotaWeekday::Wed,
            monthly_reset_day: 15,
        },
        tls_ca_cert: None,
    };

    db.insert_server(&server).unwrap();
    let servers = db.list_servers().unwrap();
    assert_eq!(servers.len(), 1);
    assert_eq!(servers[0].host, "news.example.com");
    assert_eq!(servers[0].port, 443);
    assert_eq!(servers[0].max_download_speed, 2_500_000);
    assert_eq!(servers[0].download_quota, server.download_quota);
    assert_eq!(
        servers[0].tls_name_mismatch_certificate_der,
        server.tls_name_mismatch_certificate_der
    );

    let mut updated = server.clone();
    updated.connections = 20;
    updated.max_download_speed = 5_000_000;
    updated.tls_name_mismatch_certificate_der = None;
    db.update_server(&updated).unwrap();
    let servers = db.list_servers().unwrap();
    assert_eq!(servers[0].connections, 20);
    assert_eq!(servers[0].max_download_speed, 5_000_000);
    assert_eq!(servers[0].tls_name_mismatch_certificate_der, None);

    assert!(db.delete_server(1).unwrap());
    assert!(!db.delete_server(1).unwrap());
    assert!(db.list_servers().unwrap().is_empty());
}

#[test]
fn next_server_id_empty() {
    let db = Database::open_in_memory().unwrap();
    assert_eq!(db.next_server_id().unwrap(), 1);
}

#[test]
fn config_roundtrip() {
    let db = Database::open_in_memory().unwrap();

    let config = Config {
        upload_cap: Some(500_000),
        seeding: Some(crate::settings::SeedingPolicy {
            seed_time_counting: Leaf::Set(crate::settings::SeedTimeCounting::Cumulative),
            ratio_goal: Leaf::Set(2.5),
            seed_time_goal_minutes: Leaf::Set(1_440),
            inactive_goal_minutes: Leaf::Off,
            mode: Leaf::Set(crate::settings::SeedingGoalMode::All),
            action: Leaf::Set(crate::settings::SeedingAction::Remove),
        }),
        torrent: crate::settings::TorrentConfig {
            enabled: true,
            base_port: 51_413,
            base_port_pinned: false,
            announce_port: Some(40_000),
            upnp: false,
            encryption: crate::settings::TorrentEncryption::Require,
            max_active_downloads: 7,
            max_peers_total: 120,
            session_dir: Some("/data/session".into()),
            unpack: false,
            connections_limit: crate::settings::TorrentLimit::Count(
                std::num::NonZeroU32::new(333).unwrap(),
            ),
            unchoke_slots_limit: crate::settings::TorrentLimit::Unlimited,
            connection_speed: std::num::NonZeroU32::new(12).unwrap(),
            allow_multiple_connections_per_ip: true,
            transports: crate::settings::PeerTransports {
                tcp: true,
                utp: false,
                prefer_downloading: crate::settings::PeerTransport::Tcp,
                prefer_seeding: crate::settings::PeerTransport::Tcp,
            },
            dht_bootstrap_nodes: "router.example:6881,192.0.2.1:25401".to_owned(),
            blocklist_enabled: true,
            blocklist: Some(r"C:\lists\level1.p2p".to_owned()),
            stop_tracker_timeout_secs: 0,
            banned_ips: vec!["10.0.0.0 - 10.0.0.255".to_owned(), "2001:db8::1".to_owned()],
            block_privileged_ports: true,
            filter_trackers: true,
            ssl_enabled: true,
            ssl_port: Some(4_433),
        },
        data_dir: "/tmp/weaver".to_string(),
        hardware_profile: None,
        intermediate_dir: Some("/tmp/intermediate".to_string()),
        complete_dir: Some("/tmp/complete".to_string()),
        servers: vec![ServerConfig {
            id: 1,
            host: "news.test.com".to_string(),
            port: 563,
            tls: true,
            username: None,
            password: None,
            connections: 5,
            active: true,
            supports_pipelining: true,
            pipelining_depth: None,
            tls_name_mismatch_certificate_der: None,
            priority: 0,
            backfill: false,
            retention_days: 0,
            max_download_speed: 4_000_000,
            download_quota: crate::servers::ServerDownloadQuotaConfig {
                enabled: true,
                limit_bytes: 80_000_000,
                period: crate::servers::ServerDownloadQuotaPeriod::Monthly,
                reset_time_minutes_local: 90,
                weekly_reset_weekday: crate::bandwidth::QuotaWeekday::Fri,
                monthly_reset_day: 31,
            },
            tls_ca_cert: None,
        }],
        categories: vec![CategoryConfig {
            upload_cap: Default::default(),
            seeding: Some(crate::settings::SeedingPolicy {
                ratio_goal: Leaf::Set(1.0),
                ..Default::default()
            }),
            torrent_route: true,
            unpack: None,
            id: 1,
            name: "Movies".to_string(),
            dest_dir: Some("/media/movies".to_string()),
            aliases: "movie*, film*".to_string(),
        }],
        retry: None,
        bandwidth: crate::proxies::Bandwidth {
            download_bps: Some(1_000_000),
            upload_bps: None,
            split: Some(crate::bandwidth::BandwidthSplit { usenet_pct: 70 }),
        },
        propagation_delay_secs: None,
        max_concurrent_nzbs: None,
        cleanup_after_extract: Some(false),
        delete_failed_job_files: None,
        history_retention_days: None,
        watch_folder: weaver_server_ingest::watch_folder::WatchFolderConfig {
            mode: weaver_server_ingest::watch_folder::WatchFolderMode::Polling,
            path: Some("/incoming/nzbs".to_string()),
            poll_interval_secs: 45,
            stability_secs: 2,
            category_from_subfolders: true,
            scanning_paused: false,
        },
        duplicate_policy: Default::default(),
        direct_store: Some(crate::settings::DirectStoreOverrides {
            holds_scratch_ceiling_bytes: Some(128 * 1024 * 1024),
            holds_resident_limit_bytes: Some(96 * 1024 * 1024),
            holds_scratch_total_bytes: Some(3 * 1024 * 1024 * 1024),
            holds_disk_reserve_bytes: Some(700 * 1024 * 1024),
        }),
        delivery_naming: Some(crate::settings::DeliveryNamingOverrides {
            deobfuscate_delivered_members: Some(false),
            enable_srrdb_lookup: Some(true),
        }),
        metrics: Default::default(),
        config_path: None,
    };

    db.save_config(&config).unwrap();
    let loaded = db.load_config().unwrap();

    assert_eq!(loaded.data_dir, "/tmp/weaver");
    assert_eq!(
        loaded.intermediate_dir,
        Some("/tmp/intermediate".to_string())
    );
    assert_eq!(loaded.complete_dir, Some("/tmp/complete".to_string()));
    assert_eq!(loaded.bandwidth, config.bandwidth);
    assert_eq!(loaded.servers[0].max_download_speed, 4_000_000);
    assert_eq!(
        loaded.servers[0].download_quota,
        config.servers[0].download_quota
    );
    assert_eq!(loaded.cleanup_after_extract, Some(false));
    assert_eq!(loaded.watch_folder.path.as_deref(), Some("/incoming/nzbs"));
    assert_eq!(
        loaded.watch_folder.mode,
        weaver_server_ingest::watch_folder::WatchFolderMode::Polling
    );
    assert_eq!(loaded.watch_folder.poll_interval_secs, 45);
    assert_eq!(loaded.watch_folder.stability_secs, 2);
    assert_eq!(loaded.servers.len(), 1);
    assert_eq!(loaded.servers[0].host, "news.test.com");
    assert!(loaded.servers[0].supports_pipelining);
    assert_eq!(loaded.categories.len(), 1);
    assert_eq!(loaded.categories[0].name, "Movies");
    assert_eq!(
        loaded.categories[0].dest_dir,
        Some("/media/movies".to_string())
    );
    assert_eq!(loaded.categories[0].aliases, "movie*, film*");
    assert_eq!(loaded.categories[0].seeding, config.categories[0].seeding);
    assert!(loaded.categories[0].torrent_route);
    assert_eq!(loaded.seeding, config.seeding);
    assert_eq!(loaded.torrent, config.torrent);
    // The hold limits are ordinary settings rows, so they survive a
    // save/load like everything else here.
    let direct_store = loaded
        .direct_store
        .expect("the direct-store table must survive the round trip");
    assert_eq!(
        direct_store.holds_scratch_ceiling_bytes,
        Some(128 * 1024 * 1024)
    );
    assert_eq!(
        direct_store.holds_resident_limit_bytes,
        Some(96 * 1024 * 1024)
    );
    assert_eq!(
        direct_store.holds_scratch_total_bytes,
        Some(3 * 1024 * 1024 * 1024)
    );
    assert_eq!(
        direct_store.holds_disk_reserve_bytes,
        Some(700 * 1024 * 1024)
    );
    let delivery_naming = loaded
        .delivery_naming
        .expect("the delivery-naming table must survive the round trip");
    assert_eq!(delivery_naming.deobfuscate_delivered_members, Some(false));
    assert_eq!(delivery_naming.enable_srrdb_lookup, Some(true));
}

#[test]
fn loading_the_config_removes_settings_no_release_reads() {
    let db = Database::open_in_memory().unwrap();
    db.set_setting("data_dir", "/tmp/weaver").unwrap();
    for key in crate::settings::service::RETIRED_SETTING_KEYS {
        db.set_setting(key, "1").unwrap();
    }

    db.load_config().unwrap();

    for key in crate::settings::service::RETIRED_SETTING_KEYS {
        assert_eq!(db.get_setting(key).unwrap(), None, "{key} survived a load");
    }
    assert_eq!(
        db.get_setting("data_dir").unwrap(),
        Some("/tmp/weaver".to_string())
    );
}

// An install that never touched delivery naming loads no table at all, so the
// accessors answer with the shipped defaults: rename on, srrdb off.
#[test]
fn an_unconfigured_delivery_naming_table_loads_as_absent_defaults() {
    let db = Database::open_in_memory().unwrap();
    db.set_setting("data_dir", "/tmp/weaver").unwrap();

    let loaded = db.load_config().unwrap();

    assert!(loaded.delivery_naming.is_none());
    assert!(loaded.deobfuscate_delivered_members());
    assert!(!loaded.enable_srrdb_lookup());
}

// An install that never set a hold limit loads no table at all, which is what
// "every default" looks like.
#[test]
fn an_unconfigured_direct_store_loads_as_absent() {
    let db = Database::open_in_memory().unwrap();
    db.set_setting("data_dir", "/tmp/weaver").unwrap();

    let loaded = db.load_config().unwrap();

    assert!(loaded.direct_store.is_none());
}

#[test]
fn category_crud() {
    let db = Database::open_in_memory().unwrap();

    let cat = CategoryConfig {
        upload_cap: Default::default(),
        seeding: None,
        torrent_route: false,
        unpack: None,
        id: 1,
        name: "TV".to_string(),
        dest_dir: None,
        aliases: "TV*, television".to_string(),
    };

    db.insert_category(&cat).unwrap();
    let cats = db.list_categories().unwrap();
    assert_eq!(cats.len(), 1);
    assert_eq!(cats[0].name, "TV");
    assert_eq!(cats[0].dest_dir, None);

    let mut updated = cat.clone();
    updated.dest_dir = Some("/media/tv".to_string());
    db.update_category(&updated).unwrap();
    let cats = db.list_categories().unwrap();
    assert_eq!(cats[0].dest_dir, Some("/media/tv".to_string()));

    assert!(db.delete_category(1).unwrap());
    assert!(!db.delete_category(1).unwrap());
    assert!(db.list_categories().unwrap().is_empty());
}

#[test]
fn next_category_id_empty() {
    let db = Database::open_in_memory().unwrap();
    assert_eq!(db.next_category_id().unwrap(), 1);
}

#[test]
fn a_database_without_torrent_settings_loads_the_defaults() {
    let db = Database::open_in_memory().unwrap();
    db.insert_category(&CategoryConfig {
        upload_cap: Default::default(),
        id: 1,
        name: "TV".to_string(),
        dest_dir: None,
        aliases: String::new(),
        seeding: None,
        torrent_route: false,
        unpack: None,
    })
    .unwrap();

    let loaded = db.load_config().unwrap();
    assert_eq!(loaded.seeding, None);
    // The defaults, with a listen port drawn in place of the fixed one.
    assert!(crate::torrents::sessions::LISTEN_PORTS.contains(&loaded.torrent.base_port));
    assert_eq!(
        loaded.torrent,
        crate::settings::TorrentConfig {
            base_port: loaded.torrent.base_port,
            ..crate::settings::TorrentConfig::default()
        }
    );
    assert_eq!(loaded.categories[0].seeding, None);
    assert!(!loaded.categories[0].torrent_route);
}

#[test]
fn clearing_the_global_seeding_policy_removes_it() {
    let db = Database::open_in_memory().unwrap();
    let policy = crate::settings::SeedingPolicy {
        ratio_goal: Leaf::Set(1.0),
        inactive_goal_minutes: Leaf::Set(60),
        ..Default::default()
    };
    db.save_seeding_policy(Some(&policy)).unwrap();
    // At the global level nothing is inherited: a goal left unset is off,
    // and the mode and action are their defaults.
    assert_eq!(
        db.load_config().unwrap().seeding,
        Some(crate::settings::SeedingPolicy {
            seed_time_goal_minutes: Leaf::Off,
            mode: Leaf::Set(crate::settings::SeedingGoalMode::Any),
            action: Leaf::Set(crate::settings::SeedingAction::Stop),
            seed_time_counting: Leaf::Set(crate::settings::SeedTimeCounting::Cumulative),
            ..policy
        })
    );

    // Dropping one goal removes only that goal.
    let narrower = crate::settings::SeedingPolicy {
        inactive_goal_minutes: Leaf::Off,
        ..policy
    };
    db.save_seeding_policy(Some(&narrower)).unwrap();
    assert_eq!(
        db.load_config().unwrap().seeding,
        Some(narrower.at_global())
    );

    db.save_seeding_policy(None).unwrap();
    assert_eq!(db.load_config().unwrap().seeding, None);
}

#[test]
fn tracker_rules_crud() {
    use crate::settings::model::TrackerRule;
    use crate::settings::{
        PeerTransport, SeedingAction, SeedingPolicy, TorrentEncryption, TransportRule,
    };

    let db = Database::open_in_memory().unwrap();
    assert!(
        SettingsRepositoryQueries::list_tracker_rules(&db)
            .unwrap()
            .is_empty()
    );
    let seeding = SeedingPolicy {
        ratio_goal: Leaf::Set(1.5),
        seed_time_goal_minutes: Leaf::Set(4_320),
        inactive_goal_minutes: Leaf::Off,
        action: Leaf::Set(SeedingAction::Stop),
        ..SeedingPolicy::default()
    };
    let first = db
        .insert_tracker_rule(
            &TrackerRule {
                transports: TransportRule {
                    tcp: None,
                    utp: Some(true),
                    prefer_downloading: Some(PeerTransport::Utp),
                    prefer_seeding: None,
                },
                archive_passwords: Default::default(),
                bound: None,
                id: 0,
                name: "*.example.org".into(),
                host_patterns: vec!["*.example.org".into()],
                seeding: Some(seeding),
                never_pause: true,
                unpack: Some(false),
                client: Some(Client::QBittorrent),
                torrent_route: true,
                dht: Some(false),
                pex: None,
                lsd: Some(true),
                encryption: Some(TorrentEncryption::Prefer),
                upload_cap: Leaf::Off,
                forced: true,
                ssl: None,
            },
            1,
        )
        .unwrap();
    let second = db
        .insert_tracker_rule(
            &TrackerRule {
                transports: Default::default(),
                archive_passwords: Default::default(),
                bound: None,
                id: 0,
                name: "tracker.invalid".into(),
                host_patterns: vec!["tracker.invalid".into()],
                seeding: None,
                never_pause: false,
                unpack: None,
                client: None,
                torrent_route: false,
                dht: None,
                pex: None,
                lsd: None,
                encryption: None,
                upload_cap: Leaf::Inherit,
                forced: false,
                ssl: None,
            },
            2,
        )
        .unwrap();
    assert_eq!((first.id, second.id), (1, 2));
    assert_eq!(
        db.list_tracker_rules().unwrap(),
        [first.clone(), second.clone()]
    );

    // A pattern names one rule.
    assert!(
        db.insert_tracker_rule(
            &TrackerRule {
                id: 0,
                ..first.clone()
            },
            3
        )
        .is_err()
    );

    let changed = TrackerRule {
        seeding: None,
        never_pause: false,
        unpack: Some(true),
        client: Some(Client::Weaver),
        torrent_route: false,
        dht: None,
        pex: Some(false),
        lsd: Some(false),
        encryption: Some(TorrentEncryption::Disabled),
        transports: TransportRule {
            tcp: Some(false),
            utp: None,
            prefer_downloading: None,
            prefer_seeding: Some(PeerTransport::Tcp),
        },
        upload_cap: Leaf::Set(4_096),
        forced: false,
        ..first.clone()
    };
    assert!(db.update_tracker_rule(&changed, 4).unwrap());
    assert!(db.delete_tracker_rule(second.id).unwrap());
    assert!(!db.delete_tracker_rule(second.id).unwrap());
    assert_eq!(db.list_tracker_rules().unwrap(), [changed]);
}

// A rule kept naming `transmission` reads as one naming Weaver.
#[test]
fn a_kept_transmission_client_reads_as_weaver() {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
    use crate::settings::model::TrackerRule;

    let db = Database::open_in_memory().unwrap();
    let kept = db
        .insert_tracker_rule(
            &TrackerRule {
                transports: Default::default(),
                archive_passwords: Default::default(),
                bound: None,
                id: 0,
                name: "tracker.example".into(),
                host_patterns: vec!["tracker.example".into()],
                seeding: None,
                never_pause: false,
                unpack: None,
                client: Some(Client::QBittorrent),
                torrent_route: false,
                dht: None,
                pex: None,
                lsd: None,
                encryption: None,
                upload_cap: Leaf::Inherit,
                forced: false,
                ssl: None,
            },
            1,
        )
        .unwrap();
    let datastore = db.datastore();
    let args = vec![SqlArg::I64(i64::from(kept.id))];
    db.run_sql_blocking(async move {
        SqlRuntime::execute(
            datastore.write_exec(),
            "UPDATE tracker_rules SET client = 'transmission' WHERE id = {}",
            &args,
        )
        .await?;
        Ok(())
    })
    .unwrap();
    assert_eq!(
        db.list_tracker_rules().unwrap(),
        [TrackerRule {
            client: Some(Client::Weaver),
            ..kept
        }]
    );
}

// A rule's certificate comes back as it was kept, its private key sealed in
// the database; clearing it clears both.
#[test]
fn a_tracker_rules_private_key_is_encrypted_at_rest() {
    use crate::persistence::encryption::is_encrypted;
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
    use crate::settings::model::TrackerRule;

    let db = Database::open_in_memory().unwrap();
    let key = rcgen::KeyPair::generate().unwrap();
    let params = rcgen::CertificateParams::new(vec!["user-1".to_owned()]).unwrap();
    let issued = crate::settings::model::TrackerCertificate {
        certificate_pem: params.self_signed(&key).unwrap().pem(),
        private_key_pem: key.serialize_pem(),
    };
    let kept = db
        .insert_tracker_rule(
            &TrackerRule {
                transports: Default::default(),
                archive_passwords: Default::default(),
                bound: None,
                id: 0,
                name: "tracker.example".into(),
                host_patterns: vec!["tracker.example".into()],
                seeding: None,
                never_pause: false,
                unpack: None,
                client: None,
                torrent_route: false,
                dht: None,
                pex: None,
                lsd: None,
                encryption: None,
                upload_cap: Leaf::Inherit,
                forced: false,
                ssl: Some(issued.clone()),
            },
            1,
        )
        .unwrap();
    assert_eq!(
        db.list_tracker_rules().unwrap(),
        std::slice::from_ref(&kept)
    );
    assert_eq!(kept.ssl.as_ref(), Some(&issued));

    let stored = |column: &'static str| {
        let datastore = db.datastore();
        let sql = match column {
            "cert" => "SELECT ssl_certificate AS value FROM tracker_rules WHERE id = {}",
            _ => "SELECT ssl_private_key AS value FROM tracker_rules WHERE id = {}",
        };
        let args = vec![SqlArg::I64(i64::from(kept.id))];
        db.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(datastore.read_exec(), sql, &args)
                .await?
                .expect("the rule's row");
            row.opt_text("value")
        })
        .unwrap()
    };
    let key = stored("key").unwrap();
    assert!(is_encrypted(&key));
    assert!(!key.contains("PRIVATE KEY"));
    assert_eq!(
        stored("cert").as_deref(),
        Some(issued.certificate_pem.as_str())
    );

    let cleared = TrackerRule {
        ssl: None,
        ..kept.clone()
    };
    assert!(db.update_tracker_rule(&cleared, 2).unwrap());
    assert_eq!(db.list_tracker_rules().unwrap(), [cleared]);
    assert_eq!((stored("cert"), stored("key")), (None, None));
}

// A tracker file read from `text`, with an identity for `subject` when one
// is given.
fn tracker_file(text: &str, subject: Option<&str>) -> crate::settings::tracker_file::TrackerFile {
    let ssl = subject.map(|subject| {
        let key = rcgen::KeyPair::generate().unwrap();
        let params = rcgen::CertificateParams::new(vec![subject.to_owned()]).unwrap();
        format!(
            "[ssl]\ncertificate_pem = '''\n{}'''\nprivate_key_pem = '''\n{}'''\n",
            params.self_signed(&key).unwrap().pem(),
            key.serialize_pem()
        )
    });
    let text = format!(
        "version = 1\nname = \"Example Tracker\"\nhost_patterns = [\"*.tracker.example\"]\n{text}\n{}",
        ssl.unwrap_or_default()
    );
    crate::settings::tracker_file::read(text.as_bytes(), None).unwrap()
}

// A rule a tracker file filled refuses, on the ordinary save, a change to a
// field the file locked, naming the field and the tracker; a change to any
// other field saves, and the rule stays bound.
#[test]
fn a_bound_rule_refuses_a_change_to_a_locked_field() {
    use crate::settings::TorrentEncryption;

    let db = Database::open_in_memory().unwrap();
    let file = tracker_file(
        "revision = \"1\"\n[required]\ndht = false\nencryption = \"require\"\n[recommended]\npex = false",
        Some("user-1"),
    );
    let rule = db.apply_tracker_file(&file, 10).unwrap();
    let bound = rule.bound.clone().unwrap();
    assert_eq!(bound.name, "Example Tracker");
    assert_eq!(bound.revision, "1");
    assert_eq!(bound.imported_at_ms, 10);
    assert_eq!(bound.locked, ["dht", "encryption", "ssl"]);
    assert_eq!(
        (rule.dht, rule.pex, rule.encryption),
        (Some(false), Some(false), Some(TorrentEncryption::Require))
    );
    assert_eq!(rule.ssl, file.ssl);

    // The ordinary input names no binding; the save keeps the rule's.
    let unbound_input =
        |rule: &crate::settings::model::TrackerRule| crate::settings::model::TrackerRule {
            bound: None,
            ..rule.clone()
        };
    for (changed, keeps_ssl, field) in [
        (
            crate::settings::model::TrackerRule {
                dht: Some(true),
                ..unbound_input(&rule)
            },
            true,
            "dht",
        ),
        (
            crate::settings::model::TrackerRule {
                encryption: None,
                ..unbound_input(&rule)
            },
            true,
            "encryption",
        ),
        (
            crate::settings::model::TrackerRule {
                ssl: None,
                ..unbound_input(&rule)
            },
            false,
            "ssl",
        ),
    ] {
        let refusal = db
            .save_tracker_rule(Some(rule.id), changed, keeps_ssl, true, 11)
            .unwrap()
            .unwrap_err();
        assert_eq!(
            refusal.to_string(),
            format!(
                "{field} is locked by Example Tracker's tracker file; unbind the rule to change it."
            )
        );
    }
    assert_eq!(
        db.list_tracker_rules().unwrap(),
        std::slice::from_ref(&rule)
    );

    let changed = crate::settings::model::TrackerRule {
        pex: Some(true),
        never_pause: true,
        ssl: None,
        ..unbound_input(&rule)
    };
    let saved = db
        .save_tracker_rule(Some(rule.id), changed, true, true, 12)
        .unwrap()
        .unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!((saved[0].pex, saved[0].never_pause), (Some(true), true));
    assert_eq!(saved[0].ssl, rule.ssl);
    assert_eq!(saved[0].bound, rule.bound);
}

// A tracker file fills a rule's encryption and transports with any value
// they take, as the API names them: a required one is locked against the
// ordinary save by its field's name, and the preview shows each.
#[test]
fn a_tracker_file_fills_and_locks_a_rules_encryption_and_transports() {
    use crate::settings::model::TrackerRule;
    use crate::settings::{PeerTransport, TorrentEncryption, TransportRule};

    let db = Database::open_in_memory().unwrap();
    let file = tracker_file(
        "revision = \"1\"\n[required]\nencryption = \"disabled\"\npeer_utp = false\nprefer_seeding = \"tcp\"\n\
         [recommended]\npeer_tcp = true\nprefer_downloading = \"tcp\"",
        None,
    );
    let preview = file.preview(None, String::new());
    let shown: Vec<(&str, bool, Option<&str>)> = preview
        .changes
        .iter()
        .map(|change| (change.field, change.locked, change.new.as_deref()))
        .collect();
    assert_eq!(
        shown,
        [
            ("peer_tcp", false, Some("true")),
            ("peer_utp", true, Some("false")),
            ("encryption", true, Some("disabled")),
            ("prefer_downloading", false, Some("tcp")),
            ("prefer_seeding", true, Some("tcp")),
        ]
    );
    let rule = db.apply_tracker_file(&file, 10).unwrap();
    assert_eq!(
        rule.bound.as_ref().unwrap().locked,
        ["encryption", "peer_utp", "prefer_seeding"]
    );
    assert_eq!(rule.encryption, Some(TorrentEncryption::Disabled));
    assert_eq!(
        rule.transports,
        TransportRule {
            tcp: Some(true),
            utp: Some(false),
            prefer_downloading: Some(PeerTransport::Tcp),
            prefer_seeding: Some(PeerTransport::Tcp),
        }
    );
    assert_eq!(
        db.list_tracker_rules().unwrap(),
        std::slice::from_ref(&rule)
    );

    let input = |transports| TrackerRule {
        bound: None,
        transports,
        ..rule.clone()
    };
    for (transports, field) in [
        (
            TransportRule {
                utp: None,
                ..rule.transports
            },
            "peer_utp",
        ),
        (
            TransportRule {
                prefer_seeding: Some(PeerTransport::Utp),
                ..rule.transports
            },
            "prefer_seeding",
        ),
    ] {
        let refusal = db
            .save_tracker_rule(Some(rule.id), input(transports), true, true, 11)
            .unwrap()
            .unwrap_err();
        assert!(
            refusal
                .to_string()
                .starts_with(&format!("{field} is locked")),
            "{refusal}"
        );
    }
    // A recommended value is the user's to change.
    let saved = db
        .save_tracker_rule(
            Some(rule.id),
            input(TransportRule {
                tcp: None,
                prefer_downloading: None,
                ..rule.transports
            }),
            true,
            true,
            12,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            saved[0].transports.tcp,
            saved[0].transports.prefer_downloading
        ),
        (None, None)
    );
}

// A bound rule's file hosts are what its file finds it by, so the ordinary
// save refuses to drop them while the binding stands; once unbound, they
// go. A host added beside them, and a new name, are the user's.
#[test]
fn a_bound_rule_refuses_dropping_its_files_hosts() {
    let db = Database::open_in_memory().unwrap();
    let file = tracker_file("revision = \"1\"\n[recommended]\npex = false", None);
    let rule = db.apply_tracker_file(&file, 10).unwrap();
    let added = crate::settings::model::TrackerRule {
        name: "ET".to_owned(),
        host_patterns: vec![
            "*.tracker.example".to_owned(),
            "*.elsewhere.example".to_owned(),
        ],
        ..rule.clone()
    };
    let saved = db
        .save_tracker_rule(Some(rule.id), added, true, true, 11)
        .unwrap()
        .unwrap();
    assert_eq!(saved[0].name, "ET");
    assert_eq!(
        saved[0].host_patterns,
        ["*.tracker.example", "*.elsewhere.example"]
    );
    assert!(saved[0].bound.is_some());
    let rule = saved[0].clone();
    let moved = crate::settings::model::TrackerRule {
        host_patterns: vec!["*.elsewhere.example".to_owned()],
        bound: None,
        ..rule.clone()
    };
    let refusal = db
        .save_tracker_rule(Some(rule.id), moved.clone(), true, true, 11)
        .unwrap()
        .unwrap_err();
    assert_eq!(
        refusal.to_string(),
        "*.tracker.example is locked by Example Tracker's tracker file; unbind the rule to remove it."
    );
    assert_eq!(
        db.list_tracker_rules().unwrap(),
        std::slice::from_ref(&rule)
    );

    assert!(db.unbind_tracker_rule(rule.id, 12).unwrap());
    let saved = db
        .save_tracker_rule(Some(rule.id), moved, true, true, 13)
        .unwrap()
        .unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].host_patterns, ["*.elsewhere.example"]);
    assert_eq!(saved[0].bound, None);
}

// One rule holds a name, whatever its case, and a pattern. A rule keeps its
// patterns in the order saved, and deleting it frees them.
#[test]
fn a_rule_holds_its_name_and_patterns_alone() {
    use crate::settings::model::TrackerRule;

    let db = Database::open_in_memory().unwrap();
    let made = |name: &str, patterns: &[&str]| TrackerRule {
        transports: Default::default(),
        archive_passwords: Default::default(),
        bound: None,
        id: 0,
        name: name.to_owned(),
        host_patterns: patterns.iter().map(|&p| p.to_owned()).collect(),
        seeding: None,
        never_pause: false,
        unpack: None,
        client: None,
        torrent_route: false,
        dht: None,
        pex: None,
        lsd: None,
        encryption: None,
        upload_cap: Leaf::Inherit,
        forced: false,
        ssl: None,
    };
    let ipt = db
        .save_tracker_rule(
            None,
            made(
                "IPT",
                &[
                    "*.iptorrents.com",
                    "*.stackoverflow.tech",
                    "ssl.empirehost.me",
                ],
            ),
            true,
            true,
            1,
        )
        .unwrap()
        .unwrap()
        .remove(0);
    assert_eq!(
        ipt.host_patterns,
        [
            "*.iptorrents.com",
            "*.stackoverflow.tech",
            "ssl.empirehost.me"
        ]
    );

    let refusal = db
        .save_tracker_rule(None, made("ipt", &["*.other.example"]), true, true, 2)
        .unwrap()
        .unwrap_err();
    assert_eq!(refusal.to_string(), "a rule named IPT exists");
    let refusal = db
        .save_tracker_rule(
            None,
            made("Other", &["*.other.example", "ssl.empirehost.me"]),
            true,
            true,
            2,
        )
        .unwrap()
        .unwrap_err();
    assert_eq!(refusal.to_string(), "ssl.empirehost.me is covered by IPT");
    assert_eq!(db.list_tracker_rules().unwrap(), std::slice::from_ref(&ipt));

    // The rule's own name and patterns are no collision with itself.
    let renamed = TrackerRule {
        name: "iPT".to_owned(),
        host_patterns: vec![
            "ssl.empirehost.me".to_owned(),
            "*.iptorrents.com".to_owned(),
        ],
        ..ipt.clone()
    };
    let saved = db
        .save_tracker_rule(Some(ipt.id), renamed, true, true, 3)
        .unwrap()
        .unwrap();
    assert_eq!(saved[0].name, "iPT");
    assert_eq!(
        saved[0].host_patterns,
        ["ssl.empirehost.me", "*.iptorrents.com"]
    );

    assert!(db.delete_tracker_rule(ipt.id).unwrap());
    let other = db
        .save_tracker_rule(
            None,
            made("IPT", &["*.stackoverflow.tech", "ssl.empirehost.me"]),
            true,
            true,
            4,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        other[0].host_patterns,
        ["*.stackoverflow.tech", "ssl.empirehost.me"]
    );
}

// A file whose pattern a rule has fills that rule again: its required
// values replace the rule's, a recommendation the user left takes the new
// one and one the user changed stays. A file for another pattern makes a
// rule of its own.
#[test]
fn reimporting_a_tracker_file_replaces_required_and_keeps_edited_recommendations() {
    let db = Database::open_in_memory().unwrap();
    let first = tracker_file(
        "revision = \"1\"
[required]
dht = false
[recommended]
pex = false
lsd = false
seeding = { ratio_goal = 1.0, seed_time_goal_minutes = 100 }",
        None,
    );
    let rule = db.apply_tracker_file(&first, 1).unwrap();
    let seeding = rule.seeding.unwrap();
    assert_eq!(
        (seeding.ratio_goal, seeding.seed_time_goal_minutes),
        (Leaf::Set(1.0), Leaf::Set(100))
    );

    let edited = crate::settings::model::TrackerRule {
        lsd: Some(true),
        seeding: Some(crate::settings::SeedingPolicy {
            ratio_goal: Leaf::Set(2.0),
            ..seeding
        }),
        bound: None,
        ..rule.clone()
    };
    db.save_tracker_rule(Some(rule.id), edited, true, true, 2)
        .unwrap()
        .unwrap();

    let second = tracker_file(
        "revision = \"2\"
[required]
dht = true
[recommended]
pex = true
lsd = false
seeding = { ratio_goal = 3.0, seed_time_goal_minutes = 200 }",
        None,
    );
    let again = db.apply_tracker_file(&second, 3).unwrap();
    assert_eq!(again.id, rule.id);
    assert_eq!(again.dht, Some(true), "a required value is replaced");
    assert_eq!(again.pex, Some(true), "a recommendation left as it came");
    assert_eq!(again.lsd, Some(true), "a recommendation the user changed");
    let seeding = again.seeding.unwrap();
    assert_eq!(seeding.ratio_goal, Leaf::Set(2.0));
    assert_eq!(seeding.seed_time_goal_minutes, Leaf::Set(200));
    assert_eq!(again.bound.as_ref().unwrap().revision, "2");
    assert_eq!(
        db.list_tracker_rules().unwrap(),
        std::slice::from_ref(&again)
    );

    let other = tracker_file("revision = \"1\"", None);
    let other = crate::settings::tracker_file::TrackerFile {
        name: "other.example".into(),
        host_patterns: vec!["other.example".into()],
        ..other
    };
    let made = db.apply_tracker_file(&other, 4).unwrap();
    assert_ne!(made.id, again.id);
    assert_eq!(db.list_tracker_rules().unwrap().len(), 2);
}

// A seeding goal a file recommended stays the file's to replace across a
// revision that recommends something else, or no seeding at all: the next
// recommendation replaces it, and a goal set by hand is never replaced.
#[test]
fn a_recommended_goal_is_replaced_across_a_revision_that_skips_it() {
    let db = Database::open_in_memory().unwrap();
    let revision = |text: &str| tracker_file(text, None);
    let rule = db
        .apply_tracker_file(
            &revision(
                "revision = \"1\"
[recommended]
seeding = { ratio_goal = 2.0, inactive_goal_minutes = 30 }",
            ),
            1,
        )
        .unwrap();
    let edited = crate::settings::model::TrackerRule {
        seeding: Some(crate::settings::SeedingPolicy {
            inactive_goal_minutes: Leaf::Set(45),
            ..rule.seeding.unwrap()
        }),
        bound: None,
        ..rule.clone()
    };
    db.save_tracker_rule(Some(rule.id), edited, true, true, 2)
        .unwrap()
        .unwrap();
    db.apply_tracker_file(
        &revision(
            "revision = \"2\"
[recommended]
seeding = { seed_time_goal_minutes = 100 }",
        ),
        3,
    )
    .unwrap();
    db.apply_tracker_file(&revision("revision = \"3\""), 4)
        .unwrap();
    let last = db
        .apply_tracker_file(
            &revision(
                "revision = \"4\"
[recommended]
seeding = { ratio_goal = 3.0, seed_time_goal_minutes = 200, inactive_goal_minutes = 60 }",
            ),
            5,
        )
        .unwrap();
    let seeding = last.seeding.unwrap();
    assert_eq!(seeding.ratio_goal, Leaf::Set(3.0), "skipped twice");
    assert_eq!(
        seeding.seed_time_goal_minutes,
        Leaf::Set(200),
        "skipped once"
    );
    assert_eq!(seeding.inactive_goal_minutes, Leaf::Set(45), "set by hand");
}

// A file for a rule the user made by hand fills each goal the user left to
// inherit and keeps each one the user set.
#[test]
fn a_tracker_file_fills_only_the_goals_a_hand_made_rule_leaves() {
    let db = Database::open_in_memory().unwrap();
    let made = crate::settings::model::TrackerRule {
        name: "*.tracker.example".into(),
        host_patterns: vec!["*.tracker.example".into()],
        seeding: Some(crate::settings::SeedingPolicy {
            ratio_goal: Leaf::Set(5.0),
            ..crate::settings::SeedingPolicy::default()
        }),
        ..crate::settings::model::TrackerRule::default()
    };
    let rule = db
        .save_tracker_rule(None, made, true, true, 1)
        .unwrap()
        .unwrap()
        .remove(0);
    let file = tracker_file(
        "revision = \"1\"
[recommended]
seeding = { ratio_goal = 2.0, seed_time_goal_minutes = 100 }",
        None,
    );
    assert_eq!(file.host_patterns, rule.host_patterns);
    let applied = db.apply_tracker_file(&file, 2).unwrap();
    assert_eq!(applied.id, rule.id);
    let seeding = applied.seeding.unwrap();
    assert_eq!(seeding.ratio_goal, Leaf::Set(5.0));
    assert_eq!(seeding.seed_time_goal_minutes, Leaf::Set(100));
}

// Unbinding keeps every value and the identity and lets the user change
// them; deleting a rule takes its binding with it.
#[test]
fn unbinding_keeps_the_values_and_deleting_takes_the_binding() {
    let db = Database::open_in_memory().unwrap();
    let file = tracker_file(
        "revision = \"1\"\n[required]\ndht = false\npex = false",
        Some("user-1"),
    );
    let rule = db.apply_tracker_file(&file, 1).unwrap();
    assert!(db.unbind_tracker_rule(rule.id, 2).unwrap());
    assert!(!db.unbind_tracker_rule(rule.id + 1, 2).unwrap());
    let unbound = db.list_tracker_rules().unwrap().remove(0);
    assert_eq!(unbound.bound, None);
    assert_eq!((unbound.dht, unbound.pex), (Some(false), Some(false)));
    assert_eq!(unbound.ssl, file.ssl);

    let changed = crate::settings::model::TrackerRule {
        dht: Some(true),
        ssl: None,
        ..unbound.clone()
    };
    let saved = db
        .save_tracker_rule(Some(rule.id), changed, false, true, 3)
        .unwrap()
        .unwrap();
    assert_eq!((saved[0].dht, saved[0].ssl.as_ref()), (Some(true), None));

    let bound = db.apply_tracker_file(&file, 4).unwrap();
    assert!(bound.bound.is_some());
    assert!(db.delete_tracker_rule(bound.id).unwrap());
    assert!(db.list_tracker_rules().unwrap().is_empty());
    let fresh = db.apply_tracker_file(&file, 5).unwrap();
    assert_eq!(fresh.bound.unwrap().imported_at_ms, 5);
}

// A rule's archive passwords are sealed at rest. A reimport with a list
// replaces them, an empty list clears them, and a file naming none keeps
// them; the editor's save keeps them unless it clears them, an unbind keeps
// them, and none of it writes to the global list.
#[test]
fn a_rules_archive_passwords_are_sealed_and_follow_its_tracker_file() {
    let db = Database::open_in_memory().unwrap();
    db.save_archive_password_settings(Some(vec!["global".to_owned()]), None)
        .unwrap();
    let global = || {
        db.global_archive_password_candidates()
            .unwrap()
            .iter()
            .map(|candidate| candidate.value().to_owned())
            .collect::<Vec<_>>()
    };
    let passwords = |id: u32| {
        db.list_tracker_rules()
            .unwrap()
            .into_iter()
            .find(|rule| rule.id == id)
            .unwrap()
            .archive_passwords
            .as_slice()
            .to_vec()
    };
    let listed = |values: &[&str]| {
        values
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>()
    };

    let file = tracker_file(
        "revision = \"1\"\n[archive_passwords]\nvalues = [\"first secret\", \"second\"]",
        None,
    );
    let rule = db.apply_tracker_file(&file, 1).unwrap();
    assert_eq!(passwords(rule.id), listed(&["first secret", "second"]));
    let datastore = db.datastore();
    let args = vec![SqlArg::I64(i64::from(rule.id))];
    let sealed = db
        .run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT archive_passwords AS value FROM tracker_rules WHERE id = {}",
                &args,
            )
            .await?
            .expect("the rule's row");
            row.opt_text("value")
        })
        .unwrap()
        .unwrap();
    assert!(is_encrypted(&sealed));
    assert!(!sealed.contains("first secret"));

    let replaced = tracker_file(
        "revision = \"2\"\n[archive_passwords]\nvalues = [\"third\"]",
        None,
    );
    db.apply_tracker_file(&replaced, 2).unwrap();
    assert_eq!(passwords(rule.id), listed(&["third"]));
    db.apply_tracker_file(&tracker_file("revision = \"3\"", None), 3)
        .unwrap();
    assert_eq!(passwords(rule.id), listed(&["third"]));

    let edited = db.list_tracker_rules().unwrap().remove(0);
    db.save_tracker_rule(
        Some(rule.id),
        TrackerRule {
            archive_passwords: Default::default(),
            ..edited.clone()
        },
        true,
        true,
        4,
    )
    .unwrap()
    .unwrap();
    assert_eq!(passwords(rule.id), listed(&["third"]));
    assert!(db.unbind_tracker_rule(rule.id, 5).unwrap());
    assert_eq!(passwords(rule.id), listed(&["third"]));
    db.save_tracker_rule(
        Some(rule.id),
        TrackerRule {
            archive_passwords: Default::default(),
            ..edited
        },
        true,
        false,
        6,
    )
    .unwrap()
    .unwrap();
    assert!(passwords(rule.id).is_empty());

    db.apply_tracker_file(&replaced, 7).unwrap();
    assert_eq!(passwords(rule.id), listed(&["third"]));
    let cleared = tracker_file("revision = \"4\"\n[archive_passwords]\nvalues = []", None);
    db.apply_tracker_file(&cleared, 8).unwrap();
    assert!(passwords(rule.id).is_empty());
    assert_eq!(global(), listed(&["global"]));
}

fn store_route_rows(db: &Database, consumers: &[&str]) {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
    let store = db.datastore();
    let keys: Vec<String> = consumers.iter().map(|key| (*key).to_owned()).collect();
    let policy = serde_json::to_string(&crate::proxies::RoutingPolicy::default()).unwrap();
    db.run_sql_blocking(async move {
        SqlRuntime::run_in_transaction(&store, "route_rows", |tx| {
            let (keys, policy) = (keys.clone(), policy.clone());
            Box::pin(async move {
                for key in keys {
                    tx.execute(
                        "INSERT INTO proxy_routes (consumer, policy) VALUES ({}, {})",
                        &[SqlArg::Text(key), SqlArg::Text(policy.clone())],
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await
    })
    .unwrap();
}
fn route_rows(db: &Database) -> Vec<String> {
    let mut keys: Vec<_> = db
        .list_proxy_routes()
        .unwrap()
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    keys.sort();
    keys
}

// Removing a category or a tracker rule removes its torrent route with it,
// and only its own; so does a replacement of the categories that drops one.
#[test]
fn a_removed_route_owner_takes_its_route_row() {
    let db = Database::open_in_memory().unwrap();
    let category = |id: u32| CategoryConfig {
        upload_cap: Default::default(),
        seeding: None,
        torrent_route: true,
        unpack: None,
        id,
        name: format!("category {id}"),
        dest_dir: None,
        aliases: String::new(),
    };
    for id in 1..=3 {
        db.insert_category(&category(id)).unwrap();
    }
    let rule = db
        .insert_tracker_rule(
            &TrackerRule {
                transports: Default::default(),
                archive_passwords: Default::default(),
                bound: None,
                id: 0,
                name: "tracker.invalid".into(),
                host_patterns: vec!["tracker.invalid".into()],
                seeding: None,
                never_pause: false,
                unpack: None,
                client: None,
                torrent_route: true,
                dht: None,
                pex: None,
                lsd: None,
                encryption: None,
                upload_cap: Leaf::Inherit,
                forced: false,
                ssl: None,
            },
            1,
        )
        .unwrap();
    store_route_rows(
        &db,
        &[
            "torrent:category:1",
            "torrent:category:2",
            "torrent:category:3",
            "torrent:tracker:1",
            "torrent:default",
        ],
    );

    assert!(db.delete_category(1).unwrap());
    assert_eq!(
        route_rows(&db),
        [
            "torrent:category:2",
            "torrent:category:3",
            "torrent:default",
            "torrent:tracker:1"
        ]
    );
    assert!(db.delete_tracker_rule(rule.id).unwrap());
    assert_eq!(
        route_rows(&db),
        [
            "torrent:category:2",
            "torrent:category:3",
            "torrent:default"
        ]
    );
    db.replace_categories(&[category(3)]).unwrap();
    assert_eq!(route_rows(&db), ["torrent:category:3", "torrent:default"]);
}
