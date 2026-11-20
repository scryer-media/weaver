// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::servers::persistence::ServersPersistenceQueries;
use crate::servers::service_queries::ServersServiceInternalQueries;
use std::path::PathBuf;

use super::*;
use crate::servers::{ServerConfig, ServerDownloadQuotaConfig};

fn server(id: u32) -> ServerConfig {
    ServerConfig {
        id,
        host: format!("news-{id}.example.com"),
        port: 563,
        tls: true,
        username: None,
        password: None,
        connections: 4,
        active: true,
        supports_pipelining: false,
        pipelining_depth: None,
        tls_name_mismatch_certificate_der: None,
        priority: 0,
        backfill: false,
        retention_days: 0,
        max_download_speed: 0,
        download_quota: ServerDownloadQuotaConfig::default(),
        tls_ca_cert: None::<PathBuf>,
    }
}

#[test]
fn usage_roundtrip_reset_and_delete_cascade() {
    let db = Database::open_in_memory().unwrap();
    db.insert_server(&server(7)).unwrap();
    let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
    let end = DateTime::from_timestamp(1_700_086_400, 0).unwrap();
    let updated_at = DateTime::from_timestamp(1_700_000_123, 0).unwrap();
    let usage = ServerDownloadUsage {
        server_id: 7,
        lifetime_bytes: 5_000,
        quota_baseline_bytes: 1_000,
        window_start: Some(start),
        window_end: Some(end),
        updated_at,
    };

    db.upsert_server_download_usage(&usage).unwrap();
    assert_eq!(db.server_download_usage(7).unwrap(), Some(usage.clone()));
    let mut updated_server = server(7);
    updated_server.connections = 8;
    db.replace_servers(&[updated_server]).unwrap();
    assert_eq!(db.server_download_usage(7).unwrap(), Some(usage.clone()));
    assert_eq!(db.list_server_download_usage().unwrap(), vec![usage]);

    let reset = db.reset_server_download_usage(7).unwrap();
    assert_eq!(reset.lifetime_bytes, 5_000);
    assert_eq!(reset.quota_baseline_bytes, 5_000);
    assert_eq!(reset.used_bytes(), 0);
    assert_eq!(reset.window_start, None);
    assert_eq!(reset.window_end, None);

    assert!(db.delete_server(7).unwrap());
    assert_eq!(db.server_download_usage(7).unwrap(), None);
}
