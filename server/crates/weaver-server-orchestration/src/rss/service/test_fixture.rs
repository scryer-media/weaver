// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The RSS service the tests build, and the NZB they serve.

use std::path::Path;
use std::sync::Arc;

use tokio::sync::RwLock;
use weaver_server_base::security::RuntimeSecurityConfig;

use super::RssService;
use crate::settings::Config;
use crate::{Database, PipelineMetrics, SchedulerHandle, SharedPipelineState};

pub fn build_service(data_dir: &Path, db: Database) -> RssService {
    build_service_with_security(data_dir, db, RuntimeSecurityConfig::default())
}

pub fn build_service_with_security(
    data_dir: &Path,
    db: Database,
    security: RuntimeSecurityConfig,
) -> RssService {
    let config = Arc::new(RwLock::new(Config {
        upload_cap: None,
        seeding: None,
        torrent: Default::default(),
        data_dir: data_dir.display().to_string(),
        hardware_profile: None,
        intermediate_dir: None,
        complete_dir: None,
        servers: vec![],
        categories: vec![],
        retry: None,
        bandwidth: Default::default(),
        propagation_delay_secs: None,
        max_concurrent_nzbs: None,
        cleanup_after_extract: Some(true),
        delete_failed_job_files: None,
        history_retention_days: None,
        watch_folder: crate::watch_folder::WatchFolderConfig::default(),
        duplicate_policy: Default::default(),
        direct_store: None,
        delivery_naming: None,
        metrics: Default::default(),
        config_path: None,
    }));
    let handle = test_scheduler_handle(data_dir, db.clone());
    crate::ingest::init_job_counter(20_000);
    let service = RssService::new_with_security(handle, config, db.clone(), security);
    route_feeds(&service, &db);
    service
}

// Routes every feed saved so far, direct: a feed is fetched only on a route.
pub fn route_feeds(service: &RssService, db: &Database) {
    service.inner.handle.set_proxy_runtime(
        crate::proxies::ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap(),
    );
}

pub fn test_scheduler_handle(data_dir: &Path, db: Database) -> SchedulerHandle {
    let (events, _) = tokio::sync::broadcast::channel(8);
    let state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
    crate::pipeline::test_support::install_admission_resources(&state, db, data_dir);
    SchedulerHandle::new(events, state)
}

pub fn sample_nzb_bytes() -> Vec<u8> {
    br#"<?xml version="1.0" encoding="UTF-8"?>
        <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
          <file poster="poster" date="1700000000" subject="Silver Horizon.Sample.rar">
            <groups><group>alt.binaries.test</group></groups>
            <segments><segment bytes="100" number="1">msgid@example.com</segment></segments>
          </file>
        </nzb>"#
        .to_vec()
}
