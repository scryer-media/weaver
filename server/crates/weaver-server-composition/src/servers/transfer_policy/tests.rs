// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::proxies::ProxiesNetworkPersistenceQueries;
use crate::servers::persistence::ServersPersistenceQueries;
use crate::servers::repository::ServersRepositoryQueries;
use chrono::{Local, TimeZone};

use weaver_server_net::bandwidth::QuotaWeekday;

use super::*;

fn quota(period: ServerDownloadQuotaPeriod) -> ServerDownloadQuotaConfig {
    ServerDownloadQuotaConfig {
        enabled: true,
        limit_bytes: 1_000,
        period,
        reset_time_minutes_local: 4 * 60,
        weekly_reset_weekday: QuotaWeekday::Mon,
        monthly_reset_day: 31,
    }
}

fn egress_quota(period: ServerDownloadQuotaPeriod) -> crate::proxies::EgressQuota {
    let server = quota(period);
    crate::proxies::EgressQuota {
        download_bytes: Some(server.limit_bytes),
        period,
        reset_time_minutes_local: server.reset_time_minutes_local,
        weekly_reset_weekday: server.weekly_reset_weekday,
        monthly_reset_day: server.monthly_reset_day,
        ..Default::default()
    }
}

fn quota_server(id: u32) -> ServerConfig {
    ServerConfig {
        id,
        host: format!("news-{id}.example.com"),
        port: 563,
        tls: true,
        username: None,
        password: None,
        connections: 1,
        active: true,
        supports_pipelining: true,
        pipelining_depth: None,
        tls_name_mismatch_certificate_der: None,
        priority: 0,
        backfill: false,
        retention_days: 0,
        max_download_speed: 0,
        download_quota: quota(ServerDownloadQuotaPeriod::OneTime),
        tls_ca_cert: None,
    }
}

// Twenty-five servers over ten priority levels, every other one with
// stored usage. One read loads them all, and each holder gets what a
// read of its own row gives it, an empty count where it has none.
#[test]
fn a_reconfigure_reads_every_servers_usage_at_once() {
    let db = Database::open_in_memory().unwrap();
    let servers = (1..=25)
        .map(|id| ServerConfig {
            priority: id % 10,
            ..quota_server(id)
        })
        .collect::<Vec<_>>();
    for server in &servers {
        db.insert_server(server).unwrap();
    }
    let stored = servers
        .iter()
        .filter(|server| server.id % 2 == 1)
        .map(|server| ServerDownloadUsage {
            lifetime_bytes: u64::from(server.id) * 1_000,
            quota_baseline_bytes: u64::from(server.id),
            ..ServerDownloadUsage::empty(server.id)
        })
        .collect::<Vec<_>>();
    db.store_download_usage(&stored, &[]).unwrap();
    let registry = ServerTransferPolicyRegistry::new(db.clone(), &[]).unwrap();
    let holders = servers
        .iter()
        .map(|server| QuotaHolder {
            id: server.id,
            rate: (server.max_download_speed != 0).then_some(server.max_download_speed),
            quota: (&server.download_quota).into(),
            upload_caps: UploadCaps::default(),
        })
        .collect::<Vec<_>>();
    let statements = || {
        db.wal_checkpoint_metrics_snapshot()
            .expect("a file database")
            .statement_duration
            .count
    };

    let before = statements();
    let loaded = registry
        .load_usages(TransferScope::Server, &holders)
        .unwrap();
    assert_eq!(statements() - before, 1, "one read for every server");

    let one_by_one = servers
        .iter()
        .map(|server| {
            db.server_download_usage(server.id)
                .unwrap()
                .unwrap_or_else(|| ServerDownloadUsage::empty(server.id))
        })
        .collect::<Vec<_>>();
    // An empty count is stamped with the time it was made.
    let without_stamp = |usages: Vec<ServerDownloadUsage>| {
        usages
            .into_iter()
            .map(|usage| ServerDownloadUsage {
                updated_at: chrono::DateTime::<Utc>::UNIX_EPOCH,
                ..usage
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(without_stamp(loaded.clone()), without_stamp(one_by_one));
    assert_eq!(
        loaded
            .iter()
            .filter(|usage| usage.lifetime_bytes > 0)
            .count(),
        13
    );
}

// A bad usage row of a server the reconfigure does not hold fails
// nothing, as when each holder's row was read by its id; a bad row of a
// holder still fails the read.
#[test]
fn a_bad_usage_row_fails_only_its_own_holder() {
    let db = Database::open_in_memory().unwrap();
    let servers = (1..=3).map(quota_server).collect::<Vec<_>>();
    for server in &servers {
        db.insert_server(server).unwrap();
    }
    db.insert_server(&quota_server(4)).unwrap();
    let corrupt = |id: i64| {
        let datastore = db.datastore();
        db.run_sql_blocking(async move {
            weaver_server_db::persistence::sql_runtime::SqlRuntime::execute(
                datastore.write_exec(),
                "INSERT INTO server_download_usage
                    (server_id, lifetime_bytes, quota_baseline_bytes,
                     window_start_epoch_seconds, window_end_epoch_seconds,
                     updated_at_epoch_seconds)
                 VALUES ({}, -1, 0, NULL, NULL, 0)",
                &[weaver_server_db::persistence::sql_runtime::SqlArg::I64(id)],
            )
            .await
            .map(|_| ())
        })
        .unwrap();
    };
    // Server 4 is stored but not among the holders.
    corrupt(4);
    let registry = ServerTransferPolicyRegistry::new(db.clone(), &[]).unwrap();
    let holders = servers
        .iter()
        .map(|server| QuotaHolder {
            id: server.id,
            rate: (server.max_download_speed != 0).then_some(server.max_download_speed),
            quota: (&server.download_quota).into(),
            upload_caps: UploadCaps::default(),
        })
        .collect::<Vec<_>>();
    let loaded = registry
        .load_usages(TransferScope::Server, &holders)
        .unwrap();
    assert_eq!(
        loaded
            .iter()
            .map(|usage| usage.server_id)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );

    corrupt(2);
    assert!(
        registry
            .load_usages(TransferScope::Server, &holders)
            .is_err()
    );
    assert!(db.server_download_usage(2).is_err());
}

#[test]
fn one_time_quota_has_no_automatic_window() {
    assert!(
        quota_window(
            Local::now(),
            (&quota(ServerDownloadQuotaPeriod::OneTime)).into()
        )
        .is_none()
    );
}

#[test]
fn recurring_quota_reuses_dst_safe_bandwidth_windows() {
    let now = Local.with_ymd_and_hms(2026, 7, 9, 12, 0, 0).unwrap();
    let daily = quota_window(now, (&quota(ServerDownloadQuotaPeriod::Daily)).into()).unwrap();
    assert!(daily.start < now.with_timezone(&Utc));
    assert!(daily.end > now.with_timezone(&Utc));

    let monthly = quota_window(now, (&quota(ServerDownloadQuotaPeriod::Monthly)).into()).unwrap();
    assert!(monthly.start < monthly.end);
}

#[test]
fn policy_change_subscription_retains_changes_until_observed() {
    let registry =
        ServerTransferPolicyRegistry::new(Database::open_in_memory().unwrap(), &[]).unwrap();
    let changes = registry.subscribe_changes();

    registry.reconfigure(&[]).unwrap();

    assert!(changes.has_changed().unwrap());
}

#[test]
fn reset_after_rejection_makes_the_old_rejection_stale() {
    let server = quota_server(7);
    let db = Database::open_in_memory().unwrap();
    db.insert_server(&server).unwrap();
    let registry = ServerTransferPolicyRegistry::new(db, &[server]).unwrap();
    let control = registry.transfer_registry().control(StableServerId(7));
    let _reservation = control.try_reserve(1_000).unwrap();
    let rejection = control
        .try_reserve(1)
        .err()
        .expect("quota should reject an overbooked reservation");
    assert!(registry.quota_rejection_is_current(&rejection));

    registry.reset_usage(7).unwrap();

    assert!(!registry.quota_rejection_is_current(&rejection));
}

#[test]
fn one_flush_stores_server_and_egress_usage_together() {
    let db = Database::open_in_memory().unwrap();
    let egress = db
        .create_egress_interface(&crate::proxies::EgressInterface {
            id: 0,
            name: "Metered link".into(),
            binding: crate::proxies::EgressBinding::SourceAddress {
                address: "127.0.0.1".parse().unwrap(),
            },
            enabled: true,
            download_bps: None,
            upload_bps: None,
            quota: egress_quota(ServerDownloadQuotaPeriod::OneTime),
        })
        .unwrap();
    let server = quota_server(egress.id);
    db.insert_server(&server).unwrap();
    let registry = ServerTransferPolicyRegistry::new(db.clone(), &[server]).unwrap();
    registry
        .reconfigure_egresses(std::slice::from_ref(&egress))
        .unwrap();
    for control in [
        registry
            .transfer_registry()
            .control(StableServerId(egress.id)),
        registry
            .egress_transfer_registry()
            .control(StableServerId(egress.id)),
    ] {
        let mut permit = control.try_reserve(400).unwrap();
        permit.record_blocking(400);
        permit.finish();
    }

    registry.flush_usage().unwrap();

    let server_usage = db.server_download_usage(egress.id).unwrap().unwrap();
    assert_eq!(server_usage.lifetime_bytes, 400);
    let egress_usage = db.egress_download_usage(egress.id).unwrap().unwrap();
    assert_eq!(egress_usage.lifetime_bytes, 400);
    assert!(!registry.usage_changed_since_flush());
}

#[test]
fn resetting_egress_usage_preserves_lifetime_and_other_scopes() {
    let db = Database::open_in_memory().unwrap();
    let egress = db
        .create_egress_interface(&crate::proxies::EgressInterface {
            id: 0,
            name: "Metered link".into(),
            binding: crate::proxies::EgressBinding::SourceAddress {
                address: "127.0.0.1".parse().unwrap(),
            },
            enabled: true,
            download_bps: None,
            upload_bps: None,
            quota: egress_quota(ServerDownloadQuotaPeriod::OneTime),
        })
        .unwrap();
    let server = quota_server(egress.id);
    db.insert_server(&server).unwrap();
    let registry = ServerTransferPolicyRegistry::new(db.clone(), &[server]).unwrap();
    registry
        .reconfigure_egresses(std::slice::from_ref(&egress))
        .unwrap();
    let control = registry
        .egress_transfer_registry()
        .control(StableServerId(egress.id));
    let mut permit = control.try_reserve(1_000).unwrap();
    permit.record_blocking(1_000);
    permit.finish();
    let rejection = control.try_reserve(1).err().unwrap();
    let server_before = registry.snapshot(egress.id).unwrap();
    let changes = registry.subscribe_changes();
    let reset = registry.reset_egress_usage(egress.id).unwrap();
    assert_eq!(reset.lifetime_bytes, 1_000);
    assert_eq!(reset.used_bytes, 0);
    assert!(!reset.blocked);
    assert!(!registry.quota_rejection_is_current(&rejection));
    assert!(changes.has_changed().unwrap());
    assert_eq!(registry.snapshot(egress.id).unwrap(), server_before);
    let stored = db.egress_download_usage(egress.id).unwrap().unwrap();
    assert_eq!(stored.lifetime_bytes, 1_000);
    assert_eq!(stored.quota_baseline_bytes, 1_000);
}

#[test]
fn an_egress_rejection_is_judged_against_the_egress_not_a_server_with_its_id() {
    // A server and an egress share the id 7; only the egress is metered.
    let mut server = quota_server(7);
    server.download_quota.enabled = false;
    let db = Database::open_in_memory().unwrap();
    db.insert_server(&server).unwrap();
    let registry = ServerTransferPolicyRegistry::new(db, &[server]).unwrap();
    let mut egress = crate::proxies::EgressInterface {
        id: 7,
        name: "Metered link".into(),
        binding: crate::proxies::EgressBinding::System,
        enabled: true,
        download_bps: None,
        upload_bps: None,
        quota: egress_quota(ServerDownloadQuotaPeriod::OneTime),
    };
    registry
        .reconfigure_egresses(std::slice::from_ref(&egress))
        .unwrap();
    let control = registry
        .egress_transfer_registry()
        .control(StableServerId(7));
    let limit = egress.quota.download_bytes.unwrap();
    let _reservation = control.try_reserve(limit).unwrap();
    let rejection = control
        .try_reserve(1)
        .err()
        .expect("the egress quota should reject an overbooked reservation");
    assert_eq!(rejection.scope, TransferScope::Egress);
    assert!(registry.quota_rejection_is_current(&rejection));
    assert!(registry.egress_snapshot(7).unwrap().blocked);
    assert!(!registry.snapshot(7).unwrap().blocked);

    // The server with the same id has nothing to refuse; only a change to
    // the egress makes the rejection stale.
    assert!(
        registry
            .transfer_registry()
            .control(StableServerId(7))
            .quota_rejection_for(1)
            .is_none()
    );
    egress.quota.download_bytes = Some(limit * 2);
    registry
        .reconfigure_egresses(std::slice::from_ref(&egress))
        .unwrap();
    assert!(!registry.quota_rejection_is_current(&rejection));
    assert!(!registry.egress_snapshot(7).unwrap().blocked);
}

#[test]
fn an_egress_given_a_deleted_egress_id_follows_the_metering_for_every_egress() {
    let db = Database::open_in_memory().unwrap();
    let registry = ServerTransferPolicyRegistry::new(db, &[]).unwrap();
    let egress = crate::proxies::EgressInterface {
        id: 7,
        name: "Metered link".into(),
        binding: crate::proxies::EgressBinding::System,
        enabled: true,
        download_bps: None,
        upload_bps: None,
        quota: egress_quota(ServerDownloadQuotaPeriod::OneTime),
    };
    registry
        .reconfigure_egresses(std::slice::from_ref(&egress))
        .unwrap();
    registry.set_egress_quota_metering(true);
    registry.set_one_egress_quota_metering(7, false);
    let transfers = registry.egress_transfer_registry();
    assert!(!transfers.quota_metering_of(StableServerId(7)));

    // Deleted, then made again under the same id.
    registry.reconfigure_egresses(&[]).unwrap();
    registry
        .reconfigure_egresses(std::slice::from_ref(&egress))
        .unwrap();
    assert!(transfers.quota_metering_of(StableServerId(7)));
    let control = transfers.control(StableServerId(7));
    let _reservation = control
        .try_reserve(egress.quota.download_bytes.unwrap())
        .unwrap();
    assert!(control.try_reserve(1).is_err());
}

// An egress at a download rate of 0 moves nothing, but its download
// quota stands where its bytes put it: room left reads Normal and turns
// nothing away, its budget taken reads Stopped.
#[test]
fn a_closed_egress_reads_its_download_quota_by_its_bytes() {
    let db = Database::open_in_memory().unwrap();
    let registry = ServerTransferPolicyRegistry::new(db, &[]).unwrap();
    let mut egress = crate::proxies::EgressInterface {
        id: 7,
        name: "Metered link".into(),
        binding: crate::proxies::EgressBinding::System,
        enabled: true,
        download_bps: Some(0),
        upload_bps: None,
        quota: egress_quota(ServerDownloadQuotaPeriod::OneTime),
    };
    registry
        .reconfigure_egresses(std::slice::from_ref(&egress))
        .unwrap();
    let control = registry
        .egress_transfer_registry()
        .control(StableServerId(7));
    // Closed, it refuses work, and its quota has all its room.
    assert!(control.try_reserve(1).is_err());
    assert!(control.snapshot().quota_blocked);
    assert_eq!(
        registry.egress_quota_states(7).unwrap().download,
        QuotaState::Normal
    );
    assert!(!registry.egress_snapshot(7).unwrap().blocked);

    // Open, its budget taken; closed again, the quota is what stops it.
    egress.download_bps = None;
    registry
        .reconfigure_egresses(std::slice::from_ref(&egress))
        .unwrap();
    let _reservation = control
        .try_reserve(egress.quota.download_bytes.unwrap())
        .unwrap();
    egress.download_bps = Some(0);
    registry
        .reconfigure_egresses(std::slice::from_ref(&egress))
        .unwrap();
    assert_eq!(
        registry.egress_quota_states(7).unwrap().download,
        QuotaState::Stopped
    );
    assert!(registry.egress_snapshot(7).unwrap().blocked);
}

fn upload_capped_egress(db: &Database) -> crate::proxies::EgressInterface {
    db.create_egress_interface(&crate::proxies::EgressInterface {
        id: 0,
        name: "Upload capped".into(),
        binding: crate::proxies::EgressBinding::SourceAddress {
            address: "127.0.0.1".parse().unwrap(),
        },
        enabled: true,
        download_bps: Some(1_000),
        upload_bps: Some(4_000),
        quota: crate::proxies::EgressQuota {
            upload_soft_bytes: Some(100),
            upload_trickle_bps: 512,
            upload_hard_bytes: Some(200),
            ..Default::default()
        },
    })
    .unwrap()
}

#[test]
fn upload_moves_an_egress_through_trickle_to_stopped_and_a_reset_restores_it() {
    let db = Database::open_in_memory().unwrap();
    let egress = upload_capped_egress(&db);
    let registry = ServerTransferPolicyRegistry::new(db, &[]).unwrap();
    registry
        .reconfigure_egresses(std::slice::from_ref(&egress))
        .unwrap();
    let mut changes = registry.subscribe_changes();
    changes.borrow_and_update();

    assert_eq!(registry.record_egress_upload(egress.id + 1, 1), None);
    assert_eq!(
        registry.record_egress_upload(egress.id, 99),
        Some(QuotaState::Normal)
    );
    assert!(!changes.has_changed().unwrap());
    assert_eq!(
        registry.record_egress_upload(egress.id, 1),
        Some(QuotaState::Trickle)
    );
    assert!(changes.has_changed().unwrap());
    changes.borrow_and_update();
    assert_eq!(
        registry.record_egress_upload(egress.id, 99),
        Some(QuotaState::Trickle)
    );
    assert!(!changes.has_changed().unwrap());
    assert_eq!(
        registry.record_egress_upload(egress.id, 1),
        Some(QuotaState::Stopped)
    );
    assert!(changes.has_changed().unwrap());
    let usage = registry.egress_usage(egress.id).unwrap();
    assert_eq!(usage.upload.used_bytes, 200);
    assert_eq!(usage.upload.remaining_bytes, Some(0));
    assert_eq!(
        registry.egress_quota_states(egress.id).unwrap(),
        EgressQuotaStates {
            download: QuotaState::Normal,
            upload: QuotaState::Stopped,
        }
    );

    registry.reset_egress_usage(egress.id).unwrap();
    assert_eq!(
        registry.egress_quota_states(egress.id).unwrap().upload,
        QuotaState::Normal
    );
    assert_eq!(
        registry.egress_usage(egress.id).unwrap().upload.used_bytes,
        0
    );
}

#[test]
fn an_egress_rate_is_the_tightest_limit_and_its_quota_state_a_floor() {
    let db = Database::open_in_memory().unwrap();
    let egress = upload_capped_egress(&db);
    let registry = ServerTransferPolicyRegistry::new(db, &[]).unwrap();
    registry
        .reconfigure_egresses(std::slice::from_ref(&egress))
        .unwrap();
    let rate =
        |direction, global, schedule| registry.egress_rate(egress.id, direction, global, schedule);
    // The egress cap, then a tighter global or scheduled limit.
    assert_eq!(rate(Direction::Download, None, None), Some(1_000));
    assert_eq!(rate(Direction::Download, Some(500), None), Some(500));
    assert_eq!(rate(Direction::Download, Some(500), Some(300)), Some(300));
    assert_eq!(rate(Direction::Download, None, Some(0)), Some(0));
    assert_eq!(rate(Direction::Upload, None, None), Some(4_000));
    // An egress the registry does not hold adds no limit of its own.
    assert_eq!(
        registry.egress_rate(egress.id + 1, Direction::Upload, Some(9), None),
        Some(9)
    );
    assert_eq!(
        registry.egress_rate(egress.id + 1, Direction::Upload, None, None),
        None
    );

    // Past the soft cap uploads trickle; a looser schedule cannot lift it.
    registry.record_egress_upload(egress.id, 100);
    assert_eq!(rate(Direction::Upload, None, Some(1_000_000)), Some(512));
    assert_eq!(rate(Direction::Upload, None, Some(100)), Some(100));
    assert_eq!(rate(Direction::Download, None, None), Some(1_000));
    // Past the hard cap they stop.
    registry.record_egress_upload(egress.id, 100);
    assert_eq!(rate(Direction::Upload, Some(1_000_000), None), Some(0));
}

#[test]
fn reset_notifies_waiters_even_when_persistence_fails() {
    let registry =
        ServerTransferPolicyRegistry::new(Database::open_in_memory().unwrap(), &[quota_server(8)])
            .unwrap();
    let mut changes = registry.subscribe_changes();
    let before = *changes.borrow_and_update();

    assert!(registry.reset_usage(8).is_err());

    assert!(changes.has_changed().unwrap());
    assert_eq!(*changes.borrow_and_update(), before.wrapping_add(1));
}

#[test]
fn multi_server_reconfigure_publishes_one_complete_revision() {
    let db = Database::open_in_memory().unwrap();
    let servers = [quota_server(9), quota_server(10)];
    for server in &servers {
        db.insert_server(server).unwrap();
    }
    let registry = ServerTransferPolicyRegistry::new(db, &[]).unwrap();
    let mut changes = registry.subscribe_changes();
    let before = *changes.borrow_and_update();

    registry.reconfigure(&servers).unwrap();

    assert!(changes.has_changed().unwrap());
    assert_eq!(*changes.borrow_and_update(), before.wrapping_add(1));
}

#[test]
fn remainder_too_small_rejection_stays_current_until_capacity_changes() {
    let db = Database::open_in_memory().unwrap();
    let server = quota_server(11);
    db.insert_server(&server).unwrap();
    let registry = ServerTransferPolicyRegistry::new(db, &[server]).unwrap();
    let control = registry.transfer_registry().control(StableServerId(11));
    let _reservation = control.try_reserve(900).unwrap();
    let rejection = control
        .try_reserve(200)
        .err()
        .expect("remaining quota must reject an oversized estimate");
    assert!(!rejection.snapshot.quota_blocked);
    assert!(registry.quota_rejection_is_current(&rejection));

    control.update_config(ServerTransferConfig {
        closed: false,
        rate_bytes_per_sec: 0,
        quota: Some(QuotaRuntimeConfig {
            limit_bytes: 1_200,
            generation: rejection.snapshot.quota_generation,
            retry_at: rejection.retry_at,
        }),
    });

    assert!(!registry.quota_rejection_is_current(&rejection));
}

#[test]
fn snapshots_are_cached_reads_and_leave_rollover_to_maintenance() {
    let db = Database::open_in_memory().unwrap();
    let mut server = quota_server(12);
    server.download_quota.period = ServerDownloadQuotaPeriod::Daily;
    db.insert_server(&server).unwrap();
    let registry = ServerTransferPolicyRegistry::new(db.clone(), &[server]).unwrap();
    let expired = ServerQuotaWindow {
        start: Utc::now() - chrono::Duration::days(2),
        end: Utc::now() - chrono::Duration::days(1),
    };
    registry.servers.policies().get_mut(&12).unwrap().window = Some(expired);

    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        weaver_server_db::persistence::sql_runtime::SqlRuntime::execute(
            datastore.write_exec(),
            "DROP TABLE server_download_usage",
            &[],
        )
        .await?;
        Ok(())
    })
    .unwrap();

    let snapshot = registry.snapshot(12).unwrap();
    assert_eq!(snapshot.window_end, Some(expired.end));
    let snapshots = registry.snapshots();
    assert_eq!(snapshots[0].window_end, Some(expired.end));
    assert_eq!(registry.servers.policies()[&12].window, Some(expired));
}
