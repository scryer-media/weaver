// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::proxies::ProxiesNetworkPersistenceQueries;

// A torrent that holds `0` on its leg.
struct Holds(super::super::SessionTraffic);

impl super::super::LegTenant for Holds {
    fn traffic(&self) -> super::super::SessionTraffic {
        self.0
    }
}

#[tokio::test]
async fn a_torrent_session_on_its_legs_own_sockets_counts_on_that_leg() {
    let runtime = NetworkRuntime::new(
        crate::Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let consumer = super::super::Consumer::Torrent(super::super::TorrentScope::Default);
    let leg = |snapshot: &NetworkMetricsSnapshot| {
        let leg = snapshot
            .legs
            .iter()
            .find(|leg| leg.consumer == consumer.key() && leg.position == 0)
            .unwrap();
        (leg.live, leg.open, leg.opening, leg.bytes_per_second)
    };
    assert_eq!(leg(&snapshot(&runtime)), (false, 0, 0, 0));

    let tenancy = runtime.tenant_torrent(
        consumer,
        0,
        Holds(super::super::SessionTraffic {
            open: 63,
            opening: 5,
            bytes_per_second: 31_000_000,
            ..Default::default()
        }),
    );
    assert_eq!(leg(&snapshot(&runtime)), (true, 63, 5, 31_000_000));

    // The sessions let the leg go: it holds nothing.
    drop(tenancy);
    assert_eq!(leg(&snapshot(&runtime)), (false, 0, 0, 0));
}

#[tokio::test]
async fn torrent_bytes_count_in_the_egress_totals_and_not_its_download_quota() {
    let db = crate::Database::open_in_memory().unwrap();
    let egress = db
        .create_egress_interface(&super::super::EgressInterface {
            id: 0,
            name: "Metered link".into(),
            binding: super::super::EgressBinding::SourceAddress {
                address: "127.0.0.1".parse().unwrap(),
            },
            enabled: true,
            download_bps: None,
            upload_bps: None,
            quota: super::super::EgressQuota {
                download_bytes: Some(1_000_000),
                period: crate::servers::ServerDownloadQuotaPeriod::OneTime,
                ..Default::default()
            },
        })
        .unwrap();
    let policy = Arc::new(
        crate::servers::transfer_policy::ServerTransferPolicyRegistry::new(db.clone(), &[])
            .unwrap(),
    );
    let runtime =
        NetworkRuntime::with_quota_policy(db, tokio::runtime::Handle::current(), Some(policy))
            .unwrap();

    runtime.record_torrent_bytes(egress.id, 1_270_000, 300);
    runtime.record_torrent_bytes(egress.id, 30_000, 0);

    let transfer = runtime
        .egress_controls
        .snapshot(weaver_nntp::transfer::StableServerId(egress.id));
    assert_eq!(
        (transfer.lifetime_body_bytes, transfer.quota_used_bytes),
        (1_300_000, 0)
    );
    let metrics = snapshot(&runtime);
    let row = metrics
        .egresses
        .iter()
        .find(|row| row.egress_id == egress.id)
        .unwrap();
    assert_eq!(row.upload_bytes, 300);
}

#[test]
fn state_changes_are_dated_to_the_scrape_that_saw_them() {
    let mut map = HashMap::new();
    assert_eq!(since(&mut map, 1u32, LegStateLabel::Up, 10), 10);
    assert_eq!(since(&mut map, 1u32, LegStateLabel::Up, 20), 10);
    assert_eq!(since(&mut map, 1u32, LegStateLabel::Down, 30), 30);
    assert_eq!(since(&mut map, 1u32, LegStateLabel::Down, 40), 30);
}
