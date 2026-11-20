// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Every configured server has a depth explorer before the first lane runs.

use std::time::Duration;
use weaver_nntp::client::{NntpClient, NntpClientConfig};
use weaver_server_pipeline::pipeline::tests::dispatch_fixture::{
    DispatchFixture, lanes::UNCLEAN_BURST_WINDOW,
};

fn pool_with(servers: Vec<weaver_nntp::pool::ServerPoolConfig>) -> NntpClient {
    NntpClient::new(NntpClientConfig {
        servers,
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(1),
    })
}

fn server(
    host: &str,
    pipelining: weaver_nntp::PipeliningCapability,
    pipelining_depth: Option<u8>,
) -> weaver_nntp::pool::ServerPoolConfig {
    weaver_nntp::pool::ServerPoolConfig {
        server: weaver_nntp::ServerConfig {
            host: host.to_string(),
            port: 119,
            tls: false,
            pipelining,
            pipelining_depth,
            ..Default::default()
        },
        max_connections: 4,
        ..Default::default()
    }
}

// The first lease a lane takes is dispatched pipelined.
//
// Nothing had built an explorer for a server until its first BODY response
// came back, so the opening of every download — every lane, every job — was
// dispatched sequential and gave back a whole round trip per article before
// the ladder could even begin.
#[tokio::test]
async fn the_first_lease_on_a_pipelining_server_is_already_pipelined() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    pipeline.seed_explorers(pool_with(vec![server(
        "pipelines.example.invalid",
        weaver_nntp::PipeliningCapability::Known(true),
        None,
    )]));
    let pressure = pipeline.pressure();

    assert_eq!(pipeline.pipelined_depth(0, pressure, true), Some(2));
}

// The rung the last run proved is where this one starts.
#[tokio::test]
async fn a_proven_depth_seeds_the_first_lease() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    pipeline.seed_explorers(pool_with(vec![server(
        "proven.example.invalid",
        weaver_nntp::PipeliningCapability::Known(true),
        Some(8),
    )]));
    let pressure = pipeline.pressure();

    assert_eq!(pipeline.pipelined_depth(0, pressure, true), Some(8));
}

// A server that does not pipeline, or whose capability has never been
// established, is seeded too — and stays sequential. The entry exists so the
// per-server lookup finds a definite answer rather than a missing one.
#[tokio::test]
async fn a_server_without_a_known_capability_is_seeded_sequential() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    pipeline.seed_explorers(pool_with(vec![
        server(
            "unknown.example.invalid",
            weaver_nntp::PipeliningCapability::Probe,
            Some(8),
        ),
        server(
            "refuses.example.invalid",
            weaver_nntp::PipeliningCapability::Known(false),
            Some(8),
        ),
        server(
            "pipelines.example.invalid",
            weaver_nntp::PipeliningCapability::Known(true),
            Some(4),
        ),
    ]));
    let pressure = pipeline.pressure();

    let modes: Vec<(usize, Option<u8>)> = (0..3)
        .map(|server_idx| {
            (
                server_idx,
                pipeline.pipelined_depth(server_idx, pressure, true),
            )
        })
        .collect();
    assert_eq!(modes, vec![(0, None), (1, None), (2, Some(4)),]);
    // A lane whose own connection never negotiated pipelining runs sequential
    // whatever its server's explorer says.
    assert_eq!(pipeline.pipelined_depth(2, pressure, false), None);
}

// Re-seeding against a new pool layout keeps what a surviving server had
// already measured, matched by stable id rather than by position.
#[tokio::test]
async fn reseeding_carries_a_surviving_servers_measurements_across() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let survivor = || {
        let mut config = server(
            "survivor.example.invalid",
            weaver_nntp::PipeliningCapability::Known(true),
            None,
        );
        config.stable_id = weaver_nntp::transfer::StableServerId(7);
        config
    };
    let newcomer = || {
        let mut config = server(
            "newcomer.example.invalid",
            weaver_nntp::PipeliningCapability::Known(true),
            None,
        );
        config.stable_id = weaver_nntp::transfer::StableServerId(9);
        config
    };

    pipeline.seed_explorers(pool_with(vec![survivor()]));
    // A round trip four times the article's wire time: the link wants eight.
    pipeline.note_timing(0, Duration::from_millis(100), Duration::from_millis(25));

    // Rebuild the pool with a second server, which reshuffles the positions
    // the explorers are keyed by.
    pipeline.seed_explorers(pool_with(vec![newcomer(), survivor()]));
    let pressure = pipeline.pressure();

    let by_stable_id: std::collections::HashMap<u32, Option<u8>> = (0..2)
        .map(|server_idx| {
            let mode = pipeline.pipelined_depth(server_idx, pressure, true);
            let stable_id = pipeline
                .client()
                .pool()
                .stable_server_id(weaver_nntp::pool::ServerId(server_idx))
                .expect("every pool position has a stable id");
            (stable_id.0, mode)
        })
        .collect();
    assert_eq!(
        by_stable_id.get(&7),
        Some(&Some(8)),
        "the survivor keeps the depth its own measurements ask for"
    );
    assert_eq!(
        by_stable_id.get(&9),
        Some(&Some(2)),
        "the newcomer has measured nothing and starts at the shallowest rung"
    );
}

// A server pinned to sequential fetch stays pinned through a pool rebuild.
//
// The rebuild used to seed a fresh explorer for every server, so a server
// that had twice failed a pipelined batch came back at depth 2 and unpinned:
// the lanes pipelined to it again, and its health card showed a depth the
// log had just said was given up.
#[tokio::test]
async fn a_pinned_server_stays_sequential_across_a_pool_rebuild() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let with_id = |host: &str, id: u32| {
        let mut config = server(
            host,
            weaver_nntp::PipeliningCapability::Known(true),
            Some(4),
        );
        config.stable_id = weaver_nntp::transfer::StableServerId(id);
        config
    };

    pipeline.seed_explorers(pool_with(vec![
        with_id("pinned.example.invalid", 7),
        with_id("clean.example.invalid", 8),
    ]));
    let now = std::time::Instant::now();
    pipeline.note_unclean(0, now);
    pipeline.note_unclean(0, now + UNCLEAN_BURST_WINDOW);
    assert!(pipeline.pinned(0));

    // The rebuild swaps the two positions.
    pipeline.seed_explorers(pool_with(vec![
        with_id("clean.example.invalid", 8),
        with_id("pinned.example.invalid", 7),
    ]));
    let pressure = pipeline.pressure();

    assert_eq!(pipeline.pipelined_depth(1, pressure, true), None);
    assert_eq!(pipeline.pipelined_depth(0, pressure, true), Some(4));
    let shown = pipeline.transport_health();
    assert_eq!(
        shown
            .iter()
            .map(|entry| (
                entry.server_idx,
                entry.pipeline_depth,
                entry.pinned_sequential
            ))
            .collect::<Vec<_>>(),
        vec![(0, 4, false), (1, 1, true)]
    );
}

// The pin belongs to the server that earned it: the same stable id, host,
// port and username, as a rebuild knows a server. A reorder or a priority
// change keeps it. An edit that points the entry at another host or port
// starts it unpinned: that backend never failed a pipelined batch.
#[tokio::test]
async fn a_pin_follows_its_server_and_not_an_edited_host_or_port() {
    let entry = |host: &str, port: u16, group: u32, id: u32| {
        let mut config = server(
            host,
            weaver_nntp::PipeliningCapability::Known(true),
            Some(4),
        );
        config.server.port = port;
        config.group = group;
        config.stable_id = weaver_nntp::transfer::StableServerId(id);
        config
    };
    let other = |group: u32| entry("clean.example.invalid", 119, group, 8);
    // Each case rebuilds the pool from a pinned "nova" at position 0, and
    // names where the entry is afterwards and whether it is still pinned.
    let cases = [
        (
            "reordered",
            vec![other(0), entry("nova.example.invalid", 119, 0, 7)],
            1,
            true,
        ),
        (
            "moved to another level",
            vec![entry("nova.example.invalid", 119, 3, 7), other(0)],
            0,
            true,
        ),
        (
            "pointed at another port",
            vec![entry("nova.example.invalid", 563, 0, 7), other(0)],
            0,
            false,
        ),
        (
            "pointed at another host",
            vec![entry("vega.example.invalid", 119, 0, 7), other(0)],
            0,
            false,
        ),
        (
            "given another username",
            vec![
                {
                    let mut renamed = entry("nova.example.invalid", 119, 0, 7);
                    renamed.server.username = Some("another-account".to_string());
                    renamed
                },
                other(0),
            ],
            0,
            false,
        ),
    ];
    for (case, rebuilt, position, pinned) in cases {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut pipeline = DispatchFixture::new(&temp_dir).await;
        pipeline.seed_explorers(pool_with(vec![
            entry("nova.example.invalid", 119, 0, 7),
            other(0),
        ]));
        let now = std::time::Instant::now();
        pipeline.note_unclean(0, now);
        pipeline.note_unclean(0, now + UNCLEAN_BURST_WINDOW);
        assert!(pipeline.pinned(0));

        pipeline.seed_explorers(pool_with(rebuilt));
        assert_eq!(pipeline.pinned(position), pinned, "{case}");
    }
}
