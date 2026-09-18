//! Every configured server has a depth explorer before the first lane runs.

use super::*;
use crate::pipeline::download::transport::DownloadLaneMode;

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

/// The first lease a lane takes is dispatched pipelined.
///
/// Nothing had built an explorer for a server until its first BODY response
/// came back, so the opening of every download — every lane, every job — was
/// dispatched sequential and gave back a whole round trip per article before
/// the ladder could even begin.
#[tokio::test]
async fn the_first_lease_on_a_pipelining_server_is_already_pipelined() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = std::sync::Arc::new(pool_with(vec![server(
        "pipelines.example.invalid",
        weaver_nntp::PipeliningCapability::Known(true),
        None,
    )]));
    pipeline.seed_download_lane_explorers();
    let pressure = pipeline.refresh_download_pressure();

    assert_eq!(
        pipeline.download_lane_mode_for_server(0, pressure, true),
        DownloadLaneMode::Pipelined { depth: 2 }
    );
}

/// The rung the last run proved is where this one starts.
#[tokio::test]
async fn a_proven_depth_seeds_the_first_lease() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = std::sync::Arc::new(pool_with(vec![server(
        "proven.example.invalid",
        weaver_nntp::PipeliningCapability::Known(true),
        Some(8),
    )]));
    pipeline.seed_download_lane_explorers();
    let pressure = pipeline.refresh_download_pressure();

    assert_eq!(
        pipeline.download_lane_mode_for_server(0, pressure, true),
        DownloadLaneMode::Pipelined { depth: 8 }
    );
}

/// A server that does not pipeline, or whose capability has never been
/// established, is seeded too — and stays sequential. The entry exists so the
/// per-server lookup finds a definite answer rather than a missing one.
#[tokio::test]
async fn a_server_without_a_known_capability_is_seeded_sequential() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = std::sync::Arc::new(pool_with(vec![
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
    pipeline.seed_download_lane_explorers();
    let pressure = pipeline.refresh_download_pressure();

    let modes: Vec<(usize, DownloadLaneMode)> = (0..3)
        .map(|server_idx| {
            (
                server_idx,
                pipeline.download_lane_mode_for_server(server_idx, pressure, true),
            )
        })
        .collect();
    assert_eq!(
        modes,
        vec![
            (0, DownloadLaneMode::Sequential),
            (1, DownloadLaneMode::Sequential),
            (2, DownloadLaneMode::Pipelined { depth: 4 }),
        ]
    );
    // A lane whose own connection never negotiated pipelining runs sequential
    // whatever its server's explorer says.
    assert_eq!(
        pipeline.download_lane_mode_for_server(2, pressure, false),
        DownloadLaneMode::Sequential
    );
}

/// Re-seeding against a new pool layout keeps what a surviving server had
/// already measured, matched by stable id rather than by position.
#[tokio::test]
async fn reseeding_carries_a_surviving_servers_measurements_across() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
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

    pipeline.nntp = std::sync::Arc::new(pool_with(vec![survivor()]));
    pipeline.seed_download_lane_explorers();
    // A round trip four times the article's wire time: the link wants eight.
    let explorer = pipeline
        .download_lane_runtime
        .servers
        .get_mut(&0)
        .expect("the only server is seeded");
    explorer.note_latency(Duration::from_millis(100));
    explorer.note_transfer(Duration::from_millis(25));

    // Rebuild the pool with a second server, which reshuffles the positions
    // the explorers are keyed by.
    pipeline.nntp = std::sync::Arc::new(pool_with(vec![newcomer(), survivor()]));
    pipeline.seed_download_lane_explorers();
    let pressure = pipeline.refresh_download_pressure();

    let by_stable_id: std::collections::HashMap<u32, DownloadLaneMode> = (0..2)
        .map(|server_idx| {
            let mode = pipeline.download_lane_mode_for_server(server_idx, pressure, true);
            let stable_id = pipeline
                .nntp
                .pool()
                .stable_server_id(weaver_nntp::pool::ServerId(server_idx))
                .expect("every pool position has a stable id");
            (stable_id.0, mode)
        })
        .collect();
    assert_eq!(
        by_stable_id.get(&7),
        Some(&DownloadLaneMode::Pipelined { depth: 8 }),
        "the survivor keeps the depth its own measurements ask for"
    );
    assert_eq!(
        by_stable_id.get(&9),
        Some(&DownloadLaneMode::Pipelined { depth: 2 }),
        "the newcomer has measured nothing and starts at the shallowest rung"
    );
}
