use super::*;
use std::collections::HashMap;

/// Two fill servers, the second in a lower priority group, plus an optional
/// backfill server in the highest group there is.
fn grouped_client(groups: &[(u32, bool)]) -> weaver_nntp::client::NntpClient {
    let servers = groups
        .iter()
        .enumerate()
        .map(
            |(idx, (group, backfill))| weaver_nntp::pool::ServerPoolConfig {
                server: weaver_nntp::ServerConfig {
                    host: format!("grouped-{idx}.example.com"),
                    ..Default::default()
                },
                max_connections: 4,
                group: *group,
                backfill: *backfill,
                ..weaver_nntp::pool::ServerPoolConfig::default()
            },
        )
        .collect();
    weaver_nntp::client::NntpClient::new(weaver_nntp::client::NntpClientConfig {
        servers,
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(1),
    })
}

/// An idle cached lane is a reason to prefer a server over its peers, not a
/// reason to outrank the group above it. Priority in this pipeline is a
/// preference order: the lower group takes ordinary work only once the
/// higher one is out of seats. A job retrying hard on a low-priority server
/// keeps an idle lane there almost continuously, and ordering on idleness
/// alone then handed that server every other job's articles — which reads
/// downstream as a primary that never failed.
#[test]
fn an_idle_lane_does_not_promote_a_server_over_the_group_above_it() {
    let client = grouped_client(&[(0, false), (1, false)]);
    let groups = client.pool().server_groups().to_vec();
    let backfill = client.pool().server_backfill_flags().to_vec();
    let idle_by_server = HashMap::from([(1usize, 1usize)]);

    // The pool's own ranking, highest group first.
    let mut servers = vec![0usize, 1usize];
    Pipeline::order_dispatch_candidates(&mut servers, &groups, &backfill, &idle_by_server);
    assert_eq!(
        servers[0], 0,
        "the group-0 server still has seats, so it is asked first"
    );
    assert!(
        servers.contains(&1),
        "the group-1 server stays a candidate for when group 0 runs out of seats"
    );
}

/// Inside one group the preference is the point: a server that already holds
/// a connection saves a dial, so it goes first among its equals.
#[test]
fn an_idle_lane_wins_inside_its_own_group() {
    let client = grouped_client(&[(0, false), (0, false)]);
    let groups = client.pool().server_groups().to_vec();
    let backfill = client.pool().server_backfill_flags().to_vec();
    let idle_by_server = HashMap::from([(1usize, 1usize)]);

    let mut servers = vec![0usize, 1usize];
    Pipeline::order_dispatch_candidates(&mut servers, &groups, &backfill, &idle_by_server);
    assert_eq!(servers, vec![1, 0]);
}

/// Backfill is a reservation, not a preference: it is ordered after every
/// fill server even when its group number is the highest and it is the one
/// holding an idle lane.
#[test]
fn a_backfill_server_stays_last_however_it_is_grouped() {
    let client = grouped_client(&[(1, false), (0, true)]);
    let groups = client.pool().server_groups().to_vec();
    let backfill = client.pool().server_backfill_flags().to_vec();
    let idle_by_server = HashMap::from([(1usize, 1usize)]);

    let mut servers = vec![0usize, 1usize];
    Pipeline::order_dispatch_candidates(&mut servers, &groups, &backfill, &idle_by_server);
    assert_eq!(servers, vec![0, 1]);
}
