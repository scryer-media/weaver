use std::time::Duration;

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use weaver_nntp::connection::ServerConfig;
use weaver_nntp::health::HealthConfig;
use weaver_nntp::pool::{NntpPool, PoolConfig, ServerPoolConfig};
use weaver_nntp::transfer::StableServerId;

fn test_pool(server_count: usize) -> NntpPool {
    let servers = (0..server_count)
        .map(|index| ServerPoolConfig {
            server: ServerConfig {
                host: format!("server-{index}.example.com"),
                ..Default::default()
            },
            stable_id: StableServerId(index as u32 + 1),
            max_connections: 1,
            group: 0,
            // Force availability inspection to scan every server before it
            // reaches the one fill server at the end.
            backfill: index + 1 < server_count,
            retention_days: 0,
            transfer_control: None,
        })
        .collect();
    NntpPool::new(PoolConfig {
        servers,
        max_idle_age: Duration::from_secs(300),
        health_config: HealthConfig::default(),
        reconnect_delay: Duration::from_secs(1),
        stale_check_age: Duration::from_secs(30),
    })
}

fn bench_body_server_availability(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("body_server_availability");
    for server_count in [1_usize, 8, 64] {
        let pool = test_pool(server_count);
        group.bench_with_input(
            BenchmarkId::from_parameter(server_count),
            &server_count,
            |b, _| {
                b.iter(|| black_box(runtime.block_on(pool.body_server_availability(&[], &[], 0))));
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_body_server_availability);
criterion_main!(benches);
