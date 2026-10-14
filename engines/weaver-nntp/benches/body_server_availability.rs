use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use weaver_nntp::connection::ServerConfig;
use weaver_nntp::health::HealthConfig;
use weaver_nntp::pool::{NntpPool, PoolConfig, ServerPoolConfig};
use weaver_nntp::route_dialer::{AddressPlanned, RouteDialer};
use weaver_nntp::transfer::{ServerTransferRegistry, StableServerId};

// A route for configs the bench never dials.
#[expect(
    clippy::disallowed_methods,
    reason = "a bench pool that never dials needs a route to hold"
)]
fn idle_route(runtime: &tokio::runtime::Runtime) -> Arc<RouteDialer> {
    let planned = Arc::new(AddressPlanned::new(
        "bench".into(),
        Arc::new(weaver_tunnel::pipe::Egress::new(
            0,
            weaver_nntp::egress::SocketEgress::System,
            Duration::from_secs(30),
        )),
    ));
    Arc::new(RouteDialer {
        plan: Some(planned.plan.clone()),
        inner: planned,
        egress_controls: Arc::new(ServerTransferRegistry::new()),
        runtime: runtime.handle().clone(),
        server: 0,
    })
}

fn test_pool(server_count: usize, runtime: &tokio::runtime::Runtime) -> NntpPool {
    let route = idle_route(runtime);
    let servers = (0..server_count)
        .map(|index| ServerPoolConfig {
            server: ServerConfig {
                host: format!("server-{index}.example.com"),
                ..ServerConfig::new(Arc::clone(&route))
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
        let pool = test_pool(server_count, &runtime);
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
