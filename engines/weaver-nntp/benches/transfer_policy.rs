use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use weaver_nntp::transfer::{
    QuotaRuntimeConfig, ServerTransferConfig, ServerTransferRegistry, StableServerId,
};

const CHUNKS: usize = 16;
const CHUNK_BYTES: usize = 4 * 1024;

fn bench_transfer_policy(c: &mut Criterion) {
    let mut group = c.benchmark_group("transfer_policy");
    let unlimited_registry = ServerTransferRegistry::new();
    let unlimited_control =
        unlimited_registry.configure(StableServerId(1), ServerTransferConfig::default());

    group.bench_function("unlimited_marker_relaxed_counter", |b| {
        b.iter(|| {
            for _ in 0..CHUNKS {
                unlimited_control.record_unlimited_body_bytes(CHUNK_BYTES);
            }
            black_box(&unlimited_control)
        })
    });

    let rate_registry = ServerTransferRegistry::new();
    let rate_control = rate_registry.configure(StableServerId(2), ServerTransferConfig::default());
    let rate_config = ServerTransferConfig {
        rate_bytes_per_sec: 1_000_000_000,
        quota: None,
    };
    group.bench_function("rate_only_atomic_tickets", |b| {
        b.iter_batched(
            || {
                rate_control.update_config(ServerTransferConfig::default());
                rate_control.update_config(rate_config);
                rate_control.try_reserve(0).unwrap()
            },
            |mut permit| {
                for _ in 0..CHUNKS {
                    black_box(permit.record_blocking(CHUNK_BYTES));
                }
                black_box(permit.finish())
            },
            BatchSize::PerIteration,
        )
    });

    let quota_registry = ServerTransferRegistry::new();
    let quota_control =
        quota_registry.configure(StableServerId(3), ServerTransferConfig::default());
    let quota_generation = std::cell::Cell::new(0_u64);
    group.bench_function("quota_only_terminal_reconcile", |b| {
        b.iter_batched(
            || {
                let generation = quota_generation.get().wrapping_add(1);
                quota_generation.set(generation);
                quota_control.update_config(ServerTransferConfig {
                    rate_bytes_per_sec: 0,
                    quota: Some(QuotaRuntimeConfig {
                        limit_bytes: 1024 * 1024,
                        generation,
                        retry_at: None,
                    }),
                });
                quota_control
                    .try_reserve((CHUNKS * CHUNK_BYTES) as u64)
                    .unwrap()
            },
            |mut permit| {
                for _ in 0..CHUNKS {
                    black_box(permit.record_blocking(CHUNK_BYTES));
                }
                black_box(permit.finish())
            },
            BatchSize::PerIteration,
        )
    });

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let mixed_registry = ServerTransferRegistry::new();
    let mixed_control =
        mixed_registry.configure(StableServerId(4), ServerTransferConfig::default());
    let mixed_generation = std::cell::Cell::new(0_u64);
    group.bench_function("mixed_async_blocking", |b| {
        b.iter_batched(
            || {
                let generation = mixed_generation.get().wrapping_add(1);
                mixed_generation.set(generation);
                mixed_control.update_config(ServerTransferConfig::default());
                mixed_control.update_config(ServerTransferConfig {
                    rate_bytes_per_sec: 1_000_000_000,
                    quota: Some(QuotaRuntimeConfig {
                        limit_bytes: 1024 * 1024,
                        generation,
                        retry_at: None,
                    }),
                });
                (
                    mixed_control
                        .try_reserve((CHUNKS * CHUNK_BYTES) as u64)
                        .unwrap(),
                    mixed_control
                        .try_reserve((CHUNKS * CHUNK_BYTES) as u64)
                        .unwrap(),
                )
            },
            |(mut async_permit, mut blocking_permit)| {
                runtime.block_on(async {
                    for _ in 0..CHUNKS {
                        black_box(async_permit.record_async(CHUNK_BYTES).await);
                    }
                });
                for _ in 0..CHUNKS {
                    black_box(blocking_permit.record_blocking(CHUNK_BYTES));
                }
                black_box(async_permit.finish());
                black_box(blocking_permit.finish());
            },
            BatchSize::PerIteration,
        )
    });

    group.finish();
}

criterion_group!(benches, bench_transfer_policy);
criterion_main!(benches);
