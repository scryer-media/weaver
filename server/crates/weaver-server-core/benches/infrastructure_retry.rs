use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use tokio::time::Instant;

#[allow(dead_code, unused_imports)]
#[path = "../src/pipeline/infrastructure_retry.rs"]
mod infrastructure_retry;

use infrastructure_retry::InfrastructureRetryQueue;

fn populated_queue(count: usize) -> InfrastructureRetryQueue<(usize, usize)> {
    let mut queue = InfrastructureRetryQueue::default();
    for index in 0..count {
        let delay = (index % 2 == 0).then_some(Duration::ZERO);
        queue.schedule(delay, (index % 8, index));
    }
    queue
}

fn bench_infrastructure_retry_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("infrastructure_retry_queue");
    group.sample_size(10);

    for count in [1_000_usize, 10_000, 100_000] {
        group.bench_with_input(BenchmarkId::new("schedule", count), &count, |b, &count| {
            b.iter(|| {
                let queue = populated_queue(count);
                black_box(queue.len());
            });
        });
        group.bench_with_input(BenchmarkId::new("take_due", count), &count, |b, &count| {
            b.iter_batched(
                || populated_queue(count),
                |mut queue| black_box(queue.take_due(Instant::now()).len()),
                BatchSize::LargeInput,
            );
        });
        group.bench_with_input(BenchmarkId::new("wake_all", count), &count, |b, &count| {
            b.iter_batched(
                || populated_queue(count),
                |mut queue| black_box(queue.wake_all().len()),
                BatchSize::LargeInput,
            );
        });
        group.bench_with_input(
            BenchmarkId::new("cancel_job", count),
            &count,
            |b, &count| {
                b.iter_batched(
                    || populated_queue(count),
                    |mut queue| black_box(queue.cancel_where(|(job, _)| *job == 3)),
                    BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_infrastructure_retry_queue);
criterion_main!(benches);
