//! CRC32 combine: the native zlib-style operator against `crc-fast`'s
//! zeros-operator matrices, at the lengths the pipeline actually combines
//! (checkpoint segments, article sizes, file-sized tails).

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use weaver_yenc::{Crc32Combine, crc32_combine};

fn crc_fast_combine(crc_a: u32, crc_b: u32, len_b: u64) -> u32 {
    crc_fast::checksum_combine(
        crc_fast::CrcAlgorithm::Crc32IsoHdlc,
        u64::from(crc_a),
        u64::from(crc_b),
        len_b,
    ) as u32
}

fn bench_combine(c: &mut Criterion) {
    let mut group = c.benchmark_group("crc32_combine");
    for &len in &[4_096u64, 768_000, 50 << 20, 1 << 40] {
        group.bench_function(format!("crc_fast/len={len}"), |b| {
            b.iter(|| {
                crc_fast_combine(
                    black_box(0x1234_5678),
                    black_box(0x9abc_def0),
                    black_box(len),
                )
            })
        });
        group.bench_function(format!("native/len={len}"), |b| {
            b.iter(|| {
                crc32_combine(
                    black_box(0x1234_5678),
                    black_box(0x9abc_def0),
                    black_box(len),
                )
            })
        });
        let op = Crc32Combine::new(len);
        group.bench_function(format!("native_reused_op/len={len}"), |b| {
            b.iter(|| black_box(op).combine(black_box(0x1234_5678), black_box(0x9abc_def0)))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_combine);
criterion_main!(benches);
