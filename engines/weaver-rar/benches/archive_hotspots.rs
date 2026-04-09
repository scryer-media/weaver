use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn fixture(dir: &str, name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(dir)
        .join(name)
}

fn extract_options() -> weaver_rar::ExtractOptions {
    weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    }
}

fn bench_solid_extract_all_members(c: &mut Criterion, dir: &str, name: &str, bench_name: &str) {
    let path = fixture(dir, name);
    let options = extract_options();
    c.bench_function(bench_name, |b| {
        b.iter(|| {
            let mut archive =
                weaver_rar::RarArchive::open(std::fs::File::open(&path).unwrap()).unwrap();
            let member_count = archive.metadata().members.len();
            for member_index in 0..member_count {
                let data = archive
                    .extract_member(member_index, &options, None)
                    .unwrap();
                black_box(data);
            }
        });
    });
}

fn bench_solid_reopen_later_member(c: &mut Criterion, dir: &str, name: &str, bench_name: &str) {
    let path = fixture(dir, name);
    let options = extract_options();
    c.bench_function(bench_name, |b| {
        b.iter(|| {
            let mut archive =
                weaver_rar::RarArchive::open(std::fs::File::open(&path).unwrap()).unwrap();
            let data = archive.extract_member(1, &options, None).unwrap();
            black_box(data);
        });
    });
}

fn bench_non_solid_lz_chunked(c: &mut Criterion) {
    let path = fixture("rar5", "rar5_lz.rar");
    let options = extract_options();
    c.bench_function("rar_non_solid_lz_chunked_extract", |b| {
        b.iter(|| {
            let mut archive =
                weaver_rar::RarArchive::open(std::fs::File::open(&path).unwrap()).unwrap();
            let provider = weaver_rar::StaticVolumeProvider::from_ordered(vec![path.clone()]);
            let chunks = archive
                .extract_member_streaming_chunked(0, &options, &provider, |_| {
                    Ok(Box::new(std::io::sink()))
                })
                .unwrap();
            black_box(chunks)
        });
    });
}

fn bench_solid_lz_chunked(c: &mut Criterion) {
    let path = fixture("rar5", "rar5_solid.rar");
    let options = extract_options();
    c.bench_function("rar_solid_lz_chunked_extract", |b| {
        b.iter(|| {
            let mut archive =
                weaver_rar::RarArchive::open(std::fs::File::open(&path).unwrap()).unwrap();
            let member_count = archive.metadata().members.len();
            for member_index in 0..member_count {
                let chunks = archive
                    .extract_member_solid_chunked(member_index, &options, |_| {
                        Ok(Box::new(std::io::sink()))
                    })
                    .unwrap();
                black_box(chunks);
            }
        });
    });
}

fn bench_rar4_solid_extract_all_members(c: &mut Criterion) {
    bench_solid_extract_all_members(
        c,
        "rar4",
        "rar4_solid.rar",
        "rar4_solid_extract_all_members",
    );
}

fn bench_rar5_solid_extract_all_members(c: &mut Criterion) {
    bench_solid_extract_all_members(
        c,
        "rar5",
        "rar5_solid.rar",
        "rar5_solid_extract_all_members",
    );
}

fn bench_rar4_solid_reopen_later_member(c: &mut Criterion) {
    bench_solid_reopen_later_member(
        c,
        "rar4",
        "rar4_solid.rar",
        "rar4_solid_reopen_later_member",
    );
}

fn bench_rar5_solid_reopen_later_member(c: &mut Criterion) {
    bench_solid_reopen_later_member(
        c,
        "rar5",
        "rar5_solid.rar",
        "rar5_solid_reopen_later_member",
    );
}

fn bench_archive_planner_view(c: &mut Criterion) {
    let path = fixture("rar5", "rar5_multifile_lz.rar");
    c.bench_function("rar_archive_planner_view", |b| {
        b.iter(|| {
            let archive =
                weaver_rar::RarArchive::open(std::fs::File::open(&path).unwrap()).unwrap();
            black_box(archive.metadata());
            black_box(archive.topology_members());
            black_box(archive.planner_member_states());
        });
    });
}

fn bench_filter_e8e9(c: &mut Criterion) {
    let mut seed = vec![0u8; 1024 * 1024];
    for i in (0..seed.len().saturating_sub(5)).step_by(64) {
        seed[i] = if (i / 64) % 2 == 0 { 0xE8 } else { 0xE9 };
        let addr = ((i as i32) + 0x1234).to_le_bytes();
        seed[i + 1..i + 5].copy_from_slice(&addr);
    }

    c.bench_function("rar_filter_e8e9", |b| {
        b.iter(|| {
            let mut data = seed.clone();
            weaver_rar::decompress::lz::filter::apply_e8e9(&mut data, 0);
            black_box(data);
        });
    });
}

fn bench_crc_hasher(c: &mut Criterion) {
    let data: Vec<u8> = (0..(8 * 1024 * 1024))
        .map(|i| (i as u8).wrapping_mul(31))
        .collect();

    c.bench_function("rar_crc32fast_baseline", |b| {
        b.iter(|| {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(black_box(&data));
            black_box(hasher.finalize());
        });
    });
}

criterion_group!(
    benches,
    bench_non_solid_lz_chunked,
    bench_solid_lz_chunked,
    bench_rar4_solid_extract_all_members,
    bench_rar5_solid_extract_all_members,
    bench_rar4_solid_reopen_later_member,
    bench_rar5_solid_reopen_later_member,
    bench_archive_planner_view,
    bench_filter_e8e9,
    bench_crc_hasher
);
criterion_main!(benches);
