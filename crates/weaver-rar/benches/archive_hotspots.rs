use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn fixture(dir: &str, name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(dir)
        .join(name)
}

fn bench_non_solid_lz_chunked(c: &mut Criterion) {
    let path = fixture("rar5", "rar5_lz.rar");
    let options = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
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
    let options = weaver_rar::ExtractOptions {
        verify: true,
        password: None,
    };
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

criterion_group!(
    benches,
    bench_non_solid_lz_chunked,
    bench_solid_lz_chunked,
    bench_archive_planner_view
);
criterion_main!(benches);
