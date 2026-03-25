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
                let data = archive.extract_member(member_index, &options, None).unwrap();
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

criterion_group!(
    benches,
    bench_non_solid_lz_chunked,
    bench_solid_lz_chunked,
    bench_rar4_solid_extract_all_members,
    bench_rar5_solid_extract_all_members,
    bench_rar4_solid_reopen_later_member,
    bench_rar5_solid_reopen_later_member,
    bench_archive_planner_view
);
criterion_main!(benches);
