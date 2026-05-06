use std::env;
use std::fs;
use std::path::{Path, PathBuf};

#[path = "src/migration_assets.rs"]
mod migration_assets;
#[path = "src/migration_hook_ids.rs"]
mod migration_hook_ids;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/migration_assets.rs");
    println!("cargo:rerun-if-changed=src/migration_hook_ids.rs");

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    compile_migration_bundle(&manifest_dir);
}

fn compile_migration_bundle(manifest_dir: &Path) {
    let db_root = manifest_dir.join("src/db");
    watch_tree(&db_root);

    let bundle = migration_assets::compile_source_bundle(&db_root)
        .unwrap_or_else(|error| panic!("failed to compile migration bundle: {error}"));
    let catalog_bytes = migration_assets::encode_catalog(&bundle.catalog)
        .unwrap_or_else(|error| panic!("failed to encode migration catalog: {error}"));
    let compressed_catalog =
        zstd::stream::encode_all(catalog_bytes.as_slice(), zstd::zstd_safe::max_c_level())
            .unwrap_or_else(|error| panic!("failed to compress migration catalog: {error}"));
    let compressed_payload = zstd::stream::encode_all(
        bundle.payload_bytes.as_slice(),
        zstd::zstd_safe::max_c_level(),
    )
    .unwrap_or_else(|error| panic!("failed to compress migration payload: {error}"));

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));
    let catalog_path = out_dir.join("migration_catalog.json.zst");
    fs::write(&catalog_path, compressed_catalog)
        .unwrap_or_else(|error| panic!("failed to write {}: {error}", catalog_path.display()));
    let payload_path = out_dir.join("migration_payload.bin.zst");
    fs::write(&payload_path, compressed_payload)
        .unwrap_or_else(|error| panic!("failed to write {}: {error}", payload_path.display()));
}

fn watch_tree(root: &Path) {
    if !root.exists() {
        return;
    }

    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        println!("cargo:rerun-if-changed={}", dir.display());
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            println!("cargo:rerun-if-changed={}", path.display());
            if path.is_dir() {
                stack.push(path);
            }
        }
    }
}
