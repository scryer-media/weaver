#![cfg(feature = "slow-tests")]

use std::ffi::OsStr;
use std::fs;
use std::path::PathBuf;

use tempfile::TempDir;

use weaver_model::files::FileRole;
use weaver_server_core::pipeline::archive::split_reader::SplitFileReader;

const TEST_PASSWORD: &str = "TestPass123";

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/sevenz")
}

fn original_bytes() -> Vec<u8> {
    fs::read(fixture_root().join("originals/generated_split_clip.mkv")).unwrap()
}

fn collect_split_paths(prefix: &str) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = fs::read_dir(fixture_root())
        .unwrap()
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .is_some_and(|name| {
                    name.starts_with(prefix)
                        && name.len() > prefix.len() + 1
                        && name[prefix.len() + 1..].parse::<u32>().is_ok()
                })
        })
        .collect();
    paths.sort();
    assert!(paths.len() > 1, "expected split archive for {prefix}");
    paths
}

fn extract_split(paths: &[PathBuf], password: Option<&str>) -> Vec<u8> {
    let out_dir = TempDir::new().unwrap();
    let reader = SplitFileReader::open(paths).unwrap();
    let password = password.map_or_else(sevenz_rust2::Password::empty, sevenz_rust2::Password::new);
    sevenz_rust2::decompress_with_password(reader, out_dir.path(), password).unwrap();
    fs::read(out_dir.path().join("generated_split_clip.mkv")).unwrap()
}

#[test]
fn fixture_splitfile_matrix_extracts() {
    let expected = original_bytes();
    let cases = [
        ("generated_split_store_plain.7z", None),
        ("generated_split_lzma2_plain.7z", None),
        ("generated_split_store_enc.7z", Some(TEST_PASSWORD)),
        ("generated_split_lzma2_enc.7z", Some(TEST_PASSWORD)),
    ];

    for (prefix, password) in cases {
        let paths = collect_split_paths(prefix);
        for (i, path) in paths.iter().enumerate() {
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            assert_eq!(
                FileRole::from_filename(&name),
                FileRole::SevenZipSplit { number: i as u32 },
                "wrong classification for {name}"
            );
        }

        let extracted = extract_split(&paths, password);
        assert_eq!(extracted, expected, "{prefix}: extracted bytes mismatch");
    }
}

#[test]
fn fixture_splitfile_wrong_password_fails() {
    let out_dir = TempDir::new().unwrap();
    let paths = collect_split_paths("generated_split_store_enc.7z");
    let reader = SplitFileReader::open(&paths).unwrap();
    let result = sevenz_rust2::decompress_with_password(
        reader,
        out_dir.path(),
        sevenz_rust2::Password::new("WrongPassword"),
    );
    assert!(result.is_err(), "expected wrong-password failure");
}
