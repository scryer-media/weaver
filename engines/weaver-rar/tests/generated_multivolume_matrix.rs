#![cfg(feature = "slow-tests")]

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use tempfile::TempDir;

const TEST_PASSWORD: &str = "testpass123";

#[derive(Clone, Copy)]
struct MatrixCase {
    fixture_dir: &'static str,
    prefix: &'static str,
    encrypted: bool,
}

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn original(name: &str) -> Vec<u8> {
    fs::read(fixture_root().join("originals").join(name)).unwrap()
}

fn collect_volume_paths(dir: &str, prefix: &str) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = fs::read_dir(fixture_root().join(dir))
        .unwrap()
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| {
            path.extension() == Some(OsStr::new("rar"))
                && path
                    .file_name()
                    .and_then(OsStr::to_str)
                    .is_some_and(|name| name.starts_with(prefix))
        })
        .collect();
    paths.sort();
    assert!(paths.len() > 1, "expected multivolume archive for {prefix}");
    paths
}

fn open_archive(paths: &[PathBuf]) -> weaver_rar::RarArchive {
    let readers: Vec<Box<dyn weaver_rar::ReadSeek>> = paths
        .iter()
        .map(|path| Box::new(File::open(path).unwrap()) as Box<dyn weaver_rar::ReadSeek>)
        .collect();
    weaver_rar::RarArchive::open_volumes(readers).unwrap()
}

fn extract_chunked(paths: &[PathBuf], password: Option<&str>, chunk_dir: &Path) -> Vec<u8> {
    let mut archive = open_archive(paths);
    if let Some(password) = password {
        archive.set_password(password);
    }

    fs::create_dir_all(chunk_dir).unwrap();
    let provider = weaver_rar::StaticVolumeProvider::from_ordered(paths.to_vec());
    let opts = weaver_rar::ExtractOptions {
        verify: true,
        password: password.map(str::to_owned),
    };
    let mut chunk_paths = BTreeMap::new();
    let chunks = archive
        .extract_member_streaming_chunked(0, &opts, &provider, |volume_index| {
            let path = chunk_dir.join(format!("vol{volume_index:03}.chunk"));
            chunk_paths.insert(volume_index, path.clone());
            File::create(path)
                .map(|file| Box::new(file) as Box<dyn Write>)
                .map_err(weaver_rar::RarError::Io)
        })
        .unwrap();

    let mut reassembled = Vec::new();
    for (volume_index, bytes_written) in chunks {
        let path = chunk_paths.get(&volume_index).unwrap();
        let data = fs::read(path).unwrap();
        assert_eq!(
            data.len() as u64,
            bytes_written,
            "chunk size mismatch for volume {volume_index}"
        );
        reassembled.extend_from_slice(&data);
    }
    reassembled
}

#[test]
fn generated_multivolume_single_member_matrix_extracts() {
    let expected = original("generated_matrix_clip.mkv");
    let cases = [
        MatrixCase {
            fixture_dir: "rar5",
            prefix: "generated_matrix_rar5_store_plain",
            encrypted: false,
        },
        MatrixCase {
            fixture_dir: "rar5",
            prefix: "generated_matrix_rar5_store_enc",
            encrypted: true,
        },
        MatrixCase {
            fixture_dir: "rar5",
            prefix: "generated_matrix_rar5_lz_plain",
            encrypted: false,
        },
        MatrixCase {
            fixture_dir: "rar5",
            prefix: "generated_matrix_rar5_lz_enc",
            encrypted: true,
        },
        MatrixCase {
            fixture_dir: "rar4",
            prefix: "generated_matrix_rar4_store_plain",
            encrypted: false,
        },
        MatrixCase {
            fixture_dir: "rar4",
            prefix: "generated_matrix_rar4_store_enc",
            encrypted: true,
        },
        MatrixCase {
            fixture_dir: "rar4",
            prefix: "generated_matrix_rar4_lz_plain",
            encrypted: false,
        },
        MatrixCase {
            fixture_dir: "rar4",
            prefix: "generated_matrix_rar4_lz_enc",
            encrypted: true,
        },
    ];

    for case in cases {
        let paths = collect_volume_paths(case.fixture_dir, case.prefix);
        let password = case.encrypted.then_some(TEST_PASSWORD);

        let archive = open_archive(&paths);
        let names = archive.member_names();
        assert_eq!(names.len(), 1, "{}: expected single member", case.prefix);
        let info = archive.member_info(0).unwrap();
        assert_eq!(
            info.unpacked_size,
            Some(expected.len() as u64),
            "{}: unexpected unpacked size",
            case.prefix
        );
        assert!(
            info.volumes.is_split(),
            "{}: archive should span multiple volumes",
            case.prefix
        );

        let mut batch_archive = open_archive(&paths);
        if let Some(password) = password {
            batch_archive.set_password(password);
        }
        let opts = weaver_rar::ExtractOptions {
            verify: true,
            password: password.map(str::to_owned),
        };
        let batch = batch_archive.extract_member(0, &opts, None).unwrap();
        assert_eq!(batch, expected, "{}: batch extract mismatch", case.prefix);

        let mut streaming_archive = open_archive(&paths);
        if let Some(password) = password {
            streaming_archive.set_password(password);
        }
        let provider = weaver_rar::StaticVolumeProvider::from_ordered(paths.clone());
        let mut streaming = Vec::new();
        streaming_archive
            .extract_member_streaming(0, &opts, &provider, &mut streaming)
            .unwrap();
        assert_eq!(
            streaming, expected,
            "{}: streaming extract mismatch",
            case.prefix
        );

        let temp = TempDir::new().unwrap();
        let chunked = extract_chunked(&paths, password, temp.path());
        assert_eq!(
            chunked, expected,
            "{}: chunked extract mismatch",
            case.prefix
        );
    }
}
