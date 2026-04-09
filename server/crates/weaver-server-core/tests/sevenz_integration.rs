//! Integration tests for 7z archive support.
//!
//! These tests create real 7z archives using the system `7z` command,
//! then verify that our classification, split reader, and sevenz-rust2
//! extraction all work correctly end-to-end.

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

use weaver_model::files::FileRole;
use weaver_server_core::pipeline::archive::split_reader::SplitFileReader;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a temp dir with test files of known content.
fn create_test_files(dir: &Path) -> HashMap<String, Vec<u8>> {
    let mut files = HashMap::new();

    // Small text file.
    let hello = b"Hello from weaver 7z tests!\n".to_vec();
    fs::write(dir.join("hello.txt"), &hello).unwrap();
    files.insert("hello.txt".into(), hello);

    // Binary file with all byte values.
    let binary: Vec<u8> = (0..=255).cycle().take(4096).collect();
    fs::write(dir.join("binary.bin"), &binary).unwrap();
    files.insert("binary.bin".into(), binary);

    // Larger file for split testing (~64KB).
    let large: Vec<u8> = (0..65536u32).map(|i| (i % 251) as u8).collect();
    fs::write(dir.join("large.dat"), &large).unwrap();
    files.insert("large.dat".into(), large);

    files
}

/// Run 7z with args, panic on failure.
fn run_7z(args: &[&str]) {
    let output = Command::new("7z")
        .args(args)
        .output()
        .expect("failed to run 7z — is p7zip installed?");
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        panic!("7z failed:\nstdout: {stdout}\nstderr: {stderr}");
    }
}

/// Extract a 7z archive using sevenz-rust2 and return (name -> content) map.
fn extract_with_sevenz(
    source: impl std::io::Read + std::io::Seek,
    dest: &Path,
) -> HashMap<String, Vec<u8>> {
    sevenz_rust2::decompress_with_password(source, dest, sevenz_rust2::Password::empty()).unwrap();
    read_dir_contents(dest)
}

/// Read all files in a directory recursively into a map.
fn read_dir_contents(dir: &Path) -> HashMap<String, Vec<u8>> {
    let mut result = HashMap::new();
    read_dir_recursive(dir, dir, &mut result);
    result
}

fn read_dir_recursive(base: &Path, dir: &Path, out: &mut HashMap<String, Vec<u8>>) {
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            read_dir_recursive(base, &path, out);
        } else {
            let rel = path
                .strip_prefix(base)
                .unwrap()
                .to_string_lossy()
                .to_string();
            out.insert(rel, fs::read(&path).unwrap());
        }
    }
}

// ---------------------------------------------------------------------------
// Classification tests
// ---------------------------------------------------------------------------

#[test]
fn classify_7z_filenames() {
    // Single archives.
    assert_eq!(
        FileRole::from_filename("movie.7z"),
        FileRole::SevenZipArchive
    );
    assert_eq!(
        FileRole::from_filename("ARCHIVE.7Z"),
        FileRole::SevenZipArchive
    );
    assert_eq!(
        FileRole::from_filename("My.File.Name.7z"),
        FileRole::SevenZipArchive
    );

    // Split archives.
    assert_eq!(
        FileRole::from_filename("movie.7z.001"),
        FileRole::SevenZipSplit { number: 0 }
    );
    assert_eq!(
        FileRole::from_filename("movie.7z.005"),
        FileRole::SevenZipSplit { number: 4 }
    );
    assert_eq!(
        FileRole::from_filename("movie.7z.100"),
        FileRole::SevenZipSplit { number: 99 }
    );

    // Priority ordering.
    let sz = FileRole::SevenZipArchive;
    let split1 = FileRole::SevenZipSplit { number: 0 };
    let split5 = FileRole::SevenZipSplit { number: 4 };
    let par2_idx = FileRole::Par2 {
        is_index: true,
        recovery_block_count: 0,
    };
    let par2_rec = FileRole::Par2 {
        is_index: false,
        recovery_block_count: 5,
    };

    assert!(par2_idx.download_priority() < sz.download_priority());
    assert!(sz.download_priority() < split1.download_priority());
    assert!(split1.download_priority() < split5.download_priority());
    assert!(split5.download_priority() < par2_rec.download_priority());

    // Not recovery files.
    assert!(!sz.is_recovery());
    assert!(!split1.is_recovery());
}

// ---------------------------------------------------------------------------
// Single 7z archive extraction
// ---------------------------------------------------------------------------

#[test]
fn extract_single_7z_copy_method() {
    let tmp = TempDir::new().unwrap();
    let src_dir = tmp.path().join("src");
    let archive_path = tmp.path().join("test.7z");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&src_dir).unwrap();
    fs::create_dir_all(&out_dir).unwrap();

    let expected = create_test_files(&src_dir);

    // Create archive with copy (store) method.
    run_7z(&[
        "a",
        "-mx0",
        archive_path.to_str().unwrap(),
        &format!("{}/*", src_dir.to_str().unwrap()),
    ]);

    assert!(archive_path.exists());

    let file = fs::File::open(&archive_path).unwrap();
    let extracted = extract_with_sevenz(file, &out_dir);

    for (name, content) in &expected {
        let got = extracted
            .get(name)
            .unwrap_or_else(|| panic!("missing {name}"));
        assert_eq!(got, content, "content mismatch for {name}");
    }
}

#[test]
fn extract_single_7z_lzma2() {
    let tmp = TempDir::new().unwrap();
    let src_dir = tmp.path().join("src");
    let archive_path = tmp.path().join("test.7z");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&src_dir).unwrap();
    fs::create_dir_all(&out_dir).unwrap();

    let expected = create_test_files(&src_dir);

    // Create archive with LZMA2 compression.
    run_7z(&[
        "a",
        "-mx9",
        archive_path.to_str().unwrap(),
        &format!("{}/*", src_dir.to_str().unwrap()),
    ]);

    let file = fs::File::open(&archive_path).unwrap();
    let extracted = extract_with_sevenz(file, &out_dir);

    for (name, content) in &expected {
        let got = extracted
            .get(name)
            .unwrap_or_else(|| panic!("missing {name}"));
        assert_eq!(got, content, "content mismatch for {name}");
    }
}

#[test]
fn extract_single_7z_solid() {
    let tmp = TempDir::new().unwrap();
    let src_dir = tmp.path().join("src");
    let archive_path = tmp.path().join("test.7z");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&src_dir).unwrap();
    fs::create_dir_all(&out_dir).unwrap();

    let expected = create_test_files(&src_dir);

    // Create solid archive.
    run_7z(&[
        "a",
        "-ms=on",
        archive_path.to_str().unwrap(),
        &format!("{}/*", src_dir.to_str().unwrap()),
    ]);

    let file = fs::File::open(&archive_path).unwrap();
    let extracted = extract_with_sevenz(file, &out_dir);

    for (name, content) in &expected {
        let got = extracted
            .get(name)
            .unwrap_or_else(|| panic!("missing {name}"));
        assert_eq!(got, content, "content mismatch for {name}");
    }
}

// ---------------------------------------------------------------------------
// Split 7z archive extraction
// ---------------------------------------------------------------------------

#[test]
fn extract_split_7z() {
    let tmp = TempDir::new().unwrap();
    let src_dir = tmp.path().join("src");
    let archive_base = tmp.path().join("split.7z");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&src_dir).unwrap();
    fs::create_dir_all(&out_dir).unwrap();

    let expected = create_test_files(&src_dir);

    // Create split archive — volumes of ~10KB each.
    run_7z(&[
        "a",
        "-mx0",
        "-v10k",
        archive_base.to_str().unwrap(),
        &format!("{}/*", src_dir.to_str().unwrap()),
    ]);

    // Find split files.
    let mut split_files: Vec<PathBuf> = fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            let name = p.file_name().unwrap().to_string_lossy().to_string();
            if name.starts_with("split.7z.") && name[9..].parse::<u32>().is_ok() {
                Some(p)
            } else {
                None
            }
        })
        .collect();
    split_files.sort();

    assert!(
        split_files.len() >= 2,
        "expected at least 2 split volumes, got {}",
        split_files.len()
    );

    // Verify classification of split files.
    for (i, path) in split_files.iter().enumerate() {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        let role = FileRole::from_filename(&name);
        assert_eq!(
            role,
            FileRole::SevenZipSplit { number: i as u32 },
            "wrong classification for {name}"
        );
    }

    // Open via SplitFileReader and extract.
    let reader = SplitFileReader::open(&split_files).unwrap();
    let extracted = extract_with_sevenz(reader, &out_dir);

    for (name, content) in &expected {
        let got = extracted
            .get(name)
            .unwrap_or_else(|| panic!("missing {name}"));
        assert_eq!(got, content, "content mismatch for {name}");
    }
}

#[test]
fn extract_split_7z_compressed() {
    let tmp = TempDir::new().unwrap();
    let src_dir = tmp.path().join("src");
    let archive_base = tmp.path().join("split.7z");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&src_dir).unwrap();
    fs::create_dir_all(&out_dir).unwrap();

    let expected = create_test_files(&src_dir);

    // Create compressed split archive.
    run_7z(&[
        "a",
        "-mx5",
        "-v10k",
        archive_base.to_str().unwrap(),
        &format!("{}/*", src_dir.to_str().unwrap()),
    ]);

    let mut split_files: Vec<PathBuf> = fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            let name = p.file_name().unwrap().to_string_lossy().to_string();
            if name.starts_with("split.7z.") && name[9..].parse::<u32>().is_ok() {
                Some(p)
            } else {
                None
            }
        })
        .collect();
    split_files.sort();

    let reader = SplitFileReader::open(&split_files).unwrap();
    let extracted = extract_with_sevenz(reader, &out_dir);

    for (name, content) in &expected {
        let got = extracted
            .get(name)
            .unwrap_or_else(|| panic!("missing {name}"));
        assert_eq!(got, content, "content mismatch for {name}");
    }
}

// ---------------------------------------------------------------------------
// SplitFileReader edge cases with real archives
// ---------------------------------------------------------------------------

#[test]
fn split_reader_seek_consistency() {
    let tmp = TempDir::new().unwrap();
    let src_dir = tmp.path().join("src");
    let archive_base = tmp.path().join("seek.7z");
    fs::create_dir_all(&src_dir).unwrap();

    // Create a file large enough to split.
    let data: Vec<u8> = (0..32768u32).map(|i| (i % 251) as u8).collect();
    fs::write(src_dir.join("data.bin"), &data).unwrap();

    run_7z(&[
        "a",
        "-mx0",
        "-v8k",
        archive_base.to_str().unwrap(),
        &format!("{}/*", src_dir.to_str().unwrap()),
    ]);

    let mut split_files: Vec<PathBuf> = fs::read_dir(tmp.path())
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            let name = p.file_name().unwrap().to_string_lossy().to_string();
            if name.starts_with("seek.7z.") && name[8..].parse::<u32>().is_ok() {
                Some(p)
            } else {
                None
            }
        })
        .collect();
    split_files.sort();

    let mut reader = SplitFileReader::open(&split_files).unwrap();

    // Read the whole thing.
    let mut all = Vec::new();
    reader.read_to_end(&mut all).unwrap();

    // Seek back and read again — should be identical.
    reader.seek(SeekFrom::Start(0)).unwrap();
    let mut all2 = Vec::new();
    reader.read_to_end(&mut all2).unwrap();
    assert_eq!(all, all2, "re-read after seek(0) should match");

    // Seek to middle and read.
    let mid = all.len() / 2;
    reader.seek(SeekFrom::Start(mid as u64)).unwrap();
    let mut tail = Vec::new();
    reader.read_to_end(&mut tail).unwrap();
    assert_eq!(tail, &all[mid..], "reading from midpoint should match");

    // Seek from end.
    reader.seek(SeekFrom::End(-100)).unwrap();
    let mut last100 = vec![0u8; 100];
    reader.read_exact(&mut last100).unwrap();
    assert_eq!(
        last100,
        &all[all.len() - 100..],
        "last 100 bytes should match"
    );
}

// ---------------------------------------------------------------------------
// Password-protected archives
// ---------------------------------------------------------------------------

#[test]
fn extract_encrypted_7z() {
    let tmp = TempDir::new().unwrap();
    let src_dir = tmp.path().join("src");
    let archive_path = tmp.path().join("encrypted.7z");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&src_dir).unwrap();
    fs::create_dir_all(&out_dir).unwrap();

    let content = b"secret data for encryption test\n";
    fs::write(src_dir.join("secret.txt"), content).unwrap();

    run_7z(&[
        "a",
        "-mx0",
        "-pTestPass123",
        archive_path.to_str().unwrap(),
        &format!("{}/*", src_dir.to_str().unwrap()),
    ]);

    let file = fs::File::open(&archive_path).unwrap();
    sevenz_rust2::decompress_with_password(
        file,
        &out_dir,
        sevenz_rust2::Password::new("TestPass123"),
    )
    .unwrap();

    let extracted = fs::read(out_dir.join("secret.txt")).unwrap();
    assert_eq!(extracted, content);
}

#[test]
fn extract_encrypted_7z_wrong_password_fails() {
    let tmp = TempDir::new().unwrap();
    let src_dir = tmp.path().join("src");
    let archive_path = tmp.path().join("encrypted.7z");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&src_dir).unwrap();
    fs::create_dir_all(&out_dir).unwrap();

    fs::write(src_dir.join("secret.txt"), b"secret data").unwrap();

    run_7z(&[
        "a",
        "-mx0",
        "-pCorrectPassword",
        archive_path.to_str().unwrap(),
        &format!("{}/*", src_dir.to_str().unwrap()),
    ]);

    let file = fs::File::open(&archive_path).unwrap();
    let result = sevenz_rust2::decompress_with_password(
        file,
        &out_dir,
        sevenz_rust2::Password::new("WrongPassword"),
    );
    assert!(result.is_err(), "should fail with wrong password");
}

// ---------------------------------------------------------------------------
// Subdirectory structure preservation
// ---------------------------------------------------------------------------

#[test]
fn extract_preserves_directory_structure() {
    let tmp = TempDir::new().unwrap();
    let src_dir = tmp.path().join("src");
    let archive_path = tmp.path().join("dirs.7z");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(src_dir.join("sub/nested")).unwrap();
    fs::create_dir_all(&out_dir).unwrap();

    fs::write(src_dir.join("root.txt"), b"root file").unwrap();
    fs::write(src_dir.join("sub/middle.txt"), b"middle file").unwrap();
    fs::write(src_dir.join("sub/nested/deep.txt"), b"deep file").unwrap();

    run_7z(&[
        "a",
        "-mx0",
        archive_path.to_str().unwrap(),
        &format!("{}/*", src_dir.to_str().unwrap()),
        "-r",
    ]);

    let file = fs::File::open(&archive_path).unwrap();
    extract_with_sevenz(file, &out_dir);

    assert_eq!(fs::read(out_dir.join("root.txt")).unwrap(), b"root file");
    assert_eq!(
        fs::read(out_dir.join("sub/middle.txt")).unwrap(),
        b"middle file"
    );
    assert_eq!(
        fs::read(out_dir.join("sub/nested/deep.txt")).unwrap(),
        b"deep file"
    );
}

// ---------------------------------------------------------------------------
// Empty archive
// ---------------------------------------------------------------------------

#[test]
fn extract_empty_archive() {
    let tmp = TempDir::new().unwrap();
    let src_dir = tmp.path().join("src");
    let archive_path = tmp.path().join("empty.7z");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&src_dir).unwrap();
    fs::create_dir_all(&out_dir).unwrap();

    // Create an empty directory and archive it.
    run_7z(&[
        "a",
        archive_path.to_str().unwrap(),
        &format!("{}/*", src_dir.to_str().unwrap()),
    ]);

    // If 7z refuses to create an empty archive, that's OK.
    // Also, if the archive exists but is empty/invalid, sevenz-rust2 may fail to parse it.
    if archive_path.exists() {
        let file = fs::File::open(&archive_path).unwrap();
        match sevenz_rust2::decompress_with_password(
            file,
            &out_dir,
            sevenz_rust2::Password::empty(),
        ) {
            Ok(()) => {
                let extracted = read_dir_contents(&out_dir);
                assert!(extracted.is_empty() || extracted.values().all(|v| v.is_empty()));
            }
            Err(_) => {
                // Empty or malformed archive — acceptable.
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Pipeline extraction callback pattern (mirrors completion.rs)
// ---------------------------------------------------------------------------

#[test]
fn extract_with_callback_pattern() {
    let tmp = TempDir::new().unwrap();
    let src_dir = tmp.path().join("src");
    let archive_path = tmp.path().join("callback.7z");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&src_dir).unwrap();
    fs::create_dir_all(&out_dir).unwrap();

    let expected = create_test_files(&src_dir);

    run_7z(&[
        "a",
        "-mx5",
        archive_path.to_str().unwrap(),
        &format!("{}/*", src_dir.to_str().unwrap()),
    ]);

    // Use the same callback pattern as our pipeline.
    let file = fs::File::open(&archive_path).unwrap();
    let mut extracted_count = 0u32;
    let out = out_dir.clone();

    sevenz_rust2::decompress_with_extract_fn_and_password(
        file,
        &out_dir,
        sevenz_rust2::Password::empty(),
        |entry: &sevenz_rust2::ArchiveEntry, reader: &mut dyn Read, _dest: &PathBuf| {
            if entry.is_directory() {
                fs::create_dir_all(out.join(entry.name()))?;
                return Ok(true);
            }

            let out_path = out.join(entry.name());
            if let Some(parent) = out_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut file = fs::File::create(&out_path)?;
            std::io::copy(reader, &mut file)?;
            extracted_count += 1;
            Ok(true)
        },
    )
    .unwrap();

    assert_eq!(extracted_count, expected.len() as u32);

    for (name, content) in &expected {
        let got = fs::read(out_dir.join(name)).unwrap();
        assert_eq!(&got, content, "content mismatch for {name}");
    }
}

// ---------------------------------------------------------------------------
// Assembly topology tests
// ---------------------------------------------------------------------------

#[test]
fn topology_single_7z() {
    use weaver_server_core::jobs::ids::{JobId, NzbFileId};

    let mut job = weaver_server_core::jobs::assembly::JobAssembly::new(JobId(1));

    let fa = weaver_server_core::jobs::assembly::FileAssembly::new(
        NzbFileId {
            job_id: JobId(1),
            file_index: 0,
        },
        "archive.7z".into(),
        FileRole::SevenZipArchive,
        vec![10000],
    );
    job.add_file(fa);

    // Before topology is set, readiness should be Blocked.
    assert!(matches!(
        job.extraction_readiness(),
        weaver_server_core::jobs::assembly::ExtractionReadiness::Blocked { .. }
    ));

    // Set topology for single 7z.
    let topo = weaver_server_core::jobs::assembly::ArchiveTopology {
        archive_type: weaver_server_core::jobs::assembly::ArchiveType::SevenZip,
        volume_map: [("archive.7z".into(), 0)].into_iter().collect(),
        complete_volumes: [0].into_iter().collect(),
        expected_volume_count: Some(1),
        members: vec![weaver_server_core::jobs::assembly::ArchiveMember {
            name: "archive".into(),
            first_volume: 0,
            last_volume: 0,
            unpacked_size: 0,
        }],
        unresolved_spans: Vec::new(),
    };
    job.set_archive_topology("test".into(), topo);

    assert_eq!(
        job.extraction_readiness(),
        weaver_server_core::jobs::assembly::ExtractionReadiness::Ready
    );
}

#[test]
fn topology_split_7z() {
    use std::collections::HashSet;
    use weaver_server_core::jobs::ids::{JobId, NzbFileId};

    let mut job = weaver_server_core::jobs::assembly::JobAssembly::new(JobId(1));

    for i in 0..3 {
        let fa = weaver_server_core::jobs::assembly::FileAssembly::new(
            NzbFileId {
                job_id: JobId(1),
                file_index: i,
            },
            format!("archive.7z.{:03}", i + 1),
            FileRole::SevenZipSplit { number: i },
            vec![5000],
        );
        job.add_file(fa);
    }

    // No topology yet.
    assert!(matches!(
        job.extraction_readiness(),
        weaver_server_core::jobs::assembly::ExtractionReadiness::Blocked { .. }
    ));

    // Set topology for 3-part split.
    let topo = weaver_server_core::jobs::assembly::ArchiveTopology {
        archive_type: weaver_server_core::jobs::assembly::ArchiveType::SevenZip,
        volume_map: [
            ("archive.7z.001".into(), 0),
            ("archive.7z.002".into(), 1),
            ("archive.7z.003".into(), 2),
        ]
        .into_iter()
        .collect(),
        complete_volumes: HashSet::new(),
        expected_volume_count: Some(3),
        members: vec![weaver_server_core::jobs::assembly::ArchiveMember {
            name: "archive".into(),
            first_volume: 0,
            last_volume: 2,
            unpacked_size: 0,
        }],
        unresolved_spans: Vec::new(),
    };
    job.set_archive_topology("test".into(), topo);

    // Still blocked — no volumes complete.
    assert!(matches!(
        job.extraction_readiness(),
        weaver_server_core::jobs::assembly::ExtractionReadiness::Blocked { .. }
    ));

    // Complete volumes one by one.
    job.mark_volume_complete("test", 0);
    assert!(!matches!(
        job.extraction_readiness(),
        weaver_server_core::jobs::assembly::ExtractionReadiness::Ready
    ));

    job.mark_volume_complete("test", 1);
    assert!(!matches!(
        job.extraction_readiness(),
        weaver_server_core::jobs::assembly::ExtractionReadiness::Ready
    ));

    job.mark_volume_complete("test", 2);
    assert_eq!(
        job.extraction_readiness(),
        weaver_server_core::jobs::assembly::ExtractionReadiness::Ready
    );
}

// ---------------------------------------------------------------------------
// Multi-set archive tests
// ---------------------------------------------------------------------------

#[test]
fn multi_set_base_name_grouping() {
    use weaver_model::files::archive_base_name;

    // Different episodes produce different base names.
    let e01 = archive_base_name("Show.S01E01.7z.001", &FileRole::SevenZipSplit { number: 0 });
    let e02 = archive_base_name("Show.S01E02.7z.001", &FileRole::SevenZipSplit { number: 0 });
    assert_eq!(e01, Some("Show.S01E01.7z".into()));
    assert_eq!(e02, Some("Show.S01E02.7z".into()));
    assert_ne!(e01, e02);

    // Same episode, different parts, produce the same base name.
    let e01_p1 = archive_base_name("Show.S01E01.7z.001", &FileRole::SevenZipSplit { number: 0 });
    let e01_p2 = archive_base_name("Show.S01E01.7z.002", &FileRole::SevenZipSplit { number: 1 });
    assert_eq!(e01_p1, e01_p2);
}

#[test]
fn multi_set_readiness_partial() {
    use std::collections::HashSet;
    use weaver_server_core::jobs::ids::{JobId, NzbFileId};

    let mut job = weaver_server_core::jobs::assembly::JobAssembly::new(JobId(1));

    // Add files for two independent sets: ep01 (2 parts) and ep02 (2 parts).
    for i in 0..2 {
        let fa = weaver_server_core::jobs::assembly::FileAssembly::new(
            NzbFileId {
                job_id: JobId(1),
                file_index: i,
            },
            format!("ep01.7z.{:03}", i + 1),
            FileRole::SevenZipSplit { number: i },
            vec![5000],
        );
        job.add_file(fa);
    }
    for i in 0..2 {
        let fa = weaver_server_core::jobs::assembly::FileAssembly::new(
            NzbFileId {
                job_id: JobId(1),
                file_index: 10 + i,
            },
            format!("ep02.7z.{:03}", i + 1),
            FileRole::SevenZipSplit { number: i },
            vec![5000],
        );
        job.add_file(fa);
    }

    // Set topologies for both sets.
    let topo1 = weaver_server_core::jobs::assembly::ArchiveTopology {
        archive_type: weaver_server_core::jobs::assembly::ArchiveType::SevenZip,
        volume_map: [("ep01.7z.001".into(), 0), ("ep01.7z.002".into(), 1)]
            .into_iter()
            .collect(),
        complete_volumes: [0, 1].into_iter().collect(), // ep01 is complete
        expected_volume_count: Some(2),
        members: vec![weaver_server_core::jobs::assembly::ArchiveMember {
            name: "ep01.7z".into(),
            first_volume: 0,
            last_volume: 1,
            unpacked_size: 0,
        }],
        unresolved_spans: Vec::new(),
    };
    let topo2 = weaver_server_core::jobs::assembly::ArchiveTopology {
        archive_type: weaver_server_core::jobs::assembly::ArchiveType::SevenZip,
        volume_map: [("ep02.7z.001".into(), 0), ("ep02.7z.002".into(), 1)]
            .into_iter()
            .collect(),
        complete_volumes: HashSet::new(), // ep02 is NOT complete
        expected_volume_count: Some(2),
        members: vec![weaver_server_core::jobs::assembly::ArchiveMember {
            name: "ep02.7z".into(),
            first_volume: 0,
            last_volume: 1,
            unpacked_size: 0,
        }],
        unresolved_spans: Vec::new(),
    };

    job.set_archive_topology("ep01.7z".into(), topo1);
    job.set_archive_topology("ep02.7z".into(), topo2);

    // Aggregate readiness should NOT be Ready (ep02 is incomplete).
    assert!(!matches!(
        job.extraction_readiness(),
        weaver_server_core::jobs::assembly::ExtractionReadiness::Ready
    ));

    // But ready_archive_sets should return ep01.
    let ready = job.ready_archive_sets();
    assert_eq!(ready.len(), 1);
    assert!(ready.contains(&"ep01.7z".to_string()));

    // Per-set readiness.
    assert_eq!(
        job.set_extraction_readiness("ep01.7z"),
        weaver_server_core::jobs::assembly::ExtractionReadiness::Ready
    );
    assert!(matches!(
        job.set_extraction_readiness("ep02.7z"),
        weaver_server_core::jobs::assembly::ExtractionReadiness::Blocked { .. }
    ));

    // Complete ep02 — now aggregate should be Ready.
    job.mark_volume_complete("ep02.7z", 0);
    job.mark_volume_complete("ep02.7z", 1);
    assert_eq!(
        job.extraction_readiness(),
        weaver_server_core::jobs::assembly::ExtractionReadiness::Ready
    );
}

#[test]
fn multi_set_extraction() {
    // Create two independent 7z archives, extract both from the same "job".
    let tmp = TempDir::new().unwrap();

    // Create source files for ep01 and ep02.
    let src1 = tmp.path().join("src1");
    let src2 = tmp.path().join("src2");
    fs::create_dir_all(&src1).unwrap();
    fs::create_dir_all(&src2).unwrap();

    let ep01_content = b"Episode 01 content data here\n";
    let ep02_content = b"Episode 02 different content\n";
    fs::write(src1.join("ep01.txt"), ep01_content).unwrap();
    fs::write(src2.join("ep02.txt"), ep02_content).unwrap();

    // Create two separate 7z archives.
    let archive1 = tmp.path().join("ep01.7z");
    let archive2 = tmp.path().join("ep02.7z");

    run_7z(&[
        "a",
        "-mx0",
        archive1.to_str().unwrap(),
        &format!("{}/*", src1.to_str().unwrap()),
    ]);
    run_7z(&[
        "a",
        "-mx0",
        archive2.to_str().unwrap(),
        &format!("{}/*", src2.to_str().unwrap()),
    ]);

    // Extract both independently to the same output directory.
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&out_dir).unwrap();

    // Extract ep01.
    let file1 = fs::File::open(&archive1).unwrap();
    extract_with_sevenz(file1, &out_dir);
    assert_eq!(fs::read(out_dir.join("ep01.txt")).unwrap(), ep01_content);

    // Extract ep02 to same dir — should not conflict.
    let file2 = fs::File::open(&archive2).unwrap();
    extract_with_sevenz(file2, &out_dir);
    assert_eq!(fs::read(out_dir.join("ep02.txt")).unwrap(), ep02_content);

    // Both files should exist.
    assert!(out_dir.join("ep01.txt").exists());
    assert!(out_dir.join("ep02.txt").exists());
}
