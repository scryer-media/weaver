use super::*;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::process::Command;

use tempfile::TempDir;

fn create_test_files(dir: &Path) -> HashMap<String, Vec<u8>> {
    let mut files = HashMap::new();

    let hello = b"Hello from weaver zip tests!\n".to_vec();
    fs::write(dir.join("hello.txt"), &hello).unwrap();
    files.insert("hello.txt".into(), hello);

    let binary: Vec<u8> = (0..=255).cycle().take(4096).collect();
    fs::write(dir.join("binary.bin"), &binary).unwrap();
    files.insert("binary.bin".into(), binary);

    let nested_dir = dir.join("nested");
    fs::create_dir_all(&nested_dir).unwrap();
    let nested = b"nested file".to_vec();
    fs::write(nested_dir.join("note.txt"), &nested).unwrap();
    files.insert("nested/note.txt".into(), nested);

    files
}

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
                .replace('\\', "/");
            out.insert(rel, fs::read(&path).unwrap());
        }
    }
}

fn run_7z(args: &[String]) {
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

fn create_zip_archive(archive_path: &Path, source_dir: &Path, extra_args: &[&str]) {
    let mut args = vec![
        "a".to_string(),
        "-tzip".to_string(),
        archive_path.to_string_lossy().into_owned(),
        format!("{}/*", source_dir.to_string_lossy()),
    ];
    args.extend(extra_args.iter().map(|arg| arg.to_string()));
    run_7z(&args);
}

fn extract_with_7z(archive_path: &Path, output_dir: &Path, password: Option<&str>) {
    let mut args = vec![
        "x".to_string(),
        archive_path.to_string_lossy().into_owned(),
        format!("-o{}", output_dir.to_string_lossy()),
        "-y".to_string(),
    ];
    if let Some(password) = password {
        args.push(format!("-p{password}"));
    }
    run_7z(&args);
}

fn extract_with_weaver_zip(
    archive_path: &Path,
    output_dir: &Path,
    password: Option<&str>,
) -> Vec<String> {
    let (event_tx, _event_rx) = tokio::sync::broadcast::channel(32);
    extract_zip(
        archive_path,
        output_dir,
        password,
        &event_tx,
        JobId(1),
        archive_path.file_name().unwrap().to_string_lossy().as_ref(),
    )
    .unwrap()
}

fn assert_zip_method_matches_7z(extra_args: &[&str], password: Option<&str>) {
    let tmp = TempDir::new().unwrap();
    let source_dir = tmp.path().join("src");
    let archive_path = tmp.path().join("archive.zip");
    let out_7z = tmp.path().join("out_7z");
    let out_weaver = tmp.path().join("out_weaver");
    fs::create_dir_all(&source_dir).unwrap();
    fs::create_dir_all(&out_7z).unwrap();
    fs::create_dir_all(&out_weaver).unwrap();

    let expected = create_test_files(&source_dir);
    create_zip_archive(&archive_path, &source_dir, extra_args);

    extract_with_7z(&archive_path, &out_7z, password);
    let extracted_names = extract_with_weaver_zip(&archive_path, &out_weaver, password);

    let seven_zip = read_dir_contents(&out_7z);
    let weaver_zip = read_dir_contents(&out_weaver);

    assert_eq!(seven_zip, expected);
    assert_eq!(weaver_zip, expected);
    assert_eq!(weaver_zip, seven_zip);

    let mut actual_names: Vec<_> = extracted_names.into_iter().collect();
    actual_names.sort();
    let mut expected_names: Vec<_> = expected.keys().cloned().collect();
    expected_names.sort();
    assert_eq!(actual_names, expected_names);
}

#[test]
fn zip_store_matches_7z() {
    assert_zip_method_matches_7z(&["-mm=Copy", "-mx0"], None);
}

#[test]
fn zip_deflate_matches_7z() {
    assert_zip_method_matches_7z(&["-mm=Deflate"], None);
}

#[test]
fn zip_deflate64_matches_7z() {
    assert_zip_method_matches_7z(&["-mm=Deflate64"], None);
}

#[test]
fn zip_bzip2_matches_7z() {
    assert_zip_method_matches_7z(&["-mm=BZip2"], None);
}

#[test]
fn zip_aes_matches_7z() {
    assert_zip_method_matches_7z(
        &["-mm=Deflate", "-mem=AES256", "-pTestPass123"],
        Some("TestPass123"),
    );
}

#[test]
fn zip64_archive_from_7z_is_readable() {
    let tmp = TempDir::new().unwrap();
    let source_dir = tmp.path().join("src");
    let archive_path = tmp.path().join("zip64.zip");
    let out_dir = tmp.path().join("zip64_out");
    fs::create_dir_all(&source_dir).unwrap();
    fs::create_dir_all(&out_dir).unwrap();

    let entry_count = 70_000usize;
    for i in 0..entry_count {
        fs::write(source_dir.join(format!("entry_{i:05}.txt")), []).unwrap();
    }

    create_zip_archive(&archive_path, &source_dir, &["-mm=Copy", "-mx0"]);
    run_7z(&[
        "t".to_string(),
        archive_path.to_string_lossy().into_owned(),
        "-y".to_string(),
    ]);

    let extracted_names = extract_with_weaver_zip(&archive_path, &out_dir, None);
    assert_eq!(extracted_names.len(), entry_count);
    assert!(out_dir.join("entry_00000.txt").exists());
    assert!(out_dir.join("entry_35000.txt").exists());
    assert!(out_dir.join("entry_69999.txt").exists());
}
