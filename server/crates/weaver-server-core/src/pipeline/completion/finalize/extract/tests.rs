use super::*;
use std::collections::HashMap;
use std::fs;
use std::io::{Cursor, Read, Write};
use std::num::NonZeroU64;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard};

use tempfile::TempDir;
use zip::ZipWriter;
use zip::write::SimpleFileOptions;

use lzma_rust2::{XzOptions, XzWriter, XzWriterMt};

static XZ_MT_DECODER_TEST_LOCK: Mutex<()> = Mutex::new(());

fn lock_xz_mt_decoder_test() -> MutexGuard<'static, ()> {
    let guard = XZ_MT_DECODER_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    XZ_MT_DECODER_TEST_LOCK.clear_poison();
    XZ_MT_DECODER_PERMIT.clear_poison();
    guard
}

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
    extract_with_weaver_zip_result(archive_path, output_dir, password).unwrap()
}

fn test_extraction_security(output_dir: &Path) -> (ExtractionRoot, Arc<JobExtractionBudget>) {
    let limits = Arc::new(ExtractionLimits {
        max_job_bytes: 2 * 1024 * 1024 * 1024 * 1024,
        max_member_bytes: 1024 * 1024 * 1024 * 1024,
        max_entries: 100_000,
        max_ratio: 100,
        max_seconds: 43_200,
        min_free_bytes: 1,
        max_memory_bytes: 1024 * 1024 * 1024,
    });
    let root = ExtractionRoot::open(output_dir).unwrap();
    let budget = JobExtractionBudget::new(
        limits,
        output_dir.to_path_buf(),
        1024 * 1024 * 1024,
        0,
        0,
        PipelineMetrics::new(),
    )
    .unwrap();
    (root, budget)
}

fn extract_with_weaver_zip_result(
    archive_path: &Path,
    output_dir: &Path,
    password: Option<&str>,
) -> Result<Vec<String>, String> {
    let (event_tx, _event_rx) = tokio::sync::broadcast::channel(32);
    let (root, budget) = test_extraction_security(output_dir);
    extract_zip(
        archive_path,
        &root,
        &budget,
        password,
        &event_tx,
        JobId(1),
        archive_path.file_name().unwrap().to_string_lossy().as_ref(),
        None,
    )
}

fn extract_with_weaver_zip_result_with_phase(
    archive_path: &Path,
    output_dir: &Path,
    password: Option<&str>,
) -> (Result<Vec<String>, String>, Arc<PhaseCounters>) {
    let (event_tx, _event_rx) = tokio::sync::broadcast::channel(32);
    let phase_counters = Arc::new(PhaseCounters::default());
    let (root, budget) = test_extraction_security(output_dir);
    let result = extract_zip(
        archive_path,
        &root,
        &budget,
        password,
        &event_tx,
        JobId(1),
        archive_path.file_name().unwrap().to_string_lossy().as_ref(),
        Some(Arc::clone(&phase_counters)),
    );
    (result, phase_counters)
}

fn create_zip_with_entries(archive_path: &Path, entries: &[(&str, &[u8])]) {
    let file = fs::File::create(archive_path).unwrap();
    let mut zip = ZipWriter::new(file);
    let options = SimpleFileOptions::default();
    for (name, contents) in entries {
        zip.start_file(*name, options).unwrap();
        zip.write_all(contents).unwrap();
    }
    zip.finish().unwrap();
}

fn create_tar_with_entries(archive_path: &Path, entries: &[(&str, &[u8])]) {
    let file = fs::File::create(archive_path).unwrap();
    let mut tar = tar::Builder::new(file);
    for (name, contents) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_size(contents.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar.append_data(&mut header, name, Cursor::new(*contents))
            .unwrap();
    }
    tar.finish().unwrap();
}

fn xz_compress(bytes: &[u8], block_size: Option<NonZeroU64>) -> Vec<u8> {
    let mut options = XzOptions::with_preset(0);
    options.set_block_size(block_size);
    let mut writer = XzWriter::new(Vec::new(), options).unwrap();
    writer.write_all(bytes).unwrap();
    writer.finish().unwrap()
}

fn xz_compress_multiblock(bytes: &[u8]) -> Vec<u8> {
    let mut options = XzOptions::with_preset(0);
    options.set_block_size(NonZeroU64::new(options.lzma_options.dict_size.into()));
    let mut writer = XzWriterMt::new(Vec::new(), options, 2).unwrap();
    writer.write_all(bytes).unwrap();
    writer.finish().unwrap()
}

fn create_raw_tar_with_entries(archive_path: &Path, entries: &[(&str, &[u8])]) {
    let mut file = fs::File::create(archive_path).unwrap();
    for (name, contents) in entries {
        assert!(name.len() <= 100, "test tar name too long: {name}");
        let mut header = [0u8; 512];
        header[..name.len()].copy_from_slice(name.as_bytes());
        write_tar_octal_field(&mut header[100..108], 0o644);
        write_tar_octal_field(&mut header[108..116], 0);
        write_tar_octal_field(&mut header[116..124], 0);
        write_tar_octal_field(&mut header[124..136], contents.len() as u64);
        write_tar_octal_field(&mut header[136..148], 0);
        header[148..156].fill(b' ');
        header[156] = b'0';
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");
        let checksum = header.iter().map(|byte| *byte as u32).sum::<u32>();
        let checksum_field = format!("{checksum:06o}\0 ");
        header[148..156].copy_from_slice(checksum_field.as_bytes());

        file.write_all(&header).unwrap();
        file.write_all(contents).unwrap();
        let padding = (512 - (contents.len() % 512)) % 512;
        if padding > 0 {
            file.write_all(&vec![0u8; padding]).unwrap();
        }
    }
    file.write_all(&[0u8; 1024]).unwrap();
}

fn write_tar_octal_field(field: &mut [u8], value: u64) {
    let text = format!("{value:0width$o}\0", width = field.len() - 1);
    field.copy_from_slice(text.as_bytes());
}

fn extract_with_weaver_tar_result(
    archive_path: &Path,
    output_dir: &Path,
) -> Result<Vec<String>, String> {
    let (event_tx, _event_rx) = tokio::sync::broadcast::channel(32);
    let (root, budget) = test_extraction_security(output_dir);
    extract_tar(
        archive_path,
        &root,
        &budget,
        &event_tx,
        JobId(1),
        archive_path.file_name().unwrap().to_string_lossy().as_ref(),
    )
}

fn extract_with_weaver_xz_result(
    archive_path: &Path,
    output_dir: &Path,
) -> Result<Vec<String>, String> {
    let _test_guard = lock_xz_mt_decoder_test();
    let (event_tx, _event_rx) = tokio::sync::broadcast::channel(32);
    let (root, budget) = test_extraction_security(output_dir);
    extract_xz(
        archive_path,
        &root,
        &budget,
        &event_tx,
        JobId(1),
        archive_path.file_name().unwrap().to_string_lossy().as_ref(),
        2,
    )
}

fn extract_with_weaver_tar_xz_result(
    archive_path: &Path,
    output_dir: &Path,
) -> Result<Vec<String>, String> {
    let (event_tx, _event_rx) = tokio::sync::broadcast::channel(32);
    let (root, budget) = test_extraction_security(output_dir);
    extract_tar_xz(
        archive_path,
        &root,
        &budget,
        &event_tx,
        JobId(1),
        archive_path.file_name().unwrap().to_string_lossy().as_ref(),
    )
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
    let (extracted_names, phase_counters) =
        extract_with_weaver_zip_result_with_phase(&archive_path, &out_weaver, password);
    let extracted_names = extracted_names.unwrap();

    let seven_zip = read_dir_contents(&out_7z);
    let weaver_zip = read_dir_contents(&out_weaver);
    let expected_total = expected
        .values()
        .map(|bytes| bytes.len() as u64)
        .sum::<u64>();

    assert_eq!(seven_zip, expected);
    assert_eq!(weaver_zip, expected);
    assert_eq!(weaver_zip, seven_zip);
    assert_eq!(
        phase_counters.total_bytes.load(Ordering::Relaxed),
        expected_total
    );
    assert_eq!(
        phase_counters.completed_bytes.load(Ordering::Relaxed),
        expected_total
    );

    let mut actual_names: Vec<_> = extracted_names.into_iter().collect();
    actual_names.sort();
    let mut expected_names: Vec<_> = expected.keys().cloned().collect();
    expected_names.sort();
    assert_eq!(actual_names, expected_names);
}

#[test]
fn simple_decoder_reservations_use_realistic_codec_bounds() {
    let large_limit = 48_u64 * 1024 * 1024 * 1024;
    assert_eq!(
        simple_decoder_memory_bytes(SimpleArchiveKind::Zstd, large_limit),
        large_limit
    );
    assert_eq!(
        simple_decoder_memory_bytes(SimpleArchiveKind::Brotli, large_limit),
        32 * 1024 * 1024
    );
    assert_eq!(
        simple_decoder_memory_bytes(SimpleArchiveKind::Tar, large_limit),
        1024 * 1024
    );
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
fn zip_direct_writer_nested_entry_extracts_with_normalized_name() {
    let tmp = TempDir::new().unwrap();
    let archive_path = tmp.path().join("direct.zip");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&out_dir).unwrap();
    create_zip_with_entries(&archive_path, &[("nested/note.txt", b"safe nested")]);

    let extracted_names = extract_with_weaver_zip(&archive_path, &out_dir, None);

    assert_eq!(extracted_names, vec!["nested/note.txt".to_string()]);
    assert_eq!(
        fs::read(out_dir.join("nested/note.txt")).unwrap(),
        b"safe nested"
    );
}

#[test]
fn zip_rejects_unsafe_entry_paths() {
    let unsafe_names = [
        "../escape.txt",
        "nested/../../escape.txt",
        "/absolute.txt",
        "C:/windows.txt",
        "..\\escape.txt",
        "nested\\..\\escape.txt",
    ];

    for name in unsafe_names {
        let tmp = TempDir::new().unwrap();
        let archive_path = tmp.path().join("unsafe.zip");
        let out_dir = tmp.path().join("out");
        fs::create_dir_all(&out_dir).unwrap();
        create_zip_with_entries(&archive_path, &[(name, b"unsafe")]);

        let error = extract_with_weaver_zip_result(&archive_path, &out_dir, None).unwrap_err();

        assert!(
            error.contains("unsafe zip entry path"),
            "unexpected error for {name}: {error}"
        );
        assert!(
            read_dir_contents(&out_dir).is_empty(),
            "unsafe zip entry {name} should not write under output dir"
        );
        assert!(
            !tmp.path().join("escape.txt").exists(),
            "unsafe zip entry {name} should not write beside output dir"
        );
    }
}

#[test]
fn tar_extracts_safe_nested_entry() {
    let tmp = TempDir::new().unwrap();
    let archive_path = tmp.path().join("safe.tar");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&out_dir).unwrap();
    create_tar_with_entries(&archive_path, &[("nested/note.txt", b"safe nested")]);

    let extracted_names = extract_with_weaver_tar_result(&archive_path, &out_dir).unwrap();

    assert_eq!(extracted_names, vec!["nested/note.txt".to_string()]);
    assert_eq!(
        fs::read(out_dir.join("nested/note.txt")).unwrap(),
        b"safe nested"
    );
}

#[test]
fn tar_ignores_current_dir_entry() {
    let tmp = TempDir::new().unwrap();
    let archive_path = tmp.path().join("current-dir.tar");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&out_dir).unwrap();

    let file = fs::File::create(&archive_path).unwrap();
    let mut tar = tar::Builder::new(file);
    tar.append_dir("./", tmp.path()).unwrap();

    let mut header = tar::Header::new_gnu();
    header.set_size(b"safe nested".len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(
        &mut header,
        "./nested/note.txt",
        Cursor::new(b"safe nested"),
    )
    .unwrap();
    tar.finish().unwrap();

    let extracted_names = extract_with_weaver_tar_result(&archive_path, &out_dir).unwrap();

    assert_eq!(extracted_names, vec!["nested/note.txt".to_string()]);
    assert_eq!(
        fs::read(out_dir.join("nested/note.txt")).unwrap(),
        b"safe nested"
    );
}

#[test]
fn tar_rejects_unsafe_entry_paths() {
    let unsafe_names = [
        "../escape.txt",
        "nested/../../escape.txt",
        "/absolute.txt",
        "C:/windows.txt",
        "..\\escape.txt",
        "nested\\note.txt",
        "nested\\..\\escape.txt",
    ];

    for name in unsafe_names {
        let tmp = TempDir::new().unwrap();
        let archive_path = tmp.path().join("unsafe.tar");
        let out_dir = tmp.path().join("out");
        fs::create_dir_all(&out_dir).unwrap();
        create_raw_tar_with_entries(&archive_path, &[(name, b"unsafe")]);

        let error = extract_with_weaver_tar_result(&archive_path, &out_dir).unwrap_err();

        assert!(
            error.contains("unsafe tar entry path"),
            "unexpected error for {name}: {error}"
        );
        assert!(
            read_dir_contents(&out_dir).is_empty(),
            "unsafe tar entry {name} should not write under output dir"
        );
        assert!(
            !tmp.path().join("escape.txt").exists(),
            "unsafe tar entry {name} should not write beside output dir"
        );
    }
}

#[test]
fn tar_rejects_links_and_special_entries_without_touching_targets() {
    for (label, entry_type, link_target) in [
        ("symlink", tar::EntryType::Symlink, Some("../outside.txt")),
        ("hardlink", tar::EntryType::Link, Some("../outside.txt")),
        ("fifo", tar::EntryType::Fifo, None),
        ("sparse", tar::EntryType::GNUSparse, None),
    ] {
        let tmp = TempDir::new().unwrap();
        let archive_path = tmp.path().join(format!("{label}.tar"));
        let out_dir = tmp.path().join("out");
        let outside = tmp.path().join("outside.txt");
        fs::create_dir_all(&out_dir).unwrap();
        fs::write(&outside, b"unchanged").unwrap();

        let file = fs::File::create(&archive_path).unwrap();
        let mut builder = tar::Builder::new(file);
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(entry_type);
        header.set_size(0);
        header.set_mode(0o644);
        if let Some(target) = link_target {
            header.set_link_name(target).unwrap();
        }
        header.set_cksum();
        builder
            .append_data(&mut header, "payload", std::io::empty())
            .unwrap();
        builder.finish().unwrap();

        let error = extract_with_weaver_tar_result(&archive_path, &out_dir).unwrap_err();
        assert!(
            error.contains("unsupported_entry")
                || (label == "sparse" && error.contains("failed to read tar entry")),
            "{label}: {error}"
        );
        assert_eq!(fs::read(&outside).unwrap(), b"unchanged");
        assert!(read_dir_contents(&out_dir).is_empty());
    }
}

#[test]
fn xz_extracts_a_single_file_without_its_suffix() {
    let temp = TempDir::new().unwrap();
    let archive_path = temp.path().join("payload.txt.xz");
    let output_dir = temp.path().join("out");
    fs::create_dir_all(&output_dir).unwrap();
    fs::write(&archive_path, xz_compress(b"xz payload", None)).unwrap();

    let extracted = extract_with_weaver_xz_result(&archive_path, &output_dir).unwrap();

    assert_eq!(extracted, vec!["payload.txt"]);
    assert_eq!(
        fs::read(output_dir.join("payload.txt")).unwrap(),
        b"xz payload"
    );
}

#[test]
fn tar_xz_uses_the_hardened_tar_member_extractor() {
    let temp = TempDir::new().unwrap();
    let tar_path = temp.path().join("payload.tar");
    let archive_path = temp.path().join("payload.tar.xz");
    let output_dir = temp.path().join("out");
    fs::create_dir_all(&output_dir).unwrap();
    create_tar_with_entries(&tar_path, &[("nested/payload.txt", b"tar xz payload")]);
    fs::write(
        &archive_path,
        xz_compress(&fs::read(&tar_path).unwrap(), None),
    )
    .unwrap();

    extract_with_weaver_tar_xz_result(&archive_path, &output_dir).unwrap();

    assert_eq!(
        fs::read(output_dir.join("nested/payload.txt")).unwrap(),
        b"tar xz payload"
    );
}

#[test]
fn concatenated_xz_streams_decode_as_one_stream() {
    let temp = TempDir::new().unwrap();
    let archive_path = temp.path().join("payload.txt.xz");
    let output_dir = temp.path().join("out");
    fs::create_dir_all(&output_dir).unwrap();
    let mut streams = xz_compress(b"first ", None);
    streams.extend(xz_compress(b"second", None));
    fs::write(&archive_path, streams).unwrap();

    extract_with_weaver_xz_result(&archive_path, &output_dir).unwrap();

    assert_eq!(
        fs::read(output_dir.join("payload.txt")).unwrap(),
        b"first second"
    );
}

#[test]
fn multi_block_xz_extracts_with_the_filesystem_decoder() {
    let temp = TempDir::new().unwrap();
    let archive_path = temp.path().join("payload.bin.xz");
    let output_dir = temp.path().join("out");
    fs::create_dir_all(&output_dir).unwrap();
    let payload: Vec<u8> = (0..(1024 * 1024))
        .map(|index| (index % 251) as u8)
        .collect();
    fs::write(
        &archive_path,
        xz_compress(&payload, NonZeroU64::new(64 * 1024)),
    )
    .unwrap();

    extract_with_weaver_xz_result(&archive_path, &output_dir).unwrap();

    assert_eq!(fs::read(output_dir.join("payload.bin")).unwrap(), payload);
}

#[test]
fn filesystem_xz_decoder_uses_parallel_for_a_multiblock_single_stream() {
    let _test_guard = lock_xz_mt_decoder_test();
    let temp = TempDir::new().unwrap();
    let archive_path = temp.path().join("payload.bin.xz");
    let output_dir = temp.path().join("out");
    fs::create_dir_all(&output_dir).unwrap();
    let payload: Vec<u8> = (0..(1024 * 1024))
        .map(|index| (index % 251) as u8)
        .collect();
    fs::write(&archive_path, xz_compress_multiblock(&payload)).unwrap();

    let (_root, budget) = test_extraction_security(&output_dir);
    let mut decoder = open_filesystem_xz_decoder(&archive_path, &budget, 2).unwrap();
    assert!(matches!(&decoder, FilesystemXzDecoder::Parallel { .. }));

    let mut output = Vec::new();
    decoder.read_to_end(&mut output).unwrap();
    assert_eq!(output, payload);
}

#[test]
fn filesystem_xz_decoder_falls_back_to_sequential_when_mt_is_busy() {
    let _test_guard = lock_xz_mt_decoder_test();
    let temp = TempDir::new().unwrap();
    let archive_path = temp.path().join("payload.bin.xz");
    let output_dir = temp.path().join("out");
    fs::create_dir_all(&output_dir).unwrap();
    let payload: Vec<u8> = (0..(1024 * 1024))
        .map(|index| (index % 251) as u8)
        .collect();
    fs::write(&archive_path, xz_compress_multiblock(&payload)).unwrap();

    let (_root, budget) = test_extraction_security(&output_dir);
    let _permit = XZ_MT_DECODER_PERMIT.lock().unwrap();
    let mut decoder = open_filesystem_xz_decoder(&archive_path, &budget, 2).unwrap();
    assert!(matches!(&decoder, FilesystemXzDecoder::Sequential(_)));

    let mut output = Vec::new();
    decoder.read_to_end(&mut output).unwrap();
    assert_eq!(output, payload);
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
