use super::*;
mod zip64;
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
    test_extraction_security_with_memory(output_dir, 1024 * 1024 * 1024)
}

fn test_extraction_security_with_memory(
    output_dir: &Path,
    max_memory_bytes: u64,
) -> (ExtractionRoot, Arc<JobExtractionBudget>) {
    let limits = Arc::new(ExtractionLimits {
        max_job_bytes: 2 * 1024 * 1024 * 1024 * 1024,
        max_member_bytes: 1024 * 1024 * 1024 * 1024,
        max_entries: 100_000,
        max_ratio: 100,
        max_seconds: 43_200,
        min_free_bytes: 1,
        max_memory_bytes,
    });
    let root = ExtractionRoot::open(output_dir).unwrap();
    let budget = JobExtractionBudget::new(
        limits,
        output_dir.to_path_buf(),
        max_memory_bytes,
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

/// The simple-archive task and the xz decoder must not both admit the same
/// footprint. The decoder measures and reserves what it will hold; a
/// ceiling-sized permit held over it leaves a job whose ceiling is the xz
/// limit with nothing for the decoder to reserve, and the decoder then waits
/// for room only the permit above it could release.
#[test]
fn xz_extraction_admits_its_decoder_footprint_once() {
    let _test_guard = lock_xz_mt_decoder_test();
    let temp = TempDir::new().unwrap();
    let archive_path = temp.path().join("payload.bin.xz");
    let output_dir = temp.path().join("out");
    fs::create_dir_all(&output_dir).unwrap();
    let payload: Vec<u8> = (0..(1024 * 1024))
        .map(|index| (index % 251) as u8)
        .collect();
    fs::write(&archive_path, xz_compress_multiblock(&payload)).unwrap();

    let (root, budget) = test_extraction_security_with_memory(
        &output_dir,
        crate::ingest::XZ_DECODER_MEMORY_LIMIT_BYTES,
    );
    let (event_tx, _event_rx) = tokio::sync::broadcast::channel(32);

    // What the simple-archive task admits before it opens the decoder.
    let _task_memory = simple_archive_task_memory_permit(
        SimpleArchiveKind::Xz,
        Some(archive_path.as_path()),
        &budget,
    )
    .unwrap();

    extract_xz(
        &archive_path,
        &root,
        &budget,
        &event_tx,
        JobId(1),
        "payload.bin.xz",
        2,
    )
    .unwrap();

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

/// The parallel decoder is only taken when the job's memory budget can hold
/// one worker; a budget one byte short of that goes to the sequential decoder,
/// whose dictionary is bounded by the block it decodes and still fits.
#[test]
fn filesystem_xz_decoder_falls_back_to_sequential_when_a_worker_does_not_fit() {
    let _test_guard = lock_xz_mt_decoder_test();
    let temp = TempDir::new().unwrap();
    let archive_path = temp.path().join("payload.bin.xz");
    let output_dir = temp.path().join("out");
    fs::create_dir_all(&output_dir).unwrap();
    let payload: Vec<u8> = (0..(1024 * 1024))
        .map(|index| (index % 251) as u8)
        .collect();
    fs::write(&archive_path, xz_compress_multiblock(&payload)).unwrap();

    let one_worker = lzma_turbo::xz::XzParallelReader::with_options(
        fs::File::open(&archive_path).unwrap(),
        lzma_turbo::xz::XzOptions::default().with_threads(1),
    )
    .unwrap()
    .memory_estimate();

    let (_root, budget) = test_extraction_security_with_memory(&output_dir, one_worker - 1);
    let mut decoder = open_filesystem_xz_decoder(&archive_path, &budget, 2).unwrap();
    assert!(matches!(&decoder, FilesystemXzDecoder::Sequential { .. }));

    let mut output = Vec::new();
    decoder.read_to_end(&mut output).unwrap();
    assert_eq!(output, payload);
}

/// A budget that holds exactly one worker gets the parallel decoder with its
/// thread count trimmed to one, not the sequential fallback.
#[test]
fn filesystem_xz_decoder_trims_its_threads_to_the_memory_budget() {
    let _test_guard = lock_xz_mt_decoder_test();
    let temp = TempDir::new().unwrap();
    let archive_path = temp.path().join("payload.bin.xz");
    let output_dir = temp.path().join("out");
    fs::create_dir_all(&output_dir).unwrap();
    let payload: Vec<u8> = (0..(1024 * 1024))
        .map(|index| (index % 251) as u8)
        .collect();
    fs::write(&archive_path, xz_compress_multiblock(&payload)).unwrap();

    let one_worker = lzma_turbo::xz::XzParallelReader::with_options(
        fs::File::open(&archive_path).unwrap(),
        lzma_turbo::xz::XzOptions::default().with_threads(1),
    )
    .unwrap()
    .memory_estimate();

    let (_root, budget) = test_extraction_security_with_memory(&output_dir, one_worker);
    let mut decoder = open_filesystem_xz_decoder(&archive_path, &budget, 4).unwrap();
    match &decoder {
        FilesystemXzDecoder::Parallel { decoder, .. } => assert_eq!(decoder.threads(), 1),
        FilesystemXzDecoder::Sequential { .. } => panic!("expected the parallel decoder"),
    }

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

/// A 7z archive carrying the metadata 7-Zip records and the extractor used to
/// ignore: per-entry times on a directory, a file and an empty file, and an
/// anti-item — an update archive's deletion marker for a path that must not
/// exist after extraction.
fn sevenz_archive_with_times_and_anti_item(
    directory_time: std::time::SystemTime,
    file_time: std::time::SystemTime,
    access_time: std::time::SystemTime,
) -> Vec<u8> {
    use sevenz_turbo::{ArchiveEntry, ArchiveWriter, NtTime};

    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).expect("writer");

    let mut directory = ArchiveEntry::new_directory("Silver.Horizon");
    directory.has_last_modified_date = true;
    directory.last_modified_date = NtTime::try_from(directory_time).expect("directory time");
    writer
        .push_archive_entry(directory, None::<Cursor<Vec<u8>>>)
        .expect("directory entry");

    let mut episode = ArchiveEntry::new_file("Silver.Horizon/episode.txt");
    episode.has_last_modified_date = true;
    episode.last_modified_date = NtTime::try_from(file_time).expect("file time");
    episode.has_access_date = true;
    episode.access_date = NtTime::try_from(access_time).expect("access time");
    writer
        .push_archive_entry(episode, Some(Cursor::new(b"silver horizon".to_vec())))
        .expect("file entry");

    let mut empty = ArchiveEntry::new_file("Silver.Horizon/empty.txt");
    empty.has_last_modified_date = true;
    empty.last_modified_date = NtTime::try_from(file_time).expect("file time");
    writer
        .push_archive_entry(empty, None::<Cursor<Vec<u8>>>)
        .expect("empty entry");

    let mut stale = ArchiveEntry::new_file("Silver.Horizon/stale.txt");
    stale.is_anti_item = true;
    writer
        .push_archive_entry(stale, None::<Cursor<Vec<u8>>>)
        .expect("anti-item entry");

    writer.finish().expect("finish").into_inner()
}

#[test]
fn sevenzip_extraction_restores_entry_times_and_skips_anti_items() {
    use std::time::{Duration, UNIX_EPOCH};

    let tmp = TempDir::new().unwrap();
    let archive_path = tmp.path().join("silver_horizon.7z");
    let out_dir = tmp.path().join("out");
    fs::create_dir_all(&out_dir).unwrap();

    // Whole seconds, so no filesystem's time resolution can round them.
    let directory_time = UNIX_EPOCH + Duration::from_secs(1_588_561_321);
    let file_time = UNIX_EPOCH + Duration::from_secs(1_623_053_350);
    let access_time = UNIX_EPOCH + Duration::from_secs(1_623_139_750);
    let archive = sevenz_archive_with_times_and_anti_item(directory_time, file_time, access_time);
    let end_header_bytes = sevenz_end_header_bytes(&archive);
    fs::write(&archive_path, &archive).unwrap();

    let (root, budget) = test_extraction_security(&out_dir);
    let (event_tx, _event_rx) = tokio::sync::broadcast::channel(32);
    let context = SevenZipExtractionContext {
        job_id: JobId(41812),
        set_name: "silver_horizon.7z".to_string(),
        output_dir: out_dir.clone(),
        root: Arc::new(root),
        budget,
        password: sevenz_turbo::Password::empty(),
        event_tx,
        phase_counters: Arc::new(PhaseCounters::default()),
        decode_memory: SevenZipDecodeMemory::ReservedForFixedThreads { end_header_bytes },
        decode_threads: 1,
    };
    let outcome = extract_7z_stream(&context, || {
        fs::File::open(&archive_path).map_err(|error| error.to_string())
    })
    .expect("7z extraction");

    assert_eq!(
        outcome.extracted,
        vec![
            "Silver.Horizon/episode.txt".to_string(),
            "Silver.Horizon/empty.txt".to_string(),
        ],
        "the anti-item is neither extracted nor reported as a member"
    );
    assert!(
        !out_dir.join("Silver.Horizon/stale.txt").exists(),
        "an anti-item marks a deletion and must not become an output"
    );

    // Metadata before content: reading the file would move its access time.
    let episode = fs::metadata(out_dir.join("Silver.Horizon/episode.txt")).unwrap();
    assert_eq!(episode.modified().unwrap(), file_time);
    assert_eq!(episode.accessed().unwrap(), access_time);
    assert_eq!(
        fs::read(out_dir.join("Silver.Horizon/episode.txt")).unwrap(),
        b"silver horizon"
    );

    let empty = fs::metadata(out_dir.join("Silver.Horizon/empty.txt")).unwrap();
    assert_eq!(empty.len(), 0);
    assert_eq!(empty.modified().unwrap(), file_time);

    // The directory is stamped after its members were written into it.
    let directory = fs::metadata(out_dir.join("Silver.Horizon")).unwrap();
    assert!(directory.is_dir());
    assert_eq!(directory.modified().unwrap(), directory_time);
}

/// A 7z archive whose one LZMA2 block declares a `dictionary`-byte
/// dictionary, holding `members` in order.
fn sevenz_archive_with_dictionary(dictionary: u32, members: &[(&str, &[u8])]) -> Vec<u8> {
    use sevenz_turbo::encoder_options::Lzma2Options;
    use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration};

    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).expect("writer");
    let mut options = Lzma2Options::from_level(1);
    options.set_dictionary_size(dictionary);
    writer.set_content_methods(vec![EncoderConfiguration::from(options)]);
    for (name, bytes) in members {
        writer
            .push_archive_entry(
                ArchiveEntry::new_file(name),
                Some(Cursor::new(bytes.to_vec())),
            )
            .expect("entry");
    }
    writer.finish().expect("finish").into_inner()
}

fn conventional_7z_context(
    out_dir: &Path,
    root: ExtractionRoot,
    budget: Arc<JobExtractionBudget>,
    archive: &[u8],
    decode_threads: u32,
) -> SevenZipExtractionContext {
    let (event_tx, _event_rx) = tokio::sync::broadcast::channel(32);
    SevenZipExtractionContext {
        job_id: JobId(41813),
        set_name: "wide_dictionary.7z".to_string(),
        output_dir: out_dir.to_path_buf(),
        root: Arc::new(root),
        budget,
        password: sevenz_turbo::Password::empty(),
        event_tx,
        phase_counters: Arc::new(PhaseCounters::default()),
        decode_memory: SevenZipDecodeMemory::ReservedForFixedThreads {
            end_header_bytes: sevenz_end_header_bytes(archive),
        },
        decode_threads,
    }
}

/// An encoded end header decodes under the reader's limits, which reach the
/// ceiling, before anything about the archive has been measured. The
/// conventional metadata pass never parks, so it is admitted up to the
/// ceiling rather than on a header-sized allowance.
#[test]
fn conventional_7z_metadata_pass_is_admitted_up_to_the_ceiling() {
    let temp = TempDir::new().unwrap();
    let archive_path = temp.path().join("wide_dictionary.7z");
    let out_dir = temp.path().join("out");
    fs::create_dir_all(&out_dir).unwrap();
    let archive = sevenz_archive_with_dictionary(
        1024 * 1024,
        &[("Wide.Dictionary/episode.txt", b"wide dictionary")],
    );
    fs::write(&archive_path, &archive).unwrap();
    let header_floor =
        chase_header_pass_memory_bytes(sevenz_end_header_bytes(&archive), 512 * 1024 * 1024);

    let (root, budget) = test_extraction_security_with_memory(&out_dir, 512 * 1024 * 1024);
    let context = conventional_7z_context(&out_dir, root, Arc::clone(&budget), &archive, 1);
    let reserved_at_open = std::sync::Mutex::new(Vec::new());
    extract_7z_stream(&context, || {
        reserved_at_open
            .lock()
            .unwrap()
            .push(budget.memory_reserved_bytes());
        fs::File::open(&archive_path).map_err(|error| error.to_string())
    })
    .expect("7z extraction");

    let metadata_pass = reserved_at_open.lock().unwrap()[0];
    assert!(
        metadata_pass > header_floor,
        "the metadata pass held {metadata_pass} bytes, no more than the header-sized \
         {header_floor}"
    );
}

/// The dictionary an archive declares is allocated on the archive's say-so.
/// One the job's memory ceiling cannot hold is refused when the archive is
/// opened, before a decoder exists and before anything is created on disk.
#[test]
fn conventional_7z_extraction_refuses_a_dictionary_the_memory_ceiling_cannot_hold() {
    let temp = TempDir::new().unwrap();
    let archive_path = temp.path().join("wide_dictionary.7z");
    let out_dir = temp.path().join("out");
    fs::create_dir_all(&out_dir).unwrap();
    let archive = sevenz_archive_with_dictionary(
        16 * 1024 * 1024,
        &[("Wide.Dictionary/episode.txt", b"wide dictionary")],
    );
    fs::write(&archive_path, &archive).unwrap();

    let (root, budget) = test_extraction_security_with_memory(&out_dir, 8 * 1024 * 1024);
    let context = conventional_7z_context(&out_dir, root, budget, &archive, 1);
    let error = match extract_7z_stream(&context, || {
        fs::File::open(&archive_path).map_err(|error| error.to_string())
    }) {
        Ok(_) => panic!("a 16 MiB dictionary was decoded under an 8 MiB ceiling"),
        Err(error) => error,
    };

    assert!(
        error.contains("MemoryLimited"),
        "the refusal names the memory limit: {error}"
    );
    assert!(
        !out_dir.join("Wide.Dictionary").exists(),
        "nothing is created for an archive that was refused at open"
    );
}

/// The conventional path decodes with as many threads as it is given, and a
/// thread count wider than the archive has runs to give it changes nothing
/// about the output.
#[test]
fn conventional_7z_extraction_decodes_with_the_threads_it_is_given() {
    let temp = TempDir::new().unwrap();
    let archive_path = temp.path().join("wide_dictionary.7z");
    let out_dir = temp.path().join("out");
    fs::create_dir_all(&out_dir).unwrap();
    let first: Vec<u8> = (0..(256 * 1024)).map(|index| (index % 251) as u8).collect();
    let second: Vec<u8> = (0..(128 * 1024)).map(|index| (index % 239) as u8).collect();
    let archive = sevenz_archive_with_dictionary(
        1024 * 1024,
        &[
            ("Wide.Dictionary/first.bin", &first),
            ("Wide.Dictionary/second.bin", &second),
        ],
    );
    fs::write(&archive_path, &archive).unwrap();

    let (root, budget) = test_extraction_security(&out_dir);
    let context = conventional_7z_context(&out_dir, root, Arc::clone(&budget), &archive, 4);
    let outcome = extract_7z_stream(&context, || {
        fs::File::open(&archive_path).map_err(|error| error.to_string())
    })
    .expect("7z extraction");

    assert_eq!(
        outcome.extracted,
        vec![
            "Wide.Dictionary/first.bin".to_string(),
            "Wide.Dictionary/second.bin".to_string(),
        ]
    );
    assert_eq!(
        fs::read(out_dir.join("Wide.Dictionary/first.bin")).unwrap(),
        first
    );
    assert_eq!(
        fs::read(out_dir.join("Wide.Dictionary/second.bin")).unwrap(),
        second
    );
    assert_eq!(
        budget.memory_reserved_bytes(),
        0,
        "the conventional decode gives its per-pass reservations back"
    );
}

/// A conventional decode whose thread room does not fit the ceiling takes
/// what fits beside the process's retained state instead of waiting for all
/// of it: another queued job's scheduling state is held for that job's whole
/// life, so waiting for it to clear would wait for as long as the job exists.
#[test]
fn conventional_7z_extraction_admits_beside_a_peer_job_retained_state() {
    use crate::pipeline::extraction::ProcessMemoryBudget;

    const MIB: u64 = 1024 * 1024;
    let temp = TempDir::new().unwrap();
    let archive_path = temp.path().join("wide_dictionary.7z");
    let out_dir = temp.path().join("out");
    fs::create_dir_all(&out_dir).unwrap();
    let archive = sevenz_archive_with_dictionary(
        1024 * 1024,
        &[("Wide.Dictionary/episode.txt", b"wide dictionary")],
    );
    fs::write(&archive_path, &archive).unwrap();

    // Four threads' room is more than the 512 MiB ceiling holds, so the
    // decode reserves up to the ceiling rather than a measured amount.
    let limit = 512 * MIB;
    let pool = Arc::new(ProcessMemoryBudget::new(limit));
    let peer = pool
        .for_job(2)
        .try_reserve_retained(64 * MIB)
        .expect("a queued peer's scheduling state");
    let limits = Arc::new(ExtractionLimits {
        max_job_bytes: 2 * 1024 * 1024 * 1024 * 1024,
        max_member_bytes: 1024 * 1024 * 1024 * 1024,
        max_entries: 100_000,
        max_ratio: 100,
        max_seconds: 43_200,
        min_free_bytes: 1,
        max_memory_bytes: limit,
    });
    let root = ExtractionRoot::open(&out_dir).unwrap();
    let budget = JobExtractionBudget::new_with_process_memory(
        limits,
        pool.for_job(1),
        out_dir.clone(),
        limit,
        0,
        0,
        PipelineMetrics::new(),
    )
    .unwrap();
    assert_eq!(budget.max_memory_bytes(), limit);

    let context = conventional_7z_context(&out_dir, root, Arc::clone(&budget), &archive, 4);
    let outcome = extract_7z_stream(&context, || {
        fs::File::open(&archive_path).map_err(|error| error.to_string())
    })
    .expect("the decode is admitted while the peer's state is still held");

    assert_eq!(
        outcome.extracted,
        vec!["Wide.Dictionary/episode.txt".to_string()]
    );
    assert_eq!(
        fs::read(out_dir.join("Wide.Dictionary/episode.txt")).unwrap(),
        b"wide dictionary"
    );
    assert_eq!(budget.memory_reserved_bytes(), 0);
    assert_eq!(
        pool.reserved_bytes(),
        64 * MIB,
        "only the peer's retained state is still held"
    );
    drop(peer);
    assert_eq!(pool.reserved_bytes(), 0);
}

/// The conventional path reserves what the archive's decoders need plus room
/// for each of its threads past the first, not the whole ceiling, so it does
/// not hold every other extraction in the process behind it.
#[test]
fn conventional_7z_decode_reservation_is_sized_from_the_archive_and_its_threads() {
    let archive = sevenz_archive_with_dictionary(
        4 * 1024 * 1024,
        &[("Wide.Dictionary/episode.txt", b"wide dictionary")],
    );
    let parsed = sevenz_turbo::ArchiveReader::new(
        Cursor::new(archive.clone()),
        sevenz_turbo::Password::empty(),
    )
    .expect("parse");
    let end_header = sevenz_end_header_bytes(&archive);
    let decoders =
        crate::pipeline::direct_unpack::decode_memory::decoder_memory_bytes(parsed.archive())
            .expect("sized");
    let needed = decoders + end_header + CHASE_DECODE_ALLOWANCE_BYTES;
    let job_id = JobId(41817);
    let ceiling = 64 * 1024 * 1024 * 1024;

    assert_eq!(
        fixed_decode_memory_bytes(job_id, "wide", parsed.archive(), end_header, 1, ceiling),
        SevenZipDecodeReservation::Measured(needed),
        "one thread holds no runs beyond the one it decodes"
    );
    assert_eq!(
        fixed_decode_memory_bytes(job_id, "wide", parsed.archive(), end_header, 4, ceiling),
        SevenZipDecodeReservation::Measured(needed + 4 * CHASE_WIDENING_BYTES_PER_THREAD),
    );
    let tight = needed + CHASE_WIDENING_BYTES_PER_THREAD;
    assert_eq!(
        fixed_decode_memory_bytes(job_id, "wide", parsed.archive(), end_header, 4, tight),
        SevenZipDecodeReservation::UpToCeiling { floor: needed },
        "thread room is trimmed to what fits under the ceiling before the decoders are"
    );
    assert_eq!(
        chase_header_pass_memory_bytes(end_header, ceiling),
        end_header + CHASE_HEADER_PASS_ALLOWANCE_BYTES
    );
    assert_eq!(
        chase_header_pass_memory_bytes(end_header, 1024),
        1024,
        "the header pass never reserves past the ceiling"
    );

    let temp = TempDir::new().unwrap();
    let first_part = temp.path().join("wide.7z.001");
    fs::write(&first_part, &archive[..40]).unwrap();
    assert_eq!(sevenz_declared_end_header_bytes(&first_part), end_header);
    fs::write(&first_part, b"not a 7z").unwrap();
    assert_eq!(
        sevenz_declared_end_header_bytes(&first_part),
        0,
        "an unreadable signature header sizes nothing; the reader reports it"
    );
}

/// A 7z block whose bytes are wrong is worded for the scheduler to keep for
/// the recovery data; a method this build cannot decode, a password problem,
/// and a read that failed for any reason but malformed or short bytes keep the
/// ordinary wording, which ends the job. The I/O cases are rendered the way
/// the decoder renders them, from the reader's own error.
#[test]
fn sevenz_extraction_error_marks_only_data_errors() {
    use sevenz_turbo::BlockErrorKind;

    let block = |kind, message: String| sevenz_turbo::Error::BlockDecode {
        block_index: 0,
        packed_offset: 32,
        kind,
        message,
    };
    let read_failure = |error: std::io::Error| {
        block(
            BlockErrorKind::Io,
            sevenz_turbo::Error::Io(error, "".into()).to_string(),
        )
    };
    let data_errors = [
        block(BlockErrorKind::Corrupted, "detail".to_string()),
        block(BlockErrorKind::ChecksumMismatch, "detail".to_string()),
        read_failure(std::io::ErrorKind::UnexpectedEof.into()),
        read_failure(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "kind: NotFound quoted in the message",
        )),
        read_failure(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "short read",
        )),
    ];
    for error in &data_errors {
        let worded = sevenz_extraction_error(error);
        assert!(
            worded.starts_with(SEVENZ_BLOCK_DATA_ERROR_PREFIX),
            "{worded}"
        );
    }
    let terminal = [
        block(BlockErrorKind::UnsupportedMethod, "detail".to_string()),
        block(BlockErrorKind::Password, "detail".to_string()),
        block(BlockErrorKind::Io, "detail".to_string()),
        read_failure(std::io::Error::from_raw_os_error(5)),
        read_failure(std::io::ErrorKind::PermissionDenied.into()),
        read_failure(std::io::Error::new(
            std::io::ErrorKind::StorageFull,
            "kind: InvalidData quoted in the message",
        )),
    ];
    for error in &terminal {
        let worded = sevenz_extraction_error(error);
        assert!(worded.starts_with("7z extraction failed: "), "{worded}");
    }
}

/// `bytes` of word salad: compressible enough that decoding it takes real
/// time per byte, varied enough that no two runs are alike.
fn word_salad(bytes: usize, seed: u64) -> Vec<u8> {
    const WORDS: [&str; 12] = [
        "silver",
        "horizon",
        "reel",
        "episode",
        "chase",
        "frontier",
        "widen",
        "backlog",
        "run",
        "dictionary",
        "stream",
        "member",
    ];
    let mut state = seed.max(1);
    let mut out = Vec::with_capacity(bytes + 16);
    while out.len() < bytes {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let word = WORDS[(state >> 33) as usize % WORDS.len()];
        out.extend_from_slice(word.as_bytes());
        out.push(if (state >> 20) & 7 == 0 { b'\n' } else { b' ' });
    }
    out.truncate(bytes);
    out
}

/// A 7z archive whose one LZMA2 block was written by the multi-threaded
/// encoder in independent `chunk`-byte pieces, each starting with a
/// dictionary reset: the shape `7zz -mmt=on` writes, and the only one a
/// decoder can widen on.
fn sevenz_multi_run_archive(chunk: u64, members: &[(&str, &[u8])]) -> Vec<u8> {
    use sevenz_turbo::encoder_options::Lzma2Options;
    use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration};

    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).expect("writer");
    writer.set_content_methods(vec![EncoderConfiguration::from(
        Lzma2Options::from_level_mt(1, 4, chunk),
    )]);
    for (name, bytes) in members {
        writer
            .push_archive_entry(
                ArchiveEntry::new_file(name),
                Some(Cursor::new(bytes.to_vec())),
            )
            .expect("entry");
    }
    writer.finish().expect("finish").into_inner()
}

/// `next_header_size` from a 7z signature header: what a chase declares as
/// its end header.
fn sevenz_end_header_bytes(archive: &[u8]) -> u64 {
    u64::from_le_bytes(archive[20..28].try_into().unwrap())
}

/// One thread while the decoder is at the frontier, one more per complete
/// run waiting behind it, never past the ceiling, and back to one once the
/// backlog is gone.
#[test]
fn chase_decode_threads_follow_the_backlog_under_the_ceiling() {
    assert_eq!(chase_decode_thread_target(0, 8), 1);
    assert_eq!(chase_decode_thread_target(1, 8), 2);
    assert_eq!(chase_decode_thread_target(3, 8), 4);
    assert_eq!(chase_decode_thread_target(7, 8), 8);
    assert_eq!(chase_decode_thread_target(500, 8), 8);
    assert_eq!(chase_decode_thread_target(0, 1), 1);
    assert_eq!(chase_decode_thread_target(9, 1), 1);
    assert_eq!(
        chase_decode_thread_target(9, 0),
        1,
        "a ceiling of zero is one"
    );
    assert_eq!(chase_decode_thread_target(usize::MAX, 16), 16);
}

/// A chase's decode reservation is its decoders, end header and allowance:
/// nothing up front for widening, which it pays for a thread at a time.
#[test]
fn chase_decode_reservation_holds_no_widening_room() {
    let archive = sevenz_archive_with_dictionary(
        4 * 1024 * 1024,
        &[("Wide.Dictionary/episode.txt", b"wide dictionary")],
    );
    let parsed = sevenz_turbo::ArchiveReader::new(
        Cursor::new(archive.clone()),
        sevenz_turbo::Password::empty(),
    )
    .expect("parse");
    let end_header = sevenz_end_header_bytes(&archive);
    let decoders =
        crate::pipeline::direct_unpack::decode_memory::decoder_memory_bytes(parsed.archive())
            .expect("sized");
    let needed = decoders + end_header + CHASE_DECODE_ALLOWANCE_BYTES;
    let job_id = JobId(41814);
    let ceiling = 4 * 1024 * 1024 * 1024;

    assert_eq!(
        chase_decode_memory_bytes(job_id, "wide", parsed.archive(), end_header, ceiling),
        SevenZipDecodeReservation::Measured(needed),
        "the decode reservation holds nothing for threads the chase is not using"
    );
    assert_eq!(
        chase_decode_memory_bytes(job_id, "wide", parsed.archive(), end_header, needed - 1),
        SevenZipDecodeReservation::UpToCeiling {
            floor: end_header + CHASE_DECODE_ALLOWANCE_BYTES
        },
        "decoders past the ceiling take what fits up to it, never waiting for all of it"
    );
}

/// Thirty chases starting together on small-dictionary archives fit the
/// process limit side by side. Had each reserved its widening room up front,
/// the last of them would have waited, and every parked chase would have
/// yielded to it.
#[test]
fn thirty_chases_fit_where_up_front_widening_room_would_not() {
    let archive = sevenz_archive_with_dictionary(
        4 * 1024 * 1024,
        &[("Wide.Dictionary/episode.txt", b"wide dictionary")],
    );
    let parsed = sevenz_turbo::ArchiveReader::new(
        Cursor::new(archive.clone()),
        sevenz_turbo::Password::empty(),
    )
    .expect("parse");
    let end_header = sevenz_end_header_bytes(&archive);
    let limit: u64 = 64 * 1024 * 1024 * 1024;
    let decode_threads = 9;
    let SevenZipDecodeReservation::Measured(chase) =
        chase_decode_memory_bytes(JobId(41818), "wide", parsed.archive(), end_header, limit)
    else {
        panic!("a small dictionary is measured, not taken up to the ceiling");
    };

    assert!(
        30 * chase <= limit,
        "thirty chases reserve {} bytes against a {limit}-byte limit",
        30 * chase
    );
    let up_front = chase + chase_widening_bytes(decode_threads);
    assert!(
        30 * up_front > limit,
        "thirty up-front widening reservations would have fit: {}",
        30 * up_front
    );
}

/// Widening room is taken a thread at a time from free memory and stops at
/// the first thread there is none for. Narrowing keeps it, because the
/// decoder's workers outlive a narrowing; it goes back with the decode.
#[test]
fn widening_room_widens_only_as_far_as_free_memory_allows() {
    let temp = TempDir::new().unwrap();
    // 512 MiB less its 128 MiB headroom holds one thread's room, not two.
    let (_root, budget) = test_extraction_security_with_memory(temp.path(), 512 * 1024 * 1024);
    let mut room = WideningRoom::new(Arc::clone(&budget));
    assert_eq!(room.threads(), 1);

    assert_eq!(room.widen_to(4), 2, "room for one thread past the first");
    assert_eq!(
        budget.memory_reserved_bytes(),
        CHASE_WIDENING_BYTES_PER_THREAD
    );
    assert_eq!(room.widen_to(4), 2, "and still no room for a third");

    room.narrow_to(1);
    assert_eq!(room.threads(), 1);
    assert_eq!(
        budget.memory_reserved_bytes(),
        CHASE_WIDENING_BYTES_PER_THREAD,
        "narrowing keeps the room its workers may still be using"
    );
    assert!(
        budget
            .try_reserve_memory(CHASE_WIDENING_BYTES_PER_THREAD)
            .is_none(),
        "so no other decode can take it while they do"
    );
    assert_eq!(room.widen_to(1), 1, "one thread needs no room");
    assert_eq!(room.widen_to(4), 2, "a later widening reuses the held room");
    assert_eq!(
        budget.memory_reserved_bytes(),
        CHASE_WIDENING_BYTES_PER_THREAD
    );

    drop(room);
    assert_eq!(
        budget.memory_reserved_bytes(),
        0,
        "the room goes back when the decode ends"
    );
}

/// An adaptive decode starts on one thread and widens while complete runs
/// wait behind it. Fed a whole multi-run block at once — every run already
/// downloaded, which is the backlog a chase finds after a park — the governor
/// sees runs waiting and widens as far as the memory it can take without
/// waiting pays for; the bytes come out identical either way.
#[test]
fn chase_decode_widens_on_a_backlog_of_complete_runs() {
    let temp = TempDir::new().unwrap();
    let out_dir = temp.path().join("out");
    fs::create_dir_all(&out_dir).unwrap();
    let reel = word_salad(12 * 1024 * 1024, 7);
    let archive = sevenz_multi_run_archive(1024 * 1024, &[("Silver.Horizon/reel.txt", &reel)]);

    // 512 MiB less its 128 MiB headroom is room for one thread past the
    // first; the backlog asks for up to four.
    let (root, budget) = test_extraction_security_with_memory(&out_dir, 512 * 1024 * 1024);
    let root = Arc::new(root);
    let job_id = JobId(41815);
    let mut written = Vec::new();
    // Which of the decode and its governor finishes first is scheduling, and
    // scheduling must not decide a test. The consumer takes one chunk and then
    // holds until the governor has actually read the backlog behind it, so the
    // decode cannot race past the decision this test is about.
    adaptive_probe::watch(job_id.0);
    let mut waited_for_the_governor = false;
    let report = decode_7z_streaming(
        job_id,
        "silver_horizon.7z",
        Cursor::new(archive.clone()),
        &out_dir,
        sevenz_turbo::Password::empty(),
        sevenz_archive_limits(&budget),
        SevenZipDecodeThreads::Adaptive {
            ceiling: 4,
            poll: std::time::Duration::from_millis(1),
            budget: Arc::clone(&budget),
        },
        |entry, reader, _dest| {
            assert_eq!(entry.name(), "Silver.Horizon/reel.txt");
            let mut chunk = vec![0u8; 64 * 1024];
            loop {
                let read = reader.read(&mut chunk)?;
                if read == 0 {
                    break;
                }
                written.extend_from_slice(&chunk[..read]);
                if !waited_for_the_governor {
                    adaptive_probe::wait_for_backlog(job_id.0);
                    waited_for_the_governor = true;
                }
            }
            Ok(true)
        },
    )
    .expect("adaptive decode");
    adaptive_probe::forget(job_id.0);
    assert!(
        waited_for_the_governor,
        "the decode produced no bytes to hold on"
    );

    assert_eq!(written, reel, "a widened decode produces the same bytes");
    assert_eq!(
        report.widest_threads, 2,
        "twelve complete runs fed at once are a backlog the chase widens on, \
         as far as the free memory pays for and no further: {report:?}"
    );
    assert_eq!(
        budget.memory_reserved_bytes(),
        0,
        "the widening room goes back when the decode ends"
    );
    drop(root);
}

/// The chase's own path through `extract_7z_stream`: a per-pass reservation
/// sized from the archive, widening paid for as it goes, and the output
/// identical to a conventional decode.
#[test]
fn chase_7z_extraction_decodes_a_multi_run_block_adaptively() {
    let temp = TempDir::new().unwrap();
    let archive_path = temp.path().join("silver_horizon.7z");
    let out_dir = temp.path().join("out");
    fs::create_dir_all(&out_dir).unwrap();
    let first = word_salad(6 * 1024 * 1024, 11);
    let second = word_salad(3 * 1024 * 1024, 13);
    let archive = sevenz_multi_run_archive(
        1024 * 1024,
        &[
            ("Silver.Horizon/first.txt", &first),
            ("Silver.Horizon/second.txt", &second),
        ],
    );
    let end_header_bytes = sevenz_end_header_bytes(&archive);
    fs::write(&archive_path, &archive).unwrap();

    let (root, budget) = test_extraction_security_with_memory(&out_dir, 256 * 1024 * 1024);
    let (event_tx, _event_rx) = tokio::sync::broadcast::channel(32);
    let context = SevenZipExtractionContext {
        job_id: JobId(41816),
        set_name: "silver_horizon.7z".to_string(),
        output_dir: out_dir.clone(),
        root: Arc::new(root),
        budget: Arc::clone(&budget),
        password: sevenz_turbo::Password::empty(),
        event_tx,
        phase_counters: Arc::new(PhaseCounters::default()),
        decode_memory: SevenZipDecodeMemory::ReservedPerPass { end_header_bytes },
        decode_threads: 4,
    };
    let outcome = extract_7z_stream(&context, || {
        fs::File::open(&archive_path).map_err(|error| error.to_string())
    })
    .expect("7z chase extraction");

    assert_eq!(
        outcome.extracted,
        vec![
            "Silver.Horizon/first.txt".to_string(),
            "Silver.Horizon/second.txt".to_string(),
        ]
    );
    assert_eq!(
        fs::read(out_dir.join("Silver.Horizon/first.txt")).unwrap(),
        first
    );
    assert_eq!(
        fs::read(out_dir.join("Silver.Horizon/second.txt")).unwrap(),
        second
    );
    assert_eq!(
        budget.memory_reserved_bytes(),
        0,
        "the decode pass gives its reservation back"
    );
}

fn phase_counters(total_bytes: u64, completed_bytes: u64) -> Arc<PhaseCounters> {
    let counters = Arc::new(PhaseCounters::default());
    counters.total_bytes.store(total_bytes, Ordering::Relaxed);
    counters
        .completed_bytes
        .store(completed_bytes, Ordering::Relaxed);
    counters
}

fn phase_bytes(counters: &PhaseCounters) -> (u64, u64) {
    (
        counters.total_bytes.load(Ordering::Relaxed),
        counters.completed_bytes.load(Ordering::Relaxed),
    )
}

#[test]
fn chase_mirror_follows_a_finishing_chase_and_withdraws_when_not_installed() {
    // Another set's extraction already has bytes in the phase.
    let phase = phase_counters(100, 40);
    let chase = phase_counters(1_000, 250);
    let mut mirror = ChaseMirror::new(Arc::clone(&phase));

    mirror.sync(&chase);
    assert_eq!(phase_bytes(&phase), (1_100, 290));

    chase.completed_bytes.store(600, Ordering::Relaxed);
    mirror.sync(&chase);
    assert_eq!(phase_bytes(&phase), (1_100, 640));

    // A member that decoded shorter than its header declared gives back the
    // difference.
    chase.total_bytes.store(900, Ordering::Relaxed);
    mirror.sync(&chase);
    assert_eq!(phase_bytes(&phase), (1_000, 640));

    drop(mirror);
    assert_eq!(phase_bytes(&phase), (100, 40));
}

#[test]
fn chase_mirror_settles_installed_bytes_exactly_once() {
    let phase = phase_counters(0, 0);
    let chase = phase_counters(1_000, 500);
    let mut mirror = ChaseMirror::new(Arc::clone(&phase));
    mirror.sync(&chase);

    mirror.settle(1_000, 1_000);
    assert_eq!(phase_bytes(&phase), (1_000, 1_000));

    // A chase that was already done when consumption began is settled
    // without ever being mirrored.
    let phase = phase_counters(0, 0);
    ChaseMirror::new(Arc::clone(&phase)).settle(700, 700);
    assert_eq!(phase_bytes(&phase), (700, 700));
}

/// A zstd frame says in its header how large a window its decoder will hold.
/// Reading it is the difference between a reservation that can be granted
/// and one that can only be granted when the whole process is idle.
#[test]
fn zstd_window_is_read_from_the_frame_header() {
    // Frame header descriptor 0x00: no content size, not single-segment, no
    // dictionary id. Window descriptor 0x00: exponent 0, mantissa 0 — the
    // format's smallest window, 1 KiB.
    let smallest = [0x28, 0xB5, 0x2F, 0xFD, 0x00, 0x00];
    assert_eq!(zstd_frame_window_bytes(&smallest).unwrap(), 1024);

    // Exponent 10, mantissa 3: base 1 MiB plus three eighths of it.
    let window_descriptor = (10u8 << 3) | 3;
    let mid = [0x28, 0xB5, 0x2F, 0xFD, 0x00, window_descriptor];
    let base = 1024 * 1024u64;
    assert_eq!(
        zstd_frame_window_bytes(&mid).unwrap(),
        base + (base / 8) * 3
    );

    // Single-segment frame: no window descriptor, and the one-byte content
    // size is what the decoder holds.
    let single = [0x28, 0xB5, 0x2F, 0xFD, 0b0010_0000, 42];
    assert_eq!(zstd_frame_window_bytes(&single).unwrap(), 42);

    // A window larger than the content it can ever hold is not held.
    let content_size_flag = 0b0100_0000u8;
    let mut capped = vec![0x28, 0xB5, 0x2F, 0xFD, content_size_flag, window_descriptor];
    // Two-byte content size is stored offset by 256.
    capped.extend_from_slice(&(1024u16 - 256).to_le_bytes());
    assert_eq!(zstd_frame_window_bytes(&capped).unwrap(), 1024);

    // Anything that is not a zstd frame, or a header that stops early, is an
    // error rather than a reservation.
    assert!(zstd_frame_window_bytes(&[0x00, 0x01, 0x02, 0x03]).is_err());
    assert!(zstd_frame_window_bytes(&[0x28, 0xB5, 0x2F]).is_err());
    assert!(zstd_frame_window_bytes(&[0x28, 0xB5, 0x2F, 0xFD, 0b0000_1000, 0]).is_err());
}

/// A real frame's reservation has to be small enough to be granted while the
/// rest of the process is working. The old sizing asked for the entire
/// allowance, which registers as a waiter and makes every running direct
/// unpack yield its decoder before this one can even start.
#[test]
fn a_zstd_reservation_is_a_fraction_of_the_process_allowance() {
    let temp = TempDir::new().unwrap();
    let archive = temp.path().join("payload.zst");
    let payload = vec![7u8; 512 * 1024];
    fs::write(
        &archive,
        zstd::stream::encode_all(payload.as_slice(), 3).unwrap(),
    )
    .unwrap();

    let limit = 64_u64 * 1024 * 1024 * 1024;
    let reserved =
        measured_decoder_memory_bytes(SimpleArchiveKind::Zstd, Some(archive.as_path()), limit)
            .unwrap();
    assert!(
        reserved < 64 * 1024 * 1024,
        "a half-megabyte payload must not reserve {reserved} bytes"
    );
    assert!(
        reserved * 1000 < limit,
        "the reservation must leave the allowance usable by everything else"
    );

    // A file that is not a zstd frame fails the extraction instead of
    // reserving the allowance and waiting for it.
    let bogus = temp.path().join("not-really.zst");
    fs::write(&bogus, b"this is not a frame").unwrap();
    assert!(
        measured_decoder_memory_bytes(SimpleArchiveKind::Zstd, Some(bogus.as_path()), limit)
            .is_err()
    );
}

/// A frame whose window is larger than the whole allowance cannot be decoded
/// here, and says so instead of parking forever on a reservation nothing can
/// grant.
#[test]
fn a_zstd_window_above_the_limit_is_refused() {
    // Exponent 21, mantissa 0: a 2 GiB window (the base is 1 KiB shifted by
    // the exponent).
    let huge = [0x28, 0xB5, 0x2F, 0xFD, 0x00, 21u8 << 3];
    assert_eq!(
        zstd_frame_window_bytes(&huge).unwrap(),
        2 * 1024 * 1024 * 1024
    );

    let temp = TempDir::new().unwrap();
    let archive = temp.path().join("wide.zst");
    fs::write(&archive, huge).unwrap();
    assert!(
        measured_decoder_memory_bytes(
            SimpleArchiveKind::Zstd,
            Some(archive.as_path()),
            32 * 1024 * 1024,
        )
        .is_err()
    );
}
