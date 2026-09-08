use super::*;

#[test]
fn zip64_offset_sentinel_without_end_record_is_rejected() {
    let tmp = TempDir::new().unwrap();
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    writer
        .start_file("payload.bin", SimpleFileOptions::default().large_file(true))
        .unwrap();
    writer.write_all(b"payload").unwrap();
    let mut bytes = writer.finish().unwrap().into_inner();
    let end = bytes.len() - 22;
    assert_eq!(&bytes[end..end + 4], b"PK\x05\x06");
    bytes[end + 16..end + 20].copy_from_slice(&u32::MAX.to_le_bytes());
    let archive = tmp.path().join("invalid.zip");
    fs::write(&archive, bytes).unwrap();
    let out = tmp.path().join("out");
    fs::create_dir(&out).unwrap();
    assert!(extract_with_weaver_zip_result(&archive, &out, None).is_err());
    assert!(!out.join("payload.bin").exists());
}

fn digest_file(path: &Path) -> blake3::Hash {
    let mut file = fs::File::open(path).unwrap();
    let mut hash = blake3::Hasher::new();
    let mut chunk = vec![0; 1024 * 1024];
    loop {
        let len = file.read(&mut chunk).unwrap();
        if len == 0 {
            break;
        }
        hash.update(&chunk[..len]);
    }
    hash.finalize()
}

fn write_payload(path: &Path, mebibytes: usize) -> blake3::Hash {
    let mut file = fs::File::create(path).unwrap();
    let chunk: Vec<u8> = (0..1024 * 1024).map(|i| (i % 251) as u8).collect();
    let mut hash = blake3::Hasher::new();
    for _ in 0..mebibytes {
        file.write_all(&chunk).unwrap();
        hash.update(&chunk);
    }
    hash.finalize()
}

#[test]
#[ignore = "external Info-ZIP writer; 150 MiB fixture"]
fn zip64_infozip_forced_and_streamed_150_mib() {
    let tmp = TempDir::new().unwrap();
    let expected = write_payload(&tmp.path().join("payload.bin"), 150);
    for streamed in [false, true] {
        let archive_path = tmp.path().join(if streamed {
            "streamed.zip"
        } else {
            "forced.zip"
        });
        let mut command = Command::new("zip");
        command.current_dir(tmp.path()).args(["-q", "-0"]);
        if streamed {
            // An unknown-length stdin member selects ZIP64 automatically.
            // Apple's Info-ZIP emits an invalid EOCD with -fz to a pipe.
            command
                .args(["-", "-"])
                .stdin(fs::File::open(tmp.path().join("payload.bin")).unwrap())
                .stdout(std::process::Stdio::piped());
            let mut child = command.spawn().expect("Info-ZIP must be installed");
            std::io::copy(
                &mut child.stdout.take().unwrap(),
                &mut fs::File::create(&archive_path).unwrap(),
            )
            .unwrap();
            assert!(child.wait().unwrap().success());
        } else {
            command.arg("-fz").arg(&archive_path).arg("payload.bin");
            assert!(
                command
                    .status()
                    .expect("Info-ZIP must be installed")
                    .success()
            );
        }
        let mut header = [0u8; 30];
        fs::File::open(&archive_path)
            .unwrap()
            .read_exact(&mut header)
            .unwrap();
        assert_eq!(&header[18..26], &[0xff; 8], "forced ZIP64 size sentinels");
        if streamed {
            assert_ne!(
                u16::from_le_bytes([header[6], header[7]]) & 8,
                0,
                "pipe writer must use a data descriptor"
            );
        }
        let out = tmp.path().join(if streamed {
            "out-streamed"
        } else {
            "out-forced"
        });
        fs::create_dir(&out).unwrap();
        extract_with_weaver_zip_result(&archive_path, &out, None).unwrap();
        assert_eq!(
            digest_file(&out.join(if streamed { "-" } else { "payload.bin" })),
            expected
        );
    }
}

#[test]
#[ignore = "5 GiB member and offsets beyond 4 GiB; requires 16 GiB free disk and external writers"]
fn zip64_real_5_gib_member_and_large_offsets_from_both_writers() {
    use std::io::{Seek, SeekFrom};
    let tmp = TempDir::new().unwrap();
    let expected = write_payload(&tmp.path().join("a-payload.bin"), 5 * 1024);
    fs::write(tmp.path().join("z-tail.txt"), b"member after 4 GiB").unwrap();
    for writer in ["zip", "7z"] {
        let archive_path = tmp.path().join("large.zip");
        let mut command = Command::new(writer);
        command.current_dir(tmp.path());
        if writer == "zip" {
            command.args(["-q", "-0", "large.zip", "a-payload.bin", "z-tail.txt"]);
        } else {
            command
                .args([
                    "a",
                    "-tzip",
                    "-mx0",
                    "large.zip",
                    "a-payload.bin",
                    "z-tail.txt",
                ])
                .stdout(std::process::Stdio::null());
        }
        assert!(
            command
                .status()
                .expect("ZIP fixture writer must be installed")
                .success()
        );
        let archive_len = fs::metadata(&archive_path).unwrap().len();
        assert!(archive_len > u64::from(u32::MAX));
        let mut file = fs::File::open(&archive_path).unwrap();
        file.seek(SeekFrom::End(-128)).unwrap();
        let mut end = Vec::new();
        file.read_to_end(&mut end).unwrap();
        assert!(
            end.windows(4).any(|bytes| bytes == b"PK\x06\x06"),
            "ZIP64 EOCD required"
        );
        let out = tmp.path().join("out");
        fs::create_dir(&out).unwrap();
        let coverage = Arc::new(crate::pipeline::direct_unpack::coverage::SetCoverage::new(
            1,
        ));
        coverage.set_total_len(archive_len);
        coverage.advance_watermark(0, 16 * 1024 * 1024);
        coverage.note_committed_range(0, archive_len - 1024 * 1024, archive_len);
        let reader = crate::pipeline::direct_unpack::reader::GatedSplitReader::open(
            std::slice::from_ref(&archive_path),
            Arc::clone(&coverage),
        )
        .unwrap();
        let (root, budget) = test_extraction_security(&out);
        let (events, _) = tokio::sync::broadcast::channel(1);
        let worker = std::thread::spawn(move || {
            extract_zip_stream(
                std::io::BufReader::new(reader),
                &root,
                &budget,
                None,
                &events,
                JobId(1),
                "large.zip",
                None,
            )
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let overlap = loop {
            if fs::metadata(out.join("a-payload.bin")).is_ok_and(|m| m.len() >= 1024 * 1024) {
                break true;
            }
            if worker.is_finished() || std::time::Instant::now() >= deadline {
                break false;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        };
        coverage.advance_watermark(0, archive_len);
        coverage.mark_part_complete(0);
        worker.join().unwrap().unwrap();
        assert!(
            overlap,
            "large ZIP64 must unpack before its middle is readable"
        );
        assert_eq!(
            fs::metadata(out.join("a-payload.bin")).unwrap().len(),
            5 * 1024 * 1024 * 1024
        );
        assert_eq!(digest_file(&out.join("a-payload.bin")), expected);
        assert_eq!(
            fs::read(out.join("z-tail.txt")).unwrap(),
            b"member after 4 GiB"
        );
        fs::remove_dir_all(out).unwrap();
        fs::remove_file(archive_path).unwrap();
    }
}
