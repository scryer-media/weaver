use std::io::Cursor;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;

use crc32fast::hash as crc32;
use weaver_rar::{ArchiveFormat, ExtractOptions, FileHash, RarArchive, ReadSeek, UnixOwnerInfo};

const LIBARCHIVE_PASSWORD: &str = "password";

fn fixture(dir: &str, name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(dir)
        .join(name)
}

fn open_single(dir: &str, filename: &str) -> RarArchive {
    let data = std::fs::read(fixture(dir, filename)).unwrap();
    RarArchive::open(Cursor::new(data)).unwrap()
}

fn open_single_with_password(dir: &str, filename: &str, password: &str) -> RarArchive {
    let data = std::fs::read(fixture(dir, filename)).unwrap();
    RarArchive::open_with_password(Cursor::new(data), password).unwrap()
}

fn open_multi(dir: &str, filenames: &[&str]) -> RarArchive {
    let readers: Vec<Box<dyn ReadSeek>> = filenames
        .iter()
        .map(|name| {
            let data = std::fs::read(fixture(dir, name)).unwrap();
            Box::new(Cursor::new(data)) as Box<dyn ReadSeek>
        })
        .collect();
    RarArchive::open_volumes(readers).unwrap()
}

fn extract_bytes(archive: &mut RarArchive, index: usize) -> Vec<u8> {
    archive
        .extract_member(index, &ExtractOptions::default(), None)
        .unwrap()
        .into_bytes()
        .unwrap()
}

#[test]
fn imported_archive_flags_are_surfaced() {
    let rar4_locked = open_single("rar4", "ssokolow_rar3_locked.rar");
    let rar4_locked_metadata = rar4_locked.metadata();
    assert_eq!(rar4_locked_metadata.format, ArchiveFormat::Rar4);
    assert!(rar4_locked_metadata.is_locked);
    assert!(rar4_locked.is_locked());
    assert_eq!(rar4_locked.member_names(), vec!["testfile.txt"]);

    let rar5_locked = open_single("rar5", "ssokolow_rar5_locked.rar");
    let rar5_locked_metadata = rar5_locked.metadata();
    assert_eq!(rar5_locked_metadata.format, ArchiveFormat::Rar5);
    assert!(rar5_locked_metadata.is_locked);
    assert!(rar5_locked.is_locked());
    assert_eq!(rar5_locked.member_names(), vec!["testfile.txt"]);

    let rar4_recovery = open_single("rar4", "rar4_recovery.rar");
    assert!(rar4_recovery.metadata().has_recovery_record);
    assert!(rar4_recovery.has_recovery_record());

    let rar5_recovery = open_single("rar5", "rar5_recovery.rar");
    assert!(rar5_recovery.metadata().has_recovery_record);
    assert!(rar5_recovery.has_recovery_record());

    let rarity_check = open_single("rar4", "ssokolow_rar3_authenticity_verification.rar");
    assert_eq!(rarity_check.member_names(), vec!["testfile.txt"]);
}

#[test]
fn imported_sfx_fixtures_open_and_scan() {
    let ssokolow_sfx = [
        ("rar4", "ssokolow_rar3_dos_sfx.exe"),
        ("rar4", "ssokolow_rar3_wincon_sfx.exe"),
        ("rar4", "ssokolow_rar3_wingui_sfx.exe"),
        ("rar5", "ssokolow_rar5_linux_sfx.bin"),
        ("rar5", "ssokolow_rar5_wincon_sfx.exe"),
        ("rar5", "ssokolow_rar5_wingui_sfx.exe"),
    ];

    for (dir, filename) in ssokolow_sfx {
        let archive = open_single(dir, filename);
        assert_eq!(archive.member_names(), vec!["testfile.txt", "acknow.txt"]);
    }

    let mut rar4_sfx = open_single("rar4", "test_read_format_rar_sfx.exe");
    assert_eq!(
        rar4_sfx.member_names(),
        vec![
            "test.txt",
            "testshortcut.lnk",
            "testdir\\test.txt",
            "testdir",
            "testemptydir",
        ]
    );
    assert_eq!(extract_bytes(&mut rar4_sfx, 0), b"test text file\r\n");
    assert_eq!(extract_bytes(&mut rar4_sfx, 2), b"test text file\r\n");

    let mut rar5_sfx = open_single("rar5", "test_read_format_rar5_sfx.exe");
    assert_eq!(rar5_sfx.member_names(), vec!["test.txt.txt"]);
    assert_eq!(extract_bytes(&mut rar5_sfx, 0), b"123");
}

#[test]
fn imported_encrypted_filename_fixtures_extract_with_password() {
    let cases = [
        ("rar4", "test_read_format_rar4_encrypted_filenames.rar"),
        ("rar4", "test_read_format_rar4_solid_encrypted.rar"),
        (
            "rar4",
            "test_read_format_rar4_solid_encrypted_filenames.rar",
        ),
        ("rar5", "test_read_format_rar5_encrypted_filenames.rar"),
        (
            "rar5",
            "test_read_format_rar5_solid_encrypted_filenames.rar",
        ),
    ];
    let expected_names = ["a.txt", "b.txt", "c.txt", "d.txt"];

    for (dir, filename) in cases {
        let mut archive = open_single_with_password(dir, filename, LIBARCHIVE_PASSWORD);
        let metadata = archive.metadata();
        assert_eq!(
            metadata
                .members
                .iter()
                .map(|member| member.name.as_str())
                .collect::<Vec<_>>(),
            expected_names
        );
        assert!(metadata.members.iter().all(|member| member.is_encrypted));

        for (index, expected_name) in expected_names.iter().enumerate() {
            let expected = format!("This is from {expected_name}");
            assert_eq!(extract_bytes(&mut archive, index), expected.as_bytes());
        }
    }
}

#[test]
fn imported_rar5_metadata_and_hash_fixtures_are_exercised() {
    let mut blake2 = open_single("rar5", "test_read_format_rar5_blake2.rar");
    let blake2_metadata = blake2.metadata();
    assert!(matches!(
        blake2_metadata.members[0].hash,
        Some(FileHash::Blake2sp(_))
    ));
    let blake2_bytes = extract_bytes(&mut blake2, 0);
    assert_eq!(blake2_bytes.len(), 814);
    assert_eq!(crc32(&blake2_bytes), 0x7E5E_C49E);

    let hardlink = open_single("rar5", "test_read_format_rar5_hardlink.rar");
    let hardlink_metadata = hardlink.metadata();
    assert_eq!(hardlink_metadata.members[0].name, "file.txt");
    assert_eq!(hardlink_metadata.members[1].name, "hardlink.txt");
    assert!(hardlink_metadata.members[1].is_hardlink);
    assert_eq!(
        hardlink_metadata.members[1].link_target.as_deref(),
        Some("file.txt")
    );

    let owner = open_single("rar5", "test_read_format_rar5_owner.rar");
    let owner_metadata = owner.metadata();
    assert_eq!(
        owner_metadata.members[0].owner,
        Some(UnixOwnerInfo {
            user_name: Some("root".to_string()),
            group_name: Some("wheel".to_string()),
            uid: None,
            gid: None,
        })
    );
    assert_eq!(
        owner_metadata.members[1].owner,
        Some(UnixOwnerInfo {
            user_name: Some("nobody".to_string()),
            group_name: Some("nogroup".to_string()),
            uid: None,
            gid: None,
        })
    );
    assert_eq!(
        owner_metadata.members[2].owner,
        Some(UnixOwnerInfo {
            user_name: None,
            group_name: None,
            uid: Some(9999),
            gid: Some(8888),
        })
    );

    let fileattr = open_single("rar5", "test_read_format_rar5_fileattr.rar");
    let fileattr_metadata = fileattr.metadata();
    let expected_attrs = [
        ("readonly.txt", true, false, false, false),
        ("hidden.txt", false, true, false, false),
        ("system.txt", false, false, true, false),
        ("ro_hidden.txt", true, true, false, false),
        ("dir_readonly", true, false, false, true),
        ("dir_hidden", false, true, false, true),
        ("dir_system", false, false, true, true),
        ("dir_rohidden", true, true, false, true),
    ];
    for (index, (name, readonly, hidden, system, is_dir)) in expected_attrs.into_iter().enumerate()
    {
        let member = &fileattr_metadata.members[index];
        assert_eq!(member.name, name);
        assert_eq!(member.attributes.is_readonly(), readonly);
        assert_eq!(member.attributes.is_hidden(), hidden);
        assert_eq!(member.attributes.is_system(), system);
        assert_eq!(member.is_directory, is_dir);
    }

    let win32 = open_single("rar5", "test_read_format_rar5_win32.rar");
    let win32_metadata = win32.metadata();
    assert!(win32_metadata.members[0].is_directory);
    assert_eq!(win32_metadata.members[3].name, "test2.bin");
    assert!(win32_metadata.members[3].attributes.is_readonly());

    let extra_field_version = open_single("rar5", "test_read_format_rar5_extra_field_version.rar");
    assert_eq!(
        extra_field_version.member_names(),
        vec!["bin/2to3;1", "bin/2to3"]
    );
}

#[test]
fn imported_filter_and_multifile_fixtures_extract() {
    let mut rar4_multifile = open_single("rar4", "rar4_multifile_lz.rar");
    let originals = [
        (
            "hello.txt",
            std::fs::read(fixture("originals", "hello.txt")).unwrap(),
        ),
        (
            "second.txt",
            std::fs::read(fixture("originals", "second.txt")).unwrap(),
        ),
        (
            "zeros_64k.bin",
            std::fs::read(fixture("originals", "zeros_64k.bin")).unwrap(),
        ),
    ];
    for (index, (name, expected)) in originals.into_iter().enumerate() {
        assert_eq!(rar4_multifile.member_names()[index], name);
        assert_eq!(extract_bytes(&mut rar4_multifile, index), expected);
    }

    let stored_manyfiles = open_single("rar5", "test_read_format_rar5_stored_manyfiles.rar");
    assert_eq!(
        stored_manyfiles.member_names(),
        vec!["make_uue.tcl", "cebula.txt", "test.bin"]
    );
    let mut stored_manyfiles = stored_manyfiles;
    let cebula = extract_bytes(&mut stored_manyfiles, 1);
    assert!(cebula.starts_with(b"Cebula"));

    let mut multiarchive = open_multi(
        "rar5",
        &[
            "test_read_format_rar5_multiarchive_solid.part01.rar",
            "test_read_format_rar5_multiarchive_solid.part02.rar",
            "test_read_format_rar5_multiarchive_solid.part03.rar",
            "test_read_format_rar5_multiarchive_solid.part04.rar",
        ],
    );
    assert_eq!(
        multiarchive.member_names(),
        vec![
            "cebula.txt",
            "test.bin",
            "test1.bin",
            "test2.bin",
            "test3.bin",
            "test4.bin",
            "test5.bin",
            "test6.bin",
            "elf-Linux-ARMv7-ls",
        ]
    );
    let first_crc = multiarchive.member_info(0).unwrap().crc32.unwrap();
    let first = extract_bytes(&mut multiarchive, 0);
    assert_eq!(crc32(&first), first_crc);

    let windows = open_single("rar4", "test_read_format_rar_windows.rar");
    assert!(windows.member_names().contains(&"testdir\\test.txt"));
    assert!(windows.member_names().contains(&"testemptydir"));

    let unicode = open_single("rar4", "test_read_format_rar_unicode.rar");
    assert_eq!(unicode.metadata().members.len(), 6);
    assert!(
        unicode
            .metadata()
            .members
            .iter()
            .any(|member| member.name == "abcdefghijklmnopqrsテスト.txt")
    );
}

#[test]
fn imported_filter_and_arm_gap_fixtures_extract() {
    let passing = [
        ("rar4", "test_read_format_rar_filter.rar"),
        ("rar4", "test_read_format_rar_multi_lzss_blocks.rar"),
        ("rar5", "test_read_format_rar5_arm.rar"),
    ];

    for (dir, filename) in passing {
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            corpus_outcome(dir, filename)
        }))
        .unwrap_or_else(|_| panic!("fixture panicked during decode: {dir}/{filename}"));
        assert_eq!(
            outcome,
            CorpusOutcome::Completed,
            "expected successful extract for {dir}/{filename}"
        );
    }
}

#[cfg(feature = "slow-tests")]
#[test]
fn imported_ppmd_transition_fixture_extracts_successfully() {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        corpus_outcome("rar4", "test_read_format_rar_ppmd_lzss_conversion.rar")
    }))
    .unwrap_or_else(|_| {
        panic!("fixture panicked during decode: rar4/test_read_format_rar_ppmd_lzss_conversion.rar")
    });
    assert_eq!(
        outcome,
        CorpusOutcome::Completed,
        "expected successful extract for rar4/test_read_format_rar_ppmd_lzss_conversion.rar"
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CorpusOutcome {
    RejectedOnOpen,
    RejectedOnExtract,
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpectedOutcome {
    Failure,
    Completed,
}

fn corpus_outcome(dir: &str, filename: &str) -> CorpusOutcome {
    let data = std::fs::read(fixture(dir, filename)).unwrap();
    let mut archive = match RarArchive::open(Cursor::new(data)) {
        Ok(archive) => archive,
        Err(_) => return CorpusOutcome::RejectedOnOpen,
    };

    let member_count = archive.metadata().members.len();
    for index in 0..member_count {
        if archive
            .extract_member(index, &ExtractOptions::default(), None)
            .is_err()
        {
            return CorpusOutcome::RejectedOnExtract;
        }
    }

    CorpusOutcome::Completed
}

#[test]
fn malformed_imported_corpus_is_rejected_or_handled_without_panicking() {
    let cases = [
        (
            "rar4",
            "test_read_format_rar_invalid1.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar4",
            "test_read_format_rar_noeof.rar",
            ExpectedOutcome::Completed,
        ),
        (
            "rar4",
            "test_read_format_rar_overflow.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar4",
            "test_read_format_rar_ppmd_use_after_free.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar4",
            "test_read_format_rar_ppmd_use_after_free2.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar4",
            "test_read_format_rar_newsub_huge.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar4",
            "test_read_format_rar_endarc_huge.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar4",
            "test_read_format_rar_symlink_huge.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_bad_tables.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_bad_window_sz_in_mltarc_file.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_block_size_is_too_small.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_data_ready_pointer_leak.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_decode_number_out_of_bounds_read.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_distance_overflow.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_invalid_dict_reference.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_invalid_hash_valid_htime_exfld.rar",
            ExpectedOutcome::Completed,
        ),
        (
            "rar5",
            "test_read_format_rar5_leftshift1.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_leftshift2.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_loop_bug.rar",
            ExpectedOutcome::Completed,
        ),
        (
            "rar5",
            "test_read_format_rar5_only_crypt_exfld.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_readtables_overflow.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_truncated_huff.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_unsupported_exfld.rar",
            ExpectedOutcome::Completed,
        ),
        (
            "rar5",
            "test_read_format_rar5_window_buf_and_size_desync.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_arm_filter_on_window_boundary.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_different_window_size.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_different_solid_window_size.rar",
            ExpectedOutcome::Failure,
        ),
        (
            "rar5",
            "test_read_format_rar5_nonempty_dir_stream.rar",
            ExpectedOutcome::Failure,
        ),
    ];

    for (dir, filename, expected) in cases {
        let outcome = catch_unwind(AssertUnwindSafe(|| corpus_outcome(dir, filename)))
            .unwrap_or_else(|_| panic!("fixture panicked during decode: {dir}/{filename}"));

        match expected {
            ExpectedOutcome::Failure => {
                assert!(
                    matches!(
                        outcome,
                        CorpusOutcome::RejectedOnOpen | CorpusOutcome::RejectedOnExtract
                    ),
                    "expected failure for {dir}/{filename}, got {outcome:?}"
                );
            }
            ExpectedOutcome::Completed => {
                assert_eq!(
                    outcome,
                    CorpusOutcome::Completed,
                    "expected graceful completion for {dir}/{filename}"
                );
            }
        }
    }
}
