#![cfg(feature = "slow-tests")]

//! Adversarial extraction failure paths.
//!
//! Damaged input must fail cleanly: an `Err` from open or extract, never a
//! panic, hang, or silently wrong output (`verify: true` turns wrong bytes
//! into CRC errors). The sweeps intentionally allow `Ok` — truncation or a
//! flipped byte can land in dead space — the invariant under test is "clean
//! result", not "always an error".

use std::fs::{self, File};
use std::io::{Cursor, Write};
use std::path::PathBuf;

const PASSWORD: &str = "testpass123";
const HP_PASSWORD: &str = "secretpass";
#[cfg(feature = "slow-tests")]
const E2E_PASSWORD: &str = "e2e-test-password";

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn fixture_bytes(dir: &str, name: &str) -> Vec<u8> {
    fs::read(fixture_root().join(dir).join(name)).unwrap()
}

/// Open + fully extract every member; the only acceptable outcomes are a
/// clean error anywhere along the way or verified-correct output.
fn extract_all_clean(bytes: Vec<u8>, password: Option<&str>) {
    let Ok(mut archive) = weaver_unrar::RarArchive::open(Cursor::new(bytes)) else {
        return;
    };
    if let Some(password) = password {
        archive.set_password(password);
    }
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: password.map(str::to_owned),
        restore_owners: false,
    };
    let count = archive.metadata().members.len();
    for index in 0..count {
        let _ = archive.extract_member(index, &opts, None);
    }
}

/// Fast default set: one representative per format/mode family.
#[cfg(not(feature = "slow-tests"))]
const DAMAGE_TARGETS: &[(&str, &str, Option<&str>)] = &[
    ("rar4", "rar4_lz.rar", None),
    ("rar4", "rar4_solid.rar", None),
    ("rar4", "rar4_enc_lz.rar", Some(PASSWORD)),
    ("rar4", "rar4_hp_lz.rar", Some(HP_PASSWORD)),
    ("rar5", "rar5_lz.rar", None),
    ("rar5", "rar5_solid.rar", None),
    ("rar5", "rar5_enc_lz.rar", Some(PASSWORD)),
    ("rar5", "rar5_hp_lz.rar", Some(HP_PASSWORD)),
];

/// Full matrix, including the slow PPMd and multi-member solid fixtures.
#[cfg(feature = "slow-tests")]
const DAMAGE_TARGETS: &[(&str, &str, Option<&str>)] = &[
    ("rar4", "rar4_lz.rar", None),
    ("rar4", "rar4_store.rar", None),
    ("rar4", "rar4_solid.rar", None),
    ("rar4", "rar4_lz_solid_mv.rar", None),
    ("rar4", "rar4_ppm_solid_restart.rar", None),
    ("rar4", "rar4_ppm_solid_mv.rar", None),
    ("rar4", "rar4_enc_lz.rar", Some(PASSWORD)),
    ("rar4", "rar4_hp_lz.rar", Some(HP_PASSWORD)),
    ("rar5", "rar5_lz.rar", None),
    ("rar5", "rar5_store.rar", None),
    ("rar5", "rar5_solid.rar", None),
    ("rar5", "rar5_multifile_lz.rar", None),
    ("rar5", "rar5_enc_lz.rar", Some(PASSWORD)),
    ("rar5", "rar5_hp_lz.rar", Some(HP_PASSWORD)),
    ("rar5", "rar5_solid_encrypted.rar", Some(E2E_PASSWORD)),
];

#[test]
fn truncated_archives_fail_cleanly() {
    for (dir, name, password) in DAMAGE_TARGETS {
        let full = fixture_bytes(dir, name);
        let len = full.len();

        let mut cuts = vec![len / 4, len / 2, len - 1];
        // Header-region cuts: signature and first block boundaries.
        cuts.extend((0..24).step_by(4));
        for cut in cuts {
            extract_all_clean(full[..cut.min(len)].to_vec(), *password);
        }
    }
}

#[test]
fn corrupted_archives_fail_cleanly() {
    for (dir, name, password) in DAMAGE_TARGETS {
        let full = fixture_bytes(dir, name);
        let len = full.len();

        // Header region and spread through the data area.
        #[cfg(not(feature = "slow-tests"))]
        let positions = [40, len / 2];
        #[cfg(feature = "slow-tests")]
        let positions = [16, 40, len / 4, len / 2, len * 3 / 4, len - 8];
        for &pos in &positions {
            let pos = pos.min(len - 1);
            for flip in [0xFFu8, 0x01] {
                let mut damaged = full.clone();
                damaged[pos] ^= flip;
                extract_all_clean(damaged, *password);
            }
        }
    }
}

#[test]
fn multivolume_truncated_last_volume_fails_cleanly() {
    for (dir, prefix, parts, password) in [
        ("rar4", "rar4_mv_video", 5, None),
        ("rar5", "rar5_mv_video", 5, None),
        ("rar5", "rar5_enc_mv_video", 5, Some(PASSWORD)),
    ] {
        let mut volumes: Vec<Vec<u8>> = (1..=parts)
            .map(|part| fixture_bytes(dir, &format!("{prefix}.part{part}.rar")))
            .collect();
        let last = volumes.last_mut().unwrap();
        last.truncate(last.len() / 2);

        let readers: Vec<Box<dyn weaver_unrar::ReadSeek>> = volumes
            .into_iter()
            .map(|bytes| Box::new(Cursor::new(bytes)) as Box<dyn weaver_unrar::ReadSeek>)
            .collect();
        let Ok(mut archive) = weaver_unrar::RarArchive::open_volumes(readers) else {
            continue;
        };
        if let Some(password) = password {
            archive.set_password(password);
        }
        let opts = weaver_unrar::ExtractOptions {
            verify: true,
            password: password.map(str::to_owned),
            restore_owners: false,
        };
        let count = archive.metadata().members.len();
        for index in 0..count {
            let _ = archive.extract_member(index, &opts, None);
        }
    }
}

#[test]
fn multivolume_extraction_without_later_volumes_errors() {
    for (dir, first, password) in [
        ("rar4", "rar4_mv_video.part1.rar", None),
        ("rar5", "rar5_mv_video.part1.rar", None),
        ("rar5", "rar5_enc_mv_video.part1.rar", Some(PASSWORD)),
    ] {
        let bytes = fixture_bytes(dir, first);
        let mut archive = weaver_unrar::RarArchive::open(Cursor::new(bytes)).unwrap();
        if let Some(password) = password {
            archive.set_password(password);
        }
        let opts = weaver_unrar::ExtractOptions {
            verify: true,
            password: password.map(str::to_owned),
            restore_owners: false,
        };

        let result = archive.extract_member(0, &opts, None);
        assert!(
            result.is_err(),
            "{dir}/{first}: extracting a member spanning absent volumes must error"
        );

        // Streaming with a provider that only knows the first volume.
        let provider = weaver_unrar::StaticVolumeProvider::from_ordered(vec![
            fixture_root().join(dir).join(first),
        ]);
        let mut sink = Vec::new();
        let result = archive.extract_member_streaming(0, &opts, &provider, &mut sink);
        assert!(
            result.is_err(),
            "{dir}/{first}: streaming without later volumes must error"
        );
    }
}

#[test]
fn wrong_password_fails_across_encryption_modes() {
    // Data-encrypted RAR4: headers list fine, extraction must fail.
    let bytes = fixture_bytes("rar4", "rar4_enc_lz.rar");
    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(bytes)).unwrap();
    archive.set_password("not-the-password");
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: Some("not-the-password".into()),
        restore_owners: false,
    };
    assert!(
        archive.extract_member(0, &opts, None).is_err(),
        "rar4_enc_lz: wrong password must fail extraction"
    );

    // Missing password on encrypted data must also fail, not return garbage.
    let bytes = fixture_bytes("rar4", "rar4_enc_lz.rar");
    let mut archive = weaver_unrar::RarArchive::open(Cursor::new(bytes)).unwrap();
    let no_pw = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };
    assert!(
        archive.extract_member(0, &no_pw, None).is_err(),
        "rar4_enc_lz: extraction without a password must fail"
    );

    // Header-encrypted archives: the wrong password must surface an error at
    // open or at extraction, never a panic or silent success.
    for (dir, name) in [("rar4", "rar4_hp_lz.rar"), ("rar5", "rar5_hp_lz.rar")] {
        let path = fixture_root().join(dir).join(name);
        let file = File::open(&path).unwrap();
        match weaver_unrar::RarArchive::open_with_password(file, "not-the-password") {
            Err(err) => {
                assert!(
                    !matches!(err, weaver_unrar::RarError::Io(_)),
                    "{dir}/{name}: wrong password must not surface as a bare IO error: {err}"
                );
            }
            Ok(mut archive) => {
                let opts = weaver_unrar::ExtractOptions {
                    verify: true,
                    password: Some("not-the-password".into()),
                    restore_owners: false,
                };
                let count = archive.metadata().members.len();
                let mut any_err = count == 0;
                for index in 0..count {
                    if archive.extract_member(index, &opts, None).is_err() {
                        any_err = true;
                    }
                }
                assert!(
                    any_err,
                    "{dir}/{name}: wrong password produced successful extraction"
                );
            }
        }
    }
}

/// Byte-identical output through a writer that fails must surface the IO
/// error instead of being swallowed.
#[test]
fn writer_errors_propagate_from_streaming_extraction() {
    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("sink exploded"))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let path = fixture_root().join("rar5").join("rar5_lz.rar");
    let mut archive = weaver_unrar::RarArchive::open(File::open(&path).unwrap()).unwrap();
    let opts = weaver_unrar::ExtractOptions {
        verify: true,
        password: None,
        restore_owners: false,
    };
    let provider = weaver_unrar::StaticVolumeProvider::from_ordered(vec![path]);
    let result = archive.extract_member_streaming(0, &opts, &provider, &mut FailingWriter);
    assert!(
        result.is_err(),
        "failing writer must propagate an error from streaming extraction"
    );
}
