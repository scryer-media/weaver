use std::collections::HashSet;
use std::io::Write;
use std::path::PathBuf;

use sha2::{Digest, Sha256};
use weaver_server_core::ingest::{
    cleanup_orphaned_persisted_nzbs, hash_persisted_nzb, parse_persisted_nzb,
};

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

fn minimal_nzb(name: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="test@test.com" date="1234567890" subject="{name} - &quot;file.rar&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="500000" number="1">{name}-seg1@test.com</segment></segments>
  </file>
</nzb>"#
    )
}

fn finalize_sha256(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    out
}

#[test]
fn parse_persisted_zstd_nzb() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("job.nzb");
    let xml = minimal_nzb("persisted-zstd");
    let compressed = zstd::bulk::compress(xml.as_bytes(), 3).unwrap();
    std::fs::write(&path, compressed).unwrap();

    let nzb = parse_persisted_nzb(&path).unwrap();

    assert_eq!(nzb.files.len(), 1);
    assert_eq!(
        nzb.files[0].subject,
        "persisted-zstd - \"file.rar\" yEnc (1/1)"
    );
}

#[test]
fn parse_persisted_plain_nzb() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("job.nzb");
    std::fs::write(&path, minimal_nzb("persisted-plain")).unwrap();

    let nzb = parse_persisted_nzb(&path).unwrap();

    assert_eq!(nzb.files.len(), 1);
    assert_eq!(
        nzb.files[0].subject,
        "persisted-plain - \"file.rar\" yEnc (1/1)"
    );
}

#[test]
fn parse_persisted_corrupt_zstd_fails() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("job.nzb");
    let mut file = std::fs::File::create(&path).unwrap();
    file.write_all(&ZSTD_MAGIC).unwrap();
    file.write_all(b"not-a-valid-frame").unwrap();
    file.flush().unwrap();

    assert!(parse_persisted_nzb(&path).is_err());
}

#[test]
fn hash_persisted_nzb_matches_stored_bytes_hash() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("job.nzb");
    let compressed = zstd::bulk::compress(minimal_nzb("persisted-hash").as_bytes(), 3).unwrap();
    std::fs::write(&path, &compressed).unwrap();

    let expected = finalize_sha256(&compressed);

    assert_eq!(hash_persisted_nzb(&path).unwrap(), expected);
}

#[test]
fn cleanup_orphaned_persisted_nzbs_removes_only_unreferenced_files() {
    let tempdir = tempfile::tempdir().unwrap();
    let nzb_dir = tempdir.path().join(".weaver-nzbs");
    std::fs::create_dir_all(&nzb_dir).unwrap();

    let kept = nzb_dir.join("kept.nzb");
    let removed = nzb_dir.join("removed.nzb");
    std::fs::write(&kept, b"kept").unwrap();
    std::fs::write(&removed, b"removed").unwrap();

    let referenced = HashSet::from([PathBuf::from(&kept)]);
    let removed_count = cleanup_orphaned_persisted_nzbs(&nzb_dir, &referenced).unwrap();

    assert_eq!(removed_count, 1);
    assert!(kept.exists());
    assert!(!removed.exists());
}
