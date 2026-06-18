use weaver_server_core::ingest::{
    hash_persisted_nzb_bytes, load_persisted_nzb_storage_bytes, parse_persisted_nzb_bytes,
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

fn finalize_blake3(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_bytes());
    out
}

#[test]
fn parse_persisted_zstd_nzb_bytes() {
    let xml = minimal_nzb("persisted-zstd");
    let compressed = zstd::bulk::compress(xml.as_bytes(), 3).unwrap();

    let nzb = parse_persisted_nzb_bytes(&compressed).unwrap();

    assert_eq!(nzb.files.len(), 1);
    assert_eq!(
        nzb.files[0].subject,
        "persisted-zstd - \"file.rar\" yEnc (1/1)"
    );
}

#[test]
fn parse_persisted_plain_nzb_bytes() {
    let xml = minimal_nzb("persisted-plain");

    let nzb = parse_persisted_nzb_bytes(xml.as_bytes()).unwrap();

    assert_eq!(nzb.files.len(), 1);
    assert_eq!(
        nzb.files[0].subject,
        "persisted-plain - \"file.rar\" yEnc (1/1)"
    );
}

#[test]
fn parse_persisted_corrupt_zstd_bytes_fails() {
    let mut bytes = ZSTD_MAGIC.to_vec();
    bytes.extend_from_slice(b"not-a-valid-frame");

    assert!(parse_persisted_nzb_bytes(&bytes).is_err());
}

#[test]
fn load_persisted_nzb_storage_bytes_compresses_legacy_plain_file() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = temp_dir.path().join("job.nzb");
    let xml = minimal_nzb("legacy-file-backed");
    std::fs::write(&path, xml.as_bytes()).unwrap();

    let bytes = load_persisted_nzb_storage_bytes(&path).unwrap();
    let nzb = parse_persisted_nzb_bytes(&bytes).unwrap();

    assert_ne!(bytes, xml.as_bytes());
    assert_eq!(
        nzb.files[0].subject,
        "legacy-file-backed - \"file.rar\" yEnc (1/1)"
    );
}

#[test]
fn hash_persisted_nzb_bytes_matches_plain_and_zstd_storage() {
    let xml = minimal_nzb("persisted-hash-bytes");
    let compressed = zstd::bulk::compress(xml.as_bytes(), 3).unwrap();
    let expected = finalize_blake3(xml.as_bytes());

    assert_eq!(hash_persisted_nzb_bytes(xml.as_bytes()), expected);
    assert_eq!(hash_persisted_nzb_bytes(&compressed), expected);
}
