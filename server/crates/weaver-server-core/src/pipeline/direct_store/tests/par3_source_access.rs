use super::*;
use crate::pipeline::repair::par3::virtual_source::{ReaderCache, VirtualInput, VirtualSource};
use par3_rs::runtime::{EngineError, ExecutionOptions, HandleBudget};
use par3_rs::source::{SourceAccess, SourceId};
use std::io::Read;

const SOURCE: SourceId = SourceId(7);

fn access(
    volume: super::super::provider::VirtualVolume,
    options: &ExecutionOptions,
    cache: &Arc<ReaderCache>,
) -> VirtualSource {
    VirtualSource::new(
        SOURCE,
        VirtualInput::new(volume, options).unwrap(),
        options.clone(),
        Arc::clone(cache),
    )
    .unwrap()
}

#[test]
fn par3_virtual_source_exposes_an_honest_prefix_and_reads_beyond_interior_holes() {
    let mut coverage = ByteRanges::new();
    coverage.insert(0, 300);
    coverage.insert(500, 100_000);
    let fixture = provider_fixture(coverage);
    let options = ExecutionOptions::default();
    let cache = Arc::new(ReaderCache::default());
    let source = access(fixture.volume.clone(), &options, &cache);
    assert_eq!(
        source.next_available(SOURCE, 300).unwrap(),
        Some(500..fixture.volume.len)
    );
    let mut sequential = source.open_sequential(SOURCE).unwrap().unwrap();
    let mut prefix = Vec::new();
    sequential.read_to_end(&mut prefix).unwrap();
    assert_eq!(prefix, fixture.conventional[..300]);
    let mut bytes = [0xAA; 64];
    assert_eq!(source.read_at(SOURCE, 300, &mut bytes).unwrap(), 0);
    assert_eq!(bytes, [0xAA; 64], "holes never synthesize zero padding");
    let count = source.read_at(SOURCE, 500, &mut bytes).unwrap();
    assert!(count > 0);
    assert_eq!(&bytes[..count], &fixture.conventional[500..500 + count]);
    drop(sequential);
    drop(source);
    drop(cache);
    assert_eq!(options.handles.used(), 0);
}

#[test]
fn par3_encrypted_ranged_reads_keep_the_cipher_frontier_and_posted_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let (posted, plain, crypt, covered) = encrypted_member_facts(64 * 1024, 4096);
    let facts = crypt.cipher_facts(plain.len() as u64, &covered).unwrap();
    let volume = cipher_volume(dir.path(), &plain, facts, plain.len() as u64);
    let mut options = ExecutionOptions::default();
    options.handles = HandleBudget::new(3);
    let cache = Arc::new(ReaderCache::default());
    let source = access(volume, &options, &cache);
    let counters = source.cipher_counters();
    let mut actual = vec![0; plain.len()];
    for offset in (0..actual.len()).step_by(512) {
        let end = (offset + 512).min(actual.len());
        let count = source
            .read_at(SOURCE, offset as u64, &mut actual[offset..end])
            .unwrap();
        assert_eq!(count, end - offset);
    }
    assert_eq!(actual, posted[..plain.len()]);
    assert_eq!(counters.chained_bytes(), 0);
    assert_eq!(counters.refusals(), 0);
    assert!(options.handles.peak() <= 3);
    drop(source);
    drop(cache);
    assert_eq!(options.handles.used(), 0);
}

#[test]
fn par3_virtual_publication_detects_same_length_backing_changes() {
    let mut coverage = ByteRanges::new();
    coverage.insert(0, 100_000);
    let fixture = provider_fixture(coverage);
    let options = ExecutionOptions::default();
    let cache = Arc::new(ReaderCache::default());
    let source = access(fixture.volume.clone(), &options, &cache);
    let before = source.snapshot(SOURCE).unwrap();
    assert!(before.is_some());
    let partial = fixture.volume.partials.values().next().unwrap();
    let mut bytes = std::fs::read(partial).unwrap();
    bytes[0] ^= 1;
    std::fs::write(partial, bytes).unwrap();
    assert!(matches!(
        EngineError::from(source.snapshot(SOURCE).unwrap_err()),
        EngineError::SourceChanged(SOURCE)
    ));
    assert!(source.read_at(SOURCE, 0, &mut [0; 16]).is_err());
}

#[test]
fn par3_virtual_reader_limits_and_cancellation_remain_typed() {
    let mut coverage = ByteRanges::new();
    coverage.insert(0, 100_000);
    let fixture = provider_fixture(coverage);
    let mut options = ExecutionOptions::default();
    options.handles = HandleBudget::new(1);
    let cache = Arc::new(ReaderCache::default());
    let source = access(fixture.volume, &options, &cache);
    assert!(matches!(
        EngineError::from(source.read_at(SOURCE, 0, &mut [0; 16]).unwrap_err()),
        EngineError::ResourceLimit(_)
    ));
    assert_eq!(
        options.handles.used(),
        0,
        "failed reader admission releases its first lease"
    );
    options.cancel.cancel();
    assert!(matches!(
        EngineError::from(source.snapshot(SOURCE).unwrap_err()),
        EngineError::Cancelled
    ));
}

#[test]
fn par3_replacing_a_virtual_source_cannot_reuse_the_old_reader_image() {
    let mut coverage = ByteRanges::new();
    coverage.insert(0, 100_000);
    let fixture = provider_fixture(coverage.clone());
    let replacement = provider_fixture(coverage);
    let options = ExecutionOptions::default();
    let cache = Arc::new(ReaderCache::default());
    let source = access(fixture.volume, &options, &cache);
    let mut bytes = [0; 16];
    source.read_at(SOURCE, 0, &mut bytes).unwrap();
    let mut different = std::fs::read(&replacement.volume.envelope).unwrap();
    different[..16].fill(0xAB);
    std::fs::write(&replacement.volume.envelope, different).unwrap();
    let replacement = access(replacement.volume, &options, &cache);
    assert_eq!(replacement.read_at(SOURCE, 0, &mut bytes).unwrap(), 16);
    assert_eq!(bytes, [0xAB; 16]);
    assert_eq!(source.read_at(SOURCE, 0, &mut bytes).unwrap(), 16);
    assert_eq!(bytes, fixture.conventional[..16]);
}
