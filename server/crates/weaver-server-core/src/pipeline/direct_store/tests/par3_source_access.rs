use super::*;
use crate::pipeline::repair::par3::virtual_source::{ReaderCache, VirtualInput, VirtualSource};
use par3_rs::runtime::{EngineError, ExecutionOptions, HandleBudget};
use par3_rs::source::{SourceAccess, SourceId};
use std::io::Read;

const SOURCE: SourceId = SourceId(7);

/// Handles a freshness check holds while it runs. On Windows it opens the
/// backing file to read its fence, so an idle reader can keep only what leaves
/// the check room.
const SNAPSHOT_CHECK_HANDLES: usize = if cfg!(windows) { 1 } else { 0 };

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
fn par3_large_shared_held_images_and_readers_release_every_payload_owner() {
    use super::super::provider::HeldRun;
    let bytes: Arc<[u8]> = vec![0x5a; 24 << 20].into();
    let weak = Arc::downgrade(&bytes);
    let mut volume = provider_fixture(ByteRanges::new()).volume;
    volume.len = bytes.len() as u64;
    volume.extents.clear();
    volume.partials = Arc::default();
    volume.held = Arc::new(vec![
        HeldRun::memory(0, Arc::clone(&bytes), 0, 12 << 20),
        HeldRun::memory(12 << 20, Arc::clone(&bytes), 12 << 20, 12 << 20),
    ]);
    let options = ExecutionOptions::default();
    let cache = Arc::new(ReaderCache::default());
    let first = access(volume.clone(), &options, &cache);
    let mut reader = first.open_sequential(SOURCE).unwrap().unwrap();
    let second = access(volume, &options, &cache);
    let mut buffer = [0; 64];
    assert_eq!(second.read_at(SOURCE, 20 << 20, &mut buffer).unwrap(), 64);
    assert_eq!(buffer, [0x5a; 64]);
    drop(first);
    drop(second);
    drop(cache);
    drop(bytes);
    assert!(
        weak.upgrade().is_some(),
        "the sequential reader still owns the allocation"
    );
    reader.read_exact(&mut buffer).unwrap();
    assert_eq!(buffer, [0x5a; 64]);
    options.cancel.cancel();
    assert!(reader.read(&mut buffer).is_err());
    drop(reader);
    assert!(weak.upgrade().is_none());
    assert_eq!(options.handles.used(), 0);
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
fn par3_interleaved_encrypted_sources_keep_independent_frontiers() {
    let first_dir = tempfile::tempdir().unwrap();
    let second_dir = tempfile::tempdir().unwrap();
    let (posted, plain, crypt, covered) = encrypted_member_facts(256 << 10, 4096);
    let facts = crypt.cipher_facts(plain.len() as u64, &covered).unwrap();
    let mut options = ExecutionOptions::default();
    options.handles = HandleBudget::new(6);
    let cache = Arc::new(ReaderCache::default());
    let first = access(
        cipher_volume(first_dir.path(), &plain, facts.clone(), plain.len() as u64),
        &options,
        &cache,
    );
    let volume = cipher_volume(second_dir.path(), &plain, facts, plain.len() as u64);
    let second = VirtualSource::new(
        SourceId(8),
        VirtualInput::new(volume, &options).unwrap(),
        options.clone(),
        cache.clone(),
    )
    .unwrap();
    for offset in (0..plain.len()).step_by(4096) {
        for (source, id) in [(&first, SOURCE), (&second, SourceId(8))] {
            let mut bytes = vec![0; (plain.len() - offset).min(4096)];
            assert_eq!(
                source.read_at(id, offset as u64, &mut bytes).unwrap(),
                bytes.len()
            );
            assert_eq!(bytes, posted[offset..offset + bytes.len()]);
        }
    }
    assert_eq!(first.cipher_counters().chained_bytes(), 0);
    assert_eq!(second.cipher_counters().chained_bytes(), 0);
    drop(first);
    drop(second);
    assert_eq!(options.handles.used(), 0);
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
fn par3_idle_cache_is_bounded_and_evicts_before_handle_fallback() {
    for limit in [2, 64] {
        let mut coverage = ByteRanges::new();
        coverage.insert(0, 100_000);
        let fixture = provider_fixture(coverage);
        let mut options = ExecutionOptions::default();
        options.handles = HandleBudget::new(limit);
        // Production pairs the ceiling with the per-open cap. Windows hashes a
        // backing file through that cap as each source is built, and the
        // default cap would already be spent by a full idle cache.
        options.open_handles = limit;
        let cache = Arc::new(ReaderCache::default());
        let mut sources = Vec::new();
        for number in 0..20 {
            let id = SourceId(number);
            let source = VirtualSource::new(
                id,
                VirtualInput::new(fixture.volume.clone(), &options).unwrap(),
                options.clone(),
                cache.clone(),
            )
            .unwrap();
            let mut bytes = [0; 16];
            source.read_at(id, 0, &mut bytes).unwrap();
            assert_eq!(bytes, fixture.conventional[..16]);
            sources.push(source);
            assert_eq!(
                options.handles.used(),
                (sources.len().min(16) * 2).min((limit - SNAPSHOT_CHECK_HANDLES) / 2 * 2)
            );
        }
        // The oldest evicted publication can still reopen safely.
        assert_eq!(
            sources[0].read_at(SourceId(0), 0, &mut [0; 16]).unwrap(),
            16
        );
        drop(sources);
        assert_eq!(options.handles.used(), 0);
    }
}

#[test]
fn par3_cache_pressure_never_reclaims_an_in_use_reader() {
    let mut coverage = ByteRanges::new();
    coverage.insert(0, 100_000);
    let fixture = provider_fixture(coverage);
    let mut options = ExecutionOptions::default();
    options.handles = HandleBudget::new(2);
    let cache = Arc::new(ReaderCache::default());
    let source = access(fixture.volume.clone(), &options, &cache);
    let mut active = source.open_sequential(SOURCE).unwrap().unwrap();
    assert!(matches!(
        EngineError::from(source.read_at(SOURCE, 0, &mut [0; 16]).unwrap_err()),
        EngineError::ResourceLimit(_)
    ));
    let mut bytes = [0; 16];
    active.read_exact(&mut bytes).unwrap();
    assert_eq!(bytes, fixture.conventional[..16]);
    drop(active);
    source.read_at(SOURCE, 0, &mut bytes).unwrap();
    drop(source);
    assert_eq!(options.handles.used(), 0);
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
#[test]
#[ignore = "explicit encrypted-read performance counter experiment"]
fn par3_encrypted_interleave_counter_experiment() {
    for stripe in [64 << 10, 1 << 20] {
        let first_dir = tempfile::tempdir().unwrap();
        let second_dir = tempfile::tempdir().unwrap();
        let (posted, plain, crypt, covered) = encrypted_member_facts(4 << 20, 4096);
        let facts = crypt.cipher_facts(plain.len() as u64, &covered).unwrap();
        let options = ExecutionOptions::default();
        let cache = Arc::new(ReaderCache::default());
        let first = access(
            cipher_volume(first_dir.path(), &plain, facts.clone(), plain.len() as u64),
            &options,
            &cache,
        );
        let second = VirtualSource::new(
            SourceId(8),
            VirtualInput::new(
                cipher_volume(second_dir.path(), &plain, facts, plain.len() as u64),
                &options,
            )
            .unwrap(),
            options.clone(),
            cache.clone(),
        )
        .unwrap();
        let mut requested = 0u64;
        for offset in (0..plain.len()).step_by(stripe) {
            for (source, id) in [(&first, SOURCE), (&second, SourceId(8))] {
                let mut bytes = vec![0; (plain.len() - offset).min(stripe)];
                let mut read = 0;
                while read < bytes.len() {
                    requested += (bytes.len() - read) as u64;
                    let count = source
                        .read_at(id, (offset + read) as u64, &mut bytes[read..])
                        .unwrap();
                    assert!(count != 0);
                    read += count;
                }
                assert_eq!(bytes, posted[offset..offset + bytes.len()]);
            }
        }
        println!(
            "PAR3_READER_SAMPLE stripe={stripe} requested={requested} reencrypted={} chained={}",
            first.cipher_counters().reencrypted_bytes()
                + second.cipher_counters().reencrypted_bytes(),
            first.cipher_counters().chained_bytes() + second.cipher_counters().chained_bytes()
        );
    }
}
