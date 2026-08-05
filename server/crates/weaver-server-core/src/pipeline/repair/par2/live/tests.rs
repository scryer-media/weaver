use super::*;

use std::collections::BTreeMap;

const JOB: JobId = JobId(9001);

fn file(index: u32) -> NzbFileId {
    NzbFileId {
        job_id: JOB,
        file_index: index,
    }
}

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|value| (value % 251) as u8).collect()
}

fn par2_file_id(filename: &str, data: &[u8]) -> par2_rs::FileId {
    let mut input = Vec::new();
    input.extend_from_slice(&par2_rs::checksum::md5(&data[..data.len().min(16 * 1024)]));
    input.extend_from_slice(&(data.len() as u64).to_le_bytes());
    input.extend_from_slice(filename.as_bytes());
    par2_rs::FileId::from_bytes(par2_rs::checksum::md5(&input))
}

fn slice_checksums(data: &[u8], slice_size: u64) -> Vec<par2_rs::SliceChecksum> {
    let mut checksums = Vec::new();
    let mut offset = 0usize;
    while offset < data.len() {
        let end = (offset + slice_size as usize).min(data.len());
        let mut state = par2_rs::SliceChecksumState::new();
        state.update(&data[offset..end]);
        let (crc32, md5) = state.finalize(Some(slice_size));
        checksums.push(par2_rs::SliceChecksum { crc32, md5 });
        offset = end;
    }
    checksums
}

/// One-file PAR2 set with complete IFSC data, built the way the fixtures in
/// `pipeline/tests` build theirs.
fn fixture_set(files: &[(&str, &[u8])], slice_size: u64) -> Arc<Par2FileSet> {
    let mut recovery_file_ids = Vec::new();
    let mut descriptions = HashMap::new();
    let mut checksums = HashMap::new();
    for (filename, data) in files {
        let file_id = par2_file_id(filename, data);
        recovery_file_ids.push(file_id);
        descriptions.insert(
            file_id,
            par2_rs::FileDescription {
                file_id,
                hash_full: par2_rs::checksum::md5(data),
                hash_16k: par2_rs::checksum::md5(&data[..data.len().min(16 * 1024)]),
                length: data.len() as u64,
                par2_name: (*filename).to_string(),
                filename: (*filename).to_string(),
            },
        );
        checksums.insert(file_id, slice_checksums(data, slice_size));
    }

    Arc::new(Par2FileSet {
        recovery_set_id: par2_rs::RecoverySetId::from_bytes([7; 16]),
        slice_size,
        recovery_file_ids,
        non_recovery_file_ids: Vec::new(),
        files: descriptions,
        slice_checksums: checksums,
        recovery_slices: BTreeMap::new(),
        creator: None,
    })
}

fn binding(set: &Par2FileSet, filename: &str, data: &[u8]) -> LiveBinding {
    let _ = set;
    LiveBinding {
        par2_file_id: par2_file_id(filename, data),
        length: data.len() as u64,
    }
}

fn active_registry(
    files: &[(&str, &[u8])],
    slice_size: u64,
) -> (LivePar2Registry, Arc<Par2FileSet>) {
    let set = fixture_set(files, slice_size);
    let mut registry = LivePar2Registry::new();
    registry.set_enabled(true);
    registry.activate(JOB, Arc::clone(&set));
    for (index, (filename, data)) in files.iter().enumerate() {
        registry.bind(file(index as u32), Some(binding(&set, filename, data)));
    }
    (registry, set)
}

fn feed(registry: &mut LivePar2Registry, file_id: NzbFileId, offset: u64, bytes: &[u8]) {
    registry.note_segment(file_id, offset, &DecodedChunk::from(bytes.to_vec()));
}

#[test]
fn segments_equal_to_slice_size_claim_whole_blocks_in_stream() {
    let data = payload(256);
    let (mut registry, _set) = active_registry(&[("Silver.Horizon.part01.rar", &data)], 64);

    for index in 0..4u64 {
        let offset = index * 64;
        feed(
            &mut registry,
            file(0),
            offset,
            &data[offset as usize..offset as usize + 64],
        );
    }

    assert_eq!(
        registry.block_states(file(0)).unwrap(),
        vec![BlockState::Ok; 4]
    );
    assert_eq!(registry.metrics().blocks_claimed_in_stream, 4);
    assert_eq!(registry.partial_bytes(), 0);
}

#[test]
fn segments_smaller_than_slice_size_are_staged_until_the_block_completes() {
    let data = payload(128);
    let (mut registry, _set) = active_registry(&[("Silver.Horizon.part01.rar", &data)], 64);

    feed(&mut registry, file(0), 0, &data[0..16]);
    assert_eq!(
        registry.block_states(file(0)).unwrap()[0],
        BlockState::Pending
    );
    assert_eq!(registry.partial_bytes(), 64);

    feed(&mut registry, file(0), 16, &data[16..64]);
    assert_eq!(registry.block_states(file(0)).unwrap()[0], BlockState::Ok);
    assert_eq!(registry.partial_bytes(), 0);
}

#[test]
fn segments_straddling_slice_boundaries_claim_both_blocks() {
    let data = payload(192);
    let (mut registry, _set) = active_registry(&[("Silver.Horizon.part01.rar", &data)], 64);

    // 96-byte spans: block 0 whole + half of block 1, then the rest.
    feed(&mut registry, file(0), 0, &data[0..96]);
    assert_eq!(registry.block_states(file(0)).unwrap()[0], BlockState::Ok);
    assert_eq!(
        registry.block_states(file(0)).unwrap()[1],
        BlockState::Pending
    );

    feed(&mut registry, file(0), 96, &data[96..192]);
    assert_eq!(
        registry.block_states(file(0)).unwrap(),
        vec![BlockState::Ok; 3]
    );
    assert_eq!(registry.partial_bytes(), 0);
}

#[test]
fn out_of_order_fragments_still_complete_their_block() {
    let data = payload(128);
    let (mut registry, _set) = active_registry(&[("Silver.Horizon.part01.rar", &data)], 64);

    feed(&mut registry, file(0), 96, &data[96..128]);
    feed(&mut registry, file(0), 32, &data[32..64]);
    feed(&mut registry, file(0), 64, &data[64..96]);
    feed(&mut registry, file(0), 0, &data[0..32]);

    assert_eq!(
        registry.block_states(file(0)).unwrap(),
        vec![BlockState::Ok; 2]
    );
    assert_eq!(registry.partial_bytes(), 0);
}

#[test]
fn short_tail_block_is_padded_to_slice_size() {
    let data = payload(160);
    let (mut registry, _set) = active_registry(&[("Silver.Horizon.part01.rar", &data)], 64);

    feed(&mut registry, file(0), 0, &data);

    assert_eq!(
        registry.block_states(file(0)).unwrap(),
        vec![BlockState::Ok; 3]
    );
}

#[test]
fn corrupted_span_yields_bad_and_never_ok() {
    let data = payload(128);
    let (mut registry, _set) = active_registry(&[("Silver.Horizon.part01.rar", &data)], 64);

    let mut damaged = data.clone();
    damaged[10] ^= 0xFF;

    // Whole-block fast path.
    feed(&mut registry, file(0), 0, &damaged[0..64]);
    assert_eq!(registry.block_states(file(0)).unwrap()[0], BlockState::Bad);

    // Staged path: a corrupt fragment must not resolve to Ok either.
    let mut damaged_tail = data.clone();
    damaged_tail[100] ^= 0xFF;
    feed(&mut registry, file(0), 64, &damaged_tail[64..96]);
    feed(&mut registry, file(0), 96, &damaged_tail[96..128]);
    assert_eq!(registry.block_states(file(0)).unwrap()[1], BlockState::Bad);
    assert_eq!(registry.metrics().blocks_bad, 2);
    assert_eq!(registry.metrics().blocks_claimed_in_stream, 0);
}

#[test]
fn pre_activation_ranges_are_coalesced_and_retain_no_data() {
    let mut registry = LivePar2Registry::new();
    registry.set_enabled(true);

    feed(&mut registry, file(0), 0, &payload(64));
    feed(&mut registry, file(0), 128, &payload(64));
    assert_eq!(
        registry.recorded_ranges(file(0)).unwrap(),
        vec![(0, 64), (128, 192)]
    );

    // The hole closes and the three spans collapse into one range.
    feed(&mut registry, file(0), 64, &payload(64));
    assert_eq!(registry.recorded_ranges(file(0)).unwrap(), vec![(0, 192)]);
    assert_eq!(registry.recorded_bytes(file(0)), 192);
    assert_eq!(registry.partial_bytes(), 0);
}

#[test]
fn late_activation_backfills_fully_covered_blocks_from_disk() {
    let data = payload(256);
    let set = fixture_set(&[("Silver.Horizon.part01.rar", &data)], 64);
    let mut registry = LivePar2Registry::new();
    registry.set_enabled(true);

    // Three blocks land before the set installs; the fourth is still missing.
    feed(&mut registry, file(0), 0, &data[0..192]);

    let awaiting = registry.activate(JOB, Arc::clone(&set));
    assert_eq!(awaiting, vec![file(0)]);
    registry.bind(
        file(0),
        Some(binding(&set, "Silver.Horizon.part01.rar", &data)),
    );

    let reads = registry.take_queued_reads(JOB);
    assert_eq!(
        reads,
        vec![LiveRead {
            file_id: file(0),
            offset: 0,
            len: 192,
        }]
    );
    registry.apply_read(file(0), 0, &data[0..192], true);

    assert_eq!(
        registry.block_states(file(0)).unwrap(),
        vec![
            BlockState::Ok,
            BlockState::Ok,
            BlockState::Ok,
            BlockState::Pending
        ]
    );
    assert_eq!(registry.metrics().blocks_backfilled, 3);

    // The remaining block still arrives in-stream.
    feed(&mut registry, file(0), 192, &data[192..256]);
    assert_eq!(
        registry.block_states(file(0)).unwrap(),
        vec![BlockState::Ok; 4]
    );
}

#[test]
fn partially_pre_covered_blocks_fall_to_settle_rather_than_in_stream() {
    let data = payload(128);
    let set = fixture_set(&[("Silver.Horizon.part01.rar", &data)], 64);
    let mut registry = LivePar2Registry::new();
    registry.set_enabled(true);

    // Half of block 0 arrives before activation, so in-stream feeding of the
    // other half can never finish it.
    feed(&mut registry, file(0), 0, &data[0..32]);
    registry.activate(JOB, Arc::clone(&set));
    registry.bind(
        file(0),
        Some(binding(&set, "Silver.Horizon.part01.rar", &data)),
    );
    assert!(registry.take_queued_reads(JOB).is_empty());

    feed(&mut registry, file(0), 32, &data[32..64]);
    assert_eq!(
        registry.block_states(file(0)).unwrap()[0],
        BlockState::Pending
    );
    assert_eq!(registry.partial_bytes(), 0);

    let reads = registry.take_settle_reads(file(0));
    assert_eq!(reads.len(), 1);
    assert_eq!(reads[0].offset, 0);
    assert_eq!(reads[0].len, 128);
    registry.apply_read(file(0), 0, &data, false);
    assert_eq!(
        registry.block_states(file(0)).unwrap(),
        vec![BlockState::Ok; 2]
    );
    assert_eq!(registry.metrics().blocks_settled, 2);
}

#[test]
fn global_cap_overflow_abandons_partials_and_settle_recovers_them() {
    let data = payload(256);
    let (mut registry, _set) = active_registry(&[("Silver.Horizon.part01.rar", &data)], 64);
    // Room for exactly one staged block.
    registry.set_partial_budget(64);

    feed(&mut registry, file(0), 0, &data[0..32]);
    assert_eq!(registry.partial_bytes(), 64);

    // Block 1 cannot be staged: the cap is already spent.
    feed(&mut registry, file(0), 64, &data[64..96]);
    assert_eq!(registry.partial_bytes(), 64);
    assert_eq!(registry.metrics().partials_abandoned, 1);
    assert_eq!(
        registry.block_states(file(0)).unwrap()[1],
        BlockState::Pending
    );

    // Finishing block 0 releases its buffer.
    feed(&mut registry, file(0), 32, &data[32..64]);
    assert_eq!(registry.partial_bytes(), 0);
    assert_eq!(registry.block_states(file(0)).unwrap()[0], BlockState::Ok);

    // Settle reclaims the abandoned block (and everything else still pending).
    let reads = registry.take_settle_reads(file(0));
    let mut total = 0u64;
    for read in reads {
        let start = read.offset as usize;
        let end = start + read.len as usize;
        registry.apply_read(file(0), read.offset, &data[start..end], false);
        total += read.len;
    }
    assert_eq!(total, 192);
    assert_eq!(
        registry.block_states(file(0)).unwrap(),
        vec![BlockState::Ok; 4]
    );
}

#[test]
fn partial_accounting_never_exceeds_the_cap_on_a_many_boundary_workload() {
    // Every span lands one byte short of a block boundary across many files,
    // which is the arrival shape that grows partial buffers fastest.
    let slice_size = 1024u64;
    let data = payload(16 * 1024);
    let files: Vec<(String, Vec<u8>)> = (0..32)
        .map(|index| (format!("Silver.Horizon.part{index:02}.rar"), data.clone()))
        .collect();
    let borrowed: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let (mut registry, _set) = active_registry(&borrowed, slice_size);
    let cap = 8 * 1024;
    registry.set_partial_budget(cap);

    for (index, (name, bytes)) in borrowed.iter().enumerate() {
        let file_id = file(index as u32);
        let _ = name;
        let mut offset = 0u64;
        while offset < bytes.len() as u64 {
            // 384-byte spans never align to the 1 KiB block grid.
            let end = (offset + 384).min(bytes.len() as u64);
            feed(
                &mut registry,
                file_id,
                offset,
                &bytes[offset as usize..end as usize],
            );
            assert!(
                registry.partial_bytes() <= registry.partial_budget_bytes(),
                "partial bytes {} exceeded cap {}",
                registry.partial_bytes(),
                registry.partial_budget_bytes()
            );
            offset = end;
        }
    }

    assert!(registry.metrics().partial_bytes_peak <= cap);
    assert!(registry.metrics().blocks_claimed_in_stream > 0);
}

#[test]
fn disabled_registry_records_nothing() {
    let mut registry = LivePar2Registry::new();
    registry.set_enabled(false);
    feed(&mut registry, file(0), 0, &payload(64));
    assert!(registry.recorded_ranges(file(0)).is_none());
    assert!(registry.fully_verified_files(JOB).is_none());
}

#[test]
fn removing_a_job_releases_its_partial_budget() {
    let data = payload(128);
    let (mut registry, _set) = active_registry(&[("Silver.Horizon.part01.rar", &data)], 64);

    feed(&mut registry, file(0), 0, &data[0..32]);
    assert_eq!(registry.partial_bytes(), 64);

    registry.remove_job(JOB);
    assert_eq!(registry.partial_bytes(), 0);
    assert!(registry.fully_verified_files(JOB).is_none());
}

#[test]
fn fully_verified_files_reports_only_block_complete_files() {
    let first = payload(128);
    let second = payload(64);
    let (mut registry, _set) = active_registry(
        &[
            ("Silver.Horizon.part01.rar", &first),
            ("Silver.Horizon.part02.rar", &second),
        ],
        64,
    );

    feed(&mut registry, file(0), 0, &first);
    let verified = registry.fully_verified_files(JOB).unwrap();
    assert_eq!(verified.len(), 1);
    assert!(verified.contains_key(&par2_file_id("Silver.Horizon.part01.rar", &first)));

    feed(&mut registry, file(1), 0, &second);
    assert_eq!(registry.fully_verified_files(JOB).unwrap().len(), 2);
}

#[test]
fn short_read_leaves_uncovered_blocks_pending() {
    let data = payload(192);
    let (mut registry, _set) = active_registry(&[("Silver.Horizon.part01.rar", &data)], 64);

    let reads = registry.take_settle_reads(file(0));
    assert_eq!(reads.len(), 1);
    // Truncated file: only two of the three blocks are readable.
    registry.apply_read(file(0), 0, &data[0..128], false);
    assert_eq!(
        registry.block_states(file(0)).unwrap(),
        vec![BlockState::Ok, BlockState::Ok, BlockState::Pending]
    );
}

#[test]
fn boundary_refeed_of_a_decided_block_demotes_it_to_a_settle_read() {
    let data = payload(128);
    let (mut registry, _set) = active_registry(&[("Silver.Horizon.part01.rar", &data)], 64);

    feed(&mut registry, file(0), 0, &data[0..64]);
    assert_eq!(registry.block_states(file(0)).unwrap()[0], BlockState::Ok);

    // A re-write of part of a decided block (duplicate segment, CRC-failed
    // decode that still committed) cannot be re-verified by a partial feed the
    // way the whole-block path re-verifies, so the verdict is retired.
    feed(&mut registry, file(0), 0, &data[0..32]);
    assert_eq!(
        registry.block_states(file(0)).unwrap()[0],
        BlockState::Pending
    );
    assert_eq!(registry.metrics().blocks_demoted, 1);
    assert_eq!(registry.partial_bytes(), 0);

    // In-stream feeding can never finish it again...
    feed(&mut registry, file(0), 32, &data[32..64]);
    assert_eq!(
        registry.block_states(file(0)).unwrap()[0],
        BlockState::Pending
    );

    // ...the settle read re-verifies it from what is actually on disk.
    let reads = registry.take_settle_reads(file(0));
    assert_eq!(reads.len(), 1);
    assert_eq!((reads[0].offset, reads[0].len), (0, 128));
    registry.apply_read(file(0), 0, &data, false);
    assert_eq!(
        registry.block_states(file(0)).unwrap(),
        vec![BlockState::Ok; 2]
    );
}

#[test]
fn whole_block_refeed_with_different_bytes_flips_ok_to_bad() {
    let data = payload(128);
    let (mut registry, _set) = active_registry(&[("Silver.Horizon.part01.rar", &data)], 64);

    feed(&mut registry, file(0), 0, &data[0..64]);
    assert_eq!(registry.block_states(file(0)).unwrap()[0], BlockState::Ok);

    let mut damaged = data.clone();
    damaged[5] ^= 0xFF;
    feed(&mut registry, file(0), 0, &damaged[0..64]);
    assert_eq!(registry.block_states(file(0)).unwrap()[0], BlockState::Bad);
    assert_eq!(registry.metrics().blocks_bad, 1);
    assert!(registry.fully_verified_files(JOB).unwrap().is_empty());
}

#[test]
fn a_file_completing_at_the_wrong_decoded_length_is_rejected() {
    let data = payload(128);
    let (mut registry, _set) = active_registry(&[("Silver.Horizon.part01.rar", &data)], 64);

    feed(&mut registry, file(0), 0, &data);
    assert_eq!(registry.fully_verified_files(JOB).unwrap().len(), 1);

    // The decoded length is the only length comparable with the description:
    // a disagreement means these blocks are not that file's blocks.
    registry.note_file_complete(file(0), data.len() as u64 - 1);
    assert!(registry.fully_verified_files(JOB).unwrap().is_empty());
    assert!(registry.block_states(file(0)).is_none());
    assert!(!registry.needs_binding(file(0)));
}

#[test]
fn a_file_completing_at_the_described_length_keeps_its_blocks() {
    let data = payload(128);
    let (mut registry, _set) = active_registry(&[("Silver.Horizon.part01.rar", &data)], 64);

    feed(&mut registry, file(0), 0, &data);
    registry.note_file_complete(file(0), data.len() as u64);
    assert_eq!(registry.fully_verified_files(JOB).unwrap().len(), 1);
}

#[test]
fn spans_past_the_described_length_are_ignored_and_counted() {
    let data = payload(128);
    let (mut registry, _set) = active_registry(&[("Silver.Horizon.part01.rar", &data)], 64);

    // The block vector is sized from the PAR2 description, so a file whose
    // decoded bytes run past it must clamp rather than index off the end.
    let mut overlong = data.clone();
    overlong.extend_from_slice(&payload(32));
    feed(&mut registry, file(0), 0, &overlong);
    assert_eq!(
        registry.block_states(file(0)).unwrap(),
        vec![BlockState::Ok; 2]
    );
    assert_eq!(registry.metrics().spans_out_of_range, 1);

    feed(&mut registry, file(0), 128, &payload(32));
    assert_eq!(
        registry.block_states(file(0)).unwrap(),
        vec![BlockState::Ok; 2]
    );
    assert_eq!(registry.metrics().spans_out_of_range, 2);
}

#[test]
fn an_over_budget_activation_backfill_is_skipped_whole() {
    let data = payload(256);
    let set = fixture_set(&[("Silver.Horizon.part01.rar", &data)], 64);
    let mut registry = LivePar2Registry::new();
    registry.set_enabled(true);

    feed(&mut registry, file(0), 0, &data[0..192]);
    registry.activate(JOB, Arc::clone(&set));
    // Less room than the recorded coverage needs: the eager read is skipped
    // whole instead of half-spent, leaving the budget to the settle pass that
    // actually decides.
    registry.set_disk_read_budget(JOB, 128);
    registry.bind(
        file(0),
        Some(binding(&set, "Silver.Horizon.part01.rar", &data)),
    );

    assert!(registry.take_queued_reads(JOB).is_empty());
    assert_eq!(registry.disk_read_budget(JOB), Some(128));
    assert_eq!(
        registry.block_states(file(0)).unwrap(),
        vec![BlockState::Pending; 4]
    );
}

#[test]
fn a_budget_truncated_settle_leaves_the_file_unsettled() {
    let data = payload(256);
    let (mut registry, _set) = active_registry(&[("Silver.Horizon.part01.rar", &data)], 64);

    registry.set_disk_read_budget(JOB, 0);
    assert!(registry.take_settle_reads(file(0)).is_empty());
    assert_eq!(registry.files_needing_settle(JOB), vec![file(0)]);

    registry.set_disk_read_budget(JOB, 1024);
    let reads = registry.take_settle_reads(file(0));
    assert_eq!(reads.len(), 1);
    registry.apply_read(file(0), 0, &data, false);
    assert_eq!(
        registry.block_states(file(0)).unwrap(),
        vec![BlockState::Ok; 4]
    );
    assert!(registry.files_needing_settle(JOB).is_empty());
}

#[test]
fn a_skipped_job_never_allocates_recording_state() {
    let mut registry = LivePar2Registry::new();
    registry.set_enabled(true);

    registry.skip_job(JOB);
    assert!(registry.knows_job(JOB));
    feed(&mut registry, file(0), 0, &payload(64));
    assert!(registry.recorded_ranges(file(0)).is_none());

    // A PAR2 set turning up anyway (an obfuscated PAR2 file the spec's roles
    // never named) still activates the job.
    let data = payload(128);
    let set = fixture_set(&[("Silver.Horizon.part01.rar", &data)], 64);
    assert!(registry.activate(JOB, set).is_empty());
    assert!(registry.is_active(JOB));
}

#[test]
fn kill_switch_parsing_matches_the_documented_values() {
    assert!(parse_enabled(None));
    assert!(parse_enabled(Some("")));
    assert!(!parse_enabled(Some("0")));
    assert!(!parse_enabled(Some("false")));
    assert!(!parse_enabled(Some(" FALSE ")));
    assert!(!parse_enabled(Some("off")));
    assert!(!parse_enabled(Some("no")));
    assert!(parse_enabled(Some("1")));
    assert!(parse_enabled(Some("true")));
}

#[test]
fn a_fresh_registry_follows_the_kill_switch() {
    assert_eq!(LivePar2Registry::new().enabled(), env_enabled());
}
