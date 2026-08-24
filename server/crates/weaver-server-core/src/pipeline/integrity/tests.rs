use std::collections::{BTreeMap, HashMap};

use super::*;
use crate::jobs::ids::JobId;

const JOB: JobId = JobId(4242);

fn file(index: u32) -> NzbFileId {
    NzbFileId {
        job_id: JOB,
        file_index: index,
    }
}

/// Deterministic xorshift64*: every fixture below is a pure function of a seed.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn range(&mut self, low: u64, high: u64) -> u64 {
        low + self.next_u64() % (high - low + 1)
    }
}

fn random_bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut rng = Rng(seed | 1);
    (0..len).map(|_| (rng.next_u64() >> 33) as u8).collect()
}

fn direct_crc(data: &[u8]) -> u32 {
    par2_rs::checksum::crc32(data)
}

fn block(size: u64) -> NonZeroU64 {
    NonZeroU64::new(size).expect("non-zero block size")
}

/// Run one article's bytes through the real decoder's checkpointing CRC pass,
/// returning what the decoder would hand the collector.
fn decode_article_segments(
    data: &[u8],
    file_offset: u64,
    block_size: Option<NonZeroU64>,
    chunk: usize,
) -> (u32, Vec<Segment>) {
    let checkpoint_plan = block_size
        .map(weaver_yenc::CheckpointPlan::Single)
        .unwrap_or(weaver_yenc::CheckpointPlan::None);
    let mut pass = weaver_yenc::SegmentedCrc32::new(file_offset, checkpoint_plan);
    for slice in data.chunks(chunk.max(1)) {
        pass.update(slice);
    }
    pass.finish_article()
}

#[test]
fn crc32_of_zeros_matches_a_direct_hash() {
    for len in [
        0u64, 1, 2, 3, 7, 8, 15, 16, 17, 63, 64, 255, 256, 1000, 4096, 65_537,
    ] {
        assert_eq!(
            crc32_of_zeros(len),
            direct_crc(&vec![0u8; len as usize]),
            "len {len}"
        );
    }
}

#[test]
fn fold_tiling_rejects_gaps_overlaps_and_short_tilings() {
    let a = Segment {
        file_offset: 0,
        len: 10,
        crc32: 0x1111_1111,
    };
    let gapped = Segment {
        file_offset: 11,
        len: 9,
        crc32: 0x2222_2222,
    };
    let overlapping = Segment {
        file_offset: 9,
        len: 11,
        crc32: 0x3333_3333,
    };
    let empty = Segment {
        file_offset: 10,
        len: 0,
        crc32: 0,
    };
    assert_eq!(fold_tiling(&[], 0, 20), None);
    assert_eq!(fold_tiling(&[a], 0, 10), Some(a.crc32));
    assert_eq!(fold_tiling(&[a], 0, 20), None, "short tiling");
    assert_eq!(fold_tiling(&[a, gapped], 0, 20), None, "gap");
    assert_eq!(fold_tiling(&[a, overlapping], 0, 20), None, "overlap");
    // A zero-length record must not be able to bridge anything: the combine
    // operator's zero-length case is the identity on its first argument, so
    // accepting one would discard the record that followed it.
    assert_eq!(fold_tiling(&[a, empty], 0, 10), None, "zero-length record");
}

/// Gate 1 over the collector rather than the primitive: derived block CRC32s
/// must equal a direct CRC over the block's bytes, for random files split into
/// random articles at random offsets against random block sizes, including
/// articles that straddle several boundaries and the short final block.
#[test]
fn derived_block_crcs_match_direct_over_random_article_tilings() {
    let mut blocks_checked = 0usize;
    let mut short_final_blocks = 0usize;
    let mut multi_article_blocks = 0usize;
    let mut straddle_histogram = [0usize; 8];

    for seed in 0..48u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
        let block_size = block(match seed % 6 {
            0 => 1,
            1 => 64,
            2 => 512,
            3 => 1000,
            4 => rng.range(2, 4096),
            _ => rng.range(2, 400),
        });
        let file_len = rng.range(1, 30_000) as usize;
        let payload = random_bytes(seed ^ 0xfeed_face, file_len);

        // Random article tiling of the whole file.
        let mut bounds = vec![0usize];
        while *bounds.last().expect("seeded") < file_len {
            let start = *bounds.last().expect("seeded");
            let step = rng.range(1, 2500) as usize;
            bounds.push((start + step).min(file_len));
        }

        let mut collector = BlockCrcCollector::new();
        let file_id = file(seed as u32);
        // Some articles arrive out of order, and a leading prefix of them
        // arrives before the block size is known.
        let mut order: Vec<usize> = (0..bounds.len() - 1).collect();
        if seed % 3 == 0 {
            order.reverse();
        }
        let pre_blocksize_count = (seed % 4) as usize;

        for (position, &article) in order.iter().enumerate() {
            let (start, end) = (bounds[article], bounds[article + 1]);
            let known = position >= pre_blocksize_count;
            let (part_crc, segments) = decode_article_segments(
                &payload[start..end],
                start as u64,
                known.then_some(block_size),
                rng.range(1, 700) as usize,
            );
            assert_eq!(part_crc, direct_crc(&payload[start..end]));
            if known {
                straddle_histogram[(segments.len() - 1).min(7)] += 1;
            }
            collector.note_article(
                file_id,
                block_size,
                start as u64,
                (end - start) as u64,
                part_crc,
                true,
                false,
                &segments,
            );
        }
        collector.note_file_len(file_id, file_len as u64);

        let size = block_size.get();
        for (block_index, derived) in collector.derived_blocks(file_id) {
            let start = u64::from(block_index) * size;
            let end = (start + size).min(file_len as u64);
            assert_eq!(
                derived,
                direct_crc(&payload[start as usize..end as usize]),
                "seed {seed} block {block_index} [{start},{end}) size {size}"
            );
            blocks_checked += 1;
            if end - start < size {
                short_final_blocks += 1;
            }
            let spanning = bounds
                .windows(2)
                .filter(|w| (w[0] as u64) < end && (w[1] as u64) > start)
                .count();
            if spanning > 1 {
                multi_article_blocks += 1;
            }
        }
    }

    eprintln!(
        "derived_block_crcs: {blocks_checked} blocks, {short_final_blocks} short final, \
         {multi_article_blocks} spanning >1 article, articles by boundaries straddled \
         {straddle_histogram:?}"
    );
    assert!(blocks_checked > 500, "blocks checked {blocks_checked}");
    assert!(short_final_blocks > 0, "no short final block was closed");
    assert!(
        multi_article_blocks > 0,
        "no block was assembled from more than one article"
    );
    for straddled in 0..3usize {
        assert!(
            straddle_histogram[straddled] > 0,
            "no article straddling {straddled} boundaries; {straddle_histogram:?}"
        );
    }
}

#[test]
fn a_missing_article_leaves_its_blocks_unclaimed() {
    let payload = random_bytes(0xa11, 4096);
    let block_size = block(1024);
    let file_id = file(0);
    let mut collector = BlockCrcCollector::new();

    // Feed everything except [1024, 2048): blocks 0 and 2..3 stay claimable
    // (block 0 is covered by the first article), block 1 has a hole.
    for (start, end) in [(0usize, 1024usize), (2048, 4096)] {
        let (part_crc, segments) =
            decode_article_segments(&payload[start..end], start as u64, Some(block_size), 97);
        collector.note_article(
            file_id,
            block_size,
            start as u64,
            (end - start) as u64,
            part_crc,
            true,
            false,
            &segments,
        );
    }
    collector.note_file_len(file_id, payload.len() as u64);

    let derived: Vec<u32> = collector
        .derived_blocks(file_id)
        .map(|(index, _)| index)
        .collect();
    assert_eq!(derived, vec![0, 2, 3], "block 1 must stay unclaimed");
}

#[test]
fn aligned_segments_close_without_pending_runs() {
    let payload = random_bytes(0xa12, 2048);
    let block_size = block(1024);
    let file_id = file(1);
    let mut collector = BlockCrcCollector::new();

    for start in (0..payload.len()).step_by(1024) {
        let end = start + 1024;
        let (part_crc, segments) =
            decode_article_segments(&payload[start..end], start as u64, Some(block_size), 97);
        collector.note_article(
            file_id,
            block_size,
            start as u64,
            1024,
            part_crc,
            true,
            false,
            &segments,
        );
    }

    assert_eq!(
        collector.entry_counts_for_job(JOB),
        EntryCounts {
            pending: 0,
            derived: 2
        }
    );
    assert_eq!(
        collector.derived_block_crc(file_id, 1),
        Some(direct_crc(&payload[1024..]))
    );
}

#[test]
fn known_short_final_segment_closes_without_a_pending_run() {
    let payload = random_bytes(0xa13, 1536);
    let block_size = block(1024);
    let file_id = file(2);
    let mut collector = BlockCrcCollector::new();

    let (first_crc, first_segments) =
        decode_article_segments(&payload[..1024], 0, Some(block_size), 97);
    collector.note_article(
        file_id,
        block_size,
        0,
        1024,
        first_crc,
        true,
        false,
        &first_segments,
    );
    collector.note_file_len(file_id, payload.len() as u64);

    let (last_crc, last_segments) =
        decode_article_segments(&payload[1024..], 1024, Some(block_size), 97);
    collector.note_article(
        file_id,
        block_size,
        1024,
        512,
        last_crc,
        true,
        false,
        &last_segments,
    );

    assert_eq!(
        collector.entry_counts_for_job(JOB),
        EntryCounts {
            pending: 0,
            derived: 2
        }
    );
    assert_eq!(
        collector.derived_block_crc(file_id, 1),
        Some(direct_crc(&payload[1024..]))
    );
}

#[test]
fn segments_contradicting_the_pipeline_placement_are_reduced_to_one_record() {
    let payload = random_bytes(0xbad0, 2048);
    let block_size = block(512);
    let file_id = file(0);
    let mut collector = BlockCrcCollector::new();

    // The poster's `=ypart begin` claimed offset 0 while the pipeline placed the
    // article at 1024: the segments were cut on a grid that does not exist.
    let (part_crc, segments) = decode_article_segments(&payload[1024..], 0, Some(block_size), 64);
    collector.note_article(
        file_id, block_size, 1024, 1024, part_crc, true, false, &segments,
    );
    collector.note_file_len(file_id, 2048);

    assert_eq!(collector.rebased_articles(), 1);
    // The whole-article record still tiles blocks 2 and 3's union, but neither
    // block on its own, so nothing is claimed from a grid that was never real.
    assert_eq!(collector.derived_blocks(file_id).count(), 0);

    // The same article placed where its own segments say it is composes fully.
    let mut honest = BlockCrcCollector::new();
    honest.note_article(
        file_id, block_size, 0, 1024, part_crc, true, false, &segments,
    );
    honest.note_file_len(file_id, 1024);
    assert_eq!(honest.rebased_articles(), 0);
    assert_eq!(honest.derived_blocks(file_id).count(), 2);
}

#[test]
fn pending_segments_are_retired_as_blocks_close() {
    let payload = random_bytes(0xc0de, 8192);
    let block_size = block(1024);
    let file_id = file(0);
    let mut collector = BlockCrcCollector::new();

    for start in (0..8192).step_by(512) {
        let end = start + 512;
        let (part_crc, segments) =
            decode_article_segments(&payload[start..end], start as u64, Some(block_size), 128);
        collector.note_article(
            file_id,
            block_size,
            start as u64,
            512,
            part_crc,
            true,
            false,
            &segments,
        );
    }
    collector.note_file_len(file_id, 8192);

    assert_eq!(collector.derived_blocks(file_id).count(), 8);
    let entry = collector.files.get(&file_id).expect("file tracked");
    assert!(
        entry.grids.values().all(|grid| grid.pending.is_empty()),
        "closed blocks must not retain their runs: {} left",
        entry.pending_entries
    );
}

#[test]
fn out_of_order_islands_merge_only_when_their_boundaries_meet() {
    let payload = random_bytes(0xc0df, 1024);
    let block_size = block(1024);
    let file_id = file(1);
    let mut collector = BlockCrcCollector::new();

    // Three distinct islands arrive right-to-left. The first two must both
    // remain pending until the final left-hand segment bridges them.
    for (start, end) in [(768usize, 1024usize), (512, 768), (0, 512)] {
        let (part_crc, segments) =
            decode_article_segments(&payload[start..end], start as u64, Some(block_size), 64);
        collector.note_article(
            file_id,
            block_size,
            start as u64,
            (end - start) as u64,
            part_crc,
            true,
            false,
            &segments,
        );
    }

    assert_eq!(
        collector.derived_block_crc(file_id, 0),
        Some(direct_crc(&payload))
    );
    assert_eq!(collector.files[&file_id].pending_entries, 0);
}

#[test]
fn job_totals_follow_promotion_invalidation_and_retirement() {
    let block_size = block(1024);
    let data = random_bytes(0xc0e0, 2048);
    let mut collector = BlockCrcCollector::new();
    let primary = file(2);

    let (left_crc, left_segments) = decode_article_segments(&data[..512], 0, Some(block_size), 128);
    collector.note_article(
        primary,
        block_size,
        0,
        512,
        left_crc,
        true,
        false,
        &left_segments,
    );
    assert_eq!(
        collector.entry_counts_for_job(JOB),
        EntryCounts {
            pending: 1,
            derived: 0,
        }
    );

    let (right_crc, right_segments) =
        decode_article_segments(&data[512..1024], 512, Some(block_size), 128);
    collector.note_article(
        primary,
        block_size,
        512,
        512,
        right_crc,
        true,
        false,
        &right_segments,
    );
    assert_eq!(
        collector.entry_counts_for_job(JOB),
        EntryCounts {
            pending: 0,
            derived: 1,
        }
    );

    // A duplicate spanning the block boundary invalidates the retired block,
    // but cannot itself be retained because its CRC cannot be split.
    collector.note_article(primary, block_size, 512, 1024, 0x9a7b_6c5d, true, true, &[]);
    assert_eq!(collector.entry_counts_for_job(JOB), EntryCounts::default());

    // A rewrite/repair of one file must not retire a different recovery set's
    // raw bytes merely because both belong to the same job.
    let untouched_file = file(4);
    let (untouched_crc, untouched_segments) =
        decode_article_segments(&data[..1024], 0, Some(block_size), 128);
    collector.note_article(
        untouched_file,
        block_size,
        0,
        1024,
        untouched_crc,
        true,
        false,
        &untouched_segments,
    );
    assert_eq!(collector.entry_counts_for_job(JOB).derived, 1);

    let pending_file = file(3);
    collector.note_article(
        pending_file,
        block_size,
        0,
        512,
        left_crc,
        true,
        false,
        &left_segments,
    );
    assert_eq!(
        collector.entry_counts_for_job(JOB),
        EntryCounts {
            pending: 1,
            derived: 1,
        }
    );
    collector.forget_file(pending_file);
    assert_eq!(
        collector.entry_counts_for_job(JOB),
        EntryCounts {
            pending: 0,
            derived: 1,
        }
    );
    assert!(collector.derived_block_crc(untouched_file, 0).is_some());

    let teardown_file = file(5);
    collector.note_article(
        teardown_file,
        block_size,
        0,
        512,
        left_crc,
        true,
        false,
        &left_segments,
    );
    collector.forget_job(JOB);
    assert_eq!(collector.entry_counts_for_job(JOB), EntryCounts::default());
    assert!(collector.files.is_empty());
}

// --- PAR2 fixture plumbing, matching the shape the live-PAR2 tests use. ---

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

fn fixture_set(filename: &str, data: &[u8], slice_size: u64) -> par2_rs::Par2FileSet {
    let file_id = par2_file_id(filename, data);
    let mut files = HashMap::new();
    files.insert(
        file_id,
        par2_rs::FileDescription {
            file_id,
            hash_full: par2_rs::checksum::md5(data),
            hash_16k: par2_rs::checksum::md5(&data[..data.len().min(16 * 1024)]),
            length: data.len() as u64,
            par2_name: filename.to_string(),
            filename: filename.to_string(),
        },
    );
    let mut slice_checksums_map = HashMap::new();
    slice_checksums_map.insert(file_id, slice_checksums(data, slice_size));

    par2_rs::Par2FileSet {
        recovery_set_id: par2_rs::RecoverySetId::from_bytes([3; 16]),
        slice_size,
        recovery_file_ids: vec![file_id],
        non_recovery_file_ids: Vec::new(),
        files,
        slice_checksums: slice_checksums_map,
        recovery_slices: BTreeMap::new(),
        creator: None,
    }
}

/// Encode `payload` into multi-part yEnc articles and decode them back through
/// the production streaming decoder with the recovery set's block size
/// declared, so this exercises decode -> segments -> collector rather than a
/// hand-built segment list.
fn decode_articles_into(
    collector: &mut BlockCrcCollector,
    file_id: NzbFileId,
    payload: &[u8],
    article_len: usize,
    block_size: NonZeroU64,
    damage: &dyn Fn(usize, &mut Vec<u8>),
) {
    let total = payload.len().div_ceil(article_len) as u32;
    for (index, chunk) in payload.chunks(article_len).enumerate() {
        let begin = (index * article_len) as u64;
        let mut part = chunk.to_vec();
        damage(index, &mut part);

        let mut article = Vec::new();
        weaver_yenc::encode_part(
            &part,
            &mut article,
            128,
            "payload.bin",
            index as u32 + 1,
            total,
            begin + 1,
            begin + part.len() as u64,
            payload.len() as u64,
        )
        .expect("encode");

        let mut decoder = weaver_yenc::StreamingArticleDecoder::new();
        decoder.set_checkpoint_plan(weaver_yenc::CheckpointPlan::Single(block_size));
        let mut output = Vec::new();
        // Feed in awkward chunks: the segments must not depend on them.
        for slice in article.chunks(37) {
            decoder.feed_chunk(slice, &mut output).expect("feed");
        }
        let decoded = decoder.finish(output).expect("finish");

        collector.note_article(
            file_id,
            block_size,
            begin,
            decoded.result.bytes_written as u64,
            decoded.result.part_crc,
            true,
            false,
            &decoded.result.segments,
        );
    }
    collector.note_file_len(file_id, payload.len() as u64);
}

/// Gate 4: a damaged article flows decode -> segments -> collector -> verdicts,
/// and the blocks it damaged are named exactly, with no read-back of any block.
#[test]
fn a_damaged_article_is_localised_to_exactly_its_blocks_with_no_read_back() {
    // 9500 bytes over 1024-byte blocks: 9 full blocks and a 316-byte final one,
    // with 700-byte articles so blocks and articles are mutually unaligned and
    // one damaged article touches more than one block.
    let payload = random_bytes(0xd00d, 9500);
    let block_size = block(1024);
    let par2_set = fixture_set("payload.bin", &payload, block_size.get());
    let par2_file_id = par2_file_id("payload.bin", &payload);
    let file_id = file(0);

    // Article 3 covers [2100, 2800): blocks 2 ([2048,3072)) only.
    // Article 8 covers [5600, 6300): blocks 5 ([5120,6144)) and 6.
    let damaged_articles = [3usize, 8];
    let mut collector = BlockCrcCollector::new();
    decode_articles_into(
        &mut collector,
        file_id,
        &payload,
        700,
        block_size,
        &|index, part| {
            if damaged_articles.contains(&index) {
                part[0] ^= 0xff;
            }
        },
    );

    let verdicts = collector.verdicts_against(file_id, &par2_set, par2_file_id);
    let damaged: Vec<u32> = verdicts
        .iter()
        .filter(|(_, verdict)| **verdict == BlockVerdict::Damaged)
        .map(|(index, _)| *index)
        .collect();
    let intact: Vec<u32> = verdicts
        .iter()
        .filter(|(_, verdict)| matches!(verdict, BlockVerdict::Intact { .. }))
        .map(|(index, _)| *index)
        .collect();

    // Every block is claimed: the articles tile the file and the block size was
    // known from the first one, so nothing is left for settle-time read-back.
    assert_eq!(verdicts.len(), 10, "verdicts {verdicts:?}");
    // Damage at [2100] and [5600] lands in blocks 2 and 5 respectively.
    assert_eq!(damaged, vec![2, 5], "verdicts {verdicts:?}");
    assert_eq!(
        intact,
        vec![0, 1, 3, 4, 6, 7, 8, 9],
        "verdicts {verdicts:?}"
    );
    assert_eq!(collector.rebased_articles(), 0);

    // The short final block ([9216, 9500), 316 real bytes) is among the intact
    // verdicts, which is the zero-padding rule being applied: comparing the
    // unpadded CRC against the IFSC entry would have called it damaged.
    let final_block = 9u32;
    let derived = collector
        .derived_block_crc(file_id, final_block)
        .expect("final block derived");
    assert_eq!(derived, direct_crc(&payload[9216..]));
    assert_ne!(
        derived,
        par2_set
            .file_checksums(&par2_file_id)
            .expect("IFSC")
            .last()
            .expect("final slice")
            .crc32,
        "the fixture's final slice must actually be zero-padded, or this \
         assertion proves nothing"
    );
}

/// The clean counterpart: with no damage every block is intact, and the derived
/// verdicts agree with a direct CRC of every block.
#[test]
fn an_undamaged_download_claims_every_block_intact() {
    let payload = random_bytes(0x5a5a, 7000);
    let block_size = block(512);
    let par2_set = fixture_set("payload.bin", &payload, block_size.get());
    let par2_file_id = par2_file_id("payload.bin", &payload);
    let file_id = file(0);

    let mut collector = BlockCrcCollector::new();
    decode_articles_into(
        &mut collector,
        file_id,
        &payload,
        333,
        block_size,
        &|_, _| {},
    );

    let verdicts = collector.verdicts_against(file_id, &par2_set, par2_file_id);
    assert_eq!(verdicts.len(), 7000_u64.div_ceil(512) as usize);
    assert!(
        verdicts
            .values()
            .all(|verdict| matches!(verdict, BlockVerdict::Intact { .. })),
        "verdicts {verdicts:?}"
    );
}

/// Articles decoded before the recovery set was parsed carry one segment each,
/// so they claim only the blocks their own boundaries happen to tile, and the
/// rest stay unclaimed for settle-time verification. Never delayed, never
/// re-decoded.
#[test]
fn pre_block_size_articles_claim_only_what_they_tile() {
    let payload = random_bytes(0x7e57, 4096);
    let block_size = block(1024);
    let file_id = file(0);
    let mut collector = BlockCrcCollector::new();

    // Two 1024-byte articles decoded before the block size was known: their own
    // boundaries do tile blocks 0 and 1. Then a 2048-byte article covering
    // blocks 2 and 3 as one segment: it tiles neither block alone.
    for start in [0usize, 1024] {
        let (part_crc, segments) =
            decode_article_segments(&payload[start..start + 1024], start as u64, None, 100);
        assert_eq!(segments.len(), 1);
        collector.note_article(
            file_id,
            block_size,
            start as u64,
            1024,
            part_crc,
            true,
            false,
            &segments,
        );
    }
    let (part_crc, segments) = decode_article_segments(&payload[2048..], 2048, None, 100);
    collector.note_article(
        file_id, block_size, 2048, 2048, part_crc, true, false, &segments,
    );
    collector.note_file_len(file_id, 4096);

    let claimed: Vec<u32> = collector
        .derived_blocks(file_id)
        .map(|(index, _)| index)
        .collect();
    assert_eq!(claimed, vec![0, 1]);
    for (index, derived) in collector.derived_blocks(file_id) {
        let start = index as usize * 1024;
        assert_eq!(derived, direct_crc(&payload[start..start + 1024]));
    }
}

/// Add `count` recovery slices over `data` so a fixture set can actually repair.
fn with_recovery_slices(
    mut set: par2_rs::Par2FileSet,
    data: &[u8],
    slice_size: u64,
    count: u32,
) -> par2_rs::Par2FileSet {
    let slice_size = slice_size as usize;
    let slice_count = data.len().div_ceil(slice_size);
    let mut padded = data.to_vec();
    padded.resize(slice_count * slice_size, 0);

    let constants = par2_rs::input_slice_constants(slice_count);
    for exponent in 0..count {
        let mut recovery = vec![0u8; slice_size];
        for (index, &constant) in constants.iter().enumerate() {
            par2_rs::mul_acc_region(
                par2_rs::gf_pow(constant, exponent),
                &padded[index * slice_size..(index + 1) * slice_size],
                &mut recovery,
            );
        }
        set.recovery_slices.insert(
            exponent,
            par2_rs::RecoverySlice {
                exponent,
                data: bytes::Bytes::from(recovery).into(),
            },
        );
    }
    set
}

/// Gate 4, end to end: a damaged-article fixture flows decode -> segments ->
/// collector -> verdicts -> slice evidence -> repair session -> repair.
///
/// The session is access-backed and never scans, so everything it concludes
/// comes from the in-stream evidence alone. It must identify exactly the blocks
/// the damaged articles touched, repair those from recovery data, and produce
/// the true payload — with no MD5 computed anywhere on the download path.
#[test]
fn a_damaged_article_flows_from_evidence_through_a_session_to_repair() {
    // The fixture of `a_damaged_article_is_localised_to_exactly_its_blocks_with_no_read_back`:
    // 9500 bytes over 1024-byte blocks, 700-byte articles, damage in articles 3
    // and 8, which lands in blocks 2 and 5.
    let payload = random_bytes(0xd00d, 9500);
    let block_size = block(1024);
    let article_len = 700usize;
    let damaged_articles = [3usize, 8];
    let par2_file_id = par2_file_id("payload.bin", &payload);
    let file_id = file(0);
    let par2_set = with_recovery_slices(
        fixture_set("payload.bin", &payload, block_size.get()),
        &payload,
        block_size.get(),
        4,
    );

    let mut collector = BlockCrcCollector::new();
    decode_articles_into(
        &mut collector,
        file_id,
        &payload,
        article_len,
        block_size,
        &|index, part| {
            if damaged_articles.contains(&index) {
                part[0] ^= 0xff;
            }
        },
    );

    // What actually landed: the same damage the decoder saw.
    let mut stored = payload.clone();
    for index in damaged_articles {
        stored[index * article_len] ^= 0xff;
    }

    let verdicts = collector.verdicts_against(file_id, &par2_set, par2_file_id);
    let evidence = super::slice_evidence_from_verdicts(
        par2_set.recovery_set_id,
        par2_file_id,
        payload.len() as u64,
        block_size.get(),
        &verdicts,
    );

    // Settle-time scoping (gate 4's second half), expressed where this change
    // owns it: the blocks seeded as evidence are exactly the blocks settle-time
    // will not read, and they are exactly the intact ones. Nothing is both
    // claimed and read back, and nothing is neither.
    let seeded: Vec<u32> = evidence
        .iter()
        .map(par2_rs::SliceEvidence::slice_index)
        .collect();
    let unclaimed: Vec<u32> = verdicts
        .iter()
        .filter(|(_, verdict)| !matches!(verdict, BlockVerdict::Intact { .. }))
        .map(|(index, _)| *index)
        .collect();
    assert_eq!(unclaimed, vec![2, 5], "verdicts {verdicts:?}");
    assert_eq!(seeded, vec![0, 1, 3, 4, 6, 7, 8, 9]);
    assert!(
        seeded.iter().all(|index| !unclaimed.contains(index)),
        "a block must never be both claimed in stream and read back"
    );
    // Every verdict is CRC32-only: no MD5 was computed on the download path.
    assert!(evidence.iter().all(|slice| {
        slice.strength() == par2_rs::SliceEvidenceStrength::Crc32Only
            && slice.is_valid()
            && slice.may_seed_repair_input()
    }));

    // Evidence -> session. The session is access-backed, so it never scans and
    // reports exactly what the evidence established.
    let working = tempfile::tempdir().expect("temp dir");
    let mut access = par2_rs::MemoryFileAccess::new();
    access.add_file(par2_file_id, stored.clone());
    let mut options = par2_rs::Par2RepairSessionOptions::with_source_access(
        working.path().to_path_buf(),
        Vec::new(),
        std::sync::Arc::new(access),
    );
    options.file_set = Some(par2_set.clone());
    let mut session = par2_rs::Par2RepairSession::open(options).expect("open session");
    for slice in evidence {
        session
            .add_slice_evidence_for_file(slice)
            .expect("in-stream evidence seeds the session");
    }

    let assessment = session.analyze().expect("analyze");
    assert_eq!(
        assessment.status,
        par2_rs::Par2RepairStatus::RepairPossible,
        "the two damaged blocks must be repairable from recovery data"
    );
    assert_eq!(assessment.missing_blocks, 2);
    assert_eq!(session.diagnostics().source_scan_passes, 0);
    assert_eq!(session.diagnostics().scan.bytes_scanned, 0);

    // The session names exactly the damaged blocks, from evidence alone.
    let file = assessment
        .verification
        .files
        .iter()
        .find(|file| file.file_id == par2_file_id)
        .expect("the protected file");
    let missing: Vec<u32> = file
        .valid_slices
        .iter()
        .enumerate()
        .filter(|(_, valid)| !**valid)
        .map(|(index, _)| index as u32)
        .collect();
    assert_eq!(missing, vec![2, 5], "valid_slices {:?}", file.valid_slices);

    // Session -> repair. Repair re-derives CRC32 and MD5 over every byte it
    // consumes, so this also proves the in-stream verdicts were truthful.
    let outcome = session.repair().expect("repair from in-stream evidence");
    assert_eq!(outcome.status, par2_rs::Par2RepairStatus::Repaired);
    assert_eq!(
        std::fs::read(working.path().join("payload.bin")).expect("repaired file"),
        payload,
        "repair must reproduce the true payload"
    );
}

// ---------------------------------------------------------------------------
// Evidence-correctness battery for the quick-verification consistency fix:
// pCRC-gated independent coverage + duplicate/replay invalidation.
// ---------------------------------------------------------------------------

/// A recovery set whose IFSC entries are computed from the given bytes, so
/// verdicts against it are `Intact` exactly when the collector derived the
/// true CRC of those bytes.
fn set_for_bytes(bytes: &[u8], block_size: u64) -> (par2_rs::Par2FileSet, par2_rs::FileId) {
    let set = fixture_set("payload", bytes, block_size);
    let par2_file_id = par2_file_id("payload", bytes);
    (set, par2_file_id)
}

#[test]
fn unverified_pcrc_blocks_close_but_mint_no_independent_evidence() {
    let block_size = block(1024);
    let file_id = file(90);
    let data = random_bytes(0x9e1, 2048);
    let mut collector = BlockCrcCollector::new();

    let (crc_a, segs_a) = decode_article_segments(&data[..1024], 0, Some(block_size), 333);
    let (crc_b, segs_b) = decode_article_segments(&data[1024..], 1024, Some(block_size), 333);
    // Article A never verified its declared pcrc32; article B did.
    collector.note_article(file_id, block_size, 0, 1024, crc_a, false, false, &segs_a);
    collector.note_article(file_id, block_size, 1024, 1024, crc_b, true, false, &segs_b);
    collector.note_file_len(file_id, 2048);

    // Both blocks closed with correct CRCs...
    assert_eq!(
        collector.derived_block_crc(file_id, 0),
        Some(direct_crc(&data[..1024]))
    );
    assert_eq!(
        collector.derived_block_crc(file_id, 1),
        Some(direct_crc(&data[1024..]))
    );

    let (set, par2_file_id) = set_for_bytes(&data, 1024);
    let verdicts = collector.verdicts_against(file_id, &set, par2_file_id);
    assert_eq!(
        verdicts.get(&0),
        Some(&BlockVerdict::Intact {
            independently_covered: false
        })
    );
    assert_eq!(
        verdicts.get(&1),
        Some(&BlockVerdict::Intact {
            independently_covered: true
        })
    );

    // ...but only the independently covered block mints slice evidence.
    let evidence =
        slice_evidence_from_verdicts(set.recovery_set_id, par2_file_id, 2048, 1024, &verdicts);
    assert_eq!(evidence.len(), 1);
}

#[test]
fn mixed_pcrc_contributions_to_one_block_deny_independent_coverage() {
    let block_size = block(1024);
    let file_id = file(91);
    let data = random_bytes(0x9e2, 1024);
    let mut collector = BlockCrcCollector::new();

    // Two articles tile one block; only one verified its pcrc32.
    let (crc_a, segs_a) = decode_article_segments(&data[..512], 0, Some(block_size), 100);
    let (crc_b, segs_b) = decode_article_segments(&data[512..], 512, Some(block_size), 100);
    collector.note_article(file_id, block_size, 0, 512, crc_a, true, false, &segs_a);
    collector.note_article(file_id, block_size, 512, 512, crc_b, false, false, &segs_b);
    collector.note_file_len(file_id, 1024);

    let (set, par2_file_id) = set_for_bytes(&data, 1024);
    let verdicts = collector.verdicts_against(file_id, &set, par2_file_id);
    assert_eq!(
        verdicts.get(&0),
        Some(&BlockVerdict::Intact {
            independently_covered: false
        })
    );
    assert!(
        slice_evidence_from_verdicts(set.recovery_set_id, par2_file_id, 1024, 1024, &verdicts)
            .is_empty()
    );
}

#[test]
fn identical_replay_readjudicates_instead_of_trusting_crc_equality() {
    let block_size = block(1024);
    let file_id = file(92);
    let data = random_bytes(0x9e3, 1024);
    let mut collector = BlockCrcCollector::new();

    let (crc, segs) = decode_article_segments(&data, 0, Some(block_size), 256);
    collector.note_article(file_id, block_size, 0, 1024, crc, true, false, &segs);
    collector.note_file_len(file_id, 1024);
    let derived_before = collector.derived_block_crc(file_id, 0);
    assert!(derived_before.is_some());
    let blocks_before = collector.blocks_derived();

    // A replay with the same placement and part CRC still rewrote the range,
    // and CRC32 equality cannot prove it wrote the same bytes. The earlier
    // verdict dies and the block re-derives from the replay's own segments —
    // which tile it completely here, so it closes right back to the same
    // value, through a fresh adjudication rather than a trusted shortcut.
    collector.note_article(file_id, block_size, 0, 1024, crc, true, true, &segs);
    assert_eq!(collector.derived_block_crc(file_id, 0), derived_before);
    assert_eq!(collector.blocks_derived(), blocks_before + 1);
}

#[test]
fn identical_replay_of_a_partial_contribution_leaves_the_block_unclaimed() {
    let block_size = block(1024);
    let file_id = file(94);
    let data = random_bytes(0x9e4, 1024);
    let mut collector = BlockCrcCollector::new();

    // Two articles tile one block; closing it retires both contributions.
    let (crc_a, segs_a) = decode_article_segments(&data[..512], 0, Some(block_size), 256);
    let (crc_b, segs_b) = decode_article_segments(&data[512..], 512, Some(block_size), 256);
    collector.note_article(file_id, block_size, 0, 512, crc_a, true, false, &segs_a);
    collector.note_article(file_id, block_size, 512, 512, crc_b, true, false, &segs_b);
    collector.note_file_len(file_id, 1024);
    assert_eq!(
        collector.derived_block_crc(file_id, 0),
        Some(direct_crc(&data))
    );

    // An identical replay of just the first article invalidates the block —
    // the rewrite spans only half of it, and the other half's retired
    // contribution cannot vouch for a range it did not re-observe — so the
    // block stays unclaimed for the read paths until the neighbor replays.
    collector.note_article(file_id, block_size, 0, 512, crc_a, true, true, &segs_a);
    assert_eq!(collector.derived_block_crc(file_id, 0), None);

    // The unchanged neighbor replaying is what re-tiles and re-closes it.
    collector.note_article(file_id, block_size, 512, 512, crc_b, true, true, &segs_b);
    assert_eq!(
        collector.derived_block_crc(file_id, 0),
        Some(direct_crc(&data))
    );
}

/// Not a correctness gate: measures `verdicts_against` at a large-file
/// shape so the completion-time cost of building the verdict map twice
/// (damage veto + grid match) is a number rather than a guess. Run with
/// `--release -- --nocapture` for meaningful output.
#[test]
fn verdict_map_construction_cost_at_large_file_shape() {
    let block_size = block(1024);
    let file_id = file(120);
    let blocks = 16_384usize;
    let data = random_bytes(0xbe9c, blocks * 1024);
    let (set, par2_id) = set_for_bytes(&data, 1024);
    let mut collector = BlockCrcCollector::new();
    for index in 0..blocks {
        let start = index * 1024;
        let (crc, segs) = decode_article_segments(
            &data[start..start + 1024],
            start as u64,
            Some(block_size),
            512,
        );
        collector.note_article(
            file_id,
            block_size,
            start as u64,
            1024,
            crc,
            true,
            false,
            &segs,
        );
    }
    collector.note_file_len(file_id, (blocks * 1024) as u64);

    let iterations = 100u32;
    let started = std::time::Instant::now();
    let mut verdict_count = 0usize;
    for _ in 0..iterations {
        verdict_count += collector.verdicts_against(file_id, &set, par2_id).len();
    }
    let elapsed = started.elapsed();
    assert_eq!(verdict_count, blocks * iterations as usize);
    eprintln!(
        "verdicts_against: {blocks} blocks, {:?}/build ({:.1} ns/block); double-build cost/file = {:?}",
        elapsed / iterations,
        elapsed.as_nanos() as f64 / f64::from(iterations) / blocks as f64,
        elapsed / iterations
    );
}

#[test]
fn duplicate_at_a_different_offset_invalidates_what_it_overlaps() {
    let block_size = block(1024);
    let file_id = file(99);
    let data = random_bytes(0x9e8, 2048);
    let mut collector = BlockCrcCollector::new();

    // Two articles close both blocks.
    let (crc_a, segs_a) = decode_article_segments(&data[..1024], 0, Some(block_size), 256);
    let (crc_b, segs_b) = decode_article_segments(&data[1024..], 1024, Some(block_size), 256);
    collector.note_article(file_id, block_size, 0, 1024, crc_a, true, false, &segs_a);
    collector.note_article(file_id, block_size, 1024, 1024, crc_b, true, false, &segs_b);
    collector.note_file_len(file_id, 2048);
    assert!(collector.derived_block_crc(file_id, 0).is_some());
    assert!(collector.derived_block_crc(file_id, 1).is_some());

    // The same ordinal re-delivered with a different bounded placement: it
    // rewrote [512, 1536), straddling both blocks, at an offset no earlier
    // evidence was keyed by. Both verdicts must die; the replay's own
    // segments cannot tile either block alone, so both stay unclaimed.
    let rewrite = random_bytes(0x9e9, 1024);
    let (crc_r, segs_r) = decode_article_segments(&rewrite, 512, Some(block_size), 256);
    collector.note_article(file_id, block_size, 512, 1024, crc_r, true, true, &segs_r);
    assert_eq!(collector.derived_block_crc(file_id, 0), None);
    assert_eq!(collector.derived_block_crc(file_id, 1), None);

    // Fresh observations of the un-rewritten flanks provide complete
    // coverage again; both blocks re-derive over the disk as it is NOW.
    let (crc_l, segs_l) = decode_article_segments(&data[..512], 0, Some(block_size), 256);
    let (crc_t, segs_t) = decode_article_segments(&data[1536..], 1536, Some(block_size), 256);
    collector.note_article(file_id, block_size, 0, 512, crc_l, true, false, &segs_l);
    collector.note_article(file_id, block_size, 1536, 512, crc_t, true, false, &segs_t);
    let mut expected_block0 = data[..512].to_vec();
    expected_block0.extend_from_slice(&rewrite[..512]);
    let mut expected_block1 = rewrite[512..].to_vec();
    expected_block1.extend_from_slice(&data[1536..]);
    assert_eq!(
        collector.derived_block_crc(file_id, 0),
        Some(direct_crc(&expected_block0))
    );
    assert_eq!(
        collector.derived_block_crc(file_id, 1),
        Some(direct_crc(&expected_block1))
    );
}

#[test]
fn shorter_duplicate_invalidates_only_the_bytes_it_rewrote() {
    let block_size = block(1024);
    let file_id = file(100);
    let data = random_bytes(0x9ea, 2048);
    let mut collector = BlockCrcCollector::new();

    let (crc_a, segs_a) = decode_article_segments(&data[..1024], 0, Some(block_size), 256);
    let (crc_b, segs_b) = decode_article_segments(&data[1024..], 1024, Some(block_size), 256);
    collector.note_article(file_id, block_size, 0, 1024, crc_a, true, false, &segs_a);
    collector.note_article(file_id, block_size, 1024, 1024, crc_b, true, false, &segs_b);
    collector.note_file_len(file_id, 2048);

    // The ordinal returns with a shorter valid extent: only [0, 512) was
    // rewritten. Block 0 dies and stays unclaimed — its other half's retired
    // contribution cannot vouch again — while block 1's bytes were never
    // touched and its verdict survives.
    let (crc_s, segs_s) = decode_article_segments(&data[..512], 0, Some(block_size), 256);
    collector.note_article(file_id, block_size, 0, 512, crc_s, true, true, &segs_s);
    assert_eq!(collector.derived_block_crc(file_id, 0), None);
    assert!(collector.derived_block_crc(file_id, 1).is_some());

    // Re-observing the other half completes coverage and re-derives.
    let (crc_h, segs_h) = decode_article_segments(&data[512..1024], 512, Some(block_size), 256);
    collector.note_article(file_id, block_size, 512, 512, crc_h, true, true, &segs_h);
    assert_eq!(
        collector.derived_block_crc(file_id, 0),
        Some(direct_crc(&data[..1024]))
    );
}

#[test]
fn pending_pressure_clears_only_incomplete_runs() {
    let block_size = block(1024);
    let file_id = file(95);
    let data = random_bytes(0x9e5, 1024);
    let (set, par2_id) = set_for_bytes(&data, 1024);
    let mut collector = BlockCrcCollector::new();

    // A block derives Intact before anything goes wrong.
    let (crc, segs) = decode_article_segments(&data, 0, Some(block_size), 256);
    collector.note_article(file_id, block_size, 0, 1024, crc, true, false, &segs);
    collector.note_file_len(file_id, 1024);
    assert!(collector.derived_block_crc(file_id, 0).is_some());

    // Non-tiling one-byte articles pile up until the per-file pending cap
    // clears that grid's incomplete runs. Retired evidence is already exact
    // and must survive this bounded-memory pressure.
    for index in 0..=MAX_PENDING_RUNS_PER_FILE {
        let offset = 10 * 1024 + (index as u64 * 2);
        let segment = Segment {
            file_offset: offset,
            len: 1,
            crc32: 0xdead_beef,
        };
        collector.note_article(
            file_id,
            block_size,
            offset,
            1,
            0xdead_beef,
            true,
            false,
            &[segment],
        );
    }
    assert_eq!(collector.derived_block_crc(file_id, 0), Some(crc));
    assert_eq!(collector.verdicts_against(file_id, &set, par2_id).len(), 1);
    assert_eq!(
        collector.entry_counts_for_job(JOB),
        EntryCounts {
            pending: 1,
            derived: 1,
        }
    );

    // A later conflicting rewrite replaces exactly the affected retired
    // evidence. Its whole-block fallback is sufficient to retain the new
    // bytes, which then compare as a contradiction against the old PAR2 set.
    collector.note_article(file_id, block_size, 0, 1024, !crc, true, true, &[]);
    assert_eq!(collector.derived_block_crc(file_id, 0), Some(!crc));
    assert_eq!(collector.verdicts_against(file_id, &set, par2_id).len(), 1);
}

#[test]
fn short_final_block_verdict_requires_length_congruence() {
    let block_size = block(1024);
    let data = random_bytes(0x9e6, 1536); // block 0 full, block 1 short

    // Congruent lengths: both blocks get verdicts.
    let file_id = file(96);
    let (set, par2_id) = set_for_bytes(&data, 1024);
    let mut collector = BlockCrcCollector::new();
    for (start, end) in [(0usize, 1024usize), (1024, 1536)] {
        let (crc, segs) =
            decode_article_segments(&data[start..end], start as u64, Some(block_size), 256);
        collector.note_article(
            file_id,
            block_size,
            start as u64,
            (end - start) as u64,
            crc,
            true,
            false,
            &segs,
        );
    }
    collector.note_file_len(file_id, 1536);
    let verdicts = collector.verdicts_against(file_id, &set, par2_id);
    assert_eq!(verdicts.len(), 2);

    // The collector believes a different final extent than the description:
    // the full interior block still compares 1:1, but the short final block
    // was closed — and would be zero-padded — on the wrong basis, so it gets
    // no verdict at all rather than a wrong one.
    let file_id = file(97);
    let mut longer = data.clone();
    longer.extend_from_slice(&random_bytes(0x9e7, 64));
    let (set_longer, par2_id_longer) = set_for_bytes(&longer, 1024);
    let mut collector = BlockCrcCollector::new();
    for (start, end) in [(0usize, 1024usize), (1024, 1536)] {
        let (crc, segs) =
            decode_article_segments(&data[start..end], start as u64, Some(block_size), 256);
        collector.note_article(
            file_id,
            block_size,
            start as u64,
            (end - start) as u64,
            crc,
            true,
            false,
            &segs,
        );
    }
    collector.note_file_len(file_id, 1536); // description says 1600
    let verdicts = collector.verdicts_against(file_id, &set_longer, par2_id_longer);
    assert_eq!(verdicts.len(), 1, "only the full interior block may speak");
    assert!(verdicts.contains_key(&0));

    // Description shorter than the collector's interior extent: a full block
    // that reaches past the described end is not a slice of this description
    // and must stay silent too.
    let file_id = file(98);
    let short_desc = &data[..900];
    let (set_short, par2_id_short) = set_for_bytes(short_desc, 1024);
    let mut collector = BlockCrcCollector::new();
    let (crc, segs) = decode_article_segments(&data[..1024], 0, Some(block_size), 256);
    collector.note_article(file_id, block_size, 0, 1024, crc, true, false, &segs);
    collector.note_file_len(file_id, 1536);
    assert!(collector.derived_block_crc(file_id, 0).is_some());
    assert!(
        collector
            .verdicts_against(file_id, &set_short, par2_id_short)
            .is_empty()
    );
}

#[test]
fn conflicting_replay_invalidates_and_rederives_from_the_rewrite() {
    let block_size = block(1024);
    let file_id = file(93);
    let original = random_bytes(0x1a2b_3c4d, 1024);
    let rewritten = random_bytes(0x5f6e_7d8c, 1024);
    let mut collector = BlockCrcCollector::new();

    let (crc_orig, segs_orig) = decode_article_segments(&original, 0, Some(block_size), 256);
    collector.note_article(
        file_id, block_size, 0, 1024, crc_orig, true, false, &segs_orig,
    );
    collector.note_file_len(file_id, 1024);
    assert_eq!(
        collector.derived_block_crc(file_id, 0),
        Some(direct_crc(&original))
    );

    // The replay carries different bytes: the earlier verdict must die and
    // the block must re-derive from the rewrite, never keep describing the
    // pre-rewrite content.
    let (crc_new, segs_new) = decode_article_segments(&rewritten, 0, Some(block_size), 256);
    assert_ne!(crc_orig, crc_new, "fixture must actually differ");
    collector.note_article(file_id, block_size, 0, 1024, crc_new, true, true, &segs_new);
    assert_eq!(
        collector.derived_block_crc(file_id, 0),
        Some(direct_crc(&rewritten))
    );

    // And the verdict grid agrees with the rewritten content, not the original.
    let (set_new, id_new) = set_for_bytes(&rewritten, 1024);
    let verdicts = collector.verdicts_against(file_id, &set_new, id_new);
    assert!(matches!(
        verdicts.get(&0),
        Some(BlockVerdict::Intact { .. })
    ));
    let (set_old, id_old) = set_for_bytes(&original, 1024);
    let verdicts_old = collector.verdicts_against(file_id, &set_old, id_old);
    assert_eq!(verdicts_old.get(&0), Some(&BlockVerdict::Damaged));
}

#[test]
fn partial_block_replay_keeps_unrewritten_contributions() {
    let block_size = block(1024);
    let file_id = file(94);
    let first_half = random_bytes(0x1357_9bdf, 512);
    let second_half = random_bytes(0x2468_ace0, 512);
    let replacement_first_half = random_bytes(0xfdb9_7531, 512);
    let mut collector = BlockCrcCollector::new();

    let (crc_a, segs_a) = decode_article_segments(&first_half, 0, Some(block_size), 128);
    let (crc_b, segs_b) = decode_article_segments(&second_half, 512, Some(block_size), 128);
    collector.note_article(file_id, block_size, 0, 512, crc_a, true, false, &segs_a);
    collector.note_article(file_id, block_size, 512, 512, crc_b, true, false, &segs_b);
    collector.note_file_len(file_id, 1024);
    let mut whole: Vec<u8> = first_half.clone();
    whole.extend_from_slice(&second_half);
    assert_eq!(
        collector.derived_block_crc(file_id, 0),
        Some(direct_crc(&whole))
    );

    // Rewrite only the first article's range. The earlier verdict dies, and —
    // because the second half's segments were retired when the block first
    // closed — the block is UNCLAIMED, not re-derived: current segments alone
    // cannot reconstruct the rewritten slice, so settle-time verification
    // owns it (the handoff's rule, verbatim).
    let (crc_a2, segs_a2) =
        decode_article_segments(&replacement_first_half, 0, Some(block_size), 128);
    assert_ne!(crc_a, crc_a2, "fixture must actually differ");
    collector.note_article(file_id, block_size, 0, 512, crc_a2, true, true, &segs_a2);
    assert_eq!(collector.derived_block_crc(file_id, 0), None);

    // The pipeline re-feeds duplicates through this seam deliberately. When
    // the unchanged second half replays (byte-identical duplicate), its
    // segments re-enter pending and the block re-closes over the bytes that
    // are actually on disk now: replacement + surviving second half.
    collector.note_article(file_id, block_size, 512, 512, crc_b, true, true, &segs_b);
    let mut rewritten: Vec<u8> = replacement_first_half.clone();
    rewritten.extend_from_slice(&second_half);
    assert_eq!(
        collector.derived_block_crc(file_id, 0),
        Some(direct_crc(&rewritten))
    );
}

#[test]
fn common_refinement_segments_close_each_admitted_grid_independently() {
    let file_id = file(95);
    let bytes = random_bytes(0x1eaf_cafe, 192);
    let mut collector = BlockCrcCollector::new();
    let segments = [
        Segment {
            file_offset: 0,
            len: 64,
            crc32: direct_crc(&bytes[..64]),
        },
        Segment {
            file_offset: 64,
            len: 32,
            crc32: direct_crc(&bytes[64..96]),
        },
        Segment {
            file_offset: 96,
            len: 32,
            crc32: direct_crc(&bytes[96..128]),
        },
        Segment {
            file_offset: 128,
            len: 64,
            crc32: direct_crc(&bytes[128..]),
        },
    ];

    collector.note_article_on_grids(
        file_id,
        &[block(64), block(96)],
        0,
        bytes.len() as u64,
        direct_crc(&bytes),
        true,
        false,
        &segments,
    );
    collector.note_file_len(file_id, bytes.len() as u64);

    let entry = collector.files.get(&file_id).expect("file tracked");
    assert_eq!(entry.grids.get(&64).expect("64 KiB grid").derived.len(), 3);
    assert_eq!(entry.grids.get(&96).expect("96 KiB grid").derived.len(), 2);
    assert_eq!(
        entry.grids.get(&64).expect("64 KiB grid").derived[&1].crc32,
        direct_crc(&bytes[64..128])
    );
    assert_eq!(
        entry.grids.get(&96).expect("96 KiB grid").derived[&1].crc32,
        direct_crc(&bytes[96..])
    );
}

#[test]
fn a_coarse_fallback_crossing_a_grid_boundary_is_not_retained() {
    let file_id = file(96);
    let bytes = random_bytes(0x715e_5afe, 100);
    let segment = Segment {
        file_offset: 0,
        len: bytes.len() as u64,
        crc32: direct_crc(&bytes),
    };
    let mut collector = BlockCrcCollector::new();

    collector.note_article_on_grids(
        file_id,
        &[block(64), block(96)],
        0,
        bytes.len() as u64,
        segment.crc32,
        true,
        false,
        &[segment],
    );
    collector.note_file_len(file_id, bytes.len() as u64);

    let entry = collector.files.get(&file_id).expect("file tracked");
    assert!(
        entry
            .grids
            .get(&64)
            .is_none_or(|grid| grid.pending.is_empty())
    );
    assert!(
        entry
            .grids
            .get(&96)
            .is_none_or(|grid| grid.pending.is_empty())
    );
    assert!(
        entry
            .grids
            .get(&64)
            .is_none_or(|grid| grid.derived.is_empty())
    );
    assert!(
        entry
            .grids
            .get(&96)
            .is_none_or(|grid| grid.derived.is_empty())
    );
}
