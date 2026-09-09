//! Tests for the direct-store coverage checkpoint.
//!
//! Fixture names are invented throughout — never real media titles.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use weaver_model::files::FileRole;

use super::barrier::{
    BARRIER_DIRTY_AGE, BARRIER_DIRTY_BYTES, BARRIER_FAILURE_BACKOFF, BARRIER_FAILURE_BACKOFF_MAX,
    BarrierDemand, BarrierDrain, BarrierError, BarrierStep, BarrierTrigger, CoverageBarrier,
    CoveragePersist, DatabaseCoveragePersist, DestinationSync, RoutedWrite, WriteRefused,
};
use super::plan::DirectSetPlan;
use super::restart::{
    CoverageRejection, DestinationProbe, DestinationRoots, ExpectedSet, ProbedDestination,
    complete_files, coverage_skip_plan, refetch_floors, restore_job, restore_set,
    restore_set_with_probe,
};
use super::router::{
    CrcRuns, DemotionReason, DirectSetRouter, HoldsScratch, SparseImage,
    restored_volume_is_confirmed,
};
use super::snapshot::{
    CoverageSnapshot, DestinationClaim, DestinationExtent, SNAPSHOT_MAGIC, SNAPSHOT_SCHEMA_VERSION,
    SnapshotError, VolumeFloor, decode, encode,
};
use super::{ByteRanges, DirectStoreGate, parse_enabled};
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::jobs::model::{FileSpec, JobSpec, SegmentSpec};

use super::par2_access::{DirectVolumeFileAccess, VirtualPar2Volume};

use par2_rs::FileAccess;

/// The reconstruction sweep as its single-volume callers read it: `Ok` when
/// every volume rebuilt, `Err(first failure)` when one did not.
///
/// [`super::reconstruct::reconstruct_volumes`] itself reports per volume now —
/// a refused volume comes back with `contiguous: 0` and its own `failure`, and
/// its siblings keep their bytes. Every test below drives exactly one volume,
/// where the two shapes say the same thing; the per-volume behaviour has its
/// own test in `pipeline::tests::direct_store`.
fn sweep_volumes(
    provider: &super::provider::HybridVolumeProvider,
    plans: &[super::reconstruct::VolumeReconstruction],
    sparse: super::sparse::SparseMarking,
) -> Result<Vec<super::reconstruct::ReconstructedVolume>, super::reconstruct::ReconstructionFailure>
{
    let rebuilt = super::reconstruct::reconstruct_volumes(provider, plans, sparse);
    match rebuilt.iter().find_map(|volume| volume.failure.clone()) {
        Some(failure) => Err(failure),
        None => Ok(rebuilt),
    }
}

const JOB: JobId = JobId(7701);

const SET: &str = "Silver.Horizon.S01E04";

const PLAN_DIGEST: [u8; 32] = [0xA5; 32];

const OTHER_DIGEST: [u8; 32] = [0x5A; 32];

#[derive(Debug, Clone, PartialEq, Eq)]
enum Op {
    Drain,
    Sync(String),
    Write { set_name: String, bytes: usize },
    Delete { set_name: String },
}

#[derive(Debug, Default)]
struct Journal {
    ops: Vec<Op>,
    fail_drain: Option<String>,
    fail_sync: Option<String>,
    fail_write: Option<String>,
    committed: Option<Vec<u8>>,
}

/// One shared journal behind all three barrier traits, so the recorded order is
/// a single interleaved log across drain, sync and persist.
#[derive(Debug, Clone, Default)]
struct Recorder {
    journal: Arc<Mutex<Journal>>,
}

impl Recorder {
    fn with<T>(&self, apply: impl FnOnce(&mut Journal) -> T) -> T {
        apply(&mut self.journal.lock().unwrap())
    }

    fn ops(&self) -> Vec<Op> {
        self.with(|journal| journal.ops.clone())
    }

    fn committed(&self) -> Option<Vec<u8>> {
        self.with(|journal| journal.committed.clone())
    }

    fn fail_drain(&self, error: &str) {
        self.with(|journal| journal.fail_drain = Some(error.to_string()));
    }

    fn fail_sync(&self, error: &str) {
        self.with(|journal| journal.fail_sync = Some(error.to_string()));
    }

    fn fail_write(&self, error: &str) {
        self.with(|journal| journal.fail_write = Some(error.to_string()));
    }

    fn writes(&self) -> usize {
        self.ops()
            .iter()
            .filter(|op| matches!(op, Op::Write { .. }))
            .count()
    }

    fn deletes(&self) -> usize {
        self.ops()
            .iter()
            .filter(|op| matches!(op, Op::Delete { .. }))
            .count()
    }

    fn synced(&self) -> Vec<String> {
        self.ops()
            .iter()
            .filter_map(|op| match op {
                Op::Sync(path) => Some(path.clone()),
                _ => None,
            })
            .collect()
    }

    fn steps(&self) -> Vec<&'static str> {
        self.ops()
            .iter()
            .map(|op| match op {
                Op::Drain => "drain",
                Op::Sync(_) => "sync",
                Op::Write { .. } => "write",
                Op::Delete { .. } => "delete",
            })
            .collect()
    }
}

impl BarrierDrain for Recorder {
    fn drain(&mut self) -> Result<(), String> {
        self.with(|journal| {
            if let Some(error) = journal.fail_drain.clone() {
                return Err(error);
            }
            journal.ops.push(Op::Drain);
            Ok(())
        })
    }
}

impl DestinationSync for Recorder {
    fn sync(&mut self, relative_path: &str) -> Result<(), String> {
        self.with(|journal| {
            if let Some(error) = journal.fail_sync.clone() {
                return Err(error);
            }
            journal.ops.push(Op::Sync(relative_path.to_string()));
            Ok(())
        })
    }
}

impl CoveragePersist for Recorder {
    fn write(&mut self, _job_id: JobId, set_name: &str, blob: &[u8]) -> Result<(), String> {
        self.with(|journal| {
            if let Some(error) = journal.fail_write.clone() {
                return Err(error);
            }
            journal.ops.push(Op::Write {
                set_name: set_name.to_string(),
                bytes: blob.len(),
            });
            journal.committed = Some(blob.to_vec());
            Ok(())
        })
    }

    fn delete(&mut self, _job_id: JobId, set_name: &str) -> Result<(), String> {
        self.with(|journal| {
            journal.ops.push(Op::Delete {
                set_name: set_name.to_string(),
            });
            journal.committed = None;
            Ok(())
        })
    }
}

/// Drives the barrier with one recorder standing in for all three traits.
fn run_barrier(
    barrier: &mut CoverageBarrier,
    recorder: &Recorder,
    trigger: BarrierTrigger,
) -> Result<super::barrier::BarrierReport, BarrierError> {
    run_barrier_at(barrier, recorder, trigger, Instant::now())
}

/// [`run_barrier`] on a synthetic clock, for the failure-backoff tests.
fn run_barrier_at(
    barrier: &mut CoverageBarrier,
    recorder: &Recorder,
    trigger: BarrierTrigger,
    now: Instant,
) -> Result<super::barrier::BarrierReport, BarrierError> {
    let (mut drain, mut sync, mut persist) = (recorder.clone(), recorder.clone(), recorder.clone());
    barrier.barrier(trigger, now, &mut drain, &mut sync, &mut persist)
}

fn write(volume_index: u32, source_offset: u64, len: u64, member_index: u32) -> RoutedWrite {
    RoutedWrite {
        volume_index,
        source_offset,
        len,
        member_index,
        destination_offset: source_offset,
    }
}

fn sample_barrier() -> CoverageBarrier {
    let mut barrier = CoverageBarrier::new(JOB, SET, PLAN_DIGEST);
    barrier.register_volume(0, 0);
    barrier.register_volume(1, 1);
    barrier.register_destination(0, "silver-horizon.mkv.f0.direct.partial");
    barrier.register_destination(1, "silver-horizon.nfo.f0.direct.partial");
    barrier
}

fn sample_snapshot() -> CoverageSnapshot {
    CoverageSnapshot {
        generation: 3,
        plan_digest: PLAN_DIGEST,
        destinations: vec![DestinationClaim {
            member_index: 0,
            relative_path: "silver-horizon.mkv.f0.direct.partial".to_string(),
            extents: vec![DestinationExtent { start: 0, end: 60 }],
            crypt: None,
        }],
        floors: vec![VolumeFloor {
            volume_index: 0,
            file_index: 0,
            floor: 60,
            complete: false,
        }],
    }
}

/// The plan facts [`sample_snapshot`] was written against: one volume, mapped
/// to NZB file 0.
fn sample_expected() -> ExpectedSet {
    ExpectedSet {
        plan_digest: PLAN_DIGEST,
        volume_files: HashMap::from([(0u32, 0u32)]),
        fact_volumes: HashSet::from([0u32]),
    }
}

fn forge_garbage() -> Vec<u8> {
    let mut blob = Vec::new();
    blob.extend_from_slice(&SNAPSHOT_MAGIC);
    blob.extend_from_slice(&SNAPSHOT_SCHEMA_VERSION.to_le_bytes());
    blob.extend_from_slice(b"not messagepack at all");
    blob
}

fn barrier_with_one_committed_checkpoint() -> (CoverageBarrier, Recorder) {
    let mut barrier = sample_barrier();
    let now = Instant::now();
    barrier.record_write(&write(0, 0, 4_096, 0), now).unwrap();
    let recorder = Recorder::default();
    run_barrier(&mut barrier, &recorder, BarrierTrigger::DirtyBytes).unwrap();
    assert_eq!(barrier.generation(), 1);
    (barrier, recorder)
}

fn barrier_with_volumes(volume_count: u32) -> CoverageBarrier {
    let mut barrier = CoverageBarrier::new(JOB, SET, PLAN_DIGEST);
    barrier.register_destination(0, "silver-horizon.s01.mkv.f0.direct.partial");
    let now = Instant::now();
    for volume_index in 0..volume_count {
        barrier.register_volume(volume_index, volume_index);
        barrier
            .record_write(&write(volume_index, 0, 4_096, 0), now)
            .unwrap();
    }
    barrier
}

fn write_destination(dir: &Path, relative_path: &str, len: usize) {
    if let Some(parent) = dir.join(relative_path).parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(dir.join(relative_path), vec![0u8; len]).unwrap();
}

/// A job's two roots, **deliberately on different paths** inside one temp dir.
///
/// The whole point of the split is that member payload and working data live
/// apart, so a restart test that resolved a member claim against the working
/// directory would pass against a single shared root and prove nothing. Here the
/// staging root is the only place a `.direct.partial` is written, so a claim sent
/// to the wrong root fails the probe.
fn sample_roots(temp_dir: &Path) -> DestinationRoots {
    let roots = DestinationRoots {
        working_dir: temp_dir.join("intermediate").join("Silver Horizon"),
        destination_dir: temp_dir
            .join("complete")
            .join(".weaver-staging")
            .join(JOB.0.to_string()),
    };
    std::fs::create_dir_all(&roots.working_dir).unwrap();
    std::fs::create_dir_all(&roots.destination_dir).unwrap();
    roots
}

fn direct_job_spec() -> JobSpec {
    JobSpec {
        name: "Silver Horizon".to_string(),
        password: None,
        total_bytes: 60,
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: "silver-horizon.part01.rar".to_string(),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: vec![
                SegmentSpec {
                    ordinal: 0,
                    article_number: 1,
                    bytes: 10,
                    message_id: "one@example.invalid".to_string(),
                },
                SegmentSpec {
                    ordinal: 1,
                    article_number: 2,
                    bytes: 20,
                    message_id: "two@example.invalid".to_string(),
                },
                SegmentSpec {
                    ordinal: 2,
                    article_number: 3,
                    bytes: 30,
                    message_id: "three@example.invalid".to_string(),
                },
            ],
        }],
    }
}

fn segment(segment_number: u32) -> SegmentId {
    SegmentId {
        file_id: NzbFileId {
            job_id: JOB,
            file_index: 0,
        },
        segment_number,
    }
}

fn direct_active_job() -> crate::ActiveJob {
    crate::ActiveJob {
        job_id: JOB,
        nzb_hash: [0xAA; 32],
        nzb_path: std::path::PathBuf::from("/tmp/silver-horizon.nzb"),
        nzb_zstd: crate::ingest::compress_nzb_bytes(
            br#"<?xml version="1.0" encoding="UTF-8"?>
            <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
              <file poster="poster" date="1700000000" subject="sample">
                <groups><group>alt.binaries.test</group></groups>
                <segments>
                  <segment bytes="10" number="1">abc@example.invalid</segment>
                </segments>
              </file>
            </nzb>"#,
        )
        .unwrap(),
        output_dir: std::path::PathBuf::from("/tmp/silver-horizon"),
        created_at: 1_700_000_000,
        category: None,
        metadata: vec![],
        status: "queued",
        download_state: "queued",
        post_state: "idle",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
    }
}

fn envelope_plan() -> DirectSetPlan {
    DirectSetPlan {
        set_name: "Silver.Horizon.S01E05".to_string(),
        volumes: [(0u32, 0u32), (1, 1)].into_iter().collect(),
        files: [(0u32, 0u32), (1, 1)].into_iter().collect(),
        identity: None,
        working_dir: std::path::PathBuf::from("/tmp/silver-horizon"),
        destination_dir: std::path::PathBuf::from("/tmp/complete/.weaver-staging/1"),
    }
}

/// A hand-built virtual volume over one envelope and two member partials.
///
/// The physical layout is deliberately the awkward one: header, member A, a gap
/// of envelope, member B, trailer — so a whole-volume read crosses four
/// destination boundaries in both directions.
struct ProviderFixture {
    _dir: tempfile::TempDir,
    volume: super::provider::VirtualVolume,
    /// The bytes a conventionally downloaded volume would have held.
    conventional: Vec<u8>,
}

const PROVIDER_HEADER: usize = 40;

const PROVIDER_MEMBER_A: usize = 300;

const PROVIDER_GAP: usize = 24;

const PROVIDER_MEMBER_B: usize = 180;

const PROVIDER_TRAILER: usize = 16;

/// The physical ranges this fixture's envelope file actually received: the
/// non-member regions, clipped to what was covered.
///
/// Derived from the fixture's own layout rather than from the `extents` the
/// [`super::provider::VirtualVolume`] is given, because the failure the provider
/// has to survive is exactly an extent going missing — a map derived from the
/// extents would hand the missing member's range straight back to the envelope.
fn provider_envelope_covered(covered: &ByteRanges) -> ByteRanges {
    let member_a_at = PROVIDER_HEADER as u64;
    let member_b_at = (PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP) as u64;
    let mut envelope = ByteRanges::new();
    for (start, len) in [
        (0u64, PROVIDER_HEADER as u64),
        (member_a_at + PROVIDER_MEMBER_A as u64, PROVIDER_GAP as u64),
        (
            member_b_at + PROVIDER_MEMBER_B as u64,
            PROVIDER_TRAILER as u64,
        ),
    ] {
        let end = start + len;
        for &(covered_start, covered_end) in covered.ranges() {
            let overlap_start = covered_start.max(start);
            let overlap_end = covered_end.min(end);
            if overlap_start < overlap_end {
                envelope.insert(overlap_start, overlap_end - overlap_start);
            }
        }
    }
    envelope
}

fn provider_fixture(covered: ByteRanges) -> ProviderFixture {
    provider_fixture_with_extents(covered, true)
}

/// `with_extents == false` builds the volume the router used to hand the
/// provider once a routed member turned ineligible: the bytes are covered, the
/// partial still holds them, and the extent that says so is gone.
fn provider_fixture_with_extents(covered: ByteRanges, with_extents: bool) -> ProviderFixture {
    use std::io::{Seek, SeekFrom, Write};

    let dir = tempfile::tempdir().unwrap();
    let total =
        PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP + PROVIDER_MEMBER_B + PROVIDER_TRAILER;
    // Distinct per offset, so a read that returns the *wrong* file's bytes at
    // the right length still fails.
    let conventional: Vec<u8> = (0..total).map(|index| (index % 251) as u8).collect();

    let member_a_at = PROVIDER_HEADER;
    let member_b_at = PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP;

    // The envelope is a sparse image of the volume: every non-member byte at its
    // true physical offset, holes where the members were routed away.
    let envelope = dir.path().join("silver.horizon.f0.vol00000.envelope");
    let mut file = std::fs::File::create(&envelope).unwrap();
    for (offset, len) in [
        (0usize, PROVIDER_HEADER),
        (member_a_at + PROVIDER_MEMBER_A, PROVIDER_GAP),
        (member_b_at + PROVIDER_MEMBER_B, PROVIDER_TRAILER),
    ] {
        file.seek(SeekFrom::Start(offset as u64)).unwrap();
        file.write_all(&conventional[offset..offset + len]).unwrap();
    }
    drop(file);

    let partial_a = dir
        .path()
        .join("Silver.Horizon.S01E01.mkv.f0.direct.partial");
    std::fs::write(
        &partial_a,
        &conventional[member_a_at..member_a_at + PROVIDER_MEMBER_A],
    )
    .unwrap();
    let partial_b = dir
        .path()
        .join("Silver.Horizon.S01E01.nfo.f0.direct.partial");
    std::fs::write(
        &partial_b,
        &conventional[member_b_at..member_b_at + PROVIDER_MEMBER_B],
    )
    .unwrap();

    let extents = if with_extents {
        vec![
            super::router::MemberExtent {
                member_id: 0,
                physical_offset: member_a_at as u64,
                logical_offset: 0,
                len: PROVIDER_MEMBER_A as u64,
            },
            super::router::MemberExtent {
                member_id: 1,
                physical_offset: member_b_at as u64,
                logical_offset: 0,
                len: PROVIDER_MEMBER_B as u64,
            },
        ]
    } else {
        Vec::new()
    };
    let envelope_covered = provider_envelope_covered(&covered);

    ProviderFixture {
        volume: super::provider::VirtualVolume {
            volume_index: 0,
            envelope,
            extents,
            partials: std::sync::Arc::new(
                [(0u32, partial_a), (1u32, partial_b)].into_iter().collect(),
            ),
            covered,
            envelope_covered,
            held: std::sync::Arc::new(Vec::new()),
            len: total as u64,
            // No encrypted member: the re-encrypting overlay is off, which is
            // the shape every assertion below was written against.
            ciphers: std::sync::Arc::default(),
        },
        _dir: dir,
        conventional,
    }
}

fn whole_volume_covered() -> ByteRanges {
    let mut covered = ByteRanges::new();
    covered.insert(
        0,
        (PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP + PROVIDER_MEMBER_B + PROVIDER_TRAILER)
            as u64,
    );
    covered
}

const CIPHER_SALT: [u8; 16] = [0x2B; 16];

const CIPHER_IV: [u8; 16] = [0x7C; 16];

const CIPHER_LG2: u8 = 4;

/// A whole encrypted member, built the way the write side builds one: derive the
/// real key material, encrypt the padded plaintext, then feed the cipher through
/// [`super::router::crypt::MemberCrypt::decrypt_range`] in `chunk`-sized pieces
/// so the checkpoints and the retained padding come out of the production path
/// rather than out of a constructor.
///
/// Returns `(posted cipher, plaintext, write-side state, destination coverage)`;
/// the read-side facts come from `crypt.cipher_facts(len, &covered)`, which is
/// the production hand-off, and a test wanting a *holed* member simply passes a
/// coverage map with a gap in it.
fn encrypted_member_facts(
    payload_len: usize,
    chunk: usize,
) -> (
    Vec<u8>,
    Vec<u8>,
    super::router::crypt::MemberCrypt,
    ByteRanges,
) {
    let material = unrar_rs::derive_rar5_material("moonlit-harbour", &CIPHER_SALT, CIPHER_LG2)
        .expect("the fixture KDF count is derivable");
    let facts = unrar_rs::RarVolumeMemberEncryptionFacts {
        version: 0,
        kdf_count_lg2: CIPHER_LG2,
        salt: CIPHER_SALT,
        iv: CIPHER_IV,
        psw_check_present: false,
        psw_check: None,
    };
    let plain: Vec<u8> = (0..payload_len).map(|index| (index % 251) as u8).collect();
    let cipher_len = payload_len.div_ceil(16) * 16;
    let mut padded = plain.clone();
    // The padding a real writer emits is whatever was in its buffer; a
    // recognisable pattern proves the overlay reads the *retained* bytes rather
    // than zero-filling.
    for index in payload_len..cipher_len {
        padded.push(0xE0 | (index % 16) as u8);
    }
    let posted = unrar_rs::test_support::encrypt_aes256_cbc(&material.key, &CIPHER_IV, &padded);

    let mut crypt = super::router::crypt::MemberCrypt::new(
        super::router::crypt::MemberKeys {
            key: unrar_rs::MemberCipherKey::Aes256(material.key),
            hash_key: Some(material.hash_key),
            iv: CIPHER_IV,
        },
        &unrar_rs::MemberKeying::Rar5(facts),
    );
    crypt.observe(&unrar_rs::EncryptedStore {
        format: unrar_rs::ArchiveFormat::Rar5,
        crypt: Some(facts),
        rar4_salt: None,
        cipher_size: Some(cipher_len as u64),
        tail_padding: Some((cipher_len - payload_len) as u8),
        resolved: true,
    });
    let mut covered = ByteRanges::new();
    let mut preceding = CIPHER_IV;
    let mut at = 0usize;
    while at < cipher_len {
        let step = chunk.min(cipher_len - at);
        let mut piece = posted[at..at + step].to_vec();
        let next: [u8; 16] = posted[at + step - 16..at + step]
            .try_into()
            .expect("a whole block");
        assert!(crypt.decrypt_range(at as u64, &preceding, &mut piece));
        crypt.retain_tail_padding(payload_len as u64, at as u64, &piece);
        let destination = payload_len.saturating_sub(at).min(step);
        if destination > 0 {
            covered.insert(at as u64, destination as u64);
        }
        preceding = next;
        at += step;
    }
    (posted, plain, crypt, covered)
}

/// A one-member virtual volume whose whole image is that member, so a read at
/// physical offset *n* is a read at member-logical offset *n*.
fn cipher_volume(
    dir: &Path,
    plain: &[u8],
    facts: super::router::crypt::MemberCipher,
    volume_len: u64,
) -> super::provider::VirtualVolume {
    let partial = dir.join("Silver.Horizon.S04E02.mkv.f0.direct.partial");
    std::fs::write(&partial, plain).unwrap();
    let envelope = dir.join("silver.horizon.f0.vol00000.envelope");
    std::fs::write(&envelope, Vec::new()).unwrap();
    let mut covered = ByteRanges::new();
    covered.insert(0, volume_len);
    super::provider::VirtualVolume {
        volume_index: 0,
        envelope,
        extents: vec![super::router::MemberExtent {
            member_id: 0,
            physical_offset: 0,
            logical_offset: 0,
            len: plain.len() as u64,
        }],
        partials: Arc::new([(0u32, partial)].into_iter().collect()),
        covered,
        envelope_covered: ByteRanges::new(),
        held: Arc::new(Vec::new()),
        len: volume_len,
        ciphers: Arc::new([(0u32, facts)].into_iter().collect()),
    }
}

/// One article per 100 bytes of the fixture volume, which is the granularity the
/// coverage map's boundaries actually fall on.
fn provider_article_crcs(conventional: &[u8]) -> CrcRuns {
    let mut runs = CrcRuns::default();
    let mut offset = 0usize;
    while offset < conventional.len() {
        let end = (offset + 100).min(conventional.len());
        runs.insert(
            offset as u64,
            (end - offset) as u64,
            par2_rs::checksum::crc32(&conventional[offset..end]),
        );
        offset = end;
    }
    runs
}

fn reconstruction_target(
    fixture: &ProviderFixture,
    path: std::path::PathBuf,
    covered: ByteRanges,
    crcs: CrcRuns,
) -> super::reconstruct::VolumeReconstruction {
    super::reconstruct::VolumeReconstruction {
        volume_index: 0,
        path,
        len: fixture.conventional.len() as u64,
        assembly_complete: false,
        covered,
        crcs,
        partial_article: super::reconstruct::PartialArticle::Refuse,
    }
}

/// [`reconstruction_target`] with the repair scratch's policy for a run that
/// stops inside an article.
fn repair_scratch_target(
    fixture: &ProviderFixture,
    path: std::path::PathBuf,
    covered: ByteRanges,
    crcs: CrcRuns,
) -> super::reconstruct::VolumeReconstruction {
    let mut target = reconstruction_target(fixture, path, covered, crcs);
    target.partial_article = super::reconstruct::PartialArticle::CarryThrough;
    target
}

/// A PAR2 set describing one file with **descriptions only** — no IFSC packet,
/// so no slice checksums.
///
/// That is the shape that argument names: with no per-slice data the verifier
/// falls back to a whole-file MD5, which is the read that degrades into
/// thousands of ranged reads across member partials unless the adapter offers a
/// real sequential reader.
fn descriptor_only_par2_set(filename: &str, bytes: &[u8]) -> par2_rs::Par2FileSet {
    let file_id = par2_rs::FileId::from_bytes([7u8; 16]);
    par2_rs::Par2FileSet {
        recovery_set_id: par2_rs::RecoverySetId::from_bytes([3; 16]),
        slice_size: 64,
        recovery_file_ids: vec![file_id],
        non_recovery_file_ids: Vec::new(),
        files: HashMap::from([(
            file_id,
            par2_rs::FileDescription {
                file_id,
                hash_full: par2_rs::checksum::md5(bytes),
                hash_16k: par2_rs::checksum::md5(&bytes[..bytes.len().min(16 * 1024)]),
                length: bytes.len() as u64,
                par2_name: filename.to_string(),
                filename: filename.to_string(),
            },
        )]),
        slice_checksums: HashMap::new(),
        recovery_slices: std::collections::BTreeMap::new(),
        creator: None,
    }
}

/// The adapter under test, over the provider fixture's single virtual volume.
fn virtual_file_access(
    fixture: &ProviderFixture,
    par2_set: &par2_rs::Par2FileSet,
    base_dir: &Path,
) -> (DirectVolumeFileAccess, par2_rs::FileId) {
    let file_id = par2_set.recovery_file_ids[0];
    let inner = par2_rs::PlacementFileAccess::new(
        base_dir.to_path_buf(),
        par2_set,
        std::collections::HashMap::new(),
    );
    let provider = super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    (
        DirectVolumeFileAccess::new(
            inner,
            provider,
            &[VirtualPar2Volume {
                par2_file_id: file_id,
                volume_index: fixture.volume.volume_index,
            }],
        ),
        file_id,
    )
}

/// The adapter over one encrypted virtual volume, plus the overlay counters.
fn encrypted_file_access(
    volume: super::provider::VirtualVolume,
    par2_set: &par2_rs::Par2FileSet,
    base_dir: &Path,
) -> (
    DirectVolumeFileAccess,
    par2_rs::FileId,
    Arc<super::provider::CipherOverlayCounters>,
) {
    let file_id = par2_set.recovery_file_ids[0];
    let volume_index = volume.volume_index;
    let inner = par2_rs::PlacementFileAccess::new(
        base_dir.to_path_buf(),
        par2_set,
        std::collections::HashMap::new(),
    );
    let provider = super::provider::HybridVolumeProvider::new(vec![volume]);
    let counters = provider.cipher_counters();
    (
        DirectVolumeFileAccess::new(
            inner,
            provider,
            &[VirtualPar2Volume {
                par2_file_id: file_id,
                volume_index,
            }],
        ),
        file_id,
        counters,
    )
}

fn floor_entry(volume_index: u32, file_index: u32, floor: u64, complete: bool) -> VolumeFloor {
    VolumeFloor {
        volume_index,
        file_index,
        floor,
        complete,
    }
}

/// One member header record, in the shape a split RAR5 member has. Callers set
/// the four fields that differ between a chain's parts on the value returned.
fn member_facts(
    name: &str,
    data_offset: u64,
    data_size: u64,
    unpacked_size: u64,
) -> unrar_rs::RarVolumeMemberFacts {
    unrar_rs::RarVolumeMemberFacts {
        order: 0,
        name: name.to_string(),
        name_raw: None,
        unpacked_size: Some(unpacked_size),
        data_crc32: None,
        data_blake2_hash: None,
        version: None,
        packed_crc32: None,
        packed_blake2_hash: None,
        packed_hash_uses_mac: false,
        split_before: false,
        split_after: false,
        is_directory: false,
        is_encrypted: false,
        encryption: None,
        rar4_salt: None,
        host_os: None,
        attributes: None,
        owner: None,
        mtime_ns: None,
        ctime_ns: None,
        atime_ns: None,
        data_offset,
        data_size,
        compression_method: 0,
        compression_version: 0,
        compression_solid: false,
        dict_size: 0,
        use_hash_mac: false,
        redirection_type: None,
        redirection_target: None,
        redirection_target_raw: None,
        redirection_target_is_directory: false,
    }
}

fn volume_facts(
    volume_number: u32,
    more_volumes: bool,
    members: Vec<unrar_rs::RarVolumeMemberFacts>,
) -> unrar_rs::RarVolumeFacts {
    unrar_rs::RarVolumeFacts {
        // RAR5.
        format: 5,
        volume_number: Some(volume_number),
        more_volumes,
        is_solid: false,
        is_encrypted: false,
        is_volume: true,
        has_recovery_record: false,
        is_locked: false,
        has_authenticity_verification: false,
        has_locator: false,
        quick_open_offset: None,
        headers_from_quick_open: false,
        recovery_record_offset: None,
        original_name: None,
        original_name_raw: None,
        original_creation_time_ns: None,
        members,
        services: Vec::new(),
    }
}

const REARM_PART: u64 = 400;

const REARM_MEMBER: &str = "Silver.Horizon.S01E04.mkv";

/// A router rebuilt exactly the way restore rebuilds one: from cached facts for
/// a two-volume set holding a single member split across both, with the whole
/// member seeded as restart coverage.
fn rearm_router() -> DirectSetRouter {
    let plan = DirectSetPlan {
        set_name: SET.to_string(),
        volumes: [(0u32, 0u32), (1u32, 1u32)].into_iter().collect(),
        files: [(0u32, 0u32), (1u32, 1u32)].into_iter().collect(),
        identity: None,
        working_dir: std::path::PathBuf::from("/nonexistent"),
        destination_dir: std::path::PathBuf::from("/nonexistent-staging"),
    };
    let mut router = DirectSetRouter::new(plan);
    let facts = std::collections::BTreeMap::from([
        (
            0u32,
            volume_facts(0, true, {
                // The chain's first part: continues into volume 1, and carries
                // the CRC32 of *its own* packed bytes the way RAR5 states it.
                let mut first = member_facts(REARM_MEMBER, 64, REARM_PART, REARM_PART * 2);
                first.split_after = true;
                first.packed_crc32 = Some(0x1111_1111);
                vec![first]
            }),
        ),
        (
            1u32,
            volume_facts(1, false, {
                // The final part: closes the chain and carries the whole-member
                // CRC32 instead.
                let mut last = member_facts(REARM_MEMBER, 64, REARM_PART, REARM_PART * 2);
                last.split_before = true;
                last.data_crc32 = Some(0x2222_2222);
                vec![last]
            }),
        ),
    ]);
    router.restore_layout(&facts).expect("the facts rebuild");
    let partial = format!("{REARM_MEMBER}.f0.direct.partial");
    router
        .restore_member_coverage(&partial, &[(0, REARM_PART * 2)])
        .expect("the member is in the rebuilt layout");
    router
}

/// A PAR2 set describing one file with **slice checksums**, which is what makes
/// per-slice damage attribution a question at all.
fn sliced_par2_set(filename: &str, bytes: &[u8], slice_size: u64) -> par2_rs::Par2FileSet {
    let file_id = par2_rs::FileId::from_bytes([11u8; 16]);
    let mut checksums = Vec::new();
    let mut offset = 0u64;
    while offset < bytes.len() as u64 {
        let end = (offset + slice_size).min(bytes.len() as u64);
        let mut state = par2_rs::SliceChecksumState::new();
        state.update(&bytes[offset as usize..end as usize]);
        let (crc32, md5) = state.finalize((end - offset < slice_size).then_some(slice_size));
        checksums.push(par2_rs::SliceChecksum { crc32, md5 });
        offset = end;
    }
    par2_rs::Par2FileSet {
        recovery_set_id: par2_rs::RecoverySetId::from_bytes([4; 16]),
        slice_size,
        recovery_file_ids: vec![file_id],
        non_recovery_file_ids: Vec::new(),
        files: HashMap::from([(
            file_id,
            par2_rs::FileDescription {
                file_id,
                hash_full: par2_rs::checksum::md5(bytes),
                hash_16k: par2_rs::checksum::md5(&bytes[..bytes.len().min(16 * 1024)]),
                length: bytes.len() as u64,
                par2_name: filename.to_string(),
                filename: filename.to_string(),
            },
        )]),
        slice_checksums: HashMap::from([(file_id, checksums)]),
        recovery_slices: std::collections::BTreeMap::new(),
        creator: None,
    }
}

/// The provider fixture's volume with one **interior** hole: everything is
/// covered except `[hole_start, hole_end)`, which sits in the middle of member A
/// with healthy bytes on both sides.
fn covered_with_interior_hole(hole_start: u64, hole_end: u64) -> ByteRanges {
    let total =
        (PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP + PROVIDER_MEMBER_B + PROVIDER_TRAILER)
            as u64;
    let mut covered = ByteRanges::new();
    covered.insert(0, hole_start);
    covered.insert(hole_end, total - hole_end);
    covered
}

/// Which slices `verify_slices` calls damaged, as a set of indices.
fn damaged_slice_indices(valid: &[bool]) -> Vec<usize> {
    valid
        .iter()
        .enumerate()
        .filter_map(|(index, valid)| (!*valid).then_some(index))
        .collect()
}

const HOLE_SLICE_SIZE: u64 = 64;

/// A one-volume set holding one whole stored member, rebuilt from facts the way
/// restore rebuilds one — so a test can drive [`DirectSetRouter`]'s drain
/// without a parseable RAR image in front of it.
///
/// `member` is the member's final (post-repair) bytes: the layout's whole-member
/// CRC32 is taken over them, so the set verifies exactly when the composition
/// ends up describing the repaired image and not the damaged one.
fn straddle_router(member: &[u8], header_bytes: u64) -> (DirectSetRouter, u32) {
    let plan = DirectSetPlan {
        set_name: SET.to_string(),
        volumes: [(0u32, 0u32)].into_iter().collect(),
        files: [(0u32, 0u32)].into_iter().collect(),
        identity: None,
        working_dir: std::path::PathBuf::from("/nonexistent"),
        destination_dir: std::path::PathBuf::from("/nonexistent-staging"),
    };
    let mut router = DirectSetRouter::new(plan);
    let facts = std::collections::BTreeMap::from([(
        0u32,
        volume_facts(0, false, {
            let mut only = member_facts(
                STRADDLE_MEMBER,
                header_bytes,
                member.len() as u64,
                member.len() as u64,
            );
            // One part, chain closed, so the whole-member gate is armed the
            // moment the composition covers it. No packed CRC32: layer 1 would
            // otherwise fire on the *damaged* prefix during the set-up drain and
            // demote before the repair the test is about ever happens.
            only.data_crc32 = Some(par2_rs::checksum::crc32(member));
            vec![only]
        }),
    )]);
    router.restore_layout(&facts).expect("the facts rebuild");
    let member_id = router
        .member_partials()
        .first()
        .map(|(member_id, _, _)| *member_id)
        .expect("the member was adopted");
    (router, member_id)
}

const STRADDLE_MEMBER: &str = "Silver.Horizon.S01E25.mkv";

/// Deliberately all under `0x80`: MessagePack encodes those as one-byte
/// positive fixints, so the salt survives into the blob as a literal 16-byte
/// run and a byte scan can prove the row is really in there.
const CRYPT_SALT: [u8; 16] = [0x5A; 16];

const CRYPT_IV: [u8; 16] = [0x3E; 16];

const CRYPT_KDF_LG2: u8 = 4;

const CRYPT_MEMBER: &str = "Silver.Horizon.S01E26.mkv";

const CRYPT_PASSWORD: &str = "moonlit-harbour";

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

/// A one-volume set holding one whole **encrypted** stored member, rebuilt from
/// facts the way restore rebuilds one and then driven through the router's own
/// routing path.
///
/// The point of going the long way round is that the crypt rows the test reads
/// are then the ones a real download produces — derived keys, real AES-CBC
/// ciphertext, a real checkpoint at the decrypted frontier and real retained
/// padding. A hand-written row proves only that the struct it was written into
/// serializes.
fn encrypted_crypt_router(plain: &[u8], header_bytes: u64) -> DirectSetRouter {
    encrypted_crypt_router_partial(plain, header_bytes, usize::MAX).0
}

/// [`encrypted_crypt_router`] with only the first `staged` cipher bytes routed,
/// handing back the whole cipher so the caller can stage the rest and watch what
/// moves. `staged` is clamped to the member, so `usize::MAX` is "all of it".
fn encrypted_crypt_router_partial(
    plain: &[u8],
    header_bytes: u64,
    staged: usize,
) -> (DirectSetRouter, Vec<u8>) {
    let material = unrar_rs::derive_rar5_material(CRYPT_PASSWORD, &CRYPT_SALT, CRYPT_KDF_LG2)
        .expect("the fixture KDF count is derivable");
    let cipher_len = plain.len().div_ceil(16) * 16;
    let mut padded = plain.to_vec();
    // Distinctive, non-zero padding. Those bytes are exactly what `tail_plain`
    // retains, so making them recognisable is what lets the snapshot test tell
    // the member's real trailing plaintext from a zero placeholder.
    for index in 0..cipher_len - plain.len() {
        padded.push(b'a' + index as u8);
    }
    let cipher = unrar_rs::test_support::encrypt_aes256_cbc(&material.key, &CRYPT_IV, &padded);

    let plan = DirectSetPlan {
        set_name: SET.to_string(),
        volumes: [(0u32, 0u32)].into_iter().collect(),
        files: [(0u32, 0u32)].into_iter().collect(),
        identity: None,
        working_dir: std::path::PathBuf::from("/nonexistent"),
        destination_dir: std::path::PathBuf::from("/nonexistent-staging"),
    };
    let mut router = DirectSetRouter::new(plan);
    // Before the layout, not after: admission runs from `sync_members`, which
    // `restore_layout` calls, and an encrypted member with no password refuses
    // the whole set rather than waiting.
    router.set_password(Some(CRYPT_PASSWORD));
    let facts = std::collections::BTreeMap::from([(
        0u32,
        volume_facts(0, false, {
            let mut only = member_facts(
                CRYPT_MEMBER,
                header_bytes,
                cipher_len as u64,
                plain.len() as u64,
            );
            only.is_encrypted = true;
            only.encryption = Some(unrar_rs::RarVolumeMemberEncryptionFacts {
                version: 0,
                kdf_count_lg2: CRYPT_KDF_LG2,
                salt: CRYPT_SALT,
                iv: CRYPT_IV,
                psw_check_present: false,
                psw_check: None,
            });
            // Layer 2's value, over the plaintext: the member verifies, so the
            // drain below routes rather than demoting on its own gate.
            only.data_crc32 = Some(par2_rs::checksum::crc32(plain));
            vec![only]
        }),
    )]);
    router.restore_layout(&facts).expect("the facts rebuild");
    let staged = staged.min(cipher.len());
    router.stage_for_test(0, header_bytes, &cipher[..staged]);
    router
        .drain_for_test(0)
        .expect("the encrypted drain routes rather than demoting");
    (router, cipher)
}

mod par2_fileaccess_adapter_over;
mod par3_source_access;
mod recording_test_doubles;
mod repair_transactions;
