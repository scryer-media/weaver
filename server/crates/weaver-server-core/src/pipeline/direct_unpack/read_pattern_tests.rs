//! What the 7z decoder's read pattern actually looks like, measured rather
//! than assumed.
//!
//! # What is being measured, and why it is not a correctness proof
//!
//! [`GatedSplitReader`] is correct under any access pattern: the parts are on
//! disk, so anything below a watermark is servable and only the frontier
//! blocks. Nothing here is load-bearing for that.
//!
//! What these tests measure is *overlap* — how early direct unpack can get
//! going. A chain that walks its packed streams in ascending order can be
//! chased from the first committed bytes; one that revisits packed bytes has to
//! wait for more of the archive to land before it can move, shrinking the
//! overlap toward "extract after download" even though the result is identical.
//!
//! # The measured shape
//!
//! Every chain the writer can encode reads the same way: at most one probe into
//! the archive's **tail**, then exactly one ascending sweep of the payload.
//!
//! The tail probe is the end header. For a single-member store or compress
//! chain that header is plain and sits behind the packed region, so it never
//! appears in these numbers at all. Give the archive several members or a
//! password and 7z encodes or encrypts the header, which makes it a packed
//! stream of its own living *inside* the packed region — hence a read near the
//! end before the sweep starts. Either way the decoder never returns to payload
//! it has already passed, which is what direct unpack needs, and which is why
//! the tail is worth prefetching ahead of the streams in front of it.
//!
//! Fixtures are built in-process by the 7z writer (a test-only feature) across
//! every chain it can encode, rather than checked in one file per chain.

use std::collections::BTreeMap;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use sevenz_rust2::encoder_options::{AesEncoderOptions, DeltaOptions, EncoderOptions};
use sevenz_rust2::{
    ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password, SourceReader,
};

use super::coverage::SetCoverage;
use super::reader::GatedSplitReader;
use super::start_header::StartHeader;

/// Payload per member. Big enough that every chain performs many reads, small
/// enough that the whole matrix stays a unit test.
const MEMBER_LEN: usize = 3 * 1024 * 1024;

const TEST_PASSWORD: &str = "SilverHorizonPass1";

// ---------------------------------------------------------------------------
// Recording reader
// ---------------------------------------------------------------------------

/// Every read the decoder issued, as `(absolute offset, length)`.
type ReadLog = Arc<Mutex<Vec<(u64, usize)>>>;

/// Wraps a reader and logs the absolute offset and length of every read.
struct RecordingReader<R> {
    inner: R,
    position: u64,
    reads: ReadLog,
}

impl<R: Read + Seek> RecordingReader<R> {
    fn new(inner: R) -> (Self, ReadLog) {
        let reads = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                inner,
                position: 0,
                reads: Arc::clone(&reads),
            },
            reads,
        )
    }
}

impl<R: Read> Read for RecordingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let at = self.position;
        let read = self.inner.read(buf)?;
        if read > 0 {
            self.reads.lock().expect("read log").push((at, read));
            self.position += read as u64;
        }
        Ok(read)
    }
}

impl<R: Seek> Seek for RecordingReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.position = self.inner.seek(pos)?;
        Ok(self.position)
    }
}

// ---------------------------------------------------------------------------
// Fixture construction
// ---------------------------------------------------------------------------

/// Deterministic pseudo-random bytes: compressible chains still have to move
/// real volume, so the read counts mean something.
fn payload(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}

/// One cell of the codec matrix.
struct Chain {
    name: &'static str,
    /// Coder chain in library order, which is the reverse of the data's path:
    /// the last entry receives the raw bytes and the first writes to the file.
    /// So a filter chain reads `[compressor, filter]`, and encryption sits
    /// first because it is applied last, to already-compressed bytes.
    methods: Vec<EncoderConfiguration>,
    password: Option<&'static str>,
    encrypt_header: bool,
    /// More than one member packed into a single block.
    solid: bool,
    members: usize,
}

impl Chain {
    fn simple(name: &'static str, method: EncoderMethod) -> Self {
        Self {
            name,
            methods: vec![EncoderConfiguration::new(method)],
            password: None,
            encrypt_header: false,
            solid: false,
            members: 1,
        }
    }

    fn with_methods(name: &'static str, methods: Vec<EncoderConfiguration>) -> Self {
        Self {
            name,
            methods,
            password: None,
            encrypt_header: false,
            solid: false,
            members: 1,
        }
    }

    fn encrypted(mut self, encrypt_header: bool) -> Self {
        self.password = Some(TEST_PASSWORD);
        self.encrypt_header = encrypt_header;
        self
    }

    fn members(mut self, members: usize, solid: bool) -> Self {
        self.members = members;
        self.solid = solid;
        self
    }

    fn password(&self) -> Password {
        match self.password {
            Some(value) => Password::new(value),
            None => Password::empty(),
        }
    }
}

/// The members a chain's fixture should contain, by name.
fn members_for(chain: &Chain) -> BTreeMap<String, Vec<u8>> {
    (0..chain.members)
        .map(|index| {
            (
                format!("silver_horizon/part_{index:02}.bin"),
                payload(MEMBER_LEN, 1_000 + index as u64),
            )
        })
        .collect()
}

/// Encode a fixture archive for `chain` entirely in memory.
fn build_archive(chain: &Chain, members: &BTreeMap<String, Vec<u8>>) -> Vec<u8> {
    let buffer = io::Cursor::new(Vec::new());
    let mut writer = ArchiveWriter::new(buffer).expect("create 7z writer");

    let mut methods = Vec::with_capacity(chain.methods.len());
    for method in &chain.methods {
        methods.push(method.clone());
    }
    if let Some(password) = chain.password {
        // Encryption is applied to already-compressed bytes, so it sits at the
        // output end of the chain.
        methods.insert(
            0,
            EncoderConfiguration::from(AesEncoderOptions::new(Password::new(password))),
        );
    }
    writer.set_content_methods(methods);
    if chain.encrypt_header {
        writer.set_encrypt_header(true);
    }

    if chain.solid {
        let entries = members
            .keys()
            .map(|name| ArchiveEntry::new_file(name))
            .collect::<Vec<_>>();
        let readers = members
            .values()
            .map(|bytes| SourceReader::new(io::Cursor::new(bytes.clone())))
            .collect::<Vec<_>>();
        writer
            .push_archive_entries(entries, readers)
            .expect("write solid block");
    } else {
        for (name, bytes) in members {
            writer
                .push_archive_entry(
                    ArchiveEntry::new_file(name),
                    Some(io::Cursor::new(bytes.clone())),
                )
                .expect("write entry");
        }
    }

    writer.finish().expect("finish archive").into_inner()
}

// ---------------------------------------------------------------------------
// Extraction harness
// ---------------------------------------------------------------------------

/// Extract every member through `reader`, returning the member bytes.
fn extract_members<R: Read + Seek>(reader: R, password: Password) -> BTreeMap<String, Vec<u8>> {
    let dest = tempfile::tempdir().expect("tempdir");
    let extracted = Arc::new(Mutex::new(BTreeMap::new()));
    let sink = Arc::clone(&extracted);

    sevenz_rust2::decompress_with_extract_fn_and_password(
        reader,
        dest.path(),
        password,
        move |entry: &ArchiveEntry, entry_reader: &mut dyn Read, _dest: &PathBuf| {
            if entry.is_directory() {
                return Ok(true);
            }
            let mut bytes = Vec::new();
            entry_reader.read_to_end(&mut bytes)?;
            sink.lock()
                .expect("member sink")
                .insert(entry.name().to_string(), bytes);
            Ok(true)
        },
    )
    .expect("extraction succeeds");

    Arc::try_unwrap(extracted)
        .expect("sole owner")
        .into_inner()
        .expect("member sink")
}

/// One uninterrupted ascending sweep over the packed region.
#[derive(Debug, Clone, Copy)]
struct Run {
    first: u64,
    last: u64,
    reads: usize,
}

/// What the read log says about a chain's access pattern.
#[derive(Debug)]
struct PatternStats {
    total_reads: usize,
    payload_reads: usize,
    /// Ascending sweeps: one means the decoder walked the packed region front
    /// to back exactly once, which is the best case for overlap.
    runs: Vec<Run>,
    /// Largest backward jump between consecutive packed reads.
    max_backward: u64,
}

impl PatternStats {
    /// The payload sweep: the run that carries the bulk of the reads.
    fn sweep(&self) -> Run {
        *self
            .runs
            .iter()
            .max_by_key(|run| run.reads)
            .expect("at least one run")
    }

    /// Runs that happen before the payload sweep. Every chain measured so far
    /// spends these on the archive's tail, reading the end header — which for
    /// an encoded or encrypted header is itself a packed stream, and so falls
    /// inside the packed region rather than behind it.
    fn tail_probes(&self) -> &[Run] {
        let sweep_at = self
            .runs
            .iter()
            .enumerate()
            .max_by_key(|(_, run)| run.reads)
            .map(|(index, _)| index)
            .expect("at least one run");
        &self.runs[..sweep_at]
    }
}

/// Measure the packed-region read pattern.
///
/// Only reads that land in the packed region are considered: the signature
/// header at the front and the end header at the back are read out of order by
/// design (the decoder has to see the end header before it can decode
/// anything), and they say nothing about how the payload is consumed.
fn measure(archive: &[u8], reads: &[(u64, usize)]) -> PatternStats {
    let header = StartHeader::parse(archive).expect("fixture has a valid signature header");
    let packed = header.packed_range().expect("no overflow");

    let mut payload_reads = 0usize;
    let mut max_backward = 0u64;
    let mut runs: Vec<Run> = Vec::new();
    let mut previous: Option<u64> = None;

    for (offset, len) in reads {
        if *len == 0 || *offset < packed.start || *offset >= packed.end {
            continue;
        }
        payload_reads += 1;

        let starts_new_run = match previous {
            Some(previous) if *offset < previous => {
                max_backward = max_backward.max(previous - offset);
                true
            }
            Some(_) => false,
            None => true,
        };

        if starts_new_run {
            runs.push(Run {
                first: *offset,
                last: *offset,
                reads: 1,
            });
        } else if let Some(run) = runs.last_mut() {
            run.last = *offset;
            run.reads += 1;
        }
        previous = Some(*offset);
    }

    PatternStats {
        total_reads: reads.len(),
        payload_reads,
        runs,
        max_backward,
    }
}

/// Every chain the workspace's sevenz-rust2 configuration can encode.
fn matrix() -> Vec<Chain> {
    vec![
        Chain::simple("copy", EncoderMethod::COPY),
        Chain::simple("lzma", EncoderMethod::LZMA),
        Chain::simple("lzma2", EncoderMethod::LZMA2),
        Chain::simple("bzip2", EncoderMethod::BZIP2),
        Chain::simple("deflate", EncoderMethod::DEFLATE),
        Chain::simple("ppmd", EncoderMethod::PPMD),
        Chain::simple("zstd", EncoderMethod::ZSTD),
        Chain::simple("brotli", EncoderMethod::BROTLI),
        Chain::simple("lz4", EncoderMethod::LZ4),
        Chain::with_methods(
            "delta+lzma2",
            vec![
                EncoderConfiguration::new(EncoderMethod::LZMA2),
                EncoderConfiguration::new(EncoderMethod::DELTA_FILTER)
                    .with_options(EncoderOptions::Delta(DeltaOptions::from_distance(4))),
            ],
        ),
        Chain::with_methods(
            "bcj_x86+lzma2",
            vec![
                EncoderConfiguration::new(EncoderMethod::LZMA2),
                EncoderConfiguration::new(EncoderMethod::BCJ_X86_FILTER),
            ],
        ),
        Chain::simple("lzma2+aes256", EncoderMethod::LZMA2).encrypted(false),
        Chain::simple("lzma2+aes256_header", EncoderMethod::LZMA2).encrypted(true),
        Chain::simple("lzma2_solid_x3", EncoderMethod::LZMA2).members(3, true),
        Chain::simple("lzma2_nonsolid_x3", EncoderMethod::LZMA2).members(3, false),
    ]
}

#[test]
fn packed_reads_are_ascending_across_the_codec_matrix() {
    let mut rows: Vec<(String, PatternStats, usize)> = Vec::new();

    for chain in matrix() {
        let members = members_for(&chain);
        let archive = build_archive(&chain, &members);

        let (recording, reads) = RecordingReader::new(io::Cursor::new(archive.clone()));
        let extracted = extract_members(recording, chain.password());

        assert_eq!(
            extracted, members,
            "chain {} did not round-trip its members",
            chain.name
        );

        let reads = reads.lock().expect("read log").clone();
        let stats = measure(&archive, &reads);

        assert!(
            stats.payload_reads > 0,
            "chain {} performed no packed-region reads",
            chain.name
        );

        rows.push((chain.name.to_string(), stats, archive.len()));
    }

    println!(
        "\n{:<24} {:>10} {:>8} {:>8} {:>7} {:>20} {:>12}",
        "chain", "archive", "reads", "packed", "probes", "sweep", "max back"
    );
    for (name, stats, archive_len) in &rows {
        let sweep = stats.sweep();
        println!(
            "{name:<24} {archive_len:>10} {:>8} {:>8} {:>7} {:>20} {:>12}",
            stats.total_reads,
            stats.payload_reads,
            stats.tail_probes().len(),
            format!("{}..={}", sweep.first, sweep.last),
            stats.max_backward
        );
        for (index, probe) in stats.tail_probes().iter().enumerate() {
            println!(
                "    tail probe {index}: {}..={} over {} reads",
                probe.first, probe.last, probe.reads
            );
        }
    }

    // Asserted after the table so a regression reports the whole matrix rather
    // than only the first chain that moved.
    let mut failures = Vec::new();
    for (name, stats, _) in &rows {
        let sweep = stats.sweep();

        // The payload is consumed in ONE forward pass: the sweep is the last
        // run, so nothing re-reads packed bytes behind it.
        if stats.runs.last().map(|run| run.first) != Some(sweep.first) {
            failures.push(format!(
                "{name}: the payload sweep is not the final run ({} runs)",
                stats.runs.len()
            ));
        }

        // Anything read before the sweep is the end header, which sits past
        // where the payload ends. A probe *inside* the payload would mean the
        // decoder revisits packed bytes, which is what would cost overlap.
        for probe in stats.tail_probes() {
            if probe.first <= sweep.last {
                failures.push(format!(
                    "{name}: pre-sweep read at {} is inside the payload sweep {}..={}",
                    probe.first, sweep.first, sweep.last
                ));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "read-pattern regressions: {failures:#?}"
    );
}

/// The branch filters are not behind any cargo feature — they come from
/// lzma-rust2 and are compiled in unconditionally. Asserted by round-tripping
/// each one rather than by reading the dependency's feature list, so a
/// dependency bump that quietly drops one is caught here.
///
/// BCJ2 is deliberately absent: sevenz-rust2 decodes it but cannot encode it,
/// so there is no way to build the fixture in-process, and hand-forging one
/// would be asserting against our own bytes rather than a real archive.
#[test]
fn every_branch_filter_round_trips() {
    let filters = [
        ("bcj_x86", EncoderMethod::BCJ_X86_FILTER),
        ("bcj_arm", EncoderMethod::BCJ_ARM_FILTER),
        ("bcj_arm64", EncoderMethod::BCJ_ARM64_FILTER),
        ("bcj_arm_thumb", EncoderMethod::BCJ_ARM_THUMB_FILTER),
        ("bcj_ppc", EncoderMethod::BCJ_PPC_FILTER),
        ("bcj_sparc", EncoderMethod::BCJ_SPARC_FILTER),
        ("bcj_ia64", EncoderMethod::BCJ_IA64_FILTER),
        ("bcj_riscv", EncoderMethod::BCJ_RISCV_FILTER),
        ("delta", EncoderMethod::DELTA_FILTER),
    ];

    for (name, filter) in filters {
        let chain = Chain::with_methods(
            name,
            vec![
                EncoderConfiguration::new(EncoderMethod::LZMA2),
                EncoderConfiguration::new(filter),
            ],
        );
        let members = BTreeMap::from([(
            "silver_horizon/reel.bin".to_string(),
            payload(256 * 1024, 4_242),
        )]);
        let archive = build_archive(&chain, &members);

        let (recording, reads) = RecordingReader::new(io::Cursor::new(archive.clone()));
        let extracted = extract_members(recording, chain.password());
        assert_eq!(extracted, members, "{name} did not round-trip");

        let stats = measure(&archive, &reads.lock().expect("read log"));
        assert!(
            stats
                .tail_probes()
                .iter()
                .all(|probe| probe.first > stats.sweep().last),
            "{name} revisited payload it had already read"
        );
    }
}

#[test]
fn extraction_through_the_gated_reader_matches_a_plain_reader() {
    let chain = Chain::simple("lzma2", EncoderMethod::LZMA2).members(2, true);
    let members = members_for(&chain);
    let archive = build_archive(&chain, &members);

    let expected = extract_members(io::Cursor::new(archive.clone()), chain.password());
    assert_eq!(expected, members);

    // Split at boundaries that fall inside the packed streams and inside the
    // end header, so the reader has to stitch across both.
    let dir = tempfile::tempdir().expect("tempdir");
    let cuts = [
        archive.len() / 4,
        archive.len() / 2,
        archive.len() - (archive.len() / 16).max(1),
    ];
    let mut bounds = vec![0usize];
    bounds.extend_from_slice(&cuts);
    bounds.push(archive.len());

    let mut paths = Vec::new();
    let mut lens = Vec::new();
    for (index, window) in bounds.windows(2).enumerate() {
        let path = dir
            .path()
            .join(format!("silver_horizon.7z.{:03}", index + 1));
        std::fs::write(&path, &archive[window[0]..window[1]]).expect("write part");
        lens.push((window[1] - window[0]) as u64);
        paths.push(path);
    }

    let coverage = Arc::new(SetCoverage::new(paths.len()));
    coverage.set_total_len(archive.len() as u64);
    for (index, len) in lens.iter().enumerate() {
        coverage.note_part_len(index, *len);
        coverage.advance_watermark(index, *len);
        coverage.mark_part_complete(index);
    }

    let reader = GatedSplitReader::open(&paths, Arc::clone(&coverage)).expect("open gated reader");
    let through_gate = extract_members(reader, chain.password());

    assert_eq!(through_gate, expected);
    assert_eq!(
        coverage.park_count(),
        0,
        "fully-populated coverage must never park"
    );
}

#[test]
fn extraction_keeps_up_with_a_drip_fed_download() {
    let chain = Chain::simple("lzma2", EncoderMethod::LZMA2).members(2, false);
    let members = members_for(&chain);
    let archive = build_archive(&chain, &members);

    let dir = tempfile::tempdir().expect("tempdir");
    let part_len = archive.len().div_ceil(4);
    let mut paths = Vec::new();
    let mut lens = Vec::new();
    for (index, piece) in archive.chunks(part_len).enumerate() {
        let path = dir
            .path()
            .join(format!("silver_horizon.7z.{:03}", index + 1));
        // The part files start empty: the download has not written them yet.
        std::fs::write(&path, b"").expect("create part");
        paths.push(path);
        lens.push(piece.len() as u64);
    }

    let coverage = Arc::new(SetCoverage::new(paths.len()));
    // The signature header is the first thing on disk, so the total length is
    // known before any payload has landed — exactly as it will be in service.
    coverage.set_total_len(archive.len() as u64);
    for (index, len) in lens.iter().enumerate() {
        coverage.note_part_len(index, *len);
    }

    let reader_paths = paths.clone();
    let reader_coverage = Arc::clone(&coverage);
    let password = chain.password();
    let worker = std::thread::spawn(move || {
        let reader =
            GatedSplitReader::open(&reader_paths, reader_coverage).expect("open gated reader");
        extract_members(reader, password)
    });

    // Feed the set in 64 KiB commits, appending to each part file and moving
    // its watermark, the way the download path will.
    const COMMIT: usize = 64 * 1024;
    for (index, piece) in archive.chunks(part_len).enumerate() {
        let mut written = 0usize;
        while written < piece.len() {
            let end = (written + COMMIT).min(piece.len());
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&paths[index])
                .expect("open part for append");
            std::io::Write::write_all(&mut file, &piece[written..end]).expect("append");
            drop(file);
            written = end;
            coverage.advance_watermark(index, written as u64);
        }
        coverage.mark_part_complete(index);
    }

    assert_eq!(worker.join().expect("extraction thread"), members);
    assert!(
        coverage.park_count() > 0,
        "a drip-fed extraction should have parked at least once"
    );
}
