//! Gating behaviour of [`GatedSplitReader`]: what parks, what does not, and
//! what wakes a parked reader.
//!
//! The part files here are written to disk in full up front while the coverage
//! is advanced by hand. That gap is the point: the reader must believe the
//! coverage and not the filesystem, so a test can hold it at an offset whose
//! bytes are already sitting in the file and prove it waits anyway.

use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tempfile::TempDir;

use super::coverage::SetCoverage;
use super::reader::GatedSplitReader;

/// Long enough that a reader which was going to spin would have spun many
/// thousands of times before it elapses.
const SETTLE: Duration = Duration::from_millis(250);

struct SplitFixture {
    _dir: TempDir,
    paths: Vec<PathBuf>,
    bytes: Vec<u8>,
    part_lens: Vec<u64>,
}

/// Deterministic pseudo-random bytes; incompressible enough that a codec has
/// to actually move them.
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

/// Split `bytes` at `boundaries` and write each piece as a part file.
fn split_fixture(bytes: Vec<u8>, boundaries: &[usize]) -> SplitFixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut cuts = vec![0usize];
    cuts.extend_from_slice(boundaries);
    cuts.push(bytes.len());

    let mut paths = Vec::new();
    let mut part_lens = Vec::new();
    for (index, window) in cuts.windows(2).enumerate() {
        let piece = &bytes[window[0]..window[1]];
        let path = dir
            .path()
            .join(format!("silver_horizon.7z.{:03}", index + 1));
        std::fs::write(&path, piece).expect("write part");
        paths.push(path);
        part_lens.push(piece.len() as u64);
    }

    SplitFixture {
        _dir: dir,
        paths,
        bytes,
        part_lens,
    }
}

/// Coverage that already knows everything: every length declared, every part
/// complete.
fn settled_coverage(fixture: &SplitFixture) -> Arc<SetCoverage> {
    let coverage = Arc::new(SetCoverage::new(fixture.paths.len()));
    coverage.set_total_len(fixture.bytes.len() as u64);
    for (index, len) in fixture.part_lens.iter().enumerate() {
        coverage.note_part_len(index, *len);
        coverage.advance_watermark(index, *len);
        coverage.mark_part_complete(index);
    }
    coverage
}

#[test]
fn settled_coverage_reads_the_whole_concatenated_stream() {
    let fixture = split_fixture(payload(300_000, 7), &[64_000, 190_000]);
    let coverage = settled_coverage(&fixture);
    let mut reader =
        GatedSplitReader::open(&fixture.paths, Arc::clone(&coverage)).expect("open reader");

    let mut read = Vec::new();
    reader.read_to_end(&mut read).expect("read to end");

    assert_eq!(read, fixture.bytes);
    assert_eq!(coverage.park_count(), 0, "settled coverage must never park");
}

#[test]
fn reads_park_until_the_watermark_advances() {
    let fixture = split_fixture(payload(200_000, 11), &[80_000]);
    let coverage = Arc::new(SetCoverage::new(2));
    coverage.set_total_len(fixture.bytes.len() as u64);
    coverage.note_part_len(0, fixture.part_lens[0]);
    coverage.note_part_len(1, fixture.part_lens[1]);
    // Only the first 1 KiB of part 0 is committed so far.
    coverage.advance_watermark(0, 1_024);

    let paths = fixture.paths.clone();
    let reader_coverage = Arc::clone(&coverage);
    let worker = thread::spawn(move || {
        let mut reader = GatedSplitReader::open(&paths, reader_coverage).expect("open reader");
        let mut read = Vec::new();
        reader.read_to_end(&mut read).expect("read to end");
        read
    });

    // Let the reader consume the committed prefix and park on the rest.
    thread::sleep(SETTLE);
    let parked_at = coverage.park_count();
    assert!(
        parked_at > 0,
        "reader should have parked past the watermark"
    );

    // Drip the rest of the set in.
    for watermark in [20_000u64, 60_000, fixture.part_lens[0]] {
        coverage.advance_watermark(0, watermark);
        thread::sleep(Duration::from_millis(10));
    }
    coverage.mark_part_complete(0);
    for watermark in [10_000u64, fixture.part_lens[1]] {
        coverage.advance_watermark(1, watermark);
        thread::sleep(Duration::from_millis(10));
    }
    coverage.mark_part_complete(1);

    assert_eq!(worker.join().expect("reader thread"), fixture.bytes);
}

#[test]
fn a_parked_reader_waits_instead_of_spinning() {
    let fixture = split_fixture(payload(50_000, 13), &[25_000]);
    let coverage = Arc::new(SetCoverage::new(2));
    coverage.set_total_len(fixture.bytes.len() as u64);
    coverage.note_part_len(0, fixture.part_lens[0]);
    coverage.note_part_len(1, fixture.part_lens[1]);

    let paths = fixture.paths.clone();
    let reader_coverage = Arc::clone(&coverage);
    let worker = thread::spawn(move || {
        let mut reader = GatedSplitReader::open(&paths, reader_coverage).expect("open reader");
        let mut buf = [0u8; 4_096];
        reader.read(&mut buf).map(|read| read > 0)
    });

    thread::sleep(SETTLE);
    let first = coverage.park_count();
    thread::sleep(SETTLE);
    let second = coverage.park_count();

    // A spinning reader would climb without bound across two settle windows; a
    // parked one is asleep on the condvar and cannot move at all.
    assert_eq!(
        first, second,
        "park count grew from {first} to {second} while nothing changed"
    );
    assert!(first <= 2, "reader parked {first} times before blocking");

    coverage.advance_watermark(0, fixture.part_lens[0]);
    assert!(worker.join().expect("reader thread").expect("read"));
}

#[test]
fn abort_unblocks_a_parked_reader_with_its_reason() {
    let fixture = split_fixture(payload(40_000, 17), &[20_000]);
    let coverage = Arc::new(SetCoverage::new(2));
    coverage.set_total_len(fixture.bytes.len() as u64);
    coverage.note_part_len(0, fixture.part_lens[0]);
    coverage.note_part_len(1, fixture.part_lens[1]);

    let paths = fixture.paths.clone();
    let reader_coverage = Arc::clone(&coverage);
    let worker = thread::spawn(move || {
        let mut reader = GatedSplitReader::open(&paths, reader_coverage).expect("open reader");
        let mut buf = [0u8; 1_024];
        reader.read(&mut buf).expect_err("aborted mid-read")
    });

    thread::sleep(SETTLE);
    assert!(coverage.park_count() > 0, "reader should be parked");

    coverage.abort("article 42 unavailable on every server");

    let error = worker.join().expect("reader thread");
    assert!(
        error
            .to_string()
            .contains("article 42 unavailable on every server"),
        "unexpected error: {error}"
    );
}

#[test]
fn an_unknown_middle_part_length_parks_rather_than_guessing() {
    let fixture = split_fixture(payload(90_000, 19), &[30_000, 60_000]);
    let coverage = Arc::new(SetCoverage::new(3));
    coverage.set_total_len(fixture.bytes.len() as u64);
    coverage.note_part_len(0, fixture.part_lens[0]);
    // Part 1's length is deliberately withheld: without it, where part 2
    // starts is unknowable, and guessing would misplace every later offset.
    coverage.note_part_len(2, fixture.part_lens[2]);
    coverage.advance_watermark(0, fixture.part_lens[0]);
    coverage.advance_watermark(1, fixture.part_lens[1]);
    coverage.advance_watermark(2, fixture.part_lens[2]);

    let paths = fixture.paths.clone();
    let reader_coverage = Arc::clone(&coverage);
    let worker = thread::spawn(move || {
        let mut reader = GatedSplitReader::open(&paths, reader_coverage).expect("open reader");
        reader
            .seek(SeekFrom::Start(70_000))
            .expect("seek within the archive");
        let mut buf = [0u8; 512];
        reader.read_exact(&mut buf).expect("read");
        buf
    });

    thread::sleep(SETTLE);
    assert!(
        coverage.park_count() > 0,
        "an unknown middle length must park the mapping"
    );

    coverage.note_part_len(1, fixture.part_lens[1]);

    let read = worker.join().expect("reader thread");
    assert_eq!(read.as_slice(), &fixture.bytes[70_000..70_512]);
}

#[test]
fn seeking_past_the_declared_total_is_an_error() {
    let fixture = split_fixture(payload(10_000, 23), &[4_000]);
    let coverage = settled_coverage(&fixture);
    let mut reader = GatedSplitReader::open(&fixture.paths, coverage).expect("open reader");

    // The end itself is a legal position.
    assert_eq!(reader.seek(SeekFrom::End(0)).expect("seek to end"), 10_000);

    let error = reader
        .seek(SeekFrom::Start(10_001))
        .expect_err("past the declared total");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(
        error
            .to_string()
            .contains("past the declared archive length")
    );

    let error = reader
        .seek(SeekFrom::Current(-20_000))
        .expect_err("before the start");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn backward_seeks_into_committed_bytes_never_park() {
    let fixture = split_fixture(payload(120_000, 29), &[40_000, 90_000]);
    let coverage = settled_coverage(&fixture);
    let mut reader =
        GatedSplitReader::open(&fixture.paths, Arc::clone(&coverage)).expect("open reader");

    // Walk forward across every part, then re-read the same windows backwards.
    let probes = [0u64, 39_500, 41_000, 89_000, 95_000, 119_000];
    let mut forward = Vec::new();
    for offset in probes {
        reader.seek(SeekFrom::Start(offset)).expect("seek");
        let mut buf = [0u8; 256];
        reader.read_exact(&mut buf).expect("read");
        forward.push(buf);
    }

    for (index, offset) in probes.iter().enumerate().rev() {
        reader.seek(SeekFrom::Start(*offset)).expect("seek back");
        let mut buf = [0u8; 256];
        reader.read_exact(&mut buf).expect("re-read");
        assert_eq!(buf, forward[index], "re-read at {offset} differs");
        assert_eq!(
            buf.as_slice(),
            &fixture.bytes[*offset as usize..*offset as usize + 256]
        );
    }

    assert_eq!(
        coverage.park_count(),
        0,
        "committed bytes are on disk and must be served without waiting"
    );
}

#[test]
fn a_far_forward_read_parks_then_a_backward_read_is_served_immediately() {
    let fixture = split_fixture(payload(150_000, 31), &[50_000, 100_000]);
    let coverage = Arc::new(SetCoverage::new(3));
    coverage.set_total_len(fixture.bytes.len() as u64);
    for (index, len) in fixture.part_lens.iter().enumerate() {
        coverage.note_part_len(index, *len);
    }
    // Only part 0 is committed.
    coverage.advance_watermark(0, fixture.part_lens[0]);

    let paths = fixture.paths.clone();
    let reader_coverage = Arc::clone(&coverage);
    let worker = thread::spawn(move || {
        let mut reader = GatedSplitReader::open(&paths, reader_coverage).expect("open reader");

        // Jump into the uncommitted tail: this must park.
        reader.seek(SeekFrom::Start(140_000)).expect("seek forward");
        let mut tail = [0u8; 256];
        reader.read_exact(&mut tail).expect("tail read");

        // Now go back into long-committed territory. The bytes never left the
        // disk, so this must be served without another wait.
        let parks_before = reader.coverage().park_count();
        reader.seek(SeekFrom::Start(10_000)).expect("seek back");
        let mut head = [0u8; 256];
        reader.read_exact(&mut head).expect("head read");

        (tail, head, parks_before, reader.coverage().park_count())
    });

    thread::sleep(SETTLE);
    assert!(coverage.park_count() > 0, "far-forward read should park");

    coverage.advance_watermark(1, fixture.part_lens[1]);
    coverage.advance_watermark(2, fixture.part_lens[2]);

    let (tail, head, parks_before, parks_after) = worker.join().expect("reader thread");
    assert_eq!(tail.as_slice(), &fixture.bytes[140_000..140_256]);
    assert_eq!(head.as_slice(), &fixture.bytes[10_000..10_256]);
    assert_eq!(
        parks_before, parks_after,
        "the backward re-read parked, but its bytes were already committed"
    );
}

#[test]
fn a_part_absent_at_open_is_opened_once_coverage_reaches_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let bytes = payload(8_000, 37);
    let first = dir.path().join("silver_horizon.7z.001");
    let second = dir.path().join("silver_horizon.7z.002");
    std::fs::write(&first, &bytes[..4_000]).expect("write part 1");
    // Part 2 does not exist yet — the download has not created it.

    let paths = vec![first, second.clone()];
    let coverage = Arc::new(SetCoverage::new(2));
    coverage.set_total_len(bytes.len() as u64);
    coverage.note_part_len(0, 4_000);
    coverage.note_part_len(1, 4_000);
    coverage.advance_watermark(0, 4_000);

    let mut reader =
        GatedSplitReader::open(&paths, Arc::clone(&coverage)).expect("open with a missing part");

    let mut head = vec![0u8; 4_000];
    reader.read_exact(&mut head).expect("first part reads");
    assert_eq!(head, bytes[..4_000]);

    let reader_coverage = Arc::clone(&coverage);
    let worker = thread::spawn(move || {
        let mut tail = vec![0u8; 4_000];
        reader.read_exact(&mut tail).expect("second part reads");
        tail
    });

    thread::sleep(SETTLE);
    assert!(
        reader_coverage.park_count() > 0,
        "should park on the absent part"
    );

    std::fs::write(&second, &bytes[4_000..]).expect("write part 2");
    reader_coverage.advance_watermark(1, 4_000);

    assert_eq!(worker.join().expect("reader thread"), bytes[4_000..]);
}

#[test]
fn a_part_count_mismatch_is_refused_at_open() {
    let fixture = split_fixture(payload(1_000, 41), &[500]);
    let coverage = Arc::new(SetCoverage::new(3));

    let error = GatedSplitReader::open(&fixture.paths, coverage).expect_err("mismatch");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}
