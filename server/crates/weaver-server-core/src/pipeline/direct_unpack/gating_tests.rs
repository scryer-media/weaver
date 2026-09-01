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

// ---------------------------------------------------------------------------
// Repair-resume: damage caps, consumed high-water, release
// ---------------------------------------------------------------------------

#[test]
fn a_damage_cap_holds_the_frontier_below_the_damaged_byte() {
    let coverage = SetCoverage::new(1);
    coverage.set_total_len(100_000);
    coverage.advance_watermark(0, 80_000);

    // Everything up to the watermark is servable while nothing is known damaged.
    assert_eq!(coverage.readable_at(0, 0).expect("readable"), 80_000);

    // The recovery set reports damage at 40_000 and vouches for everything
    // below it: the frontier stops there even though the download has committed
    // far past it. The cap also gates the set, which is why the vouched prefix
    // has to arrive with it — production computes both from the same verdicts.
    coverage.cap_at_damage(0, 40_000);
    coverage.note_vouched_prefix(0, 40_000);
    assert!(coverage.has_damage_cap());
    assert!(coverage.is_gated(), "damage gates the set");
    assert_eq!(coverage.readable_at(0, 0).expect("readable"), 40_000);
    assert_eq!(coverage.readable_at(0, 39_999).expect("readable"), 1);
}

#[test]
fn a_damage_cap_only_ever_lowers_the_frontier() {
    let coverage = SetCoverage::new(1);
    coverage.set_total_len(100_000);
    coverage.advance_watermark(0, 90_000);

    coverage.note_vouched_prefix(0, 90_000);
    coverage.cap_at_damage(0, 50_000);
    // A later, higher damage report does not make the earlier one wrong.
    coverage.cap_at_damage(0, 70_000);
    assert_eq!(coverage.readable_at(0, 0).expect("readable"), 50_000);
    // A lower one does lower it further.
    coverage.cap_at_damage(0, 20_000);
    assert_eq!(coverage.readable_at(0, 0).expect("readable"), 20_000);
}

/// A capped part is not "finished" for the reader even when the download says
/// it is: repair is still to come, and the bytes above the cap are exactly the
/// ones it will rewrite.
#[test]
fn a_capped_part_parks_rather_than_reporting_end_of_part() {
    let fixture = split_fixture(payload(60_000, 71), &[]);
    let coverage = std::sync::Arc::new(SetCoverage::new(1));
    coverage.set_total_len(60_000);
    coverage.note_part_len(0, 60_000);
    coverage.advance_watermark(0, 60_000);
    coverage.cap_at_damage(0, 20_000);
    coverage.note_vouched_prefix(0, 20_000);
    coverage.mark_part_complete(0);

    let paths = fixture.paths.clone();
    let reader_coverage = std::sync::Arc::clone(&coverage);
    let worker = thread::spawn(move || {
        let mut reader = GatedSplitReader::open(&paths, reader_coverage).expect("open reader");
        reader
            .seek(SeekFrom::Start(20_000))
            .expect("seek to the cap");
        let mut buf = [0u8; 512];
        reader.read_exact(&mut buf).map(|()| buf)
    });

    thread::sleep(SETTLE);
    assert!(
        coverage.park_count() > 0,
        "a read at the damage cap must park, not report end of part"
    );

    // Repair lands: the cap goes, the frontier opens, the reader finishes.
    coverage.release_after_repair(0, 60_000);
    let read = worker.join().expect("reader thread").expect("read");
    assert_eq!(read.as_slice(), &fixture.bytes[20_000..20_512]);
}

#[test]
fn the_consumed_high_water_tracks_what_the_decoder_actually_read() {
    let fixture = split_fixture(payload(40_000, 73), &[15_000]);
    let coverage = settled_coverage(&fixture);
    let mut reader =
        GatedSplitReader::open(&fixture.paths, std::sync::Arc::clone(&coverage)).expect("open");

    assert_eq!(coverage.consumed_high_water(0), 0, "nothing read yet");

    let mut buf = [0u8; 4_096];
    reader.read_exact(&mut buf).expect("read");
    assert_eq!(
        coverage.consumed_high_water(0),
        4_096,
        "the high-water is what was taken, not what was available"
    );

    // Reading into the second part moves that part's high-water, not part 0's.
    reader.seek(SeekFrom::Start(15_000)).expect("seek");
    reader.read_exact(&mut buf).expect("read");
    assert_eq!(coverage.consumed_high_water(1), 4_096);
    assert_eq!(coverage.consumed_high_water(0), 4_096);
}

#[test]
fn releasing_after_repair_clears_the_cap_and_settles_the_length() {
    let coverage = SetCoverage::new(1);
    coverage.set_total_len(50_000);
    coverage.advance_watermark(0, 30_000);
    coverage.cap_at_damage(0, 10_000);

    coverage.release_after_repair(0, 50_000);

    assert!(!coverage.has_damage_cap());
    let progress = coverage.part_progress(0).expect("in range");
    assert_eq!(progress.len, Some(50_000));
    assert_eq!(progress.watermark, 50_000);
    assert!(progress.complete);
    assert_eq!(coverage.readable_at(0, 0).expect("readable"), 50_000);
}

/// Review question (a): a reader parked *under a damage cap* is parked on a
/// condition only repair can satisfy. If repair never comes, the abort path has
/// to reach it — otherwise the blocking thread waits forever.
#[test]
fn abort_unblocks_a_reader_parked_under_a_damage_cap() {
    let fixture = split_fixture(payload(50_000, 79), &[]);
    let coverage = std::sync::Arc::new(SetCoverage::new(1));
    coverage.set_total_len(50_000);
    coverage.note_part_len(0, 50_000);
    coverage.advance_watermark(0, 50_000);
    coverage.mark_part_complete(0);
    // Damage known: the frontier stops at 10_000 even though the part is
    // complete and every byte is on disk.
    coverage.cap_at_damage(0, 10_000);
    coverage.note_vouched_prefix(0, 10_000);

    let paths = fixture.paths.clone();
    let reader_coverage = std::sync::Arc::clone(&coverage);
    let worker = thread::spawn(move || {
        let mut reader = GatedSplitReader::open(&paths, reader_coverage).expect("open reader");
        reader
            .seek(SeekFrom::Start(10_000))
            .expect("seek to the cap");
        let mut buf = [0u8; 256];
        reader
            .read_exact(&mut buf)
            .expect_err("aborted while parked")
    });

    thread::sleep(SETTLE);
    assert!(
        coverage.park_count() > 0,
        "the reader should be parked under the cap"
    );

    coverage.abort("PAR2 repair failed");

    let error = worker.join().expect("reader thread");
    assert!(
        error.to_string().contains("PAR2 repair failed"),
        "a capped park must be reachable by abort: {error}"
    );
}

/// Review question (b): repair writes the file the recovery set describes,
/// which can be *shorter* than what was on disk. Releasing must not leave a
/// watermark describing bytes the repaired file no longer has.
#[test]
fn releasing_a_shrunk_part_clamps_the_watermark_to_the_repaired_length() {
    let coverage = SetCoverage::new(1);
    coverage.set_total_len(30_000);
    coverage.advance_watermark(0, 50_000);
    coverage.cap_at_damage(0, 5_000);

    // Repair truncated the over-long part to its described 30_000 bytes.
    coverage.release_after_repair(0, 30_000);

    let progress = coverage.part_progress(0).expect("in range");
    assert_eq!(progress.len, Some(30_000));
    assert_eq!(
        progress.watermark, 30_000,
        "the pre-repair watermark described bytes the repaired file does not have"
    );
    assert_eq!(coverage.readable_at(0, 0).expect("readable"), 30_000);
    assert!(coverage.abort_reason().is_none(), "nothing was over-read");
}

/// The same shrink, but the decoder had already read past where repair cut. The
/// vouch this release rests on was about bytes that no longer exist.
#[test]
fn releasing_below_what_was_already_read_aborts_instead() {
    let coverage = SetCoverage::new(1);
    coverage.set_total_len(40_000);
    coverage.advance_watermark(0, 40_000);
    coverage.note_consumed(0, 25_000);
    coverage.cap_at_damage(0, 30_000);

    coverage.release_after_repair(0, 20_000);

    let reason = coverage
        .abort_reason()
        .expect("a cut below the read head must abort");
    assert!(
        reason.contains("20000") && reason.contains("25000"),
        "unexpected reason: {reason}"
    );
    assert!(coverage.readable_at(0, 0).is_err());
}

/// The whole point of repair-resume: a chase parked below damage picks up over
/// the repaired bytes and finishes, rather than being thrown away.
#[test]
fn a_parked_chase_resumes_over_repaired_bytes_and_reads_the_whole_stream() {
    // The "damaged" part on disk, and what repair will write in its place.
    let repaired = payload(80_000, 83);
    let mut damaged = repaired.clone();
    for byte in damaged[40_000..48_000].iter_mut() {
        *byte = 0;
    }
    let fixture = split_fixture(damaged, &[]);

    let coverage = std::sync::Arc::new(SetCoverage::new(1));
    coverage.set_total_len(80_000);
    coverage.note_part_len(0, 80_000);
    coverage.advance_watermark(0, 80_000);
    coverage.mark_part_complete(0);
    // The grid reports damage at 40_000 and vouches for the run below it, so
    // the chase is held exactly there.
    coverage.cap_at_damage(0, 40_000);
    coverage.note_vouched_prefix(0, 40_000);

    let paths = fixture.paths.clone();
    let reader_coverage = std::sync::Arc::clone(&coverage);
    let worker = thread::spawn(move || {
        let mut reader = GatedSplitReader::open(&paths, reader_coverage).expect("open reader");
        let mut read = Vec::new();
        reader.read_to_end(&mut read).expect("read to end");
        read
    });

    thread::sleep(SETTLE);
    assert!(
        coverage.park_count() > 0,
        "the chase must park at the damage cap, not read the damaged bytes"
    );
    assert!(
        coverage.consumed_high_water(0) <= 40_000,
        "nothing past the cap may have been consumed"
    );

    // Repair lands: the file on disk becomes the repaired one, and the set is
    // released.
    std::fs::write(&fixture.paths[0], &repaired).unwrap();
    coverage.release_after_repair(0, repaired.len() as u64);

    assert_eq!(
        worker.join().expect("reader thread"),
        repaired,
        "the resumed chase must deliver the repaired bytes end to end"
    );
}

// ---------------------------------------------------------------------------
// Gate-on-first-damage
// ---------------------------------------------------------------------------

/// A clean set pays nothing for gating: no verdict, no gate, and the frontier
/// is exactly the download's own.
#[test]
fn an_ungated_set_serves_everything_the_download_committed() {
    let coverage = SetCoverage::new(1);
    coverage.set_total_len(100_000);
    coverage.advance_watermark(0, 80_000);

    assert!(!coverage.is_gated());
    assert_eq!(coverage.readable_at(0, 0).expect("readable"), 80_000);
    // A vouched prefix nobody asked for changes nothing while ungated.
    coverage.note_vouched_prefix(0, 1_024);
    assert_eq!(coverage.readable_at(0, 0).expect("readable"), 80_000);
}

/// Damage anywhere gates the whole set — including the parts the chase has not
/// reached, which is the point: their unverified bytes stop being served before
/// the chase can race into them.
#[test]
fn damage_in_one_part_gates_every_other_part() {
    let coverage = SetCoverage::new(3);
    coverage.set_total_len(300_000);
    for index in 0..3 {
        coverage.advance_watermark(index, 100_000);
    }
    assert_eq!(coverage.readable_at(2, 0).expect("readable"), 100_000);

    // Damage lands in part 0. Part 2 is untouched by it and entirely committed,
    // but nothing has vouched for it, so it now serves nothing new.
    coverage.cap_at_damage(0, 40_000);
    coverage.note_vouched_prefix(0, 40_000);

    assert!(coverage.is_gated());
    assert_eq!(
        coverage.readable_at(0, 0).expect("readable"),
        40_000,
        "the damaged part serves its vouched run"
    );
    // Part 2 is asserted through its state rather than by reading it: with
    // nothing vouched its frontier is zero, so any read there parks, and a test
    // that called one would hang rather than fail.
    assert_eq!(
        coverage.part_progress(2).expect("in range").vouched_prefix,
        None,
        "an untouched part has vouched for nothing and so serves nothing new"
    );
}

/// A prefix banked while the set was still clean pays out when it gates. A
/// part that completes before the first damage lands gets no later refresh —
/// its commits are over — so the prefix noted at its completion is the only
/// evidence it will ever have, and it must keep the part serving fully.
#[test]
fn a_prefix_banked_before_gating_serves_fully_once_the_set_gates() {
    let coverage = SetCoverage::new(2);
    coverage.set_total_len(200_000);

    // Part 0 completes clean while the set is ungated; its full length is
    // vouched at completion, exactly as the wiring does.
    coverage.advance_watermark(0, 100_000);
    coverage.note_part_len(0, 100_000);
    coverage.mark_part_complete(0);
    coverage.note_vouched_prefix(0, 100_000);
    assert!(!coverage.is_gated());

    // Damage then lands in part 1 and gates the set.
    coverage.advance_watermark(1, 100_000);
    coverage.cap_at_damage(1, 30_000);
    coverage.note_vouched_prefix(1, 30_000);
    assert!(coverage.is_gated());

    // The clean, complete, fully vouched part still serves to its end — and
    // reports end-of-part there rather than parking.
    assert_eq!(coverage.readable_at(0, 0).expect("readable"), 100_000);
    assert_eq!(coverage.readable_at(0, 100_000).expect("readable"), 0);
}

/// A gated part serves exactly its vouched prefix, and grows with it.
#[test]
fn a_gated_part_serves_its_vouched_prefix_and_grows_with_it() {
    let coverage = SetCoverage::new(2);
    coverage.set_total_len(200_000);
    coverage.advance_watermark(0, 100_000);
    coverage.advance_watermark(1, 100_000);
    coverage.cap_at_damage(1, 50_000);
    coverage.note_vouched_prefix(1, 50_000);

    // Part 0 is fully committed but unvouched, so its frontier is zero and a
    // read there would park. Claims arrive for it, a block at a time.
    assert_eq!(
        coverage.part_progress(0).expect("in range").vouched_prefix,
        None
    );
    coverage.note_vouched_prefix(0, 65_536);
    assert_eq!(coverage.readable_at(0, 0).expect("readable"), 65_536);
    coverage.note_vouched_prefix(0, 100_000);
    assert_eq!(coverage.readable_at(0, 0).expect("readable"), 100_000);

    // Monotone: a stale, lower claim cannot retract a proved prefix.
    coverage.note_vouched_prefix(0, 1_000);
    assert_eq!(coverage.readable_at(0, 0).expect("readable"), 100_000);
}

/// A complete part whose blocks are all Intact serves fully even while gated —
/// the tail probe has to keep working on an undamaged last part.
#[test]
fn a_fully_vouched_complete_part_serves_to_its_end_while_gated() {
    let coverage = SetCoverage::new(2);
    coverage.set_total_len(150_000);
    coverage.advance_watermark(0, 50_000);
    coverage.cap_at_damage(0, 10_000);
    coverage.note_vouched_prefix(0, 10_000);

    coverage.advance_watermark(1, 100_000);
    coverage.note_part_len(1, 100_000);
    coverage.mark_part_complete(1);
    coverage.note_vouched_prefix(1, 100_000);

    assert!(coverage.is_gated());
    assert_eq!(coverage.readable_at(1, 0).expect("readable"), 100_000);
    // And it reports end-of-part rather than parking, because nothing holds it.
    assert_eq!(coverage.readable_at(1, 100_000).expect("readable"), 0);
}

/// A gated part that is complete but only partly vouched must PARK at its
/// vouched edge, not report end-of-part — the rest is what repair will rewrite.
#[test]
fn a_gated_complete_but_unvouched_part_parks_at_its_edge() {
    let fixture = split_fixture(payload(60_000, 91), &[]);
    let coverage = std::sync::Arc::new(SetCoverage::new(1));
    coverage.set_total_len(60_000);
    coverage.note_part_len(0, 60_000);
    coverage.advance_watermark(0, 60_000);
    coverage.mark_part_complete(0);
    coverage.cap_at_damage(0, 30_000);
    coverage.note_vouched_prefix(0, 30_000);

    let paths = fixture.paths.clone();
    let reader_coverage = std::sync::Arc::clone(&coverage);
    let worker = thread::spawn(move || {
        let mut reader = GatedSplitReader::open(&paths, reader_coverage).expect("open reader");
        reader
            .seek(SeekFrom::Start(30_000))
            .expect("seek to the edge");
        let mut buf = [0u8; 256];
        reader.read_exact(&mut buf).map(|()| buf)
    });

    thread::sleep(SETTLE);
    assert!(
        coverage.park_count() > 0,
        "a complete-but-unvouched part must park at its vouched edge"
    );

    coverage.release_after_repair(0, 60_000);
    let read = worker.join().expect("reader thread").expect("read");
    assert_eq!(read.as_slice(), &fixture.bytes[30_000..30_256]);
}

/// Release lifts gating with the caps: post-repair bytes are verified by the
/// repair itself, so the vouched prefixes have nothing left to say.
#[test]
fn releasing_after_repair_lifts_gating_as_well_as_the_cap() {
    let coverage = SetCoverage::new(1);
    coverage.set_total_len(50_000);
    coverage.advance_watermark(0, 30_000);
    coverage.cap_at_damage(0, 10_000);
    coverage.note_vouched_prefix(0, 10_000);
    assert!(coverage.is_gated());

    coverage.release_after_repair(0, 50_000);

    assert!(!coverage.is_gated(), "repair verified what it wrote");
    assert!(!coverage.has_damage_cap());
    assert_eq!(coverage.readable_at(0, 0).expect("readable"), 50_000);
}

/// A gated chase parked on evidence that never arrives must stay reachable by
/// abort — otherwise the blocking thread waits for ever.
#[test]
fn abort_unblocks_a_reader_parked_under_the_gate() {
    let fixture = split_fixture(payload(50_000, 93), &[]);
    let coverage = std::sync::Arc::new(SetCoverage::new(1));
    coverage.set_total_len(50_000);
    coverage.note_part_len(0, 50_000);
    coverage.advance_watermark(0, 50_000);
    coverage.mark_part_complete(0);
    // Gated with nothing vouched: the frontier is zero and no claim is coming.
    coverage.cap_at_damage(0, 0);

    let paths = fixture.paths.clone();
    let reader_coverage = std::sync::Arc::clone(&coverage);
    let worker = thread::spawn(move || {
        let mut reader = GatedSplitReader::open(&paths, reader_coverage).expect("open reader");
        let mut buf = [0u8; 128];
        reader
            .read_exact(&mut buf)
            .expect_err("aborted under the gate")
    });

    thread::sleep(SETTLE);
    assert!(coverage.park_count() > 0, "the reader must be parked");

    coverage.abort("gated chase stalled: no vouching evidence after repair");

    let error = worker.join().expect("reader thread");
    assert!(
        error.to_string().contains("gated chase stalled"),
        "a gated park must be reachable by abort: {error}"
    );
}
