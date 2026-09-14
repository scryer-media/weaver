use super::*;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

fn reader_for(paths: &[PathBuf], sequential: bool) -> (GatedSplitReader, Arc<SetCoverage>) {
    let coverage = Arc::new(SetCoverage::new(paths.len()));
    let mut total = 0;
    for (index, path) in paths.iter().enumerate() {
        let len = std::fs::metadata(path).unwrap().len();
        total += len;
        coverage.advance_watermark(index, len);
        coverage.mark_part_complete(index);
    }
    if !sequential {
        coverage.set_total_len(total);
    }
    let mut reader = GatedSplitReader::open(paths, Arc::clone(&coverage)).unwrap();
    reader.sequential = sequential;
    (reader, coverage)
}

fn barrier(reader: &mut GatedSplitReader) -> (mpsc::Receiver<()>, mpsc::SyncSender<()>) {
    let (entered, observer) = mpsc::sync_channel(1);
    let (release, resume) = mpsc::sync_channel(1);
    reader.read_barrier = Some((entered, resume));
    (observer, release)
}

fn wait_for_park(coverage: &SetCoverage, previous: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while coverage.park_count() == previous {
        assert!(Instant::now() < deadline, "reader never parked");
        thread::yield_now();
    }
}

fn replace(path: &Path, bytes: &[u8]) {
    std::fs::rename(path, path.with_extension("old")).unwrap();
    std::fs::write(path, bytes).unwrap();
}

#[test]
fn a_read_in_flight_retries_after_repair_replaces_its_open_file() {
    for sequential in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part");
        std::fs::write(&path, b"Abad!").unwrap();
        let (mut reader, coverage) = reader_for(std::slice::from_ref(&path), sequential);
        reader.read_exact(&mut [0]).unwrap();
        let (entered, resume) = barrier(&mut reader);
        let worker = thread::spawn(move || {
            let mut rest = Vec::new();
            reader.read_to_end(&mut rest).unwrap();
            rest
        });
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        coverage.cap_at_damage(0, 1);
        coverage.note_vouched_prefix(0, 1);
        coverage.pause_for_repair();
        assert_eq!(coverage.consumed_high_water(0), 1);
        replace(&path, b"AGOOD");
        coverage.release_after_repair(0, 5);
        coverage.resume_after_repair();
        resume.send(()).unwrap();
        assert_eq!(worker.join().unwrap(), b"GOOD");
        assert_eq!(coverage.consumed_high_water(0), 5);
    }
}

#[test]
fn a_read_in_flight_cannot_cross_a_new_damage_cap_or_repair_pause() {
    for pause in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let paths = [dir.path().join("first"), dir.path().join("second")];
        std::fs::write(&paths[0], b"Abad!").unwrap();
        std::fs::write(&paths[1], b"tail").unwrap();
        let (mut reader, coverage) = reader_for(&paths, true);
        reader.read_exact(&mut [0]).unwrap();
        let (entered, resume) = barrier(&mut reader);
        let worker = thread::spawn(move || {
            let mut rest = Vec::new();
            reader.read_to_end(&mut rest).unwrap();
            rest
        });
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        // Test the pause without a damage cap too: some evidence arrives only
        // in the disk verification pass, after the last download verdict.
        if pause {
            coverage.pause_for_repair();
        } else {
            coverage.cap_at_damage(0, 1);
            coverage.note_vouched_prefix(0, 1);
        }
        let parks = coverage.park_count();
        resume.send(()).unwrap();
        wait_for_park(&coverage, parks);
        assert_eq!(coverage.consumed_high_water(0), 1);
        replace(&paths[0], b"AGOOD");
        replace(&paths[1], b"TAIL");
        coverage.release_after_repair(0, 5);
        // Publication remains frozen while individual parts are reconciled.
        if pause {
            assert!(!coverage.commit_read(0, 1, 4, 1).unwrap());
            assert!(!coverage.commit_read(1, 0, 4, 0).unwrap());
        }
        coverage.release_after_repair(1, 4);
        coverage.resume_after_repair();
        assert_eq!(worker.join().unwrap(), b"GOODTAIL");
    }
}

#[test]
fn abort_rejects_bytes_already_read_but_not_published() {
    for sequential in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part");
        std::fs::write(&path, b"payload").unwrap();
        let (mut reader, coverage) = reader_for(&[path], sequential);
        let (entered, resume) = barrier(&mut reader);
        let worker = thread::spawn(move || {
            let error = reader.read(&mut [0; 7]).unwrap_err();
            assert_eq!(reader.position(), 0);
            error
        });
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        coverage.pause_for_repair();
        coverage.abort("cancel during repair");
        resume.send(()).unwrap();
        assert!(
            worker
                .join()
                .unwrap()
                .to_string()
                .contains("cancel during repair")
        );
        assert_eq!(coverage.consumed_high_water(0), 0);
    }
}

#[test]
fn growing_a_part_after_crossing_its_boundary_invalidates_the_reader() {
    let dir = tempfile::tempdir().unwrap();
    let paths = [dir.path().join("first"), dir.path().join("second")];
    std::fs::write(&paths[0], b"Aa").unwrap();
    std::fs::write(&paths[1], b"BB").unwrap();
    let (mut reader, coverage) = reader_for(&paths, true);
    let mut prefix = [0; 3];
    reader.read_exact(&mut prefix).unwrap();
    assert_eq!(&prefix, b"AaB");
    coverage.pause_for_repair();
    replace(&paths[0], b"AaX");
    coverage.release_after_repair(0, 3);
    coverage.release_after_repair(1, 2);
    coverage.resume_after_repair();
    let error = reader.read(&mut [0; 8]).unwrap_err();
    assert!(error.to_string().contains("after its boundary was used"));
}

#[test]
fn shrinking_an_unread_part_after_seeking_across_it_invalidates_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let paths = [dir.path().join("first"), dir.path().join("second")];
    std::fs::write(&paths[0], b"AAA").unwrap();
    std::fs::write(&paths[1], b"BB").unwrap();
    let (mut reader, coverage) = reader_for(&paths, false);
    reader.seek(SeekFrom::Start(3)).unwrap();
    reader.read_exact(&mut [0]).unwrap();
    assert_eq!(coverage.consumed_high_water(0), 0);
    coverage.pause_for_repair();
    replace(&paths[0], b"AA");
    coverage.release_after_repair(0, 2);
    coverage.resume_after_repair();
    let error = reader.read(&mut [0]).unwrap_err();
    assert!(error.to_string().contains("after its boundary was used"));
}

#[test]
fn changing_a_part_before_its_boundary_is_used_keeps_the_vouched_prefix() {
    for repaired in [b"AaX".as_slice(), b"A".as_slice()] {
        let dir = tempfile::tempdir().unwrap();
        let paths = [dir.path().join("first"), dir.path().join("second")];
        std::fs::write(&paths[0], b"Aa").unwrap();
        std::fs::write(&paths[1], b"BB").unwrap();
        let (mut reader, coverage) = reader_for(&paths, true);
        let mut output = vec![0];
        reader.read_exact(&mut output).unwrap();
        coverage.pause_for_repair();
        replace(&paths[0], repaired);
        coverage.release_after_repair(0, repaired.len() as u64);
        coverage.release_after_repair(1, 2);
        coverage.resume_after_repair();
        reader.read_to_end(&mut output).unwrap();
        assert_eq!(output, [repaired, b"BB"].concat());
    }
}

#[test]
fn a_paused_reader_at_cached_eof_is_still_cancelled() {
    for sequential in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part");
        std::fs::write(&path, b"A").unwrap();
        let (mut reader, coverage) = reader_for(&[path], sequential);
        reader.read_to_end(&mut Vec::new()).unwrap();
        coverage.pause_for_repair();
        let parks = coverage.park_count();
        let worker = thread::spawn(move || reader.read(&mut [0]).unwrap_err());
        wait_for_park(&coverage, parks);
        coverage.abort("cancel at EOF");
        assert!(worker.join().unwrap().to_string().contains("cancel at EOF"));
    }
}
