// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::io::{self, Cursor, Write as _};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use ::zip::write::SimpleFileOptions;
use ::zip::{AesMode, CompressionMethod, ZipArchive, ZipWriter};
use weaver_graph::{
    ByteRange, Emit, Engine, Kind, MemberId, Object, ObjectId, Placement, VolumeIndex, Volumes, Why,
};

use super::ZipEngine;
use crate::testing::{payload, zip64_end};
use crate::zip::records;

const NOTES: &[u8] = b"the first member, stored";
const PASSWORD: &str = "a-test-passphrase";

fn second() -> Vec<u8> {
    payload(9, 3_000).repeat(4)
}

// A directory, a link, a stored member and a deflated one.
fn archive(options: SimpleFileOptions) -> Vec<u8> {
    let stored = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    writer.add_directory("deep/", stored).unwrap();
    writer
        .add_symlink("deep/link", "notes.txt", stored)
        .unwrap();
    writer.start_file("notes.txt", options).unwrap();
    writer.write_all(NOTES).unwrap();
    writer
        .start_file(
            "deep/second.bin",
            options.compression_method(CompressionMethod::Deflated),
        )
        .unwrap();
    writer.write_all(&second()).unwrap();
    writer.finish().unwrap().into_inner()
}

fn plain() -> Vec<u8> {
    archive(SimpleFileOptions::default().compression_method(CompressionMethod::Stored))
}

fn encrypted() -> Vec<u8> {
    archive(
        SimpleFileOptions::default()
            .compression_method(CompressionMethod::Stored)
            .with_aes_encryption(AesMode::Aes256, PASSWORD),
    )
}

fn plain_said() -> Vec<Said> {
    vec![
        Said::Member(0, "deep/".into(), Some(0)),
        Said::Closed(0),
        Said::Member(1, "notes.txt".into(), Some(NOTES.len() as u64)),
        Said::Bytes(1, NOTES.to_vec()),
        Said::Closed(1),
        Said::Member(2, "deep/second.bin".into(), Some(second().len() as u64)),
        Said::Bytes(2, second()),
        Said::Closed(2),
    ]
}

// Volumes in memory whose bytes the test hands out, each up to a point. A
// read past what was handed out waits, and says which volume it waits on,
// so a test can act once the run is truly blocked. A volume is closed once
// all of it is handed out.
struct Gate {
    volumes: Vec<Vec<u8>>,
    state: Mutex<State>,
    changed: Condvar,
}

struct State {
    released: Vec<usize>,
    waiting: Option<u32>,
}

impl Gate {
    fn new(volumes: Vec<Vec<u8>>, released: Vec<usize>) -> Arc<Self> {
        Arc::new(Self {
            volumes,
            state: Mutex::new(State {
                released,
                waiting: None,
            }),
            changed: Condvar::new(),
        })
    }

    fn whole(volumes: Vec<Vec<u8>>) -> Arc<Self> {
        let released = volumes.iter().map(Vec::len).collect();
        Self::new(volumes, released)
    }

    fn release(&self, volume: u32) {
        let mut state = self.state.lock().unwrap();
        state.released[volume as usize] = self.volumes[volume as usize].len();
        self.changed.notify_all();
    }

    fn wait_for_reader(&self, volume: u32) {
        let mut state = self.state.lock().unwrap();
        while state.waiting != Some(volume) {
            state = self.changed.wait(state).unwrap();
        }
    }
}

impl Volumes for Gate {
    fn next(&self, after: Option<VolumeIndex>) -> std::io::Result<Option<VolumeIndex>> {
        let next = after.map_or(0, |after| after.0 as usize + 1);
        Ok((next < self.volumes.len()).then_some(VolumeIndex(next as u32)))
    }

    fn len(&self, volume: VolumeIndex) -> Option<u64> {
        let state = self.state.lock().unwrap();
        let full = self.volumes[volume.0 as usize].len();
        (state.released[volume.0 as usize] == full).then_some(full as u64)
    }

    fn read_at(&self, volume: VolumeIndex, offset: u64, into: &mut [u8]) -> io::Result<usize> {
        let bytes = self
            .volumes
            .get(volume.0 as usize)
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
        let offset = offset as usize;
        let mut state = self.state.lock().unwrap();
        while offset >= state.released[volume.0 as usize]
            && state.released[volume.0 as usize] < bytes.len()
        {
            state.waiting = Some(volume.0);
            self.changed.notify_all();
            state = self.changed.wait(state).unwrap();
        }
        let released = state.released[volume.0 as usize];
        let start = offset.min(released);
        let read = into.len().min(released - start);
        into[..read].copy_from_slice(&bytes[start..start + read]);
        Ok(read)
    }
}

// What a run said, with each member's bytes joined in order.
#[derive(Debug, PartialEq, Eq)]
enum Said {
    Member(u32, String, Option<u64>),
    Bytes(u32, Vec<u8>),
    Closed(u32),
    Retract(u32),
    NeedsPassword,
    Refuse(Why),
}

fn said(emits: &[Emit]) -> Vec<Said> {
    let mut out: Vec<Said> = Vec::new();
    for emit in emits {
        match emit {
            Emit::Member {
                member,
                name,
                length,
            } => out.push(Said::Member(member.0, name.clone(), *length)),
            Emit::Bytes {
                member,
                offset,
                bytes,
                provenance,
            } => {
                assert_eq!(*provenance, weaver_graph::Provenance::Fresh);
                match out.last_mut() {
                    Some(Said::Bytes(last, joined)) if *last == member.0 => {
                        assert_eq!(*offset, joined.len() as u64);
                        joined.extend_from_slice(bytes);
                    }
                    _ => {
                        assert_eq!(*offset, 0);
                        out.push(Said::Bytes(member.0, bytes.to_vec()));
                    }
                }
            }
            Emit::MemberClosed(member) => out.push(Said::Closed(member.0)),
            Emit::Retract(member) => out.push(Said::Retract(member.0)),
            Emit::NeedsPassword => out.push(Said::NeedsPassword),
            Emit::Refuse(why) => out.push(Said::Refuse(*why)),
            // Said beside the walk; `checksums` reads them.
            Emit::Checksum { .. } => {}
            other => panic!("an engine run does not say {other:?}"),
        }
    }
    out
}

fn run_over(engine: &mut ZipEngine, gate: Arc<dyn Volumes>) -> Vec<Said> {
    let mut emits = Vec::new();
    let mut sink = |emit| {
        emits.push(emit);
        true
    };
    engine
        .start()
        .run(gate, &mut sink, weaver_graph::Budget::default());
    said(&emits)
}

fn run(engine: &mut ZipEngine, volumes: Vec<Vec<u8>>) -> Vec<Said> {
    run_over(engine, Gate::whole(volumes))
}

fn named(names: &[&str]) -> ZipEngine {
    let mut engine = ZipEngine::new();
    for (volume, name) in names.iter().enumerate() {
        engine.bound(VolumeIndex(volume as u32), name, &[], None);
    }
    engine
}

// The archive as a spanned set of `parts` volumes, its directory naming
// each entry's disk and the offset within it.
fn spanned(single: &[u8], parts: usize) -> Vec<Vec<u8>> {
    const MARKER: [u8; 4] = [0x50, 0x4b, 0x07, 0x08];
    let end = records::find_end(single).unwrap();
    let mut directory = single[end.offset as usize..(end.offset + end.size) as usize].to_vec();
    let entries = records::directory(&directory, end.entries).unwrap();
    let start = end.offset + MARKER.len() as u64;
    let part = (start + end.size + 22).div_ceil(parts as u64);
    let locate = |at: u64| ((at / part) as u16, (at % part) as u32);
    for entry in &entries {
        let (disk, offset) = locate(entry.offset + MARKER.len() as u64);
        directory[entry.disk_at.0..entry.disk_at.0 + 2].copy_from_slice(&disk.to_le_bytes());
        directory[entry.offset_at.0..entry.offset_at.0 + 4].copy_from_slice(&offset.to_le_bytes());
    }
    let (disk, offset) = locate(start);
    let mut joined = MARKER.to_vec();
    joined.extend_from_slice(&single[..end.offset as usize]);
    joined.extend_from_slice(&directory);
    joined.extend(records::END.to_le_bytes());
    joined.extend((parts as u16 - 1).to_le_bytes());
    joined.extend(disk.to_le_bytes());
    joined.extend((end.entries as u16).to_le_bytes());
    joined.extend((end.entries as u16).to_le_bytes());
    joined.extend((end.size as u32).to_le_bytes());
    joined.extend(offset.to_le_bytes());
    joined.extend(0u16.to_le_bytes());
    let volumes: Vec<_> = joined.chunks(part as usize).map(<[u8]>::to_vec).collect();
    assert_eq!(volumes.len(), parts);
    volumes
}

#[test]
fn a_zip_extracts_its_files_in_directory_order_and_skips_the_rest() {
    assert_eq!(run(&mut ZipEngine::new(), vec![plain()]), plain_said());
}

#[test]
fn a_run_waits_for_the_volume_to_close_and_extracts_once_it_has() {
    let archive = plain();
    let gate = Gate::new(vec![archive.clone()], vec![archive.len() / 2]);
    let (sent, heard) = mpsc::channel();
    let extraction = ZipEngine::new().start();
    let volumes: Arc<dyn Volumes> = gate.clone();
    let runner = thread::spawn(move || {
        let mut sink = |emit| sent.send(emit).is_ok();
        extraction.run(volumes, &mut sink, weaver_graph::Budget::default());
    });

    gate.wait_for_reader(0);
    assert!(
        heard.try_iter().next().is_none(),
        "nothing is named before the directory lands"
    );

    gate.release(0);
    runner.join().unwrap();
    assert_eq!(said(&heard.try_iter().collect::<Vec<_>>()), plain_said());
}

#[test]
fn a_spanned_set_extracts_as_one_once_its_last_volume_lands() {
    let volumes = spanned(&plain(), 3);
    let released = vec![volumes[0].len(), volumes[1].len(), 0];
    let gate = Gate::new(volumes, released);
    let (sent, heard) = mpsc::channel();
    let extraction = named(&["alpha.z01", "alpha.z02", "alpha.zip"]).start();
    let volumes: Arc<dyn Volumes> = gate.clone();
    let runner = thread::spawn(move || {
        let mut sink = |emit| sent.send(emit).is_ok();
        extraction.run(volumes, &mut sink, weaver_graph::Budget::default());
    });

    gate.wait_for_reader(2);
    assert!(heard.try_iter().next().is_none());

    gate.release(2);
    runner.join().unwrap();
    assert_eq!(said(&heard.try_iter().collect::<Vec<_>>()), plain_said());
}

#[test]
fn a_wrong_password_asks_for_another_and_the_right_one_extracts() {
    let mut engine = named(&["locked.zip"]);
    assert_eq!(
        run(&mut engine, vec![encrypted()]),
        vec![
            Said::Member(0, "deep/".into(), Some(0)),
            Said::Closed(0),
            Said::NeedsPassword,
        ],
        "an encrypted member with no password is never announced, though the directory before it is"
    );

    let mut out = Vec::new();
    engine.offer_password("not-the-password", &mut out);
    let wrong = run(&mut engine, vec![encrypted()]);
    assert_eq!(wrong.last(), Some(&Said::NeedsPassword));
    assert!(
        !wrong
            .iter()
            .any(|said| matches!(said, Said::Closed(id) if *id > 0))
    );

    engine.offer_password(PASSWORD, &mut out);
    assert!(out.is_empty());
    assert_eq!(run(&mut engine, vec![encrypted()]), plain_said());
}

#[test]
fn a_member_whose_checksum_fails_is_retracted_and_refused() {
    let mut archive = plain();
    let at = archive
        .windows(NOTES.len())
        .position(|window| window == NOTES)
        .unwrap();
    archive[at + 3] ^= 0x20;
    let said = run(&mut ZipEngine::new(), vec![archive]);
    assert_eq!(
        said[said.len() - 2..],
        [Said::Retract(1), Said::Refuse(Why::Unrepairable)]
    );
    assert!(!said.contains(&Said::Closed(1)));
}

#[test]
fn a_set_the_names_disagree_with_is_refused_as_unsupported() {
    let volumes = spanned(&plain(), 3);
    assert_eq!(
        run(
            &mut named(&["alpha.z01", "alpha.zip"]),
            vec![volumes[0].clone(), volumes[2].clone()]
        ),
        vec![Said::Refuse(Why::Unsupported)]
    );
}

// The spanned set ended by zip64 records saying what `edit` makes them say.
fn spanned64(edit: impl FnOnce(&mut crate::testing::Zip64)) -> Vec<Vec<u8>> {
    let mut volumes = spanned(&plain(), 3);
    zip64_end(&mut volumes[2], 3, edit);
    volumes
}

fn run_spanned(volumes: Vec<Vec<u8>>) -> Vec<Said> {
    run(
        &mut named(&["alpha.z01", "alpha.z02", "alpha.zip"]),
        volumes,
    )
}

#[test]
fn a_spanned_set_ended_by_zip64_records_extracts_as_one() {
    assert_eq!(run_spanned(spanned64(|_| {})), plain_said());
}

#[test]
fn zip64_places_past_the_end_of_the_bytes_are_refused_as_unsupported() {
    let refused = vec![Said::Refuse(Why::Unsupported)];
    assert_eq!(
        run_spanned(spanned64(|said| said.record = u64::MAX - 8)),
        refused,
        "a zip64 record past the last byte"
    );
    assert_eq!(
        run_spanned(spanned64(|said| said.offset = u64::MAX - 8)),
        refused,
        "a directory past the last byte"
    );
    assert_eq!(
        run_spanned(spanned64(|said| said.size = 1 << 62)),
        refused,
        "a directory longer than the bytes before its end records"
    );
    assert_eq!(
        run_spanned(spanned64(|said| said.size += 1)),
        refused,
        "a directory that runs into its end records"
    );
}

#[test]
fn claim_names_the_volumes_of_its_own_set_with_the_last_after_them() {
    let mut engine = named(&["Set.Z01"]);
    assert_eq!(engine.claim("set.z02", b"", None), Some(VolumeIndex(1)));
    assert_eq!(engine.claim("set.z100", b"", None), Some(VolumeIndex(99)));
    assert_eq!(
        engine.claim("set.zip", b"", None),
        Some(VolumeIndex(u32::MAX)),
        "the last is after every numbered volume, whatever their count"
    );
    assert_eq!(
        engine.claim("set.z01", b"", None),
        Some(VolumeIndex(0)),
        "a volume posted again is that volume"
    );
    assert_eq!(engine.claim("set.z00", b"", None), None);
    assert_eq!(engine.claim("other.z02", b"", None), None);
    assert_eq!(engine.claim("set.7z.002", b"", None), None);
    engine.bound(VolumeIndex(u32::MAX), "set.zip", &[], None);
    assert_eq!(
        engine.claim("set.zip", b"", None),
        Some(VolumeIndex(u32::MAX))
    );
    assert_eq!(
        engine.claim("set.z02", b"", None),
        Some(VolumeIndex(1)),
        "a numbered volume posted after the last still takes its number"
    );

    let mut lone = named(&["set.zip"]);
    assert_eq!(
        lone.claim("set.z01", b"", None),
        None,
        "a lone zip has no siblings"
    );
}

#[test]
fn the_object_is_an_engine_that_reads_in_order_and_keeps_nothing() {
    let object = crate::build(
        Kind::ZipEngine,
        ObjectId(4),
        weaver_graph::Budget::default(),
    )
    .unwrap();
    assert_eq!(object.kind(), Kind::ZipEngine);
    assert!(object.reads_in_order());
    assert!(object.engine().is_some());
    let mut placed = Vec::new();
    object.locate(VolumeIndex(2), ByteRange::new(5, 9), &mut placed);
    assert_eq!(
        placed,
        vec![Placement::Envelope {
            envelope: 2,
            range: ByteRange::new(5, 9)
        }]
    );
    let mut extents = Vec::new();
    object.extent(MemberId(0), ByteRange::new(0, 4), &mut extents);
    assert!(extents.is_empty());
}

// A piece put into a sparse volume: its offset and its bytes.
type Piece = (u64, Vec<u8>);

// Volumes of any length that hold their bytes only where a piece is put and
// zeros everywhere else, so a set can run past what a narrow field holds
// without its bytes in memory.
struct Sparse {
    volumes: Vec<(u64, Vec<Piece>)>,
}

impl Volumes for Sparse {
    fn next(&self, after: Option<VolumeIndex>) -> std::io::Result<Option<VolumeIndex>> {
        let next = after.map_or(0, |after| after.0 as usize + 1);
        Ok((next < self.volumes.len()).then_some(VolumeIndex(next as u32)))
    }

    fn len(&self, volume: VolumeIndex) -> Option<u64> {
        Some(self.volumes[volume.0 as usize].0)
    }

    fn read_at(&self, volume: VolumeIndex, offset: u64, into: &mut [u8]) -> io::Result<usize> {
        let (len, pieces) = &self.volumes[volume.0 as usize];
        let read = into.len().min(len.saturating_sub(offset) as usize);
        let into = &mut into[..read];
        into.fill(0);
        let wanted = offset..offset + read as u64;
        for (at, bytes) in pieces {
            let start = wanted.start.max(*at);
            let end = wanted.end.min(at + bytes.len() as u64);
            if start < end {
                into[(start - offset) as usize..(end - offset) as usize]
                    .copy_from_slice(&bytes[(start - at) as usize..(end - at) as usize]);
            }
        }
        Ok(read)
    }
}

// Past what a narrow field holds: the first volume runs this far.
const FAR: u64 = (1 << 32) + 4_096;

// Two stored members, the second marked large so its directory entry has a
// zip64 field for its sizes when `large`.
fn far_archive(large: bool) -> Vec<u8> {
    let stored = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    writer.start_file("near.txt", stored).unwrap();
    writer.write_all(NOTES).unwrap();
    writer
        .start_file("far.bin", stored.large_file(large))
        .unwrap();
    writer.write_all(&second()).unwrap();
    writer.finish().unwrap().into_inner()
}

fn far_said() -> Vec<Said> {
    vec![
        Said::Member(0, "near.txt".into(), Some(NOTES.len() as u64)),
        Said::Bytes(0, NOTES.to_vec()),
        Said::Closed(0),
        Said::Member(1, "far.bin".into(), Some(second().len() as u64)),
        Said::Bytes(1, second()),
        Said::Closed(1),
    ]
}

// The archive spanned over two volumes, the first `FAR` long: its first
// `before` entries, then zeros to its end; the second the other entries,
// the directory and the end record. Each entry's offset is within its disk,
// so it fits its narrow field, and only the joined image's does not.
fn far_set(single: &[u8], before: usize) -> Sparse {
    const MARKER: [u8; 4] = [0x50, 0x4b, 0x07, 0x08];
    let end = records::find_end(single).unwrap();
    let mut directory = single[end.offset as usize..(end.offset + end.size) as usize].to_vec();
    let entries = records::directory(&directory, end.entries).unwrap();
    let split = entries.get(before).map_or(end.offset, |entry| entry.offset) as usize;
    for entry in &entries {
        let (disk, offset) = match entry.offset < split as u64 {
            true => (0u16, entry.offset as u32 + MARKER.len() as u32),
            false => (1, (entry.offset - split as u64) as u32),
        };
        directory[entry.disk_at.0..entry.disk_at.0 + 2].copy_from_slice(&disk.to_le_bytes());
        directory[entry.offset_at.0..entry.offset_at.0 + 4].copy_from_slice(&offset.to_le_bytes());
    }
    let mut first = MARKER.to_vec();
    first.extend_from_slice(&single[..split]);
    let mut last = single[split..end.offset as usize].to_vec();
    let offset = last.len() as u32;
    last.extend_from_slice(&directory);
    last.extend(records::END.to_le_bytes());
    last.extend(1u16.to_le_bytes());
    last.extend(1u16.to_le_bytes());
    last.extend((end.entries as u16).to_le_bytes());
    last.extend((end.entries as u16).to_le_bytes());
    last.extend((end.size as u32).to_le_bytes());
    last.extend(offset.to_le_bytes());
    last.extend(0u16.to_le_bytes());
    Sparse {
        volumes: vec![
            (FAR, vec![(0, first)]),
            (last.len() as u64, vec![(0, last)]),
        ],
    }
}

fn run_far(set: Sparse) -> Vec<Said> {
    run_over(&mut named(&["far.z01", "far.zip"]), Arc::new(set))
}

// The rewritten directory, as the store's own reader reads it, for a set
// whose image opens.
fn rewritten(set: Sparse) -> Vec<records::Central> {
    let image = match super::image::Image::open(crate::engine::Joined::new(Arc::new(set))) {
        Ok(image) => image,
        Err(_) => panic!("the set opens as an image"),
    };
    use std::io::{Read as _, Seek as _};
    let mut image = image;
    let len = image.seek(io::SeekFrom::End(0)).unwrap();
    let tail = len - 1_024.min(len);
    image.seek(io::SeekFrom::Start(tail)).unwrap();
    let mut whole = Vec::new();
    image.read_to_end(&mut whole).unwrap();
    let end = records::find_end(&whole).unwrap();
    assert!(
        end.wide,
        "a directory past a narrow field is ended by zip64 records"
    );
    let at = end.at as usize;
    let (disk, record) = records::locator(&whole[at - records::LOCATOR_LEN as usize..at]).unwrap();
    assert_eq!(disk, 0);
    let record = (record - tail) as usize;
    let end = records::end64(&whole[record..], end).unwrap();
    assert_eq!((end.this_disk, end.directory_disk), (0, 0));
    let directory = (end.offset - tail) as usize;
    assert_eq!(directory + end.size as usize, record);
    records::directory(&whole[directory..record], end.entries).unwrap()
}

#[test]
fn a_spanned_set_whose_directory_starts_past_a_narrow_field_is_ended_by_zip64_records() {
    assert_eq!(run_far(far_set(&far_archive(false), 2)), far_said());
    let entries = rewritten(far_set(&far_archive(false), 2));
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.offset_at.1)
            .collect::<Vec<_>>(),
        [false, false],
        "entries that fit their narrow fields keep them"
    );
}

#[test]
fn a_spanned_entry_whose_place_is_past_a_narrow_field_gains_a_zip64_offset() {
    for large in [false, true] {
        assert_eq!(
            run_far(far_set(&far_archive(large), 1)),
            far_said(),
            "large: {large}"
        );
        let entries = rewritten(far_set(&far_archive(large), 1));
        assert_eq!(entries[0].offset, 4);
        assert!(!entries[0].offset_at.1);
        assert_eq!(entries[1].offset, FAR, "large: {large}");
        assert!(entries[1].offset_at.1, "large: {large}");
        assert_eq!(entries[1].disk, 0);
        assert_eq!(entries[1].header.length, second().len() as u64);
        assert_eq!(entries[1].header.compressed, second().len() as u64);
    }
}

#[test]
fn a_directory_of_more_entries_than_a_narrow_count_is_ended_by_zip64_records() {
    const ENTRIES: u64 = 1 << 16;
    let mut directory = Vec::new();
    for index in 0..ENTRIES {
        let name = format!("{index:05x}");
        directory.extend(records::CENTRAL.to_le_bytes());
        directory.extend([20, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        directory.extend([0u8; 12]);
        directory.extend((name.len() as u16).to_le_bytes());
        directory.extend([0u8; 12]);
        directory.extend(0u32.to_le_bytes());
        directory.extend(name.as_bytes());
    }
    let end = records::End {
        at: 0,
        this_disk: 0,
        directory_disk: 0,
        entries: ENTRIES,
        size: directory.len() as u64,
        offset: 0,
        wide: true,
    };
    let rest =
        super::image::rewrite(&directory, &end, 0, &|disk| (disk == 0).then_some(0)).unwrap();
    let archive = ZipArchive::with_config(
        ::zip::read::Config {
            archive_offset: ::zip::read::ArchiveOffset::Known(0),
        },
        Cursor::new(rest),
    )
    .unwrap();
    assert_eq!(archive.len() as u64, ENTRIES);
    let name = |index: u64| archive.name_for_index(index as usize).unwrap().unwrap();
    assert_eq!(name(0), "00000");
    assert_eq!(name(ENTRIES - 1), "0ffff");
}

// A zip of one empty stored entry named by `name`'s raw bytes in both of its
// headers, each flagged UTF-8 or not as `flagged` says.
fn raw_named(name: &[u8], flagged: bool) -> Vec<u8> {
    const UTF8: u16 = 1 << 11;
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    let stored = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
    writer
        .start_file("n".repeat(name.len()).as_str(), stored)
        .unwrap();
    let mut archive = writer.finish().unwrap().into_inner();
    for (signature, flags_at, name_at) in [(records::LOCAL, 6, 30), (records::CENTRAL, 8, 46)] {
        let at = archive
            .windows(4)
            .position(|window| window == signature.to_le_bytes())
            .unwrap();
        let flags = records::u16_at(&archive, at + flags_at).unwrap();
        let flags = if flagged { flags | UTF8 } else { flags & !UTF8 };
        archive[at + flags_at..at + flags_at + 2].copy_from_slice(&flags.to_le_bytes());
        archive[at + name_at..at + name_at + name.len()].copy_from_slice(name);
    }
    archive
}

#[test]
fn the_store_and_the_engine_name_an_entry_alike_whatever_its_bytes_and_flag() {
    let cases: [(&[u8], bool); 4] = [
        ("café.txt".as_bytes(), false),
        (b"caf\x82.txt", false),
        ("café.txt".as_bytes(), true),
        (b"caf\x82.txt", true),
    ];
    for (name, flagged) in cases {
        let archive = raw_named(name, flagged);
        let engine = run(&mut ZipEngine::new(), vec![archive.clone()])
            .into_iter()
            .find_map(|said| match said {
                Said::Member(_, name, _) => Some(name),
                _ => None,
            });
        let find = |signature: u32| {
            archive
                .windows(4)
                .position(|window| window == signature.to_le_bytes())
                .unwrap()
        };
        let (central, end) = (find(records::CENTRAL), find(records::END));
        let store = records::directory(&archive[central..end], 1).unwrap()[0]
            .header
            .name
            .clone();
        let local = records::local(&archive).unwrap().name;
        let case = format!("{name:x?} flagged {flagged}");
        assert_eq!(
            engine.as_deref(),
            Some(store.as_str()),
            "{case}: engine and store"
        );
        assert_eq!(local, store, "{case}: local and central headers");
        assert_eq!(store, "café.txt", "{case}");
    }
}

// Each member's checksum as the run said it, in the order it was said.
fn checksums(emits: &[Emit]) -> Vec<(u32, u32)> {
    emits
        .iter()
        .filter_map(|emit| match emit {
            Emit::Checksum { member, crc32 } => Some((member.0, *crc32)),
            _ => None,
        })
        .collect()
}

fn crc(bytes: &[u8]) -> u32 {
    crate::grid::crc32(bytes)
}

#[test]
fn a_run_says_each_files_header_checksum_and_none_for_a_directory() {
    let mut emits = Vec::new();
    let mut sink = |emit| {
        emits.push(emit);
        true
    };
    ZipEngine::new().start().run(
        Gate::whole(vec![plain()]),
        &mut sink,
        weaver_graph::Budget::default(),
    );
    assert_eq!(
        checksums(&emits),
        vec![(1, crc(NOTES)), (2, crc(&second()))]
    );
}
