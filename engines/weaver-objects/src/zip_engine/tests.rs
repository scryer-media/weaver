// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::io::{self, Cursor, Write as _};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use ::zip::write::SimpleFileOptions;
use ::zip::{AesMode, CompressionMethod, ZipWriter};
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
        Said::Member(0, "notes.txt".into(), Some(NOTES.len() as u64)),
        Said::Bytes(0, NOTES.to_vec()),
        Said::Closed(0),
        Said::Member(1, "deep/second.bin".into(), Some(second().len() as u64)),
        Said::Bytes(1, second()),
        Said::Closed(1),
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
            other => panic!("an engine run does not say {other:?}"),
        }
    }
    out
}

fn run_over(engine: &mut ZipEngine, gate: Arc<Gate>) -> Vec<Said> {
    let mut emits = Vec::new();
    let mut sink = |emit| {
        emits.push(emit);
        true
    };
    engine.start().run(gate, &mut sink);
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
        extraction.run(volumes, &mut sink);
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
        extraction.run(volumes, &mut sink);
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
        vec![Said::NeedsPassword],
        "an encrypted member with no password is never announced"
    );

    let mut out = Vec::new();
    engine.offer_password("not-the-password", &mut out);
    let wrong = run(&mut engine, vec![encrypted()]);
    assert_eq!(wrong.last(), Some(&Said::NeedsPassword));
    assert!(!wrong.iter().any(|said| matches!(said, Said::Closed(_))));

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
        [Said::Retract(0), Said::Refuse(Why::Unrepairable)]
    );
    assert!(!said.contains(&Said::Closed(0)));
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
    let object = crate::build(Kind::ZipEngine, ObjectId(4)).unwrap();
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
