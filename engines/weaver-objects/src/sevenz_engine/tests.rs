// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::io::{self, Cursor};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use sevenz_turbo::encoder_options::{AesEncoderOptions, Lzma2Options};
use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password};
use weaver_graph::{ObjectId, Purpose, Volumes, Why};

use super::*;
use crate::testing::payload;

const NOTES: &[u8] = b"the first member, stored";
const PASSWORD: &str = "a-test-passphrase";

fn second() -> Vec<u8> {
    (0..64u8).collect()
}

// A link's body is its target; 7-Zip marks it with a Unix mode in the high
// half of the attributes.
fn link(name: &str) -> ArchiveEntry {
    let mut entry = ArchiveEntry::new_file(name);
    entry.has_windows_attributes = true;
    entry.windows_attributes = (0o120_777 << 16) | 0x8000;
    entry
}

fn archive(methods: Vec<EncoderConfiguration>, members: &[(&str, &[u8])]) -> Vec<u8> {
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(methods);
    writer
        .push_archive_entry::<Cursor<Vec<u8>>>(ArchiveEntry::new_directory("deep"), None)
        .unwrap();
    writer
        .push_archive_entry(link("deep/link"), Some(Cursor::new(b"notes.txt".to_vec())))
        .unwrap();
    for (name, bytes) in members {
        writer
            .push_archive_entry(
                ArchiveEntry::new_file(name),
                Some(Cursor::new(bytes.to_vec())),
            )
            .unwrap();
    }
    writer.finish().unwrap().into_inner()
}

fn store() -> Vec<u8> {
    archive(
        vec![EncoderConfiguration::new(EncoderMethod::COPY)],
        &[("notes.txt", NOTES), ("deep/second.bin", &second())],
    )
}

fn store_said() -> Vec<Said> {
    vec![
        Said::Member(0, "notes.txt".into(), Some(NOTES.len() as u64)),
        Said::Bytes(0, NOTES.to_vec()),
        Said::Closed(0),
        Said::Member(1, "deep/second.bin".into(), Some(64)),
        Said::Bytes(1, second()),
        Said::Closed(1),
    ]
}

fn lzma2(members: &[(&str, &[u8])]) -> Vec<u8> {
    let mut options = Lzma2Options::from_level(1);
    options.set_dictionary_size(64 << 10);
    archive(vec![EncoderConfiguration::from(options)], members)
}

fn encrypted() -> Vec<u8> {
    archive(
        vec![
            EncoderConfiguration::from(AesEncoderOptions::new(Password::new(PASSWORD))),
            EncoderConfiguration::new(EncoderMethod::COPY),
        ],
        &[("notes.txt", NOTES)],
    )
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
    fn count(&self) -> usize {
        self.volumes.len()
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

fn run_over(engine: &SevenZipEngine, gate: Arc<Gate>) -> Vec<Said> {
    let mut emits = Vec::new();
    let mut sink = |emit| {
        emits.push(emit);
        true
    };
    engine.start().run(gate, &mut sink);
    said(&emits)
}

fn run(engine: &SevenZipEngine, volumes: Vec<Vec<u8>>) -> Vec<Said> {
    run_over(engine, Gate::whole(volumes))
}

fn named(names: &[&str]) -> SevenZipEngine {
    let mut engine = SevenZipEngine::new();
    for (volume, name) in names.iter().enumerate() {
        engine.bound(VolumeIndex(volume as u32), name);
    }
    engine
}

fn split(bytes: &[u8], at: usize) -> Vec<Vec<u8>> {
    vec![bytes[..at].to_vec(), bytes[at..].to_vec()]
}

#[test]
fn a_store_archive_extracts_its_files_by_name_and_skips_the_rest() {
    assert_eq!(run(&SevenZipEngine::new(), vec![store()]), store_said());
}

#[test]
fn an_lzma2_archive_extracts_its_members() {
    // Repeated so the coder has something to compress.
    let first = payload(7, 2_000).repeat(10);
    let second = payload(8, 500).repeat(10);
    let archive = lzma2(&[("first.bin", &first), ("second.bin", &second)]);
    assert!(archive.len() < first.len(), "the fixture is compressed");
    assert_eq!(
        run(&SevenZipEngine::new(), vec![archive]),
        vec![
            Said::Member(0, "first.bin".into(), Some(20_000)),
            Said::Bytes(0, first),
            Said::Closed(0),
            Said::Member(1, "second.bin".into(), Some(5_000)),
            Said::Bytes(1, second),
            Said::Closed(1),
        ]
    );
}

#[test]
fn a_set_in_two_volumes_extracts_as_one() {
    let engine = named(&["set.7z.001", "set.7z.002"]);
    for at in [1, 32, 45, store().len() - 1] {
        assert_eq!(
            run(&engine, split(&store(), at)),
            store_said(),
            "split at {at}"
        );
    }
}

#[test]
fn a_run_waits_for_the_tail_and_continues_when_it_lands() {
    let archive = store();
    let gate = Gate::new(vec![archive.clone()], vec![archive.len() - 1]);
    let (sent, heard) = mpsc::channel();
    let extraction = SevenZipEngine::new().start();
    let volumes: Arc<dyn Volumes> = gate.clone();
    let runner = thread::spawn(move || {
        let mut sink = |emit| sent.send(emit).is_ok();
        extraction.run(volumes, &mut sink);
    });

    gate.wait_for_reader(0);
    assert!(
        heard.try_iter().next().is_none(),
        "nothing is named before the end header lands"
    );

    gate.release(0);
    runner.join().unwrap();
    assert_eq!(said(&heard.try_iter().collect::<Vec<_>>()), store_said());
}

#[test]
fn a_run_over_a_set_waits_for_the_volume_holding_the_tail() {
    let gate = Gate::new(split(&store(), 40), vec![40, 0]);
    let (sent, heard) = mpsc::channel();
    let extraction = named(&["set.7z.001", "set.7z.002"]).start();
    let volumes: Arc<dyn Volumes> = gate.clone();
    let runner = thread::spawn(move || {
        let mut sink = |emit| sent.send(emit).is_ok();
        extraction.run(volumes, &mut sink);
    });

    gate.wait_for_reader(1);
    assert!(heard.try_iter().next().is_none());

    gate.release(1);
    runner.join().unwrap();
    assert_eq!(said(&heard.try_iter().collect::<Vec<_>>()), store_said());
}

#[test]
fn a_wrong_password_asks_for_another_and_the_right_one_extracts() {
    let mut engine = named(&["locked.7z"]);
    assert_eq!(
        run(&engine, vec![encrypted()]),
        vec![Said::NeedsPassword],
        "an encrypted member with no password is never announced"
    );

    let mut out = Vec::new();
    engine.offer_password("not-the-password", &mut out);
    let wrong = run(&engine, vec![encrypted()]);
    assert_eq!(wrong.last(), Some(&Said::NeedsPassword));
    assert!(!wrong.iter().any(|said| matches!(said, Said::Closed(_))));

    engine.offer_password(PASSWORD, &mut out);
    assert!(out.is_empty());
    assert_eq!(
        run(&engine, vec![encrypted()]),
        vec![
            Said::Member(0, "notes.txt".into(), Some(NOTES.len() as u64)),
            Said::Bytes(0, NOTES.to_vec()),
            Said::Closed(0),
        ]
    );
}

#[test]
fn a_member_whose_checksum_fails_is_retracted_and_refused() {
    let mut archive = store();
    let at = archive
        .windows(NOTES.len())
        .position(|window| window == NOTES)
        .unwrap();
    archive[at + 3] ^= 0x20;
    let said = run(&SevenZipEngine::new(), vec![archive]);
    assert_eq!(
        said[said.len() - 2..],
        [Said::Retract(0), Said::Refuse(Why::Unrepairable)]
    );
    assert!(!said.contains(&Said::Closed(0)));
}

#[test]
fn something_that_is_not_a_7z_is_refused_as_unsupported() {
    let mut archive = store();
    archive[0] ^= 0xff;
    assert_eq!(
        run(&SevenZipEngine::new(), vec![archive]),
        vec![Said::Refuse(Why::Unsupported)]
    );
}

#[test]
fn a_sink_that_says_stop_hears_nothing_more() {
    let mut emits = Vec::new();
    let mut sink = |emit: Emit| {
        let stop = matches!(emit, Emit::Bytes { .. });
        emits.push(emit);
        !stop
    };
    SevenZipEngine::new()
        .start()
        .run(Gate::whole(vec![store()]), &mut sink);
    let said = said(&emits);
    assert_eq!(said.len(), 2);
    assert_eq!(
        said[0],
        Said::Member(0, "notes.txt".into(), Some(NOTES.len() as u64))
    );
    assert!(matches!(said[1], Said::Bytes(0, _)));
}

#[test]
fn claim_names_the_volumes_of_its_own_set_only() {
    let mut engine = named(&["Set.7z.001"]);
    assert_eq!(engine.claim("set.7Z.002", b""), Some(VolumeIndex(1)));
    assert_eq!(engine.claim("set.7z.1000", b""), Some(VolumeIndex(999)));
    assert_eq!(engine.claim("set.7z.001", b""), None);
    assert_eq!(engine.claim("set.7z.000", b""), None);
    assert_eq!(engine.claim("other.7z.002", b""), None);
    assert_eq!(engine.claim("set.7z", b""), None);
    assert_eq!(engine.claim("set.part2.rar", b""), None);

    let mut lone = named(&["set.7z"]);
    assert_eq!(
        lone.claim("set.7z.002", b""),
        None,
        "a lone 7z has no siblings"
    );
}

#[test]
fn the_engine_bids_for_the_volumes_after_the_one_the_run_reached() {
    let mut engine = named(&["set.7z.001", "set.7z.002", "set.7z.003"]);
    let mut seeks = Vec::new();
    engine.wants(&mut seeks);
    assert!(
        seeks.is_empty(),
        "nothing is bid before the run says where it is"
    );

    engine.reached(VolumeIndex(0), 10);
    engine.wants(&mut seeks);
    let bids: Vec<_> = seeks
        .iter()
        .map(|seek| (seek.volume.0, seek.weight.0, seek.purpose))
        .collect();
    assert_eq!(bids, vec![(1, 1, Purpose::Data), (2, 2, Purpose::Data)]);

    seeks.clear();
    engine.bound(VolumeIndex(3), "set.7z.004");
    engine.reached(VolumeIndex(1), 0);
    engine.wants(&mut seeks);
    let bids: Vec<_> = seeks
        .iter()
        .map(|seek| (seek.volume.0, seek.weight.0))
        .collect();
    assert_eq!(
        bids,
        vec![(2, 1), (3, 2)],
        "the bids are restated from where the run stands"
    );
}

#[test]
fn the_object_is_an_engine_that_reads_in_order_and_keeps_nothing() {
    let object = crate::build(Kind::SevenZipEngine, ObjectId(4)).unwrap();
    assert_eq!(object.kind(), Kind::SevenZipEngine);
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
    assert_eq!(object.envelope(2, ByteRange::new(5, 9)), None);
}
