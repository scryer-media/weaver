// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::io::{self, Cursor};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use sevenz_turbo::encoder_options::{AesEncoderOptions, Lzma2Options};
use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password};
use weaver_graph::{ObjectId, Volumes, Why};

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
        Said::Member(2, "deep/".into(), Some(0)),
        Said::Closed(2),
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

fn run_over(engine: &mut SevenZipEngine, gate: Arc<Gate>) -> Vec<Said> {
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

fn run(engine: &mut SevenZipEngine, volumes: Vec<Vec<u8>>) -> Vec<Said> {
    run_over(engine, Gate::whole(volumes))
}

fn named(names: &[&str]) -> SevenZipEngine {
    let mut engine = SevenZipEngine::new();
    for (volume, name) in names.iter().enumerate() {
        engine.bound(VolumeIndex(volume as u32), name, &[], None);
    }
    engine
}

#[test]
fn a_store_archive_extracts_its_files_by_name_and_skips_the_rest() {
    assert_eq!(run(&mut SevenZipEngine::new(), vec![store()]), store_said());
}

#[test]
fn an_lzma2_archive_extracts_its_members() {
    // Repeated so the coder has something to compress.
    let first = payload(7, 2_000).repeat(10);
    let second = payload(8, 500).repeat(10);
    let archive = lzma2(&[("first.bin", &first), ("second.bin", &second)]);
    assert!(archive.len() < first.len(), "the fixture is compressed");
    assert_eq!(
        run(&mut SevenZipEngine::new(), vec![archive]),
        vec![
            Said::Member(0, "first.bin".into(), Some(20_000)),
            Said::Bytes(0, first),
            Said::Closed(0),
            Said::Member(1, "second.bin".into(), Some(5_000)),
            Said::Bytes(1, second),
            Said::Closed(1),
            Said::Member(2, "deep/".into(), Some(0)),
            Said::Closed(2),
        ]
    );
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
        extraction.run(volumes, &mut sink, weaver_graph::Budget::default());
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
fn a_wrong_password_asks_for_another_and_the_right_one_extracts() {
    let mut engine = named(&["locked.7z"]);
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
    assert_eq!(
        run(&mut engine, vec![encrypted()]),
        vec![
            Said::Member(0, "notes.txt".into(), Some(NOTES.len() as u64)),
            Said::Bytes(0, NOTES.to_vec()),
            Said::Closed(0),
            Said::Member(1, "deep/".into(), Some(0)),
            Said::Closed(1),
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
    let said = run(&mut SevenZipEngine::new(), vec![archive]);
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
        run(&mut SevenZipEngine::new(), vec![archive]),
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
    SevenZipEngine::new().start().run(
        Gate::whole(vec![store()]),
        &mut sink,
        weaver_graph::Budget::default(),
    );
    let said = said(&emits);
    assert_eq!(said.len(), 2);
    assert_eq!(
        said[0],
        Said::Member(0, "notes.txt".into(), Some(NOTES.len() as u64))
    );
    assert!(matches!(said[1], Said::Bytes(0, _)));
}

// A 7z is one file: no name numbers a piece of it, and a copy of the file
// is the file when its signature header measures the same container.
#[test]
fn claim_takes_a_copy_of_its_file_and_no_numbered_piece() {
    let bytes = store();
    let mut engine = SevenZipEngine::new();
    engine.bound(
        VolumeIndex(0),
        "set.7z",
        &bytes[..32],
        Some(bytes.len() as u64),
    );
    assert_eq!(
        engine.claim("e93a07c1d25b4f68a0c3e1f7b9d24c85", &bytes[..32], None),
        vec![VolumeIndex(0)]
    );
    for name in ["set.7z", "set.7z.001", "set.7z.002", "set.part2.rar"] {
        assert_eq!(
            engine.claim(name, b"", None),
            Vec::<VolumeIndex>::new(),
            "{name}"
        );
    }
    let mut numbered = named(&["Set.7z.001"]);
    assert_eq!(
        numbered.claim("set.7Z.002", b"", None),
        Vec::<VolumeIndex>::new(),
        "a numbered name founds no set of the engine's"
    );
}

#[test]
fn the_object_is_an_engine_that_reads_in_order_and_keeps_nothing() {
    let object = crate::build(
        Kind::SevenZipEngine,
        ObjectId(4),
        weaver_graph::Budget::default(),
    )
    .unwrap();
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
    SevenZipEngine::new().start().run(
        Gate::whole(vec![store()]),
        &mut sink,
        weaver_graph::Budget::default(),
    );
    assert_eq!(
        checksums(&emits),
        vec![(0, crc(NOTES)), (1, crc(&second()))]
    );
}

// A read the owner failed is the owner's to explain, not a verdict on the
// format.
#[test]
fn an_owner_read_failure_is_no_format_refusal() {
    struct Failed {
        kind: io::ErrorKind,
    }
    impl Volumes for Failed {
        fn next(&self, after: Option<VolumeIndex>) -> io::Result<Option<VolumeIndex>> {
            Ok(after.is_none().then_some(VolumeIndex(0)))
        }
        fn len(&self, _: VolumeIndex) -> Option<u64> {
            Some(100)
        }
        fn read_at(&self, _: VolumeIndex, _: u64, _: &mut [u8]) -> io::Result<usize> {
            Err(self.kind.into())
        }
    }
    for kind in [
        io::ErrorKind::NotFound,
        io::ErrorKind::Interrupted,
        io::ErrorKind::Other,
    ] {
        let mut object = crate::build(
            Kind::SevenZipEngine,
            ObjectId(1),
            weaver_graph::Budget::default(),
        )
        .unwrap();
        let mut emits = Vec::new();
        object.engine_mut().unwrap().start().run(
            Arc::new(Failed { kind }),
            &mut |emit| {
                emits.push(emit);
                true
            },
            weaver_graph::Budget::default(),
        );
        assert!(emits.is_empty(), "{kind:?}: {emits:?}");
    }
}

fn wide() -> weaver_graph::Budget {
    weaver_graph::Budget {
        memory_bytes: Some(1 << 30),
        threads: 4,
        pool: None,
    }
}

fn many() -> (Vec<u8>, Vec<(String, Vec<u8>)>) {
    let members: Vec<(String, Vec<u8>)> = (0..12u8)
        .map(|i| {
            let bytes = payload(u64::from(i) + 20, 3_000 + usize::from(i) * 700).repeat(4);
            (format!("set/part-{i:02}.bin"), bytes)
        })
        .collect();
    let borrowed: Vec<(&str, &[u8])> = members
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    (lzma2(&borrowed), members)
}

// Each member's bytes joined from wherever they came, the order members were
// announced and closed in, and the checksums said.
#[derive(Debug, PartialEq, Eq)]
struct Heard {
    announced: Vec<(u32, String, Option<u64>)>,
    closed: Vec<u32>,
    bytes: std::collections::BTreeMap<u32, Vec<u8>>,
    checksums: Vec<(u32, u32)>,
}

fn heard(emits: &[Emit]) -> Heard {
    let mut heard = Heard {
        announced: Vec::new(),
        closed: Vec::new(),
        bytes: std::collections::BTreeMap::new(),
        checksums: checksums(emits),
    };
    for emit in emits {
        match emit {
            Emit::Member {
                member,
                name,
                length,
            } => heard.announced.push((member.0, name.clone(), *length)),
            Emit::Bytes {
                member,
                offset,
                bytes,
                ..
            } => {
                assert!(
                    heard.announced.iter().any(|(id, ..)| *id == member.0),
                    "bytes of a member not announced"
                );
                assert!(!heard.closed.contains(&member.0), "bytes after a close");
                let joined = heard.bytes.entry(member.0).or_default();
                assert_eq!(*offset, joined.len() as u64);
                joined.extend_from_slice(bytes);
            }
            Emit::MemberClosed(member) => heard.closed.push(member.0),
            Emit::Checksum { .. } => {}
            other => panic!("a clean run does not say {other:?}"),
        }
    }
    heard
}

fn run_with(volumes: Arc<dyn Volumes>, budget: weaver_graph::Budget) -> Vec<Emit> {
    let mut emits = Vec::new();
    SevenZipEngine::new().start().run(
        volumes,
        &mut |emit| {
            emits.push(emit);
            true
        },
        budget,
    );
    emits
}

#[test]
fn folders_decoded_side_by_side_say_what_one_by_one_says() {
    let (archive, members) = many();
    let one = heard(&run_with(
        Gate::whole(vec![archive.clone()]),
        weaver_graph::Budget::default(),
    ));
    let wide = heard(&run_with(Gate::whole(vec![archive]), wide()));
    assert_eq!(wide, one);
    // Announced and closed in archive order, every member whole.
    let ids: Vec<u32> = (0..13).collect();
    assert_eq!(
        wide.announced
            .iter()
            .map(|(id, ..)| *id)
            .collect::<Vec<_>>(),
        ids
    );
    assert_eq!(wide.closed, ids);
    for (id, (name, bytes)) in members.iter().enumerate() {
        assert_eq!(&wide.announced[id].1, name);
        assert_eq!(&wide.bytes[&(id as u32)], bytes);
    }
}

// The archive's bytes, with reads of one folder's packed bytes held until
// the test lets them go.
struct Held {
    bytes: Vec<u8>,
    held: ByteRange,
    open: Mutex<bool>,
    changed: Condvar,
}

impl Held {
    fn open(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

impl Volumes for Held {
    fn next(&self, after: Option<VolumeIndex>) -> io::Result<Option<VolumeIndex>> {
        Ok(after.is_none().then_some(VolumeIndex(0)))
    }

    fn len(&self, _: VolumeIndex) -> Option<u64> {
        Some(self.bytes.len() as u64)
    }

    fn read_at(&self, _: VolumeIndex, offset: u64, into: &mut [u8]) -> io::Result<usize> {
        let range = ByteRange::at(offset, into.len() as u64);
        if range.start < self.held.end && self.held.start < range.end {
            let mut open = self.open.lock().unwrap();
            while !*open {
                open = self.changed.wait(open).unwrap();
            }
        }
        let start = (offset as usize).min(self.bytes.len());
        let read = into.len().min(self.bytes.len() - start);
        into[..read].copy_from_slice(&self.bytes[start..start + read]);
        Ok(read)
    }
}

#[test]
fn a_later_member_decodes_while_an_earlier_one_waits_and_closes_after_it() {
    // The first member's folder cannot be read until the second member has
    // arrived whole, which a run decoding them one after another never gets
    // past; the second still closes only after the first.
    let first = payload(41, 9_000).repeat(3);
    let second = payload(42, 7_000).repeat(3);
    let archive = lzma2(&[("first.bin", &first), ("second.bin", &second)]);
    let parsed =
        sevenz_turbo::Archive::read(&mut Cursor::new(&archive), &Password::empty()).unwrap();
    // The link's body is the first folder; the first member's is the next.
    let pack = parsed.block_pack_streams(1)[0];
    let held = Arc::new(Held {
        bytes: archive,
        held: ByteRange::at(pack.offset, pack.size),
        open: Mutex::new(false),
        changed: Condvar::new(),
    });
    let mut emits = Vec::new();
    let mut second_got = 0usize;
    let gate = Arc::clone(&held);
    SevenZipEngine::new().start().run(
        Arc::clone(&held) as Arc<dyn Volumes>,
        &mut |emit| {
            if let Emit::Bytes { member, bytes, .. } = &emit
                && member.0 == 1
            {
                second_got += bytes.len();
                if second_got == second.len() {
                    gate.open();
                }
            }
            emits.push(emit);
            true
        },
        wide(),
    );
    let said = said(&emits);
    assert_eq!(
        said,
        vec![
            Said::Member(0, "first.bin".into(), Some(first.len() as u64)),
            Said::Member(1, "second.bin".into(), Some(second.len() as u64)),
            Said::Bytes(1, second),
            Said::Bytes(0, first),
            Said::Closed(0),
            Said::Closed(1),
            Said::Member(2, "deep/".into(), Some(0)),
            Said::Closed(2),
        ]
    );
}

#[test]
fn a_failed_folder_retracts_every_member_left_open_and_closes_only_those_before() {
    let (mut archive, _) = many();
    let parsed =
        sevenz_turbo::Archive::read(&mut Cursor::new(&archive), &Password::empty()).unwrap();
    // The link's body is the first folder, so member 4 is in the sixth.
    let damaged = 4;
    let pack = parsed.block_pack_streams(damaged + 1)[0];
    for at in [pack.offset + 2, pack.offset + pack.size / 2] {
        archive[at as usize] ^= 0xff;
    }
    let one = said(&run_with(
        Gate::whole(vec![archive.clone()]),
        weaver_graph::Budget::default(),
    ));
    let wide = said(&run_with(Gate::whole(vec![archive]), wide()));
    let closed = |said: &[Said]| {
        said.iter()
            .filter_map(|said| match said {
                Said::Closed(id) => Some(*id),
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    let before: Vec<u32> = (0..damaged as u32).collect();
    assert_eq!(closed(&one), before);
    assert_eq!(closed(&wide), before);
    assert_eq!(wide.last(), Some(&Said::Refuse(Why::Unrepairable)));
    // Every member announced and not closed is retracted, the damaged one
    // first, before the verdict.
    let announced: Vec<u32> = wide
        .iter()
        .filter_map(|said| match said {
            Said::Member(id, ..) => Some(*id),
            _ => None,
        })
        .collect();
    let retracted: Vec<u32> = wide
        .iter()
        .filter_map(|said| match said {
            Said::Retract(id) => Some(*id),
            _ => None,
        })
        .collect();
    let open: Vec<u32> = announced
        .into_iter()
        .filter(|id| !before.contains(id))
        .collect();
    assert_eq!(retracted, open);
    assert_eq!(retracted.first(), Some(&(damaged as u32)));
    assert_eq!(
        one[one.len() - 2..],
        [
            Said::Retract(damaged as u32),
            Said::Refuse(Why::Unrepairable)
        ]
    );
}

#[test]
fn a_sink_that_says_stop_ends_a_wide_run() {
    let (archive, _) = many();
    let mut emits = Vec::new();
    SevenZipEngine::new().start().run(
        Gate::whole(vec![archive]),
        &mut |emit: Emit| {
            let stop = matches!(emit, Emit::Bytes { .. });
            emits.push(emit);
            !stop
        },
        wide(),
    );
    let bytes = emits
        .iter()
        .filter(|emit| matches!(emit, Emit::Bytes { .. }))
        .count();
    assert_eq!(bytes, 1, "nothing after the stop: {emits:?}");
    assert!(matches!(emits.last(), Some(Emit::Bytes { .. })));
}

#[test]
fn an_earlier_member_closes_while_a_later_one_waits_for_its_bytes() {
    // The last member's folder cannot be read until the first member has
    // closed: a worker waiting on bytes still to come holds up no folder
    // before its own.
    let first = payload(43, 9_000).repeat(3);
    let second = payload(44, 7_000).repeat(3);
    let archive = lzma2(&[("first.bin", &first), ("second.bin", &second)]);
    let parsed =
        sevenz_turbo::Archive::read(&mut Cursor::new(&archive), &Password::empty()).unwrap();
    let pack = parsed.block_pack_streams(2)[0];
    let held = Arc::new(Held {
        bytes: archive,
        held: ByteRange::at(pack.offset, pack.size),
        open: Mutex::new(false),
        changed: Condvar::new(),
    });
    let mut emits = Vec::new();
    let gate = Arc::clone(&held);
    SevenZipEngine::new().start().run(
        Arc::clone(&held) as Arc<dyn Volumes>,
        &mut |emit| {
            if matches!(emit, Emit::MemberClosed(MemberId(0))) {
                gate.open();
            }
            emits.push(emit);
            true
        },
        wide(),
    );
    let said = said(&emits);
    let closed = said.iter().position(|said| *said == Said::Closed(0));
    let second_bytes = said
        .iter()
        .position(|said| matches!(said, Said::Bytes(1, _)));
    assert!(closed < second_bytes, "{closed:?} {second_bytes:?}");
    assert!(said.contains(&Said::Bytes(1, second)));
    assert!(said.contains(&Said::Bytes(0, first)));
    assert_eq!(said.last(), Some(&Said::Closed(2)));
}
