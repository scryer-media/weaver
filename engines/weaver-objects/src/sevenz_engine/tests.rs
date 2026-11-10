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

// A folder whose decoder needs more memory than the run was granted is
// refused as too large, with the sizes the decoder states, before any of
// its bytes are said.
#[test]
fn a_folder_larger_than_the_memory_granted_is_refused_too_large() {
    let first = payload(7, 2_000).repeat(10);
    let archive = lzma2(&[("first.bin", &first)]);
    let mut emits = Vec::new();
    let mut sink = |emit| {
        emits.push(emit);
        true
    };
    SevenZipEngine::new().start().run(
        Gate::whole(vec![archive]),
        &mut sink,
        weaver_graph::Budget {
            memory_bytes: Some(1 << 10),
            threads: 1,
            ..Default::default()
        },
    );
    let said = said(&emits);
    assert!(
        !said.iter().any(|said| matches!(said, Said::Bytes(..))),
        "{said:?}"
    );
    let refused: Vec<&Said> = said
        .iter()
        .filter(|said| matches!(said, Said::Refuse(_)))
        .collect();
    assert!(
        matches!(
            refused.as_slice(),
            [Said::Refuse(Why::TooLarge {
                needed: Some(needed),
                allowed: Some(1024),
            })] if *needed > 1024
        ),
        "{said:?}"
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

// The grant the lender tests hold a run to: room in RAM for a folder's
// input buffer and coder state, none for its dictionary or model.
const GRANT: u64 = 2 << 20;
// A dictionary over the grant, and a member larger than it, so the decoder
// does not clamp the dictionary to the member.
const DICTIONARY: u32 = 4 << 20;

fn big() -> Vec<u8> {
    payload(9, 4_096).repeat(3 * DICTIONARY as usize / 4_096)
}

// One LZMA2 folder of `bytes` whose dictionary is over the grant: one solid
// stream, or, `threaded`, runs a dictionary long that a threaded decoder
// decodes side by side.
fn lzma2_over_grant(bytes: &[u8], threaded: bool) -> Vec<u8> {
    let mut options = if threaded {
        Lzma2Options::from_level_mt(1, 4, u64::from(DICTIONARY))
    } else {
        Lzma2Options::from_level(1)
    };
    options.set_dictionary_size(DICTIONARY);
    archive(
        vec![EncoderConfiguration::from(options)],
        &[("big.bin", bytes)],
    )
}

// One PPMd folder of `bytes` whose model is over the grant.
fn ppmd_over_grant(bytes: &[u8]) -> Vec<u8> {
    let options = sevenz_turbo::encoder_options::PpmdOptions::from_order_memory_size(6, DICTIONARY);
    archive(
        vec![EncoderConfiguration::from(options)],
        &[("big.bin", bytes)],
    )
}

// A run whose memory and window are both the grant: it decodes a folder on
// one thread whatever it is handed, since a threaded LZMA2 decode needs room
// in flight (sevenz-turbo plans one only with 32 MiB beyond the decoder's
// own state).
fn grant(threads: usize) -> weaver_graph::Budget {
    weaver_graph::Budget {
        memory_bytes: Some(GRANT),
        window_bytes: Some(GRANT),
        threads,
        ..Default::default()
    }
}

// A run whose memory holds the dictionary but not its window: the
// dictionary stays in RAM, for the owner reserved the run its memory.
fn room_for_dictionary(threads: usize) -> weaver_graph::Budget {
    weaver_graph::Budget {
        memory_bytes: Some(64 << 20),
        ..grant(threads)
    }
}

// A dictionary a decode of on one thread holds in 13 MiB of RAM, whose
// threaded LZMA2 workers may each take a buffer of up to four of it.
const WORKERS_DICTIONARY: u32 = 12 << 20;
// Each run of the stream, which a threaded worker holds whole: over the
// memory `room_to_thread` grants, within the four dictionaries a worker may
// take. The smallest the decoder's rules allow: threading needs 32 MiB past
// the dictionary and the decoder's 1 MiB of state, and a worker's buffer is
// at most four dictionaries.
const WORKERS_RUN: u64 = 47 << 20;

// One LZMA2 folder of two runs of `WORKERS_RUN`, which a threaded decoder
// decodes side by side; built once.
fn lzma2_workers() -> (Vec<u8>, Vec<u8>) {
    static BUILT: std::sync::OnceLock<(Vec<u8>, Vec<u8>)> = std::sync::OnceLock::new();
    BUILT
        .get_or_init(|| {
            let bytes = payload(11, 4_096).repeat(2 * WORKERS_RUN as usize / 4_096);
            let mut options = Lzma2Options::from_level_mt(1, 4, WORKERS_RUN);
            options.set_dictionary_size(WORKERS_DICTIONARY);
            let archive = archive(
                vec![EncoderConfiguration::from(options)],
                &[("big.bin", &bytes)],
            );
            (archive, bytes)
        })
        .clone()
}

// A run whose memory holds the dictionary and room to thread beside it (32
// MiB past the dictionary and the decoder's state), but no worker's run:
// each worker borrows its own buffer.
fn room_to_thread(threads: usize) -> weaver_graph::Budget {
    weaver_graph::Budget {
        memory_bytes: Some(46 << 20),
        ..grant(threads)
    }
}

// A run over `archive` under `budget`, handed `lender`.
fn lent_run(
    archive: &[u8],
    budget: weaver_graph::Budget,
    lender: Option<&Arc<crate::lent::testing::TestLender>>,
) -> Vec<Said> {
    lent_run_of(&mut SevenZipEngine::new(), archive, budget, lender)
}

// As `lent_run`, of `engine`, with whatever password it was offered.
fn lent_run_of(
    engine: &mut SevenZipEngine,
    archive: &[u8],
    budget: weaver_graph::Budget,
    lender: Option<&Arc<crate::lent::testing::TestLender>>,
) -> Vec<Said> {
    let budget = match lender {
        Some(lender) => lender.lending(budget),
        None => budget,
    };
    let mut emits = Vec::new();
    engine.start().run(
        Gate::whole(vec![archive.to_vec()]),
        &mut |emit| {
            emits.push(emit);
            true
        },
        budget,
    );
    said(&emits)
}

// The most windows the decoder says a run on `threads` threads under
// `budget` can have lent at once.
fn lent_windows(archive: &[u8], budget: &weaver_graph::Budget, threads: u32) -> usize {
    decoder_memory(archive, budget, threads).lent_windows
}

fn decoder_memory(
    archive: &[u8],
    budget: &weaver_graph::Budget,
    threads: u32,
) -> sevenz_turbo::DecoderMemory {
    parsed(archive)
        .decoder_memory(&crate::lent::sevenz(budget), threads)
        .unwrap()
}

fn parsed(archive: &[u8]) -> sevenz_turbo::Archive {
    sevenz_turbo::Archive::read(&mut Cursor::new(archive), &Password::empty()).unwrap()
}

// What a decode of `archive` holds in RAM on one thread, nothing lent.
fn in_ram(archive: &[u8]) -> u64 {
    crate::lent::sevenz_in_ram(&parsed(archive), &crate::lent::sevenz(&grant(1))).unwrap()
}

fn refusals(said: &[Said]) -> Vec<Why> {
    said.iter()
        .filter_map(|said| match said {
            Said::Refuse(why) => Some(*why),
            _ => None,
        })
        .collect()
}

// A run handed a lender borrows each buffer its memory cannot hold, and
// decodes through them what a run with room for them on the heap decodes,
// although every lent byte held garbage first. Every window asked for is
// over the memory, no more are out at once than the decoder says a run on
// as many threads can have, and every one is given back.
//
// With no memory to decode side by side, the folder's dictionary is the one
// window asked for, on one thread or four.
#[test]
fn a_7z_lzma2_dictionary_over_the_grant_is_lent_and_given_back() {
    use crate::lent::testing::TestLender;
    let bytes = big();
    let solid = lzma2_over_grant(&bytes, false);
    let runs = lzma2_over_grant(&bytes, true);
    for (name, archive, budget) in [
        ("one stream, one thread", &solid, grant(1)),
        ("one stream, four threads", &solid, grant(4)),
        ("three runs, one thread", &runs, grant(1)),
        ("three runs, no memory to thread", &runs, grant(4)),
    ] {
        let threads = budget.threads();
        let lender = TestLender::new();
        let said = lent_run(archive, budget.clone(), Some(&lender));
        assert_eq!(
            said,
            run(&mut SevenZipEngine::new(), vec![archive.clone()]),
            "{name}"
        );
        assert!(said.contains(&Said::Bytes(0, bytes.clone())), "{name}");
        assert_eq!(lender.asked(), [DICTIONARY as usize], "{name}");
        let most = lent_windows(archive, &lender.lending(budget), threads as u32);
        assert!(
            lender.peak() <= most,
            "{name}: {} lent at once, {most} said",
            lender.peak()
        );
        assert_eq!(lender.live(), 0, "{name}");
    }
}

// With memory to thread but none for a worker's buffer, a stream of three
// runs is decoded by workers that each borrow their own: more than one is
// asked for, each over the memory. How many are out at once is the
// workers' timing, so only its bound is held.
#[test]
fn a_threaded_7z_decode_borrows_each_worker_buffer_its_memory_cannot_hold() {
    use crate::lent::testing::TestLender;
    let (archive, bytes) = lzma2_workers();
    let budget = room_to_thread(4);
    let lender = TestLender::new();
    let said = lent_run(&archive, budget.clone(), Some(&lender));
    assert_eq!(said, run(&mut SevenZipEngine::new(), vec![archive.clone()]));
    assert!(said.contains(&Said::Bytes(0, bytes)));
    let asked = lender.asked();
    assert!(asked.len() > 1, "{asked:?}");
    let memory = budget.memory_bytes.unwrap();
    assert!(asked.iter().all(|&ask| ask as u64 > memory), "{asked:?}");
    let most = lent_windows(&archive, &lender.lending(budget), 4);
    assert!(
        lender.peak() <= most,
        "{} lent at once, {most} said",
        lender.peak()
    );
    assert_eq!(lender.live(), 0);
}

// A dictionary over the run's window that its memory holds stays in RAM,
// as it did before a run was handed a lender: nothing is asked for, on one
// thread or threaded, and the folder decodes in its grant.
#[test]
fn a_7z_dictionary_the_runs_memory_holds_stays_in_ram() {
    use crate::lent::testing::TestLender;
    let bytes = big();
    for (name, archive, threads) in [
        ("one stream", lzma2_over_grant(&bytes, false), 1),
        (
            "three runs, four threads",
            lzma2_over_grant(&bytes, true),
            4,
        ),
    ] {
        let budget = room_for_dictionary(threads);
        assert!(u64::from(DICTIONARY) > budget.window_or(u64::MAX));
        let lender = TestLender::new();
        let said = lent_run(&archive, budget, Some(&lender));
        assert!(said.contains(&Said::Bytes(0, bytes.clone())), "{name}");
        assert_eq!(refusals(&said), [], "{name}");
        assert_eq!(lender.asked(), Vec::<usize>::new(), "{name}");
    }
}

// A threaded decode whose workers would borrow more at once than its
// lender has room for runs on fewer threads, down to one, before it is
// refused: on one thread this dictionary fits the memory, so a lender with
// no room is asked for nothing and the folder decodes. With room for two
// workers' buffers it runs on two, and every window asked is lent.
#[test]
fn a_threaded_7z_decode_runs_on_the_threads_its_lenders_room_holds() {
    use crate::lent::testing::TestLender;
    let (archive, bytes) = lzma2_workers();
    let budget = room_to_thread(4);
    let two = decoder_memory(&archive, &TestLender::new().lending(budget.clone()), 2).lent_bytes;
    let three = decoder_memory(&archive, &TestLender::new().lending(budget.clone()), 3).lent_bytes;
    assert!(two > 0 && three > two, "{two} {three}");
    for (name, room, threads) in [("no room", 0, 1), ("room for two", two, 2)] {
        let lender = TestLender::with_room(room);
        let lending = lender.lending(budget.clone());
        let limits = crate::lent::sevenz(&lending);
        assert_eq!(
            crate::lent::sevenz_threads(&parsed(&archive), &limits, &lending, 4),
            threads,
            "{name}"
        );
        let said = lent_run(&archive, budget.clone(), Some(&lender));
        assert_eq!(refusals(&said), [], "{name}: {said:?}");
        assert!(said.contains(&Said::Bytes(0, bytes.clone())), "{name}");
        assert_eq!(lender.live(), 0, "{name}");
        if room == 0 {
            assert_eq!(lender.asked(), Vec::<usize>::new(), "{name}");
        }
    }
}

// Without a lender the same grant refuses the folder with its sizes, as it
// always did.
#[test]
fn a_7z_dictionary_over_the_grant_without_a_lender_is_refused() {
    let bytes = big();
    for (threaded, threads) in [(false, 1), (true, 4)] {
        let said = lent_run(&lzma2_over_grant(&bytes, threaded), grant(threads), None);
        assert!(
            matches!(
                refusals(&said).as_slice(),
                [Why::TooLarge {
                    needed: Some(needed),
                    allowed: Some(GRANT),
                }] if *needed > GRANT
            ),
            "threaded {threaded}: {said:?}"
        );
        assert!(!said.iter().any(|said| matches!(said, Said::Bytes(..))));
    }
}

// A lender's refusal is the member's verdict: a filesystem's refusal asks
// for the decode in RAM, with all it holds there on one thread, which the
// owner gives or refuses by its policy; any other refusal stands. Handed
// that memory and no lender, the run decodes: one start in RAM is enough.
#[test]
fn a_7z_window_the_lender_refuses_says_why() {
    use crate::lent::testing::TestLender;
    let bytes = big();
    let archive = lzma2_over_grant(&bytes, false);
    let full = TestLender::refusing(io::ErrorKind::StorageFull);
    let said = lent_run(&archive, grant(1), Some(&full));
    let asked = full.asked();
    assert_eq!(asked.len(), 1, "{asked:?}");
    let needed = in_ram(&archive);
    assert!(needed > asked[0] as u64, "{needed} {asked:?}");
    assert_eq!(
        refusals(&said),
        [Why::TooLarge {
            needed: Some(needed),
            allowed: None,
        }],
        "{said:?}"
    );
    assert!(!said.iter().any(|said| matches!(said, Said::Bytes(..))));
    let in_ram = weaver_graph::Budget {
        memory_bytes: Some(needed),
        window_bytes: Some(needed),
        ..grant(1)
    };
    let said = lent_run(&archive, in_ram, None);
    assert_eq!(refusals(&said), [], "{said:?}");
    assert!(said.contains(&Said::Bytes(0, bytes)));

    let denied = TestLender::refusing(io::ErrorKind::PermissionDenied);
    let said = lent_run(&archive, grant(1), Some(&denied));
    assert_eq!(refusals(&said), [Why::Unsupported], "{said:?}");
    assert!(!said.iter().any(|said| matches!(said, Said::Bytes(..))));
}

// A threaded decode whose lender lends the first window and refuses the
// rest says a refusal of one of the windows refused, with its size, and
// gives back the window it was lent.
#[test]
fn a_threaded_7z_decode_whose_lender_runs_out_gives_back_what_it_was_lent() {
    use crate::lent::testing::TestLender;
    let (archive, _) = lzma2_workers();
    let full = TestLender::refusing_after(1, io::ErrorKind::StorageFull);
    let said = lent_run(&archive, room_to_thread(4), Some(&full));
    let asked = full.asked();
    assert!(asked.len() > 1, "{asked:?}");
    let refused = refusals(&said);
    let least = in_ram(&archive);
    assert!(
        matches!(
            refused.as_slice(),
            [Why::TooLarge {
                needed: Some(needed),
                allowed: None,
            }] if *needed >= least
                && (*needed == least || asked[1..].contains(&(*needed as usize)))
        ),
        "{said:?}, asked {asked:?}, {least} in RAM"
    );
    assert!(!said.iter().any(|said| matches!(said, Said::Closed(0))));
    assert_eq!(full.live(), 0);
}

// A window over the lender's ceiling is not asked for: the decoder refuses
// it with its size against the ceiling.
#[test]
fn a_7z_dictionary_over_the_lenders_ceiling_is_refused_unasked() {
    use crate::lent::testing::TestLender;
    let ceiling = 3 << 20;
    let low = TestLender::up_to(ceiling);
    let said = lent_run(&lzma2_over_grant(&big(), false), grant(1), Some(&low));
    assert!(
        matches!(
            refusals(&said).as_slice(),
            [Why::TooLarge {
                needed: Some(needed),
                allowed: Some(allowed),
            }] if *needed > ceiling && *allowed == ceiling
        ),
        "{said:?}"
    );
    assert_eq!(low.asked(), Vec::<usize>::new());
}

// A PPMd model over the grant is lent under the same rule as a dictionary,
// decodes what a run with room for it on the heap decodes, and is given
// back; without a lender the folder is refused with its sizes.
//
// The archive is two PPMd folders, the link's nine bytes and the member,
// each declaring the same model. A model is the size its coder declares,
// not clamped to the folder's output, so each folder borrows its own, one
// at a time.
#[test]
fn a_7z_ppmd_model_over_the_grant_is_lent_as_a_dictionary_is() {
    use crate::lent::testing::TestLender;
    let bytes = payload(10, 4_096).repeat(256);
    let archive = ppmd_over_grant(&bytes);
    let parsed =
        sevenz_turbo::Archive::read(&mut Cursor::new(&archive), &Password::empty()).unwrap();
    let models: Vec<&[u8]> = parsed
        .blocks
        .iter()
        .flat_map(|block| &block.coders)
        .map(|coder| coder.properties())
        .collect();
    let declared = [6, 0, 0, 0x40, 0];
    assert_eq!(models, [&declared[..], &declared[..]]);

    let lender = TestLender::new();
    let said = lent_run(&archive, grant(1), Some(&lender));
    assert_eq!(said, run(&mut SevenZipEngine::new(), vec![archive.clone()]));
    assert!(said.contains(&Said::Bytes(0, bytes)));
    assert_eq!(lender.asked(), [DICTIONARY as usize; 2]);
    assert!(lender.peak() <= lent_windows(&archive, &lender.lending(grant(1)), 1));
    assert_eq!(lender.live(), 0);

    let said = lent_run(&archive, grant(1), None);
    assert!(
        matches!(
            refusals(&said).as_slice(),
            [Why::TooLarge {
                needed: Some(needed),
                allowed: Some(GRANT),
            }] if *needed > GRANT
        ),
        "{said:?}"
    );
}

// An encrypted folder whose dictionary the lender has no room for is the
// scratch's refusal, as a plain one is: never a wrong password, never
// damage, under the right password.
#[test]
fn an_encrypted_7z_the_lender_refuses_is_refused_by_the_scratch() {
    use crate::lent::testing::TestLender;
    let mut options = Lzma2Options::from_level(1);
    options.set_dictionary_size(DICTIONARY);
    let archive = archive(
        vec![
            EncoderConfiguration::from(AesEncoderOptions::new(Password::new(PASSWORD))),
            EncoderConfiguration::from(options),
        ],
        &[("big.bin", &big())],
    );
    let mut engine = named(&["locked.7z"]);
    engine.offer_password(PASSWORD, &mut Vec::new());
    // Room for the folder's coders, the cipher's too, but not its
    // dictionary.
    let budget = weaver_graph::Budget {
        memory_bytes: Some(3 << 20),
        window_bytes: Some(3 << 20),
        ..grant(1)
    };
    let full = TestLender::refusing(io::ErrorKind::StorageFull);
    let said = lent_run_of(&mut engine, &archive, budget, Some(&full));
    assert!(!full.asked().is_empty(), "{said:?}");
    assert!(!said.contains(&Said::NeedsPassword), "{said:?}");
    assert!(
        matches!(
            refusals(&said).as_slice(),
            [Why::TooLarge {
                needed: Some(needed),
                allowed: None,
            }] if *needed > 3 << 20
        ),
        "{said:?}"
    );
}
