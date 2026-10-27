// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod facts;

use std::io::{self, Write};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use weaver_graph::{Delivery, Provenance, Volumes, Why};

use super::*;
use crate::testing::payload;

fn first_payload() -> Vec<u8> {
    payload(1, 3_000).repeat(8)
}

fn second_payload() -> Vec<u8> {
    payload(2, 2_000).repeat(5)
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn deflate(bytes: &[u8]) -> Vec<u8> {
    let mut encoder =
        flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn bzip2_at(bytes: &[u8], level: u32) -> Vec<u8> {
    let mut encoder = ::bzip2::write::BzEncoder::new(Vec::new(), ::bzip2::Compression::new(level));
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn bzip2(bytes: &[u8]) -> Vec<u8> {
    bzip2_at(bytes, 9)
}

fn zstd_at(bytes: &[u8], window_log: u32) -> Vec<u8> {
    let mut encoder = ::zstd::stream::Encoder::new(Vec::new(), 3).unwrap();
    encoder.window_log(window_log).unwrap();
    // As the zstd tool writes by default: a frame's checksum is what tells
    // a flipped byte of its literals.
    encoder.include_checksum(true).unwrap();
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn zstd(bytes: &[u8]) -> Vec<u8> {
    zstd_at(bytes, 20)
}

fn brotli_at(bytes: &[u8], window_bits: u32) -> Vec<u8> {
    let mut encoder = ::brotli::CompressorWriter::new(Vec::new(), 4096, 5, window_bits);
    encoder.write_all(bytes).unwrap();
    encoder.into_inner()
}

fn brotli(bytes: &[u8]) -> Vec<u8> {
    brotli_at(bytes, 18)
}

fn xz_at(bytes: &[u8], dictionary: u32) -> Vec<u8> {
    let properties = lzma_turbo::LzmaEncProps::new().with_dict_size(dictionary);
    let mut encoder = lzma_turbo::XzWriter::new(Vec::new(), &properties).unwrap();
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn xz(bytes: &[u8]) -> Vec<u8> {
    xz_at(bytes, 1 << 20)
}

// A volume in memory whose bytes the test hands out a piece at a time. A
// read past what was handed out waits, and says where, so the test can hand
// out the next piece only once the run has used up the last one.
struct Gate {
    bytes: Vec<u8>,
    state: Mutex<State>,
    changed: Condvar,
}

#[derive(Default)]
struct State {
    released: usize,
    closed: bool,
    waiting: Option<u64>,
    done: bool,
}

impl Gate {
    fn new(bytes: Vec<u8>) -> Arc<Self> {
        Arc::new(Self {
            bytes,
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
        })
    }

    fn release(&self, to: usize) {
        let mut state = self.state.lock().unwrap();
        state.released = to.min(self.bytes.len());
        self.changed.notify_all();
    }

    fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.released = self.bytes.len();
        state.closed = true;
        self.changed.notify_all();
    }

    fn finish(&self) {
        self.state.lock().unwrap().done = true;
        self.changed.notify_all();
    }

    // Until the run waits past everything released, or has ended.
    fn await_reader(&self) {
        let mut state = self.state.lock().unwrap();
        while !state.done
            && !state
                .waiting
                .is_some_and(|waiting| waiting >= state.released as u64)
        {
            state = self.changed.wait(state).unwrap();
        }
    }
}

impl Volumes for Gate {
    fn next(&self, after: Option<VolumeIndex>) -> std::io::Result<Option<VolumeIndex>> {
        let next = after.map_or(0, |after| after.0 as usize + 1);
        Ok((next < 1).then_some(VolumeIndex(next as u32)))
    }

    fn len(&self, _volume: VolumeIndex) -> Option<u64> {
        let state = self.state.lock().unwrap();
        state.closed.then_some(self.bytes.len() as u64)
    }

    fn read_at(&self, _volume: VolumeIndex, offset: u64, into: &mut [u8]) -> io::Result<usize> {
        let mut state = self.state.lock().unwrap();
        while offset >= state.released as u64 && !state.closed {
            state.waiting = Some(offset);
            self.changed.notify_all();
            state = self.changed.wait(state).unwrap();
        }
        state.waiting = None;
        let start = (offset as usize).min(state.released);
        let read = into.len().min(state.released - start);
        into[..read].copy_from_slice(&self.bytes[start..start + read]);
        Ok(read)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Said {
    Member(String),
    Bytes(Vec<u8>),
    Closed,
    Retract,
    Damaged(ByteRange),
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
            } => {
                assert_eq!((*member, *length), (MemberId(0), None));
                out.push(Said::Member(name.clone()));
            }
            Emit::Bytes { offset, bytes, .. } => match out.last_mut() {
                Some(Said::Bytes(joined)) => {
                    assert_eq!(*offset, joined.len() as u64);
                    joined.extend_from_slice(bytes);
                }
                _ => {
                    assert_eq!(*offset, 0);
                    out.push(Said::Bytes(bytes.to_vec()));
                }
            },
            Emit::MemberClosed(_) => out.push(Said::Closed),
            Emit::Retract(_) => out.push(Said::Retract),
            Emit::Damaged { volume, range } => {
                assert_eq!(*volume, VolumeIndex(0));
                out.push(Said::Damaged(*range));
            }
            Emit::Refuse(why) => out.push(Said::Refuse(*why)),
            other => panic!("a stream run does not say {other:?}"),
        }
    }
    out
}

fn stream<D: Decoder>(name: &str) -> Stream<D> {
    let mut stream = Stream::<D>::new();
    stream.bound(VolumeIndex(0), name, &[], None);
    stream
}

// A run over `bytes`, the test handing them out as `hand_out` says, then
// closing the volume.
fn decode<D: Decoder>(
    stream: &mut Stream<D>,
    bytes: Vec<u8>,
    hand_out: impl FnOnce(&Gate),
) -> Vec<Said> {
    let gate = Gate::new(bytes);
    let extraction = stream.start();
    let runner = {
        let gate = Arc::clone(&gate);
        thread::spawn(move || {
            let mut emits = Vec::new();
            let volumes: Arc<dyn Volumes> = gate.clone();
            extraction.run(
                volumes,
                &mut |emit| {
                    emits.push(emit);
                    true
                },
                weaver_graph::Budget::default(),
            );
            gate.finish();
            emits
        })
    };
    hand_out(&gate);
    gate.close();
    said(&runner.join().unwrap())
}

fn whole<D: Decoder>(stream: &mut Stream<D>, bytes: Vec<u8>) -> Vec<Said> {
    decode(stream, bytes, |_| {})
}

fn growing<D: Decoder>(stream: &mut Stream<D>, bytes: Vec<u8>, piece: usize) -> Vec<Said> {
    let len = bytes.len();
    decode(stream, bytes, |gate| {
        let mut released = 0;
        while released < len {
            released = (released + piece).min(len);
            gate.release(released);
            gate.await_reader();
        }
    })
}

fn decoded(name: &str, bytes: Vec<u8>) -> Vec<Said> {
    vec![
        Said::Member(name.to_owned()),
        Said::Bytes(bytes),
        Said::Closed,
    ]
}

fn each_piece<D: Decoder>(volume: &str, member: &str, bytes: &[u8], plain: &[u8]) {
    let mut stream = stream::<D>(volume);
    let expected = decoded(member, plain.to_vec());
    assert_eq!(
        whole(&mut stream, bytes.to_vec()),
        expected,
        "{volume} whole"
    );
    for piece in [1, 4096] {
        assert_eq!(
            growing(&mut stream, bytes.to_vec(), piece),
            expected,
            "{volume} in {piece}-byte pieces"
        );
    }
}

#[test]
fn each_stream_decodes_one_member_through_a_growing_volume() {
    let plain = first_payload();
    each_piece::<Xz>("alpha.bin.xz", "alpha.bin", &xz(&plain), &plain);
    each_piece::<Gzip>("alpha.bin.gz", "alpha.bin", &gzip(&plain), &plain);
    each_piece::<Bzip2>("alpha.bin.bz2", "alpha.bin", &bzip2(&plain), &plain);
    each_piece::<Zstd>("alpha.bin.zst", "alpha.bin", &zstd(&plain), &plain);
    each_piece::<Brotli>("alpha.bin.br", "alpha.bin", &brotli(&plain), &plain);
    each_piece::<Deflate>("alpha.bin", "alpha.bin", &deflate(&plain), &plain);
}

fn concatenated<D: Decoder>(encode: fn(&[u8]) -> Vec<u8>) {
    let (first, second) = (first_payload(), second_payload());
    let mut bytes = encode(&first);
    bytes.extend_from_slice(&encode(&second));
    let joined = [first, second].concat();
    assert_eq!(
        whole(&mut stream::<D>("alpha.tar.gz"), bytes),
        decoded("alpha.tar", joined)
    );
}

#[test]
fn concatenated_streams_decode_through_as_one_member() {
    concatenated::<Gzip>(gzip);
    concatenated::<Xz>(xz);
    concatenated::<Bzip2>(bzip2);
    concatenated::<Zstd>(zstd);
}

// What a run said after its member's bytes, whatever of them it got out.
fn ending(said: &[Said]) -> &[Said] {
    let start = said
        .iter()
        .position(|said| !matches!(said, Said::Member(_) | Said::Bytes(_)))
        .unwrap_or(said.len());
    &said[start..]
}

fn refused(said: &[Said]) {
    refused_as("", said);
}

fn refused_as(case: &str, said: &[Said]) {
    assert_eq!(
        ending(said),
        [Said::Retract, Said::Refuse(Why::Unrepairable)],
        "{case} {said:?}"
    );
}

#[test]
fn a_corrupt_stream_is_refused() {
    let plain = first_payload();
    // A byte of each stream's compressed body, flipped where it breaks what
    // the format can tell: gzip, bzip2 and zstd by their checks, the others
    // by codes no encoder writes.
    let corrupt = |mut bytes: Vec<u8>, at: usize| {
        bytes[at] ^= 0xFF;
        bytes
    };
    let gz = gzip(&plain);
    let at = gz.len() - 12;
    refused_as("Gzip", &whole(&mut stream::<Gzip>("a.gz"), corrupt(gz, at)));
    let bz = bzip2(&plain);
    let at = bz.len() / 2;
    refused_as(
        "Bzip2",
        &whole(&mut stream::<Bzip2>("a.bz2"), corrupt(bz, at)),
    );
    let zs = zstd(&plain);
    let at = zs.len() / 2;
    refused_as(
        "Zstd",
        &whole(&mut stream::<Zstd>("a.zst"), corrupt(zs, at)),
    );
    let x = xz(&plain);
    refused_as("Xz", &whole(&mut stream::<Xz>("a.xz"), corrupt(x, 13)));
    // A block type no encoder writes.
    let mut raw = deflate(&plain);
    raw[0] |= 0x06;
    refused_as("Deflate", &whole(&mut stream::<Deflate>("a"), raw));
    // Brotli checks nothing, so a flipped literal decodes; its window field
    // set to the large-window code a standard stream never carries does not.
    let mut br = brotli(&plain);
    br[0] = (br[0] & 0x80) | 0x11;
    refused_as("Brotli", &whole(&mut stream::<Brotli>("a.br"), br));
}

fn truncated<D: Decoder>(name: &str, mut bytes: Vec<u8>) {
    bytes.truncate(bytes.len() / 2);
    refused(&whole(&mut stream::<D>(name), bytes));
}

#[test]
fn a_stream_cut_short_at_a_closed_volume_is_refused() {
    let plain = first_payload();
    truncated::<Xz>("a.xz", xz(&plain));
    truncated::<Gzip>("a.gz", gzip(&plain));
    truncated::<Bzip2>("a.bz2", bzip2(&plain));
    truncated::<Zstd>("a.zst", zstd(&plain));
    truncated::<Brotli>("a.br", brotli(&plain));
    truncated::<Deflate>("a", deflate(&plain));
}

#[test]
fn a_stream_short_of_its_end_waits_while_its_volume_is_open() {
    let plain = first_payload();
    let bytes = gzip(&plain);
    let half = bytes.len() / 2;
    let said = decode(&mut stream::<Gzip>("a.gz"), bytes, |gate| {
        gate.release(half);
        gate.await_reader();
    });
    assert_eq!(said, decoded("a", plain));
}

// Where the check of the one block of the xz stream ending at `end` lies:
// right before the stream's index, whose size the footer gives.
fn xz_check_byte(bytes: &[u8], end: usize) -> usize {
    let backward = u32::from_le_bytes(bytes[end - 8..end - 4].try_into().unwrap());
    let index = end - 12 - (backward as usize + 1) * 4;
    index - 1
}

#[test]
fn a_failed_xz_check_names_the_stream_it_failed_in() {
    let first = xz(&first_payload());
    let second = xz(&second_payload());
    let mut bytes = [first.clone(), second.clone()].concat();
    let at = xz_check_byte(&bytes, bytes.len());
    bytes[at] ^= 0x01;
    let said = whole(&mut stream::<Xz>("a.xz"), bytes);
    assert_eq!(
        ending(&said),
        [
            Said::Retract,
            Said::Damaged(ByteRange::at(first.len() as u64, second.len() as u64)),
        ]
    );
    // The first stream decoded whole before the second failed.
    let Said::Bytes(out) = &said[1] else {
        panic!("{said:?}");
    };
    assert!(out.len() >= first_payload().len());
}

#[test]
fn the_member_is_named_for_the_volume_without_its_compression_extension() {
    let names = [
        ("subs.srt.gz", "subs.srt"),
        ("a.tar.xz", "a.tar"),
        ("a.tgz", "a.tar"),
        ("a.TXZ", "a.tar"),
        ("a.tbz2", "a.tar"),
        ("a.tbz", "a.tar"),
        ("a.tzst", "a.tar"),
        ("a.bin.bz2", "a.bin"),
        ("a.bin.zst", "a.bin"),
        ("a.bin.br", "a.bin"),
        ("a.bin", "a.bin"),
        ("noext", "noext"),
        (".gz", ".gz"),
    ];
    for (volume, member) in names {
        assert_eq!(member_name(volume), member, "{volume}");
    }
}

// The reservation, read from the stream's first bytes as they arrive one at
// a time.
fn reserve<D: Decoder>(bytes: &[u8]) -> u64 {
    let mut stream = Stream::<D>::new();
    let mut out = Vec::new();
    for (offset, byte) in bytes.iter().enumerate() {
        stream.deliver(
            Delivery {
                volume: VolumeIndex(0),
                offset: offset as u64,
                bytes: std::slice::from_ref(byte),
                provenance: Provenance::Fresh,
            },
            &mut |_, _| false,
            &mut out,
        );
    }
    assert!(out.is_empty());
    D::window(&stream.first, &stream.budget)
}

#[test]
fn each_stream_reserves_the_window_its_first_bytes_declare() {
    let plain = first_payload();
    let lzma2 = |property| lzma_turbo::Lzma2Reader::<io::Empty>::memory_required(property).unwrap();
    // 1 MiB is dictionary property 16, 4 MiB property 20.
    assert_eq!(reserve::<Xz>(&xz_at(&plain, 1 << 20)), lzma2(16));
    assert_eq!(reserve::<Xz>(&xz_at(&plain, 4 << 20)), lzma2(20));
    // A zstd frame reserves the decoder's own limit, whatever it declares.
    assert_eq!(reserve::<Zstd>(&zstd_at(&plain, 20)), 1 << 27);
    assert_eq!(reserve::<Zstd>(&zstd_at(&plain, 23)), 1 << 27);
    assert_eq!(reserve::<Brotli>(&brotli_at(&plain, 18)), 1 << 18);
    assert_eq!(reserve::<Brotli>(&brotli_at(&plain, 22)), 1 << 22);
    assert_eq!(reserve::<Bzip2>(&bzip2_at(&plain, 1)), 100_000);
    assert_eq!(reserve::<Bzip2>(&bzip2_at(&plain, 9)), 900_000);
    assert_eq!(reserve::<Gzip>(&gzip(&plain)), 32 << 10);
    assert_eq!(reserve::<Deflate>(&deflate(&plain)), 32 << 10);
}

#[test]
fn a_stream_whose_front_has_not_arrived_reserves_its_largest_window() {
    assert_eq!(reserve::<Xz>(&[]), 128 << 20);
    assert_eq!(reserve::<Zstd>(&[]), 1 << 27);
    assert_eq!(reserve::<Brotli>(&[]), 1 << 30);
    assert_eq!(reserve::<Bzip2>(&[]), 900_000);
}

// A frame of one raw byte whose header asks for a window of 1 << log.
fn zstd_window(log: u8) -> Vec<u8> {
    let mut frame = vec![0x28, 0xB5, 0x2F, 0xFD, 0x00, (log - 10) << 3];
    // The last block, raw, one byte long.
    frame.extend_from_slice(&[0x09, 0x00, 0x00, b'z']);
    frame
}

// The decoder keeps its default window limit: a frame that asks for more is
// refused, however little it holds.
#[test]
fn a_zstd_frame_wider_than_the_decoders_limit_is_refused() {
    let mut stream = stream::<Zstd>("a.zst");
    assert_eq!(
        whole(&mut stream, zstd_window(27)),
        decoded("a", b"z".to_vec())
    );
    refused(&whole(&mut stream, zstd_window(28)));
}

#[test]
fn every_byte_of_the_volume_is_its_envelope() {
    let stream = stream::<Gzip>("a.gz");
    let mut placements = Vec::new();
    stream.locate(VolumeIndex(0), ByteRange::new(10, 900), &mut placements);
    assert_eq!(
        placements,
        [Placement::Envelope {
            envelope: 0,
            range: ByteRange::new(10, 900)
        }]
    );
    assert!(stream.reads_in_order());
    assert!(stream.engine().is_some());
}

// The lengths of the byte emits a run says over `bytes`, handed out
// `piece` bytes at a time.
fn emitted<D: Decoder>(name: &str, bytes: Vec<u8>, piece: usize) -> Vec<usize> {
    let gate = Gate::new(bytes);
    let extraction = stream::<D>(name).start();
    let runner = {
        let gate = Arc::clone(&gate);
        thread::spawn(move || {
            let mut lens = Vec::new();
            let volumes: Arc<dyn Volumes> = gate.clone();
            extraction.run(
                volumes,
                &mut |emit| {
                    if let Emit::Bytes { bytes, .. } = emit {
                        lens.push(bytes.len());
                    }
                    true
                },
                weaver_graph::Budget::default(),
            );
            gate.finish();
            lens
        })
    };
    let len = gate.bytes.len();
    let mut released = 0;
    while released < len {
        released = (released + piece).min(len);
        gate.release(released);
        gate.await_reader();
    }
    gate.close();
    runner.join().unwrap()
}

// Every emit is a step of the owner's, so a run hands its output on a
// whole chunk at a time, however little each read of its decoder gives:
// gzip's reads give half a chunk, brotli's a sixteenth.
#[test]
fn a_run_hands_its_output_on_in_whole_chunks() {
    let plain = payload(7, 4 * super::run::CHUNK + 1_000);
    let chunks = plain.len().div_ceil(super::run::CHUNK);
    type Case = (
        &'static str,
        Vec<u8>,
        fn(&str, Vec<u8>, usize) -> Vec<usize>,
    );
    let cases: [Case; 6] = [
        ("a.gz", gzip(&plain), emitted::<Gzip>),
        ("a.deflate", deflate(&plain), emitted::<Deflate>),
        ("a.bz2", bzip2(&plain), emitted::<Bzip2>),
        ("a.zst", zstd(&plain), emitted::<Zstd>),
        ("a.br", brotli(&plain), emitted::<Brotli>),
        ("a.xz", xz(&plain), emitted::<Xz>),
    ];
    for (name, bytes, emitted) in cases {
        for piece in [bytes.len(), 4096] {
            let lens = emitted(name, bytes.clone(), piece);
            assert_eq!(lens.len(), chunks, "{name} in {piece}-byte pieces");
            assert_eq!(lens.iter().sum::<usize>(), plain.len(), "{name}");
            assert!(
                lens[..chunks - 1]
                    .iter()
                    .all(|len| *len == super::run::CHUNK),
                "{name} in {piece}-byte pieces: {lens:?}"
            );
        }
    }
}

// A closed volume in memory whose bytes at `lost` nothing will bring: a read
// ends short where they begin and fails on them, as the owner's does, and
// what the run read on past is kept.
struct Holed {
    bytes: Vec<u8>,
    lost: ByteRange,
    bridged: Mutex<Vec<weaver_graph::Lost>>,
}

impl Volumes for Holed {
    fn next(&self, after: Option<VolumeIndex>) -> io::Result<Option<VolumeIndex>> {
        Ok(after.is_none().then_some(VolumeIndex(0)))
    }

    fn len(&self, _volume: VolumeIndex) -> Option<u64> {
        Some(self.bytes.len() as u64)
    }

    fn read_at(&self, volume: VolumeIndex, offset: u64, into: &mut [u8]) -> io::Result<usize> {
        let end = (offset + into.len() as u64).min(self.bytes.len() as u64);
        let want = ByteRange::new(offset.min(end), end);
        if self.lost.contains(offset) {
            let range = ByteRange::new(offset, self.lost.end.min(want.end));
            let lost = weaver_graph::Lost { volume, range };
            return Err(io::Error::new(io::ErrorKind::NotFound, lost));
        }
        let end = if offset < self.lost.start {
            want.end.min(self.lost.start)
        } else {
            want.end
        };
        let read = (end - want.start) as usize;
        into[..read].copy_from_slice(&self.bytes[want.start as usize..end as usize]);
        Ok(read)
    }

    fn bridged(&self, lost: weaver_graph::Lost) {
        self.bridged.lock().unwrap().push(lost);
    }
}

fn holed<D: Decoder>(name: &str, bytes: Vec<u8>, lost: ByteRange) -> (Vec<Emit>, Arc<Holed>) {
    let volumes = Arc::new(Holed {
        bytes,
        lost,
        bridged: Mutex::default(),
    });
    let mut emits = Vec::new();
    let reader: Arc<dyn Volumes> = volumes.clone();
    stream::<D>(name).start().run(
        reader,
        &mut |emit| {
            emits.push(emit);
            true
        },
        weaver_graph::Budget::default(),
    );
    (emits, volumes)
}

// Pieces joined as they are lose nothing beside a lost span: the member
// holds it as a gap marked damaged at the same offsets, every other byte as
// posted, and closes at its full length for a recovery set to rebuild.
#[test]
fn a_join_reads_on_past_a_lost_span_and_marks_it_damaged() {
    let plain = payload(9, 3 * super::run::CHUNK);
    let lost = ByteRange::new(70_000, 75_000);
    let (emits, volumes) = holed::<Pieces>("lantern.bin.001", plain.clone(), lost);
    let mut joined = vec![1u8; plain.len()];
    let mut damaged = Vec::new();
    for emit in &emits {
        if let Emit::Bytes {
            offset,
            bytes,
            provenance,
            ..
        } = emit
        {
            let range = ByteRange::at(*offset, bytes.len() as u64);
            joined[range.start as usize..range.end as usize].copy_from_slice(bytes);
            if *provenance == Provenance::Damaged {
                damaged.push(range);
            }
        }
    }
    assert_eq!(
        damaged.iter().map(|range| range.len()).sum::<u64>(),
        lost.len()
    );
    assert!(damaged.iter().all(|range| lost.contains(range.start)));
    assert_eq!(
        &joined[..lost.start as usize],
        &plain[..lost.start as usize]
    );
    assert_eq!(&joined[lost.end as usize..], &plain[lost.end as usize..]);
    assert_eq!(emits.last(), Some(&Emit::MemberClosed(MemberId(0))));
    assert_eq!(
        *volumes.bridged.lock().unwrap(),
        vec![weaver_graph::Lost {
            volume: VolumeIndex(0),
            range: lost
        }]
    );
}

// A decoder's output is not its input's bytes: it reads on past nothing
// lost, and never closes its member.
#[test]
fn a_decoder_reads_on_past_no_lost_span() {
    let bytes = gzip(&payload(9, 3 * super::run::CHUNK));
    let lost = ByteRange::new(100, 200);
    let (emits, volumes) = holed::<Gzip>("a.gz", bytes, lost);
    assert!(!emits.contains(&Emit::MemberClosed(MemberId(0))));
    assert!(volumes.bridged.lock().unwrap().is_empty());
}

// A set known by names alone claims only its own pieces: another set's
// piece is not one of them, whichever of its pieces founded the stream.
#[test]
fn a_split_set_claims_only_pieces_of_its_own_stem() {
    let plain = b"plain bytes, no signature";
    let mut founded_by_first = stream::<Pieces>("alpha.bin.001");
    assert_eq!(
        founded_by_first.claim("alpha.bin.002", plain, Some(100)),
        Some(VolumeIndex(1))
    );
    assert_eq!(
        founded_by_first.claim("beta.bin.002", plain, Some(100)),
        None
    );
    let mut founded_by_later = Stream::<Pieces>::new();
    founded_by_later.bound(VolumeIndex(2), "alpha.bin.003", &[], None);
    assert_eq!(
        founded_by_later.claim("alpha.bin.001", plain, Some(100)),
        Some(VolumeIndex(0))
    );
    assert_eq!(
        founded_by_later.claim("beta.bin.001", plain, Some(100)),
        None
    );
    assert_eq!(
        Stream::<Pieces>::new().claim("beta.bin.002", plain, Some(100)),
        Some(VolumeIndex(1))
    );
}

fn xz_multiblock(bytes: &[u8]) -> Vec<u8> {
    let properties = lzma_turbo::LzmaEncProps::xz_preset(0, false).unwrap();
    let mut encoder = lzma_turbo::XzWriter::new(Vec::new(), &properties).unwrap();
    encoder.set_block_size(u64::from(properties.dict_size()));
    encoder.set_threads(2);
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

// A run over a volume closed before the run opens it, within `budget`.
fn closed_run<D: Decoder>(
    stream: &mut Stream<D>,
    bytes: Vec<u8>,
    budget: weaver_graph::Budget,
) -> Vec<Said> {
    let gate = Gate::new(bytes);
    gate.close();
    let volumes: Arc<dyn Volumes> = gate;
    let mut emits = Vec::new();
    stream.start().run(
        volumes,
        &mut |emit| {
            emits.push(emit);
            true
        },
        budget,
    );
    said(&emits)
}

fn budget(memory_bytes: u64, threads: usize) -> weaver_graph::Budget {
    weaver_graph::Budget {
        memory_bytes: Some(memory_bytes),
        threads,
        ..Default::default()
    }
}

// A closed xz of several blocks decodes alike on one thread and on four,
// and on four it is the run's budget that holds the decoder: one too small
// for any block's buffers fails it, where the front-to-back decoder on one
// thread holds only the window it reserved.
#[test]
fn a_closed_multiblock_xz_decodes_its_blocks_side_by_side_within_the_budget() {
    let plain: Vec<u8> = (0..(1024 * 1024))
        .map(|index| (index % 251) as u8)
        .collect();
    let bytes = xz_multiblock(&plain);
    let one = closed_run(
        &mut stream::<Xz>("ember.bin.xz"),
        bytes.clone(),
        budget(u64::MAX, 1),
    );
    let four = closed_run(
        &mut stream::<Xz>("ember.bin.xz"),
        bytes.clone(),
        budget(u64::MAX, 4),
    );
    assert_eq!(one, decoded("ember.bin", plain.clone()));
    assert_eq!(four, one);

    let held = closed_run(
        &mut stream::<Xz>("ember.bin.xz"),
        bytes.clone(),
        budget(1, 4),
    );
    assert_eq!(
        ending(&held),
        [Said::Retract, Said::Refuse(Why::Unsupported)]
    );
    let sequential = closed_run(&mut stream::<Xz>("ember.bin.xz"), bytes, budget(1, 1));
    assert_eq!(sequential, decoded("ember.bin", plain));
}

// A run granted less than the 128 MiB an xz decoder may hold sizes its
// window to the grant, whether the first bytes declare more or say nothing
// yet; one granted more still holds to the 128 MiB. A dictionary the grant
// cannot hold is refused rather than decoded past it.
#[test]
fn an_xz_window_is_no_larger_than_the_memory_granted() {
    let window = |bytes: &[u8], granted: u64| Xz::window(bytes, &budget(granted, 1));
    let declared = xz_at(&first_payload(), 1 << 20);
    let needs = Xz::window(&declared, &weaver_graph::Budget::default());
    assert_eq!(window(&[], 64 << 20), 64 << 20);
    assert_eq!(window(&declared, 64 << 20), needs);
    assert_eq!(window(&declared, needs / 2), needs / 2);
    assert_eq!(window(&[], 256 << 20), 128 << 20);

    // The block is smaller than its dictionary, and the decoder needs only
    // the block; a grant below even that is refused.
    let held = budget(4 << 10, 1);
    let mut short = stream::<Xz>("ember.bin.xz").within(held.clone());
    let run = closed_run(&mut short, declared.clone(), held);
    assert_eq!(ending(&run)[0], Said::Retract, "{run:?}");
    assert!(matches!(ending(&run)[1], Said::Refuse(_)), "{run:?}");

    let granted = budget(64 << 20, 1);
    let mut room = stream::<Xz>("ember.bin.xz").within(granted.clone());
    assert_eq!(
        closed_run(&mut room, declared, granted),
        decoded("ember.bin", first_payload())
    );
}

// A volume still growing when the run opens it is read from the front: its
// tail is never waited on, so the grant's threads go unused.
#[test]
fn a_growing_multiblock_xz_is_read_from_the_front() {
    let plain: Vec<u8> = (0..(1024 * 1024))
        .map(|index| (index % 251) as u8)
        .collect();
    let bytes = xz_multiblock(&plain);
    let gate = Gate::new(bytes.clone());
    let extraction = stream::<Xz>("ember.bin.xz").start();
    let runner = {
        let gate = Arc::clone(&gate);
        thread::spawn(move || {
            let mut emits = Vec::new();
            let volumes: Arc<dyn Volumes> = gate.clone();
            extraction.run(
                volumes,
                &mut |emit| {
                    emits.push(emit);
                    true
                },
                budget(1, 4),
            );
            gate.finish();
            emits
        })
    };
    // The run reads the front before the volume grows any further.
    gate.await_reader();
    gate.release(bytes.len());
    gate.close();
    assert_eq!(said(&runner.join().unwrap()), decoded("ember.bin", plain));
}
