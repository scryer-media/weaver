// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod facts;

use std::collections::BTreeMap;

use weaver_graph::{
    ByteRange, Coverage, Delivery, Emit, Extent, Located, MemberId, Object, Placement, Provenance,
    VolumeIndex, Why,
};

use super::Tar;
use crate::testing::{Quiet, payload};

const BLOCK: usize = 512;

#[derive(Clone, Copy)]
enum Dialect {
    Ustar,
    Gnu,
    PrePosix,
}

// One header: name, size, type, and the magic of its dialect, its checksum
// summed over the rest.
fn header(name: &str, prefix: &str, size: u64, typeflag: u8, dialect: Dialect) -> Vec<u8> {
    let mut header = vec![0u8; BLOCK];
    let name = name.as_bytes();
    header[..name.len().min(100)].copy_from_slice(&name[..name.len().min(100)]);
    header[100..108].copy_from_slice(b"0000644\0");
    header[108..116].copy_from_slice(b"0001750\0");
    header[116..124].copy_from_slice(b"0001750\0");
    header[124..136].copy_from_slice(format!("{size:011o}\0").as_bytes());
    header[136..148].copy_from_slice(b"14712345670\0");
    header[156] = typeflag;
    match dialect {
        Dialect::Ustar => {
            header[257..265].copy_from_slice(b"ustar\x0000");
            header[345..345 + prefix.len()].copy_from_slice(prefix.as_bytes());
        }
        Dialect::Gnu => header[257..265].copy_from_slice(b"ustar  \0"),
        Dialect::PrePosix => {}
    }
    seal(&mut header);
    header
}

fn seal(header: &mut [u8]) {
    header[148..156].fill(b' ');
    let sum: u64 = header.iter().map(|&byte| u64::from(byte)).sum();
    header[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
}

// A tar written entry by entry, remembering where each header starts.
#[derive(Default)]
struct Writer {
    bytes: Vec<u8>,
    headers: Vec<usize>,
    data: Vec<usize>,
}

impl Writer {
    fn raw(&mut self, header: Vec<u8>, data: &[u8]) {
        self.bytes.extend_from_slice(&header);
        self.bytes.extend_from_slice(data);
        let padding = data.len().div_ceil(BLOCK) * BLOCK - data.len();
        self.bytes.resize(self.bytes.len() + padding, 0);
    }

    fn entry(&mut self, header: Vec<u8>, data: &[u8]) {
        self.headers.push(self.bytes.len());
        self.data.push(self.bytes.len() + BLOCK);
        self.raw(header, data);
    }

    fn file(&mut self, name: &str, data: &[u8]) {
        self.entry(
            header(name, "", data.len() as u64, b'0', Dialect::Ustar),
            data,
        );
    }

    fn pax(&mut self, path: &str, data: &[u8]) {
        let start = self.bytes.len();
        let record = pax_record("path", path);
        self.raw(
            header(
                "PaxHeaders/x",
                "",
                record.len() as u64,
                b'x',
                Dialect::Ustar,
            ),
            &record,
        );
        self.entry(
            header("short.bin", "", data.len() as u64, b'0', Dialect::Ustar),
            data,
        );
        *self.headers.last_mut().unwrap() = start;
    }

    fn gnu_long(&mut self, name: &str, data: &[u8]) {
        let start = self.bytes.len();
        let mut long = name.as_bytes().to_vec();
        long.push(0);
        self.raw(
            header("././@LongLink", "", long.len() as u64, b'L', Dialect::Gnu),
            &long,
        );
        self.entry(
            header(&name[..100], "", data.len() as u64, b'0', Dialect::Gnu),
            data,
        );
        *self.headers.last_mut().unwrap() = start;
    }

    // The two zero blocks, then a record's worth of zeros, as GNU tar pads.
    fn finish(mut self) -> Self {
        self.bytes.resize(self.bytes.len() + 2 * BLOCK, 0);
        let record = 10_240;
        self.bytes
            .resize(self.bytes.len().div_ceil(record) * record, 0);
        self
    }
}

fn pax_record(key: &str, value: &str) -> Vec<u8> {
    let body = format!(" {key}={value}\n");
    let mut length = body.len() + 1;
    while length.to_string().len() + body.len() != length {
        length = length.to_string().len() + body.len();
    }
    format!("{length}{body}").into_bytes()
}

fn long_path() -> String {
    format!("deep/{}/gamma.bin", "x".repeat(120))
}

fn long_name() -> String {
    format!("long/{}.bin", "y".repeat(110))
}

// Every dialect, a directory, an empty file, a link, and a size in base-256.
fn mixed() -> Writer {
    let mut writer = Writer::default();
    writer.file("alpha.bin", &payload(1, 1_000));
    writer.entry(header("deep/", "", 0, b'5', Dialect::Ustar), &[]);
    writer.entry(
        header("beta.bin", "deep", 5_000, b'0', Dialect::Ustar),
        &payload(2, 5_000),
    );
    writer.file("empty.bin", &[]);
    writer.pax(&long_path(), &payload(3, 700));
    writer.gnu_long(&long_name(), &payload(4, 513));
    writer.entry(header("link.bin", "", 0, b'2', Dialect::Ustar), &[]);
    writer.entry(
        header("old.bin", "", 300, 0, Dialect::PrePosix),
        &payload(5, 300),
    );
    let mut big = header("big.bin", "", 0, b'0', Dialect::Gnu);
    big[124..136].fill(0);
    big[124] = 0x80;
    big[134..136].copy_from_slice(&600u16.to_be_bytes());
    seal(&mut big);
    writer.entry(big, &payload(6, 600));
    writer.finish()
}

fn mixed_files() -> Vec<(String, Vec<u8>)> {
    vec![
        ("alpha.bin".into(), payload(1, 1_000)),
        ("deep/".into(), Vec::new()),
        ("deep/beta.bin".into(), payload(2, 5_000)),
        ("empty.bin".into(), Vec::new()),
        (long_path(), payload(3, 700)),
        (long_name(), payload(4, 513)),
        ("old.bin".into(), payload(5, 300)),
        ("big.bin".into(), payload(6, 600)),
    ]
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Got {
    name: String,
    length: Option<u64>,
    bytes: Vec<u8>,
    closed: bool,
}

// The tar's owner, in miniature: it delivers the volume's bytes, holds what
// the tar cannot place yet and hands it back once it can, and writes members
// the way a namespace does.
struct Owner {
    tar: Tar,
    volume: Vec<u8>,
    members: BTreeMap<u32, Got>,
    coverage: BTreeMap<u32, Coverage>,
    // Every emit but members and their bytes.
    said: Vec<Emit>,
    held: Vec<(ByteRange, Provenance, Vec<u8>)>,
    next: u32,
}

impl Owner {
    fn new(volume: Vec<u8>) -> Self {
        let mut owner = Self {
            tar: Tar::new(),
            volume,
            members: BTreeMap::new(),
            coverage: BTreeMap::new(),
            said: Vec::new(),
            held: Vec::new(),
            next: 0,
        };
        let mut out = Vec::new();
        owner
            .tar
            .sized(VolumeIndex(0), owner.volume.len() as u64, &mut out);
        owner.apply(out);
        owner
    }

    fn deliver(&mut self, range: ByteRange) {
        self.deliver_as(range, Provenance::Fresh, None);
    }

    fn deliver_as(&mut self, range: ByteRange, provenance: Provenance, bytes: Option<&[u8]>) {
        let bytes = bytes
            .unwrap_or(&self.volume[range.start as usize..range.end as usize])
            .to_vec();
        self.push(range, provenance, bytes);
        self.settle();
    }

    fn push(&mut self, range: ByteRange, provenance: Provenance, bytes: Vec<u8>) {
        let mut out = Vec::new();
        self.tar.deliver(
            Delivery {
                volume: VolumeIndex(0),
                offset: range.start,
                bytes: &bytes,
                provenance,
            },
            &mut |_, _| false,
            &mut out,
        );
        self.apply(out);
        let mut placements = Vec::new();
        self.tar.locate(VolumeIndex(0), range, &mut placements);
        let mut at = range.start;
        for placement in &placements {
            assert!(!placement.is_empty(), "no empty pieces");
            if let Placement::Held { range: held } = placement {
                assert_eq!(held.start, at, "held pieces are in volume order");
                let from = (held.start - range.start) as usize;
                let piece = bytes[from..from + held.len() as usize].to_vec();
                self.held.push((*held, provenance, piece));
            }
            at += placement.len();
        }
        assert_eq!(at, range.end, "locate accounts for every byte");
    }

    // Hand held bytes back until the tar places no more of them.
    fn settle(&mut self) {
        loop {
            let mut changed = false;
            for (range, provenance, bytes) in std::mem::take(&mut self.held) {
                let mut placements = Vec::new();
                self.tar.locate(VolumeIndex(0), range, &mut placements);
                if placements
                    .iter()
                    .all(|placement| matches!(placement, Placement::Held { .. }))
                {
                    self.held.push((range, provenance, bytes));
                } else {
                    changed = true;
                    self.push(range, provenance, bytes);
                }
            }
            if !changed {
                break;
            }
        }
    }

    fn pieces(&self, frame: usize) -> Vec<ByteRange> {
        let length = self.volume.len() as u64;
        (0..length)
            .step_by(frame)
            .map(|start| ByteRange::new(start, (start + frame as u64).min(length)))
            .collect()
    }

    fn close(&mut self) {
        let mut out = Vec::new();
        self.tar.volume_closed(VolumeIndex(0), &mut out);
        self.apply(out);
    }

    fn whole(&mut self, frame: usize) {
        for piece in self.pieces(frame) {
            self.deliver(piece);
        }
        self.close();
    }

    fn apply(&mut self, out: Vec<Emit>) {
        for emit in out {
            match emit {
                Emit::Member {
                    member,
                    name,
                    length,
                } => {
                    assert_eq!(member.0, self.next, "members are announced in id order");
                    self.next += 1;
                    self.members.insert(
                        member.0,
                        Got {
                            name,
                            length,
                            ..Got::default()
                        },
                    );
                    self.coverage.insert(member.0, Coverage::new());
                }
                Emit::Bytes {
                    member,
                    offset,
                    bytes,
                    provenance,
                } => {
                    let got = self.members.get_mut(&member.0).expect("announced first");
                    let end = offset as usize + bytes.len();
                    assert!(
                        got.length.is_some_and(|length| end as u64 <= length),
                        "bytes within the member"
                    );
                    if got.bytes.len() < end {
                        got.bytes.resize(end, 0);
                    }
                    got.bytes[offset as usize..end].copy_from_slice(&bytes);
                    self.coverage
                        .get_mut(&member.0)
                        .unwrap()
                        .write(ByteRange::at(offset, bytes.len() as u64), provenance);
                }
                Emit::MemberClosed(member) => {
                    let got = self.members.get_mut(&member.0).expect("announced first");
                    assert!(!got.closed, "closed once");
                    let length = got.length.unwrap();
                    assert!(self.coverage[&member.0].covered(ByteRange::new(0, length)));
                    got.closed = true;
                }
                Emit::Retract(member) => {
                    self.members.remove(&member.0);
                    self.coverage.remove(&member.0);
                    self.next = self.next.min(member.0);
                    self.said.push(Emit::Retract(member));
                }
                other => self.said.push(other),
            }
        }
    }

    fn files(&self) -> Vec<(String, Vec<u8>)> {
        self.members
            .values()
            .map(|got| {
                assert!(got.closed, "{} closed", got.name);
                assert_eq!(got.length, Some(got.bytes.len() as u64), "{}", got.name);
                (got.name.clone(), got.bytes.clone())
            })
            .collect()
    }

    // The volume rebuilt from the members and the tar's envelope.
    fn posted(&self) -> Option<Vec<u8>> {
        let mut read = |placement: &Placement, into: &mut Vec<u8>| -> bool {
            match placement {
                Placement::Member { member, range } => {
                    let Some(got) = self.members.get(&member.0) else {
                        return false;
                    };
                    into.extend_from_slice(&got.bytes[range.start as usize..range.end as usize]);
                    true
                }
                Placement::Envelope { envelope, range } => {
                    match self.tar.envelope(*envelope, *range) {
                        Some(bytes) => {
                            into.extend_from_slice(&bytes);
                            true
                        }
                        None => false,
                    }
                }
                Placement::Held { .. } => false,
            }
        };
        let mut into = Vec::new();
        self.tar
            .posted(
                VolumeIndex(0),
                ByteRange::new(0, self.volume.len() as u64),
                &mut read,
                &mut into,
            )
            .then_some(into)
    }
}

fn clean(owner: &Owner) {
    assert!(owner.said.is_empty(), "unexpected {:?}", owner.said);
    assert!(owner.held.is_empty(), "nothing left held");
}

#[test]
fn every_dialect_streams_whole_in_frames_of_any_size() {
    let archive = mixed().bytes;
    for frame in [archive.len(), 4096, 1] {
        let mut owner = Owner::new(archive.clone());
        owner.whole(frame);
        clean(&owner);
        assert_eq!(owner.files(), mixed_files(), "frames of {frame}");
        assert_eq!(owner.posted().as_deref(), Some(archive.as_slice()));
    }
}

// A GNU sparse entry whose map runs on into an extension block, and a
// multi-volume continuation: neither is a file, so both stay envelope and
// the walk finds the regular entry behind them.
#[test]
fn sparse_and_continued_entries_are_not_extracted() {
    let mut writer = Writer::default();
    let mut sparse = header("holes.bin", "", 1_024, b'S', Dialect::Gnu);
    sparse[482] = 1;
    seal(&mut sparse);
    let mut extension = vec![0u8; BLOCK];
    extension[..12].copy_from_slice(b"00000004000\0");
    extension[12..24].copy_from_slice(b"00000002000\0");
    extension[504] = 0;
    extension.extend_from_slice(&payload(7, 1_024));
    writer.raw(sparse, &extension);
    writer.raw(
        header("part.bin", "", 600, b'M', Dialect::Gnu),
        &payload(8, 600),
    );
    writer.file("after.bin", &payload(9, 800));
    let archive = writer.finish().bytes;
    let mut owner = Owner::new(archive.clone());
    owner.whole(4096);
    clean(&owner);
    assert_eq!(
        owner.files(),
        vec![("after.bin".to_owned(), payload(9, 800))]
    );
    assert_eq!(owner.posted().as_deref(), Some(archive.as_slice()));
}

#[test]
fn bytes_that_arrive_before_their_headers_are_held_until_the_walk_reaches_them() {
    let archive = mixed().bytes;
    let mut owner = Owner::new(archive.clone());
    let mut pieces = owner.pieces(4096);
    pieces.reverse();
    // The front last but one, so most of the archive waits on it.
    let front = pieces.pop().unwrap();
    for piece in pieces {
        owner.deliver(piece);
    }
    assert!(owner.members.is_empty());
    assert!(!owner.held.is_empty());
    owner.deliver(front);
    owner.close();
    clean(&owner);
    assert_eq!(owner.files(), mixed_files());
    assert_eq!(owner.posted().as_deref(), Some(archive.as_slice()));
}

// The header's own checksum speaks of the walk, not of the volume: the walk
// holds there, marks nothing, and reads on when a repair delivers the
// header again.
#[test]
fn a_header_whose_checksum_fails_holds_the_walk_until_it_is_repaired() {
    let writer = mixed();
    let at = writer.headers[2];
    let mut bad = writer.bytes.clone();
    bad[at + 3] ^= 0x20;
    let mut owner = Owner::new(writer.bytes.clone());
    let header = ByteRange::at(at as u64, BLOCK as u64);
    for piece in owner.pieces(4096) {
        let bytes = bad[piece.start as usize..piece.end as usize].to_vec();
        owner.deliver_as(piece, Provenance::Damaged, Some(&bytes));
    }
    owner.close();
    assert!(owner.said.is_empty(), "{:?}", owner.said);
    assert_eq!(owner.members.len(), 2, "only the entries before it");
    let repaired = writer.bytes[at..at + BLOCK].to_vec();
    owner.deliver_as(header, Provenance::Repaired, Some(&repaired));
    clean(&owner);
    assert_eq!(owner.files(), mixed_files());
}

#[test]
fn a_repair_of_a_header_already_read_retracts_from_that_entry_on() {
    let writer = mixed();
    // A wrong name that still sums right: the walk believes it.
    let at = writer.headers[2];
    let mut wrong = writer.bytes.clone();
    wrong[at + 1] = b'x';
    seal(&mut wrong[at..at + BLOCK]);
    let mut owner = Owner::new(wrong);
    owner.whole(4096);
    clean(&owner);
    assert_eq!(owner.members[&2].name, "deep/bxta.bin");
    let header = ByteRange::at(at as u64, BLOCK as u64);
    let repaired = writer.bytes[at..at + BLOCK].to_vec();
    owner.deliver_as(header, Provenance::Repaired, Some(&repaired));
    let retracted: Vec<u32> = owner
        .said
        .iter()
        .map(|emit| match emit {
            Emit::Retract(member) => member.0,
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert_eq!(
        retracted,
        (2..mixed_files().len() as u32).collect::<Vec<_>>()
    );
    assert_eq!(owner.members[&2].name, "deep/beta.bin");
    // The retracted members' data comes again, as the owner re-reads it.
    owner.said.clear();
    owner.volume = writer.bytes.clone();
    owner.whole(4096);
    clean(&owner);
    assert_eq!(owner.files(), mixed_files());
}

// The missing tail of a volume that ends inside an entry speaks of the walk,
// not of the volume: it marks nothing, and the archive is refused once
// nothing more will arrive.
#[test]
fn a_volume_that_closes_inside_an_entry_refuses_the_archive_once_quiet() {
    let writer = mixed();
    let data = writer.data[2];
    let cut = data + 1_234;
    let mut owner = Owner::new(writer.bytes[..cut].to_vec());
    owner.whole(4096);
    assert!(owner.said.is_empty(), "{:?}", owner.said);
    let beta = &owner.members[&2];
    assert_eq!(beta.name, "deep/beta.bin");
    assert!(!beta.closed);
    assert!(owner.members[&0].closed);
    let mut out = Vec::new();
    owner.tar.quiet(&Quiet(Vec::new()), &mut out);
    assert_eq!(out, [Emit::Refuse(Why::Unrepairable)]);
}

#[test]
fn an_archive_that_ends_at_an_entry_boundary_without_its_end_blocks_is_whole() {
    let mut writer = Writer::default();
    writer.file("alpha.bin", &payload(1, 1_000));
    writer.file("beta.bin", &payload(2, 512));
    let mut owner = Owner::new(writer.bytes.clone());
    owner.whole(1000);
    clean(&owner);
    assert_eq!(owner.files().len(), 2);
}

#[test]
fn a_header_that_will_never_arrive_refuses_the_archive() {
    let writer = mixed();
    let at = writer.headers[3] as u64;
    let mut owner = Owner::new(writer.bytes.clone());
    owner.deliver(ByteRange::new(0, at));
    let mut out = Vec::new();
    owner
        .tar
        .volume_lost(VolumeIndex(0), ByteRange::at(at, 100), &mut out);
    assert_eq!(out, [Emit::Refuse(Why::Unrepairable)]);
}

#[test]
fn a_members_bytes_lie_where_its_entry_put_them() {
    let writer = mixed();
    let mut owner = Owner::new(writer.bytes.clone());
    owner.whole(writer.bytes.len());
    let mut extents = Vec::new();
    owner.tar.extent(
        weaver_graph::MemberId(2),
        ByteRange::new(100, 200),
        &mut extents,
    );
    let data = writer.data[2] as u64;
    assert_eq!(
        extents,
        [weaver_graph::Extent {
            volume: VolumeIndex(0),
            range: ByteRange::new(data + 100, data + 200)
        }]
    );
    assert!(owner.tar.reads_in_order());
}

// A member's bytes are raw runs of the archive, so a seek into a member the
// walk has not reached is not located yet, the same seek once its header is
// read lies where the entry put it, and only a range past the entry's data
// is never located.
#[test]
fn a_seek_into_a_member_the_walk_has_not_reached_is_located_once_it_has() {
    let writer = mixed();
    let mut owner = Owner::new(writer.bytes.clone());
    owner.deliver(ByteRange::new(0, writer.headers[2] as u64));
    let beta = MemberId(2);
    let mut extents = Vec::new();
    assert_eq!(
        owner
            .tar
            .extent(beta, ByteRange::new(100, 200), &mut extents),
        Located::NotYet
    );
    assert!(extents.is_empty());
    owner.whole(4096);
    clean(&owner);
    let data = writer.data[2] as u64;
    assert_eq!(
        owner
            .tar
            .extent(beta, ByteRange::new(100, 200), &mut extents),
        Located::Whole
    );
    assert_eq!(
        extents,
        [Extent {
            volume: VolumeIndex(0),
            range: ByteRange::new(data + 100, data + 200)
        }]
    );
    extents.clear();
    assert_eq!(
        owner
            .tar
            .extent(beta, ByteRange::new(5_000, 5_100), &mut extents),
        Located::Never
    );
    assert!(extents.is_empty());
}

// An archive under a compressed stream is the stream's decoded member, and a
// decoded member never lies in the volumes.
#[test]
fn a_seek_under_a_compressed_stream_is_unseekable() {
    let stream = crate::stream::GzipStream::new();
    let mut extents = Vec::new();
    assert_eq!(
        stream.extent(MemberId(0), ByteRange::new(0, 100), &mut extents),
        Located::Never
    );
    assert!(extents.is_empty());
}
