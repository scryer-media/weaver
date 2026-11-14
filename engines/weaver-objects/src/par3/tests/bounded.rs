// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A repair reads the volumes through par3-rs's packet scanner a piece at a
// time and keeps each data and recovery block only as where it lies. What
// it reads them as is what a scan of each span's whole bytes read them as:
// the same sets, with the same recovery blocks and carried data in the same
// order. A span whose recovery a scan of its whole bytes could not keep is
// read and used, no read of a volume is longer than a piece, and the kept
// fronts are scanned again only once they change.

use par3_rs::ingest::PayloadRef;
use par3_rs::{DataPacket, Fingerprint, InputSetId, Packet, PacketBody, Par3Set, ScanLimits};
use weaver_graph::recovery::{MemberAccess, RepairOutcome};
use weaver_graph::{
    Budget, ByteRange, Delivery, MemberId, Object, ObjectId, Provenance, Recovery, VolumeIndex,
};

use super::super::volumes::{self, PIECE, Volumes};
use super::super::{MAX_SETS, Par3};
use crate::testing::{Job, Store, par3_set, payload};

const OWN: ObjectId = ObjectId(11);
const BLOCK: u64 = 1024;

// What a read of the volumes found of one set: its description, and its
// recovery blocks and carried data with their bytes.
#[derive(Debug, PartialEq)]
struct Inventory {
    described: String,
    recovery: Vec<(u64, Fingerprint, bool, Vec<u8>)>,
    data: Vec<(u64, Vec<u8>)>,
}

fn described(set: &Par3Set) -> String {
    let matrices: Vec<Fingerprint> = set.matrix_packets().iter().map(Packet::hash).collect();
    format!(
        "{:?}",
        (
            set.input_set_id(),
            set.root_hash(),
            set.block_size(),
            set.block_count(),
            set.galois_field(),
            set.files(),
            set.directories(),
            set.block_checksums(),
            matrices,
            set.recovery_block_checksums(),
        )
    )
}

// The sets as a scan of each span's whole bytes read them.
fn whole(store: &Store, spans: &[(VolumeIndex, ByteRange)], limits: &ScanLimits) -> Vec<Inventory> {
    let mut packets = Vec::new();
    for (volume, span) in spans {
        let bytes = &store.volumes[volume][span.start as usize..span.end as usize];
        if let Ok(found) = par3_rs::scan_packets_with_limits(bytes, limits) {
            packets.extend(found.into_iter().map(|(_, packet)| packet));
        }
    }
    let sets = Par3Set::from_packets(packets).unwrap_or_default();
    if sets.len() > MAX_SETS {
        return Vec::new();
    }
    sets.iter()
        .map(|set| Inventory {
            described: described(set),
            recovery: set
                .recovery_blocks()
                .iter()
                .map(|block| {
                    (
                        block.index(),
                        block.matrix_hash(),
                        block.matrix_present(),
                        block.data().to_vec(),
                    )
                })
                .collect(),
            data: set
                .data_packets()
                .iter()
                .map(|data| (data.block_index, data.data.clone()))
                .collect(),
        })
        .collect()
}

fn bytes(volumes: &Volumes, payload: &PayloadRef, access: &mut dyn MemberAccess) -> Vec<u8> {
    let mut out = vec![0; payload.len() as usize];
    assert!(volumes.read(payload, 0, &mut out, access));
    out
}

// The sets as the repair reads them, a piece at a time.
fn pieces(
    store: &mut Store,
    spans: &[(VolumeIndex, ByteRange)],
    limits: ScanLimits,
) -> Vec<Inventory> {
    let read = volumes::read(spans.to_vec(), limits, MAX_SETS, store);
    read.sets
        .iter()
        .zip(&read.payloads)
        .map(|(set, payloads)| Inventory {
            described: described(set),
            recovery: payloads
                .recovery
                .iter()
                .map(|row| {
                    let bytes = bytes(&read.volumes, &row.payload, store);
                    (row.index, row.matrix, row.present, bytes)
                })
                .collect(),
            data: payloads
                .data
                .iter()
                .map(|(index, payload)| (*index, bytes(&read.volumes, payload, store)))
                .collect(),
        })
        .collect()
}

struct Seeded(u64);

impl Seeded {
    fn next(&mut self, below: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % below
    }
}

fn input_set(index: &[u8]) -> InputSetId {
    par3_rs::scan_packets(index).unwrap()[0].1.input_set_id()
}

fn data_packet(set: InputSetId, block_index: u64, data: Vec<u8>) -> Vec<u8> {
    Packet::new(set, PacketBody::Data(DataPacket { block_index, data })).to_bytes()
}

// A post of one or two sets with volumes whole, cut by a hole, damaged or
// posted twice, some carrying input blocks as data, and the spans of them
// that arrived.
fn post(seed: u64) -> (Store, Vec<(VolumeIndex, ByteRange)>) {
    let mut rng = Seeded(seed);
    let mut volumes: Vec<Vec<u8>> = Vec::new();
    let sets = 1 + u64::from(rng.next(3) == 0);
    for set in 0..sets {
        let one = payload(seed * 7 + set, 1_000 + rng.next(8_000) as usize);
        let two = payload(seed * 7 + set + 3, 500 + rng.next(4_000) as usize);
        let names = [format!("one{set}.bin"), format!("two{set}.bin")];
        let files: Vec<(&str, &[u8])> = if rng.next(2) == 0 {
            vec![(names[0].as_str(), one.as_slice())]
        } else {
            vec![
                (names[0].as_str(), one.as_slice()),
                (names[1].as_str(), two.as_slice()),
            ]
        };
        let made = par3_set(&format!("set{set}"), &files, BLOCK, 2 + rng.next(5));
        let mut made: Vec<Vec<u8>> = made.into_iter().map(|(_, bytes)| bytes).collect();
        if rng.next(2) == 0 {
            let id = input_set(&made[0]);
            let carried = data_packet(id, 0, one[..BLOCK as usize].to_vec());
            made[0].extend_from_slice(&carried);
            made[0].extend_from_slice(&carried);
            made[0].extend_from_slice(&data_packet(id, 1, payload(seed, 300)));
            made[0].extend_from_slice(&data_packet(id, 1, payload(seed + 1, 200)));
            made[0].extend_from_slice(&data_packet(id, 2, payload(seed + 2, BLOCK as usize + 9)));
        }
        volumes.extend(made);
    }
    if rng.next(4) == 0 {
        let again = volumes[rng.next(volumes.len() as u64) as usize].clone();
        volumes.push(again);
    }
    let mut store = Store::default();
    let mut spans = Vec::new();
    for (at, mut bytes) in volumes.into_iter().enumerate() {
        let volume = VolumeIndex(at as u32);
        let len = bytes.len() as u64;
        match rng.next(4) {
            0 => {
                let cut = rng.next(len);
                let gap = 1 + rng.next(BLOCK);
                spans.push((volume, ByteRange::new(0, cut)));
                if cut + gap < len {
                    spans.push((volume, ByteRange::new(cut + gap, len)));
                }
            }
            1 => {
                let at = rng.next(len) as usize;
                bytes[at] ^= 0x5A;
                spans.push((volume, ByteRange::new(0, len)));
            }
            _ => spans.push((volume, ByteRange::new(0, len))),
        }
        store.volumes.insert(volume, bytes);
    }
    (store, spans)
}

// Read a piece at a time, the volumes give the same sets, recovery and
// carried data, in the same order, as each span's bytes scanned whole.
#[test]
fn a_piecewise_read_finds_what_a_whole_scan_found() {
    let (mut found, mut recovery, mut data) = (0, 0, 0);
    for seed in 0..48 {
        let (mut store, spans) = post(seed);
        let before = whole(&store, &spans, &ScanLimits::default());
        let after = pieces(&mut store, &spans, ScanLimits::default());
        assert_eq!(after, before, "seed {seed}");
        found += after.len();
        recovery += after.iter().map(|set| set.recovery.len()).sum::<usize>();
        data += after.iter().map(|set| set.data.len()).sum::<usize>();
    }
    assert!(
        found > 0 && recovery > 0 && data > 0,
        "{found} {recovery} {data}"
    );
}

// A span whose recovery is more than a scan of its whole bytes may keep is
// left out by that scan; read a piece at a time it is used whole. The limit
// here stands for the 4 GiB one at a size a test can post.
#[test]
fn a_span_past_the_retained_limit_is_used() {
    let body = payload(41, 40_000);
    let files = par3_set("big", &[("big.bin", &body)], 4096, 8);
    let mut store = Store::default();
    let mut spans = Vec::new();
    for (at, (_, bytes)) in files.iter().enumerate() {
        let volume = VolumeIndex(at as u32);
        spans.push((volume, ByteRange::new(0, bytes.len() as u64)));
        store.volumes.insert(volume, bytes.clone());
    }
    let mut limits = ScanLimits::default();
    limits.max_retained_bytes = files[0].1.len() as u64 + 1024;
    let unlimited = whole(&store, &spans, &ScanLimits::default());
    assert_eq!(unlimited[0].recovery.len(), 8);
    let limited = whole(&store, &spans, &limits);
    assert!(limited.iter().all(|set| set.recovery.is_empty()));
    assert_eq!(pieces(&mut store, &spans, limits), unlimited);
}

// The volumes as the namespace answers a repair, with the longest read of
// one measured.
struct Measured {
    store: Store,
    longest: u64,
}

impl MemberAccess for Measured {
    fn read(&mut self, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.store.read(member, range, into)
    }
    fn write_repaired(&mut self, member: MemberId, offset: u64, bytes: &[u8]) {
        self.store.write_repaired(member, offset, bytes);
    }
    fn length(&self, member: MemberId) -> Option<u64> {
        self.store.length(member)
    }
    fn arrived(&self, member: MemberId, range: ByteRange, out: &mut Vec<ByteRange>) {
        self.store.arrived(member, range, out);
    }
    fn read_volume(&mut self, volume: VolumeIndex, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.longest = self.longest.max(range.len());
        self.store.read_volume(volume, range, into)
    }
}

// A volume longer than a piece is read a piece at a time, and the repair
// it feeds rebuilds the member.
#[test]
fn a_volume_longer_than_a_piece_is_read_in_pieces() {
    let block = 64 * 1024;
    let body = payload(51, 20 * block as usize + 1_234);
    let files = par3_set("long", &[("long.bin", &body)], block, 40);
    assert!(files.iter().any(|(_, bytes)| bytes.len() as u64 > PIECE));
    let mut job = Job::new(Par3::new(OWN), OWN);
    for (name, bytes) in &files {
        job.volume(name, bytes, 64 * 1024, false);
    }
    let mut wrong = body.clone();
    for at in [3, 9, 17] {
        wrong[at * block as usize + 77] ^= 0x42;
    }
    let member = job.member("long.bin", &wrong);
    let mut renamed = Vec::new();
    job.object.bind(&mut job.namespace, &mut renamed);
    let mut measured = Measured {
        store: std::mem::take(&mut job.store),
        longest: 0,
    };
    let outcome = job.object.repair(&mut measured, Budget::default());
    assert!(
        matches!(outcome, RepairOutcome::Repaired { slices } if slices >= 3),
        "{outcome:?}"
    );
    assert_eq!(measured.store.members[&member], body);
    assert!(
        measured.longest > 0 && measured.longest <= PIECE,
        "{}",
        measured.longest
    );
}

// A lost block the set carries as data is written from the volume.
#[test]
fn a_carried_block_is_read_from_its_volume() {
    let body = payload(61, 6 * BLOCK as usize + 100);
    let mut files = par3_set("carry", &[("carry.bin", &body)], BLOCK, 1);
    let id = input_set(&files[0].1);
    let carried = data_packet(id, 4, body[4 * BLOCK as usize..5 * BLOCK as usize].to_vec());
    files[0].1.extend_from_slice(&carried);
    let mut job = Job::new(Par3::new(OWN), OWN);
    for (name, bytes) in &files {
        job.volume(name, bytes, 700, false);
    }
    let mut wrong = body.clone();
    wrong[BLOCK as usize + 5] ^= 0x42;
    wrong[4 * BLOCK as usize + 5] ^= 0x42;
    let member = job.member("carry.bin", &wrong);
    let mut renamed = Vec::new();
    job.object.bind(&mut job.namespace, &mut renamed);
    let outcome = job.object.repair(&mut job.store, Budget::default());
    assert!(
        matches!(outcome, RepairOutcome::Repaired { .. }),
        "{outcome:?}"
    );
    assert_eq!(job.store.members[&member], body);
}

// The fronts' sets as a scan of every kept front reads them now.
fn fresh(object: &Par3) -> String {
    let packets = object
        .volumes
        .iter()
        .flat_map(|volume| volume.front_packets())
        .collect();
    format!("{:?}", super::super::sets(packets))
}

fn scans(object: &Par3) -> u64 {
    object.fronts.lock().unwrap().scans
}

// However the fronts arrive, come again or arrive past the kept front, the
// kept sets are those a scan of every front reads, and a front is scanned
// again only once it changed.
#[test]
fn kept_fronts_answer_as_a_fresh_scan() {
    for seed in 0..12 {
        let mut rng = Seeded(seed);
        let one = payload(seed, 3_000 + rng.next(5_000) as usize);
        let files = par3_set("front", &[("front.bin", &one)], BLOCK, 3 + rng.next(4));
        let mut job = Job::new(Par3::new(OWN), OWN);
        let volumes: Vec<VolumeIndex> = files
            .iter()
            .map(|(name, bytes)| job.name(name, bytes))
            .collect();
        let mut pieces = Vec::new();
        for (at, (_, bytes)) in files.iter().enumerate() {
            let chunk = 100 + rng.next(900) as usize;
            for (step, piece) in bytes.chunks(chunk).enumerate() {
                pieces.push((volumes[at], (step * chunk) as u64, piece.to_vec()));
            }
        }
        let mut out = Vec::new();
        while !pieces.is_empty() {
            let (volume, offset, piece) =
                pieces.swap_remove(rng.next(pieces.len() as u64) as usize);
            let provenance = if rng.next(5) == 0 {
                Provenance::Damaged
            } else {
                Provenance::Fresh
            };
            let offset = if rng.next(8) == 0 {
                offset + super::super::FRONT_CAP as u64
            } else {
                offset
            };
            job.object.deliver(
                Delivery {
                    volume,
                    offset,
                    bytes: &weaver_graph::Evidenced::from(piece.clone()),
                    provenance,
                },
                &mut |_, _| false,
                &mut out,
            );
            if rng.next(6) == 0 {
                let len = piece.len() as u64;
                job.object
                    .unposted(volume, ByteRange::at(offset, len.min(1 + rng.next(len))));
            }
            if rng.next(3) == 0 {
                let kept = format!("{:?}", *job.object.front_sets());
                assert_eq!(kept, fresh(&job.object), "seed {seed}");
                let scanned = scans(&job.object);
                let again = format!("{:?}", *job.object.front_sets());
                assert_eq!(again, kept);
                assert_eq!(scans(&job.object), scanned, "seed {seed}");
            }
        }
        assert_eq!(
            format!("{:?}", *job.object.front_sets()),
            fresh(&job.object)
        );
    }
}

// A delivery past the kept front changes no front, so no front is scanned
// again; one into a front scans that front alone.
#[test]
fn only_a_changed_front_is_scanned_again() {
    let one = payload(71, 5_000);
    let files = par3_set("again", &[("again.bin", &one)], BLOCK, 4);
    assert!(files.len() >= 2);
    let mut job = Job::new(Par3::new(OWN), OWN);
    for (name, bytes) in &files {
        job.volume(name, bytes, 700, false);
    }
    let sets = job.object.front_sets();
    assert_eq!(sets.len(), 1);
    let scanned = scans(&job.object);
    let mut out = Vec::new();
    let deliver = |job: &mut Job<Par3>, offset: u64, out: &mut Vec<_>| {
        job.object.deliver(
            Delivery {
                volume: VolumeIndex(1),
                offset,
                bytes: &weaver_graph::Evidenced::from(vec![0; 16]),
                provenance: Provenance::Fresh,
            },
            &mut |_, _| false,
            out,
        );
    };
    deliver(&mut job, super::super::FRONT_CAP as u64 + 64, &mut out);
    job.object.front_sets();
    assert_eq!(scans(&job.object), scanned);
    let len = files[1].1.len() as u64;
    deliver(&mut job, len, &mut out);
    assert_eq!(
        format!("{:?}", *job.object.front_sets()),
        fresh(&job.object)
    );
    assert_eq!(scans(&job.object), scanned + 1);
}
