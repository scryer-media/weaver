// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Walks the packets of one PAR2 volume as its bytes arrive, front to back,
// and hands on only packets whose MD5 passes.
//
// A metadata packet is held whole and parsed by par2-rs, which checks its
// hash; one that fails is not a packet, and the walk resyncs from the byte
// after its magic. A recovery packet is mostly payload, so it is hashed as it
// passes and only its exponent kept; the payload stays in the volume's file
// and is read back when a repair spends it. Bytes that arrive ahead of the
// front are held until the front reaches them, within a bound.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use par2_rs::packet::{HEADER_SIZE, MAGIC, PacketHeader, PacketType};
use par2_rs::{FileHashState, Packet, ScannedPacket, parse_packet};
use weaver_graph::ByteRange;

// The longest body par2-rs buffers for each metadata type. A longer one, and
// a packet of a type it does not know, is hashed and dropped.
const MAIN_BODY_CAP: u64 = 12 + 32_768 * 16;
const FILE_DESC_BODY_CAP: u64 = 56 + 100_000;
const IFSC_BODY_CAP: u64 = 16 + 32_768 * 20;
const CREATOR_BODY_CAP: u64 = 100_000;
// Bytes ahead of the front a volume may hold. Past it, deliveries are not
// walked; the payloads are still on disk and a repair reads them back.
const HOLD_CAP: usize = 8 * 1024 * 1024;

// What the walker found at one place in the volume.
#[derive(Debug)]
pub(super) enum Found {
    Metadata {
        set: [u8; 16],
        at: u64,
        hash: [u8; 16],
        packet: Packet,
    },
    Recovery {
        set: [u8; 16],
        exponent: u32,
        range: ByteRange,
    },
    // A packet that passed its hash and is of no use.
    Unused,
}

// A packet hashed as it passes.
struct Streamed {
    at: u64,
    header: PacketHeader,
    hasher: FileHashState,
    exponent: u32,
}

#[derive(Default)]
pub(super) struct Walker {
    // Where the next packet header is expected, or the next byte of the
    // packet being streamed.
    next: u64,
    streamed: Option<Streamed>,
    // Coalesced islands of bytes at or past `next`, keyed by their offset.
    held: BTreeMap<u64, Vec<u8>>,
    held_bytes: usize,
    // Ranges no server has; the front steps over them and resyncs, or every
    // packet past a lost article would stay unfound.
    lost: Vec<ByteRange>,
}

impl Walker {
    // A walk resumed at a packet boundary: a packet streamed when the
    // checkpoint was taken is hashed again from its header, and what was
    // held past the front is delivered again by the replay.
    pub(super) fn resumed(front: u64, lost: Vec<ByteRange>) -> Self {
        Self {
            next: front,
            lost,
            ..Self::default()
        }
    }

    // The packet boundary the walk has reached.
    pub(super) fn front(&self) -> u64 {
        self.streamed.as_ref().map_or(self.next, |open| open.at)
    }

    pub(super) fn lost(&self) -> &[ByteRange] {
        &self.lost
    }

    pub(super) fn deliver(&mut self, offset: u64, bytes: &[u8], found: &mut Vec<Found>) {
        if offset + bytes.len() as u64 <= self.next {
            return;
        }
        let skip = self.next.saturating_sub(offset) as usize;
        let start = offset.max(self.next);
        let bytes = &bytes[skip..];
        if self.held_bytes + bytes.len() > HOLD_CAP {
            return;
        }
        self.hold(start, bytes);
        self.walk(found);
    }

    pub(super) fn lose(&mut self, range: ByteRange, found: &mut Vec<Found>) {
        if range.end > self.next {
            self.lost.push(range);
            self.walk(found);
        }
    }

    // Merge bytes into the islands; new bytes win where they overlap.
    fn hold(&mut self, start: u64, bytes: &[u8]) {
        let end = start + bytes.len() as u64;
        let touching: Vec<u64> = self
            .held
            .range(..=end)
            .filter(|(at, island)| **at + island.len() as u64 >= start)
            .map(|(at, _)| *at)
            .collect();
        let mut merged_start = start;
        let mut merged_end = end;
        let mut islands = Vec::with_capacity(touching.len());
        for at in touching {
            if let Some(island) = self.held.remove(&at) {
                self.held_bytes -= island.len();
                merged_start = merged_start.min(at);
                merged_end = merged_end.max(at + island.len() as u64);
                islands.push((at, island));
            }
        }
        let mut merged = vec![0u8; (merged_end - merged_start) as usize];
        for (at, island) in islands {
            let from = (at - merged_start) as usize;
            merged[from..from + island.len()].copy_from_slice(&island);
        }
        let from = (start - merged_start) as usize;
        merged[from..from + bytes.len()].copy_from_slice(bytes);
        self.held_bytes += merged.len();
        self.held.insert(merged_start, merged);
    }

    // Advance the front over every packet the held bytes complete, and over
    // a lost range the front has reached.
    fn walk(&mut self, found: &mut Vec<Found>) {
        while self.step(found) || self.cross_loss() {}
    }

    // Where the held bytes run out and a lost range begins, the front skips
    // to the range's end; a packet the loss cuts is dropped, and what lies
    // past it is not a trusted header and is resynced from.
    fn cross_loss(&mut self) -> bool {
        let reach = self.next + self.held.get(&self.next).map_or(0, Vec::len) as u64;
        let Some(lost) = self
            .lost
            .iter()
            .find(|lost| lost.start <= reach && lost.end > self.next)
            .copied()
        else {
            return false;
        };
        self.streamed = None;
        self.advance(lost.end);
        true
    }

    // One packet past the front, or false when the held bytes do not reach
    // the next one.
    fn step(&mut self, found: &mut Vec<Found>) -> bool {
        if self.streamed.is_some() {
            return self.stream(found);
        }
        let Some(front) = self.held.get(&self.next) else {
            return false;
        };
        if front.len() < HEADER_SIZE {
            return false;
        }
        let at = self.next;
        let header = (&front[..8] == MAGIC)
            .then(|| PacketHeader::parse(front, at).ok())
            .flatten();
        let Some(header) = header else {
            let step = resync(front);
            self.advance(at + step);
            return true;
        };
        if body_cap(header.packet_type).is_none_or(|cap| header.body_length() > cap) {
            let Some(exponent) = front.get(HEADER_SIZE..HEADER_SIZE + 4) else {
                return false;
            };
            let mut hasher = FileHashState::new();
            hasher.update(&front[32..HEADER_SIZE]);
            self.streamed = Some(Streamed {
                at,
                exponent: u32::from_le_bytes([exponent[0], exponent[1], exponent[2], exponent[3]]),
                header,
                hasher,
            });
            self.advance(at + HEADER_SIZE as u64);
            return true;
        }
        if (front.len() as u64) < header.length {
            return false;
        }
        match parse_packet(&front[..header.length as usize], at) {
            Ok((packet, _)) => {
                found.push(Found::Metadata {
                    set: *header.recovery_set_id.as_bytes(),
                    at,
                    hash: header.packet_hash,
                    packet,
                });
                self.advance(at + header.length);
            }
            Err(_) => self.advance(at + 1),
        }
        true
    }

    // Hash what is held of the streamed packet; at its end, hand it on if
    // its hash passes. One that fails was read past and is dropped: its
    // bytes are gone from the hold, so the walk resumes at its end.
    fn stream(&mut self, found: &mut Vec<Found>) -> bool {
        let (Some(open), Some(front)) = (self.streamed.as_mut(), self.held.get(&self.next)) else {
            return false;
        };
        let end = open.at + open.header.length;
        let take = (end - self.next).min(front.len() as u64) as usize;
        open.hasher.update(&front[..take]);
        let to = self.next + take as u64;
        self.advance(to);
        if to < end {
            return true;
        }
        let Some(open) = self.streamed.take() else {
            return true;
        };
        if open.hasher.finalize() != open.header.packet_hash {
            return true;
        }
        found.push(match open.header.packet_type {
            PacketType::RecoverySlice if open.header.body_length() > 4 => Found::Recovery {
                set: *open.header.recovery_set_id.as_bytes(),
                exponent: open.exponent,
                range: ByteRange::new(open.at, end),
            },
            _ => Found::Unused,
        });
        true
    }

    // Move the front, dropping what it passed.
    fn advance(&mut self, to: u64) {
        self.next = to;
        let passed: Vec<u64> = self.held.range(..to).map(|(at, _)| *at).collect();
        for at in passed {
            let Some(island) = self.held.remove(&at) else {
                continue;
            };
            self.held_bytes -= island.len();
            let end = at + island.len() as u64;
            if end > to {
                let keep = island[(to - at) as usize..].to_vec();
                self.held_bytes += keep.len();
                self.held.insert(to, keep);
            }
        }
    }
}

fn body_cap(packet_type: PacketType) -> Option<u64> {
    match packet_type {
        PacketType::Main => Some(MAIN_BODY_CAP),
        PacketType::FileDescription => Some(FILE_DESC_BODY_CAP),
        PacketType::InputFileSliceChecksum => Some(IFSC_BODY_CAP),
        PacketType::Creator => Some(CREATOR_BODY_CAP),
        PacketType::RecoverySlice | PacketType::Unknown(_) => None,
    }
}

// How far to step from a front that is not a trusted header: to the next
// magic in hand, or to where the next magic could still begin.
fn resync(front: &[u8]) -> u64 {
    let found = front[1..]
        .windows(MAGIC.len())
        .position(|window| window == MAGIC)
        .map(|at| at + 1);
    found.unwrap_or(front.len() + 1 - MAGIC.len()) as u64
}

// What one job's packets may cost across every volume, by par2-rs's three
// scan meters. Its budget keeps them in cells, which an object shared across
// threads cannot hold, so they are counted here the same way.
const MAX_EXAMINED: u64 = 1_048_576;
const MAX_RETAINED: usize = 262_144;
const MAX_RETAINED_BYTES: usize = 256 * 1024 * 1024;

#[derive(Clone, Default, Serialize, Deserialize)]
pub(super) struct Meter {
    examined: u64,
    retained: usize,
    bytes: usize,
}

impl Meter {
    // Charge one packet the walk found; false once a limit is passed.
    // Variable metadata plus container and allocation overhead; recovery
    // payloads stay in the volume and cost nothing here.
    pub(super) fn charge(&mut self, found: &Found) -> bool {
        self.examined += 1;
        let variable = match found {
            Found::Unused => None,
            Found::Recovery { .. } => Some(0),
            Found::Metadata { packet, .. } => Some(match packet {
                Packet::Main(main) => (main.recovery_file_ids.len()
                    + main.non_recovery_file_ids.len())
                .saturating_mul(16),
                Packet::FileDescription(file) => {
                    file.filename.len().saturating_add(file.par2_name.len())
                }
                Packet::InputFileSliceChecksum(ifsc) => ifsc
                    .checksums
                    .len()
                    .saturating_mul(size_of::<par2_rs::SliceChecksum>()),
                Packet::Creator(creator) => creator.creator_id.len(),
                Packet::Unknown { body, .. } => body.len(),
                Packet::RecoverySlice(_) => 0,
            }),
        };
        if let Some(variable) = variable {
            self.retained += 1;
            self.bytes = self.bytes.saturating_add(
                variable
                    .saturating_mul(2)
                    .saturating_add(2 * size_of::<ScannedPacket>() + 256),
            );
        }
        self.examined <= MAX_EXAMINED
            && self.retained <= MAX_RETAINED
            && self.bytes <= MAX_RETAINED_BYTES
    }
}
