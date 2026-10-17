// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Walks the packets of one PAR2 volume as its bytes arrive, front to back.
//
// Metadata packets are small and are parsed whole. A recovery packet is
// mostly payload the object never needs until a repair spends it, and the
// namespace keeps the volume's raw bytes, so only its header and exponent
// are read here and its place recorded; the payload is read back, and its
// hash checked, when it is spent. Bytes that arrive ahead of the front are
// held until the front reaches them, within a bound.

use std::collections::BTreeMap;

use par2_rs::packet::{HEADER_SIZE, MAGIC, PacketHeader, PacketType};
use par2_rs::{Packet, parse_packet};
use weaver_graph::ByteRange;

// A metadata packet longer than this is not one a sane creator writes: an
// IFSC for the old engine's largest file grid is a third of this.
const METADATA_CAP: u64 = 4 * 1024 * 1024;
// Bytes ahead of the front a volume may hold. Past it, deliveries are not
// walked; the payloads are still on disk and a repair reads them back.
const HOLD_CAP: usize = 8 * 1024 * 1024;
// A recovery packet's header and its four-byte exponent.
const RECOVERY_HEAD: usize = HEADER_SIZE + 4;

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
}

#[derive(Debug, Default)]
pub(super) struct Walker {
    // Where the next packet header is expected.
    next: u64,
    // Coalesced islands of bytes at or past `next`, keyed by their offset.
    held: BTreeMap<u64, Vec<u8>>,
    held_bytes: usize,
    // Ranges whose bytes arrived damaged; a header there is not trusted.
    damaged: Vec<ByteRange>,
    // Ranges no server has; the front steps over them and resyncs, or every
    // packet past a lost article would stay unfound.
    lost: Vec<ByteRange>,
    // Some delivery was dropped for the hold bound, so the walk may stop
    // short of the volume's end.
    pub(super) overflowed: bool,
}

impl Walker {
    pub(super) fn deliver(
        &mut self,
        offset: u64,
        bytes: &[u8],
        damaged: bool,
        found: &mut Vec<Found>,
    ) {
        let range = ByteRange::at(offset, bytes.len() as u64);
        if damaged {
            self.damaged.push(range);
        }
        if range.end <= self.next {
            return;
        }
        let skip = self.next.saturating_sub(offset) as usize;
        let start = offset.max(self.next);
        let bytes = &bytes[skip..];
        if self.held_bytes + bytes.len() > HOLD_CAP {
            self.overflowed = true;
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

    pub(super) fn is_damaged(&self, range: ByteRange) -> bool {
        self.damaged
            .iter()
            .any(|bad| bad.intersect(range).is_some())
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
    // to the range's end; what lies there is not a trusted header and is
    // resynced from.
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
        self.advance(lost.end);
        true
    }

    // One packet past the front, or false when the held bytes do not reach
    // the next one.
    fn step(&mut self, found: &mut Vec<Found>) -> bool {
        let Some(front) = self.held.get(&self.next) else {
            return false;
        };
        if front.len() < HEADER_SIZE {
            return false;
        }
        let at = self.next;
        let header_range = ByteRange::at(at, HEADER_SIZE as u64);
        let header = (&front[..8] == MAGIC && !self.is_damaged(header_range))
            .then(|| PacketHeader::parse(front, at).ok())
            .flatten();
        let Some(header) = header else {
            let step = resync(front);
            self.advance(at + step);
            return true;
        };
        let range = ByteRange::at(at, header.length);
        let set = *header.recovery_set_id.as_bytes();
        if header.packet_type == PacketType::RecoverySlice {
            if front.len() < RECOVERY_HEAD {
                return false;
            }
            let mut exponent = [0u8; 4];
            exponent.copy_from_slice(&front[HEADER_SIZE..RECOVERY_HEAD]);
            found.push(Found::Recovery {
                set,
                exponent: u32::from_le_bytes(exponent),
                range,
            });
            self.advance(range.end);
            return true;
        }
        if header.length > METADATA_CAP {
            self.advance(range.end);
            return true;
        }
        if (front.len() as u64) < header.length {
            return false;
        }
        match parse_packet(&front[..header.length as usize], at) {
            // The packet hash is checked by the parse, so a packet that
            // parses is right even where its bytes arrived marked damaged.
            Ok((packet, _)) => {
                found.push(Found::Metadata {
                    set,
                    at,
                    hash: header.packet_hash,
                    packet,
                });
                self.advance(range.end);
            }
            // A metadata packet that fails its hash is not a packet; the
            // next magic is.
            _ => self.advance(at + 1),
        }
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

// How far to step from a front that is not a trusted header: to the next
// magic in hand, or to where the next magic could still begin.
fn resync(front: &[u8]) -> u64 {
    let found = front[1..]
        .windows(MAGIC.len())
        .position(|window| window == MAGIC)
        .map(|at| at + 1);
    found.unwrap_or(front.len() + 1 - MAGIC.len()) as u64
}
