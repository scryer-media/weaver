// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The RAR store: a RAR4 or RAR5 set, one volume or many, streamed into its
// members as its bytes arrive, in any order, with no engine and no file of
// its own. Every volume's headers are walked by unrar-rs from its front, so
// nothing is ever wanted from a tail, and unrar-rs's stored layout links
// them into members: a stored file maps straight to its member, piece by
// piece across the volumes it spans; an AES file maps through the cipher
// once a password opens it. Anything compressed or solid goes to an engine.
//
// The object is pure: no I/O, no threads, no files. Bytes it cannot place yet
// are held by its owner, who hands them back once the headers place them.

mod claim;
pub(crate) mod facts;
mod image;
mod layout;
mod lock;
mod place;
mod record;
mod walk;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use unrar_rs::{EncryptedStore, StoredLayoutBuilder};
use weaver_graph::{
    ByteRange, Coverage, Delivery, Emit, Extent, Kind, MemberId, Object, Placement, Provenance,
    Pull, Seek, VolumeIndex, Weight, Why,
};
use zeroize::Zeroizing;

use crate::crypt::{Cbc, Chain, Padding};
use crate::grid::crc32;
use crate::kept::Kept;
use crate::keys::Keys;
use crate::runs::CrcRuns;
use walk::{Step, Walk};

// How many volumes past the one being read are bid for. A solid set never
// streams here, so only the non-solid window applies.
const WINDOW: u32 = 8;

struct Volume {
    name: Option<String>,
    length: Option<u64>,
    closed: bool,
    kept: Kept,
    walk: Walk,
}

impl Volume {
    fn new() -> Self {
        Self {
            name: None,
            length: None,
            closed: false,
            kept: Kept::default(),
            walk: Walk::default(),
        }
    }
}

// A file's cipher, by what the object knows of its key. A sealed one was
// open before a restart: it keeps the cipher blocks it held, which are on
// no disk, and waits, as a locked one does, for the password that keys it
// again.
enum Lock {
    Plain,
    Locked,
    Open(Box<Cbc>),
    Sealed(Box<Chain>),
}

// One volume's part of a file: its data area there and where it starts in
// the file's packed stream.
struct Piece {
    volume: VolumeIndex,
    data: ByteRange,
    at: u64,
    // The CRC of this part's packed bytes, which every part but the last
    // carries in place of the file's, and whether it is keyed.
    crc: Option<u32>,
    uses_mac: bool,
    after: bool,
}

// What has been said for one member: what of it was placed, which an open
// cipher reads as what came out of it, and the plaintext its cipher stream
// carries past it, which no member holds.
#[derive(Default)]
struct Out {
    coverage: Coverage,
    closed: bool,
    padding: Padding,
}

struct Entry {
    // The name as the headers spell it, which every part repeats.
    name: String,
    length: Option<u64>,
    crc: Option<u32>,
    uses_mac: bool,
    store: Option<EncryptedStore>,
    pieces: Vec<Piece>,
    complete: bool,
    lock: Lock,
    hash_key: Option<Zeroizing<[u8; 32]>>,
    out: Out,
    // The plaintext's runs for the whole-file CRC, and every part's packed
    // runs, by piece, for its own.
    plain_runs: CrcRuns,
    parts: BTreeMap<usize, CrcRuns>,
}

impl Entry {
    fn packed(&self) -> u64 {
        self.pieces.iter().map(|piece| piece.data.len()).sum()
    }

    // The member's length: what the header said, or, for a size it left
    // unknown, what the parts add up to once the last is in.
    fn plain_len(&self) -> Option<u64> {
        self.length.or_else(|| self.complete.then(|| self.packed()))
    }
}

// A password offered.
struct Candidate {
    secret: Zeroizing<String>,
    headers_wrong: bool,
}

pub struct RarStore {
    // The process's derivations, shared by every walk, check and key.
    keys: Arc<Keys>,
    volumes: BTreeMap<VolumeIndex, Volume>,
    layout: Option<StoredLayoutBuilder>,
    entries: Vec<Entry>,
    candidates: Vec<Candidate>,
    // The candidate that opened the headers.
    headers_open: Option<usize>,
    asked: bool,
    damage_said: HashSet<(VolumeIndex, u64, u64, u32)>,
    recovery_said: BTreeSet<(VolumeIndex, u64)>,
    gaps_said: BTreeSet<VolumeIndex>,
    stale: bool,
    // The object gave its set up; it says nothing more.
    gone: bool,
}

impl Default for RarStore {
    fn default() -> Self {
        Self::new()
    }
}

impl RarStore {
    pub fn new() -> Self {
        Self::with_keys(Keys::global())
    }

    // With a key cache of its own, so a test counts its own derivations.
    pub fn with_keys(keys: Arc<Keys>) -> Self {
        Self {
            keys,
            volumes: BTreeMap::new(),
            layout: None,
            entries: Vec::new(),
            candidates: Vec::new(),
            headers_open: None,
            asked: false,
            damage_said: HashSet::new(),
            recovery_said: BTreeSet::new(),
            gaps_said: BTreeSet::new(),
            stale: true,
            gone: false,
        }
    }

    fn volume(&mut self, volume: VolumeIndex) -> &mut Volume {
        self.volumes.entry(volume).or_insert_with(Volume::new)
    }

    fn give_up(&mut self, emit: Emit, out: &mut Vec<Emit>) {
        self.gone = true;
        out.push(emit);
    }

    // A range failed its checksum: say so once for each version of its
    // bytes, then wait for a repair to deliver it again.
    fn say_damaged(
        &mut self,
        volume: VolumeIndex,
        range: ByteRange,
        crc: u32,
        out: &mut Vec<Emit>,
    ) {
        if self
            .damage_said
            .insert((volume, range.start, range.end, crc))
        {
            out.push(Emit::Damaged { volume, range });
        }
    }

    // Look again at everything, after anything changed.
    fn settle(&mut self, out: &mut Vec<Emit>) {
        while !self.gone && std::mem::take(&mut self.stale) {
            self.walk_all(out);
            if self.gone {
                return;
            }
            self.sync(out);
            if self.gone {
                return;
            }
            self.open_files(out);
        }
    }

    // Walk every volume's headers as far as its bytes go.
    fn walk_all(&mut self, out: &mut Vec<Emit>) {
        let indices: Vec<VolumeIndex> = self.volumes.keys().copied().collect();
        for volume in indices {
            let password = self.header_password();
            let Some(found) = self.volumes.get_mut(&volume) else {
                continue;
            };
            let step = found.walk.step(
                &mut found.kept,
                found.length,
                password.as_deref().map(String::as_str),
                self.keys.kdf(),
                found.closed,
            );
            match step {
                Step::Walked { moved, damaged } => {
                    self.stale |= moved;
                    if let Some(range) = damaged {
                        let crc = found.kept.read(range).map_or(0, crc32);
                        self.say_damaged(volume, range, crc, out);
                    }
                }
                Step::Locked(encryption) => self.open_headers(&encryption, out),
                Step::Broken(range) => {
                    if found.kept.verified(range) {
                        return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
                    }
                    let crc = found.kept.read(range).map_or(0, crc32);
                    self.say_damaged(volume, range, crc, out);
                }
                Step::Refuse(emit) => return self.give_up(emit, out),
            }
            if self.gone {
                return;
            }
        }
    }

    // Everything bound has closed: nothing more will arrive.
    fn finished(&self) -> bool {
        self.volumes
            .values()
            .filter(|volume| volume.name.is_some())
            .all(|volume| volume.closed)
    }

    // The last volume, once some volume's headers say it is.
    fn last(&self) -> Option<VolumeIndex> {
        self.volumes
            .iter()
            .find(|(_, volume)| volume.walk.is_last())
            .map(|(index, _)| *index)
    }

    // Numbered volumes that never came, once everything else has: damaged
    // whole, at the length every volume but the last shares.
    fn say_gaps(&mut self, out: &mut Vec<Emit>) {
        let Some(last) = self.last().filter(|_| self.finished()) else {
            return;
        };
        let Some(length) = self
            .volumes
            .iter()
            .filter(|(index, volume)| **index != last && volume.closed)
            .find_map(|(_, volume)| volume.length)
        else {
            return;
        };
        for index in 0..last.0 {
            let volume = VolumeIndex(index);
            let missing = self
                .volumes
                .get(&volume)
                .is_none_or(|found| found.name.is_none());
            if missing && self.gaps_said.insert(volume) {
                out.push(Emit::Damaged {
                    volume,
                    range: ByteRange::new(0, length),
                });
            }
        }
    }
}

impl Object for RarStore {
    fn kind(&self) -> Kind {
        Kind::RarStore
    }

    fn facts(&self) -> weaver_graph::Facts {
        facts::of(self)
    }

    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        if self.gone || delivery.bytes.is_empty() {
            return;
        }
        let volume = delivery.volume;
        let range = delivery.range();
        // Absorbing can make the object want more of this very delivery: a
        // header's first bytes name its size.
        loop {
            let found = self.volume(volume);
            let generation = found.kept.generation();
            if found
                .kept
                .absorb(range, delivery.bytes, delivery.provenance)
            {
                found.walk.touch();
                self.stale = true;
            }
            self.settle(out);
            if self.gone {
                // The delivery that made the store give its set up still
                // reaches the members announced before it, so what the
                // store answers for those bytes is where they went. Giving
                // up stays its last word, after which nothing is heard.
                let last = out.pop();
                self.route(
                    volume,
                    range,
                    delivery.bytes,
                    delivery.provenance,
                    read,
                    out,
                );
                out.extend(last);
                return;
            }
            // A damaged delivery over the walk's next bytes is kept whole, so
            // it is said once and its repair is seen.
            let found = self.volume(volume);
            if delivery.provenance == Provenance::Damaged && !found.closed {
                let hole = found.walk.hole(&found.kept, false);
                if range.contains(hole) {
                    found.kept.want(ByteRange::new(hole, range.end));
                }
            }
            if found.kept.generation() == generation {
                break;
            }
        }
        self.route(
            volume,
            range,
            delivery.bytes,
            delivery.provenance,
            read,
            out,
        );
    }

    fn volume_closed(&mut self, volume: VolumeIndex, out: &mut Vec<Emit>) {
        if self.gone {
            return;
        }
        let found = self.volume(volume);
        found.closed = true;
        found.walk.touch();
        self.stale = true;
        self.settle(out);
        if self.gone {
            return;
        }
        self.say_gaps(out);
        self.exhausted(out);
    }

    fn volume_lost(&mut self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Emit>) {
        if self.gone {
            return;
        }
        let Some(found) = self.volumes.get(&volume) else {
            return;
        };
        let next = ByteRange::at(found.walk.hole(&found.kept, found.closed), 1);
        if found.walk.ended || range.intersect(next).is_none() {
            return;
        }
        // The next header will never come, and nothing past it can be read.
        let why = match volume == VolumeIndex(0) && found.walk.facts.is_none() {
            true => Why::FirstBytesLost,
            false => Why::Unrepairable,
        };
        self.give_up(Emit::Refuse(why), out);
    }

    fn wants(&mut self, out: &mut Vec<Seek>) {
        if self.gone {
            return;
        }
        // The first volume not read to its end.
        let reading = (0..)
            .find(|index| {
                self.volumes
                    .get(&VolumeIndex(*index))
                    .is_none_or(|found| !found.walk.ended)
            })
            .unwrap_or(0);
        for distance in 1..=WINDOW {
            let volume = VolumeIndex(reading + distance);
            let Some(found) = self.volumes.get(&volume) else {
                continue;
            };
            let (true, Some(length), false) = (found.name.is_some(), found.length, found.closed)
            else {
                continue;
            };
            out.push(Seek::data(
                volume,
                ByteRange::new(0, length),
                Weight(distance),
            ));
        }
    }

    fn pulled(&mut self, _seek: Seek, _pull: Pull, _out: &mut Vec<Emit>) {
        // The bids only bring volumes forward; one that cannot be brought
        // forward arrives in the normal order, which the store reads anyway.
    }

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        self.place(volume, range, out);
    }

    fn extent(&self, member: MemberId, range: ByteRange, out: &mut Vec<Extent>) {
        self.member_extent(member, range, out);
    }

    fn posted(
        &self,
        volume: VolumeIndex,
        range: ByteRange,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        self.rebuild_posted(volume, range, read, into)
    }

    fn sized(&mut self, volume: VolumeIndex, length: u64, out: &mut Vec<Emit>) {
        let found = self.volume(volume);
        if found.length.is_some() {
            return;
        }
        found.length = Some(length);
        found.walk.touch();
        self.stale = true;
        self.settle(out);
    }

    fn offer_password(&mut self, secret: &str, out: &mut Vec<Emit>) {
        if self.gone {
            return;
        }
        // The offer answers the question the store asked: a candidate that
        // opens nothing leaves it free to ask for the next one.
        self.asked = false;
        if !self
            .candidates
            .iter()
            .any(|candidate| candidate.secret.as_str() == secret)
        {
            self.candidates.push(Candidate {
                secret: Zeroizing::new(secret.to_owned()),
                headers_wrong: false,
            });
        }
        for volume in self.volumes.values_mut() {
            volume.walk.touch();
        }
        self.stale = true;
        self.settle(out);
    }

    fn envelope(&self, envelope: u32, range: ByteRange) -> Option<bytes::Bytes> {
        self.volumes
            .get(&VolumeIndex(envelope))?
            .kept
            .read(range)
            .map(bytes::Bytes::copy_from_slice)
    }

    fn bound(&mut self, volume: VolumeIndex, name: &str) {
        self.volume(volume).name = Some(name.to_owned());
        self.stale = true;
    }

    fn claim(&mut self, name: &str, first_bytes: &[u8]) -> Option<VolumeIndex> {
        if self.gone {
            return None;
        }
        self.claim_volume(name, first_bytes)
    }
}
