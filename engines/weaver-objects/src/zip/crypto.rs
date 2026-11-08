// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// ZipCrypto feeds every plaintext byte into the next byte's key. The header,
// a bounded ciphertext proof and pending repair input are durable; keys are
// rebuilt from an offered password and the member's existing plaintext.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use weaver_graph::{ByteRange, Coverage, Provenance};
use zeroize::Zeroizing;

use super::records::Header;
use crate::crypt::Placed;

pub(super) const HEADER: u64 = 12;
const STEP: u64 = 64 * 1024;

const fn crc_table() -> [u32; 256] {
    let mut table = [0; 256];
    let mut i = 0;
    while i < table.len() {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = (crc >> 1) ^ (0xedb8_8320 & 0u32.wrapping_sub(crc & 1));
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}
const CRC: [u32; 256] = crc_table();

#[derive(Clone)]
struct Keys(Zeroizing<[u32; 3]>);

impl Keys {
    fn new(password: &[u8]) -> Self {
        let mut keys = Self(Zeroizing::new([0x1234_5678, 0x2345_6789, 0x3456_7890]));
        for &byte in password {
            keys.update(byte);
        }
        keys
    }

    fn mask(&self) -> u8 {
        let temp = (self.0[2] as u16) | 2;
        (temp.wrapping_mul(temp ^ 1) >> 8) as u8
    }

    fn update(&mut self, plain: u8) {
        let crc = |key: u32, byte: u8| (key >> 8) ^ CRC[((key as u8) ^ byte) as usize];
        self.0[0] = crc(self.0[0], plain);
        self.0[1] = self.0[1]
            .wrapping_add(self.0[0] & 0xff)
            .wrapping_mul(134_775_813)
            .wrapping_add(1);
        self.0[2] = crc(self.0[2], (self.0[1] >> 24) as u8);
    }

    fn decrypt(&mut self, byte: u8) -> u8 {
        let plain = byte ^ self.mask();
        self.update(plain);
        plain
    }
}

#[derive(Clone)]
struct State {
    keys: Keys,
    provenance: Provenance,
    // The CRC32 of the plaintext before this point, so a replay from it
    // can say the prefix's checksum without reading the prefix back.
    crc: u32,
}

impl State {
    fn fold(&mut self, plain: &[u8]) {
        if !plain.is_empty() {
            self.crc =
                crate::grid::crc32_combine(self.crc, crate::grid::crc32(plain), plain.len() as u64);
        }
    }
}

#[cfg(test)]
pub(super) fn encrypt_fixture(password: &str, plain: &[u8]) -> Vec<u8> {
    encrypt_fixture_with_check(password, plain, (crate::grid::crc32(plain) >> 24) as u8)
}

#[cfg(test)]
pub(super) fn encrypt_fixture_with_check(password: &str, plain: &[u8], check: u8) -> Vec<u8> {
    let mut header = [0x73; HEADER as usize];
    header[11] = check;
    let mut keys = Keys::new(password.as_bytes());
    header
        .into_iter()
        .chain(plain.iter().copied())
        .map(|byte| {
            let cipher = byte ^ keys.mask();
            keys.update(byte);
            cipher
        })
        .collect()
}

struct Decrypted {
    end: u64,
    plain: Vec<u8>,
    // The state at the delivery's start, and every checkpoint after it.
    before: State,
    passed: Vec<(u64, State)>,
    after: State,
}

#[derive(Clone)]
pub(super) struct Open {
    checkpoints: BTreeMap<u64, State>,
    frontier: (u64, State),
    pub(super) candidate: usize,
    pub(super) checked: bool,
}

impl std::fmt::Debug for Open {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Open")
            .field("frontier", &self.frontier.0)
            .finish_non_exhaustive()
    }
}

impl Open {
    fn proves(&self, plain: &[u8], cipher: &[u8]) -> bool {
        let Some(state) = self.checkpoints.get(&0) else {
            return false;
        };
        if plain.len() != cipher.len() {
            return false;
        }
        let mut keys = state.keys.clone();
        plain.iter().zip(cipher).all(|(&plain, &cipher)| {
            let matches = plain ^ keys.mask() == cipher;
            keys.update(plain);
            matches
        })
    }

    pub(super) fn reaches(&self) -> u64 {
        self.frontier.0
    }

    // The state at `offset`, walked from the last one kept before it, each
    // checkpoint it passes handed to `passed`, none kept.
    fn walk(
        &self,
        offset: u64,
        coverage: &Coverage,
        read: &mut dyn FnMut(ByteRange, &mut Vec<u8>) -> bool,
        passed: &mut Vec<(u64, State)>,
    ) -> Option<State> {
        let (&mut_at, initial) = self.checkpoints.range(..=offset).next_back()?;
        let (mut at, mut state) = (mut_at, initial.clone());
        if self.frontier.0 <= offset && self.frontier.0 > at {
            (at, state) = self.frontier.clone();
        }
        while at < offset {
            let end = ((at / STEP + 1) * STEP).min(offset);
            let range = ByteRange::new(at, end);
            let provenance = coverage.placed(range)?;
            let mut plain = Vec::new();
            if !read(range, &mut plain) || plain.len() as u64 != range.len() {
                return None;
            }
            for &byte in &plain {
                state.keys.update(byte);
            }
            state.fold(&plain);
            state.provenance = crate::crypt::worse(state.provenance, provenance);
            at = end;
            if at % STEP == 0 {
                passed.push((at, state.clone()));
            }
        }
        Some(state)
    }

    // A trial taken at once, as the tests decrypt.
    #[cfg(test)]
    pub(super) fn decrypt(
        &mut self,
        offset: u64,
        cipher: &[u8],
        provenance: Provenance,
        coverage: &Coverage,
        read: &mut dyn FnMut(ByteRange, &mut Vec<u8>) -> bool,
    ) -> Option<(Vec<u8>, Provenance)> {
        let decrypted = self.trial(offset, cipher, provenance, coverage, read)?;
        Some(self.keep(decrypted))
    }

    // What decrypting `cipher` at `offset` gives, the keys untouched: a
    // trial of bytes said already costs no copy of every checkpoint.
    fn trial(
        &self,
        offset: u64,
        cipher: &[u8],
        provenance: Provenance,
        coverage: &Coverage,
        read: &mut dyn FnMut(ByteRange, &mut Vec<u8>) -> bool,
    ) -> Option<Decrypted> {
        let mut passed = Vec::new();
        let before = self.walk(offset, coverage, read, &mut passed)?;
        let mut state = before.clone();
        state.provenance = crate::crypt::worse(state.provenance, provenance);
        let mut plain = Vec::with_capacity(cipher.len());
        let mut folded = 0;
        for (at, &byte) in (offset..).zip(cipher) {
            plain.push(state.keys.decrypt(byte));
            if (at + 1) % STEP == 0 {
                state.fold(&plain[folded..]);
                folded = plain.len();
                passed.push((at + 1, state.clone()));
            }
        }
        state.fold(&plain[folded..]);
        Some(Decrypted {
            end: offset + cipher.len() as u64,
            plain,
            before,
            passed,
            after: state,
        })
    }

    // A trial's result taken: its checkpoints kept and the frontier moved.
    fn keep(&mut self, decrypted: Decrypted) -> (Vec<u8>, Provenance) {
        self.checkpoints.extend(decrypted.passed);
        let provenance = decrypted.after.provenance;
        if decrypted.end >= self.frontier.0 {
            self.frontier = (decrypted.end, decrypted.after);
        }
        (decrypted.plain, provenance)
    }

    fn seal(
        &self,
        frontier: &mut Option<(usize, u64, State)>,
        range: ByteRange,
        coverage: &Coverage,
        read: &mut dyn FnMut(ByteRange, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        let Some((&start, initial)) = self.checkpoints.range(..=range.start).next_back() else {
            return false;
        };
        let (mut at, mut state) = (start, initial.clone());
        if let Some((_, cached, value)) = frontier.as_ref().filter(|(candidate, cached, _)| {
            *candidate == self.candidate && *cached <= range.start && *cached > start
        }) {
            (at, state) = (*cached, value.clone());
        }
        while at < range.start {
            let end = (at + STEP).min(range.start);
            let prefix = ByteRange::new(at, end);
            let mut plain = Vec::new();
            if !coverage.covered(prefix)
                || !read(prefix, &mut plain)
                || plain.len() as u64 != prefix.len()
            {
                return false;
            }
            for byte in plain {
                state.keys.update(byte);
            }
            at = end;
        }
        let mut plain = Vec::new();
        if !coverage.covered(range) || !read(range, &mut plain) || plain.len() as u64 != range.len()
        {
            return false;
        }
        for byte in plain {
            into.push(byte ^ state.keys.mask());
            state.keys.update(byte);
        }
        *frontier = Some((self.candidate, range.end, state));
        true
    }

    pub(super) fn forget(&mut self, from: u64) {
        let retained = self
            .checkpoints
            .range(..=from)
            .next_back()
            .map(|(&at, state)| (at, state.clone()));
        self.checkpoints.retain(|at, _| *at <= from);
        if self.frontier.0 > from
            && let Some(retained) = retained
        {
            self.frontier = retained;
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Cipher {
    check: u8,
    pub(super) header: Option<[u8; HEADER as usize]>,
    pub(super) proven: bool,
    proof: Vec<u8>,
    // Set by snapshots that replayed a whole restored member; nothing
    // sets it now, an old one's replay still starts the member over.
    replay: bool,
    // A replacement waits until the old suffix has been read back through
    // its old cipher. Replayed bytes are overlaid with the replacement.
    pub(super) pending: Vec<(ByteRange, Vec<u8>, Provenance)>,
    pub(super) retry: bool,
    pub(super) announce: bool,
    #[serde(skip)]
    pub(super) open: Option<Arc<Open>>,
    #[serde(skip)]
    rejected: BTreeSet<usize>,
}

impl Cipher {
    pub(super) fn sound(&self, length: u64) -> bool {
        self.proof.len() as u64 <= length.min(32)
            && self.pending.iter().all(|(range, bytes, _)| {
                !range.is_empty() && range.end <= length && range.len() == bytes.len() as u64
            })
    }

    pub(super) fn new(header: &Header) -> Self {
        Self {
            check: if header.deferred() {
                (header.modified >> 8) as u8
            } else {
                (header.crc >> 24) as u8
            },
            header: None,
            proven: false,
            proof: Vec::new(),
            replay: false,
            pending: Vec::new(),
            retry: false,
            announce: false,
            open: None,
            rejected: BTreeSet::new(),
        }
    }

    // The cipher as its header alone states it: nothing proven, opened,
    // waiting or rejected.
    fn bare(&self) -> Self {
        Self {
            check: self.check,
            header: self.header,
            proven: false,
            proof: Vec::new(),
            replay: false,
            pending: Vec::new(),
            retry: false,
            announce: false,
            open: None,
            rejected: BTreeSet::new(),
        }
    }

    pub(super) fn unlock(
        &mut self,
        candidates: &[Zeroizing<String>],
        provenance: Provenance,
    ) -> bool {
        if self.open.is_some() {
            return true;
        }
        let Some(header) = self.header else {
            return false;
        };
        for (candidate, password) in candidates.iter().enumerate() {
            if self.rejected.contains(&candidate) {
                continue;
            }
            let mut keys = Keys::new(password.as_bytes());
            let mut last = 0;
            for byte in header {
                last = keys.decrypt(byte);
            }
            if last != self.check {
                self.rejected.insert(candidate);
                continue;
            }
            let state = State {
                keys,
                provenance,
                crc: 0,
            };
            self.open = Some(Arc::new(Open {
                checkpoints: BTreeMap::from([(0, state.clone())]),
                frontier: (0, state),
                candidate,
                checked: true,
            }));
            return true;
        }
        false
    }

    // Whether a candidate other than the open one, not rejected, passes the
    // header's check byte: only then is a whole-member CRC failure over
    // verified bytes a reason to give the open candidate up. Otherwise the
    // bytes are what is wrong, and a repair is what puts them right.
    pub(super) fn alternative(&self, candidates: &[Zeroizing<String>]) -> bool {
        let (Some(header), Some(open)) = (self.header, &self.open) else {
            return false;
        };
        candidates.iter().enumerate().any(|(candidate, password)| {
            if candidate == open.candidate || self.rejected.contains(&candidate) {
                return false;
            }
            let mut keys = Keys::new(password.as_bytes());
            let mut last = 0;
            for byte in header {
                last = keys.decrypt(byte);
            }
            last == self.check
        })
    }

    // The member's ciphertext changed: a candidate given up on the old
    // bytes' CRC may be the right one for the new.
    pub(super) fn changed(&mut self) {
        self.retry = false;
        self.rejected.clear();
    }

    fn replace_header(&mut self, header: [u8; HEADER as usize]) {
        self.header = Some(header);
        self.proven = false;
        self.proof.clear();
        self.replay = false;
        self.pending.clear();
        self.retry = false;
        self.announce = true;
        self.open = None;
        self.rejected.clear();
    }

    pub(super) fn unposted(&mut self, from: u64) {
        if self.replay && from == 0 {
            self.announce = true;
            self.replay = false;
        }
        if self.retry {
            if let Some(open) = self.open.take() {
                self.rejected.insert(open.candidate);
            }
            self.retry = false;
            self.proven = false;
            self.announce = true;
        } else if let Some(open) = &mut self.open {
            Arc::make_mut(open).forget(from);
        }
    }
}

impl super::Zip {
    pub(super) fn posted_over(&self, ranges: &[(weaver_graph::VolumeIndex, ByteRange)]) -> Posted {
        use weaver_graph::{Placement, Placements};
        let placed = Placements::take(ranges, |volume, range, out: &mut Vec<Placement>| {
            self.place_as(volume, range, true, out)
        });
        let mut sealed = Vec::new();
        let mut shared: BTreeMap<weaver_graph::MemberId, Arc<MemberKeys>> = BTreeMap::new();
        for &(volume, range) in ranges {
            let (Some(global), Some(map)) = (self.global(volume, range), &self.map) else {
                continue;
            };
            let base = global.start - range.start;
            for entry in &map.entries {
                let (Some(span), Some(member), Some(open)) = (
                    entry.span(),
                    entry.member,
                    entry
                        .cipher
                        .as_ref()
                        .and_then(|cipher| cipher.open.as_ref()),
                ) else {
                    continue;
                };
                let Some(part) = span.intersect(global) else {
                    continue;
                };
                let coverage = &self.members[member.0 as usize].coverage;
                for run in coverage.overlapping(part.shift(-(span.start as i64))) {
                    let Some(part) = part.intersect(run.range.shift(span.start as i64)) else {
                        continue;
                    };
                    // One member's runs share its coverage, keys and proof.
                    let keys = shared.entry(member).or_insert_with(|| {
                        Arc::new(MemberKeys {
                            coverage: coverage.clone(),
                            open: Arc::clone(open),
                            frontier: std::sync::Mutex::new(None),
                            proof: cipher_proof(entry),
                            bare: entry.cipher.as_ref().map(Cipher::bare),
                            candidates: self.candidates.clone(),
                            alternatives: std::sync::OnceLock::new(),
                        })
                    });
                    sealed.push(Sealed {
                        volume,
                        part: part.shift(-(base as i64)),
                        offset: part.start - span.start,
                        member,
                        keys: Arc::clone(keys),
                        reach: 0,
                    });
                }
            }
        }
        let mut posted = Posted { placed, sealed };
        posted.order();
        posted
    }

    pub(super) fn open_entries(&mut self, out: &mut Vec<weaver_graph::Emit>) {
        use weaver_graph::{Emit, MemberId};
        let Some(mut map) = self.map.take() else {
            return;
        };
        let mut again = Vec::new();
        let mut locked = false;
        for entry in &mut map.entries {
            let (Some(cipher), Some(data), Some(member)) =
                (&mut entry.cipher, entry.data, entry.member)
            else {
                continue;
            };
            let header = ByteRange::new(data - HEADER, data);
            self.kept.want(header);
            if !self.kept.verified(header) {
                if self.kept.read(header).is_some() {
                    self.say_damaged(header, out);
                }
                continue;
            }
            let Some(header) = self
                .kept
                .read(header)
                .and_then(|bytes| bytes.try_into().ok())
            else {
                continue;
            };
            if cipher.header.is_some_and(|previous| previous != header) {
                if self.members[member.0 as usize]
                    .coverage
                    .overlapping(ByteRange::new(0, entry.length))
                    .next()
                    .is_some()
                {
                    // The old keys still reproduce the ciphertext already
                    // posted as plaintext. The engine reads it through them
                    // before opening the repaired header, after a restore too.
                    if !cipher.unlock(&self.candidates, Provenance::Fresh) {
                        locked = true;
                        continue;
                    }
                    self.map = Some(map);
                    return self.give_up(Emit::NeedsEngine(weaver_graph::Why::Unmapped), out);
                }
                cipher.replace_header(header);
            } else {
                cipher.header = Some(header);
            }
            let was_open = cipher.open.is_some();
            if !cipher.unlock(&self.candidates, Provenance::Fresh) {
                locked |= cipher.header.is_some();
                continue;
            }
            let announced = cipher.announce;
            if announced {
                out.push(Emit::Retract(member));
                out.push(Emit::Member {
                    member,
                    name: entry.name.clone(),
                    length: Some(entry.length),
                });
                self.members[member.0 as usize] = super::map::Out::default();
                self.grid.forget(member);
                cipher.announce = false;
            }
            // Plaintext said before the keys opened, a restore's, must be
            // proved to be this candidate's before any key comes from it.
            if !was_open || announced {
                let said = self.members[member.0 as usize].coverage.runs().next();
                Arc::make_mut(cipher.open.as_mut().unwrap()).checked = said.is_none();
            }
            // A repair that waited for its replay across a restore is
            // replayed now the keys are back. Restored plaintext alone
            // goes again only on a failed CRC's retry.
            if !was_open
                && let Some(from) = cipher
                    .pending
                    .iter()
                    .map(|(wanted, _, _)| wanted.start)
                    .min()
            {
                again.push(ByteRange::new(data + from, data + entry.length));
            }
            if entry.length == 0 && entry.crc == 0 && !self.members[member.0 as usize].closed {
                cipher.proven = true;
                self.members[member.0 as usize].closed = true;
                out.push(Emit::MemberClosed(MemberId(member.0)));
            }
        }
        self.map = Some(map);
        for range in again {
            let mut pieces = Vec::new();
            self.to_volumes(range, &mut pieces);
            out.extend(
                pieces
                    .into_iter()
                    .map(|(volume, range)| Emit::Again { volume, range }),
            );
        }
        if locked && !self.asked {
            self.asked = true;
            out.push(Emit::NeedsPassword);
        }
    }

    // No candidate offered reproduces the ciphertext of the plaintext a
    // restored member holds: it cannot be read back, nor can keys be
    // derived from it. The member starts over, the ranges that plaintext
    // stood for are said damaged so the recovery brings their ciphertext
    // again, and a password is asked for.
    fn disown(&mut self, member: weaver_graph::MemberId, out: &mut Vec<weaver_graph::Emit>) {
        use weaver_graph::Emit;
        let Some(map) = &mut self.map else {
            return;
        };
        let index = map.members[member.0 as usize];
        let entry = &mut map.entries[index];
        let (Some(data), Some(cipher)) = (entry.data, &mut entry.cipher) else {
            return;
        };
        let stood: Vec<ByteRange> = self.members[member.0 as usize]
            .coverage
            .runs()
            .map(|run| run.range.shift(data as i64))
            .collect();
        out.push(Emit::Retract(member));
        out.push(Emit::Member {
            member,
            name: entry.name.clone(),
            length: Some(entry.length),
        });
        self.members[member.0 as usize] = super::map::Out::default();
        self.grid.forget(member);
        cipher.announce = false;
        cipher.proven = false;
        cipher.changed();
        // Nothing stands now for a candidate to prove against.
        cipher.unlock(&self.candidates, Provenance::Fresh);
        let mut pieces = Vec::new();
        for range in stood {
            self.to_volumes(range, &mut pieces);
        }
        out.extend(
            pieces
                .into_iter()
                .map(|(volume, range)| Emit::Damaged { volume, range }),
        );
        if !self.asked {
            self.asked = true;
            out.push(Emit::NeedsPassword);
        }
    }

    // Whether some member's keys are proved and stand where ciphertext the
    // object kept starts: it can be deciphered with nothing read.
    pub(super) fn kept_due(&self) -> bool {
        self.map.as_ref().is_some_and(|map| {
            map.entries.iter().any(|entry| {
                let (Some(member), Some(data), Some(cipher)) =
                    (entry.member, entry.data, &entry.cipher)
                else {
                    return false;
                };
                cipher.open.as_ref().is_some_and(|open| {
                    let at = open.reaches();
                    open.checked
                        && !cipher.retry
                        && at < entry.length
                        && !self.kept.from(data + at).is_empty()
                        && !self.members[member.0 as usize]
                            .coverage
                            .covered(ByteRange::at(at, 1))
                })
            })
        })
    }

    pub(super) fn emit_kept_encrypted(
        &mut self,
        read: &mut dyn FnMut(&weaver_graph::Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<weaver_graph::Emit>,
    ) {
        loop {
            let next = self.map.as_ref().and_then(|map| {
                map.entries.iter().find_map(|entry| {
                    let (Some(member), Some(data), Some(cipher)) =
                        (entry.member, entry.data, &entry.cipher)
                    else {
                        return None;
                    };
                    let open = cipher.open.as_ref()?;
                    if cipher.retry || open.reaches() >= entry.length {
                        return None;
                    }
                    let offset = open.reaches();
                    let start = data + offset;
                    let len = (self.kept.from(start).len() as u64).min(entry.length - offset);
                    if len == 0 {
                        return None;
                    }
                    let range = ByteRange::at(start, len);
                    let provenance = if self.kept.verified(range) {
                        Provenance::Fresh
                    } else {
                        Provenance::Damaged
                    };
                    Some((
                        member,
                        offset,
                        self.kept.from(start)[..len as usize].to_vec(),
                        provenance,
                    ))
                })
            });
            let Some((member, offset, bytes, provenance)) = next else {
                return;
            };
            self.decrypt_record(member, offset, bytes.into(), provenance, read, out);
            let reached = self
                .map
                .as_ref()
                .and_then(|map| map.entry(member))
                .and_then(|entry| entry.cipher.as_ref())
                .and_then(|cipher| cipher.open.as_ref())
                .map_or(offset, |open| open.reaches());
            if reached <= offset {
                return;
            }
        }
    }

    pub(super) fn decrypt_record(
        &mut self,
        member: weaver_graph::MemberId,
        offset: u64,
        bytes: weaver_graph::Evidenced,
        provenance: Provenance,
        read: &mut dyn FnMut(&weaver_graph::Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<weaver_graph::Emit>,
    ) {
        use weaver_graph::{Emit, Evidenced, Placement};
        let Some(map) = &mut self.map else {
            return;
        };
        let index = map.members[member.0 as usize];
        let entry = &mut map.entries[index];
        let Some(mut cipher) = entry.cipher.take() else {
            self.record(member, offset, bytes, provenance, out);
            return;
        };
        let coverage = &self.members[member.0 as usize].coverage;
        let mut plain =
            |range, into: &mut Vec<u8>| read(&Placement::Member { member, range }, into);
        // Plaintext already said was produced by some candidate: no key is
        // derived from it until a candidate's keys reproduce the proof.
        let mut disproved = false;
        while let Some(open) = &cipher.open {
            if open.checked {
                break;
            }
            // What of the proof's span is still said: none once a replay
            // uncovered the front, and then there is nothing to prove.
            let said = coverage
                .overlapping(ByteRange::new(0, cipher.proof.len() as u64))
                .next()
                .filter(|run| run.range.start == 0)
                .map_or(0, |run| run.range.end.min(cipher.proof.len() as u64));
            // Plaintext said with none of it in the proof's span, a
            // restore's whose front was uncovered, proves nothing of the
            // candidate that made it, and keys derived through it once the
            // front comes again would be this candidate's fed another's
            // plaintext. It starts over, and this delivery goes to the
            // member as it stands then.
            if said == 0 && entry.data.is_some() && coverage.runs().next().is_some() {
                entry.cipher = Some(cipher);
                self.disown(member, out);
                return self.decrypt_record(member, offset, bytes, provenance, read, out);
            }
            let proof = ByteRange::new(0, said);
            let mut expected = Vec::new();
            if said > 0 && !plain(proof, &mut expected) {
                entry.cipher = Some(cipher);
                return;
            }
            if said == 0 || open.proves(&expected, &cipher.proof[..said as usize]) {
                Arc::make_mut(cipher.open.as_mut().unwrap()).checked = true;
                break;
            }
            disproved = true;
            cipher.rejected.insert(open.candidate);
            cipher.open = None;
            if !cipher.unlock(&self.candidates, Provenance::Fresh) {
                break;
            }
            Arc::make_mut(cipher.open.as_mut().unwrap()).checked = false;
        }
        let Some(open) = &cipher.open else {
            entry.cipher = Some(cipher);
            if disproved {
                return self.disown(member, out);
            }
            if !self.asked {
                self.asked = true;
                out.push(Emit::NeedsPassword);
            }
            return;
        };
        if cipher.retry {
            entry.cipher = Some(cipher);
            return;
        }
        let mut input = std::borrow::Cow::Borrowed(&bytes[..]);
        let range = ByteRange::at(offset, input.len() as u64);
        // Where the replay already asked for starts: a repair at or after
        // it is replayed with it.
        let replay_from = cipher
            .pending
            .iter()
            .map(|(wanted, _, _)| wanted.start)
            .min();
        let mut provenance = provenance;
        for (wanted, replacement, source) in &cipher.pending {
            if let Some(part) = wanted.intersect(range) {
                let from = (part.start - range.start) as usize;
                let at = (part.start - wanted.start) as usize;
                input.to_mut()[from..from + part.len() as usize]
                    .copy_from_slice(&replacement[at..at + part.len() as usize]);
                provenance = crate::crypt::worse(provenance, *source);
            }
        }
        // Bytes said already are tried on a copy of the keys: what they
        // decrypt to replaces nothing until the suffix after them is
        // replayed through the new feedback.
        let trial = coverage.covered(range);
        // The delivery's own provenance, not the chain's: a repair waits
        // as what it is, whatever damage stood before it.
        let source = provenance;
        let Some(decrypted) = open.trial(offset, &input, provenance, coverage, &mut plain) else {
            entry.cipher = Some(cipher);
            return;
        };
        // Capture the old suffix before replacing its prefix. Every byte
        // after a changed ciphertext byte needs the new feedback state.
        // The prefix before the change cannot change: the replay starts
        // at it, and runs to the member's end, as far as anything came.
        if trial {
            let mut previous = Vec::new();
            if plain(range, &mut previous) && previous != decrypted.plain {
                cipher.changed();
                cipher.pending.push((range, input.into_owned(), source));
                let data = entry.data.unwrap();
                let again = ByteRange::new(data + range.start, data + entry.length);
                entry.cipher = Some(cipher);
                if replay_from.is_some_and(|from| from <= range.start) {
                    return;
                }
                // The replay says everything from the change again; the
                // prefix's checksum, from the keys' state at the change,
                // stands for the bytes before it in the member's CRC.
                let prefix = decrypted.before;
                self.grid.offer(
                    member,
                    crate::grid::WHOLE,
                    &weaver_graph::Segment {
                        range: ByteRange::new(0, range.start),
                        crc32: prefix.crc,
                        verified: prefix.provenance.is_verified(),
                    },
                );
                let mut pieces = Vec::new();
                self.to_volumes(again, &mut pieces);
                out.extend(
                    pieces
                        .into_iter()
                        .map(|(volume, range)| Emit::Again { volume, range }),
                );
                return;
            }
        }
        if offset <= cipher.proof.len() as u64 && offset < 32 {
            let end = (offset + input.len() as u64).min(32) as usize;
            cipher.proof.resize(end.max(cipher.proof.len()), 0);
            cipher.proof[offset as usize..end].copy_from_slice(&input[..end - offset as usize]);
        }
        let (bytes, provenance) = Arc::make_mut(cipher.open.as_mut().unwrap()).keep(decrypted);
        // Keep only replacement pieces not consumed by this replay.
        cipher.pending = cipher
            .pending
            .into_iter()
            .flat_map(|(wanted, bytes, source)| {
                let mut rest = Vec::new();
                if wanted.start < range.start {
                    let end = wanted.end.min(range.start);
                    rest.push((
                        ByteRange::new(wanted.start, end),
                        bytes[..(end - wanted.start) as usize].to_vec(),
                        source,
                    ));
                }
                if wanted.end > range.end {
                    let start = wanted.start.max(range.end);
                    rest.push((
                        ByteRange::new(start, wanted.end),
                        bytes[(start - wanted.start) as usize..].to_vec(),
                        source,
                    ));
                }
                rest
            })
            .collect();
        entry.cipher = Some(cipher);
        self.record(member, offset, Evidenced::from(bytes), provenance, out);
    }
}

pub(super) struct Sealed {
    volume: weaver_graph::VolumeIndex,
    part: ByteRange,
    offset: u64,
    member: weaver_graph::MemberId,
    pub(super) keys: Arc<MemberKeys>,
    // The furthest any run of this volume at or before this one reaches:
    // entries' spans may overlap, so `part.end` alone is not in order.
    reach: u64,
}

// What every sealed run of one member reads through: its coverage, its
// keys, the proof they answer, and the other candidates' keys, made only
// when the open ones do not prove the plaintext.
pub(super) struct MemberKeys {
    coverage: Coverage,
    open: Arc<Open>,
    frontier: std::sync::Mutex<Option<(usize, u64, State)>>,
    proof: Vec<u8>,
    bare: Option<Cipher>,
    candidates: Vec<Zeroizing<String>>,
    pub(super) alternatives: std::sync::OnceLock<Vec<Arc<Open>>>,
}

fn cipher_proof(entry: &super::map::Entry) -> Vec<u8> {
    entry
        .cipher
        .as_ref()
        .map(|cipher| cipher.proof.clone())
        .unwrap_or_default()
}

impl MemberKeys {
    // The keys that prove `plain`, the member's front: the open ones, or
    // the first other candidate's that does.
    fn proving(&self, plain: &[u8]) -> Option<&Open> {
        if self.open.proves(plain, &self.proof) {
            return Some(&self.open);
        }
        self.alternatives
            .get_or_init(|| {
                let Some(bare) = &self.bare else {
                    return Vec::new();
                };
                self.candidates
                    .iter()
                    .enumerate()
                    .filter_map(|(candidate, secret)| {
                        let mut cipher = bare.clone();
                        cipher
                            .unlock(std::slice::from_ref(secret), Provenance::Fresh)
                            .then_some(())?;
                        let mut open = Open::clone(cipher.open.as_ref()?);
                        open.candidate = candidate;
                        Some(Arc::new(open))
                    })
                    .collect()
            })
            .iter()
            .map(|open| &**open)
            .find(|open| open.proves(plain, &self.proof))
    }
}

pub(super) struct Posted {
    placed: weaver_graph::Placements,
    pub(super) sealed: Vec<Sealed>,
}

impl Posted {
    // Runs in volume order, each knowing how far the runs before it reach.
    fn order(&mut self) {
        let sealed = &mut self.sealed;
        sealed.sort_by_key(|sealed| (sealed.volume, sealed.part.start, sealed.part.end));
        sealed.dedup_by_key(|sealed| (sealed.volume, sealed.part));
        let mut reach: Option<(weaver_graph::VolumeIndex, u64)> = None;
        for sealed in sealed.iter_mut() {
            let before = reach
                .filter(|(volume, _)| *volume == sealed.volume)
                .map_or(0, |(_, end)| end);
            sealed.reach = before.max(sealed.part.end);
            reach = Some((sealed.volume, sealed.reach));
        }
    }

    // Seal again, as a run of its own, the part of the run at `index`
    // that `part` names, as an entry whose span overlaps another's would.
    #[cfg(test)]
    pub(super) fn seal_within(&mut self, index: usize, part: ByteRange) {
        let run = &self.sealed[index];
        let part = run.part.intersect(part).expect("a part of the run");
        let within = Sealed {
            volume: run.volume,
            part,
            offset: run.offset + part.start - run.part.start,
            member: run.member,
            keys: Arc::clone(&run.keys),
            reach: 0,
        };
        self.sealed.push(within);
        self.order();
    }
}

impl weaver_graph::Posting for Posted {
    fn posted(
        &self,
        volume: weaver_graph::VolumeIndex,
        range: ByteRange,
        read: &mut dyn FnMut(&weaver_graph::Placement, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        use weaver_graph::Placement;
        let mut cursor = range.start;
        // The runs are in volume order and their reach never falls: the
        // first whose reach passes the range's start is found, and every
        // run that meets the range is it or follows it.
        let first = self
            .sealed
            .partition_point(|sealed| (sealed.volume, sealed.reach) <= (volume, range.start));
        for sealed in self.sealed[first..]
            .iter()
            .take_while(|sealed| sealed.volume == volume && sealed.part.start < range.end)
        {
            // Bytes an earlier, overlapping run already read are not read
            // twice.
            let Some(part) = sealed.part.intersect(ByteRange::new(cursor, range.end)) else {
                continue;
            };
            let keys = &sealed.keys;
            if part.start > cursor
                && !self
                    .placed
                    .posted(volume, ByteRange::new(cursor, part.start), read, into)
            {
                return false;
            }
            let local = ByteRange::at(sealed.offset + part.start - sealed.part.start, part.len());
            let mut plain = |range, into: &mut Vec<u8>| {
                read(
                    &Placement::Member {
                        member: sealed.member,
                        range,
                    },
                    into,
                )
            };
            let mut proof = Vec::new();
            let proof_range = ByteRange::new(0, keys.proof.len() as u64);
            if !keys.coverage.covered(proof_range) || !plain(proof_range, &mut proof) {
                return false;
            }
            let Some(open) = keys.proving(&proof) else {
                return false;
            };
            let Ok(mut frontier) = keys.frontier.lock() else {
                return false;
            };
            if !open.seal(&mut frontier, local, &keys.coverage, &mut plain, into) {
                return false;
            }
            cursor = part.end;
        }
        cursor >= range.end
            || self
                .placed
                .posted(volume, ByteRange::new(cursor, range.end), read, into)
    }
}
