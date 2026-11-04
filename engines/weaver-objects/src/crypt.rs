// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// AES-CBC over a stream whose bytes arrive in any order. Block N deciphers
// from block N and block N-1 alone: a run of whole blocks deciphers the
// moment the block before it is in, and until then it waits, staged. The 16
// cipher bytes ending at a run's frontier, at its start, every STRIDE bytes
// inside it, either side of damage and a block an arrival started inside are
// kept as checkpoints.
//
// What came out is the owner's record, read through `Placed`, as is the
// plaintext past the payload, its `Padding`. CBC encryption is deterministic:
// `seal` gives back the posted bytes by re-enciphering that plaintext from
// the nearest checkpoint, and a delivery over blocks that came out gets
// their cipher back the same way, so whatever arrives there is judged by its
// bytes against the ones it replaces. Plaintext offsets are the stream's own.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use unrar_rs::MemberCipherKey;
use weaver_graph::{ByteRange, Coverage, Provenance};

pub const BLOCK: u64 = 16;

// Checkpoint spacing inside a run: the most a ranged seal re-enciphers.
pub const STRIDE: u64 = 4 * 1024 * 1024;

// The block-aligned offset at or below `offset`.
pub fn block_floor(offset: u64) -> u64 {
    offset & !(BLOCK - 1)
}

// The block-aligned offset at or above `offset`. Saturates.
pub fn block_ceil(offset: u64) -> u64 {
    block_floor(offset.saturating_add(BLOCK - 1))
}

#[derive(Debug, thiserror::Error)]
pub enum CryptError {
    #[error("a key of {0} bytes is neither AES-128 nor AES-256")]
    KeyLength(usize),
    #[error("a cipher stream of {0} bytes is not whole blocks")]
    Unaligned(u64),
    #[error("a payload of {plain} bytes does not fit a cipher stream of {cipher}")]
    Overlong { plain: u64, cipher: u64 },
    #[error("the cipher failed")]
    Cipher,
    #[error("a kept chain no cipher could have kept")]
    Chain,
}

// Plaintext that came out, at its offset, as trustworthy as the worse of its
// own cipher block and the predecessor it was chained through.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Plain {
    pub offset: u64,
    pub bytes: Vec<u8>,
    pub provenance: Provenance,
}

pub fn worse(left: Provenance, right: Provenance) -> Provenance {
    match (left, right) {
        (Provenance::Refuted, _) | (_, Provenance::Refuted) => Provenance::Refuted,
        (left, right) if left.is_damaged() || right.is_damaged() => Provenance::Damaged,
        (Provenance::Repaired, _) | (_, Provenance::Repaired) => Provenance::Repaired,
        _ => Provenance::Fresh,
    }
}

// The cipher and its key: the key's length picks AES-128 or AES-256.
fn cipher_key(key: &[u8]) -> Result<MemberCipherKey, CryptError> {
    if let Ok(key) = <[u8; 32]>::try_from(key) {
        return Ok(MemberCipherKey::Aes256(key));
    }
    <[u8; 16]>::try_from(key)
        .map(MemberCipherKey::Aes128)
        .map_err(|_| CryptError::KeyLength(key.len()))
}

// Decipher whole blocks in place, chained from `seed`.
pub fn decipher(key: &[u8], seed: [u8; 16], blocks: &mut [u8]) -> Result<(), CryptError> {
    cipher_key(key)?
        .decrypt_range(&seed, blocks)
        .map_err(|_| CryptError::Cipher)
}

const FULL: u16 = u16::MAX;

// One cipher block as far as it arrived, byte by byte.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Block {
    cipher: [u8; 16],
    have: u16,
    damaged: u16,
    repaired: bool,
    // An arrival started inside it: its bytes before that point came in
    // another, which an un-say from there on does not bring again, so once
    // whole it is kept, and that un-say deciphers it again from here.
    cut: bool,
}

impl Block {
    fn of(cipher: &[u8], provenance: Provenance) -> Block {
        let mut block = Block::default();
        block.write(0, cipher, provenance);
        block
    }

    fn full(&self) -> bool {
        self.have == FULL
    }

    fn provenance(&self) -> Provenance {
        match (self.damaged != 0, self.repaired) {
            (true, _) => Provenance::Damaged,
            (_, true) => Provenance::Repaired,
            _ => Provenance::Fresh,
        }
    }

    // Write bytes at `at` inside the block. Whether what it holds changed,
    // in its bytes or in which of them can be trusted. A block's output is
    // (bytes, provenance): the bytes depend on cipher bytes alone, the
    // provenance on the block's marks, and a change in either is a change
    // the owner must see. So the whole state is compared, marks included.
    fn write(&mut self, at: usize, bytes: &[u8], provenance: Provenance) -> bool {
        let before = *self;
        let mask = (((1u32 << bytes.len()) - 1) << at) as u16;
        self.cipher[at..at + bytes.len()].copy_from_slice(bytes);
        self.have |= mask;
        if provenance.is_damaged() {
            self.damaged |= mask;
        } else {
            self.damaged &= !mask;
        }
        if provenance == Provenance::Repaired {
            self.repaired = true;
        }
        before != *self
    }

    fn holds(&self, within: ByteRange) -> bool {
        (within.start..within.end).all(|index| self.have & (1u16 << index) != 0)
    }

    // The block's marks without its bytes: what blocks alike share.
    fn marks(&self) -> Block {
        Block {
            cipher: [0; 16],
            ..*self
        }
    }
}

// Whole blocks of one arrival that go to the cipher without being staged:
// the marks they all carry, and which of them an earlier arrival cut.
struct Overlay<'a> {
    range: ByteRange,
    cipher: &'a [u8],
    marks: Block,
    // The starts of the cut blocks, in order: each keeps its cut.
    cut: &'a [u64],
}

// Notes the marks of `range` onto the stretches of blocks alike.
fn note(marks: &mut Vec<(ByteRange, Block)>, range: ByteRange, block: Block) {
    match marks.last_mut() {
        Some((stretch, last)) if stretch.end == range.start && *last == block => {
            *stretch = ByteRange::new(stretch.start, range.end);
        }
        _ => marks.push((range, block)),
    }
}

// Work a test counts: cipher blocks staged and decipher calls.
#[cfg(test)]
thread_local! {
    static COUNTED: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

#[cfg(test)]
fn count(staged: usize, deciphered: usize) {
    COUNTED.with(|counted| {
        let (s, d) = counted.get();
        counted.set((s + staged, d + deciphered));
    });
}

// Plaintext bytes a seal enciphered again, for the tests that count it.
#[cfg(test)]
thread_local! {
    static SEALED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// The bytes sealed since the last look.
#[cfg(test)]
pub(crate) fn sealed() -> u64 {
    SEALED.with(|sealed| sealed.replace(0))
}

// What of a cipher stream's plaintext its owner has placed. The owner's
// record is the one ledger of what came out: the cipher reads it and keeps
// none of its own, so a cipher opened again over a restored record knows
// everything the record does.
pub trait Placed {
    // The worst provenance `range` was placed in, or `None` while a byte of
    // it never was.
    fn placed(&self, range: ByteRange) -> Option<Provenance>;

    // Where the first byte of `range` that was placed is, if any was.
    fn first_placed(&self, range: ByteRange) -> Option<u64>;
}

impl Placed for Coverage {
    fn placed(&self, range: ByteRange) -> Option<Provenance> {
        self.covered(range).then(|| {
            self.overlapping(range)
                .map(|run| run.provenance)
                .fold(Provenance::Fresh, worse)
        })
    }

    fn first_placed(&self, range: ByteRange) -> Option<u64> {
        let run = self.overlapping(range).next()?;
        Some(run.range.start.max(range.start))
    }
}

// The plaintext past the payload, which no member holds, each byte's
// arrival, and how it came out. The owner keeps it beside the member's
// record, as facts. It is the user's plaintext: `Debug` prints none of it.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Padding {
    tail_plain: Vec<u8>,
    tail_filled: u16,
    provenance: Option<Provenance>,
}

impl fmt::Debug for Padding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Padding(<redacted>)")
    }
}

impl Padding {
    // Whether every byte of a padding `padding` long is in hand.
    fn whole(&self, padding: u64) -> bool {
        self.tail_filled == (1u16 << padding.min(15)) - 1
    }

    // Keeps the plaintext past the payload's end. `offset` is where `plain`
    // starts; only the part of it at or past `unpacked_size` is kept, at its
    // true position inside the padding, so an out-of-order arrival cannot
    // scramble it.
    fn retain(
        &mut self,
        padding: u64,
        unpacked_size: u64,
        offset: u64,
        plain: &[u8],
        provenance: Provenance,
    ) {
        if padding == 0 {
            return;
        }
        let end = offset.saturating_add(plain.len() as u64);
        if end <= unpacked_size {
            return;
        }
        let from = unpacked_size.max(offset);
        let take = &plain[(from - offset) as usize..];
        if self.tail_plain.len() < padding as usize {
            self.tail_plain.resize(padding as usize, 0);
        }
        let at = (from - unpacked_size) as usize;
        let room = self.tail_plain.len().saturating_sub(at);
        let take = &take[..take.len().min(room)];
        self.tail_plain[at..at + take.len()].copy_from_slice(take);
        // Clamped at the mask's width: a shift past 15 would be a panic, and
        // a header field is not something to take a panic on.
        for index in at..(at + take.len()).min(BLOCK as usize - 1) {
            self.tail_filled |= 1u16 << (index as u32);
        }
        self.provenance = Some(provenance);
    }

    // The plaintext past a payload `unpacked_size` long at stream `range` is
    // not in hand any more: it comes out again when its cipher comes again.
    pub fn forget(&mut self, unpacked_size: u64, range: ByteRange) {
        let from = range.start.max(unpacked_size) - unpacked_size;
        let to = range.end.saturating_sub(unpacked_size).min(BLOCK - 1);
        for index in from..to {
            self.tail_filled &= !(1u16 << index);
        }
    }
}

#[derive(Clone)]
pub struct Cbc {
    key: MemberCipherKey,
    chain: Chain,
}

// A cipher's state without its key. A restart keeps this, and the key comes
// back from the password.
#[derive(Clone, Serialize, Deserialize)]
pub struct Chain {
    iv: [u8; 16],
    cipher_len: u64,
    plain_len: u64,
    // Cipher not yet deciphered, by the offset its block starts at.
    staged: Staged,
    // Whole cipher blocks, by the offset each ends at.
    checkpoints: BTreeMap<u64, Block>,
}

impl fmt::Debug for Cbc {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Cbc")
            .field("cipher_len", &self.chain.cipher_len)
            .field("plain_len", &self.chain.plain_len)
            .field("kept", &self.kept())
            .finish_non_exhaustive()
    }
}

// Cipher not yet deciphered, kept as runs of consecutive blocks, each run by
// the offset its first block starts at. Runs never overlap and may abut. An
// arrival stages its blocks onto the end of one run, and a run that deciphers
// leaves as one stretch, so the cost of staging is a run's, not a block's.
// Kept, it is the map of each block by its offset, as it always was.
#[derive(Clone, Default)]
struct Staged {
    runs: BTreeMap<u64, VecDeque<Block>>,
}

fn run_end(start: u64, run: &VecDeque<Block>) -> u64 {
    start + run.len() as u64 * BLOCK
}

impl Staged {
    // The run holding the block at `at`, and its start.
    fn run_at(&self, at: u64) -> Option<(u64, &VecDeque<Block>)> {
        let (start, run) = self.runs.range(..=at).next_back()?;
        (at < run_end(*start, run)).then_some((*start, run))
    }

    fn get(&self, at: u64) -> Option<&Block> {
        let (start, run) = self.run_at(at)?;
        run.get(((at - start) / BLOCK) as usize)
    }

    fn contains(&self, at: u64) -> bool {
        self.run_at(at).is_some()
    }

    // Stages the block at `at`, over one staged there before.
    fn insert(&mut self, at: u64, block: Block) {
        #[cfg(test)]
        count(1, 0);
        if let Some((start, run)) = self.runs.range_mut(..=at).next_back() {
            let end = run_end(*start, run);
            if at < end {
                run[((at - *start) / BLOCK) as usize] = block;
                return;
            }
            if at == end {
                run.push_back(block);
                return;
            }
        }
        self.runs.insert(at, VecDeque::from([block]));
    }

    // Where the run of whole staged blocks from `start` ends, across runs
    // that abut; `start` when the block there is not staged whole.
    fn whole_from(&self, start: u64) -> u64 {
        let mut end = start;
        while let Some((first, run)) = self.run_at(end) {
            let from = ((end - first) / BLOCK) as usize;
            let whole = run.range(from..).take_while(|block| block.full()).count();
            end += whole as u64 * BLOCK;
            if from + whole < run.len() {
                break;
            }
        }
        end
    }

    // The cipher of the blocks of `range` onto `cipher`, each block's marks
    // to `each`; `None` when one is not staged.
    fn copy(
        &self,
        range: ByteRange,
        cipher: &mut Vec<u8>,
        each: &mut dyn FnMut(u64, &Block),
    ) -> Option<()> {
        let mut at = range.start;
        while at < range.end {
            let (start, run) = self.run_at(at)?;
            let from = ((at - start) / BLOCK) as usize;
            let take = (run.len() - from).min(((range.end - at) / BLOCK) as usize);
            (take > 0).then_some(())?;
            for block in run.range(from..from + take) {
                cipher.extend_from_slice(&block.cipher);
                each(at, block);
                at += BLOCK;
            }
        }
        Some(())
    }

    // Unstages the blocks of `range`, keeping the rest of every run it cuts.
    fn remove(&mut self, range: ByteRange) {
        let starts: Vec<u64> = (self.runs.range(..range.end).rev())
            .take_while(|(start, run)| run_end(**start, run) > range.start)
            .map(|(start, _)| *start)
            .collect();
        for start in starts {
            let Some(mut run) = self.runs.remove(&start) else {
                continue;
            };
            let end = run_end(start, &run);
            if end > range.end {
                let tail = run.split_off(((range.end - start) / BLOCK) as usize);
                self.runs.insert(range.end, tail);
            }
            if start < range.start {
                run.truncate(((range.start - start) / BLOCK) as usize);
                self.runs.insert(start, run);
            }
        }
    }

    // The greatest staged offset in `[from, to]`.
    fn last_in(&self, from: u64, to: u64) -> Option<u64> {
        let (start, run) = self.runs.range(..=to).next_back()?;
        let last = to.min(run_end(*start, run) - BLOCK);
        (last >= from).then_some(last)
    }

    // The least staged offset in `[from, below)`.
    fn first_in(&self, from: u64, below: u64) -> Option<u64> {
        let first = match self.run_at(from) {
            Some(_) => from,
            None => *self.runs.range(from..).next()?.0,
        };
        (first < below).then_some(first)
    }

    fn iter(&self) -> impl Iterator<Item = (u64, &Block)> + '_ {
        (self.runs.iter()).flat_map(|(start, run)| (*start..).step_by(BLOCK as usize).zip(run))
    }

    fn len(&self) -> usize {
        self.runs.values().map(VecDeque::len).sum()
    }
}

impl Serialize for Staged {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.iter())
    }
}

impl<'de> Deserialize<'de> for Staged {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let blocks = BTreeMap::<u64, Block>::deserialize(deserializer)?;
        let mut staged = Staged::default();
        for (at, block) in blocks {
            // A block off the grid stays a run of its own, where `unseal`
            // sees it and refuses the chain.
            match at % BLOCK {
                0 => staged.insert(at, block),
                _ => {
                    staged.runs.insert(at, VecDeque::from([block]));
                }
            }
        }
        Ok(staged)
    }
}

fn block_at(start: u64) -> ByteRange {
    ByteRange::at(start, BLOCK)
}

// What has come out, as one call sees it: the owner's record and padding,
// and on top of them what this call deciphered.
struct Seen<'a> {
    placed: &'a dyn Placed,
    now: Coverage,
    padding: Padding,
    plain_len: u64,
    cipher_len: u64,
}

impl Seen<'_> {
    // Whether a block of `range` may have come out: one did only where every
    // payload byte of it was placed, and a block past the payload is never
    // judged here.
    fn may_have_come_out(&self, range: ByteRange) -> bool {
        let payload = ByteRange::new(range.start, range.end.min(self.plain_len).max(range.start));
        payload.is_empty()
            || self.placed.first_placed(payload).is_some()
            || self.now.first_placed(payload).is_some()
    }

    // How the plaintext of whole blocks `range` came out, or `None` while a
    // byte of it has not.
    fn came_out(&self, range: ByteRange) -> Option<Provenance> {
        if range.end > self.cipher_len {
            return None;
        }
        let mut worst = Provenance::Fresh;
        let payload_end = range.end.min(self.plain_len);
        if range.start < payload_end {
            let payload = ByteRange::new(range.start, payload_end);
            let mut cursor = payload.start;
            for run in self.now.overlapping(payload) {
                let Some(part) = run.range.intersect(payload) else {
                    continue;
                };
                if part.start > cursor {
                    let before = self.placed.placed(ByteRange::new(cursor, part.start))?;
                    worst = worse(worst, before);
                }
                worst = worse(worst, run.provenance);
                cursor = part.end;
            }
            if cursor < payload_end {
                let rest = self.placed.placed(ByteRange::new(cursor, payload_end))?;
                worst = worse(worst, rest);
            }
        }
        if range.end > self.plain_len {
            if !self.padding.whole(self.cipher_len - self.plain_len) {
                return None;
            }
            worst = worse(worst, self.padding.provenance?);
        }
        Some(worst)
    }
}

impl Cbc {
    pub fn new(
        key: &[u8],
        iv: [u8; 16],
        cipher_len: u64,
        plain_len: u64,
    ) -> Result<Self, CryptError> {
        if !cipher_len.is_multiple_of(BLOCK) {
            return Err(CryptError::Unaligned(cipher_len));
        }
        if block_ceil(plain_len) != cipher_len {
            return Err(CryptError::Overlong {
                plain: plain_len,
                cipher: cipher_len,
            });
        }
        let (staged, checkpoints) = (Staged::default(), BTreeMap::new());
        let chain = Chain {
            iv,
            cipher_len,
            plain_len,
            staged,
            checkpoints,
        };
        let key = cipher_key(key)?;
        Ok(Self { key, chain })
    }

    pub fn chain(&self) -> Chain {
        self.chain.clone()
    }

    // The cipher again, keyed, at the state it was kept in. A chain no cipher
    // could have kept is refused.
    pub fn unseal(key: &[u8], chain: Chain) -> Result<Self, CryptError> {
        let key = Self::new(key, chain.iv, chain.cipher_len, chain.plain_len)?.key;
        let len = chain.cipher_len;
        let staged = (chain.staged.iter()).all(|(at, _)| at % BLOCK == 0 && at < len);
        let checkpoints = (chain.checkpoints.iter())
            .all(|(at, block)| at % BLOCK == 0 && *at > 0 && *at <= len && block.full());
        if !staged || !checkpoints {
            return Err(CryptError::Chain);
        }
        Ok(Self { key, chain })
    }

    pub fn cipher_len(&self) -> u64 {
        self.chain.cipher_len
    }

    // How many cipher blocks are held, for bounds and tests.
    pub fn kept(&self) -> usize {
        self.chain.staged.len() + self.chain.checkpoints.len()
    }

    // How many runs the staged blocks are kept in.
    #[cfg(test)]
    fn staged_runs(&self) -> usize {
        self.chain.staged.runs.len()
    }

    // Cipher bytes at a stream offset, with their provenance, over what the
    // owner has `placed` and its `padding`. The plaintext that came out goes
    // into `out` for the owner to place, the padding that came out into
    // `padding`. `plain` reads placed plaintext back, as for `seal`.
    pub fn deliver(
        &mut self,
        placed: &dyn Placed,
        padding: &mut Padding,
        arrival: (u64, &[u8], Provenance),
        plain: &mut dyn FnMut(ByteRange, &mut Vec<u8>) -> bool,
        out: &mut Vec<Plain>,
    ) {
        let mut seen = self.chain.seen(placed, padding);
        self.chain
            .deliver(&self.key, &mut seen, arrival, plain, out);
        *padding = seen.padding;
    }

    // The cipher bytes posted for `range`, from what is staged and the
    // plaintext `plain` reads back, chained from the nearest seed. `false`
    // when a byte never arrived or its plaintext cannot be read.
    pub fn seal(
        &self,
        placed: &dyn Placed,
        padding: &Padding,
        range: ByteRange,
        plain: &mut dyn FnMut(ByteRange, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        self.seal_on(&Frontier::default(), placed, padding, range, plain, into)
    }

    // The same, chained on from where the last seal through `frontier`
    // ended when that is nearer than every seed the chain keeps: a sweep
    // of ranges in order enciphers each byte once.
    pub fn seal_on(
        &self,
        frontier: &Frontier,
        placed: &dyn Placed,
        padding: &Padding,
        range: ByteRange,
        plain: &mut dyn FnMut(ByteRange, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        if range.is_empty() {
            return true;
        }
        let seen = self.chain.seen(placed, padding);
        let reached = (self.chain)
            .seal(&self.key, &seen, range, frontier.get(), plain, into)
            .map(|reached| frontier.set(reached));
        reached.is_some()
    }
}

impl Chain {
    // Whether this chain was kept by a cipher over a stream of this shape.
    pub fn fits(&self, cipher_len: u64, plain_len: u64) -> bool {
        self.cipher_len == cipher_len && self.plain_len == plain_len
    }

    fn seen<'a>(&self, placed: &'a dyn Placed, padding: &Padding) -> Seen<'a> {
        let (plain_len, cipher_len) = (self.plain_len, self.cipher_len);
        let (now, padding) = (Coverage::new(), padding.clone());
        Seen {
            placed,
            now,
            padding,
            plain_len,
            cipher_len,
        }
    }

    fn deliver(
        &mut self,
        key: &MemberCipherKey,
        seen: &mut Seen<'_>,
        (offset, bytes, provenance): (u64, &[u8], Provenance),
        plain: &mut dyn FnMut(ByteRange, &mut Vec<u8>) -> bool,
        out: &mut Vec<Plain>,
    ) {
        let whole = ByteRange::new(0, self.cipher_len);
        let Some(range) = ByteRange::at(offset, bytes.len() as u64).intersect(whole) else {
            return;
        };
        let first = block_floor(range.start);
        let around = ByteRange::new(first.saturating_sub(BLOCK), block_ceil(range.end) + BLOCK);
        let around = around.intersect(whole).unwrap_or(range);
        // The arrival's whole blocks go to the cipher straight from the
        // arrival, written as given, when none of them is staged and every
        // cipher filed for them may go: a fresh copy over blocks none of
        // which came out, or a repair over anything. Only its edges, and a
        // first block whose predecessor is not in, are staged block by block,
        // and only around them is a cipher that came out filed first.
        let mid = ByteRange::new(
            block_ceil(range.start),
            block_floor(range.end).max(block_ceil(range.start)),
        );
        let mut cut = Vec::new();
        let open = !mid.is_empty()
            && provenance.is_verified()
            && self.staged.first_in(mid.start, mid.end).is_none()
            && (provenance == Provenance::Repaired || !seen.may_have_come_out(mid))
            && (self.checkpoints.range(mid.start + BLOCK..=mid.end))
                .all(|(_, filed)| provenance == Provenance::Repaired || !filed.repaired);
        if open {
            self.file_ciphers(key, seen, ByteRange::new(around.start, mid.start), plain);
            self.file_ciphers(key, seen, ByteRange::new(mid.end, around.end), plain);
            let filed: Vec<u64> = (self.checkpoints.range(mid.start + BLOCK..=mid.end))
                .map(|(&at, _)| at)
                .collect();
            for at in filed {
                if self.checkpoints.remove(&at).is_some_and(|block| block.cut) {
                    cut.push(at - BLOCK);
                }
            }
        } else {
            self.file_ciphers(key, seen, around, plain);
        }
        let arrival = (offset, bytes, provenance);
        let mut changed = Vec::new();
        let mut overlay = None;
        if open {
            self.stage(
                seen,
                ByteRange::new(range.start, mid.start),
                arrival,
                &mut changed,
            );
            let mut lead = mid;
            if self.preceding_block(mid.start).is_none() {
                // Its first block waits for the one before it, and seeds the
                // rest.
                let at = (mid.start - offset) as usize;
                let mut block = Block::of(&bytes[at..at + BLOCK as usize], provenance);
                if cut.first() == Some(&mid.start) {
                    cut.remove(0);
                    block.cut = true;
                }
                self.staged.insert(mid.start, block);
                changed.push(mid.start);
                lead = ByteRange::new(mid.start + BLOCK, mid.end);
            }
            if !lead.is_empty() {
                overlay = Some(lead);
            }
            self.stage(
                seen,
                ByteRange::new(mid.end, range.end),
                arrival,
                &mut changed,
            );
        } else {
            self.stage(seen, range, arrival, &mut changed);
        }
        // A changed block re-chains the one after it, staged or out. Both
        // are in offset order, each once. The arrival's last whole block
        // re-chains the one after it the same way.
        let mut dirty = Vec::with_capacity(changed.len() + 2);
        let last = overlay
            .filter(|_| mid.end == range.end)
            .map(|_| mid.end - BLOCK);
        let changed = changed.into_iter().chain(last);
        for start in changed {
            if dirty.last() != Some(&start) && Some(start) != last {
                dirty.push(start);
            }
            let next = start + BLOCK;
            if self.staged.contains(next) {
                dirty.push(next);
            } else if let Some(follower) = self.checkpoints.remove(&(next + BLOCK)) {
                self.staged.insert(next, follower);
                dirty.push(next);
            }
        }
        // The run the arrival's whole blocks decipher in starts at the block
        // before them when that one deciphers now too.
        let mut overlay = overlay.map(|lead| {
            let head = lead.start.checked_sub(BLOCK);
            let start = match head {
                Some(head)
                    if dirty.binary_search(&head).is_ok() && self.run_from(head).is_some() =>
                {
                    head
                }
                _ => lead.start,
            };
            (start, lead)
        });
        let mut touched = around;
        let mut pending = dirty.into_iter().peekable();
        loop {
            let next = pending.peek().copied();
            if let Some((start, lead)) = overlay
                && next.is_none_or(|next| next >= start)
            {
                overlay = None;
                let run = ByteRange::new(start, self.staged.whole_from(lead.end));
                while pending.next_if(|next| *next < run.end).is_some() {}
                touched = ByteRange::new(touched.start.min(run.start), touched.end.max(run.end));
                let cipher = &bytes[(lead.start - offset) as usize..(lead.end - offset) as usize];
                let marks = Block::of(&[0; 16], provenance);
                let lead = Overlay {
                    range: lead,
                    cipher,
                    marks,
                    cut: &cut,
                };
                self.decrypt_range(key, seen, run, Some(lead), out);
                continue;
            }
            let Some(start) = pending.next() else {
                break;
            };
            if let Some(run) = self.run_from(start) {
                // The run took every dirty block inside it.
                while pending.next_if(|next| *next < run.end).is_some() {}
                touched = ByteRange::new(touched.start.min(run.start), touched.end.max(run.end));
                self.decrypt_range(key, seen, run, None, out);
            }
        }
        self.prune_checkpoints(seen, touched);
    }

    // Stages the blocks of `part` of an arrival, each over what it held
    // before, while it has not come out or once it changes, in its bytes or
    // in how far they can be trusted: the cipher of one that came out was
    // filed already. The blocks staged go onto `changed`, in offset order.
    fn stage(
        &mut self,
        seen: &Seen<'_>,
        part: ByteRange,
        (offset, bytes, provenance): (u64, &[u8], Provenance),
        changed: &mut Vec<u64>,
    ) {
        let mut cursor = part.start;
        while cursor < part.end {
            let start = block_floor(cursor);
            let end = (start + BLOCK).min(part.end);
            let piece = &bytes[(cursor - offset) as usize..(end - offset) as usize];
            let at = (cursor - start) as usize;
            cursor = end;
            let next = start + BLOCK;
            let filed = self.checkpoints.get(&next).copied();
            let mut block = self
                .staged
                .get(start)
                .copied()
                .or(filed)
                .unwrap_or_default();
            let wrote = block.write(at, piece, provenance);
            block.cut |= at != 0;
            if wrote || seen.came_out(block_at(start)).is_none() {
                if filed.is_some() {
                    self.checkpoints.remove(&next);
                }
                self.staged.insert(start, block);
                changed.push(start);
            } else if let Some(kept) = self.checkpoints.get_mut(&next) {
                // Unchanged and out: the cipher filed for it keeps the cut.
                kept.cut = block.cut;
            }
        }
    }

    // Before a byte of `around` changes, the cipher of each of its blocks
    // that came out is in hand, as a checkpoint: one kept, or the block's
    // plaintext enciphered again, which gives back exactly the cipher it was
    // deciphered from. A block that changes deciphers from the one before it
    // and re-chains the one after it, and both are put back here while the
    // chain they were enciphered on is still the posted one.
    fn file_ciphers(
        &mut self,
        key: &MemberCipherKey,
        seen: &Seen<'_>,
        around: ByteRange,
        plain: &mut dyn FnMut(ByteRange, &mut Vec<u8>) -> bool,
    ) {
        let missing = |chain: &Chain, start: u64| {
            !chain.staged.contains(start)
                && !chain.checkpoints.contains_key(&(start + BLOCK))
                && seen.came_out(block_at(start)).is_some()
        };
        // Below the payload's end, a block came out only where a byte of it
        // was placed, so the blocks before the next placed byte are skipped.
        let skip = |start: u64| {
            let payload = ByteRange::new(start, around.end.min(seen.plain_len).max(start));
            let placed = [
                seen.placed.first_placed(payload),
                seen.now.first_placed(payload),
            ];
            let next = placed.into_iter().flatten().min().unwrap_or(payload.end);
            start.max(block_floor(next))
        };
        let mut start = around.start;
        while start < around.end {
            if !missing(self, start) {
                start = skip(start + BLOCK);
                continue;
            }
            let mut end = start + BLOCK;
            while end < around.end && missing(self, end) {
                end += BLOCK;
            }
            let mut cipher = Vec::new();
            if self
                .seal(
                    key,
                    seen,
                    ByteRange::new(start, end),
                    None,
                    plain,
                    &mut cipher,
                )
                .is_some()
            {
                for (block, at) in cipher
                    .chunks_exact(BLOCK as usize)
                    .zip((start..).step_by(16))
                {
                    let provenance = seen.came_out(block_at(at)).unwrap_or(Provenance::Damaged);
                    self.checkpoints
                        .insert(at + BLOCK, Block::of(block, provenance));
                }
            }
            start = end;
        }
    }

    // The longest run of whole staged blocks from `start` whose predecessor
    // is in.
    fn run_from(&self, start: u64) -> Option<ByteRange> {
        self.staged.get(start).filter(|block| block.full())?;
        self.preceding_block(start)?;
        Some(ByteRange::new(start, self.staged.whole_from(start)))
    }

    // The CBC predecessor of the block starting at `block_start`: the IV at
    // offset 0, a checkpoint ending there, or the whole staged block before
    // it.
    fn preceding_block(&self, block_start: u64) -> Option<Block> {
        if block_start == 0 {
            return Some(Block::of(&self.iv, Provenance::Fresh));
        }
        let staged = || {
            self.staged
                .get(block_start - BLOCK)
                .filter(|block| block.full())
        };
        self.checkpoints.get(&block_start).or_else(staged).copied()
    }

    // Deciphers one run, its staged blocks and the arrival's whole blocks
    // `overlay` names, in one call over one buffer, filing its checkpoints
    // before the cipher is gone.
    fn decrypt_range(
        &mut self,
        key: &MemberCipherKey,
        seen: &mut Seen<'_>,
        run: ByteRange,
        overlay: Option<Overlay<'_>>,
        out: &mut Vec<Plain>,
    ) -> Option<()> {
        let preceding = self.preceding_block(run.start)?;
        let (start, len) = (run.start, run.len());
        let mut cipher = Vec::with_capacity(len as usize);
        // The marks of the run's blocks, in stretches of blocks alike.
        let mut marks = Vec::new();
        let (before, after) = match &overlay {
            Some(lead) => (
                ByteRange::new(start, lead.range.start),
                ByteRange::new(lead.range.end, run.end),
            ),
            None => (run, ByteRange::at(run.end, 0)),
        };
        let mut each = |at: u64, block: &Block| note(&mut marks, block_at(at), block.marks());
        self.staged.copy(before, &mut cipher, &mut each)?;
        if let Some(lead) = &overlay {
            cipher.extend_from_slice(lead.cipher);
            let cut = Block {
                cut: true,
                ..lead.marks
            };
            let mut at = lead.range.start;
            for &block in lead.cut.iter().chain([&lead.range.end]) {
                if block > at {
                    note(&mut marks, ByteRange::new(at, block), lead.marks);
                }
                if block < lead.range.end {
                    note(&mut marks, block_at(block), cut);
                    at = block + BLOCK;
                }
            }
        }
        let mut each = |at: u64, block: &Block| note(&mut marks, block_at(at), block.marks());
        self.staged.copy(after, &mut cipher, &mut each)?;
        // The run's frontier block, its own predecessor, and one block per
        // stride boundary the range crosses, each under the offset it ends at,
        // so a lookup keyed by a block start finds the predecessor it needs.
        let mut kept = vec![start + len];
        if start > 0 {
            kept.push(start);
        }
        let mut at = start.div_ceil(STRIDE).saturating_mul(STRIDE).max(STRIDE);
        while at < start + len {
            if at >= start + BLOCK {
                kept.push(at);
            }
            at = at.saturating_add(STRIDE);
        }
        // Either side of a block that comes out damaged, so a repair can
        // re-decipher it and the block it seeds; a cut block itself. A
        // block's provenance is the worse of its own and its seed's, so in a
        // stretch of blocks alike only the first can differ from the rest.
        let mut pieces: Vec<(ByteRange, Provenance)> = Vec::new();
        let mut seed = preceding.provenance();
        for (stretch, block) in &marks {
            let own = block.provenance();
            let head = ByteRange::at(stretch.start, BLOCK);
            let rest = ByteRange::new(head.end, stretch.end);
            for (part, provenance) in [(head, worse(own, seed)), (rest, own)] {
                if part.is_empty() {
                    continue;
                }
                if provenance.is_damaged() || block.cut {
                    for at in (part.start..part.end).step_by(BLOCK as usize) {
                        kept.push(at + BLOCK);
                        if provenance.is_damaged() && at > 0 {
                            kept.push(at);
                        }
                    }
                }
                match pieces.last_mut() {
                    Some((piece, last)) if *last == provenance => {
                        *piece = ByteRange::new(piece.start, part.end);
                    }
                    _ => pieces.push((part, provenance)),
                }
            }
            seed = own;
        }
        // Each kept block is the one ending at its offset.
        let block_ending = |end: u64| -> Block {
            if end == start {
                return preceding;
            }
            let at = end - BLOCK;
            let index = marks.partition_point(|(stretch, _)| stretch.end <= at);
            let mut block = marks[index].1;
            let from = (at - start) as usize;
            block
                .cipher
                .copy_from_slice(&cipher[from..from + BLOCK as usize]);
            block
        };
        let filed: Vec<(u64, Block)> = kept
            .into_iter()
            .map(|end| (end, block_ending(end)))
            .collect();
        key.decrypt_range(&preceding.cipher, &mut cipher).ok()?;
        #[cfg(test)]
        count(0, 1);
        self.staged.remove(run);
        self.checkpoints.extend(filed);
        // The payload goes out for the owner to place, the plaintext past it
        // to the owner's padding, grouped by how it came out. A run that
        // came out alike goes out in the buffer it was deciphered in.
        let (plain_len, padding) = (self.plain_len, self.cipher_len - self.plain_len);
        let payload_of = |piece: ByteRange| {
            ByteRange::new(piece.start, piece.end.min(plain_len).max(piece.start))
        };
        if let [(piece, provenance)] = pieces[..] {
            seen.padding
                .retain(padding, plain_len, piece.start, &cipher, provenance);
            let payload = payload_of(piece);
            if !payload.is_empty() {
                seen.now.write(payload, provenance);
                cipher.truncate(payload.len() as usize);
                out.push(Plain {
                    offset: payload.start,
                    bytes: cipher,
                    provenance,
                });
            }
            return Some(());
        }
        for (piece, provenance) in pieces {
            let bytes = &cipher[(piece.start - start) as usize..(piece.end - start) as usize];
            let payload = payload_of(piece);
            if !payload.is_empty() {
                seen.now.write(payload, provenance);
                let offset = piece.start;
                let bytes = bytes[..payload.len() as usize].to_vec();
                out.push(Plain {
                    offset,
                    bytes,
                    provenance,
                });
            }
            seen.padding
                .retain(padding, plain_len, piece.start, bytes, provenance);
        }
        Some(())
    }

    // Whether the checkpoint ending at `at` is still a seed someone needs:
    // at a deciphered run's start or end, every STRIDE bytes inside one,
    // either side of damage, and a cut block.
    fn keep(&self, seen: &Seen<'_>, at: u64) -> bool {
        if self.checkpoints.get(&at).is_some_and(|block| block.cut) {
            return true;
        }
        let before = at
            .checked_sub(BLOCK)
            .and_then(|block| seen.came_out(block_at(block)));
        match (before, seen.came_out(block_at(at))) {
            (Some(before), Some(after)) => {
                at.is_multiple_of(STRIDE) || before.is_damaged() || after.is_damaged()
            }
            _ => true,
        }
    }

    fn prune_checkpoints(&mut self, seen: &Seen<'_>, around: ByteRange) {
        let spent: Vec<u64> = self
            .checkpoints
            .range(around.start..=around.end + BLOCK)
            .map(|(at, _)| *at)
            .filter(|at| !self.keep(seen, *at))
            .collect();
        for at in spent {
            self.checkpoints.remove(&at);
        }
    }

    // The nearest place a re-encryption reaching `block_start` may chain
    // from: the IV at offset 0, or the greatest checkpoint at or below it.
    fn seed(&self, block_start: u64) -> (u64, [u8; 16]) {
        match self.checkpoints.range(..=block_start).next_back() {
            Some((at, block)) => (*at, block.cipher),
            None => (0, self.iv),
        }
    }

    // The posted cipher of `range`, chained from the nearest seed: a kept
    // checkpoint, or `reached`, where a seal of this cipher last left off,
    // when that is nearer. Gives back where this one leaves off: the start
    // of its last block and the cipher before it, when both are known.
    fn seal(
        &self,
        key: &MemberCipherKey,
        seen: &Seen<'_>,
        range: ByteRange,
        reached: Option<Reached>,
        plain: &mut dyn FnMut(ByteRange, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> Option<Option<Reached>> {
        if range.end > self.cipher_len {
            return None;
        }
        let block_start = block_floor(range.start);
        let block_end = block_ceil(range.end);
        let (seeded, seed) = match (self.seed(block_start), reached) {
            (kept, Some(reached)) if kept.0 < reached.0 && reached.0 <= block_start => reached,
            (kept, _) => kept,
        };
        // A staged block is posted as it came, whatever is before it: the
        // walk starts at the last one at or below the range when it is past
        // the seed, and the gap behind it is never enciphered again.
        let mut cursor = self.staged.last_in(seeded, block_start).unwrap_or(seeded);
        let mut preceding = Some(seed);
        // The posted cipher before the last block walked: the next seal
        // chains on from the start of that block, so one that starts in it,
        // as the next slice of a volume does when the cipher stream is not
        // aligned to the slices, still chains on.
        let mut trail = None;
        let mut posted: Vec<u8> = Vec::with_capacity((block_end - block_start) as usize);
        while cursor < block_end {
            if let Some(block) = self.staged.get(cursor) {
                if cursor >= block_start {
                    let wanted =
                        ByteRange::new(range.start.max(cursor), range.end.min(cursor + BLOCK));
                    block.holds(wanted.shift(-(cursor as i64))).then_some(())?;
                    posted.extend_from_slice(&block.cipher);
                }
                trail = preceding;
                preceding = block.full().then_some(block.cipher);
                cursor += BLOCK;
                continue;
            }
            let mut stop = self.staged.first_in(cursor, block_end).unwrap_or(block_end);
            if cursor < block_start {
                stop = stop.min(block_start);
            }
            // The plaintext behind `[cursor, stop)`: the owner's below the
            // payload's end, the padding above it. Refused whenever a byte of
            // it is not really there.
            seen.came_out(ByteRange::new(cursor, stop))?;
            let mut buffer = Vec::with_capacity((stop - cursor) as usize);
            let on_disk = ByteRange::new(cursor, stop.min(self.plain_len).max(cursor));
            if !on_disk.is_empty() && !plain(on_disk, &mut buffer) {
                return None;
            }
            (buffer.len() as u64 == on_disk.len()).then_some(())?;
            if stop > self.plain_len {
                let take = (stop - self.plain_len) as usize;
                buffer.extend_from_slice(seen.padding.tail_plain.get(..take)?);
            }
            key.encrypt_range(&preceding?, &mut buffer).ok()?;
            #[cfg(test)]
            SEALED.with(|sealed| sealed.set(sealed.get() + buffer.len() as u64));
            trail = match buffer.len().checked_sub(2 * BLOCK as usize) {
                Some(at) => buffer[at..].first_chunk::<16>().copied(),
                None => preceding,
            };
            preceding = buffer.last_chunk::<16>().copied();
            if cursor >= block_start {
                posted.extend_from_slice(&buffer);
            }
            cursor = stop;
        }
        let from = (range.start - block_start) as usize;
        into.extend_from_slice(&posted[from..from + range.len() as usize]);
        Some(
            preceding
                .and(trail)
                .map(|cipher| (block_end - BLOCK, cipher)),
        )
    }
}

// An offset in the cipher stream and the posted cipher of the block that
// ends there: what a seal chains on from.
type Reached = (u64, [u8; 16]);

// Where the last seal through it ended, for the next to chain on from. It
// holds posted cipher and nothing else, so one that is stale or shared costs
// at most a seed, never a wrong byte: a seal takes it only when it is at or
// below where its range starts and nearer than every kept checkpoint. It
// belongs with one standing of the cipher and the plaintext that came out of
// it, and goes when they do.
#[derive(Default)]
pub struct Frontier(std::sync::Mutex<Option<Reached>>);

impl Frontier {
    fn get(&self) -> Option<Reached> {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn set(&self, reached: Option<Reached>) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = reached;
    }
}

#[cfg(test)]
mod tests;
