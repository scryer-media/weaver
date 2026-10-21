// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// AES-CBC over a stream whose bytes arrive in any order. Block N deciphers
// from block N and block N-1 alone: a run of whole blocks deciphers the
// moment the block before it is in, and until then it waits, staged. The 16
// cipher bytes ending at a run's frontier, at its start, every STRIDE bytes
// inside it and either side of damage are kept as checkpoints.
//
// What came out is the owner's record, read through `Placed`, as is the
// plaintext past the payload, its `Padding`. CBC encryption is deterministic:
// `seal` gives back the posted bytes by re-enciphering that plaintext from
// the nearest checkpoint, and a delivery over blocks that came out gets
// their cipher back the same way, so whatever arrives there is judged by its
// bytes against the ones it replaces. Plaintext offsets are the stream's own.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};
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
        (Provenance::Damaged, _) | (_, Provenance::Damaged) => Provenance::Damaged,
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
        for (index, byte) in bytes.iter().enumerate() {
            let bit = 1u16 << (at + index);
            self.cipher[at + index] = *byte;
            self.have |= bit;
            if provenance == Provenance::Damaged {
                self.damaged |= bit;
            } else {
                self.damaged &= !bit;
            }
        }
        if provenance == Provenance::Repaired {
            self.repaired = true;
        }
        before != *self
    }

    fn holds(&self, within: ByteRange) -> bool {
        (within.start..within.end).all(|index| self.have & (1u16 << index) != 0)
    }
}

// What of a cipher stream's plaintext its owner has placed. The owner's
// record is the one ledger of what came out: the cipher reads it and keeps
// none of its own, so a cipher opened again over a restored record knows
// everything the record does.
pub trait Placed {
    // The worst provenance `range` was placed in, or `None` while a byte of
    // it never was.
    fn placed(&self, range: ByteRange) -> Option<Provenance>;
}

impl Placed for Coverage {
    fn placed(&self, range: ByteRange) -> Option<Provenance> {
        self.covered(range).then(|| {
            self.overlapping(range)
                .map(|run| run.provenance)
                .fold(Provenance::Fresh, worse)
        })
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
    staged: BTreeMap<u64, Block>,
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
        let (staged, checkpoints) = (BTreeMap::new(), BTreeMap::new());
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
        let staged = chain.staged.keys().all(|at| at % BLOCK == 0 && *at < len);
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
        let seen = self.chain.seen(placed, padding);
        range.is_empty()
            || self
                .chain
                .seal(&self.key, &seen, range, plain, into)
                .is_some()
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
        self.file_ciphers(key, seen, around, plain);
        // A block is staged, over what it held before, while it has not come
        // out or once it changes, in its bytes or in how far they can be
        // trusted: the cipher of one that came out was filed above.
        let mut changed = BTreeSet::new();
        let mut cursor = range.start;
        while cursor < range.end {
            let start = block_floor(cursor);
            let end = (start + BLOCK).min(range.end);
            let piece = &bytes[(cursor - offset) as usize..(end - offset) as usize];
            let at = (cursor - start) as usize;
            cursor = end;
            let next = start + BLOCK;
            let held = self.staged.get(&start).or(self.checkpoints.get(&next));
            let mut block = held.copied().unwrap_or_default();
            let wrote = block.write(at, piece, provenance);
            if wrote || seen.came_out(block_at(start)).is_none() {
                self.checkpoints.remove(&next);
                self.staged.insert(start, block);
                changed.insert(start);
            }
        }
        // A changed block re-chains the one after it, staged or out.
        let mut dirty = changed.clone();
        for next in changed.iter().map(|start| start + BLOCK) {
            if self.staged.contains_key(&next) {
                dirty.insert(next);
            } else if let Some(follower) = self.checkpoints.get(&(next + BLOCK)).copied() {
                self.checkpoints.remove(&(next + BLOCK));
                self.staged.insert(next, follower);
                dirty.insert(next);
            }
        }
        let mut touched = around;
        while let Some(start) = dirty.pop_first() {
            if let Some(run) = self.run_from(start, &mut dirty) {
                touched = ByteRange::new(touched.start.min(run.start), touched.end.max(run.end));
                self.decrypt_range(key, seen, run, out);
            }
        }
        self.prune_checkpoints(seen, touched);
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
            !chain.staged.contains_key(&start)
                && !chain.checkpoints.contains_key(&(start + BLOCK))
                && seen.came_out(block_at(start)).is_some()
        };
        let mut start = around.start;
        while start < around.end {
            if !missing(self, start) {
                start += BLOCK;
                continue;
            }
            let mut end = start + BLOCK;
            while end < around.end && missing(self, end) {
                end += BLOCK;
            }
            let mut cipher = Vec::new();
            if self
                .seal(key, seen, ByteRange::new(start, end), plain, &mut cipher)
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
    fn run_from(&self, start: u64, dirty: &mut BTreeSet<u64>) -> Option<ByteRange> {
        self.staged.get(&start).filter(|block| block.full())?;
        self.preceding_block(start)?;
        let mut end = start + BLOCK;
        while self.staged.get(&end).is_some_and(Block::full) {
            dirty.remove(&end);
            end += BLOCK;
        }
        Some(ByteRange::new(start, end))
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
                .get(&(block_start - BLOCK))
                .filter(|block| block.full())
        };
        self.checkpoints.get(&block_start).or_else(staged).copied()
    }

    // Deciphers one run of staged blocks, filing its checkpoints before the
    // cipher is gone.
    fn decrypt_range(
        &mut self,
        key: &MemberCipherKey,
        seen: &mut Seen<'_>,
        run: ByteRange,
        out: &mut Vec<Plain>,
    ) -> Option<()> {
        let preceding = self.preceding_block(run.start)?;
        let blocks: Vec<Block> = (run.start..run.end)
            .step_by(BLOCK as usize)
            .filter_map(|start| self.staged.get(&start).copied())
            .collect();
        let mut cipher: Vec<u8> = blocks.iter().flat_map(|block| block.cipher).collect();
        (cipher.len() as u64 == run.len()).then_some(())?;
        key.decrypt_range(&preceding.cipher, &mut cipher).ok()?;
        let (start, len) = (run.start, run.len());
        // The run's frontier block, its own predecessor, and one block per
        // stride boundary the range crosses, each under the offset it ends at,
        // so a lookup keyed by a block start finds the predecessor it needs.
        self.checkpoints
            .insert(start + len, blocks[blocks.len() - 1]);
        if start > 0 {
            self.checkpoints.insert(start, preceding);
        }
        let mut at = start.div_ceil(STRIDE).saturating_mul(STRIDE).max(STRIDE);
        while at < start + len {
            if at >= start + BLOCK {
                self.checkpoints
                    .insert(at, blocks[((at - start) / BLOCK - 1) as usize]);
            }
            at = at.saturating_add(STRIDE);
        }
        // Either side of a block that comes out damaged, so a repair can
        // re-decipher it and the block it seeds.
        let mut seed = preceding;
        let mut provenances = Vec::with_capacity(blocks.len());
        for (index, block) in blocks.iter().enumerate() {
            let provenance = worse(block.provenance(), seed.provenance());
            let at = start + index as u64 * BLOCK;
            if provenance == Provenance::Damaged {
                self.checkpoints.insert(at + BLOCK, *block);
                if at > 0 {
                    self.checkpoints.insert(at, seed);
                }
            }
            self.staged.remove(&at);
            provenances.push(provenance);
            seed = *block;
        }
        // The payload goes out for the owner to place, the plaintext past it
        // to the owner's padding, grouped by how it came out.
        let (plain_len, padding) = (self.plain_len, self.cipher_len - self.plain_len);
        let mut piece = ByteRange::at(start, 0);
        for group in provenances.chunk_by(|left, right| left == right) {
            let provenance = group[0];
            piece = ByteRange::at(piece.end, group.len() as u64 * BLOCK);
            let bytes = &cipher[(piece.start - start) as usize..(piece.end - start) as usize];
            let payload = ByteRange::new(piece.start, piece.end.min(plain_len).max(piece.start));
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
    // at a deciphered run's start or end, every STRIDE bytes inside one, and
    // either side of damage.
    fn keep(&self, seen: &Seen<'_>, at: u64) -> bool {
        let before = at
            .checked_sub(BLOCK)
            .and_then(|block| seen.came_out(block_at(block)));
        match (before, seen.came_out(block_at(at))) {
            (Some(before), Some(after)) => {
                at.is_multiple_of(STRIDE)
                    || before == Provenance::Damaged
                    || after == Provenance::Damaged
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

    fn seal(
        &self,
        key: &MemberCipherKey,
        seen: &Seen<'_>,
        range: ByteRange,
        plain: &mut dyn FnMut(ByteRange, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> Option<()> {
        if range.end > self.cipher_len {
            return None;
        }
        let block_start = block_floor(range.start);
        let block_end = block_ceil(range.end);
        let (mut cursor, seed) = self.seed(block_start);
        let mut preceding = Some(seed);
        let mut posted: Vec<u8> = Vec::with_capacity((block_end - block_start) as usize);
        while cursor < block_end {
            if let Some(block) = self.staged.get(&cursor) {
                if cursor >= block_start {
                    let wanted =
                        ByteRange::new(range.start.max(cursor), range.end.min(cursor + BLOCK));
                    block.holds(wanted.shift(-(cursor as i64))).then_some(())?;
                    posted.extend_from_slice(&block.cipher);
                }
                preceding = block.full().then_some(block.cipher);
                cursor += BLOCK;
                continue;
            }
            let next = self.staged.range(cursor..block_end).next();
            let mut stop = next.map_or(block_end, |(start, _)| *start);
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
            preceding = buffer.last_chunk::<16>().copied();
            if cursor >= block_start {
                posted.extend_from_slice(&buffer);
            }
            cursor = stop;
        }
        let from = (range.start - block_start) as usize;
        into.extend_from_slice(&posted[from..from + range.len() as usize]);
        Some(())
    }
}

#[cfg(test)]
mod tests;
