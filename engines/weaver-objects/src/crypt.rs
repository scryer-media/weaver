// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// AES-CBC over a stream whose bytes arrive in any order. A block can be
// deciphered as soon as the cipher block before it is in, so a run deciphers
// the moment its seed arrives and a block whose seed has not stays parked.
//
// Nothing here keeps the stream. A deciphered block is dropped once the block
// after it is deciphered too, and kept only where it is still a seed someone
// needs: at the end of a run, every STRIDE bytes inside one, and either side
// of damage, which a repair re-deciphers through. Partial edge blocks are kept
// until they fill, and the final block's plaintext past the payload, the
// padding no member holds, is kept for good. That is all `seal` needs to give
// back exactly the bytes that were posted: it re-enciphers the plaintext the
// caller reads back from the nearest kept seed forward.
//
// Format-blind: offsets are the cipher stream's own, plaintext offsets are
// the same numbers, and the caller says where the payload ends.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use aws_lc_rs::cipher::{
    AES_128, AES_256, DecryptingKey, DecryptionContext, EncryptingKey, EncryptionContext,
    UnboundCipherKey,
};
use aws_lc_rs::iv::FixedLength;
use weaver_graph::{ByteRange, Coverage, Provenance};

pub const BLOCK: u64 = 16;

// How far apart seeds are kept inside a deciphered run: sealing any range
// re-enciphers at most this much plaintext ahead of it.
pub const STRIDE: u64 = 4 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum CryptError {
    #[error("the cipher rejected the key")]
    Key,
    #[error("a key of {0} bytes is neither AES-128 nor AES-256")]
    KeyLength(usize),
    #[error("a cipher stream of {0} bytes is not whole blocks")]
    Unaligned(u64),
    #[error("a payload of {plain} bytes does not fit a cipher stream of {cipher}")]
    Overlong { plain: u64, cipher: u64 },
    #[error("the cipher failed")]
    Cipher,
}

// Plaintext that came out, at its offset, as trustworthy as the worse of its
// own cipher block and the seed it was chained through.
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

// Decipher whole blocks in place, chained from `seed`. The key's length picks
// AES-128 or AES-256.
pub fn decipher(key: &[u8], seed: [u8; 16], blocks: &mut [u8]) -> Result<(), CryptError> {
    let cipher = Keys::new(key)?;
    cipher.decipher(seed, blocks)
}

// The two halves of one key, built once.
struct Keys {
    decrypt: DecryptingKey,
    encrypt: EncryptingKey,
}

impl Keys {
    fn new(key: &[u8]) -> Result<Self, CryptError> {
        let algorithm = match key.len() {
            16 => &AES_128,
            32 => &AES_256,
            other => return Err(CryptError::KeyLength(other)),
        };
        let unbound = || UnboundCipherKey::new(algorithm, key).map_err(|_| CryptError::Key);
        Ok(Self {
            decrypt: DecryptingKey::cbc(unbound()?).map_err(|_| CryptError::Key)?,
            encrypt: EncryptingKey::cbc(unbound()?).map_err(|_| CryptError::Key)?,
        })
    }

    fn decipher(&self, seed: [u8; 16], blocks: &mut [u8]) -> Result<(), CryptError> {
        if blocks.is_empty() {
            return Ok(());
        }
        let context = DecryptionContext::Iv128(FixedLength::from(seed));
        self.decrypt
            .decrypt(blocks, context)
            .map(|_| ())
            .map_err(|_| CryptError::Cipher)
    }

    fn encipher(&self, seed: [u8; 16], blocks: &mut [u8]) -> Result<(), CryptError> {
        if blocks.is_empty() {
            return Ok(());
        }
        let context = EncryptionContext::Iv128(FixedLength::from(seed));
        self.encrypt
            .less_safe_encrypt(blocks, context)
            .map(|_| ())
            .map_err(|_| CryptError::Cipher)
    }
}

const FULL: u16 = u16::MAX;

// One cipher block as far as it arrived, byte by byte.
#[derive(Clone, Copy, Debug)]
struct Block {
    cipher: [u8; 16],
    have: u16,
    damaged: u16,
    repaired: bool,
}

impl Block {
    const EMPTY: Block = Block {
        cipher: [0; 16],
        have: 0,
        damaged: 0,
        repaired: false,
    };

    fn full(&self) -> bool {
        self.have == FULL
    }

    fn provenance(&self) -> Provenance {
        if self.damaged != 0 {
            Provenance::Damaged
        } else if self.repaired {
            Provenance::Repaired
        } else {
            Provenance::Fresh
        }
    }

    // Write bytes at `at` inside the block. Whether what it holds changed,
    // in its bytes or in how far they can be trusted.
    fn write(&mut self, at: usize, bytes: &[u8], provenance: Provenance) -> bool {
        let before = (self.cipher, self.have, self.provenance());
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
        before != (self.cipher, self.have, self.provenance())
    }

    fn holds(&self, within: ByteRange) -> bool {
        (within.start..within.end).all(|index| self.have & (1u16 << index) != 0)
    }
}

pub struct Cbc {
    keys: Keys,
    iv: [u8; 16],
    cipher_len: u64,
    plain_len: u64,
    // Cipher blocks still needed, by offset: parked, partial, or seeds.
    blocks: BTreeMap<u64, Block>,
    // Deciphered blocks, with the provenance their plaintext came out in.
    done: Coverage,
    // The plaintext past the payload, which no member holds, and how much
    // of it came out.
    padding: Vec<u8>,
    padded: Coverage,
}

impl fmt::Debug for Cbc {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Cbc")
            .field("cipher_len", &self.cipher_len)
            .field("plain_len", &self.plain_len)
            .field("kept", &self.blocks.len())
            .finish_non_exhaustive()
    }
}

fn block_at(start: u64) -> ByteRange {
    ByteRange::at(start, BLOCK)
}

fn floor(offset: u64) -> u64 {
    offset - offset % BLOCK
}

fn ceil(offset: u64) -> u64 {
    offset.div_ceil(BLOCK) * BLOCK
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
        if plain_len > cipher_len {
            return Err(CryptError::Overlong {
                plain: plain_len,
                cipher: cipher_len,
            });
        }
        Ok(Self {
            keys: Keys::new(key)?,
            iv,
            cipher_len,
            plain_len,
            blocks: BTreeMap::new(),
            done: Coverage::new(),
            padding: vec![0; (cipher_len - plain_len) as usize],
            padded: Coverage::new(),
        })
    }

    pub fn cipher_len(&self) -> u64 {
        self.cipher_len
    }

    // How many cipher blocks are held, for bounds and tests.
    pub fn kept(&self) -> usize {
        self.blocks.len()
    }

    // Whether every block under this plaintext range has been deciphered.
    pub fn deciphered(&self, range: ByteRange) -> bool {
        range.is_empty()
            || self
                .done
                .covered(ByteRange::new(floor(range.start), ceil(range.end)))
    }

    // Cipher bytes at a stream offset. Plaintext that came out of them, or
    // out of blocks they unparked or re-chained, goes into `out`.
    pub fn deliver(
        &mut self,
        offset: u64,
        bytes: &[u8],
        provenance: Provenance,
        out: &mut Vec<Plain>,
    ) {
        let Some(range) =
            ByteRange::at(offset, bytes.len() as u64).intersect(ByteRange::new(0, self.cipher_len))
        else {
            return;
        };
        let mut dirty = BTreeSet::new();
        let mut cursor = range.start;
        while cursor < range.end {
            let start = floor(cursor);
            let end = (start + BLOCK).min(range.end);
            let piece = &bytes[(cursor - offset) as usize..(end - offset) as usize];
            // A block deciphered and dropped was only dropped because the
            // block after it no longer needed it, which means nothing about
            // it was in doubt: a replay or a repair over it is the same bytes.
            if self.blocks.contains_key(&start) || !self.done.covered(block_at(start)) {
                let block = self.blocks.entry(start).or_insert(Block::EMPTY);
                if block.write((cursor - start) as usize, piece, provenance) {
                    dirty.insert(start);
                }
            }
            cursor = end;
        }
        // A changed block re-chains the one after it.
        let followers: Vec<u64> = dirty
            .iter()
            .map(|start| start + BLOCK)
            .filter(|next| self.blocks.contains_key(next))
            .collect();
        dirty.extend(followers);
        let (first, last) = match (dirty.first(), dirty.last()) {
            (Some(first), Some(last)) => (*first, *last),
            _ => return,
        };
        let mut touched = ByteRange::new(first, last + BLOCK);
        while let Some(start) = dirty.pop_first() {
            if let Some(run) = self.run_from(start, &mut dirty) {
                touched = ByteRange::new(touched.start.min(run.start), touched.end.max(run.end));
                self.decipher_run(run, out);
            }
        }
        self.drop_spent(touched);
    }

    // The longest run of full blocks from `start` that can be deciphered now:
    // its seed is in, and every block in it is new, changed or parked.
    fn run_from(&self, start: u64, dirty: &mut BTreeSet<u64>) -> Option<ByteRange> {
        if !self.blocks.get(&start).is_some_and(Block::full) {
            return None;
        }
        self.seed(start)?;
        let mut end = start + BLOCK;
        while end < self.cipher_len
            && self.blocks.get(&end).is_some_and(Block::full)
            && (dirty.contains(&end) || !self.done.covered(block_at(end)))
        {
            dirty.remove(&end);
            end += BLOCK;
        }
        Some(ByteRange::new(start, end))
    }

    fn seed(&self, start: u64) -> Option<([u8; 16], Provenance)> {
        if start == 0 {
            return Some((self.iv, Provenance::Fresh));
        }
        self.blocks
            .get(&(start - BLOCK))
            .filter(|block| block.full())
            .map(|block| (block.cipher, block.provenance()))
    }

    fn decipher_run(&mut self, run: ByteRange, out: &mut Vec<Plain>) {
        let Some((seed, mut seed_provenance)) = self.seed(run.start) else {
            return;
        };
        let mut plain = Vec::with_capacity(run.len() as usize);
        let mut provenances = Vec::with_capacity((run.len() / BLOCK) as usize);
        for start in (run.start..run.end).step_by(BLOCK as usize) {
            let Some(block) = self.blocks.get(&start) else {
                return;
            };
            plain.extend_from_slice(&block.cipher);
            provenances.push(worse(block.provenance(), seed_provenance));
            seed_provenance = block.provenance();
        }
        if self.keys.decipher(seed, &mut plain).is_err() {
            return;
        }
        let mut index = 0;
        while index < provenances.len() {
            let provenance = provenances[index];
            let mut until = index + 1;
            while until < provenances.len() && provenances[until] == provenance {
                until += 1;
            }
            let piece = ByteRange::new(
                run.start + index as u64 * BLOCK,
                run.start + until as u64 * BLOCK,
            );
            self.done.write(piece, provenance);
            let bytes =
                &plain[(piece.start - run.start) as usize..(piece.end - run.start) as usize];
            self.emit(piece, bytes, provenance, out);
            index = until;
        }
    }

    // Hand plaintext out, keeping what lies past the payload as padding.
    fn emit(
        &mut self,
        piece: ByteRange,
        bytes: &[u8],
        provenance: Provenance,
        out: &mut Vec<Plain>,
    ) {
        let payload_end = piece.end.min(self.plain_len);
        if piece.start < payload_end {
            out.push(Plain {
                offset: piece.start,
                bytes: bytes[..(payload_end - piece.start) as usize].to_vec(),
                provenance,
            });
        }
        if piece.end > self.plain_len {
            let from = self.plain_len.max(piece.start);
            let at = (from - self.plain_len) as usize;
            let tail = &bytes[(from - piece.start) as usize..];
            self.padding[at..at + tail.len()].copy_from_slice(tail);
            self.padded
                .write(ByteRange::new(from, piece.end), Provenance::Fresh);
        }
    }

    // Drop deciphered blocks nobody needs as a seed any more.
    fn drop_spent(&mut self, around: ByteRange) {
        let from = around.start.saturating_sub(BLOCK);
        let spent: Vec<u64> = self
            .blocks
            .range(from..around.end)
            .map(|(start, _)| *start)
            .filter(|start| self.done.covered(block_at(*start)) && !self.needed(*start))
            .collect();
        for start in spent {
            self.blocks.remove(&start);
        }
    }

    fn needed(&self, start: u64) -> bool {
        let next = start + BLOCK;
        next >= self.cipher_len
            || next.is_multiple_of(STRIDE)
            || !self.done.covered(block_at(next))
            || self.came_out_damaged(start)
            || self.came_out_damaged(next)
    }

    fn came_out_damaged(&self, start: u64) -> bool {
        self.done
            .overlapping(block_at(start))
            .any(|run| run.provenance == Provenance::Damaged)
    }

    // The cipher bytes that were posted for `range`, rebuilt from what is
    // kept and from the plaintext `plain` reads back. `false` when a byte in
    // it never arrived or its plaintext cannot be read.
    pub fn seal(
        &self,
        range: ByteRange,
        plain: &mut dyn FnMut(ByteRange, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        if range.is_empty() {
            return true;
        }
        if range.end > self.cipher_len {
            return false;
        }
        let first = floor(range.start);
        let end = ceil(range.end);
        let mut posted: Vec<u8> = Vec::with_capacity((end - first) as usize);
        let mut cursor = first;
        while cursor < end {
            if let Some(block) = self.blocks.get(&cursor) {
                let wanted = ByteRange::new(range.start.max(cursor), range.end.min(cursor + BLOCK))
                    .shift(-(cursor as i64));
                if !block.holds(wanted) {
                    return false;
                }
                posted.extend_from_slice(&block.cipher);
                cursor += BLOCK;
                continue;
            }
            // A stretch that was deciphered and dropped: re-encipher it.
            let stop = self
                .blocks
                .range(cursor..end)
                .next()
                .map_or(end, |(start, _)| *start);
            let Some(bytes) = self.reseal(cursor, stop, &posted, plain) else {
                return false;
            };
            posted.extend_from_slice(&bytes);
            cursor = stop;
        }
        let from = (range.start - first) as usize;
        into.extend_from_slice(&posted[from..from + range.len() as usize]);
        true
    }

    // Re-encipher blocks `[start, stop)`, all deciphered and dropped, from
    // the nearest seed: the block just sealed before them, a kept block, or
    // the IV.
    fn reseal(
        &self,
        start: u64,
        stop: u64,
        posted: &[u8],
        plain: &mut dyn FnMut(ByteRange, &mut Vec<u8>) -> bool,
    ) -> Option<Vec<u8>> {
        let (seed, from) = if posted.len() >= BLOCK as usize {
            let mut seed = [0; 16];
            seed.copy_from_slice(&posted[posted.len() - BLOCK as usize..]);
            (seed, start)
        } else {
            match self.blocks.range(..start).next_back() {
                Some((at, block)) if block.full() => (block.cipher, at + BLOCK),
                Some(_) => return None,
                None => (self.iv, 0),
            }
        };
        if !self.done.covered(ByteRange::new(from, stop)) {
            return None;
        }
        let payload_end = stop.min(self.plain_len);
        let payload = ByteRange::new(from.min(payload_end), payload_end);
        let mut buffer = Vec::with_capacity((stop - from) as usize);
        if !payload.is_empty() && !plain(payload, &mut buffer) {
            return None;
        }
        if buffer.len() as u64 != payload.len() {
            return None;
        }
        if stop > self.plain_len {
            let tail = ByteRange::new(from.max(self.plain_len), stop);
            if !self.padded.covered(tail) {
                return None;
            }
            let at = (tail.start - self.plain_len) as usize;
            buffer.extend_from_slice(&self.padding[at..at + tail.len() as usize]);
        }
        if buffer.len() as u64 != stop - from {
            return None;
        }
        self.keys.encipher(seed, &mut buffer).ok()?;
        Some(buffer.split_off((start - from) as usize))
    }
}

#[cfg(test)]
mod tests;
