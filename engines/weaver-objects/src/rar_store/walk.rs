// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One volume's headers, read in order from its front. Each step wants the
// next header's first bytes, then the whole header once its size is known,
// checks its CRC and moves past it and its data. Nothing past the cursor is
// ever needed, so a volume that arrives strictly in order is read as it
// arrives. Enciphered headers are read through a key the store supplies.

use weaver_graph::ByteRange;
use zeroize::Zeroizing;

use super::header::{self, Format, HeaderCrypt, Read, Signature, What};
use super::{rar4, rar5};
use crate::crypt::{self, BLOCK};
use crate::grid::crc32;
use crate::kept::Kept;

// A header read, where it lies, and the CRC of the bytes it was read from,
// so a repair that changes them is noticed.
#[derive(Clone, Debug)]
pub(super) struct Block {
    pub(super) header: ByteRange,
    pub(super) data: ByteRange,
    pub(super) what: What,
    pub(super) basis: u32,
}

// How the headers after a point are enciphered.
#[derive(Clone, Copy, Debug)]
pub(super) enum Cipher {
    // A 16-byte IV before each header, the key from the encryption header.
    Rar5(HeaderCrypt),
    // An 8-byte salt before each header, key and IV from it.
    Rar4,
}

// What a header key is asked for.
pub(super) enum KeyFor<'a> {
    Rar5(&'a HeaderCrypt),
    Rar4(&'a [u8; 8]),
}

// A header key, and the IV when the key derivation makes one.
pub(super) struct HeaderKey {
    pub(super) key: Zeroizing<Vec<u8>>,
    pub(super) iv: Option<[u8; 16]>,
}

pub(super) type Keyring<'a> = &'a dyn Fn(KeyFor<'_>) -> Option<HeaderKey>;

#[derive(Debug)]
pub(super) enum Step {
    // The bytes the next header needs are not in yet.
    Wait,
    Moved,
    // A header failed its CRC over this range; these are its bytes' CRC.
    Damaged(ByteRange, u32),
    // The next header is enciphered and no key opens it.
    Locked,
    // The volume does not start with a RAR signature.
    Foreign,
    // A header passed its CRC but makes no sense.
    Broken,
    // The headers say the volume runs on past its length.
    Short(ByteRange),
}

#[derive(Clone, Debug, Default)]
pub(super) struct Walk {
    pub(super) format: Option<Format>,
    pub(super) cursor: u64,
    pub(super) blocks: Vec<Block>,
    pub(super) ended: bool,
    // What the end header said about more volumes.
    pub(super) more: Option<bool>,
    pub(super) cipher: Option<Cipher>,
}

fn align(len: u64) -> u64 {
    len.div_ceil(BLOCK) * BLOCK
}

impl Walk {
    pub(super) fn step(
        &mut self,
        kept: &mut Kept,
        length: Option<u64>,
        keyring: Option<Keyring<'_>>,
    ) -> Step {
        if self.ended {
            return Step::Wait;
        }
        let Some(format) = self.format else {
            return self.signature(kept, length);
        };
        if let Some(length) = length
            && self.cursor >= length
        {
            self.ended = true;
            return match self.cursor > length {
                true => Step::Short(ByteRange::new(length, self.cursor)),
                false => Step::Moved,
            };
        }
        // What precedes an enciphered header, and the least of one.
        let (prefix, least) = match (self.cipher, format) {
            (None, Format::Rar5) => (0, rar5::MIN),
            (None, Format::Rar4) => (0, rar4::MIN),
            (Some(Cipher::Rar5(_)), _) => (16, 16 + BLOCK),
            (Some(Cipher::Rar4), _) => (8, 8 + BLOCK),
        };
        let mut need = least;
        loop {
            let wanted = ByteRange::at(self.cursor, need);
            if let Some(length) = length
                && wanted.end > length
            {
                self.ended = true;
                return Step::Short(ByteRange::new(length, wanted.end));
            }
            kept.want(wanted);
            let Some(bytes) = kept.read(wanted) else {
                return Step::Wait;
            };
            let plain = match self.cipher {
                None => Zeroizing::new(bytes.to_vec()),
                Some(cipher) => {
                    let Some(keyring) = keyring else {
                        return Step::Locked;
                    };
                    match decipher(cipher, bytes, prefix, keyring) {
                        Some(plain) => plain,
                        None => return Step::Locked,
                    }
                }
            };
            let read = match format {
                Format::Rar5 => rar5::read(&plain),
                Format::Rar4 => rar4::read(&plain),
            };
            let span = |len: u64| match self.cipher {
                None => len,
                Some(_) => prefix + align(len),
            };
            match read {
                Read::Short(len) if span(len) > need => need = span(len),
                Read::Short(_) | Read::Broken => return Step::Broken,
                Read::Bad(len) => {
                    let range = ByteRange::at(self.cursor, span(len).max(need));
                    let crc = kept.read(range).map_or(0, crc32);
                    return Step::Damaged(range, crc);
                }
                Read::Header(found) => {
                    let header = ByteRange::at(self.cursor, span(found.len));
                    let Some(raw) = kept.read(header) else {
                        return Step::Wait;
                    };
                    let block = Block {
                        header,
                        data: ByteRange::at(header.end, found.data),
                        what: found.what,
                        basis: crc32(raw),
                    };
                    self.advance(block);
                    return Step::Moved;
                }
            }
        }
    }

    fn signature(&mut self, kept: &mut Kept, length: Option<u64>) -> Step {
        let end = length.map_or(header::RAR5.len() as u64, |length| {
            length.min(header::RAR5.len() as u64)
        });
        let range = ByteRange::new(0, end);
        kept.want(range);
        let Some(bytes) = kept.read(range) else {
            return Step::Wait;
        };
        match header::signature(bytes) {
            Signature::Format(format) => {
                self.format = Some(format);
                self.cursor = format.signature_len();
                Step::Moved
            }
            Signature::Short if length.is_none() => Step::Wait,
            Signature::Short | Signature::Foreign => Step::Foreign,
        }
    }

    fn advance(&mut self, block: Block) {
        self.cursor = block.data.end;
        match &block.what {
            What::Crypt(crypt) => self.cipher = Some(Cipher::Rar5(*crypt)),
            What::Main(main) if main.encrypted => self.cipher = Some(Cipher::Rar4),
            What::End { more } => {
                self.ended = true;
                self.more = Some(*more);
            }
            _ => {}
        }
        self.blocks.push(block);
    }

    // Forget block `index` and everything after it, to read them again.
    pub(super) fn rewind(&mut self, index: usize) {
        let Some(block) = self.blocks.get(index) else {
            return;
        };
        self.cursor = block.header.start;
        self.blocks.truncate(index);
        self.ended = false;
        self.more = None;
        self.cipher = self
            .blocks
            .iter()
            .fold(None, |cipher, block| match &block.what {
                What::Crypt(crypt) => Some(Cipher::Rar5(*crypt)),
                What::Main(main) if main.encrypted => Some(Cipher::Rar4),
                _ => cipher,
            });
    }

    // The first block whose header bytes in `range` are no longer the ones
    // it was read from.
    pub(super) fn changed(&self, kept: &Kept, range: ByteRange) -> Option<usize> {
        self.blocks.iter().position(|block| {
            block.header.intersect(range).is_some()
                && kept
                    .read(block.header)
                    .is_some_and(|bytes| crc32(bytes) != block.basis)
        })
    }

    // The main header, which every volume starts with.
    pub(super) fn main(&self) -> Option<header::Main> {
        self.blocks.iter().find_map(|block| match block.what {
            What::Main(main) => Some(main),
            _ => None,
        })
    }

    // Whether no volume follows this one.
    pub(super) fn is_last(&self) -> bool {
        self.more == Some(false) || self.main().is_some_and(|main| !main.volume)
    }
}

// An enciphered header's plaintext: the IV or salt, then whole blocks.
fn decipher(
    cipher: Cipher,
    bytes: &[u8],
    prefix: u64,
    keyring: Keyring<'_>,
) -> Option<Zeroizing<Vec<u8>>> {
    let prefix = prefix as usize;
    let (key, iv) = match cipher {
        Cipher::Rar5(crypt) => {
            let found = keyring(KeyFor::Rar5(&crypt))?;
            let mut iv = [0; 16];
            iv.copy_from_slice(bytes.get(..16)?);
            (found.key, iv)
        }
        Cipher::Rar4 => {
            let mut salt = [0; 8];
            salt.copy_from_slice(bytes.get(..8)?);
            let found = keyring(KeyFor::Rar4(&salt))?;
            (found.key, found.iv?)
        }
    };
    let body = bytes.get(prefix..)?;
    let whole = body.len() - body.len() % BLOCK as usize;
    let mut plain = Zeroizing::new(body[..whole].to_vec());
    crypt::decipher(&key, iv, &mut plain).ok()?;
    Some(plain)
}
