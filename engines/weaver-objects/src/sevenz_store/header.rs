// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The container's headers. The signature header is the first 32 bytes and
// says where the end header is; the end header is parsed by sevenz-turbo,
// over a reader that serves only the bytes in hand and says which range it
// missed, so a parse that runs into a hole becomes a bid for that hole.

use std::io::{self, Read, Seek, SeekFrom};

use sevenz_turbo::{Archive, ArchiveLimits, Error, Password};
use weaver_graph::{ByteRange, Coverage, Provenance};

use crate::grid::crc32;
use crate::kept::Kept;

pub(super) const SIGNATURE: ByteRange = ByteRange { start: 0, end: 32 };

pub(super) const MAGIC: [u8; 6] = [b'7', b'z', 0xBC, 0xAF, 0x27, 0x1C];

// How far past a hole a bid reaches: a decoder reads its input in chunks,
// so one bid per chunk would be a round trip per chunk.
const BID_AHEAD: u64 = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(super) struct Start {
    pub(super) end_header: ByteRange,
    pub(super) crc: u32,
}

impl Start {
    // Where the container ends: just after its end header.
    pub(super) fn total(&self) -> u64 {
        self.end_header.end
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Signature {
    // Not a 7z container this object reads.
    Foreign,
    // Its own checksum failed.
    Damaged,
    // A 7z of a major version this object cannot read: its fields lie
    // elsewhere.
    Unreadable,
    Start(Start),
}

pub(super) fn signature(bytes: &[u8]) -> Signature {
    let field = |at: usize| -> [u8; 8] {
        let mut out = [0; 8];
        out.copy_from_slice(&bytes[at..at + 8]);
        out
    };
    let word = |at: usize| -> u32 {
        let mut out = [0; 4];
        out.copy_from_slice(&bytes[at..at + 4]);
        u32::from_le_bytes(out)
    };
    if bytes.len() < 32 || bytes[..6] != MAGIC {
        return Signature::Foreign;
    }
    if crc32(&bytes[12..32]) != word(8) {
        return Signature::Damaged;
    }
    if bytes[6] != 0 {
        return Signature::Unreadable;
    }
    let offset = u64::from_le_bytes(field(12));
    let size = u64::from_le_bytes(field(20));
    let Some(start) = offset.checked_add(SIGNATURE.end) else {
        return Signature::Foreign;
    };
    let Some(end) = start.checked_add(size) else {
        return Signature::Foreign;
    };
    Signature::Start(Start {
        end_header: ByteRange::new(start, end),
        crc: word(28),
    })
}

pub(super) enum Parsed {
    // The parse ran into bytes not in hand.
    Missing(ByteRange),
    // The header is encrypted and this password does not open it.
    Locked,
    // Bytes it read did not arrive verified: wait for their repair before
    // calling anything wrong.
    Unverified,
    // The bytes are all in, verified, and still do not parse.
    Broken,
    Archive {
        archive: Box<Archive>,
        // Every container range the parse read.
        read: Vec<ByteRange>,
    },
}

// Parse the end header out of the bytes in hand, with a password when one is
// offered, held to `limits`. `encrypted` says the header is known to be encrypted, so any
// failure but a hole is that password failing.
pub(super) fn parse(
    kept: &Kept,
    total: u64,
    password: Option<&Password>,
    encrypted: bool,
    limits: &ArchiveLimits,
) -> Parsed {
    let mut reader = Sparse {
        kept,
        len: total,
        position: 0,
        missing: None,
        read: Coverage::new(),
    };
    let empty = Password::empty();
    let result = Archive::read_with_limits(&mut reader, password.unwrap_or(&empty), limits);
    if let Some(missing) = reader.missing {
        return Parsed::Missing(missing);
    }
    let read: Vec<ByteRange> = reader.read.runs().map(|run| run.range).collect();
    let verified = read.iter().all(|range| kept.verified(*range));
    match result {
        Ok(archive) => Parsed::Archive {
            archive: Box::new(archive),
            read,
        },
        Err(_) if !verified => Parsed::Unverified,
        Err(Error::PasswordRequired) => Parsed::Locked,
        Err(Error::MaybeBadPassword(_)) if password.is_some() => Parsed::Locked,
        Err(_) if encrypted && password.is_some() => Parsed::Locked,
        Err(_) => Parsed::Broken,
    }
}

// A reader over the bytes in hand. A read that reaches a hole fails and
// remembers the hole.
struct Sparse<'a> {
    kept: &'a Kept,
    len: u64,
    position: u64,
    missing: Option<ByteRange>,
    read: Coverage,
}

impl Read for Sparse<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() || self.position >= self.len {
            return Ok(0);
        }
        let ahead = self.kept.from(self.position);
        let take = ahead
            .len()
            .min(buffer.len())
            .min((self.len - self.position) as usize);
        if take == 0 {
            let reach = (buffer.len() as u64).max(BID_AHEAD);
            let end = self
                .kept
                .next_in_hand(self.position)
                .unwrap_or(self.len)
                .min(self.position.saturating_add(reach))
                .min(self.len);
            self.missing = Some(ByteRange::new(self.position, end.max(self.position + 1)));
            return Err(io::Error::other("not in hand"));
        }
        buffer[..take].copy_from_slice(&ahead[..take]);
        self.read
            .write(ByteRange::at(self.position, take as u64), Provenance::Fresh);
        self.position += take as u64;
        Ok(take)
    }
}

impl Seek for Sparse<'_> {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        let position = match to {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::End(delta) => self.len.checked_add_signed(delta),
            SeekFrom::Current(delta) => self.position.checked_add_signed(delta),
        };
        self.position = position.ok_or_else(|| io::Error::other("seek out of range"))?;
        Ok(self.position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(offset: u64, size: u64, crc: u32) -> Vec<u8> {
        let mut tail = Vec::new();
        tail.extend_from_slice(&offset.to_le_bytes());
        tail.extend_from_slice(&size.to_le_bytes());
        tail.extend_from_slice(&crc.to_le_bytes());
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&[0, 4]);
        bytes.extend_from_slice(&crc32(&tail).to_le_bytes());
        bytes.extend_from_slice(&tail);
        bytes
    }

    #[test]
    fn the_signature_header_says_where_the_end_header_is() {
        let bytes = header(100, 20, 7);
        assert_eq!(
            signature(&bytes),
            Signature::Start(Start {
                end_header: ByteRange::new(132, 152),
                crc: 7,
            })
        );
        let mut damaged = bytes.clone();
        damaged[20] ^= 1;
        assert_eq!(signature(&damaged), Signature::Damaged);
        let mut newer = bytes.clone();
        newer[6] = 1;
        assert_eq!(signature(&newer), Signature::Unreadable);
        let mut foreign = bytes;
        foreign[0] = b'X';
        assert_eq!(signature(&foreign), Signature::Foreign);
        assert_eq!(signature(&header(u64::MAX, 1, 0)), Signature::Foreign);
    }
}
