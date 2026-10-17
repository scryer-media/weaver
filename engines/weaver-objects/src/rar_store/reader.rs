// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A cursor over header bytes. Every read is checked: a field that runs past
// the bytes in hand is `None`, which the caller treats as a header too short
// for what it says, never as a panic.

pub(super) struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    pub(super) fn position(&self) -> usize {
        self.at
    }

    pub(super) fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.at.checked_add(len)?;
        let taken = self.bytes.get(self.at..end)?;
        self.at = end;
        Some(taken)
    }

    pub(super) fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
        let mut array = [0; N];
        array.copy_from_slice(self.take(N)?);
        Some(array)
    }

    pub(super) fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    pub(super) fn u16(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes(self.array()?))
    }

    pub(super) fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.array()?))
    }

    // RAR5's variable-length integer: seven bits a byte, low first, the top
    // bit saying another byte follows. Ten bytes at most for 64 bits.
    pub(super) fn vint(&mut self) -> Option<u64> {
        let mut value = 0u64;
        for index in 0..10 {
            let byte = self.u8()?;
            value |= u64::from(byte & 0x7f) << (7 * index);
            if byte & 0x80 == 0 {
                return Some(value);
            }
        }
        None
    }
}
