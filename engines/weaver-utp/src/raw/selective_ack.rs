// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-10.

use bitvec::{BitArr, order::Lsb0, slice::BitSlice};

use crate::constants::SACK_DEPTH;

/// The longest SACK the wire can carry: its length is one byte. libtorrent
/// sizes the SACK it sends to its whole receive buffer, up to the packet
/// (v2.0.15 utp_stream.cpp:1536-1547), and reads every byte of one it
/// receives (utp_stream.cpp:1238-1281).
pub const SACK_MAX_BYTES: usize = u8::MAX as usize;

type SelectiveAckData = BitArr!(for SACK_MAX_BYTES * 8, in u8, Lsb0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectiveAck {
    data: SelectiveAckData,
    // In bits; a whole number of bytes.
    len: usize,
}

impl SelectiveAck {
    /// The SACK Weaver sends: `SACK_DEPTH` bits.
    pub fn new(unacked: impl Iterator<Item = usize>) -> Self {
        let mut data = SelectiveAckData::default();
        for idx in unacked.take_while(|i| *i < SACK_DEPTH) {
            data.set(idx, true);
        }
        Self {
            data,
            len: SACK_DEPTH,
        }
    }

    #[cfg(test)]
    pub fn new_test(acked: impl IntoIterator<Item = usize>) -> Option<Self> {
        let mut data = SelectiveAckData::default();
        let mut count = 0;
        let mut len = SACK_DEPTH;
        for idx in acked {
            data.get_mut(idx).unwrap().set(true);
            count += 1;
            len = len.max((idx / 32 + 1) * 32);
        }
        if count > 0 {
            Some(Self { data, len })
        } else {
            None
        }
    }

    /// How many packets the SACK speaks for, in bits.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.data.as_raw_slice()[..self.len / 8]
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        // The spec says the len must be a multiple of 4, but there's a ton of messages
        // in the wild that are 1 bytes long. So we can
        // thus deserialize any payload. Every byte is read: the length
        // field is one byte, so the data always fits.
        let len = bytes.len().min(SACK_MAX_BYTES);
        let mut data = SelectiveAckData::default();
        data.as_raw_mut_slice()[..len].copy_from_slice(&bytes[..len]);
        Self { data, len: len * 8 }
    }

    pub fn as_bitslice(&self) -> &BitSlice<u8> {
        &self.data.as_bitslice()[..self.len]
    }

    pub fn iter(&self) -> impl Iterator<Item = bool> + '_ {
        self.as_bitslice().iter().map(|b| *b)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use crate::{
        message::UtpMessage,
        raw::{Type::Data, selective_ack::SelectiveAck},
        stream_rx::OutOfOrderQueue,
    };

    fn asm() -> OutOfOrderQueue {
        OutOfOrderQueue::new(NonZeroUsize::new(65).unwrap())
    }

    fn msg() -> UtpMessage {
        UtpMessage::new_test(
            crate::raw::UtpHeader {
                htype: Data,
                ..Default::default()
            },
            b"a",
        )
    }

    #[test]
    fn test_empty_is_none() {
        let asm = asm();
        assert!(asm.selective_ack().is_none());
    }

    #[test]
    fn test_holes() {
        let mut asm = asm();
        asm.add_remove(msg(), 8).unwrap();
        asm.add_remove(msg(), 1).unwrap();
        asm.add_remove(msg(), 2).unwrap();
        asm.add_remove(msg(), 64).unwrap();

        let sack = asm.selective_ack().unwrap();
        assert_eq!(
            sack.as_bytes(),
            [0b1000_0011, 0, 0, 0, 0, 0, 0, 0b1000_0000]
        );

        assert_eq!(sack, SelectiveAck::deserialize(sack.as_bytes()));
    }
}
