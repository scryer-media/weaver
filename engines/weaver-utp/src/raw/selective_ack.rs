// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

use bitvec::{BitArr, order::Lsb0, slice::BitSlice};

use crate::constants::SACK_DEPTH;

type SelectiveAckData = BitArr!(for SACK_DEPTH, in u8, Lsb0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectiveAck {
    data: SelectiveAckData,
    len: usize,
}

impl SelectiveAck {
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
        for idx in acked {
            data.get_mut(idx).unwrap().set(true);
            count += 1;
        }
        if count > 0 {
            Some(Self {
                data,
                len: SACK_DEPTH,
            })
        } else {
            None
        }
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.data.as_raw_slice()
    }

    pub fn deserialize(bytes: &[u8]) -> Self {
        // The spec says the len must be a multiple of 4, but there's a ton of messages
        // in the wild that are 1 bytes long. So we can
        // thus deserialize any payload.
        //
        // If it's longer than 8 bytes (unlikely), it will truncate the end, which is fine, as
        // we'll just resend that data if anything.
        let len = bytes.len().min(std::mem::size_of::<SelectiveAckData>());
        let mut data = SelectiveAckData::default();
        data.as_raw_mut_slice()[..len].copy_from_slice(&bytes[..len]);
        Self {
            data,
            len: bytes.len() * 8,
        }
    }

    pub fn as_bitslice(&self) -> &BitSlice<u8> {
        self.data.as_bitslice()
    }

    pub fn iter(&self) -> impl Iterator<Item = bool> + '_ {
        self.data.iter().map(|b| *b)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use crate::{
        message::UtpMessage,
        raw::{Type::ST_DATA, selective_ack::SelectiveAck},
        stream_rx::OutOfOrderQueue,
    };

    fn asm() -> OutOfOrderQueue {
        OutOfOrderQueue::new(NonZeroUsize::new(65).unwrap())
    }

    fn msg() -> UtpMessage {
        UtpMessage::new_test(
            crate::raw::UtpHeader {
                htype: ST_DATA,
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
            sack.data.as_raw_slice(),
            [0b1000_0011, 0, 0, 0, 0, 0, 0, 0b1000_0000]
        );

        assert_eq!(sack, SelectiveAck::deserialize(sack.as_bytes()));
    }
}
