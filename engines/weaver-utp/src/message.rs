// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.

use tokio::time::Instant;
use tracing::trace;

use crate::{
    Payload,
    raw::{Type, UtpHeader},
};

#[derive(Clone)]
pub struct UtpMessage {
    pub header: UtpHeader,
    pub data: Payload,
    pub(crate) received: Instant,
}

// Arrival time is local metadata, not part of the packet's identity.
impl PartialEq for UtpMessage {
    fn eq(&self, other: &Self) -> bool {
        self.header == other.header && self.data == other.data
    }
}

impl Eq for UtpMessage {}

impl std::fmt::Debug for UtpMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.header.short_repr())?;
        if cfg!(test) {
            write!(
                f,
                ":payload_len={}:payload={:?}",
                self.payload().len(),
                std::str::from_utf8(self.payload()).unwrap()
            )
        } else {
            write!(f, ":payload_len={}", self.payload().len())
        }
    }
}

impl UtpMessage {
    #[cfg(test)]
    pub fn new_test(header: UtpHeader, payload: &[u8]) -> Self {
        UtpMessage {
            header,
            data: payload.to_owned(),
            received: Instant::now(),
        }
    }

    pub fn deserialize(buf: &[u8], received: Instant) -> Option<Self> {
        let (header, hsize) = UtpHeader::deserialize(buf)?;

        let payload_size = buf.len() - hsize;

        match header.get_type() {
            Type::ST_DATA => {
                if payload_size == 0 {
                    trace!("ST_DATA packet with 0 payload, ignoring");
                    return None;
                }
            }
            other => {
                if payload_size > 0 {
                    trace!("{other:?} packet with payload, ignoring");
                    return None;
                }
            }
        }

        Some(Self {
            header,
            data: buf[hsize..].to_owned(),
            received,
        })
    }

    pub fn payload(&self) -> &[u8] {
        &self.data
    }
}
