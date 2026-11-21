// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A DNS reply's header, read before or in place of the reply itself. The
// reply is read by `weaver_dnssec::Reply`.
use super::INVALID;
use crate::TunnelError;

fn invalid() -> TunnelError {
    TunnelError::Engine(INVALID.into())
}

// The error code a reply to query `id` carries in place of an answer, when
// it is one: the server could not, or would not, resolve the name.
pub(super) fn refusal(bytes: &[u8], id: u16) -> Option<u16> {
    let flags = u16_at(bytes, 2).ok()?;
    let rcode = flags & 0x000f;
    (u16_at(bytes, 0).ok()? == id && flags & 0x8000 != 0 && !matches!(rcode, 0 | 3))
        .then_some(rcode)
}

pub(super) fn u16_at(bytes: &[u8], at: usize) -> Result<u16, TunnelError> {
    Ok(u16::from_be_bytes(
        bytes
            .get(at..at + 2)
            .ok_or_else(invalid)?
            .try_into()
            .map_err(|_| invalid())?,
    ))
}
