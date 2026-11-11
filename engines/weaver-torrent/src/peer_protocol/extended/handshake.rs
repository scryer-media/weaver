// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use crate::buffers::{ByteBuf, ByteBufT};
use crate::core::compact_ip::{CompactIpAddr, CompactIpV4, CompactIpV6};
use serde_derive::{Deserialize, Serialize};

use super::PeerExtendedMessageIds;

#[derive(Deserialize, Serialize, Debug, Default, Eq, PartialEq)]
pub struct ExtendedHandshake<ByteBuf: ByteBufT> {
    // BEP 10 says all items of the extended handshake dictionary are optional.
    // Missing "m" means the peer supports no outgoing extensions.
    #[serde(default)]
    pub m: PeerExtendedMessageIds,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub v: Option<ByteBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub yourip: Option<CompactIpAddr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ipv6: Option<CompactIpV6>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ipv4: Option<CompactIpV4>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reqq: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_size: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub complete_ago: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload_only: Option<u32>,
}

impl ExtendedHandshake<ByteBuf<'_>> {
    pub fn new() -> Self {
        Self {
            m: PeerExtendedMessageIds::my(),
            ..Default::default()
        }
    }

    pub fn port(&self) -> Option<u16> {
        self.p.and_then(|p| u16::try_from(p).ok())
    }
}
