// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use crate::buffers::{ByteBuf, ByteBufT};
use crate::core::compact_ip::{CompactIpAddr, CompactIpV4, CompactIpV6};
use serde::Deserializer;
use serde_derive::{Deserialize, Serialize};

use super::PeerExtendedMessageIds;

// BEP 10 says all items of the extended handshake dictionary are optional.
// Each is read on its own: one of the wrong type or length is taken as
// absent, and the rest of the handshake still counts (libtorrent
// `bt_peer_connection::on_extended_handshake` looks each up with
// `dict_find_int_value`, `dict_find_string_value` and `dict_find_dict`, and
// skips one that is not what it looks for). A handshake that is not a
// dictionary is not one.
#[derive(Deserialize, Serialize, Debug, Default, Eq, PartialEq)]
#[serde(bound(deserialize = "ByteBuf: serde::Deserialize<'de>"))]
pub struct ExtendedHandshake<ByteBuf: ByteBufT> {
    // Missing "m" means the peer supports no outgoing extensions.
    #[serde(default, deserialize_with = "lenient_ids")]
    pub m: PeerExtendedMessageIds,
    #[serde(
        default,
        deserialize_with = "lenient",
        skip_serializing_if = "Option::is_none"
    )]
    pub p: Option<u32>,
    #[serde(
        default,
        deserialize_with = "lenient",
        skip_serializing_if = "Option::is_none"
    )]
    pub v: Option<ByteBuf>,
    #[serde(
        default,
        deserialize_with = "lenient",
        skip_serializing_if = "Option::is_none"
    )]
    pub yourip: Option<CompactIpAddr>,
    #[serde(
        default,
        deserialize_with = "lenient",
        skip_serializing_if = "Option::is_none"
    )]
    pub ipv6: Option<CompactIpV6>,
    #[serde(
        default,
        deserialize_with = "lenient",
        skip_serializing_if = "Option::is_none"
    )]
    pub ipv4: Option<CompactIpV4>,
    #[serde(
        default,
        deserialize_with = "lenient",
        skip_serializing_if = "Option::is_none"
    )]
    pub reqq: Option<u32>,
    #[serde(
        default,
        deserialize_with = "lenient",
        skip_serializing_if = "Option::is_none"
    )]
    pub metadata_size: Option<u32>,
    #[serde(
        default,
        deserialize_with = "lenient",
        skip_serializing_if = "Option::is_none"
    )]
    pub complete_ago: Option<i32>,
    #[serde(
        default,
        deserialize_with = "lenient",
        skip_serializing_if = "Option::is_none"
    )]
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

/// An optional field of the extended handshake: none when it is not a `T`.
/// A value that is not bencode at all is still an error.
pub(crate) fn lenient<'de, D, T>(de: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Field<T> {
        Valid(T),
        Other(serde::de::IgnoredAny),
    }
    Ok(match <Field<T> as serde::Deserialize>::deserialize(de)? {
        Field::Valid(value) => Some(value),
        Field::Other(_) => None,
    })
}

/// The handshake's `m`: no ids when it is not a dictionary (libtorrent
/// `dict_find_dict("m")`).
fn lenient_ids<'de, D>(de: D) -> Result<PeerExtendedMessageIds, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(lenient(de)?.unwrap_or_default())
}
