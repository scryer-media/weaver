// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

mod bencode_value;
mod deserialize;
pub mod raw_value;
mod serialize;
mod soft_error;

pub use bencode_value::*;
pub use deserialize::{
    BencodeDeserializer, DEFAULT_MAX_DEPTH, Error as DeserializeError,
    ErrorWithContext as DeserializeErrorWithContext, WithRawBytes, from_bytes, from_bytes_limited,
    from_bytes_with_rest, from_bytes_with_rest_limited,
};
pub use serialize::{Error as SerializeError, bencode_serialize_to_writer};
pub use soft_error::has_soft_error;

pub use crate::buffers::{ByteBuf, ByteBufOwned};
