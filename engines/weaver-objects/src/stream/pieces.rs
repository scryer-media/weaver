// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A plain file posted in numbered pieces, `name.001`, `name.002`, …, whose
// first piece opens no signature: the pieces are one member, `name`, their
// bytes joined in order and nothing done to them. The pieces are its
// volumes, never delivered themselves.

use std::io::{self, Read};

use weaver_graph::{Kind, Standing, Why};

use super::{Decoder, Front};
use crate::split;

pub struct Pieces;

// How much of the joined bytes a run holds at once.
const WINDOW: u64 = 64 << 10;

impl Decoder for Pieces {
    const KIND: Kind = Kind::Split;

    fn window(_first: &[u8]) -> u64 {
        WINDOW
    }

    fn open(input: Front, _window: u64) -> io::Result<Box<dyn Read + Send>> {
        Ok(Box::new(input))
    }

    // What the pieces were cut from.
    fn member(volume: &str) -> String {
        split::whole(volume).unwrap_or(volume).to_owned()
    }

    // The member is the pieces' bytes with nothing done to them, so damage
    // in it is theirs as they stand: it is delivered as the member, and the
    // pieces would carry the same damage under their own names. Only a
    // member the run never closed gives them back.
    fn quiet(standing: &dyn Standing) -> Option<Why> {
        crate::quiet::unplaced(standing)
    }
}
