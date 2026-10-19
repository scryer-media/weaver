// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An object's durable state, as one versioned blob, so a job's tree can be
// rebuilt after a restart without fetching again what it already placed.
// Each kind owns what its blob holds and which versions it reads; the graph
// only carries the blob. A blob never holds key material: an object that
// deciphers keeps what lets it derive its key again from a password the
// owner offers anew, never the key.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::object::{Kind, Object};
use crate::tree::NodeId;

mod sealed;

pub use sealed::{Give, Part, Sealed, Shelf, Take, decode_parts, encode_parts};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Facts {
    pub kind: Kind,
    pub version: u16,
    pub bytes: Vec<u8>,
    // The sealed parts `bytes` names, each kept apart so a checkpoint
    // writes again only the ones that changed.
    pub parts: Vec<Part>,
}

impl Facts {
    // The version of an object that keeps no facts yet: nothing restores it,
    // and its owner fetches what it held again.
    pub const UNSUPPORTED: u16 = 0;

    pub fn unsupported(kind: Kind) -> Self {
        Self {
            kind,
            version: Self::UNSUPPORTED,
            bytes: Vec::new(),
            parts: Vec::new(),
        }
    }

    // A state that does not encode is one nothing could restore, so it says
    // so as an unsupported blob rather than failing the checkpoint.
    pub fn of<T: Durable>(kind: Kind, state: &T) -> Self {
        match encode_parts(state) {
            Ok((bytes, parts)) => Self {
                kind,
                version: T::VERSION,
                bytes,
                parts,
            },
            Err(_) => Self::unsupported(kind),
        }
    }

    pub fn read<T: Durable>(&self, kind: Kind) -> Result<T, RestoreError> {
        if self.kind != kind {
            return Err(RestoreError::Kind {
                expected: kind,
                found: self.kind,
            });
        }
        match self.version {
            Self::UNSUPPORTED => return Err(RestoreError::Unsupported { kind }),
            version if version != T::VERSION => {
                return Err(RestoreError::Version { kind, version });
            }
            _ => {}
        }
        let state: T = decode_parts(&self.bytes, &self.parts)
            .map_err(|reason| RestoreError::Decode { kind, reason })?;
        match state.impossible() {
            Some(reason) => Err(RestoreError::Impossible { kind, reason }),
            None => Ok(state),
        }
    }
}

// A state that is its own facts: what it encodes to, at which version, its
// sealed values, and what makes a decoded one impossible.
pub trait Durable: Serialize + DeserializeOwned {
    const VERSION: u16;

    // Each sealed value the state holds, in the order its encoding meets
    // them.
    fn parts(&self, _take: &mut Take) -> Result<(), String> {
        Ok(())
    }

    // Each sealed value of a decoded state its parts, in the same order.
    fn restore_parts(&mut self, _give: &mut Give) -> Result<(), String> {
        Ok(())
    }

    fn impossible(&self) -> Option<&'static str> {
        None
    }
}

// How a tree gets its objects back from their facts. Beside `Builder`,
// which makes them new.
pub trait Restore {
    fn restore(&self, facts: &Facts) -> Result<Box<dyn Object>, RestoreError>;
}

// Why a restore was refused. The owner refuses the whole restore with the
// reason and fetches again; it never guesses at a state.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RestoreError {
    #[error("{kind} keeps no facts")]
    Unsupported { kind: Kind },
    #[error("{kind} facts are version {version}, which this build does not read")]
    Version { kind: Kind, version: u16 },
    #[error("facts of {found} offered as {expected}")]
    Kind { expected: Kind, found: Kind },
    #[error("{kind} facts do not decode: {reason}")]
    Decode { kind: Kind, reason: String },
    #[error("{kind} facts describe an impossible state: {reason}")]
    Impossible { kind: Kind, reason: &'static str },
    #[error("tree record node {}: {reason}", node.0)]
    Tree { node: NodeId, reason: &'static str },
}
