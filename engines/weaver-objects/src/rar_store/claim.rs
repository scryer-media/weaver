// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Which sibling members are this set's volumes, and where in the set each
// one is: the position the set gives it, never the order it arrived in.
// Every volume of a RAR set starts with the signature, so a member without
// one is never claimed. A name in the set's own numbering is its own
// number. A name that numbers nothing of ours, an obfuscated one, is placed
// by its own first headers: at the number they state, or, stating none,
// after the held volume whose last member it continues. The store's first
// volume is placed the same way, with nothing held yet.

use std::io::Cursor;
use std::sync::Arc;

use unrar_rs::{ArchiveFormat, RarArchive, RarVolumeFacts, RarVolumeMemberFacts};
use weaver_graph::VolumeIndex;

use super::{RarStore, Volume};
use crate::engine::SetName;
use crate::rar_engine::naming;

// One volume's headers, as far as they place it in its set.
struct Nameless<'a> {
    facts: &'a RarVolumeFacts,
}

impl Nameless<'_> {
    // The position the headers state. A RAR5 volume numbers itself in its
    // main header, all but the first, which states no number; a RAR4
    // volume numbers itself only in its end record.
    fn position(&self) -> Option<u32> {
        match (self.facts.volume_number, self.facts.archive_format()) {
            (Some(stated), _) => Some(stated),
            (None, ArchiveFormat::Rar5) => self.facts.is_volume.then_some(0),
            (None, _) => None,
        }
    }

    fn ordered_members(&self) -> Vec<&RarVolumeMemberFacts> {
        let mut members: Vec<_> = self.facts.members.iter().collect();
        members.sort_by_key(|member| member.order);
        members
    }

    // The member this volume opens on, when it continues one begun earlier.
    fn continued_member(&self) -> Option<&RarVolumeMemberFacts> {
        self.ordered_members()
            .first()
            .copied()
            .filter(|member| member.split_before)
    }

    // The member this volume ends on, when it carries on into the next one.
    fn continuing_member(&self) -> Option<&RarVolumeMemberFacts> {
        self.ordered_members()
            .last()
            .copied()
            .filter(|member| member.split_after)
    }

    // Whether this volume can follow `previous` at index `index`, judged only
    // by what both volumes' headers state.
    // Whether both volumes are of one kind of set: the same format, and
    // both enciphered or neither.
    fn kin(&self, other: &Nameless<'_>) -> bool {
        self.facts.format == other.facts.format
            && self.facts.is_encrypted == other.facts.is_encrypted
    }

    fn follows(&self, previous: &Nameless<'_>, index: u32) -> bool {
        if !self.kin(previous) || self.position().is_some_and(|stated| stated != index) {
            return false;
        }
        match (previous.continuing_member(), self.continued_member()) {
            (Some(tail), Some(head)) => {
                head.name == tail.name
                    && match (head.unpacked_size, tail.unpacked_size) {
                        (Some(head_size), Some(tail_size)) => head_size == tail_size,
                        _ => true,
                    }
            }
            // A boundary that falls between two members links only on a
            // stated number; nothing else in the headers ties the volumes.
            (None, None) => self.position() == Some(index),
            _ => false,
        }
    }
}

impl RarStore {
    pub(super) fn claim_volume(&self, name: &str, first_bytes: &[u8]) -> Option<VolumeIndex> {
        let format = unrar_rs::signature::read_signature(&mut Cursor::new(first_bytes)).ok()?;
        if self.volumes.values().any(|ours| {
            ours.walk.in_set() == Some(false)
                || ours
                    .walk
                    .facts
                    .as_ref()
                    .is_some_and(|facts| facts.archive_format() != format)
        }) {
            return None;
        }
        // The set's numbering is the one the bound names share; before any
        // is bound, the one this member's own name gives.
        let wanted = naming::parse(name);
        let unnamed = self.volumes.values().all(|volume| volume.name.is_none());
        let named = wanted.as_ref().is_some_and(|wanted| {
            unnamed
                || self
                    .volumes
                    .values()
                    .filter_map(|volume| volume.name.as_deref().and_then(naming::parse))
                    .any(|bound| bound.same_set(wanted))
        });
        let index = match (named, wanted) {
            (true, Some(wanted)) => VolumeIndex(wanted.index()),
            _ => self.claim_by_headers(first_bytes)?,
        };
        let taken = self
            .volumes
            .get(&index)
            .is_some_and(|volume| volume.name.is_some());
        (!taken).then_some(index)
    }

    // The position this volume's headers give it. One that states its
    // number is at that number, when the held volumes beside it agree: the
    // one before carries on into it, the one after is of the same kind and
    // states the next number. One that states none, a nameless RAR4 front,
    // follows the one held volume whose last member it continues.
    fn claim_by_headers(&self, first_bytes: &[u8]) -> Option<VolumeIndex> {
        let password = self.header_password();
        let facts = RarArchive::parse_volume_facts_walk_with_shared_kdf_cache(
            Cursor::new(first_bytes.to_vec()),
            password.as_deref().map(String::as_str),
            Arc::clone(self.keys.kdf()),
        )
        .ok()?
        .facts;
        if !facts.is_volume {
            return None;
        }
        let this = Nameless { facts: &facts };
        let held = |index: u32| {
            self.volumes
                .get(&VolumeIndex(index))
                .filter(|volume| volume.name.is_some())
        };
        let joins = |previous: &Volume, index: u32| {
            previous.walk.successor() != Some(false)
                && previous
                    .walk
                    .facts
                    .as_ref()
                    .is_some_and(|facts| this.follows(&Nameless { facts }, index))
        };
        if let Some(stated) = this.position() {
            let before = stated
                .checked_sub(1)
                .and_then(held)
                .is_none_or(|previous| joins(previous, stated));
            let after = held(stated + 1).is_none_or(|next| {
                next.walk.facts.as_ref().is_none_or(|facts| {
                    let next = Nameless { facts };
                    next.kin(&this) && next.position() == Some(stated + 1)
                })
            });
            return (before && after).then_some(VolumeIndex(stated));
        }
        let candidates: Vec<u32> = self
            .volumes
            .iter()
            .filter(|(index, volume)| {
                volume.name.is_some() && held(index.0 + 1).is_none() && joins(volume, index.0 + 1)
            })
            .map(|(index, _)| index.0 + 1)
            .collect();
        match candidates.as_slice() {
            [only] => Some(VolumeIndex(*only)),
            _ => None,
        }
    }
}
