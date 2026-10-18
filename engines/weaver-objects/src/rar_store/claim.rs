// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Which sibling members are this set's volumes. Every volume of a RAR set
// starts with the signature, so a member without one is never claimed. A
// name in the set's own numbering is its own number. A name that numbers
// nothing of ours, an obfuscated one, is placed by its own first headers:
// it follows the held volume whose last member it continues, or, on a
// boundary between members, the one before the number it states.

use std::io::Cursor;
use std::sync::Arc;

use unrar_rs::{RarArchive, RarVolumeFacts, RarVolumeMemberFacts};
use weaver_graph::VolumeIndex;

use super::RarStore;
use crate::engine::SetName;
use crate::rar_engine::naming;

// One volume's headers, as far as they place it in its set.
struct Nameless<'a> {
    facts: &'a RarVolumeFacts,
}

impl Nameless<'_> {
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
    fn follows(&self, previous: &Nameless<'_>, index: u32) -> bool {
        if self.facts.format != previous.facts.format
            || self.facts.is_encrypted != previous.facts.is_encrypted
            || self
                .facts
                .volume_number
                .is_some_and(|stated| stated != index)
        {
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
            (None, None) => self.facts.volume_number == Some(index),
            _ => false,
        }
    }
}

impl RarStore {
    pub(super) fn claim_volume(&mut self, name: &str, first_bytes: &[u8]) -> Option<VolumeIndex> {
        let format = unrar_rs::signature::read_signature(&mut Cursor::new(first_bytes)).ok()?;
        let ours = self.volumes.get(&VolumeIndex(0))?;
        if ours.walk.in_set() == Some(false)
            || ours
                .walk
                .facts
                .as_ref()
                .is_some_and(|facts| facts.archive_format() != format)
        {
            return None;
        }
        let wanted = naming::parse(name);
        let named = wanted.as_ref().is_some_and(|wanted| {
            self.volumes
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

    // The one held volume this one can follow, preferring the one its
    // stated number names.
    fn claim_by_headers(&self, first_bytes: &[u8]) -> Option<VolumeIndex> {
        let password = self.header_password();
        let facts = RarArchive::parse_volume_facts_walk_with_shared_kdf_cache(
            Cursor::new(first_bytes.to_vec()),
            password.as_deref().map(String::as_str),
            Arc::clone(&self.kdf),
        )
        .ok()?
        .facts;
        if !facts.is_volume {
            return None;
        }
        let this = Nameless { facts: &facts };
        let candidates: Vec<u32> =
            self.volumes
                .iter()
                .filter(|(index, volume)| {
                    let next = VolumeIndex(index.0 + 1);
                    volume.name.is_some()
                        && self
                            .volumes
                            .get(&next)
                            .is_none_or(|next| next.name.is_none())
                        && volume.walk.successor() != Some(false)
                        && volume.walk.facts.as_ref().is_some_and(|previous| {
                            this.follows(&Nameless { facts: previous }, next.0)
                        })
                })
                .map(|(index, _)| index.0 + 1)
                .collect();
        let stated: Vec<u32> = candidates
            .iter()
            .copied()
            .filter(|candidate| facts.volume_number == Some(*candidate))
            .collect();
        match (stated.as_slice(), candidates.as_slice()) {
            ([only], _) | ([], [only]) => Some(VolumeIndex(*only)),
            _ => None,
        }
    }
}
