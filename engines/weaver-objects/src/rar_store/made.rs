// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The members of a mixed set the store's run makes. A set that is not solid
// and stores a file before it compresses one streams every stored member as
// it does a set with nothing compressed, and keeps each compressed member's
// packed data in its envelope, where the layout maps it, beside the headers.
// Its run is the RAR engine's own decode loop over that envelope, scoped to
// those members: it walks the headers, seeks past every member it does not
// make, and decodes the rest under the store's ids, announcing none.
//
// A compressed member is made when the archive is not solid, the member is
// not solid either, and a member the store streams comes before it; an
// enciphered one when it is keyed by AES as unrar keys it, so a password
// opens it. A set whose first file is compressed goes to the engine whole,
// as before: nothing the headers state up front says whether a stored
// member follows. Everything else a member's headers refuse is refused as
// before.
//
// The run starts once the layout reaches the volume that ends the set: the
// members it makes are fixed when it starts, and a later header could add
// one.

use std::collections::BTreeMap;

use unrar_rs::{
    ArchiveFormat, IneligibilityReason, MemberEligibility, RarVolumeMemberFacts, StoredMember,
};
use weaver_graph::{Engine, Extraction, MemberId, VolumeIndex};

use super::{RarStore, claim};
use crate::engine::Format;
use crate::rar_engine::run::{Made, Scope};
use crate::rar_engine::{Rar, member_window};

// The name a made member is announced under: the RAR engine's for the same
// header, so its file lands where the engine's would, refused where the
// engine's would be. The namespace stays the store's.
pub(super) fn name(raw: &str) -> String {
    unrar_rs::sanitize_path(raw)
}

// RAR4 unpack versions that select a cipher other than AES, which unrar
// keys and nothing here can: RAR 1.3, 1.5 and 2.0 (unrar-rs
// `Rar4EncryptionMethod::for_unpack_version`, read by `rar4_uses_aes` in its
// stored layout).
const RAR4_PRE_AES: [u8; 4] = [13, 15, 20, 26];

impl RarStore {
    // Whether member `id` is the run's to make: newly met, or made before,
    // compressed or enciphered and compressed, in a set that streams a
    // member before it.
    pub(super) fn made_by_run(
        &self,
        id: usize,
        member: &StoredMember,
        format: ArchiveFormat,
    ) -> bool {
        let fresh = self.entries.get(id).is_none_or(|entry| entry.made);
        let streamed_before = self.first_stored.is_some_and(|first| first < id);
        fresh
            && streamed_before
            && match &member.eligibility {
                MemberEligibility::Ineligible(IneligibilityReason::Compressed { .. }) => true,
                MemberEligibility::Ineligible(IneligibilityReason::Encrypted) => {
                    self.coded_and_keyed(member, format)
                }
                _ => false,
            }
    }

    // An enciphered member the layout refused that is compressed, not
    // solid, and keyed by AES in every part the store has read: what
    // unrar-rs folds into `Encrypted` beside the ciphers it cannot key.
    fn coded_and_keyed(&self, member: &StoredMember, format: ArchiveFormat) -> bool {
        let mut parts = member
            .parts
            .iter()
            .map(|part| self.part_facts(part.volume, &member.name))
            .peekable();
        if parts.peek().is_none() {
            return false;
        }
        let mut compressed = false;
        for part in parts {
            let Some(part) = part else {
                return false;
            };
            let keyed = match format {
                ArchiveFormat::Rar5 => part.encryption.is_some(),
                ArchiveFormat::Rar4 => !RAR4_PRE_AES.contains(&part.compression_version),
                ArchiveFormat::Rar14 => false,
            };
            if !part.is_encrypted || part.compression_solid || !keyed {
                return false;
            }
            compressed |= part.compression_method != 0;
        }
        compressed
    }

    // The header of `name`'s part in volume `volume`, as the walk read it.
    fn part_facts(&self, volume: u32, name: &str) -> Option<&RarVolumeMemberFacts> {
        self.volumes
            .get(&VolumeIndex(volume))?
            .walk
            .facts
            .as_ref()?
            .members
            .iter()
            .find(|part| part.name == name)
    }

    // Whether the store runs: it makes a member, and the layout holds the
    // set to the volume that says none follows it, so no header still to
    // come adds another.
    pub(super) fn runs(&self) -> bool {
        !self.gone
            && !self.made.is_empty()
            && self
                .laid_through()
                .and_then(|through| through.checked_sub(1))
                .and_then(|last| self.volumes.get(&VolumeIndex(last)))
                .is_some_and(|last| last.walk.is_last())
    }

    // The password the run decodes under: the one that opened the headers,
    // or the latest the job offered, which answers what a run asked, as the
    // engine's run takes it.
    fn password(&self) -> Option<String> {
        self.headers_open
            .and_then(|index| self.candidates.get(index))
            .or_else(|| self.candidates.last())
            .map(|candidate| candidate.secret.as_str().to_owned())
    }

    // The window each made member's decode holds, by the engine's rule.
    fn made_windows(&self) -> impl Iterator<Item = u64> + '_ {
        self.made.iter().filter_map(|&id| {
            let entry = self.entries.get(id)?;
            let first = entry.pieces.first()?;
            member_window(false, self.part_facts(first.volume.0, &entry.name)?)
        })
    }
}

// The run over a mixed set's made members: the RAR engine's run, reading
// the store's envelope as the set's volumes. It reserves and borrows as
// the engine does, from the made members' windows alone. It bids for
// nothing: the store bids ahead of the volume it reads headers in
// (`Object::wants`), and the run reads behind the store. A volume's place
// is settled as the store holds it: by its headers, its name or its end,
// as a volume the engine holds on trust is placed by its tail.
impl Engine for RarStore {
    fn start(&mut self) -> Box<dyn Extraction> {
        // The layout numbers members in archive order, as a run's walk
        // does, so each made entry is the member at its own index.
        let made: BTreeMap<usize, Made> = self
            .made
            .iter()
            .filter_map(|&id| {
                let entry = self.entries.get(id)?;
                Some((
                    id,
                    Made {
                        id: MemberId(id as u32),
                        name: entry.name.clone(),
                    },
                ))
            })
            .collect();
        crate::rar_engine::run_of(self.password(), Scope::Members(made))
    }

    fn placed(&self, volume: VolumeIndex) -> bool {
        self.volumes
            .get(&volume)
            .is_none_or(|found| found.closed || claim::settled(volume, found))
    }

    fn reserve(&self) -> Option<u64> {
        crate::engine::reserve::<Rar>(self.made_windows())
    }

    fn lends(&self) -> bool {
        Rar::lends()
    }

    fn makes(&self, member: MemberId) -> bool {
        self.made.contains(&(member.0 as usize))
    }
}
