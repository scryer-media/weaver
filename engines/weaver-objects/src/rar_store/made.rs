// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The members of a set the store's run makes. A set that is not solid
// streams every stored member as it does a set with nothing compressed, and
// keeps each compressed member's packed data in its envelope, where the
// layout maps it, beside the headers. Its run is the RAR engine's own decode
// loop over that envelope, scoped to those members: it walks the headers,
// seeks past every member it does not make, and decodes the rest under the
// store's ids, announcing none.
//
// Which members the run makes is one rule (`by_run`), over what a member's
// first header states, that the store and the run each apply to the headers
// they read: the store as it lays a member, the run as it meets it. A
// compressed member is made when the archive is not solid and the member is
// not solid either; an enciphered one when it is keyed by AES as unrar keys
// it, so a password opens it. A solid set goes to the engine whole.
// Everything else a member's headers refuse is refused as before.
//
// The run starts once the store makes a member, and chases the layout: the
// store lays a member's header, and announces it, before any byte of its
// packed data reaches the envelope, so the run meets each member it makes
// only after the store has announced it.

use std::collections::BTreeMap;

use unrar_rs::{
    ArchiveFormat, IneligibilityReason, MemberEligibility, RarVolumeMemberFacts, StoredMember,
};
use weaver_graph::{Engine, Extraction, MemberId, VolumeIndex};

use super::{RarStore, claim};
use crate::engine::Format;
use crate::rar_engine::run::Scope;
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

// What a member's first header states that decides whether the run makes
// it, as the store and the run each read it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Traits {
    pub(crate) directory: bool,
    pub(crate) link: bool,
    pub(crate) compressed: bool,
    pub(crate) solid: bool,
    pub(crate) encrypted: bool,
    // Keyed by AES as unrar keys it: a RAR5 part with its encryption record,
    // a RAR4 part of an unpack version that selects AES.
    pub(crate) keyed: bool,
}

impl Traits {
    // As the store's walk read one part's header.
    pub(crate) fn of_part(part: &RarVolumeMemberFacts, format: ArchiveFormat) -> Self {
        Self {
            directory: part.is_directory,
            link: part.redirection_type.is_some() || rar4_unix_symlink(part, format),
            compressed: part.compression_method != 0,
            solid: part.compression_solid,
            encrypted: part.is_encrypted,
            keyed: keyed(format, part.encryption.is_some(), part.compression_version),
        }
    }

    // As a run's archive read the member's header. A RAR5 member the
    // archive opens as enciphered carries its encryption record: one that
    // states none is one unrar cannot key, and the store refuses its set
    // (`made_by_run`).
    pub(crate) fn of_info(info: &unrar_rs::MemberInfo, link: bool) -> Self {
        let format = info.compression.format;
        Self {
            directory: info.is_directory,
            link,
            compressed: info.compression.method != unrar_rs::CompressionMethod::Store,
            solid: info.compression.solid,
            encrypted: info.is_encrypted,
            keyed: keyed(format, info.is_encrypted, info.compression.version),
        }
    }
}

// A RAR4 header has no redirection record: a symlink is a Unix host's
// header whose attributes give the link file type, as unrar-rs marks the
// member it reads (`is_rar4_unix_symlink`, the run's `MemberInfo.is_symlink`).
fn rar4_unix_symlink(part: &RarVolumeMemberFacts, format: ArchiveFormat) -> bool {
    format != ArchiveFormat::Rar5
        && part.host_os == Some(unrar_rs::RarVolumeHostOs::Unix)
        && part
            .attributes
            .is_some_and(|attributes| attributes & 0xF000 == 0xA000)
}

fn keyed(format: ArchiveFormat, record: bool, version: u8) -> bool {
    match format {
        ArchiveFormat::Rar5 => record,
        ArchiveFormat::Rar4 => !RAR4_PRE_AES.contains(&version),
        ArchiveFormat::Rar14 => false,
    }
}

// Whether the run makes a member whose first header states `first`, in an
// archive whose main header says whether it is solid: the one rule the store
// lays members by and the run decodes them by.
pub(crate) fn by_run(solid_archive: bool, first: Traits) -> bool {
    !solid_archive
        && !first.directory
        && !first.link
        && !first.solid
        && first.compressed
        && (!first.encrypted || first.keyed)
}

impl RarStore {
    // Whether member `id` is the run's to make: newly met, laid again into
    // a slot retracted where it stood, or made before, by the rule the run
    // reads its headers by (`by_run`), and refused by
    // nothing only the store sees. Both go by the member's first header
    // only, as unrar decodes a member by it: a later part's header that
    // disagrees, or one held on trust in a volume not yet proven its place,
    // changes nothing here. What only the store sees, the run cannot: a
    // malformed chain, a member streamed before that a repaired header now
    // says is compressed, a RAR5 part enciphered with no encryption record.
    // Each
    // leaves the member unmade under a verdict that gives the set up
    // (`verdict`), before any byte of its packed data is mapped: the run,
    // having read its header, waits on those bytes until the store is gone.
    pub(super) fn made_by_run(
        &self,
        id: usize,
        member: &StoredMember,
        format: ArchiveFormat,
    ) -> bool {
        let fresh = self
            .entries
            .get(id)
            .is_none_or(|entry| entry.made || entry.waiting);
        let eligible = matches!(
            &member.eligibility,
            MemberEligibility::Ineligible(
                IneligibilityReason::Compressed { .. } | IneligibilityReason::Encrypted
            )
        );
        let Some(first) = self.first_traits(member, format) else {
            return false;
        };
        fresh && eligible && by_run(self.solid_archive(), first)
    }

    // Whether member `member` is a link by its first header, as the run
    // reads one (`Traits::link`): a RAR5 redirection, or a RAR4 Unix
    // symlink. It holds no file body, so the store streams none of it and
    // gives the set to the engine as it does any redirection (`verdict`),
    // whose run passes the link over as no file.
    pub(super) fn links(&self, member: &StoredMember, format: ArchiveFormat) -> bool {
        self.first_traits(member, format)
            .is_some_and(|first| first.link)
    }

    // What member `member`'s first header states, as the walk read it.
    fn first_traits(&self, member: &StoredMember, format: ArchiveFormat) -> Option<Traits> {
        let part = member.parts.first()?;
        self.part_facts(part.volume, part.data_offset)
            .map(|first| Traits::of_part(first, format))
    }

    // Whether the set's first volume's main header says it is solid.
    fn solid_archive(&self) -> bool {
        self.volumes
            .get(&VolumeIndex(0))
            .and_then(|found| found.walk.facts.as_ref())
            .is_some_and(|facts| facts.is_solid)
    }

    // The header of the part whose packed data starts at `data` in volume
    // `volume`, as the walk read it: found by where it lies, not by the
    // name a header not yet repaired may still give it.
    fn part_facts(&self, volume: u32, data: u64) -> Option<&RarVolumeMemberFacts> {
        self.volumes.get(&VolumeIndex(volume))?.walk.part_at(data)
    }

    // Whether the store runs: it makes a member. The run chases the layout
    // from there, making each member it meets that the store makes.
    pub(super) fn runs(&self) -> bool {
        !self.gone && !self.made.is_empty()
    }

    // The candidate whose password the run decodes under: the one that
    // opened the headers, or the latest the job offered, which answers what
    // a run asked, as the engine's run takes it. An offer of a candidate
    // offered before, as a job trying its candidates again from the first
    // after a repair, is the latest.
    fn password(&self) -> Option<usize> {
        self.headers_open
            .or(self.offered)
            .filter(|&index| index < self.candidates.len())
            .or_else(|| self.candidates.len().checked_sub(1))
    }

    // The windows of the made entries from `from` on, worked out again
    // from the headers of their first pieces. A look files them from the
    // first entry it looked at: no entry before it changed, nor the headers
    // it was laid on.
    pub(super) fn file_windows(&mut self, from: usize) {
        let stale: Vec<(usize, u64)> = self
            .windows
            .range(from..)
            .map(|(&id, &window)| (id, window))
            .collect();
        for (id, window) in stale {
            self.windows.remove(&id);
            if let Some(count) = self.widest.get_mut(&window) {
                *count -= 1;
                if *count == 0 {
                    self.widest.remove(&window);
                }
            }
        }
        let filed: Vec<(usize, u64)> = self
            .made
            .range(from..)
            .filter_map(|&id| Some((id, self.window_of(id)?)))
            .collect();
        for (id, window) in filed {
            #[cfg(test)]
            super::counts::bump(|counts| counts.windows_filed += 1);
            self.windows.insert(id, window);
            *self.widest.entry(window).or_default() += 1;
        }
    }

    // The window a made entry's decode holds, by the engine's rule.
    fn window_of(&self, id: usize) -> Option<u64> {
        let first = self.entries.get(id)?.pieces.first()?;
        member_window(false, self.part_facts(first.volume.0, first.data.start)?)
    }

    // The window each made member's decode holds, by the engine's rule,
    // worked out from every made entry: what the windows filed answer.
    #[cfg(test)]
    pub(super) fn made_windows(&self) -> impl Iterator<Item = u64> + '_ {
        self.made.iter().filter_map(|&id| {
            let entry = self.entries.get(id)?;
            let first = entry.pieces.first()?;
            member_window(false, self.part_facts(first.volume.0, first.data.start)?)
        })
    }
}

// The run over a set's made members: the RAR engine's run, reading the
// store's envelope as the set's volumes. It reserves and borrows as the
// engine does, from the windows of the members made when it starts; a later
// member's wider window is borrowed, or widens the run, as the engine's
// does. It bids for nothing: the store bids ahead of the volume it reads
// headers in (`Object::wants`), and the run reads behind the store. A
// volume's place is settled as the store holds it: by its headers, its name
// or its end, as a volume the engine holds on trust is placed by its tail.
impl Engine for RarStore {
    fn start(&mut self) -> Box<dyn Extraction> {
        self.started = true;
        // The layout numbers members in archive order, as a run's walk
        // does, so each made entry is the member at its own index: the run
        // checks the name of each one laid by now, and makes those laid
        // later by the same rule.
        let named: BTreeMap<usize, String> = self
            .made
            .iter()
            .filter_map(|&id| Some((id, self.entries.get(id)?.name.clone())))
            .collect();
        // The password is handed as proved by the one answer a file's key
        // is put on trial by (`proven`). The run's own decode proves nothing
        // here: the store does not hear how a member it makes closes.
        let password = self.password();
        let proven = password.is_some_and(|index| self.proven(index));
        let password = password.map(|index| self.candidates[index].secret.as_str().to_owned());
        crate::rar_engine::run_of(password, proven, Scope::Members(named))
    }

    fn placed(&self, volume: VolumeIndex) -> bool {
        self.volumes
            .get(&volume)
            .is_none_or(|found| found.closed || claim::settled(volume, found))
    }

    // The widest window filed, as the engine's rule takes the widest of
    // every made member's.
    fn reserve(&self) -> Option<u64> {
        let widest = self.widest.last_key_value().map(|(&window, _)| window);
        #[cfg(test)]
        assert_eq!(
            widest,
            self.made_windows().max(),
            "the windows filed are the made members' windows"
        );
        crate::engine::reserve::<Rar>(widest)
    }

    fn lends(&self) -> bool {
        Rar::lends()
    }

    fn makes(&self, member: MemberId) -> bool {
        self.made.contains(&(member.0 as usize))
    }
}
