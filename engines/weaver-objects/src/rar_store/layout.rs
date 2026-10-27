// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The set's layout. Every volume's facts go into unrar-rs's stored-layout
// builder in volume order, from the first, each one once the volume before
// it has been read to its end. The builder links the parts of each member
// across volumes, says what each member is, and maps every volume byte to a
// member or the envelope. Members are numbered in its order, which is the
// order every run of the same set numbers them; when new facts disagree with
// a member already announced, it and every member after it are retracted.
// A layout the store gives the set up over never becomes its map: no byte
// was placed by it, so the bytes go back from where the last one put them.

use std::sync::Arc;

use unrar_rs::{
    ArchiveFormat, IneligibilityReason, MalformedReason, MemberEligibility, StoredLayoutBuilder,
    StoredMember,
};
use weaver_graph::{ByteRange, Emit, Handed, MemberId, RecoveryKind, VolumeIndex, Why};

use super::{Entry, Lock, Piece, RarStore, claim};
use crate::runs::CrcRuns;

// A name as the namespace numbers it: forward slashes, a directory with a
// trailing one, as the 7z store names them.
fn member_name(raw: &str, dir: bool) -> String {
    let mut name = raw.replace('\\', "/");
    if dir && !name.ends_with('/') {
        name.push('/');
    }
    name
}

// What a member's eligibility leaves the store to say: nothing while it
// streams here, else why an engine or nobody takes the set.
fn verdict(member: &StoredMember) -> Option<Emit> {
    use IneligibilityReason as Reason;
    let reason = match &member.eligibility {
        MemberEligibility::DirectEligible | MemberEligibility::ProvisionallyDirect => return None,
        MemberEligibility::EncryptedStore(store) => {
            return store
                .cipher_size
                .is_none()
                .then_some(Emit::NeedsEngine(Why::Unsupported));
        }
        MemberEligibility::Ineligible(reason) => reason,
    };
    Some(match reason {
        Reason::Directory => return None,
        Reason::Compressed { .. } | Reason::Solid => Emit::NeedsEngine(Why::Compressed),
        Reason::Encrypted => Emit::NeedsEngine(Why::Encrypted),
        Reason::Redirection => Emit::NeedsEngine(Why::NotAFile),
        Reason::MalformedChain(MalformedReason::ContinuationInFirstVolume) => {
            Emit::Refuse(Why::FirstBytesLost)
        }
        Reason::MalformedChain(
            MalformedReason::SizeMismatch { .. }
            | MalformedReason::ExceedsDeclaredSize { .. }
            | MalformedReason::MissingUnpackedSize
            | MalformedReason::InconsistentUnpackedSize { .. }
            | MalformedReason::MixedEncryption { .. },
        )
        | Reason::NoChecksum
        | Reason::Blake2OnlyNoCrc32 => Emit::NeedsEngine(Why::Unsupported),
        Reason::MalformedChain(_) => Emit::Refuse(Why::Unsupported),
    })
}

// Whether an announced entry is still the member the layout lists.
fn same(entry: &Entry, member: &StoredMember) -> bool {
    entry.name == member.name
        && entry.pieces.len() <= member.parts.len()
        && entry.pieces.iter().zip(&member.parts).all(|(piece, part)| {
            piece.volume.0 == part.volume
                && piece.data == ByteRange::at(part.data_offset, part.data_size)
                && piece.after == part.split_after
        })
}

impl RarStore {
    // A fresh layout over every volume read far enough.
    fn build(&mut self, out: &mut Vec<Emit>) -> Option<StoredLayoutBuilder> {
        // A volume held where its headers do not place it is mapped by no
        // layout: it is given back at its close. One only its links put
        // where it is held waits for its end record before it is mapped.
        // One held first stops the layout before it starts.
        let first = self
            .volumes
            .get(&VolumeIndex(0))?
            .walk
            .facts
            .as_ref()
            .filter(|facts| !claim::misplaced(VolumeIndex(0), facts))?;
        let format = first.archive_format();
        if !matches!(format, ArchiveFormat::Rar4 | ArchiveFormat::Rar5) {
            self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
            return None;
        }
        let mut layout = StoredLayoutBuilder::new(format);
        for (next, (index, found)) in (0..).zip(&self.volumes) {
            if index.0 != next {
                break;
            }
            let Some(facts) = found.walk.facts.as_ref() else {
                break;
            };
            if (facts.members.is_empty() && !found.walk.ended)
                || claim::misplaced(*index, facts)
                || !claim::settled(*index, found)
            {
                break;
            }
            let refusal = if facts.is_solid {
                Some(Emit::NeedsEngine(Why::Compressed))
            } else {
                layout
                    .add_volume(index.0, facts)
                    .err()
                    .map(|_| Emit::Refuse(Why::Unsupported))
            };
            if let Some(emit) = refusal {
                self.give_up(emit, out);
                return None;
            }
            if !found.walk.ended || found.walk.is_last() {
                break;
            }
        }
        Some(layout)
    }

    pub(super) fn sync(&mut self, out: &mut Vec<Emit>) {
        let Some(layout) = self.build(out) else {
            return;
        };
        let members = layout.members();
        let keep = self
            .entries
            .iter()
            .zip(members)
            .take_while(|(entry, member)| same(entry, member))
            .count();
        for id in (keep..self.entries.len()).rev() {
            out.push(Emit::Retract(MemberId(id as u32)));
        }
        self.entries.truncate(keep);
        let mut refusal = None;
        for (id, member) in members.iter().enumerate() {
            if let Some(emit) = verdict(member) {
                refusal = Some(emit);
                break;
            }
            if id == self.entries.len() {
                self.announce(member, out);
            }
            self.extend(id, member, out);
        }
        self.say_recovery(&layout, out);
        match refusal {
            Some(emit) => self.give_up(emit, out),
            None => self.layout = Some(layout),
        }
    }

    fn announce(&mut self, member: &StoredMember, out: &mut Vec<Emit>) {
        let dir = matches!(
            member.eligibility,
            MemberEligibility::Ineligible(IneligibilityReason::Directory)
        );
        let length = if dir { Some(0) } else { member.unpacked_size };
        let id = MemberId(self.entries.len() as u32);
        out.push(Emit::Member {
            member: id,
            name: member_name(&member.name, dir),
            length,
        });
        let store = member.eligibility.encrypted_store();
        self.entries.push(Entry {
            name: member.name.clone(),
            length,
            crc: None,
            uses_mac: false,
            lock: match store {
                Some(_) => Lock::Locked,
                None => Lock::Plain,
            },
            store,
            pieces: Vec::new(),
            complete: dir,
            hash_key: None,
            trial: None,
            out: Arc::default(),
            plain_runs: CrcRuns::default(),
            parts: Default::default(),
        });
        if dir {
            Arc::make_mut(&mut self.entries[id.0 as usize].out).closed = true;
            out.push(Emit::MemberClosed(id));
        }
    }

    // The parts the layout gives a member past those it already has.
    fn extend(&mut self, id: usize, member: &StoredMember, out: &mut Vec<Emit>) {
        for part in member.parts.iter().skip(self.entries[id].pieces.len()) {
            let Some(at) = part.logical_offset else {
                break;
            };
            self.entries[id].pieces.push(Piece {
                volume: VolumeIndex(part.volume),
                data: ByteRange::at(part.data_offset, part.data_size),
                at,
                crc: part.split_after.then_some(part.packed_crc32).flatten(),
                uses_mac: part.packed_hash_uses_mac,
                after: part.split_after,
            });
        }
        let entry = &mut self.entries[id];
        entry.store = member.eligibility.encrypted_store();
        if entry.complete || !member.chain_complete {
            return;
        }
        entry.complete = true;
        entry.crc = member.data_crc32;
        entry.uses_mac = member.data_hash_uses_mac;
        // A keyed checksum is not the member's own CRC32.
        if !entry.uses_mac
            && let Some(crc32) = entry.crc
        {
            out.push(Emit::Checksum {
                member: MemberId(id as u32),
                crc32,
            });
        }
        if entry.plain_len() == Some(0) && !entry.out.closed {
            Arc::make_mut(&mut entry.out).closed = true;
            out.push(Emit::MemberClosed(MemberId(id as u32)));
        }
    }

    // Each volume's recovery record, once, handed the candidate that opened
    // the headers it was found under, or nothing when they are open to
    // anyone: the record reads its volume under it and proves no candidate
    // of its own.
    fn say_recovery(&mut self, layout: &StoredLayoutBuilder, out: &mut Vec<Emit>) {
        let handed = self
            .header_password()
            .map_or_else(Handed::default, Handed::new);
        for (volume, found) in &self.volumes {
            let Some(facts) = found.walk.facts.as_ref() else {
                continue;
            };
            if !layout.has_volume(volume.0) {
                continue;
            }
            for service in facts.services.iter().filter(|service| service.name == "RR") {
                if self.recovery_said.insert((*volume, service.data_offset)) {
                    out.push(Emit::RecoveryAt {
                        volume: *volume,
                        range: ByteRange::at(service.data_offset, service.data_size),
                        kind: RecoveryKind::Rar,
                        handed: handed.clone(),
                    });
                }
            }
        }
    }
}
