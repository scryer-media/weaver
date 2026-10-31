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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use unrar_rs::{
    ArchiveFormat, IneligibilityReason, MalformedReason, MemberEligibility, RarVolumeFacts,
    StoredLayoutBuilder, StoredMember,
};
use weaver_graph::{ByteRange, Emit, Handed, MemberId, RecoveryKind, VolumeIndex, Why};

use super::{Entry, Lock, Piece, RarStore, Volume, claim};
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

// Whether an announced entry is still the member the layout lists: every
// part the layout lists for it so far is the part the entry holds.
fn same(entry: &Entry, member: &StoredMember) -> bool {
    entry.name == member.name
        && entry.pieces.iter().zip(&member.parts).all(|(piece, part)| {
            piece.volume.0 == part.volume
                && piece.data == ByteRange::at(part.data_offset, part.data_size)
                && piece.after == part.split_after
        })
}

// The headers the layout takes a volume on, and whether it reads on past
// it. A volume held where its headers do not place it is mapped by no
// layout: it is given back at its close. One only its links put where it
// is held waits for its end record before it is mapped. A layout reads past
// a volume only once its headers are read to their end and say another
// follows.
fn lays(index: VolumeIndex, found: &Volume) -> Option<(&RarVolumeFacts, bool)> {
    let facts = found.walk.facts.as_ref()?;
    let unread = facts.members.is_empty() && !found.walk.ended;
    if unread || claim::misplaced(index, facts) || !claim::settled(index, found) {
        return None;
    }
    Some((facts, found.walk.ended && !found.walk.is_last()))
}

// How many volumes a layout holds: always the first ones, in order.
fn laid(layout: &StoredLayoutBuilder) -> u32 {
    layout.header_frontier().map_or(0, |last| last + 1)
}

// Whether a layout still stands after the volumes in `touched` changed:
// every one it holds is still laid on headers listing the members they
// did, and, short of its tail, still reads on. Adding a volume again on the
// same headers changes nothing; on others it is refused, and the layout is
// laid afresh.
fn stands(
    volumes: &BTreeMap<VolumeIndex, Volume>,
    layout: &mut StoredLayoutBuilder,
    touched: &BTreeSet<VolumeIndex>,
) -> bool {
    let laid = laid(layout);
    touched.range(..VolumeIndex(laid)).all(|index| {
        let Some((facts, on)) = volumes.get(index).and_then(|found| lays(*index, found)) else {
            return false;
        };
        (on || index.0 + 1 == laid) && !facts.is_solid && layout.add_volume(index.0, facts).is_ok()
    })
}

// One past the volumes the layout takes past its tail, from `from`, or the
// refusal one of them makes: found before any is added, so no layout the
// set is given up over is ever extended.
fn reach(
    volumes: &BTreeMap<VolumeIndex, Volume>,
    layout: &StoredLayoutBuilder,
    from: u32,
) -> Result<u32, Emit> {
    let tail = from.checked_sub(1).map(VolumeIndex);
    if tail.is_some_and(|tail| {
        volumes
            .get(&tail)
            .and_then(|found| lays(tail, found))
            .is_none_or(|(_, on)| !on)
    }) {
        return Ok(from);
    }
    let mut end = from;
    for (index, found) in volumes.range(VolumeIndex(from)..) {
        if index.0 != end {
            break;
        }
        let Some((facts, on)) = lays(*index, found) else {
            break;
        };
        if facts.is_solid {
            return Err(Emit::NeedsEngine(Why::Compressed));
        }
        if facts.archive_format() != layout.format() {
            return Err(Emit::Refuse(Why::Unsupported));
        }
        end += 1;
        if !on {
            break;
        }
    }
    Ok(end)
}

impl RarStore {
    // The format the set's first volume lays its layout in, once its
    // headers are read and put it first. One held first that they do not
    // stops the layout before it starts; a format no layout reads gives the
    // set up.
    fn first_format(&mut self, out: &mut Vec<Emit>) -> Option<ArchiveFormat> {
        let format = self
            .volumes
            .get(&VolumeIndex(0))?
            .walk
            .facts
            .as_ref()
            .filter(|facts| !claim::misplaced(VolumeIndex(0), facts))?
            .archive_format();
        if !matches!(format, ArchiveFormat::Rar4 | ArchiveFormat::Rar5) {
            self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
            return None;
        }
        Some(format)
    }

    // A layout laid afresh over the first `through` volumes, as one kept
    // over them was before anything was added past them.
    fn laid_afresh(&self, format: ArchiveFormat, through: u32) -> StoredLayoutBuilder {
        let mut layout = StoredLayoutBuilder::new(format);
        for (index, found) in self.volumes.range(..VolumeIndex(through)) {
            if let Some(facts) = found.walk.facts.as_ref() {
                // Each was added before on these same headers.
                let _ = layout.add_volume(index.0, facts);
            }
        }
        layout
    }

    // Hold the layout to the volumes touched and extend it past its tail;
    // lay it afresh where a volume it holds changed what it says.
    pub(super) fn sync(&mut self, touched: &BTreeSet<VolumeIndex>, out: &mut Vec<Emit>) {
        let Some(format) = self.first_format(out) else {
            return;
        };
        let kept = self
            .layout
            .take_if(|layout| layout.format() == format && stands(&self.volumes, layout, touched));
        let afresh = kept.is_none();
        let mut layout = kept.unwrap_or_else(|| {
            #[cfg(test)]
            super::counts::bump(|counts| counts.layouts += 1);
            StoredLayoutBuilder::new(format)
        });
        let from = laid(&layout);
        let end = match reach(&self.volumes, &layout, from) {
            Ok(end) => end,
            Err(emit) => {
                if !afresh {
                    self.layout = Some(layout);
                }
                return self.give_up(emit, out);
            }
        };
        let mut refusal = None;
        for (index, found) in self.volumes.range(VolumeIndex(from)..VolumeIndex(end)) {
            let added = found
                .walk
                .facts
                .as_ref()
                .is_some_and(|facts| layout.add_volume(index.0, facts).is_ok());
            if !added {
                refusal = Some(Emit::Refuse(Why::Unsupported));
                break;
            }
        }
        let members = layout.members();
        // An entry stands until the layout lists something else for it. A
        // layout laid afresh may reach less far than the one before, as when
        // a volume's length is said anew and the headers at its new end are
        // not in yet: what it does not list yet is unread, not refuted, and
        // its bytes stay where they were placed.
        let keep = self
            .entries
            .iter()
            .zip(members)
            .position(|(entry, member)| !same(entry, member))
            .unwrap_or(self.entries.len());
        for id in (keep..self.entries.len()).rev() {
            out.push(Emit::Retract(MemberId(id as u32)));
        }
        if keep < self.entries.len() {
            self.entries.truncate(keep);
            self.remap();
        }
        for (id, member) in members.iter().enumerate() {
            if refusal.is_some() {
                break;
            }
            if let Some(emit) = verdict(member) {
                refusal = Some(emit);
                break;
            }
            if id == self.entries.len() {
                self.announce(member, out);
            }
            self.extend(id, member, out);
        }
        // Recovery records are said for the volumes the layout newly holds
        // or that changed, every one it holds when it was laid afresh.
        let recovery: BTreeSet<VolumeIndex> = match afresh {
            true => self.volumes.keys().copied().collect(),
            false => touched
                .iter()
                .copied()
                .chain((from..end).map(VolumeIndex))
                .collect(),
        };
        self.say_recovery(&layout, &recovery, out);
        match refusal {
            Some(emit) => {
                // The set is given up over what was added: the map stays
                // the one kept before.
                if !afresh {
                    self.layout = Some(self.laid_afresh(format, from));
                }
                self.give_up(emit, out);
            }
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
            self.unmapped.remove(&VolumeIndex(part.volume));
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
        if !entry.judges_its_checksum()
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
    // of its own. Only the volumes in `changed` can have one not yet said.
    fn say_recovery(
        &mut self,
        layout: &StoredLayoutBuilder,
        changed: &BTreeSet<VolumeIndex>,
        out: &mut Vec<Emit>,
    ) {
        let handed = self
            .header_password()
            .map_or_else(Handed::default, Handed::new);
        for volume in changed {
            let Some(found) = self.volumes.get(volume) else {
                continue;
            };
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
