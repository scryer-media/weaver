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

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use unrar_rs::{
    ArchiveFormat, IneligibilityReason, MalformedReason, MemberEligibility, RarVolumeFacts,
    RarVolumeMemberFacts, StoredLayoutBuilder, StoredMember,
};
use weaver_graph::{ByteRange, Coverage, Emit, Handed, MemberId, RecoveryKind, VolumeIndex, Why};

use super::walk::Walk;
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

// The member a volume ends on, when it carries on into the next one.
fn carried_on(facts: &RarVolumeFacts) -> Option<&RarVolumeMemberFacts> {
    let last = facts.members.iter().max_by_key(|member| member.order)?;
    last.split_after.then_some(last)
}

// Where among `facts`' members sits the one the volume opens on, when it
// carries on `tail`, the member the volume before ends on, and says of it
// otherwise than `tail` does: another name, or another size for the whole
// member. Which set a volume is of, and where in it, is the claim's to say,
// and a name is evidence there. Once the volume is laid after the one
// before, RAR reads the part it opens on as the part that volume carries
// on, by place alone: unrar's MergeArchive (volume.cpp) reads the next
// volume's header in place of the last and compares neither name nor size,
// and CmdExtract::UnstoreFile (extract.cpp) writes a stored member to the
// size its first part states. Packers name a member one way in its first
// part and another in the rest, and misstate its size in the middle parts.
// The member is what its first part says, and the CRC its last part states
// proves its bytes, as unrar's ValidCRC check does.
fn continuation(tail: &RarVolumeMemberFacts, facts: &RarVolumeFacts) -> Option<usize> {
    let (at, head) = facts
        .members
        .iter()
        .enumerate()
        .min_by_key(|(_, member)| member.order)?;
    let differs = head.name != tail.name || head.unpacked_size != tail.unpacked_size;
    (head.split_before && differs).then_some(at)
}

// Whether a volume is held where it is on trust: its headers state no
// position, so only its name or its links put it there.
fn on_trust(found: &Volume) -> bool {
    found
        .stated()
        .is_none_or(|facts| !claim::states_position(facts))
}

// The volume at which a member's parts stop joining into one member, when
// what the volumes laid say of it, in the places they are laid, disagrees:
// the volume the layout names, else the one holding the last part it took,
// where the chain closed or ran past the member's size.
fn breaks_at(member: &StoredMember) -> Option<u32> {
    use MalformedReason as Reason;
    let MemberEligibility::Ineligible(IneligibilityReason::MalformedChain(reason)) =
        &member.eligibility
    else {
        return None;
    };
    match reason {
        Reason::DuplicatePartInVolume { volume }
        | Reason::OverlappingParts { volume }
        | Reason::UnexpectedChainStart { volume }
        | Reason::UnexpectedChainEnd { volume }
        | Reason::MissingPartInAddedVolume { volume }
        | Reason::MixedEncryption { volume } => Some(*volume),
        Reason::ContinuationInFirstVolume => Some(0),
        _ => member.parts.last().map(|part| part.volume),
    }
}

// A layout and, for each volume it holds, the facts it took it on: their
// stamp and how many members they list, and the member it ends on when it
// carries one on, as the layout took it.
pub(super) struct Laid {
    builder: StoredLayoutBuilder,
    added: BTreeMap<u32, (u64, usize)>,
    tails: BTreeMap<u32, RarVolumeMemberFacts>,
}

impl std::ops::Deref for Laid {
    type Target = StoredLayoutBuilder;

    fn deref(&self) -> &StoredLayoutBuilder {
        &self.builder
    }
}

impl Laid {
    fn new(format: ArchiveFormat) -> Self {
        Self {
            builder: StoredLayoutBuilder::new(format),
            added: BTreeMap::new(),
            tails: BTreeMap::new(),
        }
    }

    // A volume's facts as the layout takes them. The builder links a
    // member's parts by name and holds them to one stated size, so a volume
    // that opens on the member the one before carries on, saying otherwise
    // of it, has it as the layout took that member: the name and size its
    // first part gives it, as unrar extracts it. The sum of the parts' bytes
    // must still come to that size, and the last part's CRC proves them.
    fn joined<'a>(&self, index: u32, facts: &'a RarVolumeFacts) -> Cow<'a, RarVolumeFacts> {
        let carried = index
            .checked_sub(1)
            .and_then(|before| self.tails.get(&before))
            .and_then(|tail| Some((tail, continuation(tail, facts)?)));
        let Some((tail, at)) = carried else {
            return Cow::Borrowed(facts);
        };
        let mut joined = facts.clone();
        let head = &mut joined.members[at];
        head.name.clone_from(&tail.name);
        head.name_raw.clone_from(&tail.name_raw);
        head.unpacked_size = tail.unpacked_size;
        Cow::Owned(joined)
    }

    // Add a volume on its walk's facts, or say the layout refuses them.
    // unrar-rs takes a volume it holds again only on headers whose member
    // snapshots, one per member and drawn from the members and the format
    // alone, equal the ones it took it on. So it takes it again unchanged
    // on the very facts it took it on, and refuses facts listing another
    // number of members, without either being asked.
    fn add(&mut self, index: u32, walk: &Walk) -> bool {
        let Some(facts) = walk.facts.as_ref() else {
            return false;
        };
        let known = match self.added.get(&index) {
            Some(&(stamp, _)) if stamp == walk.stamp => Some(true),
            Some(&(_, members)) if members != facts.members.len() => Some(false),
            _ => None,
        };
        let added = match known {
            Some(added) => {
                #[cfg(test)]
                {
                    // Taking a volume it holds again changes nothing either
                    // way, so asking costs the test only time.
                    let joined = self.joined(index, facts);
                    assert_eq!(
                        added,
                        self.builder.add_volume(index, &joined).is_ok(),
                        "the layout answers as known"
                    );
                    super::counts::bump(|counts| counts.known += 1);
                }
                return added;
            }
            None => {
                #[cfg(test)]
                super::counts::bump(|counts| counts.asked.push((index, walk.stamp)));
                let joined = self.joined(index, facts);
                let added = self.builder.add_volume(index, &joined).is_ok();
                if added {
                    match carried_on(&joined) {
                        Some(tail) => self.tails.insert(index, tail.clone()),
                        None => self.tails.remove(&index),
                    };
                }
                added
            }
        };
        if added {
            self.added.insert(index, (walk.stamp, facts.members.len()));
        }
        added
    }
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
    layout: &mut Laid,
    touched: &BTreeSet<VolumeIndex>,
) -> bool {
    let laid = laid(layout);
    touched.range(..VolumeIndex(laid)).all(|index| {
        let Some(found) = volumes.get(index) else {
            return false;
        };
        let Some((facts, on)) = lays(*index, found) else {
            return false;
        };
        (on || index.0 + 1 == laid) && !facts.is_solid && layout.add(index.0, &found.walk)
    })
}

// One past the volumes the layout takes past its tail, from `from`, or the
// refusal one of them makes: found before any is added, so no layout the
// set is given up over is ever extended. A volume it refuted on the headers
// it holds now is not taken.
fn reach(
    volumes: &BTreeMap<VolumeIndex, Volume>,
    layout: &StoredLayoutBuilder,
    from: u32,
    refuted: Option<(VolumeIndex, u64)>,
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
        if index.0 != end || refuted == Some((*index, found.walk.stamp)) {
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
    fn laid_afresh(&self, format: ArchiveFormat, through: u32) -> Laid {
        let mut layout = Laid::new(format);
        for (index, found) in self.volumes.range(..VolumeIndex(through)) {
            // Each was added before on these same headers.
            layout.add(index.0, &found.walk);
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
        if afresh {
            self.refuted = None;
        }
        let mut layout = kept.unwrap_or_else(|| {
            #[cfg(test)]
            super::counts::bump(|counts| counts.layouts += 1);
            Laid::new(format)
        });
        let from = laid(&layout);
        let mut end = match reach(&self.volumes, &layout, from, self.refuted) {
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
            if !layout.add(index.0, &found.walk) {
                refusal = Some(Emit::Refuse(Why::Unsupported));
                end = index.0 + 1;
                break;
            }
        }
        // A volume only its name or its links hold where it is, its headers
        // stating no number, as a RAR4 volume's do short of a numbered end
        // record, is held there on trust. A layout that cannot take it there
        // refutes that place, not the set: when every volume from the one it
        // breaks at to its tail is held on trust, it is laid once more short
        // of that volume, which waits for its own headers or a new name to
        // place it, and is not laid there again on the same headers.
        let broken = match refusal {
            Some(_) => Some(end - 1),
            None => layout.members().iter().filter_map(breaks_at).min(),
        };
        if let Some(at) = broken.filter(|at| (from..end).contains(at))
            && self
                .volumes
                .range(VolumeIndex(at)..VolumeIndex(end))
                .all(|(_, found)| on_trust(found))
        {
            #[cfg(test)]
            super::counts::bump(|counts| counts.cut += 1);
            self.refuted = Some((VolumeIndex(at), self.volumes[&VolumeIndex(at)].walk.stamp));
            end = at;
            refusal = None;
            layout = self.laid_afresh(format, end);
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
            condemned_on: Coverage::new(),
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
            let at_piece = self.entries[id].pieces.len();
            self.pieces_in
                .entry(VolumeIndex(part.volume))
                .or_default()
                .push((id, at_piece));
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
    //
    // A volume carries one record, and the record finds itself in the
    // volume when it rules, so a volume's first recovery header is the one
    // said: further headers in it would build more records over the same
    // volume, each ruling alike. A volume said already is passed over
    // without reading its headers again.
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
            let said = self
                .recovery_said
                .range((*volume, 0)..=(*volume, u64::MAX))
                .next()
                .is_some();
            if said {
                continue;
            }
            let Some(found) = self.volumes.get(volume) else {
                continue;
            };
            let Some(facts) = found.walk.facts.as_ref() else {
                continue;
            };
            if !layout.has_volume(volume.0) {
                continue;
            }
            let Some(service) = facts.services.iter().find(|service| service.name == "RR") else {
                continue;
            };
            self.recovery_said.insert((*volume, service.data_offset));
            out.push(Emit::RecoveryAt {
                volume: *volume,
                range: ByteRange::at(service.data_offset, service.data_size),
                kind: RecoveryKind::Rar,
                handed: handed.clone(),
            });
        }
    }

    // The record of each volume in `changed` held by its name alone, its
    // headers stating nothing and no byte of its front in hand, as a
    // volume claimed past its hole holds none: no header says
    // where its record lies, and the record finds itself from the
    // volume's end, as RAR finds a record whose volume lost its headers.
    // It is said there, over no bytes, once the volume is closed;
    // a volume said already is passed over, so the record its headers
    // name once it is rebuilt is not said again.
    pub(super) fn say_records_unheaded(
        &mut self,
        changed: &BTreeSet<VolumeIndex>,
        out: &mut Vec<Emit>,
    ) {
        for volume in changed {
            let Some(found) = self.volumes.get(volume) else {
                continue;
            };
            let (Some(_), None, Some(length)) = (&found.name, found.stated(), found.length) else {
                continue;
            };
            // Only a closed volume has lost its front for good: before then
            // its first bytes may still be on their way.
            if !found.closed || found.walk.hole(&found.kept, found.closed) > 0 {
                continue;
            }
            let said = self
                .recovery_said
                .range((*volume, 0)..=(*volume, u64::MAX))
                .next()
                .is_some();
            if said {
                continue;
            }
            self.recovery_said.insert((*volume, length));
            out.push(Emit::RecoveryAt {
                volume: *volume,
                range: ByteRange::new(length, length),
                kind: RecoveryKind::Rar,
                handed: self
                    .header_password()
                    .map_or_else(Handed::default, Handed::new),
            });
        }
    }
}
