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
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use unrar_rs::{
    ArchiveFormat, EncryptedStore, IneligibilityReason, MalformedReason, MemberEligibility,
    RarVolumeFacts, RarVolumeFactsDelta, RarVolumeMemberFacts, StoredLayoutBuilder, StoredMember,
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

// Whether a member streams here at the bytes its parts carry, short of the
// size its first part states: a stored member in the clear whose parts, all
// in, carry fewer bytes than that. unrar writes such a member until its
// last part ends (CmdExtract::UnstoreFile, extract.cpp) and judges those
// bytes by the CRC its last part states (ValidCRC), so a CRC32 is required,
// and the store closes it there only once that CRC proves them. Parts
// carrying more than it states are not: unrar writes only the stated size,
// and the CRC fails. `in_clear` reads its first part's headers.
fn carried_short(member: &StoredMember, in_clear: impl FnOnce() -> bool) -> bool {
    matches!(
        member.eligibility,
        MemberEligibility::Ineligible(IneligibilityReason::MalformedChain(
            MalformedReason::SizeMismatch {
                packed_total,
                unpacked_size,
            },
        )) if packed_total < unpacked_size
    ) && member.data_crc32.is_some()
        && in_clear()
}

// What a member's eligibility leaves the store to say: nothing while it
// streams here, else why an engine or nobody takes the set.
fn verdict(member: &StoredMember, in_clear: impl FnOnce() -> bool) -> Option<Emit> {
    use IneligibilityReason as Reason;
    if carried_short(member, in_clear) {
        return None;
    }
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

fn is_directory(member: &StoredMember) -> bool {
    matches!(
        member.eligibility,
        MemberEligibility::Ineligible(IneligibilityReason::Directory)
    )
}

// Whether a member's first part is stored in the clear, as the headers of
// the volume it starts in state it.
fn in_clear(volumes: &BTreeMap<VolumeIndex, Volume>, member: &StoredMember) -> bool {
    let Some(facts) = volumes
        .get(&VolumeIndex(member.first_volume))
        .and_then(|found| found.walk.facts.as_ref())
    else {
        return false;
    };
    let mut parts = facts
        .members
        .iter()
        .filter(|part| part.name == member.name)
        .peekable();
    parts.peek().is_some() && parts.all(|part| !part.is_encrypted)
}

// Whether an announced entry is still made of what the layout lists for
// the member: every part the layout lists for it so far is the part the
// entry holds. The name is not part of it: a repaired header that only
// names the member otherwise leaves its pieces as they are.
fn same(entry: &Entry, member: &StoredMember) -> bool {
    entry.pieces.iter().zip(&member.parts).all(|(piece, part)| {
        piece.volume.0 == part.volume
            && piece.data == ByteRange::at(part.data_offset, part.data_size)
            && piece.after == part.split_after
    })
}

// Whether an entry `same` holds, that its headers now name otherwise, is
// renamed where it stands, stored or made alike: one whose length stands
// and, when it is enciphered, whose pieces are keyed as before, so its
// bytes read the same. A made member's new name reaches its run as the
// run starts again: the repair replaced header bytes its shelf holds, so
// the job restarts the run, which names what it makes as it starts. An
// enciphered entry whose keying is not known, as one restored before its
// headers are walked again, is retracted as any other changed entry.
fn renamed_in_place(entry: &Entry, member: &StoredMember) -> bool {
    let length = match is_directory(member) {
        true => Some(0),
        false => member.unpacked_size,
    };
    let keyed = |store: Option<EncryptedStore>| store.map(|store| store.keying());
    entry.length == length && keyed(entry.store) == keyed(member.eligibility.encrypted_store())
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

// The member a volume ends on, and where it sits, when it carries on into
// the next one.
fn carried_on(facts: &RarVolumeFacts) -> Option<(usize, &RarVolumeMemberFacts)> {
    let (at, last) = facts
        .members
        .iter()
        .enumerate()
        .max_by_key(|(_, member)| member.order)?;
    last.split_after.then_some((at, last))
}

// A member renamed as the member the volume before carries on: the name
// and size its first part gives it.
fn rename(member: &mut RarVolumeMemberFacts, tail: &RarVolumeMemberFacts) {
    member.name.clone_from(&tail.name);
    member.name_raw.clone_from(&tail.name_raw);
    member.unpacked_size = tail.unpacked_size;
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
pub(super) fn on_trust(found: &Volume) -> bool {
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
// carries one on, as the layout took it. It finds a member by the name it
// lists it under, and keeps the first member any volume it took since it
// was last asked names: no member before that one changed since.
pub(super) struct Laid {
    builder: StoredLayoutBuilder,
    added: BTreeMap<u32, (u64, usize)>,
    tails: BTreeMap<u32, RarVolumeMemberFacts>,
    names: HashMap<String, usize>,
    low: Option<usize>,
    // Whether a volume it held was taken again on more headers since it was
    // last asked.
    grew: bool,
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
            names: HashMap::new(),
            low: None,
            grew: false,
        }
    }

    // The first member a volume taken since the last ask names, or none.
    fn take_low(&mut self) -> Option<usize> {
        self.low.take()
    }

    // Whether a volume it held was taken again on more headers since the
    // last ask.
    fn take_grew(&mut self) -> bool {
        std::mem::take(&mut self.grew)
    }

    // A volume's facts as the layout takes them. The builder links a
    // member's parts by name and holds them to one stated size, so a volume
    // that opens on the member the one before carries on, saying otherwise
    // of it, has it as the layout took that member: the name and size its
    // first part gives it, as unrar extracts it. The sum of the parts' bytes
    // must still come to that size, and the last part's CRC proves them.
    fn joined<'a>(&self, index: u32, facts: &'a RarVolumeFacts) -> Cow<'a, RarVolumeFacts> {
        let Some((tail, at)) = self.carried(index, facts) else {
            return Cow::Borrowed(facts);
        };
        let mut joined = facts.clone();
        rename(&mut joined.members[at], tail);
        Cow::Owned(joined)
    }

    // The member the volume before carries on, and where among `facts`'
    // members sits the one the volume opens on, when it says otherwise of
    // it.
    fn carried(
        &self,
        index: u32,
        facts: &RarVolumeFacts,
    ) -> Option<(&RarVolumeMemberFacts, usize)> {
        index
            .checked_sub(1)
            .and_then(|before| self.tails.get(&before))
            .and_then(|tail| Some((tail, continuation(tail, facts)?)))
    }

    // A delta of a volume's facts as the layout takes them: the member the
    // volume opens on is renamed as `joined` renames it when the delta
    // states it again, and is left as the layout took it when not.
    fn joined_delta(&self, index: u32, facts: &RarVolumeFacts, delta: &mut RarVolumeFactsDelta) {
        let Some((tail, at)) = self.carried(index, facts) else {
            return;
        };
        if let Some(head) = at
            .checked_sub(delta.members_from)
            .and_then(|at| delta.facts.members.get_mut(at))
        {
            rename(head, tail);
        }
    }

    // Add a volume on its walk's facts, or say the layout refuses them.
    // unrar-rs takes a volume it holds again unchanged on the very facts it
    // took it on, without being asked. Any other facts for it are the
    // builder's to judge: it takes the volume it added last again on the
    // headers it took it on followed by more, learning only those, and
    // refuses the rest without changing the layout.
    //
    // A volume it holds is handed only what changed in its facts since it
    // took them, when the walk can say: the work is in the size of what
    // changed, and the answer the one the whole facts get.
    fn add(&mut self, index: u32, walk: &Walk) -> bool {
        let Some(facts) = walk.facts.as_ref() else {
            return false;
        };
        let held = self.added.get(&index).copied();
        // The way the store looked before: a volume it held, on headers
        // listing another number of members, was refused unasked, and the
        // layout laid afresh.
        #[cfg(test)]
        if super::counts::old() && held.is_some_and(|(_, members)| members != facts.members.len()) {
            return false;
        }
        let known = match held {
            Some((stamp, _)) if stamp == walk.stamp => Some(true),
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
                let delta = held.and_then(|(stamp, _)| walk.delta_since(stamp));
                #[cfg(test)]
                let delta = delta.filter(|_| !super::counts::whole());
                // The members the layout is handed, and the first of them.
                let (added, members_from, members): (_, _, Cow<'_, [RarVolumeMemberFacts]>) =
                    match delta {
                        Some(mut delta) => {
                            self.joined_delta(index, facts, &mut delta);
                            let added = self.builder.add_volume_delta(index, &delta).is_ok();
                            (added, delta.members_from, Cow::Owned(delta.facts.members))
                        }
                        None => {
                            let joined = self.joined(index, facts);
                            let added = self.builder.add_volume(index, &joined).is_ok();
                            let members = match joined {
                                Cow::Borrowed(facts) => Cow::Borrowed(facts.members.as_slice()),
                                Cow::Owned(facts) => Cow::Owned(facts.members),
                            };
                            (added, 0, members)
                        }
                    };
                #[cfg(test)]
                super::counts::bump(|counts| counts.laid_members += members.len());
                if added {
                    match carried_on(facts) {
                        Some((at, tail)) => {
                            // The member the volume ends on as the layout
                            // took it: renamed when it is the one it opens on.
                            let mut tail = tail.clone();
                            if let Some((before, opens)) = self.carried(index, facts)
                                && opens == at
                            {
                                rename(&mut tail, before);
                            }
                            self.tails.insert(index, tail)
                        }
                        None => self.tails.remove(&index),
                    };
                    // A volume changes only the members it names: those it
                    // carries a part of, and so every one whose chain runs
                    // through it, the layout holding the first volumes only.
                    let builder_members = self.builder.members();
                    for (at, member) in builder_members.iter().enumerate().skip(self.names.len()) {
                        self.names.insert(member.name.clone(), at);
                    }
                    // A volume it held taken on more headers changes only
                    // the members those name: the builder learns only them.
                    let count = members_from + members.len();
                    let grown = held
                        .map(|(_, members)| members)
                        .filter(|members| *members < count);
                    self.grew |= grown.is_some();
                    let skip = grown.map_or(0, |held| held - members_from);
                    let low = members[skip..]
                        .iter()
                        .filter_map(|member| self.names.get(&member.name).copied())
                        .min();
                    self.low = self.low.into_iter().chain(low).min();
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
// every one it holds is still laid on headers the layout takes, and, short
// of its tail, still reads on. Adding a volume again on the same headers
// changes nothing, and the tail on those followed by more learns only
// those; on others it is refused, and the layout is laid afresh.
fn stands(
    volumes: &mut BTreeMap<VolumeIndex, Volume>,
    layout: &mut Laid,
    touched: &BTreeSet<VolumeIndex>,
) -> bool {
    let laid = laid(layout);
    touched.range(..VolumeIndex(laid)).all(|index| {
        let Some(found) = volumes.get_mut(index) else {
            return false;
        };
        let Some((facts, on)) = lays(*index, found) else {
            return false;
        };
        let added =
            (on || index.0 + 1 == laid) && !facts.is_solid && layout.add(index.0, &found.walk);
        if added {
            found.walk.handed();
        }
        added
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
    held_out: Option<u32>,
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
        if index.0 != end
            || refuted == Some((*index, found.walk.stamp))
            || held_out.is_some_and(|at| index.0 >= at)
        {
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

    // How many volumes the kept layout holds, from the first.
    pub(super) fn laid_through(&self) -> Option<u32> {
        self.layout.as_ref().map(|layout| laid(layout))
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

    // Every answer the kept layout gives is the one a layout laid in one go
    // gives, over the headers each volume it holds has now.
    #[cfg(test)]
    pub(super) fn assert_laid_as_in_one_go(&self) {
        let Some(layout) = self.layout.as_ref() else {
            return;
        };
        let through = laid(layout);
        let once = self.laid_afresh(layout.format(), through);
        assert_eq!(laid(&once), through, "the volumes laid");
        assert_eq!(layout.members(), once.members(), "the members");
        for (index, found) in self.volumes.range(..VolumeIndex(through)) {
            let volume = index.0;
            assert_eq!(
                layout.has_volume(volume),
                once.has_volume(volume),
                "{index:?}"
            );
            assert_eq!(
                layout.split_evidence(volume),
                once.split_evidence(volume),
                "{index:?}"
            );
            let length = found.length.unwrap_or(0);
            assert_eq!(
                layout.map_physical_range(volume, 0, length),
                once.map_physical_range(volume, 0, length),
                "{index:?}"
            );
        }
    }

    // Hold the layout to the volumes touched and extend it past its tail;
    // lay it afresh where a volume it holds changed what it says.
    pub(super) fn sync(&mut self, touched: &BTreeSet<VolumeIndex>, out: &mut Vec<Emit>) {
        let Some(format) = self.first_format(out) else {
            return;
        };
        #[cfg(test)]
        super::counts::bump(|counts| counts.syncs += 1);
        let mut kept = self.layout.take_if(|layout| {
            layout.format() == format && stands(&mut self.volumes, layout, touched)
        });
        let afresh = kept.is_none();
        // A volume taken again on more headers is what laying the layout
        // afresh once took it on: the volume refuted is looked at again.
        let grew = kept.as_mut().is_some_and(Laid::take_grew);
        if afresh || grew {
            self.refuted = None;
        }
        let mut layout = kept.unwrap_or_else(|| {
            #[cfg(test)]
            super::counts::bump(|counts| counts.layouts += 1);
            Laid::new(format)
        });
        // Nothing is mapped from a volume held the wrong way round beside
        // the next until the set is ordered: a layout already past it is
        // laid again short of it.
        let held_out = self.held_out();
        // Whether every member the layout lists is to be looked at: those
        // of one laid afresh are all new to it.
        let mut relaid = afresh;
        if let Some(at) = held_out.filter(|at| *at < laid(&layout)) {
            layout = self.laid_afresh(format, at);
            relaid = true;
        }
        let from = laid(&layout);
        let mut end = match reach(&self.volumes, &layout, from, self.refuted, held_out) {
            Ok(end) => end,
            Err(emit) => {
                if !afresh {
                    self.layout = Some(layout);
                }
                return self.give_up(emit, out);
            }
        };
        let mut refusal = None;
        for (index, found) in self.volumes.range_mut(VolumeIndex(from)..VolumeIndex(end)) {
            if !layout.add(index.0, &found.walk) {
                refusal = Some(Emit::Refuse(Why::Unsupported));
                end = index.0 + 1;
                break;
            }
            found.walk.handed();
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
            // A chain that closes short of its member's size where a volume
            // is held on trust breaks there too, until the job is quiet with
            // nothing to order the volumes otherwise: the places they are
            // held at stand then, as unrar reads volumes in the order it
            // is given them.
            None => layout
                .members()
                .iter()
                .filter(|member| {
                    !(self.trusting && carried_short(member, || in_clear(&self.volumes, member)))
                })
                .filter_map(breaks_at)
                .min(),
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
            relaid = true;
        }
        // A kept layout changed no member before the first one a volume it
        // took since the last look names. Each member before it was looked
        // at then and said nothing that gave the set up, and is still the
        // member its entry holds: it is passed over. So is no announced
        // entry, which the loop below announces.
        let low = layout.take_low();
        // A slot retracted where it stood is looked at again from its own
        // place: the layout may lay its member there now.
        let waiting = self.waiting.first().copied().unwrap_or(usize::MAX);
        let start = match relaid {
            true => 0,
            false => low
                .unwrap_or(usize::MAX)
                .min(self.entries.len())
                .min(waiting),
        };
        #[cfg(test)]
        let start = if super::counts::old() { 0 } else { start };
        let members = layout.members();
        #[cfg(test)]
        for (entry, member) in self.entries.iter().zip(members).take(start) {
            // What the old look found of each member passed over: the same
            // member, no verdict but the run's to make it, and nothing to
            // extend.
            assert!(same(entry, member), "{}", member.name);
            assert!(
                entry.made || verdict(member, || in_clear(&self.volumes, member)).is_none(),
                "{}",
                member.name
            );
            assert!(
                member
                    .parts
                    .get(entry.pieces.len())
                    .is_none_or(|part| part.logical_offset.is_none()),
                "{}",
                member.name
            );
            assert!(entry.complete || !member.chain_complete, "{}", member.name);
            assert_eq!(
                entry.store,
                member.eligibility.encrypted_store(),
                "{}",
                member.name
            );
        }
        // An entry stands while it is still the member the layout lists at
        // its id: the same pieces, the same name or one it is renamed to
        // where it stands, and the same kind, made by the run or not. Each
        // is judged on its own: one that no longer stands is retracted where
        // it is, its slot and id kept for the member the layout lays there,
        // and the entries after it that stand are not touched. A layout laid
        // afresh may reach less far than the one before, as when a volume's
        // length is said anew and the headers at its new end are not in
        // yet: what it does not list yet is unread, not refuted, and a
        // stored entry's bytes stay where they were placed. A member the run
        // made that its headers no longer give the run, as a repaired header
        // that says it is stored, is something else: the store streams it.
        // A made entry stands only while the store holds every volume its
        // pieces lie in: its bytes stayed in the envelope, so one with a
        // piece in a volume the store gave back was made in an order no
        // longer the set's, and is made again where the layout lays it. One
        // whose volume the layout only dropped, held still, is compared
        // again when the layout lays that volume once more.
        let mut retracted = Vec::new();
        for (id, entry) in self.entries.iter().enumerate().skip(start) {
            if entry.waiting {
                continue;
            }
            let gone = entry.made
                && entry
                    .pieces
                    .iter()
                    .any(|piece| !self.volumes.contains_key(&piece.volume));
            let stands = match members.get(id) {
                None => !gone,
                Some(member) => {
                    #[cfg(test)]
                    super::counts::bump(|counts| counts.compared += 1);
                    same(entry, member)
                        && (entry.name == member.name || renamed_in_place(entry, member))
                        && !gone
                        && !(entry.made && !self.made_by_run(id, member, format))
                }
            };
            if !stands {
                retracted.push(id);
            }
        }
        // A made entry whose place the layout now gives a member the run
        // does not make, as a repaired header that says it is stored: its
        // data the shelf kept for the run is no longer the shelf's, and
        // comes again to the member streamed in its place. One made again
        // stays the run's, and its bytes stay where they were.
        for &id in &retracted {
            let entry = &self.entries[id];
            let streamed = entry.made
                && members
                    .get(id)
                    .is_some_and(|member| !self.made_by_run(id, member, format));
            if !streamed {
                continue;
            }
            for piece in &entry.pieces {
                out.push(Emit::Unkept {
                    volume: piece.volume,
                    range: piece.data,
                    envelope: piece.volume.0,
                    at: piece.data.start,
                });
            }
        }
        for &id in retracted.iter().rev() {
            out.push(Emit::Retract(MemberId(id as u32)));
        }
        if !retracted.is_empty() {
            for &id in &retracted {
                let name = std::mem::take(&mut self.entries[id].name);
                self.entries[id] = Entry::waiting(name);
            }
            // Slots at the tail hold nothing after them: the next member
            // laid there is announced as any new one is.
            while self.entries.last().is_some_and(|entry| entry.waiting) {
                self.entries.pop();
            }
            self.remap();
        }
        // An entry its headers now name otherwise is renamed where it
        // stands, under the name `announce` gives its kind: its bytes, and
        // every entry after it, are as they were.
        for (id, member) in members.iter().enumerate().skip(start) {
            let Some(entry) = self.entries.get_mut(id) else {
                break;
            };
            if entry.waiting || entry.name == member.name {
                continue;
            }
            entry.name.clone_from(&member.name);
            let name = match entry.made {
                true => super::made::name(&member.name),
                false => member_name(&member.name, is_directory(member)),
            };
            out.push(Emit::Member {
                member: MemberId(id as u32),
                name,
                length: entry.length,
            });
        }
        for (id, member) in members.iter().enumerate().skip(start) {
            if refusal.is_some() {
                break;
            }
            #[cfg(test)]
            super::counts::bump(|counts| counts.visited += 1);
            let made = self.made_by_run(id, member, format);
            // A link is one by the rule the run reads (`links`), whatever
            // the member's eligibility says: a RAR4 header has no
            // redirection record for it to show.
            let refused = || match self.links(member, format) {
                true => Some(Emit::NeedsEngine(Why::NotAFile)),
                false => verdict(member, || in_clear(&self.volumes, member)),
            };
            if !made && let Some(emit) = refused() {
                refusal = Some(emit);
                break;
            }
            if self.entries.get(id).is_none_or(|entry| entry.waiting) {
                self.announce(id, member, made, out);
            }
            self.extend(id, member, out);
        }
        self.file_windows(start);
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
            None => {
                self.layout = Some(layout);
            }
        }
    }

    // The member the layout lays at `id`: the next slot, or one retracted
    // where it stood.
    fn announce(&mut self, at: usize, member: &StoredMember, made: bool, out: &mut Vec<Emit>) {
        let dir = is_directory(member);
        let length = if dir { Some(0) } else { member.unpacked_size };
        let id = MemberId(at as u32);
        // A member the run makes is named as the RAR engine names it, so a
        // mixed set delivers it where the engine would (`made`).
        let name = match made {
            true => super::made::name(&member.name),
            false => member_name(&member.name, dir),
        };
        out.push(Emit::Member {
            member: id,
            name,
            length,
        });
        if made {
            self.made.insert(id.0 as usize);
        }
        let store = member.eligibility.encrypted_store();
        if store.is_some() {
            self.locked.insert(id.0 as usize);
        }
        self.waiting.remove(&at);
        let entry = Entry {
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
            made,
            waiting: false,
        };
        if entry.streams() {
            self.streamed += 1;
        }
        match self.entries.get_mut(at) {
            Some(slot) => *slot = entry,
            None => self.entries.push(entry),
        }
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
            // Only a stored member's part sends the volume's bytes to a
            // member's file; a made one's stay in the envelope, as the
            // engine's do, so its volume can still go back.
            if !self.entries[id].made {
                self.unmapped.remove(&VolumeIndex(part.volume));
            }
            let at_piece = self.entries[id].pieces.len();
            // Filed in data order, which a layout laying members in archive
            // order keeps by appending; one laid again into a slot before
            // others goes where its data starts.
            let filed = self.pieces_in.entry(VolumeIndex(part.volume)).or_default();
            let key = (part.data_offset, id, at_piece);
            let entries = &self.entries;
            let place =
                filed.partition_point(|&(other, at)| super::piece_order(entries, other, at) < key);
            filed.insert(place, (id, at_piece));
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
        // A member the run makes is the run's to check and close, as the
        // engine checks and closes it, once each run decodes it.
        if entry.made {
            return;
        }
        // A keyed checksum is not the member's own CRC32.
        if !entry.judges_its_checksum()
            && let Some(crc32) = entry.crc
        {
            out.push(Emit::Checksum {
                member: MemberId(id as u32),
                crc32,
            });
        }
        // No bytes have the CRC32 zero: a member whose parts carry none,
        // short of its stated size, ends there only when that is its CRC.
        let short = entry.short();
        let proved = !short || (!entry.uses_mac && entry.crc == Some(0));
        if entry.plain_len() == Some(0) && !entry.out.closed && proved {
            Arc::make_mut(&mut entry.out).closed = true;
            let member = MemberId(id as u32);
            if short {
                out.push(Emit::Ends { member, length: 0 });
            }
            out.push(Emit::MemberClosed(member));
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
        #[cfg(test)]
        super::counts::bump(|counts| counts.recovery_looked += changed.len());
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::sync::Arc;

    use unrar_rs::{KdfCache, RarArchive, RarVolumeFactsCursor};
    use weaver_graph::Provenance;

    use super::super::counts;
    use super::super::image::SparseImage;
    use super::super::tests::writer::{Item, Options, Version, write};
    use super::*;
    use crate::grid::crc32;
    use crate::kept::Kept;
    use crate::testing::payload;

    // One step of a volume as the layout taking deltas was handed it: the
    // members handed, the members the volume lists, and how many of them
    // the layout held before.
    #[derive(Debug)]
    struct Handing {
        handed: usize,
        listed: usize,
        held: usize,
    }

    // A set fed volume by volume in pieces of `piece` bytes, each piece
    // stepping the walk and handing the volume to two layouts, one taking
    // what changed and one the whole facts: after every step the two answer
    // alike. The walk restarts from the signature before piece `restart.1`
    // of volume `restart.0`.
    fn fed(volumes: &[Vec<u8>], piece: u64, restart: Option<(usize, usize)>) -> Vec<Handing> {
        let format = RarArchive::parse_volume_facts(Cursor::new(volumes[0].clone()), None)
            .expect("the first volume")
            .archive_format();
        let kdf = Arc::new(KdfCache::new());
        let mut by_delta = Laid::new(format);
        let mut by_whole = Laid::new(format);
        let mut handings = Vec::new();
        for (index, bytes) in volumes.iter().enumerate() {
            let length = bytes.len() as u64;
            let mut kept = Kept::default();
            kept.want(ByteRange::new(0, length));
            let mut walk = Walk::default();
            let pieces = length.div_ceil(piece) as usize;
            for at in 0..pieces {
                let range =
                    ByteRange::new(at as u64 * piece, ((at as u64 + 1) * piece).min(length));
                kept.absorb(
                    range,
                    &bytes[range.start as usize..range.end as usize],
                    Provenance::Fresh,
                );
                if restart == Some((index, at)) {
                    assert!(walk.cursor.resumes_at().is_some(), "a cursor to restart");
                    walk.changed_from(0);
                    assert_eq!(walk.cursor.resumes_at(), None);
                }
                walk.step(&mut kept, Some(length), None, &kdf, false);
                let where_ = format!("volume {index}, piece {at}");
                hand(
                    &mut by_delta,
                    &mut by_whole,
                    index as u32,
                    &mut walk,
                    &mut handings,
                );
                assert_alike(&by_delta, &by_whole, volumes, &where_);
            }
            assert!(walk.ended, "volume {index} is read to its end");
        }
        assert_eq!(
            laid(&by_delta),
            volumes.len() as u32,
            "every volume is laid"
        );
        handings
    }

    // Hand a volume to both layouts, as the store does once its headers
    // are read past the main header.
    fn hand(
        by_delta: &mut Laid,
        by_whole: &mut Laid,
        index: u32,
        walk: &mut Walk,
        handings: &mut Vec<Handing>,
    ) {
        let Some(facts) = walk.facts.as_ref() else {
            return;
        };
        if facts.members.is_empty() && !walk.ended {
            return;
        }
        let listed = facts.members.len();
        let held = by_delta
            .added
            .get(&index)
            .map_or(0, |(_, members)| *members);
        counts::take();
        let taken = by_delta.add(index, walk);
        let handed = counts::take().laid_members;
        counts::set_whole(true);
        let whole = by_whole.add(index, walk);
        counts::set_whole(false);
        assert_eq!(taken, whole, "volume {index}: both take it or neither");
        if taken {
            walk.handed();
        }
        handings.push(Handing {
            handed,
            listed,
            held,
        });
    }

    fn assert_alike(by_delta: &Laid, by_whole: &Laid, volumes: &[Vec<u8>], at: &str) {
        assert_eq!(
            by_delta.header_frontier(),
            by_whole.header_frontier(),
            "{at}"
        );
        assert_eq!(by_delta.members(), by_whole.members(), "{at}");
        assert_eq!(by_delta.added, by_whole.added, "{at}");
        assert_eq!(by_delta.tails, by_whole.tails, "{at}");
        assert_eq!(by_delta.names, by_whole.names, "{at}");
        assert_eq!(by_delta.grew, by_whole.grew, "{at}");
        for (volume, bytes) in volumes.iter().enumerate() {
            let volume = volume as u32;
            assert_eq!(
                by_delta.has_volume(volume),
                by_whole.has_volume(volume),
                "{at}: {volume}"
            );
            assert_eq!(
                by_delta.split_evidence(volume),
                by_whole.split_evidence(volume),
                "{at}: {volume}"
            );
            assert_eq!(
                by_delta.map_physical_range(volume, 0, bytes.len() as u64),
                by_whole.map_physical_range(volume, 0, bytes.len() as u64),
                "{at}: {volume}"
            );
        }
    }

    // Whether a cursor fed `bytes` in pieces ever states again members or
    // services it reported before.
    fn restates(bytes: &[u8], piece: u64) -> (bool, bool) {
        let kdf = Arc::new(KdfCache::new());
        let length = bytes.len() as u64;
        let mut kept = Kept::default();
        kept.want(ByteRange::new(0, length));
        let mut cursor = RarVolumeFactsCursor::new();
        let mut held: Option<RarVolumeFacts> = None;
        let mut restated = (false, false);
        for at in (0..length).step_by(piece as usize) {
            let range = ByteRange::new(at, (at + piece).min(length));
            kept.absorb(
                range,
                &bytes[range.start as usize..range.end as usize],
                Provenance::Fresh,
            );
            let Ok(delta) =
                cursor.step_delta(SparseImage::of(&kept, false), None, Arc::clone(&kdf))
            else {
                continue;
            };
            match held.as_mut() {
                Some(facts) => {
                    restated.0 |= delta.members_from < facts.members.len();
                    restated.1 |= delta.services_from < facts.services.len();
                    delta.apply_to(facts);
                }
                None => held = Some(delta.facts),
            }
        }
        restated
    }

    // A RAR4 block: its CRC, type, flags and size, then `body`.
    fn rar4_block(kind: u8, flags: u16, body: &[u8]) -> Vec<u8> {
        let mut block = vec![0, 0, kind];
        block.extend_from_slice(&flags.to_le_bytes());
        block.extend_from_slice(&((7 + body.len()) as u16).to_le_bytes());
        block.extend_from_slice(body);
        let crc = (crc32(&block[2..]) as u16).to_le_bytes();
        block[..2].copy_from_slice(&crc);
        block
    }

    // A RAR4 service header named `name`, with `trailer` after its name and
    // `payload` stored after it.
    fn rar4_service(name: &[u8], payload: &[u8], trailer: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        body.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        body.push(3);
        body.extend_from_slice(&crc32(payload).to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes());
        body.push(29);
        body.push(0x30);
        body.extend_from_slice(&(name.len() as u16).to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes());
        body.extend_from_slice(name);
        body.extend_from_slice(trailer);
        let flags = match payload.is_empty() {
            true => 0x4000,
            false => 0x8000 | 0x4000,
        };
        let mut block = rar4_block(0x7A, flags, &body);
        block.extend_from_slice(payload);
        block
    }

    // A RAR4 volume of twelve stored files with records after their data:
    // owners stated in a payload or in the header, comments, an old comment
    // and an old-style service. Each owner names the file before it, which a
    // walk reports before the owner arrives.
    fn owned_rar4() -> Vec<u8> {
        let items: Vec<Item> = (0..12)
            .map(|at| {
                let name: &'static str = Box::leak(format!("owned{at:02}.bin").into_boxed_str());
                Item::File(name, payload(at, 40 + at as usize * 9))
            })
            .collect();
        let mut volume = write(&items, &Options::new(Version::Rar4)).remove(0);
        let facts =
            RarArchive::parse_volume_facts(Cursor::new(volume.clone()), None).expect("the volume");
        for (at, member) in facts.members.iter().enumerate().rev() {
            let mut records = Vec::new();
            match at % 4 {
                1 => records.extend(rar4_service(
                    b"UOW",
                    format!("user{at}\0group{at}").as_bytes(),
                    &[],
                )),
                2 => records.extend(rar4_service(b"UOW", &[], b"harbour\0crew")),
                _ => {}
            }
            if at % 5 == 3 {
                records.extend(rar4_service(b"CMT", b"a comment", &[]));
            }
            if at == 4 {
                let mut body = 4u16.to_le_bytes().to_vec();
                body.extend_from_slice(&[29, 0x30, 0xEF, 0xBE]);
                body.extend_from_slice(b"note");
                records.extend(rar4_block(0x75, 0, &body));
            }
            if at == 7 {
                let mut body = 4u32.to_le_bytes().to_vec();
                body.extend_from_slice(&0x104u16.to_le_bytes());
                body.push(0);
                body.extend_from_slice(&4u32.to_le_bytes());
                body.extend_from_slice(&[20, 0x30]);
                body.extend_from_slice(&0u32.to_le_bytes());
                records.extend(rar4_block(0x77, 0x8000, &body));
                records.extend_from_slice(b"acl!");
            }
            let end = (member.data_offset + member.data_size) as usize;
            volume.splice(end..end, records);
        }
        volume
    }

    // A RAR4 set whose first file spans three volumes and is named
    // otherwise in every part past its first, as some packers write it,
    // with more files after it in the last volume.
    fn renamed_rar4() -> Vec<Vec<u8>> {
        let mut options = Options::new(Version::Rar4);
        options.split = Some(5_000);
        let mut items = vec![Item::File("track.flac", payload(9, 14_000))];
        for at in 0..6 {
            let name: &'static str = Box::leak(format!("after{at}.bin").into_boxed_str());
            items.push(Item::File(name, payload(20 + at, 120)));
        }
        let mut set = write(&items, &options);
        assert!(set.len() >= 3);
        for volume in &mut set[1..] {
            // The signature, then the main header, then the file header
            // the volume opens on, its name last.
            const AT: usize = 7 + 13;
            assert_eq!(volume[AT + 2], 0x74, "a file header");
            let size = u16::from_le_bytes([volume[AT + 5], volume[AT + 6]]) as usize;
            let body = &mut volume[AT + 7..AT + size];
            let name = body.len() - "track.flac".len();
            assert_eq!(&body[name..], b"track.flac");
            body[name..].copy_from_slice(b"TRACK.flac");
            let crc = (crc32(&volume[AT + 2..AT + size]) as u16).to_le_bytes();
            volume[AT..AT + 2].copy_from_slice(&crc);
        }
        set
    }

    // A stored RAR5 set of `count` files of `size` bytes, split at `split`.
    fn rar5(count: usize, size: usize, split: Option<usize>) -> Vec<Vec<u8>> {
        let items: Vec<Item> = (0..count)
            .map(|at| {
                let name: &'static str = Box::leak(format!("m{at:05}.bin").into_boxed_str());
                Item::File(name, payload(at as u64, size))
            })
            .collect();
        let mut options = Options::new(Version::Rar5);
        options.split = split;
        write(&items, &options)
    }

    // The piece of `piece` bytes that completes the header of a volume's
    // member `member`: a walk restarted on it reads from the signature and
    // lists that member anew in one step.
    fn listing(volume: &[u8], member: usize, piece: u64) -> usize {
        let facts =
            RarArchive::parse_volume_facts(Cursor::new(volume.to_vec()), None).expect("the volume");
        ((facts.members[member].data_offset - 1) / piece) as usize
    }

    // Whether some step handed the layout less than the whole facts.
    fn handed_less(handings: &[Handing]) -> bool {
        handings
            .iter()
            .any(|handing| handing.held > 0 && handing.handed < handing.listed)
    }

    // Volumes fed in pieces lay as they do handed whole each time: the same
    // layout, the same refusals, after every step, in a RAR5 set, a RAR4
    // set whose later parts name its first member otherwise, a RAR4 volume
    // whose owners and comments arrive after the files they follow, and a
    // walk restarted from the signature in the middle of a volume.
    #[test]
    fn a_layout_handed_what_changed_lays_as_one_handed_the_whole_facts() {
        let spanning = rar5(30, 3_000, Some(7_000));
        assert!(spanning.len() > 5);
        let handings = fed(&spanning, 211, None);
        assert!(handed_less(&handings), "RAR5 set: deltas were handed");

        let renamed = renamed_rar4();
        // The later volumes open on the part named otherwise, which the
        // layout takes under the first part's name.
        let tail = RarArchive::parse_volume_facts(Cursor::new(renamed[1].clone()), None)
            .expect("the second volume");
        assert_eq!(tail.members[0].name, "TRACK.flac");
        let last =
            RarArchive::parse_volume_facts(Cursor::new(renamed[renamed.len() - 1].clone()), None)
                .expect("the last volume");
        assert!(last.members.len() > 2, "files after the renamed part");
        let handings = fed(&renamed, 97, None);
        assert!(
            handed_less(&handings),
            "renamed RAR4 set: deltas were handed"
        );
        // Restarted in the last volume, the delta states the part it opens
        // on again, and carries the layout's name for it.
        let at = renamed.len() - 1;
        let handings = fed(&renamed, 97, Some((at, listing(&renamed[at], 3, 97))));
        assert!(
            handings
                .iter()
                .any(|handing| handing.held > 0 && handing.handed == handing.listed),
            "the restart hands the renamed part again: {handings:?}"
        );

        let owned = owned_rar4();
        let whole =
            RarArchive::parse_volume_facts(Cursor::new(owned.clone()), None).expect("the volume");
        assert_eq!(whole.members.len(), 12);
        assert!(whole.members.iter().any(|member| member.owner.is_some()));
        // Fed in small pieces, an owner arriving after its file restates
        // that file, and a service arriving restates the old comment and
        // old-style service listed after every service.
        assert_eq!(
            restates(&owned, 23),
            (true, true),
            "the RAR4 volume restates"
        );
        let handings = fed(std::slice::from_ref(&owned), 23, None);
        assert!(handed_less(&handings), "RAR4 volume: deltas were handed");

        // A restart mid-volume: the walk reads from the signature again,
        // and the layout is handed every member once more.
        let one = rar5(40, 300, None);
        let handings = fed(&one, 97, Some((0, listing(&one[0], 20, 97))));
        assert!(
            handings
                .iter()
                .any(|handing| handing.held > 0 && handing.handed == handing.listed),
            "the restart hands every member again: {handings:?}"
        );
        assert!(handed_less(&handings));
        let handings = fed(
            std::slice::from_ref(&owned),
            23,
            Some((0, listing(&owned, 6, 23))),
        );
        assert!(handed_less(&handings));
        assert!(
            handings
                .iter()
                .any(|handing| handing.held > 0 && handing.handed == handing.listed),
            "the restart hands every member again: {handings:?}"
        );
    }

    // What a layout is handed for each step of a volume does not grow with
    // the members it already holds: each member is handed once, in the step
    // that lists it, however many the volume lists before it. Handed the
    // whole facts, the same steps hand it about the square of them.
    #[test]
    fn a_step_hands_the_layout_only_the_members_it_added() {
        for members in [100, 400] {
            let volumes = rar5(members, 200, None);
            let handings = fed(&volumes, 64, None);
            for handing in &handings {
                assert!(
                    handing.handed <= handing.listed - handing.held,
                    "M={members}: {handing:?}"
                );
            }
            let handed: usize = handings.iter().map(|handing| handing.handed).sum();
            assert_eq!(handed, members, "M={members}: each member handed once");
            let whole: usize = handings
                .iter()
                .filter(|handing| handing.handed > 0 || handing.listed > handing.held)
                .map(|handing| handing.listed)
                .sum();
            assert!(
                whole > members * members / 4,
                "M={members}: the whole facts hand {whole}"
            );
        }
    }
}
