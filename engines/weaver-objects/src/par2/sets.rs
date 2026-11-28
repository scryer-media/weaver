// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What the object knows of each recovery set, and of each of its volumes.
//
// A set is identified by its recovery set id, the id in every packet's
// header, so a packet belongs to the set it names wherever it was found: a
// shared recovery file feeds every set whose packets it carries.

use std::collections::{BTreeMap, BTreeSet};

use par2_rs::checksum::md5;
use par2_rs::{
    DEFAULT_MAX_RETAINED_METADATA_BYTES, DEFAULT_MAX_RETAINED_PACKETS, FileDescription, FileId,
    Packet, Par2FileSet,
};
use serde::{Deserialize, Serialize};
use weaver_graph::recovery::{RepairOutcome, UnboundReason};
use weaver_graph::{ByteRange, Coverage, MemberId, Sealed, Seek, VolumeIndex, Weight};

use super::bind::{Bindings, Unnamed};
use super::facts::Packets;
use super::repair::SetRepair;
use super::walker::Walker;
pub(super) use crate::elimination::Length;
use crate::lost::Lost;
use crate::recovery_volume;

const MAX_PAR2_INPUT_SLICES: u64 = 32_768;
const MAX_TRACKED_SLICES: u64 = 1_048_576;
// par2-rs keeps at most this many descriptions, and slice checksum packets,
// of one set: the most files a main packet can name.
const MAX_FILES_PER_SET: usize = 32_768;

// Where one recovery packet sits, header included.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Location {
    pub(super) volume: VolumeIndex,
    pub(super) range: ByteRange,
}

#[derive(Debug)]
pub(super) struct SetState {
    // Where the set's first packet was found: sets are taken in this order,
    // which is the order of the volumes that carry them.
    pub(super) first_seen: (VolumeIndex, u64),
    // Sealed: the metadata settles in the first volumes, and every
    // checkpoint after that carries it as it was encoded then.
    packets: Sealed<Packets>,
    hashes: BTreeSet<[u8; 16]>,
    // The set as far as its metadata has arrived; `None` until its main
    // packet has.
    pub(super) index: Option<Par2FileSet>,
    described: bool,
    pub(super) checksummed: bool,
    // Each described file's whole CRC32, folded from its slice checksums
    // when the index is built: the index is fixed until a new packet
    // rebuilds it, so the fold is too.
    whole: BTreeMap<FileId, u32>,
    // The slices that share their checksums with another, by those
    // checksums, folded with `whole`: equal checksums are equal bytes, so
    // each holds the others' bytes.
    alike: BTreeMap<SliceKey, Vec<(FileId, u64)>>,
    // The described files by the hash of their front, folded with `whole`:
    // what a member's front digest finds its file by.
    fronts: BTreeMap<Front, Vec<FileId>>,
    // The described files by their name in ASCII lower case, each with its
    // length, in file order, folded with `whole`: what a member's posted
    // name finds its file by, in either case.
    names: BTreeMap<Box<str>, Vec<(FileId, u64)>>,
    // What the index and the maps beside it took of the packets, so a new
    // packet is taken into them alone; `None` until they are first built
    // from the packets.
    taken: Option<Taken>,
    // Recovery packets by exponent: the first copy found is the one kept.
    pub(super) recovery: BTreeMap<u32, Location>,
    // Which member each described file is bound to: by its front, its
    // name, or the slices a repair read back.
    members: BTreeMap<FileId, MemberId>,
    // The complete members nothing named that could be a file of the set
    // no member is bound to: what a repair reads back for its slices, or
    // binds by elimination.
    unnamed: Vec<(MemberId, Length)>,
    // What the set's last repair came to, over the members bound then.
    pub(super) last: Option<SetRepair>,
    // Kept beside `members`, so a binding weighs only the files and
    // members it changes: the files the set protects with their lengths,
    // while its index is usable; each bound member with each file it is
    // bound to; and how many protected files no member is bound to, by
    // length.
    protected: BTreeMap<FileId, u64>,
    by_member: BTreeSet<(MemberId, FileId)>,
    free: BTreeMap<u64, u32>,
    // The members whose file the set changed since the object last folded
    // it: a member's evidence reads the file it is bound to, so a fold
    // builds only these afresh.
    unfolded: BTreeSet<MemberId>,
    // How many files and unnamed members binding the set weighed.
    #[cfg(test)]
    pub(super) visited: usize,
    // How many packets building the index read, and how many times it was
    // built from every packet.
    #[cfg(test)]
    pub(super) walked: usize,
    #[cfg(test)]
    pub(super) built_whole: usize,
}

// What the index took of the set's packets, beside the index itself: how
// many, under which output limit, and what taking the next one reads.
#[derive(Debug, Default, PartialEq)]
struct Taken {
    packets: usize,
    limit: u64,
    // Whether a Unicode filename packet is among them: it renames the
    // descriptions as the index is built.
    unicode: bool,
    // The first slice checksum packet of each file, by its place among the
    // packets: par2-rs keeps it before the file's description arrives.
    checksums: BTreeMap<FileId, usize>,
    // At least what par2-rs charges its packet budget for them.
    retained: usize,
    bytes: usize,
    // The sums the geometry is admitted by.
    described_bytes: u64,
    tracked_slices: u64,
    input_slices: u64,
    // How many times the main packet lists each file it protects, and how
    // many of those listings have no description, and no slice checksums.
    recovery: BTreeMap<FileId, u64>,
    undescribed: u64,
    unchecksummed: u64,
    // Each slice whose checksums no other slice shares: what a slice of
    // the same checksums joins in `alike`.
    lone: BTreeMap<SliceKey, (FileId, u64)>,
}

impl Taken {
    // What the set's packets come to, for the index they built.
    fn of(packets: &[Packet], index: Option<&Par2FileSet>, limit: u64, lone: Lone) -> Self {
        let mut taken = Self {
            packets: packets.len(),
            limit,
            ..Self::default()
        };
        for (at, packet) in packets.iter().enumerate() {
            taken.note(at, packet);
        }
        let Some(index) = index else {
            return taken;
        };
        taken.lone = lone;
        for description in index.files.values() {
            taken.described_bytes = taken.described_bytes.saturating_add(description.length);
            taken.tracked_slices = taken
                .tracked_slices
                .saturating_add(description.length.div_ceil(index.slice_size));
        }
        for file in &index.recovery_file_ids {
            *taken.recovery.entry(*file).or_default() += 1;
            match index.files.get(file) {
                Some(description) => {
                    taken.input_slices = taken
                        .input_slices
                        .saturating_add(description.length.div_ceil(index.slice_size));
                }
                None => taken.undescribed += 1,
            }
            if !index.slice_checksums.contains_key(file) {
                taken.unchecksummed += 1;
            }
        }
        taken
    }

    // Charge the packet, and hold its place when it is the first slice
    // checksum packet of its file par2-rs keeps.
    fn note(&mut self, at: usize, packet: &Packet) {
        self.retained += 1;
        self.bytes = self.bytes.saturating_add(retained_bytes(packet));
        match packet {
            Packet::UnicodeFilename(_) => self.unicode = true,
            Packet::InputFileSliceChecksum(ifsc)
                if self.checksums.len() < MAX_FILES_PER_SET
                    && !self.checksums.contains_key(&ifsc.file_id) =>
            {
                self.checksums.insert(ifsc.file_id, at);
            }
            _ => {}
        }
    }

    // Count `length` of a new description into the geometry sums; whether
    // they still admit it.
    fn admit(&mut self, file: FileId, length: u64, slice_size: u64, limit: u64) -> bool {
        let slices = length.div_ceil(slice_size);
        let listed = self.recovery.get(&file).copied().unwrap_or(0);
        self.undescribed -= listed;
        let described = self.described_bytes.checked_add(length);
        let tracked = self.tracked_slices.checked_add(slices);
        let input = slices
            .checked_mul(listed)
            .and_then(|slices| self.input_slices.checked_add(slices));
        self.described_bytes = described.unwrap_or(u64::MAX);
        self.tracked_slices = tracked.unwrap_or(u64::MAX);
        self.input_slices = input.unwrap_or(u64::MAX);
        described.is_some_and(|bytes| bytes <= limit)
            && tracked.is_some_and(|slices| slices <= MAX_TRACKED_SLICES)
            && input.is_some_and(|slices| slices <= MAX_PAR2_INPUT_SLICES)
    }
}

// At least what par2-rs charges for keeping a packet (`packet_retained_bytes`
// in its budget): the packet's variable part as it counts it, and the whole
// packet's size for the fixed part, which no variant's own size passes.
fn retained_bytes(packet: &Packet) -> usize {
    let variable = match packet {
        Packet::Main(main) => (main.recovery_file_ids.len() + main.non_recovery_file_ids.len())
            .saturating_mul(size_of::<FileId>()),
        Packet::FileDescription(file) => file
            .filename
            .len()
            .saturating_add(file.par2_name.len())
            .saturating_add(file.par2_name_bytes.len()),
        Packet::InputFileSliceChecksum(ifsc) => ifsc
            .checksums
            .len()
            .saturating_mul(size_of::<par2_rs::SliceChecksum>()),
        Packet::RecoverySlice(_) => 0,
        Packet::Creator(creator) => creator.creator_id.len(),
        Packet::UnicodeFilename(name) => name.name.len().saturating_mul(5),
        Packet::Unknown { body, .. } => body.len(),
    };
    variable.saturating_add(size_of::<Packet>() + 64)
}

// Whether a slice checksum packet of `checksums` entries is the one par2-rs
// keeps for a file of `length`: one entry for each slice.
fn checksums_fit(checksums: usize, length: u64, slice_size: u64) -> bool {
    let slices = if length == 0 {
        0
    } else {
        length.div_ceil(slice_size)
    };
    u64::try_from(checksums).is_ok_and(|checksums| checksums == slices)
}

// What a rebuild left of the catalogue's filing of the set.
#[derive(Debug)]
pub(super) enum Before {
    // The set was not filed: its index was not usable.
    Unfiled,
    // The set was filed under this index, which it no longer has.
    Index(Box<Par2FileSet>),
    // The set stays filed, and describes these files besides: files it
    // does not protect, since a usable set describes every one it does.
    Described(Vec<FileId>),
}

// What a binding changed of a set: the members it came to bind, and those
// it bound no longer.
#[derive(Debug, Default)]
pub(super) struct Change {
    pub(super) left: Vec<MemberId>,
    pub(super) entered: Vec<MemberId>,
}

// The entries of `by_member` for one member.
fn files_of(member: MemberId) -> std::ops::RangeInclusive<(MemberId, FileId)> {
    (member, FileId::from_bytes([0; 16]))..=(member, FileId::from_bytes([u8::MAX; 16]))
}

impl SetState {
    pub(super) fn new(first_seen: (VolumeIndex, u64)) -> Self {
        Self {
            first_seen,
            packets: Sealed::default(),
            hashes: BTreeSet::new(),
            index: None,
            described: false,
            checksummed: false,
            whole: BTreeMap::new(),
            alike: BTreeMap::new(),
            fronts: BTreeMap::new(),
            names: BTreeMap::new(),
            taken: None,
            recovery: BTreeMap::new(),
            members: BTreeMap::new(),
            unnamed: Vec::new(),
            last: None,
            protected: BTreeMap::new(),
            by_member: BTreeSet::new(),
            free: BTreeMap::new(),
            unfolded: BTreeSet::new(),
            #[cfg(test)]
            visited: 0,
            #[cfg(test)]
            walked: 0,
            #[cfg(test)]
            built_whole: 0,
        }
    }

    // Collect distinct packets; the caller rebuilds once per ingestion batch.
    pub(super) fn take_metadata(&mut self, hash: [u8; 16], packet: Packet) -> bool {
        if !self.hashes.insert(hash) {
            return false;
        }
        self.packets.change(|packets| packets.0.push(packet));
        true
    }

    // A verdict was on the index it ruled over: one that rules alike, as
    // when a packet that changes no file, slice or checksum arrives (a
    // creator packet, say), keeps it. Ruling again would read back what
    // the repair already wrote and find nothing damaged.
    //
    // What the rebuild changed: whether binding reads the index alike (the
    // set stays usable, and its main packet and every description are as
    // they were), and then the files whose slice checksums or whole CRC32
    // changed, the only parts of it a fold reads that can differ.
    //
    // A rebuild takes only the packets that arrived since the last one into
    // the index and the maps beside it. It builds them from every packet
    // only when the output limit changed, or when one of the new packets
    // is one whose effect is not taken in place: a main packet, which
    // sets the set's id, slice size and protected files; a Unicode
    // filename packet, which renames descriptions; a description once one
    // has arrived; or a recovery slice packet. Nor is it taken in place
    // when par2-rs's packet budget could have run out.
    pub(super) fn rebuild(&mut self, output_limit: u64) -> Rebuilt {
        #[cfg(test)]
        let was = (
            self.index.clone(),
            self.whole.clone(),
            self.usable().is_some(),
            self.last.is_some(),
        );
        let rebuilt = if self.takes_in_place(output_limit) {
            self.take_in_place()
        } else {
            self.rebuild_whole(output_limit)
        };
        #[cfg(test)]
        self.assert_rebuilt(was, &rebuilt, output_limit);
        rebuilt
    }

    // Whether the packets that arrived since the last rebuild can be taken
    // into the index in place.
    fn takes_in_place(&self, output_limit: u64) -> bool {
        let Some(taken) = self
            .taken
            .as_ref()
            .filter(|taken| taken.limit == output_limit)
        else {
            return false;
        };
        let fresh = &self.packets.get().0[taken.packets..];
        // An index that did not build stays unbuilt until a main packet
        // comes: it lacked one, or what refused it only grows.
        if self.index.is_none() {
            return !fresh.iter().any(|packet| matches!(packet, Packet::Main(_)));
        }
        let (mut retained, mut bytes) = (taken.retained, taken.bytes);
        for packet in fresh {
            match packet {
                Packet::Main(_) | Packet::UnicodeFilename(_) | Packet::RecoverySlice(_) => {
                    return false;
                }
                Packet::FileDescription(_) if taken.unicode => return false,
                _ => {}
            }
            retained += 1;
            bytes = bytes.saturating_add(retained_bytes(packet));
        }
        retained <= DEFAULT_MAX_RETAINED_PACKETS && bytes <= DEFAULT_MAX_RETAINED_METADATA_BYTES
    }

    // Take the packets that arrived since the last rebuild into the index
    // as par2-rs builds it: the first description of each file and the
    // first slice checksum packet, each up to its file bound, the
    // checksums kept only for a described file with one entry per slice,
    // and the first creator; an unknown packet is ignored. Then the maps
    // beside the index for the files those packets changed.
    fn take_in_place(&mut self) -> Rebuilt {
        let usable = self.usable().is_some();
        let packets = &self.packets.get().0;
        let taken = self.taken.as_mut().expect("taken in place");
        let fresh = taken.packets..packets.len();
        taken.packets = packets.len();
        #[cfg(test)]
        {
            self.walked += fresh.len();
        }
        let mut described = Vec::new();
        let mut checksummed = Vec::new();
        let mut admitted = true;
        let mut creator = false;
        for at in fresh {
            let packet = &packets[at];
            taken.note(at, packet);
            let Some(index) = self.index.as_mut() else {
                continue;
            };
            match packet {
                Packet::FileDescription(file) => {
                    if index.files.len() >= MAX_FILES_PER_SET
                        || index.files.contains_key(&file.file_id)
                    {
                        continue;
                    }
                    index.files.insert(
                        file.file_id,
                        FileDescription {
                            file_id: file.file_id,
                            hash_full: file.hash_full,
                            hash_16k: file.hash_16k,
                            length: file.file_length,
                            par2_name: file.par2_name.clone(),
                            filename: file.filename.clone(),
                        },
                    );
                    described.push(file.file_id);
                    admitted &= taken.admit(
                        file.file_id,
                        file.file_length,
                        index.slice_size,
                        taken.limit,
                    );
                    let held = taken.checksums.get(&file.file_id).map(|at| &packets[*at]);
                    if let Some(Packet::InputFileSliceChecksum(ifsc)) = held
                        && checksums_fit(ifsc.checksums.len(), file.file_length, index.slice_size)
                    {
                        index
                            .slice_checksums
                            .insert(file.file_id, ifsc.checksums.clone());
                        checksummed.push(file.file_id);
                    }
                }
                Packet::InputFileSliceChecksum(ifsc) => {
                    // Kept only when `note` held this packet's place.
                    if taken.checksums.get(&ifsc.file_id) != Some(&at) {
                        continue;
                    }
                    if let Some(description) = index.files.get(&ifsc.file_id)
                        && checksums_fit(ifsc.checksums.len(), description.length, index.slice_size)
                    {
                        index
                            .slice_checksums
                            .insert(ifsc.file_id, ifsc.checksums.clone());
                        checksummed.push(ifsc.file_id);
                    }
                }
                Packet::Creator(id) if index.creator.is_none() => {
                    index.creator = Some(id.creator_id.clone());
                    creator = true;
                }
                _ => {}
            }
        }
        if self.index.is_none() {
            return Rebuilt {
                before: Before::Unfiled,
                binds_alike: false,
                checked: Vec::new(),
            };
        }
        // Past the geometry's admission the index no longer builds; what
        // refused it only grows.
        if !admitted {
            let limit = taken.limit;
            let mut before = self.index.take().expect("an index taken into");
            // The catalogue filed the index as it stood before this take.
            for file in &checksummed {
                before.slice_checksums.remove(file);
            }
            for file in &described {
                before.files.remove(file);
            }
            if creator {
                before.creator = None;
            }
            self.last = None;
            self.whole.clear();
            self.alike.clear();
            self.fronts.clear();
            self.names.clear();
            self.taken = Some(Taken::of(packets, None, limit, Lone::new()));
            self.index_changed();
            return Rebuilt {
                before: if usable {
                    Before::Index(Box::new(before))
                } else {
                    Before::Unfiled
                },
                binds_alike: false,
                checked: Vec::new(),
            };
        }
        for file in &checksummed {
            taken.unchecksummed -= taken.recovery.get(file).copied().unwrap_or(0);
        }
        self.described = taken.undescribed == 0;
        self.checksummed = taken.unchecksummed == 0;
        let index = self.index.as_ref().expect("an index taken into");
        for file in &described {
            let description = &index.files[file];
            let front = Front::described(description.length, description.hash_16k);
            insert_sorted(self.fronts.entry(front).or_default(), *file);
            let name = description.filename.to_ascii_lowercase().into_boxed_str();
            insert_sorted(
                self.names.entry(name).or_default(),
                (*file, description.length),
            );
        }
        for file in described.iter().chain(&checksummed) {
            match index.expected_file_crc32(*file) {
                Some(crc) => self.whole.insert(*file, crc),
                None => self.whole.remove(file),
            };
        }
        for file in &checksummed {
            for (slice, checksum) in index.file_checksums(file).into_iter().flatten().enumerate() {
                let key = (checksum.crc32, checksum.md5);
                let at = (*file, slice as u64);
                match self.alike.get_mut(&key) {
                    Some(slices) => insert_sorted(slices, at),
                    None => match taken.lone.remove(&key) {
                        Some(first) => {
                            let mut slices = vec![first, at];
                            slices.sort();
                            self.alike.insert(key, slices);
                        }
                        None => {
                            taken.lone.insert(key, at);
                        }
                    },
                }
            }
        }
        // A set comes to be usable when its last protected file is
        // described; one usable already describes every file it protects,
        // so what it protects and what is free stay.
        if !usable && self.described {
            self.protected = self.usable().map_or_else(BTreeMap::new, protected_of);
            self.free = free_of(&self.protected, &self.members);
        }
        if !described.is_empty() || !checksummed.is_empty() {
            self.last = None;
        }
        let binds_alike = usable && described.is_empty();
        Rebuilt {
            before: if usable && !described.is_empty() {
                Before::Described(described)
            } else {
                Before::Unfiled
            },
            binds_alike,
            checked: if binds_alike { checksummed } else { Vec::new() },
        }
    }

    // Build the index and every map beside it from all of the set's
    // packets, as the reference a rebuild in place keeps to.
    fn rebuild_whole(&mut self, output_limit: u64) -> Rebuilt {
        let index = index_of(&self.packets.get().0, output_limit);
        if !rules_alike(self.index.as_ref(), index.as_ref()) {
            self.last = None;
        }
        let usable = self.usable().is_some();
        let before = std::mem::replace(&mut self.index, index);
        let whole = whole_of(self.index.as_ref());
        let binds_alike = usable && describes_alike(before.as_ref(), self.index.as_ref());
        let checked = if binds_alike {
            let (Some(was), Some(now)) = (before.as_ref(), self.index.as_ref()) else {
                unreachable!("an index described alike is there before and after");
            };
            now.files
                .keys()
                .filter(|file| {
                    was.slice_checksums.get(file) != now.slice_checksums.get(file)
                        || self.whole.get(file) != whole.get(file)
                })
                .copied()
                .collect()
        } else {
            Vec::new()
        };
        self.whole = whole;
        self.derive(output_limit);
        debug_assert!(!binds_alike || self.usable().is_some());
        Rebuilt {
            before: match before {
                Some(before) if usable => Before::Index(Box::new(before)),
                _ => Before::Unfiled,
            },
            binds_alike,
            checked,
        }
    }

    // Build every map beside the index, but `whole`, from it and the
    // packets.
    fn derive(&mut self, output_limit: u64) {
        #[cfg(test)]
        {
            self.walked += self.packets.get().0.len();
            self.built_whole += 1;
        }
        let (alike, lone) = alike_of(self.index.as_ref());
        self.alike = alike;
        self.fronts = fronts_of(self.index.as_ref());
        self.names = names_of(self.index.as_ref());
        self.taken = Some(Taken::of(
            &self.packets.get().0,
            self.index.as_ref(),
            output_limit,
            lone,
        ));
        self.index_changed();
    }

    // Readiness is a property of metadata, never work for a dispatch query.
    pub(super) fn index_changed(&mut self) {
        self.described = self.index.as_ref().is_some_and(|index| {
            index
                .recovery_file_ids
                .iter()
                .all(|id| index.file_description(id).is_some())
        });
        self.checksummed = self.index.as_ref().is_some_and(|index| {
            index
                .recovery_file_ids
                .iter()
                .all(|id| index.file_checksums(id).is_some())
        });
        self.protected = self.usable().map_or_else(BTreeMap::new, protected_of);
        self.free = free_of(&self.protected, &self.members);
    }

    // A set as a checkpoint recorded it: the index is rebuilt from its
    // packets, as each new one rebuilt it.
    pub(super) fn restored(
        first_seen: (VolumeIndex, u64),
        packets: Sealed<Packets>,
        hashes: BTreeSet<[u8; 16]>,
        output_limit: u64,
    ) -> Self {
        let index = index_of(&packets.get().0, output_limit);
        let mut state = Self {
            whole: whole_of(index.as_ref()),
            index,
            packets,
            hashes,
            ..Self::new(first_seen)
        };
        state.derive(output_limit);
        state
    }

    pub(super) fn packets(&self) -> &Sealed<Packets> {
        &self.packets
    }

    pub(super) fn hashes(&self) -> &BTreeSet<[u8; 16]> {
        &self.hashes
    }

    // An index that describes every file it protects: each has its
    // description, which names it, sizes it and hashes it whole. Its slice
    // checksums, when the set carries them, say which slices are good; a
    // file without them is checked whole and rebuilt whole.
    pub(super) fn usable(&self) -> Option<&Par2FileSet> {
        self.described.then_some(self.index.as_ref()).flatten()
    }

    // The CRC32 the described file's slices fold to.
    pub(super) fn whole(&self, file: FileId) -> Option<u32> {
        self.whole.get(&file).copied()
    }

    // Every slice whose bytes are those of `slice` of `file`, itself among
    // them; none when no other slice shares its checksums.
    pub(super) fn alike(&self, file: FileId, slice: u64) -> &[(FileId, u64)] {
        self.index
            .as_ref()
            .and_then(|index| slice_key(index, file, slice))
            .and_then(|key| self.alike.get(&key))
            .map_or(&[], Vec::as_slice)
    }

    // The described files whose front is `front`, in file order.
    pub(super) fn fronted(&self, front: &Front) -> &[FileId] {
        self.fronts.get(front).map_or(&[], Vec::as_slice)
    }

    // The described files whose name is `name` in ASCII lower case, each
    // with its length, in file order.
    pub(super) fn named(&self, name: &str) -> &[(FileId, u64)] {
        self.names
            .get(name.to_ascii_lowercase().as_str())
            .map_or(&[], Vec::as_slice)
    }

    // A set over `index` as though its packets had built it.
    #[cfg(test)]
    pub(super) fn indexed(index: Par2FileSet) -> Self {
        let index = Some(index);
        let mut state = Self {
            whole: whole_of(index.as_ref()),
            index,
            ..Self::new((VolumeIndex(0), 0))
        };
        state.derive(u64::MAX);
        // No packets built it: the next rebuild builds it from them.
        state.taken = None;
        state
    }

    pub(super) fn members(&self) -> &BTreeMap<FileId, MemberId> {
        &self.members
    }

    // Why the set bound no member, when it bound none. With no index, a
    // main packet among its packets is an index that did not build, and
    // none is an index that never came.
    pub(super) fn unbound(&self) -> Option<UnboundReason> {
        if !self.members.is_empty() {
            return None;
        }
        Some(match &self.index {
            Some(_) if self.described => UnboundReason::Unmatched,
            Some(_) => UnboundReason::Undescribed,
            None if self
                .packets
                .get()
                .0
                .iter()
                .any(|packet| matches!(packet, Packet::Main(_))) =>
            {
                UnboundReason::Unreadable
            }
            None => UnboundReason::Undescribed,
        })
    }

    // The members a checkpoint recorded, as a binding with no unnamed member
    // binds them.
    pub(super) fn restore_members(&mut self, members: BTreeMap<FileId, MemberId>) {
        self.replace_members(members);
        self.unnamed.clear();
    }

    // Bind the set to `given` and the unnamed members `unnamed` keeps, as
    // `bind_whole` binds it, weighing every file and every unnamed member
    // that could be one of its free files: for a set whose members changed
    // other than by binding, or that was not bound when the unnamed members
    // last changed.
    pub(super) fn recompute(&mut self, given: &Bindings, unnamed: &Unnamed) -> Option<Change> {
        let mut members = given.clone();
        for (file, member) in &self.members {
            if unnamed.get(*member).is_some() {
                members.entry(*file).or_insert(*member);
            }
        }
        #[cfg(test)]
        {
            self.visited += self.members.len() + given.len();
        }
        let mut change = Change::default();
        let members_changed = members != self.members;
        if members_changed {
            let was: BTreeSet<MemberId> = self.members.values().copied().collect();
            let now: BTreeSet<MemberId> = members.values().copied().collect();
            change.left = was.difference(&now).copied().collect();
            change.entered = now.difference(&was).copied().collect();
            self.replace_members(members);
        }
        // The unnamed members that fit a free file: those of a free length
        // exactly, and those bounded at or past the shortest free length.
        let mut fitting = BTreeSet::new();
        for length in self.free.keys() {
            fitting.extend(unnamed.exact(*length));
        }
        if let Some((shortest, _)) = self.free.first_key_value() {
            fitting.extend(unnamed.at_most(*shortest..));
        }
        let now: Vec<(MemberId, Length)> = fitting
            .into_iter()
            .filter(|member| !self.is_bound(*member))
            .filter_map(|member| Some((member, unnamed.get(member)?)))
            .collect();
        #[cfg(test)]
        {
            self.visited += now.len();
        }
        if !members_changed && now == self.unnamed {
            return None;
        }
        self.unnamed = now;
        self.last = None;
        Some(change)
    }

    // Bind the set as `bind_whole` binds it, weighing only what changed
    // since its last binding: `files`, whose given member changed, and
    // `changed`, the unnamed members that came, went or changed length.
    // The set's last binding was by these rules, over the unnamed members
    // as they were before `changed`, and nothing changed its members since.
    pub(super) fn bind_changes(
        &mut self,
        given: &Bindings,
        files: &[FileId],
        unnamed: &Unnamed,
        changed: &[MemberId],
    ) -> Option<Change> {
        // A file is weighed when its given member changed, or when the
        // member bound to it came or went among the unnamed.
        let mut weighed: BTreeSet<FileId> = files.iter().copied().collect();
        for member in changed {
            weighed.extend(
                self.by_member
                    .range(files_of(*member))
                    .map(|(_, file)| *file),
            );
        }
        let decided: Vec<(FileId, Option<MemberId>)> = weighed
            .iter()
            .map(|file| {
                let kept = self
                    .members
                    .get(file)
                    .copied()
                    .filter(|member| unnamed.get(*member).is_some());
                (*file, given.get(file).copied().or(kept))
            })
            .collect();
        #[cfg(test)]
        {
            self.visited += decided.len();
        }
        let mut moving = BTreeSet::new();
        for (file, now) in &decided {
            let was = self.members.get(file).copied();
            if was != *now {
                moving.extend(was);
                moving.extend(*now);
            }
        }
        let shortest = self.free.first_key_value().map(|(length, _)| *length);
        let before: Vec<bool> = moving.iter().map(|member| self.is_bound(*member)).collect();
        let mut toggled = BTreeSet::new();
        let mut members_changed = false;
        for (file, now) in decided {
            let free = self
                .protected
                .get(&file)
                .map(|length| self.free.contains_key(length));
            if self.put(file, now) != now {
                members_changed = true;
                if let Some(free) = free {
                    let length = self.protected[&file];
                    if free != self.free.contains_key(&length) {
                        toggled.insert(length);
                    }
                }
            }
        }
        let mut change = Change::default();
        for (member, was) in moving.iter().zip(before) {
            match (was, self.is_bound(*member)) {
                (true, false) => change.left.push(*member),
                (false, true) => change.entered.push(*member),
                _ => {}
            }
        }
        // The unnamed members whose place among the set's may change: those
        // that changed, those bound or unbound here, those of a length that
        // came free or stopped being, and those bounded between the
        // shortest free length before and after.
        let mut weigh: BTreeSet<MemberId> = changed.iter().copied().collect();
        weigh.extend(moving.iter().copied());
        for length in &toggled {
            weigh.extend(unnamed.exact(*length));
        }
        let after = self.free.first_key_value().map(|(length, _)| *length);
        if shortest != after {
            let (low, high) = match (shortest, after) {
                (Some(a), Some(b)) => (a.min(b), Some(a.max(b))),
                (Some(a), None) | (None, Some(a)) => (a, None),
                (None, None) => unreachable!(),
            };
            match high {
                Some(high) => weigh.extend(unnamed.at_most(low..high)),
                None => weigh.extend(unnamed.at_most(low..)),
            }
        }
        #[cfg(test)]
        {
            self.visited += weigh.len();
        }
        let mut unnamed_changed = false;
        for member in weigh {
            let now = unnamed
                .get(member)
                .filter(|length| self.fits_free(*length) && !self.is_bound(member));
            let at = self
                .unnamed
                .binary_search_by_key(&member, |(listed, _)| *listed);
            match (now, at) {
                (Some(length), Ok(at)) => {
                    if self.unnamed[at].1 != length {
                        self.unnamed[at].1 = length;
                        unnamed_changed = true;
                    }
                }
                (Some(length), Err(at)) => {
                    self.unnamed.insert(at, (member, length));
                    unnamed_changed = true;
                }
                (None, Ok(at)) => {
                    self.unnamed.remove(at);
                    unnamed_changed = true;
                }
                (None, Err(_)) => {}
            }
        }
        if !members_changed && !unnamed_changed {
            return None;
        }
        self.last = None;
        Some(change)
    }

    // Bind the file to `member`, or to none; the member it was bound to.
    fn put(&mut self, file: FileId, member: Option<MemberId>) -> Option<MemberId> {
        let was = match member {
            Some(member) => self.members.insert(file, member),
            None => self.members.remove(&file),
        };
        if was == member {
            return was;
        }
        self.unfolded.extend(was);
        self.unfolded.extend(member);
        if let Some(was) = was {
            self.by_member.remove(&(was, file));
        }
        if let Some(member) = member {
            self.by_member.insert((member, file));
        }
        if let Some(length) = self.protected.get(&file).copied() {
            match (was, member) {
                (None, Some(_)) => {
                    if let Some(count) = self.free.get_mut(&length) {
                        *count -= 1;
                        if *count == 0 {
                            self.free.remove(&length);
                        }
                    }
                }
                (Some(_), None) => *self.free.entry(length).or_default() += 1,
                _ => {}
            }
        }
        was
    }

    fn replace_members(&mut self, members: BTreeMap<FileId, MemberId>) {
        for (file, member) in &self.members {
            if members.get(file) != Some(member) {
                self.unfolded.insert(*member);
            }
        }
        for (file, member) in &members {
            if self.members.get(file) != Some(member) {
                self.unfolded.insert(*member);
            }
        }
        self.by_member = members
            .iter()
            .map(|(file, member)| (*member, *file))
            .collect();
        self.free = free_of(&self.protected, &members);
        self.members = members;
    }

    fn is_bound(&self, member: MemberId) -> bool {
        self.by_member.range(files_of(member)).next().is_some()
    }

    // Whether an unnamed member of `length` fits a file no member is bound
    // to.
    pub(super) fn fits_free(&self, length: Length) -> bool {
        match length {
            Length::Exact(length) => self.free.contains_key(&length),
            Length::AtMost(bound) => self
                .free
                .first_key_value()
                .is_some_and(|(shortest, _)| *shortest <= bound),
        }
    }

    // The files `member` is bound to, in file order.
    pub(super) fn files_of(&self, member: MemberId) -> impl Iterator<Item = FileId> + '_ {
        self.by_member
            .range(files_of(member))
            .map(|(_, file)| *file)
    }

    // What is kept beside the members agrees with them, and the unnamed
    // members are those of `unnamed` that fit a free file.
    #[cfg(test)]
    pub(super) fn assert_kept(&self, unnamed: Option<&[(MemberId, Length)]>) {
        let by_member: BTreeSet<_> = self
            .members
            .iter()
            .map(|(file, member)| (*member, *file))
            .collect();
        assert_eq!(self.by_member, by_member, "members by member");
        let protected: BTreeMap<FileId, u64> = self.usable().map_or_else(BTreeMap::new, |index| {
            index
                .recovery_file_ids
                .iter()
                .filter_map(|file| Some((*file, index.file_description(file)?.length)))
                .collect()
        });
        assert_eq!(self.protected, protected, "protected");
        assert_eq!(self.free, free_of(&protected, &self.members), "free");
        if let Some(unnamed) = unnamed {
            let free: Vec<u64> = self.free.keys().copied().collect();
            let fitting: Vec<_> = unnamed
                .iter()
                .copied()
                .filter(|(member, length)| {
                    free.iter().any(|long| length.fits(*long))
                        && !self.members.values().any(|bound| bound == member)
                })
                .collect();
            assert_eq!(self.unnamed, fitting, "unnamed");
        }
    }

    // A rebuild came to what building the index and every map beside it
    // from all of the packets comes to, and reported what that would: the
    // index, the maps, the readiness, what is protected and free, the
    // verdict kept or dropped, and how the set stood in the catalogue.
    #[cfg(test)]
    fn assert_rebuilt(
        &self,
        (was, whole, usable, last): (Option<Par2FileSet>, BTreeMap<FileId, u32>, bool, bool),
        rebuilt: &Rebuilt,
        output_limit: u64,
    ) {
        let packets = &self.packets.get().0;
        let built = index_of(packets, output_limit);
        assert!(rules_alike(built.as_ref(), self.index.as_ref()), "index");
        if let (Some(built), Some(index)) = (&built, &self.index) {
            assert_eq!(built.creator, index.creator, "creator");
            assert!(
                built
                    .recovery_slices
                    .keys()
                    .eq(index.recovery_slices.keys()),
                "recovery slices"
            );
        }
        assert_eq!(self.whole, whole_of(built.as_ref()), "whole");
        let (alike, lone) = alike_of(built.as_ref());
        assert_eq!(self.alike, alike, "alike");
        assert_eq!(self.fronts, fronts_of(built.as_ref()), "fronts");
        assert_eq!(self.names, names_of(built.as_ref()), "names");
        assert_eq!(
            self.taken.as_ref(),
            Some(&Taken::of(packets, built.as_ref(), output_limit, lone)),
            "taken"
        );
        let all = |has: &dyn Fn(&Par2FileSet, &FileId) -> bool| {
            built
                .as_ref()
                .is_some_and(|index| index.recovery_file_ids.iter().all(|file| has(index, file)))
        };
        assert_eq!(
            self.described,
            all(&|index, file| index.file_description(file).is_some()),
            "described"
        );
        assert_eq!(
            self.checksummed,
            all(&|index, file| index.file_checksums(file).is_some()),
            "checksummed"
        );
        self.assert_kept(None);
        if rules_alike(was.as_ref(), built.as_ref()) {
            assert_eq!(self.last.is_some(), last, "verdict kept");
        } else {
            assert!(self.last.is_none(), "verdict dropped");
        }
        let binds_alike = usable && describes_alike(was.as_ref(), built.as_ref());
        assert_eq!(rebuilt.binds_alike, binds_alike, "binds alike");
        if binds_alike {
            let (Some(was), Some(now)) = (&was, &built) else {
                unreachable!("an index described alike is there before and after");
            };
            let now_whole = whole_of(Some(now));
            let checked: BTreeSet<FileId> = now
                .files
                .keys()
                .filter(|file| {
                    was.slice_checksums.get(file) != now.slice_checksums.get(file)
                        || whole.get(file) != now_whole.get(file)
                })
                .copied()
                .collect();
            let reported: BTreeSet<FileId> = rebuilt.checked.iter().copied().collect();
            assert_eq!(reported, checked, "checked");
            assert_eq!(reported.len(), rebuilt.checked.len(), "checked once each");
            return;
        }
        assert!(rebuilt.checked.is_empty(), "checked");
        match (&rebuilt.before, was.filter(|_| usable)) {
            (Before::Unfiled, None) => {}
            (Before::Index(before), Some(was)) => {
                assert!(rules_alike(Some(before), Some(&was)), "before");
            }
            // Unfiling the index it had and filing the one it has comes to
            // filing the descriptions it gained: it lost none, changed none
            // and protects the same files.
            (Before::Described(files), Some(was)) => {
                let now = self.usable().expect("a set filed anew is usable");
                assert_eq!(was.recovery_file_ids, now.recovery_file_ids, "protected");
                assert!(
                    was.files
                        .iter()
                        .all(|(file, was)| now
                            .files
                            .get(file)
                            .is_some_and(|now| now.filename == was.filename
                                && now.length == was.length
                                && now.hash_16k == was.hash_16k)),
                    "described before"
                );
                let gained: BTreeSet<FileId> = now
                    .files
                    .keys()
                    .filter(|file| !was.files.contains_key(file))
                    .copied()
                    .collect();
                assert_eq!(files.iter().copied().collect::<BTreeSet<_>>(), gained);
                assert_eq!(files.len(), gained.len(), "described once each");
            }
            (before, was) => panic!("filed as {before:?}, was {:?}", was.is_some()),
        }
    }

    // What binding did before it weighed only what changed, kept to
    // compare with: every file and every unnamed member weighed.
    #[cfg(test)]
    pub(super) fn bind_whole(
        &mut self,
        mut members: BTreeMap<FileId, MemberId>,
        unnamed: &[(MemberId, Length)],
    ) -> Option<BTreeMap<FileId, MemberId>> {
        // A member bound before that nothing names now, as one a repair
        // knew by its slices, stays that file while no other member is.
        for (file, member) in &self.members {
            if unnamed.iter().any(|(candidate, _)| candidate == member) {
                members.entry(*file).or_insert(*member);
            }
        }
        let free: Vec<u64> = self.usable().map_or_else(Vec::new, |index| {
            index
                .recovery_file_ids
                .iter()
                .filter(|file| !members.contains_key(file))
                .filter_map(|file| Some(index.file_description(file)?.length))
                .collect()
        });
        let unnamed: Vec<_> = unnamed
            .iter()
            .copied()
            .filter(|(member, length)| {
                free.iter().any(|long| length.fits(*long))
                    && !members.values().any(|bound| bound == member)
            })
            .collect();
        if members == self.members && unnamed == self.unnamed {
            return None;
        }
        self.unnamed = unnamed;
        self.last = None;
        let was = self.members.clone();
        self.replace_members(members);
        Some(was)
    }

    pub(super) fn unnamed(&self) -> &[(MemberId, Length)] {
        &self.unnamed
    }

    // An unnamed member that could be a described file may be it until a
    // repair has read it.
    pub(super) fn may_be(&self, length: u64) -> bool {
        self.last.is_none() && self.unnamed.iter().any(|(_, long)| long.fits(length))
    }

    // The repair knew the unnamed member as the file, by a slice it read
    // back or by elimination: it is it.
    pub(super) fn know(&mut self, file: FileId, member: MemberId) {
        self.unnamed.retain(|(candidate, _)| *candidate != member);
        self.put(file, Some(member));
    }

    // Damage shown since an unread verdict may be what the descriptions
    // the set has not read cover, so the set is tried again. An impossible
    // verdict stands: damage only grows, so what the set could not rebuild
    // it still cannot.
    pub(super) fn damage_shown(&mut self) {
        if self
            .last
            .is_some_and(|last| last.outcome == RepairOutcome::Unread)
        {
            self.last = None;
        }
    }

    // An impossible verdict counted the blocks of the volumes bound then,
    // and an unread one the descriptions they could bring; a volume bound
    // since may bring more of either, so the set is tried again. Without
    // this the verdict would depend on whether a volume's front decided
    // before or after the damage arrived.
    pub(super) fn reopen(&mut self) {
        let stale = matches!(
            self.last.map(|last| last.outcome),
            Some(RepairOutcome::Impossible { .. } | RepairOutcome::Unread)
        );
        if stale {
            self.last = None;
        }
    }

    // The recovery blocks the set must hold before it is tried again, when
    // its last repair came up short of them.
    pub(super) fn short_of(&self) -> Option<u64> {
        self.last
            .filter(|last| last.outcome == RepairOutcome::Waiting)
            .map(|last| last.needed)
    }

    // Whether the set holds nothing a binding, a fold or a verdict reads:
    // no member, no unnamed member, no member still to fold and no verdict.
    pub(super) fn holds_nothing(&self) -> bool {
        self.members.is_empty()
            && self.unnamed.is_empty()
            && self.unfolded.is_empty()
            && self.last.is_none()
    }

    // Whether the last repair settled the set: only a wait is tried again.
    pub(super) fn concluded(&self) -> Option<RepairOutcome> {
        self.last
            .map(|last| last.outcome)
            .filter(|outcome| *outcome != RepairOutcome::Waiting)
    }

    pub(super) fn bound(&self, member: MemberId) -> Option<FileId> {
        self.files_of(member).next()
    }

    // The members whose file changed since the set was last folded, handed
    // to the fold that builds them afresh.
    pub(super) fn take_unfolded(&mut self) -> BTreeSet<MemberId> {
        std::mem::take(&mut self.unfolded)
    }

    #[cfg(test)]
    pub(super) fn unfolded(&self) -> &BTreeSet<MemberId> {
        &self.unfolded
    }

    #[cfg(test)]
    pub(super) fn restore_unfolded(&mut self, unfolded: BTreeSet<MemberId>) {
        self.unfolded = unfolded;
    }
}

#[derive(Default)]
pub(super) struct Volume {
    pub(super) walker: Walker,
    pub(super) arrived: Coverage,
    pub(super) lost: Lost,
    pub(super) closed: bool,
    // The recovery blocks the volume's name says it carries.
    pub(super) named_blocks: Option<u64>,
    pub(super) walked_blocks: u64,
    // The sets whose packets the walk has found in it.
    pub(super) sets: BTreeSet<[u8; 16]>,
    // The name it was bound under, which a later volume of the set continues.
    pub(super) name: Option<String>,
    // The volume's length, once known.
    pub(super) length: Option<u64>,
}

impl Volume {
    pub(super) fn name(&mut self, name: &str) {
        self.named_blocks = recovery_volume::named_blocks(name, ".par2");
        self.name = Some(name.to_owned());
    }

    // Whether the volume can bring the set blocks: it carries the set's
    // packets, or nothing of it has been walked to say whose it is.
    pub(super) fn serves(&self, set: &[u8; 16]) -> bool {
        self.sets.is_empty() || self.sets.contains(set)
    }

    // Whether every byte of the volume was walked: it is closed and nothing
    // of it was lost. One closed over a loss was never read where the loss
    // lies, and a volume lost whole was never read at all.
    pub(super) fn read_through(&self) -> bool {
        self.closed && self.lost.is_empty()
    }

    // Whether the walk has shown the volume carries recovery blocks: one
    // passed, the packet it is reading is one, or its name says it carries
    // some and its packets say whose.
    pub(super) fn shows_recovery(&self) -> bool {
        self.walked_blocks > 0
            || self.walker.reading_recovery()
            || (self.named_blocks.unwrap_or(0) > 0 && !self.sets.is_empty())
    }

    // Ask for every byte of the volume not yet in hand.
    pub(super) fn ask(&self, index: VolumeIndex, out: &mut Vec<Seek>) {
        recovery_volume::gaps(&self.arrived, |range| {
            out.push(Seek::recovery(index, range, Weight::FIRST))
        });
    }

    // Say every byte of the volume not yet in hand is not wanted yet.
    pub(super) fn defer(&self, index: VolumeIndex, out: &mut Vec<Seek>) {
        recovery_volume::gaps(&self.arrived, |range| {
            out.push(Seek::deferred(index, range))
        });
    }

    // The most recovery blocks the volume may still bring: what its name
    // says it carries, less what was walked. A name that gives no count
    // bounds nothing: then it is what the bytes past the walk's front have
    // room for, `packet` the smallest recovery packet any known set writes,
    // and with no length or packet known yet no bound is known, `u64::MAX`.
    // A loss inside it is not counted against it, so this never falls short
    // of the blocks really there.
    pub(super) fn left(&self, packet: Option<u64>) -> u64 {
        if self.closed {
            return 0;
        }
        match (self.named_blocks, self.length, packet) {
            (Some(named), _, _) => named.saturating_sub(self.walked_blocks),
            (None, Some(length), Some(packet)) if packet > 0 => {
                length.saturating_sub(self.walker.front()).div_ceil(packet)
            }
            (None, _, _) => u64::MAX,
        }
    }

    // The fewest recovery blocks the volume will still bring: what is left,
    // less what a lost range could have held. `packet` is the smallest
    // recovery packet any known set writes, so a loss is never undercounted.
    // A name that gives no count promises none.
    pub(super) fn coming(&self, packet: Option<u64>) -> u64 {
        if self.named_blocks.is_none() {
            return 0;
        }
        let lost: u64 = match packet {
            Some(packet) if packet > 0 => self
                .lost
                .iter()
                .map(|range| range.len().div_ceil(packet) + 1)
                .sum(),
            _ if self.lost.is_empty() => 0,
            _ => self.left(packet),
        };
        self.left(packet).saturating_sub(lost)
    }
}

fn index_of(packets: &[Packet], output_limit: u64) -> Option<Par2FileSet> {
    Par2FileSet::from_packets(packets.to_vec())
        .ok()
        .filter(|set| validate_par2_geometry(set, output_limit).is_ok())
}

// Whether a repair over either index rules the same: the same files, by
// the same descriptions, cut into the same slices with the same checksums.
// Recovery blocks are not in it; a set counts those apart.
fn rules_alike(before: Option<&Par2FileSet>, after: Option<&Par2FileSet>) -> bool {
    describes_alike(before, after)
        && before
            .zip(after)
            .is_none_or(|(before, after)| before.slice_checksums == after.slice_checksums)
}

// What a rebuild left of the index that binding and the catalogue read: how
// the set stood in the catalogue, whether it describes alike, and then the
// files a fold reads afresh.
pub(super) struct Rebuilt {
    pub(super) before: Before,
    pub(super) binds_alike: bool,
    pub(super) checked: Vec<FileId>,
}

// Whether two indexes describe alike: the main packet and every file's
// description, all that binding and the catalogue read of an index. Slice
// checksums are not among them.
fn describes_alike(before: Option<&Par2FileSet>, after: Option<&Par2FileSet>) -> bool {
    let (Some(before), Some(after)) = (before, after) else {
        return before.is_none() && after.is_none();
    };
    before.recovery_set_id == after.recovery_set_id
        && before.slice_size == after.slice_size
        && before.recovery_file_ids == after.recovery_file_ids
        && before.non_recovery_file_ids == after.non_recovery_file_ids
        && before.files.len() == after.files.len()
        && before.files.iter().all(|(id, was)| {
            after.files.get(id).is_some_and(|now| {
                was.file_id == now.file_id
                    && was.hash_full == now.hash_full
                    && was.hash_16k == now.hash_16k
                    && was.length == now.length
                    && was.par2_name == now.par2_name
                    && was.filename == now.filename
            })
        })
}

fn whole_of(index: Option<&Par2FileSet>) -> BTreeMap<FileId, u32> {
    let Some(index) = index else {
        return BTreeMap::new();
    };
    index
        .files
        .keys()
        .filter_map(|file| Some((*file, index.expected_file_crc32(*file)?)))
        .collect()
}

// A slice's checksums, which name its bytes.
type SliceKey = (u32, [u8; 16]);

fn slice_key(index: &Par2FileSet, file: FileId, slice: u64) -> Option<SliceKey> {
    let checksum = index
        .file_checksums(&file)?
        .get(usize::try_from(slice).ok()?)?;
    Some((checksum.crc32, checksum.md5))
}

// Each slice under checksums no other slice shares.
type Lone = BTreeMap<SliceKey, (FileId, u64)>;

// The slices that share their checksums with another, each list in order,
// and those that share them with none.
fn alike_of(index: Option<&Par2FileSet>) -> (BTreeMap<SliceKey, Vec<(FileId, u64)>>, Lone) {
    let Some(index) = index else {
        return (BTreeMap::new(), Lone::new());
    };
    let mut alike: BTreeMap<SliceKey, Vec<(FileId, u64)>> = BTreeMap::new();
    for file in index.files.keys() {
        for (slice, checksum) in index.file_checksums(file).into_iter().flatten().enumerate() {
            alike
                .entry((checksum.crc32, checksum.md5))
                .or_default()
                .push((*file, slice as u64));
        }
    }
    let mut lone = Lone::new();
    alike.retain(|key, slices| {
        if let [slice] = slices.as_slice() {
            lone.insert(*key, *slice);
            return false;
        }
        slices.sort();
        true
    });
    (alike, lone)
}

fn insert_sorted<T: Ord>(items: &mut Vec<T>, item: T) {
    let at = items.binary_search(&item).unwrap_or_else(|at| at);
    items.insert(at, item);
}

// The files a usable index protects, with their lengths.
fn protected_of(index: &Par2FileSet) -> BTreeMap<FileId, u64> {
    index
        .recovery_file_ids
        .iter()
        .filter_map(|file| Some((*file, index.file_description(file)?.length)))
        .collect()
}

// PAR2 identifies a file by the MD5 of at most this much of its front.
const HASH_WINDOW: u64 = 16 * 1024;

// A front as PAR2 names a file by it: the MD5 of its first bytes, up to
// HASH_WINDOW, with how many there were. A front shorter than the window
// answers only for a file of its own length, since a longer front would
// already contradict a shorter file, and a full one for every file at least
// the window long: one front answers for every description it could be.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Front {
    window: u64,
    md5: [u8; 16],
}

impl Front {
    // The front of a member's first bytes; none until one arrived.
    pub(super) fn of(first_bytes: &[u8]) -> Option<Self> {
        let window = HASH_WINDOW.min(first_bytes.len() as u64);
        (window > 0).then(|| Self {
            window,
            md5: md5(&first_bytes[..window as usize]),
        })
    }

    // The front a description states for its file.
    pub(super) fn described(length: u64, hash_16k: [u8; 16]) -> Self {
        Self {
            window: length.min(HASH_WINDOW),
            md5: hash_16k,
        }
    }
}

// How many protected files no member is bound to, by length.
fn free_of(
    protected: &BTreeMap<FileId, u64>,
    members: &BTreeMap<FileId, MemberId>,
) -> BTreeMap<u64, u32> {
    let mut free = BTreeMap::new();
    for (file, length) in protected {
        if !members.contains_key(file) {
            *free.entry(*length).or_default() += 1;
        }
    }
    free
}

fn fronts_of(index: Option<&Par2FileSet>) -> BTreeMap<Front, Vec<FileId>> {
    let mut fronts: BTreeMap<Front, Vec<FileId>> = BTreeMap::new();
    for description in index.into_iter().flat_map(|index| index.files.values()) {
        fronts
            .entry(Front::described(description.length, description.hash_16k))
            .or_default()
            .push(description.file_id);
    }
    for files in fronts.values_mut() {
        files.sort();
    }
    fronts
}

// ASCII lower case is the equivalence `eq_ignore_ascii_case` tests, so one
// bucket holds every description a posted name matches in either case.
fn names_of(index: Option<&Par2FileSet>) -> BTreeMap<Box<str>, Vec<(FileId, u64)>> {
    let mut names: BTreeMap<Box<str>, Vec<(FileId, u64)>> = BTreeMap::new();
    for description in index.into_iter().flat_map(|index| index.files.values()) {
        names
            .entry(description.filename.to_ascii_lowercase().into_boxed_str())
            .or_default()
            .push((description.file_id, description.length));
    }
    for files in names.values_mut() {
        files.sort();
    }
    names
}

// Admit declared geometry before anything is built on it.
fn validate_par2_geometry(set: &Par2FileSet, output_limit: u64) -> par2_rs::Result<()> {
    if set.slice_size == 0 {
        return Err(par2_resource_error("zero slice size"));
    }
    if set.slice_size > output_limit {
        return Err(par2_resource_error("slice size exceeds the output budget"));
    }
    let mut described_bytes = 0u64;
    let mut tracked_slices = 0u64;
    for description in set.files.values() {
        described_bytes = described_bytes
            .checked_add(description.length)
            .filter(|bytes| *bytes <= output_limit)
            .ok_or_else(|| par2_resource_error("described output exceeds the admitted job size"))?;
        // Non-recovery descriptions also reach verification bitmaps. Bound
        // those allocations independently of PAR2's repair-input cap.
        tracked_slices = tracked_slices
            .checked_add(description.length.div_ceil(set.slice_size))
            .filter(|slices| *slices <= 1_048_576)
            .ok_or_else(|| {
                par2_resource_error("verification bitmap exceeds the metadata budget")
            })?;
    }
    let mut input_slices = 0u64;
    for description in set
        .recovery_file_ids
        .iter()
        .filter_map(|id| set.files.get(id))
    {
        input_slices = input_slices
            .checked_add(description.length.div_ceil(set.slice_size))
            .filter(|slices| *slices <= MAX_PAR2_INPUT_SLICES)
            .ok_or_else(|| par2_resource_error("input slice count exceeds the repair limit"))?;
    }
    Ok(())
}

fn par2_resource_error(reason: &str) -> par2_rs::Par2Error {
    par2_rs::Par2Error::ResourceLimitExceeded {
        reason: format!("WEAVER_RESOURCE_LIMIT[par2_metadata]: {reason}"),
    }
}

#[cfg(test)]
mod tests {
    use par2_rs::{Packet, parse_packet};
    use weaver_graph::{Object, ObjectId, Recovery};

    use super::super::Par2;
    use super::super::facts::par2;
    use super::{SetState, names_of};
    use crate::testing::{Job, par2_set, payload};

    const OWN: ObjectId = ObjectId(7);

    // Every metadata packet of a recovery file, its main packet first and
    // its descriptions after it, one by one.
    fn metadata(bytes: &[u8]) -> Vec<Packet> {
        let mut packets = Vec::new();
        let mut at = 0;
        while at < bytes.len() {
            let (packet, length) = parse_packet(&bytes[at..], at as u64).expect("a packet");
            at += length;
            if !matches!(packet, Packet::RecoverySlice(_) | Packet::Unknown { .. }) {
                packets.push(packet);
            }
        }
        packets.sort_by_key(|packet| match packet {
            Packet::Main(_) => 0,
            Packet::FileDescription(_) => 1,
            _ => 2,
        });
        packets
    }

    // The name index is built where the index is, from it alone: a later
    // description adds its name, and an index that no longer builds
    // leaves none.
    #[test]
    fn the_name_index_follows_the_index() {
        let (a, b) = (payload(1, 2048), payload(2, 1024));
        let files = par2_set("set", &[("Alpha.bin", &a), ("beta.BIN", &b)], 512, 1, 1);
        let packets = metadata(&files[0].1);
        let mut state = SetState::new((weaver_graph::VolumeIndex(0), 0));
        let mut named = Vec::new();
        for (at, packet) in packets.into_iter().enumerate() {
            assert!(state.take_metadata([at as u8; 16], packet));
            state.rebuild(u64::MAX);
            assert_eq!(state.names, names_of(state.index.as_ref()), "packet {at}");
            named.push(state.names.len());
        }
        assert_eq!(named.first(), Some(&0), "{named:?}");
        assert!(named.contains(&1), "{named:?}");
        assert_eq!(named.last(), Some(&2), "{named:?}");
        assert!(state.usable().is_some());
        let lengths = |name: &str| {
            state
                .named(name)
                .iter()
                .map(|(_, length)| *length)
                .collect::<Vec<_>>()
        };
        assert_eq!(lengths("ALPHA.BIN"), vec![2048]);
        assert_eq!(lengths("beta.bin"), vec![1024]);

        // A slice larger than the output budget: the index no longer builds.
        state.rebuild(1);
        assert!(state.index.is_none());
        assert!(state.names.is_empty());
        assert!(state.named("alpha.bin").is_empty());
    }

    // A take reads the packets it brought, not the set's: the index is
    // built from every packet once, at the first rebuild, which is the main
    // packet's, and each description, slice checksum packet and creator
    // after it is taken in place alone. That holds with the checksums
    // before the descriptions, with a description coming to a set already
    // usable, and with a description past the output limit. Every rebuild
    // is also checked against building from every packet.
    #[test]
    fn a_take_walks_only_the_packets_it_brought() {
        const FILES: usize = 6;
        let mut contents: Vec<Vec<u8>> = (0..FILES - 1)
            .map(|at| payload(20 + at as u64, 1500 + 300 * at))
            .collect();
        // Slices of one file's bytes under another name: alike slices.
        contents.push(contents[0].clone());
        let names: Vec<String> = (0..FILES).map(|at| format!("file{at}.bin")).collect();
        let files: Vec<(&str, &[u8])> = names
            .iter()
            .zip(&contents)
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        let volumes = par2_set("set", &files, 512, 1, 1);
        let packets = metadata(&volumes[0].1);
        // A main packet, a description and checksums for each file, and a
        // creator.
        assert_eq!(packets.len(), 2 + 2 * FILES);
        let kind = |packet: &Packet| match packet {
            Packet::Main(_) => 0,
            Packet::InputFileSliceChecksum(_) => 1,
            Packet::FileDescription(_) => 2,
            _ => 3,
        };
        let mut checksums_first = packets.clone();
        checksums_first.sort_by_key(kind);
        // One file the main packet lists as unprotected, described last:
        // its description comes to a usable set.
        let mut unprotected = checksums_first.clone();
        let Some(Packet::Main(main)) = unprotected.first_mut() else {
            panic!("a main packet first");
        };
        let spare = main.recovery_file_ids.remove(0);
        main.non_recovery_file_ids.push(spare);
        let last = unprotected
            .iter()
            .position(
                |packet| matches!(packet, Packet::FileDescription(file) if file.file_id == spare),
            )
            .expect("its description");
        let moved = unprotected.remove(last);
        unprotected.push(moved);
        let total: u64 = contents.iter().map(|bytes| bytes.len() as u64).sum();
        for (case, packets, limit) in [
            ("descriptions first", &packets, u64::MAX),
            ("checksums first", &checksums_first, u64::MAX),
            ("unprotected last", &unprotected, u64::MAX),
            ("past the limit", &unprotected, total - 1),
        ] {
            let mut state = SetState::new((weaver_graph::VolumeIndex(0), 0));
            let mut befores = Vec::new();
            for (at, packet) in packets.iter().cloned().enumerate() {
                assert!(state.take_metadata([at as u8; 16], packet));
                let rebuilt = state.rebuild(limit);
                befores.push(match rebuilt.before {
                    super::Before::Unfiled => "unfiled".to_owned(),
                    super::Before::Index(_) => "index".to_owned(),
                    super::Before::Described(files) => format!("described {}", files.len()),
                });
            }
            assert_eq!(
                (state.walked, state.built_whole),
                (packets.len(), 1),
                "{case}"
            );
            match case {
                "past the limit" => {
                    assert!(state.index.is_none(), "{case}");
                    assert_eq!(befores.iter().filter(|b| *b == "index").count(), 1);
                }
                "unprotected last" => {
                    assert!(state.usable().is_some() && state.checksummed, "{case}");
                    assert_eq!(befores.last().map(String::as_str), Some("described 1"));
                }
                _ => assert!(state.usable().is_some() && state.checksummed, "{case}"),
            }
            assert!(
                state.alike.values().all(|slices| slices.len() == 2),
                "{case}"
            );
            assert_eq!(state.alike.is_empty(), case == "past the limit", "{case}");
        }
    }

    // A checkpoint carries no name index: the restored set builds it again
    // from its packets, alike.
    #[test]
    fn a_restored_set_builds_the_same_name_index() {
        let (a, b) = (payload(3, 2048), payload(4, 1024));
        let mut job = Job::new(Par2::new(OWN), OWN);
        for (name, bytes) in par2_set("set", &[("Alpha.bin", &a), ("beta.BIN", &b)], 512, 1, 1) {
            job.volume(&name, &bytes, 700, false);
        }
        let member = job.member("ALPHA.BIN", &a);
        job.object.bind(&mut job.namespace, &mut Vec::new());
        assert!(job.object.covers(member));
        let restored = par2(&job.object.facts()).expect("restores");
        assert_eq!(restored.facts(), job.object.facts());
        assert_eq!(restored.sets.len(), job.object.sets.len());
        for (key, state) in &job.object.sets {
            assert_eq!(state.names.len(), 2);
            assert_eq!(restored.sets[key].names, state.names);
        }
    }
}
