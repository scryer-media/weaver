// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The RAR store's facts: each volume as its owner bound it (its index is
// how a nameless volume was chained), the header bytes it kept, and what
// unrar-rs read from them (members and their extents, the solid flag, the
// end record, the recovery record's range); each member as it was
// announced, its pieces across the volumes and what was said of it; and
// what was said once. An encrypted member records its key tuple as the
// header states it (salt, KDF count, IV, check value), never a key: it
// comes back locked and opens again on the password the server offers.
//
// Restore walks the kept header bytes again and requires them to read as
// recorded, so the layout is rebuilt rather than trusted. A member's CRC
// runs and an open cipher's seeds are rebuilt by the server's replay of
// the placed ranges, which every restart does; the cipher blocks it held,
// parked or partial, are on no disk, so its chain is kept, never its key. What came out of the cipher
// is the member's coverage, which the cipher reads; the one thing coverage
// cannot say, the plaintext past the payload, is the member's padding.

use std::collections::BTreeSet;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use unrar_rs::{
    MemberKeying, RarVolumeFacts, RarVolumeHeaderEncryption, RarVolumeMemberEncryptionFacts,
};
use weaver_graph::{
    ByteRange, Coverage, Durable, Facts, Give, Kind, Object, RestoreError, Sealed, Take,
    VolumeIndex,
};

use super::walk::Block;
use super::{Entry, Lock, Out, Piece, RarStore, Volume};
use crate::crypt::{Chain, Padding};
use crate::kept::Kept;
use crate::runs::CrcRuns;

#[derive(Serialize, Deserialize)]
pub(super) struct StoreFacts<V = Sealed<VolumeFacts>> {
    volumes: Vec<V>,
    pub(super) entries: Vec<EntryFacts>,
    asked: bool,
    damage_said: BTreeSet<(VolumeIndex, u64, u64, u32)>,
    recovery_said: BTreeSet<(VolumeIndex, u64)>,
    gaps_said: BTreeSet<VolumeIndex>,
    gone: bool,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct VolumeFacts {
    index: VolumeIndex,
    name: Option<String>,
    length: Option<u64>,
    closed: bool,
    kept: Kept,
    headers: Option<RarVolumeFacts>,
    ended: bool,
    locked: Option<RarVolumeHeaderEncryption>,
    // What the front stated when it was bound, which can precede the walk's
    // first header read.
    front: Option<RarVolumeFacts>,
}

impl Default for VolumeFacts {
    fn default() -> Self {
        Self {
            index: VolumeIndex(0),
            name: None,
            length: None,
            closed: false,
            kept: Kept::default(),
            headers: None,
            ended: false,
            locked: None,
            front: None,
        }
    }
}

impl VolumeFacts {
    // A debug check on mutation-site invalidation, including provenance:
    // a later repair can replace headers that were already parsed.
    fn agrees(&self, index: VolumeIndex, volume: &Volume) -> bool {
        self.index == index
            && self.name == volume.name
            && self.length == volume.length
            && self.closed == volume.closed
            && self.kept == volume.kept
            && self.headers == volume.walk.facts
            && self.ended == volume.walk.ended
            && self.locked == volume.walk.locked
            && self.front == volume.front
    }
}

fn volume(index: VolumeIndex, volume: &Volume) -> Sealed<VolumeFacts> {
    let mut cached = volume
        .facts
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if let Some(facts) = cached.as_ref()
        && facts.get().index == index
    {
        debug_assert!(
            facts.get().agrees(index, volume),
            "volume metadata changed without invalidating its facts"
        );
        return facts.clone();
    }
    let facts = Sealed::new(VolumeFacts {
        index,
        name: volume.name.clone(),
        length: volume.length,
        closed: volume.closed,
        kept: volume.kept.clone(),
        headers: volume.walk.facts.clone(),
        ended: volume.walk.ended,
        locked: volume.walk.locked,
        front: volume.front.clone(),
    });
    *cached = Some(facts.clone());
    facts
}

#[derive(Serialize, Deserialize)]
pub(super) struct EntryFacts {
    name: String,
    length: Option<u64>,
    crc: Option<u32>,
    uses_mac: bool,
    pieces: Vec<PieceFacts>,
    complete: bool,
    pub(super) keying: Option<KeyTuple>,
    coverage: Coverage,
    closed: bool,
    padding: Padding,
    // An open cipher's blocks, never its key.
    chain: Option<Chain>,
}

#[derive(Serialize, Deserialize)]
struct PieceFacts {
    volume: VolumeIndex,
    data: ByteRange,
    at: u64,
    crc: Option<u32>,
    uses_mac: bool,
    after: bool,
}

// The header's own statement of how a member is keyed.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum KeyTuple {
    Rar5(RarVolumeMemberEncryptionFacts),
    Rar4(Option<[u8; 8]>),
}

impl From<MemberKeying> for KeyTuple {
    fn from(keying: MemberKeying) -> Self {
        match keying {
            MemberKeying::Rar5(facts) => Self::Rar5(facts),
            MemberKeying::Rar4 { salt } => Self::Rar4(salt),
        }
    }
}

impl Durable for StoreFacts {
    // 2: the front each volume stated when it was bound.
    // 3: a kept cipher block says whether an arrival started inside it.
    // 4: each volume's metadata is an independently reusable sealed part.
    const VERSION: u16 = 4;

    fn parts(&self, take: &mut Take) -> Result<(), String> {
        self.volumes
            .iter()
            .try_for_each(|volume| take.sealed(volume))
    }

    fn restore_parts(&mut self, give: &mut Give) -> Result<(), String> {
        self.volumes
            .iter_mut()
            .try_for_each(|volume| give.sealed(volume))
    }

    fn impossible(&self) -> Option<&'static str> {
        impossible(self.volumes.iter().map(Sealed::get), &self.entries)
    }
}

// Version 3 kept the same volume records inline. Read those checkpoints
// too; only newly captured facts use sealed parts.
impl Durable for StoreFacts<VolumeFacts> {
    const VERSION: u16 = 3;

    fn impossible(&self) -> Option<&'static str> {
        impossible(self.volumes.iter(), &self.entries)
    }
}

impl<V> StoreFacts<V> {
    fn map_volumes<W>(self, map: impl FnMut(V) -> W) -> StoreFacts<W> {
        StoreFacts {
            volumes: self.volumes.into_iter().map(map).collect(),
            entries: self.entries,
            asked: self.asked,
            damage_said: self.damage_said,
            recovery_said: self.recovery_said,
            gaps_said: self.gaps_said,
            gone: self.gone,
        }
    }
}

fn impossible<'a>(
    volumes: impl Iterator<Item = &'a VolumeFacts> + Clone,
    entries: &[EntryFacts],
) -> Option<&'static str> {
    let indices: BTreeSet<_> = volumes.clone().map(|volume| volume.index).collect();
    if indices.len() != volumes.clone().count() {
        return Some("a volume recorded twice");
    }
    if !volumes.clone().all(|volume| volume.kept.sound()) {
        return Some("kept header bytes that overlap");
    }
    let placed = entries.iter().all(|entry| {
        entry.pieces.windows(2).all(|pair| pair[0].at < pair[1].at)
            && entry
                .pieces
                .iter()
                .all(|piece| indices.contains(&piece.volume))
    });
    (!placed).then_some("a member piece out of order or in no volume")
}

pub(super) fn of(store: &RarStore) -> Facts {
    let volumes = store
        .volumes
        .iter()
        .map(|(index, found)| volume(*index, found))
        .collect();
    let entries = store
        .entries
        .iter()
        .map(|entry| EntryFacts {
            name: entry.name.clone(),
            length: entry.length,
            crc: entry.crc,
            uses_mac: entry.uses_mac,
            pieces: entry
                .pieces
                .iter()
                .map(|piece| PieceFacts {
                    volume: piece.volume,
                    data: piece.data,
                    at: piece.at,
                    crc: piece.crc,
                    uses_mac: piece.uses_mac,
                    after: piece.after,
                })
                .collect(),
            complete: entry.complete,
            keying: keying(entry),
            coverage: entry.out.coverage.clone(),
            closed: entry.out.closed,
            padding: entry.out.padding.clone(),
            chain: match &entry.lock {
                Lock::Open(cbc) => Some(cbc.chain()),
                Lock::Sealed(chain) => Some(Chain::clone(chain)),
                Lock::Plain | Lock::Locked => None,
            },
        })
        .collect();
    let facts = StoreFacts {
        volumes,
        entries,
        asked: store.asked,
        damage_said: store.damage_said.iter().copied().collect(),
        recovery_said: store.recovery_said.clone(),
        gaps_said: store.gaps_said.clone(),
        gone: store.gone,
    };
    Facts::of(Kind::RarStore, &facts)
}

fn keying(entry: &Entry) -> Option<KeyTuple> {
    entry.store.map(|store| store.keying().into())
}

pub(crate) fn restore(facts: &Facts) -> Result<Box<dyn Object>, RestoreError> {
    let kept: StoreFacts<VolumeFacts> = match facts.version {
        3 => facts.read(Kind::RarStore)?,
        _ => facts
            .read::<StoreFacts>(Kind::RarStore)?
            .map_volumes(|sealed| sealed.get().clone()),
    };
    let mut store = RarStore::new();
    for found in kept.volumes {
        let mut volume = Volume::new();
        if let Some(name) = &found.name {
            volume.name_as(name);
        }
        volume.length = found.length;
        volume.closed = found.closed;
        volume.kept = found.kept;
        volume.front = found.front;
        volume.walk.blocks = found.headers.as_ref().map_or_else(Vec::new, |headers| {
            headers
                .members
                .iter()
                .map(|member| Block {
                    data: ByteRange::at(member.data_offset, member.data_size),
                })
                .collect()
        });
        volume.walk.restore(found.headers);
        volume.walk.ended = found.ended;
        volume.walk.locked = found.locked;
        store.volumes.insert(found.index, volume);
        // The walk reads the kept headers again at the look below.
        store.file(found.index);
        store.touch(found.index);
    }
    let keyings: Vec<_> = kept.entries.iter().map(|entry| entry.keying).collect();
    store.entries = kept.entries.into_iter().map(entry).collect();
    store.remap();
    store.damage_said = kept.damage_said.into_iter().collect();
    store.recovery_said = kept.recovery_said;
    store.gaps_said = kept.gaps_said;
    store.gone = kept.gone;
    // The walk reads the kept headers again; what it says now was said
    // before the checkpoint, so it goes nowhere.
    store.settle(None, &mut Vec::new());
    // Nor is a question it asked while no password was on hand yet: the
    // server offers the winning one again, and one never asked is asked
    // by the next look.
    store.asked = kept.asked;
    let agrees = store.entries.len() == keyings.len()
        && store
            .entries
            .iter()
            .zip(&keyings)
            .all(|(entry, recorded)| keying(entry) == *recorded);
    if !agrees && !store.gone {
        return Err(RestoreError::Impossible {
            kind: Kind::RarStore,
            reason: "kept headers that no longer read as recorded",
        });
    }
    Ok(Box::new(store))
}

fn entry(facts: EntryFacts) -> Entry {
    Entry {
        name: facts.name,
        length: facts.length,
        crc: facts.crc,
        uses_mac: facts.uses_mac,
        store: None,
        pieces: facts
            .pieces
            .into_iter()
            .map(|piece| Piece {
                volume: piece.volume,
                data: piece.data,
                at: piece.at,
                crc: piece.crc,
                uses_mac: piece.uses_mac,
                after: piece.after,
            })
            .collect(),
        complete: facts.complete,
        lock: match (facts.keying, facts.chain) {
            (Some(_), Some(chain)) => Lock::Sealed(Box::new(chain)),
            (Some(_), None) => Lock::Locked,
            (None, _) => Lock::Plain,
        },
        hash_key: None,
        trial: None,
        condemned_on: Coverage::new(),
        out: Arc::new(Out {
            coverage: facts.coverage,
            closed: facts.closed,
            padding: facts.padding,
        }),
        plain_runs: CrcRuns::default(),
        parts: Default::default(),
    }
}

#[cfg(test)]
mod cache_tests {
    use weaver_graph::{Delivery, Evidenced, Provenance};

    use super::*;
    use crate::rar_store::tests::writer::{Item, Options, Version, write};

    // Captured with the version 3 writer from bad838928: one RAR5
    // legacy.bin member containing 32 bytes of value 37, fully delivered.
    #[test]
    fn original_version_three_facts_restore_and_upgrade_without_losing_metadata() {
        let bytes = include_bytes!("tests/rar-facts-v3.msgpack");
        let original = Facts {
            kind: Kind::RarStore,
            version: 3,
            bytes: bytes.to_vec(),
            parts: Vec::new(),
        };
        let legacy: StoreFacts<VolumeFacts> = original.read(Kind::RarStore).unwrap();
        assert_eq!(Facts::of(Kind::RarStore, &legacy).bytes, bytes);
        let restored = restore(&original).unwrap().facts();
        assert_eq!(restored.version, 4);
        let current: StoreFacts = restored.read(Kind::RarStore).unwrap();
        assert_eq!(current.entries.len(), 1);
        assert_eq!(current.entries[0].name, "legacy.bin");
        assert_eq!(current.entries[0].length, Some(32));
        assert_eq!(current.entries[0].pieces[0].data, ByteRange::new(44, 76));
        assert!(current.entries[0].coverage.covered(ByteRange::at(0, 32)));
        assert!(current.entries[0].closed);
        assert_eq!(current.volumes[0].get().kept, legacy.volumes[0].kept);
    }

    fn archive(version: Version, name: &'static str, length: usize) -> Vec<u8> {
        write(
            &[Item::File(name, vec![37; length])],
            &Options::new(version),
        )
        .remove(0)
    }

    fn deliver(store: &mut RarStore, offset: u64, bytes: &[u8], provenance: Provenance) {
        store.deliver(
            Delivery {
                volume: VolumeIndex(0),
                offset,
                bytes: &Evidenced::from(bytes.to_vec()),
                provenance,
            },
            &mut |_, _| false,
            &mut Vec::new(),
        );
        assert!(!store.gone, "the stored layout remains usable");
    }

    fn loaded(bytes: &[u8]) -> RarStore {
        let mut store = RarStore::new();
        store.bound(VolumeIndex(0), "alpha.rar", &[], None);
        store.sized(VolumeIndex(0), bytes.len() as u64, &mut Vec::new());
        deliver(&mut store, 0, bytes, Provenance::Fresh);
        store
    }

    fn same_parts(before: &Facts, after: &Facts) {
        assert_eq!(before.parts.len(), after.parts.len());
        for (before, after) in before.parts.iter().zip(&after.parts) {
            assert_eq!(before.key, after.key);
            assert!(Arc::ptr_eq(&before.bytes, &after.bytes), "reused encoding");
        }
    }

    #[test]
    fn payload_arrivals_reuse_volume_metadata_but_capture_current_coverage() {
        for version in [Version::Rar4, Version::Rar5] {
            let bytes = archive(version, "payload.bin", 4096);
            let mut store = loaded(&bytes);
            let before = store.facts();
            same_parts(&before, &store.facts());
            let data = store.entries[0].pieces[0].data;
            store.unposted(VolumeIndex(0), data);
            let erased = store.facts();
            same_parts(&before, &erased);
            let decoded: StoreFacts = erased.read(Kind::RarStore).unwrap();
            assert!(!decoded.entries[0].coverage.covered(ByteRange::at(0, 4096)));
            deliver(
                &mut store,
                data.start,
                &bytes[data.start as usize..data.end as usize],
                Provenance::Repaired,
            );
            let after = store.facts();
            same_parts(&before, &after);
            let decoded: StoreFacts = after.read(Kind::RarStore).unwrap();
            assert!(decoded.entries[0].coverage.verified(ByteRange::at(0, 4096)));
            assert_ne!(erased.bytes, after.bytes, "coverage is never cached");
            let restored = restore(&after).unwrap().facts();
            let decoded: StoreFacts = restored.read(Kind::RarStore).unwrap();
            assert!(decoded.entries[0].coverage.verified(ByteRange::at(0, 4096)));
        }
    }

    #[test]
    fn repair_replaces_cached_headers_names_sizes_and_piece_geometry() {
        for version in [Version::Rar4, Version::Rar5] {
            let original = archive(version, "old.bin", 80);
            let repaired = archive(version, "repaired-long.bin", 70);
            assert_eq!(original.len(), repaired.len());
            let mut store = loaded(&original);
            let before = store.facts();
            same_parts(&before, &store.facts());
            // Reuse the cache for a payload arrival before the repair lands.
            let old_data = store.entries[0].pieces[0].data;
            deliver(
                &mut store,
                old_data.start,
                &original[old_data.start as usize..old_data.end as usize],
                Provenance::Fresh,
            );
            same_parts(&before, &store.facts());
            let repaired_data = loaded(&repaired).entries[0].pieces[0].data;
            assert_eq!(old_data.end, repaired_data.end);
            // Only the replacement header arrives; existing payload bytes
            // must not mask a stale encoding of the old header or layout.
            deliver(
                &mut store,
                0,
                &repaired[..repaired_data.start as usize],
                Provenance::Repaired,
            );
            let after = store.facts();
            assert_eq!(after.parts.len(), 1);
            assert!(!Arc::ptr_eq(&before.parts[0].bytes, &after.parts[0].bytes));
            assert_ne!(before.parts[0].bytes, after.parts[0].bytes);
            for facts in [&after, &restore(&after).unwrap().facts()] {
                let decoded: StoreFacts = facts.read(Kind::RarStore).unwrap();
                let volume = decoded.volumes[0].get();
                assert_eq!(
                    volume.headers.as_ref().unwrap().members[0].name,
                    "repaired-long.bin"
                );
                assert_eq!(decoded.entries[0].name, "repaired-long.bin");
                assert_eq!(decoded.entries[0].length, Some(70));
                assert_eq!(decoded.entries[0].pieces[0].data.len(), 70);
                assert_ne!(decoded.entries[0].pieces[0].data.start, old_data.start);
                assert!(!crate::testing::replay::contains(
                    &facts.parts[0].bytes,
                    b"old.bin"
                ));
            }
            same_parts(&after, &store.facts());
        }
    }

    #[test]
    fn a_provenance_only_header_repair_replaces_the_cached_part() {
        let bytes = archive(Version::Rar5, "payload.bin", 128);
        let mut store = loaded(&bytes);
        let before = store.facts();
        let header_end = store.entries[0].pieces[0].data.start;
        deliver(
            &mut store,
            0,
            &bytes[..header_end as usize],
            Provenance::Repaired,
        );
        let after = store.facts();
        assert_ne!(before.parts[0].bytes, after.parts[0].bytes);
        let decoded: StoreFacts = after.read(Kind::RarStore).unwrap();
        assert!(
            decoded.volumes[0]
                .get()
                .kept
                .runs()
                .iter()
                .any(|(_, _, provenance)| *provenance == Provenance::Repaired)
        );
        assert!(restore(&after).is_ok());
        same_parts(&after, &store.facts());
    }

    #[test]
    fn later_headers_names_lengths_and_closure_refresh_cached_metadata() {
        let bytes = archive(Version::Rar5, "payload.bin", 4096);
        let geometry = loaded(&bytes);
        let data = geometry.entries[0].pieces[0].data;
        let mut store = RarStore::new();
        store.bound(VolumeIndex(0), "alpha.rar", &[], None);
        deliver(
            &mut store,
            0,
            &bytes[..data.start as usize],
            Provenance::Fresh,
        );
        let first = store.facts();
        let decoded: StoreFacts = first.read(Kind::RarStore).unwrap();
        assert!(!decoded.volumes[0].get().ended);
        deliver(
            &mut store,
            data.end,
            &bytes[data.end as usize..],
            Provenance::Fresh,
        );
        let ended = store.facts();
        assert_ne!(first.parts[0].bytes, ended.parts[0].bytes);
        let decoded: StoreFacts = ended.read(Kind::RarStore).unwrap();
        assert!(decoded.volumes[0].get().ended);
        same_parts(&ended, &store.facts());
        store.renamed(VolumeIndex(0), "renamed.rar", &mut Vec::new());
        let renamed = store.facts();
        assert_ne!(ended.parts[0].bytes, renamed.parts[0].bytes);
        store.sized(VolumeIndex(0), bytes.len() as u64, &mut Vec::new());
        let sized = store.facts();
        assert_ne!(renamed.parts[0].bytes, sized.parts[0].bytes);
        store.volume_closed(VolumeIndex(0), &mut Vec::new());
        let closed = store.facts();
        assert_ne!(sized.parts[0].bytes, closed.parts[0].bytes);
        let decoded: StoreFacts = closed.read(Kind::RarStore).unwrap();
        let volume = decoded.volumes[0].get();
        assert_eq!(volume.name.as_deref(), Some("renamed.rar"));
        assert_eq!(volume.length, Some(bytes.len() as u64));
        assert!(volume.closed);
        assert!(restore(&closed).is_ok());
        same_parts(&closed, &store.facts());
    }

    #[test]
    fn a_password_scheduled_walk_refreshes_metadata_cached_while_headers_were_locked() {
        for version in [Version::Rar4, Version::Rar5] {
            let mut options = Options::new(version);
            options.password = Some("cache-test-password");
            options.encrypt_headers = true;
            let bytes = write(&[Item::File("payload.bin", vec![37; 128])], &options).remove(0);
            let mut store = loaded(&bytes);
            let locked = store.facts();
            let decoded: StoreFacts = locked.read(Kind::RarStore).unwrap();
            assert!(decoded.volumes[0].get().locked.is_some());
            assert!(decoded.entries.is_empty());
            same_parts(&locked, &store.facts());

            store.offer_password("cache-test-password", &mut Vec::new());
            let offered = store.facts();
            assert!(!Arc::ptr_eq(
                &locked.parts[0].bytes,
                &offered.parts[0].bytes
            ));
            assert!(restore(&offered).is_ok());
            deliver(&mut store, 0, &bytes, Provenance::Fresh);
            let opened = store.facts();
            let decoded: StoreFacts = opened.read(Kind::RarStore).unwrap();
            assert!(decoded.volumes[0].get().locked.is_none());
            assert_eq!(decoded.entries[0].name, "payload.bin");
            assert_eq!(decoded.entries[0].length, Some(128));
            assert!(restore(&opened).is_ok());
            same_parts(&opened, &store.facts());
        }
    }

    #[test]
    fn a_damaged_arrival_growing_wanted_headers_refreshes_cached_metadata() {
        let bytes = archive(Version::Rar5, "payload.bin", 4096);
        let mut store = RarStore::new();
        store.bound(VolumeIndex(0), "alpha.rar", &[], None);
        store.sized(VolumeIndex(0), bytes.len() as u64, &mut Vec::new());
        let before = store.facts();
        same_parts(&before, &store.facts());
        let prefix = ByteRange::at(0, 100);
        deliver(&mut store, 0, &bytes[..100], Provenance::Damaged);
        let damaged = store.facts();
        assert_ne!(before.parts[0].bytes, damaged.parts[0].bytes);
        let decoded: StoreFacts = damaged.read(Kind::RarStore).unwrap();
        let kept = &decoded.volumes[0].get().kept;
        assert_eq!(kept.read(prefix), Some(&bytes[..100]));
        assert_eq!(kept.len(), prefix.len());
        assert!(
            kept.runs()
                .iter()
                .all(|(_, _, provenance)| *provenance == Provenance::Damaged)
        );
        assert!(restore(&damaged).is_ok());
        same_parts(&damaged, &store.facts());
        deliver(&mut store, 0, &bytes[..100], Provenance::Repaired);
        let repaired = store.facts();
        assert_ne!(damaged.parts[0].bytes, repaired.parts[0].bytes);
        let decoded: StoreFacts = repaired.read(Kind::RarStore).unwrap();
        assert_eq!(decoded.entries[0].name, "payload.bin");
        assert!(decoded.volumes[0].get().kept.verified(prefix));
        assert!(restore(&repaired).is_ok());
    }
}
