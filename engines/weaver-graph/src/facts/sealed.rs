// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The parts of an object's facts that are encoded where they change, not
// each time the facts are taken, and kept apart from the facts' own bytes.
// A job checkpoints after every step, and an object's facts often hold a
// large part that settles early, or one that grows a piece at a time,
// beside a small part that moves with every delivery; encoding and writing
// the large part each step makes every step cost the whole of it.
//
// A `Sealed` value is one part; a `Shelf` is a run of them, one per chunk,
// for a value that grows. Either is reached for change only through
// `change`, which encodes the part again before returning, so its bytes
// always say what it holds and nothing outside has a copy to keep fresh.
// Within its owner's facts it encodes as nothing: the owner walks its sealed
// values in the order its encoding meets them, and each is numbered by its
// place in that walk. Its bytes are the facts' parts, each under that
// number and its chunk, so a checkpoint writes again only the parts whose
// bytes changed. A restore decodes the owner with each sealed value empty,
// then walks them in the same order to give each its parts. The values and
// their bytes are shared, so taking the owner's facts copies neither.
//
// A part does not hold another: a part's own encoding is no owner's walk,
// so a sealed value inside it keeps no bytes and comes back empty.
//
// A sealed value counts its changes, and a shelf marks each chunk it
// changes under a generation of its own, so the same walk can take only the
// parts changed since the generations it took last: an owner that keeps
// them pays for what changed since, not for all it holds.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::Durable;
use crate::marks::Marks;

// One part of a facts blob: its shelf in the high half of the key, its
// chunk in the low half.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Part {
    pub key: u64,
    pub bytes: Arc<[u8]>,
}

// A part changed since the walk before: its bytes now, or `None` for one
// that went away.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change {
    pub key: u64,
    pub bytes: Option<Arc<[u8]>>,
}

fn key(shelf: u32, chunk: u32) -> u64 {
    (u64::from(shelf) << 32) | u64::from(chunk)
}

// A state's bytes and the parts they name.
pub fn encode_parts<T: Durable>(state: &T) -> Result<(Vec<u8>, Vec<Part>), String> {
    let taken = encode_since(state, None)?;
    Ok((taken.bytes, taken.parts))
}

// A state's bytes, and the generation of each of its sealed values, by its
// number in the walk: with `since` the generations of a walk before, the
// parts changed after them; without, every part.
pub struct Taken {
    pub bytes: Vec<u8>,
    pub parts: Vec<Part>,
    pub changes: Vec<Change>,
    pub at: Vec<u64>,
}

pub fn encode_since<T: Durable>(state: &T, since: Option<&[u64]>) -> Result<Taken, String> {
    let mut take = Take {
        since,
        ..Take::default()
    };
    state.parts(&mut take)?;
    Ok(Taken {
        bytes: rmp_serde::to_vec(state).map_err(|error| error.to_string())?,
        parts: take.parts,
        changes: take.changes,
        at: take.at,
    })
}

// A state back from its bytes and every part they name: a part they do not
// name is a blob that is not the one its bytes were taken with.
pub fn decode_parts<T: Durable>(bytes: &[u8], parts: &[Part]) -> Result<T, String> {
    let mut state: T = rmp_serde::from_slice(bytes).map_err(|error| error.to_string())?;
    let mut give = Give::new(parts)?;
    state.restore_parts(&mut give)?;
    if give.parts.is_empty() {
        Ok(state)
    } else {
        Err("a part the facts do not name".to_owned())
    }
}

// The walk of an owner's sealed values as its facts are taken: each leaves
// its parts under the next number, every part, or with `since` only those
// changed after the generation its number had then.
#[derive(Default)]
pub struct Take<'a> {
    next: u32,
    since: Option<&'a [u64]>,
    parts: Vec<Part>,
    changes: Vec<Change>,
    at: Vec<u64>,
}

impl Take<'_> {
    pub fn sealed<T>(&mut self, sealed: &Sealed<T>) -> Result<(), String> {
        let shelf = self.shelve()?;
        self.at.push(sealed.at);
        match self.since(shelf) {
            None => self.keep(shelf, 0, sealed.bytes.as_ref()),
            Some(since) if sealed.at > since => self.change(shelf, 0, Some(sealed)),
            Some(_) => Ok(()),
        }
    }

    pub fn shelf<T>(&mut self, shelf: &Shelf<T>) -> Result<(), String> {
        let number = self.shelve()?;
        self.at.push(shelf.marks.at());
        match self.since(number) {
            None => shelf
                .chunks
                .iter()
                .try_for_each(|(chunk, sealed)| self.keep(number, *chunk, sealed.bytes.as_ref())),
            Some(since) => shelf
                .marks
                .since(since)
                .try_for_each(|chunk| self.change(number, chunk, shelf.chunks.get(&chunk))),
        }
    }

    // A value the walk before did not reach has changed whole.
    fn since(&self, number: u32) -> Option<u64> {
        self.since
            .map(|since| since.get(number as usize).copied().unwrap_or(0))
    }

    fn change<T>(
        &mut self,
        shelf: u32,
        chunk: u32,
        sealed: Option<&Sealed<T>>,
    ) -> Result<(), String> {
        let bytes = match sealed {
            None => None,
            Some(sealed) => Some(Arc::clone(
                sealed
                    .bytes
                    .as_ref()
                    .ok_or_else(|| "a sealed part does not encode".to_owned())?,
            )),
        };
        self.changes.push(Change {
            key: key(shelf, chunk),
            bytes,
        });
        Ok(())
    }

    fn shelve(&mut self) -> Result<u32, String> {
        let shelf = self.next;
        self.next = shelf
            .checked_add(1)
            .ok_or_else(|| "more sealed parts than a key numbers".to_owned())?;
        Ok(shelf)
    }

    fn keep(&mut self, shelf: u32, chunk: u32, bytes: Option<&Arc<[u8]>>) -> Result<(), String> {
        let bytes = bytes.ok_or_else(|| "a sealed part does not encode".to_owned())?;
        self.parts.push(Part {
            key: key(shelf, chunk),
            bytes: Arc::clone(bytes),
        });
        Ok(())
    }
}

// The walk of an owner's sealed values as its facts are restored, in the
// order they were taken: each is given the parts under its number.
pub struct Give {
    parts: BTreeMap<u64, Arc<[u8]>>,
    next: u32,
}

impl Give {
    fn new(parts: &[Part]) -> Result<Self, String> {
        let kept: BTreeMap<u64, Arc<[u8]>> = parts
            .iter()
            .map(|part| (part.key, Arc::clone(&part.bytes)))
            .collect();
        if kept.len() != parts.len() {
            return Err("a part recorded twice".to_owned());
        }
        Ok(Self {
            parts: kept,
            next: 0,
        })
    }

    pub fn sealed<T: DeserializeOwned>(&mut self, sealed: &mut Sealed<T>) -> Result<(), String> {
        let shelf = self.shelve();
        match self.chunks(shelf).as_slice() {
            [(0, bytes)] => {
                let at = sealed.at + 1;
                *sealed = decode(Arc::clone(bytes))?;
                sealed.at = at;
                Ok(())
            }
            _ => Err("a sealed part is not one part".to_owned()),
        }
    }

    pub fn shelf<T: DeserializeOwned>(&mut self, shelf: &mut Shelf<T>) -> Result<(), String> {
        let number = self.shelve();
        for (chunk, bytes) in self.chunks(number) {
            shelf.chunks.insert(chunk, decode(bytes)?);
            shelf.touch(chunk);
        }
        Ok(())
    }

    fn shelve(&mut self) -> u32 {
        let shelf = self.next;
        self.next = shelf.saturating_add(1);
        shelf
    }

    // Every chunk of a shelf, in chunk order, each given once.
    fn chunks(&mut self, shelf: u32) -> Vec<(u32, Arc<[u8]>)> {
        let keys: Vec<u64> = self
            .parts
            .range(key(shelf, 0)..=key(shelf, u32::MAX))
            .map(|(key, _)| *key)
            .collect();
        keys.into_iter()
            .filter_map(|key| self.parts.remove_entry(&key))
            .map(|(key, bytes)| (key as u32, bytes))
            .collect()
    }
}

fn decode<T: DeserializeOwned>(bytes: Arc<[u8]>) -> Result<Sealed<T>, String> {
    let value = rmp_serde::from_slice(&bytes).map_err(|error| error.to_string())?;
    Ok(Sealed {
        value: Arc::new(value),
        bytes: Some(bytes),
        at: 1,
    })
}

// A part's own bytes.
fn encode<T: Serialize>(value: &T) -> Option<Arc<[u8]>> {
    rmp_serde::to_vec(value).ok().map(Arc::from)
}

#[derive(Clone)]
pub struct Sealed<T> {
    value: Arc<T>,
    // `None` when the value would not encode: the owner's facts then fail
    // to encode too, and say so as unsupported.
    bytes: Option<Arc<[u8]>>,
    // How many times it changed, its making the first.
    at: u64,
}

impl<T: Serialize + Clone> Sealed<T> {
    pub fn new(value: T) -> Self {
        let bytes = encode(&value);
        Self {
            value: Arc::new(value),
            bytes,
            at: 1,
        }
    }

    pub fn get(&self) -> &T {
        &self.value
    }

    // The one way to change the value: it is encoded again on the way out.
    pub fn change<R>(&mut self, change: impl FnOnce(&mut T) -> R) -> R {
        let out = change(Arc::make_mut(&mut self.value));
        self.bytes = encode(&self.value);
        self.at += 1;
        out
    }

    // The value made `value`, encoded again only when that changes it.
    pub fn set(&mut self, value: T)
    where
        T: PartialEq,
    {
        if *self.value != value {
            self.bytes = encode(&value);
            self.value = Arc::new(value);
            self.at += 1;
        }
    }

    // The bytes the value last encoded to.
    pub fn bytes(&self) -> Option<&[u8]> {
        self.bytes.as_deref()
    }
}

impl<T: Serialize + Clone + Default> Default for Sealed<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

impl<T: fmt::Debug> fmt::Debug for Sealed<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(f)
    }
}

impl<T: PartialEq> PartialEq for Sealed<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T: Eq> Eq for Sealed<T> {}

impl<T> Serialize for Sealed<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_unit()
    }
}

// Empty: its owner's restore gives it its value.
impl<'de, T: Default> Deserialize<'de> for Sealed<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <()>::deserialize(deserializer)?;
        Ok(Self {
            value: Arc::default(),
            bytes: None,
            at: 0,
        })
    }
}

// A value kept as chunks, each its own part: a value that grows a chunk at
// a time costs each change one chunk. The owner says which chunk an entry
// lies in, so a chunk keeps its number whatever grows around it.
#[derive(Clone)]
pub struct Shelf<T> {
    chunks: BTreeMap<u32, Sealed<T>>,
    // Each chunk changed, a chunk that went away among them.
    marks: Marks<u32>,
}

impl<T> Default for Shelf<T> {
    fn default() -> Self {
        Self {
            chunks: BTreeMap::new(),
            marks: Marks::default(),
        }
    }
}

impl<T> Shelf<T> {
    fn touch(&mut self, chunk: u32) {
        self.marks.mark(chunk);
    }
}

impl<T: Serialize + Clone + Default> Shelf<T> {
    pub fn get(&self, chunk: u32) -> Option<&T> {
        self.chunks.get(&chunk).map(Sealed::get)
    }

    pub fn chunks(&self) -> impl DoubleEndedIterator<Item = (u32, &T)> {
        self.chunks
            .iter()
            .map(|(chunk, sealed)| (*chunk, sealed.get()))
    }

    // Change one chunk, made empty first if the shelf has none; it is
    // encoded again on the way out, and dropped if `empty` says it holds
    // nothing then.
    pub fn change<R>(
        &mut self,
        chunk: u32,
        empty: impl FnOnce(&T) -> bool,
        change: impl FnOnce(&mut T) -> R,
    ) -> R {
        let sealed = self.chunks.entry(chunk).or_default();
        let out = sealed.change(change);
        if empty(sealed.get()) {
            self.chunks.remove(&chunk);
        }
        self.touch(chunk);
        out
    }

    // The shelf made these chunks, each encoded again only when it changed.
    pub fn set(&mut self, chunks: impl IntoIterator<Item = (u32, T)>)
    where
        T: PartialEq,
    {
        let mut next = BTreeMap::new();
        for (chunk, value) in chunks {
            self.put(chunk, Some(value));
            if let Some(sealed) = self.chunks.remove(&chunk) {
                next.insert(chunk, sealed);
            }
        }
        let gone: Vec<u32> = self.chunks.keys().copied().collect();
        for chunk in gone {
            self.put(chunk, None);
        }
        self.chunks = next;
    }

    // One chunk made `value`, encoded again only when that changes it, or
    // dropped for `None`; every other chunk is left as it is.
    pub fn put(&mut self, chunk: u32, value: Option<T>)
    where
        T: PartialEq,
    {
        let changed = match (self.chunks.get_mut(&chunk), value) {
            (Some(sealed), Some(value)) => {
                let changed = *sealed.get() != value;
                sealed.set(value);
                changed
            }
            (None, Some(value)) => {
                self.chunks.insert(chunk, Sealed::new(value));
                true
            }
            (Some(_), None) => {
                self.chunks.remove(&chunk);
                true
            }
            (None, None) => false,
        };
        if changed {
            self.touch(chunk);
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for Shelf<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.chunks.iter()).finish()
    }
}

impl<T: PartialEq> PartialEq for Shelf<T> {
    fn eq(&self, other: &Self) -> bool {
        self.chunks == other.chunks
    }
}

impl<T: Eq> Eq for Shelf<T> {}

impl<T> Serialize for Shelf<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_unit()
    }
}

// Empty: its owner's restore gives it its chunks.
impl<'de, T> Deserialize<'de> for Shelf<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <()>::deserialize(deserializer)?;
        Ok(Self::default())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde::{Deserialize, Serialize};

    use super::{Change, Give, Sealed, Shelf, Take, decode_parts, encode_parts, encode_since};
    use crate::facts::Durable;

    // Counts its own encodings, so a test can see when a value is encoded.
    #[derive(Clone, Debug, Default, Deserialize)]
    #[serde(from = "Vec<u32>")]
    struct Counted {
        items: Vec<u32>,
        seen: Option<&'static AtomicUsize>,
    }

    impl From<Vec<u32>> for Counted {
        fn from(items: Vec<u32>) -> Self {
            Self { items, seen: None }
        }
    }

    impl Serialize for Counted {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            if let Some(seen) = self.seen {
                seen.fetch_add(1, Ordering::SeqCst);
            }
            self.items.serialize(serializer)
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Owner {
        moving: u64,
        settled: Sealed<Counted>,
        grown: Shelf<Vec<u32>>,
    }

    impl Durable for Owner {
        const VERSION: u16 = 1;

        fn parts(&self, take: &mut Take) -> Result<(), String> {
            take.sealed(&self.settled)?;
            take.shelf(&self.grown)
        }

        fn restore_parts(&mut self, give: &mut Give) -> Result<(), String> {
            give.sealed(&mut self.settled)?;
            give.shelf(&mut self.grown)
        }
    }

    // Holds a part inside a part.
    #[derive(Serialize, Deserialize)]
    struct Nested {
        outer: Sealed<Sealed<Vec<u32>>>,
    }

    impl Durable for Nested {
        const VERSION: u16 = 1;

        fn parts(&self, take: &mut Take) -> Result<(), String> {
            take.sealed(&self.outer)
        }

        fn restore_parts(&mut self, give: &mut Give) -> Result<(), String> {
            give.sealed(&mut self.outer)
        }
    }

    #[test]
    fn an_unchanged_part_is_not_encoded_again_when_its_owner_is() {
        static SEEN: AtomicUsize = AtomicUsize::new(0);
        let mut owner = Owner {
            moving: 0,
            settled: Sealed::new(Counted {
                items: vec![7, 8, 9],
                seen: Some(&SEEN),
            }),
            grown: Shelf::default(),
        };
        assert_eq!(SEEN.load(Ordering::SeqCst), 1, "sealed once");
        let (first, first_parts) = encode_parts(&owner).unwrap();
        owner.moving += 1;
        let (second, second_parts) = encode_parts(&owner).unwrap();
        assert_ne!(first, second);
        assert_eq!(first_parts, second_parts, "the parts are the same parts");
        assert_eq!(
            SEEN.load(Ordering::SeqCst),
            1,
            "the owner's encodings reuse it"
        );
        owner.settled.change(|counted| counted.items.push(10));
        assert_eq!(SEEN.load(Ordering::SeqCst), 2, "a change seals it again");
        let (bytes, parts) = encode_parts(&owner).unwrap();
        let restored: Owner = decode_parts(&bytes, &parts).unwrap();
        assert_eq!(restored.moving, 1);
        assert_eq!(restored.settled.get().items, [7, 8, 9, 10]);
        assert_eq!(restored.settled.bytes(), owner.settled.bytes());
    }

    #[test]
    fn a_shelf_changes_only_the_chunk_it_was_changed_in() {
        let mut owner = Owner {
            moving: 0,
            settled: Sealed::default(),
            grown: Shelf::default(),
        };
        owner.grown.change(0, Vec::is_empty, |chunk| chunk.push(1));
        owner.grown.change(3, Vec::is_empty, |chunk| chunk.push(30));
        let (_, before) = encode_parts(&owner).unwrap();
        owner.grown.change(3, Vec::is_empty, |chunk| chunk.push(31));
        let (bytes, after) = encode_parts(&owner).unwrap();
        assert_eq!(before.len(), 3, "the sealed value and two chunks");
        let changed: Vec<_> = after.iter().filter(|part| !before.contains(part)).collect();
        assert_eq!(changed.len(), 1, "one chunk changed: {changed:?}");
        let restored: Owner = decode_parts(&bytes, &after).unwrap();
        assert_eq!(restored.grown, owner.grown);
        owner.grown.change(0, Vec::is_empty, Vec::clear);
        assert_eq!(owner.grown.get(0), None, "an emptied chunk is dropped");
    }

    #[test]
    fn a_walk_since_another_takes_the_parts_changed_after_it() {
        let mut owner = Owner {
            moving: 0,
            settled: Sealed::default(),
            grown: Shelf::default(),
        };
        owner.grown.put(2, Some(vec![20]));
        owner.grown.put(5, Some(vec![50]));
        let (_, before) = encode_parts(&owner).unwrap();
        let since = encode_since(&owner, None).unwrap().at;
        let changes_since = |owner: &Owner| {
            let taken = encode_since(owner, Some(&since)).unwrap();
            (taken.bytes, taken.changes)
        };
        let (_, none) = changes_since(&owner);
        assert!(none.is_empty(), "nothing changed: {none:?}");
        owner.grown.put(5, Some(vec![50]));
        owner.grown.put(7, None);
        let (_, same) = changes_since(&owner);
        assert!(same.is_empty(), "a value put again unchanged: {same:?}");
        owner.grown.put(5, Some(vec![51]));
        owner.grown.put(2, None);
        owner.moving += 1;
        let (bytes, changes) = changes_since(&owner);
        let (whole, after) = encode_parts(&owner).unwrap();
        assert_eq!(bytes, whole, "the owner's bytes are the one encoding");
        let changed = after.iter().find(|part| !before.contains(part)).unwrap();
        let gone = before
            .iter()
            .find(|part| !after.iter().any(|now| now.key == part.key));
        assert_eq!(
            changes,
            vec![
                Change {
                    key: changed.key,
                    bytes: Some(changed.bytes.clone()),
                },
                Change {
                    key: gone.unwrap().key,
                    bytes: None,
                },
            ]
        );
        let restored: Owner = decode_parts(&bytes, &after).unwrap();
        let restored_since = encode_since(&restored, Some(&[])).unwrap().changes;
        assert_eq!(
            restored_since.len(),
            after.len(),
            "a restore no walk has taken changes every part"
        );
    }

    #[test]
    fn facts_and_parts_that_do_not_belong_together_are_refused() {
        let owner = Owner {
            moving: 0,
            settled: Sealed::default(),
            grown: Shelf::default(),
        };
        let (bytes, mut parts) = encode_parts(&owner).unwrap();
        parts.push(parts[0].clone());
        assert!(decode_parts::<Owner>(&bytes, &parts).is_err(), "twice");
        parts.pop();
        let mut extra = parts[0].clone();
        extra.key = u64::MAX;
        parts.push(extra);
        assert!(decode_parts::<Owner>(&bytes, &parts).is_err(), "unnamed");
        parts.pop();
        parts.clear();
        assert!(decode_parts::<Owner>(&bytes, &parts).is_err(), "missing");
    }

    #[test]
    fn a_part_inside_a_part_comes_back_empty() {
        let outer = Sealed::new(Sealed::new(vec![1u32]));
        let (bytes, parts) = encode_parts(&Nested { outer }).unwrap();
        assert_eq!(parts.len(), 1, "only the outer part is taken");
        let restored: Nested = decode_parts(&bytes, &parts).unwrap();
        let inner = restored.outer.get();
        assert!(inner.get().is_empty());
        assert_eq!(inner.bytes(), None);
    }
}
