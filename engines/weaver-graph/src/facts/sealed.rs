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
// Within its owner's facts it is only a number, the order the owner's
// encoding met it in; its bytes are the facts' parts, each under that
// number and its chunk, so a checkpoint writes again only the parts whose
// bytes changed. The values and their bytes are shared, so taking the
// owner's facts copies neither.
//
// A part does not hold another: a sealed value's bytes are its own, so one
// inside another does not encode, and the owner's facts say so.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use serde::de::{self, DeserializeOwned, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// One part of a facts blob: its shelf in the high half of the key, its
// chunk in the low half.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Part {
    pub key: u64,
    pub bytes: Arc<[u8]>,
}

fn key(shelf: u32, chunk: u32) -> u64 {
    (u64::from(shelf) << 32) | u64::from(chunk)
}

thread_local! {
    // The parts an encoding in progress has met, when one is.
    static TAKING: RefCell<Option<Taking>> = const { RefCell::new(None) };
    // The parts a decoding in progress reads from, when one is.
    static READING: RefCell<Option<Reading>> = const { RefCell::new(None) };
}

#[derive(Default)]
struct Taking {
    next: u32,
    parts: Vec<Part>,
}

struct Reading {
    parts: BTreeMap<u64, Arc<[u8]>>,
    read: usize,
}

// Puts back what a thread's context held when it is dropped, so a panic in
// the middle of an encoding leaves nothing behind.
struct Scope<T: 'static> {
    cell: &'static std::thread::LocalKey<RefCell<Option<T>>>,
    previous: Option<Option<T>>,
}

impl<T: 'static> Scope<T> {
    fn enter(cell: &'static std::thread::LocalKey<RefCell<Option<T>>>, value: Option<T>) -> Self {
        Self {
            cell,
            previous: Some(cell.with(|slot| slot.replace(value))),
        }
    }

    fn leave(mut self) -> Option<T> {
        let previous = self.previous.take().flatten();
        self.cell.with(|slot| slot.replace(previous))
    }
}

impl<T: 'static> Drop for Scope<T> {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.take() {
            self.cell.with(|slot| slot.replace(previous));
        }
    }
}

// A value's bytes and the parts they name.
pub fn encode_parts<T: Serialize>(value: &T) -> Result<(Vec<u8>, Vec<Part>), String> {
    let scope = Scope::enter(&TAKING, Some(Taking::default()));
    let bytes = rmp_serde::to_vec(value).map_err(|error| error.to_string());
    let taken = scope.leave().unwrap_or_default();
    Ok((bytes?, taken.parts))
}

// A value back from its bytes and every part they name: a part they do not
// name is a blob that is not the one its bytes were taken with.
pub fn decode_parts<T: DeserializeOwned>(bytes: &[u8], parts: &[Part]) -> Result<T, String> {
    let kept: BTreeMap<u64, Arc<[u8]>> = parts
        .iter()
        .map(|part| (part.key, Arc::clone(&part.bytes)))
        .collect();
    if kept.len() != parts.len() {
        return Err("a part recorded twice".to_owned());
    }
    let scope = Scope::enter(
        &READING,
        Some(Reading {
            parts: kept,
            read: 0,
        }),
    );
    let value = rmp_serde::from_slice(bytes).map_err(|error| error.to_string());
    let reading = scope.leave();
    let value = value?;
    match reading {
        Some(reading) if reading.read == reading.parts.len() => Ok(value),
        _ => Err("a part the facts do not name".to_owned()),
    }
}

// A part's own bytes, encoded where no owner is taking parts, so a part
// inside it does not encode.
fn encode<T: Serialize>(value: &T) -> Option<Arc<[u8]>> {
    let scope = Scope::enter(&TAKING, None);
    let bytes = rmp_serde::to_vec(value).ok().map(Arc::from);
    drop(scope);
    bytes
}

fn decode<T: DeserializeOwned, E: de::Error>(bytes: Arc<[u8]>) -> Result<Sealed<T>, E> {
    let value = rmp_serde::from_slice(&bytes).map_err(E::custom)?;
    Ok(Sealed {
        value: Arc::new(value),
        bytes: Some(bytes),
    })
}

// The next shelf of the encoding in progress, with these chunks' bytes.
fn take<'a, E: serde::ser::Error>(
    chunks: impl Iterator<Item = (u32, Option<&'a Arc<[u8]>>)>,
) -> Result<u32, E> {
    TAKING.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(taking) = slot.as_mut() else {
            return Err(E::custom(
                "a sealed part encodes only inside its owner's facts",
            ));
        };
        let shelf = taking.next;
        taking.next += 1;
        for (chunk, bytes) in chunks {
            let bytes = bytes.ok_or_else(|| E::custom("a sealed part does not encode"))?;
            taking.parts.push(Part {
                key: key(shelf, chunk),
                bytes: Arc::clone(bytes),
            });
        }
        Ok(shelf)
    })
}

// Every chunk of a shelf the decoding in progress holds, in chunk order.
fn read<E: de::Error>(shelf: u32) -> Result<Vec<(u32, Arc<[u8]>)>, E> {
    READING.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(reading) = slot.as_mut() else {
            return Err(E::custom(
                "a sealed part decodes only inside its owner's facts",
            ));
        };
        let found: Vec<(u32, Arc<[u8]>)> = reading
            .parts
            .range(key(shelf, 0)..=key(shelf, u32::MAX))
            .map(|(key, bytes)| (*key as u32, Arc::clone(bytes)))
            .collect();
        reading.read += found.len();
        Ok(found)
    })
}

#[derive(Clone)]
pub struct Sealed<T> {
    value: Arc<T>,
    // `None` when the value would not encode: the owner's facts then fail
    // to encode too, and say so as unsupported.
    bytes: Option<Arc<[u8]>>,
}

impl<T: Serialize + Clone> Sealed<T> {
    pub fn new(value: T) -> Self {
        let bytes = encode(&value);
        Self {
            value: Arc::new(value),
            bytes,
        }
    }

    pub fn get(&self) -> &T {
        &self.value
    }

    // The one way to change the value: it is encoded again on the way out.
    pub fn change<R>(&mut self, change: impl FnOnce(&mut T) -> R) -> R {
        let out = change(Arc::make_mut(&mut self.value));
        self.bytes = encode(&self.value);
        out
    }

    // The value made `value`, encoded again only when that changes it.
    pub fn set(&mut self, value: T)
    where
        T: PartialEq,
    {
        if *self.value != value {
            *self = Self::new(value);
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
        let shelf = take::<S::Error>(std::iter::once((0, self.bytes.as_ref())))?;
        serializer.serialize_u32(shelf)
    }
}

impl<'de, T: DeserializeOwned> Deserialize<'de> for Sealed<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let shelf = deserializer.deserialize_u32(ShelfNumber)?;
        match read::<D::Error>(shelf)?.as_slice() {
            [(0, bytes)] => decode(Arc::clone(bytes)),
            _ => Err(de::Error::custom("a sealed part is not one part")),
        }
    }
}

// A value kept as chunks, each its own part: a value that grows a chunk at
// a time costs each change one chunk. The owner says which chunk an entry
// lies in, so a chunk keeps its number whatever grows around it.
#[derive(Clone)]
pub struct Shelf<T> {
    chunks: BTreeMap<u32, Sealed<T>>,
}

impl<T> Default for Shelf<T> {
    fn default() -> Self {
        Self {
            chunks: BTreeMap::new(),
        }
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
        out
    }

    // The shelf made these chunks, each encoded again only when it changed.
    pub fn set(&mut self, chunks: impl IntoIterator<Item = (u32, T)>)
    where
        T: PartialEq,
    {
        let mut next = BTreeMap::new();
        for (chunk, value) in chunks {
            let sealed = match self.chunks.remove(&chunk) {
                Some(mut sealed) => {
                    sealed.set(value);
                    sealed
                }
                None => Sealed::new(value),
            };
            next.insert(chunk, sealed);
        }
        self.chunks = next;
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
        let shelf = take::<S::Error>(
            self.chunks
                .iter()
                .map(|(chunk, sealed)| (*chunk, sealed.bytes.as_ref())),
        )?;
        serializer.serialize_u32(shelf)
    }
}

impl<'de, T: DeserializeOwned> Deserialize<'de> for Shelf<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let shelf = deserializer.deserialize_u32(ShelfNumber)?;
        let chunks = read::<D::Error>(shelf)?
            .into_iter()
            .map(|(chunk, bytes)| Ok((chunk, decode(bytes)?)))
            .collect::<Result<_, D::Error>>()?;
        Ok(Self { chunks })
    }
}

struct ShelfNumber;

impl Visitor<'_> for ShelfNumber {
    type Value = u32;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the number of a sealed part")
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<u32, E> {
        u32::try_from(value).map_err(|_| E::custom("a sealed part's number"))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde::{Deserialize, Serialize};

    use super::{Sealed, Shelf, decode_parts, encode_parts};

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
    fn a_part_inside_a_part_does_not_encode() {
        let inner = Sealed::new(vec![1u32]);
        let outer = Sealed::new(inner);
        assert_eq!(outer.bytes(), None);
        assert!(encode_parts(&outer).is_err());
    }
}
