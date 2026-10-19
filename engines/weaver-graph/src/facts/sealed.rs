// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A part of an object's facts that is encoded where it changes, not each
// time the facts are taken. A job checkpoints after every step, and an
// object's facts often hold a large part that settles early beside a small
// part that moves with every delivery; re-encoding the settled part each
// step makes every step cost the whole of it. The value is reached for
// change only through `change`, which encodes it again before returning,
// so the bytes always say what the value holds and nothing outside has a
// copy to keep fresh. Within its owner's facts it is those bytes, nested.
// The value and its bytes are shared, so taking the owner's facts copies
// neither.

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::{self, DeserializeOwned, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

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

fn encode<T: Serialize>(value: &T) -> Option<Arc<[u8]>> {
    rmp_serde::to_vec(value).ok().map(Arc::from)
}

impl<T> Serialize for Sealed<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.bytes {
            Some(bytes) => serializer.serialize_bytes(bytes),
            None => Err(serde::ser::Error::custom("a sealed part does not encode")),
        }
    }
}

impl<'de, T: DeserializeOwned> Deserialize<'de> for Sealed<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_bytes(Part(PhantomData))
    }
}

struct Part<T>(PhantomData<T>);

impl<T: DeserializeOwned> Visitor<'_> for Part<T> {
    type Value = Sealed<T>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the bytes of a sealed part")
    }

    fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
        let value = rmp_serde::from_slice(bytes).map_err(E::custom)?;
        Ok(Sealed {
            value: Arc::new(value),
            bytes: Some(Arc::from(bytes)),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde::{Deserialize, Serialize};

    use super::Sealed;

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
        };
        assert_eq!(SEEN.load(Ordering::SeqCst), 1, "sealed once");
        let first = rmp_serde::to_vec(&owner).unwrap();
        owner.moving += 1;
        let second = rmp_serde::to_vec(&owner).unwrap();
        assert_ne!(first, second);
        assert_eq!(
            SEEN.load(Ordering::SeqCst),
            1,
            "the owner's encodings reuse it"
        );
        owner.settled.change(|counted| counted.items.push(10));
        assert_eq!(SEEN.load(Ordering::SeqCst), 2, "a change seals it again");
        let restored: Owner = rmp_serde::from_slice(&rmp_serde::to_vec(&owner).unwrap()).unwrap();
        assert_eq!(restored.moving, 1);
        assert_eq!(restored.settled.get().items, [7, 8, 9, 10]);
        assert_eq!(restored.settled.bytes(), owner.settled.bytes());
    }
}
