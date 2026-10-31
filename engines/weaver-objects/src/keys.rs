// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Derived keys, once per process. A key derivation is deliberately slow, and
// every object of a job, and every job of a set offered the same password,
// derives the same key from the same tuple. The cache is keyed by a digest of
// the whole tuple, so this map never holds the password, and bounded by
// count, oldest first out. An entry is the derived key or the finding that
// the password is wrong for that tuple, so a wrong candidate is tried once.
//
// RAR header walks derive inside unrar-rs, which needs its own KdfCache: the
// process's one is kept here beside the keys, so a walk, a check and a key
// derive a tuple once wherever they run. That cache holds passwords and its
// `Debug` prints them, so it is never printed.
//
// Nothing here is ever printed: `Debug` says "redacted", and no key leaves
// through a log, a metric, an emit or a checkpoint.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use aws_lc_rs::digest::{Context, SHA256};
use unrar_rs::KdfCache;
use zeroize::Zeroizing;

// Entries kept. A job offers a handful of passwords; this is many jobs'.
pub const CAPACITY: usize = 256;

// The longest key an entry holds.
pub const MAX_KEY: usize = 64;

// What a key is derived from. The format names its own tag, so two formats
// with the same password and salt never share an entry.
#[derive(Clone, Copy)]
pub struct Tuple<'a> {
    pub format: &'static str,
    pub password: &'a str,
    pub salt: &'a [u8],
    pub params: &'a [u8],
}

impl fmt::Debug for Tuple<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Tuple(redacted)")
    }
}

impl Tuple<'_> {
    fn digest(&self) -> [u8; 32] {
        let mut context = Context::new(&SHA256);
        for field in [
            self.format.as_bytes(),
            self.password.as_bytes(),
            self.salt,
            self.params,
        ] {
            // Length-prefixed, so no two tuples run together into one.
            context.update(&(field.len() as u64).to_le_bytes());
            context.update(field);
        }
        let mut digest = [0; 32];
        digest.copy_from_slice(context.finish().as_ref());
        digest
    }
}

// What the cache knows about a tuple.
pub enum Derived {
    Key(Zeroizing<Vec<u8>>),
    Wrong,
}

impl fmt::Debug for Derived {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Derived::Key(_) => formatter.write_str("Key(redacted)"),
            Derived::Wrong => formatter.write_str("Wrong"),
        }
    }
}

impl Clone for Derived {
    fn clone(&self) -> Self {
        match self {
            Derived::Key(key) => Derived::Key(Zeroizing::new(key.to_vec())),
            Derived::Wrong => Derived::Wrong,
        }
    }
}

#[derive(Default)]
struct Inner {
    entries: HashMap<[u8; 32], Derived>,
    order: VecDeque<[u8; 32]>,
    derivations: u64,
}

pub struct Keys {
    inner: Mutex<Inner>,
    capacity: usize,
    kdf: Arc<KdfCache>,
}

impl fmt::Debug for Keys {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Keys(redacted)")
    }
}

impl Keys {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            capacity: capacity.max(1),
            kdf: Arc::new(KdfCache::new()),
        }
    }

    // unrar-rs's derivations, for the walks and checks that run inside it.
    pub fn kdf(&self) -> &Arc<KdfCache> {
        &self.kdf
    }

    // The process's one cache.
    pub fn global() -> Arc<Keys> {
        static GLOBAL: OnceLock<Arc<Keys>> = OnceLock::new();
        GLOBAL.get_or_init(|| Arc::new(Keys::new(CAPACITY))).clone()
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // A panic elsewhere while holding the lock leaves only whole entries
        // behind, so the cache is still sound.
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn get(&self, tuple: &Tuple<'_>) -> Option<Derived> {
        self.lock().entries.get(&tuple.digest()).cloned()
    }

    pub fn is_wrong(&self, tuple: &Tuple<'_>) -> bool {
        matches!(self.get(tuple), Some(Derived::Wrong))
    }

    // The key for a tuple, derived by `derive` only when the cache has no
    // entry for it. The derivation runs outside the lock: it is slow, and two
    // objects racing on one tuple derive the same key.
    pub fn derive<E>(
        &self,
        tuple: &Tuple<'_>,
        derive: impl FnOnce() -> Result<Zeroizing<Vec<u8>>, E>,
    ) -> Result<Derived, E> {
        let digest = tuple.digest();
        if let Some(found) = self.lock().entries.get(&digest) {
            return Ok(found.clone());
        }
        let key = derive()?;
        let mut inner = self.lock();
        inner.derivations += 1;
        if key.len() <= MAX_KEY {
            self.insert(
                &mut inner,
                digest,
                Derived::Key(Zeroizing::new(key.to_vec())),
            );
        }
        Ok(Derived::Key(key))
    }

    // The password is wrong for this tuple: say so to everyone who asks.
    pub fn mark_wrong(&self, tuple: &Tuple<'_>) {
        let digest = tuple.digest();
        let mut inner = self.lock();
        self.insert(&mut inner, digest, Derived::Wrong);
    }

    // How many derivations ran, for tests and metrics.
    pub fn derivations(&self) -> u64 {
        self.lock().derivations
    }

    fn insert(&self, inner: &mut Inner, digest: [u8; 32], entry: Derived) {
        if inner.entries.insert(digest, entry).is_none() {
            inner.order.push_back(digest);
        }
        while inner.order.len() > self.capacity {
            if let Some(oldest) = inner.order.pop_front() {
                inner.entries.remove(&oldest);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tuple(password: &str) -> Tuple<'_> {
        Tuple {
            format: "test",
            password,
            salt: b"salt",
            params: &[4],
        }
    }

    #[test]
    fn a_tuple_is_derived_once_and_a_wrong_one_is_remembered() {
        let keys = Keys::new(4);
        for _ in 0..3 {
            let derived = keys
                .derive(&tuple("right"), || Ok::<_, ()>(Zeroizing::new(vec![1; 32])))
                .unwrap();
            assert!(matches!(derived, Derived::Key(key) if key.as_slice() == [1; 32]));
        }
        assert_eq!(keys.derivations(), 1);
        keys.mark_wrong(&tuple("wrong"));
        assert!(keys.is_wrong(&tuple("wrong")));
        assert!(!keys.is_wrong(&tuple("right")));
        assert_eq!(format!("{keys:?}"), "Keys(redacted)");
        assert_eq!(
            format!("{:?}", keys.get(&tuple("right"))),
            "Some(Key(redacted))"
        );
    }

    #[test]
    fn the_oldest_entry_goes_first() {
        let keys = Keys::new(2);
        for password in ["a", "b", "c"] {
            keys.mark_wrong(&tuple(password));
        }
        assert!(keys.get(&tuple("a")).is_none());
        assert!(keys.is_wrong(&tuple("b")));
        assert!(keys.is_wrong(&tuple("c")));
    }
}
