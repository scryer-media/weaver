// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Bounded storage of proven keys and delegation facts, never answer records.

use std::collections::BTreeMap;

use crate::limits::{
    BOGUS_HOLD_SECONDS, MAX_BOGUS_HOLDS, MAX_CACHE_ENTRIES, MAX_TRUST_TTL_SECONDS,
};
use crate::rdata::{Dnskey, Ds};
use crate::{Bogus, InsecureReason, Name, RecordType, UnixSeconds};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Status {
    SecureDs(Vec<Ds>),
    InsecureCut(InsecureReason),
    NotACut,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CacheKind {
    Keys,
    Status,
    InsecureZone,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CacheValue {
    Keys(Vec<Dnskey>),
    Status { parent: Name, status: Status },
    InsecureZone(InsecureReason),
}

impl CacheValue {
    fn kind(&self) -> CacheKind {
        match self {
            Self::Keys(_) => CacheKind::Keys,
            Self::Status { .. } => CacheKind::Status,
            Self::InsecureZone(_) => CacheKind::InsecureZone,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CacheEntry {
    pub(crate) value: CacheValue,
    pub(crate) inserted: UnixSeconds,
    pub(crate) expires: UnixSeconds,
}

/// A clock that steps back before the insertion time ends the entry, so a
/// step back never adds to its lifetime.
fn live(inserted: UnixSeconds, expires: UnixSeconds, now: UnixSeconds) -> bool {
    inserted <= now && now < expires
}

impl CacheEntry {
    pub(crate) fn remaining(&self, now: UnixSeconds) -> u32 {
        u32::try_from(self.expires.saturating_sub(now)).unwrap_or(u32::MAX)
    }
}

#[derive(Default)]
pub(crate) struct TrustCache {
    entries: BTreeMap<(Name, CacheKind), CacheEntry>,
}

impl TrustCache {
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn get(&self, name: &Name, kind: CacheKind, now: UnixSeconds) -> Option<CacheEntry> {
        self.entries
            .get(&(name.clone(), kind))
            .filter(|entry| live(entry.inserted, entry.expires, now))
            .cloned()
    }

    pub(crate) fn insert(
        &mut self,
        name: Name,
        value: CacheValue,
        now: UnixSeconds,
        ttl_bound: u32,
    ) {
        let ttl = ttl_bound.min(MAX_TRUST_TTL_SECONDS);
        if ttl == 0 {
            return;
        }
        let key = (name, value.kind());
        if !self.entries.contains_key(&key) && self.entries.len() >= MAX_CACHE_ENTRIES {
            self.entries.retain(|_, entry| entry.expires > now);
            if self.entries.len() >= MAX_CACHE_ENTRIES {
                // Iteration is in canonical name/kind order, so min_by_key's
                // first equal expiry implements the specified tie break.
                if let Some(oldest) = self
                    .entries
                    .iter()
                    .min_by_key(|(_, entry)| entry.expires)
                    .map(|(key, _)| key.clone())
                {
                    self.entries.remove(&oldest);
                }
            }
        }
        self.entries.insert(
            key,
            CacheEntry {
                value,
                inserted: now,
                expires: now.saturating_add(u64::from(ttl)),
            },
        );
    }
}

/// Proven-bogus names and types per answer source, held briefly so retries
/// to the same source repeat no crypto. An entry is only ever returned as the
/// same failure, never as an answer. The bound is a total over all sources.
#[derive(Default)]
pub(crate) struct BogusHolds {
    // The failure, its insertion time and its expiry.
    entries: BTreeMap<(u64, Name, RecordType), (Bogus, UnixSeconds, UnixSeconds)>,
}

impl BogusHolds {
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn get(
        &self,
        source: u64,
        name: &Name,
        rtype: RecordType,
        now: UnixSeconds,
    ) -> Option<Bogus> {
        self.entries
            .get(&(source, name.clone(), rtype))
            .filter(|(_, inserted, expires)| live(*inserted, *expires, now))
            .map(|(bogus, _, _)| bogus.clone())
    }

    pub(crate) fn insert(
        &mut self,
        source: u64,
        name: Name,
        rtype: RecordType,
        bogus: Bogus,
        now: UnixSeconds,
    ) {
        let key = (source, name, rtype);
        if !self.entries.contains_key(&key) && self.entries.len() >= MAX_BOGUS_HOLDS {
            self.entries.retain(|_, (_, _, expires)| *expires > now);
            if self.entries.len() >= MAX_BOGUS_HOLDS
                && let Some(oldest) = self
                    .entries
                    .iter()
                    .min_by_key(|(_, (_, _, expires))| *expires)
                    .map(|(key, _)| key.clone())
            {
                self.entries.remove(&oldest);
            }
        }
        let expires = now.saturating_add(u64::from(BOGUS_HOLD_SECONDS));
        self.entries.insert(key, (bogus, now, expires));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(label: usize) -> Name {
        Name::from_text(&format!("n{label}.example.")).unwrap()
    }

    #[test]
    fn expiration_zero_ttl_and_cap_use_only_supplied_time() {
        let mut cache = TrustCache::default();
        cache.insert(
            name(0),
            CacheValue::InsecureZone(InsecureReason::UnsignedDelegation),
            100,
            0,
        );
        assert!(cache.entries.is_empty());
        cache.insert(
            name(0),
            CacheValue::InsecureZone(InsecureReason::UnsignedDelegation),
            100,
            u32::MAX,
        );
        let entry = cache.get(&name(0), CacheKind::InsecureZone, 101).unwrap();
        assert_eq!(entry.expires, 3700);
        assert_eq!(entry.remaining(101), 3599);
        assert!(cache.get(&name(0), CacheKind::InsecureZone, 3700).is_none());
    }

    #[test]
    fn kinds_coexist_and_replacement_does_not_evict() {
        let mut cache = TrustCache::default();
        cache.insert(name(0), CacheValue::Keys(vec![]), 0, 30);
        cache.insert(
            name(0),
            CacheValue::Status {
                parent: Name::root(),
                status: Status::NotACut,
            },
            0,
            30,
        );
        assert_eq!(cache.entries.len(), 2);
        cache.insert(name(0), CacheValue::Keys(vec![]), 5, 30);
        assert_eq!(cache.entries.len(), 2);
        assert_eq!(cache.get(&name(0), CacheKind::Keys, 5).unwrap().expires, 35);
        assert_eq!(
            cache.get(&name(0), CacheKind::Status, 5).unwrap().expires,
            30
        );
    }

    #[test]
    fn eviction_is_expiry_then_canonical_name_then_kind() {
        let mut cache = TrustCache::default();
        let apex = Name::from_text("example.").unwrap();
        cache.insert(apex.clone(), CacheValue::Keys(vec![]), 0, 30);
        cache.insert(
            apex.clone(),
            CacheValue::Status {
                parent: Name::root(),
                status: Status::NotACut,
            },
            0,
            30,
        );
        for index in 0..MAX_CACHE_ENTRIES - 2 {
            cache.insert(
                name(index),
                CacheValue::InsecureZone(InsecureReason::UnsignedDelegation),
                0,
                30,
            );
        }
        cache.insert(
            name(MAX_CACHE_ENTRIES),
            CacheValue::InsecureZone(InsecureReason::UnsignedDelegation),
            0,
            60,
        );
        assert_eq!(cache.entries.len(), MAX_CACHE_ENTRIES);
        assert!(cache.get(&apex, CacheKind::Keys, 0).is_none());
        assert!(cache.get(&apex, CacheKind::Status, 0).is_some());
        cache.insert(
            name(MAX_CACHE_ENTRIES + 1),
            CacheValue::InsecureZone(InsecureReason::UnsignedDelegation),
            30,
            60,
        );
        assert_eq!(cache.entries.len(), 2);
    }

    #[test]
    fn a_clock_stepped_back_before_insertion_ends_the_entry() {
        let mut cache = TrustCache::default();
        cache.insert(name(0), CacheValue::Keys(vec![]), 1_000, 300);
        assert!(cache.get(&name(0), CacheKind::Keys, 1_000).is_some());
        assert!(cache.get(&name(0), CacheKind::Keys, 1_299).is_some());
        // Stepped back by an hour, the entry would otherwise live 3,900 s.
        assert!(cache.get(&name(0), CacheKind::Keys, 999).is_none());
        assert!(cache.get(&name(0), CacheKind::Keys, 0).is_none());
        // A step forward again inside the original window still finds it.
        assert!(cache.get(&name(0), CacheKind::Keys, 1_100).is_some());
        assert!(cache.get(&name(0), CacheKind::Keys, 1_300).is_none());

        let mut holds = BogusHolds::default();
        holds.insert(0, name(0), RecordType::A, failure(0), 1_000);
        assert_eq!(
            holds.get(0, &name(0), RecordType::A, 1_059),
            Some(failure(0))
        );
        assert_eq!(holds.get(0, &name(0), RecordType::A, 999), None);
        assert_eq!(holds.get(0, &name(0), RecordType::A, 0), None);
    }

    fn failure(label: usize) -> Bogus {
        Bogus {
            reason: crate::BogusReason::SignatureInvalid,
            name: name(label),
            rtype: RecordType::A,
            extended_errors: Vec::new(),
        }
    }

    #[test]
    fn bogus_holds_last_sixty_seconds_per_name_and_type_and_stay_bounded() {
        let mut holds = BogusHolds::default();
        holds.insert(0, name(0), RecordType::A, failure(0), 100);
        assert_eq!(holds.get(0, &name(0), RecordType::A, 100), Some(failure(0)));
        assert_eq!(holds.get(0, &name(0), RecordType::A, 159), Some(failure(0)));
        assert_eq!(holds.get(0, &name(0), RecordType::A, 160), None);
        assert_eq!(holds.get(0, &name(0), RecordType::AAAA, 100), None);
        assert_eq!(holds.get(0, &name(1), RecordType::A, 100), None);
        // Refreshing an entry neither evicts nor duplicates it.
        holds.insert(0, name(0), RecordType::A, failure(0), 150);
        assert_eq!(holds.entries.len(), 1);
        assert_eq!(holds.get(0, &name(0), RecordType::A, 209), Some(failure(0)));
        // A full table drops expired entries first, then the soonest to expire.
        for index in 1..MAX_BOGUS_HOLDS {
            holds.insert(
                0,
                name(index),
                RecordType::A,
                failure(index),
                200 + index as u64,
            );
        }
        assert_eq!(holds.entries.len(), MAX_BOGUS_HOLDS);
        holds.insert(0, name(MAX_BOGUS_HOLDS), RecordType::A, failure(0), 211);
        assert_eq!(holds.entries.len(), MAX_BOGUS_HOLDS);
        assert_eq!(holds.get(0, &name(0), RecordType::A, 211), None);
        holds.insert(0, name(MAX_BOGUS_HOLDS + 1), RecordType::A, failure(0), 211);
        assert_eq!(holds.entries.len(), MAX_BOGUS_HOLDS);
        assert_eq!(holds.get(0, &name(1), RecordType::A, 211), None);
        assert!(holds.get(0, &name(2), RecordType::A, 211).is_some());
    }

    #[test]
    fn bogus_holds_are_per_source_and_bounded_in_total() {
        let mut holds = BogusHolds::default();
        holds.insert(1, name(0), RecordType::A, failure(0), 100);
        assert_eq!(holds.get(1, &name(0), RecordType::A, 100), Some(failure(0)));
        assert_eq!(holds.get(0, &name(0), RecordType::A, 100), None);
        assert_eq!(holds.get(2, &name(0), RecordType::A, 100), None);
        // Sources share the one bound.
        for index in 1..MAX_BOGUS_HOLDS {
            holds.insert(
                index as u64 + 1,
                name(0),
                RecordType::A,
                failure(0),
                100 + index as u64,
            );
        }
        assert_eq!(holds.entries.len(), MAX_BOGUS_HOLDS);
        holds.insert(0, name(0), RecordType::A, failure(0), 120);
        assert_eq!(holds.entries.len(), MAX_BOGUS_HOLDS);
        assert_eq!(holds.get(1, &name(0), RecordType::A, 120), None);
        assert!(holds.get(0, &name(0), RecordType::A, 120).is_some());
    }
}
