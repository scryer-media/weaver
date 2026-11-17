// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Pure denial proofs over records already authenticated under the supplied zone.

use std::collections::BTreeMap;

use crate::algorithm;
use crate::limits::{MAX_DENIAL_RRSETS, MAX_NSEC3_HASHES, MAX_NSEC3_ITERATIONS};
use crate::rdata::{Nsec, Nsec3, RdataKind, TypeBitmap, decode_base32hex};
use crate::rrset::WorkBudget;
use crate::{BogusReason, Name, RecordType};

#[derive(Clone, Debug)]
pub(crate) struct VerifiedDenial {
    pub(crate) owner: Name,
    pub(crate) kind: RdataKind,
    pub(crate) ttl_bound: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Proof {
    Proven,
    Insecure,
    /// Not proven, and NSEC3 records over the iteration limit were offered
    /// and left unread (RFC 9276). A failure, never a reason for insecure.
    IterationsLimited,
    NotProven,
}

impl Proof {
    /// The proof holds, either secure or as an insecure span.
    pub(crate) fn holds(self) -> bool {
        matches!(self, Self::Proven | Self::Insecure)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ProofResult {
    pub(crate) proof: Proof,
    /// Only records actually used by the proof contribute to this bound.
    pub(crate) ttl_bound: u32,
}

impl ProofResult {
    fn missing() -> Self {
        Self {
            proof: Proof::NotProven,
            ttl_bound: 0,
        }
    }
    fn proven(ttl_bound: u32) -> Self {
        Self {
            proof: Proof::Proven,
            ttl_bound,
        }
    }
    fn insecure(ttl_bound: u32) -> Self {
        Self {
            proof: Proof::Insecure,
            ttl_bound,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Status {
    InsecureCut,
    NotACut,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StatusResult {
    pub(crate) status: Status,
    pub(crate) ttl_bound: u32,
}

#[derive(Default)]
pub(crate) struct DenialContext {
    // Parameters are part of the key: separate replies can use different chains.
    hashes: BTreeMap<(Name, Vec<u8>, u16), [u8; 20]>,
}

impl DenialContext {
    fn hash(
        &mut self,
        name: &Name,
        salt: &[u8],
        iterations: u16,
        budget: &mut WorkBudget,
    ) -> Result<[u8; 20], BogusReason> {
        let key = (name.to_lowercase(), salt.to_vec(), iterations);
        if let Some(hash) = self.hashes.get(&key) {
            return Ok(*hash);
        }
        if budget.nsec3_hashes >= MAX_NSEC3_HASHES {
            return Err(BogusReason::WorkLimit);
        }
        budget.charge_crypto()?;
        budget.nsec3_hashes = budget
            .nsec3_hashes
            .checked_add(1)
            .ok_or(BogusReason::WorkLimit)?;
        let hash = algorithm::nsec3_hash(name, salt, iterations)
            .try_into()
            .map_err(|_| BogusReason::DenialInvalid)?;
        self.hashes.insert(key, hash);
        Ok(hash)
    }
}

fn delegation(types: &TypeBitmap) -> bool {
    types.has(RecordType::NS.0) && !types.has(RecordType::SOA.0)
}
fn absent(types: &TypeBitmap, rtype: RecordType) -> bool {
    !types.has(rtype.0) && !types.has(RecordType::CNAME.0)
}

fn wildcard_name(ce: &Name) -> Option<Name> {
    let mut wire = Vec::with_capacity(ce.as_wire().len().checked_add(2)?);
    wire.extend_from_slice(&[1, b'*']);
    wire.extend_from_slice(ce.as_wire());
    Name::from_wire(&wire).ok()
}

fn common_suffix(a: &Name, b: &Name) -> Name {
    let mut common = Name::root();
    for count in 1..=a.label_count().min(b.label_count()) {
        match (a.suffix(count), b.suffix(count)) {
            (Some(a), Some(b)) if a == b => common = a,
            _ => break,
        }
    }
    common
}

fn closest(name: &Name, owner: &Name, next: &Name) -> Name {
    let a = common_suffix(name, owner);
    let b = common_suffix(name, next);
    if a.label_count() >= b.label_count() {
        a
    } else {
        b
    }
}

fn nsec_usable(owner: &Name, nsec: &Nsec, name: &Name) -> bool {
    !(delegation(&nsec.types) && name.is_at_or_below(owner)
        || nsec.types.has(RecordType::DNAME.0) && name != owner && name.is_at_or_below(owner))
}

fn nsec_covers(owner: &Name, nsec: &Nsec, zone: &Name, name: &Name) -> bool {
    owner < name && (name < &nsec.next || &nsec.next == zone)
}

struct Nsecs<'a> {
    zone: &'a Name,
    by_owner: BTreeMap<&'a Name, (&'a Nsec, u32)>,
}

impl<'a> Nsecs<'a> {
    fn new(records: &'a [VerifiedDenial], zone: &'a Name) -> Self {
        let mut by_owner = BTreeMap::new();
        for record in records {
            if let RdataKind::Nsec(nsec) = &record.kind
                && record.owner.is_at_or_below(zone)
                && nsec.next.is_at_or_below(zone)
            {
                by_owner.insert(&record.owner, (nsec, record.ttl_bound));
            }
        }
        Self { zone, by_owner }
    }

    fn unsigned_ancestor(&self, name: &Name) -> Option<u32> {
        let mut ancestor = name.parent();
        while let Some(owner) = ancestor {
            if !owner.is_at_or_below(self.zone) {
                break;
            }
            if let Some((nsec, ttl)) = self.by_owner.get(&owner)
                && delegation(&nsec.types)
                && !nsec.types.has(RecordType::DS.0)
            {
                return Some(*ttl);
            }
            ancestor = owner.parent();
        }
        None
    }

    fn cover(&self, name: &Name) -> Option<(&'a Name, &'a Nsec, u32)> {
        let (&owner, &(nsec, ttl)) = self
            .by_owner
            .range::<Name, _>((std::ops::Bound::Unbounded, std::ops::Bound::Excluded(name)))
            .next_back()?;
        (nsec_usable(owner, nsec, name) && nsec_covers(owner, nsec, self.zone, name))
            .then_some((owner, nsec, ttl))
    }

    fn ent(&self, name: &Name) -> Option<u32> {
        // One bounded linear pass; callers never rescan this for each record.
        self.by_owner.iter().find_map(|(owner, (nsec, ttl))| {
            (**owner < *name
                && nsec.next != *name
                && nsec.next.is_at_or_below(name)
                && nsec_usable(owner, nsec, name))
            .then_some(*ttl)
        })
    }

    fn no_data(&self, name: &Name, rtype: RecordType) -> ProofResult {
        if let Some(ttl) = self.unsigned_ancestor(name) {
            return ProofResult::insecure(ttl);
        }
        if let Some((nsec, ttl)) = self.by_owner.get(name) {
            return if absent(&nsec.types, rtype) && nsec_usable(name, nsec, name) {
                ProofResult::proven(*ttl)
            } else {
                ProofResult::missing()
            };
        }
        if let Some(ttl) = self.ent(name) {
            return ProofResult::proven(ttl);
        }
        let Some((owner, nsec, ttl)) = self.cover(name) else {
            return ProofResult::missing();
        };
        if nsec.next.is_at_or_below(name) {
            return ProofResult::missing();
        }
        let Some(wildcard) = wildcard_name(&closest(name, owner, &nsec.next)) else {
            return ProofResult::missing();
        };
        let Some((wild, wild_ttl)) = self.by_owner.get(&wildcard) else {
            return ProofResult::missing();
        };
        if absent(&wild.types, rtype) && !delegation(&wild.types) {
            ProofResult::proven(ttl.min(*wild_ttl))
        } else {
            ProofResult::missing()
        }
    }

    fn name_error(&self, name: &Name) -> ProofResult {
        if let Some(ttl) = self.unsigned_ancestor(name) {
            return ProofResult::insecure(ttl);
        }
        if self.by_owner.contains_key(name) {
            return ProofResult::missing();
        }
        let Some((owner, nsec, ttl)) = self.cover(name) else {
            return ProofResult::missing();
        };
        if nsec.next.is_at_or_below(name) {
            return ProofResult::missing();
        }
        let Some(wildcard) = wildcard_name(&closest(name, owner, &nsec.next)) else {
            return ProofResult::proven(ttl);
        };
        self.cover(&wildcard)
            .map_or_else(ProofResult::missing, |(_, _, wild_ttl)| {
                ProofResult::proven(ttl.min(wild_ttl))
            })
    }

    fn wildcard(&self, name: &Name, ce: &Name) -> ProofResult {
        if let Some(ttl) = self.unsigned_ancestor(name) {
            return ProofResult::insecure(ttl);
        }
        let Some((owner, nsec, ttl)) = self.cover(name) else {
            return ProofResult::missing();
        };
        if !nsec.next.is_at_or_below(name) && closest(name, owner, &nsec.next) == *ce {
            ProofResult::proven(ttl)
        } else {
            ProofResult::missing()
        }
    }
}

struct Nsec3Record<'a> {
    nsec: &'a Nsec3,
    ttl: u32,
}
struct Nsec3s<'a> {
    salt: &'a [u8],
    iterations: u16,
    by_hash: BTreeMap<[u8; 20], Nsec3Record<'a>>,
}

struct Prepared<'a> {
    chains: Vec<Nsec3s<'a>>,
    // Records over the iteration limit were offered and left unhashed.
    limited: bool,
}

impl Prepared<'_> {
    fn proof(
        &self,
        mut evaluate: impl FnMut(&Nsec3s<'_>) -> Result<ProofResult, BogusReason>,
    ) -> Result<ProofResult, BogusReason> {
        for chain in &self.chains {
            let result = evaluate(chain)?;
            if result.proof != Proof::NotProven {
                return Ok(result);
            }
        }
        Ok(self.unproven())
    }

    /// Records over the iteration limit never prove anything, and never
    /// make an answer insecure: any record of the zone's can be replayed
    /// beside any name. They only name the failure.
    fn unproven(&self) -> ProofResult {
        ProofResult {
            proof: if self.limited {
                Proof::IterationsLimited
            } else {
                Proof::NotProven
            },
            ttl_bound: 0,
        }
    }
}

impl<'a> Nsec3s<'a> {
    fn prepare(records: &'a [VerifiedDenial], zone: &Name) -> Prepared<'a> {
        // A rollover can publish multiple chains. Never join proof fragments
        // with different parameters, and keep evaluation order independent of
        // the order of authenticated records in the response.
        let mut chains = BTreeMap::<(u8, u16, &'a [u8]), Self>::new();
        let mut limited = false;
        for record in records {
            let RdataKind::Nsec3(nsec) = &record.kind else {
                continue;
            };
            if nsec.hash_algorithm != 1
                || nsec.flags & !1 != 0
                || record.owner.parent().as_ref() != Some(zone)
                || nsec.next_hashed_owner.len() != 20
            {
                continue;
            }
            let Some((&length, rest)) = record.owner.as_wire().split_first() else {
                continue;
            };
            if length != 32 {
                continue;
            }
            let Some(label) = rest.get(..32) else {
                continue;
            };
            let Some(hash) = decode_base32hex(label)
                .ok()
                .and_then(|hash| <[u8; 20]>::try_from(hash).ok())
            else {
                continue;
            };
            if nsec.iterations > MAX_NSEC3_ITERATIONS {
                limited = true;
                continue;
            }
            let chain = chains
                .entry((nsec.hash_algorithm, nsec.iterations, nsec.salt.as_slice()))
                .or_insert_with(|| Self {
                    salt: &nsec.salt,
                    iterations: nsec.iterations,
                    by_hash: BTreeMap::new(),
                });
            chain.by_hash.insert(
                hash,
                Nsec3Record {
                    nsec,
                    ttl: record.ttl_bound,
                },
            );
        }
        Prepared {
            chains: chains.into_values().collect(),
            limited,
        }
    }

    fn hash(
        &self,
        name: &Name,
        ctx: &mut DenialContext,
        budget: &mut WorkBudget,
    ) -> Result<[u8; 20], BogusReason> {
        ctx.hash(name, self.salt, self.iterations, budget)
    }
    fn matching(
        &self,
        name: &Name,
        ctx: &mut DenialContext,
        budget: &mut WorkBudget,
    ) -> Result<Option<&Nsec3Record<'a>>, BogusReason> {
        Ok(self.by_hash.get(&self.hash(name, ctx, budget)?))
    }

    fn covering_hash(&self, hash: &[u8; 20]) -> Option<&Nsec3Record<'a>> {
        if self.by_hash.contains_key(hash) {
            return None;
        }
        let (owner, record) = self
            .by_hash
            .range::<[u8; 20], _>((std::ops::Bound::Unbounded, std::ops::Bound::Excluded(hash)))
            .next_back()
            .or_else(|| self.by_hash.last_key_value())?;
        let next = record.nsec.next_hashed_owner.as_slice();
        let covers = if next <= owner.as_slice() {
            hash.as_slice() > owner.as_slice() || hash.as_slice() < next
        } else {
            hash.as_slice() > owner.as_slice() && hash.as_slice() < next
        };
        covers.then_some(record)
    }

    fn cover(
        &self,
        name: &Name,
        ctx: &mut DenialContext,
        budget: &mut WorkBudget,
    ) -> Result<Option<&Nsec3Record<'a>>, BogusReason> {
        Ok(self.covering_hash(&self.hash(name, ctx, budget)?))
    }

    fn closest(
        &self,
        name: &Name,
        zone: &Name,
        ctx: &mut DenialContext,
        budget: &mut WorkBudget,
    ) -> Result<Closest, BogusReason> {
        let mut candidate = Some(name.clone());
        while let Some(ce) = candidate {
            if !ce.is_at_or_below(zone) {
                break;
            }
            if let Some(record) = self.matching(&ce, ctx, budget)? {
                if ce == *name {
                    return Ok(Closest::Missing);
                }
                let Some(count) = ce.label_count().checked_add(1) else {
                    return Ok(Closest::Missing);
                };
                let Some(next) = name.suffix(count) else {
                    return Ok(Closest::Missing);
                };
                let Some(cover) = self.cover(&next, ctx, budget)? else {
                    return Ok(Closest::Missing);
                };
                let ttl = record.ttl.min(cover.ttl);
                if record.nsec.types.has(RecordType::DNAME.0) {
                    return Ok(Closest::Missing);
                }
                if delegation(&record.nsec.types) {
                    return Ok(if record.nsec.types.has(RecordType::DS.0) {
                        Closest::Missing
                    } else {
                        Closest::Insecure(ttl)
                    });
                }
                return Ok(Closest::Found {
                    ce,
                    ttl,
                    opt_out: cover.nsec.flags & 1 != 0,
                });
            }
            candidate = ce.parent();
        }
        Ok(Closest::Missing)
    }

    fn proof(
        &self,
        name: &Name,
        zone: &Name,
        rtype: Option<RecordType>,
        ctx: &mut DenialContext,
        budget: &mut WorkBudget,
    ) -> Result<ProofResult, BogusReason> {
        if let Some(record) = self.matching(name, ctx, budget)? {
            return Ok(
                if rtype.is_some_and(|rtype| {
                    absent(&record.nsec.types, rtype) && !delegation(&record.nsec.types)
                }) {
                    ProofResult::proven(record.ttl)
                } else {
                    ProofResult::missing()
                },
            );
        }
        let (ce, ttl, opt_out) = match self.closest(name, zone, ctx, budget)? {
            Closest::Missing => return Ok(ProofResult::missing()),
            Closest::Insecure(ttl) => return Ok(ProofResult::insecure(ttl)),
            Closest::Found { ce, ttl, opt_out } => (ce, ttl, opt_out),
        };
        let Some(wildcard) = wildcard_name(&ce) else {
            return Ok(ProofResult::missing());
        };
        let wild = if let Some(rtype) = rtype {
            self.matching(&wildcard, ctx, budget)?.filter(|record| {
                absent(&record.nsec.types, rtype) && !delegation(&record.nsec.types)
            })
        } else {
            self.cover(&wildcard, ctx, budget)?
        };
        Ok(wild.map_or_else(ProofResult::missing, |wild| {
            if opt_out {
                ProofResult::insecure(ttl.min(wild.ttl))
            } else {
                ProofResult::proven(ttl.min(wild.ttl))
            }
        }))
    }
}

enum Closest {
    Missing,
    Insecure(u32),
    Found { ce: Name, ttl: u32, opt_out: bool },
}

fn check_inputs(records: &[VerifiedDenial], zone: &Name, name: &Name) -> Result<bool, BogusReason> {
    if records.len() > MAX_DENIAL_RRSETS {
        return Err(BogusReason::WorkLimit);
    }
    Ok(name.is_at_or_below(zone))
}

pub(crate) fn no_data(
    records: &[VerifiedDenial],
    zone: &Name,
    name: &Name,
    rtype: RecordType,
    ctx: &mut DenialContext,
    budget: &mut WorkBudget,
) -> Result<ProofResult, BogusReason> {
    if !check_inputs(records, zone, name)? {
        return Ok(ProofResult::missing());
    }
    let result = Nsecs::new(records, zone).no_data(name, rtype);
    if result.proof != Proof::NotProven {
        return Ok(result);
    }
    Nsec3s::prepare(records, zone).proof(|chain| chain.proof(name, zone, Some(rtype), ctx, budget))
}

pub(crate) fn name_error(
    records: &[VerifiedDenial],
    zone: &Name,
    name: &Name,
    ctx: &mut DenialContext,
    budget: &mut WorkBudget,
) -> Result<ProofResult, BogusReason> {
    if !check_inputs(records, zone, name)? {
        return Ok(ProofResult::missing());
    }
    let result = Nsecs::new(records, zone).name_error(name);
    if result.proof != Proof::NotProven {
        return Ok(result);
    }
    Nsec3s::prepare(records, zone).proof(|chain| chain.proof(name, zone, None, ctx, budget))
}

pub(crate) fn wildcard(
    records: &[VerifiedDenial],
    zone: &Name,
    name: &Name,
    ce: &Name,
    ctx: &mut DenialContext,
    budget: &mut WorkBudget,
) -> Result<ProofResult, BogusReason> {
    if !check_inputs(records, zone, name)?
        || ce == name
        || !name.is_at_or_below(ce)
        || !ce.is_at_or_below(zone)
    {
        return Ok(ProofResult::missing());
    }
    let result = Nsecs::new(records, zone).wildcard(name, ce);
    if result.proof != Proof::NotProven {
        return Ok(result);
    }
    let Some(next) = ce
        .label_count()
        .checked_add(1)
        .and_then(|count| name.suffix(count))
    else {
        return Ok(ProofResult::missing());
    };
    Nsec3s::prepare(records, zone).proof(|chain| {
        Ok(chain
            .cover(&next, ctx, budget)?
            .map_or_else(ProofResult::missing, |record| {
                if record.nsec.flags & 1 != 0 {
                    ProofResult::insecure(record.ttl)
                } else {
                    ProofResult::proven(record.ttl)
                }
            }))
    })
}

pub(crate) fn delegation_status(
    records: &[VerifiedDenial],
    zone: &Name,
    name: &Name,
    ctx: &mut DenialContext,
    budget: &mut WorkBudget,
) -> Result<StatusResult, BogusReason> {
    if !check_inputs(records, zone, name)? {
        return Err(BogusReason::DelegationUnproven);
    }
    let nsecs = Nsecs::new(records, zone);
    if let Some((record, ttl)) = nsecs.by_owner.get(name) {
        return exact_status(&record.types, name, *ttl);
    }
    if let Some(ttl) = nsecs.ent(name) {
        return Ok(StatusResult {
            status: Status::NotACut,
            ttl_bound: ttl,
        });
    }
    for result in [nsecs.name_error(name), nsecs.no_data(name, RecordType::DS)] {
        if result.proof != Proof::NotProven {
            return Ok(status_from_proof(result));
        }
    }
    let prepared = Nsec3s::prepare(records, zone);
    for chain in &prepared.chains {
        if let Some(record) = chain.matching(name, ctx, budget)? {
            if let Ok(status) = exact_status(&record.nsec.types, name, record.ttl) {
                return Ok(status);
            }
            continue;
        }
        match chain.closest(name, zone, ctx, budget)? {
            Closest::Missing => continue,
            Closest::Insecure(ttl) => {
                return Ok(StatusResult {
                    status: Status::InsecureCut,
                    ttl_bound: ttl,
                });
            }
            Closest::Found { ttl, opt_out, .. } => {
                return Ok(StatusResult {
                    status: if opt_out {
                        Status::InsecureCut
                    } else {
                        Status::NotACut
                    },
                    ttl_bound: ttl,
                });
            }
        }
    }
    Err(if prepared.limited {
        BogusReason::Nsec3Iterations
    } else {
        BogusReason::DelegationUnproven
    })
}

fn status_from_proof(proof: ProofResult) -> StatusResult {
    StatusResult {
        status: if proof.proof == Proof::Insecure {
            Status::InsecureCut
        } else {
            Status::NotACut
        },
        ttl_bound: proof.ttl_bound,
    }
}

fn exact_status(
    types: &TypeBitmap,
    name: &Name,
    ttl_bound: u32,
) -> Result<StatusResult, BogusReason> {
    if types.has(RecordType::DS.0) || types.has(RecordType::SOA.0) && *name != Name::root() {
        return Err(BogusReason::DelegationUnproven);
    }
    Ok(StatusResult {
        status: if types.has(RecordType::NS.0) {
            Status::InsecureCut
        } else {
            Status::NotACut
        },
        ttl_bound,
    })
}

#[cfg(test)]
#[path = "denial_tests.rs"]
mod tests;
