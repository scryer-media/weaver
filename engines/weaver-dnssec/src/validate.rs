// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Restartable, memoized DNSSEC evaluation. The caller owns transport and time.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use crate::anchor::{private_name, root_anchors, validate_anchors};
use crate::cache::{BogusHolds, CacheKind, CacheValue, Status, TrustCache};
use crate::denial::{self, DenialContext, Proof, VerifiedDenial};
use crate::limits::{MAX_ALIAS_LINKS, MAX_DENIAL_RRSETS, MAX_EXTENDED_ERRORS, MAX_QUERIES};
use crate::rdata::{Dnskey, Ds, RdataKind};
use crate::rrset::{SignatureLimit, Verified, WorkBudget, plausible_signer, verify_rrset};
use crate::wire::{Message, ParseError, Rrset};
use crate::{
    AnchorError, Answer, Bogus, BogusReason, InsecureReason, Name, Outcome, Policy, Query,
    Question, RecordType, Step, SupplyError, TrustAnchor, Unanswered, UnansweredCause, UnixSeconds,
    Verdict, keys,
};

/// Shared trust configuration and bounded caches. Performs no I/O.
pub struct Validator {
    anchors: Vec<TrustAnchor>,
    policy: Policy,
    cache: Mutex<TrustCache>,
    holds: Mutex<BogusHolds>,
}

impl Validator {
    /// Use the built-in IANA root trust anchors and the default policy.
    pub fn with_root_anchors() -> Self {
        Self {
            anchors: root_anchors(),
            policy: Policy::default(),
            cache: Mutex::default(),
            holds: Mutex::default(),
        }
    }

    /// Use caller-supplied trust anchors in place of the built-in root anchors.
    pub fn new(anchors: Vec<TrustAnchor>, policy: Policy) -> Result<Self, AnchorError> {
        validate_anchors(&anchors, &policy)?;
        Ok(Self {
            anchors,
            policy,
            cache: Mutex::default(),
            holds: Mutex::default(),
        })
    }

    /// Start one assessment at the caller's time, from source 0.
    pub fn begin(&self, question: Question, now: UnixSeconds) -> Validation<'_> {
        self.begin_from(0, question, now)
    }

    /// Start one assessment of answers from the caller's `source`. A name and
    /// type proven bogus from the same source in the last minute is done at
    /// once with the same failure; another source is asked afresh. Proven
    /// keys and cuts are shared across sources.
    pub fn begin_from(&self, source: u64, question: Question, now: UnixSeconds) -> Validation<'_> {
        let now = now.min(1_u64 << 62);
        let held = self
            .holds
            .lock()
            .ok()
            .and_then(|holds| holds.get(source, question.name(), question.rtype(), now));
        Validation {
            validator: self,
            source,
            question,
            now,
            replies: BTreeMap::new(),
            outstanding: BTreeSet::new(),
            blocked: None,
            hints: Vec::new(),
            extended_errors: Vec::new(),
            server_error: false,
            final_verdict: held.map(Verdict::Bogus),
            budget: WorkBudget::default(),
            verifications: BTreeMap::new(),
            rrset_checks: BTreeMap::new(),
            proven_keys: BTreeMap::new(),
            next_keyset_id: 0,
            statuses: BTreeMap::new(),
            insecure_cuts: BTreeMap::new(),
            insecure_zones: BTreeMap::new(),
            denial: DenialContext::default(),
        }
    }

    /// The closest configured anchor zone at or above `target`.
    fn anchor_zone(&self, target: &Name) -> Option<&Name> {
        self.anchors
            .iter()
            .filter(|anchor| target.is_at_or_below(&anchor.zone))
            .max_by_key(|anchor| anchor.zone.label_count())
            .map(|anchor| &anchor.zone)
    }

    /// A built-in negative trust anchor covers the name, and no configured
    /// anchor inside that private zone overrides it.
    fn private(&self, name: &Name) -> bool {
        private_name(name) && !self.anchor_zone(name).is_some_and(private_name)
    }
}

type RrsetIdentity = (Query, u8, Name, RecordType);
type VerifyIdentity = (RrsetIdentity, Name, usize);

// Every immutable snapshot receives an ID that is never reused within this
// Validation. It permanently identifies the exact flags, protocol, algorithm,
// public key bytes and order, without cloning a key collection per RRset.
struct ProvenKeys {
    id: usize,
    values: Vec<Dnskey>,
}

struct ParsedReply {
    message: Message,
    answers: BTreeMap<(Name, RecordType), usize>,
    denials: Vec<usize>,
}

impl ParsedReply {
    fn new(message: Message) -> Self {
        let answers = message
            .answer
            .iter()
            .enumerate()
            .map(|(i, set)| ((set.owner.clone(), set.rtype), i))
            .collect();
        let denials = message
            .authority
            .iter()
            .enumerate()
            .filter(|(_, set)| matches!(set.rtype, RecordType::NSEC | RecordType::NSEC3))
            .take(MAX_DENIAL_RRSETS)
            .map(|(i, _)| i)
            .collect();
        Self {
            message,
            answers,
            denials,
        }
    }

    fn answer(&self, name: &Name, rtype: RecordType) -> Option<&Rrset> {
        self.answers
            .get(&(name.clone(), rtype))
            .and_then(|i| self.message.answer.get(*i))
    }

    fn dname(&self, name: &Name) -> Option<&Rrset> {
        let mut ancestor = name.parent();
        while let Some(owner) = ancestor {
            if let Some(set) = self.answer(&owner, RecordType::DNAME) {
                return Some(set);
            }
            ancestor = owner.parent();
        }
        None
    }

    /// Not one signature in the answer or authority section, whether or
    /// not an OPT record came back: the server dropped DNSSEC.
    fn stripped(&self) -> bool {
        self.message.signatures == 0
    }

    /// The reply ends without data, denial or SOA: the resolver did not
    /// follow the alias to its end.
    fn unfinished(&self) -> bool {
        !self.message.authority.iter().any(|set| {
            matches!(
                set.rtype,
                RecordType::SOA | RecordType::NSEC | RecordType::NSEC3
            )
        })
    }
}

enum ReplyState {
    Parsed(Arc<ParsedReply>),
    DsServfail,
    Terminal(Verdict),
}
enum Flow {
    Need,
    Terminal(Verdict),
}
type Eval<T> = Result<T, Flow>;

#[derive(Clone)]
enum Zone {
    Secure {
        name: Name,
        keys: Arc<ProvenKeys>,
        exact: bool,
    },
    Insecure(InsecureReason),
}

struct Judgment {
    insecure: Option<InsecureReason>,
    ttl: u32,
    wildcard: Option<Name>,
    zone: Option<(Name, Arc<ProvenKeys>)>,
}

impl Judgment {
    fn insecure(reason: InsecureReason, ttl: u32) -> Self {
        Self {
            insecure: Some(reason),
            ttl,
            wildcard: None,
            zone: None,
        }
    }
}

/// Signature, key and denial failures that a reply without DNSSEC records
/// explains better.
fn missing_dnssec(reason: BogusReason) -> bool {
    matches!(
        reason,
        BogusReason::SignaturesMissing
            | BogusReason::KeysNotProven
            | BogusReason::DelegationUnproven
            | BogusReason::DenialMissing
            | BogusReason::DenialInvalid
            | BogusReason::WildcardUnproven
    )
}

/// Why a denial proof that does not hold failed.
fn unproven_reason(proof: Proof, otherwise: BogusReason) -> BogusReason {
    if proof == Proof::IterationsLimited {
        BogusReason::Nsec3Iterations
    } else {
        otherwise
    }
}

/// One assessment at one fixed caller-supplied time.
pub struct Validation<'v> {
    validator: &'v Validator,
    // Whose answers these are, for the bogus hold.
    source: u64,
    question: Question,
    now: UnixSeconds,
    replies: BTreeMap<Query, ReplyState>,
    // Handed out and not yet supplied or failed.
    outstanding: BTreeSet<Query>,
    // The query the last evaluation stopped on.
    blocked: Option<Query>,
    // Queries the last evaluation will likely need next, for also_needed.
    hints: Vec<Query>,
    extended_errors: Vec<u16>,
    // A server error fed the proof, so a retry elsewhere may succeed.
    server_error: bool,
    final_verdict: Option<Verdict>,
    budget: WorkBudget,
    verifications: BTreeMap<VerifyIdentity, Result<Verified, BogusReason>>,
    rrset_checks: BTreeMap<RrsetIdentity, usize>,
    proven_keys: BTreeMap<Name, Arc<ProvenKeys>>,
    next_keyset_id: usize,
    statuses: BTreeMap<(Name, Name), (Status, u32)>,
    insecure_cuts: BTreeMap<Name, InsecureReason>,
    insecure_zones: BTreeMap<Name, InsecureReason>,
    denial: DenialContext,
}

impl Validation<'_> {
    /// The query the assessment waits on, or the verdict once complete.
    pub fn step(&mut self) -> Step {
        if let Some(verdict) = &self.final_verdict {
            return Step::Done(verdict.clone());
        }
        if let Some(query) = self
            .blocked
            .as_ref()
            .filter(|query| self.outstanding.contains(*query))
        {
            return Step::Need(query.clone());
        }
        self.blocked = None;
        self.hints.clear();
        let result = self.evaluate();
        match result {
            Ok(verdict) | Err(Flow::Terminal(verdict)) => Step::Done(self.finish(verdict)),
            Err(Flow::Need) => {
                // reply() installs the query before unwinding evaluation.
                match &self.blocked {
                    Some(query) => Step::Need(query.clone()),
                    None => {
                        let verdict = self.bogus(
                            BogusReason::WorkLimit,
                            self.question.name(),
                            self.question.rtype(),
                        );
                        Step::Done(self.finish(verdict))
                    }
                }
            }
        }
    }

    /// After `step` returns `Need`, further queries this assessment expects
    /// to need. Each counts toward the query limit; send them with the
    /// stepped query in parallel and supply the replies in any order.
    pub fn also_needed(&mut self) -> Vec<Query> {
        let mut issued = Vec::new();
        if self.final_verdict.is_some() {
            return issued;
        }
        for query in std::mem::take(&mut self.hints) {
            if self.replies.contains_key(&query) || self.outstanding.contains(&query) {
                continue;
            }
            if self.replies.len() + self.outstanding.len() >= MAX_QUERIES {
                break;
            }
            self.outstanding.insert(query.clone());
            issued.push(query);
        }
        issued
    }

    /// Supply the reply to any outstanding query.
    pub fn supply(&mut self, query: &Query, reply: &[u8]) -> Result<(), SupplyError> {
        if self.final_verdict.is_some() || !self.outstanding.contains(query) {
            return Err(SupplyError::UnexpectedQuery);
        }
        let state = match Message::parse_any(reply, query) {
            Ok(message) => {
                for code in &message.extended_errors {
                    if self.extended_errors.len() < MAX_EXTENDED_ERRORS
                        && !self.extended_errors.contains(code)
                    {
                        self.extended_errors.push(*code);
                    }
                }
                match message.rcode {
                    0 | 3 => ReplyState::Parsed(Arc::new(ParsedReply::new(message))),
                    2 if query.rtype == RecordType::DS => {
                        self.server_error = true;
                        ReplyState::DsServfail
                    }
                    rcode => {
                        ReplyState::Terminal(self.unanswered(query, UnansweredCause::Rcode(rcode)))
                    }
                }
            }
            Err(ParseError::Truncated) => {
                ReplyState::Terminal(self.unanswered(query, UnansweredCause::Truncated))
            }
            Err(_) => ReplyState::Terminal(self.bogus(
                BogusReason::MalformedReply,
                &query.name,
                query.rtype,
            )),
        };
        self.outstanding.remove(query);
        self.replies.insert(query.clone(), state);
        Ok(())
    }

    /// Report that an outstanding query's fetch failed.
    pub fn failed(&mut self, query: &Query) -> Result<(), SupplyError> {
        if self.final_verdict.is_some() || !self.outstanding.contains(query) {
            return Err(SupplyError::UnexpectedQuery);
        }
        self.outstanding.remove(query);
        let state = ReplyState::Terminal(self.unanswered(query, UnansweredCause::FetchFailed));
        self.replies.insert(query.clone(), state);
        Ok(())
    }

    #[cfg(feature = "test-support")]
    pub(crate) fn signature_checks(&self) -> usize {
        self.budget.signatures
    }

    fn finish(&mut self, mut verdict: Verdict) -> Verdict {
        match &mut verdict {
            Verdict::Bogus(bogus) => {
                bogus.extended_errors.clone_from(&self.extended_errors);
                // Transport-shaped failures may clear on a retry elsewhere,
                // so only proof failures are held.
                if !self.server_error
                    && !matches!(
                        bogus.reason,
                        BogusReason::MalformedReply | BogusReason::NoDnssecRecords
                    )
                    && let Ok(mut holds) = self.validator.holds.lock()
                {
                    holds.insert(
                        self.source,
                        self.question.name().clone(),
                        self.question.rtype(),
                        bogus.clone(),
                        self.now,
                    );
                }
            }
            Verdict::Unanswered(unanswered) => {
                unanswered.extended_errors.clone_from(&self.extended_errors);
            }
            Verdict::Secure(_) | Verdict::Insecure(_) => {}
        }
        self.outstanding.clear();
        self.blocked = None;
        self.hints.clear();
        self.final_verdict = Some(verdict.clone());
        verdict
    }

    fn unanswered(&self, query: &Query, cause: UnansweredCause) -> Verdict {
        Verdict::Unanswered(Unanswered {
            query: query.clone(),
            cause,
            extended_errors: Vec::new(),
        })
    }

    fn bogus(&self, reason: BogusReason, name: &Name, rtype: RecordType) -> Verdict {
        Verdict::Bogus(Bogus {
            reason,
            name: name.clone(),
            rtype,
            extended_errors: Vec::new(),
        })
    }

    fn error(&self, reason: BogusReason, name: &Name, rtype: RecordType) -> Flow {
        Flow::Terminal(self.bogus(reason, name, rtype))
    }

    /// A failure proven from the reply to `source`. Missing signatures or
    /// denials in a reply without DNSSEC records name the server instead.
    fn fail(&self, source: &Query, reason: BogusReason, name: &Name, rtype: RecordType) -> Flow {
        let stripped = matches!(
            self.replies.get(source),
            Some(ReplyState::Parsed(reply)) if reply.stripped()
        );
        let reason = if stripped && missing_dnssec(reason) {
            BogusReason::NoDnssecRecords
        } else {
            reason
        };
        self.error(reason, name, rtype)
    }

    fn reply(&mut self, query: &Query) -> Eval<Option<Arc<ParsedReply>>> {
        if let Some(state) = self.replies.get(query) {
            return match state {
                ReplyState::Parsed(message) => Ok(Some(Arc::clone(message))),
                ReplyState::DsServfail => Ok(None),
                ReplyState::Terminal(verdict) => Err(Flow::Terminal(verdict.clone())),
            };
        }
        if !self.outstanding.contains(query) {
            // Replies and queries in flight together share one total limit.
            if self.replies.len() + self.outstanding.len() >= MAX_QUERIES {
                return Err(self.error(BogusReason::WorkLimit, &query.name, query.rtype));
            }
            self.outstanding.insert(query.clone());
        }
        self.blocked = Some(query.clone());
        Err(Flow::Need)
    }

    fn hint(&mut self, name: &Name, rtype: RecordType) {
        let query = Query {
            name: name.clone(),
            rtype,
        };
        if self.hints.len() < MAX_QUERIES
            && !self.replies.contains_key(&query)
            && !self.hints.contains(&query)
        {
            self.hints.push(query);
        }
    }

    fn cache(&self, name: &Name, kind: CacheKind) -> Option<crate::cache::CacheEntry> {
        self.validator.cache.lock().ok()?.get(name, kind, self.now)
    }

    fn remember(&self, name: &Name, value: CacheValue, ttl: u32) {
        if let Ok(mut cache) = self.validator.cache.lock() {
            cache.insert(name.clone(), value, self.now, ttl);
        }
    }

    fn verify(
        &mut self,
        query: &Query,
        section: u8,
        set: &Rrset,
        zone: &Name,
        keys: &ProvenKeys,
    ) -> Result<Verified, BogusReason> {
        let rrset = (query.clone(), section, set.owner.clone(), set.rtype);
        let identity = (rrset.clone(), zone.clone(), keys.id);
        if let Some(result) = self.verifications.get(&identity) {
            return result.clone();
        }
        let checks = self.rrset_checks.entry(rrset).or_default();
        let result = verify_rrset(
            set,
            zone,
            &keys.values,
            self.now,
            self.validator.policy,
            &mut self.budget,
            SignatureLimit::CachedKeys(checks),
        );
        self.verifications.insert(identity, result.clone());
        result
    }

    fn intern_keys(&mut self, zone: &Name, values: Vec<Dnskey>) -> Arc<ProvenKeys> {
        // The cache, name length, alias and query budgets bound the number
        // of snapshots, so this monotonic ID cannot wrap or be reused.
        let snapshot = Arc::new(ProvenKeys {
            id: self.next_keyset_id,
            values,
        });
        self.next_keyset_id += 1;
        self.proven_keys.insert(zone.clone(), Arc::clone(&snapshot));
        snapshot
    }

    fn insecure_zone(&mut self, zone: &Name, reason: InsecureReason, ttl: u32) {
        self.insecure_zones.insert(zone.clone(), reason);
        self.remember(zone, CacheValue::InsecureZone(reason), ttl);
    }

    /// Prove a zone's keys from its DS set. `anchor` marks DS records taken
    /// from configured trust anchors.
    fn keys_for(
        &mut self,
        zone: &Name,
        ds: &[Ds],
        ds_ttl: u32,
        anchor: bool,
    ) -> Eval<Result<Arc<ProvenKeys>, InsecureReason>> {
        if let Some(keys) = self.proven_keys.get(zone) {
            return Ok(Ok(keys.clone()));
        }
        let ds = keys::filter_ds(ds, self.validator.policy);
        if ds.is_empty() {
            self.insecure_zone(zone, InsecureReason::UnsupportedAlgorithm, ds_ttl);
            return Ok(Err(InsecureReason::UnsupportedAlgorithm));
        }
        let query = Query {
            name: zone.clone(),
            rtype: RecordType::DNSKEY,
        };
        let reply = self
            .reply(&query)?
            .ok_or_else(|| self.error(BogusReason::KeysNotProven, zone, RecordType::DNSKEY))?;
        // No suspension occurs inside prove_keys. Its local exact-key memo
        // lasts through the entire proof; this proven-zone memo then makes
        // every restarted evaluation reuse the result without more work.
        let proof = keys::prove_keys(
            zone,
            &ds,
            ds_ttl,
            &reply.message,
            self.now,
            self.validator.policy,
            &mut self.budget,
        )
        .map_err(|reason| {
            let reason = match reason {
                BogusReason::TrustAnchorMismatch if !anchor => BogusReason::KeysNotProven,
                reason => reason,
            };
            self.fail(&query, reason, zone, RecordType::DNSKEY)
        })?;
        match proof {
            keys::KeyProof::Secure { keys, ttl_bound } => {
                self.remember(zone, CacheValue::Keys(keys.clone()), ttl_bound);
                Ok(Ok(self.intern_keys(zone, keys)))
            }
            keys::KeyProof::Insecure => {
                let ttl = reply
                    .answer(zone, RecordType::DNSKEY)
                    .map_or(0, |set| set.ttl)
                    .min(ds_ttl);
                self.insecure_zone(zone, InsecureReason::UnsupportedAlgorithm, ttl);
                Ok(Err(InsecureReason::UnsupportedAlgorithm))
            }
        }
    }

    fn status(&mut self, name: &Name, zone: &Name, keys: &ProvenKeys) -> Eval<(Status, u32)> {
        let identity = (name.clone(), zone.clone());
        if let Some(result) = self.statuses.get(&identity) {
            return Ok(result.clone());
        }
        if let Some(entry) = self.cache(name, CacheKind::Status) {
            let ttl = entry.remaining(self.now);
            if let CacheValue::Status { parent, status } = entry.value
                && &parent == zone
            {
                let value = (status, ttl);
                self.statuses.insert(identity, value.clone());
                return Ok(value);
            }
        }
        let query = Query {
            name: name.clone(),
            rtype: RecordType::DS,
        };
        let Some(reply) = self.reply(&query)? else {
            let value = (Status::NotACut, 0);
            self.statuses.insert(identity, value.clone());
            return Ok(value);
        };
        let (status, ttl) = if let Some(set) = reply.answer(name, RecordType::DS) {
            let verified = self
                .verify(&query, 0, set, zone, keys)
                .map_err(|reason| self.fail(&query, reason, name, RecordType::DS))?;
            if verified.wildcard.is_some() {
                return Err(self.error(BogusReason::DelegationUnproven, name, RecordType::DS));
            }
            let ds = set
                .records
                .iter()
                .filter_map(|record| match &record.kind {
                    RdataKind::Ds(ds) => Some(ds.clone()),
                    _ => None,
                })
                .collect();
            (Status::SecureDs(ds), verified.ttl_bound)
        } else if let Some(set) = reply.answer(name, RecordType::CNAME) {
            let verified = self.verify(&query, 0, set, zone, keys).map_err(|reason| {
                self.error(
                    if reason == BogusReason::WorkLimit {
                        reason
                    } else {
                        BogusReason::DelegationUnproven
                    },
                    name,
                    RecordType::DS,
                )
            })?;
            if verified.wildcard.is_some() {
                return Err(self.error(BogusReason::DelegationUnproven, name, RecordType::DS));
            }
            (Status::NotACut, verified.ttl_bound)
        } else {
            let (records, out_of_time) =
                self.verified_denials(&query, &reply, zone, keys, BogusReason::DelegationUnproven)?;
            let result =
                denial::delegation_status(&records, zone, name, &mut self.denial, &mut self.budget)
                    .map_err(|reason| {
                        let reason = match (reason, out_of_time) {
                            (BogusReason::DelegationUnproven, Some(time)) => time,
                            (reason, _) => reason,
                        };
                        self.fail(&query, reason, name, RecordType::DS)
                    })?;
            let status = match result.status {
                denial::Status::InsecureCut => {
                    Status::InsecureCut(InsecureReason::UnsignedDelegation)
                }
                denial::Status::NotACut => Status::NotACut,
            };
            (status, result.ttl_bound)
        };
        self.statuses.insert(identity, (status.clone(), ttl));
        if let Status::InsecureCut(reason) = status {
            self.insecure_cuts.insert(name.clone(), reason);
        }
        self.remember(
            name,
            CacheValue::Status {
                parent: zone.clone(),
                status: status.clone(),
            },
            ttl,
        );
        Ok((status, ttl))
    }

    /// An insecure fact cached at `name` that overrides any zone above it.
    fn insecure_fact(&self, name: &Name) -> Option<InsecureReason> {
        if let Some(reason) = self.insecure_zones.get(name) {
            return Some(*reason);
        }
        if let Some(entry) = self.cache(name, CacheKind::InsecureZone)
            && let CacheValue::InsecureZone(reason) = entry.value
        {
            return Some(reason);
        }
        if let Some(reason) = self.insecure_cuts.get(name) {
            return Some(*reason);
        }
        if let Some(entry) = self.cache(name, CacheKind::Status)
            && let CacheValue::Status {
                status: Status::InsecureCut(reason),
                ..
            } = entry.value
        {
            return Some(reason);
        }
        None
    }

    /// Whether proving `target` would start by fetching the anchor's keys.
    fn anchor_keys_needed(&self, target: &Name) -> Option<Name> {
        if self.validator.private(target) {
            return None;
        }
        let anchor_zone = self.validator.anchor_zone(target)?;
        for count in (anchor_zone.label_count()..=target.label_count()).rev() {
            let name = target.suffix(count)?;
            if self.proven_keys.contains_key(&name)
                || self.cache(&name, CacheKind::Keys).is_some()
                || self.insecure_fact(&name).is_some()
            {
                return None;
            }
        }
        Some(anchor_zone.clone())
    }

    /// Find the secure zone whose keys speak for `target`. `claimed` marks a
    /// target that a signature names as a zone apex.
    fn zone_for(&mut self, target: &Name, claimed: bool) -> Eval<Zone> {
        if self.validator.private(target) {
            return Ok(Zone::Insecure(InsecureReason::PrivateName));
        }
        let Some(anchor_zone) = self.validator.anchor_zone(target).cloned() else {
            return Ok(Zone::Insecure(InsecureReason::NoTrustAnchor));
        };
        let mut cached_zone = None;
        // Facts above the closest configured anchor cannot replace that
        // anchor's independent trust path, whether secure or insecure.
        for count in (anchor_zone.label_count()..=target.label_count()).rev() {
            let Some(name) = target.suffix(count) else {
                continue;
            };
            // Only insecure facts strictly below the selected secure zone
            // override it (handoff 6.7.2). A name may have several cache kinds.
            if let Some(keys) = self.proven_keys.get(&name) {
                cached_zone = Some((name, keys.clone()));
                break;
            }
            if let Some(entry) = self.cache(&name, CacheKind::Keys)
                && let CacheValue::Keys(keys) = entry.value
            {
                let keys = self.intern_keys(&name, keys);
                cached_zone = Some((name, keys));
                break;
            }
            if let Some(reason) = self.insecure_fact(&name) {
                return Ok(Zone::Insecure(reason));
            }
        }
        // The walk below fetches a DS for each label and the claimed zone's
        // keys. These fetches are independent, so a caller may send them now.
        let start = cached_zone
            .as_ref()
            .map_or(anchor_zone.label_count(), |(name, _)| name.label_count());
        for count in start + 1..=target.label_count() {
            if let Some(name) = target.suffix(count) {
                self.hint(&name, RecordType::DS);
            }
        }
        if claimed && start < target.label_count() {
            self.hint(target, RecordType::DNSKEY);
        }
        let (mut zone, mut keys) = if let Some(pair) = cached_zone {
            pair
        } else {
            let ds: Vec<_> = self
                .validator
                .anchors
                .iter()
                .filter(|a| a.zone == anchor_zone)
                .map(|a| Ds {
                    key_tag: a.key_tag,
                    algorithm: a.algorithm,
                    digest_type: a.digest_type,
                    digest: a.digest.clone(),
                })
                .collect();
            match self.keys_for(&anchor_zone, &ds, u32::MAX, true)? {
                Ok(keys) => (anchor_zone, keys),
                Err(reason) => return Ok(Zone::Insecure(reason)),
            }
        };
        let mut position = zone.clone();
        while &position != target {
            let next = target
                .suffix(position.label_count() + 1)
                .ok_or_else(|| self.error(BogusReason::SignerMismatch, target, RecordType::DS))?;
            let (status, ttl) = self.status(&next, &zone, &keys)?;
            match status {
                Status::InsecureCut(reason) => return Ok(Zone::Insecure(reason)),
                Status::NotACut => position = next,
                Status::SecureDs(ds) => {
                    let child_keys = match self.keys_for(&next, &ds, ttl, false)? {
                        Ok(keys) => keys,
                        Err(reason) => return Ok(Zone::Insecure(reason)),
                    };
                    zone = next.clone();
                    position = next;
                    keys = child_keys;
                }
            }
        }
        Ok(Zone::Secure {
            exact: &zone == target,
            name: zone,
            keys,
        })
    }

    fn judge(&mut self, query: &Query, set: &Rrset) -> Eval<Judgment> {
        if self.validator.private(&set.owner) {
            return Ok(Judgment::insecure(InsecureReason::PrivateName, set.ttl));
        }
        let signer =
            plausible_signer(set).map_err(|reason| self.error(reason, &set.owner, set.rtype))?;
        if let Some(signer) = signer {
            if self.validator.anchors.iter().any(|anchor| {
                set.owner.is_at_or_below(&anchor.zone)
                    && anchor.zone.label_count() > signer.label_count()
            }) {
                return Err(self.error(BogusReason::SignerMismatch, &set.owner, set.rtype));
            }
            match self.zone_for(signer, true)? {
                Zone::Insecure(reason) => Ok(Judgment::insecure(reason, set.ttl)),
                Zone::Secure { exact: false, .. } => {
                    Err(self.error(BogusReason::SignerNotAZone, signer, set.rtype))
                }
                Zone::Secure {
                    name,
                    keys,
                    exact: true,
                } => {
                    let verified = self
                        .verify(query, 0, set, &name, &keys)
                        .map_err(|reason| self.fail(query, reason, &set.owner, set.rtype))?;
                    Ok(Judgment {
                        insecure: None,
                        ttl: verified.ttl_bound,
                        wildcard: verified.wildcard,
                        zone: Some((name, keys)),
                    })
                }
            }
        } else {
            match self.zone_for(&set.owner, false)? {
                Zone::Insecure(reason) => Ok(Judgment::insecure(reason, set.ttl)),
                Zone::Secure { .. } => {
                    Err(self.fail(query, BogusReason::SignaturesMissing, &set.owner, set.rtype))
                }
            }
        }
    }

    /// Authenticated denial records, and the clock reason of any that were
    /// otherwise valid but outside their validity window. A record this zone
    /// signed that fails its signature ends the validation with `failure`.
    fn verified_denials(
        &mut self,
        query: &Query,
        reply: &ParsedReply,
        zone: &Name,
        keys: &ProvenKeys,
        failure: BogusReason,
    ) -> Eval<(Vec<VerifiedDenial>, Option<BogusReason>)> {
        let mut records = Vec::new();
        let mut out_of_time = None;
        for index in &reply.denials {
            let set = &reply.message.authority[*index];
            match self.verify(query, 1, set, zone, keys) {
                // Denial records authenticate their literal owner; wildcard
                // expansion must never turn one into an exact-name proof.
                Ok(verified) if verified.wildcard.is_none() => {
                    records.extend(set.records.iter().map(|record| VerifiedDenial {
                        owner: set.owner.clone(),
                        kind: record.kind.clone(),
                        ttl_bound: verified.ttl_bound,
                    }))
                }
                Err(BogusReason::WorkLimit) => {
                    return Err(self.error(BogusReason::WorkLimit, &set.owner, set.rtype));
                }
                Err(
                    reason @ (BogusReason::SignatureExpired | BogusReason::SignatureNotYetValid),
                ) => {
                    out_of_time.get_or_insert(reason);
                }
                // As unbound does, a denial record that claims this zone's
                // signature and fails it makes the reply bogus, even beside
                // a proof that would hold without it. Records signed by
                // another zone, such as an alias target's, are left aside.
                // The reason is the one the caller gives for a failed proof.
                Err(_) if set.rrsigs.iter().any(|rrsig| rrsig.signer == *zone) => {
                    return Err(self.error(failure, &set.owner, set.rtype));
                }
                Ok(_) | Err(_) => {}
            }
        }
        Ok((records, out_of_time))
    }

    fn positive_judgment(
        &mut self,
        query: &Query,
        reply: &ParsedReply,
        set: &Rrset,
    ) -> Eval<Judgment> {
        let mut judgment = self.judge(query, set)?;
        if let (Some(ce), Some((zone, keys))) = (&judgment.wildcard, &judgment.zone) {
            let (records, out_of_time) =
                self.verified_denials(query, reply, zone, keys, BogusReason::WildcardUnproven)?;
            let proof = denial::wildcard(
                &records,
                zone,
                &set.owner,
                ce,
                &mut self.denial,
                &mut self.budget,
            )
            .map_err(|reason| self.error(reason, &set.owner, set.rtype))?;
            match proof.proof {
                Proof::NotProven | Proof::IterationsLimited => {
                    let reason = unproven_reason(
                        proof.proof,
                        out_of_time.unwrap_or(BogusReason::WildcardUnproven),
                    );
                    return Err(self.fail(query, reason, &set.owner, set.rtype));
                }
                Proof::Insecure => judgment.insecure = Some(InsecureReason::UnsignedDelegation),
                Proof::Proven => judgment.ttl = judgment.ttl.min(proof.ttl_bound),
            }
        }
        Ok(judgment)
    }

    fn alias_target(&self, set: &Rrset) -> Result<Name, Flow> {
        match set.records.as_slice() {
            [record] => match &record.kind {
                RdataKind::Name(name) => Ok(name.clone()),
                _ => Err(self.error(BogusReason::AliasInvalid, &set.owner, set.rtype)),
            },
            _ => Err(self.error(BogusReason::AliasInvalid, &set.owner, set.rtype)),
        }
    }

    fn dname_target(
        &mut self,
        query: &Query,
        current: &Name,
        set: &Rrset,
    ) -> Eval<(Name, Judgment)> {
        if self.validator.anchors.iter().any(|anchor| {
            current.is_at_or_below(&anchor.zone)
                && anchor.zone.label_count() > set.owner.label_count()
        }) {
            return Err(self.error(BogusReason::SignerMismatch, &set.owner, set.rtype));
        }
        let target = self.alias_target(set)?;
        let judgment = self.judge(query, set)?;
        if judgment.wildcard.is_some() {
            return Err(self.error(BogusReason::AliasInvalid, &set.owner, set.rtype));
        }
        let prefix = current
            .as_wire()
            .len()
            .checked_sub(set.owner.as_wire().len())
            .ok_or_else(|| self.error(BogusReason::AliasInvalid, current, RecordType::DNAME))?;
        let mut bytes = current.as_wire()[..prefix].to_vec();
        bytes.extend_from_slice(target.as_wire());
        let name = Name::from_wire(&bytes)
            .map_err(|_| self.error(BogusReason::AliasInvalid, current, RecordType::DNAME))?;
        Ok((name, judgment))
    }

    /// A negative answer: its outcome, why it is insecure if it is, the TTL
    /// bound from validated data, and the TTL to use when insecure.
    fn negative(
        &mut self,
        query: &Query,
        reply: &ParsedReply,
        name: &Name,
        aliased: bool,
    ) -> Eval<(Outcome, Option<InsecureReason>, u32, u32)> {
        let outcome = || {
            if reply.message.rcode == 3 {
                Outcome::NoSuchName
            } else {
                Outcome::NoData
            }
        };
        // Only for answers nothing can authenticate: an unsigned zone's SOA.
        let raw_ttl = reply
            .message
            .authority
            .iter()
            .filter(|set| set.rtype == RecordType::SOA)
            .flat_map(|set| {
                set.records
                    .iter()
                    .filter_map(move |record| match &record.kind {
                        RdataKind::Soa(soa) => Some(set.ttl.min(soa.minimum)),
                        _ => None,
                    })
            })
            .min()
            .unwrap_or(0);
        if self.validator.private(name) {
            return Ok((outcome(), Some(InsecureReason::PrivateName), 0, raw_ttl));
        }
        let signer = reply
            .denials
            .iter()
            .filter_map(|i| {
                let set = &reply.message.authority[*i];
                let signer = plausible_signer(set).ok()??;
                (name.is_at_or_below(signer)
                    && !self.validator.anchors.iter().any(|anchor| {
                        name.is_at_or_below(&anchor.zone)
                            && anchor.zone.label_count() > signer.label_count()
                    }))
                .then_some(signer.clone())
            })
            .max_by_key(Name::label_count);
        let Some(signer) = signer else {
            return match self.zone_for(name, false)? {
                Zone::Insecure(reason) => Ok((outcome(), Some(reason), 0, raw_ttl)),
                Zone::Secure { .. } => {
                    Err(self.fail(query, BogusReason::DenialMissing, name, query.rtype))
                }
            };
        };
        let keys = match self.zone_for(&signer, true)? {
            Zone::Insecure(reason) => return Ok((outcome(), Some(reason), 0, raw_ttl)),
            Zone::Secure { exact: false, .. } => {
                return Err(self.error(BogusReason::SignerNotAZone, &signer, query.rtype));
            }
            Zone::Secure {
                keys, exact: true, ..
            } => keys,
        };
        let (records, out_of_time) =
            self.verified_denials(query, reply, &signer, &keys, BogusReason::DenialInvalid)?;
        let unproven = out_of_time.unwrap_or(BogusReason::DenialInvalid);
        if records.is_empty() {
            return Err(self.fail(query, unproven, name, query.rtype));
        }
        let mut proof;
        let mut chosen = Outcome::NoData;
        if reply.message.rcode == 3 {
            proof = denial::name_error(&records, &signer, name, &mut self.denial, &mut self.budget)
                .map_err(|reason| self.error(reason, name, query.rtype))?;
            chosen = Outcome::NoSuchName;
            if !proof.proof.holds() {
                proof = denial::no_data(
                    &records,
                    &signer,
                    name,
                    query.rtype,
                    &mut self.denial,
                    &mut self.budget,
                )
                .map_err(|reason| self.error(reason, name, query.rtype))?;
                chosen = Outcome::NoData;
            }
        } else {
            proof = denial::no_data(
                &records,
                &signer,
                name,
                query.rtype,
                &mut self.denial,
                &mut self.budget,
            )
            .map_err(|reason| self.error(reason, name, query.rtype))?;
            if !proof.proof.holds() && aliased {
                proof =
                    denial::name_error(&records, &signer, name, &mut self.denial, &mut self.budget)
                        .map_err(|reason| self.error(reason, name, query.rtype))?;
                chosen = Outcome::NoSuchName;
            }
        }
        if !proof.proof.holds() {
            let reason = unproven_reason(proof.proof, unproven);
            return Err(self.fail(query, reason, name, query.rtype));
        }
        // The zone is signed, so its SOA must validate, and the TTL a caller
        // may cache comes only from validated records.
        let mut ttl = proof.ttl_bound;
        for set in &reply.message.authority {
            if set.rtype != RecordType::SOA || set.owner != signer {
                continue;
            }
            let verified = self
                .verify(query, 1, set, &signer, &keys)
                .map_err(|reason| self.fail(query, reason, &set.owner, set.rtype))?;
            for record in &set.records {
                if let RdataKind::Soa(soa) = &record.kind {
                    ttl = ttl.min(verified.ttl_bound).min(soa.minimum);
                }
            }
        }
        if proof.proof == Proof::Insecure {
            Ok((
                outcome(),
                Some(InsecureReason::UnsignedDelegation),
                ttl,
                ttl,
            ))
        } else {
            Ok((chosen, None, ttl, raw_ttl))
        }
    }

    fn evaluate(&mut self) -> Eval<Verdict> {
        let query = Query {
            name: self.question.name().clone(),
            rtype: self.question.rtype(),
        };
        // The question and the anchor's keys are independent first fetches.
        if !self.replies.contains_key(&query)
            && let Some(anchor) = self.anchor_keys_needed(&query.name)
        {
            self.hint(&anchor, RecordType::DNSKEY);
        }
        let mut source = query.clone();
        let mut reply = self
            .reply(&source)?
            .ok_or_else(|| self.error(BogusReason::MalformedReply, &query.name, query.rtype))?;
        let mut current = query.name.clone();
        let mut seen = BTreeSet::from([current.clone()]);
        let mut links = 0;
        let mut insecure = None;
        let mut secure_ttl = u32::MAX;
        let mut raw_ttl = u32::MAX;
        loop {
            if let Some(set) = reply.answer(&current, query.rtype) {
                let judgment = self.positive_judgment(&source, &reply, set)?;
                insecure = insecure.or(judgment.insecure);
                secure_ttl = secure_ttl.min(judgment.ttl);
                raw_ttl = raw_ttl.min(set.ttl);
                let answer = Answer {
                    outcome: Outcome::Records {
                        name: current,
                        rdata: set.records.iter().map(|r| r.uncompressed.clone()).collect(),
                    },
                    ttl: if insecure.is_none() {
                        secure_ttl
                    } else {
                        raw_ttl
                    },
                    insecure,
                    rtype: query.rtype,
                };
                return Ok(if insecure.is_none() {
                    Verdict::Secure(answer)
                } else {
                    Verdict::Insecure(answer)
                });
            }
            let (target, judgment, ttl) =
                if let Some(set) = reply.answer(&current, RecordType::CNAME) {
                    let target = self.alias_target(set)?;
                    if set.rrsigs.is_empty()
                        && let Some(dname) = reply.dname(&current)
                    {
                        let (expected, judgment) = self.dname_target(&source, &current, dname)?;
                        if target != expected {
                            return Err(self.error(
                                BogusReason::AliasInvalid,
                                &current,
                                RecordType::CNAME,
                            ));
                        }
                        (target, judgment, dname.ttl.min(set.ttl))
                    } else {
                        (
                            target,
                            self.positive_judgment(&source, &reply, set)?,
                            set.ttl,
                        )
                    }
                } else if let Some(set) = reply.dname(&current) {
                    let (target, judgment) = self.dname_target(&source, &current, set)?;
                    (target, judgment, set.ttl)
                } else if links > 0 && source.name != current && reply.unfinished() {
                    // A validated alias ends at a name this reply says nothing
                    // about: ask for that name itself and carry on there.
                    source = Query {
                        name: current.clone(),
                        rtype: query.rtype,
                    };
                    reply = self.reply(&source)?.ok_or_else(|| {
                        self.error(BogusReason::MalformedReply, &current, query.rtype)
                    })?;
                    continue;
                } else {
                    let (outcome, negative_insecure, ttl, negative_raw) =
                        self.negative(&source, &reply, &current, links > 0)?;
                    insecure = insecure.or(negative_insecure);
                    let answer = Answer {
                        outcome,
                        ttl: if insecure.is_none() {
                            secure_ttl.min(ttl)
                        } else {
                            raw_ttl.min(negative_raw)
                        },
                        insecure,
                        rtype: query.rtype,
                    };
                    return Ok(if insecure.is_none() {
                        Verdict::Secure(answer)
                    } else {
                        Verdict::Insecure(answer)
                    });
                };
            insecure = insecure.or(judgment.insecure);
            secure_ttl = secure_ttl.min(judgment.ttl);
            raw_ttl = raw_ttl.min(ttl);
            links += 1;
            if !seen.insert(target.clone()) {
                return Err(self.error(BogusReason::AliasInvalid, &current, RecordType::CNAME));
            }
            if links > MAX_ALIAS_LINKS {
                return Err(self.error(BogusReason::WorkLimit, &current, RecordType::CNAME));
            }
            current = target;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn question() -> Question {
        Question::new(Name::from_text("www.example.").unwrap(), RecordType::A).unwrap()
    }

    #[test]
    fn validator_is_send_sync_and_validation_is_send() {
        fn send_sync<T: Send + Sync>() {}
        fn send<T: Send>() {}
        send_sync::<Validator>();
        send::<Validation<'_>>();
    }

    #[test]
    fn repeated_need_wrong_supply_and_repeated_done_preserve_state() {
        let validator = Validator::with_root_anchors();
        let mut validation = validator.begin(question(), 0);
        let query = Query {
            name: question().name().clone(),
            rtype: RecordType::A,
        };
        assert_eq!(validation.step(), Step::Need(query.clone()));
        assert_eq!(validation.step(), Step::Need(query.clone()));
        let wrong = Query {
            name: query.name.clone(),
            rtype: RecordType::AAAA,
        };
        assert_eq!(
            validation.supply(&wrong, &[]),
            Err(SupplyError::UnexpectedQuery)
        );
        assert_eq!(validation.failed(&wrong), Err(SupplyError::UnexpectedQuery));
        assert_eq!(validation.step(), Step::Need(query.clone()));
        validation.failed(&query).unwrap();
        let done = Step::Done(Verdict::Unanswered(Unanswered {
            query: query.clone(),
            cause: UnansweredCause::FetchFailed,
            extended_errors: Vec::new(),
        }));
        assert_eq!(validation.step(), done);
        assert_eq!(validation.step(), done);
        assert_eq!(
            validation.supply(&query, &[]),
            Err(SupplyError::UnexpectedQuery)
        );
    }

    #[test]
    fn keyset_snapshots_keep_flags_protocol_and_public_bytes_distinct() {
        let validator = Validator::with_root_anchors();
        let (key, _, set) = crate::test_vectors::p256();
        let zone = set.rrsigs[0].signer.clone();
        let query = Query {
            name: set.owner.clone(),
            rtype: set.rtype,
        };
        let mut validation = validator.begin(question(), u64::from(set.rrsigs[0].inception) + 1);
        let original = validation.intern_keys(&zone, vec![key.clone()]);
        assert!(validation.verify(&query, 0, &set, &zone, &original).is_ok());
        assert_eq!(validation.budget.signatures, 1);
        for changed in [
            Dnskey {
                flags: key.flags ^ 1,
                ..key.clone()
            },
            Dnskey {
                protocol: 2,
                ..key.clone()
            },
            Dnskey {
                algorithm: 15,
                ..key.clone()
            },
            {
                let mut changed = key.clone();
                changed.public_key[0] ^= 1;
                changed
            },
        ] {
            let snapshot = validation.intern_keys(&zone, vec![changed]);
            assert_ne!(snapshot.id, original.id);
            assert!(
                validation
                    .verify(&query, 0, &set, &zone, &snapshot)
                    .is_err()
            );
        }
        // Keeping the original immutable snapshot alive also preserves its
        // memoized success without additional cryptographic verification.
        assert!(validation.verify(&query, 0, &set, &zone, &original).is_ok());
        assert_eq!(validation.budget.signatures, 1);
    }

    #[test]
    fn cached_insecure_cut_below_secure_keys_prevents_reusing_the_zone() {
        let validator = Validator::with_root_anchors();
        let (key, _, _) = crate::test_vectors::p256();
        let cut = Name::from_text("example.").unwrap();
        let mut validation = validator.begin(question(), 0);
        validation.remember(&Name::root(), CacheValue::Keys(vec![key.clone()]), 3600);
        validation.remember(
            &cut,
            CacheValue::Status {
                parent: Name::root(),
                status: Status::InsecureCut(InsecureReason::UnsignedDelegation),
            },
            3600,
        );
        assert!(matches!(
            validation.zone_for(question().name(), false),
            Ok(Zone::Insecure(InsecureReason::UnsignedDelegation))
        ));
        // The selected secure zone itself is excluded from the check: only
        // a cut strictly below that zone can override the starting keys.
        validation.remember(&cut, CacheValue::Keys(vec![key]), 3600);
        assert!(matches!(
            validation.zone_for(&cut, true),
            Ok(Zone::Secure { exact: true, .. })
        ));
        assert_eq!(validation.budget.signatures, 0);
    }

    #[test]
    fn poisoned_cache_misses_and_skips_writes() {
        let validator = Validator::with_root_anchors();
        let _ = std::panic::catch_unwind(|| {
            let _guard = validator.cache.lock().unwrap();
            panic!("poison the trust cache for this test");
        });
        let validation = validator.begin(question(), 0);
        assert!(validation.cache(&Name::root(), CacheKind::Keys).is_none());
        validation.remember(
            &Name::root(),
            CacheValue::InsecureZone(InsecureReason::UnsignedDelegation),
            3600,
        );
        assert!(
            validation
                .cache(&Name::root(), CacheKind::InsecureZone)
                .is_none()
        );
    }
}
