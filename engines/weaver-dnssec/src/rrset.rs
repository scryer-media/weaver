// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Bounded signature validation over parsed records, with no I/O or clock.

use std::collections::BTreeMap;

use crate::algorithm;
use crate::canonical::{signed_data, signing_labels};
use crate::limits::{
    MAX_CLOCK_SKEW_SECONDS, MAX_KEY_TAG_MATCHES, MAX_RRSIGS_PER_RRSET, MAX_SIGNATURE_CHECKS,
    MAX_SIGNATURE_CHECKS_PER_RRSET, MIN_CLOCK_SKEW_SECONDS,
};
use crate::rdata::{Dnskey, RdataKind, Rrsig};
use crate::wire::Rrset;
use crate::{BogusReason, Name, Policy, RecordType};

#[derive(Clone, Debug, Default)]
pub(crate) struct WorkBudget {
    pub(crate) crypto_operations: usize,
    pub(crate) signatures: usize,
    pub(crate) ds_digests: usize,
    pub(crate) nsec3_hashes: usize,
}

impl WorkBudget {
    pub(crate) fn charge_crypto(&mut self) -> Result<(), BogusReason> {
        if self.crypto_operations >= MAX_SIGNATURE_CHECKS {
            return Err(BogusReason::WorkLimit);
        }
        self.crypto_operations += 1;
        Ok(())
    }
}

pub(crate) enum SignatureLimit<'a> {
    CachedKeys(&'a mut usize),
    ExplicitKey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Verified {
    pub(crate) wildcard: Option<Name>,
    pub(crate) ttl_bound: u32,
}

/// Check inclusive validity bounds using RFC 4034 serial-number arithmetic.
/// Both ends allow unbound's clock tolerance: a tenth of the signature's
/// lifetime, at least an hour and at most a day.
pub(crate) fn signature_time(
    inception: u32,
    expiration: u32,
    now: u64,
) -> Result<u32, BogusReason> {
    if expiration.wrapping_sub(inception) >= 1_u32 << 31 {
        return Err(BogusReason::SignaturesMissing);
    }
    // The driver also clamps this. Keeping the bound here makes this pure
    // function safe for every u64 caller.
    let now = now.min(1_u64 << 62);
    let signed_now = now as i64;
    let map = |field: u32| signed_now.checked_add(i64::from(field.wrapping_sub(now as u32) as i32));
    let inception = map(inception).ok_or(BogusReason::SignaturesMissing)?;
    let expiration = map(expiration).ok_or(BogusReason::SignaturesMissing)?;
    let lifetime = expiration
        .checked_sub(inception)
        .filter(|duration| *duration >= 0)
        .ok_or(BogusReason::SignaturesMissing)?;
    let skew = (lifetime / 10).clamp(
        i64::from(MIN_CLOCK_SKEW_SECONDS),
        i64::from(MAX_CLOCK_SKEW_SECONDS),
    );
    if signed_now < inception.saturating_sub(skew) {
        return Err(BogusReason::SignatureNotYetValid);
    }
    if signed_now > expiration.saturating_add(skew) {
        return Err(BogusReason::SignatureExpired);
    }
    let remaining = expiration.saturating_sub(signed_now).max(0);
    Ok(u32::try_from(remaining).unwrap_or(u32::MAX))
}

/// Ignore signatures from unrelated names, while rejecting ambiguous ancestors.
pub(crate) fn plausible_signer(set: &Rrset) -> Result<Option<&Name>, BogusReason> {
    let mut signer = None;
    for signature in &set.rrsigs {
        if signature.type_covered != set.rtype.0 || !set.owner.is_at_or_below(&signature.signer) {
            continue;
        }
        if signer.is_some_and(|name| name != &signature.signer) {
            return Err(BogusReason::SignerMismatch);
        }
        signer = Some(&signature.signer);
    }
    Ok(signer)
}

pub(crate) fn verify_rrset(
    set: &Rrset,
    zone: &Name,
    keys: &[Dnskey],
    now: u64,
    policy: Policy,
    budget: &mut WorkBudget,
    signature_limit: SignatureLimit<'_>,
) -> Result<Verified, BogusReason> {
    let signer = plausible_signer(set)?.ok_or(BogusReason::SignaturesMissing)?;
    if signer != zone {
        return Err(BogusReason::SignerMismatch);
    }

    // Build once: attacker-controlled signatures do not repeatedly scan keys.
    let mut candidates: BTreeMap<(u16, u8), Vec<&Dnskey>> = BTreeMap::new();
    for key in keys {
        if key.flags & 0x0100 == 0 || key.flags & 0x0080 != 0 || key.protocol != 3 {
            continue;
        }
        let matches = candidates
            .entry((algorithm::key_tag(key), key.algorithm))
            .or_default();
        if matches.len() < MAX_KEY_TAG_MATCHES {
            matches.push(key);
        }
    }
    let owner_fits = set.owner.is_at_or_below(zone)
        && (set.rtype != RecordType::NSEC3 || set.owner.parent().as_ref() == Some(zone))
        && (set.rtype != RecordType::DS || &set.owner != zone)
        && (set.rtype != RecordType::NSEC
            || set.records.iter().all(|record| {
                matches!(&record.kind, RdataKind::Nsec(nsec) if nsec.next.is_at_or_below(zone))
            }));
    let mut attempt = Attempt {
        candidates: &candidates,
        budget,
        signature_limit,
        invalid: false,
        no_key: false,
    };
    // A signature outside its validity window is checked only after every
    // in-window one failed, so a clock reason is given only for a signature
    // that is otherwise valid.
    let mut out_of_time = Vec::new();
    for signature in set.rrsigs.iter().take(MAX_RRSIGS_PER_RRSET) {
        if signature.type_covered != set.rtype.0 || &signature.signer != zone {
            continue;
        }
        if !algorithm::supported_algorithm(signature.algorithm, &policy) {
            continue;
        }
        if !owner_fits {
            continue;
        }
        let labels = usize::from(signature.labels);
        if labels > signing_labels(&set.owner) || labels < zone.label_count() {
            continue;
        }
        let remaining = match signature_time(signature.inception, signature.expiration, now) {
            Ok(remaining) => remaining,
            Err(reason @ (BogusReason::SignatureExpired | BogusReason::SignatureNotYetValid)) => {
                out_of_time.push((signature, reason));
                continue;
            }
            Err(_) => continue,
        };
        if let Some(wildcard) = attempt.check(set, signature)? {
            return Ok(Verified {
                wildcard,
                ttl_bound: set
                    .ttl
                    .min(signature.received_ttl)
                    .min(signature.original_ttl)
                    .min(remaining),
            });
        }
    }
    for (signature, reason) in out_of_time {
        if attempt.check(set, signature)?.is_some() {
            return Err(reason);
        }
    }
    Err(if attempt.invalid {
        BogusReason::SignatureInvalid
    } else if attempt.no_key {
        BogusReason::NoMatchingKey
    } else {
        BogusReason::SignaturesMissing
    })
}

struct Attempt<'a, 'k> {
    candidates: &'a BTreeMap<(u16, u8), Vec<&'k Dnskey>>,
    budget: &'a mut WorkBudget,
    signature_limit: SignatureLimit<'a>,
    invalid: bool,
    no_key: bool,
}

impl Attempt<'_, '_> {
    /// Verify one signature with its candidate keys. `Some` holds the
    /// wildcard closest encloser of a verified signature.
    fn check(
        &mut self,
        set: &Rrset,
        signature: &Rrsig,
    ) -> Result<Option<Option<Name>>, BogusReason> {
        let Some(keys) = self
            .candidates
            .get(&(signature.key_tag, signature.algorithm))
        else {
            self.no_key = true;
            return Ok(None);
        };
        let signed = signed_data(set, signature)?;
        let mut usable = false;
        for key in keys {
            if !algorithm::key_usable(key) {
                continue;
            }
            usable = true;
            // At most MAX_SIGNATURE_CHECKS_PER_RRSET failed attempts are made.
            if matches!(&self.signature_limit, SignatureLimit::CachedKeys(failures) if **failures >= MAX_SIGNATURE_CHECKS_PER_RRSET)
            {
                return Err(BogusReason::WorkLimit);
            }
            self.budget.charge_crypto()?;
            self.budget.signatures = self
                .budget
                .signatures
                .checked_add(1)
                .ok_or(BogusReason::WorkLimit)?;
            if algorithm::verify(key, &signed.bytes, &signature.signature) {
                return Ok(Some(signed.wildcard));
            }
            if let SignatureLimit::CachedKeys(failures) = &mut self.signature_limit {
                **failures = failures.checked_add(1).ok_or(BogusReason::WorkLimit)?;
            }
            self.invalid = true;
        }
        self.no_key |= !usable;
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_vectors;

    // All signed inputs originate in rfc6605.txt lines 206..219 via p256().
    // Changes below are deliberate mutations of that published example.
    fn check(set: &Rrset, keys: &[Dnskey], now: u64) -> (Result<Verified, BogusReason>, usize) {
        let mut budget = WorkBudget::default();
        let mut checks = 0;
        let result = verify_rrset(
            set,
            &Name::from_text("example.net.").unwrap(),
            keys,
            now,
            Policy::default(),
            &mut budget,
            SignatureLimit::CachedKeys(&mut checks),
        );
        (result, budget.signatures)
    }

    #[test]
    fn validity_uses_serial_arithmetic_and_skewed_inclusive_boundaries() {
        // A 10 000 s lifetime gets the one-hour minimum tolerance.
        assert_eq!(signature_time(10_000, 20_000, 15_000), Ok(5_000));
        assert_eq!(signature_time(10_000, 20_000, 10_000), Ok(10_000));
        assert_eq!(signature_time(10_000, 20_000, 10_001), Ok(9_999));
        assert_eq!(signature_time(10_000, 20_000, 9_999), Ok(10_001));
        assert_eq!(signature_time(10_000, 20_000, 6_400), Ok(13_600));
        assert_eq!(
            signature_time(10_000, 20_000, 6_399),
            Err(BogusReason::SignatureNotYetValid)
        );
        assert_eq!(signature_time(10_000, 20_000, 19_999), Ok(1));
        assert_eq!(signature_time(10_000, 20_000, 20_000), Ok(0));
        assert_eq!(signature_time(10_000, 20_000, 20_001), Ok(0));
        assert_eq!(signature_time(10_000, 20_000, 23_600), Ok(0));
        assert_eq!(
            signature_time(10_000, 20_000, 23_601),
            Err(BogusReason::SignatureExpired)
        );
        // A tenth of a 100 000 s lifetime lies between the clamps.
        assert_eq!(signature_time(100_000, 200_000, 90_000), Ok(110_000));
        assert_eq!(
            signature_time(100_000, 200_000, 89_999),
            Err(BogusReason::SignatureNotYetValid)
        );
        assert_eq!(signature_time(100_000, 200_000, 210_000), Ok(0));
        assert_eq!(
            signature_time(100_000, 200_000, 210_001),
            Err(BogusReason::SignatureExpired)
        );
        // A month-long lifetime is clamped to a day.
        let month = 30 * 86_400;
        assert_eq!(
            signature_time(month, 2 * month, u64::from(month) - 86_400),
            Ok(month + 86_400)
        );
        assert_eq!(
            signature_time(month, 2 * month, u64::from(month) - 86_401),
            Err(BogusReason::SignatureNotYetValid)
        );
        assert_eq!(
            signature_time(month, 2 * month, u64::from(2 * month) + 86_400),
            Ok(0)
        );
        assert_eq!(
            signature_time(month, 2 * month, u64::from(2 * month) + 86_401),
            Err(BogusReason::SignatureExpired)
        );
        assert_eq!(
            signature_time(u32::MAX - 100, 1_000, (1_u64 << 32) + 20),
            Ok(980)
        );
        assert_eq!(
            signature_time(20_000, 10_000, 15_000),
            Err(BogusReason::SignaturesMissing)
        );
        assert_eq!(signature_time(0, 10_000, u64::MAX), Ok(10_000));
        for now in [0, 15_000, 1_u64 << 31, u64::MAX] {
            assert_eq!(
                signature_time(20_000, 10_000, now),
                Err(BogusReason::SignaturesMissing)
            );
            assert_eq!(
                signature_time(0, 1_u32 << 31, now),
                Err(BogusReason::SignaturesMissing)
            );
        }
        let inception = u32::MAX - 100;
        for (now, expected) in [
            (
                u64::from(inception) - 3_601,
                Err(BogusReason::SignatureNotYetValid),
            ),
            (u64::from(inception) - 3_600, Ok(4_701)),
            (u64::from(inception), Ok(1_101)),
            (u64::from(inception) + 1, Ok(1_100)),
            ((1_u64 << 32) + 999, Ok(1)),
            ((1_u64 << 32) + 1_000, Ok(0)),
            ((1_u64 << 32) + 4_600, Ok(0)),
            ((1_u64 << 32) + 4_601, Err(BogusReason::SignatureExpired)),
        ] {
            assert_eq!(signature_time(inception, 1_000, now), expected);
        }
        assert_eq!(signature_time(10, 10, 10), Ok(0));
        assert_eq!(signature_time(0, 10_000, 1_u64 << 62), Ok(10_000));
    }

    #[test]
    fn rfc_p256_verifies_and_uses_each_ttl_bound() {
        let (key, _, mut set) = test_vectors::p256();
        let sig = set.rrsigs[0].clone();
        let now = u64::from(sig.inception) + 1;
        let (result, checks) = check(&set, std::slice::from_ref(&key), now);
        assert_eq!(
            result,
            Ok(Verified {
                wildcard: None,
                ttl_bound: 3600
            })
        );
        assert_eq!(checks, 1);
        set.ttl = 42;
        assert_eq!(
            check(&set, std::slice::from_ref(&key), now)
                .0
                .unwrap()
                .ttl_bound,
            42
        );
        set.ttl = u32::MAX;
        assert_eq!(
            check(&set, std::slice::from_ref(&key), now)
                .0
                .unwrap()
                .ttl_bound,
            sig.original_ttl
        );
        assert_eq!(
            check(
                &set,
                std::slice::from_ref(&key),
                u64::from(sig.expiration) - 7
            )
            .0
            .unwrap()
            .ttl_bound,
            7
        );
        assert_eq!(
            check(&set, &[key], u64::from(sig.expiration))
                .0
                .unwrap()
                .ttl_bound,
            0
        );
    }

    #[test]
    fn record_owner_key_and_signature_mutations_fail_crypto() {
        let (key, _, set) = test_vectors::p256();
        let now = u64::from(set.rrsigs[0].inception) + 1;
        let mut changed = set.clone();
        changed.records[0].canonical[0] ^= 1;
        assert_eq!(
            check(&changed, std::slice::from_ref(&key), now),
            (Err(BogusReason::SignatureInvalid), 1)
        );
        let mut changed = set.clone();
        changed.owner = Name::from_text("wwv.example.net.").unwrap();
        assert_eq!(
            check(&changed, std::slice::from_ref(&key), now),
            (Err(BogusReason::SignatureInvalid), 1)
        );
        let mut changed = set.clone();
        changed.rrsigs[0].signature[0] ^= 1;
        assert_eq!(
            check(&changed, std::slice::from_ref(&key), now),
            (Err(BogusReason::SignatureInvalid), 1)
        );
        let mut changed_key = key;
        changed_key.public_key[0] ^= 1;
        let mut changed = set;
        changed.rrsigs[0].key_tag = algorithm::key_tag(&changed_key);
        assert_eq!(
            check(&changed, &[changed_key], now),
            (Err(BogusReason::SignatureInvalid), 1)
        );
    }

    #[test]
    fn time_reasons_need_a_valid_signature_and_preserve_reason_priority() {
        let (key, _, set) = test_vectors::p256();
        let sig = &set.rrsigs[0];
        let skew = u64::from((sig.expiration - sig.inception) / 10).clamp(
            u64::from(MIN_CLOCK_SKEW_SECONDS),
            u64::from(MAX_CLOCK_SKEW_SECONDS),
        );
        let early = u64::from(sig.inception) - skew;
        let late = u64::from(sig.expiration) + skew;
        assert!(check(&set, std::slice::from_ref(&key), early).0.is_ok());
        assert!(check(&set, std::slice::from_ref(&key), late).0.is_ok());
        assert_eq!(
            check(&set, std::slice::from_ref(&key), early - 1),
            (Err(BogusReason::SignatureNotYetValid), 1)
        );
        assert_eq!(
            check(&set, std::slice::from_ref(&key), late + 1),
            (Err(BogusReason::SignatureExpired), 1)
        );
        // A forged signature outside its window is invalid, not a clock problem.
        let mut forged = set.clone();
        forged.rrsigs[0].signature[0] ^= 1;
        for now in [early - 1, late + 1] {
            assert_eq!(
                check(&forged, std::slice::from_ref(&key), now),
                (Err(BogusReason::SignatureInvalid), 1)
            );
        }
        // An expired signature listed first is deferred: the in-window one
        // after it is the only one tried.
        let mut both = set.clone();
        both.rrsigs[0].expiration = sig.inception.wrapping_add(10);
        both.rrsigs.push(sig.clone());
        let now = u64::from(sig.inception) + 10_000;
        assert!(now + 1 < u64::from(sig.expiration));
        let (result, attempts) = check(&both, std::slice::from_ref(&key), now);
        assert!(result.is_ok());
        assert_eq!(attempts, 1);
        let mut changed = set.clone();
        changed.rrsigs[0].inception = sig.expiration;
        changed.rrsigs[0].expiration = sig.inception;
        assert_eq!(
            check(&changed, &[key], u64::from(sig.inception) + 1),
            (Err(BogusReason::SignaturesMissing), 0)
        );
    }

    #[test]
    fn required_prechecks_never_reach_crypto() {
        let (key, _, set) = test_vectors::p256();
        let now = u64::from(set.rrsigs[0].inception) + 1;
        let mut changed = set.clone();
        changed.rrsigs.clear();
        assert_eq!(
            check(&changed, std::slice::from_ref(&key), now),
            (Err(BogusReason::SignaturesMissing), 0)
        );
        let mut changed = set.clone();
        changed.rrsigs[0].type_covered = RecordType::MX.0;
        assert_eq!(
            check(&changed, std::slice::from_ref(&key), now),
            (Err(BogusReason::SignaturesMissing), 0)
        );
        let mut changed = set.clone();
        changed.rrsigs[0].signer = set.owner.clone();
        assert_eq!(
            check(&changed, std::slice::from_ref(&key), now),
            (Err(BogusReason::SignerMismatch), 0)
        );
        let mut changed = set.clone();
        changed.rrsigs[0].labels = 4;
        assert_eq!(
            check(&changed, std::slice::from_ref(&key), now),
            (Err(BogusReason::SignaturesMissing), 0)
        );
        changed.rrsigs[0].labels = 1;
        assert_eq!(
            check(&changed, std::slice::from_ref(&key), now),
            (Err(BogusReason::SignaturesMissing), 0)
        );
        for (flags, protocol) in [
            (key.flags & !0x0100, 3),
            (key.flags | 0x0080, 3),
            (key.flags, 2),
        ] {
            let mut changed_key = key.clone();
            changed_key.flags = flags;
            changed_key.protocol = protocol;
            let mut changed = set.clone();
            changed.rrsigs[0].key_tag = algorithm::key_tag(&changed_key);
            assert_eq!(
                check(&changed, &[changed_key], now),
                (Err(BogusReason::NoMatchingKey), 0)
            );
        }
        let mut changed = set.clone();
        changed.rrsigs[0].algorithm = 253;
        assert_eq!(
            check(&changed, &[key], now),
            (Err(BogusReason::SignaturesMissing), 0)
        );
    }

    #[test]
    fn owner_zone_checks_and_mixed_signers_reject_without_crypto() {
        let (key, _, set) = test_vectors::p256();
        let now = u64::from(set.rrsigs[0].inception) + 1;
        let mut changed = set.clone();
        changed.owner = Name::root();
        assert_eq!(check(&changed, std::slice::from_ref(&key), now).1, 0);
        let mut changed = set.clone();
        changed.rtype = RecordType::DS;
        changed.rrsigs[0].type_covered = RecordType::DS.0;
        changed.owner = Name::from_text("example.net.").unwrap();
        assert_eq!(check(&changed, std::slice::from_ref(&key), now).1, 0);
        changed.rtype = RecordType::NSEC3;
        changed.rrsigs[0].type_covered = RecordType::NSEC3.0;
        assert_eq!(check(&changed, std::slice::from_ref(&key), now).1, 0);
        let mut changed = set.clone();
        changed.rtype = RecordType::NSEC;
        changed.rrsigs[0].type_covered = RecordType::NSEC.0;
        changed.records[0].kind = RdataKind::Nsec(crate::rdata::Nsec {
            next: Name::root(),
            types: crate::rdata::TypeBitmap::parse(&[]).unwrap(),
        });
        assert_eq!(
            check(&changed, std::slice::from_ref(&key), now),
            (Err(BogusReason::SignaturesMissing), 0)
        );
        let mut changed = set.clone();
        let mut other = changed.rrsigs[0].clone();
        other.signer = set.owner.clone();
        changed.rrsigs.push(other);
        assert_eq!(
            check(&changed, &[key], now),
            (Err(BogusReason::SignerMismatch), 0)
        );
    }

    #[test]
    fn later_valid_signature_succeeds_after_invalid_signature() {
        let (key, _, mut set) = test_vectors::p256();
        let now = u64::from(set.rrsigs[0].inception) + 1;
        let valid = set.rrsigs[0].clone();
        set.rrsigs[0].signature[0] ^= 1;
        set.rrsigs.push(valid);
        let (result, checks) = check(&set, &[key], now);
        assert!(result.is_ok());
        assert_eq!(checks, 2);
    }

    #[test]
    fn signature_failure_limit_counts_failures_and_global_limit_counts_attempts() {
        let (key, _, mut set) = test_vectors::p256();
        let now = u64::from(set.rrsigs[0].inception) + 1;
        let valid = set.rrsigs[0].clone();
        set.rrsigs[0].signature[0] ^= 1;
        // Exactly MAX_SIGNATURE_CHECKS_PER_RRSET failures are made: the next
        // signature is not tried, even when it would verify.
        set.rrsigs = vec![set.rrsigs[0].clone(); MAX_SIGNATURE_CHECKS_PER_RRSET + 1];
        assert_eq!(
            check(&set, std::slice::from_ref(&key), now),
            (Err(BogusReason::WorkLimit), MAX_SIGNATURE_CHECKS_PER_RRSET)
        );
        set.rrsigs.pop();
        set.rrsigs.push(valid.clone());
        assert_eq!(
            check(&set, std::slice::from_ref(&key), now),
            (Err(BogusReason::WorkLimit), MAX_SIGNATURE_CHECKS_PER_RRSET)
        );
        set.rrsigs.truncate(MAX_SIGNATURE_CHECKS_PER_RRSET - 1);
        set.rrsigs.push(valid);
        let (result, attempts) = check(&set, std::slice::from_ref(&key), now);
        assert!(result.is_ok());
        assert_eq!(attempts, MAX_SIGNATURE_CHECKS_PER_RRSET);
        set.rrsigs.truncate(1);
        let zone = Name::from_text("example.net.").unwrap();
        let mut budget = WorkBudget::default();
        let mut shared = MAX_SIGNATURE_CHECKS_PER_RRSET - 1;
        assert_eq!(
            verify_rrset(
                &set,
                &zone,
                std::slice::from_ref(&key),
                now,
                Policy::default(),
                &mut budget,
                SignatureLimit::CachedKeys(&mut shared)
            ),
            Err(BogusReason::SignatureInvalid)
        );
        assert_eq!(
            verify_rrset(
                &set,
                &zone,
                std::slice::from_ref(&key),
                now,
                Policy::default(),
                &mut budget,
                SignatureLimit::CachedKeys(&mut shared)
            ),
            Err(BogusReason::WorkLimit)
        );
        assert_eq!(budget.signatures, 1);
        assert_eq!(shared, MAX_SIGNATURE_CHECKS_PER_RRSET);
        let mut valid_set = set.clone();
        valid_set.rrsigs[0].signature[0] ^= 1;
        let mut budget = WorkBudget::default();
        let mut failures = MAX_SIGNATURE_CHECKS_PER_RRSET;
        assert_eq!(
            verify_rrset(
                &valid_set,
                &zone,
                std::slice::from_ref(&key),
                now,
                Policy::default(),
                &mut budget,
                SignatureLimit::CachedKeys(&mut failures),
            ),
            Err(BogusReason::WorkLimit)
        );
        assert_eq!(budget.signatures, 0);
        let mut budget = WorkBudget {
            crypto_operations: MAX_SIGNATURE_CHECKS - 1,
            signatures: MAX_SIGNATURE_CHECKS - 1,
            ..WorkBudget::default()
        };
        let mut failures = MAX_SIGNATURE_CHECKS_PER_RRSET - 1;
        assert!(
            verify_rrset(
                &valid_set,
                &zone,
                std::slice::from_ref(&key),
                now,
                Policy::default(),
                &mut budget,
                SignatureLimit::CachedKeys(&mut failures),
            )
            .is_ok()
        );
        assert_eq!(budget.signatures, MAX_SIGNATURE_CHECKS);
        assert_eq!(failures, MAX_SIGNATURE_CHECKS_PER_RRSET - 1);
        let mut budget = WorkBudget {
            crypto_operations: MAX_SIGNATURE_CHECKS,
            signatures: MAX_SIGNATURE_CHECKS,
            ..WorkBudget::default()
        };
        let mut checks = 0;
        assert_eq!(
            verify_rrset(
                &set,
                &zone,
                &[key],
                now,
                Policy::default(),
                &mut budget,
                SignatureLimit::CachedKeys(&mut checks)
            ),
            Err(BogusReason::WorkLimit)
        );
        assert_eq!(checks, 0);
    }

    #[test]
    fn explicit_key_can_validate_after_twenty_one_failures_but_cannot_exceed_global_work() {
        let (key, _, mut set) = test_vectors::p256();
        let now = u64::from(set.rrsigs[0].inception) + 1;
        let zone = set.rrsigs[0].signer.clone();
        let valid = set.rrsigs[0].clone();
        let mut bad = valid.clone();
        bad.signature[0] ^= 1;
        set.rrsigs = vec![bad; MAX_SIGNATURE_CHECKS_PER_RRSET + 1];
        set.rrsigs.push(valid);
        let mut budget = WorkBudget::default();
        assert!(
            verify_rrset(
                &set,
                &zone,
                std::slice::from_ref(&key),
                now,
                Policy::default(),
                &mut budget,
                SignatureLimit::ExplicitKey,
            )
            .is_ok()
        );
        assert_eq!(budget.signatures, MAX_SIGNATURE_CHECKS_PER_RRSET + 2);
        assert_eq!(budget.crypto_operations, budget.signatures);
        let mut budget = WorkBudget {
            crypto_operations: MAX_SIGNATURE_CHECKS - MAX_SIGNATURE_CHECKS_PER_RRSET - 1,
            ..WorkBudget::default()
        };
        assert_eq!(
            verify_rrset(
                &set,
                &zone,
                std::slice::from_ref(&key),
                now,
                Policy::default(),
                &mut budget,
                SignatureLimit::ExplicitKey,
            ),
            Err(BogusReason::WorkLimit)
        );
        assert_eq!(budget.crypto_operations, MAX_SIGNATURE_CHECKS);
        assert_eq!(budget.signatures, MAX_SIGNATURE_CHECKS_PER_RRSET + 1);
        set.rrsigs.pop();
        let mut budget = WorkBudget::default();
        assert_eq!(
            verify_rrset(
                &set,
                &zone,
                &[key],
                now,
                Policy::default(),
                &mut budget,
                SignatureLimit::ExplicitKey,
            ),
            Err(BogusReason::SignatureInvalid)
        );
    }

    #[test]
    fn unrelated_and_other_type_signatures_never_mask_the_relevant_signer() {
        let (key, _, set) = test_vectors::p256();
        let now = u64::from(set.rrsigs[0].inception) + 1;
        for before in [false, true] {
            for wrong_type in [false, true] {
                let mut changed = set.clone();
                let mut other = changed.rrsigs[0].clone();
                if wrong_type {
                    other.type_covered = RecordType::MX.0;
                    other.signer = Name::root();
                } else {
                    other.signer = Name::from_text("unrelated.test").unwrap();
                }
                other.signature[0] ^= 1;
                if before {
                    changed.rrsigs.insert(0, other.clone());
                } else {
                    changed.rrsigs.push(other.clone());
                }
                assert!(check(&changed, std::slice::from_ref(&key), now).0.is_ok());
                changed.rrsigs = vec![other];
                assert_eq!(
                    check(&changed, std::slice::from_ref(&key), now),
                    (Err(BogusReason::SignaturesMissing), 0)
                );
            }
        }
    }
}
