// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Synchronous DNSSEC validation over caller-supplied DNS messages.
//!
//! The caller supplies transport and time. Validation performs no I/O and
//! requests the additional DNSKEY and DS messages needed to prove an answer.

#![forbid(unsafe_code)]

mod algorithm;
mod anchor;
mod cache;
mod canonical;
mod denial;
mod keys;
#[cfg(test)]
mod ml_dsa_tests;
#[cfg(test)]
mod rfc4035_tests;
mod rrset;
#[cfg(test)]
mod test_vectors;

mod limits;
mod name;
mod rdata;
#[cfg(feature = "test-support")]
pub mod testing;
mod validate;
mod wire;

pub use anchor::{AnchorError, Policy, TrustAnchor};
pub use name::{Name, NameError};
pub use validate::{Validation, Validator};

/// A DNS record type number. Unknown numbers are preserved.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct RecordType(pub u16);

impl RecordType {
    pub const A: Self = Self(1);
    pub const NS: Self = Self(2);
    pub const CNAME: Self = Self(5);
    pub const SOA: Self = Self(6);
    pub const PTR: Self = Self(12);
    pub const MX: Self = Self(15);
    pub const TXT: Self = Self(16);
    pub const AAAA: Self = Self(28);
    pub const SRV: Self = Self(33);
    pub const DNAME: Self = Self(39);
    pub const OPT: Self = Self(41);
    pub const DS: Self = Self(43);
    pub const RRSIG: Self = Self(46);
    pub const NSEC: Self = Self(47);
    pub const DNSKEY: Self = Self(48);
    pub const NSEC3: Self = Self(50);
    pub const SVCB: Self = Self(64);
    pub const HTTPS: Self = Self(65);
}

/// An IN-class question for a final record type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Question {
    name: Name,
    rtype: RecordType,
}

/// The requested record type is reserved for validation, aliases, or meta queries.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum QuestionError {
    #[error("record type {0:?} cannot be requested as a final answer")]
    UnsupportedType(RecordType),
}

impl Question {
    /// Rejects alias, validation, and meta query types. DS and DNSKEY replies
    /// are shared with the chain of trust's own fetches, and aliases are
    /// followed rather than returned, so neither can be a final answer.
    pub fn new(name: Name, rtype: RecordType) -> Result<Question, QuestionError> {
        if matches!(
            rtype.0,
            0 | 5 | 39 | 41 | 43 | 46 | 47 | 48 | 50 | 249..=255
        ) {
            return Err(QuestionError::UnsupportedType(rtype));
        }
        Ok(Self { name, rtype })
    }

    /// The name asked about.
    pub fn name(&self) -> &Name {
        &self.name
    }

    /// The record type asked for.
    pub fn rtype(&self) -> RecordType {
        self.rtype
    }
}

/// One IN-class query for the caller to send.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct Query {
    /// The name to ask about.
    pub name: Name,
    /// The record type to ask for.
    pub rtype: RecordType,
}

/// Seconds since 1970-01-01 UTC, supplied by the caller.
pub type UnixSeconds = u64;

/// The reason DNSSEC proof failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BogusReason {
    /// A reply could not be parsed as a DNS message for the query.
    MalformedReply,
    /// A signed zone's records carry no usable signature.
    SignaturesMissing,
    /// Every signature that could be checked failed cryptographically.
    SignatureInvalid,
    /// A signature is valid but expired, beyond the clock tolerance; the host clock may be wrong.
    SignatureExpired,
    /// A signature is valid but not yet in force, beyond the clock tolerance; the host clock may be wrong.
    SignatureNotYetValid,
    /// No proven key matches a signature's key tag and algorithm.
    NoMatchingKey,
    /// The zone's DNSKEY set could not be proven with its parent's DS records.
    KeysNotProven,
    /// No DNSKEY at a trust anchor's zone matches any configured trust anchor.
    TrustAnchorMismatch,
    /// The server answered without DNSSEC records (no OPT or no signatures) for a signed zone.
    NoDnssecRecords,
    /// A signer name is not a proven zone apex.
    SignerNotAZone,
    /// Signatures name a signer that does not fit the records.
    SignerMismatch,
    /// A delegation's DS presence or absence could not be proven.
    DelegationUnproven,
    /// A signed zone's negative answer has no denial records.
    DenialMissing,
    /// A negative answer's denial records do not prove it.
    DenialInvalid,
    /// A wildcard expansion lacks proof that no closer name exists.
    WildcardUnproven,
    /// A CNAME or DNAME chain is broken, looped or inconsistent.
    AliasInvalid,
    /// The zone's NSEC3 records use more iterations than this validator will compute (RFC 9276).
    Nsec3Iterations,
    /// A query, signature, digest or hash budget ran out.
    WorkLimit,
}

/// Why an answer is not authenticated, without being a failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum InsecureReason {
    /// No configured trust anchor covers the name.
    NoTrustAnchor,
    /// A parent proved there is no DS for a delegation, or an NSEC3 opt-out span covers it.
    UnsignedDelegation,
    /// The zone is signed only with algorithms or DS digests this validator does not use.
    UnsupportedAlgorithm,
    /// The name is under a private or special-use zone with a built-in negative trust anchor.
    PrivateName,
}

/// A completed DNSSEC assessment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Every record and denial in the answer is authenticated.
    Secure(Answer),
    /// An unauthenticated answer; `Answer::insecure` says why.
    Insecure(Answer),
    /// Proof failed; never use the reply.
    Bogus(Bogus),
    /// A needed reply did not arrive or was an error.
    Unanswered(Unanswered),
}

/// The next query required, or a completed assessment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// Send this query and supply its reply, or report that it failed.
    Need(Query),
    /// The assessment is complete.
    Done(Verdict),
}

/// Validated records or an authenticated negative answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Answer {
    /// The records, or which negative answer was given.
    pub outcome: Outcome,
    /// Seconds the answer may be cached, bounded by validated data when secure.
    pub ttl: u32,
    /// Why the answer is insecure; `Some` exactly for `Verdict::Insecure`.
    pub insecure: Option<InsecureReason>,
    // Needed to distinguish address records from other types of the same length.
    pub(crate) rtype: RecordType,
}

/// What the answer says about the question.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Records of the requested type, owned by `name` (the final alias target).
    Records { name: Name, rdata: Vec<Vec<u8>> },
    /// The name exists but has no records of the type.
    NoData,
    /// The name does not exist.
    NoSuchName,
}
impl Answer {
    /// Decode addresses only for the A or AAAA type originally requested.
    pub fn addresses(&self) -> Vec<core::net::IpAddr> {
        use core::net::{IpAddr, Ipv4Addr, Ipv6Addr};
        let Outcome::Records { rdata, .. } = &self.outcome else {
            return Vec::new();
        };
        match self.rtype {
            RecordType::A => rdata
                .iter()
                .filter_map(|bytes| {
                    <[u8; 4]>::try_from(bytes.as_slice())
                        .ok()
                        .map(|octets| IpAddr::V4(Ipv4Addr::from(octets)))
                })
                .collect(),
            RecordType::AAAA => rdata
                .iter()
                .filter_map(|bytes| {
                    <[u8; 16]>::try_from(bytes.as_slice())
                        .ok()
                        .map(|octets| IpAddr::V6(Ipv6Addr::from(octets)))
                })
                .collect(),
            _ => Vec::new(),
        }
    }
}

/// A failed proof.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bogus {
    /// Why the proof failed.
    pub reason: BogusReason,
    /// The owner name whose proof failed.
    pub name: Name,
    /// The record type whose proof failed.
    pub rtype: RecordType,
    /// Extended DNS Error codes (RFC 8914) the replies carried, for logging only.
    pub extended_errors: Vec<u16>,
}

/// A needed reply that did not arrive or was an error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unanswered {
    /// The query without a usable reply.
    pub query: Query,
    /// What went wrong.
    pub cause: UnansweredCause,
    /// Extended DNS Error codes (RFC 8914) the replies carried, for logging only.
    pub extended_errors: Vec<u16>,
}

/// Why a query has no usable reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnansweredCause {
    /// The caller reported the fetch failed.
    FetchFailed,
    /// The reply had the TC bit set.
    Truncated,
    /// The reply's RCODE was neither NOERROR nor NXDOMAIN.
    Rcode(u16),
}

/// A reply was supplied for a query that is not currently outstanding.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SupplyError {
    #[error("the supplied query is not outstanding")]
    UnexpectedQuery,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn question_type_policy_preserves_unknown_types() {
        for number in 0..=u16::MAX {
            let rejected = matches!(number, 0 | 5 | 39 | 41 | 43 | 46 | 47 | 48 | 50 | 249..=255);
            let question = Question::new(Name::root(), RecordType(number));
            assert_eq!(question.is_err(), rejected);
            if let Ok(question) = question {
                assert_eq!(question.name(), &Name::root());
                assert_eq!(question.rtype(), RecordType(number));
            }
        }
    }
}
