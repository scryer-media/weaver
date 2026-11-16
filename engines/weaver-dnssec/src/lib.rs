// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Synchronous DNSSEC validation over caller-supplied DNS messages.
//!
//! The caller supplies transport and time. Validation performs no I/O and
//! requests the additional DNSKEY and DS messages needed to prove an answer.
//! A caller that does not validate reads a reply with [`Reply::read`], by
//! the same parser.
//!
//! # How it works
//!
//! 1. Build one [`Validator`]. It holds the trust anchors, the [`Policy`] and
//!    a bounded cache of proven keys and delegations, and is shared by every
//!    lookup.
//! 2. For each lookup, start a [`Validation`] for a [`Question`].
//! 3. Call [`Validation::step`]. It returns [`Step::Need`] with the
//!    queries to send, or [`Step::Done`] with the [`Verdict`].
//! 4. Send each query (for example with [`Query::encode`]) to the server
//!    being asked. Give each reply to [`Validation::supply`], or call
//!    [`Validation::supply_failure`] when a query gets no reply.
//! 5. Step again until it is done.
//!
//! ```
//! use weaver_dnssec::{Name, Query, Question, RecordType, Step, Validator, Verdict};
//!
//! // A stand-in for the caller's transport. This one never gets a reply.
//! fn send(query: &Query, id: u16) -> Option<Vec<u8>> {
//!     let _packet = query.encode(id);
//!     None
//! }
//!
//! let validator = Validator::with_root_anchors();
//! let name = Name::from_text("www.example.com.").unwrap();
//! let question = Question::new(name, RecordType::A).unwrap();
//! let now = 1_700_000_000; // seconds since 1970, from the caller's clock
//! let mut validation = validator.begin(question, now);
//! let verdict = loop {
//!     match validation.step() {
//!         Step::Done { verdict, .. } => break verdict,
//!         Step::Need { awaited, ahead, .. } => {
//!             for query in std::iter::once(awaited).chain(ahead) {
//!                 match send(&query, 0) {
//!                     Some(reply) => validation.supply(&query, &reply).unwrap(),
//!                     None => validation.supply_failure(&query).unwrap(),
//!                 }
//!             }
//!         }
//!         // A step this code does not know: end the lookup as a failure.
//!         _ => return Err("unknown validation step".to_string()),
//!     }
//! };
//! match verdict {
//!     Verdict::Secure { answer, .. } => println!("proven: {:?}", answer.addresses()),
//!     Verdict::Insecure { answer, reason, .. } => {
//!         println!("not signed ({reason}): {:?}", answer.addresses())
//!     }
//!     Verdict::Bogus { failure, .. } => println!("do not use the reply: {failure}"),
//!     Verdict::Unanswered { failure, .. } => println!("try another server: {failure}"),
//!     _ => println!("treat an unknown verdict as a failure"),
//! }
//! # Ok::<(), String>(())
//! ```
//!
//! # What is trusted
//!
//! [`Validator::with_root_anchors`] trusts the IANA root keys
//! ([`TrustAnchor::iana_root`]). [`Validator::new`] trusts the anchors the
//! caller gives instead. Names under the zones in [`PRIVATE_ZONES`] are
//! treated as insecure unless a configured anchor sits inside that zone.
//! The bounds on work and storage are in [`limits`].
//!
//! # Features
//!
//! Exactly one crypto backend verifies signatures and computes digests. Both
//! accept and reject the same inputs.
//!
//! - `aws-lc-rs` (the default): the aws-lc-rs crate, which builds aws-lc, a C
//!   library.
//! - `rust-crypto`: pure Rust, from the RustCrypto and dalek crates. Build
//!   with `default-features = false, features = ["rust-crypto"]` to leave C
//!   out.
//! - With both enabled, aws-lc-rs is used.
//! - With neither, the crate does not compile.
//! - `test-support`: signing test equipment in the `testing` module. It signs with
//!   aws-lc-rs, so it builds aws-lc under either backend. Running this crate's
//!   own tests always builds aws-lc too, since the tests sign with it. The
//!   `testing` module is for tests only: it is not part of the stable API and
//!   may change in any release.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod algorithm;
mod anchor;
mod cache;
mod canonical;
mod crypto;
mod denial;
mod keys;
#[cfg(test)]
mod ml_dsa_tests;
#[cfg(test)]
mod rfc4035_tests;
mod rrset;
#[cfg(test)]
mod test_vectors;

pub mod limits;
mod name;
mod rdata;
/// Signing test equipment. Not part of the stable API: it may change in any
/// release.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod testing;
mod validate;
mod wire;

pub use anchor::{AnchorError, PRIVATE_ZONES, Policy, TrustAnchor};
pub use name::{Name, NameError};
pub use validate::{Source, Validation, Validator};

/// A DNS record type number. Unknown numbers are preserved.
///
/// ```
/// use weaver_dnssec::RecordType;
///
/// assert_eq!(RecordType::new(28), RecordType::AAAA);
/// assert_eq!(u16::from(RecordType::AAAA), 28);
/// assert_eq!(RecordType::AAAA.to_string(), "AAAA");
/// assert_eq!(RecordType::new(65_280).to_string(), "TYPE65280");
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct RecordType(u16);

impl RecordType {
    /// An IPv4 address (1).
    pub const A: Self = Self(1);
    /// An authoritative name server (2).
    pub const NS: Self = Self(2);
    /// A canonical name alias (5).
    pub const CNAME: Self = Self(5);
    /// The start of a zone of authority (6).
    pub const SOA: Self = Self(6);
    /// A domain name pointer (12).
    pub const PTR: Self = Self(12);
    /// A mail exchange (15).
    pub const MX: Self = Self(15);
    /// Text strings (16).
    pub const TXT: Self = Self(16);
    /// An IPv6 address (28).
    pub const AAAA: Self = Self(28);
    /// A service location (33).
    pub const SRV: Self = Self(33);
    /// A redirection of a whole subtree (39).
    pub const DNAME: Self = Self(39);
    /// The EDNS pseudo-record (41).
    pub const OPT: Self = Self(41);
    /// A delegation signer (43).
    pub const DS: Self = Self(43);
    /// A DNSSEC signature (46).
    pub const RRSIG: Self = Self(46);
    /// A DNSSEC denial of existence (47).
    pub const NSEC: Self = Self(47);
    /// A DNSSEC public key (48).
    pub const DNSKEY: Self = Self(48);
    /// A hashed DNSSEC denial of existence (50).
    pub const NSEC3: Self = Self(50);
    /// A general service binding (64).
    pub const SVCB: Self = Self(64);
    /// An HTTPS service binding (65).
    pub const HTTPS: Self = Self(65);

    /// The record type with this number.
    pub const fn new(number: u16) -> Self {
        Self(number)
    }

    /// This record type's number.
    pub const fn get(self) -> u16 {
        self.0
    }

    fn mnemonic(self) -> Option<&'static str> {
        Some(match self {
            Self::A => "A",
            Self::NS => "NS",
            Self::CNAME => "CNAME",
            Self::SOA => "SOA",
            Self::PTR => "PTR",
            Self::MX => "MX",
            Self::TXT => "TXT",
            Self::AAAA => "AAAA",
            Self::SRV => "SRV",
            Self::DNAME => "DNAME",
            Self::OPT => "OPT",
            Self::DS => "DS",
            Self::RRSIG => "RRSIG",
            Self::NSEC => "NSEC",
            Self::DNSKEY => "DNSKEY",
            Self::NSEC3 => "NSEC3",
            Self::SVCB => "SVCB",
            Self::HTTPS => "HTTPS",
            _ => return None,
        })
    }
}

impl From<u16> for RecordType {
    fn from(number: u16) -> Self {
        Self(number)
    }
}

impl From<RecordType> for u16 {
    fn from(rtype: RecordType) -> Self {
        rtype.0
    }
}

/// The type's mnemonic, such as `AAAA`, or `TYPE` and its number for a type
/// without one here (RFC 3597).
impl core::fmt::Display for RecordType {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.mnemonic() {
            Some(mnemonic) => f.write_str(mnemonic),
            None => write!(f, "TYPE{}", self.0),
        }
    }
}

/// An IN-class question for a final record type.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Question {
    name: Name,
    rtype: RecordType,
}

/// The requested record type is reserved for validation, aliases, or meta queries.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum QuestionError {
    /// This record type is never a final answer.
    #[error("record type {0} cannot be requested as a final answer")]
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
    name: Name,
    rtype: RecordType,
}

impl Query {
    /// A query for `name`'s records of `rtype`.
    pub fn new(name: Name, rtype: RecordType) -> Self {
        Self { name, rtype }
    }

    /// The name to ask about.
    pub fn name(&self) -> &Name {
        &self.name
    }

    /// The record type to ask for.
    pub fn rtype(&self) -> RecordType {
        self.rtype
    }
}

/// The query's name and type, such as `www.example.com. AAAA`.
impl core::fmt::Display for Query {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{} {}", self.name, self.rtype)
    }
}

/// Seconds since 1970-01-01 UTC, supplied by the caller.
pub type UnixSeconds = u64;

/// The reason DNSSEC proof failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, thiserror::Error)]
#[non_exhaustive]
pub enum BogusReason {
    /// A reply could not be parsed as a DNS message for the query.
    #[error("a reply could not be read as a DNS message for its query")]
    MalformedReply,
    /// A signed zone's records carry no usable signature.
    #[error("a signed zone's records carry no usable signature")]
    SignaturesMissing,
    /// Every signature that could be checked failed cryptographically.
    #[error("the signatures do not match the records")]
    SignatureInvalid,
    /// A signature is valid but expired, beyond the clock tolerance; the host clock may be wrong.
    #[error("a signature has expired; the clock may be wrong")]
    SignatureExpired,
    /// A signature is valid but not yet in force, beyond the clock tolerance; the host clock may be wrong.
    #[error("a signature is not valid yet; the clock may be wrong")]
    SignatureNotYetValid,
    /// No proven key matches a signature's key tag and algorithm.
    #[error("no proven key matches a signature")]
    NoMatchingKey,
    /// The zone's DNSKEY set could not be proven with its parent's DS records.
    #[error("a zone's keys could not be proven from its parent's DS records")]
    KeysNotProven,
    /// No DNSKEY at a trust anchor's zone matches any configured trust anchor.
    #[error("no key at a trust anchor's zone matches a configured trust anchor")]
    TrustAnchorMismatch,
    /// The server answered without DNSSEC records (no OPT or no signatures) for a signed zone.
    #[error("the server answered without DNSSEC records for a signed zone")]
    NoDnssecRecords,
    /// A signer name is not a proven zone apex.
    #[error("a signer name is not a proven zone")]
    SignerNotAZone,
    /// Signatures name a signer that does not fit the records.
    #[error("a signature's signer does not fit the records")]
    SignerMismatch,
    /// A delegation's DS presence or absence could not be proven.
    #[error("a delegation could not be proven signed or unsigned")]
    DelegationUnproven,
    /// A signed zone's negative answer has no denial records.
    #[error("a signed zone's negative answer carries no proof")]
    DenialMissing,
    /// A negative answer's denial records do not prove it.
    #[error("a negative answer's proof does not hold")]
    DenialInvalid,
    /// A wildcard expansion lacks proof that no closer name exists.
    #[error("a wildcard answer lacks proof that no closer name exists")]
    WildcardUnproven,
    /// A CNAME or DNAME chain is broken, looped or inconsistent.
    #[error("a CNAME or DNAME chain is broken, loops or disagrees")]
    AliasInvalid,
    /// The zone's NSEC3 records use more iterations than this validator will compute (RFC 9276).
    #[error("a zone's NSEC3 records use more hash iterations than this validator computes")]
    Nsec3Iterations,
    /// A query, signature, digest or hash budget ran out. The budgets are in
    /// [`limits`].
    #[error("the validation needed more queries, signatures, digests or hashes than allowed")]
    WorkLimit,
}

/// Why an answer is not authenticated, without being a failure.
///
/// It implements [`std::error::Error`] for a caller that refuses insecure
/// answers and reports why.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, thiserror::Error)]
#[non_exhaustive]
pub enum InsecureReason {
    /// No configured trust anchor covers the name.
    #[error("no configured trust anchor covers the name")]
    NoTrustAnchor,
    /// A parent proved there is no DS for a delegation, or an NSEC3 opt-out span covers it.
    #[error("the zone is proven to be unsigned")]
    UnsignedDelegation,
    /// The zone is signed only with algorithms or DS digests this validator does not use.
    #[error("the zone is signed only with algorithms or digests this validator does not check")]
    UnsupportedAlgorithm,
    /// The name is under a private or special-use zone with a built-in
    /// negative trust anchor. The zones are listed in [`PRIVATE_ZONES`].
    #[error("the name is under a private or special-use zone")]
    PrivateName,
}

/// A completed DNSSEC assessment.
///
/// Match it with `..` in each variant and a wildcard arm: later releases may
/// add fields and variants. Treat an unknown variant as a failure.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Verdict {
    /// Every record and denial in the answer is authenticated.
    #[non_exhaustive]
    Secure {
        /// The proven answer.
        answer: Answer,
    },
    /// An answer that cannot be authenticated, without being a failure.
    #[non_exhaustive]
    Insecure {
        /// The unauthenticated answer.
        answer: Answer,
        /// Why it cannot be authenticated.
        reason: InsecureReason,
    },
    /// Proof failed; never use the reply.
    #[non_exhaustive]
    Bogus {
        /// What failed, and why.
        failure: Bogus,
    },
    /// A needed reply did not arrive or was an error.
    #[non_exhaustive]
    Unanswered {
        /// Which query went without a usable reply, and why.
        failure: Unanswered,
    },
}

impl Verdict {
    /// The answer of a secure or insecure verdict.
    pub fn answer(&self) -> Option<&Answer> {
        match self {
            Self::Secure { answer } | Self::Insecure { answer, .. } => Some(answer),
            Self::Bogus { .. } | Self::Unanswered { .. } => None,
        }
    }
}

/// What the validation needs next, or its verdict.
///
/// Match it with `..` in each variant and a wildcard arm: later releases may
/// add fields and variants.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Step {
    /// Send these queries and supply each reply, or call
    /// [`Validation::supply_failure`] for one that gets none.
    #[non_exhaustive]
    Need {
        /// The query the validation cannot go on without. Step again once
        /// its reply is supplied.
        awaited: Query,
        /// Queries the validation expects to need after `awaited`. They may
        /// be sent at the same time, and their replies supplied in any
        /// order. Each is handed out once and counts toward
        /// [`limits::MAX_QUERIES`]; empty after
        /// [`Validation::set_ask_ahead`]`(false)`.
        ahead: Vec<Query>,
    },
    /// The assessment is complete. Further steps return the same verdict.
    #[non_exhaustive]
    Done {
        /// The verdict.
        verdict: Verdict,
    },
}

/// Records or a negative answer, from a secure or an insecure verdict.
///
/// In a [`Verdict::Secure`] verdict the answer was validated. In a
/// [`Verdict::Insecure`] verdict it was not validated; the verdict's
/// [`InsecureReason`] says why. Such an answer is only as good as the
/// server that sent it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Answer {
    pub(crate) outcome: Outcome,
    pub(crate) ttl: u32,
    // The question's name, then each alias target followed, in order.
    pub(crate) alias_chain: Vec<Name>,
    // Needed to distinguish address records from other types of the same length.
    pub(crate) rtype: RecordType,
}

/// What the answer says about the question.
///
/// Match it with `..` in `Records` and a wildcard arm: later releases may
/// add fields and variants.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Outcome {
    /// Records of the requested type.
    #[non_exhaustive]
    Records {
        /// The records' owner: the question's name, or the last alias
        /// target followed.
        name: Name,
        /// Each record's data, uncompressed, in the order received.
        rdata: Vec<Vec<u8>>,
    },
    /// The name exists but has no records of the type.
    NoData,
    /// The name does not exist.
    NoSuchName,
}

impl Answer {
    /// The records, or which negative answer was given.
    pub fn outcome(&self) -> &Outcome {
        &self.outcome
    }

    /// Seconds the answer may be cached. For a secure answer it is bounded
    /// by the validated records and signatures.
    pub fn ttl(&self) -> u32 {
        self.ttl
    }

    /// The record type the question asked for.
    pub fn rtype(&self) -> RecordType {
        self.rtype
    }

    /// The names followed to reach the answer, in order: the question's name
    /// first, then each CNAME or DNAME target. One name when no alias was
    /// followed. The last name owns the records or the negative answer.
    pub fn alias_chain(&self) -> &[Name] {
        &self.alias_chain
    }

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

/// One reply read as the server sent it, with nothing validated, for a
/// caller that does not validate. Made by [`Reply::read`].
///
/// ```
/// use weaver_dnssec::{Name, Query, RecordType, Reply};
///
/// let name = Name::from_text("www.example.").unwrap();
/// let query = Query::new(name, RecordType::A);
/// // A NOERROR reply with one A record, owned by the question's name.
/// let mut message = vec![0, 1, 0x81, 0x80, 0, 1, 0, 1, 0, 0, 0, 0];
/// message.extend_from_slice(b"\x03www\x07example\0\0\x01\0\x01");
/// message.extend_from_slice(&[0xc0, 12, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4, 192, 0, 2, 1]);
/// let reply = Reply::read(&query, &message).unwrap();
/// assert_eq!(reply.answer().addresses(), ["192.0.2.1".parse::<std::net::IpAddr>().unwrap()]);
/// assert_eq!(reply.answer().ttl(), 60);
/// assert_eq!(reply.alias(), None);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reply {
    pub(crate) answer: Answer,
    pub(crate) rcode: u16,
    pub(crate) negative_ttl: Option<u32>,
}

impl Reply {
    /// What the reply says about the query, as [`Reply::read`] describes.
    pub fn answer(&self) -> &Answer {
        &self.answer
    }

    /// The header's response code: 0 (NOERROR) or 3 (NXDOMAIN). A reply
    /// with any other is [`ReplyError::Rcode`].
    pub fn rcode(&self) -> u16 {
        self.rcode
    }

    /// The name to ask about next: under NOERROR, the last name of the
    /// alias chain, when an alias was followed and the reply holds no
    /// records of the type there. None otherwise.
    pub fn alias(&self) -> Option<&Name> {
        let followed = self.rcode == 0
            && self.answer.alias_chain.len() > 1
            && !matches!(self.answer.outcome, Outcome::Records { .. });
        followed.then(|| self.answer.alias_chain.last()).flatten()
    }

    /// How long the reply's lack of usable records holds, when it says: the
    /// lower of the aliases' TTLs and the hold of the first SOA, in
    /// received order, owned by the answer's last name or a zone above it.
    /// That hold is the lower of the SOA's TTL and its minimum (RFC 2308
    /// section 5). None when there is no such SOA, or the first is short of
    /// its minimum; a later SOA is not taken in its place. This is
    /// dnsmasq's rule.
    pub fn negative_ttl(&self) -> Option<u32> {
        self.negative_ttl
    }
}

/// Why a reply could not be read as an answer to its query.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, thiserror::Error)]
#[non_exhaustive]
pub enum ReplyError {
    /// The reply is not a well-formed DNS message answering the query, or
    /// its aliases loop or run past
    /// [`MAX_ALIAS_LINKS`](limits::MAX_ALIAS_LINKS). This is so whatever
    /// its response code says.
    #[error("the reply could not be read as a DNS message for its query")]
    Malformed,
    /// The reply had the TC bit set.
    #[error("the reply was truncated")]
    Truncated,
    /// The header's response code was neither NOERROR nor NXDOMAIN.
    #[error("the server answered with response code {0}")]
    Rcode(u16),
    /// The query asked for RRSIG or OPT records, which are never read as
    /// an answer.
    #[error("record type {0} cannot be read as an answer")]
    UnsupportedType(RecordType),
}

/// A failed proof.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bogus {
    pub(crate) reason: BogusReason,
    pub(crate) name: Name,
    pub(crate) rtype: RecordType,
    pub(crate) extended_errors: Vec<u16>,
}

impl Bogus {
    /// Why the proof failed.
    pub fn reason(&self) -> BogusReason {
        self.reason
    }

    /// The owner name whose proof failed.
    pub fn name(&self) -> &Name {
        &self.name
    }

    /// The record type whose proof failed.
    pub fn rtype(&self) -> RecordType {
        self.rtype
    }

    /// Extended DNS Error codes (RFC 8914) the replies carried, in the order
    /// first seen, for logging only. At most [`limits::MAX_EXTENDED_ERRORS`].
    pub fn extended_errors(&self) -> &[u16] {
        &self.extended_errors
    }
}

/// Reads as `DNSSEC validation failed for <name> <type>: <reason>`, with any
/// Extended DNS Error codes as numbers. The name is escaped as in
/// [`Name`]'s `Display`.
impl core::fmt::Display for Bogus {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "DNSSEC validation failed for {} {}: {}",
            self.name, self.rtype, self.reason
        )?;
        write_extended_errors(f, &self.extended_errors)
    }
}

impl std::error::Error for Bogus {}

/// A needed reply that did not arrive or was an error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unanswered {
    pub(crate) query: Query,
    pub(crate) cause: UnansweredCause,
    pub(crate) extended_errors: Vec<u16>,
}

impl Unanswered {
    /// The query without a usable reply.
    pub fn query(&self) -> &Query {
        &self.query
    }

    /// What went wrong.
    pub fn cause(&self) -> UnansweredCause {
        self.cause
    }

    /// Extended DNS Error codes (RFC 8914) the replies carried, in the order
    /// first seen, for logging only. At most [`limits::MAX_EXTENDED_ERRORS`].
    pub fn extended_errors(&self) -> &[u16] {
        &self.extended_errors
    }
}

/// Reads as `no usable reply to <name> <type>: <cause>`, with any Extended
/// DNS Error codes as numbers. The name is escaped as in [`Name`]'s
/// `Display`.
impl core::fmt::Display for Unanswered {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "no usable reply to {}: {}", self.query, self.cause)?;
        write_extended_errors(f, &self.extended_errors)
    }
}

impl std::error::Error for Unanswered {}

fn write_extended_errors(f: &mut core::fmt::Formatter<'_>, codes: &[u16]) -> core::fmt::Result {
    for (at, code) in codes.iter().enumerate() {
        let lead = if at == 0 {
            " (Extended DNS Error codes "
        } else {
            ", "
        };
        write!(f, "{lead}{code}")?;
    }
    if codes.is_empty() {
        Ok(())
    } else {
        f.write_str(")")
    }
}

/// Why a query has no usable reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, thiserror::Error)]
#[non_exhaustive]
pub enum UnansweredCause {
    /// The caller called [`Validation::supply_failure`] for the query.
    #[error("the query got no reply")]
    FetchFailed,
    /// The reply had the TC bit set. Retry the query over TCP and supply
    /// that reply instead: signed replies often exceed the 1232-byte buffer
    /// that [`Query::encode`] advertises, mostly for RSA and ML-DSA keys.
    #[error("the reply was truncated")]
    Truncated,
    /// The reply's RCODE was neither NOERROR nor NXDOMAIN.
    #[error("the server answered with response code {0}")]
    Rcode(u16),
}

/// A reply or failure was supplied for a query that is not currently outstanding.
#[derive(Clone, Debug, PartialEq, Eq, Hash, thiserror::Error)]
#[non_exhaustive]
pub enum SupplyError {
    /// The validation did not hand out this query, already has its reply,
    /// or is done.
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
            let question = Question::new(Name::root(), RecordType::new(number));
            assert_eq!(question.is_err(), rejected);
            if let Ok(question) = question {
                assert_eq!(question.name(), &Name::root());
                assert_eq!(question.rtype(), RecordType::new(number));
            }
        }
    }

    #[test]
    fn public_values_are_send_and_sync() {
        fn send<T: Send>() {}
        fn send_sync<T: Send + Sync>() {}
        send_sync::<Validator>();
        send::<Validation<'_>>();
        send_sync::<Verdict>();
        send_sync::<Step>();
        send_sync::<Outcome>();
        send_sync::<Answer>();
        send_sync::<Bogus>();
        send_sync::<Unanswered>();
        send_sync::<BogusReason>();
        send_sync::<InsecureReason>();
        send_sync::<UnansweredCause>();
        send_sync::<SupplyError>();
        send_sync::<Reply>();
        send_sync::<ReplyError>();
        send_sync::<QuestionError>();
        send_sync::<AnchorError>();
        send_sync::<NameError>();
        send_sync::<TrustAnchor>();
        send_sync::<Policy>();
        send_sync::<Source>();
        send_sync::<Query>();
        send_sync::<Question>();
        send_sync::<Name>();
    }

    #[test]
    fn errors_and_reasons_read_in_plain_words_and_escape_names() {
        fn error<T: std::error::Error>(_: &T) {}
        let name = Name::from_wire(b"\x05a\x07b\n\x00\x00").unwrap();
        let bogus = Bogus {
            reason: BogusReason::SignatureExpired,
            name: name.clone(),
            rtype: RecordType::AAAA,
            extended_errors: vec![7, 9],
        };
        error(&bogus);
        assert_eq!(
            bogus.to_string(),
            "DNSSEC validation failed for a\\007b\\010\\000. AAAA: a signature has \
             expired; the clock may be wrong (Extended DNS Error codes 7, 9)"
        );
        let unanswered = Unanswered {
            query: Query::new(name, RecordType::new(65_280)),
            cause: UnansweredCause::Rcode(2),
            extended_errors: Vec::new(),
        };
        error(&unanswered);
        assert_eq!(
            unanswered.to_string(),
            "no usable reply to a\\007b\\010\\000. TYPE65280: the server answered with \
             response code 2"
        );
        error(&InsecureReason::PrivateName);
        error(&UnansweredCause::FetchFailed);
        error(&SupplyError::UnexpectedQuery);
        error(&ReplyError::Malformed);
        assert_eq!(
            ReplyError::Rcode(2).to_string(),
            "the server answered with response code 2"
        );
        assert_eq!(
            QuestionError::UnsupportedType(RecordType::CNAME).to_string(),
            "record type CNAME cannot be requested as a final answer"
        );
    }
}
