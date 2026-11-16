// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The bounds on work and storage.
//!
//! Each bound is fixed for a release and is not configurable. A later
//! release may tune one. That changes behaviour, but it does not break the
//! build.
//!
//! A validation that would pass a work bound (queries, signature checks,
//! digests, hashes, alias links) ends [`Bogus`](crate::Verdict::Bogus) with
//! [`BogusReason::WorkLimit`](crate::BogusReason::WorkLimit). NSEC3 records
//! over [`MAX_NSEC3_ITERATIONS`] are skipped. The validation ends with
//! [`BogusReason::Nsec3Iterations`](crate::BogusReason::Nsec3Iterations)
//! only when the remaining records do not prove the answer. A
//! reply over a size bound is not read, and ends it with
//! [`BogusReason::MalformedReply`](crate::BogusReason::MalformedReply).
//! Records past a "kept" bound are left out without failing.
//!
//! There is no separate bound on the depth of the chain of trust: a name has
//! at most 127 labels, and every key and delegation fetched counts toward
//! [`MAX_QUERIES`].

/// The longest reply read, in bytes: the DNS over TCP length field's
/// maximum.
pub const MAX_MESSAGE_BYTES: usize = 65_535;
/// The most compression pointers followed while reading one name, as
/// dnsmasq's 255 (unbound allows 256). A name that needs more makes the
/// reply unreadable.
pub const MAX_POINTER_HOPS: usize = 255;
/// The most queries one validation sends, of every kind together, as
/// dnsmasq's LIMIT_WORK.
pub const MAX_QUERIES: usize = 40;
/// The most CNAME and DNAME links one answer follows.
pub const MAX_ALIAS_LINKS: usize = MAX_SIGNATURE_CHECKS;
/// The most failed signature checks for one record set, as dnsmasq's
/// LIMIT_SIG_FAIL.
pub const MAX_SIGNATURE_CHECKS_PER_RRSET: usize = 20;
/// The most signature checks made in one validation, as dnsmasq's
/// LIMIT_CRYPTO.
pub const MAX_SIGNATURE_CHECKS: usize = 200;
/// The most keys kept for one key tag and algorithm.
pub const MAX_KEY_TAG_MATCHES: usize = 16;
/// The most DS digests computed in one validation, as unbound's
/// val-hash-attempts.
pub const MAX_DS_DIGESTS: usize = 32;
/// The most records in one DNSKEY record set.
pub const MAX_DNSKEYS: usize = 64;
/// The most records in one record set.
pub const MAX_RRSET_RECORDS: usize = 512;
/// The most signatures kept for one record set.
pub const MAX_RRSIGS_PER_RRSET: usize = 32;
/// The most NSEC3 hash iterations computed, as unbound and RFC 9276
/// section 3.2. An NSEC3 record with more is skipped. The result is
/// [`BogusReason::Nsec3Iterations`](crate::BogusReason::Nsec3Iterations)
/// only when the remaining records do not prove the answer.
pub const MAX_NSEC3_ITERATIONS: u16 = 150;
/// The most NSEC3 hashes computed in one validation.
pub const MAX_NSEC3_HASHES: usize = 64;
/// The most denial record sets kept from one reply.
pub const MAX_DENIAL_RRSETS: usize = 32;
/// The longest a proven key or delegation fact is cached, in seconds. It is
/// also bounded by the records' TTLs and signatures.
pub const MAX_TRUST_TTL_SECONDS: u32 = 3_600;
/// The most proven keys and delegation facts the cache holds.
pub const MAX_CACHE_ENTRIES: usize = 512;
/// The least clock tolerance on a signature's validity window, in seconds,
/// as unbound's val-sig-skew-min.
pub const MIN_CLOCK_SKEW_SECONDS: u32 = 3_600;
/// The most clock tolerance on a signature's validity window, in seconds, as
/// unbound's val-sig-skew-max.
pub const MAX_CLOCK_SKEW_SECONDS: u32 = 86_400;
/// The most Extended DNS Error codes kept from one reply, and for one
/// validation.
pub const MAX_EXTENDED_ERRORS: usize = 8;
/// How long a name and type proven bogus from one source is held, in
/// seconds, as unbound's val-bogus-ttl.
pub const BOGUS_HOLD_SECONDS: u32 = 60;
/// The most bogus names and types held, over all sources.
pub const MAX_BOGUS_HOLDS: usize = 512;
