// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Work and storage bounds from the DNSSEC handoff, section 5.

/// DNS over TCP message length field.
pub(crate) const MAX_MESSAGE_BYTES: usize = 65_535;
/// Local compression traversal guard.
pub(crate) const MAX_POINTER_HOPS: usize = 32;
/// dnsmasq LIMIT_WORK; shared by all query kinds.
pub(crate) const MAX_QUERIES: usize = 40;
/// Local alias traversal bound, aligned with the shared cryptographic work bound.
pub(crate) const MAX_ALIAS_LINKS: usize = MAX_SIGNATURE_CHECKS;
/// dnsmasq LIMIT_SIG_FAIL; the most failed verification attempts per RRset.
pub(crate) const MAX_SIGNATURE_CHECKS_PER_RRSET: usize = 20;
/// dnsmasq LIMIT_CRYPTO; bounds all verification attempts in one validation.
pub(crate) const MAX_SIGNATURE_CHECKS: usize = 200;
/// Local matching-key bound.
pub(crate) const MAX_KEY_TAG_MATCHES: usize = 16;
/// unbound val-hash-attempts.
pub(crate) const MAX_DS_DIGESTS: usize = 32;
/// Local DNSKEY RRset record bound.
pub(crate) const MAX_DNSKEYS: usize = 64;
/// Local RRset record bound.
pub(crate) const MAX_RRSET_RECORDS: usize = 512;
/// Local retained signature bound per RRset.
pub(crate) const MAX_RRSIGS_PER_RRSET: usize = 32;
/// unbound and RFC 9276 section 3.2.
pub(crate) const MAX_NSEC3_ITERATIONS: u16 = 150;
/// Local whole-validation hash budget; unbound permits eight per resumable pass.
pub(crate) const MAX_NSEC3_HASHES: usize = 64;
/// Local denial RRset examination bound per reply.
pub(crate) const MAX_DENIAL_RRSETS: usize = 32;
/// Local trust-cache lifetime bound.
pub(crate) const MAX_TRUST_TTL_SECONDS: u32 = 3_600;
/// Local trust-cache entry bound.
pub(crate) const MAX_CACHE_ENTRIES: usize = 512;
/// unbound val-sig-skew-min: the least clock tolerance on a signature.
pub(crate) const MIN_CLOCK_SKEW_SECONDS: u32 = 3_600;
/// unbound val-sig-skew-max: the most clock tolerance on a signature.
pub(crate) const MAX_CLOCK_SKEW_SECONDS: u32 = 86_400;
/// Local bound on Extended DNS Error codes kept per reply.
pub(crate) const MAX_EXTENDED_ERRORS: usize = 8;
/// How long a proven-bogus name and type is held, as unbound's val-bogus-ttl.
pub(crate) const BOGUS_HOLD_SECONDS: u32 = 60;
/// Local bound on held bogus names and types.
pub(crate) const MAX_BOGUS_HOLDS: usize = 512;
