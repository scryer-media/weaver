// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! In-memory DNSSEC test equipment. Keys are generated; all time is supplied.
//! The signing encoder deliberately does not use the validator's canonicalizer.

mod signer;
mod zone;

pub use signer::{EquipmentError, KeyAlgorithm, Record, SigningKey, base32hex, nsec3_hash};
pub use zone::{DenialMode, FakeResolver, Reply, SignatureWindow, Zone};

/// Number of signature verification attempts made by this validation.
pub fn signature_checks(validation: &crate::Validation<'_>) -> usize {
    validation.signature_checks()
}

/// One signed RRset in canonical form: lower-cased owner, type and record data.
pub type SignedRrset = (Vec<u8>, crate::RecordType, Vec<Vec<u8>>);

/// The records a reply's signatures cover, in canonical form: each answer and
/// authority RRset that carries an RRSIG, as its lower-cased owner, type and
/// canonical record data, without TTLs or signatures. `None` when the reply
/// does not parse.
pub fn signed_records(reply: &[u8], query: &crate::Query) -> Option<Vec<SignedRrset>> {
    let message = crate::wire::Message::parse_any(reply, query).ok()?;
    Some(
        message
            .answer
            .iter()
            .chain(&message.authority)
            .filter(|set| !set.rrsigs.is_empty())
            .map(|set| {
                (
                    set.owner.as_wire().to_ascii_lowercase(),
                    set.rtype,
                    set.records
                        .iter()
                        .map(|record| record.canonical.clone())
                        .collect(),
                )
            })
            .collect(),
    )
}

/// Run the public, synchronous validation protocol against an in-memory resolver.
pub fn drive(
    validator: &crate::Validator,
    resolver: &FakeResolver,
    question: crate::Question,
    now: crate::UnixSeconds,
) -> (crate::Verdict, Vec<crate::Query>) {
    let mut validation = validator.begin(question, now);
    let mut queries = Vec::new();
    loop {
        match validation.step() {
            crate::Step::Done(verdict) => return (verdict, queries),
            crate::Step::Need(query) => {
                queries.push(query.clone());
                match resolver.answer(&query) {
                    Some(bytes) => {
                        // The query came from this same outstanding validation.
                        let _ = validation.supply(&query, &bytes);
                    }
                    None => {
                        let _ = validation.failed(&query);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
