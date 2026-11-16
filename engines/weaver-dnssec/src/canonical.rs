// SPDX-License-Identifier: MIT OR Apache-2.0
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Reconstruction of the octets covered by an RRSIG.

use crate::rdata::Rrsig;
use crate::wire::Rrset;
use crate::{BogusReason, Name};

pub(crate) struct SignedData {
    pub(crate) bytes: Vec<u8>,
    pub(crate) wildcard: Option<Name>,
}

/// RFC 4034 section 3.1.3 excludes an initial wildcard label.
pub(crate) fn signing_labels(owner: &Name) -> usize {
    owner
        .label_count()
        .saturating_sub(usize::from(owner.as_wire().starts_with(&[1, b'*'])))
}

/// RFC 4034 section 3.1.8.1 and RFC 4035 section 5.3.2.
/// Called once per signature, outside the candidate-key loop.
pub(crate) fn signed_data(set: &Rrset, signature: &Rrsig) -> Result<SignedData, BogusReason> {
    let labels = usize::from(signature.labels);
    let count = signing_labels(&set.owner);
    if labels > count {
        return Err(BogusReason::SignaturesMissing);
    }
    let (owner, wildcard) = if labels < count {
        let closest = set
            .owner
            .suffix(labels)
            .ok_or(BogusReason::MalformedReply)?;
        let mut wire = vec![1, b'*'];
        wire.extend_from_slice(closest.as_wire());
        let owner = Name::from_wire(&wire).map_err(|_| BogusReason::MalformedReply)?;
        (owner.to_lowercase(), Some(closest))
    } else {
        (set.owner.to_lowercase(), None)
    };

    let mut bytes = Vec::new();
    bytes.extend_from_slice(&signature.type_covered.to_be_bytes());
    bytes.extend_from_slice(&[signature.algorithm, signature.labels]);
    bytes.extend_from_slice(&signature.original_ttl.to_be_bytes());
    bytes.extend_from_slice(&signature.expiration.to_be_bytes());
    bytes.extend_from_slice(&signature.inception.to_be_bytes());
    bytes.extend_from_slice(&signature.key_tag.to_be_bytes());
    bytes.extend_from_slice(signature.signer.to_lowercase().as_wire());

    let mut records: Vec<_> = set.records.iter().map(|record| &record.canonical).collect();
    records.sort_unstable();
    for record in records {
        let length = u16::try_from(record.len()).map_err(|_| BogusReason::MalformedReply)?;
        bytes.extend_from_slice(owner.as_wire());
        bytes.extend_from_slice(&set.rtype.0.to_be_bytes());
        bytes.extend_from_slice(&1_u16.to_be_bytes());
        bytes.extend_from_slice(&signature.original_ttl.to_be_bytes());
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.extend_from_slice(record);
    }
    Ok(SignedData { bytes, wildcard })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_vectors;

    // All records and signature fields start with rfc6605.txt lines 214..219;
    // case, owner, label count and RDATA order are deliberate mutations.
    #[test]
    fn case_and_received_ttl_do_not_change_the_signed_bytes() {
        let (_, _, mut set) = test_vectors::p256();
        let original = signed_data(&set, &set.rrsigs[0]).unwrap().bytes;
        set.owner = Name::from_text("WWW.EXAMPLE.NET.").unwrap();
        set.rrsigs[0].signer = Name::from_text("EXAMPLE.NET.").unwrap();
        set.ttl = 1;
        assert_eq!(signed_data(&set, &set.rrsigs[0]).unwrap().bytes, original);
        let signature = set.rrsigs[0].clone();
        set.rrsigs[0].signature[0] ^= 1;
        assert_eq!(signed_data(&set, &set.rrsigs[0]).unwrap().bytes, original);
        assert_eq!(
            original.len(),
            18 + signature.signer.as_wire().len() + set.owner.as_wire().len() + 10 + 4
        );
    }

    #[test]
    fn wildcard_expansion_reconstructs_the_signed_owner_and_closest_encloser() {
        let (_, _, mut set) = test_vectors::p256();
        set.owner = Name::from_text("*.example.net.").unwrap();
        set.rrsigs[0].labels = 2;
        assert_eq!(signing_labels(&set.owner), 2);
        let literal = signed_data(&set, &set.rrsigs[0]).unwrap();
        assert_eq!(literal.wildcard, None);
        for owner in ["www.example.net.", "www.www.example.net."] {
            set.owner = Name::from_text(owner).unwrap();
            let expanded = signed_data(&set, &set.rrsigs[0]).unwrap();
            assert_eq!(expanded.bytes, literal.bytes);
            assert_eq!(
                expanded.wildcard,
                Some(Name::from_text("example.net.").unwrap())
            );
        }
        set.rrsigs[0].labels = 5;
        assert!(signed_data(&set, &set.rrsigs[0]).is_err());
        assert_eq!(signing_labels(&Name::root()), 0);
    }

    #[test]
    fn rdata_sorts_as_unsigned_octets_with_a_shorter_prefix_first() {
        let (_, _, mut set) = test_vectors::p256();
        let mut smaller = set.records[0].clone();
        smaller.canonical[3] ^= 1;
        let mut shorter = smaller.clone();
        shorter.canonical.pop();
        set.records.extend([smaller.clone(), shorter.clone()]);
        let bytes = signed_data(&set, &set.rrsigs[0]).unwrap().bytes;
        set.records.reverse();
        assert_eq!(signed_data(&set, &set.rrsigs[0]).unwrap().bytes, bytes);
        let prefix = 18 + set.rrsigs[0].signer.as_wire().len();
        let fixed = set.owner.as_wire().len() + 8;
        let mut offset = prefix + fixed;
        for expected in [
            &shorter.canonical,
            &smaller.canonical,
            &set.records[2].canonical,
        ] {
            let length = u16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());
            assert_eq!(usize::from(length), expected.len());
            offset += 2;
            assert_eq!(&bytes[offset..offset + expected.len()], expected);
            offset += expected.len() + fixed;
        }
    }
}
