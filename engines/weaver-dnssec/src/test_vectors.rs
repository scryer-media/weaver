// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Published RFC fixtures and explicitly identified independent corrections.
//! All parsing and decoders in this module exist only in unit-test builds.

use std::collections::{BTreeMap, BTreeSet};

use crate::rdata::{self, Dnskey, Ds, RdataKind};
use crate::wire::Rrset;
use crate::{Name, RecordType};

pub(crate) fn hex(text: &str) -> Vec<u8> {
    let text: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    assert_eq!(text.len() % 2, 0);
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(core::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

pub(crate) fn base64(text: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let mut bits = 0_u32;
    let mut accumulator = 0_u32;
    let mut padding = false;
    for byte in text.bytes().filter(|b| !b.is_ascii_whitespace()) {
        if byte == b'=' {
            padding = true;
            continue;
        }
        assert!(!padding);
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => panic!("invalid fixture base64"),
        };
        accumulator = (accumulator << 6) | u32::from(value);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((accumulator >> bits) as u8);
            accumulator &= (1 << bits) - 1;
        }
    }
    assert_eq!(accumulator, 0);
    assert!(bits < 6);
    out
}

fn rtype(text: &str) -> u16 {
    match text {
        "A" => 1,
        "NS" => 2,
        "CNAME" => 5,
        "SOA" => 6,
        "PTR" => 12,
        "MX" => 15,
        "TXT" => 16,
        "AAAA" => 28,
        "DNAME" => 39,
        "DS" => 43,
        "RRSIG" => 46,
        "NSEC" => 47,
        "DNSKEY" => 48,
        "NSEC3" => 50,
        text => text.strip_prefix("TYPE").unwrap().parse().unwrap(),
    }
}

fn timestamp(text: &str) -> u32 {
    if text.len() != 14 {
        return text.parse().unwrap();
    }
    let year: u32 = text[..4].parse().unwrap();
    let month: usize = text[4..6].parse().unwrap();
    let day: u32 = text[6..8].parse().unwrap();
    let leap = |year: u32| {
        year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
    };
    let mut days = (1970..year)
        .map(|year| if leap(year) { 366 } else { 365 })
        .sum::<u32>();
    let months = [
        31,
        if leap(year) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    days += months[..month - 1].iter().sum::<u32>() + day - 1;
    days * 86400
        + text[8..10].parse::<u32>().unwrap() * 3600
        + text[10..12].parse::<u32>().unwrap() * 60
        + text[12..].parse::<u32>().unwrap()
}

/// Parse only complete fixture records; RFC prose and page headers are omitted.
pub(crate) fn parse(text: &str) -> Vec<Rrset> {
    let text = text
        .lines()
        .map(|line| line.split(';').next().unwrap())
        .collect::<Vec<_>>()
        .join(" ");
    let text = text.replace(['(', ')'], " ");
    let tokens: Vec<_> = text.split_whitespace().collect();
    let mut offset = 0;
    let mut sets: BTreeMap<(Name, RecordType), Rrset> = BTreeMap::new();
    let mut seen: BTreeSet<(Name, u16, Vec<u8>)> = BTreeSet::new();
    let mut signatures = Vec::new();
    while offset < tokens.len() {
        let owner = Name::from_text(tokens[offset]).unwrap();
        let ttl: u32 = tokens[offset + 1].parse().unwrap();
        assert_eq!(tokens[offset + 2], "IN");
        let kind = rtype(tokens[offset + 3]);
        let start = offset + 4;
        let end = (start..tokens.len())
            .find(|i| tokens.get(i + 2) == Some(&"IN"))
            .unwrap_or(tokens.len());
        let body = &tokens[start..end];
        let mut bytes = Vec::new();
        match kind {
            1 => bytes.extend(body[0].parse::<std::net::Ipv4Addr>().unwrap().octets()),
            28 => bytes.extend(body[0].parse::<std::net::Ipv6Addr>().unwrap().octets()),
            2 | 5 | 12 | 39 => bytes.extend(Name::from_text(body[0]).unwrap().as_wire()),
            15 => {
                bytes.extend(body[0].parse::<u16>().unwrap().to_be_bytes());
                bytes.extend(Name::from_text(body[1]).unwrap().as_wire());
            }
            48 => {
                bytes.extend(body[0].parse::<u16>().unwrap().to_be_bytes());
                bytes.push(body[1].parse().unwrap());
                bytes.push(body[2].parse().unwrap());
                bytes.extend(base64(&body[3..].join("")));
            }
            43 => {
                bytes.extend(body[0].parse::<u16>().unwrap().to_be_bytes());
                bytes.push(body[1].parse().unwrap());
                bytes.push(body[2].parse().unwrap());
                bytes.extend(hex(&body[3..].join("")));
            }
            46 => {
                bytes.extend(rtype(body[0]).to_be_bytes());
                bytes.push(body[1].parse().unwrap());
                bytes.push(body[2].parse().unwrap());
                bytes.extend(body[3].parse::<u32>().unwrap().to_be_bytes());
                bytes.extend(timestamp(body[4]).to_be_bytes());
                bytes.extend(timestamp(body[5]).to_be_bytes());
                bytes.extend(body[6].parse::<u16>().unwrap().to_be_bytes());
                bytes.extend(Name::from_text(body[7]).unwrap().as_wire());
                bytes.extend(base64(&body[8..].join("")));
            }
            _ => panic!("fixture RDATA not supported"),
        }
        let data = rdata::parse(&bytes, 0, bytes.len(), kind).unwrap();
        if let RdataKind::Rrsig(sig) = data.kind {
            signatures.push((owner, sig));
        } else {
            let unique = seen.insert((owner.clone(), kind, data.canonical.clone()));
            let set = sets
                .entry((owner.clone(), RecordType(kind)))
                .or_insert_with(|| Rrset {
                    owner,
                    rtype: RecordType(kind),
                    ttl,
                    records: vec![],
                    rrsigs: vec![],
                });
            set.ttl = set.ttl.min(ttl);
            if unique {
                set.records.push(data);
            }
        }
        offset = end;
    }
    for (owner, sig) in signatures {
        sets.get_mut(&(owner, RecordType(sig.type_covered)))
            .unwrap()
            .rrsigs
            .push(sig);
    }
    sets.into_values().collect()
}

fn triple(text: &str) -> (Dnskey, Ds, Rrset) {
    let mut key = None;
    let mut ds = None;
    let mut signed = None;
    for set in parse(text) {
        if let Some(record) = set.records.first() {
            match &record.kind {
                RdataKind::Dnskey(value) => key = Some(value.clone()),
                RdataKind::Ds(value) => ds = Some(value.clone()),
                _ => signed = Some(set),
            }
        }
    }
    (key.unwrap(), ds.unwrap(), signed.unwrap())
}

// rfc6605.txt lines 207-219 (P-256 DNSKEY, DS, A and RRSIG).
pub(crate) fn p256() -> (Dnskey, Ds, Rrset) {
    triple(
        r#"
example.net. 3600 IN DNSKEY 257 3 13 (
 GojIhhXUN/u4v54ZQqGSnyhWJwaubCvTmeexv7bR6edb
 krSqQpF64cYbcB7wNcP+e+MAnLr+Wi9xMWyQLc8NAA== )
example.net. 3600 IN DS 55648 13 2 (
 b4c8c1fe2e7477127b27115656ad6256f424625bf5c1 e2770ce6d6e37df61d17 )
www.example.net. 3600 IN A 192.0.2.1
www.example.net. 3600 IN RRSIG A 13 3 3600 (
 20100909100439 20100812100439 55648 example.net.
 qx6wLYqmh+l9oCKTN6qIc+bw6ya+KJ8oMz0YP107epXA
 yGmt+3SNruPFKG7tZoLBLlUzGGus7ZwmwWep666VCw== )
"#,
    )
}

// rfc6605.txt lines 238-253 (P-384 DNSKEY, DS, A and RRSIG).
pub(crate) fn p384() -> (Dnskey, Ds, Rrset) {
    triple(
        r#"
example.net. 3600 IN DNSKEY 257 3 14 (
 xKYaNhWdGOfJ+nPrL8/arkwf2EY3MDJ+SErKivBVSum1
 w/egsXvSADtNJhyem5RCOpgQ6K8X1DRSEkrbYQ+OB+v8
 /uX45NBwY8rp65F6Glur8I/mlVNgF6W/qTI37m40 )
example.net. 3600 IN DS 10771 14 4 (
 72d7b62976ce06438e9c0bf319013cf801f09ecc84b8 d7e9495f27e305c6a9b0563a9b5f4d288405c3008a94 6df983d6 )
www.example.net. 3600 IN A 192.0.2.1
www.example.net. 3600 IN RRSIG A 14 3 3600 (
 20100909102025 20100812102025 10771 example.net.
 /L5hDKIvGDyI1fcARX3z65qrmPsVz73QD1Mr5CEqOiLP
 95hxQouuroGCeZOvzFaxsT8Glr74hbavRKayJNuydCuz
 WTSSPdz7wnqXL5bdcJzusdnI0RSMROxxwGipWcJm )
"#,
    )
}

// rfc8080.txt lines 154-166 and 179-191. Only the misprinted RRSIG
// algorithm/labels field is repaired to 15/2 as directed by the handoff.
// The published signatures cover labels=3 instead; preserve them unchanged
// to demonstrate that repairing the malformed header does not repair crypto.
pub(crate) fn published_ed25519() -> Vec<(Dnskey, Ds, Rrset)> {
    vec![
        triple(
            r#"
example.com. 3600 IN DNSKEY 257 3 15 ( l02Woi0iS8Aa25FQkUd9RMzZHJpBoRQwAQEX1SxZJA4= )
example.com. 3600 IN DS 3613 15 2 (
 3aa5ab37efce57f737fc1627013fee07bdf241bd10f3b1964ab55c78e79 a304b )
example.com. 3600 IN MX 10 mail.example.com.
example.com. 3600 IN RRSIG MX 15 2 3600 (
 1440021600 1438207200 3613 example.com. (
 Edk+IB9KNNWg0HAjm7FazXyrd5m3Rk8zNZbvNpAcM+eysqcUOMIjWoevFkj H5GaMWeG96GUVZu6ECKOQmemHDg== )
"#,
        ),
        triple(
            r#"
example.com. 3600 IN DNSKEY 257 3 15 ( zPnZ/QwEe7S8C5SPz2OfS5RR40ATk2/rYnE9xHIEijs= )
example.com. 3600 IN DS 35217 15 2 (
 401781b934e392de492ec77ae2e15d70f6575a1c0bc59c5275c04ebe80c 6614c )
example.com. 3600 IN MX 10 mail.example.com.
example.com. 3600 IN RRSIG MX 15 2 3600 (
 1440021600 1438207200 35217 example.com. (
 5LL2obmzdqjWI+Xto5eP5adXt/T5tMhasWvwcyW4L3SzfcRawOle9bodhC+ oip9ayUGjY9T/rL4rN3bOuESGDA== )
"#,
        ),
    ]
}

// rfc5702.txt lines 308-310, 314, 320-323 (512-bit RSA/SHA-256).
fn rsa256() -> (Dnskey, Rrset) {
    key_and_signed(
        r#"
example.net. 3600 IN DNSKEY (256 3 8 AwEAAcFcGsaxxdgiuuGmCkVI
 my4h99CqT7jwY3pexPGcnUFtR2Fh36BponcwtkZ4cAgtvd4Qs8P kxUdp6p/DlUmObdk= )
www.example.net. 3600 IN A 192.0.2.91
www.example.net. 3600 IN RRSIG (A 8 3 3600 20300101000000
 20000101000000 9033 example.net. kRCOH6u7l0QGy9qpC9
 l1sLncJcOKFLJ7GhiUOibu4teYp5VE9RncriShZNz85mwlMgNEa cFYK/lPtPiVYP4bwg==)
"#,
    )
}

// rfc5702.txt lines 371-375, 379, 385-390 (1024-bit RSA/SHA-512).
fn rsa512() -> (Dnskey, Rrset) {
    key_and_signed(
        r#"
example.net. 3600 IN DNSKEY (256 3 10 AwEAAdHoNTOW+et86KuJOWRD
 p1pndvwb6Y83nSVXXyLA3DLroROUkN6X0O6pnWnjJQujX/AyhqFD
 xj13tOnD9u/1kTg7cV6rklMrZDtJCQ5PCl/D7QNPsgVsMu1J2Q8g
 pMpztNFLpPBz1bWXjDtaR7ZQBlZ3PFY12ZTSncorffcGmhOL )
www.example.net. 3600 IN A 192.0.2.91
www.example.net. 3600 IN RRSIG (A 10 3 3600 20300101000000
 20000101000000 3740 example.net. tsb4wnjRUDnB1BUi+t
 6TMTXThjVnG+eCkWqjvvjhzQL1d0YRoOe0CbxrVDYd0xDtsuJRa
 eUw1ep94PzEWzr0iGYgZBWm/zpq+9fOuagYJRfDqfReKBzMweOL
 DiNa8iP5g9vMhpuv6OPlvpXwm9Sa9ZXIbNl1MBGk0fthPgxdDLw =)
"#,
    )
}

fn key_and_signed(text: &str) -> (Dnskey, Rrset) {
    let sets = parse(text);
    let key = sets
        .iter()
        .flat_map(|set| &set.records)
        .find_map(|record| match &record.kind {
            RdataKind::Dnskey(key) => Some(key.clone()),
            _ => None,
        })
        .unwrap();
    let signed = sets.into_iter().find(|set| !set.rrsigs.is_empty()).unwrap();
    (key, signed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algorithm;
    use crate::rrset::{SignatureLimit, WorkBudget, verify_rrset};
    use crate::{BogusReason, Policy};

    fn check(set: &Rrset, key: &Dnskey, now: u64) -> Result<(), BogusReason> {
        verify_rrset(
            set,
            &set.rrsigs[0].signer,
            core::slice::from_ref(key),
            now,
            Policy::default(),
            &mut WorkBudget::default(),
            SignatureLimit::CachedKeys(&mut 0),
        )
        .map(|_| ())
    }

    /// Test reference signatures independently from the future test signer.
    fn signature_vector(key: Dnskey, set: Rrset) {
        let signature = &set.rrsigs[0];
        let now = (u64::from(signature.inception) + u64::from(signature.expiration)) / 2;
        let result = check(&set, &key, now);
        let bytes = crate::canonical::signed_data(&set, signature)
            .unwrap()
            .bytes;
        let signed_hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(
            result,
            Ok(()),
            "reference algorithm {} signature failed; signed bytes {signed_hex}",
            key.algorithm
        );
        assert_eq!(algorithm::key_tag(&key), signature.key_tag);
        let mut changed = set.clone();
        changed.records[0].canonical[0] ^= 1;
        assert_eq!(
            check(&changed, &key, now),
            Err(BogusReason::SignatureInvalid)
        );
        let mut changed = set.clone();
        let mut owner = changed.owner.as_wire().to_vec();
        owner[1] ^= 1;
        changed.owner = Name::from_wire(&owner).unwrap();
        assert!(check(&changed, &key, now).is_err());
        let mut changed = set.clone();
        changed.rrsigs[0].signature[0] ^= 1;
        assert_eq!(
            check(&changed, &key, now),
            Err(BogusReason::SignatureInvalid)
        );
        let mut changed_key = key.clone();
        changed_key.public_key[0] ^= 1;
        // Keep the published tag to ensure this reaches the crypto shape check.
        let mut changed = set.clone();
        changed.rrsigs[0].key_tag = algorithm::key_tag(&changed_key);
        assert_eq!(
            check(&changed, &changed_key, now),
            Err(BogusReason::SignatureInvalid)
        );
        assert_eq!(
            check(&set, &key, u64::from(signature.expiration) + 86401),
            Err(BogusReason::SignatureExpired)
        );
        assert_eq!(
            check(&set, &key, u64::from(signature.inception) - 86401),
            Err(BogusReason::SignatureNotYetValid)
        );
    }

    #[test]
    fn rfc6605_p256_signature_and_ds() {
        let (key, ds, set) = p256();
        assert_eq!(
            algorithm::ds_digest(&set.rrsigs[0].signer, &key, ds.digest_type),
            Some(ds.digest)
        );
        signature_vector(key, set);
    }

    #[test]
    fn rfc6605_p384_signature_and_ds() {
        let (key, ds, set) = p384();
        assert_eq!(
            algorithm::ds_digest(&set.rrsigs[0].signer, &key, ds.digest_type),
            Some(ds.digest)
        );
        signature_vector(key, set);
    }

    #[test]
    fn rfc8080_published_signatures_use_an_invalid_label_count() {
        // Independent OpenSSL 3.6.3 verification locates the RFC's defect:
        // both published signatures cover labels=3, although example.com has
        // only two labels. Repairing the header to labels=2 invalidates them.
        let messages = [
            "000f0f0200000e1055d4fc6055b94ce00e1d076578616d706c6503636f6d00076578616d706c6503636f6d00000f000100000e100014000a046d61696c076578616d706c6503636f6d00",
            "000f0f0200000e1055d4fc6055b94ce08991076578616d706c6503636f6d00076578616d706c6503636f6d00000f000100000e100014000a046d61696c076578616d706c6503636f6d00",
        ];
        for ((key, ds, mut set), message) in published_ed25519().into_iter().zip(messages) {
            assert_eq!(
                algorithm::ds_digest(&set.owner, &key, ds.digest_type),
                Some(ds.digest)
            );
            assert_eq!(algorithm::key_tag(&key), set.rrsigs[0].key_tag);
            let mut signed = hex(message);
            assert_eq!(
                crate::canonical::signed_data(&set, &set.rrsigs[0])
                    .unwrap()
                    .bytes,
                signed
            );
            let now = 1_439_000_000;
            assert_eq!(check(&set, &key, now), Err(BogusReason::SignatureInvalid));
            signed[3] = 3;
            assert!(algorithm::verify(&key, &signed, &set.rrsigs[0].signature));
            // A correct primitive must not make this invalid DNSSEC header
            // usable: the label-count check rejects it before crypto.
            set.rrsigs[0].labels = 3;
            let mut budget = WorkBudget::default();
            assert_eq!(
                verify_rrset(
                    &set,
                    &set.owner,
                    &[key],
                    now,
                    Policy::default(),
                    &mut budget,
                    SignatureLimit::CachedKeys(&mut 0)
                ),
                Err(BogusReason::SignaturesMissing)
            );
            assert_eq!(budget.signatures, 0);
        }
    }

    #[test]
    fn openssl_ed25519_signatures_for_repaired_rfc8080_headers() {
        // These are independent reference signatures, not published RFC
        // signatures. OpenSSL 3.6.3 pkeyutl -sign -rawin signed the exact
        // labels=2 messages above using the private seeds in rfc8080.txt
        // lines 152 and 177. Each seed's derived public key matched the RFC.
        // OpenSSL pkeyutl -verify -rawin also verified both results.
        let signatures = [
            "oL9krJun7xfBOIWcGHi7mag5/hdZrKWw15jPGrHpjQeRAvTdszaPD+QLs3fx8A4M3e23mRZ9VrbpMngwcrqNAg==",
            "zXQ0bkYgQTEFyfLyi9QoiY6D8ZdYo4wyUhVioYZXFdT410QPRITQSqJSnzQoSm5poJ7gD7AQR0O7KuI5k2pcBg==",
        ];
        for ((key, _, mut set), signature) in published_ed25519().into_iter().zip(signatures) {
            set.rrsigs[0].signature = base64(signature);
            signature_vector(key, set);
        }
    }

    #[test]
    fn rfc5702_rsa_sha256_is_unusable_and_does_no_crypto() {
        let (key, set) = rsa256();
        assert_eq!(algorithm::key_tag(&key), 9033);
        assert!(!algorithm::key_usable(&key));
        let mut budget = WorkBudget::default();
        assert!(
            verify_rrset(
                &set,
                &set.rrsigs[0].signer,
                &[key],
                1_200_000_000,
                Policy::default(),
                &mut budget,
                SignatureLimit::CachedKeys(&mut 0)
            )
            .is_err()
        );
        assert_eq!(budget.signatures, 0);
    }

    #[test]
    fn rfc5702_rsa_sha512_1024_bit_signature() {
        let (key, set) = rsa512();
        assert!(algorithm::key_usable(&key));
        signature_vector(key, set);
    }

    #[test]
    fn rfc4034_and_rfc4509_ds_sha1_and_sha256() {
        // rfc4034.txt lines 967-978; rfc4509.txt lines 142-150,155-157.
        let records = parse(
            r#"
 dskey.example.com. 86400 IN DNSKEY 256 3 5 (
 AQOeiiR0GOMYkDshWoSKz9Xz fwJr1AYtsmx3TGkJaNXVbfi/ 2pHm822aJ5iI9BMzNXxeYCmZ
 DRD99WYwYqUSdjMmmAphXdvx egXd/M5+X7OrzKBaMbCVdFLU Uh6DhweJBjEVv5f2wwjM9Xzc
 nOf+EPbtG9DMBmADjFDc2w/r ljwvFw== ) ; key id = 60485
 dskey.example.com. 86400 IN DS 60485 5 1 ( 2BB183AF5F22588179A53B0A 98631FAD1A292118 )
 dskey.example.com. 86400 IN DS 60485 5 2 ( D4B7D520E7BB5F0F67674A0C CEB1E3E0614B93C4F9E99B83 83F6A1E4469DA50A )
"#,
        );
        let key = records
            .iter()
            .flat_map(|set| &set.records)
            .find_map(|r| match &r.kind {
                RdataKind::Dnskey(key) => Some(key),
                _ => None,
            })
            .unwrap();
        assert_eq!(algorithm::key_tag(key), 60485);
        let mut entries = Vec::new();
        for record in records.iter().flat_map(|set| &set.records) {
            if let RdataKind::Ds(ds) = &record.kind {
                assert_eq!(
                    algorithm::ds_digest(
                        &Name::from_text("dskey.example.com.").unwrap(),
                        key,
                        ds.digest_type
                    ),
                    Some(ds.digest.clone())
                );
                entries.push(ds.clone());
            }
        }
        assert!(crate::keys::filter_ds(&entries, Policy::default()).is_empty());
        let compatibility = Policy {
            sha1_signatures: true,
            ..Policy::default()
        };
        let filtered = crate::keys::filter_ds(&entries, compatibility);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].digest_type, 2);
    }

    #[test]
    fn iana_root_public_keys_match_tags_digests_and_builtin_anchors() {
        let anchors = crate::anchor::root_anchors();
        assert_eq!(anchors.len(), 2);
        // root-anchors.xml lines 10-25; each public key's Flags and Algorithm
        // come from the same KeyDigest element; protocol is DNSSEC's value 3.
        for (tag, flags, algorithm, encoded, digest) in [
            (
                20326_u16,
                257_u16,
                8_u8,
                "AwEAAaz/tAm8yTn4Mfeh5eyI96WSVexTBAvkMgJzkKTOiW1vkIbzxeF3+/4RgWOq7HrxRixHlFlExOLAJr5emLvN7SWXgnLh4+B5xQlNVz8Og8kvArMtNROxVQuCaSnIDdD5LKyWbRd2n9WGe2R8PzgCmr3EgVLrjyBxWezF0jLHwVN8efS3rCj/EWgvIWgb9tarpVUDK/b58Da+sqqls3eNbuv7pr+eoZG+SrDK6nWeL3c6H5Apxz7LjVc1uTIdsIXxuOLYA4/ilBmSVIzuDWfdRUfhHdY6+cn8HFRm+2hM8AnXGXws9555KrUB5qihylGa8subX2Nn6UwNR1AkUTV74bU=",
                "E06D44B80B8F1D39A95C0B0D7C65D08458E880409BBC683457104237C7F8EC8D",
            ),
            (
                38696_u16,
                257_u16,
                8_u8,
                "AwEAAa96jeuknZlaeSrvyAJj6ZHv28hhOKkx3rLGXVaC6rXTsDc449/cidltpkyGwCJNnOAlFNKF2jBosZBU5eeHspaQWOmOElZsjICMQMC3aeHbGiShvZsx4wMYSjH8e7Vrhbu6irwCzVBApESjbUdpWWmEnhathWu1jo+siFUiRAAxm9qyJNg/wOZqqzL/dL/q8PkcRU5oUKEpUge71M3ej2/7CPqpdVwuMoTvoB+ZOT4YeGyxMvHmbrxlFzGOHOijtzN+u1TQNatX2XBuzZNQ1K+s2CXkPIZo7s6JgZyvaBevYtxPvYLw4z9mR7K2vaF18UYH9Z9GNUUeayffKC73PYc=",
                "683D2D0ACB8C9B712A1948B27F741219298D0A450D612C483AF444A4C0FB2B16",
            ),
        ] {
            let key = Dnskey {
                flags,
                protocol: 3,
                algorithm,
                public_key: base64(encoded),
            };
            assert_eq!(algorithm::key_tag(&key), tag);
            let expected = hex(digest);
            assert_eq!(
                algorithm::ds_digest(&Name::root(), &key, 2),
                Some(expected.clone())
            );
            let anchor = anchors.iter().find(|anchor| anchor.key_tag == tag).unwrap();
            assert_eq!(anchor.zone, Name::root());
            assert_eq!(anchor.algorithm, algorithm);
            assert_eq!(anchor.digest_type, 2);
            assert_eq!(anchor.digest, expected);
        }
    }
}
