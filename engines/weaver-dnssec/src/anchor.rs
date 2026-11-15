// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::LazyLock;

use crate::{Name, algorithm};

/// A configured DS trust anchor for one zone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrustAnchor {
    pub zone: Name,
    pub key_tag: u16,
    pub algorithm: u8,
    pub digest_type: u8,
    pub digest: Vec<u8>,
}

/// Cryptographic choices. Limits are fixed independently of policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    /// Accept RSA/SHA-1 signatures and delegations (algorithms 5 and 7).
    /// Disabled by default per RFC 9905; retained for explicit compatibility.
    pub sha1_signatures: bool,
    /// Accept SHA-1 DS digests (digest type 1). Enabled by default.
    pub sha1_ds_digests: bool,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            sha1_signatures: false,
            sha1_ds_digests: true,
        }
    }
}

/// A trust-anchor configuration that cannot initialize a validator.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AnchorError {
    #[error("at least one trust anchor is required")]
    EmptyAnchors,
    #[error("digest type {digest_type} needs {expected} octets, received {actual}")]
    InvalidDigestLength {
        digest_type: u8,
        expected: usize,
        actual: usize,
    },
    #[error("no trust anchor is usable under this policy")]
    NoUsableAnchors,
}

pub(crate) fn validate_anchors(
    anchors: &[TrustAnchor],
    policy: &Policy,
) -> Result<(), AnchorError> {
    if anchors.is_empty() {
        return Err(AnchorError::EmptyAnchors);
    }
    let mut usable = false;
    for anchor in anchors {
        if let Some(expected) = algorithm::digest_length(anchor.digest_type)
            && anchor.digest.len() != expected
        {
            return Err(AnchorError::InvalidDigestLength {
                digest_type: anchor.digest_type,
                expected,
                actual: anchor.digest.len(),
            });
        }
        usable |= algorithm::supported_algorithm(anchor.algorithm, policy)
            && algorithm::supported_digest(anchor.digest_type, policy);
    }
    if !usable {
        return Err(AnchorError::NoUsableAnchors);
    }
    Ok(())
}

// root-anchors.xml lines 10–17 and 18–25. The retired 19036 anchor is omitted.
const ROOT_DS_20326: [u8; 32] = [
    0xe0, 0x6d, 0x44, 0xb8, 0x0b, 0x8f, 0x1d, 0x39, 0xa9, 0x5c, 0x0b, 0x0d, 0x7c, 0x65, 0xd0, 0x84,
    0x58, 0xe8, 0x80, 0x40, 0x9b, 0xbc, 0x68, 0x34, 0x57, 0x10, 0x42, 0x37, 0xc7, 0xf8, 0xec, 0x8d,
];
const ROOT_DS_38696: [u8; 32] = [
    0x68, 0x3d, 0x2d, 0x0a, 0xcb, 0x8c, 0x9b, 0x71, 0x2a, 0x19, 0x48, 0xb2, 0x7f, 0x74, 0x12, 0x19,
    0x29, 0x8d, 0x0a, 0x45, 0x0d, 0x61, 0x2c, 0x48, 0x3a, 0xf4, 0x44, 0xa4, 0xc0, 0xfb, 0x2b, 0x16,
];

// Built-in negative trust anchors. The list is systemd-resolved's
// (src/resolve/resolved-dns-trust-anchor.c, private_domains), joined with the
// locally served zones of RFC 6303 section 4, home.arpa (RFC 8375), local
// (RFC 6762) and internal. The root cannot prove anything under these names.
const PRIVATE_ZONES: &[&str] = &[
    "test.",
    "local.",
    "home.",
    "corp.",
    "lan.",
    "intranet.",
    "internal.",
    "private.",
    "home.arpa.",
    "resolver.arpa.",
    "ipv4only.arpa.",
    "10.in-addr.arpa.",
    "16.172.in-addr.arpa.",
    "17.172.in-addr.arpa.",
    "18.172.in-addr.arpa.",
    "19.172.in-addr.arpa.",
    "20.172.in-addr.arpa.",
    "21.172.in-addr.arpa.",
    "22.172.in-addr.arpa.",
    "23.172.in-addr.arpa.",
    "24.172.in-addr.arpa.",
    "25.172.in-addr.arpa.",
    "26.172.in-addr.arpa.",
    "27.172.in-addr.arpa.",
    "28.172.in-addr.arpa.",
    "29.172.in-addr.arpa.",
    "30.172.in-addr.arpa.",
    "31.172.in-addr.arpa.",
    "168.192.in-addr.arpa.",
    "170.0.0.192.in-addr.arpa.",
    "171.0.0.192.in-addr.arpa.",
    "0.in-addr.arpa.",
    "127.in-addr.arpa.",
    "254.169.in-addr.arpa.",
    "2.0.192.in-addr.arpa.",
    "100.51.198.in-addr.arpa.",
    "113.0.203.in-addr.arpa.",
    "255.255.255.255.in-addr.arpa.",
    "0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.ip6.arpa.",
    "1.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.ip6.arpa.",
    "d.f.ip6.arpa.",
    "8.e.f.ip6.arpa.",
    "9.e.f.ip6.arpa.",
    "a.e.f.ip6.arpa.",
    "b.e.f.ip6.arpa.",
    "8.b.d.0.1.0.0.2.ip6.arpa.",
];

/// The name is at or below a built-in negative trust anchor.
pub(crate) fn private_name(name: &Name) -> bool {
    static ZONES: LazyLock<Vec<Name>> = LazyLock::new(|| {
        PRIVATE_ZONES
            .iter()
            .filter_map(|zone| Name::from_text(zone).ok())
            .collect()
    });
    ZONES.iter().any(|zone| name.is_at_or_below(zone))
}

pub(crate) fn root_anchors() -> Vec<TrustAnchor> {
    [(20326, ROOT_DS_20326), (38696, ROOT_DS_38696)]
        .into_iter()
        .map(|(key_tag, digest)| TrustAnchor {
            zone: Name::root(),
            key_tag,
            algorithm: 8,
            digest_type: 2,
            digest: digest.to_vec(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_names_cover_their_zones_only() {
        assert_eq!(PRIVATE_ZONES.len(), 46);
        for zone in PRIVATE_ZONES {
            let zone = Name::from_text(zone).unwrap();
            assert!(private_name(&zone));
            let child =
                Name::from_wire(&[b"\x04host".as_slice(), zone.as_wire()].concat()).unwrap();
            assert!(private_name(&child));
        }
        for text in [
            "NAS.Home.Arpa.",
            "printer.lan.",
            "x.local.",
            "1.1.168.192.in-addr.arpa.",
        ] {
            assert!(private_name(&Name::from_text(text).unwrap()), "{text}");
        }
        for text in [
            ".",
            "arpa.",
            "in-addr.arpa.",
            "example.com.",
            "testing.",
            "localhost.example.",
            "lan.example.",
            "1.1.1.1.in-addr.arpa.",
            "32.172.in-addr.arpa.",
            "c.f.ip6.arpa.",
        ] {
            assert!(!private_name(&Name::from_text(text).unwrap()), "{text}");
        }
    }

    #[test]
    fn root_anchors_are_current_and_usable_by_default() {
        let anchors = root_anchors();
        assert_eq!(anchors.len(), 2);
        assert!(anchors.iter().all(|anchor| anchor.zone == Name::root()));
        assert_eq!(
            anchors
                .iter()
                .map(|anchor| anchor.key_tag)
                .collect::<Vec<_>>(),
            [20326, 38696]
        );
        assert_eq!(validate_anchors(&anchors, &Policy::default()), Ok(()));
    }

    #[test]
    fn empty_unsupported_and_malformed_configurations_are_rejected() {
        let policy = Policy::default();
        assert_eq!(
            validate_anchors(&[], &policy),
            Err(AnchorError::EmptyAnchors)
        );
        let mut anchors = root_anchors();
        for anchor in &mut anchors {
            anchor.algorithm = 253;
        }
        assert_eq!(
            validate_anchors(&anchors, &policy),
            Err(AnchorError::NoUsableAnchors)
        );
        anchors = root_anchors();
        let last = anchors.last_mut().unwrap();
        last.digest.pop();
        assert_eq!(
            validate_anchors(&anchors, &policy),
            Err(AnchorError::InvalidDigestLength {
                digest_type: 2,
                expected: 32,
                actual: 31,
            })
        );
    }

    #[test]
    fn known_digest_lengths_are_checked_even_when_policy_disables_them() {
        let base = root_anchors().remove(0);
        for (digest_type, length) in [(1, 20_usize), (2, 32), (4, 48)] {
            let mut anchor = base.clone();
            anchor.digest_type = digest_type;
            anchor.digest = vec![0; length];
            let permitted = Policy {
                sha1_ds_digests: true,
                ..Policy::default()
            };
            assert_eq!(validate_anchors(&[anchor.clone()], &permitted), Ok(()));
            anchor.digest.pop();
            assert_eq!(
                validate_anchors(&[anchor], &Policy::default()),
                Err(AnchorError::InvalidDigestLength {
                    digest_type,
                    expected: length,
                    actual: length - 1,
                })
            );
        }
    }

    #[test]
    fn unsupported_anchors_can_coexist_with_a_usable_anchor() {
        let mut anchors = root_anchors();
        let unsupported = anchors.last_mut().unwrap();
        unsupported.algorithm = 253;
        unsupported.digest_type = 255;
        unsupported.digest.clear();
        assert_eq!(validate_anchors(&anchors, &Policy::default()), Ok(()));
        for anchor in &mut anchors {
            anchor.algorithm = 5;
            anchor.digest_type = 1;
            anchor.digest = vec![0; 20];
        }
        for policy in [
            Policy {
                sha1_signatures: false,
                sha1_ds_digests: false,
            },
            Policy {
                sha1_signatures: false,
                ..Policy::default()
            },
            Policy {
                sha1_signatures: true,
                sha1_ds_digests: false,
            },
        ] {
            assert_eq!(
                validate_anchors(&anchors, &policy),
                Err(AnchorError::NoUsableAnchors)
            );
        }
        assert_eq!(
            validate_anchors(
                &anchors,
                &Policy {
                    sha1_signatures: true,
                    ..Policy::default()
                }
            ),
            Ok(())
        );
    }
}
