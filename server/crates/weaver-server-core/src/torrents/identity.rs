// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! How Weaver names itself to peers and trackers. Fixed by the build.

use librqbit_core::Id20;

/// The Azureus-style peer-id prefix and user agent of this build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientIdentity {
    prefix: [u8; 8],
    user_agent: &'static str,
}

/// The identity of this build: `-WV<major><minor:02><patch>-`, `Weaver/<version>`.
pub const IDENTITY: ClientIdentity = ClientIdentity {
    prefix: prefix_for(env!("CARGO_PKG_VERSION")),
    user_agent: concat!("Weaver/", env!("CARGO_PKG_VERSION")),
};

const DIGITS: &[u8; 64] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz.-";

/// The eight prefix bytes for a `major.minor.patch` version. Major and patch
/// take one digit each from the Azureus alphabet, minor two decimal digits.
const fn prefix_for(version: &str) -> [u8; 8] {
    let bytes = version.as_bytes();
    let mut parts = [0usize; 3];
    let mut part = 0;
    let mut at = 0;
    while at < bytes.len() && part < 3 {
        let byte = bytes[at];
        if byte == b'.' {
            part += 1;
        } else if byte.is_ascii_digit() {
            parts[part] = parts[part] * 10 + (byte - b'0') as usize;
        } else {
            break;
        }
        at += 1;
    }
    assert!(parts[0] < 64 && parts[1] < 100 && parts[2] < 64);
    [
        b'-',
        b'W',
        b'V',
        DIGITS[parts[0]],
        b'0' + (parts[1] / 10) as u8,
        b'0' + (parts[1] % 10) as u8,
        DIGITS[parts[2]],
        b'-',
    ]
}

impl ClientIdentity {
    pub const fn prefix(&self) -> &[u8; 8] {
        &self.prefix
    }

    pub const fn user_agent(&self) -> &'static str {
        self.user_agent
    }

    /// A fresh peer id carrying this prefix.
    pub fn peer_id(&self) -> Id20 {
        let mut id = [0u8; 20];
        id[..8].copy_from_slice(&self.prefix);
        getrandom::fill(&mut id[8..]).expect("system randomness");
        Id20::new(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_encodes_major_minor_patch() {
        assert_eq!(&prefix_for("0.17.0"), b"-WV0170-");
        assert_eq!(&prefix_for("1.2.3"), b"-WV1023-");
        assert_eq!(&prefix_for("0.16.12"), b"-WV016C-");
        assert_eq!(&prefix_for("0.17.0-rc.1"), b"-WV0170-");
    }

    #[test]
    fn identity_follows_the_package_version() {
        assert_eq!(IDENTITY.prefix(), &prefix_for(env!("CARGO_PKG_VERSION")));
        assert_eq!(
            IDENTITY.user_agent(),
            format!("Weaver/{}", env!("CARGO_PKG_VERSION"))
        );
    }

    #[test]
    fn peer_ids_share_the_prefix_and_differ() {
        let first = IDENTITY.peer_id();
        let second = IDENTITY.peer_id();
        assert_eq!(&first.0[..8], IDENTITY.prefix());
        assert_eq!(&second.0[..8], IDENTITY.prefix());
        assert_ne!(first, second);
        assert!(librqbit_core::peer_id::try_decode_peer_id(first).is_some());
    }
}
