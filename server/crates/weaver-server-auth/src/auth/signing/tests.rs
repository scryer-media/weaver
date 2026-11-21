// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use base64::Engine as _;

// The master key 00 01 02 .. 1f, a fixed test value.
fn counting_key() -> EncryptionKey {
    let bytes: Vec<u8> = (0..32).collect();
    EncryptionKey::from_base64(&base64::engine::general_purpose::STANDARD.encode(bytes)).unwrap()
}

const HASH: &str = "$argon2id$v=19$m=19456,t=2,p=1$c2FsdA$aGFzaA";

// Pinned from an independent HKDF-SHA256 (RFC 5869) and HMAC-SHA256
// computation. A change here signs every session out.
#[test]
fn a_known_master_key_gives_the_pinned_keys() {
    let keys = SigningKeys::derive(&counting_key()).unwrap();
    assert_eq!(
        hex::encode(keys.session_salt()),
        "d66d5120a6ed0323141a2154de516d77763687321de82475b82a90b202eff907"
    );
    assert_eq!(
        hex::encode(keys.script_run_key()),
        "5813b08055e473ee9964a2293be105cd22be14ba82ed2f2f16b279e99a12c973"
    );
    assert_ne!(keys.session_salt(), keys.script_run_key());
    assert_eq!(
        hex::encode(keys.login_key(HASH)),
        "ced4c89535f162a2aaa41ff068f8bf08859cacfc5207161cf2abdc835d332769"
    );
}

#[test]
fn a_login_key_follows_the_master_key_and_the_hash() {
    let master = counting_key();
    let first = SigningKeys::derive(&master).unwrap();
    let second = SigningKeys::derive(&master).unwrap();
    assert_eq!(first.login_key(HASH), second.login_key(HASH));
    assert_eq!(first.script_run_key(), second.script_run_key());

    assert_ne!(first.login_key(HASH), first.login_key("$argon2id$other"));

    let other = SigningKeys::derive(&EncryptionKey::generate()).unwrap();
    assert_ne!(first.login_key(HASH), other.login_key(HASH));
    assert_ne!(first.script_run_key(), other.script_run_key());
}

#[test]
fn signing_keys_never_print_their_bytes() {
    let keys = SigningKeys::derive(&counting_key()).unwrap();
    assert_eq!(format!("{keys:?}"), "SigningKeys { .. }");
}
