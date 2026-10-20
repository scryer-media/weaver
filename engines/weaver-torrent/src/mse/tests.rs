// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use crate::core::hash_id::Id20;
use crate::peer_protocol::Handshake;
use crate::sha1w::{ISha1, Sha1};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;

fn pipe() -> (
    (BoxAsyncReadVectored, BoxAsyncWrite),
    (BoxAsyncReadVectored, BoxAsyncWrite),
) {
    let (a, b) = tokio::io::duplex(64 * 1024);
    let (ar, aw) = tokio::io::split(a);
    let (br, bw) = tokio::io::split(b);
    (
        (Box::new(ar.into_vectored_compat()), Box::new(aw)),
        (Box::new(br.into_vectored_compat()), Box::new(bw)),
    )
}

fn bt_handshake(info_hash: Id20, peer_id: u8) -> Vec<u8> {
    let mut buf = vec![0u8; 68];
    let len = Handshake::new(info_hash, Id20::new([peer_id; 20])).serialize_unchecked_len(&mut buf);
    buf.truncate(len);
    buf
}

async fn read_bt_handshake(r: &mut BoxAsyncReadVectored) -> Handshake {
    let mut buf = [0u8; 68];
    r.read_exact(&mut buf).await.unwrap();
    Handshake::deserialize(&buf).unwrap().0
}

fn sha1(parts: &[&[u8]]) -> [u8; 20] {
    let mut h = Sha1::new();
    for p in parts {
        h.update(p);
    }
    h.finish()
}

/// The key derivation, recomputed here from the primitives with fixed private
/// keys. With xa = 0x01 repeated and xb = 0x02 repeated (160 bits each):
///
/// - Ya = 2^xa mod P, Yb = 2^xb mod P (96 bytes, big-endian)
/// - S = Yb^xa mod P = Ya^xb mod P
/// - req1 = SHA1("req1" || S)
/// - req2 xor req3 = SHA1("req2" || SKEY) xor SHA1("req3" || S)
/// - keyA = SHA1("keyA" || S || SKEY), keyB = SHA1("keyB" || S || SKEY)
/// - the first RC4 output after the 1024-byte discard encrypts VC.
#[test]
fn test_key_derivation_from_the_primitives() {
    let xa = [0x01u8; 20];
    let xb = [0x02u8; 20];
    let ya = dh::public_key(&xa);
    let yb = dh::public_key(&xb);
    let s = dh::shared_secret(&xa, &yb).unwrap();
    assert_eq!(s, dh::shared_secret(&xb, &ya).unwrap());
    // Fixed inputs give fixed outputs.
    assert_eq!(ya, dh::public_key(&xa));

    let skey = Id20::new([0xab; 20]);
    let keys = Keys::derive(&s, &skey);

    let mut req1_input = b"req1".to_vec();
    req1_input.extend_from_slice(&s);
    assert_eq!(keys.req1, sha1(&[&req1_input]));

    let req2 = sha1(&[b"req2", &[0xab; 20]]);
    let req3 = sha1(&[b"req3", &s]);
    let expected: Vec<u8> = req2.iter().zip(req3.iter()).map(|(a, b)| a ^ b).collect();
    assert_eq!(keys.req2_xor_req3.to_vec(), expected);

    let mut key_a_input = b"keyA".to_vec();
    key_a_input.extend_from_slice(&s);
    key_a_input.extend_from_slice(&[0xab; 20]);
    assert_eq!(keys.key_a, sha1(&[&key_a_input]));
    assert_eq!(keys.key_b, sha1(&[b"keyB", &s, &[0xab; 20]]));
    assert_ne!(keys.key_a, keys.key_b);

    // The encrypted VC the responder sends is keyB's keystream at offset 1024.
    let mut full = vec![0u8; 1024 + 8];
    Rc4::new(&keys.key_b).apply(&mut full);
    let mut vc = VC;
    Rc4::new_discard_1024(&keys.key_b).apply(&mut vc);
    assert_eq!(&vc[..], &full[1024..]);
}

struct Pair {
    initiator: Result<Established, MseError>,
    responder: Result<Established, MseError>,
}

async fn run_pair(
    initiator_hash: Id20,
    offer: u32,
    session_hashes: Vec<Id20>,
    allowed: u32,
) -> Pair {
    let ((ar, aw), (br, bw)) = pipe();
    let ia = bt_handshake(initiator_hash, 1);
    let (initiator, responder) = tokio::join!(
        initiate(ar, aw, &initiator_hash, offer, &ia),
        respond(br, bw, allowed, true, || session_hashes),
    );
    Pair {
        initiator,
        responder,
    }
}

async fn exchange(mut a: Established, mut b: Established, info_hash: Id20) {
    // The responder finds the initiator's handshake (sent in IA) first.
    let h = read_bt_handshake(&mut b.reader).await;
    assert_eq!(h.info_hash, info_hash);
    assert_eq!(h.peer_id, Id20::new([1; 20]));
    b.writer
        .write_all(&bt_handshake(info_hash, 2))
        .await
        .unwrap();
    let h = read_bt_handshake(&mut a.reader).await;
    assert_eq!(h.peer_id, Id20::new([2; 20]));

    // Then a stream larger than the pipe both ways at once.
    let data: Vec<u8> = (0..200_000u32)
        .map(|i| u8::try_from(i % 241).unwrap())
        .collect();
    let (sent_a, sent_b, got_a, got_b) = tokio::join!(
        a.writer.write_all(&data),
        b.writer.write_all(&data),
        async {
            let mut got = vec![0u8; data.len()];
            a.reader.read_exact(&mut got).await.unwrap();
            got
        },
        async {
            let mut got = vec![0u8; data.len()];
            b.reader.read_exact(&mut got).await.unwrap();
            got
        },
    );
    sent_a.unwrap();
    sent_b.unwrap();
    assert!(got_a == data);
    assert!(got_b == data);
}

#[tokio::test]
async fn test_handshake_completes_and_carries_the_bt_handshake() {
    let ih = Id20::new([7; 20]);
    let other = Id20::new([8; 20]);
    let pair = run_pair(
        ih,
        CRYPTO_RC4 | CRYPTO_PLAINTEXT,
        vec![other, ih],
        CRYPTO_RC4,
    )
    .await;
    let a = pair.initiator.unwrap();
    let b = pair.responder.unwrap();
    assert_eq!(a.method, CRYPTO_RC4);
    assert_eq!(b.method, CRYPTO_RC4);
    exchange(a, b, ih).await;
}

#[tokio::test]
async fn test_header_only_encryption_when_the_responder_selects_plaintext() {
    let ih = Id20::new([7; 20]);
    let pair = run_pair(
        ih,
        CRYPTO_RC4 | CRYPTO_PLAINTEXT,
        vec![ih],
        CRYPTO_PLAINTEXT,
    )
    .await;
    let a = pair.initiator.unwrap();
    let b = pair.responder.unwrap();
    assert_eq!(a.method, CRYPTO_PLAINTEXT);
    assert_eq!(b.method, CRYPTO_PLAINTEXT);
    exchange(a, b, ih).await;
}

#[tokio::test]
async fn test_wrong_info_hash_fails_to_synchronise() {
    let pair = run_pair(
        Id20::new([7; 20]),
        CRYPTO_RC4,
        vec![Id20::new([8; 20]), Id20::new([9; 20])],
        CRYPTO_RC4,
    )
    .await;
    assert!(
        matches!(pair.responder, Err(MseError::UnknownInfoHash)),
        "{:?}",
        pair.responder.err()
    );
    // The responder dropped the connection, so the initiator never found the VC.
    assert!(
        matches!(pair.initiator, Err(MseError::Closed { .. })),
        "{:?}",
        pair.initiator.err()
    );
}

#[tokio::test]
async fn test_no_common_method_is_refused() {
    let ih = Id20::new([7; 20]);
    let pair = run_pair(ih, CRYPTO_PLAINTEXT, vec![ih], CRYPTO_RC4).await;
    assert!(
        matches!(
            pair.responder,
            Err(MseError::NoCommonMethod {
                offered: CRYPTO_PLAINTEXT,
                allowed: CRYPTO_RC4
            })
        ),
        "{:?}",
        pair.responder.err()
    );
    assert!(matches!(pair.initiator, Err(MseError::Closed { .. })));
}

/// A plaintext BitTorrent handshake arriving at a responder with `policy`.
/// Returns the upgrade result, the responder's reader, and the initiator's
/// halves (kept so the pipe stays open).
async fn plaintext_against(
    policy: EncryptionPolicy,
) -> (
    Result<u32, MseError>,
    BoxAsyncReadVectored,
    (BoxAsyncReadVectored, BoxAsyncWrite),
) {
    let ih = Id20::new([7; 20]);
    let ((ar, mut aw), (mut reader, mut writer)) = pipe();
    aw.write_all(&bt_handshake(ih, 1)).await.unwrap();
    let res = upgrade_incoming(policy, &mut reader, &mut writer, Duration::MAX, || vec![ih]).await;
    (res, reader, (ar, aw))
}

#[tokio::test]
async fn test_require_responder_drops_plaintext() {
    let (res, _, _) = plaintext_against(EncryptionPolicy::Require).await;
    assert!(matches!(res, Err(MseError::PlaintextRefused)), "{res:?}");
}

#[tokio::test]
async fn test_prefer_responder_accepts_plaintext() {
    let (res, mut reader, _initiator) = plaintext_against(EncryptionPolicy::Prefer).await;
    assert_eq!(res.unwrap(), 0);
    let h = read_bt_handshake(&mut reader).await;
    assert_eq!(h.info_hash, Id20::new([7; 20]));
}

#[tokio::test]
async fn test_disabled_responder_reads_nothing() {
    let (res, mut reader, _initiator) = plaintext_against(EncryptionPolicy::Disabled).await;
    assert_eq!(res.unwrap(), 0);
    let h = read_bt_handshake(&mut reader).await;
    assert_eq!(h.peer_id, Id20::new([1; 20]));
}

#[tokio::test]
async fn test_prefer_and_require_responders_accept_encrypted() {
    for policy in [EncryptionPolicy::Prefer, EncryptionPolicy::Require] {
        let ih = Id20::new([7; 20]);
        let ((ar, aw), (br, bw)) = pipe();
        let ia = bt_handshake(ih, 1);
        let mut reader = br;
        let mut writer = bw;
        let (a, b) = tokio::join!(
            initiate(ar, aw, &ih, outgoing_offer(EncryptionPolicy::Require), &ia),
            upgrade_incoming(policy, &mut reader, &mut writer, Duration::MAX, || vec![ih]),
        );
        assert_eq!(b.unwrap(), CRYPTO_RC4);
        let b = Established {
            reader,
            writer,
            method: CRYPTO_RC4,
        };
        exchange(a.unwrap(), b, ih).await;
    }
}
