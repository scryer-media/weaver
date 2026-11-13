// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::collections::HashMap;

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

fn by_obfuscated(
    hashes: &[Id20],
    policy: EncryptionPolicy,
) -> HashMap<Id20, (Id20, EncryptionPolicy)> {
    hashes
        .iter()
        .map(|ih| (obfuscated_hash(ih), (*ih, policy)))
        .collect()
}

/// What a session whose policy and every torrent's is `policy` takes.
fn incoming(policy: EncryptionPolicy) -> Incoming {
    Incoming {
        encrypted: policy != EncryptionPolicy::Disabled,
        plaintext: policy != EncryptionPolicy::Require,
    }
}

async fn run_pair(
    initiator_hash: Id20,
    offer: u32,
    session_hashes: Vec<Id20>,
    policy: EncryptionPolicy,
) -> Pair {
    let torrents = by_obfuscated(&session_hashes, policy);
    let ((ar, aw), (br, bw)) = pipe();
    let ia = bt_handshake(initiator_hash, 1);
    let (initiator, responder) = tokio::join!(
        initiate(ar, aw, &initiator_hash, offer, &ia),
        respond(br, bw, true, |o| torrents.get(o).copied()),
    );
    if let Ok((_, skey)) = &responder {
        assert_eq!(*skey, Some(initiator_hash));
    }
    Pair {
        initiator,
        responder: responder.map(|(established, _)| established),
    }
}

/// A writer that keeps a copy of every byte it puts on the wire.
struct Tap {
    inner: BoxAsyncWrite,
    seen: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
}

impl tokio::io::AsyncWrite for Tap {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let polled = std::pin::Pin::new(&mut self.inner).poll_write(cx, buf);
        if let std::task::Poll::Ready(Ok(n)) = &polled {
            self.seen.lock().unwrap().extend_from_slice(&buf[..*n]);
        }
        polled
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// What the initiator put on the wire over a whole connection: its side
/// of the handshake, then [`exchange`]'s stream.
struct Wiretap {
    initiator: Encryption,
    responder: Encryption,
    sent: Vec<u8>,
}

impl Wiretap {
    /// Whether [`exchange`]'s stream crossed the wire as it was written.
    fn payload_in_clear(&self) -> bool {
        let needle = &stream_data()[..4096];
        self.sent.windows(needle.len()).any(|w| w == needle)
    }

    /// Whether the initiator's BitTorrent handshake, sent in IA, crossed
    /// the wire as it was written.
    fn handshake_in_clear(&self) -> bool {
        self.sent
            .windows(PLAINTEXT_HANDSHAKE_PREFIX.len())
            .any(|w| w == PLAINTEXT_HANDSHAKE_PREFIX)
    }
}

/// A Weaver initiator offering `offer` to a Weaver responder whose torrent
/// is under `policy`, then [`exchange`] over the result, the initiator's
/// bytes recorded.
async fn tapped(offer: u32, policy: EncryptionPolicy) -> Wiretap {
    let ih = Id20::new([7; 20]);
    let torrents = by_obfuscated(&[ih], policy);
    let ((ar, aw), (br, bw)) = pipe();
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let aw: BoxAsyncWrite = Box::new(Tap {
        inner: aw,
        seen: seen.clone(),
    });
    let ia = bt_handshake(ih, 1);
    let mut reader = br;
    let (a, b) = tokio::join!(
        initiate(ar, aw, &ih, offer, &ia),
        upgrade_incoming(incoming(policy), &mut reader, bw, Duration::MAX, |o| {
            torrents.get(o).copied()
        }),
    );
    let a = a.unwrap();
    let (responder, writer, skey) = b.unwrap();
    assert_eq!(skey, Some(ih));
    let initiator = a.encryption;
    let b = Established {
        reader,
        writer,
        encryption: responder,
    };
    exchange(a, b, ih).await;
    let sent = std::mem::take(&mut *seen.lock().unwrap());
    Wiretap {
        initiator,
        responder,
        sent,
    }
}

fn stream_data() -> Vec<u8> {
    (0..200_000u32)
        .map(|i| u8::try_from(i % 241).unwrap())
        .collect()
}

async fn exchange(mut a: Established, mut b: Established, info_hash: Id20) {
    // The responder finds the initiator's handshake (sent in IA) first.
    let h = read_bt_handshake(&mut b.reader).await;
    assert_eq!(h.info_hash, info_hash);
    assert_eq!(h.peer_id, Id20::new([1; 20]));
    b.writer
        .write_all(&mut bt_handshake(info_hash, 2))
        .await
        .unwrap();
    let h = read_bt_handshake(&mut a.reader).await;
    assert_eq!(h.peer_id, Id20::new([2; 20]));

    // Then a stream larger than the pipe both ways at once.
    let data = stream_data();
    let (mut out_a, mut out_b) = (data.clone(), data.clone());
    let (sent_a, sent_b, got_a, got_b) = tokio::join!(
        a.writer.write_all(&mut out_a),
        b.writer.write_all(&mut out_b),
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
        EncryptionPolicy::Require,
    )
    .await;
    let a = pair.initiator.unwrap();
    let b = pair.responder.unwrap();
    assert_eq!(a.encryption, Encryption::Rc4);
    assert_eq!(b.encryption, Encryption::Rc4);
    exchange(a, b, ih).await;
}

#[tokio::test]
async fn test_header_only_encryption_when_the_responder_selects_plaintext() {
    let ih = Id20::new([7; 20]);
    let pair = run_pair(ih, CRYPTO_PLAINTEXT, vec![ih], EncryptionPolicy::Prefer).await;
    let a = pair.initiator.unwrap();
    let b = pair.responder.unwrap();
    assert_eq!(a.encryption, Encryption::HeaderOnly);
    assert_eq!(b.encryption, Encryption::HeaderOnly);
    exchange(a, b, ih).await;
}

#[tokio::test]
async fn test_wrong_info_hash_fails_to_synchronise() {
    let pair = run_pair(
        Id20::new([7; 20]),
        CRYPTO_RC4,
        vec![Id20::new([8; 20]), Id20::new([9; 20])],
        EncryptionPolicy::Require,
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
    let pair = run_pair(ih, CRYPTO_PLAINTEXT, vec![ih], EncryptionPolicy::Require).await;
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
    Result<Encryption, MseError>,
    BoxAsyncReadVectored,
    (BoxAsyncReadVectored, BoxAsyncWrite),
) {
    let ih = Id20::new([7; 20]);
    let ((ar, mut aw), (mut reader, writer)) = pipe();
    aw.write_all(&bt_handshake(ih, 1)).await.unwrap();
    let torrents = by_obfuscated(&[ih], policy);
    let res = upgrade_incoming(incoming(policy), &mut reader, writer, Duration::MAX, |o| {
        torrents.get(o).copied()
    })
    .await
    .map(|(encryption, _, skey)| {
        assert_eq!(skey, None);
        encryption
    });
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
    assert_eq!(res.unwrap(), Encryption::None);
    let h = read_bt_handshake(&mut reader).await;
    assert_eq!(h.info_hash, Id20::new([7; 20]));
}

#[tokio::test]
async fn test_disabled_responder_reads_nothing() {
    let (res, mut reader, _initiator) = plaintext_against(EncryptionPolicy::Disabled).await;
    assert_eq!(res.unwrap(), Encryption::None);
    let h = read_bt_handshake(&mut reader).await;
    assert_eq!(h.peer_id, Id20::new([1; 20]));
}

/// Two connected uTP streams on loopback, split into the halves a peer
/// connection reads and writes.
async fn utp_pipe() -> (
    (BoxAsyncReadVectored, BoxAsyncWrite),
    (BoxAsyncReadVectored, BoxAsyncWrite),
    [crate::network::UtpSocket; 2],
) {
    let f = crate::test_support::socket_factory();
    let bind = || {
        crate::listen::bind_utp(
            f.as_ref(),
            (std::net::Ipv4Addr::LOCALHOST, 0).into(),
            Default::default(),
            crate::udp_options::STREAM_WINDOW,
            None,
        )
    };
    let (a, b) = (bind().await.unwrap(), bind().await.unwrap());
    let (accepted, dialled) = tokio::join!(a.accept(), b.connect(a.bind_addr()));
    let (ar, aw) = dialled.unwrap().split();
    let (br, bw) = accepted.unwrap().split();
    (
        (Box::new(ar), Box::new(aw)),
        (Box::new(br), Box::new(bw)),
        [a, b],
    )
}

/// Reads exactly `len` bytes through vectored reads into two slices at a
/// time, as the peer connection reads its ring buffer.
async fn read_vectored_exact(r: &mut BoxAsyncReadVectored, len: usize) -> Vec<u8> {
    use crate::vectored_traits::AsyncReadVectoredExt;
    let mut got = Vec::with_capacity(len);
    let mut a = vec![0u8; 1000];
    let mut b = vec![0u8; 3000];
    while got.len() < len {
        let want = len - got.len();
        let (a_len, b_len) = (want.min(a.len()), want.saturating_sub(a.len()).min(b.len()));
        let n = r
            .read_vectored(&mut [
                std::io::IoSliceMut::new(&mut a[..a_len]),
                std::io::IoSliceMut::new(&mut b[..b_len]),
            ])
            .await
            .unwrap();
        assert_ne!(n, 0, "stream closed after {} of {len} bytes", got.len());
        got.extend_from_slice(&a[..n.min(a_len)]);
        if n > a_len {
            got.extend_from_slice(&b[..n - a_len]);
        }
    }
    got
}

/// The encrypted handshake and then a stream larger than one read, both ways,
/// over uTP, read with vectored reads: what the peer connection does.
async fn mse_over_utp(
    offer_policy: EncryptionPolicy,
    responder_policy: EncryptionPolicy,
    expected: Encryption,
) {
    let ih = Id20::new([7; 20]);
    let ((ar, aw), (br, bw), _sockets) = utp_pipe().await;
    let ia = bt_handshake(ih, 1);
    let mut reader = br;
    let torrents = by_obfuscated(&[ih], responder_policy);
    let (a, b) = tokio::join!(
        initiate(ar, aw, &ih, outgoing_offer(offer_policy), &ia),
        upgrade_incoming(
            incoming(responder_policy),
            &mut reader,
            bw,
            Duration::MAX,
            |o| torrents.get(o).copied()
        ),
    );
    let (encryption, writer, _) = b.unwrap();
    let mut a = a.unwrap();
    assert_eq!(encryption, expected);
    assert_eq!(a.encryption, expected);
    let mut b = Established {
        reader,
        writer,
        encryption,
    };
    let got = read_vectored_exact(&mut b.reader, 68).await;
    assert_eq!(Handshake::deserialize(&got).unwrap().0.info_hash, ih);

    let data: Vec<u8> = (0..300_000u32)
        .map(|i| u8::try_from(i % 241).unwrap())
        .collect();
    let (mut out_a, mut out_b) = (data.clone(), data.clone());
    let (sent_a, sent_b, got_a, got_b) = tokio::join!(
        a.writer.write_all(&mut out_a),
        b.writer.write_all(&mut out_b),
        read_vectored_exact(&mut a.reader, data.len()),
        read_vectored_exact(&mut b.reader, data.len()),
    );
    sent_a.unwrap();
    sent_b.unwrap();
    assert!(got_a == data, "initiator decrypted different bytes");
    assert!(got_b == data, "responder decrypted different bytes");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rc4_over_utp_moves_data_under_require() {
    mse_over_utp(
        EncryptionPolicy::Require,
        EncryptionPolicy::Require,
        Encryption::Rc4,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_header_only_over_utp_moves_data_under_prefer() {
    mse_over_utp(
        EncryptionPolicy::Prefer,
        EncryptionPolicy::Prefer,
        Encryption::HeaderOnly,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_rc4_over_utp_moves_data_from_prefer_to_require() {
    mse_over_utp(
        EncryptionPolicy::Prefer,
        EncryptionPolicy::Require,
        Encryption::Rc4,
    )
    .await;
}

/// libtorrent 2.0.15 `bt_peer_connection::on_receive_impl`
/// (src/bt_peer_connection.cpp:3139-3161): without `prefer_rc4` the
/// responder keeps the least significant method both sides allow.
#[test]
fn test_select_method_keeps_the_least_significant_common_method() {
    let both = CRYPTO_RC4 | CRYPTO_PLAINTEXT;
    let prefer = EncryptionPolicy::Prefer.methods();
    let require = EncryptionPolicy::Require.methods();
    assert_eq!(select_method(both, prefer), Some(CRYPTO_PLAINTEXT));
    assert_eq!(select_method(CRYPTO_RC4, prefer), Some(CRYPTO_RC4));
    assert_eq!(
        select_method(CRYPTO_PLAINTEXT, prefer),
        Some(CRYPTO_PLAINTEXT)
    );
    assert_eq!(select_method(both, require), Some(CRYPTO_RC4));
    assert_eq!(select_method(CRYPTO_RC4, require), Some(CRYPTO_RC4));
    assert_eq!(select_method(CRYPTO_PLAINTEXT, require), None);
    // Methods neither side knows are never selected.
    assert_eq!(select_method(0xfc, prefer), None);
    assert_eq!(select_method(0xfc | both, prefer), Some(CRYPTO_PLAINTEXT));
}

#[tokio::test]
async fn test_prefer_responder_selects_plaintext_from_an_offer_of_both() {
    let tap = tapped(CRYPTO_RC4 | CRYPTO_PLAINTEXT, EncryptionPolicy::Prefer).await;
    assert_eq!(tap.initiator, Encryption::HeaderOnly);
    assert_eq!(tap.responder, Encryption::HeaderOnly);
    assert!(!tap.handshake_in_clear(), "the handshake went out in clear");
    assert!(tap.payload_in_clear(), "the payload went out enciphered");
}

#[tokio::test]
async fn test_prefer_responder_selects_rc4_from_an_rc4_only_offer() {
    let tap = tapped(CRYPTO_RC4, EncryptionPolicy::Prefer).await;
    assert_eq!(tap.initiator, Encryption::Rc4);
    assert_eq!(tap.responder, Encryption::Rc4);
    assert!(!tap.handshake_in_clear(), "the handshake went out in clear");
    assert!(!tap.payload_in_clear(), "the payload went out in clear");
}

#[tokio::test]
async fn test_two_prefer_ends_are_header_only() {
    let tap = tapped(
        outgoing_offer(EncryptionPolicy::Prefer),
        EncryptionPolicy::Prefer,
    )
    .await;
    assert_eq!(tap.initiator, Encryption::HeaderOnly);
    assert_eq!(tap.responder, Encryption::HeaderOnly);
    assert!(!tap.handshake_in_clear(), "the handshake went out in clear");
    assert!(tap.payload_in_clear(), "the payload went out enciphered");
}

#[tokio::test]
async fn test_prefer_initiator_runs_rc4_when_the_responder_selects_it() {
    let tap = tapped(
        outgoing_offer(EncryptionPolicy::Prefer),
        EncryptionPolicy::Require,
    )
    .await;
    assert_eq!(tap.initiator, Encryption::Rc4);
    assert_eq!(tap.responder, Encryption::Rc4);
    assert!(!tap.handshake_in_clear(), "the handshake went out in clear");
    assert!(!tap.payload_in_clear(), "the payload went out in clear");
}

#[tokio::test]
async fn test_require_initiator_runs_rc4_against_either_responder() {
    for policy in [EncryptionPolicy::Prefer, EncryptionPolicy::Require] {
        let tap = tapped(outgoing_offer(EncryptionPolicy::Require), policy).await;
        assert_eq!(tap.initiator, Encryption::Rc4, "{policy:?}");
        assert_eq!(tap.responder, Encryption::Rc4, "{policy:?}");
        assert!(!tap.payload_in_clear(), "{policy:?}: payload in clear");
    }
}

#[tokio::test]
async fn test_prefer_and_require_responders_accept_encrypted() {
    for policy in [EncryptionPolicy::Prefer, EncryptionPolicy::Require] {
        let ih = Id20::new([7; 20]);
        let ((ar, aw), (br, bw)) = pipe();
        let ia = bt_handshake(ih, 1);
        let mut reader = br;
        let torrents = by_obfuscated(&[ih], policy);
        let (a, b) = tokio::join!(
            initiate(ar, aw, &ih, outgoing_offer(EncryptionPolicy::Require), &ia),
            upgrade_incoming(incoming(policy), &mut reader, bw, Duration::MAX, |o| {
                torrents.get(o).copied()
            }),
        );
        let (encryption, writer, skey) = b.unwrap();
        assert_eq!(encryption, Encryption::Rc4);
        assert_eq!(skey, Some(ih));
        let b = Established {
            reader,
            writer,
            encryption: Encryption::Rc4,
        };
        exchange(a.unwrap(), b, ih).await;
    }
}
