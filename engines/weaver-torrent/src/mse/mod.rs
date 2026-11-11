// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

//! Message stream encryption (also called protocol encryption): the
//! Diffie-Hellman key exchange and RC4 stream that peers wrap the BitTorrent
//! peer protocol in.
//!
//! The exchange, initiator A and responder B:
//!
//! 1. A->B: Ya, PadA
//! 2. B->A: Yb, PadB
//! 3. A->B: HASH('req1', S), HASH('req2', SKEY) xor HASH('req3', S),
//!    ENCRYPT(VC, crypto_provide, len(PadC), PadC, len(IA)), ENCRYPT(IA)
//! 4. B->A: ENCRYPT(VC, crypto_select, len(PadD), PadD), ENCRYPT2(payload stream)
//! 5. A->B: ENCRYPT2(payload stream)
//!
//! Y is a 768-bit public key, S the shared secret, SKEY the info hash, VC eight
//! zero bytes, HASH is SHA-1. ENCRYPT is RC4 keyed with HASH('keyA', S, SKEY)
//! from A to B and HASH('keyB', S, SKEY) from B to A, the first 1024 bytes of
//! keystream discarded. ENCRYPT2 is that same RC4 stream when crypto_select is
//! RC4, and nothing when it is the plaintext method. IA carries the
//! initiator's BitTorrent handshake.

mod dh;
mod rc4;
mod stream;

use std::time::Duration;

use crate::core::hash_id::Id20;
use crate::sha1w::{ISha1, Sha1};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    type_aliases::{BoxAsyncReadVectored, BoxAsyncWrite},
    vectored_traits::AsyncReadVectoredIntoCompat,
};

pub(crate) use rc4::Rc4;
use stream::DecryptingReader;
pub(crate) use stream::Outbound;

/// Whether peer connections use message stream encryption.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncryptionPolicy {
    /// Plain BitTorrent connections only; incoming encrypted handshakes are dropped.
    #[default]
    Disabled,
    /// Outgoing: each connection to a peer tries the handshake the peer last
    /// completed one with, encrypted until it has; one that fails makes the
    /// next connection try the other. Incoming: accept encrypted and
    /// plaintext.
    Prefer,
    /// Every connection is RC4-encrypted; plaintext is never sent or accepted.
    Require,
}

impl EncryptionPolicy {
    /// Whether a connection whose handshake went as `encryption` is one
    /// this policy keeps.
    pub fn allows(self, encryption: Encryption) -> bool {
        match self {
            EncryptionPolicy::Disabled => encryption == Encryption::None,
            EncryptionPolicy::Prefer => true,
            EncryptionPolicy::Require => encryption == Encryption::Rc4,
        }
    }

    /// The crypto methods this policy allows after the encrypted handshake.
    fn methods(self) -> u32 {
        match self {
            EncryptionPolicy::Disabled => 0,
            EncryptionPolicy::Prefer => CRYPTO_RC4 | CRYPTO_PLAINTEXT,
            EncryptionPolicy::Require => CRYPTO_RC4,
        }
    }
}

/// How a connection's handshake went (libtorrent
/// `bt_peer_connection::m_encrypted` and `m_rc4_encrypted`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Encryption {
    /// A plain BitTorrent handshake.
    #[default]
    None,
    /// The encrypted handshake, then the plaintext method: only the
    /// handshake header was encrypted.
    HeaderOnly,
    /// The encrypted handshake, then the RC4 method: the whole stream is.
    Rc4,
}

impl Encryption {
    /// Whether the connection used the encrypted handshake.
    pub fn is_encrypted(self) -> bool {
        self != Self::None
    }
}

/// crypto_provide / crypto_select bit: only the handshake header is encrypted.
pub(crate) const CRYPTO_PLAINTEXT: u32 = 0x01;
/// crypto_provide / crypto_select bit: the whole stream is RC4-encrypted.
pub(crate) const CRYPTO_RC4: u32 = 0x02;

const VC: [u8; 8] = [0u8; 8];
const MAX_PAD: usize = 512;
/// How a plaintext BitTorrent handshake starts.
const PLAINTEXT_HANDSHAKE_PREFIX: &[u8; 20] = b"\x13BitTorrent protocol";

#[derive(Debug, thiserror::Error)]
pub enum MseError {
    #[error("peer closed the connection during the encryption handshake, {stage}")]
    Closed {
        stage: &'static str,
        #[source]
        source: Option<std::io::Error>,
    },
    #[error("I/O error during the encryption handshake, {stage}: {source:#}")]
    Io {
        stage: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("peer sent an invalid Diffie-Hellman public key")]
    InvalidPublicKey,
    #[error("no synchronisation within the padding window, {stage}")]
    NotSynchronised { stage: &'static str },
    #[error("the encrypted handshake names no torrent of this session")]
    UnknownInfoHash,
    #[error("verification constant mismatch")]
    BadVerificationConstant,
    #[error("padding of {0} bytes is longer than 512")]
    PadTooLong(usize),
    #[error("no common crypto method: peer offered {offered:#x}, we allow {allowed:#x}")]
    NoCommonMethod { offered: u32, allowed: u32 },
    #[error("peer selected crypto method {selected:#x}, we offered {offered:#x}")]
    BadSelect { selected: u32, offered: u32 },
    #[error("plaintext connection refused: encryption is required")]
    PlaintextRefused,
    #[error("initial payload of {0} bytes is too long")]
    PayloadTooLong(usize),
    #[error("timeout during the encryption handshake")]
    Timeout,
}

fn io_error(stage: &'static str, e: std::io::Error) -> MseError {
    use std::io::ErrorKind::*;
    match e.kind() {
        UnexpectedEof | ConnectionReset | ConnectionAborted | BrokenPipe => MseError::Closed {
            stage,
            source: Some(e),
        },
        _ => MseError::Io { stage, source: e },
    }
}

fn hash(parts: &[&[u8]]) -> [u8; 20] {
    let mut h = Sha1::new();
    for p in parts {
        h.update(p);
    }
    h.finish()
}

fn xor20(a: [u8; 20], b: [u8; 20]) -> [u8; 20] {
    std::array::from_fn(|i| a[i] ^ b[i])
}

/// `HASH('req2', info_hash)`: the name an encryption handshake gives a
/// torrent, masked on the wire with `HASH('req3', S)`.
pub(crate) fn obfuscated_hash(info_hash: &Id20) -> Id20 {
    Id20::new(hash(&[b"req2", &info_hash.0]))
}

/// What an accepted connection may be taken as, over the session's policy
/// and every torrent's: encrypted while any of them is not `Disabled`,
/// plaintext while any is not `Require`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Incoming {
    pub encrypted: bool,
    pub plaintext: bool,
}

/// The values both sides derive from the shared secret and the info hash.
pub(crate) struct Keys {
    pub req1: [u8; 20],
    /// HASH('req2', SKEY) xor HASH('req3', S)
    pub req2_xor_req3: [u8; 20],
    /// Keys traffic from the initiator to the responder.
    pub key_a: [u8; 20],
    /// Keys traffic from the responder to the initiator.
    pub key_b: [u8; 20],
}

impl Keys {
    pub fn derive(s: &[u8; dh::KEY_LEN], skey: &Id20) -> Self {
        Self {
            req1: hash(&[b"req1", s]),
            req2_xor_req3: xor20(hash(&[b"req2", &skey.0]), hash(&[b"req3", s])),
            key_a: hash(&[b"keyA", s, &skey.0]),
            key_b: hash(&[b"keyB", s, &skey.0]),
        }
    }
}

fn random_pad() -> Vec<u8> {
    let mut rng = rand::rng();
    let len = rng.random_range(0..=MAX_PAD);
    (0..len).map(|_| rng.random::<u8>()).collect()
}

/// The handshake's view of the incoming bytes: everything read so far, so
/// that bytes read past a field stay available for the next one and, at the
/// end, for the payload stream.
struct Wire<'a> {
    r: &'a mut BoxAsyncReadVectored,
    buf: Vec<u8>,
}

impl Wire<'_> {
    async fn read_more(&mut self, stage: &'static str) -> Result<(), MseError> {
        let mut tmp = [0u8; 1024];
        let n = self
            .r
            .read(&mut tmp)
            .await
            .map_err(|e| io_error(stage, e))?;
        if n == 0 {
            return Err(MseError::Closed {
                stage,
                source: None,
            });
        }
        self.buf.extend_from_slice(&tmp[..n]);
        Ok(())
    }

    async fn fill(&mut self, len: usize, stage: &'static str) -> Result<(), MseError> {
        while self.buf.len() < len {
            self.read_more(stage).await?;
        }
        Ok(())
    }

    /// Find `pattern` starting at or after `from`, no later than `from + max_pad`.
    /// Returns the offset just past it.
    async fn sync(
        &mut self,
        from: usize,
        pattern: &[u8],
        max_pad: usize,
        stage: &'static str,
    ) -> Result<usize, MseError> {
        let window_end = from + max_pad + pattern.len();
        loop {
            let end = self.buf.len().min(window_end);
            if end >= from + pattern.len()
                && let Some(i) = self.buf[from..end]
                    .windows(pattern.len())
                    .position(|w| w == pattern)
            {
                return Ok(from + i + pattern.len());
            }
            if self.buf.len() >= window_end {
                return Err(MseError::NotSynchronised { stage });
            }
            self.read_more(stage).await?;
        }
    }

    /// Decrypt `len` bytes at `pos` in place and return them.
    async fn decrypt(
        &mut self,
        pos: usize,
        len: usize,
        cipher: &mut Rc4,
        stage: &'static str,
    ) -> Result<&[u8], MseError> {
        self.fill(pos + len, stage).await?;
        cipher.apply(&mut self.buf[pos..pos + len]);
        Ok(&self.buf[pos..pos + len])
    }
}

fn be_u16(b: &[u8]) -> usize {
    usize::from(u16::from_be_bytes([b[0], b[1]]))
}

fn be_u32(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

/// The connection after a successful handshake.
pub(crate) struct Established {
    pub reader: BoxAsyncReadVectored,
    pub writer: Outbound,
    /// What the handshake encrypted: none of it for a plaintext BitTorrent
    /// handshake that never went through the key exchange.
    pub encryption: Encryption,
}

/// The payload-stream halves once the method is settled. `rest` is what was
/// read past the handshake: still encrypted under RC4, already plain otherwise.
fn establish(
    reader: BoxAsyncReadVectored,
    writer: BoxAsyncWrite,
    mut prefix: Vec<u8>,
    mut rest: Vec<u8>,
    method: u32,
    mut dec: Rc4,
    enc: Rc4,
) -> Established {
    if method == CRYPTO_RC4 {
        dec.apply(&mut rest);
        prefix.append(&mut rest);
        Established {
            reader: Box::new(DecryptingReader::new(prefix, reader, Some(dec))),
            writer: Outbound::rc4(writer, enc),
            encryption: Encryption::Rc4,
        }
    } else {
        prefix.append(&mut rest);
        Established {
            reader: DecryptingReader::new(prefix, reader, None).into_boxed(),
            writer: writer.into(),
            encryption: Encryption::HeaderOnly,
        }
    }
}

/// Run the handshake as the side that opened the connection. `initial_payload`
/// (the BitTorrent handshake) goes in IA; `offer` is the crypto_provide bitmask.
pub(crate) async fn initiate(
    mut reader: BoxAsyncReadVectored,
    mut writer: BoxAsyncWrite,
    info_hash: &Id20,
    offer: u32,
    initial_payload: &[u8],
) -> Result<Established, MseError> {
    let ia_len = u16::try_from(initial_payload.len())
        .map_err(|_| MseError::PayloadTooLong(initial_payload.len()))?;

    let x = dh::random_private_key();
    let mut msg = dh::public_key(&x).to_vec();
    msg.extend_from_slice(&random_pad());
    writer
        .write_all(&msg)
        .await
        .map_err(|e| io_error("sending our public key", e))?;

    let mut wire = Wire {
        r: &mut reader,
        buf: Vec::new(),
    };
    let stage = "reading the peer's public key";
    wire.fill(dh::KEY_LEN, stage).await?;
    let mut yb = [0u8; dh::KEY_LEN];
    yb.copy_from_slice(&wire.buf[..dh::KEY_LEN]);
    let s = dh::shared_secret(&x, &yb).ok_or(MseError::InvalidPublicKey)?;
    let keys = Keys::derive(&s, info_hash);
    let mut enc = Rc4::new_discard_1024(&keys.key_a);
    let mut dec = Rc4::new_discard_1024(&keys.key_b);

    let mut msg = Vec::with_capacity(40 + 16 + initial_payload.len());
    msg.extend_from_slice(&keys.req1);
    msg.extend_from_slice(&keys.req2_xor_req3);
    let encrypted_from = msg.len();
    msg.extend_from_slice(&VC);
    msg.extend_from_slice(&offer.to_be_bytes());
    msg.extend_from_slice(&0u16.to_be_bytes()); // len(PadC)
    msg.extend_from_slice(&ia_len.to_be_bytes());
    msg.extend_from_slice(initial_payload);
    enc.apply(&mut msg[encrypted_from..]);
    writer
        .write_all(&msg)
        .await
        .map_err(|e| io_error("sending the crypto offer", e))?;

    let mut encrypted_vc = VC;
    dec.clone().apply(&mut encrypted_vc);
    let mut pos = wire
        .sync(
            dh::KEY_LEN,
            &encrypted_vc,
            MAX_PAD,
            "looking for the peer's verification constant",
        )
        .await?;
    dec.skip(VC.len());

    let stage = "reading the peer's crypto selection";
    let header = wire.decrypt(pos, 6, &mut dec, stage).await?;
    let selected = be_u32(&header[..4]);
    let pad_d = be_u16(&header[4..6]);
    pos += 6;
    if pad_d > MAX_PAD {
        return Err(MseError::PadTooLong(pad_d));
    }
    wire.decrypt(pos, pad_d, &mut dec, stage).await?;
    pos += pad_d;
    if selected.count_ones() != 1 || selected & offer == 0 {
        return Err(MseError::BadSelect {
            selected,
            offered: offer,
        });
    }

    let rest = wire.buf.split_off(pos);
    Ok(establish(
        reader,
        writer,
        Vec::new(),
        rest,
        selected,
        dec,
        enc,
    ))
}

/// Run the handshake as the side that accepted the connection. A plaintext
/// BitTorrent handshake is passed through when `allow_plaintext_handshake`;
/// anything else is taken as the start of the key exchange. `find` names
/// the torrent whose `HASH('req2', info_hash)` it is given, with that
/// torrent's policy, which sets the crypto methods we may select. An
/// encrypted connection comes back with the info hash it named (SKEY).
pub(crate) async fn respond(
    mut reader: BoxAsyncReadVectored,
    mut writer: BoxAsyncWrite,
    allow_plaintext_handshake: bool,
    find: impl Fn(&Id20) -> Option<(Id20, EncryptionPolicy)>,
) -> Result<(Established, Option<Id20>), MseError> {
    let mut wire = Wire {
        r: &mut reader,
        buf: Vec::new(),
    };
    wire.fill(PLAINTEXT_HANDSHAKE_PREFIX.len(), "reading the first bytes")
        .await?;
    if wire.buf.starts_with(PLAINTEXT_HANDSHAKE_PREFIX) {
        if !allow_plaintext_handshake {
            return Err(MseError::PlaintextRefused);
        }
        let prefix = std::mem::take(&mut wire.buf);
        let established = Established {
            reader: DecryptingReader::new(prefix, reader, None).into_boxed(),
            writer: writer.into(),
            encryption: Encryption::None,
        };
        return Ok((established, None));
    }

    wire.fill(dh::KEY_LEN, "reading the peer's public key")
        .await?;
    let mut ya = [0u8; dh::KEY_LEN];
    ya.copy_from_slice(&wire.buf[..dh::KEY_LEN]);

    let x = dh::random_private_key();
    let mut msg = dh::public_key(&x).to_vec();
    msg.extend_from_slice(&random_pad());
    writer
        .write_all(&msg)
        .await
        .map_err(|e| io_error("sending our public key", e))?;
    let s = dh::shared_secret(&x, &ya).ok_or(MseError::InvalidPublicKey)?;

    let req1 = hash(&[b"req1", &s]);
    let mut pos = wire
        .sync(
            dh::KEY_LEN,
            &req1,
            MAX_PAD,
            "looking for the peer's req1 hash",
        )
        .await?;
    wire.fill(pos + 20, "reading the torrent hash").await?;
    // libtorrent `session_impl::find_encrypted_torrent`: unmask once and
    // look the torrent up by its obfuscated hash.
    let req3 = hash(&[b"req3", &s]);
    let mut masked = [0u8; 20];
    masked.copy_from_slice(&wire.buf[pos..pos + 20]);
    let (skey, policy) =
        find(&Id20::new(xor20(masked, req3))).ok_or(MseError::UnknownInfoHash)?;
    let allowed = policy.methods();
    pos += 20;

    let keys = Keys::derive(&s, &skey);
    let mut dec = Rc4::new_discard_1024(&keys.key_a);
    let mut enc = Rc4::new_discard_1024(&keys.key_b);

    let stage = "reading the peer's crypto offer";
    let header = wire.decrypt(pos, 14, &mut dec, stage).await?;
    if header[..8] != VC {
        return Err(MseError::BadVerificationConstant);
    }
    let offered = be_u32(&header[8..12]);
    let pad_c = be_u16(&header[12..14]);
    pos += 14;
    if pad_c > MAX_PAD {
        return Err(MseError::PadTooLong(pad_c));
    }
    let pad_c_and_len = wire.decrypt(pos, pad_c + 2, &mut dec, stage).await?;
    let ia_len = be_u16(&pad_c_and_len[pad_c..]);
    pos += pad_c + 2;
    let ia = wire
        .decrypt(pos, ia_len, &mut dec, "reading the initial payload")
        .await?
        .to_vec();
    pos += ia_len;

    let selected = [CRYPTO_RC4, CRYPTO_PLAINTEXT]
        .into_iter()
        .find(|m| offered & allowed & m != 0)
        .ok_or(MseError::NoCommonMethod { offered, allowed })?;

    let mut msg = Vec::with_capacity(14);
    msg.extend_from_slice(&VC);
    msg.extend_from_slice(&selected.to_be_bytes());
    msg.extend_from_slice(&0u16.to_be_bytes()); // len(PadD)
    enc.apply(&mut msg);
    writer
        .write_all(&msg)
        .await
        .map_err(|e| io_error("sending our crypto selection", e))?;

    let rest = wire.buf.split_off(pos);
    Ok((
        establish(reader, writer, ia, rest, selected, dec, enc),
        Some(skey),
    ))
}

/// Upgrade an accepted connection as `incoming` allows: an encrypted
/// handshake selects a method its torrent's policy allows, the torrent
/// found by `find` as [`respond`] does, and a plaintext one passes while
/// `incoming.plaintext`, to be judged by its torrent once its BitTorrent
/// handshake names it. The reader is replaced in place and the writer
/// comes back with the cipher it writes under, and with the info hash an
/// encrypted handshake named. When nothing is taken encrypted no byte is
/// read and the writer comes back as it was.
pub(crate) async fn upgrade_incoming(
    incoming: Incoming,
    reader: &mut BoxAsyncReadVectored,
    writer: BoxAsyncWrite,
    timeout: Duration,
    find: impl Fn(&Id20) -> Option<(Id20, EncryptionPolicy)>,
) -> Result<(Encryption, Outbound, Option<Id20>), MseError> {
    if !incoming.encrypted {
        return Ok((Encryption::None, writer.into(), None));
    }
    let r = std::mem::replace(reader, Box::new(tokio::io::empty().into_vectored_compat()));
    let (established, skey) =
        tokio::time::timeout(timeout, respond(r, writer, incoming.plaintext, find))
            .await
            .map_err(|_| MseError::Timeout)??;
    *reader = established.reader;
    Ok((established.encryption, established.writer, skey))
}

/// The crypto_provide bitmask an outgoing connection offers under `policy`.
pub(crate) fn outgoing_offer(policy: EncryptionPolicy) -> u32 {
    policy.methods()
}

#[cfg(test)]
mod tests;
