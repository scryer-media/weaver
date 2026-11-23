// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use crate::meter::Meter;
use crate::tracker::Unreachable;
use crate::{
    listen::PeerAcceptor,
    read_buf::ReadBuf,
    session_stats::incoming::IncomingEnd,
    stream_connect::{ConnectionKind, Link},
    torrent_state::{AddIncomingPeerResult, TorrentStateLive},
    type_aliases::{BoxAsyncReadVectored, BoxAsyncWrite},
};
use anyhow::{Context, bail};
use futures::{StreamExt, TryFutureExt, stream::FuturesUnordered};
use tracing::{Instrument, debug, debug_span, trace, warn};

use super::{ACCEPT_BACKOFF, CheckedIncomingConnection, Session};

impl Session {
    pub(super) async fn check_incoming_connection(
        self: Arc<Self>,
        addr: SocketAddr,
        kind: ConnectionKind,
        tls: bool,
        reader: BoxAsyncReadVectored,
        writer: BoxAsyncWrite,
    ) -> anyhow::Result<(Arc<TorrentStateLive>, CheckedIncomingConnection)> {
        let handshake_timeout = self.peer_opts.handshake_timeout();
        let refused = |end: IncomingEnd| self.stats.incoming.ended(kind, end);

        // The session's one address rule, whichever direction the connection
        // goes (libtorrent `session_impl::incoming_connection` refuses a
        // filtered endpoint with `peer_blocked_alert::ip_filter`; it never
        // sees one of a family it has no listen socket for).
        if let Err(why) = self.may_reach(addr.ip()) {
            if why == Unreachable::Filtered {
                self.stats
                    .counters
                    .blocked_incoming
                    .fetch_add(1, Ordering::Relaxed);
                refused(IncomingEnd::Filtered);
            } else {
                refused(IncomingEnd::Family);
            }
            bail!("incoming connection refused: {} is {why}", addr.ip());
        }
        // An address banned for bad data sent to any torrent is refused
        // before a byte is read, whichever torrent it greets.
        if self.bad_data_bans.contains(addr.ip()) {
            refused(IncomingEnd::Banned);
            bail!("incoming connection refused: {} is banned", addr.ip());
        }
        // No peer on a privileged port is kept, whichever way it connects
        // (libtorrent `torrent::privileged_port_updated` disconnects one).
        if self.refuses_port(addr.port()) {
            refused(IncomingEnd::PrivilegedPort);
            bail!("incoming connection refused: {addr} is on a privileged port");
        }

        // Every byte from here on crossed the peer's socket for the torrent
        // it greets, and is charged to it once the connection is its own.
        let (meter, mut reader, writer) = Meter::wrap(reader, writer);

        // A connection on the SSL listener is TLS from its first byte, under
        // the torrent its SNI names: that torrent's CA checks the peer's
        // chain and our identity under it answers. No encryption handshake
        // runs inside it.
        let (encryption, writer, named, skey_named) = if tls {
            let greeted = tokio::time::timeout(handshake_timeout, async {
                let incoming = crate::ssl::Incoming::start(reader, writer)
                    .await
                    .context("not a TLS connection")?;
                let sni = incoming
                    .server_name()
                    .context("the TLS connection names no torrent")?;
                let (id, config) = crate::ssl::SslTorrent::hash_named(&sni)
                    .and_then(|hash| {
                        let db = self.db.read();
                        let (id, t) = db.by_hash(hash)?;
                        let ssl = t.shared().ssl.as_ref()?;
                        ssl.answers_to(&sni).then(|| (id, ssl.server_config()))
                    })
                    .with_context(|| format!("no SSL torrent is named {sni}"))?;
                let config =
                    config.with_context(|| format!("torrent {id} has no identity to present"))?;
                let (r, w) = incoming.accept(config).await.context("TLS handshake")?;
                anyhow::Ok((r, w, id))
            })
            .await
            .context("TLS handshake timed out");
            let (r, w, named) = match greeted {
                Ok(Ok(greeted)) => greeted,
                Ok(Err(e)) | Err(e) => {
                    refused(IncomingEnd::Tls);
                    return Err(e);
                }
            };
            reader = r;
            (crate::mse::Encryption::None, w.into(), Some(named), None)
        } else {
            // With encryption enabled, an encrypted connection is unwrapped here
            // and a plaintext one passed through (or refused); with it disabled
            // the halves are left as they are. A method is settled only when the
            // encryption handshake ran.
            // Each connection is kept under its torrent's encryption: an
            // encrypted one names its torrent in the encryption handshake, a
            // plaintext one only in its BitTorrent handshake.
            let session_policy = self.connector.encryption();
            let incoming = self.db.read().index.incoming(session_policy);
            let (encryption, writer, skey) = crate::mse::upgrade_incoming(
                incoming,
                &mut reader,
                writer,
                handshake_timeout,
                |obfuscated| {
                    let (hash, _, policy) = self
                        .db
                        .read()
                        .index
                        .by_obfuscated(obfuscated, session_policy)?;
                    Some((hash, policy))
                },
            )
            .await
            .inspect_err(|_| refused(IncomingEnd::Encryption))
            .context("error in the encryption handshake")?;
            (encryption, writer, None, skey)
        };

        let mut read_buf = ReadBuf::new();
        let h = read_buf
            .read_handshake(&mut reader, handshake_timeout)
            .await
            .inspect_err(|_| refused(IncomingEnd::Handshake))
            .context("error reading handshake")?;
        trace!("received handshake from {addr}: {:?}", h);

        let (id, torrent) = self
            .db
            .read()
            .by_hash(h.info_hash)
            .map(|(id, t)| (id, t.clone()))
            .with_context(|| format!("didn't find a matching torrent {:?}", h.info_hash))
            .inspect_err(|_| refused(IncomingEnd::UnknownTorrent))?;
        // The encryption handshake keyed the stream to one torrent; the
        // BitTorrent handshake must greet that torrent, by either of a
        // hybrid's ids (libtorrent `bt_peer_connection::on_receive`, "verify
        // info hash" once attached by `find_encrypted_torrent`).
        if let Some(skey) = skey_named
            && !torrent.info_hashes().answers_to(skey)
        {
            refused(IncomingEnd::Encryption);
            bail!(
                "the encryption handshake named {skey:?} and the BitTorrent handshake greets torrent {id}"
            );
        }
        if h.peer_id == torrent.shared().peer_id {
            refused(IncomingEnd::Ourselves);
            bail!("seems like we are connecting to ourselves, ignoring");
        }
        // An SSL torrent takes a connection only through TLS under its own
        // CA, and a TLS connection only greets the torrent it named.
        match named {
            Some(named) if named != id => {
                refused(IncomingEnd::Tls);
                bail!("the TLS connection named torrent {named} and greets torrent {id}")
            }
            Some(_) => {}
            None if torrent.is_ssl() => {
                refused(IncomingEnd::NotAllowed);
                bail!("torrent {id} is an SSL torrent and takes no plain connection")
            }
            None => {
                let policy = torrent
                    .shared()
                    .options
                    .peer_opts
                    .encryption_under(self.connector.encryption());
                if !policy.allows(encryption) {
                    refused(IncomingEnd::NotAllowed);
                    bail!("torrent {id} keeps no {encryption:?} connection under {policy:?}");
                }
            }
        }

        let live = torrent
            .live_wait_initializing(Duration::from_secs(5))
            .await
            .with_context(|| format!("torrent {id} is not live, ignoring connection"))
            .inspect_err(|_| refused(IncomingEnd::NotLive))?;

        Ok((
            live,
            CheckedIncomingConnection {
                addr,
                reader,
                writer,
                kind,
                handshake: h,
                read_buf,
                encryption,
                meter,
            },
        ))
    }

    /// Accepts on a listener's sockets; `tls` says it is the SSL listener.
    pub(super) fn spawn_listeners(
        self: &Arc<Self>,
        listening: crate::listen::Listening,
        tls: bool,
        max_pending_incoming_handshake_checks: usize,
    ) {
        let addr = listening.addr;
        for acceptor in listening.acceptors() {
            let kind = acceptor.kind();
            let this = self.clone();
            self.spawn_on_swarm(
                kind.listen_span(self.rs(), addr, tls),
                kind.listen_task(),
                async move {
                    this.task_listener(acceptor, tls, max_pending_incoming_handshake_checks)
                        .await
                },
            );
        }
    }

    async fn task_listener(
        self: Arc<Self>,
        l: Box<dyn PeerAcceptor>,
        tls: bool,
        max_pending_incoming_handshake_checks: usize,
    ) -> anyhow::Result<()> {
        let kind = l.kind();
        let mut futs = FuturesUnordered::new();
        let session = Arc::downgrade(&self);
        drop(self);
        // After a failed accept (out of descriptors, say) the listener rests
        // for ACCEPT_BACKOFF while the handshakes in hand go on, so one that
        // completes frees its descriptor or takes its slot meanwhile.
        let mut backoff = std::pin::pin!(tokio::time::sleep(Duration::ZERO));
        let mut resting = false;

        loop {
            tokio::select! {
                () = backoff.as_mut(), if resting => resting = false,
                r = l.accept(), if !resting && futs.len() < max_pending_incoming_handshake_checks => {
                    match r {
                        Ok(Link { remote: addr, read, write, home, .. }) => {
                            trace!("accepted connection from {addr}");
                            let session = session.upgrade().context("session is dead")?;
                            // Dropping the halves closes a connection past the
                            // session's cap and its slack.
                            let Some(slot) = session.connection_slots.try_accept() else {
                                debug!(%addr, "at the session's connection limit, closing incoming connection");
                                session.stats.incoming.ended(kind, IncomingEnd::SessionLimit);
                                continue;
                            };
                            let span = debug_span!(parent: session.rs(), "incoming", addr=%addr);
                            futs.push(
                                session.check_incoming_connection(addr, kind, tls, read, write)
                                    .map_ok(|(live, checked)| (live, checked, slot, home))
                                    .map_err(|e| {
                                        debug!("error checking incoming connection: {e:#}");
                                        e
                                    })
                                    .instrument(span)
                            );
                        }
                        Err(e) if l.is_closed(&e) => {
                            debug!("listener closed: {e:#}");
                            return Ok(());
                        }
                        Err(e) => {
                            warn!("error accepting: {e:#}");
                            backoff.as_mut().reset(tokio::time::Instant::now() + ACCEPT_BACKOFF);
                            resting = true;
                        }
                    }
                },
                // Every finished check is taken here, a failed one too. A
                // pattern that refuses a value turns its branch off for the
                // rest of this select!, so a failed check would leave every
                // other check unpolled until the next accept. The failure is
                // logged where the check is pushed.
                Some(r) = futs.next(), if !futs.is_empty() => {
                    let Ok((live, checked, slot, home)) = r else { continue };
                    let (addr, kind) = (checked.addr, checked.kind);
                    let added = live.add_incoming_peer(checked, slot, home);
                    let session = session.upgrade();
                    if let Some(session) = &session {
                        session.stats.incoming.ended(kind, IncomingEnd::of(&added));
                        if let Ok(AddIncomingPeerResult::AlreadyActive(already)) = &added {
                            session.stats.incoming.already(kind, *already);
                        }
                    }
                    match added {
                        Ok(AddIncomingPeerResult::Added) => {
                            if let Some(session) = session {
                                session.settle_over_limit(&live, addr.into());
                            }
                        }
                        Ok(_) => {}
                        Err(e) => {
                            warn!(?addr, ?kind, "error handing over incoming connection: {e:#}");
                        }
                    }
                },
                else => continue,
            }
        }
    }
}
