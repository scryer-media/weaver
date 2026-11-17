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

use crate::buffers::ByteBuf;
use crate::core::{lengths::ChunkInfo, spawn_utils::spawn_with_cancel};
use crate::peer_protocol::{
    Handshake, MY_EXTENDED_UT_HOLEPUNCH, Message, PartialBlock,
    extended::{ExtendedMessage, handshake::ExtendedHandshake, ut_metadata::UtMetadata},
};
use anyhow::Context;
use futures::FutureExt;
use tracing::{debug, debug_span, trace};

use crate::{
    disk::{Block, BlockPool},
    limits::Channel,
    peer_connection::{AnswerBacklog, PeerConnectionHandler},
    session_stats::swarm::{self, Direction, MessageKind},
    stream_connect::ConnectionKind,
};

use super::peer::{LivePeerState, MAX_ALLOWED_IN_REQUEST_QUEUE};

use super::{PeerHandler, UploadReader, format_peer_client_name};

impl PeerConnectionHandler for &'_ PeerHandler {
    fn on_connected(&self, connection_time: Duration) {
        self.counters
            .outgoing_connections
            .fetch_add(1, Ordering::Relaxed);
        #[allow(clippy::cast_possible_truncation)]
        self.counters
            .total_time_connecting_ms
            .fetch_add(connection_time.as_millis() as u64, Ordering::Relaxed);
    }

    fn encrypt_outgoing(&self) -> bool {
        self.state.peers.encrypt_outgoing(self.addr)
    }

    fn seeding(&self) -> bool {
        self.state.peers.is_finished()
    }

    fn tcp_wants(&self) -> Option<&crate::stream_connect::TcpWants> {
        Some(&self.state.tcp_wants)
    }

    fn on_utp_dialed(&self, opened: bool) {
        self.state.peers.utp_dialed(self.addr, opened);
    }

    fn on_outgoing_method_worked(&self, encrypted: bool) {
        self.state
            .peers
            .outgoing_method_worked(self.addr, encrypted);
    }

    fn on_handshake_encryption(&self, encryption: crate::mse::Encryption) {
        if let Some(transport) = self.transport() {
            let direction = if self.incoming {
                Direction::In
            } else {
                Direction::Out
            };
            let encryption = swarm::Encryption::of(encryption);
            self.swarm().handshake(transport, direction, encryption);
        }
        self.state.peers.handshake_encryption(self.addr, encryption);
    }

    async fn on_received_block(&self, block: Block) -> anyhow::Result<()> {
        // As a Piece message first (see `on_received_message`).
        if !self.first_message_received.swap(true, Ordering::Relaxed) {
            self.state.peers.connection_delivered(self.addr);
            self.on_have_all_or_none(0).context("on_have_none")?;
        }
        self.queue_block(block).await.context("on_received_piece")
    }

    fn block_pool(&self) -> Option<BlockPool> {
        Some(self.state.shared.spawner.pool().clone())
    }

    async fn disk_room(&self) {
        if self.disk.blocked() {
            self.disk.room(self.waits_pool()).await;
            self.state.shared.spawner.stats().peer_resumed();
        }
    }

    async fn on_received_message(&self, message: Message<'_>) -> anyhow::Result<()> {
        let first = !self.first_message_received.swap(true, Ordering::Relaxed);
        if first {
            // The connection works, so the failures before it no longer count.
            self.state.peers.connection_delivered(self.addr);
            // The peer's pieces are known from its first message. One that
            // is not a bitfield, Have All or Have None means the bitfield
            // was omitted, which is the same as Have None (libtorrent
            // `peer_connection::incoming_have`, `incoming_request`,
            // `incoming_piece`). It is taken before the message is, so a
            // first HAVE lands in pieces of the torrent's size.
            if !matches!(
                &message,
                Message::Bitfield(..) | Message::HaveAll | Message::HaveNone
            ) {
                self.on_have_all_or_none(0).context("on_have_none")?;
            }
        }

        match message {
            Message::Request(request) => {
                self.on_download_request(request)
                    .context("on_download_request")?;
            }
            Message::Bitfield(b) => self.on_bitfield(b.as_ref()).context("on_bitfield")?,
            Message::HaveAll => self.on_have_all_or_none(0xff).context("on_have_all")?,
            Message::HaveNone => self.on_have_all_or_none(0).context("on_have_none")?,
            Message::Choke => self.on_i_am_choked().context("on_i_am_choked")?,
            Message::Reject(request) => self.on_rejected(request).context("on_rejected")?,
            Message::Unchoke => self.on_i_am_unchoked(),
            Message::Interested => self.on_peer_interested(),
            Message::AllowedFast(index) => {
                self.on_allowed_fast(index).context("on_allowed_fast")?
            }
            Message::Suggest(index) => self.on_suggest(index).context("on_suggest")?,
            Message::Piece(piece) => self
                .on_received_piece(piece)
                .await
                .context("on_received_piece")?,
            Message::KeepAlive => {
                trace!("keepalive received");
            }
            Message::Have(h) => self.on_have(h).context("on_have")?,
            Message::Port(port) => self.on_dht_port(port),
            Message::Hashes(hashes) => self.on_hashes(hashes).context("on_hashes")?,
            Message::HashReject(request) => self.on_hash_reject(request),
            Message::HashRequest(request) => {
                self.on_hash_request(request).context("on_hash_request")?
            }
            Message::NotInterested => {
                if let Some(session) = self.state.shared.session.upgrade() {
                    session.choker.peer_not_interested(&self.state, self.addr);
                }
            }
            Message::Cancel(request) => self.on_upload_cancelled(request),
            Message::Extended(ExtendedMessage::UtMetadata(UtMetadata::Request(
                metadata_piece_id,
            ))) => {
                self.send_metadata_piece(metadata_piece_id)
                    .with_context(|| format!("error sending metadata piece {metadata_piece_id}"))?;
            }
            Message::Extended(ExtendedMessage::UtHolepunch(msg)) => self.on_holepunch(msg),
            Message::Extended(ExtendedMessage::UploadOnly(upload_only)) => {
                self.on_upload_only(upload_only)
            }
            Message::Extended(ExtendedMessage::DontHave(index)) => self.on_dont_have(index),
            Message::Extended(ExtendedMessage::WrongSize(id)) => {
                trace!(id, "ignoring an extended message of the wrong size");
            }
            Message::Extended(ExtendedMessage::NotAdvertised(id)) => {
                trace!(id, "ignoring an extended message we did not advertise");
            }
            Message::Extended(ExtendedMessage::UtPex(pex)) => {
                if self.state.pex {
                    self.on_pex_message(pex)?;
                } else {
                    trace!(
                        "ignoring a PEX message from {}: we did not advertise it",
                        self.addr
                    );
                }
            }
            // A metadata piece or reject we did not ask for (libtorrent
            // `ut_metadata_peer_plugin::on_extended` logs an unwanted piece
            // and reads on). Its payload is not logged.
            _ => trace!("ignoring a message from {} we did not ask for", self.addr),
        };
        // A seed that has sent us data is a complete copy seen now; one that
        // has not may only say it is a seed (libtorrent
        // `peer_connection::incoming_have` and `on_receive_data`).
        if self.counters.fetched_bytes.load(Ordering::Relaxed) > 0
            && self
                .state
                .peers
                .with_live(self.addr, |live| live.seeder)
                .unwrap_or(false)
        {
            self.state.seen_complete(tokio::time::Instant::now());
        }
        if first {
            // The peer's pieces are known now: its bitfield, or none.
            self.super_seed();
        }
        Ok(())
    }

    /// What we have, as libtorrent `bt_peer_connection::write_bitfield` tells
    /// it: on a fast connection Have All when we have every piece and Have
    /// None when we show none, a bitfield otherwise. With upload off, or
    /// while we super-seed, we show no piece.
    fn serialize_bitfield_message_to_buf(&self, buf: &mut [u8]) -> anyhow::Result<usize> {
        let fast = self.speaks_fast();
        let total_pieces = self.state.lengths.total_pieces() as usize;
        let upload = !self.state.torrent().options.disable_upload();
        let g = self.state.lock_read("serialize_bitfield_message_to_buf");
        let have = g.get_chunks()?.get_have_pieces();
        let pieces = &have.as_slice()[..total_pieces];
        let msg = if fast && (!upload || pieces.not_any() || self.state.super_seeding) {
            Message::HaveNone
        } else if fast && pieces.all() {
            Message::HaveAll
        } else {
            Message::Bitfield(ByteBuf(have.as_bytes()))
        };
        let len = msg.serialize(buf, &Default::default)?;
        trace!("sending: {:?}, length={}", &msg, len);
        Ok(len)
    }

    fn on_handshake(
        &self,
        handshake: Handshake,
        ckind: ConnectionKind,
        speaks_v2: bool,
    ) -> anyhow::Result<()> {
        self.speaks_v2.store(speaks_v2, Ordering::Relaxed);
        self.transport.store(ckind as u8 + 1, Ordering::Relaxed);
        // A connection closed for its peer id is closed by us, which is no
        // failure (libtorrent disconnects with `duplicate_peer_id` at normal
        // severity).
        let supports_dht = handshake.supports_dht();
        if !self
            .state
            .set_peer_live(self.addr, handshake, ckind, &self.upload)
        {
            return Err(crate::Error::Disconnect.into());
        }
        if supports_dht {
            self.send_dht_port();
        }
        Ok(())
    }

    async fn request_bandwidth(&self, channel: Channel, bytes: u64) -> u64 {
        let mut grant = std::pin::pin!(self.state.grant_bandwidth(channel, bytes));
        if let Some(granted) = (&mut grant).now_or_never() {
            return granted;
        }
        // While its reads wait on the limits, the peer is not held to its
        // request and piece timeouts (libtorrent `peer_connection::second_tick`
        // snubs and times out only a peer it reads, `bw_network`).
        let gate = |waiting: bool| {
            self.state
                .peers
                .with_live_mut(self.addr, "download quota", |live| {
                    live.set_waiting_for_download_quota(waiting)
                })
        };
        if channel == Channel::Download {
            gate(true);
        }
        let granted = grant.await;
        if channel == Channel::Download {
            gate(false);
        }
        granted
    }

    fn return_bandwidth(&self, channel: Channel, bytes: u64) {
        self.state.give_back_bandwidth(channel, bytes);
    }

    fn on_wire_messages(&self, direction: Direction, kind: MessageKind, count: u64) {
        self.state
            .session_stats
            .swarm
            .messages(direction, kind, count);
    }

    fn on_protocol_bytes(&self, channel: Channel, bytes: u64) {
        let (peer, torrent, session) = match channel {
            Channel::Download => (
                &self.counters.fetched_protocol_bytes,
                &self.state.stats.fetched_protocol_bytes,
                &self.state.session_stats.counters.fetched_protocol_bytes,
            ),
            Channel::Upload => (
                &self.counters.uploaded_protocol_bytes,
                &self.state.stats.uploaded_protocol_bytes,
                &self.state.session_stats.counters.uploaded_protocol_bytes,
            ),
        };
        for counter in [peer, torrent, session] {
            counter.fetch_add(bytes, Ordering::Relaxed);
        }
        let direction = match channel {
            Channel::Download => Direction::In,
            Channel::Upload => Direction::Out,
        };
        self.count_bytes(direction, swarm::ByteKind::Protocol, bytes);
    }

    fn on_ip_overhead(&self, bytes: u64) {
        self.counters
            .ip_overhead_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// The part of a block of the torrent that arrived is not used.
    fn on_partial_block(&self, block: PartialBlock) {
        let lengths = &self.state.lengths;
        let names_a_block = lengths
            .validate_piece_index(block.index)
            .and_then(|index| lengths.chunk_info_from_received_data(index, block.begin, block.len))
            .is_some();
        if names_a_block {
            self.state
                .add_closing_redundant_bytes(block.received.into());
        }
    }

    fn download_wanted(&self) -> u64 {
        self.state
            .peers
            .with_live(self.addr, LivePeerState::download_wanted)
            .unwrap_or(0)
    }

    fn on_uploaded_bytes(&self, bytes: u32) {
        self.counters
            .uploaded_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.state
            .session_stats
            .counters
            .uploaded_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.count_bytes(Direction::Out, swarm::ByteKind::Payload, bytes.into());
    }

    // The torrent's count moves once per write, after its stamp: whoever
    // reads the count with these bytes in it reads this stamp or a later
    // one, never an older.
    fn on_payload_written(&self, bytes: u64) {
        let state = &self.state;
        let at = u64::try_from(state.upload_origin.elapsed().as_nanos()).unwrap_or(u64::MAX - 1);
        state.last_upload.fetch_max(at + 1, Ordering::Relaxed);
        // Sequentially consistent with the watch's count of listeners, so
        // the add and a listener's arrival cannot both miss each other: a
        // listener that read the total before this add is counted by the
        // load below and rung. The add also publishes the stamp above to
        // whoever reads the total.
        state
            .stats
            .uploaded_bytes
            .fetch_add(bytes, Ordering::SeqCst);
        if state.upload_watches.load(Ordering::SeqCst) > 0 {
            state.uploaded_notify.notify_waiters();
        }
    }

    // A block cancelled or choked off while the writer held it gave its
    // room back then, and the writer's claim of it is the next word from
    // the writer: the peer may want a turn by now.
    // Taken from the peer's send buffer, on the writer's thread, without
    // the peer map.
    fn claim_upload(&self, chunk: &ChunkInfo) -> bool {
        let Some(upload) = self.upload_ledger() else {
            return false;
        };
        let (claimed, turn) = upload.claim(chunk);
        if turn {
            self.state.upload_turn(self.addr);
        }
        claimed
    }

    fn upload_done(&self, chunk: &ChunkInfo) {
        self.uploads_done(std::slice::from_ref(chunk));
    }

    // One write's blocks free their room under one hold of the send buffer.
    // The send buffer fell below its watermark: the peer's next blocks wait
    // their turn.
    fn uploads_done(&self, chunks: &[ChunkInfo]) {
        if let Some(upload) = self.upload_ledger()
            && upload.done(chunks)
        {
            self.state.upload_turn(self.addr);
        }
    }

    // A seed-mode piece is checked on a blocking thread, and the peer's
    // writer waits for it (libtorrent `peer_connection::fill_send_buffer`
    // queues `disk_interface::async_hash` and keeps the request until
    // `on_seed_mode_hashed`).
    async fn serves(&self, chunk: &ChunkInfo) -> anyhow::Result<bool> {
        self.state.serve_seeded(chunk.piece_index).await
    }

    fn read_chunk(&self, chunk: &ChunkInfo, buf: &mut [u8]) -> anyhow::Result<()> {
        self.state.file_ops().read_chunk(self.addr, chunk, buf)
    }

    fn read_chunks(
        &self,
        chunks: &[ChunkInfo],
        bufs: &mut [&mut [u8]],
        held: Option<&mut crate::file_ops::HeldPiece>,
    ) -> std::result::Result<(), (usize, anyhow::Error)> {
        self.state
            .file_ops()
            .read_chunks(self.addr, chunks, bufs, held)
    }

    fn read_turns(&self) -> crate::storage::ReadTurns {
        crate::storage::ReadTurns::clone(&self.state.reads.load())
    }

    fn chunk_reader(&self) -> Option<Arc<dyn crate::peer_connection::ChunkReader>> {
        Some(Arc::new(UploadReader {
            state: self.state.clone(),
            addr: self.addr,
        }))
    }

    // Only a swarm peer, at an address, speaks the extension protocol: a
    // web seed sends no extended handshake, so the address-bound steps
    // below never meet one.
    fn on_extended_handshake(&self, hs: &ExtendedHandshake<ByteBuf>) -> anyhow::Result<()> {
        // Only a swarm peer, at an address, speaks the extension protocol.
        debug_assert!(
            self.addr.addr().is_some(),
            "a web seed sent an extended handshake"
        );
        if let Some(client_name) = hs.v.as_ref().and_then(format_peer_client_name) {
            self.state
                .peers
                .with_live_mut(self.addr, "update peer client name", |live| {
                    live.client_name = Some(client_name);
                });
        }
        self.state
            .peers
            .with_live_mut(self.addr, "update peer holepunch support", |live| {
                let holepunch = hs.m.ut_holepunch.is_some();
                if live.supports_holepunch != holepunch {
                    live.supports_holepunch = holepunch;
                    self.state.peers.pex_changed();
                }
                live.supports_upload_only = hs.m.upload_only.is_some();
            });
        // A nonzero `upload_only` says the peer only uploads; zero or none
        // says nothing (libtorrent `bt_peer_connection::on_extended_handshake`).
        if hs.upload_only.is_some_and(|upload_only| upload_only != 0) {
            self.on_upload_only(true);
        }
        // The peer saw a complete copy `complete_ago` seconds ago; a
        // negative one says it never did (libtorrent
        // `bt_peer_connection::on_extended_handshake`, into
        // `peer_connection::set_last_seen_complete`). Its word is the
        // swarm's status, never our own sighting.
        if let Some(ago) = hs.complete_ago.and_then(|ago| u64::try_from(ago).ok())
            && let Some(at) = tokio::time::Instant::now().checked_sub(Duration::from_secs(ago))
        {
            self.state
                .peers
                .with_live_mut(self.addr, "complete_ago", |live| {
                    live.last_seen_complete = Some(at)
                });
        }

        // The peer holds at most its `reqq` requests (libtorrent
        // `bt_peer_connection::on_extended_handshake`); our own limit stays
        // the ceiling.
        if let Some(reqq) = hs.reqq.and_then(|reqq| usize::try_from(reqq).ok())
            && reqq > 0
        {
            let request_window = reqq.min(self.state.max_request_window());
            let mut flow = self.lock_flow_control("update request window");
            if flow.request_window != request_window {
                debug!(
                    reqq,
                    request_window, "updated peer request window from extended handshake"
                );
                flow.request_window = request_window;
                self.notify_request_slots_changed();
            }
        }

        // A later handshake that names ut_pex again starts no second sender;
        // once one names it 0, the writer drops what the sender queues
        // (libtorrent `ut_pex_peer_plugin`, one per connection).
        if self.state.pex
            && hs.m.ut_pex.is_some()
            && let Some(at) = self.addr.addr()
            && !self.pex_started.swap(true, Ordering::Relaxed)
        {
            spawn_with_cancel(
                debug_span!(
                    parent: self.state.shared.span.clone(),
                    "sending_pex_to_peer",
                    peer = ?self.addr,
                ),
                format!(
                    "[{}][addr={}]sending_pex_to_peer",
                    self.state.shared.id, self.addr
                ),
                self.cancel_token.clone(),
                self.state
                    .clone()
                    .task_send_pex_to_peer(at, self.tx.clone()),
            );
        }
        // `yourip`, our address as the peer sees it, is the peer's vote on
        // our external address (libtorrent
        // `bt_peer_connection::on_extended_handshake`, into
        // `session_impl::set_external_address`).
        if let Some(yourip) = hs.yourip
            && let Some(ip) = self.addr.ip()
            && let Some(session) = self.state.shared.session.upgrade()
        {
            session.external_ips.cast_vote(
                yourip.0,
                crate::session::external_ip::Voter::Peer(ip),
                tokio::time::Instant::now(),
            );
        }
        // A peer that connected to us says where it listens: at the address
        // it connected from, on port `p`. Its own word on its address
        // (`ipv4`, `ipv6`) is never taken (libtorrent
        // `bt_peer_connection::on_extended_handshake`, which reads only `p`,
        // into `peer_list::update_peer_port`).
        if self.incoming
            && let Some(port) = hs.port().filter(|&port| port != 0)
            && let Some(ip) = self.addr.ip()
        {
            let listen = SocketAddr::new(ip, port);
            self.state
                .peers
                .with_peer_mut(self.addr, "update outgoing addr", |peer| {
                    peer.listens_at(listen, &self.state.peers)
                });
        }

        Ok(())
    }

    /// A fast connection always opens with what we have (BEP 6); a plain one
    /// only when we show a piece (libtorrent
    /// `bt_peer_connection::write_bitfield`).
    fn should_send_bitfield(&self) -> bool {
        if self.speaks_fast() {
            return true;
        }
        if self.state.torrent().options.disable_upload() {
            return false;
        }

        // A super-seed tells of no piece but those it hands out
        // (libtorrent `bt_peer_connection::write_bitfield`).
        !self.state.super_seeding && self.state.get_approx_have_bytes() > 0
    }

    fn update_my_extended_handshake(
        &self,
        handshake: &mut ExtendedHandshake<ByteBuf>,
    ) -> anyhow::Result<()> {
        // The metadata we share, and the extension that shares it, only when
        // we share it.
        let shared = self.shared_metadata();
        handshake.metadata_size = shared.and_then(|info| info.len().try_into().ok());
        handshake.m.ut_metadata = handshake.m.ut_metadata.filter(|_| shared.is_some());
        handshake.m.ut_pex = handshake.m.ut_pex.filter(|_| self.state.pex);
        // The most blocks the peer may have asked us for and not been sent
        // (libtorrent `bt_peer_connection::write_extensions`).
        handshake.reqq = Some(MAX_ALLOWED_IN_REQUEST_QUEUE);
        // Every connection speaks it; we act on `connect` only over uTP
        // (libtorrent `bt_peer_connection::write_extensions`).
        handshake.m.ut_holepunch = Some(MY_EXTENDED_UT_HOLEPUNCH);
        // We only upload (libtorrent `bt_peer_connection::write_extensions`).
        handshake.upload_only = self.state.is_upload_only().then_some(1);
        // Seconds since we last saw a complete copy, -1 for never
        // (libtorrent `bt_peer_connection::write_extensions`).
        handshake.complete_ago = Some(
            self.state
                .complete_ago()
                .map_or(-1, |ago| i32::try_from(ago.as_secs()).unwrap_or(i32::MAX)),
        );
        // Where we listen, on a connection we made: a peer that reached us
        // knows it (libtorrent `bt_peer_connection::write_extensions`,
        // bt_peer_connection.cpp:2459-2463, the port
        // `session_impl::listen_port(ssl, local_addr)` gives: the one mapped
        // on the gateway over the connection's family, else the one we
        // listen on, whatever announce port is set).
        if !self.incoming {
            let shared = &self.state.shared;
            let ipv6 = self.addr.ip().is_some_and(|ip| ip.to_canonical().is_ipv6());
            handshake.p = shared
                .session
                .upgrade()
                .and_then(|session| session.external_port_of(shared.ssl.is_some(), ipv6))
                .map(u32::from);
        }

        Ok(())
    }

    fn client_name_and_version(&self) -> &str {
        &self.client_name_and_version
    }

    fn num_pieces(&self) -> Option<u32> {
        Some(self.state.lengths.total_pieces())
    }

    fn dht_port(&self) -> Option<u16> {
        self.told_dht_port()
    }

    fn answers(&self) -> Option<&AnswerBacklog> {
        Some(&self.answers)
    }
}
