// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::buffers::{ByteBuf, ByteBufOwned};
use crate::core::{
    hash_id::{Id20, InfoHashes},
    lengths::ChunkInfo,
    peer_id::try_decode_peer_id,
};
use crate::peer_protocol::{
    Handshake, Hashes, Message,
    extended::{
        ExtendedMessage, PeerExtendedMessageIds, handshake::ExtendedHandshake,
        ut_metadata::UtMetadata, ut_pex::UtPex,
    },
    max_frame_len, serialize_piece_preamble,
};
use crate::{Error, Result, session::CheckedIncomingConnection, stream_connect::ConnectionKind};
use futures::TryFutureExt;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use tracing::{Instrument, trace, trace_span};

use crate::{
    read_buf::ReadBuf,
    spawn_utils::BlockingSpawner,
    stream_connect::{StreamConnector, Transports},
    type_aliases::{BoxAsyncReadVectored, BoxAsyncWrite},
};

pub trait PeerConnectionHandler {
    fn on_connected(&self, _connection_time: Duration) {}
    fn should_send_bitfield(&self) -> bool;
    fn serialize_bitfield_message_to_buf(&self, buf: &mut [u8]) -> anyhow::Result<usize>;
    /// `speaks_v2`: the peer speaks BEP 52, so it may exchange hashes.
    fn on_handshake(
        &self,
        handshake: Handshake,
        ckind: ConnectionKind,
        speaks_v2: bool,
    ) -> anyhow::Result<()>;
    fn on_extended_handshake(
        &self,
        extended_handshake: &ExtendedHandshake<ByteBuf>,
    ) -> anyhow::Result<()>;
    async fn on_received_message(&self, msg: Message<'_>) -> anyhow::Result<()>;
    fn on_uploaded_bytes(&self, bytes: u32);
    /// Takes `chunk` off the peer's upload queue to send it. False when a
    /// Cancel or a Choke took it off first, and the block is not sent. A
    /// handler that queues no uploads has none to take.
    fn claim_upload(&self, _chunk: &ChunkInfo) -> bool {
        false
    }
    fn read_chunk(&self, chunk: &ChunkInfo, buf: &mut [u8]) -> anyhow::Result<()>;
    fn update_my_extended_handshake(
        &self,
        _handshake: &mut ExtendedHandshake<ByteBuf>,
    ) -> anyhow::Result<()> {
        Ok(())
    }
    fn client_name_and_version(&self) -> &str {
        crate::client_name_and_version()
    }
    /// The torrent's piece count once its metadata is known: it sets the
    /// length of a bitfield, and so of the longest frame.
    fn num_pieces(&self) -> Option<u32> {
        None
    }
    /// The UDP port of the session's DHT node, when it runs one: the
    /// handshake then sets the DHT bit (BEP 5).
    fn dht_port(&self) -> Option<u16> {
        None
    }
}

#[derive(Debug)]
pub enum WriterRequest {
    Message(Message<'static>),
    UtMetadata(UtMetadata<ByteBufOwned>),
    UtPex(UtPex<ByteBufOwned>),
    Hashes(Hashes<ByteBufOwned>),
    ReadChunkRequest(ChunkInfo),
    Disconnect(anyhow::Result<()>),
}

#[derive(Default, Debug, Copy, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct PeerConnectionOptions {
    #[serde(default, with = "optional_seconds")]
    pub connect_timeout: Option<Duration>,

    #[serde(default, with = "optional_seconds")]
    pub read_write_timeout: Option<Duration>,

    #[serde(default, with = "optional_seconds")]
    pub keep_alive_interval: Option<Duration>,

    /// How long the encryption handshake, and then the BitTorrent
    /// handshake, may take on a connection either way; default
    /// [`DEFAULT_HANDSHAKE_TIMEOUT`].
    #[serde(default, with = "optional_seconds")]
    pub handshake_timeout: Option<Duration>,

    /// The most chunk requests to keep outstanding per peer (default 128, and
    /// never more than the peer's reqq). A newly needed piece, such as the
    /// one at a stream's read position after a seek, is requested only after
    /// those, so a smaller window makes streams react faster to seeks.
    pub max_request_window: Option<usize>,
}

/// libtorrent `settings_pack::handshake_timeout`: a peer that has not
/// finished its handshake this long after it last sent anything is
/// dropped (`peer_connection::second_tick`).
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// The read and write timeout when none is set.
const DEFAULT_READ_WRITE_TIMEOUT: Duration = Duration::from_secs(10);

impl PeerConnectionOptions {
    pub(crate) fn handshake_timeout(&self) -> Duration {
        self.handshake_timeout.unwrap_or(DEFAULT_HANDSHAKE_TIMEOUT)
    }

    fn read_write_timeout(&self) -> Duration {
        self.read_write_timeout
            .unwrap_or(DEFAULT_READ_WRITE_TIMEOUT)
    }

    /// A keepalive goes out once nothing has been sent for half the read
    /// timeout, so a peer that times out a silent connection as we do
    /// hears from us in time (libtorrent `peer_connection::keep_alive`:
    /// `timeout() / 2`, the `settings_pack::peer_timeout`).
    fn keep_alive_interval(&self) -> Duration {
        self.keep_alive_interval
            .unwrap_or_else(|| self.read_write_timeout() / 2)
    }
}

// A duration as whole seconds, rounded to the nearest one; `None` as none.
mod optional_seconds {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        value: &Option<Duration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(duration) => {
                let rounded = duration.as_secs() + u64::from(duration.subsec_millis() >= 500);
                serializer.serialize_some(&rounded)
            }
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Duration>, D::Error> {
        Ok(Option::<u64>::deserialize(deserializer)?.map(Duration::from_secs))
    }
}

pub(crate) struct PeerConnection<H> {
    handler: H,
    addr: SocketAddr,
    info_hashes: InfoHashes,
    peer_id: Id20,
    options: PeerConnectionOptions,
    spawner: BlockingSpawner,
    connector: Arc<StreamConnector>,
    // What an outgoing connection may go over.
    transports: Transports,
}

pub(crate) async fn with_timeout<T>(
    name: &'static str,
    timeout_value: Duration,
    fut: impl std::future::Future<Output = Result<T>>,
) -> crate::Result<T> {
    match timeout(timeout_value, fut).await {
        Ok(v) => v,
        Err(_) => Err(Error::Timeout(name)),
    }
}

struct ManagePeerArgs {
    handshake_supports_extended: bool,
    speaks_v2: bool,
    /// The peer set the fast bit in its handshake (libtorrent
    /// `bt_peer_connection::m_supports_fast`).
    supports_fast: bool,
    read_buf: ReadBuf,
    write_buf: Box<[u8]>,
    read: BoxAsyncReadVectored,
    write: BoxAsyncWrite,
    outgoing_chan: tokio::sync::mpsc::UnboundedReceiver<WriterRequest>,
}

impl<H: PeerConnectionHandler> PeerConnection<H> {
    pub fn new(
        addr: SocketAddr,
        info_hashes: InfoHashes,
        peer_id: Id20,
        handler: H,
        options: Option<PeerConnectionOptions>,
        spawner: BlockingSpawner,
        connector: Arc<StreamConnector>,
    ) -> Self {
        PeerConnection {
            handler,
            addr,
            info_hashes,
            peer_id,
            spawner,
            options: options.unwrap_or_default(),
            connector,
            transports: Transports::Any,
        }
    }

    /// The outgoing connection goes over `transports`.
    pub fn over(self, transports: Transports) -> Self {
        Self { transports, ..self }
    }

    // By the time this is called:
    // read_buf should start with valuable data. The handshake should be removed from it.
    pub async fn manage_peer_incoming(
        &self,
        outgoing_chan: tokio::sync::mpsc::UnboundedReceiver<WriterRequest>,
        mut incoming: CheckedIncomingConnection,
    ) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        // A peer speaks v2 when it greets with the truncated v2 hash or
        // offers v2, which a torrent without v2 refuses (libtorrent
        // `bt_peer_connection::on_receive_handshake`).
        let greeted = incoming.handshake.info_hash;
        let speaks_v2 = self.info_hashes.is_v2(greeted) || incoming.handshake.offers_v2();
        if !self.info_hashes.answers_to(greeted) || (speaks_v2 && self.info_hashes.v2.is_none()) {
            return Err(Error::WrongInfoHash);
        }

        if incoming.handshake.peer_id == self.peer_id {
            return Err(Error::ConnectingToOurselves);
        }

        trace!(
            "incoming connection: id={:?}",
            try_decode_peer_id(incoming.handshake.peer_id)
        );

        let mut write_buf = self.frame_buf();
        let my_handshake = self.greeting(speaks_v2);
        let hlen = my_handshake.serialize_unchecked_len(&mut *write_buf);
        with_timeout(
            "writing handshake",
            self.options.handshake_timeout(),
            incoming
                .writer
                .write_all(&write_buf[..hlen])
                .map_err(Error::WriteHandshake),
        )
        .await?;

        // What the peer supports is in the peer's handshake, not in ours.
        let handshake_supports_extended = incoming.handshake.supports_extended();
        let supports_fast = incoming.handshake.supports_fast();

        self.handler
            .on_handshake(incoming.handshake, incoming.kind, speaks_v2)
            .map_err(Error::Anyhow)?;

        self.manage_peer(ManagePeerArgs {
            handshake_supports_extended,
            speaks_v2,
            supports_fast,
            read_buf: incoming.read_buf,
            write_buf,
            read: incoming.reader,
            write: incoming.writer,
            outgoing_chan,
        })
        .await
    }

    pub async fn manage_peer_outgoing(
        &self,
        outgoing_chan: tokio::sync::mpsc::UnboundedReceiver<WriterRequest>,
    ) -> Result<()> {
        let handshake_timeout = self.options.handshake_timeout();
        let connect_timeout = self
            .options
            .connect_timeout
            .unwrap_or_else(|| Duration::from_secs(10));

        let mut write_buf = self.frame_buf();
        // Every peer is greeted as v1 first, which a hybrid's v2 peers
        // answer too (libtorrent `peer_connection::associated_info_hash`).
        let handshake = self.greeting(false);
        let greeting = handshake.info_hash;
        let hsz = handshake.serialize_unchecked_len(&mut *write_buf);

        // Connecting includes sending our handshake: an encrypted connection
        // carries it inside the encryption handshake.
        let now = Instant::now();
        let (ckind, mut read, write) = self
            .connector
            .connect_and_send_handshake(
                self.addr,
                self.transports,
                &greeting,
                &write_buf[..hsz],
                connect_timeout,
                handshake_timeout,
            )
            .await?;

        async move {
            self.handler.on_connected(now.elapsed());

            let mut read_buf = ReadBuf::new();
            let h = read_buf
                .read_handshake(&mut read, handshake_timeout)
                .await?;
            let handshake_supports_extended = h.supports_extended();
            let supports_fast = h.supports_fast();
            trace!(
                peer_id=?h.peer_id,
                decoded_id=?try_decode_peer_id(h.peer_id),
                "connected",
            );
            if !self.info_hashes.accepts_answer(greeting, h.info_hash) {
                return Err(Error::WrongInfoHash);
            }
            // Only the hash it answers with says the peer speaks v2
            // (libtorrent `bt_peer_connection::on_receive_handshake`).
            let speaks_v2 = self.info_hashes.is_v2(h.info_hash);

            if h.peer_id == self.peer_id {
                return Err(Error::ConnectingToOurselves);
            }

            self.handler
                .on_handshake(h, ckind, speaks_v2)
                .map_err(Error::Anyhow)?;

            self.manage_peer(ManagePeerArgs {
                handshake_supports_extended,
                speaks_v2,
                supports_fast,
                read_buf,
                write_buf,
                read,
                write,
                outgoing_chan,
            })
            .await
        }
        .instrument(trace_span!("", kind=%ckind))
        .await
    }

    /// A buffer for the longest frame of the torrent.
    fn frame_buf(&self) -> Box<[u8]> {
        vec![0u8; max_frame_len(self.handler.num_pieces())].into_boxed_slice()
    }

    /// Our handshake: the hash of the protocol the peer speaks, offering
    /// v2 to a peer that does not speak it when the torrent has v2
    /// (libtorrent `bt_peer_connection::write_handshake`).
    fn greeting(&self, speaks_v2: bool) -> Handshake {
        Handshake::new(self.info_hashes.for_peer(speaks_v2), self.peer_id)
            .offering_v2(!speaks_v2 && self.info_hashes.v2.is_some())
            .offering_dht(self.handler.dht_port().is_some())
    }

    async fn manage_peer(&self, args: ManagePeerArgs) -> Result<()> {
        let ManagePeerArgs {
            handshake_supports_extended,
            speaks_v2,
            supports_fast,
            mut read_buf,
            mut write_buf,
            mut read,
            mut write,
            mut outgoing_chan,
        } = args;

        use tokio::io::AsyncWriteExt;

        let rwtimeout = self.options.read_write_timeout();

        let extended_handshake: RwLock<Option<PeerExtendedMessageIds>> = RwLock::new(None);
        let extended_handshake_ref = &extended_handshake;
        let supports_extended = handshake_supports_extended;

        if supports_extended {
            let mut my_extended = ExtendedHandshake::new();
            my_extended.v = Some(ByteBuf(self.handler.client_name_and_version().as_bytes()));
            my_extended.yourip = Some(self.addr.ip().into());
            self.handler
                .update_my_extended_handshake(&mut my_extended)
                .map_err(Error::Anyhow)?;
            let my_extended = Message::Extended(ExtendedMessage::Handshake(my_extended));
            trace!("sending extended handshake: {:?}", &my_extended);
            let esz = my_extended.serialize(&mut *write_buf, &Default::default)?;
            with_timeout(
                "writing extended handshake",
                rwtimeout,
                write.write_all(&write_buf[..esz]).map_err(Error::Write),
            )
            .await?;
        }

        let writer = async move {
            let keep_alive_interval = self.options.keep_alive_interval();

            if self.handler.should_send_bitfield() {
                let len = self
                    .handler
                    .serialize_bitfield_message_to_buf(&mut *write_buf)
                    .map_err(Error::Anyhow)?;
                with_timeout(
                    "writing bitfield",
                    rwtimeout,
                    write.write_all(&write_buf[..len]).map_err(Error::Write),
                )
                .await?;
                trace!("sent bitfield");
            }

            loop {
                let req = match timeout(keep_alive_interval, outgoing_chan.recv()).await {
                    Ok(Some(msg)) => msg,
                    Ok(None) => {
                        return Err(Error::TorrentIsNotLive);
                    }
                    Err(_) => WriterRequest::Message(Message::KeepAlive),
                };

                tokio::task::yield_now().await;

                let mut uploaded_add = None;

                trace!("about to send: {:?}", &req);
                let ext_msg_ids = &|| {
                    extended_handshake_ref
                        .read()
                        .as_ref()
                        .map(|e| *e)
                        .unwrap_or_default()
                };

                let len = match req {
                    WriterRequest::Message(msg) => msg.serialize(&mut *write_buf, ext_msg_ids)?,
                    WriterRequest::UtMetadata(utm) => {
                        Message::Extended(ExtendedMessage::UtMetadata(utm.as_borrowed()))
                            .serialize(&mut *write_buf, ext_msg_ids)?
                    }
                    WriterRequest::UtPex(ut_pex) => {
                        Message::Extended(ExtendedMessage::UtPex(ut_pex.as_borrowed()))
                            .serialize(&mut *write_buf, ext_msg_ids)?
                    }
                    WriterRequest::Hashes(hashes) => Message::Hashes(hashes.as_borrowed())
                        .serialize(&mut *write_buf, ext_msg_ids)?,
                    WriterRequest::ReadChunkRequest(chunk) => {
                        if !self.handler.claim_upload(&chunk) {
                            trace!(?chunk, "upload taken off the queue before it was sent");
                            continue;
                        }

                        #[allow(unused_mut)]
                        let mut skip_reading_for_e2e_tests = false;

                        #[cfg(test)]
                        {
                            use tracing::warn;
                            // This is poor-mans fault injection for running e2e tests.
                            use crate::tests::test_util::TestPeerMetadata;
                            let tpm = TestPeerMetadata::from_peer_id(self.peer_id);
                            use rand::RngExt;
                            if rand::rng().random_bool(tpm.disconnect_probability()) {
                                return Err(Error::TestDisconnect);
                            }

                            #[allow(clippy::cast_possible_truncation)]
                            let sleep_ms = (rand::rng().random::<f64>()
                                * (tpm.max_random_sleep_ms as f64))
                                as u64;
                            if sleep_ms > 0 {
                                tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                            }

                            if rand::rng().random_bool(tpm.bad_data_probability()) {
                                warn!(
                                    "will NOT actually read the data to simulate a malicious peer that sends garbage"
                                );
                                write_buf.fill(0);
                                skip_reading_for_e2e_tests = true;
                            }
                        }

                        // this whole section is an optimization
                        let preamble_len = serialize_piece_preamble(&chunk, &mut *write_buf);
                        let full_len = preamble_len + chunk.size as usize;
                        if !skip_reading_for_e2e_tests {
                            self.spawner
                                .block_in_place_with_semaphore(|| {
                                    self.handler
                                        .read_chunk(&chunk, &mut write_buf[preamble_len..full_len])
                                })
                                .await
                                .map_err(Error::ReadChunk)?;
                        }

                        uploaded_add = Some(chunk.size);
                        full_len
                    }
                    WriterRequest::Disconnect(res) => {
                        trace!("disconnect requested, closing writer");
                        match res {
                            Ok(()) => return Err(Error::Disconnect),
                            Err(e) => return Err(Error::DisconnectWithSource(e)),
                        }
                    }
                };

                with_timeout(
                    "writing",
                    rwtimeout,
                    write.write_all(&write_buf[..len]).map_err(Error::Write),
                )
                .await?;

                if let Some(uploaded_add) = uploaded_add {
                    self.handler.on_uploaded_bytes(uploaded_add)
                }
            }

            // For type inference.
            #[allow(unreachable_code)]
            Ok::<_, Error>(())
        };

        let reader = async move {
            let num_pieces = self.handler.num_pieces();
            loop {
                let message = read_buf
                    .read_message(&mut read, rwtimeout, num_pieces)
                    .await?;
                trace!("received: {:?}", &message);
                // libtorrent `bt_peer_connection::on_hash_request`, `on_hashes`
                // and `on_hash_reject` disconnect a peer that does not speak v2.
                if message.is_v2_only() && !speaks_v2 {
                    return Err(Error::HashMessageFromV1Peer);
                }
                // libtorrent `bt_peer_connection::on_suggest_piece`,
                // `on_have_all`, `on_have_none`, `on_reject_request` and
                // `on_allowed_fast` disconnect a peer that did not set the
                // fast bit.
                if message.is_fast_only() && !supports_fast {
                    return Err(Error::Anyhow(anyhow::anyhow!(
                        "fast extension message from a peer without it: {message:?}"
                    )));
                }

                tokio::task::yield_now().await;

                if let Message::Extended(ExtendedMessage::Handshake(h)) = &message {
                    *extended_handshake_ref.write() = Some(h.peer_extended_messages());
                    self.handler
                        .on_extended_handshake(h)
                        .map_err(Error::Anyhow)?;
                } else {
                    self.handler
                        .on_received_message(message)
                        .await
                        .map_err(Error::Anyhow)?;
                }
            }

            // For type inference.
            #[allow(unreachable_code)]
            Ok::<_, Error>(())
        };

        tokio::select! {
            r = reader => {
                if let Err(e) = r.as_ref() {
                    trace!("reader finished with error: {e:#}");
                } else {
                    trace!("reader finished without error");
                }
                r
            }
            r = writer => {
                if let Err(e) = r.as_ref() {
                    trace!("writer finished with error: {e:#}");
                } else {
                    trace!("writer finished without error");
                }
                r
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc, time::Duration};

    use tokio::{sync::mpsc, time::Instant};

    use super::{
        DEFAULT_HANDSHAKE_TIMEOUT, PeerConnection, PeerConnectionHandler, PeerConnectionOptions,
    };
    use crate::{
        Error,
        buffers::ByteBuf,
        core::{
            hash_id::{Id20, InfoHashes},
            lengths::ChunkInfo,
        },
        mse::EncryptionPolicy,
        peer_info_reader::tests::{Dialed, halves},
        peer_protocol::{Handshake, Message, extended::handshake::ExtendedHandshake},
        read_buf::ReadBuf,
        session::CheckedIncomingConnection,
        spawn_utils::BlockingSpawner,
        stream_connect::{ConnectionKind, StreamConnector, StreamConnectorArgs},
    };

    struct Quiet;

    impl PeerConnectionHandler for Quiet {
        fn should_send_bitfield(&self) -> bool {
            false
        }
        fn serialize_bitfield_message_to_buf(&self, _: &mut [u8]) -> anyhow::Result<usize> {
            Ok(0)
        }
        fn on_handshake(&self, _: Handshake, _: ConnectionKind, _: bool) -> anyhow::Result<()> {
            Ok(())
        }
        fn on_extended_handshake(&self, _: &ExtendedHandshake<ByteBuf>) -> anyhow::Result<()> {
            Ok(())
        }
        async fn on_received_message(&self, _: Message<'_>) -> anyhow::Result<()> {
            Ok(())
        }
        fn on_uploaded_bytes(&self, _: u32) {}
        fn read_chunk(&self, _: &ChunkInfo, _: &mut [u8]) -> anyhow::Result<()> {
            Ok(())
        }
    }

    const HASH: [u8; 20] = [7; 20];

    /// A connection whose reads and writes may wait two minutes, so only
    /// the handshake timeout can end a handshake sooner.
    async fn connection(
        encryption: EncryptionPolicy,
    ) -> (
        PeerConnection<Quiet>,
        mpsc::UnboundedReceiver<(SocketAddr, tokio::io::DuplexStream)>,
    ) {
        let (tx, dialed) = mpsc::unbounded_channel();
        let connector = StreamConnector::new(StreamConnectorArgs {
            enable_tcp: true,
            socket_factory: Some(Arc::new(Dialed(tx))),
            encryption,
            ..Default::default()
        })
        .await
        .unwrap();
        let options = PeerConnectionOptions {
            read_write_timeout: Some(Duration::from_secs(120)),
            ..Default::default()
        };
        let connection = PeerConnection::new(
            "127.0.0.1:6881".parse().unwrap(),
            InfoHashes {
                v1: Some(Id20::new(HASH)),
                v2: None,
            },
            Id20::new([1; 20]),
            Quiet,
            Some(options),
            BlockingSpawner::new(1),
            Arc::new(connector),
        );
        (connection, dialed)
    }

    // libtorrent `peer_connection::second_tick` drops a peer still in its
    // handshake `handshake_timeout` after it last heard from it, whatever
    // the peer timeout; the encryption handshake is part of it.
    #[tokio::test(start_paused = true)]
    async fn an_outgoing_handshake_unanswered_ends_at_the_handshake_timeout() {
        for (encryption, step) in [
            (EncryptionPolicy::Disabled, "reading"),
            (EncryptionPolicy::Require, "encryption handshake"),
        ] {
            let (connection, _dialed) = connection(encryption).await;
            let (_tx, rx) = mpsc::unbounded_channel();
            let start = Instant::now();
            let result = connection.manage_peer_outgoing(rx).await;
            assert!(
                matches!(result, Err(Error::Timeout(s)) if s == step),
                "{encryption:?}: {result:?}"
            );
            assert_eq!(start.elapsed(), DEFAULT_HANDSHAKE_TIMEOUT, "{encryption:?}");
        }
    }

    // libtorrent `peer_connection::keep_alive` sends a keepalive once
    // nothing has gone out for half the peer timeout.
    #[tokio::test(start_paused = true)]
    async fn a_keepalive_goes_out_at_half_the_read_timeout_unless_set() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for (read_write_timeout, keep_alive_interval, expected) in [
            (Some(120), None, 60),
            (None, None, 5),
            (Some(120), Some(30), 30),
        ] {
            let (mut connection, mut dialed) = connection(EncryptionPolicy::Disabled).await;
            connection.options = PeerConnectionOptions {
                read_write_timeout: read_write_timeout.map(Duration::from_secs),
                keep_alive_interval: keep_alive_interval.map(Duration::from_secs),
                ..Default::default()
            };
            let (_tx, rx) = mpsc::unbounded_channel();
            let task = tokio::spawn(async move { connection.manage_peer_outgoing(rx).await });
            let (_, mut peer) = dialed.recv().await.unwrap();
            let mut theirs = [0; 68];
            peer.read_exact(&mut theirs).await.unwrap();
            let mut ours = [0; 68];
            let len = Handshake::new(Id20::new(HASH), Id20::new([2; 20]))
                .serialize_unchecked_len(&mut ours);
            peer.write_all(&ours[..len]).await.unwrap();
            // Whatever goes out first, the keepalive is the first empty frame.
            let start = Instant::now();
            loop {
                let frame = peer.read_u32().await.unwrap();
                if frame == 0 {
                    break;
                }
                let mut body = vec![0; frame as usize];
                peer.read_exact(&mut body).await.unwrap();
            }
            assert_eq!(
                start.elapsed(),
                Duration::from_secs(expected),
                "{read_write_timeout:?} {keep_alive_interval:?}"
            );
            task.abort();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_incoming_peer_that_takes_no_handshake_ends_at_the_handshake_timeout() {
        let (connection, _dialed) = connection(EncryptionPolicy::Disabled).await;
        // The peer reads nothing, so our handshake does not fit.
        let (ours, _theirs) = tokio::io::duplex(16);
        let (reader, writer) = halves(ours);
        let incoming = CheckedIncomingConnection {
            kind: ConnectionKind::Tcp,
            addr: "127.0.0.1:6881".parse().unwrap(),
            reader,
            writer,
            read_buf: ReadBuf::new(),
            handshake: Handshake::new(Id20::new(HASH), Id20::new([2; 20])),
        };
        let (_tx, rx) = mpsc::unbounded_channel();
        let start = Instant::now();
        let result = connection.manage_peer_incoming(rx, incoming).await;
        assert!(
            matches!(result, Err(Error::Timeout("writing handshake"))),
            "{result:?}"
        );
        assert_eq!(start.elapsed(), DEFAULT_HANDSHAKE_TIMEOUT);
    }

    #[test]
    fn timeouts_serialize_as_rounded_whole_seconds() {
        let options = PeerConnectionOptions {
            connect_timeout: Some(Duration::from_millis(1499)),
            read_write_timeout: Some(Duration::from_millis(1500)),
            keep_alive_interval: None,
            handshake_timeout: Some(Duration::from_millis(2500)),
            max_request_window: None,
        };
        assert_eq!(
            serde_json::to_value(options).unwrap(),
            serde_json::json!({
                "connect_timeout": 1,
                "read_write_timeout": 2,
                "keep_alive_interval": null,
                "handshake_timeout": 3,
                "max_request_window": null,
            })
        );
        let parsed: PeerConnectionOptions =
            serde_json::from_str(r#"{"connect_timeout": 3, "max_request_window": null}"#).unwrap();
        assert_eq!(
            parsed,
            PeerConnectionOptions {
                connect_timeout: Some(Duration::from_secs(3)),
                ..Default::default()
            }
        );
    }
}
