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
    Handshake, Hashes, MAX_MSG_LEN, Message,
    extended::{
        ExtendedMessage, PeerExtendedMessageIds, handshake::ExtendedHandshake,
        ut_metadata::UtMetadata, ut_pex::UtPex,
    },
    serialize_piece_preamble,
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
    stream_connect::StreamConnector,
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

    /// The most chunk requests to keep outstanding per peer (default 128, and
    /// never more than the peer's reqq). A newly needed piece, such as the
    /// one at a stream's read position after a seek, is requested only after
    /// those, so a smaller window makes streams react faster to seeks.
    pub max_request_window: Option<usize>,
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
    read_buf: ReadBuf,
    write_buf: Box<[u8; MAX_MSG_LEN]>,
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
        }
    }

    // By the time this is called:
    // read_buf should start with valuable data. The handshake should be removed from it.
    pub async fn manage_peer_incoming(
        &self,
        outgoing_chan: tokio::sync::mpsc::UnboundedReceiver<WriterRequest>,
        mut incoming: CheckedIncomingConnection,
    ) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        let rwtimeout = self
            .options
            .read_write_timeout
            .unwrap_or_else(|| Duration::from_secs(10));

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

        let mut write_buf = Box::new([0u8; MAX_MSG_LEN]);
        let my_handshake = self.greeting(speaks_v2);
        let hlen = my_handshake.serialize_unchecked_len(&mut *write_buf);
        with_timeout(
            "writing handshake",
            rwtimeout,
            incoming
                .writer
                .write_all(&write_buf[..hlen])
                .map_err(Error::WriteHandshake),
        )
        .await?;

        // What the peer supports is in the peer's handshake, not in ours.
        let handshake_supports_extended = incoming.handshake.supports_extended();

        self.handler
            .on_handshake(incoming.handshake, incoming.kind, speaks_v2)
            .map_err(Error::Anyhow)?;

        self.manage_peer(ManagePeerArgs {
            handshake_supports_extended,
            speaks_v2,
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
        let rwtimeout = self
            .options
            .read_write_timeout
            .unwrap_or_else(|| Duration::from_secs(10));

        let connect_timeout = self
            .options
            .connect_timeout
            .unwrap_or_else(|| Duration::from_secs(10));

        let mut write_buf = Box::new([0u8; MAX_MSG_LEN]);
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
                &greeting,
                &write_buf[..hsz],
                connect_timeout,
                rwtimeout,
            )
            .await?;

        async move {
            self.handler.on_connected(now.elapsed());

            let mut read_buf = ReadBuf::new();
            let h = read_buf.read_handshake(&mut read, rwtimeout).await?;
            let handshake_supports_extended = h.supports_extended();
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

    /// Our handshake: the hash of the protocol the peer speaks, offering
    /// v2 to a peer that does not speak it when the torrent has v2
    /// (libtorrent `bt_peer_connection::write_handshake`).
    fn greeting(&self, speaks_v2: bool) -> Handshake {
        Handshake::new(self.info_hashes.for_peer(speaks_v2), self.peer_id)
            .offering_v2(!speaks_v2 && self.info_hashes.v2.is_some())
    }

    async fn manage_peer(&self, args: ManagePeerArgs) -> Result<()> {
        let ManagePeerArgs {
            handshake_supports_extended,
            speaks_v2,
            mut read_buf,
            mut write_buf,
            mut read,
            mut write,
            mut outgoing_chan,
        } = args;

        use tokio::io::AsyncWriteExt;

        let rwtimeout = self
            .options
            .read_write_timeout
            .unwrap_or_else(|| Duration::from_secs(10));

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
            let keep_alive_interval = self
                .options
                .keep_alive_interval
                .unwrap_or_else(|| Duration::from_secs(120));

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

            let len = Message::Unchoke.serialize(&mut *write_buf, &Default::default)?;
            with_timeout(
                "writing",
                rwtimeout,
                write.write_all(&write_buf[..len]).map_err(Error::Write),
            )
            .await?;
            trace!("sent unchoke");

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
            loop {
                let message = read_buf.read_message(&mut read, rwtimeout).await?;
                trace!("received: {:?}", &message);
                // libtorrent `bt_peer_connection::on_hash_request`, `on_hashes`
                // and `on_hash_reject` disconnect a peer that does not speak v2.
                if message.is_v2_only() && !speaks_v2 {
                    return Err(Error::HashMessageFromV1Peer);
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
    use std::time::Duration;

    use super::PeerConnectionOptions;

    #[test]
    fn timeouts_serialize_as_rounded_whole_seconds() {
        let options = PeerConnectionOptions {
            connect_timeout: Some(Duration::from_millis(1499)),
            read_write_timeout: Some(Duration::from_millis(1500)),
            keep_alive_interval: None,
            max_request_window: None,
        };
        assert_eq!(
            serde_json::to_value(options).unwrap(),
            serde_json::json!({
                "connect_timeout": 1,
                "read_write_timeout": 2,
                "keep_alive_interval": null,
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
