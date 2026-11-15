// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! What a session's peer connections did, counted where it happens.
//!
//! Every count is one relaxed atomic add at the event: no lock, no clock
//! and no lookup. A count that has kinds is an array indexed by a small
//! enum, so the kind costs nothing to find. The counts only grow; a rate is
//! for whoever reads them to take. Reading them takes no lock either and
//! does the same work however many peers and torrents the session has.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

use crate::{
    peer_connection::WriterRequest,
    peer_protocol::{Message, extended::ExtendedMessage},
    type_aliases::PeerSourceFlag,
};

// An enum whose variants index an array of counts, each with the name it
// is read under.
macro_rules! counted {
    ($(#[$meta:meta])* $name:ident { $($(#[$vmeta:meta])* $variant:ident => $label:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum $name {
            $($(#[$vmeta])* $variant),+
        }

        impl $name {
            /// Every one, in the order they index their counts.
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];
            pub const COUNT: usize = Self::ALL.len();

            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $label),+
                }
            }
        }
    };
}
pub(super) use counted;

counted! {
    /// Which way something crossed a connection.
    Direction {
        /// From the peer to us.
        In => "in",
        /// From us to the peer.
        Out => "out",
    }
}

counted! {
    /// A peer wire message, by what it is.
    MessageKind {
        KeepAlive => "keep_alive",
        Choke => "choke",
        Unchoke => "unchoke",
        Interested => "interested",
        NotInterested => "not_interested",
        Have => "have",
        Bitfield => "bitfield",
        Request => "request",
        /// A block.
        Piece => "piece",
        Cancel => "cancel",
        /// BEP 5: the sender's DHT port.
        Port => "port",
        Suggest => "suggest",
        HaveAll => "have_all",
        HaveNone => "have_none",
        Reject => "reject",
        AllowedFast => "allowed_fast",
        HashRequest => "hash_request",
        Hashes => "hashes",
        HashReject => "hash_reject",
        ExtendedHandshake => "extended_handshake",
        Pex => "pex",
        /// BEP 9: a request for, a piece of, or a refusal of the metadata.
        Metadata => "metadata",
        Holepunch => "holepunch",
        UploadOnly => "upload_only",
        DontHave => "dont_have",
        /// An extended message that says nothing: one of the wrong size, or
        /// under an extension the connection did not advertise.
        OtherExtended => "other_extended",
    }
}

impl MessageKind {
    pub fn of(message: &Message<'_>) -> Self {
        match message {
            Message::KeepAlive => Self::KeepAlive,
            Message::Choke => Self::Choke,
            Message::Unchoke => Self::Unchoke,
            Message::Interested => Self::Interested,
            Message::NotInterested => Self::NotInterested,
            Message::Have(_) => Self::Have,
            Message::Bitfield(_) => Self::Bitfield,
            Message::Request(_) => Self::Request,
            Message::Piece(_) => Self::Piece,
            Message::Cancel(_) => Self::Cancel,
            Message::Port(_) => Self::Port,
            Message::Suggest(_) => Self::Suggest,
            Message::HaveAll => Self::HaveAll,
            Message::HaveNone => Self::HaveNone,
            Message::Reject(_) => Self::Reject,
            Message::AllowedFast(_) => Self::AllowedFast,
            Message::HashRequest(_) => Self::HashRequest,
            Message::Hashes(_) => Self::Hashes,
            Message::HashReject(_) => Self::HashReject,
            Message::Extended(extended) => match extended {
                ExtendedMessage::Handshake(_) => Self::ExtendedHandshake,
                ExtendedMessage::UtPex(_) => Self::Pex,
                ExtendedMessage::UtMetadata(_) => Self::Metadata,
                ExtendedMessage::UtHolepunch(_) => Self::Holepunch,
                ExtendedMessage::UploadOnly(_) => Self::UploadOnly,
                ExtendedMessage::DontHave(_) => Self::DontHave,
                ExtendedMessage::WrongSize(_) | ExtendedMessage::NotAdvertised(_) => {
                    Self::OtherExtended
                }
            },
        }
    }
}

impl WriterRequest {
    /// The message the writer frames for the request when the frame is not
    /// a block: a block request it does not serve is answered with a
    /// Reject. None for a request to close, which frames nothing.
    pub(crate) fn frame_kind(&self) -> Option<MessageKind> {
        use crate::peer_connection::Answer;
        match self {
            Self::Message(message) => Some(MessageKind::of(message)),
            Self::UtPex(_) => Some(MessageKind::Pex),
            Self::Answer(Answer::Reject(_)) | Self::ReadChunkRequest(_) => {
                Some(MessageKind::Reject)
            }
            Self::Answer(Answer::HashReject(_)) => Some(MessageKind::HashReject),
            Self::Answer(Answer::Hashes(_)) => Some(MessageKind::Hashes),
            Self::Answer(Answer::Metadata(_)) => Some(MessageKind::Metadata),
            Self::Disconnect(_) => None,
        }
    }
}

/// What carries a connection, as the counts index it: its kind, read
/// under the kind's metrics name.
pub use crate::stream_connect::ConnectionKind as Transport;

counted! {
    /// How a handshake that completed left its connection.
    Encryption {
        /// A plain BitTorrent handshake.
        Plaintext => "plaintext",
        /// An encrypted handshake, then a plain stream.
        HeaderOnly => "header_only",
        /// An encrypted handshake and stream.
        Rc4 => "rc4",
    }
}

impl Encryption {
    pub fn of(encryption: crate::mse::Encryption) -> Self {
        match encryption {
            crate::mse::Encryption::None => Self::Plaintext,
            crate::mse::Encryption::HeaderOnly => Self::HeaderOnly,
            crate::mse::Encryption::Rc4 => Self::Rc4,
        }
    }
}

counted! {
    /// How far a connection had come when it ended.
    ClosePhase {
        /// Before its handshake completed: dialling, or shaking hands.
        Connecting => "connecting",
        Live => "live",
    }
}

counted! {
    /// Why a connection ended.
    CloseReason {
        /// We closed it, or its torrent or session stopped.
        Closed => "closed",
        /// The peer closed it.
        PeerClosed => "peer_closed",
        /// The peer went silent: a connect, handshake, read or write ran
        /// out of time.
        Timeout => "timeout",
        /// The connect did not complete.
        ConnectFailed => "connect_failed",
        /// Our own settings turned the connect away.
        NotAllowed => "not_allowed",
        /// A read or write of the open transport failed.
        Io => "io",
        /// The encryption handshake failed.
        Encryption => "encryption",
        WrongInfoHash => "wrong_info_hash",
        /// The peer was us.
        Ourselves => "ourselves",
        /// The peer sent what the protocol does not allow.
        Protocol => "protocol",
        /// A block the peer asked for could not be read.
        Disk => "disk",
        Other => "other",
    }
}

impl CloseReason {
    /// Why a connection that ended with `error` did; none is one that ended
    /// with nothing wrong.
    pub fn of(error: Option<&crate::Error>) -> Self {
        use crate::Error;
        let Some(error) = error else {
            return Self::Closed;
        };
        if error.is_local_refusal() {
            return Self::NotAllowed;
        }
        match error {
            Error::Disconnect
            | Error::DisconnectWithSource(_)
            | Error::TorrentIsNotLive
            | Error::SessionDestroyed
            | Error::PeerTaskDead => Self::Closed,
            Error::PeerDisconnected | Error::PeerDisconnectedReadingHandshake => Self::PeerClosed,
            Error::Timeout(_) | Error::ConnectTimeout(_) => Self::Timeout,
            Error::TcpConnect(_)
            | Error::UtpConnect(_)
            | Error::Connect { .. }
            | Error::TlsConnect(_) => Self::ConnectFailed,
            Error::Read(_)
            | Error::Write(_)
            | Error::ReadHandshake(_)
            | Error::WriteHandshake(_) => Self::Io,
            Error::Mse(_) => Self::Encryption,
            Error::WrongInfoHash => Self::WrongInfoHash,
            Error::ConnectingToOurselves => Self::Ourselves,
            Error::Deserialize(_)
            | Error::DeserializeHandshake(_)
            | Error::HashMessageFromV1Peer
            | Error::ProtoSerialize(_) => Self::Protocol,
            Error::ReadChunk(_) => Self::Disk,
            _ => Self::Other,
        }
    }
}

counted! {
    /// What bytes on a connection carry.
    ByteKind {
        /// The data of blocks.
        Payload => "payload",
        /// Everything else: message framing and every other message.
        Protocol => "protocol",
    }
}

counted! {
    /// What became of a block request other than its being served.
    RequestEvent {
        /// Ours: the peer did not answer in time, and is snubbed.
        TimedOut => "timed_out",
        /// The peer's: for what we cannot serve.
        Invalid => "invalid",
        /// The peer's: sent while we choke it, and dropped.
        WhileChoked => "while_choked",
        /// The peer's: past its request queue, and dropped.
        QueueFull => "queue_full",
        /// Ours: not sent, the peer was asked for the block already.
        AskedAlready => "asked_already",
    }
}

counted! {
    /// A block that arrived, by whether we were waiting for it.
    BlockArrival {
        Requested => "requested",
        /// After we cancelled the request.
        Cancelled => "cancelled",
        /// Never asked for.
        Unrequested => "unrequested",
    }
}

counted! {
    /// How the check of a piece whose last block landed came out.
    PieceCheck {
        Passed => "passed",
        Failed => "failed",
        /// The torrent's own hashes disagree; the torrent stops.
        Inconsistent => "inconsistent",
        /// No hash to check it against yet; it is downloaded again.
        Unknown => "unknown",
        /// It passed, and the storage would not take it.
        StorageRefused => "storage_refused",
    }
}

counted! {
    /// What a connection got when it asked for its next piece.
    PiecePick {
        Reserved => "reserved",
        /// A piece taken from a slower peer.
        Stolen => "stolen",
        /// A block asked of another peer too, in the end game.
        EndGame => "end_game",
        Nothing => "nothing",
        /// The storage has no room for another piece.
        NoRoom => "no_room",
    }
}

counted! {
    /// Why bytes that arrived were not used.
    Waste {
        /// Their piece failed its hash check.
        HashFailed => "hash_failed",
        /// The block was had, or another copy came first.
        Redundant => "redundant",
        /// Their connection closed before the rest of the block arrived.
        CutShort => "cut_short",
    }
}

counted! {
    /// Who named a peer to a torrent.
    PeerSource {
        Tracker => "tracker",
        Dht => "dht",
        Pex => "pex",
        Lsd => "lsd",
        /// The torrent's own list: added with it, or saved by a restart.
        ResumeData => "resume_data",
        Incoming => "incoming",
    }
}

impl PeerSource {
    pub fn of(source: PeerSourceFlag) -> Self {
        match source {
            PeerSourceFlag::Tracker => Self::Tracker,
            PeerSourceFlag::Dht => Self::Dht,
            PeerSourceFlag::Pex => Self::Pex,
            PeerSourceFlag::Lsd => Self::Lsd,
            PeerSourceFlag::ResumeData => Self::ResumeData,
            PeerSourceFlag::Incoming => Self::Incoming,
        }
    }
}

pub(super) type Tally<const N: usize> = [AtomicU64; N];

#[inline]
pub(super) fn add<const N: usize>(tally: &Tally<N>, kind: usize, count: u64) {
    tally[kind].fetch_add(count, Ordering::Relaxed);
}

pub(super) fn read<const N: usize>(tally: &Tally<N>) -> [u64; N] {
    std::array::from_fn(|kind| tally[kind].load(Ordering::Relaxed))
}

/// The counts of one session, over every connection of every torrent on it.
#[derive(Debug, Default)]
pub struct SwarmStats {
    // By direction, then kind.
    messages: [Tally<{ MessageKind::COUNT }>; Direction::COUNT],
    // By transport, direction, then encryption.
    handshakes: [[Tally<{ Encryption::COUNT }>; Direction::COUNT]; Transport::COUNT],
    // By phase, then reason.
    closes: [Tally<{ CloseReason::COUNT }>; ClosePhase::COUNT],
    // By transport, direction, then kind.
    bytes: [[Tally<{ ByteKind::COUNT }>; Direction::COUNT]; Transport::COUNT],
    request_events: Tally<{ RequestEvent::COUNT }>,
    blocks: Tally<{ BlockArrival::COUNT }>,
    piece_checks: Tally<{ PieceCheck::COUNT }>,
    piece_picks: Tally<{ PiecePick::COUNT }>,
    wasted_bytes: Tally<{ Waste::COUNT }>,
    peers_named: Tally<{ PeerSource::COUNT }>,
}

impl SwarmStats {
    /// `count` messages of `kind` crossed a connection.
    #[inline]
    pub fn messages(&self, direction: Direction, kind: MessageKind, count: u64) {
        add(&self.messages[direction as usize], kind as usize, count);
    }

    /// A connection completed its handshake.
    pub fn handshake(&self, transport: Transport, direction: Direction, encryption: Encryption) {
        add(
            &self.handshakes[transport as usize][direction as usize],
            encryption as usize,
            1,
        );
    }

    /// A connection ended.
    pub fn closed(&self, phase: ClosePhase, reason: CloseReason) {
        add(&self.closes[phase as usize], reason as usize, 1);
    }

    /// `bytes` crossed a connection whose handshake is done.
    #[inline]
    pub fn bytes(&self, transport: Transport, direction: Direction, kind: ByteKind, bytes: u64) {
        add(
            &self.bytes[transport as usize][direction as usize],
            kind as usize,
            bytes,
        );
    }

    #[inline]
    pub fn request_event(&self, event: RequestEvent) {
        add(&self.request_events, event as usize, 1);
    }

    #[inline]
    pub fn block(&self, arrival: BlockArrival) {
        add(&self.blocks, arrival as usize, 1);
    }

    pub fn piece_check(&self, result: PieceCheck) {
        add(&self.piece_checks, result as usize, 1);
    }

    #[inline]
    pub fn piece_pick(&self, result: PiecePick) {
        add(&self.piece_picks, result as usize, 1);
    }

    #[inline]
    pub fn wasted(&self, why: Waste, bytes: u64) {
        add(&self.wasted_bytes, why as usize, bytes);
    }

    /// A source named a peer to a live torrent, known to it already or not.
    #[inline]
    pub fn peer_named(&self, source: PeerSource) {
        add(&self.peers_named, source as usize, 1);
    }

    pub fn snapshot(&self) -> SwarmSnapshot {
        SwarmSnapshot {
            messages: self.messages.each_ref().map(read),
            handshakes: self.handshakes.each_ref().map(|by| by.each_ref().map(read)),
            closes: self.closes.each_ref().map(read),
            bytes: self.bytes.each_ref().map(|by| by.each_ref().map(read)),
            request_events: read(&self.request_events),
            blocks: read(&self.blocks),
            piece_checks: read(&self.piece_checks),
            piece_picks: read(&self.piece_picks),
            wasted_bytes: read(&self.wasted_bytes),
            peers_named: read(&self.peers_named),
        }
    }
}

/// [`SwarmStats`] as read at one moment. Each array is indexed by the enums
/// named, in the order given.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SwarmSnapshot {
    /// By [`Direction`], then [`MessageKind`].
    pub messages: [[u64; MessageKind::COUNT]; Direction::COUNT],
    /// By [`Transport`], [`Direction`], then [`Encryption`].
    pub handshakes: [[[u64; Encryption::COUNT]; Direction::COUNT]; Transport::COUNT],
    /// By [`ClosePhase`], then [`CloseReason`].
    pub closes: [[u64; CloseReason::COUNT]; ClosePhase::COUNT],
    /// By [`Transport`], [`Direction`], then [`ByteKind`].
    pub bytes: [[[u64; ByteKind::COUNT]; Direction::COUNT]; Transport::COUNT],
    pub request_events: [u64; RequestEvent::COUNT],
    pub blocks: [u64; BlockArrival::COUNT],
    pub piece_checks: [u64; PieceCheck::COUNT],
    pub piece_picks: [u64; PiecePick::COUNT],
    pub wasted_bytes: [u64; Waste::COUNT],
    pub peers_named: [u64; PeerSource::COUNT],
}

impl SwarmSnapshot {
    /// The messages that crossed, of every kind each way.
    pub fn messages(&self) -> impl Iterator<Item = (Direction, MessageKind, u64)> + '_ {
        Direction::ALL.iter().flat_map(move |direction| {
            MessageKind::ALL.iter().map(move |kind| {
                (
                    *direction,
                    *kind,
                    self.messages[*direction as usize][*kind as usize],
                )
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        sync::{mpsc, watch},
    };

    use super::{Direction, MessageKind, SwarmStats};
    use crate::{
        Error,
        buffers::ByteBuf,
        core::{
            hash_id::{Id20, InfoHashes},
            lengths::{ChunkInfo, Lengths},
        },
        meter::Meter,
        peer_connection::{
            Answer, PeerConnection, PeerConnectionHandler, PeerConnectionOptions, WriterRequest,
        },
        peer_info_reader::tests::{Dialed, halves},
        peer_protocol::{
            Handshake, Message, Piece, Request,
            extended::{
                ExtendedMessage, PeerExtendedMessageIds, handshake::ExtendedHandshake,
                ut_pex::UtPex,
            },
        },
        read_buf::ReadBuf,
        session::CheckedIncomingConnection,
        spawn_utils::BlockingSpawner,
        stream_connect::{ConnectionKind, StreamConnector, StreamConnectorArgs},
    };

    const HASH: [u8; 20] = [7; 20];
    const BLOCK: u32 = 16384;

    /// Counts what its connection tells it crossed, serves every block, and
    /// says when the peer's extended handshake and each Have arrive.
    #[derive(Default)]
    struct Counting {
        stats: SwarmStats,
        shook: watch::Sender<bool>,
        had: watch::Sender<u32>,
    }

    impl PeerConnectionHandler for &Counting {
        fn should_send_bitfield(&self) -> bool {
            true
        }
        fn serialize_bitfield_message_to_buf(&self, buf: &mut [u8]) -> anyhow::Result<usize> {
            // A have-all, in place of the bitfield.
            buf[..5].copy_from_slice(&[0, 0, 0, 1, 14]);
            Ok(5)
        }
        fn on_handshake(&self, _: Handshake, _: ConnectionKind, _: bool) -> anyhow::Result<()> {
            Ok(())
        }
        fn on_extended_handshake(&self, _: &ExtendedHandshake<ByteBuf>) -> anyhow::Result<()> {
            self.shook.send_replace(true);
            Ok(())
        }
        async fn on_received_message(&self, message: Message<'_>) -> anyhow::Result<()> {
            if let Message::Have(piece) = message {
                self.had.send_replace(piece);
            }
            Ok(())
        }
        fn on_uploaded_bytes(&self, _: u32) {}
        fn claim_upload(&self, _: &ChunkInfo) -> bool {
            true
        }
        fn read_chunk(&self, _: &ChunkInfo, buf: &mut [u8]) -> anyhow::Result<()> {
            buf.fill(3);
            Ok(())
        }
        fn on_wire_messages(&self, direction: Direction, kind: MessageKind, count: u64) {
            self.stats.messages(direction, kind, count);
        }
    }

    /// What a frame of ours is, told from its bytes alone.
    fn kind_on_the_wire(body: &[u8]) -> MessageKind {
        match body {
            [] => MessageKind::KeepAlive,
            [0, ..] => MessageKind::Choke,
            [1, ..] => MessageKind::Unchoke,
            [4, ..] => MessageKind::Have,
            [7, ..] => MessageKind::Piece,
            [14, ..] => MessageKind::HaveAll,
            [16, ..] => MessageKind::Reject,
            [20, 0, ..] => MessageKind::ExtendedHandshake,
            [20, id, ..] if Some(*id) == PeerExtendedMessageIds::my().ut_pex => MessageKind::Pex,
            other => panic!(
                "a frame the test did not queue: {:?}",
                &other[..other.len().min(8)]
            ),
        }
    }

    // Each message that crosses a connection is counted once under what it
    // is and the way it went: the ones we frame against what the peer read
    // off the wire, the ones the peer sent against what it wrote. Blocks one
    // write carries are counted together.
    #[tokio::test(start_paused = true)]
    async fn each_message_that_crosses_is_counted_once_by_kind_and_direction() {
        let counting = Counting::default();
        let (dialed, _dials) = mpsc::unbounded_channel();
        let connector = StreamConnector::new(StreamConnectorArgs {
            enable_tcp: true,
            ..StreamConnectorArgs::over(Arc::new(Dialed(dialed)))
        })
        .await
        .unwrap();
        let connection = PeerConnection::new(
            "127.0.0.1:6881".parse().unwrap(),
            InfoHashes {
                v1: Some(Id20::new(HASH)),
                v2: None,
            },
            Id20::new([1; 20]),
            &counting,
            Some(PeerConnectionOptions::default()),
            BlockingSpawner::new(1),
            Arc::new(connector),
        );
        let (ours, mut theirs) = tokio::io::duplex(1 << 16);
        let (reader, writer) = halves(ours);
        let (meter, reader, writer) = Meter::wrap(reader, writer);
        let incoming = CheckedIncomingConnection {
            kind: ConnectionKind::Tcp,
            addr: "127.0.0.1:6881".parse().unwrap(),
            reader,
            writer: writer.into(),
            read_buf: ReadBuf::new(),
            handshake: Handshake::new(Id20::new(HASH), Id20::new([2; 20])),
            encryption: crate::mse::Encryption::None,
            meter,
        };

        let lengths = Lengths::new(u64::from(BLOCK) * 4, BLOCK).unwrap();
        let block = |piece: u32| {
            lengths
                .chunk_info_from_received_data(
                    lengths.validate_piece_index(piece).unwrap(),
                    0,
                    BLOCK,
                )
                .unwrap()
        };
        let (tx, rx) = mpsc::unbounded_channel();
        let mut shook = counting.shook.subscribe();
        let mut had = counting.had.subscribe();
        let mut read_by_peer = [0u64; MessageKind::COUNT];
        let peer = async {
            let mut handshake = [0; 68];
            theirs.read_exact(&mut handshake).await.unwrap();
            // The peer names its extension ids, so that ours can be
            // addressed to it.
            let mut wire = vec![0; 1024];
            let len = Message::Extended(ExtendedMessage::Handshake(ExtendedHandshake::new()))
                .serialize(&mut wire, &PeerExtendedMessageIds::my)
                .unwrap();
            theirs.write_all(&wire[..len]).await.unwrap();
            shook.wait_for(|shook| *shook).await.unwrap();

            for request in [
                WriterRequest::Message(Message::Unchoke),
                WriterRequest::Message(Message::Have(1)),
                WriterRequest::Message(Message::Have(2)),
                WriterRequest::UtPex(UtPex::default()),
                WriterRequest::Answer(Answer::Reject(Request::new(3, 0, BLOCK))),
                WriterRequest::ReadChunkRequest(block(0)),
                WriterRequest::ReadChunkRequest(block(1)),
                WriterRequest::ReadChunkRequest(block(2)),
                WriterRequest::Message(Message::Choke),
                // The last of ours.
                WriterRequest::Message(Message::Have(9)),
            ] {
                tx.send(request).unwrap();
            }
            loop {
                let mut body = vec![0; theirs.read_u32().await.unwrap() as usize];
                theirs.read_exact(&mut body).await.unwrap();
                read_by_peer[kind_on_the_wire(&body) as usize] += 1;
                if body == [4, 0, 0, 0, 9] {
                    break;
                }
            }

            let data = [9; 100];
            let mut len = 0;
            for message in [
                Message::KeepAlive,
                Message::Interested,
                Message::Request(Request::new(0, 0, BLOCK)),
                Message::Request(Request::new(1, 0, BLOCK)),
                Message::Piece(Piece::from_data(0, 0, &data)),
                Message::Cancel(Request::new(1, 0, BLOCK)),
                Message::Extended(ExtendedMessage::UtPex(UtPex::default())),
                Message::NotInterested,
                // The last of theirs.
                Message::Have(5),
            ] {
                len += message
                    .serialize(&mut wire[len..], &PeerExtendedMessageIds::my)
                    .unwrap();
            }
            theirs.write_all(&wire[..len]).await.unwrap();
            had.wait_for(|piece| *piece == 5).await.unwrap();
            drop(tx);
        };
        let (result, ()) = tokio::join!(connection.manage_peer_incoming(rx, incoming), peer);
        assert!(matches!(result, Err(Error::TorrentIsNotLive)), "{result:?}");

        let snapshot = counting.stats.snapshot();
        let counted = |direction: Direction| -> Vec<(&'static str, u64)> {
            snapshot
                .messages()
                .filter(|(d, _, count)| *d == direction && *count > 0)
                .map(|(_, kind, count)| (kind.as_str(), count))
                .collect()
        };
        let read_by_peer: Vec<_> = MessageKind::ALL
            .iter()
            .map(|kind| (kind.as_str(), read_by_peer[*kind as usize]))
            .filter(|(_, count)| *count > 0)
            .collect();
        assert_eq!(counted(Direction::Out), read_by_peer);
        assert_eq!(
            read_by_peer,
            [
                ("choke", 1),
                ("unchoke", 1),
                ("have", 3),
                ("piece", 3),
                ("have_all", 1),
                ("reject", 1),
                ("extended_handshake", 1),
                ("pex", 1),
            ]
        );
        assert_eq!(
            counted(Direction::In),
            [
                ("keep_alive", 1),
                ("interested", 1),
                ("not_interested", 1),
                ("have", 1),
                ("request", 2),
                ("piece", 1),
                ("cancel", 1),
                ("extended_handshake", 1),
                ("pex", 1),
            ]
        );
    }

    // A connection's end is counted under what ended it: nothing, or the
    // error it ended with.
    #[test]
    fn a_close_is_named_for_what_ended_the_connection() {
        use super::CloseReason;
        let io = || std::io::Error::other("test");
        for (error, reason) in [
            (None, "closed"),
            (Some(Error::Disconnect), "closed"),
            (Some(Error::TorrentIsNotLive), "closed"),
            (Some(Error::PeerDisconnected), "peer_closed"),
            (Some(Error::PeerDisconnectedReadingHandshake), "peer_closed"),
            (Some(Error::Timeout("reading")), "timeout"),
            (Some(Error::ConnectTimeout("connecting")), "timeout"),
            (Some(Error::TcpConnect(io())), "connect_failed"),
            (Some(Error::TcpDisabled), "not_allowed"),
            (Some(Error::Read(io())), "io"),
            (Some(Error::WriteHandshake(io())), "io"),
            (Some(Error::WrongInfoHash), "wrong_info_hash"),
            (Some(Error::ConnectingToOurselves), "ourselves"),
            (Some(Error::HashMessageFromV1Peer), "protocol"),
            (Some(Error::ReadChunk(anyhow::anyhow!("test"))), "disk"),
            (Some(Error::Anyhow(anyhow::anyhow!("test"))), "other"),
        ] {
            assert_eq!(
                CloseReason::of(error.as_ref()).as_str(),
                reason,
                "{error:?}"
            );
        }
    }

    // Every kind has a name of its own, and its place among its counts.
    #[test]
    fn every_kind_has_its_own_name_and_place() {
        let names: std::collections::HashSet<_> =
            MessageKind::ALL.iter().map(|kind| kind.as_str()).collect();
        assert_eq!(names.len(), MessageKind::COUNT);
        for (place, kind) in MessageKind::ALL.iter().enumerate() {
            assert_eq!(*kind as usize, place);
        }
        assert_eq!(Direction::In as usize, 0);
        assert_eq!(Direction::Out as usize, 1);
    }

    // Under RC4 the write encrypts the buffer in place, so the kind of the
    // first frame is taken before the write: a have-all sent on an RC4
    // connection is counted as one.
    #[tokio::test(start_paused = true)]
    async fn a_have_all_sent_under_rc4_is_counted_as_a_have_all() {
        let counting = Counting::default();
        let (dialed, _dials) = mpsc::unbounded_channel();
        let connector = StreamConnector::new(StreamConnectorArgs {
            enable_tcp: true,
            ..StreamConnectorArgs::over(Arc::new(Dialed(dialed)))
        })
        .await
        .unwrap();
        let connection = PeerConnection::new(
            "127.0.0.1:6881".parse().unwrap(),
            InfoHashes {
                v1: Some(Id20::new(HASH)),
                v2: None,
            },
            Id20::new([1; 20]),
            &counting,
            Some(PeerConnectionOptions::default()),
            BlockingSpawner::new(1),
            Arc::new(connector),
        );
        let (ours, mut theirs) = tokio::io::duplex(1 << 16);
        let (reader, writer) = halves(ours);
        let (meter, reader, writer) = Meter::wrap(reader, writer);
        const KEY: &[u8] = b"an rc4 key for the test";
        let incoming = CheckedIncomingConnection {
            kind: ConnectionKind::Tcp,
            addr: "127.0.0.1:6881".parse().unwrap(),
            reader,
            writer: crate::mse::Outbound::rc4(writer, crate::mse::Rc4::new(KEY)),
            read_buf: ReadBuf::new(),
            handshake: Handshake::new(Id20::new(HASH), Id20::new([2; 20])),
            encryption: crate::mse::Encryption::Rc4,
            meter,
        };
        let (tx, rx) = mpsc::unbounded_channel();
        let mut shook = counting.shook.subscribe();
        let mut saw_have_all = false;
        let peer = async {
            let mut decrypt = crate::mse::Rc4::new(KEY);
            let mut handshake = [0; 68];
            theirs.read_exact(&mut handshake).await.unwrap();
            decrypt.apply(&mut handshake);
            let mut wire = vec![0; 1024];
            let len = Message::Extended(ExtendedMessage::Handshake(ExtendedHandshake::new()))
                .serialize(&mut wire, &PeerExtendedMessageIds::my)
                .unwrap();
            theirs.write_all(&wire[..len]).await.unwrap();
            shook.wait_for(|shook| *shook).await.unwrap();
            tx.send(WriterRequest::Message(Message::Have(9))).unwrap();
            loop {
                let mut head = [0u8; 4];
                theirs.read_exact(&mut head).await.unwrap();
                decrypt.apply(&mut head);
                let mut body = vec![0; u32::from_be_bytes(head) as usize];
                theirs.read_exact(&mut body).await.unwrap();
                decrypt.apply(&mut body);
                saw_have_all |= body == [14];
                // The writer counted the bitfield frame before it framed
                // this one.
                if body == [4, 0, 0, 0, 9] {
                    break;
                }
            }
            drop(tx);
        };
        let (_result, ()) = tokio::join!(connection.manage_peer_incoming(rx, incoming), peer);
        assert!(saw_have_all, "the peer read a have-all off the wire");
        let snapshot = counting.stats.snapshot();
        let out: Vec<_> = snapshot
            .messages()
            .filter(|(d, _, count)| *d == Direction::Out && *count > 0)
            .map(|(_, kind, count)| (kind.as_str(), count))
            .collect();
        assert!(out.contains(&("have_all", 1)), "{out:?}");
        assert!(!out.iter().any(|(kind, _)| *kind == "bitfield"), "{out:?}");
    }
}
