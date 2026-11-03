// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error sending SYN: {0}")]
    ErrorSendingSyn(std::io::Error),

    #[error("task cancelled")]
    TaskCancelled,

    #[error("error receiving: {0}")]
    Recv(std::io::Error),

    #[error("error sending UDP packet: {0}")]
    Send(std::io::Error),

    #[error("ST_RESET received")]
    StResetReceived,

    #[error("remote was inactive for too long")]
    RemoteInactiveForTooLong,

    #[error(
        "bug: error filling output buffer from user_tx: too small buffer: out_buf.len() < len ({out_buf_len} < {len})"
    )]
    BugTooSmallBuffer { out_buf_len: i32, len: i32 },
    #[error(
        "bug in buffer computations: user_tx_buflen={user_tx_buflen} segmented_len={segmented_len}"
    )]
    BugInBufferComputations {
        user_tx_buflen: i32,
        segmented_len: i32,
    },
    #[error("bug: truncate_front: skipped({skipped}) != count({count})")]
    BugTruncateFront { skipped: i32, count: i32 },
    #[error("bug in assembler: slot {0} should be there")]
    BugAssemblerMissingSlot(usize),
    #[error("bug: can't enqueue next segment")]
    BugCantEnqueue,
    #[error(
        "bug: got EMSGSIZE error, but the last message was not a matching MTU probe that we could pop."
    )]
    BugEmsgSizeNoProbe,
    #[error("bug: received a packet in Closed state, we shouldn't have reached here")]
    BugRecvInClosed,
    #[error("bug: unexpected packet in SynReceived state. We should have sent the SYN-ACK first.")]
    BugUnexpectedPacketInSynReceived,
    #[error("bug: unreachable")]
    BugUnreachable,
    #[error("bug: invalid message, expected ST_DATA or ST_FIN")]
    BugInvalidMessageExpectedStDataOrFin,
    #[error("bug: offset beyond buffer bounds")]
    BugOffsetBeyondBufferBounds,
    #[error("bug: requested length exceeds buffer bounds")]
    BugRequestedLengthExceedsBufferBounds,

    #[error("ST_DATA has zero payload")]
    ZeroPayloadStData,

    #[error("serialize: too small buffer")]
    SerializeTooSmallBuffer,

    #[error("link mtu exceeds u16")]
    ValidateLinkMtuExceedsU16,
    #[error(
        "provided link_mtu ({link_mtu}) too low, not enough for even 1-byte IPv4 packets (min {min_mtu})"
    )]
    ValidateMtuTooLow { link_mtu: u16, min_mtu: u16 },

    #[error("too many active connections")]
    TooManyActiveConnections,

    #[error("max number of retransmissions reached")]
    MaxRetransmissionsReached,
    #[error("max syn-ack retransmissions reached")]
    MaxSynAckRetransmissionsReached,
    #[error("the remote stopped acknowledging")]
    TimedOut,
    #[error("the remote did not answer the connect")]
    ConnectTimedOut,

    #[error("dispatcher dead")]
    DispatcherDead,
}

pub type Result<T> = std::result::Result<T, Error>;
