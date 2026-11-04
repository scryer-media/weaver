// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

use std::{num::NonZeroUsize, time::Duration};

use tracing::Level;

pub const IPV4_HEADER: u16 = 20;
pub const IPV6_HEADER: u16 = 40;

pub const UDP_HEADER: u16 = 8;
pub const UTP_HEADER: u16 = 20;

/// Default per-stream receive capacity, in bytes.
pub const RX_BUF_SIZE_PER_VSOCK_DEFAULT: NonZeroUsize = non_zero_const!(1024 * 1024);

/// How many unACKed bytes the socket can store without blocking writer.
/// Also how many bytes can we send without receivng an ACK.
/// This should be governed by BDP (bandwidth delay product). If it's too low, the pipeline
/// would be stalling.
/// If it's too high, every VSock would take too much memory.
pub const TX_BUF_SIZE_PER_VSOCK_INITIAL_DEFAULT: NonZeroUsize = non_zero_const!(32 * 1024);
pub const TX_BUF_SIZE_PER_VSOCK_MAX_DEFAULT: NonZeroUsize = non_zero_const!(1024 * 1024);

// Delayed ACK timer. Linux has 40ms, so we set to it too.
pub const ACK_DELAY: Duration = Duration::from_millis(40);

/// This HUGELY impacts perf. The higher this number, the better the perf, as a lot of CPU is
/// spent to send ACKs.
/// However if N * mss happens to exceed cwnd, the performance would tank as we'll start sending
/// delayed ACKs only, and the pipeline would be stalling.
pub const IMMEDIATE_ACK_EVERY_RMSS: usize = 2;

pub const SYNACK_RESEND_INTERNAL: Duration = Duration::from_millis(200);

// u16 SeqNrs wrap around. If they are too far apart, this is used to detect if they wrapped or not.
pub const WRAP_TOLERANCE: u16 = 1024;

pub const RTTE_TRACING_LOG_LEVEL: Level = Level::TRACE;
pub const RECOVERY_TRACING_LOG_LEVEL: Level = Level::TRACE;

// How long to wait to kill the connection if the remote is non-responsive.
pub const DEFAULT_REMOTE_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(10);

// The least retransmission timeout once a round trip is known.
pub const DEFAULT_MIN_TIMEOUT: Duration = Duration::from_millis(500);
// Sends of one data packet before the connection gives up.
pub const DEFAULT_NUM_RESENDS: usize = 3;
// Sends of a SYN-ACK before an incoming connection gives up.
pub const DEFAULT_SYN_RESENDS: usize = 2;
// Sends of our FIN before the connection gives up on its ACK.
pub const DEFAULT_FIN_RESENDS: usize = 2;
// How long a connect waits for the remote's answer to its SYN.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

pub const SACK_DUP_THRESH: u8 = 3;
pub const SACK_DEPTH: usize = 64;

pub fn calc_pipe_expiry(rtt: Duration) -> Duration {
    // rtt/2 might be too aggressive
    rtt * 3 / 4
}
