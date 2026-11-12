// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

#[macro_use]
mod macros;
mod addr;
mod byte_ring;
mod congestion;
mod constants;
mod delay;
#[cfg(test)]
mod e2e_tests;
mod error;
mod ingress;
mod message;
pub mod mtu;
pub mod raw;
mod pacer;
mod recovery;
mod rtte;
mod runtime;
mod seq_nr;
mod socket;
mod spawn_utils;
mod stats;
mod stream;
mod stream_dispatch;
mod stream_rx;
mod stream_tx;
mod stream_tx_segments;
#[cfg(test)]
mod test_util;
mod traits;
mod udp_error;
mod utils;

pub use addr::{canonical_ip_addr, canonical_socket_addr};
pub use congestion::ledbat::LedbatParams;
pub use error::{Error, Result};
pub use socket::{SocketOpts, UtpSocket};
pub use stats::{UtpStats, UtpStatsSnapshot};
pub use stream::UtpStream;
pub use stream_rx::UtpStreamReadHalf;
pub use stream_tx::UtpStreamWriteHalf;
pub use traits::{
    DatagramSend, DefaultUtpEnvironment, PollSendToVectored, Transport, UtpEnvironment,
};
pub use udp_error::{is_message_size_error, message_size_error};

type Payload = Vec<u8>;
