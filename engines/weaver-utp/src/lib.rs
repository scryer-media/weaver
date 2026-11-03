// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

#[macro_use]
mod macros;
mod byte_ring;
mod congestion;
mod constants;
#[cfg(test)]
mod e2e_tests;
mod error;
mod message;
pub mod mtu;
pub mod raw;
mod recovery;
mod rtte;
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
mod utils;

pub use error::{Error, Result};
pub use socket::{CongestionConfig, CongestionControllerKind, SocketOpts, UtpSocket};
pub use stats::UtpStats;
pub use stream::UtpStream;
pub use stream_rx::UtpStreamReadHalf;
pub use stream_tx::UtpStreamWriteHalf;
pub use traits::{DefaultUtpEnvironment, PollSendToVectored, Transport, UtpEnvironment};

type Payload = Vec<u8>;
