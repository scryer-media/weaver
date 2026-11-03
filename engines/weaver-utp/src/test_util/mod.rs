// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

use std::net::{Ipv4Addr, SocketAddr};

use env::MockUtpEnvironment;
use transport::MockUtpTransport;

use crate::{UtpSocket, UtpStream, socket::Dispatcher};

pub mod cmphead;
pub mod env;
pub mod transport;

pub type MockUtpSocket = UtpSocket<MockUtpTransport, MockUtpEnvironment>;
pub type MockUtpStream = UtpStream;
pub type MockDispatcher = Dispatcher<MockUtpTransport, MockUtpEnvironment>;

// Logs to the test's output at the level RUST_LOG names, none when unset.
pub fn setup_test_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

pub const ADDR_1: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 1);
pub const ADDR_2: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 2);
