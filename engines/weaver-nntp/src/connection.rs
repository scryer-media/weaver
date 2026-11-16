// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What one NNTP server connection is configured with: its route, address,
// credentials, TLS and timeouts, the read buffer sizes, and how PIPELINING
// support is learned. The connection itself lives in `blocking`.

use std::sync::Arc;
use std::time::Duration;

// The read buffer sizes of one NNTP connection, and the only source of them.
//
// A connection holds at most `read_buf_capacity` plus `socket_read_size`
// bytes of read buffers: the read buffer the decoder reads from, and, on a
// rustls connection, the ciphertext buffer each socket read fills. A plain
// connection reads the socket straight into the read buffer and holds only
// that; s2n buffers records itself and reads `socket_read_size` per call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NntpBufferProfile {
    pub read_buf_capacity: usize,
    pub socket_read_size: usize,
}

// The largest socket read, taken whenever memory allows. Reads from a real
// provider rarely pass 64 KiB; on loopback, where reads arrive in bursts,
// 64 KiB cost a plain or rustls lane about 4% more CPU and 128 KiB nothing.
const SOCKET_READ_MAX: usize = 128 * 1024;
// The smallest: two TLS records.
const SOCKET_READ_FLOOR: usize = 32 * 1024;
// All connections together keep their read buffers within this share of
// memory, and within `READ_BUFFER_BUDGET_MAX`, unless the floor needs more.
const READ_BUFFER_MEMORY_SHARE: u64 = 32;
const READ_BUFFER_BUDGET_MAX: u64 = 256 * 1024 * 1024;

impl NntpBufferProfile {
    // Sizes for one of `total_connections` connections on a host with
    // `available_bytes` of memory. Every connection's buffers fit the shared
    // budget until the floor stops them shrinking.
    pub fn adaptive(available_bytes: u64, total_connections: usize) -> Self {
        let total_budget =
            (available_bytes / READ_BUFFER_MEMORY_SHARE).min(READ_BUFFER_BUDGET_MAX) as usize;
        let per_connection = total_budget / total_connections.max(1);
        // The read buffer is two socket reads: one read, what the last read
        // left of a line, and room for the records a TLS read decrypts.
        Self::with_socket_read(allocation_size_at_most(per_connection / 3))
    }

    fn with_socket_read(socket_read_size: usize) -> Self {
        let socket_read_size = socket_read_size.clamp(SOCKET_READ_FLOOR, SOCKET_READ_MAX);
        Self {
            read_buf_capacity: 2 * socket_read_size,
            socket_read_size,
        }
    }

    // The most read buffer memory one connection holds.
    #[cfg(test)]
    pub(crate) fn per_connection_bytes(&self) -> usize {
        self.read_buf_capacity + self.socket_read_size
    }
}

// The largest allocator size class at or below `bytes`. Classes come four to
// each doubling, so a buffer of this size is never rounded up past its share.
fn allocation_size_at_most(bytes: usize) -> usize {
    if bytes < 16 * 1024 {
        return bytes;
    }
    let step = (1usize << bytes.ilog2()) / 4;
    bytes / step * step
}

// A connection outside the download pool: the smallest sizes.
impl Default for NntpBufferProfile {
    fn default() -> Self {
        Self::with_socket_read(SOCKET_READ_FLOOR)
    }
}

// How an NNTP connection determines PIPELINING support.
//
// Application runtime connections use the persisted server setting. Explicit
// server validation opts into [`Self::Probe`] and performs one post-auth
// CAPABILITIES exchange instead.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PipeliningCapability {
    // Discover the capability from the server.
    #[default]
    Probe,
    // Use the persisted server setting and skip CAPABILITIES.
    Known(bool),
}

// Configuration for connecting to a single NNTP server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    // The route every connection to this server dials through.
    pub dialer: Arc<crate::route_dialer::RouteDialer>,
    // Hostname or IP address.
    pub host: String,
    // Port number.
    pub port: u16,
    // Use implicit TLS (true for port 563).
    pub tls: bool,
    // Use STARTTLS after plain TCP connect.
    pub starttls: bool,
    // Username for AUTHINFO USER.
    pub username: Option<String>,
    // Password for AUTHINFO PASS.
    pub password: Option<String>,
    // Timeout for the entire connection setup (connect + TLS + auth).
    // Clamped to a minimum of 1 second.
    pub connect_timeout: Duration,
    // Timeout for individual command responses.
    // Clamped to a minimum of 1 second.
    pub command_timeout: Duration,
    // Internal read-buffer sizing profile.
    pub buffer_profile: NntpBufferProfile,
    // Optional path to a PEM-encoded CA certificate to trust in addition
    // to the system/Mozilla roots (e.g. self-signed or internal CAs).
    pub tls_ca_cert: Option<std::path::PathBuf>,
    // One explicitly adopted leaf certificate allowed only when normal TLS
    // verification fails because this server's hostname does not match.
    pub tls_name_mismatch_certificate_der: Option<Vec<u8>>,
    // Source of the PIPELINING capability for this connection.
    pub pipelining: PipeliningCapability,
    // BODY pipelining depth a previous run proved for this server, if any.
    // Inert for the connection itself; the download lanes read it to start
    // where the last run left off instead of rediscovering the depth.
    pub pipelining_depth: Option<u8>,
    // Which AEAD family the TLS ClientHello offers first.
    pub tls_cipher_preference: crate::tls::TlsCipherPreference,
}

// Tests build their configs from this. A product build has no default: a
// config always carries the route it dials through.
#[cfg(any(test, feature = "test-support"))]
impl Default for ServerConfig {
    fn default() -> Self {
        Self::new(crate::route_dialer::RouteDialer::direct_for_tests())
    }
}

impl ServerConfig {
    // A config that dials through `dialer`, with every other setting at its
    // default.
    pub fn new(dialer: Arc<crate::route_dialer::RouteDialer>) -> Self {
        ServerConfig {
            dialer,
            host: String::new(),
            port: 563,
            tls: true,
            starttls: false,
            username: None,
            password: None,
            connect_timeout: Duration::from_secs(30),
            command_timeout: Duration::from_mins(1),
            buffer_profile: NntpBufferProfile::default(),
            tls_ca_cert: None,
            tls_name_mismatch_certificate_der: None,
            pipelining: PipeliningCapability::Probe,
            pipelining_depth: None,
            tls_cipher_preference: crate::tls::TlsCipherPreference::Auto,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_config_defaults() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.port, 563);
        assert!(cfg.tls);
        assert!(!cfg.starttls);
        assert!(cfg.username.is_none());
        assert!(cfg.password.is_none());
        assert_eq!(cfg.pipelining, PipeliningCapability::Probe);
    }
}
