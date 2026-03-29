//! `weaver-nntp` — Async NNTP protocol client for the Weaver Usenet downloader.
//!
//! This crate provides a single-connection NNTP client that supports:
//!
//! - Implicit TLS (port 563) and STARTTLS upgrades
//! - AUTHINFO USER/PASS authentication (RFC 4643)
//! - Multi-line response handling with dot-unstuffing
//! - Article retrieval via BODY, HEAD, ARTICLE commands
//! - Streaming article body support
//! - Multi-server connection pooling with health checks
//! - High-level client with automatic failover
//! - Structured error types
//!
//! # Usage
//!
//! ```rust,no_run
//! use weaver_nntp::client::{NntpClient, NntpClientConfig};
//! use weaver_nntp::connection::ServerConfig;
//!
//! # async fn example() -> weaver_nntp::error::Result<()> {
//! let config = NntpClientConfig::single(
//!     ServerConfig {
//!         host: "news.example.com".into(),
//!         port: 563,
//!         tls: true,
//!         username: Some("user".into()),
//!         password: Some("pass".into()),
//!         ..Default::default()
//!     },
//!     10, // max connections
//! );
//!
//! let client = NntpClient::new(config);
//! let body = client.fetch_body("<article-id@example.com>").await?;
//! println!("Got {} bytes", body.len());
//! client.shutdown().await;
//! # Ok(())
//! # }
//! ```

pub mod client;
pub mod codec;
pub mod commands;
pub mod connection;
pub mod error;
pub mod health;
pub mod pool;
pub mod response;
pub mod tls;
pub mod types;

// Re-export primary types for convenience.
pub use client::{DecodedBody, DecodedBodyError, NntpClient};
pub use codec::StreamChunk;
pub use connection::{NntpConnection, ServerConfig};
pub use error::{NntpError, Result};
pub use health::{CooldownReason, HealthConfig, HealthTracker, ServerHealth, ServerState};
pub use pool::{NntpPool, PoolConfig, PooledConnection, ServerId, ServerPoolConfig};
pub use types::{ArticleId, Capabilities, MultiLineResponse, Response, StatusCode};
