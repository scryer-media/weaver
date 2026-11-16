// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// `weaver-nntp` — NNTP protocol client for the Weaver Usenet downloader.
//
// One connection implementation carries every session: the blocking
// connection in [`blocking`], which download lanes drive from their own
// threads. The crate provides:
//
// - Implicit TLS (port 563) and STARTTLS upgrades
// - AUTHINFO USER/PASS authentication (RFC 4643)
// - Multi-line response handling with dot-unstuffing
// - Pipelined BODY fetches with inline yEnc decoding
// - A multi-server pool that admits connections: permits, socket budget,
//   reconnect delay, over-limit holdoff and recovery quarantine
// - A client that ranks servers and hands out BODY lanes
// - Structured error types
//
// A caller builds an [`NntpClient`] from an [`client::NntpClientConfig`]
// and takes a [`BlockingBodyLane`] with
// [`NntpClient::try_acquire_blocking_body_lane`] on a blocking thread.

// Tests dial their own loopback fixtures.
#![cfg_attr(test, allow(clippy::disallowed_methods))]

mod address_plan;
pub mod blocking;
pub mod candidate_plan;
pub mod client;
pub mod codec;
pub mod commands;
pub mod connection;
pub mod egress;
pub mod error;
pub mod fused_yenc;
pub mod health;
pub mod plan_timing;
pub mod pool;
pub mod provider_policy;
pub mod recovery;
pub mod response;
pub mod revocation;
pub mod route_dialer;
mod route_stream;
pub mod server_caps;
pub mod server_set;
pub mod socket_budget;
pub mod tls;
pub mod transfer;
pub mod types;
pub mod uu;

#[cfg(test)]
mod test_support;

// Re-export primary types for convenience.
pub use address_plan::{
    ADDRESS_ATTEMPT_LIMIT, ADDRESS_REPLAN_INTERVAL, AddressPlanSnapshot, AddressSnapshot,
    RaceReason,
};
pub use blocking::{BlockingBodyLane, BlockingLaneStats, BlockingNntpConnection};
pub use client::{
    BodyLaneMode, BodyServerSelection, DecodedBody, DecodedBodyError, DecodedBodyTrace, NntpClient,
};
pub use codec::StreamChunk;
pub use connection::{PipeliningCapability, ServerConfig};
pub use error::{NntpError, Result};
pub use health::{CooldownReason, HealthConfig, HealthTracker, ServerHealth, ServerState};
pub use pool::{NntpPool, PoolConfig, ServerId, ServerIdentity, ServerPoolConfig};
pub use server_set::{MAX_PROVIDERS, ServerRemap, ServerSet};
pub use tls::TlsCipherPreference;
pub use types::{ArticleId, Capabilities, MultiLineResponse, Response, StatusCode};
