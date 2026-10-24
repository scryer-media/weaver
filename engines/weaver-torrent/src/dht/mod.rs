// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

pub(crate) mod bprotocol;
mod dht;
mod error;
mod peer_store;
mod persistence;
mod query_limiter;
mod routing_table;
mod utils;

use std::sync::Arc;
use std::time::Duration;

pub use error::{Error, Result};

pub use crate::core::hash_id::Id20;
pub use crate::dht::dht::DhtStats;
pub use crate::dht::dht::{DhtConfig, DhtState, RequestPeersStream};
pub use persistence::{DhtPersistenceConfig, PersistentDht, dht_listen_addr};

pub type Dht = Arc<DhtState>;

// How long do we wait for a response from a DHT node.
pub(crate) const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
// How often a torrent searches the DHT for peers and announces itself, at
// the end of that search: libtorrent's `dht_announce_interval`
// (settings_pack.cpp), the period `session_impl::update_dht_announce_interval`
// gives each torrent.
pub(crate) const DHT_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(15 * 60);
// After how long we consider a routing table node questionable.
pub(crate) const INACTIVITY_TIMEOUT: Duration = Duration::from_secs(15 * 60);

pub struct DhtBuilder {}

impl DhtBuilder {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new() -> crate::dht::Result<Dht> {
        DhtState::new().await
    }

    pub async fn with_config(config: DhtConfig<'_>) -> crate::dht::Result<Dht> {
        DhtState::with_config(config).await
    }
}

pub static DHT_BOOTSTRAP: &[&str] = &["dht.transmissionbt.com:6881", "dht.libtorrent.org:25401"];
