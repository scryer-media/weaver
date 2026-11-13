// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

pub(crate) mod bprotocol;
mod error;
mod peer_store;
mod persistence;
mod query_limiter;
mod routing_table;
mod send_quota;
mod send_windows;
mod state;
mod utils;

use std::sync::Arc;
use std::time::Duration;

pub use error::{Error, Result};

pub use crate::core::hash_id::Id20;
pub use crate::dht::state::DhtStats;
pub use crate::dht::state::{DhtConfig, DhtState, RequestPeersStream};
pub use persistence::{DhtPersistenceConfig, PersistentDht, dht_listen_addr};

pub type Dht = Arc<DhtState>;

// How long a query waits for its answer, counted from the moment it is sent
// (libtorrent `rpc_manager::tick`, rpc_manager.cpp:397 and 420-421, v2.0.15,
// against the send time `observer::set_target` stamps as `invoke` sends it,
// rpc_manager.cpp:87-89 and 497-508).
pub(crate) const RESPONSE_TIMEOUT: Duration = Duration::from_secs(15);
// How often a torrent searches the DHT for peers and announces itself, at
// the end of that search: libtorrent's `dht_announce_interval`
// (settings_pack.cpp), the period `session_impl::update_dht_announce_interval`
// gives each torrent.
pub(crate) const DHT_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(15 * 60);
// After how long we consider a routing table node questionable.
pub(crate) const INACTIVITY_TIMEOUT: Duration = Duration::from_secs(15 * 60);
// A search that starts from fewer nodes we can count on than this also asks
// the routers (libtorrent `traversal_algorithm::start`); a table that holds
// this many is seeded.
pub(crate) const SEARCH_MIN_SEEDS: usize = 3;

pub struct DhtBuilder {}

impl DhtBuilder {
    pub async fn with_config(config: DhtConfig) -> crate::dht::Result<Dht> {
        DhtState::with_config(config).await
    }
}

pub static DHT_BOOTSTRAP: &[&str] = &["dht.transmissionbt.com:6881", "dht.libtorrent.org:25401"];
