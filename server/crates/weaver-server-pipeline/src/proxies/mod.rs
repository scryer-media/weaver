// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_net::proxies::dns;
mod dns_persistence;
pub use dns_persistence::ProxiesDnsPersistenceQueries;
pub(crate) use dns_persistence::validate_restored_dns;
pub use dns_persistence::{StoredDns, UnreadableDnsList};
use weaver_server_net::proxies::fault;
use weaver_server_net::proxies::interfaces;
use weaver_server_net::proxies::model;
use weaver_server_net::proxies::network;
pub mod network_metrics;
mod network_persistence;
pub use network_persistence::ProxiesNetworkPersistenceQueries;
pub(crate) use network_persistence::lock_network;
mod network_runtime;
pub(crate) mod persistence;
pub use persistence::ProxiesPersistenceQueries;
use weaver_server_net::proxies::pool_stage;
use weaver_server_net::proxies::refusal;
use weaver_server_net::proxies::route_health;
mod routed_http;
mod runtime;
mod socket_ledger;
pub use dns::*;
pub use fault::{ProxyFault, ProxyTestPass, fault_params};
pub use interfaces::*;
pub use model::*;
pub use network::*;
pub use network_runtime::*;
pub use pool_stage::*;
pub use refusal::*;
pub use route_health::{QUIET_PERIOD, RouteChange, RouteEvent, RouteHealth, RouteKind};
pub(crate) use routed_http::{
    AdmitError, Admitted, AttemptError, admit, fetch_on, pinned, start_bridge, through_bridge,
};
pub use routed_http::{
    FetchOptions, FetchPath, PathBridge, RoutedBody, read_body, routed_fetch, with_routed_client,
};
#[cfg(test)]
pub(crate) use routed_http::{direct_runtime, outbound_only_through};
pub use runtime::*;
pub use socket_ledger::*;
use weaver_server_net::proxies::weighted;
pub use weighted::*;
