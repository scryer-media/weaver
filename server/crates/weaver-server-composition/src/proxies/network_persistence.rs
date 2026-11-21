// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved to weaver-server-net-runtime; the tests open the composed database,
// so they stay here under the same module path.
pub use weaver_server_net_runtime::proxies::network_persistence::*;

#[cfg(test)]
use super::{
    EgressBinding, EgressInterface, LegPath, ProxyPool, ProxyProfile, Route, RoutingPolicy, Rung,
};
#[cfg(test)]
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
#[cfg(test)]
use crate::{Database, StateError};

#[cfg(test)]
#[path = "network_persistence_tests.rs"]
mod tests;
