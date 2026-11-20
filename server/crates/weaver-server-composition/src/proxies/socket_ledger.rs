// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved to weaver-server-net-runtime; the tests open the composed database,
// so they stay here under the same module path.
pub use weaver_server_net_runtime::proxies::socket_ledger::*;

#[cfg(test)]
use super::{
    Consumer, Failover, LegPath, ProxyKind, ProxyPool, ProxyProfile, Route, Rung,
    WIREGUARD_INSTANCE_BYTES,
};
#[cfg(test)]
use weaver_tunnel::pipe::{Purpose, socket_buffers};

#[cfg(test)]
#[path = "socket_ledger_tests.rs"]
mod tests;
