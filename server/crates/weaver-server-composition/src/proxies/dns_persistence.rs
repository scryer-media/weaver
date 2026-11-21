// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved to weaver-server-net-runtime; the tests open the composed database,
// so they stay here under the same module path.
pub use weaver_server_net_runtime::proxies::dns_persistence::*;

#[cfg(test)]
use super::{
    Consumer, DnsProfile, DnsProfileKind, DnsServerList, DnsTrust, DnsWho, LegPath, ProxyKind,
    ProxyProfile, Rung, SYSTEM_DNS_PROFILE_ID,
};
#[cfg(test)]
use crate::Database;
#[cfg(test)]
use crate::StateError;
#[cfg(test)]
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
#[cfg(test)]
use std::collections::HashMap;

#[cfg(test)]
#[path = "dns_persistence_tests.rs"]
mod tests;
