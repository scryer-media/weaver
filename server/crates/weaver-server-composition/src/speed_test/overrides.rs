// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved to weaver-server-net-runtime; the tests open the composed database,
// so they stay here under the same module path.
pub use weaver_server_net_runtime::speed_test::overrides::*;

#[cfg(test)]
use weaver_server_speedtest::speed_test::overrides::{next_leg, weight_shares};

#[cfg(test)]
use super::model::PathOverride;
#[cfg(test)]
use crate::proxies::{LegPath, ProxyPool, Route, RouteLeg, Rung};

#[cfg(test)]
#[path = "overrides_tests.rs"]
mod tests;
