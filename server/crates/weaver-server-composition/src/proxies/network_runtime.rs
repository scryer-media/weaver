// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved to weaver-server-net-runtime; the tests open the composed database,
// so they stay here under the same module path.
pub use weaver_server_net_runtime::proxies::network_runtime::*;

#[cfg(test)]
use super::*;
#[cfg(test)]
use crate::Database;
#[cfg(test)]
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};
#[cfg(test)]
use weaver_tunnel::pipe::{
    DialError, Dialed, Dialer, NamesSlot, Revocable, SessionSockets, SocketBudget, Target,
};

#[cfg(test)]
#[path = "network_runtime_tests.rs"]
mod tests;
