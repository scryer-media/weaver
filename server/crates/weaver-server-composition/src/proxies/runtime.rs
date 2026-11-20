// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved to weaver-server-net-runtime; the tests open the composed database,
// so they stay here under the same module path.
pub use weaver_server_net_runtime::proxies::runtime::*;

#[cfg(test)]
use super::*;
#[cfg(test)]
use crate::Database;
#[cfg(test)]
use std::{
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};

#[cfg(test)]
mod tests;
