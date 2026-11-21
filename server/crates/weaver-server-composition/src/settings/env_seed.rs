// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved to weaver-server-net-runtime; the tests open the composed database, so they
// stay here under the same module path.
pub use weaver_server_net_runtime::settings::env_seed::*;

#[cfg(test)]
use crate::bandwidth::QuotaWeekday;
#[cfg(test)]
use crate::persistence::Database;
#[cfg(test)]
use crate::servers::{
    ServerConfig, ServerConnectivityResult, ServerDownloadQuotaConfig, ServerDownloadQuotaPeriod,
};
#[cfg(test)]
use crate::settings::Config;

#[cfg(test)]
mod tests;
