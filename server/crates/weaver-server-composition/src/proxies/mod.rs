// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The proxies moved to weaver-server-net-runtime. The modules below keep the
// tests that open the composed database under their old paths.
pub use weaver_server_net_runtime::proxies::*;
pub mod dns_persistence;
pub mod network_metrics;
pub mod network_persistence;
pub mod network_runtime;
pub mod routed_http;
pub mod runtime;
pub mod socket_ledger;
#[cfg(any(test, feature = "test-support"))]
pub use routed_http::{direct_runtime, outbound_only_through};
