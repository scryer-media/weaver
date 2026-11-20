// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The speed test's runs as the rest of the server sees them moved to
// weaver-server-net-runtime. The modules below keep the tests that open the
// composed database under their old paths.
pub use weaver_server_net_runtime::speed_test::*;
pub mod http;
pub mod overrides;
pub mod progress;
pub mod providers;
