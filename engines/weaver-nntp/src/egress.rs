// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use weaver_tunnel::egress::SocketEgress;

#[cfg(test)]
#[path = "egress_tests.rs"]
mod tests;
