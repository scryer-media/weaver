// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// HTTP clients on the host's own egress, for a consumer's tests that need a
// finished client and have no proxy runtime to take one from.

/// The client `builder` makes, leaving as the host routes it: the system
/// egress a route with nothing saved takes.
pub fn direct_client(builder: reqwest::ClientBuilder) -> reqwest::Result<reqwest::Client> {
    builder.build()
}
