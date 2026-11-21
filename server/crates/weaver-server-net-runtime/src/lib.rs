// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The network runtime above the settings: the saved proxies, routes and DNS,
// the servers and their transfer policies, the routed HTTP client and the
// speed test legs that fetch over it. The composed database reaches them
// through `NetDatabase`.
// Pinned as blanket in the route guard: the speed test's live probe moved
// here with its providers, and dials the public endpoints over a direct leg
// of its own, which trips the one lint that carries both the route ban and
// the writer gate.
#![cfg_attr(
    test,
    allow(
        clippy::disallowed_methods,
        reason = "the live probe dials the public endpoints over its own direct leg"
    )
)]
#![deny(clippy::await_holding_lock)]

pub mod database;
pub mod ingest;
pub mod post_processing;
pub mod proxies;
pub mod rss;
pub mod servers;
pub mod settings;
pub mod speed_test;
pub mod torrents;
