// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

mod e2e;
mod e2e_another_local_client;
mod e2e_client_identity;
mod e2e_dht_identity;
mod e2e_initial_have;
mod e2e_mse;
mod e2e_pause;
mod e2e_peers;
mod e2e_piece_priority;
mod e2e_ratelimits;
mod e2e_session_stop;
mod e2e_socket_factory;
mod e2e_ssl;
mod e2e_stream;
pub(crate) mod e2e_tracker_announces;
pub mod embedder_util;
pub mod test_util;
