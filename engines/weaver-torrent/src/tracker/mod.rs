// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

mod handover;
mod tiers;
mod tracker_comms;
mod tracker_comms_http;
mod tracker_comms_udp;

pub use handover::TrackerHandover;
pub use tiers::{Tracker, added_tiers, tidy_tiers, without_trackers};
pub use tracker_comms::*;
pub use tracker_comms_udp::UdpTrackerClient;
