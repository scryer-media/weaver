// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::sync::atomic::AtomicU32;

use crate::{
    stream_connect::ConnectionKind,
    torrent_state::{
        live::peer::PeerState,
        peer::LivePeerState,
        utils::{atomic_dec, atomic_inc},
    },
};

gen_stats!(AggregatePeerStatsAtomic AggregatePeerStats, [
    queued u32,
    connecting u32,
    live u32,
    live_tcp u32,
    live_utp u32,
    live_seeders u32,
    seen u32,
    dead u32,
    not_needed u32,
    steals u32
], []);

impl AggregatePeerStatsAtomic {
    fn counter(&self, state: &PeerState) -> &AtomicU32 {
        match state {
            PeerState::Connecting(_) => &self.connecting,
            PeerState::Live(_) => &self.live,
            PeerState::Queued => &self.queued,
            PeerState::Dead => &self.dead,
            PeerState::NotNeeded => &self.not_needed,
        }
    }

    fn live_kind_counter(&self, l: &LivePeerState) -> &AtomicU32 {
        match l.connection_kind {
            ConnectionKind::Tcp => &self.live_tcp,
            ConnectionKind::Utp => &self.live_utp,
        }
    }

    pub(crate) fn inc(&self, state: &PeerState) {
        if let PeerState::Live(l) = state {
            atomic_inc(self.live_kind_counter(l));
            if l.seeder {
                atomic_inc(&self.live_seeders);
            }
        }
        atomic_inc(self.counter(state));
    }

    pub(crate) fn dec(&self, state: &PeerState) {
        if let PeerState::Live(l) = state {
            atomic_dec(self.live_kind_counter(l));
            if l.seeder {
                atomic_dec(&self.live_seeders);
            }
        }
        atomic_dec(self.counter(state));
    }

    pub(crate) fn seeder_flag_changed(&self, seeder: bool) {
        if seeder {
            atomic_inc(&self.live_seeders);
        } else {
            atomic_dec(&self.live_seeders);
        }
    }

    pub fn inc_steals(&self) {
        atomic_inc(&self.steals);
    }
}
