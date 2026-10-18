// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The unlock window: the volumes fetched ahead of the normal order because an
// object reading in order will reach them next. Objects bid for volumes with
// a weight; the root draws the lightest bids into a window of a few volumes.
//
// The caps bound how much of the post is fetched out of order. A reader that
// can only go in order (a solid engine, a stream) gets fewer volumes ahead of
// it, since everything it is handed early waits for the volume before it.
// The window holds only volumes with work still queued: one whose articles
// are all in flight or done takes no place.
//
// An object bids for a volume once. A bid the window has no room for stands
// until a later round draws it or its volume has nothing left queued, so a
// full window delays a bidder and never drops it.

use weaver_graph::{ByteRange, NodeId, VolumeIndex, Weight};

pub(crate) const WINDOW_VOLUMES: usize = 8;
pub(crate) const WINDOW_IN_ORDER_VOLUMES: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Bid {
    pub(crate) volume: VolumeIndex,
    pub(crate) range: ByteRange,
    pub(crate) weight: Weight,
    pub(crate) bidder: NodeId,
    // The bidder reads only in order.
    pub(crate) in_order: bool,
}

#[derive(Clone, Copy, Debug)]
struct Held {
    volume: VolumeIndex,
    rank: u32,
    in_order: bool,
}

#[derive(Debug, Default)]
pub(crate) struct Window {
    bids: Vec<Bid>,
    held: Vec<Held>,
    next_rank: u32,
}

impl Window {
    pub(crate) fn bid(&mut self, bid: Bid) {
        if !self.bids.contains(&bid) {
            self.bids.push(bid);
        }
    }

    // The window's rank for a volume, if it holds it.
    pub(crate) fn rank(&self, volume: VolumeIndex) -> Option<u32> {
        self.held
            .iter()
            .find(|held| held.volume == volume)
            .map(|held| held.rank)
    }

    // Let go of the volumes with nothing left queued, then draw the standing
    // bids into the free places, lightest first. Returns every bid that
    // landed in the window, with its volume's rank; the rest stand.
    pub(crate) fn draw(&mut self, mut queued: impl FnMut(VolumeIndex) -> bool) -> Vec<(Bid, u32)> {
        self.held.retain(|held| queued(held.volume));
        let mut bids = std::mem::take(&mut self.bids);
        bids.sort_by_key(|bid| (bid.weight, bid.bidder, bid.volume, bid.range.start));
        let mut drawn = Vec::new();
        for bid in bids {
            if let Some(rank) = self.rank(bid.volume) {
                drawn.push((bid, rank));
                continue;
            }
            if !queued(bid.volume) {
                continue;
            }
            if !self.has_room(bid.in_order) {
                self.bids.push(bid);
                continue;
            }
            let rank = self.next_rank;
            self.next_rank += 1;
            self.held.push(Held {
                volume: bid.volume,
                rank,
                in_order: bid.in_order,
            });
            drawn.push((bid, rank));
        }
        drawn
    }

    fn has_room(&self, in_order: bool) -> bool {
        let in_order_held = self.held.iter().filter(|held| held.in_order).count();
        self.held.len() < WINDOW_VOLUMES && (!in_order || in_order_held < WINDOW_IN_ORDER_VOLUMES)
    }
}
