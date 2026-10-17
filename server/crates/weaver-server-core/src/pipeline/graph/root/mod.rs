// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The article root: the one object that fetches from outside. Its volumes are
// the post's files, in name-table order, so root volume N is root member N.
// It is the article object, so this is the one place in the graph layer that
// knows a post is made of articles.
//
//   source   the seek-up side: a seek becomes a new class for the articles
//            that carry the range, answered Moved, Lost or Unseekable
//   window   the unlock window the weighted bids are drawn into
//   intake   the deliver-down side: a decoded article becomes a delivery
//   sample   the first article of every member, and the bootstrap of sets
//   phase    where the job stands, read off the tree
//   seams    the pipeline's hooks into all of the above

pub(crate) mod intake;
pub(crate) mod phase;
pub(crate) mod sample;
mod seams;
pub(crate) mod source;
pub(crate) mod window;

#[cfg(test)]
mod tests;

use weaver_graph::{ByteRange, NodeId, Pull, Purpose, Seek, Segment, VolumeIndex, Weight};

use self::intake::{Checksums, Decoded};
use self::source::{ArticleMap, Class};
use self::window::{Bid, Window};
use crate::pipeline::graph::namespace::{Arrival, Segments};

// A seek that reached the root, with the object that asked it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Ask {
    pub(crate) seek: Seek,
    pub(crate) bidder: NodeId,
    pub(crate) in_order: bool,
}

#[derive(Debug)]
pub(crate) struct ArticleRoot {
    pub(crate) map: ArticleMap,
    pub(crate) window: Window,
    checksums: Checksums,
}

impl ArticleRoot {
    // Each volume's articles: ordinal and declared encoded size.
    pub(crate) fn new(volumes: &[Vec<(u32, u32)>]) -> Self {
        Self {
            map: ArticleMap::new(volumes),
            window: Window::default(),
            checksums: Checksums::default(),
        }
    }

    // A decoded article: record where it landed and what it was, and hand
    // back the delivery for the namespace.
    pub(crate) fn receive(&mut self, decoded: Decoded) -> Arrival {
        let range = decoded.range();
        self.map.arrived(decoded.volume, decoded.ordinal, range);
        if let Some(length) = decoded.length {
            self.map.set_length(decoded.volume, length);
        }
        if decoded.positioned {
            self.checksums.record(decoded.volume, &decoded.segments);
        } else {
            // A position inferred from order is no position: its checksums
            // could be credited to the wrong bytes.
            self.map.set_ordered_only(decoded.volume);
        }
        decoded.into_arrival()
    }

    // Answer one round of seeks. A recovery seek is promoted; an unweighted
    // data seek is critical; a weighted one is a bid, and only bids the
    // window draws move anything. Bids already in the window (the
    // bootstrap's) take part in the same draw.
    pub(crate) fn answer(
        &mut self,
        asks: &[Ask],
        queued: impl FnMut(VolumeIndex) -> bool,
    ) -> Vec<Pull> {
        for ask in asks {
            if is_bid(ask.seek) {
                self.window.bid(Bid {
                    volume: ask.seek.volume,
                    range: ask.seek.range,
                    weight: ask.seek.weight,
                    bidder: ask.bidder,
                    in_order: ask.in_order,
                });
            }
        }
        let drawn = self.window.draw(queued);
        let answers = asks
            .iter()
            .map(|ask| {
                let class = match (ask.seek.purpose, is_bid(ask.seek)) {
                    (Purpose::Recovery, _) => Some(Class::Recovery),
                    (Purpose::Data, false) => Some(Class::Critical),
                    (Purpose::Data, true) => self.window.rank(ask.seek.volume).map(Class::Window),
                };
                self.map.seek(ask.seek.volume, ask.seek.range, class)
            })
            .collect();
        // Bids no seek of this round carried still move their articles.
        for (bid, rank) in drawn {
            self.map
                .seek(bid.volume, bid.range, Some(Class::Window(rank)));
        }
        answers
    }

    // The ranges of lost articles that can now be marked damaged.
    pub(crate) fn damage(&mut self) -> Vec<(VolumeIndex, ByteRange)> {
        let mut out = Vec::new();
        self.map.take_damage(&mut out);
        out
    }
}

// The block-aligned checksums of the bytes the root delivered, for the
// namespace write path, which calls this with every delivery it feeds to a
// recovery object. Root coordinates: member M is volume M.
impl Segments for ArticleRoot {
    fn segments_for(
        &self,
        node: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        out: &mut Vec<Segment>,
    ) {
        self.checksums.segments_for(node, volume, range, out);
    }
}

fn is_bid(seek: Seek) -> bool {
    seek.purpose == Purpose::Data && seek.weight > Weight::FIRST
}
