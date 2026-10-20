// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The roots: the one object of a job that fetches from outside. The article
// root's volumes are the post's files, in name-table order, so root volume N
// is root member N. It is the article object, so this is the one place in
// the graph layer that knows a post is made of articles. The piece root
// (`pieces`) is the other: its volumes are a torrent's files, fetched in
// pieces.
//
//   source   the seek-up side: a seek becomes a new class for the articles
//            that carry the range, answered Moved, Lost or Unseekable
//   window   the unlock window the weighted bids are drawn into
//   intake   the deliver-down side: a decoded article becomes a delivery
//   sample   the first article of every member, and the bootstrap of sets
//   phase    where the job stands, read off the tree
//   seams    the pipeline's hooks into all of the above

pub(crate) mod facts;
pub(crate) mod intake;
pub(crate) mod phase;
pub(crate) mod pieces;
pub(crate) mod sample;
mod seams;
pub(crate) mod source;
pub(crate) mod window;

#[cfg(test)]
mod tests;

use serde::Serialize;
use serde::de::DeserializeOwned;
use weaver_graph::{
    ByteRange, NodeId, Part, Pull, Purpose, Seek, Segment, Tree, VolumeIndex, Weight,
};

use self::facts::{PostFacts, RootFactsError};
use self::intake::{Checksums, Decoded};
use self::source::{ArticleMap, Class, Move};
use self::window::{Bid, Window};
use crate::pipeline::graph::job::RootFile;
use crate::pipeline::graph::namespace::{Arrival, Segments};

// A seek that reached the root, with the object that asked it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Ask {
    pub(crate) seek: Seek,
    pub(crate) bidder: NodeId,
    pub(crate) in_order: bool,
}

// What a graph job asks of its root, the one object that fetches from
// outside. The root's volumes are the root namespace's members, volume N
// being member N, and each volume is fetched in units the root numbers. How
// a unit is fetched, and what it carries, is the root's own.
pub(crate) trait Root: Segments {
    // One fetched unit, as the fetch path placed it.
    type Intake;
    // How soon the root fetches a unit.
    type Class;
    // A change of class the fetch queue has to apply.
    type Move;
    // What a checkpoint keeps of the root, under the root's name and the
    // version of its facts, so a restore reads them only into its own kind.
    type Facts: Serialize + DeserializeOwned;
    // What the root keeps between encodings of its facts, so a part that
    // did not change is not encoded again.
    type Kept: Default + Send;
    const NAME: &'static str;
    const FACTS_VERSION: u16;

    fn facts(&self) -> Self::Facts;

    // The facts' bytes and the parts they name, encoding again only what
    // changed since `kept` last encoded. Facts kept whole name no parts.
    fn encode(&self, _kept: &mut Self::Kept) -> Result<(Vec<u8>, Vec<Part>), RootFactsError> {
        rmp_serde::to_vec(&self.facts())
            .map(|bytes| (bytes, Vec::new()))
            .map_err(|error| RootFactsError::Encode(error.to_string()))
    }

    // The facts back from their bytes and parts.
    fn decode(bytes: &[u8], parts: &[Part]) -> Result<Self::Facts, RootFactsError> {
        if !parts.is_empty() {
            return Err(RootFactsError::Decode(
                "parts of facts kept whole".to_owned(),
            ));
        }
        rmp_serde::from_slice(bytes).map_err(|error| RootFactsError::Decode(error.to_string()))
    }

    // Units the queues do not hold were fetched by an earlier run.
    fn arrived_before(&mut self, queued: impl FnMut(VolumeIndex, u32) -> bool);
    // Some volume's opening bytes have yet to arrive.
    fn any_head_waiting(&self) -> bool;
    // The class the root fetches a unit under now.
    fn class(&self, volume: VolumeIndex, unit: u32) -> Option<Self::Class>;
    // Raise what samples the post ahead of the payload.
    fn sample(&mut self);
    // The class changes since the last call, soonest first.
    fn take_moves(&mut self) -> Vec<Self::Move>;
    // A fetched unit: the delivery for the namespace, and the volume's
    // length when the unit declared it.
    fn receive(&mut self, intake: Self::Intake) -> (Arrival, Option<u64>);
    // Every server failed the unit; nothing will fetch it again.
    fn lost(&mut self, volume: VolumeIndex, unit: u32);
    // Whether the volume still has units to come.
    fn volume_waiting(&self, volume: VolumeIndex) -> bool;
    // Whether the volume has units the queue fetches unasked still to come.
    // A unit fetched only when something asks for it holds nothing back.
    fn volume_to_come(&self, volume: VolumeIndex) -> bool {
        self.volume_waiting(volume)
    }
    // The units the queue holds parked when the job is installed: fetched
    // only once something asks for them.
    fn parked(&mut self, _held: impl FnMut(VolumeIndex, u32) -> bool) {}
    // What the root asks for itself before a round of seeks, from what the
    // tree knows now.
    fn bootstrap(&mut self, tree: &Tree);
    // Answer one round of seeks that reached the root, one pull per ask.
    fn answer(&mut self, asks: &[Ask], queued: impl FnMut(VolumeIndex) -> bool) -> Vec<Pull>;
    // The ranges of lost units that can now be marked damaged.
    fn damage(&mut self) -> Vec<(VolumeIndex, ByteRange)>;
    // How many volumes the root fetches.
    fn volume_count(&self) -> usize;
    // The units of a volume in hand, with the range each was placed at.
    fn arrived_ranges(&self, volume: VolumeIndex) -> Vec<(u32, ByteRange)>;
    // Units an earlier run fetched whose placement this run never saw.
    fn unseen(&self) -> Vec<(VolumeIndex, u32)>;
    // The unit is no longer in hand: it waits to be fetched again.
    fn unarrive(&mut self, volume: VolumeIndex, unit: u32);
    // Units this run does not fetch: in hand, or lost for good.
    fn settled(&self) -> Vec<(VolumeIndex, u32)>;
    // Whether bytes read back still match what the root was handed.
    fn matches(&self, volume: VolumeIndex, range: ByteRange, bytes: &[u8]) -> bool;
}

#[derive(Debug)]
pub(crate) struct ArticleRoot {
    pub(crate) map: ArticleMap,
    pub(crate) window: Window,
    checksums: Checksums,
    // Each file's name as the post lists it. A root member is the post's
    // file of its number whatever a recovery set renames it to, so this,
    // not the member's name, is what says a restore has the right post.
    posted: Vec<String>,
}

impl ArticleRoot {
    pub(crate) fn new(files: &[RootFile]) -> Self {
        let volumes: Vec<_> = files.iter().map(|file| file.articles.clone()).collect();
        Self {
            map: ArticleMap::new(&volumes),
            window: Window::default(),
            checksums: Checksums::default(),
            posted: files.iter().map(|file| file.name.clone()).collect(),
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

    // Answer one round of seeks: each moves its articles to the class the
    // round gives it.
    pub(crate) fn answer(
        &mut self,
        asks: &[Ask],
        queued: impl FnMut(VolumeIndex) -> bool,
    ) -> Vec<Pull> {
        let (classes, drawn) = round(&mut self.window, asks, queued);
        let answers = asks
            .iter()
            .zip(classes)
            .map(|(ask, asked)| {
                let class = asked.map(|asked| match asked {
                    Asked::Critical => Class::Critical,
                    Asked::Recovery => Class::Recovery,
                    Asked::Window(rank) => Class::Window(rank),
                });
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

impl Root for ArticleRoot {
    type Intake = Decoded;
    type Class = Class;
    type Move = Move;
    type Facts = PostFacts;
    type Kept = facts::Encoded;
    const NAME: &'static str = "articles";
    const FACTS_VERSION: u16 = PostFacts::VERSION;

    fn facts(&self) -> PostFacts {
        self.post_facts()
    }

    fn encode(&self, kept: &mut facts::Encoded) -> Result<(Vec<u8>, Vec<Part>), RootFactsError> {
        facts::encode_post(kept, &self.post_facts())
    }

    fn decode(bytes: &[u8], parts: &[Part]) -> Result<PostFacts, RootFactsError> {
        facts::decode_post(bytes, parts)
    }

    fn arrived_before(&mut self, queued: impl FnMut(VolumeIndex, u32) -> bool) {
        self.map.arrived_before(queued);
    }

    fn any_head_waiting(&self) -> bool {
        self.map.any_head_waiting()
    }

    fn class(&self, volume: VolumeIndex, unit: u32) -> Option<Class> {
        self.map.class(volume, unit)
    }

    fn sample(&mut self) {
        self.map.sample();
    }

    fn take_moves(&mut self) -> Vec<Move> {
        self.map.take_moves()
    }

    fn receive(&mut self, intake: Decoded) -> (Arrival, Option<u64>) {
        let length = intake.length;
        (ArticleRoot::receive(self, intake), length)
    }

    fn lost(&mut self, volume: VolumeIndex, unit: u32) {
        self.map.lost(volume, unit);
    }

    fn volume_waiting(&self, volume: VolumeIndex) -> bool {
        self.map.volume_waiting(volume)
    }

    fn volume_to_come(&self, volume: VolumeIndex) -> bool {
        self.map.volume_to_come(volume)
    }

    fn parked(&mut self, held: impl FnMut(VolumeIndex, u32) -> bool) {
        self.map.parked(held);
    }

    fn bootstrap(&mut self, tree: &Tree) {
        let map = &self.map;
        sample::bootstrap(tree, |volume| map.volume_waiting(volume), &mut self.window);
        sample::fronts(tree, &mut self.map);
    }

    fn answer(&mut self, asks: &[Ask], queued: impl FnMut(VolumeIndex) -> bool) -> Vec<Pull> {
        ArticleRoot::answer(self, asks, queued)
    }

    fn damage(&mut self) -> Vec<(VolumeIndex, ByteRange)> {
        ArticleRoot::damage(self)
    }

    fn volume_count(&self) -> usize {
        self.map.volume_count()
    }

    fn arrived_ranges(&self, volume: VolumeIndex) -> Vec<(u32, ByteRange)> {
        self.map.arrived_ranges(volume)
    }

    fn unseen(&self) -> Vec<(VolumeIndex, u32)> {
        self.map.unseen()
    }

    fn unarrive(&mut self, volume: VolumeIndex, unit: u32) {
        self.map.unarrive(volume, unit);
    }

    fn settled(&self) -> Vec<(VolumeIndex, u32)> {
        self.map.settled()
    }

    fn matches(&self, volume: VolumeIndex, range: ByteRange, bytes: &[u8]) -> bool {
        ArticleRoot::matches(self, volume, range, bytes)
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

// How soon a round fetches what one ask carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Asked {
    Critical,
    Recovery,
    Window(u32),
}

// One round of seeks, for every root alike. A recovery seek is promoted; an
// unweighted data seek is critical; a weighted one is a bid, and only bids
// the window draws move anything, so an undrawn bid's ask has no class.
// Bids already in the window (the bootstrap's) take part in the same draw,
// and every bid drawn comes back with its rank, so the ones no seek of this
// round carried still move what they cover.
pub(crate) fn round(
    window: &mut Window,
    asks: &[Ask],
    queued: impl FnMut(VolumeIndex) -> bool,
) -> (Vec<Option<Asked>>, Vec<(Bid, u32)>) {
    for ask in asks {
        if is_bid(ask.seek) {
            window.bid(Bid {
                volume: ask.seek.volume,
                range: ask.seek.range,
                weight: ask.seek.weight,
                bidder: ask.bidder,
                in_order: ask.in_order,
            });
        }
    }
    let drawn = window.draw(queued);
    let classes = asks
        .iter()
        .map(|ask| match (ask.seek.purpose, is_bid(ask.seek)) {
            (Purpose::Recovery, _) => Some(Asked::Recovery),
            (Purpose::Data, false) => Some(Asked::Critical),
            (Purpose::Data, true) => window.rank(ask.seek.volume).map(Asked::Window),
        })
        .collect();
    (classes, drawn)
}

fn is_bid(seek: Seek) -> bool {
    seek.purpose == Purpose::Data && seek.weight > Weight::FIRST
}
