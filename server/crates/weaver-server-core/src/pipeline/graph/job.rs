// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One graph job's runtime: its tree, its article root, the builder its
// objects come from, and the sink its bytes go to. The pipeline holds one per
// job admitted under the graph engine and hands it what the download and
// decode paths learn; nothing else of the pipeline reaches into the tree.

use std::collections::BTreeMap;

use weaver_graph::{
    Builder, Effect, Kind, MemberId, NodeId, Object, ObjectId, Pull, Seek, SeekUp, Tree,
    VolumeIndex,
};

use super::Engine;
use super::namespace::{Arrival, Segments};
use super::root::intake::Decoded;
use super::root::phase::{self, Activity, RootPhase};
use super::root::sample::{self, reads_in_order};
use super::root::source::Move;
use super::root::{ArticleRoot, Ask};

#[derive(Debug, thiserror::Error)]
pub(crate) enum GraphError {
    #[error("the namespace write path failed")]
    Write(#[source] Box<dyn std::error::Error + Send + Sync>),
}

// Where the root's bytes and the tree's effects go: the namespace write path.
// Kept behind a trait so the root never depends on how a namespace writes.
pub(crate) trait Sink: Send + Sync {
    // Bytes of a member, through the write path. The effects it leaves for
    // the job's owner come back in `out`.
    fn deliver(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        arrival: Arrival,
        out: &mut Vec<Effect>,
    ) -> Result<(), GraphError>;

    // Effects the tree raised outside a delivery: an answered seek, a range
    // marked damaged. The ones left for the owner come back in `out`.
    fn apply(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        effects: Vec<Effect>,
        out: &mut Vec<Effect>,
    ) -> Result<(), GraphError>;
}

// The builder before any format's object exists: every member is a leaf.
pub(crate) struct NoObjects;

impl Builder for NoObjects {
    fn build(&mut self, _: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        None
    }
}

// The sink while the classic path still writes every byte: the tree records
// coverage and routes, and leaf writes are left to the classic path.
pub(crate) struct Shadow;

impl Sink for Shadow {
    fn deliver(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        _: &dyn Segments,
        arrival: Arrival,
        out: &mut Vec<Effect>,
    ) -> Result<(), GraphError> {
        let mut effects = Vec::new();
        tree.deliver(
            arrival.node,
            arrival.member,
            arrival.offset,
            arrival.bytes,
            arrival.provenance,
            builder,
            &mut effects,
        );
        self.apply(tree, builder, effects, out)
    }

    fn apply(
        &mut self,
        _: &mut Tree,
        _: &mut dyn Builder,
        effects: Vec<Effect>,
        out: &mut Vec<Effect>,
    ) -> Result<(), GraphError> {
        out.extend(
            effects
                .into_iter()
                .filter(|effect| !matches!(effect, Effect::WriteLeaf { .. })),
        );
        Ok(())
    }
}

pub(crate) struct GraphJob {
    tree: Tree,
    root: ArticleRoot,
    builder: Box<dyn Builder + Send + Sync>,
    sink: Box<dyn Sink>,
    // What each bound node is, from the tree's own announcements.
    kinds: BTreeMap<NodeId, Kind>,
    activity: Activity,
    // A recovery object has said what the members are called.
    names_trusted: bool,
    // The phase last reported, so only a change is reported.
    reported: Option<RootPhase>,
}

// A root member as the post lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RootFile {
    pub(crate) name: String,
    // Each article's ordinal and declared encoded size.
    pub(crate) articles: Vec<(u32, u32)>,
}

impl GraphJob {
    // The runtime a job admitted under `engine` gets, if any: a classic job
    // has none.
    pub(crate) fn for_engine(engine: Engine, files: &[RootFile]) -> Option<Self> {
        (engine == Engine::Graph).then(|| Self::new(files, Box::new(NoObjects), Box::new(Shadow)))
    }

    pub(crate) fn new(
        files: &[RootFile],
        builder: Box<dyn Builder + Send + Sync>,
        sink: Box<dyn Sink>,
    ) -> Self {
        let mut tree = Tree::new();
        for file in files {
            tree.announce_root(&file.name, None);
        }
        let volumes: Vec<_> = files.iter().map(|file| file.articles.clone()).collect();
        Self {
            tree,
            root: ArticleRoot::new(&volumes),
            builder,
            sink,
            kinds: BTreeMap::new(),
            activity: Activity::Idle,
            names_trusted: false,
            reported: None,
        }
    }

    pub(crate) fn tree(&self) -> &Tree {
        &self.tree
    }

    pub(crate) fn root(&self) -> &ArticleRoot {
        &self.root
    }

    // Articles the queues do not hold were fetched by an earlier run.
    pub(crate) fn arrived_before(&mut self, queued: impl FnMut(VolumeIndex, u32) -> bool) {
        self.root.map.arrived_before(queued);
    }

    // The first article of every member leads the payload.
    pub(crate) fn sample(&mut self) -> Vec<Move> {
        self.root.map.sample();
        self.root.map.take_moves()
    }

    // A recovery object bound the members' names; the sets they form can
    // be bootstrapped from now on.
    pub(crate) fn trust_names(&mut self) {
        self.names_trusted = true;
    }

    pub(crate) fn set_activity(&mut self, activity: Activity) {
        self.activity = activity;
    }

    pub(crate) fn intake(&mut self, decoded: Decoded) -> Result<(), GraphError> {
        if let Some(length) = decoded.length
            && let Some(member) = self
                .tree
                .namespace_mut(NodeId::ROOT)
                .and_then(|namespace| namespace.get_mut(MemberId(decoded.volume.0)))
            && member.length.is_none()
        {
            member.length = Some(length);
        }
        let arrival = self.root.receive(decoded);
        let mut out = Vec::new();
        self.sink.deliver(
            &mut self.tree,
            &mut *self.builder,
            &self.root,
            arrival,
            &mut out,
        )?;
        self.observe(&out);
        self.mark_damage()
    }

    // Every server failed the article; nothing will fetch it again.
    pub(crate) fn lost(&mut self, volume: VolumeIndex, ordinal: u32) -> Result<(), GraphError> {
        self.root.map.lost(volume, ordinal);
        self.mark_damage()
    }

    // Ranges of lost articles become damage in the tree as soon as they can
    // be told; whatever covers them repairs them.
    fn mark_damage(&mut self) -> Result<(), GraphError> {
        let mut effects = Vec::new();
        for (volume, range) in self.root.damage() {
            self.tree.damage(NodeId::ROOT, volume, range, &mut effects);
        }
        self.settle(effects)
    }

    // One round of seeks: every object's standing wants lifted to the root,
    // answered together so the window draws from all of them at once, and
    // the answers handed back. Returns the class changes for the queue.
    pub(crate) fn seek_round(
        &mut self,
        queued: impl FnMut(VolumeIndex) -> bool,
    ) -> Result<Vec<Move>, GraphError> {
        if self.names_trusted {
            let kinds = &self.kinds;
            sample::bootstrap(
                &self.tree,
                &self.root.map,
                |node| kinds.get(&node).copied(),
                &mut self.root.window,
            );
        }
        let mut wants = Vec::new();
        self.tree.collect_wants(&mut wants);
        let mut asks = Vec::new();
        let mut lifted = Vec::with_capacity(wants.len());
        for (node, seek) in wants {
            let start = asks.len();
            let mut recorder = Recorder {
                asks: &mut asks,
                bidder: node,
                in_order: self
                    .kinds
                    .get(&node)
                    .is_some_and(|kind| reads_in_order(*kind)),
            };
            let pull = self.tree.lift(node, seek, &mut recorder);
            lifted.push((node, seek, pull, start..asks.len()));
        }
        let answers = self.root.answer(&asks, queued);
        let mut effects = Vec::new();
        for (node, seek, pull, span) in lifted {
            let pull = answers[span]
                .iter()
                .fold(pull, |pull, answer| pull.fold(*answer));
            self.tree
                .pulled(node, seek, pull, &mut *self.builder, &mut effects);
        }
        self.settle(effects)?;
        Ok(self.root.map.take_moves())
    }

    // The phase, when it changed since the last call.
    pub(crate) fn phase_change(&mut self) -> Option<RootPhase> {
        let phase = self.phase();
        (self.reported != Some(phase)).then(|| {
            self.reported = Some(phase);
            phase
        })
    }

    pub(crate) fn phase(&self) -> RootPhase {
        RootPhase::derive(
            self.root.map.waiting(),
            self.activity,
            phase::extracting(&self.tree, &self.kinds),
        )
    }

    fn settle(&mut self, effects: Vec<Effect>) -> Result<(), GraphError> {
        if effects.is_empty() {
            return Ok(());
        }
        let mut out = Vec::new();
        self.sink
            .apply(&mut self.tree, &mut *self.builder, effects, &mut out)?;
        self.observe(&out);
        Ok(())
    }

    fn observe(&mut self, effects: &[Effect]) {
        for effect in effects {
            if let Effect::Bound { kind, child, .. } = effect {
                self.kinds.insert(*child, *kind);
            }
        }
    }
}

// Lifts a seek and keeps what reached the root, to be answered with the
// round's other seeks.
struct Recorder<'a> {
    asks: &'a mut Vec<Ask>,
    bidder: NodeId,
    in_order: bool,
}

impl SeekUp for Recorder<'_> {
    fn seek(&mut self, seek: Seek) -> Pull {
        self.asks.push(Ask {
            seek,
            bidder: self.bidder,
            in_order: self.in_order,
        });
        Pull::Moved(0)
    }
}
