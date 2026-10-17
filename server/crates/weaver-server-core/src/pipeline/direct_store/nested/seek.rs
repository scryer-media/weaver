//! Seeked reads through the nested graph.
//!
//! Every archive in the graph reads one stream, and every stream is carried by
//! the stream above it, up to the source the job downloads. A node that needs
//! bytes ahead of the in-order flow (a 7z container's map, say) asks its parent
//! for a byte range. A stored archive above it maps the range onto its own
//! volumes and asks its own parent, until the source pulls the bytes forward.
//! A stream that only yields bytes in order (a compressed member, or an
//! archive an engine reads) answers no, and the node that asked is refused
//! with that reason.
//!
//! A stored archive that has not read the headers placing the range yet asks
//! for those headers instead: the front of the volume that would place it,
//! which is just another seek of its own input, carried up the same way.
//!
//! Only bytes cross this boundary: a source maps the range onto whatever it
//! fetches, and nothing here knows what that is.

use std::ops::Range;

use super::super::router::{HeaderProbe, MemberExtent, sevenz};
use super::*;

// How far back from its end a one-volume container whose start header is
// unread pulls its tail. Its map is its file list, and a megabyte of map
// names tens of thousands of files; past that the tail is payload, and
// pulling it backwards would only hold it ahead of the set's room until the
// start header arrives and places the map exactly.
const TAIL_WALK_BYTES: u64 = 1 << 20;

// A byte range of one volume of a stream. Half-open; an end of `u64::MAX`
// runs to the volume's end, wherever that turns out to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Seek {
    pub(crate) volume: u32,
    pub(crate) range: Range<u64>,
}

impl Seek {
    // The one byte at `offset`. A source fetches whole units, so asking for
    // one byte pulls the unit that holds it.
    pub(crate) fn byte(volume: u32, offset: u64) -> Self {
        Self {
            volume,
            range: offset..offset.saturating_add(1),
        }
    }

    // Whether the range shares a byte with `[start, end)`.
    pub(crate) fn overlaps(&self, (start, end): (u64, u64)) -> bool {
        start < self.range.end && self.range.start < end
    }

    // The range cut to a volume of `length` bytes, or `None` when nothing of
    // it is left. An unknown length leaves the range as asked.
    fn clamped(&self, length: Option<u64>) -> Option<Range<u64>> {
        let end = length.map_or(self.range.end, |length| self.range.end.min(length));
        (self.range.start < end).then_some(self.range.start..end)
    }

    // The part of a member's `range` one extent carries, as that part of the
    // member and as the seek of the volume the extent lies in.
    fn through(
        range: &Range<u64>,
        volume: u32,
        extent: &MemberExtent,
    ) -> Option<(Range<u64>, Self)> {
        let start = range.start.max(extent.logical_offset);
        let end = range
            .end
            .min(extent.logical_offset.saturating_add(extent.len));
        (start < end).then(|| {
            (
                start..end,
                Self {
                    volume,
                    range: extent.physical_offset + (start - extent.logical_offset)
                        ..extent.physical_offset + (end - extent.logical_offset),
                },
            )
        })
    }
}

// Where a stored archive's volumes carry `range` of one member. What the
// layout cannot place yet becomes an ask for the front of the volume whose
// headers would place it; while no volume could, nothing is asked, and the
// range is asked again next time.
pub(super) fn member_seeks(router: &DirectSetRouter, member: u32, range: Range<u64>) -> Vec<Seek> {
    let end = router
        .member_declared_length(member)
        .map_or(range.end, |length| range.end.min(length));
    let range = range.start..end;
    if range.is_empty() {
        return Vec::new();
    }
    let mut placed = ByteRanges::new();
    let mut seeks = Vec::new();
    for (volume, extent) in router.member_layout_extents(member) {
        if let Some((part, seek)) = Seek::through(&range, volume, &extent) {
            placed.insert(part.start, part.end - part.start);
            seeks.push(seek);
        }
    }
    // One header at a time: once it is read the next ask starts from what
    // it placed.
    seeks.extend(
        placed
            .missing(range.start, range.end - range.start)
            .first()
            .and_then(|&(start, _)| router.member_header_volume(member, start))
            .map(|volume| Seek::byte(volume, 0)),
    );
    seeks
}

// What a source did with a seek it could act on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Pull {
    // The units holding the range are ahead of the in-order flow, or already
    // on their way; this many changed place for the ask.
    Moved(usize),
    // Every unit holding the range is lost at the source and will not
    // arrive, in order or out of it. Only repair can still deliver the
    // bytes, so the asker stays routed and keeps wanting them.
    Lost,
}

// Whatever delivers the root's volumes, which it can deliver out of order:
// a graph whose root has no such source refuses its askers itself.
pub(crate) trait SeekableSource {
    // Moves the units holding `seek` ahead of the in-order flow.
    fn pull(&mut self, seek: &Seek) -> Pull;
}

impl NodeInput {
    // Every byte of the input that has arrived, routed or raw.
    fn arrived(&self) -> ByteRanges {
        let mut arrived = self.delivered.clone();
        for &(start, end) in self.raw_placed.ranges() {
            arrived.insert(start, end - start);
        }
        arrived
    }

    // The first byte of the input not yet arrived.
    fn frontier(&self) -> u64 {
        self.arrived().contiguous_from_zero()
    }

    // Whether every byte of the input has arrived.
    fn whole(&self) -> bool {
        self.length.is_some_and(|length| self.frontier() >= length)
    }
}

impl NestedNode {
    // What the node needs ahead of the in-order flow to learn its layout, in
    // its own input coordinates.
    //
    // A RAR set reads each volume's headers from that volume's own front, so
    // it never seeks: its bytes in order are all it needs. A 7z container
    // reads its start header at the front, which is where the flow starts,
    // and its map at the very end, which is the one thing it asks for.
    fn wants(&self) -> Vec<Seek> {
        let HeaderProbe::Container { front, tail, map } = self.router.header_probe() else {
            return Vec::new();
        };
        // The start header placed the map exactly: ask for all of it that
        // has not arrived, at once.
        if !map.is_empty() {
            return map
                .into_iter()
                .flat_map(|(volume, range)| {
                    let Some(input) = self.inputs.get(&volume) else {
                        return Vec::new();
                    };
                    let Some(range) = (Seek { volume, range }).clamped(input.length) else {
                        return Vec::new();
                    };
                    input
                        .arrived()
                        .missing(range.start, range.end - range.start)
                        .into_iter()
                        .map(|(start, end)| Seek {
                            volume,
                            range: start..end,
                        })
                        .collect()
                })
                .collect();
        }
        // The start header is not readable yet, so nothing places the map.
        // A one-volume container of known length still ends where its volume
        // does: ask for its last byte still missing, which walks backwards
        // from the end one unit at a time as each lands, alongside the front
        // the flow is bringing, as far back as a map runs.
        //
        // Not before the signature header has arrived: a name says the member
        // is a container, only its magic proves it. Asking on the name alone
        // would have a parent that cannot seek refuse a member that is no
        // archive at all, when reading its front tells that right away.
        let (Some(volume), None) = (front, tail) else {
            return Vec::new();
        };
        if !self.roster_closed || self.inputs.len() != 1 {
            return Vec::new();
        }
        self.inputs
            .get(&volume)
            .and_then(|input| {
                let length = input.length?;
                if input.frontier() < sevenz::SIGNATURE_HEADER_LEN {
                    return None;
                }
                let &(_, end) = input.arrived().missing(0, length).last()?;
                (length - end < TAIL_WALK_BYTES).then(|| Seek::byte(volume, end - 1))
            })
            .into_iter()
            .collect()
    }

    // Whether `seek` starts at the next byte the parent's in-order flow
    // hands the node anyway: the frontier of its earliest input not yet
    // whole. A later input's front waits behind every byte before it.
    fn at_frontier(&self, seek: &Seek) -> bool {
        self.inputs
            .iter()
            .find(|(_, input)| !input.whole())
            .is_some_and(|(&volume, input)| {
                volume == seek.volume && seek.range.start == input.frontier()
            })
    }
}

impl NestedGraph {
    // Translates a seek of `node`'s input into the root's volumes.
    //
    // Each hop asks the node's parent for its member's range. A stored parent
    // maps the range onto its own volumes, or asks for the headers that would
    // place it; one that is not routed answers
    // [`DemotionReason::UnseekableParent`], and so does a root that cannot
    // seek. A header ask is carried on like any other range: its bytes are
    // what the asker's ancestors need first.
    pub(crate) fn lift(
        &self,
        root: &dyn RoutingNamespace,
        node: u32,
        seek: Seek,
    ) -> Result<Vec<Seek>, DemotionReason> {
        let mut seeks = vec![seek];
        let mut owner = node;
        while owner != ROOT_NODE {
            let child = self.checked_node(owner)?;
            let parent = self.seekable(root, child.parent)?;
            let mut next = Vec::new();
            for seek in seeks {
                let input = child
                    .inputs
                    .get(&seek.volume)
                    .ok_or(DemotionReason::ConflictingVolumeFacts)?;
                let Some(range) = seek.clamped(input.length) else {
                    continue;
                };
                next.extend(parent.lift_seek(input.parent_member, range)?);
            }
            seeks = next;
            owner = child.parent;
        }
        Ok(seeks)
    }

    fn seekable<'a>(
        &'a self,
        root: &'a dyn RoutingNamespace,
        id: u32,
    ) -> Result<&'a dyn RoutingNamespace, DemotionReason> {
        if id != ROOT_NODE && self.node(id).mode != NodeMode::Direct {
            return Err(DemotionReason::UnseekableParent);
        }
        Ok(self.catalog(root, id))
    }

    // Records the position an engine reading `node` is blocked on, as
    // `(volume, offset)`, or that it is blocked on nothing.
    pub(crate) fn note_engine_want(&mut self, node: u32, want: Option<(u32, u64)>) {
        match want {
            Some((volume, offset)) => {
                self.engine_wants.insert(node, Seek::byte(volume, offset));
            }
            None => {
                self.engine_wants.remove(&node);
            }
        }
    }

    // Node `id`'s wants, each lifted into the root's volumes or refused with
    // the reason it cannot be.
    fn lifted_wants(
        &self,
        root: &dyn RoutingNamespace,
        id: u32,
    ) -> Vec<Result<Vec<Seek>, DemotionReason>> {
        let node = self.node(id);
        let wants = match node.mode {
            NodeMode::Direct => node.wants(),
            NodeMode::Engine => self.engine_wants.get(&id).cloned().into_iter().collect(),
            NodeMode::MaterializationPending(_) | NodeMode::Materialized => Vec::new(),
        };
        wants
            .into_iter()
            // The next byte in order is coming anyway, and an engine is
            // blocked on it nearly always: pulling it would rescan the queue
            // on every placement for nothing.
            .filter(|want| !node.at_frontier(want))
            .map(|want| self.lift(root, id, want))
            .collect()
    }

    // Node `id`'s wants that can be lifted, in the root's volumes.
    fn liftable_wants(&self, root: &dyn RoutingNamespace, id: u32) -> Vec<Seek> {
        self.lifted_wants(root, id)
            .into_iter()
            .filter_map(Result::ok)
            .flatten()
            .collect()
    }

    // Refuses every routed node whose wants cannot be lifted, which turns it
    // into an engine that waits for the bytes in order. An engine's own
    // unliftable want changes nothing: it already waits.
    pub(crate) fn refuse_unseekable(&mut self, root: &dyn RoutingNamespace) {
        for id in 1..=self.nodes.len() as u32 {
            if self.node(id).mode != NodeMode::Direct {
                continue;
            }
            if let Some(reason) = self
                .lifted_wants(root, id)
                .into_iter()
                .find_map(Result::err)
            {
                self.node_mut(id).refuse(reason);
            }
        }
    }

    // Every node's wants that can be lifted, in the root's volumes. Changes
    // nothing: a want that cannot be lifted is left out here, and refused by
    // [`Self::refuse_unseekable`].
    pub(crate) fn seeks(&self, root: &dyn RoutingNamespace) -> Vec<Seek> {
        (1..=self.nodes.len() as u32)
            .flat_map(|id| self.liftable_wants(root, id))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_seek_is_cut_to_its_volume_and_mapped_through_an_extent() {
        let seek = Seek {
            volume: 0,
            range: 10..u64::MAX,
        };
        assert_eq!(seek.clamped(Some(40)), Some(10..40));
        assert_eq!(seek.clamped(None), Some(10..u64::MAX));
        assert_eq!(seek.clamped(Some(10)), None);
        let extent = MemberExtent {
            member_id: 3,
            physical_offset: 100,
            logical_offset: 30,
            len: 20,
        };
        assert_eq!(
            Seek::through(&(10..40), 2, &extent),
            Some((
                30..40,
                Seek {
                    volume: 2,
                    range: 100..110,
                }
            ))
        );
        assert_eq!(Seek::through(&(10..30), 2, &extent), None);
        assert!(Seek::byte(0, 7).overlaps((0, 8)));
        assert!(!Seek::byte(0, 8).overlaps((0, 8)));
    }

    fn router(format: SetFormat) -> DirectSetRouter {
        DirectSetRouter::new(DirectSetPlan {
            set_name: "child".into(),
            format,
            volumes: BTreeMap::from([(0, 0)]),
            files: HashMap::from([(0, 0)]),
            identity: Some(IdentityPlanFacts {
                kind: format.nested_identity(),
                expected_volumes: Some(1),
                discriminator: 1,
            }),
            unposted: Default::default(),
            working_dir: PathBuf::from("working"),
            destination_dir: PathBuf::from("destination"),
        })
    }

    fn input(parent_member: u32) -> NodeInput {
        NodeInput {
            parent_member,
            length: Some(4_096),
            delivered: ByteRanges::new(),
            placed: ByteRanges::new(),
            envelope_placed: ByteRanges::new(),
            raw_placed: ByteRanges::new(),
            confirmed: false,
        }
    }

    fn node(parent: u32, format: SetFormat, mode: NodeMode) -> NestedNode {
        NestedNode {
            parent,
            depth: parent + 2,
            generation: 1,
            backing_lease: std::sync::Arc::new(()),
            router: router(format),
            inputs: BTreeMap::from([(0, input(0))]),
            mode,
            roster_closed: true,
            members_closed: false,
            discovered_revision: None,
        }
    }

    // What discovery builds under a RAR root: a stored set it found in a
    // root member, and an archive that set's own member names. Discovery only
    // looks inside a routed set, so a refused parent is one refused after its
    // child was found.
    fn discovered(parent: (SetFormat, NodeMode), child: (SetFormat, NodeMode)) -> NestedGraph {
        let mut graph = NestedGraph::default();
        graph.nodes.push(node(ROOT_NODE, parent.0, parent.1));
        graph.nodes.push(node(1, child.0, child.1));
        graph
    }

    // The flow brings a node's inputs one after another, so only the
    // earliest one not yet whole has a next byte that is coming anyway.
    #[test]
    fn only_the_earliest_unfinished_input_has_its_next_byte_coming() {
        let mut child = node(ROOT_NODE, SetFormat::SevenZip, NodeMode::Direct);
        child.inputs.insert(1, input(1));
        let first = child.inputs.get_mut(&0).unwrap();
        first.delivered.insert(0, 10);
        assert!(child.at_frontier(&Seek::byte(0, 10)));
        assert!(!child.at_frontier(&Seek::byte(1, 0)));
        let first = child.inputs.get_mut(&0).unwrap();
        first.delivered.insert(10, 4_086);
        assert!(child.at_frontier(&Seek::byte(1, 0)));
    }

    // The walk back from a container's end stops once a map's worth of the
    // tail has arrived: what lies before that is payload, and the start
    // header will place the map exactly when the flow reaches it.
    #[test]
    fn a_tail_walk_stops_where_no_map_runs() {
        let mut child = node(ROOT_NODE, SetFormat::SevenZip, NodeMode::Direct);
        let length = 4 * TAIL_WALK_BYTES;
        let input = child.inputs.get_mut(&0).unwrap();
        input.length = Some(length);
        input
            .delivered
            .insert(length - TAIL_WALK_BYTES + 1, TAIL_WALK_BYTES - 1);
        // Nothing is asked until the signature header has arrived.
        assert!(child.wants().is_empty());
        let input = child.inputs.get_mut(&0).unwrap();
        input.delivered.insert(0, sevenz::SIGNATURE_HEADER_LEN);
        assert_eq!(child.wants(), vec![Seek::byte(0, length - TAIL_WALK_BYTES)]);
        let input = child.inputs.get_mut(&0).unwrap();
        input.delivered.insert(length - TAIL_WALK_BYTES, 1);
        assert!(child.wants().is_empty());
    }

    #[test]
    fn a_rar_set_under_a_routed_rar_set_asks_for_nothing_and_stays_routed() {
        let root = router(SetFormat::Rar);
        let mut graph = discovered(
            (SetFormat::Rar, NodeMode::Direct),
            (SetFormat::Rar, NodeMode::Direct),
        );
        assert!(graph.node(2).wants().is_empty());
        assert!(graph.seeks(&root).is_empty());
        graph.refuse_unseekable(&root);
        assert_eq!(graph.node(2).mode, NodeMode::Direct);
        assert_eq!(graph.node(2).router.demotion(), None);
    }

    #[test]
    fn a_routed_container_under_a_refused_parent_is_refused_with_why() {
        let root = router(SetFormat::Rar);
        let mut graph = discovered(
            (SetFormat::Rar, NodeMode::Engine),
            (SetFormat::SevenZip, NodeMode::Direct),
        );
        // The signature header has arrived but nothing has read the start
        // header, so the one-volume container asks for its last byte.
        graph
            .node_mut(2)
            .inputs
            .get_mut(&0)
            .unwrap()
            .delivered
            .insert(0, sevenz::SIGNATURE_HEADER_LEN);
        assert_eq!(graph.node(2).wants(), vec![Seek::byte(0, 4_095)]);
        assert!(graph.seeks(&root).is_empty());
        assert_eq!(graph.node(2).mode, NodeMode::Direct);
        graph.refuse_unseekable(&root);
        assert_eq!(graph.node(2).mode, NodeMode::Engine);
        assert_eq!(
            graph.node(2).router.demotion(),
            Some(DemotionReason::UnseekableParent)
        );
    }

    #[test]
    fn an_engine_want_under_a_refused_parent_is_dropped_without_a_verdict() {
        let root = router(SetFormat::Rar);
        let mut graph = discovered(
            (SetFormat::Rar, NodeMode::Engine),
            (SetFormat::SevenZip, NodeMode::Engine),
        );
        graph.note_engine_want(2, Some((0, 4_000)));
        assert!(graph.seeks(&root).is_empty());
        graph.refuse_unseekable(&root);
        assert_eq!(graph.node(2).mode, NodeMode::Engine);
        assert_eq!(graph.node(2).router.demotion(), None);
        graph.note_engine_want(2, None);
        assert!(graph.engine_wants.is_empty());
    }
}
