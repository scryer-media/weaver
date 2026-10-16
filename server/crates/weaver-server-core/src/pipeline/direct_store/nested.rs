//! Root-owned routing state for archive members that are themselves archives.
//! Source coordinates stay local to their node. Only completed physical writes
//! are projected through ancestors into the root coverage barrier.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::PathBuf;

use super::ByteRanges;
use super::plan::{DirectSetPlan, IdentityKind, IdentityPlanFacts, SetFormat};
use super::router::{DemotionReason, DirectDestination, DirectSetRouter, RawChild, RoutedSpan};
use weaver_model::files::{FileRole, archive_base_name};

pub(crate) mod catalog;
pub(crate) mod checkpoint;
pub(crate) mod materialize;
pub(crate) mod placement;
mod views;
pub(crate) use views::RepairInventory;

use catalog::{MemberCatalog, RoutingNamespace};

pub(crate) const ROOT_NODE: u32 = 0;
pub(crate) const MAX_NODES: usize = 4_096;
pub(crate) const MAX_INPUTS: usize = 65_536;
pub(crate) const MAX_DEPTH: u32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeMode {
    Direct,
    // Old outputs must finish placement before this node can be materialized.
    MaterializationPending(DemotionReason),
    Materialized,
}

pub(crate) struct NodeInput {
    pub(crate) parent_member: u32,
    pub(crate) length: Option<u64>,
    pub(crate) delivered: ByteRanges,
    pub(crate) placed: ByteRanges,
    pub(crate) envelope_placed: ByteRanges,
    // Writes received after forwarding stopped, in original input coordinates.
    pub(crate) raw_placed: ByteRanges,
    pub(crate) confirmed: bool,
}

pub(crate) struct NestedNode {
    pub(crate) parent: u32,
    pub(crate) depth: u32,
    pub(crate) generation: u64,
    pub(crate) backing_lease: std::sync::Arc<()>,
    pub(crate) router: DirectSetRouter,
    pub(crate) inputs: BTreeMap<u32, NodeInput>,
    pub(crate) mode: NodeMode,
    // A complete known volume is not proof that no further sibling exists.
    pub(crate) roster_closed: bool,
    members_closed: bool,
    pub(crate) whole_sevenz: bool,
    discovered_revision: Option<u64>,
}

impl NestedNode {
    fn settle_inputs(&mut self, parent: &dyn MemberCatalog) -> Vec<RoutedSpan> {
        let mut emitted = Vec::new();
        if self.mode != NodeMode::Direct {
            return emitted;
        }
        for (&volume, input) in &mut self.inputs {
            if input.length.is_none() {
                input.length = parent.length(input.parent_member);
                if let Some(length) = input.length
                    && let Err(reason) = self.router.note_declared_volume_size(volume, length)
                {
                    self.mode = NodeMode::MaterializationPending(reason);
                    break;
                }
            }
            let Some(length) = input.length else { continue };
            if input.confirmed || !input.delivered.missing(0, length).is_empty() {
                continue;
            }
            input.confirmed = true;
            if let Err(reason) = self
                .router
                .note_volume_complete_into(volume, length, &mut emitted)
            {
                self.mode = NodeMode::MaterializationPending(reason);
                break;
            }
        }
        emitted
    }
}

pub(crate) struct NodeSpan {
    pub(crate) owner: u32,
    pub(crate) generation: u64,
    // A refused child's new input writes to its containing member partial.
    // This is separate from the physical sink owner and its source mapping.
    pub(crate) raw_child: Option<RawChild>,
    pub(crate) span: RoutedSpan,
}

pub(crate) type NodePlacement = super::router::PlacementReceipt;

impl NodeSpan {
    pub(crate) fn into_span(mut self) -> RoutedSpan {
        self.span.owner = super::router::SpanOwner {
            node: self.owner,
            generation: self.generation,
            raw_child: self.raw_child,
        };
        self.span
    }

    pub(crate) fn placement(&self) -> NodePlacement {
        let mut receipt = self.span.placement();
        receipt.owner = super::router::SpanOwner {
            node: self.owner,
            generation: self.generation,
            raw_child: self.raw_child,
        };
        receipt
    }
}

pub(crate) use crate::pipeline::extraction::catalog::InputRange as SourceSpan;

#[derive(Default)]
pub(crate) struct NestedGraph {
    pub(crate) nodes: Vec<NestedNode>,
    bindings: BTreeMap<(u32, u32), (u32, u32)>,
    groups: BTreeMap<(u32, String, u8), u32>,
    root_discovered_revision: Option<u64>,
    root_epoch: Option<catalog::NamespaceEpoch>,
    depth_limit: Option<u32>,
}

impl NestedGraph {
    pub(crate) fn set_namespace_depth(&mut self, depth: u32) -> Result<(), DemotionReason> {
        let limit = MAX_DEPTH
            .checked_sub(
                depth
                    .checked_sub(1)
                    .ok_or(DemotionReason::ConflictingVolumeFacts)?,
            )
            .filter(|limit| *limit != 0)
            .ok_or(DemotionReason::ConflictingVolumeFacts)?;
        if self.nodes.iter().any(|node| node.depth > limit) {
            return Err(DemotionReason::ConflictingVolumeFacts);
        }
        self.depth_limit = Some(limit);
        Ok(())
    }

    fn bind_root(&mut self, root: &dyn RoutingNamespace) -> Result<(), DemotionReason> {
        let epoch = root
            .live_epoch()
            .map_err(|_| DemotionReason::ConflictingVolumeFacts)?;
        match self.root_epoch {
            Some(expected) if expected != epoch => Err(DemotionReason::ConflictingVolumeFacts),
            Some(_) => Ok(()),
            None => {
                self.root_epoch = Some(epoch);
                Ok(())
            }
        }
    }

    fn validate_root(&self, root: &dyn RoutingNamespace) -> Result<(), DemotionReason> {
        let epoch = root
            .live_epoch()
            .map_err(|_| DemotionReason::ConflictingVolumeFacts)?;
        if self.root_epoch != Some(epoch) {
            return Err(DemotionReason::ConflictingVolumeFacts);
        }
        Ok(())
    }

    pub(crate) fn node(&self, id: u32) -> &NestedNode {
        &self.nodes[id as usize - 1]
    }

    pub(crate) fn node_mut(&mut self, id: u32) -> &mut NestedNode {
        &mut self.nodes[id as usize - 1]
    }

    pub(crate) fn router<'a>(&'a self, root: &'a DirectSetRouter, id: u32) -> &'a DirectSetRouter {
        if id == ROOT_NODE {
            root
        } else {
            &self.node(id).router
        }
    }

    fn catalog<'a>(&'a self, root: &'a dyn RoutingNamespace, id: u32) -> &'a dyn RoutingNamespace {
        if id == ROOT_NODE {
            root
        } else {
            &self.node(id).router
        }
    }

    pub(crate) fn binding(&self, parent: u32, member: u32) -> Option<(u32, u32)> {
        self.bindings.get(&(parent, member)).copied()
    }

    // This is only the reader half of retirement. The placement owner must
    // also join writers and commit the replacement checkpoint before unlink.
    #[cfg(test)]
    pub(crate) fn backing_readers_drained(&self, subtree: u32) -> bool {
        self.nodes.iter().enumerate().all(|(index, node)| {
            let mut ancestor = index as u32 + 1;
            while ancestor != ROOT_NODE && ancestor != subtree {
                ancestor = self.node(ancestor).parent;
            }
            ancestor != subtree || std::sync::Arc::strong_count(&node.backing_lease) == 1
        })
    }

    // Admission uses the containing archive's names and lengths, never NZB
    // rows or a synthetic child job. Unknown and encrypted parent members keep
    // their ordinary physical destination.
    fn discover(&mut self, root: &dyn RoutingNamespace, parent: u32) {
        let depth = if parent == ROOT_NODE {
            1
        } else {
            self.node(parent).depth
        };
        if depth >= self.depth_limit.unwrap_or(MAX_DEPTH) {
            return;
        }
        if parent != ROOT_NODE && self.node(parent).mode != NodeMode::Direct {
            return;
        }
        let revision = self.catalog(root, parent).revision();
        let discovered = if parent == ROOT_NODE {
            self.root_discovered_revision
        } else {
            self.node(parent).discovered_revision
        };
        if discovered == Some(revision) && revision != u64::MAX {
            return;
        }
        if parent == ROOT_NODE {
            self.root_discovered_revision = Some(revision);
        } else {
            self.node_mut(parent).discovered_revision = Some(revision);
        }
        let candidates = catalog::archive_candidates(self.catalog(root, parent));

        for (member, name, format, volume, whole, length) in candidates {
            if let Some((id, input_volume)) = self.binding(parent, member) {
                let node = self.node_mut(id);
                let input = node.inputs.get_mut(&input_volume).expect("bound input");
                if input.length.is_some() && length.is_some() && input.length != length {
                    node.mode =
                        NodeMode::MaterializationPending(DemotionReason::ConflictingVolumeFacts);
                } else if input.length.is_none()
                    && let Some(length) = length
                {
                    input.length = Some(length);
                    if let Err(reason) = node.router.note_declared_volume_size(input_volume, length)
                    {
                        node.mode = NodeMode::MaterializationPending(reason);
                    }
                }
                continue;
            }
            let family = u8::from(format == SetFormat::SevenZip);
            let key = (parent, name.clone(), family);
            if self.bindings.len() >= MAX_INPUTS {
                if let Some(&child) = self.groups.get(&key) {
                    self.node_mut(child).mode =
                        NodeMode::MaterializationPending(DemotionReason::HoldsBudgetExceeded);
                }
                continue;
            }
            let id = if let Some(&id) = self.groups.get(&key) {
                id
            } else {
                // Refusal leaves this parent member physical. No partially
                // admitted child or checkpoint exists to clean up.
                if self.nodes.len() + 1 >= MAX_NODES {
                    continue;
                }
                let id = self.nodes.len() as u32 + 1;
                let root_name =
                    blake3::hash(root.plan().envelope_relative_path(0).as_bytes()).to_hex();
                let relative = PathBuf::from(".weaver-nested")
                    .join(root_name.as_str())
                    .join(id.to_string());
                let plan = DirectSetPlan {
                    set_name: name,
                    format,
                    volumes: BTreeMap::from([(volume, member)]),
                    files: HashMap::from([(member, volume)]),
                    identity: Some(IdentityPlanFacts {
                        kind: if format == SetFormat::Rar {
                            IdentityKind::Roster
                        } else {
                            IdentityKind::SevenZipRoster
                        },
                        expected_volumes: whole.then_some(1),
                        discriminator: id,
                    }),
                    unposted: Default::default(),
                    working_dir: root.plan().working_dir.join(&relative),
                    destination_dir: root.plan().destination_dir.join(&relative),
                };
                let mut router = root.resources().router(plan);
                if whole && !router.close_nested_roster(1) {
                    continue;
                }
                self.nodes.push(NestedNode {
                    parent,
                    depth: depth + 1,
                    generation: 1,
                    backing_lease: std::sync::Arc::new(()),
                    router,
                    inputs: BTreeMap::new(),
                    mode: NodeMode::Direct,
                    roster_closed: whole,
                    members_closed: false,
                    whole_sevenz: whole,
                    discovered_revision: None,
                });
                self.groups.insert(key, id);
                id
            };
            let node = self.node_mut(id);
            if node.whole_sevenz != whole || !node.router.bind_identity_volume(volume, member) {
                node.mode =
                    NodeMode::MaterializationPending(DemotionReason::ConflictingVolumeFacts);
                // The conflicting member itself never entered this child.
                continue;
            }
            node.inputs.insert(
                volume,
                NodeInput {
                    parent_member: member,
                    length,
                    delivered: ByteRanges::new(),
                    placed: ByteRanges::new(),
                    envelope_placed: ByteRanges::new(),
                    raw_placed: ByteRanges::new(),
                    confirmed: false,
                },
            );
            if let Some(length) = length
                && let Err(reason) = node.router.note_declared_volume_size(volume, length)
            {
                node.mode = NodeMode::MaterializationPending(reason);
            }
            self.bindings.insert((parent, member), (id, volume));
        }
    }

    // The caller supplies proof that the containing namespace's complete
    // member roster is known. Input completion alone never closes siblings.
    pub(crate) fn close_parent_roster(
        &mut self,
        root: &dyn RoutingNamespace,
        parent: u32,
    ) -> Result<Vec<NodeSpan>, DemotionReason> {
        self.bind_root(root)?;
        if parent != ROOT_NODE {
            self.checked_node(parent)?;
        }
        self.discover(root, parent);
        let children: Vec<_> = self
            .nodes
            .iter()
            .enumerate()
            .filter_map(|(index, node)| (node.parent == parent).then_some(index as u32 + 1))
            .collect();
        let mut output = Vec::new();
        for child in children {
            let node = self.node_mut(child);
            let was_closed = node.roster_closed;
            node.roster_closed = true;
            if node.mode != NodeMode::Direct {
                continue;
            }
            let count = node.inputs.len() as u32;
            if node.inputs.keys().copied().ne(0..count)
                || (!was_closed && !node.router.close_nested_roster(count))
            {
                node.mode =
                    NodeMode::MaterializationPending(DemotionReason::ConflictingVolumeFacts);
                continue;
            }
            let emitted = self.settle_child_inputs(root, child);
            output.extend(self.forward(root, child, emitted)?);
        }
        Ok(output)
    }

    // Drains forwarding before returning any physical write. On refusal the
    // old emitted outputs still traverse existing descendants; only subsequent
    // input to the refused child switches to its parent's physical partial.
    pub(crate) fn forward(
        &mut self,
        root: &dyn RoutingNamespace,
        owner: u32,
        spans: Vec<RoutedSpan>,
    ) -> Result<Vec<NodeSpan>, DemotionReason> {
        self.bind_root(root)?;
        if owner != ROOT_NODE {
            self.checked_node(owner)?;
        }
        let mut pending = VecDeque::from([(owner, spans)]);
        let mut physical = Vec::new();
        while let Some((owner, spans)) = pending.pop_front() {
            self.discover(root, owner);
            let generation = if owner == ROOT_NODE {
                root.generation()
            } else {
                self.node(owner).generation
            };
            for span in spans {
                let child = match span.destination {
                    DirectDestination::Member { member_id } => self.binding(owner, member_id),
                    DirectDestination::Envelope { .. } => None,
                };
                let Some((child, volume)) = child else {
                    physical.push(NodeSpan {
                        owner,
                        generation,
                        raw_child: None,
                        span,
                    });
                    continue;
                };
                let node = self.node_mut(child);
                if node.mode != NodeMode::Direct {
                    physical.push(NodeSpan {
                        owner,
                        generation,
                        raw_child: Some(RawChild {
                            node: child,
                            volume,
                            generation: node.generation,
                        }),
                        span,
                    });
                    continue;
                }
                let input = node.inputs.get_mut(&volume).expect("bound child input");
                input.delivered.insert(span.destination_offset, span.len());
                let mut emitted = Vec::new();
                let result = node.router.route_into(
                    volume,
                    span.destination_offset,
                    &span.bytes,
                    &mut emitted,
                );
                if let Err(reason) = result {
                    node.mode = NodeMode::MaterializationPending(reason);
                }
                if !emitted.is_empty() {
                    pending.push_back((child, emitted));
                }
            }
            // Metadata may resolve an input length without delivering another
            // byte. Completion is a state transition, not a side effect of the
            // next payload arrival (which may never occur).
            for index in 0..self.nodes.len() {
                let node = &self.nodes[index];
                if node.parent != owner || node.mode != NodeMode::Direct {
                    continue;
                }
                let emitted = self.settle_child_inputs(root, index as u32 + 1);
                if !emitted.is_empty() {
                    pending.push_back((index as u32 + 1, emitted));
                }
            }
        }
        // A complete input roster and every input's final parse together close
        // this node's output namespace. Propagate that proof, not merely the
        // completion of whichever child volume happened to arrive first.
        while let Some(index) = self.nodes.iter().position(|node| {
            !node.members_closed
                && node.roster_closed
                && node.mode == NodeMode::Direct
                && node.inputs.values().all(|input| input.confirmed)
        }) {
            self.nodes[index].members_closed = true;
            physical.extend(self.close_parent_roster(root, index as u32 + 1)?);
        }
        Ok(physical)
    }

    fn settle_child_inputs(&mut self, root: &dyn RoutingNamespace, child: u32) -> Vec<RoutedSpan> {
        // Nodes are admitted after their parent. Borrow the source catalog and
        // its consumer separately; unresolved lengths do not rescan names.
        let (ancestors, descendants) = self.nodes.split_at_mut(child as usize - 1);
        let node = &mut descendants[0];
        let parent: &dyn MemberCatalog = if node.parent == ROOT_NODE {
            root
        } else {
            &ancestors[node.parent as usize - 1].router
        };
        node.settle_inputs(parent)
    }

    pub(crate) fn record_placement(
        &mut self,
        root: &dyn RoutingNamespace,
        receipt: NodePlacement,
    ) -> Result<Vec<SourceSpan>, DemotionReason> {
        let chain = self.validate_placement(root, receipt)?;
        Ok(self.apply_placement(receipt, chain))
    }

    fn apply_placement(
        &mut self,
        receipt: NodePlacement,
        chain: Vec<(u32, SourceSpan)>,
    ) -> Vec<SourceSpan> {
        let placed = receipt;
        if let Some(raw) = receipt.owner.raw_child {
            self.node_mut(raw.node)
                .inputs
                .get_mut(&raw.volume)
                .expect("validated input")
                .raw_placed
                .insert(placed.destination_offset, placed.len);
        }
        let mut root_sources = Vec::new();
        for (owner, source) in chain {
            if owner == ROOT_NODE {
                root_sources.push(source);
                continue;
            }
            let input = self
                .node_mut(owner)
                .inputs
                .get_mut(&source.volume)
                .expect("projection validated input");
            input.placed.insert(source.offset, source.len);
            if owner == receipt.owner.node
                && matches!(placed.destination, DirectDestination::Envelope { .. })
            {
                input
                    .envelope_placed
                    .insert(placed.destination_offset, placed.len);
            }
        }
        root_sources.sort_by_key(|span| span.write_offset);
        root_sources
    }

    fn checked_node(&self, id: u32) -> Result<&NestedNode, DemotionReason> {
        id.checked_sub(1)
            .and_then(|index| self.nodes.get(index as usize))
            .ok_or(DemotionReason::ConflictingVolumeFacts)
    }

    fn validate_placement(
        &self,
        root: &dyn RoutingNamespace,
        receipt: NodePlacement,
    ) -> Result<Vec<(u32, SourceSpan)>, DemotionReason> {
        self.validate_root(root)?;
        let owner = receipt.owner.node;
        let generation = if owner == ROOT_NODE {
            root.generation()
        } else {
            self.checked_node(owner)?.generation
        };
        if generation != receipt.owner.generation {
            return Err(DemotionReason::ConflictingVolumeFacts);
        }
        receipt
            .destination_offset
            .checked_add(receipt.len)
            .ok_or(DemotionReason::ConflictingVolumeFacts)?;
        if let DirectDestination::Envelope { volume_index } = receipt.destination
            && (volume_index != receipt.volume_index
                || receipt.destination_offset != receipt.source_offset)
        {
            return Err(DemotionReason::ConflictingVolumeFacts);
        }
        let mut ancestor = owner;
        while ancestor != ROOT_NODE {
            let node = self.checked_node(ancestor)?;
            if node.mode == NodeMode::Materialized {
                return Err(DemotionReason::ConflictingVolumeFacts);
            }
            ancestor = node.parent;
        }
        let placed = receipt;
        let chain = if owner == ROOT_NODE {
            use crate::pipeline::extraction::catalog::InputProjection;
            match root
                .project_write(receipt)
                .map_err(|_| DemotionReason::ConflictingVolumeFacts)?
            {
                InputProjection::Mapped(ranges) => {
                    ranges.into_iter().map(|range| (ROOT_NODE, range)).collect()
                }
                InputProjection::NotReversible => Vec::new(),
            }
        } else {
            self.projection_chain(
                root,
                owner,
                SourceSpan {
                    volume: placed.volume_index,
                    offset: placed.source_offset,
                    len: placed.len,
                    write_offset: 0,
                },
            )?
        };
        // Validate the entire chain before changing any placement evidence.
        if let Some(raw) = receipt.owner.raw_child {
            let child = self.checked_node(raw.node)?;
            let input = child
                .inputs
                .get(&raw.volume)
                .ok_or(DemotionReason::ConflictingVolumeFacts)?;
            let end = placed
                .destination_offset
                .checked_add(placed.len)
                .ok_or(DemotionReason::ConflictingVolumeFacts)?;
            if child.parent != owner
                || child.generation != raw.generation
                || child.mode == NodeMode::Direct
                || placed.destination
                    != (DirectDestination::Member {
                        member_id: input.parent_member,
                    })
                || self.binding(owner, input.parent_member) != Some((raw.node, raw.volume))
                || input.length.is_some_and(|length| end > length)
            {
                return Err(DemotionReason::ConflictingVolumeFacts);
            }
        }
        Ok(chain)
    }

    fn projection_chain(
        &self,
        root: &dyn RoutingNamespace,
        node: u32,
        source: SourceSpan,
    ) -> Result<Vec<(u32, SourceSpan)>, DemotionReason> {
        self.validate_root(root)?;
        let mut pending = vec![(node, source)];
        let mut projected = Vec::new();
        while let Some((owner, source)) = pending.pop() {
            let source_end = source
                .offset
                .checked_add(source.len)
                .ok_or(DemotionReason::ConflictingVolumeFacts)?;
            source
                .write_offset
                .checked_add(source.len)
                .ok_or(DemotionReason::ConflictingVolumeFacts)?;
            projected.push((owner, source));
            if owner == ROOT_NODE {
                root.validate_input(source)
                    .map_err(|_| DemotionReason::ConflictingVolumeFacts)?;
                continue;
            }
            let node = self.checked_node(owner)?;
            let input = node
                .inputs
                .get(&source.volume)
                .ok_or(DemotionReason::ConflictingVolumeFacts)?;
            if input.length.is_some_and(|length| source_end > length) {
                return Err(DemotionReason::ConflictingVolumeFacts);
            }
            let parent = self.catalog(root, node.parent);
            use crate::pipeline::extraction::catalog::{InputProjection, MemberRange};
            match parent
                .project_member(
                    input.parent_member,
                    MemberRange {
                        offset: source.offset,
                        len: source.len,
                        write_offset: source.write_offset,
                    },
                )
                .map_err(|_| DemotionReason::ConflictingVolumeFacts)?
            {
                InputProjection::Mapped(ranges) => {
                    pending.extend(ranges.into_iter().map(|range| (node.parent, range)))
                }
                InputProjection::NotReversible => {}
            }
        }
        Ok(projected)
    }
}
