use super::*;
use std::sync::Arc;

use super::super::provider::{NestedMembers, VirtualVolume};
use super::super::router::MemberExtent;

type MemberBackings = (Arc<HashMap<u32, PathBuf>>, NestedMembers);

pub(crate) struct RepairInventory {
    pub(crate) inputs: BTreeMap<u32, (String, VirtualVolume)>,
    pub(crate) siblings: BTreeMap<String, PathBuf>,
}

impl NestedGraph {
    pub(crate) fn repair_inventory(
        &self,
        root: &dyn RoutingNamespace,
        owner: u32,
    ) -> Option<RepairInventory> {
        let node = self.nodes.get(owner.checked_sub(1)? as usize)?;
        if !(node.roster_closed || node.whole_sevenz)
            || node.inputs.values().any(|input| input.length.is_none())
        {
            return None;
        }
        let parent = self.catalog(root, node.parent);
        let members = parent.members();
        let names: BTreeMap<_, _> = members
            .iter()
            .map(|member| (member.id, member.name.to_owned()))
            .collect();
        let siblings = members
            .into_iter()
            .filter(|member| member.routable)
            .filter(|member| {
                !node
                    .inputs
                    .values()
                    .any(|input| input.parent_member == member.id)
            })
            .filter_map(|member| {
                parent
                    .member_destination(member.id)
                    .map(|path| (member.name.to_owned(), path))
            })
            .collect();
        let inputs = self
            .input_views(root, owner)
            .into_iter()
            .map(|(volume, mut view)| {
                view.retain_placed_only();
                (
                    volume,
                    (names[&node.inputs[&volume].parent_member].clone(), view),
                )
            })
            .collect();
        Some(RepairInventory { inputs, siblings })
    }

    fn has_direct_ancestors(&self, mut parent: u32) -> bool {
        while parent != ROOT_NODE {
            let node = self.node(parent);
            if node.mode != NodeMode::Direct {
                return false;
            }
            parent = node.parent;
        }
        true
    }

    pub(crate) fn live_child_descriptions(
        &self,
        root: &dyn RoutingNamespace,
    ) -> Vec<super::super::set::LiveChildDescription> {
        self.nodes
            .iter()
            .enumerate()
            .filter_map(|(index, node)| {
                if node.mode == NodeMode::Direct || !self.has_direct_ancestors(node.parent) {
                    return None;
                }
                let names: BTreeMap<_, _> = self
                    .catalog(root, node.parent)
                    .members()
                    .into_iter()
                    .map(|member| (member.id, member.name.to_owned()))
                    .collect();
                let node_id = index as u32 + 1;
                let parts = self
                    .input_views(root, node_id)
                    .into_iter()
                    .map(|(volume, mut view)| {
                        view.retain_placed_only();
                        let input = &node.inputs[&volume];
                        let complete = input.length.is_some_and(|length| {
                            let mut ranges = super::super::ByteRanges::new();
                            for (start, end) in view.readable_ranges() {
                                ranges.insert(start, end - start);
                            }
                            ranges.missing(0, length).is_empty()
                        });
                        (
                            volume,
                            crate::pipeline::extraction::input::LivePart {
                                view,
                                length: input.length,
                                complete,
                            },
                        )
                    })
                    .collect();
                Some(super::super::set::LiveChildDescription {
                    node: node_id,
                    parent: node.parent,
                    format: node.router.plan().format,
                    set_name: node.router.plan().set_name.clone(),
                    input_names: node
                        .inputs
                        .iter()
                        .map(|(&volume, input)| (volume, names[&input.parent_member].clone()))
                        .collect(),
                    parts,
                    roster_closed: node.roster_closed || node.whole_sevenz,
                })
            })
            .collect()
    }

    pub(crate) fn direct_producer_states(&self) -> Vec<(u32, Option<u32>, u64, bool)> {
        self.nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| {
                node.mode == NodeMode::Direct && self.has_direct_ancestors(node.parent)
            })
            .map(|(index, node)| {
                (
                    index as u32 + 1,
                    Some(node.parent),
                    node.generation,
                    node.roster_closed
                        && node.inputs.values().all(|input| input.confirmed)
                        && node.router.all_members_verified()
                        && !node.router.awaits_par3_verdict(),
                )
            })
            .collect()
    }

    // Freeze the entire ownership tree before handing a reader to a worker.
    // These views retain original input bytes even when a repair derivative
    // later becomes the source for descendant extraction.
    pub(crate) fn member_backings(
        &self,
        root: &dyn RoutingNamespace,
        owner: u32,
    ) -> MemberBackings {
        let catalog = self.catalog(root, owner);
        let mut physical = HashMap::new();
        let mut nested = HashMap::new();
        let mut children = HashMap::<u32, BTreeMap<u32, VirtualVolume>>::new();
        for fact in catalog.members() {
            let member = fact.id;
            let Some(path) = catalog.member_destination(member) else {
                continue;
            };
            let Some((child, volume)) = self.binding(owner, member) else {
                physical.insert(member, path);
                continue;
            };
            if self.node(child).mode == NodeMode::Materialized {
                physical.insert(member, path);
                continue;
            }
            let images = children
                .entry(child)
                .or_insert_with(|| self.input_views(root, child));
            // A binding without an input view has no virtual image to serve, so
            // the member reads from its physical destination.
            if let Some(image) = images.remove(&volume) {
                nested.insert(member, image);
            } else {
                physical.insert(member, path);
            }
        }
        (Arc::new(physical), nested.into())
    }

    pub(crate) fn input_views(
        &self,
        root: &dyn RoutingNamespace,
        owner: u32,
    ) -> BTreeMap<u32, VirtualVolume> {
        let node = self.node(owner);
        let (partials, nested) = self.member_backings(root, owner);
        let ciphers = node.router.member_ciphers();
        let backing_leases = Arc::new(vec![Arc::clone(&node.backing_lease)]);
        node.inputs
            .iter()
            .map(|(&volume, input)| {
                let held = Arc::new(node.router.held_runs(volume));
                let len = input.length.unwrap_or_else(|| {
                    [&input.delivered, &input.placed, &input.raw_placed]
                        .into_iter()
                        .filter_map(|ranges| ranges.ranges().last().map(|&(_, end)| end))
                        .chain(held.iter().map(|run| run.start.saturating_add(run.len)))
                        .max()
                        .unwrap_or(0)
                });
                let image = VirtualVolume {
                    backing_leases: Some(Arc::clone(&backing_leases)),
                    volume_index: volume,
                    envelope: node.router.plan().envelope_path(volume),
                    extents: node.router.volume_member_extents(volume),
                    partials: Arc::clone(&partials),
                    nested: nested.clone(),
                    covered: input.placed.clone(),
                    envelope_covered: input.envelope_placed.clone(),
                    held,
                    len,
                    ciphers: Arc::clone(&ciphers),
                };
                if input.raw_placed.ranges().is_empty() {
                    return (volume, image);
                }
                // Without the parent member's destination the raw ranges have no
                // backing, so they stay uncovered instead of reading elsewhere.
                let Some(path) = self
                    .catalog(root, node.parent)
                    .member_destination(input.parent_member)
                else {
                    return (volume, image);
                };
                let mut covered = input.placed.clone();
                for &(start, end) in input.raw_placed.ranges() {
                    covered.insert(start, end - start);
                }
                // One virtual member retains the old image; only receipt-backed
                // raw ranges read from the parent's partial. Never register that
                // partial and the old image as two backings for the same member.
                let extents = input
                    .raw_placed
                    .missing(0, len)
                    .into_iter()
                    .map(|(start, end)| MemberExtent {
                        member_id: 0,
                        physical_offset: start,
                        logical_offset: start,
                        len: end - start,
                    })
                    .collect();
                (
                    volume,
                    VirtualVolume {
                        backing_leases: None,
                        volume_index: volume,
                        envelope: path,
                        extents,
                        partials: Arc::default(),
                        nested: HashMap::from([(0, image)]).into(),
                        covered,
                        envelope_covered: input.raw_placed.clone(),
                        held: Arc::default(),
                        len,
                        ciphers: Arc::default(),
                    },
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod execution_tests {
    use super::*;

    #[test]
    fn replaced_ancestor_hides_old_descendant_execution_without_removing_backings() {
        let plan = DirectSetPlan {
            set_name: "archive".into(),
            format: SetFormat::Rar,
            volumes: BTreeMap::new(),
            files: HashMap::new(),
            identity: None,
            unposted: Default::default(),
            working_dir: PathBuf::from("working"),
            destination_dir: PathBuf::from("destination"),
        };
        let root = DirectSetRouter::new(plan.clone());
        let mut graph = NestedGraph::default();
        for parent in 0..2 {
            graph.nodes.push(NestedNode {
                parent,
                depth: parent + 2,
                generation: 1,
                backing_lease: Arc::new(()),
                router: DirectSetRouter::new(plan.clone()),
                inputs: BTreeMap::new(),
                mode: if parent == 0 {
                    NodeMode::Direct
                } else {
                    NodeMode::MaterializationPending(DemotionReason::UnsupportedFormat)
                },
                roster_closed: true,
                members_closed: false,
                whole_sevenz: false,
                discovered_revision: None,
            });
        }
        assert_eq!(
            graph
                .live_child_descriptions(&root)
                .iter()
                .map(|child| child.node)
                .collect::<Vec<_>>(),
            vec![2]
        );
        for replacement in [
            NodeMode::MaterializationPending(DemotionReason::UnsupportedFormat),
            NodeMode::Materialized,
        ] {
            graph.node_mut(1).mode = replacement;
            assert!(graph.direct_producer_states().is_empty());
            assert_eq!(
                graph
                    .live_child_descriptions(&root)
                    .iter()
                    .map(|child| child.node)
                    .collect::<Vec<_>>(),
                vec![1]
            );
            assert_eq!(
                graph.nodes.len(),
                2,
                "historical descendants remain original-image backings"
            );
        }
    }
}
