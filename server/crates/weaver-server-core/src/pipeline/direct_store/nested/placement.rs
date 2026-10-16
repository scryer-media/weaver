use super::*;
use crate::pipeline::extraction::catalog::{InputProjection, MemberRange};

// A physical member write has member coordinates even when its producer has
// no reversible mapping to input bytes. Never invent source offsets for it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MemberPlacement {
    pub(crate) owner: super::super::router::SpanOwner,
    pub(crate) member: u32,
    pub(crate) offset: u64,
    pub(crate) len: u64,
}

impl NestedGraph {
    // Records the graph's reconstruction evidence, not the destination ledger.
    // The placement owner must retain this typed physical receipt in its
    // owner-qualified destination registry before checkpointing. An empty
    // returned projection still represents a physical member write; catalog
    // delivery/completion must never replace that physical coverage record.
    #[cfg(test)]
    pub(crate) fn record_member_placement(
        &mut self,
        root: &dyn RoutingNamespace,
        receipt: MemberPlacement,
    ) -> Result<Vec<SourceSpan>, DemotionReason> {
        let chain = self.validate_member_placement(root, receipt)?;
        Ok(self.apply_member_placement(receipt, chain))
    }

    pub(super) fn validate_member_placement(
        &self,
        root: &dyn RoutingNamespace,
        receipt: MemberPlacement,
    ) -> Result<Vec<(u32, SourceSpan)>, DemotionReason> {
        self.validate_root(root)?;
        let owner = receipt.owner.node;
        let generation = if owner == ROOT_NODE {
            root.generation()
        } else {
            self.checked_node(owner)?.generation
        };
        let end = receipt
            .offset
            .checked_add(receipt.len)
            .ok_or(DemotionReason::ConflictingVolumeFacts)?;
        if generation != receipt.owner.generation {
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
        if let Some(raw) = receipt.owner.raw_child {
            let child = self.checked_node(raw.node)?;
            let input = child
                .inputs
                .get(&raw.volume)
                .ok_or(DemotionReason::ConflictingVolumeFacts)?;
            if child.parent != owner
                || child.generation != raw.generation
                || child.mode == NodeMode::Direct
                || input.parent_member != receipt.member
                || self.binding(owner, receipt.member) != Some((raw.node, raw.volume))
                || input.length.is_some_and(|length| end > length)
            {
                return Err(DemotionReason::ConflictingVolumeFacts);
            }
        }
        let projection = self
            .catalog(root, owner)
            .project_member(
                receipt.member,
                MemberRange {
                    offset: receipt.offset,
                    len: receipt.len,
                    write_offset: 0,
                },
            )
            .map_err(|_| DemotionReason::ConflictingVolumeFacts)?;
        let mut chain = Vec::new();
        if let InputProjection::Mapped(ranges) = projection {
            for range in ranges {
                chain.extend(self.projection_chain(root, owner, range)?);
            }
        }
        Ok(chain)
    }

    pub(super) fn apply_member_placement(
        &mut self,
        receipt: MemberPlacement,
        chain: Vec<(u32, SourceSpan)>,
    ) -> Vec<SourceSpan> {
        if let Some(raw) = receipt.owner.raw_child {
            self.node_mut(raw.node)
                .inputs
                .get_mut(&raw.volume)
                .expect("validated input")
                .raw_placed
                .insert(receipt.offset, receipt.len);
        }
        let mut root_sources = Vec::new();
        for (owner, source) in chain {
            if owner == ROOT_NODE {
                root_sources.push(source);
            } else {
                self.node_mut(owner)
                    .inputs
                    .get_mut(&source.volume)
                    .expect("validated input")
                    .placed
                    .insert(source.offset, source.len);
            }
        }
        root_sources.sort_by_key(|span| span.write_offset);
        root_sources
    }
}
