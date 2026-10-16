use super::*;
#[cfg(test)]
use std::fs::OpenOptions;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use super::super::provider::{VirtualVolume, VirtualVolumeReader};
use super::super::router::SpanOwner;
use super::super::sparse::{SparseMarker, SparseMarking};
use super::placement::MemberPlacement;

pub(crate) struct Materialization {
    node: u32,
    generation: u64,
    root_epoch: catalog::NamespaceEpoch,
    lineage: Vec<(u32, u64)>,
    parent: u32,
    parent_generation: u64,
    inputs: Vec<InputCopy>,
}

struct InputCopy {
    volume: u32,
    member: u32,
    image: VirtualVolume,
    destination: PathBuf,
    ranges: Vec<(u64, u64)>,
}

impl Materialization {
    // Admit every physical write before dispatching the blocking copy. The
    // worker consumes this exact frozen range plan, never a later live view.
    pub(crate) fn writes(&self) -> impl Iterator<Item = (&std::path::Path, MemberPlacement)> {
        self.inputs.iter().flat_map(move |input| {
            input.ranges.iter().map(move |&(start, end)| {
                (
                    input.destination.as_path(),
                    MemberPlacement {
                        owner: SpanOwner {
                            node: self.parent,
                            generation: self.parent_generation,
                            raw_child: Some(RawChild {
                                node: self.node,
                                volume: input.volume,
                                generation: self.generation,
                            }),
                        },
                        member: input.member,
                        offset: start,
                        len: end - start,
                    },
                )
            })
        })
    }
}

pub(crate) struct Materialized {
    node: u32,
    generation: u64,
    root_epoch: catalog::NamespaceEpoch,
    lineage: Vec<(u32, u64)>,
    copied: Vec<(u32, Vec<(u64, u64)>)>,
}

impl NestedGraph {
    fn materialization_lineage(&self, mut id: u32) -> Result<Vec<(u32, u64)>, DemotionReason> {
        let mut lineage = Vec::new();
        while id != ROOT_NODE {
            let node = self.checked_node(id)?;
            if node.mode == NodeMode::Materialized {
                return Err(DemotionReason::ConflictingVolumeFacts);
            }
            lineage.push((id, node.generation));
            id = node.parent;
        }
        Ok(lineage)
    }

    // The placement owner calls this only after it fences and joins the
    // current flight. The fence remains held through apply_materialization.
    pub(crate) fn prepare_materialization(
        &self,
        root: &dyn RoutingNamespace,
        id: u32,
    ) -> Option<Materialization> {
        self.validate_root(root).ok()?;
        let node = self.checked_node(id).ok()?;
        if !matches!(node.mode, NodeMode::MaterializationPending(_)) {
            return None;
        }
        let parent = self.catalog(root, node.parent);
        let inputs = self
            .input_views(root, id)
            .into_iter()
            .map(|(volume, image)| {
                let input = &node.inputs[&volume];
                let ranges = image
                    .readable_ranges()
                    .into_iter()
                    .flat_map(|(start, end)| input.raw_placed.missing(start, end - start))
                    .collect();
                Some(InputCopy {
                    volume,
                    member: input.parent_member,
                    image,
                    destination: parent.member_destination(input.parent_member)?,
                    ranges,
                })
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Materialization {
            node: id,
            generation: node.generation,
            root_epoch: self.root_epoch?,
            lineage: self.materialization_lineage(id).ok()?,
            parent: node.parent,
            parent_generation: if node.parent == ROOT_NODE {
                root.generation()
            } else {
                self.node(node.parent).generation
            },
            inputs,
        })
    }

    // Returns ordinary parent-owned placement receipts for the copied ranges.
    // Applying these and committing a replacement root checkpoint must precede
    // retiring any of the old descendant backings or holds.
    pub(crate) fn apply_materialization(
        &mut self,
        root: &dyn RoutingNamespace,
        result: Materialized,
    ) -> Result<Vec<(MemberPlacement, Vec<SourceSpan>)>, DemotionReason> {
        self.validate_root(root)?;
        if self.root_epoch != Some(result.root_epoch)
            || self.materialization_lineage(result.node)? != result.lineage
        {
            return Err(DemotionReason::ConflictingVolumeFacts);
        }
        let node = self.checked_node(result.node)?;
        if node.generation != result.generation
            || !matches!(node.mode, NodeMode::MaterializationPending(_))
        {
            return Err(DemotionReason::ConflictingVolumeFacts);
        }
        let parent = node.parent;
        let parent_generation = if parent == ROOT_NODE {
            root.generation()
        } else {
            self.node(parent).generation
        };
        let next_generation = node
            .generation
            .checked_add(1)
            .ok_or(DemotionReason::ConflictingVolumeFacts)?;
        let mut receipts = Vec::new();
        for (volume, ranges) in result.copied {
            let input = node
                .inputs
                .get(&volume)
                .ok_or(DemotionReason::ConflictingVolumeFacts)?;
            for (start, end) in ranges {
                let len = end
                    .checked_sub(start)
                    .ok_or(DemotionReason::ConflictingVolumeFacts)?;
                receipts.push(MemberPlacement {
                    owner: SpanOwner {
                        node: parent,
                        generation: parent_generation,
                        raw_child: Some(RawChild {
                            node: result.node,
                            volume,
                            generation: result.generation,
                        }),
                    },
                    member: input.parent_member,
                    offset: start,
                    len,
                });
            }
        }
        // Validate the complete batch, including generation ownership, before
        // changing any coverage. A rejected tail cannot commit an earlier prefix.
        let validated = receipts
            .into_iter()
            .map(|receipt| {
                self.validate_member_placement(root, receipt)
                    .map(|chain| (receipt, chain))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let applied = validated
            .into_iter()
            .map(|(receipt, chain)| (receipt, self.apply_member_placement(receipt, chain)))
            .collect();
        let node = self.node_mut(result.node);
        node.generation = next_generation;
        node.mode = NodeMode::Materialized;
        Ok(applied)
    }
}

// Blocking worker. Never truncate a partial: new raw input may already occupy
// disjoint ranges. A failed copy leaves old views authoritative and may be
// retried; none of its unreported writes becomes coverage evidence.
#[cfg(test)]
pub(crate) fn copy_inputs(
    plan: Materialization,
    sparse: SparseMarking,
) -> io::Result<Materialized> {
    copy_inputs_with(
        plan,
        sparse,
        || Ok(()),
        |path| {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(path)
        },
    )
}

pub(crate) fn copy_inputs_to_checked(
    plan: Materialization,
    root: Arc<crate::pipeline::extraction::safety::ExtractionRoot>,
    base: PathBuf,
    sparse: SparseMarking,
    check: impl FnMut() -> io::Result<()>,
) -> io::Result<Materialized> {
    copy_inputs_with(plan, sparse, check, |path| {
        let relative = path.strip_prefix(&base).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "placement outside namespace root",
            )
        })?;
        root.open_partial(relative).map(|file| file.into_std())
    })
}

fn copy_inputs_with(
    plan: Materialization,
    sparse: SparseMarking,
    mut check: impl FnMut() -> io::Result<()>,
    mut open: impl FnMut(&std::path::Path) -> io::Result<std::fs::File>,
) -> io::Result<Materialized> {
    let mut copied = Vec::new();
    let mut buffer = vec![0u8; 64 * 1024];
    for input in plan.inputs {
        check()?;
        let ranges = input.ranges;
        if ranges.is_empty() {
            copied.push((input.volume, ranges));
            continue;
        }
        let mut output = open(&input.destination)?;
        sparse.mark_sparse(&output)?;
        let mut source = VirtualVolumeReader::<true>::new(input.image, Arc::default());
        for &(start, end) in &ranges {
            source.seek(SeekFrom::Start(start))?;
            output.seek(SeekFrom::Start(start))?;
            let mut remaining = end - start;
            while remaining > 0 {
                check()?;
                let take = remaining.min(buffer.len() as u64) as usize;
                source.read_exact(&mut buffer[..take])?;
                output.write_all(&buffer[..take])?;
                remaining -= take as u64;
            }
        }
        output.sync_all()?;
        copied.push((input.volume, ranges));
    }
    Ok(Materialized {
        node: plan.node,
        generation: plan.generation,
        root_epoch: plan.root_epoch,
        lineage: plan.lineage,
        copied,
    })
}
