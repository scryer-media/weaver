//! Rebuild direct child routers from immutable input facts and durable backing.
use super::super::manifest::graph::{JobGraph, PhysicalDestination};
use super::*;

// A node's mode as the record stores it. The record keeps the disposition a
// refusal chose — engine or file — and drops the refusal itself, which a
// restart does not rederive from retained bytes.
const MODE_DIRECT: u8 = 0;
const MODE_PENDING: u8 = 1;
const MODE_MATERIALIZED: u8 = 2;
const MODE_ENGINE: u8 = 3;
// The highest code the graph manifest accepts.
pub(in crate::pipeline::direct_store) const LAST_MODE: u8 = MODE_ENGINE;

#[track_caller]
fn invalid() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "nested graph cannot be reconstructed at line {}",
            std::panic::Location::caller().line()
        ),
    )
}

impl NestedGraph {
    pub(crate) fn capture_nodes(
        &self,
        root: &dyn RoutingNamespace,
        images: &BTreeMap<(u32, u32), (u32, u32)>,
    ) -> std::io::Result<Vec<super::super::manifest::graph::NodeRecord>> {
        use super::super::manifest::graph::{NodeInputRecord, NodeRecord};
        use super::super::snapshot::{CoverageSnapshot, DestinationClaim};
        self.nodes
            .iter()
            .enumerate()
            .map(|(index, node)| {
                let id = index as u32 + 1;
                let crypt = node.router.member_crypt_snapshots();
                let snapshot = CoverageSnapshot {
                    generation: node.generation,
                    plan_digest: node
                        .router
                        .plan()
                        .digest(&node.router.member_digest_entries()),
                    destinations: node
                        .router
                        .member_partials()
                        .into_iter()
                        .map(|(member, _, relative)| DestinationClaim {
                            member_index: member,
                            relative_path: relative.to_owned(),
                            extents: Vec::new(),
                            crypt: crypt.get(&member).cloned(),
                        })
                        .collect(),
                    floors: Vec::new(),
                    identity: None,
                    fingerprints: Vec::new(),
                    repost_digests: Vec::new(),
                };
                let inputs = node
                    .inputs
                    .iter()
                    .map(|(&volume, input)| {
                        let parent = self.catalog(root, node.parent);
                        let parent_path = parent
                            .member_destination(input.parent_member)
                            .ok_or_else(invalid)?
                            .strip_prefix(&parent.plan().destination_dir)
                            .map_err(|_| invalid())?
                            .to_str()
                            .ok_or_else(invalid)?
                            .to_owned();
                        let &(original, effective) =
                            images.get(&(id, volume)).ok_or_else(invalid)?;
                        Ok(NodeInputRecord {
                            volume,
                            parent_member: input.parent_member,
                            parent_path,
                            length: input.length,
                            original,
                            effective,
                            confirmed: input.confirmed,
                        })
                    })
                    .collect::<std::io::Result<_>>()?;
                Ok(NodeRecord {
                    id,
                    parent: node.parent,
                    depth: node.depth,
                    generation: node.generation,
                    set_name: node.router.plan().set_name.clone(),
                    format: node.router.plan().format.code(),
                    inputs,
                    snapshot: super::super::snapshot::encode(&snapshot).map_err(|_| invalid())?,
                    mode: match node.mode {
                        NodeMode::Direct => MODE_DIRECT,
                        NodeMode::Engine => MODE_ENGINE,
                        NodeMode::MaterializationPending(_) => MODE_PENDING,
                        NodeMode::Materialized => MODE_MATERIALIZED,
                    },
                    roster_closed: node.roster_closed,
                })
            })
            .collect()
    }

    pub(crate) fn restore_checkpoint(
        root: &dyn RoutingNamespace,
        job: &JobGraph,
        root_index: u32,
    ) -> std::io::Result<Self> {
        Self::restore_checkpoint_at(
            root,
            job,
            root_index,
            &root.plan().working_dir,
            &root.plan().destination_dir,
        )
    }

    pub(crate) fn restore_checkpoint_at(
        root: &dyn RoutingNamespace,
        job: &JobGraph,
        root_index: u32,
        working: &std::path::Path,
        destination: &std::path::Path,
    ) -> std::io::Result<Self> {
        job.validate()?;
        let saved = job.roots.get(root_index as usize).ok_or_else(invalid)?;
        let mut restored = Self::default();
        restored.bind_root(root).map_err(|_| invalid())?;
        for record in &saved.nodes {
            let format = SetFormat::from_code(record.format).ok_or_else(invalid)?;
            let parent_catalog = restored.catalog(root, record.parent);
            let volumes: BTreeMap<_, _> = record
                .inputs
                .iter()
                .map(|input| {
                    let expected = parent_catalog.plan().destination_path(&input.parent_path);
                    let member = parent_catalog
                        .members()
                        .into_iter()
                        .find(|member| {
                            parent_catalog
                                .member_destination(member.id)
                                .is_some_and(|path| path == expected)
                        })
                        .ok_or_else(invalid)?;
                    Ok((input.volume, member.id))
                })
                .collect::<std::io::Result<_>>()?;
            let parent_members = volumes.clone();
            let plan = super::nested_plan(
                root.plan(),
                record.id,
                record.set_name.clone(),
                format,
                volumes,
                record.roster_closed.then_some(record.inputs.len() as u32),
            );
            let mut router = root.resources().router(plan);
            let facts = record
                .inputs
                .iter()
                .filter_map(|input| {
                    job.images[input.original as usize]
                        .facts
                        .clone()
                        .map(|facts| (input.volume, facts))
                })
                .collect();
            if record.mode == MODE_DIRECT {
                router.restore_layout(&facts)
            } else {
                router.restore_retained_layout(&facts)
            }
            .map_err(|_| invalid())?;
            let snapshot =
                super::super::snapshot::decode(&record.snapshot).map_err(|_| invalid())?;
            let mut original_plan = router.plan().clone();
            original_plan.volumes = record
                .inputs
                .iter()
                .map(|input| (input.volume, input.parent_member))
                .collect();
            original_plan.files = original_plan
                .volumes
                .iter()
                .map(|(&volume, &member)| (member, volume))
                .collect();
            if snapshot.plan_digest != original_plan.digest(&router.member_digest_entries()) {
                return Err(invalid());
            }
            for claim in &snapshot.destinations {
                router
                    .restore_member_crypt(&claim.relative_path, claim.crypt.as_ref())
                    .map_err(|_| invalid())?;
            }
            for file in &job.files {
                if file.archive_root() == Some(root_index)
                    && file.node() == record.id
                    && let PhysicalDestination::Member { relative_path } = &file.destination
                    && !(saved.decoded.is_some()
                        && file.length == 0
                        && file.extents.is_empty()
                        && router
                            .dataless_entries()
                            .iter()
                            .enumerate()
                            .any(|(index, entry)| {
                                !entry.is_directory && relative_path == &format!("dataless-{index}")
                            }))
                    && router
                        .restore_member_coverage(relative_path, &file.extents)
                        .is_none()
                {
                    return Err(invalid());
                }
            }
            for child in saved.nodes.iter().filter(|child| child.parent == record.id) {
                for input in &child.inputs {
                    let image = &job.images[input.original as usize];
                    let ranges = image
                        .claims
                        .iter()
                        .map(|claim| (claim.start, claim.end))
                        .collect::<Vec<_>>();
                    if router
                        .restore_member_coverage(&input.parent_path, &ranges)
                        .is_none()
                    {
                        return Err(invalid());
                    }
                }
            }
            let mut inputs = BTreeMap::new();
            for input in &record.inputs {
                let image = &job.images[input.original as usize];
                if image.root != root_index {
                    return Err(invalid());
                }
                let mut placed = ByteRanges::new();
                for claim in &image.claims {
                    placed.insert(claim.start, claim.end - claim.start);
                }
                let mut envelope_placed = ByteRanges::new();
                let mut raw_placed = ByteRanges::new();
                for file in &job.files {
                    if file.archive_root() != Some(root_index) {
                        continue;
                    }
                    match file.destination {
                        PhysicalDestination::Envelope { volume }
                            if file.node() == record.id && volume == input.volume =>
                        {
                            for &(start, end) in &file.extents {
                                envelope_placed.insert(start, end - start);
                            }
                        }
                        PhysicalDestination::RawInput { child, volume }
                            if child == record.id && volume == input.volume =>
                        {
                            for &(start, end) in &file.extents {
                                raw_placed.insert(start, end - start);
                            }
                        }
                        _ => {}
                    }
                }
                router.restore_volume_coverage(
                    input.volume,
                    &placed,
                    input.confirmed.then_some(input.length).flatten(),
                );
                inputs.insert(
                    input.volume,
                    NodeInput {
                        parent_member: parent_members[&input.volume],
                        length: input.length,
                        delivered: placed.clone(),
                        placed,
                        envelope_placed,
                        raw_placed,
                        confirmed: input.confirmed,
                    },
                );
                if restored
                    .bindings
                    .insert(
                        (record.parent, parent_members[&input.volume]),
                        (record.id, input.volume),
                    )
                    .is_some()
                {
                    return Err(invalid());
                }
            }
            for image in job
                .images
                .iter()
                .filter(|image| image.root == root_index && image.node == record.id)
            {
                for claim in &image.claims {
                    use super::super::manifest::graph::Backing;
                    let backing = match claim.backing {
                        Backing::Member { file, offset } => {
                            let PhysicalDestination::Member { relative_path } =
                                &job.files[file as usize].destination
                            else {
                                return Err(invalid());
                            };
                            Some((relative_path.as_str(), offset))
                        }
                        Backing::Image {
                            image: source,
                            offset,
                        } if job.images[source as usize].node != record.id => {
                            let source = &job.images[source as usize];
                            let child = saved
                                .nodes
                                .get(source.node as usize - 1)
                                .ok_or_else(invalid)?;
                            let input = child
                                .inputs
                                .iter()
                                .find(|input| input.original == source.id)
                                .ok_or_else(invalid)?;
                            Some((input.parent_path.as_str(), offset))
                        }
                        _ => None,
                    };
                    if let Some((relative, offset)) = backing {
                        router
                            .restore_backed_extent(
                                image.volume,
                                claim.start,
                                relative,
                                offset,
                                claim.end - claim.start,
                            )
                            .map_err(|_| invalid())?;
                    }
                }
            }
            restored.groups.insert(
                (record.parent, record.set_name.clone(), record.format),
                record.id,
            );
            let mode = match record.mode {
                MODE_DIRECT => NodeMode::Direct,
                MODE_ENGINE => NodeMode::Engine,
                MODE_PENDING => {
                    NodeMode::MaterializationPending(DemotionReason::ConflictingVolumeFacts)
                }
                _ => NodeMode::Materialized,
            };
            restored.nodes.push(NestedNode {
                parent: record.parent,
                depth: record.depth,
                generation: record.generation,
                backing_lease: std::sync::Arc::new(()),
                router,
                inputs,
                mode,
                roster_closed: record.roster_closed,
                members_closed: record.roster_closed
                    && record.inputs.iter().all(|input| input.confirmed),
                discovered_revision: None,
            });
        }
        for file in &job.files {
            if file.archive_root() != Some(root_index) {
                continue;
            }
            let catalog = restored.catalog(root, file.node());
            let (expected, physical_root) = match &file.destination {
                PhysicalDestination::SessionInput { .. } => return Err(invalid()),
                PhysicalDestination::Envelope { volume } => {
                    if !catalog.plan().volumes.contains_key(volume) {
                        return Err(invalid());
                    }
                    (catalog.plan().envelope_path(*volume), 0)
                }
                PhysicalDestination::Member { relative_path } => {
                    if saved.decoded.is_some()
                        && file.node() != ROOT_NODE
                        && file.length == 0
                        && file.extents.is_empty()
                        && restored
                            .node(file.node())
                            .router
                            .dataless_entries()
                            .iter()
                            .enumerate()
                            .any(|(index, entry)| {
                                !entry.is_directory && relative_path == &format!("dataless-{index}")
                            })
                    {
                        if file.root != 0
                            || working.join(&file.path)
                                != catalog.plan().destination_path(relative_path)
                        {
                            return Err(invalid());
                        }
                        continue;
                    }
                    let member = catalog
                        .members()
                        .into_iter()
                        .find(|member| {
                            catalog.member_destination(member.id).is_some_and(|path| {
                                path == catalog.plan().destination_path(relative_path)
                            })
                        })
                        .ok_or_else(invalid)?;
                    if catalog.length(member.id).is_some_and(|length| {
                        file.extents.last().is_some_and(|&(_, end)| end > length)
                    }) {
                        return Err(invalid());
                    }
                    (
                        catalog.member_destination(member.id).ok_or_else(invalid)?,
                        u8::from(saved.decoded.is_none()),
                    )
                }
                PhysicalDestination::RawInput { child, volume } => {
                    let node = restored.checked_node(*child).map_err(|_| invalid())?;
                    if node.parent != file.node() {
                        return Err(invalid());
                    }
                    let input = node.inputs.get(volume).ok_or_else(invalid)?;
                    (
                        catalog
                            .member_destination(input.parent_member)
                            .ok_or_else(invalid)?,
                        u8::from(saved.decoded.is_none()),
                    )
                }
            };
            let base = if file.root == 0 { working } else { destination };
            if file.root != physical_root || base.join(&file.path) != expected {
                return Err(invalid());
            }
        }
        use super::super::manifest::graph::Backing;
        use crate::pipeline::extraction::catalog::{InputProjection, MemberRange};
        for image in job.images.iter().filter(|image| image.root == root_index) {
            let catalog = restored.catalog(root, image.node);
            for claim in &image.claims {
                let projection = match claim.backing {
                    Backing::SessionInput { .. } => continue,
                    Backing::Physical { file, offset } | Backing::Member { file, offset } => {
                        let file = &job.files[file as usize];
                        match &file.destination {
                            PhysicalDestination::SessionInput { .. } => return Err(invalid()),
                            PhysicalDestination::Envelope { volume } => {
                                if *volume != image.volume || offset != claim.start {
                                    return Err(invalid());
                                }
                                continue;
                            }
                            PhysicalDestination::RawInput { child, volume } => {
                                if *child != image.node
                                    || *volume != image.volume
                                    || offset != claim.start
                                {
                                    return Err(invalid());
                                }
                                continue;
                            }
                            PhysicalDestination::Member { relative_path } => {
                                let member = catalog
                                    .members()
                                    .into_iter()
                                    .find(|member| {
                                        catalog.member_destination(member.id).is_some_and(|path| {
                                            path == catalog.plan().destination_path(relative_path)
                                        })
                                    })
                                    .ok_or_else(invalid)?;
                                catalog
                                    .project_member(
                                        member.id,
                                        MemberRange {
                                            offset,
                                            len: claim.end - claim.start,
                                            write_offset: 0,
                                        },
                                    )
                                    .map_err(|_| invalid())?
                            }
                        }
                    }
                    Backing::Image {
                        image: source,
                        offset,
                    } => {
                        let source = &job.images[source as usize];
                        if source.node == image.node {
                            if source.volume != image.volume || offset != claim.start {
                                return Err(invalid());
                            }
                            continue;
                        }
                        let child = restored.checked_node(source.node).map_err(|_| invalid())?;
                        let input = child.inputs.get(&source.volume).ok_or_else(invalid)?;
                        catalog
                            .project_member(
                                input.parent_member,
                                MemberRange {
                                    offset,
                                    len: claim.end - claim.start,
                                    write_offset: 0,
                                },
                            )
                            .map_err(|_| invalid())?
                    }
                };
                let InputProjection::Mapped(spans) = projection else {
                    return Err(invalid());
                };
                let mut coverage = ByteRanges::new();
                for span in spans {
                    if span.volume != image.volume
                        || span.offset
                            != claim
                                .start
                                .checked_add(span.write_offset)
                                .ok_or_else(invalid)?
                    {
                        return Err(invalid());
                    }
                    coverage.insert(span.offset, span.len);
                }
                if coverage.ranges() != [(claim.start, claim.end)] {
                    return Err(invalid());
                }
            }
        }
        Ok(restored)
    }
}
