//! Capture durable backing from completed placements, never parser progress.
use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use super::ByteRanges;
use super::manifest::graph::*;
use super::router::DirectDestination;
use super::set::DirectSet;

impl crate::pipeline::Pipeline {
    /// Publish one complete job image after syncing every claimed backing.
    /// Callers retain old inputs until this succeeds, including on ambiguous SQL errors.
    pub(crate) async fn commit_archive_graph(
        &mut self,
        job_id: crate::jobs::ids::JobId,
    ) -> Result<(), String> {
        use super::manifest::ManifestStore;
        use crate::jobs::archive_checkpoint::{
            ArchiveAuthority, ArchiveCheckpointRef, CheckpointCommit,
        };
        let state = self.jobs.get(&job_id).ok_or("archive job disappeared")?;
        let working = state.working_dir.clone();
        let output = state.staging_dir.clone().unwrap_or_else(|| working.clone());
        let store = ManifestStore::for_job(&working, job_id);
        let authority = self
            .db
            .load_archive_authority(job_id)
            .map_err(|error| error.to_string())?;
        let (current, installed) = match authority {
            ArchiveAuthority::JobGone => return Err("archive job disappeared".into()),
            ArchiveAuthority::Legacy(rows) => (
                None,
                lift_legacy_installed(&working, job_id, &rows)
                    .map_err(|error| error.to_string())?,
            ),
            ArchiveAuthority::Job(reference) => {
                let previous = JobGraph::decode(
                    &store
                        .read(&reference.reference)
                        .map_err(|error| error.to_string())?,
                )
                .map_err(|error| error.to_string())?;
                (Some(reference), previous.installed)
            }
        };
        let expected = current.as_ref().map(|reference| reference.generation);
        let generation = expected
            .unwrap_or(0)
            .checked_add(1)
            .ok_or("archive checkpoint generation exhausted")?;
        let mut graph = JobGraph::empty(generation);
        graph.installed = installed;
        for set in self.direct_store.sets_for(job_id) {
            if let Some(marker) = set.installed_snapshot() {
                graph
                    .installed
                    .retain(|saved| saved.set_name != set.set_name());
                graph.installed.push(InstalledRoot {
                    set_name: set.set_name().to_owned(),
                    marker: marker.clone(),
                });
                if set.has_nested_checkpoint_authority() {
                    capture_direct_root(&mut graph, set).map_err(|error| error.to_string())?;
                }
            } else if !set.is_finalized() {
                capture_direct_root(&mut graph, set).map_err(|error| error.to_string())?;
            }
        }
        if let Some(runtime) = self.archive_runtime.get(&job_id) {
            runtime
                .append_checkpoint(&mut graph)
                .map_err(|error| error.to_string())?;
        }
        graph
            .probe(&working, &output)
            .map_err(|error| error.to_string())?;
        let body = graph.encode().map_err(|error| error.to_string())?;
        let mut paths: Vec<_> = graph
            .files
            .iter()
            .map(|file| {
                if file.root == 0 {
                    working.join(&file.path)
                } else {
                    output.join(&file.path)
                }
            })
            .collect();
        paths.extend(graph.installed.iter().flat_map(|root| {
            root.marker
                .members
                .iter()
                .map(|member| output.join(&member.relative_path))
        }));
        for outcome in crate::pipeline::orchestrator::sync_direct_destinations(paths).await {
            outcome.map_err(|error| error.to_string())?;
        }
        let current_bytes = current
            .as_ref()
            .map(|reference| reference.reference.clone());
        let writer = store.clone();
        let directory_working = working.clone();
        let directory_output = output.clone();
        let (reference, graph) = tokio::task::spawn_blocking(move || {
            graph.sync_directories(&directory_working, &directory_output)?;
            let reference = writer.prepare(&body, current_bytes.as_deref())?;
            Ok::<_, io::Error>((reference, graph))
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
        let candidate = ArchiveCheckpointRef {
            generation,
            reference: reference.clone(),
        };
        match self
            .db
            .commit_archive_checkpoint(job_id, expected, candidate.clone())
        {
            Ok(CheckpointCommit::Committed) => {}
            Ok(CheckpointCommit::Conflict) => {
                return Err("archive checkpoint ownership changed".into());
            }
            Ok(CheckpointCommit::JobGone) => return Err("archive job disappeared".into()),
            Err(error) => {
                if self
                    .db
                    .load_archive_checkpoint(job_id)
                    .ok()
                    .flatten()
                    .as_ref()
                    != Some(&candidate)
                {
                    return Err(error.to_string());
                }
            }
        }
        let now = std::time::Instant::now();
        for index in 0..self.direct_store.sets_for(job_id).len() {
            if let Some(set) = self.direct_store.set_mut(job_id, index)
                && graph
                    .roots
                    .iter()
                    .any(|root| root.decoded.is_none() && root.set_name == set.set_name())
            {
                set.enable_nested_checkpoint_authority();
                set.mark_graph_checkpoint_committed(now);
            }
        }
        if let Some(runtime) = self.archive_runtime.get_mut(&job_id) {
            runtime.confirm_replacement_checkpoint(&graph.sessions)?;
        }
        Box::pin(self.replay_direct_archive_prefixes(job_id)).await?;
        // Reclamation names only manifests owned by this job, never input files.
        if let Err(error) = tokio::task::spawn_blocking(move || store.reclaim(Some(&reference)))
            .await
            .map_err(|error| error.to_string())
            .and_then(|result| result.map_err(|error| error.to_string()))
        {
            tracing::warn!(job_id = job_id.0, %error, "archive checkpoint committed; orphan manifest cleanup deferred");
        }
        Ok(())
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn add_file(
    graph: &mut JobGraph,
    owner: PhysicalOwner,
    destination: PhysicalDestination,
    path: &Path,
    ranges: &[(u64, u64)],
    working: &Path,
    output: &Path,
) -> io::Result<()> {
    let PhysicalOwner::Archive {
        root: archive_root,
        node,
        generation,
    } = owner
    else {
        return Err(invalid("direct backing requires an archive owner"));
    };
    if ranges.is_empty() {
        return Ok(());
    }
    let (root, relative) = if let Ok(relative) = path.strip_prefix(output) {
        (1, relative)
    } else {
        (
            0,
            path.strip_prefix(working)
                .map_err(|_| invalid("backing outside job roots"))?,
        )
    };
    let relative = relative
        .to_str()
        .ok_or_else(|| invalid("non-UTF8 backing path"))?
        .to_owned();
    if let Some(existing) = graph
        .files
        .iter_mut()
        .find(|file| file.root == root && file.path == relative)
    {
        if existing.archive_root() != Some(archive_root)
            || existing.node() != node
            || existing.generation() != generation
        {
            return Err(invalid("physical backing has conflicting owners"));
        }
        match (&existing.destination, &destination) {
            (
                PhysicalDestination::Member { relative_path: a },
                PhysicalDestination::Member { relative_path: b },
            ) if a == b => {}
            (
                PhysicalDestination::Envelope { volume: a },
                PhysicalDestination::Envelope { volume: b },
            ) if a == b => {}
            (
                PhysicalDestination::RawInput {
                    child: a,
                    volume: av,
                },
                PhysicalDestination::RawInput {
                    child: b,
                    volume: bv,
                },
            ) if a == b && av == bv => {}
            (PhysicalDestination::Member { .. }, PhysicalDestination::RawInput { .. }) => {
                existing.destination = destination
            }
            _ => return Err(invalid("physical backing has conflicting destinations")),
        }
        let mut merged = ByteRanges::new();
        for &(start, end) in existing.extents.iter().chain(ranges) {
            merged.insert(start, end - start);
        }
        existing.extents = merged.ranges().to_vec();
        existing.length = std::fs::metadata(path)?.len();
        return Ok(());
    }
    graph.files.push(PhysicalFile {
        id: graph.files.len() as u32,
        root,
        path: relative,
        length: std::fs::metadata(path)?.len(),
        extents: ranges.to_vec(),
        owner,
        destination,
    });
    Ok(())
}

fn append_claim(
    claims: &mut Vec<ImageClaim>,
    covered: &mut ByteRanges,
    start: u64,
    end: u64,
    backing: Backing,
) {
    for (a, b) in covered.missing(start, end - start) {
        let delta = a - start;
        let backing = match backing {
            Backing::Physical { file, offset } => Backing::Physical {
                file,
                offset: offset + delta,
            },
            Backing::Member { file, offset } => Backing::Member {
                file,
                offset: offset + delta,
            },
            Backing::SessionInput { file, offset } => Backing::SessionInput {
                file,
                offset: offset + delta,
            },
            Backing::Image { image, offset } => Backing::Image {
                image,
                offset: offset + delta,
            },
        };
        claims.push(ImageClaim {
            start: a,
            end: b,
            backing,
        });
        covered.insert(a, b - a);
    }
}

pub(crate) fn capture_direct_root(graph: &mut JobGraph, set: &DirectSet) -> io::Result<()> {
    let root = graph.roots.len() as u32;
    let plan = set.plan();
    for claim in set.legacy_physical_claims() {
        let destination = if plan.is_envelope_destination(claim.member_index) {
            PhysicalDestination::Envelope {
                volume: super::set::envelope_volume_for_key(claim.member_index),
            }
        } else {
            PhysicalDestination::Member {
                relative_path: claim.relative_path.clone(),
            }
        };
        let ranges: Vec<_> = claim
            .extents
            .iter()
            .map(|extent| (extent.start, extent.end))
            .collect();
        add_file(
            graph,
            PhysicalOwner::Archive {
                root,
                node: 0,
                generation: 0,
            },
            destination,
            &plan.barrier_destination_path(claim.member_index, &claim.relative_path),
            &ranges,
            &plan.working_dir,
            &plan.destination_dir,
        )?;
    }
    for placement in set.qualified_placements() {
        let router = set
            .owner_router(placement.owner.node)
            .ok_or_else(|| invalid("unknown placement owner"))?;
        let destination = if let Some(raw) = placement.owner.raw_child {
            let nested = set
                .nested_graph()
                .ok_or_else(|| invalid("raw input without graph"))?;
            let child = nested
                .nodes
                .get(
                    raw.node
                        .checked_sub(1)
                        .ok_or_else(|| invalid("invalid raw child"))? as usize,
                )
                .ok_or_else(|| invalid("unknown raw child"))?;
            let DirectDestination::Member { member_id } = placement.destination else {
                return Err(invalid("raw input is not a containing member"));
            };
            if child.parent != placement.owner.node
                || child
                    .inputs
                    .get(&raw.volume)
                    .is_none_or(|input| input.parent_member != member_id)
            {
                return Err(invalid("raw input binding mismatch"));
            }
            PhysicalDestination::RawInput {
                child: raw.node,
                volume: raw.volume,
            }
        } else {
            match placement.destination {
                DirectDestination::Envelope { volume_index } => PhysicalDestination::Envelope {
                    volume: volume_index,
                },
                DirectDestination::Member { member_id } => PhysicalDestination::Member {
                    relative_path: router
                        .member_partials()
                        .into_iter()
                        .find(|(id, _, _)| *id == member_id)
                        .ok_or_else(|| invalid("unknown placement member"))?
                        .2
                        .to_owned(),
                },
            }
        };
        add_file(
            graph,
            PhysicalOwner::Archive {
                root,
                node: placement.owner.node,
                generation: placement.owner.generation,
            },
            destination,
            &placement.path,
            placement.ranges.ranges(),
            &plan.working_dir,
            &plan.destination_dir,
        )?;
    }
    let nested = set.nested_graph();
    let ids = capture_namespace_images(
        graph,
        root,
        Some(&set.router),
        nested,
        &set.checkpoint_volume_lengths(),
    )?;
    let mut snapshot = set.checkpoint_metadata(graph.generation);
    for claim in &mut snapshot.destinations {
        claim.extents.clear();
    }
    for floor in &snapshot.floors {
        let mut covered = ByteRanges::new();
        if let Some(&(image, _)) = ids.get(&(0, floor.volume_index)) {
            for claim in &graph.images[image as usize].claims {
                covered.insert(claim.start, claim.end - claim.start);
            }
        }
        if covered.contiguous_from(0) < floor.floor {
            return Err(invalid("source floor awaits complete physical backing"));
        }
    }
    let inputs = ids
        .iter()
        .filter(|((node, _), _)| *node == 0)
        .map(|(&(_, volume), &(original, effective))| RootInputRecord {
            volume,
            original,
            effective,
        })
        .collect();
    graph.roots.push(RootGraph {
        set_name: set.set_name().to_owned(),
        snapshot: super::snapshot::encode(&snapshot)
            .map_err(|_| invalid("cannot encode root metadata"))?,
        nodes: nested
            .map(|nested| nested.capture_nodes(&set.router, &ids))
            .transpose()?
            .unwrap_or_default(),
        inputs,
        decoded: None,
        retained: set.installed_snapshot().is_some(),
    });
    Ok(())
}

pub(crate) fn capture_namespace_images(
    graph: &mut JobGraph,
    root: u32,
    root_router: Option<&super::router::DirectSetRouter>,
    nested: Option<&super::nested::NestedGraph>,
    root_lengths: &BTreeMap<u32, u64>,
) -> io::Result<BTreeMap<(u32, u32), (u32, u32)>> {
    let count = nested.map_or(0, |nested| nested.nodes.len() as u32);
    let mut ids = BTreeMap::new();
    for node in (u32::from(root_router.is_none())..=count).rev() {
        let router = if node == 0 {
            root_router.ok_or_else(|| invalid("missing namespace router"))?
        } else {
            &nested
                .ok_or_else(|| invalid("missing nested graph"))?
                .node(node)
                .router
        };
        let facts = router.checkpoint_facts();
        let lengths = if node == 0 {
            root_lengths.clone()
        } else {
            nested
                .unwrap()
                .node(node)
                .inputs
                .iter()
                .map(|(&volume, input)| {
                    let length = input.length.unwrap_or_else(|| {
                        [&input.delivered, &input.placed, &input.raw_placed]
                            .into_iter()
                            .filter_map(|ranges| ranges.ranges().last().map(|&(_, end)| end))
                            .max()
                            .unwrap_or(0)
                    });
                    (volume, length)
                })
                .collect()
        };
        for (volume, length) in lengths {
            let mut claims = Vec::new();
            let mut covered = ByteRanges::new();
            // Raw replacement ranges take precedence over the old routed image.
            for file in graph
                .files
                .iter()
                .filter(|file| file.archive_root() == Some(root))
            {
                if matches!(file.destination, PhysicalDestination::RawInput { child, volume: input } if child == node && input == volume)
                    || (file.node() == node
                        && matches!(file.destination, PhysicalDestination::Envelope { volume: input } if input == volume))
                {
                    for &(start, end) in &file.extents {
                        append_claim(
                            &mut claims,
                            &mut covered,
                            start,
                            end,
                            Backing::Physical {
                                file: file.id,
                                offset: start,
                            },
                        );
                    }
                }
            }
            let members = router.member_partials();
            for extent in router.volume_member_extents(volume) {
                let stop = extent
                    .logical_offset
                    .checked_add(extent.len)
                    .ok_or_else(|| invalid("member range overflow"))?;
                if let Some((child, child_volume)) =
                    nested.and_then(|nested| nested.binding(node, extent.member_id))
                    && let Some(&(image, _)) = ids.get(&(child, child_volume))
                {
                    let child_claims = graph.images[image as usize].claims.clone();
                    for claim in child_claims {
                        let a = extent.logical_offset.max(claim.start);
                        let b = stop.min(claim.end);
                        if a < b {
                            append_claim(
                                &mut claims,
                                &mut covered,
                                extent.physical_offset + a - extent.logical_offset,
                                extent.physical_offset + b - extent.logical_offset,
                                Backing::Image { image, offset: a },
                            );
                        }
                    }
                }
                let Some((_, _, relative)) =
                    members.iter().find(|(id, _, _)| *id == extent.member_id)
                else {
                    continue;
                };
                for file in graph.files.iter().filter(|file| file.archive_root() == Some(root) && file.node() == node
                    && matches!(&file.destination, PhysicalDestination::Member { relative_path } if relative_path == relative)) {
                    for &(start, end) in &file.extents {
                        let a = extent.logical_offset.max(start);
                        let b = stop.min(end);
                        if a < b { append_claim(&mut claims, &mut covered,
                            extent.physical_offset + a - extent.logical_offset,
                            extent.physical_offset + b - extent.logical_offset,
                            Backing::Member { file: file.id, offset: a }); }
                    }
                }
            }
            claims.sort_by_key(|claim| claim.start);
            let id = graph.images.len() as u32;
            graph.images.push(InputImage {
                id,
                root,
                node,
                version: 0,
                volume,
                length,
                claims,
                facts: facts.get(&volume).cloned(),
            });
            ids.insert((node, volume), (id, id));
        }
    }
    Ok(ids)
}
