use super::*;

use crate::pipeline::direct_store::manifest::graph::{
    JobGraph, PhysicalDestination, PhysicalFile, PhysicalOwner, RootGraph,
};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ScopedPath {
    anchor: u8,
    path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WorkCheckpoint {
    owner: StreamOwner,
    kind: u8,
    set_name: String,
    volumes: Vec<u32>,
    output: ScopedPath,
    inputs: Vec<ScopedPath>,
    original_inputs: Vec<ScopedPath>,
    input_names: Vec<String>,
    virtual_source: Option<VirtualInputSource>,
    original_virtual_source: Option<VirtualInputSource>,
    destination: ScopedPath,
    outputs: Vec<(String, ScopedPath)>,
    root_set: Option<String>,
    ordinary: Vec<String>,
    members: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RuntimeCheckpoint {
    pub(crate) sessions: super::super::session::checkpoint::Checkpoint,
    graph_owners: Vec<(String, u32, StreamOwner)>,
    consumed: Vec<String>,
    output_owners: Vec<(String, StreamOwner)>,
    completed: Vec<WorkCheckpoint>,
    retained: Vec<WorkCheckpoint>,
    replay: Vec<WorkCheckpoint>,
}

impl RuntimeCheckpoint {
    fn selected_effective_works(&self) -> BTreeMap<(String, u32), &WorkCheckpoint> {
        let mut selected = BTreeMap::new();
        for work in self
            .completed
            .iter()
            .chain(&self.retained)
            .chain(&self.replay)
        {
            if work.virtual_source.is_some() || work.inputs.is_empty() {
                continue;
            }
            let Some(source) = &work.original_virtual_source else {
                continue;
            };
            let key = (source.root_set.clone(), source.node);
            if selected.get(&key).is_none_or(|previous: &&WorkCheckpoint| {
                previous.owner.generation < work.owner.generation
            }) {
                selected.insert(key, work);
            }
        }
        selected
    }

    fn bind_effective_images(&self, job: &mut JobGraph) -> std::io::Result<()> {
        use crate::pipeline::direct_store::manifest::graph::{Backing, ImageClaim, InputImage};
        for ((set, node), work) in self.selected_effective_works() {
            let root_index = job
                .roots
                .iter()
                .position(|root| root.set_name == set)
                .ok_or_else(|| invalid("selected virtual source root absent"))?;
            let version = self
                .sessions
                .nodes
                .get(work.owner.node as usize)
                .ok_or_else(|| invalid("selected virtual source session absent"))?
                .effective;
            for (&volume, path) in work.volumes.iter().zip(&work.inputs) {
                let file = job
                    .files
                    .iter()
                    .find(|file| file.root == path.anchor && file.path == path.path)
                    .ok_or_else(|| invalid("selected effective backing absent"))?;
                if !matches!(file.destination,PhysicalDestination::SessionInput{version: saved,volume: saved_volume} if saved==version && saved_volume==volume)
                {
                    return Err(invalid("selected effective backing role differs"));
                }
                let original_exists = if node == 0 {
                    job.roots[root_index]
                        .inputs
                        .iter()
                        .any(|input| input.volume == volume)
                } else {
                    job.roots[root_index]
                        .nodes
                        .get(node as usize - 1)
                        .filter(|record| record.id == node)
                        .ok_or_else(|| invalid("selected child node absent"))?
                        .inputs
                        .iter()
                        .any(|input| input.volume == volume)
                };
                // Recovery can add a volume that never existed in the original
                // parent namespace. Only selected runtime inventory owns it.
                if !original_exists {
                    continue;
                }
                let id = job.images.len() as u32;
                job.images.push(InputImage {
                    id,
                    root: root_index as u32,
                    node,
                    version,
                    volume,
                    length: file.length,
                    facts: None,
                    claims: if file.length == 0 {
                        Vec::new()
                    } else {
                        vec![ImageClaim {
                            start: 0,
                            end: file.length,
                            backing: Backing::SessionInput {
                                file: file.id,
                                offset: 0,
                            },
                        }]
                    },
                });
                let root = &mut job.roots[root_index];
                if node == 0 {
                    let input = root
                        .inputs
                        .iter_mut()
                        .find(|input| input.volume == volume)
                        .ok_or_else(|| {
                            invalid("selected root volume absent from original graph")
                        })?;
                    input.effective = id;
                    let mut snapshot =
                        crate::pipeline::direct_store::snapshot::decode(&root.snapshot)
                            .map_err(|_| invalid("selected root snapshot invalid"))?;
                    let floor = snapshot
                        .floors
                        .iter_mut()
                        .find(|floor| floor.volume_index == volume)
                        .ok_or_else(|| invalid("selected root floor absent"))?;
                    floor.floor = file.length;
                    floor.complete = true;
                    root.snapshot = crate::pipeline::direct_store::snapshot::encode(&snapshot)
                        .map_err(|_| invalid("selected root snapshot encode failed"))?;
                } else {
                    let input = root
                        .nodes
                        .get_mut(node as usize - 1)
                        .filter(|record| record.id == node)
                        .and_then(|record| {
                            record
                                .inputs
                                .iter_mut()
                                .find(|input| input.volume == volume)
                        })
                        .ok_or_else(|| {
                            invalid("selected child volume absent from original graph")
                        })?;
                    input.effective = id;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_virtual_sources(&self, job: &JobGraph) -> std::io::Result<()> {
        let selected = self.selected_effective_works();
        for work in self
            .completed
            .iter()
            .chain(&self.retained)
            .chain(&self.replay)
        {
            for source in work
                .virtual_source
                .iter()
                .chain(&work.original_virtual_source)
            {
                let root = job
                    .roots
                    .iter()
                    .find(|root| root.set_name == source.root_set)
                    .ok_or_else(|| invalid("runtime virtual source root absent"))?;
                if source.node != 0
                    && root
                        .nodes
                        .get(source.node as usize - 1)
                        .is_none_or(|node| node.id != source.node)
                {
                    return Err(invalid("runtime virtual source node absent"));
                }
                let session = self
                    .sessions
                    .nodes
                    .get(work.owner.node as usize)
                    .ok_or_else(|| invalid("virtual source session absent"))?;
                for volume in &work.volumes {
                    let original = if source.node == 0 {
                        root.inputs
                            .iter()
                            .find(|input| input.volume == *volume)
                            .map(|input| input.original)
                    } else {
                        root.nodes[source.node as usize - 1]
                            .inputs
                            .iter()
                            .find(|input| input.volume == *volume)
                            .map(|input| input.original)
                    };
                    let Some(original) = original else {
                        if work.virtual_source.is_none()
                            && selected
                                .get(&(source.root_set.clone(), source.node))
                                .is_some_and(|current| std::ptr::eq(*current, work))
                        {
                            continue;
                        }
                        return Err(invalid("virtual source original volume absent"));
                    };
                    if job.images[original as usize].version != session.original {
                        return Err(invalid("virtual source original version differs"));
                    }
                }
            }
        }
        for ((set, node), work) in self.selected_effective_works() {
            let root = job
                .roots
                .iter()
                .find(|root| root.set_name == set)
                .ok_or_else(|| invalid("effective source absent"))?;
            let version = self
                .sessions
                .nodes
                .get(work.owner.node as usize)
                .ok_or_else(|| invalid("effective session absent"))?
                .effective;
            for (&volume, path) in work.volumes.iter().zip(&work.inputs) {
                let file = job
                    .files
                    .iter()
                    .find(|file| file.root == path.anchor && file.path == path.path)
                    .ok_or_else(|| invalid("effective file absent"))?;
                if !matches!(file.destination,PhysicalDestination::SessionInput{version:saved,volume:saved_volume} if saved==version && saved_volume==volume)
                {
                    return Err(invalid("selected effective input role differs"));
                }
                let effective = if node == 0 {
                    root.inputs
                        .iter()
                        .find(|input| input.volume == volume)
                        .map(|input| input.effective)
                } else {
                    root.nodes[node as usize - 1]
                        .inputs
                        .iter()
                        .find(|input| input.volume == volume)
                        .map(|input| input.effective)
                };
                let Some(effective) = effective else {
                    continue;
                };
                let image = &job.images[effective as usize];
                if image.version != version
                    || image.length != file.length
                    || (file.length > 0
                        && !matches!(image.claims.as_slice(),[claim] if claim.start==0 && claim.end==file.length && matches!(claim.backing,crate::pipeline::direct_store::manifest::graph::Backing::SessionInput{file: id,offset:0} if id==file.id)))
                {
                    return Err(invalid(
                        "effective input binding differs from selected runtime input",
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn selected_delegated_roots(&self) -> std::collections::BTreeSet<String> {
        self.completed
            .iter()
            .chain(&self.retained)
            .chain(&self.replay)
            .filter(|work| work.virtual_source.is_none() && !work.inputs.is_empty())
            .filter_map(|work| {
                work.original_virtual_source
                    .as_ref()
                    .filter(|source| source.node == 0)
            })
            .map(|source| source.root_set.clone())
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DatalessCheckpoint {
    node: u32,
    generation: u64,
    index: u32,
    name: String,
    directory: bool,
    path: ScopedPath,
}

impl DatalessCheckpoint {
    fn created(
        &self,
        files: &[PhysicalFile],
        root_index: u32,
        working: &Path,
        staging: &Path,
    ) -> std::io::Result<bool> {
        if !self.directory {
            return Ok(files.iter().any(|file| file.archive_root() == Some(root_index)
                && file.node() == self.node && file.generation() == self.generation
                && file.root == self.path.anchor && file.path == self.path.path
                && file.length == 0 && file.extents.is_empty()
                && matches!(&file.destination, PhysicalDestination::Member { relative_path } if relative_path == &format!("dataless-{}", self.index))));
        }
        let root =
            crate::pipeline::extraction::ExtractionRoot::open_existing(if self.path.anchor == 0 {
                working
            } else {
                staging
            })
            .map_err(std::io::Error::other)?;
        match root.probe_directory(Path::new(&self.path.path)) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DecodedCheckpoint {
    pub(crate) owner: StreamOwner,
    pub(crate) catalog: super::super::catalog::decoded::checkpoint::Checkpoint,
    pub(crate) registry: super::super::storage::checkpoint::Checkpoint,
    base: ScopedPath,
    destinations: Vec<(u32, ScopedPath)>,
    dataless: Vec<DatalessCheckpoint>,
    set_name: String,
    holds: u64,
    scratch: u64,
    migration: u64,
}

fn invalid(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

impl ScopedPath {
    fn capture(path: &Path, working: &Path, staging: &Path) -> std::io::Result<Self> {
        let (anchor, relative) = if let Ok(relative) = path.strip_prefix(working) {
            (0, relative)
        } else {
            (
                1,
                path.strip_prefix(staging)
                    .map_err(|_| invalid("runtime path outside job roots"))?,
            )
        };
        let text = relative
            .to_str()
            .ok_or_else(|| invalid("runtime path encoding"))?
            .to_owned();
        if !text.is_empty() {
            crate::pipeline::extraction::ExtractionRoot::parse_relative_path(&text)
                .map_err(|_| invalid("unsafe runtime path"))?;
        }
        Ok(Self { anchor, path: text })
    }
    fn resolve(&self, working: &Path, staging: &Path) -> std::io::Result<PathBuf> {
        if self.anchor > 1 {
            return Err(invalid("invalid runtime path root"));
        }
        if !self.path.is_empty() {
            crate::pipeline::extraction::ExtractionRoot::parse_relative_path(&self.path)
                .map_err(|_| invalid("unsafe runtime path"))?;
        }
        Ok(if self.anchor == 0 { working } else { staging }.join(&self.path))
    }
}

impl WorkCheckpoint {
    fn capture(
        owner: StreamOwner,
        work: &NamespaceWork,
        members: Vec<String>,
        working: &Path,
        staging: &Path,
    ) -> std::io::Result<Self> {
        Ok(Self {
            owner,
            kind: encode_kind(work.kind),
            set_name: work.set_name.clone(),
            volumes: work.volumes.clone(),
            output: ScopedPath::capture(&work.output, working, staging)?,
            inputs: work
                .inputs
                .iter()
                .map(|p| ScopedPath::capture(p, working, staging))
                .collect::<std::io::Result<_>>()?,
            virtual_source: work.virtual_source.clone(),
            original_virtual_source: work.original_virtual_source.clone(),
            input_names: work.input_names.clone(),
            original_inputs: work
                .original_inputs
                .iter()
                .map(|p| ScopedPath::capture(p, working, staging))
                .collect::<std::io::Result<_>>()?,
            destination: ScopedPath::capture(&work.destination, working, staging)?,
            outputs: work
                .outputs
                .iter()
                .map(|(name, path)| {
                    Ok((name.clone(), ScopedPath::capture(path, working, staging)?))
                })
                .collect::<std::io::Result<_>>()?,
            root_set: work.root_set.clone(),
            ordinary: work.ordinary.clone(),
            members,
        })
    }
    fn restore(&self, working: &Path, staging: &Path) -> std::io::Result<NamespaceWork> {
        for name in self
            .outputs
            .iter()
            .map(|(name, _)| name)
            .chain(self.ordinary.iter())
            .chain(self.members.iter())
        {
            crate::pipeline::extraction::ExtractionRoot::parse_relative_path(name)
                .map_err(|_| invalid("unsafe runtime member"))?;
        }
        let output = self.output.resolve(working, staging)?;
        if !output.starts_with(working.join(crate::pipeline::direct_store::manifest::DIRECTORY)) {
            return Err(invalid("runtime namespace outside private root"));
        }
        let outputs = self
            .outputs
            .iter()
            .map(|(name, path)| Ok((name.clone(), path.resolve(working, staging)?)))
            .collect::<std::io::Result<BTreeMap<_, _>>>()?;
        if outputs.values().any(|path| !path.starts_with(&output)) {
            return Err(invalid("runtime backing escaped namespace"));
        }
        Ok(NamespaceWork {
            kind: decode_kind(self.kind)?,
            set_name: self.set_name.clone(),
            volumes: self.volumes.clone(),
            output,
            inputs: self
                .inputs
                .iter()
                .map(|path| path.resolve(working, staging))
                .collect::<std::io::Result<_>>()?,
            virtual_source: self.virtual_source.clone(),
            original_virtual_source: self.original_virtual_source.clone(),
            input_names: self.input_names.clone(),
            original_inputs: self
                .original_inputs
                .iter()
                .map(|path| path.resolve(working, staging))
                .collect::<std::io::Result<_>>()?,
            destination: self.destination.resolve(working, staging)?,
            outputs,
            root_set: self.root_set.clone(),
            ordinary: self.ordinary.clone(),
            selected_password: None,
        })
    }
}

impl ArchiveRuntime {
    pub(crate) fn set_roots(&mut self, working: PathBuf, staging: PathBuf) {
        self.checkpoint_roots = Some((working, staging));
    }

    pub(crate) fn append_checkpoint(&self, job: &mut JobGraph) -> std::io::Result<()> {
        let Some((working, staging)) = &self.checkpoint_roots else {
            if self.decoded.is_empty() && self.requests.is_empty() && self.completed.is_empty() {
                return Ok(());
            }
            return Err(invalid("runtime checkpoint roots absent"));
        };
        let sessions = self
            .staged_replacement
            .as_ref()
            .map(|staged| staged.sessions.clone())
            .unwrap_or_else(|| self.sessions.capture_checkpoint());
        job.sessions = sessions.clone();
        let mut replay = self
            .requests
            .values()
            .filter(|request| {
                self.checkpoint_keeps_owner(request.owner) && !self.requests.is_speculative(request)
            })
            .map(|request| {
                WorkCheckpoint::capture(request.owner, &request.work, Vec::new(), working, staging)
            })
            .chain(
                self.replay
                    .iter()
                    .chain(self.deferred_repairs.iter())
                    .filter(|(owner, _)| self.checkpoint_keeps_owner(*owner))
                    .map(|(owner, work)| {
                        WorkCheckpoint::capture(*owner, work, Vec::new(), working, staging)
                    }),
            )
            .collect::<std::io::Result<Vec<_>>>()?;
        for (owner, work, _) in &self.completed {
            if self.checkpoint_keeps_owner(*owner) && !self.sessions.ancestry_verified(*owner) {
                replay.push(WorkCheckpoint::capture(
                    *owner,
                    work,
                    Vec::new(),
                    working,
                    staging,
                )?);
            }
        }
        if let Some(staged) = &self.staged_replacement {
            replay.push(WorkCheckpoint::capture(
                staged.selected,
                &staged.work,
                Vec::new(),
                working,
                staging,
            )?);
        }
        let saved = RuntimeCheckpoint {
            graph_owners: self
                .graph_owners
                .iter()
                .filter(|((namespace, _), owner)| {
                    !self.speculative_namespaces.contains(namespace)
                        && !self.requests.values().any(|request| {
                            request.owner == **owner && self.requests.is_speculative(request)
                        })
                })
                .filter_map(|((set, node), owner)| {
                    let selected = self
                        .staged_replacement
                        .as_ref()
                        .filter(|staged| staged.selected.node == owner.node)
                        .map(|staged| staged.selected)
                        .unwrap_or(*owner);
                    (selected != *owner || self.checkpoint_keeps_owner(*owner)).then_some((
                        set.clone(),
                        *node,
                        selected,
                    ))
                })
                .collect(),
            sessions,
            consumed: self
                .consumed
                .iter()
                .filter(|name| {
                    self.output_owners
                        .get(*name)
                        .is_none_or(|owner| self.checkpoint_keeps_owner(*owner))
                })
                .cloned()
                .collect(),
            output_owners: self
                .output_owners
                .iter()
                .filter(|(_, owner)| self.checkpoint_keeps_owner(**owner))
                .map(|(name, owner)| (name.clone(), *owner))
                .collect(),
            retained: self
                .retained_work
                .iter()
                .filter(|(owner, _)| self.checkpoint_keeps_owner(*owner))
                .map(|(owner, work)| {
                    WorkCheckpoint::capture(*owner, work, Vec::new(), working, staging)
                })
                .collect::<std::io::Result<_>>()?,
            completed: self
                .completed
                .iter()
                .filter(|(owner, _, _)| {
                    self.checkpoint_keeps_owner(*owner) && self.sessions.ancestry_verified(*owner)
                })
                .map(|(owner, work, members)| {
                    WorkCheckpoint::capture(*owner, work, members.clone(), working, staging)
                })
                .collect::<std::io::Result<_>>()?,
            replay,
        };
        job.runtime = Some(saved);
        for state in self.decoded.iter().filter(|state| {
            self.checkpoint_keeps_owner(state.generation.owner())
                && self.sessions.ancestry_verified(state.generation.owner())
        }) {
            let root_index = job.roots.len() as u32;
            let owner = state.generation.owner();
            for (destination, relative, length, extents) in state
                .registry
                .checkpoint_claims()
                .map_err(|_| invalid("decoded registry busy"))?
            {
                let absolute = state.base.join(&relative);
                let role = match destination.destination {
                    crate::pipeline::direct_store::router::DirectDestination::Envelope {
                        volume_index,
                    } => PhysicalDestination::Envelope {
                        volume: volume_index,
                    },
                    crate::pipeline::direct_store::router::DirectDestination::Member {
                        member_id,
                    } => {
                        if let Some((child, volume)) =
                            state.graph.binding(destination.owner.node, member_id)
                            && state.graph.node(child).mode
                                == crate::pipeline::direct_store::nested::NodeMode::Materialized
                        {
                            PhysicalDestination::RawInput { child, volume }
                        } else {
                            let relative_path = if destination.owner.node == 0 {
                                relative.to_string_lossy().into_owned()
                            } else {
                                absolute
                                    .strip_prefix(
                                        &state
                                            .graph
                                            .node(destination.owner.node)
                                            .router
                                            .plan()
                                            .destination_dir,
                                    )
                                    .map_err(|_| invalid("decoded physical node path"))?
                                    .to_string_lossy()
                                    .into_owned()
                            };
                            PhysicalDestination::Member { relative_path }
                        }
                    }
                };
                let relative = absolute
                    .strip_prefix(working)
                    .map_err(|_| invalid("decoded physical root"))?
                    .to_str()
                    .ok_or_else(|| invalid("decoded physical path"))?
                    .to_owned();
                job.files.push(PhysicalFile {
                    id: job.files.len() as u32,
                    root: 0,
                    path: relative,
                    length,
                    extents,
                    owner: PhysicalOwner::Archive {
                        root: root_index,
                        node: destination.owner.node,
                        generation: destination.owner.generation,
                    },
                    destination: role,
                });
            }
            let images = crate::pipeline::direct_store::graph_checkpoint::capture_namespace_images(
                job,
                root_index,
                None,
                Some(&state.graph),
                &BTreeMap::new(),
            )?;
            let nodes = state.graph.capture_nodes(&state.namespace(), &images)?;
            let snapshot = crate::pipeline::direct_store::snapshot::CoverageSnapshot {
                generation: owner.generation,
                plan_digest: [0; 32],
                destinations: Vec::new(),
                floors: Vec::new(),
                identity: None,
                fingerprints: Vec::new(),
                repost_digests: Vec::new(),
            };
            let mut saved = DecodedCheckpoint {
                owner,
                catalog: state.catalog.capture_checkpoint(),
                registry: state
                    .registry
                    .capture_checkpoint()
                    .map_err(|_| invalid("decoded registry busy"))?,
                base: ScopedPath::capture(&state.base, working, staging)?,
                destinations: state
                    .destinations
                    .iter()
                    .map(|(id, path)| Ok((*id, ScopedPath::capture(path, working, staging)?)))
                    .collect::<std::io::Result<_>>()?,
                dataless: state
                    .graph
                    .nodes
                    .iter()
                    .enumerate()
                    .filter(|(_, node)| {
                        let mut ancestor = node.parent;
                        while ancestor != 0
                            && state.graph.node(ancestor).mode
                                == crate::pipeline::direct_store::nested::NodeMode::Direct
                        {
                            ancestor = state.graph.node(ancestor).parent;
                        }
                        ancestor == 0
                            && node.mode == crate::pipeline::direct_store::nested::NodeMode::Direct
                    })
                    .flat_map(|(id, node)| {
                        node.router.dataless_entries().into_iter().enumerate().map(
                            move |(index, entry)| {
                                Ok(DatalessCheckpoint {
                                    node: id as u32 + 1,
                                    generation: node.generation,
                                    index: index as u32,
                                    name: entry.name,
                                    directory: entry.is_directory,
                                    path: ScopedPath::capture(
                                        &node
                                            .router
                                            .plan()
                                            .destination_dir
                                            .join(format!("dataless-{index}")),
                                        working,
                                        staging,
                                    )?,
                                })
                            },
                        )
                    })
                    .collect::<std::io::Result<_>>()?,
                set_name: state.plan.set_name.clone(),
                holds: state.resources.holds_budget,
                scratch: state.resources.scratch_ceiling,
                migration: state.resources.migration_ceiling_bytes,
            };
            let mut created = Vec::new();
            for entry in saved.dataless {
                if entry.created(&job.files, root_index, working, staging)? {
                    created.push(entry);
                }
            }
            saved.dataless = created;
            job.roots.push(RootGraph {
                set_name: format!("decoded-{}-{}", owner.node, owner.generation),
                snapshot: crate::pipeline::direct_store::snapshot::encode(&snapshot)
                    .map_err(|_| invalid("decoded snapshot"))?,
                nodes,
                inputs: Vec::new(),
                decoded: Some(saved),
                retained: false,
            });
        }
        if let Some(runtime) = &job.runtime {
            for work in runtime
                .completed
                .iter()
                .chain(runtime.retained.iter())
                .chain(runtime.replay.iter())
            {
                let node = runtime
                    .sessions
                    .nodes
                    .get(work.owner.node as usize)
                    .ok_or_else(|| invalid("session input node absent"))?;
                for (version, inputs) in [
                    (node.original, &work.original_inputs),
                    (node.effective, &work.inputs),
                ] {
                    for (&volume, input) in work.volumes.iter().zip(inputs) {
                        let path = input.resolve(working, staging)?;
                        let metadata = std::fs::symlink_metadata(&path).map_err(|error| {
                            std::io::Error::new(
                                error.kind(),
                                format!("session input {}: {error}", input.path),
                            )
                        })?;
                        if !metadata.is_file() {
                            return Err(invalid("session input is not a regular file"));
                        }
                        let length = metadata.len();
                        let covered = |file: &PhysicalFile| {
                            file.length == length
                                && if length == 0 {
                                    file.extents.is_empty()
                                } else {
                                    file.extents == [(0, length)]
                                }
                        };
                        if let Some(file) = job
                            .files
                            .iter()
                            .find(|file| file.root == input.anchor && file.path == input.path)
                        {
                            if !covered(file) {
                                return Err(invalid(
                                    "session input lacks complete authoritative coverage",
                                ));
                            }
                            continue;
                        }
                        job.files.push(PhysicalFile {
                            id: job.files.len() as u32,
                            root: input.anchor,
                            path: input.path.clone(),
                            length,
                            extents: if length == 0 {
                                Vec::new()
                            } else {
                                vec![(0, length)]
                            },
                            owner: PhysicalOwner::Session {
                                node: work.owner.node,
                                generation: work.owner.generation,
                            },
                            destination: PhysicalDestination::SessionInput { version, volume },
                        });
                    }
                }
            }
        }
        if let Some(runtime) = job.runtime.clone() {
            runtime.bind_effective_images(job)?;
        }
        Ok(())
    }
}

impl ArchiveRuntime {
    pub(crate) fn restore_runtime_checkpoint(
        saved: &RuntimeCheckpoint,
        working: &Path,
        staging: &Path,
    ) -> std::io::Result<Self> {
        let mut runtime = Self::restore_checkpoint(&saved.sessions)
            .map_err(|_| invalid("runtime lifecycle restore"))?;
        runtime.set_roots(working.to_owned(), staging.to_owned());
        if saved.consumed.len() > 65_536
            || saved.output_owners.len() > 65_536
            || saved.completed.len() > 8_192
            || saved.replay.len() > 8_192
        {
            return Err(invalid("runtime checkpoint inventory exceeds admission"));
        }
        for name in &saved.consumed {
            crate::pipeline::extraction::ExtractionRoot::parse_relative_path(name)
                .map_err(|_| invalid("unsafe consumed archive"))?;
            runtime.consumed.insert(name.clone());
        }
        let remap = |owner: StreamOwner| -> std::io::Result<StreamOwner> {
            if owner.root != saved.sessions.root {
                return Err(invalid("runtime owner root mismatch"));
            }
            Ok(StreamOwner {
                root: runtime.root,
                ..owner
            })
        };
        for (set, node, owner) in &saved.graph_owners {
            let owner = remap(*owner)?;
            if runtime
                .graph_owners
                .insert((set.clone(), *node), owner)
                .is_some()
            {
                return Err(invalid("duplicate graph session binding"));
            }
        }
        for (name, owner) in &saved.output_owners {
            crate::pipeline::extraction::ExtractionRoot::parse_relative_path(name)
                .map_err(|_| invalid("unsafe runtime output"))?;
            let owner = remap(*owner)?;
            if !runtime.sessions.ancestry_verified(owner) {
                return Err(invalid("unverified runtime output owner"));
            }
            if runtime.output_owners.insert(name.clone(), owner).is_some() {
                return Err(invalid("duplicate runtime output"));
            }
        }
        for work in &saved.retained {
            let owner = remap(work.owner)?;
            if !runtime.sessions.ancestry_verified(owner) {
                return Err(invalid("unverified retained namespace"));
            }
            runtime
                .retained_work
                .push((owner, work.restore(working, staging)?));
        }
        for work in &saved.completed {
            let owner = remap(work.owner)?;
            if !runtime.sessions.ancestry_verified(owner) {
                return Err(invalid("unverified runtime completion"));
            }
            runtime
                .completed
                .push((owner, work.restore(working, staging)?, work.members.clone()));
        }
        for work in &saved.replay {
            let owner = remap(work.owner)?;
            let restored = work.restore(working, staging)?;
            // In-flight decoder success is never inferred after restart. The
            // selected original input remains replayable under a new owner.
            for input in &restored.inputs {
                if let Ok(relative) = input.strip_prefix(staging)
                    && let Some(text) = relative.to_str()
                {
                    runtime.consumed.remove(text);
                }
            }
            runtime.replay.push((owner, restored));
        }
        Ok(runtime)
    }
}

impl crate::pipeline::Pipeline {
    pub(crate) async fn build_decoded_graph_root(
        &mut self,
        job_id: crate::jobs::ids::JobId,
        working: &Path,
        job: &JobGraph,
        index: u32,
        runtime: &ArchiveRuntime,
    ) -> Result<decoded::DecodedState, RuntimeError> {
        const RESTORE: &str = "decoded graph restore";
        let saved = job
            .roots
            .get(index as usize)
            .and_then(|root| root.decoded.as_ref())
            .ok_or(RuntimeError::Invariant("decoded graph record"))?;
        let staging = self.deterministic_extraction_staging_dir(job_id);
        let owner = saved.restored_owner(job, runtime)?;
        let sessions = ArchiveSessions::restore_checkpoint(
            &runtime.sessions.capture_checkpoint(),
            runtime.root,
        )
        .map_err(RuntimeError::session("decoded lifecycle view"))?;
        let base = saved
            .base
            .resolve(working, &staging)
            .map_err(RuntimeError::io(RESTORE))?;
        if !base.starts_with(working.join(crate::pipeline::direct_store::manifest::DIRECTORY)) {
            return Err(RuntimeError::Invariant(
                "decoded namespace outside private root",
            ));
        }
        let destinations = saved.restored_destinations(working, &staging, &base)?;
        let budget = self.extraction_budget(job_id, &base)?;
        // A restored generation never carries a frame; the channel exists only
        // so the generation has an owner to revoke.
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        let memory = budget
            .try_reserve_memory(limits::RESTORED_GENERATION_ADMISSION_BYTES)
            .ok_or(RuntimeError::Admission("decoded generation"))?;
        let (_output, generation) = super::super::output::channel::ChannelOutput::new(
            owner,
            memory,
            sender,
            tokio::runtime::Handle::current(),
            Arc::clone(&budget),
        )
        .map_err(RuntimeError::io("decoded generation"))?;
        let catalog = super::super::catalog::decoded::DecodedCatalog::restore_checkpoint(
            &saved.catalog,
            generation.clone(),
            budget
                .try_reserve_memory(limits::CATALOG_ADMISSION_BYTES)
                .ok_or(RuntimeError::Admission("decoded catalog"))?,
        )
        .map_err(RuntimeError::io("decoded catalog restore"))?;
        let registry = super::super::storage::DestinationRegistry::restore_checkpoint(
            &saved.registry,
            budget
                .try_reserve_memory(limits::CATALOG_ADMISSION_BYTES)
                .ok_or(RuntimeError::Admission("decoded registry"))?,
            saved.owner.root,
            owner.root,
        )
        .map_err(RuntimeError::session("decoded registry restore"))?;
        saved.validate_restored_claims(&registry, job, index, &base, working, &staging)?;
        let plan = crate::pipeline::direct_store::plan::DirectSetPlan::restored_namespace(
            saved.set_name.clone(),
            &base,
        );
        let resources = crate::pipeline::direct_store::router::RouterResources {
            accountant: Arc::default(),
            holds_budget: saved.holds,
            scratch_ceiling: saved.scratch,
            sparse: Default::default(),
            migration_ceiling_bytes: saved.migration,
        };
        let namespace = crate::pipeline::direct_store::nested::catalog::DecodedNamespace {
            catalog: &catalog,
            plan: &plan,
            resources: &resources,
            destinations: &destinations,
        };
        let mut graph = crate::pipeline::direct_store::nested::NestedGraph::restore_checkpoint_at(
            &namespace, job, index, working, &staging,
        )
        .map_err(RuntimeError::io(RESTORE))?;
        graph
            .set_namespace_depth(
                sessions
                    .depth(owner)
                    .ok_or(RuntimeError::Invariant("decoded owner"))?,
            )
            .map_err(RuntimeError::routing("restored decoded namespace depth"))?;
        saved
            .validate_dataless_graph(&graph, working, &staging)
            .map_err(RuntimeError::io(RESTORE))?;
        let root = Arc::new(
            crate::pipeline::extraction::ExtractionRoot::open(&base)
                .map_err(RuntimeError::refused("decoded namespace root"))?,
        );
        let mut state = decoded::DecodedState {
            graph,
            catalog,
            registry,
            plan,
            resources,
            destinations,
            generation,
            sessions,
            physical: BTreeMap::new(),
            base,
            root,
            budget,
            delegated: runtime.namespace_nodes(&job.roots[index as usize].set_name),
        };
        state.rearm_restored_graph().await?;
        Ok(state)
    }
}

impl DecodedCheckpoint {
    // The saved owner under this run's runtime root. Only a namespace whose
    // whole ancestry closed with integrity may be rebuilt.
    fn restored_owner(
        &self,
        job: &JobGraph,
        runtime: &ArchiveRuntime,
    ) -> Result<StreamOwner, RuntimeError> {
        if self.owner.root != job.sessions.root {
            return Err(RuntimeError::Invariant("decoded owner root mismatch"));
        }
        let owner = StreamOwner {
            root: runtime.root,
            ..self.owner
        };
        if !runtime.sessions.ancestry_verified(owner) {
            return Err(RuntimeError::Invariant(
                "decoded graph namespace lacks integrity closure",
            ));
        }
        Ok(owner)
    }

    fn restored_destinations(
        &self,
        working: &Path,
        staging: &Path,
        base: &Path,
    ) -> Result<BTreeMap<u32, PathBuf>, RuntimeError> {
        let destinations = self
            .destinations
            .iter()
            .map(|(id, path)| Ok((*id, path.resolve(working, staging)?)))
            .collect::<std::io::Result<BTreeMap<_, _>>>()
            .map_err(RuntimeError::io("decoded destinations"))?;
        if destinations.len() != self.destinations.len()
            || destinations.values().any(|path| !path.starts_with(base))
        {
            return Err(RuntimeError::Invariant("decoded destinations"));
        }
        Ok(destinations)
    }

    // Every registry claim must be exactly one physical file the job graph
    // owns for this root, with the same length and extents.
    fn validate_restored_claims(
        &self,
        registry: &super::super::storage::DestinationRegistry,
        job: &JobGraph,
        index: u32,
        base: &Path,
        working: &Path,
        staging: &Path,
    ) -> Result<(), RuntimeError> {
        let claims = registry
            .checkpoint_claims()
            .map_err(RuntimeError::session("decoded physical restore"))?;
        let selected: Vec<_> = job
            .files
            .iter()
            .filter(|file| file.archive_root() == Some(index))
            .collect();
        if claims.len() != selected.len() {
            return Err(RuntimeError::Invariant(
                "decoded physical inventory mismatch",
            ));
        }
        for (destination, relative, length, extents) in claims {
            let path = base.join(relative);
            let expected = selected
                .iter()
                .find(|file| {
                    working.join(&file.path) == path
                        && file.node() == destination.owner.node
                        && file.generation() == destination.owner.generation
                })
                .ok_or(RuntimeError::Invariant(
                    "decoded registry claim lacks authoritative physical file",
                ))?;
            if expected.length != length || expected.extents != extents {
                return Err(RuntimeError::Invariant(
                    "decoded registry extents differ from authoritative graph",
                ));
            }
            if let Some(entry) = self.dataless.iter().find(|entry| {
                entry
                    .path
                    .resolve(working, staging)
                    .is_ok_and(|candidate| candidate == path)
            }) && (entry.directory
                || !matches!(destination.destination, crate::pipeline::direct_store::router::DirectDestination::Member { member_id } if member_id == u32::MAX - entry.index))
            {
                return Err(RuntimeError::Invariant(
                    "dataless registry identity mismatch",
                ));
            }
        }
        Ok(())
    }
}

impl decoded::DecodedState {
    async fn rearm_restored_graph(&mut self) -> Result<(), RuntimeError> {
        let nodes: Vec<_> = self
            .graph
            .direct_producer_states()
            .into_iter()
            .map(|state| state.0)
            .collect();
        for node in nodes {
            let runs = self.graph.node(node).router.restart_read_plan();
            if runs.is_empty() {
                continue;
            }
            let (physical, nested) = self.graph.member_backings(&self.namespace(), node);
            let (runs, checksums) = super::rearm::RearmRead {
                runs,
                physical,
                nested,
                root: Arc::clone(&self.root),
                base: self.base.clone(),
                budget: Arc::clone(&self.budget),
            }
            .checksum()
            .await?;
            for (run, checksum) in runs.iter().zip(checksums) {
                self.graph
                    .node_mut(node)
                    .router
                    .note_restored_member_crc(run.member_id, run.logical_offset, run.len, checksum)
                    .map_err(RuntimeError::routing("decoded graph integrity rearm"))?;
            }
        }
        Ok(())
    }
}

impl RuntimeCheckpoint {
    pub(crate) fn validate(&self) -> std::io::Result<()> {
        let sessions = ArchiveSessions::restore_checkpoint(&self.sessions, self.sessions.root)
            .map_err(|_| invalid("invalid runtime lifecycle"))?;
        if self.consumed.len() > 65_536
            || self.output_owners.len() > 65_536
            || self.completed.len() > 8_192
            || self.retained.len() > 8_192
            || self.replay.len() > 8_192
        {
            return Err(invalid("runtime inventory limit"));
        }
        let mut bindings = std::collections::BTreeSet::new();
        for (set, node, owner) in &self.graph_owners {
            if set.is_empty()
                || !bindings.insert((set, node))
                || owner.root != self.sessions.root
                || !self.sessions.sessions.iter().any(|session| {
                    session.owner.node == owner.node && session.owner.generation == owner.generation
                })
            {
                return Err(invalid("invalid graph session binding"));
            }
        }
        for name in self
            .consumed
            .iter()
            .chain(self.output_owners.iter().map(|(name, _)| name))
        {
            crate::pipeline::extraction::ExtractionRoot::parse_relative_path(name)
                .map_err(|_| invalid("unsafe runtime member"))?;
        }
        for (_, owner) in &self.output_owners {
            if !sessions.ancestry_verified(*owner) {
                return Err(invalid("unverified runtime output"));
            }
        }
        for (complete, work) in self
            .completed
            .iter()
            .chain(self.retained.iter())
            .map(|work| (true, work))
            .chain(self.replay.iter().map(|work| (false, work)))
        {
            if work.owner.root != self.sessions.root
                || (complete && !sessions.ancestry_verified(work.owner))
            {
                return Err(invalid("invalid runtime request owner"));
            }
            work.output.validate()?;
            work.destination.validate()?;
            if work.output.anchor != 0
                || !Path::new(&work.output.path)
                    .starts_with(crate::pipeline::direct_store::manifest::DIRECTORY)
            {
                return Err(invalid("runtime namespace outside private root"));
            }
            for path in work.inputs.iter().chain(work.original_inputs.iter()) {
                path.validate()?;
            }
            if (work.virtual_source.is_none() && work.inputs.len() != work.volumes.len())
                || (work.original_virtual_source.is_none()
                    && work.original_inputs.len() != work.volumes.len())
                || work.input_names.len() != work.volumes.len()
                || decode_kind(work.kind).is_err()
            {
                return Err(invalid("invalid runtime input roster"));
            }
            for (name, path) in &work.outputs {
                crate::pipeline::extraction::ExtractionRoot::parse_relative_path(name)
                    .map_err(|_| invalid("unsafe runtime output"))?;
                path.validate()?;
                if path.anchor != work.output.anchor
                    || !Path::new(&path.path).starts_with(&work.output.path)
                {
                    return Err(invalid("runtime output escaped private namespace"));
                }
            }
            for name in work
                .members
                .iter()
                .chain(work.ordinary.iter())
                .chain(work.input_names.iter())
            {
                crate::pipeline::extraction::ExtractionRoot::parse_relative_path(name)
                    .map_err(|_| invalid("unsafe runtime completion"))?;
            }
        }
        Ok(())
    }
}
impl ScopedPath {
    fn validate(&self) -> std::io::Result<()> {
        if self.anchor > 1 {
            return Err(invalid("invalid runtime path root"));
        }
        if !self.path.is_empty() {
            crate::pipeline::extraction::ExtractionRoot::parse_relative_path(&self.path)
                .map_err(|_| invalid("unsafe runtime path"))?;
        }
        Ok(())
    }
}
impl DecodedCheckpoint {
    pub(crate) fn validate(&self) -> std::io::Result<()> {
        self.base.validate()?;
        if self.base.anchor != 0
            || !Path::new(&self.base.path)
                .starts_with(crate::pipeline::direct_store::manifest::DIRECTORY)
            || self.owner.generation == 0
            || self.set_name.is_empty()
            || self.destinations.len() > 65_536
            || self.dataless.len() > 65_536
        {
            return Err(invalid("invalid decoded namespace"));
        }
        let mut ids = std::collections::BTreeSet::new();
        for (id, path) in &self.destinations {
            path.validate()?;
            if !ids.insert(*id)
                || path.anchor != self.base.anchor
                || !Path::new(&path.path).starts_with(&self.base.path)
            {
                return Err(invalid("invalid decoded destination"));
            }
        }
        self.catalog.validate()?;
        for (member, _) in self.catalog.directories() {
            if !self.destinations.iter().any(|(id, _)| *id == member) {
                return Err(invalid("decoded directory destination absent"));
            }
        }
        self.registry.validate()?;
        let mut entries = std::collections::BTreeSet::new();
        let mut paths: std::collections::BTreeSet<_> = self
            .destinations
            .iter()
            .map(|(_, path)| (path.anchor, &path.path))
            .collect();
        for entry in &self.dataless {
            entry.path.validate()?;
            crate::pipeline::extraction::ExtractionRoot::parse_relative_path(&entry.name)
                .map_err(|_| invalid("unsafe dataless name"))?;
            if entry.node == 0
                || entry.generation == 0
                || entry.index >= 65_536
                || entry.path.anchor != self.base.anchor
                || !Path::new(&entry.path.path).starts_with(&self.base.path)
                || !entries.insert((entry.node, entry.index))
                || !paths.insert((entry.path.anchor, &entry.path.path))
            {
                return Err(invalid("invalid dataless destination"));
            }
        }
        Ok(())
    }

    fn validate_dataless_graph(
        &self,
        graph: &crate::pipeline::direct_store::nested::NestedGraph,
        working: &Path,
        staging: &Path,
    ) -> std::io::Result<()> {
        for saved in &self.dataless {
            let node = graph
                .nodes
                .get(saved.node as usize - 1)
                .ok_or_else(|| invalid("dataless node absent"))?;
            let entry = node
                .router
                .dataless_entries()
                .into_iter()
                .nth(saved.index as usize)
                .ok_or_else(|| invalid("dataless intrinsic entry absent"))?;
            let mut ancestor = node.parent;
            while ancestor != 0
                && graph.node(ancestor).mode
                    == crate::pipeline::direct_store::nested::NodeMode::Direct
            {
                ancestor = graph.node(ancestor).parent;
            }
            if ancestor != 0
                || node.mode != crate::pipeline::direct_store::nested::NodeMode::Direct
                || node.generation != saved.generation
                || entry.name != saved.name
                || entry.is_directory != saved.directory
                || saved.path.resolve(working, staging)?
                    != node
                        .router
                        .plan()
                        .destination_dir
                        .join(format!("dataless-{}", saved.index))
            {
                return Err(invalid("dataless intrinsic destination mismatch"));
            }
        }
        Ok(())
    }

    fn directory_paths(&self) -> std::io::Result<Vec<&ScopedPath>> {
        let mut paths = self
            .catalog
            .directories()
            .map(|(member, _)| {
                self.destinations
                    .iter()
                    .find_map(|(id, path)| (*id == member).then_some(path))
                    .ok_or_else(|| invalid("decoded directory destination absent"))
            })
            .collect::<std::io::Result<Vec<_>>>()?;
        paths.extend(
            self.dataless
                .iter()
                .filter(|entry| entry.directory)
                .map(|entry| &entry.path),
        );
        Ok(paths)
    }

    pub(crate) fn validate_directory_files(&self, files: &[PhysicalFile]) -> std::io::Result<()> {
        for path in self.directory_paths()? {
            if files
                .iter()
                .any(|file| file.root == path.anchor && file.path == path.path)
            {
                return Err(invalid("decoded directory cannot claim byte extents"));
            }
        }
        for entry in self.dataless.iter().filter(|entry| !entry.directory) {
            if !files.iter().any(|file| file.root == entry.path.anchor && file.path == entry.path.path
                && file.node() == entry.node && file.generation() == entry.generation
                && file.length == 0 && file.extents.is_empty()
                && matches!(&file.destination, PhysicalDestination::Member { relative_path } if relative_path == &format!("dataless-{}", entry.index)))
            { return Err(invalid("empty member lacks physical authority")); }
        }
        Ok(())
    }

    pub(crate) fn probe_directories(&self, working: &Path, staging: &Path) -> std::io::Result<()> {
        self.check_directories(working, staging, false)
    }

    pub(crate) fn sync_directories(&self, working: &Path, staging: &Path) -> std::io::Result<()> {
        self.check_directories(working, staging, true)
    }

    fn check_directories(&self, working: &Path, staging: &Path, sync: bool) -> std::io::Result<()> {
        for path in self.directory_paths()? {
            let root =
                crate::pipeline::extraction::ExtractionRoot::open_existing(if path.anchor == 0 {
                    working
                } else {
                    staging
                })
                .map_err(std::io::Error::other)?;
            if sync {
                root.sync_directory(Path::new(&path.path))?;
            } else {
                root.probe_directory(Path::new(&path.path))?;
            }
        }
        Ok(())
    }
}

fn encode_kind(kind: crate::jobs::assembly::ArchiveType) -> u8 {
    use crate::jobs::assembly::ArchiveType::*;
    match kind {
        Rar => 0,
        SevenZip => 1,
        Zip => 2,
        Tar => 3,
        TarGz => 4,
        TarBz2 => 5,
        TarXz => 6,
        Gz => 7,
        Deflate => 8,
        Brotli => 9,
        Zstd => 10,
        Bzip2 => 11,
        Xz => 12,
        Split => 13,
    }
}
fn decode_kind(kind: u8) -> std::io::Result<crate::jobs::assembly::ArchiveType> {
    use crate::jobs::assembly::ArchiveType::*;
    Ok(match kind {
        0 => Rar,
        1 => SevenZip,
        2 => Zip,
        3 => Tar,
        4 => TarGz,
        5 => TarBz2,
        6 => TarXz,
        7 => Gz,
        8 => Deflate,
        9 => Brotli,
        10 => Zstd,
        11 => Bzip2,
        12 => Xz,
        13 => Split,
        _ => return Err(invalid("invalid archive engine kind")),
    })
}

impl RuntimeCheckpoint {
    fn directory_output(
        job: &JobGraph,
        work: &WorkCheckpoint,
        name: &str,
        output: &ScopedPath,
    ) -> bool {
        job.roots
            .iter()
            .filter_map(|root| root.decoded.as_ref())
            .any(|decoded| {
                decoded.owner == work.owner
                    && (decoded.catalog.directories().any(|(member, logical)| {
                        logical == name
                            && decoded.destinations.iter().any(|(id, path)| {
                                *id == member
                                    && path.anchor == output.anchor
                                    && path.path == output.path
                            })
                    }) || decoded.dataless.iter().any(|entry| {
                        entry.directory
                            && entry.name == name
                            && entry.path.anchor == output.anchor
                            && entry.path.path == output.path
                    }))
            })
    }

    pub(crate) fn validate_input_files(&self, job: &JobGraph) -> std::io::Result<()> {
        let files = &job.files;
        for work in self
            .completed
            .iter()
            .chain(self.retained.iter())
            .chain(self.replay.iter())
        {
            for input in work.original_inputs.iter().chain(work.inputs.iter()) {
                let file = files
                    .iter()
                    .find(|file| file.root == input.anchor && file.path == input.path)
                    .ok_or_else(|| invalid("runtime input missing physical authority"))?;
                if if file.length == 0 {
                    !file.extents.is_empty()
                } else {
                    file.extents != [(0, file.length)]
                } {
                    return Err(invalid("runtime input physical coverage incomplete"));
                }
            }
            for (name, output) in &work.outputs {
                if Self::directory_output(job, work, name, output) {
                    if files
                        .iter()
                        .any(|file| file.root == output.anchor && file.path == output.path)
                    {
                        return Err(invalid("directory output has byte-file authority"));
                    }
                    continue;
                }
                let file = files
                    .iter()
                    .find(|file| file.root == output.anchor && file.path == output.path)
                    .ok_or_else(|| invalid("runtime output missing physical authority"))?;
                if if file.length == 0 {
                    !file.extents.is_empty()
                } else {
                    file.extents != [(0, file.length)]
                } {
                    return Err(invalid("runtime output physical coverage incomplete"));
                }
            }
        }
        Ok(())
    }
}

impl RuntimeCheckpoint {
    pub(crate) fn probe_input_files(
        &self,
        job: &JobGraph,
        working: &Path,
        staging: &Path,
    ) -> std::io::Result<()> {
        use crate::pipeline::extraction::ExtractionRoot;
        let files = &job.files;
        for work in self
            .completed
            .iter()
            .chain(self.retained.iter())
            .chain(self.replay.iter())
        {
            for input in work.original_inputs.iter().chain(work.inputs.iter()) {
                let selected = files
                    .iter()
                    .find(|file| file.root == input.anchor && file.path == input.path)
                    .ok_or_else(|| invalid("runtime input authority missing"))?;
                let root = ExtractionRoot::open_existing(if input.anchor == 0 {
                    working
                } else {
                    staging
                })
                .map_err(|_| invalid("runtime input root absent"))?;
                let file = root.open_input(Path::new(&input.path))?;
                if file.metadata()?.len() != selected.length {
                    return Err(invalid("runtime input length changed"));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod empty_input_tests {
    use super::*;

    #[tokio::test]
    async fn repaired_virtual_input_selects_effective_image_without_filling_original_holes() {
        use crate::pipeline::direct_store::manifest::graph::{
            InputImage, RootGraph, RootInputRecord,
        };
        use crate::pipeline::direct_store::snapshot::{CoverageSnapshot, VolumeFloor};
        let directory = tempfile::tempdir().unwrap();
        let working = directory.path().join("working");
        let staging = directory.path().join("staging");
        std::fs::create_dir(&working).unwrap();
        std::fs::create_dir(&staging).unwrap();
        let effective = working.join("effective.7z");
        std::fs::write(&effective, b"repaired").unwrap();
        let recovered = working.join("recovered.7z.002");
        std::fs::write(&recovered, b"new volume").unwrap();
        let mut runtime = ArchiveRuntime::new();
        runtime.set_roots(working.clone(), staging.clone());
        let owner = runtime.admit(None, InputVersion(0)).unwrap().owner;
        runtime.begin_repair(owner).await.unwrap();
        let work = NamespaceWork {
            kind: crate::jobs::assembly::ArchiveType::SevenZip,
            set_name: "archive".into(),
            volumes: vec![0],
            output: working.join("unused"),
            inputs: Vec::new(),
            original_inputs: Vec::new(),
            input_names: vec!["archive.7z".into()],
            virtual_source: None,
            original_virtual_source: Some(VirtualInputSource {
                root_set: "archive".into(),
                node: 0,
            }),
            destination: staging.clone(),
            outputs: BTreeMap::new(),
            root_set: Some("archive".into()),
            ordinary: Vec::new(),
            selected_password: None,
        };
        let mut work = work;
        work.volumes.push(1);
        runtime
            .stage_repair_replacement(
                owner,
                work,
                vec![effective, recovered.clone()],
                vec!["archive.7z".into(), "archive.7z.002".into()],
            )
            .unwrap();
        let mut graph = JobGraph::empty(1);
        graph.roots.push(RootGraph {
            set_name: "archive".into(),
            snapshot: crate::pipeline::direct_store::snapshot::encode(&CoverageSnapshot {
                generation: 1,
                plan_digest: [0; 32],
                destinations: Vec::new(),
                floors: vec![VolumeFloor {
                    volume_index: 0,
                    file_index: 0,
                    floor: 0,
                    complete: false,
                }],
                identity: None,
                fingerprints: Vec::new(),
                repost_digests: Vec::new(),
            })
            .unwrap(),
            nodes: Vec::new(),
            inputs: vec![RootInputRecord {
                volume: 0,
                original: 0,
                effective: 0,
            }],
            decoded: None,
            retained: false,
        });
        graph.images.push(InputImage {
            id: 0,
            root: 0,
            node: 0,
            version: 0,
            volume: 0,
            length: 8,
            claims: Vec::new(),
            facts: None,
        });
        runtime.append_checkpoint(&mut graph).unwrap();
        assert_eq!(graph.roots[0].inputs[0].original, 0);
        assert_ne!(graph.roots[0].inputs[0].effective, 0);
        assert!(graph.images[0].claims.is_empty());
        assert_eq!(graph.roots[0].inputs.len(), 1);
        assert_eq!(graph.images.len(), 2);
        assert_eq!(graph.files.len(), 2);
        let decoded = JobGraph::decode(&graph.encode().unwrap()).unwrap();
        decoded.probe(&working, &staging).unwrap();
        let restored = ArchiveRuntime::restore_runtime_checkpoint(
            decoded.runtime.as_ref().unwrap(),
            &working,
            &staging,
        )
        .unwrap();
        assert_eq!(restored.replay[0].1.volumes, vec![0, 1]);
        assert_eq!(restored.replay[0].1.inputs[1], recovered);
        let mut wrong_extra = decoded.clone();
        wrong_extra.files[1].destination = PhysicalDestination::SessionInput {
            version: 1,
            volume: 0,
        };
        assert!(wrong_extra.encode().is_err());
        let mut invalid = decoded.clone();
        invalid.roots[0].inputs[0].effective = 0;
        assert!(invalid.encode().is_err());
    }

    #[test]
    fn runtime_capture_round_trips_zero_length_session_input() {
        let directory = tempfile::tempdir().unwrap();
        let working = directory.path().join("working");
        let staging = directory.path().join("staging");
        std::fs::create_dir(&working).unwrap();
        std::fs::create_dir(&staging).unwrap();
        let input = working.join("empty.7z");
        std::fs::write(&input, []).unwrap();
        let mut runtime = ArchiveRuntime::new();
        runtime.set_roots(working.clone(), staging.clone());
        let owner = runtime.admit(None, InputVersion(0)).unwrap().owner;
        runtime.replay.push((
            owner,
            NamespaceWork {
                kind: crate::jobs::assembly::ArchiveType::SevenZip,
                set_name: "empty".into(),
                volumes: vec![0],
                output: working
                    .join(crate::pipeline::direct_store::manifest::DIRECTORY)
                    .join("output"),
                inputs: vec![input.clone()],
                original_inputs: vec![input],
                input_names: vec!["empty.7z".into()],
                virtual_source: None,
                original_virtual_source: None,
                destination: staging.clone(),
                outputs: BTreeMap::new(),
                root_set: None,
                ordinary: Vec::new(),
                selected_password: None,
            },
        ));
        let mut graph = JobGraph::empty(1);
        runtime.append_checkpoint(&mut graph).unwrap();
        assert_eq!(graph.files.len(), 1);
        assert_eq!(graph.files[0].length, 0);
        assert!(graph.files[0].extents.is_empty());
        let restored = JobGraph::decode(&graph.encode().unwrap()).unwrap();
        restored.probe(&working, &staging).unwrap();
    }
}
