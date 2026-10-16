//! Durable intrinsic archive facts and byte backing, without live worker state.
use std::collections::BTreeSet;
use std::io;
use std::path::Path;

use super::super::restart::DirectVolumeFacts;
use serde::{Deserialize, Serialize};

const MAGIC: &[u8; 6] = b"WJAG\x01\x00";
const MAX_ITEMS: usize = 65_536;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct JobGraph {
    pub(crate) generation: u64,
    pub(crate) roots: Vec<RootGraph>,
    pub(crate) files: Vec<PhysicalFile>,
    pub(crate) images: Vec<InputImage>,
    pub(crate) sessions: crate::pipeline::extraction::session::checkpoint::Checkpoint,
    pub(crate) installed: Vec<InstalledRoot>,
    pub(crate) runtime: Option<crate::pipeline::extraction::runtime::checkpoint::RuntimeCheckpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InstalledRoot {
    pub(crate) set_name: String,
    pub(crate) marker: super::super::snapshot::InstalledSet,
}

pub(crate) fn lift_legacy_installed(
    working: &Path,
    job_id: crate::jobs::ids::JobId,
    rows: &std::collections::BTreeMap<String, Vec<u8>>,
) -> io::Result<Vec<InstalledRoot>> {
    let mut installed = Vec::new();
    for (set_name, selected) in rows {
        let body = if super::is_reference(selected) {
            super::ManifestStore::new(working, job_id, set_name).read(selected)?
        } else {
            selected.clone()
        };
        if super::super::snapshot::is_installed_marker(&body) {
            installed.push(InstalledRoot {
                set_name: set_name.clone(),
                marker: super::super::snapshot::decode_installed(&body)
                    .map_err(|_| invalid("invalid installed root during handover"))?,
            });
        }
    }
    Ok(installed)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RootGraph {
    pub(crate) set_name: String,
    pub(crate) snapshot: Vec<u8>,
    pub(crate) nodes: Vec<NodeRecord>,
    pub(crate) inputs: Vec<RootInputRecord>,
    pub(crate) decoded: Option<crate::pipeline::extraction::runtime::checkpoint::DecodedCheckpoint>,
    pub(crate) retained: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RootInputRecord {
    pub(crate) volume: u32,
    pub(crate) original: u32,
    pub(crate) effective: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeRecord {
    pub(crate) id: u32,
    pub(crate) parent: u32,
    pub(crate) depth: u32,
    pub(crate) generation: u64,
    pub(crate) set_name: String,
    pub(crate) format: u8,
    pub(crate) inputs: Vec<NodeInputRecord>,
    pub(crate) snapshot: Vec<u8>,
    pub(crate) mode: u8,
    pub(crate) roster_closed: bool,
    pub(crate) whole_sevenz: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeInputRecord {
    pub(crate) volume: u32,
    pub(crate) parent_member: u32,
    pub(crate) parent_path: String,
    pub(crate) length: Option<u64>,
    pub(crate) original: u32,
    pub(crate) effective: u32,
    pub(crate) confirmed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PhysicalFile {
    pub(crate) id: u32,
    pub(crate) root: u8,
    pub(crate) path: String,
    pub(crate) length: u64,
    pub(crate) extents: Vec<(u64, u64)>,
    pub(crate) owner: PhysicalOwner,
    pub(crate) destination: PhysicalDestination,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) enum PhysicalOwner {
    Archive {
        root: u32,
        node: u32,
        generation: u64,
    },
    Session {
        node: u32,
        generation: u64,
    },
}

impl PhysicalFile {
    pub(crate) fn archive_root(&self) -> Option<u32> {
        match self.owner {
            PhysicalOwner::Archive { root, .. } => Some(root),
            PhysicalOwner::Session { .. } => None,
        }
    }
    pub(crate) fn node(&self) -> u32 {
        match self.owner {
            PhysicalOwner::Archive { node, .. } | PhysicalOwner::Session { node, .. } => node,
        }
    }
    pub(crate) fn generation(&self) -> u64 {
        match self.owner {
            PhysicalOwner::Archive { generation, .. }
            | PhysicalOwner::Session { generation, .. } => generation,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum PhysicalDestination {
    Member { relative_path: String },
    Envelope { volume: u32 },
    RawInput { child: u32, volume: u32 },
    SessionInput { version: u64, volume: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InputImage {
    pub(crate) id: u32,
    pub(crate) root: u32,
    pub(crate) node: u32,
    pub(crate) version: u64,
    pub(crate) volume: u32,
    pub(crate) length: u64,
    pub(crate) claims: Vec<ImageClaim>,
    pub(crate) facts: Option<DirectVolumeFacts>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ImageClaim {
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) backing: Backing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Backing {
    Physical { file: u32, offset: u64 },
    // Member payload may be plaintext. Reconstruct posted bytes through the
    // image's intrinsic layout and restored crypt metadata, never a byte alias.
    Member { file: u32, offset: u64 },
    SessionInput { file: u32, offset: u64 },
    Image { image: u32, offset: u64 },
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn ranges(ranges: &[(u64, u64)], length: u64) -> io::Result<()> {
    let mut previous = None;
    if ranges.len() > MAX_ITEMS {
        return Err(invalid("too many backing ranges"));
    }
    for &(start, end) in ranges {
        if start >= end || end > length || previous.is_some_and(|end| end > start) {
            return Err(invalid("invalid backing range"));
        }
        previous = Some(end);
    }
    Ok(())
}

fn contains(ranges: &[(u64, u64)], start: u64, end: u64) -> bool {
    let mut cursor = start;
    for &(a, b) in ranges {
        if b <= cursor {
            continue;
        }
        if a > cursor {
            return false;
        }
        cursor = b;
        if cursor >= end {
            return true;
        }
    }
    false
}

impl JobGraph {
    pub(crate) fn empty(generation: u64) -> Self {
        Self {
            generation,
            roots: Vec::new(),
            files: Vec::new(),
            images: Vec::new(),
            installed: Vec::new(),
            runtime: None,
            sessions: crate::pipeline::extraction::session::ArchiveSessions::new(0, 0)
                .capture_checkpoint(),
        }
    }

    pub(crate) fn encode(&self) -> io::Result<Vec<u8>> {
        self.validate()?;
        let body = rmp_serde::to_vec(self).map_err(|_| invalid("cannot encode job graph"))?;
        if body.len() + MAGIC.len() > super::MAX_MANIFEST_BYTES {
            return Err(invalid("job graph exceeds size limit"));
        }
        let mut bytes = MAGIC.to_vec();
        bytes.extend(body);
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() > super::MAX_MANIFEST_BYTES || !bytes.starts_with(MAGIC) {
            return Err(invalid("unsupported job graph framing"));
        }
        let body = &bytes[MAGIC.len()..];
        let mut decoder = rmp_serde::Deserializer::new(std::io::Cursor::new(body));
        let graph = Self::deserialize(&mut decoder).map_err(|_| invalid("invalid job graph"))?;
        if decoder.position() != body.len() as u64 {
            return Err(invalid("trailing job graph bytes"));
        }
        graph.validate()?;
        Ok(graph)
    }

    pub(crate) fn validate(&self) -> io::Result<()> {
        if self.generation == 0
            || self.roots.len() > 4_096
            || self.files.len() > MAX_ITEMS
            || self.images.len() > MAX_ITEMS
        {
            return Err(invalid("invalid job graph inventory"));
        }
        let mut names = BTreeSet::new();
        if self.installed.len() > 4_096 {
            return Err(invalid("too many installed roots"));
        }
        for installed in &self.installed {
            if installed.set_name.is_empty() || !names.insert(&installed.set_name) {
                return Err(invalid("duplicate installed root"));
            }
            let bytes = super::super::snapshot::encode_installed(&installed.marker)
                .map_err(|_| invalid("invalid installed root"))?;
            super::super::snapshot::decode_installed(&bytes)
                .map_err(|_| invalid("invalid installed root paths"))?;
        }
        let mut root_names = BTreeSet::new();
        for (root_index, root) in self.roots.iter().enumerate() {
            if let Some(decoded) = &root.decoded {
                decoded.validate()?;
                decoded.validate_directory_files(&self.files)?;
            }
            let overlaps_installed = self
                .installed
                .iter()
                .any(|installed| installed.set_name == root.set_name);
            if root.set_name.is_empty()
                || !root_names.insert(&root.set_name)
                || (overlaps_installed != root.retained)
                || (!overlaps_installed && !names.insert(&root.set_name))
                || root.nodes.len() >= 4_096
            {
                return Err(invalid("invalid graph root"));
            }
            metadata_snapshot(&root.snapshot)?;
            let mut root_volumes = BTreeSet::new();
            for input in &root.inputs {
                if !root_volumes.insert(input.volume) {
                    return Err(invalid("duplicate root input"));
                }
                for id in [input.original, input.effective] {
                    let image = self
                        .images
                        .get(id as usize)
                        .ok_or_else(|| invalid("missing root input image"))?;
                    if image.root as usize != root_index
                        || image.node != 0
                        || image.volume != input.volume
                    {
                        return Err(invalid("root input identity mismatch"));
                    }
                }
            }
            let mut bindings = BTreeSet::new();
            let mut stable_bindings = BTreeSet::new();
            for (index, node) in root.nodes.iter().enumerate() {
                let depth = if node.parent == 0 {
                    2
                } else {
                    root.nodes
                        .get(node.parent as usize - 1)
                        .ok_or_else(|| invalid("missing graph parent"))?
                        .depth
                        + 1
                };
                if node.id as usize != index + 1
                    || node.parent >= node.id
                    || node.depth != depth
                    || depth > 3
                    || node.generation == 0
                    || node.format > 1
                    || node.mode > 2
                    || node.inputs.len() > MAX_ITEMS
                {
                    return Err(invalid("invalid graph topology"));
                }
                metadata_snapshot(&node.snapshot)?;
                let mut volumes = BTreeSet::new();
                for input in &node.inputs {
                    if input.confirmed && input.length.is_none() {
                        return Err(invalid("unknown input length cannot prove completion"));
                    }
                    crate::pipeline::extraction::validate_sanitized_rar_member_path(
                        &input.parent_path,
                    )
                    .map_err(|_| invalid("unsafe parent member identity"))?;
                    if !volumes.insert(input.volume)
                        || !bindings.insert((node.parent, input.parent_member))
                        || !stable_bindings.insert((node.parent, &input.parent_path))
                    {
                        return Err(invalid("duplicate graph input binding"));
                    }
                    for id in [input.original, input.effective] {
                        let image = self
                            .images
                            .get(id as usize)
                            .ok_or_else(|| invalid("missing input image"))?;
                        if image.root as usize != root_index
                            || image.node != node.id
                            || image.volume != input.volume
                            || (id == input.original
                                && input.length.is_some_and(|length| image.length != length))
                        {
                            return Err(invalid("input image identity mismatch"));
                        }
                    }
                }
            }
        }
        let mut paths = BTreeSet::new();
        for (index, file) in self.files.iter().enumerate() {
            if file.id as usize != index || file.root > 1 || !paths.insert((file.root, &file.path))
            {
                return Err(invalid("duplicate physical backing"));
            }
            crate::pipeline::extraction::validate_sanitized_rar_member_path(&file.path)
                .map_err(|_| invalid("unsafe physical backing path"))?;
            ranges(&file.extents, file.length)?;
            if matches!(file.owner, PhysicalOwner::Session { .. }) {
                let PhysicalDestination::SessionInput { version, .. } = file.destination else {
                    return Err(invalid("unscoped physical input owner"));
                };
                let node = self
                    .sessions
                    .nodes
                    .get(file.node() as usize)
                    .ok_or_else(|| invalid("missing session input node"))?;
                let fully_covered = if file.length == 0 {
                    file.extents.is_empty()
                } else {
                    file.extents == [(0, file.length)]
                };
                if !fully_covered
                    || (version != node.original && version != node.effective)
                    || !self.sessions.sessions.iter().any(|session| {
                        session.owner.node == file.node()
                            && session.owner.generation == file.generation()
                    })
                {
                    return Err(invalid("invalid session input backing"));
                }
                continue;
            }
            if matches!(file.destination, PhysicalDestination::SessionInput { .. }) {
                return Err(invalid("session input has a direct owner"));
            }
            let root = self
                .roots
                .get(
                    file.archive_root()
                        .ok_or_else(|| invalid("missing archive owner"))?
                        as usize,
                )
                .ok_or_else(|| invalid("missing physical owner"))?;
            if file.node() as usize > root.nodes.len() {
                return Err(invalid("invalid physical owner generation"));
            }
            let expected_generation = if file.node() == 0 {
                root.decoded
                    .as_ref()
                    .map_or(0, |decoded| decoded.owner.generation)
            } else {
                root.nodes[file.node() as usize - 1].generation
            };
            if file.generation() > expected_generation {
                return Err(invalid("future physical owner generation"));
            }
            if file.generation() < expected_generation {
                let retained = root.nodes.get(file.node().checked_sub(1).ok_or_else(|| invalid("stale root owner"))? as usize)
                    .is_some_and(|node| node.inputs.iter().any(|input| self.images.get(input.original as usize).is_some_and(|image| image.claims.iter().any(|claim| matches!(claim.backing, Backing::Physical { file: id, .. } | Backing::Member { file: id, .. } if id == file.id)))));
                if !retained {
                    return Err(invalid("unreferenced historical physical generation"));
                }
            }
        }
        let mut identities = BTreeSet::new();
        for (index, image) in self.images.iter().enumerate() {
            let root = self
                .roots
                .get(image.root as usize)
                .ok_or_else(|| invalid("missing image root"))?;
            if image.id as usize != index
                || image.node as usize > root.nodes.len()
                || !identities.insert((image.root, image.node, image.version, image.volume))
            {
                return Err(invalid("invalid input image identity"));
            }
            ranges(
                &image
                    .claims
                    .iter()
                    .map(|claim| (claim.start, claim.end))
                    .collect::<Vec<_>>(),
                image.length,
            )?;
            for claim in &image.claims {
                let (offset, length) = match claim.backing {
                    Backing::SessionInput { file, offset } => {
                        let file = self
                            .files
                            .get(file as usize)
                            .ok_or_else(|| invalid("missing effective input backing"))?;
                        let selected = if image.node == 0 {
                            root.inputs.iter().any(|input| {
                                input.volume == image.volume
                                    && input.effective == image.id
                                    && input.original != image.id
                            })
                        } else {
                            root.nodes[image.node as usize - 1]
                                .inputs
                                .iter()
                                .any(|input| {
                                    input.volume == image.volume
                                        && input.effective == image.id
                                        && input.original != image.id
                                })
                        };
                        if !selected
                            || !matches!(file.destination, PhysicalDestination::SessionInput { version, volume } if version == image.version && volume == image.volume)
                        {
                            return Err(invalid(
                                "effective input is not selected by its logical node",
                            ));
                        }
                        let end = offset
                            .checked_add(claim.end - claim.start)
                            .ok_or_else(|| invalid("effective input overflow"))?;
                        if !contains(&file.extents, offset, end) {
                            return Err(invalid("effective input exceeds its backing"));
                        }
                        (offset, file.length)
                    }
                    Backing::Physical { file, offset } | Backing::Member { file, offset } => {
                        let file = self
                            .files
                            .get(file as usize)
                            .ok_or_else(|| invalid("missing physical backing"))?;
                        if matches!(claim.backing, Backing::Member { .. })
                            != matches!(file.destination, PhysicalDestination::Member { .. })
                        {
                            return Err(invalid(
                                "member reconstruction requires explicit member backing",
                            ));
                        }
                        if file.archive_root() != Some(image.root)
                            || (file.node() != image.node
                                && !matches!(file.destination, PhysicalDestination::RawInput { child, volume } if child == image.node && volume == image.volume))
                        {
                            return Err(invalid("physical backing owner mismatch"));
                        }
                        let end = offset
                            .checked_add(claim.end - claim.start)
                            .ok_or_else(|| invalid("backing overflow"))?;
                        if !contains(&file.extents, offset, end) {
                            return Err(invalid("claim exceeds physical coverage"));
                        }
                        (offset, file.length)
                    }
                    Backing::Image {
                        image: backing,
                        offset,
                    } => {
                        // Topological image order is independent of logical ancestry.
                        if backing >= image.id {
                            return Err(invalid("cyclic input image backing"));
                        }
                        let source = &self.images[backing as usize];
                        if source.root != image.root {
                            return Err(invalid("cross-root image backing"));
                        }
                        if source.node != image.node {
                            let child = root
                                .nodes
                                .get(
                                    source
                                        .node
                                        .checked_sub(1)
                                        .ok_or_else(|| invalid("ancestor image backing"))?
                                        as usize,
                                )
                                .ok_or_else(|| invalid("missing child image backing"))?;
                            if child.parent != image.node
                                || !child.inputs.iter().any(|input| input.original == source.id)
                            {
                                return Err(invalid(
                                    "ancestor must retain child's original input image",
                                ));
                            }
                        } else if source.version > image.version {
                            return Err(invalid("original image depends on a newer derivative"));
                        }
                        let end = offset
                            .checked_add(claim.end - claim.start)
                            .ok_or_else(|| invalid("backing overflow"))?;
                        let source_ranges: Vec<_> = source
                            .claims
                            .iter()
                            .map(|claim| (claim.start, claim.end))
                            .collect();
                        if !contains(&source_ranges, offset, end) {
                            return Err(invalid("claim exceeds image coverage"));
                        }
                        (offset, source.length)
                    }
                };
                if offset
                    .checked_add(claim.end - claim.start)
                    .is_none_or(|end| end > length)
                {
                    return Err(invalid("backing bounds exceeded"));
                }
            }
        }
        let lifecycle = crate::pipeline::extraction::session::ArchiveSessions::restore_checkpoint(
            &self.sessions,
            self.sessions.root,
        )
        .map_err(|_| invalid("invalid archive lifecycle checkpoint"))?;
        for root in &self.roots {
            if let Some(decoded) = &root.decoded
                && !lifecycle.ancestry_verified(decoded.owner)
            {
                return Err(invalid(
                    "decoded namespace lacks durable ancestry verification",
                ));
            }
        }
        if let Some(runtime) = &self.runtime {
            runtime.validate()?;
            runtime.validate_input_files(self)?;
            runtime.validate_virtual_sources(self)?;
            if rmp_serde::to_vec(&runtime.sessions)
                .map_err(|_| invalid("invalid runtime lifecycle"))?
                != rmp_serde::to_vec(&self.sessions)
                    .map_err(|_| invalid("invalid graph lifecycle"))?
            {
                return Err(invalid("conflicting graph lifecycle inventories"));
            }
        }
        Ok(())
    }

    pub(crate) fn sync_directories(&self, working: &Path, destination: &Path) -> io::Result<()> {
        for decoded in self.roots.iter().filter_map(|root| root.decoded.as_ref()) {
            decoded.sync_directories(working, destination)?;
        }
        Ok(())
    }

    pub(crate) fn probe(&self, working: &Path, destination: &Path) -> io::Result<()> {
        self.validate()?;
        if let Some(runtime) = &self.runtime {
            runtime.probe_input_files(self, working, destination)?;
        }
        for decoded in self.roots.iter().filter_map(|root| root.decoded.as_ref()) {
            decoded.probe_directories(working, destination)?;
        }
        for installed in &self.installed {
            for member in &installed.marker.members {
                let metadata =
                    std::fs::symlink_metadata(owned_path(destination, &member.relative_path)?)?;
                if !metadata.file_type().is_file() || metadata.len() != member.len {
                    return Err(invalid("installed graph member is unavailable"));
                }
            }
            for directory in &installed.marker.directories {
                if !std::fs::symlink_metadata(owned_path(destination, directory)?)?
                    .file_type()
                    .is_dir()
                {
                    return Err(invalid("installed graph directory is unavailable"));
                }
            }
        }
        for file in &self.files {
            let root = if file.root == 0 { working } else { destination };
            let path = owned_path(root, &file.path)?;
            let metadata = std::fs::symlink_metadata(path)?;
            if !metadata.file_type().is_file()
                || (matches!(file.destination, PhysicalDestination::SessionInput { .. })
                    && metadata.len() != file.length)
                || file
                    .extents
                    .last()
                    .is_some_and(|&(_, end)| metadata.len() < end)
            {
                return Err(invalid("physical backing is unavailable or short"));
            }
        }
        Ok(())
    }
}

fn owned_path(root: &Path, relative: &str) -> io::Result<std::path::PathBuf> {
    let mut path = root.to_path_buf();
    let components: Vec<_> = Path::new(relative).components().collect();
    for (index, component) in components.iter().enumerate() {
        path.push(component);
        if index + 1 != components.len() && !std::fs::symlink_metadata(&path)?.file_type().is_dir()
        {
            return Err(invalid("backing parent is not an owned directory"));
        }
    }
    Ok(path)
}

fn metadata_snapshot(bytes: &[u8]) -> io::Result<()> {
    let snapshot = super::super::snapshot::decode(bytes)
        .map_err(|_| invalid("invalid graph snapshot metadata"))?;
    if snapshot
        .destinations
        .iter()
        .any(|destination| !destination.extents.is_empty())
    {
        return Err(invalid("graph snapshot duplicates physical coverage"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn graph() -> JobGraph {
        let snapshot = super::super::super::snapshot::CoverageSnapshot {
            generation: 1,
            plan_digest: [0; 32],
            destinations: Vec::new(),
            floors: Vec::new(),
            identity: None,
            fingerprints: Vec::new(),
            repost_digests: Vec::new(),
        };
        JobGraph {
            generation: 1,
            roots: vec![RootGraph {
                set_name: "archive".into(),
                snapshot: super::super::super::snapshot::encode(&snapshot).unwrap(),
                nodes: Vec::new(),
                decoded: None,
                retained: false,
                inputs: vec![RootInputRecord {
                    volume: 0,
                    original: 0,
                    effective: 0,
                }],
            }],
            files: vec![PhysicalFile {
                id: 0,
                root: 0,
                path: "volume.envelope".into(),
                length: 12,
                extents: vec![(0, 12)],
                owner: PhysicalOwner::Archive {
                    root: 0,
                    node: 0,
                    generation: 0,
                },
                destination: PhysicalDestination::Envelope { volume: 0 },
            }],
            images: vec![InputImage {
                id: 0,
                root: 0,
                node: 0,
                version: 1,
                volume: 0,
                length: 12,
                facts: None,
                claims: vec![ImageClaim {
                    start: 0,
                    end: 12,
                    backing: Backing::Physical { file: 0, offset: 0 },
                }],
            }],
            sessions: crate::pipeline::extraction::session::ArchiveSessions::new(1, 0)
                .capture_checkpoint(),
            installed: Vec::new(),
            runtime: None,
        }
    }

    #[test]
    fn zero_length_session_input_round_trips_captured_owner_and_probes() {
        use crate::pipeline::extraction::session::{ArchiveSessions, InputVersion};
        let mut sessions = ArchiveSessions::new(1, 0);
        let node = sessions.register(None, InputVersion(0)).unwrap();
        let owner = sessions.start(node).unwrap().owner;
        let mut saved = JobGraph::empty(1);
        saved.sessions = sessions.capture_checkpoint();
        saved.files.push(PhysicalFile {
            id: 0,
            root: 0,
            path: "empty-input".into(),
            length: 0,
            extents: Vec::new(),
            owner: PhysicalOwner::Session {
                node: owner.node,
                generation: owner.generation,
            },
            destination: PhysicalDestination::SessionInput {
                version: 0,
                volume: 0,
            },
        });
        let decoded = JobGraph::decode(&saved.encode().unwrap()).unwrap();
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("empty-input"), []).unwrap();
        decoded.probe(directory.path(), directory.path()).unwrap();
        saved.files[0].extents = vec![(0, 0)];
        assert!(saved.encode().is_err());
        saved.files[0].length = 1;
        saved.files[0].extents.clear();
        assert!(saved.encode().is_err());
        saved.files[0].extents = vec![(0, 1)];
        assert!(saved.encode().is_ok());
    }

    #[test]
    fn graph_codec_rejects_cycles_missing_coverage_and_trailing_bytes() {
        let mut saved = graph();
        let encoded = saved.encode().unwrap();
        assert_eq!(JobGraph::decode(&encoded).unwrap().images.len(), 1);
        let mut trailing = encoded;
        trailing.push(0);
        assert!(JobGraph::decode(&trailing).is_err());
        saved.images[0].claims[0].backing = Backing::Image {
            image: 0,
            offset: 0,
        };
        assert!(saved.encode().is_err());
        saved.images[0].claims[0].backing = Backing::Physical { file: 0, offset: 0 };
        saved.files[0].extents = vec![(1, 12)];
        assert!(saved.encode().is_err());
    }

    #[test]
    fn graph_probe_requires_explicit_coverage_and_preserves_short_file() {
        let saved = graph();
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("volume.envelope"), b"short").unwrap();
        assert!(saved.probe(directory.path(), directory.path()).is_err());
        assert_eq!(
            std::fs::read(directory.path().join("volume.envelope")).unwrap(),
            b"short"
        );
        std::fs::write(directory.path().join("volume.envelope"), [0; 12]).unwrap();
        saved.probe(directory.path(), directory.path()).unwrap();
    }

    #[test]
    fn ancestors_cannot_read_repaired_effective_child_images() {
        let mut saved = graph();
        saved.files[0].owner = PhysicalOwner::Archive {
            root: 0,
            node: 1,
            generation: 1,
        };
        saved.images[0].node = 1;
        saved.images.push(InputImage {
            id: 1,
            root: 0,
            node: 1,
            version: 2,
            volume: 0,
            length: 12,
            facts: None,
            claims: vec![ImageClaim {
                start: 0,
                end: 12,
                backing: Backing::Image {
                    image: 0,
                    offset: 0,
                },
            }],
        });
        saved.images.push(InputImage {
            id: 2,
            root: 0,
            node: 0,
            version: 1,
            volume: 0,
            length: 12,
            facts: None,
            claims: vec![ImageClaim {
                start: 0,
                end: 12,
                backing: Backing::Image {
                    image: 0,
                    offset: 0,
                },
            }],
        });
        saved.roots[0].inputs[0].original = 2;
        saved.roots[0].inputs[0].effective = 2;
        let snapshot = saved.roots[0].snapshot.clone();
        saved.roots[0].nodes.push(NodeRecord {
            id: 1,
            parent: 0,
            depth: 2,
            generation: 1,
            set_name: "child".into(),
            format: 0,
            inputs: vec![NodeInputRecord {
                volume: 0,
                parent_member: 0,
                parent_path: "child.partial".into(),
                length: Some(12),
                original: 0,
                effective: 1,
                confirmed: true,
            }],
            snapshot,
            mode: 0,
            roster_closed: true,
            whole_sevenz: false,
        });
        saved.validate().unwrap();
        saved.images[2].claims[0].backing = Backing::Image {
            image: 1,
            offset: 0,
        };
        assert!(saved.validate().is_err());
    }

    #[test]
    fn legacy_installed_reference_is_lifted_and_probed_without_losing_outputs() {
        use super::super::super::snapshot::{InstalledMember, InstalledSet};
        let directory = tempfile::tempdir().unwrap();
        let marker = InstalledSet {
            volumes: vec![(0, 2)],
            members: vec![InstalledMember {
                relative_path: "finished.bin".into(),
                len: 5,
            }],
            directories: Vec::new(),
            extracted: vec!["finished.bin".into()],
        };
        let body = super::super::super::snapshot::encode_installed(&marker).unwrap();
        let job_id = crate::jobs::ids::JobId(1);
        let reference = super::super::ManifestStore::new(directory.path(), job_id, "finished")
            .prepare(&body, None)
            .unwrap();
        let rows = std::collections::BTreeMap::from([("finished".into(), reference)]);
        let mut saved = JobGraph::empty(1);
        saved.installed = lift_legacy_installed(directory.path(), job_id, &rows).unwrap();
        std::fs::write(directory.path().join("finished.bin"), b"ready").unwrap();
        let decoded = JobGraph::decode(&saved.encode().unwrap()).unwrap();
        decoded.probe(directory.path(), directory.path()).unwrap();
        std::fs::write(directory.path().join("finished.bin"), b"shorter").unwrap();
        assert!(decoded.probe(directory.path(), directory.path()).is_err());
    }
}
