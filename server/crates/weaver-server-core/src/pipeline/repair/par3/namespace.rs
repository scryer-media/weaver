//! Operation-local recovery over an explicit immutable namespace inventory.

use super::*;
use crate::pipeline::extraction::safety::{ExtractionRoot, MemoryPermit};
use par3_rs::session::RepairStatus;
use std::collections::BTreeSet;
use std::path::Path;

pub(in crate::pipeline) struct NamespaceInput {
    pub path: PathBuf,
    pub name: String,
    pub ranges: Vec<std::ops::Range<u64>>,
}

pub(in crate::pipeline) struct VerifiedMember {
    pub path: PathBuf,
    pub length: u64,
}

pub(in crate::pipeline) struct NamespaceOutcome {
    pub verified: Vec<VerifiedMember>,
    pub unresolved_sets: usize,
}

pub(in crate::pipeline) struct NamespaceRepair {
    job: Par3Job,
    input_paths: BTreeMap<String, PathBuf>,
    next_source: u64,
    output_limit: u64,
}

impl NamespaceRepair {
    pub fn scan(
        inputs: Vec<NamespaceInput>,
        output_limit: u64,
        memory_limit: u64,
        cancellation: par3_rs::runtime::CancellationToken,
    ) -> EngineResult<Self> {
        if inputs.len() > MAX_CARRIERS {
            return Err(budget::host_limit("namespace PAR3 source count"));
        }
        let mut job = Par3Job::default();
        let memory_limit = usize::try_from(memory_limit)
            .map_err(|_| budget::host_limit("namespace PAR3 memory bytes"))?;
        job.options.memory = par3_rs::runtime::MemoryBudget::new(memory_limit);
        job.options.retained_bytes = memory_limit / 2;
        job.options.cancel = cancellation;
        job.options.cancel.check()?;
        let mut names = BTreeSet::new();
        let mut input_paths = BTreeMap::new();
        let mut next_source = 0;
        for input in inputs {
            check_path(&input.name)?;
            if !names.insert(input.name.to_lowercase()) {
                return Err(EngineError::InvalidState("namespace input paths alias"));
            }
            let source = SourceId(next_source);
            next_source += 1;
            input_paths.insert(input.name.clone(), input.path.clone());
            job.publish_file(source, input.path.clone(), input.name, input.ranges.clone())?;
            job.scan_file(source, input.path, Some(input.ranges))?;
        }
        let mut operation = Self {
            job,
            input_paths,
            next_source,
            output_limit,
        };
        operation.validate_outputs()?;
        operation.assess()?;
        Ok(operation)
    }

    fn assess(&mut self) -> EngineResult<()> {
        // Native content discovery proposes a binding, never a rename of an
        // immutable carrier. Apply it only to this operation's namespace.
        for _ in 0..=MAX_CARRIERS {
            self.job.assess()?;
            let Some(found) = self.job.take_name_match()? else {
                return Ok(());
            };
            check_path(&found.name)?;
            let path = found.path.ok_or(EngineError::InvalidState(
                "namespace name match has no immutable backing",
            ))?;
            self.job.retire_name_bindings(found.source, &found.name)?;
            self.job
                .bindings
                .retain(|_, source| *source != found.source);
            self.input_paths.insert(found.name.clone(), path);
            self.job.bindings.insert(found.name, found.source);
        }
        Err(budget::host_limit("namespace PAR3 identity bindings"))
    }

    fn validate_outputs(&mut self) -> EngineResult<()> {
        self.job.validate_shared_layouts()?;
        if self.job.absolute_paths {
            return Err(EngineError::InvalidState(
                "absolute namespace recovery paths",
            ));
        }
        let mut paths = BTreeMap::new();
        let mut aliases = BTreeMap::new();
        let mut total = 0u64;
        for set in self.job.sets.values_mut() {
            let Some(layout) = set.native.layout()? else {
                continue;
            };
            for file in layout.files() {
                check_path(&file.path)?;
                let alias = file.path.to_lowercase();
                if let Some(previous) = aliases.insert(alias, file.path.clone())
                    && previous != file.path
                {
                    return Err(EngineError::InvalidState("namespace recovery paths alias"));
                }
                if paths.insert(file.path.clone(), file.len).is_none() {
                    total = total
                        .checked_add(file.len)
                        .filter(|bytes| *bytes <= self.output_limit)
                        .ok_or_else(|| budget::host_limit("namespace PAR3 output bytes"))?;
                }
            }
        }
        if paths.len() > MAX_CARRIERS {
            return Err(budget::host_limit("namespace PAR3 output count"));
        }
        for path in aliases.keys() {
            if path
                .match_indices('/')
                .any(|(index, _)| aliases.contains_key(&path[..index]))
            {
                return Err(EngineError::InvalidState(
                    "namespace output file is an output directory",
                ));
            }
        }
        Ok(())
    }

    // The caller owns an empty private output root, its disk admission, and
    // this memory lease. Immutable input files are never installation targets.
    pub fn repair(
        mut self,
        private_root: &Path,
        _memory: MemoryPermit,
    ) -> EngineResult<NamespaceOutcome> {
        let mut completed = BTreeSet::new();
        let mut published = BTreeMap::new();
        loop {
            self.job.options.cancel.check()?;
            self.validate_outputs()?;
            self.assess()?;
            let ready: Vec<_> = self
                .job
                .sets
                .iter()
                .filter_map(|(id, set)| {
                    let status = set.view.as_ref()?.status;
                    (!completed.contains(id)
                        && matches!(status, RepairStatus::Ready | RepairStatus::Complete))
                    .then_some((*id, status))
                })
                .collect();
            if ready.is_empty() {
                break;
            }
            for (id, status) in ready {
                if status == RepairStatus::Ready {
                    let layout = self
                        .job
                        .sets
                        .get_mut(&id)
                        .unwrap()
                        .native
                        .layout()?
                        .ok_or(EngineError::InvalidState("missing namespace output layout"))?;
                    for file in layout.files().iter().filter(|file| {
                        file.extents.iter().any(|extent| {
                            matches!(extent.kind, par3_rs::layout::ExtentKind::Unprotected)
                        })
                    }) {
                        let destination = private_root.join(&file.path);
                        if !destination.exists() {
                            let source = self.input_paths.get(&file.path).ok_or(
                                EngineError::InvalidState("embedded namespace source absent"),
                            )?;
                            if let Some(parent) = destination.parent() {
                                std::fs::create_dir_all(parent)?;
                            }
                            std::fs::copy(source, destination)?;
                        }
                    }
                    self.job.repair(id, private_root)?;
                }
                let layout = self
                    .job
                    .sets
                    .get_mut(&id)
                    .unwrap()
                    .native
                    .layout()?
                    .ok_or(EngineError::InvalidState("missing namespace output layout"))?;
                let outputs: Vec<_> = layout
                    .files()
                    .iter()
                    .map(|file| (file.path.clone(), file.len))
                    .collect();
                for (name, length) in outputs {
                    let path = private_root.join(&name);
                    // Complete immutable donors also become private outputs.
                    // Copying makes the next generation independent of their
                    // source ownership without granting repair write access.
                    if !path.exists() {
                        let source =
                            self.input_paths
                                .get(&name)
                                .ok_or(EngineError::InvalidState(
                                    "verified namespace output absent",
                                ))?;
                        if let Some(parent) = path.parent() {
                            std::fs::create_dir_all(parent)?;
                        }
                        std::fs::copy(source, &path)?;
                    }
                    if std::fs::symlink_metadata(&path)?.len() != length {
                        return Err(EngineError::InvalidState("namespace output length changed"));
                    }
                    let source = match published.get(&name) {
                        Some(source) => *source,
                        None => {
                            if self.next_source >= MAX_CARRIERS as u64 {
                                return Err(budget::host_limit("namespace PAR3 source count"));
                            }
                            let source = SourceId(self.next_source);
                            self.next_source += 1;
                            published.insert(name.clone(), source);
                            source
                        }
                    };
                    self.job.publish_file(
                        source,
                        path,
                        name,
                        std::iter::once(0..length).collect(),
                    )?;
                }
                completed.insert(id);
            }
        }
        // A later set can share destinations. Evidence is always re-read
        // from the final private generation after every installation.
        self.assess()?;
        let mut verified = BTreeMap::new();
        for id in &completed {
            let set = self.job.sets.get_mut(id).unwrap();
            if set.view.as_ref().map(|view| view.status) != Some(RepairStatus::Complete) {
                return Err(EngineError::InvalidState(
                    "namespace repair invalidated verified output",
                ));
            }
            let layout = set.native.layout()?.ok_or(EngineError::InvalidState(
                "missing verified namespace layout",
            ))?;
            for file in layout.files() {
                verified.insert(PathBuf::from(&file.path), file.len);
            }
        }
        Ok(NamespaceOutcome {
            unresolved_sets: self.job.sets.len() - completed.len(),
            verified: verified
                .into_iter()
                .map(|(path, length)| VerifiedMember { path, length })
                .collect(),
        })
    }
}

fn check_path(name: &str) -> EngineResult<()> {
    paths::check_install_path(name)
        .map_err(|_| EngineError::InvalidState("unsafe namespace recovery path"))?;
    ExtractionRoot::parse_relative_path(name)
        .map_err(|_| EngineError::InvalidState("unsafe namespace recovery path"))?;
    if name.split('/').any(|part| part.ends_with(['.', ' '])) {
        return Err(EngineError::InvalidState(
            "noncanonical namespace recovery path",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
