use super::StreamOwner;
use crate::pipeline::extraction::safety::{MemoryPermit, ProcessMemoryBudget};
use crate::pipeline::repair::{par2, par3};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
pub(crate) enum RepairFormat {
    Par2,
    Par3,
}

#[derive(Clone, Default)]
pub(crate) struct RepairCancellation {
    par2: par2_rs::CancellationToken,
    par3: par3_rs::runtime::CancellationToken,
}

impl RepairCancellation {
    pub(crate) fn cancel(&self) {
        self.par2.cancel();
        self.par3.cancel();
    }
}

pub(crate) struct NamespaceInput {
    pub(crate) path: PathBuf,
    pub(crate) name: String,
    pub(crate) ranges: Vec<std::ops::Range<u64>>,
}

// Dispatch receives the exact effective input generation selected by the
// session owner. A repair worker has no job, assembly, or catalog identity.
pub(crate) struct NamespaceRepairRequest {
    pub(crate) owner: StreamOwner,
    pub(crate) format: RepairFormat,
    pub(crate) inputs: Vec<NamespaceInput>,
    pub(crate) output: PathBuf,
    pub(crate) donors: PathBuf,
    pub(crate) output_limit: u64,
    pub(crate) process: Arc<ProcessMemoryBudget>,
    pub(crate) cancellation: RepairCancellation,
    pub(crate) memory: MemoryPermit,
}

pub(crate) struct VerifiedMember {
    pub(crate) path: PathBuf,
    pub(crate) length: u64,
}

pub(crate) struct NamespaceRepairOutcome {
    #[cfg(test)]
    pub(crate) owner: StreamOwner,
    pub(crate) verified: Vec<VerifiedMember>,
    #[cfg(test)]
    pub(crate) unresolved_sets: usize,
}

impl NamespaceRepairRequest {
    // Run only on an admitted blocking worker. The owner must retire/join
    // the previous decoder before accepting this output as a new generation.
    pub(crate) fn execute(self) -> Result<NamespaceRepairOutcome, String> {
        self.validate_private_roots()?;
        let (verified, unresolved_sets) = match self.format {
            RepairFormat::Par2 => {
                let carriers: Vec<_> = self.inputs.iter().map(|input| input.path.clone()).collect();
                let operation = par2::namespace::NamespaceRepair::scan(
                    &carriers,
                    self.process,
                    self.cancellation.par2,
                    self.output_limit,
                )
                .map_err(|error| error.to_string())?;
                let outcome = operation
                    .repair(&self.output, &self.donors, carriers, self.memory)
                    .map_err(|error| error.to_string())?;
                (
                    outcome
                        .verified
                        .into_iter()
                        .map(|member| VerifiedMember {
                            path: member.path,
                            length: member.length,
                        })
                        .collect(),
                    outcome.unresolved_sets,
                )
            }
            RepairFormat::Par3 => {
                let inputs = self
                    .inputs
                    .into_iter()
                    .map(|input| par3::namespace::NamespaceInput {
                        path: input.path,
                        name: input.name,
                        ranges: input.ranges,
                    })
                    .collect();
                let operation = par3::namespace::NamespaceRepair::scan(
                    inputs,
                    self.output_limit,
                    self.memory.bytes(),
                    self.cancellation.par3,
                )
                .map_err(|error| error.to_string())?;
                let outcome = operation
                    .repair(&self.output, self.memory)
                    .map_err(|error| error.to_string())?;
                (
                    outcome
                        .verified
                        .into_iter()
                        .map(|member| VerifiedMember {
                            path: member.path,
                            length: member.length,
                        })
                        .collect(),
                    outcome.unresolved_sets,
                )
            }
        };
        tracing::debug!(owner = ?self.owner, unresolved_sets, "scoped archive recovery completed");
        Ok(NamespaceRepairOutcome {
            #[cfg(test)]
            owner: self.owner,
            verified,
            #[cfg(test)]
            unresolved_sets,
        })
    }

    fn validate_private_roots(&self) -> Result<(), String> {
        let mut roots = Vec::new();
        {
            let root = &self.output;
            let metadata = std::fs::symlink_metadata(root).map_err(|error| error.to_string())?;
            if !metadata.file_type().is_dir()
                || std::fs::read_dir(root)
                    .map_err(|error| error.to_string())?
                    .next()
                    .is_some()
            {
                return Err("namespace repair requires empty private directories".into());
            }
            roots.push(root.canonicalize().map_err(|error| error.to_string())?);
        }
        match std::fs::symlink_metadata(&self.donors) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => return Err("namespace donor scratch already exists".into()),
        }
        let parent = self
            .donors
            .parent()
            .ok_or("namespace donor scratch has no parent")?
            .canonicalize()
            .map_err(|error| error.to_string())?;
        let name = self
            .donors
            .file_name()
            .ok_or("namespace donor scratch has no name")?;
        roots.push(parent.join(name));
        if roots[0].starts_with(&roots[1]) || roots[1].starts_with(&roots[0]) {
            return Err("namespace repair directories overlap".into());
        }
        let mut names = std::collections::BTreeSet::new();
        for input in &self.inputs {
            par3::paths::check_install_path(&input.name).map_err(|error| error.to_string())?;
            if !names.insert(input.name.to_lowercase()) {
                return Err("namespace repair input names alias".into());
            }
            let metadata =
                std::fs::symlink_metadata(&input.path).map_err(|error| error.to_string())?;
            if !metadata.file_type().is_file() {
                return Err("namespace repair input is not an immutable regular file".into());
            }
            let path = input
                .path
                .canonicalize()
                .map_err(|error| error.to_string())?;
            if roots.iter().any(|root| path.starts_with(root)) {
                return Err("namespace repair input overlaps writable output".into());
            }
            let mut previous = 0;
            for range in &input.ranges {
                if range.start < previous || range.start > range.end || range.end > metadata.len() {
                    return Err("namespace repair input ranges are invalid".into());
                }
                previous = range.end;
            }
        }
        Ok(())
    }
}
