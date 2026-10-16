use super::{RuntimeError, StreamOwner};
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
    pub(crate) fn execute(self) -> Result<NamespaceRepairOutcome, RuntimeError> {
        self.validate_private_roots()?;
        let owner = self.owner;
        let (verified, unresolved_sets) = match self.format {
            RepairFormat::Par2 => self.repair_par2()?,
            RepairFormat::Par3 => self.repair_par3()?,
        };
        tracing::debug!(owner = ?owner, unresolved_sets, "scoped archive recovery completed");
        Ok(NamespaceRepairOutcome {
            #[cfg(test)]
            owner,
            verified,
            #[cfg(test)]
            unresolved_sets,
        })
    }

    fn repair_par2(self) -> Result<(Vec<VerifiedMember>, usize), RuntimeError> {
        let carriers: Vec<_> = self.inputs.iter().map(|input| input.path.clone()).collect();
        let outcome = par2::namespace::NamespaceRepair::scan(
            &carriers,
            self.process,
            self.cancellation.par2,
            self.output_limit,
        )
        .map_err(RuntimeError::refused("namespace PAR2 scan"))?
        .repair(&self.output, &self.donors, carriers, self.memory)
        .map_err(RuntimeError::refused("namespace PAR2 repair"))?;
        let verified = outcome
            .verified
            .into_iter()
            .map(|member| VerifiedMember {
                path: member.path,
                length: member.length,
            })
            .collect();
        Ok((verified, outcome.unresolved_sets))
    }

    fn repair_par3(self) -> Result<(Vec<VerifiedMember>, usize), RuntimeError> {
        let inputs = self
            .inputs
            .into_iter()
            .map(|input| par3::namespace::NamespaceInput {
                path: input.path,
                name: input.name,
                ranges: input.ranges,
            })
            .collect();
        let outcome = par3::namespace::NamespaceRepair::scan(
            inputs,
            self.output_limit,
            self.memory.bytes(),
            self.cancellation.par3,
        )
        .map_err(RuntimeError::refused("namespace PAR3 scan"))?
        .repair(&self.output, self.memory)
        .map_err(RuntimeError::refused("namespace PAR3 repair"))?;
        let verified = outcome
            .verified
            .into_iter()
            .map(|member| VerifiedMember {
                path: member.path,
                length: member.length,
            })
            .collect();
        Ok((verified, outcome.unresolved_sets))
    }

    fn validate_private_roots(&self) -> Result<(), RuntimeError> {
        let output = self.validated_output_root()?;
        let donors = self.validated_donor_root()?;
        if output.starts_with(&donors) || donors.starts_with(&output) {
            return Err(RuntimeError::Invariant(
                "namespace repair directories overlap",
            ));
        }
        self.validate_inputs(&[output, donors])
    }

    // The output must be an existing, empty, real directory.
    fn validated_output_root(&self) -> Result<PathBuf, RuntimeError> {
        let metadata = std::fs::symlink_metadata(&self.output)
            .map_err(RuntimeError::io("namespace repair output"))?;
        let empty = metadata.file_type().is_dir()
            && std::fs::read_dir(&self.output)
                .map_err(RuntimeError::io("namespace repair output"))?
                .next()
                .is_none();
        if !empty {
            return Err(RuntimeError::Invariant(
                "namespace repair requires empty private directories",
            ));
        }
        self.output
            .canonicalize()
            .map_err(RuntimeError::io("namespace repair output"))
    }

    // The donor scratch must not exist yet; its parent must.
    fn validated_donor_root(&self) -> Result<PathBuf, RuntimeError> {
        match std::fs::symlink_metadata(&self.donors) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => {
                return Err(RuntimeError::Invariant(
                    "namespace donor scratch already exists",
                ));
            }
        }
        let parent = self
            .donors
            .parent()
            .ok_or(RuntimeError::Invariant(
                "namespace donor scratch has no parent",
            ))?
            .canonicalize()
            .map_err(RuntimeError::io("namespace donor scratch"))?;
        let name = self.donors.file_name().ok_or(RuntimeError::Invariant(
            "namespace donor scratch has no name",
        ))?;
        Ok(parent.join(name))
    }

    fn validate_inputs(&self, writable: &[PathBuf]) -> Result<(), RuntimeError> {
        let mut names = std::collections::BTreeSet::new();
        for input in &self.inputs {
            par3::paths::check_install_path(&input.name)
                .map_err(RuntimeError::refused("namespace repair input name"))?;
            if !names.insert(input.name.to_lowercase()) {
                return Err(RuntimeError::Invariant(
                    "namespace repair input names alias",
                ));
            }
            let metadata = std::fs::symlink_metadata(&input.path)
                .map_err(RuntimeError::io("namespace repair input"))?;
            if !metadata.file_type().is_file() {
                return Err(RuntimeError::Invariant(
                    "namespace repair input is not an immutable regular file",
                ));
            }
            let path = input
                .path
                .canonicalize()
                .map_err(RuntimeError::io("namespace repair input"))?;
            if writable.iter().any(|root| path.starts_with(root)) {
                return Err(RuntimeError::Invariant(
                    "namespace repair input overlaps writable output",
                ));
            }
            if !ranges_fit(&input.ranges, metadata.len()) {
                return Err(RuntimeError::Invariant(
                    "namespace repair input ranges are invalid",
                ));
            }
        }
        Ok(())
    }
}

// Ranges must be ordered, non-overlapping and inside the file.
fn ranges_fit(ranges: &[std::ops::Range<u64>], length: u64) -> bool {
    let mut previous = 0;
    ranges.iter().all(|range| {
        let fits = range.start >= previous && range.start <= range.end && range.end <= length;
        previous = range.end;
        fits
    })
}
