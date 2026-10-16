use std::collections::BTreeMap;

use super::*;
use crate::pipeline::extraction::safety::{ExtractionRoot, MemoryPermit};

#[derive(Debug, thiserror::Error)]
pub(in crate::pipeline) enum NamespaceError {
    #[error("namespace repair cancelled")]
    Cancelled,
    #[error("namespace repair exceeded its resource admission")]
    ResourceLimited,
    #[error("{0}")]
    Invalid(String),
    #[error(transparent)]
    Scan(#[from] par2_rs::Par2Error),
    #[error(transparent)]
    Session(#[from] par2_rs::Par2SessionError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl From<String> for NamespaceError {
    fn from(value: String) -> Self {
        Self::Invalid(value)
    }
}

impl From<&str> for NamespaceError {
    fn from(value: &str) -> Self {
        Self::Invalid(value.to_owned())
    }
}

// An operation owns its parser reservations. No session, packet, source index,
// or child identity is installed in the download job's repair runtime.
pub(in crate::pipeline) struct NamespaceRepair {
    sets: Vec<Par2FileSet>,
    incomplete_metadata: Vec<[u8; 16]>,
    _scan_budget: SharedPar2ScanBudget,
    cancellation: par2_rs::CancellationToken,
    output_limit: u64,
}

pub(in crate::pipeline) struct NamespaceOutcome {
    pub(crate) verified: Vec<VerifiedMember>,
    pub(crate) unresolved_sets: usize,
    #[cfg(test)]
    pub(crate) incomplete_metadata: Vec<[u8; 16]>,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(in crate::pipeline) struct VerifiedMember {
    pub(crate) path: PathBuf,
    pub(crate) length: u64,
    pub(crate) hash_full: [u8; 16],
}

impl NamespaceRepair {
    // Carriers must be immutable private copies for this operation's lifetime:
    // authenticated recovery packets retain file-backed extents into them.
    // Inventory is explicit; discovery never walks an ancestor's namespace.
    pub(crate) fn scan(
        carriers: &[PathBuf],
        process: Arc<ProcessMemoryBudget>,
        cancellation: par2_rs::CancellationToken,
        output_limit: u64,
    ) -> Result<Self, NamespaceError> {
        if cancellation.is_cancelled() {
            return Err(NamespaceError::Cancelled);
        }
        let budget = Arc::new(std::sync::Mutex::new(Par2ScanBudget::new(
            par2_rs::PacketScanLimits::default()
                .with_max_examined_packets(1_048_576)
                .with_max_retained_packets(262_144)
                .with_max_retained_metadata_bytes(256 * 1024 * 1024),
            process,
            cancellation.clone(),
        )));
        let mut groups = BTreeMap::<_, Vec<_>>::new();
        for path in carriers {
            for group in scan_completed_par2_packet_groups(path, &budget)? {
                if !groups.contains_key(group.set_id.as_bytes())
                    && groups.len() >= MAX_PAR2_RECOVERY_SETS
                {
                    return Err("too many recovery sets in one namespace".into());
                }
                groups
                    .entry(*group.set_id.as_bytes())
                    .or_default()
                    .extend(group.packets);
            }
        }
        let mut sets = Vec::with_capacity(groups.len());
        let mut incomplete_metadata = Vec::new();
        let mut claims = BTreeMap::new();
        let mut aliases = BTreeMap::new();
        let mut total = 0u64;
        for (id, packets) in groups {
            let set = match Par2FileSet::from_packets(packets) {
                Ok(set) => set,
                Err(par2_rs::Par2Error::NoMainPacket) => {
                    incomplete_metadata.push(id);
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            if set.recovery_set_id.as_bytes() != &id {
                return Err("recovery set identity changed during packet merge".into());
            }
            validate_par2_geometry(&set, output_limit)?;
            for description in set.files.values() {
                super::super::par3::paths::check_install_path(&description.filename)
                    .map_err(|error| error.to_string())?;
                let relative = ExtractionRoot::parse_relative_path(&description.filename)?;
                // Native repair uses this name itself. Do not accept a spelling
                // that only becomes safe after host normalization.
                if relative != Path::new(&description.filename)
                    || description.filename.contains('\\')
                    || description.filename.contains(':')
                    || description
                        .filename
                        .split('/')
                        .any(|part| part.ends_with(['.', ' ']))
                {
                    return Err("noncanonical recovery output path".into());
                }
                let alias = description.filename.to_lowercase();
                if let Some(previous) = aliases.insert(alias, relative.clone())
                    && previous != relative
                {
                    return Err("recovery output paths alias each other".into());
                }
                let identity = (description.length, description.hash_full);
                match claims.entry(relative) {
                    std::collections::btree_map::Entry::Occupied(entry) => {
                        if *entry.get() != identity {
                            return Err("recovery sets disagree about an output path".into());
                        }
                    }
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        total = total
                            .checked_add(description.length)
                            .filter(|bytes| *bytes <= output_limit)
                            .ok_or("namespace recovery output exceeds admitted size")?;
                        entry.insert(identity);
                    }
                }
            }
            sets.push(set);
        }
        for path in aliases.keys() {
            if path
                .match_indices('/')
                .any(|(index, _)| aliases.contains_key(&path[..index]))
            {
                return Err("recovery output file collides with an output directory".into());
            }
        }
        Ok(Self {
            sets,
            incomplete_metadata,
            _scan_budget: budget,
            cancellation,
            output_limit,
        })
    }

    // The coordinator supplies an exclusively owned scratch namespace and
    // admits its disk/worker resources before calling this blocking operation.
    // Original archive backings must never be passed as this writable root.
    // The memory lease remains live through every native session and scan.
    pub(crate) fn repair(
        self,
        private_root: &Path,
        donor_root: &Path,
        candidates: Vec<PathBuf>,
        memory: MemoryPermit,
    ) -> Result<NamespaceOutcome, NamespaceError> {
        let bytes = usize::try_from(memory.bytes()).map_err(|_| "repair memory limit overflow")?;
        if bytes < 64 * 1024 {
            return Err(NamespaceError::ResourceLimited);
        }
        let mut candidates: std::collections::BTreeSet<_> = candidates.into_iter().collect();
        if candidates.len() > weaver_nzb::parser::MAX_NZB_FILES {
            return Err(NamespaceError::ResourceLimited);
        }
        let open = |set: &Par2FileSet,
                    candidates: &[PathBuf]|
         -> Result<par2_rs::Par2RepairSession, NamespaceError> {
            if self.cancellation.is_cancelled() {
                return Err(NamespaceError::Cancelled);
            }
            let mut options =
                par2_rs::Par2RepairSessionOptions::new(private_root.to_path_buf(), Vec::new());
            options.file_set = Some(set.clone());
            options.discover_extras = false;
            options.extra_paths = candidates.to_vec();
            options.memory_limit = Some(bytes / 2);
            options.retained_state_limit = bytes / 2;
            options.cancel = Some(self.cancellation.clone());
            par2_rs::Par2RepairSession::open(options).map_err(NamespaceError::Session)
        };
        let mut complete = vec![false; self.sets.len()];
        loop {
            let mut changed = false;
            for (index, set) in self.sets.iter().enumerate() {
                if complete[index] {
                    continue;
                }
                // The native engine may consume complete extra donors. Never
                // let that retire an output another set has already verified.
                let donors = donors::Copies::create(
                    private_root,
                    donor_root,
                    &candidates,
                    self.output_limit,
                    &self.cancellation,
                )?;
                let mut session = open(set, &donors.paths)?;
                let assessment = session.analyze()?;
                let status = if assessment.status == par2_rs::Par2RepairStatus::RepairPossible {
                    session.repair()?.status
                } else {
                    assessment.status
                };
                match status {
                    par2_rs::Par2RepairStatus::Verified | par2_rs::Par2RepairStatus::Repaired => {
                        complete[index] = true;
                        changed = true;
                        for description in set
                            .recovery_file_ids
                            .iter()
                            .filter_map(|id| set.files.get(id))
                        {
                            candidates.insert(private_root.join(&description.filename));
                            if candidates.len() > weaver_nzb::parser::MAX_NZB_FILES {
                                return Err(NamespaceError::ResourceLimited);
                            }
                        }
                    }
                    par2_rs::Par2RepairStatus::Insufficient => {}
                    par2_rs::Par2RepairStatus::ResourceLimited => {
                        return Err(NamespaceError::ResourceLimited);
                    }
                    par2_rs::Par2RepairStatus::RepairPossible => {
                        return Err("namespace repair did not verify its output".into());
                    }
                }
            }
            // Each changed round completes at least one formerly unresolved
            // set. No set is repaired twice, so this is bounded by set count.
            if !changed || complete.iter().all(|done| *done) {
                break;
            }
        }
        let mut outcome = NamespaceOutcome {
            verified: Vec::new(),
            unresolved_sets: complete.iter().filter(|done| !**done).count()
                + self.incomplete_metadata.len(),
            #[cfg(test)]
            incomplete_metadata: self.incomplete_metadata.clone(),
        };
        // Later sets can write shared destinations. Only the final namespace,
        // re-read after all writes, supplies evidence for publication.
        for (set, done) in self.sets.iter().zip(complete) {
            if done {
                let mut session = open(set, &[])?;
                if session.analyze()?.status != par2_rs::Par2RepairStatus::Verified {
                    return Err("namespace repair invalidated an earlier verified output".into());
                }
                outcome.verified.extend(
                    set.recovery_file_ids
                        .iter()
                        .filter_map(|id| set.files.get(id))
                        .map(|description| VerifiedMember {
                            path: PathBuf::from(&description.filename),
                            length: description.length,
                            hash_full: description.hash_full,
                        }),
                );
            }
        }
        outcome.verified.sort();
        outcome.verified.dedup();
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests;

mod donors;
