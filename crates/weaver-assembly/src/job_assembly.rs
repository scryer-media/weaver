use std::collections::{HashMap, HashSet};

use weaver_core::classify::FileRole;
use weaver_core::id::{JobId, NzbFileId};

use crate::file_assembly::FileAssembly;

/// Aggregates file assemblies for an entire download job.
pub struct JobAssembly {
    job_id: JobId,
    files: HashMap<NzbFileId, FileAssembly>,

    /// Archive topologies keyed by archive set name (e.g., "Show.S01E01.7z").
    /// Supports multiple independent archive sets per job (e.g., season packs).
    archive_topologies: HashMap<String, ArchiveTopology>,
}

/// Type of archive detected in a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveType {
    Rar,
    SevenZip,
}

/// Archive topology for extraction readiness.
pub struct ArchiveTopology {
    /// What kind of archive this topology describes.
    pub archive_type: ArchiveType,
    /// Maps filename -> volume number.
    pub volume_map: HashMap<String, u32>,
    /// Volume numbers that are fully downloaded and verified.
    pub complete_volumes: HashSet<u32>,
    /// Total expected volume count (if known).
    pub expected_volume_count: Option<u32>,
    /// Archive members and which volumes they span.
    pub members: Vec<ArchiveMember>,
}

impl ArchiveTopology {
    /// Returns volume numbers that can be safely deleted because every member
    /// that uses them has been extracted. `extracted` is the set of member names
    /// that completed extraction successfully.
    pub fn deletable_volumes(&self, extracted: &HashSet<String>) -> HashSet<u32> {
        // For each volume, check if ALL members spanning it have been extracted.
        // A volume is deletable only when no unextracted member needs it.
        let max_vol = self.expected_volume_count
            .unwrap_or_else(|| self.volume_map.values().max().copied().map_or(0, |v| v + 1));

        let mut deletable: HashSet<u32> = (0..max_vol).collect();

        for member in &self.members {
            if !extracted.contains(&member.name) {
                // This member still needs its volumes — remove them from deletable.
                for v in member.first_volume..=member.last_volume {
                    deletable.remove(&v);
                }
            }
        }

        deletable
    }
}

/// A member (file) within an archive set.
pub struct ArchiveMember {
    pub name: String,
    /// First volume this member starts in.
    pub first_volume: u32,
    /// Last volume this member ends in.
    pub last_volume: u32,
    /// Unpacked size in bytes.
    pub unpacked_size: u64,
}

/// Whether the job's archives are ready for extraction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtractionReadiness {
    /// No archive detected (standalone files only).
    NotApplicable,
    /// Some members can be extracted now.
    Partial {
        extractable: Vec<String>,
        waiting_on: Vec<String>,
    },
    /// All volumes are ready, full extraction possible.
    Ready,
    /// Cannot proceed without repair or missing data.
    Blocked { reason: String },
}

impl JobAssembly {
    pub fn new(job_id: JobId) -> Self {
        Self {
            job_id,
            files: HashMap::new(),
            archive_topologies: HashMap::new(),
        }
    }

    /// Add a file to track.
    pub fn add_file(&mut self, assembly: FileAssembly) {
        self.files.insert(assembly.file_id(), assembly);
    }

    /// Get a file assembly by id (mutable).
    pub fn file_mut(&mut self, file_id: NzbFileId) -> Option<&mut FileAssembly> {
        self.files.get_mut(&file_id)
    }

    /// Get a file assembly by id (immutable).
    pub fn file(&self, file_id: NzbFileId) -> Option<&FileAssembly> {
        self.files.get(&file_id)
    }

    /// Set archive topology for a named set.
    pub fn set_archive_topology(&mut self, set_name: String, topology: ArchiveTopology) {
        self.archive_topologies.insert(set_name, topology);
    }

    /// Mark a volume as complete and verified within a named set.
    pub fn mark_volume_complete(&mut self, set_name: &str, volume_number: u32) {
        if let Some(topo) = self.archive_topologies.get_mut(set_name) {
            topo.complete_volumes.insert(volume_number);
        }
    }

    /// Check aggregate extraction readiness across all archive sets.
    ///
    /// - `Ready` if all sets are individually ready.
    /// - `NotApplicable` if no archive files exist.
    /// - `Blocked` if any set is blocked and none are ready.
    /// - `Partial` if some sets are ready, others not.
    pub fn extraction_readiness(&self) -> ExtractionReadiness {
        if self.archive_topologies.is_empty() {
            // Check if there are archive files without any topology yet.
            let has_archive = self.files.values().any(|f| {
                matches!(
                    f.role(),
                    FileRole::RarVolume { .. }
                        | FileRole::SevenZipArchive
                        | FileRole::SevenZipSplit { .. }
                )
            });
            if has_archive {
                return ExtractionReadiness::Blocked {
                    reason: "archive topology not yet available".into(),
                };
            }
            return ExtractionReadiness::NotApplicable;
        }

        // Check if all archive files appear in some topology's volume_map.
        let all_archive_files_covered = self.files.values().all(|f| {
            match f.role() {
                FileRole::RarVolume { .. } | FileRole::SevenZipArchive | FileRole::SevenZipSplit { .. } => {
                    self.archive_topologies.values().any(|topo| topo.volume_map.contains_key(f.filename()))
                }
                _ => true,
            }
        });
        if !all_archive_files_covered {
            return ExtractionReadiness::Blocked {
                reason: "archive topology not yet available for all sets".into(),
            };
        }

        // Single set: delegate directly for backward compatibility
        // (preserves per-member Partial for RAR batch extraction).
        if self.archive_topologies.len() == 1 {
            let set_name = self.archive_topologies.keys().next().unwrap();
            return self.set_extraction_readiness(set_name);
        }

        // Multiple sets: aggregate across all.
        let mut all_ready = true;
        let mut any_ready = false;
        let mut extractable = Vec::new();
        let mut waiting_on = Vec::new();

        for set_name in self.archive_topologies.keys() {
            match self.set_extraction_readiness(set_name) {
                ExtractionReadiness::Ready => {
                    any_ready = true;
                    extractable.push(set_name.clone());
                }
                ExtractionReadiness::NotApplicable => {}
                _ => {
                    all_ready = false;
                    waiting_on.push(set_name.clone());
                }
            }
        }

        if all_ready && any_ready {
            ExtractionReadiness::Ready
        } else if any_ready {
            ExtractionReadiness::Partial {
                extractable,
                waiting_on,
            }
        } else {
            ExtractionReadiness::Blocked {
                reason: "no archive sets are complete yet".into(),
            }
        }
    }

    /// Check extraction readiness for a single named archive set.
    pub fn set_extraction_readiness(&self, set_name: &str) -> ExtractionReadiness {
        let topo = match self.archive_topologies.get(set_name) {
            Some(t) => t,
            None => {
                return ExtractionReadiness::Blocked {
                    reason: format!("no topology for set '{set_name}'"),
                };
            }
        };

        // If we know the expected volume count, check for missing volumes.
        if let Some(expected) = topo.expected_volume_count {
            let all_complete = (0..expected).all(|v| topo.complete_volumes.contains(&v));
            if all_complete {
                return ExtractionReadiness::Ready;
            }
        } else if !topo.members.is_empty() {
            let max_from_members = topo
                .members
                .iter()
                .map(|m| m.last_volume)
                .max()
                .unwrap_or(0);
            let max_from_map = topo.volume_map.values().max().copied().unwrap_or(0);
            let max_volume = max_from_members.max(max_from_map);
            let all_complete = (0..=max_volume).all(|v| topo.complete_volumes.contains(&v));
            if all_complete {
                return ExtractionReadiness::Ready;
            }
        }

        // Check which members are extractable.
        let mut extractable = Vec::new();
        let mut waiting = Vec::new();

        for member in &topo.members {
            let all_volumes_ready = (member.first_volume..=member.last_volume)
                .all(|v| topo.complete_volumes.contains(&v));
            if all_volumes_ready {
                extractable.push(member.name.clone());
            } else {
                waiting.push(member.name.clone());
            }
        }

        if waiting.is_empty() && !extractable.is_empty() {
            ExtractionReadiness::Ready
        } else if !extractable.is_empty() {
            ExtractionReadiness::Partial {
                extractable,
                waiting_on: waiting,
            }
        } else {
            ExtractionReadiness::Blocked {
                reason: "no volumes are complete yet".into(),
            }
        }
    }

    /// Return names of archive sets that are fully ready for extraction.
    pub fn ready_archive_sets(&self) -> Vec<String> {
        self.archive_topologies
            .keys()
            .filter(|name| matches!(self.set_extraction_readiness(name), ExtractionReadiness::Ready))
            .cloned()
            .collect()
    }

    /// Check if streaming extraction can start for any member (RAR only).
    ///
    /// Returns `Some((member_name, first_volume))` when the first volume of
    /// a member is complete. This allows extraction to begin immediately,
    /// blocking on subsequent volumes as they arrive.
    pub fn streaming_extraction_ready(&self) -> Option<(String, u32)> {
        // Streaming extraction only applies to RAR — find the first RAR topology.
        for topo in self.archive_topologies.values() {
            if topo.archive_type != ArchiveType::Rar {
                continue;
            }
            for member in &topo.members {
                if topo.complete_volumes.contains(&member.first_volume) {
                    return Some((member.name.clone(), member.first_volume));
                }
            }
        }
        None
    }

    /// Get the first archive topology (backward-compat convenience for single-set jobs).
    pub fn archive_topology(&self) -> Option<&ArchiveTopology> {
        self.archive_topologies.values().next()
    }

    /// Get a specific archive set's topology.
    pub fn archive_topology_for(&self, set_name: &str) -> Option<&ArchiveTopology> {
        self.archive_topologies.get(set_name)
    }

    /// Get a mutable reference to a specific archive set's topology.
    pub fn archive_topology_for_mut(&mut self, set_name: &str) -> Option<&mut ArchiveTopology> {
        self.archive_topologies.get_mut(set_name)
    }

    /// Get all archive topologies.
    pub fn archive_topologies(&self) -> &HashMap<String, ArchiveTopology> {
        &self.archive_topologies
    }

    /// Get all archive topologies (mutable).
    pub fn archive_topologies_mut(&mut self) -> &mut HashMap<String, ArchiveTopology> {
        &mut self.archive_topologies
    }

    /// Overall job progress (average of file progresses, weighted by bytes).
    pub fn progress(&self) -> f64 {
        let total_bytes: u64 = self.files.values().map(|f| f.total_bytes()).sum();
        if total_bytes == 0 {
            return 1.0;
        }

        let weighted_progress: f64 = self
            .files
            .values()
            .map(|f| f.progress() * f.total_bytes() as f64)
            .sum();

        weighted_progress / total_bytes as f64
    }

    /// Number of complete files (all types).
    pub fn complete_file_count(&self) -> usize {
        self.files.values().filter(|f| f.is_complete()).count()
    }

    /// Total number of files (all types).
    pub fn total_file_count(&self) -> usize {
        self.files.len()
    }

    /// Number of data files (excludes PAR2 recovery volumes).
    /// Includes PAR2 index, archive volumes, standalone files.
    pub fn data_file_count(&self) -> usize {
        self.files.values().filter(|f| !matches!(f.role(), FileRole::Par2 { is_index: false, .. })).count()
    }

    /// Number of complete data files (excludes PAR2 recovery volumes).
    pub fn complete_data_file_count(&self) -> usize {
        self.files.values().filter(|f| {
            !matches!(f.role(), FileRole::Par2 { is_index: false, .. }) && f.is_complete()
        }).count()
    }

    /// Iterator over all files.
    pub fn files(&self) -> impl Iterator<Item = &FileAssembly> {
        self.files.values()
    }

    /// The job id.
    pub fn job_id(&self) -> JobId {
        self.job_id
    }
}
