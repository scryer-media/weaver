use std::collections::{HashMap, HashSet};

use weaver_core::classify::FileRole;
use weaver_core::id::{JobId, NzbFileId};

use crate::file_assembly::FileAssembly;

/// Aggregates file assemblies for an entire download job.
pub struct JobAssembly {
    job_id: JobId,
    files: HashMap<NzbFileId, FileAssembly>,

    /// PAR2 metadata (populated once PAR2 index is parsed).
    par2_metadata: Option<Par2Metadata>,

    /// Archive topology (populated as RAR headers are parsed).
    archive_topology: Option<ArchiveTopology>,
}

/// PAR2 metadata needed for verification.
pub struct Par2Metadata {
    pub slice_size: u64,
    pub recovery_block_count: u32,
    /// Maps filename -> slice checksums for that file.
    pub file_checksums: HashMap<String, Vec<(u32, [u8; 16])>>,
}

/// RAR archive topology for extraction readiness.
pub struct ArchiveTopology {
    /// Maps filename -> RAR volume number.
    pub volume_map: HashMap<String, u32>,
    /// Volume numbers that are fully downloaded and verified.
    pub complete_volumes: HashSet<u32>,
    /// Total expected volume count (if known).
    pub expected_volume_count: Option<u32>,
    /// Archive members and which volumes they span.
    pub members: Vec<ArchiveMember>,
}

/// A member (file) within a RAR archive set.
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
            par2_metadata: None,
            archive_topology: None,
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

    /// Set PAR2 metadata. This attaches slice checksums to all matching files.
    pub fn set_par2_metadata(&mut self, metadata: Par2Metadata) {
        let slice_size = metadata.slice_size;

        // Attach checksums to matching files by filename.
        for file in self.files.values_mut() {
            if let Some(checksums) = metadata.file_checksums.get(file.filename()) {
                file.attach_par2_metadata(slice_size, checksums);
            }
        }

        self.par2_metadata = Some(metadata);
    }

    /// Get PAR2 metadata if loaded.
    pub fn par2_metadata(&self) -> Option<&Par2Metadata> {
        self.par2_metadata.as_ref()
    }

    /// Set archive topology.
    pub fn set_archive_topology(&mut self, topology: ArchiveTopology) {
        self.archive_topology = Some(topology);
    }

    /// Mark a volume as complete and verified.
    pub fn mark_volume_complete(&mut self, volume_number: u32) {
        if let Some(ref mut topo) = self.archive_topology {
            topo.complete_volumes.insert(volume_number);
        }
    }

    /// Check extraction readiness.
    pub fn extraction_readiness(&self) -> ExtractionReadiness {
        let topo = match &self.archive_topology {
            Some(t) => t,
            None => {
                // Check if there are any RAR volumes at all.
                let has_rar = self.files.values().any(|f| {
                    matches!(f.role(), FileRole::RarVolume { .. })
                });
                if has_rar {
                    // RAR files exist but no topology parsed yet.
                    return ExtractionReadiness::Blocked {
                        reason: "archive topology not yet available".into(),
                    };
                }
                return ExtractionReadiness::NotApplicable;
            }
        };

        // If we know the expected volume count, check for missing volumes.
        if let Some(expected) = topo.expected_volume_count {
            let all_complete = (0..expected).all(|v| topo.complete_volumes.contains(&v));
            if all_complete {
                return ExtractionReadiness::Ready;
            }
        } else if !topo.members.is_empty() {
            // Determine required volumes from members.
            let max_volume = topo
                .members
                .iter()
                .map(|m| m.last_volume)
                .max()
                .unwrap_or(0);
            let all_complete = (0..=max_volume).all(|v| topo.complete_volumes.contains(&v));
            if all_complete {
                return ExtractionReadiness::Ready;
            }
        }

        // Check which members are extractable (all their volumes are complete).
        let mut extractable = Vec::new();
        let mut waiting_on = Vec::new();

        for member in &topo.members {
            let all_volumes_ready = (member.first_volume..=member.last_volume)
                .all(|v| topo.complete_volumes.contains(&v));
            if all_volumes_ready {
                extractable.push(member.name.clone());
            } else {
                waiting_on.push(member.name.clone());
            }
        }

        if waiting_on.is_empty() && !extractable.is_empty() {
            ExtractionReadiness::Ready
        } else if !extractable.is_empty() {
            ExtractionReadiness::Partial {
                extractable,
                waiting_on,
            }
        } else {
            ExtractionReadiness::Blocked {
                reason: "no volumes are complete yet".into(),
            }
        }
    }

    /// Check if streaming extraction can start for any member.
    ///
    /// Returns `Some((member_name, first_volume))` when the first volume of
    /// a member is complete. This allows extraction to begin immediately,
    /// blocking on subsequent volumes as they arrive.
    pub fn streaming_extraction_ready(&self) -> Option<(String, u32)> {
        let topo = self.archive_topology.as_ref()?;
        for member in &topo.members {
            if topo.complete_volumes.contains(&member.first_volume) {
                return Some((member.name.clone(), member.first_volume));
            }
        }
        None
    }

    /// Get the archive topology, if available.
    pub fn archive_topology(&self) -> Option<&ArchiveTopology> {
        self.archive_topology.as_ref()
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

    /// Number of complete files.
    pub fn complete_file_count(&self) -> usize {
        self.files.values().filter(|f| f.is_complete()).count()
    }

    /// Total number of files.
    pub fn total_file_count(&self) -> usize {
        self.files.len()
    }

    /// Repair confidence: (damaged_slices, total_verified_slices, recovery_blocks_available).
    /// Returns None if no PAR2 metadata is available.
    pub fn repair_confidence(&self) -> Option<(u32, u32, u32)> {
        let metadata = self.par2_metadata.as_ref()?;

        let mut damaged = 0u32;
        let mut total_verified = 0u32;

        for file in self.files.values() {
            damaged += file.damaged_slice_count();
            total_verified += file.verified_slice_count();
        }

        // Include damaged slices in total count.
        total_verified += damaged;

        Some((damaged, total_verified, metadata.recovery_block_count))
    }

    /// Get all PAR2 filenames (index + recovery volumes) for re-reading during repair.
    pub fn par2_filenames(&self) -> Vec<&str> {
        self.files
            .values()
            .filter(|f| matches!(f.role(), FileRole::Par2 { .. }))
            .map(|f| f.filename())
            .collect()
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
