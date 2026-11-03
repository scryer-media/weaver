// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashMap;

use crate::jobs::ids::{JobId, NzbFileId};
use weaver_model::files::FileRole;

use super::file::FileAssembly;

// Aggregates file assemblies for an entire download job.
pub struct JobAssembly {
    job_id: JobId,
    files: HashMap<NzbFileId, FileAssembly>,
    has_par3_candidates: bool,
}

impl JobAssembly {
    pub fn new(job_id: JobId) -> Self {
        Self {
            job_id,
            files: HashMap::new(),
            has_par3_candidates: false,
        }
    }

    // Add a file to track.
    pub fn add_file(&mut self, assembly: FileAssembly) {
        self.has_par3_candidates |= matches!(assembly.role(), FileRole::Par3 { .. });
        self.files.insert(assembly.file_id(), assembly);
    }

    // Whether admission saw a declared PAR3 carrier. This is a discovery hint,
    // retained across renaming; it does not establish a set or its capacity.
    pub fn has_par3_candidates(&self) -> bool {
        self.has_par3_candidates
    }

    // Get a file assembly by id (mutable).
    pub fn file_mut(&mut self, file_id: NzbFileId) -> Option<&mut FileAssembly> {
        self.files.get_mut(&file_id)
    }

    // Get a file assembly by id (immutable).
    pub fn file(&self, file_id: NzbFileId) -> Option<&FileAssembly> {
        self.files.get(&file_id)
    }

    // Overall job progress (average of file progresses, weighted by bytes).
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

    // Total optional recovery bytes and how many of those bytes were received.
    //
    // Protection-only jobs treat recovery bytes as required, so both values return 0.
    pub fn optional_recovery_bytes(&self) -> (u64, u64) {
        let has_payload_files = self
            .files
            .values()
            .any(|file| file.role().counts_toward_health());
        if !has_payload_files {
            return (0, 0);
        }

        self.files.values().fold((0u64, 0u64), |acc, file| {
            if file.role().is_recovery() {
                (
                    acc.0.saturating_add(file.total_bytes()),
                    acc.1.saturating_add(file.received_bytes()),
                )
            } else {
                acc
            }
        })
    }

    // Number of complete files (all types).
    pub fn complete_file_count(&self) -> usize {
        self.files.values().filter(|f| f.is_complete()).count()
    }

    // Total number of files (all types).
    pub fn total_file_count(&self) -> usize {
        self.files.len()
    }

    // Iterator over all files.
    pub fn files(&self) -> impl Iterator<Item = &FileAssembly> {
        self.files.values()
    }

    // Mutable iterator over all files.
    pub fn files_mut(&mut self) -> impl Iterator<Item = &mut FileAssembly> {
        self.files.values_mut()
    }

    // The job id.
    pub fn job_id(&self) -> JobId {
        self.job_id
    }
}

#[cfg(test)]
mod tests;
