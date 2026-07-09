use super::rar_state::{self, RarDerivedPlan, RarSetPhase, RarSetState};
use crate::jobs::assembly::{
    ArchiveMember as JobArchiveMember, ArchivePendingSpan as JobArchivePendingSpan,
    ArchiveTopology as JobArchiveTopology, ArchiveType,
};
use crate::jobs::ids::{JobId, NzbFileId};
use crate::pipeline::{
    ComputedRarSetState, Pipeline, RarCapacityRetryKind, RarRefreshDone, RarRefreshError,
    RarRefreshRequest, RefreshReason,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use tracing::{debug, error, info, warn};

fn open_rar_volume_file(path: &Path) -> std::io::Result<Box<dyn weaver_unrar::ReadSeek>> {
    #[cfg(test)]
    {
        rar_refresh_open_tracking::open(path)
    }
    #[cfg(not(test))]
    {
        Ok(Box::new(std::fs::File::open(path)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::rar_state::RarSetState;
    use std::io::Cursor;
    use weaver_par2::checksum;

    const TEST_RAR5_SIG: [u8; 8] = [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00];

    fn encode_test_rar_vint(mut value: u64) -> Vec<u8> {
        let mut result = Vec::new();
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            result.push(byte);
            if value == 0 {
                break;
            }
        }
        result
    }

    fn build_test_rar_header(
        header_type: u64,
        common_flags: u64,
        type_body: &[u8],
        extra: &[u8],
    ) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_test_rar_vint(header_type));

        let mut flags = common_flags;
        if !extra.is_empty() {
            flags |= 0x0001;
        }
        body.extend_from_slice(&encode_test_rar_vint(flags));
        if !extra.is_empty() {
            body.extend_from_slice(&encode_test_rar_vint(extra.len() as u64));
        }
        body.extend_from_slice(type_body);
        body.extend_from_slice(extra);

        let header_size = body.len() as u64;
        let header_size_bytes = encode_test_rar_vint(header_size);
        let crc = checksum::crc32(&[header_size_bytes.as_slice(), body.as_slice()].concat());

        let mut result = Vec::new();
        result.extend_from_slice(&crc.to_le_bytes());
        result.extend_from_slice(&header_size_bytes);
        result.extend_from_slice(&body);
        result
    }

    fn build_test_rar_main_header(archive_flags: u64, volume_number: Option<u64>) -> Vec<u8> {
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_test_rar_vint(archive_flags));
        if let Some(volume_number) = volume_number {
            type_body.extend_from_slice(&encode_test_rar_vint(volume_number));
        }
        build_test_rar_header(1, 0, &type_body, &[])
    }

    fn build_test_rar_end_header(more_volumes: bool) -> Vec<u8> {
        let end_flags: u64 = if more_volumes { 0x0001 } else { 0 };
        build_test_rar_header(5, 0, &encode_test_rar_vint(end_flags), &[])
    }

    fn build_test_rar_file_header(
        filename: &str,
        common_flags_extra: u64,
        data_size: u64,
        unpacked_size: u64,
        data_crc: Option<u32>,
    ) -> Vec<u8> {
        let file_flags: u64 = if data_crc.is_some() { 0x0004 } else { 0 };
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_test_rar_vint(file_flags));
        type_body.extend_from_slice(&encode_test_rar_vint(unpacked_size));
        type_body.extend_from_slice(&encode_test_rar_vint(0o644));
        if let Some(data_crc) = data_crc {
            type_body.extend_from_slice(&data_crc.to_le_bytes());
        }
        type_body.extend_from_slice(&encode_test_rar_vint(0));
        type_body.extend_from_slice(&encode_test_rar_vint(1));
        type_body.extend_from_slice(&encode_test_rar_vint(filename.len() as u64));
        type_body.extend_from_slice(filename.as_bytes());

        let mut body = Vec::new();
        body.extend_from_slice(&encode_test_rar_vint(2));
        body.extend_from_slice(&encode_test_rar_vint(0x0002 | common_flags_extra));
        body.extend_from_slice(&encode_test_rar_vint(data_size));
        body.extend_from_slice(&type_body);

        let header_size = body.len() as u64;
        let header_size_bytes = encode_test_rar_vint(header_size);
        let crc = checksum::crc32(&[header_size_bytes.as_slice(), body.as_slice()].concat());

        let mut result = Vec::new();
        result.extend_from_slice(&crc.to_le_bytes());
        result.extend_from_slice(&header_size_bytes);
        result.extend_from_slice(&body);
        result
    }

    fn build_many_volume_rar_set(volume_count: usize) -> Vec<(String, Vec<u8>)> {
        assert!(volume_count >= 2);
        let filename = "big.bin";
        let payload = vec![b'x'; volume_count];
        let payload_crc = checksum::crc32(&payload);

        (0..volume_count)
            .map(|volume| {
                let mut bytes = Vec::new();
                bytes.extend_from_slice(&TEST_RAR5_SIG);
                bytes.extend_from_slice(&build_test_rar_main_header(
                    if volume == 0 { 0x0001 } else { 0x0001 | 0x0002 },
                    (volume > 0).then_some(volume as u64),
                ));

                let split_flags = match volume {
                    0 => 0x0010,
                    v if v + 1 == volume_count => 0x0008,
                    _ => 0x0010 | 0x0008,
                };
                bytes.extend_from_slice(&build_test_rar_file_header(
                    filename,
                    split_flags,
                    1,
                    payload.len() as u64,
                    (volume + 1 == volume_count).then_some(payload_crc),
                ));
                bytes.push(payload[volume]);
                bytes.extend_from_slice(&build_test_rar_end_header(volume + 1 != volume_count));

                (format!("show.part{volume:03}.rar"), bytes)
            })
            .collect()
    }

    #[test]
    fn rar_refresh_compute_bounds_live_source_readers_for_many_volumes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let volume_count = 260usize;
        let files = build_many_volume_rar_set(volume_count);

        let first_archive = weaver_unrar::RarArchive::open(Cursor::new(files[0].1.clone()))
            .expect("volume 0 should parse");
        let cached_headers = first_archive.serialize_headers();

        let mut volume_map = HashMap::new();
        let mut volume_paths = BTreeMap::new();
        let mut facts = BTreeMap::new();
        for (volume, (filename, bytes)) in files.iter().enumerate() {
            let path = temp_dir.path().join(filename);
            std::fs::write(&path, bytes).unwrap();
            volume_map.insert(filename.clone(), volume as u32);
            volume_paths.insert(volume as u32, path);
            facts.insert(
                volume as u32,
                weaver_unrar::RarArchive::parse_volume_facts(Cursor::new(bytes.clone()), None)
                    .expect("synthetic RAR volume facts should parse"),
            );
        }

        let input = RarSetComputeInput {
            job_id: JobId(99),
            set_name: "show".to_string(),
            existing: RarSetState::default(),
            volume_map,
            volume_paths,
            password_candidates: Vec::new(),
            extracted: HashSet::new(),
            failed: HashSet::new(),
            facts,
            verified_suspect_volumes: HashSet::new(),
            worker_active: false,
            cached_headers: Some(cached_headers),
            extraction_generation: 0,
            reason: RefreshReason::CoverageExpansion,
        };

        let _tracking = rar_refresh_open_tracking::start();
        let computed = Pipeline::compute_rar_set_state_blocking(input)
            .expect("bounded refresh compute should integrate all volumes");

        assert!(
            rar_refresh_open_tracking::peak() <= 1,
            "refresh retained {} live source readers",
            rar_refresh_open_tracking::peak()
        );
        assert_eq!(
            computed.plan.topology.complete_volumes.len(),
            volume_count,
            "bounded refresh should preserve complete-volume coverage"
        );
        assert_eq!(
            computed
                .plan
                .ready_members
                .iter()
                .map(|member| member.name.as_str())
                .collect::<Vec<_>>(),
            vec!["big.bin"]
        );
    }
}

#[cfg(test)]
mod rar_refresh_open_tracking {
    use std::cell::Cell;
    use std::io::{Read, Seek};
    use std::path::Path;

    thread_local! {
        static ENABLED: Cell<bool> = const { Cell::new(false) };
        static ACTIVE: Cell<usize> = const { Cell::new(0) };
        static PEAK: Cell<usize> = const { Cell::new(0) };
    }

    pub(super) struct Guard {
        previous: bool,
    }

    pub(super) fn start() -> Guard {
        let previous = ENABLED.with(|enabled| {
            let previous = enabled.get();
            enabled.set(true);
            previous
        });
        ACTIVE.with(|active| active.set(0));
        PEAK.with(|peak| peak.set(0));
        Guard { previous }
    }

    pub(super) fn peak() -> usize {
        PEAK.with(Cell::get)
    }

    pub(super) fn open(path: &Path) -> std::io::Result<Box<dyn weaver_unrar::ReadSeek>> {
        let file = std::fs::File::open(path)?;
        let counted = ENABLED.with(Cell::get);
        if counted {
            ACTIVE.with(|active| {
                let next = active.get() + 1;
                active.set(next);
                PEAK.with(|peak| peak.set(peak.get().max(next)));
            });
        }
        Ok(Box::new(TrackedReader { file, counted }))
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            ENABLED.with(|enabled| enabled.set(self.previous));
        }
    }

    struct TrackedReader {
        file: std::fs::File,
        counted: bool,
    }

    impl Drop for TrackedReader {
        fn drop(&mut self) {
            if self.counted {
                ACTIVE.with(|active| active.set(active.get().saturating_sub(1)));
            }
        }
    }

    impl Read for TrackedReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.file.read(buf)
        }
    }

    impl Seek for TrackedReader {
        fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
            self.file.seek(pos)
        }
    }
}

pub(crate) type ArchiveMember = JobArchiveMember;
pub(crate) type ArchivePendingSpan = JobArchivePendingSpan;
pub(crate) type ArchiveTopology = JobArchiveTopology;

#[derive(Clone, Copy)]
pub(crate) enum RarTopologyRebuildSource {
    CachedHeaders,
    VolumeZero,
}

impl RarTopologyRebuildSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::CachedHeaders => "cached-headers",
            Self::VolumeZero => "volume-0",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::pipeline) enum CachedRarSnapshotAlignment {
    Consistent,
    StaleGrowth,
    CompletedGrowthRequiresRefresh,
    TrueConflict,
}

pub(in crate::pipeline) fn cached_rar_snapshot_alignment_with_volume_facts(
    facts: &BTreeMap<u32, weaver_unrar::RarVolumeFacts>,
    archive: &weaver_unrar::RarArchive,
) -> CachedRarSnapshotAlignment {
    let metadata = archive.metadata();
    let mut saw_stale_growth = false;
    let mut saw_completed_growth = false;

    for (&first_volume, volume_facts) in facts {
        for member in &volume_facts.members {
            if member.is_directory || member.split_before || !member.split_after {
                continue;
            }

            let name = weaver_unrar::sanitize_path(&member.name);
            let mut observed_last = first_volume;
            let mut observed_complete = false;
            let mut next_volume = first_volume + 1;

            while let Some(next_facts) = facts.get(&next_volume) {
                let Some(next_member) = next_facts.members.iter().find(|candidate| {
                    !candidate.is_directory
                        && candidate.split_before
                        && weaver_unrar::sanitize_path(&candidate.name) == name
                }) else {
                    break;
                };

                observed_last = next_volume;
                if !next_member.split_after {
                    observed_complete = true;
                    break;
                }
                next_volume += 1;
            }

            if observed_last <= first_volume {
                continue;
            }

            let Some(candidate) = metadata.members.iter().find(|candidate| {
                candidate.name == name && candidate.volumes.first_volume as u32 == first_volume
            }) else {
                return CachedRarSnapshotAlignment::TrueConflict;
            };

            let cached_last = candidate.volumes.last_volume as u32;
            if cached_last >= observed_last {
                continue;
            }

            if observed_complete {
                saw_completed_growth = true;
            } else {
                saw_stale_growth = true;
            }
        }
    }

    if saw_completed_growth {
        CachedRarSnapshotAlignment::CompletedGrowthRequiresRefresh
    } else if saw_stale_growth {
        CachedRarSnapshotAlignment::StaleGrowth
    } else {
        CachedRarSnapshotAlignment::Consistent
    }
}

fn cached_rar_plan_is_incoherent(
    plan: &RarDerivedPlan,
    failed: &HashSet<String>,
    worker_active: bool,
    facts: &BTreeMap<u32, weaver_unrar::RarVolumeFacts>,
    volume_paths: &BTreeMap<u32, PathBuf>,
) -> bool {
    matches!(plan.phase, RarSetPhase::WaitingForVolumes)
        && plan.waiting_on_volumes.is_empty()
        && plan.ready_members.is_empty()
        && failed.is_empty()
        && !worker_active
        && !volume_paths.is_empty()
        && volume_paths.keys().all(|volume| facts.contains_key(volume))
        && facts.values().any(|volume_facts| {
            volume_facts
                .members
                .iter()
                .any(|member| !member.is_directory || member.data_size > 0)
        })
}

pub(in crate::pipeline) fn ownerless_present_member_volumes(
    plan: &RarDerivedPlan,
    facts: &BTreeMap<u32, weaver_unrar::RarVolumeFacts>,
) -> Vec<u32> {
    let mut volumes: Vec<u32> = plan
        .delete_decisions
        .iter()
        .filter_map(|(volume, decision)| {
            let has_named_member = facts.get(volume).is_some_and(|volume_facts| {
                volume_facts.members.iter().any(|member| {
                    (!member.is_directory || member.data_size > 0)
                        && !weaver_unrar::sanitize_path(&member.name).is_empty()
                })
            });
            (decision.owners.is_empty() && has_named_member).then_some(*volume)
        })
        .collect();
    volumes.sort_unstable();
    volumes
}

fn present_waiting_rar_volumes(
    plan: &RarDerivedPlan,
    facts: &BTreeMap<u32, weaver_unrar::RarVolumeFacts>,
    volume_paths: &BTreeMap<u32, PathBuf>,
) -> Vec<u32> {
    let mut volumes: Vec<u32> = plan
        .waiting_on_volumes
        .iter()
        .copied()
        .filter(|volume| facts.contains_key(volume) && volume_paths.contains_key(volume))
        .collect();
    volumes.sort_unstable();
    volumes.dedup();
    volumes
}

const INCOHERENT_RAR_WAITING_STATE_ERROR_MARKER: &str =
    "produced incoherent waiting state with no missing volumes after rebuild";
const OWNERLESS_RAR_PLAN_ERROR_MARKER: &str =
    "produced ownerless present RAR volumes after rebuild";

#[derive(Clone)]
struct RarSetComputeInput {
    job_id: JobId,
    set_name: String,
    existing: RarSetState,
    volume_map: HashMap<String, u32>,
    volume_paths: BTreeMap<u32, PathBuf>,
    password_candidates: Vec<crate::jobs::ArchivePasswordCandidate>,
    extracted: HashSet<String>,
    failed: HashSet<String>,
    facts: BTreeMap<u32, weaver_unrar::RarVolumeFacts>,
    verified_suspect_volumes: HashSet<u32>,
    worker_active: bool,
    cached_headers: Option<Vec<u8>>,
    extraction_generation: u64,
    reason: RefreshReason,
}

pub(in crate::pipeline) fn is_incoherent_rar_waiting_state_error(error: &str) -> bool {
    error.contains(INCOHERENT_RAR_WAITING_STATE_ERROR_MARKER)
}

pub(in crate::pipeline) fn is_ownerless_rar_plan_error(error: &str) -> bool {
    error.contains(OWNERLESS_RAR_PLAN_ERROR_MARKER)
}

pub(in crate::pipeline) fn ownerless_rar_plan_error(set_name: &str, volumes: &[u32]) -> String {
    format!("RAR set '{set_name}' {OWNERLESS_RAR_PLAN_ERROR_MARKER}: {volumes:?}")
}

impl Pipeline {
    fn register_rar_volume_file(state: &mut RarSetState, logical_volume: u32, filename: String) {
        state.volume_files.retain(|volume, existing_filename| {
            *existing_filename != filename || *volume == logical_volume
        });
        state.volume_files.insert(logical_volume, filename);
    }

    fn ownerless_rar_volumes(plan: &RarDerivedPlan) -> Vec<u32> {
        plan.delete_decisions
            .iter()
            .filter_map(|(volume, decision)| decision.owners.is_empty().then_some(*volume))
            .collect()
    }

    pub(crate) async fn parse_rar_volume_facts_from_path(
        path: PathBuf,
        password_candidates: Vec<crate::jobs::ArchivePasswordCandidate>,
    ) -> Result<weaver_unrar::RarVolumeFacts, String> {
        tokio::task::spawn_blocking(move || {
            let context = format!("failed to parse RAR volume facts from {}", path.display());
            Self::try_rar_password_candidates(&context, &password_candidates, |password| {
                let file = std::fs::File::open(&path).map_err(|e| {
                    crate::pipeline::RarPasswordAttemptError::Fatal(format!(
                        "failed to open {}: {e}",
                        path.display()
                    ))
                })?;
                weaver_unrar::RarArchive::parse_volume_facts(file, password)
                    .map_err(crate::pipeline::RarPasswordAttemptError::from)
            })
            .map(|selection| selection.value)
        })
        .await
        .map_err(|e| format!("RAR facts parser task panicked: {e}"))?
    }

    pub(in crate::pipeline::archive) fn persist_rar_volume_facts(
        &mut self,
        job_id: JobId,
        set_name: &str,
        filename: &str,
        observed_volume: Option<u32>,
        facts: weaver_unrar::RarVolumeFacts,
    ) -> Result<bool, String> {
        if let Some(expected_volume) = observed_volume
            && facts.volume_number != expected_volume
        {
            info!(
                job_id = job_id.0,
                set_name = %set_name,
                filename = %filename,
                expected_volume,
                parsed_volume = facts.volume_number,
                "RAR facts parse detected filename-to-volume mismatch"
            );
        }

        let state_key = (job_id, set_name.to_string());
        let unchanged = self.rar_sets.get(&state_key).is_some_and(|state| {
            state.facts.get(&facts.volume_number) == Some(&facts)
                && state.volume_files.get(&facts.volume_number) == Some(&filename.to_string())
        });
        if unchanged {
            return Ok(false);
        }

        let encoded = rmp_serde::to_vec_named(&facts)
            .map_err(|error| format!("failed to encode RAR volume facts: {error}"))?;
        self.db
            .save_rar_volume_facts(job_id, set_name, facts.volume_number, &encoded)
            .map_err(|error| format!("failed to persist RAR volume facts: {error}"))?;

        let state = self.rar_sets.entry(state_key).or_default();
        Self::register_rar_volume_file(state, facts.volume_number, filename.to_string());
        state.facts.insert(facts.volume_number, facts);

        Ok(true)
    }

    pub(in crate::pipeline) fn rar_volume_filename(
        volume_map: &HashMap<String, u32>,
        volume: u32,
    ) -> Option<&str> {
        volume_map.iter().find_map(|(filename, volume_number)| {
            (*volume_number == volume).then_some(filename.as_str())
        })
    }

    pub(in crate::pipeline) fn is_rar_volume_deleted(
        &self,
        job_id: JobId,
        volume_map: &HashMap<String, u32>,
        volume: u32,
    ) -> bool {
        let Some(filename) = Self::rar_volume_filename(volume_map, volume) else {
            return false;
        };
        self.eagerly_deleted
            .get(&job_id)
            .is_some_and(|deleted| deleted.contains(filename))
    }

    pub(in crate::pipeline) fn volume_paths_for_rar_set(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> std::collections::BTreeMap<u32, PathBuf> {
        let mut volume_paths = std::collections::BTreeMap::new();
        let Some(state) = self.jobs.get(&job_id) else {
            return volume_paths;
        };

        if let Some(rar_state) = self.rar_sets.get(&(job_id, set_name.to_string())) {
            for (logical_volume, filename) in &rar_state.volume_files {
                if let Some(path) = self.resolve_job_input_path(job_id, filename)
                    && path.exists()
                {
                    volume_paths.insert(*logical_volume, path);
                }
            }
        }

        for file_asm in state.assembly.files() {
            let weaver_model::files::FileRole::RarVolume { volume_number } =
                self.classified_role_for_file(job_id, file_asm)
            else {
                continue;
            };
            if self
                .classified_archive_set_name_for_file(job_id, file_asm)
                .as_deref()
                != Some(set_name)
                || !file_asm.is_complete()
            {
                continue;
            }
            let current_filename = self.current_filename_for_file(job_id, file_asm);
            if let Some(path) = self.resolve_job_input_path(job_id, &current_filename)
                && path.exists()
            {
                volume_paths.entry(volume_number).or_insert(path);
            }
        }

        volume_paths
    }

    fn apply_rar_plan(&mut self, job_id: JobId, set_name: &str, plan: RarDerivedPlan) {
        let key = (job_id, set_name.to_string());
        let old_phase = self.rar_sets.get(&key).map(|state| state.phase);
        let old_eligible = self
            .rar_sets
            .get(&key)
            .and_then(|state| state.plan.as_ref())
            .map(|plan| plan.deletion_eligible.clone())
            .unwrap_or_default();
        let verified_suspect_volumes = self
            .rar_sets
            .get(&key)
            .map(|state| state.verified_suspect_volumes.clone())
            .unwrap_or_default();

        if let Some(state) = self.jobs.get_mut(&job_id) {
            state
                .assembly
                .set_archive_topology(set_name.to_string(), plan.topology.clone());
        }

        if let Some(state) = self.rar_sets.get_mut(&key) {
            state.phase = plan.phase;
            state.plan = Some(plan.clone());
        }

        if old_phase != Some(plan.phase) || old_eligible != plan.deletion_eligible {
            info!(
                job_id = job_id.0,
                set_name = %set_name,
                phase = plan.phase.as_str(),
                solid = plan.is_solid,
                known_volumes = plan.topology.complete_volumes.len(),
                ready_members = plan.ready_members.len(),
                waiting_on = ?plan.waiting_on_volumes,
                ownership_eligible = ?plan.deletion_eligible,
                verified_suspect_volumes = ?verified_suspect_volumes,
                "RAR set recomputed"
            );
        }

        for member in &plan.ready_members {
            debug!(
                job_id = job_id.0,
                set_name = %set_name,
                member = %member.name,
                "RAR member ready"
            );
        }
        for volume in &plan.waiting_on_volumes {
            debug!(
                job_id = job_id.0,
                set_name = %set_name,
                volume,
                "RAR set waiting on volume"
            );
        }
        for (volume, decision) in &plan.delete_decisions {
            debug!(
                job_id = job_id.0,
                set_name = %set_name,
                volume,
                owners = ?decision.owners,
                clean_owners = ?decision.clean_owners,
                failed_owners = ?decision.failed_owners,
                pending_owners = ?decision.pending_owners,
                unresolved_boundary = decision.unresolved_boundary,
                ownership_eligible = decision.ownership_eligible,
                claim_clean = Self::claim_clean_rar_volume(decision),
                verified_suspect = verified_suspect_volumes.contains(volume),
                deleted = self.is_rar_volume_deleted(job_id, &plan.topology.volume_map, *volume),
                "RAR volume ownership audit"
            );
        }
        if let Some(reason) = &plan.fallback_reason {
            warn!(
                job_id = job_id.0,
                set_name = %set_name,
                error = %reason,
                "RAR set entered fallback full-set mode"
            );
        }
        self.mark_rar_unlock_priorities_dirty(job_id);
    }

    pub(in crate::pipeline) async fn recompute_rar_set_state(
        &mut self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<(), String> {
        let Some(input) =
            self.prepare_rar_set_compute_input(job_id, set_name, RefreshReason::ValidationFailure)
        else {
            return Ok(());
        };
        let existing = input.existing.clone();
        match Self::run_rar_set_compute(input).await {
            Ok(computed) => {
                self.apply_computed_rar_set_state(job_id, set_name, computed);
                Ok(())
            }
            Err(error) => {
                self.apply_failed_rar_set_compute(job_id, set_name, existing, error.to_string())
            }
        }
    }

    fn prepare_rar_set_compute_input(
        &self,
        job_id: JobId,
        set_name: &str,
        reason: RefreshReason,
    ) -> Option<RarSetComputeInput> {
        let key = (job_id, set_name.to_string());
        let existing = self.rar_sets.get(&key).cloned()?;
        let volume_map = self.build_rar_volume_map(job_id, set_name);
        if volume_map.is_empty() {
            return None;
        }
        let volume_paths = self.volume_paths_for_rar_set(job_id, set_name);
        let password_candidates = self.archive_password_candidates_for_set(job_id, set_name);
        let extracted = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let failed = self
            .failed_extractions
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let facts = existing.facts.clone();
        let verified_suspect_volumes = existing.verified_suspect_volumes.clone();
        let worker_active = existing.active_workers > 0;
        let extraction_generation = existing.extraction_generation;
        let cached_headers = self.load_rar_snapshot(job_id, set_name);

        Some(RarSetComputeInput {
            job_id,
            set_name: set_name.to_string(),
            existing,
            volume_map,
            volume_paths,
            password_candidates,
            extracted,
            failed,
            facts,
            verified_suspect_volumes,
            worker_active,
            cached_headers,
            extraction_generation,
            reason,
        })
    }

    async fn run_rar_set_compute(
        input: RarSetComputeInput,
    ) -> Result<ComputedRarSetState, RarRefreshError> {
        tokio::task::spawn_blocking(move || Self::compute_rar_set_state_blocking(input))
            .await
            .map_err(|e| RarRefreshError::Other(format!("RAR plan task panicked: {e}")))?
            .map_err(RarRefreshError::from_message)
    }

    fn compute_rar_set_state_blocking(
        input: RarSetComputeInput,
    ) -> Result<ComputedRarSetState, String> {
        let RarSetComputeInput {
            job_id,
            set_name: set_name_owned,
            volume_map,
            volume_paths,
            password_candidates,
            extracted,
            failed,
            facts,
            verified_suspect_volumes,
            worker_active,
            cached_headers,
            reason,
            ..
        } = input;

        let open_from_volume_zero =
            || -> Result<crate::pipeline::ArchivePasswordSelection<weaver_unrar::RarArchive>, String> {
            let first_path = volume_paths.get(&0).ok_or_else(|| {
                format!(
                    "RAR set '{set_name_owned}' cannot be rebuilt without cached headers or volume 0"
                )
            })?;
            Self::open_rar_volume_zero_with_password_candidates(
                &set_name_owned,
                first_path,
                &password_candidates,
                std::sync::Arc::new(weaver_unrar::crypto::KdfCache::new()),
            )
        };

        let attach_volumes_and_build_plan = |mut archive: weaver_unrar::RarArchive,
                                             rebuild_source: RarTopologyRebuildSource,
                                             using_cached_headers: bool,
                                             force_refresh_all_volumes: bool|
         -> Result<_, String> {
            for (volume_number, path) in &volume_paths {
                let volume_needs_refresh =
                    force_refresh_all_volumes || !facts.contains_key(volume_number);
                if archive.has_volume(*volume_number as usize) && !volume_needs_refresh {
                    archive.attach_volume_reader(
                        *volume_number as usize,
                        Box::new(std::io::Cursor::new(Vec::<u8>::new())),
                    );
                    continue;
                }

                let volume_file = match open_rar_volume_file(path) {
                    Ok(file) => file,
                    Err(error)
                        if using_cached_headers && error.kind() == std::io::ErrorKind::NotFound =>
                    {
                        continue;
                    }
                    Err(error) => {
                        let context = format!(
                            "failed to open RAR volume {volume_number} for set '{set_name_owned}'"
                        );
                        return Err(crate::pipeline::capacity::format_fd_capacity_error(
                            &context, &error,
                        ));
                    }
                };
                if using_cached_headers
                    && volume_needs_refresh
                    && archive.has_volume(*volume_number as usize)
                {
                    archive
                        .refresh_volume(*volume_number as usize, volume_file)
                        .map_err(|e| {
                            format!(
                                "failed to refresh cached RAR volume {volume_number} for set '{set_name_owned}': {e}"
                            )
                        })?;
                } else if archive.has_volume(*volume_number as usize) {
                    archive.attach_volume_reader(*volume_number as usize, volume_file);
                } else {
                    archive
                        .add_volume(*volume_number as usize, volume_file)
                        .map_err(|e| {
                            format!(
                                "failed to integrate RAR volume {volume_number} for set '{set_name_owned}': {e}"
                            )
                        })?;
                }
                // Refresh planning is metadata-only. Keep presence visible to
                // the planner without retaining one source FD per volume.
                archive.attach_volume_reader(
                    *volume_number as usize,
                    Box::new(std::io::Cursor::new(Vec::<u8>::new())),
                );
            }

            let plan = rar_state::build_plan(
                volume_map.clone(),
                &facts,
                &archive,
                &extracted,
                &failed,
                worker_active,
            )?;

            Ok((
                plan,
                archive.serialize_headers(),
                rebuild_source,
                using_cached_headers,
            ))
        };

        let mut rebuild_source = if cached_headers.is_some() {
            RarTopologyRebuildSource::CachedHeaders
        } else {
            RarTopologyRebuildSource::VolumeZero
        };
        let has_cached_headers = cached_headers.is_some();
        let mut using_cached_headers = has_cached_headers;
        let mut force_refresh_all_volumes = false;
        let initial_selection = match cached_headers {
            Some(headers) => {
                match Self::deserialize_rar_headers_with_password_candidates(
                    &set_name_owned,
                    &headers,
                    &password_candidates,
                    std::sync::Arc::new(weaver_unrar::crypto::KdfCache::new()),
                ) {
                    Ok(selection) => selection,
                    Err(error) => {
                        warn!(
                            set_name = %set_name_owned,
                            error = %error,
                            "failed to deserialize cached RAR headers during plan rebuild, falling back to volume 0"
                        );
                        rebuild_source = RarTopologyRebuildSource::VolumeZero;
                        using_cached_headers = false;
                        open_from_volume_zero()?
                    }
                }
            }
            None => open_from_volume_zero()?,
        };
        let mut archive = initial_selection.value;

        if using_cached_headers
            && reason >= RefreshReason::ValidationFailure
            && verified_suspect_volumes.iter().any(|volume| {
                volume_paths.contains_key(volume) && !archive.has_volume(*volume as usize)
            })
            && volume_paths.contains_key(&0)
        {
            let selection = open_from_volume_zero()?;
            archive = selection.value;
            rebuild_source = RarTopologyRebuildSource::VolumeZero;
            using_cached_headers = false;
        }

        if using_cached_headers {
            match cached_rar_snapshot_alignment_with_volume_facts(&facts, &archive) {
                CachedRarSnapshotAlignment::Consistent => {}
                CachedRarSnapshotAlignment::StaleGrowth => {
                    debug!(
                        set_name = %set_name_owned,
                        "cached RAR headers lag in-progress tail growth; deferring registered volume refresh"
                    );
                }
                CachedRarSnapshotAlignment::CompletedGrowthRequiresRefresh => {
                    force_refresh_all_volumes = true;
                    debug!(
                        set_name = %set_name_owned,
                        "cached RAR headers lag completed member spans; refreshing registered volumes"
                    );
                }
                CachedRarSnapshotAlignment::TrueConflict => {
                    force_refresh_all_volumes = true;
                    warn!(
                        set_name = %set_name_owned,
                        "cached RAR headers contradicted per-volume facts; forcing registered volume refresh"
                    );
                    if volume_paths.contains_key(&0) {
                        let selection = open_from_volume_zero()?;
                        archive = selection.value;
                        rebuild_source = RarTopologyRebuildSource::VolumeZero;
                        using_cached_headers = false;
                    }
                }
            }
        }

        let (mut plan, mut headers, mut rebuild_source, mut used_cached_headers) =
            attach_volumes_and_build_plan(
                archive,
                rebuild_source,
                using_cached_headers,
                force_refresh_all_volumes,
            )?;

        let present_waiting_volumes = present_waiting_rar_volumes(&plan, &facts, &volume_paths);
        if used_cached_headers
            && !present_waiting_volumes.is_empty()
            && volume_paths.contains_key(&0)
        {
            let mut waiting_on: Vec<u32> = plan.waiting_on_volumes.iter().copied().collect();
            waiting_on.sort_unstable();
            warn!(
                job_id = job_id.0,
                set_name = %set_name_owned,
                waiting_on = ?waiting_on,
                present_waiting_volumes = ?present_waiting_volumes,
                "cached RAR headers waited on present volumes; retrying from live volumes"
            );

            let selection = open_from_volume_zero()?;
            (plan, headers, rebuild_source, used_cached_headers) = attach_volumes_and_build_plan(
                selection.value,
                RarTopologyRebuildSource::VolumeZero,
                false,
                false,
            )?;
        }

        let ownerless_volumes = ownerless_present_member_volumes(&plan, &facts);
        if used_cached_headers && !ownerless_volumes.is_empty() && volume_paths.contains_key(&0) {
            warn!(
                set_name = %set_name_owned,
                volumes = ?ownerless_volumes,
                "cached RAR headers produced ownerless present volumes; retrying from live volumes"
            );

            let selection = open_from_volume_zero()?;
            (plan, headers, rebuild_source, used_cached_headers) = attach_volumes_and_build_plan(
                selection.value,
                RarTopologyRebuildSource::VolumeZero,
                false,
                false,
            )?;
        }

        if used_cached_headers
            && cached_rar_plan_is_incoherent(&plan, &failed, worker_active, &facts, &volume_paths)
            && volume_paths.contains_key(&0)
        {
            warn!(
                set_name = %set_name_owned,
                "cached RAR headers produced incoherent waiting plan; retrying from live volumes"
            );

            let selection = open_from_volume_zero()?;
            (plan, headers, rebuild_source, used_cached_headers) = attach_volumes_and_build_plan(
                selection.value,
                RarTopologyRebuildSource::VolumeZero,
                false,
                false,
            )?;
        }

        let ownerless_volumes = ownerless_present_member_volumes(&plan, &facts);
        if !ownerless_volumes.is_empty() {
            return Err(ownerless_rar_plan_error(
                &set_name_owned,
                &ownerless_volumes,
            ));
        }

        if cached_rar_plan_is_incoherent(&plan, &failed, worker_active, &facts, &volume_paths) {
            return Err(format!(
                "RAR set '{set_name_owned}' {INCOHERENT_RAR_WAITING_STATE_ERROR_MARKER}"
            ));
        }

        let _ = used_cached_headers;
        Ok(ComputedRarSetState {
            plan,
            headers,
            rebuild_source,
        })
    }

    fn apply_computed_rar_set_state(
        &mut self,
        job_id: JobId,
        set_name: &str,
        computed: ComputedRarSetState,
    ) {
        info!(
            job_id = job_id.0,
            set_name = %set_name,
            rebuild_source = computed.rebuild_source.as_str(),
            "RAR plan rebuilt"
        );
        let refreshed_volumes: BTreeSet<u32> = computed
            .plan
            .topology
            .complete_volumes
            .iter()
            .copied()
            .collect();
        let latest_refreshed_volume = refreshed_volumes.iter().next_back().copied().unwrap_or(0);
        self.set_rar_snapshot(job_id, set_name, computed.headers);
        self.apply_rar_plan(job_id, set_name, computed.plan);
        if let Some(refresh_state) = self
            .rar_refresh_state
            .get_mut(&(job_id, set_name.to_string()))
        {
            // Per-set refreshes are serialized; exact replacement here is only
            // safe while that invariant holds. Concurrent refreshes would need
            // generation/staleness guards before applying computed plans.
            refresh_state.refreshed_volumes = refreshed_volumes;
            refresh_state.latest_completed_volume = refresh_state
                .latest_completed_volume
                .max(latest_refreshed_volume);
        }
    }

    fn apply_failed_rar_set_compute(
        &mut self,
        job_id: JobId,
        set_name: &str,
        existing: RarSetState,
        error: String,
    ) -> Result<(), String> {
        if is_ownerless_rar_plan_error(&error) {
            self.clear_rar_snapshot(job_id, set_name);
            return Err(error);
        }

        let mut fallback = existing.plan.clone().unwrap_or_else(|| RarDerivedPlan {
            phase: RarSetPhase::FallbackFullSet,
            is_solid: false,
            ready_members: Vec::new(),
            member_names: Vec::new(),
            member_dependencies: HashMap::new(),
            waiting_on_volumes: HashSet::new(),
            deletion_eligible: HashSet::new(),
            delete_decisions: BTreeMap::new(),
            topology: ArchiveTopology {
                archive_type: ArchiveType::Rar,
                volume_map: self.build_rar_volume_map(job_id, set_name),
                complete_volumes: existing.facts.keys().copied().collect(),
                expected_volume_count: None,
                members: Vec::new(),
                unresolved_spans: Vec::new(),
            },
            fallback_reason: Some(error.clone()),
        });
        fallback.phase = RarSetPhase::FallbackFullSet;
        fallback.fallback_reason = Some(error.clone());
        self.apply_rar_plan(job_id, set_name, fallback);
        Err(error)
    }

    pub(in crate::pipeline) fn latest_completed_rar_volume(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> u32 {
        self.rar_sets
            .get(&(job_id, set_name.to_string()))
            .and_then(|state| state.facts.keys().copied().max())
            .unwrap_or(0)
    }

    pub(in crate::pipeline) fn enqueue_rar_set_refresh(
        &mut self,
        job_id: JobId,
        set_name: &str,
        target_completed_volume: u32,
        reason: RefreshReason,
    ) {
        let key = (job_id, set_name.to_string());
        let request = RarRefreshRequest {
            target_completed_volume,
            reason,
        };
        let current_plan_volumes: BTreeSet<u32> = self
            .rar_sets
            .get(&key)
            .and_then(|state| state.plan.as_ref())
            .map(|plan| plan.topology.complete_volumes.iter().copied().collect())
            .unwrap_or_default();
        let mut launch = None;
        {
            let state = self.rar_refresh_state.entry(key.clone()).or_default();
            state.refreshed_volumes = current_plan_volumes;
            state.latest_completed_volume =
                state.latest_completed_volume.max(target_completed_volume);
            if reason >= RefreshReason::IdentityRebind {
                state.structure_dirty = true;
            }
            if state.in_flight.is_some() {
                match &mut state.queued {
                    Some(queued) => queued.merge(request),
                    None => state.queued = Some(request),
                }
                debug!(
                    job_id = job_id.0,
                    set_name,
                    target_completed_volume,
                    reason = ?reason,
                    in_flight = ?state.in_flight,
                    queued = ?state.queued,
                    "coalesced RAR refresh request"
                );
            } else {
                let mut launch_request = request;
                if let Some(mut queued) = state.queued.take() {
                    queued.merge(launch_request);
                    launch_request = queued;
                }
                state.in_flight = Some(launch_request);
                state.last_error = None;
                launch = Some(launch_request);
            }
        }

        if let Some(request) = launch {
            self.spawn_rar_refresh(job_id, set_name.to_string(), request);
        }
    }

    pub(in crate::pipeline) fn launch_queued_rar_capacity_refresh(
        &mut self,
        job_id: JobId,
        set_name: &str,
    ) {
        let key = (job_id, set_name.to_string());
        let mut launch = None;
        if let Some(state) = self.rar_refresh_state.get_mut(&key)
            && state.in_flight.is_none()
            && let Some(request) = state.queued.take()
        {
            state.in_flight = Some(request);
            state.last_error = None;
            launch = Some(request);
        }

        if let Some(request) = launch {
            self.spawn_rar_refresh(job_id, set_name.to_string(), request);
        }
    }

    pub(in crate::pipeline) fn spawn_rar_refresh(
        &mut self,
        job_id: JobId,
        set_name: String,
        request: RarRefreshRequest,
    ) {
        let Some(input) = self.prepare_rar_set_compute_input(job_id, &set_name, request.reason)
        else {
            if let Some(state) = self.rar_refresh_state.get_mut(&(job_id, set_name)) {
                state.in_flight = None;
            }
            return;
        };
        if input.cached_headers.is_none() && !input.volume_paths.contains_key(&0) {
            // The compute path is guaranteed to fail this state ("cannot be
            // rebuilt without cached headers or volume 0"). Park instead of
            // burning a blocking task per completed volume; the volume-0
            // completion or the next identity rebind re-enqueues a refresh.
            // With downloads still running that re-enqueue is guaranteed, so
            // parking is routine; with none left it may never come, so say so
            // loudly.
            let active_downloads = self
                .active_downloads_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0);
            if active_downloads == 0 {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    reason = ?request.reason,
                    "RAR refresh parked waiting on volume 0 with no active downloads"
                );
            } else {
                debug!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    reason = ?request.reason,
                    "RAR refresh parked waiting on volume 0"
                );
            }
            if let Some(state) = self.rar_refresh_state.get_mut(&(job_id, set_name)) {
                state.in_flight = None;
            }
            return;
        }
        let extraction_generation = input.extraction_generation;
        let refresh_done_tx = self.rar_refresh_done_tx.clone();
        tokio::spawn(async move {
            let result = Self::run_rar_set_compute(input).await;
            let _ = refresh_done_tx
                .send(RarRefreshDone {
                    job_id,
                    set_name,
                    request,
                    extraction_generation,
                    result,
                })
                .await;
        });
    }

    pub(in crate::pipeline) async fn handle_rar_refresh_done(&mut self, done: RarRefreshDone) {
        let key = (done.job_id, done.set_name.clone());
        let current_generation = self
            .rar_sets
            .get(&key)
            .map(|set_state| set_state.extraction_generation);
        let stale_refresh =
            current_generation.is_some_and(|generation| generation > done.extraction_generation);
        if stale_refresh {
            debug!(
                job_id = done.job_id.0,
                set_name = %done.set_name,
                reason = ?done.request.reason,
                refresh_generation = done.extraction_generation,
                current_generation = ?current_generation,
                "discarding stale RAR refresh result"
            );
        }
        let error = match done.result {
            Ok(computed) if !stale_refresh => {
                self.apply_computed_rar_set_state(done.job_id, &done.set_name, computed);
                None
            }
            Ok(_) => None,
            Err(error) if !stale_refresh => {
                warn!(
                    job_id = done.job_id.0,
                    set_name = %done.set_name,
                    reason = ?done.request.reason,
                    error = %error,
                    "background RAR refresh failed"
                );
                Some(error)
            }
            Err(error) => {
                debug!(
                    job_id = done.job_id.0,
                    set_name = %done.set_name,
                    reason = ?done.request.reason,
                    error = %error,
                    "discarding stale RAR refresh error"
                );
                None
            }
        };
        let success = error.is_none() && !stale_refresh;
        let capacity_pressure = error
            .as_ref()
            .is_some_and(RarRefreshError::is_capacity_pressure);
        let live_fact_volumes: BTreeSet<u32> = self
            .rar_sets
            .get(&key)
            .map(|set_state| set_state.facts.keys().copied().collect())
            .unwrap_or_default();
        let latest_live_volume = live_fact_volumes.iter().next_back().copied().unwrap_or(0);
        let active_rar_workers = self.rar_sets.get(&key).is_some_and(|set_state| {
            set_state.active_workers > 0 || !set_state.in_flight_members.is_empty()
        });

        let mut follow_up = None;
        let mut schedule_capacity_retry = false;
        if let Some(state) = self.rar_refresh_state.get_mut(&key) {
            state.in_flight = None;
            if stale_refresh {
                let mut request = state
                    .queued
                    .take()
                    .filter(|queued| queued.reason != RefreshReason::PostExtraction);
                if request.is_none() && done.request.reason != RefreshReason::PostExtraction {
                    request = Some(RarRefreshRequest {
                        target_completed_volume: state
                            .latest_completed_volume
                            .max(latest_live_volume)
                            .max(done.request.target_completed_volume),
                        reason: done.request.reason,
                    });
                }
                if request.is_none() && state.structure_dirty {
                    request = Some(RarRefreshRequest {
                        target_completed_volume: state
                            .latest_completed_volume
                            .max(latest_live_volume)
                            .max(done.request.target_completed_volume),
                        reason: RefreshReason::CoverageExpansion,
                    });
                }
                if let Some(mut request) = request {
                    request.target_completed_volume = request
                        .target_completed_volume
                        .max(state.latest_completed_volume)
                        .max(latest_live_volume);
                    if active_rar_workers {
                        state.queued = Some(request);
                    } else {
                        state.in_flight = Some(request);
                        follow_up = Some(request);
                    }
                }
            } else {
                if let Some(error) = error.as_ref() {
                    state.last_error = Some(error.clone());
                } else {
                    state.last_error = None;
                }

                let coverage_gap =
                    success && !live_fact_volumes.is_subset(&state.refreshed_volumes);
                if let Some(mut queued) = state.queued.take() {
                    if coverage_gap || capacity_pressure {
                        queued.target_completed_volume = queued
                            .target_completed_volume
                            .max(state.latest_completed_volume)
                            .max(latest_live_volume);
                        queued.reason = queued.reason.max(RefreshReason::CoverageExpansion);
                    }
                    let still_needed = coverage_gap
                        || queued.reason > done.request.reason
                        || state.structure_dirty
                        || !success;
                    if capacity_pressure {
                        state.queued = Some(queued);
                        schedule_capacity_retry = true;
                    } else if still_needed {
                        state.in_flight = Some(queued);
                        follow_up = Some(queued);
                    }
                } else if coverage_gap {
                    let request = RarRefreshRequest {
                        target_completed_volume: state
                            .latest_completed_volume
                            .max(latest_live_volume)
                            .max(done.request.target_completed_volume),
                        reason: RefreshReason::CoverageExpansion,
                    };
                    state.in_flight = Some(request);
                    follow_up = Some(request);
                } else if capacity_pressure {
                    let request = RarRefreshRequest {
                        target_completed_volume: state
                            .latest_completed_volume
                            .max(latest_live_volume)
                            .max(done.request.target_completed_volume),
                        reason: done.request.reason.max(RefreshReason::CoverageExpansion),
                    };
                    state.queued = Some(request);
                    schedule_capacity_retry = true;
                }

                if follow_up.is_none() && success {
                    state.structure_dirty = false;
                }
            }
        }

        if let Some(request) = follow_up {
            let can_extract_covered_spans =
                success && request.reason <= RefreshReason::CoverageExpansion;
            self.spawn_rar_refresh(done.job_id, done.set_name.clone(), request);
            if can_extract_covered_spans {
                self.purge_empty_rar_set_if_idle(done.job_id, &done.set_name);
                self.try_rar_extraction(done.job_id).await;
                if self.rar_sets.contains_key(&key) {
                    self.try_delete_volumes(done.job_id, &done.set_name);
                }
                self.check_job_completion(done.job_id).await;
            }
            return;
        }

        if schedule_capacity_retry {
            self.schedule_rar_capacity_retry(
                done.job_id,
                &done.set_name,
                RarCapacityRetryKind::Refresh,
            );
            return;
        }

        if success {
            self.purge_empty_rar_set_if_idle(done.job_id, &done.set_name);
            self.try_rar_extraction(done.job_id).await;
            if self.rar_sets.contains_key(&key) {
                self.try_delete_volumes(done.job_id, &done.set_name);
            }
            self.check_job_completion(done.job_id).await;
        }
    }

    #[cfg(test)]
    pub(in crate::pipeline) async fn refresh_rar_volume_facts_for_set(
        &mut self,
        job_id: JobId,
        set_name: &str,
        touched_filenames: &HashSet<String>,
    ) -> Result<(), String> {
        let volume_paths: Vec<(u32, String, PathBuf)> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return Ok(());
            };

            state
                .assembly
                .files()
                .filter_map(|file_asm| {
                    let current_filename = self.current_filename_for_file(job_id, file_asm);
                    if !touched_filenames.contains(&current_filename) {
                        return None;
                    }
                    let weaver_model::files::FileRole::RarVolume { volume_number } =
                        self.classified_role_for_file(job_id, file_asm)
                    else {
                        return None;
                    };
                    if self
                        .classified_archive_set_name_for_file(job_id, file_asm)
                        .as_deref()
                        != Some(set_name)
                        || !file_asm.is_complete()
                    {
                        return None;
                    }
                    let path = state.working_dir.join(&current_filename);
                    path.exists()
                        .then_some((volume_number, current_filename, path))
                })
                .collect()
        };
        if volume_paths.is_empty() {
            return Ok(());
        }

        let password_candidates = self.archive_password_candidates_for_set(job_id, set_name);
        let mut parsed = Vec::new();
        for (expected_volume_number, filename, path) in volume_paths {
            let facts =
                Self::parse_rar_volume_facts_from_path(path, password_candidates.clone()).await?;
            if facts.volume_number != expected_volume_number {
                info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    filename = %filename,
                    expected_volume = expected_volume_number,
                    parsed_volume = facts.volume_number,
                    "RAR facts refresh detected volume-number mismatch after normalization"
                );
            }
            parsed.push((facts.volume_number, filename, facts));
        }

        let state = self
            .rar_sets
            .entry((job_id, set_name.to_string()))
            .or_default();
        state
            .volume_files
            .retain(|_, existing_filename| !touched_filenames.contains(existing_filename));
        for (volume_number, filename, facts) in parsed {
            let encoded = rmp_serde::to_vec_named(&facts)
                .map_err(|e| format!("failed to encode RAR facts: {e}"))?;
            if let Err(error) =
                self.db
                    .save_rar_volume_facts(job_id, set_name, volume_number, &encoded)
            {
                return Err(format!("failed to persist RAR facts: {error}"));
            }
            Self::register_rar_volume_file(state, volume_number, filename);
            state.facts.insert(volume_number, facts);
        }
        self.recompute_rar_set_state(job_id, set_name).await
    }

    fn set_rar_snapshot(&mut self, job_id: JobId, set_name: &str, headers: Vec<u8>) {
        let state = self
            .rar_sets
            .entry((job_id, set_name.to_string()))
            .or_default();
        if state.cached_headers.as_deref() == Some(headers.as_slice()) {
            crate::runtime::perf_probe::record(
                "pipeline.rar_snapshot.persist.skipped_unchanged",
                std::time::Duration::ZERO,
            );
            return;
        }
        state.cached_headers = Some(headers.clone());
        if let Err(e) = self.db.save_archive_headers(job_id, set_name, &headers) {
            error!(job_id = job_id.0, set_name, error = %e, "failed to persist RAR headers");
        }
    }

    pub(in crate::pipeline) fn load_rar_snapshot(
        &self,
        job_id: JobId,
        set_name: &str,
    ) -> Option<Vec<u8>> {
        self.rar_sets
            .get(&(job_id, set_name.to_string()))
            .and_then(|state| state.cached_headers.clone())
            .or_else(|| {
                self.db
                    .load_archive_headers(job_id, set_name)
                    .ok()
                    .flatten()
            })
    }

    pub(in crate::pipeline) fn clear_rar_snapshot(&mut self, job_id: JobId, set_name: &str) {
        if let Some(state) = self.rar_sets.get_mut(&(job_id, set_name.to_string())) {
            state.cached_headers = None;
        }
        if let Err(e) = self.db.delete_archive_headers(job_id, set_name) {
            error!(job_id = job_id.0, set_name, error = %e, "failed to delete cached RAR headers");
        }
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.assembly.archive_topologies_mut().remove(set_name);
        }
    }

    fn build_rar_volume_map(&self, job_id: JobId, set_name: &str) -> HashMap<String, u32> {
        let Some(state) = self.jobs.get(&job_id) else {
            return HashMap::new();
        };
        let mut volume_map = HashMap::new();
        if let Some(rar_state) = self.rar_sets.get(&(job_id, set_name.to_string())) {
            for (logical_volume, filename) in &rar_state.volume_files {
                volume_map.insert(filename.clone(), *logical_volume);
            }
        }
        for file_asm in state.assembly.files() {
            let weaver_model::files::FileRole::RarVolume { volume_number } =
                self.classified_role_for_file(job_id, file_asm)
            else {
                continue;
            };
            if self
                .classified_archive_set_name_for_file(job_id, file_asm)
                .as_deref()
                == Some(set_name)
                && file_asm.is_complete()
            {
                volume_map
                    .entry(self.current_filename_for_file(job_id, file_asm))
                    .or_insert(volume_number);
            }
        }
        volume_map
    }

    pub(crate) async fn restore_rar_state_for_job(&mut self, job_id: JobId) {
        let password_candidates = self.archive_password_candidates_for_job(job_id);
        match self
            .db_blocking(move |db| db.load_all_archive_headers(job_id))
            .await
        {
            Ok(headers_by_set) => {
                for (set_name, headers) in headers_by_set {
                    match Self::deserialize_rar_headers_with_password_candidates(
                        &set_name,
                        &headers,
                        &password_candidates,
                        std::sync::Arc::new(weaver_unrar::crypto::KdfCache::new()),
                    ) {
                        Ok(_) => {
                            self.rar_sets
                                .entry((job_id, set_name))
                                .or_default()
                                .cached_headers = Some(headers);
                        }
                        Err(error) => {
                            warn!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                error = %error,
                                "dropping invalid persisted RAR header snapshot"
                            );
                            self.clear_rar_snapshot(job_id, &set_name);
                        }
                    }
                }
            }
            Err(error) => {
                error!(
                    job_id = job_id.0,
                    error = %error,
                    "failed to load persisted RAR header snapshots"
                );
            }
        }

        let facts_by_set = match self
            .db_blocking(move |db| db.load_all_rar_volume_facts(job_id))
            .await
        {
            Ok(facts) => facts,
            Err(error) => {
                error!(
                    job_id = job_id.0,
                    error = %error,
                    "failed to load persisted RAR volume facts"
                );
                return;
            }
        };

        for (set_name, facts_rows) in facts_by_set {
            let state = self.rar_sets.entry((job_id, set_name.clone())).or_default();
            state.facts.clear();
            state.volume_files.clear();
            for (volume_index, blob) in facts_rows {
                match rmp_serde::from_slice::<weaver_unrar::RarVolumeFacts>(&blob) {
                    Ok(facts) => {
                        if facts.volume_number != volume_index {
                            warn!(
                                job_id = job_id.0,
                                set_name = %set_name,
                                stored_volume = volume_index,
                                parsed_volume = facts.volume_number,
                                "persisted RAR facts volume key did not match parsed volume number"
                            );
                        }
                        state.facts.insert(facts.volume_number, facts);
                    }
                    Err(error) => {
                        warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            volume_index,
                            error = %error,
                            "dropping invalid persisted RAR volume facts"
                        );
                    }
                }
            }
        }

        let mut set_names: BTreeSet<String> = self
            .rar_sets
            .keys()
            .filter_map(|(jid, set_name)| (*jid == job_id).then_some(set_name.clone()))
            .collect();
        if let Some(state) = self.jobs.get(&job_id) {
            for file_asm in state.assembly.files() {
                if matches!(
                    self.classified_role_for_file(job_id, file_asm),
                    weaver_model::files::FileRole::RarVolume { .. }
                ) && file_asm.is_complete()
                    && let Some(set_name) =
                        self.classified_archive_set_name_for_file(job_id, file_asm)
                {
                    set_names.insert(set_name);
                }
            }
        }
        let set_names: Vec<String> = set_names.into_iter().collect();
        for set_name in &set_names {
            let password_candidates = self.archive_password_candidates_for_set(job_id, set_name);
            let volume_files = {
                let Some(state) = self.jobs.get(&job_id) else {
                    return;
                };
                state
                    .assembly
                    .files()
                    .filter_map(|file_asm| {
                        let weaver_model::files::FileRole::RarVolume { .. } =
                            self.classified_role_for_file(job_id, file_asm)
                        else {
                            return None;
                        };
                        if self
                            .classified_archive_set_name_for_file(job_id, file_asm)
                            .as_deref()
                            != Some(set_name.as_str())
                            || !file_asm.is_complete()
                        {
                            return None;
                        }
                        let current_filename = self.current_filename_for_file(job_id, file_asm);
                        let path = state.working_dir.join(&current_filename);
                        path.exists().then_some((current_filename, path))
                    })
                    .collect::<Vec<_>>()
            };
            let mut live_volumes = HashSet::new();
            for (filename, path) in volume_files {
                match Self::parse_rar_volume_facts_from_path(path, password_candidates.clone())
                    .await
                {
                    Ok(facts) => {
                        live_volumes.insert(facts.volume_number);
                        if let Some(state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) {
                            Self::register_rar_volume_file(state, facts.volume_number, filename);
                            state.facts.entry(facts.volume_number).or_insert(facts);
                        }
                    }
                    Err(error) => {
                        warn!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            filename = %filename,
                            error,
                            "failed to rebuild live RAR volume-file map during restore"
                        );
                    }
                }
            }
            let remove_empty_set =
                if let Some(state) = self.rar_sets.get_mut(&(job_id, set_name.clone())) {
                    let before = state.facts.len();
                    state
                        .facts
                        .retain(|volume, _| live_volumes.contains(volume));
                    let discarded = before.saturating_sub(state.facts.len());
                    if discarded > 0 {
                        debug!(
                            job_id = job_id.0,
                            set_name = %set_name,
                            discarded,
                            "discarded stale RAR discovery facts without matching completed files"
                        );
                    }
                    state.facts.is_empty() && state.volume_files.is_empty()
                } else {
                    false
                };
            if remove_empty_set {
                self.rar_sets.remove(&(job_id, set_name.clone()));
                if let Err(error) = self.db.delete_rar_volume_facts_for_set(job_id, set_name) {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error = %error,
                        "failed to delete invalidated RAR discovery facts"
                    );
                }
                continue;
            }
            let _ = self.recompute_rar_set_state(job_id, set_name).await;
        }

        for set_name in set_names {
            let ownerless_volumes = self
                .rar_sets
                .get(&(job_id, set_name.clone()))
                .and_then(|state| state.plan.as_ref())
                .map(Self::ownerless_rar_volumes)
                .unwrap_or_default();
            if !ownerless_volumes.is_empty() {
                error!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    volumes = ?ownerless_volumes,
                    "skipping restore-time eager delete because restored RAR plan has ownerless volumes"
                );
                continue;
            }
            self.try_delete_volumes(job_id, &set_name);
        }
    }

    /// When a RAR volume file completes, rebuild topology from the persistent
    /// multi-volume header snapshot instead of inferring spans by volume order.
    pub(crate) async fn try_update_archive_topology(&mut self, job_id: JobId, file_id: NzbFileId) {
        let (observed_volume, filename, set_name, path, password_candidates) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            let Some(file_asm) = state.assembly.file(file_id) else {
                return;
            };
            let weaver_model::files::FileRole::RarVolume { volume_number } =
                self.classified_role_for_file(job_id, file_asm)
            else {
                return;
            };
            let Some(set_name) = self.classified_archive_set_name_for_file(job_id, file_asm) else {
                return;
            };
            let password_candidates = self.archive_password_candidates_for_set(job_id, &set_name);
            (
                volume_number,
                self.current_filename_for_file(job_id, file_asm),
                set_name,
                match self.resolve_job_input_path(
                    job_id,
                    &self.current_filename_for_file(job_id, file_asm),
                ) {
                    Some(path) => path,
                    None => return,
                },
                password_candidates,
            )
        };

        #[cfg(test)]
        {
            self.try_update_archive_topology_calls += 1;
        }

        match Self::parse_rar_volume_facts_from_path(path, password_candidates).await {
            Ok(facts) => {
                let parsed_volume = facts.volume_number;
                let changed = match self.persist_rar_volume_facts(
                    job_id,
                    &set_name,
                    &filename,
                    Some(observed_volume),
                    facts,
                ) {
                    Ok(changed) => changed,
                    Err(error) => {
                        warn!(
                            job_id = job_id.0,
                            volume = parsed_volume,
                            set_name = %set_name,
                            error = %error,
                            "failed to register RAR volume facts"
                        );
                        return;
                    }
                };
                debug!(
                    job_id = job_id.0,
                    volume = parsed_volume,
                    set_name = %set_name,
                    "RAR volume facts parsed"
                );
                let topology_missing_volume = self.jobs.get(&job_id).is_some_and(|state| {
                    state
                        .assembly
                        .archive_topology_for(&set_name)
                        .is_none_or(|topology| !topology.complete_volumes.contains(&parsed_volume))
                });
                if changed || topology_missing_volume {
                    self.enqueue_rar_set_refresh(
                        job_id,
                        &set_name,
                        parsed_volume,
                        RefreshReason::CoverageExpansion,
                    );
                }
            }
            Err(error) => warn!(
                job_id = job_id.0,
                filename = %filename,
                set_name = %set_name,
                error,
                "failed to parse RAR volume facts"
            ),
        }
    }

    /// When a non-RAR archive file completes, build or update the archive topology.
    ///
    /// Handles 7z, zip, tar, tar.gz, gz, and plain split files.
    /// Groups files by base name so that multiple independent archive sets
    /// within a single job are tracked separately.
    pub(crate) fn try_update_7z_topology(&mut self, job_id: JobId, file_id: NzbFileId) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let Some(file_asm) = state.assembly.file(file_id) else {
            return;
        };

        let role = self.classified_role_for_file(job_id, file_asm);
        let filename = self.current_filename_for_file(job_id, file_asm);
        let set_name = match self.classified_archive_set_name_for_file(job_id, file_asm) {
            Some(name) => name,
            None => return,
        };

        match role {
            weaver_model::files::FileRole::SevenZipArchive => {
                if state.assembly.archive_topology_for(&set_name).is_some() {
                    let state = self.jobs.get_mut(&job_id).unwrap();
                    state.assembly.mark_volume_complete(&set_name, 0);
                    debug!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        "7z single-file volume complete"
                    );
                    return;
                }

                let mut volume_map = std::collections::HashMap::new();
                volume_map.insert(filename.clone(), 0);

                let topology = ArchiveTopology {
                    archive_type: ArchiveType::SevenZip,
                    volume_map,
                    complete_volumes: std::collections::HashSet::new(),
                    expected_volume_count: Some(1),
                    members: vec![ArchiveMember {
                        name: set_name.clone(),
                        first_volume: 0,
                        last_volume: 0,
                        unpacked_size: 0,
                    }],
                    unresolved_spans: vec![],
                };

                let state = self.jobs.get_mut(&job_id).unwrap();
                state
                    .assembly
                    .set_archive_topology(set_name.clone(), topology);
                state.assembly.mark_volume_complete(&set_name, 0);

                info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    "7z topology set (single archive)"
                );
            }
            weaver_model::files::FileRole::SevenZipSplit { number } => {
                let completing_number = number;

                if state.assembly.archive_topology_for(&set_name).is_some() {
                    let state = self.jobs.get_mut(&job_id).unwrap();
                    state
                        .assembly
                        .mark_volume_complete(&set_name, completing_number);
                    debug!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        volume = completing_number,
                        "7z split volume complete"
                    );
                    return;
                }

                let mut volume_map = std::collections::HashMap::new();
                let mut max_number = 0u32;
                for f in state.assembly.files() {
                    if let weaver_model::files::FileRole::SevenZipSplit { number: n } =
                        self.classified_role_for_file(job_id, f)
                        && self
                            .classified_archive_set_name_for_file(job_id, f)
                            .as_deref()
                            == Some(&set_name)
                    {
                        volume_map.insert(self.current_filename_for_file(job_id, f), n);
                        max_number = max_number.max(n);
                    }
                }

                let expected = max_number + 1;
                let topology = ArchiveTopology {
                    archive_type: ArchiveType::SevenZip,
                    volume_map,
                    complete_volumes: std::collections::HashSet::new(),
                    expected_volume_count: Some(expected),
                    members: vec![ArchiveMember {
                        name: set_name.clone(),
                        first_volume: 0,
                        last_volume: max_number,
                        unpacked_size: 0,
                    }],
                    unresolved_spans: vec![],
                };

                let completed: Vec<u32> = state
                    .assembly
                    .files()
                    .filter(|f| f.is_complete())
                    .filter_map(|f| {
                        if let weaver_model::files::FileRole::SevenZipSplit { number: n } =
                            self.classified_role_for_file(job_id, f)
                            && self
                                .classified_archive_set_name_for_file(job_id, f)
                                .as_deref()
                                == Some(&set_name)
                        {
                            return Some(n);
                        }
                        None
                    })
                    .collect();
                let state = self.jobs.get_mut(&job_id).unwrap();
                state
                    .assembly
                    .set_archive_topology(set_name.clone(), topology);
                for n in completed {
                    state.assembly.mark_volume_complete(&set_name, n);
                }

                info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    expected_volumes = expected,
                    "7z topology set (split archive)"
                );
            }
            weaver_model::files::FileRole::ZipArchive
            | weaver_model::files::FileRole::TarArchive
            | weaver_model::files::FileRole::TarGzArchive
            | weaver_model::files::FileRole::TarBz2Archive
            | weaver_model::files::FileRole::GzArchive
            | weaver_model::files::FileRole::DeflateArchive
            | weaver_model::files::FileRole::BrotliArchive
            | weaver_model::files::FileRole::ZstdArchive
            | weaver_model::files::FileRole::Bzip2Archive => {
                if state.assembly.archive_topology_for(&set_name).is_some() {
                    let state = self.jobs.get_mut(&job_id).unwrap();
                    state.assembly.mark_volume_complete(&set_name, 0);
                    debug!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        "single-file archive complete"
                    );
                    return;
                }

                let archive_type = match role {
                    weaver_model::files::FileRole::ZipArchive => ArchiveType::Zip,
                    weaver_model::files::FileRole::TarArchive => ArchiveType::Tar,
                    weaver_model::files::FileRole::TarGzArchive => ArchiveType::TarGz,
                    weaver_model::files::FileRole::TarBz2Archive => ArchiveType::TarBz2,
                    weaver_model::files::FileRole::GzArchive => ArchiveType::Gz,
                    weaver_model::files::FileRole::DeflateArchive => ArchiveType::Deflate,
                    weaver_model::files::FileRole::BrotliArchive => ArchiveType::Brotli,
                    weaver_model::files::FileRole::ZstdArchive => ArchiveType::Zstd,
                    weaver_model::files::FileRole::Bzip2Archive => ArchiveType::Bzip2,
                    _ => unreachable!(),
                };

                let mut volume_map = std::collections::HashMap::new();
                volume_map.insert(filename.clone(), 0);

                let topology = ArchiveTopology {
                    archive_type,
                    volume_map,
                    complete_volumes: std::collections::HashSet::new(),
                    expected_volume_count: Some(1),
                    members: vec![ArchiveMember {
                        name: set_name.clone(),
                        first_volume: 0,
                        last_volume: 0,
                        unpacked_size: 0,
                    }],
                    unresolved_spans: vec![],
                };

                let state = self.jobs.get_mut(&job_id).unwrap();
                state
                    .assembly
                    .set_archive_topology(set_name.clone(), topology);
                state.assembly.mark_volume_complete(&set_name, 0);

                info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    "single-file archive topology set"
                );
            }
            weaver_model::files::FileRole::SplitFile { number } => {
                let completing_number = number;

                if state.assembly.archive_topology_for(&set_name).is_some() {
                    let state = self.jobs.get_mut(&job_id).unwrap();
                    state
                        .assembly
                        .mark_volume_complete(&set_name, completing_number);
                    debug!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        volume = completing_number,
                        "split file volume complete"
                    );
                    return;
                }

                let mut volume_map = std::collections::HashMap::new();
                let mut max_number = 0u32;
                for f in state.assembly.files() {
                    if let weaver_model::files::FileRole::SplitFile { number: n } =
                        self.classified_role_for_file(job_id, f)
                        && self
                            .classified_archive_set_name_for_file(job_id, f)
                            .as_deref()
                            == Some(&set_name)
                    {
                        volume_map.insert(self.current_filename_for_file(job_id, f), n);
                        max_number = max_number.max(n);
                    }
                }

                let expected = max_number + 1;
                let topology = ArchiveTopology {
                    archive_type: ArchiveType::Split,
                    volume_map,
                    complete_volumes: std::collections::HashSet::new(),
                    expected_volume_count: Some(expected),
                    members: vec![ArchiveMember {
                        name: set_name.clone(),
                        first_volume: 0,
                        last_volume: max_number,
                        unpacked_size: 0,
                    }],
                    unresolved_spans: vec![],
                };

                let completed: Vec<u32> = state
                    .assembly
                    .files()
                    .filter(|f| f.is_complete())
                    .filter_map(|f| {
                        if let weaver_model::files::FileRole::SplitFile { number: n } =
                            self.classified_role_for_file(job_id, f)
                            && self
                                .classified_archive_set_name_for_file(job_id, f)
                                .as_deref()
                                == Some(&set_name)
                        {
                            return Some(n);
                        }
                        None
                    })
                    .collect();
                let state = self.jobs.get_mut(&job_id).unwrap();
                state
                    .assembly
                    .set_archive_topology(set_name.clone(), topology);
                for n in completed {
                    state.assembly.mark_volume_complete(&set_name, n);
                }

                info!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    expected_volumes = expected,
                    "split file topology set"
                );
            }
            _ => {}
        }
    }
}
