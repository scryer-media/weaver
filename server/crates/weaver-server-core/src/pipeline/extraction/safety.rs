// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tracing::info;

use crate::operations::disk::probe_nearest_disk_space;

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;
const TIB: u64 = 1024 * GIB;
const MIN_MEMORY_LIMIT: u64 = 64 * MIB;
const MAX_MEMORY_LIMIT: u64 = 64 * GIB;
const MAX_MEMORY_ENV: &str = "WEAVER_EXTRACTION_MAX_MEMORY_BYTES";

#[derive(Debug, Clone)]
pub(crate) struct ExtractionLimits {
    pub(crate) max_job_bytes: u64,
    pub(crate) max_member_bytes: u64,
    pub(crate) max_entries: u64,
    pub(crate) max_ratio: u64,
    // The output a job may always write, however little its archives
    // declared: the ratio limit never falls under it.
    pub(crate) ratio_floor_bytes: u64,
    pub(crate) max_seconds: u64,
    pub(crate) min_free_bytes: u64,
    pub(crate) max_memory_bytes: u64,
}

impl ExtractionLimits {
    // The same limits with the memory ceiling the chosen hardware profile
    // decides. An explicit `WEAVER_EXTRACTION_MAX_MEMORY_BYTES` still wins,
    // and says so once, so an operator who pinned the ceiling is not left
    // wondering why the profile did nothing.
    pub(crate) fn from_env_with_profile_ceiling(
        complete_dir: &Path,
        ceiling_bytes: u64,
    ) -> Result<Self, String> {
        if std::env::var(MAX_MEMORY_ENV).is_ok() {
            info!(
                variable = MAX_MEMORY_ENV,
                "extraction memory ceiling is pinned by the environment; the hardware profile's ceiling is not used"
            );
        }
        Self::resolve(complete_dir, Some(ceiling_bytes))
    }

    // These limits under another hardware profile's memory ceiling. Nothing
    // else here depends on the profile, and a ceiling pinned by the
    // environment stays pinned.
    pub(crate) fn with_profile_ceiling(&self, ceiling_bytes: u64) -> Self {
        self.with_memory_ceiling(ceiling_bytes, std::env::var_os(MAX_MEMORY_ENV).is_some())
    }

    // [`Self::with_profile_ceiling`] with the environment's answer given:
    // the ceiling clamped as at startup, or left alone when it is pinned.
    fn with_memory_ceiling(&self, ceiling_bytes: u64, pinned: bool) -> Self {
        let mut limits = self.clone();
        if !pinned {
            limits.max_memory_bytes = ceiling_bytes.clamp(MIN_MEMORY_LIMIT, MAX_MEMORY_LIMIT);
        }
        limits
    }

    fn resolve(complete_dir: &Path, profile_ceiling_bytes: Option<u64>) -> Result<Self, String> {
        // The completed-download directory may not exist yet at startup; its
        // nearest existing ancestor answers for the filesystem the reserve is
        // derived from.
        let total_filesystem_bytes = probe_nearest_disk_space(complete_dir)
            .ok()
            .map(|space| space.total_bytes);
        let default_min_free = default_disk_reserve_bytes(total_filesystem_bytes);
        // Without a profile — the fallback path, which builds its own limits
        // without the runtime's — half the machine's memory stands in.
        let default_memory = profile_ceiling_bytes
            .unwrap_or_else(|| {
                crate::runtime::system_probe::detect_total_memory_bytes()
                    .unwrap_or(2 * MAX_MEMORY_LIMIT)
                    / 2
            })
            .clamp(MIN_MEMORY_LIMIT, MAX_MEMORY_LIMIT);

        Ok(Self {
            max_job_bytes: parse_positive_u64("WEAVER_EXTRACTION_MAX_JOB_BYTES", 2 * TIB)?,
            max_member_bytes: parse_positive_u64("WEAVER_EXTRACTION_MAX_MEMBER_BYTES", TIB)?,
            max_entries: parse_positive_u64("WEAVER_EXTRACTION_MAX_ENTRIES", 100_000)?,
            max_ratio: parse_positive_u64("WEAVER_EXTRACTION_MAX_RATIO", 100)?,
            ratio_floor_bytes: parse_positive_u64("WEAVER_EXTRACTION_RATIO_FLOOR_BYTES", GIB)?,
            max_seconds: parse_positive_u64("WEAVER_EXTRACTION_MAX_SECONDS", 43_200)?,
            min_free_bytes: parse_positive_u64(
                "WEAVER_EXTRACTION_MIN_FREE_BYTES",
                default_min_free,
            )?,
            max_memory_bytes: parse_positive_u64(MAX_MEMORY_ENV, default_memory)?,
        })
    }

    // The most a job may write whose archives declared
    // `declared_archive_bytes`, and the limit that sets it: the ratio over
    // what was declared, never under the ratio floor, unless the job byte cap is
    // tighter. A breach names whichever of the two it is.
    pub(crate) fn job_output_limit(
        &self,
        declared_archive_bytes: u64,
    ) -> (u64, ExtractionRejectionReason) {
        let ratio_limit_bytes = declared_archive_bytes
            .saturating_mul(self.max_ratio)
            .max(self.ratio_floor_bytes);
        if ratio_limit_bytes <= self.max_job_bytes {
            (ratio_limit_bytes, ExtractionRejectionReason::Ratio)
        } else {
            (self.max_job_bytes, ExtractionRejectionReason::JobBytes)
        }
    }
}

// The free space a filesystem of `total_filesystem_bytes` should keep: a
// twentieth of it, between 512 MiB and 20 GiB, and 512 MiB when the size is
// unknown. The extractor's default output reserve, and the same rule the
// direct-store holds scratch keeps for the working directory.
pub(crate) fn default_disk_reserve_bytes(total_filesystem_bytes: Option<u64>) -> u64 {
    total_filesystem_bytes
        .map(|total| (total / 20).clamp(512 * MIB, 20 * GIB))
        .unwrap_or(512 * MIB)
}

fn parse_positive_u64(name: &str, default: u64) -> Result<u64, String> {
    match std::env::var(name) {
        Ok(raw) => {
            let value = raw
                .parse::<u64>()
                .map_err(|error| format!("invalid {name} value '{raw}': {error}"))?;
            if value == 0 {
                return Err(format!(
                    "invalid {name} value '{raw}': must be greater than zero"
                ));
            }
            Ok(value)
        }
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("failed to read {name}: {error}")),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExtractionRejectionReason {
    MemberBytes,
    JobBytes,
    Ratio,
    Entries,
    Deadline,
    Memory,
    DiskReserve,
    ContentPolicy,
}

impl ExtractionRejectionReason {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::MemberBytes => "member_bytes",
            Self::JobBytes => "job_bytes",
            Self::Ratio => "ratio",
            Self::Entries => "entries",
            Self::Deadline => "deadline",
            Self::Memory => "memory",
            Self::DiskReserve => "disk_reserve",
            Self::ContentPolicy => "content_policy",
        }
    }
}

// A refusal by the job's extraction budget: which limit or policy, and what
// it refused. Carried typed by `ExtractionError` so a consumer never has to
// recognise it from its rendering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExtractionFailure {
    reason: ExtractionRejectionReason,
    detail: String,
}

impl ExtractionFailure {
    pub(crate) fn new(reason: ExtractionRejectionReason, detail: impl Into<String>) -> Self {
        Self {
            reason,
            detail: detail.into(),
        }
    }

    pub(crate) fn reason(&self) -> ExtractionRejectionReason {
        self.reason
    }

    // The reason a job keeps when it fails on this refusal. A content-policy
    // refusal is kept as its own sentence, without the marker, because the
    // consumer app and the Jobs screen show it as it stands. Every other
    // refusal keeps the whole rendering.
    pub(crate) fn job_failure_reason(&self) -> String {
        match self.reason {
            ExtractionRejectionReason::ContentPolicy => self.detail.clone(),
            _ => self.to_string(),
        }
    }
}

impl std::error::Error for ExtractionFailure {}

impl From<ExtractionFailure> for String {
    fn from(failure: ExtractionFailure) -> Self {
        failure.to_string()
    }
}

impl std::fmt::Display for ExtractionFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "WEAVER_EXTRACTION_REJECTED[{}]: {}",
            self.reason.as_str(),
            self.detail
        )
    }
}

// The process-wide ledger of the memory that jobs' article lists and
// repeated-article indexes hold. Those are recorded as charges and nothing
// admits a job against the ledger (see
// [`ProcessMemoryBudget::charge_retained`]). The repeated-article cache is
// the only thing that refuses: a cache reservation that would pass the limit
// fails outright and the cache does without it.
#[derive(Debug)]
pub(crate) struct ProcessMemoryBudget {
    limit: u64,
    reserved: Arc<AtomicU64>,
    // The limit a lowered one replaced, while the charges recorded under it
    // still exceed the new one; zero otherwise. Without this a lowered limit
    // would refuse every repeated-article cache reservation until the work
    // charged under the wider limit had finished.
    lowered_from: Arc<AtomicU64>,
    admission: Arc<Mutex<()>>,
}

impl ProcessMemoryBudget {
    pub(crate) fn new(limit: u64) -> Self {
        Self {
            limit,
            reserved: Arc::new(AtomicU64::new(0)),
            lowered_from: Arc::new(AtomicU64::new(0)),
            admission: Arc::new(Mutex::new(())),
        }
    }

    // The same ledger under another limit, for the reservations made from
    // now on.
    //
    // Charges and reservations are shared with the budget this one
    // replaces. A view already handed to running work keeps the limit it was
    // given.
    pub(crate) fn with_limit(&self, limit: u64) -> Self {
        let _guard = self
            .admission
            .lock()
            .expect("process memory state poisoned");
        if limit < self.limit {
            self.lowered_from.fetch_max(self.limit, Ordering::AcqRel);
        }
        Self {
            limit,
            reserved: Arc::clone(&self.reserved),
            lowered_from: Arc::clone(&self.lowered_from),
            admission: Arc::clone(&self.admission),
        }
    }

    // The limit a reservation is judged against: this budget's own, or the
    // wider one it replaced for as long as the charges recorded under that one
    // are still above this one. Called with `admission` held.
    fn admission_limit(&self) -> u64 {
        let lowered_from = self.lowered_from.load(Ordering::Acquire);
        if lowered_from <= self.limit {
            return self.limit;
        }
        if self.reserved.load(Ordering::Acquire) <= self.limit {
            self.lowered_from.store(0, Ordering::Release);
            return self.limit;
        }
        lowered_from
    }

    // Reserve `bytes` until the returned permit drops, or fail at once when
    // they do not fit.
    pub(crate) fn try_reserve_retained(
        self: &Arc<Self>,
        bytes: u64,
    ) -> Result<ProcessMemoryPermit, String> {
        let _guard = self
            .admission
            .lock()
            .expect("process memory state poisoned");
        let limit = self.admission_limit();
        self.reserved
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                let requested = current.saturating_add(bytes);
                (requested <= limit).then_some(requested)
            })
            .map_err(|current| {
                let requested = current.saturating_add(bytes);
                format!(
                    "WEAVER_RESOURCE_LIMIT[memory]: reservation would reach {requested} bytes; process limit is {limit}"
                )
            })?;
        Ok(ProcessMemoryPermit {
            budget: Arc::clone(self),
            bytes,
        })
    }

    // Record `bytes` the process already holds, or is about to hold, until
    // the returned permit drops. Never refuses: the charge is recorded as it
    // is, even past the limit, and only counts against the reservations the
    // repeated-article cache asks for afterwards.
    pub(crate) fn charge_retained(self: &Arc<Self>, bytes: u64) -> ProcessMemoryPermit {
        let _guard = self
            .admission
            .lock()
            .expect("process memory state poisoned");
        self.reserved.fetch_add(bytes, Ordering::AcqRel);
        ProcessMemoryPermit {
            budget: Arc::clone(self),
            bytes,
        }
    }

    #[cfg(test)]
    pub(crate) fn reserved_bytes(&self) -> u64 {
        self.reserved.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn limit(&self) -> u64 {
        self.limit
    }
}

#[derive(Debug)]
pub(crate) struct ProcessMemoryPermit {
    budget: Arc<ProcessMemoryBudget>,
    bytes: u64,
}

impl Drop for ProcessMemoryPermit {
    fn drop(&mut self) {
        let _guard = self
            .budget
            .admission
            .lock()
            .expect("process memory state poisoned");
        self.budget.reserved.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> ExtractionLimits {
        ExtractionLimits {
            max_job_bytes: 32,
            max_member_bytes: 24,
            max_entries: 8,
            max_ratio: 1,
            ratio_floor_bytes: 1 << 30,
            max_seconds: 60,
            min_free_bytes: 1,
            max_memory_bytes: 64 * MIB,
        }
    }

    #[test]
    fn a_budget_view_taken_before_the_limit_changes_keeps_its_limit() {
        let wide = Arc::new(ProcessMemoryBudget::new(1024));
        let narrow = Arc::new(wide.with_limit(512));

        assert_eq!(wide.limit(), 1024);
        assert_eq!(narrow.limit(), 512);
        assert!(narrow.try_reserve_retained(900).is_err());
        let held = wide
            .try_reserve_retained(900)
            .expect("the earlier view keeps its limit");
        assert_eq!(narrow.reserved_bytes(), 900, "the views share one pool");
        drop(held);
        assert_eq!(wide.reserved_bytes(), 0);
    }

    #[test]
    fn a_lowered_limit_admits_retained_state_up_to_the_old_one_until_reservations_drain() {
        let wide = Arc::new(ProcessMemoryBudget::new(1024));
        let held = wide.try_reserve_retained(700).unwrap();
        let narrow = Arc::new(wide.with_limit(512));

        // Reservations made under the wider limit are still above the new
        // one: a submission is judged against the old limit, not refused.
        let submitted = narrow.try_reserve_retained(200).unwrap();
        assert_eq!(narrow.reserved_bytes(), 900);
        assert!(
            narrow.try_reserve_retained(200).is_err(),
            "the old limit still bounds the pool"
        );

        // Once the pool is back within the new limit, the new limit applies.
        drop(held);
        assert_eq!(narrow.reserved_bytes(), 200);
        let error = narrow
            .try_reserve_retained(400)
            .expect_err("the new limit applies once reservations fit it");
        assert!(error.contains("process limit is 512"), "{error}");
        let fits = narrow.try_reserve_retained(312).unwrap();
        assert_eq!(narrow.reserved_bytes(), 512);
        drop(fits);
        drop(submitted);
        assert_eq!(narrow.reserved_bytes(), 0);
    }

    #[test]
    fn a_raised_limit_admits_more_at_once() {
        let narrow = Arc::new(ProcessMemoryBudget::new(512));
        let held = narrow.try_reserve_retained(500).unwrap();
        assert!(narrow.try_reserve_retained(100).is_err());

        let wide = Arc::new(narrow.with_limit(1024));
        assert_eq!(wide.limit(), 1024);
        let more = wide.try_reserve_retained(500).unwrap();
        assert_eq!(wide.reserved_bytes(), 1000);
        drop(more);
        drop(held);
        assert_eq!(narrow.reserved_bytes(), 0);
    }

    #[test]
    fn a_profile_ceiling_is_clamped_unless_the_environment_pins_it() {
        let base = ExtractionLimits {
            max_memory_bytes: 3 * GIB,
            ..limits()
        };
        assert_eq!(
            base.with_memory_ceiling(2 * GIB, false).max_memory_bytes,
            2 * GIB
        );
        assert_eq!(
            base.with_memory_ceiling(MIB, false).max_memory_bytes,
            MIN_MEMORY_LIMIT
        );
        assert_eq!(
            base.with_memory_ceiling(512 * GIB, false).max_memory_bytes,
            MAX_MEMORY_LIMIT
        );
        assert_eq!(
            base.with_memory_ceiling(2 * GIB, true).max_memory_bytes,
            3 * GIB
        );

        // Only the memory ceiling depends on the profile.
        let other = base.with_memory_ceiling(2 * GIB, false);
        assert_eq!(other.max_job_bytes, base.max_job_bytes);
        assert_eq!(other.max_member_bytes, base.max_member_bytes);
        assert_eq!(other.max_entries, base.max_entries);
        assert_eq!(other.min_free_bytes, base.min_free_bytes);
    }
}
