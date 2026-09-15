//! Typed PAR3 outcomes.
//!
//! Every terminal verdict the PAR3 path can reach is one of these classes, so
//! the failure message, the exported counter and the retained per-job record
//! all come from one value instead of from an ad-hoc string at each call site.
//! Engine I/O and internal-state errors are *not* outcomes and keep their own
//! error text.

use super::*;
use crate::operations::metrics::{Par3AdmissionReason, Par3OutcomeClass};
use par3_rs::session::RecoveryRequirement;

/// One cohort's shortfall, as the engine's assessment reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) struct CohortDeficit {
    pub matrix: par3_rs::Fingerprint,
    pub cohort: u64,
    pub cohorts: u64,
    pub lost: u64,
    /// Distinct compatible recovery indices already held.
    pub available: u64,
    /// Minimum further recovery blocks this cohort needs.
    pub additional: u64,
}

impl CohortDeficit {
    pub fn from_requirement(need: &RecoveryRequirement) -> Self {
        Self {
            matrix: need.matrix,
            cohort: need.cohort,
            cohorts: need.cohorts,
            lost: need.lost,
            available: need.available.len() as u64,
            additional: need.additional,
        }
    }

    /// First four fingerprint bytes, enough to tell two matrices apart in a
    /// message without printing a full hash.
    fn matrix_prefix(&self) -> String {
        self.matrix
            .iter()
            .take(4)
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}

impl std::fmt::Display for CohortDeficit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "matrix {} cohort {}/{} (lost {}, available {}, short {})",
            self.matrix_prefix(),
            self.cohort,
            self.cohorts,
            self.lost,
            self.available,
            self.additional
        )
    }
}

/// How far short of an executable plan the authenticated metadata is.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::pipeline) struct MissingMetadata {
    /// Sets whose Start/Root or referenced children have not all arrived.
    pub sets: u64,
    /// Carriers still reporting a gap the scanner needs before it can resume.
    pub carriers_awaiting_bytes: u64,
    /// Referenced files with no authenticated layout entry yet.
    pub unresolved_files: u64,
    /// A packet family no carrier produced a single authenticated copy of.
    /// This is a different statement from `sets`: the set is not merely
    /// incomplete, it is short one of the packets nothing can proceed without,
    /// and no further carrier byte of the ones already scanned will supply it.
    pub missing_vital: Option<super::carriers::Par3PacketKind>,
}

impl std::fmt::Display for MissingMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} set(s), {} carrier(s) awaiting bytes, {} unresolved file(s)",
            self.sets, self.carriers_awaiting_bytes, self.unresolved_files
        )?;
        if let Some(kind) = self.missing_vital {
            write!(f, ", no authenticated {} packet", kind.label())?;
        }
        Ok(())
    }
}

/// A terminal or near-terminal PAR3 verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::pipeline) enum Par3Outcome {
    /// At least one cohort's deficit exceeds every admissible recovery index
    /// that exists in the set, and donor search cannot close it.
    Unrecoverable { cohorts: Vec<CohortDeficit> },
    /// Not a failure: the acquisition plan for the cohorts still short.
    NeedsRecovery {
        cohorts: Vec<CohortDeficit>,
        bytes: u64,
    },
    /// Authenticated metadata is incomplete, as far as the assessment exposes.
    MetadataIncomplete { missing: MissingMetadata },
    /// A matrix kind or geometry the engine does not execute.
    Unsupported { detail: &'static str },
    /// Admissible, but over a hard engine ceiling.
    NotExecutable { limit: &'static str },
    /// Fits alone, but not beside the peer work unit currently holding
    /// PAR3 memory.
    WaitingForMemory { need: u64, have: u64 },
    /// Cannot fit even alone under the configured budget.
    DoesNotFit { need: u64, limit: u64 },
    /// Informational: what the carrier scanner refused or could not read,
    /// with the carriers it happened on.
    CarrierDamage {
        carriers: Vec<super::carriers::CarrierDamage>,
        rejected_packets: u64,
        unavailable_ranges: u64,
        damaged_bytes: u64,
    },
    /// A set names a file weaver will not create under that name.
    UnsafePath {
        path: String,
        reason: super::paths::UnsafePath,
    },
    /// The working directory cannot hold the set's outputs and the staging
    /// copy the installation writes beside them.
    NoOutputSpace {
        need: u64,
        available: u64,
        shortfall: u64,
    },
}

impl Par3Outcome {
    pub fn class(&self) -> Par3OutcomeClass {
        match self {
            Self::Unrecoverable { .. } => Par3OutcomeClass::Unrecoverable,
            Self::NeedsRecovery { .. } => Par3OutcomeClass::NeedsRecovery,
            Self::MetadataIncomplete { .. } => Par3OutcomeClass::MetadataIncomplete,
            Self::Unsupported { .. } => Par3OutcomeClass::Unsupported,
            Self::NotExecutable { .. } => Par3OutcomeClass::NotExecutable,
            Self::WaitingForMemory { .. } => Par3OutcomeClass::WaitingForMemory,
            Self::DoesNotFit { .. } => Par3OutcomeClass::DoesNotFit,
            Self::CarrierDamage { .. } => Par3OutcomeClass::CarrierDamage,
            Self::UnsafePath { .. } => Par3OutcomeClass::UnsafePath,
            Self::NoOutputSpace { .. } => Par3OutcomeClass::NoOutputSpace,
        }
    }

    /// Whether this class ends the job. `NeedsRecovery` and `CarrierDamage`
    /// describe a plan or a condition, and `WaitingForMemory` describes a
    /// collision that the peer work unit's handback resolves; none of the
    /// three fails a job on its own.
    pub fn is_terminal(&self) -> bool {
        !matches!(
            self,
            Self::NeedsRecovery { .. } | Self::CarrierDamage { .. } | Self::WaitingForMemory { .. }
        )
    }

    /// A bounded rendering of a damage summary, on the same rule as the
    /// cohort list: a few entries and a count of the rest, never one line per
    /// carrier in a set that has hundreds.
    fn render_carriers(carriers: &[super::carriers::CarrierDamage]) -> String {
        const SHOWN: usize = 3;
        let mut rendered = carriers
            .iter()
            .take(SHOWN)
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ");
        if carriers.len() > SHOWN {
            rendered.push_str(&format!("; and {} more", carriers.len() - SHOWN));
        }
        rendered
    }

    /// A refused path, shortened so a hostile name cannot flood a message or
    /// a log line with its own length.
    fn render_path(path: &str) -> String {
        const SHOWN: usize = 120;
        if path.len() <= SHOWN {
            return path.to_string();
        }
        let mut cut = SHOWN;
        while cut > 0 && !path.is_char_boundary(cut) {
            cut -= 1;
        }
        format!("{}...", &path[..cut])
    }

    fn render_cohorts(cohorts: &[CohortDeficit]) -> String {
        const SHOWN: usize = 4;
        let mut rendered = cohorts
            .iter()
            .take(SHOWN)
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ");
        if cohorts.len() > SHOWN {
            rendered.push_str(&format!("; and {} more", cohorts.len() - SHOWN));
        }
        rendered
    }
}

impl std::fmt::Display for Par3Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unrecoverable { cohorts } if cohorts.is_empty() => {
                f.write_str("PAR3 recovery exhausted: compatible recovery remains insufficient")
            }
            Self::Unrecoverable { cohorts } => write!(
                f,
                "PAR3 recovery exhausted: no admissible recovery remains for {}",
                Self::render_cohorts(cohorts)
            ),
            Self::NeedsRecovery { cohorts, bytes } => write!(
                f,
                "PAR3 needs {bytes} more recovery bytes for {}",
                Self::render_cohorts(cohorts)
            ),
            Self::MetadataIncomplete { missing } => write!(
                f,
                "PAR3 recovery exhausted: authenticated metadata remains incomplete ({missing})"
            ),
            Self::Unsupported { detail } => {
                write!(
                    f,
                    "PAR3 set requires an unsupported repair geometry: {detail}"
                )
            }
            Self::NotExecutable { limit } => {
                write!(f, "PAR3 repair exceeds an engine execution limit: {limit}")
            }
            Self::WaitingForMemory { need, have } => write!(
                f,
                "PAR3 repair is waiting for memory: needs {need} bytes, {have} available beside \
                 the work unit in flight"
            ),
            Self::DoesNotFit { need, limit } => write!(
                f,
                "PAR3 memory admission failed: needs {need} bytes against a {limit} byte budget"
            ),
            Self::CarrierDamage {
                carriers,
                rejected_packets,
                unavailable_ranges,
                damaged_bytes,
            } => {
                write!(
                    f,
                    "PAR3 carriers rejected {rejected_packets} packet(s), left \
                     {unavailable_ranges} range(s) unreadable and could not \
                     authenticate {damaged_bytes} byte(s)"
                )?;
                if carriers.is_empty() {
                    return Ok(());
                }
                write!(f, ": {}", Self::render_carriers(carriers))
            }
            Self::UnsafePath { path, reason } => write!(
                f,
                "PAR3 set names a file weaver will not create: {reason} in {:?}",
                Self::render_path(path)
            ),
            Self::NoOutputSpace {
                need,
                available,
                shortfall,
            } => write!(
                f,
                "PAR3 output planning is {shortfall} bytes short: installing this set needs \
                 {need} bytes and the working directory can grant {available}"
            ),
        }
    }
}

/// Decide whether a refused PAR3 reservation is a transient peer collision or
/// a budget the set can never fit into.
///
/// **This is an inference, not engine data.** par3-rs 0.3.1 reports only
/// `ResourceLimit(&'static str)`: it does not say how many bytes the refused
/// operation wanted, nor how many the budget could ever grant. Until the
/// engine reports its own `{need, limit, available}` for a refusal, weaver
/// infers both from the budget it configured and from whether another work
/// unit held PAR3 memory at the moment of refusal. Replace the whole body when
/// that engine data exists; the callers and the two outcome classes stay.
pub(in crate::pipeline) fn classify_memory_refusal(
    need: u64,
    limit: u64,
    available: u64,
    peer_in_flight: bool,
) -> Par3Outcome {
    if peer_in_flight && need <= limit {
        Par3Outcome::WaitingForMemory {
            need,
            have: available,
        }
    } else {
        Par3Outcome::DoesNotFit { need, limit }
    }
}

/// The engine ceiling an error names, when it names one.
///
/// Hostile or merely enormous metadata — thousands of File packets, a
/// directory tree that nests without end, a chunk list longer than the data it
/// describes — reaches weaver as a named `ResourceLimit` rather than as host
/// exhaustion. Naming that ceiling turns it into a typed verdict instead of an
/// opaque engine string.
pub(in crate::pipeline) fn execution_limit(error: &EngineError) -> Option<&'static str> {
    match error {
        EngineError::ResourceLimit(limit) => Some(limit),
        EngineError::Io(error) => error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<EngineError>())
            .and_then(execution_limit),
        EngineError::RepairInterrupted { cause, .. } => execution_limit(cause),
        _ => None,
    }
}

/// Which admission budget an engine resource limit names. The strings are the
/// engine's own labels and the ones weaver passes to `ResourceLimit`.
pub(in crate::pipeline) fn admission_reason(error: &EngineError) -> Par3AdmissionReason {
    match error {
        EngineError::ResourceLimit(label) => match *label {
            "memory budget" | "minimum repair stripe" | "open handles" => {
                Par3AdmissionReason::RetainedState
            }
            "PAR3 retained payload" => Par3AdmissionReason::ResolvedMetadata,
            "PAR3 host state" | "PAR3 assessment view size" => Par3AdmissionReason::AssessmentView,
            "PAR3 disk fallback space" => Par3AdmissionReason::DiskFallbackSpace,
            "job carrier count"
            | "PAR3 disk publications"
            | "PAR3 source bindings"
            | "PAR3 source count"
            | "PAR3 source ranges" => Par3AdmissionReason::CarrierCount,
            "job PAR3 set count" | "PAR3 job count" => Par3AdmissionReason::SetCount,
            _ => Par3AdmissionReason::Other,
        },
        EngineError::Io(error) => error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<EngineError>())
            .map_or(Par3AdmissionReason::Other, admission_reason),
        EngineError::RepairInterrupted { cause, .. } => admission_reason(cause),
        _ => Par3AdmissionReason::Other,
    }
}

impl Pipeline {
    /// Count a typed verdict once, retain it on the job, and fail the job with
    /// the outcome's own message when the class is terminal. Every PAR3 path
    /// that reaches a verdict goes through here, so the exported counter and
    /// the user-facing text can never disagree.
    pub(in crate::pipeline) fn settle_par3_outcome(&mut self, job_id: JobId, outcome: Par3Outcome) {
        let message = outcome.to_string();
        let terminal = outcome.is_terminal();
        // Any verdict other than the wait itself ends a memory wait, so the
        // active gauge cannot survive the job failing, completing, or simply
        // reaching a different answer while it was parked.
        let waiting = matches!(outcome, Par3Outcome::WaitingForMemory { .. });
        match self.par3_runtime.as_mut() {
            Some(runtime) => {
                if !waiting {
                    runtime.resume_from_memory(job_id);
                }
                runtime.note_outcome(job_id, outcome);
            }
            // A verdict can be reached before admission created a coordinator.
            None => self.metrics.par3.note_outcome(outcome.class()),
        }
        if terminal {
            self.fail_job(job_id, message);
        }
    }

    /// The deficient cohorts of every retained assessment for this job, and
    /// the bytes they are short by. Publishes the two acquisition gauges.
    pub(in crate::pipeline) fn par3_cohort_plan(
        &self,
        job_id: JobId,
    ) -> super::cohorts::CohortPlan {
        use std::sync::atomic::Ordering::Relaxed;
        let mut plan = super::cohorts::CohortPlan::default();
        if let Some(runtime) = self.par3_runtime.as_ref() {
            for (_, view) in runtime.assessments(job_id) {
                plan.push_view(&view.requirements, view.block_size);
                plan.metadata_incomplete |=
                    view.status == par3_rs::session::RepairStatus::IncompleteMetadata;
            }
        }
        let par3 = &self.metrics.par3;
        par3.recovery_needed_bytes.store(plan.needed_bytes, Relaxed);
        par3.cohorts_with_deficit.store(plan.windows.len(), Relaxed);
        plan
    }

    /// Classify a refused PAR3 memory reservation and act on the verdict: a
    /// collision with the peer work unit parks the job until that unit hands
    /// back, while a set that cannot fit alone fails now.
    pub(in crate::pipeline) fn refuse_par3_memory(
        &mut self,
        job_id: JobId,
        source: SourceId,
        need: u64,
    ) {
        let Some(runtime) = self.par3_runtime.as_ref() else {
            return;
        };
        let (limit, available) = runtime.native_budget();
        let outcome = classify_memory_refusal(
            need,
            limit,
            available,
            runtime.peer_holds_par3_memory(job_id),
        );
        let waiting = matches!(outcome, Par3Outcome::WaitingForMemory { .. });
        self.settle_par3_outcome(job_id, outcome);
        if let Some(runtime) = self.par3_runtime.as_mut() {
            if waiting {
                runtime.park_for_memory(job_id, source);
            } else {
                runtime.resume_from_memory(job_id);
            }
        }
    }

    /// What the authenticated metadata is still short of, as far as the
    /// retained assessments and the carrier scanners expose it.
    pub(in crate::pipeline) fn par3_missing_metadata(&self, job_id: JobId) -> MissingMetadata {
        let Some(runtime) = self.par3_runtime.as_ref() else {
            return MissingMetadata::default();
        };
        let mut missing = MissingMetadata {
            carriers_awaiting_bytes: runtime.carriers_awaiting_bytes(job_id),
            missing_vital: runtime.missing_vital_packet(job_id),
            ..MissingMetadata::default()
        };
        for (_, view) in runtime.assessments(job_id) {
            if view.status == par3_rs::session::RepairStatus::IncompleteMetadata {
                missing.sets = missing.sets.saturating_add(1);
            }
            missing.unresolved_files = missing.unresolved_files.saturating_add(
                view.files
                    .iter()
                    .filter(|file| file.source.is_none() || !file.unresolved.is_empty())
                    .count() as u64,
            );
        }
        missing
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deficit(cohort: u64, additional: u64) -> CohortDeficit {
        CohortDeficit {
            matrix: [0xab; 16],
            cohort,
            cohorts: 2,
            lost: 3,
            available: 1,
            additional,
        }
    }

    #[test]
    fn every_class_has_a_distinct_index_and_a_message() {
        let outcomes = [
            Par3Outcome::Unrecoverable {
                cohorts: vec![deficit(1, 1)],
            },
            Par3Outcome::NeedsRecovery {
                cohorts: vec![deficit(0, 2)],
                bytes: 4096,
            },
            Par3Outcome::MetadataIncomplete {
                missing: MissingMetadata {
                    sets: 1,
                    carriers_awaiting_bytes: 2,
                    unresolved_files: 3,
                    missing_vital: Some(super::super::carriers::Par3PacketKind::Root),
                },
            },
            Par3Outcome::Unsupported { detail: "matrix" },
            Par3Outcome::NotExecutable { limit: "lost cap" },
            Par3Outcome::WaitingForMemory { need: 10, have: 4 },
            Par3Outcome::DoesNotFit { need: 10, limit: 4 },
            Par3Outcome::CarrierDamage {
                carriers: vec![super::super::carriers::CarrierDamage {
                    source: SourceId(3),
                    first_damage_offset: 672,
                    damaged_bytes: 109,
                    rejected: 2,
                    unavailable_ranges: 1,
                }],
                rejected_packets: 2,
                unavailable_ranges: 1,
                damaged_bytes: 109,
            },
            Par3Outcome::UnsafePath {
                path: "sub/CON".into(),
                reason: super::super::paths::UnsafePath::ReservedDeviceName,
            },
            Par3Outcome::NoOutputSpace {
                need: 4096,
                available: 1024,
                shortfall: 3072,
            },
        ];
        let mut seen = std::collections::BTreeSet::new();
        for outcome in &outcomes {
            assert!(
                seen.insert(outcome.class().index()),
                "duplicate outcome index for {outcome:?}"
            );
            assert!(!outcome.to_string().is_empty());
        }
        assert_eq!(seen.len(), Par3OutcomeClass::COUNT);
        assert!(outcomes[0].is_terminal());
        assert!(!outcomes[1].is_terminal());
        assert!(!outcomes[7].is_terminal());
    }

    #[test]
    fn memory_refusal_separates_a_peer_collision_from_an_impossible_set() {
        assert_eq!(
            classify_memory_refusal(100, 400, 150, true),
            Par3Outcome::WaitingForMemory {
                need: 100,
                have: 150
            }
        );
        assert_eq!(
            classify_memory_refusal(100, 400, 150, false),
            Par3Outcome::DoesNotFit {
                need: 100,
                limit: 400
            }
        );
        // Over the whole budget is never a peer collision, even under one.
        assert_eq!(
            classify_memory_refusal(900, 400, 0, true),
            Par3Outcome::DoesNotFit {
                need: 900,
                limit: 400
            }
        );
    }

    #[test]
    fn admission_reasons_follow_the_budget_the_engine_named() {
        for (label, expected) in [
            ("memory budget", Par3AdmissionReason::RetainedState),
            ("PAR3 host state", Par3AdmissionReason::AssessmentView),
            (
                "PAR3 retained payload",
                Par3AdmissionReason::ResolvedMetadata,
            ),
            (
                "PAR3 disk fallback space",
                Par3AdmissionReason::DiskFallbackSpace,
            ),
            ("job carrier count", Par3AdmissionReason::CarrierCount),
            ("job PAR3 set count", Par3AdmissionReason::SetCount),
            ("something else entirely", Par3AdmissionReason::Other),
        ] {
            assert_eq!(
                admission_reason(&EngineError::ResourceLimit(label)),
                expected,
                "{label}"
            );
        }
        assert_eq!(
            admission_reason(&EngineError::Cancelled),
            Par3AdmissionReason::Other
        );
    }

    #[test]
    fn a_long_cohort_list_renders_without_growing_without_bound() {
        let cohorts: Vec<_> = (0..9).map(|index| deficit(index, 1)).collect();
        let rendered = Par3Outcome::Unrecoverable { cohorts }.to_string();
        assert!(rendered.contains("and 5 more"), "{rendered}");
    }
}
