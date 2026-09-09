//! Retained ownership and bounded dispatch for PAR3 blocking work.

use super::*;
use crate::pipeline::RepairWorkDone;
use par3_rs::runtime::CancellationToken;
use tokio::sync::mpsc;

const MAX_JOBS: usize = 256;
const MAX_PENDING: usize = 4096;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum WorkKey {
    Source(SourceId),
    Repair(par3_rs::InputSetId),
    Readback,
}

enum WorkOutput {
    Published,
    Repaired(RepairCompletion),
    Readback(readback::ReadbackDone),
}

/// Keep output-path accounting alive through channel handback, partial-error
/// handling and asynchronous assembly/database reconciliation.
pub(super) struct RepairCompletion {
    pub result: EngineResult<par3_rs::session_repair::SessionRepairReport>,
    pub outputs: EngineResult<Vec<readback::VerifiedOutput>>,
    /// True only when the embedded repair path installed a replacement.
    pub embedded_replacement: bool,
    pub _reservation: Option<assessment::ViewReservation>,
}

enum PendingInput {
    Readback(Box<readback::Installation>),
    Virtual {
        image: virtual_source::VirtualInput,
        name: String,
    },
    Repair {
        set: par3_rs::InputSetId,
        path: PathBuf,
    },
    CompleteFile {
        path: PathBuf,
        name: String,
    },
    Embedded {
        path: PathBuf,
        name: String,
        ranges: Option<Vec<std::ops::Range<u64>>>,
        start: u64,
    },
    Carrier {
        path: PathBuf,
        ranges: Option<Vec<std::ops::Range<u64>>>,
    },
    File {
        path: PathBuf,
        name: String,
        ranges: Vec<std::ops::Range<u64>>,
    },
}

impl PendingInput {
    fn retained_cost(&self) -> EngineResult<usize> {
        let (path, extra) = match self {
            Self::Readback(_) => return Ok(readback::STRIPE_RESERVATION),
            Self::Virtual { name, .. } => {
                return name
                    .capacity()
                    .checked_mul(2)
                    .and_then(|cost| cost.checked_add(1024))
                    .ok_or(EngineError::ResourceLimit("PAR3 virtual publication"));
            }
            Self::Repair { path, .. } => (path, Some(0)),
            Self::CompleteFile { path, name } => (path, name.capacity().checked_mul(2)),
            Self::Embedded {
                path, name, ranges, ..
            } => (
                path,
                name.capacity().checked_mul(2).and_then(|bytes| {
                    bytes.checked_add(
                        ranges
                            .as_ref()
                            .map_or(Some(0), |ranges| ranges.capacity().checked_mul(32))?,
                    )
                }),
            ),
            Self::Carrier { path, ranges } => (
                path,
                ranges
                    .as_ref()
                    .map_or(Some(0), |ranges| ranges.capacity().checked_mul(32)),
            ),
            Self::File { path, name, ranges } => (
                path,
                name.capacity()
                    .checked_mul(2)
                    .and_then(|bytes| bytes.checked_add(ranges.capacity().checked_mul(32)?)),
            ),
        };
        1024usize
            .checked_add(
                path.capacity()
                    .checked_mul(2)
                    .ok_or(EngineError::ResourceLimit("PAR3 queued path"))?,
            )
            .and_then(|bytes| bytes.checked_add(extra?))
            .ok_or(EngineError::ResourceLimit("PAR3 queued publication"))
    }
}

struct QueuedInput {
    input: PendingInput,
    reservation: assessment::ViewReservation,
}

struct KnownSource {
    carrier: bool,
    embedded_start: Option<u64>,
    complete_disk_image: bool,
    promoted: BTreeMap<u32, assessment::ViewReservation>,
    // Retained source, dirty and error bookkeeping outlives queued work,
    // including failed publications which never entered the engine.
    _reservation: assessment::ViewReservation,
}

struct JobSlot {
    runtime: Option<Par3Job>,
    sources: PublishedSources,
    epoch: u64,
    last_used: u64,
    known: BTreeMap<SourceId, KnownSource>,
    dirty: std::collections::BTreeSet<SourceId>,
    pending: BTreeMap<WorkKey, QueuedInput>,
    ticket: Option<u64>,
    errors: BTreeMap<SourceId, EngineError>,
    completed_repair: Option<RepairCompletion>,
    completed_readback: Option<EngineResult<readback::ReadbackDone>>,
    installing: bool,
    verification: Option<verification::Receipt>,
}

impl Default for JobSlot {
    fn default() -> Self {
        let runtime = Par3Job::default();
        Self {
            sources: runtime.sources.clone(),
            runtime: Some(runtime),
            epoch: 0,
            last_used: 0,
            known: BTreeMap::new(),
            dirty: std::collections::BTreeSet::new(),
            pending: BTreeMap::new(),
            ticket: None,
            errors: BTreeMap::new(),
            completed_repair: None,
            completed_readback: None,
            installing: false,
            verification: None,
        }
    }
}

pub(crate) struct WorkDone {
    job_id: JobId,
    ticket: u64,
    epoch: u64,
    key: WorkKey,
    runtime: Option<Par3Job>,
    result: EngineResult<WorkOutput>,
}

/// Created only on PAR3 admission. One carrier worker bounds dispatch even
/// when many jobs arrive together. Retired tickets hold capacity until their
/// cancelled workers return; recreating a job cannot evade that bound.
pub(in crate::pipeline) struct Coordinator {
    jobs: BTreeMap<JobId, JobSlot>,
    in_flight: BTreeMap<u64, (JobId, CancellationToken)>,
    next_ticket: u64,
    last_job: Option<JobId>,
    tx: mpsc::Sender<RepairWorkDone>,
    #[cfg(test)]
    test_rx: Option<mpsc::Receiver<RepairWorkDone>>,
}

#[cfg(test)]
impl Default for Coordinator {
    fn default() -> Self {
        let (tx, rx) = mpsc::channel(1);
        let mut coordinator = Self::new(tx);
        coordinator.test_rx = Some(rx);
        coordinator
    }
}

impl Coordinator {
    pub(super) fn new(tx: mpsc::Sender<RepairWorkDone>) -> Self {
        Self {
            jobs: BTreeMap::new(),
            in_flight: BTreeMap::new(),
            next_ticket: 0,
            last_job: None,
            tx,
            #[cfg(test)]
            test_rx: None,
        }
    }
    pub(in crate::pipeline) fn authenticated_set_count(&self, job_id: JobId) -> usize {
        self.jobs
            .get(&job_id)
            .and_then(|job| job.runtime.as_ref())
            .map_or(0, |runtime| {
                runtime.sets.len()
                    + runtime
                        .dormant_views
                        .keys()
                        .filter(|id| !runtime.sets.contains_key(id))
                        .count()
            })
    }

    pub(in crate::pipeline) fn has_work(&self, job_id: JobId) -> bool {
        self.jobs.get(&job_id).is_some_and(|job| {
            job.installing
                || job.ticket.is_some()
                || !job.pending.is_empty()
                || !job.dirty.is_empty()
        })
    }

    #[cfg(test)]
    pub(super) fn enqueue(
        &mut self,
        job_id: JobId,
        source: SourceId,
        path: PathBuf,
    ) -> EngineResult<()> {
        self.enqueue_complete_carrier(job_id, source, path)
    }

    pub(super) fn enqueue_complete_carrier(
        &mut self,
        job_id: JobId,
        source: SourceId,
        path: PathBuf,
    ) -> EngineResult<()> {
        self.enqueue_input(job_id, source, PendingInput::Carrier { path, ranges: None })
    }

    pub(super) fn enqueue_carrier_ranges(
        &mut self,
        job_id: JobId,
        source: SourceId,
        path: PathBuf,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> EngineResult<()> {
        self.enqueue_input(
            job_id,
            source,
            PendingInput::Carrier {
                path,
                ranges: Some(ranges),
            },
        )
    }

    pub(super) fn enqueue_embedded(
        &mut self,
        job_id: JobId,
        source: SourceId,
        path: PathBuf,
        name: String,
        ranges: Option<Vec<std::ops::Range<u64>>>,
        start: u64,
    ) -> EngineResult<()> {
        self.enqueue_input(
            job_id,
            source,
            PendingInput::Embedded {
                path,
                name,
                ranges,
                start,
            },
        )
    }

    pub(super) fn embedded_start(&self, job_id: JobId, source: SourceId) -> Option<u64> {
        self.jobs.get(&job_id)?.known.get(&source)?.embedded_start
    }

    pub(super) fn has_complete_disk_image(&self, job_id: JobId, source: SourceId) -> bool {
        self.jobs
            .get(&job_id)
            .and_then(|job| job.known.get(&source))
            .is_some_and(|known| known.complete_disk_image)
    }

    pub(super) fn enqueue_file(
        &mut self,
        job_id: JobId,
        source: SourceId,
        path: PathBuf,
        name: String,
        ranges: Vec<std::ops::Range<u64>>,
    ) -> EngineResult<()> {
        self.enqueue_input(job_id, source, PendingInput::File { path, name, ranges })
    }

    pub(super) fn contains_job(&self, job_id: JobId) -> bool {
        self.jobs.contains_key(&job_id)
    }

    pub(super) fn enqueue_virtual(
        &mut self,
        job_id: JobId,
        source: SourceId,
        volume: crate::pipeline::direct_store::provider::VirtualVolume,
        name: String,
    ) -> EngineResult<()> {
        let image = virtual_source::VirtualInput::new(volume, &execution_options())?;
        self.enqueue_input(job_id, source, PendingInput::Virtual { image, name })
    }

    pub(super) fn admit(&mut self, job_id: JobId) -> EngineResult<()> {
        if !self.jobs.contains_key(&job_id) && self.jobs.len() >= MAX_JOBS {
            return Err(EngineError::ResourceLimit("PAR3 job count"));
        }
        self.jobs.entry(job_id).or_default();
        Ok(())
    }

    /// Publish actual disk bytes as candidates; this does not admit verification evidence.
    pub(super) fn enqueue_complete_file(
        &mut self,
        job_id: JobId,
        source: SourceId,
        path: PathBuf,
        name: String,
    ) -> EngineResult<()> {
        self.enqueue_input(job_id, source, PendingInput::CompleteFile { path, name })
    }

    pub(super) fn request_repair(
        &mut self,
        job_id: JobId,
        set: par3_rs::InputSetId,
        path: PathBuf,
    ) -> EngineResult<()> {
        let Some((_, view)) = self.assessments(job_id).find(|(id, view)| {
            *id == set
                && (view.status == par3_rs::session::RepairStatus::Ready
                    || (view.status == par3_rs::session::RepairStatus::Complete
                        && view
                            .embedded_source
                            .is_some_and(|source| self.embedded_start(job_id, source).is_some())))
        }) else {
            return Err(EngineError::InvalidState("PAR3 repair is not ready"));
        };
        // Reserve before execution can install anything. Include the native
        // report, partial-error temporary paths, and reconciliation copies.
        let replacing_carrier = view.status == par3_rs::session::RepairStatus::Complete;
        let outputs = view
            .files
            .iter()
            .filter(|file| replacing_carrier || !file.complete)
            .try_fold(2048usize, |bytes, file| {
                bytes
                    .checked_add(4096)?
                    .checked_add(path.capacity().checked_mul(16)?)?
                    .checked_add(file.path.len().checked_mul(16)?)
            });
        let outputs = outputs.ok_or(EngineError::ResourceLimit("PAR3 repair result paths"))?;
        self.check_pending_capacity(job_id, WorkKey::Repair(set))?;
        let input = PendingInput::Repair { set, path };
        let reservation = assessment::ViewReservation::acquire(
            input
                .retained_cost()?
                .checked_add(outputs)
                .ok_or(EngineError::ResourceLimit("PAR3 repair result paths"))?,
        )?;
        self.jobs
            .get_mut(&job_id)
            .expect("assessed job")
            .pending
            .insert(WorkKey::Repair(set), QueuedInput { input, reservation });
        self.dispatch()
    }

    pub(super) fn is_installing(&self, job_id: JobId) -> bool {
        self.jobs.get(&job_id).is_some_and(|job| job.installing)
    }

    pub(super) fn queue_readback(
        &mut self,
        job_id: JobId,
        installation: Box<readback::Installation>,
    ) -> EngineResult<()> {
        self.check_pending_capacity(job_id, WorkKey::Readback)?;
        let reservation = assessment::ViewReservation::acquire(readback::STRIPE_RESERVATION)?;
        let job = self
            .jobs
            .get_mut(&job_id)
            .ok_or(EngineError::InvalidState("missing PAR3 installation job"))?;
        if job.ticket.is_some() || job.pending.contains_key(&WorkKey::Readback) {
            return Err(EngineError::InvalidState("PAR3 readback already queued"));
        }
        job.installing = true;
        job.pending.insert(
            WorkKey::Readback,
            QueuedInput {
                input: PendingInput::Readback(installation),
                reservation,
            },
        );
        self.dispatch()
    }

    pub(super) fn take_readback(
        &mut self,
        job_id: JobId,
    ) -> Option<EngineResult<readback::ReadbackDone>> {
        self.jobs.get_mut(&job_id)?.completed_readback.take()
    }

    pub(super) fn finish_installation(&mut self, job_id: JobId) {
        if let Some(job) = self.jobs.get_mut(&job_id) {
            job.installing = false;
        }
    }

    pub(super) fn take_repair_result(&mut self, job_id: JobId) -> Option<RepairCompletion> {
        self.jobs.get_mut(&job_id)?.completed_repair.take()
    }

    pub(super) fn error(&self, job_id: JobId) -> Option<&EngineError> {
        self.jobs.get(&job_id)?.errors.values().next()
    }

    pub(in crate::pipeline) fn is_promoted(&self, job_id: JobId, file_index: u32) -> bool {
        self.jobs
            .get(&job_id)
            .and_then(|job| job.known.get(&SourceId(u64::from(file_index))))
            .is_some_and(|source| !source.promoted.is_empty())
    }

    pub(in crate::pipeline) fn article_promoted(
        &self,
        job_id: JobId,
        file_index: u32,
        ordinal: u32,
    ) -> bool {
        self.jobs
            .get(&job_id)
            .and_then(|job| job.known.get(&SourceId(u64::from(file_index))))
            .is_some_and(|source| source.promoted.contains_key(&ordinal))
    }

    pub(super) fn needed_offset(&self, job_id: JobId, file_index: u32) -> Option<u64> {
        self.jobs
            .get(&job_id)?
            .runtime
            .as_ref()?
            .carriers
            .get(&SourceId(u64::from(file_index)))?
            .needed
    }

    pub(super) fn promote(
        &mut self,
        job_id: JobId,
        file_index: u32,
        ordinal: u32,
    ) -> EngineResult<()> {
        let job = self
            .jobs
            .get_mut(&job_id)
            .ok_or(EngineError::InvalidState("unknown PAR3 job"))?;
        let source = SourceId(u64::from(file_index));
        if !job.known.contains_key(&source) {
            if job.known.len() >= MAX_PENDING {
                return Err(EngineError::ResourceLimit("PAR3 recovery candidates"));
            }
            job.known.insert(
                source,
                KnownSource {
                    carrier: true,
                    embedded_start: None,
                    complete_disk_image: false,
                    promoted: BTreeMap::new(),
                    _reservation: assessment::ViewReservation::acquire(512)?,
                },
            );
        }
        let known = job.known.get_mut(&source).expect("admitted source");
        if let std::collections::btree_map::Entry::Vacant(entry) = known.promoted.entry(ordinal) {
            entry.insert(assessment::ViewReservation::acquire(96)?);
        }
        Ok(())
    }

    pub(super) fn is_carrier(&self, job_id: JobId, source: SourceId) -> bool {
        self.jobs
            .get(&job_id)
            .and_then(|job| job.known.get(&source))
            .is_some_and(|source| source.carrier)
    }

    pub(super) fn knows_source(&self, job_id: JobId, source: SourceId) -> bool {
        self.jobs
            .get(&job_id)
            .is_some_and(|job| job.known.contains_key(&source))
    }

    pub(super) fn dirty_sources(&self, job_id: JobId) -> Vec<SourceId> {
        self.jobs
            .get(&job_id)
            .filter(|job| job.ticket.is_none() && !job.installing)
            .map(|job| {
                job.dirty
                    .iter()
                    .filter(|source| !job.pending.contains_key(&WorkKey::Source(**source)))
                    .copied()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(super) fn invalidate_source(
        &mut self,
        job_id: JobId,
        source: SourceId,
    ) -> EngineResult<()> {
        let Some(job) = self.jobs.get_mut(&job_id) else {
            return Ok(());
        };
        if !job.known.contains_key(&source) {
            return Ok(());
        }
        let epoch = job
            .epoch
            .checked_add(1)
            .ok_or(EngineError::ResourceLimit("PAR3 source epochs"))?;
        job.sources.withdraw(source)?;
        job.known
            .get_mut(&source)
            .expect("known source")
            .complete_disk_image = false;
        job.epoch = epoch;
        job.pending.remove(&WorkKey::Source(source));
        job.pending
            .retain(|key, _| !matches!(key, WorkKey::Repair(_)));
        job.dirty.insert(source);
        if let Some(runtime) = job.runtime.as_mut() {
            for set in runtime.sets.values_mut() {
                set.invalidate(source);
            }
            runtime.carriers.remove(&source);
        }
        Ok(())
    }

    pub(super) fn invalidate_bindings(&mut self, job_id: JobId) -> EngineResult<()> {
        let sources: Vec<_> = self
            .jobs
            .get(&job_id)
            .into_iter()
            .flat_map(|job| job.known.keys().copied())
            .collect();
        for source in sources {
            self.invalidate_source(job_id, source)?;
        }
        Ok(())
    }

    pub(super) fn name_match_source(&self, job_id: JobId) -> Option<SourceId> {
        if self.has_work(job_id) {
            return None;
        }
        self.jobs
            .get(&job_id)
            .filter(|job| job.errors.is_empty())
            .and_then(|job| job.runtime.as_ref())
            .and_then(|runtime| runtime.name_search.source())
    }

    pub(super) fn take_name_match(
        &mut self,
        job_id: JobId,
    ) -> EngineResult<Option<placement::NameMatch>> {
        if self.has_work(job_id) {
            return Ok(None);
        }
        match self
            .jobs
            .get_mut(&job_id)
            .and_then(|job| job.runtime.as_mut())
        {
            Some(runtime) => runtime.take_name_match(),
            None => Ok(None),
        }
    }

    pub(in crate::pipeline) fn assessments(
        &self,
        job_id: JobId,
    ) -> impl Iterator<Item = (par3_rs::InputSetId, &assessment::AssessmentView)> {
        self.jobs
            .get(&job_id)
            .filter(|job| {
                !job.installing
                    && job.ticket.is_none()
                    && job.pending.is_empty()
                    && job.errors.is_empty()
                    && job.dirty.is_empty()
            })
            .and_then(|job| job.runtime.as_ref())
            .into_iter()
            .flat_map(|runtime| {
                runtime
                    .sets
                    .iter()
                    .filter_map(|(&id, set)| set.view.as_ref().map(|view| (id, view)))
                    .chain(runtime.dormant_views.iter().map(|(&id, view)| (id, view)))
            })
    }

    #[cfg(test)]
    pub(in crate::pipeline) fn source_verifications(&self, job_id: JobId) -> u64 {
        self.jobs
            .get(&job_id)
            .and_then(|job| job.runtime.as_ref())
            .map_or(0, |runtime| {
                runtime
                    .sets
                    .values()
                    .map(|set| set.native.diagnostics().source_verifications)
                    .sum()
            })
    }

    pub(in crate::pipeline) fn verified(&self, job_id: JobId) -> bool {
        let count = self.authenticated_set_count(job_id);
        count != 0
            && self
                .assessments(job_id)
                .filter(|(_, view)| view.status == par3_rs::session::RepairStatus::Complete)
                .count()
                == count
    }

    /// A terminal claim must name a bound protected source. A clean job does
    /// not establish evidence for its unprotected files or recovery carriers.
    pub(in crate::pipeline) fn verified_file(&self, job_id: JobId, source: SourceId) -> bool {
        self.verified(job_id) && self.source_verified(job_id, source)
    }

    /// Current native evidence for one bound source can survive damage in a
    /// sibling. Terminal delivery still requires the aggregate verdict above.
    pub(in crate::pipeline) fn source_verified(&self, job_id: JobId, source: SourceId) -> bool {
        self.assessments(job_id)
            .any(|(_, view)| view.verified_sources.contains(&source))
    }

    fn enqueue_input(
        &mut self,
        job_id: JobId,
        source: SourceId,
        input: PendingInput,
    ) -> EngineResult<()> {
        if !self.jobs.contains_key(&job_id) && self.jobs.len() >= MAX_JOBS {
            return Err(EngineError::ResourceLimit("PAR3 job count"));
        }
        self.check_pending_capacity(job_id, WorkKey::Source(source))?;
        let reservation = assessment::ViewReservation::acquire(input.retained_cost()?)?;
        let job = self.jobs.entry(job_id).or_default();
        if !job.known.contains_key(&source) && job.known.len() >= MAX_PENDING {
            return Err(EngineError::ResourceLimit("PAR3 known sources"));
        }
        let carrier = matches!(
            input,
            PendingInput::Carrier { .. } | PendingInput::Embedded { .. }
        );
        // This records byte availability, never a hash verdict. Explicit full
        // disk publications supersede article holes until a later source write
        // or identity rebind withdraws the publication.
        let complete_disk_image = matches!(
            input,
            PendingInput::CompleteFile { .. }
                | PendingInput::Carrier { ranges: None, .. }
                | PendingInput::Embedded { ranges: None, .. }
        );
        let embedded_start = match &input {
            PendingInput::Embedded { start, .. } => Some(*start),
            _ => None,
        };
        if let Some(known) = job.known.get_mut(&source) {
            known.carrier = carrier;
            known.embedded_start = embedded_start;
            known.complete_disk_image = complete_disk_image;
        } else {
            let reservation = assessment::ViewReservation::acquire(512)?;
            job.known.insert(
                source,
                KnownSource {
                    carrier,
                    embedded_start,
                    complete_disk_image,
                    promoted: BTreeMap::new(),
                    _reservation: reservation,
                },
            );
        }
        job.pending
            .insert(WorkKey::Source(source), QueuedInput { input, reservation });
        Ok(())
    }

    fn check_pending_capacity(&self, job_id: JobId, key: WorkKey) -> EngineResult<()> {
        let already_pending = self
            .jobs
            .get(&job_id)
            .is_some_and(|job| job.pending.contains_key(&key));
        if !already_pending
            && self
                .jobs
                .values()
                .map(|job| job.pending.len())
                .sum::<usize>()
                >= MAX_PENDING
        {
            return Err(EngineError::ResourceLimit("pending PAR3 work"));
        }
        Ok(())
    }

    /// Reclaim the least recently used recovery-waiting sessions before
    /// admitting another work unit. PAR2 budgets and policy are independent.
    fn evict_idle_sessions(&mut self, protected: JobId, headroom: usize) {
        let Some(memory) = self
            .jobs
            .get(&protected)
            .and_then(|job| job.runtime.as_ref())
            .map(|runtime| runtime.options.memory.clone())
        else {
            return;
        };
        while memory.available() < headroom {
            let victim = self
                .jobs
                .iter()
                .filter(|(id, job)| {
                    **id != protected
                        && job.ticket.is_none()
                        && !job.installing
                        && job.pending.is_empty()
                        && job.dirty.is_empty()
                        && job.errors.is_empty()
                        && job.completed_repair.is_none()
                        && job.completed_readback.is_none()
                        && job.runtime.as_ref().is_some_and(|runtime| {
                            !runtime.sets.is_empty()
                                && runtime.name_search.source().is_none()
                                && runtime.sets.values().all(|set| {
                                    set.view.as_ref().is_some_and(|view| {
                                        view.status == par3_rs::session::RepairStatus::NeedRecovery
                                    })
                                })
                        })
                })
                .min_by_key(|(id, job)| (job.last_used, **id))
                .map(|(&id, _)| id);
            let Some(victim) = victim else {
                break;
            };
            let before = memory.used();
            self.jobs
                .get_mut(&victim)
                .expect("idle victim")
                .runtime
                .as_mut()
                .expect("retained idle runtime")
                .evict_native_sessions();
            tracing::debug!(
                job_id = victim.0,
                released_bytes = before.saturating_sub(memory.used()),
                "evicted idle PAR3 sessions; current arrivals will reopen native analysis"
            );
        }
    }

    pub(super) fn dispatch(&mut self) -> EngineResult<()> {
        if !self.in_flight.is_empty() {
            return Ok(());
        }
        let ready = |job: &&JobSlot| {
            job.ticket.is_none()
                && if job.installing {
                    job.pending.contains_key(&WorkKey::Readback)
                } else {
                    !job.pending.is_empty()
                }
        };
        let next = self
            .jobs
            .iter()
            .filter(|(_, job)| ready(job))
            .find(|(id, _)| self.last_job.is_none_or(|last| **id > last))
            .or_else(|| self.jobs.iter().find(|(_, job)| ready(job)))
            .map(|(&id, _)| id);
        let Some(job_id) = next else {
            return Ok(());
        };
        self.evict_idle_sessions(job_id, 128 << 20);
        let job = self.jobs.get_mut(&job_id).expect("selected ready job");
        let ticket = self
            .next_ticket
            .checked_add(1)
            .ok_or(EngineError::ResourceLimit("PAR3 worker tickets"))?;
        let Some(mut runtime) = job.runtime.take() else {
            return Err(EngineError::InvalidState(
                "PAR3 session already owned by a worker",
            ));
        };
        if !runtime.dormant_views.is_empty() {
            job.verification = None;
        }
        self.next_ticket = ticket;
        self.last_job = Some(job_id);
        let (key, input) = if job.installing {
            (
                WorkKey::Readback,
                job.pending
                    .remove(&WorkKey::Readback)
                    .expect("pending readback"),
            )
        } else {
            job.pending.pop_first().expect("pending input")
        };
        let epoch = job.epoch;
        let assess =
            job.pending.is_empty() && job.dirty.iter().all(|id| key == WorkKey::Source(*id));
        job.ticket = Some(ticket);
        self.in_flight
            .insert(ticket, (job_id, runtime.options.cancel.clone()));
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                // Keep the queue lease live while the worker owns its input;
                // successful publication transfers it into retained state.
                if let PendingInput::Readback(mut installation) = input.input {
                    let result = installation.read_unit(&runtime.sources, &runtime.options);
                    return (
                        runtime,
                        Ok(WorkOutput::Readback(readback::ReadbackDone {
                            installation,
                            result,
                            _reservation: input.reservation,
                        })),
                    );
                }
                if let PendingInput::Repair { set, path } = input.input {
                    let layout = runtime
                        .sets
                        .get_mut(&set)
                        .ok_or(EngineError::InvalidState("missing PAR3 repair set"))
                        .and_then(|set| set.native.layout())
                        .and_then(|layout| {
                            layout.ok_or(EngineError::InvalidState("missing PAR3 repair layout"))
                        });
                    let layout = match layout {
                        Ok(layout) => layout,
                        Err(error) => {
                            return (
                                runtime,
                                Ok(WorkOutput::Repaired(RepairCompletion {
                                    result: Err(error),
                                    outputs: Ok(Vec::new()),
                                    embedded_replacement: false,
                                    _reservation: Some(input.reservation),
                                })),
                            );
                        }
                    };
                    let result = runtime.repair(set, &path);
                    let installed = match &result {
                        Ok(report) => report.installed.as_slice(),
                        Err(EngineError::RepairInterrupted { installed, .. }) => {
                            installed.as_slice()
                        }
                        _ => &[],
                    };
                    let embedded_replacement = !installed.is_empty()
                        && layout.files().iter().any(|file| {
                            file.extents.iter().any(|extent| {
                                matches!(extent.kind, par3_rs::layout::ExtentKind::Unprotected)
                            })
                        });
                    let outputs = installed
                        .iter()
                        .map(|output| {
                            let file = layout
                                .files()
                                .iter()
                                .find(|file| path.join(&file.path) == output.path)
                                .ok_or(EngineError::InvalidState(
                                    "PAR3 output has no authenticated length",
                                ))?;
                            readback::VerifiedOutput::capture(
                                output.path.clone(),
                                file.len,
                                &runtime.options,
                            )
                        })
                        .collect();
                    return (
                        runtime,
                        Ok(WorkOutput::Repaired(RepairCompletion {
                            result,
                            outputs,
                            embedded_replacement,
                            _reservation: Some(input.reservation),
                        })),
                    );
                }
                let WorkKey::Source(source) = key else {
                    return (
                        runtime,
                        Err(EngineError::InvalidState("invalid PAR3 work key")),
                    );
                };
                let before = runtime.sources.revision(source).ok().flatten();
                let result = match input.input {
                    PendingInput::Virtual { image, name } => {
                        runtime.publish_virtual(source, image, name)
                    }
                    PendingInput::Repair { .. } | PendingInput::Readback(_) => {
                        unreachable!("repair dispatched above")
                    }
                    PendingInput::CompleteFile { path, name } => std::fs::metadata(&path)
                        .map_err(EngineError::from)
                        .and_then(|metadata| {
                            let ranges = if metadata.len() == 0 {
                                Vec::new()
                            } else {
                                std::iter::once(0..metadata.len()).collect()
                            };
                            runtime.publish_file(source, path, name, ranges)
                        }),
                    PendingInput::Embedded {
                        path,
                        name,
                        ranges,
                        start,
                    } => runtime.scan_embedded(source, path, name, ranges, start),
                    PendingInput::Carrier { path, ranges } => {
                        runtime.scan_file(source, path, ranges)
                    }
                    PendingInput::File { path, name, ranges } => {
                        runtime.publish_file(source, path, name, ranges)
                    }
                };
                if let Ok(Some(after)) = runtime.sources.revision(source)
                    && Some(after) != before
                {
                    // Scanning can fail after publication succeeded. Keep its
                    // lease even on that exit; the ranges are still retained.
                    runtime.publication_memory.insert(source, input.reservation);
                }
                let result = result.and_then(|()| if assess { runtime.assess() } else { Ok(()) });
                (runtime, result.map(|()| WorkOutput::Published))
            })
            .await;
            let (runtime, result) = match result {
                Ok((runtime, result)) => (Some(runtime), result),
                Err(error) => (None, Err(std::io::Error::other(error).into())),
            };
            let _ = tx
                .send(RepairWorkDone::Par3(Box::new(WorkDone {
                    job_id,
                    ticket,
                    epoch,
                    key,
                    runtime,
                    result,
                })))
                .await;
        });
        Ok(())
    }

    #[cfg(test)]
    async fn recv(&mut self) -> Option<WorkDone> {
        match self
            .test_rx
            .as_mut()
            .expect("test coordinator receiver")
            .recv()
            .await?
        {
            RepairWorkDone::Par3(done) => Some(*done),
            RepairWorkDone::Par2(_) => panic!("PAR3 unit worker returned a PAR2 outcome"),
        }
    }

    /// Return the live job to recheck, or discard a forgotten/stale result.
    pub(super) fn settle(&mut self, done: WorkDone) -> Option<JobId> {
        let (job_id, _) = self.in_flight.get(&done.ticket)?;
        if *job_id != done.job_id {
            return None;
        }
        self.in_flight.remove(&done.ticket);
        let job = self.jobs.get_mut(&done.job_id)?;
        if job.ticket != Some(done.ticket) {
            return None;
        }
        job.ticket = None;
        job.last_used = done.ticket;
        let mut runtime = done.runtime.unwrap_or_default();
        if done.epoch != job.epoch {
            // A write raced this operation. Keep capacity until this handback,
            // retain unrelated evidence, and require fresh publication before
            // any returned scheduling view can be consumed.
            if let WorkKey::Source(source) = done.key {
                job.dirty.insert(source);
            }
            for &source in &job.dirty {
                for set in runtime.sets.values_mut() {
                    set.invalidate(source);
                }
                runtime.carriers.remove(&source);
            }
        } else if let WorkKey::Source(source) = done.key {
            job.dirty.remove(&source);
        }
        job.sources = runtime.sources.clone();
        job.runtime = Some(runtime);
        if done.epoch != job.epoch {
            if matches!(done.key, WorkKey::Repair(_)) {
                // Installation may have finished before the racing write. The
                // caller must not certify its outputs against the newer epoch.
                job.completed_repair = Some(RepairCompletion {
                    result: Err(EngineError::InvalidState(
                        "PAR3 repair output changed before handback",
                    )),
                    outputs: Ok(Vec::new()),
                    embedded_replacement: false,
                    _reservation: None,
                });
            } else if done.key == WorkKey::Readback {
                job.completed_readback = Some(Err(EngineError::InvalidState(
                    "PAR3 readback changed before handback",
                )));
            }
            return Some(done.job_id);
        }
        match (done.key, done.result) {
            (WorkKey::Readback, result) => {
                job.completed_readback = Some(result.and_then(|output| match output {
                    WorkOutput::Readback(done) => Ok(done),
                    _ => Err(EngineError::InvalidState("missing PAR3 readback result")),
                }));
            }
            (WorkKey::Repair(_), result) => {
                job.completed_repair = Some(match result {
                    Ok(WorkOutput::Repaired(completion)) => completion,
                    other => RepairCompletion {
                        result: Err(other
                            .err()
                            .unwrap_or(EngineError::InvalidState("missing PAR3 repair report"))),
                        outputs: Ok(Vec::new()),
                        embedded_replacement: false,
                        _reservation: None,
                    },
                });
            }
            (WorkKey::Source(source), Ok(_)) => {
                job.errors.remove(&source);
            }
            (WorkKey::Source(source), Err(error)) => {
                tracing::warn!(job_id = done.job_id.0, source = source.0, error = %error, "PAR3 carrier discovery incomplete");
                job.errors.insert(source, error);
            }
        }
        Some(done.job_id)
    }

    pub(in crate::pipeline) fn forget(&mut self, job_id: JobId) {
        self.jobs.remove(&job_id);
        for (owner, token) in self.in_flight.values() {
            if *owner == job_id {
                token.cancel();
            }
        }
    }
}

impl Drop for Coordinator {
    fn drop(&mut self) {
        for (_, token) in self.in_flight.values() {
            token.cancel();
        }
    }
}

mod verification;

#[cfg(test)]
mod tests;
