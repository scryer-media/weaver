//! Retained ownership and bounded dispatch for PAR3 blocking work.

use super::*;
use crate::pipeline::RepairWorkDone;
use par3_rs::runtime::CancellationToken;
use tokio::sync::mpsc;

const MAX_JOBS: usize = 256;
const MAX_PENDING: usize = 4096;

enum PendingInput {
    Carrier(PathBuf),
    File {
        path: PathBuf,
        name: String,
        ranges: Vec<std::ops::Range<u64>>,
    },
}

impl PendingInput {
    fn retained_cost(&self) -> EngineResult<usize> {
        let (path, extra) = match self {
            Self::Carrier(path) => (path, Some(0)),
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

struct JobSlot {
    runtime: Option<Par3Job>,
    pending: BTreeMap<SourceId, QueuedInput>,
    ticket: Option<u64>,
    errors: BTreeMap<SourceId, EngineError>,
}

impl Default for JobSlot {
    fn default() -> Self {
        Self {
            runtime: Some(Par3Job::default()),
            pending: BTreeMap::new(),
            ticket: None,
            errors: BTreeMap::new(),
        }
    }
}

pub(crate) struct WorkDone {
    job_id: JobId,
    ticket: u64,
    source: SourceId,
    runtime: Option<Par3Job>,
    result: EngineResult<()>,
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
            .map_or(0, |runtime| runtime.sets.len())
    }

    pub(in crate::pipeline) fn has_work(&self, job_id: JobId) -> bool {
        self.jobs
            .get(&job_id)
            .is_some_and(|job| job.ticket.is_some() || !job.pending.is_empty())
    }

    pub(super) fn enqueue(
        &mut self,
        job_id: JobId,
        source: SourceId,
        path: PathBuf,
    ) -> EngineResult<()> {
        self.enqueue_input(job_id, source, PendingInput::Carrier(path))
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

    pub(in crate::pipeline) fn assessments(
        &self,
        job_id: JobId,
    ) -> impl Iterator<Item = (par3_rs::InputSetId, &assessment::AssessmentView)> {
        self.jobs
            .get(&job_id)
            .filter(|job| job.ticket.is_none() && job.pending.is_empty() && job.errors.is_empty())
            .and_then(|job| job.runtime.as_ref())
            .into_iter()
            .flat_map(|runtime| runtime.sets.iter())
            .filter_map(|(&id, set)| set.view.as_ref().map(|view| (id, view)))
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
        let already_pending = self
            .jobs
            .get(&job_id)
            .is_some_and(|job| job.pending.contains_key(&source));
        if !already_pending
            && self
                .jobs
                .values()
                .map(|job| job.pending.len())
                .sum::<usize>()
                >= MAX_PENDING
        {
            return Err(EngineError::ResourceLimit("pending PAR3 carriers"));
        }
        let reservation = assessment::ViewReservation::acquire(input.retained_cost()?)?;
        self.jobs
            .entry(job_id)
            .or_default()
            .pending
            .insert(source, QueuedInput { input, reservation });
        Ok(())
    }

    pub(super) fn dispatch(&mut self) -> EngineResult<()> {
        if !self.in_flight.is_empty() {
            return Ok(());
        }
        let ready = |job: &&JobSlot| job.ticket.is_none() && !job.pending.is_empty();
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
        self.next_ticket = ticket;
        self.last_job = Some(job_id);
        let (source, input) = job.pending.pop_first().expect("pending input");
        job.ticket = Some(ticket);
        self.in_flight
            .insert(ticket, (job_id, runtime.options.cancel.clone()));
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                // Keep the queue lease live while the worker owns its input;
                // successful publication transfers it into retained state.
                let result = match input.input {
                    PendingInput::Carrier(path) => runtime.scan_complete(source, path),
                    PendingInput::File { path, name, ranges } => {
                        runtime.publish_file(source, path, name, ranges)
                    }
                };
                if result.is_ok() {
                    runtime.publication_memory.insert(source, input.reservation);
                }
                let result = result.and_then(|()| runtime.assess());
                (runtime, result)
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
                    source,
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
        job.runtime = Some(done.runtime.unwrap_or_default());
        match done.result {
            Ok(()) => {
                job.errors.remove(&done.source);
            }
            Err(error) => {
                tracing::warn!(job_id = done.job_id.0, source = done.source.0, error = %error, "PAR3 carrier discovery incomplete");
                job.errors.insert(done.source, error);
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

#[cfg(test)]
mod tests;
