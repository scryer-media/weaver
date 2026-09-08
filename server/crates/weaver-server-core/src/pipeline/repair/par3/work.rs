//! Retained ownership and bounded dispatch for PAR3 blocking work.

use super::*;
use par3_rs::runtime::CancellationToken;
use tokio::sync::mpsc;

const MAX_JOBS: usize = 256;
const MAX_PENDING: usize = 4096;

struct JobSlot {
    runtime: Option<Par3Job>,
    pending: BTreeMap<SourceId, PathBuf>,
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

pub(in crate::pipeline) struct WorkDone {
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
    tx: mpsc::Sender<WorkDone>,
    rx: mpsc::Receiver<WorkDone>,
}

impl Default for Coordinator {
    fn default() -> Self {
        let (tx, rx) = mpsc::channel(1);
        Self {
            jobs: BTreeMap::new(),
            in_flight: BTreeMap::new(),
            next_ticket: 0,
            tx,
            rx,
        }
    }
}

impl Coordinator {
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
        self.jobs
            .entry(job_id)
            .or_default()
            .pending
            .insert(source, path);
        Ok(())
    }

    pub(super) fn dispatch(&mut self) -> EngineResult<()> {
        if !self.in_flight.is_empty() {
            return Ok(());
        }
        let Some((&job_id, job)) = self
            .jobs
            .iter_mut()
            .find(|(_, job)| job.ticket.is_none() && !job.pending.is_empty())
        else {
            return Ok(());
        };
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
        let (source, path) = job.pending.pop_first().expect("pending carrier");
        job.ticket = Some(ticket);
        self.in_flight
            .insert(ticket, (job_id, runtime.options.cancel.clone()));
        let tx = self.tx.clone();
        tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                let result = runtime.scan_complete(source, path);
                (runtime, result)
            })
            .await;
            let (runtime, result) = match result {
                Ok((runtime, result)) => (Some(runtime), result),
                Err(error) => (None, Err(std::io::Error::other(error).into())),
            };
            let _ = tx
                .send(WorkDone {
                    job_id,
                    ticket,
                    source,
                    runtime,
                    result,
                })
                .await;
        });
        Ok(())
    }

    pub(in crate::pipeline) async fn recv(&mut self) -> Option<WorkDone> {
        self.rx.recv().await
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
