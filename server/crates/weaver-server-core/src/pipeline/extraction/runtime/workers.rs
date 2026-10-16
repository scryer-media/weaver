//! Spawned namespace workers, each held under the owner whose backings it reads.
use crate::pipeline::extraction::output::channel::StreamOwner;
use tokio::task::JoinHandle;

#[derive(Default)]
pub(super) struct WorkerSet {
    workers: Vec<(StreamOwner, JoinHandle<()>)>,
}

impl WorkerSet {
    pub(super) fn track(&mut self, owner: StreamOwner, worker: JoinHandle<()>) {
        self.workers.push((owner, worker));
    }

    pub(super) fn owners(&self) -> impl Iterator<Item = StreamOwner> + '_ {
        self.workers.iter().map(|&(owner, _)| owner)
    }

    // Order is kept, so joining the taken workers follows spawn order.
    pub(super) fn take(&mut self, owners: &[StreamOwner]) -> Vec<JoinHandle<()>> {
        let (taken, kept) = std::mem::take(&mut self.workers)
            .into_iter()
            .partition::<Vec<_>, _>(|(owner, _)| owners.contains(owner));
        self.workers = kept;
        taken.into_iter().map(|(_, worker)| worker).collect()
    }

    pub(super) fn take_all(&mut self) -> Vec<JoinHandle<()>> {
        std::mem::take(&mut self.workers)
            .into_iter()
            .map(|(_, worker)| worker)
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.workers.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.workers.is_empty()
    }
}
