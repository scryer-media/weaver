pub(crate) mod binding;
pub(crate) mod checkpoint;
pub(crate) mod decoded;
pub(crate) mod error;
pub(crate) mod limits;
mod plan;
pub(crate) mod publication;
pub(crate) mod rearm;
pub(crate) mod repair;
pub(crate) mod replacement;
mod requests;
pub(crate) mod root;
mod selected;
mod workers;

pub(crate) use error::RuntimeError;
use requests::RequestTable;
use workers::WorkerSet;

use super::output::channel::StreamOwner;
use super::session::{ArchiveSessions, InputVersion, SessionError, SessionLease};
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// One root per job runtime. A u64 taken once per runtime does not wrap within
// any process's lifetime, so two live runtimes never share a root.
static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub(crate) struct RequestId {
    identity: Arc<()>,
    sequence: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct VirtualInputSource {
    pub(crate) root_set: String,
    pub(crate) node: u32,
}

#[derive(Clone)]
pub(crate) struct NamespaceWork {
    pub(crate) kind: crate::jobs::assembly::ArchiveType,
    pub(crate) set_name: String,
    pub(crate) volumes: Vec<u32>,
    pub(crate) output: PathBuf,
    pub(crate) inputs: Vec<PathBuf>,
    pub(crate) original_inputs: Vec<PathBuf>,
    pub(crate) input_names: Vec<String>,
    pub(crate) virtual_source: Option<VirtualInputSource>,
    pub(crate) original_virtual_source: Option<VirtualInputSource>,
    pub(crate) destination: PathBuf,
    pub(crate) outputs: BTreeMap<String, PathBuf>,
    pub(crate) root_set: Option<String>,
    pub(crate) ordinary: Vec<String>,
    pub(crate) selected_password: Option<String>,
}

// The job owner admits work and consumes completions. Workers own explicit
// immutable input views and private output roots; they never resolve NZB IDs.
pub(crate) struct ArchiveRuntime {
    pub(crate) sessions: ArchiveSessions,
    pub(crate) root: u64,
    requests: RequestTable,
    speculative_namespaces: HashSet<String>,
    pub(crate) consumed: HashSet<String>,
    pub(crate) output_owners: BTreeMap<String, StreamOwner>,
    pub(crate) retained_work: Vec<(StreamOwner, NamespaceWork)>,
    pub(crate) terminal_cleanup:
        Option<crate::pipeline::completion::finalize::TerminalArchiveCleanup>,
    pub(crate) completed: Vec<(StreamOwner, NamespaceWork, Vec<String>)>,
    pub(crate) decoded: Vec<decoded::DecodedState>,
    workers: WorkerSet,
    pub(crate) replay: Vec<(StreamOwner, NamespaceWork)>,
    pub(crate) deferred_repairs: Vec<(StreamOwner, NamespaceWork)>,
    pub(crate) graph_owners: BTreeMap<(String, u32), StreamOwner>,
    sinks: BTreeMap<String, root::PersistentSink>,
    pub(crate) gates: BTreeMap<(String, u32), super::input::LiveArchiveInput>,
    pub(crate) live_inputs: Vec<(StreamOwner, super::input::LiveArchiveInput)>,
    checkpoint_roots: Option<(PathBuf, PathBuf)>,
    staged_replacement: Option<replacement::StagedReplacement>,
    failed: bool,
}

// How owners leave the runtime, which decides what their graph slots and
// consumed outputs become.
#[derive(Clone, Copy)]
enum Departure {
    // Their whole subtree is gone: inputs abort, channels close, slots empty.
    Revoked,
    // A committed repair supersedes them: a node keeps its graph slot under
    // the selected generation, and the outputs they consumed may be claimed again.
    Replaced(StreamOwner),
}

impl ArchiveRuntime {
    pub(crate) fn new() -> Self {
        let root = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
        Self {
            sessions: ArchiveSessions::new(root, 0),
            root,
            requests: RequestTable::new(),
            speculative_namespaces: HashSet::new(),
            consumed: HashSet::new(),
            output_owners: BTreeMap::new(),
            retained_work: Vec::new(),
            terminal_cleanup: None,
            completed: Vec::new(),
            decoded: Vec::new(),
            workers: WorkerSet::default(),
            replay: Vec::new(),
            deferred_repairs: Vec::new(),
            graph_owners: BTreeMap::new(),
            sinks: BTreeMap::new(),
            gates: BTreeMap::new(),
            live_inputs: Vec::new(),
            checkpoint_roots: None,
            staged_replacement: None,
            failed: false,
        }
    }

    pub(crate) fn restore_checkpoint(
        saved: &super::session::checkpoint::Checkpoint,
    ) -> Result<Self, SessionError> {
        let mut runtime = Self::new();
        runtime.sessions = ArchiveSessions::restore_checkpoint(saved, runtime.root)?;
        Ok(runtime)
    }

    pub(crate) fn admit(
        &mut self,
        containing: Option<StreamOwner>,
        original: InputVersion,
    ) -> Result<SessionLease, SessionError> {
        let node = self.sessions.register(containing, original)?;
        self.sessions.start(node)
    }

    pub(crate) fn dispatch(
        &mut self,
        owner: StreamOwner,
        work: NamespaceWork,
    ) -> Result<RequestId, SessionError> {
        if !self.sessions.accepts(owner) || self.failed {
            return Err(SessionError::Stale);
        }
        self.requests.issue(owner, work)
    }

    // Called only after the worker has joined. The opaque identity fences a
    // late completion even after the same JobId has acquired a new runtime.
    pub(crate) fn complete(&mut self, owner: StreamOwner, id: RequestId) -> Option<NamespaceWork> {
        if self.requests.get(&id)?.owner != owner {
            return None;
        }
        let request = self.requests.remove(&id)?;
        if !self.sessions.execution_current(owner) {
            let _ = self.sessions.joined(owner);
            return None;
        }
        Some(request.work)
    }

    #[cfg(test)]
    pub(crate) fn request_set_name(&self, request: &RequestId) -> Option<&str> {
        self.requests
            .get(request)
            .map(|request| request.work.set_name.as_str())
    }

    pub(crate) fn track_worker(&mut self, owner: StreamOwner, worker: tokio::task::JoinHandle<()>) {
        self.workers.track(owner, worker);
    }

    pub(crate) fn pending(&self) -> bool {
        !self.requests.is_empty()
            || !self.deferred_repairs.is_empty()
            || !self.completed.is_empty()
            || !self.replay.is_empty()
            || self.staged_replacement.is_some()
    }

    pub(crate) fn fail(&mut self, owner: StreamOwner) {
        self.failed = true;
        self.stop_all("archive runtime failed");
        let _ = self.sessions.revoke(owner);
        self.completed.clear();
    }

    // Revokes every running worker and producer. Revocation only fences
    // authority; each worker keeps its private backings until it observes it.
    fn stop_all(&mut self, reason: &'static str) {
        for input in self
            .live_inputs
            .iter()
            .map(|(_, input)| input)
            .chain(self.gates.values())
        {
            input.abort(reason);
        }
        self.sinks.clear();
        let owners: Vec<StreamOwner> = self.workers.owners().collect();
        for owner in owners {
            let _ = self.sessions.revoke(owner);
        }
        for state in &self.decoded {
            state.generation.revoke();
        }
    }

    pub(crate) fn publication_ready(&self) -> bool {
        !self.failed
            && self.deferred_repairs.is_empty()
            && self.requests.is_empty()
            && self.replay.is_empty()
            && self.staged_replacement.is_none()
            && self
                .completed
                .iter()
                .all(|(owner, _, _)| self.sessions.ancestry_verified(*owner))
    }

    // Every per-owner field is named here, so a new one must say what an
    // owner's departure does to it.
    fn forget_owners(&mut self, gone: &[StreamOwner], departure: Departure) {
        if let Departure::Revoked = departure {
            self.abort_inputs(gone, "archive input owner revoked");
            self.retire_sinks(gone);
        }
        let Self {
            sessions: _,
            root: _,
            requests,
            speculative_namespaces,
            consumed,
            output_owners,
            retained_work,
            terminal_cleanup: _,
            completed,
            decoded,
            // Joining is the caller's choice: some wait, some leave a barrier.
            workers: _,
            replay,
            deferred_repairs,
            graph_owners,
            // Only revocation closes channels, and it has done so above.
            sinks: _,
            gates,
            live_inputs,
            checkpoint_roots: _,
            staged_replacement,
            failed: _,
        } = self;
        let kept = |owner: &StreamOwner| !gone.contains(owner);
        completed.retain(|(owner, _, _)| kept(owner));
        retained_work.retain(|(owner, _)| kept(owner));
        deferred_repairs.retain(|(owner, _)| kept(owner));
        replay.retain(|(owner, _)| kept(owner));
        decoded.retain(|state| kept(&state.generation.owner()));
        match departure {
            Departure::Revoked => {
                requests.forget(gone);
                output_owners.retain(|_, owner| kept(owner));
                graph_owners.retain(|_, owner| kept(owner));
                speculative_namespaces
                    .retain(|namespace| graph_owners.keys().any(|(bound, _)| bound == namespace));
                gates.retain(|key, _| graph_owners.contains_key(key));
                if staged_replacement.as_ref().is_some_and(|staged| {
                    !kept(&staged.selected) || staged.retiring.iter().any(|owner| !kept(owner))
                }) {
                    staged_replacement.take();
                }
            }
            Departure::Replaced(selected) => {
                output_owners.retain(|name, owner| {
                    let keep = kept(owner);
                    if !keep {
                        consumed.remove(name);
                    }
                    keep
                });
                graph_owners.retain(|_, owner| {
                    if kept(owner) {
                        return true;
                    }
                    if owner.node == selected.node {
                        *owner = selected;
                        return true;
                    }
                    false
                });
            }
        }
        live_inputs.retain(|(owner, _)| kept(owner));
    }
}

impl Drop for ArchiveRuntime {
    fn drop(&mut self) {
        self.stop_all("archive runtime removed");
        // Removal fences requests immediately, while this drainer retains each
        // worker until it has stopped touching its private backing namespace.
        // Without a Tokio runtime there is nothing to drain on, and dropping a
        // JoinHandle detaches its task rather than cancelling it, so each
        // worker still runs to its own cancellation checkpoint.
        let workers = self.workers.take_all();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                for worker in workers {
                    let _ = worker.await;
                }
            });
        }
    }
}

#[cfg(test)]
mod tests;
