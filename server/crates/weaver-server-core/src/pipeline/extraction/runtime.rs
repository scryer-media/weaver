pub(crate) mod binding;
pub(crate) mod checkpoint;
pub(crate) mod decoded;
pub(crate) mod publication;
pub(crate) mod repair;
pub(crate) mod replacement;
pub(crate) mod root;
mod selected;

use super::output::channel::StreamOwner;
use super::session::{ArchiveSessions, InputVersion, SessionError, SessionLease};
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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

struct Request {
    owner: StreamOwner,
    id: RequestId,
    work: NamespaceWork,
}

// The job owner admits work and consumes completions. Workers own explicit
// immutable input views and private output roots; they never resolve NZB IDs.
pub(crate) struct ArchiveRuntime {
    identity: Arc<()>,
    pub(crate) sessions: ArchiveSessions,
    pub(crate) root: u64,
    next_request: u64,
    requests: BTreeMap<u64, Request>,
    speculative_requests: HashSet<u64>,
    speculative_namespaces: HashSet<String>,
    pub(crate) consumed: HashSet<String>,
    pub(crate) output_owners: BTreeMap<String, StreamOwner>,
    pub(crate) retained_work: Vec<(StreamOwner, NamespaceWork)>,
    pub(crate) terminal_cleanup:
        Option<crate::pipeline::completion::finalize::TerminalArchiveCleanup>,
    pub(crate) completed: Vec<(StreamOwner, NamespaceWork, Vec<String>)>,
    pub(crate) decoded: Vec<decoded::DecodedState>,
    workers: Vec<(StreamOwner, tokio::task::JoinHandle<()>)>,
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

impl ArchiveRuntime {
    pub(crate) fn new() -> Self {
        let root = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
        assert_ne!(root, u64::MAX, "archive runtime identity exhausted");
        Self {
            identity: Arc::new(()),
            sessions: ArchiveSessions::new(root, 0),
            root,
            next_request: 0,
            requests: BTreeMap::new(),
            speculative_requests: HashSet::new(),
            speculative_namespaces: HashSet::new(),
            consumed: HashSet::new(),
            output_owners: BTreeMap::new(),
            retained_work: Vec::new(),
            terminal_cleanup: None,
            completed: Vec::new(),
            decoded: Vec::new(),
            workers: Vec::new(),
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
        let sequence = self
            .next_request
            .checked_add(1)
            .ok_or(SessionError::Admission)?;
        self.next_request = sequence;
        let id = RequestId {
            identity: Arc::clone(&self.identity),
            sequence,
        };
        self.requests.insert(
            sequence,
            Request {
                owner,
                id: id.clone(),
                work,
            },
        );
        Ok(id)
    }

    // Called only after the worker has joined. The opaque identity fences a
    // late completion even after the same JobId has acquired a new runtime.
    pub(crate) fn complete(&mut self, owner: StreamOwner, id: RequestId) -> Option<NamespaceWork> {
        if !Arc::ptr_eq(&self.identity, &id.identity) {
            return None;
        }
        let request = self.requests.get(&id.sequence)?;
        if request.owner != owner || !Arc::ptr_eq(&request.id.identity, &id.identity) {
            return None;
        }
        let request = self.requests.remove(&id.sequence)?;
        if !self.sessions.execution_current(owner) {
            let _ = self.sessions.joined(owner);
            return None;
        }
        Some(request.work)
    }

    #[cfg(test)]
    pub(crate) fn request_set_name(&self, request: &RequestId) -> Option<&str> {
        if !Arc::ptr_eq(&self.identity, &request.identity) {
            return None;
        }
        self.requests
            .get(&request.sequence)
            .map(|request| request.work.set_name.as_str())
    }

    pub(crate) fn track_worker(&mut self, owner: StreamOwner, worker: tokio::task::JoinHandle<()>) {
        self.workers.push((owner, worker));
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
        for (_, input) in &self.live_inputs {
            input.abort("archive runtime failed");
        }
        self.sinks.clear();
        let _ = self.sessions.revoke(owner);
        for &(sibling, _) in &self.workers {
            let _ = self.sessions.revoke(sibling);
        }
        for state in &self.decoded {
            state.generation.revoke();
        }
        self.completed.clear();
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
}

impl Drop for ArchiveRuntime {
    fn drop(&mut self) {
        for (_, input) in &self.live_inputs {
            input.abort("archive runtime removed");
        }
        self.sinks.clear();
        for (owner, _) in &self.workers {
            let _ = self.sessions.revoke(*owner);
        }
        for state in &self.decoded {
            state.generation.revoke();
        }
        // Removal fences requests immediately, while this drainer retains each
        // worker until it has stopped touching its private backing namespace.
        let workers = std::mem::take(&mut self.workers);
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                for (_, worker) in workers {
                    let _ = worker.await;
                }
            });
        }
    }
}

#[cfg(test)]
mod tests;
