use super::output::channel::{Generation, StreamOwner};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::watch;

pub(crate) mod checkpoint;

const MAX_NODES: usize = 4096;
const MAX_SESSIONS: usize = 8192;
const MAX_DEPTH: u32 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct InputVersion(pub(crate) u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SessionError {
    Stale,
    Busy,
    Admission,
    InvalidTransition,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Running,
    Closed,
    Draining,
    Drained,
    Retired,
}

struct Node {
    containing: Option<StreamOwner>,
    depth: u32,
    original: InputVersion,
    effective: InputVersion,
    next_generation: u64,
    active: Option<StreamOwner>,
}

struct Session {
    containing: Option<StreamOwner>,
    phase: Phase,
    cancellation: Arc<AtomicBool>,
    cancellation_wake: watch::Sender<bool>,
    backings: Arc<()>,
    channel: Option<Generation>,
}

// Workers and frozen input views retain this generation's backing lease.
// Cancellation is independent of retirement: revoked bytes remain readable
// until writers join, a replacement checkpoint commits, and readers release.
#[derive(Clone)]
pub(crate) struct SessionLease {
    pub(crate) owner: StreamOwner,
    pub(crate) cancellation: Arc<AtomicBool>,
    cancellation_wake: watch::Receiver<bool>,
    pub(crate) backings: Arc<()>,
}

impl SessionLease {
    // Select this alongside input availability. The retained watch value makes
    // cancellation observable even when revocation precedes the first poll.
    pub(crate) async fn cancelled(&self) {
        let mut wake = self.cancellation_wake.clone();
        let _ = wake.wait_for(|cancelled| *cancelled).await;
    }

    pub(crate) async fn send_event<T>(
        &self,
        sender: &tokio::sync::mpsc::Sender<T>,
        event: T,
    ) -> bool {
        // The coordinator may be joining this producer while its queue is full.
        // Retirement must release that wait without delivering stale authority.
        tokio::select! {
            biased;
            _ = self.cancelled() => false,
            result = sender.send(event) => result.is_ok(),
        }
    }
}

pub(crate) struct Replacement {
    root: u64,
    node: u32,
    owner: StreamOwner,
    revision: u64,
    effective: InputVersion,
    retiring: Vec<StreamOwner>,
}

// Engine-neutral lifecycle state. Engines consume admitted input versions and
// emit under a session owner; they neither create nodes nor publish outputs.
// All methods run on the job owner. Blocking joins, I/O and SQL remain workers.
pub(crate) struct ArchiveSessions {
    root: u64,
    nodes: BTreeMap<u32, Node>,
    sessions: BTreeMap<(u32, u64), Session>,
    revision: u64,
    checkpoint: u64,
}

impl ArchiveSessions {
    pub(crate) fn new(root: u64, checkpoint: u64) -> Self {
        Self {
            root,
            nodes: BTreeMap::new(),
            sessions: BTreeMap::new(),
            revision: 0,
            checkpoint,
        }
    }

    pub(crate) fn depth(&self, owner: StreamOwner) -> Option<u32> {
        self.session(owner).ok()?;
        self.nodes.get(&owner.node).map(|node| node.depth)
    }

    fn session(&self, owner: StreamOwner) -> Result<&Session, SessionError> {
        if owner.root != self.root {
            return Err(SessionError::Stale);
        }
        self.sessions
            .get(&(owner.node, owner.generation))
            .ok_or(SessionError::Stale)
    }

    fn session_mut(&mut self, owner: StreamOwner) -> Result<&mut Session, SessionError> {
        if owner.root != self.root {
            return Err(SessionError::Stale);
        }
        self.sessions
            .get_mut(&(owner.node, owner.generation))
            .ok_or(SessionError::Stale)
    }

    pub(crate) fn lease(&self, owner: StreamOwner) -> Result<SessionLease, SessionError> {
        if !self.accepts(owner) {
            return Err(SessionError::Stale);
        }
        let session = self.session(owner)?;
        Ok(SessionLease {
            owner,
            cancellation: Arc::clone(&session.cancellation),
            cancellation_wake: session.cancellation_wake.subscribe(),
            backings: Arc::clone(&session.backings),
        })
    }

    pub(crate) fn is_retiring(&self, owner: StreamOwner) -> bool {
        self.session(owner).is_ok_and(|session| {
            matches!(
                session.phase,
                Phase::Draining | Phase::Drained | Phase::Retired
            ) && session.cancellation.load(Ordering::Acquire)
        })
    }

    pub(crate) fn execution_current(&self, owner: StreamOwner) -> bool {
        let Ok(session) = self.session(owner) else {
            return false;
        };
        self.nodes
            .get(&owner.node)
            .is_some_and(|node| node.active == Some(owner))
            && matches!(session.phase, Phase::Running | Phase::Closed)
            && !session.cancellation.load(Ordering::Acquire)
            && session.containing.is_none_or(|parent| self.accepts(parent))
    }

    pub(crate) fn accepts(&self, mut owner: StreamOwner) -> bool {
        loop {
            let Ok(session) = self.session(owner) else {
                return false;
            };
            if !matches!(session.phase, Phase::Running | Phase::Closed)
                || session.cancellation.load(Ordering::Acquire)
                || session.channel.as_ref().is_some_and(Generation::is_revoked)
            {
                return false;
            }
            let Some(parent) = session.containing else {
                return true;
            };
            owner = parent;
        }
    }

    // Only discovery consumes another depth/node admission. Replacing the
    // execution engine below keeps this node and its original input version.
    pub(crate) fn register(
        &mut self,
        containing: Option<StreamOwner>,
        original: InputVersion,
    ) -> Result<u32, SessionError> {
        let depth = match containing {
            Some(owner) if self.accepts(owner) => self.nodes[&owner.node].depth + 1,
            Some(_) => return Err(SessionError::Stale),
            None => 1,
        };
        if self.nodes.len() >= MAX_NODES || depth > MAX_DEPTH {
            return Err(SessionError::Admission);
        }
        let id = self.nodes.len() as u32;
        self.nodes.insert(
            id,
            Node {
                containing,
                depth,
                original,
                effective: original,
                next_generation: 1,
                active: None,
            },
        );
        self.revision += 1;
        Ok(id)
    }

    pub(crate) fn restart_restored(
        &mut self,
        node: u32,
        containing: Option<StreamOwner>,
    ) -> Result<SessionLease, SessionError> {
        let state = self.nodes.get(&node).ok_or(SessionError::Stale)?;
        if state.active.is_some() {
            return Err(SessionError::Busy);
        }
        if state.containing.map(|owner| owner.node) != containing.map(|owner| owner.node)
            || containing.is_some_and(|owner| !self.accepts(owner))
        {
            return Err(SessionError::Stale);
        }
        let previous = state.containing;
        self.nodes
            .get_mut(&node)
            .expect("validated node")
            .containing = containing;
        match self.start(node) {
            Ok(lease) => Ok(lease),
            Err(error) => {
                self.nodes
                    .get_mut(&node)
                    .expect("validated node")
                    .containing = previous;
                Err(error)
            }
        }
    }

    pub(crate) fn start(&mut self, node: u32) -> Result<SessionLease, SessionError> {
        let state = self.nodes.get(&node).ok_or(SessionError::Stale)?;
        if state.active.is_some() {
            return Err(SessionError::Busy);
        }
        if state.containing.is_some_and(|owner| !self.accepts(owner)) {
            return Err(SessionError::Stale);
        }
        if self.sessions.len() >= MAX_SESSIONS {
            return Err(SessionError::Admission);
        }
        let generation = state.next_generation;
        let next = generation.checked_add(1).ok_or(SessionError::Admission)?;
        let containing = state.containing;
        let owner = StreamOwner {
            root: self.root,
            node,
            generation,
        };
        let (cancellation_wake, receiver) = watch::channel(false);
        let lease = SessionLease {
            owner,
            cancellation: Arc::new(AtomicBool::new(false)),
            cancellation_wake: receiver,
            backings: Arc::new(()),
        };
        self.sessions.insert(
            (node, generation),
            Session {
                containing,
                phase: Phase::Running,
                cancellation: Arc::clone(&lease.cancellation),
                cancellation_wake,
                backings: Arc::clone(&lease.backings),
                channel: None,
            },
        );
        let state = self.nodes.get_mut(&node).expect("validated node");
        state.next_generation = next;
        state.active = Some(owner);
        self.revision += 1;
        Ok(lease)
    }

    pub(crate) fn attach_channel(&mut self, channel: Generation) -> Result<(), SessionError> {
        let owner = channel.owner();
        if !self.accepts(owner) {
            channel.revoke();
            return Err(SessionError::Stale);
        }
        let session = self.session_mut(owner)?;
        if session.phase != Phase::Running || session.channel.is_some() {
            channel.revoke();
            return Err(SessionError::InvalidTransition);
        }
        session.channel = Some(channel);
        Ok(())
    }

    pub(crate) fn close(&mut self, owner: StreamOwner) -> Result<(), SessionError> {
        if !self.accepts(owner) {
            return Err(SessionError::Stale);
        }
        let session = self.session_mut(owner)?;
        // Progress snapshots repeat a producer's local integrity proof while
        // its ancestors can still be open. Reaffirming that proof is harmless.
        if session.phase == Phase::Closed {
            return Ok(());
        }
        if session.phase != Phase::Running {
            return Err(SessionError::InvalidTransition);
        }
        session.phase = Phase::Closed;
        self.revision += 1;
        Ok(())
    }

    fn descendants(&self, ancestor: StreamOwner) -> Vec<StreamOwner> {
        self.sessions
            .keys()
            .filter_map(|&(node, generation)| {
                let owner = StreamOwner {
                    root: self.root,
                    node,
                    generation,
                };
                let mut cursor = Some(owner);
                while let Some(current) = cursor {
                    if current == ancestor {
                        return Some(owner);
                    }
                    cursor = self.session(current).ok()?.containing;
                }
                None
            })
            .collect()
    }

    // Returns the exact generations whose workers/placements must be joined.
    // This is also the failure path for trailing archive integrity errors.
    // All generations are signalled before returning. Observe their joins
    // asynchronously; the returned order is not a blocking join order.
    pub(crate) fn revoke(&mut self, owner: StreamOwner) -> Result<Vec<StreamOwner>, SessionError> {
        self.session(owner)?;
        let descendants = self.descendants(owner);
        let mut draining = Vec::new();
        let mut changed = false;
        for child in descendants {
            let session = self.session_mut(child)?;
            if matches!(session.phase, Phase::Running | Phase::Closed) {
                session.cancellation.store(true, Ordering::Release);
                session.cancellation_wake.send_replace(true);
                if let Some(channel) = &session.channel {
                    channel.revoke();
                }
                session.phase = Phase::Draining;
                draining.push(child);
                changed = true;
            } else if session.phase == Phase::Draining {
                // A prior synchronous fence may already have cancelled this
                // descendant. It still belongs to the caller's join barrier.
                draining.push(child);
            }
        }
        if changed {
            self.revision += 1;
        }
        Ok(draining)
    }

    pub(crate) fn joined(&mut self, owner: StreamOwner) -> Result<(), SessionError> {
        let session = self.session_mut(owner)?;
        if session.phase != Phase::Draining {
            return Err(SessionError::InvalidTransition);
        }
        session.phase = Phase::Drained;
        // The revoked control is no longer needed by the coordinator. Frames
        // retain their own admission independently of this lightweight handle.
        session.channel.take();
        self.revision += 1;
        Ok(())
    }

    pub(crate) fn prepare_replacement(
        &self,
        node: u32,
        effective: InputVersion,
    ) -> Result<Replacement, SessionError> {
        if self
            .nodes
            .get(&node)
            .is_some_and(|node| node.containing.is_some_and(|parent| !self.accepts(parent)))
        {
            return Err(SessionError::Stale);
        }
        let owner = self
            .nodes
            .get(&node)
            .and_then(|node| node.active)
            .ok_or(SessionError::Stale)?;
        let retiring = self.descendants(owner);
        if retiring.iter().any(|owner| {
            !self
                .session(*owner)
                .is_ok_and(|session| matches!(session.phase, Phase::Drained | Phase::Retired))
        }) {
            return Err(SessionError::Busy);
        }
        Ok(Replacement {
            root: self.root,
            node,
            owner,
            revision: self.revision,
            effective,
            retiring,
        })
    }

    // Called only after the job's graph checkpoint committed the replacement
    // backing and selected generations. An ambiguous SQL result must be read
    // back before this transition; a prepared filesystem manifest is not proof.
    pub(crate) fn committed_replacement(
        &mut self,
        replacement: Replacement,
        checkpoint: u64,
    ) -> Result<(), SessionError> {
        if replacement.root != self.root || self.checkpoint.checked_add(1) != Some(checkpoint) {
            return Err(SessionError::Stale);
        }
        // SQL has advanced even if a failure invalidated live work while the
        // commit was in flight. Keep all old backings and select no new output;
        // the next conservative checkpoint must use the observed successor.
        self.checkpoint = checkpoint;
        if replacement.revision != self.revision
            || self
                .nodes
                .get(&replacement.node)
                .and_then(|node| node.active)
                != Some(replacement.owner)
            || self
                .nodes
                .get(&replacement.node)
                .is_some_and(|node| node.containing.is_some_and(|parent| !self.accepts(parent)))
        {
            return Err(SessionError::Stale);
        }
        for owner in replacement.retiring {
            self.session_mut(owner)?.phase = Phase::Retired;
        }
        let node = self
            .nodes
            .get_mut(&replacement.node)
            .expect("validated node");
        node.effective = replacement.effective;
        node.active = None;
        self.revision += 1;
        Ok(())
    }

    // Only the ancestry integrity gate. Recovery, sibling completion and
    // durable physical coverage remain independent publication requirements.
    pub(crate) fn ancestry_verified(&self, mut owner: StreamOwner) -> bool {
        if !self.accepts(owner) {
            return false;
        }
        loop {
            let Ok(session) = self.session(owner) else {
                return false;
            };
            if session.phase != Phase::Closed {
                return false;
            }
            let Some(parent) = session.containing else {
                return true;
            };
            owner = parent;
        }
    }

    #[cfg(test)]
    pub(crate) fn original(&self, node: u32) -> Option<InputVersion> {
        self.nodes.get(&node).map(|node| node.original)
    }

    pub(crate) fn effective(&self, node: u32) -> Option<InputVersion> {
        self.nodes.get(&node).map(|node| node.effective)
    }

    // Execution backing leases only. Original input images have independent
    // ownership and must survive as long as ancestor reconstruction needs them.
    #[cfg(test)]
    pub(crate) fn backing_leases_drained(&self, owner: StreamOwner) -> bool {
        self.session(owner).is_ok_and(|session| {
            session.phase == Phase::Retired && Arc::strong_count(&session.backings) == 1
        })
    }
}

#[cfg(test)]
mod tests;
