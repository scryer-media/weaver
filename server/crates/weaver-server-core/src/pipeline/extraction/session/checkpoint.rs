//! Durable lifecycle facts. Channels, cancellation handles and workers are rebuilt.
use super::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Checkpoint {
    pub(crate) root: u64,
    pub(crate) revision: u64,
    pub(crate) checkpoint: u64,
    pub(crate) nodes: Vec<NodeCheckpoint>,
    pub(crate) sessions: Vec<SessionCheckpoint>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_preserves_original_and_does_not_verify_crashed_worker() {
        let mut sessions = ArchiveSessions::new(10, 3);
        let node = sessions.register(None, InputVersion(7)).unwrap();
        let old = sessions.start(node).unwrap().owner;
        let saved = sessions.capture_checkpoint();
        let mut restored = ArchiveSessions::restore_checkpoint(&saved, 20).unwrap();
        assert_eq!(restored.original(node), Some(InputVersion(7)));
        assert_eq!(restored.effective(node), Some(InputVersion(7)));
        assert!(!restored.accepts(old));
        let old_remapped = StreamOwner { root: 20, ..old };
        assert!(!restored.ancestry_verified(old_remapped));
        let fresh = restored.start(node).unwrap().owner;
        assert!(fresh.generation > old.generation);
        assert!(!restored.ancestry_verified(fresh));
    }

    #[test]
    fn restart_refuses_cycles_and_duplicate_generations() {
        let mut sessions = ArchiveSessions::new(1, 0);
        let node = sessions.register(None, InputVersion(1)).unwrap();
        sessions.start(node).unwrap();
        let mut saved = sessions.capture_checkpoint();
        saved.sessions.push(saved.sessions[0].clone());
        assert!(ArchiveSessions::restore_checkpoint(&saved, 2).is_err());
        saved.sessions.pop();
        saved.nodes[0].containing = Some(Owner {
            node: 0,
            generation: 1,
        });
        assert!(ArchiveSessions::restore_checkpoint(&saved, 2).is_err());
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct Owner {
    pub(crate) node: u32,
    pub(crate) generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeCheckpoint {
    pub(crate) containing: Option<Owner>,
    pub(crate) depth: u32,
    pub(crate) original: u64,
    pub(crate) effective: u64,
    pub(crate) next_generation: u64,
    pub(crate) active: Option<Owner>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SessionCheckpoint {
    pub(crate) owner: Owner,
    pub(crate) containing: Option<Owner>,
    pub(crate) phase: u8,
}

impl SessionCheckpoint {
    pub(crate) fn is_retired(&self) -> bool {
        self.phase == Phase::Retired.code()
    }
}

impl Phase {
    const fn code(self) -> u8 {
        match self {
            Self::Running => 0,
            Self::Closed => 1,
            Self::Draining => 2,
            Self::Drained => 3,
            Self::Retired => 4,
        }
    }

    const fn restored(code: u8) -> Option<Self> {
        match code {
            // A crashed worker did not prove integrity. Preserve its
            // unverified ancestry while a new execution gets a new owner.
            0 => Some(Self::Running),
            1 => Some(Self::Closed),
            // No writer survives a restart, so a drain in progress has ended.
            2 | 3 => Some(Self::Drained),
            4 => Some(Self::Retired),
            _ => None,
        }
    }
}

impl ArchiveSessions {
    pub(crate) fn preview_replacement_checkpoint(
        &self,
        replacement: &Replacement,
    ) -> Result<(Checkpoint, StreamOwner), SessionError> {
        if replacement.root != self.root || replacement.revision != self.revision {
            return Err(SessionError::Stale);
        }
        let node = self
            .nodes
            .get(&replacement.node)
            .ok_or(SessionError::Stale)?;
        if node.active != Some(replacement.owner) {
            return Err(SessionError::Stale);
        }
        let generation = node.next_generation;
        let next = generation.checked_add(1).ok_or(SessionError::Admission)?;
        let mut saved = self.capture_checkpoint();
        saved.checkpoint = self
            .checkpoint
            .checked_add(1)
            .ok_or(SessionError::Admission)?;
        saved.revision = self
            .revision
            .checked_add(2)
            .ok_or(SessionError::Admission)?;
        for session in &mut saved.sessions {
            if replacement.retiring.iter().any(|owner| {
                owner.node == session.owner.node && owner.generation == session.owner.generation
            }) {
                session.phase = Phase::Retired.code();
            }
        }
        let selected = &mut saved.nodes[replacement.node as usize];
        selected.effective = replacement.effective.0;
        selected.active = Some(Owner {
            node: replacement.node,
            generation,
        });
        selected.next_generation = next;
        saved.sessions.push(SessionCheckpoint {
            owner: Owner {
                node: replacement.node,
                generation,
            },
            containing: selected.containing,
            phase: Phase::Running.code(),
        });
        Ok((
            saved,
            StreamOwner {
                root: self.root,
                node: replacement.node,
                generation,
            },
        ))
    }

    pub(crate) fn capture_checkpoint(&self) -> Checkpoint {
        let owner = |value: StreamOwner| Owner {
            node: value.node,
            generation: value.generation,
        };
        Checkpoint {
            root: self.root,
            revision: self.revision,
            checkpoint: self.checkpoint,
            nodes: self
                .nodes
                .values()
                .map(|node| NodeCheckpoint {
                    containing: node.containing.map(owner),
                    depth: node.depth,
                    original: node.original.0,
                    effective: node.effective.0,
                    next_generation: node.next_generation,
                    active: node.active.map(owner),
                })
                .collect(),
            sessions: self
                .sessions
                .iter()
                .map(|(&(node, generation), session)| SessionCheckpoint {
                    owner: Owner { node, generation },
                    containing: session.containing.map(owner),
                    phase: session.phase.code(),
                })
                .collect(),
        }
    }

    pub(crate) fn restore_checkpoint(saved: &Checkpoint, root: u64) -> Result<Self, SessionError> {
        if saved.nodes.len() > MAX_NODES || saved.sessions.len() > MAX_SESSIONS {
            return Err(SessionError::Admission);
        }
        let owner = |value: Owner| StreamOwner {
            root,
            node: value.node,
            generation: value.generation,
        };
        let mut restored = Self::new(root, saved.checkpoint);
        restored.revision = saved.revision;
        for (id, node) in saved.nodes.iter().enumerate() {
            let depth = match node.containing {
                Some(parent) if (parent.node as usize) < id => {
                    saved.nodes[parent.node as usize].depth + 1
                }
                None => 1,
                _ => return Err(SessionError::Admission),
            };
            if node.depth != depth || depth > MAX_DEPTH || node.next_generation == 0 {
                return Err(SessionError::Admission);
            }
            restored.nodes.insert(
                id as u32,
                Node {
                    containing: node.containing.map(owner),
                    depth,
                    original: InputVersion(node.original),
                    effective: InputVersion(node.effective),
                    next_generation: node.next_generation,
                    // Process-local workers cannot survive a restart. Their old
                    // owner remains an immutable ancestry record, never active.
                    active: None,
                },
            );
        }
        for session in &saved.sessions {
            let node = restored
                .nodes
                .get(&session.owner.node)
                .ok_or(SessionError::Admission)?;
            let phase = Phase::restored(session.phase).ok_or(SessionError::Admission)?;
            if session.owner.generation == 0 || session.owner.generation >= node.next_generation {
                return Err(SessionError::Admission);
            }
            let containing = session.containing.map(owner);
            if containing != node.containing {
                return Err(SessionError::Admission);
            }
            let cancelled = !matches!(phase, Phase::Running | Phase::Closed);
            let (wake, _) = watch::channel(cancelled);
            if restored
                .sessions
                .insert(
                    (session.owner.node, session.owner.generation),
                    Session {
                        containing,
                        phase,
                        cancellation: Arc::new(AtomicBool::new(cancelled)),
                        cancellation_wake: wake,
                        backings: Arc::new(()),
                        channel: None,
                    },
                )
                .is_some()
            {
                return Err(SessionError::Admission);
            }
        }
        for node in restored.nodes.values() {
            if let Some(parent) = node.containing {
                restored.session(parent)?;
            }
        }
        for (id, node) in saved.nodes.iter().enumerate() {
            if let Some(active) = node.active
                && (active.node as usize != id
                    || !restored
                        .sessions
                        .contains_key(&(active.node, active.generation)))
            {
                return Err(SessionError::Admission);
            }
        }
        Ok(restored)
    }
}
