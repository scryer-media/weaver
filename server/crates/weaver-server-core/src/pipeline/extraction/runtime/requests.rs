//! Admitted namespace requests. An id carries its table's identity, so an id
//! issued by a runtime that a job has since replaced never matches.
use super::{NamespaceWork, RequestId};
use crate::pipeline::extraction::output::channel::StreamOwner;
use crate::pipeline::extraction::session::SessionError;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

pub(super) struct Request {
    pub(super) owner: StreamOwner,
    pub(super) id: RequestId,
    pub(super) work: NamespaceWork,
}

pub(super) struct RequestTable {
    identity: Arc<()>,
    next: u64,
    entries: BTreeMap<u64, Request>,
    // Requests whose namespace may still be discarded. A completed request
    // keeps its mark, so a stream accepted after its worker finished still
    // finds it; retirement clears marks along with their requests.
    speculative: HashSet<u64>,
}

impl RequestTable {
    pub(super) fn new() -> Self {
        Self {
            identity: Arc::new(()),
            next: 0,
            entries: BTreeMap::new(),
            speculative: HashSet::new(),
        }
    }

    pub(super) fn issue(
        &mut self,
        owner: StreamOwner,
        work: NamespaceWork,
    ) -> Result<RequestId, SessionError> {
        let sequence = self.next.checked_add(1).ok_or(SessionError::Admission)?;
        self.next = sequence;
        let id = RequestId {
            identity: Arc::clone(&self.identity),
            sequence,
        };
        let request = Request {
            owner,
            id: id.clone(),
            work,
        };
        self.entries.insert(sequence, request);
        Ok(id)
    }

    fn issued(&self, id: &RequestId) -> bool {
        Arc::ptr_eq(&self.identity, &id.identity)
    }

    pub(super) fn get(&self, id: &RequestId) -> Option<&Request> {
        self.issued(id)
            .then(|| self.entries.get(&id.sequence))
            .flatten()
    }

    pub(super) fn get_mut(&mut self, id: &RequestId) -> Option<&mut Request> {
        if !self.issued(id) {
            return None;
        }
        self.entries.get_mut(&id.sequence)
    }

    pub(super) fn remove(&mut self, id: &RequestId) -> Option<Request> {
        if !self.issued(id) {
            return None;
        }
        self.entries.remove(&id.sequence)
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &Request> {
        self.entries.values()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(super) fn is_speculative(&self, request: &Request) -> bool {
        self.speculative.contains(&request.id.sequence)
    }

    pub(super) fn mark_speculative(&mut self, id: &RequestId) -> Result<(), SessionError> {
        if self.get(id).is_none() {
            return Err(SessionError::Stale);
        }
        self.speculative.insert(id.sequence);
        Ok(())
    }

    pub(super) fn accept_speculative(&mut self, id: &RequestId) -> Result<(), SessionError> {
        if !self.issued(id) || !self.speculative.remove(&id.sequence) {
            return Err(SessionError::Stale);
        }
        Ok(())
    }

    fn reading(&self, namespace: &str) -> impl Iterator<Item = &Request> {
        self.entries.values().filter(move |request| {
            request
                .work
                .virtual_source
                .as_ref()
                .is_some_and(|source| source.root_set == namespace)
        })
    }

    // The parent's own request is the speculation's subject, not part of it.
    pub(super) fn mark_namespace(&mut self, namespace: &str, parent: StreamOwner) {
        let marked: Vec<u64> = self
            .reading(namespace)
            .filter(|request| request.owner != parent)
            .map(|request| request.id.sequence)
            .collect();
        self.speculative.extend(marked);
    }

    pub(super) fn accept_namespace(&mut self, namespace: &str) {
        let accepted: Vec<u64> = self
            .reading(namespace)
            .map(|request| request.id.sequence)
            .collect();
        for sequence in accepted {
            self.speculative.remove(&sequence);
        }
    }

    pub(super) fn forget(&mut self, owners: &[StreamOwner]) {
        self.entries
            .retain(|_, request| !owners.contains(&request.owner));
        let entries = &self.entries;
        self.speculative
            .retain(|sequence| entries.contains_key(sequence));
    }
}
