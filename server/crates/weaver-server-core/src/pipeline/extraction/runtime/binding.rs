use super::*;

impl ArchiveRuntime {
    pub(crate) fn graph_lease(
        &mut self,
        root_set: &str,
        node: u32,
        parent: Option<u32>,
        version: InputVersion,
    ) -> Result<SessionLease, SessionError> {
        let key = (root_set.to_owned(), node);
        if let Some(owner) = self.graph_owners.get(&key).copied() {
            return self.resume_owner(owner);
        }
        let containing = parent
            .map(|parent| {
                self.graph_owners
                    .get(&(root_set.to_owned(), parent))
                    .copied()
                    .ok_or(SessionError::Stale)
            })
            .transpose()?;
        let lease = self.admit(containing, version)?;
        self.graph_owners.insert(key, lease.owner);
        Ok(lease)
    }

    pub(crate) fn resume_owner(
        &mut self,
        owner: StreamOwner,
    ) -> Result<SessionLease, SessionError> {
        if self.sessions.execution_current(owner) || self.sessions.ancestry_verified(owner) {
            return self.sessions.lease(owner);
        }
        let saved = self.sessions.capture_checkpoint();
        let node = saved
            .nodes
            .get(owner.node as usize)
            .ok_or(SessionError::Stale)?;
        if let Some(active) = node.active {
            return self.sessions.lease(StreamOwner {
                root: self.root,
                node: active.node,
                generation: active.generation,
            });
        }
        let parent = node
            .containing
            .map(|parent| {
                self.resume_owner(StreamOwner {
                    root: self.root,
                    node: parent.node,
                    generation: parent.generation,
                })
                .map(|lease| lease.owner)
            })
            .transpose()?;
        let lease = self.sessions.restart_restored(owner.node, parent)?;
        for selected in self.graph_owners.values_mut() {
            if selected.node == owner.node {
                *selected = lease.owner;
            }
        }
        Ok(lease)
    }

    pub(crate) fn close_graph_owner(
        &mut self,
        root_set: &str,
        node: u32,
    ) -> Result<(), SessionError> {
        let owner = *self
            .graph_owners
            .get(&(root_set.to_owned(), node))
            .ok_or(SessionError::Stale)?;
        if self.sessions.ancestry_verified(owner) {
            return Ok(());
        }
        self.sessions.close(owner)
    }

    pub(crate) fn abort_inputs(&self, owners: &[StreamOwner]) {
        for (owner, input) in &self.live_inputs {
            if owners.contains(owner) {
                input.abort("archive input owner revoked");
            }
        }
    }
}

impl crate::pipeline::Pipeline {
    pub(crate) async fn close_scoped_graph_owner(
        &mut self,
        job_id: crate::jobs::ids::JobId,
        root_set: &str,
        node: u32,
    ) -> Result<(), String> {
        self.archive_runtime
            .get_mut(&job_id)
            .ok_or("missing archive graph runtime")?
            .close_graph_owner(root_set, node)
            .map_err(|e| format!("archive graph close: {e:?}"))?;
        self.publish_scoped_completions(job_id).await;
        Ok(())
    }
}

impl ArchiveRuntime {
    #[cfg(test)]
    pub(crate) fn protects_input(&self, path: &std::path::Path) -> bool {
        self.requests
            .values()
            .map(|request| &request.work)
            .chain(self.completed.iter().map(|(_, work, _)| work))
            .chain(self.retained_work.iter().map(|(_, work)| work))
            .chain(self.replay.iter().map(|(_, work)| work))
            .any(|work| {
                work.original_inputs
                    .iter()
                    .chain(work.inputs.iter())
                    .any(|input| input == path)
            })
            || self.staged_replacement.as_ref().is_some_and(|staged| {
                staged
                    .work
                    .original_inputs
                    .iter()
                    .chain(staged.work.inputs.iter())
                    .any(|input| input == path)
            })
    }
    pub(crate) fn retains_archive_set(&self, set_name: &str) -> bool {
        self.graph_owners.contains_key(&(set_name.to_owned(), 0))
            || self
                .requests
                .values()
                .any(|request| request.work.set_name == set_name)
            || self
                .retained_work
                .iter()
                .any(|(_, work)| work.set_name == set_name)
    }
}

impl crate::pipeline::Pipeline {
    pub(crate) async fn resume_scoped_replay(
        &mut self,
        job_id: crate::jobs::ids::JobId,
    ) -> Result<(), String> {
        let restored: Vec<_> = self
            .archive_runtime
            .get(&job_id)
            .map(|runtime| {
                runtime
                    .decoded
                    .iter()
                    .filter_map(|state| {
                        let decoded::NamespaceProgress {
                            producers,
                            children,
                        } = state.progress_snapshot();
                        let namespace = format!(
                            "decoded-{}-{}",
                            state.generation.owner().node,
                            state.generation.owner().generation
                        );
                        children
                            .iter()
                            .any(|child| {
                                !runtime.gates.contains_key(&(namespace.clone(), child.node))
                            })
                            .then_some((namespace, producers, children))
                    })
                    .collect()
            })
            .unwrap_or_default();
        for (namespace, producers, children) in restored {
            let runtime = self.archive_runtime.get(&job_id).expect("restored runtime");
            let (working, destination) = runtime
                .checkpoint_roots
                .clone()
                .ok_or("restored namespace roots absent")?;
            Box::pin(self.publish_scoped_namespace(
                job_id,
                &namespace,
                decoded::NamespaceProgress {
                    producers,
                    children,
                },
                working,
                destination,
                None,
            ))
            .await?;
        }
        let Some(runtime) = self.archive_runtime.get_mut(&job_id) else {
            return Ok(());
        };
        let pending = std::mem::take(&mut runtime.replay);
        let mut ready = Vec::new();
        for (owner, work) in pending {
            if work.virtual_source.as_ref().is_some_and(|source| {
                !runtime
                    .gates
                    .contains_key(&(source.root_set.clone(), source.node))
            }) {
                runtime.replay.push((owner, work));
                continue;
            }
            let lease = runtime
                .resume_owner(owner)
                .map_err(|error| format!("archive replay lifecycle: {error:?}"))?;
            ready.push((work, lease));
        }
        for (work, lease) in ready {
            self.spawn_scoped_selected_extraction(job_id, work, lease)
                .await?;
        }
        Ok(())
    }
}

impl ArchiveRuntime {
    pub(crate) fn mark_speculative(&mut self, request: &RequestId) -> Result<(), SessionError> {
        if !Arc::ptr_eq(&self.identity, &request.identity)
            || !self.requests.contains_key(&request.sequence)
        {
            return Err(SessionError::Stale);
        }
        self.speculative_requests.insert(request.sequence);
        Ok(())
    }
    pub(crate) fn accept_speculative(&mut self, request: &RequestId) -> Result<(), SessionError> {
        if !Arc::ptr_eq(&self.identity, &request.identity)
            || !self.speculative_requests.remove(&request.sequence)
        {
            return Err(SessionError::Stale);
        }
        if let Some(request) = self.requests.get_mut(&request.sequence) {
            request.work.root_set = None;
        }
        Ok(())
    }
    fn retire_speculative_tree(
        &mut self,
        id: &RequestId,
    ) -> Result<Vec<StreamOwner>, SessionError> {
        if !Arc::ptr_eq(&self.identity, &id.identity) {
            return Err(SessionError::Stale);
        }
        let owner = self
            .requests
            .get(&id.sequence)
            .ok_or(SessionError::Stale)?
            .owner;
        self.retire_namespace_tree(owner)
    }
    fn retire_namespace_tree(
        &mut self,
        owner: StreamOwner,
    ) -> Result<Vec<StreamOwner>, SessionError> {
        let revoked = self.sessions.revoke(owner)?;
        self.abort_inputs(&revoked);
        self.retire_sinks(&revoked);
        self.requests
            .retain(|_, request| !revoked.contains(&request.owner));
        self.speculative_requests
            .retain(|sequence| self.requests.contains_key(sequence));
        self.completed
            .retain(|(owner, _, _)| !revoked.contains(owner));
        self.retained_work
            .retain(|(owner, _)| !revoked.contains(owner));
        self.deferred_repairs
            .retain(|(owner, _)| !revoked.contains(owner));
        self.replay.retain(|(owner, _)| !revoked.contains(owner));
        self.decoded
            .retain(|state| !revoked.contains(&state.generation.owner()));
        self.output_owners
            .retain(|_, owner| !revoked.contains(owner));
        self.graph_owners
            .retain(|_, owner| !revoked.contains(owner));
        self.speculative_namespaces.retain(|namespace| {
            self.graph_owners
                .keys()
                .any(|(bound, _)| bound == namespace)
        });
        self.gates
            .retain(|key, _| self.graph_owners.contains_key(key));
        self.live_inputs
            .retain(|(owner, _)| !revoked.contains(owner));
        if self.staged_replacement.as_ref().is_some_and(|staged| {
            revoked.contains(&staged.selected)
                || staged.retiring.iter().any(|owner| revoked.contains(owner))
        }) {
            self.staged_replacement.take();
        }
        Ok(revoked)
    }
    #[cfg(test)]
    pub(crate) fn abandon_speculative(&mut self, id: &RequestId) -> Result<(), SessionError> {
        // Synchronous demotion fences authority now. Existing tracked workers
        // retain private backings until their cancellation checkpoints return.
        self.retire_speculative_tree(id)?;
        Ok(())
    }
    pub(crate) async fn discard_speculative(
        &mut self,
        request: &RequestId,
    ) -> Result<(), SessionError> {
        let revoked = self.retire_speculative_tree(request)?;
        self.join_retired_workers(revoked).await;
        Ok(())
    }
    pub(crate) fn retire_owner_barrier(
        &mut self,
        owner: StreamOwner,
    ) -> Result<tokio::sync::watch::Receiver<bool>, SessionError> {
        let revoked = self.retire_namespace_tree(owner)?;
        let mut owned = Vec::new();
        let mut remaining = Vec::new();
        for (worker_owner, worker) in std::mem::take(&mut self.workers) {
            if revoked.contains(&worker_owner) {
                owned.push(worker);
            } else {
                remaining.push((worker_owner, worker));
            }
        }
        let (completed, observer) = tokio::sync::watch::channel(owned.is_empty());
        if !owned.is_empty() {
            // Keep the barrier in the owner registry so a later retirement
            // still joins every producer while staging cleanup observes it.
            remaining.push((
                owner,
                tokio::spawn(async move {
                    for worker in owned {
                        let _ = worker.await;
                    }
                    completed.send_replace(true);
                }),
            ));
        }
        self.workers = remaining;
        Ok(observer)
    }

    pub(crate) async fn retire_owner_joined(
        &mut self,
        owner: StreamOwner,
    ) -> Result<(), SessionError> {
        let revoked = self.retire_namespace_tree(owner)?;
        self.join_retired_workers(revoked).await;
        Ok(())
    }

    pub(crate) async fn join_abandoned_owner(
        &mut self,
        owner: StreamOwner,
    ) -> Result<(), SessionError> {
        if !self.sessions.is_retiring(owner) {
            return Err(SessionError::Stale);
        }
        let revoked = self.sessions.revoke(owner)?;
        self.join_retired_workers(revoked).await;
        Ok(())
    }
    pub(crate) async fn prepare_graph_decoder(
        &mut self,
        set_name: &str,
        node: u32,
        parent: u32,
    ) -> Result<(), SessionError> {
        let key = (set_name.to_owned(), node);
        if self.gates.contains_key(&key)
            || self.has_replay_source(set_name, node)
            || self.graph_source_replaced(set_name, node)
        {
            return Ok(());
        }
        let Some(owner) = self.graph_owners.get(&key).copied() else {
            return Ok(());
        };
        let revoked = self.retire_namespace_tree(owner)?;
        self.join_retired_workers(revoked).await;
        self.graph_lease(set_name, node, Some(parent), InputVersion(0))?;
        Ok(())
    }

    pub(crate) async fn retire_graph_namespace(
        &mut self,
        set_name: &str,
    ) -> Result<(), SessionError> {
        let Some(owner) = self.graph_owners.get(&(set_name.to_owned(), 0)).copied() else {
            return Ok(());
        };
        let revoked = self.retire_namespace_tree(owner)?;
        self.join_retired_workers(revoked).await;
        Ok(())
    }
    async fn join_retired_workers(&mut self, revoked: Vec<StreamOwner>) {
        let mut remaining = Vec::new();
        for (owner, worker) in std::mem::take(&mut self.workers) {
            if revoked.contains(&owner) {
                let _ = worker.await;
            } else {
                remaining.push((owner, worker));
            }
        }
        self.workers = remaining;
        for owner in revoked {
            let _ = self.sessions.joined(owner);
        }
    }
}

impl ArchiveRuntime {
    pub(crate) fn mark_namespace_speculative(&mut self, namespace: &str, parent: StreamOwner) {
        self.speculative_namespaces.insert(namespace.to_owned());
        for request in self.requests.values() {
            if request.owner != parent
                && request
                    .work
                    .virtual_source
                    .as_ref()
                    .is_some_and(|source| source.root_set == namespace)
            {
                self.speculative_requests.insert(request.id.sequence);
            }
        }
    }
    pub(crate) fn accept_namespace(&mut self, namespace: &str) {
        self.speculative_namespaces.remove(namespace);
        for request in self.requests.values() {
            if request
                .work
                .virtual_source
                .as_ref()
                .is_some_and(|source| source.root_set == namespace)
            {
                self.speculative_requests.remove(&request.id.sequence);
            }
        }
    }
}
impl crate::pipeline::Pipeline {
    pub(crate) async fn handle_scoped_namespace(
        &mut self,
        job_id: crate::jobs::ids::JobId,
        owner: StreamOwner,
        namespace: String,
        producers: Vec<(u32, Option<u32>, u64, bool)>,
        children: Vec<crate::pipeline::direct_store::set::LiveChildDescription>,
    ) {
        let Some(runtime) = self.archive_runtime.get_mut(&job_id) else {
            return;
        };
        if !runtime.sessions.execution_current(owner) {
            return;
        }
        let Some((working, destination)) = runtime.checkpoint_roots.clone() else {
            return;
        };
        if runtime
            .graph_owners
            .get(&(namespace.clone(), 0))
            .is_some_and(|bound| *bound != owner)
        {
            return;
        }
        runtime.graph_owners.insert((namespace.clone(), 0), owner);
        if let Err(error) = self
            .publish_scoped_namespace(
                job_id,
                &namespace,
                decoded::NamespaceProgress {
                    producers,
                    children,
                },
                working,
                destination,
                Some(owner),
            )
            .await
        {
            if let Some(runtime) = self.archive_runtime.get_mut(&job_id) {
                runtime.fail(owner);
            }
            self.fail_job(
                job_id,
                format!("archive namespace streaming failed: {error}"),
            );
        }
    }
}

impl ArchiveRuntime {
    pub(crate) fn has_replay_source(&self, namespace: &str, node: u32) -> bool {
        self.replay.iter().any(|(_, work)| {
            work.virtual_source
                .as_ref()
                .is_some_and(|source| source.root_set == namespace && source.node == node)
        })
    }
    pub(crate) fn namespace_nodes(&self, namespace: &str) -> HashSet<u32> {
        self.requests
            .values()
            .map(|request| &request.work)
            .chain(self.replay.iter().map(|(_, work)| work))
            .chain(self.retained_work.iter().map(|(_, work)| work))
            .chain(self.completed.iter().map(|(_, work, _)| work))
            .filter_map(|work| {
                work.original_virtual_source
                    .as_ref()
                    .filter(|source| source.root_set == namespace)
                    .map(|source| source.node)
            })
            .collect()
    }
}

#[cfg(test)]
mod decoder_transition_tests {
    use super::*;

    fn work() -> NamespaceWork {
        NamespaceWork {
            kind: crate::jobs::assembly::ArchiveType::SevenZip,
            set_name: "child".into(),
            volumes: Vec::new(),
            output: PathBuf::from("private"),
            inputs: Vec::new(),
            original_inputs: Vec::new(),
            input_names: Vec::new(),
            virtual_source: None,
            original_virtual_source: None,
            destination: PathBuf::from("staging"),
            outputs: BTreeMap::new(),
            root_set: None,
            ordinary: Vec::new(),
            selected_password: None,
        }
    }

    #[tokio::test]
    async fn replacing_direct_producer_with_decoder_retires_completed_descendant() {
        let mut runtime = ArchiveRuntime::new();
        let root = runtime
            .graph_lease("namespace", 0, None, InputVersion(0))
            .unwrap()
            .owner;
        let old = runtime
            .graph_lease("namespace", 1, Some(0), InputVersion(0))
            .unwrap()
            .owner;
        let descendant = runtime
            .graph_lease("namespace", 2, Some(1), InputVersion(0))
            .unwrap();
        let descendant_owner = descendant.owner;
        let request = runtime.dispatch(descendant_owner, work()).unwrap();
        let completed = runtime.complete(descendant_owner, request).unwrap();
        runtime.sessions.close(descendant_owner).unwrap();
        runtime.sessions.close(old).unwrap();
        runtime
            .completed
            .push((descendant_owner, completed, vec!["stale.bin".into()]));
        runtime
            .output_owners
            .insert("stale.bin".into(), descendant_owner);
        let (joined, finished) = tokio::sync::oneshot::channel();
        runtime.track_worker(
            descendant_owner,
            tokio::spawn(async move {
                descendant.cancelled().await;
                joined.send(()).unwrap();
            }),
        );
        runtime
            .prepare_graph_decoder("namespace", 1, 0)
            .await
            .unwrap();
        finished.await.unwrap();
        let fresh = runtime.graph_owners[&("namespace".into(), 1)];
        assert_ne!(fresh, old);
        assert!(runtime.sessions.execution_current(root));
        assert!(!runtime.sessions.execution_current(descendant_owner));
        assert!(!runtime.graph_owners.contains_key(&("namespace".into(), 2)));
        assert!(runtime.completed.is_empty());
        assert!(!runtime.output_owners.contains_key("stale.bin"));
        runtime.sessions.close(fresh).unwrap();
        runtime.sessions.close(root).unwrap();
        assert!(runtime.sessions.ancestry_verified(fresh));
        assert!(!runtime.sessions.ancestry_verified(descendant_owner));
        assert!(runtime.publication_ready());
    }

    #[tokio::test]
    async fn restored_decoder_without_gate_keeps_its_replay_owner() {
        let mut runtime = ArchiveRuntime::new();
        runtime
            .graph_lease("namespace", 0, None, InputVersion(0))
            .unwrap();
        let owner = runtime
            .graph_lease("namespace", 1, Some(0), InputVersion(0))
            .unwrap()
            .owner;
        let mut replay = work();
        replay.virtual_source = Some(VirtualInputSource {
            root_set: "namespace".into(),
            node: 1,
        });
        runtime.replay.push((owner, replay));
        runtime
            .prepare_graph_decoder("namespace", 1, 0)
            .await
            .unwrap();
        assert_eq!(runtime.graph_owners[&("namespace".into(), 1)], owner);
        assert_eq!(runtime.replay.len(), 1);
    }
}
