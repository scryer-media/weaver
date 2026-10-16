use super::*;

pub(super) struct StagedReplacement {
    replacement: super::super::session::Replacement,
    pub(super) sessions: super::super::session::checkpoint::Checkpoint,
    pub(super) selected: StreamOwner,
    pub(super) retiring: Vec<StreamOwner>,
    pub(super) work: NamespaceWork,
    committed: bool,
}

impl ArchiveRuntime {
    pub(crate) async fn begin_repair(&mut self, owner: StreamOwner) -> Result<(), RuntimeError> {
        if self.staged_replacement.is_some() {
            return Err(RuntimeError::Invariant(
                "archive replacement already staged",
            ));
        }
        let draining = self
            .sessions
            .revoke(owner)
            .map_err(RuntimeError::session("archive repair revoke"))?;
        self.retire_sinks(&draining);
        self.abort_inputs(&draining, "archive input generation retired for repair");
        let mut join_error = None;
        for worker in self.workers.take(&draining) {
            if let Err(error) = worker.await {
                join_error.get_or_insert(error);
            }
        }
        for revoked in &draining {
            self.sessions
                .joined(*revoked)
                .map_err(RuntimeError::session("archive repair join"))?;
        }
        self.requests.forget(&draining);
        match join_error {
            Some(error) => Err(RuntimeError::join("archive repair worker")(error)),
            None => Ok(()),
        }
    }

    pub(crate) fn stage_repair_replacement(
        &mut self,
        owner: StreamOwner,
        mut work: NamespaceWork,
        effective_inputs: Vec<PathBuf>,
        effective_names: Vec<String>,
    ) -> Result<(), RuntimeError> {
        if self.staged_replacement.is_some()
            || effective_inputs.is_empty()
            || effective_inputs.len() != effective_names.len()
        {
            return Err(RuntimeError::Invariant("archive replacement inventory"));
        }
        let (working, _) = self
            .checkpoint_roots
            .as_ref()
            .ok_or(RuntimeError::Invariant("archive replacement roots"))?;
        let effective = self
            .sessions
            .effective(owner.node)
            .ok_or(RuntimeError::Invariant("archive replacement node"))?
            .0
            .checked_add(1)
            .ok_or(RuntimeError::Invariant("archive input version exhausted"))?;
        let replacement = self
            .sessions
            .prepare_replacement(owner.node, InputVersion(effective))
            .map_err(RuntimeError::session("archive replacement prepare"))?;
        let (sessions, selected) = self
            .sessions
            .preview_replacement_checkpoint(&replacement)
            .map_err(RuntimeError::session("archive replacement preview"))?;
        let retiring = sessions
            .sessions
            .iter()
            .filter(|session| session.is_retired())
            .map(|session| StreamOwner {
                root: self.root,
                node: session.owner.node,
                generation: session.owner.generation,
            })
            .collect();
        if work.original_inputs.is_empty() && work.original_virtual_source.is_none() {
            work.original_inputs = work.inputs.clone();
        }
        work.inputs = effective_inputs;
        work.virtual_source = None;
        work.input_names = effective_names;
        work.output = working
            .join(crate::pipeline::direct_store::manifest::DIRECTORY)
            .join(format!(
                "replacement-{}-{}",
                owner.node, selected.generation
            ));
        work.outputs.clear();
        self.staged_replacement = Some(StagedReplacement {
            replacement,
            sessions,
            selected,
            retiring,
            work,
            committed: false,
        });
        Ok(())
    }

    // The caller may invoke this only after the job SQL owner selected the
    // staged graph, including readback reconciliation after an ambiguous CAS.
    pub(crate) fn finish_repair_replacement(
        &mut self,
    ) -> Result<(NamespaceWork, SessionLease), RuntimeError> {
        if !self
            .staged_replacement
            .as_ref()
            .is_some_and(|staged| staged.committed)
        {
            return Err(RuntimeError::Invariant(
                "archive replacement checkpoint is not confirmed",
            ));
        }
        let staged = self
            .staged_replacement
            .take()
            .ok_or(RuntimeError::Invariant("archive replacement is not staged"))?;
        self.sessions
            .committed_replacement(staged.replacement, staged.sessions.checkpoint)
            .map_err(RuntimeError::session("archive replacement activation"))?;
        self.forget_owners(&staged.retiring, Departure::Replaced(staged.selected));
        self.failed = false;
        let lease = self
            .sessions
            .start(staged.selected.node)
            .map_err(RuntimeError::session("archive replacement start"))?;
        if lease.owner != staged.selected {
            return Err(RuntimeError::Invariant(
                "archive replacement selected generation mismatch",
            ));
        }
        Ok((staged.work, lease))
    }

    pub(super) fn checkpoint_keeps_owner(&self, owner: StreamOwner) -> bool {
        self.staged_replacement
            .as_ref()
            .is_none_or(|staged| !staged.retiring.contains(&owner))
            && !self
                .graph_owners
                .iter()
                .any(|((namespace, node), selected)| {
                    *node != 0
                        && *selected == owner
                        && self.speculative_namespaces.contains(namespace)
                })
    }

    pub(crate) fn confirm_replacement_checkpoint(
        &mut self,
        selected: &super::super::session::checkpoint::Checkpoint,
    ) -> Result<(), RuntimeError> {
        let Some(staged) = &mut self.staged_replacement else {
            return Ok(());
        };
        let expected = rmp_serde::to_vec(&staged.sessions)
            .map_err(RuntimeError::checkpoint("staged archive lifecycle"))?;
        let selected = rmp_serde::to_vec(selected)
            .map_err(RuntimeError::checkpoint("committed archive lifecycle"))?;
        if expected != selected {
            return Err(RuntimeError::Invariant(
                "committed archive lifecycle differs from staged replacement",
            ));
        }
        staged.committed = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn replacement_cannot_activate_before_selected_checkpoint_confirmation() {
        let directory = tempfile::tempdir().unwrap();
        let working = directory.path().join("working");
        let staging = directory.path().join("staging");
        let original = working.join("original.7z");
        let effective = working.join("effective.7z");
        let mut runtime = ArchiveRuntime::new();
        runtime.set_roots(working.clone(), staging.clone());
        let owner = runtime.admit(None, InputVersion(9)).unwrap().owner;
        runtime.begin_repair(owner).await.unwrap();
        let work = NamespaceWork {
            kind: crate::jobs::assembly::ArchiveType::SevenZip,
            set_name: "archive".into(),
            volumes: vec![0],
            output: working.join("old-output"),
            inputs: vec![original.clone()],
            original_inputs: vec![original.clone()],
            input_names: vec!["original.7z".into()],
            destination: staging,
            outputs: BTreeMap::new(),
            root_set: None,
            ordinary: Vec::new(),
            selected_password: None,
            virtual_source: None,
            original_virtual_source: None,
        };
        runtime.retained_work.push((owner, work.clone()));
        runtime.deferred_repairs.push((owner, work.clone()));
        runtime
            .stage_repair_replacement(
                owner,
                work,
                vec![effective.clone()],
                vec!["effective.7z".into()],
            )
            .unwrap();
        let candidate = runtime
            .staged_replacement
            .as_ref()
            .unwrap()
            .sessions
            .clone();
        assert_eq!(candidate.nodes[owner.node as usize].original, 9);
        assert_eq!(candidate.nodes[owner.node as usize].effective, 10);
        assert_eq!(
            runtime.sessions.effective(owner.node),
            Some(InputVersion(9))
        );
        assert!(runtime.finish_repair_replacement().is_err());
        let mut wrong = candidate.clone();
        wrong.nodes[owner.node as usize].effective = 11;
        assert!(runtime.confirm_replacement_checkpoint(&wrong).is_err());
        assert!(runtime.finish_repair_replacement().is_err());
        runtime.confirm_replacement_checkpoint(&candidate).unwrap();
        let (work, lease) = runtime.finish_repair_replacement().unwrap();
        assert!(runtime.retained_work.is_empty());
        assert!(runtime.deferred_repairs.is_empty());
        assert_eq!(lease.owner.node, owner.node);
        assert!(lease.owner.generation > owner.generation);
        assert_eq!(work.inputs, vec![effective]);
        assert_eq!(work.original_inputs, vec![original]);
        assert_eq!(runtime.sessions.original(owner.node), Some(InputVersion(9)));
        assert_eq!(
            runtime.sessions.effective(owner.node),
            Some(InputVersion(10))
        );
        assert!(!runtime.sessions.ancestry_verified(lease.owner));
        assert_eq!(runtime.sessions.capture_checkpoint().checkpoint, 1);
    }

    #[tokio::test]
    async fn repair_revokes_and_joins_descendant_workers_before_staging() {
        let mut runtime = ArchiveRuntime::new();
        let parent = runtime.admit(None, InputVersion(1)).unwrap().owner;
        let child = runtime.admit(Some(parent), InputVersion(2)).unwrap();
        let child_owner = child.owner;
        let joined = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = Arc::clone(&joined);
        runtime.track_worker(
            child_owner,
            tokio::spawn(async move {
                child.cancelled().await;
                observed.store(true, Ordering::Release);
            }),
        );
        runtime.begin_repair(parent).await.unwrap();
        assert!(joined.load(Ordering::Acquire));
        assert!(!runtime.sessions.accepts(parent));
        assert!(!runtime.sessions.accepts(child_owner));
        assert!(
            runtime
                .sessions
                .prepare_replacement(parent.node, InputVersion(3))
                .is_ok()
        );
    }
}
