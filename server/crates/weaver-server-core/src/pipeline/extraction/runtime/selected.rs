use super::*;

impl ArchiveRuntime {
    pub(crate) fn retained_input_paths(&self) -> Vec<PathBuf> {
        self.requests
            .values()
            .map(|request| &request.work)
            .chain(self.replay.iter().map(|(_, work)| work))
            .chain(self.completed.iter().map(|(_, work, _)| work))
            .chain(self.retained_work.iter().map(|(_, work)| work))
            .chain(self.deferred_repairs.iter().map(|(_, work)| work))
            .chain(self.staged_replacement.iter().map(|staged| &staged.work))
            .flat_map(|work| work.inputs.iter().chain(&work.original_inputs).cloned())
            .collect()
    }

    pub(crate) async fn join_finished_workers(&mut self) -> Result<(), RuntimeError> {
        if !self.publication_ready() {
            return Err(RuntimeError::Invariant(
                "archive namespace publication remains unresolved",
            ));
        }
        for worker in self.workers.take_all() {
            worker
                .await
                .map_err(RuntimeError::join("archive namespace worker"))?;
        }
        Ok(())
    }

    pub(crate) fn retains_input_path(&self, path: &std::path::Path) -> bool {
        self.requests
            .values()
            .map(|request| &request.work)
            .chain(self.replay.iter().map(|(_, work)| work))
            .chain(self.completed.iter().map(|(_, work, _)| work))
            .chain(self.retained_work.iter().map(|(_, work)| work))
            .chain(self.deferred_repairs.iter().map(|(_, work)| work))
            .chain(self.staged_replacement.iter().map(|staged| &staged.work))
            .any(|work| {
                work.inputs
                    .iter()
                    .chain(&work.original_inputs)
                    .any(|input| input == path)
            })
    }

    pub(crate) fn selected_graph_files(
        &self,
        root_set: &str,
        node: u32,
    ) -> Option<BTreeMap<u32, PathBuf>> {
        self.selected_graph_work(root_set, node).map(|work| {
            work.volumes
                .iter()
                .copied()
                .zip(work.inputs.iter().cloned())
                .collect()
        })
    }

    pub(crate) fn graph_source_replaced(&self, root_set: &str, node: u32) -> bool {
        self.selected_graph_work(root_set, node).is_some()
    }

    fn selected_graph_work(&self, root_set: &str, node: u32) -> Option<&NamespaceWork> {
        self.requests
            .values()
            .map(|request| (request.owner, &request.work))
            .chain(self.replay.iter().map(|(owner, work)| (*owner, work)))
            .chain(self.completed.iter().map(|(owner, work, _)| (*owner, work)))
            .chain(
                self.retained_work
                    .iter()
                    .map(|(owner, work)| (*owner, work)),
            )
            .filter(|(owner, work)| {
                self.sessions.accepts(*owner)
                    && work.virtual_source.is_none()
                    && work
                        .original_virtual_source
                        .as_ref()
                        .is_some_and(|source| source.root_set == root_set && source.node == node)
                    && work.volumes.len() == work.inputs.len()
            })
            .max_by_key(|(owner, _)| owner.generation)
            .map(|(_, work)| work)
    }
}
