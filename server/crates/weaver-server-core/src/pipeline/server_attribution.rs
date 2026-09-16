use super::*;

impl Pipeline {
    pub(crate) fn checkpoint_server_attribution_if_due(&mut self) {
        // UI refreshes run every 100 ms; reporting durability must not turn
        // that cadence into ten database transactions per second.
        if self.server_attribution_checkpoint_at.elapsed() >= std::time::Duration::from_secs(5) {
            self.flush_server_attribution();
            self.server_attribution_checkpoint_at = Instant::now();
        }
    }

    /// Batch reporting checkpoints independently of file floors: direct-store
    /// and uuencode jobs must retain attribution too. The ordered DB writer
    /// keeps older snapshots from replacing newer ones. Abrupt termination can
    /// lose the current interval, as with other progress telemetry.
    pub(crate) fn flush_server_attribution(&mut self) {
        let snapshots: Vec<_> = self
            .dirty_server_attribution
            .iter()
            .filter_map(|id| {
                self.jobs
                    .get(id)?
                    .server_attribution
                    .to_storage_json()
                    .map(|json| (*id, json))
            })
            .collect();
        if snapshots.is_empty() {
            return;
        }
        match self
            .db
            .try_queue_write("active_server_attribution", move |db| {
                db.save_active_server_attribution(snapshots)
            }) {
            Ok(_) => self.dirty_server_attribution.clear(),
            Err(error) => tracing::error!(%error, "failed to queue server attribution checkpoint"),
        }
    }
}
