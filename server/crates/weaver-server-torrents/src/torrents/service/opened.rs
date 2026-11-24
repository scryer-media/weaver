// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The service's own waits, for its tests.

use super::*;

impl TorrentService {
    // `add`, and the wait it began for the torrent's route's session ended:
    // the torrent is in that session, or held for why it did not open.
    pub async fn add_opened(&self, request: AddRequest) -> Result<JobId, AddError> {
        let job_id = self.add(request).await?;
        self.opened(job_id).await;
        Ok(job_id)
    }

    // Resolves once `job_id` waits no more on its route's session.
    pub async fn opened(&self, job_id: JobId) {
        let mut ended = self.attaches();
        loop {
            ended.borrow_and_update();
            if !self.attaching(job_id).await {
                return;
            }
            ended.changed().await.unwrap();
        }
    }
}
