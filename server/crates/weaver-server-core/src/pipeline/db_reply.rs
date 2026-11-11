// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What the database answered for the pipeline.
//
// A turn never waits on the database. A write the turn acts on, such as a
// slot taken or let go, goes on its job's writer lane and answers here once
// it commits; a read the turn needs, such as the jobs a walk installs, runs
// on a task and answers here. The loop hears these as it hears any result.

use super::*;

pub(crate) enum DbReply {
    // A slot's write ran: `admitted` is what was written, `committed` whether
    // it committed.
    Slot {
        job_id: JobId,
        admitted: bool,
        committed: bool,
    },
    // The reads for a batch of installs came back.
    Installs(Vec<crate::jobs::service::InstallEntry>),
    // A next-up job's article lists, read ahead.
    NextUp {
        job_id: JobId,
        read: u64,
        lists: Result<Vec<Vec<crate::jobs::model::SegmentSpec>>, String>,
    },
}

impl Pipeline {
    pub(crate) async fn handle_db_reply(&mut self, reply: DbReply) {
        match reply {
            DbReply::Slot {
                job_id,
                admitted,
                committed,
            } => self.slot_written(job_id, admitted, committed),
            DbReply::Installs(entries) => {
                self.install_batch_read(entries);
                // A job installed mid-move goes on with its move once its
                // batch is in place.
                for job_id in std::mem::take(&mut self.moves_to_resume) {
                    self.resume_restored_move(job_id).await;
                }
            }
            DbReply::NextUp {
                job_id,
                read,
                lists,
            } => self.next_up_read(job_id, read, lists),
        }
    }

    // Whether a reply the loop has not heard yet is out.
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn db_replies_out(&self) -> bool {
        !self.nzb_admitting.is_empty()
            || !self.nzb_releasing.is_empty()
            || !self.installing.is_empty()
    }
}
