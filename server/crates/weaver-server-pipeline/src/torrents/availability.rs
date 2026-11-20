// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The torrent class of a server without torrents, seated on the
//! scheduler. Whether this run has torrents at all is the composition
//! crate's [`TorrentAvailability`].

pub use weaver_server_composition::torrents::availability::*;

use crate::SchedulerError;
use crate::jobs::JobId;
use crate::jobs::class::{AddRefusal, ClassIntake, JobClass, RefusalReason, Submission};
use crate::jobs::handle::{QueueAction, SchedulerHandle};
use crate::jobs::model::Protocol;
use std::collections::BTreeMap;
use std::sync::Arc;

impl SeatUnavailableTorrents for SchedulerHandle {
    fn seat_unavailable_torrents(&self) {
        self.state().classes().seat(Arc::new(Unavailable));
    }
}

// The torrents of a server that has none: no job is theirs, and an add is
// refused in words that name the variable.
pub struct Unavailable;

fn unavailable() -> AddRefusal {
    AddRefusal {
        reason: RefusalReason::Unavailable,
        text: TORRENT_ADD_UNAVAILABLE.to_owned(),
    }
}

#[async_trait::async_trait]
impl JobClass for Unavailable {
    fn protocol(&self) -> Protocol {
        Protocol::Torrent
    }

    async fn act(
        &self,
        actions: Vec<(JobId, QueueAction)>,
    ) -> BTreeMap<JobId, Result<(), SchedulerError>> {
        actions
            .into_iter()
            .map(|(job_id, _)| (job_id, Err(SchedulerError::JobNotFound(job_id))))
            .collect()
    }

    fn intake(&self) -> Option<&dyn ClassIntake> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl ClassIntake for Unavailable {
    fn takes_new(&self) -> Result<(), AddRefusal> {
        Err(unavailable())
    }

    async fn add(&self, _submission: Submission) -> Result<JobId, AddRefusal> {
        Err(unavailable())
    }
}
