// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Whether this run of the server has torrents at all.
//!
//! Torrents take two opt-ins. The first is the environment variable
//! [`TORRENTS_ENV`], read once at start by the app and handed down as a
//! [`TorrentAvailability`]; nothing below the app reads the environment for
//! it. The second is the saved `enabled` setting, which only counts while
//! the first is given.
//!
//! Unavailable, the torrent client is never built: no session, listener,
//! DHT, tracker or peer traffic, no port mapping and no load of the stored
//! torrents. Their rows, settings and files stay as they are, so giving the
//! variable later brings everything back as saved.

use crate::SchedulerError;
use crate::jobs::JobId;
use crate::jobs::class::{AddRefusal, ClassIntake, JobClass, RefusalReason, Submission};
use crate::jobs::handle::{QueueAction, SchedulerHandle};
use crate::jobs::model::Protocol;
use std::collections::BTreeMap;
use std::sync::Arc;

/// The environment variable that makes torrents available.
pub const TORRENTS_ENV: &str = "WEAVER_TORRENTS";

/// The refusal of turning torrents on while they are unavailable.
pub const TORRENTS_UNAVAILABLE: &str = "torrents are not available on this server: set the environment variable WEAVER_TORRENTS=1 and restart Weaver, then turn them on";

/// Whether this run of the server has torrents at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TorrentAvailability(bool);

impl TorrentAvailability {
    pub const AVAILABLE: Self = Self(true);
    pub const UNAVAILABLE: Self = Self(false);

    pub const fn of(available: bool) -> Self {
        Self(available)
    }

    /// As the value of [`TORRENTS_ENV`] says: available for `1` or `true`,
    /// unavailable unset or for anything else.
    pub fn of_env_value(value: Option<&str>) -> Self {
        Self(matches!(value, Some("1" | "true")))
    }

    pub const fn available(self) -> bool {
        self.0
    }

    /// Whether the saved setting may be turned on: refused, naming the
    /// variable, while torrents are unavailable.
    pub fn may_turn_on(self) -> Result<(), &'static str> {
        if self.0 {
            Ok(())
        } else {
            Err(TORRENTS_UNAVAILABLE)
        }
    }

    /// Whether torrents are on: available, and turned on by the saved
    /// setting `enabled`.
    pub const fn on(self, enabled: bool) -> bool {
        self.0 && enabled
    }

    /// Seat the torrent class of an unavailable server in `queue`: every
    /// intake's torrent is refused as it is while the settings turn
    /// torrents off. Nothing is seated while torrents are available; the
    /// client seats itself once it runs.
    pub fn seat(self, queue: &SchedulerHandle) {
        if !self.0 {
            queue.state().classes().seat(Arc::new(Unavailable));
        }
    }

    /// The torrent client of this run: what `start` builds while torrents
    /// are available. Unavailable, `start` is never called, so nothing of
    /// the client opens, listens or loads, and `queue` refuses a torrent as
    /// it does while the settings turn torrents off.
    pub async fn client<T, F>(self, queue: &SchedulerHandle, start: impl FnOnce() -> F) -> Option<T>
    where
        F: Future<Output = Option<T>>,
    {
        if self.0 {
            start().await
        } else {
            self.seat(queue);
            None
        }
    }
}

// The torrents of a server that has none: no job is theirs, and an add is
// refused as it is while torrents are off.
struct Unavailable;

fn off() -> AddRefusal {
    AddRefusal {
        reason: RefusalReason::Off,
        text: super::status::TORRENTS_OFF.to_owned(),
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
        Err(off())
    }

    async fn add(&self, _submission: Submission) -> Result<JobId, AddRefusal> {
        Err(off())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::Kind;
    use crate::jobs::class::{JobClasses, Route};

    #[test]
    fn only_one_or_true_makes_torrents_available() {
        for value in [Some("1"), Some("true")] {
            assert!(TorrentAvailability::of_env_value(value).available());
        }
        for value in [
            None,
            Some(""),
            Some("0"),
            Some("false"),
            Some("TRUE"),
            Some("yes"),
            Some(" 1"),
        ] {
            assert!(!TorrentAvailability::of_env_value(value).available());
        }
    }

    #[test]
    fn turning_torrents_on_is_refused_by_the_variables_name_while_unavailable() {
        assert_eq!(TorrentAvailability::AVAILABLE.may_turn_on(), Ok(()));
        let refusal = TorrentAvailability::UNAVAILABLE
            .may_turn_on()
            .expect_err("unavailable");
        assert!(refusal.contains(TORRENTS_ENV));
        assert!(TorrentAvailability::AVAILABLE.on(true));
        assert!(!TorrentAvailability::AVAILABLE.on(false));
        assert!(!TorrentAvailability::UNAVAILABLE.on(true));
    }

    fn queue() -> SchedulerHandle {
        let state = crate::jobs::SharedPipelineState::new(crate::PipelineMetrics::new(), vec![]);
        let (event_tx, _) = tokio::sync::broadcast::channel(16);
        SchedulerHandle::new(event_tx, state)
    }

    // Unavailable, the client is never started and the queue refuses a
    // torrent as off. Available, it is started and nothing is seated in its
    // place.
    #[tokio::test]
    async fn the_client_is_started_only_while_torrents_are_available() {
        let queue = queue();
        let mut started = false;
        let client = TorrentAvailability::UNAVAILABLE
            .client(&queue, || {
                started = true;
                std::future::ready(Some(()))
            })
            .await;
        assert_eq!(client, None);
        assert!(!started, "nothing of the client is built");
        let refused = match queue.state().classes().route(&Kind::Metainfo) {
            Ok(Route::Class(intake)) => intake.takes_new().expect_err("off"),
            _ => panic!("a torrent goes to the class of a server without torrents"),
        };
        assert_eq!(refused.reason, RefusalReason::Off);
        assert_eq!(refused.text, crate::torrents::status::TORRENTS_OFF);

        let queue = self::queue();
        let client = TorrentAvailability::AVAILABLE
            .client(&queue, || std::future::ready(Some(7)))
            .await;
        assert_eq!(client, Some(7));
        // The client seats itself once it runs; nothing stands in for it.
        assert!(queue.state().classes().get(Protocol::Torrent).is_none());
    }

    // An unavailable server's torrent class answers every intake as the
    // client does while the settings turn torrents off.
    #[tokio::test]
    async fn an_unavailable_servers_torrent_adds_are_refused_as_off() {
        let classes = JobClasses::default();
        classes.seat(Arc::new(Unavailable));
        let Ok(Route::Class(intake)) = classes.route(&Kind::Metainfo) else {
            panic!("a torrent goes to the torrent class");
        };
        let refused = intake.takes_new().expect_err("off");
        assert_eq!(refused.reason, RefusalReason::Off);
        assert_eq!(refused.text, crate::torrents::status::TORRENTS_OFF);
        let refused = intake
            .add(Submission::of(
                Kind::Metainfo,
                b"d4:infod4:name1:aee".to_vec(),
            ))
            .await
            .expect_err("off");
        assert_eq!(refused.reason, RefusalReason::Off);
        assert_eq!(refused.text, crate::torrents::status::TORRENTS_OFF);
        // No job is its own.
        let class = classes.get(Protocol::Torrent).expect("seated");
        let acted = class.act(vec![(JobId(7), QueueAction::Pause)]).await;
        assert!(matches!(
            acted.get(&JobId(7)),
            Some(Err(SchedulerError::JobNotFound(JobId(7))))
        ));
    }
}
