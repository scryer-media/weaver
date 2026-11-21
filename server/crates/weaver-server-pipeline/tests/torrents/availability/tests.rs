// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

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
// torrent by the variable's name. Available, it is started and nothing is seated in its
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
        Ok(Route::Class(intake)) => intake.takes_new().expect_err("unavailable"),
        _ => panic!("a torrent goes to the class of a server without torrents"),
    };
    assert_eq!(refused.reason, RefusalReason::Unavailable);
    assert!(refused.transient());
    assert_eq!(refused.text, TORRENT_ADD_UNAVAILABLE);
    assert!(refused.text.contains(TORRENTS_ENV));

    let queue = self::queue();
    let client = TorrentAvailability::AVAILABLE
        .client(&queue, || std::future::ready(Some(7)))
        .await;
    assert_eq!(client, Some(7));
    // The client seats itself once it runs; nothing stands in for it.
    assert!(queue.state().classes().get(Protocol::Torrent).is_none());
}

// An unavailable server's torrent class refuses every intake by the
// variable's name, as something to hold, not as a fault of the payload.
#[tokio::test]
async fn an_unavailable_servers_torrent_adds_are_refused_by_the_variables_name() {
    let classes = JobClasses::default();
    classes.seat(Arc::new(Unavailable));
    let Ok(Route::Class(intake)) = classes.route(&Kind::Metainfo) else {
        panic!("a torrent goes to the torrent class");
    };
    let refused = intake.takes_new().expect_err("unavailable");
    assert_eq!(refused.reason, RefusalReason::Unavailable);
    assert!(refused.transient());
    assert_eq!(refused.text, TORRENT_ADD_UNAVAILABLE);
    assert!(refused.text.contains(TORRENTS_ENV));
    let refused = intake
        .add(Submission::of(
            Kind::Metainfo,
            b"d4:infod4:name1:aee".to_vec(),
        ))
        .await
        .expect_err("unavailable");
    assert_eq!(refused.reason, RefusalReason::Unavailable);
    assert!(refused.transient());
    assert_eq!(refused.text, TORRENT_ADD_UNAVAILABLE);
    assert!(refused.text.contains(TORRENTS_ENV));
    // No job is its own.
    let class = classes.get(Protocol::Torrent).expect("seated");
    let acted = class.act(vec![(JobId(7), QueueAction::Pause)]).await;
    assert!(matches!(
        acted.get(&JobId(7)),
        Some(Err(SchedulerError::JobNotFound(JobId(7))))
    ));
}
