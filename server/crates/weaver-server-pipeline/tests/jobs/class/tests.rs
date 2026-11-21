// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::bandwidth::ScheduleAction;
use crate::jobs::handle::{HoldOwner, HoldScope};
use crate::jobs::model::HeldClass;

// With no class seated, every payload a class owns is refused as not
// running, in the class's words, and may be taken later; what no class
// owns goes on the pipeline's path. Every intake (RSS, the watch
// folder, GraphQL's submit and staging) asks this one question.
#[test]
fn every_class_payload_is_refused_as_not_running_while_no_class_is_seated() {
    let classes = JobClasses::default();
    let magnet = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567".to_owned();
    for kind in [Kind::Metainfo, Kind::Magnet(magnet)] {
        let refusal = match classes.route(&kind) {
            Err(refusal) => refusal,
            Ok(_) => panic!("{kind:?} was not refused"),
        };
        assert_eq!(refusal.reason, RefusalReason::NotRunning);
        assert_eq!(refusal.text, "the torrent client is not running");
        assert!(refusal.transient());
    }
    let url = Kind::Url(reqwest::Url::parse("https://example.invalid/a.nzb").unwrap());
    for kind in [Kind::Nzb, url] {
        assert!(
            matches!(classes.route(&kind), Ok(Route::Pipeline)),
            "{kind:?}"
        );
    }
}

// A class seated with no intake refuses an add in its own words, for
// good: it is running, so "not running" would not be true.
#[test]
fn a_seated_class_with_no_intake_refuses_as_taking_no_jobs() {
    struct Quiet;
    #[async_trait::async_trait]
    impl JobClass for Quiet {
        fn protocol(&self) -> Protocol {
            Protocol::Torrent
        }
        async fn act(
            &self,
            actions: Vec<(JobId, QueueAction)>,
        ) -> BTreeMap<JobId, Result<(), SchedulerError>> {
            actions
                .into_iter()
                .map(|(job_id, _)| (job_id, Ok(())))
                .collect()
        }
    }
    let classes = JobClasses::default();
    classes.seat(Arc::new(Quiet));
    let refusal = match classes.route(&Kind::Metainfo) {
        Err(refusal) => refusal,
        Ok(_) => panic!("a class with no intake took an add"),
    };
    assert_eq!(refusal, AddRefusal::no_intake(Protocol::Torrent));
    assert_eq!(
        refusal.text,
        "the torrent client takes no new jobs from here"
    );
    assert!(!refusal.transient());
    assert!(matches!(classes.route(&Kind::Nzb), Ok(Route::Pipeline)));
}

// The words and keys each class owns, as they are shown and stored.
#[test]
fn each_class_owns_its_words_and_keys() {
    let nzb = Protocol::Nzb.names();
    assert_eq!(
        (nzb.key, nzb.runner, nzb.disabled_code),
        ("nzb", "download pipeline", "NZB_DISABLED")
    );
    assert!(nzb.hold.is_none());
    assert!(crate::jobs::model::live_of(Protocol::Nzb).is_none());
    assert_eq!(HeldClass::of(Protocol::Nzb), None);

    let torrent = Protocol::Torrent.names();
    assert_eq!(
        (torrent.key, torrent.runner, torrent.disabled_code),
        ("torrent", "torrent client", "TORRENTS_DISABLED")
    );
    let hold = torrent.hold.expect("torrents hold apart");
    assert_eq!(hold.scope, HoldScope::Everything);
    assert_eq!(hold.setting, "torrent_paused");
    assert!(crate::jobs::model::live_of(Protocol::Torrent).is_some());
    assert_eq!(
        HeldClass::of(Protocol::Torrent),
        Some(crate::torrents::CLASS)
    );
    assert_eq!(
        HeldClass::all().collect::<Vec<_>>(),
        vec![crate::torrents::CLASS]
    );
    assert_eq!(
        AddRefusal::not_running(Protocol::Torrent).text,
        "the torrent client is not running"
    );
    assert_eq!(
        HoldOwner::kept()
            .map(|(_, setting)| setting)
            .collect::<Vec<_>>(),
        vec!["global_paused", "torrent_paused"]
    );

    for (action, stored) in [
        (
            ScheduleAction::PauseClass {
                class: crate::torrents::CLASS,
            },
            r#"{"type":"pause_class","class":"torrent"}"#,
        ),
        (
            ScheduleAction::ResumeClass {
                class: crate::torrents::CLASS,
            },
            r#"{"type":"resume_class","class":"torrent"}"#,
        ),
    ] {
        assert_eq!(serde_json::to_string(&action).unwrap(), stored);
        assert_eq!(
            serde_json::from_str::<ScheduleAction>(stored).unwrap(),
            action
        );
    }
    // A pause of a class with no hold, or of no class, is never read in.
    for stored in [
        r#"{"type":"pause_class","class":"nzb"}"#,
        r#"{"type":"resume_class","class":"nzb"}"#,
        r#"{"type":"pause_class","class":"http"}"#,
    ] {
        assert!(
            serde_json::from_str::<ScheduleAction>(stored).is_err(),
            "{stored}"
        );
    }
}

// Every protocol's entry is at its index, and the index is its place in
// `ALL`, so a per-protocol table reads the right row.
#[test]
fn every_protocol_is_indexed_at_its_place_in_the_table() {
    for (at, protocol) in Protocol::ALL.iter().enumerate() {
        assert_eq!(protocol.index(), at);
    }
    let mut keys = ByProtocol::<&str>::default();
    for protocol in Protocol::ALL {
        keys[*protocol] = protocol.as_str();
    }
    for (protocol, key) in keys.iter() {
        assert_eq!(*key, protocol.as_str());
    }
}
