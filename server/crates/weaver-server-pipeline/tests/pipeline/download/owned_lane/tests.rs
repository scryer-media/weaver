// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// Two leases are on the ring at a lease boundary, so a response has to
// carry its own lease's identity, not whichever lease the lane happens to
// be filling from when it lands.
#[test]
fn a_result_is_attributed_to_the_lease_its_work_came_from() {
    let old = Arc::new(LaneLeaseContext::from_lease(
        &mut test_lease(
            JobId(7),
            3,
            weaver_nntp::ServerSet::from([1]),
            vec![tail_work(1, 0)],
        ),
        0,
        true,
    ));
    let new = Arc::new(LaneLeaseContext::from_lease(
        &mut test_lease(
            JobId(7),
            4,
            weaver_nntp::ServerSet::from([2, 5]),
            vec![tail_work(2, 0)],
        ),
        0,
        true,
    ));

    let observation = DownloadLaneObservation {
        server_idx: Some(0),
        mode: DownloadLaneMode::Pipelined { depth: 4 },
        supports_pipelining: true,
        latency_sample: None,
        cold: false,
        transfer: None,
        payload_bytes: 0,
        policy_elapsed: std::time::Duration::ZERO,
        pressure_clear: true,
        fed: false,
        batch_complete: false,
        batch_clean: true,
        unresolved_count: 0,
        connection_discarded: false,
        answered_pipelined: false,
    };
    let straggler = result_from_trace(
        tail_work(1, 0),
        old.runtime_generation,
        0,
        JobId(42),
        weaver_nntp::client::DecodedBodyTrace {
            attempts: Vec::new(),
            result: Err(weaver_nntp::client::DecodedBodyError::Nntp(
                weaver_nntp::error::NntpError::ArticleNotFound,
            )),
        },
        observation,
        old.retention_excludes,
        false,
    );

    assert_eq!(
        straggler.runtime_generation, 3,
        "the tail of the old lease keeps the old generation while the new \
             lease is already on the wire"
    );
    assert_eq!(
        straggler.exclude_servers,
        vec![1],
        "the work carried no failure ledger, so the result reports the \
             lease's retention exclusions alone"
    );
    assert_eq!(new.runtime_generation, 4);
    assert_eq!(new.retention_excludes, vec![2, 5]);
    assert_eq!(new.depth(), 4);
}

// A lane's batch is cut for one job, not for one exclusion set, so the
// result has to report the article's own failure ledger. Reporting the
// lease's set instead loses every server this article has already been
// refused by, and an article missing on all of them then retries forever
// because its exclusion set never grows past the server that just
// answered.
#[test]
fn a_result_reports_the_article_s_own_failure_ledger() {
    let context = Arc::new(LaneLeaseContext::from_lease(
        &mut test_lease(
            JobId(7),
            3,
            weaver_nntp::ServerSet::EMPTY,
            vec![tail_work(1, 0)],
        ),
        1,
        true,
    ));
    let mut work = tail_work(1, 1);
    work.exclude_servers = weaver_nntp::ServerSet::from([0]);

    let result = result_from_trace(
        work,
        context.runtime_generation,
        0,
        JobId(7),
        weaver_nntp::client::DecodedBodyTrace {
            attempts: Vec::new(),
            result: Err(weaver_nntp::client::DecodedBodyError::Nntp(
                weaver_nntp::error::NntpError::ArticleNotFound,
            )),
        },
        DownloadLaneObservation {
            server_idx: Some(1),
            mode: DownloadLaneMode::Sequential,
            supports_pipelining: false,
            latency_sample: None,
            cold: false,
            transfer: None,
            payload_bytes: 0,
            policy_elapsed: std::time::Duration::ZERO,
            pressure_clear: true,
            fed: false,
            batch_complete: false,
            batch_clean: true,
            unresolved_count: 0,
            connection_discarded: false,
            answered_pipelined: false,
        },
        context.retention_excludes,
        false,
    );

    assert_eq!(
        result.exclude_servers,
        vec![0],
        "the server this article already failed on must survive the trip \
             back, or the completion path cannot tell that both servers are out"
    );
}

// The result carries the union of the article's ledger and the lease's
// retention exclusions.
#[test]
fn a_result_carries_the_ledger_and_the_retention_exclusions() {
    let unresolved = |work: DownloadWork, retention: weaver_nntp::ServerSet| {
        unresolved_result(
            work,
            3,
            0,
            JobId(7),
            1,
            DownloadLaneMode::Sequential,
            false,
            None,
            true,
            1,
            false,
            retention,
            "test",
        )
    };

    let mut work = tail_work(1, 1);
    work.exclude_servers = weaver_nntp::ServerSet::from([3, 0]);
    let result = unresolved(work, weaver_nntp::ServerSet::from([0, 2, 3, 5]));
    assert_eq!(result.exclude_servers, [0, 2, 3, 5]);

    let mut work = tail_work(2, 1);
    work.exclude_servers = weaver_nntp::ServerSet::from([4]);
    let result = unresolved(work, weaver_nntp::ServerSet::EMPTY);
    assert_eq!(result.exclude_servers, [4]);

    let result = unresolved(tail_work(3, 0), weaver_nntp::ServerSet::from([1]));
    assert_eq!(result.exclude_servers, [1]);
}

// The context takes the lease's retention exclusions and each file's
// checkpoint plan; a file the lease holds nothing of has none.
#[test]
fn a_lease_context_takes_the_lease_s_exclusions() {
    let mut lease = test_lease(
        JobId(7),
        3,
        weaver_nntp::ServerSet::from([2, 5]),
        vec![tail_work(1, 0)],
    );
    let file = lease.works[0].segment_id.file_id.file_index;
    lease.checkpoint_plans.insert(
        file,
        weaver_yenc::CheckpointPlan::Single(std::num::NonZeroU64::new(4096).unwrap()),
    );
    let context = LaneLeaseContext::from_lease(&mut lease, 0, true);
    assert_eq!(context.retention_excludes, [2, 5]);
    assert_eq!(
        *context.checkpoint_plans.of(file),
        weaver_yenc::CheckpointPlan::Single(std::num::NonZeroU64::new(4096).unwrap())
    );
    assert_eq!(
        *context.checkpoint_plans.of(file + 1),
        weaver_yenc::CheckpointPlan::None
    );
    assert_eq!(lease.dial_exclude_servers, [2, 5]);
    assert_eq!(lease.works.len(), 1);
}

// Per-article delivery is the point: decode starts on a lease's first
// article instead of its last. Only the lane's closing event waits for an
// acknowledgement, because that is what orders the results ahead of the
// park message on the other channel.
// The articles abandoned behind a fault were never answered: they keep
// their retry budget and go back out on another connection.
#[test]
fn an_abandoned_article_is_unanswered() {
    let result = unresolved_result(
        tail_work(0, 2),
        1,
        0,
        JobId(7),
        0,
        DownloadLaneMode::Sequential,
        true,
        None,
        true,
        3,
        false,
        weaver_nntp::ServerSet::EMPTY,
        "lane pipeline faulted before this article's response",
    );
    let Err(DownloadError::Fetch(failure)) = &result.data else {
        panic!("an abandoned article is a fetch failure");
    };
    assert_eq!(failure.kind, DownloadFailureKind::Unanswered);
    assert!(failure.kind.preserves_article_retry_budget());
    assert_eq!(result.retry_count, 2);
    // The kind says nothing about the socket; the abandoned result
    // carries the lane's own verdict that the connection was lost.
    let observation = result.lane_observation.unwrap();
    assert!(observation.connection_discarded);
    assert!(!observation.batch_clean);
}

#[tokio::test]
async fn results_stream_one_article_at_a_time_and_only_the_last_waits() {
    let (event_tx, mut event_rx) = mpsc::channel(8);
    let sender_tx = event_tx.clone();
    let sender = tokio::task::spawn_blocking(move || {
        let stats = weaver_nntp::blocking::BlockingLaneStats::default();
        for segment_number in 0..3u32 {
            let result = unresolved_result(
                tail_work(segment_number, 0),
                9,
                0,
                JobId(42),
                0,
                DownloadLaneMode::Sequential,
                false,
                None,
                true,
                0,
                false,
                weaver_nntp::ServerSet::EMPTY,
                "streamed",
            );
            send_owned_batch(&sender_tx, 0, vec![result], Vec::new(), stats, false)?;
        }
        send_owned_batch(
            &sender_tx,
            0,
            Vec::new(),
            vec![tail_work(9, 0)],
            stats,
            true,
        )
    });

    for segment_number in 0..3u32 {
        let event = event_rx.recv().await.expect("a streamed article");
        let OwnedDownloadLaneEvent::BatchComplete { results, ack, .. } = event else {
            panic!("streamed events are batch completions");
        };
        assert_eq!(results.len(), 1, "one article per event");
        assert_eq!(results[0].segment_id.segment_number, segment_number);
        assert!(
            ack.is_none(),
            "a streamed article must not cost a thread hop on the hot path"
        );
    }

    let event = event_rx.recv().await.expect("the closing event");
    let OwnedDownloadLaneEvent::BatchComplete {
        results,
        unrequested_works,
        ack,
        ..
    } = event
    else {
        panic!("the closing event is a batch completion");
    };
    assert!(results.is_empty());
    assert_eq!(unrequested_works.len(), 1);
    let ack = ack.expect("the closing event is the ordering barrier");
    assert!(
        !sender.is_finished(),
        "the lane is still waiting on the barrier"
    );
    ack.send(()).unwrap();
    sender.await.unwrap().unwrap();
}

#[test]
fn leased_message_ids_are_bracketed_for_the_wire() {
    // Both lanes build their BODY arguments here. `DownloadWork` stores
    // the bare id, and a bare BODY argument is an article-*number*
    // reference that every real provider answers with 430.
    let works = [tail_work(1, 0), tail_work(2, 0)];

    let wire = lease_message_id_wire_forms(&works);

    assert_eq!(
        wire,
        vec![
            "<tail-1@example.invalid>".to_string(),
            "<tail-2@example.invalid>".to_string(),
        ]
    );
    assert!(
        !works[0].message_id.0.starts_with('<'),
        "the stored id is bare, which is exactly why the wire form is needed"
    );
}

#[test]
fn owned_quota_park_keeps_the_cached_lane() {
    // A quota refusal never reaches the wire, so the socket is clean and
    // the lane is worth keeping for the next fill.
    let (reason, keep_cached_lane) =
        lane_stop_park(Some(LaneStop::PolicyBlocked)).expect("a stop always parks");

    assert_eq!(reason, LaneParkReason::ServerQuota);
    assert!(keep_cached_lane);
}

#[test]
fn owned_transport_fault_parks_as_error_and_drops_the_connection() {
    for stop in [LaneStop::ConnectionLost, LaneStop::Error] {
        let (reason, keep_cached_lane) = lane_stop_park(Some(stop)).expect("a stop always parks");

        assert_eq!(reason, LaneParkReason::Error);
        assert!(!keep_cached_lane);
    }
    assert!(lane_stop_park(None).is_none());
}

// The refill must be asked for while the ring still has runway.
//
// An ordinary lease is exactly one pipe deep, so the deadline has to be
// wider than the lease itself or the ask would land after the pipe is
// already dry — the round-trip gap this path exists to remove.
#[test]
fn refill_deadline_leaves_runway_at_every_rung() {
    assert_eq!(refill_deadline(1), 3);
    assert_eq!(refill_deadline(2), 5);
    assert_eq!(refill_deadline(4), 9);
    assert_eq!(refill_deadline(8), 17);
    // A zero depth is a Sequential lane, not a stalled one.
    assert_eq!(refill_deadline(0), refill_deadline(1));

    for depth in [1usize, 2, 4, 8] {
        assert!(
            refill_deadline(depth) > depth,
            "a one-pipe lease must trip the deadline as soon as it is adopted"
        );
    }
}

// Work no longer current is never dialled for and never asked again:
// contention retries a dial, not a lease that has gone. Work that is
// current and meets contention is dialled again, a bounded number of
// times, and the last answer stands.
#[test]
fn contention_retries_a_dial_but_never_an_obsolete_lease() {
    let dials = std::cell::Cell::new(0);
    let outcome = acquire_owned_lane_through_contention(
        || false,
        || {
            dials.set(dials.get() + 1);
            Err(weaver_nntp::client::BlockingBodyLaneAcquireError::SelectionContended)
        },
    );
    assert!(matches!(
        outcome,
        Err(weaver_nntp::client::BlockingBodyLaneAcquireError::SelectionContended)
    ));
    assert_eq!(dials.get(), 0, "an obsolete lease is never dialled for");

    let outcome = acquire_owned_lane_through_contention(
        || true,
        || {
            dials.set(dials.get() + 1);
            Err(weaver_nntp::client::BlockingBodyLaneAcquireError::SelectionContended)
        },
    );
    assert!(matches!(
        outcome,
        Err(weaver_nntp::client::BlockingBodyLaneAcquireError::SelectionContended)
    ));
    assert_eq!(dials.get(), OWNED_LANE_SELECTION_CONTENTION_RETRIES + 1);

    let mut current = [true, false].into_iter();
    dials.set(0);
    let outcome = acquire_owned_lane_through_contention(
        || current.next().unwrap_or(false),
        || {
            dials.set(dials.get() + 1);
            Err(weaver_nntp::client::BlockingBodyLaneAcquireError::SelectionContended)
        },
    );
    assert!(outcome.is_err());
    assert_eq!(
        dials.get(),
        1,
        "a lease that goes mid-retry is not dialled again"
    );
}

// A 430 must not tear down the owned lane.
//
// The article-not-found answer is a complete, bodyless server response:
// the socket is exactly where the next BODY expects it. Marking it dirty
// used to QUIT the TLS session, drop the rest of the leased batch as
// Unrequested, and block the server's pipelining proof — all because one
// article lives on another provider.
#[test]
fn owned_article_not_found_keeps_the_lane_and_the_batch_clean() {
    let result = result_from_trace(
        tail_work(9, 0),
        0,
        0,
        JobId(42),
        weaver_nntp::client::DecodedBodyTrace {
            attempts: vec![weaver_nntp::client::FetchAttemptTrace {
                route_feedback: None,
                connection_health: None,
                server_idx: 0,
                remote_ip: None,
                elapsed: Duration::from_millis(3),
                outcome: weaver_nntp::client::FetchAttemptOutcome::NotFound,
                error: Some("article not found".to_string()),
            }],
            result: Err(weaver_nntp::client::DecodedBodyError::Nntp(
                weaver_nntp::NntpError::NoSuchArticle {
                    message_id: "<tail-9@example.invalid>".to_string(),
                },
            )),
        },
        DownloadLaneObservation {
            server_idx: Some(0),
            mode: DownloadLaneMode::Pipelined { depth: 4 },
            supports_pipelining: true,
            latency_sample: None,
            cold: false,
            transfer: None,
            payload_bytes: 0,
            policy_elapsed: Duration::ZERO,
            pressure_clear: true,
            fed: false,
            batch_complete: true,
            batch_clean: true,
            unresolved_count: 0,
            connection_discarded: false,
            answered_pipelined: false,
        },
        weaver_nntp::ServerSet::EMPTY,
        false,
    );

    // The work item itself still fails, so the completion path excludes
    // this server and retries the article elsewhere.
    assert!(matches!(
        result.data,
        Err(DownloadError::Fetch(DownloadFailure {
            kind: DownloadFailureKind::ArticleNotFound,
            ..
        }))
    ));
    // …but the lane keeps its cached connection and keeps refilling.
    let observation = result.lane_observation.unwrap();
    assert!(observation.batch_clean);
    assert!(!observation.connection_discarded);
    assert!(download_outcome_keeps_connection(&result.data));

    // A clean batch is what keeps the rung: only the unclean path drops
    // one, and a 430 must not reach it.
    let mut explorer = crate::pipeline::download::transport::ServerPipelineExplorer::seeded(
        Some(observation.mode.depth()),
        None,
        None,
    );
    assert_eq!(explorer.current_depth(), 4);
    assert_eq!(
        explorer.note_unclean_batch(Instant::now()),
        Some(crate::pipeline::download::transport::RungChange::Dropped { from: 4, to: 2 }),
        "the unclean path is the only one that costs a rung"
    );

    // The rest of the lease is still requested, not handed back: a 430
    // sets no stop, so the ring keeps being topped up from `pending`.
    assert!(lane_stop_park(None).is_none());
}

// Transport faults must stay dirty. Only the "fully consumed response"
// outcomes are allowed to keep the connection.
#[test]
fn owned_transport_and_decode_failures_still_discard_the_connection() {
    for data in [
        Err::<DownloadPayload, _>(DownloadError::Fetch(DownloadFailure::new(
            DownloadFailureKind::EstablishedTransport,
            "connection closed mid-body",
        ))),
        Err(DownloadError::Decode {
            raw_size: 128,
            error: "invalid yEnc header".to_string(),
        }),
    ] {
        assert!(
            !download_outcome_keeps_connection(&data),
            "a possibly mid-body socket must not be reused"
        );
    }
}

#[test]
fn owned_quota_result_keeps_connection_observation_clean() {
    let transfers = weaver_nntp::transfer::ServerTransferRegistry::new();
    let control = transfers.configure(
        weaver_nntp::transfer::StableServerId(77),
        weaver_nntp::transfer::ServerTransferConfig {
            closed: false,
            rate_bytes_per_sec: 0,
            quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
                limit_bytes: 1,
                generation: 1,
                retry_at: Some(Instant::now() + Duration::from_secs(30)),
            }),
        },
    );
    let _reservation = control.try_reserve(1).unwrap();
    let rejection = control.try_reserve(1).err().unwrap();
    let result = result_from_trace(
        tail_work(6, 0),
        0,
        0,
        JobId(42),
        weaver_nntp::client::DecodedBodyTrace {
            attempts: Vec::new(),
            result: Err(weaver_nntp::client::DecodedBodyError::Nntp(
                weaver_nntp::NntpError::quota_blocked(rejection),
            )),
        },
        DownloadLaneObservation {
            server_idx: Some(0),
            mode: DownloadLaneMode::Sequential,
            supports_pipelining: true,
            latency_sample: None,
            cold: false,
            transfer: None,
            payload_bytes: 0,
            policy_elapsed: Duration::ZERO,
            pressure_clear: true,
            fed: false,
            batch_complete: true,
            batch_clean: true,
            unresolved_count: 0,
            connection_discarded: false,
            answered_pipelined: false,
        },
        weaver_nntp::ServerSet::EMPTY,
        false,
    );

    assert!(matches!(
        result.data,
        Err(DownloadError::Fetch(DownloadFailure {
            kind: DownloadFailureKind::ServerQuota,
            ..
        }))
    ));
    let observation = result.lane_observation.unwrap();
    assert!(observation.batch_clean);
    assert!(!observation.connection_discarded);
    assert_eq!(observation.unresolved_count, 0);
}

fn group_refusal_result(work: DownloadWork, group_selected: bool) -> DownloadFailure {
    let result = result_from_trace(
        work,
        0,
        0,
        JobId(42),
        weaver_nntp::client::DecodedBodyTrace {
            attempts: Vec::new(),
            result: Err(weaver_nntp::client::DecodedBodyError::Nntp(
                weaver_nntp::NntpError::NoGroupSelected,
            )),
        },
        DownloadLaneObservation {
            server_idx: Some(0),
            mode: DownloadLaneMode::Sequential,
            supports_pipelining: true,
            latency_sample: None,
            cold: false,
            transfer: None,
            payload_bytes: 0,
            policy_elapsed: Duration::ZERO,
            pressure_clear: true,
            fed: false,
            batch_complete: true,
            batch_clean: true,
            unresolved_count: 0,
            connection_discarded: false,
            answered_pipelined: false,
        },
        weaver_nntp::ServerSet::EMPTY,
        group_selected,
    );
    let observation = result.lane_observation.unwrap();
    assert!(
        observation.connection_discarded,
        "a 412 still drops the socket"
    );
    match result.data {
        Err(DownloadError::Fetch(failure)) => {
            assert_eq!(failure.kind, DownloadFailureKind::ConnectionEstablishment);
            failure
        }
        _ => panic!("a 412 is a fetch failure"),
    }
}

// The first 412 a server gives teaches it needs GROUP; the next
// connection selects one of the work's groups, so it costs the article
// nothing. A 412 with no group to select, or one given while a group was
// selected, is answered the same way every time, so it costs a retry.
#[test]
fn a_412_is_free_only_while_the_next_connection_can_select_a_group() {
    let learning = group_refusal_result(tail_work(1, 0), false);
    assert_eq!(learning.group_refusal, GroupRefusal::Learning);
    assert!(learning.preserves_article_retry_budget());

    let selected = group_refusal_result(tail_work(1, 0), true);
    assert_eq!(selected.group_refusal, GroupRefusal::Settled);
    assert!(!selected.preserves_article_retry_budget());

    for group_selected in [false, true] {
        let mut work = tail_work(1, 0);
        work.groups = Arc::from(Vec::<String>::new());
        let no_group = group_refusal_result(work, group_selected);
        assert_eq!(no_group.group_refusal, GroupRefusal::Settled);
        assert!(!no_group.preserves_article_retry_budget());
    }
}

// Settling looks only at 412s: every other failure keeps its kind's
// retry-budget verdict whatever the work's groups.
#[test]
fn settling_a_group_refusal_leaves_other_failures_alone() {
    for error in [
        weaver_nntp::NntpError::Timeout,
        weaver_nntp::NntpError::ConnectionClosed,
        weaver_nntp::NntpError::ArticleNotFound,
        weaver_nntp::NntpError::AuthenticationFailed,
    ] {
        let mut failure = DownloadFailure::from_nntp(error);
        let before = failure.preserves_article_retry_budget();
        failure.settle_group_refusal(true, true);
        assert_eq!(failure.group_refusal, GroupRefusal::None);
        assert_eq!(failure.preserves_article_retry_budget(), before);
    }
}

// The requirement is read per server position, before a dial picks one.
#[test]
fn servers_requiring_group_marks_only_the_servers_that_proved_it() {
    let server = |host: &str| weaver_nntp::ServerPoolConfig {
        max_connections: 1,
        ..weaver_nntp::ServerPoolConfig::new(weaver_nntp::ServerConfig {
            host: host.to_string(),
            port: 1,
            tls: false,
            ..Default::default()
        })
    };
    let nntp = weaver_nntp::client::NntpClient::new(weaver_nntp::client::NntpClientConfig {
        servers: vec![
            server("free-fetch.owned-lane.invalid"),
            server("group-required.owned-lane.invalid"),
        ],
        ..weaver_nntp::client::NntpClientConfig::single(weaver_nntp::ServerConfig::default(), 1)
    });
    assert_eq!(
        servers_requiring_group(&nntp),
        weaver_nntp::ServerSet::EMPTY
    );
    assert!(!server_requires_group(&nntp, 1));

    weaver_nntp::server_caps::note_group_required("group-required.owned-lane.invalid", 1);

    assert_eq!(servers_requiring_group(&nntp), [1]);
    assert!(!server_requires_group(&nntp, 0));
    assert!(server_requires_group(&nntp, 1));
    assert!(
        !server_requires_group(&nntp, 2),
        "past the pool reads as not required"
    );
}

#[test]
fn owned_quota_cache_survives_wait_and_reset_but_reselects_another_fill() {
    let transfers = weaver_nntp::transfer::ServerTransferRegistry::new();
    let control = transfers.configure(
        weaver_nntp::transfer::StableServerId(78),
        weaver_nntp::transfer::ServerTransferConfig {
            closed: false,
            rate_bytes_per_sec: 0,
            quota: Some(weaver_nntp::transfer::QuotaRuntimeConfig {
                limit_bytes: 1,
                generation: 1,
                retry_at: Some(Instant::now() + Duration::from_secs(30)),
            }),
        },
    );
    let _reservation = control.try_reserve(1).unwrap();
    let rejection = control.try_reserve(1).err().unwrap();
    let cached_server = weaver_nntp::pool::ServerId(0);

    let all_blocked = weaver_nntp::client::BodyServerSelection {
        eligible: Vec::new(),
        quota_blocked: Some(rejection.clone()),
    };
    assert!(keep_cached_lane_after_park(LaneParkReason::ServerQuota));
    assert!(cached_lane_matches_selection(
        cached_server,
        true,
        &all_blocked
    ));

    let after_reset = weaver_nntp::client::BodyServerSelection {
        eligible: vec![cached_server],
        quota_blocked: None,
    };
    assert!(cached_lane_matches_selection(
        cached_server,
        false,
        &after_reset
    ));

    let alternate_fill = weaver_nntp::client::BodyServerSelection {
        eligible: vec![weaver_nntp::pool::ServerId(1)],
        quota_blocked: Some(rejection),
    };
    assert!(!cached_lane_matches_selection(
        cached_server,
        true,
        &alternate_fill
    ));
}
