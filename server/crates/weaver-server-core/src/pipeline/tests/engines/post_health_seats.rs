// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A health probe's STAT batches are lane work. Each batch waits for a free
// seat on a server it has not asked, the seat an article lease would take,
// and is asked on that lane between leases. Here the post is a release
// posted as its first articles alone, and its one server is a listener this
// test owns that holds exactly those: the batches go to it over the download
// lanes, and what they find rules the post dead.

use std::collections::BTreeSet;
use std::io::{BufRead, Write};
use std::sync::{Arc, Mutex};

use super::harness::{ARTICLE, JOB, Listener, Post, Scenario, admit_with, payload, serve};
use crate::pipeline::Pipeline;
use crate::pipeline::tests::hear_graph_mail;

// Three files of `articles` articles each, every article but each file's
// first gone.
fn first_articles_alone(articles: u32) -> Scenario {
    let posts: Vec<Post> = (0..3u8)
        .map(|at| {
            Post::plain(
                &format!("lantern{at:02}.bin"),
                payload(articles as usize * ARTICLE, at),
            )
        })
        .collect();
    (0..3u32)
        .flat_map(|file| (1..articles).map(move |ordinal| (file, ordinal)))
        .fold(Scenario::new(posts), |scenario, (file, ordinal)| {
            scenario.lost(file, ordinal)
        })
}

// A message id without its brackets.
fn bare(message_id: &str) -> &str {
    message_id.trim_matches(['<', '>'])
}

// Whether the server holds the article: only a file's first is posted.
fn held(message_id: &str) -> bool {
    bare(message_id)
        .split('@')
        .next()
        .and_then(|local| local.rsplit('-').next())
        == Some("0")
}

// A server on this machine that holds every file's first article and
// nothing else, answering STAT and HEAD on each connection it is dialed,
// for as long as the test holds it. Every STAT it is asked is kept, in
// order. Asked a STAT for an article in `silent`, it answers nothing and
// closes the connection.
struct StatServer {
    listener: Listener,
    asked: Arc<Mutex<Vec<String>>>,
    silent: Arc<Mutex<BTreeSet<String>>>,
}

fn stat_server() -> StatServer {
    let asked = Arc::new(Mutex::new(Vec::new()));
    let silent = Arc::new(Mutex::new(BTreeSet::new()));
    let (recorder, unanswered) = (Arc::clone(&asked), Arc::clone(&silent));
    StatServer {
        listener: Listener::serve(move |socket| answer(socket, &recorder, &unanswered)),
        asked,
        silent,
    }
}

fn answer(
    socket: std::net::TcpStream,
    asked: &Mutex<Vec<String>>,
    silent: &Mutex<BTreeSet<String>>,
) {
    let mut writer = socket.try_clone().unwrap();
    if writer.write_all(b"200 ready\r\n").is_err() {
        return;
    }
    for line in std::io::BufReader::new(socket).lines() {
        let Ok(line) = line else { break };
        let upper = line.to_ascii_uppercase();
        let reply = if upper == "CAPABILITIES" {
            "101 Capability list\r\nVERSION 2\r\nREADER\r\n.\r\n".to_string()
        } else if upper.starts_with("STAT ") {
            let id = line[5..].trim().to_string();
            if silent.lock().unwrap().contains(bare(&id)) {
                break;
            }
            let reply = if held(&id) {
                format!("223 0 {id}\r\n")
            } else {
                "430 no such article\r\n".to_string()
            };
            asked.lock().unwrap().push(id);
            reply
        } else if upper.starts_with("HEAD ") {
            let id = line[5..].trim();
            if held(id) {
                format!("221 0 {id}\r\nSubject: lantern\r\n.\r\n")
            } else {
                "430 no such article\r\n".to_string()
            }
        } else if upper.starts_with("QUIT") {
            let _ = writer.write_all(b"205 closing\r\n");
            break;
        } else {
            "500 command not recognized\r\n".to_string()
        };
        if writer.write_all(reply.as_bytes()).is_err() {
            break;
        }
    }
}

// The pipeline's client: one server, the listener, with one connection.
fn client(port: u16) -> weaver_nntp::client::NntpClient {
    weaver_nntp::client::NntpClient::new(weaver_nntp::client::NntpClientConfig::single(
        weaver_nntp::ServerConfig {
            host: "127.0.0.1".into(),
            port,
            tls: false,
            ..Default::default()
        },
        1,
    ))
}

// The job admitted with the listener as its server and no stand-in for it,
// served its lost articles until the feed arms a round. The articles it
// still has queued are taken off its queue, so every seat the dispatch
// passes below hand out is one a probe batch asked for.
async fn armed(scenario: &Scenario, port: u16) -> (tempfile::TempDir, Pipeline) {
    let (temp_dir, mut pipeline, _) = admit_with(scenario, |pipeline, _| {
        pipeline.nntp = Arc::new(client(port));
    })
    .await;
    'serve: for &(file, ordinal) in &scenario.lost {
        serve(&mut pipeline, scenario, (file, ordinal)).await;
        hear_graph_mail(&mut pipeline, JOB).await;
        if pipeline.post_probes.out(JOB) {
            break 'serve;
        }
    }
    assert!(pipeline.post_probes.out(JOB), "the losses arm a round");
    let state = pipeline.jobs.get_mut(&JOB).unwrap();
    if let Some(active) = state.articles.active_mut() {
        active.download_queue.extract_matching(|_| true);
        active.recovery_queue.extract_matching(|_| true);
    }
    (temp_dir, pipeline)
}

// Seat the waiting batches and hear the lanes answer them until the round
// is heard. A lane sends its answer once it is idle again, so each pass
// seats a waiting batch on the seat the last answer freed. The pass that
// finds a batch every server has answered for rules on it there, so the
// round ends inside a dispatch pass, and only a round still out after one
// waits for the next answer.
async fn hear_round(pipeline: &mut Pipeline) {
    loop {
        let waiting = pipeline.post_probes.waiting.len();
        pipeline.dispatch_downloads();
        assert!(
            waiting == 0 || pipeline.post_probes.waiting.len() < waiting,
            "a free seat takes a waiting batch"
        );
        if !pipeline.post_probes.out(JOB) {
            return;
        }
        let mail = pipeline
            .post_probes
            .recv()
            .await
            .expect("a seated batch is answered");
        pipeline.post_probe_mail(mail);
    }
}

// Every seat busy: the batch waits for one rather than giving up, and a lane
// on that server yields at its next refill. Once the seat frees, the batch
// runs on it and the post is ruled dead on what it found.
#[tokio::test]
async fn a_probe_waits_for_a_busy_seat_and_rules_the_post_dead_once_it_frees() {
    let server = stat_server();
    let scenario = first_articles_alone(40);
    let (_temp_dir, mut pipeline) = armed(&scenario, server.listener.port).await;

    // A lane mid-transfer holds the server's one connection. Lanes dial
    // off the runtime, so this one does too.
    let nntp = Arc::clone(&pipeline.nntp);
    let busy = std::thread::spawn(move || {
        nntp.try_acquire_blocking_body_lane_with_estimate(&[], &[], 0)
            .expect("the server seats the busy lane")
    })
    .join()
    .unwrap();
    let waiting = pipeline.post_probes.waiting.len();
    assert_eq!(waiting, 1, "ten articles sampled, one batch");
    for _ in 0..3 {
        pipeline.dispatch_downloads();
    }
    assert_eq!(pipeline.post_probes.waiting.len(), waiting);
    assert!(
        pipeline
            .post_probes
            .waiting
            .iter()
            .all(|batch| batch.asked().is_empty()),
        "no batch is answered without a seat"
    );
    assert!(pipeline.post_probes.out(JOB), "the round waits, unanswered");
    assert!(
        pipeline.probe_waits_for_seat_on(0),
        "a lane on the busy server yields its seat at its next refill"
    );

    // The lane finishes its transfer and its seat is free.
    std::thread::spawn(move || busy.park()).join().unwrap();
    hear_round(&mut pipeline).await;

    let failure = pipeline
        .post_probes
        .dead
        .get(&JOB)
        .expect("the round rules the post dead");
    assert!(
        failure.starts_with("health probe: 9 of 10 samples missing; health "),
        "{failure}"
    );
    assert_eq!(
        server.asked.lock().unwrap().len(),
        10,
        "every sample asked once"
    );
}

// A server held back from new connections seats no batch, not even on the
// connection a lane already keeps there: that seat waits for the hold to
// lift, and no lane there is yielded for it.
#[tokio::test]
async fn a_probe_never_takes_a_seat_on_a_held_server() {
    let server = stat_server();
    // Six hundred and sixty articles sample to fifty-five, two batches.
    let scenario = first_articles_alone(220);
    let (_temp_dir, mut pipeline) = armed(&scenario, server.listener.port).await;
    assert_eq!(pipeline.post_probes.waiting.len(), 2);

    // The one seat takes the first batch; the second waits for it.
    pipeline.dispatch_downloads();
    assert_eq!(pipeline.post_probes.waiting.len(), 1);
    let mail = pipeline
        .post_probes
        .recv()
        .await
        .expect("the seated batch is answered");
    pipeline.post_probe_mail(mail);
    assert_eq!(
        pipeline.idle_download_lanes_by_server().get(&0),
        Some(&1),
        "the lane that answered keeps its connection, idle"
    );

    // The server is quarantined, waiting on its recovery probe.
    let recovery = pipeline.nntp.pool().take_recovery_probe_for_test(0);
    let mut health = pipeline.nntp.pool().server_health_changes();
    for _ in 0..3 {
        pipeline.dispatch_downloads();
    }
    assert_eq!(
        pipeline
            .post_probes
            .waiting
            .iter()
            .filter(|batch| batch.asked().is_empty())
            .count(),
        1,
        "the second batch is not seated on the held server's idle connection"
    );
    assert!(pipeline.post_probes.out(JOB));
    assert!(!pipeline.probe_waits_for_seat_on(0));
    assert_eq!(
        server.asked.lock().unwrap().len(),
        50,
        "only the first batch was asked"
    );

    recovery.answer();
    health.changed().await;
    assert_eq!(pipeline.nntp.pool().connection_hold(0, true), None);
    hear_round(&mut pipeline).await;

    let failure = pipeline
        .post_probes
        .dead
        .get(&JOB)
        .expect("the round rules the post dead");
    assert!(
        failure.starts_with("health probe: 54 of 55 samples missing; health "),
        "{failure}"
    );
    assert_eq!(server.asked.lock().unwrap().len(), 55);
}

// A batch the server leaves unanswered costs the round that batch alone: it
// is not asked of that server again, and with no other server to go to it
// is folded in unsettled. The batch beside it answered, and the round rules
// on what that batch found.
#[tokio::test]
async fn a_batch_the_server_leaves_unanswered_costs_the_round_that_batch_alone() {
    let server = stat_server();
    // Six hundred and sixty articles sample to fifty-five, two batches.
    let scenario = first_articles_alone(220);
    let (_temp_dir, mut pipeline) = armed(&scenario, server.listener.port).await;
    assert_eq!(pipeline.post_probes.waiting.len(), 2);
    let unanswered: BTreeSet<String> = pipeline.post_probes.waiting[0]
        .unfound()
        .iter()
        .map(|id| bare(id).to_string())
        .collect();
    let answered = pipeline.post_probes.waiting[1].unfound();
    server
        .silent
        .lock()
        .unwrap()
        .extend(unanswered.iter().cloned());

    hear_round(&mut pipeline).await;

    let missed = answered.iter().filter(|id| !held(id)).count();
    let failure = pipeline
        .post_probes
        .dead
        .get(&JOB)
        .expect("the round rules the post dead on the batch that answered");
    assert!(
        failure.starts_with(&format!(
            "health probe: {missed} of {} samples missing; health ",
            answered.len()
        )),
        "{failure}"
    );
    let asked = server.asked.lock().unwrap();
    assert_eq!(
        asked
            .iter()
            .filter(|id| unanswered.contains(bare(id)))
            .count(),
        0,
        "the unanswered batch is never answered, nor asked again"
    );
    assert_eq!(asked.len(), answered.len());
}
