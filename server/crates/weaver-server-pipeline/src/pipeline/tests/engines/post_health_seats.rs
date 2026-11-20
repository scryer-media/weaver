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

use super::harness::{JOB, Listener, Scenario, admit_with, serve};
use crate::pipeline::Pipeline;
use crate::pipeline::tests::hear_graph_mail;

// A message id without its brackets.
pub fn bare(message_id: &str) -> &str {
    message_id.trim_matches(['<', '>'])
}

// Whether the server holds the article: only a file's first is posted.
pub fn held(message_id: &str) -> bool {
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
pub struct StatServer {
    listener: Listener,
    pub asked: Arc<Mutex<Vec<String>>>,
    pub silent: Arc<Mutex<BTreeSet<String>>>,
}

pub fn stat_server() -> StatServer {
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
pub async fn armed(scenario: &Scenario, port: u16) -> (tempfile::TempDir, Pipeline) {
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
pub async fn hear_round(pipeline: &mut Pipeline) {
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

impl StatServer {
    pub fn port(&self) -> u16 {
        self.listener.port
    }
}

pub fn nntp(pipeline: &Pipeline) -> &Arc<weaver_nntp::client::NntpClient> {
    &pipeline.nntp
}

pub fn waiting_batches(pipeline: &Pipeline) -> usize {
    pipeline.post_probes.waiting.len()
}

pub fn unasked_batches(pipeline: &Pipeline) -> usize {
    pipeline
        .post_probes
        .waiting
        .iter()
        .filter(|batch| batch.asked().is_empty())
        .count()
}

pub fn probe_out(pipeline: &Pipeline) -> bool {
    pipeline.post_probes.out(JOB)
}

pub fn waits_for_seat(pipeline: &Pipeline, server: usize) -> bool {
    pipeline.probe_waits_for_seat_on(server)
}

pub fn dispatch(pipeline: &mut Pipeline) {
    pipeline.dispatch_downloads();
}

pub fn failed_post(pipeline: &Pipeline) -> Option<&str> {
    pipeline.post_probes.dead.get(&JOB).map(String::as_str)
}

pub async fn hear_one(pipeline: &mut Pipeline) {
    let mail = pipeline
        .post_probes
        .recv()
        .await
        .expect("the seated batch is answered");
    pipeline.post_probe_mail(mail);
}

pub fn idle_lanes(pipeline: &Pipeline, server: usize) -> Option<usize> {
    pipeline
        .idle_download_lanes_by_server()
        .get(&server)
        .copied()
}

pub fn batch_unfound(pipeline: &Pipeline, batch: usize) -> Vec<String> {
    pipeline.post_probes.waiting[batch].unfound()
}

pub fn known_gone(pipeline: &Pipeline, file: u32, ordinal: u32) -> bool {
    use weaver_server_base::jobs::ids::{NzbFileId, SegmentId};
    pipeline.given_up[&JOB].gone(SegmentId {
        file_id: NzbFileId {
            job_id: JOB,
            file_index: file,
        },
        segment_number: ordinal,
    })
}

pub fn rounds(pipeline: &Pipeline) -> usize {
    pipeline.post_probes.states[&JOB].rounds as usize
}
