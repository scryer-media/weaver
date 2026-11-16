// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An article several files of one job share is fetched once, on a download
// lane like every other article, and its other references are answered from
// the job's repeated-article cache.

use super::*;

const SHARED: &str = "shared@example.com";
const PAYLOAD: &[u8] = b"one article, several independent file placements";
const COPIES: usize = 4;

// How a fake server answers the shared article.
#[derive(Clone, Copy, PartialEq)]
enum Answer {
    Serve,
    Missing,
    // Answers 412 to a BODY sent with no group selected, and serves it after
    // a GROUP.
    NeedsGroup,
    // Refuses the login.
    RefuseLogin,
    // Drops the connection partway through the first body it sends, and
    // serves every BODY after it.
    DropFirst,
}

struct FakeServer {
    port: u16,
    commands: Arc<std::sync::Mutex<Vec<String>>>,
    served: Arc<std::sync::atomic::AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl FakeServer {
    fn commands(&self) -> Vec<String> {
        self.commands
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn served(&self) -> usize {
        self.served.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn bodies(&self) -> usize {
        self.commands()
            .iter()
            .filter(|line| line.starts_with("BODY "))
            .count()
    }

    fn groups(&self) -> usize {
        self.commands()
            .iter()
            .filter(|line| line.starts_with("GROUP "))
            .count()
    }
}

async fn fake_server(answer: Answer) -> FakeServer {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let mut body = Vec::new();
    weaver_yenc::encode(PAYLOAD, &mut body, 128, "shared.bin").unwrap();
    let body = Arc::new(body);
    let commands = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorded = commands.clone();
    let served = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = served.clone();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let task = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, _) = accepted.unwrap();
                    let body = body.clone();
                    let recorded = recorded.clone();
                    let counted = counted.clone();
                    let dropped = dropped.clone();
                    connections.spawn(async move {
                        let (read, mut write) = stream.into_split();
                        if write.write_all(b"200 fixture ready\r\n").await.is_err() {
                            return;
                        }
                        let mut lines = BufReader::new(read).lines();
                        let mut group_selected = false;
                        while let Ok(Some(line)) = lines.next_line().await {
                            recorded
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .push(line.clone());
                            let response: &[u8] = if line == "CAPABILITIES" {
                                b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n"
                            } else if line.starts_with("AUTHINFO USER") {
                                b"381 password required\r\n"
                            } else if line.starts_with("AUTHINFO PASS") {
                                if answer == Answer::RefuseLogin {
                                    b"481 authentication rejected\r\n"
                                } else {
                                    b"281 ok\r\n"
                                }
                            } else if line.starts_with("GROUP ") {
                                group_selected = true;
                                b"211 1 1 1 alt.binaries.test\r\n"
                            } else if line == format!("BODY <{SHARED}>") {
                                match answer {
                                    Answer::Missing => b"430 no such article\r\n",
                                    Answer::NeedsGroup if !group_selected => {
                                        b"412 no newsgroup selected\r\n"
                                    }
                                    Answer::DropFirst
                                        if !dropped.swap(true, std::sync::atomic::Ordering::SeqCst) =>
                                    {
                                        let header = format!("222 0 <{SHARED}> body\r\n");
                                        let _ = write.write_all(header.as_bytes()).await;
                                        let _ = write.write_all(&body[..body.len() / 2]).await;
                                        break;
                                    }
                                    _ => {
                                        counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                        let header = format!("222 0 <{SHARED}> body\r\n");
                                        if write.write_all(header.as_bytes()).await.is_err()
                                            || write.write_all(&body).await.is_err()
                                        {
                                            break;
                                        }
                                        b".\r\n"
                                    }
                                }
                            } else if line == "QUIT" {
                                let _ = write.write_all(b"205 bye\r\n").await;
                                break;
                            } else {
                                b"500 unsupported\r\n"
                            };
                            if write.write_all(response).await.is_err() {
                                break;
                            }
                        }
                    });
                }
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    });
    FakeServer {
        port,
        commands,
        served,
        task,
    }
}

// One pool entry per fake server, tried in the order given: each server is
// a priority group of its own, so the next is asked only for an article the
// ones before it could not serve.
fn client_for(servers: &[(&FakeServer, Answer)]) -> NntpClient {
    NntpClient::new(NntpClientConfig {
        servers: servers
            .iter()
            .enumerate()
            .map(|(idx, (server, answer))| {
                let login = *answer == Answer::RefuseLogin;
                weaver_nntp::pool::ServerPoolConfig {
                    server: weaver_nntp::ServerConfig {
                        host: "127.0.0.1".to_string(),
                        port: server.port,
                        tls: false,
                        username: login.then(|| "user".to_string()),
                        password: login.then(|| "secret".to_string()),
                        // Far above any real run: a stall on a loaded runner must
                        // never turn into a retry that changes what is counted.
                        connect_timeout: Duration::from_secs(3600),
                        command_timeout: Duration::from_secs(3600),
                        ..Default::default()
                    },
                    stable_id: weaver_nntp::transfer::StableServerId(idx as u32 + 1),
                    max_connections: 2,
                    group: idx as u32,
                    ..Default::default()
                }
            })
            .collect(),
        max_idle_age: Duration::from_secs(60),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(3600),
    })
}

struct Run {
    status: JobStatus,
    delivered: usize,
    failed_bytes: u64,
    wire_bytes: u64,
    not_found: u64,
    transient: u64,
}

// Every file of the job names the one shared article.
async fn run_shared_job(servers: &[(&FakeServer, Answer)], id: JobId) -> Run {
    let harness = TestHarness::new_with_nntp(client_for(servers), 2 * servers.len()).await;
    let mut spec = standalone_job_spec(
        "Shared Article",
        &(0..COPIES)
            .map(|n| (format!("copy-{n}.bin"), PAYLOAD.len() as u32))
            .collect::<Vec<_>>(),
    );
    for file in &mut spec.files {
        file.segments[0].message_ids = SHARED.into();
    }
    let mut revisions = harness.handle.subscribe_job_changes();
    harness
        .handle
        .add_job(
            id,
            spec.clone(),
            PathBuf::from("shared.nzb"),
            nzb_zstd_for(&spec),
        )
        .await
        .unwrap();
    let job = loop {
        revisions.mark_unchanged();
        let job = harness.handle.get_job(id).unwrap();
        if matches!(job.status, JobStatus::Complete | JobStatus::Failed { .. }) {
            break job;
        }
        revisions.changed().await.unwrap();
    };
    let output = harness._temp_dir.path().join("complete/Shared Article");
    let delivered = std::fs::read_dir(&output)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|entry| entry.file_type().unwrap().is_file())
        .filter(|entry| std::fs::read(entry.path()).unwrap() == PAYLOAD)
        .count();
    let metrics = harness.handle.get_live_metrics();
    harness.shutdown().await;
    Run {
        status: job.status,
        delivered,
        failed_bytes: job.failed_bytes,
        wire_bytes: metrics.bytes_downloaded,
        not_found: metrics.download_failures_article_not_found,
        transient: metrics.download_failures_transient,
    }
}

fn encoded_len() -> u64 {
    let mut body = Vec::new();
    weaver_yenc::encode(PAYLOAD, &mut body, 128, "shared.bin").unwrap();
    body.len() as u64
}

// One BODY on the wire answers all four references, and no GROUP goes out
// to a server that never asked for one.
#[tokio::test]
async fn one_fetch_on_a_lane_serves_every_reference() {
    let server = fake_server(Answer::Serve).await;
    let run = run_shared_job(&[(&server, Answer::Serve)], JobId(81001)).await;
    assert_eq!(run.status, JobStatus::Complete);
    assert_eq!(run.delivered, COPIES);
    assert_eq!(server.bodies(), 1, "{:?}", server.commands());
    assert_eq!(server.served(), 1);
    assert_eq!(server.groups(), 0, "{:?}", server.commands());
    assert_eq!(
        run.wire_bytes,
        encoded_len(),
        "cached placements are not new transfers"
    );
    server.task.abort();
}

// The first server lacks the article and the second has it: the first is
// asked once, every reference that learned that goes to the second, and the
// second's one fetch serves them all.
#[tokio::test]
async fn a_shared_article_missing_on_one_server_is_fetched_once_from_the_next() {
    let first = fake_server(Answer::Missing).await;
    let second = fake_server(Answer::Serve).await;
    let run = run_shared_job(
        &[(&first, Answer::Missing), (&second, Answer::Serve)],
        JobId(81002),
    )
    .await;
    assert_eq!(run.status, JobStatus::Complete);
    assert_eq!(run.delivered, COPIES);
    assert_eq!(first.bodies(), 1, "{:?}", first.commands());
    assert_eq!(second.bodies(), 1, "{:?}", second.commands());
    assert_eq!(first.groups() + second.groups(), 0);
    assert_eq!(run.wire_bytes, encoded_len());
    first.task.abort();
    second.task.abort();
}

// Missing everywhere: every reference is answered not found by both servers,
// and nothing is delivered. The cache keeps one answer per article, so a
// reference that looks up after another server's answer replaced the first
// server's asks that server again; how often depends on scheduling. What
// holds is that each server is asked, and never more often than the
// references it could answer.
#[tokio::test]
async fn a_shared_article_missing_everywhere_books_every_reference_missing() {
    let first = fake_server(Answer::Missing).await;
    let second = fake_server(Answer::Missing).await;
    let run = run_shared_job(
        &[(&first, Answer::Missing), (&second, Answer::Missing)],
        JobId(81003),
    )
    .await;
    assert!(
        matches!(run.status, JobStatus::Failed { .. }),
        "{:?}",
        run.status
    );
    assert_eq!(run.delivered, 0);
    for server in [&first, &second] {
        assert!(
            (1..=COPIES).contains(&server.bodies()),
            "{:?}",
            server.commands()
        );
    }
    assert_eq!(
        run.not_found,
        (COPIES * 2) as u64,
        "every reference takes each server's answer"
    );
    assert_eq!(run.failed_bytes, (PAYLOAD.len() * COPIES) as u64);
    assert_eq!(run.wire_bytes, 0);
    first.task.abort();
    second.task.abort();
}

// A server that refuses the login is never sent a BODY; the article comes
// from the next server, once.
#[tokio::test]
async fn a_shared_article_skips_a_server_refusing_the_login() {
    let first = fake_server(Answer::RefuseLogin).await;
    let second = fake_server(Answer::Serve).await;
    let run = run_shared_job(
        &[(&first, Answer::RefuseLogin), (&second, Answer::Serve)],
        JobId(81004),
    )
    .await;
    assert_eq!(run.status, JobStatus::Complete);
    assert_eq!(run.delivered, COPIES);
    assert!(
        first
            .commands()
            .iter()
            .any(|line| line.starts_with("AUTHINFO PASS")),
        "{:?}",
        first.commands()
    );
    assert_eq!(first.bodies(), 0, "{:?}", first.commands());
    assert_eq!(second.bodies(), 1, "{:?}", second.commands());
    first.task.abort();
    second.task.abort();
}

// A GROUP goes out only once the server has asked for one with a 412, and
// the fetch after it still serves every reference.
#[tokio::test]
async fn a_shared_article_selects_a_group_only_after_a_412() {
    let server = fake_server(Answer::NeedsGroup).await;
    let run = run_shared_job(&[(&server, Answer::NeedsGroup)], JobId(81005)).await;
    assert_eq!(run.status, JobStatus::Complete);
    assert_eq!(run.delivered, COPIES);
    let commands = server.commands();
    let first_body = commands
        .iter()
        .position(|line| line.starts_with("BODY "))
        .unwrap();
    let first_group = commands
        .iter()
        .position(|line| line.starts_with("GROUP "))
        .expect("a GROUP after the 412");
    assert!(first_body < first_group, "{commands:?}");
    // A connection dialled before the 412 was learned may ask once more
    // without a group; the article is still served once.
    assert_eq!(server.served(), 1, "{commands:?}");
    server.task.abort();
}

// The connection fetching the shared article drops partway through its
// body. That is the one connection's fault, not the server's answer: no
// other reference is handed it, so the only transport failure booked is the
// dropped fetch's own, and the next fetch, on another connection to the same
// server, serves every reference.
#[tokio::test]
async fn a_dropped_fetch_is_never_replayed_to_another_reference() {
    let server = fake_server(Answer::DropFirst).await;
    let run = run_shared_job(&[(&server, Answer::DropFirst)], JobId(81006)).await;
    assert_eq!(run.status, JobStatus::Complete);
    assert_eq!(run.delivered, COPIES);
    assert_eq!(server.bodies(), 2, "{:?}", server.commands());
    assert_eq!(server.served(), 1);
    assert_eq!(
        run.transient, 1,
        "only the dropped fetch meets the fault; no reference replays it"
    );
    server.task.abort();
}
