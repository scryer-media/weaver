// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    net::{Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, atomic::Ordering},
};

use anyhow::{Context, bail, ensure};
use futures::FutureExt;
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{oneshot, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    AddTorrent, AddTorrentOptions, ConnectionOptions, ListenerMode, ListenerOptions, Session,
    SessionOptions,
};

const PAYLOAD_NAME: &str = "interop-payload.bin";
const PAYLOAD_BYTES: u64 = 2 * 1024 * 1024 + 137;

struct ReadyGate {
    ready: oneshot::Sender<u32>,
    proceed: oneshot::Receiver<()>,
}

impl ReadyGate {
    async fn pause(self, child: &Child) -> anyhow::Result<()> {
        let pid = child.id().context("native peer has no process ID")?;
        self.ready
            .send(pid)
            .map_err(|_| anyhow::anyhow!("readiness observer is gone"))?;
        self.proceed.await.context("readiness gate was closed")
    }
}

#[derive(Clone, Debug)]
struct CleanupReport {
    pid: Option<u32>,
    cancelled: bool,
    child_reaped: bool,
    diagnostics_joined: bool,
    session_stopped: bool,
    error: Option<String>,
}

struct InteropRun {
    cancellation: CancellationToken,
    worker: JoinHandle<anyhow::Result<()>>,
}

impl InteropRun {
    async fn wait(mut self) -> anyhow::Result<()> {
        match (&mut self.worker).await {
            Ok(result) => result,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for InteropRun {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

struct NativePeer {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    stderr: JoinHandle<anyhow::Result<String>>,
    events: usize,
}

impl NativePeer {
    fn spawn(
        executable: &Path,
        mode: &str,
        torrent: &Path,
        directory: &Path,
        peer: Option<SocketAddr>,
    ) -> anyhow::Result<Self> {
        let mut command = Command::new(executable);
        command.arg(mode).arg(torrent).arg(directory);
        if let Some(peer) = peer {
            ensure!(
                peer.ip() == Ipv4Addr::LOCALHOST,
                "peer must be loopback IPv4"
            );
            command.arg(peer.port().to_string());
        }
        Self::spawn_command(command)
    }

    fn spawn_command(mut command: Command) -> anyhow::Result<Self> {
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().context("starting native uTP peer")?;
        let stdin = child.stdin.take().context("native control is not piped")?;
        let stdout = child.stdout.take().context("native stdout is not piped")?;
        let stderr = child.stderr.take().context("native stderr is not piped")?;
        let stderr = tokio::spawn(async move {
            let mut output = Vec::new();
            stderr
                .take(1024 * 1024 + 1)
                .read_to_end(&mut output)
                .await?;
            ensure!(
                output.len() <= 1024 * 1024,
                "native diagnostics exceeded limit"
            );
            Ok(String::from_utf8_lossy(&output).into_owned())
        });
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout).lines(),
            stderr,
            events: 0,
        })
    }

    async fn event(&mut self, wanted: &str) -> anyhow::Result<Value> {
        loop {
            let line = self
                .stdout
                .next_line()
                .await?
                .context("native peer exited before the expected event")?;
            self.events += 1;
            ensure!(self.events <= 1024, "too many native events");
            let event: Value = serde_json::from_str(&line).context("invalid native JSON event")?;
            let kind = event["event"]
                .as_str()
                .context("native event has no kind")?;
            if kind == "peer" {
                ensure!(
                    event["transport"] == "utp",
                    "native peer used another transport"
                );
            }
            if kind == wanted {
                return Ok(event);
            }
            ensure!(kind == "peer", "unexpected native event: {event}");
        }
    }

    async fn ready(&mut self, mode: &str) -> anyhow::Result<SocketAddr> {
        let event = self.event("ready").await?;
        ensure!(event["mode"] == mode, "native mode mismatch: {event}");
        ensure!(
            event["utp_only"] == true,
            "native peer permits TCP: {event}"
        );
        let port = event["port"].as_u64().context("native peer has no port")?;
        let port = u16::try_from(port).context("native port exceeds u16")?;
        ensure!(port != 0, "invalid native port");
        Ok((Ipv4Addr::LOCALHOST, port).into())
    }

    async fn finish(&mut self, native_seed: bool) -> anyhow::Result<()> {
        self.stdin.write_all(b"finish\n").await?;
        let event = self.event("complete").await?;
        ensure!(
            event["verified_bytes"] == PAYLOAD_BYTES,
            "native verified wrong size"
        );
        ensure!(
            event["tcp_connections"] == 0,
            "native peer used TCP: {event}"
        );
        ensure!(
            event["utp_connections"].as_u64().unwrap_or(0) > 0,
            "no native uTP connection"
        );
        let direction = if native_seed {
            "incoming_connections"
        } else {
            "outgoing_connections"
        };
        ensure!(
            event[direction].as_u64().unwrap_or(0) > 0,
            "wrong native connection direction: {event}"
        );
        let payload = if native_seed {
            "payload_upload"
        } else {
            "payload_download"
        };
        ensure!(
            event[payload].as_u64().unwrap_or(0) >= PAYLOAD_BYTES,
            "native did not transfer the full payload: {event}"
        );
        Ok(())
    }

    async fn reap(mut self, completed: bool) -> anyhow::Result<()> {
        let stopping = (|| -> std::io::Result<()> {
            if !completed && self.child.try_wait()?.is_none() {
                self.child.start_kill()?;
            }
            Ok(())
        })()
        .context("stopping owned native peer");
        let status = self.child.wait().await.context("reaping native peer");
        let diagnostics = self.stderr.await.context("joining native diagnostics");
        stopping?;
        let status = status?;
        let diagnostics = diagnostics??;
        if completed {
            ensure!(
                status.success(),
                "native peer exited {status}: {diagnostics}"
            );
        } else if !diagnostics.is_empty() {
            eprintln!("native uTP diagnostics:\n{diagnostics}");
        }
        Ok(())
    }
}

async fn prepare(
    executable: &Path,
    directory: &Path,
    cancellation: &CancellationToken,
) -> anyhow::Result<bool> {
    let mut command = Command::new(executable);
    command.arg("prepare").arg(directory);
    let mut native = NativePeer::spawn_command(command)?;
    let prepared = tokio::select! {
        biased;
        _ = cancellation.cancelled() => Ok(false),
        event = native.event("prepared") => event.and_then(|event| {
            ensure!(event["bytes"] == PAYLOAD_BYTES, "wrong preparation size");
            Ok(true)
        }),
    };
    let cleanup = native.reap(matches!(&prepared, Ok(true))).await;
    match (prepared, cleanup) {
        (Ok(prepared), Ok(())) => Ok(prepared),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(cleanup)) => {
            bail!("{error:#}; native preparation cleanup failed: {cleanup:#}")
        }
    }
}

async fn exact_payload(source: &Path, downloaded: &Path) -> anyhow::Result<()> {
    let expected = tokio::fs::read(source.join(PAYLOAD_NAME)).await?;
    let actual = tokio::fs::read(downloaded.join(PAYLOAD_NAME)).await?;
    ensure!(expected.len() as u64 == PAYLOAD_BYTES, "wrong source size");
    ensure!(actual == expected, "downloaded bytes differ from source");
    Ok(())
}

async fn transfer(
    executable: &Path,
    root: &Path,
    session: &Arc<Session>,
    native_seed: bool,
    peer: &mut Option<NativePeer>,
    ready_gate: Option<ReadyGate>,
) -> anyhow::Result<()> {
    let torrent = root.join("interop.torrent");
    let seed = root.join("seed");
    let download = root.join("download");
    let metadata = tokio::fs::read(&torrent).await?;
    if native_seed {
        *peer = Some(NativePeer::spawn(
            executable, "seed", &torrent, &seed, None,
        )?);
        let native = peer.as_mut().context("native seed is missing")?;
        let remote = native.ready("seed").await?;
        if let Some(gate) = ready_gate {
            gate.pause(&native.child).await?;
        }
        let handle = session
            .add_torrent(
                AddTorrent::from_bytes(metadata),
                Some(AddTorrentOptions {
                    initial_peers: Some(vec![remote]),
                    output_folder: Some(
                        download
                            .to_str()
                            .context("output path is not UTF-8")?
                            .into(),
                    ),
                    overwrite: true,
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .context("expected the downloading torrent handle")?;
        tokio::select! {
            completed = handle.wait_until_completed() => completed?,
            exited = native.child.wait() => bail!("native seed exited before download completion: {}", exited?),
        }
        exact_payload(&seed, &download).await?;
        native.finish(true).await?;
        ensure!(
            session.connector.stats().utp.snapshot().v4.successes > 0,
            "Weaver did not dial uTP"
        );
    } else {
        let handle = session
            .add_torrent(
                AddTorrent::from_bytes(metadata),
                Some(AddTorrentOptions {
                    output_folder: Some(seed.to_str().context("seed path is not UTF-8")?.into()),
                    overwrite: true,
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .context("expected the seeding torrent handle")?;
        handle.wait_until_completed().await?;
        let local = session.listen_addr().context("Weaver is not listening")?;
        *peer = Some(NativePeer::spawn(
            executable,
            "download",
            &torrent,
            &download,
            Some(local),
        )?);
        let native = peer.as_mut().context("native downloader is missing")?;
        native.ready("download").await?;
        if let Some(gate) = ready_gate {
            gate.pause(&native.child).await?;
        }
        let event = native.event("downloaded").await?;
        ensure!(
            event["verified_bytes"] == PAYLOAD_BYTES,
            "wrong native download size"
        );
        exact_payload(&seed, &download).await?;
        native.finish(false).await?;
        ensure!(
            session.utp_sockets.iter().any(|socket| socket
                .0
                .stats()
                .accepts
                .load(Ordering::Relaxed)
                > 0),
            "Weaver did not accept uTP"
        );
    }
    let tcp = session.connector.stats().tcp.snapshot();
    ensure!(
        tcp.v4.attempts == 0 && tcp.v6.attempts == 0,
        "Weaver attempted TCP fallback"
    );
    Ok(())
}

async fn interop(native_seed: bool) -> anyhow::Result<()> {
    start_interop(native_seed, None).0.wait().await
}

fn start_interop(
    native_seed: bool,
    ready_gate: Option<ReadyGate>,
) -> (InteropRun, watch::Receiver<Option<CleanupReport>>) {
    let cancellation = CancellationToken::new();
    let (completed, cleanup) = watch::channel(None);
    let worker = tokio::spawn(interop_worker(
        native_seed,
        ready_gate,
        cancellation.clone(),
        completed,
    ));
    (
        InteropRun {
            cancellation,
            worker,
        },
        cleanup,
    )
}

async fn interop_worker(
    native_seed: bool,
    ready_gate: Option<ReadyGate>,
    cancellation: CancellationToken,
    completed: watch::Sender<Option<CleanupReport>>,
) -> anyhow::Result<()> {
    let executable = PathBuf::from(
        std::env::var_os("WEAVER_UTP_INTEROP_HELPER")
            .context("set WEAVER_UTP_INTEROP_HELPER to the native helper executable")?,
    );
    ensure!(
        executable.is_absolute() && executable.is_file(),
        "helper must name an existing absolute executable path"
    );
    let directory = tempfile::tempdir()?;
    let mut session = None;
    let mut peer = None;
    let outcome = std::panic::AssertUnwindSafe(async {
        if !prepare(&executable, directory.path(), &cancellation).await? {
            return Ok(false);
        }
        session = Some(
            Session::new_with_opts(
                directory
                    .path()
                    .join(if native_seed { "download" } else { "seed" }),
                SessionOptions {
                    dht: None,
                    persistence: None,
                    disable_trackers: true,
                    disable_local_service_discovery: true,
                    listen: Some(ListenerOptions {
                        mode: ListenerMode::UtpOnly,
                        listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                        ..Default::default()
                    }),
                    connect: Some(ConnectionOptions {
                        enable_tcp: false,
                        ..Default::default()
                    }),
                    ..crate::test_support::session_options()
                },
            )
            .await?,
        );
        let session = session.as_ref().context("interop session is missing")?;
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Ok(false),
            result = transfer(
                &executable,
                directory.path(),
                session,
                native_seed,
                &mut peer,
                ready_gate,
            ) => result.map(|()| true),
        }
    })
    .catch_unwind()
    .await;
    let transferred = matches!(&outcome, Ok(Ok(true)));
    let cancelled = matches!(&outcome, Ok(Ok(false)));
    let pid = peer.as_ref().and_then(|peer| peer.child.id());
    let cleanup = if let Some(peer) = peer {
        peer.reap(transferred).await
    } else {
        Ok(())
    };
    let session_stopped = if let Some(session) = session {
        session.stop().await;
        true
    } else {
        false
    };
    drop(directory);
    completed.send_replace(Some(CleanupReport {
        pid,
        cancelled,
        child_reaped: pid.is_some() && cleanup.is_ok(),
        diagnostics_joined: pid.is_some() && cleanup.is_ok(),
        session_stopped,
        error: cleanup.as_ref().err().map(|error| format!("{error:#}")),
    }));
    match outcome {
        Ok(Ok(true)) => cleanup,
        Ok(Ok(false)) => {
            cleanup?;
            bail!("interop caller cancelled");
        }
        Ok(Err(error)) => {
            if let Err(cleanup) = cleanup {
                bail!("{error:#}; native cleanup failed: {cleanup:#}");
            }
            Err(error)
        }
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires WEAVER_UTP_INTEROP_HELPER"]
async fn test_native_utp_seed_serves_a_weaver_download() -> anyhow::Result<()> {
    interop(true).await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires WEAVER_UTP_INTEROP_HELPER"]
async fn test_weaver_utp_seed_serves_a_native_download() -> anyhow::Result<()> {
    interop(false).await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires WEAVER_UTP_INTEROP_HELPER"]
async fn test_cancelled_utp_interop_reaps_its_native_peer() -> anyhow::Result<()> {
    let (ready_tx, ready_rx) = oneshot::channel();
    let (_proceed_tx, proceed_rx) = oneshot::channel();
    let (run, mut cleanup) = start_interop(
        true,
        Some(ReadyGate {
            ready: ready_tx,
            proceed: proceed_rx,
        }),
    );
    let caller = tokio::spawn(run.wait());
    let pid = ready_rx
        .await
        .context("native peer did not reach its readiness gate")?;
    caller.abort();
    let caller_result = caller.await;
    let report = loop {
        if let Some(report) = cleanup.borrow().clone() {
            break report;
        }
        cleanup
            .changed()
            .await
            .context("interop worker exited without cleanup acknowledgement")?;
    };
    ensure!(
        matches!(caller_result, Err(error) if error.is_cancelled()),
        "interop caller was not cancelled"
    );
    ensure!(
        report.pid == Some(pid),
        "cleanup acknowledged a different native peer"
    );
    ensure!(
        report.cancelled,
        "interop worker did not observe cancellation"
    );
    ensure!(report.child_reaped, "native peer was not reaped");
    ensure!(
        report.diagnostics_joined,
        "native diagnostics were not joined"
    );
    ensure!(report.session_stopped, "interop session was not stopped");
    ensure!(
        report.error.is_none(),
        "interop cleanup failed: {:?}",
        report.error
    );
    Ok(())
}
