// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The harness: the API is described in `mod.rs`. A run stands a fresh
// pipeline up, admits the scenario's job, and plays the server: every
// article the job's download queue hands out is answered from the posts,
// damaged or lost as the scenario says, and whenever the queue runs dry the
// completion gate runs the way the orchestrator runs it, with every detached
// ticket settled before the next look. The run ends when the job is complete
// or failed; a round that changes nothing ends it too, with the job's state
// in the panic.

use crate::history::nzbget_state::HistoryNzbgetStateQueries;
use crate::history::queries::HistoryQueries;
use crate::jobs::JobsArchivePasswordInternalQueries;
use crate::jobs::JobsDownloadSummaryPersistenceInternalQueries;
use crate::post_processing::settings::PostProcessingSettingsQueries;
use crate::settings::SettingsArchivePasswordsQueries;
use crate::settings::persistence::SettingsPersistenceQueries;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use weaver_graph::recovery::RepairOutcome;
use weaver_model::files::FileRole;

use super::super::{
    checkpoint_plan, insert_active_job, job_status_for_assert, new_direct_pipeline_with_backlog,
    pump_pipeline_runtime_queues, retire_pipeline_database,
};
use crate::history::HistoryFailureKind;
use crate::jobs::model::{FileSpec, JobSpec, JobStatus, SegmentSpec};
use crate::pipeline::download::DownloadWork;
use crate::pipeline::extraction::ExtractionLimits;
use crate::pipeline::graph::status::Delivery;
use crate::pipeline::{
    DecodeResult, DecodedChunk, Pipeline, PipelineEvent, SegmentEncoding, SegmentSource,
    YencLayoutAssertions,
};
use weaver_server_base::jobs::ids::{JobId, NzbFileId, SegmentId};

pub fn health_milli(total: u64, failed_bytes: u64) -> u32 {
    crate::pipeline::health_milli(total, failed_bytes)
}

pub fn critical_health_milli(total: u64, par2_bytes: u64) -> u32 {
    Pipeline::critical_health_milli(total, par2_bytes)
}

pub const ARTICLE: usize = 64;
pub const NESTING: u32 = crate::pipeline::graph::limits::NESTING;

pub use super::state::JOB;
pub use super::state::finished;
const JOB_NAME: &str = "lantern beacon";
// A stored settings value that does not parse.
pub const CORRUPT_POLICY: &str = "{unreadable";

pub struct Post {
    pub name: String,
    pub role: FileRole,
    pub bytes: Vec<u8>,
    // The smallest article this file is posted in. A poster cuts a file
    // whose leading structure must arrive whole in articles that hold it,
    // whatever the rest of the post uses.
    pub least_article: usize,
}

impl Post {
    pub fn plain(name: &str, bytes: Vec<u8>) -> Self {
        Self {
            name: name.to_string(),
            role: FileRole::from_filename(name),
            bytes,
            least_article: 0,
        }
    }

    // The article size this file is posted in where the post uses `size`.
    pub fn article(&self, size: usize) -> usize {
        size.max(self.least_article)
    }

    pub fn articles(&self, size: usize) -> impl Iterator<Item = &[u8]> {
        self.bytes.chunks(self.article(size))
    }
}

// `len` bytes of a seeded xorshift: no period runs through them, so no two
// blocks are alike and a compressing level cannot fold them, and the same
// seed gives the same bytes every run.
pub fn payload(len: usize, seed: u8) -> Vec<u8> {
    let mut state = 0x9e37_79b9_7f4a_7c15u64 ^ u64::from(seed);
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 56) as u8
        })
        .collect()
}

// An index and one volume of `blocks` recovery blocks over `files`, the
// index first.
pub fn par2_set(base: &str, files: &[(&str, &[u8])], blocks: u32) -> Vec<Post> {
    let posts = par2_volumes(base, files, blocks, 1);
    assert_eq!(posts.len(), 2, "an index and one volume");
    posts
}

// An index and `volumes` volumes sharing `blocks` recovery blocks, as the
// creator spreads them, the index first and the volumes in block order.
pub fn par2_volumes(base: &str, files: &[(&str, &[u8])], blocks: u32, volumes: u32) -> Vec<Post> {
    par2_volumes_sliced(base, files, ARTICLE as u64, blocks, volumes)
}

// An index and the volumes par2cmdline writes for `blocks` recovery blocks
// when told no volume count: sizes doubling from one, the last taking the
// rest, so 53 blocks are `vol00+01`, `vol01+02`, `vol03+04`, `vol07+08`,
// `vol15+16` and `vol31+22`. The index first, the volumes in block order,
// sliced at `slice` bytes.
pub fn par2_doubling(base: &str, files: &[(&str, &[u8])], slice: u64, blocks: u32) -> Vec<Post> {
    par2_created(base, files, slice, blocks, None)
}

// The same, sliced at `slice` bytes rather than ARTICLE.
pub fn par2_volumes_sliced(
    base: &str,
    files: &[(&str, &[u8])],
    slice: u64,
    blocks: u32,
    volumes: u32,
) -> Vec<Post> {
    par2_created(base, files, slice, blocks, Some(volumes))
}

// The set the creator writes, in `volumes` volumes or, with none, in the
// doubling sizes it picks itself.
fn par2_created(
    base: &str,
    files: &[(&str, &[u8])],
    slice: u64,
    blocks: u32,
    volumes: Option<u32>,
) -> Vec<Post> {
    use par2_rs::create::{BlockSizing, Par2Creator, Par2CreatorOptions, RecoveryAmount};

    // Fixture generation uses hard links when publishing parity files.
    // Filesystem lanes can keep that oracle on local storage while TMPDIR
    // puts every job file on the capability-limited mount under test.
    let source = match std::env::var_os("WEAVER_TEST_FIXTURE_DIR") {
        Some(dir) => tempfile::tempdir_in(dir).unwrap(),
        None => tempfile::tempdir().unwrap(),
    };
    let inputs = files
        .iter()
        .map(|(name, bytes)| {
            // A name may hold directories, as a set describing a nested
            // file names it.
            let path = source.path().join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, bytes).unwrap();
            path
        })
        .collect();
    let mut options = Par2CreatorOptions::with_output(
        source.path().join(format!("{base}.par2")),
        Some(source.path().to_path_buf()),
        inputs,
    );
    options.block_sizing = BlockSizing::Bytes(slice);
    options.recovery_amount = RecoveryAmount::Count(blocks);
    options.volume_count = volumes;
    let creator = Par2Creator::new(options);
    let created = creator.create(&creator.plan().unwrap()).unwrap();
    let mut posts: Vec<Post> = created
        .output_paths
        .iter()
        .map(|path| {
            let name = path.file_name().unwrap().to_str().unwrap();
            Post {
                bytes: std::fs::read(path).unwrap(),
                ..Post::plain(name, Vec::new())
            }
        })
        .collect();
    posts.sort_by(|left, right| {
        let index = |post: &Post| !matches!(post.role, FileRole::Par2 { is_index: true, .. });
        index(left)
            .cmp(&index(right))
            .then_with(|| left.name.cmp(&right.name))
    });
    posts
}

// A single-volume stored RAR5 holding `files` and, beside them, a PAR2
// set of `blocks` recovery blocks over them named under `base`: the set
// is nested, reachable only once the archive opens.
pub fn par2_in_store(base: &str, files: &[(&str, &[u8])], blocks: u32) -> Post {
    let mut members: Vec<(&str, Vec<u8>)> = files
        .iter()
        .map(|(name, bytes)| (*name, bytes.to_vec()))
        .collect();
    let set = par2_set(base, files, blocks);
    members.extend(
        set.iter()
            .map(|post| (post.name.as_str(), post.bytes.clone())),
    );
    let mut volumes = crate::pipeline::tests::direct_store::multi_member_store_set(&members, 1);
    let (name, bytes) = volumes.remove(0);
    Post::plain(&name, bytes)
}

// A PAR3 index and its volumes over `files`, blocks of one article, named
// as the creator names them under `base`, the index first.
pub fn par3_set(base: &str, files: &[(&str, &[u8])], blocks: u64) -> Vec<Post> {
    par3_set_sized(base, files, ARTICLE as u64, blocks)
}

// The same, in blocks of `size` bytes.
pub fn par3_set_sized(base: &str, files: &[(&str, &[u8])], size: u64, blocks: u64) -> Vec<Post> {
    let source = tempfile::tempdir().unwrap();
    let names: Vec<std::path::PathBuf> = files
        .iter()
        .map(|(name, bytes)| {
            std::fs::write(source.path().join(name), bytes).unwrap();
            std::path::PathBuf::from(name)
        })
        .collect();
    let out = tempfile::tempdir().unwrap();
    let report = par3_rs::create(
        &par3_rs::InputSpec::new(source.path(), &names),
        &out.path().join(base),
        &par3_rs::CreateOptions::default()
            .with_block_size(size)
            .with_recovery(par3_rs::RecoveryAmount::Blocks(blocks)),
    )
    .unwrap();
    let mut written = report.files_written.clone();
    written.sort_by_key(|path| path.to_string_lossy().contains(".vol"));
    written
        .iter()
        .map(|path| {
            let name = path.file_name().unwrap().to_str().unwrap();
            Post {
                bytes: std::fs::read(path).unwrap(),
                ..Post::plain(name, Vec::new())
            }
        })
        .collect()
}

// A PAR3 set over `files` built as `options` say (its codec, its recovery
// count, whether it carries its input as Data packets), blocks of one
// article, named as the creator names them under `base`, the index first.
pub fn par3_set_with(
    base: &str,
    files: &[(&str, &[u8])],
    options: par3_rs::creation::CreationOptions,
) -> Vec<Post> {
    use par3_rs::creation::{CreationPlan, CreationSource};
    use par3_rs::source::{MemorySourceAccess, SourceId};
    let mut access = MemorySourceAccess::default();
    let mut sources = Vec::new();
    for (index, (name, bytes)) in files.iter().enumerate() {
        let source = SourceId(index as u64 + 1);
        access.insert(source, 1, bytes.to_vec().into());
        sources.push(CreationSource {
            name: (*name).into(),
            source,
        });
    }
    let options = par3_rs::creation::CreationOptions {
        block_size: ARTICLE as u64,
        ..options
    };
    let plan = CreationPlan::build(std::sync::Arc::new(access), &sources, options).unwrap();
    let out = tempfile::tempdir().unwrap();
    let mut written = plan.execute(&out.path().join(base), out.path()).unwrap();
    written.sort_by_key(|path| {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        (name != format!("{base}.par3"), name)
    });
    written
        .iter()
        .map(|path| {
            let name = path.file_name().unwrap().to_str().unwrap();
            Post {
                bytes: std::fs::read(path).unwrap(),
                ..Post::plain(name, Vec::new())
            }
        })
        .collect()
}

// For each recovery block of a PAR2 volume, the ordinals of the articles
// holding its bytes.
pub fn recovery_blocks(post: &Post) -> Vec<Vec<u32>> {
    const HEADER: usize = 64;
    const RECOVERY: &[u8; 16] = b"PAR 2.0\0RecvSlic";
    let bytes = &post.bytes;
    let mut blocks = Vec::new();
    let mut at = 0;
    while at + HEADER <= bytes.len() {
        assert_eq!(&bytes[at..at + 8], b"PAR2\0PKT", "a packet at {at}");
        let length = u64::from_le_bytes(bytes[at + 8..at + 16].try_into().unwrap()) as usize;
        if &bytes[at + 48..at + 64] == RECOVERY {
            // The exponent leads the block.
            let (start, end) = (at + HEADER + 4, at + length);
            blocks.push(((start / ARTICLE) as u32..=((end - 1) / ARTICLE) as u32).collect());
        }
        at += length;
    }
    blocks
}

// The packets of a PAR2 file with every slice-checksum packet left out: a
// set that hashes its files whole and no finer.
pub fn without_slice_checksums(bytes: &[u8]) -> Vec<u8> {
    const HEADER: usize = 64;
    const SLICE_CHECKSUMS: &[u8; 16] = b"PAR 2.0\0IFSC\0\0\0\0";
    let mut kept = Vec::new();
    let mut at = 0;
    while at + HEADER <= bytes.len() {
        assert_eq!(&bytes[at..at + 8], b"PAR2\0PKT", "a packet at {at}");
        let length = u64::from_le_bytes(bytes[at + 8..at + 16].try_into().unwrap()) as usize;
        if &bytes[at + 48..at + 64] != SLICE_CHECKSUMS {
            kept.extend_from_slice(&bytes[at..at + length]);
        }
        at += length;
    }
    kept
}

// The bytes of a volume's one recovery packet, cut from it.
pub fn recovery_packet(volume: &Post) -> Vec<u8> {
    const HEADER: usize = 64;
    const RECOVERY: &[u8; 16] = b"PAR 2.0\0RecvSlic";
    let bytes = &volume.bytes;
    let mut at = 0;
    while at + HEADER <= bytes.len() {
        let length = u64::from_le_bytes(bytes[at + 8..at + 16].try_into().unwrap()) as usize;
        if &bytes[at + 48..at + 64] == RECOVERY {
            return bytes[at..at + length].to_vec();
        }
        at += length;
    }
    panic!("the volume holds a recovery packet");
}

// An article of a PAR2 volume whose loss costs exactly one recovery block.
pub fn one_block_article(post: &Post) -> u32 {
    let blocks = recovery_blocks(post);
    blocks
        .iter()
        .flatten()
        .copied()
        .find(|ordinal| {
            blocks
                .iter()
                .filter(|block| block.contains(ordinal))
                .count()
                == 1
        })
        .expect("an article inside one block alone")
}

// A listener on this machine that answers each connection it is dialed on
// a thread of its own, for as long as the test holds it, and closes the
// connection once its answer returns. Dropped, it stops
// taking connections, closes its socket, shuts every connection it took, and
// joins every thread it ran, so nothing it started outlives the test.
pub(in crate::pipeline::tests) struct Listener {
    pub(in crate::pipeline::tests) port: u16,
    stopping: Option<tokio::sync::oneshot::Sender<()>>,
    accepting: Option<std::thread::JoinHandle<Vec<Answering>>>,
}

// A connection the listener took, and the thread answering it.
type Answering = (std::net::TcpStream, std::thread::JoinHandle<()>);

impl Listener {
    pub(in crate::pipeline::tests) fn serve(
        answer: impl Fn(std::net::TcpStream) + Send + Sync + 'static,
    ) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind the listener");
        let port = listener.local_addr().unwrap().port();
        listener
            .set_nonblocking(true)
            .expect("configure the listener");
        let (stopping, mut stop) = tokio::sync::oneshot::channel();
        let answer = std::sync::Arc::new(answer);
        let accepting = std::thread::Builder::new()
            .name("harness-listener".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .build()
                    .expect("create listener runtime");
                runtime.block_on(async move {
                    let listener =
                        tokio::net::TcpListener::from_std(listener).expect("register the listener");
                    let mut answering = Vec::new();
                    loop {
                        let accepted = tokio::select! {
                            biased;
                            _ = &mut stop => break,
                            accepted = listener.accept() => accepted,
                        };
                        let Ok((socket, _)) = accepted else { continue };
                        let socket = socket.into_std().expect("take the accepted socket");
                        socket
                            .set_nonblocking(false)
                            .expect("configure the accepted socket");
                        let (Ok(held), Ok(closing)) = (socket.try_clone(), socket.try_clone())
                        else {
                            continue;
                        };
                        let answer = std::sync::Arc::clone(&answer);
                        // The listener holds the connection too, so it is shut
                        // here once the answer is done with it, as a server
                        // closes a connection it is done answering.
                        let thread = std::thread::spawn(move || {
                            answer(socket);
                            let _ = closing.shutdown(std::net::Shutdown::Both);
                        });
                        answering.push((held, thread));
                    }
                    answering
                })
            })
            .expect("spawn the listener");
        Self {
            port,
            stopping: Some(stopping),
            accepting: Some(accepting),
        }
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        if let Some(stop) = self.stopping.take() {
            let _ = stop.send(());
        }
        let Some(answering) = self.accepting.take().and_then(|thread| thread.join().ok()) else {
            return;
        };
        for (socket, thread) in answering {
            let _ = socket.shutdown(std::net::Shutdown::Both);
            let _ = thread.join();
        }
    }
}

// The scenario's server as a health probe's STAT batches ask it on the
// download lanes: a listener that holds every article but `missing`,
// answering STAT and HEAD on each connection it is dialed. The harness
// serves the articles themselves, so nothing asks it for a body.
fn stat_server(missing: BTreeSet<String>) -> Listener {
    Listener::serve(move |socket| answer_stat(socket, &missing))
}

fn answer_stat(socket: std::net::TcpStream, missing: &BTreeSet<String>) {
    use std::io::{BufRead, Write};
    let mut writer = socket.try_clone().unwrap();
    if writer.write_all(b"200 ready\r\n").is_err() {
        return;
    }
    for line in std::io::BufReader::new(socket).lines() {
        let Ok(line) = line else { break };
        let upper = line.to_ascii_uppercase();
        let asked = |command: &str| {
            upper
                .starts_with(command)
                .then(|| line[command.len()..].trim().to_string())
        };
        let held = |id: &str| !missing.contains(id.trim_matches(['<', '>']));
        let reply = if upper == "CAPABILITIES" {
            "101 Capability list\r\nVERSION 2\r\nREADER\r\n.\r\n".to_string()
        } else if let Some(id) = asked("STAT ") {
            if held(&id) {
                format!("223 0 {id}\r\n")
            } else {
                "430 no such article\r\n".to_string()
            }
        } else if let Some(id) = asked("HEAD ") {
            if held(&id) {
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

// The pipeline's client for a scenario: `servers` servers, all the
// scenario's STAT server, one connection each.
fn scenario_client(servers: usize, port: u16) -> weaver_nntp::client::NntpClient {
    weaver_nntp::client::NntpClient::new(weaver_nntp::client::NntpClientConfig {
        servers: (0..servers)
            .map(|_| weaver_nntp::ServerPoolConfig {
                max_connections: 1,
                ..weaver_nntp::ServerPoolConfig::new(weaver_nntp::ServerConfig {
                    host: "127.0.0.1".into(),
                    port,
                    tls: false,
                    ..Default::default()
                })
            })
            .collect(),
        ..weaver_nntp::client::NntpClientConfig::single(Default::default(), 1)
    })
}

// A health probe out for the job is heard before the next article is
// served: its round ends before the download moves on. Each batch waiting
// for a seat is seated as a dispatch pass seats it, and the harness leases
// no article to a lane, so a batch finds a seat free once the one before
// it has answered. A batch every server has answered for is ruled on as it
// is seated, so the round can end there, and only a round still out after
// the seating waits for the next answer.
pub(super) async fn hear_probe(pipeline: &mut Pipeline) {
    loop {
        pipeline.seat_waiting_probes(
            std::time::Instant::now(),
            &mut std::collections::HashMap::new(),
            &mut std::collections::HashMap::new(),
        );
        if !pipeline.post_probes.out(JOB) {
            return;
        }
        let mail = pipeline
            .post_probes
            .recv()
            .await
            .expect("a probe out speaks");
        pipeline.post_probe_mail(mail);
    }
}

// What a scenario sets on a pipeline it prepares, and what it is told of a
// word a run says.
type Setup = std::sync::Arc<dyn Fn(&mut Pipeline) + Send + Sync>;
type HeardHook = std::sync::Arc<dyn Fn(&crate::pipeline::graph::engine::Said) + Send + Sync>;

pub struct Scenario {
    pub posts: Vec<Post>,
    pub damage: Vec<(u32, u32)>,
    pub lost: Vec<(u32, u32)>,
    // Articles posted but absent from the NZB, by their place in the post:
    // the job never lists them, so its articles are numbered without them,
    // and those it does list still declare where their bytes go.
    pub unlisted: Vec<(u32, u32)>,
    // Articles the first ask is refused for, as a server without them
    // answers: the article goes back on the queue the way the worker
    // retries it after a 430, and the next ask is served.
    pub(in crate::pipeline::tests) refused: Vec<(u32, u32)>,
    // Articles the primary server serves damaged while a backup serves them
    // whole. A scenario with any has both servers, and an ask that excludes
    // the primary is the backup's.
    pub(in crate::pipeline::tests) corrupt_primary: Vec<(u32, u32)>,
    // Articles a server answers with a body that has no `=ybegin` line, by
    // file, ordinal and server. A scenario with any has every server named.
    pub(in crate::pipeline::tests) headerless: Vec<(u32, u32, usize)>,
    // How many servers the pipeline has, at least.
    pub(in crate::pipeline::tests) servers: usize,
    // Articles that decode this many bytes short of the part end their
    // header declares, as a server that cut the body off serves them.
    pub short: Vec<(u32, u32, usize)>,
    // Articles served whole under a checksum that disagrees with them.
    pub(in crate::pipeline::tests) crc_wrong: Vec<(u32, u32)>,
    // Posts whose every article declares this file size, not the post's.
    pub declares: Vec<(u32, u64)>,
    pub(in crate::pipeline::tests) password: Option<String>,
    pub(in crate::pipeline::tests) global_passwords: Vec<String>,
    // The decoded size of every article but each post's last; PAR2 slices
    // stay at ARTICLE whatever it is.
    pub article: usize,
    // Every batch the download queue hands out is served last article first.
    pub backwards: bool,
    // After each article served, every word the run owes is heard before
    // the next article: each checkpoint a step waits on commits between
    // arrivals, as on a database that commits faster than articles land.
    pub settle: bool,
    // Every handout is this many articles at once, served in the order the
    // queue handed them out, as that many workers fetching side by side
    // land; none is one at a time.
    pub batch: usize,
    // Every batch the download queue hands out is served as workers fetching
    // these posts at once: one article of each in turn, in this order.
    pub together: Vec<u32>,
    // Once the queue hands out an article of one of these posts, every
    // article it holds of them arrives at once, last first, as the fetches
    // of many workers land out of order.
    pub(in crate::pipeline::tests) reversed: Vec<u32>,
    // A restarted run serves these articles first, in this order, each once
    // the queue holds it.
    pub first: Vec<(u32, u32)>,
    // Every article is uuencoded: it says nothing of where its bytes go,
    // so they are placed in order.
    pub(in crate::pipeline::tests) uuencoded: bool,
    // Every batch is leased as a download lane leases it, and its articles'
    // checksums are cut at the slice sizes known when it was leased.
    pub leased: bool,
    // The extraction limits the pipeline holds, when not the environment's.
    pub(in crate::pipeline::tests) limits: Option<ExtractionLimits>,
    // The operator's unacceptable extensions, when not the stored ones.
    pub(in crate::pipeline::tests) unacceptable: Option<Vec<String>>,
    // What a job holding one of them comes to, when not the stored action.
    action: Option<crate::post_processing::model::UnwantedExtensionAction>,
    // Every pipeline the scenario prepares finds the stored settings
    // present but not parsing.
    policy_corrupt: bool,
    // The hold budgets every graph job of the pipeline shares, when not the
    // host's.
    pub holds: Option<weaver_graph::holds::HoldBudgets>,
    // The hardware profile the operator chose, when not the recommendation.
    profile: Option<crate::runtime::HardwareProfile>,
    // Metadata the job is submitted with beside the post's own.
    metadata: Vec<(String, String)>,
    // The STAT server of every pipeline the scenario prepared, held as long
    // as the scenario is.
    stat_servers: std::sync::Mutex<Vec<Listener>>,
    // What the scenario sets on every pipeline it prepares, last, once the
    // rest is set.
    setup: Option<Setup>,
    // What the scenario is told of everything a run says, as the run loop
    // hears it and before the job does.
    heard: Option<HeardHook>,
    // The name an article's header gives its file, by file, and from the
    // ordinal given on, when not the posted name.
    headers: Vec<(u32, u32, String)>,
    // Articles a server answers with another post's article: its bytes,
    // and a header naming that post's obfuscated file, its size and a
    // range far past this file's end, as a news server that reused the
    // message-id serves it.
    foreign: Vec<(u32, u32)>,
    // Articles whose bodies are the post's own and whose header is
    // another post's, as `foreign` states it.
    foreign_header: Vec<(u32, u32)>,
}

impl Scenario {
    pub fn new(posts: Vec<Post>) -> Self {
        Self {
            posts,
            damage: Vec::new(),
            lost: Vec::new(),
            unlisted: Vec::new(),
            refused: Vec::new(),
            corrupt_primary: Vec::new(),
            headerless: Vec::new(),
            servers: 1,
            short: Vec::new(),
            crc_wrong: Vec::new(),
            declares: Vec::new(),
            password: None,
            global_passwords: Vec::new(),
            article: ARTICLE,
            backwards: false,
            settle: false,
            batch: 0,
            together: Vec::new(),
            reversed: Vec::new(),
            first: Vec::new(),
            uuencoded: false,
            leased: false,
            limits: None,
            unacceptable: None,
            action: None,
            policy_corrupt: false,
            holds: None,
            profile: None,
            metadata: Vec::new(),
            stat_servers: std::sync::Mutex::new(Vec::new()),
            setup: None,
            heard: None,
            headers: Vec::new(),
            foreign: Vec::new(),
            foreign_header: Vec::new(),
        }
    }

    // The article at `ordinal` of `file` is served as another post's.
    pub fn foreign(mut self, file: u32, ordinal: u32) -> Self {
        self.foreign.push((file, ordinal));
        self
    }

    // The article at `ordinal` of `file` is served with its own body under
    // another post's header.
    pub fn foreign_header(mut self, file: u32, ordinal: u32) -> Self {
        self.foreign_header.push((file, ordinal));
        self
    }

    // Every article of `file` names it `name` in its header.
    pub fn header(self, file: u32, name: &str) -> Self {
        self.header_from(file, 0, name)
    }

    // The articles of `file` from `ordinal` on name it `name` in their
    // header.
    pub fn header_from(mut self, file: u32, ordinal: u32, name: &str) -> Self {
        self.headers.push((file, ordinal, name.to_string()));
        self
    }

    // The name the header of an article of `file` at `ordinal` gives it.
    pub(in crate::pipeline::tests) fn header_of(&self, file: u32, ordinal: u32) -> &str {
        self.headers
            .iter()
            .filter(|(at, from, _)| *at == file && *from <= ordinal)
            .max_by_key(|(_, from, _)| *from)
            .map_or(self.posts[file as usize].name.as_str(), |(.., name)| name)
    }

    // Everything a run says is told to `heard` first.
    pub(in crate::pipeline::tests) fn heard(
        mut self,
        heard: impl Fn(&crate::pipeline::graph::engine::Said) + Send + Sync + 'static,
    ) -> Self {
        self.heard = Some(std::sync::Arc::new(heard));
        self
    }

    pub fn on_run_message(
        self,
        heard: impl Fn(weaver_graph::NodeId, u64, String) + Send + Sync + 'static,
    ) -> Self {
        self.heard(move |said| heard(said.node, said.generation, format!("{:?}", said.heard)))
    }

    pub fn on_repair(
        self,
        repaired: impl Fn(weaver_graph::NodeId) + Send + Sync + 'static,
    ) -> Self {
        use crate::pipeline::graph::engine::Heard;
        self.heard(move |said| {
            if matches!(said.heard, Heard::Repaired(_)) {
                repaired(said.node);
            }
        })
    }

    // Every pipeline the scenario prepares is set this way, last.
    pub fn setup(mut self, setup: impl Fn(&mut Pipeline) + Send + Sync + 'static) -> Self {
        self.setup = Some(std::sync::Arc::new(setup));
        self
    }

    // The operator chose this hardware profile.
    pub fn profile(mut self, profile: crate::runtime::HardwareProfile) -> Self {
        self.profile = Some(profile);
        self
    }

    // The job carries this metadata entry, as a reprocess or a client sets it.
    pub fn metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.push((key.to_string(), value.to_string()));
        self
    }

    // The stored settings are present but do not parse, on every pipeline
    // that runs the job.
    pub fn policy_corrupt(mut self) -> Self {
        self.policy_corrupt = true;
        self
    }

    // The job runs under these extraction limits.
    pub fn limits(mut self, limits: ExtractionLimits) -> Self {
        self.limits = Some(limits);
        self
    }

    // The settings refuse these extensions.
    pub fn unacceptable(mut self, extensions: &[&str]) -> Self {
        self.unacceptable = Some(extensions.iter().map(|ext| ext.to_string()).collect());
        self
    }

    // A file with one of those extensions fails the job, the action that is
    // not the default.
    pub fn failing_job(mut self) -> Self {
        self.action = Some(crate::post_processing::model::UnwantedExtensionAction::FailJob);
        self
    }

    // What the scenario sets on a pipeline before its job is admitted or
    // restored: its servers, the settings' passwords and policy, and the
    // limits. A scenario whose primary serves some articles damaged has a
    // backup that serves them whole, on every pipeline that runs the job,
    // a restarted one too.
    pub fn prepare(&self, pipeline: &mut Pipeline) {
        // A health probe asks the scenario's servers on the download lanes,
        // and each holds every article but the ones the scenario lost.
        let missing = self
            .lost
            .iter()
            .map(|(file, ordinal)| format!("lantern-{file}-{ordinal}@example.com"))
            .collect();
        let servers = self
            .headerless
            .iter()
            .map(|(.., server)| server + 1)
            .chain([
                self.servers,
                if self.corrupt_primary.is_empty() {
                    1
                } else {
                    2
                },
            ])
            .max()
            .unwrap_or(1);
        let server = stat_server(missing);
        pipeline.nntp = std::sync::Arc::new(scenario_client(servers, server.port));
        self.stat_servers.lock().unwrap().push(server);
        if !self.global_passwords.is_empty() {
            pipeline
                .db
                .save_archive_password_settings(Some(self.global_passwords.clone()), None)
                .unwrap();
        }
        if let Some(extensions) = &self.unacceptable {
            pipeline
                .db
                .save_unacceptable_extensions(extensions.clone())
                .unwrap();
            // A save forgets whether any script could run; the runtime works
            // it out at startup, so the job's scripts gate does not wait.
            crate::post_processing::events::refresh_admission_hint(&pipeline.db).unwrap();
        }
        if let Some(action) = self.action {
            pipeline.db.save_unwanted_extension_action(action).unwrap();
        }
        if let Some(limits) = &self.limits {
            pipeline.extraction_limits = std::sync::Arc::new(limits.clone());
        }
        if let Some(budgets) = self.holds {
            pipeline.graph_holds =
                std::sync::Arc::new(weaver_graph::holds::HoldsAccountant::new(budgets));
        }
        if let Some(profile) = self.profile {
            pipeline.set_configured_hardware_profile(profile);
        }
        if self.policy_corrupt {
            pipeline
                .db
                .set_setting(
                    crate::post_processing::settings::SETTINGS_KEY,
                    CORRUPT_POLICY,
                )
                .unwrap();
        }
        if let Some(setup) = &self.setup {
            setup(pipeline);
        }
    }

    // The job's holds run under these budgets.
    pub fn holds(mut self, budgets: weaver_graph::holds::HoldBudgets) -> Self {
        self.holds = Some(budgets);
        self
    }

    // Whatever the download queue hands out at once arrives in reverse.
    pub fn backwards(mut self) -> Self {
        self.backwards = true;
        self
    }

    // Every checkpoint a step waits on commits before the next article: see
    // `settle`.
    pub fn settled(mut self) -> Self {
        self.settle = true;
        self
    }

    // The queue hands out `articles` at a time, served in the order handed
    // out.
    pub fn batched(mut self, articles: usize) -> Self {
        self.batch = articles;
        self
    }

    // Whatever the download queue hands out at once arrives as workers
    // fetching `posts` side by side: the first article of each, in this
    // order, then the second of each, and so on.
    pub fn together(mut self, posts: &[u32]) -> Self {
        self.together = posts.to_vec();
        self
    }

    // Whatever the queue holds of `posts` arrives at once, last first, from
    // the first time it hands one of them out.
    pub fn reversed(mut self, posts: &[u32]) -> Self {
        self.reversed = posts.to_vec();
        self
    }

    // A restarted run serves these articles before any other, in this
    // order, so a restart lands between two of them.
    pub fn first(mut self, articles: &[(u32, u32)]) -> Self {
        self.first = articles.to_vec();
        self
    }

    // Every article posted uuencoded rather than yEnc.
    pub fn uuencoded(mut self) -> Self {
        self.uuencoded = true;
        self
    }

    // Every batch leased by the download scheduler, served last article
    // first, each cut at the slice sizes its lease was taken under.
    pub fn leased(mut self) -> Self {
        self.leased = true;
        self
    }

    // Every post cut into articles of `size` bytes instead of ARTICLE.
    pub fn framed(mut self, size: usize) -> Self {
        self.article = size;
        self
    }

    // The password the job is submitted with.
    pub fn password(mut self, password: &str) -> Self {
        self.password = Some(password.to_string());
        self
    }

    // A password the settings carry, not the job: the graph engine offers it
    // when an object asks.
    pub fn global_password(mut self, password: &str) -> Self {
        self.global_passwords.push(password.to_string());
        self
    }

    pub fn damaged(mut self, file: u32, ordinal: u32) -> Self {
        self.damage.push((file, ordinal));
        self
    }

    pub fn lost(mut self, file: u32, ordinal: u32) -> Self {
        self.lost.push((file, ordinal));
        self
    }

    // The post's article at `posted` in the file is absent from the NZB.
    pub fn unlisted(mut self, file: u32, posted: u32) -> Self {
        self.unlisted.push((file, posted));
        self
    }

    // Where in the post the file's listed article `ordinal` lies: its
    // ordinal, past every article before it the NZB does not list.
    pub fn posted(&self, file: u32, ordinal: u32) -> u32 {
        (0..)
            .filter(|posted| !self.unlisted.contains(&(file, *posted)))
            .nth(ordinal as usize)
            .unwrap()
    }

    // The article decodes `by` bytes short of the part end it declares.
    pub fn short(mut self, file: u32, ordinal: u32, by: usize) -> Self {
        self.short.push((file, ordinal, by));
        self
    }

    // The article's bytes are whole and its checksum is wrong.
    pub fn crc_wrong(mut self, file: u32, ordinal: u32) -> Self {
        self.crc_wrong.push((file, ordinal));
        self
    }

    // Every article of the post declares a file of `size` bytes.
    pub fn declares(mut self, file: u32, size: u64) -> Self {
        self.declares.push((file, size));
        self
    }

    pub fn was_refused(&self, article: &(u32, u32)) -> bool {
        self.refused.contains(article)
    }

    pub fn refused_once(mut self, file: u32, ordinal: u32) -> Self {
        self.refused.push((file, ordinal));
        self
    }

    // Whether `server` answers the article with damaged bytes.
    pub(in crate::pipeline::tests) fn spoils(&self, server: usize, article: (u32, u32)) -> bool {
        self.damage.contains(&article) || (server == 0 && self.corrupt_primary.contains(&article))
    }

    pub fn corrupt_on_primary(mut self, file: u32, ordinal: u32) -> Self {
        self.corrupt_primary.push((file, ordinal));
        self
    }

    pub fn headerless_on(mut self, file: u32, ordinal: u32, server: usize) -> Self {
        self.headerless.push((file, ordinal, server));
        self
    }

    pub fn servers(mut self, servers: usize) -> Self {
        self.servers = servers;
        self
    }

    pub fn spec(&self) -> JobSpec {
        let mut spec = JobSpec {
            password: self.password.clone(),
            ..spec(JOB_NAME, &self.posts, self.article)
        };
        spec.metadata.extend(self.metadata.iter().cloned());
        // An NZB that lacks an article numbers the ones it lists densely,
        // each keeping its own article number, as the NZB intake does.
        for (index, file) in spec.files.iter_mut().enumerate() {
            let index = index as u32;
            file.segments
                .retain(|segment| !self.unlisted.contains(&(index, segment.ordinal)));
            for (ordinal, segment) in file.segments.iter_mut().enumerate() {
                segment.ordinal = ordinal as u32;
                segment.message_ids = crate::jobs::MessageIds::new(vec![format!(
                    "lantern-{index}-{ordinal}@example.com"
                )]);
            }
        }
        spec.total_bytes = spec
            .files
            .iter()
            .flat_map(|file| &file.segments)
            .map(|segment| u64::from(segment.bytes))
            .sum();
        spec
    }
}

// The job `posts` make posted under `name`, each cut into articles of
// `article` bytes.
pub fn spec(name: &str, posts: &[Post], article: usize) -> JobSpec {
    // A file with no article is no file of a job: a post never lists one.
    assert!(
        posts.iter().all(|post| !post.bytes.is_empty()),
        "a post with no article"
    );
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: posts.iter().map(|post| post.bytes.len() as u64).sum(),
        category: None,
        metadata: vec![],
        files: posts
            .iter()
            .enumerate()
            .map(|(index, post)| FileSpec {
                filename: post.name.clone(),
                role: post.role.clone(),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: post
                    .articles(article)
                    .enumerate()
                    .map(|(ordinal, bytes)| SegmentSpec {
                        ordinal: ordinal as u32,
                        article_number: ordinal as u32,
                        bytes: bytes.len() as u32,
                        message_ids: crate::jobs::MessageIds::new(vec![format!(
                            "lantern-{index}-{ordinal}@example.com"
                        )]),
                    })
                    .collect(),
            })
            .collect(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Claim {
    // Delivered at its length, every byte as posted.
    Complete,
    // Delivered at its length, some bytes not as posted.
    Damaged,
    // Not delivered, or delivered short.
    Missing,
}

// The graph job as last seen before it finished.
#[derive(Clone, Debug, Default)]
pub struct GraphView {
    // Payload members by output name, read off the namespace's marks.
    pub claims: BTreeMap<String, Claim>,
    pub blocks_reachable: u64,
    // The damaged slices every set last reported, as the history keeps them.
    pub damaged_slices: u64,
    pub outcomes: Vec<Option<RepairOutcome>>,
    // What each recovery object last ruled, by the kind of object, in the
    // order of `outcomes`.
    pub rulings: Vec<(String, Option<RepairOutcome>)>,
    // The kind of every object the tree held at any look, by its name.
    pub kinds: BTreeSet<String>,
    // How many objects the job had sent out for their work, at the last
    // look.
    pub loans: u64,
    // How many password candidates each node was offered, at the last look.
    pub offered: BTreeMap<weaver_graph::NodeId, usize>,
    // The files the job hands over at every depth, by output name.
    pub delivery: Delivery,
    // Whether a failure of the job, at the last look, is its post's: what
    // queue scripts hear as a deletion for health.
    pub health: bool,
    // What the job's checkpoints asked the database to write, at the last
    // look.
    pub(in crate::pipeline::tests) checkpoints: crate::pipeline::graph::checkpoint::Tally,
    // The most the live namespaces objects produced kept resident for their
    // members, with the name ledger for the parts of its paths, at any
    // look, summed from the tree.
    pub resident: u64,
    // The most every object's envelopes kept at any look, over every
    // volume: the bytes the job wrote for its runs to read.
    pub shelved: u64,
    // The most each object's envelopes kept at any look, by node, with the
    // kind of object it was.
    pub shelved_by: BTreeMap<weaver_graph::NodeId, (String, u64)>,
    // Every file each object's envelopes lay in at any look, by node.
    pub shelf_files: BTreeMap<weaver_graph::NodeId, BTreeSet<std::path::PathBuf>>,
    // The time the looks themselves took, which a run's time is not.
    pub observed: std::time::Duration,
    // What the job's report showed at each look, its status and whether it
    // was fetching repair data, each change once.
    pub shown: Vec<(JobStatus, bool)>,
    // The job's report at the last look.
    pub report: Option<crate::jobs::handle::JobInfo>,
    // The bytes still to come and the bytes wanted the job's report showed
    // at each look, each change once.
    pub remaining: Vec<(u64, u64)>,
    // Across each restart: the bytes still to come and wanted the report
    // showed as the pipeline went down, and as the new one restored the job.
    pub restored_remaining: Vec<((u64, u64), (u64, u64))>,
    // The restarts after which the restored job had a run out and articles
    // queued to fetch at once: it downloads while its runs decode again.
    pub fetching_while_running: usize,
    // At each look, for every root member a set covers: its name, whether
    // its sets proved it (every one covering it ruled clean or repaired and
    // none is out), and whether the root still keeps its checksums.
    pub sealing: Vec<Vec<(String, bool, bool)>>,
    // The most the job's working directory held at any look, every file
    // under it by its length.
    pub scratch: u64,
    // The most the job's holds kept at any look, in memory, on scratch and
    // on file, over every namespace.
    pub held: u64,
    // Every name a root member carried at any look.
    pub names: BTreeSet<String>,
    // At each look that differed from the one before: what a run could read
    // off every object's envelopes then, and the members objects announced.
    pub shelf_looks: Vec<ShelfLook>,
    // Every member file below the root, by node and member: each file it
    // lay in at any look, by inode, and whether it was ever seen shorter
    // than at a look before. A member written again is a new file.
    pub member_files: BTreeMap<(weaver_graph::NodeId, weaver_graph::MemberId), MemberFile>,
}

// The file a path names: its inode, where the platform has one.
#[cfg(unix)]
fn file_id(meta: &std::fs::Metadata) -> u64 {
    std::os::unix::fs::MetadataExt::ino(meta)
}

#[cfg(not(unix))]
fn file_id(_meta: &std::fs::Metadata) -> u64 {
    0
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemberFile {
    pub inodes: BTreeSet<u64>,
    pub longest: u64,
    pub shrank: bool,
}

// What the shelves held at one look: every envelope's runs with how they
// arrived, by node and volume, and the names root members carried. A run
// reads only the verified ones.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ShelfLook {
    pub kept: BTreeMap<
        (weaver_graph::NodeId, u32),
        Vec<(weaver_graph::ByteRange, weaver_graph::Provenance)>,
    >,
    pub names: BTreeSet<String>,
}

impl GraphView {
    pub fn checkpoint_counts(&self) -> (usize, usize, usize) {
        (
            self.checkpoints.checkpoints,
            self.checkpoints.facts,
            self.checkpoints.rows,
        )
    }

    // The writes the job's checkpoints took that changed a member's output
    // name or the name its file sits at, and the member rows they changed
    // so, at the last look.
    pub fn naming_writes(&self) -> (usize, usize) {
        (self.checkpoints.naming_writes, self.checkpoints.naming_rows)
    }

    pub fn checkpoint_debug(&self) -> &impl std::fmt::Debug {
        &self.checkpoints
    }

    pub fn withheld_error(&self) -> Option<String> {
        self.delivery.withheld_error()
    }

    pub fn has_withheld_files(&self) -> bool {
        !self.delivery.withheld.is_empty()
    }

    pub fn member_crc32_by_path(&self) -> std::collections::HashMap<String, u32> {
        crate::pipeline::completion::finalize::member_crc32_by_path(&self.delivery.files)
    }

    pub fn delivery_debug(&self) -> &impl std::fmt::Debug {
        &self.delivery
    }

    pub fn whole_files(&self) -> &[String] {
        &self.delivery.whole
    }

    pub fn damaged_files(&self) -> &[String] {
        &self.delivery.damaged
    }
    pub fn unchecked_files(&self) -> &[String] {
        &self.delivery.unchecked
    }
}

#[derive(Debug)]
pub struct Outcome {
    pub status: Option<JobStatus>,
    pub failure: Option<String>,
    pub failure_kind: Option<HistoryFailureKind>,
    // The reason the job's history row keeps: a failed job's error, or why
    // a partial one is short.
    pub history_error: Option<String>,
    pub files: BTreeMap<String, Vec<u8>>,
    pub retained_files: BTreeMap<String, Vec<u8>>,
    // Every directory delivered with nothing in it, by its path and a
    // trailing slash.
    pub empty: BTreeSet<String>,
    pub claims: BTreeMap<String, Claim>,
    pub requests: BTreeMap<(u32, u32), usize>,
    // Every article in the order it was asked for.
    pub order: Vec<(u32, u32)>,
    pub reads: u64,
    pub graph: GraphView,
    // Every pipeline event the job's run sent, in order.
    pub events: Vec<PipelineEvent>,
    // The extraction refusals the run counted, in the metric's reason order.
    pub rejections: [u64; 9],
    // The pipeline's metrics as the run left them.
    pub metrics: weaver_server_base::operations::metrics::MetricsSnapshot,
    // The password the job's history says an archive opened with.
    pub validated_password: Option<String>,
    // The metadata the job's history row keeps, as stored.
    pub history_metadata: Option<String>,
    // The download summary the job's history row keeps, as stored.
    pub download_summary: Option<String>,
    // The job's history row, as stored.
    pub history: Option<crate::JobHistoryRow>,
    // How many times each job stage's duration was observed, by stage name.
    pub stages_timed: Vec<(&'static str, u64)>,
    // The job's report as the run left it: its finished record once it
    // has finished.
    pub report: Option<crate::jobs::handle::JobInfo>,
}

// The kind of every object a graph run's tree held, by its name, sorted.
pub fn kinds(graph: &Outcome) -> Vec<&str> {
    graph.graph().kinds.iter().map(String::as_str).collect()
}

impl Outcome {
    pub fn refetched(&self) -> Vec<(u32, u32)> {
        self.requests
            .iter()
            .filter(|(_, count)| **count > 1)
            .map(|(article, _)| *article)
            .collect()
    }

    pub fn completed(&self) -> bool {
        self.status == Some(JobStatus::Complete)
    }

    pub fn graph(&self) -> &GraphView {
        &self.graph
    }
}

// The repair and terminal events the run sent, in order, as words a row
// can compare: `RepairComplete <slices>`, `JobCompleted`, `JobFailed`.
pub fn repair_and_terminal_events(outcome: &Outcome) -> Vec<String> {
    outcome
        .events
        .iter()
        .filter_map(|event| match event {
            PipelineEvent::RepairComplete {
                job_id,
                slices_repaired,
            } if *job_id == JOB => Some(format!("RepairComplete {slices_repaired}")),
            PipelineEvent::JobCompleted { job_id } if *job_id == JOB => {
                Some("JobCompleted".to_string())
            }
            PipelineEvent::JobFailed { job_id, .. } if *job_id == JOB => {
                Some("JobFailed".to_string())
            }
            _ => None,
        })
        .collect()
}

// Every article was asked for once.
pub fn assert_fetched_once(outcome: &Outcome) {
    assert!(
        outcome.refetched().is_empty(),
        "asked for an article twice: {:?}",
        outcome.refetched()
    );
}

// A run whose gate rounds stopped changing anything before the job
// finished: the status it sat at, and the pipeline's account of the job.
#[derive(Debug)]
pub struct Stall {
    pub status: Option<JobStatus>,
    pub state: String,
}

pub async fn run(scenario: &Scenario) -> Outcome {
    try_run(scenario)
        .await
        .unwrap_or_else(|stall| panic!("job stalled at {:?}: {}", stall.status, stall.state))
}

// A fresh pipeline with the scenario's job admitted, nothing asked yet.
pub async fn admit(scenario: &Scenario) -> (tempfile::TempDir, Pipeline, std::path::PathBuf) {
    admit_with(scenario, |_, _| {}).await
}

// [`admit`], with `setup` given the pipeline and its directory before the
// job is admitted.
pub(in crate::pipeline::tests) async fn admit_with(
    scenario: &Scenario,
    setup: impl FnOnce(&mut Pipeline, &Path),
) -> (tempfile::TempDir, Pipeline, std::path::PathBuf) {
    let temp_dir = tempfile::tempdir().unwrap();
    // One server: an article it serves damaged has nowhere else to come
    // from, so the job gives it up for repair at once.
    let backlog_base_bytes = 16 * 1024 * 1024;
    let (mut pipeline, _, complete_dir) =
        new_direct_pipeline_with_backlog(&temp_dir, backlog_base_bytes, 1).await;
    scenario.prepare(&mut pipeline);
    setup(&mut pipeline, temp_dir.path());
    insert_active_job(&mut pipeline, JOB, scenario.spec()).await;
    (temp_dir, pipeline, complete_dir)
}

// A run that may stall, for a row that asserts the job does.
pub async fn try_run(scenario: &Scenario) -> Result<Outcome, Stall> {
    try_run_then(scenario, async |_, _| {}).await
}

// [`run`], with `after` given the pipeline and the complete directory once
// the job has finished and its outcome is read, before the pipeline goes.
pub async fn run_then(
    scenario: &Scenario,
    after: impl AsyncFnOnce(&mut Pipeline, &Path),
) -> Outcome {
    try_run_then(scenario, after)
        .await
        .unwrap_or_else(|stall| panic!("job stalled at {:?}: {}", stall.status, stall.state))
}

pub async fn run_after(scenario: &Scenario, after: impl FnOnce(&Path)) -> Outcome {
    try_run_then(scenario, async |_, complete_dir| after(complete_dir))
        .await
        .unwrap_or_else(|stall| panic!("job stalled at {:?}: {}", stall.status, stall.state))
}

async fn try_run_then(
    scenario: &Scenario,
    after: impl AsyncFnOnce(&mut Pipeline, &Path),
) -> Result<Outcome, Stall> {
    let (_temp_dir, mut pipeline, complete_dir) = admit(scenario).await;
    // Events the run sends are heard from admission on; nothing sent before
    // the job was admitted belongs to it.
    let (heard, kept) = hear(pipeline.event_tx.subscribe());
    let mut refused = BTreeSet::new();
    let mut requests = BTreeMap::new();
    let mut order = Vec::new();
    let mut graph = GraphView::default();
    let mut reads = 0;
    let mut stalled = None;
    loop {
        while let Some((batch, plan)) = handed_out(&mut pipeline, scenario) {
            for work in batch {
                let article = (
                    work.segment_id.file_id.file_index,
                    work.segment_id.segment_number,
                );
                *requests.entry(article).or_insert(0usize) += 1;
                order.push(article);
                if scenario.refused.contains(&article) && refused.insert(article) {
                    // Rebuilt as the worker's retry rebuilds it: the retry
                    // carries no promotion of its own, so where it is queued is
                    // the job's answer to whether anything waits on it.
                    let mut retry = work;
                    retry.retry_count += 1;
                    retry.completion_critical = false;
                    pipeline.enqueue_download_work(retry);
                    continue;
                }
                serve_from(
                    &mut pipeline,
                    JOB,
                    scenario,
                    article,
                    work.exclude_servers,
                    plan.as_ref(),
                )
                .await;
                hear_probe(&mut pipeline).await;
                observe(&pipeline, &mut graph, &mut reads);
                while scenario.settle
                    && (pipeline
                        .graph_jobs
                        .get(&JOB)
                        .is_some_and(|job| job.awaits_mail())
                        || pipeline.moves_awaiting_disk.contains_key(&JOB))
                {
                    let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
                    pipeline.graph_run_said(said);
                    observe(&pipeline, &mut graph, &mut reads);
                }
            }
        }
        if finished(&pipeline) {
            break;
        }
        let before = fingerprint(&pipeline, &requests);
        if pipeline.jobs.contains_key(&JOB) {
            pipeline.check_job_completion(JOB).await;
        }
        pump_pipeline_runtime_queues(&mut pipeline).await;
        observe(&pipeline, &mut graph, &mut reads);
        if finished(&pipeline) {
            break;
        }
        // A run or a repair out on a thread speaks through the pipeline's mail, as the
        // orchestrator hears it: what it says is the job's next move. So
        // does a volume's writer, to a delivery that waits for it.
        if pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.awaits_mail())
            || pipeline.moves_awaiting_disk.contains_key(&JOB)
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            if let Some(heard) = &scenario.heard {
                heard(&said);
            }
            pipeline.graph_run_said(said);
            observe(&pipeline, &mut graph, &mut reads);
            continue;
        }
        // A job that waits for room on a disk goes on at a reading, as the
        // orchestrator hears one, after it reconciles the waits begun.
        pipeline.reconcile_disk_waits();
        if pipeline.waits_for_disk_room() {
            pipeline.storage_capacity.readings().notified().await;
            pipeline.disk_reading();
            observe(&pipeline, &mut graph, &mut reads);
            continue;
        }
        if take_retry(&mut pipeline).await {
            continue;
        }
        let after = fingerprint(&pipeline, &requests);
        if before == after && stalled.replace(after.clone()) == Some(after.clone()) {
            let status = job_status_for_assert(&pipeline, JOB);
            let state = after;
            retire_pipeline_database(pipeline).await;
            return Err(Stall { status, state });
        }
    }

    super::super::settle_failed_releases(&mut pipeline).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let failure = match &status {
        Some(JobStatus::Failed { error }) => Some(error.clone()),
        _ => None,
    };
    pipeline.db.flush_write_queue().await.unwrap();
    let failure_kind = pipeline
        .db
        .nzbget_history_states(&[JOB.0])
        .unwrap()
        .get(&JOB.0)
        .and_then(|state| state.failure_kind);
    let history_error = pipeline
        .db
        .get_job_history(JOB.0)
        .unwrap()
        .and_then(|row| row.error_message);
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    let mut retained_files = BTreeMap::new();
    collect(
        &pipeline.intermediate_dir,
        &pipeline.intermediate_dir,
        &mut retained_files,
    );
    let mut empty = BTreeSet::new();
    collect_empty(&complete_dir, &complete_dir, &mut empty);
    let claims = scenario
        .posts
        .iter()
        .filter(|post| post.role.counts_toward_health())
        .map(|post| (post.name.clone(), delivered_claim(&files, post)))
        .collect();
    drop(heard);
    let events = kept.await.expect("every event the run sent is kept");
    let rejections = pipeline.metrics.extraction_rejections();
    let metrics = pipeline.metrics.raw_snapshot();
    let validated_password = pipeline.db.validated_archive_password(JOB.0).unwrap();
    let history = pipeline.db.get_job_history(JOB.0).unwrap();
    let history_metadata = history.as_ref().and_then(|row| row.metadata.clone());
    let download_summary = pipeline.db.history_download_summary_text(JOB).unwrap();
    let stages_timed = pipeline
        .metrics
        .job_lifecycle
        .snapshot()
        .stage_duration
        .into_iter()
        .map(|(stage, histogram)| (stage, histogram.count))
        .collect();
    let report = match pipeline
        .finished_jobs()
        .await
        .into_iter()
        .find(|job| job.job_id == JOB)
    {
        Some(finished) => Some(finished),
        None => pipeline.job_report(JOB),
    };
    after(&mut pipeline, &complete_dir).await;
    retire_pipeline_database(pipeline).await;
    Ok(Outcome {
        status,
        failure,
        failure_kind,
        history_error,
        files,
        retained_files,
        empty,
        claims,
        requests,
        order,
        reads,
        graph,
        events,
        rejections,
        metrics,
        validated_password,
        history_metadata,
        download_summary,
        history,
        stages_timed,
        report,
    })
}

// Every event the run sends, kept as it is sent: a task beside the run
// hears each one as the run yields, so the channel's bound never drops one
// however many the run sends. Once the returned sender is dropped, what is
// still in the channel is kept and the list is handed back.
fn hear(
    mut sent: tokio::sync::broadcast::Receiver<PipelineEvent>,
) -> (
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Vec<PipelineEvent>>,
) {
    use tokio::sync::broadcast::error::{RecvError, TryRecvError};
    let (heard, mut done) = tokio::sync::oneshot::channel::<()>();
    let kept = tokio::spawn(async move {
        let mut events = Vec::new();
        loop {
            tokio::select! {
                biased;
                event = sent.recv() => match event {
                    Ok(event) => events.push(event),
                    Err(RecvError::Closed) => return events,
                    Err(error) => panic!("the run's events were not all kept: {error}"),
                },
                _ = &mut done => break,
            }
        }
        loop {
            match sent.try_recv() {
                Ok(event) => events.push(event),
                Err(TryRecvError::Empty | TryRecvError::Closed) => return events,
                Err(error) => panic!("the run's events were not all kept: {error}"),
            }
        }
    });
    (heard, kept)
}

// A graph run whose completion gate runs only when the pipeline itself
// scheduled it, as the orchestrator's loop runs it: no idle look is taken.
// Every article is answered one at a time and ends as a decode ends, each
// check a step scheduled is run, and a live run is heard. Returns where the
// job stands once nothing is queued, scheduled or running: the recovery
// outcomes as last seen, and whether its delivery move is out.
pub async fn run_unprompted(scenario: &Scenario) -> (Vec<Option<RepairOutcome>>, bool) {
    fn look(pipeline: &Pipeline, outcomes: &mut Vec<Option<RepairOutcome>>) {
        if let Some(job) = pipeline.graph_jobs.get(&JOB) {
            *outcomes = job
                .recovery_summary()
                .iter()
                .map(|set| set.outcome)
                .collect();
        }
    }
    let (_temp_dir, mut pipeline, _) = admit(scenario).await;
    let mut outcomes = Vec::new();
    loop {
        while let Some(work) = pipeline
            .jobs
            .get_mut(&JOB)
            .and_then(|state| state.articles.active_mut().unwrap().download_queue.pop())
        {
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            serve(&mut pipeline, scenario, article).await;
            // The tail of every decode: the last one ends the download pass.
            if pipeline.jobs.contains_key(&JOB) && pipeline.job_decode_stage_drained(JOB) {
                pipeline.maybe_finish_download_pass(JOB);
            }
            look(&pipeline, &mut outcomes);
            pump_pipeline_runtime_queues(&mut pipeline).await;
        }
        if let Some(queued) = pipeline.pending_completion_checks.pop_front() {
            look(&pipeline, &mut outcomes);
            pipeline.check_job_completion(queued).await;
            pump_pipeline_runtime_queues(&mut pipeline).await;
            continue;
        }
        if pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.awaits_mail())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            let replayed = matches!(
                &said.heard,
                crate::pipeline::graph::engine::Heard::Replayed(_)
            );
            pipeline.graph_run_said(said);
            if replayed {
                assert!(
                    pipeline.pending_completion_checks.contains(&JOB),
                    "replay acknowledgment must schedule completion without an idle look"
                );
            }
            look(&pipeline, &mut outcomes);
            continue;
        }
        break;
    }
    look(&pipeline, &mut outcomes);
    let moved = pipeline.inflight_moves.contains(&JOB) || finished(&pipeline);
    retire_pipeline_database(pipeline).await;
    (outcomes, moved)
}

// The next articles the download queue hands out: one, or with `batched`
// that many in the order handed out, or with `backwards` every one it
// holds, last first, or with `together` every one it holds, a
// turn of each named post at a time, or with `leased` the batch a lane
// leases, last first, with the slice sizes it was leased under. `None` once
// it is dry.
fn handed_out(
    pipeline: &mut Pipeline,
    scenario: &Scenario,
) -> Option<(Vec<DownloadWork>, Option<weaver_yenc::CheckpointPlan>)> {
    if scenario.leased {
        let lease = pipeline.lease_for_server_for_test(0)?;
        let mut works = lease.works;
        works.reverse();
        return Some((works, Some(lease.checkpoint_plan)));
    }
    // The scheduler hands a restored job nothing while its recovery reads
    // back what arrived of its volumes; the queue popped by hand
    // here holds back the same.
    if pipeline
        .graph_jobs
        .get(&JOB)
        .is_some_and(|graph| graph.rewalks_owed())
    {
        return None;
    }
    let crate::jobs::model::JobState { articles, spec, .. } = pipeline.jobs.get_mut(&JOB)?;
    let queue = &mut articles.active_mut().unwrap().download_queue;
    let all = scenario.backwards || !scenario.together.is_empty();
    let mut batch = Vec::new();
    if scenario.batch > 0 {
        while batch.len() < scenario.batch
            && let Some(work) = queue.pop()
        {
            batch.push(work.dispatched(spec));
        }
        return (!batch.is_empty()).then_some((batch, None));
    }
    while let Some(work) = queue.pop() {
        batch.push(work.dispatched(spec));
        if !all {
            break;
        }
    }
    let reversed = |file_index: u32| scenario.reversed.contains(&file_index);
    if !all
        && batch
            .first()
            .is_some_and(|work| reversed(work.segment_id.file_id.file_index))
    {
        batch.extend(
            queue
                .extract_matching(|work| reversed(work.segment_id.file_id.file_index))
                .into_iter()
                .map(|item| item.dispatched(spec)),
        );
        batch.sort_by_key(|work| {
            std::cmp::Reverse((
                work.segment_id.segment_number,
                work.segment_id.file_id.file_index,
            ))
        });
        return Some((batch, None));
    }
    if scenario.together.is_empty() {
        batch.reverse();
    } else {
        // An article's turn is its number in its post, then its post's
        // place in the order; a post not named comes after.
        batch.sort_by_key(|work| {
            let file = work.segment_id.file_id.file_index;
            let at = scenario.together.iter().position(|named| *named == file);
            (
                work.segment_id.segment_number,
                at.unwrap_or(scenario.together.len()),
            )
        });
    }
    (!batch.is_empty()).then_some((batch, None))
}

// A body a server spoiled is asked of the next server once the worker's
// backoff ends: the retry it scheduled is the job's next move. True when one
// was taken.
pub async fn take_retry(pipeline: &mut Pipeline) -> bool {
    if !pipeline
        .pending_retries_by_job
        .get(&JOB)
        .is_some_and(|pending| *pending > 0)
    {
        return false;
    }
    let retry = pipeline
        .retry_rx
        .recv()
        .await
        .expect("a scheduled retry is sent");
    pipeline.receive_retry_work(retry);
    true
}

// An ask as the worker serves it: the first server it does not exclude
// answers.
pub async fn serve_work(pipeline: &mut Pipeline, scenario: &Scenario, work: &DownloadWork) {
    let article = (
        work.segment_id.file_id.file_index,
        work.segment_id.segment_number,
    );
    serve_from(pipeline, JOB, scenario, article, work.exclude_servers, None).await;
}

// One article as the scenario's server answers it.
pub async fn serve(pipeline: &mut Pipeline, scenario: &Scenario, article: (u32, u32)) {
    serve_job(pipeline, JOB, scenario, article).await;
}

// [`serve`] for a job other than the run's own.
pub(in crate::pipeline::tests) async fn serve_job(
    pipeline: &mut Pipeline,
    job: JobId,
    scenario: &Scenario,
    article: (u32, u32),
) {
    serve_from(
        pipeline,
        job,
        scenario,
        article,
        weaver_nntp::ServerSet::EMPTY,
        None,
    )
    .await;
}

// [`serve_job`] for an ask that excludes `exclude`: the first server it
// leaves answers.
// The post another post's article comes from: an obfuscated file far larger
// than any of the scenario's, the article at a place past their ends.
const FOREIGN_NAME: &str = "6878f815a73f3004";
const FOREIGN_SIZE: u64 = 172_462_596;
const FOREIGN_AT: u64 = 100_000_000;
const FOREIGN_PART: u32 = 300;
const FOREIGN_TOTAL: u32 = 524;

async fn serve_from(
    pipeline: &mut Pipeline,
    job: JobId,
    scenario: &Scenario,
    (file, ordinal): (u32, u32),
    exclude: weaver_nntp::ServerSet,
    leased: Option<&weaver_yenc::CheckpointPlan>,
) {
    let server = (0..).find(|server| !exclude.contains(*server)).unwrap();
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id: job,
            file_index: file,
        },
        segment_number: ordinal,
    };
    if scenario.lost.contains(&(file, ordinal)) {
        pipeline.book_failed_segment(segment_id);
        return;
    }
    if scenario.headerless.contains(&(file, ordinal, server)) {
        // The body is the article's bytes as plain lines, with no `=ybegin`
        // line: the decoder refuses it, and the worker hears its error as it
        // hears a streamed decode's.
        let post = &scenario.posts[file as usize];
        let mut body: Vec<u8> = post
            .articles(scenario.article)
            .nth(ordinal as usize)
            .unwrap()
            .iter()
            .map(|byte| b'a' + byte % 26)
            .collect();
        body.extend_from_slice(b"\r\n");
        let mut output = vec![0u8; body.len()];
        let error = weaver_yenc::decode(&body, &mut output)
            .expect_err("a body with no =ybegin line does not decode");
        pipeline.handle_decode_failure(
            segment_id,
            &error.to_string(),
            crate::pipeline::PoolServers::stamped(pipeline.pool_generation, Some(server), exclude),
        );
        return;
    }
    let decoded = article_decoded(pipeline, job, scenario, (file, ordinal), server, leased);
    pipeline
        .handle_decode_success(
            decoded,
            SegmentSource {
                servers: crate::pipeline::PoolServers::stamped(
                    pipeline.pool_generation,
                    Some(server),
                    exclude,
                ),
            },
        )
        .await;
}

// The decode of the job's article `(file, ordinal)` as `server` answers it.
pub(in crate::pipeline::tests) fn article_decoded(
    pipeline: &Pipeline,
    job: JobId,
    scenario: &Scenario,
    (file, ordinal): (u32, u32),
    server: usize,
    leased: Option<&weaver_yenc::CheckpointPlan>,
) -> DecodeResult {
    let segment_id = SegmentId {
        file_id: NzbFileId {
            job_id: job,
            file_index: file,
        },
        segment_number: ordinal,
    };
    let intact = !scenario.spoils(server, (file, ordinal));
    let post = &scenario.posts[file as usize];
    // The article's part in the post, which its header declares.
    let part = scenario.posted(file, ordinal);
    let offset = part as usize * post.article(scenario.article);
    let posted = post.articles(scenario.article).nth(part as usize).unwrap();
    // Another post's article carries its own bytes, at its own place in its
    // own file.
    let genuine = scenario.foreign_header.contains(&(file, ordinal));
    let foreign = genuine || scenario.foreign.contains(&(file, ordinal));
    let alien: Vec<u8>;
    let (offset, posted) = if foreign {
        alien = match genuine {
            true => posted.to_vec(),
            false => posted.iter().map(|byte| byte ^ 0xa5).collect(),
        };
        (FOREIGN_AT as usize + offset, alien.as_slice())
    } else {
        (offset, posted)
    };
    // The checksum the article declares is the posted one, so damaged bytes
    // disagree with it.
    let crc_wrong = scenario.crc_wrong.contains(&(file, ordinal));
    let crc = par2_rs::checksum::crc32(posted) ^ u32::from(crc_wrong);
    let mut body = posted.to_vec();
    if !intact {
        body.iter_mut().take(8).for_each(|byte| *byte ^= 0x5a);
    }
    if scenario.uuencoded {
        // The body carries no part number, range or checksum; the last
        // article carries the `end` line.
        let ended = part as usize + 1 == post.articles(scenario.article).count();
        let encoding = SegmentEncoding::Uu(crate::pipeline::UuSegmentFacts {
            damaged: !intact,
            ended,
        });
        return DecodeResult {
            segment_id,
            raw_size: body.len() as u64,
            encoding,
            yenc_layout: YencLayoutAssertions {
                file_size: 0,
                part: None,
                total: None,
                begin: None,
                end: None,
            },
            crc_valid: true,
            part_crc_verified: false,
            part_crc: 0,
            truncation_suspected: false,
            expected_file_crc: None,
            data: DecodedChunk::from(body),
            yenc_name: if ordinal == 0 {
                scenario.header_of(file, ordinal).to_string()
            } else {
                String::new()
            },
            checkpoint_plan: weaver_yenc::CheckpointPlan::None,
            segments: Vec::new(),
        };
    }
    // The part end the header declares is the posted one, whatever of it
    // decoded.
    let declared = body.len() as u64;
    if let Some((.., by)) = scenario
        .short
        .iter()
        .find(|(at, number, _)| (*at, *number) == (file, ordinal))
    {
        body.truncate(body.len().saturating_sub(*by));
    }
    let len = body.len() as u64;
    let truncated = len < declared;
    let intact = intact && !truncated && !crc_wrong;
    // The decoder checksums the bytes it decoded, cut at the block
    // boundaries of the plan the article was leased under: its lease's, or
    // what the job knows now when it was handed out alone.
    let checkpoint_plan = leased
        .cloned()
        .unwrap_or_else(|| checkpoint_plan(pipeline, job));
    let mut checksums = weaver_yenc::SegmentedCrc32::new(offset as u64, checkpoint_plan.clone());
    checksums.update(&body);
    let (_, segments) = checksums.finish_article();
    DecodeResult {
        segment_id,
        raw_size: len,
        encoding: SegmentEncoding::Yenc,
        yenc_layout: YencLayoutAssertions {
            file_size: match foreign {
                true => FOREIGN_SIZE,
                false => scenario
                    .declares
                    .iter()
                    .find(|(at, _)| *at == file)
                    .map_or(post.bytes.len() as u64, |(_, size)| *size),
            },
            part: Some(part + 1 + u32::from(foreign) * FOREIGN_PART),
            total: Some(match foreign {
                true => FOREIGN_TOTAL,
                false => post.articles(scenario.article).count() as u32,
            }),
            begin: Some(offset as u64 + 1),
            end: Some(offset as u64 + declared),
        },
        crc_valid: intact,
        part_crc_verified: intact,
        part_crc: crc,
        truncation_suspected: truncated,
        expected_file_crc: None,
        data: DecodedChunk::from(body),
        yenc_name: match foreign {
            true => FOREIGN_NAME.to_string(),
            false => scenario.header_of(file, ordinal).to_string(),
        },
        checkpoint_plan,
        segments,
    }
}

// What a gate round can change: when none of it moved, nothing will.
fn fingerprint(pipeline: &Pipeline, requests: &BTreeMap<(u32, u32), usize>) -> String {
    let queued = pipeline.jobs.get(&JOB).map(|state| {
        (
            state.articles.active().unwrap().download_queue.len(),
            state.articles.active().unwrap().recovery_queue.len(),
            state.status.clone(),
        )
    });
    format!(
        "{queued:?} {} {} {}",
        pipeline.inflight_moves.contains(&JOB),
        requests.values().sum::<usize>(),
        pipeline.pending_completion_checks.len()
    )
}

// The graph job as it stands; a finished job has left the pipeline, so the
// last look before that is what the outcome keeps.
pub(in crate::pipeline::tests) fn observe(
    pipeline: &Pipeline,
    view: &mut GraphView,
    reads: &mut u64,
) {
    let (Some(job), Some(state)) = (pipeline.graph_jobs.get(&JOB), pipeline.jobs.get(&JOB)) else {
        return;
    };
    let looked = std::time::Instant::now();
    if let Some(report) = pipeline.job_report(JOB) {
        let shown = (report.status.clone(), report.fetching_repair_data);
        if view.shown.last() != Some(&shown) {
            view.shown.push(shown);
        }
        let remaining = (report.remaining_bytes, report.wanted_bytes);
        if view.remaining.last() != Some(&remaining) {
            view.remaining.push(remaining);
        }
        view.report = Some(report);
    }
    view.loans = view.loans.max(job.loans_lent());
    let tree = job.tree();
    if let Some(namespace) = tree.namespace(weaver_graph::NodeId::ROOT) {
        let sets = tree.recovery_nodes(weaver_graph::NodeId::ROOT);
        let writer = job.writer();
        let look = namespace
            .members()
            .filter(|member| writer.covering(tree, &sets, member.id).next().is_some())
            .map(|member| {
                (
                    member.name.clone(),
                    writer.proven_whole(tree, (weaver_graph::NodeId::ROOT, &sets), member.id),
                    job.checksums_held(member.id),
                )
            })
            .collect();
        view.sealing.push(look);
        view.names
            .extend(namespace.members().map(|member| member.name.clone()));
    }
    view.kinds.extend(
        tree.live()
            .into_iter()
            .filter_map(|node| tree.kind(node))
            .map(|kind| kind.to_string()),
    );
    view.offered = job.passwords_offered();
    view.scratch = view.scratch.max(tree_bytes(&state.working_dir));
    view.shelved = view.shelved.max(job.writer().kept_bytes());
    for (node, kept, files) in job.writer().kept_by_node() {
        let kind = tree
            .kind(node)
            .map(|kind| kind.to_string())
            .unwrap_or_default();
        let entry = view.shelved_by.entry(node).or_insert((kind, 0));
        entry.1 = entry.1.max(kept);
        view.shelf_files.entry(node).or_default().extend(files);
    }
    let mut look = ShelfLook::default();
    for node in job.writer().kept_nodes() {
        let Some(shelf) = job.writer().shelf_of(node) else {
            continue;
        };
        let state = shelf.lock();
        for (&envelope, kept) in &state.kept {
            let runs: Vec<_> = kept
                .coverage
                .runs()
                .map(|run| (run.range, run.provenance))
                .collect();
            if !runs.is_empty() {
                look.kept.insert((node, envelope), runs);
            }
        }
    }
    if !look.kept.is_empty() {
        look.names = tree
            .live()
            .into_iter()
            .filter(|&node| node != weaver_graph::NodeId::ROOT)
            .filter_map(|node| tree.namespace(node))
            .flat_map(|namespace| namespace.members().map(|member| member.name.clone()))
            .collect();
        if view.shelf_looks.last() != Some(&look) {
            view.shelf_looks.push(look);
        }
    }
    for node in job.writer().file_nodes() {
        if node == weaver_graph::NodeId::ROOT {
            continue;
        }
        for (member, path) in job.writer().files(node) {
            let Ok(meta) = std::fs::metadata(path) else {
                continue;
            };
            let seen = view.member_files.entry((node, member)).or_default();
            seen.inodes.insert(file_id(&meta));
            seen.shrank |= meta.len() < seen.longest;
            seen.longest = seen.longest.max(meta.len());
        }
    }
    view.checkpoints = job.pending().tally();
    view.delivery = pipeline.graph_delivery(JOB).unwrap_or_default();
    view.health = pipeline.graph_failure_is_health(JOB);
    let namespace = job.namespace();
    view.claims = state
        .spec
        .files
        .iter()
        .zip(0u32..)
        .filter(|(file, _)| file.role.counts_toward_health())
        // A posting folded into the member it repeats claims what that
        // member holds: its own record keeps only what the member took of it.
        .filter_map(|(_, index)| {
            let posting = namespace.get(weaver_graph::MemberId(index))?;
            Some((posting, namespace.get(namespace.resolve(posting.id))?))
        })
        .map(|(posting, member)| {
            let damaged = member.coverage.damaged_ranges().next().is_some();
            let claim = match member.length {
                Some(_) if member.is_complete() && !damaged => Claim::Complete,
                Some(_) if member.is_complete() => Claim::Damaged,
                _ => Claim::Missing,
            };
            (posting.output_name.clone(), claim)
        })
        .collect();
    let summary = job.recovery_summary();
    let rulings: Vec<_> = summary
        .iter()
        .filter_map(|set| tree.recovery(set.node))
        .flat_map(|recovery| {
            let mut sets = Vec::new();
            recovery.sets(&mut sets);
            sets
        })
        .collect();
    view.blocks_reachable = rulings.iter().map(|set| set.blocks_reachable).sum();
    view.damaged_slices = rulings.iter().map(|set| set.damaged_slices).sum();
    view.outcomes = summary.iter().map(|set| set.outcome).collect();
    view.rulings = summary
        .iter()
        .map(|set| (set.kind.to_string(), set.outcome))
        .collect();
    let resident = tree
        .live()
        .into_iter()
        .filter(|node| tree.volumes(*node).is_some())
        .filter_map(|node| tree.namespace(node))
        .flat_map(|namespace| {
            namespace
                .records()
                .map(|record| namespace.resident(record.id))
        })
        .sum::<u64>()
        + tree.names_resident();
    view.resident = view.resident.max(resident);
    let held = tree
        .live()
        .into_iter()
        .map(|node| job.writer().held_bytes(node))
        .sum();
    view.held = view.held.max(held);
    *reads = job.writer().read_backs();
    view.observed += looked.elapsed();
}

fn delivered_claim(files: &BTreeMap<String, Vec<u8>>, post: &Post) -> Claim {
    let delivered = files.iter().find(|(path, _)| {
        Path::new(path).file_name().and_then(|name| name.to_str()) == Some(post.name.as_str())
    });
    match delivered {
        Some((_, bytes)) if *bytes == post.bytes => Claim::Complete,
        Some((_, bytes)) if bytes.len() == post.bytes.len() => Claim::Damaged,
        _ => Claim::Missing,
    }
}

// The delivered files, without the marker every output directory carries,
// whose bytes name the run's own paths.
// Every file under `dir` by its length, links not followed.
fn tree_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .map(|entry| {
            let entry = entry.unwrap();
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                tree_bytes(&entry.path())
            } else {
                entry.metadata().map_or(0, |metadata| metadata.len())
            }
        })
        .sum()
}

pub fn collect(root: &Path, dir: &Path, files: &mut BTreeMap<String, Vec<u8>>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.unwrap().path();
        if path.file_name() == Some(std::ffi::OsStr::new(".weaver-output-dir")) {
            continue;
        }
        if path.is_dir() {
            collect(root, &path, files);
        } else {
            let name = delivered_name(path.strip_prefix(root).unwrap());
            files.insert(name, std::fs::read(&path).unwrap());
        }
    }
}

// The delivered directories that hold nothing, the marker aside. The working
// directories a run keeps beside the output, named with a leading
// `.weaver-`, are not delivered.
fn collect_empty(root: &Path, dir: &Path, empty: &mut BTreeSet<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut held = false;
    for entry in entries {
        let path = entry.unwrap().path();
        if path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with(".weaver-"))
        {
            continue;
        }
        held = true;
        if path.is_dir() {
            collect_empty(root, &path, empty);
        }
    }
    if !held && dir != root {
        let name = delivered_name(dir.strip_prefix(root).unwrap());
        empty.insert(format!("{name}/"));
    }
}

// A delivered path below the output root, named as the engine names it:
// its components joined by `/` on every host.
fn delivered_name(relative: &Path) -> String {
    relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

// Where a job stood while the server held every article of one post, and
// how it ended once the server let them go.
pub struct Held {
    // The job's report once nothing moved while the post was held.
    pub report: crate::JobInfo,
    // What its recovery sets had ruled then.
    pub outcomes: Vec<Option<RepairOutcome>>,
    // Its status, and whether it read as fetching repair data, at each
    // round after the release.
    pub after: Vec<(JobStatus, bool)>,
    pub status: Option<JobStatus>,
    pub files: BTreeMap<String, Vec<u8>>,
    // The most password candidates each node was offered at any look.
    pub offered: BTreeMap<weaver_graph::NodeId, usize>,
}

// Plays the server, holding every article of post `held` as a server holds
// an article a worker asked for: the worker has it out, and no answer comes.
// Once nothing moves the job is looked at; then the held articles are
// answered and the job runs to its end.
pub async fn run_holding(scenario: &Scenario, held: u32) -> Held {
    run_holding_where(scenario, |article| article.0 == held, |_| {}).await
}

// [`run_holding`], holding every article `held` names, and showing
// `at_hold` the pipeline once nothing moves while they are held.
pub async fn run_holding_where(
    scenario: &Scenario,
    held: impl Fn((u32, u32)) -> bool,
    at_hold: impl FnOnce(&Pipeline),
) -> Held {
    // Every article the queue hands out is served but the held ones, and
    // the job's checks, steps and mail run, until nothing is left to serve.
    async fn settle(
        pipeline: &mut Pipeline,
        scenario: &Scenario,
        held: &dyn Fn((u32, u32)) -> bool,
        out: &mut Vec<DownloadWork>,
    ) {
        loop {
            while let Some(work) = pipeline.jobs.get_mut(&JOB).and_then(|state| {
                let item = state.articles.active_mut().unwrap().download_queue.pop()?;
                Some(item.dispatched(&state.spec))
            }) {
                let article = (
                    work.segment_id.file_id.file_index,
                    work.segment_id.segment_number,
                );
                if held(article) {
                    *pipeline.articles_in_flight_by_job.entry(JOB).or_insert(0) += 1;
                    out.push(work);
                    continue;
                }
                serve(pipeline, scenario, article).await;
                if pipeline.job_decode_stage_drained(JOB) {
                    pipeline.maybe_finish_download_pass(JOB);
                }
            }
            if finished(pipeline) {
                return;
            }
            pipeline.check_job_completion(JOB).await;
            pump_pipeline_runtime_queues(pipeline).await;
            // A run that waits on its shelf for what is held says nothing
            // until it is let go: mail is awaited only from the rest, or
            // until every run waits so.
            if let Some(blocked) = pipeline
                .graph_jobs
                .get(&JOB)
                .filter(|graph| graph.awaits_mail_unless_blocked())
                .map(crate::pipeline::graph::job::GraphJob::blocked)
            {
                tokio::select! {
                    said = pipeline.graph_runs.recv() => {
                        let said = said.expect("a live run speaks");
                        if let Some(heard) = &scenario.heard {
                            heard(&said);
                        }
                        pipeline.graph_run_said(said);
                    }
                    () = blocked.notified() => {}
                }
                continue;
            }
            // What a run said before it came to wait on its shelf is heard
            // before the job is looked at.
            let waiting = pipeline.graph_runs.waiting();
            if !waiting.is_empty() {
                for said in waiting {
                    if let Some(heard) = &scenario.heard {
                        heard(&said);
                    }
                    pipeline.graph_run_said(said);
                }
                continue;
            }
            if pipeline.jobs.get(&JOB).is_none_or(|state| {
                state
                    .articles
                    .active()
                    .is_none_or(|active| active.download_queue.is_empty())
            }) {
                return;
            }
        }
    }
    // A round that leaves the job where it stood twice over is a stall.
    fn look(pipeline: &Pipeline) -> String {
        let report = pipeline.job_report(JOB);
        format!(
            "{:?} {:?} {:?} {}",
            report.as_ref().map(|report| &report.status),
            report.as_ref().map(|report| report.post_state),
            pipeline
                .graph_jobs
                .get(&JOB)
                .map(crate::pipeline::graph::job::GraphJob::recovery_summary),
            pipeline.inflight_moves.contains(&JOB),
        )
    }

    let (_temp_dir, mut pipeline, complete_dir) = admit(scenario).await;
    let mut out = Vec::new();
    settle(&mut pipeline, scenario, &held, &mut out).await;
    assert!(!out.is_empty(), "the held post was never asked for");
    at_hold(&pipeline);
    let report = pipeline
        .job_report(JOB)
        .expect("the job waits on the held post");
    let outcomes = pipeline.graph_jobs[&JOB]
        .recovery_summary()
        .iter()
        .map(|set| set.outcome)
        .collect();

    pipeline.articles_in_flight_by_job.insert(JOB, 0);
    for work in std::mem::take(&mut out) {
        let article = (
            work.segment_id.file_id.file_index,
            work.segment_id.segment_number,
        );
        serve(&mut pipeline, scenario, article).await;
    }
    if pipeline.job_decode_stage_drained(JOB) {
        pipeline.maybe_finish_download_pass(JOB);
    }
    let mut after = Vec::new();
    let mut stalled = None;
    let mut offered: BTreeMap<weaver_graph::NodeId, usize> = BTreeMap::new();
    while !finished(&pipeline) {
        if let Some(report) = pipeline.job_report(JOB) {
            after.push((report.status.clone(), report.fetching_repair_data));
        }
        if let Some(job) = pipeline.graph_jobs.get(&JOB) {
            for (node, count) in job.passwords_offered() {
                let most = offered.entry(node).or_default();
                *most = (*most).max(count);
            }
        }
        let before = look(&pipeline);
        settle(&mut pipeline, scenario, &held, &mut out).await;
        let now = look(&pipeline);
        if before == now && stalled.replace(now.clone()) == Some(now.clone()) {
            panic!("the job stalled: {now}");
        }
    }
    assert!(out.is_empty(), "the held post was asked for again");
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    retire_pipeline_database(pipeline).await;
    Held {
        report,
        outcomes,
        after,
        status,
        files,
        offered,
    }
}

pub async fn serve_and_observe_recovery(
    pipeline: &mut Pipeline,
    scenario: &Scenario,
    article: (u32, u32),
) -> Vec<Option<RepairOutcome>> {
    serve(pipeline, scenario, article).await;
    crate::pipeline::tests::hear_graph_mail(pipeline, JOB).await;
    pipeline.graph_jobs[&JOB]
        .recovery_summary()
        .iter()
        .map(|set| set.outcome)
        .collect()
}

type RunPositions = Vec<(weaver_graph::NodeId, BTreeMap<u32, u64>)>;

pub async fn run_holding_inspecting_reads(
    scenario: &Scenario,
    held: impl Fn((u32, u32)) -> bool,
    inspect: impl FnOnce(RunPositions),
) -> Held {
    run_holding_where(scenario, held, |pipeline| {
        let graph = &pipeline.graph_jobs[&JOB];
        inspect(
            graph
                .runs()
                .running()
                .map(|node| (node, graph.run_reached(node)))
                .collect(),
        );
    })
    .await
}

pub mod remaining_walk;

// Bytes no period runs through, so a compressing level cannot fold them.
pub fn noise(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}
