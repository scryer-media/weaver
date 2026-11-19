// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::atomic::{AtomicUsize, Ordering};

use sha1::{Digest, Sha1};
use weaver_torrent::core::lengths::Lengths;
use weaver_torrent::storage::TorrentStorage;

use super::disk_room::Waiting;
use super::intake::{Resolving, absolute_placement, parse};
use super::live::{count_waste, take_file_have};
use super::moves::Moving;
use super::restore::torrent_archive_passwords;
use super::retire::{held_of, resolved};
use super::settings::WIND_DOWN_KEY;
use super::views::job_status;
use super::*;
use crate::categories::CategoryConfig;
use crate::clock::{ManualClock, RuntimeClock};
use crate::jobs::handle::HoldOrigin;
use crate::jobs::model::{FieldUpdate, JobUpdate};
use crate::jobs::{Hold, HoldOwner, HoldScope};
use crate::post_processing::model::PostProcessingSettings;
use crate::proxies::{Consumer, LegPath, RouteLeg, RoutingPolicy, SYSTEM_EGRESS_ID};
use crate::proxies::{NetworkRuntime, TorrentScope};
use crate::settings::{Client, Leaf, Level, Seeding, SeedingPolicy, TorrentConfig};
use crate::settings::{SeedingAction, SeedingGoalMode};
use crate::torrents::TrackerStatus;
use crate::torrents::admission::Refusal;
use crate::torrents::metrics::TorrentSeries;
use crate::torrents::persistence::HistoryFile;
use crate::torrents::sessions::Routes;
use crate::torrents::sessions::TorrentRouteReason;
use crate::torrents::sessions::tests::{
    FakeRoutes, env, loopback_proxy, on_network, pool_member_key, save_again, unbound_leg,
};
use crate::torrents::storage::PieceStorage;
use crate::torrents::storage::Pieces;
use crate::torrents::{ContentLayout, TorrentSource, TorrentTracker};
use bytes::Bytes;
use weaver_torrent::api::TorrentIdOrHash;
use weaver_torrent::{ResumePeers, Session, TorrentVersion, TrackerAnnounce};

mod checkpoints;
mod completion;
mod disk_room;
mod intake;
mod listing;
mod memory;
mod moves;
mod old_tick;
mod pass;
mod pass_old;
mod pass_rules;
mod policies;
mod queue;
mod readings;
mod restore;
mod retire;
mod routes;
mod scale_bench;
mod scripts;
mod settings;
mod trackers;
mod unpacking;
mod views;

const EPOCH_MS: i64 = 1_700_000_000_000;

fn job_env() -> JobEnv {
    JobEnv::unbounded()
}

fn bstr(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(format!("{}:", bytes.len()).as_bytes());
    out.extend_from_slice(bytes);
}

fn bint(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(format!("i{value}e").as_bytes());
}

// A torrent's metainfo: one file when the only file is named as the
// torrent, files under the torrent's directory otherwise.
fn metainfo(name: &str, files: &[(&str, &[u8])], piece: usize, private: bool) -> Vec<u8> {
    let whole: Vec<u8> = files
        .iter()
        .flat_map(|(_, bytes)| bytes.iter().copied())
        .collect();
    let pieces: Vec<u8> = whole
        .chunks(piece)
        .flat_map(|chunk| Sha1::digest(chunk).to_vec())
        .collect();
    let mut info = b"d".to_vec();
    if files.len() == 1 && files[0].0 == name {
        bstr(&mut info, b"length");
        bint(&mut info, whole.len());
    } else {
        bstr(&mut info, b"files");
        info.push(b'l');
        for (path, bytes) in files {
            info.push(b'd');
            bstr(&mut info, b"length");
            bint(&mut info, bytes.len());
            bstr(&mut info, b"path");
            info.push(b'l');
            for part in path.split('/') {
                bstr(&mut info, part.as_bytes());
            }
            info.extend_from_slice(b"ee");
        }
        info.push(b'e');
    }
    bstr(&mut info, b"name");
    bstr(&mut info, name.as_bytes());
    bstr(&mut info, b"piece length");
    bint(&mut info, piece);
    bstr(&mut info, b"pieces");
    bstr(&mut info, &pieces);
    if private {
        bstr(&mut info, b"private");
        bint(&mut info, 1);
    }
    info.push(b'e');
    let mut out = b"d".to_vec();
    bstr(&mut out, b"info");
    out.extend_from_slice(&info);
    out.push(b'e');
    out
}

fn plain(name: &str, seed: u8) -> Vec<u8> {
    metainfo(
        name,
        &[(&format!("{name}/a.bin"), &[seed; 3000])],
        1024,
        false,
    )
}

struct Rig {
    service: TorrentService,
    routes: Arc<FakeRoutes>,
    db: Database,
    dir: tempfile::TempDir,
}

fn rig(clock: Arc<dyn Clock>) -> Rig {
    rig_with(clock, job_env())
}

fn rig_with(clock: Arc<dyn Clock>, job_env: JobEnv) -> Rig {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let service = service_on(&db, dir.path(), &routes, job_env, clock);
    Rig {
        service,
        routes,
        db,
        dir,
    }
}

// A service over `db` and `dir`, as a restart would build it.
fn service_on(
    db: &Database,
    dir: &Path,
    routes: &Arc<FakeRoutes>,
    job_env: JobEnv,
    clock: Arc<dyn Clock>,
) -> TorrentService {
    service_over(
        db,
        dir,
        Arc::clone(routes) as Arc<dyn Routes>,
        job_env,
        clock,
    )
}

// A service over `db` and `dir` whose sessions open on `routes`.
fn service_over(
    db: &Database,
    dir: &Path,
    routes: Arc<dyn Routes>,
    mut job_env: JobEnv,
    clock: Arc<dyn Clock>,
) -> TorrentService {
    // Its working root lies under `dir`, as the daemon's lies under its data
    // directory: a restart over `dir` finds what the last run left there.
    job_env.work = dir.join("work");
    let sessions = Sessions::new(routes, env(&dir.join("sessions")), db.clone());
    let settings = TorrentSettings {
        upload_cap: None,
        complete_dir: dir.join("complete"),
        categories: vec![CategoryConfig {
            upload_cap: Default::default(),
            id: 1,
            name: "tv".into(),
            dest_dir: None,
            aliases: String::new(),
            seeding: None,
            torrent_route: true,
            unpack: None,
        }],
        seeding: None,
        torrent: TorrentConfig {
            enabled: true,
            ..TorrentConfig::default()
        },
    };
    TorrentService::new(db.clone(), sessions, settings, job_env, clock)
}

// A restarted service opened, its tick having put the torrents it restored
// in their routes' sessions.
async fn reopen(service: &TorrentService) {
    service.open().await.unwrap();
    service.tick().await;
    // The tick begins the restores off its path: the restart is through
    // once they end.
    service.restored().await;
}

fn set_clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::at_unix_ms(EPOCH_MS))
}

fn request(source: AddSource) -> AddRequest {
    AddRequest {
        source,
        category: None,
        policy_override: None,
        paused: false,
        forced: false,
        unpack: None,
        metadata: Vec::new(),
        upload_limit: None,
        download_limit: None,
        placement: Default::default(),
        tags: Vec::new(),
    }
}

impl TorrentService {
    // `add`, and the wait it began for the torrent's route's session ended:
    // the torrent is in that session, or held for why it did not open.
    async fn add_opened(&self, request: AddRequest) -> Result<JobId, AddError> {
        let job_id = self.add(request).await?;
        self.opened(job_id).await;
        Ok(job_id)
    }

    // Resolves once `job_id` waits no more on its route's session.
    async fn opened(&self, job_id: JobId) {
        let mut ended = self.attaches();
        loop {
            ended.borrow_and_update();
            if !self.attaching(job_id).await {
                return;
            }
            ended.changed().await.unwrap();
        }
    }
}

// A queue whose pipeline holds no live jobs. A retired job goes to its
// finished jobs, newest first.
fn queue() -> SchedulerHandle {
    let state = crate::jobs::SharedPipelineState::new(crate::PipelineMetrics::new(), vec![]);
    let (event_tx, _) = tokio::sync::broadcast::channel(16);
    SchedulerHandle::new(event_tx, state)
}

// Routes that count their plans, so a test that pins what planning reads
// also sees that planning ran.
struct Counted {
    routes: crate::torrents::sessions::NetworkRoutes,
    plans: AtomicUsize,
}

#[async_trait::async_trait]
impl Routes for Counted {
    fn plan(&self, scope: TorrentScope) -> crate::torrents::sessions::RoutePlan {
        self.plans.fetch_add(1, Ordering::Relaxed);
        self.routes.plan(scope)
    }

    fn leg_down(
        &self,
        plan: &crate::torrents::sessions::RoutePlan,
    ) -> Option<crate::torrents::sessions::RouteFailure> {
        self.routes.leg_down(plan)
    }

    async fn open(
        &self,
        plan: &crate::torrents::sessions::RoutePlan,
        peers: usize,
    ) -> Result<crate::torrents::sessions::OpenLeg, String> {
        self.routes.open(plan, peers).await
    }

    fn join(&self, plan: &crate::torrents::sessions::RoutePlan, peers: usize) {
        self.routes.join(plan, peers);
    }

    async fn fetch_blocklist(
        &self,
        url: &reqwest::Url,
    ) -> Result<crate::torrents::sessions::IpRanges, String> {
        self.routes.fetch_blocklist(url).await
    }
}

// The session a torrent runs in, while it runs in one.
async fn session_of(service: &TorrentService, job_id: JobId) -> Arc<Session> {
    let live = service.inner.live.lock().await;
    Arc::clone(&live[&job_id].attached.as_ref().unwrap().session)
}

// The key of the route `scope` runs on now.
fn route_key(rig: &Rig, scope: TorrentScope) -> String {
    rig.service
        .inner
        .sessions
        .routes()
        .plan(scope)
        .key
        .as_str()
        .to_owned()
}

// Every file under `dir`, at any depth.
fn files_under(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .flat_map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                files_under(&path)
            } else if path.file_name() == Some(std::ffi::OsStr::new(".weaver-output-dir")) {
                // The folder's marker is Weaver's, not a file in it.
                Vec::new()
            } else {
                vec![path]
            }
        })
        .collect()
}

// A torrent with every piece in its files, and the folder they are in.
async fn with_every_piece(rig: &Rig) -> (JobId, PathBuf) {
    with_every_piece_of(rig, "set", 1).await
}

// `with_every_piece` for the torrent `plain(name, seed)` makes.
async fn with_every_piece_of(rig: &Rig, name: &str, seed: u8) -> (JobId, PathBuf) {
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain(name, seed))))
        .await
        .unwrap();
    fetch_every_piece(&rig.service, job_id, &[seed; 3000]).await;
    let dir = PathBuf::from(
        rig.service
            .torrent(job_id)
            .await
            .unwrap()
            .storage_dir
            .unwrap(),
    );
    assert_ne!(files_under(&dir), Vec::<PathBuf>::new());
    (job_id, dir)
}

// Every piece of the torrent written through its intake, as a session
// that fetched them would: `whole` is its files' bytes end to end. The
// torrents are held meanwhile, so nothing the service settles sees the
// torrent short of every piece.
async fn fetch_every_piece(service: &TorrentService, job_id: JobId, whole: &[u8]) {
    fetch_pieces(service, job_id, whole, None, |_| {}).await;
}

// The read an upload of the torrent's first block to a peer makes: of its
// first file, through the storage its session reads by.
async fn upload_read(service: &TorrentService, job_id: JobId) -> anyhow::Result<Vec<u8>> {
    let live = service.inner.live.lock().await;
    let torrent = &live[&job_id];
    let storage = PieceStorage::new(
        Arc::clone(torrent.layout.as_ref().unwrap()),
        torrent.intake.clone().unwrap() as Arc<dyn Pieces>,
        service.resident(job_id),
    );
    let mut block = vec![0; 16_384];
    storage.pread_exact(0, 0, &mut block)?;
    Ok(block)
}

// `fetch_every_piece`, or only the pieces `only` names, and `then` done to
// the torrent before anything the service settles sees them.
async fn fetch_pieces(
    service: &TorrentService,
    job_id: JobId,
    whole: &[u8],
    only: Option<&[u32]>,
    then: impl FnOnce(&Live),
) {
    // The check its session makes of a torrent it took in is over first.
    // Until then a piece written here is one that check may have read
    // short and fails after it is had: a session fetches none while it
    // checks. The wait is on the torrent's own state, with nothing held.
    let checking = service.inner.live.lock().await[&job_id]
        .attached
        .as_ref()
        .map(|attached| Arc::clone(&attached.handle));
    if let Some(checking) = checking {
        // A check that is paused never ends: said, rather than waited on
        // for good.
        assert!(
            !(checking.is_paused()
                && matches!(
                    checking.stats().state,
                    weaver_torrent::TorrentStatsState::Initializing { .. }
                )),
            "the torrent is paused in its check, which will not end: its pieces cannot be fetched here"
        );
        // One that ended in error checks nothing.
        let _ = checking.wait_until_initialized().await;
    }
    let live = service.inner.live.lock().await;
    let (layout, intake) = {
        let torrent = &live[&job_id];
        (
            torrent.layout.clone().unwrap(),
            torrent.intake.clone().unwrap(),
        )
    };
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        intake as Arc<dyn Pieces>,
        service.resident(job_id),
    );
    let lengths = Lengths::new(
        layout.files().map(|file| file.length).sum(),
        layout.piece_length() as u32,
    )
    .unwrap();
    for piece in 0..layout.piece_count() {
        if only.is_some_and(|only| !only.contains(&piece)) {
            continue;
        }
        let range = layout.piece_range(piece);
        let index = lengths.try_validate_piece_index(piece).unwrap();
        assert!(storage.reserve_piece(index, &Arc::default()));
        for span in layout.spans(piece) {
            let at = (range.start + span.at) as usize;
            storage
                .pwrite_all(
                    span.file,
                    span.range.start,
                    &whole[at..at + span.range.len() as usize],
                )
                .unwrap();
        }
        storage.on_piece_completed(index).unwrap();
    }
    then(&live[&job_id]);
    drop(live);
}

// The metainfo with a tracker to announce to.
fn announcing(metainfo: &[u8], url: &str) -> Vec<u8> {
    let mut out = b"d".to_vec();
    bstr(&mut out, b"announce");
    bstr(&mut out, url.as_bytes());
    out.extend_from_slice(&metainfo[1..]);
    out
}

// A tracker on loopback that names `peer` to every announce.
async fn tracker(peer: std::net::SocketAddrV4) -> String {
    answering_tracker(peer).await.0
}

// The `event` of each announce an answering tracker answered ("" for
// none), and the Unix time in ms just before its answer went out.
type Answered = tokio::sync::watch::Receiver<Vec<(String, i64)>>;

// `tracker`, and what it has answered.
async fn answering_tracker(peer: std::net::SocketAddrV4) -> (String, Answered) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    let (answers, answered) = tokio::sync::watch::channel(Vec::new());
    let answers = Arc::new(answers);
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let answers = Arc::clone(&answers);
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0u8; 1024];
                while !request.windows(4).any(|end| end == b"\r\n\r\n") {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(read) => request.extend_from_slice(&buf[..read]),
                    }
                }
                let mut body = b"d8:intervali1800e5:peers6:".to_vec();
                body.extend_from_slice(&peer.ip().octets());
                body.extend_from_slice(&peer.port().to_be_bytes());
                body.push(b'e');
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let answer = [head.as_bytes(), &body].concat();
                let line = String::from_utf8_lossy(&request);
                let line = line.lines().next().unwrap_or_default();
                let event = line
                    .split(['?', '&', ' '])
                    .find_map(|pair| pair.strip_prefix("event="))
                    .unwrap_or_default()
                    .to_owned();
                let at = i64::try_from(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis(),
                )
                .unwrap();
                match stream.write_all(&answer).await {
                    Ok(()) => answers.send_modify(|answers| answers.push((event, at))),
                    Err(error) => {
                        tracing::debug!(error = %error, "a client hung up before its answer");
                    }
                }
            });
        }
    });
    (url, answered)
}

// `job_id` given `url` as a tracker the operator names, in place of the
// torrent's own listing of it. A tracker on loopback is asked only when the
// operator named it, never when the torrent did.
async fn tracked(service: &TorrentService, job_id: JobId, url: &str) {
    service
        .remove_trackers(job_id, &[url.to_owned()])
        .await
        .unwrap();
    service
        .add_trackers(job_id, vec![vec![url.to_owned()]])
        .await
        .unwrap();
}

// The magnet of `hash` fetching its metainfo from the tracker at
// 127.0.0.1:`port`, which the operator names: added paused, its tracker
// named again, then resumed.
async fn fetching_from(service: &TorrentService, hash: &str, port: u16) -> JobId {
    let job_id = service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Magnet(format!(
                "magnet:?xt=urn:btih:{hash}&tr=http%3A%2F%2F127.0.0.1%3A{port}%2Fannounce"
            )))
        })
        .await
        .unwrap();
    tracked(
        service,
        job_id,
        &format!("http://127.0.0.1:{port}/announce"),
    )
    .await;
    service.resume(job_id).await.unwrap();
    job_id
}

// A session on loopback seeding `metainfo` from `files` under `dir`, with
// no DHT, trackers or discovery.
async fn seeder(dir: &std::path::Path, metainfo: &[u8], files: &[(&str, &[u8])]) -> Arc<Session> {
    let (session, handle) = sharing(dir, metainfo, files).await;
    handle.wait_until_completed().await.unwrap();
    session
}

// A session on loopback running `metainfo` with `files` under `dir` in
// hand and wanting the rest, with no DHT, trackers or discovery.
async fn sharing(
    dir: &std::path::Path,
    metainfo: &[u8],
    files: &[(&str, &[u8])],
) -> (Arc<Session>, Arc<ManagedTorrent>) {
    use weaver_torrent::{
        AddTorrent, AddTorrentOptions, ListenerMode, ListenerOptions, SessionOptions,
    };

    for (path, bytes) in files {
        let path = dir.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }
    let session = Session::new_with_opts(
        dir.join("state"),
        SessionOptions {
            dht: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            fastresume: false,
            persistence: None,
            listen: Some(ListenerOptions {
                mode: ListenerMode::TcpOnly,
                listen_addr: "127.0.0.1:0".parse().unwrap(),
                enable_upnp_port_forwarding: false,
                ..ListenerOptions::default()
            }),
            ..SessionOptions::new(Arc::new(
                weaver_torrent::network::KernelSockets::new(None).unwrap(),
            ))
        },
    )
    .await
    .unwrap();
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(metainfo.to_vec()),
            Some(AddTorrentOptions {
                output_folder: Some(dir.to_str().unwrap().to_owned()),
                overwrite: true,
                ..AddTorrentOptions::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    (session, handle)
}

// The tee of a torrent whose files unpack.
async fn tee_of(service: &TorrentService, job_id: JobId) -> Arc<crate::torrents::tee::Tee> {
    let live = service.inner.live.lock().await;
    let store = live[&job_id].store.as_ref().unwrap();
    Arc::clone(store.tee().unwrap())
}

// The torrent's checkpoint written now, its tee's queued at once first,
// as a stop writes them.
async fn save_with_tee(service: &TorrentService, job_id: JobId) {
    let tee = {
        let live = service.inner.live.lock().await;
        live.get(&job_id)
            .and_then(|torrent| Some(Arc::clone(torrent.store.as_ref()?.tee()?)))
    };
    if let Some(tee) = tee {
        service.checkpoint_tee(job_id, tee, true).await;
    }
    service.save_checkpoints([job_id].into()).await;
}

// How many pieces the torrent's last checkpoint says it has.
fn kept_pieces(db: &Database, job_id: JobId) -> u32 {
    db.torrent_pieces(job_id).unwrap().map_or(0, |kept| {
        kept.bits.iter().map(|byte| byte.count_ones()).sum()
    })
}

// A torrent's download done, as the session would have said.
async fn complete(rig: &Rig, job_id: JobId) {
    rig.service
        .inner
        .live
        .lock()
        .await
        .get_mut(&job_id)
        .unwrap()
        .record
        .completed_at_ms = Some(EPOCH_MS);
}

// A torrent with every piece in its files, stopped with them kept, its
// folder then moved aside, as a drive not mounted yet leaves it: the folder
// and where it went.
async fn gone_at_restart(rig: &Rig) -> (JobId, PathBuf, PathBuf) {
    rig.service.open().await.unwrap();
    let (job_id, dir) = with_every_piece(rig).await;
    rig.service.stop().await;
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();
    let away = dir.with_extension("away");
    std::fs::rename(&dir, &away).unwrap();
    (job_id, dir, away)
}

// The torrent is held for its files missing: in no session, so it
// announces nothing and serves no peer, with every piece kept, its intent
// as the operator left it, and nothing made where its files were.
async fn held_for_its_files(service: &TorrentService, job_id: JobId, dir: &Path) {
    let view = service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::FilesMissing);
    assert_eq!(view.pieces.map(|pieces| pieces.have), Some(3));
    {
        let live = service.inner.live.lock().await;
        assert!(live[&job_id].store.is_some());
        assert!(live[&job_id].attached.is_none());
    }
    assert_eq!(
        service
            .inner
            .db
            .get_torrent(job_id)
            .unwrap()
            .unwrap()
            .intent(),
        TorrentIntent::Resumed
    );
    assert!(!dir.exists());
}

// The torrent in its session, every piece had from the start.
async fn runs_with_every_piece(service: &TorrentService, job_id: JobId) {
    let handle = handle_of(service, job_id).await;
    handle.wait_until_initialized().await.unwrap();
    let stats = handle.stats();
    assert!(stats.finished);
    assert_eq!(stats.progress_bytes, 3000);
    assert_eq!(job_have(service, job_id).await, 3);
    assert_ne!(
        service.torrent(job_id).await.unwrap().state,
        TorrentState::FilesMissing
    );
}

// The rig's torrents dial out on loopback.
fn on_loopback(routes: &FakeRoutes) {
    use crate::torrents::net::{LegKind, LegSockets};

    let loopback = Arc::new(weaver_tunnel::pipe::Revocable::new(Arc::new(
        weaver_tunnel::pipe::Egress::new(
            4,
            weaver_tunnel::egress::SocketEgress::SourceAddress("127.0.0.1".parse().unwrap()),
            // No connect deadline: the test waits on what it checks.
            std::time::Duration::MAX,
        ),
    )));
    routes.set_leg(
        LegKind::Ladder,
        Arc::new(LegSockets::new(loopback, &LegKind::Ladder)),
    );
}

// The torrent's place in its session.
async fn handle_of(service: &TorrentService, job_id: JobId) -> Arc<ManagedTorrent> {
    let live = service.inner.live.lock().await;
    Arc::clone(&live[&job_id].attached.as_ref().unwrap().handle)
}

// The torrent's view once it seeds, read as the queue publishes it.
async fn seeding_view(
    service: &TorrentService,
    changes: &mut watch::Receiver<u64>,
    job_id: JobId,
) -> TorrentView {
    loop {
        changes.borrow_and_update();
        let view = service.torrent(job_id).await.unwrap();
        if view.state == TorrentState::Seeding {
            return view;
        }
        changes.changed().await.unwrap();
    }
}

// The bytes the seeder has sent.
fn uploaded(seeder: &Session) -> u64 {
    seeder
        .get(TorrentIdOrHash::Id(0))
        .unwrap()
        .stats()
        .uploaded_bytes
}

// A torrent fetched whole from a seed on loopback and seeding, on a clock
// the test sets.
struct Seeded {
    rig: Rig,
    clock: Arc<ManualClock>,
    job_id: JobId,
    seeder: Arc<Session>,
    // What its tracker has answered.
    answered: Answered,
    _seeds: tempfile::TempDir,
}

async fn seeded() -> Seeded {
    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 251) as u8).collect();
    let files = [("set/a.bin", data.as_slice())];
    let private = metainfo("set", &files, 16_384, true);
    let seeds = tempfile::tempdir().unwrap();
    let seeder = seeder(seeds.path(), &private, &files).await;
    let std::net::SocketAddr::V4(peer) = seeder.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let (url, answered) = answering_tracker(peer).await;
    let bytes = private.to_vec();

    let clock = set_clock();
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
    on_loopback(&rig.routes);
    let handle = queue();
    let mut changes = handle.state().subscribe_job_changes();
    rig.service.publish_to(handle);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    handle_of(&rig.service, job_id)
        .await
        .wait_until_completed()
        .await
        .unwrap();
    seeding_view(&rig.service, &mut changes, job_id).await;
    Seeded {
        rig,
        clock,
        job_id,
        seeder,
        answered,
        _seeds: seeds,
    }
}

impl Seeded {
    fn advance(&self, ms: i64) {
        self.clock.shift_unix_ms(ms);
    }

    async fn view(&self) -> TorrentView {
        self.rig.service.torrent(self.job_id).await.unwrap()
    }

    // The ledger as the service keeps it: what the next write of it lands.
    // Its times alone are written once TIMES_EVERY_MS has passed, or at a
    // stop, so the database may hold older times between.
    async fn kept(&self) -> TorrentCounters {
        self.rig.service.inner.live.lock().await[&self.job_id].counters
    }

    // The ledger as the database holds it.
    fn stored(&self) -> TorrentCounters {
        self.rig
            .db
            .get_torrent_counters(self.job_id)
            .unwrap()
            .unwrap()
    }

    // Forces an announce and waits for its answer: the tracker's answers
    // from the forced one on. An announce still waiting for its answer is not
    // sent again, and the tracker counts an answer before the torrent reads
    // it, so the force waits until the `completed` the run owes is answered
    // and no announce of the torrent waits.
    async fn reannounced(&mut self) -> Vec<(String, i64)> {
        self.answered
            .wait_for(|answers| answers.iter().any(|(event, _)| event == "completed"))
            .await
            .unwrap();
        trackers_once(&self.rig.service, self.job_id, |announces| {
            announces.iter().all(|announce| !announce.in_flight)
        })
        .await;
        let heard = self.answered.borrow_and_update().len();
        self.rig.service.reannounce(self.job_id).await.unwrap();
        self.answered
            .wait_for(|answers| answers.len() > heard)
            .await
            .unwrap()[heard..]
            .to_vec()
    }

    async fn stop(self) {
        self.rig.service.stop().await;
        self.seeder.stop().await;
    }
}

// The job's pieces in hand, as its view shows them.
async fn job_have(service: &TorrentService, job_id: JobId) -> u32 {
    service.torrent(job_id).await.unwrap().pieces.unwrap().have
}

// The next announce the tracker on `listener` hears, unanswered.
async fn heard_announce(listener: &tokio::net::TcpListener) -> tokio::net::TcpStream {
    heard_request(listener).await.0
}

// The next announce the tracker on `listener` hears, unanswered, and its
// request.
async fn heard_request(listener: &tokio::net::TcpListener) -> (tokio::net::TcpStream, String) {
    use tokio::io::AsyncReadExt;

    let (mut stream, _) = listener.accept().await.unwrap();
    let mut request = Vec::new();
    let mut buf = [0u8; 1024];
    while !request.windows(4).any(|end| end == b"\r\n\r\n") {
        let read = stream.read(&mut buf).await.unwrap();
        assert!(read > 0, "the announce ended before its request was read");
        request.extend_from_slice(&buf[..read]);
    }
    (stream, String::from_utf8_lossy(&request).into_owned())
}

// The torrent's trackers once its session's announce table says `done`.
async fn trackers_once(
    service: &TorrentService,
    job_id: JobId,
    done: impl Fn(&[TrackerAnnounce]) -> bool,
) -> Vec<TorrentTracker> {
    let table = handle_of(service, job_id).await.tracker_status().unwrap();
    table
        .subscribe()
        .wait_for(|announces| done(announces))
        .await
        .unwrap();
    service.torrent(job_id).await.unwrap().trackers
}

// The next announce the tracker on `listener` hears, unanswered, and its
// event.
async fn heard_event(
    listener: &tokio::net::TcpListener,
) -> (tokio::net::TcpStream, Option<String>) {
    use tokio::io::AsyncReadExt;

    let (mut stream, _) = listener.accept().await.unwrap();
    let mut request = Vec::new();
    let mut buf = [0u8; 1024];
    while !request.windows(4).any(|end| end == b"\r\n\r\n") {
        let read = stream.read(&mut buf).await.unwrap();
        assert!(read > 0, "the announce ended before its request was read");
        request.extend_from_slice(&buf[..read]);
    }
    let request = String::from_utf8_lossy(&request);
    let path = request.split(' ').nth(1).unwrap();
    let event = path
        .split_once('?')
        .map(|(_, query)| query)
        .unwrap_or_default()
        .split('&')
        .find_map(|kv| kv.strip_prefix("event="))
        .map(str::to_owned);
    (stream, event)
}

// Answers an announce with no peers.
async fn answer_no_peers(mut stream: tokio::net::TcpStream) {
    use tokio::io::AsyncWriteExt;

    let body = b"d8:intervali1800e5:peers0:e";
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(&[head.as_bytes(), body].concat())
        .await
        .unwrap();
}

// Who holds `job_id` stopped, as the queue last applied the holds.
async fn paused_by(service: &TorrentService, job_id: JobId) -> Option<HoldOrigin> {
    service.inner.live.lock().await[&job_id].paused_by
}

// One torrent in category 1 under one tracker rule, with nothing set at
// any level but the global defaults.
fn chain() -> (TorrentRecord, TrackerRule, TorrentSettings) {
    let record = TorrentRecord {
        unpack_in_force: None,
        unpack_over: false,
        job_id: JobId(1),
        infohash_v1: Some([1; 20]),
        infohash_v2: None,
        name: "set".into(),
        source: TorrentSource::Metainfo,
        metainfo: None,
        category_id: Some(1),
        added_at_ms: EPOCH_MS,
        kept: KeptIntent::Intent(TorrentIntent::Resumed),
        forced: false,
        completed_at_ms: None,
        storage_dir: None,
        policy_override: None,
        unpack: None,
        metadata: Vec::new(),
        upload_limit: None,
        download_limit: None,
        sequential: false,
        first_last_piece_first: false,
        save_path: None,
        content_layout: None,
        files_dir: None,
        seed_mode: false,
        move_to: None,
        queue_position: 0,
        pool_member: None,
        private: false,
    };
    let rule = TrackerRule {
        transports: Default::default(),
        id: 1,
        host_pattern: "tracker.test".into(),
        seeding: None,
        never_pause: false,
        unpack: None,
        client: None,
        torrent_route: false,
        dht: None,
        pex: None,
        lsd: None,
        encryption: None,
        upload_cap: Leaf::Inherit,
        forced: false,
        ssl: None,
        bound: None,
        archive_passwords: Default::default(),
    };
    let settings = TorrentSettings {
        upload_cap: None,
        complete_dir: PathBuf::from("/complete"),
        categories: vec![CategoryConfig {
            upload_cap: Leaf::Inherit,
            id: 1,
            name: "tv".into(),
            dest_dir: None,
            aliases: String::new(),
            seeding: None,
            torrent_route: false,
            unpack: None,
        }],
        // As the global terms load: nothing there inherits.
        seeding: Some(SeedingPolicy::default().at_global()),
        torrent: TorrentConfig::default(),
    };
    (record, rule, settings)
}

// What a view shows it has in hand: each file's bytes, all of them, the
// progress, and the pieces.
fn in_hand(view: &TorrentView) -> (Vec<u64>, u64, f64, Option<u32>) {
    (
        view.file_have.clone(),
        view.have_bytes,
        view.progress,
        view.pieces.map(|pieces| pieces.have),
    )
}

// A torrent of one file, `a.bin`, under a folder of its own, `placed`, and
// that file's bytes: four pieces.
fn placed_torrent() -> (Vec<u8>, Vec<u8>) {
    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 241) as u8).collect();
    let metainfo = metainfo("placed", &[("a.bin", data.as_slice())], 16_384, true);
    (metainfo, data)
}

// The torrent added with `placement`, its view once it seeds, as the queue
// publishes it.
async fn seeding_placed(
    rig: &Rig,
    metainfo: Vec<u8>,
    placement: Placement,
) -> (JobId, TorrentView) {
    let handle = queue();
    let mut changes = handle.state().subscribe_job_changes();
    rig.service.publish_to(handle);
    let job_id = rig
        .service
        .add_opened(AddRequest {
            placement,
            ..request(AddSource::Metainfo(metainfo))
        })
        .await
        .unwrap();
    let view = seeding_view(&rig.service, &mut changes, job_id).await;
    (job_id, view)
}

// The torrent's view once `holds` holds of it, read as the queue publishes
// it.
async fn view_once(
    service: &TorrentService,
    changes: &mut watch::Receiver<u64>,
    job_id: JobId,
    holds: impl Fn(&TorrentView) -> bool,
) -> TorrentView {
    loop {
        changes.borrow_and_update();
        let view = service.torrent(job_id).await.unwrap();
        if holds(&view) {
            return view;
        }
        changes.changed().await.unwrap();
    }
}

// The torrent added with `placement`, its view once its job is done and it
// seeds, and the queue's changes from then on.
async fn completed_placed(
    rig: &Rig,
    metainfo: Vec<u8>,
    placement: Placement,
) -> (JobId, TorrentView, watch::Receiver<u64>) {
    let handle = queue();
    let mut changes = handle.state().subscribe_job_changes();
    rig.service.publish_to(handle);
    let job_id = rig
        .service
        .add_opened(AddRequest {
            placement,
            ..request(AddSource::Metainfo(metainfo))
        })
        .await
        .unwrap();
    let view = view_once(&rig.service, &mut changes, job_id, |view| {
        view.state == TorrentState::Seeding && view.completed_at_ms.is_some()
    })
    .await;
    (job_id, view, changes)
}

fn values(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

// The pool member each of `jobs` is pinned to. Each pin is the one stored,
// and its torrent's route is its member's session.
async fn pins(service: &TorrentService, db: &Database, jobs: &[JobId]) -> Vec<Option<u32>> {
    let live = service.inner.live.lock().await;
    jobs.iter()
        .map(|job_id| {
            let torrent = &live[job_id];
            let pin = torrent.record.pool_member;
            assert_eq!(db.get_torrent(*job_id).unwrap().unwrap().pool_member, pin);
            if let Some(member) = pin {
                assert_eq!(torrent.route, Some(pool_member_key(member)));
            }
            pin
        })
        .collect()
}
