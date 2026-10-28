// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::SchedulerError;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crate::Database;
use crate::ingest::submit_nzb_bytes;
use crate::jobs::ids::MessageId;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::runtime::buffers::{BufferPool, BufferPoolConfig};
use crate::runtime::system_profile::{
    CpuProfile, DiskProfile, FilesystemType, MemoryProfile, SimdSupport, StorageClass,
    SystemProfile,
};
use crate::settings::{Config, SharedConfig};
use crate::{
    FileSpec, JobInfo, PipelineMetrics, SchedulerHandle, SegmentSpec, SharedPipelineState,
};
use tempfile::TempDir;
use tokio::sync::{RwLock, oneshot};
use weaver_model::files::FileRole;
use weaver_nntp::client::{NntpClient, NntpClientConfig};

macro_rules! segment_spec {
    (number: $number:expr, bytes: $bytes:expr, message_id: $message_id:expr $(,)?) => {
        SegmentSpec {
            ordinal: $number,
            article_number: $number,
            bytes: $bytes,
            message_ids: crate::jobs::MessageIds::new(vec![$message_id]),
        }
    };
    (ordinal: $ordinal:expr, article_number: $article_number:expr, bytes: $bytes:expr, message_id: $message_id:expr $(,)?) => {
        SegmentSpec {
            ordinal: $ordinal,
            article_number: $article_number,
            bytes: $bytes,
            message_ids: crate::jobs::MessageIds::new(vec![$message_id]),
        }
    };
}

mod archive_passwords;
mod archive_topology;
mod cancel_batch;
mod control_lifecycle;
mod core;
mod decode_stage;
mod direct_store;
mod download_dispatch;
pub(in crate::pipeline) mod engines;
mod full_file_names;
mod hardware_profile;
mod idle;
mod nzb_admission;
mod par2;
mod par2_completion;
#[cfg(unix)]
mod post_processing_completion;
mod post_processing_pause;
mod provider_scheduling;
mod queue;
mod restore_history;
mod script_events;
mod server_attribution;
mod terminal_settlement;
mod uu_assembly;
mod yenc_compatibility;

impl Pipeline {
    pub(in crate::pipeline) async fn restore_job(
        &mut self,
        request: RestoreJobRequest,
    ) -> Result<(), SchedulerError> {
        let id = request.job_id;
        if self.jobs.contains_key(&id) {
            return Err(SchedulerError::JobExists(id));
        }
        // Synthetic restore requests still need the active record that real
        // startup recovery reads, so admission can commit before work starts.
        if !self.db.load_active_jobs()?.contains_key(&id) {
            self.db.create_active_job(&crate::ActiveJob {
                job_id: id,
                nzb_hash: request.job_hash,
                nzb_path: request.working_dir.join("restore-fixture.nzb"),
                nzb_zstd: sample_nzb_zstd(),
                output_dir: request.working_dir.clone(),
                created_at: 0,
                category: request.spec.category.clone(),
                metadata: request.spec.metadata.clone(),
                status: "queued",
                download_state: "queued",
                post_state: "idle",
                run_state: "active",
                paused_resume_status: None,
                paused_resume_download_state: None,
                paused_resume_post_state: None,
                password_override: None,
                supplied_password_candidates: Vec::new(),
            })?;
        }
        // Some crash fixtures reuse the stopped runtime's shared queue.
        self.shared_state.queue.write(|inner| {
            inner.retain_jobs(|row| row.id != id);
            crate::pipeline::queue::Change::changed(())
        });
        self.shared_state.queue.restore(request)?;
        self.apply_queue_intents().await;
        Ok(())
    }
}

struct TestHarness {
    _temp_dir: TempDir,
    _data_dir: PathBuf,
    handle: SchedulerHandle,
    config: SharedConfig,
    db: Database,
    pipeline_task: tokio::task::JoinHandle<()>,
}

impl TestHarness {
    async fn new() -> Self {
        Self::new_with_nntp(
            NntpClient::new(NntpClientConfig {
                servers: vec![],
                max_idle_age: Duration::from_secs(1),
                max_retries_per_server: 1,
                soft_timeout: Duration::from_secs(15),
            }),
            0,
        )
        .await
    }

    async fn new_with_nntp(nntp: NntpClient, total_connections: usize) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path().join("data");
        let intermediate_dir = temp_dir.path().join("intermediate");
        let complete_dir = temp_dir.path().join("complete");
        let db = Database::open(&temp_dir.path().join("weaver.db")).unwrap();
        let config: SharedConfig = Arc::new(RwLock::new(Config {
            data_dir: data_dir.display().to_string(),
            hardware_profile: None,
            intermediate_dir: Some(intermediate_dir.display().to_string()),
            complete_dir: Some(complete_dir.display().to_string()),
            buffer_pool: None,
            servers: vec![],
            categories: vec![],
            retry: None,
            max_download_speed: None,
            propagation_delay_secs: None,
            max_concurrent_nzbs: None,
            cleanup_after_extract: Some(true),
            watch_folder: crate::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: Default::default(),
            direct_store: None,
            // Pinned off rather than left at the shipped default, so an
            // extraction fixture whose member happens to be called
            // `sample.mkv` keeps asserting extraction instead of asserting the
            // delivery-naming policy. Tests that mean to exercise the pass turn
            // it on for themselves.
            delivery_naming: Some(crate::settings::DeliveryNamingOverrides {
                deobfuscate_delivered_members: Some(false),
                enable_srrdb_lookup: None,
            }),
            metrics: Default::default(),
            config_path: None,
        }));

        let (event_tx, _) = broadcast::channel::<PipelineEvent>(1024);
        let shared_state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
        let handle = SchedulerHandle::new(event_tx.clone(), shared_state.clone());

        let profile = SystemProfile {
            cpu: CpuProfile {
                physical_cores: 4,
                logical_cores: 4,
                simd: SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: DiskProfile {
                storage_class: StorageClass::Ssd,
                filesystem: FilesystemType::Apfs,
                sequential_write_mbps: 1000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        };
        let buffers = BufferPool::new(BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        });

        let mut pipeline = Pipeline::new(
            event_tx,
            nntp,
            buffers,
            profile,
            data_dir.clone(),
            intermediate_dir,
            complete_dir,
            total_connections,
            4,
            vec![],
            false,
            shared_state,
            db.clone(),
            config.clone(),
        )
        .await
        .unwrap();
        let pipeline_task = tokio::spawn(async move {
            pipeline.run().await;
        });

        Self {
            _temp_dir: temp_dir,
            _data_dir: data_dir,
            handle,
            config,
            db,
            pipeline_task,
        }
    }

    async fn shutdown(self) {
        self.handle.shutdown().await.unwrap();
        self.pipeline_task.await.unwrap();
    }
}

fn sample_nzb_bytes() -> Vec<u8> {
    br#"<?xml version="1.0" encoding="UTF-8"?>
        <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
          <file poster="poster" date="1700000000" subject="Silver Horizon.Sample.rar">
            <groups><group>alt.binaries.test</group></groups>
            <segments><segment bytes="100" number="1">msgid@example.com</segment></segments>
          </file>
        </nzb>"#
        .to_vec()
}

pub(in crate::pipeline) fn sample_nzb_zstd() -> Vec<u8> {
    crate::ingest::compress_nzb_bytes(&sample_nzb_bytes()).unwrap()
}

// The stored NZB a fixture spec would have been read from: the same files,
// in the same order, with the same articles, so a job added with it reads
// its articles back when it is first handed work.
pub(in crate::pipeline) fn nzb_zstd_for(spec: &JobSpec) -> Vec<u8> {
    fn escape(text: &str) -> String {
        text.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
    }
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">"#,
    );
    for file in &spec.files {
        let count = file.segments.len();
        xml.push_str(&format!(
            r#"<file poster="fixture" date="{}" subject="{}"><groups>"#,
            file.posted_at_epoch.unwrap_or(1_700_000_000),
            escape(&format!("\"{}\" yEnc (1/{count})", file.filename)),
        ));
        for group in &file.groups {
            xml.push_str(&format!("<group>{}</group>", escape(group)));
        }
        if file.groups.is_empty() {
            xml.push_str("<group>alt.binaries.test</group>");
        }
        xml.push_str("</groups><segments>");
        for segment in &file.segments {
            for message_id in segment.message_ids.all() {
                xml.push_str(&format!(
                    r#"<segment bytes="{}" number="{}">{}</segment>"#,
                    segment.bytes,
                    segment.article_number.max(segment.ordinal + 1),
                    escape(message_id),
                ));
            }
        }
        xml.push_str("</segments></file>");
    }
    xml.push_str("</nzb>");
    crate::ingest::compress_nzb_bytes(xml.as_bytes()).unwrap()
}

// Build a waiting job's articles as its first handout would, and wait for
// them: the read is sent for and its result delivered on the actor's turn.
pub(in crate::pipeline) async fn activate_job(pipeline: &mut Pipeline, job_id: JobId) {
    pipeline.request_job_activation(job_id);
    let done = pipeline
        .job_activated_rx
        .recv()
        .await
        .expect("the activation channel stays open");
    pipeline.handle_job_activated(done);
}

fn minimal_job_state(job_id: JobId, name: &str, working_dir: PathBuf) -> JobState {
    JobState {
        job_id,
        job_hash: [0; 32],
        spec: JobSpec {
            name: name.to_string(),
            password: None,
            files: vec![],
            total_bytes: 0,
            category: None,
            metadata: vec![],
        },
        status: JobStatus::Downloading,
        download_state: crate::jobs::model::DownloadState::Downloading,
        post_state: crate::jobs::model::PostState::Idle,
        run_state: crate::jobs::model::RunState::Active,
        articles: crate::jobs::model::JobArticles::Active(Box::new(
            crate::jobs::model::ActiveArticles {
                assembly: JobAssembly::new(job_id),
                download_queue: DownloadQueue::new(),
                recovery_queue: DownloadQueue::new(),
            },
        )),
        created_at: std::time::Instant::now(),
        created_at_epoch_ms: crate::jobs::model::epoch_ms_now(),
        queued_repair_at_epoch_ms: None,
        queued_extract_at_epoch_ms: None,
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
        failure_error: None,
        working_dir,
        downloaded_bytes: 0,
        restored_download_floor_bytes: 0,
        downloaded_wire_bytes: 0,
        server_attribution: Default::default(),
        failed_bytes: 0,
        par2_bytes: 0,
        support_facts: Default::default(),
        detected_archives: HashMap::new(),
        file_identities: HashMap::new(),
        staging_dir: None,
        category_bytes: None,
        nzb_password_candidates: Default::default(),
    }
}

fn finished_job_info(job_id: JobId) -> JobInfo {
    JobInfo {
        job_id,
        job_hash: Some([0; 32]),
        name: format!("finished-{}", job_id.0),
        error: None,
        download_wait_reason: None,
        download_retry_at_epoch_ms: None,
        status: JobStatus::Complete,
        download_state: crate::jobs::model::DownloadState::Complete,
        finalizing_download: false,
        fetching_repair_data: false,
        post_state: crate::jobs::model::PostState::Idle,
        run_state: crate::jobs::model::RunState::Active,
        progress: 1.0,
        total_bytes: 0,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        phase_progress: Vec::new(),
        failed_bytes: 0,
        health: 1000,
        terminal_discards: Vec::new(),
        total_files: 0,
        completed_files: 0,
        remaining_par_files: 0,
        file_remaining_bytes: Vec::new(),
        password: None,
        category: None,
        metadata: vec![],
        output_dir: None,
        server_attribution: Vec::new(),
        created_at_epoch_ms: job_id.0 as f64,
    }
}

fn history_row_with_output_dir(
    job_id: JobId,
    name: &str,
    status: &str,
    output_dir: PathBuf,
) -> crate::JobHistoryRow {
    crate::JobHistoryRow {
        job_id: job_id.0,
        job_hash: None,
        name: name.to_string(),
        status: status.to_string(),
        error_message: None,
        total_bytes: 1024,
        downloaded_bytes: 1024,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: Some(output_dir.display().to_string()),
        nzb_path: None,
        created_at: 1,
        completed_at: 2,
        metadata: None,
        server_attribution: None,
    }
}

fn insert_history_row_with_nzb_zstd(db: &Database, row: &crate::JobHistoryRow, nzb_zstd: &[u8]) {
    db.insert_job_history(row).unwrap();
    let datastore = db.datastore();
    let job_id = row.job_id as i64;
    let nzb_zstd = nzb_zstd.to_vec();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(
            datastore.read_exec(),
            "UPDATE job_history SET nzb_zstd = {} WHERE job_id = {}",
            &[SqlArg::Bytes(nzb_zstd), SqlArg::I64(job_id)],
        )
        .await?;
        Ok(())
    })
    .unwrap();
}

// The one server every harness pool points at.
//
// It answers each dial with a greeting the lane refuses, so a worker that
// was handed a lease fails its dial at once and gives the permit back, and
// the refusal is a protocol answer rather than a transport fault: it records
// no cooldown, so the server stays eligible for the next pass. A name that
// does not resolve did the first half of that and not the second, and left
// each test racing the worker's health bookkeeping.
fn refusing_harness_server_port() -> u16 {
    static PORT: std::sync::OnceLock<u16> = std::sync::OnceLock::new();
    *PORT.get_or_init(|| {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("bind the refusing harness server");
        let port = listener.local_addr().unwrap().port();
        std::thread::Builder::new()
            .name("refusing-harness-server".into())
            .spawn(move || {
                for socket in listener.incoming().flatten() {
                    let _ = std::io::Write::write_all(
                        &mut &socket,
                        b"555 harness server takes no session\r\n",
                    );
                    let _ = socket.shutdown(std::net::Shutdown::Both);
                }
            })
            .expect("spawn the refusing harness server");
        port
    })
}

// Wait until every worker thread the passes so far handed a lease to has
// given its dial up and returned the server's permit.
//
// The harness server refuses every session, so each worker fails its dial;
// but the pass that leased it returns before that, and before the worker has
// even taken the permit. A test that then describes one of those lanes as
// finished, by rewinding the pipeline's counters, must wait for the seat to
// be real: the dispatcher measures a server by the permits it can see, not
// by the counters. Counting free permits cannot tell a worker that gave its
// permit back from one that has not taken it yet, so this waits for each
// lane's own failure report instead, which the worker sends only after its
// permit is back.
async fn settle_lane_dials(pipeline: &mut Pipeline) {
    for _ in 0..pipeline.download_lane_owners.len() {
        match pipeline
            .owned_download_lane_event_rx
            .recv()
            .await
            .expect("lane event channel should stay open")
        {
            OwnedDownloadLaneEvent::AcquireFailed { .. } => {}
            OwnedDownloadLaneEvent::LeaseObsolete { lease } => {
                panic!("lane {} became obsolete before dialing", lease.lane_id)
            }
            OwnedDownloadLaneEvent::BatchComplete { lane_id, .. } => {
                panic!("lane {lane_id} fetched from a server that refuses every session")
            }
        }
    }
}

async fn new_direct_pipeline_with_buffers(
    temp_dir: &TempDir,
    buffer_config: BufferPoolConfig,
    total_connections: usize,
) -> (Pipeline, PathBuf, PathBuf) {
    new_direct_pipeline_with(temp_dir, buffer_config, total_connections, None).await
}

// [`new_direct_pipeline_with_buffers`], plus the `[direct_store]` config table.
//
// Everything else in this module reaches for `set_gate`, which bypasses
// configuration entirely. The tests that exist to prove the *config* is the
// gate — and that the kill switch's sweep fires off it — must come through
// here instead.
async fn new_direct_pipeline_with(
    temp_dir: &TempDir,
    buffer_config: BufferPoolConfig,
    total_connections: usize,
    direct_store: Option<crate::settings::DirectStoreOverrides>,
) -> (Pipeline, PathBuf, PathBuf) {
    new_direct_pipeline_at_roots(
        temp_dir.path().join("data"),
        temp_dir.path().join("intermediate"),
        temp_dir.path().join("complete"),
        temp_dir.path().join("weaver.db"),
        buffer_config,
        total_connections,
        direct_store,
    )
    .await
}

// [`new_direct_pipeline_with`] with the three roots given explicitly.
//
// Everything else here puts `intermediate` and `complete` under one `TempDir`,
// which puts them on one filesystem — and the whole class of question this
// exists for ("is the publish a rename or a byte copy?") is unobservable there.
// The cross-device probe in `direct_store/scenarios/cross_device.rs` hands in two roots on
// genuinely different mounts.
#[allow(clippy::too_many_arguments)]
async fn new_direct_pipeline_at_roots(
    data_dir: PathBuf,
    intermediate_dir: PathBuf,
    complete_dir: PathBuf,
    db_path: PathBuf,
    buffer_config: BufferPoolConfig,
    total_connections: usize,
    direct_store: Option<crate::settings::DirectStoreOverrides>,
) -> (Pipeline, PathBuf, PathBuf) {
    new_direct_pipeline_on_machine(
        data_dir,
        intermediate_dir,
        complete_dir,
        db_path,
        buffer_config,
        total_connections,
        direct_store,
        SystemProfile {
            cpu: CpuProfile {
                physical_cores: 4,
                logical_cores: 4,
                simd: SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: DiskProfile {
                storage_class: StorageClass::Ssd,
                filesystem: FilesystemType::Apfs,
                sequential_write_mbps: 1000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        },
    )
    .await
}

// [`new_direct_pipeline_at_roots`] on a machine the test describes, for the
// tests whose subject is what the machine can honour.
#[allow(clippy::too_many_arguments)]
async fn new_direct_pipeline_on_machine(
    data_dir: PathBuf,
    intermediate_dir: PathBuf,
    complete_dir: PathBuf,
    db_path: PathBuf,
    buffer_config: BufferPoolConfig,
    total_connections: usize,
    direct_store: Option<crate::settings::DirectStoreOverrides>,
    profile: SystemProfile,
) -> (Pipeline, PathBuf, PathBuf) {
    let mut db = Database::open(&db_path).unwrap();
    // Jobs added through the handle persist their archive password, which
    // needs a key, exactly as a running server has one.
    db.set_encryption_key(crate::persistence::encryption::EncryptionKey::generate());
    let config: SharedConfig = Arc::new(RwLock::new(Config {
        data_dir: data_dir.display().to_string(),
        hardware_profile: None,
        intermediate_dir: Some(intermediate_dir.display().to_string()),
        complete_dir: Some(complete_dir.display().to_string()),
        buffer_pool: None,
        servers: vec![],
        categories: vec![],
        retry: None,
        max_download_speed: None,
        propagation_delay_secs: None,
        max_concurrent_nzbs: None,
        cleanup_after_extract: Some(true),
        watch_folder: crate::watch_folder::WatchFolderConfig::default(),
        duplicate_policy: Default::default(),
        direct_store,
        // Pinned off for the same reason as the harness above: an extraction
        // fixture must not start asserting the delivery-naming policy.
        delivery_naming: Some(crate::settings::DeliveryNamingOverrides {
            deobfuscate_delivered_members: Some(false),
            enable_srrdb_lookup: None,
        }),
        metrics: Default::default(),
        config_path: None,
    }));

    let (event_tx, _) = broadcast::channel::<PipelineEvent>(1024);
    let shared_state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
    // Dispatch asks the pool which server a new lane should try, so a harness
    // that is given connections needs a server to rank, and the lane workers
    // those leases go to will dial it. A harness with no connections keeps
    // its empty pool.
    let harness_servers = if total_connections == 0 {
        Vec::new()
    } else {
        vec![weaver_nntp::pool::ServerPoolConfig {
            server: weaver_nntp::ServerConfig {
                host: "127.0.0.1".to_string(),
                port: refusing_harness_server_port(),
                tls: false,
                ..Default::default()
            },
            max_connections: total_connections,
            ..weaver_nntp::pool::ServerPoolConfig::default()
        }]
    };
    let nntp = NntpClient::new(NntpClientConfig {
        servers: harness_servers,
        max_idle_age: Duration::from_secs(1),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    });
    let buffers = BufferPool::new(buffer_config);

    let pipeline = Pipeline::new(
        event_tx,
        nntp,
        buffers,
        profile,
        data_dir,
        intermediate_dir.clone(),
        complete_dir.clone(),
        total_connections,
        4,
        vec![],
        false,
        shared_state,
        db,
        config,
    )
    .await
    .unwrap();

    (pipeline, intermediate_dir, complete_dir)
}

async fn new_scheduler_pipeline(temp_dir: &TempDir) -> (Pipeline, PathBuf, PathBuf) {
    let (mut pipeline, intermediate, complete) = new_direct_pipeline(temp_dir).await;
    // Scheduler-only callers name two real policy entries without dialing.
    pipeline.nntp = Arc::new(NntpClient::new(NntpClientConfig {
        servers: (0..2)
            .map(|_| weaver_nntp::pool::ServerPoolConfig {
                max_connections: 100,
                ..Default::default()
            })
            .collect(),
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(15),
    }));
    (pipeline, intermediate, complete)
}

pub(crate) async fn new_direct_pipeline(temp_dir: &TempDir) -> (Pipeline, PathBuf, PathBuf) {
    new_direct_pipeline_with_buffers(
        temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        0,
    )
    .await
}

// End a pipeline incarnation before the next one opens its database.
//
// A process that exits takes every database handle with it at once; dropping
// a `Pipeline` does not. It drops only the pipeline's own `Database`: the
// writer task keeps committing what was queued to it, other holders keep
// their clones, and each pooled connection is closed later on its own sqlx
// thread — the last one in WAL mode under an exclusive lock while it
// checkpoints. A reopen that runs while any of that is still going on races
// it for the file, and loses with `database is locked` once the race outlasts
// the busy timeout.
//
// So the queued writes land first — the same writes the old writer task would
// have committed after the drop, now ordered before the reopen — and then the
// pool is closed, which waits for every checked-out connection to come back
// and closes each one before it returns.
//
// A test that stands the drop in for a crash retires the same way. The drop
// never lost a queued write — the writer task goes on committing its queue
// after the drop — so the flush moves those writes ahead of the reopen
// instead of racing it, and loses nothing a plain drop would have kept. What
// a crash does lose is the state that was never queued, and that stays lost.
pub(in crate::pipeline) async fn retire_pipeline_database(pipeline: Pipeline) {
    let db = pipeline.db.clone();
    drop(pipeline);
    db.flush_write_queue().await.unwrap();
    // `close` blocks on the database thread until the pool has closed, and a
    // connection checked out on this runtime returns through a task spawned
    // here; block elsewhere so that task can run.
    tokio::task::spawn_blocking(move || db.close())
        .await
        .expect("closing the database should not panic")
        .unwrap();
}

// Hear the run mail until the graph job has nothing out: a repair it lent
// returns there, and is applied as it is heard.
pub(in crate::pipeline) async fn hear_graph_mail(pipeline: &mut Pipeline, job_id: JobId) {
    while pipeline
        .graph_jobs
        .get(&job_id)
        .is_some_and(|job| job.awaits_mail())
    {
        let said = pipeline.graph_runs.recv().await.unwrap();
        pipeline.graph_run_said(said);
    }
}

// The checkpoint plan a download lane hands the decoder for `job_id`: the
// block sizes its graph job's recovery declares.
fn checkpoint_plan(pipeline: &Pipeline, job_id: JobId) -> weaver_yenc::CheckpointPlan {
    pipeline
        .graph_jobs
        .get(&job_id)
        .map_or(weaver_yenc::CheckpointPlan::None, |graph| {
            weaver_yenc::CheckpointPlan::from_slice_sizes(graph.block_sizes()).plan
        })
}

fn encode_article_part(
    filename: &str,
    payload: &[u8],
    part: u32,
    total: u32,
    begin: u64,
    file_size: u64,
) -> Bytes {
    let mut encoded = Vec::new();
    weaver_yenc::encode_part(
        payload,
        &mut encoded,
        128,
        filename,
        part,
        total,
        begin,
        begin + payload.len() as u64 - 1,
        file_size,
    )
    .unwrap();
    Bytes::from(encoded)
}

const TEST_RAR5_SIG: [u8; 8] = [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00];

fn encode_test_rar_vint(mut value: u64) -> Vec<u8> {
    let mut result = Vec::new();
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        result.push(byte);
        if value == 0 {
            break;
        }
    }
    result
}

fn build_test_rar_header(
    header_type: u64,
    common_flags: u64,
    type_body: &[u8],
    extra: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&encode_test_rar_vint(header_type));

    let mut flags = common_flags;
    if !extra.is_empty() {
        flags |= 0x0001;
    }
    body.extend_from_slice(&encode_test_rar_vint(flags));
    if !extra.is_empty() {
        body.extend_from_slice(&encode_test_rar_vint(extra.len() as u64));
    }
    body.extend_from_slice(type_body);
    body.extend_from_slice(extra);

    let header_size = body.len() as u64;
    let header_size_bytes = encode_test_rar_vint(header_size);
    let crc = checksum::crc32(&[header_size_bytes.as_slice(), body.as_slice()].concat());

    let mut result = Vec::new();
    result.extend_from_slice(&crc.to_le_bytes());
    result.extend_from_slice(&header_size_bytes);
    result.extend_from_slice(&body);
    result
}

fn build_test_rar_main_header(archive_flags: u64, volume_number: Option<u64>) -> Vec<u8> {
    let mut type_body = Vec::new();
    type_body.extend_from_slice(&encode_test_rar_vint(archive_flags));
    if let Some(volume_number) = volume_number {
        type_body.extend_from_slice(&encode_test_rar_vint(volume_number));
    }
    build_test_rar_header(1, 0, &type_body, &[])
}

fn build_test_rar_end_header(more_volumes: bool) -> Vec<u8> {
    let end_flags: u64 = if more_volumes { 0x0001 } else { 0 };
    build_test_rar_header(5, 0, &encode_test_rar_vint(end_flags), &[])
}

fn build_test_rar_file_header(
    filename: &str,
    common_flags_extra: u64,
    data_size: u64,
    unpacked_size: u64,
    data_crc: Option<u32>,
) -> Vec<u8> {
    build_test_rar_file_header_with_extra(
        filename,
        common_flags_extra,
        data_size,
        unpacked_size,
        data_crc,
        &[],
    )
}

// A RAR5 file header carrying only a BLAKE2sp digest — no CRC32.
//
// BLAKE2sp accepts bytes in order only, so a member whose *closing* header
// states one and nothing else can never be verified out of order.
fn build_test_rar_blake2_extra(digest: [u8; 32]) -> Vec<u8> {
    // `vint(record size) || vint(record type = FILE_HASH) || vint(algo =
    // BLAKE2sp) || digest`, where the size covers everything after itself.
    let mut record = Vec::new();
    record.extend_from_slice(&encode_test_rar_vint(2));
    record.extend_from_slice(&encode_test_rar_vint(0));
    record.extend_from_slice(&digest);
    let mut out = encode_test_rar_vint(record.len() as u64);
    out.extend_from_slice(&record);
    out
}

fn build_test_rar_file_header_with_extra(
    filename: &str,
    common_flags_extra: u64,
    data_size: u64,
    unpacked_size: u64,
    data_crc: Option<u32>,
    extra: &[u8],
) -> Vec<u8> {
    build_test_rar_data_header(
        2,
        filename,
        common_flags_extra,
        data_size,
        unpacked_size,
        data_crc,
        extra,
    )
}

// RAR5 compression info for a member that is **not** stored.
//
// The field is bit-packed — version in bits 0-5 (0 = RAR5), the per-member
// solid flag in bit 6, the method in bits 7-9 — and the stored-layout
// classifier reads exactly those three: a non-zero method makes a member
// `Compressed`, and the solid bit makes it `Solid`, which the classifier checks
// first. The data area a header like this describes is not really compressed,
// so a fixture using it is only good for what the *classification* decides.
fn test_rar_compression_info(method: u64, solid: bool) -> u64 {
    ((method & 0x07) << 7) | u64::from(solid) << 6
}

// A RAR5 file header whose member is compressed (and optionally solid).
fn build_test_rar_compressed_file_header(
    filename: &str,
    common_flags_extra: u64,
    data_size: u64,
    unpacked_size: u64,
    data_crc: Option<u32>,
    compression_info: u64,
) -> Vec<u8> {
    build_test_rar_data_header_with_compression(
        2,
        filename,
        common_flags_extra,
        data_size,
        unpacked_size,
        data_crc,
        &[],
        compression_info,
    )
}

// A RAR5 **directory** header: dataless, with a Unix mode and mtime.
//
// The shape every archiver writes a folder as — the DIRECTORY flag set, a zero
// data area, a zero unpacked size and no CRC32, because there is nothing to
// checksum. That absence is the point: it is what makes the entry ineligible
// for direct routing, and what a set of directory headers at the end of a
// closing volume used to demote a set for.
//
// `unix_mode` lands in the attributes field and `host_os` is Unix, which is how
// [`crate::pipeline::extraction::apply_rar_member_filesystem_metadata`] reads
// the mode back out.
fn build_test_rar_directory_header(name: &str, unix_mode: u32, mtime: u32) -> Vec<u8> {
    let file_flags: u64 = 0x0001 | 0x0002;
    let mut type_body = Vec::new();
    type_body.extend_from_slice(&encode_test_rar_vint(file_flags));
    type_body.extend_from_slice(&encode_test_rar_vint(0));
    type_body.extend_from_slice(&encode_test_rar_vint(u64::from(unix_mode)));
    type_body.extend_from_slice(&mtime.to_le_bytes());
    type_body.extend_from_slice(&encode_test_rar_vint(0));
    type_body.extend_from_slice(&encode_test_rar_vint(1));
    type_body.extend_from_slice(&encode_test_rar_vint(name.len() as u64));
    type_body.extend_from_slice(name.as_bytes());

    let mut body = Vec::new();
    body.extend_from_slice(&encode_test_rar_vint(2));
    body.extend_from_slice(&encode_test_rar_vint(0x0002));
    body.extend_from_slice(&encode_test_rar_vint(0));
    body.extend_from_slice(&type_body);

    let header_size = body.len() as u64;
    let header_size_bytes = encode_test_rar_vint(header_size);
    let crc = checksum::crc32(&[header_size_bytes.as_slice(), body.as_slice()].concat());

    let mut result = Vec::new();
    result.extend_from_slice(&crc.to_le_bytes());
    result.extend_from_slice(&header_size_bytes);
    result.extend_from_slice(&body);
    result
}

// A RAR5 **service** header (type 3) with a data area.
//
// Recovery records (`-rr`) and quick-open blocks take exactly this shape: the
// header body is a file header's, and the data area that follows belongs to no
// member — so a direct-store router files every byte of it into the envelope.
fn build_test_rar_service_header(name: &str, data_size: u64) -> Vec<u8> {
    build_test_rar_data_header(3, name, 0, data_size, data_size, None, &[])
}

#[allow(clippy::too_many_arguments)]
fn build_test_rar_data_header(
    header_type: u64,
    filename: &str,
    common_flags_extra: u64,
    data_size: u64,
    unpacked_size: u64,
    data_crc: Option<u32>,
    extra: &[u8],
) -> Vec<u8> {
    build_test_rar_data_header_with_compression(
        header_type,
        filename,
        common_flags_extra,
        data_size,
        unpacked_size,
        data_crc,
        extra,
        0,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_test_rar_data_header_with_compression(
    header_type: u64,
    filename: &str,
    common_flags_extra: u64,
    data_size: u64,
    unpacked_size: u64,
    data_crc: Option<u32>,
    extra: &[u8],
    compression_info: u64,
) -> Vec<u8> {
    let file_flags: u64 = if data_crc.is_some() { 0x0004 } else { 0 };
    let mut type_body = Vec::new();
    type_body.extend_from_slice(&encode_test_rar_vint(file_flags));
    type_body.extend_from_slice(&encode_test_rar_vint(unpacked_size));
    type_body.extend_from_slice(&encode_test_rar_vint(0o644));
    if let Some(data_crc) = data_crc {
        type_body.extend_from_slice(&data_crc.to_le_bytes());
    }
    type_body.extend_from_slice(&encode_test_rar_vint(compression_info));
    type_body.extend_from_slice(&encode_test_rar_vint(1));
    type_body.extend_from_slice(&encode_test_rar_vint(filename.len() as u64));
    type_body.extend_from_slice(filename.as_bytes());

    // Field order is fixed by the format: type, flags, extra size (when the
    // extra-area flag is set), data size, type body, extra area last.
    let mut common_flags = 0x0002 | common_flags_extra;
    if !extra.is_empty() {
        common_flags |= 0x0001;
    }
    let mut body = Vec::new();
    body.extend_from_slice(&encode_test_rar_vint(header_type));
    body.extend_from_slice(&encode_test_rar_vint(common_flags));
    if !extra.is_empty() {
        body.extend_from_slice(&encode_test_rar_vint(extra.len() as u64));
    }
    body.extend_from_slice(&encode_test_rar_vint(data_size));
    body.extend_from_slice(&type_body);
    body.extend_from_slice(extra);

    let header_size = body.len() as u64;
    let header_size_bytes = encode_test_rar_vint(header_size);
    let crc = checksum::crc32(&[header_size_bytes.as_slice(), body.as_slice()].concat());

    let mut result = Vec::new();
    result.extend_from_slice(&crc.to_le_bytes());
    result.extend_from_slice(&header_size_bytes);
    result.extend_from_slice(&body);
    result
}

const TEST_RAR4_SIG: [u8; 7] = [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x00];

// RAR4's fixed-layout common header: `crc16, type, flags, header size`.
//
// The CRC16 is left zero — the parser warns on a mismatch and carries on, which
// is deliberate recovery behaviour, so a fixture does not need to compute it.
fn build_test_rar4_block(header_type: u8, flags: u16, body: &[u8]) -> Vec<u8> {
    let header_size = (7 + body.len()) as u16;
    let mut out = Vec::with_capacity(header_size as usize);
    out.extend_from_slice(&0u16.to_le_bytes());
    out.push(header_type);
    out.extend_from_slice(&flags.to_le_bytes());
    out.extend_from_slice(&header_size.to_le_bytes());
    out.extend_from_slice(body);
    out
}

fn build_test_rar4_main_header(is_first_volume: bool) -> Vec<u8> {
    // VOLUME | NEW_NUMBERING, plus FIRST_VOLUME on volume 0.
    let mut flags = 0x0001u16 | 0x0010;
    if is_first_volume {
        flags |= 0x0100;
    }
    let mut body = Vec::new();
    body.extend_from_slice(&0u16.to_le_bytes()); // high_pos_av
    body.extend_from_slice(&0u32.to_le_bytes()); // pos_av
    build_test_rar4_block(0x73, flags, &body)
}

fn build_test_rar4_end_header(more_volumes: bool) -> Vec<u8> {
    let flags: u16 = if more_volumes { 0x0001 } else { 0 };
    build_test_rar4_block(0x7b, flags, &[])
}

// A single stored member split across an **old-numbering** RAR4 set —
// `<base>.rar`, `<base>.r00`, `<base>.r01`, … — the one multi-volume RAR
// shape whose headers state no per-volume number anywhere: RAR4's main header
// has no number field (that is RAR5's), and these end records carry no
// `VOLUME_NUMBER` flag (a numbered `ENDARC` is what modern `.partNN` writers
// emit). Every volume of such a set parses as volume 0; only the filename
// says which volume a file is.
pub(in crate::pipeline::tests) fn single_member_rar4_old_numbering_set(
    base: &str,
    member_name: &str,
    payload: &[u8],
    volume_count: usize,
) -> Vec<(String, Vec<u8>)> {
    let member_crc = checksum::crc32(payload);
    let chunk = payload.len().div_ceil(volume_count);
    (0..volume_count)
        .map(|volume| {
            let start = (volume * chunk).min(payload.len());
            let end = ((volume + 1) * chunk).min(payload.len());
            let part = &payload[start..end];
            let is_first = volume == 0;
            let is_last = volume + 1 == volume_count;
            let mut split_flags = 0u16;
            if !is_first {
                split_flags |= 0x0001;
            }
            if !is_last {
                split_flags |= 0x0002;
            }
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR4_SIG);
            // VOLUME only — no NEW_NUMBERING, exactly like the old scheme.
            bytes.extend_from_slice(&build_test_rar4_block(0x73, 0x0001, &[0u8; 6]));
            bytes.extend_from_slice(&build_test_rar4_file_header(
                member_name,
                split_flags,
                part.len() as u32,
                payload.len() as u32,
                if is_last {
                    member_crc
                } else {
                    checksum::crc32(part)
                },
            ));
            bytes.extend_from_slice(part);
            bytes.extend_from_slice(&build_test_rar4_end_header(!is_last));
            let filename = if is_first {
                format!("{base}.rar")
            } else {
                format!("{base}.r{:02}", volume - 1)
            };
            (filename, bytes)
        })
        .collect()
}

// Fill in the CRC16 of every RAR4 block in `bytes`. The shared fixture
// builders leave it zero because weaver's own parser only warns on a
// mismatch; the recovery restorer's validity probe does not, and a set
// written to exercise it has to be one it would accept.
pub(in crate::pipeline::tests) fn with_rar4_header_crcs(mut bytes: Vec<u8>) -> Vec<u8> {
    let mut offset = TEST_RAR4_SIG.len();
    while offset + 7 <= bytes.len() {
        let flags = u16::from_le_bytes([bytes[offset + 3], bytes[offset + 4]]);
        let header_size = u16::from_le_bytes([bytes[offset + 5], bytes[offset + 6]]) as usize;
        let data_size = if flags & 0x8000 != 0 {
            u32::from_le_bytes([
                bytes[offset + 7],
                bytes[offset + 8],
                bytes[offset + 9],
                bytes[offset + 10],
            ]) as usize
        } else {
            0
        };
        let crc = (checksum::crc32(&bytes[offset + 2..offset + header_size]) & 0xFFFF) as u16;
        bytes[offset..offset + 2].copy_from_slice(&crc.to_le_bytes());
        offset += header_size + data_size;
    }
    bytes
}

// A RAR3 standalone recovery volume for `volumes`: the set's RS parity over
// every data volume, padded to the longest, followed by the 7-byte footer
// (`data-1`, `rec-1`, `position-1`, CRC32) a new-style `.rev` carries and the
// restorer reads its geometry from.
pub(in crate::pipeline::tests) fn single_recovery_volume_for_rar4_set(
    volumes: &[&[u8]],
) -> Vec<u8> {
    let width = volumes.iter().map(|volume| volume.len()).max().unwrap_or(0);
    let coder = reedsolomon_rs::rar3::Rar3RsCoder::new(1).expect("one recovery volume");
    let mut rev = Vec::with_capacity(width + 7);
    let mut column = vec![0u8; volumes.len()];
    let mut parity = [0u8; 1];
    for position in 0..width {
        for (slot, volume) in column.iter_mut().zip(volumes) {
            *slot = volume.get(position).copied().unwrap_or(0);
        }
        coder.encode(&column, &mut parity);
        rev.push(parity[0]);
    }
    rev.push((volumes.len() - 1) as u8);
    rev.push(0);
    rev.push(0);
    let crc = checksum::crc32(&rev);
    rev.extend_from_slice(&crc.to_le_bytes());
    rev
}

// A stored RAR4 file header. `unpack_version` 29 is what makes a split part's
// CRC32 describe that part's packed bytes rather than nothing at all.
fn build_test_rar4_file_header(
    filename: &str,
    split_flags: u16,
    packed_size: u32,
    unpacked_size: u32,
    data_crc: u32,
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&packed_size.to_le_bytes());
    body.extend_from_slice(&unpacked_size.to_le_bytes());
    body.push(3); // host OS: Unix
    body.extend_from_slice(&data_crc.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes()); // mtime
    body.push(29); // unpack version
    body.push(0x30); // method: store
    body.extend_from_slice(&(filename.len() as u16).to_le_bytes());
    body.extend_from_slice(&0o644u32.to_le_bytes());
    body.extend_from_slice(filename.as_bytes());
    build_test_rar4_block(0x74, 0x8000 | split_flags, &body)
}

fn build_multifile_multivolume_rar_set() -> Vec<(String, Vec<u8>)> {
    let episode_a = b"episode-a-payload";
    let episode_b = b"episode-b-payload";
    let episode_a_crc = checksum::crc32(episode_a);
    let episode_b_crc = checksum::crc32(episode_b);

    let a_part1 = &episode_a[..8];
    let a_part2 = &episode_a[8..];
    let b_part1 = &episode_b[..8];
    let b_part2 = &episode_b[8..];

    let mut part01 = Vec::new();
    part01.extend_from_slice(&TEST_RAR5_SIG);
    part01.extend_from_slice(&build_test_rar_main_header(0x0001, None));
    part01.extend_from_slice(&build_test_rar_file_header(
        "E01.mkv",
        0x0010,
        a_part1.len() as u64,
        episode_a.len() as u64,
        None,
    ));
    part01.extend_from_slice(a_part1);
    part01.extend_from_slice(&build_test_rar_end_header(true));

    let mut part02 = Vec::new();
    part02.extend_from_slice(&TEST_RAR5_SIG);
    part02.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(1)));
    part02.extend_from_slice(&build_test_rar_file_header(
        "E01.mkv",
        0x0008,
        a_part2.len() as u64,
        episode_a.len() as u64,
        Some(episode_a_crc),
    ));
    part02.extend_from_slice(a_part2);
    part02.extend_from_slice(&build_test_rar_end_header(true));

    let mut part03 = Vec::new();
    part03.extend_from_slice(&TEST_RAR5_SIG);
    part03.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(2)));
    part03.extend_from_slice(&build_test_rar_file_header(
        "E02.mkv",
        0x0010,
        b_part1.len() as u64,
        episode_b.len() as u64,
        None,
    ));
    part03.extend_from_slice(b_part1);
    part03.extend_from_slice(&build_test_rar_end_header(true));

    let mut part04 = Vec::new();
    part04.extend_from_slice(&TEST_RAR5_SIG);
    part04.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(3)));
    part04.extend_from_slice(&build_test_rar_file_header(
        "E02.mkv",
        0x0008,
        b_part2.len() as u64,
        episode_b.len() as u64,
        Some(episode_b_crc),
    ));
    part04.extend_from_slice(b_part2);
    part04.extend_from_slice(&build_test_rar_end_header(false));

    vec![
        ("show.part01.rar".to_string(), part01),
        ("show.part02.rar".to_string(), part02),
        ("show.part03.rar".to_string(), part03),
        ("show.part04.rar".to_string(), part04),
    ]
}

fn rar_job_spec(name: &str, files: &[(String, Vec<u8>)]) -> JobSpec {
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: files.iter().map(|(_, bytes)| bytes.len() as u64).sum(),
        category: None,
        metadata: vec![],
        files: files
            .iter()
            .enumerate()
            .map(|(index, (filename, bytes))| FileSpec {
                filename: filename.clone(),
                role: FileRole::from_filename(filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: bytes.len() as u32,
                    message_id: format!("rar-{index}@example.com"),
                }],
            })
            .collect(),
    }
}

// Article `segment_number` of file `file_index` in job `job_id`.
pub(in crate::pipeline) fn segment_id(
    job_id: JobId,
    file_index: u32,
    segment_number: u32,
) -> SegmentId {
    SegmentId {
        file_id: NzbFileId { job_id, file_index },
        segment_number,
    }
}

pub(in crate::pipeline) fn standalone_job_spec(name: &str, files: &[(String, u32)]) -> JobSpec {
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: files.iter().map(|(_, bytes)| *bytes as u64).sum(),
        category: None,
        metadata: vec![],
        files: files
            .iter()
            .enumerate()
            .map(|(index, (filename, bytes))| FileSpec {
                filename: filename.clone(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: *bytes,
                    message_id: format!("segment-{index}@example.com"),
                }],
            })
            .collect(),
    }
}

fn many_standalone_files(prefix: &str, count: usize) -> Vec<(String, u32)> {
    (0..count)
        .map(|idx| (format!("{prefix}-{idx}.bin"), 512u32))
        .collect()
}

fn standalone_with_par2_job_spec(name: &str, payload_bytes: u32, recovery_bytes: u32) -> JobSpec {
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: (payload_bytes + recovery_bytes + 16) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: "payload.bin".to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: payload_bytes,
                    message_id: "payload@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "repair.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: true,
                    recovery_block_count: 0,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 16,
                    message_id: "repair-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: "repair.vol00+01.par2".to_string(),
                role: FileRole::Par2 {
                    is_index: false,
                    recovery_block_count: 1,
                },
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: recovery_bytes,
                    message_id: "repair-volume@example.com".to_string(),
                }],
            },
        ],
    }
}

fn build_test_par2_packet(
    packet_type: &[u8; 16],
    body: &[u8],
    recovery_set_id: [u8; 16],
) -> Vec<u8> {
    let length = (par2_rs::packet::header::HEADER_SIZE + body.len()) as u64;
    let mut hash_input = Vec::new();
    hash_input.extend_from_slice(&recovery_set_id);
    hash_input.extend_from_slice(packet_type);
    hash_input.extend_from_slice(body);
    let packet_hash = checksum::md5(&hash_input);

    let mut data = Vec::new();
    data.extend_from_slice(par2_rs::packet::header::MAGIC);
    data.extend_from_slice(&length.to_le_bytes());
    data.extend_from_slice(&packet_hash);
    data.extend_from_slice(&recovery_set_id);
    data.extend_from_slice(packet_type);
    data.extend_from_slice(body);
    data
}

pub(super) fn build_test_par2_index(filename: &str, file_data: &[u8], slice_size: u64) -> Vec<u8> {
    build_test_par2_index_for_files(&[(filename, file_data)], slice_size)
}

pub(super) fn build_test_par2_index_for_files(files: &[(&str, &[u8])], slice_size: u64) -> Vec<u8> {
    struct IndexedFile {
        id: [u8; 16],
        desc_body: Vec<u8>,
        ifsc_body: Vec<u8>,
    }

    let indexed: Vec<IndexedFile> = files
        .iter()
        .map(|(filename, file_data)| {
            let file_length = file_data.len() as u64;
            let hash_full = checksum::md5(file_data);
            let hash_16k = checksum::md5(&file_data[..file_data.len().min(16 * 1024)]);

            let mut file_id_input = Vec::new();
            file_id_input.extend_from_slice(&hash_16k);
            file_id_input.extend_from_slice(&file_length.to_le_bytes());
            file_id_input.extend_from_slice(filename.as_bytes());
            let id = checksum::md5(&file_id_input);

            let num_slices = if file_length == 0 {
                0
            } else {
                file_length.div_ceil(slice_size) as usize
            };

            let mut desc_body = Vec::new();
            desc_body.extend_from_slice(&id);
            desc_body.extend_from_slice(&hash_full);
            desc_body.extend_from_slice(&hash_16k);
            desc_body.extend_from_slice(&file_length.to_le_bytes());
            desc_body.extend_from_slice(filename.as_bytes());
            while desc_body.len() % 4 != 0 {
                desc_body.push(0);
            }

            let mut ifsc_body = Vec::new();
            ifsc_body.extend_from_slice(&id);
            for slice_index in 0..num_slices {
                let start = slice_index as u64 * slice_size;
                let end = ((start + slice_size) as usize).min(file_data.len());
                let slice_data = &file_data[start as usize..end];
                let mut state = par2_rs::SliceChecksumState::new();
                state.update(slice_data);
                let (crc32, md5) =
                    state.finalize(((slice_data.len() as u64) < slice_size).then_some(slice_size));
                ifsc_body.extend_from_slice(&md5);
                ifsc_body.extend_from_slice(&crc32.to_le_bytes());
            }

            IndexedFile {
                id,
                desc_body,
                ifsc_body,
            }
        })
        .collect();

    let mut main_body = Vec::new();
    main_body.extend_from_slice(&slice_size.to_le_bytes());
    main_body.extend_from_slice(&(indexed.len() as u32).to_le_bytes());
    for file in &indexed {
        main_body.extend_from_slice(&file.id);
    }
    let recovery_set_id = checksum::md5(&main_body);

    let mut stream = Vec::new();
    stream.extend_from_slice(&build_test_par2_packet(
        par2_rs::packet::header::TYPE_MAIN,
        &main_body,
        recovery_set_id,
    ));
    for file in &indexed {
        stream.extend_from_slice(&build_test_par2_packet(
            par2_rs::packet::header::TYPE_FILE_DESC,
            &file.desc_body,
            recovery_set_id,
        ));
        stream.extend_from_slice(&build_test_par2_packet(
            par2_rs::packet::header::TYPE_IFSC,
            &file.ifsc_body,
            recovery_set_id,
        ));
    }
    stream
}

fn segmented_job_spec(name: &str, filename: &str, segment_sizes: &[u32]) -> JobSpec {
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: segment_sizes.iter().map(|size| *size as u64).sum(),
        category: None,
        metadata: vec![],
        files: vec![FileSpec {
            filename: filename.to_string(),
            role: FileRole::Standalone,
            groups: vec!["alt.binaries.test".to_string()],
            posted_at_epoch: None,
            segments: segment_sizes
                .iter()
                .enumerate()
                .map(|(index, bytes)| {
                    segment_spec! {
                        number: index as u32,
                        bytes: *bytes,
                        message_id: format!("segment-{index}@example.com"),
                    }
                })
                .collect(),
        }],
    }
}

fn with_priority(mut spec: JobSpec, priority: &str) -> JobSpec {
    spec.metadata = vec![("priority".to_string(), priority.to_string())];
    spec
}

// Record cancellation, drive its boundary, then observe the durable archive.
async fn cancel_one_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    origin: crate::jobs::handle::CancellationOrigin,
) -> Result<(), crate::SchedulerError> {
    pipeline.report_all_for_test();
    let mut events = pipeline.event_tx.subscribe();
    pipeline
        .shared_state
        .queue
        .cancel_jobs(BTreeSet::from([job_id]), origin)
        .remove(&job_id)
        .unwrap()?;
    pipeline.apply_queue_intents().await;
    finish_cancel_jobs(pipeline).await;
    loop {
        if matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id: id } if id == job_id)
        {
            return Ok(());
        }
    }
}

// Hands the actor the archive outcome of the one cancel batch in flight.
async fn finish_cancel_jobs(pipeline: &mut Pipeline) {
    match pipeline
        .terminal_post_processing_done_rx
        .recv()
        .await
        .unwrap()
    {
        TerminalPostProcessingEvent::CancelJobsDone(done) => pipeline.handle_cancel_jobs_done(done),
        _ => panic!("expected the cancel's archive done event"),
    }
}

pub(in crate::pipeline) async fn insert_active_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    spec: JobSpec,
) -> PathBuf {
    let nzb_zstd = nzb_zstd_for(&spec);
    insert_active_job_with_persisted_nzb(pipeline, job_id, spec, nzb_zstd).await
}

async fn insert_active_job_with_persisted_nzb(
    pipeline: &mut Pipeline,
    job_id: JobId,
    spec: JobSpec,
    nzb_zstd: Vec<u8>,
) -> PathBuf {
    insert_active_job_with_persisted_nzb_named(pipeline, job_id, spec, nzb_zstd, None).await
}

// [`insert_active_job_with_persisted_nzb`] with the persisted NZB's **file
// name** chosen by the caller.
//
// The name is not decoration: `nzb_password_candidates` reads a
// `{{password}}` convention out of the NZB path's stem, so it is the only way
// to exercise that candidate source. `None` keeps the job-id name every other
// caller gets.
async fn insert_active_job_with_persisted_nzb_named(
    pipeline: &mut Pipeline,
    job_id: JobId,
    spec: JobSpec,
    nzb_zstd: Vec<u8>,
    nzb_file_name: Option<&str>,
) -> PathBuf {
    let nzb_file_name = nzb_file_name
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{}.nzb", job_id.0));
    let dir_name = crate::jobs::working_dir::sanitize_dirname(&spec.name);
    let candidate = pipeline.intermediate_dir.join(&dir_name);
    let working_dir = if candidate.exists() {
        pipeline
            .intermediate_dir
            .join(format!("{}.#{}", dir_name, job_id.0))
    } else {
        candidate
    };
    tokio::fs::create_dir_all(&working_dir).await.unwrap();
    tokio::fs::write(
        crate::jobs::working_dir::working_dir_marker_path(&working_dir),
        [],
    )
    .await
    .unwrap();
    pipeline
        .db
        .create_active_job(&crate::ActiveJob {
            job_id,
            nzb_hash: [0; 32],
            nzb_path: working_dir.join(&nzb_file_name),
            nzb_zstd,
            output_dir: working_dir.clone(),
            created_at: 0,
            category: spec.category.clone(),
            metadata: spec.metadata.clone(),
            status: "downloading",
            download_state: "downloading",
            post_state: "idle",
            run_state: "active",
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            password_override: None,
            supplied_password_candidates: Vec::new(),
        })
        .unwrap();
    // This fixture represents work already admitted, not queued intake.
    pipeline.db.set_nzb_admitted(job_id, true).unwrap();
    pipeline.nzb_admitted.insert(job_id);
    pipeline.nzb_seen.insert(job_id);
    let par2_bytes = spec.par2_bytes();
    pipeline.jobs.insert(
        job_id,
        JobState {
            job_id,
            job_hash: [0; 32],
            spec,
            status: JobStatus::Downloading,
            download_state: crate::jobs::model::DownloadState::Downloading,
            post_state: crate::jobs::model::PostState::Idle,
            run_state: crate::jobs::model::RunState::Active,
            articles: crate::jobs::model::JobArticles::Queued(Default::default()),
            created_at: std::time::Instant::now(),
            created_at_epoch_ms: crate::jobs::model::epoch_ms_now(),
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            failure_error: None,
            working_dir: working_dir.clone(),
            downloaded_bytes: 0,
            restored_download_floor_bytes: 0,
            downloaded_wire_bytes: 0,
            server_attribution: Default::default(),
            failed_bytes: 0,
            par2_bytes,
            support_facts: Default::default(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            staging_dir: None,
            category_bytes: None,
            nzb_password_candidates: Default::default(),
        },
    );
    // The fixture's articles are built as a job's first handout builds them.
    pipeline.activate_job_articles(job_id, &HashSet::new(), None, None);
    pipeline.job_order.push(job_id);
    pipeline.report_all_for_test();
    working_dir
}

pub(in crate::pipeline) fn rar5_fixture_bytes(name: &str) -> Vec<u8> {
    std::fs::read(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/rar5")
            .join(name),
    )
    .unwrap()
}

async fn settle_inflight_moves(pipeline: &mut Pipeline) {
    while !pipeline.inflight_moves.is_empty() {
        // The same bound the terminal-state driver gives a move: a loaded
        // Windows runner can take seconds to close handles and scan the tree.
        let done = pipeline
            .move_done_rx
            .recv()
            .await
            .expect("move channel should stay open");
        pipeline.handle_move_to_complete_done(done).await;
        pipeline.pump_decode_queue();
        while let Some(queued_job) = pipeline.pending_completion_checks.pop_front() {
            pipeline.check_job_completion(queued_job).await;
            pipeline.pump_decode_queue();
        }
    }

    while let Ok(done) = pipeline.move_done_rx.try_recv() {
        pipeline.handle_move_to_complete_done(done).await;
        pipeline.pump_decode_queue();
        while let Some(queued_job) = pipeline.pending_completion_checks.pop_front() {
            pipeline.check_job_completion(queued_job).await;
            pipeline.pump_decode_queue();
        }
    }
}

fn set_job_status_for_test(pipeline: &mut Pipeline, job_id: JobId, status: JobStatus) {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.status = status;
    state.refresh_runtime_lanes_from_status();
}

pub(in crate::pipeline) async fn submit_decoded_segment(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    segment_number: u32,
    file_offset: u64,
    data: &[u8],
    filename: &str,
    expected_file_crc: Option<u32>,
) {
    submit_decoded_segment_with_part_crc_verified(
        pipeline,
        file_id,
        segment_number,
        file_offset,
        data,
        filename,
        expected_file_crc,
        true,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn submit_decoded_segment_with_part_crc_verified(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    segment_number: u32,
    file_offset: u64,
    data: &[u8],
    filename: &str,
    expected_file_crc: Option<u32>,
    part_crc_verified: bool,
) {
    submit_decoded_segment_with_segments(
        pipeline,
        file_id,
        segment_number,
        file_offset,
        data,
        filename,
        expected_file_crc,
        part_crc_verified,
        None,
    )
    .await;
}

// [`submit_decoded_segment_with_part_crc_verified`] with the decoder's CRC
// segmentation chosen by the caller.
//
// `None` is what a decoder emits with no PAR2 block size declared: one segment
// covering the whole article. `Some` is what it emits once the recovery set has
// parsed and the lease carries its block size — segments cut on the block grid,
// which is the only shape the dual-CRC grid can close a block from.
#[allow(clippy::too_many_arguments)]
async fn submit_decoded_segment_with_segments(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    segment_number: u32,
    file_offset: u64,
    data: &[u8],
    filename: &str,
    expected_file_crc: Option<u32>,
    part_crc_verified: bool,
    segments: Option<Vec<weaver_yenc::Segment>>,
) {
    let declared_file_len = pipeline
        .jobs
        .get(&file_id.job_id)
        .and_then(|state| state.articles.assembly()?.file(file_id))
        .expect("active test file assembly")
        .total_bytes();
    submit_decoded_segment_declaring(
        pipeline,
        file_id,
        segment_number,
        file_offset,
        data,
        filename,
        expected_file_crc,
        part_crc_verified,
        segments,
        declared_file_len,
    )
    .await;
}

// [`submit_decoded_segment_with_segments`] with the length the article's
// `=ybegin size=` states chosen by the caller.
//
// The default above states the assembly's total, which is the NZB's sum of
// encoded segment sizes. A real post states the decoded length there, so a
// caller that knows the file's true length — every volume-aware helper does —
// states that instead; the two only coincide when the spec pretends its
// articles are not yEnc-inflated.
#[allow(clippy::too_many_arguments)]
async fn submit_decoded_segment_declaring(
    pipeline: &mut Pipeline,
    file_id: NzbFileId,
    segment_number: u32,
    file_offset: u64,
    data: &[u8],
    filename: &str,
    expected_file_crc: Option<u32>,
    part_crc_verified: bool,
    segments: Option<Vec<weaver_yenc::Segment>>,
    declared_file_len: u64,
) {
    let file = pipeline
        .jobs
        .get(&file_id.job_id)
        .and_then(|state| state.articles.assembly()?.file(file_id))
        .expect("active test file assembly");
    let yenc_layout = YencLayoutAssertions {
        file_size: declared_file_len,
        part: Some(segment_number + 1),
        total: Some(file.total_segments()),
        begin: Some(file_offset + 1),
        end: Some(file_offset + data.len() as u64),
    };
    let segments = segments.unwrap_or_else(|| {
        vec![weaver_yenc::Segment {
            file_offset: yenc_layout.begin.map_or(0, |begin| begin.saturating_sub(1)),
            len: data.len() as u64,
            crc32: par2_rs::checksum::crc32(data),
        }]
    });
    pipeline
        .handle_decode_success(
            DecodeResult {
                encoding: SegmentEncoding::Yenc,
                segment_id: SegmentId {
                    file_id,
                    segment_number,
                },
                raw_size: data.len() as u64,
                yenc_layout,
                crc_valid: true,
                part_crc_verified,
                part_crc: par2_rs::checksum::crc32(data),
                truncation_suspected: false,
                expected_file_crc,
                data: DecodedChunk::from(data.to_vec()),
                yenc_name: filename.to_string(),
                checkpoint_plan: checkpoint_plan(pipeline, file_id.job_id),
                segments,
            },
            SegmentSource {
                source_server_idx: None,
                exclude_servers: Vec::new(),
            },
        )
        .await;
}

pub(in crate::pipeline) fn job_status_for_assert(
    pipeline: &Pipeline,
    job_id: JobId,
) -> Option<JobStatus> {
    pipeline
        .jobs
        .get(&job_id)
        .map(|state| state.status.clone())
        .or_else(|| pipeline.shared_state.get_job(job_id).map(|job| job.status))
}

fn drain_job_events(
    events: &mut tokio::sync::broadcast::Receiver<PipelineEvent>,
    job_id: JobId,
) -> Vec<PipelineEvent> {
    let mut drained = Vec::new();
    while let Ok(event) = events.try_recv() {
        if crate::events::publish::pipeline_job_id(&event) == Some(job_id.0) {
            drained.push(event);
        }
    }
    drained
}

async fn pump_pipeline_runtime_queues(pipeline: &mut Pipeline) {
    pipeline.pump_decode_queue();
    while let Some(queued_job) = pipeline.pending_completion_checks.pop_front() {
        pipeline.check_job_completion(queued_job).await;
        pipeline.pump_decode_queue();
    }
    settle_inflight_moves(pipeline).await;
}

// Polls until `predicate` holds. There is no deadline: the test runner
// bounds a condition that never arrives.
async fn wait_until(mut predicate: impl FnMut() -> bool) {
    while !predicate() {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn drain_decode_results(pipeline: &mut Pipeline, expected: usize) {
    for index in 0..expected {
        let done = pipeline.decode_done_rx.recv().await.unwrap_or_else(|| {
            panic!(
                "decode channel closed before result {}/{}",
                index + 1,
                expected
            )
        });
        pipeline.handle_decode_done(done).await;
        settle_inflight_moves(pipeline).await;
    }
    settle_inflight_moves(pipeline).await;
}

// Fill servers with the given retention windows (days, 0 = unlimited). No
// connections are opened — only the config matters.
fn retention_client(retention_days: &[u32]) -> weaver_nntp::client::NntpClient {
    let servers = retention_days
        .iter()
        .enumerate()
        .map(|(idx, days)| weaver_nntp::pool::ServerPoolConfig {
            server: weaver_nntp::ServerConfig {
                host: format!("retention-{idx}.example.com"),
                ..Default::default()
            },
            max_connections: 2,
            retention_days: *days,
            ..weaver_nntp::pool::ServerPoolConfig::default()
        })
        .collect();
    weaver_nntp::client::NntpClient::new(weaver_nntp::client::NntpClientConfig {
        servers,
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(1),
    })
}

// Two fill servers: index 0 holds 5 days of retention, index 1 is unlimited.
fn two_server_retention_client() -> weaver_nntp::client::NntpClient {
    retention_client(&[5, 0])
}

fn age_job_days(pipeline: &mut Pipeline, job_id: JobId, days: u64) {
    let posted = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - days * 86_400;
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    for file in &mut state.spec.files {
        file.posted_at_epoch = Some(posted);
    }
}
