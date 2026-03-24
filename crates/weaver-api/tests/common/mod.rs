// Test helper module — not all items are used by every test binary.
#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_graphql::{Request, Response, Variables};
use serde_json::Value;
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio::task::JoinHandle;

use weaver_api::auth::CallerScope;
use weaver_api::{RssService, WeaverSchema, build_schema};
use weaver_core::config::{Config, SharedConfig};
use weaver_core::event::PipelineEvent;
use weaver_core::id::JobId;
use weaver_scheduler::download_queue::DownloadQueue;
use weaver_scheduler::handle::{JobInfo, SharedPipelineState};
use weaver_scheduler::job::{JobState, JobStatus, epoch_ms_now};
use weaver_scheduler::metrics::PipelineMetrics;
use weaver_scheduler::{SchedulerCommand, SchedulerHandle};
use weaver_state::Database;

use weaver_assembly::JobAssembly;

/// Self-contained test harness that provides a fully-wired GraphQL schema
/// backed by an in-memory database and a mock scheduler.
///
/// Each test should create its own harness for full isolation.
#[allow(dead_code)]
pub struct TestHarness {
    pub schema: WeaverSchema,
    pub handle: SchedulerHandle,
    pub config: SharedConfig,
    pub db: Database,
    _scheduler_task: JoinHandle<()>,
    _tempdir: tempfile::TempDir,
}

impl TestHarness {
    /// Create a new test harness with in-memory DB, default config, mock
    /// scheduler, and a real GraphQL schema.
    pub async fn new() -> Self {
        let tempdir = tempfile::TempDir::new().expect("failed to create tempdir");
        let db = Database::open_in_memory().expect("failed to open in-memory DB");

        let config = Config {
            data_dir: tempdir.path().to_string_lossy().to_string(),
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            tuner: None,
            servers: vec![],
            categories: vec![],
            retry: None,
            max_download_speed: None,
            cleanup_after_extract: None,
            isp_bandwidth_cap: None,
            config_path: None,
        };
        let shared_config: SharedConfig = Arc::new(RwLock::new(config));

        let (handle, scheduler_task) = spawn_test_scheduler();
        let rss = RssService::new(handle.clone(), shared_config.clone(), db.clone());
        let shared_schedules: weaver_scheduler::schedule::SharedSchedules =
            Arc::new(RwLock::new(vec![]));

        let schema = build_schema(
            handle.clone(),
            shared_config.clone(),
            db.clone(),
            rss,
            shared_schedules,
            weaver_core::log_buffer::LogRingBuffer::with_default_capacity(),
        );

        Self {
            schema,
            handle,
            config: shared_config,
            db,
            _scheduler_task: scheduler_task,
            _tempdir: tempdir,
        }
    }

    /// Execute a GraphQL query/mutation with full admin access.
    pub async fn execute(&self, query: &str) -> Response {
        let request = Request::new(query).data(CallerScope::Local);
        self.schema.execute(request).await
    }

    /// Execute a GraphQL query/mutation with variables.
    #[allow(dead_code)]
    pub async fn execute_with_variables(&self, query: &str, variables: Variables) -> Response {
        let request = Request::new(query)
            .data(CallerScope::Local)
            .variables(variables);
        self.schema.execute(request).await
    }

    /// Submit a minimal test NZB and return the job ID.
    pub async fn submit_test_nzb(&self, name: &str) -> u64 {
        self.submit_test_nzb_with_options(name, None, None, &[])
            .await
    }

    /// Submit a test NZB with options, returning the job ID.
    pub async fn submit_test_nzb_with_options(
        &self,
        name: &str,
        category: Option<&str>,
        password: Option<&str>,
        metadata: &[(&str, &str)],
    ) -> u64 {
        let nzb = make_test_nzb(name);
        let nzb_b64 =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, nzb.as_bytes());

        let cat_arg = category
            .map(|c| format!(r#", category: "{c}""#))
            .unwrap_or_default();
        let pwd_arg = password
            .map(|p| format!(r#", password: "{p}""#))
            .unwrap_or_default();
        let meta_arg = if metadata.is_empty() {
            String::new()
        } else {
            let entries: Vec<String> = metadata
                .iter()
                .map(|(k, v)| format!(r#"{{ key: "{k}", value: "{v}" }}"#))
                .collect();
            format!(", metadata: [{}]", entries.join(", "))
        };

        let query = format!(
            r#"mutation {{
                submitNzb(source: {{ nzbBase64: "{nzb_b64}" }}, filename: "{name}.nzb"{cat_arg}{pwd_arg}{meta_arg}) {{
                    id
                    name
                    status
                }}
            }}"#
        );

        let resp = self.execute(&query).await;
        assert_no_errors(&resp);
        let data = resp.data.into_json().unwrap();
        data["submitNzb"]["id"].as_u64().expect("missing job id")
    }
}

/// Assert that a GraphQL response has no errors.
pub fn assert_no_errors(response: &Response) {
    assert!(
        response.errors.is_empty(),
        "GraphQL errors: {:?}",
        response.errors
    );
}

/// Assert that a GraphQL response has at least one error.
pub fn assert_has_errors(response: &Response) {
    assert!(
        !response.errors.is_empty(),
        "Expected GraphQL errors but got none"
    );
}

/// Extract the `data` field from a response as JSON.
pub fn response_data(response: &Response) -> Value {
    response.data.clone().into_json().unwrap()
}

/// Generate a minimal valid NZB XML for testing.
pub fn make_test_nzb(name: &str) -> String {
    // Use a unique message-id based on the name to avoid collisions.
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="test@test.com" date="1234567890" subject="{name} - &quot;file.rar&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="500000" number="1">{name}-seg1@test.com</segment></segments>
  </file>
</nzb>"#
    )
}

/// Generate a multi-file NZB for testing jobs with multiple files.
#[allow(dead_code)]
pub fn make_multi_file_nzb(name: &str, file_count: usize) -> String {
    let mut files = String::new();
    for i in 1..=file_count {
        files.push_str(&format!(
            r#"  <file poster="test@test.com" date="1234567890" subject="{name} - &quot;file.part{i:02}.rar&quot; yEnc (1/1)">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="500000" number="1">{name}-file{i}-seg1@test.com</segment></segments>
  </file>
"#
        ));
    }
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
{files}</nzb>"#
    )
}

// ---------------------------------------------------------------------------
// Mock scheduler
// ---------------------------------------------------------------------------

/// Spawn a mock scheduler that handles commands without performing real
/// downloads. Returns the handle and the background task.
fn spawn_test_scheduler() -> (SchedulerHandle, JoinHandle<()>) {
    let (cmd_tx, mut cmd_rx) = mpsc::channel(64);
    let (event_tx, _) = broadcast::channel(256);
    let metrics = PipelineMetrics::new();
    let shared_state = SharedPipelineState::new(metrics, vec![]);

    let handle = SchedulerHandle::new(cmd_tx, event_tx.clone(), shared_state.clone());

    let task = tokio::spawn(async move {
        let mut jobs: HashMap<JobId, JobState> = HashMap::new();

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                SchedulerCommand::AddJob {
                    job_id,
                    spec,
                    nzb_path: _,
                    reply,
                } => {
                    if jobs.contains_key(&job_id) {
                        let _ =
                            reply.send(Err(weaver_scheduler::SchedulerError::JobExists(job_id)));
                        shared_state.publish_jobs(build_job_list(&jobs));
                        continue;
                    }
                    let assembly = JobAssembly::new(job_id);
                    let par2_bytes = spec.par2_bytes();
                    let state = JobState {
                        job_id,
                        spec,
                        status: JobStatus::Queued,
                        assembly,
                        created_at: std::time::Instant::now(),
                        created_at_epoch_ms: epoch_ms_now(),
                        working_dir: PathBuf::from("/tmp/test"),
                        downloaded_bytes: 0,
                        failed_bytes: 0,
                        par2_bytes,
                        health_probing: false,
                        held_segments: Vec::new(),
                        download_queue: DownloadQueue::new(),
                        recovery_queue: DownloadQueue::new(),
                        staging_dir: None,
                    };
                    let _ = event_tx.send(PipelineEvent::JobCreated {
                        job_id,
                        name: state.spec.name.clone(),
                        total_files: state.spec.files.len() as u32,
                        total_bytes: state.spec.total_bytes,
                    });
                    jobs.insert(job_id, state);
                    let _ = reply.send(Ok(()));
                }
                SchedulerCommand::PauseJob { job_id, reply } => {
                    let result = match jobs.get_mut(&job_id) {
                        Some(state) => {
                            state.status = JobStatus::Paused;
                            let _ = event_tx.send(PipelineEvent::JobPaused { job_id });
                            Ok(())
                        }
                        None => Err(weaver_scheduler::SchedulerError::JobNotFound(job_id)),
                    };
                    let _ = reply.send(result);
                }
                SchedulerCommand::ResumeJob { job_id, reply } => {
                    let result = match jobs.get_mut(&job_id) {
                        Some(state) => {
                            state.status = JobStatus::Downloading;
                            let _ = event_tx.send(PipelineEvent::JobResumed { job_id });
                            Ok(())
                        }
                        None => Err(weaver_scheduler::SchedulerError::JobNotFound(job_id)),
                    };
                    let _ = reply.send(result);
                }
                SchedulerCommand::CancelJob { job_id, reply } => {
                    let result = if jobs.remove(&job_id).is_some() {
                        Ok(())
                    } else {
                        Err(weaver_scheduler::SchedulerError::JobNotFound(job_id))
                    };
                    let _ = reply.send(result);
                }
                SchedulerCommand::PauseAll { reply } => {
                    shared_state.set_paused(true);
                    let _ = reply.send(());
                }
                SchedulerCommand::ResumeAll { reply } => {
                    shared_state.set_paused(false);
                    let _ = reply.send(());
                }
                SchedulerCommand::SetSpeedLimit { reply, .. } => {
                    let _ = reply.send(());
                }
                SchedulerCommand::SetBandwidthCapPolicy { reply, .. } => {
                    let _ = reply.send(Ok(()));
                }
                SchedulerCommand::RebuildNntp { reply, .. } => {
                    let _ = reply.send(());
                }
                SchedulerCommand::UpdateRuntimePaths { reply, .. } => {
                    let _ = reply.send(Ok(()));
                }
                SchedulerCommand::ApplyScheduleAction { reply, .. } => {
                    let _ = reply.send(());
                }
                SchedulerCommand::ClearScheduleAction { reply } => {
                    let _ = reply.send(());
                }
                SchedulerCommand::RestoreJob {
                    job_id,
                    spec,
                    status,
                    working_dir,
                    reply,
                    ..
                } => {
                    let assembly = JobAssembly::new(job_id);
                    let par2_bytes = spec.par2_bytes();
                    let state = JobState {
                        job_id,
                        spec,
                        status,
                        assembly,
                        created_at: std::time::Instant::now(),
                        created_at_epoch_ms: epoch_ms_now(),
                        working_dir,
                        downloaded_bytes: 0,
                        failed_bytes: 0,
                        par2_bytes,
                        health_probing: false,
                        held_segments: Vec::new(),
                        download_queue: DownloadQueue::new(),
                        recovery_queue: DownloadQueue::new(),
                        staging_dir: None,
                    };
                    jobs.insert(job_id, state);
                    let _ = reply.send(Ok(()));
                }
                SchedulerCommand::DeleteHistory { job_id, reply, .. } => {
                    jobs.remove(&job_id);
                    let _ = reply.send(Ok(()));
                }
                SchedulerCommand::DeleteAllHistory { reply, .. } => {
                    jobs.retain(|_, state| {
                        !matches!(state.status, JobStatus::Complete | JobStatus::Failed { .. })
                    });
                    let _ = reply.send(Ok(()));
                }
                SchedulerCommand::ReprocessJob { job_id, reply } => {
                    let result = match jobs.get_mut(&job_id) {
                        Some(state) if matches!(state.status, JobStatus::Failed { .. }) => {
                            state.status = JobStatus::Downloading;
                            Ok(())
                        }
                        Some(_) => Err(weaver_scheduler::SchedulerError::Other(format!(
                            "job {} is not failed",
                            job_id.0
                        ))),
                        None => Err(weaver_scheduler::SchedulerError::JobNotFound(job_id)),
                    };
                    let _ = reply.send(result);
                }
                SchedulerCommand::UpdateJob {
                    job_id,
                    category,
                    metadata,
                    reply,
                } => {
                    let result = match jobs.get_mut(&job_id) {
                        Some(state) => {
                            if let Some(cat) = category {
                                state.spec.category = cat;
                            }
                            if let Some(meta) = metadata {
                                for (key, value) in meta {
                                    if let Some(existing) =
                                        state.spec.metadata.iter_mut().find(|(k, _)| k == &key)
                                    {
                                        existing.1 = value;
                                    } else {
                                        state.spec.metadata.push((key, value));
                                    }
                                }
                            }
                            Ok(())
                        }
                        None => Err(weaver_scheduler::SchedulerError::JobNotFound(job_id)),
                    };
                    let _ = reply.send(result);
                }
                SchedulerCommand::Shutdown => break,
            }
            // Publish updated job list to shared state after every command.
            shared_state.publish_jobs(build_job_list(&jobs));
        }
    });

    (handle, task)
}

fn build_job_list(jobs: &HashMap<JobId, JobState>) -> Vec<JobInfo> {
    jobs.values()
        .map(|state| JobInfo {
            job_id: state.job_id,
            name: state.spec.name.clone(),
            error: if let JobStatus::Failed { error } = &state.status {
                Some(error.clone())
            } else {
                None
            },
            status: state.status.clone(),
            progress: state.assembly.progress(),
            total_bytes: state.spec.total_bytes,
            downloaded_bytes: 0,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            password: state.spec.password.clone(),
            category: state.spec.category.clone(),
            metadata: state.spec.metadata.clone(),
            output_dir: None,
            created_at_epoch_ms: state.created_at_epoch_ms,
        })
        .collect()
}
