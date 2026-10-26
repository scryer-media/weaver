// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Test helper module — not all items are used by every test binary.
#![allow(dead_code)]

use std::sync::{
    Arc, Condvar, Mutex as StdMutex, MutexGuard as StdMutexGuard, OnceLock, RwLock as StdRwLock,
};

use async_graphql::{Request, Response, Variables};
use serde_json::Value;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use weaver_server_api::auth::{CallerIdentity, CallerScope, LoginAuthCache};
use weaver_server_api::{
    RssService, SchemaContext, SystemRuntimeContext, TestDbTaskHookGuard, WeaverSchema,
    build_schema, install_test_db_task_hook,
};
use weaver_server_core::SchedulerHandle;
use weaver_server_core::auth::ApiKeyCache;
use weaver_server_core::jobs::handle::SharedPipelineState;
use weaver_server_core::operations::metrics::PipelineMetrics;
use weaver_server_core::settings::{Config, SharedConfig};
use weaver_server_core::{Database, JobHistoryRow};

// Self-contained test harness that provides a fully-wired GraphQL schema
// backed by an in-memory database and the real idle pipeline.
//
// Each test should create its own harness for full isolation.
#[allow(dead_code)]
pub struct TestHarness {
    pub schema: WeaverSchema,
    pub handle: SchedulerHandle,
    pub scheduled_resume: weaver_server_api::ScheduledResumeCoordinator,
    pub config: SharedConfig,
    pub db: Database,
    pub metrics: Arc<PipelineMetrics>,
    pub shared_state: SharedPipelineState,
    pub server_transfer_policy:
        Arc<weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry>,
    pub auth_cache: LoginAuthCache,
    // The same config the schema holds, so a policy mutation's live effect on
    // the trusted-network list is observable from a test.
    pub security: weaver_server_core::security::RuntimeSecurityConfig,
    pub update_check: weaver_server_core::update_check::UpdateCheckService,
    _scheduler_task: JoinHandle<()>,
    _tempdir: tempfile::TempDir,
}

impl TestHarness {
    pub async fn wait_for_job_status(&self, id: u64, status: weaver_server_core::JobStatus) {
        let mut revisions = self.handle.subscribe_job_changes();
        loop {
            revisions.mark_unchanged();
            if self
                .handle
                .get_job(weaver_server_core::JobId(id))
                .is_ok_and(|job| job.status == status)
            {
                return;
            }
            revisions.changed().await.expect("pipeline revision stream");
        }
    }

    fn local_identity() -> CallerIdentity {
        CallerIdentity::Local([7; 32])
    }

    // Create a new test harness with in-memory DB, default config, and
    // a real idle pipeline and GraphQL schema. Spawns the background
    // history-delete worker, matching production.
    pub async fn new() -> Self {
        Self::new_with_options(true).await
    }

    // Like [`TestHarness::new`] but does not spawn the background
    // history-delete worker. Use this in tests that seed a delete operation and
    // assert on its initial `QUEUED` state, so the worker cannot claim the
    // operation (flipping it to `RUNNING`) before the assertion runs.
    pub async fn new_without_history_delete_worker() -> Self {
        Self::new_with_options(false).await
    }

    // Like [`TestHarness::new`] but with an explicit security posture, for
    // tests that pin what strict security, an environment pin, or a widened
    // bind address refuse. The config is shared with the schema, so a policy
    // mutation's live effect is observable on the caller's copy.
    pub async fn new_with_security(
        security: weaver_server_core::security::RuntimeSecurityConfig,
    ) -> Self {
        Self::new_with_options_and_security(true, security).await
    }

    async fn new_with_options(spawn_history_delete_worker: bool) -> Self {
        Self::new_with_options_and_security(
            spawn_history_delete_worker,
            // Loopback, no environment override: the shape a fresh install has.
            weaver_server_core::security::RuntimeSecurityConfig::default(),
        )
        .await
    }

    async fn new_with_options_and_security(
        spawn_history_delete_worker: bool,
        security: weaver_server_core::security::RuntimeSecurityConfig,
    ) -> Self {
        let tempdir = tempfile::TempDir::new().expect("failed to create tempdir");
        let mut db = Database::open_in_memory().expect("failed to open in-memory DB");
        db.set_encryption_key(
            weaver_server_core::persistence::encryption::EncryptionKey::generate(),
        );

        let config = Config {
            data_dir: tempdir.path().to_string_lossy().to_string(),
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            servers: vec![],
            categories: vec![],
            retry: None,
            max_download_speed: None,
            cleanup_after_extract: None,
            propagation_delay_secs: None,
            max_concurrent_nzbs: None,
            watch_folder: weaver_server_core::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: weaver_server_core::jobs::DuplicatePolicy::default(),
            direct_store: None,
            delivery_naming: None,
            hardware_profile: None,
            metrics: Default::default(),
            config_path: None,
        };
        let shared_config: SharedConfig = Arc::new(RwLock::new(config));

        let (mut pipeline, handle, shared_state) =
            weaver_server_core::pipeline::test_support::idle_pipeline(
                db.clone(),
                shared_config.clone(),
            )
            .await;
        let metrics = handle.pipeline_metrics();
        let scheduler_task = tokio::spawn(async move {
            pipeline.run().await;
        });
        let rss = RssService::new(handle.clone(), shared_config.clone(), db.clone());
        let watch_folder = weaver_server_core::watch_folder::WatchFolderService::new(
            db.clone(),
            handle.clone(),
            shared_config.clone(),
        );
        let auth_cache =
            LoginAuthCache::from_credentials(None, db.get_or_create_jwt_signing_secret().unwrap());
        let api_key_cache = ApiKeyCache::from_rows(vec![]);
        let shared_schedules: weaver_server_core::bandwidth::schedule::SharedSchedules =
            Arc::new(RwLock::new(vec![]));
        let server_transfer_policy = Arc::new(
            weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry::new(
                db.clone(),
                &[],
            )
            .expect("failed to create server transfer policy registry"),
        );
        handle.set_server_transfer_policy(Arc::clone(&server_transfer_policy));
        // Fetches leave only on a route; with nothing saved, each is direct.
        let proxies = weaver_server_core::proxies::ProxyRuntime::new(
            db.clone(),
            tokio::runtime::Handle::current(),
        )
        .unwrap();
        handle.set_proxy_runtime(proxies.clone());

        let scheduled_resume =
            weaver_server_api::ScheduledResumeCoordinator::new(db.clone(), handle.clone());
        let update_check =
            weaver_server_core::update_check::UpdateCheckService::new(db.clone(), proxies.clone());
        // The upgrade surface is part of the schema, so the harness carries one
        // too. Its profile directory is the test's own temporary tree: nothing
        // here may touch a real installation.
        let application_upgrade =
            weaver_server_core::application_upgrade::ApplicationUpgradeService::new(
                db.clone(),
                update_check.clone(),
                tempdir.path(),
                weaver_server_core::application_upgrade::collect_installation_assessment(),
                None,
                proxies,
            );
        let schema = build_schema(SchemaContext {
            handle: handle.clone(),
            scheduled_resume: scheduled_resume.clone(),
            config: shared_config.clone(),
            db: db.clone(),
            server_transfer_policy: Arc::clone(&server_transfer_policy),
            auth_cache: auth_cache.clone(),
            api_key_cache,
            security: security.clone(),
            rss,
            watch_folder,
            update_check: update_check.clone(),
            application_upgrade,
            schedules: shared_schedules,
            log_buffer:
                weaver_server_core::runtime::log_buffer::LogRingBuffer::with_default_capacity(),
            system_runtime: SystemRuntimeContext {
                profile: Arc::new(StdRwLock::new(
                    weaver_server_core::runtime::system_profile::SystemProfile {
                        cpu: weaver_server_core::runtime::system_profile::CpuProfile {
                            physical_cores: 4,
                            logical_cores: 8,
                            simd: weaver_server_core::runtime::system_profile::SimdSupport::default(
                            ),
                            cgroup_limit: None,
                        },
                        memory: weaver_server_core::runtime::system_profile::MemoryProfile {
                            total_bytes: 8 * 1024 * 1024 * 1024,
                            available_bytes: 4 * 1024 * 1024 * 1024,
                            cgroup_limit: None,
                        },
                        disk: weaver_server_core::runtime::system_profile::DiskProfile {
                            storage_class:
                                weaver_server_core::runtime::system_profile::StorageClass::Unknown,
                            filesystem:
                                weaver_server_core::runtime::system_profile::FilesystemType::Unknown(
                                    String::new(),
                                ),
                            sequential_write_mbps: 0.0,
                            random_read_iops: 0.0,
                            same_filesystem: true,
                        },
                    },
                )),
                started_at: std::time::Instant::now(),
            },
            nntp_pool: None,
            spawn_history_delete_worker,
            post_processing_executor: None,
        });

        Self {
            schema,
            handle,
            scheduled_resume,
            config: shared_config,
            db,
            metrics,
            shared_state,
            server_transfer_policy,
            auth_cache,
            security,
            update_check,
            _scheduler_task: scheduler_task,
            _tempdir: tempdir,
        }
    }

    // Execute a GraphQL query/mutation with full admin access.
    pub async fn execute(&self, query: &str) -> Response {
        let request = Request::new(query)
            .data(CallerScope::Local)
            .data(Self::local_identity());
        self.schema.execute(request).await
    }

    // Execute a GraphQL query/mutation as a specific caller scope.
    pub async fn execute_as(&self, query: &str, scope: CallerScope) -> Response {
        let request = Request::new(query).data(scope).data(Self::local_identity());
        self.schema.execute(request).await
    }

    // Execute a GraphQL query/mutation with variables.
    #[allow(dead_code)]
    pub async fn execute_with_variables(&self, query: &str, variables: Variables) -> Response {
        let request = Request::new(query)
            .data(CallerScope::Local)
            .data(Self::local_identity())
            .variables(variables);
        self.schema.execute(request).await
    }

    // Submit a minimal test NZB and return the job ID.
    pub async fn submit_test_nzb(&self, name: &str) -> u64 {
        self.submit_test_nzb_with_options(name, None, None, &[])
            .await
    }

    pub fn insert_history_row(&self, row: &JobHistoryRow) {
        self.db
            .insert_job_history(row)
            .expect("failed to insert history row");
    }

    // Submit a test NZB with options, returning the job ID.
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
            format!(", attributes: [{}]", entries.join(", "))
        };

        let query = format!(
            r#"mutation {{
                submitNzb(input: {{ nzbBase64: "{nzb_b64}", filename: "{name}.nzb"{cat_arg}{pwd_arg}{meta_arg} }}) {{
                    accepted
                    item {{
                        id
                        name
                        state
                    }}
                }}
            }}"#
        );

        let resp = self.execute(&query).await;
        assert_no_errors(&resp);
        let data = resp.data.into_json().unwrap();
        assert!(data["submitNzb"]["accepted"].as_bool().unwrap_or(false));
        data["submitNzb"]["item"]["id"]
            .as_u64()
            .expect("missing queue item id")
    }
}

// Assert that a GraphQL response has no errors.
pub fn assert_no_errors(response: &Response) {
    assert!(
        response.errors.is_empty(),
        "GraphQL errors: {:?}",
        response.errors
    );
}

// Assert that a GraphQL response has at least one error.
pub fn assert_has_errors(response: &Response) {
    assert!(
        !response.errors.is_empty(),
        "Expected GraphQL errors but got none"
    );
}

// Extract the `data` field from a response as JSON.
pub fn response_data(response: &Response) -> Value {
    response.data.clone().into_json().unwrap()
}

pub fn local_request(query: impl Into<String>) -> Request {
    Request::new(query.into())
        .data(CallerScope::Local)
        .data(CallerIdentity::Local([7; 32]))
}

fn blocking_db_operation_lock() -> &'static StdMutex<()> {
    static LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| StdMutex::new(()))
}

pub struct BlockingDbOperation {
    state: Arc<(StdMutex<BlockingDbOperationState>, Condvar)>,
    _guard: TestDbTaskHookGuard,
    _serialize: StdMutexGuard<'static, ()>,
}

#[derive(Default)]
struct BlockingDbOperationState {
    started: bool,
    released: bool,
}

impl BlockingDbOperation {
    pub fn new(operation: &'static str) -> Self {
        let serialize = blocking_db_operation_lock().lock().unwrap();
        let state = Arc::new((
            StdMutex::new(BlockingDbOperationState::default()),
            Condvar::new(),
        ));
        let hook_state = state.clone();
        let guard = install_test_db_task_hook(move |current_operation| {
            if current_operation != operation {
                return;
            }

            let (lock, condvar) = &*hook_state;
            let mut state = lock.lock().unwrap();
            if state.started {
                return;
            }
            state.started = true;
            condvar.notify_all();
            while !state.released {
                state = condvar.wait(state).unwrap();
            }
        });

        Self {
            state,
            _guard: guard,
            _serialize: serialize,
        }
    }

    pub async fn wait_until_started(&self) {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let (lock, condvar) = &*state;
            let mut state = lock.lock().unwrap();
            while !state.started {
                state = condvar.wait(state).unwrap();
            }
        })
        .await
        .unwrap();
    }

    pub fn release(&self) {
        let (lock, condvar) = &*self.state;
        let mut state = lock.lock().unwrap();
        state.released = true;
        condvar.notify_all();
    }
}

// Fails every database task named `operation` while it is alive: the task's
// blocking closure panics, so `spawn_blocking_db` returns an error for it.
pub struct FailingDbOperation {
    _guard: TestDbTaskHookGuard,
    _serialize: StdMutexGuard<'static, ()>,
}

impl FailingDbOperation {
    pub fn new(operation: &'static str) -> Self {
        let serialize = blocking_db_operation_lock().lock().unwrap();
        let guard = install_test_db_task_hook(move |current_operation| {
            if current_operation == operation {
                panic!("injected failure for db task {operation}");
            }
        });
        Self {
            _guard: guard,
            _serialize: serialize,
        }
    }
}

// Generate a minimal valid NZB XML for testing.
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

// Generate a multi-file NZB for testing jobs with multiple files.
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

impl Drop for TestHarness {
    fn drop(&mut self) {
        self._scheduler_task.abort();
    }
}
