// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::fmt::Display;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_graphql::{Error, ErrorExtensions};
use tracing::warn;
use weaver_server_core::settings::{Config, SharedConfig};
use weaver_server_core::{Refusal, StateError};

use crate::auth::graphql_error;

const SLOW_CONFIG_READ_WAIT_THRESHOLD: Duration = Duration::from_millis(50);
const SLOW_CONFIG_WRITE_WAIT_THRESHOLD: Duration = Duration::from_millis(50);
const SLOW_CONFIG_WRITE_HOLD_THRESHOLD: Duration = Duration::from_millis(50);
const SLOW_DB_BLOCKING_TASK_THRESHOLD: Duration = Duration::from_millis(100);

type TestDbTaskHook = Arc<dyn Fn(&'static str) + Send + Sync + 'static>;

static TEST_DB_TASK_HOOK: OnceLock<Mutex<Option<TestDbTaskHook>>> = OnceLock::new();

fn test_db_task_hook() -> &'static Mutex<Option<TestDbTaskHook>> {
    TEST_DB_TASK_HOOK.get_or_init(|| Mutex::new(None))
}

#[doc(hidden)]
pub struct TestDbTaskHookGuard;

#[doc(hidden)]
pub fn install_test_db_task_hook<F>(hook: F) -> TestDbTaskHookGuard
where
    F: Fn(&'static str) + Send + Sync + 'static,
{
    *test_db_task_hook().lock().unwrap() = Some(Arc::new(hook));
    TestDbTaskHookGuard
}

impl Drop for TestDbTaskHookGuard {
    fn drop(&mut self) {
        *test_db_task_hook().lock().unwrap() = None;
    }
}

pub(crate) async fn with_timed_config_read<T, F>(
    config: &SharedConfig,
    operation: &'static str,
    read: F,
) -> T
where
    F: FnOnce(&Config) -> T,
{
    let wait_started = Instant::now();
    let guard = config.read().await;
    let wait_elapsed = wait_started.elapsed();
    if wait_elapsed >= SLOW_CONFIG_READ_WAIT_THRESHOLD {
        warn!(
            operation,
            wait_ms = wait_elapsed.as_millis() as u64,
            "slow SharedConfig read lock wait"
        );
    }
    read(&guard)
}

pub(crate) async fn with_timed_config_write<T, F>(
    config: &SharedConfig,
    operation: &'static str,
    write: F,
) -> T
where
    F: FnOnce(&mut Config) -> T,
{
    let wait_started = Instant::now();
    let mut guard = config.write().await;
    let wait_elapsed = wait_started.elapsed();

    let hold_started = Instant::now();
    let result = write(&mut guard);
    let hold_elapsed = hold_started.elapsed();
    drop(guard);

    if wait_elapsed >= SLOW_CONFIG_WRITE_WAIT_THRESHOLD
        || hold_elapsed >= SLOW_CONFIG_WRITE_HOLD_THRESHOLD
    {
        warn!(
            operation,
            wait_ms = wait_elapsed.as_millis() as u64,
            hold_ms = hold_elapsed.as_millis() as u64,
            "slow SharedConfig write lock"
        );
    }

    result
}

// A refused input as a GraphQL error: the plain sentence as the message, the
// stable code under `extensions.code`, and under `extensions.params` any
// values the sentence names.
pub(crate) fn refusal_error(refusal: impl Into<Refusal>) -> Error {
    let refusal = refusal.into();
    Error::new(refusal.message).extend_with(|_, extensions| {
        extensions.set("code", refusal.code);
        if !refusal.params.is_empty()
            && let Ok(params) = async_graphql::Value::from_json(refusal.params.into())
        {
            extensions.set("params", params);
        }
    })
}

// What a blocking db task can fail with. A refusal of the caller's input is
// reported as itself; anything else is a fault, under `code`.
pub(crate) trait DbTaskError: Display {
    fn refusal(&self) -> Option<&Refusal> {
        None
    }

    fn code(&self) -> &'static str {
        "INTERNAL"
    }
}

impl DbTaskError for StateError {
    fn refusal(&self) -> Option<&Refusal> {
        StateError::refusal(self)
    }

    fn code(&self) -> &'static str {
        state_code(self)
    }
}

// The one code of a state failure that is not a refusal: a write that lost
// to another is a conflict, a disk with no room is full, and the rest are
// faults inside Weaver.
pub(crate) fn state_code(error: &StateError) -> &'static str {
    match error {
        StateError::Refused(refusal) => refusal.code,
        StateError::Conflict(_) => "CONFLICT",
        StateError::OutOfSpace(_) => "STORAGE_FULL",
        StateError::Database(_) | StateError::Io(_) => "INTERNAL",
    }
}

// A state error raised outside `spawn_blocking_db`, worded as it stands.
pub(crate) fn state_error(error: StateError) -> Error {
    match error {
        StateError::Refused(refusal) => refusal_error(refusal),
        error => graphql_error(state_code(&error), error.to_string()),
    }
}

// The watch folder failing to apply its settings or to scan, worded as it
// stands: its state failure under the state code, and the rest faults.
pub(crate) fn watch_folder_error(
    error: weaver_server_core::watch_folder::WatchFolderServiceError,
) -> Error {
    use weaver_server_core::watch_folder::WatchFolderServiceError;
    let code = match &error {
        WatchFolderServiceError::State(state) => state_code(state),
        WatchFolderServiceError::Watcher(_) | WatchFolderServiceError::Scanner(_) => "INTERNAL",
    };
    graphql_error(code, error.to_string())
}

// A pipeline refusal or failure worded as it stands, under the codes the job
// mutations give it.
pub(crate) fn scheduler_error(error: weaver_server_core::SchedulerError) -> Error {
    graphql_error(scheduler_code(&error), error.to_string())
}

// The code a pipeline refusal or failure answers with.
pub(crate) fn scheduler_code(error: &weaver_server_core::SchedulerError) -> &'static str {
    use weaver_server_core::SchedulerError;
    match error {
        SchedulerError::JobNotFound(_) => "NOT_FOUND",
        SchedulerError::JobExists(_) | SchedulerError::Conflict(_) => "CONFLICT",
        SchedulerError::SemanticSuperseded => "PARKED",
        SchedulerError::InvalidInput(_) => "INVALID_INPUT",
        SchedulerError::State(state) => state_code(state),
        SchedulerError::Internal(_)
        | SchedulerError::Other(_)
        | SchedulerError::ChannelClosed
        | SchedulerError::Assembly(_)
        | SchedulerError::Io(_) => "INTERNAL",
    }
}

// A blocking task that failed to run to its end: a fault, worded as it stands.
pub(crate) fn join_error(error: tokio::task::JoinError) -> Error {
    graphql_error("INTERNAL", error.to_string())
}

pub(crate) async fn spawn_blocking_db<T, E, F>(operation: &'static str, task: F) -> Result<T, Error>
where
    T: Send + 'static,
    E: DbTaskError + Send + 'static,
    F: FnOnce() -> std::result::Result<T, E> + Send + 'static,
{
    run_blocking(operation, task)
        .await?
        .map_err(|error| match error.refusal() {
            Some(refusal) => refusal_error(refusal.clone()),
            None => graphql_error(error.code(), format!("db error: {error}")),
        })
}

// `spawn_blocking_db` for a task whose error keeps its own wording: `error`
// turns it into the answer (`state_error` for a state failure), and a task
// that did not run to its end is a fault. Not timed as database work: some
// of these tasks hash a password.
pub(crate) async fn spawn_blocking_with<T, E, F, M>(task: F, error: M) -> Result<T, Error>
where
    T: Send + 'static,
    E: Send + 'static,
    F: FnOnce() -> std::result::Result<T, E> + Send + 'static,
    M: FnOnce(E) -> Error,
{
    tokio::task::spawn_blocking(task)
        .await
        .map_err(join_error)?
        .map_err(error)
}

// Runs a database task, timed and under the test hook.
async fn run_blocking<T, F>(operation: &'static str, task: F) -> Result<T, Error>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let started = Instant::now();
    let hook = test_db_task_hook().lock().unwrap().clone();
    let result = tokio::task::spawn_blocking(move || {
        if let Some(hook) = hook {
            hook(operation);
        }
        task()
    })
    .await;
    let elapsed = started.elapsed();
    if elapsed >= SLOW_DB_BLOCKING_TASK_THRESHOLD {
        warn!(
            operation,
            elapsed_ms = elapsed.as_millis() as u64,
            "slow spawn_blocking db task"
        );
    }
    result.map_err(join_error)
}

pub(crate) async fn persist_then_update_config<T, P, F>(
    config: &SharedConfig,
    operation: &'static str,
    persist: P,
    apply: F,
) -> Result<T, Error>
where
    P: Future<Output = Result<(), Error>>,
    F: FnOnce(&mut Config) -> T,
{
    persist.await?;
    Ok(with_timed_config_write(config, operation, apply).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use weaver_server_core::proxies::NetworkRefusal;

    // The error as a client receives it, apart from where in the query it is.
    fn json(error: Error) -> serde_json::Value {
        let mut error = serde_json::to_value(error.into_server_error(Default::default())).unwrap();
        error.as_object_mut().unwrap().remove("locations");
        error
    }

    #[tokio::test]
    async fn a_refusal_is_its_sentence_and_code_and_a_storage_fault_keeps_its_prefix() {
        let refused = spawn_blocking_db("test.refused", || {
            Err::<(), StateError>(NetworkRefusal::RouteProxyRepeated.into())
        })
        .await
        .unwrap_err();
        assert_eq!(
            json(refused),
            serde_json::json!({
                "message": "a proxy cannot appear twice in a ladder, including pools and chains",
                "extensions": { "code": "ROUTE_PROXY_REPEATED" },
            })
        );

        let fault = spawn_blocking_db("test.fault", || {
            Err::<(), StateError>(StateError::Database("unknown stored egress binding".into()))
        })
        .await
        .unwrap_err();
        assert_eq!(
            json(fault),
            serde_json::json!({
                "message": "db error: database error: unknown stored egress binding",
                "extensions": { "code": "INTERNAL" },
            })
        );
    }

    #[test]
    fn a_refusal_that_names_values_carries_them_as_params() {
        let error = refusal_error(NetworkRefusal::WireGuardInstanceLimit {
            required: 3,
            limit: 2,
        });
        assert_eq!(
            json(error),
            serde_json::json!({
                "message": "routes require 3 WireGuard instances × 516 MiB = 1548 MiB; this host permits 2 instances (available memory / 4 / 516 MiB, clamped to 1–8). Pool members count individually.",
                "extensions": {
                    "code": "WIREGUARD_INSTANCE_LIMIT",
                    "params": { "required": 3, "limit": 2, "instanceMib": 516 },
                },
            })
        );
        // A state error raised outside a db task is worded the same way.
        assert_eq!(
            json(state_error(NetworkRefusal::PoolGone.into())),
            serde_json::json!({
                "message": "proxy pool no longer exists",
                "extensions": { "code": "POOL_NOT_FOUND" },
            })
        );
        assert_eq!(
            json(state_error(StateError::Database("row decode".into()))),
            serde_json::json!({
                "message": "database error: row decode",
                "extensions": { "code": "INTERNAL" },
            })
        );
    }

    // Every state failure keeps its sentence and gains the code of its kind.
    #[test]
    fn each_state_failure_carries_the_code_of_its_kind() {
        let cases = [
            (
                StateError::Io(std::io::Error::other("disk gone")),
                "I/O error: disk gone",
                "INTERNAL",
            ),
            (
                StateError::Database("row decode".into()),
                "database error: row decode",
                "INTERNAL",
            ),
            (
                StateError::OutOfSpace("database or disk is full".into()),
                "database error: database or disk is full",
                "STORAGE_FULL",
            ),
            (
                StateError::Conflict("job 4 changed".into()),
                "state conflict: job 4 changed",
                "CONFLICT",
            ),
            (
                NetworkRefusal::PoolGone.into(),
                "proxy pool no longer exists",
                "POOL_NOT_FOUND",
            ),
        ];
        for (error, message, code) in cases {
            assert_eq!(error.to_string(), message);
            assert_eq!(
                json(state_error(error)),
                serde_json::json!({ "message": message, "extensions": { "code": code } })
            );
        }
    }

    // A pipeline answer keeps its sentence under the code of its kind.
    #[test]
    fn each_scheduler_failure_carries_the_code_of_its_kind() {
        use weaver_server_core::SchedulerError;
        use weaver_server_core::jobs::ids::JobId;
        let cases = [
            (
                SchedulerError::JobNotFound(JobId(7)),
                "job job-7 not found",
                "NOT_FOUND",
            ),
            (
                SchedulerError::JobExists(JobId(7)),
                "job job-7 already exists",
                "CONFLICT",
            ),
            (SchedulerError::Conflict("busy".into()), "busy", "CONFLICT"),
            (
                SchedulerError::SemanticSuperseded,
                "semantic candidate was superseded before materialization",
                "PARKED",
            ),
            (
                SchedulerError::InvalidInput("bad".into()),
                "bad",
                "INVALID_INPUT",
            ),
            (
                SchedulerError::State(StateError::OutOfSpace("full".into())),
                "state error: database error: full",
                "STORAGE_FULL",
            ),
            (SchedulerError::ChannelClosed, "channel closed", "INTERNAL"),
            (SchedulerError::Internal("boom".into()), "boom", "INTERNAL"),
            (SchedulerError::Other("other".into()), "other", "INTERNAL"),
        ];
        for (error, message, code) in cases {
            assert_eq!(
                json(scheduler_error(error)),
                serde_json::json!({ "message": message, "extensions": { "code": code } })
            );
        }
    }

    // A task whose own error is the answer keeps it whole; a state failure
    // gets no "db error" prefix.
    #[tokio::test]
    async fn a_task_with_its_own_error_keeps_its_wording() {
        let error = spawn_blocking_with(
            || Err::<(), StateError>(StateError::Conflict("taken".into())),
            state_error,
        )
        .await
        .unwrap_err();
        assert_eq!(
            json(error),
            serde_json::json!({
                "message": "state conflict: taken",
                "extensions": { "code": "CONFLICT" },
            })
        );
    }

    #[tokio::test]
    async fn a_task_that_did_not_finish_is_a_fault() {
        let error = spawn_blocking_with(
            || -> std::result::Result<(), Error> { panic!("boom") },
            |error| error,
        )
        .await
        .unwrap_err();
        let json = json(error);
        assert_eq!(json["extensions"]["code"], "INTERNAL");
        assert!(json["message"].as_str().unwrap().contains("panicked"));
    }
}
