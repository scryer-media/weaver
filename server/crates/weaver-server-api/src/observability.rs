use std::fmt::Display;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_graphql::{Error, ErrorExtensions};
use tracing::warn;
use weaver_server_core::settings::{Config, SharedConfig};
use weaver_server_core::{Refusal, StateError};

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
// reported as itself; anything else is a storage fault.
pub(crate) trait DbTaskError: Display {
    fn refusal(&self) -> Option<&Refusal> {
        None
    }
}

impl DbTaskError for StateError {
    fn refusal(&self) -> Option<&Refusal> {
        StateError::refusal(self)
    }
}

// A state error raised outside `spawn_blocking_db`, worded as it stands.
pub(crate) fn state_error(error: StateError) -> Error {
    match error {
        StateError::Refused(refusal) => refusal_error(refusal),
        error => Error::new(error.to_string()),
    }
}

pub(crate) async fn spawn_blocking_db<T, E, F>(operation: &'static str, task: F) -> Result<T, Error>
where
    T: Send + 'static,
    E: DbTaskError + Send + 'static,
    F: FnOnce() -> std::result::Result<T, E> + Send + 'static,
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

    result
        .map_err(|error| Error::new(error.to_string()))?
        .map_err(|error| match error.refusal() {
            Some(refusal) => refusal_error(refusal.clone()),
            None => Error::new(format!("db error: {error}")),
        })
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
            json(state_error(NetworkRefusal::PoolInUse.into())),
            serde_json::json!({
                "message": "pool is referenced by a server or RSS feed",
                "extensions": { "code": "POOL_IN_USE" },
            })
        );
        assert_eq!(
            json(state_error(StateError::Database("row decode".into()))),
            serde_json::json!({ "message": "database error: row decode" })
        );
    }
}
