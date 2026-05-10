use std::fmt::Display;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_graphql::Error;
use tracing::warn;
use weaver_server_core::settings::{Config, SharedConfig};

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

pub(crate) async fn spawn_blocking_db<T, E, F>(operation: &'static str, task: F) -> Result<T, Error>
where
    T: Send + 'static,
    E: Display + Send + 'static,
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
        .map_err(|error| Error::new(format!("db error: {error}")))
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
