// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The two ways shutdown waits for a step.
//
// The rule: a bound may abandon a wait, never a database write. A step that
// writes nothing durable (stopping a listener, a proxy, a watcher, hearing a
// worker out) is given `SHUTDOWN_STEP_BOUND` and then left behind, so it
// cannot hold the process up. A step that writes the database is waited for
// to its end on every way out of the process, because an exec or an exit
// while it runs cuts the write; when it is slow, one warning says which
// write shutdown is still waiting for.

use std::future::Future;
use std::time::Duration;

use tracing::warn;

// How long shutdown waits for a step that writes nothing durable, and how
// long a database write runs before shutdown says it is still waiting.
pub const SHUTDOWN_STEP_BOUND: Duration = Duration::from_secs(10);

// A step of shutdown that writes nothing durable. `None` when it did not
// finish inside the bound: shutdown goes on without it, and the warning
// names the step that was left behind.
pub async fn bounded_shutdown_step<T>(
    step: &'static str,
    future: impl Future<Output = T>,
) -> Option<T> {
    match tokio::time::timeout(SHUTDOWN_STEP_BOUND, future).await {
        Ok(output) => Some(output),
        Err(_) => {
            warn!(
                step,
                bound_secs = SHUTDOWN_STEP_BOUND.as_secs(),
                "shutdown step did not finish in time; shutting down without it"
            );
            None
        }
    }
}

// A database write at shutdown. Waited for to its end, however long that
// is; a write still running after the bound is warned about once, by name.
pub async fn waited_shutdown_write<T>(write: &'static str, future: impl Future<Output = T>) -> T {
    heard_to_its_end(future, || {
        warn!(
            write,
            waited_secs = SHUTDOWN_STEP_BOUND.as_secs(),
            "database write at shutdown is still running; waiting for it to finish"
        )
    })
    .await
}

// A step that writes nothing durable and still cannot be left behind,
// because what follows shutdown depends on it having ended. Waited for to
// its end and warned about once, like a write.
pub async fn waited_shutdown_step<T>(step: &'static str, future: impl Future<Output = T>) -> T {
    heard_to_its_end(future, || {
        warn!(
            step,
            waited_secs = SHUTDOWN_STEP_BOUND.as_secs(),
            "shutdown step is still running and cannot be left behind; waiting for it to finish"
        )
    })
    .await
}

async fn heard_to_its_end<T>(future: impl Future<Output = T>, say_it_is_slow: impl FnOnce()) -> T {
    tokio::pin!(future);
    match tokio::time::timeout(SHUTDOWN_STEP_BOUND, &mut future).await {
        Ok(output) => output,
        Err(_) => {
            say_it_is_slow();
            future.await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    // Every WARN event on this thread, as `message key=value...`.
    #[derive(Clone, Default)]
    struct Warnings(Arc<Mutex<Vec<String>>>);

    impl Warnings {
        fn heard(&self) -> Vec<String> {
            self.0.lock().unwrap().clone()
        }
    }

    impl tracing::Subscriber for Warnings {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            *metadata.level() == tracing::Level::WARN
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            use std::fmt::Write;
            struct Line(String);
            impl tracing::field::Visit for Line {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    let _ = write!(self.0, " {field}={value:?}");
                }
            }
            let mut line = Line(String::new());
            event.record(&mut line);
            self.0.lock().unwrap().push(line.0);
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    #[tokio::test(start_paused = true)]
    async fn a_step_that_finishes_in_time_is_not_warned_about() {
        let warnings = Warnings::default();
        let _capture = tracing::subscriber::set_default(warnings.clone());

        let step = bounded_shutdown_step("watch folder", async {
            tokio::time::sleep(SHUTDOWN_STEP_BOUND - Duration::from_secs(1)).await;
            7
        });
        assert_eq!(step.await, Some(7));
        let write = waited_shutdown_write("server download usage flush", async {
            tokio::time::sleep(SHUTDOWN_STEP_BOUND - Duration::from_secs(1)).await;
            8
        });
        assert_eq!(write.await, 8);
        assert!(warnings.heard().is_empty(), "{:?}", warnings.heard());
    }

    // A step that writes nothing durable and never ends is left behind at
    // the bound, with one warning that names it.
    #[tokio::test(start_paused = true)]
    async fn a_slow_step_that_writes_nothing_is_abandoned_with_its_warning() {
        let warnings = Warnings::default();
        let _capture = tracing::subscriber::set_default(warnings.clone());
        let started = tokio::time::Instant::now();

        let step = bounded_shutdown_step("HTTP server", std::future::pending::<()>());
        assert_eq!(step.await, None);

        assert_eq!(started.elapsed(), SHUTDOWN_STEP_BOUND);
        let heard = warnings.heard();
        assert_eq!(heard.len(), 1, "{heard:?}");
        assert!(heard[0].contains("step=\"HTTP server\""), "{}", heard[0]);
        assert!(
            heard[0].contains("shutting down without it"),
            "{}",
            heard[0]
        );
    }

    // A database write that outlasts the bound many times over is waited for
    // to its end and warned about once.
    #[tokio::test(start_paused = true)]
    async fn a_slow_database_write_is_waited_for_and_warned_about_once() {
        let warnings = Warnings::default();
        let _capture = tracing::subscriber::set_default(warnings.clone());
        let written = Arc::new(Mutex::new(false));
        let started = tokio::time::Instant::now();

        let write = waited_shutdown_write("ordered database writes", {
            let written = Arc::clone(&written);
            async move {
                tokio::time::sleep(SHUTDOWN_STEP_BOUND * 30).await;
                *written.lock().unwrap() = true;
                "done"
            }
        });
        assert_eq!(write.await, "done");

        assert!(*written.lock().unwrap(), "the write ran to its end");
        assert_eq!(started.elapsed(), SHUTDOWN_STEP_BOUND * 30);
        let heard = warnings.heard();
        assert_eq!(heard.len(), 1, "{heard:?}");
        assert!(
            heard[0].contains("write=\"ordered database writes\""),
            "{}",
            heard[0]
        );
        assert!(
            heard[0].contains("waiting for it to finish"),
            "{}",
            heard[0]
        );
    }
}
