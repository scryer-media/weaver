// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Bounded waits for the package's reads of the database.
//
// The package is asked for while something is wrong, and a stalled database
// is one of those things: a read that waits on it would hold the whole
// package. So every such read waits until a deadline and no longer. One that
// does not answer by then is recorded as `timed_out`, one that answers with
// an error as `read_failed`, and the package is written without it. A
// blocking read that is given up on holds only its own handle to the
// database; it is left to finish on its own, and what it returns is dropped.

use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use tokio::time::Instant;

// How long a component waits for the database. A healthy database answers
// these reads in milliseconds; one that has not answered in this long is
// stalled, and the component says so instead of holding the package.
pub(super) const DATABASE_WAIT: Duration = Duration::from_secs(3);

// The read did not answer by its deadline.
pub(super) const REASON_TIMED_OUT: &str = "timed_out";
// The read answered with an error.
pub(super) const REASON_READ_FAILED: &str = "read_failed";

// Why a bounded read produced nothing: a reason code, and the sentence for
// people beside it.
#[derive(Debug, Clone)]
pub(super) struct ReadRefused {
    pub(super) reason: &'static str,
    pub(super) message: String,
}

impl ReadRefused {
    pub(super) fn timed_out() -> Self {
        Self {
            reason: REASON_TIMED_OUT,
            message: "the database did not answer within the package's wait".to_string(),
        }
    }

    pub(super) fn read_failed(message: impl Into<String>) -> Self {
        Self {
            reason: REASON_READ_FAILED,
            message: message.into(),
        }
    }
}

// Waits for `read` until `deadline`.
pub(super) async fn within<T>(
    deadline: Instant,
    read: impl Future<Output = Result<T, String>>,
) -> Result<T, ReadRefused> {
    match tokio::time::timeout_at(deadline, read).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(message)) => Err(ReadRefused::read_failed(message)),
        Err(_) => Err(ReadRefused::timed_out()),
    }
}

// A blocking database read, run off the async runtime. Dropping the returned
// future detaches the read rather than stopping it.
pub(super) async fn blocking<T: Send + 'static>(
    read: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    tokio::task::spawn_blocking(read)
        .await
        .map_err(|error| error.to_string())
        .and_then(|read| read)
}

// Runs `futures` concurrently and returns their outputs in order, so reads
// that each wait until one deadline add that wait once, not once each.
pub(super) async fn join_all<F: Future>(futures: Vec<F>) -> Vec<F::Output> {
    let mut futures: Vec<Pin<Box<F>>> = futures.into_iter().map(Box::pin).collect();
    let mut outputs: Vec<Option<F::Output>> = futures.iter().map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut pending = false;
        for (future, output) in futures.iter_mut().zip(outputs.iter_mut()) {
            if output.is_none() {
                match future.as_mut().poll(cx) {
                    Poll::Ready(value) => *output = Some(value),
                    Poll::Pending => pending = true,
                }
            }
        }
        if pending {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    })
    .await;
    outputs
        .into_iter()
        .map(|output| output.expect("every future finished"))
        .collect()
}
