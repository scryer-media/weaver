// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.

use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, trace};

use crate::Error;

/// Wrap a registered task with tracing instrumentation.
pub(crate) fn task(
    span: tracing::Span,
    fut: impl std::future::Future<Output = crate::Result<()>> + Send + 'static,
) -> impl std::future::Future<Output = ()> + Send + 'static {
    async move {
        trace!("started");
        tokio::pin!(fut);
        let mut trace_interval = tokio::time::interval(std::time::Duration::from_secs(5));

        loop {
            tokio::select! {
                _ = trace_interval.tick() => {
                    trace!("still running");
                },
                r = &mut fut => {
                    match r {
                        Ok(_) => {
                            trace!("finished");
                        }
                        Err(e) => {
                            if matches!(&e, Error::TaskCancelled) {
                                trace!("task cancelled")
                            } else {
                                debug!("finished with error: {:#}", e)
                            }

                        }
                    }
                    return;
                }
            }
        }
    }
    .instrument(span)
}

pub(crate) fn task_with_cancel(
    span: tracing::Span,
    cancellation_token: CancellationToken,
    fut: impl std::future::Future<Output = crate::Result<()>> + Send + 'static,
) -> impl std::future::Future<Output = ()> + Send + 'static {
    task(span, async move {
        tokio::select! {
            _ = cancellation_token.cancelled() => Err(Error::TaskCancelled),
            result = fut => result,
        }
    })
}
