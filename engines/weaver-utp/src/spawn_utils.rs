// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-13.

use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, trace};

use crate::Error;

/// Wrap a registered task with tracing instrumentation. The task wakes only
/// for its own work: an idle stream or socket costs no wake.
pub(crate) fn task(
    span: tracing::Span,
    fut: impl std::future::Future<Output = crate::Result<()>> + Send + 'static,
) -> impl std::future::Future<Output = ()> + Send + 'static {
    async move {
        trace!("started");
        match fut.await {
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
