// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{borrow::Cow, fmt::Display};

use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, trace};

/// Spawns a future with tracing instrumentation.
#[track_caller]
pub fn spawn<E: Display + Send + 'static>(
    span: tracing::Span,
    name: impl Into<Cow<'static, str>>,
    fut: impl std::future::Future<Output = Result<(), E>> + Send + 'static,
) -> tokio::task::JoinHandle<()> {
    let name = name.into();
    // No timer of its own: a "still running" trace every few seconds per
    // task was a wake per task whatever the task did, ~8 per idle torrent.
    let fut = async move {
        trace!("started");
        match fut.await {
            Ok(_) => {
                trace!("finished");
            }
            Err(e) => {
                error!("{name} finished with error: {:#}", e)
            }
        }
    }
    .instrument(span);
    tokio::task::spawn(fut)
}

#[track_caller]
pub fn spawn_with_cancel<E: Display + Send + 'static>(
    span: tracing::Span,
    name: impl Into<Cow<'static, str>>,
    cancellation_token: CancellationToken,
    fut: impl std::future::Future<Output = Result<(), E>> + Send + 'static,
) -> tokio::task::JoinHandle<()> {
    spawn(span, name, async move {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                debug!("task cancelled");
                Ok(())
            },
            r = fut => r
        }
    })
}
