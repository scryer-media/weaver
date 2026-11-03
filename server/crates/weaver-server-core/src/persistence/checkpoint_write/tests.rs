// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::cell::Cell;
use std::pin::Pin;
use std::task::Poll;

use tokio::sync::oneshot;

use super::*;

async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
}

fn busy() -> StateError {
    StateError::Database("database is locked".into())
}

#[tokio::test(start_paused = true)]
async fn transient_retries_back_off_until_commit_without_reporting_blocked() {
    let control = CheckpointControl::default();
    let (_shutdown, stopped) = watch::channel(false);
    let attempts = Cell::new(0);
    let delays_ms = [250, 500, 1000, 2000, 4000, 8000, 16000, 30000, 30000];
    let write = replay(
        &control,
        stopped,
        || {
            attempts.set(attempts.get() + 1);
            std::future::ready(if attempts.get() <= delays_ms.len() {
                Err(busy())
            } else {
                Ok(())
            })
        },
        |_, _| panic!("transient failures must not require explicit resume"),
    );
    tokio::pin!(write);
    assert!(poll_once(write.as_mut()).await.is_pending());
    for (index, delay) in delays_ms.into_iter().enumerate() {
        assert_eq!(attempts.get(), index + 1);
        tokio::time::advance(Duration::from_millis(delay - 1)).await;
        assert!(poll_once(write.as_mut()).await.is_pending());
        assert_eq!(attempts.get(), index + 1);
        tokio::time::advance(Duration::from_millis(1)).await;
        let result = poll_once(write.as_mut()).await;
        if index + 1 == delays_ms.len() {
            assert!(matches!(result, Poll::Ready(Ok(true))));
        } else {
            assert!(result.is_pending());
        }
    }
    assert_eq!(attempts.get(), delays_ms.len() + 1);
}

#[tokio::test(start_paused = true)]
async fn permanent_failure_waits_for_explicit_resume() {
    let control = CheckpointControl::default();
    let (_shutdown, stopped) = watch::channel(false);
    let attempts = Cell::new(0);
    let blocked = Cell::new(0);
    let write = replay(
        &control,
        stopped,
        || {
            attempts.set(attempts.get() + 1);
            std::future::ready(if attempts.get() == 1 {
                Err(StateError::Database("disk is full".into()))
            } else {
                Ok(())
            })
        },
        |_, _| blocked.set(blocked.get() + 1),
    );
    tokio::pin!(write);
    assert!(poll_once(write.as_mut()).await.is_pending());
    tokio::time::advance(Duration::from_secs(3600)).await;
    assert!(poll_once(write.as_mut()).await.is_pending());
    assert_eq!((attempts.get(), blocked.get()), (1, 1));
    control.resume();
    assert!(matches!(
        poll_once(write.as_mut()).await,
        Poll::Ready(Ok(true))
    ));
    assert_eq!((attempts.get(), blocked.get()), (2, 1));
}

#[tokio::test]
async fn resume_during_failed_attempt_is_not_lost() {
    let control = CheckpointControl::default();
    let (_shutdown, stopped) = watch::channel(false);
    let attempts = Cell::new(0);
    let result = replay(
        &control,
        stopped,
        || {
            attempts.set(attempts.get() + 1);
            std::future::ready(if attempts.get() == 1 {
                control.resume();
                Err(StateError::Database("disk is full".into()))
            } else {
                Ok(())
            })
        },
        |revision, _| assert_ne!(revision, control.revision()),
    )
    .await;
    assert!(matches!(result, Ok(true)));
    assert_eq!(attempts.get(), 2);
}

#[tokio::test]
async fn cancellation_retains_inflight_write_until_it_returns_without_acknowledging_it() {
    let control = CheckpointControl::default();
    let (_shutdown, stopped) = watch::channel(false);
    let (release, returned) = oneshot::channel();
    let mut returned = Some(returned);
    let write = replay(
        &control,
        stopped,
        || {
            let returned = returned.take().expect("cancelled write must not replay");
            async move { returned.await.unwrap() }
        },
        |_, _| panic!("write has not failed"),
    );
    tokio::pin!(write);
    assert!(poll_once(write.as_mut()).await.is_pending());
    control.cancel();
    assert!(poll_once(write.as_mut()).await.is_pending());
    release.send(Ok(())).unwrap();
    assert!(matches!(
        poll_once(write.as_mut()).await,
        Poll::Ready(Ok(false))
    ));
}

#[tokio::test(start_paused = true)]
async fn shutdown_stops_retries_and_preserves_the_unresolved_error() {
    let control = CheckpointControl::default();
    let (shutdown, stopped) = watch::channel(false);
    let attempts = Cell::new(0);
    let write = replay(
        &control,
        stopped,
        || {
            attempts.set(attempts.get() + 1);
            std::future::ready(Err(busy()))
        },
        |_, _| {},
    );
    tokio::pin!(write);
    assert!(poll_once(write.as_mut()).await.is_pending());
    shutdown.send_replace(true);
    let Poll::Ready(Err(error)) = poll_once(write.as_mut()).await else {
        panic!("shutdown must report the unwritten checkpoint");
    };
    assert!(error.to_string().contains("database is locked"));
    assert!(control.cancelled());
    assert_eq!(attempts.get(), 1);
}
