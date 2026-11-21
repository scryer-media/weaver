// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{future::Future, io, sync::Arc};

use parking_lot::Mutex;
use tokio::{runtime::Handle, sync::watch, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{Error, traits::Transport};

#[derive(Clone)]
struct Failure {
    kind: io::ErrorKind,
    raw: Option<i32>,
    message: String,
}

impl Failure {
    fn new(error: io::Error) -> Self {
        Self {
            kind: error.kind(),
            raw: error.raw_os_error(),
            message: error.to_string(),
        }
    }

    fn error(&self) -> io::Error {
        match self.raw {
            Some(raw) => io::Error::from_raw_os_error(raw),
            None => io::Error::new(self.kind, self.message.clone()),
        }
    }
}

struct State<T> {
    transport: Option<Arc<T>>,
    tasks: JoinSet<()>,
    closing: bool,
    // Where the tasks run, when not on the runtime the socket was made on.
    // Kept until every task has finished.
    shards: Option<crate::Shards>,
}

pub(crate) struct Runtime<T> {
    state: Mutex<State<T>>,
    cancellation: CancellationToken,
    completed: watch::Sender<Option<Result<(), Failure>>>,
}

impl<T: Transport> Runtime<T> {
    pub(crate) fn new(
        transport: Arc<T>,
        parent: &CancellationToken,
        shards: Option<crate::Shards>,
    ) -> Arc<Self> {
        let (completed, _) = watch::channel(None);
        let runtime = Arc::new(Self {
            state: Mutex::new(State {
                transport: Some(transport),
                tasks: JoinSet::new(),
                closing: false,
                shards,
            }),
            cancellation: parent.child_token(),
            completed,
        });
        let weak = Arc::downgrade(&runtime);
        let cancellation = runtime.cancellation.clone();
        if runtime
            .register(move |_| async move {
                cancellation.cancelled().await;
                if let Some(runtime) = weak.upgrade() {
                    runtime.request_shutdown();
                }
            })
            .is_err()
        {
            runtime.request_shutdown();
        }
        runtime
    }

    pub(crate) fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    pub(crate) fn link_mtu(&self) -> Option<Option<u16>> {
        let state = self.state.lock();
        if state.closing || self.cancellation.is_cancelled() {
            return None;
        }
        state
            .transport
            .as_ref()
            .map(|transport| transport.link_mtu())
    }

    #[cfg(test)]
    pub(crate) fn transport(&self) -> Option<Arc<T>> {
        let state = self.state.lock();
        if state.closing || self.cancellation.is_cancelled() {
            None
        } else {
            state.transport.clone()
        }
    }

    pub(crate) fn register<F: Future<Output = ()> + Send + 'static>(
        self: &Arc<Self>,
        build: impl FnOnce(Arc<T>) -> F,
    ) -> crate::Result<()> {
        self.spawn(build, false).map(drop)
    }

    // A stream's task: on the least loaded shard when there are several.
    // Returns the shard it runs on, when it runs on one.
    pub(crate) fn register_stream<F: Future<Output = ()> + Send + 'static>(
        self: &Arc<Self>,
        build: impl FnOnce(Arc<T>) -> F,
    ) -> crate::Result<Option<Handle>> {
        self.spawn(build, true)
    }

    fn spawn<F: Future<Output = ()> + Send + 'static>(
        self: &Arc<Self>,
        build: impl FnOnce(Arc<T>) -> F,
        stream: bool,
    ) -> crate::Result<Option<Handle>> {
        // The threads start, the first time, before the state lock is taken.
        let shards = self.state.lock().shards.clone();
        if let Some(shards) = shards {
            shards.start();
        }
        let mut state = self.state.lock();
        while let Some(result) = state.tasks.try_join_next() {
            if let Err(error) = result {
                drop(state);
                self.request_shutdown_with_error(io::Error::other(error.to_string()));
                return Err(Error::DispatcherDead);
            }
        }
        if state.closing || self.cancellation.is_cancelled() {
            return Err(Error::DispatcherDead);
        }
        let transport = state.transport.clone().ok_or(Error::DispatcherDead)?;
        let spread = transport.sends_from_one_task() && transport.spreads_streams();
        // Boxed once here: the I/O task's future is large, and each arm
        // below would otherwise hold its own copy on the stack.
        let task: std::pin::Pin<Box<dyn Future<Output = ()> + Send>> = Box::pin(build(transport));
        let state = &mut *state;
        let home = match &state.shards {
            Some(shards) if stream => match shards.place_stream(spread) {
                Some((handle, placed)) => {
                    state.tasks.spawn_on(
                        async move {
                            let _placed = placed;
                            task.await;
                        },
                        &handle,
                    );
                    Some(handle)
                }
                None => {
                    state.tasks.spawn(task);
                    None
                }
            },
            Some(shards) => match shards.io() {
                Some(io) => {
                    state.tasks.spawn_on(task, &io);
                    Some(io)
                }
                None => {
                    state.tasks.spawn(task);
                    None
                }
            },
            None => {
                state.tasks.spawn(task);
                None
            }
        };
        Ok(home)
    }

    pub(crate) fn request_shutdown(&self) {
        self.close(None);
    }

    pub(crate) fn request_shutdown_with_error(&self, error: io::Error) {
        self.close(Some(Failure::new(error)));
    }

    fn close(&self, mut failure: Option<Failure>) {
        let (mut tasks, transport, shards) = {
            let mut state = self.state.lock();
            if state.closing {
                return;
            }
            state.closing = true;
            self.cancellation.cancel();
            (
                std::mem::take(&mut state.tasks),
                state.transport.take(),
                state.shards.take(),
            )
        };
        let completed = self.completed.clone();
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tasks.abort_all();
            drop(tasks);
            drop(transport);
            drop(shards);
            completed.send_replace(Some(Err(Failure::new(io::Error::other(
                "runtime unavailable during shutdown",
            )))));
            return;
        };
        // This owner survives a cancelled shutdown caller or the last socket
        // handle. It is outside the set it joins and holds no registration lock.
        handle.spawn(async move {
            while let Some(result) = tasks.join_next().await {
                if let Err(error) = result {
                    failure
                        .get_or_insert_with(|| Failure::new(io::Error::other(error.to_string())));
                }
            }
            if let Some(transport) = transport {
                let shutdown = tokio::spawn(async move {
                    let result = transport.shutdown().await.map_err(Failure::new);
                    drop(transport);
                    result
                })
                .await;
                match shutdown {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        failure.get_or_insert(error);
                    }
                    Err(error) => {
                        failure.get_or_insert_with(|| {
                            Failure::new(io::Error::other(error.to_string()))
                        });
                    }
                }
            }
            completed.send_replace(Some(failure.map_or(Ok(()), Err)));
            // The shards outlive every task that ran on them.
            drop(shards);
        });
    }

    pub(crate) async fn shutdown(&self) -> io::Result<()> {
        let mut completed = self.completed.subscribe();
        self.request_shutdown();
        loop {
            if let Some(result) = completed.borrow().clone() {
                return result.map_err(|failure| failure.error());
            }
            completed
                .changed()
                .await
                .map_err(|_| io::Error::other("shutdown owner disappeared"))?;
        }
    }
}
