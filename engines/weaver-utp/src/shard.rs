// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Threads of their own for uTP. A shard is one thread running a
//! single-threaded tokio runtime: the tasks on it hand work to one another
//! without waking another thread, and their timers share its one timer
//! wheel. A socket whose options name shards runs its I/O task on the first,
//! so one thread reads and sends on the socket, as libtorrent does all of
//! its uTP work on its network thread.

use std::{
    io,
    sync::{Arc, OnceLock},
};

use parking_lot::Mutex;
use tokio::{runtime::Handle, sync::oneshot};

thread_local! {
    static ON_SHARD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Whether the calling thread is a uTP thread. Nothing that blocks, such as
/// a disk read or write, may run on one: every stream on it would wait.
pub fn on_shard_thread() -> bool {
    ON_SHARD.with(std::cell::Cell::get)
}

struct Shard {
    handle: Handle,
    // Dropped with the last handle on the shard: the thread then ends, and
    // drops its runtime and any task left on it.
    _stop: oneshot::Sender<()>,
}

struct Inner {
    name: String,
    count: usize,
    // Started at the first task placed, so a session with no uTP socket
    // starts no thread. None when the threads could not be started: the
    // tasks then run on their caller's runtime.
    started: OnceLock<Option<Box<[Shard]>>>,
    // Streams living on each shard, of every socket. Choosing a shard and
    // counting the stream there is one step under the lock.
    streams: Mutex<Box<[usize]>>,
}

/// One or more uTP threads, shared by every socket given a clone. The
/// threads start with the first task placed on them, and end once the last
/// clone is dropped and every socket's tasks have finished, as each socket
/// keeps a clone until then.
#[derive(Clone)]
pub struct Shards(Arc<Inner>);

impl std::fmt::Debug for Shards {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Shards").field(&self.0.count).finish()
    }
}

impl Shards {
    /// `count` threads, at least one and at most the machine's cores, named
    /// `{name}-{index}`. None start until a task is placed.
    pub fn new(name: &str, count: usize) -> Self {
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        let count = count.clamp(1, cores);
        Self(Arc::new(Inner {
            name: name.to_owned(),
            count,
            started: OnceLock::new(),
            streams: Mutex::new(vec![0; count].into_boxed_slice()),
        }))
    }

    /// The threads these shards run on once started: the count asked for,
    /// held to the machine's cores.
    pub fn threads(&self) -> usize {
        self.0.count
    }

    /// The name the threads take, before each one's `-{index}`.
    pub fn name(&self) -> &str {
        &self.0.name
    }

    fn shards(&self) -> Option<&[Shard]> {
        self.0
            .started
            .get_or_init(|| match start(&self.0.name, self.0.count) {
                Ok(shards) => Some(shards),
                Err(error) => {
                    tracing::warn!(
                        "uTP runs on its caller's runtime: no thread of its own: {error:#}"
                    );
                    None
                }
            })
            .as_deref()
    }

    /// Starts the threads if they have not started. Called before a caller
    /// takes a lock it places tasks under, so no thread is spawned while
    /// that lock is held.
    pub(crate) fn start(&self) {
        self.shards();
    }

    /// The shard for a socket's I/O task, and the one its streams run on
    /// when it has no other. None when the threads could not be started.
    pub(crate) fn io(&self) -> Option<Handle> {
        Some(self.shards()?[0].handle.clone())
    }

    /// Where a new stream runs: when `spread` and there is more than one
    /// shard, the one past the first holding the fewest streams, so the
    /// first is left to reading and sending; otherwise the first. The
    /// stream counts there until the returned place is dropped. None when
    /// the threads could not be started.
    pub(crate) fn place_stream(&self, spread: bool) -> Option<(Handle, Placed)> {
        let shards = self.shards()?;
        let mut streams = self.0.streams.lock();
        let index = if spread && shards.len() > 1 {
            (1..shards.len())
                .min_by_key(|&i| (streams[i], i))
                .unwrap_or(0)
        } else {
            0
        };
        streams[index] += 1;
        Some((
            shards[index].handle.clone(),
            Placed {
                shards: self.clone(),
                index,
            },
        ))
    }

    #[cfg(test)]
    fn streams(&self) -> Vec<usize> {
        self.0.streams.lock().to_vec()
    }
}

fn start(name: &str, count: usize) -> io::Result<Box<[Shard]>> {
    let mut shards = Vec::with_capacity(count);
    for index in 0..count {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let handle = runtime.handle().clone();
        let (stop, stopped) = oneshot::channel::<()>();
        std::thread::Builder::new()
            .name(format!("{name}-{index}"))
            // As the application's runtime threads: uTP's futures are large.
            .stack_size(8 * 1024 * 1024)
            .spawn(move || {
                ON_SHARD.with(|on| on.set(true));
                let _ = runtime.block_on(stopped);
                drop(runtime);
            })?;
        shards.push(Shard {
            handle,
            _stop: stop,
        });
    }
    Ok(shards.into())
}

/// A stream's place on a shard, held by its task.
pub(crate) struct Placed {
    shards: Shards,
    index: usize,
}

impl Drop for Placed {
    fn drop(&mut self) {
        self.shards.0.streams.lock()[self.index] -= 1;
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, net::SocketAddr, sync::atomic::Ordering};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::Shards;
    use crate::{
        DefaultUtpEnvironment, SocketOpts, UtpSocket,
        test_util::transport::{MockInterface, MockUtpTransport},
    };

    const CONNECTIONS: usize = 2;

    struct Transfer {
        // Threads data and ACK datagrams left from.
        threads: BTreeSet<Option<String>>,
        // Streams each shard held while the four streams lived.
        placed: Vec<usize>,
        // Size probes and packets given up on as too large, sent by their
        // stream, and the threads they left from.
        probes: usize,
        probe_threads: BTreeSet<Option<String>>,
    }

    // Two sockets on `count` shards over transports shaped by `shape`:
    // 300 kB one way on each of two connections.
    async fn transfer(
        count: usize,
        shape: impl Fn(MockUtpTransport) -> MockUtpTransport,
    ) -> Transfer {
        let shards = Shards::new("utp-shard-test", count);
        let interface = MockInterface::new();
        let addr = |last: u8| SocketAddr::from(([10, 0, 0, last], 5000));
        let socket = |last: u8| {
            UtpSocket::new_with_opts(
                shape(interface.transport(addr(last))),
                DefaultUtpEnvironment {},
                SocketOpts {
                    shards: Some(shards.clone()),
                    ..Default::default()
                },
            )
            .unwrap()
        };
        let (server, client) = (socket(1), socket(2));
        let mut pairs = Vec::new();
        for _ in 0..CONNECTIONS {
            let (accepted, connected) = tokio::join!(server.accept(), client.connect(addr(1)));
            pairs.push((accepted.unwrap(), connected.unwrap()));
        }
        let placed = shards.streams();
        let sent: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        for (accepted, connected) in pairs {
            let (mut reader, _) = accepted.split();
            let (_, mut writer) = connected.split();
            let write = async {
                writer.write_all(&sent).await.unwrap();
                writer.shutdown().await.unwrap();
            };
            let mut received = Vec::new();
            let ((), read) = tokio::join!(write, reader.read_to_end(&mut received));
            read.unwrap();
            assert!(received == sent, "the bytes differ");
        }
        Transfer {
            threads: interface.sending_threads.lock().clone(),
            placed,
            probes: interface.fragmenting_sends.load(Ordering::Relaxed),
            probe_threads: interface.fragmenting_threads.lock().clone(),
        }
    }

    // A socket registers its transport with the runtime its I/O task runs
    // on: a uTP thread's own when it has shards, the caller's otherwise.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_socket_registers_its_transport_where_its_io_runs() {
        use tokio::runtime::RuntimeFlavor;

        let interface = MockInterface::new();
        let shards = Shards::new("utp-register-test", 1);
        let sharded = UtpSocket::new_with_opts(
            interface.transport(SocketAddr::from(([10, 0, 0, 1], 5000))),
            DefaultUtpEnvironment {},
            SocketOpts {
                shards: Some(shards),
                ..Default::default()
            },
        )
        .unwrap();
        let unsharded = UtpSocket::new_with_opts(
            interface.transport(SocketAddr::from(([10, 0, 0, 2], 5000))),
            DefaultUtpEnvironment {},
            SocketOpts::default(),
        )
        .unwrap();
        assert_eq!(
            *interface.registered_on.lock(),
            [
                Some(RuntimeFlavor::CurrentThread),
                Some(RuntimeFlavor::MultiThread)
            ]
        );
        drop((sharded, unsharded));
    }

    // A transport that cannot be registered makes no socket.
    #[tokio::test]
    async fn a_refused_registration_makes_no_socket() {
        let interface = MockInterface::new();
        let made = UtpSocket::new_with_opts(
            interface
                .transport(SocketAddr::from(([10, 0, 0, 1], 5000)))
                .refusing_registration(),
            DefaultUtpEnvironment {},
            SocketOpts {
                shards: Some(Shards::new("utp-refused-test", 1)),
                ..Default::default()
            },
        );
        assert!(matches!(made, Err(crate::Error::Register(_))));
    }

    fn named(names: &[&str]) -> BTreeSet<Option<String>> {
        names.iter().map(|name| Some((*name).to_owned())).collect()
    }

    // One shard: the I/O tasks and the streams all run, and send, there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_sharded_socket_sends_only_from_its_shard() {
        let t = transfer(1, |t| t.sending_from_one_task().spreading_streams()).await;
        assert_eq!(t.threads, named(&["utp-shard-test-0"]));
        assert_eq!(t.placed, vec![4]);
    }

    // Three shards over a kernel-socket shape: the four streams go two to
    // each shard past the first, and every data and ACK datagram leaves
    // from the first, the I/O shard. A size probe, or a packet given up on
    // as too large, leaves from its stream's own shard so the host's size
    // refusal reaches the stream at once; there are a few per connection.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn streams_spread_over_the_shards_and_one_thread_sends() {
        let t = transfer(3, |t| t.sending_from_one_task().spreading_streams()).await;
        assert_eq!(t.threads, named(&["utp-shard-test-0"]));
        assert_eq!(t.placed, vec![0, 2, 2]);
        assert!(
            t.probe_threads
                .is_subset(&named(&["utp-shard-test-1", "utp-shard-test-2"])),
            "{:?}",
            t.probe_threads
        );
        assert!(
            t.probes <= PROBES_PER_CONNECTION * 2 * CONNECTIONS,
            "{}",
            t.probes
        );
    }

    // The most size probes one end of a connection sends while its size
    // search runs: 18 were measured for the four ends here.
    const PROBES_PER_CONNECTION: usize = 8;

    // A tunnel's shape: streams hand their datagrams to the I/O task but
    // do not spread, so the socket stays on the first shard.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_tunnel_keeps_its_streams_on_the_io_shard() {
        let t = transfer(3, |t| t.sending_from_one_task()).await;
        assert_eq!(t.threads, named(&["utp-shard-test-0"]));
        assert_eq!(t.placed, vec![4, 0, 0]);
    }

    // Streams that send for themselves stay on the first shard too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn streams_sending_for_themselves_stay_on_the_io_shard() {
        let t = transfer(3, |t| t).await;
        assert_eq!(t.threads, named(&["utp-shard-test-0"]));
        assert_eq!(t.placed, vec![4, 0, 0]);
    }

    // A task on a shard knows it is on a uTP thread; one elsewhere does
    // not.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_task_on_a_shard_is_on_a_utp_thread() {
        let shards = Shards::new("utp-shard-flag", 1);
        let io = shards.io().unwrap();
        assert!(io.spawn(async { super::on_shard_thread() }).await.unwrap());
        assert!(!super::on_shard_thread());
        assert!(
            !tokio::spawn(async { super::on_shard_thread() })
                .await
                .unwrap()
        );
    }

    // No thread starts before a task is placed.
    #[test]
    fn shards_start_no_thread_until_used() {
        let shards = Shards::new("utp-shard-unused", 2);
        assert!(shards.0.started.get().is_none());
        drop(shards);
    }

    // Placement counts the stream where it chose, under one lock: two
    // placements in a row take different shards, and a dropped place
    // frees its count.
    #[test]
    fn placement_is_counted_where_it_chose() {
        let shards = Shards::new("utp-shard-place", 3);
        let first = shards.place_stream(true).unwrap().1;
        let second = shards.place_stream(true).unwrap().1;
        assert_eq!(shards.streams(), vec![0, 1, 1]);
        drop(first);
        assert_eq!(shards.streams(), vec![0, 0, 1]);
        drop(second);
        assert_eq!(shards.streams(), vec![0, 0, 0]);
    }
}
