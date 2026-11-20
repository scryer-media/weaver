// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A torrent paused holding pieces in memory unfinished writes them out and
// gives back their room; one removed writes nothing.

use std::sync::atomic::Ordering;

use weaver_torrent::api::TorrentIdOrHash;

use super::*;

const PIECE: u32 = 64 << 10;
const BLOCK: u64 = 16 << 10;
// Four whole pieces: every piece is four blocks.
const SIZES: [usize; 3] = [100_000, 150_000, 12_144];

const OTHER: HoldsNamespace = HoldsNamespace {
    scope: 3,
    node: NodeId::ROOT,
};

fn gated(layout: &Arc<PieceLayout>, watched: &Arc<Watched>, holds: Holds) -> GatedFactory {
    GatedFactory {
        factory: PieceStorageFactory::new(
            Arc::clone(layout),
            Arc::clone(watched) as Arc<dyn Pieces>,
            holds,
        ),
        gate: Arc::default(),
        refused: Arc::new(watch::channel(0).0),
        granted: Arc::default(),
        landing: Arc::default(),
    }
}

// The torrent's third block to be held by its connection before it lands,
// set before the torrent is added: what says the piece it is of once it is
// held, and the sender that lets it go.
fn hold_third_block(
    factory: &GatedFactory,
) -> (std::sync::mpsc::Receiver<u32>, std::sync::mpsc::Sender<()>) {
    let (held, holding) = std::sync::mpsc::channel();
    let (go, gate) = std::sync::mpsc::channel();
    factory.landing.lock().unwrap().hold = Some((3, held, gate));
    (holding, go)
}

// The third block held, its first two landed in memory: the piece they are
// of.
async fn held(holding: std::sync::mpsc::Receiver<u32>) -> u32 {
    tokio::task::spawn_blocking(move || holding.recv().unwrap())
        .await
        .unwrap()
}

// Whether the write touches the first `upto` bytes of `piece`.
fn touches(layout: &PieceLayout, piece: u32, upto: u64, write: (usize, u64, u64)) -> bool {
    let (file, offset, len) = write;
    layout.spans(piece).into_iter().any(|span| {
        let kept =
            span.range.start..span.range.start + upto.saturating_sub(span.at).min(span.range.len());
        span.file == file && offset < kept.end && kept.start < offset + len
    })
}

// A torrent holds a piece in memory part fetched, and the room for it,
// while another torrent's connection waits for that room. Paused, it writes
// the blocks it took to its files and gives the room back: the other
// connection is rung and its torrent is fetched whole. Resumed, it fetches
// only the rest of that piece, none of the blocks it took, and ends whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_paused_torrent_writes_out_its_unfinished_pieces_and_lets_their_room_go() {
    let seeders = [
        seeded_sized(11, PIECE, SIZES).await,
        seeded_sized(12, PIECE, SIZES).await,
    ];
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let budget = accountant(u64::from(PIECE));
    let (paused_store, mut paused_whole) =
        watched_store(&seeders[0].layout, &dirs[0].path().join("job"));
    let (waiting_store, mut waiting_whole) =
        watched_store(&seeders[1].layout, &dirs[1].path().join("job"));
    let paused_factory = gated(&seeders[0].layout, &paused_store, holds(&budget));
    assert!(!paused_factory.factory.writes_blocks());
    let waiting_factory = gated(
        &seeders[1].layout,
        &waiting_store,
        Holds {
            accountant: Arc::clone(&budget),
            namespace: OTHER,
        },
    );
    let mut refusals = waiting_factory.refused.subscribe();
    let leecher_dir = tempfile::tempdir().unwrap();
    let leecher = session(&leecher_dir.path().join("session"), None).await;

    let (holding, go) = hold_third_block(&paused_factory);
    let torrent = fetch_into(
        &leecher,
        &seeders[0],
        dirs[0].path(),
        paused_factory.clone().boxed(),
        false,
    )
    .await;
    let piece = held(holding).await;
    assert_eq!(
        paused_factory.landing.lock().unwrap().pieces[..2],
        [piece, piece]
    );
    // Whichever piece the picker started from holds its room.
    assert_eq!(
        budget.namespace_usage(NAMESPACE).resident,
        seeders[0].layout.piece_range(piece).len()
    );

    let _waiting = fetch_into(
        &leecher,
        &seeders[1],
        dirs[1].path(),
        waiting_factory.boxed(),
        false,
    )
    .await;
    refusals.wait_for(|refused| *refused > 0).await.unwrap();
    assert_eq!(waiting_store.store.count(), 0);

    leecher.pause(&torrent).await.unwrap();
    // The held block goes with the run that ended.
    go.send(()).unwrap();
    waiting_whole.wait_for(|whole| *whole).await.unwrap();
    for (name, original) in &seeders[1].originals {
        assert_eq!(
            &std::fs::read(dirs[1].path().join("job").join(name)).unwrap(),
            original
        );
    }
    // The other torrent had the whole budget: the paused one holds none.
    assert_eq!(budget.namespace_usage(NAMESPACE).resident, 0);
    assert!(paused_store.blocks.load(Ordering::Acquire) > 0);
    assert_eq!(paused_store.store.count(), 0);

    paused_factory.landing.lock().unwrap().recording = true;
    leecher.unpause(&torrent).await.unwrap();
    paused_whole.wait_for(|whole| *whole).await.unwrap();
    for (name, original) in &seeders[0].originals {
        assert_eq!(
            &std::fs::read(dirs[0].path().join("job").join(name)).unwrap(),
            original
        );
    }
    {
        let layout = &seeders[0].layout;
        let landing = paused_factory.landing.lock().unwrap();
        // The rest of the piece was written in the run after.
        assert!(landing.written.iter().any(|write| touches(
            layout,
            piece,
            layout.piece_range(piece).len(),
            *write
        )));
        // The two blocks it took before the pause were not.
        let taken: Vec<_> = landing
            .written
            .iter()
            .filter(|write| touches(layout, piece, 2 * BLOCK, **write))
            .collect();
        assert!(taken.is_empty(), "{taken:?}");
    }
    leecher.stop().await;
    for seeded in &seeders {
        seeded.seeder.stop().await;
    }
}

// A torrent removed with its files while it holds a piece in memory part
// fetched writes none of it: no block of it reaches the files, and nothing
// is left under where they were.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_removed_torrent_writes_out_none_of_its_unfinished_pieces() {
    let seeded = seeded_sized(13, PIECE, SIZES).await;
    let dir = tempfile::tempdir().unwrap();
    let budget = accountant(u64::from(PIECE));
    let (watched, _whole) = watched_store(&seeded.layout, &dir.path().join("job"));
    let factory = gated(&seeded.layout, &watched, holds(&budget));
    assert!(!factory.factory.writes_blocks());
    let leecher_dir = tempfile::tempdir().unwrap();
    let leecher = session(&leecher_dir.path().join("session"), None).await;
    let (holding, go) = hold_third_block(&factory);
    let torrent = fetch_into(
        &leecher,
        &seeded,
        dir.path(),
        factory.clone().boxed(),
        false,
    )
    .await;
    let piece = held(holding).await;
    assert_eq!(factory.landing.lock().unwrap().pieces[..2], [piece, piece]);
    assert_eq!(
        budget.namespace_usage(NAMESPACE).resident,
        seeded.layout.piece_range(piece).len()
    );

    leecher
        .delete(TorrentIdOrHash::Id(torrent.id()), true)
        .await
        .unwrap();
    go.send(()).unwrap();
    // Whatever was queued for the files is written by now.
    let store = Arc::clone(&watched.store);
    assert!(
        tokio::task::spawn_blocking(move || store.writes().drain())
            .await
            .unwrap()
    );
    assert_eq!(watched.blocks.load(Ordering::Acquire), 0);
    let job = dir.path().join("job");
    let left = if job.exists() {
        walkdir(&job)
    } else {
        Vec::new()
    };
    assert!(left.is_empty(), "{left:?}");
    leecher.stop().await;
    seeded.seeder.stop().await;
}
