// SPDX-License-Identifier: GPL-3.0-or-later

//! The fetch of a v2 torrent's piece layers from peers that speak v2,
//! and the answers a seed gives them (libtorrent `hash_picker`,
//! `bt_peer_connection::on_hashes`, `on_hash_reject`,
//! `on_hash_request`).

use super::*;
use crate::{
    core::{
        Id32,
        merkle::{self, PieceTree},
        v2_test_torrent::{TestTorrent as V2TestTorrent, file_hashes, payload},
    },
    peer_protocol::{HashRequest, Hashes},
    torrent_state::live::hash_fetch::Pick,
};

const PIECE: u64 = 16384;
/// Two runs of the layer: 512 pieces, then 88 asked for as 128.
const PIECES: u32 = 600;

/// A v2 torrent without its piece layers: a file of [`PIECES`] pieces
/// and one of a single piece.
fn layerless() -> V2TestTorrent {
    let mut torrent = V2TestTorrent::new(
        vec![
            (
                "a",
                payload(PIECES as usize * usize::try_from(PIECE).unwrap(), 1),
            ),
            ("b", payload(100, 2)),
        ],
        PIECE,
    );
    torrent.layers = false;
    torrent
}

impl Harness {
    /// `torrent` live, none of its pieces had, and no peers.
    async fn v2(name: &str, torrent: &V2TestTorrent) -> Self {
        let out = TempDir::with_prefix(name).unwrap();
        let session = Session::new_with_opts(out.path().into(), isolated_session_opts(false))
            .await
            .unwrap();
        let pieces: usize = torrent
            .files
            .iter()
            .map(|(_, data)| {
                data.len()
                    .div_ceil(usize::try_from(torrent.piece_length).unwrap())
            })
            .sum();
        let have = BF::from_boxed_slice(vec![0u8; pieces.div_ceil(8)].into_boxed_slice());
        let handle = session
            .add_torrent(
                AddTorrent::from_bytes(torrent.torrent_bytes()),
                Some(AddTorrentOptions {
                    storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                    checkpoint: Some(crate::Checkpoint::pieces(have)),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        handle.wait_until_initialized().await.unwrap();
        Self {
            live: handle.live().unwrap(),
            content: torrent.v1_payload(),
            _session: session,
            _torrent: None,
            _out: out,
        }
    }

    fn expected(&self, piece: u32) -> Option<Id32> {
        self.metadata.v2.as_ref().unwrap().expected(piece)
    }
}

/// The tree over the piece layer of file "a".
fn tree(torrent: &V2TestTorrent) -> PieceTree {
    let layer = file_hashes(&torrent.files[0].1, PIECE).1.unwrap();
    PieceTree::from_piece_layer(layer, 1)
}

/// The answer a peer holding the layers gives to `request`.
fn answer(torrent: &V2TestTorrent, request: HashRequest) -> Hashes<ByteBufOwned> {
    let hashes = tree(torrent)
        .hashes(
            request.base,
            request.index.into(),
            request.count.into(),
            request.proof_layers,
        )
        .unwrap();
    Hashes::new(request, &hashes)
}

async fn next_hash_request(task: &mut Task<'_>, rx: &mut PeerRx) -> HashRequest {
    match next_message(task, rx).await {
        Message::HashRequest(r) => r,
        other => panic!("expected a hash request, got {other:?}"),
    }
}

fn shape(r: HashRequest) -> (u32, u32, u32, u32) {
    (r.base, r.index, r.count, r.proof_layers)
}

#[tokio::test]
async fn a_v2_piece_waits_for_its_layer_run_which_a_v2_peer_proves() {
    let torrent = layerless();
    let h = Harness::v2("a_v2_piece_waits_for_its_layer", &torrent).await;
    let (peer, mut rx) = h.peer_speaking("127.0.0.1:6881", true);
    peer.on_i_am_unchoked();

    // Only the one-piece file, which its root proves, may be fetched.
    assert_eq!(
        peer.acquire_next_piece().unwrap().piece().map(|p| p.get()),
        Some(PIECES)
    );
    assert_eq!(peer.acquire_next_piece().unwrap().piece(), None);

    // The peer is asked for both runs of the layer, from the piece
    // layer, each proven to the root (libtorrent
    // `hash_picker::pick_hashes`).
    let mut fetch: Task<'_> = Box::pin(peer.fetch_layers());
    let first = next_hash_request(&mut fetch, &mut rx).await;
    let second = next_hash_request(&mut fetch, &mut rx).await;
    assert_eq!(first.root, tree(&torrent).root());
    assert_eq!(shape(first), (0, 0, 512, 9));
    assert_eq!(shape(second), (0, 512, 128, 9));

    // The second run proven: its pieces may be fetched, the first's not.
    peer.on_hashes(answer(&torrent, second).as_borrowed())
        .unwrap();
    let piece = peer.acquire_next_piece().unwrap().piece().unwrap().get();
    assert!((512..PIECES).contains(&piece), "{piece}");
    assert_eq!(h.expected(0), None);

    // Both proven: the torrent file carries the piece layers, as one
    // that came with them (libtorrent `write_torrent_file`).
    peer.on_hashes(answer(&torrent, first).as_borrowed())
        .unwrap();
    let tree = tree(&torrent);
    assert!((0..PIECES).all(|p| h.expected(p) == Some(tree.pieces()[p as usize])));
    let mut whole = layerless();
    whole.layers = true;
    assert_eq!(h.metadata.torrent_bytes(), whole.torrent_bytes());
    assert!(matches!(
        h.layer_fetch.pick(
            peer.addr,
            |_| true,
            Duration::ZERO,
            tokio::time::Instant::now()
        ),
        Pick::Wait(None)
    ));
}

#[tokio::test]
async fn hashes_that_prove_nothing_drop_the_peer_and_the_run_goes_to_another() {
    let torrent = layerless();
    let h = Harness::v2("hashes_that_prove_nothing", &torrent).await;
    let (bad, mut bad_rx) = h.peer_speaking("127.0.0.1:6881", true);
    let mut bad_fetch: Task<'_> = Box::pin(bad.fetch_layers());
    let first = next_hash_request(&mut bad_fetch, &mut bad_rx).await;
    let _second = next_hash_request(&mut bad_fetch, &mut bad_rx).await;

    // test_hash_picker.cpp `add_bad_hashes`: the last hash flipped.
    let mut hashes = answer(&torrent, first).hashes();
    hashes.last_mut().unwrap().0[0] ^= 1;
    assert!(
        bad.on_hashes(Hashes::new(first, &hashes).as_borrowed())
            .is_err()
    );
    assert_eq!(h.expected(0), None);

    // The run is free for the next peer; the other stays with the first.
    let (good, mut good_rx) = h.peer_speaking("127.0.0.2:6881", true);
    let mut good_fetch: Task<'_> = Box::pin(good.fetch_layers());
    assert_eq!(
        next_hash_request(&mut good_fetch, &mut good_rx).await,
        first
    );
    good.on_hashes(answer(&torrent, first).as_borrowed())
        .unwrap();
    assert_eq!(h.expected(0), Some(tree(&torrent).pieces()[0]));
}

#[tokio::test]
async fn a_rejected_run_goes_to_another_peer_and_an_unasked_answer_is_ignored() {
    let torrent = layerless();
    let h = Harness::v2("a_rejected_run", &torrent).await;
    let (first_peer, mut first_rx) = h.peer_speaking("127.0.0.1:6881", true);
    let mut first_fetch: Task<'_> = Box::pin(first_peer.fetch_layers());
    let first = next_hash_request(&mut first_fetch, &mut first_rx).await;
    let second = next_hash_request(&mut first_fetch, &mut first_rx).await;

    // An answer from a peer that was not asked is ignored (libtorrent
    // `bt_peer_connection::on_hashes`).
    let (other, mut other_rx) = h.peer_speaking("127.0.0.2:6881", true);
    other
        .on_hashes(answer(&torrent, first).as_borrowed())
        .unwrap();
    assert_eq!(h.expected(0), None);

    // A reject frees the run for the other peer at once (libtorrent
    // `hash_picker::hashes_rejected`), and the peer that rejected it is
    // not asked again.
    first_peer.on_hash_reject(first);
    let mut other_fetch: Task<'_> = Box::pin(other.fetch_layers());
    assert_eq!(
        next_hash_request(&mut other_fetch, &mut other_rx).await,
        first
    );
    assert!(matches!(
        h.layer_fetch.pick(
            first_peer.addr,
            |_| true,
            Duration::from_secs(60),
            tokio::time::Instant::now()
        ),
        Pick::Wait(_)
    ));
    other
        .on_hashes(answer(&torrent, first).as_borrowed())
        .unwrap();
    first_peer
        .on_hashes(answer(&torrent, second).as_borrowed())
        .unwrap();
    assert!((0..PIECES).all(|p| h.expected(p).is_some()));
}

#[tokio::test]
async fn an_unanswered_run_goes_to_another_peer_at_the_request_timeout() {
    let torrent = layerless();
    let h = Harness::v2("an_unanswered_run", &torrent).await;
    tokio::time::pause();
    let start = tokio::time::Instant::now();
    let (slow, mut slow_rx) = h.peer_speaking("127.0.0.1:6881", true);
    let mut slow_fetch: Task<'_> = Box::pin(slow.fetch_layers());
    let first = next_hash_request(&mut slow_fetch, &mut slow_rx).await;
    let second = next_hash_request(&mut slow_fetch, &mut slow_rx).await;

    // The slow peer has delivered nothing, so its timeout is the
    // longest; the other peer is asked for both runs once it passes.
    let (other, mut other_rx) = h.peer_speaking("127.0.0.2:6881", true);
    let mut other_fetch: Task<'_> = Box::pin(other.fetch_layers());
    assert_eq!(
        next_hash_request(&mut other_fetch, &mut other_rx).await,
        first
    );
    assert_eq!(
        next_hash_request(&mut other_fetch, &mut other_rx).await,
        second
    );
    let timeout = h
        .peers
        .with_live(slow.addr, |live| live.request_timeout())
        .unwrap();
    assert_eq!(timeout, Duration::from_secs(60));
    assert!(tokio::time::Instant::now() >= start + timeout);

    // A late answer from the slow peer still counts.
    slow.on_hashes(answer(&torrent, first).as_borrowed())
        .unwrap();
    assert!(h.expected(0).is_some());
}

#[tokio::test]
async fn a_seed_answers_hash_requests_from_its_layers_and_rejects_the_rest() {
    let mut torrent = layerless();
    torrent.layers = true;
    let h = Harness::v2("a_seed_answers_hash_requests", &torrent).await;
    let (peer, mut rx) = h.peer_speaking("127.0.0.1:6881", true);
    let tree = tree(&torrent);
    let ask = |root, base, index, count, proof_layers| HashRequest {
        root,
        base,
        index,
        count,
        proof_layers,
    };

    // The second run as libtorrent's `hash_picker::pick_hashes` asks
    // for it: 88 pieces padded to 512 nodes, and the uncle that proves
    // the root (libtorrent
    // `bt_peer_connection::on_hash_request`).
    let request = ask(tree.root(), 0, 512, 512, 9);
    peer.on_hash_request(request).unwrap();
    let Ok(WriterRequest::Answer(crate::peer_connection::Answer::Hashes(answer))) = rx.try_recv()
    else {
        panic!("expected hashes");
    };
    assert_eq!(answer.request, request);
    let hashes = answer.hashes();
    let (nodes, uncles) = hashes.split_at(512);
    let pad = merkle::pad_hash(1, 1);
    let real = (PIECES - 512) as usize;
    assert_eq!(&nodes[..real], &tree.pieces()[512..]);
    assert!(nodes[real..].iter().all(|n| *n == pad));
    assert_eq!(uncles.len(), 1);
    assert_eq!(merkle::proven_root(nodes, 512, pad, uncles), tree.root());

    // A malformed request, one for a root of no file, and one for the
    // one-piece file, which has no layer: each is rejected.
    let one_piece = file_hashes(&torrent.files[1].1, PIECE).0.unwrap();
    for bad in [
        ask(tree.root(), 0, 0, 0, 0),
        ask(tree.root(), 0, 0, 1, 10),
        ask(tree.pieces()[0], 0, 0, 1, 0),
        ask(one_piece, 0, 0, 1, 0),
    ] {
        peer.on_hash_request(bad).unwrap();
        assert!(
            matches!(rx.try_recv(), Ok(WriterRequest::Answer(crate::peer_connection::Answer::HashReject(r))) if r == bad),
            "{bad:?}"
        );
    }
}

/// A v2 torrent, a hybrid when `hybrid` holds, of one file of two
/// four-block pieces, with its layers.
fn two_pieces(hybrid: bool) -> V2TestTorrent {
    let mut torrent = V2TestTorrent::new(
        vec![("a", payload(2 * PIECE_LENGTH as usize, 3))],
        u64::from(PIECE_LENGTH),
    );
    torrent.hybrid = hybrid;
    torrent
}

/// The leaves under piece `index` of `torrent`'s file, as the
/// answer to the request for them carries them.
fn leaves(torrent: &V2TestTorrent, index: u32) -> Vec<Id32> {
    let start = (index * PIECE_LENGTH) as usize;
    torrent.files[0].1[start..start + PIECE_LENGTH as usize]
        .chunks(CHUNK_SIZE as usize)
        .map(merkle::hash_block)
        .collect()
}

// libtorrent torrent::piece_failed asks for the block hashes of a v2
// piece whose bad blocks are not known (verify_block_hashes); once they
// come, hash_picker::add_hashes names the blocks that differ, and
// torrent::piece_failed penalises their senders alone, as known bad
// (penalize_peers). Weaver blames no one before.
async fn the_leaves_of_a_failed_v2_piece_blame_only_its_bad_blocks_sender(hybrid: bool) {
    let torrent = two_pieces(hybrid);
    let h = Harness::v2("leaves_blame_bad_blocks", &torrent).await;
    tokio::time::pause();
    let (bad, mut bad_rx) = h.peer("127.0.0.1:6881");
    let (good, mut good_rx) = h.peer("127.0.0.2:6881");

    // `bad` sends the first half, its second block corrupt, `good` the
    // second: the piece fails, is downloaded again, and no one is
    // blamed yet.
    h.fail_shared(&bad, &good).await;
    for peer in [&bad, &good] {
        assert_eq!(h.trust(peer), (0, 0, false));
        assert!(!h.banned(peer));
    }
    assert_eq!(
        h.get_failed_bytes(),
        u64::from(PIECE_LENGTH),
        "the whole piece counts as corrupt"
    );

    // A peer that speaks v2 is asked for the piece's leaves, proven to
    // its node of the tree.
    let (v2_peer, mut v2_rx) = h.peer_speaking("127.0.0.3:6881", true);
    let mut fetch: Task<'_> = Box::pin(v2_peer.fetch_layers());
    let request = next_hash_request(&mut fetch, &mut v2_rx).await;
    assert_eq!(
        request.root,
        file_hashes(&torrent.files[0].1, PIECE_LENGTH.into())
            .0
            .unwrap()
    );
    assert_eq!(shape(request), (0, 0, CHUNKS_PER_PIECE, 1));

    // Leaves that do not prove the piece drop the peer, and blame no
    // one.
    let mut wrong = leaves(&torrent, 0);
    wrong[1] = merkle::hash_block(b"wrong");
    assert!(
        v2_peer
            .on_hashes(Hashes::new(request, &wrong).as_borrowed())
            .is_err()
    );
    assert_eq!(h.trust(&bad), (0, 0, false));
    h.layer_fetch.forget(v2_peer.addr);

    // The leaves come: the sender of the block that differs is
    // penalised and banned; the other keeps its standing.
    let (other, mut other_rx) = h.peer_speaking("127.0.0.4:6881", true);
    let mut other_fetch: Task<'_> = Box::pin(other.fetch_layers());
    assert_eq!(
        next_hash_request(&mut other_fetch, &mut other_rx).await,
        request
    );
    other
        .on_hashes(Hashes::new(request, &leaves(&torrent, 0)).as_borrowed())
        .unwrap();
    assert_eq!(h.trust(&bad), (-2, 1, true));
    assert!(h.banned(&bad) && told_to_disconnect(&mut bad_rx));
    assert_eq!(h.trust(&good), (0, 0, false));
    assert!(!h.banned(&good) && !told_to_disconnect(&mut good_rx));

    // The leaves are wanted no more.
    assert!(matches!(
        h.layer_fetch.pick(
            other.addr,
            |_| true,
            Duration::ZERO,
            tokio::time::Instant::now()
        ),
        Pick::Wait(None)
    ));
}

#[tokio::test]
async fn the_leaves_of_a_failed_v2_piece_blame_only_the_sender_of_its_bad_block() {
    the_leaves_of_a_failed_v2_piece_blame_only_its_bad_blocks_sender(false).await;
}

#[tokio::test]
async fn the_leaves_of_a_failed_hybrid_piece_blame_only_the_sender_of_its_bad_block() {
    the_leaves_of_a_failed_v2_piece_blame_only_its_bad_blocks_sender(true).await;
}

// No peer gave the leaves before the piece passed: the good copy names
// the sender of the bad block, as the smart ban does, and the leaves
// are wanted no more.
#[tokio::test]
async fn a_failed_v2_piece_that_passes_before_its_leaves_come_blames_by_the_good_copy() {
    let torrent = two_pieces(false);
    let h = Harness::v2("passes_before_leaves", &torrent).await;
    tokio::time::pause();
    let (bad, _bad_rx) = h.peer("127.0.0.1:6881");
    let (good, _good_rx) = h.peer("127.0.0.2:6881");
    let (third, _third_rx) = h.peer("127.0.0.3:6881");
    h.fail_shared(&bad, &good).await;

    h.take(&third, 0);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&third, (0, chunk), false).await.unwrap();
    }
    assert!(h.missing_chunks(0).is_empty());
    assert!(h.banned(&bad));
    assert!(!h.banned(&good) && !h.banned(&third));
    assert_eq!(h.trust(&good), (0, 0, false));
    let (v2_peer, _rx) = h.peer_speaking("127.0.0.4:6881", true);
    assert!(matches!(
        h.layer_fetch.pick(
            v2_peer.addr,
            |_| true,
            Duration::ZERO,
            tokio::time::Instant::now()
        ),
        Pick::Wait(None)
    ));
}

// libtorrent torrent::piece_failed: a piece one peer sent all of names
// that peer, with no block hashes to wait for.
#[tokio::test]
async fn the_sole_sender_of_a_failed_v2_piece_is_banned_at_once() {
    let torrent = two_pieces(false);
    let h = Harness::v2("v2_sole_sender", &torrent).await;
    tokio::time::pause();
    let (peer, mut rx) = h.peer("127.0.0.1:6881");
    h.take(&peer, 0);
    for chunk in 0..CHUNKS_PER_PIECE {
        h.send(&peer, (0, chunk), chunk == 2).await.unwrap();
    }
    assert_eq!(h.missing_chunks(0), vec![0, 1, 2, 3]);
    assert_eq!(h.trust(&peer), (-2, 1, true));
    assert!(h.banned(&peer) && told_to_disconnect(&mut rx));
    let (v2_peer, _rx) = h.peer_speaking("127.0.0.2:6881", true);
    assert!(matches!(
        h.layer_fetch.pick(
            v2_peer.addr,
            |_| true,
            Duration::ZERO,
            tokio::time::Instant::now()
        ),
        Pick::Wait(None)
    ));
}

#[tokio::test]
async fn a_hash_request_for_a_v1_torrent_drops_the_peer() {
    let h = Harness::new("a_hash_request_for_a_v1_torrent").await;
    let (peer, _rx) = h.peer_speaking("127.0.0.1:6881", true);
    let request = HashRequest {
        root: Id32::default(),
        base: 0,
        index: 0,
        count: 1,
        proof_layers: 0,
    };
    assert!(peer.on_hash_request(request).is_err());
}
