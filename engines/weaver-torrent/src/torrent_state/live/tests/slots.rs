// SPDX-License-Identifier: GPL-3.0-or-later

//! A block of a piece in flight lands under its piece's lock alone: the
//! torrent's lock is taken only for its piece's last block, which takes the
//! piece out of flight for its check.

use super::*;
use crate::piece_tracker::torrent_takes;

#[tokio::test]
async fn only_the_last_block_of_a_piece_in_flight_takes_the_torrent_lock() {
    let h = Harness::new("piece_locks").await;
    let (owner, _rx) = h.peer("127.0.0.1:6881");
    let (other, _other_rx) = h.peer("127.0.0.2:6881");
    h.take(&owner, 0);
    let land = |peer: &PeerHandler, chunk: u32| {
        let info = h.chunk(0, chunk);
        let start = (chunk * CHUNK_SIZE) as usize;
        let data = &h.content[start..start + CHUNK_SIZE as usize];
        let piece = Piece::from_data(0, chunk * CHUNK_SIZE, data);
        let before = torrent_takes();
        let landed = land_block(&h.live, peer.addr, &piece, &info).unwrap();
        (torrent_takes() - before, landed.is_some())
    };
    // The owner's blocks, and another peer's, but the last.
    assert_eq!(land(&owner, 0), (0, false));
    assert_eq!(land(&other, 1), (0, false));
    assert_eq!(land(&owner, 2), (0, false));
    // A copy of a block had: redundant, and still no torrent lock.
    let redundant = h.get_redundant_bytes();
    assert_eq!(land(&other, 2), (0, false));
    assert_eq!(h.get_redundant_bytes() - redundant, u64::from(CHUNK_SIZE));
    // The last: one take, and the piece goes to its check.
    assert_eq!(land(&owner, 3), (1, true));
}
