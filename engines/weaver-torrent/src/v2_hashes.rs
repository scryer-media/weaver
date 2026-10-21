// SPDX-License-Identifier: GPL-3.0-or-later

//! The v2 hashes a torrent checks its pieces against: each file's root and,
//! for a file larger than a piece, its piece layer once held. A v2 file
//! starts a piece of its own, so every piece lies in one file, and its v2
//! hash covers only that file's share of it.

use std::sync::OnceLock;

use crate::core::{
    Id32,
    merkle::{self, BLOCK_SIZE, PieceTree},
    torrent_metainfo::ValidatedTorrentMetaV1Info,
};

/// A file with data, its pieces and what proves them.
#[derive(Debug)]
pub struct V2File {
    pub root: Id32,
    pub length: u64,
    pub first_piece: u32,
    pub pieces: u32,
    /// The tree over the file's piece layer, once held. Never set for a
    /// file of one piece, which its root proves.
    tree: OnceLock<PieceTree>,
}

impl V2File {
    pub fn tree(&self) -> Option<&PieceTree> {
        self.tree.get()
    }
}

#[derive(Debug)]
pub struct V2Hashes {
    piece_length: u64,
    /// By first piece.
    files: Vec<V2File>,
}

impl V2Hashes {
    /// The v2 hashes of a v2 or hybrid torrent, with the piece layers it
    /// carries, one entry per file and checked against their roots
    /// ([`ValidatedTorrentMetaV1Info::piece_layers`]). `None` for v1.
    pub fn new<B: AsRef<[u8]>>(
        info: &ValidatedTorrentMetaV1Info<B>,
        layers: Vec<Option<Vec<Id32>>>,
    ) -> Option<Self> {
        let piece_length = u64::from(info.lengths().default_piece_length());
        let mut layers = layers.into_iter();
        let files = info
            .v2_files()?
            .iter()
            .filter_map(|f| {
                let layer = layers.next().flatten();
                let tree = OnceLock::new();
                if let Some(layer) = layer {
                    let _ = tree.set(PieceTree::from_piece_layer(
                        layer,
                        piece_length / BLOCK_SIZE,
                    ));
                }
                Some(V2File {
                    root: f.pieces_root?,
                    length: f.length,
                    first_piece: (f.offset / piece_length) as u32,
                    pieces: f.length.div_ceil(piece_length) as u32,
                    tree,
                })
            })
            .collect();
        Some(Self {
            piece_length,
            files,
        })
    }

    /// The file `piece` lies in.
    pub fn file_of(&self, piece: u32) -> Option<&V2File> {
        let at = self.files.partition_point(|f| f.first_piece <= piece);
        let f = &self.files[at.checked_sub(1)?];
        (piece - f.first_piece < f.pieces).then_some(f)
    }

    /// The bytes of `piece` its file's v2 hash covers: the piece up to the
    /// file's end, the padding after it left out.
    pub fn share(&self, piece: u32) -> Option<u64> {
        let f = self.file_of(piece)?;
        let start = u64::from(piece - f.first_piece) * self.piece_length;
        Some((f.length - start).min(self.piece_length))
    }

    /// The hash `piece`'s blocks must have: its entry in its file's piece
    /// layer, or the file's root for a file of one piece. `None` until the
    /// layer is held (libtorrent `hash_picker::set_block_hash`, which
    /// leaves a piece unverified until the layer above it is known).
    pub fn expected(&self, piece: u32) -> Option<Id32> {
        let f = self.file_of(piece)?;
        if f.length <= self.piece_length {
            return Some(f.root);
        }
        f.tree()
            .map(|tree| tree.pieces()[(piece - f.first_piece) as usize])
    }

    /// Whether `piece`'s block hashes, the SHA-256 of each 16 KiB block of
    /// its file's share, hash to what is expected of it; `None` when that
    /// is not known yet. The v2 sibling of
    /// [`TorrentMetaV1Info::compare_hash`](crate::core::torrent_metainfo::TorrentMetaV1Info::compare_hash)
    /// (libtorrent `torrent::on_blocks_hashed`).
    pub fn compare_hash(&self, piece: u32, blocks: &[Id32]) -> Option<bool> {
        let f = self.file_of(piece)?;
        let expected = self.expected(piece)?;
        Some(merkle::piece_root(blocks, f.length, self.piece_length) == expected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        torrent_metainfo::torrent_from_bytes,
        v2_test_torrent::{TestTorrent, file_hashes, payload},
    };

    fn blocks(data: &[u8]) -> Vec<Id32> {
        data.chunks(BLOCK_SIZE as usize)
            .map(merkle::hash_block)
            .collect()
    }

    fn hashes(t: &TestTorrent) -> V2Hashes {
        let bytes = t.torrent_bytes();
        let meta = torrent_from_bytes(&bytes).unwrap();
        let info = meta.info.data.clone().validate().unwrap();
        let layers = info
            .piece_layers(meta.piece_layers.as_ref().unwrap())
            .unwrap();
        V2Hashes::new(&info, layers).unwrap()
    }

    #[test]
    fn a_piece_checks_against_its_layer_entry_or_its_files_root() {
        // A file under a block, one of exactly a piece, and one of three
        // pieces and a tail, each starting a piece of its own.
        let piece_length = 65536;
        let t = TestTorrent::new(
            vec![
                ("a", payload(100, 1)),
                ("b", payload(65536, 2)),
                ("c", payload(3 * 65536 + 5, 3)),
            ],
            piece_length,
        );
        let v2 = hashes(&t);
        let (_, c) = &t.files[2];
        let first = |piece| v2.file_of(piece).unwrap().first_piece;
        assert_eq!(first(0), 0);
        assert_eq!(first(1), 1);
        assert_eq!(first(2), 2);
        assert_eq!(first(5), 2);
        assert!(v2.file_of(6).is_none());
        assert_eq!(v2.share(0), Some(100));
        assert_eq!(v2.share(5), Some(5));

        assert_eq!(v2.expected(0), file_hashes(&t.files[0].1, piece_length).0);
        assert_eq!(v2.compare_hash(0, &blocks(&t.files[0].1)), Some(true));
        assert_eq!(v2.compare_hash(1, &blocks(&t.files[1].1)), Some(true));
        for (i, piece) in c.chunks(piece_length as usize).enumerate() {
            assert_eq!(v2.compare_hash(2 + i as u32, &blocks(piece)), Some(true));
        }
        // A block hashed wrong fails its piece.
        let mut bad = blocks(&c[..piece_length as usize]);
        bad[1].0[0] ^= 1;
        assert_eq!(v2.compare_hash(2, &bad), Some(false));
        // The blocks of another piece of the file fail it.
        assert_eq!(
            v2.compare_hash(3, &blocks(&c[..piece_length as usize])),
            Some(false)
        );
    }

    #[test]
    fn a_multi_piece_file_is_unknown_until_its_layer_is_held() {
        let piece_length = 32768;
        let mut t = TestTorrent::new(
            vec![("a", payload(1000, 1)), ("b", payload(70000, 2))],
            piece_length,
        );
        t.layers = false;
        let bytes = t.torrent_bytes();
        let meta = torrent_from_bytes(&bytes).unwrap();
        let info = meta.info.data.clone().validate().unwrap();
        let v2 = V2Hashes::new(&info, Vec::new()).unwrap();
        // The one-piece file is proven by its root alone.
        assert_eq!(v2.compare_hash(0, &blocks(&t.files[0].1)), Some(true));
        let b = &t.files[1].1;
        assert_eq!(v2.expected(1), None);
        assert_eq!(v2.compare_hash(1, &blocks(&b[..32768])), None);
    }
}
