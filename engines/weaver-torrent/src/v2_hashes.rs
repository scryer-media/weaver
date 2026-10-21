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
use crate::peer_protocol::{HashRequest, MAX_HASHES_IN_REPLY};

/// The pieces of a file's layer one request asks for (libtorrent
/// `hash_picker::pick_hashes`).
pub const LAYER_CHUNK: u32 = MAX_HASHES_IN_REPLY;

/// A file with data, its pieces and what proves them.
#[derive(Debug)]
pub struct V2File {
    pub root: Id32,
    pub length: u64,
    pub first_piece: u32,
    pub pieces: u32,
    /// The file's piece layer in runs of [`LAYER_CHUNK`] pieces, each set
    /// once a peer's hashes for it prove `root`. Empty for a file of one
    /// piece, which its root proves.
    chunks: Vec<OnceLock<Box<[Id32]>>>,
    /// The tree over the file's piece layer, once all of it is held.
    tree: OnceLock<PieceTree>,
}

impl V2File {
    pub fn tree(&self) -> Option<&PieceTree> {
        self.tree.get()
    }

    /// Whether the file's piece layer is held, or it needs none.
    fn layer_held(&self) -> bool {
        self.pieces == 1 || self.tree.get().is_some()
    }
}

/// Hashes that do not prove the root of the file they were asked for
/// (libtorrent `errors::invalid_hashes`).
#[derive(Debug, thiserror::Error)]
#[error("hashes that do not prove their file's root")]
pub struct InvalidHashes;

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
                let pieces = f.length.div_ceil(piece_length) as u32;
                let tree = OnceLock::new();
                if let Some(layer) = layer {
                    let _ = tree.set(PieceTree::from_piece_layer(
                        layer,
                        piece_length / BLOCK_SIZE,
                    ));
                }
                let chunks = if pieces > 1 {
                    pieces.div_ceil(LAYER_CHUNK)
                } else {
                    0
                };
                Some(V2File {
                    root: f.pieces_root?,
                    length: f.length,
                    first_piece: (f.offset / piece_length) as u32,
                    pieces,
                    chunks: (0..chunks).map(|_| OnceLock::new()).collect(),
                    tree,
                })
            })
            .collect();
        Some(Self {
            piece_length,
            files,
        })
    }

    fn blocks_per_piece(&self) -> u64 {
        self.piece_length / BLOCK_SIZE
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
    /// run of the layer it lies in is held (libtorrent
    /// `hash_picker::set_block_hash`, which leaves a piece unverified until
    /// the layer above it is known).
    pub fn expected(&self, piece: u32) -> Option<Id32> {
        let f = self.file_of(piece)?;
        if f.pieces == 1 {
            return Some(f.root);
        }
        let i = piece - f.first_piece;
        if let Some(tree) = f.tree() {
            return Some(tree.pieces()[i as usize]);
        }
        f.chunks[(i / LAYER_CHUNK) as usize]
            .get()
            .map(|chunk| chunk[(i % LAYER_CHUNK) as usize])
    }

    /// Every run of a piece layer not yet held, as (file, run).
    pub fn missing_chunks(&self) -> impl Iterator<Item = (usize, u32)> + '_ {
        self.files
            .iter()
            .enumerate()
            .filter(|(_, f)| !f.layer_held())
            .flat_map(|(file, f)| {
                f.chunks
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.get().is_none())
                    .map(move |(chunk, _)| (file, chunk as u32))
            })
    }

    /// The pieces of the torrent a run of a file's layer covers.
    pub fn chunk_pieces(&self, file: usize, chunk: u32) -> std::ops::Range<u32> {
        let f = &self.files[file];
        let start = chunk * LAYER_CHUNK;
        f.first_piece + start..f.first_piece + f.pieces.min(start + LAYER_CHUNK)
    }

    /// The request for a run of a file's piece layer: the piece layer as
    /// base, the run's pieces padded to a power of two, proven to the
    /// file's root (libtorrent `hash_picker::pick_hashes`).
    pub fn chunk_request(&self, file: usize, chunk: u32) -> HashRequest {
        let f = &self.files[file];
        let index = chunk * LAYER_CHUNK;
        let pieces = merkle::num_leafs(u64::from(f.pieces));
        HashRequest {
            root: f.root,
            base: merkle::num_layers(self.blocks_per_piece()),
            index,
            count: merkle::num_leafs(u64::from(f.pieces - index)).min(u64::from(LAYER_CHUNK))
                as u32,
            proof_layers: merkle::num_layers(pieces) - 1,
        }
    }

    /// Takes the answer to [`Self::chunk_request`] for a run of a file's
    /// layer: its nodes, then their uncles. They must prove the file's root
    /// (libtorrent `hash_picker::add_hashes`); once every run is held, the
    /// file's tree is built. Returns whether every file's layer is then held.
    pub fn add_chunk(
        &self,
        file: usize,
        request: &HashRequest,
        hashes: &[Id32],
    ) -> Result<bool, InvalidHashes> {
        let f = &self.files[file];
        let (nodes, uncles) = hashes
            .split_at_checked(request.count as usize)
            .ok_or(InvalidHashes)?;
        let pad = merkle::pad_hash(self.blocks_per_piece(), 1);
        if merkle::proven_root(nodes, request.index.into(), pad, uncles) != f.root {
            return Err(InvalidHashes);
        }
        let held = (f.pieces - request.index).min(LAYER_CHUNK) as usize;
        let _ = f.chunks[(request.index / LAYER_CHUNK) as usize].set(nodes[..held].into());
        if f.tree.get().is_none() && f.chunks.iter().all(|c| c.get().is_some()) {
            let layer = f
                .chunks
                .iter()
                .flat_map(|c| c.get().into_iter().flatten().copied())
                .collect();
            let _ = f
                .tree
                .set(PieceTree::from_piece_layer(layer, self.blocks_per_piece()));
        }
        Ok(self.files.iter().all(V2File::layer_held))
    }

    /// The piece layer of every file larger than a piece, by root, when
    /// every one is held.
    pub fn layers(&self) -> Option<Vec<(Id32, &[Id32])>> {
        self.files
            .iter()
            .filter(|f| f.pieces > 1)
            .map(|f| Some((f.root, f.tree()?.pieces())))
            .collect()
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

    /// The piece layer of a file of `pieces` pieces, made up.
    fn made_up_layer(pieces: u32) -> Vec<Id32> {
        (0..pieces)
            .map(|i| merkle::hash_block(&i.to_le_bytes()))
            .collect()
    }

    /// The v2 hashes of a torrent of one file whose piece layer is `layer`,
    /// with no layer held: the info names only its root.
    fn one_file(layer: &[Id32], piece_length: u64) -> V2Hashes {
        let root = merkle::layer_root(layer, piece_length / BLOCK_SIZE);
        let length = layer.len() as u64 * piece_length;
        let mut info =
            format!("d9:file treed1:ad0:d6:lengthi{length}e11:pieces root32:").into_bytes();
        info.extend(root.0);
        info.extend(
            format!("eee12:meta versioni2e4:name1:x12:piece lengthi{piece_length}ee").bytes(),
        );
        let torrent = [b"d4:info".as_slice(), &info, b"e"].concat();
        let meta = torrent_from_bytes(&torrent).unwrap();
        let info = meta.info.data.clone().validate().unwrap();
        V2Hashes::new(&info, Vec::new()).unwrap()
    }

    fn request(base: u32, index: u32, count: u32, proof_layers: u32) -> (u32, u32, u32, u32) {
        (base, index, count, proof_layers)
    }

    fn shape(r: HashRequest) -> (u32, u32, u32, u32) {
        (r.base, r.index, r.count, r.proof_layers)
    }

    #[test]
    fn layer_runs_are_requested_as_libtorrent_picks_them() {
        // test_hash_picker.cpp `pick_piece_layer`: 16 KiB pieces, a file of
        // 2048 pieces, runs of 512 from the piece layer, proven 10 layers.
        let v2 = one_file(&made_up_layer(2048), 16384);
        let runs: Vec<_> = v2.missing_chunks().collect();
        assert_eq!(runs, vec![(0, 0), (0, 1), (0, 2), (0, 3)]);
        for (file, chunk) in runs {
            assert_eq!(
                shape(v2.chunk_request(file, chunk)),
                request(0, chunk * 512, 512, 10)
            );
        }

        // `add_piece_hashes`: pieces of four blocks, a file of 1024 pieces.
        let v2 = one_file(&made_up_layer(1024), 65536);
        assert_eq!(shape(v2.chunk_request(0, 0)), request(2, 0, 512, 9));

        // `add_piece_hashes_padded`: 1029 pieces; the last run's five
        // pieces are asked for padded to eight.
        let v2 = one_file(&made_up_layer(1029), 65536);
        assert_eq!(shape(v2.chunk_request(0, 0)), request(2, 0, 512, 10));
        assert_eq!(shape(v2.chunk_request(0, 2)), request(2, 1024, 8, 10));
        assert_eq!(v2.chunk_pieces(0, 2), 1024..1029);
    }

    #[test]
    fn a_run_is_held_once_its_hashes_prove_the_root() {
        // test_hash_picker.cpp `add_piece_hashes_padded` and `add_bad_hashes`.
        let layer = made_up_layer(1029);
        let v2 = one_file(&layer, 65536);
        let tree = PieceTree::from_piece_layer(layer.clone(), 4);
        let answer = |r: &HashRequest| {
            tree.hashes(0, r.index.into(), r.count.into(), r.proof_layers)
                .unwrap()
        };

        let last = v2.chunk_request(0, 2);
        let hashes = answer(&last);
        // Eight nodes and eight uncles.
        assert_eq!(hashes.len(), 8 + 8);
        assert_eq!(last.uncles(), 8);

        // A wrong uncle, a wrong node, or a hash short: nothing is held.
        let mut bad_uncle = hashes.clone();
        bad_uncle.last_mut().unwrap().0[0] ^= 1;
        let mut bad_node = hashes.clone();
        bad_node[0].0[0] ^= 1;
        for bad in [&bad_uncle, &bad_node, &hashes[..15].to_vec()] {
            assert!(v2.add_chunk(0, &last, bad).is_err());
        }
        assert_eq!(v2.expected(1024), None);

        // The last run first: its pieces are known, the others not yet.
        assert!(!v2.add_chunk(0, &last, &hashes).unwrap());
        assert_eq!(v2.expected(1028), Some(layer[1028]));
        assert_eq!(v2.expected(0), None);
        assert!(v2.layers().is_none());

        let first = v2.chunk_request(0, 0);
        assert!(!v2.add_chunk(0, &first, &answer(&first)).unwrap());
        let middle = v2.chunk_request(0, 1);
        assert!(v2.add_chunk(0, &middle, &answer(&middle)).unwrap());

        assert!((0..1029).all(|p| v2.expected(p) == Some(layer[p as usize])));
        assert_eq!(v2.missing_chunks().count(), 0);
        let held = v2.layers().unwrap();
        assert_eq!(held, vec![(tree.root(), layer.as_slice())]);
    }
}
