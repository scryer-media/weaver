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
    /// Each file's index in `files`, by root (libtorrent
    /// `file_storage::file_index_for_root`).
    by_root: Vec<(Id32, usize)>,
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
                let pieces = u32::try_from(f.length.div_ceil(piece_length))
                    .expect("piece indexes fit in u32");
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
                    first_piece: u32::try_from(f.offset / piece_length)
                        .expect("piece indexes fit in u32"),
                    pieces,
                    chunks: (0..chunks).map(|_| OnceLock::new()).collect(),
                    tree,
                })
            })
            .collect::<Vec<V2File>>();
        let mut by_root: Vec<_> = files.iter().enumerate().map(|(i, f)| (f.root, i)).collect();
        by_root.sort();
        Some(Self {
            piece_length,
            files,
            by_root,
        })
    }

    fn blocks_per_piece(&self) -> u64 {
        self.piece_length / BLOCK_SIZE
    }

    /// The files whose tree has `root`, the first first: files of the same
    /// content share one.
    fn with_root(&self, root: &Id32) -> impl Iterator<Item = (usize, &V2File)> {
        let start = self.by_root.partition_point(|(r, _)| r < root);
        self.by_root[start..]
            .iter()
            .take_while(move |(r, _)| r == root)
            .map(|&(_, i)| (i, &self.files[i]))
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

    /// Every run of a piece layer not yet held, as (file, run), once for
    /// files that share a root.
    pub fn missing_chunks(&self) -> impl Iterator<Item = (usize, u32)> + '_ {
        self.files
            .iter()
            .enumerate()
            .filter(|(file, f)| {
                !f.layer_held() && self.with_root(&f.root).next().map(|(i, _)| i) == Some(*file)
            })
            .flat_map(|(file, f)| {
                f.chunks
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.get().is_none())
                    .map(move |(chunk, _)| {
                        (
                            file,
                            u32::try_from(chunk).expect("a file's layer runs fit in u32"),
                        )
                    })
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
            count: u32::try_from(merkle::num_leafs(u64::from(f.pieces - index)))
                .map_or(LAYER_CHUNK, |count| count.min(LAYER_CHUNK)),
            proof_layers: merkle::num_layers(pieces) - 1,
        }
    }

    /// Takes the answer to a [`Self::chunk_request`] for a run of a piece
    /// layer: its nodes, then their uncles. They must prove the root asked
    /// for (libtorrent `hash_picker::add_hashes`), and the run is then held
    /// by every file of that root; a file's tree is built once all its runs
    /// are. Returns whether every file's layer is then held.
    pub fn add_chunk(&self, request: &HashRequest, hashes: &[Id32]) -> Result<bool, InvalidHashes> {
        let (nodes, uncles) = hashes
            .split_at_checked(request.count as usize)
            .ok_or(InvalidHashes)?;
        let pad = merkle::pad_hash(self.blocks_per_piece(), 1);
        if merkle::proven_root(nodes, request.index.into(), pad, uncles) != request.root {
            return Err(InvalidHashes);
        }
        for (_, f) in self.with_root(&request.root) {
            let held = f.pieces.saturating_sub(request.index).min(LAYER_CHUNK) as usize;
            let Some(chunk) = f.chunks.get((request.index / LAYER_CHUNK) as usize) else {
                continue;
            };
            let _ = chunk.set(nodes[..held].into());
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
        }
        Ok(self.files.iter().all(V2File::layer_held))
    }

    /// The hashes that answer a peer's request, or `None` to reject it
    /// (libtorrent `bt_peer_connection::on_hash_request`): a request
    /// `validate_hash_request` admits, for a file whose tree is held, from
    /// its piece layer up, of no more nodes than a reply carries. O(count +
    /// log n), from the tree built once per file.
    pub fn answer(&self, request: &HashRequest) -> Option<Vec<Id32>> {
        let (_, f) = self.with_root(&request.root).next()?;
        // libtorrent `validate_hash_request`.
        let leafs = merkle::num_leafs(f.length.div_ceil(BLOCK_SIZE));
        let layers = merkle::num_layers(leafs);
        let (base, index, count) = (
            request.base,
            u64::from(request.index),
            u64::from(request.count),
        );
        if count == 0 || count > u64::from(MAX_HASHES_IN_REPLY) || base >= layers {
            return None;
        }
        let level = leafs >> base;
        if index >= level || index + count > level || request.proof_layers >= layers - base {
            return None;
        }
        // Weaver keeps no hashes below the piece layer.
        let above_pieces = base.checked_sub(merkle::num_layers(self.blocks_per_piece()))?;
        f.tree()?
            .hashes(above_pieces, index, count, request.proof_layers)
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
        data.chunks(usize::try_from(BLOCK_SIZE).unwrap())
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
        for (i, piece) in c.chunks(usize::try_from(piece_length).unwrap()).enumerate() {
            assert_eq!(
                v2.compare_hash(2 + u32::try_from(i).unwrap(), &blocks(piece)),
                Some(true)
            );
        }
        // A block hashed wrong fails its piece.
        let mut bad = blocks(&c[..usize::try_from(piece_length).unwrap()]);
        bad[1].0[0] ^= 1;
        assert_eq!(v2.compare_hash(2, &bad), Some(false));
        // The blocks of another piece of the file fail it.
        assert_eq!(
            v2.compare_hash(3, &blocks(&c[..usize::try_from(piece_length).unwrap()])),
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
            assert!(v2.add_chunk(&last, bad).is_err());
        }
        assert_eq!(v2.expected(1024), None);

        // The last run first: its pieces are known, the others not yet.
        assert!(!v2.add_chunk(&last, &hashes).unwrap());
        assert_eq!(v2.expected(1028), Some(layer[1028]));
        assert_eq!(v2.expected(0), None);
        assert!(v2.layers().is_none());

        let first = v2.chunk_request(0, 0);
        assert!(!v2.add_chunk(&first, &answer(&first)).unwrap());
        let middle = v2.chunk_request(0, 1);
        assert!(v2.add_chunk(&middle, &answer(&middle)).unwrap());

        assert!((0..1029).all(|p| v2.expected(p) == Some(layer[p as usize])));
        assert_eq!(v2.missing_chunks().count(), 0);
        let held = v2.layers().unwrap();
        assert_eq!(held, vec![(tree.root(), layer.as_slice())]);
    }

    /// [`one_file`] with every run of its layer held, as a seed holds it.
    fn seeded(layer: &[Id32], piece_length: u64) -> V2Hashes {
        let v2 = one_file(layer, piece_length);
        let tree = PieceTree::from_piece_layer(layer.to_vec(), piece_length / BLOCK_SIZE);
        let runs: Vec<_> = v2.missing_chunks().collect();
        for (file, chunk) in runs {
            let r = v2.chunk_request(file, chunk);
            let hashes = tree
                .hashes(0, r.index.into(), r.count.into(), r.proof_layers)
                .unwrap();
            v2.add_chunk(&r, &hashes).unwrap();
        }
        v2
    }

    #[test]
    fn hash_requests_are_answered_as_libtorrent_validates_them() {
        // test_hash_picker.cpp `validate_hash_request`: 16 KiB pieces, a
        // file of 2048 blocks, so the piece layer is the leaves.
        let layer = made_up_layer(2048);
        let v2 = seeded(&layer, 16384);
        let root = v2.files[0].root;
        let (leaves, layers) = (2048, 11);
        let ask = |base, index, count, proof_layers| HashRequest {
            root,
            base,
            index,
            count,
            proof_layers,
        };

        // Admitted: the nodes, then the uncles the request counts; an
        // aligned range whose proof reaches the root proves it.
        for r in [
            ask(0, 0, 1, 0),
            ask(layers - 1, 0, 1, 0),
            ask(2, 0, leaves / 4, 0),
            ask(3, 0, leaves / 8, 0),
            ask(3, 5, leaves / 8 - 5, 0),
            ask(0, 0, 1, layers - 1),
            ask(1, 0, 1, layers - 2),
            ask(0, 1536, 512, 10),
        ] {
            let got = v2.answer(&r).unwrap();
            assert_eq!(got.len() as u64, u64::from(r.count) + r.uncles(), "{r:?}");
            if r.base == 0 {
                let at = r.index as usize;
                assert_eq!(&got[..r.count as usize], &layer[at..at + r.count as usize]);
            }
            let span = merkle::num_leafs(r.count.into());
            let top = r.base + (r.proof_layers + 1).max(merkle::num_layers(span));
            if u64::from(r.index) % span == 0 && top == layers {
                let (nodes, uncles) = got.split_at(r.count as usize);
                let pad = merkle::pad_hash(1 << r.base, 1);
                assert_eq!(
                    merkle::proven_root(nodes, r.index.into(), pad, uncles),
                    root,
                    "{r:?}"
                );
            }
        }

        // Refused by libtorrent's checks.
        for r in [
            ask(layers, 0, 1, 0),
            ask(0, 0, 0, 0),
            ask(0, 0, leaves + 1, 0),
            ask(0, 100, leaves - 100 + 1, 0),
            ask(0, 0, 8193, 0),
            ask(0, 0, u32::MAX, 0),
            ask(1, 0, leaves / 2 + 1, 0),
            ask(2, 0, leaves / 4 + 1, 0),
            ask(3, 0, leaves / 8 + 1, 0),
            ask(3, 5, leaves / 8 - 5 + 1, 0),
            ask(0, 0, 1, layers),
            ask(1, 0, 1, layers),
            ask(1, 0, 1, u32::MAX),
            ask(0, u32::MAX, 1, 0),
        ] {
            assert_eq!(v2.answer(&r), None, "{r:?}");
        }
        // Admitted by libtorrent, but more nodes than a reply carries.
        for r in [
            ask(0, 0, leaves, 0),
            ask(0, 100, leaves - 100, 0),
            ask(1, 0, leaves / 2, 0),
        ] {
            assert_eq!(v2.answer(&r), None, "{r:?}");
        }
        // A root of no file.
        assert_eq!(
            v2.answer(&HashRequest {
                root: layer[0],
                ..ask(0, 0, 1, 0)
            }),
            None
        );
    }

    #[test]
    fn hashes_below_the_piece_layer_or_of_a_layer_not_held_are_refused() {
        // `add_piece_hashes`: pieces of four blocks, so the piece layer is
        // base 2 and the blocks under it are not kept.
        let layer = made_up_layer(1024);
        let v2 = seeded(&layer, 65536);
        let root = v2.files[0].root;
        let ask = |base| HashRequest {
            root,
            base,
            index: 0,
            count: 1,
            proof_layers: 0,
        };
        assert_eq!(v2.answer(&ask(0)), None);
        assert_eq!(v2.answer(&ask(1)), None);
        assert_eq!(v2.answer(&ask(2)).unwrap(), vec![layer[0], layer[1]]);

        // The same file before its layer is held.
        assert_eq!(one_file(&layer, 65536).answer(&ask(2)), None);
    }

    #[test]
    fn files_that_share_a_root_share_one_fetch_of_their_layer() {
        // libtorrent `file_storage::file_index_for_root`: one root, one
        // layer, whichever file it is asked for.
        let data = payload(3 * 16384, 1);
        let mut t = TestTorrent::new(vec![("a", data.clone()), ("b", data.clone())], 16384);
        t.layers = false;
        let bytes = t.torrent_bytes();
        let meta = torrent_from_bytes(&bytes).unwrap();
        let info = meta.info.data.clone().validate().unwrap();
        let v2 = V2Hashes::new(&info, Vec::new()).unwrap();
        let (root, layer) = file_hashes(&data, 16384);
        let (root, layer) = (root.unwrap(), layer.unwrap());

        let runs: Vec<_> = v2.missing_chunks().collect();
        assert_eq!(runs.len(), 1);
        let r = v2.chunk_request(runs[0].0, runs[0].1);
        assert_eq!(r.root, root);
        let tree = PieceTree::from_piece_layer(layer.clone(), 1);
        let hashes = tree
            .hashes(0, r.index.into(), r.count.into(), r.proof_layers)
            .unwrap();
        assert!(v2.add_chunk(&r, &hashes).unwrap());
        assert!((0..6).all(|p| v2.expected(p) == Some(layer[p as usize % 3])));
        assert_eq!(
            v2.answer(&HashRequest {
                count: 1,
                proof_layers: 0,
                ..r
            })
            .unwrap()[0],
            layer[0]
        );
    }
}
