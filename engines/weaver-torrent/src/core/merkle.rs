// SPDX-License-Identifier: GPL-3.0-or-later

//! BEP 52 merkle trees. A v2 file is hashed as a binary tree whose leaves
//! are the SHA-256 of each 16 KiB block of the file, the last block hashed
//! at its own length. A tree always has a power-of-two number of leaves:
//! the leaves past the file's last block are 32 zero bytes, so every node
//! above them is a "pad" hash that depends only on its height.
//!
//! Layers are counted from the leaves: layer 0 is the block layer, layer
//! `log2(blocks per piece)` is the piece layer, and the root sits on top.

use crate::core::Id32;
use crate::sha1w::{ISha256, Sha256};

/// The size of a leaf block (libtorrent `default_block_size`).
pub const BLOCK_SIZE: u64 = 16384;

/// The SHA-256 of one block, a leaf of the tree.
pub fn hash_block(data: &[u8]) -> Id32 {
    let mut h = Sha256::new();
    h.update(data);
    Id32::new(h.finish())
}

/// The parent of two nodes.
pub fn hash_pair(left: &Id32, right: &Id32) -> Id32 {
    let mut h = Sha256::new();
    h.update(&left.0);
    h.update(&right.0);
    Id32::new(h.finish())
}

/// The leaves of a tree over `n` nodes: `n` rounded up to a power of two
/// (libtorrent `merkle_num_leafs`).
pub fn num_leafs(n: u64) -> u64 {
    n.max(1).next_power_of_two()
}

/// The layers above the leaves of a tree with `leafs` leaves, a power of
/// two (libtorrent `merkle_num_layers`).
pub fn num_layers(leafs: u64) -> u32 {
    debug_assert!(leafs.is_power_of_two());
    leafs.trailing_zeros()
}

/// The hash of a padding node on the layer that has `nodes` nodes, in a
/// tree that has `leafs` leaves (libtorrent `merkle_pad`).
pub fn pad_hash(leafs: u64, nodes: u64) -> Id32 {
    let mut pad = Id32::default();
    let mut nodes = nodes;
    while nodes < leafs {
        pad = hash_pair(&pad, &pad);
        nodes *= 2;
    }
    pad
}

/// The root of a tree of `width` leaves, a power of two, whose first
/// leaves are `leaves` and whose remaining leaves are `pad`
/// (libtorrent `merkle_root_scratch`).
pub fn root(leaves: &[Id32], width: u64, pad: Id32) -> Id32 {
    debug_assert!(width.is_power_of_two() && leaves.len() as u64 <= width);
    let mut level = leaves.to_vec();
    let mut pad = pad;
    let mut width = width;
    while width > 1 {
        level = parents(&level, &pad);
        pad = hash_pair(&pad, &pad);
        width /= 2;
    }
    level.first().copied().unwrap_or(pad)
}

/// The layer above `level`, the node past its end, if any, paired with
/// `pad`.
fn parents(level: &[Id32], pad: &Id32) -> Vec<Id32> {
    level
        .chunks(2)
        .map(|pair| hash_pair(&pair[0], pair.get(1).unwrap_or(pad)))
        .collect()
}

/// The root of one piece's subtree from its block hashes. A piece of a
/// file larger than a piece spans `blocks_per_piece` leaves; a file no
/// larger than a piece is its own tree, `num_leafs(blocks)` wide
/// (libtorrent `create_torrent` `on_hash`).
pub fn piece_root(block_hashes: &[Id32], file_length: u64, piece_length: u64) -> Id32 {
    let width = if file_length < piece_length {
        num_leafs(file_length.div_ceil(BLOCK_SIZE))
    } else {
        piece_length / BLOCK_SIZE
    };
    root(block_hashes, width, Id32::default())
}

/// The root a file's piece layer proves, the layer padded with the hash of
/// an all-padding piece (libtorrent `merkle_tree::load_piece_layer`).
pub fn layer_root(layer: &[Id32], blocks_per_piece: u64) -> Id32 {
    root(
        layer,
        num_leafs(layer.len() as u64),
        pad_hash(blocks_per_piece, 1),
    )
}

/// Every layer of a file's tree from its piece layer to its root, each
/// holding only the nodes above real pieces: a padding node is
/// [`pad_hash`] of its height. Built once per file, it answers any range
/// of a layer and its proof in O(log n).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PieceTree {
    /// `layers[0]` is the piece layer, the last is the root alone.
    layers: Vec<Vec<Id32>>,
    /// The padding node of each layer.
    pads: Vec<Id32>,
}

impl PieceTree {
    /// The tree over a piece layer of a file with `blocks_per_piece`
    /// blocks to a piece.
    pub fn from_piece_layer(layer: Vec<Id32>, blocks_per_piece: u64) -> Self {
        let height = num_layers(num_leafs(layer.len() as u64)) as usize;
        let mut layers = Vec::with_capacity(height + 1);
        let mut pads = Vec::with_capacity(height + 1);
        let mut pad = pad_hash(blocks_per_piece, 1);
        let mut level = layer;
        for _ in 0..height {
            let up = parents(&level, &pad);
            layers.push(std::mem::replace(&mut level, up));
            pads.push(pad);
            pad = hash_pair(&pad, &pad);
        }
        layers.push(level);
        pads.push(pad);
        Self { layers, pads }
    }

    pub fn root(&self) -> Id32 {
        self.layers[self.layers.len() - 1][0]
    }

    /// The piece layer.
    pub fn pieces(&self) -> &[Id32] {
        &self.layers[0]
    }

    /// The layers above the piece layer.
    pub fn height(&self) -> u32 {
        (self.layers.len() - 1) as u32
    }

    fn node(&self, layer: usize, index: u64) -> Id32 {
        self.layers[layer]
            .get(index as usize)
            .copied()
            .unwrap_or(self.pads[layer])
    }

    /// `count` nodes of `layer` (counted from the piece layer) from
    /// `index`, then the uncle hashes that prove them, lowest first: the
    /// siblings on each layer from the top of the subtree the range spans
    /// up to `layer + proof_layers` (libtorrent `merkle_tree::get_hashes`).
    /// `None` when the range or the proof leaves the tree.
    pub fn hashes(
        &self,
        layer: u32,
        index: u64,
        count: u64,
        proof_layers: u32,
    ) -> Option<Vec<Id32>> {
        let height = self.height();
        if count == 0 || layer >= height || proof_layers >= height - layer {
            return None;
        }
        let width = 1u64 << (height - layer);
        if index >= width || count > width - index {
            return None;
        }
        let layer = layer as usize;
        let mut out: Vec<Id32> = (index..index + count)
            .map(|i| self.node(layer, i))
            .collect();
        let implied = num_layers(num_leafs(count));
        let mut cursor = index >> implied;
        for up in implied..=proof_layers {
            out.push(self.node(layer + up as usize, cursor ^ 1));
            cursor >>= 1;
        }
        Some(out)
    }
}

/// The root that `nodes`, a range of `count` nodes from `index` on some
/// layer, and their `uncles`, lowest first, prove; `pad` is that layer's
/// padding node. A range shorter than its power-of-two span is padded.
pub fn proven_root(nodes: &[Id32], index: u64, pad: Id32, uncles: &[Id32]) -> Id32 {
    let width = num_leafs(nodes.len() as u64);
    let mut node = root(nodes, width, pad);
    let mut cursor = index >> num_layers(width);
    for uncle in uncles {
        node = if cursor & 1 == 0 {
            hash_pair(&node, uncle)
        } else {
            hash_pair(uncle, &node)
        };
        cursor >>= 1;
    }
    node
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(byte: u8) -> Id32 {
        hash_block(&[byte])
    }

    #[test]
    fn num_leafs_rounds_up_to_a_power_of_two() {
        // test_merkle.cpp `num_leafs`.
        let expected = [
            1, 2, 4, 4, 8, 8, 8, 8, 16, 16, 16, 16, 16, 16, 16, 16, 32, 32,
        ];
        for (n, want) in (1..=18).zip(expected) {
            assert_eq!(num_leafs(n), want, "{n}");
        }
        // test_merkle.cpp `merkle_num_layers`.
        assert_eq!(num_layers(1), 0);
        assert_eq!(num_layers(2), 1);
        assert_eq!(num_layers(4), 2);
        assert_eq!(num_layers(8), 3);
        assert_eq!(num_layers(16), 4);
    }

    #[test]
    fn pad_hash_climbs_from_the_zero_leaf() {
        // test_merkle.cpp `merkle_pad`.
        let zero = Id32::default();
        let pad1 = hash_pair(&zero, &zero);
        let pad2 = hash_pair(&pad1, &pad1);
        for n in [1, 2, 4, 8, 16] {
            assert_eq!(pad_hash(n, n), zero);
        }
        for (leafs, nodes) in [(2, 1), (4, 2), (8, 4), (16, 8)] {
            assert_eq!(pad_hash(leafs, nodes), pad1);
        }
        for (leafs, nodes) in [(4, 1), (8, 2), (16, 4), (32, 8)] {
            assert_eq!(pad_hash(leafs, nodes), pad2);
        }
    }

    #[test]
    fn root_pads_an_uneven_layer() {
        // test_merkle.cpp `merkle_root` and `merkle_root_scratch`.
        let [a, b, c, d, e, f, g, hh] = [1, 2, 3, 4, 5, 6, 7, 8].map(h);
        let o = h(0);
        let ab = hash_pair(&a, &b);
        let cd = hash_pair(&c, &d);
        let ef = hash_pair(&e, &f);
        let gh = hash_pair(&g, &hh);
        let ad = hash_pair(&ab, &cd);
        let eh = hash_pair(&ef, &gh);
        let ah = hash_pair(&ad, &eh);
        let oo = hash_pair(&o, &o);
        assert_eq!(root(&[a, b, c, d, e, f, g, hh], 8, o), ah);
        assert_eq!(
            root(&[a, b, c, d, e, f], 8, o),
            hash_pair(&ad, &hash_pair(&ef, &oo))
        );
        assert_eq!(root(&[a, b], 2, o), ab);
        assert_eq!(root(&[a], 1, o), a);
        assert_eq!(
            root(&[a, b, c], 8, o),
            hash_pair(&hash_pair(&ab, &hash_pair(&c, &o)), &hash_pair(&oo, &oo))
        );
    }

    /// The root of `len` zero bytes cut into pieces of `piece_length`, as
    /// a test payload: the file's pieces, then its layer root.
    fn zero_file_root(len: u64, piece_length: u64) -> (Vec<Id32>, Id32) {
        let zeros = vec![0u8; usize::try_from(piece_length).unwrap()];
        let blocks_per_piece = piece_length / BLOCK_SIZE;
        let mut layer = Vec::new();
        let mut at = 0;
        while at < len {
            let piece = (len - at).min(piece_length);
            let blocks: Vec<Id32> = zeros[..usize::try_from(piece).unwrap()]
                .chunks(usize::try_from(BLOCK_SIZE).unwrap())
                .map(hash_block)
                .collect();
            layer.push(piece_root(&blocks, len, piece_length));
            at += piece;
        }
        let root = if layer.len() == 1 {
            layer[0]
        } else {
            layer_root(&layer, blocks_per_piece)
        };
        (layer, root)
    }

    #[test]
    fn a_file_of_one_piece_has_libtorrents_root() {
        // test_torrent_info.cpp `v2.torrent`: test64K, 65536 zero bytes in
        // one 64 KiB piece.
        let (_, root) = zero_file_root(65536, 65536);
        assert_eq!(
            root.as_string(),
            "60aae9c7b428f87e0713e88229e18f0adf12cd7b22a0dd8a92bb2485eb7af242"
        );
    }

    #[test]
    fn a_file_of_many_pieces_has_libtorrents_root() {
        // test_torrent_info.cpp `v2_only.torrent`: test1MB, 1 MiB of zero
        // bytes in 64 KiB pieces.
        let (layer, root) = zero_file_root(1 << 20, 65536);
        assert_eq!(layer.len(), 16);
        assert_eq!(
            root.as_string(),
            "515ea9181744b817744ded9d2e8e9dc6a8450c0b0c52e24b5077f302ffbd9008"
        );
        // The same root, with the piece layer built two levels up.
        let tree = PieceTree::from_piece_layer(layer, 4);
        assert_eq!(tree.root(), root);
    }

    #[test]
    fn small_files_pad_to_their_own_blocks() {
        // A file under a block is one leaf: its root is the block's hash.
        let data = b"weaver v2 under a block";
        assert_eq!(
            piece_root(&[hash_block(data)], data.len() as u64, 65536),
            hash_block(data)
        );
        // A file between a block and a piece pads to its own power of two
        // of blocks, not to the piece.
        let data = vec![7u8; 40000];
        let blocks: Vec<Id32> = data
            .chunks(usize::try_from(BLOCK_SIZE).unwrap())
            .map(hash_block)
            .collect();
        assert_eq!(blocks.len(), 3);
        let zero = Id32::default();
        let want = hash_pair(
            &hash_pair(&blocks[0], &blocks[1]),
            &hash_pair(&blocks[2], &zero),
        );
        assert_eq!(piece_root(&blocks, 40000, 65536 * 4), want);
        // A file of exactly one piece is one full piece subtree.
        assert_eq!(
            piece_root(&blocks[..2], 32768, 32768),
            hash_pair(&blocks[0], &blocks[1])
        );
    }

    #[test]
    fn the_last_piece_of_a_large_file_pads_to_the_piece() {
        let data = vec![9u8; 65536 + 20000];
        let blocks: Vec<Id32> = data
            .chunks(usize::try_from(BLOCK_SIZE).unwrap())
            .map(hash_block)
            .collect();
        let last = piece_root(&blocks[4..], data.len() as u64, 65536);
        let zero = Id32::default();
        let pad1 = hash_pair(&zero, &zero);
        assert_eq!(last, hash_pair(&hash_pair(&blocks[4], &blocks[5]), &pad1));
    }

    #[test]
    fn hashes_carry_the_uncles_that_prove_them() {
        // 5 pieces of 4 blocks: a piece layer of 8 leaves, 3 layers up.
        let layer: Vec<Id32> = (1..=5).map(h).collect();
        let tree = PieceTree::from_piece_layer(layer.clone(), 4);
        assert_eq!(tree.height(), 3);
        assert_eq!(tree.root(), layer_root(&layer, 4));

        // Two pieces from 2, proven to the root: their pair is implied, the
        // uncles are the pair beside them and the half beside that.
        let got = tree.hashes(0, 2, 2, 2).unwrap();
        assert_eq!(&got[..2], &layer[2..4]);
        assert_eq!(got.len(), 2 + 2);
        let pad = pad_hash(4, 1);
        assert_eq!(proven_root(&got[..2], 2, pad, &got[2..]), tree.root());

        // One piece alone: its sibling is the first uncle.
        let got = tree.hashes(0, 4, 1, 2).unwrap();
        assert_eq!(
            got,
            vec![layer[4], pad, hash_pair(&pad, &pad), tree.layers[2][0]]
        );
        assert_eq!(proven_root(&got[..1], 4, pad, &got[1..]), tree.root());

        // The whole layer needs no uncles; padding fills the range.
        let got = tree.hashes(0, 0, 8, 2).unwrap();
        assert_eq!(got.len(), 8);
        assert_eq!(proven_root(&got, 0, pad, &[]), tree.root());

        // Ranges and proofs that leave the tree.
        assert_eq!(tree.hashes(0, 8, 1, 0), None);
        assert_eq!(tree.hashes(0, 6, 4, 0), None);
        assert_eq!(tree.hashes(0, 0, 1, 3), None);
        assert_eq!(tree.hashes(4, 0, 1, 0), None);
    }

    #[test]
    fn a_wrong_uncle_proves_another_root() {
        let layer: Vec<Id32> = (1..=6).map(h).collect();
        let tree = PieceTree::from_piece_layer(layer, 2);
        let mut got = tree.hashes(0, 0, 2, 2).unwrap();
        let pad = pad_hash(2, 1);
        assert_eq!(proven_root(&got[..2], 0, pad, &got[2..]), tree.root());
        got[3] = h(99);
        assert_ne!(proven_root(&got[..2], 0, pad, &got[2..]), tree.root());
    }
}
