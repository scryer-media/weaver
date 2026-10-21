// SPDX-License-Identifier: GPL-3.0-or-later

//! v2 and hybrid torrents built from a payload, for tests. Files sit at the
//! top of the tree, named as given; each file is padded to the next piece,
//! the last one too, as libtorrent's `file_storage::add_file` lays a v2
//! torrent out and lists a hybrid's v1 files.

use crate::core::{Id32, merkle};
use crate::sha1w::{ISha1, Sha1};

/// A payload whose every byte depends on its position and `seed`.
pub fn payload(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31) ^ (i >> 8) as u8 ^ (i >> 16) as u8 ^ seed)
        .collect()
}

/// A file's root and, for a file larger than a piece, its piece layer.
pub fn file_hashes(data: &[u8], piece_length: u64) -> (Option<Id32>, Option<Vec<Id32>>) {
    if data.is_empty() {
        return (None, None);
    }
    let layer: Vec<Id32> = data
        .chunks(piece_length as usize)
        .map(|piece| {
            let blocks: Vec<Id32> = piece
                .chunks(merkle::BLOCK_SIZE as usize)
                .map(merkle::hash_block)
                .collect();
            merkle::piece_root(&blocks, data.len() as u64, piece_length)
        })
        .collect();
    match layer.len() {
        1 => (Some(layer[0]), None),
        _ => (
            Some(merkle::layer_root(
                &layer,
                piece_length / merkle::BLOCK_SIZE,
            )),
            Some(layer),
        ),
    }
}

pub fn bstr(bytes: &[u8]) -> Vec<u8> {
    let mut out = format!("{}:", bytes.len()).into_bytes();
    out.extend_from_slice(bytes);
    out
}

pub struct TestTorrent {
    /// The files, in name order, each with its payload.
    pub files: Vec<(String, Vec<u8>)>,
    pub piece_length: u64,
    /// With a v1 listing: a hybrid.
    pub hybrid: bool,
    /// With every piece layer in the torrent.
    pub layers: bool,
}

impl TestTorrent {
    pub fn new(files: Vec<(&str, Vec<u8>)>, piece_length: u64) -> Self {
        let mut files: Vec<_> = files
            .into_iter()
            .map(|(name, data)| (name.to_owned(), data))
            .collect();
        files.sort_by(|a, b| a.0.cmp(&b.0));
        Self {
            files,
            piece_length,
            hybrid: false,
            layers: true,
        }
    }

    /// The payload as the torrent lays it out: every file padded to the
    /// next piece.
    pub fn v1_payload(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for (_, data) in &self.files {
            out.extend_from_slice(data);
            out.resize(out.len().next_multiple_of(self.piece_length as usize), 0);
        }
        out
    }

    /// The piece hashes of a hybrid's v1 listing.
    pub fn v1_pieces(&self) -> Vec<u8> {
        self.v1_payload()
            .chunks(self.piece_length as usize)
            .flat_map(|piece| {
                let mut h = Sha1::new();
                h.update(piece);
                h.finish()
            })
            .collect()
    }

    pub fn info_bytes(&self) -> Vec<u8> {
        self.info_bytes_with_pieces(&self.v1_pieces())
    }

    /// The info dict, a hybrid's v1 listing given `pieces`.
    pub fn info_bytes_with_pieces(&self, pieces: &[u8]) -> Vec<u8> {
        let mut info = b"d9:file treed".to_vec();
        for (name, data) in &self.files {
            info.extend(bstr(name.as_bytes()));
            info.extend_from_slice(b"d0:d6:length");
            info.extend_from_slice(format!("i{}e", data.len()).as_bytes());
            if let (Some(root), _) = file_hashes(data, self.piece_length) {
                info.extend(bstr(b"pieces root"));
                info.extend(bstr(&root.0));
            }
            info.extend_from_slice(b"ee");
        }
        info.extend_from_slice(b"e");
        if self.hybrid {
            info.extend_from_slice(b"5:filesl");
            let mut offset = 0u64;
            for (name, data) in &self.files {
                info.extend_from_slice(format!("d6:lengthi{}e4:pathl", data.len()).as_bytes());
                info.extend(bstr(name.as_bytes()));
                info.extend_from_slice(b"ee");
                offset += data.len() as u64;
                let pad = offset.next_multiple_of(self.piece_length) - offset;
                if pad > 0 {
                    info.extend_from_slice(
                        format!("d4:attr1:p6:lengthi{pad}e4:pathl4:.pad").as_bytes(),
                    );
                    info.extend(bstr(pad.to_string().as_bytes()));
                    info.extend_from_slice(b"ee");
                    offset += pad;
                }
            }
            info.extend_from_slice(b"e");
        }
        info.extend_from_slice(b"12:meta versioni2e4:name4:test12:piece length");
        info.extend_from_slice(format!("i{}e", self.piece_length).as_bytes());
        if self.hybrid {
            info.extend(b"6:pieces".iter().copied());
            info.extend(bstr(pieces));
        }
        info.extend_from_slice(b"e");
        info
    }

    /// The piece layers of the files larger than a piece, by root.
    pub fn piece_layers(&self) -> Vec<(Id32, Vec<Id32>)> {
        let mut out: Vec<_> = self
            .files
            .iter()
            .filter_map(|(_, data)| match file_hashes(data, self.piece_length) {
                (Some(root), Some(layer)) => Some((root, layer)),
                _ => None,
            })
            .collect();
        out.sort_by(|a, b| a.0.0.cmp(&b.0.0));
        out
    }

    pub fn torrent_bytes(&self) -> Vec<u8> {
        self.torrent_bytes_with_info(&self.info_bytes())
    }

    pub fn torrent_bytes_with_info(&self, info: &[u8]) -> Vec<u8> {
        let mut out = b"d4:info".to_vec();
        out.extend_from_slice(info);
        if self.layers {
            out.extend_from_slice(b"12:piece layersd");
            for (root, layer) in self.piece_layers() {
                out.extend(bstr(&root.0));
                out.extend(bstr(&layer.iter().flat_map(|h| h.0).collect::<Vec<u8>>()));
            }
            out.extend_from_slice(b"e");
        }
        out.extend_from_slice(b"e");
        out
    }
}

/// The metadata a session holds for a torrent file.
pub fn metadata(torrent_bytes: &[u8]) -> crate::torrent_state::TorrentMetadata {
    use crate::clone_to_owned::CloneToOwned;
    let bytes = bytes::Bytes::copy_from_slice(torrent_bytes);
    let meta = crate::core::torrent_metainfo::torrent_from_bytes(&bytes)
        .unwrap()
        .clone_to_owned(Some(&bytes));
    crate::torrent_state::TorrentMetadata::from_torrent(meta, bytes).unwrap()
}
