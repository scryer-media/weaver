// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use crate::bencode::raw_value::TAG as RAW_TAG;
use crate::bencode::{BencodeValue, WithRawBytes};
use crate::buffers::{ByteBuf, ByteBufOwned};
use crate::clone_to_owned::CloneToOwned;
use bytes::Bytes;
use either::Either;
use encoding_rs::Encoding;
use serde_derive::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::{HashMap, hash_map::Entry},
    iter::once,
    path::PathBuf,
};
use tracing::debug;

use crate::core::{
    Error, Id32, InfoHashes,
    hash_id::Id20,
    lengths::Lengths,
    merkle::{self, BLOCK_SIZE},
    v2_tree::{FileTree, sanitize_component},
};

pub type TorrentMetaV1Borrowed<'a> = TorrentMetaV1<ByteBuf<'a>>;
pub type TorrentMetaV1Owned = TorrentMetaV1<ByteBufOwned>;

pub struct ParsedTorrent<BufType> {
    /// The parsed torrent.
    pub meta: TorrentMetaV1<BufType>,

    /// The raw bytes of the torrent's "info" dict.
    pub info_bytes: BufType,
}

/// Parse torrent metainfo from bytes, with its info hashes. Bytes past the
/// torrent's dict are ignored, as libtorrent's `bdecode` ignores them. A v2
/// torrent's info and piece layers are checked here, as libtorrent loads
/// them (`parse_torrent_file`).
pub fn torrent_from_bytes<'de>(buf: &'de [u8]) -> crate::core::Result<TorrentMetaV1<ByteBuf<'de>>> {
    let (mut t, _): (TorrentMetaV1<ByteBuf<'_>>, _) = crate::bencode::from_bytes_with_rest(buf)
        .inspect_err(|e| tracing::trace!("error deserializing torrent: {e:#}"))
        .map_err(|e| e.into_kind())?;
    t.info_hashes = t.info.data.check_info_bytes(t.info.raw_bytes.as_ref())?;
    t.info_hash = t.info_hashes.best();
    if t.info.data.meta_version() >= 2 {
        let info = t.info.data.clone().validate()?;
        if let Some(layers) = &t.piece_layers {
            info.piece_layers(layers)?;
        }
    }
    Ok(t)
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// A parsed .torrent file.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TorrentMetaV1<BufType> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub announce: Option<BufType>,
    #[serde(
        rename = "announce-list",
        default = "Vec::new",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub announce_list: Vec<Vec<BufType>>,
    pub info: WithRawBytes<TorrentMetaV1Info<BufType>, BufType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<BufType>,
    #[serde(rename = "created by", skip_serializing_if = "Option::is_none")]
    pub created_by: Option<BufType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<BufType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publisher: Option<BufType>,
    #[serde(rename = "publisher-url", skip_serializing_if = "Option::is_none")]
    pub publisher_url: Option<BufType>,
    #[serde(rename = "creation date", skip_serializing_if = "Option::is_none")]
    pub creation_date: Option<usize>,
    /// BEP 52: each file's piece layer by its pieces root, as written.
    #[serde(
        rename = "piece layers",
        default = "none",
        skip_serializing_if = "Option::is_none"
    )]
    pub piece_layers: Option<PieceLayers<BufType>>,

    /// The hash the torrent goes by: [`InfoHashes::best`].
    #[serde(skip)]
    pub info_hash: Id20,
    #[serde(skip)]
    pub info_hashes: InfoHashes,
}

impl<BufType> TorrentMetaV1<BufType> {
    pub fn iter_announce(&self) -> impl Iterator<Item = &BufType> {
        if self.announce_list.iter().flatten().next().is_some() {
            return Either::Left(self.announce_list.iter().flatten());
        }
        Either::Right(self.announce.iter())
    }
}

/// The `piece layers` value of a torrent, kept as written. It is read only
/// for a v2 torrent and only when it is a dict, so that anything else
/// under the key loads as if it were absent (libtorrent reads it with
/// `dict_find_dict`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PieceLayers<Buf>(pub Buf);

impl<Buf: serde::Serialize> serde::Serialize for PieceLayers<Buf> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // The bytes as they were read, without a string prefix.
        serializer.serialize_newtype_struct(RAW_TAG, &self.0)
    }
}

impl<'de, Buf: serde::Deserialize<'de>> serde::Deserialize<'de> for PieceLayers<Buf> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = WithRawBytes::<serde::de::IgnoredAny, Buf>::deserialize(deserializer)?;
        Ok(PieceLayers(raw.raw_bytes))
    }
}

impl<Buf: CloneToOwned> CloneToOwned for PieceLayers<Buf> {
    type Target = PieceLayers<Buf::Target>;

    fn clone_to_owned(&self, within_buffer: Option<&Bytes>) -> Self::Target {
        PieceLayers(self.0.clone_to_owned(within_buffer))
    }
}

/// The entries of a `piece layers` dict in the order written: each key,
/// and its value when that is a string.
struct LayerEntries<'a>(Vec<(&'a [u8], Option<&'a [u8]>)>);

impl<'de> serde::Deserialize<'de> for LayerEntries<'de> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Entries;
        impl<'de> serde::de::Visitor<'de> for Entries {
            type Value = LayerEntries<'de>;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a piece layers dict")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<LayerEntries<'de>, A::Error> {
                let mut entries = Vec::new();
                while let Some(root) = map.next_key::<ByteBuf<'de>>()? {
                    let layer = match map.next_value::<BencodeValue<ByteBuf<'de>>>()? {
                        BencodeValue::Bytes(layer) => Some(layer.0),
                        _ => None,
                    };
                    entries.push((root.0, layer));
                }
                Ok(LayerEntries(entries))
            }
        }
        deserializer.deserialize_map(Entries)
    }
}

/// Main torrent information, shared by .torrent files and magnet link contents.
#[derive(Default, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct TorrentMetaV1Info<BufType> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<BufType>,
    /// SHA-1 piece hashes, present whenever the torrent has a v1 listing.
    #[serde(default = "none", skip_serializing_if = "Option::is_none")]
    pub pieces: Option<BufType>,
    #[serde(rename = "piece length")]
    pub piece_length: u32,
    /// BEP 52: 2 for a v2 or hybrid torrent.
    #[serde(
        rename = "meta version",
        default = "none",
        skip_serializing_if = "Option::is_none"
    )]
    pub meta_version: Option<i64>,
    /// BEP 52: the v2 file listing.
    #[serde(
        rename = "file tree",
        default = "none",
        skip_serializing_if = "Option::is_none"
    )]
    pub file_tree: Option<FileTree<BufType>>,

    // Single-file mode
    #[serde(skip_serializing_if = "Option::is_none")]
    pub length: Option<u64>,
    #[serde(default = "none", skip_serializing_if = "Option::is_none")]
    pub attr: Option<BufType>,
    #[serde(default = "none", skip_serializing_if = "Option::is_none")]
    pub sha1: Option<BufType>,
    #[serde(
        default = "none",
        rename = "symlink path",
        skip_serializing_if = "Option::is_none"
    )]
    pub symlink_path: Option<Vec<BufType>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub md5sum: Option<BufType>,

    // Multi-file mode
    #[serde(skip_serializing_if = "Option::is_none")]
    pub files: Option<Vec<TorrentMetaV1File<BufType>>>,

    #[serde(skip_serializing_if = "is_false", default)]
    pub private: bool,
}

#[derive(Clone, Copy)]
pub struct FileIteratorName<'a, BufType> {
    encoding: &'static Encoding,
    data: FileIteratorNameData<'a, BufType>,
}

#[derive(Clone, Copy)]
pub enum FileIteratorNameData<'a, BufType> {
    Single(Option<&'a BufType>),
    Tree(&'a [BufType]),
    /// A v2 path, its components sanitized at validation.
    V2(&'a [String]),
}

impl<BufType> std::fmt::Debug for FileIteratorName<'_, BufType>
where
    BufType: AsRef<[u8]>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
    }
}

impl<BufType> std::fmt::Display for FileIteratorName<'_, BufType>
where
    BufType: AsRef<[u8]>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (idx, bit) in self.iter_components().enumerate() {
            if idx > 0 {
                write!(f, "{}", std::path::MAIN_SEPARATOR)?;
            }
            write!(f, "{bit}")?;
        }
        Ok(())
    }
}

impl<'a, BufType> FileIteratorName<'a, BufType>
where
    BufType: AsRef<[u8]>,
{
    /// Convert path components into a vector.
    pub fn to_vec(&self) -> Vec<String> {
        self.iter_components().map(|c| c.into_owned()).collect()
    }

    /// Convert path components into a PathBuf, for use with local FS.
    pub fn to_pathbuf(&self) -> PathBuf {
        let mut buf = PathBuf::new();
        for bit in self.iter_components() {
            buf.push(&*bit)
        }
        buf
    }

    /// Iterate path components as strings. Will decode using the detected encoding, replacing
    /// unknown characters with a placeholder.
    pub fn iter_components(&self) -> impl Iterator<Item = Cow<'a, str>> + use<'a, BufType> {
        let encoding = self.encoding;
        self.iter_components_bytes()
            .map(move |part| encoding.decode(part).0)
    }

    /// Whether the name is a BitComet padding file, which carries no `attr`
    /// and is known by its name alone.
    fn is_bitcomet_padding(&self) -> bool {
        self.iter_components_bytes()
            .last()
            .is_some_and(|name| memchr::memmem::find(name, b"_____padding_file_").is_some())
    }

    /// Iterate path components as bytes.
    pub fn iter_components_bytes(&self) -> impl Iterator<Item = &'a [u8]> + use<'a, BufType> {
        let it = match self.data {
            FileIteratorNameData::Single(None) => {
                return Either::Left(once(&b"torrent-content"[..]));
            }
            FileIteratorNameData::Single(Some(name)) => {
                Either::Left(Either::Left(once((*name).as_ref())))
            }
            FileIteratorNameData::Tree(t) => {
                Either::Left(Either::Right(t.iter().map(|bb| bb.as_ref())))
            }
            FileIteratorNameData::V2(t) => Either::Right(t.iter().map(|c| c.as_bytes())),
        };
        Either::Right(it)
    }
}

#[derive(Serialize, Deserialize, Default, Debug, Clone, Copy)]
pub struct FileDetailsAttrs {
    pub symlink: bool,
    pub hidden: bool,
    pub padding: bool,
    pub executable: bool,
}

pub struct FileDetails<'a, BufType> {
    pub filename: FileIteratorName<'a, BufType>,
    pub len: u64,

    // bep-47
    attr: Option<&'a [u8]>,
    pub sha1: Option<&'a BufType>,
    pub symlink_path: Option<&'a [BufType]>,
    /// BEP 52: the root of the file's merkle tree, for a non-empty file of
    /// a v2 torrent.
    pub pieces_root: Option<Id32>,
}

impl<BufType> FileDetails<'_, BufType>
where
    BufType: AsRef<[u8]>,
{
    /// The file's BEP 47 attributes. A file is padding when its `attr` holds
    /// `p`, or when its name is BitComet's `_____padding_file_` form: either
    /// way it is all zeroes and never written. A `.pad/` path alone marks
    /// nothing.
    pub fn attrs(&self) -> FileDetailsAttrs {
        let mut result = FileDetailsAttrs {
            padding: self.filename.is_bitcomet_padding(),
            ..Default::default()
        };
        let Some(attrs) = self.attr else {
            return result;
        };
        for byte in attrs.iter().copied() {
            match byte {
                b'l' => result.symlink = true,
                b'h' => result.hidden = true,
                b'p' => result.padding = true,
                b'x' => result.executable = true,
                other => debug!(attr = other, "unknown file attribute"),
            }
        }
        result
    }
}

pub struct FileDetailsExt<'a, BufType> {
    pub details: FileDetails<'a, BufType>,
    // absolute offset in torrent if it was a flat blob of bytes
    pub offset: u64,

    // the pieces that contain this file
    pub pieces: std::ops::Range<u32>,
}

impl<BufType> FileDetailsExt<'_, BufType> {
    pub fn pieces_usize(&self) -> std::ops::Range<usize> {
        self.pieces.start as usize..self.pieces.end as usize
    }
}

/// Which hashes verify a torrent's pieces.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TorrentVersion {
    /// SHA-1 piece hashes alone.
    V1,
    /// BEP 52 merkle trees alone.
    V2,
    /// Both, over one layout.
    Hybrid,
}

/// A file of a v2 torrent's layout. Every file is followed by a pad file up
/// to the next piece boundary, as libtorrent lays a v2 torrent out, so each
/// v2 file starts a piece of its own.
#[derive(Clone, Debug)]
pub struct V2File {
    pub path: Vec<String>,
    pub length: u64,
    pub offset: u64,
    pub pieces_root: Option<Id32>,
    attr: Vec<u8>,
}

impl V2File {
    fn pad(length: u64, offset: u64) -> Self {
        V2File {
            path: vec![".pad".to_owned(), length.to_string()],
            length,
            offset,
            pieces_root: None,
            attr: b"p".to_vec(),
        }
    }

    pub fn is_pad(&self) -> bool {
        self.attr.contains(&b'p')
    }
}

/// A file may not span more 16 KiB blocks than this, nor a torrent more
/// pieces (libtorrent `file_storage::max_num_pieces`).
const MAX_NUM_PIECES: u64 = (1 << 30) - 1;
/// The largest file a v2 tree indexes (libtorrent
/// `file_storage::max_file_size`).
const MAX_FILE_SIZE: u64 = {
    let by_offset = (1u64 << 48) - 1;
    let by_leaves = (i32::MAX as u64 / 2) * BLOCK_SIZE;
    if by_offset < by_leaves {
        by_offset
    } else {
        by_leaves
    }
};
/// The end of the last byte a torrent can address (libtorrent
/// `file_storage::max_file_offset`).
const MAX_FILE_OFFSET: u64 = (1 << 48) - 1;

#[derive(Clone, Debug)]
pub struct ValidatedTorrentMetaV1Info<BufType> {
    encoding: &'static Encoding,
    lengths: Lengths,
    info: TorrentMetaV1Info<BufType>,
    /// The v2 layout, pads included, when the torrent is v2 or hybrid.
    v2: Option<Vec<V2File>>,
}

impl<BufType: AsRef<[u8]>> ValidatedTorrentMetaV1Info<BufType> {
    pub fn name(&self) -> Option<Cow<'_, str>> {
        self.info
            .name
            .as_ref()
            .map(|n| self.encoding.decode(n.as_ref()).0)
            .filter(|n| !n.is_empty())
    }

    pub fn info(&self) -> &TorrentMetaV1Info<BufType> {
        &self.info
    }

    pub fn lengths(&self) -> &Lengths {
        &self.lengths
    }

    pub fn version(&self) -> TorrentVersion {
        match (&self.v2, self.info.has_v1_listing()) {
            (None, _) => TorrentVersion::V1,
            (Some(_), false) => TorrentVersion::V2,
            (Some(_), true) => TorrentVersion::Hybrid,
        }
    }

    /// The v2 layout, pads included, in the order of
    /// [`Self::iter_file_details`].
    pub fn v2_files(&self) -> Option<&[V2File]> {
        self.v2.as_deref()
    }

    /// Whether the torrent is one file at its top, not a directory: v1
    /// with no `files`, or a v2 tree of one file.
    pub fn is_single_file(&self) -> bool {
        self.info.files.is_none()
            && self
                .info
                .file_tree
                .as_ref()
                .is_none_or(|tree| tree.single_file)
    }

    pub fn name_or_else<'a, DefaultT: Into<Cow<'a, str>>>(
        &'a self,
        default: impl Fn() -> DefaultT,
    ) -> Cow<'a, str> {
        self.name().unwrap_or_else(|| default().into())
    }

    /// Guaranteed to produce at least one file.
    pub fn iter_file_details(&self) -> impl Iterator<Item = FileDetails<'_, BufType>> {
        match &self.v2 {
            Some(files) => Either::Left(files.iter().map(|f| FileDetails {
                filename: FileIteratorName {
                    encoding: encoding_rs::UTF_8,
                    data: FileIteratorNameData::V2(&f.path),
                },
                len: f.length,
                attr: Some(&f.attr[..]).filter(|a| !a.is_empty()),
                sha1: None,
                symlink_path: None,
                pieces_root: f.pieces_root,
            })),
            // .unwrap is ok here() as we checked errors at creation time.
            None => Either::Right(self.info.iter_file_details_raw(self.encoding).unwrap()),
        }
    }

    pub fn iter_file_lengths(&self) -> impl Iterator<Item = u64> + '_ {
        self.iter_file_details().map(|d| d.len)
    }

    // Iterate file details with additional computations for offsets.
    pub fn iter_file_details_ext<'a>(
        &'a self,
    ) -> impl Iterator<Item = FileDetailsExt<'a, BufType>> + 'a {
        self.iter_file_details()
            .scan(0u64, move |acc_offset, details| {
                let offset = *acc_offset;
                *acc_offset += details.len;
                Some(FileDetailsExt {
                    pieces: self.lengths.iter_pieces_within_offset(offset, details.len),
                    details,
                    offset,
                })
            })
    }

    /// The piece layers a torrent carries, one entry per file of
    /// [`Self::iter_file_details`]: each key must be the root of a file
    /// larger than a piece, claimed once, and each layer that file's piece
    /// count of hashes, whose tree has that root. A file with no layer is allowed, and a value that
    /// is not a dict counts as no layers (libtorrent `parse_piece_layers`).
    pub fn piece_layers<B: AsRef<[u8]>>(
        &self,
        layers: &PieceLayers<B>,
    ) -> crate::core::Result<Vec<Option<Vec<Id32>>>> {
        let Some(files) = &self.v2 else {
            return Ok(Vec::new());
        };
        let mut out = vec![None; files.len()];
        let Ok(LayerEntries(entries)) = crate::bencode::from_bytes(layers.0.as_ref()) else {
            return Ok(out);
        };
        let piece_length = u64::from(self.info.piece_length);
        let mut unclaimed: HashMap<Id32, usize> = files
            .iter()
            .enumerate()
            .filter(|(_, f)| f.length > piece_length)
            .filter_map(|(index, f)| Some((f.pieces_root?, index)))
            .collect();
        for (root, layer) in entries {
            let root = <[u8; 32]>::try_from(root)
                .map(Id32::new)
                .map_err(|_| Error::V2InvalidPieceLayer)?;
            let layer = layer
                .filter(|layer| layer.len() % 32 == 0)
                .ok_or(Error::V2InvalidPieceLayer)?;
            let index = unclaimed.remove(&root).ok_or(Error::V2InvalidPieceLayer)?;
            if layer.len() as u64 != files[index].length.div_ceil(piece_length) * 32 {
                return Err(Error::V2InvalidPieceLayer);
            }
            let hashes: Vec<Id32> = layer
                .chunks_exact(32)
                .map(|h| Id32::new(h.try_into().unwrap()))
                .collect();
            if merkle::layer_root(&hashes, piece_length / BLOCK_SIZE) != root {
                return Err(Error::V2InvalidPieceLayer);
            }
            out[index] = Some(hashes);
        }
        Ok(out)
    }
}

impl<BufType: AsRef<[u8]>> TorrentMetaV1Info<BufType> {
    /// The `meta version`, -1 when absent.
    pub fn meta_version(&self) -> i64 {
        self.meta_version.unwrap_or(-1)
    }

    /// Whether the info carries a v1 listing: a v1 torrent, or a v2 one
    /// that also lists `files` or `length` (a hybrid).
    pub fn has_v1_listing(&self) -> bool {
        self.meta_version() < 2 || self.files.is_some() || self.length.is_some()
    }

    /// The info hashes of this info dict, given its bytes: SHA-1 with a v1
    /// listing, SHA-256 for meta version 2 (libtorrent
    /// `parse_info_section`).
    pub fn info_hashes(&self, info_bytes: &[u8]) -> InfoHashes {
        use crate::sha1w::{ISha1, ISha256};
        InfoHashes {
            v1: self.has_v1_listing().then(|| {
                let mut digest = crate::sha1w::Sha1::new();
                digest.update(info_bytes);
                Id20::new(digest.finish())
            }),
            v2: (self.meta_version() >= 2).then(|| {
                let mut digest = crate::sha1w::Sha256::new();
                digest.update(info_bytes);
                Id32::new(digest.finish())
            }),
        }
    }

    /// The info hashes of this info dict, given its bytes, once the bytes
    /// pass the strict reading libtorrent gives the info of any torrent
    /// with a meta version: dict keys in order and no leading zeros
    /// (`parse_info_section`, `has_soft_error`).
    pub fn check_info_bytes(&self, info_bytes: &[u8]) -> crate::core::Result<InfoHashes> {
        if self.meta_version() > 0 && crate::bencode::has_soft_error(info_bytes) {
            return Err(Error::InvalidBencoding);
        }
        Ok(self.info_hashes(info_bytes))
    }

    pub fn validate(self) -> crate::core::Result<ValidatedTorrentMetaV1Info<BufType>> {
        let version = self.meta_version();
        if version > 2 {
            return Err(Error::V2UnknownVersion(version));
        }
        let v2 = match (version >= 2, self.file_tree.as_ref()) {
            (true, Some(tree)) => Some(v2_layout(tree, self.piece_length)?),
            (true, None) => return Err(Error::V2MissingFileTree),
            (false, Some(_)) => return Err(Error::V2MissingMetaVersion),
            (false, None) => None,
        };
        if self.has_v1_listing() && self.pieces.is_none() {
            return Err(Error::MissingPieces);
        }
        let encoding = match &v2 {
            Some(_) => encoding_rs::UTF_8,
            None => self.detect_encoding(),
        };
        let v2 = match v2 {
            Some(files) if self.has_v1_listing() => {
                let v1 = self.iter_file_details_raw(encoding)?;
                Some(reconcile_hybrid(files, v1)?)
            }
            v2 => v2,
        };
        let total_length = match &v2 {
            Some(files) => files.iter().map(|f| f.length).sum(),
            None => self.iter_file_details_raw(encoding)?.map(|d| d.len).sum(),
        };
        let lengths = Lengths::new(total_length, self.piece_length)?;
        let validated = ValidatedTorrentMetaV1Info {
            encoding,
            lengths,
            info: self,
            v2,
        };

        // Ensure:
        // - there's at least one file
        // - each filename has at least one path component
        // - there's no path traversal
        // - all filenames are unique
        let mut seen_files = 0;
        for file in validated.iter_file_details() {
            seen_files += 1;
            let mut seen_a_bit = false;
            for bit in file.filename.iter_components_bytes() {
                seen_a_bit = true;
                if bit == b".." {
                    return Err(Error::BadTorrentPathTraversal);
                }
                use memchr::memchr;
                if memchr(b'/', bit).is_some() || memchr(b'\\', bit).is_some() {
                    return Err(Error::BadTorrentSeparatorInName);
                }
            }
            if !seen_a_bit {
                return Err(Error::BadTorrentFileNoName);
            }
        }
        if seen_files == 0 {
            return Err(Error::BadTorrentNoFiles);
        }

        // Every file has a name of its own, except that a padding file, which
        // is never written, may share its name with another padding file of
        // the same length. Hybrid v1+v2 torrents repeat `.pad/N` that way.
        let mut filenames = HashMap::<PathBuf, (u64, bool)>::new();
        for fd in validated.iter_file_details() {
            let pb = fd.filename.to_pathbuf();
            if pb.as_os_str().is_empty() {
                return Err(Error::BadTorrentFileNoName);
            }
            let this = (fd.len, fd.attrs().padding);
            match filenames.entry(pb) {
                Entry::Vacant(name) => {
                    name.insert(this);
                }
                Entry::Occupied(name) if *name.get() == this && this.1 => {}
                Entry::Occupied(_) => return Err(Error::BadTorrentDuplicateFilenames),
            }
        }

        Ok(validated)
    }

    pub fn get_hash(&self, piece: u32) -> Option<&[u8]> {
        let start = piece as usize * 20;
        let end = start + 20;
        let expected_hash = self.pieces.as_ref()?.as_ref().get(start..end)?;
        Some(expected_hash)
    }

    pub fn compare_hash(&self, piece: u32, hash: [u8; 20]) -> Option<bool> {
        Some(self.get_hash(piece)? == hash)
    }

    pub fn detect_encoding(&self) -> &'static Encoding {
        let mut encdetect = chardetng::EncodingDetector::new(chardetng::Iso2022JpDetection::Deny);
        if let Some(name) = self.name.as_ref() {
            encdetect.feed(name.as_ref(), false);
        }

        for file in self.files.iter().flat_map(|f| f.iter()) {
            for component in file.path.iter() {
                encdetect.feed(component.as_ref(), false);
            }
        }

        encdetect.guess(None, chardetng::Utf8Detection::Allow)
    }

    /// The v1 listing.
    pub(crate) fn iter_file_details_raw(
        &self,
        encoding: &'static Encoding,
    ) -> crate::core::Result<impl Iterator<Item = FileDetails<'_, BufType>>> {
        match (self.length, self.files.as_ref()) {
            // Single-file
            (Some(length), None) => Ok(Either::Left(once(FileDetails {
                filename: FileIteratorName {
                    encoding,
                    data: FileIteratorNameData::Single(self.name.as_ref()),
                },
                len: length,
                attr: self.attr.as_ref().map(|a| a.as_ref()),
                sha1: self.sha1.as_ref(),
                symlink_path: self.symlink_path.as_deref(),
                pieces_root: None,
            }))),

            // Multi-file
            (None, Some(files)) => {
                if files.is_empty() {
                    return Err(Error::BadTorrentMultiFileEmpty);
                }
                Ok(Either::Right(files.iter().map(move |f| FileDetails {
                    filename: FileIteratorName {
                        encoding,
                        data: FileIteratorNameData::Tree(&f.path),
                    },
                    len: f.length,
                    attr: f.attr.as_ref().map(|a| a.as_ref()),
                    sha1: f.sha1.as_ref(),
                    symlink_path: f.symlink_path.as_deref(),
                    pieces_root: None,
                })))
            }
            _ => Err(Error::BadTorrentBothSingleAndMultiFile),
        }
    }
}

/// A v2 file tree laid out over pieces: each file's rules checked, its path
/// sanitized, and a pad file after it up to the next piece boundary
/// (libtorrent `extract_single_file2`, `file_storage::add_file`).
fn v2_layout<B>(tree: &FileTree<B>, piece_length: u32) -> crate::core::Result<Vec<V2File>> {
    // BEP 52: "It must be a power of two and at least 16KiB."
    if !piece_length.is_power_of_two() || u64::from(piece_length) < BLOCK_SIZE {
        return Err(Error::V2InvalidPieceLength(piece_length));
    }
    let piece_length = u64::from(piece_length);
    let mut files = Vec::with_capacity(tree.files.len() * 2);
    let mut offset = 0u64;
    for leaf in &tree.files {
        let attr = leaf.attr.clone().unwrap_or_default();
        if attr.contains(&b'p') {
            return Err(Error::V2InvalidPadFile);
        }
        // A symlink takes no room in the payload.
        let length = match attr.contains(&b'l') {
            true => 0,
            false => leaf
                .length
                .and_then(|length| u64::try_from(length).ok())
                .filter(|&length| length / BLOCK_SIZE < MAX_NUM_PIECES && length <= MAX_FILE_SIZE)
                .ok_or(Error::V2InvalidLength)?,
        };
        let pieces_root = match length {
            0 => None,
            _ => Some(
                leaf.pieces_root
                    .as_deref()
                    .and_then(|root| <[u8; 32]>::try_from(root).ok())
                    .map(Id32::new)
                    .filter(|root| *root != Id32::default())
                    .ok_or(Error::V2MissingPiecesRoot)?,
            ),
        };
        if MAX_FILE_OFFSET - offset < length {
            return Err(Error::V2InvalidLength);
        }
        let path = leaf.path.iter().map(|c| sanitize_component(c)).collect();
        files.push(V2File {
            path,
            length,
            offset,
            pieces_root,
            attr,
        });
        offset += length;
        if offset % piece_length != 0 {
            let pad = piece_length - offset % piece_length;
            if offset > MAX_FILE_OFFSET - pad {
                return Err(Error::V2InvalidLength);
            }
            files.push(V2File::pad(pad, offset));
            offset += pad;
        }
    }
    // libtorrent `parse_info_section`: the pieces must fit the piece index.
    if offset / piece_length > MAX_NUM_PIECES {
        return Err(Error::TooManyPieces);
    }
    Ok(files)
}

/// A hybrid's v2 layout, checked against its v1 listing. Hybrids made
/// before the tail pad was written lack it in v1, so the v2 tail pad is
/// dropped to match (libtorrent `remove_tail_padding`). Then every
/// non-empty, non-pad file must match in place, length, offset and path
/// (libtorrent `files_compatible`).
fn reconcile_hybrid<'a, B: AsRef<[u8]> + 'a>(
    mut v2: Vec<V2File>,
    v1: impl Iterator<Item = FileDetails<'a, B>>,
) -> crate::core::Result<Vec<V2File>> {
    let v1: Vec<_> = v1.collect();
    if v2.len() == v1.len() + 1
        && let Some(last) = v2.iter().rposition(|f| f.length > 0)
        && v2[last].is_pad()
    {
        let pad = v2.remove(last);
        for f in &mut v2[last..] {
            f.offset = pad.offset;
        }
    }
    let v2_total: u64 = v2.iter().map(|f| f.length).sum();
    let v1_total: u64 = v1.iter().map(|f| f.len).sum();
    if v2.len() != v1.len() || v2_total != v1_total {
        return Err(Error::V2InconsistentFiles);
    }
    let mut v1_offset = 0;
    for (v2, v1) in v2.iter().zip(&v1) {
        let offset = v1_offset;
        v1_offset += v1.len;
        let v1_pad = v1.attrs().padding;
        let v2_relevant = !v2.is_pad() && v2.length > 0;
        if v2_relevant != (!v1_pad && v1.len > 0) {
            return Err(Error::V2InconsistentFiles);
        }
        if !v2_relevant {
            continue;
        }
        let same_path = v1
            .filename
            .iter_components_bytes()
            .map(sanitize_component)
            .eq(v2.path.iter().cloned());
        if v2.length != v1.len || v2.offset != offset || !same_path {
            return Err(Error::V2InconsistentFiles);
        }
    }
    Ok(v2)
}

const fn none<T>() -> Option<T> {
    None
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct TorrentMetaV1File<BufType> {
    pub length: u64,
    pub path: Vec<BufType>,

    #[serde(default = "none", skip_serializing_if = "Option::is_none")]
    pub attr: Option<BufType>,
    #[serde(default = "none", skip_serializing_if = "Option::is_none")]
    pub sha1: Option<BufType>,
    #[serde(
        default = "none",
        rename = "symlink path",
        skip_serializing_if = "Option::is_none"
    )]
    pub symlink_path: Option<Vec<BufType>>,
}

impl<BufType> CloneToOwned for TorrentMetaV1File<BufType>
where
    BufType: CloneToOwned,
{
    type Target = TorrentMetaV1File<<BufType as CloneToOwned>::Target>;

    fn clone_to_owned(&self, within_buffer: Option<&Bytes>) -> Self::Target {
        TorrentMetaV1File {
            length: self.length,
            path: self.path.clone_to_owned(within_buffer),
            attr: self.attr.clone_to_owned(within_buffer),
            sha1: self.sha1.clone_to_owned(within_buffer),
            symlink_path: self.symlink_path.clone_to_owned(within_buffer),
        }
    }
}

impl<BufType> CloneToOwned for TorrentMetaV1Info<BufType>
where
    BufType: CloneToOwned,
{
    type Target = TorrentMetaV1Info<<BufType as CloneToOwned>::Target>;

    fn clone_to_owned(&self, within_buffer: Option<&Bytes>) -> Self::Target {
        TorrentMetaV1Info {
            name: self.name.clone_to_owned(within_buffer),
            pieces: self.pieces.clone_to_owned(within_buffer),
            piece_length: self.piece_length,
            meta_version: self.meta_version,
            file_tree: self.file_tree.clone_to_owned(within_buffer),
            length: self.length,
            md5sum: self.md5sum.clone_to_owned(within_buffer),
            files: self.files.clone_to_owned(within_buffer),
            attr: self.attr.clone_to_owned(within_buffer),
            sha1: self.sha1.clone_to_owned(within_buffer),
            symlink_path: self.symlink_path.clone_to_owned(within_buffer),
            private: self.private,
        }
    }
}

impl<BufType> CloneToOwned for TorrentMetaV1<BufType>
where
    BufType: CloneToOwned,
{
    type Target = TorrentMetaV1<<BufType as CloneToOwned>::Target>;

    fn clone_to_owned(&self, within_buffer: Option<&Bytes>) -> Self::Target {
        TorrentMetaV1 {
            announce: self.announce.clone_to_owned(within_buffer),
            announce_list: self.announce_list.clone_to_owned(within_buffer),
            info: self.info.clone_to_owned(within_buffer),
            comment: self.comment.clone_to_owned(within_buffer),
            created_by: self.created_by.clone_to_owned(within_buffer),
            encoding: self.encoding.clone_to_owned(within_buffer),
            publisher: self.publisher.clone_to_owned(within_buffer),
            publisher_url: self.publisher_url.clone_to_owned(within_buffer),
            creation_date: self.creation_date,
            piece_layers: self.piece_layers.clone_to_owned(within_buffer),
            info_hash: self.info_hash,
            info_hashes: self.info_hashes,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bencode::{BencodeValue, from_bytes};

    use super::*;

    const TORRENT_BYTES: &[u8] =
        include_bytes!("../../resources/ubuntu-21.04-desktop-amd64.iso.torrent");

    #[test]
    fn test_deserialize_torrent_borrowed() {
        let torrent: TorrentMetaV1Borrowed = from_bytes(TORRENT_BYTES).unwrap();
        dbg!(torrent);
    }

    #[test]
    fn test_deserialize_torrent_with_info_hash() {
        let torrent: TorrentMetaV1Borrowed = torrent_from_bytes(TORRENT_BYTES).unwrap();
        assert_eq!(
            torrent.info_hash.as_string(),
            "64a980abe6e448226bb930ba061592e44c3781a1"
        );
    }

    #[test]
    fn test_serialize_then_deserialize_bencode() {
        let torrent = from_bytes::<TorrentMetaV1<ByteBuf>>(TORRENT_BYTES)
            .unwrap()
            .info
            .data;
        let mut writer = Vec::new();
        crate::bencode::bencode_serialize_to_writer(&torrent, &mut writer).unwrap();
        let deserialized = from_bytes::<TorrentMetaV1Info<ByteBuf>>(&writer).unwrap();

        assert_eq!(torrent, deserialized);
    }

    #[test]
    fn test_private_serialize_deserialize() {
        for private in [false, true] {
            let info: TorrentMetaV1Info<ByteBufOwned> = TorrentMetaV1Info {
                private,
                ..Default::default()
            };
            let mut buf = Vec::new();
            crate::bencode::bencode_serialize_to_writer(&info, &mut buf).unwrap();

            let deserialized = from_bytes::<TorrentMetaV1Info<ByteBuf>>(&buf).unwrap();
            assert_eq!(info.private, deserialized.private);

            let deserialized_dyn = crate::bencode::dyn_from_bytes::<ByteBuf>(&buf).unwrap();
            let hm = match deserialized_dyn {
                crate::bencode::BencodeValue::Dict(hm) => hm,
                _ => panic!("expected dict"),
            };
            match (private, hm.get(&ByteBuf(b"private"))) {
                (true, Some(BencodeValue::Integer(1))) => {}
                (false, None) => {}
                (_, v) => {
                    panic!("unexpected value for \"private\": {v:?}")
                }
            }
        }
    }

    #[test]
    fn test_private_real_torrent() {
        let buf = include_bytes!("../../resources/test/private.torrent");
        let torrent: TorrentMetaV1Borrowed = from_bytes(buf).unwrap();
        assert!(torrent.info.data.private);
    }

    fn file(path: &[&str], length: u64, attr: Option<&str>) -> TorrentMetaV1File<ByteBufOwned> {
        TorrentMetaV1File {
            length,
            path: path.iter().map(|c| c.as_bytes().into()).collect(),
            attr: attr.map(|a| a.as_bytes().into()),
            sha1: None,
            symlink_path: None,
        }
    }

    fn multi_file_info(
        files: Vec<TorrentMetaV1File<ByteBufOwned>>,
    ) -> TorrentMetaV1Info<ByteBufOwned> {
        TorrentMetaV1Info {
            name: Some(b"hybrid"[..].into()),
            piece_length: 16384,
            pieces: Some(Default::default()),
            files: Some(files),
            ..Default::default()
        }
    }

    #[test]
    fn repeated_pad_file_names_are_not_duplicates() {
        // A hybrid v1+v2 layout: every file but the last is padded to a piece
        // boundary, and the pads repeat their `.pad/N` name.
        let info = multi_file_info(vec![
            file(&["a.bin"], 100, None),
            file(&[".pad", "123"], 123, Some("p")),
            file(&["b.bin"], 100, None),
            file(&[".pad", "123"], 123, Some("p")),
            file(&["c.bin"], 100, None),
        ]);
        let validated = info.validate().unwrap();
        let padding = validated
            .iter_file_details()
            .map(|d| d.attrs().padding)
            .collect::<Vec<_>>();
        assert_eq!(padding, vec![false, true, false, true, false]);

        // A BitComet pad is known by its name alone.
        let validated = multi_file_info(vec![
            file(&["a.bin"], 100, None),
            file(&["_____padding_file_0_if you see this file, please update to BitComet 0.85 or above____"], 123, None),
            file(&["b.bin"], 100, None),
        ])
        .validate()
        .unwrap();
        let padding = validated
            .iter_file_details()
            .map(|d| d.attrs().padding)
            .collect::<Vec<_>>();
        assert_eq!(padding, vec![false, true, false]);
    }

    #[test]
    fn a_real_duplicate_name_is_refused() {
        for files in [
            vec![file(&["a.bin"], 100, None), file(&["a.bin"], 100, None)],
            // A `.pad/` path without the attribute is an ordinary file.
            vec![
                file(&[".pad", "123"], 123, None),
                file(&[".pad", "123"], 123, None),
            ],
            // A pad file never shares a name with a file that is written.
            vec![file(&["x"], 7, Some("p")), file(&["x"], 7, None)],
            // Nor with a pad file of another length.
            vec![
                file(&[".pad", "7"], 7, Some("p")),
                file(&[".pad", "7"], 8, Some("p")),
            ],
        ] {
            assert!(matches!(
                multi_file_info(files).validate(),
                Err(Error::BadTorrentDuplicateFilenames)
            ));
        }
    }

    // The fixtures below are libtorrent's `test/test_torrents/<name>.torrent`
    // (RC_2_1), copied unchanged into `resources/test/libtorrent/`. Each
    // assertion mirrors `test/test_torrent_info.cpp`: the `test_torrents`
    // table for the ones that load, `test_error_torrents` for the ones that
    // fail.
    macro_rules! fixture {
        ($name:literal) => {
            include_bytes!(concat!(
                "../../resources/test/libtorrent/",
                $name,
                ".torrent"
            ))
        };
    }

    fn load(
        bytes: &[u8],
    ) -> crate::core::Result<(
        TorrentMetaV1Borrowed<'_>,
        ValidatedTorrentMetaV1Info<ByteBuf<'_>>,
    )> {
        let meta = torrent_from_bytes(bytes)?;
        let info = meta.info.data.clone().validate()?;
        Ok((meta, info))
    }

    fn paths(info: &ValidatedTorrentMetaV1Info<ByteBuf<'_>>) -> Vec<String> {
        info.iter_file_details()
            .map(|d| d.filename.to_vec().join("/"))
            .collect()
    }

    fn root(hex: &str) -> Option<Id32> {
        Some(<Id32 as std::str::FromStr>::from_str(hex).unwrap())
    }

    #[test]
    fn a_v2_only_torrent_has_libtorrents_v2_hash_and_no_v1() {
        // libtorrent test_torrents/v2_only.torrent
        let (meta, info) = load(fixture!("v2_only")).unwrap();
        assert_eq!(meta.info_hashes.v1, None);
        assert_eq!(
            meta.info_hashes.v2.unwrap().as_string(),
            "95e04d0c4bad94ab206efa884666fd89777dbe4f7bd9945af1829037a85c6192"
        );
        assert_eq!(
            meta.info_hash,
            meta.info_hashes.v2.unwrap().truncate_for_dht()
        );
        assert_eq!(info.version(), TorrentVersion::V2);
        assert!(info.is_single_file());
        assert_eq!(paths(&info), ["test1MB"]);
        let file = info.iter_file_details().next().unwrap();
        assert_eq!(file.len, 1048576);
        assert_eq!(
            file.pieces_root,
            root("515ea9181744b817744ded9d2e8e9dc6a8450c0b0c52e24b5077f302ffbd9008")
        );
    }

    #[test]
    fn a_hybrid_torrent_has_both_of_libtorrents_hashes() {
        // libtorrent test_torrents/v2.torrent
        let (meta, info) = load(fixture!("v2")).unwrap();
        assert!(meta.info_hashes.v1.is_some());
        assert_eq!(
            meta.info_hashes.v2.unwrap().as_string(),
            "597b180c1a170a585dfc5e85d834d69013ceda174b8f357d5bb1a0ca509faf0a"
        );
        assert_eq!(meta.info_hash, meta.info_hashes.v1.unwrap());
        assert_eq!(info.version(), TorrentVersion::Hybrid);
        assert_eq!(paths(&info), ["test64K"]);
        let file = info.iter_file_details().next().unwrap();
        assert_eq!(file.len, 65536);
        assert_eq!(
            file.pieces_root,
            root("60aae9c7b428f87e0713e88229e18f0adf12cd7b22a0dd8a92bb2485eb7af242")
        );

        // libtorrent test_torrents/v2_multipiece_file.torrent
        let (meta, info) = load(fixture!("v2_multipiece_file")).unwrap();
        assert!(meta.info_hashes.v1.is_some());
        assert_eq!(
            meta.info_hashes.v2.unwrap().as_string(),
            "108ac2c3718ce722e6896edc56c4afa98f1d711ecaace7aad74fca418ebd03de"
        );
        assert_eq!(paths(&info), ["test1MB"]);
        assert_eq!(
            info.iter_file_details().next().unwrap().pieces_root,
            root("515ea9181744b817744ded9d2e8e9dc6a8450c0b0c52e24b5077f302ffbd9008")
        );
    }

    #[test]
    fn the_v1_info_hash_is_the_sha1_of_the_raw_info_bytes() {
        // libtorrent test_torrents/v2_hybrid.torrent: the v1 hash of a hybrid
        // is SHA-1 over the same info bytes the v2 hash covers.
        use crate::sha1w::{ISha1, ISha256};
        let meta = torrent_from_bytes(fixture!("v2_hybrid")).unwrap();
        let raw = meta.info.raw_bytes.as_ref();
        let mut sha1 = crate::sha1w::Sha1::new();
        sha1.update(raw);
        let mut sha256 = crate::sha1w::Sha256::new();
        sha256.update(raw);
        assert_eq!(meta.info_hashes.v1, Some(Id20::new(sha1.finish())));
        assert_eq!(meta.info_hashes.v2, Some(Id32::new(sha256.finish())));
    }

    #[test]
    fn hybrids_and_empty_files_load_with_both_hashes() {
        // libtorrent test_torrents/v2_hybrid.torrent,
        // v2_hybrid-missing-tailpad.torrent and empty-files-1..5.torrent
        for bytes in [
            &fixture!("v2_hybrid")[..],
            fixture!("v2_hybrid-missing-tailpad"),
            fixture!("empty-files-1"),
            fixture!("empty-files-2"),
            fixture!("empty-files-3"),
            fixture!("empty-files-4"),
            fixture!("empty-files-5"),
        ] {
            let (meta, info) = load(bytes).unwrap();
            assert!(meta.info_hashes.v1.is_some());
            assert!(meta.info_hashes.v2.is_some());
            assert_eq!(info.version(), TorrentVersion::Hybrid);
            let v1_lengths: Vec<u64> = info
                .info()
                .iter_file_details_raw(info.encoding)
                .unwrap()
                .map(|d| d.len)
                .collect();
            assert_eq!(
                info.lengths().total_length(),
                v1_lengths.iter().sum::<u64>()
            );
        }
    }

    #[test]
    fn a_v2_empty_file_loads_with_no_root() {
        // libtorrent test_torrents/v2_empty_file.torrent
        let (_, info) = load(fixture!("v2_empty_file")).unwrap();
        assert!(
            info.iter_file_details()
                .filter(|d| d.len == 0)
                .all(|d| d.pieces_root.is_none())
        );
        assert!(info.iter_file_details().any(|d| d.len == 0));
    }

    #[test]
    fn a_v2_file_is_padded_to_the_next_piece() {
        // libtorrent test_torrents/v2_multiple_files.torrent: five files, the
        // three listed and a pad after each that ends off a piece boundary.
        let (_, info) = load(fixture!("v2_multiple_files")).unwrap();
        assert_eq!(info.iter_file_details().count(), 5);
        let piece_length = u64::from(info.info().piece_length);
        for file in info.v2_files().unwrap().iter().filter(|f| !f.is_pad()) {
            assert_eq!(file.offset % piece_length, 0, "{:?}", file.path);
        }
        assert!(!info.is_single_file());
    }

    #[test]
    fn a_v2_name_is_sanitized_as_libtorrent_stores_it() {
        // libtorrent test_torrents/v2_invalid_filename.torrent
        let (_, info) = load(fixture!("v2_invalid_filename")).unwrap();
        assert_eq!(paths(&info), ["_estMB"]);
    }

    #[test]
    fn missing_piece_layers_load_like_a_magnet_link() {
        // libtorrent test_torrents/v2_no_piece_layers.torrent and
        // v2_incomplete_piece_layer.torrent
        for bytes in [
            &fixture!("v2_no_piece_layers")[..],
            fixture!("v2_incomplete_piece_layer"),
        ] {
            let (meta, info) = load(bytes).unwrap();
            assert_eq!(meta.info_hashes.v1, None);
            assert!(meta.info_hashes.v2.is_some());
            let layers = meta
                .piece_layers
                .as_ref()
                .map(|l| info.piece_layers(l).unwrap())
                .unwrap_or_default();
            assert!(layers.iter().any(Option::is_none) || layers.is_empty());
        }
    }

    #[test]
    fn piece_layers_attach_to_the_file_they_root() {
        // libtorrent test_torrents/v2_only.torrent: one layer of sixty-four
        // pieces for its one 1 MiB file.
        let (meta, info) = load(fixture!("v2_only")).unwrap();
        let layers = info
            .piece_layers(meta.piece_layers.as_ref().unwrap())
            .unwrap();
        let pieces = 1048576u64.div_ceil(u64::from(info.info().piece_length));
        assert_eq!(layers.len(), 1);
        assert_eq!(layers[0].as_ref().unwrap().len() as u64, pieces);
    }

    #[test]
    fn only_a_versioned_info_is_read_strictly() {
        // libtorrent parse_info_section: has_soft_error is checked when the
        // meta version is above zero.
        let info = |version: &str| {
            format!(
                "d{version}4:name1:a12:piece lengthi16384e6:lengthi1e6:pieces20:{}e",
                "x".repeat(20)
            )
        };
        let unordered = info("");
        let parsed: TorrentMetaV1Info<ByteBuf> = from_bytes(unordered.as_bytes()).unwrap();
        assert!(parsed.check_info_bytes(unordered.as_bytes()).is_ok());
        let unordered = info("12:meta versioni1e");
        let parsed: TorrentMetaV1Info<ByteBuf> = from_bytes(unordered.as_bytes()).unwrap();
        assert!(matches!(
            parsed.check_info_bytes(unordered.as_bytes()),
            Err(Error::InvalidBencoding)
        ));
    }

    #[test]
    fn piece_layers_that_are_not_a_dict_count_as_none() {
        // libtorrent load_torrent.cpp parse_torrent_file reads the key with
        // dict_find_dict. The info is libtorrent test_torrents/v2_only.torrent's.
        let meta = torrent_from_bytes(fixture!("v2_only")).unwrap();
        let raw = meta.info.raw_bytes.as_ref();
        let mut bytes = b"d4:info".to_vec();
        bytes.extend_from_slice(raw);
        bytes.extend_from_slice(b"12:piece layersi5ee");
        let (meta, info) = load(&bytes).unwrap();
        let layers = info
            .piece_layers(meta.piece_layers.as_ref().unwrap())
            .unwrap();
        assert_eq!(layers, vec![None]);
    }

    /// The pieces root of `data` and, for a file of more than one piece,
    /// its piece layer, from blocks hashed in the test.
    fn file_root(data: &[u8], piece_length: u64) -> (Option<Id32>, Option<Vec<Id32>>) {
        if data.is_empty() {
            return (None, None);
        }
        let layer: Vec<Id32> = data
            .chunks(piece_length as usize)
            .map(|piece| {
                let blocks: Vec<Id32> = piece
                    .chunks(BLOCK_SIZE as usize)
                    .map(merkle::hash_block)
                    .collect();
                merkle::piece_root(&blocks, data.len() as u64, piece_length)
            })
            .collect();
        match layer.len() {
            1 => (Some(layer[0]), None),
            _ => (
                Some(merkle::layer_root(&layer, piece_length / BLOCK_SIZE)),
                Some(layer),
            ),
        }
    }

    fn bstr(bytes: &[u8]) -> Vec<u8> {
        let mut out = format!("{}:", bytes.len()).into_bytes();
        out.extend_from_slice(bytes);
        out
    }

    #[test]
    fn a_multi_file_tree_built_here_loads_with_its_layers() {
        // The roots come from merkle.rs, which matches libtorrent's roots
        // for test_torrents/v2.torrent and v2_only.torrent.
        let piece_length = 65536u64;
        let payload = |len: usize, seed: u8| -> Vec<u8> {
            (0..len)
                .map(|i| (i as u8).wrapping_mul(31) ^ seed)
                .collect()
        };
        // Under a block, between a block and a piece, many pieces, empty,
        // exactly one piece.
        let files: Vec<(&[&str], Vec<u8>)> = vec![
            (&["a", "x"], payload(100, 1)),
            (&["a", "y"], payload(40000, 2)),
            (&["b"], payload(3 * 65536 + 5, 3)),
            (&["c"], Vec::new()),
            (&["d"], payload(65536, 4)),
        ];
        let roots: Vec<_> = files
            .iter()
            .map(|(_, data)| file_root(data, piece_length))
            .collect();
        let leaf = |index: usize| {
            let (_, data) = &files[index];
            let mut out = b"d0:d6:length".to_vec();
            out.extend_from_slice(format!("i{}e", data.len()).as_bytes());
            if let Some(root) = roots[index].0 {
                out.extend_from_slice(&bstr(b"pieces root"));
                out.extend_from_slice(&bstr(&root.0));
            }
            out.extend_from_slice(b"ee");
            out
        };
        let mut tree = b"d1:ad1:x".to_vec();
        tree.extend(leaf(0));
        tree.extend_from_slice(b"1:y");
        tree.extend(leaf(1));
        tree.extend_from_slice(b"e1:b");
        tree.extend(leaf(2));
        tree.extend_from_slice(b"1:c");
        tree.extend(leaf(3));
        tree.extend_from_slice(b"1:d");
        tree.extend(leaf(4));
        tree.extend_from_slice(b"e");
        let mut info = b"d9:file tree".to_vec();
        info.extend(tree);
        info.extend_from_slice(b"12:meta versioni2e4:name4:tree12:piece lengthi65536ee");
        let layer: Vec<u8> = roots[2]
            .1
            .as_ref()
            .unwrap()
            .iter()
            .flat_map(|h| h.0)
            .collect();
        let torrent = |layer: &[u8]| {
            let mut out = b"d4:info".to_vec();
            out.extend_from_slice(&info);
            out.extend_from_slice(b"12:piece layersd");
            out.extend(bstr(&roots[2].0.unwrap().0));
            out.extend(bstr(layer));
            out.extend_from_slice(b"ee");
            out
        };

        let bytes = torrent(&layer);
        let (meta, validated) = load(&bytes).unwrap();
        assert_eq!(validated.version(), TorrentVersion::V2);
        assert!(!validated.is_single_file());
        let real: Vec<_> = validated
            .v2_files()
            .unwrap()
            .iter()
            .filter(|f| !f.is_pad())
            .collect();
        assert_eq!(real.len(), 5);
        for ((file, (_, data)), (root, _)) in real.iter().zip(&files).zip(&roots) {
            assert_eq!(file.length, data.len() as u64);
            assert_eq!(file.pieces_root, *root);
            assert_eq!(file.offset % piece_length, 0);
        }
        let layers = validated
            .piece_layers(meta.piece_layers.as_ref().unwrap())
            .unwrap();
        let held: Vec<_> = layers.iter().map(Option::is_some).collect();
        let at_b = validated
            .v2_files()
            .unwrap()
            .iter()
            .position(|f| f.path == ["b"])
            .unwrap();
        assert!(held[at_b]);
        assert_eq!(held.iter().filter(|h| **h).count(), 1);

        // A layer that does not hash to its root is refused.
        let mut wrong = layer.clone();
        wrong[0] ^= 1;
        assert!(matches!(
            load(&torrent(&wrong)),
            Err(Error::V2InvalidPieceLayer)
        ));
    }

    #[test]
    fn libtorrents_bad_v2_torrents_are_refused() {
        // libtorrent test_torrents/<name>.torrent, each with the error
        // test_torrent_info.cpp's test_error_torrents names for it.
        macro_rules! refused {
            ($name:literal, $pattern:pat) => {{
                let err = load(fixture!($name)).err();
                assert!(matches!(err, Some($pattern)), "{}: {err:?}", $name);
            }};
        }
        // torrent_inconsistent_files
        refused!("v2_mismatching_metadata", Error::V2InconsistentFiles);
        refused!("v2_bad_file_alignment", Error::V2InconsistentFiles);
        // torrent_missing_piece_length
        refused!("v2_no_power2_piece", Error::V2InvalidPieceLength(_));
        refused!("v2_piece_size", Error::V2InvalidPieceLength(_));
        // torrent_invalid_pad_file
        refused!("v2_invalid_pad_file", Error::V2InvalidPadFile);
        // torrent_missing_pieces_root
        refused!("v2_zero_root", Error::V2MissingPiecesRoot);
        refused!("v2_zero_root_small", Error::V2MissingPiecesRoot);
        // torrent_invalid_length
        refused!("v2_large_file", Error::V2InvalidLength);
        // too_many_pieces_in_torrent
        refused!("v2_large_offset", Error::TooManyPieces);
        // invalid_bencoding: keys out of order in a versioned info dict.
        refused!("v2_unordered_files", Error::InvalidBencoding);
        // torrent_file_parse_failed, depth_exceeded: the file tree does not
        // decode.
        refused!("v2_empty_filename", Error::Bencode(_));
        refused!("v2_deep_recursion", Error::Bencode(_));
        // torrent_invalid_piece_layer
        refused!("v2_invalid_piece_layer", Error::V2InvalidPieceLayer);
        refused!("v2_invalid_piece_layer_size", Error::V2InvalidPieceLayer);
        refused!("v2_invalid_piece_layer_root", Error::V2InvalidPieceLayer);
        // This one ends in a newline past the torrent, ignored as
        // libtorrent's bdecode ignores it.
        refused!("v2_unknown_piece_layer_entry", Error::V2InvalidPieceLayer);
        refused!("v2_non_multiple_piece_layer", Error::V2InvalidPieceLayer);
        refused!(
            "v2_piece_layer_invalid_file_hash",
            Error::V2InvalidPieceLayer
        );
    }
}
