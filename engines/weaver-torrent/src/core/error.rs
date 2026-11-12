// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("expected multi-file torrent to have at least one file")]
    BadTorrentMultiFileEmpty,
    #[error("torrent has a file with no name")]
    BadTorrentFileNoName,
    #[error("torrent can't be both in single and multi-file mode")]
    BadTorrentBothSingleAndMultiFile,
    #[error("path traversal detected, \"..\" in filename")]
    BadTorrentPathTraversal,
    #[error("file path nested too deep")]
    BadTorrentPathTooDeep,
    #[error("suspicious separator in filename")]
    BadTorrentSeparatorInName,
    #[error("torrent with 0 length is useless")]
    BadTorrentZeroLength,
    #[error("invalid piece index {0}")]
    InvalidPieceIndex(u32),
    #[error("no files in torrent")]
    BadTorrentNoFiles,
    #[error("duplicate filenames in torrent")]
    BadTorrentDuplicateFilenames,
    #[error("empty filename in torrent")]
    BadTorrentEmptyFilename,

    #[error("error decoding bencode: {0}")]
    Bencode(#[from] crate::bencode::DeserializeError),

    // BEP 52 (v2), named after the libtorrent errors they mirror.
    #[error("unknown meta version {0}")]
    V2UnknownVersion(i64),
    #[error("v2 torrent has no file tree")]
    V2MissingFileTree,
    #[error("file tree without meta version 2")]
    V2MissingMetaVersion,
    #[error("invalid v2 piece length {0}: must be a power of two and at least 16 KiB")]
    V2InvalidPieceLength(u32),
    #[error("torrent with a v1 file listing has no pieces")]
    MissingPieces,
    #[error("v2 file tree lists a pad file")]
    V2InvalidPadFile,
    #[error("v2 file has an invalid length")]
    V2InvalidLength,
    #[error("v2 file has no valid pieces root")]
    V2MissingPiecesRoot,
    #[error("v1 and v2 file listings of a hybrid torrent differ")]
    V2InconsistentFiles,
    #[error("invalid piece layer")]
    V2InvalidPieceLayer,
    #[error("info dict keys out of order or with leading zeros")]
    InvalidBencoding,
    #[error("too many pieces in torrent")]
    TooManyPieces,
    // libtorrent `torrent_missing_piece_length`.
    #[error("invalid piece length {0}")]
    InvalidPieceLength(u32),
    // libtorrent `torrent_invalid_length`.
    #[error("file has an invalid length")]
    InvalidFileLength,
    // libtorrent `torrent_invalid_hashes`.
    #[error("{got} bytes of piece hashes for {expected} bytes of pieces")]
    InvalidPieceHashes { expected: usize, got: usize },
    #[error("too many 16 KiB blocks in torrent")]
    TooManyChunks,
    #[error("torrent file is too large")]
    TorrentTooLarge,
}
