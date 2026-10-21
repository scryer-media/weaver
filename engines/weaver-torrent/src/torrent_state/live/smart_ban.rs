// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Who sent each block of a piece, and the smart ban: the bytes of every
//! block of a piece that failed its hash check are remembered with their
//! sender, and once the piece passes, the sender of a block that differs
//! from the good one is banned (libtorrent `smart_ban.cpp`).

use std::collections::{BTreeSet, HashMap};

use crate::{
    core::lengths::{ChunkInfo, Lengths, ValidPieceIndex},
    sha1w::{ISha1, Sha1},
    type_aliases::PeerHandle,
};

type Digest = [u8; 20];

/// Reads a block back from storage: the peer that sent it, the block, and
/// a buffer of its size.
pub(crate) trait ReadBlock:
    FnMut(PeerHandle, &ChunkInfo, &mut [u8]) -> anyhow::Result<()>
{
}
impl<F: FnMut(PeerHandle, &ChunkInfo, &mut [u8]) -> anyhow::Result<()>> ReadBlock for F {}

/// The sender of every block of one piece, by block index; None for a
/// block whose sender is not known.
#[derive(Debug, Default)]
pub(crate) struct Senders(Vec<Option<PeerHandle>>);

impl Senders {
    /// The peers that sent a block of the piece, each once.
    pub fn participants(&self) -> BTreeSet<PeerHandle> {
        self.0.iter().flatten().copied().collect()
    }

    /// Whether one peer sent the whole piece. A block of unknown sender
    /// counts as sent by another (libtorrent `torrent::piece_failed`).
    pub fn sole_sender(&self) -> bool {
        self.0.iter().all(Option::is_some) && self.participants().len() == 1
    }
}

/// A block of a piece that failed, as `sender` sent it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SentBlock {
    chunk: ChunkInfo,
    sender: PeerHandle,
    digest: Digest,
}

#[derive(Debug, Default)]
pub(crate) struct SmartBan {
    // Who sent the blocks of each piece being downloaded (libtorrent keeps
    // this in the piece picker, `piece_picker::get_downloaders`).
    senders: HashMap<ValidPieceIndex, Vec<Option<PeerHandle>>>,
    // Every block of each piece that failed and has not passed since, one
    // per block and sender (libtorrent `smart_ban_plugin::m_block_hashes`).
    failed: HashMap<ValidPieceIndex, Vec<SentBlock>>,
}

impl SmartBan {
    /// `sender` delivered `chunk`, of a piece of `blocks` blocks, and the
    /// piece keeps it.
    pub fn block_received(&mut self, chunk: &ChunkInfo, sender: PeerHandle, blocks: u32) {
        let senders = self
            .senders
            .entry(chunk.piece_index)
            .or_insert_with(|| vec![None; blocks as usize]);
        if let Some(slot) = senders.get_mut(chunk.chunk_index as usize) {
            *slot = Some(sender);
        }
    }

    /// The piece has every block: who sent them, for its hash check to
    /// settle. The record leaves with them.
    pub fn take_senders(&mut self, piece: ValidPieceIndex) -> Senders {
        Senders(self.senders.remove(&piece).unwrap_or_default())
    }

    /// The piece failed its hash check, and `blocks` are its blocks as read
    /// back. Each is remembered; returns the senders that had sent a block
    /// of it before with other bytes, as one of the two was bad (libtorrent
    /// `smart_ban_plugin::on_piece_failed`, `on_read_failed_block`).
    pub fn piece_failed(
        &mut self,
        piece: ValidPieceIndex,
        blocks: impl IntoIterator<Item = SentBlock>,
    ) -> BTreeSet<PeerHandle> {
        let records = self.failed.entry(piece).or_default();
        let mut liars = BTreeSet::new();
        for block in blocks {
            match records.iter().find(|r| {
                r.chunk.chunk_index == block.chunk.chunk_index && r.sender == block.sender
            }) {
                Some(earlier) if earlier.digest != block.digest => {
                    liars.insert(block.sender);
                }
                Some(_) => {}
                None => records.push(block),
            }
        }
        liars
    }

    /// The piece passed its hash check: the blocks remembered from its
    /// failures, which leave with it (libtorrent
    /// `smart_ban_plugin::on_piece_pass`).
    pub fn piece_passed(&mut self, piece: ValidPieceIndex) -> Vec<SentBlock> {
        self.failed.remove(&piece).unwrap_or_default()
    }
}

fn digest(
    sender: PeerHandle,
    chunk: &ChunkInfo,
    buf: &mut Vec<u8>,
    read: &mut impl ReadBlock,
) -> Option<Digest> {
    buf.resize(chunk.size as usize, 0);
    // A block that cannot be read blames nobody (libtorrent ignores read
    // errors).
    read(sender, chunk, buf).ok()?;
    let mut hash = Sha1::new();
    hash.update(buf);
    Some(hash.finish())
}

/// Every block of `piece` whose sender is known, as `read` reads it back.
pub(crate) fn read_back(
    lengths: &Lengths,
    piece: ValidPieceIndex,
    senders: &Senders,
    mut read: impl ReadBlock,
) -> Vec<SentBlock> {
    let mut buf = Vec::new();
    lengths
        .iter_chunk_infos(piece)
        .zip(senders.0.iter())
        .filter_map(|(chunk, sender)| {
            let sender = (*sender)?;
            let digest = digest(sender, &chunk, &mut buf, &mut read)?;
            Some(SentBlock {
                chunk,
                sender,
                digest,
            })
        })
        .collect()
}

/// The senders of the `failed` blocks that differ from the good block now
/// in storage, which `read` reads back (libtorrent
/// `smart_ban_plugin::on_read_ok_block`).
pub(crate) fn senders_of_bad_blocks(
    failed: &[SentBlock],
    mut read: impl ReadBlock,
) -> BTreeSet<PeerHandle> {
    let mut buf = Vec::new();
    let mut good: HashMap<u32, Option<Digest>> = HashMap::new();
    let mut bad = BTreeSet::new();
    for block in failed {
        let good = *good
            .entry(block.chunk.chunk_index)
            .or_insert_with(|| digest(block.sender, &block.chunk, &mut buf, &mut read));
        if good.is_some_and(|good| good != block.digest) {
            bad.insert(block.sender);
        }
    }
    bad
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;
    use crate::core::constants::CHUNK_SIZE;

    const BLOCKS: u32 = 4;

    fn lengths() -> Lengths {
        Lengths::new(u64::from(CHUNK_SIZE * BLOCKS) * 2, CHUNK_SIZE * BLOCKS).unwrap()
    }

    fn peer(i: u8) -> PeerHandle {
        SocketAddr::from(([10, 0, 0, i], 6881))
    }

    // The bytes of each block of piece 0, in storage.
    struct Storage(Vec<Vec<u8>>);

    impl Storage {
        fn good() -> Self {
            Self(
                (0..BLOCKS)
                    .map(|b| vec![b as u8; CHUNK_SIZE as usize])
                    .collect(),
            )
        }

        fn with_bad_block(block: u32) -> Self {
            let mut storage = Self::good();
            storage.0[block as usize][0] ^= 0xff;
            storage
        }

        fn read(&self) -> impl ReadBlock + '_ {
            |_, chunk: &ChunkInfo, buf: &mut [u8]| {
                buf.copy_from_slice(&self.0[chunk.chunk_index as usize]);
                Ok(())
            }
        }
    }

    // Piece 0 delivered block by block by `senders`.
    fn deliver(smart_ban: &mut SmartBan, senders: [PeerHandle; BLOCKS as usize]) -> Senders {
        let lengths = lengths();
        let piece = lengths.validate_piece_index(0).unwrap();
        for (chunk, sender) in lengths.iter_chunk_infos(piece).zip(senders) {
            smart_ban.block_received(&chunk, sender, BLOCKS);
        }
        smart_ban.take_senders(piece)
    }

    fn piece() -> ValidPieceIndex {
        lengths().validate_piece_index(0).unwrap()
    }

    #[test]
    fn a_piece_has_a_sole_sender_only_when_one_known_peer_sent_every_block() {
        let (a, b) = (peer(1), peer(2));
        let mut smart_ban = SmartBan::default();
        let sole = deliver(&mut smart_ban, [a, a, a, a]);
        assert!(sole.sole_sender());
        assert_eq!(sole.participants(), BTreeSet::from([a]));

        let shared = deliver(&mut smart_ban, [a, b, a, a]);
        assert!(!shared.sole_sender());
        assert_eq!(shared.participants(), BTreeSet::from([a, b]));

        // A block of unknown sender may have come from another peer.
        for chunk in lengths().iter_chunk_infos(piece()).skip(1) {
            smart_ban.block_received(&chunk, a, BLOCKS);
        }
        let partly_unknown = smart_ban.take_senders(piece());
        assert!(!partly_unknown.sole_sender());
        assert_eq!(partly_unknown.participants(), BTreeSet::from([a]));
    }

    #[test]
    fn at_the_pass_the_sender_of_a_block_that_differs_is_named_and_the_records_go() {
        let mut smart_ban = SmartBan::default();
        let (a, b) = (peer(1), peer(2));
        let senders = deliver(&mut smart_ban, [a, a, b, b]);
        assert!(smart_ban.senders.is_empty());

        let bad = Storage::with_bad_block(1);
        let failed = read_back(&lengths(), piece(), &senders, bad.read());
        assert_eq!(failed.len(), BLOCKS as usize);
        assert!(smart_ban.piece_failed(piece(), failed).is_empty());

        let records = smart_ban.piece_passed(piece());
        assert_eq!(
            senders_of_bad_blocks(&records, Storage::good().read()),
            BTreeSet::from([a])
        );
        assert!(smart_ban.piece_passed(piece()).is_empty());
    }

    #[test]
    fn a_sender_that_sends_a_block_again_with_other_bytes_is_caught_at_the_failure() {
        let mut smart_ban = SmartBan::default();
        let (a, b) = (peer(1), peer(2));

        let senders = deliver(&mut smart_ban, [a, a, b, b]);
        let failed = read_back(
            &lengths(),
            piece(),
            &senders,
            Storage::with_bad_block(2).read(),
        );
        assert!(smart_ban.piece_failed(piece(), failed).is_empty());

        // The same blocks from the same peers fail again, but b's block 2
        // has other bytes this time.
        let senders = deliver(&mut smart_ban, [a, a, b, b]);
        let failed = read_back(
            &lengths(),
            piece(),
            &senders,
            Storage::with_bad_block(3).read(),
        );
        assert_eq!(smart_ban.piece_failed(piece(), failed), BTreeSet::from([b]));
    }

    #[test]
    fn every_sender_of_a_bad_copy_of_a_block_is_named() {
        let mut smart_ban = SmartBan::default();
        let (a, b, c) = (peer(1), peer(2), peer(3));
        for poisoner in [a, b] {
            let senders = deliver(&mut smart_ban, [poisoner, c, c, c]);
            let failed = read_back(
                &lengths(),
                piece(),
                &senders,
                Storage::with_bad_block(0).read(),
            );
            assert!(smart_ban.piece_failed(piece(), failed).is_empty());
        }
        let records = smart_ban.piece_passed(piece());
        assert_eq!(
            senders_of_bad_blocks(&records, Storage::good().read()),
            BTreeSet::from([a, b])
        );
    }

    #[test]
    fn a_block_of_unknown_sender_or_that_cannot_be_read_blames_nobody() {
        let mut smart_ban = SmartBan::default();
        let a = peer(1);
        let lengths = lengths();
        for chunk in lengths.iter_chunk_infos(piece()).skip(1) {
            smart_ban.block_received(&chunk, a, BLOCKS);
        }
        let senders = smart_ban.take_senders(piece());

        // Block 0 has no sender; block 1 cannot be read.
        let bad = Storage::with_bad_block(0);
        let mut read = bad.read();
        let failed = read_back(
            &lengths,
            piece(),
            &senders,
            |sender, chunk: &ChunkInfo, buf: &mut [u8]| {
                anyhow::ensure!(chunk.chunk_index != 1, "unreadable");
                read(sender, chunk, buf)
            },
        );
        assert_eq!(
            failed
                .iter()
                .map(|b| b.chunk.chunk_index)
                .collect::<Vec<_>>(),
            [2, 3]
        );
        smart_ban.piece_failed(piece(), failed);

        let records = smart_ban.piece_passed(piece());
        let unreadable = |_, _: &ChunkInfo, _: &mut [u8]| anyhow::bail!("unreadable");
        assert!(senders_of_bad_blocks(&records, unreadable).is_empty());
    }
}
