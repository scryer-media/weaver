// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The smart ban: the bytes of every block of a piece that failed its hash
//! check are remembered with their sender, and once the piece passes, the
//! sender of a block that differs from the good one is banned (libtorrent
//! `smart_ban.cpp`). Who sent each block of a piece in progress is the piece
//! tracker's record, [`Senders`], as libtorrent keeps it in the piece picker.

use std::collections::{BTreeSet, HashMap};

use crate::{
    core::lengths::{ChunkInfo, Lengths, ValidPieceIndex},
    piece_tracker::Senders,
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

/// A block of a piece that failed, as `sender` sent it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SentBlock {
    chunk: ChunkInfo,
    sender: PeerHandle,
    digest: Digest,
}

impl SentBlock {
    pub fn sender(&self) -> PeerHandle {
        self.sender
    }
}

/// The records of the peer list's peers: each names a peer on the list, and
/// leaves with it.
#[derive(Debug, Default, Clone)]
pub(crate) struct SmartBan {
    // Every block of each piece that failed and has not passed since, one
    // per block and sender (libtorrent `smart_ban_plugin::m_block_hashes`).
    failed: HashMap<ValidPieceIndex, Records>,
}

/// The blocks remembered of one piece, in the order they were first read,
/// and where each block and sender is among them.
#[derive(Debug, Default, Clone)]
struct Records {
    blocks: Vec<SentBlock>,
    at: HashMap<(u32, PeerHandle), usize>,
}

impl Records {
    fn index(&mut self) {
        self.at = self
            .blocks
            .iter()
            .enumerate()
            .map(|(i, r)| ((r.chunk.chunk_index, r.sender), i))
            .collect();
    }
}

impl SmartBan {
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
            let key = (block.chunk.chunk_index, block.sender);
            match records.at.get(&key).map(|&i| &records.blocks[i]) {
                Some(earlier) if earlier.digest != block.digest => {
                    liars.insert(block.sender);
                }
                Some(_) => {}
                None => {
                    records.at.insert(key, records.blocks.len());
                    records.blocks.push(block);
                }
            }
        }
        liars
    }

    /// The piece passed its hash check: the blocks remembered from its
    /// failures, which leave with it (libtorrent
    /// `smart_ban_plugin::on_piece_pass`).
    pub fn piece_passed(&mut self, piece: ValidPieceIndex) -> Vec<SentBlock> {
        self.failed
            .remove(&piece)
            .map(|records| records.blocks)
            .unwrap_or_default()
    }

    /// `peer` left the peer list: what it sent goes with it, and blames
    /// nobody (libtorrent `smart_ban_plugin::on_erase_peers`, called from
    /// `torrent::peers_erased`). A closed connection is no leaving: the
    /// peer stays on the list, and so do its records.
    pub fn forget(&mut self, peer: PeerHandle) {
        self.failed.retain(|_, records| {
            let had = records.blocks.len();
            records.blocks.retain(|r| r.sender != peer);
            if records.blocks.len() != had {
                records.index();
            }
            !records.blocks.is_empty()
        });
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
        .zip(senders.blocks())
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
        SocketAddr::from(([10, 0, 0, i], 6881)).into()
    }

    // The bytes of each block of piece 0, in storage.
    struct Storage(Vec<Vec<u8>>);

    impl Storage {
        fn good() -> Self {
            Self(
                (0..BLOCKS)
                    .map(|b| vec![u8::try_from(b & 0xff).unwrap(); CHUNK_SIZE as usize])
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
    fn deliver(senders: [PeerHandle; BLOCKS as usize]) -> Senders {
        senders.map(Some).into_iter().collect()
    }

    fn piece() -> ValidPieceIndex {
        lengths().validate_piece_index(0).unwrap()
    }

    #[test]
    fn at_the_pass_the_sender_of_a_block_that_differs_is_named_and_the_records_go() {
        let mut smart_ban = SmartBan::default();
        let (a, b) = (peer(1), peer(2));
        let senders = deliver([a, a, b, b]);

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

        let senders = deliver([a, a, b, b]);
        let failed = read_back(
            &lengths(),
            piece(),
            &senders,
            Storage::with_bad_block(2).read(),
        );
        assert!(smart_ban.piece_failed(piece(), failed).is_empty());

        // The same blocks from the same peers fail again, but b's block 2
        // has other bytes this time.
        let senders = deliver([a, a, b, b]);
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
            let senders = deliver([poisoner, c, c, c]);
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
    fn a_forgotten_sender_is_named_neither_at_the_failure_nor_at_the_pass() {
        let mut smart_ban = SmartBan::default();
        let (a, b) = (peer(1), peer(2));
        let senders = deliver([a, a, b, b]);
        let failed = read_back(
            &lengths(),
            piece(),
            &senders,
            Storage::with_bad_block(0).read(),
        );
        assert!(smart_ban.piece_failed(piece(), failed).is_empty());

        smart_ban.forget(a);

        // a sends its blocks again with other bytes: it is a stranger now.
        let failed = read_back(
            &lengths(),
            piece(),
            &senders,
            Storage::with_bad_block(1).read(),
        );
        assert!(smart_ban.piece_failed(piece(), failed).is_empty());
        smart_ban.forget(a);
        let records = smart_ban.piece_passed(piece());
        assert!(records.iter().all(|r| r.sender() == b));
        assert!(senders_of_bad_blocks(&records, Storage::good().read()).is_empty());

        // The last sender of a piece forgotten, the piece goes too.
        let failed = read_back(&lengths(), piece(), &senders, Storage::good().read());
        smart_ban.piece_failed(piece(), failed);
        smart_ban.forget(a);
        smart_ban.forget(b);
        assert!(smart_ban.failed.is_empty());
    }

    /// The records as they were kept before they were indexed: a list
    /// searched for each block. The reference the indexed records are
    /// compared with.
    #[derive(Default)]
    struct Listed(HashMap<ValidPieceIndex, Vec<SentBlock>>);

    impl Listed {
        fn piece_failed(
            &mut self,
            piece: ValidPieceIndex,
            blocks: impl IntoIterator<Item = SentBlock>,
        ) -> BTreeSet<PeerHandle> {
            let records = self.0.entry(piece).or_default();
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

        fn piece_passed(&mut self, piece: ValidPieceIndex) -> Vec<SentBlock> {
            self.0.remove(&piece).unwrap_or_default()
        }

        fn forget(&mut self, peer: PeerHandle) {
            self.0.retain(|_, records| {
                records.retain(|r| r.sender != peer);
                !records.is_empty()
            });
        }
    }

    // The indexed records name the same liars at each failure, and give the
    // same blocks in the same order at each pass, as the listed ones did,
    // over seeded runs of failures (some with a block sent twice in one
    // read), passes and forgotten peers.
    #[test]
    fn indexed_records_say_what_a_search_of_the_list_says() {
        let mut rng = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        const PIECES: u32 = 4;
        let lengths =
            Lengths::new(u64::from(CHUNK_SIZE * BLOCKS * PIECES), CHUNK_SIZE * BLOCKS).unwrap();
        for run in 0..40 {
            let mut indexed = SmartBan::default();
            let mut listed = Listed::default();
            for step in 0..400 {
                let r = next();
                let piece = lengths
                    .validate_piece_index(u32::try_from(r % u64::from(PIECES)).unwrap())
                    .unwrap();
                match (r >> 8) % 5 {
                    0..=2 => {
                        let chunks: Vec<ChunkInfo> = lengths.iter_chunk_infos(piece).collect();
                        let blocks: Vec<SentBlock> = (0..1 + (r >> 16) % 8)
                            .map(|_| {
                                let b = next();
                                SentBlock {
                                    chunk: chunks[(b % u64::from(BLOCKS)) as usize],
                                    sender: peer(u8::try_from(1 + (b >> 8) % 5).unwrap()),
                                    digest: [u8::try_from((b >> 16) % 3).unwrap(); 20],
                                }
                            })
                            .collect();
                        assert_eq!(
                            indexed.piece_failed(piece, blocks.clone()),
                            listed.piece_failed(piece, blocks),
                            "run {run} step {step}"
                        );
                    }
                    3 => assert_eq!(
                        indexed.piece_passed(piece),
                        listed.piece_passed(piece),
                        "run {run} step {step}"
                    ),
                    _ => {
                        let gone = peer(u8::try_from(1 + (r >> 16) % 5).unwrap());
                        indexed.forget(gone);
                        listed.forget(gone);
                    }
                }
            }
        }
    }

    #[test]
    fn a_block_of_unknown_sender_or_that_cannot_be_read_blames_nobody() {
        let mut smart_ban = SmartBan::default();
        let a = peer(1);
        let lengths = lengths();
        let senders: Senders = [None, Some(a), Some(a), Some(a)].into_iter().collect();

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
