// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Linking. Each volume's headers are read on their own, as its bytes come,
// but only archive order says what they are: a file part that continues
// from the volume before belongs to the entry that volume left open. So the
// blocks are linked one by one, volume after volume, and a volume is linked
// only once the one before it has been read to its end. Members are numbered
// in that order, which is the order every run of the same set numbers them.

use weaver_graph::{ByteRange, Emit, MemberId, RecoveryKind, VolumeIndex, Why};

use super::header::{File, What, member_name};
use super::walk::Block;
use super::{Chain, Entry, Lock, Out, Piece, Proof, RarStore};
use crate::crypt::BLOCK;
use crate::grid::Grid;

impl RarStore {
    pub(super) fn link(&mut self, out: &mut Vec<Emit>) {
        while !self.gone {
            let Chain { volume, block } = self.chain;
            let Some(found) = self.volumes.get(&volume) else {
                return;
            };
            if let Some(next) = found.walk.blocks.get(block).cloned() {
                self.chain.block += 1;
                self.stale = true;
                self.link_block(volume, block, &next, out);
                continue;
            }
            if !found.walk.ended || found.walk.is_last() {
                return;
            }
            self.chain = Chain {
                volume: VolumeIndex(volume.0 + 1),
                block: 0,
            };
        }
    }

    fn link_block(
        &mut self,
        volume: VolumeIndex,
        index: usize,
        block: &Block,
        out: &mut Vec<Emit>,
    ) {
        match &block.what {
            What::Main(main) => {
                if main.solid {
                    return self.give_up(Emit::NeedsEngine(Why::Compressed), out);
                }
                // The first volume bound is not the set's first: the bytes
                // that would name the members before it never came.
                if volume == VolumeIndex(0) && main.number.is_some_and(|number| number != 0) {
                    return self.give_up(Emit::Refuse(Why::FirstBytesLost), out);
                }
                if main.number.is_some_and(|number| number != volume.0) {
                    return self.give_up(Emit::Refuse(Why::Unsupported), out);
                }
            }
            What::File(file) => self.link_file(volume, index, block.data, file, out),
            What::Recovery => {
                if self.recovery_said.insert((volume, index)) {
                    let range = match block.data.is_empty() {
                        true => block.header,
                        false => block.data,
                    };
                    out.push(Emit::RecoveryAt {
                        volume,
                        range,
                        kind: RecoveryKind::Rar,
                    });
                }
            }
            What::Crypt(_) | What::End { .. } | What::Other => {}
        }
    }

    fn link_file(
        &mut self,
        volume: VolumeIndex,
        index: usize,
        data: ByteRange,
        file: &File,
        out: &mut Vec<Emit>,
    ) {
        if file.before {
            return self.continue_file(volume, index, data, file, out);
        }
        // A part was promised in this volume and a new file came instead.
        if self.open.is_some() {
            return self.give_up(Emit::Refuse(Why::Unsupported), out);
        }
        if !file.stored && !file.dir {
            return self.give_up(Emit::NeedsEngine(Why::Compressed), out);
        }
        if file.crypt.is_some() && file.length.is_none() {
            return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
        }
        let id = self.entries.len();
        let member = MemberId(id as u32);
        out.push(Emit::Member {
            member,
            name: member_name(&file.name, file.dir),
            length: if file.dir { Some(0) } else { file.length },
        });
        let lock = match file.crypt {
            Some(_) => Lock::Locked,
            None => Lock::Plain,
        };
        self.entries.push(Entry {
            raw: file.name.clone(),
            length: if file.dir { Some(0) } else { file.length },
            crc: None,
            crypt: file.crypt.clone(),
            pieces: Vec::new(),
            complete: false,
            lock,
            hash_key: None,
            out: Out::default(),
            grid: Grid::default(),
            parts: Grid::default(),
        });
        self.add_piece(id, volume, index, data, file, out);
    }

    fn continue_file(
        &mut self,
        volume: VolumeIndex,
        index: usize,
        data: ByteRange,
        file: &File,
        out: &mut Vec<Emit>,
    ) {
        let Some(id) = self.open.take() else {
            let why = match volume == VolumeIndex(0) {
                true => Why::FirstBytesLost,
                false => Why::Unsupported,
            };
            return self.give_up(Emit::Refuse(why), out);
        };
        if self.entries[id].raw != file.name {
            return self.give_up(Emit::Refuse(Why::Unsupported), out);
        }
        self.add_piece(id, volume, index, data, file, out);
    }

    fn add_piece(
        &mut self,
        id: usize,
        volume: VolumeIndex,
        index: usize,
        data: ByteRange,
        file: &File,
        out: &mut Vec<Emit>,
    ) {
        let entry = &mut self.entries[id];
        let at = entry.packed();
        entry.pieces.push(Piece {
            volume,
            block: index,
            data,
            at,
            crc: if file.after { file.crc } else { None },
            proof: Proof::Unknown,
        });
        self.links
            .insert((volume, index), (id, entry.pieces.len() - 1));
        if file.after {
            self.open = Some(id);
            return;
        }
        entry.complete = true;
        entry.crc = file.crc;
        // The parts must add up to what the header says the file is: its
        // length, or its length in whole cipher blocks.
        let expected = match (entry.length, &entry.crypt) {
            (Some(length), Some(_)) => Some(length.div_ceil(BLOCK) * BLOCK),
            (length, None) => length,
            (None, Some(_)) => None,
        };
        if expected.is_some_and(|expected| expected != entry.packed()) {
            return self.give_up(Emit::NeedsEngine(Why::Unsupported), out);
        }
        if entry.plain_len() == Some(0) && !entry.out.closed {
            entry.out.closed = true;
            out.push(Emit::MemberClosed(MemberId(id as u32)));
        }
    }
}
