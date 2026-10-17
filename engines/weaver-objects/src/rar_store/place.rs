// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Where bytes are: the inverse map from a volume range to members, the
// envelope and held bytes; the forward map from a member to its volumes;
// and the posted bytes rebuilt, re-enciphering an AES file's plaintext.
//
// A volume's signature, headers, service data and whatever follows its end
// header are its envelope, numbered as the volume. File data goes to the
// member its block was linked to. Past the last header read, the store's
// own wanted ranges are envelope and everything else is held: only the
// headers can say whose it is. A locked file's data stays held until a
// password opens it, and an open one's padding past its length is envelope.

use weaver_graph::{ByteRange, Extent, MemberId, Placement, VolumeIndex};

use super::header::What;
use super::walk::Block;
use super::{Lock, RarStore, Volume};
use crate::crypt::BLOCK;

fn push(out: &mut Vec<Placement>, placement: Placement) {
    if placement.is_empty() {
        return;
    }
    let merged = match (out.last_mut(), &placement) {
        (Some(Placement::Held { range: last }), Placement::Held { range })
            if last.end == range.start =>
        {
            last.end = range.end;
            true
        }
        (
            Some(Placement::Envelope {
                envelope: last_envelope,
                range: last,
            }),
            Placement::Envelope { envelope, range },
        ) if last_envelope == envelope && last.end == range.start => {
            last.end = range.end;
            true
        }
        (
            Some(Placement::Member {
                member: last_member,
                range: last,
            }),
            Placement::Member { member, range },
        ) if last_member == member && last.end == range.start => {
            last.end = range.end;
            true
        }
        _ => false,
    };
    if !merged {
        out.push(placement);
    }
}

// Bytes no header places yet: the store's own where it keeps them, held
// everywhere else.
fn held_or_kept(found: &Volume, envelope: u32, range: ByteRange, out: &mut Vec<Placement>) {
    let mut cursor = range.start;
    for wanted in found.kept.ranges_in(range) {
        if wanted.start > cursor {
            push(
                out,
                Placement::Held {
                    range: ByteRange::new(cursor, wanted.start),
                },
            );
        }
        push(
            out,
            Placement::Envelope {
                envelope,
                range: wanted,
            },
        );
        cursor = wanted.end;
    }
    if cursor < range.end {
        push(
            out,
            Placement::Held {
                range: ByteRange::new(cursor, range.end),
            },
        );
    }
}

impl RarStore {
    pub(super) fn place(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        let envelope = volume.0;
        let Some(found) = self.volumes.get(&volume) else {
            return push(out, Placement::Held { range });
        };
        let walk = &found.walk;
        let Some(format) = walk.format else {
            return held_or_kept(found, envelope, range, out);
        };
        let signature = ByteRange::new(0, format.signature_len());
        if let Some(part) = signature.intersect(range) {
            push(
                out,
                Placement::Envelope {
                    envelope,
                    range: part,
                },
            );
        }
        let first = walk
            .blocks
            .partition_point(|block| block.data.end <= range.start);
        for (index, block) in walk.blocks.iter().enumerate().skip(first) {
            if block.header.start >= range.end {
                break;
            }
            if let Some(part) = block.header.intersect(range) {
                push(
                    out,
                    Placement::Envelope {
                        envelope,
                        range: part,
                    },
                );
            }
            if let Some(part) = block.data.intersect(range) {
                self.place_data(volume, index, block, part, out);
            }
        }
        if walk.cursor >= range.end {
            return;
        }
        let rest = ByteRange::new(walk.cursor.max(range.start), range.end);
        match walk.ended {
            true => push(
                out,
                Placement::Envelope {
                    envelope,
                    range: rest,
                },
            ),
            false => held_or_kept(found, envelope, rest, out),
        }
    }

    fn place_data(
        &self,
        volume: VolumeIndex,
        index: usize,
        block: &Block,
        part: ByteRange,
        out: &mut Vec<Placement>,
    ) {
        if !matches!(block.what, What::File(_)) {
            return push(
                out,
                Placement::Envelope {
                    envelope: volume.0,
                    range: part,
                },
            );
        }
        let Some((id, piece)) = self.links.get(&(volume, index)) else {
            return push(out, Placement::Held { range: part });
        };
        let entry = &self.entries[*id];
        let member = MemberId(*id as u32);
        let piece = &entry.pieces[*piece];
        let stream = part.shift(piece.at as i64 - piece.data.start as i64);
        match &entry.lock {
            Lock::Locked => push(out, Placement::Held { range: part }),
            Lock::Plain => push(
                out,
                Placement::Member {
                    member,
                    range: stream,
                },
            ),
            Lock::Open(_) => {
                let length = entry.length.unwrap_or(0);
                let payload = ByteRange::new(stream.start.min(length), stream.end.min(length));
                push(
                    out,
                    Placement::Member {
                        member,
                        range: payload,
                    },
                );
                if stream.end > length {
                    let padding = ByteRange::new(stream.start.max(length), stream.end);
                    push(
                        out,
                        Placement::Envelope {
                            envelope: volume.0,
                            range: padding.shift(piece.data.start as i64 - piece.at as i64),
                        },
                    );
                }
            }
        }
    }

    pub(super) fn member_extent(&self, member: MemberId, range: ByteRange, out: &mut Vec<Extent>) {
        let Some(entry) = self.entries.get(member.0 as usize) else {
            return;
        };
        // A cipher block reads with the one before it as its seed.
        let stream = match entry.crypt {
            Some(_) => {
                let start = (range.start - range.start % BLOCK).saturating_sub(BLOCK);
                let end = (range.end.div_ceil(BLOCK) * BLOCK).min(entry.packed());
                ByteRange::new(start, end.max(start))
            }
            None => range,
        };
        for piece in &entry.pieces {
            let span = ByteRange::at(piece.at, piece.data.len());
            if let Some(part) = span.intersect(stream) {
                out.push(Extent {
                    volume: piece.volume,
                    range: part.shift(piece.data.start as i64 - piece.at as i64),
                });
            }
        }
    }

    pub(super) fn rebuild_posted(
        &self,
        volume: VolumeIndex,
        range: ByteRange,
        read: &mut dyn FnMut(&weaver_graph::Placement, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        let Some(found) = self.volumes.get(&volume) else {
            return self.read_placed(volume, range, read, into);
        };
        let mut cursor = range.start;
        for (index, block) in found.walk.blocks.iter().enumerate() {
            let Some(part) = block.data.intersect(range) else {
                continue;
            };
            let Some((id, piece)) = self.links.get(&(volume, index)) else {
                continue;
            };
            let entry = &self.entries[*id];
            let Lock::Open(cbc) = &entry.lock else {
                continue;
            };
            if part.start > cursor
                && !self.read_placed(volume, ByteRange::new(cursor, part.start), read, into)
            {
                return false;
            }
            let piece = &entry.pieces[*piece];
            let stream = part.shift(piece.at as i64 - piece.data.start as i64);
            let member = MemberId(*id as u32);
            let mut plain = |wanted: ByteRange, buffer: &mut Vec<u8>| -> bool {
                read(
                    &Placement::Member {
                        member,
                        range: wanted,
                    },
                    buffer,
                )
            };
            if !cbc.seal(stream, &mut plain, into) {
                return false;
            }
            cursor = part.end;
        }
        cursor >= range.end
            || self.read_placed(volume, ByteRange::new(cursor, range.end), read, into)
    }

    fn read_placed(
        &self,
        volume: VolumeIndex,
        range: ByteRange,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        let mut placements = Vec::new();
        self.place(volume, range, &mut placements);
        placements.iter().map(Placement::len).sum::<u64>() == range.len()
            && placements.iter().all(|placement| read(placement, into))
    }
}
