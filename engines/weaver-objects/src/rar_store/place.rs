// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Where bytes are: the inverse map from a volume range to members, the
// envelope and held bytes; the forward map from a member to its volumes;
// and the posted bytes rebuilt, re-enciphering an AES file's plaintext.
//
// Up to where its headers reach, a volume in the layout is mapped by the
// layout: member data to its member, everything else envelope, numbered as
// the volume. A volume read but not yet linked holds its file data. Past
// where the headers reach, the store's own wanted ranges are envelope and
// everything else is held: only the headers can say whose it is. A locked
// file's data stays held until a password opens it, and an open one's
// padding past its length is envelope.

use unrar_rs::{MappedSlice, StoredLayoutBuilder};
use weaver_graph::{ByteRange, Extent, MemberId, Placement, VolumeIndex};

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

// A volume read but not yet linked: its file data waits for the layout, and
// everything else its headers place is envelope.
fn place_unlinked(found: &Volume, envelope: u32, range: ByteRange, out: &mut Vec<Placement>) {
    let mut blocks: Vec<ByteRange> = found
        .walk
        .blocks
        .iter()
        .filter_map(|block| block.data.intersect(range))
        .collect();
    blocks.sort_by_key(|data| data.start);
    let mut cursor = range.start;
    for data in blocks {
        if data.start > cursor {
            push(
                out,
                Placement::Envelope {
                    envelope,
                    range: ByteRange::new(cursor, data.start),
                },
            );
        }
        push(out, Placement::Held { range: data });
        cursor = cursor.max(data.end);
    }
    if cursor < range.end {
        push(
            out,
            Placement::Envelope {
                envelope,
                range: ByteRange::new(cursor, range.end),
            },
        );
    }
}

// One run of a volume range as the layout maps it.
enum Run {
    Member { id: usize, stream: ByteRange },
    Envelope,
    Held,
}

// The layout's map of a volume range, run by run, in volume order.
fn mapped(
    layout: &StoredLayoutBuilder,
    volume: VolumeIndex,
    range: ByteRange,
) -> impl Iterator<Item = (ByteRange, Run)> {
    let mut cursor = range.start;
    layout
        .map_physical_range(volume.0, range.start, range.len())
        .into_iter()
        .map(move |slice| {
            let (len, run) = match slice {
                MappedSlice::Member {
                    member_index,
                    logical_offset,
                    len,
                }
                | MappedSlice::EncryptedMember {
                    member_index,
                    logical_offset,
                    len,
                } => (
                    len,
                    Run::Member {
                        id: member_index,
                        stream: ByteRange::at(logical_offset, len),
                    },
                ),
                MappedSlice::Envelope { len } => (len, Run::Envelope),
                MappedSlice::Unroutable { len } => (len, Run::Held),
            };
            let part = ByteRange::at(cursor, len);
            cursor += len;
            (part, run)
        })
}

impl RarStore {
    pub(super) fn place(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        let envelope = volume.0;
        let Some(found) = self.volumes.get(&volume) else {
            return push(out, Placement::Held { range });
        };
        let Some(reach) = found.walk.reach() else {
            return held_or_kept(found, envelope, range, out);
        };
        if range.start < reach {
            let below = ByteRange::new(range.start, range.end.min(reach));
            match &self.layout {
                Some(layout) if layout.has_volume(volume.0) => {
                    self.place_mapped(layout, volume, below, out);
                }
                _ => place_unlinked(found, envelope, below, out),
            }
        }
        if range.end > reach {
            let above = ByteRange::new(range.start.max(reach), range.end);
            held_or_kept(found, envelope, above, out);
        }
    }

    fn place_mapped(
        &self,
        layout: &StoredLayoutBuilder,
        volume: VolumeIndex,
        range: ByteRange,
        out: &mut Vec<Placement>,
    ) {
        for (part, run) in mapped(layout, volume, range) {
            match run {
                Run::Member { id, stream } => {
                    self.place_member(volume, id, stream, part.start, out)
                }
                Run::Envelope => push(
                    out,
                    Placement::Envelope {
                        envelope: volume.0,
                        range: part,
                    },
                ),
                Run::Held => push(out, Placement::Held { range: part }),
            }
        }
    }

    // The member runs of a volume range, as the layout maps them: the
    // member, the volume range and the packed-stream range it is. Nothing
    // past where the volume's headers reach is anyone's yet.
    pub(super) fn linked(
        &self,
        volume: VolumeIndex,
        range: ByteRange,
    ) -> Vec<(usize, ByteRange, ByteRange)> {
        let (Some(layout), Some(reach)) = (
            self.layout
                .as_ref()
                .filter(|layout| layout.has_volume(volume.0)),
            self.volumes
                .get(&volume)
                .and_then(|found| found.walk.reach()),
        ) else {
            return Vec::new();
        };
        let Some(below) = range.intersect(ByteRange::new(0, reach)) else {
            return Vec::new();
        };
        mapped(layout, volume, below)
            .filter_map(|(part, run)| match run {
                Run::Member { id, stream } => Some((id, part, stream)),
                Run::Envelope | Run::Held => None,
            })
            .collect()
    }

    // A member's bytes, by what the store knows of its key.
    fn place_member(
        &self,
        volume: VolumeIndex,
        id: usize,
        stream: ByteRange,
        at: u64,
        out: &mut Vec<Placement>,
    ) {
        let part = ByteRange::at(at, stream.len());
        let Some(entry) = self.entries.get(id) else {
            return push(out, Placement::Held { range: part });
        };
        let member = MemberId(id as u32);
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
                            range: padding.shift(at as i64 - stream.start as i64),
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
        let stream = match entry.store {
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
        let mut cursor = range.start;
        for (id, part, stream) in self.linked(volume, range) {
            let Some(Lock::Open(cbc)) = self.entries.get(id).map(|entry| &entry.lock) else {
                continue;
            };
            if part.start > cursor
                && !self.read_placed(volume, ByteRange::new(cursor, part.start), read, into)
            {
                return false;
            }
            let member = MemberId(id as u32);
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
