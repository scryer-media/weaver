// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Which sibling members are this set's volumes. Every volume of a RAR set
// starts with the signature, so a member without one is never claimed. A
// name in the set's own numbering is its own number. A name that numbers
// nothing of ours, an obfuscated one, is judged by its own first headers:
// a RAR5 main header says which volume it is, and it is claimed when the
// set holds the volume before it and the first file continues that
// volume's last when that one was split; RAR4 numbers volumes only in the
// end header, so there the first file must continue, by name and size, the
// file a volume of ours left split at its end.

use weaver_graph::{ByteRange, Provenance, VolumeIndex};

use super::RarStore;
use super::header::{File, Format, Signature, What, signature};
use super::walk::{Step, Walk};
use crate::engine::SetName;
use crate::kept::Kept;
use crate::rar_engine::naming;

// The last file a walk read, when it is split into the next volume.
fn tail(walk: &Walk) -> Option<&File> {
    walk.blocks
        .iter()
        .rev()
        .find_map(|block| match &block.what {
            What::File(file) => Some(file),
            _ => None,
        })
}

impl RarStore {
    pub(super) fn claim_volume(&mut self, name: &str, first_bytes: &[u8]) -> Option<VolumeIndex> {
        let Signature::Format(format) = signature(first_bytes) else {
            return None;
        };
        let ours = self.volumes.get(&VolumeIndex(0))?;
        if ours.walk.format.is_some_and(|known| known != format)
            || ours.walk.main().is_some_and(|main| !main.volume)
        {
            return None;
        }
        let wanted = naming::parse(name);
        let named = wanted.as_ref().is_some_and(|wanted| {
            self.volumes
                .values()
                .filter_map(|volume| volume.name.as_deref().and_then(naming::parse))
                .any(|bound| bound.same_set(wanted))
        });
        let index = match (named, wanted) {
            (true, Some(wanted)) => VolumeIndex(wanted.index()),
            _ => self.claim_by_headers(format, first_bytes)?,
        };
        let taken = self
            .volumes
            .get(&index)
            .is_some_and(|volume| volume.name.is_some());
        (!taken).then_some(index)
    }

    fn claim_by_headers(&self, format: Format, first_bytes: &[u8]) -> Option<VolumeIndex> {
        let walk = self.first_headers(first_bytes);
        let main = walk.main().filter(|main| main.volume)?;
        let first = walk.blocks.iter().find_map(|block| match &block.what {
            What::File(file) => Some(file),
            _ => None,
        });
        match (format, main.number) {
            (Format::Rar5, Some(number)) if number > 0 => {
                let previous = self.volumes.get(&VolumeIndex(number - 1))?;
                previous.name.as_ref()?;
                let continues = match (previous.walk.ended, tail(&previous.walk), first) {
                    (false, _, _) | (true, _, None) => true,
                    (true, Some(tail), Some(first)) if tail.after => {
                        first.before && first.name == tail.name
                    }
                    (true, _, Some(first)) => !first.before,
                };
                continues.then_some(VolumeIndex(number))
            }
            (Format::Rar4, _) => {
                let first = first.filter(|first| first.before)?;
                let mut found = self.volumes.iter().filter(|(index, volume)| {
                    let next = VolumeIndex(index.0 + 1);
                    volume.walk.ended
                        && !volume.walk.is_last()
                        && self
                            .volumes
                            .get(&next)
                            .is_none_or(|next| next.name.is_none())
                        && tail(&volume.walk).is_some_and(|tail| {
                            tail.after && tail.name == first.name && tail.length == first.length
                        })
                });
                let (index, _) = found.next()?;
                // Two volumes it could continue: no telling which.
                found.next().is_none().then_some(VolumeIndex(index.0 + 1))
            }
            _ => None,
        }
    }

    // The headers a member's first bytes hold, read as a volume of ours.
    fn first_headers(&self, first_bytes: &[u8]) -> Walk {
        let mut kept = Kept::default();
        let all = ByteRange::new(0, first_bytes.len() as u64);
        kept.want(all);
        kept.absorb(all, first_bytes, Provenance::Fresh);
        let keyring = self.header_keyring();
        let mut walk = Walk::default();
        while let Step::Moved = walk.step(&mut kept, None, keyring.as_deref()) {
            let read_a_file = walk
                .blocks
                .iter()
                .any(|block| matches!(block.what, What::File(_)));
            if read_a_file || walk.ended {
                break;
            }
        }
        walk
    }
}
