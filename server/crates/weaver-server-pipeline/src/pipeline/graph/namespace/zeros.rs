// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Lost bytes read as zeros. A posting pads a stored member with zeros, and
// a loss there takes bytes whose value was known all along: when nothing
// can still bring them back, the member's own stated checksum says whether
// zeros were what was lost. One rule for every stored format at every
// depth, answered by what the tree already holds: the object says its
// member's bytes are its volumes' bytes as they lie (`Tree::verbatim`), the
// member's record states its CRC, and its leaf's folded runs give the CRC
// of what arrived, so the try reads nothing back and writes only the lost
// bytes, and those only when the checksum passes. A member that fails it
// keeps its loss, and with it the verdict it had.
//
// What can still mend the bytes comes first: a set of the member's own
// namespace or of any namespace above it that has not ruled, or payload
// still to come, holds the try back until it has. A member under a layer
// that transforms its bytes (compressed or enciphered) is never marked by
// a loss, as its layer's run reads what was lost and fails.

use bytes::Bytes;
use weaver_graph::{Digest, Evidence};
use weaver_server_disk::operations::disk::volume::LoopSite;

use super::*;
use crate::pipeline::graph::namespace::recovery::Coming;

// What a zero try writes from, and how far one write of it reaches.
const BLOCK: usize = 64 * 1024;
static ZEROS: [u8; BLOCK] = [0; BLOCK];

// The CRC32 of `len` zero bytes, joined by doubling from one hashed block:
// as many joins as the length has bits, whatever the length.
fn zeros_crc(len: u64) -> u64 {
    let crc = Algorithm::Crc32;
    let join = |first: u64, second: u64, len: u64| {
        crc.join(first, second, len).expect("CRC32 digests compose")
    };
    let mut digest = crc.empty().value;
    let (mut power, mut power_len) = (crc.hash(&ZEROS).value, BLOCK as u64);
    let mut blocks = len / BLOCK as u64;
    while blocks > 0 {
        if blocks & 1 == 1 {
            digest = join(digest, power, power_len);
        }
        blocks >>= 1;
        if blocks > 0 {
            power = join(power, power, power_len);
            power_len *= 2;
        }
    }
    let rest = (len % BLOCK as u64) as usize;
    join(digest, crc.hash(&ZEROS[..rest]).value, rest as u64)
}

// Why a member was not tried, or how its try went.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Try {
    // Something may still bring its bytes back: tried again later.
    NotYet,
    // Nothing about it is for zeros to answer.
    Unfit,
    Passed,
    Failed,
}

impl NamespaceWriter {
    // Whether a member a loss marked waits for its zero try.
    pub(crate) fn lost_waiting(&self) -> bool {
        !self.lost_marked.is_empty()
    }

    // Try every member a loss marked whose bytes nothing can still bring
    // back: those whose checksum passes with zeros over what was lost take
    // them, as a repair's output, and are judged again. Each member is
    // tried once; one whose namespace or a namespace above has something
    // still to come waits. Whether any member took zeros.
    pub(crate) fn fill_lost_zeros(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        coming: &Coming,
        out: &mut Vec<Effect>,
    ) -> Result<bool, WriteError> {
        // Payload still to come ends no namespace.
        if coming.payload {
            return Ok(false);
        }
        let mut filled = false;
        let marked: Vec<(NodeId, MemberId)> = self.lost_marked.iter().copied().collect();
        for (node, member) in marked {
            let (tried, gaps) = self.zero_try(tree, coming, node, member);
            if tried == Try::NotYet {
                continue;
            }
            self.lost_marked.remove(&(node, member));
            if tried == Try::Unfit {
                continue;
            }
            tracing::info!(
                node = node.0,
                member = member.0,
                passed = tried == Try::Passed,
                "lost bytes tried as zeros"
            );
            if tried == Try::Failed {
                continue;
            }
            for gap in gaps {
                self.write_zeros(tree, builder, segments, (node, member), gap, out)?;
            }
            // Its object waits for bytes that will never come, and does not
            // close it: nothing more arrives for it, so it closes now, and
            // is judged whole by its checksum as at any close.
            let open = tree
                .namespace(node)
                .and_then(|namespace| namespace.get(member))
                .is_some_and(|record| !record.closed);
            if open {
                let mut effects = Vec::new();
                tree.close(node, member, builder, &mut effects);
                self.apply_each(
                    tree,
                    builder,
                    segments,
                    effects,
                    Write::Again(LoopSite::Zeros),
                    out,
                )?;
            }
            filled = true;
        }
        Ok(filled)
    }

    // Whether the member's lost bytes may be zeros by its stated checksum,
    // and the ranges zeros would fill: every range of it nothing was
    // written to, each one lost.
    fn zero_try(
        &self,
        tree: &Tree,
        coming: &Coming,
        node: NodeId,
        member: MemberId,
    ) -> (Try, Vec<ByteRange>) {
        let mut at = node;
        loop {
            if !self.ended(tree, at, coming) {
                return (Try::NotYet, Vec::new());
            }
            match tree.volumes(at) {
                Some((parent, _)) => at = parent,
                None => break,
            }
        }
        let Some(record) = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
        else {
            return (Try::Unfit, Vec::new());
        };
        let (Some(stated), Some(length)) = (record.crc32, record.length) else {
            return (Try::Unfit, Vec::new());
        };
        let lost_only = record
            .coverage
            .runs()
            .all(|run| !run.provenance.is_damaged() || run.provenance == Provenance::Lost);
        if !record.is_complete()
            || record.coverage.is_clean()
            || !lost_only
            || tree.child_of(node, member).is_some()
            || !tree.verbatim(node, member)
        {
            return (Try::Unfit, Vec::new());
        }
        let Some(leaf) = self
            .files
            .get(&node)
            .and_then(|files| files.leaves.get(&member))
        else {
            return (Try::Unfit, Vec::new());
        };
        // The written runs' digests, with zeros' between them.
        let crc = Algorithm::Crc32;
        let mut digest = crc.empty().value;
        let mut gaps = Vec::new();
        let mut reached = 0;
        let lost = |gap: ByteRange| {
            record
                .coverage
                .overlapping(gap)
                .all(|run| run.provenance == Provenance::Lost)
        };
        let runs = leaf
            .written
            .iter()
            .map(|(start, run)| (ByteRange::new(*start, run.end.min(length)), run.digest))
            .chain(std::iter::once((ByteRange::new(length, length), None)));
        for (run, run_digest) in runs {
            if run.start > reached {
                let gap = ByteRange::new(reached, run.start);
                if !lost(gap) {
                    return (Try::Unfit, Vec::new());
                }
                digest = crc
                    .join(digest, zeros_crc(gap.len()), gap.len())
                    .expect("CRC32 digests compose");
                gaps.push(gap);
            }
            if run.is_empty() {
                continue;
            }
            let Some(Digest { value, .. }) =
                run_digest.filter(|digest| digest.algorithm == crc && run.end > reached)
            else {
                return (Try::Unfit, Vec::new());
            };
            if run.start < reached {
                return (Try::Unfit, Vec::new());
            }
            digest = crc
                .join(digest, value, run.len())
                .expect("CRC32 digests compose");
            reached = run.end;
        }
        if gaps.is_empty() {
            return (Try::Unfit, Vec::new());
        }
        match digest == u64::from(stated) {
            true => (Try::Passed, gaps),
            false => (Try::Failed, Vec::new()),
        }
    }

    // Zeros over `gap`, as a repair's output: they replace the loss there.
    fn write_zeros(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, member): (NodeId, MemberId),
        gap: ByteRange,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let mut offset = gap.start;
        while offset < gap.end {
            let len = (gap.end - offset).min(BLOCK as u64) as usize;
            let range = ByteRange::at(0, len as u64);
            let evidence = Evidence {
                range,
                digest: Digest::crc32(zeros_crc(len as u64) as u32),
            };
            let arrival = Arrival {
                node,
                member,
                offset,
                bytes: Evidenced::with(Bytes::from_static(&ZEROS[..len]), [evidence]),
                provenance: Provenance::Repaired,
            };
            self.place(
                tree,
                builder,
                segments,
                arrival,
                Write::Again(LoopSite::Zeros),
                out,
            )?;
            offset += len as u64;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_crc_of_zeros_joined_by_doubling_is_the_crc_of_the_zeros() {
        for len in [
            0,
            1,
            4095,
            BLOCK as u64,
            BLOCK as u64 + 7,
            5 * BLOCK as u64 + 3,
        ] {
            let bytes = vec![0; len as usize];
            assert_eq!(zeros_crc(len), Algorithm::Crc32.hash(&bytes).value, "{len}");
        }
    }
}
