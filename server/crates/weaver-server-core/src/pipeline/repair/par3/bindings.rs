//! Consistency of authenticated descriptions sharing an output path.

use super::*;
use par3_rs::layout::{ExtentKind, FileLayout};

impl Par3Job {
    pub(super) fn validate_shared_layouts(&mut self) -> EngineResult<()> {
        if self.sets.len() < 2 {
            return Ok(());
        }
        let _layouts = assessment::ViewReservation::acquire(self.sets.len() * 32)?;
        let mut layouts = Vec::with_capacity(self.sets.len());
        let mut count = 0usize;
        for set in self.sets.values_mut() {
            self.options.cancel.check()?;
            if let Some(layout) = set.native.layout()? {
                count = count
                    .checked_add(layout.files().len())
                    .ok_or(EngineError::ResourceLimit("PAR3 shared descriptions"))?;
                layouts.push(layout);
            }
        }
        let _files = assessment::ViewReservation::acquire(
            count
                .checked_mul(64)
                .ok_or(EngineError::ResourceLimit("PAR3 shared descriptions"))?,
        )?;
        let mut files: Vec<&FileLayout> = Vec::with_capacity(count);
        for layout in &layouts {
            files.extend(layout.files());
        }
        files.sort_unstable_by(|a, b| a.path.cmp(&b.path));
        for group in files.chunk_by(|a, b| a.path == b.path) {
            for (index, file) in group.iter().enumerate() {
                for other in &group[..index] {
                    self.options.cancel.check()?;
                    if contradict(file, other) {
                        return Err(EngineError::InvalidState(
                            "contradictory authenticated PAR3 descriptions for one output path",
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

fn contradict(left: &FileLayout, right: &FileLayout) -> bool {
    if left.len != right.len {
        return true;
    }
    // Whole fingerprints can be compared only when they describe the same
    // protected byte stream. An embedded archive omits its packet gap whereas
    // a standalone set may protect the entire carrier.
    if left.fingerprint != [0; 16]
        && right.fingerprint != [0; 16]
        && left.fingerprint != right.fingerprint
        && unprotected(left).eq(unprotected(right))
    {
        return true;
    }
    let mut left_extents = left.extents.iter().peekable();
    let mut right_extents = right.extents.iter().peekable();
    while let (Some(left), Some(right)) = (left_extents.peek(), right_extents.peek()) {
        if left.range == right.range {
            let conflict = match (&left.kind, &right.kind) {
                (
                    ExtentKind::Block {
                        fingerprint: Some(a),
                        ..
                    },
                    ExtentKind::Block {
                        fingerprint: Some(b),
                        ..
                    },
                ) => a != b,
                (ExtentKind::Inline(a), ExtentKind::Inline(b)) => a != b,
                _ => false,
            };
            if conflict {
                return true;
            }
        }
        // Fragment digests cannot be compared. Only equal complete extents
        // above can establish a checksum contradiction across codec layouts.
        let left_end = left.range.end;
        let right_end = right.range.end;
        if left_end <= right_end {
            left_extents.next();
        }
        if right_end <= left_end {
            right_extents.next();
        }
    }
    false
}

fn unprotected(file: &FileLayout) -> impl Iterator<Item = &std::ops::Range<u64>> {
    file.extents.iter().filter_map(|extent| {
        matches!(extent.kind, ExtentKind::Unprotected).then_some(&extent.range)
    })
}
