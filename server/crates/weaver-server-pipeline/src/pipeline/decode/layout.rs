// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// What the NZB can honestly say about one segment.
//
// The NZB's `<segment bytes>` attribute is the **yEnc-encoded** size, roughly
// 3% larger than the bytes an article decodes to (measured on real fixtures:
// a 85,698,538-byte file is declared as 88,426,989). So the NZB cannot supply
// a decoded offset or a decoded size — it can only *bound* them. Treating its
// numbers as decoded truth rejects every real article, and writing at the
// offsets it implies would leave a gap between every pair of segments.
//
// These fields are therefore ceilings, not values, and only the envelope the
// NZB *can* prove — never a rejection criterion on their own, because the
// segment list can skip numbers and the byte counts can be understated:
// * `max_decoded_size` — the segment's declared size. It can only raise the
//   absolute per-article ceiling, never lower it.
// * `max_file_offset` — the encoded prefix sum up to this segment, valid only
//   when the NZB listed every segment of the file.
// * `max_file_size` — the encoded total, the same bound applied to the file.
//
// `part`/`total` identify the scheduled work for diagnostics. Poster metadata
// can be stale, so it cannot override a bounded decoded placement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExpectedSegmentLayout {
    pub max_file_offset: u64,
    pub max_decoded_size: u32,
    pub max_file_size: u64,
    pub part: u32,
    pub total: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthoritativeLayoutError {
    FileMissing,
    SegmentOutOfRange,
    ReversedSegmentBounds,
    SegmentTooLarge,
    SegmentPastFileEnd,
    InvalidPartNumber,
}

// Largest decoded payload a single article may produce.
//
// The NZB's per-segment byte count cannot serve as this bound: it is
// indexer-supplied and routinely understated, and rejecting on it abandons
// articles that decode perfectly. What is needed here is only a sanity
// ceiling — a value no ordinary post reaches — so a hostile article cannot
// claim an unbounded length. Reference decoders refuse at the same figure.
// A segment the NZB itself declares to be larger raises it rather than being
// refused by it.
pub const MAX_ARTICLE_DECODED_BYTES: u64 = 10 * 1024 * 1024;

// Largest whole-file length an article's own `=ybegin size=` may assert
// before that assertion stops being usable as a placement envelope.
pub const MAX_DECLARED_FILE_BYTES: u64 = 500 * 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum YencLayoutMismatch {
    // Decoded more bytes than any honest article can carry.
    DecodedSizeAboveCeiling,
    InvalidBegin,
    // The claimed offset is past the encoded prefix sum, i.e. further into the
    // file than this segment could possibly begin, and the article declared no
    // usable file size of its own to justify it.
    BeginAboveDeclaredPrefix,
    // The claimed range ends past every envelope that could contain it.
    EndAboveDeclaredFileSize,
    // The article declared no usable start of its own, and the ordinal before
    // it has not been placed yet, so there is nothing to lay it after. This is
    // an ordering condition, not damage: the article comes back unchanged.
    PredecessorNotPlaced,
}

#[inline]
pub fn expected_segment_layout(
    file: &crate::jobs::assembly::FileAssembly,
    segment_number: u32,
) -> Result<ExpectedSegmentLayout, AuthoritativeLayoutError> {
    let (max_file_offset, segment_end) = file
        .segment_bounds(segment_number)
        .ok_or(AuthoritativeLayoutError::SegmentOutOfRange)?;
    let segment_size = segment_end
        .checked_sub(max_file_offset)
        .ok_or(AuthoritativeLayoutError::ReversedSegmentBounds)?;
    let max_decoded_size =
        u32::try_from(segment_size).map_err(|_| AuthoritativeLayoutError::SegmentTooLarge)?;
    let max_file_size = file.total_bytes();
    if segment_end > max_file_size {
        return Err(AuthoritativeLayoutError::SegmentPastFileEnd);
    }
    let part = segment_number
        .checked_add(1)
        .ok_or(AuthoritativeLayoutError::InvalidPartNumber)?;
    let total = file.total_segments();
    if part > total {
        return Err(AuthoritativeLayoutError::InvalidPartNumber);
    }

    Ok(ExpectedSegmentLayout {
        max_file_offset,
        max_decoded_size,
        max_file_size,
        part,
        total,
    })
}

// Bound the article's own claims by what the NZB can prove, and return the
// decoded offset the segment may be written at.
//
// The offset comes from the article (`begin - 1`) because nothing else knows
// it. An article that declares no usable start is laid immediately after the
// ordinal before it (`sequential_anchor`), which is the only thing left that
// knows where its bytes go; encoded NZB prefixes cannot place multipart
// decoded bytes.
//
// The NZB envelope is a bound, not a verdict. Segment ordinals are dense, so
// an NZB that skips a segment number gives every later article a prefix sum
// below its true offset, and its `bytes` attribute can simply be understated.
// A placement is therefore accepted when it fits the NZB envelope **or** the
// article's own declared `=ybegin size=`, with absolute ceilings above both so
// neither source can claim an unbounded range.
//
// This is defence in depth, not the integrity guarantee: misplaced or corrupt
// bytes still require placement coverage and checksum/repair verification, and
// `placement_conflict` is what protects bytes already accepted.
#[inline]
pub fn validate_yenc_layout(
    expected: ExpectedSegmentLayout,
    actual: YencLayoutAssertions,
    decoded_len: usize,
    sequential_anchor: Option<u64>,
) -> Result<u64, YencLayoutMismatch> {
    // The NZB's own segment size only ever *raises* this ceiling: it is not a
    // rejection criterion, but a post that declares an article this large is
    // not making an unbounded claim either.
    if decoded_len as u64 > MAX_ARTICLE_DECODED_BYTES.max(u64::from(expected.max_decoded_size)) {
        return Err(YencLayoutMismatch::DecodedSizeAboveCeiling);
    }
    let file_offset = match actual.begin {
        Some(begin) => begin
            .checked_sub(1)
            .ok_or(YencLayoutMismatch::InvalidBegin)?,
        // An unusable `=ypart begin=` is a header defect, not a reason to
        // abandon bytes that decoded perfectly: the ordinals are ordered, so
        // the end of the part before this one is where these bytes belong.
        None => sequential_anchor.ok_or(YencLayoutMismatch::PredecessorNotPlaced)?,
    };
    let end = file_offset
        .checked_add(decoded_len as u64)
        .ok_or(YencLayoutMismatch::EndAboveDeclaredFileSize)?;
    // The article's own declared file length, when it gives one that is not
    // absurd, is the only source that knows a skipped or understated NZB
    // segment list.
    if actual.file_size != 0
        && actual.file_size <= MAX_DECLARED_FILE_BYTES
        && end <= actual.file_size
    {
        return Ok(file_offset);
    }
    if file_offset > expected.max_file_offset {
        return Err(YencLayoutMismatch::BeginAboveDeclaredPrefix);
    }
    if end > expected.max_file_size {
        return Err(YencLayoutMismatch::EndAboveDeclaredFileSize);
    }
    Ok(file_offset)
}

// Whether a placement fits the envelope the NZB's own segment sizes prove.
//
// Not a rejection criterion on its own (see [`validate_yenc_layout`]), but
// when two accepted claims contradict each other one of them is wrong, and
// the one the NZB can vouch for is the one to keep.
#[inline]
pub(super) fn placement_within_nzb_envelope(
    expected: ExpectedSegmentLayout,
    offset: u64,
    len: u32,
) -> bool {
    offset <= expected.max_file_offset
        && offset.saturating_add(u64::from(len)) <= expected.max_file_size
}

// The layout boundary this article declares for itself, when it declares a
// usable one that fits the same envelopes a placement is bounded by.
//
// This is where the *next* ordinal starts, and it is knowable even when the
// article's own bytes are damaged: the poster described the range, the decoder
// only failed to reproduce all of it. Bounding it here means a damaged part
// cannot hand the part behind it an offset further into the file than either
// the article's own declared length or the NZB envelope could contain.
#[inline]
pub(super) fn declared_part_end(
    expected: ExpectedSegmentLayout,
    actual: YencLayoutAssertions,
) -> Option<u64> {
    let begin = actual.begin?;
    let end = actual.end?;
    if end < begin {
        return None;
    }
    if actual.file_size != 0
        && actual.file_size <= MAX_DECLARED_FILE_BYTES
        && end <= actual.file_size
    {
        return Some(end);
    }
    (end <= expected.max_file_size).then_some(end)
}

#[cold]
#[inline(never)]
pub(super) fn format_authoritative_layout_error(error: AuthoritativeLayoutError) -> String {
    format!("invalid authoritative NZB segment layout: {error:?}")
}

#[cold]
#[inline(never)]
pub(super) fn format_yenc_layout_mismatch(
    mismatch: YencLayoutMismatch,
    expected: ExpectedSegmentLayout,
    actual: YencLayoutAssertions,
    decoded_len: usize,
) -> String {
    format!(
        "yEnc layout mismatch ({mismatch:?}): declared max offset={} max decoded_size={} max file_size={} part={}/{}; got decoded_size={} file_size={} begin={:?} end={:?} part={:?} total={:?}",
        expected.max_file_offset,
        expected.max_decoded_size,
        expected.max_file_size,
        expected.part,
        expected.total,
        decoded_len,
        actual.file_size,
        actual.begin,
        actual.end,
        actual.part,
        actual.total,
    )
}

// Both sides of a placement conflict: what this article's own headers claim,
// what the NZB allows for its segment, and where the segment it runs into was
// placed. Built only when an article is refused.
#[cold]
#[inline(never)]
#[expect(
    clippy::too_many_arguments,
    reason = "one cold diagnostic names every compared value"
)]
pub fn format_yenc_layout_conflict(
    segment: u32,
    offset: u64,
    len: u32,
    actual: YencLayoutAssertions,
    name: &str,
    expected: ExpectedSegmentLayout,
    other: u32,
    other_placement: Option<(u64, u32)>,
) -> String {
    let other_range = other_placement.map_or_else(
        || "an unplaced range".to_string(),
        |(other_offset, other_len)| {
            format!(
                "[{other_offset}, {})",
                other_offset.saturating_add(u64::from(other_len))
            )
        },
    );
    format!(
        "yEnc layout conflict: segment {segment} claims [{offset}, {}) which runs into segment {other} placed at {other_range}; its headers say name={name:?} size={} part={:?} total={:?} begin={:?} end={:?}; the NZB allows offset<={} size<={} part={}/{}",
        offset.saturating_add(u64::from(len)),
        actual.file_size,
        actual.part,
        actual.total,
        actual.begin,
        actual.end,
        expected.max_file_offset,
        expected.max_file_size,
        expected.part,
        expected.total,
    )
}

// A placement withdrawn because a later article the NZB vouches for
// contradicts it. The withdrawn article's own headers are not kept, so the
// line names its placement and envelope, and the headers of the article that
// contradicts it.
#[cold]
#[inline(never)]
#[expect(
    clippy::too_many_arguments,
    reason = "one cold diagnostic names every compared value"
)]
pub fn format_withdrawn_placement(
    withdrawn: u32,
    withdrawn_offset: u64,
    withdrawn_len: u32,
    withdrawn_expected: ExpectedSegmentLayout,
    contradicting: u32,
    contradicting_offset: u64,
    contradicting_len: u32,
    contradicting_actual: YencLayoutAssertions,
) -> String {
    format!(
        "yEnc layout conflict: segment {withdrawn} claims [{withdrawn_offset}, {}) past what the NZB allows for it (offset<={} size<={}), which segment {contradicting} contradicts at [{contradicting_offset}, {}) with headers size={} part={:?} total={:?} begin={:?} end={:?}",
        withdrawn_offset.saturating_add(u64::from(withdrawn_len)),
        withdrawn_expected.max_file_offset,
        withdrawn_expected.max_file_size,
        contradicting_offset.saturating_add(u64::from(contradicting_len)),
        contradicting_actual.file_size,
        contradicting_actual.part,
        contradicting_actual.total,
        contradicting_actual.begin,
        contradicting_actual.end,
    )
}
