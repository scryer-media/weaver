use super::*;

#[cfg(test)]
#[path = "layout_compatibility_tests.rs"]
mod compatibility_tests;

/// What the NZB can honestly say about one segment.
///
/// The NZB's `<segment bytes>` attribute is the **yEnc-encoded** size, roughly
/// 3% larger than the bytes an article decodes to (measured on real fixtures:
/// a 85,698,538-byte file is declared as 88,426,989). So the NZB cannot supply
/// a decoded offset or a decoded size — it can only *bound* them. Treating its
/// numbers as decoded truth rejects every real article, and writing at the
/// offsets it implies would leave a gap between every pair of segments.
///
/// These fields are therefore ceilings, not values, and only the envelope the
/// NZB *can* prove — never a rejection criterion on their own, because the
/// segment list can skip numbers and the byte counts can be understated:
/// * `max_decoded_size` — the segment's declared size. It can only raise the
///   absolute per-article ceiling, never lower it.
/// * `max_file_offset` — the encoded prefix sum up to this segment, valid only
///   when the NZB listed every segment of the file.
/// * `max_file_size` — the encoded total, the same bound applied to the file.
///
/// `part`/`total` identify the scheduled work for diagnostics. Poster metadata
/// can be stale, so it cannot override a bounded decoded placement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ExpectedSegmentLayout {
    pub(super) max_file_offset: u64,
    pub(super) max_decoded_size: u32,
    pub(super) max_file_size: u64,
    pub(super) part: u32,
    pub(super) total: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AuthoritativeLayoutError {
    FileMissing,
    SegmentOutOfRange,
    ReversedSegmentBounds,
    SegmentTooLarge,
    SegmentPastFileEnd,
    InvalidPartNumber,
}

/// Largest decoded payload a single article may produce.
///
/// The NZB's per-segment byte count cannot serve as this bound: it is
/// indexer-supplied and routinely understated, and rejecting on it abandons
/// articles that decode perfectly. What is needed here is only a sanity
/// ceiling — a value no ordinary post reaches — so a hostile article cannot
/// claim an unbounded length. Reference decoders refuse at the same figure.
/// A segment the NZB itself declares to be larger raises it rather than being
/// refused by it.
pub(super) const MAX_ARTICLE_DECODED_BYTES: u64 = 10 * 1024 * 1024;

/// Largest whole-file length an article's own `=ybegin size=` may assert
/// before that assertion stops being usable as a placement envelope.
pub(super) const MAX_DECLARED_FILE_BYTES: u64 = 500 * 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::pipeline) enum YencLayoutMismatch {
    /// Decoded more bytes than any honest article can carry.
    DecodedSizeAboveCeiling,
    InvalidBegin,
    /// The claimed offset is past the encoded prefix sum, i.e. further into the
    /// file than this segment could possibly begin, and the article declared no
    /// usable file size of its own to justify it.
    BeginAboveDeclaredPrefix,
    /// The claimed range ends past every envelope that could contain it.
    EndAboveDeclaredFileSize,
    /// The article declared no usable start of its own, and the ordinal before
    /// it has not been placed yet, so there is nothing to lay it after. This is
    /// an ordering condition, not damage: the article comes back unchanged.
    PredecessorNotPlaced,
}

#[inline]
pub(super) fn expected_segment_layout(
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

/// Bound the article's own claims by what the NZB can prove, and return the
/// decoded offset the segment may be written at.
///
/// The offset comes from the article (`begin - 1`) because nothing else knows
/// it. An article that declares no usable start is laid immediately after the
/// ordinal before it (`sequential_anchor`), which is the only thing left that
/// knows where its bytes go; encoded NZB prefixes cannot place multipart
/// decoded bytes.
///
/// The NZB envelope is a bound, not a verdict. Segment ordinals are dense, so
/// an NZB that skips a segment number gives every later article a prefix sum
/// below its true offset, and its `bytes` attribute can simply be understated.
/// A placement is therefore accepted when it fits the NZB envelope **or** the
/// article's own declared `=ybegin size=`, with absolute ceilings above both so
/// neither source can claim an unbounded range.
///
/// This is defence in depth, not the integrity guarantee: misplaced or corrupt
/// bytes still require placement coverage and checksum/repair verification, and
/// `placement_conflict` is what protects bytes already accepted.
#[inline]
pub(super) fn validate_yenc_layout(
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

/// The layout boundary this article declares for itself, when it declares a
/// usable one that fits the same envelopes a placement is bounded by.
///
/// This is where the *next* ordinal starts, and it is knowable even when the
/// article's own bytes are damaged: the poster described the range, the decoder
/// only failed to reproduce all of it. Bounding it here means a damaged part
/// cannot hand the part behind it an offset further into the file than either
/// the article's own declared length or the NZB envelope could contain.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::ids::{JobId, NzbFileId};
    use weaver_model::files::FileRole;

    fn assembly(segment_sizes: &[u32]) -> crate::jobs::assembly::FileAssembly {
        crate::jobs::assembly::FileAssembly::new(
            NzbFileId {
                job_id: JobId(1),
                file_index: 0,
            },
            "test.bin".to_string(),
            FileRole::Unknown,
            segment_sizes.to_vec(),
        )
    }

    /// A header that claims exactly the declared ceiling — the shape a fixture
    /// produces when it declares decoded sizes rather than encoded ones.
    fn assertions(expected: ExpectedSegmentLayout) -> YencLayoutAssertions {
        YencLayoutAssertions {
            file_size: expected.max_file_size,
            part: Some(expected.part),
            total: Some(expected.total),
            begin: Some(expected.max_file_offset + 1),
            end: Some(expected.max_file_offset + u64::from(expected.max_decoded_size)),
        }
    }

    #[test]
    fn derives_trusted_out_of_order_segment_layout() {
        let file = assembly(&[4, 7, 3]);
        assert_eq!(
            expected_segment_layout(&file, 1),
            Ok(ExpectedSegmentLayout {
                max_file_offset: 4,
                max_decoded_size: 7,
                max_file_size: 14,
                part: 2,
                total: 3,
            })
        );
    }

    #[test]
    fn accepts_matching_and_missing_optional_yenc_assertions() {
        let file = assembly(&[4, 7]);
        let expected = expected_segment_layout(&file, 1).unwrap();
        assert_eq!(
            validate_yenc_layout(expected, assertions(expected), 7, None),
            Ok(4)
        );
        assert_eq!(
            validate_yenc_layout(
                expected,
                YencLayoutAssertions {
                    file_size: 11,
                    part: None,
                    total: None,
                    begin: Some(5),
                    end: None,
                },
                7,
                None,
            ),
            Ok(4)
        );
    }

    /// The case the equality contract could not express: a real NZB declares
    /// yEnc-*encoded* sizes, so every article decodes to fewer bytes than its
    /// segment claims and lands at a lower offset than the declared prefix sum.
    /// Measured on a real fixture: 88,426,989 declared for 85,698,538 true.
    #[test]
    fn accepts_a_real_article_that_decodes_smaller_than_its_declared_size() {
        // ~3% yEnc overhead on two 1000-byte payloads.
        let file = assembly(&[1032, 1032]);
        let expected = expected_segment_layout(&file, 1).unwrap();
        // Segment 1's true offset is 1000, not the declared prefix sum of 1032.
        assert_eq!(
            validate_yenc_layout(
                expected,
                YencLayoutAssertions {
                    file_size: 2000,
                    part: Some(2),
                    total: Some(2),
                    begin: Some(1001),
                    end: Some(2000),
                },
                1000,
                None,
            ),
            Ok(1000)
        );
    }

    #[test]
    fn rejects_each_untrusted_layout_mismatch() {
        let file = assembly(&[4, 7]);
        let expected = expected_segment_layout(&file, 1).unwrap();
        let valid = assertions(expected);
        let cases = [
            // No usable start, and nothing placed to lay these bytes after.
            (
                YencLayoutAssertions {
                    begin: None,
                    ..valid
                },
                7,
                YencLayoutMismatch::PredecessorNotPlaced,
            ),
            // One-based offsets: zero is not a position in a file.
            (
                YencLayoutAssertions {
                    begin: Some(0),
                    ..valid
                },
                7,
                YencLayoutMismatch::InvalidBegin,
            ),
            // Placing the segment past its declared prefix sum: the attack the
            // bound exists to stop.
            (
                YencLayoutAssertions {
                    begin: Some(6),
                    end: Some(12),
                    ..valid
                },
                7,
                YencLayoutMismatch::BeginAboveDeclaredPrefix,
            ),
            // Beyond every envelope: no declared file size can rescue it and
            // the NZB total is exceeded.
            (
                YencLayoutAssertions {
                    file_size: 0,
                    ..valid
                },
                8,
                YencLayoutMismatch::EndAboveDeclaredFileSize,
            ),
            // No honest article decodes past the absolute per-article ceiling.
            (
                YencLayoutAssertions {
                    file_size: u64::MAX / 2,
                    ..valid
                },
                MAX_ARTICLE_DECODED_BYTES as usize + 1,
                YencLayoutMismatch::DecodedSizeAboveCeiling,
            ),
        ];
        for (actual, decoded_len, expected_mismatch) in cases {
            assert_eq!(
                validate_yenc_layout(expected, actual, decoded_len, None),
                Err(expected_mismatch)
            );
        }
    }

    /// An NZB that omits a segment number still lists dense ordinals, so every
    /// later article's prefix-sum ceiling sits below its true offset. The
    /// article's own declared file size is what places it.
    #[test]
    fn sparse_segment_list_places_later_articles_at_their_true_offsets() {
        // A five-article file whose NZB lists only four segments.
        let file = assembly(&[1032; 4]);
        for (ordinal, true_offset) in [(2u32, 2000u64), (3, 3000)] {
            let expected = expected_segment_layout(&file, ordinal).unwrap();
            assert_eq!(
                validate_yenc_layout(
                    expected,
                    YencLayoutAssertions {
                        file_size: 5000,
                        part: Some(ordinal + 2),
                        total: Some(5),
                        begin: Some(true_offset + 1),
                        end: Some(true_offset + 1000),
                    },
                    1000,
                    None,
                ),
                Ok(true_offset)
            );
        }
    }

    /// The `bytes` attribute is indexer-supplied and can simply be too small.
    #[test]
    fn understated_declared_bytes_yield_to_a_truthful_article_size() {
        let file = assembly(&[10, 10]);
        let expected = expected_segment_layout(&file, 1).unwrap();
        assert_eq!(
            validate_yenc_layout(
                expected,
                YencLayoutAssertions {
                    file_size: 2000,
                    part: Some(2),
                    total: Some(2),
                    begin: Some(1001),
                    end: Some(2000),
                },
                1000,
                None,
            ),
            Ok(1000)
        );
    }

    #[test]
    fn absurd_declared_size_or_begin_is_still_refused() {
        let file = assembly(&[10, 10]);
        let expected = expected_segment_layout(&file, 1).unwrap();
        // A file size past the absolute ceiling cannot justify anything.
        assert_eq!(
            validate_yenc_layout(
                expected,
                YencLayoutAssertions {
                    file_size: MAX_DECLARED_FILE_BYTES + 1,
                    part: Some(2),
                    total: Some(2),
                    begin: Some(1001),
                    end: Some(2000),
                },
                1000,
                None,
            ),
            Err(YencLayoutMismatch::BeginAboveDeclaredPrefix)
        );
        // A begin past a size the article itself declares is refused too.
        assert_eq!(
            validate_yenc_layout(
                expected,
                YencLayoutAssertions {
                    file_size: 2000,
                    part: Some(2),
                    total: Some(2),
                    begin: Some(u64::MAX / 2),
                    end: None,
                },
                1000,
                None,
            ),
            Err(YencLayoutMismatch::BeginAboveDeclaredPrefix)
        );
    }

    #[test]
    fn missing_declared_size_still_falls_back_to_the_nzb_envelope() {
        let file = assembly(&[10, 10]);
        let expected = expected_segment_layout(&file, 1).unwrap();
        assert_eq!(
            validate_yenc_layout(
                expected,
                YencLayoutAssertions {
                    file_size: 0,
                    part: Some(2),
                    total: Some(2),
                    begin: Some(1001),
                    end: None,
                },
                1000,
                None,
            ),
            Err(YencLayoutMismatch::BeginAboveDeclaredPrefix)
        );
    }

    #[test]
    fn rejects_huge_or_out_of_range_segment_numbers_without_panicking() {
        let file = assembly(&[4]);
        assert_eq!(
            expected_segment_layout(&file, u32::MAX),
            Err(AuthoritativeLayoutError::SegmentOutOfRange)
        );
        assert_eq!(
            expected_segment_layout(&file, 1),
            Err(AuthoritativeLayoutError::SegmentOutOfRange)
        );
    }

    #[test]
    fn accepts_single_part_without_optional_layout_fields() {
        let file = assembly(&[4]);
        let expected = expected_segment_layout(&file, 0).unwrap();
        assert_eq!(
            validate_yenc_layout(
                expected,
                YencLayoutAssertions {
                    file_size: 4,
                    part: None,
                    total: None,
                    begin: None,
                    end: None,
                },
                4,
                // The only ordinal in the file opens it.
                Some(0),
            ),
            Ok(0)
        );
    }

    #[test]
    fn rejects_checked_range_overflow() {
        let expected = ExpectedSegmentLayout {
            max_file_offset: u64::MAX,
            max_decoded_size: 1,
            max_file_size: u64::MAX,
            part: 1,
            total: 1,
        };
        // begin=0 has no valid predecessor byte, so `begin - 1` underflows.
        assert_eq!(
            validate_yenc_layout(
                expected,
                YencLayoutAssertions {
                    file_size: u64::MAX,
                    part: Some(1),
                    total: Some(1),
                    begin: Some(0),
                    end: Some(1),
                },
                1,
                None,
            ),
            Err(YencLayoutMismatch::InvalidBegin)
        );
        // An absurd offset against real declared bounds is caught by the bound,
        // not by panicking on the arithmetic.
        let file = assembly(&[4, 7]);
        let real = expected_segment_layout(&file, 1).unwrap();
        assert_eq!(
            validate_yenc_layout(
                real,
                YencLayoutAssertions {
                    file_size: 11,
                    part: Some(2),
                    total: Some(2),
                    begin: Some(u64::MAX),
                    end: Some(u64::MAX),
                },
                1,
                None,
            ),
            Err(YencLayoutMismatch::BeginAboveDeclaredPrefix)
        );
    }
}
