use super::*;

/// What the NZB can honestly say about one segment.
///
/// The NZB's `<segment bytes>` attribute is the **yEnc-encoded** size, roughly
/// 3% larger than the bytes an article decodes to (measured on real fixtures:
/// a 85,698,538-byte file is declared as 88,426,989). So the NZB cannot supply
/// a decoded offset or a decoded size — it can only *bound* them. Treating its
/// numbers as decoded truth rejects every real article, and writing at the
/// offsets it implies would leave a gap between every pair of segments.
///
/// These fields are therefore ceilings, not values:
/// * `max_decoded_size` — the segment's declared size. yEnc only ever expands,
///   so a decode larger than this is impossible and the article is hostile or
///   corrupt.
/// * `max_file_offset` — the encoded prefix sum up to this segment. Since every
///   earlier segment also decodes to no more than it declared, the true offset
///   can never exceed it.
/// * `max_file_size` — the encoded total, the same bound applied to the file.
///
/// `part`/`total` are the exception: the NZB *is* authoritative for a segment's
/// ordinal and the file's segment count, so those are compared for equality.
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::pipeline) enum YencLayoutMismatch {
    /// Decoded more bytes than the segment declared. yEnc expands, so this
    /// cannot happen for a well-formed article.
    DecodedSizeAboveDeclared,
    /// The header claims a file larger than the declared (encoded) total.
    FileSizeAboveDeclared,
    PartialRange,
    /// `end - (begin - 1)` disagrees with what actually decoded, so the header
    /// is not describing the bytes it shipped.
    RangeContradictsDecode,
    /// The claimed offset is past the encoded prefix sum, i.e. further into the
    /// file than this segment could possibly begin.
    BeginAboveDeclaredPrefix,
    /// The claimed range ends past the declared envelope of the whole file.
    EndAboveDeclaredFileSize,
    Part,
    Total,
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
/// it, but it is pinned inside `[0, max_file_offset]` and its length to
/// `max_decoded_size`, so a hostile server cannot choose where its bytes land —
/// only how far short of the declared ceiling they fall. A header that omits
/// the optional `=ypart` range is placed at the segment's declared offset,
/// which is the only estimate available and is exact for a single-part file.
///
/// This is defence in depth, not the integrity guarantee: misplaced or corrupt
/// bytes are caught by the per-article yEnc CRC32, the whole-file CRC32, PAR2
/// block verification and the RAR member checksums.
#[inline]
pub(super) fn validate_yenc_layout(
    expected: ExpectedSegmentLayout,
    actual: YencLayoutAssertions,
    decoded_len: usize,
) -> Result<u64, YencLayoutMismatch> {
    if decoded_len > expected.max_decoded_size as usize {
        return Err(YencLayoutMismatch::DecodedSizeAboveDeclared);
    }
    if actual.file_size > expected.max_file_size {
        return Err(YencLayoutMismatch::FileSizeAboveDeclared);
    }
    let file_offset = match (actual.begin, actual.end) {
        (None, None) => expected.max_file_offset,
        (Some(begin), Some(end)) => {
            let file_offset = begin
                .checked_sub(1)
                .ok_or(YencLayoutMismatch::RangeContradictsDecode)?;
            // The header must describe the bytes it actually shipped.
            if end.checked_sub(file_offset) != Some(decoded_len as u64) {
                return Err(YencLayoutMismatch::RangeContradictsDecode);
            }
            if file_offset > expected.max_file_offset {
                return Err(YencLayoutMismatch::BeginAboveDeclaredPrefix);
            }
            if end > expected.max_file_size {
                return Err(YencLayoutMismatch::EndAboveDeclaredFileSize);
            }
            file_offset
        }
        _ => return Err(YencLayoutMismatch::PartialRange),
    };
    if actual.part.is_some_and(|part| part != expected.part) {
        return Err(YencLayoutMismatch::Part);
    }
    if actual.total.is_some_and(|total| total != expected.total) {
        return Err(YencLayoutMismatch::Total);
    }
    Ok(file_offset)
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
            validate_yenc_layout(expected, assertions(expected), 7),
            Ok(4)
        );
        assert_eq!(
            validate_yenc_layout(
                expected,
                YencLayoutAssertions {
                    file_size: 11,
                    part: None,
                    total: None,
                    begin: None,
                    end: None,
                },
                7,
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
            // A file larger than the encoded envelope is impossible.
            (
                YencLayoutAssertions {
                    file_size: 12,
                    ..valid
                },
                7,
                YencLayoutMismatch::FileSizeAboveDeclared,
            ),
            (
                YencLayoutAssertions {
                    begin: None,
                    ..valid
                },
                7,
                YencLayoutMismatch::PartialRange,
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
            // A range that does not describe the bytes actually shipped.
            (
                YencLayoutAssertions {
                    end: Some(10),
                    ..valid
                },
                7,
                YencLayoutMismatch::RangeContradictsDecode,
            ),
            (
                YencLayoutAssertions {
                    part: Some(1),
                    ..valid
                },
                7,
                YencLayoutMismatch::Part,
            ),
            (
                YencLayoutAssertions {
                    total: Some(3),
                    ..valid
                },
                7,
                YencLayoutMismatch::Total,
            ),
            // Decoding more than the segment declared cannot happen: yEnc only
            // ever expands.
            (valid, 8, YencLayoutMismatch::DecodedSizeAboveDeclared),
        ];
        for (actual, decoded_len, expected_mismatch) in cases {
            assert_eq!(
                validate_yenc_layout(expected, actual, decoded_len),
                Err(expected_mismatch)
            );
        }
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
            ),
            Err(YencLayoutMismatch::RangeContradictsDecode)
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
            ),
            Err(YencLayoutMismatch::BeginAboveDeclaredPrefix)
        );
    }
}
