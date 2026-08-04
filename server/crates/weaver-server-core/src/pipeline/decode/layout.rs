use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ExpectedSegmentLayout {
    pub(super) file_offset: u64,
    pub(super) decoded_size: u32,
    pub(super) file_size: u64,
    pub(super) part: u32,
    pub(super) total: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AuthoritativeLayoutError {
    FileMissing,
    SegmentOutOfRange,
    ReversedSegmentBounds,
    SegmentTooLarge,
    SegmentEndOverflow,
    SegmentPastFileEnd,
    InvalidPartNumber,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum YencLayoutMismatch {
    DecodedSize,
    FileSize,
    PartialRange,
    Begin,
    End,
    Part,
    Total,
}

#[inline]
pub(super) fn expected_segment_layout(
    file: &crate::jobs::assembly::FileAssembly,
    segment_number: u32,
) -> Result<ExpectedSegmentLayout, AuthoritativeLayoutError> {
    let (file_offset, segment_end) = file
        .segment_bounds(segment_number)
        .ok_or(AuthoritativeLayoutError::SegmentOutOfRange)?;
    let segment_size = segment_end
        .checked_sub(file_offset)
        .ok_or(AuthoritativeLayoutError::ReversedSegmentBounds)?;
    let decoded_size =
        u32::try_from(segment_size).map_err(|_| AuthoritativeLayoutError::SegmentTooLarge)?;
    if file_offset
        .checked_add(u64::from(decoded_size))
        .ok_or(AuthoritativeLayoutError::SegmentEndOverflow)?
        != segment_end
    {
        return Err(AuthoritativeLayoutError::SegmentEndOverflow);
    }
    let file_size = file.total_bytes();
    if segment_end > file_size {
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
        file_offset,
        decoded_size,
        file_size,
        part,
        total,
    })
}

#[inline]
pub(super) fn validate_yenc_layout(
    expected: ExpectedSegmentLayout,
    actual: YencLayoutAssertions,
    decoded_len: usize,
) -> Result<(), YencLayoutMismatch> {
    if decoded_len != expected.decoded_size as usize {
        return Err(YencLayoutMismatch::DecodedSize);
    }
    if actual.file_size != expected.file_size {
        return Err(YencLayoutMismatch::FileSize);
    }
    match (actual.begin, actual.end) {
        (None, None) => {}
        (Some(begin), Some(end)) => {
            let expected_begin = expected
                .file_offset
                .checked_add(1)
                .ok_or(YencLayoutMismatch::Begin)?;
            let expected_end = expected
                .file_offset
                .checked_add(u64::from(expected.decoded_size))
                .ok_or(YencLayoutMismatch::End)?;
            if begin != expected_begin {
                return Err(YencLayoutMismatch::Begin);
            }
            if end != expected_end {
                return Err(YencLayoutMismatch::End);
            }
        }
        _ => return Err(YencLayoutMismatch::PartialRange),
    }
    if actual.part.is_some_and(|part| part != expected.part) {
        return Err(YencLayoutMismatch::Part);
    }
    if actual.total.is_some_and(|total| total != expected.total) {
        return Err(YencLayoutMismatch::Total);
    }
    Ok(())
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
        "yEnc layout mismatch ({mismatch:?}): expected offset={} decoded_size={} file_size={} part={}/{}; got decoded_size={} file_size={} begin={:?} end={:?} part={:?} total={:?}",
        expected.file_offset,
        expected.decoded_size,
        expected.file_size,
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

    fn assertions(expected: ExpectedSegmentLayout) -> YencLayoutAssertions {
        YencLayoutAssertions {
            file_size: expected.file_size,
            part: Some(expected.part),
            total: Some(expected.total),
            begin: Some(expected.file_offset + 1),
            end: Some(expected.file_offset + u64::from(expected.decoded_size)),
        }
    }

    #[test]
    fn derives_trusted_out_of_order_segment_layout() {
        let file = assembly(&[4, 7, 3]);
        assert_eq!(
            expected_segment_layout(&file, 1),
            Ok(ExpectedSegmentLayout {
                file_offset: 4,
                decoded_size: 7,
                file_size: 14,
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
            Ok(())
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
            Ok(())
        );
    }

    #[test]
    fn rejects_each_untrusted_layout_mismatch() {
        let file = assembly(&[4, 7]);
        let expected = expected_segment_layout(&file, 1).unwrap();
        let valid = assertions(expected);
        let cases = [
            (
                YencLayoutAssertions {
                    file_size: 12,
                    ..valid
                },
                7,
                YencLayoutMismatch::FileSize,
            ),
            (
                YencLayoutAssertions {
                    begin: None,
                    ..valid
                },
                7,
                YencLayoutMismatch::PartialRange,
            ),
            (
                YencLayoutAssertions {
                    begin: Some(4),
                    ..valid
                },
                7,
                YencLayoutMismatch::Begin,
            ),
            (
                YencLayoutAssertions {
                    end: Some(10),
                    ..valid
                },
                7,
                YencLayoutMismatch::End,
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
            (valid, 6, YencLayoutMismatch::DecodedSize),
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
            Ok(())
        );
    }

    #[test]
    fn rejects_checked_range_overflow() {
        let expected = ExpectedSegmentLayout {
            file_offset: u64::MAX,
            decoded_size: 1,
            file_size: u64::MAX,
            part: 1,
            total: 1,
        };
        assert_eq!(
            validate_yenc_layout(
                expected,
                YencLayoutAssertions {
                    file_size: u64::MAX,
                    part: Some(1),
                    total: Some(1),
                    begin: Some(u64::MAX),
                    end: Some(u64::MAX),
                },
                1,
            ),
            Err(YencLayoutMismatch::Begin)
        );
    }
}
