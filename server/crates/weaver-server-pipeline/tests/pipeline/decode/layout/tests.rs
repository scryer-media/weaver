// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use weaver_model::files::FileRole;
use weaver_server_base::jobs::ids::{JobId, NzbFileId};

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

// A header that claims exactly the declared ceiling — the shape a fixture
// produces when it declares decoded sizes rather than encoded ones.
fn assertions(expected: ExpectedSegmentLayout) -> YencLayoutAssertions {
    YencLayoutAssertions {
        file_size: expected.max_file_size,
        part: Some(expected.part),
        total: Some(expected.total),
        begin: Some(expected.max_file_offset + 1),
        end: Some(expected.max_file_offset + u64::from(expected.max_decoded_size)),
    }
}

// A refused article's line names what its headers claim, what the NZB
// allows for its segment, and where the segment it runs into sits. The
// header is shaped like an article another post carries under a reused
// message-id: its own size admits the far range, the NZB does not.
#[test]
fn a_refused_placement_names_both_sides() {
    let actual = YencLayoutAssertions {
        file_size: 181_790_224,
        part: Some(555),
        total: Some(556),
        begin: Some(180_993_553),
        end: Some(181_391_888),
    };
    let expected = ExpectedSegmentLayout {
        max_file_offset: 793_082,
        max_decoded_size: 793_082,
        max_file_size: 3_111_976,
        part: 2,
        total: 4,
    };
    let line = format_yenc_layout_conflict(
        1,
        180_993_552,
        398_336,
        actual,
        "3e4d16ff2f415130",
        expected,
        3,
        Some((2_625_536, 398_336)),
    );
    assert_eq!(
        line,
        "yEnc layout conflict: segment 1 claims [180993552, 181391888) which runs into \
             segment 3 placed at [2625536, 3023872); its headers say name=\"3e4d16ff2f415130\" \
             size=181790224 part=Some(555) total=Some(556) begin=Some(180993553) \
             end=Some(181391888); the NZB allows offset<=793082 size<=3111976 part=2/4"
    );
    let unplaced = format_yenc_layout_conflict(1, 0, 4, actual, "x", expected, 0, None);
    assert!(unplaced.contains("segment 0 placed at an unplaced range"));
}

// A withdrawn placement's line names its range and envelope, and the
// headers of the article that contradicts it.
#[test]
fn a_withdrawn_placement_names_both_sides() {
    let expected = ExpectedSegmentLayout {
        max_file_offset: 0,
        max_decoded_size: 793_082,
        max_file_size: 25_769_203,
        part: 1,
        total: 33,
    };
    let contradicting = YencLayoutAssertions {
        file_size: 25_000_000,
        part: Some(2),
        total: Some(33),
        begin: Some(768_001),
        end: Some(1_536_000),
    };
    let line = format_withdrawn_placement(
        0,
        3_023_872,
        258_048,
        expected,
        1,
        768_000,
        768_000,
        contradicting,
    );
    assert_eq!(
        line,
        "yEnc layout conflict: segment 0 claims [3023872, 3281920) past what the NZB \
             allows for it (offset<=0 size<=25769203), which segment 1 contradicts at \
             [768000, 1536000) with headers size=25000000 part=Some(2) total=Some(33) \
             begin=Some(768001) end=Some(1536000)"
    );
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

// The case the equality contract could not express: a real NZB declares
// yEnc-*encoded* sizes, so every article decodes to fewer bytes than its
// segment claims and lands at a lower offset than the declared prefix sum.
// Measured on a real fixture: 88,426,989 declared for 85,698,538 true.
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

// An NZB that omits a segment number still lists dense ordinals, so every
// later article's prefix-sum ceiling sits below its true offset. The
// article's own declared file size is what places it.
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

// The `bytes` attribute is indexer-supplied and can simply be too small.
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
