//! Metadata compatibility preserves the actual decoded-byte envelope.
use super::*;

fn decoded_layout(
    part: u32,
    total: u32,
    size: u64,
    begin: u64,
    len: usize,
) -> YencLayoutAssertions {
    let payload = vec![b'A'; len];
    let mut wire = Vec::new();
    weaver_yenc::encode_part(
        &payload,
        &mut wire,
        128,
        "fixture.bin",
        part,
        total,
        begin,
        begin + len as u64 - 1,
        size,
    )
    .unwrap();
    let mut output = vec![0; wire.len()];
    let decoded = weaver_yenc::decode(&wire, &mut output).unwrap();
    assert_eq!(decoded.crc_status, weaver_yenc::CrcVerification::Verified);
    assert_eq!(&output[..decoded.bytes_written], payload);
    let m = decoded.metadata;
    YencLayoutAssertions {
        file_size: m.size,
        part: m.part,
        total: m.total,
        begin: m.begin,
        end: m.end,
    }
}

#[test]
fn reported_total_mismatch_preserves_crc_verified_bounded_payload() {
    let expected = ExpectedSegmentLayout {
        max_file_offset: 192_636_813,
        max_decoded_size: 792_641,
        max_file_size: 206_444_061,
        part: 244,
        total: 261,
    };
    let actual = decoded_layout(244, 402, 6_108_962, 186_624_001, 768_000);
    assert_eq!(
        validate_yenc_layout(expected, actual, 768_000),
        Ok(186_624_000)
    );
}

#[test]
fn stale_part_number_does_not_override_verified_byte_placement() {
    let expected = ExpectedSegmentLayout {
        max_file_offset: 1100,
        max_decoded_size: 1100,
        max_file_size: 4400,
        part: 2,
        total: 4,
    };
    let actual = decoded_layout(99, 4, 4096, 1025, 1024);
    assert_eq!(validate_yenc_layout(expected, actual, 1024), Ok(1024));
}

#[test]
fn stale_file_size_does_not_override_verified_bounded_range() {
    let expected = ExpectedSegmentLayout {
        max_file_offset: 1100,
        max_decoded_size: 1100,
        max_file_size: 4400,
        part: 2,
        total: 4,
    };
    let actual = decoded_layout(2, 4, 8192, 1025, 1024);
    assert_eq!(validate_yenc_layout(expected, actual, 1024), Ok(1024));
}

#[test]
fn end_is_advisory_but_actual_file_end_remains_bounded() {
    let expected = ExpectedSegmentLayout {
        max_file_offset: 100,
        max_decoded_size: 100,
        max_file_size: 150,
        part: 2,
        total: 2,
    };
    for end in [None, Some(0), Some(1), Some(99), Some(u64::MAX)] {
        let actual = YencLayoutAssertions {
            file_size: u64::MAX,
            part: Some(42),
            total: Some(500),
            begin: Some(101),
            end,
        };
        assert_eq!(validate_yenc_layout(expected, actual, 50), Ok(100));
        assert_eq!(
            validate_yenc_layout(expected, actual, 51),
            Err(YencLayoutMismatch::EndAboveDeclaredFileSize)
        );
    }
}

#[test]
fn actual_file_end_addition_is_checked() {
    let expected = ExpectedSegmentLayout {
        max_file_offset: u64::MAX,
        max_decoded_size: 4,
        max_file_size: u64::MAX,
        part: 1,
        total: 1,
    };
    let actual = YencLayoutAssertions {
        file_size: 1,
        part: Some(1),
        total: Some(1),
        begin: Some(u64::MAX),
        end: None,
    };
    assert_eq!(
        validate_yenc_layout(expected, actual, 4),
        Err(YencLayoutMismatch::EndAboveDeclaredFileSize)
    );
}

#[test]
fn missing_begin_never_guesses_an_encoded_multipart_offset() {
    let mut expected = ExpectedSegmentLayout {
        max_file_offset: 8,
        max_decoded_size: 8,
        max_file_size: 16,
        part: 2,
        total: 2,
    };
    let mut actual = YencLayoutAssertions {
        file_size: 4,
        part: None,
        total: None,
        begin: None,
        end: None,
    };
    assert_eq!(
        validate_yenc_layout(expected, actual, 4),
        Err(YencLayoutMismatch::InvalidBegin)
    );
    actual.begin = Some(5);
    assert_eq!(validate_yenc_layout(expected, actual, 4), Ok(4));
    actual.begin = None;
    expected.total = 1;
    expected.part = 1;
    expected.max_file_offset = 0;
    assert_eq!(validate_yenc_layout(expected, actual, 4), Ok(0));
}
