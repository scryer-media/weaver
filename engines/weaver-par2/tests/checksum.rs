use weaver_par2::checksum::{FileHashState, SliceChecksumState, crc32, crc32_combine, md5};

#[test]
fn crc32_known_value() {
    assert_eq!(crc32(b""), 0);
    assert_eq!(crc32(b"123456789"), 0xCBF43926);
}

#[test]
fn md5_known_value() {
    let empty_md5 = md5(b"");
    assert_eq!(
        empty_md5,
        [
            0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04, 0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8,
            0x42, 0x7e
        ]
    );
}

#[test]
fn streaming_matches_oneshot() {
    let data = b"Hello, World! This is test data for streaming checksum.";

    let expected_crc = crc32(data);
    let expected_md5 = md5(data);

    for chunk_size in [1, 3, 7, 13, data.len()] {
        let mut state = SliceChecksumState::new();
        for chunk in data.chunks(chunk_size) {
            state.update(chunk);
        }
        let (crc, md5_hash) = state.finalize(None);
        assert_eq!(
            crc, expected_crc,
            "CRC32 mismatch for chunk_size={chunk_size}"
        );
        assert_eq!(
            md5_hash, expected_md5,
            "MD5 mismatch for chunk_size={chunk_size}"
        );
    }
}

#[test]
fn slice_checksum_with_padding() {
    let data = b"short";
    let pad_to = 16u64;

    let mut padded = data.to_vec();
    padded.resize(pad_to as usize, 0);
    let expected_crc = crc32(&padded);
    let expected_md5 = md5(&padded);

    let mut state = SliceChecksumState::new();
    state.update(data);
    let (crc, md5_hash) = state.finalize(Some(pad_to));
    assert_eq!(crc, expected_crc);
    assert_eq!(md5_hash, expected_md5);
}

#[test]
fn file_hash_state_works() {
    let data = b"test file content";
    let expected = md5(data);

    let mut state = FileHashState::new();
    state.update(&data[..5]);
    state.update(&data[5..]);
    assert_eq!(state.bytes_fed(), data.len() as u64);
    assert_eq!(state.finalize(), expected);
}

#[test]
fn crc32_combine_basic() {
    let a = b"Hello, ";
    let b = b"World!";
    let combined = crc32(&[a.as_slice(), b.as_slice()].concat());
    let result = crc32_combine(crc32(a), crc32(b), b.len() as u64);
    assert_eq!(result, combined);
}

#[test]
fn crc32_combine_empty_second() {
    let a = b"data";
    assert_eq!(crc32_combine(crc32(a), crc32(b""), 0), crc32(a));
}

#[test]
fn crc32_combine_zero_padding() {
    let data = b"short";
    let padded_len = 64u64;
    let padding = vec![0u8; (padded_len - data.len() as u64) as usize];
    let expected = crc32(&[data.as_slice(), &padding].concat());
    let result = crc32_combine(crc32(data), crc32(&padding), padding.len() as u64);
    assert_eq!(result, expected);
}

#[test]
fn crc32_combine_large_length() {
    let parts: Vec<Vec<u8>> = (0..100u8).map(|i| vec![i; 100]).collect();
    let full: Vec<u8> = parts.iter().flat_map(|p| p.iter().copied()).collect();
    let expected = crc32(&full);

    let mut combined = crc32(&parts[0]);
    for part in &parts[1..] {
        combined = crc32_combine(combined, crc32(part), part.len() as u64);
    }
    assert_eq!(combined, expected);
}

#[test]
fn bytes_fed_tracking() {
    let mut state = SliceChecksumState::new();
    assert_eq!(state.bytes_fed(), 0);
    state.update(b"hello");
    assert_eq!(state.bytes_fed(), 5);
    state.update(b"world");
    assert_eq!(state.bytes_fed(), 10);
}
