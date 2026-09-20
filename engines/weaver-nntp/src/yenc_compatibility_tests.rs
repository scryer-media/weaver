use super::*;
use crate::codec::{NntpCodec, NntpFrame, StreamChunk};
use tokio_util::codec::Decoder;
use weaver_yenc::{CrcVerification, DecodedArticle, StreamingArticleDecoder, YencHeaderDefects};

struct Case {
    name: &'static str,
    article: Vec<u8>,
    bytes: Vec<u8>,
    defects: YencHeaderDefects,
    crc: CrcVerification,
    offset: u64,
}

// A literal binary payload includes escaped bytes and a dot at a line start.
// Expectations below are independent of any reference client's output.
const PAYLOAD: &[u8] = &[4, 214, 0, 19, 227, 61, 42, 255];

fn article(header: &str, range: &str, trailer: &str, bytes: &[u8]) -> Vec<u8> {
    let mut wire = header.as_bytes().to_vec();
    wire.extend_from_slice(range.as_bytes());
    for line in bytes.chunks(4) {
        for (index, byte) in line.iter().enumerate() {
            let byte = byte.wrapping_add(42);
            if index == 0 && byte == b'.' {
                wire.push(b'.');
            }
            if matches!(byte, 0 | 10 | 13 | 61) {
                wire.push(b'=');
                wire.push(byte.wrapping_add(64));
            } else {
                wire.push(byte);
            }
        }
        wire.extend_from_slice(b"\r\n");
    }
    wire.extend_from_slice(trailer.as_bytes());
    wire
}

fn corpus() -> Vec<Case> {
    let mut crc = weaver_yenc::crc::Crc32::new();
    crc.update(PAYLOAD);
    let crc = crc.finalize();
    let header = "=ybegin part=2 total=3 line=128 size=24 name=sample.bin\r\n";
    let range = "=ypart begin=9 end=16\r\n";
    let trailer = format!("=yend size=8 pcrc32={crc:08x}\r\n");
    let mut cases = Vec::new();
    let mut add = |name, h: &str, r: &str, t: &str, defects, status, offset| {
        cases.push(Case {
            name,
            article: article(h, r, t, PAYLOAD),
            bytes: PAYLOAD.to_vec(),
            defects,
            crc: status,
            offset,
        });
    };
    let clean = YencHeaderDefects::default();
    let verified = CrcVerification::Verified;
    add("regular", header, range, &trailer, clean, verified, 8);
    add(
        "stale_part_total",
        "=ybegin part=99 total=402 line=128 size=24 name=sample.bin\r\n",
        range,
        &trailer,
        clean,
        verified,
        8,
    );
    add(
        "stale_small_size",
        "=ybegin part=2 total=3 line=128 size=6 name=sample.bin\r\n",
        range,
        &trailer,
        YencHeaderDefects {
            ypart_end_exceeds_size: true,
            ..clean
        },
        verified,
        8,
    );
    add(
        "stale_large_size",
        "=ybegin part=2 total=3 line=128 size=2400 name=sample.bin\r\n",
        range,
        &trailer,
        clean,
        verified,
        8,
    );
    add(
        "header_gap",
        header,
        "poster note\r\n\r\n=ypart begin=9 end=16\r\n",
        &trailer,
        clean,
        verified,
        8,
    );
    for (name, part) in [("missing_part", ""), ("invalid_part", "part=garbled ")] {
        let h = format!("=ybegin {part}total=3 line=128 size=24 name=sample.bin\r\n");
        add(name, &h, range, &trailer, clean, verified, 8);
    }
    for (name, end) in [
        ("begin_only", ""),
        ("invalid_end", " end=oops"),
        ("reversed_end", " end=3"),
    ] {
        add(
            name,
            header,
            &format!("=ypart begin=9{end}\r\n"),
            &trailer,
            YencHeaderDefects {
                invalid_ypart_end: true,
                ..clean
            },
            verified,
            8,
        );
    }
    for (name, end) in [("long_range", 17), ("short_range", 15)] {
        add(
            name,
            header,
            &format!("=ypart begin=9 end={end}\r\n"),
            &trailer,
            YencHeaderDefects {
                ypart_size_mismatch: true,
                ..clean
            },
            verified,
            8,
        );
    }
    add(
        "wrong_trailer_size",
        header,
        range,
        &format!("=yend size=999 pcrc32={crc:08x}\r\n"),
        YencHeaderDefects {
            yend_size_mismatch: true,
            ..clean
        },
        verified,
        8,
    );
    add(
        "padded_crc",
        header,
        range,
        &format!("=yend size=8 pcrc32={crc:012x}\r\n"),
        clean,
        verified,
        8,
    );
    add(
        "missing_part_crc",
        header,
        range,
        &format!("=yend size=8 crc32={crc:08x}\r\n"),
        clean,
        CrcVerification::Unverified,
        8,
    );
    add(
        "unusable_part_crc",
        header,
        range,
        "=yend size=8 pcrc32=garbled\r\n",
        YencHeaderDefects {
            invalid_pcrc32: true,
            ..clean
        },
        CrcVerification::Unverified,
        8,
    );
    add(
        "mismatching_part_crc",
        header,
        range,
        &format!("=yend size=8 pcrc32={:08x}\r\n", crc ^ 1),
        clean,
        CrcVerification::Mismatch,
        8,
    );
    add(
        "trailer_junk",
        header,
        range,
        &format!("{trailer}poster signature\r\n"),
        clean,
        verified,
        8,
    );
    add(
        "preamble",
        &format!("poster note\r\n{header}"),
        range,
        &trailer,
        YencHeaderDefects {
            junk_before_ybegin: true,
            ..clean
        },
        verified,
        8,
    );
    add(
        "missing_trailer",
        header,
        range,
        "",
        clean,
        CrcVerification::Unverified,
        8,
    );
    let single = "=ybegin line=128 size=8 name=sample.bin\r\n";
    add(
        "single_both_crc",
        single,
        "",
        &format!("=yend size=8 pcrc32={crc:08x} crc32={:08x}\r\n", crc ^ 1),
        clean,
        verified,
        0,
    );
    add(
        "single_wrong_size",
        "=ybegin line=128 size=999 name=sample.bin\r\n",
        "",
        &trailer,
        YencHeaderDefects {
            ybegin_size_mismatch: true,
            ..clean
        },
        verified,
        0,
    );
    add(
        "optional_header_fields",
        "=ybegin part=2\r\n",
        range,
        &trailer,
        YencHeaderDefects {
            missing_name: true,
            missing_line: true,
            missing_size: true,
            ..clean
        },
        verified,
        8,
    );
    add(
        "tabs_and_case",
        "=ybegin\tPART=2\tTOTAL=3\tLINE=128\tSIZE=24\tNAME=sample.bin\r\n",
        range,
        &trailer,
        clean,
        verified,
        8,
    );
    cases
}

fn wire(article: &[u8]) -> Vec<u8> {
    let mut bytes = b"222 <fixture@example.invalid> body\r\n".to_vec();
    bytes.extend_from_slice(article);
    bytes.extend_from_slice(b".\r\n");
    bytes
}

// The streaming decoder consumes codec-produced raw multiline chunks, not a
// status line or the NNTP terminator. Splitting its raw input instead hid this
// boundary in the original differential harness.
fn streaming(chunks: &[&[u8]]) -> (DecodedArticle, Vec<u8>) {
    let mut codec = NntpCodec::new();
    let mut src = BytesMut::new();
    let mut status_seen = false;
    let mut done = false;
    let mut decoder = StreamingArticleDecoder::new();
    let mut output = Vec::new();
    for chunk in chunks {
        src.extend_from_slice(chunk);
        if !status_seen {
            let Some(frame) = codec.decode(&mut src).unwrap() else {
                continue;
            };
            assert!(matches!(frame, NntpFrame::Line(_)));
            status_seen = true;
            codec.set_streaming_multiline(true);
            codec.set_raw_multiline(true);
        }
        while !done {
            match codec.decode_streaming_raw_chunk(&mut src).unwrap() {
                Some(StreamChunk::Data(data)) => decoder.feed_chunk(&data, &mut output).unwrap(),
                Some(StreamChunk::End) => done = true,
                None => break,
            }
        }
    }
    assert!(
        done,
        "complete response must reach the multiline terminator"
    );
    (decoder.finish(output).unwrap(), src.to_vec())
}

fn fused(chunks: &[&[u8]]) -> (FusedYencArticle, Vec<u8>) {
    let mut decoder = FusedYencArticleDecoder::new();
    let mut src = BytesMut::new();
    let mut result = None;
    for chunk in chunks {
        src.extend_from_slice(chunk);
        if result.is_none() {
            result = decoder.decode_available(&mut src).unwrap();
        }
    }
    (result.expect("complete response must finish"), src.to_vec())
}

fn check(case: &Case, data: &[u8], result: &DecodeResult) {
    assert_eq!(data, case.bytes, "{} bytes", case.name);
    assert_eq!(
        result.bytes_written,
        case.bytes.len(),
        "{} length",
        case.name
    );
    assert_eq!(result.crc_status, case.crc, "{} checksum", case.name);
    assert_eq!(result.defects, case.defects, "{} defects", case.name);
    assert_eq!(
        result.metadata.article_file_offset(),
        case.offset,
        "{} placement",
        case.name
    );
}

#[test]
fn compatibility_corpus_across_codec_and_fused_tcp_boundaries() {
    for case in corpus() {
        let mut output = vec![0; case.article.len() + 64];
        let result = weaver_yenc::decode_nntp(&case.article, &mut output).unwrap();
        check(&case, &output[..result.bytes_written], &result);
        let mut response = wire(&case.article);
        let next = b"223 <next@example.invalid>\r\n";
        response.extend_from_slice(next);
        // Every single split includes headers, escapes, trailers and the
        // terminator. Repeated one-byte chunks exercise retained parser state.
        for split in 0..=response.len() {
            let chunks = [&response[..split], &response[split..]];
            let (streamed, left) = streaming(&chunks);
            check(&case, &streamed.data, &streamed.result);
            assert_eq!(left, next, "{} streaming boundary {split}", case.name);
            let (decoded, left) = fused(&chunks);
            check(&case, &decoded.to_data(), decoded.yenc_result());
            assert_eq!(left, next, "{} fused boundary {split}", case.name);
        }
        let chunks: Vec<_> = response.chunks(1).collect();
        let (streamed, left) = streaming(&chunks);
        check(&case, &streamed.data, &streamed.result);
        assert_eq!(left, next);
        let (decoded, left) = fused(&chunks);
        check(&case, &decoded.to_data(), decoded.yenc_result());
        assert_eq!(left, next);
    }
}

#[test]
fn missing_trailer_does_not_consume_the_next_response() {
    let cases = corpus();
    let first = cases
        .iter()
        .find(|case| case.name == "missing_trailer")
        .unwrap();
    let second = &cases[0];
    let second_wire = wire(&second.article);
    let mut input = wire(&first.article);
    input.extend_from_slice(&second_wire);
    let chunks: Vec<_> = input.chunks(1).collect();
    let (decoded, remaining) = fused(&chunks);
    check(first, &decoded.to_data(), decoded.yenc_result());
    assert_eq!(decoded.stats.nntp_terminator_hits, 1);
    assert_eq!(decoded.stats.nntp_terminator_bytes, 3);
    assert_eq!(
        decoded.stats.encoded_bytes_consumed - decoded.stats.nntp_terminator_bytes,
        (wire(&first.article).len() - 3) as u64
    );
    assert_eq!(remaining, second_wire);
    let (decoded, remaining) = fused(&[&remaining]);
    check(second, &decoded.to_data(), decoded.yenc_result());
    assert!(remaining.is_empty());
}

#[test]
fn transport_interruption_is_not_a_missing_trailer() {
    let cases = corpus();
    let case = cases
        .iter()
        .find(|case| case.name == "missing_trailer")
        .unwrap();
    let response = wire(&case.article);
    let mut input = BytesMut::from(&response[..response.len() - 3]);
    let mut decoder = FusedYencArticleDecoder::new();
    assert!(decoder.decode_available(&mut input).unwrap().is_none());
}

#[test]
fn unusable_multipart_starts_remain_structural_errors() {
    for range in [
        "=ypart end=8\r\n",
        "=ypart begin=0 end=8\r\n",
        "=ypart begin=invalid end=8\r\n",
        "=ypart begin=-1 end=8\r\n",
    ] {
        let article = article(
            "=ybegin part=1 total=2 line=128 size=16 name=sample.bin\r\n",
            range,
            "=yend size=8\r\n",
            PAYLOAD,
        );
        let mut output = vec![0; article.len()];
        assert!(weaver_yenc::decode_nntp(&article, &mut output).is_err());
        let response = wire(&article);
        for chunk_size in [1, response.len()] {
            let mut src = BytesMut::new();
            let mut decoder = FusedYencArticleDecoder::new();
            let mut rejected = false;
            for chunk in response.chunks(chunk_size) {
                src.extend_from_slice(chunk);
                match decoder.decode_available(&mut src) {
                    Err(FusedYencError::Yenc(_)) => {
                        rejected = true;
                        break;
                    }
                    Err(error) => panic!("unexpected transport error: {error}"),
                    Ok(Some(_)) => panic!("unusable multipart start was accepted"),
                    Ok(None) => {}
                }
            }
            assert!(rejected, "range={range:?}, chunk={chunk_size}");
        }
    }
}
