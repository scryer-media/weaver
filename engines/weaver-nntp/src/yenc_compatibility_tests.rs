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

/// An `=ypart` line marks a slice of a larger file even when neither `=ybegin
/// part=` nor a usable `begin=` came with it. The whole-file size must not be
/// held against the slice, `pcrc32` is what checks it, and the file-wide
/// `crc32` must survive as the whole-file expectation rather than being
/// replaced by the part's own checksum.
#[test]
fn an_unplaceable_part_without_a_part_number_is_still_a_part() {
    let mut crc = weaver_yenc::crc::Crc32::new();
    crc.update(PAYLOAD);
    let crc = crc.finalize();
    let file_crc = crc ^ 0x5a5a_5a5a;
    let article = article(
        "=ybegin line=128 size=16 name=sample.bin\r\n",
        "=ypart end=8\r\n",
        &format!("=yend size=8 pcrc32={crc:08x} crc32={file_crc:08x}\r\n"),
        PAYLOAD,
    );
    let check = |result: &weaver_yenc::DecodeResult, how: &str| {
        assert!(result.defects.invalid_ypart_begin, "{how}");
        assert!(!result.defects.ybegin_size_mismatch, "{how}");
        assert_eq!(result.crc_status, CrcVerification::Verified, "{how}");
        assert_eq!(result.expected_part_crc, Some(crc), "{how}");
        assert_eq!(result.expected_file_crc, Some(file_crc), "{how}");
    };

    let mut output = vec![0; article.len()];
    let result = weaver_yenc::decode_nntp(&article, &mut output).unwrap();
    assert_eq!(&output[..result.bytes_written], PAYLOAD);
    check(&result, "whole buffer");

    let response = wire(&article);
    for chunk_size in [1, response.len()] {
        let chunks: Vec<_> = response.chunks(chunk_size).collect();
        let (decoded, left) = fused(&chunks);
        assert!(left.is_empty(), "chunk={chunk_size}");
        assert_eq!(decoded.to_data(), PAYLOAD, "chunk={chunk_size}");
        check(decoded.yenc_result(), &format!("chunk={chunk_size}"));
    }
}

/// A part whose `=ypart begin=` cannot be read still decodes: the bytes are
/// intact, only their position is unknown. Every chunking must agree that the
/// article is delivered, that the defect is recorded, and that no offset is
/// invented for it.
#[test]
fn unusable_multipart_starts_decode_without_a_position() {
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
        let result = weaver_yenc::decode_nntp(&article, &mut output).unwrap();
        assert_eq!(&output[..result.bytes_written], PAYLOAD, "range={range:?}");
        assert!(result.defects.invalid_ypart_begin, "range={range:?}");
        assert_eq!(result.metadata.begin, None, "range={range:?}");
        assert_eq!(result.metadata.end, None, "range={range:?}");
        assert!(!result.metadata.file_offset_is_known(), "range={range:?}");
        // No grid may be published against a position the article guessed.
        assert!(result.segments.is_empty(), "range={range:?}");

        let response = wire(&article);
        for chunk_size in [1, response.len()] {
            let chunks: Vec<_> = response.chunks(chunk_size).collect();
            let (decoded, left) = fused(&chunks);
            assert!(left.is_empty(), "range={range:?}, chunk={chunk_size}");
            assert_eq!(
                decoded.to_data(),
                PAYLOAD,
                "range={range:?}, chunk={chunk_size}"
            );
            let result = decoded.yenc_result();
            assert!(
                result.defects.invalid_ypart_begin,
                "range={range:?}, chunk={chunk_size}"
            );
            assert_eq!(
                result.metadata.begin, None,
                "range={range:?}, chunk={chunk_size}"
            );
        }
    }
}

/// What one decode path made of an article: the bytes and the verdict.
#[derive(Debug, PartialEq)]
struct Verdict {
    data: Vec<u8>,
    crc: CrcVerification,
    has_trailer: bool,
    defects: YencHeaderDefects,
}

impl Verdict {
    fn of(data: Vec<u8>, result: &DecodeResult) -> Self {
        assert_eq!(data.len(), result.bytes_written);
        Self {
            data,
            crc: result.crc_status,
            has_trailer: result.has_trailer,
            defects: result.defects,
        }
    }
}

fn whole_buffer_verdict(article: &[u8]) -> Verdict {
    let mut output = vec![0; article.len() + 64];
    let result = weaver_yenc::decode_nntp(article, &mut output).unwrap();
    Verdict::of(output[..result.bytes_written].to_vec(), &result)
}

/// The whole-buffer, streaming and fused decoders give one verdict for the
/// article at every wire split, and it is `expected`.
fn assert_three_way(article: &[u8], expected: &Verdict, name: &str) {
    assert_eq!(
        &whole_buffer_verdict(article),
        expected,
        "{name}: whole buffer"
    );
    let response = wire(article);
    let mut splits: Vec<Vec<&[u8]>> = (0..=response.len())
        .map(|split| vec![&response[..split], &response[split..]])
        .collect();
    splits.push(response.chunks(1).collect());
    for chunks in &splits {
        let (streamed, left) = streaming(chunks);
        assert!(left.is_empty());
        assert_eq!(
            &Verdict::of(streamed.data, &streamed.result),
            expected,
            "{name}: streaming {:?}",
            chunks.iter().map(|c| c.len()).collect::<Vec<_>>()
        );
        let (decoded, left) = fused(chunks);
        assert!(left.is_empty());
        assert_eq!(
            &Verdict::of(decoded.to_data(), decoded.yenc_result()),
            expected,
            "{name}: fused {:?}",
            chunks.iter().map(|c| c.len()).collect::<Vec<_>>()
        );
    }
}

const TABLE_HEADER: &[u8] = b"=ybegin part=1 line=128 size=2 name=a\r\n=ypart begin=1 end=2\r\n";

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = weaver_yenc::crc::Crc32::new();
    crc.update(bytes);
    crc.finalize()
}

fn table_article(body: &[u8], pcrc32: u32) -> Vec<u8> {
    [
        TABLE_HEADER,
        body,
        format!("=yend size=2 part=1 pcrc32={pcrc32:08x}\r\n").as_bytes(),
    ]
    .concat()
}

/// A lone `=` ending the last body line before the trailer, and every related
/// shape: whole-buffer, streaming and fused decode each the way rapidyenc and
/// sabctools do, byte for byte and verdict for verdict, at every wire split.
/// The `=` escapes the line's `\r` (0xA3), the `\r` still breaks the line, the
/// trailer is found, and a damaged article reports its CRC mismatch with its
/// data kept.
#[test]
fn escape_before_trailer_agrees_on_every_path() {
    let ab = crc32(b"AB");
    let mismatch = |data: &[u8], size_mismatch: bool| Verdict {
        data: data.to_vec(),
        crc: CrcVerification::Mismatch,
        has_trailer: true,
        defects: YencHeaderDefects {
            yend_size_mismatch: size_mismatch,
            ypart_size_mismatch: size_mismatch,
            ..YencHeaderDefects::default()
        },
    };
    let rows: Vec<(&str, Vec<u8>, Verdict)> = vec![
        // T1, T2: a lone `=` ends the last line, plain and dot-stuffed trailer.
        (
            "T1",
            table_article(b"kl=\r\n", ab),
            mismatch(b"AB\xa3", true),
        ),
        (
            "T2",
            [
                TABLE_HEADER,
                b"kl=\r\n.",
                &table_article(b"", ab)[TABLE_HEADER.len()..],
            ]
            .concat(),
            mismatch(b"AB\xa3", true),
        ),
        // T3: an odd run; the last `=` escapes the `\r`.
        (
            "T3",
            table_article(b"k===\r\n", ab),
            mismatch(b"A\xd3\xa3", true),
        ),
        // T4: an even run is a complete escape.
        (
            "T4",
            table_article(b"k==\r\n", ab),
            mismatch(b"A\xd3", false),
        ),
        // T5: a lone `=` ends a middle line.
        (
            "T5",
            table_article(b"k=\r\nl\r\n", ab),
            mismatch(b"A\xa3B", true),
        ),
        // T6: `=y` inside an escape is data, and the real trailer is found.
        (
            "T6a",
            table_article(b"k==yl\r\n", ab),
            mismatch(b"A\xd3OB", true),
        ),
        (
            "T6b",
            table_article(b"kl\r\n==yl\r\n", ab),
            mismatch(b"AB\xd3OB", true),
        ),
        // T7: the valid n+1 escaped CR.
        (
            "T7",
            table_article(b"kl=M\r\n", ab),
            mismatch(b"AB\xe3", true),
        ),
        // T12: the right CRC for the decoded bytes verifies.
        (
            "T12",
            table_article(b"kl=\r\n", crc32(b"AB\xa3")),
            Verdict {
                crc: CrcVerification::Verified,
                ..mismatch(b"AB\xa3", true)
            },
        ),
        // The correct article.
        (
            "J",
            table_article(b"kl\r\n", ab),
            Verdict {
                crc: CrcVerification::Verified,
                ..mismatch(b"AB", false)
            },
        ),
    ];
    for (name, article, expected) in &rows {
        assert_three_way(article, expected, name);
    }

    // T8: no trailer, the NNTP terminator ends the body.
    let no_trailer = [TABLE_HEADER, b"kl=\r\n"].concat();
    let verdict = whole_buffer_verdict(&no_trailer);
    assert_eq!(verdict.data, b"AB\xa3");
    assert_eq!(verdict.crc, CrcVerification::Unverified);
    assert!(!verdict.has_trailer);
    assert_three_way(&no_trailer, &verdict, "T8");

    // T9: an LF-only break is no line break before a trailer on any path.
    let lf_only = table_article(b"kl=\n", ab);
    let verdict = whole_buffer_verdict(&lf_only);
    assert!(!verdict.has_trailer);
    assert_three_way(&lf_only, &verdict, "T9");
}

/// T11: the escape before the trailer at every offset of a 64-byte window, and
/// across a 64 KiB read, deep in a body long enough for the SIMD kernels, gives
/// one verdict on every path.
#[test]
fn escape_before_trailer_agrees_at_every_window_offset() {
    let line = [b'k'; 128];
    for pad in 0..64 {
        let mut body = Vec::new();
        for _ in 0..560 {
            body.extend_from_slice(&line);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(&vec![b'k'; pad]);
        body.extend_from_slice(b"=\r\n");
        let mut decoded = vec![b'A'; 560 * 128 + pad];
        decoded.push(0xa3);
        let article = table_article(&body, crc32(b"AB"));
        let expected = Verdict {
            crc: CrcVerification::Mismatch,
            has_trailer: true,
            defects: YencHeaderDefects {
                yend_size_mismatch: true,
                ypart_size_mismatch: true,
                ..YencHeaderDefects::default()
            },
            data: decoded,
        };
        assert_eq!(whole_buffer_verdict(&article), expected, "pad {pad}");
        let response = wire(&article);
        let escape = memchr::memmem::rfind(&response, b"=\r\n=yend").unwrap();
        for split in [
            64 * 1024,
            escape,
            escape + 1,
            escape + 2,
            escape + 3,
            escape + 4,
        ] {
            let chunks = [&response[..split], &response[split..]];
            let (streamed, _) = streaming(&chunks);
            assert_eq!(
                Verdict::of(streamed.data, &streamed.result),
                expected,
                "pad {pad} split {split}"
            );
            let (fused_article, _) = fused(&chunks);
            assert_eq!(
                Verdict::of(fused_article.to_data(), fused_article.yenc_result()),
                expected,
                "pad {pad} split {split}"
            );
        }
        let chunks: Vec<_> = response.chunks(64 * 1024).collect();
        let (fused_article, _) = fused(&chunks);
        assert_eq!(
            Verdict::of(fused_article.to_data(), fused_article.yenc_result()),
            expected
        );
    }
}
