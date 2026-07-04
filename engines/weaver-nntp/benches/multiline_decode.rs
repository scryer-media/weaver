use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use tokio_util::codec::Decoder;
use weaver_nntp::codec::{NntpCodec, NntpFrame, StreamChunk};
use weaver_nntp::fused_yenc::FusedYencArticleDecoder;
use weaver_nntp::response::parse_response;
use weaver_yenc::{StreamingArticleDecoder, encode};

#[derive(Debug, Clone, Copy)]
enum YencBenchPattern {
    Clean,
    BigBangLike,
    EscapeHeavy,
    DotStuffed,
    TerminatorBoundary,
}

impl YencBenchPattern {
    fn label(self) -> &'static str {
        match self {
            Self::Clean => "clean",
            Self::BigBangLike => "bigbang_like",
            Self::EscapeHeavy => "escape_heavy",
            Self::DotStuffed => "dot_stuffed",
            Self::TerminatorBoundary => "terminator_boundary",
        }
    }
}

fn build_multiline_payload(line_count: usize, stuffed_every: usize) -> BytesMut {
    let mut body = Vec::with_capacity(line_count * 96);
    for line_idx in 0..line_count {
        if stuffed_every != 0 && line_idx % stuffed_every == 0 {
            body.extend_from_slice(b"..profiled-leading-dot-line-");
        } else {
            body.extend_from_slice(b"profiled-plain-line-");
        }
        body.extend_from_slice(line_idx.to_string().as_bytes());
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(b".\r\n");
    BytesMut::from(body.as_slice())
}

fn build_yenc_transcript(decoded_size: usize, pattern: YencBenchPattern) -> BytesMut {
    let mut decoded = Vec::with_capacity(decoded_size);
    for i in 0..decoded_size {
        match pattern {
            YencBenchPattern::Clean => decoded.push(((i.wrapping_mul(31)) % 250 + 5) as u8),
            YencBenchPattern::BigBangLike => decoded.push(((i * 31 + 17) & 0xff) as u8),
            YencBenchPattern::EscapeHeavy => {
                const SOURCE: [u8; 7] = [214, 224, 227, 19, b'A', b'B', b'C'];
                decoded.push(SOURCE[i % SOURCE.len()]);
            }
            YencBenchPattern::DotStuffed | YencBenchPattern::TerminatorBoundary => {
                if i % 128 == 0 {
                    decoded.push(4);
                } else {
                    decoded.push(((i.wrapping_mul(31)) % 250 + 5) as u8);
                }
            }
        }
    }

    let mut article = Vec::new();
    encode(&decoded, &mut article, 128, "bench.bin").unwrap();
    let article = dot_stuff_lines(&article);

    let mut transcript = b"222 <bench@local> body follows\r\n".to_vec();
    transcript.extend_from_slice(&article);
    transcript.extend_from_slice(b".\r\n");
    if matches!(pattern, YencBenchPattern::TerminatorBoundary) {
        transcript.extend_from_slice(b"223 <next@local> article follows\r\n");
    }
    BytesMut::from(transcript.as_slice())
}

fn dot_stuff_lines(input: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(input.len());
    let mut at_line_start = true;
    for &byte in input {
        if at_line_start && byte == b'.' {
            output.push(b'.');
        }
        output.push(byte);
        at_line_start = byte == b'\n';
    }
    output
}

fn decode_current_yenc(payload: &BytesMut) -> usize {
    let mut codec = NntpCodec::new();
    let mut buf = payload.clone();

    match codec.decode(&mut buf).unwrap().unwrap() {
        NntpFrame::Line(line) => {
            let response = parse_response(&line).unwrap();
            assert_eq!(response.code.raw(), 222);
        }
        other => panic!("unexpected frame: {other:?}"),
    }

    codec.set_streaming_multiline(true);
    codec.set_raw_multiline(true);

    let mut decoder = StreamingArticleDecoder::new();
    let mut output = Vec::new();
    while let StreamChunk::Data(data) = codec.decode_streaming_raw_chunk(&mut buf).unwrap().unwrap()
    {
        decoder.feed_chunk(&data, &mut output).unwrap();
    }

    decoder.finish(output).unwrap().data.len()
}

fn decode_fused_yenc(payload: &BytesMut) -> usize {
    let mut decoder = FusedYencArticleDecoder::new();
    let mut buf = payload.clone();
    decoder
        .decode_available(&mut buf)
        .unwrap()
        .expect("complete benchmark article")
        .result
        .bytes_written
}

fn decode_fused_yenc_three_chunks(payload: &BytesMut) -> usize {
    let mut decoder = FusedYencArticleDecoder::new();
    let mut buf = BytesMut::with_capacity(payload.len());
    let chunk_len = payload.len().div_ceil(3);
    let mut offset = 0usize;
    let mut article = None;
    while offset < payload.len() {
        let end = (offset + chunk_len).min(payload.len());
        buf.extend_from_slice(&payload[offset..end]);
        offset = end;
        if article.is_none() {
            article = decoder.decode_available(&mut buf).unwrap();
        }
    }
    article
        .expect("complete benchmark article")
        .result
        .bytes_written
}

fn bench_multiline_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("multiline_decode");
    for (label, stuffed_every) in [("plain", 0usize), ("stuffed", 8usize)] {
        let payload = build_multiline_payload(4096, stuffed_every);
        group.bench_with_input(BenchmarkId::new("full", label), &payload, |b, payload| {
            b.iter(|| {
                let mut codec = NntpCodec::new();
                codec.set_multiline(true);
                let mut buf = payload.clone();
                match codec.decode(black_box(&mut buf)).unwrap().unwrap() {
                    NntpFrame::MultiLineData(data) => black_box(data.len()),
                    other => panic!("unexpected frame: {other:?}"),
                }
            });
        });
        group.bench_with_input(
            BenchmarkId::new("streaming", label),
            &payload,
            |b, payload| {
                b.iter(|| {
                    let mut codec = NntpCodec::new();
                    codec.set_streaming_multiline(true);
                    let mut buf = payload.clone();
                    let mut total = 0usize;
                    loop {
                        match codec.decode_streaming_chunk(black_box(&mut buf)).unwrap() {
                            Some(StreamChunk::Data(data)) => total += data.len(),
                            Some(StreamChunk::End) => break,
                            None => panic!("unexpected partial benchmark chunk"),
                        }
                    }
                    black_box(total)
                });
            },
        );
    }
    group.finish();
}

fn bench_yenc_article_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("yenc_article_decode");
    for pattern in [
        YencBenchPattern::Clean,
        YencBenchPattern::BigBangLike,
        YencBenchPattern::EscapeHeavy,
        YencBenchPattern::DotStuffed,
        YencBenchPattern::TerminatorBoundary,
    ] {
        let label = pattern.label();
        let decoded_size = match pattern {
            YencBenchPattern::BigBangLike => 768_000,
            _ => 512 * 1024,
        };
        let payload = build_yenc_transcript(decoded_size, pattern);
        group.bench_with_input(
            BenchmarkId::new("current", label),
            &payload,
            |b, payload| {
                b.iter(|| black_box(decode_current_yenc(black_box(payload))));
            },
        );
        group.bench_with_input(BenchmarkId::new("fused", label), &payload, |b, payload| {
            b.iter(|| black_box(decode_fused_yenc(black_box(payload))));
        });
        if matches!(pattern, YencBenchPattern::BigBangLike) {
            group.bench_with_input(
                BenchmarkId::new("fused_three_chunks", label),
                &payload,
                |b, payload| {
                    b.iter(|| black_box(decode_fused_yenc_three_chunks(black_box(payload))));
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_multiline_decode, bench_yenc_article_decode);
criterion_main!(benches);
