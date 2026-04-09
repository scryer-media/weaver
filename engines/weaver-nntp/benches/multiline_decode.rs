use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use tokio_util::codec::Decoder;
use weaver_nntp::codec::{NntpCodec, NntpFrame, StreamChunk};

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

criterion_group!(benches, bench_multiline_decode);
criterion_main!(benches);
