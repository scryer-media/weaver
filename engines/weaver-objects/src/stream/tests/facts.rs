// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::super::*;
use super::{
    brotli, bzip2, decoded, deflate, first_payload, gzip, second_payload, whole, xz, xz_at, zstd,
};
use crate::testing::replay::{Step, frames, restores_alike};

// Restored mid-front, a stream sizes its run alike, and a run of the
// restored one, not started before, decodes the whole member.
fn restored<D: Decoder>(volume: &str, member: &str, encode: fn(&[u8]) -> Vec<u8>) {
    let plain = first_payload();
    let bytes = encode(&plain);
    let mut steps = vec![Step::Bound(0, volume)];
    steps.extend(frames(0, &bytes[..bytes.len().min(2 * FIRST)], 24));
    let (facts, live, again) = restores_alike(Box::new(Stream::<D>::new()), &steps, 2, &[]);
    let reserve = |object: &dyn Object| object.engine().map(Engine::reserve);
    assert_eq!(reserve(live.as_ref()), reserve(again.as_ref()));
    let mut stream: Stream<D> = facts.read(D::KIND).expect("decodes");
    assert_eq!(
        whole(&mut stream, bytes),
        decoded(member, plain),
        "{volume}"
    );
}

#[test]
fn every_stream_restored_from_its_facts_runs_as_the_live_one() {
    restored::<Xz>("alpha.bin.xz", "alpha.bin", xz);
    restored::<Gzip>("alpha.bin.gz", "alpha.bin", gzip);
    restored::<Bzip2>("alpha.bin.bz2", "alpha.bin", bzip2);
    restored::<Zstd>("alpha.bin.zst", "alpha.bin", zstd);
    restored::<Brotli>("alpha.bin.br", "alpha.bin", brotli);
    restored::<Deflate>("alpha.bin", "alpha.bin", deflate);
}

// The steps of an xz volume of two concatenated streams.
fn xz_steps(bytes: &[u8], frame: usize) -> Vec<Step<'_>> {
    let mut steps = vec![Step::Bound(0, "alpha.tar.xz")];
    steps.extend(frames(0, bytes, frame));
    steps.push(Step::Closed(0));
    steps
}

// Cut after the stream header, inside the first block header, mid-member and
// at the second stream's start: each restores with the same window, and its
// run decodes both streams through as one member.
#[test]
fn an_xz_stream_restored_at_every_point_sizes_and_decodes_as_the_live_one() {
    let (first, second) = (first_payload(), second_payload());
    let mut bytes = xz_at(&first, 1 << 18);
    let boundary = bytes.len();
    bytes.extend_from_slice(&xz(&second));
    let joined = [first, second].concat();
    let reserve = |object: &dyn Object| object.engine().map(Engine::reserve);
    let header = xz_steps(&bytes, 12);
    let block = xz_steps(&bytes, 4);
    let wide = xz_steps(&bytes, 256);
    // A cut after k frames is step 1 + k.
    let (middle, past) = ((wide.len() - 1) / 2, boundary.div_ceil(256));
    let cuts: [(&[Step<'_>], usize, u64); 4] = [
        (&header, 2, 12),
        (&block, 1 + 4, 16),
        (&wide, 1 + middle, 256 * middle as u64),
        (&wide, 1 + past, 256 * past as u64),
    ];
    for (steps, cut, frontier) in cuts {
        let (facts, live, again) = restores_alike(Box::new(XzStream::new()), steps, cut, &[]);
        let mut kept: XzStream = facts.read(Kind::Xz).expect("decodes");
        assert_eq!(kept.first.len(), FIRST.min(frontier as usize), "cut {cut}");
        assert_eq!(reserve(live.as_ref()), reserve(again.as_ref()));
        assert_eq!(
            whole(&mut kept, bytes.clone()),
            decoded("alpha.tar", joined.clone()),
            "cut {cut}"
        );
    }
    // Restored before its front is in, the stream reserves nothing yet;
    // once it is, the first block's dictionary.
    let early = header_facts(&header, 2);
    let late = header_facts(&block, 1 + FIRST / 4);
    assert_eq!(early.reserve(), None);
    assert_eq!(
        late.reserve(),
        Some(Xz::window(&bytes, &weaver_graph::Budget::default()))
    );
}

fn header_facts(steps: &[Step<'_>], cut: usize) -> XzStream {
    let (facts, _, _) = restores_alike(Box::new(XzStream::new()), steps, cut, &[]);
    facts.read(Kind::Xz).expect("decodes")
}

#[test]
fn a_stream_front_longer_than_it_keeps_is_refused() {
    let mut stream = XzStream::new();
    stream.first = vec![0; FIRST + 1];
    let facts = Facts::of(Kind::Xz, &stream);
    assert!(matches!(
        crate::restore(&facts, weaver_graph::Budget::default()),
        Err(weaver_graph::RestoreError::Impossible { .. })
    ));
}
