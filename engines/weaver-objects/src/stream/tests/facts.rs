// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::super::*;
use super::{brotli, bzip2, decoded, deflate, first_payload, gzip, whole, xz, zstd};
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
    let stream: Stream<D> = facts.read(D::KIND).expect("decodes");
    assert_eq!(whole(&stream, bytes), decoded(member, plain), "{volume}");
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
