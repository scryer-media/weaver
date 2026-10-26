// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Tail-metadata discovery under every bounded arrival/duplicate schedule.
use super::super::archive_schedules::{
    Interruption, ScheduleOptions, Selection, combined_campaign,
};
use super::*;

pub(in super::super) mod graph;

// Bytes in which no slice recurs. A recovery set mends a lost slice from any
// copy of it elsewhere in the set, so a payload that repeats survives a loss
// the set carries no recovery data for.
pub(in super::super) fn unrepeated_payload(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed;
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 56) as u8
        })
        .collect()
}

#[derive(Clone, Copy, Debug)]
enum Shape {
    Copy,
    // Four single-article volumes, so two of the volumes are middle volumes.
    CopyFourVolumes,
    // One four-article volume: the start header opens it, the end header
    // closes it, and nothing else is posted besides the recovery set.
    CopySingle,
    Multiple,
    EmptyEntry,
    Nested,
    Lzma2Fallback,
    EncryptedCopy,
    EncryptedHeaders,
    EncryptedLzma2,
    Solid,
    SolidEncrypted,
    SolidHeaders,
    // Volume names that say nothing. The recovery set carries the real
    // names, as an obfuscated post's does.
    CopyObfuscated,
    // One whole container under a name that says nothing: its own signature
    // header is the only identity it needs.
    CopySingleObfuscated,
}

// Whether a shape's entries are its archive's bytes: stored under `Copy`,
// deciphered in place when encrypted, or packed by a coder, solid or not.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Packing {
    Stored,
    Packed,
}

// A shape's archive as posted: its entries by name and bytes, how they are
// packed, the volumes the archive is split into, the names its recovery set
// describes them by, and the job, with the password when the archive needs
// one.
struct ShapeFixture {
    expected: BTreeMap<&'static str, Vec<u8>>,
    packing: Packing,
    volumes: Vec<(String, Vec<u8>)>,
    described: Option<Vec<String>>,
    password: Option<&'static str>,
    spec: JobSpec,
}

fn shape_fixture(shape: Shape) -> ShapeFixture {
    // A described volume is bound by the fingerprint of its first 16 KiB,
    // which its offset-zero article has to cover whole.
    let first = if matches!(shape, Shape::CopyObfuscated | Shape::CopySingleObfuscated) {
        unrepeated_payload(13, 70_001)
    } else {
        payload(13, 6001)
    };
    let second = payload(29, 307);
    let name = if matches!(shape, Shape::Nested) {
        "nested/feature.mkv"
    } else {
        "feature.mkv"
    };
    let mut entries = vec![Entry::file(name, first.clone())];
    let mut expected = BTreeMap::from([(name, first)]);
    match shape {
        Shape::Multiple => {
            entries.push(Entry::file("second.nfo", second.clone()));
            expected.insert("second.nfo", second);
        }
        Shape::EmptyEntry => {
            entries.push(Entry::empty_file("empty.nfo"));
            expected.insert("empty.nfo", Vec::new());
        }
        _ => {}
    }
    let coded = matches!(shape, Shape::Lzma2Fallback | Shape::EncryptedLzma2);
    let solid = matches!(
        shape,
        Shape::Solid | Shape::SolidEncrypted | Shape::SolidHeaders
    );
    let packing = if coded || solid {
        Packing::Packed
    } else {
        Packing::Stored
    };
    let method = if coded {
        EncoderMethod::LZMA2
    } else {
        EncoderMethod::COPY
    };
    let password = matches!(
        shape,
        Shape::EncryptedCopy
            | Shape::EncryptedHeaders
            | Shape::EncryptedLzma2
            | Shape::SolidEncrypted
            | Shape::SolidHeaders
    )
    .then_some("moonlit-harbour");
    let archive = if solid {
        expected.clear();
        for (index, name) in ["part0.bin", "part1.bin", "part2.bin"]
            .into_iter()
            .enumerate()
        {
            expected.insert(
                name,
                (0..8193)
                    .map(|n| ((n * 7 + n / 251 + index * 13) % 253) as u8)
                    .collect(),
            );
        }
        macro_rules! fixture {
            ($name:literal) => {
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/fixtures/extraction_profiles/",
                    $name
                ))
                .to_vec()
            };
        }
        match shape {
            Shape::Solid => fixture!("sevenz_solid.7z"),
            Shape::SolidEncrypted => fixture!("sevenz_solid_encrypted.7z"),
            Shape::SolidHeaders => fixture!("sevenz_solid_headers.7z"),
            _ => unreachable!(),
        }
    } else {
        build_7z_shaped(
            &entries,
            method,
            password,
            matches!(shape, Shape::EncryptedHeaders),
        )
    };
    let count = match shape {
        Shape::CopyFourVolumes => 4,
        Shape::CopySingle | Shape::CopySingleObfuscated => 1,
        _ => 2,
    };
    let volumes = split_volumes(&archive, count);
    let (volumes, described) =
        if matches!(shape, Shape::CopyObfuscated | Shape::CopySingleObfuscated) {
            let described = volumes.iter().map(|(name, _)| name.clone()).collect();
            (obfuscate_volumes(&volumes), Some(described))
        } else {
            (volumes, None::<Vec<String>>)
        };
    let mut spec = sevenz_job_spec(&volumes, 4 / count);
    spec.password = password.map(str::to_owned);
    ShapeFixture {
        expected,
        packing,
        volumes,
        described,
        password,
        spec,
    }
}
