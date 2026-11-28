// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The writer's literal coder: any bytes packed as a compressed member that
// unrar makes back, alone, split over volumes and enciphered, and a mixed set
// carrying such members that the store streams and runs as any other.

use super::PASSWORD;
use super::mixed::check;
use super::writer::{Item, Options, Version, literal, unrar, write};
use crate::testing::payload;

// Lengths that cover one, two and three size bytes in a RAR5 block header,
// and a final byte filled to every bit count.
const LENGTHS: [usize; 9] = [1, 2, 3, 7, 16, 255, 300, 70_000, 200_001];

fn every_byte() -> Vec<u8> {
    (0..=255u8).cycle().take(1_024).collect()
}

#[test]
fn a_literal_member_extracts_as_its_bytes() {
    for version in [Version::Rar5, Version::Rar4] {
        let mut plains: Vec<Vec<u8>> = LENGTHS
            .iter()
            .enumerate()
            .map(|(seed, &length)| payload(seed as u64 + 1, length))
            .collect();
        plains.push(every_byte());
        plains.push(vec![0; 5_000]);
        plains.push(vec![0xff; 5_000]);
        for plain in plains {
            let item = literal(version, "packed.bin", &plain);
            let Item::Packed { ref packed, .. } = item else {
                unreachable!()
            };
            assert_ne!(*packed, plain, "{version:?} {}", plain.len());
            for (split, password) in [
                (None, None),
                (Some(4_000), None),
                (None, Some(PASSWORD)),
                (Some(4_000), Some(PASSWORD)),
            ] {
                let mut options = Options::new(version);
                options.split = split;
                options.password = password;
                let volumes = write(std::slice::from_ref(&item), &options);
                let extracted = unrar(&volumes, password);
                let label = format!("{version:?} {} {split:?} {password:?}", plain.len());
                assert_eq!(extracted.len(), 1, "{label}");
                assert_eq!(extracted[0].0, "packed.bin", "{label}");
                assert!(extracted[0].1 == plain, "{label}");
            }
        }
    }
}

// A mixed set whose compressed members are literal-coded: the store streams
// its stored members, leaves the literal ones to the run, and every member
// stands as unrar extracts it.
#[test]
fn a_mixed_set_of_literal_members_streams_and_runs_as_any_other() {
    for version in [Version::Rar5, Version::Rar4] {
        let items = vec![
            Item::File("alpha.mkv", payload(1, 10_000)),
            literal(version, "notes.nfo", &payload(4, 3_000)),
            Item::File("beta.mkv", payload(2, 5_003)),
            literal(version, "extra.bin", &payload(5, 40_000)),
            Item::File("gamma.srt", payload(3, 300)),
        ];
        for (label, split, password) in [
            ("one volume", None, None),
            ("many volumes", Some(6_000), None),
            ("enciphered", Some(6_000), Some(PASSWORD)),
        ] {
            let mut options = Options::new(version);
            options.split = split;
            options.password = password;
            let volumes = write(&items, &options);
            check(&format!("{version:?} {label}"), volumes, password);
        }
    }
}
