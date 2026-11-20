// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::{folded, names, unposted};

// Names drawn from a small alphabet that mixes ASCII case and a letter
// outside ASCII, so many collide only by case and some not at all.
fn names_of(seed: &mut u64, count: usize) -> Vec<String> {
    const LETTERS: [&str; 7] = ["a", "A", "b", "B", ".", "é", "É"];
    (0..count)
        .map(|_| {
            let mut name = String::new();
            for _ in 0..3 {
                *seed = seed
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                name.push_str(LETTERS[(*seed >> 33) as usize % LETTERS.len()]);
            }
            name
        })
        .collect()
}

// The set lookups answer what comparing every pair of the two lists
// answered.
#[test]
fn a_name_set_answers_as_every_pair_compared() {
    let mut seed = 7;
    for round in 0..64 {
        let absent: Vec<(String, u64)> = names_of(&mut seed, round % 9)
            .into_iter()
            .zip(0..)
            .collect();
        let posted = names_of(&mut seed, round % 13);

        let mut kept = absent.clone();
        kept.retain(|(name, _)| posted.iter().all(|member| member != name));
        let mut found = absent.clone();
        unposted(&mut found, posted.iter().map(String::as_str));
        assert_eq!(found, kept);

        let set = folded(&absent);
        let mut buffer = String::new();
        for member in &posted {
            let before = absent
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case(member));
            assert_eq!(names(&set, member, &mut buffer), before, "{member}");
        }
    }
}
