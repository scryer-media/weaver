// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{ARTICLE, Post, par2_set, payload};
use super::sfv::crc;

pub fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

pub fn posts(volumes: Vec<(String, Vec<u8>)>) -> Vec<Post> {
    volumes
        .into_iter()
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect()
}

// The volumes, then an index and a volume of two recovery blocks over them.
pub fn with_par2(volumes: Vec<(String, Vec<u8>)>) -> Vec<Post> {
    let files: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let set = par2_set("silver.horizon", &files, 2);
    let mut out = posts(volumes.clone());
    out.extend(set);
    out
}

// RAR5's recovery volumes over `volumes`, as RAR writes them: a header
// naming the set's geometry, every data volume's size and CRC32, and each
// one's Cauchy rows over GF(2^16), the volumes zero-padded to the longest.
pub fn rar5_revs(base: &str, volumes: &[&[u8]], count: usize) -> Vec<Post> {
    let data = volumes.len();
    // unrar-rs reads an odd-width set's last parity word short, so a
    // fixture keeps the longest volume even.
    let width = volumes.iter().map(|volume| volume.len()).max().unwrap_or(0);
    assert_eq!(width % 2, 0, "the longest volume is even");
    let padded: Vec<Vec<u8>> = volumes
        .iter()
        .map(|volume| {
            let mut padded = volume.to_vec();
            padded.resize(width, 0);
            padded
        })
        .collect();
    (0..count)
        .map(|index| {
            let row = data + index;
            let mut parity = vec![0u8; width];
            for (column, volume) in padded.iter().enumerate() {
                let factor = reedsolomon_rs::gf::inv((row as u16) ^ (column as u16));
                reedsolomon_rs::gf_simd::mul_acc_region(factor, volume, &mut parity);
            }
            let mut raw = vec![1u8];
            raw.extend_from_slice(&(data as u16).to_le_bytes());
            raw.extend_from_slice(&(count as u16).to_le_bytes());
            raw.extend_from_slice(&(row as u16).to_le_bytes());
            raw.extend_from_slice(&crc(&parity).to_le_bytes());
            for volume in volumes {
                raw.extend_from_slice(&(volume.len() as u64).to_le_bytes());
                raw.extend_from_slice(&crc(volume).to_le_bytes());
            }
            let size = (raw.len() as u32).to_le_bytes();
            let mut rev = b"Rar!\x1aRev".to_vec();
            rev.extend_from_slice(&crc(&[size.as_slice(), raw.as_slice()].concat()).to_le_bytes());
            rev.extend_from_slice(&size);
            rev.extend_from_slice(&raw);
            // A poster's article holds the whole header, which grows with
            // the volume count, so the set's geometry arrives in the first.
            let header = rev.len();
            rev.extend_from_slice(&parity);
            Post {
                least_article: header.div_ceil(ARTICLE) * ARTICLE,
                ..Post::plain(&format!("{base}.part{}.rev", index + 1), rev)
            }
        })
        .collect()
}

// Enough articles that each of four volumes holds several.
pub fn body() -> Vec<u8> {
    payload(11 * ARTICLE + 29, 61)
}

// A compressed four-volume RAR4 set as RAR writes it, under part numbering:
// each volume's end header states the CRC32 of the bytes before it and
// reserves seven bytes for a recovery volume.
pub fn lz_four() -> Vec<(String, Vec<u8>)> {
    let dir = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/extraction_profiles"
    );
    (1..=4)
        .map(|part| {
            let bytes = std::fs::read(format!("{dir}/rar4_lz_four.part0{part}.rar")).unwrap();
            (format!("silver.horizon.part{part}.rar"), bytes)
        })
        .collect()
}

pub mod store {
    use super::super::harness::{ARTICLE, Claim, Outcome, Scenario, payload};
    use super::{delivered, with_par2};
    use crate::pipeline::tests::direct_store::single_member_store_set;
    use weaver_model::files::FileRole;

    pub const MEMBER: &str = "beacon.bin";

    pub const SECRET: &str = "quill-ember-secret";

    // A member that ends mid-article, over enough articles that every volume
    // of a four-volume set holds some of it.
    pub fn body() -> Vec<u8> {
        payload(7 * ARTICLE + 13, 51)
    }

    pub fn assert_delivers(outcome: &Outcome, name: &str, body: &[u8]) {
        assert!(
            outcome.completed(),
            "{:?} {:?}",
            outcome.status,
            outcome.failure
        );
        assert_eq!(
            outcome.files.keys().cloned().collect::<Vec<_>>(),
            vec![delivered(name)],
            "delivered names",
        );
        assert!(outcome.files[&delivered(name)] == body, "member bytes",);
    }

    // Every volume's mark is what its bytes are now.
    pub fn assert_volumes_complete(graph: &Outcome) {
        let claims = &graph.graph().claims;
        assert!(
            claims.values().all(|claim| *claim == Claim::Complete),
            "{claims:?}"
        );
    }

    // Every article of the scenario's recovery volumes.
    pub fn recovery_articles(scenario: &Scenario) -> Vec<(u32, u32)> {
        scenario
            .posts
            .iter()
            .zip(0u32..)
            .filter(|(post, _)| {
                matches!(
                    post.role,
                    FileRole::Par2 {
                        is_index: false,
                        ..
                    }
                )
            })
            .flat_map(|(post, file)| {
                (0..post.articles(scenario.article).count() as u32)
                    .map(move |ordinal| (file, ordinal))
            })
            .collect()
    }

    // A RAR beside PAR2 whose opening article is gone, every article of its
    // recovery volumes refused at the first ask, as a primary that holds none
    // of them answers.
    pub fn opening_lost_recovery_refused() -> Scenario {
        let mut scenario =
            Scenario::new(with_par2(single_member_store_set(MEMBER, &body(), 1))).lost(0, 0);
        let refused = recovery_articles(&scenario);
        assert!(!refused.is_empty(), "the set has recovery volumes");
        for (file, ordinal) in refused {
            scenario = scenario.refused_once(file, ordinal);
        }
        scenario
    }
}
