// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A ReScene `.srr` beside the RAR set it describes. It opens on its own
// header block, `69 69 69`, and holds, for each volume, a block naming it
// (type 0x71) followed by copies of that volume's RAR headers with the
// file data left out, so RAR signatures stand inside it, never at its
// front. It is no volume: it is delivered as a plain file, whole, and the
// set beside it extracts.

use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::multi_member_store_set;

use super::harness::{ARTICLE, Post, Scenario, kinds, payload, try_run};

const SRR: &str = "silver.horizon.srr";

// The ReScene layout over `volumes`: the SRR header block (CRC 0x6969, type
// 0x69, no flags, seven bytes), then per volume an SRR RAR-file block
// (type 0x71) carrying the volume's name, and the volume's own bytes with
// its stored data left out. `data` is the member's bytes each volume
// stores, in order.
fn srr_of(volumes: &[(String, Vec<u8>)], data: &[u8]) -> Vec<u8> {
    let mut srr = vec![0x69, 0x69, 0x69, 0x00, 0x00, 0x07, 0x00];
    let chunk = data.len().div_ceil(volumes.len());
    for ((name, bytes), stored) in volumes.iter().zip(data.chunks(chunk)) {
        let size = u16::try_from(7 + 2 + name.len()).unwrap();
        srr.extend_from_slice(&[0x71, 0x71, 0x71, 0x00, 0x00]);
        srr.extend_from_slice(&size.to_le_bytes());
        srr.extend_from_slice(&u16::try_from(name.len()).unwrap().to_le_bytes());
        srr.extend_from_slice(name.as_bytes());
        let at = bytes
            .windows(stored.len())
            .position(|window| window == stored)
            .expect("the volume stores its part");
        srr.extend_from_slice(&bytes[..at]);
        srr.extend_from_slice(&bytes[at + stored.len()..]);
    }
    srr
}

#[tokio::test]
async fn an_srr_beside_its_rar_set_is_a_plain_file_and_the_set_extracts() {
    let beacon = payload(12 * ARTICLE + 9, 83);
    let volumes = multi_member_store_set(&[("beacon.bin", beacon.clone())], 3);
    let srr = srr_of(&volumes, &beacon);
    assert_eq!(&srr[..3], &[0x69, 0x69, 0x69]);
    assert!(
        srr.windows(8)
            .any(|window| window == b"Rar!\x1a\x07\x01\x00"),
        "the srr holds the volumes' headers"
    );
    // The `.srr` posted first, between the volumes, and last, in either
    // order of arrival: backwards with it between them, it arrives after the
    // later volumes founded the store and before the first volume.
    for (at, backwards) in [
        (0, false),
        (1, false),
        (3, false),
        (0, true),
        (1, true),
        (3, true),
    ] {
        let mut posts: Vec<Post> = volumes
            .iter()
            .map(|(name, bytes)| Post::plain(name, bytes.clone()))
            .collect();
        posts.insert(at, Post::plain(SRR, srr.clone()));
        let mut scenario = Scenario::new(posts);
        if backwards {
            scenario = scenario.backwards();
        }
        let outcome = try_run(&scenario).await.unwrap_or_else(|stall| {
            panic!(
                "srr at {at}, backwards {backwards}: stalled at {:?}: {}",
                stall.status, stall.state
            )
        });
        let case = format!("srr at {at}, backwards {backwards}");
        assert_eq!(
            outcome.status,
            Some(JobStatus::Complete),
            "{case}: {:?} kinds {:?}",
            outcome.failure,
            kinds(&outcome)
        );
        let mut names: Vec<_> = outcome.files.keys().cloned().collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "lantern beacon/beacon.bin".to_string(),
                format!("lantern beacon/{SRR}"),
            ],
            "{case}"
        );
        assert!(
            outcome.files["lantern beacon/beacon.bin"] == beacon,
            "{case}"
        );
        assert!(
            outcome.files[&format!("lantern beacon/{SRR}")] == srr,
            "{case}"
        );
        assert_eq!(kinds(&outcome), vec!["rar_store"], "{case}");
    }
}
