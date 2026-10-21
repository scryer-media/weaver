// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Standalone RAR recovery volumes under both engines: a RAR5 or RAR4 set
// posted without some of its data volumes, beside `.rev` files that can
// rebuild them, or too few that cannot. The set is stored, so the member is
// whole only when every volume is.

use sha2::Digest;
use weaver_graph::recovery::RepairOutcome;

use super::harness::{ARTICLE, Outcome, Post, Scenario, payload, run, try_run};
use super::rar::{assert_classic_stalls, delivered, posts};
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::Engine;
use crate::pipeline::tests::direct_store::scenarios::{HeaderCheck, header_encrypted_store_set};
use crate::pipeline::tests::direct_store::single_member_store_set;
use crate::pipeline::tests::rar_extraction::{
    single_member_rar4_old_numbering_set, single_recovery_volume_for_rar4_set,
    with_rar4_header_crcs,
};

const MEMBER: &str = "beacon.bin";
const SECRET: &str = "quill-ember-secret";

// Enough articles that each of four volumes holds several.
pub(super) fn body() -> Vec<u8> {
    payload(11 * ARTICLE + 29, 61)
}

// RAR5's recovery volumes over `volumes`, as RAR writes them: a header
// naming the set's geometry, every data volume's size and CRC32, and each
// one's Cauchy rows over GF(2^16), the volumes zero-padded to the longest.
pub(super) fn rar5_revs(base: &str, volumes: &[&[u8]], count: usize) -> Vec<Post> {
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
            rev.extend_from_slice(&parity);
            Post::plain(&format!("{base}.part{}.rev", index + 1), rev)
        })
        .collect()
}

fn crc(bytes: &[u8]) -> u32 {
    par2_rs::checksum::crc32(bytes)
}

// The set's volumes but the ones at `absent`, then the recovery volumes.
fn posted(volumes: Vec<(String, Vec<u8>)>, absent: &[usize], revs: Vec<Post>) -> Vec<Post> {
    let kept = volumes
        .into_iter()
        .enumerate()
        .filter(|(index, _)| !absent.contains(index))
        .map(|(_, volume)| volume)
        .collect();
    let mut out = posts(kept);
    out.extend(revs);
    out
}

fn rar5_set(absent: &[usize], revs: usize) -> (Vec<u8>, Scenario) {
    let body = body();
    let volumes = single_member_store_set(MEMBER, &body, 4);
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let revs = rar5_revs("silver.horizon", &bytes, revs);
    (body, Scenario::new(posted(volumes, absent, revs)))
}

// Classic waits on the volume no article carries and never settles, as it
// does for any RAR set with a hole; the graph rules on it.
async fn run_each(scenario: &Scenario) -> Outcome {
    assert_classic_stalls(try_run(scenario, Engine::Classic).await);
    run(scenario, Engine::Graph).await
}

// The member alone, every byte as written, after one ruling that rebuilt
// the absent volumes: the `.rev` files are spent and never delivered.
fn assert_rebuilt(outcome: &Outcome, body: &[u8], volumes: u64) {
    assert_eq!(
        outcome.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: volumes })]
    );
    assert!(
        outcome.completed(),
        "{}: {:?} {:?}",
        outcome.engine,
        outcome.status,
        outcome.failure
    );
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered(MEMBER)],
        "{}: delivered names",
        outcome.engine
    );
    assert!(
        outcome.files[&delivered(MEMBER)] == body,
        "{}: member bytes",
        outcome.engine
    );
}

#[tokio::test]
async fn rar_rev_rebuilds_one_absent_rar5_volume_from_one_recovery_volume() {
    let (body, scenario) = rar5_set(&[1], 1);
    assert_rebuilt(&run_each(&scenario).await, &body, 1);
}

#[tokio::test]
async fn rar_rev_rebuilds_two_absent_rar5_volumes_from_two_recovery_volumes() {
    let (body, scenario) = rar5_set(&[1, 3], 2);
    assert_rebuilt(&run_each(&scenario).await, &body, 2);
}

#[tokio::test]
async fn rar_rev_rebuilds_one_absent_rar4_volume_from_its_recovery_volume() {
    let body = body();
    let volumes: Vec<(String, Vec<u8>)> =
        single_member_rar4_old_numbering_set("silver.horizon", MEMBER, &body, 4)
            .into_iter()
            .map(|(name, bytes)| (name, with_rar4_header_crcs(bytes)))
            .collect();
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let rev = Post::plain(
        "silver.horizon1.rev",
        single_recovery_volume_for_rar4_set(&bytes),
    );
    let scenario = Scenario::new(posted(volumes, &[2], vec![rev]));
    assert_rebuilt(&run_each(&scenario).await, &body, 1);
}

// A RAR3 recovery volume as RAR writes it beside volumes whose end headers
// reserve seven zero bytes for it (REVSPACE): the set's parity, as wide as
// the widest volume, the footer in place of the parity of those zeros.
fn rar3_rev(volumes: &[&[u8]]) -> Vec<u8> {
    const FOOTER: usize = 7;
    let width = volumes.iter().map(|volume| volume.len()).max().unwrap_or(0);
    let coder = reedsolomon_rs::rar3::Rar3RsCoder::new(1).expect("one recovery volume");
    let mut column = vec![0u8; volumes.len()];
    let mut parity = [0u8; 1];
    let mut rev = Vec::with_capacity(width);
    for position in 0..width - FOOTER {
        for (slot, volume) in column.iter_mut().zip(volumes) {
            *slot = volume.get(position).copied().unwrap_or(0);
        }
        coder.encode(&column, &mut parity);
        rev.push(parity[0]);
    }
    rev.extend_from_slice(&[(volumes.len() - 1) as u8, 0, 0]);
    let crc = crc(&rev);
    rev.extend_from_slice(&crc.to_le_bytes());
    rev
}

// A compressed RAR4 set as RAR writes it, under part numbering, its third
// volume never posted, beside the one RAR3 recovery volume RAR names
// `.part1.rev`. Its fronts state no volume number, so the fourth volume is
// placed by its name, not beside the second it would link to across the
// hole, and the rebuilt third takes the place its name gives it.
#[tokio::test]
async fn rar_rev_rebuilds_an_absent_volume_of_a_compressed_part_numbered_rar4_set() {
    let dir = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/extraction_profiles"
    );
    let volumes: Vec<(String, Vec<u8>)> = (1..=4)
        .map(|part| {
            let bytes = std::fs::read(format!("{dir}/rar4_lz_four.part0{part}.rar")).unwrap();
            (format!("silver.horizon.part{part}.rar"), bytes)
        })
        .collect();
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let rev = Post::plain("silver.horizon.part1.rev", rar3_rev(&bytes));
    let mut scenario = Scenario::new(posted(volumes, &[2], vec![rev]));
    // Each volume over a few articles, as a post carries it: the fourth
    // volume's front reaches the engine before the third is rebuilt.
    scenario.article = 4096;
    let graph = run(&scenario, Engine::Graph).await;
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    let member = delivered("compressible.txt");
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec![member.clone()]
    );
    let oracle: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/extraction_profiles/expected.json"
    )))
    .unwrap();
    let expected = &oracle["archives"]["rar4_lz_four"]["compressible.txt"];
    assert_eq!(
        graph.files[&member].len() as u64,
        expected["size"].as_u64().unwrap()
    );
    assert_eq!(
        hex::encode(sha2::Sha256::digest(&graph.files[&member])),
        expected["sha256"].as_str().unwrap()
    );
}

#[tokio::test]
async fn rar_rev_rebuilds_two_absent_header_encrypted_volumes() {
    let body = payload(11 * ARTICLE, 61);
    let volumes = header_encrypted_store_set(MEMBER, &body, 5, SECRET, HeaderCheck::For(SECRET));
    let bytes: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let revs = rar5_revs("silver.horizon", &bytes, 2);
    let scenario = Scenario::new(posted(volumes, &[1, 2], revs)).password(SECRET);
    assert_rebuilt(&run_each(&scenario).await, &body, 2);
}

#[tokio::test]
async fn rar_rev_short_of_recovery_volumes_fails_the_job_and_delivers_nothing() {
    let (_, scenario) = rar5_set(&[1, 2], 1);
    let graph = run_each(&scenario).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?} {:?}",
        graph.status,
        graph.failure
    );
    assert!(graph.files.is_empty(), "{:?}", graph.files.keys());
    let delivery = &graph.graph().delivery;
    assert!(delivery.whole.is_empty(), "{delivery:?}");
    assert!(matches!(
        graph.graph().outcomes.as_slice(),
        [Some(RepairOutcome::Impossible {
            damaged: 2,
            reachable: 1
        })]
    ));
}
