// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_graph::Clean;

use super::*;

const NAME: &str = "k3x9q0.bin";
const HOLE: usize = 64;
const RUN: usize = 16 * 1024;

// The index and one volume of a set over `seed`'s payload.
fn set(seed: u64) -> (Vec<(String, Vec<u8>)>, Vec<u8>) {
    let data = payload(seed, 5000);
    let files = par2_set("fixture.release", &[("payload.bin", &data)], BLOCK, 4, 1);
    let volume = files
        .iter()
        .find(|(name, _)| name.contains(".vol"))
        .map(|(_, bytes)| bytes.clone())
        .expect("a volume");
    let index = files
        .into_iter()
        .filter(|(name, _)| !name.contains(".vol"))
        .collect();
    (index, volume)
}

// What the namespace keeps of a volume whose first article was lost.
fn past_hole(volume: &[u8]) -> &[u8] {
    &volume[HOLE..volume.len().min(HOLE + RUN)]
}

fn claims(job: &mut Job<Par2>, bytes: &[u8]) -> bool {
    let clean = Clean::run(HOLE as u64, bytes);
    !job.object.claim(NAME, clean, None).is_empty()
}

// Where the first whole packet past the hole starts, and its length.
fn first_packet(run: &[u8]) -> (usize, usize) {
    let at = run
        .windows(8)
        .position(|window| window == b"PAR2\0PKT")
        .expect("a packet past the hole");
    let length = u64::from_le_bytes(run[at + 8..at + 16].try_into().unwrap()) as usize;
    assert!(at + length <= run.len(), "the packet is whole in the run");
    (at, length)
}

#[test]
fn a_volume_of_the_set_with_no_front_is_claimed_by_a_packet_past_its_hole() {
    let (index, volume) = set(61);
    let mut job = job(&index);
    assert!(claims(&mut job, past_hole(&volume)));
}

// An object that holds no set yet, built to judge a member past its hole
// when no file of the set opened on its own packets: a packet whose MD5
// passes founds the set, whatever set it is, and bytes that hold no such
// packet claim nothing.
#[test]
fn a_volume_with_no_front_founds_its_set_by_a_packet_past_its_hole() {
    let (_, volume) = set(61);
    assert!(
        claims(&mut job(&[]), past_hole(&volume)),
        "a packet that passes"
    );
    assert!(
        !claims(&mut job(&[]), &payload(12, RUN)),
        "no packet at all"
    );
    let run = past_hole(&volume);
    let (at, length) = first_packet(run);
    let mut spoiled = run.to_vec();
    spoiled.truncate(at + length);
    spoiled[at + length - 1] ^= 0x5a;
    assert!(
        !claims(&mut job(&[]), &spoiled),
        "a packet that fails its MD5"
    );
}

#[test]
fn a_volume_of_another_set_with_no_front_is_not_claimed() {
    let (index, _) = set(61);
    let (_, theirs) = set(62);
    let mut job = job(&index);
    assert!(!claims(&mut job, past_hole(&theirs)));
}

// A file that is no recovery data, holding a packet of the set with one
// byte of its body changed: the packet's MD5 fails, so nothing claims it.
// The same packet unchanged inside the same bytes is the set's own data,
// whole and checked, and claims it.
#[test]
fn a_packet_whose_md5_fails_claims_nothing() {
    let (index, volume) = set(61);
    let run = past_hole(&volume);
    let (at, length) = first_packet(run);
    let packet = &run[at..at + length];
    let mut file = payload(9, 4096);
    file.extend_from_slice(packet);
    file.extend_from_slice(&payload(10, 2048));
    let mut job = job(&index);
    let mut spoiled = file.clone();
    spoiled[4096 + length - 1] ^= 0x5a;
    assert!(!claims(&mut job, &spoiled), "a packet that fails its MD5");
    assert!(claims(&mut job, &file), "the same packet whole");
}

// The magic and a header of the set's id, with no packet behind them that
// the bytes hold whole, claim nothing.
#[test]
fn magic_alone_claims_nothing() {
    let (index, volume) = set(61);
    let run = past_hole(&volume);
    let (at, length) = first_packet(run);
    let mut job = job(&index);
    let mut file = payload(9, 4096);
    file.extend_from_slice(b"PAR2\0PKT");
    file.extend_from_slice(&payload(11, 512));
    assert!(!claims(&mut job, &file), "magic alone");
    let mut cut = payload(9, 4096);
    cut.extend_from_slice(&run[at..at + length - 1]);
    assert!(!claims(&mut job, &cut), "a packet the run's end cuts");
}
