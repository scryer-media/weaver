// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{Post, Scenario, par2_volumes_sliced, par3_set, payload, run};
use crate::jobs::model::JobStatus;

#[tokio::test]
async fn repaired_signature_supersedes_an_early_zip_name() {
    use sevenz_turbo::{EncoderConfiguration, EncoderMethod};
    let archive = super::sevenz::container(EncoderConfiguration::new(EncoderMethod::COPY), None);
    let mut posts = par2_volumes_sliced(
        "identity",
        &[("bundle.zip", &archive)],
        256,
        archive.len().div_ceil(256) as u32,
        2,
    );
    let source = posts.len() as u32;
    posts.push(Post::plain("bundle.zip", archive));
    let outcome = run(&Scenario::new(posts).damaged(source, 0)).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{:?}",
        outcome.graph
    );
    for (_, bytes) in super::sevenz::members() {
        assert!(outcome.files.values().any(|file| file == &bytes));
    }
}

#[tokio::test]
async fn swapped_names_with_one_damaged_file_repair_without_rename_collisions() {
    let a = payload(259, 101);
    let b = payload(259, 102);
    let mut damaged = b.clone();
    damaged[130] ^= 0x55;
    let mut posts = vec![
        Post::plain("a.bin", damaged),
        Post::plain("b.bin", a.clone()),
    ];
    posts.extend(par2_volumes_sliced(
        "identity",
        &[("a.bin", &a), ("b.bin", &b)],
        64,
        2,
        1,
    ));
    let scenario = Scenario::new(posts);
    let outcome = run(&scenario).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{:?}",
        outcome.graph
    );
    assert_eq!(outcome.files.len(), 2, "{:?}", outcome.files.keys());
    assert_eq!(outcome.files["lantern beacon/a.bin"], a);
    assert_eq!(outcome.files["lantern beacon/b.bin"], b);
}

#[tokio::test]
async fn truncated_uu_archive_is_repaired_before_decoder_retry() {
    use sevenz_turbo::{EncoderConfiguration, EncoderMethod};
    let archive = super::sevenz::container(EncoderConfiguration::new(EncoderMethod::COPY), None);
    let sources = [("bundle.7z", archive.as_slice())];
    let mut posts = vec![Post::plain(
        "bundle.7z",
        archive[..archive.len() - 12].to_vec(),
    )];
    posts.extend(par2_volumes_sliced("identity", &sources, 64, 4, 1));
    let scenario = Scenario::new(posts).uuencoded();
    let outcome = run(&scenario).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{:?}",
        outcome.graph
    );
    for (_, bytes) in super::sevenz::members() {
        assert!(outcome.files.values().any(|file| file == &bytes));
    }
    super::restart::restart_row(scenario).await;
}

// A plain RAR5 set over four volumes under PAR2. The first articles of the
// first two volumes come whole under checksums that disagree with them, so
// the store cannot read the second volume's head, and the clean third volume
// waits held behind it. The set reads the marked bytes back, finds them
// good, and rules the posting clean: its proof goes down again over the
// marked bytes, links the second volume, and reaches the held third. The
// member is whole and the job completes.
#[tokio::test]
async fn a_proof_reaches_held_bytes_of_a_volume_an_earlier_redelivery_linked() {
    let body = payload(4096, 151);
    let volumes =
        crate::pipeline::tests::direct_store::single_member_store_set("beacon.bin", &body, 4);
    let sources: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts = super::rar::posts(volumes.clone());
    posts.extend(par2_volumes_sliced(
        "proof",
        &sources,
        super::harness::ARTICLE as u64,
        8,
        2,
    ));
    let scenario = Scenario::new(posts).crc_wrong(0, 0).crc_wrong(1, 0);
    let outcome = run(&scenario).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "{:?} {:?}",
        outcome.failure,
        outcome.graph().rulings
    );
    assert!(
        outcome.files.values().any(|file| file == &body),
        "{:?}",
        outcome.files.keys()
    );
}

// One byte is silently wrong where only a whole file's checksum covers it:
// the last volume of a split RAR5 member, an unsplit RAR5 member, and a 7z
// archive. The PAR2 set made over the files as written repairs each, and
// the job completes.
#[tokio::test]
async fn silent_damage_only_a_whole_file_checksum_covers_is_repaired() {
    use sevenz_turbo::{EncoderConfiguration, EncoderMethod};
    let body = payload(4096, 152);
    let store = crate::pipeline::tests::direct_store::single_member_store_set;
    let archive = super::sevenz::container(EncoderConfiguration::new(EncoderMethod::COPY), None);
    let sevenz: Vec<Vec<u8>> = super::sevenz::members()
        .into_iter()
        .map(|(_, bytes)| bytes)
        .collect();
    let kinds = [
        ("split", store("beacon.bin", &body, 4), vec![body.clone()]),
        ("unsplit", store("beacon.bin", &body, 1), vec![body.clone()]),
        ("7z", vec![("bundle.7z".to_string(), archive)], sevenz),
    ];
    for (kind, files, expected) in kinds {
        let sources: Vec<(&str, &[u8])> = files
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        let recovery = par2_volumes_sliced("proof", &sources, super::harness::ARTICLE as u64, 8, 2);
        let mut posted = files.clone();
        let last = posted.last_mut().unwrap();
        let at = last.1.len() / 2;
        last.1[at] ^= 0x5a;
        let mut posts: Vec<Post> = posted
            .into_iter()
            .map(|(name, bytes)| Post::plain(&name, bytes))
            .collect();
        posts.extend(recovery);
        let outcome = run(&Scenario::new(posts)).await;
        assert_eq!(
            outcome.status,
            Some(JobStatus::Complete),
            "{kind}: {:?} {:?}",
            outcome.failure,
            outcome.graph().rulings
        );
        for bytes in expected {
            assert!(
                outcome.files.values().any(|file| file == &bytes),
                "{kind}: {:?}",
                outcome.files.keys()
            );
        }
    }
}

// The last article of a recovery carrier decodes 100 bytes short of the
// part end its header declares (half of itself, when it is shorter than
// twice that), as a server that cut the body off serves
// it. The root states the missing tail lost, as it states a lost article,
// so each kind steps over it at once: the job rules and completes, and
// never waits on bytes nothing will bring. Each kind's source needs the
// carrier: a damaged article for PAR2 and PAR3, an absent volume for
// `.rev`.
#[tokio::test]
async fn a_truncated_carrier_article_is_lost_for_every_recovery_kind() {
    const FRAME: usize = 512;
    const SHORT: usize = 100;
    let body = payload(6 * FRAME, 141);
    let source = [("payload.bin", body.as_slice())];
    // The set's carriers, and the set with the source posted after it.
    let with_source = |mut posts: Vec<Post>| {
        let carriers: Vec<u32> = (0..posts.len() as u32)
            .filter(|at| posts[*at as usize].name.contains(".vol"))
            .collect();
        let at = posts.len() as u32;
        posts.push(Post::plain("payload.bin", body.clone()));
        (posts, carriers, at)
    };
    let (par2, par2_carriers, par2_source) =
        with_source(par2_volumes_sliced("carrier", &source, FRAME as u64, 4, 1));
    let (par3, par3_carriers, par3_source) = with_source(par3_set("carrier", &source, 24));
    let rev_body = super::rar_rev::body();
    let volumes =
        crate::pipeline::tests::direct_store::single_member_store_set("beacon.bin", &rev_body, 4);
    let widths: Vec<&[u8]> = volumes.iter().map(|(_, bytes)| bytes.as_slice()).collect();
    let revs = super::rar_rev::rar5_revs("silver.horizon", &widths, 2);
    let mut rev = super::rar::posts(
        volumes
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != 1)
            .map(|(_, volume)| volume.clone())
            .collect(),
    );
    let first_rev = rev.len() as u32;
    rev.extend(revs);
    let kinds = [
        ("PAR2", par2, par2_carriers, Some(par2_source), body.clone()),
        ("PAR3", par3, par3_carriers, Some(par3_source), body.clone()),
        (".rev", rev, vec![first_rev], None, rev_body),
    ];
    for (kind, posts, carriers, source, expected) in kinds {
        let mut cut = Vec::new();
        let mut by = Vec::new();
        for carrier in &carriers {
            let post = &posts[*carrier as usize];
            let last = post.articles(FRAME).count() as u32 - 1;
            let tail = post.articles(FRAME).last().unwrap().len();
            // A tail shorter than the cut loses half of itself instead.
            let short = SHORT.min(tail / 2);
            assert!(short > 0, "{kind}: {} ends in {tail} bytes", post.name);
            cut.push((*carrier, last));
            by.push(short);
        }
        let mut scenario = Scenario::new(posts).framed(FRAME);
        for ((carrier, last), short) in cut.iter().zip(&by) {
            scenario = scenario.short(*carrier, *last, *short);
        }
        if let Some(source) = source {
            scenario = scenario.damaged(source, 1);
        }
        let outcome = run(&scenario).await;
        assert!(
            cut.iter()
                .any(|article| outcome.requests.contains_key(article)),
            "{kind}: no cut article was fetched: {:?}",
            outcome.order
        );
        assert_eq!(
            outcome.status,
            Some(JobStatus::Complete),
            "{kind}: {:?} {:?}",
            outcome.failure,
            outcome.graph().rulings
        );
        assert!(
            outcome.files.values().any(|file| file == &expected),
            "{kind}: {:?}",
            outcome.files.keys()
        );
    }
}
