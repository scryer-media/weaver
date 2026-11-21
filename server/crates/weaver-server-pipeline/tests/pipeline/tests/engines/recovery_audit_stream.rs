// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{Post, Scenario, par3_set, payload};
use crate::jobs::model::JobStatus;

use super::wire_recovery::drive_stream;
pub(super) use super::wire_recovery::explicit_stream;
use crate::pipeline::tests::direct_store::scenarios::sevenz_store::{
    Entry, build_7z_shaped, split_volumes,
};
use crate::pipeline::tests::direct_store::{multi_member_store_set, single_member_rar4_store_set};

#[tokio::test]
async fn reposted_solid_rar_after_nonfirst_volume_crash_is_complete() {
    let volumes = [
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../weaver-server-core/tests/fixtures/extraction_profiles/rar4_solid_two.part01.rar"
        ))
        .as_slice(),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../weaver-server-core/tests/fixtures/extraction_profiles/rar4_solid_two.part02.rar"
        ))
        .as_slice(),
    ];
    let mut reader = unrar_rs::RarArchive::open(std::io::Cursor::new(volumes[0].to_vec())).unwrap();
    reader
        .add_volume(1, Box::new(std::io::Cursor::new(volumes[1].to_vec())))
        .unwrap();
    let mut expected = Vec::new();
    for index in 0..reader.len() {
        let name = reader.member_info(index).unwrap().name.clone();
        let mut bytes = Vec::new();
        reader.by_index(index).unwrap().copy_to(&mut bytes).unwrap();
        expected.push((name, bytes));
    }
    assert_eq!(expected.len(), 3);
    let frame = volumes.iter().map(|bytes| bytes.len()).max().unwrap();
    let posts = [0, 1, 0, 1]
        .into_iter()
        .enumerate()
        .map(|(index, volume)| {
            Post::plain(
                &format!("{:032x}", 0xd1c7_0000_u128 + index as u128),
                volumes[volume].to_vec(),
            )
        })
        .collect();
    let scenario = Scenario::new(posts).framed(frame);
    let restored = explicit_crash(&scenario, &[(1, 0), (0, 0), (2, 0), (3, 0)], 1).await;
    assert_eq!(
        restored.status,
        Some(JobStatus::Complete),
        "delivery={:?}, names={:?}",
        restored.graph.delivery,
        restored.files.keys().collect::<Vec<_>>()
    );
    for (name, bytes) in expected {
        assert!(
            restored.files.get(&format!("lantern beacon/{name}")) == Some(&bytes),
            "{name}: byte-exact output"
        );
    }
}

#[tokio::test]
async fn conflicting_transport_copies_of_sevenz_front_are_repairable() {
    let mut state = 29_u64;
    let body: Vec<_> = (0..97_792)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 56) as u8
        })
        .collect();
    let archive = build_7z_shaped(
        &[Entry::file("lantern.mkv", body.clone())],
        sevenz_turbo::EncoderMethod::COPY,
        None,
        false,
    );
    let written = split_volumes(&archive, 4);
    let descriptions: Vec<_> = written
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let recovery = super::harness::par2_volumes_sliced("archive", &descriptions, 1048, 2, 2);
    let mut posts: Vec<_> = written
        .into_iter()
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect();
    let articles: Vec<_> = posts
        .iter()
        .enumerate()
        .flat_map(|(file, post)| {
            (0..post.bytes.len().div_ceil(768)).map(move |article| (file as u32, article as u32))
        })
        .collect();
    // A repeated first batch, then the tail before the preceding batch:
    // conflicting transport copies must not make ownership depend on order.
    let groups: Vec<_> = articles.chunks(articles.len().div_ceil(4)).collect();
    let order: Vec<_> = [0, 0, 1, 3, 2]
        .into_iter()
        .flat_map(|index| groups[index].iter().copied())
        .collect();
    posts.extend(recovery);
    let scenario = Scenario::new(posts).framed(768);
    let outcome = explicit_stream(&scenario, &order, None, &[(0, 0), (0, 1)]).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "delivery={:?}, rulings={:?}",
        outcome.graph.delivery,
        outcome.graph.rulings
    );
    assert_eq!(outcome.files.len(), 1);
    assert!(
        outcome.files.values().any(|bytes| bytes == &body),
        "byte-exact repaired payload"
    );
}

fn par3_batch_carriers(files: &[(&str, &[u8])], blocks: u64) -> Vec<Post> {
    use par3_rs::creation::{CreationOptions, CreationPlan, CreationSource, VolumeLayout};
    use par3_rs::source::{MemorySourceAccess, SourceId};
    let mut access = MemorySourceAccess::default();
    let sources: Vec<_> = files
        .iter()
        .enumerate()
        .map(|(index, (name, bytes))| {
            let source = SourceId(index as u64 + 1);
            access.insert(source, 1, bytes.to_vec().into());
            CreationSource {
                name: (*name).into(),
                source,
            }
        })
        .collect();
    let plan = CreationPlan::build(
        std::sync::Arc::new(access),
        &sources,
        CreationOptions {
            block_size: 1048,
            recovery_count: blocks,
            volumes: VolumeLayout::Uniform(blocks.div_ceil(4)),
            ..Default::default()
        },
    )
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut written = plan
        .execute(&directory.path().join("archive"), directory.path())
        .unwrap();
    written.sort_by_key(|path| path.file_name().unwrap().to_string_lossy().contains(".vol"));
    written
        .into_iter()
        .map(|path| {
            Post::plain(
                path.file_name().unwrap().to_str().unwrap(),
                std::fs::read(&path).unwrap(),
            )
        })
        .collect()
}

async fn repair_missing_piece_and_silent_damage(par3: bool, broad: bool) {
    let body = payload(97_792, 29);
    let archive = build_7z_shaped(
        &[Entry::file("lantern.mkv", body.clone())],
        sevenz_turbo::EncoderMethod::COPY,
        None,
        false,
    );
    let written = split_volumes(&archive, 4);
    let blocks = written[1].1.len().div_ceil(1048) as u32 + if broad { 28 } else { 3 };
    let descriptions: Vec<_> = written
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let recovery = if par3 {
        par3_batch_carriers(&descriptions, u64::from(blocks))
    } else {
        super::harness::par2_volumes_sliced("archive", &descriptions, 1048, blocks, 4)
    };
    let mut posts: Vec<_> = written
        .into_iter()
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect();
    if broad {
        // Twelve transport-valid corrupt articles span the three retained
        // pieces. Filling the missing piece closes the output before the
        // later pieces' damaged slices have all been replaced.
        for file in [0, 2, 3] {
            for article in 6..10 {
                for byte in &mut posts[file].bytes[article * 768..(article + 1) * 768] {
                    *byte ^= 0x5a;
                }
            }
        }
    } else {
        let middle = posts[3].bytes.len().div_ceil(768) / 2;
        for byte in &mut posts[3].bytes[middle * 768..(middle + 1) * 768] {
            *byte ^= 0x5a;
        }
    }
    let lost = (0..posts[1].bytes.len().div_ceil(768))
        .map(|article| (1, article as u32))
        .collect();
    let articles: Vec<_> = posts
        .iter()
        .enumerate()
        .flat_map(|(file, post)| {
            (0..post.bytes.len().div_ceil(768)).map(move |article| (file as u32, article as u32))
        })
        .collect();
    let groups: Vec<_> = articles.chunks(articles.len().div_ceil(4)).collect();
    let order: Vec<_> = [0, 2, 3, 3, 1]
        .into_iter()
        .flat_map(|index| groups[index].iter().copied())
        .collect();
    posts.extend(recovery);
    let mut scenario = Scenario::new(posts).framed(768);
    scenario.lost = lost;
    let outcome = explicit_stream(&scenario, &order, None, &[]).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "delivery={:?}, rulings={:?}",
        outcome.graph.delivery,
        outcome.graph.rulings
    );
    assert_eq!(outcome.files.len(), 1);
    assert!(
        outcome.files.values().any(|bytes| bytes == &body),
        "byte-exact repaired payload"
    );
}

#[tokio::test]
async fn a_missing_sevenz_piece_and_silent_damage_are_repaired_together() {
    repair_missing_piece_and_silent_damage(false, false).await;
}

#[tokio::test]
async fn par3_repairs_a_missing_piece_before_later_silent_damage() {
    repair_missing_piece_and_silent_damage(true, false).await;
}

#[tokio::test]
async fn par2_repairs_a_missing_piece_and_twelve_corrupt_articles() {
    repair_missing_piece_and_silent_damage(false, true).await;
}

#[tokio::test]
async fn par3_repairs_a_missing_piece_and_twelve_corrupt_articles() {
    repair_missing_piece_and_silent_damage(true, true).await;
}

#[tokio::test]
async fn two_independent_multi_member_sets_keep_their_owners_after_crash() {
    let mut posts = Vec::new();
    let mut sidecars = Vec::new();
    let mut expected = Vec::new();
    for (prefix, first, second, seed) in [
        (
            "silver.horizon",
            "harbour/lantern.mkv",
            "harbour/beacon.mkv",
            41,
        ),
        (
            "amber.lantern",
            "harbour/amber.mkv",
            "harbour/ember.mkv",
            48,
        ),
    ] {
        let members = vec![
            (first, payload(97_792, seed)),
            (second, payload(32_597, seed + 12)),
        ];
        let mut volumes = multi_member_store_set(&members, 4);
        for (name, _) in &mut volumes {
            *name = name.replace("silver.horizon", prefix);
        }
        let descriptions: Vec<_> = volumes
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        sidecars.extend(par3_set(prefix, &descriptions, 2));
        posts.extend(
            volumes
                .into_iter()
                .map(|(name, bytes)| Post::plain(&name, bytes)),
        );
        expected.extend(
            members
                .into_iter()
                .map(|(name, bytes)| (name.to_string(), bytes)),
        );
    }
    let articles: Vec<_> = posts
        .iter()
        .enumerate()
        .flat_map(|(file, post)| {
            (0..post.bytes.len().div_ceil(768)).map(move |article| (file as u32, article as u32))
        })
        .collect();
    let groups: Vec<_> = articles.chunks(articles.len().div_ceil(4)).collect();
    let first: Vec<_> = [1, 2, 3, 0]
        .into_iter()
        .flat_map(|index| groups[index].iter().copied())
        .collect();
    let crash_after = groups[1].len();
    posts.extend(sidecars);
    let scenario = Scenario::new(posts).framed(768).first(&first);
    let restored = explicit_crash(&scenario, &first, crash_after).await;
    assert_eq!(
        restored.status,
        Some(JobStatus::Complete),
        "delivery={:?}, rulings={:?}, names={:?}",
        restored.graph.delivery,
        restored.graph.rulings,
        restored.files.keys().collect::<Vec<_>>()
    );
    assert_eq!(restored.files.len(), expected.len());
    for (name, bytes) in expected {
        assert!(
            restored.files.get(&format!("lantern beacon/{name}")) == Some(&bytes),
            "{name}: byte-exact output; names={:?}",
            restored.files.keys().collect::<Vec<_>>()
        );
    }
}

async fn explicit_crash(
    scenario: &Scenario,
    articles: &[(u32, u32)],
    after: usize,
) -> super::restart::Restarted {
    explicit_stream(scenario, articles, Some(after), &[]).await
}

#[tokio::test]
async fn an_opaque_stored_archive_with_unrepairable_payload_is_not_usable() {
    let mut state = 13_u64;
    let bytes = (0..70_001)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 56) as u8
        })
        .collect();
    let archive = build_7z_shaped(
        &[Entry::file("feature.mkv", bytes)],
        sevenz_turbo::EncoderMethod::COPY,
        None,
        false,
    );
    let frame = archive.len().div_ceil(4);
    let slice = archive.len().div_ceil(32).div_ceil(4) * 4;
    let index = super::super::build_test_par2_index_for_files(
        &[("silver.horizon.7z", &archive)],
        slice as u64,
    );
    let posts = vec![
        Post::plain("5f0c9e2ab1d74c6e8a3f1b0d9c2e7a41", archive),
        Post::plain("silver.horizon.par2", index),
    ];
    let order: Vec<_> = (0..posts[1].bytes.len().div_ceil(frame))
        .map(|ordinal| (1, ordinal as u32))
        .chain((0..4).map(|ordinal| (0, ordinal)))
        .collect();
    let scenario = Scenario::new(posts).framed(frame).lost(0, 1);
    let outcome = drive_stream(&scenario, &order, None, &[], false).await;
    assert!(
        matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "status={:?}, delivery={:?}, rulings={:?}, files={:?}",
        outcome.status,
        outcome.graph.delivery,
        outcome.graph.rulings,
        outcome.files.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_recovery_named_rar_gap_restores_without_publishing_a_piece() {
    let bytes = payload(70_001, 13);
    let written = single_member_rar4_store_set("nested/feature.mkv", &bytes, 3);
    let frame = written
        .iter()
        .map(|(_, bytes)| bytes.len().div_ceil(2))
        .max()
        .unwrap();
    let descriptions: Vec<_> = written
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts = vec![
        Post::plain("5f0c9e2ab1d74c6e8a3f1b0d9c2e7a41.001", written[0].1.clone()),
        Post::plain("5f0c9e2ab1d74c6e8a3f1b0d9c2e7a41.003", written[2].1.clone()),
    ];
    let slice = (written
        .iter()
        .map(|(_, bytes)| bytes.len())
        .max()
        .unwrap()
        .div_ceil(32)
        .div_ceil(4)
        * 4)
    .max(256);
    let blocks = written
        .iter()
        .map(|(_, bytes)| bytes.len().div_ceil(slice) as u32)
        .sum();
    posts.extend(super::harness::par2_volumes_sliced(
        "archive",
        &descriptions,
        slice as u64,
        blocks,
        1,
    ));
    let mut order: Vec<_> = (0..posts[2].bytes.len().div_ceil(frame))
        .map(|article| (2, article as u32))
        .collect();
    let after = order.len() + 2;
    order.extend([(0, 1), (1, 0), (1, 1), (0, 0)]);
    let scenario = Scenario::new(posts).framed(frame);
    let restored = explicit_crash(&scenario, &order, after).await;
    assert_eq!(
        restored.status,
        Some(JobStatus::Complete),
        "delivery={:?}, rulings={:?}, names={:?}",
        restored.graph.delivery,
        restored.graph.rulings,
        restored.files.keys().collect::<Vec<_>>()
    );
    assert_eq!(
        restored.files.len(),
        1,
        "only the extracted member is delivered"
    );
    assert!(
        restored.files.values().any(|file| file == &bytes),
        "byte-exact extracted member"
    );
}

// Two pieces of a 7z, each posted twice under hash names, with nothing that
// names them. Ruling 2026-10-30 3:50 PM MDT: pieces no name numbers and no
// recovery set covers are never grouped, so each goes as posted.
fn hash_named_pieces_posted_twice(base: u128) -> (Vec<Post>, Vec<Vec<u8>>, usize) {
    let archive = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/extraction_profiles/sevenz_solid.7z"
    ));
    let cut = archive.len().div_ceil(2);
    let pieces = [archive[..cut].to_vec(), archive[cut..].to_vec()];
    let posts = [0, 1, 0, 1]
        .into_iter()
        .enumerate()
        .map(|(index, piece)| {
            Post::plain(
                &format!("{:032x}", base + index as u128),
                pieces[piece].clone(),
            )
        })
        .collect();
    (posts, pieces.to_vec(), cut)
}

// The first piece's second posting folds into its first, and it is
// withheld; the second piece's two postings are two plain files. Nothing is
// joined and no file comes twice under one name. No whole file has a known
// type, so the job fails and its files stay where they were fetched.
fn assert_each_piece_as_posted(
    outcome: &super::restart::Restarted,
    base: u128,
    pieces: &[Vec<u8>],
) {
    let name = |index: u128| format!("{:032x}", base + index);
    assert!(
        matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "status={:?} delivery={:?}",
        outcome.status,
        outcome.graph.delivery
    );
    assert!(
        !outcome.graph.kinds.iter().any(|kind| kind == "split"),
        "{:?}",
        outcome.graph.kinds
    );
    let retained = |index: u128| {
        outcome
            .retained_files
            .iter()
            .find(|(path, _)| path.rsplit('/').next() == Some(name(index).as_str()))
            .map(|(_, bytes)| bytes)
    };
    assert_eq!(
        retained(1),
        Some(&pieces[1]),
        "{:?}",
        outcome.retained_files.keys()
    );
    assert_eq!(
        retained(3),
        Some(&pieces[1]),
        "{:?}",
        outcome.retained_files.keys()
    );
    let joined = pieces.concat();
    assert!(
        !outcome
            .files
            .values()
            .chain(outcome.retained_files.values())
            .any(|file| *file == joined),
        "nothing joined"
    );
    assert_eq!(
        format!("{:?}", outcome.graph.delivery.withheld),
        format!("[({:?}, Unmapped)]", name(0)),
        "delivery={:?}",
        outcome.graph.delivery
    );
    assert_eq!(outcome.graph.delivery.whole, [name(1), name(3)]);
    let names: Vec<_> = outcome
        .files
        .keys()
        .chain(outcome.retained_files.keys())
        .cloned()
        .collect();
    assert!(
        names
            .iter()
            .all(|file| !file.contains(" (1)") && !file.ends_with(".bin")),
        "nothing joined, no second incarnation: {names:?}"
    );
}

#[tokio::test]
async fn reposted_hash_named_pieces_after_crash_go_as_posted() {
    let base = 0xd1c7_0000_u128;
    let (posts, pieces, cut) = hash_named_pieces_posted_twice(base);
    let scenario = Scenario::new(posts)
        .framed(cut)
        .first(&[(0, 0), (1, 0), (2, 0), (3, 0)]);
    let restored = explicit_crash(&scenario, &[(0, 0), (1, 0), (2, 0), (3, 0)], 1).await;
    assert_each_piece_as_posted(&restored, base, &pieces);
}

// The same, with the job restarted part way through the first postings.
#[tokio::test]
async fn a_restored_repost_of_hash_named_pieces_goes_as_posted() {
    let base = 0xd1c8_0000_u128;
    let (posts, pieces, cut) = hash_named_pieces_posted_twice(base);
    let scenario = Scenario::new(posts).framed(cut.div_ceil(2));
    let order = [
        (0, 0),
        (0, 1),
        (1, 0),
        (1, 1),
        (2, 0),
        (2, 1),
        (3, 0),
        (3, 1),
    ];
    let restored = explicit_crash(&scenario, &order, 3).await;
    assert_each_piece_as_posted(&restored, base, &pieces);
}

async fn late_rar_index_after_crash(lost_tail: bool) {
    let body: Vec<_> = (0..70_001)
        .map(|n| ((n * 7 + n / 251) % 253) as u8)
        .collect();
    let volumes = single_member_rar4_store_set("nested/feature.mkv", &body, 2);
    let frame = volumes
        .iter()
        .map(|(_, bytes)| bytes.len().div_ceil(2))
        .max()
        .unwrap();
    let sources: Vec<_> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let slice = (volumes
        .iter()
        .map(|(_, bytes)| bytes.len())
        .max()
        .unwrap()
        .div_ceil(32)
        .div_ceil(4)
        * 4)
    .max(256);
    let blocks: usize = volumes
        .iter()
        .map(|(_, bytes)| bytes.len().div_ceil(slice))
        .sum();
    let mut posts: Vec<_> = crate::pipeline::tests::direct_store::obfuscate_volumes(&volumes)
        .into_iter()
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect();
    let index = crate::pipeline::tests::direct_store::build_test_par2_with_recovery(
        &sources,
        slice as u64,
        if lost_tail { blocks } else { 0 },
    );
    // The healthy restart has descriptions only; parity must not hide lost backing.
    posts.push(Post::plain("silver.horizon.par2", index));
    let mut scenario = Scenario::new(posts).framed(frame);
    if lost_tail {
        scenario.lost.push((1, 1));
    }
    let outcome = explicit_crash(&scenario, &[(1, 0), (0, 0), (0, 1), (1, 1)], 1).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "delivery={:?}, rulings={:?}",
        outcome.graph.delivery,
        outcome.graph.rulings
    );
    assert_eq!(
        outcome.files.len(),
        1,
        "only the extracted member is delivered"
    );
    assert!(
        outcome.files.values().any(|bytes| bytes == &body),
        "byte-exact member after late recovery identity"
    );
}

#[tokio::test]
async fn late_rar_index_preserves_restored_envelope_bytes() {
    late_rar_index_after_crash(false).await;
}

#[tokio::test]
async fn late_rar_index_repairs_a_missing_tail_after_crash() {
    late_rar_index_after_crash(true).await;
}

#[tokio::test]
async fn sfv_does_not_displace_restored_solid_rar_identity() {
    let volumes = [
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../weaver-server-core/tests/fixtures/extraction_profiles/rar4_solid_two.part01.rar"
        ))
        .as_slice(),
        include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../weaver-server-core/tests/fixtures/extraction_profiles/rar4_solid_two.part02.rar"
        ))
        .as_slice(),
    ];
    let mut reader = unrar_rs::RarArchive::open(std::io::Cursor::new(volumes[0].to_vec())).unwrap();
    reader
        .add_volume(1, Box::new(std::io::Cursor::new(volumes[1].to_vec())))
        .unwrap();
    let mut expected = Vec::new();
    for index in 0..reader.len() {
        let name = reader.member_info(index).unwrap().name.clone();
        let mut bytes = Vec::new();
        reader.by_index(index).unwrap().copy_to(&mut bytes).unwrap();
        expected.push((name, bytes));
    }
    let names = [
        "5f0c9e2ab1d74c6e8a3f1b0d9c2e7a41.002",
        "5f0c9e2ab1d74c6e8a3f1b0d9c2e7a41.001",
    ];
    let frame = volumes
        .iter()
        .map(|bytes| bytes.len().div_ceil(2))
        .max()
        .unwrap();
    let sfv = names
        .iter()
        .zip(volumes)
        .map(|(name, bytes)| format!("{name} {:08X}\r\n", par2_rs::checksum::crc32(bytes)))
        .collect::<String>();
    let mut posts: Vec<_> = names
        .iter()
        .zip(volumes)
        .map(|(name, bytes)| Post::plain(name, bytes.to_vec()))
        .collect();
    posts.push(Post::plain("archive.sfv", sfv.into_bytes()));
    let mut order: Vec<_> = (0..posts[2].bytes.len().div_ceil(frame))
        .map(|part| (2, part as u32))
        .collect();
    order.push((1, 0));
    let crash_after = order.len();
    order.extend([(1, 1), (0, 1), (0, 0)]);
    let outcome = explicit_crash(&Scenario::new(posts).framed(frame), &order, crash_after).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "delivery={:?}, names={:?}",
        outcome.graph.delivery,
        outcome.files.keys().collect::<Vec<_>>()
    );
    for (name, bytes) in expected {
        assert!(
            outcome.files.get(&format!("lantern beacon/{name}")) == Some(&bytes),
            "{name} is byte-exact after restore"
        );
    }
}

#[tokio::test]
async fn recovered_reposts_with_no_original_front_have_one_usable_archive() {
    let archive = include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/extraction_profiles/sevenz_solid.7z"
    ));
    let cut = archive.len().div_ceil(2);
    let pieces = [&archive[..cut], &archive[cut..]];
    let mut posts: Vec<_> = [0, 1, 0, 1]
        .into_iter()
        .enumerate()
        .map(|(index, piece)| {
            Post::plain(
                &format!("{:032x}", 0x91c0_u128 + index as u128),
                pieces[piece].to_vec(),
            )
        })
        .collect();
    let sources: Vec<_> = posts
        .iter()
        .map(|post| (post.name.as_str(), post.bytes.as_slice()))
        .collect();
    let blocks = posts
        .iter()
        .map(|post| post.bytes.len().div_ceil(256) as u32)
        .sum();
    let recovery = super::harness::par2_volumes_sliced("archive", &sources, 256, blocks, 2);
    posts.extend(recovery);
    let mut scenario = Scenario::new(posts).framed(cut);
    scenario.lost = (0..4).map(|file| (file, 0)).collect();
    let outcome = explicit_stream(&scenario, &[], None, &[]).await;
    assert_eq!(
        outcome.status,
        Some(JobStatus::Complete),
        "delivery={:?}, names={:?}",
        outcome.graph.delivery,
        outcome.files.keys().collect::<Vec<_>>()
    );
    assert!(
        outcome.graph.delivery.damaged.is_empty(),
        "no reposted volume stays a damaged leaf: {:?}",
        outcome.graph.delivery
    );
    for index in 0..3 {
        let expected: Vec<_> = (0..8193)
            .map(|n| ((n * 7 + n / 251 + index * 13) % 253) as u8)
            .collect();
        assert!(
            outcome
                .files
                .get(&format!("lantern beacon/part{index}.mkv"))
                == Some(&expected),
            "part{index}.mkv is byte-exact"
        );
    }
    assert!(
        outcome.files.keys().all(|name| !name.contains("(1)")),
        "no duplicate extracted incarnation"
    );
}
