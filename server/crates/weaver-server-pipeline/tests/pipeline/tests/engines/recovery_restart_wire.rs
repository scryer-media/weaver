// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{self, JOB, Post};
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::{
    build_test_par2_with_recovery, direct_store_job_spec, obfuscate_volumes,
    single_member_rar4_store_set,
};
use weaver_graph::{ByteRange, MemberId};
use weaver_server_pipeline::pipeline::tests::{
    engines::{
        recovery_actor::status as job_status_for_assert,
        recovery_restart_wire::{
            article, article_in, node_kind, read_root_member, restore_from_database,
            saved_checkpoint, settle, settle_in, volume_owner,
        },
    },
    insert_active_job, new_direct_pipeline,
    queue_fixture::{flush_checkpoint, working_dir},
    retire_pipeline_database,
};

async fn late_rar_index(lost_tail: bool) {
    let lost = lost_tail.then_some((1, 1));
    let payload: Vec<_> = (0..70_001)
        .map(|n| ((n * 7 + n / 251) % 253) as u8)
        .collect();
    let written = single_member_rar4_store_set("nested/feature.mkv", &payload, 2);
    let slice = (written
        .iter()
        .map(|(_, bytes)| bytes.len())
        .max()
        .unwrap()
        .div_ceil(32)
        .div_ceil(4)
        * 4)
    .max(256);
    let blocks = if lost_tail {
        written
            .iter()
            .map(|(_, bytes)| bytes.len().div_ceil(slice))
            .sum()
    } else {
        0
    };
    let sources: Vec<_> = written
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let parity = build_test_par2_with_recovery(&sources, slice as u64, blocks);
    let mut posts = obfuscate_volumes(&written);
    let mut spec = direct_store_job_spec("Archive schedules", &posts);
    let mut carrier = harness::spec(
        "",
        &[Post::plain("silver.horizon.par2", parity.clone())],
        usize::MAX,
    )
    .files
    .remove(0);
    carrier.segments[0].bytes += carrier.segments[0].bytes.div_ceil(32) + 2;
    spec.total_bytes += u64::from(carrier.segments[0].bytes);
    spec.files.push(carrier);
    posts.push(("silver.horizon.par2".into(), parity));
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete) = new_direct_pipeline(&root).await;
    // No scenario preparation: the pool has no server and no connections.
    insert_active_job(&mut pipeline, JOB, spec.clone()).await;
    article(&mut pipeline, &posts, 1, 0, 2, lost).await;
    let owner =
        volume_owner(&pipeline, MemberId(1)).expect("the nonfirst article has an inverse owner");
    let kind = node_kind(&pipeline, owner.0);
    let working = working_dir(&pipeline, JOB).clone();
    let extent = ByteRange::new(0, posts[1].1.len().div_ceil(2) as u64);
    let mut before = Vec::new();
    assert!(read_root_member(
        &pipeline,
        MemberId(1),
        extent,
        &mut before
    ));
    assert_eq!(before, posts[1].1[..extent.end as usize]);
    flush_checkpoint(&mut pipeline, JOB);
    retire_pipeline_database(pipeline).await;
    (pipeline, _, _) = new_direct_pipeline(&root).await;
    let checkpoint = saved_checkpoint(&pipeline).expect("the crash left a graph checkpoint");
    let (saved_kind, saved_parent) = checkpoint
        .owner(owner.0)
        .expect("the inverse owner is recorded");
    assert_eq!(saved_kind, kind);
    assert_eq!(saved_parent, Some(0));
    let envelopes = checkpoint.envelopes(owner.0);
    eprintln!(
        "crash custody owner={owner:?} kind={kind} extent={extent:?} envelopes={envelopes:?} root={}",
        checkpoint.root_member_debug(MemberId(1)),
    );
    assert!(
        !envelopes.is_empty(),
        "the owner recorded envelope backing: {checkpoint:?}"
    );
    for kept in envelopes {
        let bytes = std::fs::read(working.join(&kept.name))
            .expect("a recorded envelope exists after the crash");
        assert!(kept.start >= 0 && kept.end <= extent.end as i64);
        let range = kept.start as usize..kept.end as usize;
        assert_eq!(
            &bytes[range.clone()],
            &posts[1].1[range],
            "recorded envelope bytes retain their source identity"
        );
    }
    restore_from_database(&mut pipeline, spec).await;
    assert_eq!(volume_owner(&pipeline, MemberId(1)), Some(owner));
    for (file, ordinal) in [(0, 0), (0, 1), (1, 1), (2, 0)] {
        article(&mut pipeline, &posts, file, ordinal, 2, lost).await;
    }
    settle(&mut pipeline, &posts, 2, lost).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = std::collections::BTreeMap::new();
    harness::collect(&complete, &complete, &mut files);
    retire_pipeline_database(pipeline).await;
    assert_eq!(status, Some(JobStatus::Complete));
    assert_eq!(files.len(), 1);
    assert!(files.values().any(|bytes| bytes == &payload));
}

#[tokio::test]
async fn a_late_rar_index_reuses_only_recorded_envelopes_after_a_crash() {
    late_rar_index(false).await;
}

#[tokio::test]
async fn a_late_rar_index_repairs_the_tail_with_no_stat_server_after_a_crash() {
    late_rar_index(true).await;
}

#[tokio::test]
async fn an_opaque_sevenz_with_decoded_nzb_sizes_and_inline_loss_has_no_usable_output() {
    use crate::pipeline::tests::direct_store::scenarios::sevenz_store::{
        Entry, build_7z_shaped, split_volumes,
    };
    let mut state = 13_u64;
    let payload: Vec<_> = (0..70_001)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 56) as u8
        })
        .collect();
    let archive = build_7z_shaped(
        &[Entry::file("feature.mkv", payload)],
        sevenz_turbo::EncoderMethod::COPY,
        None,
        false,
    );
    let written = split_volumes(&archive, 1);
    assert_eq!(written[0].0, "silver.horizon.7z");
    let slice = archive.len().div_ceil(32).div_ceil(4) * 4;
    let index = weaver_server_pipeline::pipeline::tests::build_test_par2_index_for_files(
        &[(written[0].0.as_str(), archive.as_slice())],
        slice as u64,
    );
    let name = "5f0c9e2ab1d74c6e8a3f1b0d9c2e7a41";
    let frame = archive.len().div_ceil(4);
    let mut spec = harness::spec(
        "Silver Horizon",
        &[Post::plain(name, archive.clone())],
        frame,
    );
    let mut carrier = harness::spec(
        "",
        &[Post::plain("silver.horizon.par2", index.clone())],
        usize::MAX,
    )
    .files
    .remove(0);
    carrier.segments[0].bytes += carrier.segments[0].bytes.div_ceil(32) + 2;
    spec.total_bytes += u64::from(carrier.segments[0].bytes);
    spec.files.push(carrier);
    assert_eq!(
        spec.files[0]
            .segments
            .iter()
            .map(|segment| u64::from(segment.bytes))
            .sum::<u64>(),
        archive.len() as u64
    );
    spec.files[1].segments[0].message_ids =
        crate::jobs::MessageIds::new(vec!["recovery-index@example.invalid".into()]);
    let mut ids = std::collections::BTreeSet::new();
    for file in &spec.files {
        for segment in &file.segments {
            for id in segment.message_ids.all() {
                assert!(
                    ids.insert(id),
                    "every posted article has its own message id"
                );
            }
        }
    }
    let posts = vec![
        (name.to_string(), archive),
        ("silver.horizon.par2".to_string(), index),
    ];
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete) = new_direct_pipeline(&root).await;
    insert_active_job(&mut pipeline, JOB, spec).await;
    let lost = Some((0, 1));
    for (file, ordinal) in [(1, 0), (0, 0), (0, 1), (0, 2), (0, 3)] {
        article(&mut pipeline, &posts, file, ordinal, 4, lost).await;
    }
    settle(&mut pipeline, &posts, 4, lost).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = std::collections::BTreeMap::new();
    harness::collect(&complete, &complete, &mut files);
    retire_pipeline_database(pipeline).await;
    assert!(
        matches!(status, Some(JobStatus::Failed { .. })),
        "status={status:?}; files={:?}",
        files.keys()
    );
    assert!(
        files.is_empty(),
        "no downloaded archive or partial payload is usable"
    );
}

async fn solid_rar_after_nonfirst_half(encrypted: bool) {
    let volumes: [&[u8]; 2] = if encrypted {
        [
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../weaver-server-core/tests/fixtures/extraction_profiles/rar4_solid_enc_two.part01.rar"
            )),
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../weaver-server-core/tests/fixtures/extraction_profiles/rar4_solid_enc_two.part02.rar"
            )),
        ]
    } else {
        [
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../weaver-server-core/tests/fixtures/extraction_profiles/rar4_solid_two.part01.rar"
            )),
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../weaver-server-core/tests/fixtures/extraction_profiles/rar4_solid_two.part02.rar"
            )),
        ]
    };
    let password = encrypted.then_some("moonlit-harbour");
    let mut reader = match password {
        Some(password) => unrar_rs::RarArchive::open_with_password(
            std::io::Cursor::new(volumes[0].to_vec()),
            password,
        ),
        None => unrar_rs::RarArchive::open(std::io::Cursor::new(volumes[0].to_vec())),
    }
    .unwrap();
    reader
        .add_volume(1, Box::new(std::io::Cursor::new(volumes[1].to_vec())))
        .unwrap();
    let mut expected = std::collections::BTreeMap::new();
    for index in 0..reader.len() {
        let name = reader.member_info(index).unwrap().name.clone();
        let mut bytes = Vec::new();
        reader.by_index(index).unwrap().copy_to(&mut bytes).unwrap();
        assert!(expected.insert(name, bytes).is_none());
    }
    assert_eq!(expected.len(), 3);
    let posts: Vec<_> = volumes
        .iter()
        .enumerate()
        .map(|(index, bytes)| {
            (
                format!("{:032x}", 0xd1c7_0000_u128 + index as u128),
                bytes.to_vec(),
            )
        })
        .collect();
    let mut spec = direct_store_job_spec("Archive schedules", &posts);
    spec.password = password.map(str::to_owned);
    for file in &spec.files {
        assert_eq!(file.segments.len(), 2);
    }
    if !encrypted {
        assert_eq!(
            volumes.map(|bytes| bytes.len().div_ceil(2)),
            [12_288, 8_759]
        );
    }
    let root = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete) = new_direct_pipeline(&root).await;
    insert_active_job(&mut pipeline, JOB, spec.clone()).await;
    article_in(&mut pipeline, &posts, 1, 0, 2, None, 2).await;
    let owner = volume_owner(&pipeline, MemberId(1)).expect("nonfirst half has a recorded owner");
    let kind = node_kind(&pipeline, owner.0);
    let extent = ByteRange::new(0, posts[1].1.len().div_ceil(2) as u64);
    let mut before = Vec::new();
    assert!(read_root_member(
        &pipeline,
        MemberId(1),
        extent,
        &mut before
    ));
    assert_eq!(before, posts[1].1[..extent.end as usize]);
    let working = working_dir(&pipeline, JOB).clone();
    flush_checkpoint(&mut pipeline, JOB);
    retire_pipeline_database(pipeline).await;
    (pipeline, _, _) = new_direct_pipeline(&root).await;
    let checkpoint = saved_checkpoint(&pipeline).expect("persisted crash checkpoint");
    assert_eq!(checkpoint.owner(owner.0), Some((kind.as_str(), Some(0))));
    let envelopes = checkpoint.envelopes(owner.0);
    assert!(
        !envelopes.is_empty(),
        "nonfirst half retains recorded backing"
    );
    for kept in envelopes {
        let bytes =
            std::fs::read(working.join(&kept.name)).expect("recorded backing survives retirement");
        assert!(kept.start >= 0 && kept.end <= extent.end as i64);
        let range = kept.start as usize..kept.end as usize;
        assert_eq!(&bytes[range.clone()], &posts[1].1[range]);
    }
    restore_from_database(&mut pipeline, spec).await;
    assert_eq!(volume_owner(&pipeline, MemberId(1)), Some(owner));
    let order = if encrypted {
        [(1, 1), (0, 1), (0, 0)]
    } else {
        [(0, 1), (0, 0), (1, 1)]
    };
    for (file, ordinal) in order {
        article_in(&mut pipeline, &posts, file, ordinal, 2, None, 2).await;
    }
    settle_in(&mut pipeline, &posts, 2, None, 2).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = std::collections::BTreeMap::new();
    harness::collect(&complete, &complete, &mut files);
    retire_pipeline_database(pipeline).await;
    assert_eq!(
        status,
        Some(JobStatus::Complete),
        "files={:?}",
        files.keys()
    );
    assert_eq!(files.len(), expected.len());
    for (name, bytes) in expected {
        assert!(
            files
                .iter()
                .any(|(path, actual)| path.ends_with(&name) && actual == &bytes),
            "byte-exact {name}"
        );
    }
}

#[tokio::test]
async fn solid_rar4_restores_after_exactly_half_of_the_nonfirst_volume() {
    solid_rar_after_nonfirst_half(false).await;
}

#[tokio::test]
async fn encrypted_solid_rar4_restores_after_exactly_half_of_the_nonfirst_volume() {
    solid_rar_after_nonfirst_half(true).await;
}
