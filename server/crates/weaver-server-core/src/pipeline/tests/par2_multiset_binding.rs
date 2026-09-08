use super::*;

use std::sync::Arc;

const SLICE_SIZE: u64 = 64;

fn fixture_bytes(seed: u8, len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| (index as u8).wrapping_mul(37).wrapping_add(seed))
        .collect()
}

fn install_two_parsed_sets(
    pipeline: &mut Pipeline,
    job_id: JobId,
    served: par2_rs::Par2FileSet,
    other: par2_rs::Par2FileSet,
) {
    let served_id = served.recovery_set_id;
    let other_id = other.recovery_set_id;
    assert_ne!(
        served_id, other_id,
        "the fixture needs independent recovery sets"
    );

    install_test_par2_runtime(pipeline, job_id, served, &[]);
    let runtime = pipeline.ensure_par2_runtime(job_id);
    runtime.ensure_set_runtime(other_id).set = Some(Arc::new(other));
    assert_eq!(runtime.served, Some(served_id));
}

#[tokio::test]
async fn a_file_binds_to_the_unserved_parsed_set_that_describes_it() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30901);
    let described = fixture_bytes(11, 128);
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Ivory Meadow Binding",
            &[("ivory.meadow.mkv".to_string(), described.len() as u32)],
        ),
    )
    .await;
    let served = build_repairable_par2_set_for_files(
        &[("silver.horizon.mkv", fixture_bytes(19, 128).as_slice())],
        SLICE_SIZE,
        1,
    );
    let other = build_repairable_par2_set_for_files(
        &[("ivory.meadow.mkv", described.as_slice())],
        SLICE_SIZE,
        1,
    );
    let other_id = other.recovery_set_id;
    install_two_parsed_sets(&mut pipeline, job_id, served, other);

    let binding = pipeline
        .resolve_par2_file_binding(NzbFileId {
            job_id,
            file_index: 0,
        })
        .expect("the non-served set uniquely describes this file");

    assert_eq!(binding.recovery_set_id, other_id);
    assert_eq!(binding.described_length, described.len() as u64);
    assert_eq!(binding.path, working_dir.join("ivory.meadow.mkv"));
    assert!(!binding.is_complete);
}

#[tokio::test]
async fn a_name_described_by_two_parsed_sets_is_not_bound() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30902);
    let primary = fixture_bytes(23, 128);
    let alternate = fixture_bytes(29, 192);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Onyx Prairie Name Ambiguity",
            &[("onyx.prairie.mkv".to_string(), primary.len() as u32)],
        ),
    )
    .await;
    let served = build_repairable_par2_set_for_files(
        &[("onyx.prairie.mkv", primary.as_slice())],
        SLICE_SIZE,
        1,
    );
    let other = build_repairable_par2_set_for_files(
        &[("onyx.prairie.mkv", alternate.as_slice())],
        SLICE_SIZE,
        1,
    );
    install_two_parsed_sets(&mut pipeline, job_id, served, other);

    assert!(
        pipeline
            .resolve_par2_file_binding(NzbFileId {
                job_id,
                file_index: 0,
            })
            .is_none(),
        "two recovery-set name answers must remain ambiguous"
    );
}

#[tokio::test]
async fn a_prefix_that_matches_two_parsed_sets_is_not_bound() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30903);
    let payload = fixture_bytes(31, 20_480);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Silver Horizon Content Ambiguity",
            &[("d8b3a1f0.bin".to_string(), payload.len() as u32)],
        ),
    )
    .await;
    let served =
        build_repairable_par2_set_for_files(&[("silver.horizon.mkv", payload.as_slice())], 1024, 1);
    let other =
        build_repairable_par2_set_for_files(&[("ivory.meadow.mkv", payload.as_slice())], 1024, 1);
    install_two_parsed_sets(&mut pipeline, job_id, served, other);
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    pipeline.file_prefix_16k.insert(
        file_id,
        payload[..crate::pipeline::PAR2_HASH_16K_BYTES].to_vec(),
    );

    assert!(
        pipeline.resolve_par2_file_binding(file_id).is_none(),
        "a content answer from two recovery sets must remain ambiguous"
    );
}

#[tokio::test]
async fn block_crc_verdicts_use_the_non_served_bindings_own_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30904);
    let payload = fixture_bytes(37, SLICE_SIZE as usize);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Amber Trail Grid Binding",
            &[("amber.trail.mkv".to_string(), payload.len() as u32)],
        ),
    )
    .await;
    let served = build_repairable_par2_set_for_files(
        &[(
            "silver.horizon.mkv",
            fixture_bytes(41, SLICE_SIZE as usize).as_slice(),
        )],
        SLICE_SIZE,
        1,
    );
    let other = build_repairable_par2_set_for_files(
        &[("amber.trail.mkv", payload.as_slice())],
        SLICE_SIZE,
        1,
    );
    install_two_parsed_sets(&mut pipeline, job_id, served, other);
    write_and_complete_file(&mut pipeline, job_id, 0, "amber.trail.mkv", &payload).await;

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let crc32 = par2_rs::checksum::crc32(&payload);
    pipeline.note_block_crc_segments(
        file_id,
        0,
        payload.len() as u64,
        crc32,
        true,
        false,
        &[weaver_yenc::Segment {
            file_offset: 0,
            len: payload.len() as u64,
            crc32,
        }],
    );

    assert!(
        pipeline
            .block_crc_verdicts(file_id)
            .is_some_and(|verdicts| {
                matches!(
                    verdicts.get(&0),
                    Some(crate::pipeline::integrity::BlockVerdict::Intact {
                        independently_covered: true
                    })
                )
            }),
        "the non-served binding must be measured against its own IFSC grid"
    );
}

#[tokio::test]
async fn conflicting_cross_set_rename_targets_are_dropped() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30905);
    let posted_name = "e4a7b9c2.bin";
    let correct_name = "silver.horizon.mkv";
    let payload = fixture_bytes(43, 20_480);
    let mut conflicting = payload.clone();
    conflicting[0] ^= 0x5A;
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Silver Horizon Rename Conflict",
            &[(posted_name.to_string(), payload.len() as u32)],
        ),
    )
    .await;
    let served =
        build_repairable_par2_set_for_files(&[(correct_name, payload.as_slice())], 1024, 1);
    let other =
        build_repairable_par2_set_for_files(&[(correct_name, conflicting.as_slice())], 1024, 1);
    install_two_parsed_sets(&mut pipeline, job_id, served, other);
    write_and_complete_file(&mut pipeline, job_id, 0, posted_name, &payload).await;

    assert_eq!(
        pipeline
            .try_deobfuscate_files_with_par2(job_id)
            .await
            .renamed,
        0
    );
    assert!(working_dir.join(posted_name).exists());
    assert!(!working_dir.join(correct_name).exists());
}

#[tokio::test]
async fn a_unique_non_served_set_rename_lands() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30906);
    let posted_name = "c9d4e7a1.bin";
    let correct_name = "ivory.meadow.mkv";
    let payload = fixture_bytes(47, 20_480);
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Ivory Meadow Rename",
            &[(posted_name.to_string(), payload.len() as u32)],
        ),
    )
    .await;
    let served = build_repairable_par2_set_for_files(
        &[("amber.trail.mkv", fixture_bytes(53, 20_480).as_slice())],
        1024,
        1,
    );
    let other = build_repairable_par2_set_for_files(&[(correct_name, payload.as_slice())], 1024, 1);
    install_two_parsed_sets(&mut pipeline, job_id, served, other);
    write_and_complete_file(&mut pipeline, job_id, 0, posted_name, &payload).await;

    assert_eq!(
        pipeline
            .try_deobfuscate_files_with_par2(job_id)
            .await
            .renamed,
        1
    );
    assert!(!working_dir.join(posted_name).exists());
    assert_eq!(
        std::fs::read(working_dir.join(correct_name)).unwrap(),
        payload
    );
}

#[tokio::test]
async fn a_single_set_keeps_its_binding_grid_and_rename_behavior() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30907);
    let filename = "onyx.prairie.mkv";
    let payload = fixture_bytes(59, SLICE_SIZE as usize);
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Onyx Prairie Single Set",
            &[(filename.to_string(), payload.len() as u32)],
        ),
    )
    .await;
    let set = build_repairable_par2_set_for_files(&[(filename, payload.as_slice())], SLICE_SIZE, 1);
    let set_id = set.recovery_set_id;
    install_test_par2_runtime(&mut pipeline, job_id, set, &[]);
    write_and_complete_file(&mut pipeline, job_id, 0, filename, &payload).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let crc32 = par2_rs::checksum::crc32(&payload);
    pipeline.note_block_crc_segments(
        file_id,
        0,
        payload.len() as u64,
        crc32,
        true,
        false,
        &[weaver_yenc::Segment {
            file_offset: 0,
            len: payload.len() as u64,
            crc32,
        }],
    );

    let binding = pipeline
        .resolve_par2_file_binding(file_id)
        .expect("the single set must still bind its file");
    assert_eq!(binding.recovery_set_id, set_id);
    assert_eq!(binding.path, working_dir.join(filename));
    assert!(binding.is_complete);
    assert!(pipeline.block_crc_verdicts(file_id).is_some());

    let (mut rename_pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let rename_job_id = JobId(30908);
    let posted_name = "f2a8c6d1.bin";
    let correct_name = "amber.trail.mkv";
    let rename_payload = fixture_bytes(61, 20_480);
    let rename_dir = insert_active_job(
        &mut rename_pipeline,
        rename_job_id,
        standalone_job_spec(
            "Amber Trail Single Set Rename",
            &[(posted_name.to_string(), rename_payload.len() as u32)],
        ),
    )
    .await;
    install_test_par2_runtime(
        &mut rename_pipeline,
        rename_job_id,
        build_repairable_par2_set_for_files(&[(correct_name, rename_payload.as_slice())], 1024, 1),
        &[],
    );
    write_and_complete_file(
        &mut rename_pipeline,
        rename_job_id,
        0,
        posted_name,
        &rename_payload,
    )
    .await;

    assert_eq!(
        rename_pipeline
            .try_deobfuscate_files_with_par2(rename_job_id)
            .await
            .renamed,
        1
    );
    assert!(!rename_dir.join(posted_name).exists());
    assert_eq!(
        std::fs::read(rename_dir.join(correct_name)).unwrap(),
        rename_payload
    );
}

#[tokio::test]
async fn analysing_one_set_does_not_offer_the_other_sets_volume_as_an_extra() {
    // Two recovery sets share one working directory, each describing its own
    // volume. When this set's damaged-path analysis looks for extras, the
    // volume the *other* set describes must not be a candidate: the scan can
    // only ever confirm zero blocks against it, and the price of finding that
    // out is a rolling read of the whole file. A file no set claims stays a
    // candidate — finding those is the entire point of the extra scan.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30931);
    let own_volume = "silver.horizon.part1.rar";
    let foreign_volume = "ivory.meadow.part1.rar";
    let unclaimed = "a3f19c40.bin";
    let own_bytes = fixture_bytes(41, 128);
    let foreign_bytes = fixture_bytes(43, 128);
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Silver Horizon And Ivory Meadow",
            &[
                (own_volume.to_string(), own_bytes.len() as u32),
                (foreign_volume.to_string(), foreign_bytes.len() as u32),
                (unclaimed.to_string(), 128),
            ],
        ),
    )
    .await;
    let served =
        build_repairable_par2_set_for_files(&[(own_volume, own_bytes.as_slice())], SLICE_SIZE, 1);
    let served_id = served.recovery_set_id;
    let other = build_repairable_par2_set_for_files(
        &[(foreign_volume, foreign_bytes.as_slice())],
        SLICE_SIZE,
        1,
    );
    install_two_parsed_sets(&mut pipeline, job_id, served, other);

    let exclusions = pipeline.par2_extra_scan_exclusions(job_id, served_id);

    assert!(
        exclusions.contains(&working_dir.join(foreign_volume)),
        "the other set's volume must be kept out of this set's extra scan, got {exclusions:?}"
    );
    assert!(
        !exclusions.contains(&working_dir.join(own_volume)),
        "this set's own source is a canonical candidate, never an exclusion, got {exclusions:?}"
    );
    assert!(
        !exclusions.contains(&working_dir.join(unclaimed)),
        "a file no set describes must stay discoverable, got {exclusions:?}"
    );
}

#[tokio::test]
async fn a_complete_volume_of_a_foreign_rar_set_is_kept_out_of_the_extra_scan() {
    // The binding half of the exclusion only reaches files a recovery set
    // describes by name or content. A second RAR set's volumes can be complete
    // on disk with no PAR2 binding at all — obfuscated names, or a set whose
    // own PAR2 has not been parsed yet — and those are exactly the multi-
    // gigabyte files the rolling scan would grind through. Set membership
    // stands in for the binding: a *complete* volume of a RAR set this
    // recovery set does not describe belongs to somebody else.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30932);
    let own_volume = "silver.horizon.part1.rar";
    let foreign_volume = "ivory.meadow.part1.rar";
    let own_bytes = fixture_bytes(47, 128);
    let foreign_bytes = fixture_bytes(53, 128);
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Silver Horizon Beside An Unbound Set",
            &[
                (own_volume.to_string(), own_bytes.len() as u32),
                (foreign_volume.to_string(), foreign_bytes.len() as u32),
            ],
        ),
    )
    .await;
    let served =
        build_repairable_par2_set_for_files(&[(own_volume, own_bytes.as_slice())], SLICE_SIZE, 1);
    let served_id = served.recovery_set_id;
    install_test_par2_runtime(&mut pipeline, job_id, served, &[]);
    for (file_index, filename, set_name) in [
        (0u32, own_volume, "silver.horizon"),
        (1, foreign_volume, "ivory.meadow"),
    ] {
        pipeline
            .set_file_identity(
                job_id,
                crate::jobs::record::ActiveFileIdentity {
                    file_index,
                    source_filename: filename.to_string(),
                    current_filename: filename.to_string(),
                    canonical_filename: Some(filename.to_string()),
                    classification: Some(crate::jobs::assembly::DetectedArchiveIdentity {
                        kind: crate::jobs::assembly::DetectedArchiveKind::Rar,
                        set_name: set_name.to_string(),
                        volume_index: Some(0),
                    }),
                    classification_source: crate::jobs::record::FileIdentitySource::Par2,
                },
            )
            .unwrap();
    }

    // Incomplete, so still worth looking at: a partially written volume is
    // where a stray block can genuinely turn up.
    assert!(
        !pipeline
            .par2_extra_scan_exclusions(job_id, served_id)
            .contains(&working_dir.join(foreign_volume)),
        "an incomplete foreign volume stays a candidate"
    );

    write_and_complete_file(&mut pipeline, job_id, 1, foreign_volume, &foreign_bytes).await;

    let exclusions = pipeline.par2_extra_scan_exclusions(job_id, served_id);
    assert!(
        exclusions.contains(&working_dir.join(foreign_volume)),
        "a complete volume of a foreign RAR set must be excluded, got {exclusions:?}"
    );
    assert!(
        !exclusions.contains(&working_dir.join(own_volume)),
        "the analysed set's own RAR set is never excluded, got {exclusions:?}"
    );
}

#[tokio::test]
async fn par2_extra_scan_excludes_what_a_repair_left_behind() {
    // Installing a repair moves the damaged file it replaces aside and leaves
    // the copy in the job directory until the whole job has settled. Without
    // this, every later pass — and every other set sharing the directory —
    // rolling-reads a second whole payload to confirm what the repaired file
    // already confirms. A leftover is named by difference against the listing
    // taken before the repair, never by the shape of its name.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30933);
    let own_volume = "silver.horizon.part1.rar";
    let own_bytes = fixture_bytes(59, 128);
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Silver Horizon Repair Leftover",
            &[(own_volume.to_string(), own_bytes.len() as u32)],
        ),
    )
    .await;
    let served =
        build_repairable_par2_set_for_files(&[(own_volume, own_bytes.as_slice())], SLICE_SIZE, 1);
    let served_id = served.recovery_set_id;
    install_test_par2_runtime(&mut pipeline, job_id, served, &[]);
    write_and_complete_file(&mut pipeline, job_id, 0, own_volume, &own_bytes).await;

    // Present before any repair: a stray the job never declared, and a
    // numbered name the job cannot account for — an obfuscated post is named
    // exactly like that, and finding this set's bytes under such a name is
    // what extras are for.
    let stray = "a3f19c40.bin";
    let obfuscated = "51273aad56a8b904e96928935278a627.102";
    for name in [stray, obfuscated] {
        std::fs::write(working_dir.join(name), fixture_bytes(67, 128)).unwrap();
    }
    let leftover = format!("{own_volume}.1");
    let second_leftover = format!("{own_volume}.2");

    // No repair has run, so nothing is a leftover yet: the numbered copies
    // could only be posted files.
    for name in [leftover.as_str(), second_leftover.as_str()] {
        std::fs::write(working_dir.join(name), fixture_bytes(61, 128)).unwrap();
    }
    let exclusions = pipeline.par2_extra_scan_exclusions(job_id, served_id);
    for name in [
        leftover.as_str(),
        second_leftover.as_str(),
        stray,
        obfuscated,
    ] {
        assert!(
            !exclusions.contains(&working_dir.join(name)),
            "before a repair nothing is a leftover, got {exclusions:?}"
        );
    }
    for name in [leftover.as_str(), second_leftover.as_str()] {
        std::fs::remove_file(working_dir.join(name)).unwrap();
    }

    // The listing the repair path takes before the repairer touches the
    // directory, then the copies the repairer leaves behind.
    let before: std::collections::HashSet<String> = std::fs::read_dir(&working_dir)
        .unwrap()
        .flatten()
        .filter_map(|entry| entry.file_name().to_str().map(str::to_string))
        .collect();
    assert!(before.contains(own_volume) && before.contains(stray) && before.contains(obfuscated));
    pipeline.par2_pre_repair_dir_entries.insert(job_id, before);
    for name in [leftover.as_str(), second_leftover.as_str()] {
        std::fs::write(working_dir.join(name), fixture_bytes(61, 128)).unwrap();
    }

    let exclusions = pipeline.par2_extra_scan_exclusions(job_id, served_id);

    assert!(
        exclusions.contains(&working_dir.join(&leftover)),
        "the copy the repair left beside its target must be excluded, got {exclusions:?}"
    );
    assert!(
        exclusions.contains(&working_dir.join(&second_leftover)),
        "a second repair leaves a second copy, and it is no different, got {exclusions:?}"
    );
    assert!(
        !exclusions.contains(&working_dir.join(own_volume)),
        "the repaired file itself is a canonical source, got {exclusions:?}"
    );
    assert!(
        !exclusions.contains(&working_dir.join(stray)),
        "a file that was there before the repair must stay discoverable, got {exclusions:?}"
    );
    assert!(
        !exclusions.contains(&working_dir.join(obfuscated)),
        "a numbered name that was there before the repair is not a leftover, got {exclusions:?}"
    );
}

#[tokio::test]
async fn par2_extra_scan_keeps_a_posted_file_that_only_looks_like_a_repair_leftover() {
    // An obfuscated post numbers its own parts the same way a repair numbers
    // the copy it sets aside, so `payload.11` sitting beside `payload.1` is two
    // posted files. Both are this set's own volumes under lying names, which is
    // precisely the case the extra scan exists to solve — and they stay
    // candidates even when they land after the pre-repair listing was taken,
    // because the NZB names them.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30934);
    let first_alias = "payload.1";
    let second_alias = "payload.11";
    let first_bytes = fixture_bytes(71, 128);
    let second_bytes = fixture_bytes(73, 128);
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Amber Trail Numbered Aliases",
            &[
                (first_alias.to_string(), first_bytes.len() as u32),
                (second_alias.to_string(), second_bytes.len() as u32),
            ],
        ),
    )
    .await;
    let served = build_repairable_par2_set_for_files(
        &[
            ("amber.trail.part1.rar", first_bytes.as_slice()),
            ("amber.trail.part2.rar", second_bytes.as_slice()),
        ],
        SLICE_SIZE,
        1,
    );
    let served_id = served.recovery_set_id;
    install_test_par2_runtime(&mut pipeline, job_id, served, &[]);
    pipeline
        .par2_pre_repair_dir_entries
        .insert(job_id, std::collections::HashSet::new());
    write_and_complete_file(&mut pipeline, job_id, 0, first_alias, &first_bytes).await;
    write_and_complete_file(&mut pipeline, job_id, 1, second_alias, &second_bytes).await;

    let exclusions = pipeline.par2_extra_scan_exclusions(job_id, served_id);

    for alias in [first_alias, second_alias] {
        assert!(
            !exclusions.contains(&working_dir.join(alias)),
            "a posted file must never be read as a repair leftover, got {exclusions:?}"
        );
    }
}
