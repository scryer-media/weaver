//! Ignorable "furniture" inside the recovery set
//! Post-verdict re-entry
//! Partial recovery volumes
//! Postings carrying more than one recovery set
//! A recovery set that describes the joined file, against a posting of parts

use super::*;

/// The repair's own leftovers must not fail the repair that produced them.
///
/// par2-rs installs a file at the name its description gives it and moves
/// whatever held that name aside as `<name>.N`. After a swap that leaves the
/// backup holding exactly the content of the file it was displaced by, so a
/// directory scan — the only way a disk file is matched to a description —
/// finds two files for one description and calls the pair a conflict. The
/// settled-layout pass used to be that scan, and it refused accepted repairs
/// over artefacts the repair had just written. It asks the narrower question
/// now: are the recovery files and newly canonicalized descriptions intact at
/// their canonical names. A leftover cannot answer that one, and it is swept
/// with the rest once the aggregate settles.
#[tokio::test]
async fn repair_leftovers_do_not_fail_the_pass_that_verifies_the_settled_layout() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut repair_events = pipeline.event_tx.subscribe();
    let job_id = JobId(30346);
    let index_filename = "silver-horizon.par2";
    let posted_notes_filename = "9f2c1a5e.dat";
    let notes_filename = "silver-horizon.nfo";
    let alpha = misplacement_payload(11);
    let beta = misplacement_payload(12);
    let gamma = misplacement_payload(13);
    let notes = misplacement_payload(14);
    let par2_bytes = build_test_par2_index_for_files(
        &[
            ("silver-horizon-a.bin", &alpha),
            ("silver-horizon-b.bin", &beta),
            ("silver-horizon-c.bin", &gamma),
        ],
        64,
    );

    let payload_spec = |index: u32, filename: &str, len: usize| FileSpec {
        filename: filename.to_string(),
        role: FileRole::from_filename(filename),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: (0..len as u32 / 64)
            .map(|segment| {
                segment_spec! {
                    number: segment,
                    bytes: 64,
                    message_id: format!("leftover-{index}-{segment}@example.com"),
                }
            })
            .collect(),
    };
    let spec = JobSpec {
        name: "Silver Horizon Repair Leftovers".to_string(),
        password: None,
        total_bytes: (alpha.len() + beta.len() + gamma.len() + notes.len() + par2_bytes.len())
            as u64,
        category: None,
        metadata: vec![],
        files: vec![
            payload_spec(0, "silver-horizon-a.bin", alpha.len()),
            payload_spec(1, "silver-horizon-b.bin", beta.len()),
            payload_spec(2, "silver-horizon-c.bin", gamma.len()),
            payload_spec(3, posted_notes_filename, notes.len()),
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "leftover-index@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // What the repair found, and what it left. Two files were posted under each
    // other's names and one was holed; the repair installed all three at their
    // canonical names, which put the two swapped originals aside as `.1`
    // backups holding each other's content.
    for (filename, bytes) in [
        ("silver-horizon-a.bin", &alpha),
        ("silver-horizon-b.bin", &beta),
        ("silver-horizon-c.bin", &gamma),
        (posted_notes_filename, &notes),
    ] {
        tokio::fs::write(working_dir.join(filename), bytes)
            .await
            .unwrap();
    }
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        for file_index in 0..4u32 {
            let file = state
                .assembly
                .file_mut(NzbFileId { job_id, file_index })
                .unwrap();
            for segment in 0..2u32 {
                file.commit_segment(segment, 64).unwrap();
            }
        }
    }
    write_and_complete_file(&mut pipeline, job_id, 4, index_filename, &par2_bytes).await;

    let mut par2_set = build_repairable_par2_set_for_files(
        &[
            ("silver-horizon-a.bin", &alpha),
            ("silver-horizon-b.bin", &beta),
            ("silver-horizon-c.bin", &gamma),
        ],
        64,
        1,
    );
    describe_non_recovery_file(&mut par2_set, notes_filename, &notes);
    describe_non_recovery_file(
        &mut par2_set,
        "silver-horizon-missing.sfv",
        b"described but never posted",
    );
    let alpha_id = par2_set.recovery_file_ids[0];
    let beta_id = par2_set.recovery_file_ids[1];
    let gamma_id = par2_set.recovery_file_ids[2];
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        par2_set,
        &[(4, index_filename, 0, false)],
    );
    let par2_set = pipeline.par2_set(job_id).cloned().unwrap();

    // The snapshot the repairer takes on its way in, and the backups it then
    // leaves behind.
    pipeline.par2_pre_repair_dir_entries.insert(
        job_id,
        HashSet::from([
            "silver-horizon-a.bin".to_string(),
            "silver-horizon-b.bin".to_string(),
            "silver-horizon-c.bin".to_string(),
            posted_notes_filename.to_string(),
            index_filename.to_string(),
        ]),
    );
    tokio::fs::write(working_dir.join("silver-horizon-a.bin.1"), &beta)
        .await
        .unwrap();
    tokio::fs::write(working_dir.join("silver-horizon-b.bin.1"), &alpha)
        .await
        .unwrap();

    let pre_repair = par2_rs::VerificationResult {
        files: vec![
            par2_rs::verify::FileVerification {
                file_id: alpha_id,
                filename: "silver-horizon-a.bin".to_string(),
                status: par2_rs::verify::FileStatus::Renamed(
                    working_dir.join("silver-horizon-b.bin"),
                ),
                valid_slices: vec![true, true],
                missing_slice_count: 0,
            },
            par2_rs::verify::FileVerification {
                file_id: beta_id,
                filename: "silver-horizon-b.bin".to_string(),
                status: par2_rs::verify::FileStatus::Renamed(
                    working_dir.join("silver-horizon-a.bin"),
                ),
                valid_slices: vec![true, true],
                missing_slice_count: 0,
            },
            par2_rs::verify::FileVerification {
                file_id: gamma_id,
                filename: "silver-horizon-c.bin".to_string(),
                status: par2_rs::verify::FileStatus::Damaged(1),
                valid_slices: vec![true, false],
                missing_slice_count: 1,
            },
        ],
        recovery_blocks_available: 1,
        total_missing_blocks: 1,
        repairable: par2_rs::verify::Repairability::Repairable {
            blocks_needed: 1,
            blocks_available: 1,
        },
    };
    let outcome = par2_rs::Par2RepairOutcome {
        status: par2_rs::Par2RepairStatus::Repaired,
        files_complete: 3,
        files_renamed: 2,
        files_damaged: 0,
        files_missing: 0,
        available_blocks: 1,
        missing_blocks: 0,
        recovery_blocks_available: 1,
        recovery_blocks_used: 1,
        bytes_copied: (alpha.len() + beta.len()) as u64,
        bytes_reconstructed: 64,
        packets: par2_rs::PacketDiagnostics::default(),
        scan: par2_rs::ScanDiagnostics::default(),
        carry: par2_rs::repairer::CarryDiagnostics::default(),
        verification: pre_repair.clone(),
    };

    pipeline
        .finish_par2_repair(
            job_id,
            Arc::clone(&par2_set),
            working_dir.clone(),
            &pre_repair,
            outcome,
            false,
        )
        .await;

    assert!(
        !matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Failed { .. })
        ),
        "the repair held; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        drain_job_repair_complete(&mut repair_events, job_id),
        1,
        "a repair whose set is intact where it now sits is a repair that held"
    );
    assert_eq!(
        pipeline.par2_selective_verify_calls, 2,
        "the non-recovery file moved into its free canonical name from 16 KiB \
         identity evidence, so the settled layout still needs strict proof — \
         delivered by the selective canonical pass over the moved files, not \
         a whole-set read; authoritative={}",
        pipeline.par2_authoritative_verify_calls,
    );
    assert_eq!(
        pipeline.par2_authoritative_verify_calls, 0,
        "and the files proven in place are carried, never re-read"
    );
    let mut remaining: Vec<String> = std::fs::read_dir(&working_dir)
        .unwrap()
        .flatten()
        .filter_map(|entry| entry.file_name().to_str().map(str::to_string))
        .collect();
    remaining.sort();
    assert_eq!(
        remaining,
        vec![
            index_filename.to_string(),
            "silver-horizon-a.bin".to_string(),
            "silver-horizon-b.bin".to_string(),
            "silver-horizon-c.bin".to_string(),
            notes_filename.to_string(),
        ]
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>(),
        "exactly the described files and the recovery index survive: the two \
         backups are swept once the aggregate settles, and the posted name the \
         set describes otherwise has been given its own"
    );
    for (filename, bytes) in [
        ("silver-horizon-a.bin", &alpha),
        ("silver-horizon-b.bin", &beta),
        ("silver-horizon-c.bin", &gamma),
        (notes_filename, &notes),
    ] {
        assert_eq!(
            tokio::fs::read(working_dir.join(filename)).await.unwrap(),
            **bytes,
            "{filename} must hold its own content"
        );
    }
}

/// Job 10000 whole: a repaired payload delivered alongside an unprotected file
/// that stayed short of articles.
///
/// Removing the veto stopped this job failing, but a job that cannot fail and
/// cannot finish just spins: the completion gate keeps `has_incomplete_data_files`
/// true forever, re-runs a full authoritative PAR2 pass every couple of seconds,
/// and never reaches finalization. Bounded here so that livelock reads as a
/// failure rather than a hang.
#[tokio::test]
async fn job_with_repaired_payload_and_short_unprotected_file_completes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30330);
    let payload_filename = "silver.horizon.mkv";
    let nfo_filename = "silver.horizon.nfo";
    let index_filename = "silver.horizon.par2";
    let recovery_filename = "silver.horizon.vol00+01.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| (value % 251) as u8).collect();
    let mut damaged_payload = original_payload.clone();
    for byte in &mut damaged_payload[64..128] {
        *byte = 0;
    }
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let recovery_bytes = vec![0xAA; 64];
    let spec = JobSpec {
        name: "Silver Horizon With Short NFO".to_string(),
        password: None,
        total_bytes: (original_payload.len() + par2_bytes.len() + recovery_bytes.len() + 128)
            as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "short-nfo-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "short-nfo-payload-1@example.com".to_string(),
                    },
                ],
            },
            // Not in the recovery set, and one article short: exactly the file
            // that used to fail the job and then used to spin it.
            FileSpec {
                filename: nfo_filename.to_string(),
                role: FileRole::from_filename(nfo_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "short-nfo-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "short-nfo-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "short-nfo-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: recovery_bytes.len() as u32,
                    message_id: "short-nfo-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
        .await
        .unwrap();
    tokio::fs::write(working_dir.join(nfo_filename), vec![7u8; 64])
        .await
        .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 0,
            })
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
        // The NFO lost its second article and nothing protects it.
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 1,
            })
            .unwrap()
            .commit_segment(0, 64)
            .unwrap();
    }
    write_and_complete_file(&mut pipeline, job_id, 2, index_filename, &par2_bytes).await;
    write_and_complete_file(&mut pipeline, job_id, 3, recovery_filename, &recovery_bytes).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 1),
        &[
            (2, index_filename, 0, false),
            (3, recovery_filename, 1, true),
        ],
    );

    for _ in 0..12 {
        if matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        pipeline.check_job_completion(job_id).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
        settle_inflight_moves(&mut pipeline).await;
    }

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "a repaired payload must be delivered even though an unprotected file is short; {}",
        debug_job_state(&pipeline, job_id)
    );
}

// ---------------------------------------------------------------------------
// Ignorable "furniture" inside the recovery set
// ---------------------------------------------------------------------------

/// A par2-*protected* `.nfo` damaged past what the recovery blocks can rebuild
/// is delivered, not failed.
///
/// Both reference downloaders ship this job: one never raises its
/// "has damaged files" flag for such a file, so the set reports repair-not-
/// needed even when the recovery data said repair was impossible; the other's
/// quick check passes it outright. weaver used to fail the whole download for
/// it, which is the protected sibling of the unprotected-`.nfo` failure.
#[tokio::test]
async fn protected_damaged_ignorable_file_is_delivered_without_repair() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30340);
    let payload = intact_furniture_payload();
    let furniture: Vec<u8> = (0..128u32).map(|value| (value % 97) as u8).collect();
    let damaged_furniture = second_half_zeroed(&furniture);

    install_furniture_par2_job(
        &mut pipeline,
        job_id,
        FurnitureJob {
            name: "Silver Horizon Damaged NFO",
            payload_filename: "silver.horizon.mkv",
            payload: &payload,
            payload_on_disk: Some(&payload),
            furniture_filename: "silver.horizon.nfo",
            furniture: &furniture,
            furniture_on_disk: Some(&damaged_furniture),
            furniture_articles_complete: true,
            recovery_blocks: 0,
        },
    )
    .await;

    settle_job_completion(&mut pipeline, job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "damage confined to furniture must be delivered, not failed; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_repairer_execute_calls, 0,
        "rebuilding one slice of an .nfo is not worth a full-set read"
    );
}

/// The same rule for a protected `.sfv` that never arrived at all, with no
/// recovery to rebuild it from.
///
/// This is the shape that also has to survive the post-verdict completion gate:
/// the file stays incomplete and bound to a description forever, so counting it
/// as outstanding work would keep the job re-arming instead of finishing.
#[tokio::test]
async fn protected_missing_ignorable_file_is_delivered_without_repair() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30341);
    let payload = intact_furniture_payload();
    let furniture: Vec<u8> = (0..128u32).map(|value| (value % 89) as u8).collect();

    install_furniture_par2_job(
        &mut pipeline,
        job_id,
        FurnitureJob {
            name: "Silver Horizon Missing SFV",
            payload_filename: "silver.horizon.mkv",
            payload: &payload,
            payload_on_disk: Some(&payload),
            furniture_filename: "silver.horizon.sfv",
            furniture: &furniture,
            furniture_on_disk: None,
            furniture_articles_complete: false,
            recovery_blocks: 0,
        },
    )
    .await;

    settle_job_completion(&mut pipeline, job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "a protected .sfv that never posted must not fail the payload; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(pipeline.par2_repairer_execute_calls, 0);
}

/// When something that is *not* furniture is damaged too and the blocks are
/// there, the repairer runs and heals the furniture in the same pass.
///
/// This is the row the spare rule must not swallow: the rebuild is free once
/// the decode matrix is being built anyway, and all three reference behaviours
/// agree on repairing here.
#[tokio::test]
async fn mixed_damage_with_sufficient_blocks_repairs_the_furniture_too() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30342);
    let payload = intact_furniture_payload();
    let furniture: Vec<u8> = (0..128u32).map(|value| (value % 83) as u8).collect();
    let job_name = "Silver Horizon Mixed Repairable";

    install_furniture_par2_job(
        &mut pipeline,
        job_id,
        FurnitureJob {
            name: job_name,
            payload_filename: "silver.horizon.mkv",
            payload: &payload,
            payload_on_disk: Some(&second_half_zeroed(&payload)),
            furniture_filename: "silver.horizon.nfo",
            furniture: &furniture,
            furniture_on_disk: Some(&second_half_zeroed(&furniture)),
            furniture_articles_complete: true,
            recovery_blocks: 2,
        },
    )
    .await;

    settle_job_completion(&mut pipeline, job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "{}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_repairer_execute_calls, 1,
        "non-furniture damage still repairs, furniture included"
    );
    let output_dir = pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(job_name));
    assert_eq!(
        tokio::fs::read(output_dir.join("silver.horizon.mkv"))
            .await
            .unwrap(),
        payload
    );
    assert_eq!(
        tokio::fs::read(output_dir.join("silver.horizon.nfo"))
            .await
            .unwrap(),
        furniture,
        "the furniture is rebuilt in the same pass"
    );
}

#[tokio::test]
async fn a_damaged_authoritative_verification_builds_a_carry_the_repairer_accepts() {
    // The hand-off `run_par2_placement_pass` performs when its whole-set pass
    // finds damage over an in-place layout, pinned at the unit: the pass's own
    // verification builds a carry, and a repairer seeded with it reaches the
    // same verdict without the pass being re-run cold.
    let temp_dir = tempfile::tempdir().unwrap();
    let dir = temp_dir.path().to_path_buf();
    let payload_filename = "payload.mkv";
    let original: Vec<u8> = (0..256u32).map(|value| (value % 251) as u8).collect();
    let mut damaged = original.clone();
    for byte in &mut damaged[128..] {
        *byte = 0;
    }
    std::fs::write(dir.join(payload_filename), &damaged).unwrap();
    let par2_set = build_repairable_par2_set(payload_filename, &original, 64, 2);

    let empty_plan = par2_rs::PlacementPlan {
        exact: Vec::new(),
        swaps: Vec::new(),
        renames: Vec::new(),
        unresolved: Vec::new(),
        conflicts: Vec::new(),
    };
    let access = par2_rs::PlacementFileAccess::from_plan(dir.clone(), &par2_set, &empty_plan);
    let verification = par2_rs::verify_all(&par2_set, &access);
    assert!(
        verification.needs_repair(),
        "precondition: the authoritative pass sees the damage"
    );

    let carry = crate::pipeline::completion::finalize::check::build_host_verification_carry(
        &dir,
        &par2_set,
        &verification,
    )
    .expect("a damaged in-place layout must build a host carry");

    let mut options = par2_rs::Par2RepairerOptions::new(dir, Vec::new());
    options.file_set = Some(par2_set.clone());
    options.repair = false;
    options.scan_carry = Some(carry);
    let repairer = par2_rs::Par2Repairer::new(options);
    let (outcome, _) = repairer
        .verify_or_repair_carrying()
        .expect("a repairer seeded with the host carry runs");
    assert_eq!(
        outcome.verification.total_missing_blocks, verification.total_missing_blocks,
        "the seeded analysis reaches the verdict the host pass already proved"
    );
}

/// Mixed damage with the blocks short still fails.
///
/// The furniture's slices cannot be excused out of the solve: a payload file's
/// missing slices are unknowns in every equation the recovery data can form, so
/// sparing the `.nfo` buys the job nothing and delivering a holed payload under
/// a verification that claims otherwise is the one thing that must not happen.
#[tokio::test]
async fn mixed_damage_with_short_blocks_still_fails() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30343);
    let payload = intact_furniture_payload();
    let furniture: Vec<u8> = (0..128u32).map(|value| (value % 79) as u8).collect();

    install_furniture_par2_job(
        &mut pipeline,
        job_id,
        FurnitureJob {
            name: "Silver Horizon Mixed Short",
            payload_filename: "silver.horizon.mkv",
            payload: &payload,
            payload_on_disk: Some(&second_half_zeroed(&payload)),
            furniture_filename: "silver.horizon.nfo",
            furniture: &furniture,
            furniture_on_disk: Some(&second_half_zeroed(&furniture)),
            furniture_articles_complete: true,
            recovery_blocks: 1,
        },
    )
    .await;

    settle_job_completion(&mut pipeline, job_id).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!(
            "damaged payload with short recovery must still fail; {}",
            debug_job_state(&pipeline, job_id)
        );
    };
    assert!(
        error.contains("not repairable"),
        "unexpected error: {error}"
    );
}

/// The override is the way back to the old rule, and it has to work.
#[tokio::test]
async fn an_empty_ignore_extension_override_restores_the_old_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.par2_ignore_extensions_override = Some(Vec::new());
    let job_id = JobId(30344);
    let payload = intact_furniture_payload();
    let furniture: Vec<u8> = (0..128u32).map(|value| (value % 97) as u8).collect();

    install_furniture_par2_job(
        &mut pipeline,
        job_id,
        FurnitureJob {
            name: "Silver Horizon Override Off",
            payload_filename: "silver.horizon.mkv",
            payload: &payload,
            payload_on_disk: Some(&payload),
            furniture_filename: "silver.horizon.nfo",
            furniture: &furniture,
            furniture_on_disk: Some(&second_half_zeroed(&furniture)),
            furniture_articles_complete: true,
            recovery_blocks: 0,
        },
    )
    .await;

    settle_job_completion(&mut pipeline, job_id).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!(
            "an empty override must restore the pre-furniture behaviour; {}",
            debug_job_state(&pipeline, job_id)
        );
    };
    assert!(
        error.contains("not repairable"),
        "unexpected error: {error}"
    );
}

// ---------------------------------------------------------------------------
// Post-verdict re-entry
// ---------------------------------------------------------------------------

/// A settled PAR2 verdict is never re-derived, and a job that keeps coming back
/// to the gate with a protected file outstanding is reported as the bug it is.
///
/// The state is reachable only through a reconciliation defect of ours — here a
/// contested alias, where two assembly entries answer to one description and
/// the binding is refused rather than guessed. The verified bytes are on disk,
/// so nothing about the download is wrong; what used to happen is that the gate
/// re-read the whole recovery set on every lap, forever, at seconds a lap.
#[tokio::test]
async fn a_settled_par2_verdict_is_not_re_read_for_a_reconciliation_defect() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30345);
    let payload_filename = "silver.horizon.mkv";
    let alias_filename = "silver.horizon.extra.bin";
    let index_filename = "silver.horizon.par2";
    let payload = intact_furniture_payload();
    let par2_bytes = build_test_par2_index(payload_filename, &payload, FURNITURE_SLICE_SIZE);

    let spec = JobSpec {
        name: "Silver Horizon Contested Alias".to_string(),
        password: None,
        total_bytes: (payload.len() * 2 + par2_bytes.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 128,
                    message_id: "contested-payload@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: alias_filename.to_string(),
                role: FileRole::from_filename(alias_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: 128,
                    message_id: "contested-alias@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "contested-index@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    // The verified bytes are on disk under the canonical name; neither assembly
    // entry was ever promoted to complete.
    tokio::fs::write(working_dir.join(payload_filename), &payload)
        .await
        .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    write_and_complete_file(&mut pipeline, job_id, 2, index_filename, &par2_bytes).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &payload, FURNITURE_SLICE_SIZE, 0),
        &[(2, index_filename, 0, false)],
    );
    // The second entry answers to the same description through a canonical
    // alias, so the description binds to neither.
    pipeline
        .set_file_identity(
            job_id,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 1,
                source_filename: alias_filename.to_string(),
                current_filename: alias_filename.to_string(),
                canonical_filename: Some(payload_filename.to_string()),
                classification: None,
                classification_source: crate::jobs::record::FileIdentitySource::Declared,
            },
        )
        .expect("recording the contested alias must succeed");
    pipeline.par2_verified.insert(job_id);
    assert!(
        pipeline.incomplete_par2_protected_data_file_count(job_id) > 0,
        "precondition: the gate must still see protected files outstanding"
    );
    pipeline.par2_authoritative_verify_calls = 0;

    pipeline.check_job_completion(job_id).await;
    assert_eq!(
        pipeline.par2_authoritative_verify_calls, 0,
        "a settled verdict must not be re-derived on the retry lap"
    );
    pipeline.check_job_completion(job_id).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!(
            "a reconciliation defect must be reported, not looped on; {}",
            debug_job_state(&pipeline, job_id)
        );
    };
    assert!(
        error.contains("BUG:"),
        "the failure must name itself as ours: {error}"
    );
    assert!(
        error.contains(payload_filename) || error.contains(alias_filename),
        "the failure must name the unbound files: {error}"
    );
    assert_eq!(
        pipeline.par2_authoritative_verify_calls, 0,
        "neither entry may re-read the recovery set"
    );
}

// ---------------------------------------------------------------------------
// Partial recovery volumes
// ---------------------------------------------------------------------------

/// A recovery volume that lost one article still contributes every block whose
/// packet survived it.
///
/// Recovery merges on file *completion*, so a volume one article short counted
/// zero blocks and the job was failed as unrepairable with its intact packets
/// sitting on disk. Both reference downloaders load such a volume's packets
/// individually — each recovery packet carries its own MD5, which is what makes
/// reading past a hole safe.
#[tokio::test]
async fn a_partial_recovery_volume_contributes_its_surviving_blocks() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30350);
    let job_name = "Silver Horizon Partial Volume";

    let fixture = install_partial_volume_par2_job(
        &mut pipeline,
        job_id,
        PartialVolumeJob {
            name: job_name,
            damaged_slices: 3,
            holed_packets: &[1],
        },
    )
    .await;

    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(
            job_id,
            pipeline.par2_served_set_id(job_id).unwrap()
        ),
        2,
        "precondition: only the complete volume's blocks are counted up front"
    );

    pipeline.check_job_completion(job_id).await;
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(
            job_id,
            pipeline.par2_served_set_id(job_id).unwrap()
        ),
        3,
        "the surviving packet of the short volume must be counted; {}",
        debug_job_state(&pipeline, job_id)
    );

    settle_job_completion(&mut pipeline, job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "{}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_repairer_execute_calls, 1,
        "the salvaged block is what makes the repair possible"
    );
    let output_dir = pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(job_name));
    assert_eq!(
        tokio::fs::read(output_dir.join("silver.horizon.mkv"))
            .await
            .unwrap(),
        fixture.payload,
        "the repaired payload must be byte-identical"
    );
}

/// A hole too wide to read past still fails — and the failure counts what was
/// actually salvaged, not what the volume's name advertised.
#[tokio::test]
async fn a_hole_too_wide_to_salvage_past_still_fails() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30351);

    install_partial_volume_par2_job(
        &mut pipeline,
        job_id,
        PartialVolumeJob {
            name: "Silver Horizon Partial Volume Short",
            // Four damaged slices against two whole blocks plus the one packet
            // the short volume still holds.
            damaged_slices: 4,
            holed_packets: &[1],
        },
    )
    .await;

    settle_job_completion(&mut pipeline, job_id).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!(
            "recovery short even after salvage must still fail; {}",
            debug_job_state(&pipeline, job_id)
        );
    };
    assert!(
        error.contains("not repairable"),
        "unexpected error: {error}"
    );
    // The count the failure reports, not which of the two shortfall gates
    // rendered it: the salvaged block is part of the arithmetic even when the
    // arithmetic still comes up short.
    assert!(
        error.contains("only 3 recovery blocks"),
        "the failure must count the salvaged block: {error}"
    );
}

/// The salvage reads a short volume once per download generation.
///
/// Nothing about a volume that can no longer complete changes between gate
/// entries, and the gate is entered many times over a job's post-processing —
/// re-reading it on every lap is the shape that turns a slow path into a hot
/// loop.
#[tokio::test]
async fn a_salvaged_recovery_volume_is_read_once_per_generation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30352);

    install_partial_volume_par2_job(
        &mut pipeline,
        job_id,
        PartialVolumeJob {
            name: "Silver Horizon Partial Volume Once",
            damaged_slices: 3,
            holed_packets: &[1],
        },
    )
    .await;

    pipeline.check_job_completion(job_id).await;
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(
            job_id,
            pipeline.par2_served_set_id(job_id).unwrap()
        ),
        3,
        "first entry salvages the surviving packet; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(pipeline.par2_recovery_salvage_scans, 1);

    pipeline.check_job_completion(job_id).await;
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(
            job_id,
            pipeline.par2_served_set_id(job_id).unwrap()
        ),
        3,
        "a second entry must not re-count what it already merged; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.par2_recovery_salvage_scans, 1,
        "a volume that cannot complete is not re-read on every gate entry"
    );
}

/// A volume that completes after a partial salvage reports the whole volume's
/// blocks, not just the ones the completion merge happened to add.
///
/// Regression guard for the accounting the salvage introduces: the completion
/// merge reports *new* slices, which after a salvage is only the remainder.
#[tokio::test]
async fn a_volume_that_completes_after_salvage_reports_its_whole_block_count() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.stateful_par2_session_forced = Some(true);
    let job_id = JobId(30353);

    let fixture = install_partial_volume_par2_job(
        &mut pipeline,
        job_id,
        PartialVolumeJob {
            name: "Silver Horizon Partial Volume Refetch",
            damaged_slices: 3,
            holed_packets: &[1],
        },
    )
    .await;

    let set_id = pipeline.par2_served_set_id(job_id).unwrap();
    let (session, fresh) = pipeline
        .take_or_open_par2_repair_session(
            job_id,
            set_id,
            fixture.working_dir.clone(),
            8 * 1024 * 1024,
            None,
            None,
        )
        .await
        .unwrap()
        .expect("the partial set opens a retained filesystem session");
    assert!(fresh);
    pipeline.restore_par2_repair_session(job_id, set_id, session);
    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    assert!(
        pipeline
            .par2_runtime(job_id)
            .unwrap()
            .set_runtime(set_id)
            .is_some_and(|set_runtime| set_runtime.session.is_none()),
        "salvage changed the validated set, so the retained session is discarded"
    );
    let (_, fresh) = pipeline
        .take_or_open_par2_repair_session(
            job_id,
            set_id,
            fixture.working_dir.clone(),
            8 * 1024 * 1024,
            None,
            None,
        )
        .await
        .unwrap()
        .expect("salvage reopens the filesystem session from its validated snapshot");
    assert!(fresh);
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(
            job_id,
            pipeline.par2_served_set_id(job_id).unwrap()
        ),
        3
    );

    // The lost article arrives after all: the whole volume lands on disk and the
    // assembly entry completes.
    let mut whole = fixture.short_volume_bytes.clone();
    let slice_bytes = PARTIAL_VOLUME_SLICE_SIZE as usize;
    let packet_len = par2_rs::packet::header::HEADER_SIZE + 4 + slice_bytes;
    let source = build_repairable_par2_set(
        "silver.horizon.mkv",
        &fixture.payload,
        PARTIAL_VOLUME_SLICE_SIZE,
        4,
    );
    let restored = source.recovery_slices[&3]
        .data
        .as_bytes()
        .expect("test recovery slices are built in memory")
        .to_vec();
    let payload_start = packet_len + par2_rs::packet::header::HEADER_SIZE + 4;
    whole[payload_start..payload_start + slice_bytes].copy_from_slice(&restored);
    tokio::fs::write(
        fixture.working_dir.join(&fixture.short_volume_filename),
        &whole,
    )
    .await
    .unwrap();
    let short_volume_id = NzbFileId {
        job_id,
        file_index: 3,
    };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let file = state.assembly.file_mut(short_volume_id).unwrap();
        file.commit_segment(1, slice_bytes as u32).unwrap();
        assert!(file.is_complete());
    }
    pipeline
        .unavailable_promoted_recovery_segments
        .retain(|segment_id| segment_id.file_id != short_volume_id);
    pipeline
        .try_merge_par2_recovery(job_id, short_volume_id)
        .await;

    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(
            job_id,
            pipeline.par2_served_set_id(job_id).unwrap()
        ),
        4,
        "the completed volume reports both of its blocks, not just the remainder"
    );
}

/// A volume read back short keeps the count it proved when a later article of
/// it names the whole volume's size.
///
/// Every decoded article registers what its yEnc name claims the volume carries,
/// and for a volume that stranded holding a fraction of that, the claim is a
/// promise nothing can keep. Letting it stand in for the read-back's count told
/// the repair arithmetic the shortfall was already covered, so it asked for
/// nothing further and waited on articles that had already run out.
#[tokio::test]
async fn a_later_articles_yenc_name_does_not_re_credit_a_salvaged_volume() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30354);

    let fixture = install_partial_volume_par2_job(
        &mut pipeline,
        job_id,
        PartialVolumeJob {
            name: "Silver Horizon Partial Volume Readvertised",
            damaged_slices: 3,
            holed_packets: &[1],
        },
    )
    .await;
    // The fixture seeds a retained set for its partial-volume tests. Discard
    // that shortcut here: replay the normal completed-index, completed-carrier,
    // and first short-volume-article transitions before the corrupt volume is
    // read back.
    pipeline.par2_runtime.remove(&job_id);
    load_par2_index(&mut pipeline, job_id, 1).await;
    pipeline
        .try_merge_par2_recovery(
            job_id,
            NzbFileId {
                job_id,
                file_index: 2,
            },
        )
        .await;
    pipeline.note_recovery_count_from_yenc_name(job_id, 3, &fixture.short_volume_filename);
    let set_id = pipeline.par2_served_set_id(job_id).unwrap();

    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(job_id, set_id),
        3,
        "precondition: the whole volume's two blocks and the one that survived; {}",
        debug_job_state(&pipeline, job_id)
    );

    // The short volume's name advertises two blocks, and the decode path reads
    // that name off every article of it that lands.
    pipeline.note_recovery_count_from_yenc_name(job_id, 3, &fixture.short_volume_filename);

    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(job_id, set_id),
        3,
        "a stranded volume contributes what it proved, not what it advertises; {}",
        debug_job_state(&pipeline, job_id)
    );
}

/// A promoted volume whose segments are parked is waiting, not stranded.
///
/// Parking is where a promoted file's work rests between the promotion and the
/// completion gate that hands it back to the download queue. Reading "nothing in
/// flight" as "cannot complete" makes that window look terminal, and the volume
/// is read back for the fraction of itself that happens to be on disk.
#[tokio::test]
async fn a_promoted_volume_whose_segments_are_parked_is_not_read_back() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30355);

    install_partial_volume_par2_job(
        &mut pipeline,
        job_id,
        PartialVolumeJob {
            name: "Silver Horizon Partial Volume Parked",
            damaged_slices: 3,
            holed_packets: &[1],
        },
    )
    .await;
    let set_id = pipeline.par2_served_set_id(job_id).unwrap();
    let short_volume_id = NzbFileId {
        job_id,
        file_index: 3,
    };
    let missing_segment = SegmentId {
        file_id: short_volume_id,
        segment_number: 1,
    };

    // The fixture's article has run out of servers. Put it back in the state it
    // passes through first: parked, waiting for the gate to promote it.
    pipeline
        .unavailable_promoted_recovery_segments
        .retain(|segment_id| *segment_id != missing_segment);
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.recovery_queue.push(DownloadWork {
            segment_id: missing_segment,
            message_id: MessageId::new("silver-horizon-parked-vol02-1@example.com"),
            groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
            priority: 1000,
            byte_estimate: PARTIAL_VOLUME_SLICE_SIZE as u32,
            retry_count: 0,
            is_recovery: true,
            completion_critical: false,
            exclude_servers: Vec::new(),
            avoid_server: None,
        });
    }

    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    assert_eq!(
        pipeline.par2_recovery_salvage_scans,
        0,
        "a volume whose work is parked has not finished arriving; {}",
        debug_job_state(&pipeline, job_id)
    );

    // Now the article really is gone.
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.recovery_queue.drain_all();
    }
    pipeline
        .unavailable_promoted_recovery_segments
        .insert(missing_segment);

    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    assert_eq!(
        pipeline.par2_recovery_salvage_scans,
        1,
        "a volume that can no longer complete is read back; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(job_id, set_id),
        3,
        "{}",
        debug_job_state(&pipeline, job_id)
    );
}

/// A volume read back short is read again once more of it lands.
///
/// One read-back per *generation of bytes*, not one ever. A volume can strand,
/// be read for what it holds, take another article and strand again — and the
/// second stranding has more on disk than the first read saw. Latching the
/// read-back to the file for good left those blocks unaccounted for the rest of
/// the job, because the latch is only ever cleared by a completion the volume
/// will never reach.
#[tokio::test]
async fn a_volume_read_back_short_is_read_again_once_more_of_it_lands() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30356);

    let fixture = install_growing_partial_volume_par2_job(&mut pipeline, job_id).await;
    let set_id = pipeline.par2_served_set_id(job_id).unwrap();

    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    assert_eq!(pipeline.par2_recovery_salvage_scans, 1);
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(job_id, set_id),
        2,
        "the whole volume's block and the one article of the short volume; {}",
        debug_job_state(&pipeline, job_id)
    );

    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    assert_eq!(
        pipeline.par2_recovery_salvage_scans, 1,
        "nothing about the volume has moved, so it is not read again"
    );

    // The volume's last article lands. It still cannot complete — the middle one
    // has run out of servers — but there is more on disk than the first read saw.
    tokio::fs::write(
        fixture.working_dir.join(&fixture.volume_filename),
        fixture.on_disk(&[0, 2]),
    )
    .await
    .unwrap();
    let packet_len = fixture.packet_len() as u32;
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let file = state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 3,
            })
            .unwrap();
        file.record_placement(2, 2 * packet_len as u64, packet_len);
        file.commit_segment(2, packet_len).unwrap();
        assert!(!file.is_complete());
    }

    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    assert_eq!(
        pipeline.par2_recovery_salvage_scans,
        2,
        "more bytes landed, so the volume is read again; {}",
        debug_job_state(&pipeline, job_id)
    );
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(job_id, set_id),
        3,
        "the second read-back adds to the first rather than replacing it; {}",
        debug_job_state(&pipeline, job_id)
    );
}

/// A repair short of blocks that nothing can still deliver is failed, not
/// waited on.
///
/// The wait branch fails only when *capacity* is short. A job whose targeted
/// total already covers the damage promotes nothing, and with nothing queued,
/// active, retrying or decoding there is no arrival that could raise the
/// available count — the one pass that could, the read-back, ran on the way in.
/// The branch nonetheless moved the job to `Downloading` and returned, forever.
#[tokio::test]
async fn a_repair_waiting_on_recovery_that_cannot_arrive_is_failed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30357);
    let payload_filename = "onyx.prairie.mkv";
    let index_filename = "onyx.prairie.par2";
    // A volume that arrived whole and whose bytes yielded no packet at all: its
    // name is the only thing that ever said how much recovery it carries.
    let recovery_filename = "onyx.prairie.vol00+02.par2";
    let original_payload: Vec<u8> = (0..128u32).map(|value| ((value * 7) % 251) as u8).collect();
    let damaged_payload = vec![0u8; original_payload.len()];
    let par2_bytes = build_test_par2_index(payload_filename, &original_payload, 64);
    let recovery_bytes = vec![0x55u8; 64];
    let spec = JobSpec {
        name: "Onyx Prairie Unreachable Recovery".to_string(),
        password: None,
        total_bytes: (original_payload.len() + par2_bytes.len() + recovery_bytes.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: payload_filename.to_string(),
                role: FileRole::from_filename(payload_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![
                    segment_spec! {
                        number: 0,
                        bytes: 64,
                        message_id: "onyx-prairie-payload-0@example.com".to_string(),
                    },
                    segment_spec! {
                        number: 1,
                        bytes: 64,
                        message_id: "onyx-prairie-payload-1@example.com".to_string(),
                    },
                ],
            },
            FileSpec {
                filename: index_filename.to_string(),
                role: FileRole::from_filename(index_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: par2_bytes.len() as u32,
                    message_id: "onyx-prairie-index@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: recovery_filename.to_string(),
                role: FileRole::from_filename(recovery_filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: recovery_bytes.len() as u32,
                    message_id: "onyx-prairie-recovery@example.com".to_string(),
                }],
            },
        ],
    };
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    tokio::fs::write(working_dir.join(payload_filename), &damaged_payload)
        .await
        .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    write_and_complete_file(&mut pipeline, job_id, 1, index_filename, &par2_bytes).await;
    write_and_complete_file(&mut pipeline, job_id, 2, recovery_filename, &recovery_bytes).await;
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set(payload_filename, &original_payload, 64, 0),
        &[
            (1, index_filename, 0, false),
            (2, recovery_filename, 2, true),
        ],
    );

    settle_job_completion(&mut pipeline, job_id).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!(
            "a wait no arrival can end must be a failure; {}",
            debug_job_state(&pipeline, job_id)
        );
    };
    assert!(
        error.contains("no further recovery can arrive"),
        "the failure must say why waiting is pointless: {error}"
    );
}

/// The state the hung job reached, end to end: one volume complete, one
/// stranded holding a fraction of what it advertises, and nothing left in
/// flight.
#[tokio::test]
async fn a_stranded_volume_advertising_more_than_it_holds_fails_instead_of_waiting() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30358);

    let fixture = install_partial_volume_par2_job(
        &mut pipeline,
        job_id,
        PartialVolumeJob {
            name: "Silver Horizon Partial Volume Stall",
            damaged_slices: 4,
            holed_packets: &[1],
        },
    )
    .await;

    // The order the hung job reached it in: the volume is read back for what it
    // holds, and only afterwards does an article of it decode and register what
    // its name claims.
    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    pipeline.note_recovery_count_from_yenc_name(job_id, 3, &fixture.short_volume_filename);

    settle_job_completion(&mut pipeline, job_id).await;

    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!(
            "a repair that can never afford its damage must be failed, not waited on; {}",
            debug_job_state(&pipeline, job_id)
        );
    };
    assert!(
        error.contains("not repairable"),
        "unexpected error: {error}"
    );
    assert!(
        error.contains("only 3 recovery blocks"),
        "the failure must count what the volume proved: {error}"
    );
}

/// The same shape with the damage inside what the read-back recovered: the
/// repair runs.
///
/// Guard for the failure above — counting a stranded volume honestly must not
/// turn a job that can afford its damage into a failure.
#[tokio::test]
async fn a_stranded_volume_whose_read_back_covers_the_damage_still_repairs() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30359);
    let job_name = "Silver Horizon Partial Volume Stall Averted";

    let fixture = install_partial_volume_par2_job(
        &mut pipeline,
        job_id,
        PartialVolumeJob {
            name: job_name,
            damaged_slices: 3,
            holed_packets: &[1],
        },
    )
    .await;

    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    pipeline.note_recovery_count_from_yenc_name(job_id, 3, &fixture.short_volume_filename);

    settle_job_completion(&mut pipeline, job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "{}",
        debug_job_state(&pipeline, job_id)
    );
    let output_dir = pipeline
        .complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname(job_name));
    assert_eq!(
        tokio::fs::read(output_dir.join("silver.horizon.mkv"))
            .await
            .unwrap(),
        fixture.payload,
        "the repaired payload must be byte-identical"
    );
}

// ---------------------------------------------------------------------------
// Postings carrying more than one recovery set
// ---------------------------------------------------------------------------

#[tokio::test]
async fn metadata_discovery_bootstraps_one_indexless_carrier_per_collection() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30799);
    let first = "release.vol00+01.par2";
    let second = "release.vol01+01.par2";
    let spec = JobSpec {
        name: "Bounded PAR2 Metadata Bootstrap".to_string(),
        password: None,
        total_bytes: 192,
        category: None,
        metadata: vec![],
        files: [first, second]
            .into_iter()
            .enumerate()
            .map(|(index, filename)| FileSpec {
                filename: filename.to_string(),
                role: FileRole::from_filename(filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: if index == 0 { 64 } else { 128 },
                    message_id: format!("bounded-metadata-{index}@example.com"),
                }],
            })
            .collect(),
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    let set = minimal_par2_file_set();
    let set_id = set.recovery_set_id;
    observe_recovery_prefix(&mut pipeline, job_id, 0, set_id);
    assert_eq!(
        pipeline.next_par2_metadata_action(job_id),
        Some((0, false, Some(set_id))),
        "one authenticated prefix selects its own carrier before any sibling"
    );

    {
        let file = pipeline
            .ensure_par2_runtime(job_id)
            .files
            .get_mut(&0)
            .unwrap();
        file.metadata_targets_attempted.insert(set_id);
        file.discovery = Par2DiscoveryState::Exhausted {
            set_ids: vec![set_id],
        };
    }
    assert_eq!(
        pipeline.next_par2_metadata_action(job_id),
        Some((1, true, None)),
        "a sibling is touched only after the selected carrier is exhausted"
    );

    {
        let runtime = pipeline.ensure_par2_runtime(job_id);
        runtime.ensure_set_runtime(set_id).set = Some(Arc::new(set));
        runtime.files.get_mut(&0).unwrap().discovery = Par2DiscoveryState::Parsed {
            set_ids: vec![set_id],
        };
    }
    assert_eq!(
        pipeline.next_par2_metadata_action(job_id),
        None,
        "installed metadata closes this collection without downloading recovery siblings"
    );
    assert!(pipeline.par2_metadata_discovery_closed(job_id));
}

#[tokio::test]
async fn two_indexless_metadata_carriers_build_two_mixed_grid_sets() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30800);
    let first_payload = vec![0x31; 256];
    let second_payload = vec![0x72; 288];
    let first_metadata = build_test_par2_index_for_files(&[("first.bin", &first_payload)], 64);
    let second_metadata = build_test_par2_index_for_files(&[("second.bin", &second_payload)], 96);
    let first_carrier = "opaque-a.vol00+01.par2";
    let second_carrier = "opaque-b.vol00+01.par2";
    let spec = JobSpec {
        name: "Two Indexless Recovery Sets".to_string(),
        password: None,
        total_bytes: (first_payload.len() + second_payload.len()) as u64,
        category: None,
        metadata: vec![],
        files: vec![
            FileSpec {
                filename: first_carrier.to_string(),
                role: FileRole::from_filename(first_carrier),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: first_metadata.len() as u32,
                    message_id: "indexless-first@example.com".to_string(),
                }],
            },
            FileSpec {
                filename: second_carrier.to_string(),
                role: FileRole::from_filename(second_carrier),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: second_metadata.len() as u32,
                    message_id: "indexless-second@example.com".to_string(),
                }],
            },
        ],
    };
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(&mut pipeline, job_id, 0, first_carrier, &first_metadata).await;
    write_and_complete_file(&mut pipeline, job_id, 1, second_carrier, &second_metadata).await;

    load_par2_index(&mut pipeline, job_id, 0).await;
    load_par2_index(&mut pipeline, job_id, 1).await;

    let runtime = pipeline.par2_runtime(job_id).unwrap();
    let mut slice_sizes = runtime
        .sets
        .values()
        .filter_map(|runtime| runtime.set.as_ref().map(|set| set.slice_size))
        .collect::<Vec<_>>();
    slice_sizes.sort_unstable();
    assert_eq!(slice_sizes, vec![64, 96]);
    let expected_plan = weaver_yenc::CheckpointPlan::from_slice_sizes([64, 96]).plan;
    assert_eq!(runtime.checkpoint_plan.as_ref(), Some(&expected_plan));
    assert!(
        runtime
            .files
            .values()
            .all(|file| matches!(file.discovery, Par2DiscoveryState::Parsed { .. }))
    );
}

#[tokio::test]
async fn checkpoint_grid_admission_keeps_only_the_overflow_sentinel() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30807);
    let payload = vec![0x5a; 128];
    let grid_count = weaver_yenc::MAX_CHECKPOINT_GRIDS + 2;
    let metadata = (0..grid_count)
        .map(|index| {
            let filename = format!("grid-{index}.par2");
            let bytes = build_test_par2_index(
                &format!("grid-payload-{index}.bin"),
                &payload,
                64 + (index as u64 * 4),
            );
            (filename, bytes)
        })
        .collect::<Vec<_>>();
    let spec = JobSpec {
        name: "Checkpoint Grid Admission Cap".to_string(),
        password: None,
        total_bytes: metadata.iter().map(|(_, bytes)| bytes.len() as u64).sum(),
        category: None,
        metadata: vec![],
        files: metadata
            .iter()
            .enumerate()
            .map(|(index, (filename, bytes))| FileSpec {
                filename: filename.clone(),
                role: FileRole::from_filename(filename),
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: vec![segment_spec! {
                    number: 0,
                    bytes: bytes.len() as u32,
                    message_id: format!("checkpoint-grid-{index}@example.com"),
                }],
            })
            .collect(),
    };
    insert_active_job(&mut pipeline, job_id, spec).await;

    for (index, (filename, bytes)) in metadata.iter().enumerate() {
        write_and_complete_file(&mut pipeline, job_id, index as u32, filename, bytes).await;
        load_par2_index(&mut pipeline, job_id, index as u32).await;
    }

    let runtime = pipeline.par2_runtime(job_id).unwrap();
    assert_eq!(runtime.sets.len(), grid_count);
    assert_eq!(
        runtime.admitted_checkpoint_sizes.len(),
        weaver_yenc::MAX_CHECKPOINT_GRIDS + 1,
        "one retained extra size is the permanent overflow sentinel"
    );
    assert_eq!(
        runtime.checkpoint_plan,
        Some(weaver_yenc::CheckpointPlan::None)
    );

    pipeline.refresh_par2_checkpoint_plan(job_id);
    assert_eq!(
        pipeline
            .par2_runtime(job_id)
            .unwrap()
            .admitted_checkpoint_sizes
            .len(),
        weaver_yenc::MAX_CHECKPOINT_GRIDS + 1,
        "later parsed sets cannot grow or rebuild a degraded plan"
    );
}

/// Whichever index lands first, the set protecting the most payload is served.
///
/// Before this, the last index to be parsed simply replaced the set — so a live
/// job served whichever index finished downloading last, and the same job
/// replayed from disk served whichever came last in the posting. The two need
/// not be the same set, which makes a repair's outcome depend on arrival order.
#[tokio::test]
async fn the_larger_recovery_set_is_served_whichever_index_lands_first() {
    let posting = TwoSetPosting::build();
    let expected = TwoSetPosting::recovery_set_id(&posting.larger_index);

    let larger_first_dir = tempfile::tempdir().unwrap();
    let (mut larger_first, _, _) = new_direct_pipeline(&larger_first_dir).await;
    let job_id = JobId(30801);
    posting.install(&mut larger_first, job_id).await;
    load_par2_index(&mut larger_first, job_id, 1).await;
    load_par2_index(&mut larger_first, job_id, 4).await;

    let smaller_first_dir = tempfile::tempdir().unwrap();
    let (mut smaller_first, _, _) = new_direct_pipeline(&smaller_first_dir).await;
    posting.install(&mut smaller_first, job_id).await;
    load_par2_index(&mut smaller_first, job_id, 4).await;
    load_par2_index(&mut smaller_first, job_id, 1).await;

    let larger_first_id = larger_first
        .par2_set(job_id)
        .expect("a set must be served")
        .recovery_set_id;
    let smaller_first_id = smaller_first
        .par2_set(job_id)
        .expect("a set must be served")
        .recovery_set_id;

    assert_eq!(
        larger_first_id, smaller_first_id,
        "arrival order must not decide which recovery set a job serves"
    );
    assert_eq!(
        larger_first_id, expected,
        "the set protecting the most payload is the one worth serving"
    );
    assert!(
        served_set_describes(&smaller_first, job_id, LARGER_PAYLOAD),
        "the served set must describe the larger payload"
    );
    assert!(
        !served_set_describes(&smaller_first, job_id, SMALLER_PAYLOAD),
        "the unserved set's file must not appear in the served set"
    );
}

/// The other set's recovery volumes are not this repair's recovery blocks.
///
/// They repair the other set's files, so counting them advertises capacity for
/// a repair they can take no part in — and sends the fetcher after blocks that
/// will never help.
#[tokio::test]
async fn another_recovery_sets_volumes_do_not_count_toward_the_served_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30802);
    let posting = TwoSetPosting::build();
    posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    load_par2_index(&mut pipeline, job_id, 4).await;
    let larger_set_id = TwoSetPosting::recovery_set_id(&posting.larger_index);
    observe_recovery_prefix(&mut pipeline, job_id, 2, larger_set_id);

    assert_eq!(
        pipeline
            .total_recovery_block_capacity(job_id, pipeline.par2_served_set_id(job_id).unwrap()),
        8,
        "only the served set's volume advertises capacity for this repair"
    );

    // The other set's volume lands whole. Its packets are real, they validate,
    // and every one of them answers to the set this job does not serve.
    write_and_complete_file(
        &mut pipeline,
        job_id,
        5,
        SMALLER_VOLUME,
        &posting.smaller_volume,
    )
    .await;
    pipeline
        .try_merge_par2_recovery(
            job_id,
            NzbFileId {
                job_id,
                file_index: 5,
            },
        )
        .await;

    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(
            job_id,
            pipeline.par2_served_set_id(job_id).unwrap()
        ),
        0,
        "a completed volume of another set contributes nothing to this repair"
    );
}

/// A file can retain known coverage when its set's index never arrives.
///
/// A foreign packet identifies the recovery set, but without an index no pass
/// can verify or repair its files. That remains distinct from an unprotected
/// file: delivery is the same, while the diagnostic explains why no recovery
/// action was possible.
#[tokio::test]
async fn a_file_of_an_unservable_recovery_set_is_not_reported_as_unprotected() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30803);
    let posting = TwoSetPosting::build();
    let working_dir = posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    let smaller_set_id = TwoSetPosting::recovery_set_id(&posting.smaller_index);
    write_and_complete_file(
        &mut pipeline,
        job_id,
        5,
        SMALLER_VOLUME,
        &posting.smaller_volume,
    )
    .await;
    pipeline
        .try_merge_par2_recovery(
            job_id,
            NzbFileId {
                job_id,
                file_index: 5,
            },
        )
        .await;
    let unservable = pipeline
        .ensure_par2_runtime(job_id)
        .set_runtime_mut(smaller_set_id)
        .expect("foreign packets must retain their recovery set");
    assert!(unservable.set.is_none());
    unservable
        .summary
        .described_filenames
        .push(SMALLER_PAYLOAD.to_string());

    tokio::fs::write(working_dir.join(LARGER_PAYLOAD), &posting.larger_payload)
        .await
        .unwrap();
    tokio::fs::write(
        working_dir.join(SMALLER_PAYLOAD),
        &posting.smaller_payload[..64],
    )
    .await
    .unwrap();
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
        for (file_index, committed) in [(0u32, 2u32), (3, 1)] {
            let file = state
                .assembly
                .file_mut(NzbFileId { job_id, file_index })
                .unwrap();
            for segment in 0..committed {
                file.commit_segment(segment, 64).unwrap();
            }
        }
    }

    let report = pipeline
        .classify_incomplete_after_par2(
            job_id,
            &crate::pipeline::completion::finalize::check::Par2Reconciliation::default(),
            "two recovery sets",
        )
        .expect("the file left short must still be reported");

    assert!(
        report.message.contains("no posted index"),
        "the report must say the file's known set cannot receive a pass: {}",
        report.message
    );
    assert!(
        report.message.contains(SMALLER_PAYLOAD),
        "the report must name the file that came up short: {}",
        report.message
    );
    assert!(
        !report.message.contains("unprotected"),
        "a file a set still covers was never unprotected: {}",
        report.message
    );
    assert_eq!(
        report.unproven_protected, 0,
        "a file of an unservable set is not a reconciliation defect: {}",
        report.message
    );
    assert_eq!(
        pipeline.incomplete_par2_protected_data_file_count(job_id),
        0,
        "nothing a servable set can act on is left, so the gate must not re-arm"
    );
}

/// The announcement is worth exactly one line per job for indexless sets.
///
/// The completion gate is entered many times and no index appears between
/// entries, so repeating the warning would add noise without new information.
#[tokio::test]
async fn an_unservable_recovery_set_is_announced_once_per_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30804);
    let posting = TwoSetPosting::build();
    posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        5,
        SMALLER_VOLUME,
        &posting.smaller_volume,
    )
    .await;
    pipeline
        .try_merge_par2_recovery(
            job_id,
            NzbFileId {
                job_id,
                file_index: 5,
            },
        )
        .await;
    pipeline.warn_unservable_recovery_sets_once(job_id);

    assert_eq!(
        pipeline.par2_unserved_set_warnings, 1,
        "meeting the second set is what there is to say"
    );

    for _ in 0..3 {
        pipeline.warn_unservable_recovery_sets_once(job_id);
    }
    assert_eq!(
        pipeline.par2_unserved_set_warnings, 1,
        "re-entering the gate must not repeat it"
    );
}

/// The ordinary single-set posting is untouched by any of this.
#[tokio::test]
async fn a_single_recovery_set_job_announces_nothing_and_counts_every_volume() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30805);
    let posting = TwoSetPosting::build();
    posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;

    assert_eq!(
        pipeline.par2_unserved_set_warnings, 0,
        "one set is not a multi-set posting"
    );
    assert_eq!(
        pipeline
            .total_recovery_block_capacity(job_id, pipeline.par2_served_set_id(job_id).unwrap()),
        12,
        "with one set known, every recovery volume in the posting still counts"
    );
}

#[tokio::test]
async fn unread_multi_set_volume_is_not_attributed_by_its_filename() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30806);
    let posting = TwoSetPosting::build();
    posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    load_par2_index(&mut pipeline, job_id, 4).await;
    let larger_set_id = TwoSetPosting::recovery_set_id(&posting.larger_index);
    let smaller_set_id = TwoSetPosting::recovery_set_id(&posting.smaller_index);

    assert_eq!(
        pipeline.total_recovery_block_capacity(job_id, larger_set_id),
        0
    );
    assert_eq!(
        pipeline.total_recovery_block_capacity(job_id, smaller_set_id),
        0
    );

    observe_recovery_prefix(&mut pipeline, job_id, 2, larger_set_id);
    assert_eq!(
        pipeline.total_recovery_block_capacity(job_id, larger_set_id),
        8
    );
    assert_eq!(
        pipeline.total_recovery_block_capacity(job_id, smaller_set_id),
        0
    );
}

#[tokio::test]
async fn targeted_promotion_routes_only_to_the_requested_recovery_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30807);
    let posting = TwoSetPosting::build();
    posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    load_par2_index(&mut pipeline, job_id, 4).await;
    let larger_set_id = TwoSetPosting::recovery_set_id(&posting.larger_index);
    let smaller_set_id = TwoSetPosting::recovery_set_id(&posting.smaller_index);
    observe_recovery_prefix(&mut pipeline, job_id, 2, larger_set_id);
    observe_recovery_prefix(&mut pipeline, job_id, 5, smaller_set_id);

    assert_eq!(
        pipeline.promote_recovery_targeted(job_id, smaller_set_id, 4),
        4,
        "the requested set's smallest volume covers all four requested blocks"
    );
    let runtime = pipeline.par2_runtime(job_id).unwrap();
    assert!(
        runtime.files[&5].promoted,
        "the requested set's volume was promoted"
    );
    assert!(
        !runtime.files.get(&2).is_some_and(|file| file.promoted),
        "the other set's volume stayed parked"
    );
}

#[tokio::test]
async fn unread_named_multi_set_volume_is_targeted_without_capacity_credit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30815);
    let posting = TwoSetPosting::build();
    posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    load_par2_index(&mut pipeline, job_id, 4).await;
    let larger_set_id = TwoSetPosting::recovery_set_id(&posting.larger_index);
    let smaller_set_id = TwoSetPosting::recovery_set_id(&posting.smaller_index);

    assert_eq!(
        pipeline.promote_recovery_targeted(job_id, smaller_set_id, 4),
        4,
        "the canonical volume is a targeted download candidate"
    );
    let runtime = pipeline.par2_runtime(job_id).unwrap();
    assert!(
        runtime.files[&5].promoted,
        "the matching named volume was promoted"
    );
    assert!(
        !runtime.files.get(&2).is_some_and(|file| file.promoted),
        "the other parsed set's volume remained parked"
    );
    assert_eq!(
        pipeline.total_recovery_block_capacity(job_id, smaller_set_id),
        0,
        "an unread filename still contributes no recovery capacity"
    );
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(job_id, smaller_set_id),
        0,
        "targeting does not credit recovery blocks before packet validation"
    );
    assert_eq!(
        pipeline.total_recovery_block_capacity(job_id, larger_set_id),
        0,
        "the selected candidate did not affect the other set"
    );
}

#[tokio::test]
async fn a_volume_completed_before_its_index_is_replayed_into_that_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30808);
    let posting = TwoSetPosting::build();
    posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    let smaller_set_id = TwoSetPosting::recovery_set_id(&posting.smaller_index);

    write_and_complete_file(
        &mut pipeline,
        job_id,
        5,
        SMALLER_VOLUME,
        &posting.smaller_volume,
    )
    .await;
    pipeline
        .try_merge_par2_recovery(
            job_id,
            NzbFileId {
                job_id,
                file_index: 5,
            },
        )
        .await;
    assert!(
        pipeline
            .par2_runtime(job_id)
            .unwrap()
            .set_runtime(smaller_set_id)
            .is_some_and(|set_runtime| set_runtime.set.is_none()),
        "the volume can identify its set before that set has an index"
    );

    load_par2_index(&mut pipeline, job_id, 4).await;
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(job_id, smaller_set_id),
        4,
        "installing the index replays the already-complete volume into its own set"
    );
    assert_eq!(
        pipeline
            .par2_set_for(job_id, smaller_set_id)
            .unwrap()
            .recovery_block_count(),
        4,
        "replay must feed the set itself, not only filename-derived arithmetic"
    );
}

#[tokio::test]
async fn recovery_arithmetic_is_strictly_isolated_per_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30809);
    let posting = TwoSetPosting::build();
    posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    load_par2_index(&mut pipeline, job_id, 4).await;
    let larger_set_id = TwoSetPosting::recovery_set_id(&posting.larger_index);
    let smaller_set_id = TwoSetPosting::recovery_set_id(&posting.smaller_index);
    observe_recovery_prefix(&mut pipeline, job_id, 2, larger_set_id);
    observe_recovery_prefix(&mut pipeline, job_id, 5, smaller_set_id);

    assert_eq!(
        pipeline.total_recovery_block_capacity(job_id, larger_set_id),
        8
    );
    assert_eq!(
        pipeline.total_recovery_block_capacity(job_id, smaller_set_id),
        4
    );

    write_and_complete_file(
        &mut pipeline,
        job_id,
        2,
        LARGER_VOLUME,
        &posting.larger_volume,
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        5,
        SMALLER_VOLUME,
        &posting.smaller_volume,
    )
    .await;
    pipeline
        .try_merge_par2_recovery(
            job_id,
            NzbFileId {
                job_id,
                file_index: 2,
            },
        )
        .await;
    pipeline
        .try_merge_par2_recovery(
            job_id,
            NzbFileId {
                job_id,
                file_index: 5,
            },
        )
        .await;

    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(job_id, larger_set_id),
        8
    );
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(job_id, smaller_set_id),
        4
    );
}

#[tokio::test]
async fn a_multi_set_recovery_file_feeds_both_sets_and_counts_for_each() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30810);
    let posting = TwoSetPosting::build();
    let packet_len = par2_rs::packet::header::HEADER_SIZE + 4 + TWO_SET_SLICE_SIZE as usize;
    let mut multi_set_volume = posting.larger_volume[..packet_len].to_vec();
    multi_set_volume.extend_from_slice(&posting.smaller_volume[..packet_len]);
    let mut spec = posting.spec();
    spec.files[5].segments[0].bytes = multi_set_volume.len() as u32;
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        1,
        LARGER_INDEX,
        &posting.larger_index,
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        4,
        SMALLER_INDEX,
        &posting.smaller_index,
    )
    .await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    load_par2_index(&mut pipeline, job_id, 4).await;
    let larger_set_id = TwoSetPosting::recovery_set_id(&posting.larger_index);
    let smaller_set_id = TwoSetPosting::recovery_set_id(&posting.smaller_index);

    // The unread standard volume has a validated prefix for the larger set.
    // Its name alone must not contribute capacity once two sets are known.
    observe_recovery_prefix(&mut pipeline, job_id, 2, larger_set_id);

    write_and_complete_file(&mut pipeline, job_id, 5, SMALLER_VOLUME, &multi_set_volume).await;
    pipeline
        .try_merge_par2_recovery(
            job_id,
            NzbFileId {
                job_id,
                file_index: 5,
            },
        )
        .await;

    let runtime = pipeline.par2_runtime(job_id).unwrap();
    assert_eq!(runtime.files[&5].recovery_set_id, None);
    assert!(runtime.files[&5].recovery_set_packets_read);
    assert_eq!(
        runtime
            .set_runtime(larger_set_id)
            .unwrap()
            .set
            .as_ref()
            .unwrap()
            .recovery_block_count(),
        1
    );
    assert_eq!(
        runtime
            .set_runtime(smaller_set_id)
            .unwrap()
            .set
            .as_ref()
            .unwrap()
            .recovery_block_count(),
        1
    );
    // The blocks are merged into both sets and the repairer counts them, so
    // the arithmetic that decides whether a repair is affordable has to see
    // them too. Attributing the *file* to neither set is still right — no one
    // set owns it — but that is a question about ownership, not about how much
    // recovery each set actually holds.
    assert_eq!(
        pipeline.total_recovery_block_capacity(job_id, larger_set_id),
        9,
        "the larger set's own eight blocks plus the one this file gave it"
    );
    assert_eq!(
        pipeline.total_recovery_block_capacity(job_id, smaller_set_id),
        1,
        "and the smaller set sees the block it was actually given"
    );
}

/// Every carrier keeps only the recovery slices it itself contributed.
///
/// A later metadata-only volume used to copy the set's accumulated total into
/// its own entry, so one real recovery volume was advertised twice.  Its
/// arrival also replaced the explicit index in the deterministic summary.
#[tokio::test]
async fn metadata_only_and_duplicate_carriers_do_not_recount_or_replace_the_index() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30811);
    let posting = TwoSetPosting::build();
    let metadata_carrier = "later-metadata.vol01+01.par2";
    let mut spec = posting.spec();
    spec.files.push(FileSpec {
        filename: metadata_carrier.to_string(),
        role: FileRole::from_filename(metadata_carrier),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: posting.larger_index.len() as u32,
            message_id: "later-metadata@example.com".to_string(),
        }],
    });
    insert_active_job(&mut pipeline, job_id, spec).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        1,
        LARGER_INDEX,
        &posting.larger_index,
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        4,
        SMALLER_INDEX,
        &posting.smaller_index,
    )
    .await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    load_par2_index(&mut pipeline, job_id, 4).await;
    let larger_set_id = TwoSetPosting::recovery_set_id(&posting.larger_index);

    write_and_complete_file(
        &mut pipeline,
        job_id,
        2,
        LARGER_VOLUME,
        &posting.larger_volume,
    )
    .await;
    load_par2_index(&mut pipeline, job_id, 2).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        6,
        metadata_carrier,
        &posting.larger_index,
    )
    .await;
    load_par2_index(&mut pipeline, job_id, 6).await;
    load_par2_index(&mut pipeline, job_id, 6).await;

    let runtime = pipeline.par2_runtime(job_id).unwrap();
    let summary = &runtime.set_runtime(larger_set_id).unwrap().summary;
    assert_eq!(summary.index_file_index, 1);
    assert_eq!(summary.index_filename, LARGER_INDEX);
    assert_eq!(
        runtime.files[&6].recovery_blocks_by_set[&larger_set_id], 0,
        "the metadata-only carrier contributed no recovery slices"
    );
    assert_eq!(
        pipeline.total_recovery_block_capacity(job_id, larger_set_id),
        8,
        "the real volume is counted once despite repeated metadata carriers"
    );
}

/// A completed recovery carrier replaces the retained snapshot with the
/// current validated set before the next filesystem session opens.
#[tokio::test]
async fn completed_recovery_carrier_reopens_the_filesystem_session_from_the_current_set() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.stateful_par2_session_forced = Some(true);
    let job_id = JobId(30817);
    let posting = TwoSetPosting::build();
    let working_dir = posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    let set_id = TwoSetPosting::recovery_set_id(&posting.larger_index);
    let (session, fresh) = pipeline
        .take_or_open_par2_repair_session(
            job_id,
            set_id,
            working_dir.clone(),
            8 * 1024 * 1024,
            None,
            None,
        )
        .await
        .unwrap()
        .expect("the parsed index opens a filesystem-backed session");
    assert!(fresh);
    pipeline.restore_par2_repair_session(job_id, set_id, session);

    write_and_complete_file(
        &mut pipeline,
        job_id,
        2,
        LARGER_VOLUME,
        &posting.larger_volume,
    )
    .await;
    load_par2_index(&mut pipeline, job_id, 2).await;

    assert!(
        pipeline
            .par2_runtime(job_id)
            .unwrap()
            .set_runtime(set_id)
            .and_then(|set_runtime| set_runtime.session.as_ref())
            .is_none(),
        "a recovery arrival invalidates the stale retained snapshot"
    );
    assert_eq!(
        pipeline
            .par2_set_for(job_id, set_id)
            .unwrap()
            .recovery_block_count(),
        8
    );
    let (_, fresh) = pipeline
        .take_or_open_par2_repair_session(job_id, set_id, working_dir, 8 * 1024 * 1024, None, None)
        .await
        .unwrap()
        .expect("the validated set reopens a filesystem-backed session");
    assert!(fresh);
}

/// A completed recovery volume whose packet headers survive but whose payloads
/// do not validate is a final zero-capacity answer, not permission to trust
/// the count encoded in its filename.
#[tokio::test]
async fn completed_payload_corrupt_recovery_volume_contributes_zero_capacity() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.stateful_par2_session_forced = Some(true);
    let job_id = JobId(30818);
    let fixture = install_partial_volume_par2_job(
        &mut pipeline,
        job_id,
        PartialVolumeJob {
            name: "Silver Horizon Completed Corrupt Volume",
            // The repair needs four blocks. The valid complete volume proves
            // two; this volume's headers name the remaining two, but neither
            // payload validates against its packet MD5.
            damaged_slices: 4,
            holed_packets: &[0, 1],
        },
    )
    .await;
    let set_id = pipeline.par2_served_set_id(job_id).unwrap();
    let source_file_id = *pipeline
        .par2_set_for(job_id, set_id)
        .unwrap()
        .files
        .keys()
        .next()
        .unwrap();
    let mut damaged_source = fixture.payload.clone();
    damaged_source[..4 * PARTIAL_VOLUME_SLICE_SIZE as usize].fill(0);
    let mut access = par2_rs::MemoryFileAccess::new();
    access.add_file(source_file_id, damaged_source);
    let source_access: std::sync::Arc<dyn par2_rs::FileAccess + Send + Sync> =
        std::sync::Arc::new(access);
    let (session, fresh) = pipeline
        .take_or_open_par2_repair_session(
            job_id,
            set_id,
            fixture.working_dir.clone(),
            8 * 1024 * 1024,
            None,
            Some(std::sync::Arc::clone(&source_access)),
        )
        .await
        .unwrap()
        .expect("the direct source opens an access-backed session");
    assert!(fresh);
    pipeline.restore_par2_repair_session(job_id, set_id, session);
    let corrupt_packets = par2_rs::scan_packets_from_path_with_set_ids(
        &fixture.working_dir.join(&fixture.short_volume_filename),
    )
    .unwrap()
    .into_iter()
    .filter(|scanned| match &scanned.packet {
        par2_rs::Packet::RecoverySlice(recovery) => !matches!(
            recovery
                .data
                .validate_packet_hash(scanned.recovery_set_id.as_bytes(), recovery.exponent),
            Ok(true)
        ),
        _ => false,
    })
    .count();
    assert_eq!(
        corrupt_packets, 2,
        "test precondition: both header-valid recovery payloads are corrupt"
    );

    let volume_id = NzbFileId {
        job_id,
        file_index: 3,
    };
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let file = state.assembly.file_mut(volume_id).unwrap();
        file.commit_segment(1, PARTIAL_VOLUME_SLICE_SIZE as u32)
            .unwrap();
        assert!(file.is_complete());
    }
    pipeline
        .unavailable_promoted_recovery_segments
        .retain(|segment_id| segment_id.file_id != volume_id);

    pipeline.try_merge_par2_recovery(job_id, volume_id).await;

    assert!(
        pipeline
            .par2_runtime(job_id)
            .unwrap()
            .set_runtime(set_id)
            .and_then(|set_runtime| set_runtime.session.as_ref())
            .is_none(),
        "the corrupt carrier must evict the pre-arrival direct snapshot"
    );
    let (mut session, fresh) = pipeline
        .take_or_open_par2_repair_session(
            job_id,
            set_id,
            fixture.working_dir.clone(),
            8 * 1024 * 1024,
            None,
            Some(source_access),
        )
        .await
        .unwrap()
        .expect("the filtered set reopens an access-backed session");
    assert!(fresh);
    let assessment = session.analyze().unwrap();
    assert_eq!(assessment.recovery_blocks_available, 2);

    assert_eq!(
        pipeline
            .par2_runtime(job_id)
            .unwrap()
            .files
            .get(&volume_id.file_index)
            .and_then(|file| file.recovery_blocks_by_set.get(&set_id)),
        Some(&0),
        "the completed carrier records an exact zero instead of a filename estimate"
    );

    assert_eq!(
        pipeline.total_recovery_block_capacity(job_id, set_id),
        2,
        "the complete payload-corrupt carrier contributes exactly zero blocks"
    );
    settle_job_completion(&mut pipeline, job_id).await;
    let Some(JobStatus::Failed { error }) = job_status_for_assert(&pipeline, job_id) else {
        panic!(
            "a repair short by two invalid packets must fail; {}",
            debug_job_state(&pipeline, job_id)
        );
    };
    assert!(
        error.contains("not repairable"),
        "unexpected error: {error}"
    );
    assert!(
        error.contains("only 2 recovery blocks"),
        "the failure must report only valid recovery payloads: {error}"
    );
}

#[tokio::test]
async fn a_second_index_of_an_unserved_set_merges_its_packets() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30811);
    let posting = TwoSetPosting::build();
    let working_dir = posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    load_par2_index(&mut pipeline, job_id, 4).await;
    let smaller_set_id = TwoSetPosting::recovery_set_id(&posting.smaller_index);
    let mut second_index = posting.smaller_index.clone();
    second_index.extend_from_slice(&posting.smaller_volume);
    tokio::fs::write(working_dir.join(SMALLER_INDEX), second_index)
        .await
        .unwrap();

    load_par2_index(&mut pipeline, job_id, 4).await;
    assert_eq!(
        pipeline
            .par2_set_for(job_id, smaller_set_id)
            .unwrap()
            .recovery_block_count(),
        4,
        "the later index augments the non-served set instead of replacing it"
    );
}

#[tokio::test]
async fn restart_replay_rebuilds_every_set_and_its_volumes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30812);
    let posting = TwoSetPosting::build();
    posting.install(&mut pipeline, job_id).await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        2,
        LARGER_VOLUME,
        &posting.larger_volume,
    )
    .await;
    write_and_complete_file(
        &mut pipeline,
        job_id,
        5,
        SMALLER_VOLUME,
        &posting.smaller_volume,
    )
    .await;
    let larger_set_id = TwoSetPosting::recovery_set_id(&posting.larger_index);
    let smaller_set_id = TwoSetPosting::recovery_set_id(&posting.smaller_index);

    pipeline.restore_par2_state_from_disk(job_id).await;

    assert_eq!(
        pipeline
            .par2_set_for(job_id, larger_set_id)
            .unwrap()
            .recovery_block_count(),
        8
    );
    assert_eq!(
        pipeline
            .par2_set_for(job_id, smaller_set_id)
            .unwrap()
            .recovery_block_count(),
        4
    );
}

#[tokio::test]
async fn a_complete_volume_bootstraps_its_recovery_set_without_its_index() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30815);
    let (spec, volume, recovery_set_id) = volume_only_par2_bootstrap_fixture();
    insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_file(&mut pipeline, job_id, 2, VOLUME_BOOTSTRAP_VOLUME, &volume).await;
    pipeline
        .try_merge_par2_recovery(
            job_id,
            NzbFileId {
                job_id,
                file_index: 2,
            },
        )
        .await;

    let set = pipeline
        .par2_set_for(job_id, recovery_set_id)
        .expect("the complete volume carries enough metadata to establish its set");
    assert!(
        set.files
            .values()
            .any(|file| file.filename == VOLUME_BOOTSTRAP_PAYLOAD),
        "the volume-built set retains its file descriptions"
    );
    assert_eq!(set.recovery_block_count(), 1);
    let expected_plan = weaver_yenc::CheckpointPlan::from_slice_sizes([set.slice_size]).plan;
    assert_eq!(pipeline.par2_checkpoint_plan(job_id), expected_plan);
    assert_eq!(
        pipeline.recovery_blocks_available_or_targeted(job_id, recovery_set_id),
        1,
        "the volume's recovery block is available to its newly established set"
    );
}

#[tokio::test]
async fn restart_replay_bootstraps_a_set_from_its_only_complete_volume() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30816);
    let (spec, volume, recovery_set_id) = volume_only_par2_bootstrap_fixture();
    insert_active_job(&mut pipeline, job_id, spec).await;

    write_and_complete_file(&mut pipeline, job_id, 2, VOLUME_BOOTSTRAP_VOLUME, &volume).await;
    pipeline.restore_par2_state_from_disk(job_id).await;

    let set = pipeline
        .par2_set_for(job_id, recovery_set_id)
        .expect("restart replay must rebuild a set from the surviving volume");
    assert!(
        set.files
            .values()
            .any(|file| file.filename == VOLUME_BOOTSTRAP_PAYLOAD)
    );
    assert_eq!(set.recovery_block_count(), 1);
}

#[test]
fn retained_sessions_evict_the_oldest_unprotected_job_and_set_pair() {
    let job_id = JobId(30813);
    let older = (job_id, par2_rs::RecoverySetId::from_bytes([1; 16]));
    let protected = (job_id, par2_rs::RecoverySetId::from_bytes([2; 16]));
    let now = std::time::Instant::now();

    assert_eq!(
        crate::pipeline::repair::par2::select_par2_session_eviction(
            [
                (older, true, Some(now - std::time::Duration::from_secs(2))),
                (
                    protected,
                    true,
                    Some(now - std::time::Duration::from_secs(1))
                ),
            ],
            protected,
        ),
        Some(older),
        "the unprotected set of the same job remains eligible for LRU eviction"
    );
}

#[tokio::test]
async fn a_single_set_keeps_capacity_promotion_salvage_and_sessions_available() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.stateful_par2_session_forced = Some(true);
    let job_id = JobId(30814);
    let posting = TwoSetPosting::build();
    let working_dir = posting.install(&mut pipeline, job_id).await;
    load_par2_index(&mut pipeline, job_id, 1).await;
    let set_id = TwoSetPosting::recovery_set_id(&posting.larger_index);

    assert_eq!(pipeline.total_recovery_block_capacity(job_id, set_id), 12);
    assert_eq!(pipeline.promote_recovery_targeted(job_id, set_id, 4), 4);
    pipeline
        .salvage_partial_promoted_recovery_volumes(job_id)
        .await;
    let session = pipeline
        .take_or_open_par2_repair_session(job_id, set_id, working_dir, 8 * 1024 * 1024, None, None)
        .await
        .unwrap()
        .expect("the single set has an index path for a retained session")
        .0;
    pipeline.restore_par2_repair_session(job_id, set_id, session);
    assert!(
        pipeline
            .par2_runtime(job_id)
            .unwrap()
            .set_runtime(set_id)
            .is_some_and(|set_runtime| set_runtime.session.is_some())
    );
}

// ---------------------------------------------------------------------------
// A recovery set that describes the joined file, against a posting of parts
// ---------------------------------------------------------------------------

/// A recovery set naming the joined file retires the split topology once it has
/// vouched for the join.
///
/// The parts carry the payload, the recovery data speaks for the file they
/// concatenate into, and the repair puts that file on disk from the parts
/// themselves. Joining them a second time afterwards would write the damaged
/// bytes back over the repaired ones, which is what shipped before: the
/// concatenation lands in staging, and staging wins the name in the final move.
#[tokio::test]
async fn a_recovery_set_naming_the_joined_file_retires_the_split_topology() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut events = pipeline.event_tx.subscribe();
    let job_id = JobId(30820);
    let job_name = "Ivory Meadow Split Join";
    let joined = split_join_payload(192);
    let posting = SplitJoinPosting {
        job_name,
        joined: joined.clone(),
        slice_size: 64,
        parts: vec![
            whole_split_join_part("Ivory.Meadow.mkv.001", &joined[0..64]),
            // The middle part's slice is a hole on disk. Only reading the parts
            // as one file puts it back.
            SplitJoinPart {
                filename: "Ivory.Meadow.mkv.002".to_string(),
                segments: vec![64],
                arrived_segments: 1,
                on_disk: vec![0u8; 64],
            },
            whole_split_join_part("Ivory.Meadow.mkv.003", &joined[128..192]),
        ],
        prefix_captured: Vec::new(),
        describes_parts: false,
    };
    posting.install(&mut pipeline, job_id).await;

    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .archive_topology_for(SPLIT_JOIN_JOINED_FILENAME)
            .is_some(),
        "precondition: the parts registered a split topology under the joined name"
    );

    pipeline.check_job_completion(job_id).await;
    // The verdict that vouches for the joined file comes back from a blocking
    // worker, so the topology is only retired once that message is serviced.
    settle_par2_analysis_work(&mut pipeline).await;

    assert!(
        pipeline.jobs.get(&job_id).is_none_or(|state| state
            .assembly
            .archive_topology_for(SPLIT_JOIN_JOINED_FILENAME)
            .is_none()),
        "a verdict that vouched for the joined file must retire the topology that \
         would rebuild it; {}",
        debug_job_state(&pipeline, job_id)
    );

    settle_split_join_completion(&mut pipeline, job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "error = {}",
        split_join_failure_error(&pipeline, job_id)
    );

    let drained = drain_job_events(&mut events, job_id);
    assert_eq!(
        drained
            .iter()
            .filter(|event| matches!(event, PipelineEvent::RepairComplete { .. }))
            .count(),
        1,
        "exactly one repair is announced; events = {drained:?}"
    );

    let delivered = split_join_delivered_dir(&pipeline, job_name);
    assert_eq!(
        tokio::fs::read(delivered.join(SPLIT_JOIN_JOINED_FILENAME))
            .await
            .unwrap(),
        joined,
        "the delivered join must be the repaired bytes, not a second concatenation"
    );
    let names = delivered_entry_names(&delivered);
    assert!(
        !names
            .iter()
            .any(|name| name.starts_with("Ivory.Meadow.mkv.")),
        "the parts a verified join consumed are not part of the release; delivered = {names:?}"
    );
}

/// A part short of its articles must not fail a job whose payload PAR2 has
/// already rebuilt.
///
/// With the joined file verified on disk, the split topology still wanted every
/// part whole, reported "no volumes are complete yet", and the completion gate
/// failed the job on that reason — after `RepairComplete` had already told the
/// UI the repair held.
#[tokio::test]
async fn a_split_part_short_of_articles_does_not_fail_a_rejoined_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30821);
    let job_name = "Ivory Meadow Short Part";
    let joined = split_join_payload(192);
    let posting = SplitJoinPosting {
        job_name,
        joined: joined.clone(),
        slice_size: 64,
        parts: vec![
            whole_split_join_part("Ivory.Meadow.mkv.001", &joined[0..64]),
            // The second of this part's two articles never arrived, so the
            // part can never be called complete and its slice is short on disk.
            SplitJoinPart {
                filename: "Ivory.Meadow.mkv.002".to_string(),
                segments: vec![32, 32],
                arrived_segments: 1,
                on_disk: joined[64..96].to_vec(),
            },
            whole_split_join_part("Ivory.Meadow.mkv.003", &joined[128..192]),
        ],
        prefix_captured: Vec::new(),
        describes_parts: false,
    };
    posting.install(&mut pipeline, job_id).await;

    pipeline.check_job_completion(job_id).await;
    // Same detached verdict: the join is only vouched for once the analysis
    // message lands back on the pipeline task.
    settle_par2_analysis_work(&mut pipeline).await;

    assert!(
        pipeline
            .classify_incomplete_after_par2(
                job_id,
                &crate::pipeline::completion::finalize::check::Par2Reconciliation::default(),
                "post-verdict probe"
            )
            .is_none(),
        "a part a verified join consumed belongs in no incomplete bucket; {}",
        debug_job_state(&pipeline, job_id)
    );

    settle_split_join_completion(&mut pipeline, job_id).await;

    let error = split_join_failure_error(&pipeline, job_id);
    assert!(
        !error.contains("no volumes are complete yet"),
        "a part short of articles is not a reason to fail a join PAR2 rebuilt; error = {error}"
    );
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "error = {error}"
    );

    let delivered = split_join_delivered_dir(&pipeline, job_name);
    assert_eq!(
        tokio::fs::read(delivered.join(SPLIT_JOIN_JOINED_FILENAME))
            .await
            .unwrap(),
        joined,
        "the delivered join must be the repaired bytes"
    );
}

/// A split fragment is not PAR2-protected before any join verdict exists.
#[tokio::test]
async fn a_part_sharing_the_joined_files_first_16_kib_is_unprotected_before_join_verdict() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30821);
    let slice_size = PAR2_HASH_16K_BYTES;
    let joined = split_join_payload(slice_size * 4);
    let posting = SplitJoinPosting {
        job_name: "Ivory Meadow Fragment Before Verdict",
        joined: joined.clone(),
        slice_size: slice_size as u64,
        parts: vec![
            SplitJoinPart {
                filename: "Ivory.Meadow.mkv.001".to_string(),
                segments: vec![slice_size as u32, slice_size as u32],
                arrived_segments: 1,
                on_disk: joined[..slice_size].to_vec(),
            },
            whole_split_join_part(
                "Ivory.Meadow.mkv.002",
                &joined[slice_size * 2..slice_size * 3],
            ),
            whole_split_join_part(
                "Ivory.Meadow.mkv.003",
                &joined[slice_size * 3..slice_size * 4],
            ),
        ],
        prefix_captured: vec![0],
        describes_parts: false,
    };
    posting.install(&mut pipeline, job_id).await;

    assert_eq!(
        pipeline.incomplete_par2_protected_data_file_count(job_id),
        0,
        "a numeric split fragment must not content-bind to the joined description"
    );
    let report = pipeline
        .classify_incomplete_after_par2(
            job_id,
            &crate::pipeline::completion::finalize::check::Par2Reconciliation::default(),
            "pre-verdict split fragment probe",
        )
        .expect("the incomplete fragment must still be classified");
    assert_eq!(
        report.unproven_protected, 0,
        "the fragment must not enter the protected bucket: {}",
        report.message
    );
    assert!(
        report.message.contains("unprotected"),
        "the report must identify the fragment as unprotected: {}",
        report.message
    );
}

/// A part sharing the joined file's first 16 KiB is not a hole in the payload.
///
/// The first part begins where the joined file begins, so its prefix matches
/// the joined description. Content binding refuses the part because its name
/// is a numeric split fragment of that description, and any declared yEnc size
/// that disagrees with the joined length corroborates the refusal. Even without
/// that refusal, the consumed-split exclusion after the join verdict shields
/// the part; this test keeps both layers honest.
#[tokio::test]
async fn a_part_sharing_the_joined_files_first_16_kib_is_not_reported_as_a_hole() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30822);
    let job_name = "Ivory Meadow Shared Prefix";
    let slice_size = PAR2_HASH_16K_BYTES;
    let joined = split_join_payload(slice_size * 4);
    let posting = SplitJoinPosting {
        job_name,
        joined: joined.clone(),
        slice_size: slice_size as u64,
        parts: vec![
            // Two slices' worth, posted as two articles, of which only the
            // first arrived: the captured prefix still covers the description's
            // whole 16 KiB window.
            SplitJoinPart {
                filename: "Ivory.Meadow.mkv.001".to_string(),
                segments: vec![slice_size as u32, slice_size as u32],
                arrived_segments: 1,
                on_disk: joined[..slice_size].to_vec(),
            },
            whole_split_join_part(
                "Ivory.Meadow.mkv.002",
                &joined[slice_size * 2..slice_size * 3],
            ),
            whole_split_join_part(
                "Ivory.Meadow.mkv.003",
                &joined[slice_size * 3..slice_size * 4],
            ),
        ],
        prefix_captured: vec![0],
        describes_parts: false,
    };
    posting.install(&mut pipeline, job_id).await;

    let first_part = NzbFileId {
        job_id,
        file_index: 0,
    };
    assert!(
        pipeline.resolve_par2_file_binding(first_part).is_none(),
        "the first part shares the joined file's first 16 KiB but is refused as its split \
         fragment — it must not bind to the joined description"
    );

    pipeline.check_job_completion(job_id).await;

    assert_eq!(
        pipeline.incomplete_par2_protected_data_file_count(job_id),
        0,
        "a part a verified join consumed is not an outstanding protected file; {}",
        debug_job_state(&pipeline, job_id)
    );

    settle_split_join_completion(&mut pipeline, job_id).await;

    let error = split_join_failure_error(&pipeline, job_id);
    assert!(
        !error.contains("BUG:") && !error.contains("nowhere on disk"),
        "a consumed part must not be reported as a missing protected file; error = {error}"
    );
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "error = {error}"
    );

    let delivered = split_join_delivered_dir(&pipeline, job_name);
    assert_eq!(
        tokio::fs::read(delivered.join(SPLIT_JOIN_JOINED_FILENAME))
            .await
            .unwrap(),
        joined,
        "the delivered join must be the repaired bytes"
    );
}

/// A join PAR2 already installed is never rebuilt over.
///
/// The guard belongs at the joiner even though a retired topology means the
/// gate does not normally dispatch it: an extraction spawned before the verdict
/// landed, or a restart that rebuilt the topology first, both reach the joiner
/// with the verified output already on disk.
#[tokio::test]
async fn a_joined_output_par2_installed_is_not_rebuilt_by_the_split_join() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30823);
    let joined = split_join_payload(192);
    let posting = SplitJoinPosting {
        job_name: "Ivory Meadow Installed Join",
        joined: joined.clone(),
        slice_size: 64,
        parts: vec![
            whole_split_join_part("Ivory.Meadow.mkv.001", &joined[0..64]),
            SplitJoinPart {
                filename: "Ivory.Meadow.mkv.002".to_string(),
                segments: vec![64],
                arrived_segments: 1,
                on_disk: vec![0u8; 64],
            },
            whole_split_join_part("Ivory.Meadow.mkv.003", &joined[128..192]),
        ],
        prefix_captured: Vec::new(),
        describes_parts: false,
    };
    let working_dir = posting.install(&mut pipeline, job_id).await;

    // The state a successful repair leaves behind: the verified join on disk
    // beside the parts it was rebuilt from, and a settled verdict.
    let joined_path = working_dir.join(SPLIT_JOIN_JOINED_FILENAME);
    tokio::fs::write(&joined_path, &joined).await.unwrap();
    pipeline.par2_verified.insert(job_id);

    pipeline
        .extract_simple_archive(
            job_id,
            SPLIT_JOIN_JOINED_FILENAME,
            crate::pipeline::completion::finalize::SimpleArchiveKind::Split,
        )
        .await
        .unwrap();
    let done = next_extraction_done(&mut pipeline).await;
    pipeline.handle_extraction_done(done).await;

    let staging = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.staging_dir.clone())
        .expect("the joiner reserves a staging directory either way");
    assert!(
        !staging.join(SPLIT_JOIN_JOINED_FILENAME).exists(),
        "the joiner must not write a second copy over a verified output; staging = {:?}",
        delivered_entry_names(&staging)
    );
    assert_eq!(
        tokio::fs::read(&joined_path).await.unwrap(),
        joined,
        "the verified join must survive untouched"
    );
    assert!(
        pipeline
            .extracted_members
            .get(&job_id)
            .is_some_and(|members| members.contains(SPLIT_JOIN_JOINED_FILENAME)),
        "the member is still reported extracted, so the gate moves on"
    );
}

/// The ordinary split posting, where the recovery set protects the parts.
///
/// Nothing here may change: no topology is retired, the parts are joined
/// exactly as they always were, and the join is the job's output.
#[tokio::test]
async fn a_recovery_set_naming_the_parts_still_joins_them() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(30824);
    let job_name = "Ivory Meadow Protected Parts";
    let joined = split_join_payload(192);
    let posting = SplitJoinPosting {
        job_name,
        joined: joined.clone(),
        slice_size: 64,
        parts: vec![
            whole_split_join_part("Ivory.Meadow.mkv.001", &joined[0..64]),
            whole_split_join_part("Ivory.Meadow.mkv.002", &joined[64..128]),
            whole_split_join_part("Ivory.Meadow.mkv.003", &joined[128..192]),
        ],
        prefix_captured: Vec::new(),
        describes_parts: true,
    };
    posting.install(&mut pipeline, job_id).await;

    pipeline.check_job_completion(job_id).await;

    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .archive_topology_for(SPLIT_JOIN_JOINED_FILENAME)
            .is_some(),
        "a verdict about the parts says nothing about the join, so the topology stands"
    );

    settle_split_join_completion(&mut pipeline, job_id).await;

    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete),
        "error = {}",
        split_join_failure_error(&pipeline, job_id)
    );
    let delivered = split_join_delivered_dir(&pipeline, job_name);
    assert_eq!(
        tokio::fs::read(delivered.join(SPLIT_JOIN_JOINED_FILENAME))
            .await
            .unwrap(),
        joined,
        "the parts still join into the release's file"
    );
}
