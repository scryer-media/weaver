use super::*;

#[test]
fn verified_placement_preserves_portable_download_names() {
    for (described, installed) in [
        ("disc1/payload.bin", "disc1_payload.bin"),
        ("disc1\\payload.bin", "disc1_payload.bin"),
        ("release:payload.bin", "release_payload.bin"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("posted.bin");
        std::fs::write(&source, b"verified payload").unwrap();
        let verification = par2_rs::VerificationResult {
            files: vec![file_verification(
                1,
                described,
                par2_rs::verify::FileStatus::Renamed(source.clone()),
                vec![true],
            )],
            recovery_blocks_available: 0,
            total_missing_blocks: 0,
            repairable: par2_rs::verify::Repairability::NotNeeded,
        };
        let plan = placement_plan_from_verification(&verification);
        assert_eq!(plan.renames[0].correct_name, installed);
        super::super::placement::apply_complete_plan(dir.path(), &plan).unwrap();
        assert_eq!(
            std::fs::read(dir.path().join(installed)).unwrap(),
            b"verified payload"
        );
        assert!(!source.exists());
    }
}

#[test]
fn clean_par2_verification_mode_labels_are_stable() {
    assert_eq!(CleanPar2VerificationMode::Grid.as_str(), "grid");
    assert_eq!(CleanPar2VerificationMode::FileCrc.as_str(), "file_crc");
    assert_eq!(
        CleanPar2VerificationMode::QuickDigest.as_str(),
        "quick_digest"
    );
    assert_eq!(
        CleanPar2VerificationMode::StrongDecode.as_str(),
        "strong_decode"
    );
    assert_eq!(
        CleanPar2VerificationMode::Authoritative.as_str(),
        "authoritative"
    );
}

fn file_id_from(seed: u8) -> par2_rs::FileId {
    par2_rs::FileId::from_bytes([seed; 16])
}

fn file_verification(
    seed: u8,
    filename: &str,
    status: par2_rs::verify::FileStatus,
    valid_slices: Vec<bool>,
) -> par2_rs::verify::FileVerification {
    let missing_slice_count = valid_slices.iter().filter(|valid| !**valid).count() as u32;
    par2_rs::verify::FileVerification {
        file_id: file_id_from(seed),
        filename: filename.to_string(),
        status,
        valid_slices,
        missing_slice_count,
    }
}

/// A four-file pre-repair verdict with one of each status.
fn mixed_pre_repair_verification() -> par2_rs::VerificationResult {
    let files = vec![
        file_verification(
            1,
            "intact.bin",
            par2_rs::verify::FileStatus::Complete,
            vec![true, true],
        ),
        file_verification(
            2,
            "damaged.bin",
            par2_rs::verify::FileStatus::Damaged(1),
            vec![true, false],
        ),
        file_verification(
            3,
            "missing.bin",
            par2_rs::verify::FileStatus::Missing,
            vec![false, false],
        ),
        file_verification(
            4,
            "moved.bin",
            par2_rs::verify::FileStatus::Renamed(std::path::PathBuf::from("/work/elsewhere.bin")),
            vec![true, true],
        ),
    ];
    let total_missing_blocks = files
        .iter()
        .map(|file| file.missing_slice_count)
        .sum::<u32>();
    par2_rs::VerificationResult {
        files,
        recovery_blocks_available: 8,
        total_missing_blocks,
        repairable: par2_rs::verify::Repairability::Repairable {
            blocks_needed: total_missing_blocks,
            blocks_available: 8,
        },
    }
}

/// Six intact parts at the wrong names is not a failed repair.
///
/// This is the shape the placement-normalization fixture produces: every
/// article arrived, nothing is damaged, and the parts simply need to be
/// moved to the names the recovery set describes. The post-repair guard
/// used to reject it — `needs_repair()` is true for a `Renamed` file — and
/// report it as "0 damaged slices", the zero being the tell that there was
/// nothing to repair at all. The rename entries the plan carries are
/// derived from those very statuses, so the guard was refusing the repair
/// for the one thing the next step fixes.
fn multi_rename_post_repair_verification() -> par2_rs::VerificationResult {
    let files = (1..=6u8)
        .map(|part| {
            file_verification(
                part,
                &format!("fixture_rar5_lz_plain.part{part}.rar"),
                par2_rs::verify::FileStatus::Renamed(std::path::PathBuf::from(format!(
                    "misplaced-{part}.rar"
                ))),
                vec![true, true],
            )
        })
        .collect::<Vec<_>>();
    par2_rs::verify::VerificationResult {
        files,
        recovery_blocks_available: 0,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    }
}

#[test]
fn a_placement_only_post_repair_result_is_not_damage() {
    let verification = multi_rename_post_repair_verification();

    assert!(
        par2_verification_needs_repair(&verification),
        "precondition: misplacement alone still makes the crate's own \
         predicate report that something needs doing"
    );
    assert_eq!(
        verification.total_missing_blocks, 0,
        "precondition: and yet nothing is damaged, which is why the old \
         message could only ever say zero"
    );

    assert_eq!(
        par2_post_repair_damage_failure(&verification),
        None,
        "so the repair tail must not fail here: there is no damaged or \
         missing file to fail over"
    );

    let plan = placement_plan_from_verification(&verification);
    assert_eq!(
        plan.renames.len(),
        6,
        "and every one of them is a rename the plan already knows how to \
         apply; plan = {plan:?}"
    );
    assert!(
        plan.unresolved.is_empty(),
        "none of it is unresolvable; plan = {plan:?}"
    );
    assert!(
        plan.swaps.is_empty(),
        "the derived plan never emits swaps, so a swap-shaped fixture would \
         not reproduce this at all; plan = {plan:?}"
    );
}

#[test]
fn post_repair_damage_still_fails_and_counts_what_remains() {
    let mut verification = multi_rename_post_repair_verification();
    verification.files.push(file_verification(
        9,
        "fixture_rar5_lz_plain.part7.rar",
        par2_rs::verify::FileStatus::Damaged(2),
        vec![false, false],
    ));
    verification.total_missing_blocks = 2;

    let failure = par2_post_repair_damage_failure(&verification)
        .expect("a damaged file after repair is still a failed repair");
    assert!(
        failure.contains("2 damaged slice(s) across 1 file(s)"),
        "and the message names what actually remains rather than a bare \
         zero; failure = {failure}"
    );
    assert!(
        failure.contains("6 file(s) still to be placed"),
        "including the misplacement it is not failing over; failure = {failure}"
    );
}

/// The write set is every file the repair could have acted on, which is
/// every file that was not already complete at its canonical name.
///
/// `Renamed` belongs in it. The rule used to read "Damaged and Missing",
/// on the reasoning that misplaced content already exists intact somewhere
/// else and is moved by placement rather than rewritten. A repair over a set
/// whose only fault was misplacement disproved it: the repairer copied every
/// one of those files onto its canonical name — the run reconstructed no
/// slice and still reported bytes copied — and left the displaced originals
/// as `<name>.N`. Carrying the pre-repair `Renamed` entries through that
/// reported six placed files as still misplaced, and the placement step then
/// tried to rename them onto names the repair had just filled.
#[test]
fn par2_repair_write_set_is_everything_not_already_complete() {
    let write_set = par2_repair_write_set(&mixed_pre_repair_verification());

    assert_eq!(
        write_set,
        vec![file_id_from(2), file_id_from(3), file_id_from(4)],
        "Damaged, Missing and Renamed are all files the repair installs at a \
         canonical name, so all three have to be re-read there afterwards. \
         Only Complete is carried: it is the one verdict a repair cannot \
         have invalidated."
    );
}

#[test]
fn selective_pass_opts_into_slice_proof_verification() {
    let options = selective_pass_verify_options();

    assert!(
        options.fast_verify,
        "the selective post-repair arm proves a rewritten file from its \
         per-slice IFSC checksums (multi-buffer engine, ~12ms/128MiB) \
         instead of the inherently serial whole-file MD5 (~149ms/128MiB). \
         The identity the strict digest exists to establish is already \
         fixed here: the file was read at the canonical name the repairer \
         just installed it to, IFSC-verified before install."
    );
    assert!(
        options.cancel.is_none() && options.progress.is_none(),
        "fast-verify is the only option the selective arm sets"
    );
}

#[test]
fn placement_plan_from_verification_places_by_verdict() {
    let plan = placement_plan_from_verification(&mixed_pre_repair_verification());

    assert_eq!(plan.exact, vec![file_id_from(1)]);
    assert_eq!(plan.unresolved, vec![file_id_from(2), file_id_from(3)]);
    assert_eq!(plan.renames.len(), 1);
    assert_eq!(plan.renames[0].file_id, file_id_from(4));
    assert_eq!(plan.renames[0].current_name, "elsewhere.bin");
    assert_eq!(plan.renames[0].correct_name, "moved.bin");
    assert!(
        plan.swaps.is_empty() && plan.conflicts.is_empty(),
        "a verdict-derived plan can express neither: the result holds one \
         entry per file ID, so nothing can be contested, and a pairwise \
         displacement was already normalized before the repair ran"
    );
}

/// A recovery set carrying nothing but the two numbers the merge reads:
/// the recovery-block count and the file order.
fn merge_test_par2_set(recovery_blocks: u32) -> par2_rs::Par2FileSet {
    let recovery_slices = (0..recovery_blocks)
        .map(|exponent| {
            (
                exponent,
                par2_rs::RecoverySlice {
                    exponent,
                    data: bytes::Bytes::from_static(&[0u8; 4]).into(),
                },
            )
        })
        .collect();
    par2_rs::Par2FileSet {
        recovery_set_id: par2_rs::RecoverySetId::from_bytes([9u8; 16]),
        slice_size: 4,
        recovery_file_ids: (1..=4).map(file_id_from).collect(),
        non_recovery_file_ids: Vec::new(),
        files: HashMap::new(),
        slice_checksums: HashMap::new(),
        recovery_slices,
        creator: None,
    }
}

/// The shape the merge actually sees. `verify_all` and
/// `verify_selected_file_ids` only ever report `Complete`, `Damaged` or
/// `Missing` — `Renamed` comes from the repairer's own scanner, never from
/// the pass that produces the pre-repair result this merges onto.
fn production_pre_repair_verification() -> par2_rs::VerificationResult {
    let mut verification = mixed_pre_repair_verification();
    verification
        .files
        .retain(|file| !matches!(file.status, par2_rs::verify::FileStatus::Renamed(_)));
    verification
}

#[test]
fn post_repair_merge_carries_untouched_entries_verbatim() {
    let par2_set = merge_test_par2_set(8);
    let base = production_pre_repair_verification();
    // What a selective pass over the write set comes back with once the
    // repair has installed both files. Deliberately out of recovery-set
    // order, so the reorder is doing work.
    let fresh = par2_rs::VerificationResult {
        files: vec![
            file_verification(
                3,
                "missing.bin",
                par2_rs::verify::FileStatus::Complete,
                vec![true, true],
            ),
            file_verification(
                2,
                "damaged.bin",
                par2_rs::verify::FileStatus::Complete,
                vec![true, true],
            ),
        ],
        recovery_blocks_available: 8,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };

    let merged = par2_rs::verify::merge_verification_results(&par2_set, &base, fresh);

    // Recovery-set order, whatever order the selective read came back in.
    let order: Vec<par2_rs::FileId> = merged.files.iter().map(|file| file.file_id).collect();
    assert_eq!(
        order,
        vec![file_id_from(1), file_id_from(2), file_id_from(3)]
    );

    // The carried entry is the pre-repair pass's, untouched.
    let before = &base.files[0];
    let after = merged
        .files
        .iter()
        .find(|file| file.file_id == file_id_from(1))
        .expect("carried file must survive the merge");
    assert_eq!(after.filename, before.filename);
    assert_eq!(after.valid_slices, before.valid_slices);
    assert_eq!(after.missing_slice_count, before.missing_slice_count);
    assert_eq!(
        format!("{:?}", after.status),
        format!("{:?}", before.status),
        "downstream has to see exactly what a full pass over unchanged bytes \
         would have reported"
    );

    // The rewritten files carry the fresh verdict, and the set-level
    // numbers were recomputed rather than inherited.
    assert!(!par2_verification_needs_repair(&merged));
    assert_eq!(merged.total_missing_blocks, 0);
    assert!(matches!(
        merged.repairable,
        par2_rs::verify::Repairability::NotNeeded
    ));
}

#[test]
fn a_carried_renamed_entry_would_keep_the_post_repair_gate_red() {
    // Not a production shape — the pre-repair pass cannot report `Renamed`
    // — but the carry rule is stated over all four statuses, so what it
    // would mean is pinned rather than assumed. `needs_repair` is any
    // status that is not `Complete`, so carrying a `Renamed` entry into the
    // post-repair gate would fail the job on "file placements remain". The
    // classification is still right: `Renamed` content is not rewritten by
    // a repair, so re-reading it would not change the verdict either.
    let par2_set = merge_test_par2_set(8);
    let base = mixed_pre_repair_verification();
    let fresh = par2_rs::VerificationResult {
        files: vec![
            file_verification(
                2,
                "damaged.bin",
                par2_rs::verify::FileStatus::Complete,
                vec![true, true],
            ),
            file_verification(
                3,
                "missing.bin",
                par2_rs::verify::FileStatus::Complete,
                vec![true, true],
            ),
        ],
        recovery_blocks_available: 8,
        total_missing_blocks: 0,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };

    let merged = par2_rs::verify::merge_verification_results(&par2_set, &base, fresh);

    let carried = merged
        .files
        .iter()
        .find(|file| file.file_id == file_id_from(4))
        .expect("carried file must survive the merge");
    assert!(matches!(
        carried.status,
        par2_rs::verify::FileStatus::Renamed(_)
    ));
    assert_eq!(merged.total_missing_blocks, 0);
    assert!(par2_verification_needs_repair(&merged));
}

#[test]
fn post_repair_merge_still_fails_a_file_the_repair_left_damaged() {
    let par2_set = merge_test_par2_set(8);
    let base = production_pre_repair_verification();
    let fresh = par2_rs::VerificationResult {
        files: vec![
            file_verification(
                2,
                "damaged.bin",
                par2_rs::verify::FileStatus::Damaged(1),
                vec![true, false],
            ),
            file_verification(
                3,
                "missing.bin",
                par2_rs::verify::FileStatus::Complete,
                vec![true, true],
            ),
        ],
        recovery_blocks_available: 8,
        total_missing_blocks: 1,
        repairable: par2_rs::verify::Repairability::Repairable {
            blocks_needed: 1,
            blocks_available: 8,
        },
    };

    let merged = par2_rs::verify::merge_verification_results(&par2_set, &base, fresh);

    assert!(
        par2_verification_needs_repair(&merged),
        "the gate that fails the job after a repair reads the merged result, \
         so a rewritten file that is still damaged has to survive the merge \
         as damage"
    );
    assert_eq!(merged.total_missing_blocks, 1);
}

#[test]
fn placement_plan_from_a_clean_post_repair_result_is_a_no_op() {
    let mut verification = mixed_pre_repair_verification();
    for file in &mut verification.files {
        file.status = par2_rs::verify::FileStatus::Complete;
        file.valid_slices.fill(true);
        file.missing_slice_count = 0;
    }
    verification.total_missing_blocks = 0;

    let plan = placement_plan_from_verification(&verification);

    assert_eq!(plan.exact.len(), 4);
    assert!(
        plan.swaps.is_empty() && plan.renames.is_empty(),
        "which is what makes `apply_placement_plan_for_retry_or_repair` a \
         no-op for the post-repair pass: every file is already at the name \
         its description gives it"
    );
}

#[test]
fn par2_repair_memory_limit_defaults_when_unset() {
    assert_eq!(
        parse_par2_repair_memory_limit_bytes(None),
        DEFAULT_PAR2_REPAIR_MEMORY_LIMIT_BYTES
    );
    assert_eq!(
        parse_par2_repair_memory_limit_bytes(Some("  ")),
        DEFAULT_PAR2_REPAIR_MEMORY_LIMIT_BYTES
    );
}

#[test]
fn par2_repair_memory_limit_accepts_positive_bytes() {
    assert_eq!(
        parse_par2_repair_memory_limit_bytes(Some("134217728")),
        134_217_728
    );
}

#[test]
fn par2_repair_memory_limit_rejects_invalid_or_zero_values() {
    let default_bytes = default_par2_repair_memory_limit_bytes();
    assert_eq!(
        parse_par2_repair_memory_limit_bytes(Some("not-bytes")),
        default_bytes
    );
    assert_eq!(
        parse_par2_repair_memory_limit_bytes(Some("0")),
        default_bytes
    );
}

#[test]
fn par2_ignore_extensions_default_to_the_baked_metadata_set() {
    assert_eq!(
        parse_par2_ignore_extensions(None),
        DEFAULT_PAR2_IGNORE_EXTENSIONS
            .iter()
            .map(|extension| (*extension).to_string())
            .collect::<Vec<_>>()
    );
    assert!(parse_par2_ignore_extensions(None).contains(&"nfo".to_string()));
    assert!(parse_par2_ignore_extensions(None).contains(&"sfv".to_string()));
}

#[test]
fn an_empty_par2_ignore_extension_override_disables_the_rule() {
    assert!(parse_par2_ignore_extensions(Some("")).is_empty());
    assert!(parse_par2_ignore_extensions(Some("   ")).is_empty());
    assert!(!par2_damage_ignorable(
        "silver.horizon.nfo",
        &parse_par2_ignore_extensions(Some(""))
    ));
}

#[test]
fn par2_ignore_extensions_accept_both_separators_and_normalize_entries() {
    assert_eq!(
        parse_par2_ignore_extensions(Some(" .NFO, sfv ;nfo;.Srr ")),
        vec!["nfo".to_string(), "sfv".to_string(), "srr".to_string()]
    );
}

#[test]
fn par2_damage_is_ignorable_by_extension_only() {
    let extensions = parse_par2_ignore_extensions(None);
    assert!(par2_damage_ignorable("silver.horizon.nfo", &extensions));
    assert!(par2_damage_ignorable("SILVER.HORIZON.SFV", &extensions));
    // The extension is the file's own, not a substring of its name: a
    // payload that merely mentions one of these words is still payload.
    assert!(!par2_damage_ignorable(
        "silver.horizon.nfo.mkv",
        &extensions
    ));
    assert!(!par2_damage_ignorable("silver.horizon.mkv", &extensions));
    assert!(!par2_damage_ignorable("nfo", &extensions));
}

#[test]
fn quick_proof_uses_crc_verified_contiguous_assembly() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("payload.bin");
    let bytes = b"crc-verified-contiguous-payload";
    std::fs::write(&path, bytes).unwrap();
    let candidate = Par2SessionEvidenceCandidate {
        file_id: NzbFileId {
            job_id: JobId(1),
            file_index: 0,
        },
        path,
        logical_name: "payload.bin".to_string(),
        expected_length: bytes.len() as u64,
        full_md5: None,
        crc32: par2_rs::checksum::crc32(bytes),
        contiguous_assembly_proven: true,
        bound_file_id: None,
    };

    let evidence = committed_evidence_from_candidate(&candidate)
        .unwrap()
        .expect("contiguous CRC-verified assembly should be quick-proved");
    assert!(evidence.assembly_proof().is_some());
    assert_eq!(evidence.assembly_crc32(), Some(candidate.crc32));
}

#[test]
fn quick_proof_refuses_unproven_assembly() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("payload.bin");
    std::fs::write(&path, b"payload").unwrap();
    let candidate = Par2SessionEvidenceCandidate {
        file_id: NzbFileId {
            job_id: JobId(1),
            file_index: 0,
        },
        path,
        logical_name: "payload.bin".to_string(),
        expected_length: 7,
        full_md5: None,
        crc32: 0,
        contiguous_assembly_proven: false,
        bound_file_id: None,
    };

    assert!(
        committed_evidence_from_candidate(&candidate)
            .unwrap()
            .is_none()
    );
}

#[test]
fn source_changed_retry_is_limited_to_one_fresh_analysis() {
    let changed: Result<(), par2_rs::Par2SessionError> =
        Err(par2_rs::Par2SessionError::SourceChanged {
            path: std::path::PathBuf::from("payload.bin"),
        });
    assert!(should_retry_par2_source_change(&changed, false));
    assert!(!should_retry_par2_source_change(&changed, true));
}

#[test]
fn incomplete_promoted_recovery_without_concrete_work_is_not_pending() {
    let state = PromotedRecoveryPipelineState {
        promoted_par2_files: 1,
        incomplete_promoted_par2_files: 1,
        ..Default::default()
    };

    assert!(!state.has_pending_work());
}

#[test]
fn concrete_promoted_recovery_work_is_pending() {
    for state in [
        PromotedRecoveryPipelineState {
            download_queue_promoted_recovery: 1,
            ..Default::default()
        },
        PromotedRecoveryPipelineState {
            active_promoted_downloads: 1,
            ..Default::default()
        },
        PromotedRecoveryPipelineState {
            pending_promoted_retries: 1,
            ..Default::default()
        },
        PromotedRecoveryPipelineState {
            pending_promoted_decode: 1,
            ..Default::default()
        },
        PromotedRecoveryPipelineState {
            active_promoted_decodes: 1,
            ..Default::default()
        },
        PromotedRecoveryPipelineState {
            write_buffered_promoted_recovery: 1,
            ..Default::default()
        },
    ] {
        assert!(state.has_pending_work(), "{state:?}");
    }
}

#[test]
fn parked_promoted_recovery_is_not_pending_until_reapplied() {
    let state = PromotedRecoveryPipelineState {
        parked_promoted_recovery: 1,
        ..Default::default()
    };

    assert!(!state.has_pending_work());
}
