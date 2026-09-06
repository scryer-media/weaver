//! RAR4/RAR3 file encryption
//! Header-encrypted (`-hp`) sets
//! The dual-CRC grid, fed from the direct seam

use super::*;

// ---------------------------------------------------------------------------
// RAR4/RAR3 file encryption
//
// The same three cipher-hold, keyed-fold and crypt-restore mechanisms over the
// other format. Nothing in the router, the holds, the gates, the overlay or the
// snapshot is RAR5-specific; the only differences the format brings are the
// cipher width (AES-128), where the key and IV come from (both out of the KDF,
// over an 8-byte per-file salt the header states), and the two things RAR4 does
// not have — a password-check value, and a tweaked-checksum flag. Those two
// absences are the subject of the tests below as much as the presence of the
// cipher is.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_encrypted_rar4_store_set_routes_plaintext_and_matches_the_conventional_extractor() {
    // The differential spine, over RAR4. 3000 is not a multiple of 16, so the
    // member carries 8 bytes of tail padding — the AES-128 final block decrypts
    // to plaintext that runs past the member's declared end, and none of it may
    // reach the destination.
    let member_name = "Silver.Horizon.S03E01.mkv";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 251) as u8).collect();
    let volumes = encrypted_rar4_store_set(
        member_name,
        &payload,
        4,
        "moonlit-harbour",
        Some(TEST_RAR4_SALT),
    );
    assert_eq!(
        &volumes[0].1[..7],
        &TEST_RAR4_SIG,
        "the fixture really is RAR4"
    );
    let arrivals = in_order_arrivals(volumes.len());

    let conventional = run_gate_with_password(
        DirectStoreGate::Disabled,
        None,
        None,
        JobId(44001),
        member_name,
        &volumes,
        &arrivals,
        Some("moonlit-harbour"),
    )
    .await;
    let direct = run_gate_with_password(
        DirectStoreGate::Enabled,
        None,
        None,
        JobId(44002),
        member_name,
        &volumes,
        &arrivals,
        Some("moonlit-harbour"),
    )
    .await;

    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written the encrypted source volumes"
    );
    assert!(
        !direct.volume_file_seen,
        "direct routing must never create a source volume file, RAR4 or RAR5"
    );
    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional extractor should decrypt the RAR4 member with the job's password"
    );
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "the routed member must be plaintext at its final offsets, with no tail padding"
    );
    assert_eq!(
        (direct.member, direct.member_location, direct.status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "an encrypted RAR4 set routed plaintext-once must be byte-identical to the \
         conventional extractor with the same password"
    );
}

#[tokio::test]
async fn a_saltless_rar4_header_keys_off_the_password_alone_and_still_routes() {
    // RAR3 archives written without a salt derive the key from the password
    // alone. That is a complete description of the member's keying, not a
    // missing record, and the router must treat it as one rather than refusing.
    let member_name = "Silver.Horizon.S03E02.mkv";
    let payload: Vec<u8> = (0..1800u32).map(|index| (index % 193) as u8).collect();
    let volumes = encrypted_rar4_store_set(member_name, &payload, 3, "moonlit-harbour", None);
    let arrivals = in_order_arrivals(volumes.len());

    let direct = run_gate_with_password(
        DirectStoreGate::Enabled,
        None,
        None,
        JobId(44011),
        member_name,
        &volumes,
        &arrivals,
        Some("moonlit-harbour"),
    )
    .await;
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "a saltless RAR4 member must route and verify like a salted one"
    );
    assert!(!direct.volume_file_seen);
}

#[tokio::test]
async fn a_wrong_password_on_a_rar4_set_is_caught_by_the_member_gate_and_nothing_earlier() {
    // The keyed-fold claim for RAR4, and the one place the format differs in a
    // way that matters: there is **no** password-check value, so
    // `WrongPassword` cannot fire at admission however wrong the password is.
    // Every RAR4 member routes provisionally and the whole-member CRC32 is the
    // only detector — a *plain* CRC32, not a keyed fold, because RAR4 has no
    // hash-MAC flag.
    let member_name = "Silver.Horizon.S03E03.mkv";
    let payload: Vec<u8> = (0..2100u32).map(|index| (index % 211) as u8).collect();
    let volumes = encrypted_rar4_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some(TEST_RAR4_SALT),
    );
    let arrivals = in_order_arrivals(volumes.len());

    let caught =
        encrypted_routing_outcome(JobId(44021), &volumes, &arrivals, Some("wrong-key")).await;
    assert!(
        caught.shape.contains("Demoted(MemberChecksumMismatch)"),
        "a wrong password on a RAR4 set must be caught by the member gate, got {}",
        caught.shape
    );
    assert!(
        !caught.shape.contains("EncryptedMemberRefused"),
        "and it must NOT be refused at admission — asserting the absence so the test \
         cannot pass by refusing early instead, which would prove nothing about the \
         gate: RAR4 has no check value to refute with. Got {}",
        caught.shape
    );

    // Non-vacuity: the identical fixture with the right password verifies
    // through the same gate and completes.
    let clean = run_gate_with_password(
        DirectStoreGate::Enabled,
        None,
        None,
        JobId(44022),
        member_name,
        &volumes,
        &arrivals,
        Some("moonlit-harbour"),
    )
    .await;
    assert_eq!(
        clean.member.as_deref(),
        Some(payload.as_slice()),
        "the same RAR4 set with the right password must verify and complete"
    );
}

#[tokio::test]
async fn an_encrypted_rar4_set_with_no_password_demotes_instead_of_routing_ciphertext() {
    let member_name = "Silver.Horizon.S03E04.mkv";
    let payload: Vec<u8> = (0..1500u32).map(|index| (index % 181) as u8).collect();
    let volumes = encrypted_rar4_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        Some(TEST_RAR4_SALT),
    );
    let arrivals = in_order_arrivals(volumes.len());

    let outcome = encrypted_routing_outcome(JobId(44031), &volumes, &arrivals, None).await;
    assert!(
        outcome
            .shape
            .contains("Demoted(EncryptedMemberRefused(NoPassword))"),
        "an encrypted RAR4 set with no password must demote, by name, got {}",
        outcome.shape
    );
    assert!(
        !outcome.partial_seen,
        "a set with no key must not create a destination for ciphertext"
    );
}

#[tokio::test]
async fn a_par2_bearing_encrypted_rar4_job_routes_direct_and_completes_byte_identically() {
    // The re-encrypting overlay, over AES-128: the destinations hold plaintext
    // while PAR2 describes the posted cipher, so every block the authoritative
    // pass reads has to be re-derived through `MemberCipher::encrypt` — which
    // is now a dispatch rather than a fixed width. If that dispatch were wrong
    // the pass would report damage in a byte-perfect volume and the set would
    // demote.
    let member_name = "Silver.Horizon.S03E05.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 199) as u8).collect();
    let volumes = encrypted_rar4_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some(TEST_RAR4_SALT),
    );

    let conventional = run_par2_direct_gate_with_password(
        DirectStoreGate::Disabled,
        JobId(44041),
        member_name,
        &volumes,
        Some("moonlit-harbour"),
    )
    .await;
    let direct = run_par2_direct_gate_with_password(
        DirectStoreGate::Enabled,
        JobId(44042),
        member_name,
        &volumes,
        Some("moonlit-harbour"),
    )
    .await;

    assert!(
        direct.admitted,
        "a par2-bearing encrypted RAR4 job must route, got {}",
        direct.demotions
    );
    assert!(
        !direct.demotions.contains("Demoted"),
        "and it must stay direct through verification, got {}",
        direct.demotions
    );
    assert!(
        !direct.volume_file_seen,
        "no source volume may be written at any point"
    );
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "the routed member must match the payload byte for byte"
    );
    assert_eq!(
        (direct.member, direct.member_location, direct.status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "a par2-bearing encrypted RAR4 set must be byte-identical to the conventional path"
    );
}

#[tokio::test]
async fn an_encrypted_rar4_set_restarted_mid_download_completes_byte_identically() {
    // Crypt-state restore over RAR4. What a restart has to rebuild here is
    // *not* what it rebuilds for RAR5: the snapshot row carries the 8-byte file
    // salt and no IV, because RAR4's IV is a KDF output and persisting one
    // would put a password verifier in the database that the archive itself
    // does not have. The resumed run re-derives both key and IV from the live
    // password, and the checkpointed cipher blocks are what let it decrypt at
    // the coverage frontier rather than from the member's start.
    const ARTICLES: usize = 4;
    let member_name = "Silver.Horizon.S03E06.mkv";
    let payload: Vec<u8> = (0..8000u32).map(|index| (index % 251) as u8).collect();
    let volumes = encrypted_rar4_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        Some(TEST_RAR4_SALT),
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(44051);
    // Volume 0 complete, volume 1 half: one file the restore can skip whole and
    // one it must skip only part of, with a part boundary that is not 16-aligned.
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1), (0, 2), (0, 3), (1, 0), (1, 1)];
    let working_dir = direct_store_before_restart_with_password(
        &temp_dir,
        job_id,
        &volumes,
        &arrivals,
        ARTICLES,
        Some("moonlit-harbour"),
    )
    .await;

    let mut pipeline = direct_store_after_restart_with_password(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
        Some("moonlit-harbour"),
    )
    .await;

    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the restored encrypted RAR4 job must carry its direct set");
    assert!(
        !set.is_demoted(),
        "a restart that still has the password must re-admit the set, not demote it — \
         which for RAR4 means the salt in the row agreed with the rebuilt headers and \
         the IV was re-derived, got {set:?}"
    );
    assert!(
        set.has_restart_seeded_coverage(),
        "restored coverage is seeded and unverified until it is re-read as plaintext"
    );

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        !queued.iter().any(|(file_index, _)| *file_index == 0),
        "volume 0 was complete at the barrier; none of its articles may be refetched, \
         got {queued:?}"
    );
    assert!(
        queued.len() < volumes.len() * ARTICLES,
        "a restart that refetches everything is not honouring any floor"
    );

    for (file_index, segment_number) in queued {
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            ARTICLES,
        )
        .await;
    }
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let complete_dir = temp_dir.path().join("complete");
    let (restarted, location) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        restarted.as_deref(),
        Some(payload.as_slice()),
        "a restarted encrypted RAR4 set must finish byte-identical to an uninterrupted one"
    );
    assert_eq!(location, Some("complete"));
    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "a restarted encrypted RAR4 set must still never materialize a source volume"
    );
}

#[tokio::test]
async fn an_encrypted_rar4_set_restarted_without_its_password_demotes_by_name() {
    // The RAR4 twin of
    // `an_encrypted_set_restarted_without_its_password_demotes_by_name`, and
    // the one case RAR4 cannot borrow the RAR5 argument for.
    //
    // RAR5 can refute a wrong password at admission against the header's check
    // value. RAR4 has no check value at all, so its admission can only answer
    // one question: is there a password? A restore that supplies none must
    // therefore refuse on *absence* — never derive a key from an empty or
    // default password, which would produce a well-formed AES-128 stream and
    // route decrypted garbage until the whole-member CRC32 noticed. The refusal
    // runs before the checkpoint can seed anything, so nothing is left
    // permanently unverifiable either.
    const ARTICLES: usize = 2;
    let member_name = "Silver.Horizon.S03E07.mkv";
    let payload: Vec<u8> = (0..4000u32).map(|index| (index % 239) as u8).collect();
    let volumes = encrypted_rar4_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        Some(TEST_RAR4_SALT),
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(44061);
    let arrivals: Vec<(u32, u32)> = vec![(0, 0), (0, 1)];
    let working_dir = direct_store_before_restart_with_password(
        &temp_dir,
        job_id,
        &volumes,
        &arrivals,
        ARTICLES,
        Some("moonlit-harbour"),
    )
    .await;

    let mut pipeline = direct_store_after_restart_with_password(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
        None,
    )
    .await;

    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_none_or(|set| !set.has_restart_seeded_coverage()),
        "a RAR4 set that cannot decrypt must not be left holding seeded coverage it can \
         never re-arm"
    );
    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert_eq!(
        queued.len(),
        volumes.len() * ARTICLES,
        "a refused checkpoint must skip nothing, got {queued:?}"
    );

    // And the live parse reaches the same decision under its own name, rather
    // than routing ciphertext or holding forever.
    submit_volume_article_of(&mut pipeline, job_id, &volumes, 0, 0, ARTICLES).await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(EncryptedMemberRefused(NoPassword))"),
        "a restored encrypted RAR4 set with no password must demote by name, got {shape}"
    );

    // The demotion is the conventional path taking over, not a dead end: the
    // stale `.direct.partial` from the pre-restart run is swept by suffix, so
    // no plaintext prefix written under the old key is left to be mistaken for
    // this run's output.
    assert!(
        !any_direct_partial(&payload_root(&temp_dir, JobId(44061))),
        "a refused restore must sweep the partials the previous run wrote"
    );
}

// ---------------------------------------------------------------------------
// Header-encrypted (`-hp`) sets
//
// `-hp` withholds the *layout*, not the *keying facts*: RAR5's type-4 record is
// plaintext and states the salt, the KDF count and — by default — a password
// check. So a set whose password rides in the NZB can be keyed at its very first
// article, before any byte routes, and route direct like any other encrypted set.
//
// The load-bearing difference from `-p` runs through every test below: `-p`
// admits on `Unverifiable`, because a wrong key there only corrupts *data* and
// the whole-member CRC32 catches it. Here a wrong key corrupts the **header
// parse**, so the layout itself would come out of garbage — and "garbage will
// not parse" is a 2^-32 argument, not a gate. `-hp` therefore admits on
// `Verified` and on nothing else, and every other outcome is a named refusal
// back to the older floor: materialize the volume, extract conventionally.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_header_encrypted_set_keyed_from_nzb_meta_matches_the_conventional_extractor() {
    // The header-encryption spine. The password is in the NZB's `<meta
    // type="password">` and nowhere else — no operator ever typed it — and the
    // set routes from its first article.
    let member_name = "Silver.Horizon.S04E01.mkv";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 251) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        4,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );
    // Non-vacuity on the fixture itself: without a password these volumes state
    // nothing at all, which is what makes this a `-hp` test rather than a `-p`
    // one with extra steps.
    assert!(
        unrar_rs::RarArchive::parse_volume_facts(std::io::Cursor::new(volumes[0].1.clone()), None)
            .is_err(),
        "a `-hp` fixture must yield no facts without a password"
    );
    let arrivals = in_order_arrivals(volumes.len());

    let conventional = run_hp_gate(
        DirectStoreGate::Disabled,
        JobId(45001),
        member_name,
        &volumes,
        &arrivals,
        None,
        sample_nzb_zstd_with_password("moonlit-harbour"),
        None,
    )
    .await;
    let direct = run_hp_gate(
        DirectStoreGate::Enabled,
        JobId(45002),
        member_name,
        &volumes,
        &arrivals,
        None,
        sample_nzb_zstd_with_password("moonlit-harbour"),
        None,
    )
    .await;

    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written source volumes"
    );
    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional extractor should reproduce the member payload"
    );
    assert!(
        !direct.volume_file_seen,
        "a routed `-hp` set must never create a source volume file"
    );
    assert_eq!(
        (direct.member, direct.member_location, direct.status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "a `-hp` set routed direct must be byte-identical to the conventional extractor, in \
         the same directory, with the same job status"
    );
}

#[tokio::test]
async fn a_tolerated_member_of_a_header_encrypted_set_extracts_with_the_proved_password() {
    // The tolerance extracts an ineligible member through the
    // hybrid provider — and an `-hp` set's *virtual* volumes are as
    // header-encrypted as the posted ones, so that extraction cannot so much as
    // open the archive without the key the router proved. It used to open with
    // no password at all, which failed at the first header and demoted the set
    // under `ToleratedExtractionFailed`: correct output by way of a full
    // materialize and a conventional re-extract, for a set that had already
    // routed every stored byte.
    //
    // The extra member is BLAKE2sp-only, which `classify_stored_chain` answers
    // with `Blake2OnlyNoCrc32` on the encrypted path exactly as on the plaintext
    // one, and `tolerated_members` takes any `Ineligible(_)`.
    let member_name = "Silver.Horizon.S04E09.mkv";
    let extra_name = "Silver.Horizon.S04E09.nfo";
    // The store member has to be at least 100x the extra for the extra to fit
    // under `min(64 MiB, 1% of packed archive bytes)`.
    let payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 97) as u8).collect();
    let volumes = header_encrypted_store_set_with_extra_member(
        member_name,
        &payload,
        extra_name,
        &extra_payload,
        4,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );
    // Non-vacuity on the fixture: these volumes state nothing without a
    // password, so the tolerated extraction genuinely needs one.
    assert!(
        unrar_rs::RarArchive::parse_volume_facts(std::io::Cursor::new(volumes[0].1.clone()), None)
            .is_err(),
        "a `-hp` fixture must yield no facts without a password"
    );
    let arrivals = in_order_arrivals(volumes.len());

    // Both gates report the *tolerated* member, which is the one under test.
    let conventional = run_hp_gate(
        DirectStoreGate::Disabled,
        JobId(45021),
        extra_name,
        &volumes,
        &arrivals,
        None,
        sample_nzb_zstd_with_password("moonlit-harbour"),
        None,
    )
    .await;
    let direct = run_hp_gate(
        DirectStoreGate::Enabled,
        JobId(45022),
        extra_name,
        &volumes,
        &arrivals,
        None,
        sample_nzb_zstd_with_password("moonlit-harbour"),
        None,
    )
    .await;

    assert_eq!(
        conventional.member.as_deref(),
        Some(extra_payload.as_slice()),
        "the conventional extractor should reproduce the tolerated member"
    );
    // The load-bearing pair. Without the password the tolerated extraction fails
    // and the set demotes, and a demotion materializes every source volume — so
    // this assertion is what tells a tolerated extraction that *worked* from one
    // that was rescued by the fallback.
    assert!(
        !direct.volume_file_seen,
        "the tolerated extraction must succeed in place, not by demoting the set"
    );
    assert_eq!(
        (direct.member, direct.member_location, direct.status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "a tolerated member of a routed `-hp` set must match the conventional extractor"
    );
}

#[tokio::test]
async fn a_header_encrypted_set_keys_from_the_filename_password_convention() {
    // The third harvest source, and the one that needs no NZB body at all: the
    // password is in the NZB *file name*'s `{{…}}` stem convention.
    let member_name = "Silver.Horizon.S04E02.mkv";
    let payload: Vec<u8> = (0..2600u32).map(|index| (index % 241) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        3,
        "harbour-lights",
        HeaderCheck::For("harbour-lights"),
    );
    let arrivals = in_order_arrivals(volumes.len());

    let direct = run_hp_gate(
        DirectStoreGate::Enabled,
        JobId(45011),
        member_name,
        &volumes,
        &arrivals,
        None,
        sample_nzb_zstd(),
        Some("Silver Horizon {{harbour-lights}}.nzb"),
    )
    .await;

    assert!(!direct.volume_file_seen);
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "the filename convention is a candidate source like any other"
    );
    assert_eq!(direct.member_location, Some("complete"));
    assert!(matches!(direct.status, Some(JobStatus::Complete)));

    // Non-vacuity: **the same job with the `{{…}}` removed from the NZB's file
    // name** — same archive, same payload, same empty spec password, same
    // password-free NZB body — has no candidate at all and refuses by name.
    // Without this, a run that ignored the filename and keyed from somewhere
    // else entirely would pass above.
    let refused = hp_routing_outcome_named(
        JobId(45012),
        &volumes,
        &arrivals,
        None,
        sample_nzb_zstd(),
        Some("Silver Horizon.nzb"),
    )
    .await;
    assert!(
        refused
            .shape
            .contains("Demoted(HeaderEncryptedRefused(NoPassword))"),
        "with the convention gone there is no candidate at all, got {}",
        refused.shape
    );
    assert!(
        refused.volume_file_seen,
        "and the set must have fallen back to materializing its volumes"
    );
}

#[tokio::test]
async fn a_header_encrypted_set_with_the_wrong_password_demotes_by_name() {
    // And then the operator corrects it, which is the half that matters. A
    // refusal here is only survivable because the set leaves direct mode
    // *intact*: the in-hand article continues conventionally, the volumes
    // materialize byte for byte, and that path — which re-harvests the job's
    // passwords per volume parse rather than memoizing them — opens the archive
    // with the corrected one and produces the member. `setJobPassword` and the
    // NZBGet facade's `*Unpack:Password` are the real mutations this stands in
    // for, and the direct set deliberately does not come back from either.
    let member_name = "Silver.Horizon.S04E03.mkv";
    let payload: Vec<u8> = (0..2000u32).map(|index| (index % 233) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );

    let outcome = hp_fallback_outcome(
        JobId(45021),
        member_name,
        &volumes,
        &in_order_arrivals(volumes.len()),
        Some("not-the-password"),
        sample_nzb_zstd(),
        Some("moonlit-harbour"),
    )
    .await;

    assert!(
        outcome
            .routing
            .shape
            .contains("Demoted(HeaderEncryptedRefused(NoVerifiedCandidate))"),
        "the archive's own check refutes every candidate, and the refusal must say so, got {}",
        outcome.routing.shape
    );
    assert!(
        !outcome.routing.partial_seen,
        "nothing may have been written on the strength of a refuted password"
    );

    assert!(
        outcome.refetched.is_empty(),
        "the refusal lands on the first parse, so its live article must continue \
         conventionally without a refetch; got {:?}",
        outcome.refetched
    );
    assert!(
        outcome.volumes_byte_exact,
        "and the volumes it materializes are the pre-E4 floor: the archive exactly as \
         posted, not an approximation of it — {} of {} reached disk",
        outcome.volumes_materialized,
        volumes.len()
    );
    assert_eq!(
        outcome.member.as_deref(),
        Some(payload.as_slice()),
        "a password that arrives after the refusal still has to produce the member, through \
         the conventional path the refusal handed the set to"
    );
    assert_eq!(outcome.member_location, Some("complete"));
    assert!(
        matches!(outcome.status, Some(JobStatus::Complete)),
        "got {:?}",
        outcome.status
    );
}

#[tokio::test]
async fn a_header_encrypted_set_with_no_password_demotes_by_name() {
    let member_name = "Silver.Horizon.S04E04.mkv";
    let payload: Vec<u8> = (0..2000u32).map(|index| (index % 199) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );

    let outcome = hp_fallback_outcome(
        JobId(45031),
        member_name,
        &volumes,
        &in_order_arrivals(volumes.len()),
        None,
        sample_nzb_zstd(),
        // Supplied after the refusal, which is the ordinary shape of this case:
        // a passworded post nobody labelled, refused in seconds rather than
        // after `MAX_HEADER_PREFIX_BYTES` of staging, and typed in later.
        Some("moonlit-harbour"),
    )
    .await;

    // Named, and named *at the first parse*. Before the named refusal this
    // volume's articles simply staged: `parse_volume_facts` failed, the router
    // read that as "the prefix is too short", and the set sat un-demoted until
    // it had burned `MAX_HEADER_PREFIX_BYTES`. So an assertion on the name is
    // also an assertion that the parse failure was recognised rather than
    // swallowed — the old code reaches no `Demoted(...)` here at all.
    assert!(
        outcome
            .routing
            .shape
            .contains("Demoted(HeaderEncryptedRefused(NoPassword))"),
        "a `-hp` set with no candidate must demote by name, got {}",
        outcome.routing.shape
    );
    assert!(
        !outcome
            .routing
            .shape
            .contains("Demoted(HeaderEncryptedRefused(NoVerifiedCandidate))"),
        "\"nothing to try\" and \"everything tried was wrong\" are different operational \
         stories, got {}",
        outcome.routing.shape
    );
    assert!(!outcome.routing.partial_seen);

    // Refusing early is only better than waiting because the wait was never
    // buying anything: the set still reaches the conventional path with every
    // byte intact, and a password supplied afterwards still produces the member.
    assert!(outcome.refetched.is_empty());
    assert!(
        outcome.volumes_byte_exact,
        "{} of {} volumes reached disk",
        outcome.volumes_materialized,
        volumes.len()
    );
    assert_eq!(outcome.member.as_deref(), Some(payload.as_slice()));
    assert_eq!(outcome.member_location, Some("complete"));
    assert!(
        matches!(outcome.status, Some(JobStatus::Complete)),
        "got {:?}",
        outcome.status
    );
}

#[tokio::test]
async fn a_header_encrypted_set_does_not_try_a_placeholder_spec_password() {
    // `spec.password` reaches the `-hp` ring by a different route from the rest
    // of the harvest — the per-article re-offer in `refresh_direct_passwords` —
    // and it has to be normalized the way
    // `archive_password_candidates_for_job` normalizes, or the two routes
    // disagree about what a password even *is*.
    //
    // The case is not hypothetical: indexers have written `password=yes` since
    // the NZBGet era to mean "this post is passworded", and
    // `normalize_archive_password_candidate` drops that whole family. Offering
    // it here anyway would spend a PBKDF2 derivation proving nothing — and
    // would report the refusal as `NoVerifiedCandidate`, "everything we tried
    // was wrong", for a job that had nothing to try.
    let payload: Vec<u8> = (0..1600u32).map(|index| (index % 163) as u8).collect();
    let volumes = header_encrypted_store_set(
        "Silver.Horizon.S04E14.mkv",
        &payload,
        2,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );

    let outcome = hp_routing_outcome_named(
        JobId(45131),
        &volumes,
        &in_order_arrivals(volumes.len()),
        Some("yes"),
        sample_nzb_zstd(),
        None,
    )
    .await;

    assert!(
        outcome
            .shape
            .contains("Demoted(HeaderEncryptedRefused(NoPassword))"),
        "a placeholder is not a candidate, so this job had nothing to try, got {}",
        outcome.shape
    );
    assert!(
        !outcome.shape.contains("NoVerifiedCandidate"),
        "and it must not be reported as a password that was tried and refuted, got {}",
        outcome.shape
    );
}

/// **The `Verified`-only decision, as a test.**
///
/// Both fixtures below carry an archive whose password the job *has* — the right
/// one, in the spec, ready to use — and both must still refuse, because neither
/// states a check that could prove it. That is the whole difference from `-p`,
/// where the same `Unverifiable` verdict admits: a wrong key there corrupts data
/// the whole-member CRC32 then catches, and a wrong key here corrupts the header
/// parse that decides where bytes go.
///
/// If admission were relaxed to "admit on `Unverifiable`", both halves would
/// pass their parse and complete — so this test fails loudly rather than
/// quietly, and it fails for the exact change it is guarding.
///
/// # And the refusal is only defensible because the floor holds
///
/// Refusing an archive the job *can* open is a real cost, and the argument that
/// it is the right trade is entirely about what happens next: the conventional
/// path takes the set and opens it with the very same password. So each half
/// runs that through — the refetch the demotion asked for, the volumes it
/// materializes, the extraction, the member. Asserting only that a volume file
/// appeared would leave the trade unproven in the one test whose whole subject
/// is the trade.
#[tokio::test]
async fn a_header_encrypted_set_with_no_usable_check_refuses_rather_than_guessing() {
    let member_name = "Silver.Horizon.S04E05.mkv";
    let payload: Vec<u8> = (0..1800u32).map(|index| (index % 211) as u8).collect();
    for (label, check) in [
        ("an omitted check", HeaderCheck::Absent),
        (
            "a check whose own tag is wrong",
            HeaderCheck::ForgedTag("moonlit-harbour"),
        ),
    ] {
        let volumes =
            header_encrypted_store_set(member_name, &payload, 2, "moonlit-harbour", check);
        let outcome = hp_fallback_outcome(
            JobId(45041),
            member_name,
            &volumes,
            &in_order_arrivals(volumes.len()),
            // The **right** password, in hand, before the first article.
            Some("moonlit-harbour"),
            sample_nzb_zstd(),
            None,
        )
        .await;

        assert!(
            outcome
                .routing
                .shape
                .contains("Demoted(HeaderEncryptedRefused(Unverifiable))"),
            "{label}: an unprovable `-hp` archive must refuse even when the password is \
             right, got {}",
            outcome.routing.shape
        );
        assert!(
            !outcome.routing.partial_seen,
            "{label}: nothing may have been written on the strength of an unproved password"
        );

        // The floor, run rather than assumed.
        assert!(
            outcome.refetched.is_empty(),
            "{label}: the decoder must hand its live article directly to conventional \
             assembly rather than refetching it; got {:?}",
            outcome.refetched
        );
        assert!(
            outcome.volumes_byte_exact,
            "{label}: and the volumes it then materializes must be the archive that was \
             posted, byte for byte — {} of {} reached disk",
            outcome.volumes_materialized,
            volumes.len()
        );
        assert_eq!(
            outcome.member.as_deref(),
            Some(payload.as_slice()),
            "{label}: and the conventional extractor must open it with the same password the \
             `-hp` gate refused to guess with, and produce the member"
        );
        assert_eq!(outcome.member_location, Some("complete"), "{label}");
        assert!(
            matches!(outcome.status, Some(JobStatus::Complete)),
            "{label}: a refused `-hp` job must reach a terminal state, got {:?}",
            outcome.status
        );
    }

    // Non-vacuity, against the same payload and the same password: the *only*
    // thing that changed is the archive stating a usable check, and with it the
    // set routes to completion. Without this the two halves above would pass
    // against a build that refused every `-hp` set for any reason at all.
    let member_name = "Silver.Horizon.S04E05.mkv";
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );
    let routed = run_hp_gate(
        DirectStoreGate::Enabled,
        JobId(45042),
        member_name,
        &volumes,
        &in_order_arrivals(volumes.len()),
        Some("moonlit-harbour"),
        sample_nzb_zstd(),
        None,
    )
    .await;
    assert!(!routed.volume_file_seen);
    assert_eq!(routed.member.as_deref(), Some(payload.as_slice()));
    assert_eq!(routed.member_location, Some("complete"));
}

#[tokio::test]
async fn a_rar4_header_encrypted_set_refuses_by_name() {
    // Permanent, and not a gap to be filled later: RAR4 derives a fresh key per
    // header from that header's own 8-byte salt and carries no password-check
    // value anywhere, so a wrong password is detected only by walking off the
    // end of the archive.
    let volumes = header_encrypted_rar4_set(2, 4096);
    assert_eq!(
        &volumes[0].1[..7],
        &TEST_RAR4_SIG,
        "the fixture really is RAR4"
    );

    let outcome = hp_fallback_outcome(
        JobId(45051),
        "Silver.Horizon.S04E09.mkv",
        &volumes,
        &in_order_arrivals(volumes.len()),
        // The password is present and correct-looking. It changes nothing,
        // which is the point: there is nothing to prove it against.
        Some("moonlit-harbour"),
        sample_nzb_zstd_with_password("moonlit-harbour"),
        None,
    )
    .await;

    assert!(
        outcome
            .routing
            .shape
            .contains("Demoted(HeaderEncryptedRefused(Rar4Headers))"),
        "RAR4 `-hp` must refuse under its own name rather than under a wrong-password one, \
         got {}",
        outcome.routing.shape
    );
    assert!(!outcome.routing.partial_seen);

    // The floor, for the one refusal that is permanent. No member is asserted
    // and none could be: this fixture is a RAR4 `-hp` header over bytes that are
    // deliberately not decryptable, because RAR4 offers nothing to decrypt them
    // *against* — which is the whole reason the refusal exists. What is asserted
    // is the handoff, and it is the entire remedy available: the conventional
    // path is given the archive exactly as posted, and reaches its own terminal
    // verdict on it rather than leaving the job wedged.
    assert!(outcome.refetched.is_empty());
    assert!(
        outcome.volumes_byte_exact,
        "{} of {} volumes reached disk",
        outcome.volumes_materialized,
        volumes.len()
    );
    assert!(
        matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "an archive nothing can open must *fail*, not hang: got {:?}",
        outcome.status
    );
}

#[tokio::test]
async fn a_header_encrypted_set_whose_kdf_count_is_over_the_ceiling_refuses_by_name() {
    // `lg2_count` is the *archive's* claim, so an unbounded one would let a
    // hostile post choose how much PBKDF2 an admission decision costs.
    // `unrar-rs` bounds it at `CRYPT5_KDF_LG2_COUNT_MAX` — RAR's own limit —
    // before it even reads the salt, and naming the refusal here is what stops
    // such a volume from also burning `MAX_HEADER_PREFIX_BYTES` of staging.
    let over = unrar_rs::CRYPT5_KDF_LG2_COUNT_MAX + 1;
    let volumes: Vec<(String, Vec<u8>)> = (0..2usize)
        .map(|volume| {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&TEST_RAR5_SIG);
            bytes.extend_from_slice(&build_test_rar_crypt_header(over, HeaderCheck::Absent));
            bytes.extend((0..4096usize).map(|index| ((index * 29 + volume * 7) % 256) as u8));
            (format!("silver.horizon.part{:02}.rar", volume + 1), bytes)
        })
        .collect();

    let outcome = hp_fallback_outcome(
        JobId(45061),
        "Silver.Horizon.S04E10.mkv",
        &volumes,
        &in_order_arrivals(volumes.len()),
        Some("moonlit-harbour"),
        sample_nzb_zstd(),
        None,
    )
    .await;

    assert!(
        outcome
            .routing
            .shape
            .contains("Demoted(HeaderEncryptedRefused(Unkeyable))"),
        "an archive demanding a derivation this build refuses must be named as such, got {}",
        outcome.routing.shape
    );
    assert!(!outcome.routing.partial_seen);

    // As with the RAR4 refusal: no member is asserted because the fixture is a
    // hostile type-4 record over filler and there is none, but the handoff is —
    // refusing the *derivation* must still preserve the posted bytes.
    assert!(outcome.refetched.is_empty());
    assert!(
        outcome.volumes_byte_exact,
        "{} of {} volumes reached disk",
        outcome.volumes_materialized,
        volumes.len()
    );
    assert!(
        matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "got {:?}",
        outcome.status
    );
}

#[tokio::test]
async fn a_header_encrypted_set_routes_payload_that_lands_before_its_volume_header() {
    // Out-of-order arrival, with the extra `-hp` twist: the article carrying
    // the type-4 record — without which nothing can be keyed at all — is the
    // *last* thing to arrive for every volume. Every byte before it has to be
    // retained, unclassifiable, and drain once the key exists.
    let member_name = "Silver.Horizon.S04E06.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 197) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );

    let mut arrivals: Vec<(u32, u32)> = (0..volumes.len() as u32).map(|index| (index, 1)).collect();
    arrivals.extend((0..volumes.len() as u32).map(|index| (index, 0)));

    let direct = run_hp_gate(
        DirectStoreGate::Enabled,
        JobId(45071),
        member_name,
        &volumes,
        &arrivals,
        None,
        sample_nzb_zstd_with_password("moonlit-harbour"),
        None,
    )
    .await;

    assert!(
        !direct.volume_file_seen,
        "bytes retained while the set was unkeyed must not have been materialized"
    );
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "held bytes must drain to their destination once the header key resolves"
    );
    assert_eq!(direct.member_location, Some("complete"));
    assert!(matches!(direct.status, Some(JobStatus::Complete)));
}

#[tokio::test]
async fn a_password_harvest_that_failed_does_not_arm_the_once_per_job_memo() {
    // The `-hp` harvest runs **once** per job, and what arms that memo decides
    // how a transient error is paid for. Its NZB half is a database read
    // followed by a parse, and both warn-and-continue with an empty list, so an
    // empty result is two different facts: "this job names no password", which
    // is permanent, and "the read failed this once", which is not.
    //
    // Both halves below are load-bearing and they pull in opposite directions:
    //
    // - Arming on a **failed** harvest costs the job its `NzbMeta` and
    //   `FilenameConvention` candidates for the rest of its life, with no second
    //   chance — `wants_header_password()` is the only other gate and it is still
    //   true. The conventional path has no such cliff: it re-harvests per volume
    //   parse.
    // - Not arming on an **empty but successful** one costs a persisted-NZB read,
    //   a zstd decompress and an XML parse on *every article of every
    //   password-free job*, which is very nearly every job there is.
    //
    // So the rule is "remember what was learned, and only that", and only the
    // memo itself can say which happened: the two runs are otherwise identical
    // and both end with zero candidates offered.
    let member_name = "Silver.Horizon.S04E13.mkv";
    let payload: Vec<u8> = (0..1600u32).map(|index| (index % 173) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );

    for (label, nzb_zstd, expect_armed) in [
        (
            "a persisted NZB that cannot be read",
            vec![0xFFu8; 64],
            false,
        ),
        (
            "a persisted NZB that reads and names no password",
            sample_nzb_zstd(),
            true,
        ),
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let job_id = JobId(45121);
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
        let spec = direct_store_job_spec("Silver Horizon", &volumes);
        insert_active_job_with_persisted_nzb(&mut pipeline, job_id, spec, nzb_zstd).await;

        // The *payload* article only. It is enough to run the harvest — the
        // routing seam calls it before it does anything with the bytes — and not
        // enough for any volume to have reached its type-4 record, so the set is
        // still asking for candidates and the memo is the only difference
        // between the two runs.
        submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;
        assert!(
            pipeline
                .direct_store
                .set(job_id, 0)
                .is_some_and(|set| set.router.wants_header_password()),
            "{label}: the set must still want candidates, or something other than the memo \
             is what stops the harvest re-running"
        );
        assert_eq!(
            pipeline.direct_store.header_candidates_offered(job_id),
            expect_armed,
            "{label}: a harvest is remembered when it ran and forgotten when it failed"
        );
    }
}

#[tokio::test]
async fn a_par2_bearing_header_encrypted_job_verifies_and_completes_byte_identically() {
    // The commonest real `-hp` shape, and the one every other test here builds
    // without: nearly every encrypted release carries PAR2, and both `-hp`
    // helpers switch live verification off.
    //
    // Nothing about `-hp` should reach the verifier — the archive key is used
    // to read *headers*, and PAR2 describes the posted volume image, which is
    // ciphertext either way. That is precisely the claim worth a test, because
    // it is the sort of claim that stays true only until something reads a
    // plaintext byte where a posted one belongs. The overlay answers the
    // verifier out of the routed member partials re-encrypted, and this asserts
    // that a `-hp` set stays in that arrangement all the way through
    // verification: it admits, it never writes a source volume, it reaches a
    // PAR2 verdict, it finalizes, and its member is byte-identical to the one
    // the conventional extractor produces from real volume files.
    let member_name = "Silver.Horizon.S04E12.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 191) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );

    let conventional = run_par2_direct_gate_with_password(
        DirectStoreGate::Disabled,
        JobId(45111),
        member_name,
        &volumes,
        Some("moonlit-harbour"),
    )
    .await;
    let direct = run_par2_direct_gate_with_password(
        DirectStoreGate::Enabled,
        JobId(45112),
        member_name,
        &volumes,
        Some("moonlit-harbour"),
    )
    .await;

    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written source volumes"
    );
    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional extractor should reproduce the member payload"
    );

    assert!(
        direct.admitted,
        "a par2-bearing `-hp` job must still admit its set, got {}",
        direct.demotions
    );
    assert!(
        !direct.volume_file_seen,
        "a routed `-hp` set must never create a source volume file, even to verify against, \
         got {}",
        direct.demotions
    );
    assert!(
        direct.demotions.contains("Finalized"),
        "the set must have stayed direct through verification and finalized, got {}",
        direct.demotions
    );
    assert!(
        direct.verdict_reached,
        "the job must have reached a PAR2 verdict rather than skipping the question; \
         authoritative={}",
        direct.authoritative_verify_calls
    );
    assert_eq!(
        (
            direct.member.as_deref(),
            direct.member_location,
            &direct.status
        ),
        (
            conventional.member.as_deref(),
            conventional.member_location,
            &conventional.status
        ),
        "a par2-bearing `-hp` job must produce the conventional gate's output, in the same \
         place, with the same status; sets = {}",
        direct.demotions
    );
    assert!(
        matches!(direct.status, Some(JobStatus::Complete)),
        "got {:?} with sets {}",
        direct.status,
        direct.demotions
    );
}

#[tokio::test]
async fn a_header_encrypted_set_keys_from_a_password_supplied_after_its_harvest_ran() {
    // **The re-offer in `refresh_direct_passwords`, as a test.**
    //
    // `offer_direct_header_passwords` runs once per job and every non-demoted
    // set wants a header password from creation, so the harvest is memoized on
    // the job's *first article* — whatever the set later turns out to be — and
    // can never run again. That is fine for the two candidate sources it reads,
    // `NzbMeta` and `FilenameConvention`, because both are immutable per job.
    // It is not fine for the third: `spec.password` is mutable, through
    // `setJobPassword` and through the NZBGet facade's `*Unpack:Password`, and
    // the per-article re-offer is its *only* route into the `-hp` ring after
    // that first article.
    //
    // Reaching that window needs two things at once, and this is the arrival
    // plan that produces them:
    //
    // 1. **The job holds no password anywhere** when its first article lands —
    //    empty spec, password-free NZB body, no `{{…}}` in the NZB's file name —
    //    so the harvest runs, finds nothing, and arms its memo on an empty list.
    // 2. **No volume has yielded its type-4 record yet**, so the `-hp` gate has
    //    not been asked to decide and has therefore not refused. Every volume's
    //    *payload* article arrives first; the record lives at the front of the
    //    volume, in the article that comes last.
    //
    // Only then does the operator supply the password. With the re-offer, the
    // header articles key the set and it completes; without it, the ring is
    // still holding zero candidates when the first record parses, and the set
    // refuses under `NoPassword` for a password the job was holding.
    let member_name = "Silver.Horizon.S04E11.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 227) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(45101);
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    // No password in the spec, none in the NZB body, and the NZB's name is the
    // job id — so `archive_password_candidates_for_job` has nothing to return.
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    assert!(spec.password.is_none());
    let working_dir =
        insert_active_job_with_persisted_nzb(&mut pipeline, job_id, spec, sample_nzb_zstd()).await;

    for file_index in 0..volumes.len() as u32 {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, 1).await;
    }

    // The window, stated: the harvest has run and can never run again, and the
    // ring is still open because nothing has parsed a record to decide against.
    assert!(
        pipeline.direct_store.header_candidates_offered(job_id),
        "the once-per-job harvest must already have run — that is what makes the re-offer \
         the only route left"
    );
    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the job must carry its direct set");
    assert!(
        !set.is_demoted(),
        "no volume has reached its type-4 record yet, so nothing can have been refused"
    );
    assert!(
        set.router.wants_header_password(),
        "and the ring must still be collecting candidates"
    );

    // The operator supplies it, mid-download.
    pipeline
        .jobs
        .get_mut(&job_id)
        .expect("the job is still active")
        .spec
        .password = Some("moonlit-harbour".to_string());

    for file_index in 0..volumes.len() as u32 {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, 0).await;
    }
    drain_rar_refreshes(&mut pipeline).await;

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "a password the job was holding when the record parsed must reach the `-hp` ring, \
         got {shape}"
    );

    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
    let (member, member_location) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert!(
        !volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists()),
        "a set keyed from a late password still routes, so no source volume may appear"
    );
    assert_eq!(
        member.as_deref(),
        Some(payload.as_slice()),
        "and the member it produces is the ordinary one"
    );
    assert_eq!(member_location, Some("complete"));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
}

#[tokio::test]
async fn a_header_encrypted_set_restarted_mid_download_completes_byte_identically() {
    // The archive key is never persisted, so a restart has to prove one of the
    // job's candidates again — and for the volume that **closes the chain** it
    // has to do it from the *envelope*: a restored volume's staged image has a
    // hole from offset zero, so the live parse path never reaches the type-4
    // record through it, and a closing volume cannot be confirmed structurally
    // the way a split one can (its last member does not continue, so an
    // undiscovered header could sit past its data area).
    //
    // The arrival plan is shaped for exactly that. Volumes 0 and 1 finish
    // before the restart; the **last** volume is the half-done one, so the
    // resumed run reaches `reconfirm_restored_volume` — and with it the
    // envelope-side keying — rather than the structural short-circuit a middle
    // volume takes.
    const ARTICLES: usize = 4;
    let member_name = "Silver.Horizon.S04E07.mkv";
    let payload: Vec<u8> = (0..8000u32).map(|index| (index % 251) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        3,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(45081);
    let arrivals: Vec<(u32, u32)> = vec![
        (0, 0),
        (0, 1),
        (0, 2),
        (0, 3),
        (1, 0),
        (1, 1),
        (1, 2),
        (1, 3),
        (2, 0),
        (2, 1),
    ];
    let working_dir = direct_store_before_restart_with_password(
        &temp_dir,
        job_id,
        &volumes,
        &arrivals,
        ARTICLES,
        Some("moonlit-harbour"),
    )
    .await;
    assert!(
        !working_dir.join(&volumes[0].0).exists(),
        "the pre-restart run must already have routed rather than materialized"
    );

    let mut pipeline = direct_store_after_restart_with_password(
        &temp_dir,
        DirectStoreGate::Enabled,
        job_id,
        &volumes,
        ARTICLES,
        &working_dir,
        Some("moonlit-harbour"),
    )
    .await;
    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the restored `-hp` job must carry its direct set");
    assert!(
        !set.is_demoted(),
        "a restart that can still prove a candidate must re-admit the set"
    );

    let queued = peek_queued_segments(&mut pipeline, job_id);
    assert!(
        !queued
            .iter()
            .any(|(file_index, _)| *file_index == 0 || *file_index == 1),
        "volumes 0 and 1 were complete at the barrier; none of their articles may be \
         refetched, got {queued:?}"
    );
    assert!(
        !queued.contains(&(2, 0)),
        "the closing volume's *first* article must stay below its floor — that is what \
         leaves the resumed run's staged image holed at offset zero, and with it the \
         type-4 record reachable only through the envelope. Got {queued:?}"
    );
    assert!(
        queued.iter().any(|(file_index, _)| *file_index == 2),
        "non-vacuity: the closing volume really was still owed articles, got {queued:?}"
    );
    for (file_index, segment_number) in queued {
        dispatch_and_submit(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            ARTICLES,
        )
        .await;
    }
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let complete_dir = temp_dir.path().join("complete");
    let (restarted, location) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert_eq!(
        restarted.as_deref(),
        Some(payload.as_slice()),
        "a restarted `-hp` set must finish byte-identical to an uninterrupted one"
    );
    assert_eq!(location, Some("complete"));
    assert!(
        !volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists()),
        "a restarted `-hp` set must still never materialize a source volume"
    );
}

#[tokio::test]
async fn a_header_encrypted_set_prefers_the_proved_candidate_over_the_spec_password() {
    // RAR uses **one** password for headers and file data alike, so the
    // candidate the archive's own check proved is the set's password — even
    // when `spec.password` holds a different one. An operator's wrong guess
    // takes priority in the spec; letting it reach the member key would open the
    // headers and then refuse the members under `EncryptedMemberRefused`.
    //
    // # The window this has to land in, and why an in-order run misses it
    //
    // [`DirectSetRouter::set_password`]'s guard — "a proved archive key is not
    // overwritten" — can only fire between two events: the plaintext type-4
    // record being staged, which is what *proves* the key, and the first
    // **file** header being staged, which admits the member and closes
    // `KeyRing`'s own door. Two articles per volume put both inside article 0:
    // `KeyRing::set_password`'s `admitted` check then blocks every later
    // overwrite by itself, the guard is never reached, and deleting it changes
    // nothing at all.
    //
    // So the arrival plan is sized to hold that window open. `ARTICLES` puts
    // article 0's boundary past the type-4 record and *inside* the encrypted
    // file header: the set keys itself on article 0 and admits nothing, and
    // `refresh_direct_passwords` on article 1 then offers `spec.password` to a
    // router that has already proved a different one. The three assertions
    // between the two articles are what keep it there — without them a fixture
    // that drifted back to "all in article 0" would pass while testing nothing.
    const ARTICLES: usize = 14;
    let member_name = "Silver.Horizon.S04E08.mkv";
    let payload: Vec<u8> = (0..1800u32).map(|index| (index % 181) as u8).collect();
    let volumes = header_encrypted_store_set(
        member_name,
        &payload,
        2,
        "moonlit-harbour",
        HeaderCheck::For("moonlit-harbour"),
    );
    let crypt_record_end = TEST_RAR5_SIG.len()
        + build_test_rar_crypt_header(TEST_HP_KDF_LG2, HeaderCheck::For("moonlit-harbour")).len();
    let first_article_end = article_extent(volumes[0].1.len(), 0, ARTICLES).1;
    assert!(
        first_article_end > crypt_record_end,
        "article 0 has to reach the plaintext type-4 record — it ends at {first_article_end} \
         and the record ends at {crypt_record_end} — or nothing is keyed and the guard is not \
         what is under test"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(45091);
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let mut spec = direct_store_job_spec_with_articles("Silver Horizon", &volumes, ARTICLES);
    // The operator's guess, which is wrong and which the spec prefers.
    spec.password = Some("not-the-password".to_string());
    let working_dir = insert_active_job_with_persisted_nzb(
        &mut pipeline,
        job_id,
        spec,
        // The NZB's, which is right.
        sample_nzb_zstd_with_password("moonlit-harbour"),
    )
    .await;

    submit_volume_article_of(&mut pipeline, job_id, &volumes, 0, 0, ARTICLES).await;
    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the `-hp` job must carry its direct set");
    assert!(
        !set.is_demoted(),
        "article 0 must have keyed the set from the NZB's password, not refused it"
    );
    assert!(
        !set.router.wants_header_password(),
        "the archive key must already be **proved** after article 0 — a ring still asking for \
         candidates has proved nothing, and there is no guard to reach"
    );
    assert!(
        !set.router.routes_encrypted(),
        "and no member may be admitted yet: the moment one is, `KeyRing`'s own `admitted` \
         check refuses every later password and the guard becomes unreachable. This is the \
         window the guard exists for."
    );

    for file_index in 0..volumes.len() as u32 {
        for segment_number in 0..ARTICLES as u32 {
            if (file_index, segment_number) == (0, 0) {
                continue;
            }
            submit_volume_article_of(
                &mut pipeline,
                job_id,
                &volumes,
                file_index,
                segment_number,
                ARTICLES,
            )
            .await;
        }
    }
    drain_rar_refreshes(&mut pipeline).await;

    // Read before extraction is driven, both because a completed job has its
    // direct-store runtime pruned and because this is the assertion that names
    // the guard: a spec password that displaced the proved one opens the headers
    // and then refuses the *members*, which is a demotion under a wholly
    // different reason.
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "the spec's password must never displace one the archive's own check proved, got {shape}"
    );

    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    let (member, member_location) = member_after_gate(&complete_dir, &working_dir, member_name);
    assert!(
        !volumes
            .iter()
            .any(|(filename, _)| working_dir.join(filename).exists()),
        "the set routed throughout and must never have materialized a source volume"
    );
    assert_eq!(
        member.as_deref(),
        Some(payload.as_slice()),
        "the proved candidate must key the member data too, and must survive every later \
         offer of the spec's"
    );
    assert_eq!(member_location, Some("complete"));
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    ));
}

/// A volume's articles arrive in whatever order twelve connections finish
/// them, so several mid-file articles routinely land before the one carrying
/// offset zero. The unparsable ceiling must judge the prefix the header walk
/// can actually consume — not the sum of tail chunks the walk cannot reach.
/// Before the fix this exact sequence demoted a store-method set
/// `UnparsableVolume` with its headers never read: six tail articles staged
/// ~4.2 MiB, segment zero arrived seventh, and the ceiling fired before the
/// first parse ever ran. (Witnessed live as 23 of 44 demotions in one
/// functional-direct run, mislabeling compressed sets and falsely demoting
/// the store sets direct routing exists to carry.)
#[tokio::test]
async fn out_of_order_arrival_does_not_trip_the_header_prefix_ceiling() {
    // Payload comfortably past MAX_HEADER_PREFIX_BYTES so the tail articles
    // alone exceed the ceiling the old measure judged.
    let payload: Vec<u8> = (0..(6 * 1024 * 1024) as u32)
        .map(|index| (index % 251) as u8)
        .collect();
    let volumes = single_member_store_set("Silver.Horizon.S01E07.mkv", &payload, 1);
    let articles = 9usize;
    let job_id = JobId(41077);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);

    let spec = direct_store_job_spec_with_articles("Out-of-order store set", &volumes, articles);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let (_, volume_bytes) = &volumes[0];
    // Segments 1.. land first; segment zero arrives last.
    let order: Vec<u32> = (1..articles as u32).chain(std::iter::once(0)).collect();
    for segment in order {
        let (start, end) = article_extent(volume_bytes.len(), segment, articles);
        submit_decoded_segment(
            &mut pipeline,
            NzbFileId {
                job_id,
                file_index: 0,
            },
            segment,
            start as u64,
            &volume_bytes[start..end],
            &volumes[0].0,
            None,
        )
        .await;
        let state = format!("{:?}", pipeline.direct_store.sets_for(job_id));
        assert!(
            !state.contains("Demoted"),
            "the set must never demote on arrival order alone; after segment \
             {segment}: {state}"
        );
    }
}

// ---------------------------------------------------------------------------
// The dual-CRC grid, fed from the direct seam
//
// A direct set's source volumes leave the conventional decode path before its
// commit seam, so until the routing seam fed the grid itself a direct volume
// carried no block verdict at all — and every PAR2 pass over a direct set read
// every byte back through the virtual-volume adapter to learn what the decode
// pass already knew.
//
// Two preconditions run through all of these, and both are production shapes:
// the recovery set has to be **parsed before the volumes decode**, because that
// is where the block size a decoder cuts on comes from; and the articles have to
// carry block-grid CRC segments, because a whole-article record tiles no block
// it does not exactly coincide with.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn direct_routed_articles_claim_their_blocks_in_the_dual_crc_grid() {
    // The wiring itself. A direct volume's bytes never become a file, but they
    // are durable — in member partials and envelopes — before this seam records
    // anything, which is the same contract the conventional seam states. So the
    // grid may claim them, and every described slice of every volume has to come
    // back Intact with independent (pCRC-verified) coverage.
    //
    // The last volume's last article is withheld so the set is still **live**
    // when the verdicts are read: a set whose volumes all complete reaches its
    // PAR2 verdict inside the feed and finalizes, and finalization takes the
    // virtual volume image apart and retires the claims with it.
    let member_name = "Silver.Horizon.S03E01.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 197) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41401);
    let (pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed {
            withhold_last_article: true,
            ..GridFeed::default()
        },
    )
    .await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !sets.contains("Demoted"),
        "non-vacuity: the set has to have stayed direct, or these verdicts came \
         off the conventional path; got {sets}"
    );
    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");

    let complete_volumes = volumes.len() as u32 - 1;
    assert!(
        complete_volumes > 1,
        "non-vacuity: more than one volume has to have completed, or this proves \
         nothing about a set"
    );
    for ordinal in 0..complete_volumes {
        let verdicts = verdicts_for(&pipeline, job_id, ordinal);
        assert!(
            !verdicts.is_empty(),
            "volume {ordinal} produced no block verdict at all — the direct seam \
             never reached the grid; sets = {sets}"
        );
        assert!(
            verdicts.values().all(|verdict| matches!(
                verdict,
                crate::pipeline::integrity::BlockVerdict::Intact {
                    independently_covered: true
                }
            )),
            "volume {ordinal} claimed a block without independent coverage or with \
             damage: {verdicts:?}"
        );

        let file_id = NzbFileId {
            job_id,
            file_index: IndexPosition::First.volume_file_index(ordinal),
        };
        let grid_match = pipeline.in_stream_verified_par2_match(file_id, &par2_set);
        assert!(
            grid_match.is_some(),
            "and the volume must bind to its description on the grid alone — every \
             described slice Intact at exactly the described length; verdicts = \
             {verdicts:?}"
        );
        let described = par2_set
            .file_description(&grid_match.expect("bound above").0)
            .expect("the description the binding named")
            .length;
        assert_eq!(
            verdicts.len() as u32,
            par2_set.slice_count_for_file(described),
            "every described slice must be claimed, not merely some of them"
        );
    }
}

#[tokio::test]
async fn a_direct_volumes_final_short_block_closes_on_its_decoded_length() {
    // The trap this exists for: the NZB's declared segment sizes are yEnc-
    // *encoded*, around 3% larger than the bytes that land, and closing the
    // short final block on that number puts its boundary past the described
    // extent — where `verdicts_against` refuses the comparison and the last
    // slice of every volume silently falls back to a read. The fixtures declare
    // inflated sizes precisely so this cannot pass by accident.
    let member_name = "Silver.Horizon.S03E02.mkv";
    let payload: Vec<u8> = (0..2350u32).map(|index| (index % 181) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41402);
    // The last volume is held back for the same reason as above: finalization
    // retires the claims, and this test is about how they were made.
    let (pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed {
            withhold_last_article: true,
            ..GridFeed::default()
        },
    )
    .await;
    let par2_set = pipeline
        .par2_set(job_id)
        .cloned()
        .expect("the index parsed");

    for ordinal in 0..volumes.len() as u32 - 1 {
        let decoded_len = volumes[ordinal as usize].1.len() as u64;
        assert_ne!(
            decoded_len % PAR2_SLICE_BYTES,
            0,
            "non-vacuity: volume {ordinal} has to end mid-block, or there is no \
             short final block to close"
        );
        let file_id = NzbFileId {
            job_id,
            file_index: IndexPosition::First.volume_file_index(ordinal),
        };
        let declared = pipeline
            .jobs
            .get(&job_id)
            .and_then(|state| state.assembly.file(file_id))
            .expect("the volume's assembly")
            .total_bytes();
        assert!(
            declared > decoded_len,
            "non-vacuity: the fixture must declare yEnc-encoded sizes, or the two \
             lengths agree and the trap cannot fire; declared={declared} \
             decoded={decoded_len}"
        );

        let last_block = u32::try_from((decoded_len - 1) / PAR2_SLICE_BYTES).unwrap();
        let verdicts = verdicts_for(&pipeline, job_id, ordinal);
        assert_eq!(
            verdicts.get(&last_block),
            Some(&crate::pipeline::integrity::BlockVerdict::Intact {
                independently_covered: true
            }),
            "volume {ordinal}'s final short block must close against the DECODED \
             length; verdicts = {verdicts:?}"
        );
        assert_eq!(
            verdicts.len() as u32,
            par2_set.slice_count_for_file(decoded_len),
            "and it must be the last of a complete claim, not an isolated one"
        );
    }
}

#[tokio::test]
async fn a_grid_verified_direct_set_takes_the_zero_io_session_pass() {
    // The prize. Every described slice of every volume was adjudicated in
    // stream, so the retained session — which reads no source bytes at all, its
    // `analyze()` skipping the scan because the volumes are absent from the
    // directory by construction — reports the verdict from evidence alone.
    // Before the seam fed the grid this arm was unreachable by construction.
    let member_name = "Silver.Horizon.S03E03.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41403);
    let (pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed {
            retained_session: true,
            ..GridFeed::default()
        },
    )
    .await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        pipeline.direct_session_pass_calls > 0,
        "the session arm must have answered for this set; sets = {sets}"
    );
    assert!(
        pipeline.direct_verify_read_splits.is_empty(),
        "and no read-and-verify pass may have run at all — that pass reads every \
         volume back through the virtual-volume adapter, which is the I/O this \
         whole seam exists to avoid; splits = {:?}",
        pipeline.direct_verify_read_splits
    );
    assert!(
        sets.contains("Finalized"),
        "and the verdict has to have cleared the set, or the zero-I/O pass \
         concluded something the job could not act on; got {sets}"
    );
}

#[tokio::test]
async fn a_damaged_direct_set_reads_only_the_volume_the_grid_could_not_claim() {
    // The damaged-set prize. One volume's envelope is corrupt, so the
    // all-or-nothing session gate refuses and the read-and-verify pass runs —
    // but re-reading the two volumes the decode pass already proved clean is
    // pure cost. They are stood in for on the same bar the session demands, and
    // only the damaged one is read.
    const RR_BYTES: usize = 512;
    let member_name = "Silver.Horizon.S03E04.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 211) as u8).collect();
    let clean = recovery_record_store_set(member_name, &payload, 3, RR_BYTES);
    let par2_bytes = repairable_par2_index(&clean, 4);
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, RR_BYTES);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41404);
    let (mut pipeline, _, complete_dir) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed::default(),
    )
    .await;

    assert_eq!(
        pipeline.direct_verify_read_splits.first().copied(),
        Some((2, 1)),
        "the pass that reached the damage verdict had to stand in for the two \
         clean volumes and read only the damaged one. (3, 0) would mean the grid \
         claimed damage it cannot see; (0, 3) would mean the direct seam never \
         reached the grid at all. splits = {:?}",
        pipeline.direct_verify_read_splits
    );
    assert!(
        pipeline.direct_session_pass_calls == 0,
        "and the zero-I/O session arm must refuse a set it can only half claim"
    );

    // The job still ends where it always did: repaired in place, member
    // extracted, no volume file ever written.
    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !sets.contains("Demoted"),
        "the set must repair in place rather than hand its volumes back; got {sets}"
    );
    drive_grid_fed_job_to_terminal(&mut pipeline, job_id).await;
    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    let member = std::fs::read(output_root.join(member_name))
        .ok()
        .or_else(|| staging_member(&complete_dir, member_name));
    assert_eq!(
        member.as_deref(),
        Some(payload.as_slice()),
        "and it must still reach the member the conventional gate produces; \
         sets = {sets}"
    );
}

#[tokio::test]
async fn demoting_a_direct_set_forgets_the_grid_state_its_volumes_carried() {
    // A demotion hands the volumes back to the conventional path, which is
    // about to fill real files with them — reconstructed from the routed bytes
    // or refetched off the wire. The direct phase's claims describe a different
    // image of the same coordinates, and merging the two would let a block
    // closed over one adjudicate bytes of the other.
    let member_name = "Silver.Horizon.S03E05.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 149) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41405);
    let (mut pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed {
            withhold_last_article: true,
            ..GridFeed::default()
        },
    )
    .await;

    let volume_file = NzbFileId {
        job_id,
        file_index: IndexPosition::First.volume_file_index(0),
    };
    let claimed_before = verdicts_for(&pipeline, job_id, 0);
    assert!(
        claimed_before.len() > 1,
        "non-vacuity: the volume has to carry claims across more than one \
         article, or 'the untouched half kept nothing' proves nothing; got \
         {claimed_before:?}"
    );

    pipeline
        .demote_direct_set(job_id, 0, DemotionReason::HoldsBudgetExceeded)
        .await;
    settle_direct_post_repair_work(&mut pipeline).await;

    assert!(
        verdicts_for(&pipeline, job_id, 0).is_empty(),
        "the demotion must retire every claim the direct phase made about this \
         volume"
    );

    // One article comes back conventionally, carrying bytes that are NOT what
    // the recovery set describes. Only the blocks it tiles may have a verdict at
    // all, and they must read Damaged: anything the direct phase claimed about
    // the rest would be a verdict about an image that no longer exists.
    let (start, end) = grid_article_extent(&volumes, 0, 0);
    let (filename, bytes) = &volumes[0];
    let rewritten: Vec<u8> = bytes[start..end].iter().map(|byte| !byte).collect();
    submit_grid_cut_article(
        &mut pipeline,
        job_id,
        volume_file.file_index,
        0,
        start as u64,
        &rewritten,
        filename,
    )
    .await;

    let after = verdicts_for(&pipeline, job_id, 0);
    assert!(
        !after.is_empty(),
        "non-vacuity: the conventional re-feed has to reach the grid, or the \
         assertion below is satisfied by an empty map"
    );
    assert!(
        after
            .values()
            .all(|verdict| matches!(verdict, crate::pipeline::integrity::BlockVerdict::Damaged)),
        "every verdict must come from the bytes the conventional path just \
         wrote: {after:?}"
    );
    assert!(
        after.len() < claimed_before.len(),
        "and the articles that did not come back must carry no verdict at all — \
         a surviving direct-phase claim is exactly what the forget prevents; \
         before={claimed_before:?} after={after:?}"
    );
}

#[tokio::test]
async fn a_replayed_direct_article_re_adjudicates_the_range_it_rewrote() {
    // The grid is positional, not sequential, so a duplicate is fed through the
    // direct seam on purpose — exactly as the conventional seam feeds one. The
    // replay rewrote its range on disk, and a verdict derived before it can only
    // stand if it is derived again from the arrival that did the rewriting.
    let member_name = "Silver.Horizon.S03E06.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 167) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41406);
    let (mut pipeline, _, _) = grid_fed_direct_job(
        &temp_dir,
        job_id,
        &volumes,
        &par2_bytes,
        GridFeed {
            withhold_last_article: true,
            ..GridFeed::default()
        },
    )
    .await;

    let before = verdicts_for(&pipeline, job_id, 0);
    let derived_before = pipeline.block_crcs.blocks_derived();
    let (start, end) = grid_article_extent(&volumes, 0, 0);
    let first_block = u32::try_from(start as u64 / PAR2_SLICE_BYTES).unwrap();
    let straddling_block = u32::try_from((end as u64 - 1) / PAR2_SLICE_BYTES).unwrap();
    assert!(
        first_block < straddling_block,
        "non-vacuity: the replayed article has to tile at least one whole block \
         AND end inside another, or 'by range' has no range to be about"
    );
    assert_ne!(
        end as u64 % PAR2_SLICE_BYTES,
        0,
        "non-vacuity: the article must end mid-block, or nothing straddles the \
         boundary"
    );
    assert!(
        before.len() as u32 > straddling_block + 1,
        "non-vacuity: there have to be blocks beyond the replayed range to \
         survive it; got {before:?}"
    );

    let (filename, bytes) = &volumes[0];
    submit_grid_cut_article(
        &mut pipeline,
        job_id,
        IndexPosition::First.volume_file_index(0),
        0,
        start as u64,
        &bytes[start..end],
        filename,
    )
    .await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !sets.contains("Demoted"),
        "non-vacuity: a byte-identical replay must not demote the set, or this \
         measures the demotion rather than the feed; got {sets}"
    );
    assert!(
        pipeline.block_crcs.blocks_derived() > derived_before,
        "the duplicate must have reached the grid and closed its blocks again: a \
         seam that skipped the feed would leave the counter where it was, and \
         with it a claim nothing re-derived"
    );

    let after = verdicts_for(&pipeline, job_id, 0);
    for block in first_block..straddling_block {
        assert_eq!(
            after.get(&block),
            Some(&crate::pipeline::integrity::BlockVerdict::Intact {
                independently_covered: true
            }),
            "a block the replay tiles on its own re-derives from the arrival that \
             rewrote it; after = {after:?}"
        );
    }
    assert!(
        !after.contains_key(&straddling_block),
        "the block straddling the replay's end must go unclaimed: the other \
         half's contribution was retired when that block first closed, and a \
         retired observation cannot vouch for bytes a rewrite has since \
         touched; after = {after:?}"
    );
    for (block, verdict) in &before {
        if *block > straddling_block {
            assert_eq!(
                after.get(block),
                Some(verdict),
                "and a block outside the rewritten range must keep the claim it \
                 already had — the invalidation is by RANGE, not by file; \
                 after = {after:?}"
            );
        }
    }
}
