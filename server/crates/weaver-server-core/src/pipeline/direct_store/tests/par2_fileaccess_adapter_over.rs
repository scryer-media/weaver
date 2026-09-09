//! The PAR2 `FileAccess` adapter over virtual volumes
//! Restart primitives — the pieces the end-to-end restart tests exercise only in
//! The restart gate re-arm, over a router rebuilt from cached facts
//! The holds scratch and its region index
//! Damage accounting over an interior hole
//! CRC composition under repair
//! The ordering rule, and what a crash in its window costs
//! What the repair is sized from
//! Sparse marking
//! Snapshot schema 4: the crypt row
//! The member tolerance's kind gate
//! What a demoted set's consumers need under it

use super::*;

// ---------------------------------------------------------------------------
// The PAR2 `FileAccess` adapter over virtual volumes
// ---------------------------------------------------------------------------

#[test]
fn a_no_ifsc_whole_file_md5_streams_through_the_sequential_reader() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = provider_fixture(whole_volume_covered());
    let par2_set = descriptor_only_par2_set("silver.horizon.part01.rar", &fixture.conventional);
    let (access, file_id) = virtual_file_access(&fixture, &par2_set, dir.path());
    let counters = access.counters();

    assert!(
        par2_rs::verify_full_hash(&par2_set, &file_id, &access)
            .expect("the description names a registered virtual volume"),
        "the whole-file MD5 of a virtual volume must match the volume the \
         conventional gate would have written"
    );
    assert!(
        counters.sequential_opens() > 0,
        "the whole-file MD5 must stream through `open_sequential_reader`"
    );
    assert_eq!(
        counters.ranged_reads(),
        0,
        "verification requires the sequential path so a no-IFSC set does not degrade into \
         ranged reads across member partials"
    );
}

#[test]
fn an_encrypted_no_ifsc_whole_file_md5_streams_through_the_sequential_reader() {
    // The cost argument, over cipher. A set with no IFSC packets is verified by
    // whole-file MD5, and the MD5 it has to match is the **posted** volume's —
    // so this is both the strongest byte-for-byte statement about the overlay
    // and the one place its read *shape* matters: degrade into ranged reads
    // here and every no-IFSC encrypted set pays a seek per slice across the
    // member partials, which is the "no worse than today" claim failing.
    let dir = tempfile::tempdir().unwrap();
    let (posted, plain, crypt, covered) = encrypted_member_facts(3000, 256);
    let facts = crypt
        .cipher_facts(plain.len() as u64, &covered)
        .expect("a sized member has read-side facts");
    let volume = cipher_volume(dir.path(), &plain, facts, plain.len() as u64);
    let par2_set = descriptor_only_par2_set("silver.horizon.part01.rar", &posted[..plain.len()]);
    let file_id = par2_set.recovery_file_ids[0];
    let inner = par2_rs::PlacementFileAccess::new(
        dir.path().to_path_buf(),
        &par2_set,
        std::collections::HashMap::new(),
    );
    let provider = super::super::provider::HybridVolumeProvider::new(vec![volume]);
    let cipher_counters = provider.cipher_counters();
    let access = DirectVolumeFileAccess::new(
        inner,
        provider,
        &[VirtualPar2Volume {
            par2_file_id: file_id,
            volume_index: 0,
        }],
    );
    let counters = access.counters();

    assert!(
        par2_rs::verify_full_hash(&par2_set, &file_id, &access)
            .expect("the description names a registered virtual volume"),
        "the whole-file MD5 of an encrypted virtual volume must match the volume \
         as it was posted, not as it was decrypted"
    );
    assert!(
        counters.sequential_opens() > 0,
        "the whole-file MD5 must stream through `open_sequential_reader`"
    );
    assert_eq!(
        counters.ranged_reads(),
        0,
        "an encrypted no-IFSC set must not degrade into ranged reads across the \
         member partials"
    );
    assert_eq!(
        cipher_counters.chained_bytes(),
        0,
        "and the sequential sweep must carry its own CBC chain, so no byte is \
         re-encrypted twice"
    );
    assert_eq!(cipher_counters.refusals(), 0);
}

#[test]
fn an_ascending_ranged_sweep_reuses_one_reader_instead_of_re_seeding_every_slice() {
    // `read_file_range_into` used to open a reader per call, so
    // `VirtualVolumeReader::chains` started empty every time and every PAR2
    // slice of an encrypted volume re-seeded from the nearest retained
    // checkpoint — re-encrypting everything between it and the slice, and
    // throwing all of it away. On the overlay's own fixture that was 51,487
    // bytes delivered against 125,828,800 chained.
    //
    // Both halves are measured here rather than asserted from a constant: the
    // baseline sweep opens a reader per read exactly as the adapter used to, and
    // the cached one goes through the adapter. The bytes have to agree, and the
    // chaining has to collapse.
    let dir = tempfile::tempdir().unwrap();
    // No strided checkpoint fits in a member this size, so the only seed a
    // per-read reader can reach is the member's IV — which is precisely the
    // shape that makes the cost quadratic.
    let (posted, plain, crypt, covered) = encrypted_member_facts(64 * 1024, 4096);
    let facts = crypt
        .cipher_facts(plain.len() as u64, &covered)
        .expect("a sized member has read-side facts");
    let volume_len = plain.len() as u64;
    let slice = 512u64;
    let windows: Vec<(u64, u64)> = (0..volume_len)
        .step_by(slice as usize)
        .map(|start| (start, slice.min(volume_len - start)))
        .collect();
    assert!(windows.len() > 32, "non-vacuity: the sweep must be a sweep");

    // The baseline: one reader per read, which is what the adapter did.
    let baseline_provider = super::super::provider::HybridVolumeProvider::new(vec![cipher_volume(
        dir.path(),
        &plain,
        facts.clone(),
        volume_len,
    )]);
    let baseline_counters = baseline_provider.cipher_counters();
    let mut baseline = Vec::with_capacity(plain.len());
    for &(start, len) in &windows {
        let mut reader = baseline_provider.open(0).expect("registered");
        std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(start)).unwrap();
        let mut got = vec![0u8; len as usize];
        std::io::Read::read_exact(&mut reader, &mut got).unwrap();
        baseline.extend_from_slice(&got);
    }
    assert_eq!(
        baseline,
        posted[..plain.len()],
        "non-vacuity: the baseline sweep must reproduce the posted bytes too"
    );
    let delivered = plain.len() as u64;
    assert!(
        baseline_counters.chained_bytes() > delivered * 32,
        "non-vacuity: a reader per read must really chain orders of magnitude \
         more than it delivers, got {} chained for {delivered} delivered",
        baseline_counters.chained_bytes()
    );

    // And the adapter, which now keeps one reader per volume.
    let volume = cipher_volume(dir.path(), &plain, facts, volume_len);
    let par2_set = descriptor_only_par2_set("silver.horizon.part01.rar", &posted[..plain.len()]);
    let (access, file_id, counters) = encrypted_file_access(volume, &par2_set, dir.path());
    let mut cached = Vec::with_capacity(plain.len());
    for &(start, len) in &windows {
        cached.extend_from_slice(
            &access
                .read_file_range(&file_id, start, len)
                .expect("a covered range reads"),
        );
    }
    assert_eq!(
        cached,
        posted[..plain.len()],
        "the cached reader must deliver exactly the posted bytes"
    );
    assert_eq!(
        counters.chained_bytes(),
        0,
        "an ascending sweep through one reader continues the previous read's \
         chain, so nothing is re-encrypted twice"
    );
    assert!(
        counters.chained_bytes() * 100 < baseline_counters.chained_bytes(),
        "the whole point: {} chained now against {} before",
        counters.chained_bytes(),
        baseline_counters.chained_bytes()
    );
    assert_eq!(counters.refusals(), 0);
}

#[test]
fn a_descending_or_gapped_ranged_sequence_reads_the_same_bytes_through_the_cached_reader() {
    // The other half of the frontier finding. A kept frontier is only ever
    // accepted on an exact predecessor match or a strictly forward one that
    // beats the checkpoint, so a read *below* the frontier — or one that skips
    // over a stretch the reader never produced — falls back to the checkpoint
    // rather than carrying a predecessor that belongs to some other block. If
    // that ever stopped holding, the first block of each such read would be
    // wrong and nothing downstream could attribute it.
    let dir = tempfile::tempdir().unwrap();
    let (posted, plain, crypt, covered) = encrypted_member_facts(32 * 1024, 4096);
    let facts = crypt
        .cipher_facts(plain.len() as u64, &covered)
        .expect("a sized member has read-side facts");
    let volume_len = plain.len() as u64;
    let volume = cipher_volume(dir.path(), &plain, facts, volume_len);
    let par2_set = descriptor_only_par2_set("silver.horizon.part01.rar", &posted[..plain.len()]);
    let (access, file_id, counters) = encrypted_file_access(volume, &par2_set, dir.path());

    // Descending, gapped, block-aligned and not, re-reading windows already
    // read, and one that ends on the member's final block.
    let mut windows: Vec<(u64, u64)> = (0..volume_len)
        .step_by(1024)
        .map(|start| (start, 1024u64.min(volume_len - start)))
        .collect();
    windows.reverse();
    windows.extend([
        (volume_len - 16, 16),
        (7, 41),
        (4096, 17),
        (4096 + 3, 1),
        (0, 15),
        (volume_len / 2, 2048),
        (7, 41),
    ]);
    for (start, len) in windows {
        let end = (start + len).min(volume_len);
        assert_eq!(
            access
                .read_file_range(&file_id, start, end - start)
                .expect("a covered range reads"),
            posted[start as usize..end as usize],
            "a read at {start} for {len} through a reused reader must still be \
             byte-identical to what was posted"
        );
    }
    assert_eq!(counters.refusals(), 0);
}

#[test]
fn the_adapter_answers_existence_length_and_ranges_like_a_downloaded_volume() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = provider_fixture(whole_volume_covered());
    let par2_set = descriptor_only_par2_set("silver.horizon.part01.rar", &fixture.conventional);
    let (access, file_id) = virtual_file_access(&fixture, &par2_set, dir.path());

    assert!(access.file_exists(&file_id));
    assert_eq!(
        access.file_length(&file_id),
        Some(fixture.conventional.len() as u64)
    );
    // A range that crosses the header, a member extent, the envelope gap and the
    // second member — every source the overlay has — in one call.
    let range = access
        .read_file_range(&file_id, 8, (fixture.conventional.len() - 12) as u64)
        .expect("a covered range reads");
    assert_eq!(
        range,
        fixture.conventional[8..fixture.conventional.len() - 4],
        "a ranged read must reassemble the source volume across every backing file"
    );
    assert!(
        access
            .read_file_range(&file_id, 0, 1)
            .is_ok_and(|bytes| bytes == fixture.conventional[..1]),
        "the first byte comes from the envelope"
    );
}

#[test]
fn a_hole_reads_as_a_short_file_and_never_as_zeros() {
    let dir = tempfile::tempdir().unwrap();
    // Everything below the second member is covered; the tail is not.
    let mut covered = ByteRanges::new();
    let hole_at = (PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP) as u64;
    covered.insert(0, hole_at);
    let fixture = provider_fixture(covered);
    let par2_set = descriptor_only_par2_set("silver.horizon.part01.rar", &fixture.conventional);
    let (access, file_id) = virtual_file_access(&fixture, &par2_set, dir.path());

    // The length is still the volume's — coverage says what is *there*, not how
    // long the volume is — so a short read is what tells the pass the file is
    // damaged, exactly as a truncated volume file would.
    assert_eq!(
        access.file_length(&file_id),
        Some(fixture.conventional.len() as u64)
    );
    let read = access
        .read_file_range(&file_id, 0, fixture.conventional.len() as u64)
        .expect("a read that starts inside coverage succeeds");
    assert_eq!(
        read.len() as u64,
        hole_at,
        "the read must stop at the hole rather than fabricating the rest"
    );
    assert_eq!(
        read,
        fixture.conventional[..hole_at as usize],
        "and the bytes it did return must be the volume's"
    );
    assert!(
        !par2_rs::verify_full_hash(&par2_set, &file_id, &access).unwrap_or(true),
        "a volume with a hole must not verify"
    );
}

#[test]
fn an_interior_hole_refuses_the_sequential_reader_rather_than_lying_through_it() {
    use std::io::Read;

    let dir = tempfile::tempdir().unwrap();
    // A hole in the middle of member A, with the whole rest of the volume —
    // member A's tail, the envelope gap, member B and the trailer — present.
    let hole_at = (PROVIDER_HEADER + 64) as u64;
    let hole_len = 32u64;
    let total =
        (PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP + PROVIDER_MEMBER_B + PROVIDER_TRAILER)
            as u64;
    let mut covered = ByteRanges::new();
    covered.insert(0, hole_at);
    covered.insert(hole_at + hole_len, total - hole_at - hole_len);
    let fixture = provider_fixture(covered);
    let par2_set = descriptor_only_par2_set("silver.horizon.part01.rar", &fixture.conventional);
    let (access, file_id) = virtual_file_access(&fixture, &par2_set, dir.path());

    // The underlying stream really does stop at the first hole — a `Read` has no
    // way to say "skip 32 bytes, then resume" — so every byte after an interior
    // gap is unreachable through it even though both the partial and the
    // envelope still hold those bytes.
    let provider = super::super::provider::HybridVolumeProvider::new(vec![fixture.volume.clone()]);
    let mut streamed = Vec::new();
    let mut reader = provider.open(0).expect("the volume is registered");
    let _ = reader.read_to_end(&mut streamed);
    assert_eq!(
        streamed.len() as u64,
        hole_at,
        "the sequential sweep stops at the first hole; it cannot skip it"
    );

    // Which is why the adapter does not offer one. An earlier shape did, and
    // every sweep that consumed it — the no-IFSC whole-file MD5, PAR2's batched
    // slice pass — saw a file ending at the first hole and reported every slice
    // after an interior gap damaged, intact bytes included. The earlier shape
    // only produced a verdict, so the cost was a demotion that refetched
    // slightly more than it had to; repair sizes itself from that same count,
    // and a repair sized from "damaged" rather than "absent" spends recovery
    // blocks rebuilding good slices — enough of them and a repairable set reads
    // as unrepairable.
    assert!(
        access
            .open_sequential_reader(&file_id)
            .expect("the volume is registered")
            .is_none(),
        "the adapter must refuse the sequential reader for an interior hole and \
         let the caller fall back to the ranged, per-slice path"
    );
    assert_eq!(access.counters().sequential_refusals(), 1);

    // Addressed directly, the bytes past the gap are all there — which is what
    // makes the paragraph above a property of the *stream*, not of the data.
    let after = access
        .read_file_range(&file_id, hole_at + hole_len, 128)
        .expect("a read that starts inside coverage succeeds");
    assert_eq!(
        after,
        fixture.conventional[(hole_at + hole_len) as usize..(hole_at + hole_len) as usize + 128],
        "the bytes after an interior hole are present and readable when addressed"
    );

    let hole_slice = (hole_at + hole_len) / par2_set.slice_size;
    assert!(
        hole_slice + 1 < total / par2_set.slice_size,
        "the fixture must have slices *after* the interior hole for that claim to bite"
    );
}

#[test]
fn a_write_to_a_virtual_volume_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = provider_fixture(whole_volume_covered());
    let par2_set = descriptor_only_par2_set("silver.horizon.part01.rar", &fixture.conventional);
    let (mut access, file_id) = virtual_file_access(&fixture, &par2_set, dir.path());

    // Repair over a virtual volume is what repair-while-direct adds. It must
    // fail loudly rather than write a recovered slice into a file the set does
    // not own.
    let error = access
        .write_file_range(&file_id, 0, b"repaired")
        .expect_err("a virtual volume has nowhere to put a repaired slice");
    assert!(
        error.to_string().contains("virtual"),
        "the refusal should say why, got {error}"
    );
}

// ---------------------------------------------------------------------------
// Restart primitives — the pieces the end-to-end restart tests exercise only in
// combination, where a wrong answer in one can be masked by another. Two
// defects both hid here.
// ---------------------------------------------------------------------------

#[test]
fn complete_files_names_only_the_files_every_entry_agrees_are_complete() {
    let mut snapshot = sample_snapshot();
    snapshot.floors = vec![
        floor_entry(0, 0, 900, true),
        floor_entry(1, 1, 100, false),
        floor_entry(2, 2, 500, true),
    ];
    assert_eq!(complete_files(&snapshot), HashSet::from([0u32, 2]));

    // A malformed blob repeating a file index is resolved the safe way: one
    // entry saying "not complete" refutes the file, whatever the others claim.
    // Anything else would skip segments on the strength of the entry that
    // happened to be read last.
    snapshot.floors = vec![
        floor_entry(0, 0, 900, true),
        floor_entry(1, 0, 100, false),
        floor_entry(2, 2, 500, true),
    ];
    assert_eq!(complete_files(&snapshot), HashSet::from([2u32]));
}

#[test]
fn coverage_skip_plan_skips_every_segment_of_a_complete_file() {
    let spec = direct_job_spec();
    // The floor is deliberately far below the file: a complete volume's floor
    // counts *decoded* bytes while the spec's segment sizes are yEnc-encoded, so
    // the two never meet and the `complete` bit is the only thing that can say
    // the file is finished.
    let plan = coverage_skip_plan(
        JOB,
        &spec,
        &HashMap::from([(0u32, 30u64)]),
        &HashSet::from([0u32]),
    );

    assert_eq!(
        plan.skip,
        [segment(0), segment(1), segment(2)].into_iter().collect(),
        "a complete file skips every segment, not only the ones under its floor"
    );
    assert_eq!(
        plan.file_progress.get(&0),
        Some(&60),
        "and its progress is the file's whole declared size"
    );

    // Without the bit the very same floor skips only what it covers. This is
    // the difference that turned into a zombie: the bit is worth three segments
    // here and worth the whole job's correctness when it is wrong.
    let plan = coverage_skip_plan(JOB, &spec, &HashMap::from([(0u32, 30u64)]), &HashSet::new());
    assert_eq!(plan.skip.len(), 2);
}

#[tokio::test]
async fn restart_refuses_a_row_claiming_a_volume_the_layout_has_no_facts_for() {
    let temp_dir = tempfile::tempdir().unwrap();
    let roots = sample_roots(temp_dir.path());
    write_destination(
        &roots.destination_dir,
        "silver-horizon.mkv.f0.direct.partial",
        60,
    );
    let blob = encode(&sample_snapshot()).unwrap();

    // The layout rebuild is tolerant of a missing volume — it contributes no
    // member, so the plan digest is unchanged and every other check passes —
    // and the set then cannot classify a byte of it.
    let expected = ExpectedSet {
        fact_volumes: HashSet::new(),
        ..sample_expected()
    };
    assert_eq!(
        restore_set(&roots, &blob, &expected).await,
        Err(CoverageRejection::UnclassifiableVolume { volume_index: 0 }),
        "a row claiming coverage in a volume with no cached facts must be refused, not \
         accepted into a set that can never place those bytes"
    );

    // A volume the row claims *nothing* in is not a reason to retire the row:
    // refusing on it would redownload sets whose later volumes had simply not
    // started.
    let mut unstarted = sample_snapshot();
    unstarted.floors = vec![floor_entry(0, 0, 0, false)];
    let blob = encode(&unstarted).unwrap();
    assert!(
        restore_set(&roots, &blob, &expected).await.is_ok(),
        "a zero floor with no completion claims nothing and must not refuse the row"
    );
}

#[test]
fn a_restored_volume_is_confirmed_only_by_a_proof_it_actually_has() {
    let mut whole = ByteRanges::new();
    whole.insert(0, 1_000);
    let mut short = ByteRanges::new();
    short.insert(0, 400);
    let mut gapped = ByteRanges::new();
    gapped.insert(0, 400);
    gapped.insert(600, 400);

    // Proof one: the cached facts carry an end-of-archive record saying more
    // volumes follow, which is the last header this volume can hold.
    assert!(restored_volume_is_confirmed(&short, None, true));

    // Proof two: the checkpoint calls the volume complete *and* the coverage in
    // front of us is contiguous to its whole decoded length.
    assert!(restored_volume_is_confirmed(&whole, Some(1_000), false));

    // The claim is checked, not taken. A row whose `complete` bit disagrees
    // with its own coverage — an older writer's latched bit, a torn row —
    // proves nothing.
    assert!(!restored_volume_is_confirmed(&short, Some(1_000), false));
    assert!(!restored_volume_is_confirmed(&gapped, Some(1_000), false));

    // And neither proof means unconfirmed, which is what the completion seam
    // turns into an explicit demotion rather than a silently held tail.
    assert!(!restored_volume_is_confirmed(&short, None, false));
    assert!(!restored_volume_is_confirmed(&whole, None, false));
}

// ---------------------------------------------------------------------------
// The restart gate re-arm, over a router rebuilt from cached facts
// ---------------------------------------------------------------------------

#[test]
fn the_restart_read_plan_splits_a_members_coverage_at_its_part_boundaries() {
    let plan = rearm_router().restart_read_plan();

    assert_eq!(
        plan.iter()
            .map(|run| (run.logical_offset, run.len))
            .collect::<Vec<_>>(),
        vec![(0, REARM_PART), (REARM_PART, REARM_PART)],
        "one seeded range spanning two parts must be read as two runs: the composition \
         the re-read feeds is per part, so a run straddling a boundary composes against \
         no reference at all"
    );
    assert!(
        plan.iter()
            .all(|run| run.relative_partial.ends_with(".direct.partial")),
        "every run names the partial that holds it, so the reader opens each file once"
    );
}

#[test]
fn a_rearm_run_the_layout_cannot_place_demotes_instead_of_failing_open() {
    let mut router = rearm_router();

    // A member id the layout does not have. Returning `Ok(())` here — which it
    // used to — leaves the seeded range in place, so `try_verify_member`
    // refuses the member forever while the completion gate re-reads it on every
    // check: a set that neither finalizes nor demotes.
    assert_eq!(
        router.note_restored_member_crc(4242, 0, REARM_PART, 0x1111_1111),
        Err(DemotionReason::RestartRearmUnplaceable),
    );

    // Same verdict for an offset no part of a real member covers.
    let mut router = rearm_router();
    assert_eq!(
        router.note_restored_member_crc(0, REARM_PART * 9, REARM_PART, 0x1111_1111),
        Err(DemotionReason::RestartRearmUnplaceable),
    );
}

#[test]
fn a_rearm_run_that_disagrees_with_its_parts_checksum_demotes() {
    let mut router = rearm_router();
    assert_eq!(
        router.note_restored_member_crc(0, 0, REARM_PART, 0xDEAD_BEEF),
        Err(DemotionReason::PartChecksumMismatch),
        "a byte that changed on disk while the process was down must fail here, which is \
         the whole point of re-reading rather than trusting the checkpoint"
    );
}

#[test]
fn a_rearm_run_that_matches_clears_its_seeded_range() {
    let mut router = rearm_router();
    assert!(router.has_restart_seeded_coverage());
    router
        .note_restored_member_crc(0, 0, REARM_PART, 0x1111_1111)
        .expect("the first part composes to its header's packed CRC32");
    assert!(
        router.has_restart_seeded_coverage(),
        "the member's second part is still seeded"
    );
    assert_eq!(
        router.restart_read_plan().len(),
        1,
        "and a second pass reads only what the first did not clear"
    );
}

// ---------------------------------------------------------------------------
// The holds scratch and its region index
// ---------------------------------------------------------------------------

#[test]
fn holds_scratch_hands_out_stable_regions_and_reads_them_back() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(".weaver-holds.silver.horizon.f0");
    let mut scratch = HoldsScratch::new(path.clone(), 1024);

    let first = scratch.append(b"header bytes").unwrap();
    let second = scratch.append(b"payload").unwrap();
    assert_eq!(
        (first, second),
        (0, "header bytes".len() as u64),
        "regions are handed out as append offsets, and the index that names them is the \
         offset itself"
    );
    assert_eq!(
        scratch.bytes(),
        ("header bytes".len() + "payload".len()) as u64
    );
    assert_eq!(
        scratch.read(second, "payload".len() as u64).as_deref(),
        Some(&b"payload"[..]),
        "a region reads back exactly what was appended at it, positionally, so a later \
         append cannot disturb it"
    );
    assert_eq!(
        scratch.read(first, "header bytes".len() as u64).as_deref(),
        Some(&b"header bytes"[..]),
        "including one written before it — the file is write-once per region"
    );

    scratch.discard();
    assert!(!path.exists(), "discard deletes the file it created");
    assert_eq!(scratch.bytes(), 0);
}

#[test]
fn holds_scratch_refuses_an_append_past_its_ceiling() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(".weaver-holds.silver.horizon.f0");
    let mut scratch = HoldsScratch::new(path.clone(), 8);

    scratch.append(b"12345").unwrap();
    assert_eq!(
        scratch.append(b"678901"),
        Err(DemotionReason::HoldsScratchCeiling),
        "the ceiling is checked before the write, so a breach costs nothing on disk"
    );
    assert_eq!(
        scratch.bytes(),
        5,
        "and leaves the append cursor where it was"
    );
}

#[test]
fn holds_scratch_compaction_reclaims_what_placed_holds_left_behind() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(".weaver-holds.silver.horizon.f0");
    let mut scratch = HoldsScratch::new(path, 64);

    let first = scratch.append(b"aaaa").unwrap();
    let _placed = scratch.append(b"bbbbbb").unwrap();
    let third = scratch.append(b"cccc").unwrap();
    assert_eq!(scratch.bytes(), 14);

    // The middle run was routed and placed, so nothing points at it any more.
    let new_offsets = scratch.compact(&[(first, 4), (third, 4)]).unwrap();

    assert_eq!(
        new_offsets,
        vec![0, 4],
        "the survivors are packed to the front in the order they were given"
    );
    assert_eq!(
        scratch.bytes(),
        8,
        "and the append cursor drops to what is actually live, which is the whole point"
    );
    assert_eq!(scratch.read(0, 4).as_deref(), Some(&b"aaaa"[..]));
    assert_eq!(
        scratch.read(4, 4).as_deref(),
        Some(&b"cccc"[..]),
        "a moved run reads back byte-identically at its new offset"
    );
}

#[test]
fn holds_scratch_compaction_moves_runs_larger_than_one_copy_slice() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(".weaver-holds.silver.horizon.f0");
    let live: Vec<u8> = (0..3_000_000u32).map(|index| (index % 251) as u8).collect();
    let mut scratch = HoldsScratch::new(path, 8 * 1024 * 1024);

    scratch.append(b"dead").unwrap();
    let survivor = scratch.append(&live).unwrap();

    let new_offsets = scratch.compact(&[(survivor, live.len() as u64)]).unwrap();

    assert_eq!(new_offsets, vec![0]);
    assert_eq!(scratch.bytes(), live.len() as u64);
    assert_eq!(
        scratch.read(0, live.len() as u64).as_deref(),
        Some(live.as_slice()),
        "a run that crosses several copy slices survives the move intact, and the copy \
         runs front to back so the overlapping source is never clobbered"
    );
}

#[test]
fn holds_scratch_compaction_refuses_extents_it_cannot_pack_safely() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(".weaver-holds.silver.horizon.f0");
    let mut scratch = HoldsScratch::new(path, 64);
    scratch.append(b"aaaabbbb").unwrap();

    assert_eq!(
        scratch.compact(&[(4, 4), (0, 4)]),
        None,
        "out-of-order extents would have the pack overwrite a source it has not read"
    );
}

/// A provider that reads holds from the scratch on demand keeps offsets past
/// the call that handed them out, and compaction is the one thing that moves
/// a region. Under a pin it packs into a fresh image instead, so the pinned
/// reader's offsets stay true for the image it holds while the router goes on
/// with the packed copy at the same path.
#[test]
fn a_pinned_scratch_compacts_into_a_fresh_image_and_the_pin_keeps_the_old_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(".weaver-holds.silver.horizon.f0");
    let packing_path = dir
        .path()
        .join(".weaver-holds.silver.horizon.f0.compacting");
    let mut scratch = HoldsScratch::new(path.clone(), 64);

    let first = scratch.append(b"aaaa").unwrap();
    let placed = scratch.append(b"bbbbbb").unwrap();
    let third = scratch.append(b"cccc").unwrap();

    let pin = scratch.pin().expect("an image exists once a byte is paged");
    assert!(scratch.is_pinned());

    let new_offsets = scratch.compact(&[(first, 4), (third, 4)]).unwrap();
    assert_eq!(new_offsets, vec![0, 4]);
    assert_eq!(scratch.bytes(), 8);
    assert_eq!(
        scratch.read(4, 4).as_deref(),
        Some(&b"cccc"[..]),
        "the router reads the packed copy at the new offsets"
    );
    assert!(path.exists(), "the packed copy took over the scratch path");
    assert!(
        !packing_path.exists(),
        "and the packing path did not linger"
    );
    assert!(
        !scratch.is_pinned(),
        "the fresh image starts unpinned: the pin is on the image it was taken against"
    );

    let mut moved = [0u8; 4];
    pin.read_at(third, &mut moved).unwrap();
    assert_eq!(
        &moved, b"cccc",
        "the pinned reader still reads the run at the offset it was handed"
    );
    let mut dead = [0u8; 6];
    pin.read_at(placed, &mut dead).unwrap();
    assert_eq!(
        &dead, b"bbbbbb",
        "the old image was left exactly as it was, dead regions included"
    );

    // Unpinned again, the next pack is in place and the offsets keep working.
    drop(pin);
    let fourth = scratch.append(b"dddd").unwrap();
    let packed_again = scratch.compact(&[(4, 4), (fourth, 4)]).unwrap();
    assert_eq!(packed_again, vec![0, 4]);
    assert_eq!(scratch.read(0, 8).as_deref(), Some(&b"ccccdddd"[..]));
}

#[test]
fn dropping_the_last_pin_unpins_the_scratch() {
    let dir = tempfile::tempdir().unwrap();
    let mut scratch = HoldsScratch::new(dir.path().join(".weaver-holds.silver.horizon.f0"), 64);
    assert!(
        scratch.pin().is_none(),
        "there is no image to pin before anything is paged"
    );
    scratch.append(b"held").unwrap();

    let first = scratch.pin().unwrap();
    let second = scratch.pin().unwrap();
    assert!(scratch.is_pinned());
    drop(first);
    assert!(scratch.is_pinned(), "one reader is still on the image");
    drop(second);
    assert!(!scratch.is_pinned());
}

/// A retained provider can outlive the set: finalization and demotion both
/// discard the scratch, and a pin must keep its bytes readable through that.
/// The handle does the keeping; the path is gone. Unix only, where an unlinked
/// file stays readable through an open handle by contract.
#[cfg(unix)]
#[test]
fn a_pin_reads_through_a_discard() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(".weaver-holds.silver.horizon.f0");
    let mut scratch = HoldsScratch::new(path.clone(), 64);
    let offset = scratch.append(b"held bytes").unwrap();
    let pin = scratch.pin().unwrap();

    scratch.discard();
    assert!(!path.exists());

    let mut out = [0u8; 10];
    pin.read_at(offset, &mut out).unwrap();
    assert_eq!(&out, b"held bytes");
}

#[test]
fn a_scratch_that_never_appended_a_byte_leaves_nothing_behind() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(".weaver-holds.silver.horizon.f0");

    // Never opened at all: nothing to delete, and nothing to blame for a
    // neighbouring file that happens to share the path.
    let mut scratch = HoldsScratch::new(path.clone(), 1024);
    std::fs::write(&path, b"someone else's file").unwrap();
    scratch.discard();
    assert!(
        path.exists(),
        "a scratch that never opened its file must not delete whatever is at that path"
    );
    std::fs::remove_file(&path).unwrap();

    // Opened and appended: deleted, cursor reset, and idempotent.
    let mut scratch = HoldsScratch::new(path.clone(), 1024);
    scratch.append(b"held").unwrap();
    assert!(path.exists());
    scratch.discard();
    scratch.discard();
    assert!(!path.exists());
}

// ---------------------------------------------------------------------------
// Damage accounting over an interior hole
// ---------------------------------------------------------------------------

#[test]
fn an_interior_hole_damages_only_the_slices_it_touches() {
    // The wave-2 review note, as a test. The sequential sweep
    // `verify_slices_batched_md5` prefers stops at the first hole and reports
    // every slice after it damaged — on this fixture that is 5 slices instead of
    // 2 — and the repair those numbers size rebuilds three healthy slices with
    // recovery blocks it did not need to spend.
    let dir = tempfile::tempdir().unwrap();
    let hole = (100u64, 180u64);
    let fixture = provider_fixture(covered_with_interior_hole(hole.0, hole.1));
    let par2_set = sliced_par2_set(
        "silver.horizon.part01.rar",
        &fixture.conventional,
        HOLE_SLICE_SIZE,
    );
    let (access, file_id) = virtual_file_access(&fixture, &par2_set, dir.path());
    let counters = access.counters();

    let valid = par2_rs::verify_slices(&par2_set, &file_id, &access)
        .expect("the description names a registered virtual volume");
    let damaged = damaged_slice_indices(&valid);

    // Only the slices the hole actually overlaps: `[100, 180)` at a 64-byte
    // slice size is slices 1 and 2.
    let expected: Vec<usize> = (0..valid.len())
        .filter(|index| {
            let start = *index as u64 * HOLE_SLICE_SIZE;
            let end = start + HOLE_SLICE_SIZE;
            start < hole.1 && hole.0 < end
        })
        .collect();
    assert_eq!(
        damaged, expected,
        "an interior hole must damage only the slices that touch it; a count \
         inflated by the sequential sweep sizes the repair from healthy slices \
         and can flip repairable to unrepairable"
    );
    assert!(
        counters.sequential_refusals() > 0,
        "the adapter must refuse the sequential reader for a volume with an \
         interior hole — that refusal is what makes the count above accurate"
    );
    assert!(
        damaged.len() < valid.len() - 1,
        "non-vacuity: the fixture must have healthy slices *after* the hole, or \
         the accounting fix would be untestable here"
    );
}

#[test]
fn interior_hole_verdicts_match_a_physically_sparse_volume() {
    // Verdict parity is the acceptance rule for the seam choice: whatever the
    // adapter answers, it must be what the same job would have concluded with
    // the gate off, where the missing articles leave a sparse file with real
    // zeros in the hole.
    let dir = tempfile::tempdir().unwrap();
    let hole = (100u64, 180u64);
    let fixture = provider_fixture(covered_with_interior_hole(hole.0, hole.1));
    let filename = "silver.horizon.part01.rar";
    let par2_set = sliced_par2_set(filename, &fixture.conventional, HOLE_SLICE_SIZE);
    let (access, file_id) = virtual_file_access(&fixture, &par2_set, dir.path());
    let virtual_valid =
        par2_rs::verify_slices(&par2_set, &file_id, &access).expect("the virtual volume verifies");

    // The same volume as the conventional path would have left it: written at
    // its offsets, with a filesystem hole where the articles never arrived.
    let physical_dir = tempfile::tempdir().unwrap();
    let path = physical_dir.path().join(filename);
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(&fixture.conventional[..hole.0 as usize])
            .unwrap();
        file.seek(SeekFrom::Start(hole.1)).unwrap();
        file.write_all(&fixture.conventional[hole.1 as usize..])
            .unwrap();
    }
    let physical = par2_rs::PlacementFileAccess::new(
        physical_dir.path().to_path_buf(),
        &par2_set,
        HashMap::new(),
    );
    let physical_valid =
        par2_rs::verify_slices(&par2_set, &file_id, &physical).expect("the file verifies");

    assert_eq!(
        virtual_valid, physical_valid,
        "a direct set's verdict must be the verdict the same damage produces on \
         a real sparse volume, slice for slice"
    );
}

#[test]
fn a_truncated_volume_still_takes_the_sequential_path() {
    // The refusal is scoped to *interior* holes on purpose. A volume covered
    // from zero and stopping short reads exactly like a truncated file, which
    // is what a sequential sweep already reports correctly — so the fast path
    // requires survives for every shape except the one that lies.
    let dir = tempfile::tempdir().unwrap();
    let mut covered = ByteRanges::new();
    covered.insert(0, 200);
    let fixture = provider_fixture(covered);
    let par2_set = sliced_par2_set(
        "silver.horizon.part01.rar",
        &fixture.conventional,
        HOLE_SLICE_SIZE,
    );
    let (access, file_id) = virtual_file_access(&fixture, &par2_set, dir.path());
    let counters = access.counters();

    let valid = par2_rs::verify_slices(&par2_set, &file_id, &access)
        .expect("the truncated volume verifies");
    assert_eq!(
        damaged_slice_indices(&valid),
        (3..valid.len()).collect::<Vec<_>>(),
        "a volume covered to byte 200 has three whole 64-byte slices and nothing else"
    );
    assert_eq!(
        counters.sequential_refusals(),
        0,
        "a prefix-readable volume must keep the sequential reader"
    );
    assert!(counters.sequential_opens() > 0);
}

#[test]
fn readable_prefix_reports_the_shape_the_reader_can_answer() {
    let whole = provider_fixture(whole_volume_covered());
    assert_eq!(
        whole.volume.readable_prefix(),
        Some(whole.conventional.len() as u64),
        "a fully covered volume reads end to end"
    );
    assert!(!whole.volume.has_interior_hole());

    let holed = provider_fixture(covered_with_interior_hole(100, 180));
    assert_eq!(holed.volume.readable_prefix(), None);
    assert!(holed.volume.has_interior_hole());

    // Covered, but by no source that holds the bytes: the extents that said the
    // members owned them are gone (the ineligible-member case), so those ranges
    // are holes however loudly the volume-level map claims coverage.
    let unbacked = provider_fixture_with_extents(whole_volume_covered(), false);
    let member_a_at = PROVIDER_HEADER as u64;
    let member_b_at = (PROVIDER_HEADER + PROVIDER_MEMBER_A + PROVIDER_GAP) as u64;
    assert_eq!(
        unbacked.volume.readable_ranges(),
        vec![
            (0, member_a_at),
            (
                member_a_at + PROVIDER_MEMBER_A as u64,
                member_a_at + PROVIDER_MEMBER_A as u64 + PROVIDER_GAP as u64
            ),
            (
                member_b_at + PROVIDER_MEMBER_B as u64,
                member_b_at + PROVIDER_MEMBER_B as u64 + PROVIDER_TRAILER as u64
            ),
        ],
        "the readable image is what a *source* holds, never what the volume map \
         claims — a byte with no source is a hole, which is the invariant that \
         keeps fabricated zeros out of a reconstruction"
    );
    assert!(
        unbacked.volume.has_interior_hole(),
        "and the shape query says so, so nothing offers a stream over it"
    );
}

// ---------------------------------------------------------------------------
// CRC composition under repair
// ---------------------------------------------------------------------------

#[test]
fn overwrite_replaces_a_run_and_reports_no_gap_when_it_lines_up() {
    let mut runs = CrcRuns::default();
    runs.insert(0, 100, 0x1111_1111);
    runs.insert(100, 100, 0x2222_2222);

    let gaps = runs.overwrite(100, 100, 0x3333_3333);
    assert!(
        gaps.is_empty(),
        "a rewrite that covers whole runs leaves nothing composed by nothing"
    );
    assert_eq!(
        runs.compose(0, 200),
        Some(par2_rs::checksum::Crc32CombineOp::new(100).combine(0x1111_1111, 0x3333_3333)),
        "the composition must carry the repaired value, not the value the \
         damaged bytes produced"
    );
}

#[test]
fn overwrite_leaves_the_uncovered_edges_as_stale_gaps() {
    let mut runs = CrcRuns::default();
    runs.insert(0, 100, 0x1111_1111);

    // A slice-shaped rewrite inside one article-shaped run: the article's value
    // describes bytes that no longer exist, and the bytes on either side of the
    // rewrite are left vouched for by nothing.
    let gaps = runs.overwrite(40, 20, 0x4444_4444);
    assert_eq!(
        gaps,
        vec![(0, 40), (60, 100)],
        "both edges of the discarded run become stale gaps"
    );
    assert_eq!(
        runs.compose(0, 100),
        None,
        "and until they are re-read the range composes to nothing, which every \
         caller treats as refuse rather than pass"
    );

    // Closing them is what re-arms the composition.
    let head = runs.overwrite(0, 40, 0x5555_5555);
    let tail = runs.overwrite(60, 40, 0x6666_6666);
    assert!(head.is_empty() && tail.is_empty());
    let expected = par2_rs::checksum::Crc32CombineOp::new(20).combine(0x5555_5555, 0x4444_4444);
    let expected = par2_rs::checksum::Crc32CombineOp::new(40).combine(expected, 0x6666_6666);
    assert_eq!(runs.compose(0, 100), Some(expected));
}

#[test]
fn insert_still_clips_a_duplicate_after_a_repair_overwrote_the_same_run() {
    // The distinction repair turns on. A repair moved the bytes, so the
    // composition moved with them; a duplicate article carrying the *old* bytes
    // must not move it back, and `insert`'s overlap refusal is what stops it.
    let mut runs = CrcRuns::default();
    runs.insert(0, 100, 0xDEAD_BEEF);
    runs.overwrite(0, 100, 0xFEED_FACE);

    runs.insert(0, 100, 0xDEAD_BEEF);
    assert_eq!(
        runs.compose(0, 100),
        Some(0xFEED_FACE),
        "a duplicate must clip against the repaired run, never replace it"
    );
}

#[test]
fn a_drain_run_straddling_repaired_and_duplicate_bytes_splits_at_the_boundary() {
    // The load-bearing gap. The drain's `replace` flag is all-or-nothing per
    // emitted run, and the two things that fix a run's extent do it for
    // unrelated reasons: `map_physical_range` splits at member boundaries, and
    // `pending` coalesces everything that abuts. So a repair routinely produces
    // **one** member run covering repaired and unrepaired bytes together — and
    // that run used to take `replace = false`, which makes `CrcRuns::insert`
    // refuse it as overlapping. The bytes reach the partial correctly and the
    // composition keeps describing the wire-damaged ones, so the member fails a
    // gate it should pass and the set demotes, throwing away a repair that
    // worked.
    const HEADER: u64 = 64;
    const MEMBER: u64 = 400;
    let damaged: Vec<u8> = (0..MEMBER as u32)
        .map(|index| (index % 251) as u8)
        .collect();
    let mut repaired = damaged.clone();
    // The repair rewrites the second half and fills a tail the set never had.
    for (index, byte) in repaired.iter_mut().enumerate().skip(200) {
        *byte = ((index * 7 + 3) % 256) as u8;
    }

    let (mut router, member_id) = straddle_router(&repaired, HEADER);

    // The set's own download: the member's first 300 bytes, damaged. Short of
    // the member, so neither gate fires yet and the set-up cannot demote.
    router.stage_for_test(0, HEADER, &damaged[..300]);
    router.drain_for_test(0).expect("the first drain routes");

    // Now the shape. `[100, 200)` re-enters as an ordinary duplicate — bytes the
    // router staged and could not place, which is the only way unrepaired bytes
    // sit next to repaired ones in one pending run — and `[200, 400)` as the
    // repair. `pending` coalesces the two into `[100, 400)`, and the layout maps
    // that as a single member run.
    router.force_stage_for_test(0, HEADER + 100, &damaged[100..200], false);
    router.force_stage_for_test(0, HEADER + 200, &repaired[200..], true);
    let spans = router
        .drain_for_test(0)
        .expect("the straddling drain routes");
    assert_eq!(
        spans.iter().map(|span| span.bytes.len()).sum::<usize>(),
        300,
        "both halves must still reach the member's partial — the bug was never \
         about the bytes, only about what the composition then claims about them"
    );

    // The repaired sub-range **overwrote**: that is what discards the article's
    // old value and leaves its uncovered head as a stale gap. Without the split
    // there is no overwrite, so there is no gap either — and nothing to re-read,
    // which is how the damaged value survived in silence.
    assert!(
        router.has_stale_gaps(),
        "the repaired sub-range must have overwritten the composition, which is \
         what opens the stale gap the subsequent re-read then closes"
    );
    assert_eq!(
        router
            .stale_gap_read_plan()
            .iter()
            .map(|run| (run.member_id, run.logical_offset, run.len))
            .collect::<Vec<_>>(),
        vec![(member_id, 0, 200)],
        "and exactly the head of the discarded article is stale: the duplicate \
         sub-range clipped, so it neither re-inserted its own value nor widened \
         the gap"
    );

    // Closing the gap the way `reread_direct_stale_gaps` does. It composes to
    // the repaired image, so the whole-member gate passes — the repair survived.
    router
        .note_restored_member_crc(
            member_id,
            0,
            200,
            par2_rs::checksum::crc32(&repaired[..200]),
        )
        .expect("the re-read closes the gap");
    assert!(
        !router.has_stale_gaps(),
        "one pass over the plan closes every gap it named"
    );
    assert!(
        router.all_members_verified(),
        "and the member must verify against the *repaired* whole-member CRC32; \
         a composition still carrying the damaged run demotes a set whose bytes \
         on disk are correct"
    );
}

// ---------------------------------------------------------------------------
// The ordering rule, and what a crash in its window costs
// ---------------------------------------------------------------------------

#[test]
fn deleting_the_row_before_a_repair_keeps_the_coverage_the_provider_reads() {
    // The ordering rule, and the distinction that makes it survivable. The row
    // has to go *before* a repair rewrites the bytes it claims — otherwise a
    // crash mid-repair leaves floors over half-rewritten data. What must
    // **not** go with it is the controller: a repair leaves every destination
    // in place, at the same offsets, holding better bytes, so its account of
    // what is on disk is still exactly right — and it is also what the hybrid
    // provider reads to answer the re-verify that follows.
    let recorder = Recorder::default();
    let mut barrier = sample_barrier();
    barrier.register_volume(0, 0);
    barrier.register_destination(0, "Silver.Horizon.S01E04.mkv.f0.direct.partial");
    barrier
        .record_write(&write(0, 0, 4096, 0), Instant::now())
        .unwrap();
    run_barrier(
        &mut barrier,
        &recorder,
        BarrierTrigger::Demand(BarrierDemand::Pause),
    )
    .unwrap();
    assert_eq!(barrier.generation(), 1);
    let covered_before = barrier.volume_coverage(0);
    assert_eq!(covered_before.as_ref().map(ByteRanges::covered), Some(4096));

    barrier.delete_committed_row(&mut recorder.clone()).unwrap();
    assert_eq!(recorder.deletes(), 1, "the durable row is gone");
    assert_eq!(
        barrier.volume_coverage(0),
        covered_before,
        "and the in-memory coverage is untouched, so the re-verify still reads a \
         volume rather than a hole"
    );
    assert_eq!(
        barrier.destination_coverage(0).map(ByteRanges::covered),
        Some(4096),
        "the destination claim survives too — it is what says the envelope or \
         partial really received a byte, which no volume-level map can answer"
    );

    // And the next barrier writes a fresh row rather than an increment of one
    // that no longer exists.
    barrier
        .record_write(&write(0, 4096, 4096, 0), Instant::now())
        .unwrap();
    run_barrier(
        &mut barrier,
        &recorder,
        BarrierTrigger::Demand(BarrierDemand::RepairRecreate),
    )
    .unwrap();
    assert_eq!(
        barrier.generation(),
        1,
        "the generation restarts from zero, because the row it would have \
         incremented was deleted"
    );
}

#[test]
fn a_crash_between_the_row_delete_and_the_next_barrier_leaves_nothing_to_trust() {
    // The deliberately lossy half, stated as a test. Between the delete and the
    // barrier that recreates coverage there is no row at all, so a restart in
    // that window finds nothing, claims nothing, and redownloads the set —
    // which is the bounded cost reconstruction accepts in exchange for not
    // having to selectively lower per-volume floors around the repaired ranges.
    let recorder = Recorder::default();
    let mut barrier = sample_barrier();
    barrier.register_volume(0, 0);
    barrier.register_destination(0, "Silver.Horizon.S01E04.mkv.f0.direct.partial");
    barrier
        .record_write(&write(0, 0, 4096, 0), Instant::now())
        .unwrap();
    run_barrier(
        &mut barrier,
        &recorder,
        BarrierTrigger::Demand(BarrierDemand::Pause),
    )
    .unwrap();
    assert!(recorder.committed().is_some());

    barrier.delete_committed_row(&mut recorder.clone()).unwrap();
    assert!(
        recorder.committed().is_none(),
        "nothing durable survives the delete, so a restart here has no floors to \
         trust and refetches the set"
    );
}

// ---------------------------------------------------------------------------
// What the repair is sized from
// ---------------------------------------------------------------------------

#[test]
fn damaged_ranges_name_only_the_slices_par2_called_invalid() {
    use super::super::repair::damaged_ranges;

    // Slices 1 and 4 damaged, on a file whose last slice is short.
    let valid = [true, false, true, true, false];
    assert_eq!(
        damaged_ranges(&valid, 64, 300),
        vec![(64, 128), (256, 300)],
        "the repair is sized from the per-slice verdict, and the tail slice is \
         clipped to the file rather than to the slice size"
    );
    assert_eq!(
        damaged_ranges(&[true, true], 64, 128),
        Vec::new(),
        "a clean file contributes nothing to rewrite"
    );
    assert_eq!(
        damaged_ranges(&[false, false, false], 64, 192),
        vec![(0, 192)],
        "adjacent damaged slices coalesce into one rewrite"
    );
}

#[test]
fn a_rewrite_widens_to_whole_articles_so_the_volume_composition_stays_exact() {
    use super::super::repair::widen_to_articles;

    // Three articles of 100 bytes. A 64-byte slice-shaped rewrite at 120 cuts
    // the second one in half.
    let extents = std::collections::BTreeMap::from([
        (0u32, (0u64, 100u64)),
        (1, (100, 100)),
        (2, (200, 100)),
    ]);
    assert_eq!(
        widen_to_articles(&[(120, 184)], &extents, 300),
        vec![(100, 200)],
        "the rewrite is read back as whole articles, so the volume's yEnc \
         composition is replaced run for run and leaves no stale gap"
    );
    assert_eq!(
        widen_to_articles(&[(0, 300)], &extents, 300),
        vec![(0, 300)],
        "a rewrite that already covers whole articles is unchanged"
    );
    // A range no article ever reached — the set never received a byte of it —
    // has no run to half-cover, so widening it would only read bytes nothing
    // asked for.
    let sparse = std::collections::BTreeMap::from([(0u32, (0u64, 100u64))]);
    assert_eq!(
        widen_to_articles(&[(200, 264)], &sparse, 300),
        vec![(200, 264)]
    );
}

#[test]
fn whole_volume_rewrite_requires_actual_end_to_end_ranges() {
    use super::super::reconstruct::{PartialArticle, VolumeReconstruction};
    use super::super::repair::DamagedDirectVolume;

    let mut volume = DamagedDirectVolume {
        volume_index: 0,
        par2_file_id: par2_rs::FileId::from_bytes([9; 16]),
        len: 300,
        path: std::path::PathBuf::new(),
        rewrite: Vec::new(),
        reconstruction: VolumeReconstruction {
            volume_index: 0,
            path: std::path::PathBuf::new(),
            len: 300,
            assembly_complete: false,
            covered: ByteRanges::new(),
            crcs: CrcRuns::default(),
            partial_article: PartialArticle::CarryThrough,
        },
    };
    for ranges in [
        vec![(0, 300)],
        vec![(0, 100), (100, 200), (200, 300)],
        vec![(0, 150), (100, 300)],
    ] {
        volume.rewrite = ranges;
        assert!(volume.rewrote_whole_volume(), "{:?}", volume.rewrite);
    }
    for ranges in [
        vec![],
        vec![(0, 100), (100, 200)], // Missing tail; 200 is an end, not a length.
        vec![(0, 100), (50, 100), (140, 300)], // An overlap cannot cover a hole.
        vec![(1, 300)],
        vec![(0, 200), (150, 300), (100, 200)], // Unsorted.
        vec![(0, 300), (200, 100)],             // Reversed.
        vec![(0, 0), (0, 300)],
        vec![(0, 301)],
        vec![(0, u64::MAX)],
    ] {
        volume.rewrite = ranges;
        assert!(!volume.rewrote_whole_volume(), "{:?}", volume.rewrite);
    }
    volume.len = 0;
    volume.rewrite = vec![(0, 0)];
    assert!(!volume.rewrote_whole_volume());
}

#[test]
fn an_encrypted_sets_read_back_carries_the_posted_bytes_on_both_sides_of_a_span() {
    use super::super::reconstruct::{PartialArticle, VolumeReconstruction};
    use super::super::repair::{DamagedDirectVolume, read_repaired_spans};

    // A repaired volume on disk, and one rewrite span with posted bytes on
    // either side of it.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("silver.horizon.part01.rar");
    let image: Vec<u8> = (0..400u32).map(|index| (index % 251) as u8).collect();
    std::fs::write(&path, &image).unwrap();
    let volume = DamagedDirectVolume {
        volume_index: 0,
        par2_file_id: par2_rs::FileId::from_bytes([9u8; 16]),
        len: image.len() as u64,
        path: path.clone(),
        // `(start, end)`, so the span is 120 bytes at 100.
        rewrite: vec![(100, 220)],
        reconstruction: VolumeReconstruction {
            volume_index: 0,
            path,
            len: image.len() as u64,
            assembly_complete: true,
            covered: ByteRanges::new(),
            crcs: CrcRuns::default(),
            partial_article: PartialArticle::CarryThrough,
        },
    };

    // A plaintext set asks for neither edge: its spans decrypt nothing, so the
    // bytes around them buy it nothing and reading them would be pure I/O.
    let plain = read_repaired_spans(&volume, false).expect("the read-back succeeds");
    assert_eq!(plain.len(), 1);
    assert!(plain[0].lead_in.is_none() && plain[0].lead_out.is_none());

    // An encrypted set asks for both. The span's first byte and its last both
    // sit inside a cipher block whose other half is not in the span, and the
    // drain can decrypt neither block without it — the low half comes from the
    // article below, the high half from the article above, and both were routed
    // and dropped from staging long before the repair ran.
    let encrypted = read_repaired_spans(&volume, true).expect("the read-back succeeds");
    let span = &encrypted[0];
    assert_eq!(
        span.lead_in
            .as_ref()
            .map(|(offset, bytes)| (*offset, bytes.to_vec())),
        Some((68, image[68..100].to_vec())),
        "the CBC predecessor of the span's first block, and that block's own \
         lower half"
    );
    assert_eq!(
        span.lead_out
            .as_ref()
            .map(|(offset, bytes)| (*offset, bytes.to_vec())),
        Some((220, image[220..236].to_vec())),
        "and the upper half of the block the span's last byte lands in"
    );
    assert_eq!(
        span.len, 120,
        "neither edge is part of the span: they did not change, so they must \
         not rewrite the volume composition"
    );
}

#[test]
fn repaired_ranges_stream_exact_posted_bytes_and_cipher_edges() {
    use super::super::repair::read_repaired_range;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("repaired-volume");
    let image: Vec<u8> = (0..700_001).map(|index| (index % 251) as u8).collect();
    std::fs::write(&path, &image).unwrap();
    let len = image.len() as u64;
    let mut combined = 0;
    // Deliberately unaligned: the reader must return both halves of a CBC
    // boundary without adding either edge to the stripe's CRC or coverage.
    for start in (0..len).step_by(65_537) {
        let end = (start + 65_537).min(len);
        let span = read_repaired_range(&path, 7, len, start..end, true)
            .unwrap()
            .unwrap();
        assert_eq!(span.volume_index, 7);
        assert_eq!(span.source_offset, start);
        assert_eq!(span.len, end - start);
        assert_eq!(span.chunks.len(), 1);
        assert_eq!(span.chunks[0].0, start);
        assert_eq!(
            span.chunks[0].1.as_ref(),
            &image[start as usize..end as usize]
        );
        let mut retained = span.chunks[0].1.len();
        if start == 0 {
            assert!(span.lead_in.is_none());
        } else {
            let (offset, bytes) = span.lead_in.as_ref().unwrap();
            assert_eq!(*offset, start.saturating_sub(32));
            assert_eq!(bytes.as_ref(), &image[*offset as usize..start as usize]);
            retained += bytes.len();
        }
        if end == len {
            assert!(span.lead_out.is_none());
        } else {
            let (offset, bytes) = span.lead_out.as_ref().unwrap();
            assert_eq!(*offset, end);
            assert_eq!(
                bytes.as_ref(),
                &image[end as usize..(end + 16).min(len) as usize]
            );
            retained += bytes.len();
        }
        assert!(retained <= 65_537 + 48);
        combined = weaver_yenc::crc32_combine(combined, span.crc32, span.len);
    }
    assert_eq!(combined, par2_rs::checksum::crc32(&image));
    assert!(
        read_repaired_range(&path, 7, len, len..len, true)
            .unwrap()
            .is_none()
    );
}

#[test]
fn repaired_range_refuses_invalid_geometry_and_preserves_io_errors() {
    use super::super::repair::read_repaired_range;
    use std::io::ErrorKind;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("repaired-volume");
    let missing = read_repaired_range(&path, 0, 100, 0..100, false).unwrap_err();
    assert_eq!(missing.kind(), ErrorKind::NotFound);
    assert!(missing.raw_os_error().is_some());
    for range in [
        std::ops::Range { start: 20, end: 10 },
        0..101,
        100..u64::MAX,
    ] {
        // Refuse the geometry before touching even a missing file.
        assert_eq!(
            read_repaired_range(&path, 0, 100, range, true)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput,
        );
    }
    std::fs::write(&path, [0; 99]).unwrap();
    assert_eq!(
        read_repaired_range(&path, 0, 100, 0..100, false)
            .unwrap_err()
            .kind(),
        ErrorKind::UnexpectedEof,
    );
    // A missing CBC edge is also a short read, never synthesized padding.
    assert_eq!(
        read_repaired_range(&path, 0, 100, 0..98, true)
            .unwrap_err()
            .kind(),
        ErrorKind::UnexpectedEof,
    );
}

// ---------------------------------------------------------------------------
// Sparse marking
// ---------------------------------------------------------------------------

#[test]
fn creating_a_sparse_file_leaves_it_empty_and_writable() {
    use super::super::sparse::{SparseMarking, create_sparse};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("silver.horizon.f0.vol00000.envelope");

    let file = create_sparse(&path, &SparseMarking::Platform)
        .expect("the platform marker must succeed on a fresh file");

    assert!(path.exists(), "the destination is created here, not later");
    assert_eq!(
        file.metadata().unwrap().len(),
        0,
        "marking happens before any length is set or byte written"
    );
    // Writable through the returned handle, which is what the holds scratch
    // relies on rather than reopening.
    use std::io::Write;
    (&file).write_all(b"header").unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"header");
}

#[test]
fn creating_a_sparse_file_over_an_existing_one_keeps_its_bytes() {
    use super::super::sparse::{SparseMarking, create_sparse};

    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join("Silver.Horizon.S01E01.mkv.f0.direct.partial");
    std::fs::write(&path, b"already routed").unwrap();

    create_sparse(&path, &SparseMarking::Platform).expect("re-marking is allowed");

    assert_eq!(
        std::fs::read(&path).unwrap(),
        b"already routed",
        "a restart re-marks a destination that already holds routed bytes; \
         truncating there would throw away the coverage the checkpoint claims"
    );
}

#[test]
fn a_marking_failure_removes_the_file_it_created_but_never_a_pre_existing_one() {
    use super::super::sparse::{SparseMarking, create_sparse};

    let dir = tempfile::tempdir().unwrap();
    let fresh = dir.path().join("fresh.f0.direct.partial");
    let existing = dir.path().join("existing.f0.direct.partial");
    std::fs::write(&existing, b"routed bytes").unwrap();

    create_sparse(&fresh, &SparseMarking::AlwaysFail)
        .expect_err("an unmarkable destination must be refused");
    assert!(
        !fresh.exists(),
        "the caller demotes, so nothing it created may be left for the restart \
         sweep to reason about"
    );

    create_sparse(&existing, &SparseMarking::AlwaysFail)
        .expect_err("an unmarkable destination must be refused");
    assert_eq!(
        std::fs::read(&existing).unwrap(),
        b"routed bytes",
        "a marking failure on restart must not destroy the bytes the \
         conventional path is about to reconstruct from"
    );
}

#[test]
fn the_platform_marker_reports_success_where_holes_are_free() {
    use super::super::sparse::{SparseMarker, SparseMarking};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("probe.bin");
    let file = std::fs::File::create(&path).unwrap();

    // Unix files are sparse by writing past a hole, so the marker must succeed
    // rather than report `Unsupported` — the caller's failure arm is a
    // demotion, and demoting every set on Linux would be an outage.
    #[cfg(not(windows))]
    SparseMarking::Platform
        .mark_sparse(&file)
        .expect("the unix marker is a no-op that succeeds");
    // On Windows this is the real `FSCTL_SET_SPARSE`; a temp dir on NTFS
    // supports it, and a filesystem that does not is exactly the case the
    // demotion arm exists for.
    #[cfg(windows)]
    let _ = SparseMarking::Platform.mark_sparse(&file);
    let _ = file;
}

#[test]
fn every_reconstruction_failure_has_its_own_metric_label() {
    use super::super::reconstruct::ReconstructionFailure;

    // `sparse_mark_failed` is a later addition, and a duplicate label would
    // silently merge two very different diagnoses in the demotion counters.
    let labels = [
        ReconstructionFailure::NoLayout.metric(),
        ReconstructionFailure::MissingBytes {
            volume_index: 0,
            offset: 0,
        }
        .metric(),
        ReconstructionFailure::ChecksumMismatch {
            volume_index: 0,
            offset: 0,
        }
        .metric(),
        ReconstructionFailure::UnverifiableRun {
            volume_index: 0,
            offset: 0,
        }
        .metric(),
        ReconstructionFailure::WriteFailed {
            volume_index: 0,
            error: String::new(),
        }
        .metric(),
        ReconstructionFailure::SparseMarkFailed {
            volume_index: 0,
            error: String::new(),
        }
        .metric(),
    ];
    let unique: std::collections::BTreeSet<&str> = labels.iter().copied().collect();
    assert_eq!(unique.len(), labels.len(), "labels must be distinct");
    assert!(unique.contains("sparse_mark_failed"));
}

// ---------------------------------------------------------------------------
// Snapshot schema 4: the crypt row
// ---------------------------------------------------------------------------

#[test]
fn the_member_cipher_snapshot_is_shared_until_a_member_moves() {
    // `member_ciphers` deep-cloned every member's checkpoint map and coverage
    // on every call, and its callers call it per provider — which live PAR2
    // assembles per read-back, one per straddling block. The first shape kept
    // one checkpoint per member, so the copy was small; It now keeps one per
    // `CHECKPOINT_STRIDE`, which for a 50 GiB member is some 12,800 `BTreeMap`
    // nodes copied to answer a question whose answer had not changed.
    //
    // Two things are asserted, and the second is the one that matters: reads
    // share the snapshot, and a mutation really does drop it — a cache that
    // outlived a coverage change would have the overlay re-encrypt from facts
    // the member has moved past.
    let plain: Vec<u8> = (0..600u32).map(|index| (index % 251) as u8).collect();
    let (mut router, cipher) = encrypted_crypt_router_partial(&plain, 64, 320);

    let first = router.member_ciphers();
    assert_eq!(
        first.len(),
        1,
        "non-vacuity: the fixture must have routed an encrypted member"
    );
    let builds = router.member_ciphers_builds();
    assert!(builds > 0, "non-vacuity: the snapshot must have been built");
    for _ in 0..16 {
        assert!(
            std::sync::Arc::ptr_eq(&first, &router.member_ciphers()),
            "a read must hand back the snapshot it already has"
        );
    }
    assert_eq!(
        router.member_ciphers_builds(),
        builds,
        "and it must not have rebuilt one behind that"
    );
    let partial = first.values().next().expect("one member").clone();
    assert!(
        partial.tail_plain().is_none(),
        "non-vacuity: the half-routed member must not have its padding yet"
    );

    // Routing the rest moves the member's coverage *and* its retained padding,
    // which is exactly what a stale snapshot would go on denying.
    router.stage_for_test(0, 64 + 320, &cipher[320..]);
    router
        .drain_for_test(0)
        .expect("the rest of the member routes");
    let second = router.member_ciphers();
    assert!(
        !std::sync::Arc::ptr_eq(&first, &second),
        "a mutation must have dropped the snapshot"
    );
    assert!(
        router.member_ciphers_builds() > builds,
        "and the next read must have rebuilt it"
    );
    let whole = second.values().next().expect("one member");
    assert!(
        whole.tail_plain().is_some() && whole.plaintext_present(0, plain.len() as u64),
        "the rebuilt snapshot must describe the member as it is now"
    );
    assert!(
        !partial.plaintext_present(0, plain.len() as u64),
        "non-vacuity: which is not what the old one described"
    );
}

#[test]
fn a_snapshot_round_trips_its_crypt_rows_and_carries_no_password() {
    // 600 is not a multiple of 16, so the member carries 8 bytes of tail
    // padding: the row under test has a real retained tail, a real checkpoint
    // and real key-derived state, all of it produced by the router.
    let plain: Vec<u8> = (0..600u32).map(|index| (index % 251) as u8).collect();
    let router = encrypted_crypt_router(&plain, 64);

    let rows = router.member_crypt_snapshots();
    assert_eq!(
        rows.len(),
        1,
        "the fixture must have routed one encrypted member"
    );
    let row = rows.values().next().expect("the row exists").clone();
    assert_eq!(
        row.tail_padding, 8,
        "the fixture must exercise the retained padding, or the most password-adjacent \
         field in the row is not in the blob at all"
    );
    assert_eq!(row.tail_plain.len(), 8);
    assert!(
        !row.checkpoints.is_empty(),
        "a decrypted run must have left its frontier checkpoint"
    );

    let mut snapshot = sample_snapshot();
    snapshot.destinations[0].crypt = Some(row.clone());

    let blob = super::super::snapshot::encode(&snapshot).expect("the crypt row encodes");
    assert_eq!(
        u16::from_le_bytes([blob[4], blob[5]]),
        super::super::snapshot::SNAPSHOT_SCHEMA_VERSION,
        "a blob carrying crypt rows is written at the current schema"
    );
    let decoded = super::super::snapshot::decode(&blob).expect("the crypt row decodes");
    assert_eq!(decoded, snapshot);
    assert_eq!(
        decoded.destinations[0].crypt.as_ref().map(|row| row.keying),
        Some(super::super::router::crypt::MemberCryptKeying::Rar5 {
            salt: CRYPT_SALT,
            kdf_count_lg2: CRYPT_KDF_LG2,
            iv: CRYPT_IV,
            psw_check_present: false,
        }),
        "the keying a restore rebuilds a key from must survive the round trip"
    );

    // Non-vacuity, and the whole reason this drives the producer: the blob
    // demonstrably *does* carry this member's crypt row, so a scan that finds
    // nothing is a scan over the right bytes. The salt and the retained padding
    // are the two things in it derived from the password's own material.
    assert!(
        contains_bytes(&blob, &CRYPT_SALT),
        "the encoded blob must really contain the crypt row's salt"
    );
    assert!(
        contains_bytes(&blob, &row.tail_plain),
        "the encoded blob must really contain the retained tail padding"
    );

    // The one thing that must never be in there. Searched as bytes rather than
    // asserted structurally, because a password could only get in by accident
    // and an accident would not respect the struct.
    assert!(
        !contains_bytes(&blob, CRYPT_PASSWORD.as_bytes()),
        "a coverage snapshot must never carry a password"
    );

    // And the type that actually holds it: a key ring in a log is a password on
    // disk, so its `Debug` is part of the same guarantee.
    let ring = router.crypt_debug();
    assert!(
        ring.contains("has_password: true"),
        "non-vacuity — the ring under test is holding one, got {ring}"
    );
    assert!(
        !ring.contains(CRYPT_PASSWORD),
        "the key ring's Debug must never print the password, got {ring}"
    );
}

#[test]
fn a_partially_retained_padding_is_not_checkpointed_as_the_members_own_bytes() {
    // The padding finding, through the producer: the router's snapshot carries
    // the padding only when every byte of it has arrived. `MemberCrypt`'s own
    // tests pin the split arrival; this pins that the row the barrier writes
    // agrees.
    let plain: Vec<u8> = (0..600u32).map(|index| (index % 251) as u8).collect();
    let router = encrypted_crypt_router(&plain, 64);
    let row = router
        .member_crypt_snapshots()
        .values()
        .next()
        .expect("the row exists")
        .clone();
    assert_eq!(
        row.tail_plain.len(),
        usize::from(row.tail_padding),
        "a padding retained whole is carried whole"
    );
    assert_eq!(
        row.tail_plain.as_slice(),
        b"abcdefgh",
        "these are the member's real trailing plaintext, byte for byte — not the \
         zero placeholders `retain_tail_padding` sizes the buffer with"
    );
}

#[test]
fn every_older_schema_is_refused_rather_than_read_as_something_it_is_not() {
    // v3: a claim over an encrypted member's destination describes plaintext
    // while a v3 reader would take it for posted bytes.
    // v4: `MemberCryptSnapshot`'s flat RAR5 fields became a discriminant, so a
    // v4 row is a shorter positional array *and* one that cannot say whether it
    // describes a RAR4 or a RAR5 member. Refused in both directions, as
    // established — the codec accepts exactly its own version.
    let blob = super::super::snapshot::encode(&sample_snapshot()).unwrap();
    for found in [3u16, 4] {
        let mut older = blob.clone();
        older[4..6].copy_from_slice(&found.to_le_bytes());
        assert_eq!(
            super::super::snapshot::decode(&older),
            Err(super::super::snapshot::SnapshotError::UnsupportedVersion {
                found,
                supported: super::super::snapshot::SNAPSHOT_SCHEMA_VERSION,
            })
        );
    }
    // And a newer one, so the refusal is not an accident of ordering.
    let mut newer = blob;
    newer[4..6]
        .copy_from_slice(&(super::super::snapshot::SNAPSHOT_SCHEMA_VERSION + 1).to_le_bytes());
    assert!(matches!(
        super::super::snapshot::decode(&newer),
        Err(super::super::snapshot::SnapshotError::UnsupportedVersion { .. })
    ));
}

/// A member name direct finalization records must be one completion can resolve
/// back to a file on disk.
///
/// RAR4 writes paths with `\` separators. The destination is derived through
/// `resolve_member_path`, which rewrites those to `/`, so recording the raw
/// archive name left the two disagreeing: completion looked for
/// `work\sample.mkv` under the working directory, found nothing, treated the
/// member as a stale extracted record and re-ran conventional extraction, which
/// failed with "no on-disk RAR volumes" — direct finalization having correctly
/// never written any. Only a member with a directory component shows it; a flat
/// name has no separator to disagree about, which is why one RAR4 fixture failed
/// while its multi-member sibling passed.
#[test]
fn a_rar4_member_name_records_under_the_path_it_was_written_to() {
    let raw = r"work\sample.mkv";
    let recorded = super::super::plan::DirectSetPlan::destination_relative_name(raw)
        .expect("a RAR4 member path resolves");
    assert_eq!(
        recorded, "work/sample.mkv",
        "the recorded name must use the separator the destination was written with"
    );
    assert!(
        !recorded.contains('\\'),
        "no archive-native separator may survive into the extracted-member record"
    );

    // A name already in destination form is unchanged, so the RAR5 path that
    // was always correct keeps recording exactly what it recorded before.
    assert_eq!(
        super::super::plan::DirectSetPlan::destination_relative_name("work/sample.mkv")
            .expect("a RAR5 member path resolves"),
        "work/sample.mkv"
    );
}

// ---------------------------------------------------------------------------
// The member tolerance's kind gate
// ---------------------------------------------------------------------------

/// Every `IneligibilityReason` classified once, exhaustively, so adding a
/// variant to the library lands here as a decision rather than as a silent
/// default.
///
/// Size is deliberately absent from the inputs: the tolerance carries a shape
/// it can stream-extract whatever that member weighs, and refuses a shape it
/// cannot however small it is. The sizes below are the ones the retired
/// `min(64 MiB, 1% of packed archive bytes)` / 256 MiB ceilings would have
/// demoted on, and none of them changes an answer.
#[test]
fn the_member_tolerance_gates_on_shape_and_never_on_size() {
    use super::super::router::member_shape_is_tolerable;
    use unrar_rs::{IneligibilityReason, MalformedReason};

    let huge = 8 * 1024 * 1024 * 1024u64;
    for (reason, tolerable, why) in [
        (
            IneligibilityReason::Compressed {
                packed_bytes: Some(1),
                unpacked_bytes: Some(1),
                totals_final: true,
            },
            true,
            "a compressed member is an unencrypted, non-solid, per-member regular file",
        ),
        (
            IneligibilityReason::Compressed {
                packed_bytes: Some(huge),
                unpacked_bytes: Some(huge),
                totals_final: true,
            },
            true,
            "size is not an input: an 8 GiB compressed member rides the same tolerance",
        ),
        (
            IneligibilityReason::Compressed {
                packed_bytes: None,
                unpacked_bytes: None,
                totals_final: false,
            },
            true,
            "an open chain with no declared totals is still a compressed regular file",
        ),
        (
            IneligibilityReason::Blake2OnlyNoCrc32,
            true,
            "out-of-order routing cannot verify it, but the streaming decode checks \
             BLAKE2sp natively",
        ),
        (
            IneligibilityReason::Directory,
            true,
            "a dataless header finalization creates through the extraction root",
        ),
        (
            IneligibilityReason::Solid,
            false,
            "decodable only against its whole solid run, which means decoding the \
             stored members direct routing carried away",
        ),
        (
            IneligibilityReason::Redirection,
            false,
            "a link has to be created, not decoded, and the tolerated writer path \
             does not create links",
        ),
        (
            IneligibilityReason::NoChecksum,
            false,
            "nothing would check the bytes the decode produced",
        ),
        (
            IneligibilityReason::Encrypted,
            false,
            "encryption direct-store cannot route at all; a routable encrypted store \
             member never reaches this predicate",
        ),
        (
            IneligibilityReason::MalformedChain(MalformedReason::ContinuationInFirstVolume),
            false,
            "the layout does not agree with itself about where the member's parts are",
        ),
    ] {
        assert_eq!(
            member_shape_is_tolerable(reason),
            tolerable,
            "{reason:?} must be {}: {why}",
            if tolerable { "tolerable" } else { "refused" }
        );
    }
}

// ---------------------------------------------------------------------------
// What a demoted set's consumers need under it
// ---------------------------------------------------------------------------

/// Every [`DemotionReason`] classified once, exhaustively, into "the virtual
/// volumes are still a truthful image" and "real files are required".
///
/// The list is written out variant by variant rather than derived, because
/// deriving it is exactly the mistake: a reason added later must arrive here as
/// a decision. `metric()` is the exhaustiveness witness — a new variant fails to
/// compile there, and the count assertion below fails if one is added to the
/// enum without being added to this table.
#[test]
fn every_demotion_reason_states_what_its_consumers_need() {
    use super::super::router::crypt::{CryptRefusal, HeaderCryptRefusal};
    use super::super::router::{MemberIneligibility, VolumeDemand};

    let virtual_volumes = [
        // Member shape is a fact about a member, never about the image.
        DemotionReason::MemberIneligible(MemberIneligibility::Compressed),
        DemotionReason::MemberIneligible(MemberIneligibility::Encrypted),
        DemotionReason::MemberIneligible(MemberIneligibility::Solid),
        DemotionReason::MemberIneligible(MemberIneligibility::Directory),
        DemotionReason::MemberIneligible(MemberIneligibility::Redirection),
        DemotionReason::MemberIneligible(MemberIneligibility::Blake2OnlyNoCrc32),
        DemotionReason::MemberIneligible(MemberIneligibility::NoChecksum),
        DemotionReason::MemberIneligible(MemberIneligibility::MalformedChain),
        // Key material, not layout: the conventional extractor asks a wider
        // candidate list, and it can ask it of the overlay.
        DemotionReason::EncryptedMemberRefused(CryptRefusal::NoPassword),
        DemotionReason::EncryptedMemberRefused(CryptRefusal::WrongPassword),
        DemotionReason::EncryptedMemberRefused(CryptRefusal::Unkeyable),
        // Staged, unrouted bytes ran out of room. The layout and the placed
        // bytes are untouched.
        DemotionReason::HoldsBudgetExceeded,
        DemotionReason::HoldsScratchFailed,
        DemotionReason::HoldsScratchCeiling,
        // A refused *destination*, over a truthful image.
        DemotionReason::UnsafeDestination,
        DemotionReason::CollidingDestinations,
    ];
    let real_files = [
        // Sealed headers: there is no layout to overlay.
        DemotionReason::HeaderEncryptedRefused(HeaderCryptRefusal::NoPassword),
        DemotionReason::HeaderEncryptedRefused(HeaderCryptRefusal::NoVerifiedCandidate),
        DemotionReason::HeaderEncryptedRefused(HeaderCryptRefusal::Unverifiable),
        DemotionReason::HeaderEncryptedRefused(HeaderCryptRefusal::Rar4Headers),
        DemotionReason::HeaderEncryptedRefused(HeaderCryptRefusal::Unkeyable),
        // The layout cannot be read as describing this archive.
        DemotionReason::EncryptedFactsDisagree,
        DemotionReason::ConflictingVolumeFacts,
        DemotionReason::QuickOpenMismatch,
        DemotionReason::FormatMismatch,
        DemotionReason::UnparsableVolume,
        DemotionReason::UnsupportedFormat,
        DemotionReason::IdentityVolumeMismatch,
        // The overlay cannot answer for a range it claims.
        DemotionReason::EncryptedPostedBytesUnavailable,
        DemotionReason::UnconfirmedRestoredVolume,
        DemotionReason::RestartRearmUnplaceable,
        DemotionReason::RestartRereadFailed,
        DemotionReason::RepairRerouteFailed,
        DemotionReason::RepairGapUnreadable,
        DemotionReason::UuencodedSourceVolume,
        DemotionReason::IdentityRosterUnfillable,
        // The bytes are provably not what was posted.
        DemotionReason::PartChecksumMismatch,
        DemotionReason::MemberChecksumMismatch,
        DemotionReason::VolumeCrcMismatch,
        // The consumer that runs next is filesystem-bound, or has already
        // failed against this exact image.
        DemotionReason::Par2Damaged,
        DemotionReason::Par2Unbindable,
        DemotionReason::ToleratedExtractionFailed,
        // The overlay's own files are what is failing, or are half renamed
        // away.
        DemotionReason::DestinationWriteFailed,
        DemotionReason::SparseMarkFailed,
        DemotionReason::FinalizationFailed,
    ];

    for reason in virtual_volumes {
        assert_eq!(
            reason.volume_demand(),
            VolumeDemand::Virtual,
            "{reason} must be servable from the set's own virtual volumes"
        );
    }
    for reason in real_files {
        assert_eq!(
            reason.volume_demand(),
            VolumeDemand::Real,
            "{reason} must materialize real volume files"
        );
    }

    // Every reason is classified exactly once, and every metric label is
    // distinct — so a variant added to the enum but not to this table shows up
    // as a missing label rather than as a silent pass.
    let mut labels: Vec<&'static str> = virtual_volumes
        .iter()
        .chain(real_files.iter())
        .map(|reason| reason.metric())
        .collect();
    let total = labels.len();
    labels.sort_unstable();
    labels.dedup();
    assert_eq!(
        labels.len(),
        total,
        "two demotion reasons in this table share a metric label"
    );
    assert_eq!(
        total, 45,
        "a demotion reason was added or retired; classify it above and update this count"
    );
}
