//! The classification frontier
//! Demotion after a routed member turns ineligible
//! The staged-bytes budget
//! Zero-length stored members
//! The member tolerance
//! Directory members: dataless, free, and never a reason to demote
//! Review fixes: what a *later* PAR2 pass sees, what an unbindable volume does,

use super::*;

// ---------------------------------------------------------------------------
// The classification frontier
// ---------------------------------------------------------------------------

#[tokio::test]
async fn payload_past_the_last_known_header_is_held_until_the_walk_proves_what_it_is() {
    // Adopting a late member (the confirming parse) is only safe if its bytes
    // are still routable when it is adopted. A byte past the last member extent
    // the walk has *reached* has no proven classification: it can be an
    // envelope byte, or it can be an undiscovered member's payload. Filing it as
    // envelope on a guess loses a whole file, because finalization deletes the
    // envelopes.
    let episode = "Silver.Horizon.S01E21.mkv";
    let notes = "Silver.Horizon.S01E21.nfo";
    let episode_payload: Vec<u8> = (0..1200u32).map(|index| (index % 251) as u8).collect();
    let notes_payload: Vec<u8> = (0..600u32).map(|index| (index % 197) as u8).collect();
    let volumes = set_with_a_second_member_behind_a_header_hole(
        episode,
        &episode_payload,
        notes,
        &notes_payload,
    );

    // Non-vacuity: with the middle article absent, the second member's header
    // really is unreachable and its data really does reach into the last one.
    let last = &volumes[1].1;
    let header_at = last
        .windows(notes.len())
        .position(|window| window == notes.as_bytes())
        .expect("the fixture carries the second member's header");
    let (first_end, _) = article_extent(last.len(), 0, 3);
    let (middle_start, middle_end) = article_extent(last.len(), 1, 3);
    assert!(
        header_at >= middle_start && header_at < middle_end,
        "the second member's header must sit in the middle article ({header_at} not in \
         {middle_start}..{middle_end})"
    );
    assert!(
        last.len() - middle_end > 0 && first_end < middle_start,
        "its data must reach into the last article"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(41047);
    let spec = direct_store_job_spec_with_articles("Silver Horizon", &volumes, 3);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // Volume 0 whole, then volume 1's first and *last* articles — leaving the
    // header hole open — and only afterwards the middle one that closes it.
    for (file_index, segment_number) in [(0u32, 0u32), (0, 1), (0, 2), (1, 0), (1, 2), (1, 1)] {
        submit_volume_article_of(
            &mut pipeline,
            job_id,
            &volumes,
            file_index,
            segment_number,
            3,
        )
        .await;
    }

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Finalized"),
        "the set must adopt the member behind the header hole and finalize, got {shape}"
    );
    for (name, expected) in [
        (episode, episode_payload.as_slice()),
        (notes, notes_payload.as_slice()),
    ] {
        assert_eq!(
            std::fs::read(
                crate::pipeline::Pipeline::member_output_paths(
                    &payload_root(&temp_dir, JobId(41047)),
                    name
                )
                .0
            )
            .ok()
            .as_deref(),
            Some(expected),
            "{name} must be byte-correct — its payload arrived before its header did"
        );
    }
    for (filename, _) in &volumes {
        assert!(!working_dir.join(filename).exists());
    }
}

// ---------------------------------------------------------------------------
// Demotion after a routed member turns ineligible
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_member_that_turns_ineligible_after_routing_never_materializes_fabricated_bytes() {
    const RR_BYTES: usize = 4096;
    let member_name = "Silver.Horizon.S01E23.mkv";
    let hidden_name = "Silver.Horizon.S01E23.nfo";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 167) as u8).collect();
    let hidden_payload: Vec<u8> = (0..600u32).map(|index| (index % 197) as u8).collect();
    let volumes = blake2_close_with_recovery_and_hidden_member(
        member_name,
        &payload,
        hidden_name,
        &hidden_payload,
        3,
        RR_BYTES,
    );
    let arrivals = in_order_arrivals(volumes.len());

    let temp_dir = tempfile::tempdir().unwrap();
    let (shape, working_dir) =
        run_direct_store_routing_only(&temp_dir, JobId(41051), &volumes, &arrivals).await;
    // The chain close no longer demotes on the member's shape: the member
    // migrates into the envelope and rides the tolerance. What demotes this
    // fixture is the tolerated decode at finalization, because its BLAKE2sp
    // digest is a deliberately bogus `[0x37; 32]` that no decode can match.
    // The claims below are the ones this test exists for, and they are the
    // same either way: whatever demoted it, the sweep must not fabricate a
    // byte out of the classification it now has.
    assert!(
        shape.contains("Demoted(ToleratedExtractionFailed)"),
        "the set must reach the tolerated extraction and demote there, got {shape}"
    );

    // Non-vacuity: the envelope for a volume whose member data was routed away is
    // sparse across exactly those offsets, and long enough — the recovery record
    // sits past them — that a read there returns zeros rather than short. That is
    // the read a sweep taking its extents from the current classification would
    // have made.
    assert!(
        RR_BYTES > payload.len(),
        "the recovery record must extend the envelope well past the member's data"
    );

    assert_volumes_are_never_fabricated(&working_dir, &volumes);
    for (volume_index, (filename, bytes)) in volumes.iter().enumerate() {
        assert_eq!(
            std::fs::read(working_dir.join(filename)).ok().as_deref(),
            Some(bytes.as_slice()),
            "volume {volume_index} must come back byte for byte, whether reconstructed from \
             routed extents or completed by the conventional handoff"
        );
    }
    assert!(
        !direct_partial(&temp_dir, JobId(41051), member_name).exists(),
        "the direct outputs are deleted once the volumes are real"
    );
}

#[tokio::test]
async fn a_partially_covered_volume_that_fails_its_composed_crc_refetches_alone() {
    // Per-run composition plus the no-reference refusal: a volume covered only
    // as far as its first article still has a composed reference for that
    // prefix, and the sweep checks it. Corrupting the member partial under the
    // covered prefix is what a partial that came back wrong looks like — and it
    // has to end in the refetch, not in a volume file nothing verified.
    //
    // The corruption sits inside volume 1's run and outside volume 0's, so the
    // two volumes reach opposite verdicts from the same sweep: volume 0 is
    // materialized and keeps its articles, volume 1 is refused and refetches.
    let member_name = "Silver.Horizon.S01E24.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    // Volume 1 carries the member's logical bytes from 800 onwards, and only its
    // first article was covered, so this offset is inside the *partially*
    // covered volume's run and outside volume 0's.
    const CORRUPTED_LOGICAL_OFFSET: u64 = 850;

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41052);
    let (mut pipeline, working_dir, other_file_bytes) =
        demote_mid_download(&temp_dir, job_id, &volumes, |_, _working_dir| {
            use std::io::{Seek, SeekFrom, Write};
            let partial = direct_partial(&temp_dir, JobId(41052), member_name);
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&partial)
                .expect("the member partial holds the routed bytes");
            file.seek(SeekFrom::Start(CORRUPTED_LOGICAL_OFFSET))
                .unwrap();
            file.write_all(&[0xFF]).unwrap();
            file.sync_all().unwrap();
        })
        .await;

    assert_eq!(
        std::fs::read(working_dir.join(&volumes[0].0))
            .ok()
            .as_deref(),
        Some(volumes[0].1.as_slice()),
        "the volume whose run composed correctly is materialized byte for byte"
    );
    for (filename, _) in &volumes[1..] {
        assert!(
            !working_dir.join(filename).exists(),
            "a run that fails its composed CRC32 must leave no volume behind"
        );
    }
    assert_eq!(
        queued_segments(&mut pipeline, job_id),
        vec![(1, 0), (1, 1), (2, 0), (2, 1)],
        "only the volume that failed verification comes back; its verified \
         neighbour's articles are never asked for again"
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().downloaded_bytes,
        other_file_bytes + volumes[0].1.len() as u64,
        "the volume that survived as a real file stays counted; the refused one does not"
    );
}

/// A demotion whose reason leaves the image truthful — the router's
/// [`VolumeDemand::Virtual`] class — must schedule no article the set already
/// received.
///
/// The fetch schedule is the assertion, not a status string: a demotion that
/// "succeeded" while quietly requeueing a routed article is exactly the excess
/// this path exists to stop paying, and only the queue can tell the difference.
#[tokio::test]
async fn a_virtual_reason_demotion_schedules_nothing_it_already_received() {
    let member_name = "Silver.Horizon.S01E26.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 179) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    // Two independent members of the Virtual class: a holds-budget refusal says
    // nothing about the layout, and a refused destination refuses where a member
    // would land rather than what the volume holds.
    for (job_id, reason) in [
        (JobId(41055), DemotionReason::HoldsBudgetExceeded),
        (JobId(41056), DemotionReason::CollidingDestinations),
    ] {
        assert_eq!(
            reason.volume_demand(),
            VolumeDemand::Virtual,
            "{reason:?} is only interesting here because the image stays truthful"
        );
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _working_dir, _) =
            demote_mid_download_for(&temp_dir, job_id, &volumes, reason, |_, _| {}).await;

        // Everything `demote_mid_download` routed before demoting. None of it may
        // come back off the wire.
        let received = [(0u32, 0u32), (0, 1), (1, 0)];
        let queued = queued_segments(&mut pipeline, job_id);
        for article in received {
            assert!(
                !queued.contains(&article),
                "{reason:?}: article {article:?} was already received and must not be \
                 fetched again, queue was {queued:?}"
            );
        }
        assert_eq!(
            queued,
            vec![(1, 1), (2, 0), (2, 1)],
            "{reason:?}: exactly the articles that never arrived are scheduled"
        );
    }
}

/// A demotion that does need real files under it still refetches only what its
/// evidence actually contradicts.
///
/// One article of one volume disagrees with the yEnc part CRC32 the wire
/// delivered for it. The covered range it sits in is *merged* — volume 0 was
/// routed end to end, so the coverage map holds one entry for the whole volume
/// — and charging the merged range would cost the volume. The article is the
/// unit the composition can vouch for, so the article is what comes back.
#[tokio::test]
async fn a_real_reason_demotion_refetches_only_the_article_that_disagreed() {
    let member_name = "Silver.Horizon.S01E27.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 181) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    // Volume 0 carries the member's logical bytes 0..800 and arrives as two
    // articles, so a byte this near the end of its range is under the second
    // article and outside the first.
    const CORRUPTED_LOGICAL_OFFSET: u64 = 700;

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41057);
    assert_eq!(
        DemotionReason::Par2Damaged.volume_demand(),
        VolumeDemand::Real,
        "the conventional repairer is filesystem-bound, so this demotion needs files"
    );
    let (mut pipeline, working_dir, other_file_bytes) = demote_mid_download_for(
        &temp_dir,
        job_id,
        &volumes,
        DemotionReason::Par2Damaged,
        |_, _| {
            use std::io::{Seek, SeekFrom, Write};
            let partial = direct_partial(&temp_dir, job_id, member_name);
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&partial)
                .expect("the member partial holds the routed bytes");
            file.seek(SeekFrom::Start(CORRUPTED_LOGICAL_OFFSET))
                .unwrap();
            file.write_all(&[0xFF]).unwrap();
            file.sync_all().unwrap();
        },
    )
    .await;

    let volume_zero_split = volumes[0].1.len().div_ceil(2);
    let written = std::fs::read(working_dir.join(&volumes[0].0)).unwrap();
    assert_eq!(
        &written[..volume_zero_split],
        &volumes[0].1[..volume_zero_split],
        "the article the corruption did not touch is rebuilt byte for byte"
    );
    // Above the split the file holds whatever the overlay answered with before
    // the reference disagreed. Those bytes are claimed by nothing — no floor
    // reaches them and the article over them is requeued below — so the refetch
    // overwrites them; what would be wrong is a *claim*, not their presence.

    assert_eq!(
        queued_segments(&mut pipeline, job_id),
        vec![(0, 1), (1, 1), (2, 0), (2, 1)],
        "only the disagreeing article of volume 0 comes back; its neighbour in the \
         same merged coverage run, and the verified prefix of volume 1, do not"
    );
    let volume_one_prefix = volumes[1].1.len().div_ceil(2) as u64;
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().downloaded_bytes,
        other_file_bytes + volume_zero_split as u64 + volume_one_prefix,
        "every article that survived stays counted, and only the refused one is given up"
    );
}

// ---------------------------------------------------------------------------
// The staged-bytes budget
// ---------------------------------------------------------------------------

#[tokio::test]
async fn retained_envelope_bytes_count_against_the_staged_budget() {
    const RR_BYTES: usize = 48 * 1024;
    const BUDGET: u64 = 16 * 1024;
    let episode = "Silver.Horizon.S01E25.mkv";
    let notes = "Silver.Horizon.S01E25.nfo";
    let episode_payload: Vec<u8> = (0..2000u32).map(|index| (index % 251) as u8).collect();
    let notes_payload: Vec<u8> = (0..60_000u32).map(|index| (index % 197) as u8).collect();
    let (volumes, rr_at) = recovery_record_between_members_set(
        episode,
        &episode_payload,
        notes,
        &notes_payload,
        RR_BYTES,
    );

    // Non-vacuity, part one: the recovery record is wholly inside volume 1's
    // first article, and the second member's data runs well past that article —
    // so the volume is still unconfirmed when the article is routed, and the
    // recovery record is below the last known member extent rather than above
    // it. Those are exactly the conditions under which the bytes are *routed*
    // and then retained.
    let (article_start, article_end) = article_extent(volumes[1].1.len(), 0, 2);
    assert!(
        article_start == 0 && rr_at + RR_BYTES < article_end,
        "the recovery record must sit inside volume 1's first article \
         ({rr_at}+{RR_BYTES} not inside {article_start}..{article_end})"
    );
    assert!(
        volumes[1].1.len() > article_end,
        "the second member's data must reach past that article"
    );

    // Part two: with a budget it cannot breach, the same arrival sequence routes
    // the recovery record into the envelope file. Bytes on disk are routed
    // bytes, not holds — so nothing here is pending, and the breach below can
    // only come from what routing retained.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(41053);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in [(0u32, 0u32), (0, 1), (1, 0)] {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "the default budget is nowhere near breached by this set, got {shape}"
    );
    let envelope = std::fs::read(working_dir.join("silver.horizon.f0.vol00001.envelope"))
        .expect("volume 1's envelope exists");
    assert_eq!(
        &envelope[rr_at..rr_at + RR_BYTES],
        &volumes[1].1[rr_at..rr_at + RR_BYTES],
        "the recovery record was routed into the envelope, so it is not a hold"
    );

    // Part three: the same sequence under a budget smaller than the retained
    // region pages it out. Counting only unrouted holds — as the first shape
    // did — would have found nothing to count at all, so nothing would have
    // breached and nothing would have paged; the retained recovery record is
    // what proves the accounting reaches the term RSS actually pays for.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(BUDGET);
    let job_id = JobId(41055);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let paged_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in [(0u32, 0u32), (0, 1), (1, 0)] {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "the retained recovery record is {RR_BYTES} bytes of RSS against a {BUDGET}-byte \
         budget and must page rather than demote, got {shape}"
    );
    let set = pipeline.direct_store.set(job_id, 0).unwrap();
    assert!(
        set.router.scratch_bytes() >= RR_BYTES as u64,
        "the retained recovery record must be what went to scratch, got {} bytes",
        set.router.scratch_bytes()
    );
    assert!(
        set.router.resident_staged_bytes() <= BUDGET,
        "paging must bring RAM back inside the budget, got {}",
        set.router.resident_staged_bytes()
    );
    // The envelope on disk is unaffected: paging moves the router's *retained
    // copy*, never the routed bytes.
    let envelope = std::fs::read(paged_dir.join("silver.horizon.f0.vol00001.envelope"))
        .expect("volume 1's envelope exists");
    assert_eq!(
        &envelope[rr_at..rr_at + RR_BYTES],
        &volumes[1].1[rr_at..rr_at + RR_BYTES],
        "paging the retained copy must not disturb what was already written"
    );
}

// ---------------------------------------------------------------------------
// Zero-length stored members
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_zero_length_member_finalizes_with_its_empty_file_present() {
    // Nothing is ever routed for a zero-length member, so the byte-driven
    // whole-member gate can never fire for it. The first shape left it
    // unverified for the life of the job: the set never finalized, never
    // demoted, and kept its suppressions armed over files that would never
    // exist. It verifies trivially instead — CRC32 of no bytes is zero, which
    // is what the header states — and finalization creates the file.
    let episode = "Silver.Horizon.S01E26.mkv";
    let empty = "Silver.Horizon.S01E26.nfo";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 251) as u8).collect();
    let volumes = store_set_with_an_empty_member(episode, &payload, empty);
    let arrivals = in_order_arrivals(volumes.len());
    let names = [episode, empty];

    let conventional = run_multi_member_gate(
        DirectStoreGate::Disabled,
        JobId(41055),
        &names,
        &volumes,
        &arrivals,
    )
    .await;
    let direct = run_multi_member_gate(
        DirectStoreGate::Enabled,
        JobId(41056),
        &names,
        &volumes,
        &arrivals,
    )
    .await;

    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written source volumes"
    );
    assert!(
        !direct.volume_file_seen,
        "a set with an empty member must still route without materializing a volume"
    );
    assert_eq!(
        direct.members[1].1.as_deref(),
        Some(&[][..]),
        "the empty member exists and is empty"
    );
    assert_eq!(
        (direct.members, direct.status),
        (conventional.members, conventional.status),
        "a set with a zero-length member must be byte-identical to the conventional extractor"
    );
}

// ---------------------------------------------------------------------------
// The member tolerance
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_small_blake2_only_member_rides_the_tolerance_and_both_members_match_the_extractor() {
    let store_name = "Silver.Horizon.S01E15.mkv";
    let extra_name = "Silver.Horizon.S01E15.nfo";
    // A small extra beside a much larger stored member: the population the
    // tolerance was first written for, kept as the control for the
    // larger-than-the-stored-member case below.
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 97) as u8).collect();
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        4,
        ToleranceExtra::Blake2OnlyStore,
    );
    let arrivals = in_order_arrivals(volumes.len());

    let conventional = run_multi_member_gate(
        DirectStoreGate::Disabled,
        JobId(41041),
        &[store_name, extra_name],
        &volumes,
        &arrivals,
    )
    .await;
    let direct = run_multi_member_gate(
        DirectStoreGate::Enabled,
        JobId(41042),
        &[store_name, extra_name],
        &volumes,
        &arrivals,
    )
    .await;

    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written source volumes"
    );
    assert!(
        !direct.volume_file_seen,
        "a set riding the tolerance still routes: no source volume may appear on disk"
    );
    assert_eq!(
        conventional.members[0].1.as_deref(),
        Some(store_payload.as_slice()),
        "the conventional extractor should reproduce the stored member"
    );
    assert_eq!(
        conventional.members[1].1.as_deref(),
        Some(extra_payload.as_slice()),
        "the conventional extractor should reproduce the BLAKE2sp-only member"
    );
    assert_eq!(
        (direct.members, direct.status),
        (conventional.members, conventional.status),
        "the stored member routes directly and the tolerated one is extracted through \
         the virtual volumes; both must be byte-identical to the conventional extractor"
    );
}

/// The shape the retired size ceiling threw whole sets away for: the ineligible
/// member is **larger than the stored one**, so it is nowhere near
/// `min(64 MiB, 1% of packed archive bytes)`. A store video beside a compressed
/// subtitle pack is this set at a different scale, and it has to route
/// everything routable and stream-extract the rest.
#[tokio::test]
async fn an_ineligible_member_larger_than_the_stored_one_rides_the_tolerance() {
    let store_name = "Amber.Trail.S02E04.mkv";
    let extra_name = "Amber.Trail.S02E04.extras.bin";
    let store_payload: Vec<u8> = (0..8_000u32).map(|index| (index % 251) as u8).collect();
    // Two and a half times the stored member — around 70% of the archive's
    // packed bytes, against a retired ceiling of 1%.
    let extra_payload: Vec<u8> = (0..20_000u32).map(|index| (index % 97) as u8).collect();
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        4,
        ToleranceExtra::Blake2OnlyStore,
    );
    let arrivals = in_order_arrivals(volumes.len());

    let conventional = run_multi_member_gate(
        DirectStoreGate::Disabled,
        JobId(41055),
        &[store_name, extra_name],
        &volumes,
        &arrivals,
    )
    .await;
    let direct = run_multi_member_gate(
        DirectStoreGate::Enabled,
        JobId(41056),
        &[store_name, extra_name],
        &volumes,
        &arrivals,
    )
    .await;

    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written source volumes"
    );
    // The regression this test exists for: the set used to demote at the chain
    // close, and a demotion materializes every volume of the group.
    assert!(
        !direct.volume_file_seen,
        "a majority-ineligible set still routes: no source volume may appear on disk"
    );
    assert_eq!(
        conventional.members[1].1.as_deref(),
        Some(extra_payload.as_slice()),
        "the conventional extractor should reproduce the ineligible member"
    );
    assert_eq!(
        (direct.members, direct.status),
        (conventional.members, conventional.status),
        "the stored member routes and the larger ineligible one is stream-extracted \
         through the virtual volumes; both must be byte-identical to the conventional \
         extractor"
    );
}

/// The case an earlier shape could only demote: a BLAKE2sp-only member the
/// router adopted while its chain was open, whose routed bytes are migrated out
/// of its partial and into the envelope so it can ride the tolerance after all.
#[tokio::test]
async fn a_split_blake2_only_member_migrates_to_the_envelope_and_matches_the_extractor() {
    let store_name = "Silver.Horizon.S01E51.mkv";
    let extra_name = "Silver.Horizon.S01E51.nfo";
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 97) as u8).collect();
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        4,
        ToleranceExtra::Blake2OnlySplit,
    );
    let arrivals = in_order_arrivals(volumes.len());

    let conventional = run_multi_member_gate(
        DirectStoreGate::Disabled,
        JobId(41091),
        &[store_name, extra_name],
        &volumes,
        &arrivals,
    )
    .await;
    let direct = run_multi_member_gate(
        DirectStoreGate::Enabled,
        JobId(41092),
        &[store_name, extra_name],
        &volumes,
        &arrivals,
    )
    .await;

    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written source volumes"
    );
    // Non-vacuity, and the whole point: an earlier shape demoted this set, and
    // a demotion materializes every volume. A run with no volume on disk is a
    // run that stayed direct through the chain close.
    assert!(
        !direct.volume_file_seen,
        "the migration must keep the set direct: no source volume may appear on disk"
    );
    assert_eq!(
        conventional.members[1].1.as_deref(),
        Some(extra_payload.as_slice()),
        "the conventional extractor should reproduce the BLAKE2sp-only member"
    );
    assert_eq!(
        (direct.members, direct.status),
        (conventional.members, conventional.status),
        "the migrated member is extracted from the virtual volumes and the stored one \
         from its own partial; both must be byte-identical to the conventional extractor"
    );
}

#[tokio::test]
async fn a_split_blake2_only_member_over_the_old_budget_migrates_and_finalizes() {
    let store_name = "Silver.Horizon.S01E52.mkv";
    let extra_name = "Silver.Horizon.S01E52.nfo";
    // Ten percent of the archive, not one: over the retired
    // `min(64 MiB, 1% of packed archive bytes)` ceiling, which used to demote
    // this set on its closing article after a complete direct route.
    let store_payload: Vec<u8> = (0..2_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 97) as u8).collect();
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        4,
        ToleranceExtra::Blake2OnlySplit,
    );

    let shape = tolerance_shape(JobId(41093), &volumes).await;
    assert!(
        shape.contains("Finalized"),
        "an adopted member over the old ceiling must migrate into the envelope and \
         finalize, got {shape}"
    );
}

#[tokio::test]
async fn a_migration_that_cannot_read_its_partial_demotes_cleanly() {
    let store_name = "Silver.Horizon.S01E53.mkv";
    let extra_name = "Silver.Horizon.S01E53.nfo";
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 97) as u8).collect();
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        4,
        ToleranceExtra::Blake2OnlySplit,
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41094);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // Everything up to the volume that closes the chain. The extra member's
    // first part is routed into its partial by the volume before it.
    let last = volumes.len() as u32 - 1;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if file_index == last {
            continue;
        }
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    let partial = direct_partial(&temp_dir, JobId(41094), extra_name);
    assert!(
        std::fs::metadata(&partial).is_ok_and(|metadata| metadata.len() > 0),
        "non-vacuity: the member must really have been adopted and routed before the \
         migration is asked to move its bytes"
    );
    // The migration's read is what fails: the bytes it is told are there are
    // gone. Truncation rather than deletion, so the destination the checkpoint
    // claims still exists and only the read can fail.
    std::fs::File::create(&partial).unwrap();

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if file_index != last {
            continue;
        }
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(MemberIneligible(Blake2OnlyNoCrc32))"),
        "a migration that cannot read the bytes it is moving must demote exactly as a \
         set with no migration at all would have, got {shape}"
    );
    // Cleanly: nothing half-moved, and nothing fabricated. The migration mutates
    // no state until every byte is in hand, so the routing history still claims
    // the member's extents and the demotion sweep reads them from the partial —
    // which is now empty, so it refuses rather than writing zeros into a volume.
    assert_volumes_are_never_fabricated(&working_dir, &volumes);
}

/// The one size bound the tolerance still has is not the tolerance's: an
/// adopted member that resolves ineligible at chain close is moved into the
/// envelope through memory, and a move over `MIGRATION_CEILING_BYTES` is
/// refused before a byte of it is read. The member then demotes on its own
/// reason, which is the answer it had before migration existed.
#[tokio::test]
async fn a_migration_over_its_ceiling_demotes_on_the_members_own_reason() {
    let store_name = "Silver.Horizon.S01E55.mkv";
    let extra_name = "Silver.Horizon.S01E55.nfo";
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 97) as u8).collect();
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        4,
        ToleranceExtra::Blake2OnlySplit,
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41096);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // The same route as `a_split_blake2_only_member_over_the_old_budget_migrates_and_finalizes`
    // up to the closing volume, which is the positive control: under the
    // default ceiling this exact member migrates and the set finalizes.
    let last = volumes.len() as u32 - 1;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if file_index == last {
            continue;
        }
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    let partial = direct_partial(&temp_dir, job_id, extra_name);
    assert!(
        std::fs::metadata(&partial).is_ok_and(|metadata| metadata.len() > 0),
        "non-vacuity: the member must really have been adopted and routed before the \
         migration is asked to move its bytes"
    );
    // Zero: every routed byte is over it, so the refusal is exercised by the
    // very move the sibling test performs.
    pipeline
        .direct_store
        .set_mut(job_id, 0)
        .expect("the set is routing")
        .router
        .set_migration_ceiling_bytes(0);

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if file_index != last {
            continue;
        }
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(MemberIneligible(Blake2OnlyNoCrc32))"),
        "a member over the migration ceiling must demote on its own ineligibility, not on \
         the ceiling, got {shape}"
    );
    // Refused before anything moved, so the demotion sweep reconstructs from a
    // partial that still holds every routed byte — and writes nothing it does
    // not have.
    assert_volumes_are_never_fabricated(&working_dir, &volumes);
}

#[tokio::test]
async fn an_ineligible_member_far_over_the_old_packed_ceiling_still_routes() {
    let store_name = "Silver.Horizon.S01E16.mkv";
    let extra_name = "Silver.Horizon.S01E16.nfo";
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();

    // The retired ceiling was `min(64 MiB, 1% of the archive's packed bytes)`.
    // 200 packed bytes against ~30 200 archive bytes is under 1%; 600 is over,
    // and 12 000 — 40% of the archive — is over by a wide margin. All three
    // must route: member shape is a member verdict, not a set one.
    let under = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &(0..200u32)
            .map(|index| (index % 97) as u8)
            .collect::<Vec<u8>>(),
        4,
        ToleranceExtra::Blake2OnlyStore,
    );
    let over = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &(0..600u32)
            .map(|index| (index % 97) as u8)
            .collect::<Vec<u8>>(),
        4,
        ToleranceExtra::Blake2OnlyStore,
    );
    let far_over = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &(0..12_000u32)
            .map(|index| (index % 97) as u8)
            .collect::<Vec<u8>>(),
        4,
        ToleranceExtra::Blake2OnlyStore,
    );

    for (job_id, volumes, label) in [
        (JobId(41043), under, "under the old 1% ceiling"),
        (JobId(41044), over, "over the old 1% ceiling"),
        (JobId(41049), far_over, "40% of the archive's packed bytes"),
    ] {
        let shape = tolerance_shape(job_id, &volumes).await;
        assert!(
            !shape.contains("Demoted"),
            "an ineligible member {label} must ride the tolerance, got {shape}"
        );
    }
}

#[tokio::test]
async fn a_declared_unpacked_size_over_the_old_tolerance_ceiling_still_routes() {
    let store_name = "Silver.Horizon.S01E17.mkv";
    let extra_name = "Silver.Horizon.S01E17.bin";
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 97) as u8).collect();

    // A compressed member declaring a 300 MiB expansion: past the retired
    // 256 MiB unpacked ceiling. What a decode may actually write is now the
    // job extraction budget's business, and refusing to *route* the set for a
    // number in a header is not a bound on anything.
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        4,
        ToleranceExtra::Compressed {
            declared_unpacked: 300 * 1024 * 1024,
            solid: false,
        },
    );
    // `ToleranceExtra::Compressed` writes a compressed *header* over a data
    // area that is not really compressed, so the decode at finalization cannot
    // succeed and the set demotes there. Reaching the decode at all is the
    // property: the old rule demoted during **routing**, on
    // `ToleranceBudgetExceeded`, without ever opening the archive.
    let shape = tolerance_shape(JobId(41045), &volumes).await;
    assert!(
        shape.contains("Demoted(ToleratedExtractionFailed)"),
        "a declared unpacked size must not end the route before extraction is tried, \
         got {shape}"
    );
}

#[tokio::test]
async fn a_split_ineligible_member_over_the_old_ceiling_migrates_and_routes() {
    let store_name = "Silver.Horizon.S01E18.mkv";
    let extra_name = "Silver.Horizon.S01E18.bin";
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    // 600 packed bytes total, split in half across the last two volumes: under
    // the retired 1% ceiling while the chain is open, over it once it closes.
    // That transition used to demote at the very last article; now it is the
    // migration into the envelope and nothing else.
    let extra_payload: Vec<u8> = (0..600u32).map(|index| (index % 97) as u8).collect();
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        4,
        ToleranceExtra::CompressedSplit,
    );

    // Everything up to (and including) the extra's first part: the chain is
    // still open and the set is routing. Non-vacuity for the assertion below —
    // the closing volume is what used to change the answer.
    let temp_dir = tempfile::tempdir().unwrap();
    let mut partial_arrivals = in_order_arrivals(volumes.len());
    partial_arrivals.truncate(2 * (volumes.len() - 1));
    let (open_shape, _) =
        run_direct_store_routing_only(&temp_dir, JobId(41046), &volumes, &partial_arrivals).await;
    assert!(
        !open_shape.contains("Demoted"),
        "an open chain must keep routing, got {open_shape}"
    );

    // As above, the fixture's compressed data area is synthetic, so the decode
    // at finalization fails; what matters is that the chain close carried the
    // member into the envelope and let the extraction be attempted, where it
    // used to end the route on `ToleranceBudgetExceeded`.
    let closed_shape = tolerance_shape(JobId(41047), &volumes).await;
    assert!(
        closed_shape.contains("Demoted(ToleratedExtractionFailed)"),
        "the chain closing over the old ceiling must migrate and reach the extraction, \
         got {closed_shape}"
    );
}

#[tokio::test]
async fn a_solid_ineligible_member_demotes_rather_than_riding_the_tolerance() {
    let store_name = "Silver.Horizon.S01E19.mkv";
    let extra_name = "Silver.Horizon.S01E19.bin";
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 97) as u8).collect();

    // Identical to a tolerated member in every budget dimension; only the
    // per-member solid flag differs. `extract_member_streaming` can only decode
    // a solid member against the rest of its solid run, so the tolerance must
    // not take it however small it is.
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        4,
        ToleranceExtra::Compressed {
            declared_unpacked: extra_payload.len() as u64,
            solid: true,
        },
    );
    let shape = tolerance_shape(JobId(41048), &volumes).await;
    assert!(
        shape.contains("Demoted(MemberIneligible(Solid))"),
        "a solid member must demote on its own reason rather than ride the tolerance, \
         got {shape}"
    );
}

// ---------------------------------------------------------------------------
// Directory members: dataless, free, and never a reason to demote
// ---------------------------------------------------------------------------

#[tokio::test]
async fn trailing_directory_headers_never_demote_a_store_set() {
    // The bench shape, in miniature: a stored member split across four volumes
    // and a folder tree whose dataless headers all sit at the very end of the
    // closing volume — so the demotion, if there is one, arrives on the last
    // article of the last volume with the whole set already routed.
    let member = "Silver.Horizon.S01E20.mkv";
    let payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    let directories: Vec<(&str, u32, u32)> = vec![
        ("Silver.Horizon.S01E20", 0o755, 1_600_000_000),
        ("Silver.Horizon.S01E20/subs", 0o755, 1_600_000_100),
        ("Silver.Horizon.S01E20/subs/forced", 0o700, 1_600_000_200),
    ];
    let volumes = store_set_with_directories(
        &[(member, payload.clone())],
        4,
        &directories,
        DirectoryPlacement::Trailing,
    );

    let shape = tolerance_shape(JobId(41210), &volumes).await;
    assert!(
        !shape.contains("Demoted"),
        "dataless directory headers must never demote a set, got {shape}"
    );

    // Leading directories are the same claim from the other side: the position
    // of the headers is not what decides it.
    let leading = store_set_with_directories(
        &[(member, payload)],
        4,
        &directories,
        DirectoryPlacement::Leading,
    );
    let leading_shape = tolerance_shape(JobId(41211), &leading).await;
    assert!(
        !leading_shape.contains("Demoted"),
        "directory headers ahead of the members must not demote either, got {leading_shape}"
    );
}

#[tokio::test]
async fn directories_ride_the_tolerance_and_never_demote_a_set() {
    // A control set with one ineligible member beside the stored one, and the
    // same set with *forty* directory entries added. Directory headers are
    // dataless: nothing is decoded for them, nothing is written for them until
    // finalization creates them, and no number of them may change the routing
    // verdict.
    let member = "Silver.Horizon.S01E21.mkv";
    let extra = "Silver.Horizon.S01E21.nfo";
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 97) as u8).collect();
    let bare = store_set_with_extra_member(
        member,
        &store_payload,
        extra,
        &extra_payload,
        4,
        ToleranceExtra::Blake2OnlyStore,
    );
    let bare_shape = tolerance_shape(JobId(41212), &bare).await;
    assert!(
        !bare_shape.contains("Demoted"),
        "the control fixture must ride the tolerance, got {bare_shape}"
    );

    let names: Vec<String> = (0..40)
        .map(|index| format!("Silver.Horizon.S01E21/part{index:02}"))
        .collect();
    let directories: Vec<(&str, u32, u32)> = names
        .iter()
        .map(|name| (name.as_str(), 0o755u32, 1_600_000_000u32))
        .collect();
    let crowded = store_set_with_directories(
        &[(member, store_payload)],
        4,
        &directories,
        DirectoryPlacement::Trailing,
    );
    let crowded_shape = tolerance_shape(JobId(41213), &crowded).await;
    assert!(
        !crowded_shape.contains("Demoted"),
        "forty directory entries must cost the tolerance nothing, got {crowded_shape}"
    );
}

#[tokio::test]
async fn a_directory_beside_a_large_ineligible_member_routes_the_whole_set() {
    // The shape that used to demote on the closing volume's last article: a
    // large ineligible member — over the retired `min(64 MiB, 1%)` ceiling —
    // with directory headers beside it. Both ride the tolerance now, and the
    // set keeps its direct route.
    let member = "Silver.Horizon.S01E22.mkv";
    let extra = "Silver.Horizon.S01E22.nfo";
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    // 600 packed bytes against ~30 600 archive bytes is over 1%.
    let over_payload: Vec<u8> = (0..600u32).map(|index| (index % 97) as u8).collect();
    let mut volumes = store_set_with_extra_member(
        member,
        &store_payload,
        extra,
        &over_payload,
        4,
        ToleranceExtra::Blake2OnlyStore,
    );
    // Directory headers spliced into the closing volume, ahead of its end
    // record, so the router classifies them in the same walk that resolves the
    // over-budget member.
    let last = volumes.len() - 1;
    let end_header = build_test_rar_end_header(false);
    let bytes = &mut volumes[last].1;
    let end_at = bytes.len() - end_header.len();
    let mut directory_headers = build_test_rar_directory_header("Silver.Horizon.S01E22", 0o755, 1);
    directory_headers.extend_from_slice(&build_test_rar_directory_header(
        "Silver.Horizon.S01E22/subs",
        0o755,
        2,
    ));
    bytes.splice(end_at..end_at, directory_headers);

    let shape = tolerance_shape(JobId(41214), &volumes).await;
    assert!(
        !shape.contains("Demoted"),
        "a large ineligible member beside directory headers must not demote the set, \
         got {shape}"
    );
}

#[tokio::test]
async fn a_folder_tree_store_set_finalizes_direct_with_its_directories_restored() {
    // The whole shape the bench found, end to end: a stored member inside a
    // folder, an *empty* folder beside it, and both directory headers written
    // after the member's last part in the closing volume. Nothing here may
    // reconstruct a volume or fall back to the conventional extractor — the
    // member routes, and the directories are created at finalization from the
    // archive's own metadata.
    let member_name = "Silver.Horizon.S01E23/file.bin";
    let outer = "Silver.Horizon.S01E23";
    let inner = "Silver.Horizon.S01E23/empty";
    const OUTER_MTIME: u32 = 1_600_000_000;
    const INNER_MTIME: u32 = 1_600_050_000;
    let payload: Vec<u8> = (0..4_000u32).map(|index| (index % 251) as u8).collect();
    let volumes = store_set_with_directories(
        &[(member_name, payload.clone())],
        3,
        &[(outer, 0o755, OUTER_MTIME), (inner, 0o700, INNER_MTIME)],
        DirectoryPlacement::Trailing,
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(41215);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
        for (filename, _) in &volumes {
            assert!(
                !working_dir.join(filename).exists(),
                "a folder-tree store set must never materialize {filename}"
            );
        }
    }
    // The directory entries are tolerated members, extracted through a
    // detached ticket the set finalizes behind.
    settle_direct_post_repair_work(&mut pipeline).await;

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Finalized"),
        "the set must finalize direct rather than demote on its directory headers, \
         got {shape}"
    );

    let root = payload_root(&temp_dir, job_id);
    let member_path = crate::pipeline::Pipeline::member_output_paths(&root, member_name).0;
    assert_eq!(
        std::fs::read(&member_path).ok(),
        Some(payload),
        "the stored member must be committed from its own routed partial"
    );
    assert!(
        !any_direct_partial(&root.join(outer)),
        "no `.direct.partial` may survive finalization"
    );

    for (name, mode, mtime) in [
        (outer, 0o755u32, OUTER_MTIME),
        (inner, 0o700u32, INNER_MTIME),
    ] {
        let path = root.join(name);
        let metadata = std::fs::symlink_metadata(&path)
            .unwrap_or_else(|error| panic!("{name} must exist as a directory: {error}"));
        assert!(
            metadata.is_dir(),
            "{name} must be a directory, never an empty file named like one"
        );
        assert_eq!(
            metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|since| since.as_secs()),
            Some(u64::from(mtime)),
            "{name} must carry the archive's mtime — applied after the member \
             renamed into it, or the commit would have overwritten it"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                metadata.permissions().mode() & 0o777,
                mode,
                "{name} must carry the archive's mode"
            );
        }
        let _ = mode;
    }
}

// ---------------------------------------------------------------------------
// Review fixes: what a *later* PAR2 pass sees, what an unbindable volume does,
// and what a mid-download set is protected from.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_finalized_direct_sets_volumes_are_not_missing_on_a_later_par2_pass() {
    // Finalization is what makes a direct set's source volumes permanently
    // absent: the partials are renamed to their destinations and the envelopes
    // are deleted. Every *later* pass over the same job — and one conventional
    // member failing extraction after the direct set finalized is enough to
    // cause one, because `par2_validation_needed` is already false so the
    // repair-first branch is skipped — would otherwise read those volumes off a
    // disk they were never on and call the job unrepairable.
    let member_name = "Silver.Horizon.S01E21.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 197) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41051);
    let (mut pipeline, working_dir) =
        direct_job_after_verification(&temp_dir, job_id, &volumes, &par2_bytes).await;

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        sets.contains("Finalized"),
        "the set must have finalized on the job's PAR2 verdict for this test to \
         mean anything, got {sets}"
    );
    assert!(
        no_volume_file(&working_dir, &volumes),
        "finalization must not have written a source volume"
    );

    // A conventional member's extraction fails afterwards. That is the whole
    // trigger: `has_crc_failures` re-opens the completion gate on a job whose
    // PAR2 is already marked verified.
    pipeline.failed_extractions.insert(
        job_id,
        ["Amber.Trail.S01E01.mkv".to_string()].into_iter().collect(),
    );
    let verifies_before = pipeline.par2_authoritative_verify_calls;
    pipeline.check_job_completion(job_id).await;

    assert!(
        pipeline.par2_authoritative_verify_calls > verifies_before,
        "non-vacuity: the later authoritative pass must actually have run, \
         calls={verifies_before} -> {}",
        pipeline.par2_authoritative_verify_calls
    );
    let status = job_status_for_assert(&pipeline, job_id);
    if let Some(JobStatus::Failed { error, .. }) = &status {
        panic!("a finalized direct set must not fail a later PAR2 pass, got: {error}");
    }
    assert!(
        no_volume_file(&working_dir, &volumes),
        "the later pass must not have had the repairer reconstruct source volumes \
         the job already finished without; status={status:?}"
    );
    assert!(
        pipeline
            .failed_extractions
            .get(&job_id)
            .is_none_or(HashSet::is_empty),
        "and the conventional failure must still have been cleared for its retry, \
         exactly as a clean PAR2 verdict does with the gate off"
    );
}

#[tokio::test]
async fn a_direct_volume_with_no_unambiguous_par2_identity_demotes_before_the_pass() {
    // The overlay is keyed by PAR2 file id, so a volume whose identity does not
    // resolve to exactly one description can neither be served virtually nor be
    // blamed for the damage the pass then reports about it. Before this fix the
    // set stayed direct and the repairer was handed a virtual volume.
    let member_name = "Silver.Horizon.S01E22.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 193) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41052);
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);

    let (spec, index_file_index) = par2_bearing_job_spec("Silver Horizon", &volumes, &par2_bytes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // The index first, so the ambiguity is provable before anything acts on it —
    // and so the PAR2 identity pass, which rewrites every file's identity as the
    // set parses, cannot undo it.
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        &par2_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;
    // Volume 0's identity now carries a canonical name that is *another*
    // volume's PAR2 name, so its candidate set spans two descriptions of the
    // same recovery set and `resolve_par2_file_binding` refuses to pick one.
    // This is the shape a rewritten identity produces in the wild; the recovery
    // set itself stays completely honest, which is what lets the job finish
    // conventionally below.
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .file_identities
        .insert(
            0,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: volumes[0].0.clone(),
                current_filename: volumes[0].0.clone(),
                canonical_filename: Some(volumes[1].0.clone()),
                classification: None,
                classification_source: crate::jobs::record::FileIdentitySource::Par2,
            },
        );
    assert!(
        pipeline
            .resolve_par2_file_binding(NzbFileId {
                job_id,
                file_index: 0
            })
            .is_none(),
        "non-vacuity: volume 0's name candidates must match two descriptions, so no \
         single PAR2 identity can be chosen for it"
    );
    assert!(
        pipeline
            .resolve_par2_file_binding(NzbFileId {
                job_id,
                file_index: 1
            })
            .is_some(),
        "and the rest of the set must still bind, so the demotion below is about the \
         one volume rather than about a job with no recovery set"
    );

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        sets.contains("Demoted(Par2Unbindable)"),
        "a set holding a volume PAR2 cannot name must leave direct mode before the \
         pass, on its own reason, got {sets}"
    );
    // Demotion materialized the volumes from the set's own routed bytes, which
    // is what the pass then reads — nothing repaired a virtual volume.
    for (filename, bytes) in &volumes {
        assert_eq!(
            std::fs::read(working_dir.join(filename)).ok().as_deref(),
            Some(bytes.as_slice()),
            "{filename} must have been materialized byte-exactly by the demotion"
        );
    }

    // The ambiguity was a property of the *identity*, and the fixture's rewritten
    // canonical name is a lie about which file this is — one the conventional
    // path would go on acting on long after it has served its purpose here.
    // Dropped now that the demotion it existed to cause has happened, so what
    // the rest of this test exercises is an ordinary conventional finish over
    // the materialized volumes.
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .file_identities
        .remove(&0);
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    let produced = std::fs::read(output_root.join(member_name))
        .ok()
        .or_else(|| staging_member(&complete_dir, member_name))
        .or_else(|| std::fs::read(working_dir.join(member_name)).ok());
    assert_eq!(
        produced.as_deref(),
        Some(payload.as_slice()),
        "and the job must finish conventionally from the materialized volumes; \
         status={:?}",
        job_status_for_assert(&pipeline, job_id)
    );
}

#[tokio::test]
async fn a_mid_download_direct_set_is_neither_verified_against_nor_demoted_for_its_holes() {
    // A set that is still receiving articles has holes where the rest of its
    // payload will go, and PAR2 cannot tell a hole from corruption. Both guards
    // are asserted: the completion gate's readiness predicate, and the demotion
    // helper that must not act on such a verdict even if one reaches it.
    let member_name = "Silver.Horizon.S01E23.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 181) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41053);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    // On, as in production: it is the live short-circuit that reaches a clean
    // verdict for a par2-bearing direct set (a direct set never enters the
    // archive topology, so the clean-integrity gate reads `None` for it).

    let (spec, index_file_index) = par2_bearing_job_spec("Silver Horizon", &volumes, &par2_bytes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // The index first, so a recovery set exists to verify against, then every
    // article except the last volume's tail.
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        &par2_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if (file_index, segment_number) == (volumes.len() as u32 - 1, 1) {
            continue;
        }
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }

    assert!(
        !pipeline.direct_sets_ready_for_authoritative_par2(job_id),
        "a set whose last volume is still downloading is not ready to be verified"
    );

    // The verdict such a pass would reach: every one of the set's volumes
    // reported missing, which is what a hole looks like from PAR2's side.
    let overlay = pipeline
        .direct_par2_overlay(job_id)
        .expect("the live set's volumes bind and are served virtually");
    let verification = par2_rs::VerificationResult {
        files: overlay
            .volumes
            .iter()
            .map(|volume| par2_rs::verify::FileVerification {
                file_id: volume.par2_file_id,
                filename: format!("virtual volume {}", volume.volume_index),
                status: par2_rs::verify::FileStatus::Missing,
                valid_slices: vec![false; 4],
                missing_slice_count: 4,
            })
            .collect(),
        recovery_blocks_available: 0,
        total_missing_blocks: 4 * overlay.volumes.len() as u32,
        repairable: par2_rs::verify::Repairability::NotNeeded,
    };
    assert!(
        !pipeline
            .demote_direct_sets_with_par2_damage(job_id, &verification)
            .await,
        "hole-damage on a set that is still downloading must not demote it"
    );
    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !sets.contains("Demoted"),
        "the set must still be routing, got {sets}"
    );
    assert!(
        no_volume_file(&working_dir, &volumes),
        "and nothing may have materialized or repaired a volume file"
    );

    // The last article lands: the same set is now verifiable, and the gate lets
    // the pass run.
    let verifies_before = pipeline.par2_authoritative_verify_calls;
    submit_volume_article(&mut pipeline, job_id, &volumes, volumes.len() as u32 - 1, 1).await;
    assert!(
        pipeline.direct_sets_ready_for_authoritative_par2(job_id),
        "once every volume has completed the set is ready to be verified"
    );
    let settled = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        pipeline.par2_authoritative_verify_calls > verifies_before || settled.contains("Finalized"),
        "and verification must have proceeded normally rather than being deferred \
         forever; sets={settled}"
    );
    assert!(
        no_volume_file(&working_dir, &volumes),
        "a clean par2-bearing direct job still never writes a source volume"
    );
}

#[tokio::test]
async fn a_tolerated_member_sharing_a_volume_with_a_routed_extent_extracts_before_the_commit() {
    // The tolerated extraction reads the *virtual volumes*: the envelopes
    // overlaid with each direct-routed member's `.direct.partial`. The commit
    // loop renames those partials to their destinations, so running the
    // extraction afterwards pointed the provider's map at paths that no longer
    // existed and turned every stored extent into a hole. It only survived
    // because a RAR header walk seeks over data areas rather than reading them;
    // the moment a read crosses a stored extent the extraction fails, and its
    // failure path is a demotion that can no longer reconstruct — a full
    // redownload.
    //
    // The shape here is the one that makes the dependency real: two volumes the
    // stored member is split across, with the tolerated member's header and data
    // sitting *after* a routed extent inside the last of them, so the walk
    // traverses the region the partial owns to reach it. The ordering itself is
    // held by a `debug_assert!` in `extract_tolerated_members`, which this test
    // exercises with a stored member present.
    let store_name = "Silver.Horizon.S01E24.mkv";
    let extra_name = "Silver.Horizon.S01E24.nfo";
    let store_payload: Vec<u8> = (0..30_000u32).map(|index| (index % 251) as u8).collect();
    let extra_payload: Vec<u8> = (0..200u32).map(|index| (index % 89) as u8).collect();
    let volumes = store_set_with_extra_member(
        store_name,
        &store_payload,
        extra_name,
        &extra_payload,
        2,
        ToleranceExtra::Blake2OnlyStore,
    );
    let arrivals = in_order_arrivals(volumes.len());

    let conventional = run_multi_member_gate(
        DirectStoreGate::Disabled,
        JobId(41054),
        &[store_name, extra_name],
        &volumes,
        &arrivals,
    )
    .await;
    let direct = run_multi_member_gate(
        DirectStoreGate::Enabled,
        JobId(41055),
        &[store_name, extra_name],
        &volumes,
        &arrivals,
    )
    .await;

    assert!(
        !direct.volume_file_seen,
        "the mixed set still routes: no source volume may appear on disk"
    );
    assert_eq!(
        conventional.members[0].1.as_deref(),
        Some(store_payload.as_slice()),
        "the conventional extractor should reproduce the routed member"
    );
    assert_eq!(
        conventional.members[1].1.as_deref(),
        Some(extra_payload.as_slice()),
        "the conventional extractor should reproduce the tolerated member"
    );
    assert_eq!(
        (direct.members, direct.status),
        (conventional.members, conventional.status),
        "the tolerated member must be extracted through virtual volumes whose stored \
         extents still resolve, and both members must be byte-identical to the \
         conventional extractor"
    );
}
