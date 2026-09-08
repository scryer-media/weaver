//! Quick Open: the cache is a hint, and direct-store never routes on one
//! The confirming parse
//! PAR2 over virtual volumes
//! Destinations, finalization and demotion accounting
//! Holds budget, and the demotion round trip it makes cheap to reach
//! Chain-close eligibility
//! The yEnc whole-volume gate
//! Suppression and runtime lifetime
//! Format detection
//! Multi-member sets
//! Envelope v2: recovery records route
//! The hybrid virtual-volume provider, differentially

use super::*;

// ---------------------------------------------------------------------------
// Quick Open: the cache is a hint, and direct-store never routes on one
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_forged_quick_open_member_never_reaches_the_router() {
    let member_name = "Silver.Horizon.S01E61.mkv";
    let forged_name = "Silver.Horizon.S01E61.forged";
    let payload: Vec<u8> = (0..400u32).map(|index| (index % 251) as u8).collect();
    let volumes = quick_open_store_set(member_name, &payload, Some(forged_name));

    // Non-vacuity, stated against the library rather than against weaver: with
    // Quick Open left on — which is `parse_all_headers`' default and what
    // `parse_volume_facts` still uses — the forged member *is* reported, and
    // `members_extend` would have adopted it and routed payload into it.
    assert_eq!(
        library_default_member_names(&volumes[1].1),
        vec![member_name.to_string(), forged_name.to_string()],
        "the fixture must actually forge a member the physical walk cannot see"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let arrivals = in_order_arrivals(volumes.len());
    let (shape, walks) =
        run_direct_store_routing_only_counting_walks(&temp_dir, JobId(41101), &volumes, &arrivals)
            .await;

    assert!(
        walks >= 1,
        "the adopted cache must have been cross-examined by a physical walk, got {walks}"
    );
    assert!(
        shape.contains("Demoted(QuickOpenMismatch)"),
        "a volume whose headers disagree with its physical walk must leave direct mode \
         under its own reason, got {shape}"
    );
    assert!(
        !direct_partial(&temp_dir, JobId(41101), forged_name).exists(),
        "no destination may be created for a member only the Quick Open cache claims"
    );
}

#[tokio::test]
async fn an_honest_quick_open_cache_still_routes() {
    let member_name = "Silver.Horizon.S01E62.mkv";
    let payload: Vec<u8> = (0..400u32).map(|index| (index % 251) as u8).collect();
    let volumes = quick_open_store_set(member_name, &payload, None);

    // The other half of the non-vacuity: the same fixture, same locator, same
    // `QO` block — and the cache agrees with the walk, so nothing is refused.
    assert_eq!(
        library_default_member_names(&volumes[1].1),
        vec![member_name.to_string()],
        "an honest cache reports exactly the physical member"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let arrivals = in_order_arrivals(volumes.len());
    let (shape, walks) =
        run_direct_store_routing_only_counting_walks(&temp_dir, JobId(41102), &volumes, &arrivals)
            .await;
    assert!(
        !shape.contains("Demoted"),
        "the cross-check must refuse a *disagreement*, not the presence of a cache, got {shape}"
    );
    // At least one: the last volume is parsed when its last article lands and
    // again on its completion notice, and each parse that adopts the cache pays
    // for its own walk.
    assert!(
        walks >= 1,
        "an adopted cache is still cross-examined: agreement is proven, not assumed"
    );
}

#[tokio::test]
async fn a_rar_shaped_quick_open_cache_is_ignored_and_still_routes() {
    // The cache shape a real archiver actually writes: file-header records and
    // nothing else. unrar adopts a Quick Open list only when the list closes
    // with a cached end-of-archive record, so this one is read and thrown away
    // and the members come from the physical walk — which is what makes the
    // other two tests in this section fixtures rather than field reports.
    //
    // The forged entry is the proof the cache was dropped rather than merely
    // agreed with: it names a member no physical header describes, and the
    // library reports it in `a_forged_quick_open_member_never_reaches_the_router`
    // where the same block does close with an end record.
    let member_name = "Silver.Horizon.S01E63.mkv";
    let forged_name = "Silver.Horizon.S01E63.forged";
    let payload: Vec<u8> = (0..400u32).map(|index| (index % 251) as u8).collect();
    let volumes = quick_open_store_set_shaped(member_name, &payload, Some(forged_name), false);

    assert_eq!(
        library_default_member_names(&volumes[1].1),
        vec![member_name.to_string()],
        "an unclosed Quick Open list must be ignored, leaving the physical member alone"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let arrivals = in_order_arrivals(volumes.len());
    let (shape, walks) =
        run_direct_store_routing_only_counting_walks(&temp_dir, JobId(41103), &volumes, &arrivals)
            .await;

    assert!(
        !shape.contains("Demoted"),
        "a locator whose cache the library never adopted is not a disagreement, got {shape}"
    );
    assert_eq!(
        walks, 0,
        "a cache the library rejected already came from a physical walk; the cross-check \
         must not pay for a second one"
    );
    assert!(
        !direct_partial(&temp_dir, JobId(41103), forged_name).exists(),
        "and nothing may be created for the entry only the discarded cache claimed"
    );
    assert!(
        !direct_partial(&temp_dir, JobId(41103), member_name).exists(),
        "the physical member's partial must have been committed, not left behind"
    );
    let routed = std::fs::read(payload_root(&temp_dir, JobId(41103)).join(member_name))
        .expect("the physical member routed to its own destination and committed");
    assert_eq!(
        routed, payload,
        "routing must complete over the physically walked layout"
    );
}

#[tokio::test]
async fn an_honest_quick_open_cache_waits_for_a_hole_the_walk_cannot_cross() {
    // The cache sits at the tail of the volume, so it can be staged — end
    // record and all — while a file header in the middle is still in flight.
    // The library adopts the cache; the physical walk stops at the hole with
    // one member fewer. Before this was told apart from a forged entry, an
    // honest `-qo` set demoted on nothing but article arrival order.
    let split_name = "Silver.Horizon.S01E64.mkv";
    let whole_name = "Silver.Horizon.S01E64.srt";
    let split_payload: Vec<u8> = (0..500u32).map(|index| (index % 251) as u8).collect();
    let whole_payload: Vec<u8> = (0..40u32).map(|index| (index % 13) as u8).collect();
    let (volumes, whole_header_offset) =
        quick_open_two_member_store_set(split_name, &split_payload, whole_name, &whole_payload);
    assert_eq!(
        library_default_member_names(&volumes[1].1),
        vec![split_name.to_string(), whole_name.to_string()],
        "the cache is honest: it reports exactly the two physical members"
    );

    // Three articles per volume: the second member's header lies in the
    // middle one, the cache in the last.
    const ARTICLES: usize = 3;
    let second_len = volumes[1].1.len();
    let (middle_start, middle_end) = article_extent(second_len, 1, ARTICLES);
    assert!(
        (middle_start as u64..middle_end as u64).contains(&whole_header_offset),
        "the fixture must put the second header inside the middle article \
         ({middle_start}..{middle_end}), got {whole_header_offset}"
    );
    assert!(
        article_extent(second_len, 2, ARTICLES).0 as u64 <= QOPEN_OFFSET,
        "the fixture must put the QO block inside the last article"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41104);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = direct_store_job_spec_with_articles("Silver Horizon", &volumes, ARTICLES);
    let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for segment_number in 0..ARTICLES as u32 {
        submit_volume_article_of(&mut pipeline, job_id, &volumes, 0, segment_number, ARTICLES)
            .await;
    }
    // The last volume's tail — end record and cache — lands before its middle.
    submit_volume_article_of(&mut pipeline, job_id, &volumes, 1, 0, ARTICLES).await;
    submit_volume_article_of(&mut pipeline, job_id, &volumes, 1, 2, ARTICLES).await;
    // The tail is staged, so the library *could* answer the whole layout from
    // the cache — and the set never asks it to. The walk that stopped at the
    // hole said which byte it needs, the tail is not that byte, and a parse
    // that cannot reach it is not repeated (`VolumeStaging::parse_short_at`).
    // The cross-check is what would have refused those cache-derived facts, so
    // not paying for either walk is the same verdict reached earlier: what
    // must not change is that nothing is adopted and nothing demotes, and both
    // are asserted below.
    let walks_before_the_hole_filled = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the set is still routing")
        .router
        .quick_open_walks();
    assert_eq!(
        walks_before_the_hole_filled, 0,
        "a parse that cannot reach the byte it stopped at is not repeated, so \
         there is nothing for the cross-check to answer yet"
    );
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "a walk that stopped at a hole is not a disagreement, got {shape}"
    );
    assert!(
        !direct_partial(&temp_dir, job_id, whole_name).exists()
            && !payload_root(&temp_dir, job_id).join(whole_name).exists(),
        "nothing may be adopted for a member only the cache has described so far"
    );

    submit_volume_article_of(&mut pipeline, job_id, &volumes, 1, 1, ARTICLES).await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "once the hole is filled the walk agrees with the cache, got {shape}"
    );
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .map(|set| set.router.quick_open_walks())
            .unwrap_or_default()
            >= 1,
        "and the article that filled it is the one that pays for the \
         cross-check"
    );
    for (name, payload) in [(split_name, &split_payload), (whole_name, &whole_payload)] {
        let routed = std::fs::read(payload_root(&temp_dir, job_id).join(name))
            .unwrap_or_else(|error| panic!("{name} routed and committed: {error}"));
        assert_eq!(
            &routed, payload,
            "{name} must route byte-exact over the physical layout"
        );
    }
}

#[tokio::test]
async fn direct_store_output_is_byte_identical_to_the_conventional_extractor() {
    let member_name = "Silver.Horizon.S01E01.mkv";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 251) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 4);
    let arrivals = in_order_arrivals(volumes.len());

    let conventional = run_direct_store_gate(
        DirectStoreGate::Disabled,
        JobId(41001),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;
    let direct = run_direct_store_gate(
        DirectStoreGate::Enabled,
        JobId(41002),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;

    // Non-vacuity: the conventional gate really did materialize volumes, so
    // "no volume file" below is a property of routing, not of the harness.
    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written source volumes"
    );
    assert!(
        !direct.volume_file_seen,
        "direct routing must never create a source volume file"
    );

    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional extractor should reproduce the member payload"
    );
    assert_eq!(
        conventional.member_location,
        Some("complete"),
        "the conventional extractor moves a finished member to the complete directory"
    );
    assert_eq!(
        (direct.member, direct.member_location, direct.status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "direct-store output must be byte-identical to the conventional extractor, \
         in the same directory, with the same job status"
    );
}

#[tokio::test]
async fn direct_store_routes_payload_that_lands_before_its_volume_header() {
    let member_name = "Silver.Horizon.S01E02.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 197) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    // Every volume's second article (pure payload) first, then the headers.
    let mut arrivals: Vec<(u32, u32)> = (0..volumes.len() as u32).map(|index| (index, 1)).collect();
    arrivals.extend((0..volumes.len() as u32).map(|index| (index, 0)));

    let direct = run_direct_store_gate(
        DirectStoreGate::Enabled,
        JobId(41003),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;

    assert!(!direct.volume_file_seen);
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "held bytes must drain to their destination once the header resolves"
    );
    assert_eq!(direct.member_location, Some("complete"));
    assert!(matches!(direct.status, Some(JobStatus::Complete)));
}

#[tokio::test]
async fn direct_store_ignores_a_duplicate_article() {
    let member_name = "Silver.Horizon.S01E03.mkv";
    let payload: Vec<u8> = (0..1600u32).map(|index| (index % 131) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let mut arrivals = in_order_arrivals(volumes.len());
    // Re-deliver every article a second time.
    arrivals.extend(in_order_arrivals(volumes.len()));

    let direct = run_direct_store_gate(
        DirectStoreGate::Enabled,
        JobId(41004),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;

    assert!(!direct.volume_file_seen);
    assert_eq!(direct.member.as_deref(), Some(payload.as_slice()));
    assert_eq!(direct.member_location, Some("complete"));
    assert!(matches!(direct.status, Some(JobStatus::Complete)));
}

#[tokio::test]
async fn a_clean_three_article_member_stays_virtual_and_routes_once() {
    let member_name = "Silver.Horizon.S01E04.Clean.mkv";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(410051);

    let (mut pipeline, working_dir) =
        route_articles_as_dispatched(&temp_dir, job_id, &volumes, 3).await;

    assert!(
        format!("{:?}", pipeline.direct_store.sets_for(job_id)).contains("Finalized"),
        "the clean twin must finish without demoting"
    );
    assert!(
        volumes
            .iter()
            .all(|(filename, _)| !working_dir.join(filename).exists()),
        "clean direct routing must not materialize either source volume"
    );
    assert_eq!(
        std::fs::read(payload_root(&temp_dir, job_id).join(member_name))
            .ok()
            .as_deref(),
        Some(payload.as_slice()),
        "the clean twin must retain the exact routed member"
    );
    assert_eq!(queued_segments(&mut pipeline, job_id), Vec::new());
}

#[tokio::test]
async fn member_checksum_demotion_hands_the_live_tail_to_a_reconstructed_prefix() {
    let member_name = "Silver.Horizon.S01E04.mkv";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 173) as u8).collect();
    let mut volumes = single_member_store_set(member_name, &payload, 2);

    // The final volume's header closes the member chain before its payload has
    // arrived. Corrupt a payload byte in article three so the fused member CRC
    // refuses that article inside `route`, before direct ownership transfers.
    let end_header_len = build_test_rar_end_header(false).len();
    let corrupt_at = volumes[1].1.len() - end_header_len - 1;
    assert!(
        corrupt_at >= article_extent(volumes[1].1.len(), 2, 3).0,
        "the corruption must live in the triggering article"
    );
    volumes[1].1[corrupt_at] ^= 0xFF;

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41005);
    let (mut pipeline, working_dir) =
        route_articles_as_dispatched(&temp_dir, job_id, &volumes, 3).await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));

    assert!(
        shape.contains("Demoted(MemberChecksumMismatch)"),
        "the whole-member gate should have demoted the set, got {shape}"
    );
    for (filename, posted) in &volumes {
        assert_eq!(
            std::fs::read(working_dir.join(filename)).ok().as_deref(),
            Some(posted.as_slice()),
            "reconstruction plus the in-hand final article must reproduce {filename}"
        );
    }
    let state = pipeline.jobs.get(&job_id).unwrap();
    assert!(
        (0..volumes.len() as u32).all(|file_index| {
            state
                .assembly
                .file(NzbFileId { job_id, file_index })
                .is_some_and(|file| file.is_complete())
        }),
        "conventional assembly must own and complete both demoted volumes"
    );
    let (tail_start, tail_end) = article_extent(volumes[1].1.len(), 2, 3);
    assert_eq!(
        state
            .assembly
            .file(NzbFileId {
                job_id,
                file_index: 1,
            })
            .and_then(|file| file.placement_of(2)),
        Some((tail_start as u64, (tail_end - tail_start) as u32)),
        "the in-hand article must restore the placement erased by demotion reset"
    );
    assert_eq!(
        state.downloaded_bytes,
        volumes
            .iter()
            .map(|(_, bytes)| bytes.len() as u64)
            .sum::<u64>(),
        "the triggering article remains counted exactly once"
    );
    assert!(
        !pipeline.write_buffers.contains_key(&NzbFileId {
            job_id,
            file_index: 0,
        }),
        "a fully reconstructed volume must not retain an empty write buffer"
    );
    assert_eq!(
        queued_segments(&mut pipeline, job_id),
        Vec::<(u32, u32)>::new(),
        "no article, especially the triggering one, should be refetched"
    );
    assert!(
        !pipeline
            .pending_retries_by_segment
            .contains_key(&SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 1,
                },
                segment_number: 2,
            }),
        "the triggering article must not enter delayed retry state"
    );
    assert!(
        !payload_root(&temp_dir, job_id).join(member_name).exists(),
        "a member failing its whole-member gate must not be committed as if it passed"
    );
    assert!(
        !direct_partial(&temp_dir, job_id, member_name).exists(),
        "demotion must delete the set's partial direct output"
    );
    assert_eq!(
        pipeline.direct_store.pending_materialization_files(job_id),
        0,
        "durable conventional completion must clear the demotion gate"
    );
}

// ---------------------------------------------------------------------------
// The confirming parse
// ---------------------------------------------------------------------------

/// The first shape's
/// `direct_store_refuses_a_set_whose_last_volume_hides_a_second_member`,
/// upgraded: the hidden member is now **adopted** rather than demoting the set.
///
/// The first shape had two reasons to demote here — the layout refused the
/// re-add, and even if it had not, a second routable member was out of scope.
/// Both are gone now: the router rebuilds its layout from every volume's newest
/// facts when a longer prefix reveals a header, and routes as many members as
/// the archive has.
#[tokio::test]
async fn a_member_hiding_past_the_first_is_adopted_and_routes_direct() {
    let member_name = "Silver.Horizon.S01E07.mkv";
    let tail_name = "Silver.Horizon.nfo";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 251) as u8).collect();
    let tail = b"invented release notes for an invented release".to_vec();
    let volumes =
        store_set_with_a_member_hidden_past_the_first(member_name, &payload, tail_name, &tail);
    let arrivals = in_order_arrivals(volumes.len());

    // Non-vacuity: the hidden header really is out of reach of the last
    // volume's first article, so the provisional parse cannot see it.
    let last = &volumes[1].1;
    let first_article_end = last.len().div_ceil(2);
    let hidden_header_at = last
        .windows(tail_name.len())
        .position(|window| window == tail_name.as_bytes())
        .expect("the fixture must contain the second member's header");
    assert!(
        hidden_header_at > first_article_end,
        "the second member's header must land in the last volume's second article"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(41010);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, segment_number) in &arrivals {
        submit_volume_article(
            &mut pipeline,
            job_id,
            &volumes,
            *file_index,
            *segment_number,
        )
        .await;
    }

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "the confirming parse must adopt the hidden member rather than demoting, got {shape}"
    );
    assert!(
        shape.contains("Finalized"),
        "both members pass their gates, so the set finalizes, got {shape}"
    );

    for (name, expected) in [
        (member_name, payload.as_slice()),
        (tail_name, tail.as_slice()),
    ] {
        assert_eq!(
            std::fs::read(
                crate::pipeline::Pipeline::member_output_paths(
                    &payload_root(&temp_dir, JobId(41010)),
                    name
                )
                .0
            )
            .ok()
            .as_deref(),
            Some(expected),
            "{name} must be committed byte for byte"
        );
    }
    for (filename, _) in &volumes {
        assert!(
            !working_dir.join(filename).exists(),
            "adopting a hidden member must not cost the set its volumes"
        );
    }
}

// ---------------------------------------------------------------------------
// PAR2 over virtual volumes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn demotion_materialization_gate_is_scoped_to_its_bound_par2_set() {
    let first_member = "Silver.Horizon.S01E30.mkv";
    let second_member = "Amber.Sky.S01E30.mkv";
    let first_payload: Vec<u8> = (0..1800u32).map(|index| (index % 173) as u8).collect();
    let second_payload: Vec<u8> = (0..1800u32).map(|index| (index % 181) as u8).collect();
    let first = single_member_store_set(first_member, &first_payload, 2);
    let second: Vec<(String, Vec<u8>)> = single_member_store_set(second_member, &second_payload, 2)
        .into_iter()
        .map(|(filename, bytes)| (filename.replace("silver.horizon", "amber.sky"), bytes))
        .collect();
    let first_par2 = par2_index_over_volumes(&first);
    let second_par2 = par2_index_over_volumes(&second);
    let mut volumes = first.clone();
    volumes.extend(second.clone());

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41058);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let mut spec = direct_store_job_spec("Two direct sets", &volumes);
    let first_index = append_par2_index(&mut spec, &first_par2);
    let second_index = spec.files.len() as u32;
    let second_index_name = "amber.sky.par2".to_string();
    spec.total_bytes += u64::from(yenc_declared_bytes(second_par2.len() as u32));
    spec.files.push(FileSpec {
        role: FileRole::from_filename(&second_index_name),
        filename: second_index_name,
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: yenc_declared_bytes(second_par2.len() as u32),
            message_id: "second-direct-par2-index@example.com".to_string(),
        }],
    });
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    deliver_par2_index(&mut pipeline, job_id, first_index, &first_par2).await;
    deliver_par2_index(&mut pipeline, job_id, second_index, &second_par2).await;

    take_queued_segment(
        &mut pipeline,
        job_id,
        SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number: 0,
        },
    );
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    let first_set_index = pipeline
        .direct_store
        .sets_for(job_id)
        .iter()
        .position(|set| {
            set.plan()
                .volumes
                .values()
                .any(|file_index| *file_index == 0)
        })
        .expect("the first archive set was admitted");
    std::fs::remove_file(working_dir.join("silver.horizon.f0.vol00000.envelope")).unwrap();
    pipeline
        .demote_direct_set(
            job_id,
            first_set_index,
            DemotionReason::MemberChecksumMismatch,
        )
        .await;
    settle_direct_post_repair_work(&mut pipeline).await;

    let first_set_id = pipeline
        .resolve_par2_file_binding(NzbFileId {
            job_id,
            file_index: 0,
        })
        .expect("first volume binding")
        .recovery_set_id;
    let second_set_id = pipeline
        .resolve_par2_file_binding(NzbFileId {
            job_id,
            file_index: first.len() as u32,
        })
        .expect("second volume binding")
        .recovery_set_id;
    assert_ne!(first_set_id, second_set_id);
    assert_eq!(pipeline.par2_served_set_id(job_id), Some(first_set_id));
    let _drained = queued_segments(&mut pipeline, job_id);
    let verifies_before = pipeline.par2_authoritative_verify_calls;
    pipeline.check_job_completion(job_id).await;
    assert_eq!(
        pipeline.par2_authoritative_verify_calls, verifies_before,
        "the shared completion funnel must stop before starting PAR2"
    );
    assert_eq!(
        pipeline.direct_store.pending_materialization_files(job_id),
        first.len(),
        "the first set remains pending while its rescued articles are queued"
    );
    assert!(
        !pipeline.demoted_materializations_ready_for_par2(job_id, first_set_id),
        "the demoted set's own PAR2 verdict must wait"
    );
    let original_identity = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .file_identities
        .insert(
            0,
            crate::jobs::record::ActiveFileIdentity {
                file_index: 0,
                source_filename: first[0].0.clone(),
                current_filename: first[0].0.clone(),
                canonical_filename: Some(first[1].0.clone()),
                classification: None,
                classification_source: crate::jobs::record::FileIdentitySource::Par2,
            },
        );
    assert!(
        pipeline
            .resolve_par2_file_binding(NzbFileId {
                job_id,
                file_index: 0,
            })
            .is_none(),
        "the conflicting canonical candidate must make this pending set unresolved"
    );
    assert!(
        !pipeline.demoted_materializations_ready_for_par2(job_id, second_set_id),
        "an unresolved pending set must conservatively block the served set"
    );
    if let Some(identity) = original_identity {
        pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .file_identities
            .insert(0, identity);
    } else {
        pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .file_identities
            .remove(&0);
    }
    assert!(
        pipeline.demoted_materializations_ready_for_par2(job_id, second_set_id),
        "an unrelated bound recovery set must not wait behind the demotion"
    );
    pipeline.clear_par2_runtime_state(job_id);
    assert_eq!(
        pipeline.direct_store.pending_materialization_files(job_id),
        0,
        "the shared cancellation/failure/teardown seam must clear the gate"
    );
}

#[tokio::test]
async fn a_par2_bearing_direct_job_completes_byte_identically_and_never_writes_a_volume() {
    // An earlier shape refused this job at admission
    // (`direct_store.refused.par2_present`) because every PAR2 path read the
    // volume files routing never creates. The `FileAccess` adapter answers
    // those reads out of the envelope plus the routed member bytes, so the
    // refusal is gone and this test states the new behaviour: the set routes,
    // the job verifies against *virtual* volumes, and the output is what the
    // conventional extractor would have produced.
    let member_name = "Silver.Horizon.S01E08.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 199) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let conventional = run_par2_direct_gate(
        DirectStoreGate::Disabled,
        JobId(41011),
        member_name,
        &volumes,
    )
    .await;
    let direct = run_par2_direct_gate(
        DirectStoreGate::Enabled,
        JobId(41012),
        member_name,
        &volumes,
    )
    .await;

    assert!(
        direct.admitted,
        "a par2-bearing job must admit its sets now that the verifier can read them virtually"
    );
    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written source volumes"
    );
    assert!(
        !direct.volume_file_seen,
        "a par2-bearing direct job must never create a source volume file, got {}",
        direct.demotions
    );
    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional extractor should reproduce the member payload"
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
        "a par2-bearing direct job must produce the conventional gate's output, \
         in the same place, with the same status; sets = {}",
        direct.demotions
    );
    assert!(
        matches!(direct.status, Some(JobStatus::Complete)),
        "the job should have completed, got {:?} with sets {}",
        direct.status,
        direct.demotions
    );
    assert!(
        direct.demotions.contains("Finalized"),
        "the set should have finalized once verification cleared it, got {}",
        direct.demotions
    );
    // The point: verification finishes with the download. The live
    // short-circuit may have fired, or the direct quiet pass may have reached
    // and settled a clean verdict on its own, or the conventional
    // authoritative pass may have run against the virtual volumes — all three
    // are wave-2 behaviour, and a job that took none of them would have
    // failed the byte comparison above against zero files.
    assert!(
        direct.verdict_reached,
        "the job must have reached a PAR2 verdict; authoritative={}",
        direct.authoritative_verify_calls
    );
}

#[tokio::test]
async fn an_obfuscated_rar_set_admits_from_par2_identity_and_never_writes_a_volume() {
    // The defect this states the fix for: a job whose files are all hex names
    // carries no `RarVolume` role, so spec discovery admits nothing and the
    // examined latch used to settle that forever. With the index parsed before
    // the first data article — the promoted-metadata shape — the descriptions
    // supply the roster and every volume binds by its own first bytes.
    let member_name = "Silver.Horizon.S01E08.mkv";
    let payload: Vec<u8> = (0..120_000u32).map(|index| (index % 199) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let arrivals = in_order_arrivals(volumes.len());

    let conventional = run_obfuscated_par2_gate(
        DirectStoreGate::Disabled,
        JobId(41090),
        member_name,
        &volumes,
        true,
        &arrivals,
    )
    .await;
    let direct = run_obfuscated_par2_gate(
        DirectStoreGate::Enabled,
        JobId(41091),
        member_name,
        &volumes,
        true,
        &arrivals,
    )
    .await;

    assert!(
        direct.admitted,
        "an obfuscated set must admit from PAR2 identity, got sets {}",
        direct.demotions
    );
    assert!(
        conventional.volume_file_seen,
        "the conventional gate should have written source volumes"
    );
    assert!(
        !direct.volume_file_seen,
        "an identity-admitted job must never create a source volume file, got {}",
        direct.demotions
    );
    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional gate should reproduce the member payload"
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
        "the identity-admitted job must produce the conventional gate's output; sets = {}",
        direct.demotions
    );
    assert!(
        matches!(direct.status, Some(JobStatus::Complete)),
        "the job should have completed, got {:?} with sets {}",
        direct.status,
        direct.demotions
    );
    assert!(
        direct.demotions.contains("Finalized"),
        "the set should have finalized once verification cleared it, got {}",
        direct.demotions
    );
}

#[tokio::test]
async fn identity_admission_refuses_once_volume_bytes_landed_conventionally() {
    // The index arriving after the data is the boundary of the roster rung:
    // every volume already has conventional bytes on disk, so no binding may
    // be made — the envelope model owns all of a routed volume's bytes or
    // none of them. RAR4 volumes, deliberately: a RAR5 set would be admitted
    // by the header rung before the index ever mattered, which is the better
    // outcome but not the boundary under test. The job must simply complete
    // the way it does today, with no admission and no wedge.
    let member_name = "Silver.Horizon.S01E09.mkv";
    let payload: Vec<u8> = (0..120_000u32).map(|index| (index % 197) as u8).collect();
    let volumes = single_member_rar4_store_set_numbered(member_name, &payload, 3);
    let arrivals = in_order_arrivals(volumes.len());

    let outcome = run_obfuscated_par2_gate(
        DirectStoreGate::Enabled,
        JobId(41092),
        member_name,
        &volumes,
        false,
        &arrivals,
    )
    .await;

    assert!(
        !outcome.admitted,
        "identity admission must refuse once volume bytes landed, got {}",
        outcome.demotions
    );
    assert!(
        outcome.volume_file_seen,
        "the volumes should have been written conventionally"
    );
    assert_eq!(
        outcome.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional fallback must still produce the member; status {:?}",
        outcome.status
    );
    assert!(
        matches!(outcome.status, Some(JobStatus::Complete)),
        "the job should have completed conventionally, got {:?}",
        outcome.status
    );
}

#[tokio::test]
async fn arming_schedules_a_probe_wave_ahead_of_candidate_payload() {
    // The dispatch half of identity admission: an obfuscated post's NZB order
    // scrambles the volume order, and streaming payload in that order piles
    // unplaceable member bytes into holds until the scratch ceiling demotes
    // the set. Arming therefore pulls every candidate's first article to the
    // front of the queue — binding every file within a few round trips — and
    // each binding then re-ranks its file to the name path's own
    // `10 + volume` priority, restoring in-order volume streaming.
    let member_name = "Silver.Horizon.S01E19.mkv";
    let payload: Vec<u8> = (0..120_000u32).map(|index| (index % 163) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);
    let obfuscated = obfuscate_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41101);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let (spec, index_file_index) =
        par2_bearing_job_spec("Silver Horizon", &obfuscated, &par2_bytes);
    insert_active_job(&mut pipeline, job_id, spec).await;
    deliver_par2_index(&mut pipeline, job_id, index_file_index, &par2_bytes).await;

    // The probe wave: after the PAR2 index (priority 0, still queued — the
    // harness delivered its bytes directly), the next three dispatches are
    // the three candidates' first articles, in file order, ahead of every
    // payload article.
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let index_work = state.download_queue.pop().expect("queued index work");
    assert_eq!(
        index_work.segment_id.file_id.file_index, index_file_index,
        "the declared index leads the queue"
    );
    for expected_file in 0..3u32 {
        let work = state.download_queue.pop().expect("queued work");
        assert_eq!(
            (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number
            ),
            (expected_file, 0),
            "the probe wave must lead the queue"
        );
        assert!(
            work.completion_critical,
            "the probe wave must stay ahead of completion-critical PAR2 work"
        );
    }

    // A binding re-ranks its file to the name path's volume priority.
    submit_volume_article(&mut pipeline, job_id, &obfuscated, 0, 0).await;
    assert!(
        !pipeline.direct_store.sets_for(job_id).is_empty(),
        "the probe article should have admitted the set"
    );
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let repriced = state
        .download_queue
        .peek_next_matching(|work| work.segment_id.file_id.file_index == 0)
        .map(|work| work.priority);
    assert_eq!(
        repriced,
        Some(10),
        "the bound file's payload must carry its volume's name-path priority"
    );
}

#[tokio::test]
async fn an_out_of_order_payload_article_no_longer_poisons_the_binding() {
    // The production shape that killed the first cut of this seam: on a wide
    // connection pool a file's later article routinely decodes before its
    // offset-zero article. The conventional path parks it in the write
    // reorder buffer — in memory, unwritten — so when the offset-zero
    // article then establishes the binding, the parked article is reclaimed
    // into the routed volume and the set streams whole. Nothing leaks, and
    // nothing demotes.
    let member_name = "Silver.Horizon.S01E10.mkv";
    let payload: Vec<u8> = (0..120_000u32).map(|index| (index % 193) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    // Volume 1's payload article arrives before its offset-zero article, and
    // volume 0's pair is inverted too, so both the admitting binding and a
    // later one exercise the reclaim.
    let arrivals = [(0, 1), (0, 0), (1, 1), (1, 0), (2, 0), (2, 1)];

    let outcome = run_obfuscated_par2_gate(
        DirectStoreGate::Enabled,
        JobId(41093),
        member_name,
        &volumes,
        true,
        &arrivals,
    )
    .await;

    assert!(
        outcome.admitted,
        "the set must admit despite out-of-order arrivals, got {}",
        outcome.demotions
    );
    assert!(
        !outcome.volume_file_seen,
        "reclaimed articles must route, never materialize a volume, got {}",
        outcome.demotions
    );
    assert!(
        outcome.demotions.contains("Finalized"),
        "the set should have finalized, got {}",
        outcome.demotions
    );
    assert_eq!(
        outcome.member.as_deref(),
        Some(payload.as_slice()),
        "the reclaimed set must reproduce the member payload; status {:?}",
        outcome.status
    );
    assert!(
        matches!(outcome.status, Some(JobStatus::Complete)),
        "the job should have completed, got {:?}",
        outcome.status
    );
}

#[tokio::test]
async fn an_unmatched_obfuscated_extra_stays_conventional_beside_an_identity_set() {
    // A junk extra — the nfo, the sample — matches no description. Its
    // offset-zero evaluation settles it as a non-member, it streams
    // conventionally, and the identity set is unaffected.
    let member_name = "Silver.Horizon.S01E11.mkv";
    let payload: Vec<u8> = (0..120_000u32).map(|index| (index % 191) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let par2_bytes = par2_index_over_volumes(&volumes);
    let obfuscated = obfuscate_volumes(&volumes);
    let junk: Vec<u8> = (0..20_000u32).map(|index| (index % 251) as u8).collect();

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41094);
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);

    let (mut spec, index_file_index) =
        par2_bearing_job_spec("Silver Horizon", &obfuscated, &par2_bytes);
    let junk_index = spec.files.len() as u32;
    let junk_name = format!("{:032x}", 0xd1c7_ffff_u128);
    spec.total_bytes += u64::from(yenc_declared_bytes(junk.len() as u32));
    spec.files.push(FileSpec {
        role: FileRole::from_filename(&junk_name),
        filename: junk_name.clone(),
        groups: vec!["alt.binaries.test".to_string()],
        posted_at_epoch: None,
        segments: vec![segment_spec! {
            number: 0,
            bytes: yenc_declared_bytes(junk.len() as u32),
            message_id: "obfuscated-junk@example.com".to_string(),
        }],
    });
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    deliver_par2_index(&mut pipeline, job_id, index_file_index, &par2_bytes).await;
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: junk_index,
        },
        0,
        0,
        &junk,
        &junk_name,
        None,
    )
    .await;
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(
            &mut pipeline,
            job_id,
            &obfuscated,
            file_index,
            segment_number,
        )
        .await;
    }
    assert!(
        !pipeline.direct_store.sets_for(job_id).is_empty(),
        "the identity set must admit despite the unmatched extra"
    );

    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    pipeline.check_job_completion(job_id).await;
    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    assert!(
        sets.contains("Finalized"),
        "the set should have finalized with the extra beside it, got {sets}"
    );
    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    let member = std::fs::read(output_root.join(member_name))
        .ok()
        .or_else(|| staging_member(&complete_dir, member_name));
    assert_eq!(
        member.as_deref(),
        Some(payload.as_slice()),
        "the identity set must produce the member beside the extra"
    );
    assert!(
        !obfuscated
            .iter()
            .chain(volumes.iter())
            .any(|(filename, _)| working_dir.join(filename).exists()),
        "no source volume file may exist for the finalized identity set"
    );
}

#[tokio::test]
async fn a_restored_job_with_a_conventional_floor_is_never_readmitted() {
    // The restart tear: restore rebuilds a conventional floor and commits the
    // skipped segments into the assembly, so the file on disk owns the
    // volume's prefix. Admitting the set at the next decoded segment would
    // route every remaining article into an envelope instead — the two
    // halves would never meet, and extraction would walk real headers into
    // a hole. The set must stay on the path that owns its bytes.
    let member_name = "Silver.Horizon.S01E20.mkv";
    let payload: Vec<u8> = (0..120_000u32).map(|index| (index % 157) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 1);
    let (volume_name, volume_bytes) = &volumes[0];
    // The spec's own first-article boundary, so the restored floor covers
    // exactly segment 0 and the run owes exactly segment 1.
    let (_, split_at) = article_extent(volume_bytes.len(), 0, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(41102);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = temp_dir.path().join("restored-floor");
    tokio::fs::create_dir_all(&working_dir).await.unwrap();
    // The prefix the previous run wrote conventionally, exactly as the
    // restart sweep would find it. The restore floor counts the spec's
    // yEnc-encoded segment sizes and clamps to the on-disk length, so the
    // file is extended to the encoded floor — the extension sits in the
    // segment-1 region, which the restored run rewrites below.
    let encoded_floor = u64::from(yenc_declared_bytes(split_at as u32));
    tokio::fs::write(working_dir.join(volume_name), &volume_bytes[..split_at])
        .await
        .unwrap();
    let partial = tokio::fs::File::options()
        .write(true)
        .open(working_dir.join(volume_name))
        .await
        .unwrap();
    partial.set_len(encoded_floor).await.unwrap();
    drop(partial);

    pipeline
        .restore_job(RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec,
            file_progress: HashMap::from([(0u32, encoded_floor)]),
            complete_files: HashSet::new(),
            detected_archives: HashMap::new(),
            file_identities: HashMap::new(),
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir: working_dir.clone(),
        })
        .await
        .unwrap();

    // The remaining article decodes on the restored run.
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: 0,
        },
        1,
        split_at as u64,
        &volume_bytes[split_at..],
        volume_name,
        None,
    )
    .await;

    assert!(
        pipeline.direct_store.sets_for(job_id).is_empty(),
        "a set whose volume already has conventional bytes must not admit, got {:?}",
        pipeline.direct_store.sets_for(job_id)
    );
    // A restored file's reorder cursor starts at zero with its skipped
    // segments never arriving, so the tail segment sits buffered until the
    // quiescent flusher (or backlog pressure) writes it at its own offset —
    // the same drain a real restored run relies on. The harness supplied the
    // article directly, so the queued copy is cleared first, exactly as the
    // other whole-job gates model an exhausted dispatcher.
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    pipeline.flush_quiescent_write_backlog().await;
    let on_disk = std::fs::read(working_dir.join(volume_name)).unwrap();
    let first_diff = on_disk
        .iter()
        .zip(volume_bytes.iter())
        .position(|(a, b)| a != b);
    assert!(
        on_disk == *volume_bytes,
        "the restored file must receive the remaining bytes and stay whole          (on_disk_len={} expected_len={} first_diff={:?} split_at={split_at})",
        on_disk.len(),
        volume_bytes.len(),
        first_diff,
    );
}

#[tokio::test]
async fn an_obfuscated_rar4_set_admits_from_par2_identity() {
    // The fingerprint rung is format-blind: the descriptions carry the real
    // RAR4 names, name classification orders them, and the per-volume
    // fingerprint places each file — none of which needs a volume number in
    // the headers. This is exactly the set shape header-based ordering can
    // never place (RAR4 interior volumes are indistinguishable), so the
    // PAR2-identity route is the only streaming admission it will ever have.
    let member_name = "Silver.Horizon.S01E12.mkv";
    let payload: Vec<u8> = (0..120_000u32).map(|index| (index % 189) as u8).collect();
    let volumes = single_member_rar4_store_set(member_name, &payload, 3);
    let arrivals = in_order_arrivals(volumes.len());

    let direct = run_obfuscated_par2_gate(
        DirectStoreGate::Enabled,
        JobId(41095),
        member_name,
        &volumes,
        true,
        &arrivals,
    )
    .await;

    assert!(
        direct.admitted,
        "an obfuscated RAR4 set must admit from PAR2 identity, got {}",
        direct.demotions
    );
    assert!(
        !direct.volume_file_seen,
        "an identity-admitted RAR4 job must never create a source volume file, got {}",
        direct.demotions
    );
    assert!(
        direct.demotions.contains("Finalized"),
        "the RAR4 set should have finalized, got {}",
        direct.demotions
    );
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "the RAR4 identity job must reproduce the member payload; status {:?}",
        direct.status
    );
    assert!(
        matches!(direct.status, Some(JobStatus::Complete)),
        "the job should have completed, got {:?}",
        direct.status
    );
}

#[tokio::test]
async fn an_identity_binding_contradicted_by_the_volumes_own_number_demotes() {
    // The hardening: the fingerprint placed this file at volume 1, but the
    // volume's own main header declares position 2. One of the two is
    // describing a different archive, so the set demotes before the layout
    // adopts a member from the wrong position — and the fingerprints still
    // match, because they hash the bytes as posted, tampered header and all.
    let member_name = "Silver.Horizon.S01E13.mkv";
    let payload: Vec<u8> = (0..120_000u32).map(|index| (index % 187) as u8).collect();
    let mut volumes = single_member_store_set(member_name, &payload, 3);
    {
        let chunk = payload.len().div_ceil(3);
        let part = &payload[chunk..2 * chunk];
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&TEST_RAR5_SIG);
        // Volume 1's head, rebuilt to lie: it claims position 2.
        bytes.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(2)));
        bytes.extend_from_slice(&build_test_rar_file_header(
            member_name,
            0x0008 | 0x0010,
            part.len() as u64,
            payload.len() as u64,
            Some(checksum::crc32(part)),
        ));
        bytes.extend_from_slice(part);
        bytes.extend_from_slice(&build_test_rar_end_header(true));
        volumes[1].1 = bytes;
    }
    // Inline rather than through the gate harness: the demotion hands the
    // job to the conventional path, which can complete and prune the
    // direct-store runtime before an end-of-run snapshot would look, so the
    // verdict has to be read mid-run — right after the lying volume's first
    // article.
    let par2_bytes = par2_index_over_volumes(&volumes);
    let obfuscated = obfuscate_volumes(&volumes);
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41096);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let (spec, index_file_index) =
        par2_bearing_job_spec("Silver Horizon", &obfuscated, &par2_bytes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    deliver_par2_index(&mut pipeline, job_id, index_file_index, &par2_bytes).await;

    submit_volume_article(&mut pipeline, job_id, &obfuscated, 0, 0).await;
    assert!(
        !pipeline.direct_store.sets_for(job_id).is_empty(),
        "the set should admit on the honest first volume"
    );
    submit_volume_article(&mut pipeline, job_id, &obfuscated, 1, 0).await;
    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        sets.contains("Demoted(IdentityVolumeMismatch)"),
        "the declared number must contradict the binding and demote, got {sets}"
    );
    let _ = working_dir;
}

#[tokio::test]
async fn a_par2_less_obfuscated_rar5_set_admits_from_its_own_headers() {
    // The header rung: no PAR2 anywhere, hex names carrying nothing — the
    // only structure left is what each volume's own RAR5 main header states,
    // and that is a position. The set opens on the first volume, grows a
    // binding per file, closes when the final volume's end record declares
    // itself last, and finalizes like any other set.
    let member_name = "Silver.Horizon.S01E14.mkv";
    let payload: Vec<u8> = (0..120_000u32).map(|index| (index % 181) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let obfuscated = obfuscate_volumes(&volumes);
    // The very first decoded article is a payload article, not an
    // offset-zero one — the production arrival shape — so the admitting
    // binding itself runs the reorder-stage reclaim.
    let arrivals = [(0, 1), (0, 0), (1, 0), (1, 1), (2, 0), (2, 1)];

    let outcome =
        run_obfuscated_headers_gate(JobId(41097), member_name, &obfuscated, &arrivals).await;

    assert!(
        outcome.admitted,
        "a par2-less obfuscated RAR5 set must admit from its own headers, got {}",
        outcome.demotions
    );
    assert!(
        !outcome.volume_file_seen,
        "a header-admitted set must never create a source volume file, got {}",
        outcome.demotions
    );
    assert!(
        outcome.demotions.contains("Finalized"),
        "the header set should have closed and finalized, got {}",
        outcome.demotions
    );
    assert_eq!(
        outcome.member.as_deref(),
        Some(payload.as_slice()),
        "the header-admitted set must reproduce the member payload; status {:?}",
        outcome.status
    );
}

#[tokio::test]
async fn a_par2_less_obfuscated_standalone_rar5_admits_as_a_set_of_one() {
    // A single archive with no volume flag: a set of one, closed at
    // admission. The common single-rar obfuscated post.
    let member_name = "Silver.Horizon.S01E15.mkv";
    let payload: Vec<u8> = (0..60_000u32).map(|index| (index % 177) as u8).collect();
    let member_crc = checksum::crc32(&payload);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&TEST_RAR5_SIG);
    bytes.extend_from_slice(&build_test_rar_main_header(0, None));
    bytes.extend_from_slice(&build_test_rar_file_header(
        member_name,
        0,
        payload.len() as u64,
        payload.len() as u64,
        Some(member_crc),
    ));
    bytes.extend_from_slice(&payload);
    bytes.extend_from_slice(&build_test_rar_end_header(false));
    let obfuscated = vec![(format!("{:032x}", 0xd1c7_0100_u128), bytes)];

    let outcome =
        run_obfuscated_headers_gate(JobId(41098), member_name, &obfuscated, &[(0, 0), (0, 1)])
            .await;

    assert!(
        outcome.admitted,
        "a standalone obfuscated RAR5 must admit from its own head, got {}",
        outcome.demotions
    );
    assert!(
        !outcome.volume_file_seen,
        "the standalone archive must never materialize, got {}",
        outcome.demotions
    );
    assert_eq!(
        outcome.member.as_deref(),
        Some(payload.as_slice()),
        "the standalone archive must extract in-stream; status {:?}, sets {}",
        outcome.status,
        outcome.demotions
    );
}

#[tokio::test]
async fn a_par2_less_obfuscated_rar4_set_stays_conventional() {
    // The measured boundary, kept deliberately: RAR4 headers carry no volume
    // number and the interior volumes of a stored set are identical in every
    // field that could place one, so with no descriptions to consult there
    // is nothing to bind on. The header rung declines rather than guesses.
    let member_name = "Silver.Horizon.S01E16.mkv";
    let payload: Vec<u8> = (0..60_000u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_rar4_store_set(member_name, &payload, 3);
    let obfuscated = obfuscate_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41099);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = direct_store_job_spec("Silver Horizon", &obfuscated);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in in_order_arrivals(obfuscated.len()) {
        submit_volume_article(
            &mut pipeline,
            job_id,
            &obfuscated,
            file_index,
            segment_number,
        )
        .await;
    }

    assert!(
        pipeline.direct_store.sets_for(job_id).is_empty(),
        "obfuscated RAR4 without descriptions must not admit"
    );
    assert!(
        obfuscated
            .iter()
            .all(|(filename, _)| working_dir.join(filename).exists()),
        "every volume must have streamed conventionally to disk"
    );
}

#[tokio::test]
async fn interleaved_obfuscated_header_sets_are_refused_not_guessed() {
    // Two obfuscated RAR5 sets in one job both open with a volume claiming
    // position zero. The bytes carry positions but no set identity, so the
    // second claimant is indistinguishable interleaving: the one header set
    // demotes, the rung is poisoned against forming another, and everything
    // streams conventionally.
    let first = single_member_store_set(
        "Silver.Horizon.S01E18.mkv",
        &(0..60_000u32)
            .map(|index| (index % 171) as u8)
            .collect::<Vec<u8>>(),
        2,
    );
    let second = single_member_store_set(
        "Amber.Sky.S01E18.mkv",
        &(0..60_000u32)
            .map(|index| (index % 167) as u8)
            .collect::<Vec<u8>>(),
        2,
    );
    let mut volumes = first.clone();
    volumes.extend(second);
    let obfuscated = obfuscate_volumes(&volumes);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41100);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = direct_store_job_spec("Silver Horizon", &obfuscated);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    // First set's volume 0 admits; the second set's volume 0 (file index 2)
    // claims the same position and condemns the rung.
    for (file_index, segment_number) in [
        (0, 0),
        (2, 0),
        (0, 1),
        (1, 0),
        (1, 1),
        (2, 1),
        (3, 0),
        (3, 1),
    ] {
        submit_volume_article(
            &mut pipeline,
            job_id,
            &obfuscated,
            file_index,
            segment_number,
        )
        .await;
    }

    let sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        sets.contains("Demoted(IdentityRosterUnfillable)"),
        "the ambiguous header set must demote, got {sets}"
    );
    assert_eq!(
        pipeline.direct_store.sets_for(job_id).len(),
        1,
        "poisoning must prevent a second header set from forming, got {sets}"
    );
    assert!(
        obfuscated
            .iter()
            .all(|(filename, _)| working_dir.join(filename).exists()),
        "every volume must end up conventional after the refusal"
    );
}

#[tokio::test]
async fn par2_damage_a_direct_set_cannot_see_demotes_it_and_ends_where_the_conventional_gate_does()
{
    let member_name = "Silver.Horizon.S01E14.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 211) as u8).collect();
    let rr_bytes = 512;
    let clean = recovery_record_store_set(member_name, &payload, 3, rr_bytes);
    // The PAR2 set describes the *clean* volumes; the job downloads damaged
    // ones. Building the index first is what makes the damage detectable.
    let par2_bytes = par2_index_over_volumes(&clean);
    let mut volumes = clean.clone();
    damage_recovery_record(&mut volumes, 1, rr_bytes);

    let (conventional, conventional_sets) = run_damaged_par2_gate(
        DirectStoreGate::Disabled,
        JobId(41031),
        member_name,
        &volumes,
        &par2_bytes,
    )
    .await;
    let (direct, direct_sets) = run_damaged_par2_gate(
        DirectStoreGate::Enabled,
        JobId(41032),
        member_name,
        &volumes,
        &par2_bytes,
    )
    .await;

    assert!(
        conventional_sets == "[]",
        "the conventional gate should never have admitted a set, got {conventional_sets}"
    );
    assert!(
        direct_sets.contains("Demoted(Par2Damaged)"),
        "PAR2 verification must catch damage the RAR and yEnc gates cannot see, on a \
         volume that only ever existed virtually, and demote — repairing a virtual \
         volume is phase 6 — got {direct_sets}"
    );
    assert!(
        direct.rearmed_after_demotion,
        "the demotion must re-arm the completion check on its way out (M5): the \
         materialized volumes are already on disk, and leaving the job's next move \
         to the 30 s reconcile sweep is a stall, not a wait"
    );
    // Non-vacuity, two ways. First: the same fixture *without* the damaged byte
    // runs clean through this very harness, so the outcome below is caused by
    // the damage rather than by the harness.
    let (clean_direct, clean_sets) = run_damaged_par2_gate(
        DirectStoreGate::Enabled,
        JobId(41033),
        member_name,
        &clean,
        &par2_bytes,
    )
    .await;
    assert!(
        matches!(clean_direct.status, Some(JobStatus::Complete)),
        "the undamaged fixture must complete through the same harness, got {:?} with \
         sets {clean_sets}",
        clean_direct.status
    );
    assert!(
        !clean_sets.contains("Demoted"),
        "an undamaged par2-bearing direct set must never demote, got {clean_sets}"
    );
    // Second: the conventional gate saw the same damage — the set is
    // unrepairable (the fixture's PAR2 carries no recovery blocks), so it does
    // not complete either.
    assert!(
        !matches!(conventional.status, Some(JobStatus::Complete)),
        "the conventional gate should have been stopped by the same damage, got {:?}",
        conventional.status
    );
    assert!(
        direct.volume_files.iter().all(Option::is_some),
        "the demotion must materialize every volume for the conventional path to \
         repair, got {:?}",
        direct
            .volume_files
            .iter()
            .map(Option::is_some)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        direct.volume_files, conventional.volume_files,
        "the volumes demotion reconstructed from the set's own routed bytes must be \
         byte-identical to the ones the conventional gate downloaded — damage included, \
         since reconstruction rebuilds what arrived and does not judge it; \
         sets = {direct_sets}"
    );
    assert_eq!(
        direct.member, conventional.member,
        "neither gate may produce a member out of an unrepairable set; sets = {direct_sets}"
    );
    assert!(
        !matches!(direct.status, Some(JobStatus::Complete)),
        "the direct gate must not complete an unrepairable job, got {:?}",
        direct.status
    );
    // Job *status* is deliberately not compared. This fixture's PAR2 carries
    // descriptions and slice checksums but no recovery blocks — the test
    // harness has no builder for a parseable multi-file recovery stream — so
    // neither gate can repair, and the two reach that dead end through
    // different waits (the direct gate through its demotion, the conventional
    // one through the targeted-recovery wait). What verification owns is the
    // verdict and the bytes, and both are asserted above; repair-to-success is
    // the repair differential.
}

// ---------------------------------------------------------------------------
// Destinations, finalization and demotion accounting
// ---------------------------------------------------------------------------

#[tokio::test]
async fn direct_store_routes_a_member_stored_inside_a_directory() {
    // The partial lives beside the member's eventual destination, so the first
    // routed byte opens a path whose parent directory nothing else creates.
    let member_name = "Silver.Horizon/S01E06.mkv";
    let payload: Vec<u8> = (0..2600u32).map(|index| (index % 241) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let arrivals = in_order_arrivals(volumes.len());

    let direct = run_direct_store_gate(
        DirectStoreGate::Enabled,
        JobId(41013),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;

    assert!(!direct.volume_file_seen);
    assert_eq!(
        direct.member.as_deref(),
        Some(payload.as_slice()),
        "a member inside a directory must route and finalize like any other"
    );
    assert_eq!(direct.member_location, Some("complete"));
    assert!(matches!(direct.status, Some(JobStatus::Complete)));
}

#[tokio::test]
async fn a_duplicate_article_after_finalization_leaves_the_finished_output_alone() {
    let member_name = "Silver.Horizon.S01E09.mkv";
    let payload: Vec<u8> = (0..1800u32).map(|index| (index % 137) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(41014);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Finalized"),
        "the set should have finalized before the duplicate arrives, got {shape}"
    );
    let member_path = crate::pipeline::Pipeline::member_output_paths(
        &payload_root(&temp_dir, JobId(41014)),
        member_name,
    )
    .0;
    let before = std::fs::read(&member_path).expect("the member is committed at finalization");
    assert_eq!(before, payload);

    // A late duplicate has nowhere to go: the partial it would be routed into
    // has already been renamed, and writing it conventionally would create the
    // source volume the whole design exists to avoid.
    submit_volume_article(&mut pipeline, job_id, &volumes, 1, 1).await;

    assert_eq!(
        std::fs::read(&member_path).ok().as_deref(),
        Some(payload.as_slice()),
        "a duplicate after finalization must not disturb the committed member"
    );
    for (filename, _) in &volumes {
        assert!(
            !working_dir.join(filename).exists(),
            "a duplicate after finalization must not materialize {filename}"
        );
    }
    assert!(
        !direct_partial(&temp_dir, JobId(41014), member_name).exists(),
        "a duplicate after finalization must not recreate the partial"
    );
}

#[tokio::test]
async fn a_demotion_returns_before_its_reconstruction_sweep_finishes() {
    let member_name = "Silver.Horizon.S01E24.mkv";
    let volumes = demotion_fixture_volumes(member_name);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41058);
    let (mut pipeline, working_dir, _) = demote_mid_download_leaving_the_sweep_outstanding(
        &temp_dir,
        job_id,
        &volumes,
        DemotionReason::HoldsBudgetExceeded,
        |_, _| {},
    )
    .await;

    // The sweep reads gigabytes on a real archive, and the demotion is reached
    // from the decode of the article that triggered it. It must be a ticket:
    // one per demoted set, fenced by the set it belongs to.
    assert_eq!(
        pipeline
            .direct_demotion_in_flight
            .get(&job_id)
            .map(|sets| sets.keys().copied().collect::<Vec<_>>()),
        Some(vec![0]),
        "the demotion must return with its reconstruction sweep still outstanding"
    );
    // Nothing durable has happened yet. The member partial is the sweep's own
    // input, and it is deleted only once every volume's floor is committed and
    // the coverage row is retired — all of which is the ticket's work.
    assert!(
        direct_partial(&temp_dir, job_id, member_name).exists(),
        "the routed output the sweep is reading may not be deleted before it lands"
    );

    settle_direct_post_repair_work(&mut pipeline).await;

    assert!(
        pipeline.direct_demotion_in_flight.is_empty(),
        "the ticket is retired by its own completion"
    );
    assert_eq!(
        std::fs::read(working_dir.join(&volumes[0].0))
            .ok()
            .as_deref(),
        Some(volumes[0].1.as_slice()),
        "and the volume the set had covered end to end is materialized then, byte for byte"
    );
    assert!(
        !direct_partial(&temp_dir, job_id, member_name).exists(),
        "with the routed output deleted behind it"
    );
}

#[tokio::test]
async fn the_archive_hook_leaves_a_volume_alone_while_its_demotion_sweep_is_outstanding() {
    // A demoted set's volumes stop being direct source files at the demotion,
    // so the conventional file-complete hook would otherwise classify and
    // probe one the moment an in-window article completes it — over an image
    // the detached sweep is still writing. The guard holds exactly for the
    // set's own volumes and exactly until the ticket lands; the handback then
    // replays the hook for every volume itself.
    let member_name = "Silver.Horizon.S01E25.mkv";
    let volumes = demotion_fixture_volumes(member_name);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41060);
    let (mut pipeline, _, _) = demote_mid_download_leaving_the_sweep_outstanding(
        &temp_dir,
        job_id,
        &volumes,
        DemotionReason::HoldsBudgetExceeded,
        |_, _| {},
    )
    .await;

    let set_volume_files: Vec<NzbFileId> = (0..16u32)
        .map(|file_index| NzbFileId { job_id, file_index })
        .filter(|file_id| {
            pipeline
                .direct_store
                .set(job_id, 0)
                .is_some_and(|set| set.plan().volume_for_file(file_id.file_index).is_some())
        })
        .collect();
    assert_eq!(set_volume_files.len(), volumes.len());
    for file_id in &set_volume_files {
        assert!(
            !pipeline.is_direct_source_file(*file_id),
            "a demoted set's volume is an ordinary file from the demotion on"
        );
        assert!(
            pipeline.demotion_sweep_owns_file(*file_id),
            "but the sweep owns it until the ticket lands"
        );
    }
    let other_file = NzbFileId {
        job_id,
        file_index: 16,
    };
    assert!(!pipeline.demotion_sweep_owns_file(other_file));
    assert!(!pipeline.demotion_sweep_owns_file(NzbFileId {
        job_id: JobId(41061),
        file_index: set_volume_files[0].file_index,
    }));

    settle_direct_post_repair_work(&mut pipeline).await;

    for file_id in &set_volume_files {
        assert!(
            !pipeline.demotion_sweep_owns_file(*file_id),
            "once the handback has run the hook must see the volume like any other file"
        );
    }
}

#[tokio::test]
async fn the_completion_gate_refuses_to_judge_a_job_whose_demotion_sweep_is_outstanding() {
    let member_name = "Silver.Horizon.S01E25.mkv";
    let volumes = demotion_fixture_volumes(member_name);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41059);
    let (mut pipeline, working_dir, _) = demote_mid_download_leaving_the_sweep_outstanding(
        &temp_dir,
        job_id,
        &volumes,
        DemotionReason::HoldsBudgetExceeded,
        |_, _| {},
    )
    .await;

    // Non-vacuity: empty the download queue, so the only thing left between this
    // job and a verdict is the sweep. Without the gate the check would read a
    // job whose volumes are neither routed nor on disk and call its files
    // unassemblable.
    let drained = queued_segments(&mut pipeline, job_id);
    assert!(
        !drained.is_empty(),
        "non-vacuity: the fixture must have had queued work to drain"
    );

    pipeline.check_job_completion(job_id).await;
    let status = pipeline
        .jobs
        .get(&job_id)
        .map(|state| state.status.clone())
        .expect("the job must still be active");
    assert!(
        !matches!(status, JobStatus::Failed { .. } | JobStatus::Complete),
        "a job with a demotion sweep outstanding must not be judged, got {status:?}"
    );

    // And the state the gate was protecting arrives with the ticket: until it
    // lands there is no volume on disk and no floor describing one, which is
    // exactly the reading that would have condemned the job above.
    settle_direct_post_repair_work(&mut pipeline).await;
    assert_eq!(
        std::fs::read(working_dir.join(&volumes[0].0))
            .ok()
            .as_deref(),
        Some(volumes[0].1.as_slice()),
        "the ticket's completion is what makes the demoted volumes real"
    );
}

/// The handoff that completes a RAR member can be the volume's *first* article,
/// arriving last: the tail articles were staged as holds, segment zero brought
/// the headers, the router placed everything, the chain closed, and the
/// member's CRC refused. That article's offset is already at the write cursor,
/// so with the sweep detached the seam used to write and commit it on the spot
/// — completing an assembly whose other articles were routed into the overlay
/// and existed nowhere else yet — and the whole-file CRC, judged over a file
/// one article long, failed the job. (Witnessed live as a mixed-grid two-set
/// posting failing with a yEnc whole-file CRC32 mismatch over 768000 of
/// 5243027 bytes, three milliseconds after its demotion ticket was submitted.)
#[tokio::test]
async fn a_handoff_at_the_write_cursor_waits_for_its_demotion_sweep() {
    let member_name = "Silver.Horizon.S01E27.mkv";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 173) as u8).collect();
    let mut volumes = single_member_store_set(member_name, &payload, 1);
    let articles = 3usize;
    // Damage in the middle article: every part CRC agrees with what was
    // posted, so only the member's own CRC — checked when the chain closes,
    // on the last article to arrive — can refuse the set.
    let (mid_start, _) = article_extent(volumes[0].1.len(), 1, articles);
    volumes[0].1[mid_start + 7] ^= 0xFF;
    let posted_crc = checksum::crc32(&volumes[0].1);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41060);
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = direct_store_job_spec_with_articles("Silver Horizon", &volumes, articles);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };

    // Tail first, segment zero last: the headers arrive with the article that
    // completes the member.
    let order: Vec<u32> = (1..articles as u32).chain(std::iter::once(0)).collect();
    for segment_number in order {
        take_queued_segment(
            &mut pipeline,
            job_id,
            SegmentId {
                file_id,
                segment_number,
            },
        );
        let (start, end) = article_extent(volumes[0].1.len(), segment_number, articles);
        submit_decoded_segment(
            &mut pipeline,
            file_id,
            segment_number,
            start as u64,
            &volumes[0].1[start..end],
            &volumes[0].0,
            Some(posted_crc),
        )
        .await;
    }

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(MemberChecksumMismatch)"),
        "the whole-member gate should have demoted the set, got {shape}"
    );
    let status = pipeline
        .jobs
        .get(&job_id)
        .map(|state| state.status.clone())
        .expect("the job must still be active");
    assert!(
        !matches!(status, JobStatus::Failed { .. }),
        "the whole-file CRC must be judged over the reconstructed volume, not over \
         the one article the seam held in hand, got {status:?}"
    );
    let (_, first_end) = article_extent(volumes[0].1.len(), 0, articles);
    assert_eq!(
        std::fs::read(working_dir.join(&volumes[0].0))
            .ok()
            .as_deref(),
        Some(&volumes[0].1[..first_end]),
        "the handback must drain exactly the in-hand first article to disk"
    );
    {
        let state = pipeline.jobs.get(&job_id).unwrap();
        let file = state
            .assembly
            .file(file_id)
            .expect("conventional assembly must own the demoted volume");
        assert!(
            !file.is_complete(),
            "a member the store could not verify keeps none of its routed articles, \
             so the assembly must still be waiting on them"
        );
        assert_eq!(
            file.placement_of(0),
            Some((0, first_end as u32)),
            "the in-hand article must restore the placement erased by demotion reset"
        );
    }
    assert!(
        pipeline
            .write_buffers
            .get(&file_id)
            .is_none_or(|write_buf| write_buf.buffered_len() == 0),
        "nothing may stay parked once the handback drained the seam's article"
    );
    {
        // Peek without draining: the refetch is dispatched below.
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let queued = state.download_queue.drain_all();
        let mut numbers: Vec<(u32, u32)> = queued
            .iter()
            .map(|work| {
                (
                    work.segment_id.file_id.file_index,
                    work.segment_id.segment_number,
                )
            })
            .collect();
        numbers.sort_unstable();
        for work in queued {
            state.download_queue.push(work);
        }
        assert_eq!(
            numbers,
            vec![(0, 1), (0, 2)],
            "the routed articles are refetched conventionally; the in-hand one is not"
        );
    }

    // The refetched tail lands conventionally and the whole-file CRC is
    // judged over the full volume, which is exactly what was posted.
    for segment_number in 1..articles as u32 {
        take_queued_segment(
            &mut pipeline,
            job_id,
            SegmentId {
                file_id,
                segment_number,
            },
        );
        let (start, end) = article_extent(volumes[0].1.len(), segment_number, articles);
        submit_decoded_segment(
            &mut pipeline,
            file_id,
            segment_number,
            start as u64,
            &volumes[0].1[start..end],
            &volumes[0].0,
            Some(posted_crc),
        )
        .await;
    }
    let status = pipeline
        .jobs
        .get(&job_id)
        .map(|state| state.status.clone())
        .expect("the job must still be active");
    assert!(
        !matches!(status, JobStatus::Failed { .. }),
        "the completed volume matches its posted CRC, got {status:?}"
    );
    assert_eq!(
        std::fs::read(working_dir.join(&volumes[0].0))
            .ok()
            .as_deref(),
        Some(volumes[0].1.as_slice()),
        "the refetched articles plus the in-hand first article must reproduce the posted volume"
    );
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .assembly
            .file(file_id)
            .is_some_and(|file| file.is_complete()),
        "the assembly must complete once the refetched tail lands"
    );
    assert!(
        !pipeline.write_buffers.contains_key(&file_id),
        "a completed volume must not retain its write buffer"
    );
    assert_eq!(
        queued_segments(&mut pipeline, job_id),
        Vec::<(u32, u32)>::new(),
        "nothing should be left to fetch"
    );
}

#[tokio::test]
async fn job_teardown_forgets_an_outstanding_demotion_sweep() {
    let member_name = "Silver.Horizon.S01E26.mkv";
    let volumes = demotion_fixture_volumes(member_name);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41060);
    let (mut pipeline, _, _) = demote_mid_download_leaving_the_sweep_outstanding(
        &temp_dir,
        job_id,
        &volumes,
        DemotionReason::HoldsBudgetExceeded,
        |_, _| {},
    )
    .await;
    assert!(
        pipeline.direct_demotion_in_flight.contains_key(&job_id),
        "non-vacuity: there has to be a ticket for the teardown to forget"
    );

    let queued_before = peek_queued_segments(&mut pipeline, job_id);
    pipeline.clear_job_progress_floor_runtime(job_id);
    assert!(
        pipeline.direct_demotion_in_flight.is_empty(),
        "job teardown must forget the ticket rather than leave the job's last \
         reference to a working directory it is about to delete"
    );

    // The detached worker still runs to its end and still posts its result. It
    // has to find no taker: the fence is what stops it applying floors, rows and
    // requeues to a job that no longer has the state they describe.
    let done = tokio::time::timeout(
        Duration::from_secs(10),
        pipeline.direct_demotion_done_rx.recv(),
    )
    .await
    .expect("the forgotten sweep still finishes")
    .expect("the demotion completion channel should stay open");
    pipeline.handle_direct_demotion_done(done).await;

    assert!(pipeline.direct_demotion_in_flight.is_empty());
    assert_eq!(
        peek_queued_segments(&mut pipeline, job_id),
        queued_before,
        "a forgotten ticket must change nothing when it lands"
    );
}

#[tokio::test]
async fn a_demoted_set_materializes_its_covered_volumes_instead_of_refetching_them() {
    let member_name = "Silver.Horizon.S01E10.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41015);
    let (mut pipeline, working_dir, other_file_bytes) =
        demote_mid_download(&temp_dir, job_id, &volumes, |_, _| {}).await;

    // Volume 0 was covered end to end, so it is rebuilt byte-exactly from its
    // envelope plus the member partial — the whole point of reconstruction is
    // that those two articles never touch the network again.
    assert_eq!(
        std::fs::read(working_dir.join(&volumes[0].0))
            .ok()
            .as_deref(),
        Some(volumes[0].1.as_slice()),
        "the covered volume must be reconstructed byte for byte"
    );
    // Volume 1 is rebuilt only as far as its coverage reaches: the prefix its
    // first article carried, and not one byte of the article that never came.
    let volume_one_prefix = volumes[1].1.len().div_ceil(2);
    assert_eq!(
        std::fs::read(working_dir.join(&volumes[1].0))
            .ok()
            .as_deref(),
        Some(&volumes[1].1[..volume_one_prefix]),
        "a partially covered volume is rebuilt exactly as far as its coverage"
    );

    assert_eq!(
        queued_segments(&mut pipeline, job_id),
        vec![(1, 1), (2, 0), (2, 1)],
        "a reconstructed article must not be fetched from the server again, and \
         everything above the floor must be"
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().downloaded_bytes,
        other_file_bytes + volumes[0].1.len() as u64 + volume_one_prefix as u64,
        "bytes that survived as a real volume stay counted; nothing else does"
    );

    // The direct outputs are gone: a sparse half-written member would
    // masquerade as finished work, and the envelopes are scratch.
    assert!(!direct_partial(&temp_dir, JobId(41015), member_name).exists());
    for volume_index in 0..volumes.len() as u32 {
        assert!(
            !working_dir
                .join(format!("silver.horizon.vol{volume_index:05}.envelope"))
                .exists(),
            "envelope {volume_index} must be deleted once the volume is real"
        );
    }

    // Reconciliation persisted legacy state in that order and shape: a whole
    // volume becomes a completed-file row, a partial one a contiguous floor,
    // and the direct coverage row is retired behind both.
    let (floors, complete) = pipeline.db.load_active_file_runtime(job_id).unwrap();
    assert!(
        complete.contains(&0),
        "a fully reconstructed volume is persisted as a completed file, got {complete:?}"
    );
    assert!(
        !complete.contains(&1),
        "a partially reconstructed volume must not be claimed complete"
    );
    assert_eq!(
        floors.get(&1).copied(),
        Some(volume_one_prefix as u64),
        "the partial volume persists a contiguous, segment-aligned floor"
    );
    assert!(
        pipeline.db.load_direct_coverage(job_id).unwrap().is_empty(),
        "the direct coverage row is retired once the legacy state replaces it"
    );
}

/// A demoted set's volumes must re-enter the conventional completion seam.
///
/// While the set was direct, `refresh_archive_state_for_completed_file`
/// suppressed itself for its files, so none of them ever reached the RAR
/// facts parser or the archive topology — correctly, because a direct set
/// never extracts through the topology. Demotion ends that: the volumes are
/// ordinary files now, and a materialized-complete volume that never gets
/// its facts registered is invisible to the extraction planner forever —
/// the plan waits on a volume whose bytes sit finished on disk, and no
/// event ever arrives to change its mind.
#[tokio::test]
async fn a_demoted_sets_materialized_volumes_register_their_rar_facts() {
    let member_name = "Silver.Horizon.S01E14.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 181) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41033);
    let (pipeline, _working_dir, _) =
        demote_mid_download(&temp_dir, job_id, &volumes, |_, _| {}).await;

    let facts_registered = pipeline
        .rar_sets
        .iter()
        .any(|((facts_job_id, _), state)| *facts_job_id == job_id && state.facts.contains_key(&0));
    assert!(
        facts_registered,
        "the materialized first volume must have parsed RAR facts through the \
         conventional completion replay; rar_sets = {:?}",
        pipeline
            .rar_sets
            .iter()
            .map(|((jid, name), state)| (
                jid.0,
                name.clone(),
                state.facts.keys().collect::<Vec<_>>()
            ))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_malformed_chain_demotion_leaves_a_partial_crc_atom_provisional() {
    let member_name = "Silver.Horizon.S01E11.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41032);
    let (mut pipeline, working_dir, other_file_bytes) =
        demote_mid_download(&temp_dir, job_id, &volumes, |pipeline, _| {
            // The SQLite ordering from the malformed-chain fixture: durable
            // placement reaches into the next article before the chain
            // contradiction demotes the set. Its whole-article CRC exists, but
            // the placed prefix cannot be composed against it exactly.
            let (start, end) = article_extent(volumes[1].1.len(), 1, 2);
            let partial_len = (end - start).div_ceil(2);
            let set = pipeline.direct_store.set_mut(job_id, 0).unwrap();
            set.note_volume_part_crc(
                1,
                start as u64,
                (end - start) as u64,
                par2_rs::checksum::crc32(&volumes[1].1[start..end]),
            );
            set.record_writes(
                &[crate::pipeline::direct_store::router::RoutedSpan {
                    destination:
                        crate::pipeline::direct_store::router::DirectDestination::Envelope {
                            volume_index: 1,
                        },
                    destination_offset: start as u64,
                    volume_index: 1,
                    source_offset: start as u64,
                    bytes: vec![0xA5; partial_len],
                }],
                std::time::Instant::now(),
            );
            assert_eq!(
                set.volume_coverage(1).end(),
                (start + partial_len) as u64,
                "the rig must end physical coverage inside the CRC atom"
            );
            set.demote(DemotionReason::MemberIneligible(
                crate::pipeline::direct_store::router::MemberIneligibility::MalformedChain,
            ));
        })
        .await;

    let volume_one_prefix = volumes[1].1.len().div_ceil(2);
    let materialized_volume_one = std::fs::read(working_dir.join(&volumes[1].0)).unwrap();
    let provisional_len = (volumes[1].1.len() - volume_one_prefix).div_ceil(2);
    assert_eq!(
        std::fs::read(working_dir.join(&volumes[0].0))
            .ok()
            .as_deref(),
        Some(volumes[0].1.as_slice()),
        "the complete neighbour must survive the malformed tail"
    );
    assert_eq!(
        materialized_volume_one.len(),
        volume_one_prefix + provisional_len,
        "physical geometry is retained even where proof coverage stops"
    );
    assert_eq!(
        &materialized_volume_one[..volume_one_prefix],
        &volumes[1].1[..volume_one_prefix],
        "the wholly CRC-vouched article must materialize byte for byte"
    );
    assert!(
        materialized_volume_one[volume_one_prefix..]
            .iter()
            .all(|byte| *byte == 0),
        "the partial CRC atom must remain a sparse, unowned hole"
    );
    assert_eq!(
        queued_segments(&mut pipeline, job_id),
        vec![(1, 1), (2, 0), (2, 1)],
        "the provisional article is targeted for conventional ownership without refetching its complete neighbours"
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().downloaded_bytes,
        other_file_bytes + volumes[0].1.len() as u64 + volume_one_prefix as u64,
        "only materialized article extents remain counted"
    );
    assert!(
        format!("{:?}", pipeline.direct_store.sets_for(job_id)).contains("MalformedChain"),
        "the regression must retain the malformed-chain demotion reason"
    );
}

#[tokio::test]
async fn a_volume_whose_envelope_is_gone_refetches_alone() {
    // Reconstruction is an optimisation; every one of its failure modes has to
    // land on the always-correct refetch rather than on a half-written volume.
    // Deleting the envelope out from under the sweep is the bluntest of them:
    // the header bytes it needs are simply not there any more.
    //
    // The refusal is *per volume*. Volume 0 loses its envelope and refetches
    // both its articles; volume 1, whose envelope is untouched, keeps the prefix
    // the sweep rebuilt for it and refetches nothing it already has. A blast
    // radius the width of the set would re-download a whole store archive to
    // pay for one unreadable header.
    let member_name = "Silver.Horizon.S01E20.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41031);
    let (mut pipeline, working_dir, other_file_bytes) =
        demote_mid_download(&temp_dir, job_id, &volumes, |_, working_dir| {
            let envelope = working_dir.join("silver.horizon.f0.vol00000.envelope");
            assert!(envelope.exists(), "the envelope must exist to be deleted");
            std::fs::remove_file(&envelope).unwrap();
        })
        .await;

    assert!(
        !working_dir.join(&volumes[0].0).exists(),
        "a failed reconstruction must not leave a partly written volume behind"
    );
    // The sibling is the point: it was rebuilt as far as its coverage reached
    // while its neighbour was being refused.
    let volume_one_prefix = volumes[1].1.len().div_ceil(2);
    assert_eq!(
        std::fs::read(working_dir.join(&volumes[1].0))
            .ok()
            .as_deref(),
        Some(&volumes[1].1[..volume_one_prefix]),
        "a volume whose own envelope is intact keeps the bytes the sweep rebuilt for it"
    );
    let expected = vec![(0, 0), (0, 1), (1, 1), (2, 0), (2, 1)];
    assert_eq!(
        queued_segments(&mut pipeline, job_id),
        expected,
        "only the refused volume's articles come back, each exactly once"
    );
    assert_eq!(
        pipeline.direct_store.pending_materialization_files(job_id),
        volumes.len(),
        "every reset source volume remains behind the PAR2 gate"
    );

    let set_id = par2_rs::RecoverySetId::from_bytes([41; 16]);
    assert!(
        !pipeline.demoted_materializations_ready_for_par2(job_id, set_id),
        "quiescent missing articles must be rescued before PAR2 can observe them"
    );
    assert_eq!(
        pipeline
            .direct_store
            .rescued_materialization_segments(job_id),
        expected.len(),
        "each ownerless segment receives one ordinary retry lineage"
    );
    assert!(
        !pipeline.demoted_materializations_ready_for_par2(job_id, set_id),
        "the queued rescue remains an owner and must not be duplicated"
    );
    assert_eq!(
        pipeline
            .direct_store
            .rescued_materialization_segments(job_id),
        expected.len()
    );
    let rescued = queued_segments(&mut pipeline, job_id);
    assert_eq!(rescued, expected);
    pipeline
        .segment_terminal_states
        .extend(rescued.iter().map(|(file_index, segment_number)| {
            (
                SegmentId {
                    file_id: NzbFileId {
                        job_id,
                        file_index: *file_index,
                    },
                    segment_number: *segment_number,
                },
                crate::pipeline::SegmentTerminalState::Missing,
            )
        }));
    assert!(
        pipeline.demoted_materializations_ready_for_par2(job_id, set_id),
        "terminally unavailable articles release the demotion gate"
    );
    assert_eq!(
        pipeline.direct_store.pending_materialization_files(job_id),
        0
    );
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().downloaded_bytes,
        other_file_bytes + volume_one_prefix as u64,
        "the job counter loses the refused volume's routed bytes and keeps the \
         floor the surviving volume earned"
    );
}

// ---------------------------------------------------------------------------
// Holds budget, and the demotion round trip it makes cheap to reach
// ---------------------------------------------------------------------------

/// Paging, at the seam the RAM budget used to demote at.
///
/// The first article is pure payload with no header yet, so it has nowhere to
/// go and is held; the budget is far below it. The router pages it to the set's
/// scratch file instead of demoting, and the set stays live.
#[tokio::test]
async fn direct_store_pages_held_bytes_to_scratch_instead_of_demoting() {
    let member_name = "Silver.Horizon.S01E12.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 149) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(64);
    let job_id = JobId(41018);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "a holds breach must page, not demote, got {shape}"
    );
    let set = pipeline.direct_store.set(job_id, 0).unwrap();
    assert!(
        set.router.scratch_bytes() > 0,
        "the held payload must have reached the scratch file"
    );
    assert!(
        set.router.resident_staged_bytes() <= 64,
        "paging must bring RAM back inside the budget, got {}",
        set.router.resident_staged_bytes()
    );
    // Named from the set so the restart sweep can find it by prefix, and
    // disambiguated by the set's lowest NZB file index so two set names that
    // sanitize to one component never share a scratch file — and with it, an
    // append cursor and a region index.
    assert!(
        working_dir.join(".weaver-holds.silver.horizon.f0").exists(),
        "the scratch file is named from the set, with its disambiguator, so the restart \
         sweep can find it"
    );
    assert_eq!(
        set.router.unaccounted_staged_bytes(),
        0,
        "every staged byte is either RAM-resident or paged; neither ceiling may be blind to it"
    );
}

/// Every set of a pipeline charges its holds to one accountant, and the limit
/// it enforces is the process total: a set well inside its own budget still
/// pages when the sets around it have spent the shared allowance. The set that
/// pages is the one routing at the time; the one that was there first keeps
/// its holds resident.
#[tokio::test]
async fn two_sets_share_one_resident_limit_and_the_one_routing_pages() {
    use crate::pipeline::direct_store::accountant::HoldsLimits;

    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 151) as u8).collect();
    let first_volumes = single_member_store_set("Silver.Horizon.S01E15.mkv", &payload, 3);
    let second_volumes = single_member_store_set("Silver.Horizon.S01E16.mkv", &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    // Each set could hold ten times its payload in RAM on its own budget; the
    // process as a whole may hold one article's worth and a little more.
    pipeline.direct_store.set_holds_budget(10_000);
    pipeline.direct_store.set_holds_limits(HoldsLimits {
        resident_bytes: 500,
        scratch_bytes: u64::MAX,
        disk_reserve_bytes: 0,
    });
    let first = JobId(41020);
    let second = JobId(41021);
    insert_active_job(
        &mut pipeline,
        first,
        direct_store_job_spec("Silver Horizon", &first_volumes),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        second,
        direct_store_job_spec("Silver Horizon II", &second_volumes),
    )
    .await;

    // The first set's payload-before-header hold fits the shared limit, so it
    // stays resident and is charged to the accountant as such.
    submit_volume_article(&mut pipeline, first, &first_volumes, 0, 1).await;
    let first_resident = pipeline
        .direct_store
        .set(first, 0)
        .unwrap()
        .router
        .resident_staged_bytes();
    assert!(first_resident > 0 && first_resident <= 500);
    assert_eq!(
        pipeline
            .direct_store
            .set(first, 0)
            .unwrap()
            .router
            .scratch_bytes(),
        0,
        "inside both limits nothing pages"
    );
    assert_eq!(
        pipeline.direct_store.holds_accountant().resident_bytes(),
        first_resident,
        "the accountant carries exactly what the set holds"
    );

    // The second set's identical hold would take the process over the limit.
    // It is inside its own budget by a wide margin, and it pages anyway — its
    // own holds, not the first set's.
    submit_volume_article(&mut pipeline, second, &second_volumes, 0, 1).await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(second));
    assert!(
        !shape.contains("Demoted"),
        "a shared breach pages, never demotes, got {shape}"
    );
    let second_set = pipeline.direct_store.set(second, 0).unwrap();
    assert!(
        second_set.router.scratch_bytes() > 0,
        "the set routing under a shared breach pages its holds"
    );
    assert_eq!(
        second_set.router.resident_staged_bytes(),
        0,
        "and pages everything it can, since the breach is not its own to size"
    );
    assert_eq!(second_set.router.unaccounted_staged_bytes(), 0);
    let first_set = pipeline.direct_store.set(first, 0).unwrap();
    assert_eq!(
        first_set.router.scratch_bytes(),
        0,
        "the set that was inside the limit first keeps its holds resident"
    );
    assert_eq!(first_set.router.resident_staged_bytes(), first_resident);
    let accountant = pipeline.direct_store.holds_accountant();
    assert_eq!(accountant.resident_bytes(), first_resident);
    assert_eq!(
        accountant.scratch_bytes(),
        second_set.router.scratch_bytes(),
        "scratch is charged to the accountant the moment it is written"
    );

    // Both sets finish, and the accountant has nothing left on its books: a
    // routed hold is released, a committed set's scratch is discarded.
    for (job_id, volumes) in [(first, &first_volumes), (second, &second_volumes)] {
        for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
            if (file_index, segment_number) == (0, 1) {
                continue;
            }
            submit_volume_article(&mut pipeline, job_id, volumes, file_index, segment_number).await;
        }
        drain_rar_refreshes(&mut pipeline).await;
        drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
        assert_eq!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete)
        );
    }
    let accountant = pipeline.direct_store.holds_accountant();
    assert_eq!(
        accountant.resident_bytes(),
        0,
        "no set holds anything after commit"
    );
    assert_eq!(
        accountant.scratch_bytes(),
        0,
        "no scratch survives a commit"
    );
}

/// The shared scratch total is judged on every set's scratch together, and a
/// spill that would exceed it demotes the set that asked — after that set has
/// compacted its own scratch and found nothing to reclaim — while the sets
/// already inside the total keep routing.
#[tokio::test]
async fn the_shared_scratch_total_demotes_the_set_that_asked_last() {
    use crate::pipeline::direct_store::accountant::HoldsLimits;

    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 163) as u8).collect();
    let first_volumes = single_member_store_set("Silver.Horizon.S01E17.mkv", &payload, 3);
    let second_volumes = single_member_store_set("Silver.Horizon.S01E18.mkv", &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    // Every hold pages; the process may hold one article's worth of scratch.
    pipeline.direct_store.set_holds_budget(64);
    pipeline.direct_store.set_holds_limits(HoldsLimits {
        resident_bytes: u64::MAX,
        scratch_bytes: 500,
        disk_reserve_bytes: 0,
    });
    let first = JobId(41022);
    let second = JobId(41023);
    insert_active_job(
        &mut pipeline,
        first,
        direct_store_job_spec("Silver Horizon", &first_volumes),
    )
    .await;
    let second_working_dir = insert_active_job(
        &mut pipeline,
        second,
        direct_store_job_spec("Silver Horizon II", &second_volumes),
    )
    .await;

    submit_volume_article(&mut pipeline, first, &first_volumes, 0, 1).await;
    let first_scratch = pipeline
        .direct_store
        .set(first, 0)
        .unwrap()
        .router
        .scratch_bytes();
    assert!(first_scratch > 0 && first_scratch <= 500);

    submit_volume_article(&mut pipeline, second, &second_volumes, 0, 1).await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(second));
    assert!(
        shape.contains("Demoted(HoldsScratchCeiling)"),
        "the spill that would exceed the shared total demotes the set that asked, got {shape}"
    );
    assert!(
        !format!("{:?}", pipeline.direct_store.sets_for(first)).contains("Demoted"),
        "the set already inside the total is untouched"
    );
    assert!(
        !second_working_dir
            .join(".weaver-holds.silver.horizon.ii.f0")
            .exists(),
        "a refused spill leaves no scratch behind"
    );
    assert_eq!(
        pipeline.direct_store.holds_accountant().scratch_bytes(),
        first_scratch,
        "the demoted set's charge is withdrawn; the first set's stands"
    );

    // The first set is unaffected end to end.
    for (file_index, segment_number) in in_order_arrivals(first_volumes.len()) {
        if (file_index, segment_number) == (0, 1) {
            continue;
        }
        submit_volume_article(
            &mut pipeline,
            first,
            &first_volumes,
            file_index,
            segment_number,
        )
        .await;
    }
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, first, 64).await;
    assert_eq!(
        job_status_for_assert(&pipeline, first),
        Some(JobStatus::Complete)
    );
    assert_eq!(pipeline.direct_store.holds_accountant().scratch_bytes(), 0);
}

/// A spill that would leave the working directory's filesystem with less than
/// its reserve is refused before the write, and the refusal is named for what
/// ran out — the disk — rather than for this set's scratch.
#[tokio::test]
async fn the_disk_reserve_refuses_a_spill_before_it_is_written() {
    use crate::pipeline::direct_store::accountant::HoldsLimits;

    let member_name = "Silver.Horizon.S01E19.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 167) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(64);
    // The filesystem reports 1200 bytes free and must keep 1000: a hold of
    // several hundred bytes cannot be paged without eating the reserve.
    pipeline.direct_store.set_holds_limits_with_disk_probe(
        HoldsLimits {
            resident_bytes: u64::MAX,
            scratch_bytes: u64::MAX,
            disk_reserve_bytes: 1000,
        },
        Box::new(|_| Some(1200)),
    );
    let job_id = JobId(41024);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(HoldsScratchDiskReserve)"),
        "a spill into the reserve demotes under the disk's own name, got {shape}"
    );
    assert!(
        !working_dir.join(".weaver-holds.silver.horizon.f0").exists(),
        "the refusal happens before the scratch is created"
    );
    assert_eq!(pipeline.direct_store.holds_accountant().scratch_bytes(), 0);

    // The job still completes the conventional way.
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if (file_index, segment_number) == (0, 1) {
            continue;
        }
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
}

/// A pass that reads a paged hold pins the scratch image, and the pin, not the
/// set, decides how long the image lives. The set's commit unlinks the path;
/// a provider still holding a pin reads the hold through the unlinked file,
/// exactly as posted; and the last pin dropping is what gives the bytes back.
/// Nothing the set does can strand a scratch image behind it, and nothing a
/// reader does can lose the bytes it was handed.
#[tokio::test]
async fn a_pinned_scratch_outlives_its_set_and_no_longer_than_its_last_reader() {
    use std::io::{Read as _, Seek as _};

    let member_name = "Silver.Horizon.S01E14.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 157) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(64);
    let job_id = JobId(41019);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    let scratch_path = working_dir.join(".weaver-holds.silver.horizon.f0");
    // A completed job takes its working directory with it, which leaves no
    // scratch behind either.
    let holds_left = |dir: &std::path::Path| {
        std::fs::read_dir(dir).map_or(0, |entries| {
            entries
                .filter(|entry| {
                    entry
                        .as_ref()
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".weaver-holds.")
                })
                .count()
        })
    };

    // Volume 0's payload before its header: held, and paged under the budget.
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_some_and(|set| set.router.scratch_bytes() > 0 && !set.router.scratch_is_pinned()),
        "non-vacuity: the hold is on the scratch and nothing reads it yet; got {:?}",
        pipeline.direct_store.sets_for(job_id)
    );
    assert!(scratch_path.exists());

    // The provider a PAR2 pass would build over the live set: it takes a pin
    // on the image rather than a copy of the hold.
    let (volume_index, _, provider) = pipeline
        .direct_virtual_volume(NzbFileId {
            job_id,
            file_index: 0,
        })
        .expect("volume 0 is a live direct volume");
    assert_eq!(volume_index, 0);
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_some_and(|set| set.router.scratch_is_pinned()),
        "a provider over a paged hold pins the scratch image"
    );

    // The set runs to its commit with the provider still alive.
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if (file_index, segment_number) == (0, 1) {
            continue;
        }
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
    assert!(
        matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete)
        ),
        "the job must complete; sets = {:?}",
        pipeline.direct_store.sets_for(job_id)
    );
    assert_eq!(
        holds_left(&working_dir),
        0,
        "the commit unlinks the scratch path whether or not a reader pins it"
    );

    // The pinned image still answers, and answers as posted: the hold the
    // budget paged out comes back through the unlinked file. The provider
    // snapshotted its coverage when it was built, so the header article that
    // had not arrived reads as the hole it was then; the hold is what it
    // claims, and the hold is what it must serve.
    let posted = &volumes[0].1;
    let (hold_start, hold_end) = article_extent(posted.len(), 1, 2);
    assert!(
        hold_end - hold_start > 64,
        "non-vacuity: the hold must be larger than the budget that paged it"
    );
    let mut reader = provider.open(0).expect("volume 0 is registered");
    reader
        .seek(std::io::SeekFrom::Start(hold_start as u64))
        .unwrap();
    let mut read_back = vec![0u8; hold_end - hold_start];
    let volume_len = reader.len();
    assert_eq!(
        volume_len,
        u64::try_from(hold_end).unwrap(),
        "a volume whose only posted bytes are a hold is as long as that hold reaches"
    );
    reader.read_exact(&mut read_back).unwrap_or_else(|error| {
        panic!(
            "a pinned image reads through the discard: {error}; volume len {volume_len}, \
             hold {hold_start}..{hold_end}"
        )
    });
    assert_eq!(
        read_back,
        posted[hold_start..hold_end],
        "every byte the provider serves after the commit is the byte that was posted"
    );

    drop(provider);
    assert_eq!(holds_left(&working_dir), 0);
}

/// The paged holds are not merely stored — they route, and the set finishes
/// byte-identically to a run that never breached its budget.
#[tokio::test]
async fn a_set_that_paged_its_holds_still_one_passes_byte_identically() {
    let member_name = "Silver.Horizon.S01E13.mkv";
    let payload: Vec<u8> = (0..3000u32).map(|index| (index % 151) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    // Every volume's payload before any header, so every one of them is held.
    let arrivals = payload_before_header_arrivals(volumes.len());

    let paged = run_direct_store_gate_with_budget(
        DirectStoreGate::Enabled,
        Some(64),
        JobId(41051),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;
    let unpaged = run_direct_store_gate(
        DirectStoreGate::Enabled,
        JobId(41052),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;
    let conventional = run_direct_store_gate(
        DirectStoreGate::Disabled,
        JobId(41053),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;

    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional gate must produce the member"
    );
    assert!(
        !paged.volume_file_seen,
        "paging holds must not materialize a source volume"
    );
    assert_eq!(
        (
            paged.member.as_ref(),
            paged.member_location,
            paged.status.clone()
        ),
        (
            unpaged.member.as_ref(),
            unpaged.member_location,
            unpaged.status
        ),
        "a paged run must match an unpaged direct run exactly"
    );
    assert_eq!(
        (paged.member, paged.member_location, paged.status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "a paged run must match the conventional extractor exactly"
    );
}

/// A ceiling breach is not the same thing as a full scratch.
///
/// The scratch is an append-only log, so a hold that gets placed leaves its
/// region behind: a set that pages, places, and pages again walks the cursor up
/// to the ceiling while holding almost nothing. Reclaiming that space is what
/// keeps such a set from demoting — and demotion here is expensive, because it
/// materializes the volumes and can refetch them.
#[tokio::test]
async fn a_reclaimable_scratch_breach_spills_instead_of_demoting() {
    let member_name = "Silver.Horizon.S01E15.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 149) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    // Each volume's payload before its own header, so every volume pages its
    // hold and then has it placed before the next one arrives.
    let arrivals = [(0u32, 1u32), (0, 0), (1, 1), (1, 0), (2, 1), (2, 0)];

    // Room for one volume's held payload but not two in sequence: the second
    // page only fits once the first volume's placed region is reclaimed.
    let direct = run_direct_store_gate_with_ceilings(
        DirectStoreGate::Enabled,
        Some(64),
        Some(600),
        JobId(41055),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;
    let conventional = run_direct_store_gate_with_budget(
        DirectStoreGate::Disabled,
        None,
        JobId(41056),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;

    assert!(
        !direct.volume_file_seen,
        "the set must have stayed direct: a demotion materializes the volumes"
    );
    assert_eq!(
        (direct.member, direct.member_location, direct.status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "and a set that reclaimed its scratch must still match the conventional extractor \
         exactly"
    );
}

/// The scratch ceiling is the last lever: past it there is nowhere left to put
/// the holds, and the set demotes with its own reason rather than the RAM one.
#[tokio::test]
async fn a_scratch_ceiling_breach_demotes_the_set() {
    let member_name = "Silver.Horizon.S01E14.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 149) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(64);
    pipeline.direct_store.set_holds_scratch_ceiling(16);
    let job_id = JobId(41054);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(HoldsScratchCeiling)"),
        "a scratch ceiling breach must demote with its own reason, got {shape}"
    );
    assert!(!direct_partial(&temp_dir, JobId(41054), member_name).exists());
}

/// A compaction under a pin cannot rewrite the image in place: the reader was
/// handed offsets into the file it holds open. The pack goes into a second
/// file that then takes over the path, and the retired image stays on disk,
/// unlinked, for as long as its last reader does. Both images are disk in use,
/// so both are charged to the shared total until that reader lets go; the
/// path carries no sibling files once the take-over is done; and the reader
/// keeps reading the bytes it was handed, as posted, through the retired image.
#[tokio::test]
async fn a_pinned_compaction_charges_the_retired_image_until_its_reader_lets_go() {
    use std::io::{Read as _, Seek as _};

    let member_name = "Silver.Horizon.S01E19.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 167) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let (a_start, a_end) = article_extent(volumes[0].1.len(), 1, 2);
    let (b_start, b_end) = article_extent(volumes[1].1.len(), 1, 2);
    let (c_start, c_end) = article_extent(volumes[2].1.len(), 1, 2);
    let (a, b, c) = (
        (a_end - a_start) as u64,
        (b_end - b_start) as u64,
        (c_end - c_start) as u64,
    );
    assert!(
        c > 1 && c <= a + 1,
        "non-vacuity: the third hold fits only once the first is reclaimed"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(64);
    // Room for two holds in sequence, not three: the third spill fits only
    // after the placed first hold's region is reclaimed.
    pipeline.direct_store.set_holds_scratch_ceiling(a + b + 1);
    let job_id = JobId(41057);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    let scratch_files = |dir: &std::path::Path| -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                    .filter(|name| name.starts_with(".weaver-holds."))
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        names
    };
    let router_scratch = |pipeline: &Pipeline| {
        pipeline
            .direct_store
            .set(job_id, 0)
            .map(|set| set.router.scratch_bytes())
            .unwrap_or(u64::MAX)
    };

    // Volume 0's payload before its header: the first hold, paged.
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;
    assert_eq!(
        router_scratch(&pipeline),
        a,
        "the first hold is the first spill"
    );
    // A reader over that hold pins the image it lives in.
    let (_, _, provider) = pipeline
        .direct_virtual_volume(NzbFileId {
            job_id,
            file_index: 0,
        })
        .expect("volume 0 is a live direct volume");
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_some_and(|set| set.router.scratch_is_pinned()),
        "non-vacuity: the provider pins the image the hold is paged to"
    );
    // Its header places it: the region is dead in the log, the log unchanged.
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    assert_eq!(
        router_scratch(&pipeline),
        a,
        "placing a hold reclaims nothing by itself"
    );
    // The second hold fills the log to its ceiling.
    submit_volume_article(&mut pipeline, job_id, &volumes, 1, 1).await;
    assert_eq!(router_scratch(&pipeline), a + b);
    assert_eq!(
        pipeline.direct_store.holds_accountant().scratch_bytes(),
        a + b
    );

    // The third hold breaches the ceiling under the pin: the live hold is
    // packed into a fresh image that takes over the path, and the third
    // hold lands behind it.
    submit_volume_article(&mut pipeline, job_id, &volumes, 2, 1).await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "a reclaimable breach under a pin must not demote, got {shape}"
    );
    assert_eq!(
        router_scratch(&pipeline),
        b + c,
        "the current image holds exactly the live holds"
    );
    assert_eq!(
        pipeline.direct_store.holds_accountant().scratch_bytes(),
        (a + b) + (b + c),
        "the retired image is charged beside the current one while its reader lives"
    );
    assert_eq!(
        scratch_files(&working_dir),
        vec![".weaver-holds.silver.horizon.f0".to_string()],
        "the take-over leaves neither the packing copy nor the retired image at a path"
    );
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_some_and(|set| !set.router.scratch_is_pinned()),
        "the pin moved with the image it was taken on; the fresh image starts unpinned"
    );

    // The reader was handed offsets into the retired image, and reads the
    // hold it was built over from it, as posted, after the take-over.
    let posted = &volumes[0].1;
    let mut reader = provider.open(0).expect("volume 0 is registered");
    reader
        .seek(std::io::SeekFrom::Start(a_start as u64))
        .unwrap();
    let mut read_back = vec![0u8; a_end - a_start];
    reader
        .read_exact(&mut read_back)
        .expect("a pinned reader reads through the retired image");
    assert_eq!(
        read_back,
        posted[a_start..a_end],
        "every byte the reader serves after the take-over is the byte that was posted"
    );

    // The last pin dropping is what gives the retired image's bytes back —
    // on the set's next publish, since nothing else runs on a drop. The
    // reader carries a pin of its own, so both have to go.
    drop(reader);
    drop(provider);
    submit_volume_article(&mut pipeline, job_id, &volumes, 1, 0).await;
    assert_eq!(
        pipeline.direct_store.holds_accountant().scratch_bytes(),
        router_scratch(&pipeline),
        "with its reader gone the retired image is forgotten, and its charge with it"
    );

    submit_volume_article(&mut pipeline, job_id, &volumes, 2, 0).await;
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
    assert!(
        matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete)
        ),
        "the job must complete; sets = {:?}",
        pipeline.direct_store.sets_for(job_id)
    );
    assert_eq!(
        pipeline.direct_store.holds_accountant().scratch_bytes(),
        0,
        "no scratch survives a commit"
    );
}

/// The packed copy a pinned compaction writes is a spill like any other: it is
/// admitted against the shared scratch total before it is written, and a
/// refusal demotes the set — it does not write a second image the process has
/// no room for, and it does not strand the half-written copy.
#[tokio::test]
async fn a_pinned_compaction_the_shared_total_cannot_admit_demotes_the_set() {
    use crate::pipeline::direct_store::accountant::HoldsLimits;

    let member_name = "Silver.Horizon.S01E19.mkv";
    // Volume sizes 734, 734, 732: the third volume's held article is one
    // byte shorter than the second's, which is the one byte of room the
    // limit below leaves between admitting the third spill and refusing the
    // packed copy of the second.
    let payload: Vec<u8> = (0..2200u32).map(|index| (index % 167) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);
    let (a_start, a_end) = article_extent(volumes[0].1.len(), 1, 2);
    let (b_start, b_end) = article_extent(volumes[1].1.len(), 1, 2);
    let (c_start, c_end) = article_extent(volumes[2].1.len(), 1, 2);
    let (a, b, c) = (
        (a_end - a_start) as u64,
        (b_end - b_start) as u64,
        (c_end - c_start) as u64,
    );
    assert!(
        b > c,
        "non-vacuity: the packed copy must be the spill that does not fit"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(64);
    pipeline.direct_store.set_holds_scratch_ceiling(a + b + 1);
    // The third spill is admitted exactly; the packed copy of the live hold,
    // asked for on top of it, is not.
    pipeline.direct_store.set_holds_limits(HoldsLimits {
        resident_bytes: u64::MAX,
        scratch_bytes: a + b + c,
        disk_reserve_bytes: 0,
    });
    let job_id = JobId(41058);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;
    let (_, _, provider) = pipeline
        .direct_virtual_volume(NzbFileId {
            job_id,
            file_index: 0,
        })
        .expect("volume 0 is a live direct volume");
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_some_and(|set| set.router.scratch_is_pinned()),
        "non-vacuity: the compaction runs under a pin"
    );
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    submit_volume_article(&mut pipeline, job_id, &volumes, 1, 1).await;
    assert_eq!(
        pipeline.direct_store.holds_accountant().scratch_bytes(),
        a + b,
        "non-vacuity: the log stands at its ceiling before the third spill"
    );

    submit_volume_article(&mut pipeline, job_id, &volumes, 2, 1).await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(HoldsScratchCeiling)"),
        "a packed copy the shared total cannot admit demotes the set, got {shape}"
    );
    let mut left: Vec<String> = std::fs::read_dir(&working_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(".weaver-holds."))
        .collect();
    left.sort();
    assert!(
        left.is_empty(),
        "a demotion discards the image and writes no copy of it, got {left:?}"
    );
    assert_eq!(
        pipeline.direct_store.holds_accountant().scratch_bytes(),
        0,
        "the demoted set's charge is withdrawn"
    );
    drop(provider);
}

#[tokio::test]
async fn a_demoted_set_completes_byte_identically_to_the_conventional_gate() {
    let member_name = "Silver.Horizon.S01E11.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 173) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    // Payload first, with neither RAM nor scratch room to absorb it: the set
    // demotes on its very first article and the archive itself stays perfectly
    // valid, so the refetch has to reproduce the conventional result exactly.
    // The trailing arrival is that first article coming back — exactly what the
    // demotion re-queues, and the only article it re-queues.
    let mut arrivals = payload_before_header_arrivals(volumes.len());
    arrivals.push((0, 1));

    let direct = run_direct_store_gate_with_ceilings(
        DirectStoreGate::Enabled,
        Some(64),
        Some(16),
        JobId(41019),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;
    let conventional = run_direct_store_gate_with_budget(
        DirectStoreGate::Disabled,
        None,
        JobId(41020),
        member_name,
        &volumes,
        &arrivals,
    )
    .await;

    assert_eq!(
        conventional.member.as_deref(),
        Some(payload.as_slice()),
        "the conventional gate must produce the member"
    );
    assert_eq!(
        (direct.member, direct.member_location, direct.status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
        "a demoted set must finish exactly as the conventional path would have"
    );
    assert!(
        direct.volume_file_seen,
        "a demoted set's volumes are materialized by the conventional path"
    );
}

// ---------------------------------------------------------------------------
// Chain-close eligibility
// ---------------------------------------------------------------------------

#[tokio::test]
async fn direct_store_demotes_when_the_chain_closes_with_a_blake2_only_member() {
    let member_name = "Silver.Horizon.S01E13.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 167) as u8).collect();
    let volumes = blake2_only_store_set(member_name, &payload, 3);
    let arrivals = in_order_arrivals(volumes.len());

    let temp_dir = tempfile::tempdir().unwrap();
    let (shape, working_dir) =
        run_direct_store_routing_only(&temp_dir, JobId(41021), &volumes, &arrivals).await;

    assert!(
        shape.contains("Demoted(MemberIneligible(Blake2OnlyNoCrc32))"),
        "a member that resolves blake2-only when its chain closes must demote the set, got {shape}"
    );
    assert!(
        !direct_partial(&temp_dir, JobId(41021), member_name).exists(),
        "the demotion deletes the bytes routed while the member was provisional"
    );

    // The label is not the outcome. The demotion runs reconstruction, and the
    // member whose eligibility just flipped is the one holding most of every
    // volume's bytes: a sweep that asked the *current* classification where
    // they live would be told "the envelope", read a sparse hole, and write
    // zeros into a volume file under a published floor. Whatever the demotion
    // decides to do, what lands on disk has to be the volume.
    assert_volumes_are_never_fabricated(&working_dir, &volumes);
    for volume_index in [0usize, 1] {
        let (filename, bytes) = &volumes[volume_index];
        assert_eq!(
            std::fs::read(working_dir.join(filename)).ok().as_deref(),
            Some(bytes.as_slice()),
            "volume {volume_index} was covered end to end and must be rebuilt byte for byte \
             from the partial its now-ineligible member still holds"
        );
    }
}

// ---------------------------------------------------------------------------
// The yEnc whole-volume gate
// ---------------------------------------------------------------------------

#[tokio::test]
async fn direct_store_demotes_a_volume_whose_yenc_whole_file_crc_disagrees() {
    let member_name = "Silver.Horizon.S01E14.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 191) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(41022);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // Volume 0's articles are internally consistent — each one's own yEnc part
    // CRC is right — but the trailer they declare for the whole file is not the
    // CRC of the bytes they carry. A physical volume fails this at
    // file-complete time; a direct one has no file to re-read, so the check has
    // to be composed from the parts.
    //
    // Volume 0 declares a trailer that is not the CRC of the bytes it carries;
    // volume 1 declares the right one, so the same gate has to let it through —
    // a check that only ever fires is not a check.
    let honest = checksum::crc32(&volumes[1].1);
    let wrong = checksum::crc32(&volumes[0].1) ^ 0xFFFF_FFFF;
    for (file_index, declared) in [(1u32, honest), (0, wrong)] {
        for segment_number in [0, 1] {
            let (filename, bytes) = &volumes[file_index as usize];
            let split = bytes.len().div_ceil(2);
            let (offset, part) = if segment_number == 0 {
                (0u64, &bytes[..split])
            } else {
                (split as u64, &bytes[split..])
            };
            submit_decoded_segment(
                &mut pipeline,
                NzbFileId { job_id, file_index },
                segment_number,
                offset,
                part,
                filename,
                Some(declared),
            )
            .await;
        }
        if file_index == 1 {
            let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
            assert!(
                !shape.contains("Demoted"),
                "a volume whose composed CRC32 matches its trailer must pass the gate, got {shape}"
            );
        }
    }

    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Demoted(VolumeCrcMismatch)"),
        "a volume whose composed yEnc CRC32 disagrees with its trailer must demote \
         at volume completion, long before any member gate could run, got {shape}"
    );
    assert!(!direct_partial(&temp_dir, JobId(41022), member_name).exists());
}

// ---------------------------------------------------------------------------
// Suppression and runtime lifetime
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_complete_direct_volume_never_refreshes_archive_state() {
    let member_name = "Silver.Horizon.S01E15.mkv";
    let payload: Vec<u8> = (0..1600u32).map(|index| (index % 131) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(41023);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    assert_eq!(
        pipeline.try_update_archive_topology_calls, 0,
        "the routing seam must not re-probe a volume that has no file"
    );

    // The refresh has nine callers; these two are the completion check's
    // (check.rs) and RAR finalization's (rar.rs), which fire for any complete
    // file whether or not routing suppressed its own call.
    for file_index in 0..volumes.len() as u32 {
        let file_id = NzbFileId { job_id, file_index };
        pipeline
            .refresh_archive_state_for_completed_file(job_id, file_id, false)
            .await;
        pipeline
            .refresh_archive_state_for_completed_file(job_id, file_id, true)
            .await;
    }
    pipeline.try_rar_extraction(job_id).await;

    assert_eq!(
        pipeline.try_update_archive_topology_calls, 0,
        "an indirect caller must not re-probe a direct volume either"
    );
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .is_some_and(|state| state.assembly.archive_topologies().is_empty()),
        "a direct set never enters the topology, so extraction is never dispatched for it"
    );
    assert!(
        pipeline
            .inflight_extractions
            .get(&job_id)
            .is_none_or(HashSet::is_empty)
    );
    for (filename, _) in &volumes {
        assert!(!working_dir.join(filename).exists());
    }
}

#[tokio::test]
async fn removing_a_job_prunes_the_direct_store_runtime() {
    let member_name = "Silver.Horizon.S01E16.mkv";
    let payload: Vec<u8> = (0..1600u32).map(|index| (index % 113) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 2);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(41024);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    insert_active_job(&mut pipeline, job_id, spec).await;
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;

    assert!(
        !pipeline.direct_store.sets_for(job_id).is_empty(),
        "the job admitted a set"
    );
    assert!(!pipeline.direct_store.is_empty_for(job_id));

    pipeline.purge_terminal_job_runtime(job_id);

    assert!(
        pipeline.direct_store.is_empty_for(job_id),
        "removing a job must leave no set, no examined mark and no prepared directory behind"
    );
    assert!(
        pipeline.direct_store.active_jobs().is_empty(),
        "a removed job's barriers must stop being polled"
    );
    // Idempotent, and safe on a job that never had a set.
    pipeline.purge_terminal_job_runtime(job_id);
    assert!(pipeline.direct_store.is_empty_for(job_id));
}

// ---------------------------------------------------------------------------
// Format detection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_rar4_set_routes_directly_because_the_format_is_read_not_assumed() {
    // A hardcoded RAR5 layout fails `add_volume`'s format check on the very
    // first header a RAR4 set produces, so every RAR4 job would route nothing
    // and pay the whole demotion round trip. The format comes from the
    // signature the first volume actually carries.
    let member_name = "Silver.Horizon.S01E17.mkv";
    let payload: Vec<u8> = (0..2400u32).map(|index| (index % 179) as u8).collect();
    let volumes = single_member_rar4_store_set(member_name, &payload, 3);
    assert_eq!(
        &volumes[0].1[..7],
        &TEST_RAR4_SIG,
        "the fixture really is RAR4"
    );
    assert_ne!(
        &volumes[0].1[..8],
        &TEST_RAR5_SIG,
        "and it is not the RAR5 shape every other fixture here uses"
    );

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(41025);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
        for (filename, _) in &volumes {
            assert!(
                !working_dir.join(filename).exists(),
                "a RAR4 set must route without ever materializing {filename}"
            );
        }
    }

    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the set was admitted");
    assert!(
        format!("{:?}", set.router).contains("format: Some(Rar4)"),
        "the router must bind the format the signature named, got {:?}",
        set.router
    );
    assert!(
        set.is_finalized(),
        "a RAR4 store set is as routable as a RAR5 one, got {set:?}"
    );
    assert_eq!(
        std::fs::read(
            crate::pipeline::Pipeline::member_output_paths(
                &payload_root(&temp_dir, JobId(41025)),
                member_name
            )
            .0
        )
        .ok()
        .as_deref(),
        Some(payload.as_slice()),
        "the routed RAR4 member must reproduce the payload byte for byte"
    );
}

// ---------------------------------------------------------------------------
// Multi-member sets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_two_member_store_set_routes_and_matches_the_conventional_extractor() {
    let episode = "Silver.Horizon.S01E01.mkv";
    let notes = "Silver.Horizon.S01E01.nfo";
    let members = vec![
        (
            episode,
            (0..3400u32).map(|index| (index % 251) as u8).collect(),
        ),
        // A tiny member, so one volume carries the tail of the first and the
        // whole of the second.
        (notes, b"invented notes for an invented release".to_vec()),
    ];
    let volumes = multi_member_store_set(&members, 3);
    let arrivals = in_order_arrivals(volumes.len());
    let names: Vec<&str> = members.iter().map(|(name, _)| *name).collect();

    let conventional = run_multi_member_gate(
        DirectStoreGate::Disabled,
        JobId(41040),
        &names,
        &volumes,
        &arrivals,
    )
    .await;
    let direct = run_multi_member_gate(
        DirectStoreGate::Enabled,
        JobId(41041),
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
        "a multi-member set must route without materializing a volume"
    );
    for (name, bytes, _) in &conventional.members {
        let expected = members
            .iter()
            .find(|(member, _)| member == name)
            .map(|(_, payload)| payload.as_slice());
        assert_eq!(
            bytes.as_deref(),
            expected,
            "the conventional extractor should reproduce {name}"
        );
    }
    assert_eq!(
        (direct.members, direct.status),
        (conventional.members, conventional.status),
        "a two-member direct set must be byte-identical to the conventional extractor"
    );
}

#[tokio::test]
async fn a_three_member_set_with_a_directory_member_matches_the_conventional_extractor() {
    let inside = "Silver.Horizon/S01E02.mkv";
    let episode = "Silver.Horizon.S01E03.mkv";
    let notes = "Silver.Horizon.nfo";
    let members = vec![
        // A member stored inside a directory: routing has to create the parent
        // before the first byte lands, where extraction creates it as it writes.
        (
            inside,
            (0..2600u32).map(|index| (index % 241) as u8).collect(),
        ),
        (
            episode,
            (0..3100u32).map(|index| (index % 193) as u8).collect(),
        ),
        (notes, b"an invented release, described briefly".to_vec()),
    ];
    let volumes = multi_member_store_set(&members, 4);
    let arrivals = in_order_arrivals(volumes.len());
    let names: Vec<&str> = members.iter().map(|(name, _)| *name).collect();

    let conventional = run_multi_member_gate(
        DirectStoreGate::Disabled,
        JobId(41042),
        &names,
        &volumes,
        &arrivals,
    )
    .await;
    let direct = run_multi_member_gate(
        DirectStoreGate::Enabled,
        JobId(41043),
        &names,
        &volumes,
        &arrivals,
    )
    .await;

    assert!(conventional.volume_file_seen);
    assert!(!direct.volume_file_seen);
    for (name, bytes, location) in &conventional.members {
        let expected = members
            .iter()
            .find(|(member, _)| member == name)
            .map(|(_, payload)| payload.as_slice());
        assert_eq!(
            bytes.as_deref(),
            expected,
            "the conventional extractor should reproduce {name}"
        );
        assert_eq!(*location, Some("complete"));
    }
    assert_eq!(
        (direct.members, direct.status),
        (conventional.members, conventional.status),
        "a three-member direct set must be byte-identical to the conventional extractor, \
         directory member included"
    );
}

#[tokio::test]
async fn two_members_that_sanitize_to_one_destination_demote_the_set() {
    // `ensure_unique_sanitized_rar_member_paths` refuses an archive whose
    // members collide after sanitization, so the extractor never overwrites one
    // with the other — and neither may direct routing, which is why the set
    // demotes and lets the ordinary path produce today's outcome. The commit
    // loop still walks members in archive order, which is what makes this
    // decidable at all rather than dependent on arrival order.
    let members = vec![
        (
            "./Silver.Horizon.nfo",
            b"the first of two names for one destination".to_vec(),
        ),
        (
            "Silver.Horizon.nfo",
            b"the second of two names for one destination".to_vec(),
        ),
    ];
    let volumes = multi_member_store_set(&members, 2);
    let arrivals = in_order_arrivals(volumes.len());

    let temp_dir = tempfile::tempdir().unwrap();
    let (shape, working_dir) =
        run_direct_store_routing_only(&temp_dir, JobId(41044), &volumes, &arrivals).await;

    assert!(
        shape.contains("Demoted(CollidingDestinations)"),
        "two members sanitizing to one path must demote rather than overwrite, got {shape}"
    );
    assert!(
        !working_dir.join("Silver.Horizon.nfo").exists(),
        "neither colliding member may be committed"
    );
}

// ---------------------------------------------------------------------------
// Envelope v2: recovery records route
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_recovery_record_set_routes_direct_and_its_envelopes_carry_the_recovery_data() {
    const RR_BYTES: usize = 48 * 1024;
    let member_name = "Silver.Horizon.S01E18.mkv";
    let payload: Vec<u8> = (0..40_000u32).map(|index| (index % 251) as u8).collect();
    let volumes = recovery_record_store_set(member_name, &payload, 3, RR_BYTES);
    let arrivals = in_order_arrivals(volumes.len());

    // Non-vacuity: the recovery record alone is bigger than the whole 64 KiB
    // slot the first shape gave a volume, let alone the 32 KiB half it
    // addressed the head from. This set could not have routed a byte before
    // envelope v2.
    const { assert!(RR_BYTES > 32 * 1024) };

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(41045);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // Halfway through, the envelope files must already hold the recovery data at
    // its true physical offset — the property that makes the whole scheme
    // restart-stable and unbounded.
    for (file_index, segment_number) in &arrivals {
        submit_volume_article(
            &mut pipeline,
            job_id,
            &volumes,
            *file_index,
            *segment_number,
        )
        .await;
        if (*file_index, *segment_number) == (0, 1) {
            let envelope = working_dir.join("silver.horizon.f0.vol00000.envelope");
            let written = std::fs::read(&envelope).expect("volume 0's envelope exists");
            let rr_at = find_recovery_offset(&volumes[0].1, RR_BYTES);
            assert!(
                written.len() >= rr_at + RR_BYTES,
                "the envelope must be long enough to hold the recovery record at its \
                 physical offset ({} < {})",
                written.len(),
                rr_at + RR_BYTES
            );
            assert_eq!(
                &written[rr_at..rr_at + RR_BYTES],
                &volumes[0].1[rr_at..rr_at + RR_BYTES],
                "the recovery record must land in the envelope byte for byte, at its \
                 true physical offset"
            );
        }
    }

    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the set was admitted");
    assert!(
        set.is_finalized(),
        "an -rr set routes and finalizes under envelope v2, got {set:?}"
    );
    assert_eq!(
        std::fs::read(
            crate::pipeline::Pipeline::member_output_paths(
                &payload_root(&temp_dir, JobId(41045)),
                member_name
            )
            .0
        )
        .ok()
        .as_deref(),
        Some(payload.as_slice()),
        "the member must be byte-correct even with a recovery record between the parts"
    );
    for (filename, _) in &volumes {
        assert!(!working_dir.join(filename).exists());
    }
    for volume_index in 0..volumes.len() as u32 {
        assert!(
            !working_dir
                .join(format!("silver.horizon.vol{volume_index:05}.envelope"))
                .exists(),
            "finalization must delete envelope {volume_index}"
        );
    }
}

// ---------------------------------------------------------------------------
// The hybrid virtual-volume provider, differentially
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_virtual_volume_reads_back_the_volume_the_conventional_gate_would_have_written() {
    use std::io::Read;

    // The unit tests build a virtual volume by hand; this one builds it the way
    // production does — out of whatever routing happened to put where — and
    // holds it to the only standard that matters: byte-for-byte agreement with
    // the volume the conventional gate writes to disk.
    let members = vec![
        (
            "Silver.Horizon.S01E19.mkv",
            (0..3400u32).map(|index| (index % 251) as u8).collect(),
        ),
        (
            "Silver.Horizon.nfo",
            b"invented notes for an invented release".to_vec(),
        ),
    ];
    let volumes = multi_member_store_set(&members, 3);

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(41046);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    insert_active_job(&mut pipeline, job_id, spec).await;

    // Volumes 0 and 1 whole, volume 2 only its first article — so the same
    // provider has to answer both "this volume is complete" and "this volume
    // stops here" without the caller telling it which is which.
    for (file_index, segment_number) in [(0u32, 0u32), (0, 1), (1, 0), (1, 1), (2, 0)] {
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        !shape.contains("Demoted"),
        "the set is still routing, got {shape}"
    );

    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the set was admitted");
    let prefix = volumes[2].1.len().div_ceil(2);
    let lengths: std::collections::BTreeMap<u32, u64> = [
        (0u32, volumes[0].1.len() as u64),
        (1, volumes[1].1.len() as u64),
        (2, volumes[2].1.len() as u64),
    ]
    .into_iter()
    .collect();
    let provider = set.virtual_provider(&lengths);

    for volume_index in [0usize, 1] {
        let mut reader = provider
            .open(volume_index as u32)
            .expect("the volume is registered");
        let mut read_back = Vec::new();
        reader.read_to_end(&mut read_back).unwrap();
        assert_eq!(
            read_back, volumes[volume_index].1,
            "virtual volume {volume_index} must equal the source volume byte for byte"
        );
    }

    // The half-arrived volume reads its covered prefix and then reports a hole
    // rather than pretending the volume ends there.
    let mut reader = provider.open(2).unwrap();
    let mut read_back = vec![0u8; prefix];
    reader.read_exact(&mut read_back).unwrap();
    assert_eq!(
        read_back,
        volumes[2].1[..prefix],
        "a partly arrived volume still reads back exactly what did arrive"
    );
    let error = reader.read(&mut [0u8; 16]).unwrap_err();
    assert!(
        crate::pipeline::direct_store::provider::is_hole(&error),
        "the bytes that never arrived must read as a hole, got {error}"
    );
}
