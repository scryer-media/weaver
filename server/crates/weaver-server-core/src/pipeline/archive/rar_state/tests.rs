use super::*;
use std::io::Cursor;
use weaver_par2::checksum;
use weaver_rar::RarArchive;

const TEST_RAR5_SIG: [u8; 8] = [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00];

fn encode_test_rar_vint(mut value: u64) -> Vec<u8> {
    let mut result = Vec::new();
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        result.push(byte);
        if value == 0 {
            break;
        }
    }
    result
}

fn build_test_rar_header(
    header_type: u64,
    common_flags: u64,
    type_body: &[u8],
    extra: &[u8],
) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&encode_test_rar_vint(header_type));

    let mut flags = common_flags;
    if !extra.is_empty() {
        flags |= 0x0001;
    }
    body.extend_from_slice(&encode_test_rar_vint(flags));
    if !extra.is_empty() {
        body.extend_from_slice(&encode_test_rar_vint(extra.len() as u64));
    }
    body.extend_from_slice(type_body);
    body.extend_from_slice(extra);

    let header_size = body.len() as u64;
    let header_size_bytes = encode_test_rar_vint(header_size);
    let crc = checksum::crc32(&[header_size_bytes.as_slice(), body.as_slice()].concat());

    let mut result = Vec::new();
    result.extend_from_slice(&crc.to_le_bytes());
    result.extend_from_slice(&header_size_bytes);
    result.extend_from_slice(&body);
    result
}

fn build_test_rar_main_header(archive_flags: u64, volume_number: Option<u64>) -> Vec<u8> {
    let mut type_body = Vec::new();
    type_body.extend_from_slice(&encode_test_rar_vint(archive_flags));
    if let Some(volume_number) = volume_number {
        type_body.extend_from_slice(&encode_test_rar_vint(volume_number));
    }
    build_test_rar_header(1, 0, &type_body, &[])
}

fn build_test_rar_end_header(more_volumes: bool) -> Vec<u8> {
    let end_flags: u64 = if more_volumes { 0x0001 } else { 0 };
    build_test_rar_header(5, 0, &encode_test_rar_vint(end_flags), &[])
}

fn build_test_rar_file_header(
    filename: &str,
    common_flags_extra: u64,
    data_size: u64,
    unpacked_size: u64,
    data_crc: Option<u32>,
) -> Vec<u8> {
    let file_flags: u64 = if data_crc.is_some() { 0x0004 } else { 0 };
    let mut type_body = Vec::new();
    type_body.extend_from_slice(&encode_test_rar_vint(file_flags));
    type_body.extend_from_slice(&encode_test_rar_vint(unpacked_size));
    type_body.extend_from_slice(&encode_test_rar_vint(0o644));
    if let Some(data_crc) = data_crc {
        type_body.extend_from_slice(&data_crc.to_le_bytes());
    }
    type_body.extend_from_slice(&encode_test_rar_vint(0));
    type_body.extend_from_slice(&encode_test_rar_vint(1));
    type_body.extend_from_slice(&encode_test_rar_vint(filename.len() as u64));
    type_body.extend_from_slice(filename.as_bytes());

    let mut body = Vec::new();
    body.extend_from_slice(&encode_test_rar_vint(2));
    body.extend_from_slice(&encode_test_rar_vint(0x0002 | common_flags_extra));
    body.extend_from_slice(&encode_test_rar_vint(data_size));
    body.extend_from_slice(&type_body);

    let header_size = body.len() as u64;
    let header_size_bytes = encode_test_rar_vint(header_size);
    let crc = checksum::crc32(&[header_size_bytes.as_slice(), body.as_slice()].concat());

    let mut result = Vec::new();
    result.extend_from_slice(&crc.to_le_bytes());
    result.extend_from_slice(&header_size_bytes);
    result.extend_from_slice(&body);
    result
}

fn build_multifile_multivolume_rar_set() -> Vec<(String, Vec<u8>)> {
    let episode_a = b"episode-a-payload";
    let episode_b = b"episode-b-payload";
    let episode_a_crc = checksum::crc32(episode_a);
    let episode_b_crc = checksum::crc32(episode_b);

    let a_part1 = &episode_a[..8];
    let a_part2 = &episode_a[8..];
    let b_part1 = &episode_b[..8];
    let b_part2 = &episode_b[8..];

    let mut part01 = Vec::new();
    part01.extend_from_slice(&TEST_RAR5_SIG);
    part01.extend_from_slice(&build_test_rar_main_header(0x0001, None));
    part01.extend_from_slice(&build_test_rar_file_header(
        "E01.mkv",
        0x0010,
        a_part1.len() as u64,
        episode_a.len() as u64,
        None,
    ));
    part01.extend_from_slice(a_part1);
    part01.extend_from_slice(&build_test_rar_end_header(true));

    let mut part02 = Vec::new();
    part02.extend_from_slice(&TEST_RAR5_SIG);
    part02.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(1)));
    part02.extend_from_slice(&build_test_rar_file_header(
        "E01.mkv",
        0x0008,
        a_part2.len() as u64,
        episode_a.len() as u64,
        Some(episode_a_crc),
    ));
    part02.extend_from_slice(a_part2);
    part02.extend_from_slice(&build_test_rar_end_header(true));

    let mut part03 = Vec::new();
    part03.extend_from_slice(&TEST_RAR5_SIG);
    part03.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(2)));
    part03.extend_from_slice(&build_test_rar_file_header(
        "E02.mkv",
        0x0010,
        b_part1.len() as u64,
        episode_b.len() as u64,
        None,
    ));
    part03.extend_from_slice(b_part1);
    part03.extend_from_slice(&build_test_rar_end_header(true));

    let mut part04 = Vec::new();
    part04.extend_from_slice(&TEST_RAR5_SIG);
    part04.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(3)));
    part04.extend_from_slice(&build_test_rar_file_header(
        "E02.mkv",
        0x0008,
        b_part2.len() as u64,
        episode_b.len() as u64,
        Some(episode_b_crc),
    ));
    part04.extend_from_slice(b_part2);
    part04.extend_from_slice(&build_test_rar_end_header(false));

    vec![
        ("show.part01.rar".to_string(), part01),
        ("show.part02.rar".to_string(), part02),
        ("show.part03.rar".to_string(), part03),
        ("show.part04.rar".to_string(), part04),
    ]
}

#[test]
fn push_unique_ready_member_preserves_order_and_dedupes_names() {
    let mut ready_members = Vec::new();
    let mut seen_members = HashSet::new();

    push_unique_ready_member(&mut ready_members, &mut seen_members, "E10.mkv");
    push_unique_ready_member(&mut ready_members, &mut seen_members, "E10.mkv");
    push_unique_ready_member(&mut ready_members, &mut seen_members, "E11.mkv");

    let names: Vec<&str> = ready_members
        .iter()
        .map(|member| member.name.as_str())
        .collect();
    assert_eq!(names, vec!["E10.mkv", "E11.mkv"]);
}

#[test]
fn build_plan_claims_missing_continuation_volumes_for_pending_owners() {
    let files = build_multifile_multivolume_rar_set();
    let mut archive = RarArchive::open(Cursor::new(files[0].1.clone())).unwrap();
    archive
        .add_volume(1, Box::new(Cursor::new(files[1].1.clone())))
        .unwrap();
    archive
        .add_volume(2, Box::new(Cursor::new(files[2].1.clone())))
        .unwrap();

    let facts: BTreeMap<u32, RarVolumeFacts> = files
        .iter()
        .enumerate()
        .map(|(volume, (_, bytes))| {
            (
                volume as u32,
                RarArchive::parse_volume_facts(Cursor::new(bytes.clone()), None).unwrap(),
            )
        })
        .collect();
    let volume_map = files
        .iter()
        .enumerate()
        .map(|(volume, (filename, _))| (filename.clone(), volume as u32))
        .collect();
    let extracted = HashSet::from(["E01.mkv".to_string()]);
    let failed = HashSet::new();

    let plan = build_plan(volume_map, &facts, &archive, &extracted, &failed, false).unwrap();

    assert!(plan.waiting_on_volumes.contains(&3));
    assert!(!plan.deletion_eligible.contains(&3));
    let decision = plan.delete_decisions.get(&3).unwrap();
    assert_eq!(decision.owners, vec!["E02.mkv".to_string()]);
    assert_eq!(decision.pending_owners, vec!["E02.mkv".to_string()]);
    assert!(!decision.ownership_eligible);
}

#[test]
fn build_plan_blocks_delete_for_missing_start_continuation_spans() {
    let files = build_multifile_multivolume_rar_set();
    let mut archive = RarArchive::open(Cursor::new(files[0].1.clone())).unwrap();
    archive
        .add_volume(3, Box::new(Cursor::new(files[3].1.clone())))
        .unwrap();

    let facts: BTreeMap<u32, RarVolumeFacts> = files
        .iter()
        .enumerate()
        .map(|(volume, (_, bytes))| {
            (
                volume as u32,
                RarArchive::parse_volume_facts(Cursor::new(bytes.clone()), None).unwrap(),
            )
        })
        .collect();
    let volume_map = files
        .iter()
        .enumerate()
        .map(|(volume, (filename, _))| (filename.clone(), volume as u32))
        .collect();

    let plan = build_plan(
        volume_map,
        &facts,
        &archive,
        &HashSet::new(),
        &HashSet::new(),
        false,
    )
    .unwrap();

    assert!(plan.waiting_on_volumes.contains(&3));
    assert!(!plan.deletion_eligible.contains(&3));
    let decision = plan.delete_decisions.get(&3).unwrap();
    assert!(decision.unresolved_boundary);
    assert!(!decision.ownership_eligible);
}

/// Build a 5-volume RAR set with a single member spanning all volumes.
/// Used to test gap scenarios where volumes arrive out of order.
fn build_single_member_five_volume_rar_set() -> Vec<(String, Vec<u8>)> {
    let payload = b"abcdefghijklmnopqrstuvwxy"; // 25 bytes
    let payload_crc = checksum::crc32(payload);
    let chunk_size = 5;

    let mut volumes = Vec::new();
    for vol in 0u64..5 {
        let chunk_start = vol as usize * chunk_size;
        let chunk = &payload[chunk_start..chunk_start + chunk_size];

        let is_first = vol == 0;
        let is_last = vol == 4;

        let main_flags = if is_first { 0x0001 } else { 0x0001 | 0x0002 };
        let vol_num = if is_first { None } else { Some(vol) };

        let mut file_common_flags = 0u64;
        if !is_first {
            file_common_flags |= 0x0008; // SPLIT_BEFORE
        }
        if !is_last {
            file_common_flags |= 0x0010; // SPLIT_AFTER
        }

        let data_crc = if is_last { Some(payload_crc) } else { None };

        let mut volume_data = Vec::new();
        volume_data.extend_from_slice(&TEST_RAR5_SIG);
        volume_data.extend_from_slice(&build_test_rar_main_header(main_flags, vol_num));
        volume_data.extend_from_slice(&build_test_rar_file_header(
            "E01.mkv",
            file_common_flags,
            chunk.len() as u64,
            payload.len() as u64,
            data_crc,
        ));
        volume_data.extend_from_slice(chunk);
        volume_data.extend_from_slice(&build_test_rar_end_header(!is_last));

        let filename = format!("show.part{:02}.rar", vol + 1);
        volumes.push((filename, volume_data));
    }
    volumes
}

#[test]
fn build_plan_split_chain_gap_preserves_claims_from_both_halves() {
    // Build 5 volumes for a single member E01.mkv spanning vols 0-4.
    // Add vols 0, 1, 3, 4 to the archive (skip vol 2 to create a gap).
    // This creates two MemberEntry records for E01.mkv that can't merge.
    // The fix ensures claims from both halves are preserved.
    let files = build_single_member_five_volume_rar_set();
    let mut archive = RarArchive::open(Cursor::new(files[0].1.clone())).unwrap();
    archive
        .add_volume(1, Box::new(Cursor::new(files[1].1.clone())))
        .unwrap();
    // Skip volume 2 — creates a gap
    archive
        .add_volume(3, Box::new(Cursor::new(files[3].1.clone())))
        .unwrap();
    archive
        .add_volume(4, Box::new(Cursor::new(files[4].1.clone())))
        .unwrap();

    // Include all 5 volumes in facts (all downloaded).
    let facts: BTreeMap<u32, RarVolumeFacts> = files
        .iter()
        .enumerate()
        .map(|(volume, (_, bytes))| {
            (
                volume as u32,
                RarArchive::parse_volume_facts(Cursor::new(bytes.clone()), None).unwrap(),
            )
        })
        .collect();
    let volume_map = files
        .iter()
        .enumerate()
        .map(|(volume, (filename, _))| (filename.clone(), volume as u32))
        .collect();

    let plan = build_plan(
        volume_map,
        &facts,
        &archive,
        &HashSet::new(),
        &HashSet::new(),
        false,
    )
    .unwrap();

    // Volumes 0, 1 should be owned by E01.mkv (first half of split chain).
    for vol in [0, 1] {
        let decision = plan.delete_decisions.get(&vol).unwrap();
        assert!(
            !decision.owners.is_empty(),
            "volume {vol} should have at least one owner"
        );
        assert!(
            decision.owners.contains(&"E01.mkv".to_string()),
            "volume {vol} should be owned by E01.mkv, got {:?}",
            decision.owners
        );
    }

    // Volumes 3, 4 should also be owned by E01.mkv (second half).
    for vol in [3, 4] {
        let decision = plan.delete_decisions.get(&vol).unwrap();
        assert!(
            !decision.owners.is_empty(),
            "volume {vol} should have at least one owner"
        );
        assert!(
            decision.owners.contains(&"E01.mkv".to_string()),
            "volume {vol} should be owned by E01.mkv, got {:?}",
            decision.owners
        );
    }

    // No volumes should be ownerless (except vol 2 which is in facts but
    // not covered by any member segment range — it's a gap volume that
    // should still have an owner from topology_members supplemental claims
    // since the topology entry for the second half covers vol 3-4, and the
    // first half covers 0-1. Vol 2 has facts but no topology coverage).
    let ownerless: Vec<u32> = plan
        .delete_decisions
        .iter()
        .filter_map(|(vol, decision)| decision.owners.is_empty().then_some(*vol))
        .collect();
    // Vol 2 may be ownerless since it's in facts but not in any archive
    // member's segment range (it wasn't added to the archive). That's
    // acceptable — the important thing is 0, 1, 3, 4 are NOT ownerless.
    let unexpected_ownerless: Vec<u32> = ownerless.iter().copied().filter(|v| *v != 2).collect();
    assert!(
        unexpected_ownerless.is_empty(),
        "unexpected ownerless volumes: {unexpected_ownerless:?}"
    );
}

#[test]
fn build_plan_boundary_orphan_continuation_has_owner() {
    // Build the standard 4-volume set (E01 on vols 0-1, E02 on vols 2-3).
    // Add only vol 0 and vol 3 to the archive. Volume 3 has an E02.mkv
    // continuation entry (split_before=true) with a single segment that
    // can't merge — creating an orphan filtered from metadata.members.
    // After the fix, vol 3 should have an owner from topology_members.
    let files = build_multifile_multivolume_rar_set();
    let mut archive = RarArchive::open(Cursor::new(files[0].1.clone())).unwrap();
    archive
        .add_volume(3, Box::new(Cursor::new(files[3].1.clone())))
        .unwrap();

    let facts: BTreeMap<u32, RarVolumeFacts> = files
        .iter()
        .enumerate()
        .map(|(volume, (_, bytes))| {
            (
                volume as u32,
                RarArchive::parse_volume_facts(Cursor::new(bytes.clone()), None).unwrap(),
            )
        })
        .collect();
    let volume_map = files
        .iter()
        .enumerate()
        .map(|(volume, (filename, _))| (filename.clone(), volume as u32))
        .collect();

    let plan = build_plan(
        volume_map,
        &facts,
        &archive,
        &HashSet::new(),
        &HashSet::new(),
        false,
    )
    .unwrap();

    // Volume 3 should have an owner (E02.mkv from topology_members).
    let decision = plan.delete_decisions.get(&3).unwrap();
    assert!(
        !decision.owners.is_empty(),
        "boundary orphan volume 3 should have at least one owner, got zero"
    );
    assert!(
        decision.owners.contains(&"E02.mkv".to_string()),
        "volume 3 should be owned by E02.mkv, got {:?}",
        decision.owners
    );
    // It should still be unresolved and not deletion-eligible.
    assert!(decision.unresolved_boundary);
    assert!(!decision.ownership_eligible);
}
