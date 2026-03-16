use std::collections::{BTreeMap, HashMap, HashSet};

use weaver_assembly::{ArchiveMember, ArchivePendingSpan, ArchiveTopology, ArchiveType};
use weaver_rar::{RarArchive, RarVolumeFacts};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RarSetPhase {
    WaitingForVolumes,
    Ready,
    Extracting,
    AwaitingRepair,
    FallbackFullSet,
    Complete,
}

impl RarSetPhase {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::WaitingForVolumes => "waiting_for_volumes",
            Self::Ready => "ready",
            Self::Extracting => "extracting",
            Self::AwaitingRepair => "awaiting_repair",
            Self::FallbackFullSet => "fallback_full_set",
            Self::Complete => "complete",
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct RarReadyMember {
    pub(super) name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RarVolumeDeleteDecision {
    pub(super) owners: Vec<String>,
    pub(super) clean_owners: Vec<String>,
    pub(super) failed_owners: Vec<String>,
    pub(super) pending_owners: Vec<String>,
    pub(super) unresolved_boundary: bool,
    pub(super) ownership_eligible: bool,
}

#[derive(Debug, Clone)]
pub(super) struct RarDerivedPlan {
    pub(super) phase: RarSetPhase,
    pub(super) is_solid: bool,
    pub(super) ready_members: Vec<RarReadyMember>,
    pub(super) member_names: Vec<String>,
    pub(super) waiting_on_volumes: HashSet<u32>,
    pub(super) deletion_eligible: HashSet<u32>,
    pub(super) delete_decisions: BTreeMap<u32, RarVolumeDeleteDecision>,
    pub(super) topology: ArchiveTopology,
    pub(super) fallback_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct RarSetState {
    pub(super) facts: BTreeMap<u32, RarVolumeFacts>,
    pub(super) volume_files: BTreeMap<u32, String>,
    pub(super) cached_headers: Option<Vec<u8>>,
    pub(super) verified_suspect_volumes: HashSet<u32>,
    pub(super) active_workers: usize,
    pub(super) in_flight_members: HashSet<String>,
    pub(super) phase: RarSetPhase,
    pub(super) plan: Option<RarDerivedPlan>,
}

impl Default for RarSetState {
    fn default() -> Self {
        Self {
            facts: BTreeMap::new(),
            volume_files: BTreeMap::new(),
            cached_headers: None,
            verified_suspect_volumes: HashSet::new(),
            active_workers: 0,
            in_flight_members: HashSet::new(),
            phase: RarSetPhase::WaitingForVolumes,
            plan: None,
        }
    }
}

fn push_unique_ready_member(
    ready_members: &mut Vec<RarReadyMember>,
    seen_members: &mut HashSet<String>,
    member_name: &str,
) {
    if seen_members.insert(member_name.to_string()) {
        ready_members.push(RarReadyMember {
            name: member_name.to_string(),
        });
    }
}

fn sort_dedup(values: &mut Vec<String>) {
    values.sort();
    values.dedup();
}

pub(super) fn contiguous_prefix_end(facts: &BTreeMap<u32, RarVolumeFacts>) -> Option<u32> {
    if !facts.contains_key(&0) {
        return None;
    }

    let mut next = 0u32;
    while facts.contains_key(&next) {
        next = next.saturating_add(1);
    }
    Some(next.saturating_sub(1))
}

pub(super) fn build_plan(
    volume_map: HashMap<String, u32>,
    facts: &BTreeMap<u32, RarVolumeFacts>,
    archive: &RarArchive,
    extracted: &HashSet<String>,
    failed: &HashSet<String>,
    worker_active: bool,
) -> Result<RarDerivedPlan, String> {
    let complete_volumes: HashSet<u32> = facts.keys().copied().collect();
    let mut topology = ArchiveTopology {
        archive_type: ArchiveType::Rar,
        volume_map,
        complete_volumes,
        expected_volume_count: None,
        members: Vec::new(),
        unresolved_spans: Vec::new(),
    };
    let mut waiting_on_volumes = HashSet::new();

    let Some(prefix_end) = contiguous_prefix_end(facts) else {
        waiting_on_volumes.insert(0);
        let phase = if worker_active {
            RarSetPhase::Extracting
        } else {
            RarSetPhase::WaitingForVolumes
        };
        return Ok(RarDerivedPlan {
            phase,
            is_solid: false,
            ready_members: Vec::new(),
            member_names: Vec::new(),
            waiting_on_volumes,
            deletion_eligible: HashSet::new(),
            delete_decisions: BTreeMap::new(),
            topology,
            fallback_reason: None,
        });
    };

    let metadata = archive.metadata();
    let topology_members = archive.topology_members();
    let planner_states: HashMap<String, (bool, Vec<u32>)> = archive
        .planner_member_states()
        .into_iter()
        .map(|state| {
            (
                state.name,
                (
                    state.extractable,
                    state
                        .missing_volumes
                        .into_iter()
                        .map(|volume| volume as u32)
                        .collect(),
                ),
            )
        })
        .collect();
    topology.expected_volume_count = metadata.volume_count.map(|count| count as u32);
    let final_volume_seen = facts
        .get(&prefix_end)
        .is_some_and(|volume_facts| !volume_facts.more_volumes);
    let missing_for_member = |member: &weaver_rar::MemberInfo| {
        let mut missing = planner_states
            .get(&member.name)
            .map(|(_, missing)| missing.clone())
            .unwrap_or_default();
        if final_volume_seen && member.volumes.last_volume as u32 == prefix_end {
            missing.retain(|volume| *volume <= prefix_end);
        }
        missing
    };
    let mut member_names = Vec::new();
    let mut member_claims: HashMap<String, HashSet<u32>> = HashMap::new();
    for member in &topology_members {
        if member.is_directory || !member.missing_start {
            continue;
        }
        let first_volume = member.volumes.first_volume as u32;
        let last_volume = member.volumes.last_volume as u32;
        waiting_on_volumes.extend(first_volume..=last_volume);
        topology.unresolved_spans.push(ArchivePendingSpan {
            first_volume,
            last_volume,
        });
    }
    for member in &metadata.members {
        if member.is_directory {
            continue;
        }
        member_names.push(member.name.clone());
        topology.members.push(ArchiveMember {
            name: member.name.clone(),
            first_volume: member.volumes.first_volume as u32,
            last_volume: member.volumes.last_volume as u32,
            unpacked_size: member.unpacked_size.unwrap_or(0),
        });

        if !extracted.contains(&member.name) {
            let missing = missing_for_member(member);
            if !missing.is_empty() {
                waiting_on_volumes.extend(missing.iter().copied());
                topology.unresolved_spans.push(ArchivePendingSpan {
                    first_volume: member.volumes.last_volume as u32,
                    last_volume: member.volumes.last_volume as u32,
                });
            }
        }

        let mut claims: HashSet<u32> =
            (member.volumes.first_volume as u32..=member.volumes.last_volume as u32).collect();
        if !extracted.contains(&member.name) {
            claims.extend(missing_for_member(member));
        }
        member_claims
            .entry(member.name.clone())
            .or_default()
            .extend(claims);
    }

    // Supplemental claims from topology members. This covers orphan
    // continuation entries (split_before=true, single segment) that
    // metadata() filters out. Without this, boundary volumes between
    // members have zero owners during incremental topology rebuilds.
    for member in &topology_members {
        if member.is_directory {
            continue;
        }
        // Continuation headers from out-of-order volume arrival can have
        // empty names — skip these to avoid phantom owners.
        if member.name.is_empty() {
            continue;
        }
        let first_volume = member.volumes.first_volume as u32;
        let last_volume = member.volumes.last_volume as u32;
        member_claims
            .entry(member.name.clone())
            .or_default()
            .extend(first_volume..=last_volume);
    }

    if facts
        .get(&prefix_end)
        .is_some_and(|facts| facts.more_volumes)
    {
        waiting_on_volumes.insert(prefix_end + 1);
        let tail_member_pending = topology
            .members
            .iter()
            .any(|member| member.last_volume == prefix_end && !extracted.contains(&member.name));
        if tail_member_pending || topology.members.is_empty() {
            topology.unresolved_spans.push(ArchivePendingSpan {
                first_volume: prefix_end,
                last_volume: prefix_end,
            });
        }
    }

    let mut ready_members = Vec::new();
    let mut ready_member_names = HashSet::new();
    if metadata.is_solid {
        for member in &metadata.members {
            if member.is_directory || extracted.contains(&member.name) {
                continue;
            }
            let extractable = planner_states
                .get(&member.name)
                .map(|(extractable, _)| *extractable)
                .unwrap_or(false);
            if extractable {
                if !failed.contains(&member.name) {
                    push_unique_ready_member(
                        &mut ready_members,
                        &mut ready_member_names,
                        &member.name,
                    );
                }
            } else {
                waiting_on_volumes.extend(missing_for_member(member));
                break;
            }
        }
    } else {
        for member in &metadata.members {
            if member.is_directory
                || extracted.contains(&member.name)
                || failed.contains(&member.name)
            {
                continue;
            }
            let extractable = planner_states
                .get(&member.name)
                .map(|(extractable, _)| *extractable)
                .unwrap_or(false);
            if extractable {
                push_unique_ready_member(&mut ready_members, &mut ready_member_names, &member.name);
            } else {
                waiting_on_volumes.extend(missing_for_member(member));
            }
        }
    }

    let mut delete_decisions = BTreeMap::new();
    let mut ownerless_volumes = Vec::new();
    // Iterate all members with claims (not just member_names from metadata),
    // so topology-only members (orphan continuations) contribute ownership.
    let all_claiming_members: Vec<String> = member_claims.keys().cloned().collect();
    for volume in facts.keys().copied() {
        let mut owners = Vec::new();
        let mut clean_owners = Vec::new();
        let mut failed_owners = Vec::new();
        let mut pending_owners = Vec::new();
        for member in &all_claiming_members {
            if !member_claims
                .get(member)
                .is_some_and(|claims| claims.contains(&volume))
            {
                continue;
            }
            owners.push(member.clone());
            if extracted.contains(member) {
                clean_owners.push(member.clone());
            } else if failed.contains(member) {
                failed_owners.push(member.clone());
            } else {
                pending_owners.push(member.clone());
            }
        }
        sort_dedup(&mut owners);
        sort_dedup(&mut clean_owners);
        sort_dedup(&mut failed_owners);
        sort_dedup(&mut pending_owners);
        let unresolved_boundary = topology
            .unresolved_spans
            .iter()
            .any(|span| (span.first_volume..=span.last_volume).contains(&volume));
        let ownerless = owners.is_empty();
        if ownerless {
            ownerless_volumes.push(volume);
        }
        let ownership_eligible = !ownerless
            && pending_owners.is_empty()
            && failed_owners.is_empty()
            && !unresolved_boundary;
        delete_decisions.insert(
            volume,
            RarVolumeDeleteDecision {
                owners,
                clean_owners,
                failed_owners,
                pending_owners,
                unresolved_boundary,
                ownership_eligible,
            },
        );
    }

    if !ownerless_volumes.is_empty() {
        tracing::error!(
            volumes = ?ownerless_volumes,
            "RAR plan invariant violated: volumes must have at least one owner"
        );
    }

    let deletion_eligible = delete_decisions
        .iter()
        .filter_map(|(volume, decision)| decision.ownership_eligible.then_some(*volume))
        .collect::<HashSet<_>>();
    let waiting_delete_conflicts: Vec<u32> = waiting_on_volumes
        .intersection(&deletion_eligible)
        .copied()
        .collect();
    let mut deletion_eligible = deletion_eligible;
    if !waiting_delete_conflicts.is_empty() {
        for volume in &waiting_delete_conflicts {
            deletion_eligible.remove(volume);
            if let Some(decision) = delete_decisions.get_mut(volume) {
                decision.ownership_eligible = false;
            }
        }
        tracing::error!(
            conflicts = ?waiting_delete_conflicts,
            "RAR plan invariant violated: waiting volumes must not be deletion eligible"
        );
    }

    let phase = if !member_names.is_empty()
        && member_names.iter().all(|member| extracted.contains(member))
        && topology
            .expected_volume_count
            .is_some_and(|expected| (0..expected).all(|volume| facts.contains_key(&volume)))
    {
        RarSetPhase::Complete
    } else if worker_active {
        RarSetPhase::Extracting
    } else if !ready_members.is_empty() {
        RarSetPhase::Ready
    } else if member_names.iter().any(|member| failed.contains(member)) {
        RarSetPhase::AwaitingRepair
    } else {
        RarSetPhase::WaitingForVolumes
    };

    Ok(RarDerivedPlan {
        phase,
        is_solid: metadata.is_solid,
        ready_members,
        member_names,
        waiting_on_volumes,
        deletion_eligible,
        delete_decisions,
        topology,
        fallback_reason: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use weaver_core::checksum;
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
        let unexpected_ownerless: Vec<u32> =
            ownerless.iter().copied().filter(|v| *v != 2).collect();
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
}
