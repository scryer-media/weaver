use std::collections::{BTreeMap, HashMap, HashSet};

use super::topology::{ArchiveMember, ArchivePendingSpan, ArchiveTopology};
use crate::jobs::assembly::ArchiveType;
use weaver_rar::{RarArchive, RarVolumeFacts, archive::MemberPlannerState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RarSetPhase {
    WaitingForVolumes,
    Ready,
    Extracting,
    AwaitingRepair,
    FallbackFullSet,
    Complete,
}

impl RarSetPhase {
    pub(crate) fn as_str(self) -> &'static str {
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
pub(crate) struct RarReadyMember {
    pub(crate) name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RarVolumeDeleteDecision {
    pub(crate) owners: Vec<String>,
    pub(crate) clean_owners: Vec<String>,
    pub(crate) failed_owners: Vec<String>,
    pub(crate) pending_owners: Vec<String>,
    pub(crate) unresolved_boundary: bool,
    pub(crate) ownership_eligible: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct RarDerivedPlan {
    pub(crate) phase: RarSetPhase,
    pub(crate) is_solid: bool,
    pub(crate) ready_members: Vec<RarReadyMember>,
    pub(crate) member_names: Vec<String>,
    pub(crate) waiting_on_volumes: HashSet<u32>,
    pub(crate) deletion_eligible: HashSet<u32>,
    pub(crate) delete_decisions: BTreeMap<u32, RarVolumeDeleteDecision>,
    pub(crate) topology: ArchiveTopology,
    pub(crate) fallback_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct RarSetState {
    pub(crate) facts: BTreeMap<u32, RarVolumeFacts>,
    pub(crate) volume_files: BTreeMap<u32, String>,
    pub(crate) cached_headers: Option<Vec<u8>>,
    pub(crate) verified_suspect_volumes: HashSet<u32>,
    pub(crate) active_workers: usize,
    pub(crate) in_flight_members: HashSet<String>,
    pub(crate) phase: RarSetPhase,
    pub(crate) plan: Option<RarDerivedPlan>,
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

#[derive(Default)]
struct PlannerMemberReadiness {
    entries: usize,
    extractable: bool,
    missing_volumes: HashSet<u32>,
}

impl PlannerMemberReadiness {
    fn push(&mut self, state: MemberPlannerState) {
        self.entries += 1;
        self.extractable |= state.extractable;
        self.missing_volumes.extend(
            state
                .missing_volumes
                .into_iter()
                .map(|volume| volume as u32),
        );
    }

    fn is_extractable(&self) -> bool {
        self.extractable && self.missing_volumes.is_empty()
    }

    fn missing_volumes(&self) -> Vec<u32> {
        let mut missing = self.missing_volumes.iter().copied().collect::<Vec<_>>();
        missing.sort_unstable();
        missing
    }
}

pub(crate) fn contiguous_prefix_end(facts: &BTreeMap<u32, RarVolumeFacts>) -> Option<u32> {
    if !facts.contains_key(&0) {
        return None;
    }

    let mut next = 0u32;
    while facts.contains_key(&next) {
        next = next.saturating_add(1);
    }
    Some(next.saturating_sub(1))
}

pub(crate) fn build_plan(
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
    let mut planner_states = HashMap::<String, PlannerMemberReadiness>::new();
    for state in archive.planner_member_states() {
        planner_states
            .entry(state.name.clone())
            .or_default()
            .push(state);
    }
    topology.expected_volume_count = metadata.volume_count.map(|count| count as u32);
    let final_volume_seen = facts
        .get(&prefix_end)
        .is_some_and(|volume_facts| !volume_facts.more_volumes);
    let missing_for_member = |member: &weaver_rar::MemberInfo| {
        let mut missing = planner_states
            .get(&member.name)
            .map(PlannerMemberReadiness::missing_volumes)
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
                .is_some_and(PlannerMemberReadiness::is_extractable);
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
                .is_some_and(PlannerMemberReadiness::is_extractable);
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
mod tests;
