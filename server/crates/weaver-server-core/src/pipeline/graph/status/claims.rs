// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a graph job says it delivered, member by member, read off the root
// namespace's marks. Nothing refetches and damaged bytes are kept, so a
// claim never changes what ships; it only says what the shipment is.

use weaver_graph::{Member, MemberId, NodeId, Tree};

// Below this share of its length, a member has produced no delivery
// evidence: what is on disk for it is a rounding error, not a short
// delivery. The same share the classic census holds a file to.
const EVIDENCE_PERCENT: u64 = 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Claim {
    // Covered at its length, every byte clean.
    Complete,
    // Every byte is accounted for and some of it is damaged. Delivered as
    // it stands.
    Damaged { bytes: u64 },
    // Bytes never arrived, and nothing will fetch them.
    Missing { bytes: u64 },
}

impl Claim {
    pub(crate) fn of(member: &Member) -> Self {
        let damaged = damaged_bytes(member);
        match member.length {
            Some(_) if member.is_complete() && damaged == 0 => Self::Complete,
            Some(_) if member.is_complete() => Self::Damaged { bytes: damaged },
            length => Self::Missing {
                bytes: length
                    .unwrap_or(0)
                    .saturating_sub(member.coverage.bytes_covered()),
            },
        }
    }
}

// One payload member as the job's census reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClaimedMember {
    pub(crate) member: MemberId,
    pub(crate) name: String,
    pub(crate) claim: Claim,
    // The member's articles that reached a terminal state without arriving.
    pub(crate) lost_articles: usize,
    // The bytes the post declared for those articles: what the terminal
    // record counts as failed, in the unit the job's size is declared in.
    pub(crate) lost_bytes: u64,
    pub(crate) declared_bytes: u64,
    // A member whose absence never fails a job, such as a text file beside
    // the payload.
    pub(crate) furniture: bool,
    // At least a tenth of the member arrived undamaged.
    pub(crate) evidence: bool,
}

impl ClaimedMember {
    pub(crate) fn read(member: &Member, furniture: bool) -> Self {
        let delivered = member
            .coverage
            .bytes_covered()
            .saturating_sub(damaged_bytes(member));
        let evidence = match member.length {
            Some(length) => {
                delivered.saturating_mul(100) >= length.saturating_mul(EVIDENCE_PERCENT)
            }
            None => delivered > 0,
        };
        Self {
            member: member.id,
            name: member.output_name.clone(),
            claim: Claim::of(member),
            lost_articles: 0,
            lost_bytes: 0,
            declared_bytes: 0,
            furniture,
            evidence,
        }
    }

    // The bytes the terminal record counts against the member. A complete
    // member owes nothing, whatever was lost on the way: a repair or a
    // later article made it whole.
    pub(crate) fn failed_bytes(&self) -> u64 {
        match self.claim {
            Claim::Complete => 0,
            Claim::Damaged { .. } | Claim::Missing { .. } => self.lost_bytes,
        }
    }
}

// The members the job cannot be delivered without. A member that produced
// nothing is a hole handed over as a delivery only when nothing else of the
// payload arrived either; beside a delivered sibling it is ordinary damage
// and ships as it stands.
pub(crate) fn refused(census: &[ClaimedMember]) -> Vec<&ClaimedMember> {
    let payload = || census.iter().filter(|row| !row.furniture);
    if payload().any(|row| row.evidence) {
        return Vec::new();
    }
    payload().filter(|row| !row.evidence).collect()
}

// Some file the member was unpacked into, at any depth, arrived whole: the
// post was delivered. What an object above a repair found damaged and
// marked on the member stays marked, though the repair below rebuilt it,
// so the member's own marks cannot say so.
pub(crate) fn delivered_below(tree: &Tree, node: NodeId, member: MemberId) -> bool {
    let Some((child, _)) = tree
        .child_of(node, member)
        .filter(|(child, _)| !tree.recovers(*child))
    else {
        return false;
    };
    tree.namespace(child).is_some_and(|namespace| {
        namespace.members().any(|below| {
            if tree.child_of(child, below.id).is_some() {
                delivered_below(tree, child, below.id)
            } else {
                Claim::of(below) == Claim::Complete
            }
        })
    })
}

fn damaged_bytes(member: &Member) -> u64 {
    member
        .coverage
        .damaged_ranges()
        .map(|range| range.len())
        .sum()
}
