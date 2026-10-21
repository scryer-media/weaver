// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a graph job says it delivered, member by member, read off the root
// namespace's marks. Nothing refetches and damaged bytes are kept, so a
// claim never changes what ships; it only says what the shipment is.

use std::collections::BTreeSet;

use weaver_graph::{Member, MemberId, NodeId, Tree, Why};

use crate::jobs::model::JobStatus;
use crate::post_processing::model::PipelineFailureStage;

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
// payload arrived either, and no file of the delivery is whole at any
// depth; beside a delivered sibling it is ordinary damage and ships as it
// stands.
pub(crate) fn refused<'c>(
    census: &'c [ClaimedMember],
    delivery: &Delivery,
) -> Vec<&'c ClaimedMember> {
    let payload = || census.iter().filter(|row| !row.furniture);
    if !delivery.whole.is_empty() || payload().any(|row| row.evidence) {
        return Vec::new();
    }
    payload().filter(|row| !row.evidence).collect()
}

// The files a graph job hands over, at every depth, named as they are
// written: a member an object unpacked is spent, and what it unpacked into
// is delivered in its place; what a refused object closed whole stays
// delivered beside the member it gave back. Read when the job ends, each
// file as its own claim says. A short file is unrepaired when a recovery
// object whose repair failed covers it, or covers a short volume it was
// unpacked from: those are the bytes the repair could not rebuild. A
// volume an object claimed is never a delivery of its own: what the object
// closed is. Refused, every volume of its set is delivered as it stands,
// never whole, however clean its bytes: the object failed, not the post.
// An object that disclaims a member (`Why::disclaims`) leaves it the
// posted file, whole when its bytes are.
// A member given back for a reason that withholds it (`Why::withholds`)
// delivered nothing: its bytes are a container nothing opened, neither
// whole nor short, and stay as they stand.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Delivery {
    pub(crate) whole: Vec<String>,
    // Damaged or missing with nothing that could rebuild them: delivered as
    // they stand.
    pub(crate) damaged: Vec<String>,
    // A recovery object that covers them could not rebuild them. Left as
    // they stand, and not a delivery.
    pub(crate) unrepaired: Vec<String>,
    // Given back withheld, with the reason its object refused it.
    pub(crate) withheld: Vec<(String, Why)>,
    // Every file delivered, whole or as it stands, as the walk met it.
    pub(crate) files: Vec<Delivered>,
}

// One file the job hands over, under the name the tree placed it at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Delivered {
    pub(crate) name: String,
    // The name the member wanted before the tree placed it.
    pub(crate) from: String,
    pub(crate) depth: u32,
    pub(crate) whole: bool,
}

impl Delivery {
    // `payload` picks the root members that are the post's files,
    // `furniture` the names of those files whose state never decides a
    // job, such as a text file posted beside the payload, and
    // `unrepaired` the members a recovery set whose repair could not
    // rebuild what it found covers, as their namespace says. `refused` says
    // why a member is a leaf.
    pub(crate) fn read(
        tree: &Tree,
        payload: impl Fn(MemberId) -> bool,
        furniture: impl Fn(&str) -> bool,
        unrepaired: impl Fn(NodeId, MemberId) -> bool,
        refused: impl Fn(NodeId, MemberId) -> Option<Why>,
    ) -> Self {
        let mut walk = Walk {
            tree,
            furniture: &furniture,
            unrepaired: &unrepaired,
            refused: &refused,
            opened: BTreeSet::new(),
            delivery: Self::default(),
        };
        if let Some(root) = tree.namespace(NodeId::ROOT) {
            for member in root.members().filter(|member| payload(member.id)) {
                walk.member(NodeId::ROOT, member, false);
            }
        }
        walk.delivery
    }

    // How a job that has nothing left to wait for ends: complete when
    // nothing it hands over is short or withheld and no recovery object
    // failed to rebuild what it found, partial when some file is whole
    // beside what is not, and failed when nothing is whole, for `failure`
    // when a recovery object could not rebuild what it found, and otherwise
    // for what was withheld. A failed rebuild is short whether or not a
    // member carries what it could not rebuild: a file the set describes
    // and nothing delivered is short too.
    pub(crate) fn status(&self, failure: Option<&str>) -> JobStatus {
        let short =
            !(self.damaged.is_empty() && self.unrepaired.is_empty() && self.withheld.is_empty());
        if !short && failure.is_none() {
            return JobStatus::Complete;
        }
        if !self.whole.is_empty() {
            return JobStatus::Partial;
        }
        let error = match failure
            .map(str::to_string)
            .or_else(|| self.withheld_error())
        {
            Some(error) => error,
            None => {
                let short: Vec<&str> = self
                    .damaged
                    .iter()
                    .chain(&self.unrepaired)
                    .map(String::as_str)
                    .collect();
                format!(
                    "no file was delivered whole; {} short: {}",
                    short.len(),
                    short.join(", ")
                )
            }
        };
        JobStatus::Failed { error }
    }

    // What was withheld, worded by why its object refused it: the names
    // one reason withheld, then the reason, for each reason in the order
    // the walk met it. `None` when nothing was.
    pub(crate) fn withheld_error(&self) -> Option<String> {
        let mut reasons: Vec<(&str, Vec<&str>)> = Vec::new();
        for (name, why) in &self.withheld {
            let reason = withheld_because(*why);
            match reasons.iter_mut().find(|(seen, _)| *seen == reason) {
                Some((_, names)) => names.push(name),
                None => reasons.push((reason, vec![name])),
            }
        }
        (!reasons.is_empty()).then(|| {
            reasons
                .iter()
                .map(|(reason, names)| format!("{} {reason}", names.join(", ")))
                .collect::<Vec<_>>()
                .join("; ")
        })
    }

    // The stage a short delivery names as the one that left it short: the
    // repair, when a file is short because a recovery object could not
    // rebuild it, and otherwise the engine that read it short.
    pub(crate) fn shortfall_stage(&self) -> PipelineFailureStage {
        if self.unrepaired.is_empty() {
            PipelineFailureStage::Extract
        } else {
            PipelineFailureStage::Repair
        }
    }
}

struct Walk<'t, F, R> {
    tree: &'t Tree,
    furniture: &'t F,
    unrepaired: &'t R,
    refused: &'t dyn Fn(NodeId, MemberId) -> Option<Why>,
    // Every volume of a set opens the same namespace; it is read once.
    opened: BTreeSet<NodeId>,
    delivery: Delivery,
}

impl<F: Fn(&str) -> bool, R: Fn(NodeId, MemberId) -> bool> Walk<'_, F, R> {
    // `short` says a volume the member was unpacked from is short where a
    // failed repair covers it. Furniture is a file of the post: a member an
    // object unpacked is payload, whatever it is named.
    fn member(&mut self, node: NodeId, member: &Member, short: bool) {
        let tree = self.tree;
        match tree.child_of(node, member.id) {
            Some((child, _)) if tree.recovers(child) => {}
            Some((child, _)) => self.namespace(child, short),
            None => {
                if !(node == NodeId::ROOT && (self.furniture)(&member.output_name)) {
                    self.leaf(node, member, short);
                }
                for kept in tree.kept(node, member.id) {
                    self.namespace(kept, short);
                }
            }
        }
    }

    // A file of the delivery. Short where a failed repair covers it, it is
    // unrepaired, since that is what left it so; given back for a reason
    // that withholds it, it is withheld; a volume of an object that refused
    // it is delivered as it stands, unless the object disclaimed it
    // (`Why::disclaims`); and otherwise it is whole when its bytes are.
    fn leaf(&mut self, node: NodeId, member: &Member, short: bool) {
        let claim = Claim::of(member);
        let name = member.output_name.clone();
        if claim != Claim::Complete && (short || (self.unrepaired)(node, member.id)) {
            self.delivery.unrepaired.push(name);
            return;
        }
        // A member given back that no object has taken since is withheld
        // by the tree, which holds its parse beside it.
        let refused =
            (self.refused)(node, member.id).or_else(|| self.tree.withholds(node, member.id));
        if let Some(why) = refused.filter(|why| why.withholds()) {
            self.delivery.withheld.push((name, why));
            return;
        }
        let whole = claim == Claim::Complete
            && (self.tree.refused_by(node, member.id).is_none()
                || refused.is_some_and(Why::disclaims));
        let delivery = &mut self.delivery;
        match whole {
            true => delivery.whole.push(name.clone()),
            false => delivery.damaged.push(name.clone()),
        }
        delivery.files.push(Delivered {
            name,
            from: member.name.clone(),
            depth: self.tree.depth(node),
            whole,
        });
    }

    fn namespace(&mut self, node: NodeId, short: bool) {
        let tree = self.tree;
        if !self.opened.insert(node) {
            return;
        }
        let Some(namespace) = tree.namespace(node) else {
            return;
        };
        let short = short || self.short_volume(node);
        // A buried member is no file: it says its object left it open.
        for below in namespace
            .members()
            .filter(|below| !tree.buried_member(node, below.id))
        {
            self.member(node, below, short);
        }
    }

    // Some volume of `node` is short and a failed repair covers it.
    fn short_volume(&self, node: NodeId) -> bool {
        let Some((parent, volumes)) = self.tree.volumes(node) else {
            return false;
        };
        let Some(namespace) = self.tree.namespace(parent) else {
            return false;
        };
        volumes
            .values()
            .filter_map(|volume| namespace.get(*volume))
            .any(|volume| {
                Claim::of(volume) != Claim::Complete && (self.unrepaired)(parent, volume.id)
            })
    }
}

// Why a member was withheld, as a job's error words it after its names. A
// password the object needed is one reason, whether it shut the member's
// bytes or the headers naming them.
fn withheld_because(why: Why) -> &'static str {
    match why {
        Why::Encrypted | Why::EncryptedHeader => "needs a password and none was given",
        Why::Compressed => "is compressed and no engine reads it",
        Why::Unmapped => "never placed every member it named",
        Why::OrderUnknown => "is a piece of a set nothing puts in order",
        _ => "was refused",
    }
}

fn damaged_bytes(member: &Member) -> u64 {
    member
        .coverage
        .damaged_ranges()
        .map(|range| range.len())
        .sum()
}
