// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a graph job says it delivered, member by member, read off the root
// namespace's marks. Nothing refetches and damaged bytes are kept, so a
// claim never changes what ships; it only says what the shipment is.

use std::collections::{BTreeMap, BTreeSet};

use weaver_graph::{Member, MemberId, NodeId, Sniff, Tree, VolumeIndex, Why, usable_type};

use crate::jobs::model::JobStatus;
use crate::pipeline::graph::leaf::Leaf;
use crate::post_processing::model::PipelineFailureStage;

use super::Failure;

// Below this share of its length, a member has produced no delivery
// evidence: what is on disk for it is a rounding error, not a short
// delivery.
const EVIDENCE_PERCENT: u64 = 10;

// How many files a partial delivery's reason names before it counts the
// rest, as the unwanted extension reason does.
const PARTIAL_NAMED: usize = 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Claim {
    // Whole: closed at its length, every byte clean.
    Complete,
    // Every byte is accounted for and some of it is damaged. Delivered as
    // it stands.
    Damaged { bytes: u64 },
    // Bytes never arrived, and nothing will fetch them.
    Missing { bytes: u64 },
}

impl Claim {
    // Only a whole member is usable: one covered and clean that nothing
    // closed, neither its producer nor a set's proof, was never checked.
    pub fn of(member: &Member) -> Self {
        let damaged = damaged_bytes(member);
        match member.length {
            Some(_) if member.is_whole() => Self::Complete,
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
pub struct ClaimedMember {
    pub(crate) member: MemberId,
    pub name: String,
    pub(crate) claim: Claim,
    // The member's articles that reached a terminal state without arriving.
    pub(crate) lost_articles: usize,
    // The bytes the post declared for those articles: what the terminal
    // record counts as failed, in the unit the job's size is declared in.
    pub lost_bytes: u64,
    pub(crate) declared_bytes: u64,
    // A member whose absence never fails a job, such as a text file beside
    // the payload.
    pub(crate) furniture: bool,
    // At least a tenth of the member arrived undamaged.
    pub evidence: bool,
}

impl ClaimedMember {
    pub fn read(member: &Member, furniture: bool) -> Self {
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
    // later article made it whole. A damaged one owes its damaged bytes as
    // well as its lost ones; a lost span read on past as damage is counted
    // once.
    pub fn failed_bytes(&self) -> u64 {
        match self.claim {
            Claim::Complete => 0,
            Claim::Damaged { bytes } => self.lost_bytes.max(bytes),
            Claim::Missing { .. } => self.lost_bytes,
        }
    }
}

// The members the job cannot be delivered without. A member that produced
// nothing is a hole handed over as a delivery only when nothing else of the
// payload arrived either, and no file of the delivery is whole at any
// depth; beside a delivered sibling it is ordinary damage and ships as it
// stands.
pub fn refused<'c>(census: &'c [ClaimedMember], delivery: &Delivery) -> Vec<&'c ClaimedMember> {
    let payload = || census.iter().filter(|row| !row.furniture);
    // Furniture delivered whole beside the payload is no delivery of it.
    let furniture: BTreeSet<&str> = census
        .iter()
        .filter(|row| row.furniture)
        .map(|row| row.name.as_str())
        .collect();
    if delivery
        .whole
        .iter()
        .any(|name| !furniture.contains(name.as_str()))
        || payload().any(|row| row.evidence)
    {
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
// posted file, whole when its bytes are, whatever its name says it is.
// Otherwise a member the post names as an archive volume, a split part or
// recovery data is never whole as a leaf: only what an object unpacks from
// it is a usable file.
// A member given back for a reason that withholds it (`Why::withholds`)
// delivered nothing: its bytes are a container nothing opened, neither
// whole nor short, and stay as they stand.
// A posted file none of whose bytes arrived is short only where its name
// says it is a file the delivery lacks (`Unarrived`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Delivery {
    pub whole: Vec<String>,
    // Damaged or missing with nothing that could rebuild them: delivered as
    // they stand.
    pub damaged: Vec<String>,
    // Posted files that never arrived under names that say nothing of what
    // they were: short unless a set extracted whole beside them.
    pub unnamed: Vec<String>,
    // Not whole, in a namespace a recovery set vouched for, and described
    // by none of its sets: nothing checks them, as only what a set
    // describes is checked. With them, a whole volume a vouching set proved
    // of a set whose first volume the release never carried, which nothing
    // can open. Delivered as they stand, and the job is no shorter for them.
    pub unchecked: Vec<String>,
    // A recovery object that covers them could not rebuild them. Left as
    // they stand, and not a delivery.
    pub unrepaired: Vec<String>,
    // Given back withheld, with the reason its object refused it.
    pub withheld: Vec<(String, Why)>,
    // Left out of the delivery for an unwanted extension
    // (`remove_unwanted`): no file of it, whole or short.
    pub removed: Vec<String>,
    // Every file delivered, whole or as it stands, as the walk met it.
    pub files: Vec<Delivered>,
    // The job was given a password to try, as its candidates say
    // (`GraphJob::has_passwords`): a member withheld for a password then
    // needs one that none given opens, not one never given.
    pub password_given: bool,
}

// What the recovery sets of a member's namespace say of it, the one answer
// the walk reads for every member it meets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Described {
    // A set whose repair failed covers it, or recovery the post carried for
    // its namespace could not be used: what is short of it is unrepaired.
    Unrepaired,
    // A set of its namespace vouched, clean or repaired, and none describes
    // it: only what a set describes is checked, and so nothing checked
    // it. Short, it ships as it stands and leaves the job no shorter.
    Unchecked,
    // Covered by a set of its namespace that vouched: its bytes are what
    // was posted, and say what it is.
    Vouched,
    // In a namespace no set vouched for: its own bytes say what it is.
    Otherwise,
}

// What a posted file that is not whole says of the delivery: short of it,
// unless none of its bytes arrived and its name says otherwise.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unarrived {
    // A file of the delivery, which is short of it.
    Short,
    // A volume or part posted again: another posted file named into the
    // same place arrived. It is no file of its own.
    Again,
    // Nothing: it may have been anything, a copy of a volume a set the
    // delivery extracted whole holds among them. Beside such a set, the
    // job completes.
    Unnamed,
}

// One file the job hands over, under the name the tree placed it at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delivered {
    pub name: String,
    // The name the member wanted before the tree placed it.
    pub from: String,
    pub depth: u32,
    pub whole: bool,
    // The CRC32 its object's headers state for its bytes, when they do.
    pub crc32: Option<u32>,
}

impl Delivery {
    // `payload` picks the root members that are the post's files,
    // `furniture` the names of those files that are side files, such as a
    // text file posted beside the payload: delivered whole, each is a file
    // of the delivery, and short beside a file that is no furniture it
    // decides nothing. `described`
    // what the recovery sets of a member's namespace say of it
    // (`Described`). `refused` says why a member is a leaf, and `unarrived`
    // what a posted file that is not whole was, when none of its bytes
    // arrived.
    pub fn read(
        tree: &Tree,
        payload: impl Fn(MemberId) -> bool,
        furniture: impl Fn(&str) -> bool,
        described: impl Fn(NodeId, MemberId) -> Described,
        refused: impl Fn(NodeId, MemberId) -> Option<Why>,
        piece: impl Fn(NodeId, MemberId) -> bool,
        unarrived: impl Fn(MemberId) -> Unarrived,
    ) -> Self {
        // Whether the post holds a file of its own that is no furniture.
        let beside = tree.namespace(NodeId::ROOT).is_some_and(|root| {
            root.members()
                .any(|member| payload(member.id) && !furniture(&member.output_name))
        });
        let mut walk = Walk {
            tree,
            beside,
            piece: &piece,
            unarrived: &unarrived,
            furniture: &furniture,
            described: &described,
            refused: &refused,
            opened: BTreeSet::new(),
            proved: BTreeMap::new(),
            delivery: Self::default(),
        };
        if let Some(root) = tree.namespace(NodeId::ROOT) {
            for member in root.members().filter(|member| payload(member.id)) {
                walk.member(NodeId::ROOT, member, false, false);
            }
        }
        walk.delivery
    }

    // One file the delivery hands over: whole, outside every set beside a
    // set that vouched (`unchecked`), never arrived under a name that says
    // nothing (`unarrived`), or as it stands.
    fn deliver(&mut self, file: Delivered, unarrived: Option<Unarrived>, unchecked: bool) {
        match (file.whole, unarrived) {
            (true, _) => self.whole.push(file.name.clone()),
            (false, _) if unchecked => self.unchecked.push(file.name.clone()),
            (false, Some(Unarrived::Unnamed)) => self.unnamed.push(file.name.clone()),
            (false, _) => self.damaged.push(file.name.clone()),
        }
        self.files.push(file);
    }

    // How a job that has nothing left to wait for ends, from the rulings:
    // complete when nothing it hands over is short or withheld and no
    // recovery set ruled it cannot rebuild what it covers, partial when some
    // file is whole beside what is not, and failed when nothing is whole,
    // in the words of the sets that could not rebuild, and otherwise for
    // what was withheld. A failed rebuild is short whether or not a member
    // carries what it could not rebuild: a file the set describes and
    // nothing delivered is short too. An unnamed file that never arrived
    // beside a set that extracted whole may have been a copy of one of its
    // volumes, and the job completes when it can; beside the post's own
    // files alone it is a file the post lacks. The feed giving the post up
    // words a failure and rules nothing: a delivery with a whole usable
    // output and nothing short is complete. A short delivery is partial only
    // when a whole file is of a type people open and no furniture beside the
    // payload (`usable_type`), whatever depth it came from; a job whole
    // files of no usable type alone stand for, an `.nfo` or a `file_id.diz`
    // among them, fails, and as every failed job publishes nothing, they
    // stay in its working directory. A file no set describes beside a set that vouched
    // (`unchecked`) is never short: nothing checked it.
    pub fn status(&self, failure: Option<&Failure>) -> JobStatus {
        let short = !(self.damaged.is_empty()
            && self.unrepaired.is_empty()
            && self.withheld.is_empty()
            && self.removed.is_empty()
            && (self.unnamed.is_empty() || self.extracted()));
        if !self.whole.is_empty() && !short && failure.is_none_or(|failure| failure.from_feed) {
            return JobStatus::Complete;
        }
        if self.whole.iter().any(|name| usable_type(name)) {
            return JobStatus::Partial;
        }
        let nothing = if self.whole.is_empty() {
            "no file was delivered whole"
        } else {
            "no usable file was delivered whole"
        };
        let error = match failure
            .map(|failure| failure.message.clone())
            .or_else(|| self.withheld_error())
        {
            Some(error) => error,
            None => {
                let short: Vec<&str> = self
                    .damaged
                    .iter()
                    .chain(&self.unrepaired)
                    .chain(&self.unnamed)
                    .chain(&self.unchecked)
                    .map(String::as_str)
                    .collect();
                match short.len() {
                    0 => nothing.to_owned(),
                    count => format!("{nothing}; {count} short: {}", short.join(", ")),
                }
            }
        };
        // What the policy left out is said beside why nothing usable is left.
        let error = match self.removed_error() {
            Some(removed) => format!("{error}; {removed}"),
            None => error,
        };
        JobStatus::Failed { error }
    }

    // Leave out of the delivery every file whose name `removes` says has an
    // unwanted extension the policy deletes: each is `removed`, whole or
    // short, and no longer a file the delivery hands over. One pass over
    // what the walk found, a name judged once.
    pub fn remove_unwanted(&mut self, removes: impl Fn(&str) -> bool) {
        let mut out: BTreeSet<String> = BTreeSet::new();
        self.files.retain(|file| {
            let removed = removes(&file.name);
            if removed {
                out.insert(file.name.clone());
            }
            !removed
        });
        // One a recovery object could not rebuild is no delivery, but it
        // stands on disk and the policy removes it all the same.
        self.unrepaired.retain(|name| {
            let removed = removes(name);
            if removed {
                out.insert(name.clone());
            }
            !removed
        });
        if out.is_empty() {
            return;
        }
        for list in [
            &mut self.whole,
            &mut self.damaged,
            &mut self.unnamed,
            &mut self.unchecked,
        ] {
            list.retain(|name| !out.contains(name));
        }
        self.removed.extend(out);
    }

    // What the policy left out, worded as the job's reason. `None` when
    // nothing was.
    pub fn removed_error(&self) -> Option<String> {
        crate::post_processing::model::unwanted_files_removed_reason(&self.removed)
    }

    // A set below the root extracted a file whole: an unnamed posted file
    // that never arrived beside it may have been a copy of one of its
    // volumes, and leaves the delivery no shorter.
    pub(crate) fn extracted(&self) -> bool {
        self.files.iter().any(|file| file.whole && file.depth > 0)
    }

    // What was withheld, worded by why its object refused it: the names
    // one reason withheld, sorted so the words never follow the order the
    // objects refused in, then the reason, for each reason in the order the
    // walk met it. A password withheld something, and the error opens with
    // the marker integrations read a password failure by. `None` when
    // nothing was.
    pub fn withheld_error(&self) -> Option<String> {
        let marker = if self
            .withheld
            .iter()
            .any(|(_, why)| matches!(why, Why::Encrypted | Why::EncryptedHeader))
        {
            crate::pipeline::extraction::error::PASSWORD_REQUIRED_MARKER
        } else {
            ""
        };
        let mut reasons: Vec<(String, Vec<&str>)> = Vec::new();
        for (name, why) in &self.withheld {
            let reason = withheld_because(name, *why, self.password_given);
            match reasons.iter_mut().find(|(seen, _)| *seen == reason) {
                Some((_, names)) => names.push(name),
                None => reasons.push((reason, vec![name])),
            }
        }
        for (_, names) in &mut reasons {
            names.sort_unstable();
        }
        (!reasons.is_empty()).then(|| {
            let reasons = reasons
                .iter()
                .map(|(reason, names)| format!("{} {reason}", names.join(", ")))
                .collect::<Vec<_>>()
                .join("; ");
            format!("{marker}{reasons}")
        })
    }

    // What a job that delivered in part lacks, worded as its reason: each
    // list of what is not whole, its names sorted, at most `PARTIAL_NAMED`
    // names in all and a count of the rest. Something withheld for a
    // password opens it with the marker integrations read a password
    // failure by.
    pub(crate) fn partial_error(&self) -> String {
        let marker = if self
            .withheld
            .iter()
            .any(|(_, why)| matches!(why, Why::Encrypted | Why::EncryptedHeader))
        {
            crate::pipeline::extraction::error::PASSWORD_REQUIRED_MARKER
        } else {
            ""
        };
        let withheld: Vec<&str> = self
            .withheld
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        let lists: [(&str, Vec<&str>); 5] = [
            (
                "unrepaired",
                self.unrepaired.iter().map(String::as_str).collect(),
            ),
            ("damaged", self.damaged.iter().map(String::as_str).collect()),
            // An unnamed file beside a set that extracted whole leaves the
            // job no shorter (`status`), and is not named.
            (
                "missing",
                if self.extracted() {
                    Vec::new()
                } else {
                    self.unnamed.iter().map(String::as_str).collect()
                },
            ),
            ("withheld", withheld),
            (
                "removed for an unwanted extension",
                self.removed.iter().map(String::as_str).collect(),
            ),
        ];
        let mut text = format!("{marker}delivered in part");
        let mut left = PARTIAL_NAMED;
        let mut more = 0usize;
        for (label, mut names) in lists {
            names.sort_unstable();
            names.dedup();
            if names.is_empty() {
                continue;
            }
            if left == 0 {
                more += names.len();
                continue;
            }
            let named = names.len().min(left);
            left -= named;
            more += names.len() - named;
            text.push_str(&format!("; {label}: {}", names[..named].join(", ")));
        }
        if more > 0 {
            text.push_str(&format!("; and {more} more"));
        }
        text
    }

    // The stage a short delivery names as the one that left it short: the
    // repair, when a recovery object ruled it could not rebuild what it
    // covers, whether or not it bound a member it could name short, or
    // when a file is short because such an object covers it; otherwise the
    // engine that read it short.
    pub fn shortfall_stage(&self, failure: Option<&Failure>) -> PipelineFailureStage {
        let ruled = failure.is_some_and(|failure| !failure.objects.is_empty());
        if ruled || !self.unrepaired.is_empty() {
            PipelineFailureStage::Repair
        } else {
            PipelineFailureStage::Extract
        }
    }
}

struct Walk<'t, F, R> {
    tree: &'t Tree,
    // The post holds a file of its own that is no furniture.
    beside: bool,
    furniture: &'t F,
    described: &'t R,
    refused: &'t dyn Fn(NodeId, MemberId) -> Option<Why>,
    // A member the post names as a piece of something else: an archive
    // volume, a split part or recovery data, never a file the user opens.
    piece: &'t dyn Fn(NodeId, MemberId) -> bool,
    unarrived: &'t dyn Fn(MemberId) -> Unarrived,
    // Every volume of a set opens the same namespace; it is read once.
    opened: BTreeSet<NodeId>,
    // Whether each namespace below the root asked of was unpacked from
    // volumes a set that vouched proved (`proved_namespace`), read once.
    proved: BTreeMap<NodeId, bool>,
    delivery: Delivery,
}

impl<F: Fn(&str) -> bool, R: Fn(NodeId, MemberId) -> Described> Walk<'_, F, R> {
    // `short` says a volume the member was unpacked from is short where a
    // failed repair covers it. Furniture is a file of the post: a member an
    // object unpacked is payload, whatever it is named.
    // `vouched` says `node`'s bytes were unpacked from volumes a set that
    // vouched proved whole (`vouched_volumes`).
    fn member(&mut self, node: NodeId, member: &Member, short: bool, vouched: bool) {
        let tree = self.tree;
        match tree.child_of(node, member.id) {
            Some((child, _)) if tree.recovers(child) => {}
            Some((child, _)) => self.namespace(child, short, vouched),
            None => {
                // Below the root, a member no object holds whose name says it
                // is recovery data is spent, as a posted recovery file at the
                // root is no payload: no file of the delivery, whole or short,
                // unless an object that is not a recovery object refused it.
                let spent = node != NodeId::ROOT
                    && tree.spends(node, member.id)
                    && tree
                        .refused_by(node, member.id)
                        .is_none_or(|owner| tree.recovers(owner));
                // Furniture posted at the root is a file of the delivery when
                // it is whole, and when the post holds nothing else; one that
                // is not whole beside other files decides nothing.
                let ignored = node == NodeId::ROOT
                    && self.beside
                    && Claim::of(member) != Claim::Complete
                    && (self.furniture)(&member.output_name);
                if !spent && !ignored {
                    self.leaf(node, member, short, vouched);
                }
                for kept in tree.kept(node, member.id) {
                    self.namespace(kept, short, vouched);
                }
            }
        }
    }

    // A file of the delivery. Short where a failed repair covers it, it is
    // unrepaired, since that is what left it so; given back for a reason
    // that withholds it, it is withheld; a volume of an object that refused
    // it is delivered as it stands, unless the object disclaimed it
    // (`Why::disclaims`); and otherwise it is whole when its bytes are.
    // A directory is no file of the delivery: it holds files, and is
    // neither whole nor short of its own.
    fn leaf(&mut self, node: NodeId, member: &Member, short: bool, vouched: bool) {
        if Leaf::is_directory(&member.output_name) {
            return;
        }
        let claim = Claim::of(member);
        let name = member.output_name.clone();
        let described = (self.described)(node, member.id);
        if claim != Claim::Complete && (short || described == Described::Unrepaired) {
            self.delivery.unrepaired.push(name);
            return;
        }
        // A member given back that no object has taken since is withheld
        // by the tree, which holds its parse beside it.
        let refused =
            (self.refused)(node, member.id).or_else(|| self.tree.withholds(node, member.id));
        // A whole volume below the root of a set that lacks a volume, where a
        // set that vouched proved what it was unpacked from: every byte of
        // it is what was posted, so the poster never shipped what the set
        // lacks. It is kept as it stands, below.
        let lacking =
            |why: Why| matches!(why, Why::Unmapped | Why::Unfound { .. } | Why::Scattered);
        let never_shipped = |walk: &mut Self| {
            claim == Claim::Complete && walk.proved_below(node, member.id, vouched)
        };
        if let Some(why) = refused.filter(|why| why.withholds())
            && !(lacking(why) && never_shipped(self))
        {
            self.delivery.withheld.push((name, why));
            return;
        }
        // A posted file none of whose bytes arrived is what its name says
        // it was: another posting of a volume is no file at all.
        let unarrived =
            (node == NodeId::ROOT && claim != Claim::Complete).then(|| (self.unarrived)(member.id));
        if unarrived == Some(Unarrived::Again) {
            return;
        }
        // A member an object disclaimed is the file it was posted as, and
        // whole when its bytes are, whatever its name says it is. Otherwise
        // a piece left as a leaf is delivered as it stands: whole bytes of a
        // volume nothing produced a file from are no usable file. So is a
        // container nothing opened, whatever its format: a leaf whose name
        // and front say an object reads it. What the object would unpack
        // from it is the end file, and nothing did.
        let whole = claim == Claim::Complete
            && (refused.is_some_and(Why::disclaims)
                || (!(self.piece)(node, member.id)
                    && !self.unopened(node, member)
                    && self.tree.refused_by(node, member.id).is_none()));
        // Short bytes no set describes, beside a set that vouched: nothing
        // checked them, and they ship as they stand. So does a whole volume
        // of a set whose first volume the release never carried, which a
        // set that vouched proved, here or in what it was unpacked from: no
        // object can open it, and its bytes are what was posted. Below the
        // root, so does a whole volume of a set short of a volume, or one no
        // object took, where a set that vouched proved what it was unpacked
        // from, at whatever depth it was lifted to: the poster never shipped
        // the rest. A set at the root, or one nothing proved, that holds its
        // first volume and lacks another is short as it was.
        let proved = vouched || described == Described::Vouched;
        let unchecked = (claim != Claim::Complete && described == Described::Unchecked)
            || (claim == Claim::Complete
                && !whole
                && ((proved && self.headless(node, member.id))
                    || ((refused.is_some_and(lacking)
                        || self.headless(node, member.id)
                        || (self.tree.refused_by(node, member.id).is_none()
                            && (self.piece)(node, member.id)))
                        && never_shipped(self))));
        self.delivery.deliver(
            Delivered {
                name,
                from: member.name.clone(),
                depth: self.tree.member_depth(node, member.id),
                whole,
                crc32: member.crc32,
            },
            unarrived,
            unchecked,
        );
    }

    // `vouched` says the namespace `node` was bound in was itself vouched.
    fn namespace(&mut self, node: NodeId, short: bool, vouched: bool) {
        let tree = self.tree;
        if !self.opened.insert(node) {
            return;
        }
        let Some(namespace) = tree.namespace(node) else {
            return;
        };
        let short = short || self.short_volume(node);
        let vouched = self.vouched_volumes(node, vouched);
        // A buried member is no file: it says its object left it open.
        for below in namespace
            .members()
            .filter(|below| !tree.holds_no_file(node, below.id))
        {
            self.member(node, below, short, vouched);
        }
    }

    // Every volume of `node` is whole, and a set that vouched covers it or
    // the namespace it lies in was vouched itself (`above`): what `node`
    // unpacks is the bytes that were posted, at any depth.
    fn vouched_volumes(&self, node: NodeId, above: bool) -> bool {
        let Some((parent, volumes)) = self.tree.volumes(node) else {
            return false;
        };
        let Some(namespace) = self.tree.namespace(parent) else {
            return false;
        };
        !volumes.is_empty()
            && volumes.values().all(|volume| {
                namespace.get(*volume).is_some_and(|volume| {
                    Claim::of(volume) == Claim::Complete
                        && (above || (self.described)(parent, volume.id) == Described::Vouched)
                })
            })
    }

    // Every byte of the member at `node` was unpacked from volumes a set
    // that vouched proved, `vouched` saying so of `node` itself; for a
    // member lifted into `node`, of the namespace it was lifted out of.
    // Never of a member posted at the root.
    fn proved_below(&mut self, node: NodeId, member: MemberId, vouched: bool) -> bool {
        if node != NodeId::ROOT && vouched {
            return true;
        }
        let tree = self.tree;
        match tree.lift_of(node, member) {
            Some(lift) => {
                let (from, below) = lift.from;
                let vouched = self.proved_namespace(from);
                self.proved_below(from, below, vouched)
            }
            None => false,
        }
    }

    // `node` lies below the root and was unpacked from volumes a set that
    // vouched proved, here or in what they were unpacked from
    // (`vouched_volumes`). Each namespace is read once.
    fn proved_namespace(&mut self, node: NodeId) -> bool {
        if node == NodeId::ROOT {
            return false;
        }
        if let Some(proved) = self.proved.get(&node) {
            return *proved;
        }
        let tree = self.tree;
        let above = tree
            .volumes(node)
            .is_some_and(|(parent, _)| self.proved_namespace(parent));
        let proved = self.vouched_volumes(node, above);
        self.proved.insert(node, proved);
        proved
    }

    // The member is a volume of an object that refused it and was bound
    // without its set's first volume: a set the release never carried the
    // head of.
    fn headless(&self, node: NodeId, member: MemberId) -> bool {
        self.tree
            .refused_by(node, member)
            .and_then(|object| self.tree.volumes(object))
            .is_some_and(|(_, volumes)| !volumes.contains_key(&VolumeIndex(0)))
    }

    // A leaf whose name and front say an object of some kind reads it: a
    // container that no object opened, so no file of the delivery.
    fn unopened(&self, node: NodeId, member: &Member) -> bool {
        self.tree.namespace(node).is_some_and(|namespace| {
            matches!(
                member.said(namespace.first_bytes(member.id), namespace, &|name| {
                    Some(self.tree.never_opens(name))
                }),
                Sniff::Object(_)
            )
        })
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
                Claim::of(volume) != Claim::Complete
                    && (self.described)(parent, volume.id) == Described::Unrepaired
            })
    }
}

// Why a member was withheld, as a job's error words it after its names. A
// password the object needed is one reason, whether it shut the member's
// bytes or the headers naming them: none was given, or `given` says the job
// had some and none of them opened it. A set short of a volume names the one
// it lacks in the numbering of `name`, a volume of the set, or its number where the name does not number it.
fn withheld_because(name: &str, why: Why, given: bool) -> String {
    let said = match why {
        Why::Encrypted | Why::EncryptedHeader if given => {
            "needs a password and no password given opens it"
        }
        Why::Encrypted | Why::EncryptedHeader => "needs a password and none was given",
        Why::Compressed => "is compressed and no engine reads it",
        Why::Unmapped => "never placed every member it named",
        Why::Unfound { volume } => {
            let lacks = weaver_objects::engine::volume_name(name, volume)
                .unwrap_or_else(|| format!("volume {}", u64::from(volume) + 1));
            return format!("never placed every member it named: unable to find {lacks}");
        }
        Why::InvalidArchive => "is an invalid archive rejected by validation",
        Why::TooLarge { needed, allowed } => return too_large(needed, allowed),
        Why::OverMachine { needed, memory } => {
            use crate::support::human_bytes;
            return format!(
                "needs {} of decode memory, this machine has {}",
                human_bytes(needed),
                human_bytes(memory)
            );
        }
        _ => "was refused",
    };
    said.to_owned()
}

// An archive whose decode needs more memory than a decode may hold, with
// the sizes the decoder that refused it said: "needs 2.0 GiB of decode
// memory, 1.0 GiB allowed".
fn too_large(needed: Option<u64>, allowed: Option<u64>) -> String {
    use crate::support::human_bytes;
    match (needed, allowed) {
        (Some(needed), Some(allowed)) => format!(
            "needs {} of decode memory, {} allowed",
            human_bytes(needed),
            human_bytes(allowed)
        ),
        (Some(needed), None) => format!(
            "needs {} of decode memory, more than could be had",
            human_bytes(needed)
        ),
        (None, Some(allowed)) => format!(
            "needs more decode memory than the {} allowed",
            human_bytes(allowed)
        ),
        (None, None) => "needs more decode memory than could be had".to_owned(),
    }
}

fn damaged_bytes(member: &Member) -> u64 {
    member
        .coverage
        .damaged_ranges()
        .map(|range| range.len())
        .sum()
}
