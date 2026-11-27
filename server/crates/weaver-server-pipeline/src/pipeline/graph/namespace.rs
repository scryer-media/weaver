// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The namespace write path: the one place bytes delivered to a namespace are
// placed, held or refused. Every delivery to every namespace of a job comes
// through here, so the holds budgets, the damage marks and the recovery
// evidence are enforced once, for the root and for every namespace below it,
// and so are the job's extraction caps: what an object writes, the members
// it announces, the names they take and how deep an object is bound.
//
// What becomes of a delivery depends only on what its member is:
//
//   undecided   its first bytes have not said what it is; the bytes are held,
//               and replayed once it is decided. Past the holds budgets
//               they are held in its spill file, and nothing pauses.
//   a leaf      the bytes are written to its file at their offset, damaged
//               or not; the namespace's coverage is the record of which.
//   bound       the bytes go down the tree to the object that owns it, and
//               whatever that object says comes back as effects, applied
//               here the same way.
//
// The tree records coverage and routes; this file is where bytes touch
// disk. It takes a node everywhere and never assumes the root.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use weaver_server_disk::operations::disk::volume::LoopSite;

use bytes::Bytes;
use weaver_graph::Posted;
use weaver_graph::holds::{
    Budget, HoldLedger, HoldsAccountant, HoldsNamespace, Location, Placed, Spans,
};
use weaver_graph::member::FIRST_BYTES;
use weaver_graph::{
    Algorithm, Builder, ByteRange, Coverage, Digest, Effect, Emit, Evidenced, Kind, Marked,
    MemberId, NodeId, Object, ObjectId, Placement, Provenance, Segment, Took, Tree, VolumeIndex,
    Why,
};

use super::checkpoint::why_name;
use super::envelope::{Envelopes, InPlace, Shelf, Shelved};
use super::leaf::{Leaf, LeafError};
use super::limits::{Caps, Device, Output};
use crate::pipeline::extraction::ExtractionFailure;
use weaver_server_disk::operations::disk::CapacityReader;

// The block-aligned segment checksums of a delivered range, which the source
// computed while it had the bytes. Asked in the namespace's own coordinates,
// where member M is addressed as volume M: exactly the root's numbering, and
// a numbering every namespace has. A source answers for the namespace it
// delivered into and leaves every other one without.
pub trait Segments {
    fn segments_for(
        &self,
        node: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        out: &mut Vec<Segment>,
    );
}

// Bytes for a member of a namespace.
#[derive(Clone, Debug)]
pub struct Arrival {
    pub node: NodeId,
    pub member: MemberId,
    pub offset: u64,
    // With the evidence of them that came with them: a member's check and
    // the sets over it fold it rather than hash the bytes again.
    pub bytes: Evidenced,
    pub provenance: Provenance,
}

impl Arrival {
    fn range(&self) -> ByteRange {
        ByteRange::at(self.offset, self.bytes.len() as u64)
    }
}

// What a write places, carried with it down the tree. New bytes reach the
// sets over every member that takes them, at any depth, once; bytes placed
// again from where they went reach none twice.
#[derive(Clone, Copy, Debug)]
// Each carries the place it was made from (`LoopSite`), which every wait of
// the loop its writes make counts under.
pub enum Write {
    // Bytes new to the job, posted as `posting` of the namespace at
    // `entered`: the source's checksums of them are the posting's.
    New {
        entered: NodeId,
        posting: MemberId,
        site: LoopSite,
    },
    Again(LoopSite),
}

impl Write {
    // Bytes new to the job, entering where `arrival` puts them, from `site`.
    fn new(arrival: &Arrival, site: LoopSite) -> Self {
        Self::New {
            entered: arrival.node,
            posting: arrival.member,
            site,
        }
    }

    // Where the write was made from.
    pub(crate) fn site(self) -> LoopSite {
        match self {
            Self::New { site, .. } | Self::Again(site) => site,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error(transparent)]
    Leaf(#[from] LeafError),
    #[error("member {member:?} of node {node:?} is not in its namespace")]
    UnknownMember { node: NodeId, member: MemberId },
    #[error("recovery {set:?} changed inputs and its original snapshot could not be restored")]
    StaleRecovery { set: NodeId },
    // A set proved or judged bytes the namespace holds, and they are not
    // there to deliver again under what it said of them.
    #[error("member {member:?} of node {node:?} lost proven bytes {range:?}")]
    ProofUnreadable {
        node: NodeId,
        member: MemberId,
        range: ByteRange,
    },
    // The job's extraction budget refused it; nothing more of the job is
    // written.
    #[error(transparent)]
    Refused(#[from] ExtractionFailure),
    // The owner is stopping and a volume the job writes to did not take
    // what it holds by the end of the stop: nothing more of the job is
    // queued. It fails nothing: the job goes on from its last checkpoint.
    #[error("the stop ended with a volume still holding the job's writes")]
    LeftAtStop,
}

// What one namespace has on disk and in hand.
#[derive(Debug)]
// A member's file and why it is a leaf are what a checkpoint keeps of it,
// so every change to either marks the member, under the leaves' marks.
pub struct Files {
    pub leaves: Marked<MemberId, Leaf>,
    pub holds: HoldLedger,
    // Where held bytes go when RAM runs short; opened on the first page-out.
    pub scratch: Option<Leaf>,
    // Where a member's held bytes go past the budgets, at their own offsets.
    //
    // A member's spill file, once made, is this one leaf until its
    // namespace goes, when it is discarded with the namespace's other files
    // (or a restore sweeps it, unnamed). It never goes through `discard` on
    // its own, so its path is never queued to leave while it can still be
    // written: a member that spills, releases and spills again writes the
    // same file. Once the ledger holds nothing of the member on file the
    // file is emptied, never removed, and not at once: bytes a committed
    // checkpoint names on file stay as it left them until a later one
    // commits (`release_spill`). A repair out with a handle on it reads
    // that range as changed, since the holds it read moved.
    pub spills: BTreeMap<MemberId, Leaf>,
    // The members the ledger holds nothing of on file since their spill
    // file's last write. The file may still have bytes, until it is emptied
    // at a commit.
    pub spills_empty: BTreeSet<MemberId>,
    // Why a member is a leaf when something refused to own it. Changed only
    // through `refuse` and `unrefuse`, which mark the member.
    refused: BTreeMap<MemberId, Why>,
    // Inside a restore only: what the checkpoint's hold rows name of each
    // member. Those bytes were on scratch or in a spill file, never in the
    // member's own file, whether or not the hold was taken back.
    recorded: BTreeMap<MemberId, Spans>,
    // Which namespace's files these are, distinct from every other made in
    // the process: what was read of `leaves` by their marks is of these
    // files alone, not of another the node had before.
    born: u64,
}

impl Files {
    fn new(namespace: HoldsNamespace, accountant: Arc<HoldsAccountant>) -> Self {
        static BORN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        Self {
            born: BORN.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            leaves: Marked::default(),
            holds: HoldLedger::new(namespace, accountant),
            scratch: None,
            spills: BTreeMap::new(),
            spills_empty: BTreeSet::new(),
            refused: BTreeMap::new(),
            recorded: BTreeMap::new(),
        }
    }

    fn refuse(&mut self, member: MemberId, why: Why) -> Option<Why> {
        self.leaves.mark(member);
        self.refused.insert(member, why)
    }

    fn unrefuse(&mut self, member: MemberId) {
        if self.refused.remove(&member).is_some() {
            self.leaves.mark(member);
        }
    }
}

// A repair's return being applied. A gap one slice fills can close a file
// before a later slice of the same batch replaces other bytes in it, so the
// members closed meanwhile are judged once the whole batch is down.
pub struct Acknowledging {
    pub set: NodeId,
    pub closed: BTreeSet<(NodeId, MemberId)>,
    // Bytes the namespace holds already are being delivered again under a
    // set's verdict: read back, they are the bytes every set over them
    // ruled on, so they take no ruling back.
    pub rereading: bool,
    // The return's own bytes are being placed: a slice, or its tail. They
    // are the set's ruling, so they take none back. Bytes that arrive
    // between two slices of a return applied a slice at a time are fed as
    // any others are.
    pub placing: bool,
}

// What a repair's return being applied has come to so far, slice by slice,
// until its tail rules on it (`NamespaceWriter::applied`).
struct Apply {
    set: NodeId,
    node: NodeId,
    found: Vec<(MemberId, ByteRange)>,
    outcome: Option<weaver_graph::recovery::RepairOutcome>,
    rejected: Vec<(NodeId, Vec<(weaver_graph::VolumeIndex, ByteRange)>)>,
    // The members a slice landed in.
    rewritten: BTreeSet<MemberId>,
    // Whether a slice was read back, and whether one landed: a return none
    // of whose slices landed rewrote nothing.
    offered: bool,
    landed: bool,
    // Whether a slice lands only where no repair wrote before it.
    contested: bool,
}

// A repair's return applied a slice at a time, as its reader reads the
// staged output back off the loop and counts each slice on the job's
// volumes (`RepairSlice`). Between two slices the loop goes on with the
// job's other mail under the open batch (`acknowledging`): a member that
// mail closes is judged with the batch at the tail, and a member it refuses
// is placed again there, as within an apply in one step. Only the return's
// own bytes are placed under the set's ruling (`Acknowledging::placing`).
struct Applying {
    apply: Apply,
    // The first failure placing a slice: nothing lands after it, and the
    // reader is told to read no more.
    error: Option<WriteError>,
    // The staged output, until the job hands it to its reader.
    output: Option<loan::Repaired>,
    // What the slices said, heard once the whole return is down, as an
    // apply in one step hears it.
    said: Vec<Effect>,
    // The slices placed so far.
    slices: u64,
}

// The tail of a repair's return (`NamespaceWriter::applied`) while it
// delivers again what its set ruled on, a read-back piece at a time. Each
// piece is counted on the job's volumes before it goes: by the loop when
// they have room now, or else by a thread standing for it while the loop
// goes on with the job's other mail under the open batch, as between two
// slices (`Loans::pay`, `Heard::TailPaid`). The loop never stands still for
// a volume here.
struct ReturnTail {
    set: NodeId,
    node: NodeId,
    outcome: Option<weaver_graph::recovery::RepairOutcome>,
    rejected: Vec<(NodeId, Vec<(weaver_graph::VolumeIndex, ByteRange)>)>,
    rewritten: BTreeSet<MemberId>,
    // A return none of whose slices landed rewrote nothing.
    turned_away: bool,
    // The step the tail is at.
    step: TailStep,
    // The namespace the piece it stands for is credited in, once counted.
    into: NodeId,
    // The ranges left to deliver again, the next first.
    ranges: std::collections::VecDeque<(MemberId, ByteRange, Provenance)>,
    // The range being delivered again: its member, what it is delivered
    // as, and the parts of it left, each with whether it arrived.
    member: MemberId,
    provenance: Provenance,
    parts: std::collections::VecDeque<(ByteRange, bool)>,
    // The next piece is paid by what the step that goes on with the tail
    // was prepaid with: the bytes that covers, zero when nothing was.
    paid: u64,
    // The bytes of the piece the tail stands for, which its credit covers.
    stood: u64,
    // What the tail said before it stood, heard once it is done.
    said: Vec<Effect>,
    // The ranges taken up to deliver again so far, and their bytes: said
    // once in the log when the tail has delivered them all.
    redelivered: (u64, u64),
}

// Where a return's tail is: the ranges its set found delivered again, what
// was held behind them released, the ranges the set vouches for delivered
// again, the members it rewrote reopened and shut, and what that left held
// released. A tail that stood goes on at the same step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TailStep {
    Found,
    Held,
    Proved,
    Close,
    Last,
}

// What one pass over the holds did (`NamespaceWriter::release_placed`).
#[derive(Debug, PartialEq, Eq)]
enum Released {
    Nothing,
    Some,
    // A paid pass stands for room for a span of `u64` bytes, credited in
    // the namespace given when it is counted.
    Standing(u64, NodeId),
}

// Where a return's tail stands.
#[derive(Debug, PartialEq, Eq)]
pub enum Tailed {
    // Done, with the slices the repair rebuilt when it rebuilt any.
    Done(Option<u64>),
    // Standing for room for its next piece of `bytes` (`Loans::pay`).
    Standing(u64),
}

// One slice of a repair's staged output, read back off the loop, with what
// its reader counted for it on the job's volumes.
#[derive(Debug)]
pub struct RepairSlice {
    pub member: MemberId,
    pub offset: u64,
    pub bytes: Result<bytes::Bytes, LeafError>,
    pub credit: weaver_server_disk::operations::disk::writer::Credit,
}

// The reader of a repair's staged output is done: every slice it read was
// said before. `whole` is false when it read no further than a slice it
// could not count before the end of a stop, or when it was told to stop.
#[derive(Debug)]
pub struct RepairRead {
    pub output: loan::Repaired,
    pub whole: bool,
}

// An archive given up on as an archive: its members are delivered or
// withheld as their bytes stand. Named by the first member given back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GivenBack {
    pub member: String,
    // The kind of object that gave it back; empty when none held it.
    pub container: String,
    pub why: Why,
}

pub struct NamespaceWriter {
    // The job's process-wide id, which with a node id names a namespace to
    // the holds accountant that every job shares.
    scope: u64,
    working_dir: PathBuf,
    // Where the files of the members the job delivers lie: the working
    // directory, unless the complete directory is on another device, when
    // it is the job's staging directory there, so what is delivered is
    // written once, on the device it is published on. Volumes, recovery
    // files, envelopes, holds and repair staging stay in the working
    // directory.
    output_dir: PathBuf,
    // The output directory is the working directory: every member's file
    // lies on the one device. Set with the directories, so no write
    // compares their paths.
    delivers_in_place: bool,
    accountant: Arc<HoldsAccountant>,
    // The free space of the working directory's device, which the holds
    // page out against, and of the output directory's, which what an
    // object produces is written against.
    pub free_disk: CapacityReader,
    pub output_free_disk: CapacityReader,
    caps: Caps,
    // Each namespace's files, marked as each changes: a checkpoint reads
    // again only the namespaces marked since it read last.
    pub files: Marked<NodeId, Files>,
    pub envelopes: Envelopes,
    // Member reads the recovery objects made while repairing.
    read_backs: u64,
    // Rejected members waiting for their input dependencies to be staged
    // off the loop, before coverage or inverse mappings can be changed.
    rechecks: BTreeMap<NodeId, Vec<(VolumeIndex, ByteRange)>>,
    // Sparse durable obligations, retained until a current verification answers.
    verifications: verification::Obligations,
    // Whether the job's holds have been paged out for a stop. Only then do
    // the holds on scratch go in the checkpoint. Once rows are written, the
    // bytes they name are kept until a later checkpoint commits, as
    // `scratch_rewinding` and `spills_emptying` say.
    stopped: bool,
    renamed: Vec<MemberId>,
    // Members whose files wait to move to the names the ledger gave them,
    // in the order it gave them.
    pub moves: moves::Moves,
    // Whether the job is ready to deliver, so the waiting moves may run.
    // Until then a file keeps the name it opened at, whatever the ledger
    // names its member meanwhile. Not stored: a restored job reaches the
    // delivery again and says so again.
    delivering: bool,
    // Where every file sits and waits to move, kept for new files to ask.
    pub kept: moves::Kept,
    // The names members left that the database may still give them, by the
    // ledger's key, each with the checkpoint that records they left once
    // one is taken.
    pub left: BTreeMap<String, Option<u64>>,
    // Every file that goes, each with the checkpoint that stops naming it
    // once one is taken. Until that checkpoint commits, the one the
    // database holds may name it, so it stays on disk until then.
    leaving: Vec<(Leaf, Option<u64>)>,
    // Every spill file the ledger holds nothing of on file, each with the
    // first checkpoint taken since, which names none of its bytes. The one
    // the database holds may still name them, so the file is emptied only
    // once that checkpoint commits, and not at all if the member spills
    // again first. Until then no new byte enters the file below the length
    // it had when let go: the ledger keeps the member's bytes there in RAM,
    // since a member started over brings different bytes to the same
    // offsets, and a restore would pair them with the state that checkpoint
    // recorded. Bytes past that length spill as ever.
    pub spills_emptying: BTreeMap<(NodeId, MemberId), Option<u64>>,
    // Each node whose scratch file nothing lives on, with the first
    // checkpoint taken since, which names none of it, and the extent then.
    // The one the database holds may name any byte below the extent, so
    // scratch offsets start over only once that checkpoint commits, and
    // only if nothing was allocated meanwhile: until then every page-out
    // lands past every byte a committed row names.
    pub scratch_rewinding: BTreeMap<NodeId, (u64, u64)>,
    // The latest checkpoint the database has committed.
    pub committed: u64,
    segments: Vec<Segment>,
    // Room for the effects of one routed delivery, empty between deliveries
    // and kept so an article's effects need no list of their own.
    pub effects: Vec<Effect>,
    // The set whose repair is being acknowledged: only its certified writes
    // retain its settled ruling.
    pub acknowledging: Option<Acknowledging>,
    // A repair's return being applied a slice at a time, until its reader
    // is done.
    applying: Option<Applying>,
    // A return's tail standing for room for its next piece.
    tail: Option<ReturnTail>,
    // What a release of held bytes outside a tail stands for: the bytes of
    // the piece a volume had no room for, and the namespace it is credited
    // in (`release_held`). The job pays it off the loop (`release_paid`).
    release_owed: Option<(u64, NodeId)>,
    // Members whose close came while a release was owed, oldest first: a
    // close says nothing more will arrive, and bytes still held for the
    // member may yet, so it waits until no release stands
    // (`close_owed_members`). Each is owed once.
    closes_owed: std::collections::VecDeque<(NodeId, MemberId)>,
    closes_owed_set: BTreeSet<(NodeId, MemberId)>,
    // Owed closes are being made (`close_owed_members`).
    closing_owed: bool,
    // Reads `Effect::ReadOn` asked whose bytes have writes still out, oldest
    // first: the loop does not wait for them. Each keeps what it waits for,
    // which the job waits for aside (`read_wait`), and is read once that
    // returned (`reads_waited`). The tree asks one read at a time of a
    // member, so a member has one here at most.
    reads_owed: Vec<ReadOwed>,
    // The credit the loop carries for a run's output it holds (`carried`).
    carried: carried::Carried,
    // For each namespace below the root, the root member's child it lies
    // under, learnt as objects are bound (`note_subtree`); and what each
    // such child's namespaces hold, carried or not, as their ledgers keep
    // it (`HoldLedger::tally_into`): what a step that passes bytes down to
    // that child may release to members' files.
    subtree_of: BTreeMap<NodeId, NodeId>,
    held_by_subtree: BTreeMap<NodeId, Arc<std::sync::atomic::AtomicU64>>,
    // Members refused while a repair's return was applied, each read as the
    // first piece of a file once the whole return is down: a later slice of
    // the same return may still rewrite the front of a piece beside it.
    refused_in_batch: BTreeSet<(NodeId, MemberId)>,
    // Archives given up on since the job last asked, for its events.
    given_back: Vec<GivenBack>,
    // The node whose run's own word, or whose run's retraction, is being
    // applied: what it retracts is the run's, not its object's.
    run_speaking: Option<NodeId>,
    // Members an object retracted of its own accord since the job last
    // asked, each with whether the tree's record made it the run's as it
    // went: one its run makes invalidates that run (`GraphJob::step_runs`).
    retracted: Vec<(NodeId, MemberId, bool)>,
    // Ranges of root members given back that came and are no longer in
    // hand, since the job last asked: the job fetches them again.
    unkept: Vec<(MemberId, ByteRange)>,
    // The archives already given up on: the object that gave its members
    // back (true), or the namespace whose members nothing held (false). One
    // archive is one event however many volumes it had.
    given_up: BTreeSet<(NodeId, bool)>,
    // Why the run of each object that places members itself last refused
    // to make the members it makes, until a run of it starts again: what
    // its container is withheld for (`run_refused`), and how many members
    // it left unmade. One reason per such object of the job, kept as long
    // as the job.
    run_refusals: BTreeMap<NodeId, (Why, usize)>,
    // What a repair or a staging that failed gave back to the disk as it
    // failed, until the job asks: the room it needs to run again.
    gave_back: Option<u64>,
    // What was left to write of the member whose write found no room, by
    // its declared length, until the job asks.
    left_to_write: Option<u64>,
    // The budgets each member has spilled past, said once each.
    spills_said: BTreeSet<(NodeId, MemberId, Budget)>,
    // The members whose file the file system refused to create for its
    // name (`skip_unnamable`), until the ledger renames them.
    unnamable: BTreeSet<(NodeId, MemberId)>,
    // The members a loss marked, until each is tried for zeros once nothing
    // can bring its bytes back (`zeros`).
    lost_marked: BTreeSet<(NodeId, MemberId)>,
    // How many times a member was skipped for its name.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub names_skipped: usize,
    // Their damage, to apply once the refused write has returned.
    unnamed_damage: Vec<Effect>,
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub diagnostics: Vec<weaver_graph::recovery::RepairDiagnostic>,
    placements: Vec<Placement>,
    // The run of a member's file going down to its object again, while it
    // does: the member, where the run lies, and the file it was read from.
    from_file: Option<FromFile>,
    // Each held volume's object, volume and placement number when a pass
    // last asked where its holds go: while the object says the same number,
    // it places them as it did then, and nothing of them goes down.
    asked: BTreeMap<(NodeId, VolumeIndex), (NodeId, VolumeIndex, u64)>,
    // Where the time a member write takes on disk is published, when the
    // job runs in a pipeline.
    metrics: Option<Arc<weaver_server_base::operations::metrics::PipelineMetrics>>,
    // The volume writers the job's files are written through; `None` for a
    // job whose leaves run their own file operations as they are asked.
    disk: Option<crate::pipeline::graph::disk::JobDisk>,
    // The nodes whose bytes are being said again, and the requests their
    // objects raised meanwhile that wait for that replay to end
    // (`Object::again_waits`), in the order raised.
    replaying: BTreeSet<NodeId>,
    again_held: Vec<Effect>,
    // What the job, waiting for room, last found its due recovery sets would
    // rebuild: asked again only once something it rests on changed.
    held_want: Option<HeldWant>,
    // The files each recovery set was last said to withhold, so the same
    // ones are said once.
    withheld_said: BTreeMap<NodeId, BTreeSet<MemberId>>,
    // What the deliveries that bound members gave their new objects, while
    // those members are handed over, innermost last: it is in each member's
    // record and with its object, but in no file or hold, so the hand-over
    // reads none of it from the member's file. Each names the object and
    // the volume of it the bytes went to.
    reached_now: Vec<(NodeId, MemberId, (NodeId, VolumeIndex), ByteRange)>,
    // A repair's read may still be checked (`moved_since`): set as a repair
    // is lent, cleared once its return is taken back or given up and no
    // other is out or kept (`end_repair_reads`). Only then do members'
    // leaves keep what their writes changed.
    repair_reads: bool,
    // The members whose leaves kept a change while a repair's read could be
    // checked, each once: what `end_repair_reads` empties.
    changes_kept: BTreeSet<(NodeId, MemberId)>,
    // How many requests are being answered at once, and the most there
    // ever were.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub answering: (u32, u32),
}

// A file of the posts with a length, none of whose bytes arrived: what a
// recovery set over it would rebuild whole.
pub(super) fn absent_from_posts(record: &weaver_graph::Member) -> bool {
    record.length.is_some_and(|length| length > 0)
        && !record.coverage.runs().any(|run| run.provenance.arrived())
}

// What a job waiting for room found its due recovery sets would rebuild,
// and what that rests on: the sets, the wait it was found in, the sizes of
// the disks, the posts as marked then, and the files none of whose bytes
// had arrived that a set covers.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct HeldWant {
    pub(super) due: Vec<NodeId>,
    pub(super) tickets: [Option<u64>; 2],
    pub(super) sizes: (Option<u64>, Option<u64>),
    pub(super) at: u64,
    pub(super) absent: BTreeSet<MemberId>,
}

// Bytes of `member` of `node` at `range`, read just now from the file
// `file`.
#[derive(Clone, Copy, Debug)]
struct FromFile {
    node: NodeId,
    member: MemberId,
    range: ByteRange,
    file: super::leaf::FileId,
}

// A cipher's tail read back before the ranges that fed its keys are
// uncovered: the node it is of, the read's serial, and its staged pieces
// with each one's provenance.
pub(crate) struct Tail {
    node: NodeId,
    serial: u64,
    staged: loan::Repaired,
    sources: Vec<Provenance>,
}

impl Tail {
    // A tail not to be said: the step failed before it.
    pub(crate) fn drop_read(self, tree: &mut Tree) {
        tree.again_done();
        let _ = self.staged.discard();
    }
}

impl NamespaceWriter {
    pub fn new(
        scope: u64,
        working_dir: PathBuf,
        accountant: Arc<HoldsAccountant>,
        free_disk: CapacityReader,
    ) -> Self {
        Self {
            scope,
            envelopes: Envelopes::new(working_dir.clone()),
            output_dir: working_dir.clone(),
            delivers_in_place: true,
            working_dir,
            accountant,
            output_free_disk: free_disk.clone(),
            free_disk,
            caps: Caps::unbounded(),
            files: Marked::default(),
            read_backs: 0,
            rechecks: BTreeMap::new(),
            verifications: verification::Obligations::default(),
            stopped: false,
            renamed: Vec::new(),
            moves: moves::Moves::default(),
            delivering: false,
            kept: moves::Kept::default(),
            left: BTreeMap::new(),
            leaving: Vec::new(),
            spills_emptying: BTreeMap::new(),
            scratch_rewinding: BTreeMap::new(),
            committed: 0,
            segments: Vec::new(),
            effects: Vec::new(),
            acknowledging: None,
            applying: None,
            tail: None,
            release_owed: None,
            closes_owed: std::collections::VecDeque::new(),
            reads_owed: Vec::new(),
            closes_owed_set: BTreeSet::new(),
            closing_owed: false,
            carried: carried::Carried::default(),
            subtree_of: BTreeMap::new(),
            held_by_subtree: BTreeMap::new(),
            refused_in_batch: BTreeSet::new(),
            given_back: Vec::new(),
            run_speaking: None,
            retracted: Vec::new(),
            unkept: Vec::new(),
            given_up: BTreeSet::new(),
            run_refusals: BTreeMap::new(),
            gave_back: None,
            left_to_write: None,
            spills_said: BTreeSet::new(),
            unnamable: BTreeSet::new(),
            lost_marked: BTreeSet::new(),
            #[cfg(any(test, feature = "pipeline-test-fixtures"))]
            names_skipped: 0,
            unnamed_damage: Vec::new(),
            #[cfg(any(test, feature = "pipeline-test-fixtures"))]
            diagnostics: Vec::new(),
            placements: Vec::new(),
            from_file: None,
            asked: BTreeMap::new(),
            metrics: None,
            disk: None,
            replaying: BTreeSet::new(),
            again_held: Vec::new(),
            held_want: None,
            withheld_said: BTreeMap::new(),
            reached_now: Vec::new(),
            repair_reads: false,
            changes_kept: BTreeSet::new(),
            #[cfg(any(test, feature = "pipeline-test-fixtures"))]
            answering: (0, 0),
        }
    }

    // Whether the member's leaf, about to be written, keeps what the write
    // changes: while a repair's read may be checked it does, and it is
    // emptied with the rest when none is left (`end_repair_reads`). Noted
    // before the write, so a write that fails part way is noted too.
    fn keeps_changes(&mut self, node: NodeId, member: MemberId) -> bool {
        if self.repair_reads {
            self.changes_kept.insert((node, member));
        }
        self.repair_reads
    }

    // No repair's read is left to check: the leaves that kept a change for
    // one forget it, and nothing more is kept until the next is lent. A
    // leaf gone since is passed over.
    pub(crate) fn end_repair_reads(&mut self) {
        self.repair_reads = false;
        // What a leaf kept for a check is neither recorded nor part of its
        // file's state: forgetting it marks nothing for a checkpoint.
        for (node, member) in std::mem::take(&mut self.changes_kept) {
            if let Some(leaf) = self
                .files
                .get_mut_unmarked(&node)
                .and_then(|files| files.leaves.get_mut_unmarked(&member))
            {
                leaf.forget_changes();
            }
        }
    }

    // The files of the members the job delivers lie under `output_dir`,
    // on the device `free_disk` reads.
    pub fn delivering_to(mut self, output_dir: PathBuf, free_disk: CapacityReader) -> Self {
        self.delivers_in_place = output_dir == self.working_dir;
        self.output_dir = output_dir;
        self.output_free_disk = free_disk;
        self
    }

    // The job's files are written through the volume writers of `disk`.
    pub fn writing_through(mut self, disk: Option<crate::pipeline::graph::disk::JobDisk>) -> Self {
        self.envelopes
            .write_through(disk.as_ref().map(|disk| Arc::clone(disk.working())));
        self.disk = disk;
        self
    }

    // The job's files are written through `disk` from here, as a job whose
    // working volume changed.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn move_disk_for_test(&mut self, disk: crate::pipeline::graph::disk::JobDisk) {
        self.envelopes
            .write_through(Some(Arc::clone(disk.working())));
        self.disk = Some(disk);
    }

    // Before the next piece an object asked for is said again, or the next
    // slice of a replay's output: the loop stands still while a volume the job writes to holds
    // its budget of bytes (`JobDisk::loop_wait_below`). An error once the
    // end of a stop passed with the budget still held: no further slice is
    // queued.
    pub(super) fn below_budget(&self, site: LoopSite) -> Result<(), WriteError> {
        match &self.disk {
            Some(disk) if !disk.loop_wait_below(site) => Err(WriteError::LeftAtStop),
            _ => Ok(()),
        }
    }

    // The namespace the slices of the return being applied land in.
    pub(crate) fn applying_into(&self) -> Option<NodeId> {
        self.applying.as_ref().map(|applying| applying.apply.node)
    }

    // The namespace the piece a standing tail waits for is credited in.
    pub(crate) fn tail_into(&self) -> Option<NodeId> {
        self.tail.as_ref().map(|tail| tail.into)
    }

    // The bytes a release of held bytes stands for, and the namespace they
    // are credited in (`release_held`).
    pub fn release_owed(&self) -> Option<(u64, NodeId)> {
        self.release_owed
    }

    // Whether a member's close waits for a release that stands, or for a
    // read owed of it (`close`).
    pub fn closes_owed(&self) -> bool {
        !self.closes_owed.is_empty()
    }

    // Whether a read `Effect::ReadOn` asked waits for writes still out.
    pub fn reads_owed(&self) -> bool {
        !self.reads_owed.is_empty()
    }

    // What the oldest read owed waits for, for the job to wait for aside.
    pub(crate) fn read_wait(
        &self,
    ) -> Option<weaver_server_disk::operations::disk::writer::Barrier> {
        self.reads_owed.first().map(|read| read.waits.clone())
    }

    // Whether the close of `member`, in any namespace, is owed.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn close_owed_of(&self, member: MemberId) -> bool {
        self.closes_owed_set.iter().any(|(_, owed)| *owed == member)
    }

    fn read_owed_of(&self, node: NodeId, member: MemberId) -> bool {
        self.reads_owed
            .iter()
            .any(|read| (read.node, read.member) == (node, member))
    }

    // The writer of the volume a file under `root` lies on, when the job
    // writes through one.
    pub fn disk_under(
        &self,
        root: &Path,
    ) -> Option<Arc<weaver_server_disk::operations::disk::writer::Submitter>> {
        self.disk.as_ref().map(|disk| Arc::clone(disk.under(root)))
    }

    // The volume of the working directory, whose read turns a loan's reads
    // of the job's scratch and spill files take.
    pub(super) fn working_volume(
        &self,
    ) -> Option<Arc<weaver_server_disk::operations::disk::volume::Volume>> {
        self.disk
            .as_ref()
            .map(|disk| Arc::clone(disk.working().volume()))
    }

    // Everything the job has submitted for its files, when some of it has
    // yet to return: what a thread of its own waits for before it reads or
    // opens them where they lie. `None` when the files already lie as said.
    pub(super) fn unsettled(&self) -> Option<crate::pipeline::graph::disk::JobBarrier> {
        self.disk
            .as_ref()
            .map(|disk| disk.barrier())
            .filter(|barrier| !barrier.returned())
    }

    pub fn disk(&self) -> Option<&crate::pipeline::graph::disk::JobDisk> {
        self.disk.as_ref()
    }

    pub(crate) fn delivering(self, output: Option<(PathBuf, CapacityReader)>) -> Self {
        match output {
            Some((dir, free_disk)) => self.delivering_to(dir, free_disk),
            None => self,
        }
    }

    // The directory a member's file lies under: the output directory for a
    // member the job delivers, which no object holds, and the working
    // directory for a recovery volume, which stays in its own file for its
    // repair to read and is never delivered.
    // The device that directory lies on.
    pub(super) fn device_for(&self, tree: &Tree, node: NodeId, member: MemberId) -> Device {
        if tree.child_of(node, member).is_none() {
            Device::Output
        } else {
            Device::Working
        }
    }

    pub(super) fn root_for(&self, tree: &Tree, node: NodeId, member: MemberId) -> &Path {
        if tree.child_of(node, member).is_none() {
            &self.output_dir
        } else {
            &self.working_dir
        }
    }

    // The volume the bytes of the posted file `member` land on as the loop
    // hands them on, and the other volume the same step may write them to:
    // while its first bytes have not decided its owner, where the article
    // that decides it may land, and for an object's volume, where that
    // object's members lie when it passes bytes on to them in the step
    // (`Object::passes_on`). Bytes an object holds lie under the working
    // directory, and so do those of a member not yet decided: its owner keeps
    // them in memory, in
    // scratch or in spill (`Tree::is_undecided`). A member with a file of its
    // own lands where that file lies, and any other where its file opens
    // (`Self::root_for`). A job whose directories lie on one volume lands
    // everything there. `None` for a job that writes through no writer. A
    // few lookups by key; nothing is walked.
    pub(crate) fn landing(
        &self,
        tree: &Tree,
        member: MemberId,
    ) -> Option<(&Arc<LandingVolume>, Option<OrLanding<'_>>)> {
        let disk = self.disk.as_ref()?;
        let working = disk.working().volume();
        let output = disk.under(&self.output_dir).volume();
        if Arc::ptr_eq(working, output) {
            return Some((working, None));
        }
        // An archive's volume is the object's: an object that passes its
        // bytes on to its members in the same step writes them there, to
        // the delivered files on output, and with them what those members,
        // and members below them, hold and release once decided.
        if let Some((child, _)) = tree.child_of(NodeId::ROOT, member) {
            let below = self
                .held_by_subtree
                .get(&child)
                .map_or(0, |held| held.load(std::sync::atomic::Ordering::Relaxed));
            let held = self.held_uncarried(NodeId::ROOT, member) + below;
            return Some((working, tree.passes_on(child).then_some((output, held))));
        }
        if tree.is_undecided(NodeId::ROOT, member) {
            let held = self.held_uncarried(NodeId::ROOT, member);
            return Some((working, Some((output, held))));
        }
        let member = tree
            .namespace(NodeId::ROOT)
            .map_or(member, |namespace| namespace.resolve(member));
        let leaf = self
            .files
            .get(&NodeId::ROOT)
            .and_then(|files| files.leaves.get(&member))
            .and_then(Leaf::volume);
        Some((leaf.unwrap_or(output), None))
    }

    // The job's extraction budget, which every write, announcement, name
    // and binding here is held to.
    pub fn capped(mut self, caps: Caps) -> Self {
        self.caps = caps;
        self
    }

    // Publish the time each member write takes on disk.
    pub(crate) fn metered(
        mut self,
        metrics: Option<Arc<weaver_server_base::operations::metrics::PipelineMetrics>>,
    ) -> Self {
        self.metrics = metrics;
        self
    }

    // The members given back since this was last asked.
    pub fn take_given_back(&mut self) -> Vec<GivenBack> {
        std::mem::take(&mut self.given_back)
    }

    // The root members' ranges a give-back left not in hand since this was
    // last asked.
    pub(crate) fn take_unkept(&mut self) -> Vec<(MemberId, ByteRange)> {
        std::mem::take(&mut self.unkept)
    }

    // The breach of the job's extraction budget, once one stands.
    pub fn refusal(&self) -> Option<&ExtractionFailure> {
        self.caps.refusal()
    }

    // The job's content policy, as the tree asks whether it may open a
    // member (`Caps::never_opens`).
    pub fn never_opens(&self) -> weaver_graph::NeverOpen {
        self.caps.never_opens()
    }

    // Whether the job's content policy has been read.
    pub(crate) fn policy_known(&self) -> bool {
        self.caps.policy_known()
    }

    // The job's content policy once read (`Caps::policy`).
    pub(crate) fn policy(
        &self,
    ) -> Option<std::sync::Arc<crate::post_processing::model::PostProcessingSettings>> {
        self.caps.policy()
    }

    // The policy was read: the members the tree asked about before it was
    // are asked again, once (`Tree::rule_known`), and what that binds is
    // handed what its members received, as any bind is.
    pub fn rule_known(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let mut effects = Vec::new();
        tree.rule_known(&mut self.placing(builder), &mut effects);
        self.apply_all(tree, builder, segments, effects, out)
    }

    // The job went quiet: the containers below the root that waited for it
    // are told whether they are layers of the release (`Tree::decide_layers`),
    // and what that binds is handed what its members received, as any bind
    // is.
    pub(crate) fn decide_layers(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let mut effects = Vec::new();
        tree.decide_layers(&mut self.placing(builder), &mut effects);
        self.apply_all(tree, builder, segments, effects, out)
    }

    // The job's content policy when it leaves files with an unwanted
    // extension out of the delivery (`Caps::removes`).
    pub fn removes(&self) -> Option<&crate::post_processing::model::PostProcessingSettings> {
        self.caps.removes()
    }

    // Read the job's content policy if it is still unread, and judge the
    // names that waited for it.
    pub fn learn_policy(&mut self) -> Result<(), String> {
        self.caps.learn_policy()
    }

    // The job paused or resumed: its extraction deadline does not run
    // while it is paused.
    pub fn hold_clock(&mut self, paused: bool) {
        self.caps.hold_clock(paused);
    }

    pub fn decode_clock(&self) -> super::decoding::DecodeClock {
        self.caps.decode_clock()
    }

    // The reader of the disk `device` is: the one a write there is judged
    // against, and so the one a job whose write found it full waits on.
    pub(crate) fn capacity(&self, device: Device) -> &CapacityReader {
        match device {
            Device::Output => &self.output_free_disk,
            Device::Working => &self.free_disk,
        }
    }

    // Whether the job waits for room on a disk, and its place among those
    // that wait.
    pub fn disk_held(&self) -> bool {
        self.caps.disk_held()
    }

    // What the write that just failed gave back to the disk, when it was a
    // repair's or a staging's, which remove their output as they fail.
    pub(crate) fn take_gave_back(&mut self) -> Option<u64> {
        self.gave_back.take()
    }

    // What was left to write of the member whose write just found no room.
    pub(crate) fn take_left_to_write(&mut self) -> Option<u64> {
        self.left_to_write.take()
    }

    // A decode window of `needs` bytes, the free space its disk keeps
    // included, found room in no folder: the job waits for that room on the
    // disk of its working directory, the first folder a window goes in, as
    // for a write that found that disk full. `quota` when a quota refused
    // the window's file: the wait is then for the free figure to rise.
    // The device a file at `path` lies on, for a failure a volume's writer
    // reports of it: the output directory's for a file under it, else the
    // working directory's. A path under both is under the nearer one.
    pub(crate) fn device_of(&self, path: &Path) -> Device {
        let under_output = path.starts_with(&self.output_dir);
        let nearer = !path.starts_with(&self.working_dir)
            || self.output_dir.components().count() >= self.working_dir.components().count();
        if under_output && nearer {
            Device::Output
        } else {
            Device::Working
        }
    }

    pub(crate) fn window_found_no_room(&mut self, needs: u64, quota: bool) -> WriteError {
        self.gave_back = Some(self.gave_back.unwrap_or(0).max(needs));
        let kind = match quota {
            true => std::io::ErrorKind::QuotaExceeded,
            false => std::io::ErrorKind::StorageFull,
        };
        WriteError::Leaf(LeafError::Io {
            path: self.working_dir.clone(),
            source: std::io::Error::from(kind),
            device: Device::Working,
        })
    }

    fn gave_back(&mut self, staged: &loan::Repaired) {
        let bytes = staged.gave_back();
        if bytes > 0 {
            self.gave_back = Some(self.gave_back.unwrap_or(0).max(bytes));
        }
    }

    pub(crate) fn disk_ticket(&self) -> Option<u64> {
        self.caps.disk_ticket()
    }

    // A reading came: whether the job has room again on every disk it
    // waits on. If it has, all it owes is promised again and its runs may
    // decode.
    pub fn wake_disk(&mut self) -> bool {
        self.caps.wake_disk(&self.output_free_disk, &self.free_disk)
    }

    // Whether the job owes nothing and has nothing to write again.
    pub fn owes_nothing(&self) -> bool {
        self.caps.owes_nothing()
    }

    // The job waits though nothing of it is left that would write: see
    // `Caps::wake_idle`. Whether it woke.
    pub fn wake_idle(&mut self) -> bool {
        self.caps.wake_idle()
    }

    // What the job waits for room for, and the folder on the disk it waits
    // on, or None when it does not wait. See `Caps::disk_wait`.
    pub(crate) fn disk_wait(&self) -> Option<(super::limits::DiskWait, PathBuf)> {
        let wait = self
            .caps
            .disk_wait((&self.output_free_disk, &self.free_disk))?;
        let dir = match wait.device {
            Device::Output => &self.output_dir,
            Device::Working => &self.working_dir,
        };
        Some((wait, dir.clone()))
    }

    // A run stopped for room takes back what it built while this is set:
    // see `Caps::rewriting`.
    pub(crate) fn rewriting(&mut self, rewriting: bool) {
        self.caps.rewriting(rewriting);
    }

    // The run at `node` speaks, or is retracted, until `None`: what is
    // retracted at `node` meanwhile is the run's (`take_retracted`).
    pub(crate) fn run_speaks(&mut self, node: Option<NodeId>) {
        self.run_speaking = node;
    }

    // Members objects retracted of their own accord since the job last
    // asked, in the order they went, each with whether it was the run's.
    pub(crate) fn take_retracted(&mut self) -> Vec<(NodeId, MemberId, bool)> {
        std::mem::take(&mut self.retracted)
    }

    // The node's run starts over, as behind a repair: the shelf moves on a
    // generation, and the node is marked so the next step brings it up.
    pub fn restart_shelf(&mut self, node: NodeId) {
        self.envelopes.restart(node);
    }

    // What the job waits for on its repairs' behalf, on both disks.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn wanted(&self) -> u64 {
        self.caps.wanted()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn clock_held(&self) -> bool {
        self.caps.clock_held()
    }

    // How many member reads every repair so far needed: the evidence fed as
    // bytes passed settles the rest without one.
    pub fn read_backs(&self) -> u64 {
        self.read_backs
    }

    // Completion takes over the filesystem only after all storage owners
    // give up their handles, including consumed volumes and partial files.
    // Coverage and names stay available for the terminal report.
    pub fn release_handles(&mut self) {
        self.envelopes.release_handles();
        let nodes: Vec<_> = self.files.keys().copied().collect();
        for node in nodes {
            let files = self.files.get_mut(&node).expect("listed namespace");
            if let Some(scratch) = &mut files.scratch {
                scratch.release_handle();
            }
            for spill in files.spills.values_mut() {
                spill.release_handle();
            }
            let members: Vec<_> = files.leaves.keys().copied().collect();
            for member in members {
                files
                    .leaves
                    .get_mut(&member)
                    .expect("listed member")
                    .release_handle();
            }
        }
        for (leaf, _) in &mut self.leaving {
            leaf.release_handle();
        }
    }

    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn open_files(&self) -> Vec<PathBuf> {
        self.files
            .values()
            .flat_map(|files| {
                files
                    .leaves
                    .values()
                    .chain(files.scratch.iter())
                    .chain(files.spills.values())
            })
            .chain(self.leaving.iter().map(|(leaf, _)| leaf))
            .filter(|leaf| leaf.is_open())
            .map(|leaf| leaf.path().to_path_buf())
            .chain(self.envelopes.open_files())
            .collect()
    }

    // Every member of the namespace with a file of its own, and its path.
    pub fn files(&self, node: NodeId) -> impl Iterator<Item = (MemberId, &Path)> + '_ {
        self.files
            .get(&node)
            .into_iter()
            .flat_map(|files| files.leaves.iter())
            .map(|(member, leaf)| (*member, leaf.path()))
    }

    // Every namespace that holds a file of its own, in node order.
    pub(crate) fn file_nodes(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.files.keys().copied()
    }

    // Every object that kept a file of a volume, in node order.
    pub(crate) fn kept_nodes(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.envelopes.nodes()
    }

    // The file an object at `node` keeps of its volume, at the volume's own
    // offsets, if it kept any of it.
    pub fn kept_file(&self, node: NodeId, volume: VolumeIndex) -> Option<PathBuf> {
        let shelf = self.envelopes.shelf_of(node)?;
        let state = shelf.lock();
        state
            .kept
            .get(&volume.0)
            .map(|kept| kept.path().to_path_buf())
    }

    // The bytes every object keeps in its envelopes, over every volume.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(crate) fn kept_bytes(&self) -> u64 {
        self.kept_by_node().map(|(_, kept, _)| kept).sum()
    }

    // The bytes each object keeps in its envelopes, over every volume, and
    // the files they lie in, by node.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(crate) fn kept_by_node(&self) -> impl Iterator<Item = (NodeId, u64, Vec<PathBuf>)> + '_ {
        self.envelopes.nodes().filter_map(|node| {
            let shelf = self.envelopes.shelf_of(node)?;
            let state = shelf.lock();
            let kept = state
                .kept
                .values()
                .map(|kept| kept.coverage.bytes_covered())
                .sum::<u64>();
            let files = state
                .kept
                .values()
                .map(|kept| kept.path().to_path_buf())
                .collect();
            Some((node, kept, files))
        })
    }

    // Why the member is a leaf, when an object or the holds budgets refused
    // to own it.
    pub fn refused(&self, node: NodeId, member: MemberId) -> Option<Why> {
        self.files.get(&node)?.refused.get(&member).copied()
    }

    // Why the run of the object at `node`, which places members itself,
    // refused to make the members it makes, while no run of it started
    // since.
    pub fn run_refused(&self, node: NodeId) -> Option<Why> {
        self.run_refusals.get(&node).map(|(why, _)| *why)
    }

    // Each object whose run refused to make members it makes, while no run
    // of it started since: its archive's name, how many members it left
    // unmade, and why. One row per such object of the job.
    pub fn unmade(&self, tree: &Tree) -> Vec<(String, usize, Why)> {
        self.run_refusals
            .iter()
            .filter(|(_, (_, unmade))| *unmade > 0)
            .map(|(node, (why, unmade))| (archive_name(tree, *node), *unmade, *why))
            .collect()
    }

    // Whether the file system refused the member's name, so no file of it
    // was made.
    pub fn unnamable(&self, node: NodeId, member: MemberId) -> bool {
        self.unnamable.contains(&(node, member))
    }

    // A repair's return is down. Each member refused while it was applied,
    // and refused still with no object holding it, may be the first piece
    // of a file, read on the bytes the return left every piece with.
    pub(crate) fn refused_after_return(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        for (node, member) in std::mem::take(&mut self.refused_in_batch) {
            let Some(why) = self.refused(node, member) else {
                continue;
            };
            if tree.child_of(node, member).is_some() {
                continue;
            }
            let mut effects = Vec::new();
            tree.refused_written(node, member, why, &mut self.placing(builder), &mut effects);
            #[cfg(any(test, feature = "pipeline-test-fixtures"))]
            tree.bound_kinds_recounted().unwrap();
            self.prune(tree)?;
            self.apply_each(
                tree,
                builder,
                segments,
                effects,
                Write::Again(LoopSite::Refused),
                out,
            )?;
        }
        Ok(())
    }

    // Whether a pass over the holds counts the member's holds in `node` as
    // asked of their object's map as it stands.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn holds_asked(&self, node: NodeId, member: MemberId) -> bool {
        self.asked.contains_key(&(node, held_volume(member)))
    }

    // Every held span goes down in this step, with no credit counted for
    // the release on the job's volumes: the step pays with whatever it was
    // prepaid with.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn release_held_uncounted(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.apply_unnamed(tree, builder, segments, out)?;
        while self.release_placed(tree, builder, segments, None, out)? == Released::Some {}
        Ok(())
    }

    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn held_bytes(&self, node: NodeId) -> u64 {
        self.files
            .get(&node)
            .map_or(0, |files| files.holds.total_held())
    }

    // Bytes for a member of a namespace. Every effect goes to `out` once
    // this writer has done its part, except writing a leaf, which is wholly
    // its own.
    pub fn deliver(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        arrival: Arrival,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let write = Write::new(&arrival, LoopSite::LeafWrite);
        self.arrive(tree, builder, segments, write, arrival, out)?;
        self.release_held(tree, builder, segments, out)
    }

    // Bytes for a member, by the one write path, inside a call that
    // releases the holds once it is done.
    pub fn arrive(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        write: Write,
        arrival: Arrival,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.place(tree, builder, segments, arrival, write, out)
    }

    // A member was announced. A directory exists at once, and so does a
    // member that will never have a byte; neither has bytes to sniff.
    pub fn announced(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.announce(tree, builder, segments, node, member, out)?;
        self.release_held(tree, builder, segments, out)
    }

    fn announce(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let record = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .ok_or(WriteError::UnknownMember { node, member })?;
        let (output_name, length) = (record.output_name.clone(), record.length);
        self.caps.admit_name(&output_name)?;
        self.caps.admit_member(tree, node, member)?;
        if Leaf::is_directory(&output_name) {
            self.settle(tree, builder, segments, node, member, out)?;
            let made = self
                .leaf(tree, node, member)?
                .create_directory()
                .map_err(WriteError::from);
            self.skip_unnamable(tree, (node, member), made)?;
        } else if length == Some(0) {
            return self.shut(tree, builder, segments, node, member, out);
        }
        self.bind_recoveries(tree, builder, segments, node, out)
    }

    // Nothing more will arrive for the member. The tree closes it and says
    // so, and the owning object hears it as a closed volume. While a release
    // of held bytes stands (`release_owed`), bytes held for the member may
    // still go down: the close is owed, and is made once no release stands
    // (`close_owed_members`). Whether the close was made now; false when it
    // is owed.
    pub fn close(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
        out: &mut Vec<Effect>,
    ) -> Result<bool, WriteError> {
        // Behind closes already owed, it is owed too, so they are made in
        // the order they came.
        if self.release_owed.is_some()
            || self.read_owed_of(node, member)
            || !self.closes_owed.is_empty()
        {
            if self.closes_owed_set.insert((node, member)) {
                self.closes_owed.push_back((node, member));
            }
            return Ok(false);
        }
        self.shut(tree, builder, segments, node, member, out)?;
        self.release_held(tree, builder, segments, out)?;
        Ok(true)
    }

    fn shut(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let mut effects = Vec::new();
        tree.close(node, member, &mut self.placing(builder), &mut effects);
        self.apply_each(
            tree,
            builder,
            segments,
            effects,
            Write::Again(LoopSite::Shut),
            out,
        )
    }

    // The member's length became known. Recovery objects match by length,
    // so they bind again.
    pub(crate) fn length_learned(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, member, length): (NodeId, MemberId, u64),
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let record = tree
            .namespace_mut(node)
            .and_then(|namespace| namespace.get_mut(member))
            .ok_or(WriteError::UnknownMember { node, member })?;
        if record.length.is_some() {
            return Ok(());
        }
        record.length = Some(length);
        self.bind_recoveries(tree, builder, segments, node, out)
    }

    // Effects the tree raised outside a delivery: an answered seek, a range
    // marked damaged. The ones left for the owner go to `out`.
    pub fn apply_all(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        effects: Vec<Effect>,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.apply_each(
            tree,
            builder,
            segments,
            effects,
            Write::Again(LoopSite::ApplyAll),
            out,
        )?;
        self.release_held(tree, builder, segments, out)
    }

    // `write` is what the bytes the effects carry are: what a write of new
    // bytes raises carries them on, and anything else carries none.
    fn apply_each(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        mut effects: Vec<Effect>,
        write: Write,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.apply_drain(tree, builder, segments, &mut effects, write, out)
    }

    // [`Self::apply_each`], draining a list the caller keeps: on an error
    // the effects not yet applied are dropped, as they always were.
    fn apply_drain(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        effects: &mut Vec<Effect>,
        write: Write,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let end = BatchEnd::of(effects);
        // What the delivery that bound a member gave its new object is in no
        // file from the first effect of the batch on, not only once the
        // batch ends: an object refused in the same batch hands those bytes
        // on from itself, as the hand-over reads the file around them.
        let depth = self.reached_now.len();
        self.reached_now.extend(end.reached.iter().copied());
        let applied = effects.drain(..).try_for_each(|effect| {
            self.apply(tree, builder, segments, effect, write, out)?;
            self.apply_unnamed(tree, builder, segments, out)
        });
        let finished = applied.and_then(|()| self.finish_batch(tree, builder, segments, end, out));
        self.reached_now.truncate(depth);
        finished
    }

    // What a batch leaves to do once every effect of it is applied.
    fn finish_batch(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        BatchEnd {
            bound,
            folded,
            given_back,
            ..
        }: BatchEnd,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        if given_back {
            self.prune(tree)?;
        }
        for (node, member, child) in bound {
            self.hand_over(tree, builder, segments, (node, member), child, out)?;
        }
        for (node, copy, held) in folded {
            self.fold_over(tree, builder, segments, (node, copy), &held, out)?;
        }
        Ok(())
    }
}

// What a batch of effects leaves to do once all of them are applied, read
// off the batch before the first is.
#[derive(Default)]
struct BatchEnd {
    bound: Vec<(NodeId, MemberId, NodeId)>,
    folded: Vec<(NodeId, MemberId, Coverage)>,
    given_back: bool,
    reached: Vec<(NodeId, MemberId, (NodeId, VolumeIndex), ByteRange)>,
}

impl BatchEnd {
    fn of(effects: &[Effect]) -> Self {
        // What a member received before it was bound reaches its object
        // once every effect raised with the bind is applied: the tree said
        // them in order, and bytes it gave the owner before the bind, for
        // this member or for one the new object's parent has not decided
        // yet, are never overtaken by what the object makes of the rest.
        // A member folded into another posting of it is handed over the
        // same way, as that member's bytes.
        let bound: Vec<(NodeId, MemberId, NodeId)> = effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Bound {
                    node,
                    member,
                    child,
                    ..
                } => Some((*node, *member, *child)),
                _ => None,
            })
            .collect();
        let folded: Vec<(NodeId, MemberId, Coverage)> = effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Folded {
                    node, copy, held, ..
                } => Some((*node, *copy, held.clone())),
                _ => None,
            })
            .collect();
        // An object that gave a volume back may have gone with it. Once
        // every volume given back in the batch has taken its envelope, what
        // the gone object still kept is pruned, once for the batch.
        let given_back = effects
            .iter()
            .any(|effect| matches!(effect, Effect::GivenBack { .. }));
        // What the delivery that bound a member gave its new object went to
        // the object, not to the member's file or holds: the hand-over
        // reads the file around it.
        let reached: Vec<(NodeId, MemberId, (NodeId, VolumeIndex), ByteRange)> = effects
            .iter()
            .filter_map(|effect| match effect {
                Effect::Reached {
                    node,
                    member,
                    child,
                    volume,
                    offset,
                    bytes,
                    ..
                } if bound.contains(&(*node, *member, *child)) => Some((
                    *node,
                    *member,
                    (*child, *volume),
                    ByteRange::at(*offset, bytes.len() as u64),
                )),
                _ => None,
            })
            .collect();
        Self {
            bound,
            folded,
            given_back,
            reached,
        }
    }
}

impl NamespaceWriter {
    // What a posting received before it folded into the member it repeats:
    // held, or in its own file when it was a leaf first. It goes down as
    // the member's bytes, each run with the provenance `held` says it
    // arrived with, since the posting's own record now keeps only what the
    // member takes of it, and the posting's file goes: it is never a file
    // of its own.
    fn fold_over(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, copy): (NodeId, MemberId),
        held: &Coverage,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.replay(
            tree,
            builder,
            segments,
            node,
            copy,
            ALL,
            Some(held),
            None,
            out,
        )?;
        self.replay_file(tree, builder, segments, node, copy, Some(held), out)?;
        let files = self.files_mut(node);
        files.unrefuse(copy);
        if let Some(leaf) = files.leaves.remove(&copy) {
            self.discard(leaf)?;
        }
        self.caps.forget(tree, node, copy);
        Ok(())
    }

    // What the member received before it was bound: held, or in its file
    // when it was a leaf first. A member that closed before then, or since
    // in this pass, has its object hear that last, from the tree. An object that gave the member up in the
    // delivery that bound it was handed none of what was held: the holds
    // stay the record of that, for the refusal to carry on.
    fn hand_over(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, member): (NodeId, MemberId),
        child: NodeId,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        // The file is shared before the holds go down, so what they carry
        // lands in it too.
        self.share_file(tree, node, member, None);
        if tree.child_of(node, member).map(|(bound, _)| bound) == Some(child) {
            self.replay(tree, builder, segments, node, member, ALL, None, None, out)?;
        }
        self.replay_file(tree, builder, segments, node, member, None, out)?;
        let mut effects = Vec::new();
        // A loss marked on the member before the bind comes down from the
        // tree now, to a data or a recovery volume alike.
        tree.handed_over(node, member, &mut self.placing(builder), &mut effects);
        self.apply_each(
            tree,
            builder,
            segments,
            effects,
            Write::Again(LoopSite::HandOver),
            out,
        )?;
        if tree.recovers(child) {
            self.bind_recoveries(tree, builder, segments, node, out)?;
        }
        Ok(())
    }

    // What an engine's run said at `node`. A run is its namespace's source,
    // as the articles are the root's, so the bytes it produces are a
    // delivery into that namespace and take the one write path: what an
    // object below keeps is captured, and a recovery volume is written to
    // its file, at any depth. Anything else it said is the object's word.
    pub fn said(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        emit: Emit,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let Emit::Bytes {
            member,
            offset,
            bytes,
            provenance,
        } = emit
        else {
            let mut effects = Vec::new();
            tree.emitted(node, vec![emit], &mut self.placing(builder), &mut effects);
            let speaking = self.run_speaking.replace(node);
            let applied = self.apply_all(tree, builder, segments, effects, out);
            self.run_speaking = speaking;
            return applied;
        };
        let arrival = Arrival {
            node,
            member,
            offset,
            bytes,
            provenance,
        };
        self.deliver(tree, builder, segments, arrival, out)
    }

    // A root member's article header named it before any of its bytes
    // came: the tree's ledger moves its name, every file whose name changed
    // follows, and the claims the name makes again bind through the tree as
    // any claim does, as a recovery set's renames do (`bind_sets`).
    pub(crate) fn header_named(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (member, name): (MemberId, &str),
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let mut effects = Vec::new();
        if tree
            .rename(
                NodeId::ROOT,
                member,
                name,
                &mut self.placing(builder),
                &mut effects,
            )
            .is_none()
        {
            return Ok(());
        }
        let (moves, claims): (Vec<Effect>, Vec<Effect>) = effects
            .into_iter()
            .partition(|effect| matches!(effect, Effect::Renamed { .. }));
        self.moved(tree, &moves)?;
        self.apply_all(tree, builder, segments, claims, out)
    }

    // A recovery object recognised the member: the tree's ledger moves its
    // name, and every file whose name changed follows.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn rename(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        node: NodeId,
        member: MemberId,
        name: &str,
    ) -> Result<String, WriteError> {
        let mut effects = Vec::new();
        let output_name = tree
            .rename(node, member, name, &mut self.placing(builder), &mut effects)
            .ok_or(WriteError::UnknownMember { node, member })?;
        self.moved(tree, &effects)?;
        Ok(output_name)
    }

    // Output names the ledger changed: each file written under its old name
    // wants its new one, in the order the ledger gave, and moves there once
    // the checkpoint that records it has committed. A member never written
    // has nothing to move.
    pub fn moved(&mut self, tree: &mut Tree, effects: &[Effect]) -> Result<(), WriteError> {
        let mut freed = Vec::new();
        for effect in effects {
            let Effect::Renamed {
                node,
                member,
                old,
                new,
            } = effect
            else {
                continue;
            };
            self.caps.admit_name(new)?;
            self.intend(*node, *member, new);
            // A member renamed since its name was refused is tried again
            // under the new one; a refusal of that marks it again.
            self.unnamable.remove(&(*node, *member));
            // A name no file of the member sits at is free in the ledger
            // once it moved the member off it. The database may still give
            // it the member, as its name or the one its file waits for, so
            // no file enters it until the checkpoint saying otherwise has
            // committed.
            if !self.sits_at(*node, *member, old) {
                if !Leaf::is_directory(old) {
                    self.left.insert(weaver_graph::names::key(old), None);
                }
                tree.vacated(old, &mut freed);
            }
        }
        if freed.is_empty() {
            return Ok(());
        }
        self.moved(tree, &freed)
    }

    // The builder, with this writer behind it: what an object taking a
    // delivery reads back of its placements is read where the writer put it.
    pub fn placing<'a>(&'a self, builder: &'a mut dyn Builder) -> Placing<'a> {
        Placing {
            builder,
            writer: self,
        }
    }

    // The namespace's face toward its members, which reads them wherever
    // their bytes are.
    // `site` is where its reads are made from: what their waits count under.
    pub fn access<'a>(&'a self, tree: &'a Tree, site: LoopSite) -> Access<'a> {
        Access {
            writer: self,
            tree,
            held: true,
            site,
        }
    }

    // Bytes placed again from where they went: a replay.
    fn route(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        arrival: Arrival,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        self.place(
            tree,
            builder,
            segments,
            arrival,
            Write::Again(LoopSite::ObjectRelease),
            out,
        )
    }

    fn place(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        arrival: Arrival,
        write: Write,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        // What is written of the arrival shares its buffer.
        let _lent = crate::pipeline::graph::leaf::lend(arrival.bytes.bytes());
        self.place_as(tree, builder, segments, (arrival, false), write, out)
            .map(|_| ())
    }

    // `place`, for a set's own repair as it comes back when `repair` says
    // so: bytes a repair already wrote stand against it. Returns how many of
    // the bytes the member took.
    fn place_as(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (arrival, repair): (Arrival, bool),
        write: Write,
        out: &mut Vec<Effect>,
    ) -> Result<u64, WriteError> {
        let (node, member) = (arrival.node, arrival.member);
        // Every article routes through here, so its effects go in the room
        // the writer keeps. A delivery that routes again while this one
        // applies finds the room taken and uses a list of its own.
        let mut effects = std::mem::take(&mut self.effects);
        let posted = Posted {
            offset: arrival.offset,
            bytes: arrival.bytes.clone(),
            provenance: arrival.provenance,
        };
        let took = if repair {
            tree.deliver_repair(
                node,
                member,
                posted,
                &mut self.placing(builder),
                &mut effects,
            )
        } else {
            tree.deliver(
                node,
                member,
                posted,
                &mut self.placing(builder),
                &mut effects,
            );
            arrival.range().len()
        };
        let recovery_volume = tree
            .child_of(node, member)
            .is_some_and(|(child, _)| tree.recovers(child));
        let applied = self.apply_drain(tree, builder, segments, &mut effects, write, out);
        self.effects = effects;
        applied?;
        // The front decides content matches, a finished member has the
        // length matches need, and a recovery volume's bytes may describe
        // more of the members.
        let front = arrival.offset < FIRST_BYTES as u64;
        let complete = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .is_some_and(|record| record.is_complete());
        if recovery_volume || front || complete {
            self.bind_recoveries(tree, builder, segments, node, out)?;
        }
        Ok(took)
    }

    // The bytes a bound member's object kept for no member of its own: its
    // envelopes, and what it could not place yet, which the namespace holds
    // for the member as it holds any bytes whose place is not known. Both
    // are as the object placed the bytes when it took them (`took`): a
    // later delivery of the same step may have given it its map since, and
    // bytes it took while it had none are still to be handed to it.
    fn capture(
        &mut self,
        tree: &Tree,
        (child, volume): (NodeId, VolumeIndex),
        arrival: &Arrival,
        took: &Took,
    ) -> Result<(), WriteError> {
        let mut placements = std::mem::take(&mut self.placements);
        placements.clear();
        let mut placed = 0;
        let mut moved = false;
        if tree.child_of(arrival.node, arrival.member) == Some((child, volume)) {
            // Pieces all into members come as their length alone; only
            // pieces that are not walk the bytes below.
            placements.extend_from_slice(&took.pieces);
            placed = took.covered;
            moved = tree.placement(child) != took.number;
        }
        // An object that gave the volume back in this delivery places none
        // of it. What no piece covers is held for the member, as any bytes
        // whose place is not known, and goes to its own file with the rest.
        if placed < arrival.range().len() {
            placements.push(Placement::Held {
                range: ByteRange::new(arrival.offset + placed, arrival.range().end),
            });
        }
        // Bytes just read from the member's file are what that file holds
        // where they were read.
        let in_place = self
            .from_file
            .filter(|read| {
                read.node == arrival.node
                    && read.member == arrival.member
                    && read.range.covers(arrival.range())
            })
            .map(|read| InPlace {
                file: read.file,
                offset: arrival.offset,
            });
        let kept = self.envelopes.capture(
            child,
            &placements,
            &arrival.bytes,
            arrival.provenance,
            in_place,
        );
        // What arrives is the member's bytes now, whatever was held of them
        // before: a repair rewrites what was held, and the held copy never
        // goes down after it.
        if let Some(files) = self.files.get_mut(&arrival.node) {
            files
                .holds
                .take(held_volume(arrival.member), arrival.range());
            // What was carried for the bytes taken goes with them.
            self.uncredit_past_resident(arrival.node, arrival.member);
        }
        let held: Vec<(u64, Evidenced)> = placements
            .iter()
            .filter_map(|placement| match placement {
                Placement::Held { range } => range.intersect(arrival.range()),
                _ => None,
            })
            .map(|piece| {
                let bytes = arrival
                    .bytes
                    .slice(index(piece.start - arrival.offset)..index(piece.end - arrival.offset));
                (piece.start, bytes)
            })
            .collect();
        self.placements = placements;
        kept?;
        // Held under a map the object has moved on from since: no pass has
        // asked the new map where these go.
        if moved && !held.is_empty() {
            self.asked
                .remove(&(arrival.node, held_volume(arrival.member)));
        }
        self.hold_for((arrival.node, arrival.member), held)
    }

    // What the object at `child` kept in its envelope of its volume, the
    // member, and now places elsewhere: read back from where it was kept,
    // a `READ_BACK` at a time, and each piece held for the member until the
    // object places it again. Nothing is held when any byte of the range was
    // never kept; a piece that does not read ends it, the pieces before it
    // held.
    pub fn unkept(
        &mut self,
        (node, member, child): (NodeId, MemberId, NodeId),
        (range, envelope, at): (ByteRange, u32, u64),
    ) -> Result<(), WriteError> {
        if !self
            .envelopes
            .keeps(child, envelope, ByteRange::at(at, range.len()))
        {
            return Ok(());
        }
        let mut done = 0;
        while done < range.len() {
            let take = (range.len() - done).min(READ_BACK);
            let mut bytes = Vec::new();
            if !self.envelopes.read(
                LoopSite::Unkept,
                child,
                envelope,
                ByteRange::at(at + done, take),
                &mut bytes,
            ) {
                return Ok(());
            }
            let held = vec![(range.start + done, Bytes::from(bytes).into())];
            self.hold_for((node, member), held)?;
            done += take;
        }
        Ok(())
    }

    // Bytes of a bound member, held until its object places them, as any
    // member's bytes are held: the member stays its object's.
    fn hold_for(
        &mut self,
        (node, member): (NodeId, MemberId),
        held: Vec<(u64, Evidenced)>,
    ) -> Result<(), WriteError> {
        for (offset, bytes) in held {
            self.keep_held(node, member, offset, bytes)?;
        }
        Ok(())
    }

    // The node's envelopes, shared with the run of an engine that reads
    // them as its volumes.
    pub fn shelf(&mut self, node: NodeId) -> Arc<Shelf> {
        self.envelopes.shelf(node)
    }

    pub fn shelf_of(&self, node: NodeId) -> Option<Arc<Shelf>> {
        self.envelopes.shelf_of(node)
    }

    // What the job asks through to hear of a run beginning to wait.
    pub(crate) fn watch(&self) -> &super::envelope::Watch {
        self.envelopes.watch()
    }

    // What rings as a run begins to wait on one of the shelves.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(crate) fn blocked(&self) -> Arc<tokio::sync::Notify> {
        self.envelopes.blocked()
    }

    // Change the node's envelopes as `change` says it did: what the shelf
    // keeps of them is a checkpoint's, so a change goes through here.
    pub(crate) fn update_shelf<E>(
        &mut self,
        node: NodeId,
        change: impl FnOnce(&Shelf, &mut Shelved) -> Result<bool, E>,
    ) -> Result<(), E> {
        self.envelopes.update_if(node, change)
    }

    fn apply(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        effect: Effect,
        write: Write,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        // Every delivery the tree takes for a member ends in a write or a
        // reach, after the namespace kept what it keeps of the bytes: the
        // member's front and the runs of what arrived. A rename changes the
        // names its record keeps. Here what changed is charged.
        if let Effect::WriteLeaf { node, member, .. }
        | Effect::Reached { node, member, .. }
        | Effect::Renamed { node, member, .. } = &effect
        {
            self.caps.admit_member(tree, *node, *member)?;
        }
        // Bytes an object produced that no member's file takes now are
        // charged here, before they are kept: a member another object reads,
        // but for a recovery volume, which is written to its own file, and a
        // member whose owner its front has not yet decided. The posts are
        // never charged.
        match &effect {
            Effect::Reached {
                node,
                member,
                child,
                offset,
                bytes,
                ..
            } if *node != NodeId::ROOT && !tree.recovers(*child) => {
                let end = offset.saturating_add(bytes.len() as u64);
                self.admit_kept(tree, *node, *member, end)?;
            }
            Effect::WriteLeaf {
                node,
                member,
                offset,
                bytes,
                ..
            } if *node != NodeId::ROOT && tree.is_undecided(*node, *member) => {
                let end = offset.saturating_add(bytes.len() as u64);
                self.admit_kept(tree, *node, *member, end)?;
            }
            _ => {}
        }
        if let Effect::Again { node, .. } = &effect {
            if self.replaying.contains(node) && tree.again_waits(*node) {
                self.again_held.push(effect);
                return Ok(());
            }
            return self.answer_again(tree, builder, segments, effect, out);
        }
        // Every effect before it is applied, so the bytes the objects are
        // owed are where the member's records say; read back a piece at a
        // time, as `Effect::Again` is.
        // A read whose bytes have writes still out is owed instead: the
        // loop does not wait for the writer, the job waits aside and the
        // read is made once they returned (`reads_waited`).
        if let Effect::ReadOn {
            node,
            member,
            range,
        } = effect
        {
            let read = ReadOwed {
                node,
                member,
                range,
                write,
                waits: match self.read_out(tree, node, member, range) {
                    Some(waits) => waits,
                    None => {
                        return self
                            .read_on_now(tree, builder, segments, node, member, range, write, out);
                    }
                },
            };
            self.reads_owed.push(read);
            return Ok(());
        }
        match &effect {
            Effect::WriteLeaf {
                node,
                member,
                offset,
                bytes,
                provenance,
            } => {
                let arrival = Arrival {
                    node: *node,
                    member: *member,
                    offset: *offset,
                    bytes: bytes.clone(),
                    provenance: *provenance,
                };
                self.taken(tree, segments, &arrival, write);
                // A repair landing on a member that closed without passing
                // its checksum lifts the damage it closed with, though
                // nothing closes it again: its bytes are judged as they are
                // now, as at a close. A set above can rebuild a member after
                // its own namespace ended, when its obligation was dropped.
                // One whose bytes had passed has had its verdict; the repair
                // is the set's proof, as on any member.
                let unproven = *provenance == Provenance::Repaired
                    && self.closed_unproven(tree, *node, *member);
                self.write_leaf(tree, arrival, write.site())?;
                if unproven {
                    self.judge_closed(tree, builder, segments, (*node, *member), out)?;
                }
                return Ok(());
            }
            Effect::Reached {
                node,
                member,
                child,
                volume,
                offset,
                bytes,
                provenance,
                took,
            } => {
                let arrival = Arrival {
                    node: *node,
                    member: *member,
                    offset: *offset,
                    bytes: bytes.clone(),
                    provenance: *provenance,
                };
                self.taken(tree, segments, &arrival, write);
                // A recovery object's volume stays in its member's own
                // file, where a repair reads its blocks back; any other
                // object's keeps are captured for it.
                if tree.recovers(*child) {
                    return self.write_file(
                        tree,
                        (*node, *member),
                        *offset,
                        bytes,
                        LoopSite::RecoveryVolume,
                    );
                }
                return self.capture(tree, (*child, *volume), &arrival, took);
            }
            Effect::Announced { node, member } => {
                self.announce(tree, builder, segments, *node, *member, out)?;
            }
            Effect::Closed { node, member } => {
                // A member that ended before its front could say what it is
                // is a leaf, whoever closed it: the root's source or an
                // object's emit.
                if tree.is_undecided(*node, *member) {
                    self.settle(tree, builder, segments, *node, *member, out)?;
                }
                // A leaf and a recovery volume live in their own files; a
                // member another object owns has no file to close.
                if in_own_file(tree, *node, *member) {
                    self.close_leaf(tree, *node, *member, write.site())?;
                    self.judge_closed(tree, builder, segments, (*node, *member), out)?;
                }
                self.bind_recoveries(tree, builder, segments, *node, out)?;
            }
            Effect::Bound {
                node,
                member,
                kind,
                child,
            } => {
                // Handed what it received before, by `apply_each`.
                self.bound(tree, *node, *member, *kind, *child);
                self.note_bound(tree, *node, *child);
            }
            // Handed over as the member's, by `apply_each`.
            Effect::Folded { .. } => {}
            Effect::Refused { node, member, why } | Effect::NeedsEngine { node, member, why } => {
                // A member handed to an engine is read by it: the archive was
                // not given up on.
                let given_up = matches!(effect, Effect::Refused { .. });
                self.refuse(tree, *node, *member, *why, given_up);
                // The engine the refusal handed the member to took it over.
                if matches!(effect, Effect::NeedsEngine { .. })
                    && let Some((engine, _)) = tree.child_of(*node, *member)
                    && let Some(kind) = tree.kind(engine)
                {
                    self.bound(tree, *node, *member, kind, engine);
                }
                self.materialise(tree, builder, segments, *node, *member, out)?;
                let mut effects = Vec::new();
                tree.bury(*node, *member, &mut effects);
                // A refused member, now in its own file, is the tree's to
                // place again (`Tree::refused_written`). One refused while a
                // repair's return is applied is placed once all of it is
                // down.
                if matches!(effect, Effect::Refused { .. }) {
                    if self.acknowledging.is_some() {
                        self.refused_in_batch.insert((*node, *member));
                    } else {
                        tree.refused_written(
                            *node,
                            *member,
                            *why,
                            &mut self.placing(builder),
                            &mut effects,
                        );
                        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
                        tree.bound_kinds_recounted().unwrap();
                    }
                }
                self.prune(tree)?;
                self.apply_each(
                    tree,
                    builder,
                    segments,
                    effects,
                    Write::Again(write.site()),
                    out,
                )?;
            }
            // Read back into its own file as a refused member is, nothing
            // refused, and then claimed again for the reason its object's
            // hold ended.
            Effect::Disclaimed { node, member, why } => {
                self.materialise(tree, builder, segments, *node, *member, out)?;
                let mut effects = Vec::new();
                tree.claim_again(
                    *node,
                    *member,
                    *why,
                    &mut self.placing(builder),
                    &mut effects,
                );
                self.prune(tree)?;
                self.apply_each(
                    tree,
                    builder,
                    segments,
                    effects,
                    Write::Again(write.site()),
                    out,
                )?;
            }
            Effect::GivenBack {
                node,
                member,
                from,
                volume,
            } => self.given_back(tree, (*node, *member), (*from, *volume))?,
            Effect::Unkept {
                node,
                member,
                child,
                range,
                envelope,
                at,
                ..
            } => {
                self.unkept((*node, *member, *child), (*range, *envelope, *at))?;
            }
            Effect::Retracted { node, member, made } => {
                if self.run_speaking != Some(*node) {
                    self.retracted.push((*node, *member, *made));
                }
                self.retract(tree, *node, *member)?;
                self.prune(tree)?;
            }
            Effect::RecoveryAt {
                node,
                volume,
                range,
                kind,
                handed,
            } => {
                self.recovery_at(
                    tree,
                    builder,
                    segments,
                    (*node, *volume, *range),
                    (*kind, handed),
                    out,
                )?;
            }
            Effect::Damaged {
                node,
                member,
                range,
            } => {
                // A member whose front stops at bytes that will never come
                // cannot say what it is from them: it is a leaf, as one that
                // ended first is. A repair there lets it say again.
                if tree.is_undecided(*node, *member) && !tree.front_to_come(*node, *member) {
                    self.settle(tree, builder, segments, *node, *member, out)?;
                }
                let mut effects = Vec::new();
                tree.damage_reached(
                    *node,
                    *member,
                    *range,
                    &mut self.placing(builder),
                    &mut effects,
                );
                self.apply_each(
                    tree,
                    builder,
                    segments,
                    effects,
                    Write::Again(write.site()),
                    out,
                )?;
            }
            // Its `Effect::Damaged` is applied on its own.
            Effect::Lost {
                origin,
                node,
                member,
                range,
            } => {
                let mut effects = Vec::new();
                self.marked_lost(
                    tree,
                    builder,
                    *origin,
                    (*node, *member, *range),
                    &mut effects,
                );
                self.apply_each(tree, builder, segments, effects, write, out)?;
            }
            Effect::Renamed { .. } => self.moved(tree, std::slice::from_ref(&effect))?,
            Effect::NeedsPassword { .. } => {}
            // Answered above.
            Effect::Again { .. } | Effect::ReadOn { .. } => {}
            Effect::Lifted {
                node,
                member,
                to,
                lifted,
            } => self.lifted(tree, (*node, *member), (*to, *lifted))?,
        }
        out.push(effect);
        Ok(())
    }

    // Every effect before it is applied, so what the object reads back of
    // its members is on disk. Each run's effects are applied before the
    // next run goes, as each delivery's are. What it reads back is staged
    // on disk a piece at a time, and read from there a piece at a time as
    // it is said again.
    //
    // A request the object raises while its bytes are said again, when it
    // may wait, is answered here once the pieces of the one before it have
    // gone and before that one's close is told again, as it would have
    // been inside it: the closes go last first. However many requests
    // follow one another, none is answered inside another.
    fn answer_again(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        effect: Effect,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let Effect::Again { node, .. } = &effect else {
            return Ok(());
        };
        let node = *node;
        let outermost = self.replaying.insert(node);
        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
        {
            self.answering.0 += 1;
            self.answering.1 = self.answering.1.max(self.answering.0);
        }
        let mut said: Vec<Effect> = Vec::new();
        let mut next = Some(effect);
        let answered = loop {
            if next.is_none() && outermost {
                next = self
                    .again_held
                    .iter()
                    .position(|held| matches!(held, Effect::Again { node: at, .. } if *at == node))
                    .map(|at| self.again_held.remove(at));
            }
            if let Some(effect) = next.take() {
                let Effect::Again { ranges, .. } = &effect else {
                    continue;
                };
                match self.say_again(tree, builder, segments, (node, ranges), out) {
                    Ok(()) => said.push(effect),
                    Err(error) => break Err(error),
                }
                continue;
            }
            let Some(effect) = said.pop() else {
                break Ok(());
            };
            let Effect::Again { ranges, .. } = &effect else {
                continue;
            };
            let mut effects = Vec::new();
            tree.again_closed(node, ranges, &mut self.placing(builder), &mut effects);
            let closed = self.apply_each(
                tree,
                builder,
                segments,
                effects,
                Write::Again(LoopSite::AnswerAgain),
                out,
            );
            tree.again_done();
            if let Err(error) = closed {
                break Err(error);
            }
            out.push(effect);
        };
        if answered.is_err() {
            for _ in said.drain(..) {
                tree.again_done();
            }
        }
        if outermost {
            self.replaying.remove(&node);
            self.again_held
                .retain(|held| !matches!(held, Effect::Again { node: at, .. } if *at == node));
        }
        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
        {
            self.answering.0 -= 1;
        }
        answered
    }

    // The pieces of one request said again. The staging they were read
    // into goes as soon as they are said: requests that follow one another
    // never hold more than one on disk.
    fn say_again(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, ranges): (NodeId, &[(VolumeIndex, ByteRange)]),
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let mut staged = loan::Repaired::again(&self.working_dir, node)?;
        staged.will_write(ranges.iter().map(|(_, range)| range.len()).sum());
        let mut sources = Vec::new();
        let serial = tree.again_read(
            node,
            ranges,
            &self.placing(builder),
            &mut |volume, posted| {
                staged.write(MemberId(volume.0), posted.offset, &posted.bytes);
                sources.push(posted.provenance);
            },
        );
        self.gave_back(&staged);
        self.staged_said(tree, node, ranges, &staged, false);
        let replayed = match staged.failed() {
            Some(error) => Err(WriteError::from(error)),
            None => self.again_staged(
                tree,
                builder,
                segments,
                (serial, node),
                &staged,
                sources,
                out,
            ),
        };
        let removed = staged.discard().map_err(WriteError::from);
        replayed.and(removed).inspect_err(|_| tree.again_done())
    }

    // What a replay read back and staged, said once per request in the log:
    // the object, how much it asked for again, and how much the staging
    // holds. `tail` for a cipher's tail (`read_tail`).
    fn staged_said(
        &self,
        tree: &Tree,
        node: NodeId,
        ranges: &[(VolumeIndex, ByteRange)],
        staged: &loan::Repaired,
        tail: bool,
    ) {
        tracing::info!(
            job_id = self.scope,
            node = node.0,
            kind = %tree.kind(node).map(|kind| kind.to_string()).unwrap_or_default(),
            ranges = ranges.len(),
            asked_bytes = ranges.iter().map(|(_, range)| range.len()).sum::<u64>(),
            staged_bytes = staged.sizes().map(|(_, bytes)| bytes).sum::<u64>(),
            tail,
            "replay staged"
        );
    }

    // Each staged piece of an `Effect::Again`, said again in the order it
    // was read, its effects applied before the next piece goes.
    #[allow(clippy::too_many_arguments)]
    fn again_staged(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (serial, node): (u64, NodeId),
        staged: &loan::Repaired,
        sources: Vec<Provenance>,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        // One slice per staged piece: no piece is longer than a slice.
        const _: () = assert!(weaver_graph::AGAIN_PIECE <= READ_BACK);
        for ((volume, offset, bytes), provenance) in staged.slices().zip(sources) {
            self.below_budget(LoopSite::AgainStaged)?;
            let posted = Posted {
                offset,
                bytes: bytes?.into(),
                provenance,
            };
            let mut effects = Vec::new();
            tree.replay_again(
                serial,
                node,
                VolumeIndex(volume.0),
                posted,
                &mut self.placing(builder),
                &mut effects,
            );
            self.apply_each(
                tree,
                builder,
                segments,
                effects,
                Write::Again(LoopSite::AgainStaged),
                out,
            )?;
        }
        Ok(())
    }

    // What uncovering ranges of a node's volume forgets past them, a
    // cipher's tail made under keys they fed, read back while those keys
    // still stand and staged on disk, to be said again by `say_tail` once
    // the ranges are uncovered.
    pub(crate) fn read_tail(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        node: NodeId,
        ranges: &[(VolumeIndex, ByteRange)],
    ) -> Result<Tail, WriteError> {
        let mut staged = loan::Repaired::again(&self.working_dir, node)?;
        let mut sources = Vec::new();
        let serial = tree.again_tail(
            node,
            ranges,
            &self.placing(builder),
            &mut |volume, posted| {
                staged.write(MemberId(volume.0), posted.offset, &posted.bytes);
                sources.push(posted.provenance);
            },
        );
        self.gave_back(&staged);
        // Every unarrived article bound below the root reads its tail, and
        // only a cipher's has one: said only when something was staged.
        if staged.sizes().next().is_some() {
            self.staged_said(tree, node, ranges, &staged, true);
        }
        if let Some(error) = staged.failed() {
            tree.again_done();
            let _ = staged.discard();
            return Err(error.into());
        }
        Ok(Tail {
            node,
            serial,
            staged,
            sources,
        })
    }

    // A tail `read_tail` staged, said again in the order it was read.
    pub(crate) fn say_tail(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        tail: Tail,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let Tail {
            node,
            serial,
            staged,
            sources,
        } = tail;
        let said = self.again_staged(
            tree,
            builder,
            segments,
            (serial, node),
            &staged,
            sources,
            out,
        );
        tree.again_done();
        let removed = staged.discard();
        said?;
        Ok(removed?)
    }

    // A member its object gave back is a leaf, and its file is what the
    // object kept of it, its envelope, which was always at the volume's own
    // offsets. Usually that file moves to the member's name. During a
    // reentrant handover a source leaf may still await replay: keep its
    // bytes and merge the returned envelope into it before releasing either.
    // An envelope nothing was written to is no file, as a member
    // none of whose bytes were written has none. What the object never
    // placed is still held for the member and goes into that file as held
    // bytes go into any leaf's. The member retains only the coverage backed
    // by that file and its holds. Until an object
    // claims it, the tree holds its parse beside it, which is what withholds
    // it; the claim that takes it reads the file in place, as it reads any
    // leaf's.
    pub fn given_back(
        &mut self,
        tree: &mut Tree,
        (node, member): (NodeId, MemberId),
        (from, volume): (NodeId, VolumeIndex),
    ) -> Result<(), WriteError> {
        let output_name = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .map(|record| record.output_name.clone())
            .ok_or(WriteError::UnknownMember { node, member })?;
        if let Some(kept) = self.envelopes.take(from, volume.0) {
            if kept.coverage.bytes_covered() == 0 {
                self.discard(kept.into_leaf())?;
            } else {
                let mut retained = kept.coverage.clone();
                let envelope = kept.into_leaf();
                let record = self.keeps_changes(node, member);
                if let Some(leaf) = self.files_mut(node).leaves.get_mut(&member) {
                    let length = leaf.len(LoopSite::GivenBack)?;
                    // A leaf awaiting replay owns the member's recorded
                    // ranges, including after restore without a CRC fold.
                    // Its allocated length never supplies new coverage.
                    let prior: Vec<_> = tree
                        .namespace(node)
                        .and_then(|namespace| namespace.get(member))
                        .into_iter()
                        .flat_map(|record| record.coverage.runs())
                        .filter_map(|run| run.range.intersect(ByteRange::new(0, length)))
                        .collect();
                    // Only this transfer reads the returned envelope. The
                    // unreplayed source stays in place, including its tail.
                    let check = tree
                        .namespace(node)
                        .and_then(|namespace| namespace.get(member))
                        .and_then(|record| record.check());
                    for run in retained.runs() {
                        let mut at = run.range.start;
                        while at < run.range.end {
                            let take = (run.range.end - at).min(READ_BACK);
                            let mut bytes = Vec::new();
                            envelope.read(LoopSite::GivenBack, at, index(take), &mut bytes)?;
                            match check {
                                Some(algorithm) => {
                                    let bytes = Evidenced::from(bytes);
                                    leaf.write_folding(
                                        LoopSite::GivenBack,
                                        at,
                                        &bytes,
                                        algorithm,
                                        None,
                                        record,
                                    )?;
                                }
                                None => {
                                    leaf.write(LoopSite::GivenBack, at, &bytes, None, record)?
                                }
                            }
                            at += take;
                        }
                    }
                    for range in prior {
                        retained.write(range, Provenance::Fresh);
                    }
                    self.discard(envelope)?;
                } else {
                    self.files_mut(node).leaves.insert(member, envelope);
                }
                self.uncover_unkept(tree, (node, member), &retained);
                self.intend(node, member, &output_name);
            }
        }
        self.release_to_leaf(tree, node, member)?;
        Ok(())
    }

    // A member lifted into the namespace above takes its file with it:
    // what it holds goes into the file first, and the file moves to the
    // member it is now, under that member's name. The member it was has no
    // file and no reason to be a leaf.
    fn lifted(
        &mut self,
        tree: &mut Tree,
        (node, member): (NodeId, MemberId),
        (to, lifted): (NodeId, MemberId),
    ) -> Result<(), WriteError> {
        self.release_to_leaf(tree, node, member)?;
        self.moves.drop_member(node, member);
        let files = self.files_mut(node);
        files.unrefuse(member);
        let leaf = files.leaves.remove(&member);
        self.caps.forget(tree, node, member);
        if let Some(leaf) = leaf {
            self.files_mut(to).leaves.insert(lifted, leaf);
        }
        let output_name = tree
            .namespace(to)
            .and_then(|namespace| namespace.get(lifted))
            .map(|record| record.output_name.clone())
            .ok_or(WriteError::UnknownMember {
                node: to,
                member: lifted,
            })?;
        self.intend(to, lifted, &output_name);
        let kept = self.kept_device();
        self.caps.restore(tree, to, lifted, kept);
        Ok(())
    }

    // A byte the member's coverage says arrived that neither the envelope
    // given back as its file nor its holds have is not in hand, as a
    // restart's lost range is not, and comes again. A restore leaves one:
    // an envelope comes back as far as its record and its file reach, the
    // member's coverage as far as what arrived, which counts bytes that
    // were only held in memory. So does an object that placed a volume's
    // bytes in a member of its own before the volume's last header said it
    // is not where the object held it. A root member's range is fetched
    // again, by the job; until it is in, the member is not whole, and
    // nothing rules on it as though it were. A range held as damage may
    // never have arrived at all, and stays.
    fn uncover_unkept(
        &mut self,
        tree: &mut Tree,
        (node, member): (NodeId, MemberId),
        kept: &Coverage,
    ) {
        let Some(record) = tree.namespace(node).and_then(|found| found.get(member)) else {
            return;
        };
        let mut unkept = Coverage::new();
        for run in record.coverage.runs() {
            if !run.provenance.is_damaged() {
                unkept.write(run.range, run.provenance);
            }
        }
        for run in kept.runs() {
            unkept.erase(run.range);
        }
        if let Some(files) = self.files.get(&node) {
            for range in files.holds.held_ranges(held_volume(member)) {
                unkept.erase(range);
            }
        }
        let unkept: Vec<ByteRange> = unkept.runs().map(|run| run.range).collect();
        if node == NodeId::ROOT {
            self.unkept
                .extend(unkept.iter().map(|range| (member, *range)));
        }
        if let Some(namespace) = tree.namespace_mut(node) {
            for range in unkept {
                namespace.uncover(member, range);
            }
        }
    }

    // New bytes a member took, at any depth, reach the sets over it as they
    // pass, with the evidence they came with, so a set has no slice of them
    // to read back. In the namespace they entered they are the posting's,
    // whose source's checksums go with them.
    fn taken(&mut self, tree: &mut Tree, segments: &dyn Segments, arrival: &Arrival, write: Write) {
        if let Write::New {
            entered, posting, ..
        } = write
        {
            let posting = if arrival.node == entered {
                posting
            } else {
                arrival.member
            };
            self.feed(tree, segments, arrival, posting);
        }
    }

    // `site` is where the write was made from (`Write::site`).
    fn write_leaf(
        &mut self,
        tree: &mut Tree,
        arrival: Arrival,
        site: LoopSite,
    ) -> Result<(), WriteError> {
        let (node, member) = (arrival.node, arrival.member);
        // Bytes whose member is not yet decided wait for it to be.
        if tree.is_undecided(node, member) {
            return self.keep_held(node, member, arrival.offset, arrival.bytes);
        }
        self.release_to_leaf(tree, node, member)?;
        // A repair's bytes into a post were charged before they went out
        // (`admit_repaired`), or write over room the post already holds:
        // they do not land as fetched bytes do.
        let lands = node != NodeId::ROOT || arrival.provenance != Provenance::Repaired;
        let written = self.write_into(
            tree,
            (node, member),
            (arrival.offset, &arrival.bytes),
            lands,
            site,
        );
        self.skip_unnamable(tree, (node, member), written)
    }

    // Hold bytes of the member in its namespace's ledger: in RAM, paged to
    // scratch as the ledger says, and past the budgets in the member's spill
    // file. Every byte a member waits with comes through here, bound or
    // undecided, and every one stays held: pressure never gives a member up.
    pub fn keep_held(
        &mut self,
        node: NodeId,
        member: MemberId,
        offset: u64,
        bytes: Evidenced,
    ) -> Result<(), WriteError> {
        let free_disk = self.free_disk_bytes();
        let spilled = bytes.clone();
        let len = bytes.len() as u64;
        let files = self.files_mut(node);
        // Bytes held over ones the member's file already has leave the
        // tree's coverage as it was, so the hold itself marks the member.
        files.leaves.mark(member);
        let outcome = files
            .holds
            .hold(held_volume(member), offset, bytes, free_disk);
        // What a run's output holds in memory is carried for; what the hold
        // cut over or paged out of memory no longer is.
        if outcome.placed == Placed::Ram {
            self.credit_held(node, member, len);
        }
        self.uncredit_past_resident(node, member);
        for page_out in &outcome.page_outs {
            self.uncredit_past_resident(node, held_member(page_out.volume));
        }
        for page_out in &outcome.page_outs {
            self.scratch(node)?.write(
                LoopSite::Spill,
                page_out.scratch_offset,
                &page_out.bytes,
                None,
                false,
            )?;
        }
        if let Placed::Spilled(budget) = outcome.placed {
            self.spill_write(node, member, offset, &spilled)?;
            self.said_spill(node, member, budget, free_disk);
            return Ok(());
        }
        // The new bytes may have replaced the last the member held on file.
        self.release_spill(node, member)
    }

    // A member's first spill past a budget is said once, with how far that
    // budget stands.
    fn said_spill(&mut self, node: NodeId, member: MemberId, budget: Budget, free_disk: u64) {
        if !self.spills_said.insert((node, member, budget)) {
            return;
        }
        let namespace = HoldsNamespace {
            scope: self.scope,
            node,
        };
        let (held, limit) = self.accountant.standing(namespace, budget, free_disk);
        tracing::info!(
            job_id = self.scope,
            node = node.0,
            member = member.0,
            budget = %budget_name(budget),
            held,
            limit,
            "holds spilled"
        );
    }

    // The member's spill file, made empty on its first spill and the same
    // leaf from then on, as `Files::spills` says.
    fn spill_file(&mut self, node: NodeId, member: MemberId) -> Result<&mut Leaf, WriteError> {
        let dir = self.working_dir.clone();
        let to = self.disk_under(&dir);
        let files = self.files_mut(node);
        if let std::collections::btree_map::Entry::Vacant(vacant) = files.spills.entry(member) {
            let mut spill = Leaf::at(&dir, &spill_name(node, member)).through(to.as_ref());
            spill.truncate(false)?;
            vacant.insert(spill);
            files.spills_empty.insert(member);
        }
        Ok(files.spills.get_mut(&member).expect("opened above"))
    }

    // Bytes of the member written to its spill file at their own offset,
    // which the ledger already holds there.
    fn spill_write(
        &mut self,
        node: NodeId,
        member: MemberId,
        offset: u64,
        bytes: &[u8],
    ) -> Result<(), WriteError> {
        if let Err(error) =
            self.spill_file(node, member)?
                .write(LoopSite::Spill, offset, bytes, None, false)
        {
            // A spill the disk had no room for: the job waits for the room
            // of what was being spilled.
            if let LeafError::Io { source, .. } = &error
                && crate::operations::is_out_of_space(source)
            {
                self.left_to_write = Some(bytes.len() as u64);
            }
            return Err(error.into());
        }
        self.files_mut(node).spills_empty.remove(&member);
        Ok(())
    }

    // Once the ledger holds nothing of the member on file, its spill file's
    // handle is let go and the file waits to be emptied at the commit of
    // the next checkpoint, as `spills_emptying` says, with nothing new
    // written below its length meanwhile; the file and its leaf stay, so no
    // later spill of the member meets a path queued to leave.
    fn release_spill(&mut self, node: NodeId, member: MemberId) -> Result<(), WriteError> {
        let Some(files) = self.files.get_mut(&node) else {
            return Ok(());
        };
        if files.spills_empty.contains(&member) || files.holds.spills(held_volume(member)) {
            return Ok(());
        }
        let Some(spill) = files.spills.get_mut(&member) else {
            return Ok(());
        };
        // A length that cannot be read keeps the whole file.
        let written = spill.len(LoopSite::Spill).unwrap_or(u64::MAX);
        spill.release_handle();
        files.holds.keep_off_file(held_volume(member), written);
        files.spills_empty.insert(member);
        self.spills_emptying.insert((node, member), None);
        Ok(())
    }

    // The checkpoints through `committed` have committed: each spill file
    // released before one of them was taken holds no bytes the database
    // names, so it takes new bytes anywhere again, and, not written since,
    // is emptied.
    fn empty_released_spills(&mut self, committed: u64) -> Result<(), WriteError> {
        let due: Vec<(NodeId, MemberId)> = self
            .spills_emptying
            .iter()
            .filter(|(_, stamp)| stamp.is_some_and(|stamp| stamp <= committed))
            .map(|(key, _)| *key)
            .collect();
        for (node, member) in due {
            self.spills_emptying.remove(&(node, member));
            let Some(files) = self.files.get_mut(&node) else {
                continue;
            };
            files.holds.allow_file(held_volume(member));
            if !files.spills_empty.contains(&member) || files.holds.spills(held_volume(member)) {
                continue;
            }
            if let Some(spill) = files.spills.get_mut(&member) {
                spill.truncate(false)?;
                spill.release_handle();
            }
        }
        Ok(())
    }

    // A member whose front will say no more is settled by the tree: an
    // object beside it may take it, and is handed what was held of it;
    // unclaimed, it is a leaf, and what was held goes to its file. The
    // object that took it, as its volume.
    fn settle(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
        out: &mut Vec<Effect>,
    ) -> Result<Option<(NodeId, VolumeIndex)>, WriteError> {
        let mut effects = Vec::new();
        tree.settle_leaf(node, member, None, builder, &mut effects);
        self.apply_each(
            tree,
            builder,
            segments,
            effects,
            Write::Again(LoopSite::FrontSettle),
            out,
        )?;
        let owner = tree.child_of(node, member);
        if owner.is_none() {
            self.release_to_leaf(tree, node, member)?;
        }
        Ok(owner)
    }

    // The member was decided as a leaf: what was held is written first, so
    // the bytes that arrived after it win where they overlap.
    fn release_to_leaf(
        &mut self,
        tree: &mut Tree,
        node: NodeId,
        member: MemberId,
    ) -> Result<(), WriteError> {
        let paid = self.pay_held(tree, node, member);
        for span in self.held_spans(node, member, ALL) {
            for (offset, bytes) in self.take_held(node, member, span)? {
                self.write_file(tree, (node, member), offset, &bytes, LoopSite::LeafRelease)?;
            }
        }
        self.paid_held(node, member, paid);
        Ok(())
    }

    // What was held of `range` of the member goes down to its object, each
    // run with the provenance the coverage recorded when it arrived: all of
    // it once the member is bound, and what an object asks for again. The
    // coverage is `said` when given, a folded posting's record from before
    // it folded, else the member's record as it stands. One hold leaves the
    // ledger at a time, as it goes down: the ledger is the record of what
    // the object has not been handed, so whatever a piece makes of the
    // member, a refusal reading the object back among it, sees the rest
    // still held.
    //
    // `pay` counts each span on the job's volumes before it leaves the
    // ledger (`ReturnTail`, `release_held`): the loop counts the span whole
    // when every volume has room for it now, and only a span refused whole
    // is cut a `READ_BACK` at a time, each piece counted as it goes. While
    // `pay` holds a count, the piece at the head is paid by what the step
    // was prepaid with, which covers that many bytes, and goes no further
    // than they do; the count is cleared. `Some` with the bytes of the
    // first piece a volume has no room for, which is left held; unpaid,
    // never `Some`.
    #[allow(clippy::too_many_arguments)]
    fn replay(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
        said: Option<&Coverage>,
        mut pay: Option<&mut u64>,
        out: &mut Vec<Effect>,
    ) -> Result<Option<u64>, WriteError> {
        let cap = |rest: ByteRange, bytes: u64| {
            ByteRange::new(rest.start, rest.end.min(rest.start.saturating_add(bytes)))
        };
        let head = |rest: ByteRange| cap(rest, READ_BACK);
        let paid = self.pay_held(tree, node, member);
        let mut stood = None;
        'spans: for span in self.held_spans(node, member, range) {
            let mut cut = false;
            let mut at = span.start;
            while at < span.end {
                let rest = ByteRange::new(at, span.end);
                let (piece, credit) = match pay.as_deref_mut().map(std::mem::take) {
                    None => (rest, None),
                    Some(prepaid @ 1..) => (cap(rest, prepaid.min(READ_BACK)), None),
                    Some(0) => match self.disk.as_ref() {
                        None => (rest, None),
                        Some(disk) => {
                            let piece = if cut { head(rest) } else { rest };
                            match disk.try_reserve(piece.len()) {
                                Some(credit) => (piece, Some(credit)),
                                None if !cut && piece.len() > READ_BACK => {
                                    cut = true;
                                    continue;
                                }
                                None => {
                                    stood = Some(piece.len());
                                    break 'spans;
                                }
                            }
                        }
                    },
                };
                let into = Self::held_into(tree, node, member);
                self.with_credit(tree, into, credit, |writer, tree| {
                    for piece in writer.take_held(node, member, piece)? {
                        writer.replay_piece(
                            tree,
                            builder,
                            segments,
                            (node, member),
                            piece,
                            said,
                            out,
                        )?;
                    }
                    Ok::<_, WriteError>(())
                })?;
                at = piece.end;
            }
        }
        self.paid_held(node, member, paid);
        Ok(stood)
    }

    // The namespace a released hold of the member is credited in: its
    // object's, which the bytes go to, else its own.
    fn held_into(tree: &Tree, node: NodeId, member: MemberId) -> NodeId {
        tree.child_of(node, member).map_or(node, |(child, _)| child)
    }

    // One step the loop counted on the job's volumes itself (`fresh`), as a
    // step that hears a producer's bytes is paid (`GraphJob::run_said`):
    // the credit the loop carries joins it, bytes held in `into` from here
    // are carried for, and what the step did not write is settled as it
    // ends. Without `fresh` the step is paid by whatever it was prepaid
    // with.
    fn with_credit<T>(
        &mut self,
        tree: &mut Tree,
        into: NodeId,
        fresh: Option<weaver_server_disk::operations::disk::writer::Credit>,
        step: impl FnOnce(&mut Self, &mut Tree) -> T,
    ) -> T {
        let paying = fresh.map(|fresh| {
            let mut credit = self.take_credit();
            credit.join(fresh);
            self.crediting(Some(into));
            credit.prepay()
        });
        let done = step(self, tree);
        if let Some(paying) = paying {
            self.settle_credit(tree, paying);
        }
        done
    }

    // One piece of what was held goes down, run by run of its coverage.
    #[allow(clippy::too_many_arguments)]
    fn replay_piece(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, member): (NodeId, MemberId),
        (offset, bytes): (u64, Evidenced),
        said: Option<&Coverage>,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let range = ByteRange::at(offset, bytes.len() as u64);
        let runs: Vec<(ByteRange, Provenance)> = said
            .or_else(|| {
                tree.namespace(node)
                    .and_then(|namespace| namespace.get(member))
                    .map(|record| &record.coverage)
            })
            .map(|coverage| {
                coverage
                    .overlapping(range)
                    .filter_map(|run| Some((run.range.intersect(range)?, run.provenance)))
                    .collect()
            })
            .unwrap_or_default();
        for (piece, provenance) in runs {
            let start = index(piece.start - offset);
            let end = index(piece.end - offset);
            let arrival = Arrival {
                node,
                member,
                offset: piece.start,
                bytes: bytes.slice(start..end),
                provenance,
            };
            self.route_held_as_brought(tree, builder, segments, arrival, out)?;
        }
        Ok(())
    }

    // What of the member is out of its file and not for the file to give:
    // its holds, and what the delivery that bound it gave its object, while
    // that object still has the member as the volume they went to. An
    // object that gave the member up gave those bytes up with it, and
    // whoever takes the member next reads them from the file like the rest,
    // the same object taking it again as another volume included. An
    // object refused while it has them gives them to no file: the refusal
    // hands them on to what takes the member (`Self::materialise`).
    fn not_in_file(&self, tree: &Tree, node: NodeId, member: MemberId) -> Vec<ByteRange> {
        let mut ranges = self
            .files
            .get(&node)
            .map(|files| files.holds.held_ranges(held_volume(member)))
            .unwrap_or_default();
        if let Some(owner) = tree.child_of(node, member) {
            ranges.extend(self.reached_by(node, member, owner));
        }
        ranges.sort_by_key(|range| range.start);
        ranges
    }

    // What the delivery that bound the member gave `child`, the object and
    // its volume the bytes went to, while the member is handed over.
    fn reached_by(
        &self,
        node: NodeId,
        member: MemberId,
        child: (NodeId, VolumeIndex),
    ) -> Vec<ByteRange> {
        self.reached_now
            .iter()
            .filter(|(at, of, to, _)| *at == node && *of == member && *to == child)
            .map(|(_, _, _, range)| *range)
            .collect()
    }

    // Held bytes go down again once the object their member is a volume of
    // places them, at whatever depth: an object's map grows only as it
    // hears its volumes or announces a member, and either may come from an
    // object above it. What just arrived may be the headers that link a
    // volume whose data came first. What its object still cannot place
    // stays held. This runs once a call into the writer has done everything
    // else, and never inside one: held bytes never overtake the effects of
    // the bytes before them, and what one release makes placeable goes in
    // the next pass, not from inside this one. The loop never stands for a
    // volume here: each span is counted on the job's volumes before it goes
    // (`replay`), and the first a volume has no room for stays held, owed
    // (`release_owed`), until the job pays it off the loop (`release_paid`).
    // One release at a time: while one is owed, this leaves every hold, and
    // the closes owed behind it, to `release_paid`, and walks nothing.
    pub fn release_held(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        if self.release_owed.is_some() {
            return Ok(());
        }
        self.release_owed = self.release_held_paid(tree, builder, segments, &mut 0, out)?;
        self.close_owed_members(tree, builder, segments, out)
    }

    // The read `Effect::ReadOn` asked, made now, and what it lets the
    // objects say applied.
    #[allow(clippy::too_many_arguments)]
    fn read_on_now(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
        write: Write,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let mut bytes = Vec::new();
        if !self
            .access(tree, LoopSite::ReadOn)
            .read_at(node, member, range, &mut bytes)
        {
            bytes.clear();
        }
        let mut effects = Vec::new();
        // A read-back that removed a node is pruned as a burial is, before
        // what it lets the objects say: the node's names went with it, and
        // so do its files.
        if tree.read_on(node, member, range.start, &bytes, builder, &mut effects) {
            self.prune(tree)?;
        }
        self.apply_each(tree, builder, segments, effects, write, out)
    }

    // What a read of the member's range would wait for on the loop, asked
    // without waiting: the writes still out to the files it reads, its own
    // where its bytes are not held, and the holds' scratch and spill files
    // where they are. `None` when it would wait for none.
    fn read_out(
        &self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
    ) -> Option<weaver_server_disk::operations::disk::writer::Barrier> {
        let (node, member) = tree.member_home((node, member));
        let files = self.files.get(&node)?;
        let leaf = |from: u64, to: u64| {
            (from < to)
                .then(|| files.leaves.get(&member)?.read_out(from, to))
                .flatten()
        };
        let mut at = range.start;
        for piece in files.holds.peek(held_volume(member), range) {
            let held = piece.range;
            let out = leaf(at, held.start.max(at)).or_else(|| match piece.location {
                Location::Ram(_) => None,
                Location::Scratch { offset, len } => files
                    .scratch
                    .as_ref()
                    .and_then(|scratch| scratch.read_out(offset, offset + len)),
                Location::Spilled => files
                    .spills
                    .get(&member)
                    .and_then(|spill| spill.read_out(held.start, held.end)),
            });
            if out.is_some() {
                return out;
            }
            at = at.max(held.end.min(range.end));
        }
        leaf(at, range.end)
    }

    // What the job waited for aside returned: each read owed whose writes
    // are all back now is made, oldest first, and one that has writes out
    // again stays owed for those. The closes behind them are made after.
    pub fn reads_waited(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        let mut left = Vec::new();
        for mut read in std::mem::take(&mut self.reads_owed) {
            if let Some(waits) = self.read_out(tree, read.node, read.member, read.range) {
                read.waits = waits;
                left.push(read);
                continue;
            }
            let ReadOwed {
                node,
                member,
                range,
                write,
                ..
            } = read;
            self.read_on_now(tree, builder, segments, node, member, range, write, out)?;
        }
        // Reads asked while these were made are younger.
        left.append(&mut self.reads_owed);
        self.reads_owed = left;
        self.close_owed_members(tree, builder, segments, out)
    }

    // The closes owed while a release stood (`close`), made oldest first
    // once none stands: each member is shut and what that lets go of is
    // released, until a release stands again and the rest stay owed. A
    // close made here that ends in another release does not come back in.
    fn close_owed_members(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        if self.closing_owed {
            return Ok(());
        }
        self.closing_owed = true;
        let closed = self.close_owed_in_turn(tree, builder, segments, out);
        self.closing_owed = false;
        closed
    }

    fn close_owed_in_turn(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        while self.release_owed.is_none() {
            let Some(&(node, member)) = self.closes_owed.front() else {
                break;
            };
            // A read owed of the member is heard before its close, as it
            // was when the read was made at once; the closes behind it wait
            // in order.
            if self.read_owed_of(node, member) {
                break;
            }
            self.closes_owed.pop_front();
            self.closes_owed_set.remove(&(node, member));
            self.shut(tree, builder, segments, node, member, out)?;
            self.release_owed = self.release_held_paid(tree, builder, segments, &mut 0, out)?;
        }
        Ok(())
    }

    // A thread counted on the job's volumes what a release stood for
    // (`release_owed`): the release goes on with that credit, which pays the
    // piece it stood at, and stands again at the next a volume has no room
    // for. What the credit did not pay is settled as the step ends. A wait
    // the end of a stop cut short leaves the bytes held, as a tail's does
    // (`tail_paid`), and so does a credit short of what was owed: nothing
    // is written on the loop from it.
    pub fn release_paid(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        credit: weaver_server_disk::operations::disk::writer::Credit,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        if self.disk.as_ref().is_some_and(|disk| disk.left_at_stop()) {
            return Err(WriteError::LeftAtStop);
        }
        let Some((bytes, into)) = self.release_owed else {
            return Ok(());
        };
        if !credit.covers(bytes) {
            return Err(WriteError::LeftAtStop);
        }
        self.release_owed = None;
        let mut prepaid = bytes;
        self.release_owed = self.with_credit(tree, into, Some(credit), |writer, tree| {
            writer.release_held_paid(tree, builder, segments, &mut prepaid, out)
        })?;
        self.close_owed_members(tree, builder, segments, out)
    }

    // The same for a return's tail, which never stands the loop still for a
    // volume: each span is counted on the job's volumes before it goes
    // (`replay`), paid by what the step was prepaid with while `paid` is
    // set. `Some` with the bytes of the span, and the namespace it is
    // credited in, when a volume has no room for it now: what is still held
    // stays held, and the next call asks it again.
    pub fn release_held_paid(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        paid: &mut u64,
        out: &mut Vec<Effect>,
    ) -> Result<Option<(u64, NodeId)>, WriteError> {
        self.apply_unnamed(tree, builder, segments, out)?;
        loop {
            match self.release_placed(tree, builder, segments, Some(&mut *paid), out)? {
                Released::Nothing => return Ok(None),
                Released::Some => {}
                Released::Standing(bytes, into) => return Ok(Some((bytes, into))),
            }
        }
    }

    // The damage `skip_unnamable` marked on a member whose name was
    // refused, applied as any checksum failure's is, once the write that
    // met the refusal has returned: the objects and sets that read the
    // member, and the owner, hear it. Empty but for that error path.
    fn apply_unnamed(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        if self.unnamed_damage.is_empty() {
            return Ok(());
        }
        let effects = std::mem::take(&mut self.unnamed_damage);
        self.apply_each(
            tree,
            builder,
            segments,
            effects,
            Write::Again(LoopSite::Unnamed),
            out,
        )
    }

    // One pass over every hold, each held volume once in volume order and
    // nothing else of its namespace: whether any went down, or, paid
    // (`replay`), the pass stands for room for its next span.
    fn release_placed(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        mut pay: Option<&mut u64>,
        out: &mut Vec<Effect>,
    ) -> Result<Released, WriteError> {
        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
        let _pass = hooks::ReleasePass::enter();
        let mut released = false;
        let parents: Vec<NodeId> = self
            .files
            .iter()
            .filter(|(_, files)| files.holds.total_held() > 0)
            .map(|(node, _)| *node)
            .collect();
        let mut placements = Vec::new();
        for parent in parents {
            let mut after = None;
            while let Some(held) = self
                .files
                .get(&parent)
                .and_then(|files| files.holds.next_held_volume(after))
            {
                after = Some(held);
                let member = held_member(held);
                // A further posting's bytes are its member's: only the
                // namespace's own members are asked.
                let listed = tree.namespace(parent).is_some_and(|namespace| {
                    namespace.get(member).is_some() && namespace.copy_of(member).is_none()
                });
                if !listed {
                    continue;
                }
                #[cfg(any(test, feature = "pipeline-test-fixtures"))]
                hooks::RELEASE_ASKED.with(|asked| asked.set(asked.get() + 1));
                let Some((child, volume)) = tree.child_of(parent, member) else {
                    continue;
                };
                let ranges = |writer: &Self| {
                    writer
                        .files
                        .get(&parent)
                        .map(|files| files.holds.held_ranges(held))
                        .unwrap_or_default()
                };
                // Asked already, of an object whose map has not moved since:
                // every hold was held by that same map, at the last pass or
                // as it came.
                let placement = tree.placement(child);
                let unmoved = placement.is_some()
                    && self.asked.get(&(parent, held)).copied()
                        == placement.map(|number| (child, volume, number));
                if unmoved {
                    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
                    for range in &ranges(self) {
                        placements.clear();
                        tree.locate(child, volume, *range, &mut placements);
                        assert!(
                            placements
                                .iter()
                                .all(|placement| matches!(placement, Placement::Held { .. })),
                            "a hold the unmoved map places: {range:?}"
                        );
                    }
                    continue;
                }
                if let Some(number) = placement {
                    self.asked.insert((parent, held), (child, volume, number));
                }
                for range in ranges(self) {
                    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
                    hooks::RELEASE_LOCATED.with(|located| located.set(located.get() + 1));
                    placements.clear();
                    tree.locate(child, volume, range, &mut placements);
                    if placements
                        .iter()
                        .any(|placement| !matches!(placement, Placement::Held { .. }))
                    {
                        let pay = pay.as_deref_mut();
                        let stood = self.replay(
                            tree, builder, segments, parent, member, range, None, pay, out,
                        )?;
                        if let Some(bytes) = stood {
                            // What is still held was not handed over: the
                            // next pass asks it again.
                            if placement.is_some() {
                                self.asked.remove(&(parent, held));
                            }
                            let into = Self::held_into(tree, parent, member);
                            return Ok(Released::Standing(bytes, into));
                        }
                        released = true;
                    }
                }
            }
        }
        Ok(if released {
            Released::Some
        } else {
            Released::Nothing
        })
    }

    // The spans the held parts of `range` of the member leave the ledger
    // in, in offset order, for `take_held`: a hold in RAM or on scratch
    // whole, as the budgets bound it, and one in the spill file, which no
    // budget bounds, a `READ_BACK` at a time. However much a member spilled,
    // no more than that of it is read back into memory at once, and what is
    // not yet read stays held on file, so a refusal or a second release
    // among the pieces still sees it there.
    fn held_spans(&self, node: NodeId, member: MemberId, range: ByteRange) -> Vec<ByteRange> {
        let Some(files) = self.files.get(&node) else {
            return Vec::new();
        };
        let mut spans = Vec::new();
        for piece in files.holds.peek(held_volume(member), range) {
            if piece.location != Location::Spilled {
                spans.push(piece.range);
                continue;
            }
            let mut at = piece.range.start;
            while at < piece.range.end {
                let end = piece.range.end.min(at.saturating_add(READ_BACK));
                spans.push(ByteRange::new(at, end));
                at = end;
            }
        }
        spans
    }

    // What is held of `range` of the member leaves the ledger unread: the
    // member's bytes are in hand elsewhere, or it is gone.
    fn drop_held(
        &mut self,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
    ) -> Result<(), WriteError> {
        let Some(files) = self.files.get_mut(&node) else {
            return Ok(());
        };
        if !files.holds.take(held_volume(member), range).is_empty() {
            // What a checkpoint says of the member's bytes changed.
            files.leaves.mark(member);
        }
        self.uncredit_past_resident(node, member);
        self.release_spill(node, member)
    }

    // What is held of `range` of the member, in offset order, read back
    // from scratch where it was paged and from the spill file where it
    // spilled. Callers that deliver what they take ask by `held_spans`.
    fn take_held(
        &mut self,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
    ) -> Result<Vec<(u64, Evidenced)>, WriteError> {
        let Some(files) = self.files.get_mut(&node) else {
            return Ok(Vec::new());
        };
        let taken = files.holds.take(held_volume(member), range);
        if !taken.is_empty() {
            // The bytes leave the ledger for the member's file or its
            // object: a checkpoint left them out of the member's coverage
            // while they were held, and the next one says them again.
            files.leaves.mark(member);
        }
        let mut held = Vec::with_capacity(taken.len());
        for piece in taken {
            let bytes = match piece.location {
                Location::Ram(bytes) => bytes,
                Location::Scratch { offset, len } => {
                    let mut buffer = Vec::new();
                    self.scratch(node)?.read(
                        LoopSite::TakeHeld,
                        offset,
                        index(len),
                        &mut buffer,
                    )?;
                    Bytes::from(buffer)
                }
                Location::Spilled => {
                    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
                    hooks::SPILL_READ_MOST.with(|most| most.set(most.get().max(piece.range.len())));
                    let mut buffer = Vec::new();
                    self.spill_file(node, member)?.read(
                        LoopSite::TakeHeld,
                        piece.range.start,
                        index(piece.range.len()),
                        &mut buffer,
                    )?;
                    Bytes::from(buffer)
                }
            };
            held.push((piece.range.start, Evidenced::with(bytes, piece.evidence)));
        }
        self.release_spill(node, member)?;
        Ok(held)
    }

    pub fn write_file(
        &mut self,
        tree: &mut Tree,
        (node, member): (NodeId, MemberId),
        offset: u64,
        bytes: &Evidenced,
        site: LoopSite,
    ) -> Result<(), WriteError> {
        let written = self.write_into(tree, (node, member), (offset, bytes), true, site);
        self.skip_unnamable(tree, (node, member), written)
    }

    // `lands` is false for bytes whose room was taken another way: they
    // write, and land on no promise.
    fn write_into(
        &mut self,
        tree: &Tree,
        (node, member): (NodeId, MemberId),
        (offset, bytes): (u64, &Evidenced),
        lands: bool,
        site: LoopSite,
    ) -> Result<(), WriteError> {
        let record = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .ok_or(WriteError::UnknownMember { node, member })?;
        // The leaf takes the member's length when it opens, so the file
        // reaches that far from its first byte.
        let end = offset
            .saturating_add(bytes.len() as u64)
            .max(record.length.unwrap_or(0));
        let landing = if lands { bytes.len() as u64 } else { 0 };
        self.admit_file(tree, node, member, end, landing)?;
        let keeps = self.keeps_changes(node, member);
        let leaf = self.leaf(tree, node, member)?;
        // The leaf folds the digest the member's stated check needs, from
        // the evidence the bytes came with, hashing only bytes no level above
        // proved. A member that states no check, or not yet, as a split RAR
        // member before its last header, folds only what its bytes came
        // proven with: nothing is hashed for a check that may never come, and
        // one stated later finds the fold without reading the file back.
        let proven = || {
            let algorithm = bytes.evidence().first()?.digest.algorithm;
            bytes.proven(algorithm).then_some(algorithm)
        };
        let wrote = match record.check().or_else(proven) {
            Some(algorithm) => {
                leaf.write_folding(site, offset, bytes, algorithm, record.length, keeps)
            }
            None => leaf.write(site, offset, bytes, record.length, keeps),
        };
        if let Err(error) = wrote {
            // A write the disk had no room for: the job waits for the room
            // of what the member still has to take, by its declared length
            // and what the leaf holds of it, and never for less than the
            // write that was refused.
            // A member that declares no length waits for the write itself.
            if let LeafError::Io { source, .. } = &error
                && crate::operations::is_out_of_space(source)
            {
                let left = record
                    .length
                    .map_or(0, |length| length.saturating_sub(leaf.written_bytes()))
                    .max(bytes.len() as u64);
                self.left_to_write = Some(left);
            }
            return Err(error.into());
        }
        // Every byte to its length is in: the file takes that length, or
        // reaches as far as bytes landed past a length its articles
        // declared, as the tree closes the member. A post's `=ybegin size=`
        // never cuts the parts after it.
        if let Some(length) = record.length.filter(|_| record.is_complete()) {
            let reached = record.coverage.runs().last().map_or(0, |run| run.range.end);
            leaf.close(site, length.max(reached), keeps)?;
            self.caps.closed(node, member);
        }
        Ok(())
    }

    // The member is closed: its file takes the length the tree gave it. A
    // member none of whose bytes were written has no file: a name is a file
    // only once something of it was written, so a file whose every article
    // was lost is none, though its coverage holds that loss as damage, and
    // a file a recovery object describes is one only when its repair
    // writes it.
    fn close_leaf(
        &mut self,
        tree: &mut Tree,
        node: NodeId,
        member: MemberId,
        site: LoopSite,
    ) -> Result<(), WriteError> {
        let closed = self.close_file(tree, node, member, site);
        self.skip_unnamable(tree, (node, member), closed)
    }

    fn close_file(
        &mut self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
        site: LoopSite,
    ) -> Result<(), WriteError> {
        let record = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .ok_or(WriteError::UnknownMember { node, member })?;
        let length = record.length.unwrap_or(0);
        let written = self
            .files
            .get(&node)
            .is_some_and(|files| files.leaves.contains_key(&member));
        if length > 0 && !written {
            return Ok(());
        }
        self.admit_file(tree, node, member, length, 0)?;
        let keeps = self.keeps_changes(node, member);
        self.leaf(tree, node, member)?.close(site, length, keeps)?;
        self.caps.closed(node, member);
        Ok(())
    }

    // A leaf whose file the file system will not create for its name, too
    // long or one it never takes, is skipped and the job goes on with the
    // rest: no file is made, and the member is damaged over its whole
    // length, which its record keeps through a restart, so the delivery
    // counts it short. That damage is applied as a failed checksum's is.
    // Every later byte for it meets the same refusal and is dropped the
    // same way. Any other error stands: a full disk, a permission, a
    // read-only volume, a file already on disk, and a member an object
    // reads, such as a recovery volume, still fail the job.
    // A volume's writer could not make the file of the leaf `file`: the
    // file system refused its name. The member is skipped as `write_file`
    // skips one whose name is refused as it writes, and its namespace is
    // returned. `None` when nothing is skipped and the refusal stands as a
    // failure, as it does there: no leaf has that file, or an object reads
    // the member.
    pub(crate) fn name_refused(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        file: u64,
        out: &mut Vec<Effect>,
    ) -> Result<Option<NodeId>, WriteError> {
        let found = self.files.iter().find_map(|(node, files)| {
            files
                .leaves
                .iter()
                .find(|(_, leaf)| leaf.revision().0 == file)
                .map(|(member, _)| (*node, *member))
        });
        let Some((node, member)) = found else {
            return Ok(None);
        };
        if tree.child_of(node, member).is_some() {
            return Ok(None);
        }
        self.skip_named(tree, (node, member));
        self.release_held(tree, builder, segments, out)?;
        Ok(Some(node))
    }

    fn skip_unnamable(
        &mut self,
        tree: &mut Tree,
        (node, member): (NodeId, MemberId),
        result: Result<(), WriteError>,
    ) -> Result<(), WriteError> {
        let Err(WriteError::Leaf(error)) = &result else {
            // A write that returns has not proved the name: on a volume's
            // writer it returns at hand-over and the refusal comes back
            // later. Only a rename (`moved`) lifts the mark.
            return result;
        };
        let creating = self
            .files
            .get(&node)
            .and_then(|files| files.leaves.get(&member))
            .is_some_and(|leaf| !leaf.on_disk());
        if !error.names_unusable() || !creating || tree.child_of(node, member).is_some() {
            return result;
        }
        self.skip_named(tree, (node, member));
        Ok(())
    }

    // The member's name was refused: its leaf goes and it is damaged.
    fn skip_named(&mut self, tree: &mut Tree, (node, member): (NodeId, MemberId)) {
        if let Some(leaf) = self.files_mut(node).leaves.remove(&member) {
            drop(leaf);
        }
        self.caps.forget(tree, node, member);
        if !self.unnamable.insert((node, member)) {
            return;
        }
        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
        {
            self.names_skipped += 1;
        }
        let name_bytes = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .map_or(0, |record| record.output_name.len());
        tracing::info!(
            job_id = self.scope,
            node = node.0,
            member = member.0,
            name_bytes,
            "member skipped: the file system refused its name"
        );
        // A leaf member (no child, checked above) raises one effect here:
        // `Damaged` over its whole length, none for a member of unknown
        // length. The write path that met the refusal has no builder to
        // apply it with, so it waits in `unnamed_damage` for the
        // `apply_drain` or `release_held` that called it. Any later write
        // it causes to this member meets the refusal again and returns at
        // the insert above, so nothing loops.
        let from = self.unnamed_damage.len();
        tree.checksum_failed(node, member, &mut self.unnamed_damage);
        debug_assert!(
            self.unnamed_damage[from..].iter().all(|effect| matches!(
                effect,
                Effect::Damaged { node: at, member: of, .. } if (*at, *of) == (node, member)
            )),
            "a refused leaf raised more than its own damage: {:?}",
            &self.unnamed_damage[from..]
        );
    }

    // A closed member in its own file is judged now, or, inside a set's
    // batch, once the batch is down, on the bytes it ends with.
    fn judge_closed(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, member): (NodeId, MemberId),
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        match &mut self.acknowledging {
            Some(batch) => {
                batch.closed.insert((node, member));
                Ok(())
            }
            None => self.judge(tree, builder, segments, (node, member), out),
        }
    }

    // Whether the member closed and its file, as it stands, holds no bytes
    // that passed the checksum its object states: no fold of the whole
    // file, or one that differs. A member with no stated checksum has none
    // to pass, and `judge` says nothing of it.
    fn closed_unproven(&self, tree: &Tree, node: NodeId, member: MemberId) -> bool {
        let Some(record) = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
        else {
            return false;
        };
        if !record.closed {
            return false;
        }
        let (Some(stated), Some(length)) = (record.crc32, record.length) else {
            return false;
        };
        self.files
            .get(&node)
            .and_then(|files| files.leaves.get(&member))
            .and_then(|leaf| leaf.folded(length, Algorithm::Crc32))
            != Some(u64::from(stated))
    }

    // A file whose bytes fail the checksum its object states for them is
    // damaged, whatever each byte's own check said: the graph hears it as
    // damage on the whole member.
    fn judge(
        &mut self,
        tree: &mut Tree,
        builder: &mut dyn Builder,
        segments: &dyn Segments,
        (node, member): (NodeId, MemberId),
        out: &mut Vec<Effect>,
    ) -> Result<(), WriteError> {
        if !self.fails_its_checksum(tree, node, member)? {
            return Ok(());
        }
        let mut effects = Vec::new();
        tree.checksum_failed(node, member, &mut effects);
        // The object that stated the checksum hears the failure, for a
        // member it closed with no ruling of its own.
        tree.member_refuted(node, member, &mut self.placing(builder), &mut effects);
        self.apply_each(
            tree,
            builder,
            segments,
            effects,
            Write::Again(LoopSite::Judge),
            out,
        )
    }

    // Whether the member's file, every byte of it in and none damaged,
    // fails the CRC32 its object's headers state for it. Every such member
    // is checked, from the CRC the leaf folded as its writes landed, which
    // a rewrite, a cut and a restore keep. A missing fold queues a worker
    // and keeps a sparse durable obligation; nothing is read back here. A
    // member with no stated checksum, or one already short or damaged, has
    // nothing more for the check to say.
    pub fn fails_its_checksum(
        &mut self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
    ) -> Result<bool, WriteError> {
        let Some(record) = tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
        else {
            return Ok(false);
        };
        let (Some(stated), Some(length)) = (record.crc32, record.length) else {
            return Ok(false);
        };
        let whole = ByteRange::new(0, length);
        if !record.coverage.covered(whole) || record.coverage.damaged(whole) {
            // Nothing to check yet, but an arrival may still clean it: the
            // obligation waits for a verdict or for the namespace's end.
            self.verifications.insert((node, member));
            return Ok(false);
        }
        // A member its producer checked as it decoded it folds the stated
        // CRC32 onto what was written of it, once.
        let Some(leaf) = self
            .files
            .get_mut(&node)
            .and_then(|files| files.leaves.get_mut(&member))
        else {
            return Ok(false);
        };
        if record.checked {
            leaf.fold_checked(length, Digest::crc32(stated));
        }
        match leaf.folded(length, Algorithm::Crc32) {
            Some(actual) => {
                self.verifications.remove(&(node, member));
                Ok(actual != u64::from(stated))
            }
            None => {
                self.verifications.insert((node, member));
                Ok(false)
            }
        }
    }

    // The member's file reaches `end`, by a write of `landing` bytes or by
    // taking its length. What an object produces is delivered, so it is
    // judged against the output directory's device. A post lands on the
    // device its directory lies on: the output directory's when no object
    // reads it, else the device a member that waits lies on.
    fn admit_file(
        &mut self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
        end: u64,
        landing: u64,
    ) -> Result<(), WriteError> {
        let device = if node != NodeId::ROOT
            || matches!(self.device_for(tree, node, member), Device::Output)
        {
            Device::Output
        } else {
            self.kept_device()
        };
        let reader = match device {
            Device::Output => &self.output_free_disk,
            Device::Working => &self.free_disk,
        };
        self.caps.admit_write(
            tree,
            (node, member),
            end,
            (Output::File, landing),
            (reader, device),
        )?;
        Ok(())
    }

    // The job started with `working` bytes of its posts promised on the
    // working disk and `output` on the output disk, when that is another:
    // its posts and what it delivers there draw on them.
    pub(crate) fn take_posted(&mut self, working: u64, output: u64) {
        self.caps
            .take_posted(Device::Working, working, &self.free_disk);
        if output > 0 {
            self.caps
                .take_posted(Device::Output, output, &self.output_free_disk);
        }
    }

    // What the start promised and did not land is given back.
    pub(crate) fn release_posted(&mut self) {
        self.caps.release_posted();
    }

    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn posted(&self) -> [u64; 2] {
        self.caps.posted()
    }

    // A writer made by a test starts as a started job does, its posts
    // promised.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn start_with_posted(&mut self, working: u64, output: u64) {
        self.take_posted(working, output);
    }

    // Bytes up to `end` of a member an object produced that wait in the
    // working directory: in the envelope of the object that reads the
    // member, or held until the member's place or owner is known. They are
    // the producing object's output as much as a file is, so they count as
    // one would, against the working directory's device, at the level of
    // their depth: what the reading object makes of them counts again at
    // its own level, never added to theirs.
    fn admit_kept(
        &mut self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
        end: u64,
    ) -> Result<(), WriteError> {
        let device = self.kept_device();
        let reader = match device {
            Device::Output => &self.output_free_disk,
            Device::Working => &self.free_disk,
        };
        self.caps.admit_write(
            tree,
            (node, member),
            end,
            (Output::Kept, 0),
            (reader, device),
        )?;
        Ok(())
    }

    // The device a member that waits lies on: the working directory's,
    // unless that is the output directory.
    pub(super) fn kept_device(&self) -> Device {
        if self.delivers_in_place {
            Device::Output
        } else {
            Device::Working
        }
    }

    // What a repair wrote into the posts, before any of it lands, each run
    // as its member, and how many bytes: no limit of the job's holds the
    // posts, but a post none of whose bytes arrived takes room on the disk
    // it lies on, as any output does. A repair of a post that did arrive
    // writes over room the post already holds, and is not counted. A post
    // no object reads lies under the output directory, and one another
    // object reads under the working directory.
    pub(super) fn admit_repaired(
        &mut self,
        tree: &Tree,
        runs: impl Iterator<Item = (MemberId, u64)>,
    ) -> Result<(), WriteError> {
        let posts = tree.namespace(NodeId::ROOT);
        let arrived = |member| {
            posts
                .and_then(|namespace| namespace.get(member))
                .is_some_and(|record| record.coverage.runs().any(|run| run.provenance.arrived()))
        };
        let (mut output, mut working) = (0u64, 0u64);
        for (member, bytes) in runs.filter(|&(member, _)| !arrived(member)) {
            match self.root_for(tree, NodeId::ROOT, member) == self.output_dir.as_path() {
                true => output = output.saturating_add(bytes),
                false => working = working.saturating_add(bytes),
            }
        }
        if self.output_dir == self.working_dir {
            output = output.saturating_add(working);
            working = 0;
        }
        self.caps
            .admit_repaired(output, (&self.output_free_disk, Device::Output))?;
        self.caps
            .admit_repaired(working, (&self.free_disk, Device::Working))?;
        Ok(())
    }

    // Before a set over the posts that can outgrow its recovery goes out,
    // as a PAR3 set can: the files it describes
    // none of whose bytes arrived, at the lengths they declare, are what its
    // rebuild would add to the disk, as the file under the post's directory
    // and as the repair's staged output under the working directory. Taken
    // smallest first, a file that with those taken before it would leave
    // less than the reserve free on a disk of its size even empty can never
    // be rebuilt here with them: it is withheld, and the set writes nothing
    // into it. The rest are rebuilt however the set describes them,
    // once the disks have room for them all above the reserve: until then
    // the set does not go out, and the job waits for room, as it does for a
    // write. `None` while it waits. The set says whether it can: any other
    // writes no more than the recovery it received, and is not judged here.
    // `noted`, when given, gains each file found absent.
    pub(super) fn withheld(
        &mut self,
        tree: &Tree,
        set: NodeId,
        noted: Option<&mut BTreeSet<MemberId>>,
    ) -> Option<BTreeSet<MemberId>> {
        let mut withheld = BTreeSet::new();
        let (Some(recovery), Some(posts)) = (tree.recovery(set), tree.namespace(NodeId::ROOT))
        else {
            return Some(withheld);
        };
        if !recovery.outgrows_recovery() {
            return Some(withheld);
        }
        let mut absent: Vec<(u64, MemberId)> = posts
            .members()
            .filter(|record| absent_from_posts(record) && recovery.covers(record.id))
            .filter_map(|record| Some((record.length?, record.id)))
            .collect();
        if let Some(noted) = noted {
            noted.extend(absent.iter().map(|&(_, member)| member));
        }
        if absent.is_empty() {
            return Some(withheld);
        }
        // Smallest first: as many files as fit the disk together are
        // rebuilt, and the rest are withheld.
        absent.sort_unstable();
        let shared = self.output_dir == self.working_dir;
        let reserve = self.caps.reserve();
        let (output_size, working_size) = self.disk_sizes();
        let (mut output, mut working) = (0u64, 0u64);
        let mut held = 0u64;
        for (length, member) in absent {
            // The bytes it adds to the output device and to the working one.
            let (onto_output, onto_working) = match shared {
                true => (length, 0),
                false if self.root_for(tree, NodeId::ROOT, member) == self.output_dir.as_path() => {
                    (length, length)
                }
                false => (0, length),
            };
            // The files taken so far and this one could never fit the disk
            // together, however much is freed on it.
            let never = |used: u64, more: u64, size: Option<u64>| {
                reserve > 0
                    && more > 0
                    && size.is_some_and(|size| {
                        used.saturating_add(more).saturating_add(reserve) > size
                    })
            };
            if never(output, onto_output, output_size) || never(working, onto_working, working_size)
            {
                withheld.insert(member);
                held = held.saturating_add(length);
            } else {
                output = output.saturating_add(onto_output);
                working = working.saturating_add(onto_working);
            }
        }
        // Said once for each change of what the set withholds.
        if !withheld.is_empty() && self.withheld_said.get(&set) != Some(&withheld) {
            self.withheld_said.insert(set, withheld.clone());
            tracing::info!(
                set = set.0,
                withheld = withheld.len(),
                withheld_bytes = held,
                rebuilt_bytes = output.max(working),
                output_size = ?output_size,
                working_size = ?working_size,
                "files a recovery set would rebuild are withheld: the disk is too small for them"
            );
        }
        self.caps
            .want_both((output, &self.output_free_disk), (working, &self.free_disk))
            .then_some(withheld)
    }

    // The size of the output disk, and of the working one when it is
    // another directory, as last read.
    pub(super) fn disk_sizes(&self) -> (Option<u64>, Option<u64>) {
        let size = |reader: &CapacityReader| {
            reader
                .current()
                .reading()
                .map(|reading| reading.total_bytes)
        };
        let working = match self.output_dir == self.working_dir {
            true => None,
            false => size(&self.free_disk),
        };
        (size(&self.output_free_disk), working)
    }

    fn retract(&mut self, tree: &Tree, node: NodeId, member: MemberId) -> Result<(), WriteError> {
        self.drop_held(node, member, ALL)?;
        if let Some(leaf) = self
            .files
            .get_mut(&node)
            .and_then(|files| files.leaves.remove(&member))
        {
            self.discard(leaf)?;
        }
        self.caps.forget(tree, node, member);
        self.unnamable.remove(&(node, member));
        Ok(())
    }

    // Forget what the tree dropped: the envelopes of every node nothing
    // reads back through, whether or not it ever wrote a file, and the file
    // of every member no namespace holds, with the namespace's own files
    // once the namespace is gone too. A namespace retired or gone holds no
    // entries and keeps nothing resident. A member lifted away is not
    // gone: its file is the member's it became, and goes there with it
    // (`Effect::Lifted`).
    fn prune(&mut self, tree: &Tree) -> Result<(), WriteError> {
        self.caps.release_gone(tree);
        let shelved: Vec<NodeId> = self.envelopes.nodes().collect();
        for node in shelved {
            if tree.buried(node) {
                for leaf in self.envelopes.forget(node) {
                    self.discard(leaf)?;
                }
            }
        }
        let nodes: Vec<NodeId> = self.files.keys().copied().collect();
        for node in nodes {
            let Some(namespace) = tree.namespace(node) else {
                // Dropping the namespace's holds gives their count back,
                // and what the loop carried for them.
                if let Some(files) = self.files.remove(&node) {
                    self.uncredit_node(node);
                    for member in files.leaves.keys() {
                        self.caps.forget(tree, node, *member);
                    }
                    for leaf in files
                        .leaves
                        .into_map()
                        .into_values()
                        .chain(files.scratch)
                        .chain(files.spills.into_values())
                    {
                        self.discard(leaf)?;
                    }
                }
                continue;
            };
            let gone: Vec<MemberId> = self.files[&node]
                .leaves
                .keys()
                .copied()
                .filter(|member| {
                    namespace.get(*member).is_none() || tree.buried_member(node, *member)
                })
                .collect();
            for member in gone {
                self.drop_held(node, member, ALL)?;
                let files = self.files_mut(node);
                files.unrefuse(member);
                if let Some(leaf) = files.leaves.remove(&member) {
                    self.discard(leaf)?;
                }
                self.caps.forget(tree, node, member);
            }
        }
        Ok(())
    }

    // An object took the member over: the one place a binding is known.
    fn bound(&self, tree: &Tree, node: NodeId, member: MemberId, kind: Kind, child: NodeId) {
        tracing::info!(
            job_id = self.scope,
            node = child.0,
            kind = %kind,
            over = %member_name(tree, node, member),
            depth = tree.depth(node),
            "object bound"
        );
    }

    // The run of an object that places members itself was refused, or
    // refused to go on: the members it makes and did not close are refused
    // for that reason, as an object's members are, so what is delivered says
    // why they are not there. The archive is noted for the job's events with
    // that reason, once per archive, as one given up is; the object and what
    // it placed stand.
    pub(crate) fn refuse_made(&mut self, tree: &Tree, node: NodeId, why: Why) {
        let open = Self::made_open(tree, node);
        for &member in &open {
            self.refuse(tree, node, member, why, false);
        }
        self.run_refusals.insert(node, (why, open.len()));
        if !open.is_empty() && why.gives_back() && self.given_up.insert((node, true)) {
            self.given_back.push(GivenBack {
                member: archive_name(tree, node),
                container: tree
                    .kind(node)
                    .map(|kind| kind.to_string())
                    .unwrap_or_default(),
                why,
            });
        }
    }

    // A new run makes them again: why the last one did not is forgotten.
    pub(crate) fn unrefuse_made(&mut self, tree: &Tree, node: NodeId) {
        self.run_refusals.remove(&node);
        for member in Self::made_open(tree, node) {
            self.files_mut(node).unrefuse(member);
        }
    }

    // The members the node's run makes, those the tree's record does not
    // give its object, that it left open.
    fn made_open(tree: &Tree, node: NodeId) -> Vec<MemberId> {
        let Some(namespace) = tree.namespace(node) else {
            return Vec::new();
        };
        namespace
            .members()
            .filter(|member| !member.own && !member.closed && !member.is_complete())
            .map(|member| member.id)
            .collect()
    }

    // The member is a leaf because something gave it back or never took
    // it. Said once, when the reason is new. When `given_up`, the archive it
    // belonged to is noted for the job's events, once per archive.
    fn refuse(&mut self, tree: &Tree, node: NodeId, member: MemberId, why: Why, given_up: bool) {
        // Said before: an engine handed the member gives it back for the
        // reason it was handed over for.
        let said = self.files_mut(node).refuse(member, why) == Some(why);
        // The object that gave the member back, if one had it.
        let object = tree.refused_by(node, member);
        let kind = object
            .and_then(|object| tree.kind(object))
            .map(|kind| kind.to_string())
            .unwrap_or_default();
        let name = member_name(tree, node, member);
        // Noted whether or not the reason is new: a member handed to an
        // engine already holds the reason the engine gives it back for.
        // Members nothing held are one archive per namespace.
        let archive = match object {
            Some(object) => (object, true),
            None => (node, false),
        };
        if given_up && why.gives_back() && self.given_up.insert(archive) {
            self.given_back.push(GivenBack {
                member: name.clone(),
                container: kind.clone(),
                why,
            });
        }
        if said {
            return;
        }
        tracing::info!(
            job_id = self.scope,
            node = node.0,
            kind = %kind,
            member = %name,
            why = %why_name(why),
            "object refused"
        );
    }

    pub fn files_mut(&mut self, node: NodeId) -> &mut Files {
        let namespace = HoldsNamespace {
            scope: self.scope,
            node,
        };
        let accountant = &self.accountant;
        let tally = self
            .subtree_of
            .get(&node)
            .and_then(|top| self.held_by_subtree.get(top));
        self.files.get_or_insert_with(node, || {
            let mut files = Files::new(namespace, Arc::clone(accountant));
            if let Some(tally) = tally {
                files.holds.tally_into(Arc::clone(tally));
            }
            files
        })
    }

    // `node`, a namespace below the root, lies under `top`, the root
    // member's child: its ledger tallies what it holds into `top`'s total
    // from here, whenever its files were made.
    fn note_subtree(&mut self, node: NodeId, top: NodeId) {
        self.subtree_of.insert(node, top);
        let tally = Arc::clone(self.held_by_subtree.entry(top).or_default());
        if self
            .files
            .get(&node)
            .is_some_and(|files| !files.holds.tallies_into(&tally))
            && let Some(files) = self.files.get_mut(&node)
        {
            files.holds.tally_into(tally);
        }
    }

    // `child`, bound over a member of `node`'s namespace, lies under the
    // root member's child `node` lies under, or is that child: one step up.
    fn note_bound(&mut self, tree: &Tree, node: NodeId, child: NodeId) {
        let top = if node == NodeId::ROOT {
            Some(child)
        } else {
            self.subtree_of
                .get(&node)
                .copied()
                .or_else(|| subtree_top(tree, node))
        };
        if let Some(top) = top {
            self.note_subtree(child, top);
        }
    }

    // A restored tree's namespaces, each under the root member's child it
    // lies under, parents first: what a restore binds again says nothing.
    pub(crate) fn note_subtrees(&mut self, tree: &Tree) {
        let mut below: Vec<(NodeId, NodeId)> = tree
            .namespaces_below(NodeId::ROOT)
            .into_iter()
            .map(|child| (child, child))
            .collect();
        while let Some((node, top)) = below.pop() {
            self.note_subtree(node, top);
            below.extend(
                tree.namespaces_below(node)
                    .into_iter()
                    .map(|child| (child, top)),
            );
        }
    }

    // The member's file, opened where `new_leaf` says when it has none.
    fn leaf(
        &mut self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
    ) -> Result<&mut Leaf, WriteError> {
        let opened = self
            .files
            .get(&node)
            .is_some_and(|files| files.leaves.contains_key(&member));
        if !opened {
            let output_name = tree
                .namespace(node)
                .and_then(|namespace| namespace.get(member))
                .map(|record| record.output_name.clone())
                .ok_or(WriteError::UnknownMember { node, member })?;
            let held_off = tree.held_off(node, member);
            let root = self.root_for(tree, node, member).to_path_buf();
            let device = self.device_for(tree, node, member);
            let leaf = self
                .new_leaf((node, member), &output_name, held_off, &root)
                .on(device);
            self.files_mut(node).leaves.insert(member, leaf);
        }
        Ok(self
            .files_mut(node)
            .leaves
            .get_mut(&member)
            .expect("opened above"))
    }

    // The free space the holds budgets judge a page-out against.
    fn free_disk_bytes(&self) -> u64 {
        self.free_disk
            .current()
            .best_available_bytes()
            .unwrap_or(u64::MAX)
    }

    // The node's holds scratch, created empty the first time anything pages,
    // unless a restore adopted it as it was left. It lives under the
    // directory finalize never delivers.
    fn scratch(&mut self, node: NodeId) -> Result<&mut Leaf, WriteError> {
        let dir = self.working_dir.clone();
        let to = self.disk_under(&dir);
        let files = self.files_mut(node);
        if files.scratch.is_none() {
            let mut scratch = Leaf::at(&dir, &scratch_name(node)).through(to.as_ref());
            scratch.truncate(false)?;
            files.scratch = Some(scratch);
        }
        Ok(files.scratch.as_mut().expect("created above"))
    }
}

pub struct Placing<'a> {
    builder: &'a mut dyn Builder,
    writer: &'a NamespaceWriter,
}

impl Builder for Placing<'_> {
    fn build(&mut self, kind: Kind, id: ObjectId) -> Option<Box<dyn Object>> {
        self.builder.build(kind, id)
    }

    fn read(
        &self,
        tree: &Tree,
        node: NodeId,
        volume: VolumeIndex,
        placement: &Placement,
        into: &mut Vec<u8>,
    ) -> bool {
        let Some((parent, member)) = tree
            .volumes(node)
            .and_then(|(parent, volumes)| Some((parent, *volumes.get(&volume)?)))
        else {
            return false;
        };
        let access = self.writer.access(tree, LoopSite::ObjectRead);
        let start = into.len();
        let read = read_placed(&access, (parent, member), node, placement, into);
        if !read {
            into.truncate(start);
        }
        read
    }
}

// Where a read of a namespace's members finds their bytes. A read follows
// the member to wherever they are: its leaf, the bytes still held for it,
// or through the inverse map of the object that owns it, into that object's
// members and envelopes, as deep as the tree goes. The live tree answers on
// the loop; a repair lent off it answers from what it took along.
pub trait Source {
    // The data object the member is bound to, and the volume it is of it.
    // A recovery volume is in its own file, so it is bound to none.
    fn bound(&self, node: NodeId, member: MemberId) -> Option<(NodeId, VolumeIndex)>;
    // Where the member lives now (`Tree::home`): itself, or the member it
    // was lifted to, whose bytes are its at the same offsets.
    fn member_home(&self, at: (NodeId, MemberId)) -> (NodeId, MemberId);
    // The bytes posted for a range of the object's volume, rebuilt as the
    // object says from what `read` hands back per placement.
    fn posted(
        &self,
        child: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool;
    fn envelope(&self, child: NodeId, envelope: u32, range: ByteRange, into: &mut Vec<u8>) -> bool;
    // Whether the object at `child` is a recovery object, whose volume stays
    // in its member's own file.
    fn recovers(&self, child: NodeId) -> bool;
    // Where a range of the volume of the object at `child` lies in the
    // member it is bound to; `None` past the window it is bound to.
    fn in_member(&self, child: NodeId, range: ByteRange) -> Option<ByteRange>;
    // The member's own file, for a range its coverage tiles: runs of any
    // provenance, lost included, so a lost range reads what the file holds
    // there.
    fn leaf(&self, node: NodeId, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool;
    // The parts of a range still held for the member, in offset order and
    // left held, and where each is kept: a member whose object has not
    // placed them yet, or one whose map became known before they went to
    // its placements.
    fn held(&self, node: NodeId, member: MemberId, range: ByteRange) -> Vec<(ByteRange, Location)>;
    // Bytes of the member's held parts kept on scratch.
    fn scratch(
        &self,
        node: NodeId,
        member: MemberId,
        offset: u64,
        len: u64,
        into: &mut Vec<u8>,
    ) -> bool;
    // Bytes of the member's held parts kept in its spill file, at their own
    // offsets.
    fn spilled(&self, node: NodeId, member: MemberId, range: ByteRange, into: &mut Vec<u8>)
    -> bool;
}

pub fn read_member(
    source: &dyn Source,
    node: NodeId,
    member: MemberId,
    range: ByteRange,
    into: &mut Vec<u8>,
) -> bool {
    let (node, member) = source.member_home((node, member));
    let Some((child, volume)) = source.bound(node, member) else {
        return read_own(source, node, member, range, into);
    };
    // The object rebuilds its volume's bytes from its placements: one that
    // deciphers puts the cipher back.
    let mut read = |placement: &Placement, into: &mut Vec<u8>| {
        read_placed(source, (node, member), child, placement, into)
    };
    // What the object cannot rebuild is read from the member's own file
    // where that has it: a volume given back stays in its member's file,
    // and an object that takes it again reads it there in place.
    let posted = |range: ByteRange, into: &mut Vec<u8>| {
        let start = into.len();
        source.posted(child, volume, range, &mut read, into) || {
            into.truncate(start);
            source.leaf(node, member, range, into)
        }
    };
    held_first(source, (node, member), range, into, posted)
}

// A member's range, its held custody first and `rest` for what lies
// between: held custody is current authority, even if its backing cannot be
// read. An older derived copy, the member's file among them, must not
// substitute for that failure. Whole, or nothing and `false`.
fn held_first(
    source: &dyn Source,
    (node, member): (NodeId, MemberId),
    range: ByteRange,
    into: &mut Vec<u8>,
    mut rest: impl FnMut(ByteRange, &mut Vec<u8>) -> bool,
) -> bool {
    let start = into.len();
    let mut at = range.start;
    let pieces = source.held(node, member, range);
    let mut read = || {
        for group in pieces.chunk_by(|(a, _), (b, _)| a.end == b.start) {
            let held = ByteRange::new(group[0].0.start, group[group.len() - 1].0.end);
            if at < held.start && !rest(ByteRange::new(at, held.start), into) {
                return false;
            }
            if !read_held(source, (node, member), group, held, into) {
                return false;
            }
            at = held.end;
        }
        at == range.end || rest(ByteRange::new(at, range.end), into)
    };
    read() || {
        into.truncate(start);
        false
    }
}

// What one placement of the object at `child`, bound to `member` of `node`,
// holds.
fn read_placed(
    source: &dyn Source,
    (node, member): (NodeId, MemberId),
    child: NodeId,
    placement: &Placement,
    into: &mut Vec<u8>,
) -> bool {
    match placement {
        Placement::Member { member, range } => read_member(source, child, *member, *range, into),
        // A recovery object's volume stays in its member's own file: its
        // envelope is that file, at the volume's offsets in the member.
        Placement::Envelope { range, .. } if source.recovers(child) => source
            .in_member(child, *range)
            .is_some_and(|within| read_own(source, node, member, within, into)),
        Placement::Envelope { envelope, range } => source.envelope(child, *envelope, *range, into),
        // What the object could not place was held for the member, and once
        // released it is in the member's own file.
        Placement::Held { range } => read_own(source, node, member, *range, into),
    }
}

// A member no object reads: its bytes still held while its front has not
// said what it is, and the rest in its own file.
fn read_own(
    source: &dyn Source,
    node: NodeId,
    member: MemberId,
    range: ByteRange,
    into: &mut Vec<u8>,
) -> bool {
    held_first(source, (node, member), range, into, |rest, into| {
        source.leaf(node, member, rest, into)
    })
}

// The held pieces of a range, read where each is kept.
fn read_held(
    source: &dyn Source,
    (node, member): (NodeId, MemberId),
    pieces: &[(ByteRange, Location)],
    range: ByteRange,
    into: &mut Vec<u8>,
) -> bool {
    read_pieces(
        pieces.iter().map(|(piece, location)| (*piece, location)),
        range,
        into,
        |offset, len, into| source.scratch(node, member, offset, len, into),
        |piece, into| source.spilled(node, member, piece, into),
    )
}

// The held pieces of a range from its start, as long as they run on, read
// with `piece`; whole, or nothing and `false`.
fn read_pieces<'p>(
    pieces: impl IntoIterator<Item = (ByteRange, &'p Location)>,
    range: ByteRange,
    into: &mut Vec<u8>,
    mut scratch: impl FnMut(u64, u64, &mut Vec<u8>) -> bool,
    mut spilled: impl FnMut(ByteRange, &mut Vec<u8>) -> bool,
) -> bool {
    let start = into.len();
    let mut at = range.start;
    for (piece, location) in pieces {
        if piece.start != at {
            break;
        }
        let read = match location {
            Location::Ram(bytes) => {
                into.extend_from_slice(bytes);
                true
            }
            Location::Scratch { offset, len } => scratch(*offset, *len, into),
            Location::Spilled => spilled(piece, into),
        };
        if !read {
            break;
        }
        at = piece.end;
    }
    let whole = at == range.end;
    if !whole {
        into.truncate(start);
    }
    whole
}

// The namespace's own face toward its members, on the loop.
pub struct Access<'a> {
    pub writer: &'a NamespaceWriter,
    pub tree: &'a Tree,
    // Whether bytes still held count as read. A repair reads them; what is
    // on disk, which a restart reads back, does not hold them.
    pub held: bool,
    // Where the reads are made from.
    pub site: LoopSite,
}

// A repair's scratch directory goes with the repair. One left behind by a
// crash is cleared before the next is lent, and lives under the directory
// finalize never delivers.
fn remove_scratch(dir: &Path) {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => tracing::warn!(dir = %dir.display(), %error, "recovery scratch left behind"),
    }
}

impl Access<'_> {
    // The range of the member appended to `into`, or nothing and `false`.
    pub fn read_at(
        &self,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
        into: &mut Vec<u8>,
    ) -> bool {
        let start = into.len();
        let read = read_member(self, node, member, range, into);
        if !read {
            into.truncate(start);
        }
        read
    }

    // Whether the range of the member is where the records say, judged
    // from them and the files' lengths alone: the read the range would make,
    // with zeros standing in for every byte a file would give. `scratch` is
    // the buffer the zeros go in.
    pub(crate) fn reaches(
        &self,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
        scratch: &mut Vec<u8>,
    ) -> bool {
        scratch.clear();
        read_member(&Probe(self), node, member, range, scratch)
    }
}

// The access as a restore judges it: a file gives zeros for every byte its
// record says it holds and its length reaches, and nothing is read.
struct Probe<'a, 'b>(&'b Access<'a>);

impl Probe<'_, '_> {
    fn zeros(into: &mut Vec<u8>, len: u64) -> bool {
        into.resize(into.len() + index(len), 0);
        true
    }
}

impl Source for Probe<'_, '_> {
    fn bound(&self, node: NodeId, member: MemberId) -> Option<(NodeId, VolumeIndex)> {
        self.0.bound(node, member)
    }

    fn member_home(&self, at: (NodeId, MemberId)) -> (NodeId, MemberId) {
        self.0.member_home(at)
    }

    fn recovers(&self, child: NodeId) -> bool {
        self.0.recovers(child)
    }

    fn in_member(&self, child: NodeId, range: ByteRange) -> Option<ByteRange> {
        self.0.in_member(child, range)
    }

    fn posted(
        &self,
        child: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        self.0.posted(child, volume, range, read, into)
    }

    fn envelope(&self, child: NodeId, envelope: u32, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.0.writer.envelopes.keeps(child, envelope, range) && Self::zeros(into, range.len())
    }

    fn leaf(&self, node: NodeId, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool {
        let written = self
            .0
            .tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .is_some_and(|record| record.coverage.covered(range));
        let Some(leaf) = self
            .0
            .writer
            .files
            .get(&node)
            .and_then(|files| files.leaves.get(&member))
        else {
            return range.is_empty();
        };
        // What the checkpoint's hold rows name was never in the file.
        let recorded = self
            .0
            .writer
            .files
            .get(&node)
            .and_then(|files| files.recorded.get(&member))
            .is_some_and(|recorded| recorded.overlaps(range));
        let held = self
            .0
            .writer
            .files
            .get(&node)
            .is_some_and(|files| files.holds.holds_in(held_volume(member), range));
        written
            && !recorded
            && !held
            && leaf
                .len(self.0.site)
                .is_ok_and(|length| length >= range.end)
            && Self::zeros(into, range.len())
    }

    fn held(&self, node: NodeId, member: MemberId, range: ByteRange) -> Vec<(ByteRange, Location)> {
        self.0.held(node, member, range)
    }

    fn scratch(
        &self,
        node: NodeId,
        _: MemberId,
        offset: u64,
        len: u64,
        into: &mut Vec<u8>,
    ) -> bool {
        self.0
            .writer
            .files
            .get(&node)
            .and_then(|files| files.scratch.as_ref())
            .is_some_and(|scratch| {
                scratch
                    .len(self.0.site)
                    .is_ok_and(|length| length >= offset + len)
            })
            && Self::zeros(into, len)
    }

    fn spilled(
        &self,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
        into: &mut Vec<u8>,
    ) -> bool {
        self.0
            .writer
            .files
            .get(&node)
            .and_then(|files| files.spills.get(&member))
            .is_some_and(|spill| {
                spill
                    .len(self.0.site)
                    .is_ok_and(|length| length >= range.end)
            })
            && Self::zeros(into, range.len())
    }
}

impl Source for Access<'_> {
    fn bound(&self, node: NodeId, member: MemberId) -> Option<(NodeId, VolumeIndex)> {
        self.tree
            .child_of(node, member)
            .filter(|(child, _)| !self.tree.recovers(*child))
    }

    fn member_home(&self, at: (NodeId, MemberId)) -> (NodeId, MemberId) {
        self.tree.member_home(at)
    }

    fn recovers(&self, child: NodeId) -> bool {
        self.tree.recovers(child)
    }

    fn in_member(&self, child: NodeId, range: ByteRange) -> Option<ByteRange> {
        self.tree.in_member(child, range)
    }

    fn posted(
        &self,
        child: NodeId,
        volume: VolumeIndex,
        range: ByteRange,
        read: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        self.tree.posted(child, volume, range, read, into)
    }

    fn envelope(&self, child: NodeId, envelope: u32, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.writer
            .envelopes
            .read(self.site, child, envelope, range, into)
    }

    // The file is read where the member's coverage tiles the range with
    // runs of any provenance, lost included: a lost run does not stop it.
    fn leaf(&self, node: NodeId, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool {
        let written = self
            .tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .is_some_and(|record| record.coverage.covered(range));
        let Some(leaf) = self
            .writer
            .files
            .get(&node)
            .and_then(|files| files.leaves.get(&member))
        else {
            return range.is_empty();
        };
        // What is held for the member is not in its file, whatever length
        // the file has: the hold gives those bytes, never the file.
        let held = self
            .writer
            .files
            .get(&node)
            .is_some_and(|files| files.holds.holds_in(held_volume(member), range));
        written
            && !held
            && leaf
                .read(self.site, range.start, index(range.len()), into)
                .is_ok()
    }

    fn held(&self, node: NodeId, member: MemberId, range: ByteRange) -> Vec<(ByteRange, Location)> {
        if !self.held {
            return Vec::new();
        }
        self.writer
            .files
            .get(&node)
            .map(|files| {
                files
                    .holds
                    .peek(held_volume(member), range)
                    .into_iter()
                    .map(|piece| (piece.range, piece.location))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn scratch(
        &self,
        node: NodeId,
        _: MemberId,
        offset: u64,
        len: u64,
        into: &mut Vec<u8>,
    ) -> bool {
        self.writer
            .files
            .get(&node)
            .and_then(|files| files.scratch.as_ref())
            .is_some_and(|scratch| scratch.read(self.site, offset, index(len), into).is_ok())
    }

    fn spilled(
        &self,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
        into: &mut Vec<u8>,
    ) -> bool {
        self.writer
            .files
            .get(&node)
            .and_then(|files| files.spills.get(&member))
            .is_some_and(|spill| {
                spill
                    .read(self.site, range.start, index(range.len()), into)
                    .is_ok()
            })
    }
}

// A read `Effect::ReadOn` asked, owed while writes to what it reads are
// still out, and what it waits for.
struct ReadOwed {
    node: NodeId,
    member: MemberId,
    range: ByteRange,
    write: Write,
    waits: weaver_server_disk::operations::disk::writer::Barrier,
}

// Holds are kept per namespace, keyed by member, which the ledger calls a
// volume: the member is the volume of whatever will own it.
// Every byte of a member.
pub const ALL: ByteRange = ByteRange {
    start: 0,
    end: u64::MAX,
};

// How much of a member is read back at once, so a large one never sits in
// memory whole.
pub const READ_BACK: u64 = 1 << 20;

// The name a member of the namespace carries now.
// The archive the object at `node` reads, as the namespace it is bound in
// names its first volume.
pub(crate) fn archive_name(tree: &Tree, node: NodeId) -> String {
    tree.volumes(node)
        .and_then(|(parent, volumes)| {
            let member = volumes.values().next()?;
            Some(tree.namespace(parent)?.get(*member)?.name.clone())
        })
        .unwrap_or_default()
}

fn member_name(tree: &Tree, node: NodeId, member: MemberId) -> String {
    tree.namespace(node)
        .and_then(|namespace| namespace.get(member))
        .map(|record| record.name.clone())
        .unwrap_or_default()
}

// Whether the member lives in its own file: a leaf, or a recovery volume.
fn in_own_file(tree: &Tree, node: NodeId, member: MemberId) -> bool {
    tree.child_of(node, member)
        .is_none_or(|(child, _)| tree.recovers(child))
}

// The node's holds scratch file, under the working directory.
pub fn scratch_name(node: NodeId) -> String {
    format!(
        "{}/graph-holds-{}.scratch",
        weaver_graph::names::INTERNAL_DIR,
        node.0
    )
}

// A member's spill file, under the working directory.
pub fn spill_name(node: NodeId, member: MemberId) -> String {
    format!(
        "{}/graph-spill-{}-{}.spill",
        weaver_graph::names::INTERNAL_DIR,
        node.0,
        member.0
    )
}

fn budget_name(budget: Budget) -> &'static str {
    match budget {
        Budget::ResidentLimit => "resident_limit",
        Budget::ScratchTotal => "scratch_total",
        Budget::ScratchCeiling => "scratch_ceiling",
        Budget::DiskReserve => "disk_reserve",
    }
}

// The root member's child `node` lies under, by the tree's parent links:
// `None` for the root, or a node the tree no longer binds.
fn subtree_top(tree: &Tree, mut node: NodeId) -> Option<NodeId> {
    loop {
        let (parent, _) = tree.volumes(node)?;
        if parent == NodeId::ROOT {
            return Some(node);
        }
        node = parent;
    }
}

// A volume an article's bytes land on (`NamespaceWriter::landing`).
type LandingVolume = weaver_server_disk::operations::disk::volume::Volume;

// The other volume a step may write to, and the held bytes it may release
// there besides its own (`NamespaceWriter::landing`).
type OrLanding<'a> = (&'a Arc<LandingVolume>, u64);

pub fn held_volume(member: MemberId) -> VolumeIndex {
    VolumeIndex(member.0)
}

// The member whose bytes a held volume holds.
fn held_member(volume: VolumeIndex) -> MemberId {
    MemberId(volume.0)
}

#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub fn crc32(bytes: &[u8]) -> u32 {
    crc_turbo::checksum(crc_turbo::CrcAlgorithm::Crc32IsoHdlc, bytes) as u32
}

pub fn index(offset: u64) -> usize {
    usize::try_from(offset).expect("an offset fits the address space")
}

mod carried;
pub(crate) mod claimed;
mod durable;
mod loan;
mod moves;
pub mod recovery;
pub mod refusal;
pub mod verification;
mod zeros;

#[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
pub(crate) use verification::checked;
pub(crate) use verification::{Verification, Verified};

pub(crate) use loan::replay::{Replay, Replayed};
pub(crate) use loan::rewalk::{RewalkLoan, Rewalked};
pub use loan::{Loan, Repaired, Returned};
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
pub use moves::TALLY;
pub use moves::restored_at;
pub use recovery::{Coming, Lender, SetState};
pub use refusal::GivingUp;

// What the unit tests compiled apart in tests/pipeline_graph.rs reach in
// this module's private modules.
#[cfg(feature = "pipeline-test-fixtures")]
#[doc(hidden)]
pub mod units {
    pub mod claimed {
        pub use super::super::claimed::*;
    }
    pub mod loan {
        pub use super::super::loan::*;
    }
    pub mod moves {
        pub use super::super::moves::*;
    }
}

// The counters the namespace keeps for its tests, compiled apart in
// tests/pipeline_graph.rs.
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
#[doc(hidden)]
pub mod hooks {
    thread_local! {
        // How many members a pass over the holds asked what is held of them.
        pub static RELEASE_ASKED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        // How many held ranges a pass asked their object to place.
        pub static RELEASE_LOCATED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
        // How many passes over the holds are running, and the most that ran
        // at once.
        pub static RELEASE_DEPTH: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
        // The most bytes one read took back from a spill file.
        pub static SPILL_READ_MOST: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
        // The most bytes one read of a lent repair took from a scratch file.
        pub static SCRATCH_READ_MOST: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
        // How many checksum obligations a sweep of the unmet ones asked
        // about.
        pub static SWEEP_ASKED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    }

    // One pass over the holds, counted in `RELEASE_DEPTH` while it runs.
    pub struct ReleasePass;

    impl ReleasePass {
        pub fn enter() -> Self {
            RELEASE_DEPTH.with(|depth| {
                let (now, most) = depth.get();
                depth.set((now + 1, most.max(now + 1)));
            });
            Self
        }
    }

    impl Drop for ReleasePass {
        fn drop(&mut self) {
            RELEASE_DEPTH.with(|depth| {
                let (now, most) = depth.get();
                depth.set((now - 1, most));
            });
        }
    }
}
