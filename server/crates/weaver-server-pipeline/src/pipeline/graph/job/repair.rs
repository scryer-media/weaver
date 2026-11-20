// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job's walk over its namespaces. Damage the source reports is handed to
// the root namespace, which marks it down the tree to every member it lands
// in. One walk over the namespaces, parents before children, asks each to
// rule its recovery sets, so a repair above is in place before anything
// below rules on bytes it rewrote; what a set covers, when it rules and what
// its repair writes are the namespace's. Once nothing more can change, every
// data object, deepest first, hears the lost ranges nothing vouched for and
// that the job is quiet, and an object that cannot finish refuses itself for
// its own reason. The server tells; it chooses no reason.

use std::collections::BTreeSet;

use weaver_graph::slots::Ticket;
use weaver_graph::{MemberId, NodeId, VolumeIndex};

use super::{GraphError, GraphJob};
use crate::pipeline::graph::namespace::{GivingUp, Lender, WriteError};
use crate::pipeline::graph::status::RepairSlots;

impl GraphJob {
    // Walk the namespaces without asking for a slot: a job just restored
    // holds none, and lends what is due once the pipeline has it.
    pub(super) fn repair(&mut self) -> Result<(), GraphError> {
        self.walk(None)
    }

    // Walk the namespaces and lend what is due, asking `slots` for a slot
    // when the job holds none. The slot goes back once nothing is out. A
    // step lends after its runs are stepped, so `reading` counts the runs
    // the step started: a namespace one of them is about to read may still
    // announce members, and no set over it is due yet. Work goes first, as
    // the walk waits for it, and again after, for what the walk's rulings
    // made due. Under no slot, as for a job paused, held or retiring,
    // nothing goes out and no work is done: the job lends once it runs.
    // While the job waits for room on a disk, only what writes nothing goes
    // out: the reads of recovery a restore asked again, and a check of what
    // was written. Repairs, replays and the objects' work wait with the job.
    pub fn lend(&mut self, slots: &mut RepairSlots, limit: usize) -> Result<(), GraphError> {
        if limit > 0 {
            self.lend_rewalks()?;
        }
        self.lend_work(limit)?;
        let walked = self.walk(Some((&mut *slots, limit)));
        if let Some(ticket) = self.loans.give_back() {
            slots.release(ticket);
        }
        walked?;
        self.lend_work(limit)
    }

    // Lend out every object that holds work too slow for the loop. Once
    // the pipeline is shutting down, or while a checkpoint that found a
    // loan out is owed, nothing goes out: the work is done here, so the
    // checkpoint finds none of it out. Work done here can make more due:
    // what it said lets the writer drop the next bytes it held, and the
    // object holds them in turn. It goes on until no object is due, or a
    // round's work said nothing, which moved nothing. It needs no repair
    // slot, so the gate lends it too before it rules. While a repair, a
    // replay or a check is out, no object goes out for its work: what that
    // loan comes to is read back through the objects its set covers, so
    // they stay home until it returns, and its return lends them. Under no
    // slot nothing is lent or done.
    pub fn lend_work(&mut self, limit: usize) -> Result<(), GraphError> {
        let admit = self.works.admit();
        if limit == 0
            || self.writer.disk_held()
            || self.quieting.is_some()
            || (admit && self.loans.out())
        {
            return Ok(());
        }
        loop {
            let mut moved = false;
            for node in self.tree.working() {
                let Some(mut object) = self.tree.lend_work(node) else {
                    continue;
                };
                if admit {
                    self.works.lend(node, object);
                    continue;
                }
                let mut said = Vec::new();
                {
                    let _decoding = self.works.decoding();
                    object.work(&mut said);
                }
                moved |= !said.is_empty();
                let mut effects = Vec::new();
                self.tree.take_back_worked(
                    node,
                    object,
                    said,
                    &mut self.writer.placing(&mut *self.builder),
                    &mut effects,
                );
                self.settle_worked(effects)?;
            }
            if admit || !moved {
                return Ok(());
            }
        }
    }

    // What each restored recovery object asked again goes out once the job
    // is installed, never inside its restore, whether or not it may run:
    // the job fetches nothing until each is heard. One reads at a time, so
    // a job holds one thread and reads one file at once; the end of each
    // lends the next.
    pub(crate) fn lend_rewalks(&mut self) -> Result<(), GraphError> {
        if !self.works.admit_rewalk() || self.works.rewalks_out() {
            return Ok(());
        }
        for node in self.tree.rewalks() {
            if let Some(loan) = self
                .writer
                .lend_rewalk(&mut self.tree, node)
                .map_err(WriteError::from)?
            {
                self.works.lend_rewalk(loan);
                break;
            }
        }
        Ok(())
    }

    // A retiring job releases an idle slot. An outstanding repair keeps
    // its loan and ticket until its matching completion acknowledgment.
    pub fn forget_slot(&mut self) -> Option<Ticket> {
        self.loans.give_back()
    }

    // Walk the namespaces that have sets in node order, which puts every
    // parent before its children, and ask each to rule them: a namespace
    // with none has nothing to rule or announce. A set the walk's own
    // repairs bring into being is walked too. A replay or a checksum check
    // waiting goes out once no repair took the job's one loan: what either
    // changes, applied, takes back the rulings it touches (an uncovered
    // input), and the set rules again through the same walk.
    fn walk(&mut self, mut slots: Option<(&mut RepairSlots, usize)>) -> Result<(), GraphError> {
        // An object out for its work is not there to read a set's inputs
        // back through, and one with work due has not yet said what it
        // holds: the walk waits for it, and its return walks again. A
        // recovery object yet to hear all that arrived of it again, asked,
        // being read or cut short, knows too little to rule on. A give-up
        // with volumes still to read back holds the tree half applied.
        if self.works.out()
            || self.tree.work_due()
            || self.tree.rewalks_owed()
            || self.quieting.is_some()
        {
            return Ok(());
        }
        let mut walked = None;
        while let Some(node) = self.tree.next_recovery_home(walked) {
            walked = Some(node);
            if !self.tree.is_live(node) {
                continue;
            }
            #[cfg(any(test, feature = "pipeline-test-fixtures"))]
            hooks::RULED.with_borrow_mut(|ruled| ruled.push(node));
            let runs = &self.runs;
            let mut lending = self
                .loans
                .lending(slots.as_mut().map(|(slots, limit)| (&mut **slots, *limit)));
            let mut out = Vec::new();
            self.writer.rule(
                &mut self.tree,
                &mut *self.builder,
                &self.root,
                (node, &|namespace| runs.reading(namespace)),
                &mut lending,
                &mut out,
            )?;
            self.heard(out)?;
        }
        self.seal_proven();
        // An ended namespace's obligations nothing can check end here.
        if self.writer.verifications_waiting() {
            self.writer
                .drop_unmet_verifications(&self.tree, &self.coming());
        }
        if !self.writer.rechecks_waiting() && !self.writer.verifications_waiting() {
            return Ok(());
        }
        let mut lending = self
            .loans
            .lending(slots.as_mut().map(|(slots, limit)| (&mut **slots, *limit)));
        if !lending.admit() {
            return Ok(());
        }
        // A replay writes what it reads again, and goes first: while the job
        // waits for room neither it nor a check behind it goes out. A check
        // with no replay before it writes nothing, and goes.
        if self.writer.disk_held() && self.writer.rechecks_waiting() {
            return Ok(());
        }
        if let Some(replay) = self
            .writer
            .take_replay(&self.tree)
            .map_err(WriteError::from)?
        {
            lending.replay(replay);
        } else if let Some(check) = self.writer.take_verification(&self.tree) {
            lending.verify(check);
        }
        Ok(())
    }

    // Every root member the walk's rulings proved whole has its decoder
    // checksums dropped: no set reads them again. A member is proved by its
    // own record being whole and by every root set over it standing clean
    // or repaired, so a pass asks only what could have become proved since
    // the last: the members the root's namespace marked, and the members a
    // set covers when it came to stand clean or a volume of its own came
    // while it stood. A set that left the root may have been the one thing
    // keeping any member unproved, so every member is asked. A job's first
    // pass asks every member.
    fn seal_proven(&mut self) {
        let Some(namespace) = self.tree.namespace(NodeId::ROOT) else {
            return;
        };
        let tree = &self.tree;
        let writer = &self.writer;
        let sets = tree.recovery_nodes(NodeId::ROOT);
        let standing: BTreeSet<NodeId> = sets
            .iter()
            .copied()
            .filter(|set| writer.stands_proved(tree, *set))
            .collect();
        let mut asked: BTreeSet<MemberId> = BTreeSet::new();
        let every = || namespace.members().map(|member| member.id);
        let covered = |set: NodeId| every().filter(move |member| writer.covers(tree, set, *member));
        match &self.sealed_at {
            None => asked.extend(every()),
            Some(sealed) if sealed.sets.iter().any(|set| !sets.contains(set)) => {
                asked.extend(every());
            }
            Some(sealed) => {
                let mut grew: BTreeSet<NodeId> =
                    standing.difference(&sealed.standing).copied().collect();
                for member in namespace.changed_since(sealed.members) {
                    asked.insert(member);
                    if let Some((child, _)) = tree.child_of(NodeId::ROOT, member)
                        && standing.contains(&child)
                    {
                        grew.insert(child);
                    }
                }
                for set in grew {
                    asked.extend(covered(set));
                }
            }
        }
        let proven: Vec<MemberId> = asked
            .into_iter()
            .filter(|member| {
                namespace.get(*member).is_some() && namespace.copy_of(*member).is_none()
            })
            .filter(|member| {
                #[cfg(any(test, feature = "pipeline-test-fixtures"))]
                hooks::SEAL_ASKED.with_borrow_mut(|asked| asked.push(*member));
                writer.proven_whole(tree, (NodeId::ROOT, &sets), *member)
            })
            .collect();
        self.sealed_at = Some(Sealed {
            members: namespace.generation(),
            sets,
            standing,
        });
        for member in proven {
            self.root.seal(VolumeIndex(member.0));
        }
    }

    // Once nothing more can change, every data object hears the lost
    // ranges of its volumes that nothing vouched for, then that the job is
    // quiet, and rules for itself: the tree tells it and reads it what the
    // tree records of it, and the server applies what it says. Children
    // come first, so the deepest object the damage reached rules before any
    // object above it, and a parent never speaks for bytes lost below it.
    // What the losses set going is settled before the object hears quiet.
    // Asked only once every run is started, since a repair's bytes reach a
    // namespace below by its run reading them again. Returns whether
    // anything changed. An object that gives more than one volume up has
    // them read back one a turn: the rest waits in `quieting`, and this
    // returns false, as nothing more is to be stepped until it is done.
    pub(super) fn tell_quiet(&mut self) -> Result<bool, GraphError> {
        if self.quieting.is_some() || !self.quiet() {
            return Ok(false);
        }
        Ok(self.begin_quiet()?.unwrap_or(false))
    }

    // The job stalled: nothing of it is out, due or owed, nothing more of
    // its posts is to come, and still a set or an object waits. No event
    // will bring what it waits for, so its objects hear what the quiet
    // tells them whether every set ruled or not, and what that frees is
    // stepped and lent as after any quiet. A pass that changed something
    // is followed by another while the job still waits on nothing, as the
    // runs' step tells quiet again. Whether anything changed; a give-up
    // with volumes left goes on at the job's next turns.
    pub(crate) fn end_input(
        &mut self,
        slots: &mut RepairSlots,
        limit: usize,
    ) -> Result<bool, GraphError> {
        let mut changed = false;
        while self.quieting.is_none() && self.waits_on_nothing() {
            match self.begin_quiet()? {
                Some(false) => break,
                Some(true) => changed = true,
                None => return Ok(true),
            }
            self.step_runs(Vec::new(), limit)?;
            self.lend(slots, limit)?;
        }
        Ok(changed)
    }

    // Nothing of the job is out on a thread, due, owed to a checkpoint or
    // waiting on a disk, nothing more of any of its posts is to come, and
    // the pipeline is not stopping: nothing the job itself is owed will
    // step it again.
    pub(crate) fn waits_on_nothing(&self) -> bool {
        self.waits_on_runs_only() && self.runs.running().next().is_none()
    }

    // As `waits_on_nothing`, but for runs still reading: none waits for a
    // slot or memory or stands stopped, and nothing else is out.
    pub(crate) fn waits_on_runs_only(&self) -> bool {
        if self.work_pending()
            || self.runs.held_back()
            || self.loans.out()
            || self.works.out()
            || self.mail_owed.is_some()
            || self.works.stopping()
            || self.owes_checkpoint()
            || self.moves_waiting()
            || self.awaits_commit()
            || self.writer.disk_held()
            || self.writer.disk().is_some_and(|disk| !disk.quiet())
        {
            return false;
        }
        let coming = self.coming();
        !coming.payload && coming.sets.is_empty()
    }

    // Whether any run is reading.
    pub(crate) fn reads(&self) -> bool {
        self.runs.running().next().is_some()
    }

    // Whether every run reading waits on its shelf for a change that has
    // not come. A run with no shelf is not waiting on one.
    pub(crate) fn runs_blocked(&self) -> bool {
        self.runs.running().all(|node| {
            self.writer
                .shelf_of(node)
                .is_some_and(|shelf| shelf.blocked())
        })
    }

    // Ask to hear of the next run to begin waiting on its shelf, or ask no
    // longer: see `envelope::Watch`.
    pub(crate) fn watch_runs(
        &self,
        asked: bool,
        tell: impl FnOnce() -> (
            tokio::sync::mpsc::UnboundedSender<super::super::engine::Said>,
            u64,
        ),
    ) {
        self.writer.watch().ask(asked, tell);
    }

    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(crate) fn watch(&self) -> &crate::pipeline::graph::envelope::Watch {
        self.writer.watch()
    }

    // Every object hears its losses then quiet, deepest first: whether
    // anything changed, `None` while a give-up has volumes left.
    fn begin_quiet(&mut self) -> Result<Option<bool>, GraphError> {
        // Nothing more comes into any namespace: the containers below the
        // root no name opened learn whether their folders make them layers,
        // before any object hears quiet, since what opens may hold more.
        if self.tree.layers_pending() {
            let mut out = Vec::new();
            self.writer
                .decide_layers(&mut self.tree, &mut *self.builder, &self.root, &mut out)?;
            self.heard(out)?;
            return Ok(Some(true));
        }
        // The tree owns the order: deepest first, nodes that may still
        // change the namespace above them first, the rest on a pass that
        // changed nothing more.
        let (first, rest) = self.tree.quiet_order();
        let told = |nodes: Vec<NodeId>| {
            nodes
                .into_iter()
                .flat_map(|node| [(node, false), (node, true)])
                .collect()
        };
        self.quieting = Some(Quieting {
            told: told(first),
            rest: told(rest),
            batch: None,
            changed: false,
            rung: false,
            admitted: Vec::new(),
        });
        self.quieten()
    }

    // Whether a give-up the job went quiet into still has volumes to read
    // back. Nothing else of the job is stepped meanwhile.
    pub fn giving_up(&self) -> bool {
        self.quieting.is_some()
    }

    // Whether the pipeline is to wake the job for the next volume of its
    // give-up: once per volume, and not once the pipeline is stopping,
    // when the give-up waits where it stands and the job keeps the
    // checkpoint from before it.
    pub fn ring_give_up(&mut self) -> bool {
        let stopping = self.works.stopping();
        self.quieting
            .as_mut()
            .is_some_and(|quieting| !stopping && !std::mem::replace(&mut quieting.rung, true))
    }

    // The next volume of the give-up, on a turn of its own. Once the last
    // is read back the objects left hear quiet, and what that frees is
    // stepped and lent as the step that went quiet would have.
    pub(crate) fn give_up_next(
        &mut self,
        slots: &mut RepairSlots,
        limit: usize,
    ) -> Result<(), GraphError> {
        let Some(quieting) = self.quieting.as_mut() else {
            return Ok(());
        };
        quieting.rung = false;
        if self.works.stopping() {
            return Ok(());
        }
        let mut admitted = std::mem::take(&mut quieting.admitted);
        if self.quieten()?.is_none() {
            self.hold_admitted(&mut admitted);
            return Ok(());
        }
        self.step_runs(admitted, limit)?;
        self.lend(slots, limit)
    }

    // While a give-up has volumes left, the runs a step admitted wait with
    // it, and nothing is stepped: true then.
    pub(super) fn hold_admitted(&mut self, admitted: &mut Vec<NodeId>) -> bool {
        let Some(quieting) = self.quieting.as_mut() else {
            return false;
        };
        quieting.admitted.append(admitted);
        true
    }

    // Tell the objects left what `quieting` holds, deepest first: whether
    // anything changed once all have heard, `None` while a give-up has
    // volumes left. A failed step drops what is left, as it always did.
    fn quieten(&mut self) -> Result<Option<bool>, GraphError> {
        let quieted = self.quieten_on();
        if quieted.is_err() {
            self.quieting = None;
        }
        quieted
    }

    fn quieten_on(&mut self) -> Result<Option<bool>, GraphError> {
        loop {
            if let Some(mut batch) = self.quieting.as_mut().and_then(|q| q.batch.take()) {
                let done = self.writer.give_up_next(
                    &mut self.tree,
                    &mut *self.builder,
                    &self.root,
                    &mut batch,
                )?;
                if !done {
                    let one_turn = self.one_turn_held();
                    if let Some(quieting) = self.quieting.as_mut() {
                        quieting.batch = Some(batch);
                    }
                    if one_turn {
                        continue;
                    }
                    return Ok(None);
                }
                self.heard(batch.into_out())?;
            }
            let Some(quieting) = self.quieting.as_mut() else {
                return Ok(Some(false));
            };
            // The rest hear once the objects a lift may take from changed
            // nothing; otherwise the next pass tells them.
            if quieting.told.is_empty() && !quieting.changed {
                quieting.told = std::mem::take(&mut quieting.rest);
            }
            let Some((node, quiet)) = quieting.told.pop_front() else {
                let changed = quieting.changed;
                self.quieting = None;
                return Ok(Some(changed));
            };
            let mut effects = Vec::new();
            let placed = self.tree.placement(node);
            let mut placing = self.writer.placing(&mut *self.builder);
            if quiet {
                self.tree.quiet(node, &mut placing, &mut effects);
            } else {
                self.tree.tell_lost(node, &mut placing, &mut effects);
            }
            // An object that placed its volumes anew changed the job though
            // the tree said nothing: the bytes held for it go down now, as
            // after any write, and it hears quiet again once they have.
            if effects.is_empty() && self.tree.placement(node) == placed {
                continue;
            }
            quieting.changed = true;
            let mut out = Vec::new();
            let batch = self.writer.apply_quiet(
                &mut self.tree,
                &mut *self.builder,
                &self.root,
                effects,
                &mut out,
            )?;
            let Some(batch) = batch else {
                self.heard(out)?;
                continue;
            };
            if let Some(quieting) = self.quieting.as_mut() {
                quieting.batch = Some(batch);
            }
            if !self.one_turn_held() {
                return Ok(None);
            }
        }
    }

    // A test may hold the job to the one turn every give-up took before.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    fn one_turn_held(&self) -> bool {
        self.one_turn
    }

    #[cfg(not(any(test, feature = "pipeline-test-fixtures")))]
    fn one_turn_held(&self) -> bool {
        false
    }

    // The payload is in, no run is reading, no repair is out, no recovery
    // object owes a rewalk, and every namespace's sets have ruled on something other than waiting, none
    // with a volume of its own still arriving.
    fn quiet(&self) -> bool {
        if self.runs.live()
            || self.loans.out()
            || self.works.out()
            || self.tree.rewalks_owed()
            || self.tree.work_due()
            || self.writer.rechecks_waiting()
        {
            return false;
        }
        let coming = self.coming();
        let runs = &self.runs;
        !coming.payload
            && !self.writer.verifications_due(&self.tree, &coming)
            && self
                .writer
                .ruled_but_for_quiet(&self.tree, &coming, &|namespace| runs.reading(namespace))
    }

    // Ranges of lost articles go to the root namespace as soon as they can
    // be told, which marks them on every member they land in at every
    // depth; whatever covers them repairs them.
    pub(super) fn mark_damage(&mut self) -> Result<(), GraphError> {
        let lost = self.root.damage()?;
        if lost.is_empty() {
            return Ok(());
        }
        let mut out = Vec::new();
        self.writer.lost(
            &mut self.tree,
            &mut *self.builder,
            &self.root,
            (NodeId::ROOT, lost),
            &mut out,
        )?;
        self.heard(out)
    }
}

// Where the last seal pass left the root: its namespace's generation, its
// sets, and those of them that stood clean or repaired.
pub(super) struct Sealed {
    members: u64,
    sets: Vec<NodeId>,
    standing: BTreeSet<NodeId>,
}

// The job's objects hearing that it is quiet, held across turns while a
// give-up reads its volumes back: what each object left is still to hear,
// its losses (`false`) then quiet (`true`), the batch being applied, whether
// anything changed so far, whether the pipeline was asked to wake the job
// for the next volume, and the runs admitted meanwhile.
pub(super) struct Quieting {
    told: std::collections::VecDeque<(NodeId, bool)>,
    // The objects no lift takes a volume out of, told once those it may
    // take from changed nothing.
    rest: std::collections::VecDeque<(NodeId, bool)>,
    batch: Option<GivingUp>,
    changed: bool,
    rung: bool,
    admitted: Vec<NodeId>,
}

// What the repair walk asked, for its tests compiled apart in
// tests/pipeline_graph.rs.
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
#[doc(hidden)]
pub mod hooks {
    use std::cell::RefCell;

    use weaver_graph::{MemberId, NodeId};

    thread_local! {
        // The namespaces the walk asked to rule, in the order it asked.
        pub static RULED: RefCell<Vec<NodeId>> = const { RefCell::new(Vec::new()) };
        // The root members a seal pass asked whether the sets proved them.
        pub static SEAL_ASKED: RefCell<Vec<MemberId>> = const { RefCell::new(Vec::new()) };
    }
}
