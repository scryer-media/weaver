// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The credit the loop carries between its steps for a run's output it still
// holds in memory (`writer::Credit`). Only bytes the run's own output put in
// a hold are carried for, member by member, on the volume each member's
// bytes are written to, and only while they stay in memory: as they are
// written, what was carried for them pays the write; as they are dropped,
// paged out or their namespace goes, it is given back. Bytes some other
// path holds in the same namespace, a download's articles say, are never
// carried for here: their own path counts them.

use std::collections::BTreeMap;
use std::sync::Arc;

use weaver_graph::tree::Tree;
use weaver_graph::{MemberId, NodeId};
use weaver_server_disk::operations::disk::volume::Volume;
use weaver_server_disk::operations::disk::writer::{Credit, Paying};

use super::{NamespaceWriter, held_volume};

#[derive(Default)]
pub(super) struct Carried {
    // What the loop carries, on each volume.
    credit: Credit,
    // The bytes of each member's holds the credit is carried for, and the
    // volume they will be written to: unknown until the step that held them
    // is settled. Never more than the member holds in memory.
    members: BTreeMap<(NodeId, MemberId), Held>,
    // What `members` carries for on each volume, kept as they change: the
    // few volumes a job writes to, so asking costs those, not the members.
    on: Vec<(Arc<Volume>, u64)>,
    // The members first carried for in the step being heard, whose volume
    // is not known yet: all the step's settling has to look at.
    unplaced: Vec<(NodeId, MemberId)>,
    // While a step hears a run's output: the namespace it lands in. Only
    // bytes held there, then, are carried for.
    into: Option<NodeId>,
}

struct Held {
    bytes: u64,
    volume: Option<Arc<Volume>>,
}

impl Carried {
    // The member is carried for `bytes`, to be written to `volume`, in place
    // of what it was: the totals on each volume follow, and a member carried
    // for nothing goes.
    fn set(&mut self, key: (NodeId, MemberId), bytes: u64, volume: Option<Arc<Volume>>) {
        if let Some(old) = self.members.remove(&key) {
            self.count(&old, false);
        }
        if bytes == 0 {
            return;
        }
        let held = Held { bytes, volume };
        self.count(&held, true);
        self.members.insert(key, held);
    }

    fn count(&mut self, held: &Held, add: bool) {
        let Some(volume) = &held.volume else {
            return;
        };
        match self.on.iter().position(|(on, _)| Arc::ptr_eq(on, volume)) {
            Some(at) if add => self.on[at].1 += held.bytes,
            Some(at) => {
                self.on[at].1 -= held.bytes;
                if self.on[at].1 == 0 {
                    self.on.swap_remove(at);
                }
            }
            None if add && held.bytes > 0 => self.on.push((Arc::clone(volume), held.bytes)),
            None => {}
        }
    }

    // What the members carry for on `volume`.
    fn on(&self, volume: &Volume) -> u64 {
        self.on
            .iter()
            .find(|(on, _)| std::ptr::eq(on.as_ref(), volume))
            .map_or(0, |(_, bytes)| *bytes)
    }
}

// What a write of held bytes was prepaid with (`NamespaceWriter::pay_held`).
pub(super) struct PaidHeld {
    paying: Paying,
    volume: Arc<Volume>,
}

impl NamespaceWriter {
    // The credit the loop carries, taken to be prepaid with a step's own.
    pub(crate) fn take_credit(&mut self) -> Credit {
        std::mem::take(&mut self.carried.credit)
    }

    // What the loop carries on `volume`, and what the members carry for
    // there.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn carried_on_for_test(&self, volume: &Volume) -> (u64, u64) {
        (self.carried.credit.on(volume), self.carried.on(volume))
    }

    // A step heard with `credit`, in which `len` bytes the member holds in
    // memory are a run's output held in `node`: the member is carried for
    // them as the step settles.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn carry_held_for_test(
        &mut self,
        tree: &Tree,
        (node, member): (NodeId, MemberId),
        len: u64,
        credit: Credit,
    ) {
        let paying = credit.prepay();
        self.crediting(Some(node));
        self.credit_held(node, member, len);
        self.settle_credit(tree, paying);
    }

    // The carried credit taken into a step with `fresh`, prepaid, as a step
    // that hears a run's output takes it (`GraphJob::run_said`).
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn begin_step_for_test(&mut self, fresh: Credit) -> Paying {
        let mut credit = self.take_credit();
        credit.join(fresh);
        credit.prepay()
    }

    // The step prepaid with `paying` is settled.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn settle_step_for_test(&mut self, tree: &Tree, paying: Paying) {
        self.settle_credit(tree, paying);
    }

    // Whether the loop carries nothing.
    pub(crate) fn carries_nothing(&self) -> bool {
        self.carried.credit.is_empty()
    }

    // Bytes held in `into` from here are a run's output the step's credit
    // was counted for; `None` once the step is heard.
    pub(crate) fn crediting(&mut self, into: Option<NodeId>) {
        self.carried.into = into;
    }

    // The step that `paying` was prepaid for is heard: carry on what it did
    // not write, on each volume, for what the run's output still holds in
    // memory to be written there, and give back the rest.
    //
    // Every other member already carries at most what it holds in memory,
    // since each path that takes held bytes out of memory gives back what
    // was carried past that, and already knows its volume: only the members
    // first carried for in this step are looked at.
    pub(crate) fn settle_credit(&mut self, tree: &Tree, paying: Paying) {
        self.carried.into = None;
        for (node, member) in std::mem::take(&mut self.carried.unplaced) {
            let Some(held) = self.carried.members.get(&(node, member)) else {
                continue;
            };
            if held.volume.is_some() {
                continue;
            }
            let bytes = held.bytes.min(self.member_resident(node, member));
            let volume = self.held_landing(tree, node, member);
            let bytes = if volume.is_some() { bytes } else { 0 };
            self.carried.set((node, member), bytes, volume);
        }
        // What a held release's prepayment left during the step is carried
        // already (`Self::paid_held`): the step keeps only the rest, so the
        // two together carry what the members do.
        let carried = &self.carried;
        let mut settled =
            paying.settle(|volume| carried.on(volume).saturating_sub(carried.credit.on(volume)));
        settled.join(std::mem::take(&mut self.carried.credit));
        settled.carry();
        self.carried.credit = settled;
    }

    // `len` bytes of the member were held in memory: carried for when they
    // are the output of the run the step hears.
    pub(super) fn credit_held(&mut self, node: NodeId, member: MemberId, len: u64) {
        if self.carried.into != Some(node) || len == 0 {
            return;
        }
        let (bytes, volume) = match self.carried.members.get(&(node, member)) {
            Some(held) => (held.bytes, held.volume.clone()),
            None => {
                self.carried.unplaced.push((node, member));
                (0, None)
            }
        };
        self.carried
            .set((node, member), bytes.saturating_add(len), volume);
    }

    // The member's holds fell: what is carried for more than it still holds
    // in memory is given back.
    pub(super) fn uncredit_past_resident(&mut self, node: NodeId, member: MemberId) {
        if !self.carried.members.contains_key(&(node, member)) {
            return;
        }
        let resident = self.member_resident(node, member);
        let carried = &mut self.carried;
        let Some(held) = carried.members.get(&(node, member)) else {
            return;
        };
        if held.bytes <= resident {
            return;
        }
        let excess = held.bytes - resident;
        let volume = held.volume.clone();
        if let Some(volume) = &volume {
            carried.credit.give_back_on(volume, excess);
        }
        carried.set((node, member), resident, volume);
    }

    // Every member of the namespace `node` held less, or the namespace is
    // gone: each is looked at as `uncredit_past_resident` does.
    pub(super) fn uncredit_node(&mut self, node: NodeId) {
        if self.carried.members.is_empty() {
            return;
        }
        // Members are keyed node first, so the node's are one range.
        let members: Vec<MemberId> = self
            .carried
            .members
            .range((node, MemberId(0))..=(node, MemberId(u32::MAX)))
            .map(|((_, member), _)| *member)
            .collect();
        for member in members {
            self.uncredit_past_resident(node, member);
        }
    }

    // Before the member's held bytes are taken to be written or handed to
    // its object: what is carried for them is prepaid on the volume they
    // are written to, moved there first when it was counted elsewhere.
    pub(super) fn pay_held(
        &mut self,
        tree: &Tree,
        node: NodeId,
        member: MemberId,
    ) -> Option<PaidHeld> {
        if !self.carried.members.contains_key(&(node, member)) {
            return None;
        }
        let to = self.disk_under(self.root_for(tree, node, member))?;
        let held = self.carried.members.get(&(node, member))?;
        let bytes = held.bytes;
        let from = held
            .volume
            .clone()
            .unwrap_or_else(|| Arc::clone(to.volume()));
        self.carried
            .set((node, member), bytes, Some(Arc::clone(to.volume())));
        let mut part = self.carried.credit.take_on(&from, bytes);
        if !Arc::ptr_eq(&from, to.volume()) {
            part = part.moved_to(&to);
        }
        Some(PaidHeld {
            paying: part.prepay(),
            volume: Arc::clone(to.volume()),
        })
    }

    // The member's held bytes were taken: what is still carried for it is at
    // most what it still holds in memory, and what its prepayment did not
    // pay goes back to the carried credit for that, or is given back.
    pub(super) fn paid_held(&mut self, node: NodeId, member: MemberId, paid: Option<PaidHeld>) {
        let resident = self.member_resident(node, member);
        let kept = match self.carried.members.get(&(node, member)) {
            Some(held) => {
                let (kept, volume) = (held.bytes.min(resident), held.volume.clone());
                self.carried.set((node, member), kept, volume);
                kept
            }
            None => 0,
        };
        if let Some(PaidHeld { paying, volume }) = paid {
            let mut left = paying.settle(|on| {
                if std::ptr::eq(on, volume.as_ref()) {
                    kept
                } else {
                    0
                }
            });
            left.join(std::mem::take(&mut self.carried.credit));
            left.carry();
            self.carried.credit = left;
        }
    }

    // The bytes the member holds, in memory or paged out, that no carried
    // credit pays the write of: what a release of them to its file
    // (`release_to_leaf`) writes unpaid. Two lookups.
    pub(super) fn held_uncarried(&self, node: NodeId, member: MemberId) -> u64 {
        let held = self
            .files
            .get(&node)
            .map_or(0, |files| files.holds.held_bytes(held_volume(member)));
        let carried = self
            .carried
            .members
            .get(&(node, member))
            .map_or(0, |held| held.bytes);
        held.saturating_sub(carried)
    }

    // The bytes the member holds in memory. Two lookups.
    fn member_resident(&self, node: NodeId, member: MemberId) -> u64 {
        self.files.get(&node).map_or(0, |files| {
            files.holds.resident_bytes_in(held_volume(member))
        })
    }

    // The volume the member's held bytes are written to, as the tree has it
    // now.
    fn held_landing(&self, tree: &Tree, node: NodeId, member: MemberId) -> Option<Arc<Volume>> {
        self.disk_under(self.root_for(tree, node, member))
            .map(|to| Arc::clone(to.volume()))
    }
}
