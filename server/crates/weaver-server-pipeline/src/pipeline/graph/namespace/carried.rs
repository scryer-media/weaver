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
    // While a step hears a run's output: the namespace it lands in. Only
    // bytes held there, then, are carried for.
    into: Option<NodeId>,
}

struct Held {
    bytes: u64,
    volume: Option<Arc<Volume>>,
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
    pub(crate) fn settle_credit(&mut self, tree: &Tree, paying: Paying) {
        self.carried.into = None;
        let keys: Vec<(NodeId, MemberId)> = self.carried.members.keys().copied().collect();
        for (node, member) in keys {
            let resident = self.member_resident(node, member);
            let landing = self.held_landing(tree, node, member);
            let Some(held) = self.carried.members.get_mut(&(node, member)) else {
                continue;
            };
            held.bytes = held.bytes.min(resident);
            if held.volume.is_none() {
                held.volume = landing;
            }
            if held.bytes == 0 || held.volume.is_none() {
                self.carried.members.remove(&(node, member));
            }
        }
        let members = &self.carried.members;
        let mut settled = paying.settle(|volume| {
            members
                .values()
                .filter(|held| {
                    held.volume
                        .as_ref()
                        .is_some_and(|on| std::ptr::eq(on.as_ref(), volume))
                })
                .map(|held| held.bytes)
                .sum()
        });
        settled.join(std::mem::take(&mut self.carried.credit));
        settled.carry();
        self.carried.credit = settled;
    }

    // `len` bytes of the member were held in memory: carried for when they
    // are the output of the run the step hears.
    pub(super) fn credit_held(&mut self, node: NodeId, member: MemberId, len: u64) {
        if self.carried.into == Some(node) && len > 0 {
            self.carried
                .members
                .entry((node, member))
                .or_insert(Held {
                    bytes: 0,
                    volume: None,
                })
                .bytes += len;
        }
    }

    // The member's holds fell: what is carried for more than it still holds
    // in memory is given back.
    pub(super) fn uncredit_past_resident(&mut self, node: NodeId, member: MemberId) {
        if !self.carried.members.contains_key(&(node, member)) {
            return;
        }
        let resident = self.member_resident(node, member);
        let carried = &mut self.carried;
        let Some(held) = carried.members.get_mut(&(node, member)) else {
            return;
        };
        if held.bytes <= resident {
            return;
        }
        let excess = held.bytes - resident;
        held.bytes = resident;
        if let Some(volume) = &held.volume {
            carried.credit.give_back_on(volume, excess);
        }
        if resident == 0 {
            carried.members.remove(&(node, member));
        }
    }

    // Every member of the namespace `node` held less, or the namespace is
    // gone: each is looked at as `uncredit_past_resident` does.
    pub(super) fn uncredit_node(&mut self, node: NodeId) {
        if self.carried.members.is_empty() {
            return;
        }
        let members: Vec<MemberId> = self
            .carried
            .members
            .keys()
            .filter(|(at, _)| *at == node)
            .map(|(_, member)| *member)
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
        let held = self.carried.members.get_mut(&(node, member))?;
        let from = held
            .volume
            .replace(Arc::clone(to.volume()))
            .unwrap_or_else(|| Arc::clone(to.volume()));
        let mut part = self.carried.credit.take_on(&from, held.bytes);
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
        let kept = match self.carried.members.get_mut(&(node, member)) {
            Some(held) => {
                held.bytes = held.bytes.min(resident);
                held.bytes
            }
            None => 0,
        };
        if kept == 0 {
            self.carried.members.remove(&(node, member));
        }
        if let Some(PaidHeld { paying, volume }) = paid {
            let mut left = paying.settle(|on| if std::ptr::eq(on, volume.as_ref()) { kept } else { 0 });
            left.join(std::mem::take(&mut self.carried.credit));
            left.carry();
            self.carried.credit = left;
        }
    }

    // The bytes the member holds in memory.
    fn member_resident(&self, node: NodeId, member: MemberId) -> u64 {
        self.files.get(&node).map_or(0, |files| {
            files
                .holds
                .resident_spans(held_volume(member))
                .map(|span| span.len())
                .sum()
        })
    }

    // The volume the member's held bytes are written to, as the tree has it
    // now.
    fn held_landing(&self, tree: &Tree, node: NodeId, member: MemberId) -> Option<Arc<Volume>> {
        self.disk_under(self.root_for(tree, node, member))
            .map(|to| Arc::clone(to.volume()))
    }
}
