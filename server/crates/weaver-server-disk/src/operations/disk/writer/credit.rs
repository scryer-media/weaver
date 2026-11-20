// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a thread that produces an owner's output counts for it before the
// queuing thread has it. The producing thread counts the bytes on every
// volume they may land on (`Credit::reserve`), standing still on its own
// thread while a volume is at its limit, and hands the credit over with the
// bytes. The queuing thread pays every write of the step that takes them
// out of it (`Credit::prepay`), so it queues them at once and never stands
// still for a disk. What the step did not write it carries to the next one
// only for the bytes of that output it still holds in memory, on the volume
// each will be written to, and gives back the rest (`Paying::settle`); as
// those bytes are written or dropped, it gives back what it carried for
// them (`Credit::take_on`, `Credit::give_back_on`). A byte is written to one
// volume, so a write on any of them frees what was counted for it on the
// others.
//
// Credit given back wakes whoever stands for room on its volume, as the
// writer does when it writes: a producing thread waiting in `reserve`, and
// the owners that asked for a wake. The credit is the bytes' count; the
// bytes themselves are handed over as they are, never copied.

use std::sync::Arc;

use super::Submitter;
use crate::operations::disk::volume::{Reservation, Volume};

// What a stop does to a producing thread's wait for room (`Credit::reserve`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stand {
    // The stop ends the wait, with nothing more counted: a thread the stop
    // ends as well, whose words after it are not heard.
    UntilStop,
    // The wait keeps the volume's limit through the stop, until its end,
    // as the queuing thread's own waits do: what the bytes are for is still
    // applied while the owner stops. At that end the key says it gave up
    // (`Submitter::gave_up`).
    UntilStopsEnd,
}

// Bytes counted on an owner's volumes ahead of output a producing thread
// hands the queuing thread, one share for each volume. In hand, the credit
// counts as any reservation does; carried by the queuing thread between its
// steps, it is marked carried under its key on its volume
// (`Submitter::carried`), and only that key's producers are granted room
// beside it. Whatever it still holds is given back when it is dropped.
#[derive(Default)]
pub struct Credit {
    shares: Vec<Share>,
}

struct Share {
    submitter: Arc<Submitter>,
    held: Reservation,
    // The share's bytes are marked carried under its key.
    carried: bool,
}

impl Share {
    fn uncarry(&mut self) {
        if std::mem::take(&mut self.carried) {
            self.submitter.carried(0, self.held.bytes());
        }
    }

    fn carry(&mut self) {
        if !self.carried {
            self.carried = true;
            self.submitter.carried(self.held.bytes(), 0);
        }
    }

    fn on(&self, volume: &Volume) -> bool {
        std::ptr::eq(self.held.volume().as_ref(), volume)
    }

    // `bytes` of the share given back, the rest kept as it was marked.
    fn give_back(&mut self, bytes: u64) {
        let bytes = bytes.min(self.held.bytes());
        if bytes == 0 {
            return;
        }
        let carried = self.carried;
        self.uncarry();
        self.held.give_back(bytes);
        if carried {
            self.carry();
        }
        gave_back(&self.submitter);
    }
}

// Counted bytes were given back on the submitter's volume, or a requester's
// carried bytes grew there: whoever waits for room looks again, as after a
// write. (A release wakes the standing threads by itself; a carry does not.)
fn gave_back(submitter: &Submitter) {
    let state = &submitter.lane.state;
    state.look_below();
    state.owners.room(state.volume.take_wake());
}

impl Credit {
    pub fn none() -> Self {
        Self::default()
    }

    // Count `bytes` on the volume of each of `submitters`, standing still on
    // the calling thread until each has them below its hard limit
    // (`Submitter::reserve_ahead`). Only a thread that produces what the
    // queuing thread writes calls this, never the queuing thread itself. A
    // share comes back short when its wait ended first: the key stopped,
    // the volume has no thread, or the stop ended it as `stand` says.
    //
    // It never stands for one volume while it holds bytes on another: when
    // a volume refuses, what the others granted goes back first, and it
    // stands holding nothing. So two producers that want the same volumes
    // in another order never each hold one and stand for the other.
    pub fn reserve<'a>(
        submitters: impl IntoIterator<Item = &'a Arc<Submitter>>,
        bytes: u64,
        stand: Stand,
    ) -> Self {
        let submitters: Vec<&Arc<Submitter>> = submitters.into_iter().collect();
        let mut held: Vec<Option<Reservation>> = submitters.iter().map(|_| None).collect();
        loop {
            let refused = submitters
                .iter()
                .zip(held.iter_mut())
                .position(|(submitter, held)| {
                    if held.is_none() {
                        *held = submitter.try_reserve_ahead(bytes, stand);
                    }
                    held.is_none()
                });
            let Some(refused) = refused else {
                break;
            };
            for (submitter, held) in submitters.iter().zip(held.iter_mut()) {
                if let Some(granted) = held.take()
                    && granted.bytes() > 0
                {
                    drop(granted);
                    gave_back(submitter);
                }
            }
            let stood = submitters[refused].reserve_ahead(bytes, stand);
            let short = stood.bytes() < bytes;
            held[refused] = Some(stood);
            if short {
                break;
            }
        }
        let shares = submitters
            .into_iter()
            .zip(held)
            .map(|(submitter, held)| Share {
                held: held.unwrap_or_else(|| Reservation::none(Arc::clone(submitter.volume()))),
                submitter: Arc::clone(submitter),
                carried: false,
            })
            .collect();
        Self { shares }
    }

    // `reserve` without standing, for the queuing thread itself: `None`
    // when any volume refuses the bytes now, with whatever the others
    // granted given back. A share that ended short as `reserve`'s would,
    // with nothing counted, is no refusal.
    pub fn try_reserve<'a>(
        submitters: impl IntoIterator<Item = &'a Arc<Submitter>>,
        bytes: u64,
        stand: Stand,
    ) -> Option<Self> {
        let mut shares = Vec::new();
        for submitter in submitters {
            let Some(held) = submitter.try_reserve_ahead(bytes, stand) else {
                for share in shares.drain(..) {
                    let Share {
                        submitter, held, ..
                    } = share;
                    let granted = held.bytes() > 0;
                    drop(held);
                    if granted {
                        gave_back(&submitter);
                    }
                }
                return None;
            };
            shares.push(Share {
                held,
                submitter: Arc::clone(submitter),
                carried: false,
            });
        }
        Some(Self { shares })
    }

    // Whether every share holds `bytes`: no wait ended short of them.
    pub fn covers(&self, bytes: u64) -> bool {
        self.shares.iter().all(|share| share.held.bytes() >= bytes)
    }

    // Whether the credit counts nothing.
    pub fn is_empty(&self) -> bool {
        self.shares.iter().all(|share| share.held.bytes() == 0)
    }

    // The bytes the credit counts on `volume`.
    pub fn on(&self, volume: &Volume) -> u64 {
        self.shares
            .iter()
            .filter(|share| share.on(volume))
            .map(|share| share.held.bytes())
            .sum()
    }

    // Hold what `other` holds as well, share by share on each volume. The
    // credit is in hand afterwards, carried nowhere.
    pub fn join(&mut self, mut other: Credit) {
        for share in &mut self.shares {
            share.uncarry();
        }
        for mut theirs in std::mem::take(&mut other.shares) {
            theirs.uncarry();
            match self
                .shares
                .iter_mut()
                .find(|share| Arc::ptr_eq(share.held.volume(), theirs.held.volume()))
            {
                Some(share) => {
                    let held = std::mem::replace(
                        &mut theirs.held,
                        Reservation::none(Arc::clone(share.held.volume())),
                    );
                    share.held.join(held);
                }
                None => self.shares.push(theirs),
            }
        }
    }

    // Mark the whole credit carried by the queuing thread from here, and
    // have whoever stands for room on its volumes look again: a producer
    // under the same key is granted room beside it (`Volume::grant`).
    pub fn carry(&mut self) {
        for share in &mut self.shares {
            if !share.carried && share.held.bytes() > 0 {
                share.carry();
                gave_back(&share.submitter);
            }
        }
    }

    // Up to `bytes` of what the credit counts on `volume`, as a credit of
    // its own, in hand: what was carried for bytes that leave the queuing
    // thread's hands now, to pay their write (`prepay`). The rest stays as
    // it was marked.
    pub fn take_on(&mut self, volume: &Volume, bytes: u64) -> Credit {
        let Some(share) = self.shares.iter_mut().find(|share| share.on(volume)) else {
            return Credit::none();
        };
        let bytes = bytes.min(share.held.bytes());
        let carried = share.carried;
        share.uncarry();
        let taken = share.held.split(bytes);
        if carried {
            share.carry();
        }
        Credit {
            shares: vec![Share {
                submitter: Arc::clone(&share.submitter),
                held: taken,
                carried: false,
            }],
        }
    }

    // Give back up to `bytes` of what the credit counts on `volume`: bytes
    // it was carried for were dropped, or no longer held in memory.
    pub fn give_back_on(&mut self, volume: &Volume, bytes: u64) {
        if let Some(share) = self.shares.iter_mut().find(|share| share.on(volume)) {
            share.give_back(bytes);
        }
    }

    // The whole credit as one share on `to`'s volume, in hand: bytes it was
    // counted for will be written there instead. What it counted elsewhere
    // is given back there and counted here, in one step on each.
    pub fn moved_to(mut self, to: &Arc<Submitter>) -> Credit {
        let mut held = Reservation::none(Arc::clone(to.volume()));
        for mut share in std::mem::take(&mut self.shares) {
            share.uncarry();
            let moved = !Arc::ptr_eq(share.held.volume(), to.volume()) && share.held.bytes() > 0;
            let bytes = share.held.bytes();
            held.join(share.held.split(bytes));
            if moved {
                gave_back(&share.submitter);
            }
        }
        Credit {
            shares: vec![Share {
                submitter: Arc::clone(to),
                held,
                carried: false,
            }],
        }
    }

    // Move the whole credit ahead of the writes the queuing thread is
    // about to queue in this step, on each volume: they take it first, and
    // are counted only past it (`Submitter::write`). The bytes stay counted
    // until the step is settled (`Paying::settle`). Called on the queuing
    // thread, which prepays and writes from one thread.
    pub fn prepay(mut self) -> Paying {
        let shares = std::mem::take(&mut self.shares)
            .into_iter()
            .map(|mut share| {
                share.uncarry();
                let bytes = share.held.bytes();
                let before =
                    super::prepay_on(share.held.volume(), share.submitter.key.owner.slot, bytes);
                share.held.hand_over(bytes);
                Prepaid {
                    submitter: share.submitter,
                    bytes,
                    before,
                }
            })
            .collect();
        Paying { shares }
    }
}

impl Drop for Credit {
    fn drop(&mut self) {
        for mut share in std::mem::take(&mut self.shares) {
            share.uncarry();
            let given = share.held.bytes();
            drop(share.held);
            if given > 0 {
                gave_back(&share.submitter);
            }
        }
    }
}

impl std::fmt::Debug for Credit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.shares.iter().map(|share| &share.held))
            .finish()
    }
}

// A credit moved ahead of one step's writes (`Credit::prepay`), until the
// step is settled. Dropped unsettled, it is settled with nothing held.
#[must_use = "what was prepaid stays counted until it is settled"]
pub struct Paying {
    shares: Vec<Prepaid>,
}

struct Prepaid {
    submitter: Arc<Submitter>,
    // What this credit moved ahead on the volume.
    bytes: u64,
    // What was ahead on it before: another prepayment's, taken after this
    // one's.
    before: u64,
}

impl Paying {
    // The step's writes are queued. What they took of the credit on any
    // volume was written there, and is no longer held for on the others.
    // Of what they left, the credit carries on to the next step what the
    // queuing thread still holds of the output in memory to be written to
    // each volume, at most `held` of that volume's bytes, marked carried;
    // the rest is given back, and whoever waits for room on its volume
    // looks again.
    pub fn settle(mut self, held: impl Fn(&Volume) -> u64) -> Credit {
        self.settle_holding(&held)
    }

    fn settle_holding(&mut self, held: &dyn Fn(&Volume) -> u64) -> Credit {
        let shares = std::mem::take(&mut self.shares);
        let left: Vec<u64> = shares
            .iter()
            .map(|share| {
                super::settle_on(
                    share.submitter.volume(),
                    share.submitter.key.owner.slot,
                    share.bytes,
                    share.before,
                )
            })
            .collect();
        let written: u64 = shares
            .iter()
            .zip(&left)
            .map(|(share, left)| share.bytes - left)
            .sum();
        let shares = shares
            .into_iter()
            .zip(left)
            .map(|(share, left)| {
                let volume = Arc::clone(share.submitter.volume());
                let kept = left
                    .min(share.bytes.saturating_sub(written))
                    .min(held(&volume));
                let mut counted = Reservation::counted(Arc::clone(&volume), left);
                counted.give_back(left - kept);
                share.submitter.carried(kept, 0);
                // Bytes given back make room, and bytes carried grant the
                // key's producers room beside them (`Volume::grant`):
                // either way whoever waits on the volume looks again, after
                // both.
                if left > 0 {
                    gave_back(&share.submitter);
                }
                Share {
                    submitter: share.submitter,
                    held: counted,
                    carried: true,
                }
            })
            .collect();
        Credit { shares }
    }
}

impl Drop for Paying {
    fn drop(&mut self) {
        if !self.shares.is_empty() {
            drop(self.settle_holding(&|_| 0));
        }
    }
}
