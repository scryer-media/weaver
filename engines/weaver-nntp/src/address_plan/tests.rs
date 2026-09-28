use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::*;

fn addr(last: u8) -> SocketAddr {
    SocketAddr::from(([192, 0, 2, last], 563))
}

enum Answer {
    Refuse,
    /// Connect once the test sends on the paired sender.
    ConnectWhenReleased(Receiver<()>),
}

/// A dialer that answers each address the way the test scripted it and
/// remembers which addresses it was asked to dial.
struct ScriptedDialer {
    resolved: Mutex<Option<Vec<SocketAddr>>>,
    answers: Mutex<HashMap<SocketAddr, Answer>>,
    dialled: Mutex<Vec<SocketAddr>>,
}

impl ScriptedDialer {
    fn new(resolved: &[SocketAddr]) -> Arc<Self> {
        Arc::new(Self {
            resolved: Mutex::new(Some(resolved.to_vec())),
            answers: Mutex::new(HashMap::new()),
            dialled: Mutex::new(Vec::new()),
        })
    }

    fn answer(&self, addr: SocketAddr, answer: Answer) {
        self.answers.lock().unwrap().insert(addr, answer);
    }

    fn gate(&self, addr: SocketAddr) -> Sender<()> {
        let (tx, rx) = channel();
        self.answer(addr, Answer::ConnectWhenReleased(rx));
        tx
    }

    fn resolve_to_nothing(&self) {
        *self.resolved.lock().unwrap() = None;
    }

    fn take_dialled(&self) -> Vec<SocketAddr> {
        std::mem::take(&mut *self.dialled.lock().unwrap())
    }
}

impl AddressDialer for ScriptedDialer {
    type Stream = SocketAddr;

    fn resolve(&self) -> io::Result<Vec<SocketAddr>> {
        self.resolved
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "resolver answered nothing"))
    }

    fn dial(&self, addr: SocketAddr) -> io::Result<SocketAddr> {
        self.dialled.lock().unwrap().push(addr);
        let answer = self.answers.lock().unwrap().remove(&addr);
        let gate = match answer {
            None => None,
            Some(Answer::Refuse) => {
                self.answers.lock().unwrap().insert(addr, Answer::Refuse);
                return Err(io::Error::from(io::ErrorKind::ConnectionRefused));
            }
            Some(Answer::ConnectWhenReleased(gate)) => Some(gate),
        };
        if let Some(gate) = gate {
            // A dropped sender releases the gate too.
            let _ = gate.recv();
        }
        Ok(addr)
    }
}

fn plan() -> Arc<AddressPlan> {
    Arc::new(AddressPlan::new("news.example.test:563".into()))
}

/// Wait until the race's losing attempts to `addrs` have finished and been
/// booked, so none of them lands in the middle of what the test does next.
fn settle(plan: &AddressPlan, addrs: &[SocketAddr]) {
    while plan
        .snapshot()
        .addresses
        .iter()
        .any(|address| addrs.contains(&address.address) && address.connect_time.is_none())
    {
        std::thread::yield_now();
    }
}

/// Pin `winner` with a first race in which every other address is held until
/// the race is over.
fn pin(
    plan: &Arc<AddressPlan>,
    dialer: &Arc<ScriptedDialer>,
    winner: SocketAddr,
    others: &[SocketAddr],
) {
    let gates: Vec<_> = others.iter().map(|other| dialer.gate(*other)).collect();
    let (_, pinned) = plan.connect(dialer, false).unwrap();
    assert_eq!(pinned, winner);
    drop(gates);
    settle(plan, others);
    dialer.take_dialled();
}

#[test]
fn the_first_address_to_answer_is_pinned_and_later_connects_dial_it() {
    let (slow, fast) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[slow, fast]);
    let release_slow = dialer.gate(slow);
    let plan = plan();

    let (_, first) = plan.connect(&dialer, false).unwrap();

    assert_eq!(first, fast);
    assert_eq!(plan.snapshot().pinned, Some(fast));
    assert_eq!(plan.snapshot().races_won, 1);
    drop(release_slow);
    settle(&plan, &[slow]);
    dialer.take_dialled();

    let (_, second) = plan.connect(&dialer, false).unwrap();
    assert_eq!(second, fast);
    assert_eq!(dialer.take_dialled(), vec![fast]);
}

#[test]
fn a_refused_pin_falls_over_to_the_candidate_that_connects_fastest() {
    let (pinned, slower, faster) = (addr(1), addr(2), addr(3));
    let dialer = ScriptedDialer::new(&[pinned, slower, faster]);
    let plan = plan();
    pin(&plan, &dialer, pinned, &[slower, faster]);
    plan.record_connect(slower, Some(Duration::from_millis(80)));
    plan.record_connect(faster, Some(Duration::from_millis(20)));
    dialer.answer(pinned, Answer::Refuse);

    let (_, connected) = plan.connect(&dialer, false).unwrap();

    assert_eq!(connected, faster);
    assert_eq!(dialer.take_dialled(), vec![pinned, faster]);
    // One refusal is not yet a reason to move the pin.
    assert_eq!(plan.snapshot().pinned, Some(pinned));
}

#[test]
fn two_refusals_on_the_pin_race_again_even_while_the_server_is_busy() {
    let (pinned, other) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, other]);
    let plan = plan();
    pin(&plan, &dialer, pinned, &[other]);
    dialer.answer(pinned, Answer::Refuse);

    for _ in 0..SUSPECT_AFTER_FAILURES {
        let (_, connected) = plan.connect(&dialer, false).unwrap();
        assert_eq!(connected, other);
    }
    assert_eq!(plan.snapshot().pinned, Some(pinned));

    let (_, raced) = plan.connect(&dialer, false).unwrap();

    assert_eq!(raced, other);
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.pinned, Some(other));
    assert_eq!(snapshot.races_won, 2);
    assert!(snapshot.repins.contains(&(RaceReason::Suspect, 1)));
}

#[test]
fn an_aged_pin_is_raced_again_only_once_the_server_is_idle() {
    let (pinned, closer) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, closer]);
    let plan = plan();
    pin(&plan, &dialer, pinned, &[closer]);
    plan.age_pin_by(ADDRESS_REPLAN_INTERVAL);

    // Busy: the aged pin is still dialled directly.
    let (_, busy) = plan.connect(&dialer, false).unwrap();
    assert_eq!(busy, pinned);
    assert_eq!(dialer.take_dialled(), vec![pinned]);
    assert_eq!(plan.snapshot().races_won, 1);

    // Idle: the plan races, and this time the other address answers first.
    let hold_pinned = dialer.gate(pinned);
    let (_, idle) = plan.connect(&dialer, true).unwrap();
    drop(hold_pinned);

    assert_eq!(idle, closer);
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.pinned, Some(closer));
    assert!(snapshot.repins.contains(&(RaceReason::Interval, 1)));
}

#[test]
fn a_fresh_pin_is_not_raced_again_while_idle() {
    let (pinned, other) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, other]);
    let plan = plan();
    pin(&plan, &dialer, pinned, &[other]);

    let (_, connected) = plan.connect(&dialer, true).unwrap();

    assert_eq!(connected, pinned);
    assert_eq!(dialer.take_dialled(), vec![pinned]);
    assert_eq!(plan.snapshot().races_won, 1);
}

#[test]
fn a_cleared_over_limit_holdoff_races_again_once_idle() {
    let (pinned, other) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, other]);
    let plan = plan();
    pin(&plan, &dialer, pinned, &[other]);
    plan.note_over_limit_cleared();

    plan.connect(&dialer, false).unwrap();
    assert_eq!(plan.snapshot().races_won, 1);

    let hold_pinned = dialer.gate(pinned);
    let (_, raced) = plan.connect(&dialer, true).unwrap();
    drop(hold_pinned);

    assert_eq!(raced, other);
    assert!(
        plan.snapshot()
            .repins
            .contains(&(RaceReason::OverLimitCleared, 1))
    );
}

#[test]
fn a_resolve_that_answers_nothing_keeps_the_last_candidates() {
    let (pinned, other) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, other]);
    let plan = plan();
    pin(&plan, &dialer, pinned, &[other]);
    plan.age_pin_by(ADDRESS_REPLAN_INTERVAL);
    dialer.resolve_to_nothing();

    let hold_pinned = dialer.gate(pinned);
    let (_, raced) = plan.connect(&dialer, true).unwrap();
    drop(hold_pinned);

    assert_eq!(raced, other);
    let candidates: Vec<_> = plan
        .snapshot()
        .addresses
        .iter()
        .map(|address| address.address)
        .collect();
    assert_eq!(candidates, vec![pinned, other]);
}

#[test]
fn a_first_race_with_nothing_resolved_fails_the_connect() {
    let dialer = ScriptedDialer::new(&[]);
    dialer.resolve_to_nothing();
    let plan = plan();

    let error = plan.connect(&dialer, false).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::AddrNotAvailable);
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.pinned, None);
    assert_eq!(snapshot.races_failed, 1);
}

#[test]
fn a_connect_that_arrives_during_a_race_dials_the_address_the_race_pins() {
    let (slow, fast) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[slow, fast]);
    let release_slow = dialer.gate(slow);
    let release_fast = dialer.gate(fast);
    let plan = plan();

    let racer = {
        let (plan, dialer) = (Arc::clone(&plan), Arc::clone(&dialer));
        std::thread::spawn(move || plan.connect(&dialer, false).unwrap().1)
    };
    // The race is under way once the plan says so; a second caller now has
    // to wait for it rather than start its own.
    while !plan.state().racing {
        std::thread::yield_now();
    }
    let waiter = {
        let (plan, dialer) = (Arc::clone(&plan), Arc::clone(&dialer));
        std::thread::spawn(move || plan.connect(&dialer, false).unwrap().1)
    };
    release_fast.send(()).unwrap();

    assert_eq!(racer.join().unwrap(), fast);
    assert_eq!(waiter.join().unwrap(), fast);
    drop(release_slow);
    assert_eq!(plan.snapshot().races_won, 1);
}

#[test]
fn body_latency_is_kept_only_for_known_addresses() {
    let known = addr(1);
    let dialer = ScriptedDialer::new(&[known]);
    let plan = plan();
    plan.connect(&dialer, false).unwrap();

    plan.record_body_latency(known.ip(), Duration::from_millis(40));
    plan.record_body_latency(addr(9).ip(), Duration::from_millis(40));

    let snapshot = plan.snapshot();
    assert_eq!(snapshot.addresses.len(), 1);
    assert_eq!(
        snapshot.addresses[0].body_latency,
        Some(Duration::from_millis(40))
    );
}
