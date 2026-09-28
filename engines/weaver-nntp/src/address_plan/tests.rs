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
    /// Fail once with this error kind; the next dial connects.
    Fail(io::ErrorKind),
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
            Some(Answer::Fail(kind)) => return Err(io::Error::from(kind)),
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

/// Wait until every race thread has finished with `dialer`, which each does
/// only after booking its attempt.
fn settle_all(dialer: &Arc<ScriptedDialer>) {
    while Arc::strong_count(dialer) > 1 {
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
    let (_, pinned) = plan.connect(dialer).unwrap();
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

    let (_, first) = plan.connect(&dialer).unwrap();

    assert_eq!(first, fast);
    assert_eq!(plan.snapshot().pinned, Some(fast));
    assert_eq!(plan.snapshot().races_won, 1);
    drop(release_slow);
    settle(&plan, &[slow]);
    dialer.take_dialled();

    let (_, second) = plan.connect(&dialer).unwrap();
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

    let (_, connected) = plan.connect(&dialer).unwrap();

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
        let (_, connected) = plan.connect(&dialer).unwrap();
        assert_eq!(connected, other);
    }
    assert_eq!(plan.snapshot().pinned, Some(pinned));

    let (_, raced) = plan.connect(&dialer).unwrap();

    assert_eq!(raced, other);
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.pinned, Some(other));
    assert_eq!(snapshot.races_won, 2);
    assert!(snapshot.repins.contains(&(RaceReason::Suspect, 1)));
}

#[test]
fn an_aged_pin_is_raced_again_once_per_interval() {
    let (pinned, closer) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, closer]);
    let plan = plan();
    pin(&plan, &dialer, pinned, &[closer]);
    plan.age_pin_by(ADDRESS_REPLAN_INTERVAL);

    // The next connect races, and this time the other address answers first.
    let hold_pinned = dialer.gate(pinned);
    let (_, raced) = plan.connect(&dialer).unwrap();
    drop(hold_pinned);
    settle_all(&dialer);
    dialer.take_dialled();

    assert_eq!(raced, closer);
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.pinned, Some(closer));
    assert_eq!(snapshot.races_won, 2);
    assert!(snapshot.repins.contains(&(RaceReason::Interval, 1)));

    // The race restarted the pin's age, so the connect after it dials the
    // new pin directly.
    let (_, next) = plan.connect(&dialer).unwrap();
    assert_eq!(next, closer);
    assert_eq!(dialer.take_dialled(), vec![closer]);
    assert_eq!(plan.snapshot().races_won, 2);
}

#[test]
fn a_fresh_pin_is_not_raced_again_while_idle() {
    let (pinned, other) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, other]);
    let plan = plan();
    pin(&plan, &dialer, pinned, &[other]);

    let (_, connected) = plan.connect(&dialer).unwrap();

    assert_eq!(connected, pinned);
    assert_eq!(dialer.take_dialled(), vec![pinned]);
    assert_eq!(plan.snapshot().races_won, 1);
}

#[test]
fn a_cleared_over_limit_holdoff_races_on_the_next_connect() {
    let (pinned, other) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, other]);
    let plan = plan();
    pin(&plan, &dialer, pinned, &[other]);
    plan.note_over_limit_cleared();

    let hold_pinned = dialer.gate(pinned);
    let (_, raced) = plan.connect(&dialer).unwrap();
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
    let (_, raced) = plan.connect(&dialer).unwrap();
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

    let error = plan.connect(&dialer).unwrap_err();

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
        std::thread::spawn(move || plan.connect(&dialer).unwrap().1)
    };
    // The race is under way once the plan says so; a second caller now has
    // to wait for it rather than start its own.
    while !plan.state().racing {
        std::thread::yield_now();
    }
    let waiter = {
        let (plan, dialer) = (Arc::clone(&plan), Arc::clone(&dialer));
        std::thread::spawn(move || plan.connect(&dialer).unwrap().1)
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
    plan.connect(&dialer).unwrap();

    plan.record_body_latency(known.ip(), Duration::from_millis(40));
    plan.record_body_latency(addr(9).ip(), Duration::from_millis(40));

    let snapshot = plan.snapshot();
    assert_eq!(snapshot.addresses.len(), 1);
    assert_eq!(
        snapshot.addresses[0].body_latency,
        Some(Duration::from_millis(40))
    );
}

/// Book `samples` warm fetches of one megabyte each against `address`, each
/// taking `wire`.
fn deliver(plan: &AddressPlan, address: SocketAddr, samples: u32, wire: Duration) {
    for _ in 0..samples {
        plan.record_delivery(address.ip(), 1_000_000, wire);
    }
}

fn delivery_samples(plan: &AddressPlan, address: SocketAddr) -> u32 {
    plan.snapshot()
        .addresses
        .iter()
        .find(|candidate| candidate.address == address)
        .map_or(0, |candidate| candidate.delivery_samples)
}

#[test]
fn delivery_is_kept_only_for_known_addresses_and_only_when_bytes_moved() {
    let known = addr(1);
    let dialer = ScriptedDialer::new(&[known]);
    let plan = plan();
    plan.connect(&dialer).unwrap();

    plan.record_delivery(known.ip(), 500_000, Duration::from_millis(50));
    plan.record_delivery(known.ip(), 0, Duration::from_millis(50));
    plan.record_delivery(known.ip(), 500_000, Duration::ZERO);
    plan.record_delivery(addr(9).ip(), 500_000, Duration::from_millis(50));

    let snapshot = plan.snapshot();
    assert_eq!(snapshot.addresses.len(), 1);
    assert_eq!(snapshot.addresses[0].delivery_samples, 1);
    assert_eq!(
        snapshot.addresses[0].delivery_bytes_per_second,
        Some(10_000_000.0)
    );
}

#[test]
fn a_reconnect_is_pointed_at_a_challenger_once_the_pin_is_measured() {
    let (pinned, challenger, refusing) = (addr(1), addr(2), addr(3));
    let dialer = ScriptedDialer::new(&[pinned, challenger, refusing]);
    let plan = plan();
    pin(&plan, &dialer, pinned, &[challenger, refusing]);
    plan.record_connect(refusing, None);
    deliver(
        &plan,
        pinned,
        DELIVERY_MIN_SAMPLES,
        Duration::from_millis(100),
    );

    // Too young a pin: every reconnect still dials the pin.
    for _ in 0..SHADOW_EVERY_CONNECTS {
        let (_, connected) = plan.connect(&dialer).unwrap();
        assert_eq!(connected, pinned);
    }
    assert_eq!(
        dialer.take_dialled(),
        vec![pinned; SHADOW_EVERY_CONNECTS as usize]
    );

    // Old enough: the reconnect that completes the count goes to the
    // challenger that has never refused, and the ones before it to the pin.
    plan.age_pin_by(SHADOW_MIN_PIN_AGE);
    let (_, shadowed) = plan.connect(&dialer).unwrap();
    assert_eq!(shadowed, challenger);
    assert_eq!(dialer.take_dialled(), vec![challenger]);
    assert_eq!(plan.snapshot().pinned, Some(pinned));

    // The next reconnects go back to the pin until both the count and the
    // interval allow another.
    for _ in 0..SHADOW_EVERY_CONNECTS {
        let (_, connected) = plan.connect(&dialer).unwrap();
        assert_eq!(connected, pinned);
    }
    dialer.take_dialled();
    plan.age_last_shadow_by(SHADOW_INTERVAL);
    let (_, shadowed) = plan.connect(&dialer).unwrap();
    assert_eq!(shadowed, challenger);
}

#[test]
fn a_measured_challenger_is_not_shadowed_again_before_the_pin_is_judged() {
    let (pinned, challenger) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, challenger]);
    let plan = plan();
    pin(&plan, &dialer, pinned, &[challenger]);
    deliver(
        &plan,
        pinned,
        DELIVERY_MIN_SAMPLES,
        Duration::from_millis(100),
    );
    deliver(
        &plan,
        challenger,
        DELIVERY_MIN_SAMPLES,
        Duration::from_millis(100),
    );
    plan.age_pin_by(SHADOW_MIN_PIN_AGE);

    for _ in 0..SHADOW_EVERY_CONNECTS {
        let (_, connected) = plan.connect(&dialer).unwrap();
        assert_eq!(connected, pinned);
    }
    assert_eq!(
        dialer.take_dialled(),
        vec![pinned; SHADOW_EVERY_CONNECTS as usize]
    );
}

#[test]
fn a_challenger_that_delivered_faster_takes_the_pin_at_the_interval_without_a_race() {
    let (pinned, challenger) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, challenger]);
    let plan = plan();
    pin(&plan, &dialer, pinned, &[challenger]);
    deliver(
        &plan,
        pinned,
        DELIVERY_MIN_SAMPLES,
        Duration::from_millis(100),
    );
    deliver(
        &plan,
        challenger,
        DELIVERY_MIN_SAMPLES,
        Duration::from_millis(80),
    );
    plan.age_pin_by(ADDRESS_REPLAN_INTERVAL);

    let (_, connected) = plan.connect(&dialer).unwrap();

    assert_eq!(connected, challenger);
    assert_eq!(dialer.take_dialled(), vec![challenger]);
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.pinned, Some(challenger));
    assert_eq!(snapshot.races_won, 1);
    assert!(snapshot.repins.contains(&(RaceReason::Delivery, 1)));
    // The verdict starts the next judgment from nothing.
    assert_eq!(delivery_samples(&plan, pinned), 0);
    assert_eq!(delivery_samples(&plan, challenger), 0);
}

#[test]
fn a_pin_its_challengers_did_not_beat_is_kept_at_the_interval_without_a_race() {
    let (pinned, challenger) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, challenger]);
    let plan = plan();
    pin(&plan, &dialer, pinned, &[challenger]);
    deliver(
        &plan,
        pinned,
        DELIVERY_MIN_SAMPLES,
        Duration::from_millis(100),
    );
    deliver(
        &plan,
        challenger,
        DELIVERY_MIN_SAMPLES,
        Duration::from_millis(90),
    );
    plan.age_pin_by(ADDRESS_REPLAN_INTERVAL);

    let (_, connected) = plan.connect(&dialer).unwrap();

    assert_eq!(connected, pinned);
    assert_eq!(dialer.take_dialled(), vec![pinned]);
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.pinned, Some(pinned));
    assert_eq!(snapshot.races_won, 1);
    assert!(snapshot.repins.iter().all(|(_, count)| *count == 0));
    assert_eq!(delivery_samples(&plan, pinned), 0);

    // The verdict restarted the pin's age: the next connect neither races
    // nor judges again.
    let (_, connected) = plan.connect(&dialer).unwrap();
    assert_eq!(connected, pinned);
    assert_eq!(dialer.take_dialled(), vec![pinned]);
    assert_eq!(plan.snapshot().races_won, 1);
}

#[test]
fn an_aged_pin_with_no_measured_challenger_races_as_before() {
    let (pinned, closer) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, closer]);
    let plan = plan();
    pin(&plan, &dialer, pinned, &[closer]);
    deliver(
        &plan,
        pinned,
        DELIVERY_MIN_SAMPLES,
        Duration::from_millis(100),
    );
    deliver(
        &plan,
        closer,
        DELIVERY_MIN_SAMPLES - 1,
        Duration::from_millis(10),
    );
    plan.age_pin_by(ADDRESS_REPLAN_INTERVAL);

    let hold_pinned = dialer.gate(pinned);
    let (_, raced) = plan.connect(&dialer).unwrap();
    drop(hold_pinned);
    settle_all(&dialer);

    assert_eq!(raced, closer);
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.races_won, 2);
    assert!(snapshot.repins.contains(&(RaceReason::Interval, 1)));
    // A race, too, starts the next judgment from nothing.
    assert_eq!(delivery_samples(&plan, closer), 0);
}

#[test]
fn a_failed_first_race_holds_further_races_off_for_a_while() {
    let (first, second) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[first, second]);
    dialer.answer(first, Answer::Refuse);
    dialer.answer(second, Answer::Refuse);
    let plan = plan();

    plan.connect(&dialer).unwrap_err();
    settle_all(&dialer);
    assert_eq!(plan.snapshot().races_failed, 1);
    dialer.take_dialled();

    // Within the holdoff the known candidates are dialled one after another,
    // and no race runs.
    plan.connect(&dialer).unwrap_err();
    let dialled = dialer.take_dialled();
    assert_eq!(dialled.len(), 2, "{dialled:?}");
    assert!(dialled.contains(&first) && dialled.contains(&second));
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.races_failed, 1);
    assert_eq!(snapshot.races_won, 0);

    // Once the holdoff has passed, the next connect races again.
    plan.age_failed_race_by(FAILED_RACE_HOLDOFF);
    dialer.answer(second, Answer::ConnectWhenReleased(channel().1));
    let (_, raced) = plan.connect(&dialer).unwrap();
    assert_eq!(raced, second);
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.races_won, 1);
    assert_eq!(snapshot.pinned, Some(second));
}

#[test]
fn a_failed_first_race_with_nothing_resolved_fails_later_connects_without_dialling() {
    let dialer = ScriptedDialer::new(&[]);
    dialer.resolve_to_nothing();
    let plan = plan();
    plan.connect(&dialer).unwrap_err();

    let error = plan.connect(&dialer).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::AddrNotAvailable);
    assert!(dialer.take_dialled().is_empty());
    assert_eq!(plan.snapshot().races_failed, 1);
}

#[test]
fn a_race_loser_that_timed_out_is_not_booked_as_a_failure() {
    let (winner, slow, refused) = (addr(1), addr(2), addr(3));
    let dialer = ScriptedDialer::new(&[winner, slow, refused]);
    dialer.answer(slow, Answer::Fail(io::ErrorKind::TimedOut));
    dialer.answer(refused, Answer::Refuse);
    let plan = plan();

    let (_, pinned) = plan.connect(&dialer).unwrap();
    assert_eq!(pinned, winner);
    settle_all(&dialer);
    dialer.take_dialled();

    let failures = |address: SocketAddr| {
        plan.snapshot()
            .addresses
            .iter()
            .find(|candidate| candidate.address == address)
            .map(|candidate| candidate.consecutive_failures)
    };
    assert_eq!(failures(slow), Some(0));
    assert_eq!(failures(refused), Some(1));

    // On failover the slow loser is tried before the address that refused.
    dialer.answer(winner, Answer::Refuse);
    let (_, connected) = plan.connect(&dialer).unwrap();
    assert_eq!(connected, slow);
    assert_eq!(dialer.take_dialled(), vec![winner, slow]);
}

#[test]
fn a_race_in_which_every_address_timed_out_books_the_timeouts() {
    let (first, second) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[first, second]);
    dialer.answer(first, Answer::Fail(io::ErrorKind::TimedOut));
    dialer.answer(second, Answer::Fail(io::ErrorKind::TimedOut));
    let plan = plan();

    let error = plan.connect(&dialer).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(
        plan.snapshot()
            .addresses
            .iter()
            .all(|address| address.consecutive_failures == 1)
    );
}
