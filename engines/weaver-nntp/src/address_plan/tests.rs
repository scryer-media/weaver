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
fn every_resolved_address_is_raced_however_many_there_are() {
    let addresses: Vec<_> = (1..=40).map(addr).collect();
    let (answers, refusing) = addresses.split_last().unwrap();
    let dialer = ScriptedDialer::new(&addresses);
    for &address in refusing {
        dialer.answer(address, Answer::Refuse);
    }
    let plan = plan();

    let (_, connected) = plan.connect(&dialer).unwrap();

    assert_eq!(connected, *answers);
    settle_all(&dialer);
    let dialled = dialer.take_dialled();
    assert_eq!(dialled.len(), addresses.len());
    assert!(addresses.iter().all(|address| dialled.contains(address)));
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.pinned, Some(connected));
    assert_eq!(
        snapshot
            .addresses
            .iter()
            .map(|candidate| candidate.address)
            .collect::<Vec<_>>(),
        addresses
    );
}

#[test]
fn addresses_beyond_one_batch_are_raced_in_the_next() {
    let addresses: Vec<_> = (0..MAX_RACE_CANDIDATES + 2)
        .map(|index| {
            SocketAddr::from(([192, 0, (index / 250) as u8, (index % 250) as u8 + 1], 563))
        })
        .collect();
    let (slow, winner) = (
        addresses[MAX_RACE_CANDIDATES],
        addresses[MAX_RACE_CANDIDATES + 1],
    );
    let dialer = ScriptedDialer::new(&addresses);
    for &address in addresses.iter().take(MAX_RACE_CANDIDATES) {
        dialer.answer(address, Answer::Refuse);
    }
    dialer.answer(slow, Answer::Fail(io::ErrorKind::TimedOut));
    let plan = plan();

    let (_, pinned) = plan.connect(&dialer).unwrap();

    assert_eq!(pinned, winner);
    settle_all(&dialer);
    assert_eq!(dialer.take_dialled().len(), addresses.len());
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.addresses.len(), addresses.len());
    let failures = |address: SocketAddr| {
        snapshot
            .addresses
            .iter()
            .find(|candidate| candidate.address == address)
            .map(|candidate| candidate.consecutive_failures)
    };
    // The timeout lost to the winner of its own batch, so it is not a failure.
    assert_eq!(failures(slow), Some(0));
    assert_eq!(failures(addresses[0]), Some(1));
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

    assert_eq!(error.kind(), io::ErrorKind::Other);
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

/// Wire time of one fetch on the pin in the delivery tests. Enough fetches to
/// be [`DELIVERY_MIN_SAMPLES`] also add up to [`DELIVERY_MIN_WIRE`].
const PIN_WIRE: Duration = Duration::from_millis(1000);

/// A challenger 25% faster than [`PIN_WIRE`], past [`DELIVERY_REPIN_RATIO`].
const FASTER_WIRE: Duration = Duration::from_millis(800);

/// A challenger about 11% faster than [`PIN_WIRE`], short of
/// [`DELIVERY_REPIN_RATIO`].
const SLIGHTLY_FASTER_WIRE: Duration = Duration::from_millis(900);

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

/// Pin `winner`, then stop the plan's clock so every later age is the
/// test's to set.
fn pin_and_freeze(
    plan: &Arc<AddressPlan>,
    dialer: &Arc<ScriptedDialer>,
    winner: SocketAddr,
    others: &[SocketAddr],
) {
    pin(plan, dialer, winner, others);
    plan.freeze_clock();
}

/// `count` connects that must all dial `expected` and nothing else.
fn connects_all_dial(
    plan: &Arc<AddressPlan>,
    dialer: &Arc<ScriptedDialer>,
    count: u32,
    expected: SocketAddr,
) {
    for _ in 0..count {
        let (_, connected) = plan.connect(dialer).unwrap();
        assert_eq!(connected, expected);
    }
    assert_eq!(dialer.take_dialled(), vec![expected; count as usize]);
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
    let (pinned, challenger, refusing, unconnected) = (addr(1), addr(2), addr(3), addr(4));
    let dialer = ScriptedDialer::new(&[pinned, challenger, refusing, unconnected]);
    let plan = plan();
    // The race pins `pinned`; `challenger` and `refusing` lose and are
    // booked with a connect time; `unconnected` only ever timed out.
    dialer.answer(unconnected, Answer::Fail(io::ErrorKind::TimedOut));
    let gates: Vec<_> = [challenger, refusing]
        .iter()
        .map(|other| dialer.gate(*other))
        .collect();
    let (_, first) = plan.connect(&dialer).unwrap();
    assert_eq!(first, pinned);
    drop(gates);
    settle_all(&dialer);
    dialer.take_dialled();
    plan.freeze_clock();
    plan.record_connect(refusing, None);
    deliver(&plan, pinned, DELIVERY_MIN_SAMPLES, PIN_WIRE);

    // Too young a pin: every reconnect still dials the pin.
    connects_all_dial(&plan, &dialer, SHADOW_EVERY_CONNECTS, pinned);

    // Old enough: the reconnect that completes the count goes to the one
    // challenger that has connected before and never refused.
    plan.advance(SHADOW_MIN_PIN_AGE);
    let (_, shadowed) = plan.connect(&dialer).unwrap();
    assert_eq!(shadowed, challenger);
    assert_eq!(dialer.take_dialled(), vec![challenger]);
    assert_eq!(plan.snapshot().pinned, Some(pinned));

    // The count alone does not open the next one while the interval has
    // not passed.
    connects_all_dial(&plan, &dialer, SHADOW_EVERY_CONNECTS + 1, pinned);

    // Once it has, the count is already complete and the next reconnect
    // goes to the challenger.
    plan.advance(SHADOW_INTERVAL);
    let (_, shadowed) = plan.connect(&dialer).unwrap();
    assert_eq!(shadowed, challenger);
}

#[test]
fn a_measured_challenger_is_not_shadowed_again_before_the_pin_is_judged() {
    let (pinned, challenger) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, challenger]);
    let plan = plan();
    pin_and_freeze(&plan, &dialer, pinned, &[challenger]);
    deliver(&plan, pinned, DELIVERY_MIN_SAMPLES, PIN_WIRE);
    deliver(&plan, challenger, DELIVERY_MIN_SAMPLES, PIN_WIRE);
    plan.advance(SHADOW_MIN_PIN_AGE);

    connects_all_dial(&plan, &dialer, SHADOW_EVERY_CONNECTS, pinned);
}

/// Let the next verdict come due and deliver the evidence it judges.
fn verdict_with(
    plan: &AddressPlan,
    pinned: SocketAddr,
    pin_wire: Duration,
    challenger: SocketAddr,
    challenger_wire: Duration,
) {
    plan.advance(DELIVERY_VERDICT_INTERVAL);
    deliver(plan, pinned, DELIVERY_MIN_SAMPLES, pin_wire);
    deliver(plan, challenger, DELIVERY_MIN_SAMPLES, challenger_wire);
}

fn repins(plan: &AddressPlan, reason: RaceReason) -> u64 {
    plan.snapshot()
        .repins
        .iter()
        .find(|(counted, _)| *counted == reason)
        .map_or(0, |(_, count)| *count)
}

#[test]
fn a_challenger_that_delivered_faster_at_two_verdicts_takes_the_pin_without_a_race() {
    let (pinned, challenger) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, challenger]);
    let plan = plan();
    pin_and_freeze(&plan, &dialer, pinned, &[challenger]);

    // First verdict: the challenger leads, but one verdict is not enough.
    verdict_with(&plan, pinned, PIN_WIRE, challenger, FASTER_WIRE);
    let (_, connected) = plan.connect(&dialer).unwrap();
    assert_eq!(connected, pinned);
    assert_eq!(dialer.take_dialled(), vec![pinned]);
    assert_eq!(plan.snapshot().pinned, Some(pinned));
    // The verdict starts the next judgment from nothing.
    assert_eq!(delivery_samples(&plan, pinned), 0);
    assert_eq!(delivery_samples(&plan, challenger), 0);

    // Second verdict, same leader: the pin moves, and no race ran.
    verdict_with(&plan, pinned, PIN_WIRE, challenger, FASTER_WIRE);
    let (_, connected) = plan.connect(&dialer).unwrap();
    assert_eq!(connected, challenger);
    assert_eq!(dialer.take_dialled(), vec![challenger]);
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.pinned, Some(challenger));
    assert_eq!(snapshot.races_won, 1);
    assert_eq!(repins(&plan, RaceReason::Delivery), 1);
    assert_eq!(repins(&plan, RaceReason::Interval), 0);
}

#[test]
fn a_busy_server_moves_the_pin_on_delivery_long_before_the_age_race() {
    let (pinned, challenger) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, challenger]);
    let plan = plan();
    pin_and_freeze(&plan, &dialer, pinned, &[challenger]);
    let mut elapsed = Duration::ZERO;

    // The pin's connections measure it; once it is old enough, a reconnect
    // is pointed at the challenger, whose connection measures it in turn.
    deliver(&plan, pinned, DELIVERY_MIN_SAMPLES, PIN_WIRE);
    plan.advance(SHADOW_MIN_PIN_AGE);
    elapsed += SHADOW_MIN_PIN_AGE;
    connects_all_dial(&plan, &dialer, SHADOW_EVERY_CONNECTS - 1, pinned);
    let (_, shadowed) = plan.connect(&dialer).unwrap();
    assert_eq!(shadowed, challenger);
    dialer.take_dialled();
    deliver(&plan, challenger, DELIVERY_MIN_SAMPLES, FASTER_WIRE);

    // First verdict: the challenger leads.
    plan.advance(DELIVERY_VERDICT_INTERVAL - SHADOW_MIN_PIN_AGE);
    elapsed += DELIVERY_VERDICT_INTERVAL - SHADOW_MIN_PIN_AGE;
    let (_, connected) = plan.connect(&dialer).unwrap();
    assert_eq!(connected, pinned);
    dialer.take_dialled();
    assert_eq!(delivery_samples(&plan, challenger), 0);

    // The verdict emptied the challenger's samples, so once the pin is
    // measured again the challenger is shadowed again, without waiting for
    // anything but the usual count.
    deliver(&plan, pinned, DELIVERY_MIN_SAMPLES, PIN_WIRE);
    connects_all_dial(&plan, &dialer, SHADOW_EVERY_CONNECTS - 2, pinned);
    let (_, shadowed) = plan.connect(&dialer).unwrap();
    assert_eq!(shadowed, challenger);
    dialer.take_dialled();
    deliver(&plan, challenger, DELIVERY_MIN_SAMPLES, FASTER_WIRE);

    // Second verdict, one verdict interval later: the pin moves.
    plan.advance(DELIVERY_VERDICT_INTERVAL);
    elapsed += DELIVERY_VERDICT_INTERVAL;
    let (_, connected) = plan.connect(&dialer).unwrap();
    assert_eq!(connected, challenger);

    assert_eq!(elapsed, Duration::from_mins(2));
    assert!(elapsed < ADDRESS_REPLAN_INTERVAL);
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.pinned, Some(challenger));
    assert_eq!(snapshot.races_won, 1);
    assert_eq!(repins(&plan, RaceReason::Delivery), 1);
    assert_eq!(repins(&plan, RaceReason::Interval), 0);
}

#[test]
fn a_challenger_short_of_the_least_wire_time_is_not_measured() {
    let (pinned, challenger) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, challenger]);
    let plan = plan();
    pin_and_freeze(&plan, &dialer, pinned, &[challenger]);
    // Enough small fetches, ten times the pin's rate, but far short of the
    // wire time that makes them evidence.
    let small = Duration::from_millis(100);
    deliver(&plan, pinned, DELIVERY_MIN_SAMPLES, PIN_WIRE);
    deliver(&plan, challenger, DELIVERY_MIN_SAMPLES, small);

    for _ in 0..2 {
        plan.advance(DELIVERY_VERDICT_INTERVAL);
        connects_all_dial(&plan, &dialer, 1, pinned);
        // No verdict ran: the samples are still gathering.
        assert_eq!(delivery_samples(&plan, challenger), DELIVERY_MIN_SAMPLES);
        assert_eq!(delivery_samples(&plan, pinned), DELIVERY_MIN_SAMPLES);
    }

    // The same fetches, until they add up to the least wire time: now the
    // challenger is measured, and a verdict runs.
    let short = DELIVERY_MIN_WIRE - small * DELIVERY_MIN_SAMPLES;
    deliver(
        &plan,
        challenger,
        (short.as_millis() / small.as_millis()) as u32,
        small,
    );
    connects_all_dial(&plan, &dialer, 1, pinned);
    assert_eq!(delivery_samples(&plan, challenger), 0);
    // One verdict only makes the challenger the leader.
    assert_eq!(plan.snapshot().pinned, Some(pinned));
}

#[test]
fn a_verdict_does_not_restart_the_pins_age() {
    let (pinned, challenger) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, challenger]);
    let plan = plan();
    pin_and_freeze(&plan, &dialer, pinned, &[challenger]);
    verdict_with(&plan, pinned, PIN_WIRE, challenger, FASTER_WIRE);
    plan.connect(&dialer).unwrap();
    verdict_with(&plan, pinned, PIN_WIRE, challenger, FASTER_WIRE);
    plan.connect(&dialer).unwrap();
    assert_eq!(plan.snapshot().pinned, Some(challenger));
    dialer.take_dialled();

    // Just short of the interval from the first pin, the new pin is dialled.
    plan.advance(ADDRESS_REPLAN_INTERVAL - DELIVERY_VERDICT_INTERVAL * 2 - Duration::from_secs(1));
    connects_all_dial(&plan, &dialer, 1, challenger);
    assert_eq!(plan.snapshot().races_won, 1);

    // At the interval the age race runs, and this time the first pin answers
    // first.
    plan.advance(Duration::from_secs(1));
    let hold_challenger = dialer.gate(challenger);
    let (_, raced) = plan.connect(&dialer).unwrap();
    drop(hold_challenger);
    settle_all(&dialer);

    assert_eq!(raced, pinned);
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.pinned, Some(pinned));
    assert_eq!(snapshot.races_won, 2);
    assert_eq!(repins(&plan, RaceReason::Interval), 1);
    assert_eq!(repins(&plan, RaceReason::Delivery), 1);
}

#[test]
fn a_lead_that_does_not_hold_needs_two_fresh_verdicts_to_move_the_pin() {
    let (pinned, challenger) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, challenger]);
    let plan = plan();
    pin_and_freeze(&plan, &dialer, pinned, &[challenger]);

    verdict_with(&plan, pinned, PIN_WIRE, challenger, FASTER_WIRE);
    plan.connect(&dialer).unwrap();
    // The challenger falls back to parity; the lead is forfeit.
    verdict_with(&plan, pinned, PIN_WIRE, challenger, PIN_WIRE);
    plan.connect(&dialer).unwrap();
    // Leading again only starts a new confirmation.
    verdict_with(&plan, pinned, PIN_WIRE, challenger, FASTER_WIRE);
    let (_, connected) = plan.connect(&dialer).unwrap();

    assert_eq!(connected, pinned);
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.pinned, Some(pinned));
    assert_eq!(snapshot.races_won, 1);
    assert!(snapshot.repins.iter().all(|(_, count)| *count == 0));

    // The confirmation: the pin moves.
    verdict_with(&plan, pinned, PIN_WIRE, challenger, FASTER_WIRE);
    let (_, connected) = plan.connect(&dialer).unwrap();
    assert_eq!(connected, challenger);
    assert_eq!(plan.snapshot().pinned, Some(challenger));
    assert_eq!(repins(&plan, RaceReason::Delivery), 1);
}

#[test]
fn verdicts_are_no_closer_than_the_verdict_interval() {
    let (pinned, challenger) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, challenger]);
    let plan = plan();
    pin_and_freeze(&plan, &dialer, pinned, &[challenger]);
    let just_short = DELIVERY_VERDICT_INTERVAL - Duration::from_secs(1);

    // Both sides are measured at once, but the pin is too young to judge.
    deliver(&plan, pinned, DELIVERY_MIN_SAMPLES, PIN_WIRE);
    deliver(&plan, challenger, DELIVERY_MIN_SAMPLES, FASTER_WIRE);
    connects_all_dial(&plan, &dialer, 1, pinned);
    plan.advance(just_short);
    connects_all_dial(&plan, &dialer, 1, pinned);
    assert_eq!(delivery_samples(&plan, challenger), DELIVERY_MIN_SAMPLES);
    plan.advance(Duration::from_secs(1));
    connects_all_dial(&plan, &dialer, 1, pinned);
    assert_eq!(delivery_samples(&plan, challenger), 0);

    // Measured again straight after the first verdict: the second waits a
    // whole interval from the first.
    deliver(&plan, pinned, DELIVERY_MIN_SAMPLES, PIN_WIRE);
    deliver(&plan, challenger, DELIVERY_MIN_SAMPLES, FASTER_WIRE);
    connects_all_dial(&plan, &dialer, 1, pinned);
    plan.advance(just_short);
    connects_all_dial(&plan, &dialer, 1, pinned);
    assert_eq!(delivery_samples(&plan, challenger), DELIVERY_MIN_SAMPLES);
    assert_eq!(plan.snapshot().pinned, Some(pinned));

    plan.advance(Duration::from_secs(1));
    let (_, connected) = plan.connect(&dialer).unwrap();
    assert_eq!(connected, challenger);
    assert_eq!(repins(&plan, RaceReason::Delivery), 1);
}

#[test]
fn a_pin_its_challengers_did_not_beat_is_kept_without_a_race() {
    let (pinned, challenger) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, challenger]);
    let plan = plan();
    pin_and_freeze(&plan, &dialer, pinned, &[challenger]);
    verdict_with(&plan, pinned, PIN_WIRE, challenger, SLIGHTLY_FASTER_WIRE);

    let (_, connected) = plan.connect(&dialer).unwrap();

    assert_eq!(connected, pinned);
    assert_eq!(dialer.take_dialled(), vec![pinned]);
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.pinned, Some(pinned));
    assert_eq!(snapshot.races_won, 1);
    assert!(snapshot.repins.iter().all(|(_, count)| *count == 0));
    assert_eq!(delivery_samples(&plan, pinned), 0);
}

#[test]
fn a_challenger_that_is_refusing_connections_cannot_take_the_pin() {
    let (pinned, challenger) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, challenger]);
    let plan = plan();
    pin_and_freeze(&plan, &dialer, pinned, &[challenger]);
    // The challenger delivered fast on the connections it already had, and
    // has since started refusing new ones.
    dialer.answer(challenger, Answer::Refuse);
    for _ in 0..2 {
        verdict_with(&plan, pinned, PIN_WIRE, challenger, FASTER_WIRE);
        plan.record_connect(challenger, None);
        let (_, connected) = plan.connect(&dialer).unwrap();
        assert_eq!(connected, pinned);
    }

    // With the only challenger out of the running the pin is judged against
    // nothing, so it holds, and nothing raced.
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.pinned, Some(pinned));
    assert_eq!(snapshot.races_won, 1);
    assert!(snapshot.repins.iter().all(|(_, count)| *count == 0));
}

#[test]
fn an_aged_pin_with_no_measured_challenger_races_as_before() {
    let (pinned, closer) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, closer]);
    let plan = plan();
    pin_and_freeze(&plan, &dialer, pinned, &[closer]);
    plan.advance(ADDRESS_REPLAN_INTERVAL);
    deliver(&plan, pinned, DELIVERY_MIN_SAMPLES, PIN_WIRE);
    deliver(&plan, closer, DELIVERY_MIN_SAMPLES - 1, FASTER_WIRE);

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
fn an_aged_pin_races_even_with_delivery_evidence_waiting() {
    let (pinned, challenger) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, challenger]);
    let plan = plan();
    pin_and_freeze(&plan, &dialer, pinned, &[challenger]);
    deliver(&plan, pinned, DELIVERY_MIN_SAMPLES, PIN_WIRE);
    deliver(&plan, challenger, DELIVERY_MIN_SAMPLES, FASTER_WIRE);
    // No connect came while the verdict was open, and now the pin is due.
    plan.advance(ADDRESS_REPLAN_INTERVAL);

    let hold_pinned = dialer.gate(pinned);
    let (_, raced) = plan.connect(&dialer).unwrap();
    drop(hold_pinned);
    settle_all(&dialer);

    assert_eq!(raced, challenger);
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.races_won, 2);
    assert_eq!(repins(&plan, RaceReason::Interval), 1);
    assert_eq!(repins(&plan, RaceReason::Delivery), 0);
}

#[test]
fn delivery_verdicts_do_not_hold_off_the_race_that_resolves_the_hostname_again() {
    let (pinned, challenger, added) = (addr(1), addr(2), addr(3));
    let dialer = ScriptedDialer::new(&[pinned, challenger]);
    let plan = plan();
    pin_and_freeze(&plan, &dialer, pinned, &[challenger]);

    // A verdict every interval up to the pin's age, each keeping the pin.
    let verdicts = (ADDRESS_REPLAN_INTERVAL.as_secs() / DELIVERY_VERDICT_INTERVAL.as_secs()) - 1;
    for _ in 0..verdicts {
        verdict_with(&plan, pinned, PIN_WIRE, challenger, PIN_WIRE);
        connects_all_dial(&plan, &dialer, 1, pinned);
        assert_eq!(delivery_samples(&plan, pinned), 0);
    }
    assert_eq!(plan.snapshot().races_won, 1);

    // The provider added an address meanwhile. The pin comes due on age all
    // the same, races, and the race sees it.
    *dialer.resolved.lock().unwrap() = Some(vec![pinned, challenger, added]);
    verdict_with(&plan, pinned, PIN_WIRE, challenger, PIN_WIRE);
    let hold_others = [dialer.gate(challenger), dialer.gate(added)];
    let (_, raced) = plan.connect(&dialer).unwrap();
    drop(hold_others);
    settle_all(&dialer);

    assert_eq!(raced, pinned);
    let snapshot = plan.snapshot();
    assert_eq!(snapshot.races_won, 2);
    let candidates: Vec<_> = snapshot
        .addresses
        .iter()
        .map(|address| address.address)
        .collect();
    assert_eq!(candidates, vec![pinned, challenger, added]);
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

    assert_eq!(error.kind(), io::ErrorKind::Other);
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

/// Pin `pinned` and have two sessions on it connect and never hear from the
/// server.
fn pin_that_never_reaches_the_server(
    plan: &Arc<AddressPlan>,
    dialer: &Arc<ScriptedDialer>,
    pinned: SocketAddr,
    others: &[SocketAddr],
) {
    pin(plan, dialer, pinned, others);
    let route = AddressRoute {
        plan: Arc::clone(plan),
    };
    drop(route.watch_setup(pinned));
    drop(route.watch_setup(pinned));
}

#[test]
fn a_pin_that_connects_but_never_reaches_the_server_gives_way_to_one_that_does() {
    let (deaf, healthy) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[deaf, healthy]);
    let plan = plan();
    pin_that_never_reaches_the_server(&plan, &dialer, deaf, &[healthy]);

    // The deaf address still connects first; the healthy one is held until
    // the test lets it go.
    let release_healthy = dialer.gate(healthy);
    let racer = {
        let (plan, dialer) = (Arc::clone(&plan), Arc::clone(&dialer));
        std::thread::spawn(move || plan.connect(&dialer).unwrap().1)
    };
    while !dialer.dialled.lock().unwrap().contains(&healthy) {
        std::thread::yield_now();
    }
    release_healthy.send(()).unwrap();

    assert_eq!(racer.join().unwrap(), healthy);
    assert_eq!(plan.snapshot().pinned, Some(healthy));
    settle_all(&dialer);
    dialer.take_dialled();
    let (_, next) = plan.connect(&dialer).unwrap();
    assert_eq!(next, healthy);
    assert_eq!(dialer.take_dialled(), vec![healthy]);
}

#[test]
fn a_pin_that_never_reaches_the_server_is_kept_when_nothing_else_connects() {
    let (deaf, refusing) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[deaf, refusing]);
    let plan = plan();
    pin_that_never_reaches_the_server(&plan, &dialer, deaf, &[refusing]);
    dialer.answer(refusing, Answer::Refuse);

    let (_, raced) = plan.connect(&dialer).unwrap();

    assert_eq!(raced, deaf);
    assert_eq!(plan.snapshot().pinned, Some(deaf));
    assert_eq!(plan.snapshot().races_won, 2);
}

#[test]
fn a_session_that_reaches_the_server_clears_the_pins_failed_setups() {
    let (pinned, other) = (addr(1), addr(2));
    let dialer = ScriptedDialer::new(&[pinned, other]);
    let plan = plan();
    pin(&plan, &dialer, pinned, &[other]);
    let route = AddressRoute {
        plan: Arc::clone(&plan),
    };

    drop(route.watch_setup(pinned));
    route.watch_setup(pinned).reached_server();
    drop(route.watch_setup(pinned));
    let (_, next) = plan.connect(&dialer).unwrap();

    assert_eq!(next, pinned);
    assert_eq!(dialer.take_dialled(), vec![pinned]);
    assert_eq!(plan.snapshot().races_won, 1);
}

#[test]
fn unresolved_server_is_destination_evidence_even_without_cached_candidates() {
    let dialer = ScriptedDialer::new(&[]);
    dialer.resolve_to_nothing();
    let plan = plan();
    for _ in 0..2 {
        let error = weaver_tunnel::pipe::DialError::destination(plan.connect(&dialer).unwrap_err());
        assert!(matches!(
            error,
            weaver_tunnel::pipe::DialError::Destination(_)
        ));
        assert!(!error.is_path_evidence());
    }
    assert!(dialer.take_dialled().is_empty());
}
