use super::*;

const MB: f64 = 1_000_000.0;

// A synthetic line the controller runs against. Each connection moves bytes
// from the tick after it opens plus `first_byte` more.
struct Line {
    controller: Controller,
    ticks: u32,
    bytes: u64,
    // Leg and opening tick of every connection still open.
    connections: Vec<(usize, u32)>,
    first_byte: u32,
    opens: Vec<usize>,
    // The leg of every connection opened in a lost one's place.
    replaced: Vec<usize>,
    // Every tick the controller was given.
    seen: Vec<Tick>,
}

impl Line {
    fn new(weights: Vec<u8>, first_byte: u32) -> Self {
        let up = vec![true; weights.len()];
        let mut controller = Controller::new(weights, up);
        let connections = controller
            .initial()
            .into_iter()
            .map(|leg| (leg, 0))
            .collect();
        Self {
            controller,
            ticks: 0,
            bytes: 0,
            connections,
            first_byte,
            opens: Vec::new(),
            replaced: Vec::new(),
            seen: Vec::new(),
        }
    }

    // One tick. `rate` gives the line's bytes per second from the legs of
    // the connections moving bytes, and the time.
    fn step(&mut self, rate: &impl Fn(&[usize], Duration) -> f64) -> Decision {
        self.ticks += 1;
        let at = TICK * self.ticks;
        let moving: Vec<usize> = self
            .connections
            .iter()
            .filter(|(_, opened)| self.ticks > opened + self.first_byte)
            .map(|(leg, _)| *leg)
            .collect();
        let waiting = self.connections.len() - moving.len();
        self.bytes += (rate(&moving, at) * TICK.as_secs_f64()) as u64;
        let tick = Tick {
            at,
            bytes: self.bytes,
            waiting,
        };
        self.seen.push(tick);
        let decision = self.controller.tick(tick);
        if let Decision::Open(leg) = decision {
            self.connections.push((leg, self.ticks));
            self.opens.push(leg);
        }
        decision
    }

    // Tick until the controller stops the run; the time it stopped at.
    fn run(&mut self, rate: impl Fn(&[usize], Duration) -> f64) -> Duration {
        loop {
            assert!(self.ticks < 1000, "the controller never stopped");
            if self.step(&rate) == Decision::Stop {
                return TICK * self.ticks;
            }
        }
    }

    // The oldest connection on `leg` ends on its own; a new one opens in its
    // place when the controller says so, as the runner does.
    fn end_leg(&mut self, leg: usize, replaceable: bool) -> bool {
        let oldest = self
            .connections
            .iter()
            .position(|(on, _)| *on == leg)
            .expect("a connection on the leg");
        self.connections.remove(oldest);
        let replaced = self.controller.ended(leg, replaceable);
        if replaced {
            self.connections.push((leg, self.ticks));
            self.replaced.push(leg);
        }
        replaced
    }

    // Bytes per second since the warm-up ended.
    fn speed(&self) -> f64 {
        let (at, bytes) = self.controller.warm_up().expect("the warm-up ended");
        (self.bytes - bytes) as f64 / (TICK * self.ticks - at).as_secs_f64()
    }
}

fn close(actual: f64, expected: f64, share: f64) -> bool {
    (actual - expected).abs() <= expected * share
}

#[test]
fn a_rising_line_gains_connections_until_one_more_adds_nothing() {
    let mut line = Line::new(vec![100], 2);
    let stopped = line.run(|moving, _| moving.len().min(3) as f64 * 10.0 * MB);

    assert_eq!(
        line.opens,
        vec![0, 0, 0],
        "two that lift the rate and one that does not"
    );
    let (warm_up, _) = line.controller.warm_up().unwrap();
    assert!(stopped >= MIN_MEASURING && stopped > warm_up);
    assert!(stopped < MAX_MEASURING);
    assert!(close(line.speed(), 30.0 * MB, 0.001), "{}", line.speed());
}

#[test]
fn a_flat_line_ends_the_ramp_on_the_first_added_connection() {
    let mut line = Line::new(vec![100], 0);
    let stopped = line.run(|_, _| 50.0 * MB);

    assert_eq!(line.opens, vec![0]);
    assert_eq!(stopped, MIN_MEASURING, "stable long before the minimum");
    assert!(close(line.speed(), 50.0 * MB, 0.001), "{}", line.speed());
}

#[test]
fn a_noisy_but_stable_line_stops_on_its_mean() {
    let noise = |at: Duration| {
        // A fixed scramble of the tick number: within 10% either way.
        let n = (at.as_millis() / TICK.as_millis()) as u64;
        let scrambled = n.wrapping_mul(2_654_435_761) % 201;
        1.0 + (scrambled as f64 - 100.0) / 1000.0
    };
    let mut line = Line::new(vec![100], 1);
    let stopped = line.run(|_, at| 40.0 * MB * noise(at));

    assert!(line.opens.len() <= 2, "noise lifts the rate at most once");
    assert!(stopped >= MIN_MEASURING && stopped < MAX_MEASURING);
    assert!(close(line.speed(), 40.0 * MB, 0.02), "{}", line.speed());
}

#[test]
fn a_settled_mean_over_a_line_still_moving_does_not_stop_the_run() {
    // Flat past the warm-up, then a step up: the running averages close in
    // on each other well before they near the new rate.
    let step = Duration::from_secs(4);
    let mut line = Line::new(vec![100], 0);
    let stopped = line.run(|_, at| if at > step { 61.0 * MB } else { 50.0 * MB });
    let (since, moved) = line.controller.warm_up().unwrap();
    assert!(since < step, "{since:?}");

    // At each tick since the warm-up: whether the last running averages
    // agree, and whether the rate over the same ticks holds to the latest.
    let average = |tick: &Tick| (tick.bytes - moved) as f64 / (tick.at - since).as_secs_f64();
    let ticks: Vec<Tick> = line
        .seen
        .iter()
        .copied()
        .filter(|t| t.at >= since)
        .collect();
    let judged: Vec<(Duration, bool, bool)> = ticks
        .windows(STABLE_TICKS + 1)
        .map(|window| {
            let now = window[STABLE_TICKS];
            let averages: VecDeque<f64> = window[1..].iter().map(average).collect();
            let recent = rate_between(window[0], now);
            (now.at, stable(&averages), steady(recent, average(&now)))
        })
        .collect();
    let converged = judged
        .iter()
        .find(|(at, stable, _)| *at >= MIN_MEASURING && *stable)
        .map(|(at, ..)| *at)
        .unwrap();
    assert!(converged < stopped, "{converged:?} {stopped:?}");
    for (at, stable, steady) in &judged {
        if *at >= converged && *at < stopped {
            assert!(*stable && !*steady, "{at:?}");
        }
    }
    assert!(judged.contains(&(stopped, true, true)), "{stopped:?}");
    assert!(
        close(line.speed(), 61.0 * MB, STEADY_BAND),
        "{}",
        line.speed()
    );
}

#[test]
fn a_line_that_never_settles_stops_at_the_cap() {
    // Still rising when the connections run out: the ramp ends on the first
    // window judged at the cap, and the rise keeps the run to its maximum.
    let mut line = Line::new(vec![100], 0);
    let growth = |at: Duration| 1.02f64.powi((at.as_millis() / TICK.as_millis()) as i32);
    let stopped = line.run(|_, at| 10.0 * MB * growth(at));
    assert_eq!(stopped, MAX_MEASURING);
    assert_eq!(line.opens.len(), MAX_CONNECTIONS - 1);
    let last_open = line.connections.iter().map(|(_, at)| *at).max().unwrap();
    let (warm_up, _) = line
        .controller
        .warm_up()
        .expect("the ramp ended at the cap");
    assert_eq!(warm_up, TICK * (last_open + 1 + RAMP_WINDOW as u32));

    // Past the warm-up, an average that keeps climbing never agrees.
    let mut line = Line::new(vec![100], 0);
    let stopped = line.run(|_, at| {
        let late = at.saturating_sub(Duration::from_secs(3));
        10.0 * MB * 1.03f64.powi((late.as_millis() / TICK.as_millis()) as i32)
    });
    assert_eq!(stopped, MAX_MEASURING);
    assert!(line.controller.warm_up().is_some());
    assert_eq!(
        line.controller.tick(Tick {
            at: MAX_MEASURING + TICK,
            bytes: line.bytes,
            waiting: 0,
        }),
        Decision::Stop,
        "a stopped run stays stopped"
    );
}

#[test]
fn a_leg_that_goes_quiet_gets_no_more_connections() {
    let mut line = Line::new(vec![50, 50], 0);
    let rate = |moving: &[usize], _: Duration| moving.len().min(4) as f64 * 10.0 * MB;
    for _ in 0..3 {
        assert_eq!(line.step(&rate), Decision::Wait);
    }
    // Its egress's quota refuses leg 1's one connection.
    assert!(!line.end_leg(1, false));
    let stopped = line.run(rate);

    assert_eq!(line.opens, vec![0, 0, 0, 0], "all on the leg still up");
    assert!(line.replaced.is_empty());
    assert!(stopped < MAX_MEASURING);
    assert!(close(line.speed(), 40.0 * MB, 0.001), "{}", line.speed());

    // A leg that stops moving bytes after the warm-up drags the average
    // down; the run still stops, on a speed between the two rates.
    let mut line = Line::new(vec![50, 50], 0);
    let stopped = line.run(|moving, at| {
        let quiet = at >= Duration::from_secs(6);
        moving
            .iter()
            .filter(|leg| !(quiet && **leg == 1))
            .count()
            .min(4) as f64
            * 10.0
            * MB
    });
    let (warm_up, _) = line.controller.warm_up().unwrap();
    assert!(warm_up < Duration::from_secs(6), "{warm_up:?}");
    assert!(stopped <= MAX_MEASURING);
    assert!(
        line.speed() < 40.0 * MB && line.speed() > 20.0 * MB,
        "{}",
        line.speed()
    );
}

#[test]
fn a_connection_that_fails_after_moving_bytes_is_replaced_on_its_leg() {
    let mut line = Line::new(vec![50, 50], 0);
    let rate = |moving: &[usize], _: Duration| moving.len().min(4) as f64 * 10.0 * MB;
    for _ in 0..3 {
        assert_eq!(line.step(&rate), Decision::Wait);
    }
    assert!(matches!(line.controller.phase, Phase::Judging { .. }));
    assert!(line.end_leg(1, true));
    assert!(
        matches!(line.controller.phase, Phase::Settling { ticks: 0 }),
        "the window under way is judged again with the new connection"
    );
    assert_eq!(
        line.controller.opened,
        vec![1, 1],
        "it takes the lost one's slot"
    );
    let stopped = line.run(rate);

    assert_eq!(line.replaced, vec![1]);
    assert_eq!(line.controller.usable, vec![true, true]);
    assert!(line.opens.contains(&1), "{:?}", line.opens);
    assert!(stopped < MAX_MEASURING);
    assert!(close(line.speed(), 40.0 * MB, 0.001), "{}", line.speed());

    // Past the warm-up a replacement leaves the measuring as it is.
    let mut line = Line::new(vec![50, 50], 0);
    while line.controller.warm_up().is_none() {
        assert_ne!(line.step(&rate), Decision::Stop);
    }
    assert!(line.end_leg(0, true));
    assert!(matches!(line.controller.phase, Phase::Measuring { .. }));
}

#[test]
fn a_connection_that_ends_before_its_first_byte_retires_its_leg() {
    // Leg 1 cannot reach the URL.
    let mut line = Line::new(vec![50, 50], 0);
    let rate = |moving: &[usize], _: Duration| moving.len().min(4) as f64 * 10.0 * MB;
    assert_eq!(line.step(&rate), Decision::Wait);
    assert!(!line.end_leg(1, false));

    assert_eq!(line.controller.usable, vec![true, false]);
    assert_eq!(line.controller.opened, vec![1, 0]);
    line.run(rate);
    assert!(line.opens.iter().all(|leg| *leg == 0), "{:?}", line.opens);
    assert!(line.replaced.is_empty());
}

#[test]
fn the_third_loss_on_a_leg_retires_it() {
    let mut line = Line::new(vec![50, 50], 0);
    let rate = |moving: &[usize], _: Duration| moving.len().min(4) as f64 * 10.0 * MB;
    for loss in 1..LEG_LOSSES {
        assert_eq!(line.step(&rate), Decision::Wait);
        assert!(line.end_leg(1, true), "loss {loss}");
    }
    assert_eq!(line.step(&rate), Decision::Wait);
    assert!(!line.end_leg(1, true));

    assert_eq!(line.replaced, vec![1; usize::from(LEG_LOSSES) - 1]);
    assert_eq!(line.controller.usable, vec![true, false]);
    assert_eq!(line.controller.opened, vec![1, 0]);
}

#[test]
fn a_retired_leg_s_slot_goes_to_the_legs_still_up() {
    // Every connection lifts the line, so the ramp runs to the cap.
    let mut line = Line::new(vec![50, 50], 0);
    let rate = |moving: &[usize], _: Duration| moving.len() as f64 * 10.0 * MB;
    assert_eq!(line.step(&rate), Decision::Wait);
    assert!(!line.end_leg(1, false));
    line.run(rate);

    assert_eq!(line.opens, vec![0; MAX_CONNECTIONS - 1]);
    assert_eq!(line.connections.len(), MAX_CONNECTIONS);
    assert_eq!(line.controller.opened, vec![MAX_CONNECTIONS as u16, 0]);
}

#[test]
fn a_source_that_sends_nothing_stops_the_run_at_the_first_byte_wait() {
    let mut line = Line::new(vec![100], 0);
    let stopped = line.run(|_, _| 0.0);
    assert_eq!(stopped, FIRST_BYTE_WAIT);
    assert!(line.opens.is_empty());
    assert_eq!(line.controller.warm_up(), None);

    // A first byte inside the wait is measured as any other.
    let ticks = (FIRST_BYTE_WAIT.as_millis() / TICK.as_millis()) as u32;
    let mut line = Line::new(vec![100], ticks - 2);
    let stopped = line.run(|moving, _| moving.len().min(1) as f64 * 50.0 * MB);
    assert!(
        stopped > FIRST_BYTE_WAIT && stopped < MAX_MEASURING,
        "{stopped:?}"
    );
    assert!(close(line.speed(), 50.0 * MB, 0.001), "{}", line.speed());
}
