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
        let decision = self.controller.tick(Tick {
            at,
            bytes: self.bytes,
            waiting,
        });
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

    fn end_leg(&mut self, leg: usize) {
        self.connections.retain(|(on, _)| *on != leg);
        self.controller.ended(leg);
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
fn a_line_that_never_settles_stops_at_the_cap() {
    // Still rising when the connections run out: the ramp never ends.
    let mut line = Line::new(vec![100], 0);
    let growth = |at: Duration| 1.02f64.powi((at.as_millis() / TICK.as_millis()) as i32);
    let stopped = line.run(|_, at| 10.0 * MB * growth(at));
    assert_eq!(stopped, MAX_MEASURING);
    assert_eq!(line.opens.len(), MAX_CONNECTIONS - 1);
    assert_eq!(line.controller.warm_up(), None);

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
    line.end_leg(1);
    let stopped = line.run(rate);

    assert_eq!(line.opens, vec![0, 0, 0, 0], "all on the leg still up");
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
