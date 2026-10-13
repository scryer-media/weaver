use std::collections::VecDeque;
use std::sync::{
    Arc, Mutex, RwLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;

use tokio::{
    sync::{Notify, watch},
    time::Instant,
};
use weaver_tunnel::pipe::{DialError, Dialed, Dialer, Resolution, Target, cooldown_from};

use super::{EgressHealth, Route};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LegHealthState {
    Up,
    Down(String),
    Probing,
    Blocked(String),
}
impl LegHealthState {
    // The health a leg has from its egress alone, before any dial evidence.
    pub fn of_egress(health: EgressHealth) -> Self {
        match health {
            EgressHealth::Up => Self::Up,
            EgressHealth::Down(reason) => Self::Down(reason),
            EgressHealth::Unknown => Self::Down("Egress health is unknown".into()),
        }
    }
}

// How many one-second windows a leg's reported rate spans.
const THROUGHPUT_WINDOWS: usize = 4;

// The bytes a leg carried over its most recent one-second windows.
//
// A single second is a noisy reading: bytes are counted as reads land while
// the egress limiter paces them afterwards in short bursts, so one second
// can land well over the limit and the next well under it even though the
// limiter holds the rate exactly. The rate over the last few windows is
// what the leg sustains, and that is what the flow reports.
struct Throughput {
    windows: VecDeque<(u64, f64)>,
    sampled_at: Instant,
}
impl Throughput {
    fn new(now: Instant) -> Self {
        Self {
            windows: VecDeque::with_capacity(THROUGHPUT_WINDOWS),
            sampled_at: now,
        }
    }
    // Whether a full second has passed since the last window closed.
    fn due(&self, now: Instant) -> bool {
        now.duration_since(self.sampled_at).as_secs_f64() >= 1.0
    }
    // Restart the open window at `now` without recording one. A quiet leg
    // whose windows are all empty gains nothing from another empty window.
    fn restart(&mut self, now: Instant) {
        self.sampled_at = now;
    }
    fn record(&mut self, bytes: u64, now: Instant) {
        let elapsed = now.duration_since(self.sampled_at).as_secs_f64();
        if self.windows.len() == THROUGHPUT_WINDOWS {
            self.windows.pop_front();
        }
        self.windows.push_back((bytes, elapsed));
        self.sampled_at = now;
    }
    fn bytes_per_second(&self) -> u64 {
        let (bytes, seconds) = self
            .windows
            .iter()
            .fold((0u64, 0f64), |(b, s), (bytes, secs)| (b + bytes, s + secs));
        if seconds <= 0.0 {
            0
        } else {
            (bytes as f64 / seconds) as u64
        }
    }
}

#[derive(Clone, Debug)]
pub struct LegAllocation {
    pub position: usize,
    pub target: u16,
    pub open: u16,
    pub opening: u16,
    pub health: LegHealthState,
    pub path: Option<weaver_tunnel::pipe::DialPath>,
    pub source: Option<std::net::SocketAddr>,
    pub bytes_per_second: u64,
}

struct LegState {
    path: Option<weaver_tunnel::pipe::DialPath>,
    source: Option<std::net::SocketAddr>,
    reads: Arc<AtomicU64>,
    throughput: Throughput,
    generation: u64,
    open: u16,
    opening: u16,
    failures: u32,
    cooldowns: u32,
    until: Option<Instant>,
    blocked: Option<String>,
    egress: EgressHealth,
    reason: String,
}
impl Default for LegState {
    fn default() -> Self {
        Self {
            path: None,
            source: None,
            reads: Default::default(),
            throughput: Throughput::new(Instant::now()),
            generation: 0,
            open: 0,
            opening: 0,
            failures: 0,
            cooldowns: 0,
            until: None,
            blocked: None,
            egress: EgressHealth::Up,
            reason: String::new(),
        }
    }
}
impl LegState {
    fn health(&self, now: Instant) -> LegHealthState {
        if let Some(reason) = &self.blocked {
            return LegHealthState::Blocked(reason.clone());
        }
        if let down @ LegHealthState::Down(_) = LegHealthState::of_egress(self.egress.clone()) {
            return down;
        }
        match self.until {
            Some(until) if until > now => LegHealthState::Down(self.reason.clone()),
            Some(_) => LegHealthState::Probing,
            None => LegHealthState::Up,
        }
    }
    // `health(now) == Probing` without building the state or its reason.
    fn is_probing(&self, now: Instant) -> bool {
        self.blocked.is_none()
            && matches!(self.egress, EgressHealth::Up)
            && self.until.is_some_and(|until| until <= now)
    }
    fn failed(&mut self, error: &DialError) {
        if !error.is_path_evidence() {
            return;
        }
        self.reason = error.to_string();
        if matches!(error, DialError::Fatal(_)) {
            self.blocked = Some(self.reason.clone());
            return;
        }
        // One outage is one cooldown. Every dial in flight when the leg
        // went down fails against the same outage, so a failure that lands
        // while the leg is already cooling neither counts nor lengthens the
        // cooldown; only a probe that fails after it lapses does.
        let now = Instant::now();
        if self.until.is_some_and(|until| until > now) {
            return;
        }
        self.failures = self.failures.saturating_add(1);
        if self.failures >= 2 || self.until.is_some() {
            self.cooldowns = self.cooldowns.saturating_add(1);
            self.until = Some(
                now + cooldown_from(
                    weaver_nntp::plan_timing::timing().leg_cooldown_initial,
                    self.cooldowns,
                ),
            );
        }
    }
    fn succeeded(&mut self) {
        self.failures = 0;
        self.cooldowns = 0;
        self.until = None;
        self.reason.clear();
    }
}

struct AllocationState {
    generation: u64,
    route: Route,
    cap: u16,
    legs: Vec<LegState>,
    stopped: bool,
}
impl AllocationState {
    fn snapshot(&self) -> Vec<LegAllocation> {
        let now = Instant::now();
        let health: Vec<_> = self.legs.iter().map(|s| s.health(now)).collect();
        let probing: Vec<_> = health
            .iter()
            .enumerate()
            .filter_map(|(i, h)| matches!(h, LegHealthState::Probing).then_some(i))
            .take(usize::from(self.cap))
            .collect();
        let ready: Vec<_> = health
            .iter()
            .map(|h| matches!(h, LegHealthState::Up))
            .collect();
        let mut targets = self
            .route
            .targets(self.cap.saturating_sub(probing.len() as u16), &ready)
            .expect("validated route");
        for index in probing {
            targets[index] = 1;
        }
        self.legs
            .iter()
            .enumerate()
            .map(|(position, state)| LegAllocation {
                position,
                target: if self.stopped { 0 } else { targets[position] },
                open: state.open,
                opening: state.opening,
                health: health[position].clone(),
                path: state.path.clone(),
                source: state.source,
                bytes_per_second: state.throughput.bytes_per_second(),
            })
            .collect()
    }
}

pub(super) struct PreparedUpdate {
    route: Route,
    legs: Vec<Arc<dyn Dialer>>,
    cap: u16,
}

struct Shared {
    state: Mutex<AllocationState>,
    changed: Arc<Notify>,
    targets: watch::Sender<Vec<LegAllocation>>,
    socket_targets: watch::Sender<Vec<u16>>,
    // Set while the rate timer sleeps with no bytes moving. The first read
    // on any leg, and any publish, wake it through `timer_wake`.
    timer_parked: AtomicBool,
    timer_wake: Notify,
    #[cfg(test)]
    timer_snapshots: AtomicU64,
}
impl Shared {
    // Wake a parked rate timer so it re-plans its next wake: a publish may
    // have set or cleared a leg's cooldown.
    fn wake_timer(&self) {
        if self.timer_parked.load(Ordering::SeqCst) {
            self.timer_wake.notify_one();
        }
    }
    // One pass of the rate timer: close due throughput windows, publish
    // what changed, and say when the next pass is due. `None` means no
    // bytes are moving and no cooldown is pending, so only a read or a
    // publish can make a pass worth running.
    fn sample(&self, state: &mut AllocationState, last_pass: Instant) -> Option<Instant> {
        let now = Instant::now();
        if self.timer_parked.swap(false, Ordering::SeqCst) {
            // Every window was empty while parked; open fresh ones from now
            // so the first busy second is not spread over the parked span.
            for leg in &mut state.legs {
                leg.throughput.restart(now);
            }
        }
        let mut evaluate = false;
        let mut moving = false;
        let mut next_until: Option<Instant> = None;
        for leg in &mut state.legs {
            let active = leg.throughput.bytes_per_second() > 0;
            if leg.throughput.due(now) {
                let bytes = leg.reads.swap(0, Ordering::SeqCst);
                if bytes > 0 || active {
                    leg.throughput.record(bytes, now);
                    evaluate = true;
                } else {
                    leg.throughput.restart(now);
                }
            }
            moving |= leg.reads.load(Ordering::SeqCst) > 0 || leg.throughput.bytes_per_second() > 0;
            if let Some(until) = leg.until {
                if until > last_pass && until <= now {
                    evaluate = true;
                }
                if until > now {
                    next_until = Some(next_until.map_or(until, |next| next.min(until)));
                }
            }
        }
        if evaluate {
            #[cfg(test)]
            self.timer_snapshots.fetch_add(1, Ordering::Relaxed);
            let next = state.snapshot();
            let previous = self.targets.borrow();
            let capacity_changed = next
                .iter()
                .zip(previous.iter())
                .any(|(a, b)| a.target != b.target || a.health != b.health);
            let metrics_changed = next
                .iter()
                .zip(previous.iter())
                .any(|(a, b)| a.bytes_per_second != b.bytes_per_second);
            drop(previous);
            if capacity_changed {
                self.publish(state);
            } else if metrics_changed {
                self.targets.send_replace(next);
            }
        }
        if moving {
            let tick = now + Duration::from_secs(1);
            return Some(next_until.map_or(tick, |until| until.min(tick)));
        }
        self.timer_parked.store(true, Ordering::SeqCst);
        // A read that landed before the flag was set saw no parked timer and
        // sent no wake; pick it up here instead of sleeping past it.
        if state
            .legs
            .iter()
            .any(|leg| leg.reads.load(Ordering::SeqCst) > 0)
        {
            self.timer_parked.store(false, Ordering::SeqCst);
            return Some(now + Duration::from_secs(1));
        }
        next_until
    }
    fn publish(&self, state: &AllocationState) {
        let snapshot = state.snapshot();
        let next = snapshot.iter().map(|leg| leg.target).collect::<Vec<_>>();
        self.socket_targets.send_if_modified(|targets| {
            if *targets == next {
                return false;
            }
            *targets = next;
            true
        });
        self.targets.send_replace(snapshot);
        self.changed.notify_waiters();
        self.wake_timer();
    }
}

// Per-consumer allocation; NNTP knows only the dialer and its target-change signal.
pub struct Weighted {
    legs: RwLock<Vec<Arc<dyn Dialer>>>,
    shared: Arc<Shared>,
    timer: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Weighted {
    pub fn new(
        route: Route,
        legs: Vec<Arc<dyn Dialer>>,
        cap: u16,
        handle: &tokio::runtime::Handle,
    ) -> Result<Arc<Self>, String> {
        route.validate_allocation()?;
        if route.legs.len() != legs.len() {
            return Err("a dialer is required for every leg".into());
        }
        let state = AllocationState {
            generation: 0,
            legs: (0..legs.len()).map(|_| LegState::default()).collect(),
            route,
            cap,
            stopped: false,
        };
        let (targets, _) = watch::channel(state.snapshot());
        let (socket_targets, _) =
            watch::channel(state.snapshot().iter().map(|leg| leg.target).collect());
        let result = Arc::new(Self {
            legs: RwLock::new(legs),
            shared: Arc::new(Shared {
                state: Mutex::new(state),
                changed: Arc::new(Notify::new()),
                targets,
                socket_targets,
                timer_parked: AtomicBool::new(false),
                timer_wake: Notify::new(),
                #[cfg(test)]
                timer_snapshots: AtomicU64::new(0),
            }),
            timer: Mutex::new(None),
        });
        let weak = Arc::downgrade(&result);
        let shared = result.shared.clone();
        *result.timer.lock().expect("allocation timer") = Some(handle.spawn(async move {
            // The timer runs only when something can change: once a second
            // while bytes move or a rate is still decaying, at the instant a
            // cooldown lapses, and when a read or a publish wakes it.
            // Otherwise it sleeps, and an idle route costs nothing.
            let mut last_pass = Instant::now();
            loop {
                let next = {
                    let Some(route) = weak.upgrade() else {
                        break;
                    };
                    let mut state = route.shared.state.lock().expect("leg allocation");
                    let next = route.shared.sample(&mut state, last_pass);
                    last_pass = Instant::now();
                    next
                };
                match next {
                    Some(at) => {
                        tokio::select! {
                            () = tokio::time::sleep_until(at) => {}
                            () = shared.timer_wake.notified() => {}
                        }
                    }
                    None => shared.timer_wake.notified().await,
                }
            }
        }));
        Ok(result)
    }
    pub fn allocations(&self) -> Vec<LegAllocation> {
        self.shared.state.lock().expect("leg allocation").snapshot()
    }
    pub fn subscribe(&self) -> watch::Receiver<Vec<LegAllocation>> {
        self.shared.targets.subscribe()
    }
    pub fn set_egress_health(&self, position: usize, health: EgressHealth) {
        let mut state = self.shared.state.lock().expect("leg allocation");
        // The network runtime restates every leg's egress health on a fixed
        // cadence; a restatement that changes nothing publishes nothing.
        match state.legs.get_mut(position) {
            Some(leg) if leg.egress == health => return,
            Some(leg) => leg.egress = health,
            None => {}
        }
        self.shared.publish(&state);
    }
    pub fn reweight(&self, route: Route, cap: u16) -> Result<(), String> {
        route.validate_allocation()?;
        let mut state = self.shared.state.lock().expect("leg allocation");
        if route.legs.len() != state.route.legs.len()
            || route
                .legs
                .iter()
                .zip(&state.route.legs)
                .any(|(a, b)| a.egress_id != b.egress_id || a.path != b.path)
        {
            return Err("reweight requires unchanged leg paths".into());
        }
        state.route = route;
        state.cap = cap;
        self.shared.publish(&state);
        Ok(())
    }
    pub fn budget_changed(&self) -> Arc<Notify> {
        self.shared.changed.clone()
    }
    pub fn track_external(
        &self,
        position: usize,
        stage: &Arc<dyn Dialer>,
        dialed: &mut Dialed,
    ) -> Result<(), DialError> {
        let generation = {
            let mut state = self.shared.state.lock().expect("leg allocation");
            if !self
                .legs
                .read()
                .expect("route stages")
                .get(position)
                .is_some_and(|current| Arc::ptr_eq(current, stage))
            {
                return Err(DialError::Skipped(
                    "leg changed during feed connection".into(),
                ));
            }
            state.legs[position].opening += 1;
            state.legs[position].generation
        };
        Opening {
            shared: self.shared.clone(),
            position,
            generation,
            dialer: stage.clone(),
            probe: false,
            completed: false,
        }
        .success(dialed)
    }
    pub fn report_external(
        &self,
        position: usize,
        stage: &Arc<dyn Dialer>,
        error: Option<&DialError>,
    ) {
        let mut state = self.shared.state.lock().expect("leg allocation");
        if self
            .legs
            .read()
            .expect("route stages")
            .get(position)
            .is_some_and(|current| Arc::ptr_eq(current, stage))
        {
            if let Some(error) = error {
                state.legs[position].failed(error);
            } else {
                state.legs[position].succeeded();
            }
            self.shared.publish(&state);
        }
    }
    pub fn update(&self, route: Route, legs: Vec<Arc<dyn Dialer>>, cap: u16) -> Result<(), String> {
        self.apply_update(Self::prepare_update(route, legs, cap)?);
        Ok(())
    }
    pub(super) fn prepare_update(
        route: Route,
        legs: Vec<Arc<dyn Dialer>>,
        cap: u16,
    ) -> Result<PreparedUpdate, String> {
        route.validate_allocation()?;
        if route.legs.len() != legs.len() {
            return Err("a dialer is required for every leg".into());
        }
        Ok(PreparedUpdate { route, legs, cap })
    }
    pub(super) fn apply_update(&self, update: PreparedUpdate) {
        let PreparedUpdate { route, legs, cap } = update;
        let mut state = self.shared.state.lock().expect("leg allocation");
        let mut old = self.legs.write().expect("route stages");
        state.generation = state.generation.wrapping_add(1);
        let generation = state.generation;
        let previous = std::mem::take(&mut state.legs);
        let mut previous = previous.into_iter();
        state.legs = legs
            .iter()
            .enumerate()
            .map(|(position, stage)| {
                let prior = previous.next();
                if old.get(position).is_some_and(|p| Arc::ptr_eq(p, stage)) {
                    prior.expect("existing leg state")
                } else {
                    LegState {
                        generation,
                        ..Default::default()
                    }
                }
            })
            .collect();
        *old = legs;
        state.route = route;
        state.cap = cap;
        self.shared.publish(&state);
    }
    fn begin(&self) -> Result<Opening, DialError> {
        let mut state = self.shared.state.lock().expect("leg allocation");
        let allocations = state.snapshot();
        let position = allocations
            .iter()
            .filter(|a| a.target > a.open.saturating_add(a.opening))
            .max_by_key(|a| {
                (
                    matches!(a.health, LegHealthState::Probing),
                    a.target - a.open - a.opening,
                    std::cmp::Reverse(a.position),
                )
            })
            .map(|a| a.position)
            .ok_or_else(|| DialError::AtCapacity(vec![self.shared.changed.clone()]))?;
        state.legs[position].opening += 1;
        self.shared.publish(&state);
        Ok(Opening {
            shared: self.shared.clone(),
            position,
            generation: state.legs[position].generation,
            probe: matches!(allocations[position].health, LegHealthState::Probing),
            dialer: self.legs.read().expect("route stages")[position].clone(),
            completed: false,
        })
    }
}
impl Drop for Weighted {
    fn drop(&mut self) {
        if let Some(timer) = self.timer.get_mut().expect("allocation timer").take() {
            timer.abort();
        }
    }
}

struct Opening {
    shared: Arc<Shared>,
    probe: bool,
    position: usize,
    generation: u64,
    dialer: Arc<dyn Dialer>,
    completed: bool,
}
impl Opening {
    fn success(mut self, dialed: &mut Dialed) -> Result<(), DialError> {
        let mut state = self.shared.state.lock().expect("leg allocation");
        let egress_id = state.route.legs.get(self.position).map(|l| l.egress_id);
        let Some(leg) = state
            .legs
            .get_mut(self.position)
            .filter(|l| l.generation == self.generation)
        else {
            self.completed = true;
            return Err(DialError::Skipped(
                "leg changed during connection setup".into(),
            ));
        };
        leg.opening -= 1;
        leg.open += 1;
        dialed.path.leg = Some(self.position);
        leg.path = Some(dialed.path.clone());
        leg.source = dialed.source;
        leg.succeeded();
        let reads = leg.reads.clone();
        if let Some(egress_id) = egress_id {
            super::network_metrics::record_leg_dial(
                egress_id,
                super::network_metrics::LegDialResult::Success,
            );
        }
        self.completed = true;
        self.shared.publish(&state);
        drop(state);
        dialed.path.leg = Some(self.position);
        let shared = self.shared.clone();
        let position = self.position;
        let generation = self.generation;
        let wake = self.shared.clone();
        dialed.outcome.on_read(move |bytes| {
            // Only the first read after the timer drained the counter can
            // find it parked, so steady traffic pays one extra load at most.
            if reads.fetch_add(bytes as u64, Ordering::SeqCst) == 0 {
                wake.wake_timer();
            }
        });
        dialed.outcome.on_close(move || {
            let mut state = shared.state.lock().expect("leg allocation");
            if let Some(leg) = state
                .legs
                .get_mut(position)
                .filter(|l| l.generation == generation)
            {
                leg.open -= 1;
            }
            shared.publish(&state);
        });
        Ok(())
    }
    fn failed(&self, error: &DialError) {
        let mut state = self.shared.state.lock().expect("leg allocation");
        let egress_id = state.route.legs.get(self.position).map(|l| l.egress_id);
        if let Some(leg) = state
            .legs
            .get_mut(self.position)
            .filter(|l| l.generation == self.generation)
        {
            let cooldowns = leg.cooldowns;
            leg.failed(error);
            let cooled = leg.cooldowns != cooldowns;
            if let Some(egress_id) = egress_id {
                super::network_metrics::record_leg_dial(
                    egress_id,
                    super::network_metrics::LegDialResult::of_error(error),
                );
                if cooled {
                    super::network_metrics::record_leg_cooldown(egress_id);
                }
            }
        }
        self.shared.publish(&state);
    }
}
impl Drop for Opening {
    fn drop(&mut self) {
        if !self.completed {
            let mut state = self.shared.state.lock().expect("leg allocation");
            if let Some(leg) = state
                .legs
                .get_mut(self.position)
                .filter(|l| l.generation == self.generation)
            {
                leg.opening -= 1;
            }
            self.shared.publish(&state);
        }
    }
}

#[async_trait::async_trait]
impl Dialer for Weighted {
    fn over_limit_cleared(&self) {
        for leg in self.legs.read().expect("route legs").iter() {
            leg.over_limit_cleared();
        }
    }
    fn leg_targets(&self) -> Option<watch::Receiver<Vec<u16>>> {
        Some(self.shared.socket_targets.subscribe())
    }
    fn needs_probe(&self) -> bool {
        // Read the leg states in place: the full allocation snapshot clones
        // every leg's path and reason, and this question needs none of it.
        let state = self.shared.state.lock().expect("leg allocation");
        let now = Instant::now();
        state
            .legs
            .iter()
            .any(|leg| leg.is_probing(now) && leg.open + leg.opening == 0)
    }
    fn has_capacity(&self) -> bool {
        self.allocations()
            .iter()
            .any(|a| a.target > a.open.saturating_add(a.opening))
    }
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        let opening = self.begin()?;
        let position = opening.position;
        let mut target = target.clone();
        if let weaver_tunnel::pipe::Purpose::Nntp { leg, .. } = &mut target.purpose {
            *leg = position;
        }
        if opening.probe
            && let weaver_tunnel::pipe::Purpose::Nntp { server, leg } = target.purpose
        {
            target.purpose = weaver_tunnel::pipe::Purpose::NntpProbe { server, leg };
        }
        // Enforce the leg's own budget here so a leg that swallows connects
        // books the timeout against itself; the caller's route budget is
        // only a backstop and fires later.
        let dialed = tokio::time::timeout(opening.dialer.budget(), opening.dialer.dial(&target))
            .await
            .unwrap_or_else(|_| {
                Err(DialError::Timeout {
                    stage: "route leg".into(),
                })
            });
        match dialed {
            Ok(mut dialed) => {
                opening.success(&mut dialed)?;
                Ok(dialed)
            }
            Err(error) => {
                opening.failed(&error);
                Err(error)
            }
        }
    }
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        let allocations = self.allocations();
        let position = allocations
            .iter()
            .find(|a| a.target > 0)
            .map(|a| a.position)
            .ok_or_else(|| DialError::AtCapacity(vec![self.shared.changed.clone()]))?;
        let stage = self
            .legs
            .read()
            .expect("route stages")
            .get(position)
            .cloned()
            .ok_or_else(|| DialError::Skipped("route changed".into()))?;
        stage.resolve(host).await
    }
    fn budget(&self) -> Duration {
        self.legs
            .read()
            .expect("route stages")
            .iter()
            .map(|l| l.budget())
            .max()
            .unwrap_or_default()
    }
    async fn shutdown(&self) {
        {
            let mut state = self.shared.state.lock().expect("leg allocation");
            state.stopped = true;
            self.shared.publish(&state);
        }
        let legs = self.legs.read().expect("route stages").clone();
        for leg in legs {
            leg.shutdown().await;
        }
    }
    fn describe(&self) -> String {
        "weighted route".into()
    }
}

#[cfg(test)]
#[path = "weighted_tests.rs"]
mod tests;
