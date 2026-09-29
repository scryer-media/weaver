use std::sync::{
    Arc, Mutex, RwLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use tokio::{
    sync::{Notify, watch},
    time::Instant,
};
use weaver_tunnel::pipe::{DialError, Dialed, Dialer, Resolution, Target, cooldown};

use super::{EgressHealth, Route};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LegHealthState {
    Up,
    Down(String),
    Probing,
    Blocked(String),
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
    bytes_per_second: u64,
    sampled_at: Instant,
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
            bytes_per_second: 0,
            sampled_at: Instant::now(),
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
        match &self.egress {
            EgressHealth::Down(reason) => return LegHealthState::Down(reason.clone()),
            EgressHealth::Unknown => {
                return LegHealthState::Down("Egress health is unknown".into());
            }
            EgressHealth::Up => {}
        }
        match self.until {
            Some(until) if until > now => LegHealthState::Down(self.reason.clone()),
            Some(_) => LegHealthState::Probing,
            None => LegHealthState::Up,
        }
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
        self.failures = self.failures.saturating_add(1);
        if self.failures >= 2 || self.until.is_some() {
            self.cooldowns = self.cooldowns.saturating_add(1);
            self.until = Some(Instant::now() + cooldown(self.cooldowns));
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
                bytes_per_second: state.bytes_per_second,
            })
            .collect()
    }
}

struct Shared {
    state: Mutex<AllocationState>,
    changed: Arc<Notify>,
    targets: watch::Sender<Vec<LegAllocation>>,
    socket_targets: watch::Sender<Vec<u16>>,
}
impl Shared {
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
    }
}

/// Per-consumer allocation; NNTP knows only the dialer and its target-change signal.
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
            }),
            timer: Mutex::new(None),
        });
        let weak = Arc::downgrade(&result);
        *result.timer.lock().expect("allocation timer") = Some(handle.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let Some(route) = weak.upgrade() else {
                    break;
                };
                let mut state = route.shared.state.lock().expect("leg allocation");
                let now = Instant::now();
                for leg in &mut state.legs {
                    let elapsed = now.duration_since(leg.sampled_at).as_secs_f64();
                    if elapsed >= 1.0 {
                        leg.bytes_per_second =
                            (leg.reads.swap(0, Ordering::Relaxed) as f64 / elapsed) as u64;
                        leg.sampled_at = now;
                    }
                }
                let next = state.snapshot();
                let previous = route.shared.targets.borrow();
                let changed = next.iter().zip(previous.iter()).any(|(a, b)| {
                    a.target != b.target
                        || a.health != b.health
                        || a.bytes_per_second != b.bytes_per_second
                });
                drop(previous);
                if changed {
                    route.shared.publish(&state);
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
        if let Some(leg) = state.legs.get_mut(position) {
            leg.egress = health;
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
        route.validate_allocation()?;
        if route.legs.len() != legs.len() {
            return Err("a dialer is required for every leg".into());
        }
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
        Ok(())
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
            .ok_or_else(|| DialError::AtCapacity(self.shared.changed.clone()))?;
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
        self.completed = true;
        self.shared.publish(&state);
        drop(state);
        dialed.path.leg = Some(self.position);
        let shared = self.shared.clone();
        let position = self.position;
        let generation = self.generation;
        dialed.outcome.on_read(move |bytes| {
            reads.fetch_add(bytes as u64, Ordering::Relaxed);
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
        if let Some(leg) = state
            .legs
            .get_mut(self.position)
            .filter(|l| l.generation == self.generation)
        {
            leg.failed(error);
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
        self.allocations()
            .iter()
            .any(|leg| matches!(leg.health, LegHealthState::Probing) && leg.open + leg.opening == 0)
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
        match opening.dialer.dial(&target).await {
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
            .ok_or_else(|| DialError::AtCapacity(self.shared.changed.clone()))?;
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
