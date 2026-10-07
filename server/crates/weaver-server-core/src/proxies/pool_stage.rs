use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Notify, oneshot},
    task::JoinSet,
    time::Instant,
};
use weaver_nntp::{
    ADDRESS_ATTEMPT_LIMIT,
    candidate_plan::{Attempt, CandidatePlan, PlanSnapshot},
};
use weaver_tunnel::pipe::{DialError, Dialed, Dialer, Resolution, SetupHandle, Target};

const POOL_IDLE_RETIRE: Duration = Duration::from_secs(600);

#[derive(Clone, Debug)]
pub struct PoolMemberStatus {
    pub id: u32,
    pub open: u32,
    pub opening: u32,
    pub session_handshake: Option<Duration>,
    pub blocked: Option<String>,
    pub warmed: bool,
}

struct Member {
    id: u32,
    dialer: Arc<dyn Dialer>,
    streams: weaver_tunnel::pipe::Revocable,
    status: Mutex<PoolMemberStatus>,
    idle_since: Mutex<Instant>,
    session_gate: tokio::sync::Mutex<()>,
    retiring: AtomicBool,
    outcomes: Mutex<Vec<std::sync::Weak<weaver_tunnel::bridge::ConnectionOutcome>>>,
}
impl Member {
    fn recall(&self) {
        self.retiring.store(true, Ordering::Release);
        let outcomes = std::mem::take(&mut *self.outcomes.lock().expect("member outcomes"));
        for outcome in outcomes.into_iter().filter_map(|outcome| outcome.upgrade()) {
            outcome.retire();
        }
    }
    async fn warm(&self) -> Result<(), DialError> {
        match tokio::time::timeout(self.dialer.budget(), self.warm_inner()).await {
            Ok(result) => result,
            Err(_) => {
                self.dialer.retire_idle().await;
                Err(DialError::Timeout {
                    stage: format!("pool member {} preparation", self.id),
                })
            }
        }
    }
    async fn warm_inner(&self) -> Result<(), DialError> {
        let _gate = self.session_gate.lock().await;
        {
            let status = self.status.lock().expect("pool member");
            if let Some(reason) = &status.blocked {
                return Err(DialError::Fatal(weaver_tunnel::TunnelError::Engine(
                    reason.clone(),
                )));
            }
            if status.warmed {
                return Ok(());
            }
        }
        let started = Instant::now();
        let result = self.dialer.prepare().await;
        let mut status = self.status.lock().expect("pool member");
        status.session_handshake = Some(started.elapsed());
        status.warmed = result.is_ok();
        if let Err(DialError::Fatal(error)) = &result {
            status.blocked = Some(error.to_string());
        }
        result
    }
    async fn dial(self: &Arc<Self>, target: &Target) -> Result<(Dialed, Duration), DialError> {
        if self.retiring.load(Ordering::Acquire) {
            return Err(DialError::Skipped("pool member removed".into()));
        }
        if let Some(reason) = self.status.lock().expect("pool member").blocked.clone() {
            return Err(DialError::Fatal(weaver_tunnel::TunnelError::Engine(reason)));
        }
        let guard = MemberOpening::new(self.clone());
        self.warm().await?;
        let started = Instant::now();
        let result = tokio::time::timeout(
            self.dialer.budget().min(ADDRESS_ATTEMPT_LIMIT),
            self.streams.dial(target),
        )
        .await
        .map_err(|_| DialError::Timeout {
            stage: format!("pool member {}", self.id),
        })?;
        if let Err(DialError::Fatal(error)) = &result {
            self.status.lock().expect("pool member").blocked = Some(error.to_string());
        }
        let mut dialed = result?;
        guard.connected(&mut dialed);
        Ok((dialed, started.elapsed()))
    }
    async fn retire_if_idle(&self) {
        let _gate = self.session_gate.lock().await;
        let retire = {
            let status = self.status.lock().expect("pool member");
            status.warmed
                && status.open == 0
                && status.opening == 0
                && self.idle_since.lock().expect("member idle").elapsed() >= POOL_IDLE_RETIRE
        };
        if retire && self.dialer.retire_idle().await {
            self.status.lock().expect("pool member").warmed = false;
        }
    }
}
struct MemberOpening {
    member: Arc<Member>,
    done: bool,
}
impl MemberOpening {
    fn new(member: Arc<Member>) -> Self {
        member.status.lock().expect("pool member").opening += 1;
        Self {
            member,
            done: false,
        }
    }
    fn connected(mut self, dialed: &mut Dialed) {
        {
            let mut status = self.member.status.lock().expect("pool member");
            status.opening -= 1;
            status.open += 1;
        }
        self.done = true;
        {
            let mut outcomes = self.member.outcomes.lock().expect("member outcomes");
            outcomes.retain(|outcome| outcome.strong_count() > 0);
            outcomes.push(Arc::downgrade(&dialed.outcome));
        }
        if self.member.retiring.load(Ordering::Acquire) {
            dialed.outcome.retire();
        }
        let member = self.member.clone();
        dialed.outcome.on_close(move || {
            let mut status = member.status.lock().expect("pool member");
            status.open -= 1;
            if status.open == 0 {
                *member.idle_since.lock().expect("member idle") = Instant::now();
            }
        });
    }
}
impl Drop for MemberOpening {
    fn drop(&mut self) {
        if !self.done {
            self.member.status.lock().expect("pool member").opening -= 1;
        }
    }
}

struct PoolState {
    id: u32,
    plan: Mutex<CandidatePlan<u32>>,
    members: RwLock<HashMap<u32, Arc<Member>>>,
    changed: Notify,
    stopped: AtomicBool,
    revision: AtomicU64,
}
impl PoolState {
    fn record(&self, member: &Arc<Member>, update: impl FnOnce(&mut CandidatePlan<u32>)) -> bool {
        let members = self.members.read().expect("pool members");
        if !members
            .get(&member.id)
            .is_some_and(|current| Arc::ptr_eq(current, member))
        {
            return false;
        }
        update(&mut self.plan.lock().expect("pool plan"));
        true
    }
    fn decorate(self: &Arc<Self>, member: Arc<Member>, mut dialed: Dialed) -> Dialed {
        let id = member.id;
        dialed.path.pool = Some(self.id);
        dialed.path.member = Some(id);
        let latency_state = self.clone();
        let latency_member = member.clone();
        dialed.outcome.on_body_latency(move |elapsed| {
            latency_state.record(&latency_member, |plan| {
                plan.body_latency(id, elapsed, Instant::now().into_std())
            });
        });
        let delivery_state = self.clone();
        let delivery_member = member.clone();
        dialed.outcome.on_delivery(move |bytes, wire| {
            delivery_state.record(&delivery_member, |plan| {
                plan.delivered(id, bytes, wire, Instant::now().into_std())
            });
        });
        let state = self.clone();
        let upstream = dialed.setup.take();
        dialed.setup = Some(SetupHandle::new(move |reached| {
            if let Some(setup) = upstream {
                setup.complete(reached);
            }
            state.record(&member, |plan| {
                plan.setup(id, reached, Instant::now().into_std())
            });
        }));
        // A connection that fails after it was set up says nothing about
        // whether its member can be connected, so it is not a connect failure
        // of the pin: a server that drops sessions mid-transfer would otherwise
        // race the pool on every pair of drops, and every race starts the
        // pin's age and its delivery evidence over, so no challenger could
        // ever be measured against a throttled pin. Addresses are judged the
        // same way: only connects and setups count against them.
        dialed
    }
    fn pool_error(&self, error: DialError) -> DialError {
        let members = self.members.read().expect("pool members");
        let all_blocked = !members.is_empty()
            && members
                .values()
                .all(|member| member.status.lock().expect("pool member").blocked.is_some());
        if all_blocked {
            DialError::Fatal(weaver_tunnel::TunnelError::Engine(
                "all pool members are blocked".into(),
            ))
        } else if matches!(error, DialError::Fatal(_)) {
            DialError::Egress(std::io::Error::other(error))
        } else {
            error
        }
    }
    async fn run(
        self: Arc<Self>,
        target: Target,
        reply: oneshot::Sender<Result<Dialed, DialError>>,
    ) {
        let mut reply = Some(reply);
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.stopped.load(Ordering::Acquire) {
                return;
            }
            {
                let members = self.members.read().expect("pool members");
                let mut plan = self.plan.lock().expect("pool plan");
                let eligible = plan
                    .snapshot()
                    .candidates
                    .into_iter()
                    .map(|candidate| candidate.candidate)
                    .filter(|id| {
                        members.get(id).is_some_and(|member| {
                            member.status.lock().expect("pool member").blocked.is_none()
                        })
                    })
                    .collect();
                plan.set_candidates(eligible, Instant::now().into_std());
            }
            let action = self
                .plan
                .lock()
                .expect("pool plan")
                .next_attempt(Instant::now().into_std());
            match action {
                Attempt::Wait(_) => notified.await,
                Attempt::Fail(_, message) => {
                    let _ = reply
                        .take()
                        .unwrap()
                        .send(Err(self.pool_error(DialError::Skipped(message))));
                    return;
                }
                Attempt::Dial {
                    order,
                    shadow,
                    announce,
                } => {
                    if let Some(announce) = announce {
                        announce.log(&format!("pool {}", self.id));
                    }
                    let mut last = DialError::Skipped("no enabled pool member".into());
                    for id in order {
                        let member = self.members.read().expect("pool members").get(&id).cloned();
                        let Some(member) = member else {
                            continue;
                        };
                        match member.dial(&target).await {
                            Ok((dialed, elapsed)) => {
                                if !self.record(&member, |plan| {
                                    plan.connected(id, elapsed, Instant::now().into_std())
                                }) {
                                    continue;
                                }
                                let dialed = self.decorate(member.clone(), dialed);
                                if shadow == Some(id) {
                                    // The plan judges a challenger by what its
                                    // shadow connection delivers; one that ends
                                    // first is owed another, and the plan hears
                                    // whether this one answered at all.
                                    let responses = Arc::new(AtomicU64::new(0));
                                    let answered = responses.clone();
                                    dialed.outcome.on_body_latency(move |_| {
                                        answered.fetch_add(1, Ordering::Relaxed);
                                    });
                                    let state = self.clone();
                                    dialed.outcome.on_close(move || {
                                        let responses = responses.load(Ordering::Relaxed);
                                        state.record(&member, |plan| {
                                            plan.shadow_ended(
                                                id,
                                                responses,
                                                Instant::now().into_std(),
                                            )
                                        });
                                    });
                                }
                                let _ = reply.take().unwrap().send(Ok(dialed));
                                return;
                            }
                            Err(error) => {
                                if error.is_evidence() {
                                    self.record(&member, |plan| {
                                        plan.failed(id, Instant::now().into_std())
                                    });
                                }
                                last = error;
                            }
                        }
                    }
                    let _ = reply.take().unwrap().send(Err(self.pool_error(last)));
                    return;
                }
                Attempt::Race { candidates, reason } => {
                    let revision = self.revision.load(Ordering::Acquire);
                    let members: Vec<_> = {
                        let members = self.members.read().expect("pool members");
                        candidates
                            .iter()
                            .filter_map(|id| members.get(id).cloned())
                            .collect()
                    };
                    let mut last = DialError::Skipped("no enabled pool member".into());
                    let mut race = JoinSet::new();
                    for member in members {
                        if member.status.lock().expect("pool member").blocked.is_some() {
                            continue;
                        }
                        let target = target.clone();
                        race.spawn(async move {
                            let result = member.dial(&target).await;
                            (member, result)
                        });
                    }
                    let mut winner = None;
                    let mut slow = Vec::new();
                    while let Some(result) = race.join_next().await {
                        let Ok((member, result)) = result else {
                            continue;
                        };
                        let id = member.id;
                        if revision != self.revision.load(Ordering::Acquire) {
                            continue;
                        }
                        match result {
                            Ok((dialed, elapsed)) => {
                                if !self.record(&member, |plan| {
                                    plan.connected(id, elapsed, Instant::now().into_std())
                                }) {
                                    continue;
                                }
                                if winner.is_none() && reply.is_some() {
                                    winner = Some(id);
                                    let _ = reply
                                        .take()
                                        .unwrap()
                                        .send(Ok(self.decorate(member, dialed)));
                                }
                            }
                            Err(error) => {
                                if matches!(&error, DialError::Timeout { stage } if !stage.ends_with("preparation"))
                                {
                                    slow.push(id);
                                } else if error.is_evidence() {
                                    self.record(&member, |plan| {
                                        plan.failed(id, Instant::now().into_std())
                                    });
                                }
                                last = error;
                            }
                        }
                    }
                    let members_guard = self.members.read().expect("pool members");
                    if revision == self.revision.load(Ordering::Acquire) {
                        let mut plan = self.plan.lock().expect("pool plan");
                        if winner.is_none() {
                            for id in slow {
                                plan.failed(id, Instant::now().into_std());
                            }
                        }
                        plan.race_finished(
                            winner.ok_or_else(|| {
                                (std::io::ErrorKind::NotConnected, last.to_string())
                            }),
                            reason,
                            Instant::now().into_std(),
                        );
                    }
                    let changed = revision != self.revision.load(Ordering::Acquire);
                    drop(members_guard);
                    self.changed.notify_waiters();
                    if changed && reply.is_some() {
                        continue;
                    }
                    if let Some(reply) = reply {
                        let _ = reply.send(Err(self.pool_error(last)));
                    }
                    return;
                }
            }
        }
    }
}

pub struct PoolStage {
    state: Arc<PoolState>,
    tasks: Mutex<JoinSet<()>>,
    retirement: tokio::task::JoinHandle<()>,
    handle: tokio::runtime::Handle,
}
impl PoolStage {
    pub fn new(
        id: u32,
        members: Vec<(u32, Arc<dyn Dialer>)>,
        handle: tokio::runtime::Handle,
    ) -> Arc<Self> {
        let mut plan = CandidatePlan::default();
        plan.set_candidates(
            members.iter().map(|(id, _)| *id).collect(),
            Instant::now().into_std(),
        );
        let members = members
            .into_iter()
            .map(|(id, dialer)| {
                (
                    id,
                    Arc::new(Member {
                        id,
                        streams: weaver_tunnel::pipe::Revocable::new(dialer.clone()),
                        dialer,
                        status: Mutex::new(PoolMemberStatus {
                            id,
                            open: 0,
                            opening: 0,
                            session_handshake: None,
                            blocked: None,
                            warmed: false,
                        }),
                        idle_since: Mutex::new(Instant::now()),
                        session_gate: tokio::sync::Mutex::new(()),
                        retiring: AtomicBool::new(false),
                        outcomes: Mutex::new(Vec::new()),
                    }),
                )
            })
            .collect();
        let state = Arc::new(PoolState {
            id,
            plan: Mutex::new(plan),
            members: RwLock::new(members),
            changed: Notify::new(),
            stopped: AtomicBool::new(false),
            revision: AtomicU64::new(0),
        });
        let weak = Arc::downgrade(&state);
        let retirement = handle.spawn(async move {
            // Only sweeps for members idle past POOL_IDLE_RETIRE; it never
            // decides which member new connections use, so it stays outside
            // the plan timing.
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                let Some(state) = weak.upgrade() else {
                    return;
                };
                let pin = state.plan.lock().expect("pool plan").snapshot().pinned;
                let members: Vec<_> = state
                    .members
                    .read()
                    .expect("pool members")
                    .values()
                    .filter(|m| Some(m.id) != pin)
                    .cloned()
                    .collect();
                for member in members {
                    member.retire_if_idle().await;
                }
            }
        });
        Arc::new(Self {
            state,
            tasks: Mutex::new(JoinSet::new()),
            retirement,
            handle,
        })
    }
    pub fn snapshot(&self) -> (PlanSnapshot<u32>, Vec<PoolMemberStatus>) {
        let plan = self
            .state
            .plan
            .lock()
            .expect("pool plan")
            .snapshot_at(Instant::now().into_std());
        let mut members: Vec<_> = self
            .state
            .members
            .read()
            .expect("pool members")
            .values()
            .map(|m| m.status.lock().expect("pool member").clone())
            .collect();
        members.sort_by_key(|m| m.id);
        (plan, members)
    }
    pub fn delivered(&self, member: u32, bytes: u64, wire: Duration) {
        self.state.plan.lock().expect("pool plan").delivered(
            member,
            bytes,
            wire,
            Instant::now().into_std(),
        );
    }
    pub fn set_members(&self, next: Vec<(u32, Arc<dyn Dialer>)>) -> Vec<u32> {
        let mut members = self.state.members.write().expect("pool members");
        if members.len() == next.len()
            && next.iter().all(|(id, stage)| {
                members
                    .get(id)
                    .is_some_and(|m| Arc::ptr_eq(&m.dialer, stage))
            })
        {
            return Vec::new();
        }
        for (id, member) in members.iter() {
            if !next
                .iter()
                .any(|(next, stage)| next == id && Arc::ptr_eq(stage, &member.dialer))
            {
                member.recall();
            }
        }
        let removed = members
            .keys()
            .filter(|id| !next.iter().any(|(next, _)| next == *id))
            .copied()
            .collect();
        let order: Vec<_> = next.iter().map(|(id, _)| *id).collect();
        let next = next
            .into_iter()
            .map(|(id, dialer)| {
                let member = members
                    .get(&id)
                    .filter(|m| Arc::ptr_eq(&m.dialer, &dialer))
                    .cloned()
                    .unwrap_or_else(|| {
                        Arc::new(Member {
                            id,
                            streams: weaver_tunnel::pipe::Revocable::new(dialer.clone()),
                            dialer,
                            status: Mutex::new(PoolMemberStatus {
                                id,
                                open: 0,
                                opening: 0,
                                session_handshake: None,
                                blocked: None,
                                warmed: false,
                            }),
                            idle_since: Mutex::new(Instant::now()),
                            session_gate: tokio::sync::Mutex::new(()),
                            retiring: AtomicBool::new(false),
                            outcomes: Mutex::new(Vec::new()),
                        })
                    });
                (id, member)
            })
            .collect();
        *members = next;
        self.state.revision.fetch_add(1, Ordering::AcqRel);
        {
            let mut plan = self.state.plan.lock().expect("pool plan");
            plan.set_candidates(order, Instant::now().into_std());
            plan.replan();
        }
        drop(members);
        self.state.changed.notify_waiters();
        removed
    }
    pub fn prewarm(&self) {
        let members: Vec<_> = self
            .state
            .members
            .read()
            .expect("pool members")
            .values()
            .cloned()
            .collect();
        let mut tasks = self.tasks.lock().expect("pool tasks");
        while tasks.try_join_next().is_some() {}
        for member in members {
            tasks.spawn_on(
                async move {
                    let _ = member.warm().await;
                },
                &self.handle,
            );
        }
    }
    pub fn revision(&self) -> u64 {
        self.state.revision.load(Ordering::Acquire)
    }
    /// Each lease keeps the same member for DNS validation and its HTTP fetch.
    pub fn lease_members(&self) -> Vec<(u32, Arc<dyn Dialer>)> {
        let members = self.state.members.read().expect("pool members");
        let order = self.state.plan.lock().expect("pool plan").lease_order();
        order
            .into_iter()
            .filter_map(|id| members.get(&id))
            .filter(|member| member.status.lock().expect("pool member").blocked.is_none())
            .map(|member| {
                (
                    member.id,
                    Arc::new(MemberLease {
                        member: member.clone(),
                        state: self.state.clone(),
                        reservation: Mutex::new(None),
                    }) as Arc<dyn Dialer>,
                )
            })
            .collect()
    }
    pub fn lease_member(&self) -> Result<(u32, Arc<dyn Dialer>), DialError> {
        self.lease_members()
            .into_iter()
            .next()
            .ok_or_else(|| DialError::Skipped("no enabled pool member".into()))
    }
    pub fn lease(&self) -> Result<Arc<dyn Dialer>, DialError> {
        self.lease_member().map(|(_, stage)| stage)
    }
}
struct MemberLease {
    member: Arc<Member>,
    state: Arc<PoolState>,
    reservation: Mutex<Option<MemberOpening>>,
}
impl MemberLease {
    fn reserve(&self) {
        self.reservation
            .lock()
            .expect("member lease")
            .get_or_insert_with(|| MemberOpening::new(self.member.clone()));
    }
}
#[async_trait::async_trait]
impl Dialer for MemberLease {
    fn report_request(&self, error: Option<&DialError>) {
        self.state.record(&self.member, |plan| {
            let now = Instant::now().into_std();
            match error {
                None => plan.lease_succeeded(self.member.id, now),
                Some(error) if error.is_path_evidence() => plan.failed(self.member.id, now),
                _ => {}
            }
        });
    }
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        if self.state.stopped.load(Ordering::Acquire)
            || !self
                .state
                .members
                .read()
                .expect("pool members")
                .get(&self.member.id)
                .is_some_and(|m| Arc::ptr_eq(m, &self.member))
        {
            return Err(DialError::Skipped("pool member changed".into()));
        }
        self.reserve();
        let (dialed, elapsed) = self
            .member
            .dial(target)
            .await
            .map_err(|error| self.state.pool_error(error))?;
        if !self.state.record(&self.member, |plan| {
            plan.connected(self.member.id, elapsed, Instant::now().into_std())
        }) {
            return Err(DialError::Skipped("pool member changed".into()));
        }
        Ok(self.state.decorate(self.member.clone(), dialed))
    }
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        self.reserve();
        self.member.dialer.resolve(host).await
    }
    fn budget(&self) -> Duration {
        self.member.dialer.budget()
    }
    fn describe(&self) -> String {
        format!("pool {} member {}", self.state.id, self.member.id)
    }
}
impl Drop for PoolStage {
    fn drop(&mut self) {
        self.state.stopped.store(true, Ordering::Release);
        self.state.changed.notify_waiters();
        self.retirement.abort();
        self.tasks.get_mut().expect("pool tasks").abort_all();
    }
}
#[async_trait::async_trait]
impl Dialer for PoolStage {
    fn over_limit_cleared(&self) {
        self.state
            .plan
            .lock()
            .expect("pool plan")
            .over_limit_cleared(Instant::now().into_std());
    }
    async fn dial(&self, target: &Target) -> Result<Dialed, DialError> {
        if self.state.stopped.load(Ordering::Acquire) {
            return Err(DialError::Skipped("pool stopped".into()));
        }
        let (reply, answer) = oneshot::channel();
        {
            let mut tasks = self.tasks.lock().expect("pool tasks");
            while tasks.try_join_next().is_some() {}
            tasks.spawn_on(self.state.clone().run(target.clone(), reply), &self.handle);
        }
        answer
            .await
            .map_err(|_| DialError::Skipped("pool stopped".into()))?
    }
    async fn resolve(&self, host: &str) -> Result<Resolution, DialError> {
        self.lease()?.resolve(host).await
    }
    fn budget(&self) -> Duration {
        self.state
            .members
            .read()
            .expect("pool members")
            .values()
            .map(|m| m.dialer.budget().saturating_add(ADDRESS_ATTEMPT_LIMIT))
            .fold(Duration::ZERO, Duration::saturating_add)
    }
    async fn shutdown(&self) {
        self.state.stopped.store(true, Ordering::Release);
        self.state.changed.notify_waiters();
        self.retirement.abort();
        let mut tasks = std::mem::take(&mut *self.tasks.lock().expect("pool tasks"));
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    fn describe(&self) -> String {
        format!("proxy pool {}", self.state.id)
    }
}

#[cfg(test)]
#[path = "pool_stage_tests.rs"]
mod tests;
