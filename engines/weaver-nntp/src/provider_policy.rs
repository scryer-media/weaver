// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Article eligibility deliberately has no connection-load or local-permit
// input. Those decide when eligible work runs, never its fallback level.

use crate::server_set::ServerSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProviderGroup {
    pub backfill: bool,
    pub priority: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProviderState {
    pub group: ProviderGroup,
    pub available: bool,
    pub refuses_credentials: bool,
    // Remaining quota before local reservations. In-flight reservations are
    // backpressure, not evidence that a provider cannot supply an article.
    pub quota_remaining: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArticleEligibility {
    Eligible(ProviderGroup),
    Excluded,
    Unavailable,
    // Up and taking this pool's credentials, but short of the article's
    // estimate in its remaining quota.
    OutOfQuota,
    HigherPriority(ProviderGroup),
    BackfillProtected,
}

// Whether the backfill tier is open for one article: every fill server is
// excluded for it, because it answered that it does not have the article or
// it is past the job's retention. This is the only rule that opens backfill.
// A fill server that is down, unroutable, out of quota or refusing this
// pool's credentials does not count: backfill fills articles the fill
// servers do not have, it never stands in for a fill server that is not
// answering. Priority levels are the fallback for that.
pub(crate) fn backfill_unlocked(fill: ServerSet, excluded: ServerSet) -> bool {
    excluded.covers(fill)
}

// The policy's servers as sets, built once per sample, so that one article's
// answer is a few word operations per priority level instead of a walk of
// every server per server asked about.
#[derive(Debug, Clone)]
struct PolicyIndex {
    // Servers that are not backfill.
    fill: ServerSet,
    // Servers available and taking this pool's credentials.
    usable: ServerSet,
    // Servers with no quota to run short of.
    unlimited: ServerSet,
    // The servers whose quota an article's estimate is weighed against.
    limited: ServerSet,
    // Each group present, best first, with its servers.
    levels: PolicyLevels,
}

// A pool's priority levels: each group present, best first, with its
// servers. A pool's groups never change while it lives, so the pool builds
// this once and every policy sampled from it shares it.
#[derive(Debug, Clone)]
pub(crate) struct PolicyLevels(std::sync::Arc<[(ProviderGroup, ServerSet)]>);

impl PolicyLevels {
    pub(crate) fn of(groups: impl IntoIterator<Item = ProviderGroup>) -> Self {
        let mut levels: Vec<(ProviderGroup, ServerSet)> = Vec::new();
        for (idx, group) in groups.into_iter().enumerate() {
            assert!(
                idx < ServerSet::CAPACITY,
                "a pool holds at most {} servers",
                ServerSet::CAPACITY
            );
            match levels.binary_search_by(|(level, _)| level.cmp(&group)) {
                Ok(at) => {
                    levels[at].1.insert(idx);
                }
                Err(at) => levels.insert(at, (group, ServerSet::of(idx))),
            }
        }
        Self(levels.into())
    }
}

impl PolicyIndex {
    // `levels` are those of `servers`' groups.
    fn of(servers: &[ProviderState], levels: PolicyLevels) -> Self {
        assert!(
            servers.len() <= ServerSet::CAPACITY,
            "a pool holds at most {} servers",
            ServerSet::CAPACITY
        );
        let mut index = Self {
            fill: ServerSet::EMPTY,
            usable: ServerSet::EMPTY,
            unlimited: ServerSet::EMPTY,
            limited: ServerSet::EMPTY,
            levels,
        };
        for (idx, state) in servers.iter().enumerate() {
            if !state.group.backfill {
                index.fill.insert(idx);
            }
            if state.available && !state.refuses_credentials {
                index.usable.insert(idx);
            }
            if state.quota_remaining == u64::MAX {
                index.unlimited.insert(idx);
            } else {
                index.limited.insert(idx);
            }
        }
        index
    }

    // The servers with quota left for an article of `bytes`.
    fn quota_ok(&self, servers: &[ProviderState], bytes: u64) -> ServerSet {
        let mut quota_ok = self.unlimited;
        for idx in self.limited.iter() {
            if servers[idx].quota_remaining >= bytes.max(1) {
                quota_ok.insert(idx);
            }
        }
        quota_ok
    }

    // The servers an article excluded from `excluded` could be asked of
    // now, at the best group any of them is in. Empty when none could.
    fn eligible(&self, servers: &[ProviderState], excluded: ServerSet, bytes: u64) -> ServerSet {
        let mut candidates = self
            .usable
            .difference(excluded)
            .intersection(self.quota_ok(servers, bytes));
        if !backfill_unlocked(self.fill, excluded) {
            candidates = candidates.intersection(self.fill);
        }
        self.levels
            .0
            .iter()
            .map(|(_, level)| level.intersection(candidates))
            .find(|level| !level.is_empty())
            .unwrap_or(ServerSet::EMPTY)
    }
}

#[derive(Debug, Clone)]
pub struct ProviderPolicy {
    pub(crate) servers: Vec<ProviderState>,
    index: PolicyIndex,
}

// Two samples are the same policy when every server stands the same; the
// index is derived from that.
impl PartialEq for ProviderPolicy {
    fn eq(&self, other: &Self) -> bool {
        self.servers == other.servers
    }
}

impl ProviderPolicy {
    #[cfg(test)]
    pub(crate) fn new(servers: Vec<ProviderState>) -> Self {
        let levels = PolicyLevels::of(servers.iter().map(|state| state.group));
        Self::sampled(servers, levels)
    }

    // A sample of a pool's servers, over the levels the pool built for them.
    pub(crate) fn sampled(servers: Vec<ProviderState>, levels: PolicyLevels) -> Self {
        let index = PolicyIndex::of(&servers, levels);
        Self { servers, index }
    }

    // The servers that are not backfill.
    pub fn fill(&self) -> ServerSet {
        self.index.fill
    }

    // The servers that could answer an article now: available and taking
    // this pool's credentials, whatever their quota.
    pub fn usable(&self) -> ServerSet {
        self.index.usable
    }

    pub fn eligible_group(
        &self,
        failures: ServerSet,
        retention: ServerSet,
        bytes: u64,
    ) -> Option<ProviderGroup> {
        self.eligible_servers(failures, retention, bytes)
            .lowest()
            .map(|server| self.servers[server].group)
    }

    // The servers an article may be asked of now: those of the best group
    // any server that may serve it is in.
    pub fn eligible_servers(
        &self,
        failures: ServerSet,
        retention: ServerSet,
        bytes: u64,
    ) -> ServerSet {
        self.index
            .eligible(&self.servers, failures.union(retention), bytes)
    }

    // Whether any of `servers` other than `except` could be asked for an
    // article now: what [`Self::eligibility`] answers `Eligible` for.
    pub fn any_eligible(
        &self,
        servers: ServerSet,
        except: usize,
        failures: ServerSet,
        retention: ServerSet,
        bytes: u64,
    ) -> bool {
        let mut asked = servers;
        asked.remove(except);
        self.eligible_servers(failures, retention, bytes)
            .intersects(asked)
    }

    // Whether `self` and `other` answer [`Self::eligibility`] for `server`
    // alike for every article whose estimate is at most `weighed`, and,
    // with `whole_pool`, [`Self::eligible_servers`] alike for every such
    // article as well. Exact, never a guess in either direction:
    //
    // - `server`'s answer reads its own standing and the servers of better
    //   groups (a candidate there makes it `HigherPriority`); servers of its
    //   own or worse groups never change it. Backfill groups sort after
    //   every fill group, so a backfill server weighs every fill server.
    // - A quota is weighed only as `remaining >= estimate.max(1)`, so two
    //   remaining figures are alike for every estimate up to `weighed` when
    //   they agree once capped at one byte past it. A server charged bytes
    //   while it still holds more than the largest article weighed answers
    //   the same.
    pub fn answers_alike(
        &self,
        other: &Self,
        server: usize,
        whole_pool: bool,
        weighed: u64,
    ) -> bool {
        if self.servers.len() != other.servers.len()
            || self
                .servers
                .iter()
                .zip(&other.servers)
                .any(|(left, right)| left.group != right.group)
        {
            return false;
        }
        let Some(own) = self.servers.get(server) else {
            // Neither sample has the server: both answer `Unavailable`.
            return !whole_pool || self.servers == other.servers;
        };
        let ceiling = weighed.max(1).saturating_add(1);
        self.servers
            .iter()
            .zip(&other.servers)
            .enumerate()
            .filter(|(idx, (state, _))| whole_pool || *idx == server || state.group < own.group)
            .all(|(_, (left, right))| {
                left.available == right.available
                    && left.refuses_credentials == right.refuses_credentials
                    && left.quota_remaining.min(ceiling) == right.quota_remaining.min(ceiling)
            })
    }

    pub fn eligibility(
        &self,
        server: usize,
        failures: ServerSet,
        retention: ServerSet,
        bytes: u64,
    ) -> ArticleEligibility {
        if failures.contains(server) || retention.contains(server) {
            return ArticleEligibility::Excluded;
        }
        let Some(state) = self.servers.get(server) else {
            return ArticleEligibility::Unavailable;
        };
        if !state.available || state.refuses_credentials {
            return ArticleEligibility::Unavailable;
        }
        if state.quota_remaining < bytes.max(1) {
            return ArticleEligibility::OutOfQuota;
        }
        match self.eligible_group(failures, retention, bytes) {
            Some(group) if group == state.group => ArticleEligibility::Eligible(group),
            Some(group) => ArticleEligibility::HigherPriority(group),
            None if state.group.backfill => ArticleEligibility::BackfillProtected,
            None => ArticleEligibility::Unavailable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The servers, to change; the policy's bit sets are rebuilt when the
    // change is done.
    struct Edit<'a>(&'a mut ProviderPolicy);

    impl std::ops::Deref for Edit<'_> {
        type Target = Vec<ProviderState>;
        fn deref(&self) -> &Self::Target {
            &self.0.servers
        }
    }

    impl std::ops::DerefMut for Edit<'_> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0.servers
        }
    }

    impl Drop for Edit<'_> {
        fn drop(&mut self) {
            let levels = PolicyLevels::of(self.0.servers.iter().map(|state| state.group));
            self.0.index = PolicyIndex::of(&self.0.servers, levels);
        }
    }

    impl ProviderPolicy {
        fn edit(&mut self) -> Edit<'_> {
            Edit(self)
        }
    }

    fn policy() -> ProviderPolicy {
        ProviderPolicy::new(
            (0..24)
                .map(|idx| ProviderState {
                    group: ProviderGroup {
                        backfill: idx >= 16,
                        priority: (idx / 8) as u32,
                    },
                    available: true,
                    refuses_credentials: false,
                    quota_remaining: u64::MAX,
                })
                .collect(),
        )
    }

    // The answers as Weaver gave them before the sets: a walk of every server
    // over index lists, which the sets are held to.
    fn walked_group(
        policy: &ProviderPolicy,
        failures: &[usize],
        retention: &[usize],
        bytes: u64,
    ) -> Option<ProviderGroup> {
        let excluded = |idx: usize| failures.contains(&idx) || retention.contains(&idx);
        let backfill_unlocked = policy
            .servers
            .iter()
            .enumerate()
            .all(|(idx, state)| state.group.backfill || excluded(idx));
        policy
            .servers
            .iter()
            .enumerate()
            .filter(|(idx, state)| {
                !excluded(*idx)
                    && state.available
                    && !state.refuses_credentials
                    && state.quota_remaining >= bytes.max(1)
                    && (!state.group.backfill || backfill_unlocked)
            })
            .map(|(_, state)| state.group)
            .min()
    }

    fn walked_eligibility(
        policy: &ProviderPolicy,
        server: usize,
        failures: &[usize],
        retention: &[usize],
        bytes: u64,
    ) -> ArticleEligibility {
        if failures.contains(&server) || retention.contains(&server) {
            return ArticleEligibility::Excluded;
        }
        let Some(state) = policy.servers.get(server) else {
            return ArticleEligibility::Unavailable;
        };
        if !state.available || state.refuses_credentials {
            return ArticleEligibility::Unavailable;
        }
        if state.quota_remaining < bytes.max(1) {
            return ArticleEligibility::OutOfQuota;
        }
        match walked_group(policy, failures, retention, bytes) {
            Some(group) if group == state.group => ArticleEligibility::Eligible(group),
            Some(group) => ArticleEligibility::HigherPriority(group),
            None if state.group.backfill => ArticleEligibility::BackfillProtected,
            None => ArticleEligibility::Unavailable,
        }
    }

    fn set(servers: &[usize]) -> ServerSet {
        ServerSet::from_indices(servers)
    }

    const NONE: ServerSet = ServerSet::EMPTY;

    // A small deterministic generator, so the seeded sets are the same on
    // every run.
    struct Seeded(u64);

    impl Seeded {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, bound: u64) -> u64 {
            self.next() % bound
        }
    }

    // 25 providers over 10 levels: levels 0-6 fill, 7-9 backfill, some
    // servers down, one refusing credentials, two with a quota.
    fn twenty_five_on_ten_levels() -> ProviderPolicy {
        ProviderPolicy::new(
            (0..25usize)
                .map(|idx| ProviderState {
                    group: ProviderGroup {
                        backfill: idx % 10 >= 7,
                        priority: (idx % 10) as u32,
                    },
                    available: idx % 6 != 5,
                    refuses_credentials: idx == 13,
                    quota_remaining: match idx {
                        3 => 400,
                        11 => 100_000,
                        _ => u64::MAX,
                    },
                })
                .collect(),
        )
    }

    // Exclusion sets drawn over every server a pool may hold, against pools
    // of 25 providers over 10 levels and of the 50-provider cap.
    fn sets_answer_as_the_walk(mut policy: ProviderPolicy, seed: u64) {
        let count = policy.servers.len() as u64;
        let mut rng = Seeded(seed);
        for round in 0..4_000 {
            // Now and then a server goes down or comes back.
            if round % 500 == 499 {
                let idx = rng.below(count) as usize;
                let mut servers = policy.edit();
                servers[idx].available = !servers[idx].available;
            }
            let pick = |rng: &mut Seeded| -> Vec<usize> {
                let mut picked: Vec<usize> = (0..rng.below(count + 1))
                    .map(|_| rng.below(count + 2) as usize)
                    .collect();
                picked.sort_unstable();
                picked.dedup();
                picked
            };
            let failures = pick(&mut rng);
            let retention = if rng.below(4) == 0 {
                pick(&mut rng)
            } else {
                Vec::new()
            };
            let bytes = [0, 1, 300, 512, 50_000, 200_000][rng.below(6) as usize];
            assert_eq!(
                policy.eligible_group(set(&failures), set(&retention), bytes),
                walked_group(&policy, &failures, &retention, bytes),
                "round {round}: {failures:?} {retention:?} {bytes}"
            );
            for server in 0..count as usize + 2 {
                assert_eq!(
                    policy.eligibility(server, set(&failures), set(&retention), bytes),
                    walked_eligibility(&policy, server, &failures, &retention, bytes),
                    "round {round}: server {server}, {failures:?} {retention:?} {bytes}"
                );
            }
            let asked: Vec<usize> = (0..rng.below(count))
                .map(|_| rng.below(count) as usize)
                .collect();
            let except = rng.below(count) as usize;
            let walked = asked.iter().any(|server| {
                *server != except
                    && matches!(
                        walked_eligibility(&policy, *server, &failures, &retention, bytes),
                        ArticleEligibility::Eligible(_)
                    )
            });
            assert_eq!(
                policy.any_eligible(set(&asked), except, set(&failures), set(&retention), bytes),
                walked
            );
        }
    }

    #[test]
    fn sets_answer_as_the_walk_for_25_providers_on_10_levels() {
        sets_answer_as_the_walk(twenty_five_on_ten_levels(), 0x9e37_79b9_7f4a_7c15);
    }

    #[test]
    fn sets_answer_as_the_walk_for_50_providers_on_10_levels() {
        let policy = ProviderPolicy::new(
            (0..crate::MAX_PROVIDERS)
                .map(|idx| ProviderState {
                    group: ProviderGroup {
                        backfill: idx % 10 >= 7,
                        priority: (idx % 10) as u32,
                    },
                    available: idx % 6 != 5,
                    refuses_credentials: idx == 13 || idx == 48,
                    quota_remaining: match idx {
                        3 => 400,
                        11 | 49 => 100_000,
                        _ => u64::MAX,
                    },
                })
                .collect(),
        );
        sets_answer_as_the_walk(policy, 0x2545_f491_4f6c_dd1d);
    }

    #[test]
    #[should_panic(expected = "a pool holds at most 64 servers")]
    fn a_pool_beyond_the_set_is_refused() {
        ProviderPolicy::new(
            (0..=ServerSet::CAPACITY)
                .map(|_| ProviderState {
                    group: ProviderGroup {
                        backfill: false,
                        priority: 0,
                    },
                    available: true,
                    refuses_credentials: false,
                    quota_remaining: u64::MAX,
                })
                .collect(),
        );
    }

    #[test]
    fn twenty_four_providers_keep_every_fresh_article_in_the_primary_group() {
        let policy = policy();
        for _ in 0..124 {
            for server in 0..24 {
                assert_eq!(
                    matches!(
                        policy.eligibility(server, NONE, NONE, 512),
                        ArticleEligibility::Eligible(_)
                    ),
                    server < 8
                );
            }
        }
    }

    #[test]
    fn missing_articles_and_retention_advance_only_the_affected_article() {
        let policy = policy();
        let primary: Vec<_> = (0..8).collect();
        assert!(matches!(
            policy.eligibility(8, set(&primary), NONE, 512),
            ArticleEligibility::Eligible(_)
        ));
        assert!(matches!(
            policy.eligibility(8, NONE, set(&primary), 512),
            ArticleEligibility::Eligible(_)
        ));
        assert!(matches!(
            policy.eligibility(8, NONE, NONE, 512),
            ArticleEligibility::HigherPriority(_)
        ));
    }

    #[test]
    fn outages_unlock_ordinary_fallback_but_never_backfill() {
        let mut policy = policy();
        for state in &mut policy.edit()[..8] {
            state.available = false;
        }
        assert!(matches!(
            policy.eligibility(8, NONE, NONE, 512),
            ArticleEligibility::Eligible(_)
        ));
        for state in &mut policy.edit()[8..16] {
            state.available = false;
        }
        assert_eq!(
            policy.eligibility(16, NONE, NONE, 512),
            ArticleEligibility::BackfillProtected
        );
        for state in &mut policy.edit()[..16] {
            state.refuses_credentials = true;
        }
        assert_eq!(
            policy.eligibility(16, NONE, NONE, 512),
            ArticleEligibility::BackfillProtected
        );
    }

    fn fill_plus_backfill() -> ProviderPolicy {
        ProviderPolicy::new(
            [false, true]
                .into_iter()
                .map(|backfill| ProviderState {
                    group: ProviderGroup {
                        backfill,
                        priority: 0,
                    },
                    available: true,
                    refuses_credentials: false,
                    quota_remaining: u64::MAX,
                })
                .collect(),
        )
    }

    #[test]
    fn a_fill_server_refusing_credentials_never_opens_backfill() {
        let mut policy = fill_plus_backfill();
        policy.edit()[0].refuses_credentials = true;
        assert_eq!(
            policy.eligibility(1, NONE, NONE, 512),
            ArticleEligibility::BackfillProtected
        );
        assert_eq!(policy.eligible_group(NONE, NONE, 512), None);
    }

    #[test]
    fn a_fill_server_without_the_article_opens_backfill() {
        let policy = fill_plus_backfill();
        assert!(matches!(
            policy.eligibility(1, set(&[0]), NONE, 512),
            ArticleEligibility::Eligible(ProviderGroup { backfill: true, .. })
        ));
        assert!(matches!(
            policy.eligibility(1, NONE, set(&[0]), 512),
            ArticleEligibility::Eligible(ProviderGroup { backfill: true, .. })
        ));
    }

    #[test]
    fn a_level_refusing_credentials_falls_through_to_the_next_level() {
        let mut policy = fill_plus_backfill();
        policy.edit()[1].group = ProviderGroup {
            backfill: false,
            priority: 1,
        };
        policy.edit()[0].refuses_credentials = true;
        assert_eq!(
            policy.eligibility(1, NONE, NONE, 512),
            ArticleEligibility::Eligible(ProviderGroup {
                backfill: false,
                priority: 1,
            })
        );
    }

    #[test]
    fn spent_quota_unlocks_ordinary_fallback_and_recovery_restores_primary() {
        let mut policy = policy();
        for state in &mut policy.edit()[..8] {
            state.quota_remaining = 511;
        }
        assert_eq!(
            policy.eligibility(0, NONE, NONE, 512),
            ArticleEligibility::OutOfQuota
        );
        policy.edit()[1].available = false;
        assert_eq!(
            policy.eligibility(1, NONE, NONE, 512),
            ArticleEligibility::Unavailable
        );
        policy.edit()[2].refuses_credentials = true;
        assert_eq!(
            policy.eligibility(2, NONE, NONE, 512),
            ArticleEligibility::Unavailable
        );
        assert!(matches!(
            policy.eligibility(8, NONE, NONE, 512),
            ArticleEligibility::Eligible(_)
        ));
        policy.edit()[0].quota_remaining = 512;
        assert!(matches!(
            policy.eligibility(8, NONE, NONE, 512),
            ArticleEligibility::HigherPriority(_)
        ));
    }

    // Two samples `answers_alike` calls alike answer every article up to the
    // weighed estimate alike, over seeded changes to availability,
    // credentials and quotas; and bytes charged to a server holding more
    // than that estimate leave the samples alike.
    #[test]
    fn samples_called_alike_answer_alike() {
        let mut rng = Seeded(0x00a1_1ce5);
        let base = twenty_five_on_ten_levels();
        let count = base.servers.len();
        let mut alike_seen = [0u32; 2];
        for round in 0..3_000 {
            let mut other = base.clone();
            {
                let mut servers = other.edit();
                for _ in 0..rng.below(3) {
                    let idx = rng.below(count as u64) as usize;
                    match rng.below(4) {
                        0 => servers[idx].available = !servers[idx].available,
                        1 => servers[idx].refuses_credentials = !servers[idx].refuses_credentials,
                        2 => servers[idx].quota_remaining = rng.below(1_000),
                        _ => {
                            let state = &mut servers[idx];
                            if state.quota_remaining != u64::MAX {
                                state.quota_remaining =
                                    state.quota_remaining.saturating_sub(rng.below(300));
                            }
                        }
                    }
                }
            }
            let server = rng.below(count as u64) as usize;
            let whole_pool = rng.below(2) == 0;
            let weighed = [0, 1, 300, 399, 400, 512, 50_000][rng.below(7) as usize];
            if !base.answers_alike(&other, server, whole_pool, weighed) {
                continue;
            }
            alike_seen[usize::from(whole_pool)] += 1;
            for _ in 0..40 {
                let failures: Vec<usize> = (0..rng.below(8))
                    .map(|_| rng.below(count as u64) as usize)
                    .collect();
                let bytes = rng.below(weighed + 1);
                assert_eq!(
                    base.eligibility(server, set(&failures), NONE, bytes),
                    other.eligibility(server, set(&failures), NONE, bytes),
                    "round {round}: server {server}, {failures:?}, {bytes} of {weighed}"
                );
                if whole_pool {
                    assert_eq!(
                        base.eligible_servers(set(&failures), NONE, bytes),
                        other.eligible_servers(set(&failures), NONE, bytes),
                        "round {round}: {failures:?}, {bytes} of {weighed}"
                    );
                }
            }
        }
        assert!(alike_seen.iter().all(|seen| *seen > 100), "{alike_seen:?}");

        // Server 11 holds 100,000 bytes of quota: charging it 40,000 leaves
        // every article up to 50,000 bytes answered alike, for itself and
        // for every server it could outrank.
        let mut charged = base.clone();
        charged.edit()[11].quota_remaining = 60_000;
        for server in 0..count {
            assert!(base.answers_alike(&charged, server, true, 50_000));
        }
        // Charged below the estimate weighed, it is not.
        charged.edit()[11].quota_remaining = 40_000;
        assert!(!base.answers_alike(&charged, 11, false, 50_000));
        // A server going down changes nothing for a server of a better
        // group: server 0 (priority 0) is outranked by no one.
        let mut down = base.clone();
        down.edit()[4].available = false;
        assert!(base.answers_alike(&down, 0, false, 50_000));
        assert!(!base.answers_alike(&down, 4, false, 50_000));
        assert!(!base.answers_alike(&down, 7, false, 50_000));
    }
}
