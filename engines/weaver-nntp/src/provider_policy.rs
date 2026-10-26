// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Article eligibility deliberately has no connection-load or local-permit
// input. Those decide when eligible work runs, never its fallback level.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProviderGroup {
    pub backfill: bool,
    pub priority: u32,
}

#[derive(Debug, Clone)]
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
    HigherPriority(ProviderGroup),
    BackfillProtected,
}

#[derive(Debug, Clone)]
pub struct ProviderPolicy {
    pub(crate) servers: Vec<ProviderState>,
}

impl ProviderPolicy {
    pub fn eligible_group(
        &self,
        failures: &[usize],
        retention: &[usize],
        bytes: u64,
    ) -> Option<ProviderGroup> {
        let excluded = |idx: usize| failures.contains(&idx) || retention.contains(&idx);
        let backfill_unlocked =
            self.servers.iter().enumerate().all(|(idx, state)| {
                state.group.backfill || excluded(idx) || state.refuses_credentials
            });
        self.servers
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

    pub fn eligibility(
        &self,
        server: usize,
        failures: &[usize],
        retention: &[usize],
        bytes: u64,
    ) -> ArticleEligibility {
        if failures.contains(&server) || retention.contains(&server) {
            return ArticleEligibility::Excluded;
        }
        let Some(state) = self.servers.get(server) else {
            return ArticleEligibility::Unavailable;
        };
        if !state.available || state.refuses_credentials || state.quota_remaining < bytes.max(1) {
            return ArticleEligibility::Unavailable;
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

    fn policy() -> ProviderPolicy {
        ProviderPolicy {
            servers: (0..24)
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
        }
    }

    #[test]
    fn twenty_four_providers_keep_every_fresh_article_in_the_primary_group() {
        let policy = policy();
        for _ in 0..124 {
            for server in 0..24 {
                assert_eq!(
                    matches!(
                        policy.eligibility(server, &[], &[], 512),
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
            policy.eligibility(8, &primary, &[], 512),
            ArticleEligibility::Eligible(_)
        ));
        assert!(matches!(
            policy.eligibility(8, &[], &primary, 512),
            ArticleEligibility::Eligible(_)
        ));
        assert!(matches!(
            policy.eligibility(8, &[], &[], 512),
            ArticleEligibility::HigherPriority(_)
        ));
    }

    #[test]
    fn outages_unlock_ordinary_fallback_but_never_backfill() {
        let mut policy = policy();
        for state in &mut policy.servers[..8] {
            state.available = false;
        }
        assert!(matches!(
            policy.eligibility(8, &[], &[], 512),
            ArticleEligibility::Eligible(_)
        ));
        for state in &mut policy.servers[8..16] {
            state.available = false;
        }
        assert_eq!(
            policy.eligibility(16, &[], &[], 512),
            ArticleEligibility::BackfillProtected
        );
        for state in &mut policy.servers[..16] {
            state.refuses_credentials = true;
        }
        assert!(matches!(
            policy.eligibility(16, &[], &[], 512),
            ArticleEligibility::Eligible(_)
        ));
    }

    #[test]
    fn spent_quota_unlocks_ordinary_fallback_and_recovery_restores_primary() {
        let mut policy = policy();
        for state in &mut policy.servers[..8] {
            state.quota_remaining = 511;
        }
        assert!(matches!(
            policy.eligibility(8, &[], &[], 512),
            ArticleEligibility::Eligible(_)
        ));
        policy.servers[0].quota_remaining = 512;
        assert!(matches!(
            policy.eligibility(8, &[], &[], 512),
            ArticleEligibility::HigherPriority(_)
        ));
    }
}
