use super::*;

use crate::HardwareProfileInForce;
use crate::runtime::HardwareProfile;

impl Pipeline {
    /// Make `profile` the operator's choice. A profile this machine cannot
    /// honour is never offered, so one arriving here means the machine shrank
    /// since it was offered; the recommendation stands in, as it does at
    /// startup.
    pub(super) fn set_configured_hardware_profile(&mut self, profile: HardwareProfile) {
        let probe = self.tuner.system_profile();
        self.configured_hardware_profile = match profile.unmet_requirement(probe) {
            None => profile,
            Some(requirement) => {
                let recommended = HardwareProfile::recommended(probe);
                warn!(
                    requested = profile.as_str(),
                    using = recommended.as_str(),
                    requirement,
                    "hardware profile unavailable on this machine; using the recommendation"
                );
                recommended
            }
        };
        self.apply_hardware_profile_in_force();
    }

    /// Record the profile the schedule asks for, or `None` when no schedule
    /// rule asks for one. A profile this machine cannot honour is skipped:
    /// the operator's choice stays in force and the skip is logged once here,
    /// since the schedule only sends a profile when it changes.
    pub(super) fn set_scheduled_hardware_profile(&mut self, profile: Option<HardwareProfile>) {
        if let Some(profile) = profile
            && let Some(requirement) = profile.unmet_requirement(self.tuner.system_profile())
        {
            warn!(
                scheduled = profile.as_str(),
                using = self.configured_hardware_profile.as_str(),
                requirement,
                "scheduled hardware profile unavailable on this machine; keeping the chosen profile"
            );
        }
        self.scheduled_hardware_profile = profile;
        self.apply_hardware_profile_in_force();
    }

    /// The scheduled profile when there is one this machine can honour, the
    /// operator's choice otherwise.
    fn hardware_profile_in_force(&self) -> HardwareProfileInForce {
        let probe = self.tuner.system_profile();
        let scheduled = self
            .scheduled_hardware_profile
            .filter(|profile| profile.unmet_requirement(probe).is_none());
        HardwareProfileInForce {
            active: scheduled.unwrap_or(self.configured_hardware_profile),
            scheduled,
        }
    }

    /// Put the limits of the profile in force behind everything that starts
    /// from now on.
    ///
    /// Nothing running is stopped or resized. Each limit is taken where an
    /// activity starts: a download reads the tuner when it is dispatched, a
    /// decode when it is spawned, an extraction or repair clones the pool,
    /// the extraction limits and its memory-budget view when it is admitted.
    /// Replacing them here therefore leaves running work with what it started
    /// on and gives the next activity the new values. A replaced pool is
    /// dropped when the last activity holding it finishes.
    fn apply_hardware_profile_in_force(&mut self) {
        let in_force = self.hardware_profile_in_force();
        self.shared_state.set_hardware_profile_in_force(in_force);
        let tuning = in_force.active.tuning(self.tuner.system_profile());
        if tuning == self.tuner.profile_tuning() {
            return;
        }

        let prior_extract_threads = self.tuner.params().extract_thread_count;
        self.tuner.set_profile_tuning(tuning);
        self.shared_state
            .set_sevenz_decode_memory_bytes(tuning.sevenz_decode_memory_bytes);

        let limits = self
            .extraction_limits
            .with_profile_ceiling(tuning.extraction_memory_bytes);
        if limits.max_memory_bytes != self.extraction_limits.max_memory_bytes {
            self.process_memory_budget = Arc::new(
                self.process_memory_budget
                    .with_limit(limits.max_memory_bytes),
            );
            self.extraction_limits = Arc::new(limits);
        }

        // Both pools are sized from the same count. A chase already running
        // on the old chase pool still counts as occupied when a new one asks
        // for a worker, so admission against the new pool's size stays
        // conservative until the old chases end.
        let extract_threads = self.tuner.params().extract_thread_count;
        if extract_threads != prior_extract_threads {
            self.pp_pool =
                crate::runtime::postprocess_pool::build_postprocess_pool(extract_threads);
            self.chase_pool =
                crate::runtime::postprocess_pool::build_postprocess_pool(extract_threads);
        }

        info!(
            hardware_profile = in_force.active.as_str(),
            scheduled = in_force.scheduled.is_some(),
            max_downloads = self.tuner.params().max_concurrent_downloads,
            decode_threads = self.tuner.params().decode_thread_count,
            extract_threads,
            sevenz_decode_memory_mb = tuning.sevenz_decode_memory_bytes / (1024 * 1024),
            extraction_memory_mb = self.extraction_limits.max_memory_bytes / (1024 * 1024),
            "hardware profile in force changed; applies to work that starts from now on"
        );

        // A raised download cap is used at once rather than at the next
        // completion.
        self.dispatch_downloads();
    }
}
