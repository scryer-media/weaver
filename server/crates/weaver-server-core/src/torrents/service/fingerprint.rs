// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! A view's fingerprint, so a follower can tell a torrent that moved from
//! one that only aged: some fields move on every reading with the clock
//! alone, and a follower that does not show them leaves them out.

use std::hash::{DefaultHasher, Hash, Hasher};

use super::TorrentView;
use crate::settings::{Leaf, Seeding, SeedingPolicy};

/// Which of a view's fields a fingerprint covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ViewParts {
    /// Every field.
    Whole,
    /// Every field but those that move with the clock alone.
    Steady,
}

impl TorrentView {
    /// A hash of the fields `parts` covers: two views with the same
    /// fingerprint read the same in those fields.
    pub fn fingerprint(&self, parts: ViewParts) -> u64 {
        // Every field is named, so a new one is a compile error here until
        // it is put on one side or the other.
        let TorrentView {
            job_id,
            name,
            infohash,
            infohash_v1,
            infohash_v2,
            version,
            magnet,
            source,
            state,
            detail,
            route_reason,
            paused_by,
            category,
            scope,
            private,
            forced,
            progress,
            check_progress,
            total_bytes,
            have_bytes,
            wanted_bytes,
            wanted_have_bytes,
            uploaded_bytes,
            downloaded_bytes,
            run_uploaded_bytes,
            run_downloaded_bytes,
            ratio,
            download_bps,
            upload_bps,
            upload_limit,
            download_limit,
            peers,
            seeds,
            leechers,
            max_peers,
            swarm_seeds,
            swarm_leechers,
            wasted_bytes,
            pieces,
            created_by,
            created_at_ms,
            comment,
            added_at_ms,
            completed_at_ms,
            storage_dir,
            files_dir,
            queue_position,
            seeding_override,
            seeding_policy,
            seeding_levels,
            tracker_rule,
            upload_cap,
            unpack,
            unpack_override,
            route,
            files,
            sequential,
            first_last_piece_first,
            file_have,
            trackers,
            metadata,
            tags,
            // The fields that move with the clock alone: the time run and
            // seeded, the averages over that time, the countdowns, the
            // seeding goal's time left, and when it was last seen complete,
            // which is now while it is.
            active_seconds,
            seeding_seconds,
            consecutive_seeding_seconds,
            download_avg_bps,
            upload_avg_bps,
            reannounce_seconds,
            eta_seconds,
            seeding_left,
            last_seen_complete_ms,
            last_activity_ms,
        } = self;
        let mut hasher = DefaultHasher::new();
        let hasher = &mut hasher;
        (
            job_id,
            name,
            infohash,
            infohash_v1,
            infohash_v2,
            version,
            magnet,
        )
            .hash(hasher);
        (
            source,
            state,
            detail,
            route_reason,
            paused_by,
            category,
            scope,
        )
            .hash(hasher);
        (private, forced, progress.to_bits(), total_bytes, have_bytes).hash(hasher);
        check_progress.map(f64::to_bits).hash(hasher);
        (
            wanted_bytes,
            wanted_have_bytes,
            uploaded_bytes,
            downloaded_bytes,
        )
            .hash(hasher);
        (run_uploaded_bytes, run_downloaded_bytes, ratio.to_bits()).hash(hasher);
        (download_bps, upload_bps, upload_limit, download_limit).hash(hasher);
        (
            peers,
            seeds,
            leechers,
            max_peers,
            swarm_seeds,
            swarm_leechers,
        )
            .hash(hasher);
        (wasted_bytes, pieces, created_by, created_at_ms, comment).hash(hasher);
        (
            added_at_ms,
            completed_at_ms,
            storage_dir,
            files_dir,
            queue_position,
        )
            .hash(hasher);
        seeding_override.as_ref().map(policy_key).hash(hasher);
        seeding_key(seeding_policy).hash(hasher);
        (
            seeding_levels,
            tracker_rule,
            upload_cap,
            unpack,
            unpack_override,
        )
            .hash(hasher);
        (route, files, sequential, first_last_piece_first, file_have).hash(hasher);
        (trackers, metadata, tags).hash(hasher);
        if parts == ViewParts::Whole {
            (active_seconds, seeding_seconds, consecutive_seeding_seconds).hash(hasher);
            (download_avg_bps, upload_avg_bps, reannounce_seconds).hash(hasher);
            (eta_seconds, seeding_left, last_seen_complete_ms).hash(hasher);
            last_activity_ms.hash(hasher);
        }
        hasher.finish()
    }
}

// A seeding policy as hashable parts, its ratio by its bits.
fn policy_key(policy: &SeedingPolicy) -> impl Hash {
    let SeedingPolicy {
        ratio_goal,
        seed_time_goal_minutes,
        inactive_goal_minutes,
        mode,
        action,
        seed_time_counting,
    } = *policy;
    let ratio_goal = match ratio_goal {
        Leaf::Inherit => Leaf::Inherit,
        Leaf::Off => Leaf::Off,
        Leaf::Set(ratio) => Leaf::Set(ratio.to_bits()),
    };
    (
        ratio_goal,
        seed_time_goal_minutes,
        inactive_goal_minutes,
        mode,
        action,
        seed_time_counting,
    )
}

// The seeding terms as hashable parts, the ratio by its bits.
fn seeding_key(seeding: &Seeding) -> impl Hash {
    let Seeding {
        ratio_goal,
        seed_time_goal_minutes,
        inactive_goal_minutes,
        mode,
        action,
        seed_time_counting,
    } = *seeding;
    (
        ratio_goal.map(f32::to_bits),
        seed_time_goal_minutes,
        inactive_goal_minutes,
        mode,
        action,
        seed_time_counting,
    )
}
