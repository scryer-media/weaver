// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! A view's fingerprint, so a follower can tell a torrent that moved from
//! one that only aged: some fields move on every reading with the clock
//! alone, and a follower that does not show them leaves them out.

use std::fmt::{self, Debug, Write as _};
use std::hash::{DefaultHasher, Hasher};

use super::TorrentView;

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
        } = self;
        let mut hasher = DefaultHasher::new();
        feed(
            &mut hasher,
            &[
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
            ],
        );
        if parts == ViewParts::Whole {
            feed(
                &mut hasher,
                &[
                    active_seconds,
                    seeding_seconds,
                    consecutive_seeding_seconds,
                    download_avg_bps,
                    upload_avg_bps,
                    reannounce_seconds,
                    eta_seconds,
                    seeding_left,
                    last_seen_complete_ms,
                ],
            );
        }
        hasher.finish()
    }
}

// Each field's `Debug` reading into `hasher`, with no string kept: every
// field type has one, where few have `Hash`, and floats have none.
fn feed(hasher: &mut DefaultHasher, fields: &[&dyn Debug]) {
    struct Into<'a>(&'a mut DefaultHasher);
    impl fmt::Write for Into<'_> {
        fn write_str(&mut self, text: &str) -> fmt::Result {
            self.0.write(text.as_bytes());
            Ok(())
        }
    }
    for field in fields {
        // Writing into a hasher cannot fail.
        let _ = write!(Into(hasher), "{field:?}");
        hasher.write_u8(0x1f);
    }
}
