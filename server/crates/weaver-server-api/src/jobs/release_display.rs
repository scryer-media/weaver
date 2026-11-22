// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_core::ingest::{derive_release_name, original_release_title};

#[derive(Clone)]
pub(crate) struct ReleaseDisplayInfo {
    pub original_title: String,
    pub display_title: String,
}

// A job's titles from its name and metadata: the original release title, and
// the display title cut at the first release tag.
// "Show.Name.S01E02.1080p.WEB-DL.H264-Group" shows as "Show Name — S01E02".
pub(crate) fn release_display_info(
    job_name: &str,
    metadata: &[(String, String)],
) -> ReleaseDisplayInfo {
    let original_title = original_release_title(job_name, metadata);
    let display_title = derive_release_name(Some(&original_title), Some(job_name));
    ReleaseDisplayInfo {
        original_title,
        display_title,
    }
}

// The titles the API derives for a job: the original and the display. The
// admin diagnostics package reads these for its stand-in table, so the display
// is derived the one way the API derives it.
pub fn release_display_names(job_name: &str, metadata: &[(String, String)]) -> Vec<String> {
    let info = release_display_info(job_name, metadata);
    vec![info.original_title, info.display_title]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn original_title_metadata_wins_over_job_name() {
        let metadata = vec![(
            "weaver.original_title".to_string(),
            "Stoneguard.S04E29.1080p.WEB-DL.H.265".to_string(),
        )];

        let info = release_display_info("Display Name", &metadata);

        assert_eq!(info.original_title, metadata[0].1);
        assert_eq!(info.display_title, "Stoneguard — S04E29");
    }

    #[test]
    fn display_titles_of_realistic_job_names() {
        for (name, display) in [
            (
                "Show.Name.S01E02.1080p.WEB-DL.H264-Group",
                "Show Name — S01E02",
            ),
            (
                "Glass.Harbor.2024.2160p.BluRay.Remux.H.265-GROUP",
                "Glass Harbor",
            ),
            (
                "a3f9c2e17b8d4f0e9c6a5b2d1e8f7c3a",
                "a3f9c2e17b8d4f0e9c6a5b2d1e8f7c3a",
            ),
            (
                "Some.Movie.2020.1080p.BluRay.x264-GRP{{secret}}",
                "Some Movie",
            ),
            (
                "ubuntu-24.04.2-live-server-amd64",
                "ubuntu-24 04 2-live-server-amd64",
            ),
        ] {
            let info = release_display_info(name, &[]);
            assert_eq!(info.original_title, name);
            assert_eq!(info.display_title, display, "{name}");
        }
    }

    #[test]
    fn diagnostics_names_are_the_original_and_the_display() {
        assert_eq!(
            release_display_names("Show.Name.S01E02.1080p.WEB-DL.H264-Group", &[]),
            vec![
                "Show.Name.S01E02.1080p.WEB-DL.H264-Group".to_string(),
                "Show Name — S01E02".to_string(),
            ]
        );
    }
}
