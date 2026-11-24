// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::jobs::model::{FileSpec, FileSummaries, FileSummary, MessageIds, SegmentSpec};
use weaver_model::files::FileRole;

fn file(name: &str, sizes: &[u32]) -> FileSpec {
    FileSpec {
        filename: name.to_string(),
        role: FileRole::from_filename(name),
        groups: Vec::new(),
        posted_at_epoch: None,
        segments: (0u32..)
            .zip(sizes)
            .map(|(ordinal, &bytes)| SegmentSpec {
                ordinal,
                article_number: ordinal + 1,
                bytes,
                message_ids: MessageIds::new(vec![format!("{name}-{ordinal}")]),
            })
            .collect(),
    }
}

fn spec() -> JobSpec {
    JobSpec {
        name: "post".to_string(),
        password: None,
        files: vec![
            file("post.part01.rar", &[100, 100]),
            file("post.par2", &[10]),
            file("post.vol00+02.par2", &[50, 50]),
            file("post.vol00+04.par3", &[40]),
            file("post.part01.rev", &[30]),
        ],
        total_bytes: 430,
        category: None,
        metadata: Vec::new(),
    }
}

// A job that never ran shows every PAR2 and PAR3 file to come, its
// recovery volumes apart from what it is out to bring, and the same
// wanted bytes the queue showed before.
#[test]
fn a_job_that_never_ran_counts_its_recovery_from_the_spec() {
    let spec = spec();
    let figures = LeanFigures::of(&spec, None);
    let summaries = FileSummaries(spec.files.iter().map(FileSummary::of).collect());
    assert_eq!(figures, LeanFigures::of(&spec, Some(&summaries)));
    assert_eq!(figures.wanted_bytes, spec.wanted_bytes());
    assert_eq!(figures.wanted_bytes, summaries.wanted_bytes(&spec));
    assert_eq!(
        figures,
        LeanFigures {
            remaining_bytes: 240,
            wanted_bytes: 240,
            optional_recovery_bytes: 140,
            optional_recovery_downloaded_bytes: 0,
            remaining_par_files: 3,
        }
    );
}

// A job waiting its turn after it ran shows what its summaries hold in
// hand, without its article lists.
#[test]
fn a_job_that_ran_counts_what_its_summaries_hold_in_hand() {
    let mut spec = spec();
    let in_hand = |file: &FileSpec, held: u32| FileSummary {
        remaining: file.segments.len() as u32 - held,
        ..FileSummary::of(file)
    };
    let summaries = FileSummaries(vec![
        in_hand(&spec.files[0], 1),
        in_hand(&spec.files[1], 1),
        in_hand(&spec.files[2], 1),
        in_hand(&spec.files[3], 0),
        in_hand(&spec.files[4], 1),
    ]);
    spec.drop_article_lists();
    assert_eq!(
        LeanFigures::of(&spec, Some(&summaries)),
        LeanFigures {
            remaining_bytes: 100,
            wanted_bytes: 240,
            optional_recovery_bytes: 140,
            optional_recovery_downloaded_bytes: 50,
            remaining_par_files: 2,
        }
    );
    let info = initial_info(
        JobId(1),
        &spec,
        Some(&summaries),
        [0; 32],
        Path::new("/work"),
        true,
    );
    assert_eq!(info.optional_recovery_bytes, 140);
    assert_eq!(info.optional_recovery_downloaded_bytes, 50);
    assert_eq!(info.remaining_par_files, 2);
    assert_eq!((info.remaining_bytes, info.wanted_bytes), (100, 240));
    assert!((info.progress - 140.0 / 240.0).abs() < 1e-9);
}
