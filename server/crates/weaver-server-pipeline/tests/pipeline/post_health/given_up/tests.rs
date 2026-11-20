// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use weaver_server_base::jobs::ids::{JobId, NzbFileId};

fn segment(file_index: u32, segment_number: u32) -> SegmentId {
    SegmentId {
        file_id: NzbFileId {
            job_id: JobId(1),
            file_index,
        },
        segment_number,
    }
}

// An article no server can bring because the one that might refuses
// the login counts missing, whether or not the servers asked said so.
#[test]
fn an_article_only_a_refusing_server_could_bring_counts_missing() {
    for end in [
        SegmentTerminalState::Unservable,
        SegmentTerminalState::UnservableNotFound,
    ] {
        let mut given_up = GivenUp::default();
        given_up.give_up(segment(0, 0), Some(end), 40);
        assert_eq!(given_up.missing_bytes(), 40, "{end:?}");
        assert_eq!(given_up.file(0).missing, 1, "{end:?}");
        assert_eq!(given_up.missing_files().collect::<Vec<_>>(), vec![0]);
    }
}

#[test]
fn an_article_told_again_is_held_once_at_its_first_end() {
    let mut given_up = GivenUp::default();
    given_up.give_up(segment(0, 1), None, 100);
    given_up.give_up(segment(0, 1), Some(SegmentTerminalState::Abandoned), 100);
    given_up.give_up(segment(0, 2), Some(SegmentTerminalState::Abandoned), 50);
    given_up.give_up(segment(0, 2), Some(SegmentTerminalState::Missing), 50);
    given_up.give_up(
        segment(1, 0),
        Some(SegmentTerminalState::RetriesExhausted),
        10,
    );
    assert_eq!(given_up.bytes(), 160);
    assert_eq!(given_up.missing_bytes(), 100);
    assert_eq!(
        given_up.file(0),
        FileLosses {
            missing: 1,
            lost: 1,
            bytes: 150,
            missing_bytes: 100,
        }
    );
    assert_eq!(given_up.missing_files().collect::<Vec<_>>(), vec![0]);
    assert!(given_up.holds(segment(1, 0)));
    assert!(!given_up.holds(segment(1, 1)));
}

// One volume posted twice under names numbered alike: the post is the
// size of one posting of it, and the volume is lost only where both
// postings lost it. Every article of one posting lost loses nothing.
#[test]
fn a_volume_posted_twice_is_lost_where_every_posting_lost_it() {
    use crate::pipeline::tests::engines::harness::{Post, spec};
    let posts = ["alpha.part01.rar", "alpha.part02.rar", "alpha.part1.rar"]
        .map(|name| Post::plain(name, vec![7; 300]));
    let mut given_up = GivenUp::new(&spec("alpha", &posts, 100));
    assert_eq!(given_up.again_bytes(), 300);
    for article in 0..3 {
        given_up.give_up(
            segment(0, article),
            Some(SegmentTerminalState::Missing),
            100,
        );
    }
    assert_eq!(given_up.file(0).missing_bytes, 300);
    assert_eq!(given_up.missing_bytes(), 0);
    given_up.give_up(segment(2, 1), Some(SegmentTerminalState::Missing), 100);
    assert_eq!(given_up.missing_bytes(), 100);
    given_up.give_up(segment(1, 0), None, 100);
    assert_eq!(given_up.missing_bytes(), 200);
}

// Two postings of one volume that lost different articles hold every
// byte of it between them: the post lost nothing. A byte both lost is
// lost, counted once.
#[test]
fn postings_that_lost_different_articles_lose_nothing() {
    use crate::pipeline::tests::engines::harness::{Post, spec};
    let posts = ["alpha.part01.rar", "alpha.part1.rar"].map(|name| Post::plain(name, vec![7; 300]));
    let mut given_up = GivenUp::new(&spec("alpha", &posts, 100));
    given_up.give_up(segment(0, 0), Some(SegmentTerminalState::Missing), 100);
    given_up.give_up(segment(1, 1), Some(SegmentTerminalState::Missing), 100);
    given_up.give_up(segment(0, 2), None, 100);
    assert_eq!(given_up.missing_bytes(), 0);
    given_up.give_up(segment(1, 2), Some(SegmentTerminalState::Missing), 100);
    assert_eq!(given_up.missing_bytes(), 100);
    assert_eq!(given_up.file(0).missing_bytes, 200);
    assert_eq!(given_up.file(1).missing_bytes, 200);
}
