// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::model::{FilePriority, KeptIntent, TorrentIntent, TorrentSource};
use weaver_torrent::NamedBy;

#[test]
fn a_stored_file_level_reads_back_as_the_level_it_was() {
    use crate::torrents::model::{file_priorities_str, parse_file_priorities};
    use FilePriority::*;
    assert_eq!(file_priorities_str(&[]), None);
    assert_eq!(file_priorities_str(&[Normal, Normal]), None);
    assert_eq!(
        file_priorities_str(&[Ignored, Normal, High, Maximum, Normal]).as_deref(),
        Some("0167")
    );
    assert_eq!(
        parse_file_priorities("0167"),
        Some(vec![Ignored, Normal, High, Maximum])
    );
    // A number qBittorrent does not define is no level.
    for value in ["2", "4", "x", "1-"] {
        assert_eq!(parse_file_priorities(value), None, "{value}");
    }
}

// Every stored word of a torrent's row reads back as what wrote it, and
// stays the word rows already hold: a rename here would strand them.
#[test]
fn each_stored_word_of_a_torrent_reads_back_and_keeps_its_text() {
    use crate::torrents::model::{ContentLayout, HistoryEnd, parse_named_by};
    use crate::torrents::model::{TorrentState, named_by_str};
    use crate::torrents::persistence::RetiringMark;

    let sources = [TorrentSource::Magnet, TorrentSource::Metainfo];
    for source in sources {
        assert_eq!(TorrentSource::parse(source.as_str()), Some(source));
    }
    assert_eq!(sources.map(TorrentSource::as_str), ["magnet", "metainfo"]);

    let layouts = [
        ContentLayout::Original,
        ContentLayout::Subfolder,
        ContentLayout::NoSubfolder,
    ];
    for layout in layouts {
        assert_eq!(ContentLayout::parse(layout.as_str()), Some(layout));
    }
    assert_eq!(
        layouts.map(ContentLayout::as_str),
        ["Original", "Subfolder", "NoSubfolder"]
    );

    let states = [
        TorrentState::Queued,
        TorrentState::CheckingResumeData,
        TorrentState::FetchingMetadata,
        TorrentState::CheckingFiles,
        TorrentState::Downloading,
        TorrentState::Stalled,
        TorrentState::Seeding,
        TorrentState::Paused,
        TorrentState::Stopped,
        TorrentState::Moving,
        TorrentState::NeedsCertificate,
        TorrentState::FilesMissing,
    ];
    for state in states {
        assert_eq!(TorrentState::parse(state.as_str()), Some(state));
    }
    assert_eq!(
        states.map(TorrentState::as_str),
        [
            "queued",
            "checking_resume_data",
            "fetching_metadata",
            "checking_files",
            "downloading",
            "stalled",
            "seeding",
            "paused",
            "stopped",
            "moving",
            "needs_certificate",
            "files_missing",
        ]
    );

    let kept = [
        KeptIntent::Intent(TorrentIntent::Resumed),
        KeptIntent::Intent(TorrentIntent::Paused),
        KeptIntent::Intent(TorrentIntent::Stopped),
        KeptIntent::WoundDown,
    ];
    for kept in kept {
        assert_eq!(KeptIntent::parse(kept.as_str()), Some(kept));
        if let KeptIntent::Intent(intent) = kept {
            assert_eq!(TorrentIntent::parse(intent.as_str()), Some(intent));
        }
    }
    assert_eq!(
        kept.map(KeptIntent::as_str),
        ["resumed", "paused", "stopped", "wound_down"]
    );
    // A wind-down's stop is a stop; it is no intent of the operator's.
    assert_eq!(KeptIntent::WoundDown.intent(), TorrentIntent::Stopped);
    assert_eq!(TorrentIntent::parse("wound_down"), None);
    assert_eq!(KeptIntent::parse("Stopped"), None);

    for named_by in [NamedBy::Torrent, NamedBy::Operator] {
        assert_eq!(parse_named_by(named_by_str(named_by)), Some(named_by));
    }
    assert_eq!(
        [NamedBy::Torrent, NamedBy::Operator].map(named_by_str),
        ["torrent", "operator"]
    );

    for mark in RetiringMark::ALL {
        assert_eq!(RetiringMark::parse(mark.as_str()), Some(mark));
    }
    assert_eq!(
        RetiringMark::ALL.map(RetiringMark::as_str),
        ["kept", "deleted"]
    );
    assert_eq!(RetiringMark::of(true), RetiringMark::Deleted);
    assert_eq!(RetiringMark::of(false), RetiringMark::Kept);
    assert_eq!(RetiringMark::parse("delete"), None);

    for end in HistoryEnd::ALL {
        assert_eq!(HistoryEnd::parse(end.as_str()), Some(end));
    }
    assert_eq!(
        HistoryEnd::ALL.map(HistoryEnd::as_str),
        ["complete", "failed", "cancelled"]
    );

    // A cause this build does not know reads as no cause, not as an error.
    use crate::torrents::model::UnpackCause;
    for cause in UnpackCause::ALL {
        assert_eq!(UnpackCause::parse(cause.as_str()), Some(cause));
    }
    assert_eq!(
        UnpackCause::ALL.map(UnpackCause::as_str),
        [
            "refused",
            "nothing_delivered",
            "not_placed",
            "delivered_in_part",
            "unwanted_removed",
            "unwanted_not_removed",
        ]
    );
    assert_eq!(UnpackCause::parse("Refused"), None);
    assert_eq!(UnpackCause::parse("later_cause"), None);
}
