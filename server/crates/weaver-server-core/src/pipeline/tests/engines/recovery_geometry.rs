// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{Scenario, kinds, run};
use super::sevenz;
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use sevenz_turbo::{EncoderConfiguration, EncoderMethod};

// Padding is the stream's business, and the delivered bytes are the truth:
// nothing trims, guesses at or reflows them because of the way they were
// encoded. A set posted with a byte of padding on every piece joins into
// the archive those bytes make, and the archive beneath judges it: the
// split store takes the pieces whole, the 7z store founds on the joined
// file, finds it damaged and gives it back as it stands, never withheld as
// a file nothing mapped. The same posting ends the same way whether it was
// uuencoded or not.
#[tokio::test]
async fn a_padded_set_is_judged_by_what_was_decoded_whatever_the_encoding() {
    let archive = sevenz::container(EncoderConfiguration::new(EncoderMethod::COPY), None);
    for uuencoded in [false, true] {
        let mut posts = sevenz::split(&archive, 3);
        for post in &mut posts {
            post.bytes.push(0);
        }
        let posted = Scenario::new(posts);
        let posted = match uuencoded {
            true => posted.uuencoded(),
            false => posted,
        };
        let outcome = run(&posted).await;
        assert!(
            matches!(outcome.status, Some(JobStatus::Failed { .. })),
            "uuencoded={uuencoded} {:?}",
            outcome.graph
        );
        assert!(
            outcome.files.is_empty(),
            "uuencoded={uuencoded} {:?}",
            outcome.files
        );
        assert_eq!(
            kinds(&outcome),
            ["sevenz_store", "split"],
            "uuencoded={uuencoded}"
        );
        let delivery = &outcome.graph().delivery;
        assert_eq!(
            delivery.damaged,
            ["lantern.7z"],
            "uuencoded={uuencoded} {delivery:?}"
        );
        assert!(
            delivery.withheld.is_empty() && delivery.whole.is_empty(),
            "uuencoded={uuencoded} {delivery:?}"
        );
        let files: Vec<_> = delivery
            .files
            .iter()
            .map(|file| (file.name.as_str(), file.depth, file.whole))
            .collect();
        assert_eq!(files, [("lantern.7z", 1, false)], "uuencoded={uuencoded}");
        assert_eq!(
            outcome.failure.as_deref(),
            Some("no file was delivered whole; 1 short: lantern.7z"),
            "uuencoded={uuencoded}"
        );
        assert_eq!(
            outcome.failure_kind,
            Some(HistoryFailureKind::Unpack),
            "uuencoded={uuencoded}"
        );
    }
}

// The same set as it was posted, with nothing added to its pieces: the
// pieces join, the archive beneath reads the member whole, and every one of
// its members is delivered, whichever way the articles were encoded.
#[tokio::test]
async fn an_unpadded_set_delivers_its_members_whatever_the_encoding() {
    let archive = sevenz::container(EncoderConfiguration::new(EncoderMethod::COPY), None);
    for uuencoded in [false, true] {
        let posted = Scenario::new(sevenz::split(&archive, 3));
        let posted = match uuencoded {
            true => posted.uuencoded(),
            false => posted,
        };
        let outcome = run(&posted).await;
        assert_eq!(
            outcome.status,
            Some(JobStatus::Complete),
            "uuencoded={uuencoded} {:?}",
            outcome.graph
        );
        for (_, bytes) in sevenz::members() {
            assert!(
                outcome.files.values().any(|file| file == &bytes),
                "uuencoded={uuencoded}"
            );
        }
    }
}
