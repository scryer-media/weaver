// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The deeper schedule families: a fifth article slot, a second duplicate,
// and two interruptions in one schedule.
//
// Every case is judged as the
// four-slot matrix is: the member is delivered whole unless the schedule
// starves the recovery set, and then only if the post still holds it whole;
// no article is asked twice but those a restart or a crash dropped.
// Neither interruption of a two-interruption case changes that, and a second
// duplicate is an article already held, never one asked for again.
use super::super::depth::{Family, depth_shards};
use super::*;

async fn graph_family_campaign(family: Family, format: Format, selection: Selection) {
    eprintln!("graph {family:?} {format:?} selection={selection:?}");
    let cases = family.selected(format, selection);
    assert_cells(&graph_fixture_cases(&graph_fixture(format, family.slots()), cases).await);
}

macro_rules! graph_depth_smoke {
    ($($name:ident $family:ident $format:ident),+ $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                graph_family_campaign(Family::$family, Format::$format, Selection::Smoke).await;
            }
        )+
    };
}

graph_depth_smoke!(
    fifth_article_rar4_smoke FifthArticle Rar4,
    fifth_article_rar5_smoke FifthArticle Rar5,
    fifth_article_rar4_encrypted_smoke FifthArticle Rar4Encrypted,
    fifth_article_rar5_encrypted_smoke FifthArticle Rar5Encrypted,
    fifth_article_rar4_four_volume_smoke FifthArticle Rar4FourVolumes,
    fifth_article_rar5_four_volume_smoke FifthArticle Rar5FourVolumes,
    second_duplicate_rar4_smoke SecondDuplicate Rar4,
    second_duplicate_rar5_smoke SecondDuplicate Rar5,
    second_duplicate_rar4_encrypted_smoke SecondDuplicate Rar4Encrypted,
    second_duplicate_rar5_encrypted_smoke SecondDuplicate Rar5Encrypted,
    second_duplicate_rar4_four_volume_smoke SecondDuplicate Rar4FourVolumes,
    second_duplicate_rar5_four_volume_smoke SecondDuplicate Rar5FourVolumes,
    two_interruption_rar4_smoke TwoInterruptions Rar4,
    two_interruption_rar5_smoke TwoInterruptions Rar5,
    two_interruption_rar4_encrypted_smoke TwoInterruptions Rar4Encrypted,
    two_interruption_rar5_encrypted_smoke TwoInterruptions Rar5Encrypted,
    two_interruption_rar4_four_volume_smoke TwoInterruptions Rar4FourVolumes,
    two_interruption_rar5_four_volume_smoke TwoInterruptions Rar5FourVolumes,
);

// One module per family and format, sharded as the campaign is.
macro_rules! graph_depth_campaigns {
    ($module:ident, $family:ident; $($name:ident $format:ident $decades:tt),+ $(,)?) => {
        mod $module {
            use super::*;
            $(
                mod $name {
                    use super::*;
                    depth_shards!(
                        graph_family_campaign,
                        [Family::$family, Format::$format],
                        Family::$family.shards(Format::$format);
                        $decades
                    );
                }
            )+
        }
    };
}

graph_depth_campaigns!(combined_fifth_article, FifthArticle;
    rar4 Rar4 [0 1 2 3 4 5 6 7],
    rar5 Rar5 [0 1 2 3 4 5 6 7],
    rar4_encrypted Rar4Encrypted [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15],
    rar5_encrypted Rar5Encrypted [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15],
    rar4_four_volume Rar4FourVolumes [0 1 2 3 4 5 6 7],
    rar5_four_volume Rar5FourVolumes [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15],
);
graph_depth_campaigns!(combined_second_duplicate, SecondDuplicate;
    rar4 Rar4 [0 1 2 3],
    rar5 Rar5 [0 1 2 3],
    rar4_encrypted Rar4Encrypted [0 1 2 3 4 5 6 7],
    rar5_encrypted Rar5Encrypted [0 1 2 3],
    rar4_four_volume Rar4FourVolumes [0 1 2 3],
    rar5_four_volume Rar5FourVolumes [0 1 2 3],
);
graph_depth_campaigns!(combined_two_interruptions, TwoInterruptions;
    rar4 Rar4 [0 1 2 3 4 5 6 7],
    rar5 Rar5 [0 1 2 3 4 5 6 7],
    rar4_encrypted Rar4Encrypted [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15],
    rar5_encrypted Rar5Encrypted [0 1 2 3 4 5 6 7],
    rar4_four_volume Rar4FourVolumes [0 1 2 3 4 5 6 7],
    rar5_four_volume Rar5FourVolumes [0 1 2 3 4 5 6 7],
);
