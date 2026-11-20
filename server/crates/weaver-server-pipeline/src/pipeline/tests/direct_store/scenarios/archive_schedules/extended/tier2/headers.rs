// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The header axis: articles whose yEnc headers lie about a body that is
// whole.
//
// Every other family posts headers that agree with each other and with the
// body. Here the body and its checksums are always the posted bytes and only
// the fields around them lie, each the way a post in the field or a
// reference decoder's own tolerance shows it.
//
// What the reference decoders do with a part decides each verdict: each
// writes the body where its own `=ypart begin=` puts it and ends the file at
// the furthest byte written (sabctools `src/yenc.cc:361-377` and `:802-804`,
// SABnzbd `sabnzbd/assembler.py:567`, NZBGet
// `daemon/nntp/ArticleWriter.cpp:557-558`, nzbfast
// `crates/nzbfast-engine/src/get/workers.rs:823-863`). A lie that leaves
// every `begin=` where it belongs therefore leaves a whole post byte-equal,
// whatever else it says, and must complete; one that moves a `begin=` lands
// the body where it says, and only a recovery set can put it back.
use super::damage::with_recovery;
use super::fixtures::{Container, payload};
use super::post::{Damage, Encoding, Header, Post, Posted, Role, Wire};
use super::recovery::{self, Geometry, Margin, Par2Volumes};
use super::*;

// Decoded bytes per article, PAR2 slice, and volumes: the tier's field
// shape at a thousandth.
const ARTICLE: usize = 768;
const SLICE: usize = 1048;
const VOLUMES: usize = 4;
const ARTICLES_PER_VOLUME: usize = 32;

// How far a size that claims more than the file runs past it. A declared
// size is what Weaver sizes the file from before its bytes arrive, so the
// lie stays a step past the truth, never a figure that books a disk.
const SIZE_OVER: u64 = 1024 * 1024;

// The lies, each with where it comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Lie {
    // Every header states the same size, two articles' worth, far below the
    // file: the field post that declared `size=1536000` on a file of
    // 21,504,000 bytes (two of its 768,000-byte parts).
    SizeSmall,
    // Every header states a different size, all far below the file: the
    // poster nzbfast records at `workers.rs:792-801` (4.8 to 15.6 MB against
    // a 200 MB volume, a fresh figure per article).
    SizeRandom,
    // The middle third of each file's articles state the field post's size,
    // the rest the truth: a run of articles, as the same poster's figure
    // changes from article to article.
    SizeRun,
    // Every header states a mebibyte more than the file. nzbfast bounds a
    // part by `size=` only while its range fits inside it
    // (`workers.rs:845-851`), so a larger size bounds nothing; SABnzbd and
    // NZBGet never read it for placement.
    SizeLarge,
    // Every header states `size=0`: nzbfast reads it as no bound
    // (`workers.rs:829-844`), and so does a missing size.
    SizeZero,
    // Every part's trailer states the whole file's size: the poster quirk
    // nzbfast waives the trailer length for (`nzbkit-base/src/yenc.rs:609-626`).
    YendSizeFile,
    // Every article numbered from zero, header and trailer alike: the
    // zero-based poster nzbfast records (`nzbkit-base/src/yenc.rs:732-770`).
    PartZeroBased,
    // Every header states a `total=` below the real count, and above it:
    // the poster at nzbfast `workers.rs:797` writes a random `total=`.
    TotalSmall,
    TotalLarge,
    // Every header states `line=64` over 128-character lines: sabctools
    // never reads `line=` (`yenc.cc:337-360`), so SABnzbd writes the bytes.
    LineWrong,
    // A single-part article whose `=ybegin size=` is half its body, and a
    // mebibyte more than it, its trailer right: sabctools places it at the start with the
    // stated size (`yenc.cc:343-349`) and writes what decoded; NZBGet
    // refuses the disagreement (`Decoder.cpp:333`).
    SingleSizeSmall,
    SingleSizeLarge,
    // One part's `end=` below its `begin=`: sabctools drops the range
    // (`yenc.cc:366-377`) and SABnzbd appends the body after the part
    // before it (`nzb/file.py:276-291`, `nzb/article.py:197-202`); NZBGet
    // refuses it (`ArticleDownloader.cpp:484-494`).
    EndBelowBegin,
    // One part's `begin=` and `end=` exchanged: nzbfast places it at the
    // smaller (`nzbkit-base/src/yenc.rs:927`), sabctools drops the range and
    // SABnzbd appends it, as above.
    Swapped,
    // One part's `begin=0`: sabctools drops a range that does not begin
    // above zero (`yenc.cc:366`) and SABnzbd appends it, as above.
    BeginZero,
    // One part's range moved down by half an article, over the part before
    // it, and up by half an article, leaving a gap behind it and running
    // into the part after: every decoder writes it where it says, so the
    // file is wrong where it lands. Weaver keeps the placement that runs
    // into an arrived neighbour from overwriting it
    // (`FileAssembly::placement_conflict`), and nzbfast cuts a part that
    // runs past the posted bytes (`workers.rs:845-863`).
    Overlap,
    Gap,
    // Two neighbouring parts each state the other's range, so the later
    // begins below the earlier's end: the inverted ranges the field post
    // built from its declared size, without the size lie. Weaver's
    // neighbour check is the path for it, as above.
    NonMonotonic,
    // One part's trailer states half its body: SABnzbd never reads the
    // trailer size, NZBGet refuses it (`Decoder.cpp:333`).
    YendSizeHalf,
    // One part numbers itself as its neighbour, header and trailer: the
    // body for the wrong article nzbfast steers away from
    // (`nzbkit/src/pool/queue.rs:1103`); SABnzbd never reads `part=` for
    // placement.
    PartDuplicate,
}

const EVERY: [Lie; 10] = [
    Lie::SizeSmall,
    Lie::SizeRandom,
    Lie::SizeRun,
    Lie::SizeLarge,
    Lie::SizeZero,
    Lie::YendSizeFile,
    Lie::PartZeroBased,
    Lie::TotalSmall,
    Lie::TotalLarge,
    Lie::LineWrong,
];

const SINGLE: [Lie; 2] = [Lie::SingleSizeSmall, Lie::SingleSizeLarge];

const ONE: [Lie; 8] = [
    Lie::EndBelowBegin,
    Lie::Swapped,
    Lie::BeginZero,
    Lie::Overlap,
    Lie::Gap,
    Lie::NonMonotonic,
    Lie::YendSizeHalf,
    Lie::PartDuplicate,
];

// The article of the target file a one-article lie or a loss lands on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Spot {
    First,
    Middle,
    Last,
}

const SPOTS: [Spot; 3] = [Spot::First, Spot::Middle, Spot::Last];

impl Spot {
    fn article(self, articles: u32) -> u32 {
        match self {
            Self::First => 0,
            Self::Middle => articles / 2,
            Self::Last => articles - 1,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Layout {
    Rar5,
    Rar4,
    // A 7z archive split into numbered pieces.
    SevenZipSplit,
    // One media file, no archive.
    Plain,
}

const LAYOUTS: [Layout; 4] = [
    Layout::Rar5,
    Layout::Rar4,
    Layout::SevenZipSplit,
    Layout::Plain,
];

// Where a lie lands.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Scope {
    // Every article of every file, recovery files included.
    Every,
    // One article of the target file.
    One(Spot),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct HeaderCell {
    pub lie: Lie,
    pub scope: Scope,
    pub layout: Layout,
    // The article of the target file the post lost, if any.
    pub lost: Option<Spot>,
    // A covering PAR2 set posted beside the data.
    pub par2: bool,
    // A provider's damage to one article of the target file. Only a lie
    // every article carries meets it: a poster that lies in every header
    // still has its articles damaged on a provider's spool like any other,
    // while a one-article lie and a damaged article in one post is two
    // independent accidents no field post shows together.
    pub damage: Option<Damage>,
}

// The provider damage a lying post can also carry: a bit flipped on the
// spool and a body cut short in transit.
const DAMAGES: [Option<Damage>; 3] = [None, Some(Damage::CrcWrong), Some(Damage::Truncated)];

pub(super) fn cells() -> Vec<HeaderCell> {
    let mut cells = Vec::new();
    for layout in LAYOUTS {
        for par2 in [true, false] {
            for lost in [
                None,
                Some(Spot::First),
                Some(Spot::Middle),
                Some(Spot::Last),
            ] {
                for lie in EVERY {
                    for damage in DAMAGES {
                        cells.push(HeaderCell {
                            lie,
                            scope: Scope::Every,
                            layout,
                            lost,
                            par2,
                            damage,
                        });
                    }
                }
                for lie in SINGLE {
                    cells.push(HeaderCell {
                        lie,
                        scope: Scope::Every,
                        layout,
                        lost,
                        par2,
                        damage: None,
                    });
                }
                for lie in ONE {
                    for spot in SPOTS {
                        let cell = HeaderCell {
                            lie,
                            scope: Scope::One(spot),
                            layout,
                            lost,
                            par2,
                            damage: None,
                        };
                        if possible(cell) {
                            cells.push(cell);
                        }
                    }
                }
            }
        }
    }
    cells
}

// Whether a post can present the cell: a part lost is never heard lying,
// and no part begins below the first.
fn possible(cell: HeaderCell) -> bool {
    match cell.scope {
        Scope::Every => true,
        Scope::One(spot) => {
            Some(spot) != cell.lost && !(cell.lie == Lie::Overlap && spot == Spot::First)
        }
    }
}

// Schedules each cell samples from the matrix's smoke schedules.
const PER_UNIT: usize = 4;

// 1,576 cells, four schedules each.
pub(super) const CELLS: usize = 1_576;
pub(super) const TOTAL: usize = CELLS * PER_UNIT + field::TOTAL;

pub(super) fn family() -> Family<HeaderCell> {
    let cells = cells();
    assert_eq!(cells.len(), CELLS);
    Family {
        units: cells
            .into_iter()
            .enumerate()
            .map(|(index, cell)| (cell, Pool::Smoke, PER_UNIT, index))
            .collect(),
    }
}

impl HeaderCell {
    fn single(self) -> bool {
        SINGLE.contains(&self.lie)
    }

    fn data(self) -> Post {
        let payload = payload(131, VOLUMES * ARTICLES_PER_VOLUME * ARTICLE - 512);
        let (volumes, member) = match self.layout {
            Layout::Rar5 => (
                Container::Rar5.volumes(&payload, VOLUMES),
                Container::Rar5.member(),
            ),
            Layout::Rar4 => (
                Container::Rar4.volumes(&payload, VOLUMES),
                Container::Rar4.member(),
            ),
            Layout::SevenZipSplit => (
                Container::SevenZip.volumes(&payload, VOLUMES),
                Container::SevenZip.member(),
            ),
            Layout::Plain => (vec![(PLAIN.to_string(), payload.clone())], PLAIN),
        };
        let single = self.single();
        Post {
            files: volumes
                .into_iter()
                .map(|(name, bytes)| {
                    // A single-part post carries each file in one article
                    // with no `=ypart`.
                    let article = if single { bytes.len() } else { ARTICLE };
                    let mut posted = Posted::new(name, bytes, article, Role::Data);
                    posted.encoding = Encoding::Yenc {
                        line: 128,
                        ypart: !single,
                    };
                    posted
                })
                .collect(),
            password: None,
            expected: vec![(member.to_string(), payload)],
            allowed: Vec::new(),
        }
    }

    // The file one-article lies, losses and damage land on: the middle data
    // file, or for a single-part post every data file in turn, one article
    // each, so a spot names a file.
    fn target(self, post: &Post, spot: Spot) -> (usize, u32) {
        let data = post.index_of(Role::Data);
        if self.single() {
            let file = data[spot.article(data.len() as u32) as usize];
            return (file, 0);
        }
        let file = data[data.len() / 2];
        (file, spot.article(post.files[file].articles()))
    }

    // The lies and losses on the data files.
    fn mark(self, post: &mut Post) {
        if let Some(spot) = self.lost {
            let (file, article) = self.target(post, spot);
            post.files[file].wire.insert(article, Wire::Absent);
        }
        if let Some(damage) = self.damage {
            let (file, _) = self.target(post, Spot::Middle);
            let posted = &mut post.files[file];
            // Next to the middle article, so a middle loss leaves it heard.
            let article = (posted.articles() / 2 + 1).min(posted.articles() - 1);
            if !posted.is_absent(article) {
                posted.wire.insert(article, Wire::Damaged(damage));
            }
        }
        match self.scope {
            Scope::Every => {
                for file in post.index_of(Role::Data) {
                    tell_every(&mut post.files[file], self.lie);
                }
            }
            Scope::One(spot) => {
                let (file, article) = self.target(post, spot);
                tell_one(&mut post.files[file], article, self.lie);
            }
        }
    }
}

// The plain layout's one file.
const PLAIN: &str = "silver.horizon.mkv";

// Every article of `posted` told `lie`.
fn tell_every(posted: &mut Posted, lie: Lie) {
    let len = posted.bytes.len() as u64;
    let articles = posted.articles();
    for article in 0..articles {
        let told = match lie {
            Lie::SizeSmall => Header::Size(2 * posted.article as u64),
            Lie::SizeRandom => {
                Header::Size((len * (48 + u64::from(article) * 37 % 108) / 1000).max(1))
            }
            Lie::SizeRun => {
                if !(articles / 3..2 * articles / 3).contains(&article) {
                    continue;
                }
                Header::Size(2 * posted.article as u64)
            }
            Lie::SizeLarge => Header::Size(len + SIZE_OVER),
            Lie::SizeZero => Header::Size(0),
            Lie::YendSizeFile => Header::YendSize(len),
            Lie::PartZeroBased => Header::Number(article),
            Lie::TotalSmall => Header::Total((articles / 2).max(1)),
            Lie::TotalLarge => Header::Total(articles * 2),
            Lie::LineWrong => Header::Line(64),
            Lie::SingleSizeSmall => Header::Size(len / 2),
            Lie::SingleSizeLarge => Header::Size(len + SIZE_OVER),
            _ => unreachable!("{lie:?} lies in one article"),
        };
        posted.headers.insert(article, told);
    }
}

// Article `article` of `posted` told `lie`.
fn tell_one(posted: &mut Posted, article: u32, lie: Lie) {
    let articles = posted.articles();
    let half = (posted.article / 2) as i64;
    match lie {
        Lie::EndBelowBegin => {
            posted.headers.insert(article, Header::EndBelowBegin);
        }
        Lie::Swapped => {
            posted.headers.insert(article, Header::Swapped);
        }
        Lie::BeginZero => {
            posted.headers.insert(article, Header::BeginZero);
        }
        Lie::Overlap => {
            posted.headers.insert(article, Header::Shifted(-half));
        }
        Lie::Gap => {
            posted.headers.insert(article, Header::Shifted(half));
        }
        Lie::NonMonotonic => {
            let (earlier, later) = if article + 1 < articles {
                (article, article + 1)
            } else {
                (article - 1, article)
            };
            posted.headers.insert(earlier, Header::RangeOf(later));
            posted.headers.insert(later, Header::RangeOf(earlier));
        }
        Lie::YendSizeHalf => {
            let half = posted.extent(article).len() as u64 / 2;
            posted.headers.insert(article, Header::YendSize(half));
        }
        Lie::PartDuplicate => {
            // The neighbour's 1-based number: the part before, or for the
            // first part the one after.
            let number = if article > 0 { article } else { 2 };
            posted.headers.insert(article, Header::Number(number));
        }
        _ => unreachable!("{lie:?} lies in every article"),
    }
}

impl Cell for HeaderCell {
    fn post(self, interruption: Interruption) -> Built {
        let mut post = self.data();
        self.mark(&mut post);
        if !self.par2 {
            return Built {
                post,
                geometry: None,
                ruling: None,
            };
        }
        let geometry = Geometry {
            block: SLICE,
            packed: false,
        };
        let floor = post
            .index_of(Role::Data)
            .into_iter()
            .map(|file| post.files[file].bytes.len().div_ceil(SLICE))
            .sum::<usize>()
            / 10;
        let mut post = with_recovery(
            post,
            move |post: &Post| run::lost_articles(post, interruption),
            Margin::With,
            floor,
            geometry,
            |sources, blocks| recovery::par2_set(sources, SLICE, blocks, Par2Volumes::Uniform),
            |_| {},
        );
        // The recovery files are posted by the same poster: a lie every
        // article carries is in theirs too. A single-part lie stays with the
        // single-part data files.
        if self.scope == Scope::Every && !self.single() {
            for file in post
                .index_of(Role::Index)
                .into_iter()
                .chain(post.index_of(Role::Recovery))
            {
                tell_every(&mut post.files[file], self.lie);
            }
        }
        Built {
            post,
            geometry: Some(geometry),
            ruling: None,
        }
    }

    fn par2(self) -> bool {
        self.par2
    }
}

// The field post at its own size: one 21,504,000-byte file in 28 parts of
// 768,000 bytes, every header declaring `size=1536000`, so part 28 begins at
// 20,736,000 (`begin=20736001`) far past the size every header states.
pub(super) mod field {
    use super::*;

    const PART: usize = 768_000;
    const PARTS: usize = 28;
    const DECLARED: u64 = 1_536_000;
    // The field's PAR2 slice.
    const FIELD_SLICE: usize = 1024 * 1024;

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub(in super::super) enum Lost {
        None,
        // Articles 2 to 27: only the first and last parts arrive, the two
        // whose ranges the declared size and the last begin describe.
        Inner,
        One(Spot),
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub(in super::super) struct FieldCell {
        pub lost: Lost,
        pub par2: bool,
    }

    pub(in super::super) fn cells() -> Vec<FieldCell> {
        let mut cells = Vec::new();
        for lost in [
            Lost::None,
            Lost::Inner,
            Lost::One(Spot::First),
            Lost::One(Spot::Middle),
            Lost::One(Spot::Last),
        ] {
            for par2 in [true, false] {
                cells.push(FieldCell { lost, par2 });
            }
        }
        cells
    }

    const PER_UNIT: usize = 2;
    pub(in super::super) const TOTAL: usize = 10 * PER_UNIT;

    pub(in super::super) fn family() -> Family<FieldCell> {
        let cells = cells();
        assert_eq!(cells.len(), 10);
        Family {
            units: cells
                .into_iter()
                .enumerate()
                .map(|(index, cell)| (cell, Pool::Smoke, PER_UNIT, index))
                .collect(),
        }
    }

    fn told(posted: &mut Posted) {
        for article in 0..posted.articles() {
            posted.headers.insert(article, Header::Size(DECLARED));
        }
    }

    impl Cell for FieldCell {
        fn post(self, interruption: Interruption) -> Built {
            let bytes = payload(137, PART * PARTS);
            let mut file = Posted::new(PLAIN, bytes.clone(), PART, Role::Data);
            assert_eq!(file.articles() as usize, PARTS);
            assert_eq!(file.extent(27).start, 20_736_000);
            match self.lost {
                Lost::None => {}
                Lost::Inner => {
                    for article in 1..27 {
                        file.wire.insert(article, Wire::Absent);
                    }
                }
                Lost::One(spot) => {
                    file.wire.insert(spot.article(PARTS as u32), Wire::Absent);
                }
            }
            told(&mut file);
            let post = Post {
                files: vec![file],
                password: None,
                expected: vec![(PLAIN.to_string(), bytes)],
                allowed: Vec::new(),
            };
            if !self.par2 {
                return Built {
                    post,
                    geometry: None,
                    ruling: None,
                };
            }
            let geometry = Geometry {
                block: FIELD_SLICE,
                packed: false,
            };
            let mut post = with_recovery(
                post,
                move |post: &Post| run::lost_articles(post, interruption),
                Margin::With,
                0,
                geometry,
                |sources, blocks| {
                    recovery::par2_set(sources, FIELD_SLICE, blocks, Par2Volumes::Uniform)
                },
                |_| {},
            );
            // Posted as the data is, in parts of the same size, every header
            // stating the same size.
            for posted in &mut post.files {
                if posted.role != Role::Data {
                    posted.article = PART;
                    told(posted);
                }
            }
            Built {
                post,
                geometry: Some(geometry),
                ruling: None,
            }
        }

        fn par2(self) -> bool {
            self.par2
        }
    }

    mod field_smoke {
        use super::*;

        // Every part arrives: complete and byte-equal, whatever the size
        // every header states.
        #[tokio::test]
        async fn every_part_arrives() {
            smoke(FieldCell {
                lost: Lost::None,
                par2: false,
            })
            .await;
        }

        // Parts 2 to 27 lost with nothing to mend them: failed, never a
        // complete or partial 1.5 MB file.
        #[tokio::test]
        async fn inner_parts_lost_fails() {
            smoke(FieldCell {
                lost: Lost::Inner,
                par2: false,
            })
            .await;
        }

        // The same loss under a PAR2 set that covers it: repaired,
        // byte-equal.
        #[tokio::test]
        async fn inner_parts_lost_repaired() {
            smoke(FieldCell {
                lost: Lost::Inner,
                par2: true,
            })
            .await;
        }
    }

    // The campaign: ten shards of two cases.
    mod field_campaign {
        use super::*;

        tier2_shards!(family, 100; h0 0);
    }
}

mod header_smoke {
    use super::*;

    #[tokio::test]
    async fn every_header_small_size_rar5() {
        smoke(HeaderCell {
            lie: Lie::SizeSmall,
            scope: Scope::Every,
            layout: Layout::Rar5,
            lost: None,
            par2: false,
            damage: None,
        })
        .await;
    }

    #[tokio::test]
    async fn end_below_begin_plain() {
        smoke(HeaderCell {
            lie: Lie::EndBelowBegin,
            scope: Scope::One(Spot::Middle),
            layout: Layout::Plain,
            lost: None,
            par2: false,
            damage: None,
        })
        .await;
    }

    #[tokio::test]
    async fn overlap_repaired_sevenzip() {
        smoke(HeaderCell {
            lie: Lie::Overlap,
            scope: Scope::One(Spot::Middle),
            layout: Layout::SevenZipSplit,
            lost: None,
            par2: true,
            damage: None,
        })
        .await;
    }

    #[tokio::test]
    async fn single_part_large_size_loss_par2() {
        loss_smoke(HeaderCell {
            lie: Lie::SingleSizeLarge,
            scope: Scope::Every,
            layout: Layout::Rar4,
            lost: None,
            par2: true,
            damage: None,
        })
        .await;
    }
}

// The campaign: a hundred shards of about seventy cases.
mod header_campaign {
    use super::*;

    tier2_shards!(family, 100; h0 0);
}
