// Uuencode as the field still posts it: the dialects of the old posting
// tools, over a lone volume, a whole set, a set beside a yEnc PAR2 set, a set
// whose volumes alternate between the two encodings, and a PAR2 set that is
// itself uuencoded; each clean and under each damage a uuencode article can
// carry.
//
// Every uuencode part is placed by the decoded length of the parts before it,
// so a schedule that delivers groups out of order parks parts and a damaged
// part shifts everything behind it: that is the surface this family covers.
use super::fixtures::{Container, payload};
use super::post::{
    Damage, Encoding, Post, Posted, Role, UU_DAMAGES, UuEnd, UuHeader, UuStyle, Wire,
};
use super::recovery::{self, Geometry, Margin, Par2Volumes};
use super::*;

const ARTICLE: usize = 768;
const SLICE: usize = 1048;
const VOLUMES: usize = 4;
const ARTICLES_PER_VOLUME: usize = 32;

// The dialects, by index into [`STYLES`].
const STYLES: [UuStyle; 12] = [
    UuStyle::STANDARD,
    UuStyle {
        header: UuHeader::NoMode,
        ..UuStyle::STANDARD
    },
    UuStyle {
        header: UuHeader::Missing,
        ..UuStyle::STANDARD
    },
    UuStyle {
        preamble: true,
        ..UuStyle::STANDARD
    },
    UuStyle {
        space_zero: true,
        ..UuStyle::STANDARD
    },
    UuStyle {
        unpadded_tail: true,
        ..UuStyle::STANDARD
    },
    UuStyle {
        wrong_last_length: true,
        ..UuStyle::STANDARD
    },
    UuStyle {
        end: UuEnd::NoTerminator,
        ..UuStyle::STANDARD
    },
    UuStyle {
        end: UuEnd::Missing,
        ..UuStyle::STANDARD
    },
    UuStyle {
        bare_lf: true,
        ..UuStyle::STANDARD
    },
    UuStyle {
        line_bytes: 30,
        ..UuStyle::STANDARD
    },
    // Everything the lenient decoders of the day forgave at once.
    UuStyle {
        header: UuHeader::NoMode,
        space_zero: true,
        unpadded_tail: true,
        bare_lf: true,
        ..UuStyle::STANDARD
    },
];

// What is uuencoded and what is posted beside it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Shape {
    // One volume, uuencoded.
    SingleVolume,
    // Four volumes, all uuencoded.
    Set,
    // Four uuencoded volumes and a yEnc PAR2 set with margin.
    SetWithPar2,
    // Volumes alternating uuencode and yEnc, and a yEnc PAR2 set.
    Mixed,
    // Four yEnc volumes and a PAR2 set that is itself uuencoded.
    Par2AsUu,
}

const SHAPES: [Shape; 5] = [
    Shape::SingleVolume,
    Shape::Set,
    Shape::SetWithPar2,
    Shape::Mixed,
    Shape::Par2AsUu,
];

const CONTAINERS: [Container; 4] = [
    Container::Rar5,
    Container::Rar4,
    Container::Rar5Encrypted,
    Container::SevenZip,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct UuCell {
    pub style: usize,
    pub shape: Shape,
    // Damage on the middle article of the second data file, if any.
    pub damage: Option<Damage>,
    pub container: Container,
}

pub(super) fn cells() -> Vec<UuCell> {
    let mut cells = Vec::new();
    for style in 0..STYLES.len() {
        for shape in SHAPES {
            for damage in std::iter::once(None).chain(UU_DAMAGES.into_iter().map(Some)) {
                for container in CONTAINERS {
                    cells.push(UuCell {
                        style,
                        shape,
                        damage,
                        container,
                    });
                }
            }
        }
    }
    cells
}

// Schedules each (cell, profile) unit samples from the matrix's smoke
// schedules.
pub(super) const PER_UNIT: usize = 139;

// 1,200 cells under three profiles, 139 schedules each.
pub(super) const TOTAL: usize = 500_400;

pub(super) fn family() -> Family<UuCell> {
    let cells = cells();
    assert_eq!(cells.len(), 1_200);
    Family {
        units: cells
            .into_iter()
            .enumerate()
            .flat_map(|(index, cell)| {
                PROFILES
                    .into_iter()
                    .map(move |profile| (cell, profile, Pool::Smoke, PER_UNIT, index))
            })
            .collect(),
    }
}

impl UuCell {
    fn style(self) -> UuStyle {
        STYLES[self.style]
    }

    fn has_par2(self) -> bool {
        matches!(
            self.shape,
            Shape::SetWithPar2 | Shape::Mixed | Shape::Par2AsUu
        )
    }

    // Whether data file `index` is uuencoded.
    fn data_is_uu(self, index: usize) -> bool {
        match self.shape {
            Shape::SingleVolume | Shape::Set | Shape::SetWithPar2 => true,
            Shape::Mixed => index.is_multiple_of(2),
            Shape::Par2AsUu => false,
        }
    }

    fn data(self) -> Post {
        let payload = payload(37, VOLUMES * ARTICLES_PER_VOLUME * ARTICLE - 512);
        let volumes = if self.shape == Shape::SingleVolume {
            1
        } else {
            VOLUMES
        };
        let volumes = self.container.volumes(&payload, volumes);
        let style = self.style();
        let mut post = Post {
            files: volumes
                .into_iter()
                .enumerate()
                .map(|(index, (name, bytes))| {
                    let mut posted = Posted::new(name, bytes, ARTICLE, Role::Data);
                    if self.data_is_uu(index) {
                        posted.encoding = Encoding::Uu(style);
                    }
                    posted
                })
                .collect(),
            password: self.container.password(),
            expected: vec![(self.container.member().to_string(), payload)],
            allowed: Vec::new(),
        };
        if let Some(damage) = self.damage {
            let data = post.index_of(Role::Data);
            let target = data[1.min(data.len() - 1)];
            let article = post.files[target].articles() / 2;
            post.files[target]
                .wire
                .insert(article, Wire::Damaged(damage));
        }
        post
    }

    // Uuencodes the recovery files where the shape posts them so.
    fn encode_recovery(self, post: &mut Post) {
        if self.shape != Shape::Par2AsUu {
            return;
        }
        let style = self.style();
        for file in &mut post.files {
            if file.role != Role::Data {
                file.encoding = Encoding::Uu(style);
            }
        }
    }
}

impl Cell for UuCell {
    fn post(self, interruption: Interruption) -> Built {
        let post = self.data();
        if !self.has_par2() {
            return Built {
                post,
                geometry: None,
                ruling: None,
            };
        }
        let lost = move |post: &Post| run::lost_articles(post, interruption);
        let source_blocks: usize = post
            .index_of(Role::Data)
            .into_iter()
            .map(|file| post.files[file].bytes.len().div_ceil(SLICE))
            .sum();
        let geometry = Geometry {
            block: SLICE,
            packed: false,
        };
        let post = super::damage::with_recovery(
            post,
            lost,
            Margin::With,
            source_blocks / 10,
            geometry,
            |sources, blocks| recovery::par2_set(sources, SLICE, blocks, Par2Volumes::Uniform),
            |post| self.encode_recovery(post),
        );
        Built {
            post,
            geometry: Some(geometry),
            ruling: None,
        }
    }

    fn par2(self) -> bool {
        self.has_par2()
    }
}

macro_rules! uu_smokes {
    ($($name:ident $style:literal $shape:ident $damage:expr, $container:ident;)+) => {
        mod uu_smoke {
            use super::*;
            $(
                #[tokio::test]
                async fn $name() {
                    smoke(UuCell {
                        style: $style,
                        shape: Shape::$shape,
                        damage: $damage,
                        container: Container::$container,
                    })
                    .await;
                }
            )+
        }
    };
}

uu_smokes! {
    standard_single 0 SingleVolume None, Rar5;
    no_mode_set 1 Set None, Rar4;
    missing_header_set_par2 2 SetWithPar2 None, Rar5;
    preamble_mixed 3 Mixed None, Rar5Encrypted;
    space_zero_par2_as_uu 4 Par2AsUu None, Rar5;
    unpadded_tail_sevenz 5 Set None, SevenZip;
    wrong_last_length_truncated 6 SetWithPar2 Some(Damage::Truncated), Rar5;
    no_terminator_overlong 7 Set Some(Damage::Overlong), Rar4;
    missing_end_bad_byte 8 SetWithPar2 Some(Damage::NoChecksum), Rar5;
    bare_lf_duplicate 9 Mixed Some(Damage::DifferingDuplicate), Rar5;
    short_lines_single 10 SingleVolume None, SevenZip;
    lenient_combo_par2 11 SetWithPar2 None, Rar5Encrypted;
}

mod uu_loss_smoke {
    use super::*;

    #[tokio::test]
    async fn standard_set_par2() {
        loss_smoke(UuCell {
            style: 0,
            shape: Shape::SetWithPar2,
            damage: None,
            container: Container::Rar5,
        })
        .await;
    }

    #[tokio::test]
    async fn bare_lf_set_unprotected() {
        loss_smoke(UuCell {
            style: 9,
            shape: Shape::Set,
            damage: None,
            container: Container::Rar4,
        })
        .await;
    }
}

// The campaign: 500 shards of about a thousand cases.
mod combined_uu {
    use super::*;

    tier2_shards!(family, 500; h0 0 h1 1 h2 2 h3 3 h4 4);
}
