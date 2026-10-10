// The wider shapes of real posts, one small family each: a PAR2 index that
// lies about its files, archives nested in archives, posts at the far ends
// of the size axes, passwords right, wrong and missing over every locked
// container, the yEnc wire's own variants, volume naming the field writes,
// and jobs of several sets with the furniture posted beside them.
use super::super::super::super::sevenz_store::{Entry, build_7z_shaped, split_volumes};
use super::damage::with_recovery;
use super::fixtures::{Container, MEMBER, PASSWORD as KEY, SEVENZ_MEMBER, payload};
use super::post::{Encoding, Post, Posted, Role};
use super::recovery::{
    self, Code, Geometry, Margin, Par2Volumes, par2_packet, par2_packets, par2_set_id, par2_type,
};
use super::*;

const ARTICLE: usize = 768;
const SLICE: usize = 1048;
const BLOCK: usize = 1024;
const VOLUMES: usize = 4;
const ARTICLES_PER_VOLUME: usize = 32;

// Each family's name and scenario count.
pub(super) fn totals() -> Vec<(&'static str, usize)> {
    vec![
        ("index that lies", lies::family().total()),
        ("nesting", nesting::family().total()),
        ("scale", scale::family().total()),
        ("password", password::family().total()),
        ("wire", wire::family().total()),
        ("naming", naming::family().total()),
        ("sets and furniture", sets::family().total()),
    ]
}

pub(super) const TOTAL: usize = lies::TOTAL
    + nesting::TOTAL
    + scale::TOTAL
    + password::TOTAL
    + wire::TOTAL
    + naming::TOTAL
    + sets::TOTAL;

// What recovery a post carries beside its archive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Recovery {
    Par2(Margin),
    Par3,
    None,
}

// A stored single-member set of `container` under `member`, `payload`
// across `count` volumes, named `silver.horizon`.
fn stored(
    container: Container,
    member: &'static str,
    payload: &[u8],
    count: usize,
) -> Vec<(String, Vec<u8>)> {
    let mut volumes = match container {
        Container::SevenZip => split_volumes(
            &build_7z_shaped(
                &[Entry::file(member, payload.to_vec())],
                sevenz_turbo::EncoderMethod::COPY,
                None,
                false,
            ),
            count,
        ),
        Container::Rar4 if count > 2 => {
            single_member_rar4_store_set_numbered(member, payload, count)
        }
        Container::Rar4 => single_member_rar4_store_set(member, payload, count),
        Container::Rar5 => single_member_store_set(member, payload, count),
        Container::Rar5Encrypted => {
            encrypted_store_set(member, payload, count, KEY, Some(KEY), false)
        }
        Container::Rar5EncryptedHeaders => {
            header_encrypted_store_set(member, payload, count, KEY, HeaderCheck::For(KEY))
        }
    };
    if count == 1 && container != Container::SevenZip {
        volumes[0].0 = "silver.horizon.rar".to_string();
    }
    volumes
}

fn data_post(
    volumes: Vec<(String, Vec<u8>)>,
    article: usize,
    expected: Vec<(String, Vec<u8>)>,
) -> Post {
    Post {
        files: volumes
            .into_iter()
            .map(|(name, bytes)| Posted::new(name, bytes, article, Role::Data))
            .collect(),
        password: None,
        expected,
        allowed: Vec::new(),
    }
}

fn source_blocks(post: &Post, block: usize) -> usize {
    post.index_of(Role::Data)
        .into_iter()
        .map(|file| post.files[file].bytes.len().div_ceil(block))
        .sum()
}

// `post` with the recovery it carries authored to the margin, and the
// geometry the oracle counts in.
fn with(
    post: Post,
    recovery: Recovery,
    interruption: Interruption,
    volumes: Par2Volumes,
    article: usize,
    rewrite: impl Fn(Vec<(String, Vec<u8>)>) -> Vec<(String, Vec<u8>)>,
) -> (Post, Option<Geometry>) {
    let lost = move |post: &Post| run::lost_articles(post, interruption);
    match recovery {
        Recovery::None => (post, None),
        Recovery::Par2(margin) => {
            let geometry = Geometry {
                block: SLICE,
                packed: false,
            };
            let floor = source_blocks(&post, SLICE) / 10;
            let post = with_recovery_in(
                post,
                lost,
                margin,
                floor,
                geometry,
                |sources, blocks| rewrite(recovery::par2_set(sources, SLICE, blocks, volumes)),
                article,
            );
            (post, Some(geometry))
        }
        Recovery::Par3 => {
            let geometry = Geometry {
                block: BLOCK,
                packed: false,
            };
            let floor = source_blocks(&post, BLOCK) / 10;
            let post = with_recovery_in(
                post,
                lost,
                Margin::With,
                floor,
                geometry,
                |sources, blocks| {
                    recovery::par3_set(
                        sources,
                        BLOCK,
                        blocks,
                        Code::Cauchy,
                        blocks.div_ceil(4) as u64,
                    )
                },
                article,
            );
            (post, Some(geometry))
        }
    }
}

// [`with_recovery`] with the set posted in `article`-sized articles.
fn with_recovery_in(
    post: Post,
    lost: impl Fn(&Post) -> BTreeMap<usize, BTreeSet<u32>>,
    margin: Margin,
    floor: usize,
    geometry: Geometry,
    author: impl Fn(&[(String, Vec<u8>)], usize) -> Vec<(String, Vec<u8>)>,
    article: usize,
) -> Post {
    if article == ARTICLE {
        return with_recovery(post, lost, margin, floor, geometry, author, |_| {});
    }
    // The shared builder posts in the standard article; re-cut the set.
    let mut post = with_recovery(post, lost, margin, floor, geometry, author, |_| {});
    for file in &mut post.files {
        if file.role != Role::Data {
            file.article = article;
        }
    }
    post
}

fn standard_payload(seed: u64) -> Vec<u8> {
    payload(seed, VOLUMES * ARTICLES_PER_VOLUME * ARTICLE - 512)
}

macro_rules! breadth_family {
    ($cell:ty, $cells:expr, $pool:expr, $per:expr, $total:expr, $count:expr) => {
        pub(in super::super) const PER_UNIT: usize = $per;
        pub(in super::super) const TOTAL: usize = $total;

        pub(in super::super) fn family() -> Family<$cell> {
            let cells = $cells;
            assert_eq!(cells.len(), $count);
            Family {
                units: cells
                    .into_iter()
                    .enumerate()
                    .flat_map(|(index, cell)| {
                        PROFILES
                            .into_iter()
                            .map(move |profile| (cell, profile, $pool, PER_UNIT, index))
                    })
                    .collect(),
            }
        }
    };
}

// A PAR2 index whose descriptions are wrong about the files they name.
pub(super) mod lies {
    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub(in super::super) enum Lie {
        None,
        // The whole-file MD5 is wrong; every slice checks.
        FileHash,
        // The first-16k MD5 is wrong.
        Hash16k,
        // The file is described one article longer than it is.
        LengthLong,
        // The file is described one article shorter than it is.
        LengthShort,
        // One slice's MD5 is wrong; the bytes are right.
        SliceHash,
        // The names are uppercased.
        NameCase,
        // The names are of other files.
        NameOther,
        // The main packet's slice size is doubled.
        SliceSize,
    }

    const LIES: [Lie; 9] = [
        Lie::None,
        Lie::FileHash,
        Lie::Hash16k,
        Lie::LengthLong,
        Lie::LengthShort,
        Lie::SliceHash,
        Lie::NameCase,
        Lie::NameOther,
        Lie::SliceSize,
    ];

    const CONTAINERS: [Container; 3] = [Container::Rar5, Container::Rar4, Container::SevenZip];

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub(in super::super) struct LieCell {
        pub lie: Lie,
        pub margin: Margin,
        pub volumes: Par2Volumes,
        pub container: Container,
    }

    pub(in super::super) fn cells() -> Vec<LieCell> {
        let mut cells = Vec::new();
        for lie in LIES {
            for margin in [Margin::With, Margin::Exact] {
                for volumes in [
                    Par2Volumes::One,
                    Par2Volumes::Exponent,
                    Par2Volumes::Uniform,
                ] {
                    for container in CONTAINERS {
                        cells.push(LieCell {
                            lie,
                            margin,
                            volumes,
                            container,
                        });
                    }
                }
            }
        }
        cells
    }

    // 162 cells under three profiles, 617 of the combined cases each.
    breadth_family!(LieCell, cells(), Pool::Combined, 617, 299_862, 162);

    // The set rewritten to tell the lie, in every file that describes.
    fn tell(lie: Lie, set: Vec<(String, Vec<u8>)>) -> Vec<(String, Vec<u8>)> {
        if lie == Lie::None {
            return set;
        }
        let id = par2_set_id(&set[0].1);
        set.into_iter()
            .map(|(name, bytes)| {
                let mut rebuilt = Vec::new();
                for packet in par2_packets(&bytes) {
                    let mut body = bytes[packet.range.start + 64..packet.range.end].to_vec();
                    let kind = packet.kind;
                    let changed = match (lie, &kind) {
                        (Lie::FileHash, k) if k == par2_type::FILE_DESC => {
                            body[16] ^= 0xff;
                            true
                        }
                        (Lie::Hash16k, k) if k == par2_type::FILE_DESC => {
                            body[32] ^= 0xff;
                            true
                        }
                        (Lie::LengthLong | Lie::LengthShort, k) if k == par2_type::FILE_DESC => {
                            let length = u64::from_le_bytes(body[48..56].try_into().unwrap());
                            let length = if lie == Lie::LengthLong {
                                length + ARTICLE as u64
                            } else {
                                length.saturating_sub(ARTICLE as u64)
                            };
                            body[48..56].copy_from_slice(&length.to_le_bytes());
                            true
                        }
                        (Lie::NameCase, k) if k == par2_type::FILE_DESC => {
                            body[56..].make_ascii_uppercase();
                            true
                        }
                        (Lie::NameOther, k) if k == par2_type::FILE_DESC => {
                            if body[56] != 0 {
                                body[56] = if body[56] == b'z' { b'a' } else { body[56] + 1 };
                            }
                            true
                        }
                        (Lie::SliceHash, k) if k == par2_type::IFSC => {
                            if body.len() >= 36 {
                                body[16] ^= 0xff;
                            }
                            true
                        }
                        (Lie::SliceSize, k) if k == par2_type::MAIN => {
                            let slice = u64::from_le_bytes(body[..8].try_into().unwrap());
                            body[..8].copy_from_slice(&(slice * 2).to_le_bytes());
                            true
                        }
                        _ => false,
                    };
                    if changed {
                        rebuilt.extend(par2_packet(&id, &kind, &body));
                    } else {
                        rebuilt.extend_from_slice(&bytes[packet.range]);
                    }
                }
                (name, rebuilt)
            })
            .collect()
    }

    impl Cell for LieCell {
        fn post(self, interruption: Interruption) -> Built {
            let payload = standard_payload(61);
            let volumes = stored(self.container, self.container.member(), &payload, VOLUMES);
            let post = data_post(
                volumes,
                ARTICLE,
                vec![(self.container.member().to_string(), payload)],
            );
            let (post, geometry) = with(
                post,
                Recovery::Par2(self.margin),
                interruption,
                self.volumes,
                ARTICLE,
                |set| tell(self.lie, set),
            );
            // A set that lies may be refused, repaired around or trusted: the
            // job ends by identity or by name either way.
            let ruling = (self.lie != Lie::None).then_some(Verdict::Either);
            Built {
                post,
                geometry,
                ruling,
            }
        }

        fn defect(self, profile: ExtractionProfile) -> Option<Defect> {
            let _ = profile;
            None
        }

        fn par2(self) -> bool {
            true
        }
    }

    mod lies_smoke {
        use super::*;

        #[tokio::test]
        async fn truthful_uniform() {
            smoke(LieCell {
                lie: Lie::None,
                margin: Margin::With,
                volumes: Par2Volumes::Uniform,
                container: Container::Rar5,
            })
            .await;
        }

        #[tokio::test]
        async fn file_hash_exponent() {
            smoke(LieCell {
                lie: Lie::FileHash,
                margin: Margin::Exact,
                volumes: Par2Volumes::Exponent,
                container: Container::SevenZip,
            })
            .await;
        }

        #[tokio::test]
        async fn slice_size_one_volume() {
            loss_smoke(LieCell {
                lie: Lie::SliceSize,
                margin: Margin::With,
                volumes: Par2Volumes::One,
                container: Container::Rar4,
            })
            .await;
        }
    }

    // The campaign: 300 shards of about a thousand cases.
    mod combined_lies {
        use super::*;

        tier2_shards!(family, 300; h0 0 h1 1 h2 2);
    }
}

// Archives inside archives.
pub(super) mod nesting {
    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub(in super::super) enum Depth {
        // A stored RAR holding a stored RAR holding the member.
        Two,
        // Three stored RARs deep.
        Three,
        // A RAR holding a 7z holding the member.
        SevenZipInside,
        // A 7z holding a RAR holding the member.
        RarInside7z,
    }

    const DEPTHS: [Depth; 4] = [
        Depth::Two,
        Depth::Three,
        Depth::SevenZipInside,
        Depth::RarInside7z,
    ];

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub(in super::super) struct NestCell {
        pub depth: Depth,
        // Volumes of the outer archive.
        pub volumes: usize,
        pub recovery: Recovery,
        // The innermost archive is encrypted under the job's password.
        pub locked: bool,
        pub members: usize,
        // The outer container where the depth leaves a choice.
        pub outer: Container,
    }

    pub(in super::super) fn cells() -> Vec<NestCell> {
        let mut cells = Vec::new();
        for depth in DEPTHS {
            for volumes in [1, VOLUMES] {
                for recovery in [Recovery::Par2(Margin::With), Recovery::None] {
                    for locked in [false, true] {
                        for members in [1, 2] {
                            for outer in [Container::Rar5, Container::Rar4] {
                                cells.push(NestCell {
                                    depth,
                                    volumes,
                                    recovery,
                                    locked,
                                    members,
                                    outer,
                                });
                            }
                        }
                    }
                }
            }
        }
        cells
    }

    // 128 cells under three profiles, 781 of the combined cases each.
    breadth_family!(NestCell, cells(), Pool::Combined, 781, 299_904, 128);

    impl NestCell {
        fn members(self) -> Vec<(String, Vec<u8>)> {
            let mut members = vec![(MEMBER.to_string(), payload(67, 40_000))];
            if self.members == 2 {
                members.push(("harbour/beacon.srt".to_string(), payload(71, 9_000)));
            }
            members
        }

        // The innermost archive, one volume, over the members.
        fn innermost(self, members: &[(String, Vec<u8>)]) -> Vec<u8> {
            if self.depth == Depth::SevenZipInside {
                let entries: Vec<Entry> = members
                    .iter()
                    .map(|(name, bytes)| Entry::file(leak(name), bytes.clone()))
                    .collect();
                return build_7z_shaped(
                    &entries,
                    sevenz_turbo::EncoderMethod::COPY,
                    self.locked.then_some(KEY),
                    false,
                );
            }
            let borrowed: Vec<(&str, Vec<u8>)> = members
                .iter()
                .map(|(name, bytes)| (name.as_str(), bytes.clone()))
                .collect();
            if self.locked {
                let mut set = Vec::new();
                for (index, (name, bytes)) in borrowed.iter().enumerate() {
                    // One encrypted member per archive; a second rides as a
                    // second archive inside the same outer member set.
                    let _ = index;
                    set.push(
                        encrypted_store_set(name, bytes, 1, KEY, Some(KEY), false)
                            .remove(0)
                            .1,
                    );
                }
                // Encrypted multi-member stored sets are not written by the
                // fixture builders: the first member is the archive.
                return set.remove(0);
            }
            multi_member_store_set(&borrowed, 1).remove(0).1
        }
    }

    fn leak(name: &str) -> &'static str {
        Box::leak(name.to_string().into_boxed_str())
    }

    impl Cell for NestCell {
        fn post(self, interruption: Interruption) -> Built {
            let mut members = self.members();
            if self.locked && self.depth != Depth::SevenZipInside {
                // See `innermost`: one member when the inner RAR is encrypted.
                members.truncate(1);
            }
            let inner = self.innermost(&members);
            let outer = match self.depth {
                Depth::Two | Depth::SevenZipInside => {
                    let inner_name = if self.depth == Depth::SevenZipInside {
                        "inner.7z"
                    } else {
                        "inner.rar"
                    };
                    stored(self.outer, inner_name, &inner, self.volumes)
                }
                Depth::Three => {
                    let middle = stored(Container::Rar5, "inner.rar", &inner, 1).remove(0).1;
                    stored(self.outer, "middle.rar", &middle, self.volumes)
                }
                Depth::RarInside7z => {
                    stored(Container::SevenZip, "inner.rar", &inner, self.volumes)
                }
            };
            let mut post = data_post(outer, ARTICLE, members);
            post.password = self.locked.then(|| KEY.to_string());
            let (post, geometry) = with(
                post,
                self.recovery,
                interruption,
                Par2Volumes::Uniform,
                ARTICLE,
                |set| set,
            );
            Built {
                post,
                geometry,
                ruling: None,
            }
        }

        fn defect(self, profile: ExtractionProfile) -> Option<Defect> {
            let _ = profile;
            if self.depth == Depth::RarInside7z {
                return Some(Defect::Diverges(RAR_INSIDE_7Z_PUBLISHED_AS_IS));
            }
            if self.members == 2 && matches!(self.depth, Depth::Two | Depth::Three) && self.par2() {
                return Some(Defect::Diverges(NESTED_TWO_MEMBERS_PAR2_FILE_MISSING));
            }
            None
        }

        fn par2(self) -> bool {
            matches!(self.recovery, Recovery::Par2(_))
        }
    }

    // A 7z whose member is a RAR publishes the RAR itself: nested extraction
    // is not entered from a 7z outer. Not PAR2, so not release-blocking.
    const RAR_INSIDE_7Z_PUBLISHED_AS_IS: &str =
        "a RAR inside a 7z is published as the archive, never extracted";

    // A four-volume RAR holding a two-member RAR beside a PAR2 set completes
    // every volume, fetches the PAR2 set and then fails "invalid authoritative
    // NZB segment layout: FileMissing" under DirectStore with nothing lost.
    // PAR2 with sufficient margin: release-blocking.
    const NESTED_TWO_MEMBERS_PAR2_FILE_MISSING: &str =
        "a nested two-member RAR beside a PAR2 set fails FileMissing after completing every volume";

    mod nesting_smoke {
        use super::*;

        #[tokio::test]
        async fn two_deep_four_volumes_par2() {
            smoke(NestCell {
                depth: Depth::Two,
                volumes: VOLUMES,
                recovery: Recovery::Par2(Margin::With),
                locked: false,
                members: 2,
                outer: Container::Rar5,
            })
            .await;
        }

        #[tokio::test]
        async fn three_deep_locked() {
            smoke(NestCell {
                depth: Depth::Three,
                volumes: 1,
                recovery: Recovery::None,
                locked: true,
                members: 1,
                outer: Container::Rar4,
            })
            .await;
        }

        #[tokio::test]
        async fn sevenzip_inside_loss() {
            loss_smoke(NestCell {
                depth: Depth::SevenZipInside,
                volumes: VOLUMES,
                recovery: Recovery::Par2(Margin::With),
                locked: false,
                members: 1,
                outer: Container::Rar5,
            })
            .await;
        }

        #[tokio::test]
        async fn rar_inside_sevenzip() {
            smoke(NestCell {
                depth: Depth::RarInside7z,
                volumes: VOLUMES,
                recovery: Recovery::None,
                locked: false,
                members: 2,
                outer: Container::Rar5,
            })
            .await;
        }
    }

    // The campaign: 300 shards of about a thousand cases.
    mod combined_nesting {
        use super::*;

        tier2_shards!(family, 300; h0 0 h1 1 h2 2);
    }
}

// The far ends of the size axes.
pub(super) mod scale {
    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub(in super::super) enum Shape {
        // Twenty-four volumes of eight articles.
        ManyVolumes,
        // Two volumes of four hundred small articles.
        ManyArticles,
        // Four volumes of eight 12 KiB articles.
        BigArticles,
        // Forty-eight volumes of two articles.
        ManyFiles,
        // One volume in one article.
        Tiny,
    }

    const SHAPES: [Shape; 5] = [
        Shape::ManyVolumes,
        Shape::ManyArticles,
        Shape::BigArticles,
        Shape::ManyFiles,
        Shape::Tiny,
    ];

    const RECOVERIES: [Recovery; 4] = [
        Recovery::Par2(Margin::With),
        Recovery::Par2(Margin::Exact),
        Recovery::Par3,
        Recovery::None,
    ];

    const CONTAINERS: [Container; 5] = [
        Container::Rar5,
        Container::Rar5Encrypted,
        Container::Rar5EncryptedHeaders,
        Container::Rar4,
        Container::SevenZip,
    ];

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub(in super::super) struct ScaleCell {
        pub shape: Shape,
        pub recovery: Recovery,
        pub container: Container,
    }

    pub(in super::super) fn cells() -> Vec<ScaleCell> {
        let mut cells = Vec::new();
        for shape in SHAPES {
            for recovery in RECOVERIES {
                for container in CONTAINERS {
                    cells.push(ScaleCell {
                        shape,
                        recovery,
                        container,
                    });
                }
            }
        }
        cells
    }

    // 100 cells under three profiles, 1,000 of the combined cases each.
    breadth_family!(ScaleCell, cells(), Pool::Combined, 1_000, 300_000, 100);

    impl ScaleCell {
        // Volumes, articles per volume, article size.
        fn geometry(self) -> (usize, usize, usize) {
            match self.shape {
                Shape::ManyVolumes => (24, 8, ARTICLE),
                Shape::ManyArticles => (2, 400, 256),
                Shape::BigArticles => (4, 8, 12 * 1024),
                Shape::ManyFiles => (48, 2, ARTICLE),
                Shape::Tiny => (1, 1, 4096),
            }
        }
    }

    impl Cell for ScaleCell {
        fn post(self, interruption: Interruption) -> Built {
            let (volumes, per, article) = self.geometry();
            let len = if self.shape == Shape::Tiny {
                600
            } else {
                volumes * per * article - 512
            };
            let payload = payload(73, len);
            let set = self.container.volumes(&payload, volumes);
            let mut post = data_post(
                set,
                article,
                vec![(self.container.member().to_string(), payload)],
            );
            post.password = self.container.password();
            let (post, geometry) = with(
                post,
                self.recovery,
                interruption,
                Par2Volumes::Uniform,
                article.min(4096),
                |set| set,
            );
            Built {
                post,
                geometry,
                ruling: None,
            }
        }

        fn defect(self, profile: ExtractionProfile) -> Option<Defect> {
            let _ = profile;
            None
        }

        fn par2(self) -> bool {
            matches!(self.recovery, Recovery::Par2(_))
        }
    }

    mod scale_smoke {
        use super::*;

        #[tokio::test]
        async fn many_volumes_par2() {
            smoke(ScaleCell {
                shape: Shape::ManyVolumes,
                recovery: Recovery::Par2(Margin::With),
                container: Container::Rar5,
            })
            .await;
        }

        #[tokio::test]
        async fn many_articles_par3() {
            smoke(ScaleCell {
                shape: Shape::ManyArticles,
                recovery: Recovery::Par3,
                container: Container::SevenZip,
            })
            .await;
        }

        #[tokio::test]
        async fn big_articles_loss() {
            loss_smoke(ScaleCell {
                shape: Shape::BigArticles,
                recovery: Recovery::Par2(Margin::Exact),
                container: Container::Rar5Encrypted,
            })
            .await;
        }

        #[tokio::test]
        async fn tiny_unprotected() {
            smoke(ScaleCell {
                shape: Shape::Tiny,
                recovery: Recovery::None,
                container: Container::Rar4,
            })
            .await;
        }
    }

    // The campaign: 300 shards of about a thousand cases.
    mod combined_scale {
        use super::*;

        tier2_shards!(family, 300; h0 0 h1 1 h2 2);
    }
}

// Passwords right, wrong and missing over every locked container.
pub(super) mod password {
    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub(in super::super) enum Given {
        Right,
        Wrong,
        Missing,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub(in super::super) enum Locked {
        // Not encrypted at all: a password is ignored.
        Rar5Plain,
        Rar5Data,
        Rar5Headers,
        Rar4Data,
        SevenZipData,
        SevenZipHeaders,
    }

    const LOCKS: [Locked; 6] = [
        Locked::Rar5Plain,
        Locked::Rar5Data,
        Locked::Rar5Headers,
        Locked::Rar4Data,
        Locked::SevenZipData,
        Locked::SevenZipHeaders,
    ];

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub(in super::super) struct PasswordCell {
        pub given: Given,
        pub locked: Locked,
        pub recovery: Recovery,
        pub volumes: usize,
    }

    pub(in super::super) fn cells() -> Vec<PasswordCell> {
        let mut cells = Vec::new();
        for given in [Given::Right, Given::Wrong, Given::Missing] {
            for locked in LOCKS {
                for recovery in [Recovery::Par2(Margin::With), Recovery::None] {
                    for volumes in [1, VOLUMES] {
                        cells.push(PasswordCell {
                            given,
                            locked,
                            recovery,
                            volumes,
                        });
                    }
                }
            }
        }
        cells
    }

    // 72 cells under three profiles, 925 of the combined cases each.
    breadth_family!(PasswordCell, cells(), Pool::Combined, 925, 199_800, 72);

    impl PasswordCell {
        fn member(self) -> &'static str {
            if matches!(self.locked, Locked::SevenZipData | Locked::SevenZipHeaders) {
                SEVENZ_MEMBER
            } else {
                MEMBER
            }
        }

        fn volumes(self, payload: &[u8]) -> Vec<(String, Vec<u8>)> {
            let member = self.member();
            match self.locked {
                Locked::Rar5Plain => stored(Container::Rar5, member, payload, self.volumes),
                Locked::Rar5Data => stored(Container::Rar5Encrypted, member, payload, self.volumes),
                Locked::Rar5Headers => stored(
                    Container::Rar5EncryptedHeaders,
                    member,
                    payload,
                    self.volumes,
                ),
                Locked::Rar4Data => {
                    let mut volumes =
                        encrypted_rar4_store_set(member, payload, self.volumes, KEY, None);
                    if self.volumes == 1 {
                        volumes[0].0 = "silver.horizon.rar".to_string();
                    }
                    volumes
                }
                Locked::SevenZipData | Locked::SevenZipHeaders => split_volumes(
                    &build_7z_shaped(
                        &[Entry::file(member, payload.to_vec())],
                        sevenz_turbo::EncoderMethod::COPY,
                        Some(KEY),
                        self.locked == Locked::SevenZipHeaders,
                    ),
                    self.volumes,
                ),
            }
        }
    }

    impl Cell for PasswordCell {
        fn post(self, interruption: Interruption) -> Built {
            let payload = standard_payload(79);
            let volumes = self.volumes(&payload);
            let mut post = data_post(volumes, ARTICLE, vec![(self.member().to_string(), payload)]);
            post.password = match self.given {
                Given::Right => Some(KEY.to_string()),
                Given::Wrong => Some("wrong-lantern".to_string()),
                Given::Missing => None,
            };
            let (post, geometry) = with(
                post,
                self.recovery,
                interruption,
                Par2Volumes::Uniform,
                ARTICLE,
                |set| set,
            );
            // A locked archive without its password never publishes the
            // member; it fails by name.
            let ruling = (self.locked != Locked::Rar5Plain && self.given != Given::Right)
                .then_some(Verdict::Fails);
            Built {
                post,
                geometry,
                ruling,
            }
        }

        fn defect(self, profile: ExtractionProfile) -> Option<Defect> {
            let _ = profile;
            None
        }

        fn par2(self) -> bool {
            matches!(self.recovery, Recovery::Par2(_))
        }
    }

    mod password_smoke {
        use super::*;

        #[tokio::test]
        async fn right_rar5_headers() {
            smoke(PasswordCell {
                given: Given::Right,
                locked: Locked::Rar5Headers,
                recovery: Recovery::Par2(Margin::With),
                volumes: VOLUMES,
            })
            .await;
        }

        #[tokio::test]
        async fn wrong_sevenzip_data() {
            smoke(PasswordCell {
                given: Given::Wrong,
                locked: Locked::SevenZipData,
                recovery: Recovery::None,
                volumes: 1,
            })
            .await;
        }

        #[tokio::test]
        async fn missing_rar4_data() {
            smoke(PasswordCell {
                given: Given::Missing,
                locked: Locked::Rar4Data,
                recovery: Recovery::Par2(Margin::With),
                volumes: VOLUMES,
            })
            .await;
        }

        #[tokio::test]
        async fn wrong_on_plain_loss() {
            loss_smoke(PasswordCell {
                given: Given::Wrong,
                locked: Locked::Rar5Plain,
                recovery: Recovery::Par2(Margin::With),
                volumes: VOLUMES,
            })
            .await;
        }
    }

    // The campaign: 200 shards of about a thousand cases.
    mod combined_password {
        use super::*;

        tier2_shards!(family, 200; h0 0 h1 1);
    }
}

// The yEnc wire's own variants.
pub(super) mod wire {
    use super::*;

    const LINES: [usize; 4] = [64, 128, 256, 990];

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub(in super::super) enum Articles {
        // The standard article.
        Many,
        // Each volume in one single-part article, with no `=ypart`.
        One,
    }

    const CONTAINERS: [Container; 5] = [
        Container::Rar5,
        Container::Rar5Encrypted,
        Container::Rar5EncryptedHeaders,
        Container::Rar4,
        Container::SevenZip,
    ];

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub(in super::super) struct WireCell {
        pub line: usize,
        pub articles: Articles,
        pub container: Container,
        pub recovery: Recovery,
    }

    pub(in super::super) fn cells() -> Vec<WireCell> {
        let mut cells = Vec::new();
        for line in LINES {
            for articles in [Articles::Many, Articles::One] {
                for container in CONTAINERS {
                    for recovery in [Recovery::Par2(Margin::With), Recovery::None] {
                        cells.push(WireCell {
                            line,
                            articles,
                            container,
                            recovery,
                        });
                    }
                }
            }
        }
        cells
    }

    // 80 cells under three profiles, 833 of the combined cases each.
    breadth_family!(WireCell, cells(), Pool::Combined, 833, 199_920, 80);

    impl Cell for WireCell {
        fn post(self, interruption: Interruption) -> Built {
            let payload = standard_payload(83);
            let volumes = self.container.volumes(&payload, VOLUMES);
            let article = match self.articles {
                Articles::Many => ARTICLE,
                Articles::One => volumes.iter().map(|(_, bytes)| bytes.len()).max().unwrap(),
            };
            let mut post = data_post(
                volumes,
                article,
                vec![(self.container.member().to_string(), payload)],
            );
            post.password = self.container.password();
            let encoding = Encoding::Yenc {
                line: self.line,
                ypart: self.articles == Articles::Many,
            };
            for file in &mut post.files {
                file.encoding = encoding;
            }
            let (mut post, geometry) = with(
                post,
                self.recovery,
                interruption,
                Par2Volumes::Uniform,
                ARTICLE,
                |set| set,
            );
            for file in &mut post.files {
                if file.role != Role::Data {
                    file.encoding = Encoding::Yenc {
                        line: self.line,
                        ypart: true,
                    };
                }
            }
            Built {
                post,
                geometry,
                ruling: None,
            }
        }

        fn defect(self, profile: ExtractionProfile) -> Option<Defect> {
            let _ = profile;
            None
        }

        fn par2(self) -> bool {
            matches!(self.recovery, Recovery::Par2(_))
        }
    }

    mod wire_smoke {
        use super::*;

        #[tokio::test]
        async fn long_lines_many() {
            smoke(WireCell {
                line: 990,
                articles: Articles::Many,
                container: Container::Rar5,
                recovery: Recovery::Par2(Margin::With),
            })
            .await;
        }

        #[tokio::test]
        async fn short_lines_single_part() {
            smoke(WireCell {
                line: 64,
                articles: Articles::One,
                container: Container::SevenZip,
                recovery: Recovery::None,
            })
            .await;
        }

        #[tokio::test]
        async fn single_part_loss_par2() {
            loss_smoke(WireCell {
                line: 128,
                articles: Articles::One,
                container: Container::Rar5EncryptedHeaders,
                recovery: Recovery::Par2(Margin::With),
            })
            .await;
        }
    }

    // The campaign: 200 shards of about a thousand cases.
    mod combined_wire {
        use super::*;

        tier2_shards!(family, 200; h0 0 h1 1);
    }
}

// Volume naming as the field writes it.
pub(super) mod naming {
    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub(in super::super) enum Naming {
        Standard,
        // `.RAR`, `.PAR2`, `.7Z.001`.
        UpperExtension,
        // `Silver.Horizon.Part01.RAR`.
        MixedCase,
        // `part001`.
        WidePadding,
        // `part1`.
        NarrowPadding,
        // The third volume is posted under its name and never arrives.
        Gap,
    }

    const NAMINGS: [Naming; 6] = [
        Naming::Standard,
        Naming::UpperExtension,
        Naming::MixedCase,
        Naming::WidePadding,
        Naming::NarrowPadding,
        Naming::Gap,
    ];

    const CONTAINERS: [Container; 3] = [Container::Rar5, Container::Rar4, Container::SevenZip];

    const RECOVERIES: [Recovery; 3] = [
        Recovery::Par2(Margin::With),
        Recovery::Par2(Margin::Exact),
        Recovery::None,
    ];

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub(in super::super) struct NameCell {
        pub naming: Naming,
        pub container: Container,
        pub recovery: Recovery,
        pub volumes: usize,
    }

    // A 7z set's three-digit numbering is the format's own.
    fn possible(cell: NameCell) -> bool {
        !(cell.container == Container::SevenZip
            && matches!(cell.naming, Naming::WidePadding | Naming::NarrowPadding))
    }

    pub(in super::super) fn cells() -> Vec<NameCell> {
        let mut cells = Vec::new();
        for naming in NAMINGS {
            for container in CONTAINERS {
                for recovery in RECOVERIES {
                    for volumes in [VOLUMES, 2 * VOLUMES] {
                        let cell = NameCell {
                            naming,
                            container,
                            recovery,
                            volumes,
                        };
                        if possible(cell) {
                            cells.push(cell);
                        }
                    }
                }
            }
        }
        cells
    }

    // 96 cells under three profiles, 694 of the combined cases each.
    breadth_family!(NameCell, cells(), Pool::Combined, 694, 199_872, 96);

    fn rename(naming: Naming, name: &str) -> String {
        match naming {
            Naming::Standard | Naming::Gap => name.to_string(),
            Naming::UpperExtension => {
                let (stem, extension) = name.rsplit_once('.').unwrap();
                if extension.chars().all(|c| c.is_ascii_digit()) {
                    // `.7z.001`: the extension before the number.
                    let (inner, ext) = stem.rsplit_once('.').unwrap();
                    format!("{inner}.{}.{extension}", ext.to_ascii_uppercase())
                } else {
                    format!("{stem}.{}", extension.to_ascii_uppercase())
                }
            }
            Naming::MixedCase => name
                .split('.')
                .map(|piece| {
                    let mut chars = piece.chars();
                    match chars.next() {
                        Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
                        None => String::new(),
                    }
                })
                .collect::<Vec<_>>()
                .join("."),
            Naming::WidePadding => name.replace(".part", ".part0"),
            Naming::NarrowPadding => name.replace(".part0", ".part"),
        }
    }

    impl Cell for NameCell {
        fn post(self, interruption: Interruption) -> Built {
            let payload = standard_payload(89);
            let mut volumes = stored(
                self.container,
                self.container.member(),
                &payload,
                self.volumes,
            );
            for (name, _) in &mut volumes {
                *name = rename(self.naming, name);
            }
            let mut post = data_post(
                volumes,
                ARTICLE,
                vec![(self.container.member().to_string(), payload)],
            );
            if self.naming == Naming::Gap {
                post.files[2].absent();
            }
            let (mut post, geometry) = with(
                post,
                self.recovery,
                interruption,
                Par2Volumes::Uniform,
                ARTICLE,
                |set| set,
            );
            if self.naming == Naming::UpperExtension {
                for file in &mut post.files {
                    if file.role != Role::Data {
                        file.name = rename(self.naming, &file.name);
                    }
                }
            }
            Built {
                post,
                geometry,
                ruling: None,
            }
        }

        fn defect(self, profile: ExtractionProfile) -> Option<Defect> {
            let _ = profile;
            None
        }

        fn par2(self) -> bool {
            matches!(self.recovery, Recovery::Par2(_))
        }
    }

    mod naming_smoke {
        use super::*;

        #[tokio::test]
        async fn upper_extension_par2() {
            smoke(NameCell {
                naming: Naming::UpperExtension,
                container: Container::Rar5,
                recovery: Recovery::Par2(Margin::With),
                volumes: VOLUMES,
            })
            .await;
        }

        #[tokio::test]
        async fn mixed_case_sevenzip() {
            smoke(NameCell {
                naming: Naming::MixedCase,
                container: Container::SevenZip,
                recovery: Recovery::None,
                volumes: 2 * VOLUMES,
            })
            .await;
        }

        #[tokio::test]
        async fn narrow_padding_rar4() {
            smoke(NameCell {
                naming: Naming::NarrowPadding,
                container: Container::Rar4,
                recovery: Recovery::Par2(Margin::Exact),
                volumes: 2 * VOLUMES,
            })
            .await;
        }

        #[tokio::test]
        async fn gap_repaired() {
            loss_smoke(NameCell {
                naming: Naming::Gap,
                container: Container::Rar5,
                recovery: Recovery::Par2(Margin::With),
                volumes: VOLUMES,
            })
            .await;
        }
    }

    // The campaign: 200 shards of about a thousand cases.
    mod combined_naming {
        use super::*;

        tier2_shards!(family, 200; h0 0 h1 1);
    }
}

// Jobs of several sets and the furniture posted beside them.
pub(super) mod sets {
    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub(in super::super) enum Layout {
        // One set with an nfo, sfv, jpg, srr and txt beside it.
        Furniture,
        // Two sets, nothing else.
        TwoSets,
        // Two sets and the furniture.
        TwoSetsFurniture,
        // One set and a bare media file posted beside it.
        LooseMedia,
        // One set and a small sample set.
        Sample,
    }

    const LAYOUTS: [Layout; 5] = [
        Layout::Furniture,
        Layout::TwoSets,
        Layout::TwoSetsFurniture,
        Layout::LooseMedia,
        Layout::Sample,
    ];

    const CONTAINERS: [Container; 3] = [Container::Rar5, Container::Rar4, Container::SevenZip];

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub(in super::super) struct SetsCell {
        pub layout: Layout,
        pub container: Container,
        pub recovery: Recovery,
        // The sfv lists a wrong checksum.
        pub stale_sfv: bool,
        pub volumes: usize,
    }

    pub(in super::super) fn cells() -> Vec<SetsCell> {
        let mut cells = Vec::new();
        for layout in LAYOUTS {
            for container in CONTAINERS {
                for recovery in [Recovery::Par2(Margin::With), Recovery::None] {
                    for stale_sfv in [false, true] {
                        for volumes in [2, VOLUMES] {
                            cells.push(SetsCell {
                                layout,
                                container,
                                recovery,
                                stale_sfv,
                                volumes,
                            });
                        }
                    }
                }
            }
        }
        cells
    }

    // 120 cells under three profiles, 555 of the combined cases each.
    breadth_family!(SetsCell, cells(), Pool::Combined, 555, 199_800, 120);

    impl SetsCell {
        fn second_member(self) -> &'static str {
            if self.container == Container::SevenZip {
                "amber.mkv"
            } else {
                "harbour/amber.mkv"
            }
        }

        fn has_furniture(self) -> bool {
            matches!(self.layout, Layout::Furniture | Layout::TwoSetsFurniture)
        }
    }

    impl Cell for SetsCell {
        fn post(self, interruption: Interruption) -> Built {
            let first = standard_payload(97);
            let mut volumes = stored(
                self.container,
                self.container.member(),
                &first,
                self.volumes,
            );
            let mut expected = vec![(self.container.member().to_string(), first)];
            let mut allowed = Vec::new();
            if matches!(self.layout, Layout::TwoSets | Layout::TwoSetsFurniture) {
                let second = payload(101, 30_000);
                for (name, bytes) in
                    stored(self.container, self.second_member(), &second, self.volumes)
                {
                    volumes.push((name.replacen("silver.horizon", "amber.lantern", 1), bytes));
                }
                expected.push((self.second_member().to_string(), second));
            }
            if self.layout == Layout::Sample {
                let sample = payload(103, 6_000);
                let member = if self.container == Container::SevenZip {
                    "sample.mkv"
                } else {
                    "sample/lantern.sample.mkv"
                };
                for (name, bytes) in stored(self.container, member, &sample, 1) {
                    volumes.push((
                        name.replacen("silver.horizon", "silver.horizon.sample", 1),
                        bytes,
                    ));
                }
                allowed.push(member.to_string());
            }
            if self.layout == Layout::LooseMedia {
                let trailer = payload(107, 20_000);
                volumes.push(("silver.horizon.trailer.mkv".to_string(), trailer.clone()));
                expected.push(("silver.horizon.trailer.mkv".to_string(), trailer));
            }
            if self.has_furniture() {
                let listing: String = volumes
                    .iter()
                    .map(|(name, bytes)| {
                        let crc = checksum::crc32(bytes) ^ u32::from(self.stale_sfv);
                        format!("{name} {crc:08x}\r\n")
                    })
                    .collect();
                let furniture: Vec<(String, Vec<u8>)> = vec![
                    (
                        "silver.horizon.nfo".to_string(),
                        b"Silver Horizon\r\nposted for the harbour\r\n".to_vec(),
                    ),
                    ("silver.horizon.sfv".to_string(), listing.into_bytes()),
                    ("silver.horizon.jpg".to_string(), payload(109, 2_000)),
                    ("silver.horizon.srr".to_string(), payload(113, 1_500)),
                    (
                        "readme.txt".to_string(),
                        b"nothing to see here\r\n".to_vec(),
                    ),
                ];
                for (name, bytes) in furniture {
                    allowed.push(name.clone());
                    volumes.push((name, bytes));
                }
            }
            let mut post = data_post(volumes, ARTICLE, expected);
            post.allowed = allowed;
            let (post, geometry) = with(
                post,
                self.recovery,
                interruption,
                Par2Volumes::Uniform,
                ARTICLE,
                |set| set,
            );
            // A listing that disagrees may be believed: identity or a name.
            let ruling = (self.has_furniture() && self.stale_sfv).then_some(Verdict::Either);
            Built {
                post,
                geometry,
                ruling,
            }
        }

        fn defect(self, profile: ExtractionProfile) -> Option<Defect> {
            let _ = profile;
            None
        }

        fn par2(self) -> bool {
            matches!(self.recovery, Recovery::Par2(_))
        }
    }

    mod sets_smoke {
        use super::*;

        #[tokio::test]
        async fn furniture_par2() {
            smoke(SetsCell {
                layout: Layout::Furniture,
                container: Container::Rar5,
                recovery: Recovery::Par2(Margin::With),
                stale_sfv: false,
                volumes: VOLUMES,
            })
            .await;
        }

        #[tokio::test]
        async fn two_sets_sevenzip() {
            smoke(SetsCell {
                layout: Layout::TwoSets,
                container: Container::SevenZip,
                recovery: Recovery::None,
                stale_sfv: false,
                volumes: 2,
            })
            .await;
        }

        #[tokio::test]
        async fn two_sets_stale_sfv_loss() {
            loss_smoke(SetsCell {
                layout: Layout::TwoSetsFurniture,
                container: Container::Rar4,
                recovery: Recovery::Par2(Margin::With),
                stale_sfv: true,
                volumes: VOLUMES,
            })
            .await;
        }

        #[tokio::test]
        async fn loose_media_and_sample() {
            smoke(SetsCell {
                layout: Layout::LooseMedia,
                container: Container::Rar5,
                recovery: Recovery::None,
                stale_sfv: false,
                volumes: 2,
            })
            .await;
            smoke(SetsCell {
                layout: Layout::Sample,
                container: Container::Rar5,
                recovery: Recovery::Par2(Margin::With),
                stale_sfv: false,
                volumes: VOLUMES,
            })
            .await;
        }
    }

    // The campaign: 200 shards of about a thousand cases.
    mod combined_sets {
        use super::*;

        tier2_shards!(family, 200; h0 0 h1 1);
    }
}
