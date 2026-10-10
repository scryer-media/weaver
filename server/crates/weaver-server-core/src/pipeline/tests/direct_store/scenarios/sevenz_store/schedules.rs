//! Tail-metadata discovery under every bounded arrival/duplicate schedule.
use super::super::archive_schedules::{
    ExtractionProfile, Interruption, RecoveryFormat, Route, Schedule, ScheduleOptions, Selection,
    combined_campaign, run_schedule_with, selected_schedules, wrong_password_schedules,
};
use super::*;
use crate::pipeline::direct_store::router::sevenz::SevenZipRefusal;

mod extended;

/// Bytes in which no slice recurs. A recovery set mends a lost slice from any
/// copy of it elsewhere in the set, so a payload that repeats survives a loss
/// the set carries no recovery data for.
pub(in super::super) fn unrepeated_payload(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed;
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 56) as u8
        })
        .collect()
}

/// Reads one 7z variable-length number at `*at`, advancing past it.
fn sevenz_number(bytes: &[u8], at: &mut usize) -> u64 {
    let first = bytes[*at];
    *at += 1;
    let extra = first.leading_ones() as usize;
    let mut value = 0u64;
    for index in 0..extra {
        value |= u64::from(bytes[*at + index]) << (8 * index);
    }
    *at += extra;
    if extra < 8 {
        value |= u64::from(u16::from(first) & (0xFF >> (extra + 1))) << (8 * extra);
    }
    value
}

/// The archive byte ranges a reader needs before it knows the container's
/// layout: the start header, the end header it points at, and, when that end
/// header only names a compressed header stored earlier, that stream too. An
/// encrypted header refuses the set from the end header alone, so its stream
/// is never needed.
fn map_extents(archive: &[u8]) -> Vec<(usize, usize)> {
    const SIGNATURE_HEADER: usize = 32;
    const ENCODED_HEADER: u8 = 0x17;
    const PACK_INFO: u8 = 0x06;
    const AES_CODER: [u8; 4] = [0x06, 0xF1, 0x07, 0x01];
    let offset = u64::from_le_bytes(archive[12..20].try_into().unwrap()) as usize;
    let size = u64::from_le_bytes(archive[20..28].try_into().unwrap()) as usize;
    let start = SIGNATURE_HEADER + offset;
    let header = &archive[start..start + size];
    let mut extents = vec![(0, SIGNATURE_HEADER), (start, start + size)];
    if header.first() == Some(&ENCODED_HEADER)
        && header.get(1) == Some(&PACK_INFO)
        && !header.windows(AES_CODER.len()).any(|id| id == AES_CODER)
    {
        let mut at = 2;
        let pack_position = sevenz_number(header, &mut at) as usize;
        let streams = sevenz_number(header, &mut at);
        assert_eq!(streams, 1, "one packed header stream");
        assert_eq!(header[at], 0x09, "packed sizes follow");
        at += 1;
        let packed = sevenz_number(header, &mut at) as usize;
        let packed_start = SIGNATURE_HEADER + pack_position;
        extents.push((packed_start, packed_start + packed));
    }
    extents
}

/// The schedule slots holding any byte of [`map_extents`], as a loss mask.
pub(in super::super) fn map_slots(archive: &[u8], count: usize, articles: usize) -> u8 {
    let chunk = archive.len().div_ceil(count);
    let extents = map_extents(archive);
    let mut slots = 0u8;
    for volume in 0..count {
        let volume_start = volume * chunk;
        let volume_len = archive.len().min(volume_start + chunk) - volume_start;
        for article in 0..articles {
            let (from, to) = article_extent(volume_len, article as u32, articles);
            let (from, to) = (volume_start + from, volume_start + to);
            if extents.iter().any(|&(start, end)| start < to && from < end) {
                slots |= 1 << (volume * articles + article);
            }
        }
    }
    slots
}

/// `mask` loses an article of `MAP`.
fn loses<const MAP: u8>(mask: u8) -> bool {
    mask & MAP != 0
}

/// [`loses`] for every four-slot map, indexed by the map's own mask.
pub(in super::super) const LOSES: [fn(u8) -> bool; 16] = [
    loses::<0>,
    loses::<1>,
    loses::<2>,
    loses::<3>,
    loses::<4>,
    loses::<5>,
    loses::<6>,
    loses::<7>,
    loses::<8>,
    loses::<9>,
    loses::<10>,
    loses::<11>,
    loses::<12>,
    loses::<13>,
    loses::<14>,
    loses::<15>,
];

#[derive(Clone, Copy, Debug)]
enum Shape {
    Copy,
    /// Four single-article volumes, so two of the volumes are middle volumes.
    CopyFourVolumes,
    /// One four-article volume: the start header opens it, the end header
    /// closes it, and nothing else is posted besides the recovery set.
    CopySingle,
    Multiple,
    EmptyEntry,
    Nested,
    Lzma2Fallback,
    EncryptedCopy,
    EncryptedHeaders,
    EncryptedLzma2,
    Solid,
    SolidEncrypted,
    SolidHeaders,
    /// Volume names that say nothing. The recovery set carries the real
    /// names, as an obfuscated post's does.
    CopyObfuscated,
    /// One whole container under a name that says nothing: its own signature
    /// header is the only identity it needs.
    CopySingleObfuscated,
}

async fn campaign(shape: Shape, selection: Selection) {
    profile_campaign(shape, selection, ExtractionProfile::DirectStore).await;
}

async fn chase_campaign(shape: Shape, selection: Selection) {
    profile_campaign(shape, selection, ExtractionProfile::Chase).await;
}

async fn conventional_campaign(shape: Shape, selection: Selection) {
    profile_campaign(shape, selection, ExtractionProfile::Conventional).await;
}

async fn profile_campaign(shape: Shape, selection: Selection, profile: ExtractionProfile) {
    run_shape(
        shape,
        profile,
        ScheduleOptions::MATRIX,
        wrong_password_schedules(selection),
        selected_schedules(selection),
    )
    .await;
}

/// Runs `cases`, and `wrong_password` under a password the archive does not
/// open with, over `shape` and holds each to what `profile` allows.
async fn run_shape(
    shape: Shape,
    profile: ExtractionProfile,
    options: ScheduleOptions,
    wrong_password: Vec<Schedule>,
    cases: Vec<(usize, Schedule)>,
) {
    // A described volume is bound by the fingerprint of its first 16 KiB,
    // which its offset-zero article has to cover whole.
    let first = if matches!(shape, Shape::CopyObfuscated | Shape::CopySingleObfuscated) {
        unrepeated_payload(13, 70_001)
    } else {
        payload(13, 6001)
    };
    let second = payload(29, 307);
    let name = if matches!(shape, Shape::Nested) {
        "nested/feature.mkv"
    } else {
        "feature.mkv"
    };
    let mut entries = vec![Entry::file(name, first.clone())];
    let mut expected = BTreeMap::from([(name, first)]);
    match shape {
        Shape::Multiple => {
            entries.push(Entry::file("second.nfo", second.clone()));
            expected.insert("second.nfo", second);
        }
        Shape::EmptyEntry => {
            entries.push(Entry::empty_file("empty.nfo"));
            expected.insert("empty.nfo", Vec::new());
        }
        _ => {}
    }
    let method = if matches!(shape, Shape::Lzma2Fallback | Shape::EncryptedLzma2) {
        EncoderMethod::LZMA2
    } else {
        EncoderMethod::COPY
    };
    let password = matches!(
        shape,
        Shape::EncryptedCopy
            | Shape::EncryptedHeaders
            | Shape::EncryptedLzma2
            | Shape::SolidEncrypted
            | Shape::SolidHeaders
    )
    .then_some("moonlit-harbour");
    let archive = if matches!(
        shape,
        Shape::Solid | Shape::SolidEncrypted | Shape::SolidHeaders
    ) {
        expected.clear();
        for (index, name) in ["part0.bin", "part1.bin", "part2.bin"]
            .into_iter()
            .enumerate()
        {
            expected.insert(
                name,
                (0..8193)
                    .map(|n| ((n * 7 + n / 251 + index * 13) % 253) as u8)
                    .collect(),
            );
        }
        macro_rules! fixture {
            ($name:literal) => {
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/fixtures/extraction_profiles/",
                    $name
                ))
                .to_vec()
            };
        }
        match shape {
            Shape::Solid => fixture!("sevenz_solid.7z"),
            Shape::SolidEncrypted => fixture!("sevenz_solid_encrypted.7z"),
            Shape::SolidHeaders => fixture!("sevenz_solid_headers.7z"),
            _ => unreachable!(),
        }
    } else {
        build_7z_shaped(
            &entries,
            method,
            password,
            matches!(shape, Shape::EncryptedHeaders),
        )
    };
    let count = match shape {
        Shape::CopyFourVolumes => 4,
        Shape::CopySingle | Shape::CopySingleObfuscated => 1,
        _ => 2,
    };
    let volumes = split_volumes(&archive, count);
    let (volumes, described) =
        if matches!(shape, Shape::CopyObfuscated | Shape::CopySingleObfuscated) {
            let described = volumes.iter().map(|(name, _)| name.clone()).collect();
            (obfuscate_volumes(&volumes), Some(described))
        } else {
            (volumes, None::<Vec<String>>)
        };
    let mut spec = sevenz_job_spec(&volumes, 4 / count);
    spec.password = password.map(str::to_owned);
    let wanted = expected.keys().copied().collect::<Vec<_>>();
    let direct_compatible = matches!(
        shape,
        Shape::Copy
            | Shape::CopyFourVolumes
            | Shape::CopySingle
            | Shape::CopySingleObfuscated
            | Shape::Multiple
            | Shape::EmptyEntry
            | Shape::Nested
    ) || (matches!(shape, Shape::CopyObfuscated)
        && options.recovery != RecoveryFormat::Par3);
    // A 7z set's layout lives in articles of its own: the start header opens
    // the first volume and the end header closing the last volume holds the
    // map. Every schedule spans four article slots, so those are slots 0 and
    // 3, plus whichever slot holds the compressed header an end header may
    // only point at. While any of them is lost no byte has a destination, and
    // only a repair of the whole payload brings it back.
    let map = map_slots(&archive, count, 4 / count);
    assert_eq!(map & 0b1001, 0b1001, "{shape:?}: map slots {map:#06b}");
    let unmapped_loss = LOSES[usize::from(map)];
    let route = match shape {
        // Its own front is the only thing that names the container, so
        // while that article is lost nothing admits it. A PAR3 set names a
        // file only once its bytes are on disk, so any damage it has to
        // repair hands the container back to be named there.
        Shape::CopySingleObfuscated => Route {
            unmapped_loss,
            unnamed_loss: if options.recovery == RecoveryFormat::Par3 {
                |mask| mask != 0
            } else {
                |mask| mask & 0b0001 != 0
            },
            ..Route::DIRECT
        },
        // A PAR3 set says which file is which only once the bytes are on
        // disk, so nothing names a part in time: the set extracts from the
        // volumes once they carry their names.
        Shape::CopyObfuscated if options.recovery == RecoveryFormat::Par3 => Route {
            unmapped_loss,
            ..Route::refused(|_| false)
        },
        // The parts carry nothing that says which part they are, so only the
        // recovery set's descriptions name them: the set routes direct when
        // they arrive before its body, and a part whose front is lost is
        // never named. Slots 0 and 2 are the two parts' offset-zero articles.
        Shape::CopyObfuscated => Route {
            unmapped_loss,
            unnamed_loss: |mask| mask & 0b0101 != 0,
            named_by_early_index: true,
            ..Route::DIRECT
        },
        _ if direct_compatible => Route {
            unmapped_loss,
            ..Route::DIRECT
        },
        // Coder output is not the archive's bytes, so it has nowhere to go.
        Shape::Lzma2Fallback | Shape::Solid => Route {
            unmapped_loss,
            ..Route::refused(|reason| {
                matches!(reason, DemotionReason::SevenZip(SevenZipRefusal::Coder))
            })
        },
        // Ciphertext is not the member's bytes either, and a header that is
        // itself encrypted hides the layout.
        Shape::EncryptedCopy
        | Shape::EncryptedHeaders
        | Shape::EncryptedLzma2
        | Shape::SolidEncrypted
        | Shape::SolidHeaders => Route {
            unmapped_loss,
            ..Route::refused(|reason| {
                matches!(
                    reason,
                    DemotionReason::SevenZip(
                        SevenZipRefusal::EncryptedContent
                            | SevenZipRefusal::EncryptedHeader
                            | SevenZipRefusal::Coder
                    )
                )
            })
        },
        _ => unreachable!("{shape:?} is direct-compatible"),
    };
    // A password the archive does not open with. An archive that needs none
    // must not notice it.
    for (order, interruption) in wrong_password {
        if !profile.includes(interruption) {
            continue;
        }
        let mut wrong = spec.clone();
        wrong.password = Some("incorrect-key".to_string());
        eprintln!(
            "wrong password {shape:?} profile={profile:?} order={order:?} interruption={interruption:?}"
        );
        let outcome = run_schedule_with(
            options,
            profile,
            wrong,
            &volumes,
            described.as_deref(),
            &order,
            &wanted,
            interruption,
        )
        .await;
        if password.is_some() {
            profile.assert_rejected(&outcome, &wanted);
        } else {
            assert_eq!(
                outcome.status,
                Some(JobStatus::Complete),
                "{:?}",
                outcome.trace
            );
            profile.assert_delivery(&outcome, route, &wanted, interruption);
            for (name, bytes) in &expected {
                assert_eq!(
                    outcome.files.get(*name).and_then(Option::as_deref),
                    Some(bytes.as_slice())
                );
            }
        }
    }
    for (case, (order, interruption)) in cases {
        if !profile.includes(interruption) {
            continue;
        }
        eprintln!(
            "{shape:?} profile={profile:?} recovery={:?} case={case} order={order:?} interruption={interruption:?}",
            options.recovery
        );
        let outcome = run_schedule_with(
            options,
            profile,
            spec.clone(),
            &volumes,
            described.as_deref(),
            &order,
            &wanted,
            interruption,
        )
        .await;
        if interruption.fails() {
            profile.assert_rejected(&outcome, &wanted);
            continue;
        }
        assert_eq!(
            outcome.status,
            Some(JobStatus::Complete),
            "{shape:?} case={case} order={order:?} interruption={interruption:?}: {:?}",
            outcome.trace
        );
        profile.assert_delivery(&outcome, route, &wanted, interruption);
        if matches!(interruption, Interruption::None)
            && (profile == ExtractionProfile::Chase
                || (profile == ExtractionProfile::DirectStore && !direct_compatible))
        {
            assert_eq!(outcome.chase_consumed, 1, "{shape:?}: {:?}", outcome.trace);
        }
        if profile == ExtractionProfile::DirectStore && matches!(interruption, Interruption::None) {
            let expected = usize::from(direct_compatible);
            assert_eq!(
                outcome.finalized, expected,
                "{shape:?} case={case} order={order:?}: {:?}",
                outcome.trace
            );
        }
        for (name, bytes) in &expected {
            assert_eq!(
                outcome.files.get(*name).and_then(Option::as_deref),
                Some(bytes.as_slice()),
                "{shape:?} member={name} case={case} order={order:?}: {:?}",
                outcome.trace
            );
        }
    }
}

#[tokio::test]
async fn obfuscated_copy_demotion_waits_for_pending_downloads() {
    run_shape(
        Shape::CopyObfuscated,
        ExtractionProfile::DirectStore,
        ScheduleOptions::MATRIX,
        Vec::new(),
        vec![(
            4555,
            (
                vec![(0, 0), (0, 0), (1, 0), (1, 1), (0, 1)],
                Interruption::Demote(1),
            ),
        )],
    )
    .await;
}

#[tokio::test]
async fn copy_arrival_schedules() {
    campaign(Shape::Copy, Selection::Smoke).await;
}
#[tokio::test]
async fn copy_four_volume_arrival_schedules() {
    campaign(Shape::CopyFourVolumes, Selection::Smoke).await;
}
#[tokio::test]
async fn multiple_member_arrival_schedules() {
    campaign(Shape::Multiple, Selection::Smoke).await;
}
#[tokio::test]
async fn empty_entry_arrival_schedules() {
    campaign(Shape::EmptyEntry, Selection::Smoke).await;
}
#[tokio::test]
async fn nested_member_arrival_schedules() {
    campaign(Shape::Nested, Selection::Smoke).await;
}
#[tokio::test]
async fn compressed_fallback_arrival_schedules() {
    campaign(Shape::Lzma2Fallback, Selection::Smoke).await;
}

#[tokio::test]
async fn encrypted_copy_schedules() {
    campaign(Shape::EncryptedCopy, Selection::Smoke).await;
}
#[tokio::test]
async fn encrypted_header_schedules() {
    campaign(Shape::EncryptedHeaders, Selection::Smoke).await;
}
#[tokio::test]
async fn encrypted_compressed_schedules() {
    campaign(Shape::EncryptedLzma2, Selection::Smoke).await;
}

combined_campaign!(combined_copy, Shape::Copy, campaign);
combined_campaign!(combined_copy_four_volume, Shape::CopyFourVolumes, campaign);
combined_campaign!(
    combined_chase_copy_four_volume,
    Shape::CopyFourVolumes,
    chase_campaign
);
combined_campaign!(
    combined_conventional_copy_four_volume,
    Shape::CopyFourVolumes,
    conventional_campaign
);
combined_campaign!(combined_multiple, Shape::Multiple, campaign);
combined_campaign!(combined_empty_entry, Shape::EmptyEntry, campaign);
combined_campaign!(combined_nested, Shape::Nested, campaign);
combined_campaign!(combined_lzma2, Shape::Lzma2Fallback, campaign);
combined_campaign!(combined_encrypted_copy, Shape::EncryptedCopy, campaign);
combined_campaign!(combined_encrypted_header, Shape::EncryptedHeaders, campaign);
combined_campaign!(combined_encrypted_lzma2, Shape::EncryptedLzma2, campaign);

combined_campaign!(combined_chase_copy, Shape::Copy, chase_campaign);
combined_campaign!(combined_chase_multiple, Shape::Multiple, chase_campaign);
combined_campaign!(
    combined_chase_empty_entry,
    Shape::EmptyEntry,
    chase_campaign
);
combined_campaign!(combined_chase_nested, Shape::Nested, chase_campaign);
combined_campaign!(combined_chase_lzma2, Shape::Lzma2Fallback, chase_campaign);
combined_campaign!(
    combined_chase_encrypted_copy,
    Shape::EncryptedCopy,
    chase_campaign
);
combined_campaign!(
    combined_chase_encrypted_header,
    Shape::EncryptedHeaders,
    chase_campaign
);
combined_campaign!(
    combined_chase_encrypted_lzma2,
    Shape::EncryptedLzma2,
    chase_campaign
);

combined_campaign!(
    combined_conventional_copy,
    Shape::Copy,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_multiple,
    Shape::Multiple,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_empty_entry,
    Shape::EmptyEntry,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_nested,
    Shape::Nested,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_lzma2,
    Shape::Lzma2Fallback,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_encrypted_copy,
    Shape::EncryptedCopy,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_encrypted_header,
    Shape::EncryptedHeaders,
    conventional_campaign
);
combined_campaign!(
    combined_conventional_encrypted_lzma2,
    Shape::EncryptedLzma2,
    conventional_campaign
);
combined_campaign!(combined_solid, Shape::Solid, campaign);
combined_campaign!(combined_chase_solid, Shape::Solid, chase_campaign);
combined_campaign!(
    combined_conventional_solid,
    Shape::Solid,
    conventional_campaign
);
combined_campaign!(combined_solid_encrypted, Shape::SolidEncrypted, campaign);
combined_campaign!(
    combined_chase_solid_encrypted,
    Shape::SolidEncrypted,
    chase_campaign
);
combined_campaign!(
    combined_conventional_solid_encrypted,
    Shape::SolidEncrypted,
    conventional_campaign
);
combined_campaign!(combined_solid_headers, Shape::SolidHeaders, campaign);
combined_campaign!(
    combined_chase_solid_headers,
    Shape::SolidHeaders,
    chase_campaign
);
combined_campaign!(
    combined_conventional_solid_headers,
    Shape::SolidHeaders,
    conventional_campaign
);
