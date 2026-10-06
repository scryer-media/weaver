//! Tail-metadata discovery under every bounded arrival/duplicate schedule.
use super::super::archive_schedules::{
    ExtractionProfile, Interruption, Route, Selection, combined_campaign, run_described_schedule,
    selected_schedules, wrong_password_schedules,
};
use super::*;
use crate::pipeline::direct_store::router::sevenz::SevenZipRefusal;

mod extended;

/// Bytes in which no slice recurs. A recovery set mends a lost slice from any
/// copy of it elsewhere in the set, so a payload that repeats survives a loss
/// the set carries no recovery data for.
fn unrepeated_payload(seed: u64, len: usize) -> Vec<u8> {
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

#[derive(Clone, Copy, Debug)]
enum Shape {
    Copy,
    /// Four single-article volumes, so two of the volumes are middle volumes.
    CopyFourVolumes,
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
    // A described volume is bound by the fingerprint of its first 16 KiB,
    // which its offset-zero article has to cover whole.
    let first = if matches!(shape, Shape::CopyObfuscated) {
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
    let count = if matches!(shape, Shape::CopyFourVolumes) {
        4
    } else {
        2
    };
    let volumes = split_volumes(&archive, count);
    let (volumes, described) = if matches!(shape, Shape::CopyObfuscated) {
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
        Shape::Copy | Shape::CopyFourVolumes | Shape::Multiple | Shape::EmptyEntry | Shape::Nested
    );
    // A 7z set's layout lives in two articles of its own: the start header
    // opens the first volume and the end header closing the last volume holds
    // the map. Every schedule spans four article slots, so those are always
    // slots 0 and 3. While either is lost no byte has a destination, and
    // only a repair of the whole payload brings it back.
    let unmapped_loss: fn(u8) -> bool = |mask| mask & 0b1001 != 0;
    let route = match shape {
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
        // The recovery set's descriptions admit RAR volumes only, so an
        // obfuscated 7z set is never admitted and nothing in it routes
        // direct: it extracts from the volumes once they carry their names.
        Shape::CopyObfuscated => Route {
            unmapped_loss,
            ..Route::refused(|_| false)
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
    for (order, interruption) in wrong_password_schedules(selection) {
        if !profile.includes(interruption) {
            continue;
        }
        let mut wrong = spec.clone();
        wrong.password = Some("incorrect-key".to_string());
        eprintln!(
            "wrong password {shape:?} profile={profile:?} order={order:?} interruption={interruption:?}"
        );
        let outcome = run_described_schedule(
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
    for (case, (order, interruption)) in selected_schedules(selection) {
        if !profile.includes(interruption) {
            continue;
        }
        eprintln!(
            "{shape:?} profile={profile:?} selection={selection:?} case={case} order={order:?} interruption={interruption:?}"
        );
        let outcome = run_described_schedule(
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
