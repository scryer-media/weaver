// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Every 7z shape's schedules under the graph engine, judged by the graph
// matrix's one rule set. The post is the shape's archive split into the
// volumes [`shape_fixture`] names, beside the matrix's PAR2 set; its members
// are the archive's entries, an empty entry as an empty file.
use super::super::super::archive_schedules::graph::{
    Deliverable, GraphFixture, Leaves, graph_fixture_campaign,
};
use super::*;

// A 7z post of one set. The matrix's recovery set mends whatever a schedule
// damages unless the schedule starves it, so every member is deliverable;
// a starved set leaves what the reference reader reads whole from the post
// as the schedule leaves it on the server. A password that does not open
// the archive leaves nothing.
fn sevenz_post(
    volumes: &[(String, Vec<u8>)],
    spec: &JobSpec,
    members: &BTreeMap<String, Vec<u8>>,
) -> Deliverable {
    let volumes = volumes.to_vec();
    let layout: Vec<usize> = spec.files[..volumes.len()]
        .iter()
        .map(|file| file.segments.len())
        .collect();
    let password = spec.password.clone();
    let members = members.clone();
    Box::new(move |opens, interruption| {
        let deliverable = match interruption {
            _ if !opens => BTreeSet::new(),
            Interruption::Starved { mask } => {
                sevenz_recovers(&volumes, &layout, password.as_deref(), &members, mask)
            }
            _ => members.keys().cloned().collect(),
        };
        Leaves {
            deliverable,
            mended: !interruption.fails(),
        }
    })
}

// The members the reference reader reads whole from a 7z post with each
// lost article's bytes zeroed in place.
fn sevenz_recovers(
    volumes: &[(String, Vec<u8>)],
    layout: &[usize],
    password: Option<&str>,
    members: &BTreeMap<String, Vec<u8>>,
    mask: u8,
) -> BTreeSet<String> {
    let mut archive = Vec::new();
    let mut slot = 0;
    for ((_, bytes), &articles) in volumes.iter().zip(layout) {
        let mut bytes = bytes.clone();
        for article in 0..articles as u32 {
            if mask & (1 << slot) != 0 {
                let (start, end) = article_extent(bytes.len(), article, articles);
                bytes[start..end].fill(0);
            }
            slot += 1;
        }
        archive.extend(bytes);
    }
    sevenz_reads(archive, password, members)
}

// The members the reference reader reads whole from a 7z archive's bytes.
pub(in super::super::super) fn sevenz_reads(
    archive: Vec<u8>,
    password: Option<&str>,
    members: &BTreeMap<String, Vec<u8>>,
) -> BTreeSet<String> {
    let password = password.map_or_else(sevenz_turbo::Password::empty, sevenz_turbo::Password::new);
    let Ok(mut reader) = sevenz_turbo::ArchiveReader::new(std::io::Cursor::new(archive), password)
    else {
        return BTreeSet::new();
    };
    members
        .iter()
        .filter(|(name, bytes)| reader.read_file(name).is_ok_and(|read| read == **bytes))
        .map(|(name, _)| name.clone())
        .collect()
}

// A shape's campaign, as the single-set campaign runs it: the wrong
// password, then the selection, every case by its replay index in the pool
// it was drawn from.
async fn campaign(shape: Shape, selection: Selection) {
    let ShapeFixture {
        expected,
        packing,
        volumes,
        described,
        password,
        spec,
        ..
    } = shape_fixture(shape);
    let members: BTreeMap<String, Vec<u8>> = expected
        .into_iter()
        .map(|(name, bytes)| (name.to_string(), bytes))
        .collect();
    let deliverable = sevenz_post(&volumes, &spec, &members);
    graph_fixture_campaign(
        GraphFixture {
            label: format!("{shape:?} {packing:?}"),
            options: ScheduleOptions::MATRIX,
            spec,
            volumes,
            described,
            members,
            needs_password: password.is_some(),
            deliverable,
        },
        selection,
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
async fn copy_single_arrival_schedules() {
    campaign(Shape::CopySingle, Selection::Smoke).await;
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
#[tokio::test]
async fn solid_arrival_schedules() {
    campaign(Shape::Solid, Selection::Smoke).await;
}
#[tokio::test]
async fn solid_encrypted_schedules() {
    campaign(Shape::SolidEncrypted, Selection::Smoke).await;
}
#[tokio::test]
async fn solid_header_schedules() {
    campaign(Shape::SolidHeaders, Selection::Smoke).await;
}
#[tokio::test]
async fn copy_obfuscated_arrival_schedules() {
    campaign(Shape::CopyObfuscated, Selection::Smoke).await;
}
#[tokio::test]
async fn copy_single_obfuscated_arrival_schedules() {
    campaign(Shape::CopySingleObfuscated, Selection::Smoke).await;
}

combined_campaign!(combined_copy, Shape::Copy, campaign);
combined_campaign!(combined_copy_four_volume, Shape::CopyFourVolumes, campaign);
combined_campaign!(combined_copy_single, Shape::CopySingle, campaign);
combined_campaign!(combined_multiple, Shape::Multiple, campaign);
combined_campaign!(combined_empty_entry, Shape::EmptyEntry, campaign);
combined_campaign!(combined_nested, Shape::Nested, campaign);
combined_campaign!(combined_lzma2, Shape::Lzma2Fallback, campaign);
combined_campaign!(combined_encrypted_copy, Shape::EncryptedCopy, campaign);
combined_campaign!(combined_encrypted_header, Shape::EncryptedHeaders, campaign);
combined_campaign!(combined_encrypted_lzma2, Shape::EncryptedLzma2, campaign);
combined_campaign!(combined_solid, Shape::Solid, campaign);
combined_campaign!(combined_solid_encrypted, Shape::SolidEncrypted, campaign);
combined_campaign!(combined_solid_headers, Shape::SolidHeaders, campaign);
combined_campaign!(combined_copy_obfuscated, Shape::CopyObfuscated, campaign);
combined_campaign!(
    combined_copy_single_obfuscated,
    Shape::CopySingleObfuscated,
    campaign
);
