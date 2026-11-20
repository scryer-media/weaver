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
            files: None,
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
pub async fn campaign(shape: Shape, selection: Selection) {
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
