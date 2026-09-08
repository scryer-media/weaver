//! A PAR2 repair re-entering an **encrypted** set, at the router.
//!
//! The set is driven through [`DirectSetRouter`] directly rather than through a
//! job: what is under test is the routing decision a repaired span produces, and
//! a whole pipeline in front of it would only add ways for the test to fail for
//! other reasons. The volumes are the real fixture archives, so the parse, the
//! layout, the keys and both integrity layers are the ones a download builds.

use super::*;

use crate::pipeline::direct_store::plan::DirectSetPlan;
use crate::pipeline::direct_store::router::{
    DirectDestination, DirectSetRouter, RepairedChunk, RoutedSpan,
};

/// The password every fixture in this file is written with.
const REPAIR_PASSWORD: &str = "moonlit-harbour";

const REPAIR_MEMBER: &str = "Silver.Horizon.S02E01.mkv";

/// A router over `volumes`, keyed for them, with nothing routed yet.
fn encrypted_router(volumes: &[(String, Vec<u8>)], password: &str) -> DirectSetRouter {
    let plan = DirectSetPlan {
        set_name: "silver.horizon".to_string(),
        volumes: (0..volumes.len() as u32)
            .map(|index| (index, index))
            .collect(),
        files: (0..volumes.len() as u32)
            .map(|index| (index, index))
            .collect(),
        identity: None,
        working_dir: std::path::PathBuf::from("/nonexistent"),
        destination_dir: std::path::PathBuf::from("/nonexistent-staging"),
    };
    let mut router = DirectSetRouter::new(plan);
    router.set_password(Some(password));
    // The same password again, for the `-hp` ring: headers and file data are
    // keyed separately in the router because they are answered from different
    // inputs, and a set whose headers are encrypted needs both.
    router.offer_header_password("Explicit", password);
    router
}

/// Closes the composition gaps the rewrite left, the way `reread_direct_stale_gaps`
/// does: read the member's own bytes back and feed their CRC32 in.
///
/// An encrypted member's partial holds **plaintext**, so the payload is what the
/// production read would have found there.
fn close_stale_gaps(router: &mut DirectSetRouter, payload: &[u8]) {
    for run in router.stale_gap_read_plan() {
        let from = run.logical_offset as usize;
        let to = (from + run.len as usize).min(payload.len());
        let crc = checksum::crc32(&payload[from..to]);
        router
            .note_restored_member_crc(run.member_id, run.logical_offset, run.len, crc)
            .expect("the re-read closes the gap");
    }
}

/// Routes every volume whole, the way a set whose articles all arrived leaves
/// the router: one member, verified, nothing held.
fn route_all(router: &mut DirectSetRouter, volumes: &[(String, Vec<u8>)]) {
    for (index, (_, bytes)) in volumes.iter().enumerate() {
        router
            .route(index as u32, 0, bytes)
            .expect("an undamaged encrypted volume routes");
        router
            .note_volume_complete(index as u32)
            .expect("the volume's articles are all in");
    }
}

/// The cipher stream the `-p` fixtures encrypt `payload` into, and where each
/// volume's part of it starts inside that volume's posted image.
fn cipher_and_part_offsets(
    payload: &[u8],
    volumes: &[(String, Vec<u8>)],
) -> (Vec<u8>, Vec<(u64, u64)>) {
    let material =
        unrar_rs::derive_rar5_material(REPAIR_PASSWORD, &TEST_CRYPT_SALT, TEST_CRYPT_KDF_LG2)
            .expect("the fixture KDF count is derivable");
    let cipher_len = payload.len().div_ceil(16) * 16;
    let mut padded = payload.to_vec();
    padded.resize(cipher_len, 0);
    let cipher = unrar_rs::test_support::encrypt_aes256_cbc(&material.key, &TEST_CRYPT_IV, &padded);
    (cipher.clone(), part_offsets(&cipher, volumes))
}

/// `(physical offset, length)` of each volume's part of `cipher`, located in the
/// posted image by searching for the bytes themselves — the fixture's header
/// sizes are its own business.
fn part_offsets(cipher: &[u8], volumes: &[(String, Vec<u8>)]) -> Vec<(u64, u64)> {
    let mut offsets = Vec::new();
    let mut start = 0usize;
    for (part_len, (_, image)) in misaligned_parts(cipher.len(), volumes.len())
        .into_iter()
        .zip(volumes)
    {
        let part = &cipher[start..start + part_len];
        let at = image
            .windows(part_len)
            .position(|window| window == part)
            .expect("the fixture's part is in its own volume");
        offsets.push((at as u64, part_len as u64));
        start += part_len;
    }
    offsets
}

/// The lead-in a repair supplies for a span: the posted bytes just below it,
/// which the real caller reads off the materialized volume, plus the
/// neighbouring-volume halves of this volume's member edge blocks.
///
/// The fixture holds the posted image, so both come out of it directly — the
/// production reader re-encrypts the neighbour's destination to get the same
/// bytes back.
fn lead_in_for(
    router: &DirectSetRouter,
    volumes: &[(String, Vec<u8>)],
    volume_index: u32,
    source_offset: u64,
    len: u64,
) -> Vec<(u32, u64, std::sync::Arc<[u8]>)> {
    let image = &volumes[volume_index as usize].1;
    let mut lead_in = Vec::new();
    if source_offset > 0 {
        let from = source_offset.saturating_sub(32);
        lead_in.push((
            volume_index,
            from,
            std::sync::Arc::from(&image[from as usize..source_offset as usize]),
        ));
    }
    let end = source_offset + len;
    if (end as usize) < image.len() {
        let to = (end + 16).min(image.len() as u64);
        lead_in.push((
            volume_index,
            end,
            std::sync::Arc::from(&image[end as usize..to as usize]),
        ));
    }
    for (volume, from, len) in router.cipher_edge_reads(volume_index) {
        let image = &volumes[volume as usize].1;
        let end = (from + len) as usize;
        if end > image.len() {
            continue;
        }
        lead_in.push((
            volume,
            from,
            std::sync::Arc::from(&image[from as usize..end]),
        ));
    }
    lead_in
}

/// Re-enters one repaired span, exactly as the repair wiring does: the bytes
/// PAR2 rebuilt are the posted ones, so the fixture's own image supplies them.
fn route_repaired_span(
    router: &mut DirectSetRouter,
    volumes: &[(String, Vec<u8>)],
    volume_index: u32,
    source_offset: u64,
    len: u64,
) -> Result<Vec<RoutedSpan>, DemotionReason> {
    let image = &volumes[volume_index as usize].1;
    let bytes: std::sync::Arc<[u8]> =
        std::sync::Arc::from(&image[source_offset as usize..(source_offset + len) as usize]);
    let lead_in = lead_in_for(router, volumes, volume_index, source_offset, len);
    router.route_repaired(volume_index, &[(source_offset, bytes)], &lead_in, false)
}

fn member_bytes_written(spans: &[RoutedSpan]) -> u64 {
    spans
        .iter()
        .filter(|span| matches!(span.destination, DirectDestination::Member { .. }))
        .map(RoutedSpan::len)
        .sum()
}

/// A repaired span in the middle of a member's part must route back in.
///
/// PAR2 rebuilt these very cipher bytes, so the composition that describes them
/// has to be rewritten with them and the part gate re-run over the rewrite.
/// Refusing here throws away a repair that worked and demotes a set whose bytes
/// on disk are correct.
#[tokio::test]
async fn a_repaired_mid_part_span_reroutes_into_an_encrypted_member() {
    let payload: Vec<u8> = (0..600u32).map(|index| (index % 251) as u8).collect();
    let volumes = encrypted_store_set(
        REPAIR_MEMBER,
        &payload,
        2,
        REPAIR_PASSWORD,
        Some(REPAIR_PASSWORD),
        true,
    );
    let mut router = encrypted_router(&volumes, REPAIR_PASSWORD);
    route_all(&mut router, &volumes);
    assert!(
        router.all_members_verified(),
        "the undamaged download must verify before the repair is the question"
    );
    let (_, parts) = cipher_and_part_offsets(&payload, &volumes);

    // Unaligned at both ends, so the span's head and tail cipher blocks each
    // straddle bytes it does not carry — the shape a slice-shaped repair takes
    // over a block-shaped stream.
    let (part_at, part_len) = parts[0];
    let source_offset = part_at + 37;
    let len = part_len - 37 - 21;
    let spans = route_repaired_span(&mut router, &volumes, 0, source_offset, len)
        .expect("a repaired mid-part span must route back into the member");
    assert!(
        member_bytes_written(&spans) > 0,
        "the repaired bytes must reach the member's partial"
    );
    close_stale_gaps(&mut router, &payload);
    assert!(
        !router.has_stale_gaps(),
        "one pass over the plan closes every gap it named"
    );
    assert!(
        router.all_members_verified(),
        "and the member must verify against the repaired image"
    );
}

/// The same, for a volume PAR2 rebuilt in its entirety.
#[tokio::test]
async fn a_repaired_whole_volume_reroutes_into_an_encrypted_member() {
    let payload: Vec<u8> = (0..600u32).map(|index| (index % 251) as u8).collect();
    let volumes = encrypted_store_set(
        REPAIR_MEMBER,
        &payload,
        2,
        REPAIR_PASSWORD,
        Some(REPAIR_PASSWORD),
        true,
    );
    let mut router = encrypted_router(&volumes, REPAIR_PASSWORD);
    route_all(&mut router, &volumes);
    let (_, parts) = cipher_and_part_offsets(&payload, &volumes);

    let (part_at, part_len) = parts[1];
    let spans = route_repaired_span(&mut router, &volumes, 1, part_at, part_len)
        .expect("a repaired whole part must route back into the member");
    assert!(
        member_bytes_written(&spans) > 0,
        "the repaired bytes must reach the member's partial"
    );
}

/// And for `-hp`, where the headers are encrypted too: the repaired bytes are
/// still member payload, and the walk that placed them is the keyed one.
#[tokio::test]
async fn a_repaired_span_reroutes_into_a_header_encrypted_member() {
    let payload: Vec<u8> = (0..600u32).map(|index| (index % 251) as u8).collect();
    let volumes = header_encrypted_store_set(
        REPAIR_MEMBER,
        &payload,
        2,
        REPAIR_PASSWORD,
        HeaderCheck::For(REPAIR_PASSWORD),
    );
    let mut router = encrypted_router(&volumes, REPAIR_PASSWORD);
    route_all(&mut router, &volumes);
    assert!(
        router.all_members_verified(),
        "the undamaged download must verify before the repair is the question"
    );
    let (_, parts) = cipher_and_part_offsets(&payload, &volumes);

    let (part_at, part_len) = parts[0];
    let source_offset = part_at + 37;
    let len = part_len - 37 - 21;
    let spans = route_repaired_span(&mut router, &volumes, 0, source_offset, len)
        .expect("a repaired mid-part span must route back into an -hp member");
    assert!(
        member_bytes_written(&spans) > 0,
        "the repaired bytes must reach the member's partial"
    );
}

/// The shape a real repair takes: an article was lost, so the part never
/// completed, and the repaired span is what closes it.
///
/// This is the case the integrity layers actually adjudicate. A part with a hole
/// has no composed value at all, so layer 1 has never fired for it; the repaired
/// bytes are what makes it complete, and the gate then runs for the first time
/// over a composition that is part downloaded and part rebuilt.
#[tokio::test]
async fn a_repaired_hole_completes_an_encrypted_part_and_passes_its_gate() {
    let payload: Vec<u8> = (0..600u32).map(|index| (index % 251) as u8).collect();
    let volumes = encrypted_store_set(
        REPAIR_MEMBER,
        &payload,
        2,
        REPAIR_PASSWORD,
        Some(REPAIR_PASSWORD),
        true,
    );
    let mut router = encrypted_router(&volumes, REPAIR_PASSWORD);
    let (_, parts) = cipher_and_part_offsets(&payload, &volumes);
    let (part_at, _) = parts[0];

    // The lost article: a run inside the first volume's part, unaligned at both
    // ends, that never arrives on the wire.
    let hole_at = part_at + 37;
    let hole_len = 120;
    let first = &volumes[0].1;
    router
        .route(0, 0, &first[..hole_at as usize])
        .expect("the articles below the hole route");
    router
        .route(
            0,
            hole_at + hole_len,
            &first[(hole_at + hole_len) as usize..],
        )
        .expect("the articles above the hole route");
    for (index, (_, bytes)) in volumes.iter().enumerate().skip(1) {
        router
            .route(index as u32, 0, bytes)
            .expect("the remaining volumes route");
        router
            .note_volume_complete(index as u32)
            .expect("their articles are all in");
    }
    assert!(
        !router.all_members_verified(),
        "a member with a hole in it must not verify before the repair"
    );

    let spans = route_repaired_span(&mut router, &volumes, 0, hole_at, hole_len)
        .expect("the repaired hole must route back into the member");
    assert!(
        member_bytes_written(&spans) >= hole_len,
        "every repaired byte is member payload and must reach the partial — \
         along with whatever the hole was holding above it, whose cipher blocks \
         could not be chained across the gap"
    );
    close_stale_gaps(&mut router, &payload);
    assert!(
        router.all_members_verified(),
        "the completed part must pass its packed gate and the member its own"
    );
}

/// The shape the tail-loss fixture takes: an interior volume's **last**
/// articles never arrive, so the hole runs from inside the member's part to the
/// end of the volume and swallows the end-of-archive record with it. The volume
/// is never reported complete — nothing arrived to complete it — and the repair
/// is what closes both the part and the volume.
///
/// Both the member bytes and the end record are repaired bytes here, so every
/// one of them has to find a destination in the same drain: the record can only
/// be filed once the walk over the repaired image confirms the volume, and that
/// walk must run even though the volume's own article stream never delivered a
/// last article.
async fn a_repaired_volume_tail_reroutes(volumes: Vec<(String, Vec<u8>)>, payload: &[u8]) {
    let mut router = encrypted_router(&volumes, REPAIR_PASSWORD);
    let (_, parts) = cipher_and_part_offsets(payload, &volumes);
    let (part_at, part_len) = parts[1];

    // The lost tail: from an unaligned byte inside the interior volume's part
    // through the volume's last byte.
    let tail_at = part_at + part_len - 53;
    let interior = &volumes[1].1;
    let tail_len = interior.len() as u64 - tail_at;
    for (index, (_, bytes)) in volumes.iter().enumerate() {
        if index == 1 {
            router
                .route(1, 0, &interior[..tail_at as usize])
                .expect("the articles below the lost tail route");
            continue;
        }
        router
            .route(index as u32, 0, bytes)
            .expect("the intact volumes route");
        router
            .note_volume_complete(index as u32)
            .expect("their articles are all in");
    }
    assert!(
        !router.all_members_verified(),
        "a member missing its tail must not verify before the repair"
    );

    let spans = route_repaired_span(&mut router, &volumes, 1, tail_at, tail_len)
        .expect("a repaired volume tail must route back into the set");
    assert!(
        member_bytes_written(&spans) >= 53,
        "the repaired member bytes must reach the partial"
    );
    close_stale_gaps(&mut router, payload);
    assert!(
        router.all_members_verified(),
        "the completed part must pass its gate and the member its own"
    );
}

#[tokio::test]
async fn a_repaired_volume_tail_reroutes_into_an_encrypted_member() {
    let payload: Vec<u8> = (0..900u32).map(|index| (index % 251) as u8).collect();
    let volumes = encrypted_store_set(
        REPAIR_MEMBER,
        &payload,
        3,
        REPAIR_PASSWORD,
        Some(REPAIR_PASSWORD),
        true,
    );
    a_repaired_volume_tail_reroutes(volumes, &payload).await;
}

#[tokio::test]
async fn a_repaired_volume_tail_reroutes_into_a_header_encrypted_member() {
    let payload: Vec<u8> = (0..900u32).map(|index| (index % 251) as u8).collect();
    let volumes = header_encrypted_store_set(
        REPAIR_MEMBER,
        &payload,
        3,
        REPAIR_PASSWORD,
        HeaderCheck::For(REPAIR_PASSWORD),
    );
    a_repaired_volume_tail_reroutes(volumes, &payload).await;
}

/// The shape a corrupt-but-present article takes, scaled down: a middle volume
/// posted as several articles, one of them carrying flipped bytes the wire
/// checks could not see, and PAR2 rewriting the slice that holds it — which
/// starts at the volume's **first byte**, so the rewrite carries the volume's
/// headers and the first two articles whole.
///
/// Every article of the part is on record when the repair lands, so the part's
/// runs tile the rewrite exactly and go on tiling after each piece of it. The
/// rewrite reaches the composition as a head block, an aligned middle and a
/// tail block, and a gate that judged the part after the head block alone
/// composed that block's repaired value with the middle's wire-damaged one —
/// a mismatch over bytes that never existed, and with the repair already
/// re-routed, a demotion of a set whose bytes on disk were correct.
#[tokio::test]
async fn a_repaired_leading_slice_of_a_multi_article_encrypted_volume_reroutes() {
    let payload: Vec<u8> = (0..12_000u32).map(|index| (index % 251) as u8).collect();
    let volumes = encrypted_store_set(
        REPAIR_MEMBER,
        &payload,
        3,
        REPAIR_PASSWORD,
        Some(REPAIR_PASSWORD),
        true,
    );
    let mut router = encrypted_router(&volumes, REPAIR_PASSWORD);
    router.note_par2_available(true);
    let (_, parts) = cipher_and_part_offsets(&payload, &volumes);
    let (part_at, part_len) = parts[1];
    assert!(
        part_at < 500 && part_len > 3_000,
        "the fixture must hold several articles"
    );

    const ARTICLE: usize = 1_000;
    let pristine = &volumes[1].1;
    let mut damaged = pristine.clone();
    for byte in &mut damaged[500..508] {
        *byte ^= 0x5A;
    }

    for (index, (_, bytes)) in volumes.iter().enumerate() {
        if index == 1 {
            for (article, chunk) in damaged.chunks(ARTICLE).enumerate() {
                router
                    .route(1, (article * ARTICLE) as u64, chunk)
                    .expect("a damaged article the wire checks passed routes");
            }
        } else {
            router
                .route(index as u32, 0, bytes)
                .expect("an undamaged encrypted volume routes");
        }
        router
            .note_volume_complete(index as u32)
            .expect("the volume's articles are all in");
    }
    assert!(
        router.damaged_volumes().contains(&1),
        "the part gate must see the flipped cipher bytes and record the volume as damaged: {:?}",
        router.damaged_volumes()
    );
    assert!(!router.all_members_verified());

    let rewrite_len = (2 * ARTICLE) as u64;
    let spans = route_repaired_span(&mut router, &volumes, 1, 0, rewrite_len)
        .expect("a repaired leading slice must route back into the member");
    assert!(
        member_bytes_written(&spans) > 0,
        "the repaired bytes must reach the member's partial"
    );
    close_stale_gaps(&mut router, &payload);
    assert!(
        router.all_members_verified(),
        "the member must verify against the repaired image"
    );
}

/// A router over unencrypted `volumes`, with nothing routed yet.
fn plain_router(volumes: &[(String, Vec<u8>)]) -> DirectSetRouter {
    DirectSetRouter::new(DirectSetPlan {
        set_name: "silver.horizon".to_string(),
        volumes: (0..volumes.len() as u32)
            .map(|index| (index, index))
            .collect(),
        files: (0..volumes.len() as u32)
            .map(|index| (index, index))
            .collect(),
        identity: None,
        working_dir: std::path::PathBuf::from("/nonexistent"),
        destination_dir: std::path::PathBuf::from("/nonexistent-staging"),
    })
}

/// The plain-member counterpart. A plain slice reaches the composition as one
/// run, so a single rewritten slice is judged whole — but a volume with **two**
/// damaged slices is rewritten as two runs, and a gate that judged the part
/// after the first composed its repaired value with the second's wire-damaged
/// one. Same mixture, same wrongful demotion.
#[tokio::test]
async fn a_repair_of_two_slices_in_one_plain_volume_reroutes() {
    let payload: Vec<u8> = (0..12_000u32).map(|index| (index % 251) as u8).collect();
    let volumes = single_member_store_set(REPAIR_MEMBER, &payload, 3);
    let mut router = plain_router(&volumes);
    router.note_par2_available(true);

    const ARTICLE: usize = 1_000;
    let pristine = &volumes[1].1;
    let part = &payload[4_000..8_000];
    let part_at = pristine
        .windows(part.len())
        .position(|window| window == part)
        .expect("the fixture's part is in its own volume");
    assert!(part_at < 500, "the fixture must hold several articles");
    let mut damaged = pristine.clone();
    for at in [500usize, 2_500] {
        for byte in &mut damaged[at..at + 8] {
            *byte ^= 0x5A;
        }
    }

    for (index, (_, bytes)) in volumes.iter().enumerate() {
        if index == 1 {
            for (article, chunk) in damaged.chunks(ARTICLE).enumerate() {
                router
                    .route(1, (article * ARTICLE) as u64, chunk)
                    .expect("a damaged article the wire checks passed routes");
            }
        } else {
            router
                .route(index as u32, 0, bytes)
                .expect("an undamaged volume routes");
        }
        router
            .note_volume_complete(index as u32)
            .expect("the volume's articles are all in");
    }
    assert!(
        router.damaged_volumes().contains(&1),
        "the part gate must see the flipped bytes and record the volume as damaged: {:?}",
        router.damaged_volumes()
    );
    assert!(!router.all_members_verified());

    // Both damaged articles rewritten in one call, the way a volume's repaired
    // spans always arrive: two chunks, two runs into the same part.
    let chunks: Vec<RepairedChunk> = [0usize, 2 * ARTICLE]
        .into_iter()
        .map(|at| (at as u64, std::sync::Arc::from(&pristine[at..at + ARTICLE])))
        .collect();
    let spans = router
        .route_repaired(1, &chunks, &[], false)
        .expect("two repaired slices of one plain volume must route back into the member");
    assert!(
        member_bytes_written(&spans) > 0,
        "the repaired bytes must reach the member's partial"
    );
    assert!(
        !router.has_stale_gaps(),
        "article-shaped rewrites over article-shaped runs leave nothing to re-read"
    );
    assert!(
        router.all_members_verified(),
        "the member must verify against the repaired image"
    );
}
