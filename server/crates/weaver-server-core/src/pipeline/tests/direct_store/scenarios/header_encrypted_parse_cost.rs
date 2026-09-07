//! What a header-encrypted (`-hp`) set pays to learn its own layout.
//!
//! A volume's headers are parsed out of the image its articles stage, and the
//! image grows one article at a time — so the walk is retried as the volume
//! arrives. On a `-hp` set every one of those walks has to decrypt the headers,
//! and the key for that comes from a PBKDF2 run of up to 2^24 iterations over
//! (password, salt, KDF count). All three are properties of the volume rather
//! than of the walk, and a store volume's end-of-archive record sits past the
//! whole payload, so the naive shape pays the full derivation once per arriving
//! article for the entire volume and learns nothing new until its last one.
//!
//! Two things bound that, and this file pins both: the set derives into one
//! cache that outlives the walks, and a walk that ran out of bytes is not
//! repeated until the image can serve the byte it stopped at.

use super::*;

use crate::pipeline::direct_store::plan::DirectSetPlan;
use crate::pipeline::direct_store::router::DirectSetRouter;

const PARSE_COST_PASSWORD: &str = "lantern-tide";

const PARSE_COST_MEMBER: &str = "Silver.Horizon.S03E04.mkv";

/// The piece size the volumes are handed to the router in. Small enough that a
/// fixture volume takes tens of them, which is the point: the cost under test
/// is the one that scales with this number.
const PIECE_BYTES: usize = 512;

fn parse_cost_router(volumes: &[(String, Vec<u8>)]) -> DirectSetRouter {
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
    router.set_password(Some(PARSE_COST_PASSWORD));
    router.offer_header_password("Explicit", PARSE_COST_PASSWORD);
    router
}

/// Feeds every volume in ascending, in-order pieces — the cheapest possible
/// arrival order for the parser, and the one that makes the retry count a
/// property of the gate rather than of the shuffle.
fn route_in_pieces(router: &mut DirectSetRouter, volumes: &[(String, Vec<u8>)]) -> usize {
    let mut pieces = 0;
    for (index, (_, image)) in volumes.iter().enumerate() {
        for (piece, bytes) in image.chunks(PIECE_BYTES).enumerate() {
            router
                .route(index as u32, (piece * PIECE_BYTES) as u64, bytes)
                .expect("a `-hp` volume routes as it arrives");
            pieces += 1;
        }
        router
            .note_volume_complete(index as u32)
            .expect("the volume's articles are all in");
    }
    pieces
}

/// The whole point, in one number each: a set that arrives in a hundred pieces
/// derives its keys a fixed number of times and walks its headers a fixed
/// number of times.
#[tokio::test]
async fn a_header_encrypted_set_pays_for_its_key_once_however_many_articles_arrive() {
    let payload: Vec<u8> = (0..40_000u32).map(|index| (index % 251) as u8).collect();
    let volumes = header_encrypted_store_set(
        PARSE_COST_MEMBER,
        &payload,
        2,
        PARSE_COST_PASSWORD,
        HeaderCheck::For(PARSE_COST_PASSWORD),
    );
    let mut router = parse_cost_router(&volumes);
    let pieces = route_in_pieces(&mut router, &volumes);
    assert!(
        pieces > 60,
        "the fixture has to arrive in enough pieces for a per-piece cost to \
         show: {pieces}"
    );
    assert!(
        router.all_members_verified(),
        "the set still has to route and verify — a cheap parse that gets the \
         layout wrong is not the fix"
    );

    // Two tuples exist in this set: the archive key the headers are sealed
    // with, and the member key its data area is encrypted with. Each is
    // derived once, by whichever of the header ring, the key ring or a header
    // walk needed it first.
    assert_eq!(
        router.kdf_derivations(),
        2,
        "one derivation per key the set actually has, not one per walk"
    );
    // Per volume: the first piece's walk, which comes back
    // `EncryptedArchive`; the immediate retry once the archive key is proved;
    // and the walk over the complete image that confirms the volume. Anything
    // beyond those would be a walk that stopped where the previous one did.
    // (The set's later volumes take two: the key is already proved.)
    assert!(
        router.parse_walks() <= 3 * volumes.len() as u64,
        "the header walk is retried per answer, not per article: {} walks for \
         {pieces} pieces",
        router.parse_walks()
    );
}

/// The gate is about what the image can *reach*, not about how much of it has
/// arrived — and the walk that ends the volume still runs.
///
/// A stored volume's end-of-archive record sits past its whole payload, and
/// the walk that stops there asks for the byte after it: the volume's own end.
/// So every piece between the first and the last leaves the answer where it
/// was, and the walk that settles the volume is the one over its complete
/// image.
#[tokio::test]
async fn a_walk_is_repeated_only_once_it_could_answer_differently() {
    let payload: Vec<u8> = (0..20_000u32).map(|index| (index % 251) as u8).collect();
    let volumes = header_encrypted_store_set(
        PARSE_COST_MEMBER,
        &payload,
        1,
        PARSE_COST_PASSWORD,
        HeaderCheck::For(PARSE_COST_PASSWORD),
    );
    let image = &volumes[0].1;
    let mut router = parse_cost_router(&volumes);

    // The first piece carries the signature, the encryption record and the
    // headers, so the walk runs and stops in the volume's tail. It runs
    // *twice*: the first walk cannot read an encrypted header at all and
    // returns `EncryptedArchive`, which is what sends the volume's type-4
    // record to the header ring, and the retry is the walk that reads it.
    router
        .route(0, 0, &image[..PIECE_BYTES])
        .expect("the volume's first piece routes");
    let after_first = router.parse_walks();
    assert_eq!(
        after_first, 2,
        "the opening piece is walked once to find the headers encrypted and \
         once with the key that proves"
    );

    // Every remaining piece: the member's payload, which the walk seeks over,
    // and finally the tail. None of it can move an answer that is waiting on
    // the byte past the volume's end.
    let mut pieces = 1;
    for offset in (PIECE_BYTES..image.len()).step_by(PIECE_BYTES) {
        let to = (offset + PIECE_BYTES).min(image.len());
        router
            .route(0, offset as u64, &image[offset..to])
            .expect("the volume's remaining pieces route");
        pieces += 1;
    }
    assert!(pieces > 30, "the volume arrives in enough pieces: {pieces}");
    assert_eq!(
        router.parse_walks(),
        after_first,
        "a piece that cannot supply the byte the walk stopped at is not a \
         reason to walk again"
    );

    // The last article is the other half of the rule: a complete source image
    // is a proof in itself, so that walk runs however the gate feels about it
    // — and it is the walk that confirms the volume.
    router
        .note_volume_complete(0)
        .expect("the volume's articles are all in");
    assert_eq!(
        router.parse_walks(),
        after_first + 1,
        "the complete image is always walked"
    );
    assert!(
        router.all_members_verified(),
        "and that walk is what finishes the member"
    );
}
