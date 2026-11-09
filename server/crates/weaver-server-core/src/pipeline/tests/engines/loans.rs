// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Work lent out of the loop: a zip's keys over the ciphertext it holds, and
// the key a RAR or 7z store derives from the job's password. Each row posts
// one enciphered member, flat or inside a stored zip, serves it in one order,
// and wants it delivered whole. A ZipCrypto member whose sizes wait in a data
// descriptor is placed only as far as its keys reach, so its ciphertext comes
// down one article per pass and each pass makes the zip due again: the job
// keeps lending, or doing the work itself while a checkpoint is owed, until
// nothing is held, and never rules the member short while work is due.

use std::io::{Cursor, Write};

use ::zip::unstable::write::FileOptionsExt;
use ::zip::write::SimpleFileOptions;
use ::zip::{AesMode, ZipWriter};
use sevenz_turbo::EncoderMethod;

use super::harness::{Outcome, Post, Scenario, par2_set, payload, run};
use super::restart::restart_row;
use super::zip::stored;
use crate::pipeline::tests::direct_store::scenarios::sevenz_store::{
    Entry, build_7z_shaped, split_volumes,
};
use crate::pipeline::tests::direct_store::scenarios::{
    HeaderCheck, encrypted_rar4_store_set, encrypted_store_set, header_encrypted_store_set,
};

pub(in crate::pipeline) const SECRET: &str = "lantern-loan-secret";
const MEMBER: &str = "feature.mkv";
// Small articles, so a member spans dozens and its ciphertext comes down in
// as many passes.
pub(in crate::pipeline) const FRAME: usize = 1024;
const BODY: usize = 40 * FRAME + 77;

#[derive(Clone, Copy)]
pub(in crate::pipeline) enum Kind {
    // ZipCrypto, sizes in a data descriptor: zero in the local header.
    ZipCryptoDescriptor,
    // ZipCrypto, sizes in the local header.
    ZipCryptoSized,
    ZipAes,
    SevenZipAes,
    Rar5,
    Rar5Sealed,
    Rar4,
}

#[derive(Clone, Copy)]
enum Shape {
    Flat,
    // Inside a stored outer zip.
    Nested,
}

#[derive(Clone, Copy)]
enum Order {
    Forwards,
    Backwards,
    Leased,
    // The first post's last article first, as a tail seek asks it.
    TailFirst,
}

pub(in crate::pipeline) fn body() -> Vec<u8> {
    payload(BODY, 61)
}

fn zip_of(options: SimpleFileOptions, name: &str, bytes: &[u8]) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    writer.start_file(name, options).unwrap();
    writer.write_all(bytes).unwrap();
    writer.finish().unwrap().into_inner()
}

// One stored entry: the outer zip a nested row posts.
pub(in crate::pipeline) fn stored_zip(name: &str, bytes: &[u8]) -> Vec<u8> {
    zip_of(stored(), name, bytes)
}

// A writer that cannot seek back writes the sizes after the data, in a
// descriptor, and zeroes them in the local header.
pub(in crate::pipeline) fn zipcrypto_descriptor(bytes: &[u8]) -> Vec<u8> {
    let options = stored()
        .with_deprecated_encryption(SECRET.as_bytes())
        .unwrap();
    let mut writer = ZipWriter::new_stream(Vec::new());
    writer.start_file(MEMBER, options).unwrap();
    writer.write_all(bytes).unwrap();
    let archive = writer.finish().unwrap().into_inner();
    let flags = u16::from_le_bytes(archive[6..8].try_into().unwrap());
    let sizes = &archive[18..26];
    assert!(
        flags & 0x9 == 0x9,
        "encrypted, sizes in a descriptor: {flags:#x}"
    );
    assert!(sizes.iter().all(|byte| *byte == 0), "zero local sizes");
    archive
}

// The member's posts, as `kind` writes it: one archive, or a set's volumes.
pub(in crate::pipeline) fn volumes(
    kind: Kind,
    bytes: &[u8],
    parts: usize,
) -> Vec<(String, Vec<u8>)> {
    match kind {
        Kind::ZipCryptoDescriptor => vec![("lantern.zip".into(), zipcrypto_descriptor(bytes))],
        Kind::ZipCryptoSized => {
            let options = stored()
                .with_deprecated_encryption(SECRET.as_bytes())
                .unwrap();
            vec![("lantern.zip".into(), zip_of(options, MEMBER, bytes))]
        }
        Kind::ZipAes => {
            let options = stored().with_aes_encryption(AesMode::Aes256, SECRET);
            vec![("lantern.zip".into(), zip_of(options, MEMBER, bytes))]
        }
        Kind::SevenZipAes => {
            let archive = build_7z_shaped(
                &[Entry::file(MEMBER, bytes.to_vec())],
                EncoderMethod::COPY,
                Some(SECRET),
                false,
            );
            split_volumes(&archive, parts)
        }
        Kind::Rar5 => encrypted_store_set(MEMBER, bytes, parts, SECRET, Some(SECRET), false),
        Kind::Rar5Sealed => {
            header_encrypted_store_set(MEMBER, bytes, parts, SECRET, HeaderCheck::For(SECRET))
        }
        Kind::Rar4 => encrypted_rar4_store_set(MEMBER, bytes, parts, SECRET, None),
    }
}

fn posts(kind: Kind, shape: Shape, bytes: &[u8]) -> Vec<Post> {
    match shape {
        // A set spans two volumes where its kind has sets.
        Shape::Flat => volumes(kind, bytes, 2)
            .into_iter()
            .map(|(name, bytes)| Post::plain(&name, bytes))
            .collect(),
        Shape::Nested => {
            let [(name, inner)] = <[_; 1]>::try_from(volumes(kind, bytes, 1)).unwrap();
            vec![Post::plain("outer.zip", zip_of(stored(), &name, &inner))]
        }
    }
}

fn ordered(scenario: Scenario, order: Order) -> Scenario {
    match order {
        Order::Forwards => scenario,
        Order::Backwards => scenario.backwards(),
        Order::Leased => scenario.leased(),
        Order::TailFirst => {
            let last = scenario.posts[0].articles(FRAME).count() as u32 - 1;
            scenario.first(&[(0, last)])
        }
    }
}

fn scenario(kind: Kind, shape: Shape, order: Order) -> Scenario {
    let scenario = Scenario::new(posts(kind, shape, &body()))
        .framed(FRAME)
        .password(SECRET);
    ordered(scenario, order)
}

// Where the member's work was done: an AES zip entry is refused by the store
// and its key is derived and its bytes deciphered by an engine run, which
// lends nothing; every other kind lends its work out.
fn assert_worked(kind: Kind, outcome: &Outcome) {
    let graph = outcome.graph();
    match kind {
        Kind::ZipAes => {
            assert!(graph.kinds.contains("zip_engine"), "{:?}", graph.kinds);
            assert_eq!(graph.loans, 0, "a loan went out");
        }
        _ => assert!(graph.loans > 0, "a loan went out"),
    }
}

fn assert_delivered(kind: Kind, outcome: &Outcome) {
    assert_worked(kind, outcome);
    assert!(
        outcome.completed(),
        "{:?} {:?}",
        outcome.status,
        outcome.failure
    );
    let names: Vec<&String> = outcome.files.keys().collect();
    assert_eq!(
        names,
        [&format!("lantern beacon/{MEMBER}")],
        "delivered names"
    );
    assert!(
        outcome.files.values().next().unwrap() == &body(),
        "{MEMBER} bytes"
    );
}

async fn delivers(kind: Kind, shape: Shape, order: Order) {
    assert_delivered(kind, &run(&scenario(kind, shape, order)).await);
}

// One article of the first post damaged beside a PAR2 set over every post:
// the repair writes it back while the work is out or due, and the member
// still comes whole.
async fn repaired(kind: Kind, shape: Shape, order: Order) {
    let mut posts = posts(kind, shape, &body());
    let files: Vec<(String, Vec<u8>)> = posts
        .iter()
        .map(|post| (post.name.clone(), post.bytes.clone()))
        .collect();
    let named: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    posts.extend(par2_set("lantern", &named, 2));
    let middle = posts[0].articles(FRAME).count() as u32 / 2;
    let scenario = Scenario::new(posts)
        .framed(FRAME)
        .password(SECRET)
        .damaged(0, middle);
    assert_delivered(kind, &run(&ordered(scenario, order)).await);
}

// One article of the first post damaged and nothing to rebuild it: it is
// sent back while the work is out or due, its server has no other copy, and
// the one member is short. No usable file came whole, so the job fails.
async fn lost(kind: Kind, shape: Shape, order: Order) {
    let middle = posts(kind, shape, &body())[0].articles(FRAME).count() as u32 / 2;
    let outcome = run(&scenario(kind, shape, order).damaged(0, middle)).await;
    assert_worked(kind, &outcome);
    assert!(
        matches!(outcome.status, Some(crate::JobStatus::Failed { .. })),
        "{:?} {:?}",
        outcome.status,
        outcome.files.keys().collect::<Vec<_>>()
    );
}

// Restarted after every article served: a loan out or due when the
// pipeline goes down is found again on restore.
async fn restarted(kind: Kind, shape: Shape) {
    let whole = restart_row(scenario(kind, shape, Order::Forwards)).await;
    assert_worked(kind, &whole);
}

// A ZipCrypto member's keys never run on the loop, which runs on the test's
// thread: every byte of its ciphertext is deciphered once, on a loan.
async fn deciphered_on_loans(kind: Kind, order: Order) {
    use weaver_objects::zip::counting;
    let outcome = run(&scenario(kind, Shape::Flat, order)).await;
    assert_delivered(kind, &outcome);
    assert!(counting::delivered_here() > 0, "the loop delivered here");
    assert_eq!(counting::deciphered_here(), 0, "deciphered on the loop");
    assert_eq!(counting::deciphered_everywhere(), BODY as u64);
}

#[tokio::test]
async fn zipcrypto_sized_tail_first_deciphers_on_loans() {
    deciphered_on_loans(Kind::ZipCryptoSized, Order::TailFirst).await
}

#[tokio::test]
async fn zipcrypto_sized_backwards_deciphers_on_loans() {
    deciphered_on_loans(Kind::ZipCryptoSized, Order::Backwards).await
}

#[tokio::test]
async fn zipcrypto_descriptor_in_order_deciphers_on_loans() {
    deciphered_on_loans(Kind::ZipCryptoDescriptor, Order::Forwards).await
}

#[tokio::test]
async fn zipcrypto_descriptor_leased_deciphers_on_loans() {
    deciphered_on_loans(Kind::ZipCryptoDescriptor, Order::Leased).await
}

macro_rules! delivery_rows {
    ($($kind:ident => $flat:ident, $nested:ident;)*) => {
        $(
            mod $flat {
                use super::*;
                #[tokio::test]
                async fn in_order() { delivers(Kind::$kind, Shape::Flat, Order::Forwards).await }
                #[tokio::test]
                async fn backwards() { delivers(Kind::$kind, Shape::Flat, Order::Backwards).await }
                #[tokio::test]
                async fn leased() { delivers(Kind::$kind, Shape::Flat, Order::Leased).await }
                #[tokio::test]
                async fn tail_first() { delivers(Kind::$kind, Shape::Flat, Order::TailFirst).await }
                #[tokio::test]
                async fn repaired_in_order() { repaired(Kind::$kind, Shape::Flat, Order::Forwards).await }
                #[tokio::test]
                async fn repaired_backwards() { repaired(Kind::$kind, Shape::Flat, Order::Backwards).await }
                #[tokio::test]
                async fn restarted_after_every_article() { restarted(Kind::$kind, Shape::Flat).await }
                #[tokio::test]
                async fn lost_in_order() { lost(Kind::$kind, Shape::Flat, Order::Forwards).await }
                #[tokio::test]
                async fn lost_backwards() { lost(Kind::$kind, Shape::Flat, Order::Backwards).await }
            }
            mod $nested {
                use super::*;
                #[tokio::test]
                async fn in_order() { delivers(Kind::$kind, Shape::Nested, Order::Forwards).await }
                #[tokio::test]
                async fn backwards() { delivers(Kind::$kind, Shape::Nested, Order::Backwards).await }
                #[tokio::test]
                async fn leased() { delivers(Kind::$kind, Shape::Nested, Order::Leased).await }
                #[tokio::test]
                async fn tail_first() { delivers(Kind::$kind, Shape::Nested, Order::TailFirst).await }
                #[tokio::test]
                async fn repaired_in_order() { repaired(Kind::$kind, Shape::Nested, Order::Forwards).await }
                #[tokio::test]
                async fn repaired_backwards() { repaired(Kind::$kind, Shape::Nested, Order::Backwards).await }
                #[tokio::test]
                async fn restarted_after_every_article() { restarted(Kind::$kind, Shape::Nested).await }
                #[tokio::test]
                async fn lost_in_order() { lost(Kind::$kind, Shape::Nested, Order::Forwards).await }
            }
        )*
    };
}

delivery_rows! {
    ZipCryptoDescriptor => zipcrypto_descriptor_flat, zipcrypto_descriptor_nested;
    ZipCryptoSized => zipcrypto_sized_flat, zipcrypto_sized_nested;
    ZipAes => zip_aes_flat, zip_aes_nested;
    SevenZipAes => sevenz_aes_flat, sevenz_aes_nested;
    Rar5 => rar5_flat, rar5_nested;
    Rar5Sealed => rar5_sealed_flat, rar5_sealed_nested;
    Rar4 => rar4_flat, rar4_nested;
}
