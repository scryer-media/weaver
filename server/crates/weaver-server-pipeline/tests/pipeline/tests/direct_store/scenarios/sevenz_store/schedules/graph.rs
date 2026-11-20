// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::Selection;
use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::sevenz_store::schedules::{
    Shape, graph::campaign,
};

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
