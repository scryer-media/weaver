// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The extended format coverage under the graph: obfuscated RAR4 sets and the
// solid, encrypted and header-encrypted multi-volume compressed layouts.
//
// Every layout's member is wanted. A schedule delivers every member whole
// unless it starves the recovery set; then it delivers what the post still
// holds whole as the schedule left it. A wrong password delivers nothing
// from an encrypted layout and changes nothing for a plain one.
use super::*;

#[tokio::test]
async fn rar4_obfuscated_arrival_schedules() {
    graph_campaign(Format::Rar4Obfuscated, Selection::Smoke).await;
}

macro_rules! compressed_smokes {
    ($($name:ident $format:ident),+ $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                graph_compressed_campaign(CompressedFormat::$format, Selection::Smoke).await;
            }
        )+
    };
}

compressed_smokes!(
    compressed_rar4_solid_two_volume_arrival_schedules Rar4SolidTwoVolumes,
    compressed_rar5_solid_two_volume_arrival_schedules Rar5SolidTwoVolumes,
    compressed_rar4_solid_four_volume_arrival_schedules Rar4SolidFourVolumes,
    compressed_rar5_solid_four_volume_arrival_schedules Rar5SolidFourVolumes,
    compressed_rar4_encrypted_two_volume_arrival_schedules Rar4EncryptedTwoVolumes,
    compressed_rar5_encrypted_two_volume_arrival_schedules Rar5EncryptedTwoVolumes,
    compressed_rar4_encrypted_four_volume_arrival_schedules Rar4EncryptedFourVolumes,
    compressed_rar5_encrypted_four_volume_arrival_schedules Rar5EncryptedFourVolumes,
    compressed_rar4_headers_two_volume_arrival_schedules Rar4HeadersTwoVolumes,
    compressed_rar5_headers_two_volume_arrival_schedules Rar5HeadersTwoVolumes,
    compressed_rar4_headers_four_volume_arrival_schedules Rar4HeadersFourVolumes,
    compressed_rar5_headers_four_volume_arrival_schedules Rar5HeadersFourVolumes,
    compressed_rar4_solid_encrypted_two_volume_arrival_schedules Rar4SolidEncryptedTwoVolumes,
    compressed_rar5_solid_encrypted_two_volume_arrival_schedules Rar5SolidEncryptedTwoVolumes,
    compressed_rar4_solid_encrypted_four_volume_arrival_schedules Rar4SolidEncryptedFourVolumes,
    compressed_rar5_solid_encrypted_four_volume_arrival_schedules Rar5SolidEncryptedFourVolumes,
);
