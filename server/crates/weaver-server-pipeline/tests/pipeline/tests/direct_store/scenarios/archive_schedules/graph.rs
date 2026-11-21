// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::graph::{
    graph_campaign, graph_compressed_campaign,
};
use weaver_server_pipeline::pipeline::tests::direct_store::scenarios::archive_schedules::{
    CompressedFormat, Format, Selection,
};

mod two_sets;

// The smokes run in the archive-matrix profile, which alone selects this
// module.
macro_rules! graph_smokes {
    ($run:ident; $($name:ident $variant:expr),+ $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                $run($variant, Selection::Smoke).await;
            }
        )+
    };
}

graph_smokes!(graph_campaign;
    rar4_arrival_schedules Format::Rar4,
    rar5_arrival_schedules Format::Rar5,
    rar4_encrypted_arrival_schedules Format::Rar4Encrypted,
    rar4_unsalted_arrival_schedules Format::Rar4Unsalted,
    rar5_encrypted_arrival_schedules Format::Rar5Encrypted,
    rar5_keyed_checksum_arrival_schedules Format::Rar5KeyedChecksum,
    rar5_header_encrypted_arrival_schedules Format::Rar5EncryptedHeaders,
    rar5_unchecked_header_arrival_schedules Format::Rar5UncheckedHeaders,
    quick_open_arrival_schedules Format::QuickOpen,
    blake2_arrival_schedules Format::Blake2,
    rar4_four_volume_arrival_schedules Format::Rar4FourVolumes,
    rar5_four_volume_arrival_schedules Format::Rar5FourVolumes,
    rar4_encrypted_four_volume_arrival_schedules Format::Rar4EncryptedFourVolumes,
    rar5_obfuscated_arrival_schedules Format::Rar5Obfuscated,
    rar5_hex_bare_arrival_schedules Format::Rar5HexBare,
    rar4_hex_bare_arrival_schedules Format::Rar4HexBare,
    rar5_hex_self_described_arrival_schedules Format::Rar5HexSelfDescribed,
    rar4_hex_self_described_arrival_schedules Format::Rar4HexSelfDescribed,
    rar5_hex_late_index_arrival_schedules Format::Rar5HexLateIndex,
    rar4_hex_late_index_arrival_schedules Format::Rar4HexLateIndex,
    rar5_hex_misnumbered_arrival_schedules Format::Rar5HexMisnumbered,
    rar4_hex_misnumbered_arrival_schedules Format::Rar4HexMisnumbered,
    rar5_hex_swapped_rar_arrival_schedules Format::Rar5HexSwappedRar,
    rar4_hex_swapped_rar_arrival_schedules Format::Rar4HexSwappedRar,
    rar5_hex_scattered_arrival_schedules Format::Rar5HexScattered,
    rar4_hex_scattered_arrival_schedules Format::Rar4HexScattered,
    rar5_hex_single_arrival_schedules Format::Rar5HexSingle,
    rar4_hex_single_arrival_schedules Format::Rar4HexSingle,
);

graph_smokes!(graph_compressed_campaign;
    compressed_rar4_mixed_arrival_schedules CompressedFormat::Rar4Mixed,
    compressed_rar4_lz_arrival_schedules CompressedFormat::Rar4Lz,
    compressed_rar4_solid_arrival_schedules CompressedFormat::Rar4Solid,
    compressed_rar4_solid_encrypted_arrival_schedules CompressedFormat::Rar4SolidEncrypted,
    compressed_rar4_solid_headers_arrival_schedules CompressedFormat::Rar4SolidHeaders,
    compressed_rar4_ppmd_arrival_schedules CompressedFormat::Rar4Ppmd,
    compressed_rar4_ppmd_encrypted_arrival_schedules CompressedFormat::Rar4PpmdEncrypted,
    compressed_rar4_ppmd_headers_arrival_schedules CompressedFormat::Rar4PpmdHeaders,
    compressed_rar4_encrypted_arrival_schedules CompressedFormat::Rar4Encrypted,
    compressed_rar4_headers_arrival_schedules CompressedFormat::Rar4Headers,
    compressed_rar5_mixed_arrival_schedules CompressedFormat::Rar5Mixed,
    compressed_rar5_lz_arrival_schedules CompressedFormat::Rar5Lz,
    compressed_rar5_encrypted_arrival_schedules CompressedFormat::Rar5Encrypted,
    compressed_rar5_headers_arrival_schedules CompressedFormat::Rar5Headers,
    compressed_rar5_solid_arrival_schedules CompressedFormat::Rar5Solid,
    compressed_rar5_solid_encrypted_arrival_schedules CompressedFormat::Rar5SolidEncrypted,
    compressed_rar5_solid_headers_arrival_schedules CompressedFormat::Rar5SolidHeaders,
    compressed_rar4_lz_two_volume_arrival_schedules CompressedFormat::Rar4LzTwoVolumes,
    compressed_rar5_lz_two_volume_arrival_schedules CompressedFormat::Rar5LzTwoVolumes,
    compressed_rar4_lz_four_volume_arrival_schedules CompressedFormat::Rar4LzFourVolumes,
    compressed_rar5_lz_four_volume_arrival_schedules CompressedFormat::Rar5LzFourVolumes,
);

// The combined matrix under the graph: every case in shards, and the wrong
// password's schedules as a test of their own.
